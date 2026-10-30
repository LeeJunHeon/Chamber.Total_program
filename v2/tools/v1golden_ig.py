"""v1 IG 골든 생성 — v1 device/ig.py(AsyncIG)를 127.0.0.1 가짜 TCP 장비에 붙여 실제 함수(wait_for_base_pressure·cancel_wait·
ensure_on·read_pressure·ensure_off·set_process_status)를 실행하고, 가짜 장비가 받은 바이트와 v1 이벤트(base_reached·base_failed 문구·
pressure 값)를 v2/tests/golden/dev_ig.json 에 쓴다.

실행: cd <저장소 루트> && QT_QPA_PLATFORM=offscreen python v2/tools/v1golden_ig.py
- v1 구성은 main.py:439-449(cfg = 채널 모듈)와 TSP 런타임(cfg 없음 → config_ch2, runtime/tsp_runtime.py:522-523).
- 간격·대기·재연결 값만 줄였다(TIMING — 사례마다 IG_WAIT_TIMEOUT 등 추가). 보내는 바이트·순서와 무관하다. tsp 사례는 줄이지 않는다
  (cfg 없이 만든 AsyncIG 가 config_ch2 를 쓰는지 그대로 본다).
- 실제 장비·192.168.1.x 에는 접속하지 않는다(host 127.0.0.1).
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_ig.json"
TIMING = {"IG_GAP_MS": 30, "IG_FIRST_READ_DELAY_MS": 50, "IG_WATCHDOG_INTERVAL_MS": 20, "IG_RECONNECT_BACKOFF_START_MS": 20,
          "IG_RECONNECT_BACKOFF_MAX_MS": 40, "IG_REIGNITE_BACKOFF_MS": [20, 20, 20]}


class FakeIg:
    """받은 명령 기록. SIG 1 → sig1_reply, SIG 0 → 'OK', RDI → rdi 목록을 차례로(마지막 값 반복)."""

    def __init__(self, rdi, sig1_reply: str = "OK") -> None:
        self.rx: list = []
        self.rdi = list(rdi) or ["IG OFF"]
        self.sig1_reply = sig1_reply

    def answer(self, cmd: str):
        self.rx.append(cmd)
        if cmd == "SIG 1":
            return self.sig1_reply
        if cmd == "SIG 0":
            return "OK"
        if cmd == "RDI":
            return self.rdi.pop(0) if len(self.rdi) > 1 else self.rdi[0]
        return "ERR"

    async def handle(self, reader, writer):
        buf = b""
        try:
            while True:
                chunk = await reader.read(128)
                if not chunk:
                    break
                buf += chunk
                while b"\r" in buf:
                    line, buf = buf.split(b"\r", 1)
                    writer.write((self.answer(line.decode("ascii")) + "\r\n").encode())
                    await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()


def _cfg_ns(mod, extra: dict) -> types.SimpleNamespace:
    ns = types.SimpleNamespace(**{k: getattr(mod, k) for k in dir(mod) if k.isupper()})
    for k, v in {**TIMING, **extra}.items():
        setattr(ns, k, v)
    return ns


async def _case(ig_mod, cfg_mod, case: dict) -> dict:
    dev = FakeIg(case.get("rdi", []), case.get("sig1", "OK"))
    server = await asyncio.start_server(dev.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    if case.get("default_cfg"):
        ig = ig_mod.AsyncIG(host="127.0.0.1", port=port)          # TSP 런타임과 같게 cfg 없이
    else:
        ig = ig_mod.AsyncIG(host="127.0.0.1", port=port, cfg=_cfg_ns(cfg_mod, case.get("cfg", {})))
    events: list = []

    async def pump():
        async for ev in ig.events():
            if ev.kind in ("base_reached", "base_failed", "pressure"):
                events.append({"kind": ev.kind, "message": ev.message, "pressure": ev.pressure})
    pt = asyncio.create_task(pump())
    result = None
    steps = case["steps"]
    for st in steps:
        if st == "wait":
            result = await asyncio.wait_for(ig.wait_for_base_pressure(case["target"], interval_ms=case.get("interval_ms", 50)), 60)
        elif st == "wait_then_cancel":
            t = asyncio.create_task(ig.wait_for_base_pressure(case["target"], interval_ms=case.get("interval_ms", 50)))
            await asyncio.sleep(case.get("cancel_after", 0.4))
            await ig.cancel_wait()
            result = await asyncio.wait_for(t, 30)
        elif st == "ensure_on":
            await ig.ensure_on()
        elif st == "read_pressure":
            result = await ig.read_pressure()
        elif st == "ensure_off":
            await ig.ensure_off()
        elif st == "bg_poll":
            ig.set_process_status(True)
            for _ in range(500):
                if "RDI" in dev.rx:
                    break
                await asyncio.sleep(0.01)
            ig.set_process_status(False)
        await asyncio.sleep(0.3)
    await asyncio.sleep(0.3)
    used = getattr(ig._cfg, "__name__", "SimpleNamespace(채널 모듈 복사)")
    sent = list(dev.rx)                                       # 도구의 마지막 정리(SIG 0) 전까지만 기록
    await ig.cleanup()
    await asyncio.sleep(0.1)
    pt.cancel()
    server.close()
    await server.wait_closed()
    return {"name": case["name"], "chamber": case["chamber"], "steps": steps, "target": case.get("target"),
            "interval_ms": case.get("interval_ms"), "rdi": case.get("rdi", []), "sig1": case.get("sig1", "OK"),
            "cfg": case.get("cfg", {}), "default_cfg": bool(case.get("default_cfg")), "cfg_used": used,
            "result": result, "sent": [(c + "\r").encode("ascii").hex() for c in sent], "sent_text": sent,
            "events": events}


CASES = [
    {"name": "base_reached", "chamber": "ch1", "steps": ["wait"], "target": 1e-5,
     "rdi": ["1.5x10e-5", "1.2x10e-5", "8.1x10e-6"]},
    {"name": "ig_on_failed", "chamber": "ch1", "steps": ["wait"], "target": 1e-5, "sig1": "ERR", "rdi": ["1.0x10e-6"]},
    {"name": "reignite_then_reached", "chamber": "ch2", "steps": ["wait"], "target": 1e-5, "rdi": ["IG OFF", "5.0x10e-6"]},
    {"name": "reignite_limit", "chamber": "ch2", "steps": ["wait"], "target": 1e-5, "rdi": ["IG OFF"]},
    {"name": "wait_timeout_no_tail", "chamber": "ch1", "steps": ["wait"], "target": 1e-5, "interval_ms": 100,
     "rdi": ["5.0x10e-5"], "cfg": {"IG_WAIT_TIMEOUT": 0.25}},
    {"name": "parse_fail_then_reached", "chamber": "ch1", "steps": ["wait"], "target": 1e-5, "rdi": ["abc", "1.0x10e-6"]},
    {"name": "cancel_wait", "chamber": "ch2", "steps": ["wait_then_cancel"], "target": 1e-5, "interval_ms": 100,
     "rdi": ["5.0x10e-5"], "cancel_after": 0.4},
    {"name": "ensure_read_off", "chamber": "ch1", "steps": ["ensure_on", "read_pressure", "ensure_off"], "rdi": ["3.3x10e-6"]},
    {"name": "bg_poll", "chamber": "ch2", "steps": ["bg_poll"], "rdi": ["2.0x10e-6"], "cfg": {"IG_POLLING_INTERVAL_MS": 50}},
    {"name": "tsp_default_cfg", "chamber": "tsp", "steps": ["ensure_on", "read_pressure", "ensure_off"], "rdi": ["4.0x10e-7"],
     "default_cfg": True},
]


async def _main() -> dict:
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    importlib.import_module("lib._config_loader").load_settings()
    ig_mod = importlib.import_module("device.ig")
    mods = {"ch1": importlib.import_module("lib.config_ch1"), "ch2": importlib.import_module("lib.config_ch2")}
    out = []
    for c in CASES:
        out.append(await _case(ig_mod, mods.get(c["chamber"], mods["ch2"]), c))
    ch2 = mods["ch2"]
    return {"meta": {"source": "v1 device/ig.py AsyncIG — 실제 실행", "generator": "v2/tools/v1golden_ig.py",
                     "timing_overrides": TIMING, "ch1_IG_WAIT_TIMEOUT": mods["ch1"].IG_WAIT_TIMEOUT,
                     "ch2_IG_WAIT_TIMEOUT": ch2.IG_WAIT_TIMEOUT, "default_cfg_module": ig_mod.cfg_default.__name__,
                     "fake_device": "SIG 1 → 'OK'(또는 사례 값), SIG 0 → 'OK', RDI → 사례 목록 차례로(마지막 반복), 줄 끝 CR LF"},
            "cases": out}


def main() -> None:
    data = asyncio.run(_main())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT}: {len(data['cases'])} 사례")


if __name__ == "__main__":
    main()
