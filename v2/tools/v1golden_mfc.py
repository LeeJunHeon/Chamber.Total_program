"""v1 MFC 골든 생성 — v1 device/mfc.py(AsyncMFC)를 127.0.0.1 가짜 TCP 장비에 붙여 실제 명령 함수(handle_command·set_process_status)를
실행하고, 가짜 장비가 받은 바이트(명령 하나 = '…\\r' 한 덩이)와 v1 이 낸 확인/실패 이벤트를 v2/tests/golden/dev_mfc.json 에 쓴다.

실행: cd <저장소 루트> && QT_QPA_PLATFORM=offscreen python v2/tools/v1golden_mfc.py
- v1 코드는 importlib 로 동적으로 불러온다(v2 안의 다른 모듈은 v1 을 import 하지 않는다 — test_isolation).
- v1 구성은 main.py:419-436 그대로: enable_verify=False·enable_stabilization=True, cfg = 채널 모듈(config_ch1/config_ch2 를
  settings.json 적용 뒤 복사한 이름 공간). 시간을 줄이려고 간격·대기 값만 줄였다(TIMING) — 보내는 바이트·순서와 무관하다
  (v1 은 간격을 sleep 으로만 쓴다). 줄인 값은 골든 meta 에 남긴다.
- 실제 장비·192.168.1.x 에는 접속하지 않는다(host 를 127.0.0.1 로 덮어씀).
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
OUT = ROOT / "v2" / "tests" / "golden" / "dev_mfc.json"
TIMING = {"MFC_GAP_MS": 30, "MFC_DELAY_MS": 30, "MFC_DELAY_MS_VALVE": 30, "MFC_ZEROING_GAP_MS": 30,
          "MFC_POST_OPEN_QUIET_MS": 10, "MFC_STABILIZATION_INTERVAL_MS": 30, "MFC_PRESSURE_CHECK_INTERVAL_SEC": 0.03,
          "MFC_POLLING_INTERVAL_MS": 200, "MFC_ALLOW_NO_REPLY_DRAIN_MS": 10}


class FakeMfc:
    """v1 파서가 읽는 응답 모양의 가짜 MFC — 받은 명령을 기록하고 상태를 바꾼다(유량은 즉시 설정값)."""

    def __init__(self) -> None:
        self.rx: list = []
        self.set_pct = [0.0] * 4
        self.on = [False] * 4
        self.sp = [0.0] * 5
        self.pressure_hw = 0.20

    def answer(self, cmd: str):
        self.rx.append(cmd)
        if cmd == "R60":
            return "Q0" + "".join(f"{(self.set_pct[i] if self.on[i] else 0.0):+.1f}" for i in range(4))
        if cmd == "R5":
            return f"P{self.pressure_hw:+.2f}"
        if cmd in ("R1", "R2", "R3", "R4"):
            n = int(cmd[1])
            return f"S{n}{self.sp[n - 1]:+07.2f}"
        if cmd.startswith("Q") and " " in cmd:
            self.set_pct[int(cmd[1]) - 1] = float(cmd.split()[1])
        elif cmd.startswith("L") and len(cmd) == 3 and cmd[1] in "1234":
            self.on[int(cmd[1]) - 1] = cmd[2] == "1"
        elif cmd.startswith("S") and " " in cmd:
            self.sp[int(cmd[1]) - 1] = float(cmd.split()[1])
        return None

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
                    ans = self.answer(line.decode("ascii"))
                    if ans is not None:
                        writer.write((ans + "\r\n").encode())
                        await writer.drain()
        finally:
            writer.close()


def _cfg_ns(mod) -> types.SimpleNamespace:
    ns = types.SimpleNamespace(**{k: getattr(mod, k) for k in dir(mod) if k.isupper()})
    for k, v in TIMING.items():
        setattr(ns, k, v)
    return ns


async def _run_chamber(mfc_mod, cfg_mod, chamber: str, cases: list) -> list:
    dev = FakeMfc()
    server = await asyncio.start_server(dev.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    cfg = _cfg_ns(cfg_mod)
    m = mfc_mod.AsyncMFC(host="127.0.0.1", port=port, enable_verify=False, enable_stabilization=True,
                         scale_factors=dict(cfg.MFC_SCALE_FACTORS), cfg=cfg)
    events: list = []

    async def pump():
        async for ev in m.events():
            if ev.kind in ("command_confirmed", "command_failed"):
                events.append({"kind": ev.kind, "cmd": ev.cmd, "reason": ev.reason, "channel": ev.channel})
    pt = asyncio.create_task(pump())
    await m.start()
    for _ in range(200):
        if m.is_connected():
            break
        await asyncio.sleep(0.01)
    out = []
    for case in cases:
        name, cmd, args, setup = case["name"], case["cmd"], case.get("args") or {}, case.get("setup") or {}
        if "pressure_hw" in setup:
            dev.pressure_hw = setup["pressure_hw"]
        if "sp" in setup:
            for n, v in setup["sp"].items():
                dev.sp[int(n) - 1] = float(v)
        n0, e0 = len(dev.rx), len(events)
        if cmd == "POLL_CYCLE":
            m.set_process_status(True)
            for _ in range(300):
                if len(dev.rx) >= n0 + 2:
                    break
                await asyncio.sleep(0.01)
            m.set_process_status(False)
        else:
            await m.handle_command(cmd, args, owner=f"chamber{chamber[-1]}")
            if case.get("wait_event"):
                for _ in range(500):
                    if any(e["cmd"] == case["wait_event"] for e in events[e0:]):
                        break
                    await asyncio.sleep(0.01)
        await asyncio.sleep(0.15)
        out.append({"name": name, "chamber": chamber, "cmd": cmd, "args": args, "setup": setup,
                    "sent": [(c + "\r").encode("ascii").hex() for c in dev.rx[n0:]],
                    "sent_text": dev.rx[n0:], "events": events[e0:]})
    await m.cleanup()
    pt.cancel()
    server.close()
    await server.wait_closed()
    return out


CASES_CH1 = [
    {"name": "flow_set_ch1_10", "cmd": "FLOW_SET", "args": {"channel": 1, "value": 10.0}},
    {"name": "flow_on_ch1_stab", "cmd": "FLOW_ON", "args": {"channel": 1}, "wait_event": "FLOW_ON"},
    {"name": "flow_off_ch1", "cmd": "FLOW_OFF", "args": {"channel": 1}},
    {"name": "flow_on_ch2_no_target", "cmd": "FLOW_ON", "args": {"channel": 2}, "wait_event": "FLOW_ON"},
    {"name": "flow_off_ch2", "cmd": "FLOW_OFF", "args": {"channel": 2}},
    {"name": "flow_set_ch2_0_3", "cmd": "FLOW_SET", "args": {"channel": 2, "value": 0.3}},
    {"name": "valve_open", "cmd": "VALVE_OPEN"},
    {"name": "valve_close", "cmd": "VALVE_CLOSE"},
    {"name": "sp1_set_2_5", "cmd": "SP1_SET", "args": {"value": 2.5}},
    {"name": "sp2_set_30", "cmd": "SP2_SET", "args": {"value": 30.0}},
    {"name": "sp4_set_1_234", "cmd": "SP4_SET", "args": {"value": 1.234}},
    {"name": "sp1_on", "cmd": "SP1_ON"}, {"name": "sp2_on", "cmd": "SP2_ON"},
    {"name": "sp3_on", "cmd": "SP3_ON"}, {"name": "sp4_on", "cmd": "SP4_ON"},
    {"name": "ps_zeroing", "cmd": "PS_ZEROING"},
    {"name": "mfc_zeroing_ch1", "cmd": "MFC_ZEROING", "args": {"channel": 1}},
    {"name": "mfc_zeroing_ch3", "cmd": "MFC_ZEROING", "args": {"channel": 3}},
    {"name": "wait_pressure_2", "cmd": "WAIT_PRESSURE", "args": {"target": 2.0}, "setup": {"pressure_hw": 0.20}},
    {"name": "wait_pressure_sp3", "cmd": "WAIT_PRESSURE", "args": {"target": 1.0, "use_sp_target": True, "sp_index": 3},
     "setup": {"pressure_hw": 0.30, "sp": {"3": 0.30}}},
    {"name": "read_flow_all", "cmd": "READ_FLOW_ALL"},
    {"name": "read_pressure", "cmd": "READ_PRESSURE"},
    {"name": "sp3_set_unsupported", "cmd": "SP3_SET", "args": {"value": 1.0}},
    {"name": "flow_set_missing_arg", "cmd": "FLOW_SET", "args": {"channel": 1}},
    {"name": "poll_cycle", "cmd": "POLL_CYCLE"},
]
CASES_CH2 = [
    {"name": "flow_set_ch2_o2_1_1", "cmd": "FLOW_SET", "args": {"channel": 2, "value": 1.1}},
    {"name": "flow_set_ch2_o2_0_07", "cmd": "FLOW_SET", "args": {"channel": 2, "value": 0.07}},
    {"name": "flow_set_ch2_o2_0_09", "cmd": "FLOW_SET", "args": {"channel": 2, "value": 0.09}},
    {"name": "flow_set_ch2_n2_1_15", "cmd": "FLOW_SET", "args": {"channel": 3, "value": 1.15}},
    {"name": "flow_on_ch3_stab", "cmd": "FLOW_ON", "args": {"channel": 3}, "wait_event": "FLOW_ON"},
    {"name": "flow_off_ch3", "cmd": "FLOW_OFF", "args": {"channel": 3}},
    {"name": "flow_set_ch1_ar_20", "cmd": "FLOW_SET", "args": {"channel": 1, "value": 20}},
    {"name": "sp4_set_4", "cmd": "SP4_SET", "args": {"value": 4.0}},
    {"name": "sp4_on", "cmd": "SP4_ON"},
    {"name": "wait_pressure_sp4", "cmd": "WAIT_PRESSURE", "args": {"target": 1.0, "use_sp_target": True, "sp_index": 4},
     "setup": {"pressure_hw": 0.40}},
    {"name": "wait_pressure_zero_target", "cmd": "WAIT_PRESSURE", "args": {"target": 0.0}},
]


async def _main() -> dict:
    sys.path.insert(0, str(ROOT))
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    loader = importlib.import_module("lib._config_loader")
    loader.load_settings()                                  # 운영과 같게 settings.json 적용(전부 아니면 전무)
    mfc_mod = importlib.import_module("device.mfc")
    ch1 = importlib.import_module("lib.config_ch1")
    ch2 = importlib.import_module("lib.config_ch2")
    cases = await _run_chamber(mfc_mod, ch1, "ch1", CASES_CH1)
    cases += await _run_chamber(mfc_mod, ch2, "ch2", CASES_CH2)
    return {"meta": {"source": "v1 device/mfc.py AsyncMFC(enable_verify=False, enable_stabilization=True) — 실제 실행",
                     "generator": "v2/tools/v1golden_mfc.py", "timing_overrides": TIMING,
                     "scale_ch1": {str(k): v for k, v in ch1.MFC_SCALE_FACTORS.items()},
                     "scale_ch2": {str(k): v for k, v in ch2.MFC_SCALE_FACTORS.items()},
                     "fake_device": "R60 'Q0+…'(켜진 채널 = 설정 %FS 즉시) · R5 'P+x.xx' · R1~R4 'S{n}+xxx.xx' · 설정 명령 무응답"},
            "cases": cases}


def main() -> None:
    data = asyncio.run(_main())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT}: {len(data['cases'])} 사례")


if __name__ == "__main__":
    main()
