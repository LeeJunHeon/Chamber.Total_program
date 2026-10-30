"""v1 두께 모니터(device/thickness_monitor.py + controller/thickness_service.py)를 실제로 실행해 골든을 만든다
→ v2/tests/golden/dev_stm100.json.

실행: cd <저장소 루트> && python3 v2/tools/v1golden_stm100.py

- v1 모듈은 importlib 로 동적으로 부른다(v2 안에서 v1 을 import 문으로 쓰지 않는다 — test_isolation).
- pure   : build_frame·extract_frame·parse_*·check_material·fmt_compact·strip_echo 의 결과(또는 예외).
- device : AsyncThicknessMonitor 를 127.0.0.1 가짜 TCP 서버에 붙여 읽기·영점·재료값·재시도·리셋 플래그를 실제로 실행 —
           서버가 받은 프레임과 결과.
- service: ThicknessService.poll_once() 를 같은 서버 위에서 차례로 — poll 마다 받은 명령 순서(연결 직후 전부, 다음은 S·T,
           S 값 없음이면 U·V·M 바로, 통신 실패 6회 연속이면 재연결).
서버 응답은 경우마다 정한 대본(받은 프레임마다 응답 하나, None = 무응답).
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_stm100.json"


def _load_v1():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    tm = importlib.import_module("device.thickness_monitor")
    ts = importlib.import_module("controller.thickness_service")
    tc = importlib.import_module("lib.thickness_config")
    return tm, ts, tc


def fr(payload: str) -> bytes:
    d = payload.encode("ascii")
    return bytes([2, len(d)]) + d + bytes([sum(d) & 0xFF])


def _exc(e: BaseException) -> dict:
    return {"exc": type(e).__name__, "msg": str(e)}


def _call(fn, *a):
    try:
        return {"value": fn(*a)}
    except Exception as e:
        return _exc(e)


# ───────── 순수 함수 ─────────
def pure(tm) -> dict:
    cmds = ["S", "T", "U", "V", "M", "L", "B", "C", "@", "a", "E?", "F?", "J?", "E=8.9", "F=1", "J=100",
            "E=19.3", "J=399", "", "12345678901", "1234567890"]
    out = {"build": [dict(cmd=c, **({"hex": tm.build_frame(c).hex()} if 1 <= len(c) <= 10 else _call(tm.build_frame, c)))
                     for c in cmds]}
    bufs = [fr("A1234"), b"\xff\x00" + fr("A1234"), fr("A12")[:-1], b"\x02\x00", b"\x02\x30" + b"x" * 60,
            fr("A12")[:-1] + b"\x00", b"abc", b"", fr("A1") + fr("B2"), b"\x02", fr("A" + "9" * 31)]
    ex = []
    for b in bufs:
        buf = bytearray(b)
        r = _call(tm.extract_frame, buf)
        r.update(buf=b.hex(), rest=bytes(buf).hex())
        ex.append(r)
    out["extract"] = ex
    bodies = ["1234", "-12", "+5", " 1234 ", "S 1234", "s 77", "--------", "    . ", "", "N/A", "12 .4", "12.5", "x",
              "+001.2", "-000.5", ".5", "5.", "T +001.2", "5319234", "U 5319234", "0", "-5", "100.0", "100.1",
              "@", "!", "M !", "M @", "#", "8.900", "E 8.90", "1e3"]
    parsers = ["parse_thickness_a", "parse_rate_a_s", "parse_frequency_hz", "parse_life_pct", "parse_crystal_ok"]
    pr = []
    for p in parsers:
        for b in bodies:
            pr.append(dict(fn=p, body=b, **_call(getattr(tm, p), b)))
    for tok in ("E", "F", "J"):
        for b in bodies:
            pr.append(dict(fn="parse_number", token=tok, body=b, **_call(tm.parse_number, b, tok)))
    out["parse"] = pr
    mats = [(8.9, 1.0, 100.0), (8.96, 0.437, 112.5), (1.23, 0.1, 10.0), (99.99, 9.999, 399.0), (0.5, 0.1, 10.0),
            (0.4999, 1.0, 100.0), (100.0, 1.0, 100.0), (8.9, 10.0, 100.0), (8.9, 1.0, 9.9), (8.9, 1.0, 399.06),
            (19.3001, 0.3815, 87.44), (2.7, 1.08, 400.0), (0.0, 1.0, 100.0), (12.3456, 1.23456, 123.456)]
    out["material"] = [dict(args=list(m), **_call(tm.check_material, *m)) for m in mats]
    out["compact"] = [dict(x=x, d=d, value=tm.fmt_compact(x, d)) for x, d in
                      ((1.230, 3), (1.0, 3), (100.0, 1), (0.0, 3), (-0.0001, 3), (12.3456, 3), (99.99, 2), (0.1, 3),
                       (399.06, 1), (8.9, 3), (-1.5, 1))]
    out["echo"] = [dict(body=b, token=t, value=tm.strip_echo(b, t)) for b, t in
                   (("U 5319234", "U"), ("M !", "M"), ("u 1", "U"), ("1234", "S"), ("E 8.9", "E"), ("", "S"),
                    ("S", "S"), ("@x", "@"), ("  T +1.0", "T"), ("a1", "a"))]
    out["error_text_labels"] = {"ThicknessTimeout": "응답 없음", "ThicknessCommError": "통신 실패",
                                "ThicknessChecksumError": "체크섬 오류", "ThicknessFrameError": "프레임 오류",
                                "ThicknessCommandRejected": "명령 거부", "ThicknessValueUnavailable": "값 없음",
                                "ThicknessVerifyError": "되읽기 불일치", "ThicknessError": "응답 형식 오류"}
    return out


# ───────── 가짜 서버 ─────────
class FakeServer:
    """STX LEN data CHK 프레임 하나마다 대본에서 응답 하나(bytes), None = 무응답. 대본이 비면 기본 응답(default)."""

    def __init__(self) -> None:
        self.script: list = []
        self.default = None
        self.rx: list = []
        self.conn = 0
        self.closed: list = []
        self.port = 0
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader, writer):
        self.conn += 1
        n = self.conn
        buf = b""
        try:
            while True:
                chunk = await reader.read(256)
                if not chunk:
                    break
                buf += chunk
                while len(buf) >= 2 and len(buf) >= buf[1] + 3:
                    f, buf = buf[:buf[1] + 3], buf[buf[1] + 3:]
                    self.rx.append({"conn": n, "cmd": f[2:-1].decode("ascii", "replace"), "hex": f.hex()})
                    r = self.script.pop(0) if self.script else (self.default(f) if self.default else None)
                    if r is not None:
                        writer.write(r)
                        await writer.drain()
        except Exception:
            pass
        self.closed.append(n)

    def reset(self, script, default=None):
        self.script, self.default, self.rx, self.conn, self.closed = list(script), default, [], 0, []


def normal_reply(f: bytes) -> bytes:
    c = f[2:-1].decode()
    return fr({"S": "A1234", "T": "A+001.2", "U": "A5319234", "V": "A87.5", "M": "A@", "E?": "A8.900",
               "F?": "A1.000", "J?": "A100.0", "L": "A"}.get(c, "A"))


def _dump(script):
    return [None if x is None else x.hex() for x in script]


async def device_cases(tm, server: FakeServer) -> list:
    out = []

    async def run(name, script, ops, timeout=0.3):
        server.reset(script)
        dev = tm.AsyncThicknessMonitor("127.0.0.1", server.port, cmd_timeout_s=timeout, settle_s=0.1)
        await dev.connect()
        res = []
        for op, args in ops:
            try:
                v = await getattr(dev, op)(*args)
                res.append({"op": op, "args": list(args), "ok": True, "value": v})
            except Exception as e:
                res.append(dict({"op": op, "args": list(args), "ok": False}, **_exc(e)))
        await dev.stop()
        await asyncio.sleep(0.05)
        out.append({"name": name, "script": _dump(script), "timeout_s": timeout, "tx": list(server.rx),
                    "results": res, "reset_seen": dev.reset_seen})

    await run("reads", [fr("A1234"), fr("A+001.2"), fr("A5319234"), fr("A87.5"), fr("A@"), fr("A8.900"),
                        fr("A1.000"), fr("A100.0"), fr("ASTM-100"), fr("A0")],
              [("read_thickness_a", ()), ("read_rate_a_s", ()), ("read_frequency_hz", ()), ("read_crystal_life", ()),
               ("read_crystal_ok", ()), ("read_density", ()), ("read_z_factor", ()), ("read_tooling", ()),
               ("read_version", ()), ("read_reset_status", ())])
    await run("echo_and_unavailable", [fr("AU 5319234"), fr("AM !"), fr("A--------"), fr("A    . "), fr("A12 .4")],
              [("read_frequency_hz", ()), ("read_crystal_ok", ()), ("read_thickness_a", ()), ("read_crystal_life", ()),
               ("read_thickness_a", ())])
    await run("retry_no_reply_then_ok", [None, fr("A55")], [("read_thickness_a", ())])
    await run("retry_bad_checksum_then_ok", [fr("A55")[:-1] + b"\x00", fr("A56")], [("read_thickness_a", ())])
    await run("retry_bad_len_then_ok", [b"\x02\x00", fr("A57")], [("read_thickness_a", ())])
    await run("retry_unknown_code_then_ok", [fr("X12"), fr("A58")], [("read_thickness_a", ())])
    await run("timeout_all", [None, None, None], [("read_thickness_a", ())])
    await run("frame_error_all", [fr("X1"), fr("X2"), fr("X3")], [("read_thickness_a", ())])
    await run("rejected", [fr("F"), fr("H"), fr("J")], [("read_thickness_a", ()), ("zero", ("B",)), ("read_rate_a_s", ())])
    await run("zero", [fr("A"), fr("A")], [("zero", ("B",)), ("zero", ("c",)), ("zero", ("D",))])
    await run("reset_flag_ack", [fr("B1234"), fr("A"), fr("B1235"), fr("A1236"), fr("B1237"), fr("A")],
              [("read_thickness_a", ()), ("read_thickness_a", ()), ("read_thickness_a", ()), ("read_thickness_a", ())])
    await run("set_material_ok", [fr("A"), fr("A"), fr("A"), fr("A8.960"), fr("A0.437"), fr("A112.5")],
              [("set_material", (8.96, 0.437, 112.5))])
    await run("set_material_retry_ok", [fr("A"), fr("A"), fr("A"), fr("A8.960"), fr("A0.437"), fr("A113.5"),
                                        fr("A"), fr("A"), fr("A"), fr("A8.960"), fr("A0.437"), fr("A112.5")],
              [("set_material", (8.96, 0.437, 112.5))])
    await run("set_material_verify_fail", [fr("A"), fr("A"), fr("A"), fr("A8.960"), fr("A0.437"), fr("A113.5")] * 2,
              [("set_material", (8.96, 0.437, 112.5))])
    await run("set_material_out_of_range", [], [("set_material", (0.4, 1.0, 100.0))])
    await run("set_material_rejected", [fr("H")], [("set_material", (8.9, 1.0, 100.0))])
    return out


class OffsetClock:
    def __init__(self):
        self.off = 0.0

    def __call__(self):
        return time.monotonic() + self.off


async def service_cases(tm, ts, tc, server: FakeServer, tmp: Path) -> list:
    out = []

    async def run(name, plan, default=normal_reply, script=(), timeout=0.3):
        """plan = [(시각 오프셋, 대본에 더할 응답 목록)] — 그 시각에 poll_once 한 번."""
        server.reset(script, default)
        clk = OffsetClock()
        cfg = tc.ThicknessConfig(enabled=True, ch=2, host="127.0.0.1", port=server.port, poll_interval_s=0.5,
                                 slow_poll_s=5.0, stale_s=10.0, cmd_timeout_s=timeout, log_dir=str(tmp),
                                 factor_file=str(tmp / "none.json"))
        dev = tm.AsyncThicknessMonitor("127.0.0.1", server.port, cmd_timeout_s=timeout, clock=clk)
        svc = ts.ThicknessService(cfg, log=lambda *_a: None, device=dev, materials_file=str(tmp / "none_m.json"),
                                  clock=clk)
        await dev.connect()
        polls = []
        t0 = time.monotonic()
        for off, extra in plan:
            clk.off = off - (time.monotonic() - t0)
            server.script += list(extra)
            n0 = len(server.rx)
            await svc.poll_once()
            await asyncio.sleep(0.02)
            st = svc.status()
            polls.append({"at": off, "cmds": [x["cmd"] for x in server.rx[n0:]],
                          "connected": bool(dev.is_connected), "seq": st["seq"],
                          "thickness_nm": st["thickness_nm"], "rate_nm_s": st["rate_nm_s"],
                          "crystal_ok": st["crystal_ok"], "freq_hz": st["freq_hz"], "life_pct": st["life_pct"],
                          "density": st["density"], "tooling": st["tooling"]})
        await dev.stop()
        await asyncio.sleep(0.05)
        out.append({"name": name, "timeout_s": timeout, "polls": polls, "closed": list(server.closed)})

    await run("normal_cadence", [(0.0, []), (0.5, []), (1.0, []), (5.25, []), (5.75, []), (10.5, [])])
    await run("xtal_fail_reads_m_now", [(0.0, []), (0.5, [fr("A--------"), fr("A--------"), fr("A    . "),
                                                         fr("A    . "), fr("A!")]),
                                        (1.0, [fr("A--------"), fr("A--------")]), (1.5, [])])
    await run("format_error_no_reconnect", [(0.0, []), (0.5, [fr("A12 .4")]), (1.0, [fr("F")]), (1.5, [])])
    await run("dead_device_first_poll", [(0.0, [None] * 18), (0.5, [])])
    await run("dead_after_ok", [(0.0, []), (0.5, [None] * 3), (1.0, [None] * 3), (1.5, [])])
    return out


async def _all(tm, ts, tc):
    server = FakeServer()
    await server.start()
    with tempfile.TemporaryDirectory() as d:
        dev = await device_cases(tm, server)
        svc = await service_cases(tm, ts, tc, server, Path(d))
    server.server.close()
    return dev, svc


def main() -> None:
    tm, ts, tc = _load_v1()
    dev, svc = asyncio.run(_all(tm, ts, tc))
    data = {"source": "v1 device/thickness_monitor.py·controller/thickness_service.py 를 실제로 실행"
                      "(v2/tools/v1golden_stm100.py) — 127.0.0.1 가짜 TCP 서버",
            "v1_files": ["device/thickness_monitor.py", "controller/thickness_service.py", "lib/thickness_config.py"],
            "pure": pure(tm), "device": dev, "service": svc}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{OUT} — device {len(dev)}, service {len(svc)}")


if __name__ == "__main__":
    main()
