"""v1 DC Pulse 드라이버(device/dc_pulse.py)를 실제로 실행해 골든을 만든다 → v2/tests/golden/dev_dcpulse.json.

실행: cd <저장소 루트> && python3 v2/tools/v1golden_dcpulse.py      (실제 시간으로 돈다 — 약 2분)

- v1 모듈은 importlib 로 동적으로 부른다(v2 안에서 v1 을 import 문으로 쓰지 않는다 — test_isolation).
  v1 설정은 lib/_config_loader.load_settings() 로 config/settings.json 을 적용한 운영값(DC_OVERCURRENT_ENABLE false 등).
- frames   : v1 BinaryProtocol.pack_write/pack_read 가 만든 바이트(명령 이름·인자·hex).
- functions: v1 순수 함수(dcp_max_off_time_us·duty↔off·_arc_reset_state·_arc_split·_piv_scale_check·
             lib/power_limits.dc_overcurrent_limit_a) 결과.
- parser   : v1 _tcp_reader_loop 에 원시 바이트를 흘려 넣고 프레임 큐에 나온 토큰.
- cases    : v1 AsyncDCPulse 를 127.0.0.1 가짜 TCP 서버에 붙여 prepare_and_start·output_off·output_off_unused·
             set_reference_power·read_actual_pulse_params 를 실제로 부르고, 서버가 받은 프레임(순서대로)과 결과를 적는다.
             가짜 장비는 v2/ch12/sim/dcpulse_sim.DcPulseSim(경우마다 정한 초기 상태) — v2 시험도 같은 시뮬레이터·같은 상태로
             돌려 보낸 바이트를 비교한다(활성화 대기 중 PIV 반복 횟수는 실제 시간 지터가 있어 연속 중복은 하나로 접어 비교).
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_dcpulse.json"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from v2.ch12.sim.chamber import Chamber            # noqa: E402
from v2.ch12.sim.dcpulse_sim import DcPulseSim     # noqa: E402

# 경우(이름, 부를 함수, 인자, 시뮬레이터 초기 상태, 먼저 켜 둘지)
CASES = [
    ("start_normal", "prepare_and_start", dict(power_w=100.0, freq=50, off_time_us=5.0), {}, False),
    ("start_dc_freq_keep", "prepare_and_start", dict(power_w=200.0, freq=None, off_time_us="DC"), {}, False),
    ("start_duty_legacy", "prepare_and_start", dict(power_w=100.0, freq=100, duty=70), {}, False),
    ("start_freq_only", "prepare_and_start", dict(power_w=150.0, freq=80), {}, False),
    ("start_off_ignored_once", "prepare_and_start", dict(power_w=100.0, freq=50, off_time_us=4.0), {"ignore_off_once": True}, False),
    ("start_freq_ignored_once", "prepare_and_start", dict(power_w=100.0, freq=60, off_time_us=3.0), {"ignore_freq_once": True}, False),
    ("start_remote_mode", "prepare_and_start", dict(power_w=100.0, freq=50, off_time_us=5.0), {"ctrl_mode": 2}, False),
    ("start_fault_reset", "prepare_and_start", dict(power_w=100.0, freq=50, off_time_us=5.0), {"fault": 0x0010}, False),
    ("start_freq_out_of_range", "prepare_and_start", dict(power_w=100.0, freq=151, off_time_us=2.0), {}, False),
    ("start_off_over_limit", "prepare_and_start", dict(power_w=100.0, freq=100, off_time_us=5.0), {}, False),
    ("start_hv_not_on", "prepare_and_start", dict(power_w=100.0, freq=50, off_time_us=5.0), {"on_ignored": True}, False),
    ("start_sync_ext", "prepare_and_start", dict(power_w=100.0, freq=50, off_time_us=5.0), {"sync": 1}, False),
    ("off_after_on", "output_off", {}, {}, True),
    ("off_err_reopen", "output_off", {}, {"off_err_n": 1}, True),
    ("off_unused", "output_off_unused", {}, {}, False),
    ("set_reference_power", "set_reference_power", dict(value_w=250.0), {}, False),
    ("read_actual_pulse_params", "read_actual_pulse_params", {}, {"freq_khz": 80, "off_raw": 30}, False),
]


def _load_v1():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    importlib.import_module("lib._config_loader").load_settings()
    return importlib.import_module("device.dc_pulse"), importlib.import_module("lib.power_limits")


def frames(m) -> list:
    pr = m.BinaryProtocol()
    out = []
    writes = [("MASTER_ONOFF", 0x7B, 3), ("MASTER_REFER", 0x7C, 3), ("MASTER_MODE", 0x7D, 3), ("REG_POWER", 0x81, 3),
              ("OUTPUT_ON", 0x80, 1), ("OUTPUT_OFF", 0x80, 2), ("PULSE_SYNC_INT", 0x65, 0), ("PULSE_SYNC_EXT", 0x65, 1),
              ("FAULT_RESET", 0x6F, 1), ("ARC_COUNT_RESET", 0x8C, 2), ("OFF_TIME_DC", 0x67, 9)]
    writes += [(f"REF_POWER_RAW_{v}", 0x83, v) for v in (0, 1, 10, 25, 50, 100, 255, 256, 0x303)]
    writes += [(f"PULSE_FREQ_{v}", 0x66, v) for v in (20, 40, 50, 100, 150)]
    writes += [(f"OFF_TIME_RAW_{v}", 0x67, v) for v in (10, 30, 32, 50, 100)]
    for name, code, val in writes:
        out.append({"name": name, "kind": "write", "code": code, "value": val, "hex": pr.pack_write(code, val, width=2).hex(" ")})
    for code in (0x90, 0x91, 0x96, 0x99, 0x9A, 0x9C, 0x9E, 0xA5, 0xA6, 0xA7, 0xAE, 0xAF, 0xBB, 0xBC, 0xBD):
        out.append({"name": f"READ_{code:02X}", "kind": "read", "code": code, "hex": pr.pack_read(code).hex(" ")})
    return out


def functions(m, pl) -> dict:
    f = {}
    f["max_off_time_us"] = [[k, m.dcp_max_off_time_us(k)] for k in (0, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150)]
    f["off_time_to_raw"] = [[u, m.dcp_off_time_to_raw(u)] for u in (1.0, 2.5, 3.0, 4.04, 5.0, 9.99, 10.0)]
    f["raw_to_off_time"] = [[r, m.dcp_raw_to_off_time(r)] for r in (9, 10, 32, 100)]
    f["duty_to_off"] = [[k, d, m.dcp_duty_to_off_time_us(k, d)] for k in (20, 50, 100, 150) for d in (0, 50, 70, 90, 100)]
    f["off_to_duty"] = [[k, o, m.dcp_off_time_to_duty_pct(k, o)] for k in (20, 50, 100) for o in (1.0, 3.0, 5.0)]
    f["arc_reset_state"] = [[a, b, c, d, m._arc_reset_state(a, b, c, d)] for a, b, c, d in
                            ((None, None, None, None), (0, 0, 0, 0), (2, 3, 2, 3), (2, 3, 0, 3), (5, None, 4, None), (0, 7, 0, 9))]
    f["arc_split"] = [[a, b, c, d, m._arc_split(a, b, c, d)] for a, b, c, d in
                      ((10, 5, None, None), (10, 5, 3, 2), (10, 5, 20, 20), (-1, 0, 0, 0))]
    f["piv_scale_check"] = [[p, i, v, list(r) if (r := m._piv_scale_check(p, i, v)) else None] for p, i, v in
                            ((0, 1, 1), (100, 0, 400), (100, 0.25, 400), (100, 0.3, 400), (500, 1.0, 600))]
    f["overcurrent_limit"] = [[w, pl.dc_overcurrent_limit_a(w)] for w in (None, 0, 100, 300, 450, 600, 900, -5)]
    return f


def parser_cases(m) -> list:
    """v1 _tcp_reader_loop 에 바이트를 흘려 넣고 프레임 큐에 나온 토큰(hex)."""
    raws = ["06", "04", "06 04 02 9c 00 01 03 a2", "02 9a 00 0a 00 19 01 90 03 cc", "02 96 03 00 03 9e",
            "ff 02 9e 00 00 03 a3", "02 9e 00 00 03 a4 02 9c 00 01 03 a2", "aa bb 06", "02 a7 00 32 03 de"]
    out = []
    for hx in raws:
        out.append({"raw": hx, "tokens": asyncio.run(_parse_one(m, bytes.fromhex(hx)))})
    return out


async def _parse_one(m, raw: bytes) -> list:
    d = m.AsyncDCPulse(host="127.0.0.1", port=1)
    r = asyncio.StreamReader()
    r.feed_data(raw)
    r.feed_eof()
    d._reader, d._connected = r, True
    d._on_tcp_disconnected = lambda: None          # 끝(EOF)에서 큐를 비우지 않게 — 토큰을 꺼내 보려고
    try:
        await asyncio.wait_for(d._tcp_reader_loop(), 2.0)
    except Exception:
        pass
    toks = []
    while not d._frame_q.empty():
        toks.append(d._frame_q.get_nowait().hex(" "))
    return toks


def _split_req(buf: bytes) -> list:
    out, i = [], 0
    while i < len(buf):
        for n in (4, 6):
            fr = buf[i:i + n]
            if len(fr) == n and fr[0] == 0x02 and fr[-2] == 0x03:
                from v2.ch12.drivers.dcpulse_proto import chk
                if chk(fr[:-1]) == fr[-1]:
                    out.append(fr)
                    i += n
                    break
        else:
            out.append(buf[i:])
            break
    return out


async def run_case(m, name, call, args, state, pre_on) -> dict:
    ch = Chamber()
    ch.gas_sccm["ar"] = 20.0
    ch.pressure_override = 0.005
    sim = DcPulseSim(ch)
    t0 = time.monotonic()
    sent, rec = [], {"on": False, "ev": False}
    loop = asyncio.get_running_loop()

    async def handle(reader, writer):
        try:
            while True:
                data = await reader.read(256)
                if not data:
                    break
                for fr in _split_req(data):
                    now = time.monotonic() - t0
                    if rec["on"]:
                        sent.append(fr.hex(" "))
                    for t, b in sim.respond(fr, now):
                        loop.call_later(max(0.0, t - now), lambda b=b: (not writer.is_closing()) and writer.write(b))
        finally:
            writer.close()
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    d = m.AsyncDCPulse(host="127.0.0.1", port=port)
    events = []

    async def pump():
        async for ev in d.events():
            if rec["ev"] and ev.kind != "status":
                events.append([ev.kind, ev.cmd, ev.reason])
    pump_t = asyncio.create_task(pump())
    await d.start()
    await d._wait_until_connected(5.0)
    if pre_on:
        assert await d.prepare_and_start(power_w=100.0, freq=50, off_time_us=5.0)
        d.set_process_status(False)
        await asyncio.sleep(0.5)
    for k, v in state.items():
        setattr(sim, k, v)
    rec["on"] = rec["ev"] = True
    t_call = time.monotonic()
    res = await getattr(d, call)(**args)
    dur = time.monotonic() - t_call
    rec["on"] = False
    await asyncio.sleep(0.05)                             # 마지막 이벤트까지 받기
    rec["ev"] = False
    d.set_process_status(False)
    d._stop_guard = True
    d._out_on = d._output_maybe_on = False                # 정리 때 OFF 를 더 보내지 않게(기록은 이미 끝)
    await d.cleanup()
    pump_t.cancel()
    server.close()
    await server.wait_closed()
    return {"name": name, "call": call, "args": args, "sim": state, "pre_on": pre_on, "sent": sent,
            "result": res, "last_failure": d.last_failure, "events": events, "duration_s": round(dur, 2)}


def main() -> None:
    m, pl = _load_v1()
    out = {"source": "device/dc_pulse.py (v1, 실제 실행) + lib/power_limits.py", "frames": frames(m),
           "functions": functions(m, pl), "parser": parser_cases(m), "cases": []}
    for c in CASES:
        r = asyncio.run(run_case(m, *c))
        print(f"{r['name']}: {len(r['sent'])} 프레임, 결과 {r['result']}, {r['duration_s']} s", flush=True)
        out["cases"].append(r)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str) + "\n", encoding="utf-8")
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
