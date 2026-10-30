"""v1 RF Pulse 드라이버(device/rf_pulse.py)를 실제로 실행해 골든을 만든다 → v2/tests/golden/dev_rfpulse.json.

실행: cd <저장소 루트> && python3 v2/tools/v1golden_rfpulse.py      (실제 시간으로 돈다 — CMD_GAP_MS 1.5 s 라 약 4분)

- v1 모듈은 importlib 로 동적으로 부른다(v2 안에서 v1 을 import 문으로 쓰지 않는다 — test_isolation).
  v1 설정은 lib/_config_loader.load_settings() 로 config/settings.json 을 적용한 운영값.
- frames   : v1 _build_packet(addr, cmd, data) 바이트(명령 이름·인자·hex).
- functions: v1 rfp_duty_limits_pct·_parse_status_0xA2.
- cases    : v1 RFPulseAsync 를 127.0.0.1 가짜 TCP 서버에 붙여 start_pulse_process·set_reference_power·stop_process·
             stop_unused·read_actual_pulse_params 를 실제로 부르고, 서버가 받은 프레임(순서대로)·결과·이벤트를 적는다.
             가짜 장비는 v2/ch12/sim/rfpulse_sim.RfPulseSim(경우마다 정한 초기 상태·csr_once) — v2 시험도 같은 시뮬레이터·같은
             상태로 돌려 보낸 바이트를 비교한다.
"""
from __future__ import annotations

import asyncio
import copy
import importlib
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_rfpulse.json"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from v2.ch12.sim.chamber import Chamber            # noqa: E402
from v2.ch12.sim.rfpulse_sim import RfPulseSim     # noqa: E402

# (이름, 부를 함수, 인자, 시뮬레이터 초기 상태, 먼저 RF 를 켜 둘지)
CASES = [
    ("start_not_host", "start_pulse_process", dict(target_w=100.0, freq_hz=1000, duty_percent=50), {"control": 6}, False),
    ("start_host_plain", "start_pulse_process", dict(target_w=150.0), {"control": 2}, False),
    ("start_csr1_setpoint", "start_pulse_process", dict(target_w=120.0), {"control": 2, "csr_once": {8: 1}}, False),
    ("start_csr2_mode", "start_pulse_process", dict(target_w=120.0), {"control": 2, "csr_once": {3: 2}}, False),
    ("start_freq_csr50_duty_first", "start_pulse_process", dict(target_w=100.0, freq_hz=20000, duty_percent=50),
     {"control": 2, "csr_once": {93: 50}}, False),
    ("start_freq_csr50_retry_ok", "start_pulse_process", dict(target_w=100.0, freq_hz=2000), {"control": 2, "csr_once": {93: 50}}, False),
    ("start_readback_mismatch", "start_pulse_process", dict(target_w=100.0, freq_hz=5000, duty_percent=40),
     {"control": 2, "readback_offset": 7}, False),
    ("set_reference_power", "set_reference_power", dict(target_w=200.0), {"control": 2}, False),
    ("set_reference_power_csr1", "set_reference_power", dict(target_w=210.0), {"control": 6}, False),
    ("stop_after_on", "stop_process", {}, {}, True),
    ("stop_csr7_status", "stop_process", {}, {"csr_once": {1: [7, 7, 7]}}, True),
    ("stop_unused", "stop_unused", {}, {"control": 2}, False),
    ("read_actual_pulse_params", "read_actual_pulse_params", {}, {"control": 2, "freq": 2500, "duty": 30}, False),
]


def _load_v1():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    importlib.import_module("lib._config_loader").load_settings()
    return importlib.import_module("device.rf_pulse")


def frames(m) -> list:
    out = []
    for name, cmd, data in (("RF_OFF", 1, b""), ("RF_ON", 2, b""), ("MODE_FWD", 3, b"\x06"), ("MODE_LOAD", 3, b"\x07"),
                            ("SETPOINT_100", 8, bytes([100, 0])), ("SETPOINT_300", 8, bytes([0x2C, 0x01])),
                            ("HOST", 14, b"\x02"), ("PULSING_1", 27, b"\x01"), ("PULSING_0", 27, b"\x00"),
                            ("FREQ_1000", 93, bytes([0xE8, 0x03, 0x00])), ("FREQ_30000", 93, bytes([0x30, 0x75, 0x00])),
                            ("DUTY_50", 96, bytes([50, 0])), ("Q155", 155, b""), ("Q162", 162, b""), ("Q165", 165, b""),
                            ("Q166", 166, b""), ("Q193", 193, b""), ("Q196", 196, b""), ("LONG7", 200, bytes(range(7)))):
        for addr in (1, 2, 31):
            out.append({"name": name, "addr": addr, "cmd": cmd, "data": data.hex(" "), "hex": m._build_packet(addr, cmd, data).hex(" ")})
    return out


def functions(m) -> dict:
    d = m.RFPulseAsync()
    st = []
    for raw in ("20 00 00 00", "60 80 00 00", "e0 08 00 a1", "00 00 00 00", "20 00 00"):
        s = d._parse_status_0xA2(bytes.fromhex(raw))
        st.append([raw, None if s is None else [s.rf_output_on, s.rf_on_requested, s.setpoint_mismatch, s.interlock_open,
                                                 s.overtemp, s.current_limit, s.extended_fault, s.cex_lock]])
    return {"duty_limits": [[k, list(m.rfp_duty_limits_pct(k))] for k in (0.001, 1, 5, 10, 20, 30)], "status": st}


def _split(buf: bytes) -> list:
    out, i = [], 0
    while i < len(buf):
        n = buf[i] & 0x07
        total = (4 + buf[i + 2]) if n == 7 else 3 + n
        out.append(buf[i:i + total])
        i += total
    return out


async def run_case(m, name, call, args, state, pre_on) -> dict:
    ch = Chamber()
    sim = RfPulseSim(ch, control=2)
    t0 = time.monotonic()
    sent, rec = [], {"on": False, "ev": False}
    loop = asyncio.get_running_loop()

    async def handle(reader, writer):
        try:
            while True:
                data = await reader.read(256)
                if not data:
                    break
                for fr in _split(data):
                    now = time.monotonic() - t0
                    if rec["on"]:
                        sent.append(fr.hex(" "))
                    for t, b in sim.respond(fr, now):
                        loop.call_later(max(0.0, t - now), lambda b=b: (not writer.is_closing()) and writer.write(b))
        finally:
            writer.close()
    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    d = m.RFPulseAsync()
    d.set_endpoint("127.0.0.1", port)
    events = []

    async def pump():
        async for ev in d.events():
            if rec["ev"] and ev.kind not in ("status", "rf_status", "power"):
                events.append([ev.kind, ev.cmd, ev.reason])
    pump_t = asyncio.create_task(pump())
    await d.start()
    for _ in range(100):
        if d.is_connected():
            break
        await asyncio.sleep(0.05)
    if pre_on:
        assert await d.start_pulse_process(100.0)
        d.set_process_status(False)
        await asyncio.sleep(2.0)
    rec_state = {k: (sorted(v.items()) if isinstance(v, dict) else v) for k, v in copy.deepcopy(state).items()}
    for k, v in copy.deepcopy(state).items():
        setattr(sim, k, v)
    rec["on"] = rec["ev"] = True
    t_call = time.monotonic()
    res = getattr(d, call)(**args)
    if asyncio.iscoroutine(res):
        res = await res
    else:                                                # stop_process·stop_unused: OFF 태스크가 끝날 때까지
        t = d._off_task
        if t is not None:
            await asyncio.wait({t}, timeout=30.0)
    dur = time.monotonic() - t_call
    rec["on"] = False
    await asyncio.sleep(0.05)
    rec["ev"] = False
    d.set_process_status(False)
    d._output_maybe_on = False                            # 정리 때 OFF 를 더 보내지 않게(기록은 이미 끝)
    await d.cleanup()
    pump_t.cancel()
    server.close()
    await server.wait_closed()
    return {"name": name, "call": call, "args": args, "sim": rec_state,
            "pre_on": pre_on, "sent": sent, "result": res, "events": events, "duration_s": round(dur, 2)}


def main() -> None:
    m = _load_v1()
    out = {"source": "device/rf_pulse.py (v1, 실제 실행)", "frames": frames(m), "functions": functions(m), "cases": []}
    for c in CASES:
        r = asyncio.run(run_case(m, *c))
        print(f"{r['name']}: {len(r['sent'])} 프레임, 결과 {r['result']}, {r['duration_s']} s", flush=True)
        out["cases"].append(r)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str) + "\n", encoding="utf-8")
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
