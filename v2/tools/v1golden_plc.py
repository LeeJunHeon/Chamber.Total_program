"""v1 PLC(device/plc.py AsyncPLC)·DC 연속(device/dc_power.py)·RF 연속(device/rf_power.py)을 실제로 실행해 골든을 만든다
→ v2/tests/golden/dev_plc.json.

실행: cd <저장소 루트> && python3 v2/tools/v1golden_plc.py      (QT_QPA_PLATFORM=offscreen 은 스크립트가 넣는다)

- v1 모듈은 importlib 로 부른다(v2 안에서 v1·pymodbus 를 import 문으로 쓰지 않는다 — test_isolation).
  v1 설정은 v1 로더(lib/_config_loader.load_settings — main.py:152 와 같이 config/settings.json 적용)로 읽은 뒤
  PLC_TCP_HOST/PORT 만 127.0.0.1 가짜 서버로, PLC_DIAG_PROBE=False(ping 금지)로 바꾼다.
- 가짜 서버: 127.0.0.1 TCP, 받은 요청을 MBAP 로 나눠(v2.common.modbus_tcp.split_request) 메모리(코일·레지스터)로 답한다.
  대본: 다음 n 개 요청 무응답(drop), 블록 읽기 짧은 응답(short). 받은 요청마다 (연결 번호, MBAP 뒤 PDU hex, 시각) 기록 —
  트랜잭션 id 는 적지 않는다(결정 D02).
- cases    : AsyncPLC 의 실제 함수(코일·레지스터 읽기/쓰기·블록·순간 스위치·고수준·전원·RF·스냅샷·하트비트 ping·
             타임아웃 정책)를 부르고 받은 PDU·결과(값·예외 코드)를 적는다.
- dc / rf  : DCPowerAsync·RFPowerAsync 를 v1 런타임 결선 그대로(runtime/chamber_runtime.py:693-786 — 같은 AsyncPLC 로
             power_write/power_read/power_enable, rf_apply/rf_write_w/rf_read_fwd_ref/rf_enable) 돌린다. 측정은 대본의
             원시 레지스터 값을 읽기 직전에 서버 메모리에 넣는다. 제어기의 asyncio.sleep·time.monotonic 만 가상 시각으로
             바꾼다(PLC I/O 는 실제 소켓·150 ms 간격). 기록: 쓰기 PDU 목록·이벤트(kind, message)·가상 시각.
v2 시험(test_plc.py·test_dc.py·test_rf.py)은 v1 을 import 하지 않고 이 골든만 읽는다.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import socket
import struct
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "dev_plc.json"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from v2.common.modbus_tcp import mbap, split_request   # noqa: E402


# ───────── 가짜 Modbus TCP 서버 ─────────
class FakePlc:
    def __init__(self) -> None:
        self.coils = {}
        self.regs = {}
        self.log = []                  # (conn, pdu_hex, t)
        self.drop = 0
        self.short = 0
        self.conn = 0
        self.t0 = time.monotonic()
        self.lock = threading.Lock()
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(8)
        self.port = self.srv.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            with self.lock:
                self.conn += 1
                cid = self.conn
            threading.Thread(target=self._serve, args=(c, cid), daemon=True).start()

    def _serve(self, c, cid) -> None:
        buf = b""
        while True:
            try:
                d = c.recv(4096)
            except OSError:
                break
            if not d:
                break
            buf += d
            while True:
                got = split_request(buf)
                if got is None:
                    break
                tid, unit, pdu, buf = got
                with self.lock:
                    self.log.append((cid, pdu.hex(), round(time.monotonic() - self.t0, 3)))
                    if self.drop > 0:
                        self.drop -= 1
                        continue
                    body = self._answer(pdu)
                try:
                    c.sendall(mbap(tid, unit, body))
                except OSError:
                    break
        c.close()

    def _answer(self, pdu: bytes) -> bytes:
        fc = pdu[0]
        if fc in (1, 3):
            addr, n = struct.unpack(">HH", pdu[1:5])
            if self.short > 0:
                self.short -= 1
                n = max(1, n // 2)
            if fc == 1:
                body = bytearray((n + 7) // 8)
                for i in range(n):
                    if self.coils.get(addr + i):
                        body[i // 8] |= 1 << (i % 8)
                return bytes([1, len(body)]) + bytes(body)
            return bytes([3, 2 * n]) + b"".join(struct.pack(">H", self.regs.get(addr + i, 0) & 0xFFFF) for i in range(n))
        if fc == 5:
            addr, v = struct.unpack(">HH", pdu[1:5])
            self.coils[addr] = v == 0xFF00
            return pdu[:5]
        if fc == 6:
            addr, v = struct.unpack(">HH", pdu[1:5])
            self.regs[addr] = v
            return pdu[:5]
        return bytes([fc | 0x80, 1])

    def take(self):
        with self.lock:
            out, self.log = self.log, []
        return out


def _load_v1(port: int):
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    importlib.import_module("lib._config_loader").load_settings()
    cc = importlib.import_module("lib.config_common")
    cc.PLC_TCP_HOST, cc.PLC_TCP_PORT, cc.PLC_DIAG_PROBE = "127.0.0.1", port, False
    return cc, importlib.import_module("device.plc"), importlib.import_module("device.dc_power"), \
        importlib.import_module("device.rf_power")


def _res(v):
    if isinstance(v, tuple):
        return [_res(x) for x in v]
    if isinstance(v, float):
        return round(v, 6)
    return v


async def _plc_cases(P, srv: FakePlc, cc):
    logs = []
    plc = P.AsyncPLC(logger=lambda fmt, *a: logs.append(fmt % a if a else fmt))
    await plc.connect()
    await plc.pause_watchdog()
    srv.take()
    srv.regs.update({0: 1000, 1: 500, 8: 700, 9: 40, 2: 600, 3: 100, 10: 2000, 11: 1027, 4: 1200})
    for a in (203, 234, 640, 642, 1600, 2400, 3040, 803, 809):
        srv.coils[a] = True
    cases = []

    async def run(name, args, coro_fn, short=0, drop=0):
        if os.environ.get("GOLDEN_VERBOSE"):
            print("case", name, args, flush=True)
        srv.take()
        srv.short, srv.drop = short, drop
        t0 = round(time.monotonic() - srv.t0, 3)
        try:
            out = {"ok": True, "value": _res(await coro_fn())}
        except Exception as e:
            out = {"ok": False, "code": getattr(e, "code", type(e).__name__), "error": str(e)[:160]}
        frames = srv.take()
        cases.append(dict(name=name, args=args, **out,
                          pdus=[f[1] for f in frames], conns=[f[0] for f in frames],
                          t=[round(f[2] - t0, 3) for f in frames]))
        srv.short = srv.drop = 0

    await run("read_coil", [203], lambda: plc.read_coil(203))
    await run("write_coil", [234, True], lambda: plc.write_coil(234, True))
    await run("write_coil", [234, False], lambda: plc.write_coil(234, False))
    await run("read_reg", [8], lambda: plc.read_reg(8))
    await run("write_reg", [6, 17], lambda: plc.write_reg(6, 17))
    await run("read_coils_block", [1, 481], lambda: plc.read_coils_block(1, 481))
    await run("read_coils_block", [640, 19], lambda: plc.read_coils_block(640, 19))
    await run("read_regs_block", [0, 12], lambda: plc.read_regs_block(0, 12))
    await run("read_regs_block", [110, 51], lambda: plc.read_regs_block(110, 51))
    await run("read_coils_block_short", [1, 481], lambda: plc.read_coils_block(1, 481), short=1)
    await run("read_regs_block_short", [0, 12], lambda: plc.read_regs_block(0, 12), short=1)
    await run("write_switch", ["MAIN_SHUTTER_2_SW", True], lambda: plc.write_switch("MAIN_SHUTTER_2_SW", True))
    await run("write_switch", ["AR_2_GAS_SW", False], lambda: plc.write_switch("AR_2_GAS_SW", False))
    await run("press_switch", ["G_V_2_OPEN_SW"], lambda: plc.press_switch("G_V_2_OPEN_SW"))
    await run("pulse", [210, 300], lambda: plc.pulse(210, ms=300))
    await run("read_bit", ["L_ATM"], lambda: plc.read_bit("L_ATM"))
    await run("read_bit", ["L_ATM_SENSOR"], lambda: plc.read_bit("L_ATM_SENSOR"))
    await run("write_coil_p_mirror", [803, True], lambda: plc.write_coil(803, True))
    await run("gas", [2, "AR", True], lambda: plc.gas(2, "AR", on=True))
    await run("gas", [1, "O2", False], lambda: plc.gas(1, "O2", on=False))
    await run("gas", [2, "N2", True], lambda: plc.gas(2, "N2", on=True))
    await run("gas", [1, "N2", True], lambda: plc.gas(1, "N2", on=True))
    await run("gas", [2, "MAIN", False], lambda: plc.gas(2, "MAIN", on=False))
    await run("main_shutter", [1, True], lambda: plc.main_shutter(1, open=True))
    await run("gate_valve", [2, False, True], lambda: plc.gate_valve(2, open=False, momentary=True))
    await run("lift_pin", [True, True], lambda: plc.lift_pin(up=True, momentary=True))
    await run("rf_select", [True], lambda: plc.rf_select(rf_mode=True))
    await run("power_select", [True], lambda: plc.power_select(on=True))
    await run("power_select", [False], lambda: plc.power_select(on=False))
    await run("buzzer_stop", [True], lambda: plc.buzzer_stop(momentary=True))
    await run("set", ["MV", True, 2], lambda: plc.set("MV", True, ch=2))
    await run("set", ["MS", False, 1], lambda: plc.set("MS", False, ch=1))
    await run("set", ["G2", True, 2], lambda: plc.set("G2", True, ch=2))
    await run("set", ["SW_RF_SELECT", False, 2], lambda: plc.set("SW_RF_SELECT", False, ch=2))
    await run("power_enable", [True, 0], lambda: plc.power_enable(True, family="DCV", set_idx=0))
    await run("power_enable", [False, 3], lambda: plc.power_enable(False, family="DCV", set_idx=3))
    for w in (0.0, 0.1, 0.13, 1.0, 123.4, 300.6, 1000.0, 1500.0, -5.0):
        await run("power_write", [w, 0], lambda w=w: plc.power_write(w, family="DCV", write_idx=0))
    await run("power_write", [500.0, 3], lambda: plc.power_write(500.0, family="DCV", write_idx=3))
    await run("power_read", [0, 1], lambda: plc.power_read(family="DCV", v_idx=0, i_idx=1))
    await run("power_read", [6, 7], lambda: plc.power_read(family="DCV", v_idx=6, i_idx=7))
    await run("rf_enable", [True, 2], lambda: plc.rf_enable(True, rf_ch=2))
    await run("rf_enable", [False, 1], lambda: plc.rf_enable(False, rf_ch=1))
    await run("rf_write_w", [4.2869, 2], lambda: plc.rf_write_w(4.2869, rf_ch=2))
    await run("rf_apply", [100.0, False, 2], lambda: plc.rf_apply(100.0, ensure_set=False, rf_ch=2))
    await run("rf_apply", [50.0, True, 2], lambda: plc.rf_apply(50.0, ensure_set=True, rf_ch=2))
    await run("rf_apply", [10.0, False, 1], lambda: plc.rf_apply(10.0, ensure_set=False, rf_ch=1))
    await run("rf_read_fwd_ref", [2, False], lambda: plc.rf_read_fwd_ref(rf_ch=2, zeroing=False))
    await run("rf_read_fwd_ref", [2, None], lambda: plc.rf_read_fwd_ref(rf_ch=2))
    await run("rf_read_fwd_ref", [1, None], lambda: plc.rf_read_fwd_ref(rf_ch=1))
    await run("snapshot_all_coils_fast", [], lambda: plc.snapshot_all_coils_fast())
    await run("snapshot_regs_fast", [], lambda: plc.snapshot_regs_fast())

    async def hb():
        async with plc._io_lock("heartbeat", addr=0):
            await plc._run_io("heartbeat", 0, "read_coils", 0, count=1)
        return True
    await run("heartbeat_ping", [], hb)
    # 타임아웃 정책(무응답 대본) — 응답 제한 PLC_TIMEOUT_S 2.0 s 그대로
    await run("timeout_once_retry_same_socket", [8], lambda: plc.read_reg(8), drop=1)
    await run("timeout_two_fail_same_socket", [8], lambda: plc.read_reg(8), drop=2)
    await run("timeout_third_recreates_socket", [8], lambda: plc.read_reg(8), drop=1)
    await run("low_priority_no_retry", [0, 12], lambda: plc.read_regs_block(0, 12, priority="low"), drop=1)
    await plc.close()
    return dict(plan_idx=plc._coil_plan_idx, cases=cases, logs=logs[:200],
                cfg=dict(PLC_COIL_LOG_BLOCK_PLAN=cc.PLC_COIL_LOG_BLOCK_PLAN, PLC_CMD_GAP_MS=cc.PLC_CMD_GAP_MS,
                         PLC_CMD_PULSE_MS=cc.PLC_CMD_PULSE_MS))


# ───────── DC·RF 제어기(가상 시각) ─────────
class VClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps = []


def _shim(mod, vc: VClock):
    real = asyncio

    class _A:
        def __getattr__(self, n):
            return getattr(real, n)

        @staticmethod
        async def sleep(d, *a, **k):
            vc.now += float(d)
            vc.sleeps.append(round(float(d), 3))
            await real.sleep(0)

    class _T:
        @staticmethod
        def monotonic():
            return vc.now

    mod.asyncio = _A()
    mod.time = _T()


async def _drain(q):
    out = []
    while not q.empty():
        e = q.get_nowait()
        out.append([e.kind, e.message] if e.kind not in ("display", "state_changed") else
                   [e.kind, getattr(e, "running", None)])
    return out


async def _controller(P, mod, srv: FakePlc, kind: str, scen: dict):
    if os.environ.get("GOLDEN_VERBOSE"):
        print("scenario", scen["name"], flush=True)
    vc = VClock()
    _shim(mod, vc)
    plc = P.AsyncPLC(logger=lambda *a: None)
    await plc.connect()
    await plc.pause_watchdog()
    srv.take()
    srv.regs.clear()
    srv.coils.clear()
    script = list(scen["meas"])
    reads = {"n": 0}
    exhausted = asyncio.Event()
    marks = []

    def put(m):
        if kind == "dc":
            srv.regs[scen.get("v_reg", 0)], srv.regs[scen.get("i_reg", 1)] = m
        else:
            srv.regs[8], srv.regs[9] = m

    if kind == "dc":
        widx, sidx = scen.get("idx", 0), scen.get("idx", 0)

        async def send(w):
            await plc.power_write(w, family="DCV", write_idx=widx)

        async def read():
            if reads["n"] >= len(script):
                exhausted.set()
                return None
            put(script[reads["n"]])
            reads["n"] += 1
            marks.append(len(srv.log))
            P_, V, I = await plc.power_read(family="DCV", v_idx=scen.get("v_idx", 0), i_idx=scen.get("i_idx", 1))
            return (P_, V, I)

        async def toggle(on):
            await plc.power_enable(on, family="DCV", set_idx=sidx)
        ctl = mod.DCPowerAsync(send_dc_power=send, send_dc_power_unverified=send, request_status_read=read,
                               toggle_enable=toggle, name="DC1")
    else:
        async def send(w):
            await plc.rf_apply(float(w), ensure_set=False, rf_ch=2)

        async def unv(w):
            await plc.rf_write_w(float(w), rf_ch=2)

        async def read():
            if reads["n"] >= len(script):
                exhausted.set()
                return None
            put(script[reads["n"]])
            reads["n"] += 1
            marks.append(len(srv.log))
            return await plc.rf_read_fwd_ref(rf_ch=2, zeroing=False)

        async def toggle(on):
            await plc.rf_enable(bool(on), rf_ch=2)
        ctl = mod.RFPowerAsync(send_rf_power=send, send_rf_power_unverified=unv, request_status_read=read,
                               toggle_enable=toggle, poll_interval_ms=1000, rampdown_interval_ms=50,
                               direct_mode=False, write_inv_a=1.6546, write_inv_b=2.6323)
    await ctl.start_process(scen["target"])
    t_end = time.monotonic() + 120.0
    while not exhausted.is_set() and ctl._is_running and time.monotonic() < t_end:
        await asyncio.sleep(0.01)
    stopped_by_trip = not ctl._is_running
    if not stopped_by_trip:
        # 마지막 측정의 보정 쓰기가 끝난 뒤 정지(대본 끝을 정지와 겹치지 않게 — 공정이 정지를 부르는 시각은 임의)
        for _ in range(500):
            t = ctl._adjust_task
            if t is None or t.done():
                break
            await asyncio.sleep(0.01)
        await ctl.cleanup()
    for _ in range(4000):
        await asyncio.sleep(0.01)
        rd = ctl._rampdown_task
        if rd is not None and rd.done():
            break
    await asyncio.sleep(0.2)
    log = srv.take()
    await plc.close()
    writes = [p for _, p, _ in log if p[:2] in ("05", "06")]
    return dict(name=scen["name"], target=scen["target"], meas=script, reads_used=reads["n"], tripped=stopped_by_trip,
                writes=writes, events=await _drain(ctl._event_q), sleeps=vc.sleeps[:400])


DC_SCEN = [
    dict(name="dc_reach_then_stop", target=300.0,
         meas=[(0, 0), (1500, 400), (1720, 400), (1730, 400), (1731, 400), (1731, 400)]),
    dict(name="dc_low_power_trip", target=200.0, meas=[(0, 0), (0, 0), (0, 0), (0, 0)]),
    dict(name="dc_overcurrent_trip", target=200.0, meas=[(900, 1200), (900, 1200)]),
    dict(name="dc_low_current_after_reach", target=100.0,
         meas=[(1000, 205), (1000, 205), (1000, 40), (1000, 40), (1000, 40), (1000, 40)]),
]
# CH2 RF 원시값: fwd = 0.15059·raw − 0.598, ref = 0.12940·raw − 0.267
RF_SCEN = [
    dict(name="rf_ramp_5w", target=5.0,
         meas=[(0, 0), (0, 0), (14, 3), (21, 3), (27, 3), (34, 3), (37, 3), (37, 3), (30, 3), (30, 3), (37, 3)]),
    dict(name="rf_low_power_trip", target=20.0, meas=[(0, 0), (10, 2), (10, 2), (10, 2), (10, 2)]),
    dict(name="rf_ref_wait_recover", target=10.0,
         meas=[(14, 3), (21, 3), (27, 3), (34, 500), (34, 500), (40, 3), (47, 3), (60, 3), (70, 3), (70, 3)]),
    dict(name="rf_blind_settle_early_exit", target=80.0,
         meas=[(470, 200), (480, 200), (470, 5), (470, 5), (480, 5), (540, 5), (544, 5)]),
]


async def _all():
    srv = FakePlc()
    cc, P, DCM, RFM = _load_v1(srv.port)
    plc = await _plc_cases(P, srv, cc)
    dc = [await _controller(P, DCM, srv, "dc", s) for s in DC_SCEN]
    rf = [await _controller(P, RFM, srv, "rf", s) for s in RF_SCEN]
    return dict(source="v1 device/plc.py·dc_power.py·rf_power.py 실제 실행(pymodbus ModbusTcpClient → 127.0.0.1 가짜 서버)",
                pymodbus=importlib.import_module("pymodbus").__version__, plc=plc, dc=dc, rf=rf,
                rf_cfg=dict(RF_REFLECTED_THRESHOLD_W=cc.RF_REFLECTED_THRESHOLD_W,
                            RF_BLIND_RAMP_REACH_TIMEOUT_S=cc.RF_BLIND_RAMP_REACH_TIMEOUT_S,
                            CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS=cc.CHAMBER_RF_CONT_RAMPDOWN_INTERVAL_MS))


def main() -> None:
    data = asyncio.run(_all())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"골든 {OUT} — PLC {len(data['plc']['cases'])}건, DC {len(data['dc'])}건, RF {len(data['rf'])}건")


if __name__ == "__main__":
    main()
