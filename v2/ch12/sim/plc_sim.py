"""챔버12 PLC 시뮬레이터 — Modbus TCP 슬레이브(MBAP, v2.common.modbus_tcp.split_request) + 래더 동작 모델.

통신: 유닛 id 가 다르면 응답하지 않는다. 함수 1·3·5·6·15·16. 주소가 메모리(코일 COIL_N·레지스터 REG_N) 또는 주입한
  주소 창(coil_limit — 블록 계획 강등 시험) 밖이면 예외 2(→ 드라이버 E403), 모르는 함수 예외 1, 값 형식 예외 3.
  응답 지연 latency_s(가정 3 ms). 고장(SimDevice): no_response·delay·disconnect·garbage + 이 장비 고유:
  short(블록 읽기 응답을 반으로 — 짧은 응답), exception(모든 요청에 예외 4), stale(앞 트랜잭션 id 의 늦은 응답을 먼저 보냄),
  truncate(응답 앞 절반만 — 무응답처럼 보임). drop_next = 다음 n 개 요청 무응답, forced_regs = 레지스터 값 고정(측정 대본),
  coil_limit = 코일 주소 창. restart() = 메모리 초기화(PLC 재기동).
래더 모델(0.1 s 단계, 요청이 올 때와 advance_to 때 — 값·시간은 가정값):
  - 밸브·펌프: CHn 의 R_P·R_V·F_V·M_V·VENT·TURBO·MAIN_SHUTTER·가스(Ar·O2·N2·MAIN)·건 셔터(G1~G3, CH2) 코일 = 출력.
    해당 '인터락' 코일이 False 면 출력 꺼짐. 공용 챔버 모델(v2/ch12/sim/chamber.py, 챔버마다 하나)에 밸브 상태를 넣는다.
  - 게이트(G_V_n_OPEN/CLOSE_SW 상승 에지) → GATE_MOVE_S 뒤 OPEN/CLOSE 램프. 도어 DOOR_MOVE_S. 4핀(L_PIN_UP/DOWN_SW) PIN_MOVE_S.
  - 척(Z_M_P_n_SW 전원 + CW=위 / CCW=아래 / MID_SW=가운데): 끝에서 끝 CHUCK_TRAVEL_S, 위치 램프는 센서 위치에
    CHUCK_LAMP_HOLD_S(래더 T0004~T0006 3 s) 머문 뒤 점등, 움직이는 중·그 전엔 모두 꺼짐(host/handlers.py:2244-2248).
  - 로드락: L_R_P+L_R_V → LL_ROUGH_S 뒤 L_GAUGE_A(P 미러 803)·L_GAUGE_A_INTERLOCK·LP_STEP1 → LP_STEP1_S(60 s) 뒤 L_R_V OFF·
    LP_STEP2 → LP_STEP2_S(3 s) 뒤 L_R_P OFF·L_VAC_READY_SW(host/handlers.py:1228-1230 문서 순서). LL_NOT_READY_S(T0052 600 s)
    안에 못 닿으면 L_VAC_NOT_READY. L_VENT_SW → LL_VENT_S 뒤 L_ATM·L_ATM_SENSOR(809), 진공 준비 해제. P 미러 848/849/852 = 출력.
  - 전원(PLC D/A·A/D): SET_n 래치 ∧ D(4+n) > 0 → 출력 W = 코드/4(1 W = 4 카운트), 1차 지연 POWER_TAU_S.
    CH2 DC1(SET_0·D4 → D0 V·D1 I), CH2 DC2(SET_3·D7 → D10·D11): 점화(chamber.can_ignite) 때만 P = 출력, V = √(P·DC_LOAD_OHM),
    raw = V/0.50316·I/0.00097405. CH2 RF(SET_2·D6 → D8·D9): 발생기 fwd = (W − 2.6323)/1.6546(역변환이 맞다고 가정),
    ref = fwd × (점화 RF_REF_RATIO_LIT, 아니면 RF_REF_RATIO_DARK) + ref_inject_w, raw = (W − b)/a(CH2 계수).
    PC RF(SET_1·D5 → D2·D3): fwd = W/1.74, CH1 계수·제로 보정의 역. ch2 챔버의 dc_set_w·rf_set_w 에 넣는다.
  - CH2 히터: plc_ladder.HeaterLadder(v1 tests/heater_ladder_sim.py 근사, D114 30 s 무변화 → WD_ERR). 온도는 ch2.temp_c 로.
"""
from __future__ import annotations

import math
import struct
from typing import Dict, List, Optional

from v2.common.modbus_rtu import READ_COILS, READ_REGS, WRITE_COIL, WRITE_COILS, WRITE_REG, WRITE_REGS
from v2.common.modbus_tcp import mbap, split_request
from v2.common.simdev import SimDevice, sim_log
from v2.ch12.drivers import plc_map as PM
from v2.ch12.sim.chamber import Chamber
from v2.ch12.sim.plc_ladder import HeaterLadder

C = PM.COIL_MAP
COIL_N, REG_N = 4096, 512
GATE_MOVE_S = 3.0
DOOR_MOVE_S = 2.0
PIN_MOVE_S = 2.0
CHUCK_TRAVEL_S = 10.0
CHUCK_LAMP_HOLD_S = 3.0
LL_ROUGH_S = 30.0
LP_STEP1_S = 60.0
LP_STEP2_S = 3.0
LL_NOT_READY_S = 600.0
LL_VENT_S = 20.0
POWER_TAU_S = 0.5
DC_LOAD_OHM = 400.0
RF_REF_RATIO_LIT = 0.01
RF_REF_RATIO_DARK = 0.3
VALVE_KEYS = (("R_P", "rotary"), ("R_V", "rv"), ("F_V", "fv"), ("M_V", "mv"), ("VENT", "vent"), ("TURBO", "turbo"))


class PlcSim(SimDevice):
    def __init__(self, ch1: Optional[Chamber] = None, ch2: Optional[Chamber] = None, unit: int = 1,
                 latency_s: float = 0.003, start: float = 0.0) -> None:
        super().__init__("plc", latency_s=latency_s)
        self.unit = unit
        self.t = start
        self.ch = {1: ch1 or Chamber("ch1", start), 2: ch2 or Chamber("ch2", start)}
        self.frames = sim_log()
        self.coil_limit: Optional[int] = None         # 주입: 이 주소 이상 코일 읽기 → 예외 2
        self.ref_inject_w = 0.0
        self.dc_fail = False                          # 주입: DC 출력이 나오지 않음(점화 실패와 별개)
        self.loading = {1: False, 2: False}
        self.forced_regs: Dict[int, int] = {}         # 주입: 래더 출력보다 우선하는 레지스터 값(측정 대본)
        self.drop_next = 0                            # 주입: 다음 n 개 요청 무응답(횟수 기준 — 타임아웃 정책 시험)
        self._buf = b""
        self._init_memory()

    # ───────── 메모리 ─────────
    def _init_memory(self) -> None:
        self.coils: List[bool] = [False] * COIL_N
        self.regs: List[int] = [0] * REG_N
        for n, a in C.items():
            if "인터락" in n or (n.endswith("_INTERLOCK") and not n.startswith("L_GAUGE")):
                self.coils[a] = True                  # 인터락 허용·챔버 게이지 진공 상태로 시작
        for ch in (1, 2):
            for k in ("R_P", "F_V", "M_V", "TURBO"):
                self.coils[C[f"{k}_{ch}_SW"]] = True
            self.coils[C[f"G_V_{ch}_CLOSE_LAMP"]] = True
            self.coils[C[f"DOOR_{ch}_CLOSE_LAMP"]] = True
        self.coils[C["L_PIN_DOWN_LAMP"]] = True
        self.coils[C["L_ATM"]] = True
        self.gate = {1: 0.0, 2: 0.0}                  # 0 닫힘 … 1 열림
        self.gate_dir = {1: 0, 2: 0}
        self.door = {1: 0.0, 2: 0.0}
        self.door_dir = {1: 0, 2: 0}
        self.pin, self.pin_dir = 0.0, 0
        self.chuck = {1: 0.0, 2: 0.0}                 # 0 아래 … 0.5 가운데 … 1 위
        self.chuck_at = {1: (0.0, self.t - CHUCK_LAMP_HOLD_S), 2: (0.0, self.t - CHUCK_LAMP_HOLD_S)}
        self.chuck_mid_target = {1: False, 2: False}
        self.ll = dict(atm=True, rough_t=None, step1_t=None, step2_t=None, vent_t=None, start_t=None)
        self.out_w = {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0}  # D/A 채널 0~3 실제 출력(W)
        self.heater = HeaterLadder(self.regs, self.coils)
        self._edge_prev: Dict[int, bool] = {}
        self._outputs()

    def restart(self) -> None:
        self._init_memory()

    # ───────── 시각 ─────────
    def advance_to(self, now: float) -> None:
        while self.t < now - 1e-12:
            dt = min(0.1, now - self.t)
            self._step(dt)
            self.t += dt
        for c in self.ch.values():
            c.advance_to(self.t)

    def _rise(self, addr: int) -> bool:
        v = self.coils[addr]
        prev = self._edge_prev.get(addr, False)
        self._edge_prev[addr] = v
        return v and not prev

    @staticmethod
    def _move(pos: float, d: int, dt: float, span_s: float) -> float:
        return min(1.0, max(0.0, pos + d * dt / span_s))

    def _step(self, dt: float) -> None:
        c = self.coils
        for ch in (1, 2):
            if self._rise(C[f"G_V_{ch}_OPEN_SW"]) and c[C[f"G_V_{ch}_인터락"]]:
                self.gate_dir[ch] = 1
            if self._rise(C[f"G_V_{ch}_CLOSE_SW"]):
                self.gate_dir[ch] = -1
            self.gate[ch] = self._move(self.gate[ch], self.gate_dir[ch], dt, GATE_MOVE_S)
            if self._rise(C[f"DOOR_{ch}_OPEN_SW"]) and c[C[f"DOOR_{ch}_인터락"]]:
                self.door_dir[ch] = 1
            if self._rise(C[f"DOOR_{ch}_CLOSE_SW"]):
                self.door_dir[ch] = -1
            self.door[ch] = self._move(self.door[ch], self.door_dir[ch], dt, DOOR_MOVE_S)
            self._chuck_step(ch, dt)
        if self._rise(C["L_PIN_UP_SW"]) and c[C["L_PIN_인터락"]]:
            self.pin_dir = 1
        if self._rise(C["L_PIN_DOWN_SW"]) and c[C["L_PIN_인터락"]]:
            self.pin_dir = -1
        self.pin = self._move(self.pin, self.pin_dir, dt, PIN_MOVE_S)
        self._loadlock_step()
        for i in range(4):
            on = c[C[f"DCV_SET_{i}"]] and self.regs[PM.REG_MAP[f"DCV_WRITE_{i}"]] > 0
            tgt = self.regs[PM.REG_MAP[f"DCV_WRITE_{i}"]] / 4.0 if on else 0.0
            if dt > 0:
                self.out_w[i] += (tgt - self.out_w[i]) * min(1.0, dt / POWER_TAU_S)
        self.heater.scan(dt)
        self._outputs()
        for chm in self.ch.values():
            chm.advance_to(self.t + dt)
        self.ch[2].temp_c = self.heater.T

    def _chuck_step(self, ch: int, dt: float) -> None:
        c = self.coils
        pwr = c[C[f"Z_M_P_{ch}_SW"]]
        d = 0
        if pwr and c[C[f"Z_M_P_{ch}_CW_SW"]]:
            d = 1
        elif pwr and c[C[f"Z_M_P_{ch}_CCW_SW"]]:
            d = -1
        elif pwr and c[C[f"Z_M_P_{ch}_MID_SW"]]:
            p = self.chuck[ch]
            d = 0 if abs(p - 0.5) < 1e-6 else (1 if p < 0.5 else -1)
            self.chuck_mid_target[ch] = True
        old = self.chuck[ch]
        new = self._move(old, d, dt, CHUCK_TRAVEL_S)
        if d and self.chuck_mid_target[ch] and (old - 0.5) * (new - 0.5) <= 0:
            new = 0.5
        if not c[C[f"Z_M_P_{ch}_MID_SW"]]:
            self.chuck_mid_target[ch] = False
        if new != old:
            self.chuck_at[ch] = (new, self.t)
        self.chuck[ch] = new

    def _loadlock_step(self) -> None:
        c, ll, t = self.coils, self.ll, self.t
        rough = c[C["L_R_P_SW"]] and c[C["L_R_V_SW"]]
        if c[C["L_VENT_SW"]]:
            if ll["vent_t"] is None:
                ll["vent_t"] = t
            ll["rough_t"] = ll["step1_t"] = ll["step2_t"] = ll["start_t"] = None
            c[C["L_VAC_READY_SW"]] = False
            if t - ll["vent_t"] >= LL_VENT_S:
                ll["atm"] = True
        else:
            ll["vent_t"] = None
        if rough and ll["rough_t"] is None and ll["step1_t"] is None:
            ll["rough_t"] = t
            ll["start_t"] = ll["start_t"] if ll["start_t"] is not None else t
        if ll["rough_t"] is not None and t - ll["rough_t"] >= LL_ROUGH_S and ll["step1_t"] is None:
            ll["atm"] = False
            ll["step1_t"] = t
        if ll["step1_t"] is not None and ll["step2_t"] is None and t - ll["step1_t"] >= LP_STEP1_S:
            c[C["L_R_V_SW"]] = False
            ll["step2_t"] = t
        if ll["step2_t"] is not None and t - ll["step2_t"] >= LP_STEP2_S:
            c[C["L_R_P_SW"]] = False
            c[C["L_VAC_READY_SW"]] = True
            ll["rough_t"] = ll["step1_t"] = ll["step2_t"] = ll["start_t"] = None
        c[C["LP_STEP1"]] = ll["step1_t"] is not None and ll["step2_t"] is None
        c[C["LP_STEP2"]] = ll["step2_t"] is not None
        c[C["L_VAC_NOT_READY"]] = (ll["start_t"] is not None and t - ll["start_t"] >= LL_NOT_READY_S
                                   and not c[C["L_VAC_READY_SW"]])
        if not c[C["L_R_P_SW"]] and ll["step1_t"] is None:
            ll["rough_t"] = None

    def _outputs(self) -> None:
        c, r = self.coils, self.regs
        for ch in (1, 2):
            g = self.gate[ch]
            c[C[f"G_V_{ch}_OPEN_LAMP"]] = g >= 1.0
            c[C[f"G_V_{ch}_CLOSE_LAMP"]] = g <= 0.0
            c[C[f"DOOR_{ch}_OPEN_LAMP"]] = self.door[ch] >= 1.0
            c[C[f"DOOR_{ch}_CLOSE_LAMP"]] = self.door[ch] <= 0.0
            c[C[f"VENT_{ch}_LAMP"]] = c[C[f"VENT_{ch}_SW"]]
            pos, since = self.chuck_at[ch]
            hold = self.t - since >= CHUCK_LAMP_HOLD_S - 1e-9
            for name, p in (("UP", 1.0), ("MID", 0.5), ("DOWN", 0.0)):
                c[C[f"Z{ch}_{name}_LOCATION"]] = hold and abs(pos - p) < 1e-6
                c[C[f"Z{ch}_{name}"]] = abs(self.chuck[ch] - p) < 1e-6
            v = {}
            for k, key in VALVE_KEYS:
                itl = C.get(f"{k}_{ch}_인터락")
                v[key] = bool(c[C[f"{k}_{ch}_SW"]] and (itl is None or c[itl]))
            v["ms"] = bool(c[C[f"MAIN_SHUTTER_{ch}_SW"]] and c[C[f"MAIN_SHUTTER_{ch}_인터락"]])
            v["gate"] = g >= 1.0
            v["door"] = self.door[ch] > 0.0
            for gname in ("Ar", "O2", "N2", "MAIN"):
                k = f"{gname}_{ch}_GAS_SW"
                if k in C:
                    itl = C.get(f"{gname}_{ch}_GAS_인터락")
                    v[gname.lower() if gname != "MAIN" else "main_gas"] = bool(c[C[k]] and (itl is None or c[itl]))
            if ch == 2:
                for i in (1, 2, 3):
                    v[f"g{i}"] = c[C[f"SHUTTER_{i}_SW"]]
            v["chuck_pos"] = self.chuck[ch]
            self.ch[ch].set_valves(**v)
        c[C["L_PIN_UP_LAMP"]] = self.pin >= 1.0
        c[C["L_PIN_DOWN_LAMP"]] = self.pin <= 0.0
        c[C["L_VENT_LAMP"]] = c[C["L_VENT_SW"]]
        c[C["L_ATM"]] = self.ll["atm"]
        c[C["LOADING_1_SENSOR_LAMP"]] = bool(self.loading[1])
        c[C["LOADING_2_SENSOR_LAMP"]] = bool(self.loading[2])
        gauge = (not self.ll["atm"]) or c[C["LP_STEP1"]] or c[C["LP_STEP2"]] or c[C["L_VAC_READY_SW"]]
        c[PM.P_MIRROR_MAP["L_GAUGE_A"]] = gauge
        c[C["L_GAUGE_A_INTERLOCK"]] = gauge
        c[PM.P_MIRROR_MAP["L_ATM_SENSOR"]] = self.ll["atm"]
        c[PM.P_MIRROR_MAP["L_R_P_OUT"]] = c[C["L_R_P_SW"]]
        c[PM.P_MIRROR_MAP["L_R_V_OUT"]] = c[C["L_R_V_SW"]]
        c[PM.P_MIRROR_MAP["L_VENT_OUT"]] = c[C["L_VENT_SW"]]
        # 전원 A/D
        ch2 = self.ch[2]
        lit = ch2.can_ignite()
        dc_ps = []
        for i, (vr, ir) in ((0, (0, 1)), (3, (10, 11))):
            p = self.out_w[i] if (lit and not self.dc_fail) else 0.0
            dc_ps.append(p)
            volt = math.sqrt(max(0.0, p) * DC_LOAD_OHM)
            amp = p / volt if volt > 0 else 0.0
            r[vr] = max(0, min(4000, int(round(volt / 0.50316))))
            r[ir] = max(0, min(4000, int(round(amp / 0.00097405))))
        w = self.out_w[2]
        fwd = max(0.0, (w - 2.6323) / 1.6546) if w > 0.01 else 0.0
        ref = fwd * (RF_REF_RATIO_LIT if lit else RF_REF_RATIO_DARK) + (self.ref_inject_w if fwd > 1.0 else 0.0)
        r[8] = max(0, min(4000, int(round((fwd + 0.598) / 0.15059)))) if fwd > 0 else 0
        r[9] = max(0, min(4000, int(round((ref + 0.267) / 0.12940)))) if fwd > 0 else 0
        ch2.dc_set_w = sum(dc_ps)
        ch2.rf_set_w = fwd
        pw = self.out_w[1]
        pfwd = pw / 1.74 if pw > 0.01 else 0.0
        self.pc_fwd_w = pfwd
        r[2] = max(0, min(4000, int(round((pfwd + 4.0 - 3.0664228165) / 0.1503488383)))) if pfwd > 0 else 0
        r[3] = max(0, min(4000, int(round((pfwd * 0.01 + 14.0 - 12.2067054791) / 0.1565388751)))) if pfwd > 0 else 0
        ch2.heater_mv = self.heater.output
        for k, v in self.forced_regs.items():
            r[k] = v

    # ───────── Modbus TCP 슬레이브 ─────────
    def respond(self, data: bytes, now: float):
        self.advance_to(now)
        self._buf += data
        out = []
        while True:
            got = split_request(self._buf)
            if got is None:
                break
            tid, unit, pdu, self._buf = got
            frame = mbap(tid, unit, pdu)
            self.frames.append((now, frame))
            if self.drop_next > 0:
                self.drop_next -= 1
                continue
            res = super().respond(frame, now)
            if not res:
                continue
            (t, resp), = res
            if self._active("truncate", now):
                resp = resp[: max(1, len(resp) // 2)]
            if self._active("stale", now):
                out.append((t, mbap((tid - 1) or 0xFFFF, unit, bytes([pdu[0]]) + b"\x00" * 4)))
            out.append((t, resp))
        return out

    def handle(self, frame: bytes, now: float) -> Optional[bytes]:
        got = split_request(frame)
        if got is None:
            return None
        tid, unit, pdu, _ = got
        if unit != self.unit:
            return None
        body = self._pdu(pdu, now)
        return None if body is None else mbap(tid, unit, body)

    def _pdu(self, pdu: bytes, now: float) -> Optional[bytes]:
        fc = pdu[0] if pdu else 0
        if self._active("exception", now):
            return bytes([fc | 0x80, 4])
        try:
            if fc in (READ_COILS, READ_REGS):
                addr, n = struct.unpack(">HH", pdu[1:5])
                if fc == READ_COILS:
                    lim = COIL_N if self.coil_limit is None else min(COIL_N, self.coil_limit)
                    if n < 1 or n > 2000 or addr + n > lim:
                        return bytes([fc | 0x80, 2])
                    if self._active("short", now) and n > 1:
                        n = n // 2
                    body = bytearray((n + 7) // 8)
                    for i, b in enumerate(self.coils[addr:addr + n]):
                        if b:
                            body[i // 8] |= 1 << (i % 8)
                    return bytes([fc, len(body)]) + bytes(body)
                if n < 1 or n > 125 or addr + n > REG_N:
                    return bytes([fc | 0x80, 2])
                if self._active("short", now) and n > 1:
                    n = n // 2
                return bytes([fc, 2 * n]) + b"".join(struct.pack(">H", v & 0xFFFF) for v in self.regs[addr:addr + n])
            if fc == WRITE_COIL:
                addr, v = struct.unpack(">HH", pdu[1:5])
                if addr >= COIL_N:
                    return bytes([fc | 0x80, 2])
                if v not in (0, 0xFF00):
                    return bytes([fc | 0x80, 3])
                self.coils[addr] = v == 0xFF00
                self._step(0.0)
                return pdu[:5]
            if fc == WRITE_REG:
                addr, v = struct.unpack(">HH", pdu[1:5])
                if addr >= REG_N:
                    return bytes([fc | 0x80, 2])
                self.regs[addr] = v
                self._step(0.0)
                return pdu[:5]
            if fc == WRITE_COILS:
                addr, n, nb = struct.unpack(">HHB", pdu[1:6])
                if addr + n > COIL_N:
                    return bytes([fc | 0x80, 2])
                for i in range(n):
                    self.coils[addr + i] = bool(pdu[6 + i // 8] >> (i % 8) & 1)
                self._step(0.0)
                return pdu[:5]
            if fc == WRITE_REGS:
                addr, n, nb = struct.unpack(">HHB", pdu[1:6])
                if addr + n > REG_N:
                    return bytes([fc | 0x80, 2])
                for i in range(n):
                    self.regs[addr + i] = struct.unpack(">H", pdu[6 + 2 * i:8 + 2 * i])[0]
                self._step(0.0)
                return pdu[:5]
        except (struct.error, IndexError):
            return bytes([fc | 0x80, 3])
        return bytes([fc | 0x80, 1])

    # ───────── 시험 도우미 ─────────
    def pdus(self) -> List[bytes]:
        """받은 요청의 MBAP 뒤 PDU 목록(트랜잭션 id 제외 비교용)."""
        return [f[7:] for _, f in self.frames]

    def write_pdus(self) -> List[bytes]:
        return [p for p in self.pdus() if p and p[0] in (5, 6, 15, 16)]

    def coil(self, name: str) -> bool:
        return self.coils[PM.resolve(name)]

    def reg(self, name) -> int:
        return self.regs[PM.resolve(name)]
