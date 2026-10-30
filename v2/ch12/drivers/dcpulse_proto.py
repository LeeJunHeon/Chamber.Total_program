"""DC Pulse(EnerPulse EP5) 프로토콜·순수 함수 — v1 device/dc_pulse.py 를 읽고 다시 쓴 것(import 안 함).

프레임(Protocol Type 4, RS-232) = STX 02 · CMD · DATA(0/1/2 B, 2 B 는 MSB 먼저) · ETX 03 · CHK
  CHK: STX~ETX 바이트별 상위 니블 합 hi·하위 니블 합 lo, hi += lo>>4, CHK = ((hi&0xF)<<4)|(lo&0xF)
  예) Off Time 5 us = 02 67 00 32 03 9E (매뉴얼 p.50, v1 dc_pulse.py:88-103·164-201)
응답: 1 B ACK 06 / ERR 04 단독, 또는 02 CMD DATA 03 CHK 프레임. 읽기 응답 데이터 길이 = 0x9A(PIV) 6 B, 그 밖 2 B 를 먼저
  보고(길이 규칙), 안 맞으면 CHK 가 맞는 0x03 을 찾는다(ETX 위치 ≤ 8). 못 찾고 10 B 이상 쌓이면 1 B 버리고 다시 맞춘다
  (v1 _tcp_reader_loop, dc_pulse.py:2340-2479 — 그대로).
펄스 규격(매뉴얼 품기28-EPPEP102-V08, v1 dc_pulse.py:114-162): 주파수 20~150 kHz 정수, Off Time DC(raw 9) 또는
  1.0~min(10.0, 400/f) us(×10 스케일). 듀티는 장비에 없는 개념(레거시 레시피 환산용).
과전류 상한(v1 lib/power_limits.py): P ≤ P1 → I1, P1~P2 직선, P ≥ P2 → 같은 기울기 연장(EXTRAPOLATE) 또는 I2.
"""
from __future__ import annotations

import math
from collections import deque
from typing import Deque, Dict, List, Optional, Tuple

STX, ETX, ACK, ERR = 0x02, 0x03, 0x06, 0x04

# 명령 코드(v1 dc_pulse.py:5-9 독스트링·각 메서드)
MASTER_ONOFF, MASTER_REFER, MASTER_MODE = 0x7B, 0x7C, 0x7D
REGULATION, REF_LEVEL, OUTPUT = 0x81, 0x83, 0x80
PULSE_SYNC, PULSE_FREQ, OFF_TIME = 0x65, 0x66, 0x67
FAULT_RESET, ARC_COUNT_RESET = 0x6F, 0x8C
R_STATUS, R_OPERATION, R_ARC_SOFT, R_ARC_HARD, R_PIV = 0x90, 0x91, 0x96, 0x99, 0x9A
R_CTRL_MODE, R_FAULT, R_SYNC, R_FREQ, R_OFF = 0x9C, 0x9E, 0xA5, 0xA6, 0xA7
R_ARC_SOFT_RATE, R_ARC_HARD_RATE = 0xAE, 0xAF
R_MASTER_ONOFF, R_MASTER_REFER, R_MASTER_MODE = 0xBB, 0xBC, 0xBD
READ_LABELS = {R_STATUS: "READ_STATUS", R_OPERATION: "READ_OPERATION", R_ARC_SOFT: "READ_ARC_SOFT",
               R_ARC_HARD: "READ_ARC_HARD", R_PIV: "READ_PIV", R_CTRL_MODE: "READ_CTRL_MODE", R_FAULT: "READ_FAULT",
               R_SYNC: "READ_PULSE_SYNC", R_FREQ: "READ_PULSE_FREQ", R_OFF: "READ_OFF_TIME",
               R_ARC_SOFT_RATE: "READ_ARC_SOFT_RATE", R_ARC_HARD_RATE: "READ_ARC_HARD_RATE",
               R_MASTER_ONOFF: "READ_MASTER_ONOFF", R_MASTER_REFER: "READ_MASTER_REFER",
               R_MASTER_MODE: "READ_MASTER_MODE"}
CTRL_MODES = {1: "HOST", 2: "REMOTE", 4: "LOCAL"}          # 0x9C (매뉴얼 p.56)

FREQ_MIN_KHZ, FREQ_MAX_KHZ = 20, 150
OFF_MIN_US, OFF_ABS_MAX_US, OFF_RAW_DC = 1.0, 10.0, 9
PIV_SCALE_ERR_MAX_PCT = 15.0
ARC_RESET_LABEL = {"yes": "예", "no": "아니오", "unknown": "확인불가"}


# ───────────── 프레임 ─────────────
def chk(items: bytes) -> int:
    hi = lo = 0
    for b in items:
        hi += (b >> 4) & 0x0F
        lo += b & 0x0F
    hi += lo >> 4
    return ((hi & 0x0F) << 4) | (lo & 0x0F)


def frame(cmd: int, data: bytes = b"") -> bytes:
    core = bytes([STX, cmd & 0xFF]) + data + bytes([ETX])
    return core + bytes([chk(core)])


def pack_write(cmd: int, value: Optional[int] = None, width: int = 0) -> bytes:
    """v1 BinaryProtocol.pack_write — width 0/1/2, 2 B 는 MSB 먼저."""
    if width == 0 or value is None:
        data = b""
    elif width == 1:
        data = bytes([int(value) & 0xFF])
    elif width == 2:
        v = int(value) & 0xFFFF
        data = bytes([(v >> 8) & 0xFF, v & 0xFF])
    else:
        raise ValueError("width must be 0/1/2")
    return frame(cmd, data)


def pack_read(cmd: int) -> bytes:
    return frame(cmd, b"")


class FrameParser:
    """v1 _tcp_reader_loop 의 파서 그대로. feed(바이트) → 토큰(1 B ACK/ERR 또는 CMD+DATA) 목록과 진단 문구."""

    def __init__(self) -> None:
        self.buf = bytearray()
        self.notes: Deque[str] = deque(maxlen=64)          # [CHKFAIL] 등(이벤트로 옮긴 뒤 비운다)

    def reset(self) -> None:
        self.buf.clear()

    def feed(self, chunk: bytes) -> List[bytes]:
        buf = self.buf
        buf.extend(chunk)
        out: List[bytes] = []
        while True:
            emitted = False
            while buf and buf[0] in (ACK, ERR):
                out.append(bytes([buf[0]]))
                del buf[0]
                emitted = True
            if emitted:
                continue
            try:
                i_stx = buf.index(STX)
            except ValueError:
                i, found = 0, False
                while i < len(buf):
                    if buf[i] in (ACK, ERR):
                        out.append(bytes([buf[i]]))
                        del buf[i]
                        found = True
                        continue
                    i += 1
                if not found:
                    buf.clear()
                break
            if i_stx > 0:
                for b in bytes(buf[:i_stx]):
                    if b in (ACK, ERR):
                        out.append(bytes([b]))
                del buf[:i_stx]
            i_etx = -1
            if len(buf) >= 2:
                dlen = 6 if buf[1] == R_PIV else 2
                pos = 2 + dlen
                if len(buf) >= pos + 2:
                    if buf[pos] == ETX and chk(bytes(buf[:pos + 1])) == buf[pos + 1]:
                        i_etx = pos
                    else:
                        j = 2
                        while j <= min(8, len(buf) - 2):
                            try:
                                j = buf.index(ETX, j)
                            except ValueError:
                                break
                            if j > 8 or len(buf) < j + 2:
                                break
                            if chk(bytes(buf[:j + 1])) == buf[j + 1]:
                                i_etx = j
                                break
                            j += 1
            if i_etx < 0:
                if len(buf) >= 10:
                    self.notes.append(f"[CHKFAIL] resync: head={bytes(buf[:10]).hex(' ')}")
                    del buf[0]
                    continue
                break
            core = bytes(buf[:i_etx + 1])
            got = buf[i_etx + 1]
            if chk(core) == got:
                out.append(core[1:-1])
            else:
                self.notes.append(f"[CHKFAIL] core={core.hex(' ')} recv_chk={got:02X} expect={chk(core):02X}")
            del buf[:i_etx + 2]
        return out


# ───────────── 응답 해석(v1 read_* 와 같은 규칙) ─────────────
def u16_tail(resp: Optional[bytes]) -> Optional[int]:
    """0x96/0x99/0xA6/0xA7/0xAE/0xAF: 응답이 없거나 2 B 미만이면 None, 아니면 끝 2 B(MSB 먼저)."""
    if not resp or len(resp) < 2:
        return None
    return (resp[-2] << 8) | resp[-1]


def flags16(data: bytes) -> Optional[int]:
    if not data:
        return None
    if len(data) >= 2:
        return ((data[-2] << 8) | data[-1]) & 0xFFFF
    return data[-1] & 0xFF


def hv_on(flags: int) -> bool:
    """0x90 f0 니블 LSB = HV On."""
    return bool(flags & 0x0001)


def piv_raw(resp: Optional[bytes]) -> Optional[Dict[str, int]]:
    """0x9A: 응답 꼬리 6 B = P, I, V(각 MSB 먼저). ERR·짧은 응답이면 None."""
    if not resp or (len(resp) == 1 and resp[0] == ERR) or len(resp) < 6:
        return None
    d = resp[-6:]
    return {"P": (d[0] << 8) | d[1], "I": (d[2] << 8) | d[3], "V": (d[4] << 8) | d[5]}


def piv_scale(raw: Dict[str, int], w_lsb: float, a_lsb: float, v_lsb: float) -> Dict[str, float]:
    return {"P_W": raw["P"] * float(w_lsb), "I_A": raw["I"] * float(a_lsb), "V_V": raw["V"] * float(v_lsb)}


def piv_scale_check(p_w: float, i_a: float, v_v: float) -> Optional[Tuple[bool, float]]:
    """P 와 V×I 의 정합성(오차 ≤ 15 %). P 또는 I 가 0 이하면 None(판정 불가)."""
    if p_w <= 0.0 or i_a <= 0.0:
        return None
    err = abs(p_w - v_v * i_a) / max(1.0, p_w) * 100.0
    return (err <= PIV_SCALE_ERR_MAX_PCT, err)


# ───────────── 펄스 규격 ─────────────
def is_keep(x) -> bool:
    return isinstance(x, str) and x.strip().lower() == "keep"


def is_dc_mode(x) -> bool:
    return isinstance(x, str) and x.strip().upper() == "DC"


def max_off_time_us(f_khz: float) -> float:
    f = float(f_khz)
    if f <= 0.0:
        return OFF_ABS_MAX_US
    return round(min(OFF_ABS_MAX_US, 400.0 / f), 1)


def off_time_to_raw(off_us: float) -> int:
    return int(round(float(off_us) * 10.0))


def raw_to_off_time(raw: int) -> Optional[float]:
    return None if int(raw) == OFF_RAW_DC else int(raw) / 10.0


def duty_to_off_time_us(f_khz: float, duty_pct: float) -> float:
    period_us = 1000.0 / max(1e-6, float(f_khz))
    return round(max(0.0, period_us * (1.0 - float(duty_pct) / 100.0)), 1)


def off_time_to_duty_pct(f_khz: float, off_us: float) -> float:
    period_us = 1000.0 / max(1e-6, float(f_khz))
    return round((period_us - float(off_us)) / period_us * 100.0, 1)


def actual_pulse_params(freq_khz: Optional[int], off_raw: Optional[int]) -> Optional[dict]:
    """v1 read_actual_pulse_params 의 계산(0xA6·0xA7 값)."""
    if freq_khz is None or off_raw is None:
        return None
    if off_raw == OFF_RAW_DC:
        return {"freq_khz": freq_khz, "off_time_us": None, "duty_pct": 100.0}
    off_us = off_raw / 10.0
    period_us = 1000.0 / max(freq_khz, 1)
    return {"freq_khz": freq_khz, "off_time_us": off_us, "duty_pct": round((period_us - off_us) / period_us * 100, 1)}


# ───────────── 아크 ─────────────
def arc_reset_state(pre_s, pre_h, post_s, post_h) -> str:
    """0x8C 리셋 판정 "yes"/"no"/"unknown" — 리셋 전 1 이상이던 카운터가 줄었는지로만."""
    judged = [(int(a), int(b)) for a, b in ((pre_s, post_s), (pre_h, post_h))
              if a is not None and b is not None and int(a) >= 1]
    if not judged:
        return "unknown"
    return "yes" if any(b < a for a, b in judged) else "no"


def arc_split(run_s: int, run_h: int, ign_s: Optional[int], ign_h: Optional[int]) -> dict:
    rs, rh = max(0, int(run_s or 0)), max(0, int(run_h or 0))
    if ign_s is None or ign_h is None:
        i_s, i_h = rs, rh
    else:
        i_s, i_h = min(rs, max(0, int(ign_s))), min(rh, max(0, int(ign_h)))
    d_s, d_h = max(0, rs - i_s), max(0, rh - i_h)
    return {"run_soft": rs, "run_hard": rh, "run_total": rs + rh, "ign_soft": i_s, "ign_hard": i_h,
            "ign_total": i_s + i_h, "depo_soft": d_s, "depo_hard": d_h, "depo_total": d_s + d_h,
            "ign_closed": not (ign_s is None or ign_h is None)}


# ───────────── 과전류 상한(v1 lib/power_limits.dc_overcurrent_limit_a) ─────────────
def overcurrent_limit_a(p_set_w, p1: float, i1: float, p2: float, i2: float, extrapolate: bool) -> float:
    try:
        _i1 = float(i1)
    except Exception:
        _i1 = 1.0
    if not math.isfinite(_i1) or _i1 <= 0.0:
        _i1 = 1.0
    try:
        _p1, _p2, _i2 = float(p1), float(p2), float(i2)
        p = float(p_set_w) if p_set_w is not None else 0.0
    except Exception:
        return _i1
    if not all(math.isfinite(x) for x in (_p1, _p2, _i2, p)) or _p2 <= _p1 or _i2 <= 0.0:
        return _i1
    if p <= _p1:
        return _i1
    slope = (_i2 - _i1) / (_p2 - _p1)
    if p >= _p2:
        lim = _i2 + (p - _p2) * slope if extrapolate else _i2
    else:
        lim = _i1 + (p - _p1) * slope
    if not math.isfinite(lim) or lim <= 0.0:
        return _i1
    return lim
