"""CH2 히터 PLC 드라이버 — VARITAP VSCD-30 + PLC 내장 PID(XBL-EMTA HEATER 래더). v1 controller/heater_service.py 의 PLC 쪽.

주소(heater_service.py:36-58 — XBL-EMTA: 레지스터 = D 번호, 코일 = M 워드×16 + 비트). PLC_REG_MAP·PLC_COIL_MAP 에 넣지 않는다
  (넣으면 1초 스냅샷 레지스터 범위·PLC 코일 CSV 열이 바뀐다 — HEAT-006). 읽기는 PLC 1초 묶음(plc.py source_read ②:
  FC3 D110..D160 51워드 + FC1 640..658 19비트, 저우선 — 재시도·재연결 없음, 짧은 응답은 0/False 채움 HEAT-007·010)이 하고,
  이 드라이버는 해독기(register_decoder)로 그 값을 받는다.
쓰기 허용(HEAT-004): 레지스터 {D112,D113,D114,D118,D120,D121,D126,D127,D128,D142~D145}, 코일 {640 RUN, 643 RST}.
  밖이면 ValueError('히터 쓰기 허용 밖 …') — PLC 로 보내지 않는다. 세션 마커(D160)는 v1 처럼 이 목록을 거치지 않는 직접 쓰기
  (heater_service.py:933, 결정 D146) — PLC 드라이버 허용 목록에는 마커 레지스터를 더해 둔다.
쓰기 횟수(write_seq, HEAT-014): 상태를 바꾸는 쓰기마다 +1(워치독 D114·세션 마커 제외). 쓰기·읽기는 모두 PLC worker 스레드에서
  차례로 일어나므로, 읽은 순간의 write_seq 와 처리 순간의 write_seq 가 다르면 그 읽기는 '쓰기 전 상태'다 — 엔진이 버린다.
쓰기 함수(w_reg·w_coil·w_marker·w_wd)는 worker 요청 fn 안(session s)에서만 부른다. 실패는 예외(v1 await 실패와 같음).
  bench(writes_allowed=False)면 RuntimeError('BENCH_READ_ONLY')(worker·Modbus 클라이언트의 이중 보호 위에 한 번 더).
IO 상한: v1 은 모든 히터 호출을 asyncio.wait_for 6 s 로 묶었다(HEAT-005). v2 는 PLC 원시 연산 정책(plc_io — 응답 제한
  PLC_TIMEOUT_S 2 s·재시도 1회)과 요청 실행 상한(cmd_grace_s 10.5 s)이 그 역할을 한다.

1초 행 블록 '<chamber>.heater' — 해독 값(pv·sv·mv·run·itl…)과 엔진 값(owner·target·hold·phase…, 엔진 snapshot_fields).
BUDGET: 읽기는 PLC 1초 묶음에 포함(질의 0). 쓰기는 엔진 폴링(1 s RUN 중·3 s 정지)·워치독(5 s)·명령 때만.
"""
from __future__ import annotations

import struct
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType

# ── 주소(controller/heater_service.py:36-52) ──
REG_BLOCK_START = 110
REG_BLOCK_COUNT = 51          # D110~D160
REG_PV, REG_SV, REG_SV_LIMIT, REG_WD = 110, 112, 113, 114
REG_CUR_SV, REG_PID_ERR, REG_MV_LIMIT, REG_SV_RAMP = 116, 117, 118, 119
REG_RAMP_RATE, REG_HOLDBACK = 120, 121
REG_OT_LIMIT, REG_SLOW_ZONE, REG_SLOW_RATE = 126, 127, 128
REG_MV = 141
REG_KP_LO, REG_KP_HI, REG_TI_LO, REG_TI_HI = 142, 143, 144, 145
REG_KP_ACT_LO, REG_KP_ACT_HI, REG_TI_ACT_LO, REG_TI_ACT_HI = 146, 147, 148, 149

COIL_BASE = 640
COIL_COUNT = 19               # 640~658
COIL_RUN, COIL_ITL, COIL_FAULT, COIL_RST = 640, 641, 642, 643
COIL_OT, COIL_TC_ERR, COIL_WD_ERR = 644, 645, 646
COIL_PID_RUN, COIL_DAC_EN, COIL_TC_OK, COIL_DA_OK = 649, 652, 653, 656
COIL_WATER_OK, COIL_VAC_OK = 657, 658   # M00411 냉각수(WATER_2_SGN) · M00412 진공(M_V_2_인터락)

WRITE_REGS = frozenset({REG_SV, REG_SV_LIMIT, REG_WD, REG_MV_LIMIT, REG_RAMP_RATE, REG_HOLDBACK,
                        REG_OT_LIMIT, REG_SLOW_ZONE, REG_SLOW_RATE,
                        REG_KP_LO, REG_KP_HI, REG_TI_LO, REG_TI_HI})
WRITE_COILS = frozenset({COIL_RUN, COIL_RST})

BUDGET = dict(queries_per_tick=0, bytes_tx=0, bytes_rx=0, min_gap_ms=150, fits_1s=True, alternate="",
              note="읽기는 PLC 1초 묶음(FC3 D110..D160·FC1 640..658, plc.py BUDGET 에 포함). 쓰기만 PLC worker 로 — "
                   "엔진 폴링(RUN 중 1 s·정지 3 s)의 램프 SV·D118, 워치독 D114 5 s, 설정 push(12건, 접속·재기동 때), 명령")


def s16(v: int) -> int:
    v = int(v) & 0xFFFF
    return v - 65536 if v >= 32768 else v


def float_to_words(v: float):
    """IEEE754 단정도 → (하위 워드, 상위 워드). 하위 워드가 앞 주소다(lib/heater_config.py:93-96)."""
    bits = struct.unpack(">I", struct.pack(">f", float(v)))[0]
    return bits & 0xFFFF, (bits >> 16) & 0xFFFF


def words_to_float(low: int, high: int) -> float:
    """(하위 워드, 상위 워드) → float(lib/heater_config.py:99-102)."""
    bits = ((int(high) & 0xFFFF) << 16) | (int(low) & 0xFFFF)
    return struct.unpack(">f", struct.pack(">I", bits))[0]


def decode_block(regs: List[int], bits: List[bool], c, now: float) -> Dict[str, Any]:
    """블록 원시값 → v1 상태 사전(controller/heater_service.py:656-700). c = HeaterCfg."""
    sc = float(c.temp_scale)

    def r(addr):
        return int(regs[addr - REG_BLOCK_START])

    def b(addr):
        return bool(bits[addr - COIL_BASE])

    raw_pv = s16(r(REG_PV))
    tc_bad = raw_pv == -1
    mv = r(REG_MV)
    span = c.mv_abs_max - c.mv_min
    mv_pct = 0.0 if (mv < c.mv_min or span <= 0) else max(0.0, min(100.0, (mv - c.mv_min) / span * 100.0))
    mreg = int(c.session_mark_reg)
    return {
        "ok": not tc_bad,
        "pv": None if tc_bad else raw_pv * sc,
        "sv": r(REG_SV) * sc,
        "sv_limit": r(REG_SV_LIMIT) * sc,
        "cur_sv": s16(r(REG_CUR_SV)) * sc,
        "pid_err": r(REG_PID_ERR),
        "mv": mv,
        "mv_pct": mv_pct,
        "mv_limit": r(REG_MV_LIMIT),
        "sv_ramp": s16(r(REG_SV_RAMP)) * sc,
        "ramp_rate": r(REG_RAMP_RATE) * 6,
        "holdback": r(REG_HOLDBACK) * sc,
        "ot_limit": r(REG_OT_LIMIT) * sc,
        "slow_zone": r(REG_SLOW_ZONE) * sc,
        "slow_rate": r(REG_SLOW_RATE) * 6,
        "kp_act": words_to_float(r(REG_KP_ACT_LO), r(REG_KP_ACT_HI)),
        "ti_act": words_to_float(r(REG_TI_ACT_LO), r(REG_TI_ACT_HI)),
        "marker": r(mreg) if (REG_BLOCK_START <= mreg < REG_BLOCK_START + REG_BLOCK_COUNT) else None,
        "run": b(COIL_RUN), "itl": b(COIL_ITL), "fault": b(COIL_FAULT),
        "ot": b(COIL_OT), "tc_err": b(COIL_TC_ERR), "wd_err": b(COIL_WD_ERR),
        "pid_run": b(COIL_PID_RUN), "dac_en": b(COIL_DAC_EN), "tc_ok": b(COIL_TC_OK), "da_ok": b(COIL_DA_OK),
        "water_ok": b(COIL_WATER_OK), "vac_ok": b(COIL_VAC_OK),
        "rst": b(COIL_RST),
        "noresp": None,
        "stale": False, "age_s": 0.0, "t": now,
        "_raw_pv": raw_pv, "_regs": list(regs),
    }


@dataclass
class HeaterRead:
    """1초 묶음 읽기 한 번의 히터 원시값(worker 스레드에서 만든다)."""
    regs: List[int]
    bits: List[bool]
    t_mono: float
    seq: int                      # 읽은 순간의 write_seq


@dataclass
class HeaterReadFail:
    why: str
    t_mono: float


@dataclass(frozen=True)
class HeaterEvent:
    kind: str           # log / card / process / state
    detail: str = ""
    reason: Optional[str] = None
    code: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


# ── 1초 행 필드 ──
_DEC = (
    ("pv", FType.FLOAT, "°C", "히터 PV", "plc:D110×0.1(s16, -1 = TC 이상)", "mean"),
    ("sv", FType.FLOAT, "°C", "히터 SV(D112)", "plc:D112×0.1", "last"),
    ("sv_ramp", FType.FLOAT, "°C", "램프 중간 목표(D119)", "plc:D119×0.1(s16)", "last"),
    ("cur_sv", FType.FLOAT, "°C", "현재 SV(D116)", "plc:D116×0.1(s16)", "last"),
    ("sv_limit", FType.FLOAT, "°C", "SV 상한(D113)", "plc:D113×0.1", "last"),
    ("ot_limit", FType.FLOAT, "°C", "과온 트립(D126)", "plc:D126×0.1", "last"),
    ("mv", FType.INT, "count", "DAC 출력(D141)", "plc:D141", "mean"),
    ("mv_pct", FType.FLOAT, "%", "출력%", "(MV−MV_MIN)/(MV_ABS_MAX−MV_MIN)", "mean"),
    ("mv_limit", FType.INT, "count", "DAC 상한(D118)", "plc:D118", "last"),
    ("pid_err", FType.INT, "", "PID 오류(D117)", "plc:D117", "last"),
    ("ramp_rate", FType.INT, "°C/min", "래더 램프(D120×6)", "plc:D120", "last"),
    ("holdback", FType.FLOAT, "°C", "홀드백(D121)", "plc:D121×0.1", "last"),
    ("slow_zone", FType.FLOAT, "°C", "감속 구간(D127)", "plc:D127×0.1", "last"),
    ("slow_rate", FType.INT, "°C/min", "감속 램프(D128×6)", "plc:D128", "last"),
    ("kp_act", FType.FLOAT, "", "적용 Kp(D146/D147)", "plc:D146~D147 REAL", "last"),
    ("ti_act", FType.FLOAT, "s", "적용 Ti(D148/D149)", "plc:D148~D149 REAL", "last"),
    ("marker", FType.INT, "", "세션 마커(D160)", "plc:D160", "last"),
    ("wd", FType.INT, "", "워치독(D114)", "plc:D114", "last"),
)
_BITS = (("run", COIL_RUN, "RUN"), ("itl", COIL_ITL, "인터락"), ("fault", COIL_FAULT, "이상"), ("rst", COIL_RST, "리셋"),
         ("ot", COIL_OT, "과온"), ("tc_err", COIL_TC_ERR, "TC 이상"), ("wd_err", COIL_WD_ERR, "워치독 이상"),
         ("pid_run", COIL_PID_RUN, "PID 동작"), ("dac_en", COIL_DAC_EN, "DAC 출력"), ("tc_ok", COIL_TC_OK, "TC 정상"),
         ("da_ok", COIL_DA_OK, "D/A 정상"), ("water_ok", COIL_WATER_OK, "냉각수"), ("vac_ok", COIL_VAC_OK, "진공 인터락"))
_ENGINE = (
    ("owner", FType.STR, "", "히터 제어 주체(manual/process/external)"),
    ("target", FType.FLOAT, "°C", "최종 목표(램프 목표 또는 SV)"),
    ("ramp_active", FType.BOOL, "", "파이썬 램프 중"),
    ("ramp_set_rate", FType.FLOAT, "°C/min", "파이썬 램프 속도"),
    ("hold", FType.STR, "", "유지 모드 상태(idle/arming/holding)"),
    ("hold_value", FType.INT, "count", "유지 DAC 상한"),
    ("sat_clamp", FType.INT, "count", "포화 클램프 D118"),
    ("phase", FType.STR, "", "공정 연동 단계(set/wait/soak/hold/watch)"),
    ("ready", FType.BOOL, "", "켤 수 있음(is_ready)"),
    ("stale", FType.BOOL, "", "엔진 상태가 오래됨(HEATER_STALE_SEC)"),
)


def blocks(chamber: str = "ch2") -> List[DeviceBlock]:
    f = [FieldDef(n, t, u, lb, fmt=(".1f" if t is FType.FLOAT else ""), source=src, summary=sm)
         for n, t, u, lb, src, sm in _DEC]
    f += [FieldDef(n, FType.BOOL, "", lb, source=f"plc:FC1 {a}", summary="any") for n, a, lb in _BITS]
    f += [FieldDef(n, t, u, lb, source="engine", detail=True) for n, t, u, lb in _ENGINE]
    return [DeviceBlock(f"{chamber}.heater", "CH2 히터(PLC PID)", tuple(f),
                        desc="VARITAP VSCD-30 + PLC 내장 PID — D110~D160·M640~M658")]


def engine_columns(chamber: str = "ch2") -> List[str]:
    return [f"{chamber}.heater.{n}" for n, *_ in _ENGINE]


class HeaterDriver:
    """PLC 드라이버 위의 히터 입출력. plc = PlcDriver(또는 같은 API 의 시험용 가짜)."""

    def __init__(self, clock, plc, cfg: Dict[str, Any], writes_allowed: bool,
                 on_event: Optional[Callable[[HeaterEvent], None]] = None, chamber: str = "ch2") -> None:
        self.clock, self.plc, self.cfg = clock, plc, cfg
        self.writes_allowed = bool(writes_allowed)
        self.on_event = on_event
        self.chamber = chamber
        self.block = f"{chamber}.heater"
        self.prefix = chamber.upper() + "_"
        self.write_seq = 0
        self._lock = threading.Lock()
        self._newest: Optional[object] = None        # HeaterRead | HeaterReadFail (아직 엔진이 안 가져간 가장 새것)
        self.last_read: Optional[HeaterRead] = None
        self.reads = 0
        self.fails = 0
        mreg = int(cfg.get(self.prefix + "HEATER_SESSION_MARK_REG", 160) or 0)
        plc.allow_writes(coils=sorted(WRITE_COILS), regs=sorted(WRITE_REGS) + ([mreg] if mreg else []))
        plc.register_decoder((self.block,), self._decode, self._read_failed)

    # ───────── 상태 ─────────
    @property
    def connected(self) -> bool:
        try:
            return bool(self.plc.connected)
        except Exception:
            return False

    def hcfg(self):
        from v2.ch12.heater_profile import load_heater_cfg
        return load_heater_cfg(self.cfg, self.prefix)[0]

    # ───────── 1초 읽기(PLC worker 스레드) ─────────
    def _decode(self, poll) -> Dict[str, Any]:
        regs = [poll.regs.get(a) for a in range(REG_BLOCK_START, REG_BLOCK_START + REG_BLOCK_COUNT)]
        bits = [poll.coils.get(a) for a in range(COIL_BASE, COIL_BASE + COIL_COUNT)]
        if any(v is None for v in regs) or any(v is None for v in bits):
            return {}
        rd = HeaterRead([int(v) for v in regs], [bool(v) for v in bits], self.clock.monotonic(), self.write_seq)
        with self._lock:
            self._newest = rd
            self.last_read = rd
            self.reads += 1
        st = decode_block(rd.regs, rd.bits, self.hcfg(), rd.t_mono)
        out = {f"{self.block}.{n}": st.get(n) for n, *_ in _DEC if n != "wd"}
        out[f"{self.block}.wd"] = rd.regs[REG_WD - REG_BLOCK_START]
        for n, _a, _lb in _BITS:
            out[f"{self.block}.{n}"] = st.get(n)
        for k in ("pv", "sv", "sv_ramp", "cur_sv", "sv_limit", "ot_limit", "holdback", "slow_zone", "mv_pct"):
            v = out.get(f"{self.block}.{k}")
            if v is not None:
                out[f"{self.block}.{k}"] = round(float(v), 3)
        return out

    def _read_failed(self, why: str) -> None:
        with self._lock:
            self._newest = HeaterReadFail(str(why), self.clock.monotonic())
            self.fails += 1

    def take(self):
        """엔진이 가져가지 않은 가장 새 읽기 결과(HeaterRead/HeaterReadFail) — 없으면 None."""
        with self._lock:
            v, self._newest = self._newest, None
            return v

    def inject(self, item) -> None:
        """시험·조립용 — 읽기 결과를 직접 넣는다."""
        with self._lock:
            self._newest = item

    # ───────── 요청 ─────────
    def submit(self, name: str, fn: Callable, on_done: Optional[Callable] = None, kind: Optional[str] = None):
        """PLC worker 요청 하나(출처 HEATER). bench 면 읽기로(쓰기 없이 해독·판정만 돌게)."""
        k = kind or ("write" if self.writes_allowed else "read")
        return self.plc.submit(f"heater:{name}", fn, kind=k, source="HEATER", on_done=on_done)

    # ───────── worker 스레드 안 쓰기 ─────────
    def _check(self, s) -> None:
        if not self.writes_allowed:
            raise RuntimeError("BENCH_READ_ONLY — 히터 쓰기 거부")
        if s is None:
            raise RuntimeError("PLC 링크 없음 — 쓰기 못 함")

    def w_reg(self, s, addr: int, value: int) -> None:
        if int(addr) not in WRITE_REGS:
            raise ValueError(f"히터 쓰기 허용 밖 레지스터 D{addr}")
        self._check(s)
        if int(addr) != REG_WD:                # 워치독 갱신은 상태를 바꾸지 않는다(heater_service.py:478)
            self.write_seq += 1
        self.plc.io_write_reg(s, int(addr), int(value) & 0xFFFF)

    def w_coil(self, s, addr: int, on: bool) -> None:
        if int(addr) not in WRITE_COILS:
            raise ValueError(f"히터 쓰기 허용 밖 코일 {addr}")
        self._check(s)
        self.write_seq += 1
        self.plc.io_write_coil(s, int(addr), bool(on))

    def w_marker(self, s, reg: int, value: int) -> None:
        """세션 마커 — v1 처럼 히터 허용 목록·쓰기 횟수를 거치지 않는 직접 쓰기(heater_service.py:933)."""
        self._check(s)
        self.plc.io_write_reg(s, int(reg), int(value) & 0xFFFF)
