# controller/heater_service.py
# -*- coding: utf-8 -*-
"""CH2 히터(PLC 내장 PID) 서비스 — asyncio 단일 루프, Qt 없음.

PLC I/O 는 AsyncPLC 의 공개 메서드만 쓴다(is_connected / read_regs_block / read_coils_block / write_reg /
write_coil). 접속·재접속은 기존 하트비트 몫이다 — 여기서는 접속을 시도하지 않는다.
주소는 PLC_REG_MAP·PLC_COIL_MAP 에 넣지 않고 이 모듈의 상수로만 둔다
(REG_MAP 에 넣으면 snapshot_regs_fast 범위가 FC3 한도 125 를 넘고, COIL_MAP 에 넣으면 코일 CSV 열이 바뀐다).

폴링 루프(성공한 폴링 한 번의 처리 순서):
  (1) 잔존 RUN 정리 → (2) 보류 OFF → (3) 세션 마커·설정 push/되읽기(실패는 첫 쓰기에서 멈추고 30초 백오프)
  → (4) FAULT 엣지 → RST 코일 고착 확인 → 리셋 결과 확인 → (5) 출력 죽음
  → (6) RUN 엣지(CSV·챗, 종료 사유) → (7) 램프 tick → 포화 감시(도달 후 새 포화만 클램프, 도달 전은 승온 정체만)
  → 유지 모드 + D118 요청 실행 → 출력 무응답 감시(판정되면 OFF·카드, 상태 noresp) → (8) CSV 행 → (9) 리스너
첫 성공 폴링 전의 '미연결'은 로그를 남기지 않는다(프로그램 시작 직후 PLC 접속 전).
설정·세션 마커가 PLC 에 들어가기 전에는 ON 을 거부한다(is_ready).
워치독 루프: WD_PERIOD 마다 RUN 이고 링크가 살아 있을 때만 D114 = (이전+1) & 0x7FFF.
명령(turn_on/turn_off/apply_target/reset_fault)과 폴링 쓰기는 같은 asyncio.Lock 으로 섞이지 않게 한다.
"""
from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from controller.heater_hold import HeaterHold
from controller.heater_process import itl_missing_text
from controller.heater_ramp import RampProfiler
from controller.heater_response import HeaterResponseGuard
from controller.heater_saturation import HeaterSaturationGuard
from lib.heater_config import HeaterConfig, float_to_words, words_to_float
from lib.heater_logger import HeaterCsvLogger
from lib.heater_profile import ramp_minutes

# ── 주소(XBL-EMTA: 레지스터 = D 번호, 코일 = M 워드×16 + 비트) ──
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
COIL_WATER_OK, COIL_VAC_OK = 657, 658   # M00411 냉각수(WATER_2_SGN) · M00412 진공(M_V_2_인터락) — 인터락(ITL) 조건

# 쓰기 허용 주소(이 밖으로는 절대 쓰지 않는다)
WRITE_REGS = frozenset({REG_SV, REG_SV_LIMIT, REG_WD, REG_MV_LIMIT, REG_RAMP_RATE, REG_HOLDBACK,
                        REG_OT_LIMIT, REG_SLOW_ZONE, REG_SLOW_RATE,
                        REG_KP_LO, REG_KP_HI, REG_TI_LO, REG_TI_HI})
WRITE_COILS = frozenset({COIL_RUN, COIL_RST})

IO_TIMEOUT_S = 6.0            # 모든 PLC 호출 상한
OUT_DEAD_GRACE_SEC = 5.0      # RUN 상승 뒤 출력 죽음 판정 유예
OUT_DEAD_COUNT = 3            # 연속 횟수
MARKER_MISMATCH_N = 2         # 세션 마커 연속 불일치 횟수
SV_LOG_MIN_DELTA_C = 1.0
SV_LOG_MIN_INTERVAL_S = 30.0
WD_FAIL_LOG_INTERVAL_S = 60.0
RESIDUAL_FAIL_LOG_INTERVAL_S = 10.0
PUSH_RETRY_SEC = 30.0           # 설정·마커 쓰기 실패 뒤 다음 시도까지
PUSH_FAIL_LOG_INTERVAL_S = 60.0 # 설정 쓰기 실패 로그 간격
RST_STUCK_POLLS = 2             # RST 코일이 이 횟수 연속 1 이면 0 으로 되돌린다

OWNER_LABEL = {"manual": "수동", "process": "공정", "external": "외부"}
# 종료 카드 ✅ 사유(이 밖의 사유·이상 사건·PLC 이상 플래그가 있으면 ❌)
NORMAL_END_REASONS = frozenset({"정지(패널)", "공정 종료", "공정 중단 — 사용자 STOP", "공정 중단 — 원격 STOP"})
EVENTS_SHOWN_MAX = 4          # 특이사항에 보이는 이상 사건 수(넘으면 " 외 N건")


def _s16(v: int) -> int:
    v = int(v) & 0xFFFF
    return v - 65536 if v >= 32768 else v


class HeaterService:
    def __init__(self, plc: Any, cfg: HeaterConfig, *,
                 log: Callable[[str, str], None],
                 chat: Any = None,
                 clock: Callable[[], float] = time.monotonic):
        self._plc = plc
        self.cfg = cfg
        self._log_cb = log
        self._chat = chat
        self._now = clock
        self._lock = asyncio.Lock()
        self._listeners: List[Callable[[dict], None]] = []
        self._poll_task: Optional[asyncio.Task] = None
        self._wd_task: Optional[asyncio.Task] = None
        self._stopping = False

        self.owner: Optional[str] = None
        self._st: Dict[str, Any] = self._blank_status()
        self._last_ok_t: Optional[float] = None
        self._first_poll = True              # 잔존 RUN/기존 래치 판정용(첫 '성공' 폴링)
        self._pending_off = False
        self._need_push = True
        self._marker: Optional[int] = None
        self._marker_mismatch = 0
        self._verify: Optional[dict] = None  # push 직후 다음 블록에서 확인할 값
        self._verify_warned = False
        self._pid_warned = False
        self._noprog_warned = False
        self._fault_prev = False
        self._fault_off_sent = False
        self._fault_why = ""
        self._out_dead_cnt = 0
        self._out_dead_notified = False
        self._run_prev = False
        self._run_since = 0.0
        self._end_reason: Optional[str] = None
        self._episode_notes: List[str] = []   # 이번 운전(RUN 상승~하강)의 이상 사건 — 같은 라벨은 한 번
        self._proc_ctx: Optional[str] = None  # 공정이 넣은 카드 맥락(공정 "이름" (현재/전체))
        self._run_ctx: Optional[str] = None   # 이번 운전의 카드 맥락(시작 때 정하고 공정이 바꾸면 따라감)
        self._fail_since: Optional[float] = None
        self._wd = 0
        self._wd_fail_log_t = -1e9
        self._write_seq = 0                  # 상태를 바꾸는 PLC 쓰기 횟수(워치독 제외) — 읽는 사이 쓰기가 끼면 그 폴링을 버린다
        self._residual_fail_log_t = -1e9
        self._sv_log_c: Optional[float] = None
        self._sv_log_t = -1e9
        self._csv_last_t = -1e9
        self._mv_requests: List[int] = []
        self._push_retry_at = 0.0
        self._push_fail_n = 0
        self._push_fail_log_t = -1e9
        self._rst_n = 0
        self._reset_pending = False
        self._give_up_seq = 0                # 유지 모드 포기 횟수(공정 대기가 '시작 뒤 포기'를 놓치지 않게)
        self._give_up_text = ""

        self.profiler = RampProfiler(self.status_raw, self._write_sv, self._write_ramp_counts,
                                     lambda m: self._log(m), cfg, clock=clock)
        self.hold = HeaterHold(
            cfg.hold_mode, mv_limit=cfg.mv_limit, mv_min=cfg.mv_min,
            enter_tol_c=cfg.hold_enter_tol, enter_sec=cfg.hold_enter_sec,
            arrive_tol_c=cfg.hold_arrive_tol, drift_pv_c=cfg.hold_drift_pv, drift_mv=cfg.hold_drift_mv,
            clock=clock, request_mv_limit=self._mv_requests.append,
            message=self._on_hold_msg, engaged=self._on_hold_engaged, give_up=self._on_hold_give_up)
        self.sat = HeaterSaturationGuard(
            mv_limit=cfg.mv_limit, mv_min=cfg.mv_min, sat_sec=cfg.sat_sec,
            arrive_tol_c=cfg.hold_arrive_tol, clock=clock,
            request_mv_limit=self._mv_requests.append, message=self._on_hold_msg,
            saturated=self._on_saturated, stalled=self._on_stalled)
        self.resp = HeaterResponseGuard(
            enabled=cfg.noresp_enable, mv_high=cfg.noresp_mv, sec=cfg.noresp_sec, rise_c=cfg.noresp_rise_c,
            loss_sec=cfg.noresp_loss_sec, loss_below_c=cfg.noresp_loss_below_c,
            loss_fall_c=cfg.noresp_loss_fall_c, mv_limit=cfg.mv_limit, clock=clock)
        self.csv = HeaterCsvLogger(cfg.log_dir, warn=lambda m: self._log(f"⚠ {m}"), clock=clock)

    # ═══════════════════════ 공개 API ═══════════════════════
    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """폴링·워치독 태스크를 띄운다. loop 를 주면 그 루프에(MainWindow 는 루프가 돌기 전에 생성된다)."""
        lp = loop
        if lp is None:
            try:
                lp = asyncio.get_running_loop()
            except RuntimeError:
                lp = asyncio.get_event_loop()
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = lp.create_task(self._poll_loop(), name="CH2HeaterPoll")
        if self._wd_task is None or self._wd_task.done():
            self._wd_task = lp.create_task(self._wd_loop(), name="CH2HeaterWD")

    @property
    def clock(self) -> Callable[[], float]:
        """서비스가 쓰는 단조 시계(공정 대기 판정도 같은 시계를 쓴다 — 에뮬레이터 시험에서 가상 시계)."""
        return self._now

    def status(self) -> dict:
        return {k: v for k, v in self._st.items() if not k.startswith("_")}

    def status_raw(self) -> dict:
        """복사 없이 마지막 상태(내부·램프용)."""
        return self._st

    def add_listener(self, cb: Callable[[dict], None]) -> None:
        if cb not in self._listeners:
            self._listeners.append(cb)

    def claim(self, owner: str) -> bool:
        if self.owner in (None, owner):
            self.owner = owner
            return True
        return False

    def release(self, owner: str) -> None:
        if self.owner == owner:
            self.owner = None
            if owner == "process":
                self._proc_ctx = None

    def set_context(self, text: Optional[str]) -> None:
        """구글챗 카드 부제 맥락(공정 "이름" (현재/전체)). None 이면 수동/외부 기본값으로 되돌린다.
        운전 중에 바뀌면(연속 실행에서 다음 공정이 넘겨받음) 이번 운전의 맥락도 따라간다."""
        self._proc_ctx = str(text) if text else None
        if self._proc_ctx and self._run_prev:
            self._run_ctx = self._proc_ctx

    def context(self) -> str:
        """카드 부제 맥락 — 운전 중이면 이번 운전의 맥락, 아니면 공정 맥락 → 제어 주체(수동/외부)."""
        if self._run_prev and self._run_ctx:
            return self._run_ctx
        if self.owner == "process" and self._proc_ctx:
            return self._proc_ctx
        return OWNER_LABEL.get(self.owner or "manual", self.owner or "수동")

    def notify_reached(self, target, pv, took_sec: float, next_step: str = "",
                       soak_sec: float = 0.0, soak_pv: str = "") -> None:
        """공정 소유 히터의 승온 대기 통과 카드 1장 — 이상 사건이 없으면 ✅, 있으면 ❌.
        soak_sec > 0(레시피 heater_soak): '승온 소요'는 SOAK 을 뺀 시간, 'SOAK'·'SOAK 중 PV' 칸을 단다."""
        took = int(max(0.0, float(took_sec or 0.0)))
        soak = int(round(max(0.0, float(soak_sec or 0.0))))
        ramp = max(0, took - soak)
        fields = {"목표": self._fmt(target), "도달 PV": self._fmt(pv), "승온 소요": f"{ramp // 60}분 {ramp % 60}초"}
        if soak > 0:
            fields["SOAK"] = f"{soak // 60}분 {soak % 60}초"
            fields["SOAK 중 PV"] = str(soak_pv)
        if next_step:
            fields["다음 단계"] = str(next_step)
        if self._episode_notes:
            fields["특이사항"] = self._events_text()
        self._card("도달", self.context(), "FAIL" if self._episode_notes else "SUCCESS", fields)

    def is_ready(self) -> Tuple[bool, str]:
        st = self._st
        if not self._link_up():
            return False, "PLC 링크 없음"
        if st.get("stale"):
            return False, f"PLC 응답 없음 ({float(st.get('age_s') or 0):.0f}초 전 값)"
        if st.get("fault"):
            return False, f"히터 이상: {self._fault_text(st)}"
        if not st.get("itl"):
            return False, f"인터락 미충족: {self._itl_missing(st)}"
        if self._need_push:
            return False, "히터 설정·세션 마커 적용 전(PLC 쓰기 대기/실패) — 잠시 후 다시 시도하십시오"
        return True, ""

    def final_target(self) -> Optional[float]:
        t = self.profiler.target()
        if t is not None:
            return t
        return self._st.get("sv")

    def ramp_info(self) -> Tuple[bool, Optional[float], float, Optional[float]]:
        """(활성, 목표, 속도 °C/min, 남은 분)."""
        if not self.profiler.is_active():
            return False, None, 0.0, None
        return True, self.profiler.target(), self.profiler.rate(), self.profiler.remaining_minutes()

    def hold_snapshot(self) -> dict:
        h = self.hold
        return {"mode": h.mode, "state": h.state, "holding": h.is_holding(), "value": h.value,
                "arrived": h.arrived, "gave_up": h.gave_up,
                "give_up_seq": self._give_up_seq, "give_up_text": self._give_up_text}

    # ═══════════════════════ 명령 ═══════════════════════
    async def turn_on(self, target_c: float, rate: Optional[float] = None,
                      owner: str = "manual") -> Tuple[bool, str]:
        async with self._lock:
            ok, why = self.is_ready()
            if not ok:
                return False, why
            st = self._st
            if st.get("run"):
                return False, "이미 운전 중입니다 — 목표를 바꾸려면 [적용]을 쓰십시오"
            ok, why = self._target_ok(target_c)
            if not ok:
                return False, why
            if self.owner not in (None, owner):
                return False, f"제어 주체 충돌 — 현재 {OWNER_LABEL.get(self.owner, self.owner)}"
            target = float(target_c)
            r = float(rate) if rate else float(self.cfg.ramp_rate)
            try:
                started = await self.profiler.start(target, r, OWNER_LABEL.get(owner, owner))
                if not started:
                    await self._write_sv(target, force_log=True)
                await self._w_coil(COIL_RUN, True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await self.profiler.stop(restore_rate=True)
                self._log(f"⚠ 히터 ON 실패: {e!r}")
                return False, f"PLC 쓰기 실패: {e}"
            self.owner = owner
            self._end_reason = None
            self._log(f"운전 ON 요청 — 목표 {target:.1f}°C ({OWNER_LABEL.get(owner, owner)})")
        await self._kick_wd()
        return True, ""

    def _process_locked_out(self, owner: Optional[str]) -> bool:
        """공정이 운전 중인 히터를 공정 밖(수동 등)이 바꾸려 하는가. 수동↔외부 사이는 막지 않는다(패널로 외부 운전을 끌 수 있어야 함)."""
        return (owner is not None and owner != "process" and self.owner == "process"
                and bool(self._st.get("run")))

    async def turn_off(self, reason: str = "정지", owner: Optional[str] = None) -> Tuple[bool, str]:
        async with self._lock:
            if self._process_locked_out(owner):
                return False, "공정이 히터를 제어 중입니다"
            return await self._turn_off_locked(reason)

    def take_over(self, owner: str) -> None:
        """공정이 수동·외부 운전 중인 히터를 넘겨받는다(챔버K 와 같음)."""
        if self.owner not in (None, owner):
            self._log(f"{OWNER_LABEL.get(self.owner, self.owner)} 운전 중인 히터를 {OWNER_LABEL.get(owner, owner)}이(가) 넘겨받음")
        self.owner = owner

    async def set_hold_armable(self, on: bool, why: str = "") -> None:
        """유지 진입·포화 클램프 허용/금지(공정 전용). 금지: holding·클램프를 풀고 D118 을 MV_LIMIT 로 바로 되돌린다.
        허용: 유지 모드가 지금 조건(작업 압력·플라즈마)에서 새로 측정하고, 포화 감시는 지금부터의 이력으로 다시 무장한다."""
        async with self._lock:
            self._mv_requests.clear()
            if not on and self.hold.is_holding():
                self.hold.release(why or "공정이 유지 재측정 요청")
            self.sat.set_clamp_enabled(on, why)
            self.hold.set_armable(on)
            for v in list(self._mv_requests):
                try:
                    await self._write_mv_limit(v)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._log(f"⚠ DAC 상한(D118) 원복 실패: {e!r}")
            self._mv_requests.clear()

    async def force_hold(self, why: str) -> Tuple[bool, str]:
        """공정 유지 진입 대기가 시간 초과일 때 dac 유지를 강제로 건다(챔버K 2단계 강제 진입).
        값: 이번 측정 창 평균(set_armable 뒤 측정분) → 최근 60초 MV 평균 → 현재 MV."""
        async with self._lock:
            st = self._st
            if not st.get("run") or st.get("stale"):
                return False, "히터가 꺼져 있거나 PLC 응답 없음"
            if self.hold.is_holding():
                return True, f"이미 유지 중(DAC 상한 {self.hold.value})"
            recent = self.sat.recent_mv_avg()
            self._mv_requests.clear()
            if not self.hold.force_dac_hold(recent if recent is not None else st.get("mv"), prefer_current=False):
                return False, self.hold.last_force_reason or "강제 진입 실패"
            for v in list(self._mv_requests):
                try:
                    await self._write_mv_limit(v)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._log(f"⚠ DAC 상한(D118) 쓰기 실패: {e!r}")
            self._mv_requests.clear()
            src = self.hold.last_force_source
            self._event("유지 모드 강제 진입")
            self._card("유지 모드 강제 진입", f"{self.context()} · {why} — DAC 상한 {self.hold.value} 고정({src})", "FAIL",
                       {"PV": self._fmt(st.get("pv")), "MV": str(st.get("mv"))})
            return True, f"DAC 상한 {self.hold.value} 고정({src})"

    async def _turn_off_locked(self, reason: str) -> Tuple[bool, str]:
        self._end_reason = reason
        if self.profiler.is_active():
            await self.profiler.stop(restore_rate=True)
        elif self._st.get("ok") is not None and self._st.get("ramp_rate") not in (None, self._ramp_c_per_min()):
            if self._link_up():
                try:
                    await self._write_ramp_counts(self.cfg.rate_counts(self.cfg.ramp_rate))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
        if not self._link_up():
            self._pending_off = True
            self._log("링크 다운 — 히터 OFF 를 보류(복구 시 1회 적용)")
            return True, "링크 다운 — OFF 보류(복구 시 적용)"
        try:
            await self._w_coil(COIL_RUN, False)
            self._pending_off = False
            self._log(f"운전 OFF ({reason})")
            return True, ""
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._pending_off = True
            self._log(f"⚠ 히터 OFF 쓰기 실패 → 보류(다음 폴링에 재시도): {e!r}")
            return True, "쓰기 실패 — OFF 보류(다음 폴링에 재시도)"

    async def apply_target(self, target_c: float, rate: Optional[float] = None,
                           owner: Optional[str] = None) -> Tuple[bool, str]:
        async with self._lock:
            if self._process_locked_out(owner):
                return False, "공정이 히터를 제어 중입니다"
            if not self._link_up():
                return False, "PLC 링크 없음"
            if self._st.get("stale"):
                return False, "PLC 응답 없음"
            ok, why = self._target_ok(target_c)
            if not ok:
                return False, why
            target = float(target_c)
            try:
                if self._st.get("run"):
                    if not rate:
                        rate = self.profiler.rate() if self.profiler.is_active() else float(self.cfg.ramp_rate)
                    await self.profiler.stop(restore_rate=False)
                    if not await self.profiler.start(target, rate, "공정" if owner == "process" else "적용"):
                        await self._write_sv(target, force_log=True)
                else:
                    await self._write_sv(target, force_log=True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                return False, f"PLC 쓰기 실패: {e}"
            return True, ""

    async def reset_fault(self) -> Tuple[bool, str]:
        async with self._lock:
            if self._st.get("run"):
                return False, "히터 RUN 이 켜져 있습니다 — 먼저 OFF 한 뒤 리셋하십시오"
            if not self._link_up():
                return False, "PLC 링크 없음"
            try:
                await self._w_coil(COIL_RST, True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                return False, f"PLC 쓰기 실패: {e}"
            self._log("사용자가 히터 이상 리셋을 요청했습니다(RST)")
            self._reset_pending = True
            return True, ""

    async def shutdown(self, timeout: float = 2.0) -> None:
        """루프 정지 → 링크가 살아 있고 RUN 이면 RUN=0 1회 → CSV 닫기."""
        self._stopping = True
        for t in (self._poll_task, self._wd_task):
            if t is not None and not t.done():
                t.cancel()
        for t in (self._poll_task, self._wd_task):
            if t is not None:
                try:
                    await asyncio.wait_for(asyncio.gather(t, return_exceptions=True), timeout=1.0)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass
        self._poll_task = self._wd_task = None
        if self._st.get("run"):
            done = False
            if self._link_up():
                try:
                    await asyncio.wait_for(self._plc.write_coil(COIL_RUN, False), timeout=float(timeout))
                    done = True
                    self._log("프로그램 종료 — 히터 RUN OFF")
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._log(f"⚠ 종료 시 히터 OFF 실패: {e!r}")
            if not done:
                self._log("⚠ 종료 시 히터 OFF 를 보내지 못함 — PLC 워치독이 30초 뒤 차단")
        try:
            if self.csv.is_open():
                self.csv.write_row(self._st, owner=self.owner or "", note="프로그램 종료")
            self.csv.stop()
        except Exception:
            pass

    # ═══════════════════════ 내부: I/O ═══════════════════════
    def _link_up(self) -> bool:
        try:
            return bool(self._plc.is_connected())
        except Exception:
            return False

    async def _w_reg(self, addr: int, value: int) -> None:
        if addr not in WRITE_REGS:
            raise ValueError(f"히터 쓰기 허용 밖 레지스터 D{addr}")
        if addr != REG_WD:                   # 워치독 갱신은 상태를 바꾸지 않는다
            self._write_seq += 1
        await asyncio.wait_for(self._plc.write_reg(int(addr), int(value) & 0xFFFF), timeout=IO_TIMEOUT_S)

    async def _w_coil(self, addr: int, on: bool) -> None:
        if addr not in WRITE_COILS:
            raise ValueError(f"히터 쓰기 허용 밖 코일 {addr}")
        self._write_seq += 1
        await asyncio.wait_for(self._plc.write_coil(int(addr), bool(on)), timeout=IO_TIMEOUT_S)

    async def _write_sv(self, temp_c: float, force_log: bool = False) -> None:
        raw = self.cfg.c_to_raw(temp_c)
        await self._w_reg(REG_SV, raw)
        now = self._now()
        if (force_log or self._sv_log_c is None or abs(float(temp_c) - self._sv_log_c) >= SV_LOG_MIN_DELTA_C
                or (now - self._sv_log_t) >= SV_LOG_MIN_INTERVAL_S):
            self._sv_log_c = float(temp_c)
            self._sv_log_t = now
            self._log(f"목표(SV) {float(temp_c):.1f}°C 기록 (D112={raw})")

    async def _write_ramp_counts(self, counts: int) -> None:
        v = max(1, min(100, int(counts)))
        await self._w_reg(REG_RAMP_RATE, v)

    async def _write_mv_limit(self, v: int) -> None:
        v = max(self.cfg.mv_min + 20, min(self.cfg.mv_limit, int(v)))
        await self._w_reg(REG_MV_LIMIT, v)

    def _ramp_c_per_min(self) -> float:
        return float(self.cfg.rate_counts(self.cfg.ramp_rate) * 6)

    async def _kick_wd(self) -> None:
        if not self._link_up():
            return
        self._wd = (self._wd + 1) & 0x7FFF
        try:
            await self._w_reg(REG_WD, self._wd)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            now = self._now()
            if (now - self._wd_fail_log_t) >= WD_FAIL_LOG_INTERVAL_S:
                self._wd_fail_log_t = now
                self._log(f"워치독(D114) 쓰기 실패 — 다음 주기에 재시도: {e!r}")

    # ═══════════════════════ 내부: 로그·챗 ═══════════════════════
    def _log(self, msg: str) -> None:
        try:
            self._log_cb("HEATER", msg)
        except Exception:
            pass

    def _card(self, title: str, subtitle: str, status: str, fields: Optional[dict] = None) -> None:
        if self._chat is None:
            return
        try:
            self._chat._post_card(f"[CH2] 히터 {title}", subtitle, status, fields or {}, urgent=True)
        except Exception:
            pass

    def _on_hold_msg(self, level: str, text: str) -> None:
        self._log(("⚠ " if level == "경고" else "") + text)

    def _on_hold_engaged(self, kind: str) -> None:
        v = self.hold.value
        self._log(f"유지 진입 DAC 상한 {v}")     # 정상 기록 — 특이사항에 넣지 않는다

    def _on_hold_give_up(self, why: str) -> None:
        self._give_up_seq += 1
        self._give_up_text = str(why)
        self._event("유지 모드 포기")
        self._card("유지 모드 포기", f"{self.context()} · {why}", "FAIL", {"PV": self._fmt(self._st.get("pv"))})

    def _on_saturated(self, info: dict) -> None:
        clamp = info.get("clamp")
        self._event(f"DAC 포화{'(클램프 ' + str(clamp) + ')' if clamp is not None else ''}")
        self._card("DAC 포화", f"{self.context()} · 도달 후 {float(info.get('sec') or 0):.0f}초째 상한({info.get('limit')})에 물림 → D118 {clamp}",
                   "FAIL", {"PV": self._fmt(info.get("pv")), "MV": str(info.get("mv")),
                            "클램프": "-" if clamp is None else str(clamp)})

    async def _on_no_response(self, st: dict, info: dict) -> None:
        """출력 무응답·끊김 판정(HeaterResponseGuard) — ❌ 카드 → 히터 OFF(종료 사유 = 라벨).
        이번 폴링의 발행 상태 st['noresp'] 에 원인 문구를 실어 공정(ChamberRuntime)이 E832 로 멈추게 한다
        (다음 폴링의 'RUN 꺼짐'으로 일반 문구가 나가기 전에). 수동·외부 운전은 히터만 끈다."""
        label = str(info.get("label") or "출력 무응답")
        detail = str(info.get("text") or "")
        hint = str(info.get("hint") or "")
        st["noresp"] = f"{label} — {detail}"
        self._event(label)
        self._log(f"⚠ 히터 {label}: {detail} → 히터를 끕니다 ({hint})")
        self._card(label, f"{self.context()} · {label} — 배선·VARITAP·차단기 확인", "FAIL",
                   {"판정": detail, "PV": self._fmt(info.get("pv")),
                    "MV": f"{float(info.get('mv') or 0):.0f}/{float(info.get('limit') or 0):.0f}",
                    "확인": hint})
        await self._turn_off_locked(label)

    def _on_stalled(self, info: dict) -> None:
        self._event("승온 정체")
        eta = info.get("eta_min")
        self._card("승온 정체",
                   f"{self.context()} · 목표 {float(info.get('target') or 0):.1f}°C · 출력 상한 {info.get('limit')} 에서 상승 "
                   f"{float(info.get('rate') or 0):.2f}°C/min", "FAIL",
                   {"PV": self._fmt(info.get("pv")), "MV": f"{info.get('mv'):.0f}/{info.get('limit')}",
                    "현재 속도로": "도달 안 됨" if eta is None else f"약 {float(eta):.0f}분",
                    "확인": "HEATER_MV_LIMIT(출력 상한)"})

    @staticmethod
    def _fmt(v) -> str:
        return "--.-" if v is None else f"{float(v):.1f}°C"

    @staticmethod
    def _fault_label(st: dict) -> str:
        """카드 사유·특이사항 라벨 — 종료 카드와 이상 카드가 같은 문구를 쓴다."""
        if st.get("ot"):
            return "이상 — 과온"
        if st.get("tc_err"):
            return "이상 — 온도센서"
        if st.get("wd_err"):
            return "이상 — 통신 워치독"
        return "이상"

    def _event(self, label: str) -> None:
        """이번 운전의 이상 사건(같은 라벨은 한 번) — 종료·도달 카드의 특이사항."""
        if label and label not in self._episode_notes:
            self._episode_notes.append(label)

    def _events_text(self) -> str:
        ev = list(self._episode_notes)
        if not ev:
            return "없음"
        if len(ev) > EVENTS_SHOWN_MAX:
            return " · ".join(ev[:EVENTS_SHOWN_MAX]) + f" 외 {len(ev) - EVENTS_SHOWN_MAX}건"
        return " · ".join(ev)

    @staticmethod
    def _fmt_duration(sec: float) -> str:
        m = int(max(0.0, float(sec)) // 60)
        return f"{m // 60}시간 {m % 60}분" if m >= 60 else f"{m}분"

    @staticmethod
    def _fault_text(st: dict) -> str:
        if st.get("ot"):
            return "과온 트립"
        if st.get("tc_err"):
            return "온도센서 이상(단선/모듈)"
        if st.get("wd_err"):
            return "통신 워치독 타임아웃"
        return f"히터 이상 (PID err={st.get('pid_err')})"

    @staticmethod
    def _itl_missing(st: dict) -> str:
        """인터락이 꺼진 원인 — TC · D/A · 냉각수 · 진공(메인밸브 인터락) · FAULT 중 꺼진 것."""
        return itl_missing_text(st)

    def _target_ok(self, target_c) -> Tuple[bool, str]:
        try:
            t = float(target_c)
        except Exception:
            return False, "목표 온도가 숫자가 아닙니다"
        lim = float(self.cfg.max_temp)
        svl = self._st.get("sv_limit")
        if svl is not None and float(svl) > 0:
            lim = min(lim, float(svl))
        if not (0.0 < t <= lim):
            return False, f"목표 범위 밖 — 0 초과 {lim:.1f}°C 이하"
        return True, ""

    # ═══════════════════════ 내부: 상태 ═══════════════════════
    def _blank_status(self) -> dict:
        return {"ok": None, "pv": None, "sv": None, "sv_limit": None, "cur_sv": None, "pid_err": None,
                "mv": None, "mv_pct": 0.0, "mv_limit": None, "sv_ramp": None, "ramp_rate": None,
                "holdback": None, "ot_limit": None, "slow_zone": None, "slow_rate": None,
                "kp_act": None, "ti_act": None, "marker": None,
                "run": False, "itl": False, "fault": False, "ot": False, "tc_err": False, "wd_err": False,
                "pid_run": False, "dac_en": False, "tc_ok": False, "da_ok": False,
                "water_ok": False, "vac_ok": False, "rst": False,
                "output_dead": False, "noresp": None, "stale": True, "age_s": None, "t": None}

    def _decode(self, regs: list, bits: list, now: float) -> dict:
        c = self.cfg
        sc = float(c.temp_scale)

        def r(addr):
            return int(regs[addr - REG_BLOCK_START])

        def b(addr):
            return bool(bits[addr - COIL_BASE])

        raw_pv = _s16(r(REG_PV))
        tc_bad = raw_pv == -1
        mv = r(REG_MV)
        span = c.mv_abs_max - c.mv_min
        mv_pct = 0.0 if (mv < c.mv_min or span <= 0) else max(0.0, min(100.0, (mv - c.mv_min) / span * 100.0))
        mreg = int(c.session_mark_reg)
        st = {
            "ok": not tc_bad,
            "pv": None if tc_bad else raw_pv * sc,
            "sv": r(REG_SV) * sc,
            "sv_limit": r(REG_SV_LIMIT) * sc,
            "cur_sv": _s16(r(REG_CUR_SV)) * sc,
            "pid_err": r(REG_PID_ERR),
            "mv": mv,
            "mv_pct": mv_pct,
            "mv_limit": r(REG_MV_LIMIT),
            "sv_ramp": _s16(r(REG_SV_RAMP)) * sc,
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
            "noresp": None,                  # 출력 무응답·끊김으로 이번 폴링에 히터를 껐으면 원인 문구(그 폴링에만)
            "stale": False, "age_s": 0.0, "t": now,
            "_raw_pv": raw_pv, "_regs": regs,
        }
        return st

    def _publish(self) -> None:
        snap = {k: v for k, v in self._st.items() if not k.startswith("_")}
        for cb in list(self._listeners):
            try:
                cb(dict(snap))
            except Exception as e:
                self._log(f"상태 리스너 예외: {e!r}")

    def _next_interval(self) -> float:
        st = self._st
        busy = (st.get("run") or self.profiler.is_active() or self._pending_off
                or (self._need_push and not st.get("stale") and self._now() >= self._push_retry_at))
        return float(self.cfg.poll_interval_s if busy else self.cfg.poll_idle_s)

    # ═══════════════════════ 루프 ═══════════════════════
    async def _poll_loop(self) -> None:
        while not self._stopping:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log(f"⚠ 히터 폴링 처리 예외: {e!r}")
            await asyncio.sleep(self._next_interval())

    async def _wd_loop(self) -> None:
        while not self._stopping:
            await asyncio.sleep(float(self.cfg.wd_period_s))
            try:
                if self._st.get("run") and self._link_up():
                    await self._kick_wd()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    def _on_fail(self, why: str, quiet: bool = False) -> None:
        """quiet: 첫 성공 폴링 전의 미연결 — 로그·실패 시각 없이 stale·발행만."""
        now = self._now()
        if self._fail_since is None and not quiet:
            self._fail_since = now
            self._log(f"⚠ 히터 상태 읽기 실패: {why}")
        st = self._st
        age = None if self._last_ok_t is None else now - self._last_ok_t
        st["age_s"] = age
        st["stale"] = age is None or age > float(self.cfg.stale_sec)
        self._publish()

    async def _poll_once(self) -> None:
        if not self._link_up():
            self._on_fail("PLC 미연결", quiet=self._last_ok_t is None)
            return
        seq0 = self._write_seq               # 읽기 시작 시점의 쓰기 횟수(잠금을 잡은 뒤 비교)
        try:
            regs = await asyncio.wait_for(
                self._plc.read_regs_block(REG_BLOCK_START, REG_BLOCK_COUNT, priority="low"), timeout=IO_TIMEOUT_S)
            bits = await asyncio.wait_for(
                self._plc.read_coils_block(COIL_BASE, COIL_COUNT, priority="low"), timeout=IO_TIMEOUT_S)
            if len(regs) < REG_BLOCK_COUNT or len(bits) < COIL_COUNT:
                raise ValueError(f"응답 길이 부족 regs={len(regs)} bits={len(bits)}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._on_fail(repr(e))
            return
        now = self._now()
        if self._fail_since is not None:
            self._log(f"히터 상태 읽기 복구 (끊긴 {now - self._fail_since:.0f}초)")
            self._fail_since = None
        st = self._decode(regs, bits, now)
        async with self._lock:
            if self._write_seq != seq0:
                # 읽는 동안(잠금 밖) 켜기·끄기·목표 변경 같은 쓰기가 끼었다 — 이 값은 쓰기 전 상태라 확정·발행하지 않는다.
                # (예: 공정이 히터를 켠 직후 옛 RUN=0 이 발행돼 '공정 중 히터 꺼짐' E832 로 잘못 멈추던 경합) 다음 폴링이 새로 읽는다.
                return
            if not await self._process(st, now):
                return                       # 잔존 RUN 정리 실패 — 발행하지 않고 다음 폴링에 재시도
        self._last_ok_t = now
        self._publish()

    async def _process(self, st: dict, now: float) -> bool:
        first = self._first_poll
        # (1) 잔존 RUN
        if first and st["run"]:
            if not await self._clear_residual_run(st):
                return False
        # (2) 보류된 OFF
        if self._pending_off:
            try:
                await self._w_coil(COIL_RUN, False)
                self._pending_off = False
                self._log("보류했던 히터 OFF 적용")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log(f"⚠ 보류 OFF 적용 실패 — 다음 폴링에 재시도: {e!r}")
        # (3) 세션 마커·설정
        await self._marker_and_push(st)
        self._check_program(st)
        # (4) FAULT 엣지
        await self._fault_edges(st, first)
        await self._rst_check(st)
        if self._reset_pending:
            self._reset_pending = False
            if st["fault"]:
                self._log(f"⚠ 리셋 후에도 히터 이상 유지 — {self._fault_text(st)}: 원인이 남아 있습니다")
        # (5) 출력 죽음 (RUN 상승 시각은 여기서 잡는다)
        rise = bool(st["run"]) and not self._run_prev
        fall = (not st["run"]) and self._run_prev
        if rise:
            self._run_since = now
        self._output_dead(st, now)
        # 상태 확정(램프·유지가 읽는다)
        self._st = st
        # (6) RUN 엣지
        if rise:
            self._on_run_rise(st)
        elif fall:
            await self._on_run_fall(st, now)
        self._run_prev = bool(st["run"])
        # (7) 램프 → 포화 → 유지 + D118 요청
        if self.profiler.is_active():
            await self.profiler.tick()
        self._mv_requests.clear()
        self.sat.tick(st, self.hold.is_holding(), "dac", final_target=self.final_target(),
                      ramp_active=self.profiler.is_active())
        self.hold.tick(st, self.final_target())
        for v in list(self._mv_requests):
            try:
                await self._write_mv_limit(v)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log(f"⚠ DAC 상한(D118) 쓰기 실패: {e!r}")
        self._mv_requests.clear()
        # 출력 무응답 감시 — MV 를 올려도 온도가 반응하지 않으면 끈다(EL-092)
        trip = self.resp.tick(st, self.final_target(), holding=self.hold.is_holding())
        if trip is not None:
            await self._on_no_response(st, trip)
        # (8) CSV
        if st["run"] and self.cfg.log_enabled and (now - self._csv_last_t) >= float(self.cfg.log_period_s):
            self._csv_last_t = now
            self.csv.write_row(st, owner=OWNER_LABEL.get(self.owner, self.owner or ""),
                               hold=self.hold.log_tuple(), sat_clamp=self.sat.clamped,
                               ramp_target=self.profiler.target())
        self._first_poll = False
        return True

    async def _clear_residual_run(self, st: dict) -> bool:
        """이전 세션 잔존 RUN: RUN=0 → (TC 정상이면) SV=PV. 실패하면 False(발행하지 않고 재시도)."""
        try:
            await self._w_coil(COIL_RUN, False)
            if st["pv"] is not None:
                await self._w_reg(REG_SV, max(0, min(32767, int(st["_raw_pv"]))))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            now = self._now()
            if (now - self._residual_fail_log_t) >= RESIDUAL_FAIL_LOG_INTERVAL_S:
                self._residual_fail_log_t = now
                self._log(f"⚠ 이전 세션 히터 RUN 정리 실패 — 재시도: {e!r}")
            return False
        sv_old = float(st.get("sv") or 0.0)
        pv = st.get("pv")
        st["run"] = False
        st["residual_run"] = True
        if pv is not None:
            st["sv"] = float(pv)
        txt = (f"이전 세션 히터 RUN 잔존 → OFF, SV {sv_old:.1f}°C → "
               f"{('현재 온도 %.1f°C' % pv) if pv is not None else 'PV 무효(SV 유지)'}")
        self._log(txt)
        if self._run_prev:
            self._event("잔존 RUN 정리")
        self._card("잔존 RUN 정리", txt, "WARN", {"PV": self._fmt(pv), "이전 SV": f"{sv_old:.1f}°C"})
        return True

    async def _marker_and_push(self, st: dict) -> None:
        mreg = int(self.cfg.session_mark_reg)
        # 직전 push 의 되읽기 확인(이번 블록에 반영돼 있어야 한다)
        if self._verify is not None:
            self._check_verify(st)
        if self._need_push or (mreg and self._marker is None):
            if self._now() < self._push_retry_at:
                return                       # 실패 백오프 중
            await self._push_and_mark(st)
            return
        if mreg and self._marker is not None:
            v = st.get("marker")
            if v == self._marker:
                self._marker_mismatch = 0
                return
            self._marker_mismatch += 1
            if self._marker_mismatch < MARKER_MISMATCH_N:
                return
            why = (f"PLC 재기동/메모리 초기화 감지 — 세션 마커 D{mreg} 기대 0x{self._marker:04X}, "
                   f"읽은 값 0x{int(v or 0):04X} ({MARKER_MISMATCH_N}회 연속)")
            self._log(f"⚠ {why} → 설정 재적용, 램프 정지")
            if self._run_prev:
                self._event("PLC 재기동")
            self._card("PLC 재기동 감지", why, "WARN", {"PV": self._fmt(st.get("pv"))})
            self._marker = None
            self._marker_mismatch = 0
            if self.profiler.is_active():
                await self.profiler.stop(restore_rate=False)
            if self._run_prev:
                self._end_reason = "PLC 재기동"
            self.owner = None
            self._need_push = True
            self._push_retry_at = 0.0            # 재기동 감지 — 백오프 무시하고 바로 push
            await self._push_and_mark(st)

    async def _push_and_mark(self, st: dict) -> None:
        c = self.cfg
        ok = True
        if c.push_config:
            ok = await self._push_config(st)
        if not ok:
            return                           # 실패 → _push_failed 가 백오프(마커도 쓰지 않음)
        mreg = int(c.session_mark_reg)
        if mreg:
            m = random.randint(0x1000, 0xFFFE)
            try:
                await asyncio.wait_for(self._plc.write_reg(mreg, m), timeout=IO_TIMEOUT_S)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._push_failed(f"세션 마커 D{mreg}: {e!r}")
                return
            self._marker = m
            self._marker_mismatch = 0
        self._need_push = False
        if self._push_fail_n:
            self._log(f"히터 설정 쓰기 복구 ({self._push_fail_n}회 실패 뒤)")
        self._push_fail_n = 0
        self._push_retry_at = 0.0

    def _push_failed(self, what: str) -> None:
        """설정·마커 쓰기 실패 — 30초 백오프, 로그는 60초에 1줄."""
        now = self._now()
        self._push_fail_n += 1
        self._push_retry_at = now + PUSH_RETRY_SEC
        if (now - self._push_fail_log_t) >= PUSH_FAIL_LOG_INTERVAL_S:
            self._push_fail_log_t = now
            self._log(f"⚠ 히터 설정 쓰기 실패({self._push_fail_n}회째) — {PUSH_RETRY_SEC:.0f}초 뒤 재시도: {what}")

    async def _push_config(self, st: dict) -> bool:
        """D113 → D126 → D118 → D120 → D121 → D127 → D128 → Kp(D143→D142) → Ti(D145→D144). 2워드는 상위 먼저."""
        c = self.cfg
        items = [
            (REG_SV_LIMIT, c.c_to_raw(c.sv_limit_c), "SV상한"),
            (REG_OT_LIMIT, c.c_to_raw(c.ot_limit_c), "OT"),
            (REG_MV_LIMIT, int(c.mv_limit), "DAC상한"),
            (REG_RAMP_RATE, c.rate_counts(c.ramp_rate), "램프"),
            (REG_HOLDBACK, c.c_to_raw(c.holdback_c), "홀드백"),
            (REG_SLOW_ZONE, c.c_to_raw(c.slow_zone_c), "감속구간"),
            (REG_SLOW_RATE, c.rate_counts(c.slow_rate), "감속램프"),
        ]
        expect: Dict[int, int] = {}
        for addr, raw, name in items:
            try:
                await self._w_reg(addr, raw)
                expect[addr] = raw
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # 첫 실패에서 멈춘다 — 링크가 반쯤 죽었을 때 6초×11 동안 잠금을 잡지 않게
                self._push_failed(f"{name}(D{addr}): {e!r}")
                return False
        pid = {}
        for valid, val, lo, hi, act_key, name in ((c.kp_valid, c.kp, REG_KP_LO, REG_KP_HI, "kp_act", "Kp"),
                                                   (c.ti_valid, c.ti_s, REG_TI_LO, REG_TI_HI, "ti_act", "Ti")):
            if not valid:
                continue
            pid[act_key] = float(val)
            act = st.get(act_key)
            if act is not None and abs(float(act) - float(val)) <= max(1e-6, abs(float(val)) * 1e-5):
                continue                     # 이미 같은 값 — 다시 쓰지 않는다
            w_lo, w_hi = float_to_words(val)
            try:
                await self._w_reg(hi, w_hi)  # 상위 워드 먼저 — 중간 상태가 새 값의 ±1% 안
                await self._w_reg(lo, w_lo)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._push_failed(f"{name}(D{lo}/D{hi}): {e!r}")
                return False
        self._verify = {"regs": expect, "pid": pid}
        self._verify_warned = False
        self._pid_warned = False
        self._log(
            f"히터 설정 적용: SV상한 {c.sv_limit_c:.1f}°C · OT {c.ot_limit_c:.1f}°C · DAC상한 {c.mv_limit}"
            f" · 램프 {c.rate_counts(c.ramp_rate) * 6}°C/min · 홀드백 {c.holdback_c:.1f}°C"
            f" · 감속 {c.slow_zone_c:.1f}°C/{c.rate_counts(c.slow_rate) * 6}°C/min"
            f" · Kp {c.kp:g}{'' if c.kp_valid else '(미적용)'} · Ti {c.ti_s:g}s{'' if c.ti_valid else '(미적용)'}")
        return True

    def _check_verify(self, st: dict) -> None:
        v = self._verify or {}
        self._verify = None
        regs = st.get("_regs") or []
        mism = []
        for addr, raw in (v.get("regs") or {}).items():
            try:
                got = int(regs[addr - REG_BLOCK_START])
            except Exception:
                continue
            if got != int(raw):
                mism.append(f"D{addr} 요청 {raw} → 실제 {got}")
        if mism and not self._verify_warned:
            self._verify_warned = True
            txt = "설정값이 되읽기와 다릅니다 (래더가 강제 중일 수 있음): " + ", ".join(mism)
            self._log("⚠ " + txt)
            if self._run_prev:
                self._event("설정 되읽기 불일치")
            self._card("설정 되읽기 불일치", txt, "WARN")
        pmism = []
        for key, want in (v.get("pid") or {}).items():
            act = st.get(key)
            if act is None or abs(float(act) - float(want)) > max(1e-6, abs(float(want)) * 1e-3):
                pmism.append(f"{'Kp' if key == 'kp_act' else 'Ti'} 요청 {want:g} → 적용 {act if act is None else round(float(act), 4)}")
        if pmism and not self._pid_warned:
            self._pid_warned = True
            txt = "PID 값이 적용되지 않음(범위·워드 순서 확인, 래더 기본값 사용 중): " + ", ".join(pmism)
            self._log("⚠ " + txt)
            if self._run_prev:
                self._event("설정 되읽기 불일치")
            self._card("설정 되읽기 불일치", txt, "WARN")

    def _check_program(self, st: dict) -> None:
        if self._noprog_warned:
            return
        kp = st.get("kp_act")
        if not st.get("tc_ok") and not st.get("da_ok") and (kp is None or float(kp) == 0.0):
            self._noprog_warned = True
            self._log("⚠ PLC 에 HEATER 프로그램이 없는 것 같음 (TC1_OK·DA_OK 꺼짐, KP_ACT=0)")

    async def _fault_edges(self, st: dict, first: bool) -> None:
        if st["fault"] and not self._fault_prev:
            why = self._fault_text(st)
            if first:
                why = f"{why} — 기존 래치(프로그램 시작 시 확인)"
            self._fault_why = why
            if st["run"] and not self._fault_off_sent:
                self._fault_off_sent = True
                try:
                    await self._w_coil(COIL_RUN, False)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._pending_off = True
                    self._log(f"⚠ 이상 시 RUN OFF 쓰기 실패 → 보류: {e!r}")
            if self.profiler.is_active():
                await self.profiler.stop(restore_rate=False)
            label = self._fault_label(st)
            ctx = self.context()
            if self._run_prev or st["run"]:
                self._event(label)
            self._end_reason = label
            self.owner = None
            self._log(f"⚠ 히터 이상: {why}")
            self._card("이상", f"{ctx} · {label}", "FAIL",
                       {"상세": why, "PV": self._fmt(st.get("pv")), "PID err": str(st.get("pid_err"))})
        elif (not st["fault"]) and self._fault_prev:
            self._log("히터 이상 해제됨 (정상 복귀)")
            self._fault_off_sent = False
        self._fault_prev = bool(st["fault"])

    async def _rst_check(self, st: dict) -> None:
        """RST(M00403)가 RST_STUCK_POLLS 회 연속 1 이면 0 으로 되돌린다 — 래더가 스스로 0 으로 되돌리지 않으면
        래치가 매 스캔 풀려 이상이 잡히지 않는다."""
        if not st.get("rst"):
            self._rst_n = 0
            return
        self._rst_n += 1
        if self._rst_n < RST_STUCK_POLLS:
            return
        try:
            await self._w_coil(COIL_RST, False)
            self._rst_n = 0
            self._log("⚠ 히터 리셋 코일(M00403)이 1 로 남아 있어 0 으로 되돌림 — 래더의 RST 자기 리셋 확인 필요")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._log(f"⚠ 히터 리셋 코일(M00403) 0 쓰기 실패 — 다음 폴링에 재시도: {e!r}")

    def _output_dead(self, st: dict, now: float) -> None:
        cond = bool(st["run"] and st["itl"] and not st["fault"] and st.get("mv") is not None
                    and int(st["mv"]) < int(self.cfg.mv_min))
        st["output_dead"] = cond
        if cond:
            self._out_dead_cnt += 1
            grace = self._run_since > 0.0 and (now - self._run_since) >= OUT_DEAD_GRACE_SEC
            if self._out_dead_cnt >= OUT_DEAD_COUNT and grace and not self._out_dead_notified:
                self._out_dead_notified = True
                self._event("출력 죽음")
                txt = f"운전 중인데 DAC 출력이 최소치 미만입니다 (MV {st['mv']} < {self.cfg.mv_min}, PID 에러 D117={st['pid_err']})"
                self._log("⚠ " + txt)
                self._card("출력 죽음", f"{self.context()} · {txt}", "FAIL", {"PV": self._fmt(st.get("pv"))})
        else:
            self._out_dead_cnt = 0
            self._out_dead_notified = False

    def _on_run_rise(self, st: dict) -> None:
        if self.owner is None:
            self.owner = "external"
        c = self.cfg
        path = self.csv.start() if c.log_enabled else None
        self._csv_last_t = -1e9
        tgt = self.profiler.target()
        tgt_txt = f"{tgt:.1f}°C(램프)" if tgt is not None else self._fmt(st.get("sv"))
        ramp_txt = f"{self.profiler.rate():g}°C/min" if self.profiler.is_active() else "없음(래더 램프만)"
        txt = (f"운전 시작 ({OWNER_LABEL.get(self.owner, self.owner)}) — 목표 {tgt_txt} · 램프 {ramp_txt}"
               f" · 래더 램프 {st.get('ramp_rate')}°C/min · 홀드백 {self._fmt(st.get('holdback'))}"
               f" · SV상한 {self._fmt(st.get('sv_limit'))} · OT {self._fmt(st.get('ot_limit'))}"
               f" · DAC상한 {st.get('mv_limit')} · Kp/Ti 적용 {self._num(st.get('kp_act'))}/{self._num(st.get('ti_act'))}"
               f" · 유지 모드 {c.hold_mode} · CSV {path if path else '없음'}")
        self._log(txt)
        # 이번 운전의 맥락·이상 사건을 새로 시작한다
        self._episode_notes = []
        self._run_ctx = (self._proc_ctx if (self.owner == "process" and self._proc_ctx)
                         else OWNER_LABEL.get(self.owner, self.owner))
        fields = {"목표": self._fmt(self.final_target()), "램프": ramp_txt, "현재 PV": self._fmt(st.get("pv"))}
        if c.hold_mode == "dac":
            fields["유지 모드"] = "dac"
        self._card("시작", self._run_ctx, "INFO", fields)

    @staticmethod
    def _num(v) -> str:
        return "-" if v is None else f"{float(v):g}"

    async def _on_run_fall(self, st: dict, now: float) -> None:
        try:
            if self.csv.is_open():
                self.csv.write_row(st, owner=OWNER_LABEL.get(self.owner, self.owner or ""),
                                   hold=self.hold.log_tuple(), sat_clamp=self.sat.clamped, note="운전 종료")
            self.csv.stop()
        except Exception:
            pass
        if self.profiler.is_active():
            await self.profiler.stop(restore_rate=True)
        elif st.get("ramp_rate") is not None and float(st["ramp_rate"]) != self._ramp_c_per_min():
            try:
                await self._write_ramp_counts(self.cfg.rate_counts(self.cfg.ramp_rate))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log(f"⚠ 래더 램프 속도 원복 실패: {e!r}")
        dur = (now - self._run_since) if self._run_since > 0 else 0.0
        plc_bad = any(st.get(k) for k in ("fault", "ot", "tc_err", "wd_err"))
        if plc_bad:
            reason = self._fault_label(st)
        elif self._end_reason:
            reason = self._end_reason
        elif self._marker_mismatch > 0:
            reason = "PLC 재기동 의심(세션 마커 불일치)"
        elif not st.get("itl") and not st.get("fault"):
            # 래더 H9: 운전 중 인터락(ITL)이 풀리면 PLC 가 RUN 을 리셋한다 — 인터락이 돌아와도 저절로 다시 켜지지 않는다
            reason = f"인터락 해제({itl_missing_text(st)}) — PLC 가 RUN 을 끔"
        else:
            # 인터락이 폴링 사이에 잠깐 풀렸다 돌아와도 RUN 은 꺼진 채로 남는다(H9) — 그 경우도 여기로 온다
            reason = "외부 OFF 또는 순간 인터락 해제(이 프로그램이 끄지 않음)"
        if reason not in NORMAL_END_REASONS:
            self._event(reason)              # 비정상 종료 사유는 특이사항에도 같은 라벨로
        ok = (not plc_bad) and (not self._episode_notes) and reason in NORMAL_END_REASONS
        notes = self._events_text()
        ctx = self._run_ctx or OWNER_LABEL.get(self.owner, self.owner or "-")
        self._log(f"운전 종료 — 사유 {reason} · 마지막 PV {self._fmt(st.get('pv'))} · 운전 {dur / 60:.1f}분 · 특이사항 {notes}")
        self._card("종료", f"{ctx} · {reason}", "SUCCESS" if ok else "FAIL",
                   {"마지막 PV": self._fmt(st.get("pv")), "운전 시간": self._fmt_duration(dur), "사유": reason,
                    "특이사항": notes})
        self.owner = None
        self._end_reason = None
        self._episode_notes = []
        self._run_ctx = None
        self._proc_ctx = None
        self._run_since = 0.0
        self.hold.set_armable(True)
        self.sat.set_clamp_enabled(True)
