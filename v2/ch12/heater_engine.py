"""CH2 히터 엔진 — v1 controller/heater_service.py(HeaterService) 의 폴링·판정·설정 push 를 v2 방식(1초 tick + PLC worker)으로.

호출(조립 system 이 연결)
- on_tick_start(tick): 워치독 하트비트 시각(HEATER_WD_PERIOD_S, 5 s) — RUN 이고 링크가 있을 때만 D114 = (이전+1) & 0x7FFF.
- on_row(view): 1초 행마다. v1 폴링 주기 그대로(HEATER_POLL_INTERVAL_S 1 s — RUN·램프·보류 OFF·push 대기, 아니면 POLL_IDLE_S 3 s)
  가 되면 PLC 1초 묶음 읽기의 히터 원시값(드라이버 take())으로 '폴링 1회'를 PLC worker 요청 하나로 처리한다
  (v1 asyncio.Lock 아래의 처리 = worker 안에서 차례로 — 명령과 섞이지 않는다).
성공 폴링 처리 순서(heater_service.py:10-14·789-857 그대로)
  (1) 첫 성공 폴링의 잔존 RUN 정리 → (2) 보류 OFF → (3) 되읽기 확인·세션 마커·설정 push(실패는 첫 쓰기에서 멈추고 30 s 백오프)
  → HEATER 프로그램 유무 → (4) FAULT 엣지 → RST 고착 → 리셋 결과 → (5) 출력 죽음 → 상태 확정 → (6) RUN 엣지(CSV·카드·종료 사유)
  → (7) 램프 tick → 포화 감시 → 유지 tick → D118 요청 실행 → 출력 무응답 감시(판정되면 끄고 상태 noresp) → (8) CSV 행 → 발행.
쓰기-읽기 경합(HEAT-014): 읽은 순간의 쓰기 횟수(HeaterRead.seq)와 처리 순간이 다르면 그 읽기는 쓰기 전 값 — 확정·발행하지 않는다.
  읽기 실패 때 마지막 확정 상태 뒤에 쓰기가 있었으면 stale 전(age ≤ HEATER_STALE_SEC)에는 다시 발행하지 않는다
  (옛 RUN=0 으로 HEATER_WAIT E830·공정 중 감시 E832 오판 방지 — 2026-10-26 검토).
첫 성공 폴링 전의 'PLC 미연결'은 로그를 남기지 않는다. 설정·세션 마커가 PLC 에 들어가기 전에는 ON 을 거부한다(is_ready).
나눔: 설정 push·세션 마커는 heater_push(HeaterPush), 잔존 RUN·이상·RUN 엣지는 heater_edges(HeaterEdges), 명령은 heater_commands,
공정 연동·저널(state_for_journal·resume_checks·adopt_journal — 이어갈 때 잔존 RUN 정리를 하지 않는다)은 heater_link,
로그·카드 문구는 heater_notify. 판정 모듈은 heater_ramp·heater_hold·heater_saturation·heater_response·heater_process.
잠금은 ClockLock(시계 대기로 기다림) — status()·snapshot_fields() 는 잠금 없이 읽는다(시계 대기 조건식 안에서도 부를 수 있게).
"""
from __future__ import annotations

import random
import threading
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.common.worker import Status
from v2.ch12.drivers import heater as H
from v2.ch12.drivers.heater import HeaterRead, HeaterReadFail, decode_block
from v2.ch12.heater_commands import HeaterCommands
from v2.ch12.heater_edges import HeaterEdges
from v2.ch12.heater_hold import HeaterHold
from v2.ch12.heater_link import HeaterProcessLink
from v2.ch12.heater_notify import OWNER_LABEL, HeaterNotify
from v2.ch12.heater_profile import load_heater_cfg
from v2.ch12.heater_push import HeaterPush
from v2.ch12.heater_ramp import RampProfiler
from v2.ch12.heater_response import HeaterResponseGuard
from v2.ch12.heater_saturation import HeaterSaturationGuard

SV_LOG_MIN_DELTA_C = 1.0
SV_LOG_MIN_INTERVAL_S = 30.0
WD_FAIL_LOG_INTERVAL_S = 60.0
NO_READ_FAIL_S = 2.0          # 연결돼 있는데 새 읽기가 이만큼 없으면 읽기 실패로 본다(v1 은 폴링이 직접 읽었다)


class ClockLock:
    """재진입 잠금 — 잠금 대기를 시계 대기(clock.wait_until)로 한다. 엔진 잠금은 PLC worker 가 쓰기(통신 대기) 중에도 잡고 있으므로,
    가상 시계에서 다른 참가자 스레드(tick·공정)가 보통 잠금으로 막히면 시계가 멈춘다 — 그래서 '기다리는 중'으로 세게 한다."""

    def __init__(self, clock) -> None:
        self.clock = clock
        self._owner: Optional[int] = None
        self._n = 0

    def acquire(self) -> bool:
        me = threading.get_ident()
        cond = self.clock.cond
        with cond:
            if self._owner == me:
                self._n += 1
                return True
            if self._owner is not None:
                try:
                    self.clock.wait_until(None, lambda: self._owner is None)
                except RuntimeError:          # 가상 시계에 등록 안 된 스레드 — 조건 변수로만 기다린다
                    while self._owner is not None:
                        cond.wait(0.05)
            self._owner, self._n = me, 1
            return True

    def release(self) -> None:
        cond = self.clock.cond
        with cond:
            self._n -= 1
            if self._n <= 0:
                self._owner, self._n = None, 0
                cond.notify_all()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc) -> bool:
        self.release()
        return False


class HeaterEngine(HeaterNotify, HeaterPush, HeaterEdges, HeaterCommands, HeaterProcessLink):
    def __init__(self, clock, drv, cfg: Dict[str, Any], on_event: Optional[Callable] = None, csv=None,
                 chamber: str = "ch2", rng: Optional[random.Random] = None) -> None:
        self.clock, self.drv, self.cfg_dict = clock, drv, cfg
        self.chamber = chamber
        self.on_event = on_event
        self.events: "deque" = deque(maxlen=20000)
        self.cfg, self.cfg_warns = load_heater_cfg(cfg, chamber.upper() + "_")
        self._now = clock.monotonic
        self._lock = ClockLock(clock) if hasattr(clock, "cond") else threading.RLock()
        self._s = None                       # 지금 실행 중인 worker 요청의 세션(쓰기는 이것으로만)
        self._after: List[Callable[[], None]] = []
        self.rng = rng or random.Random()
        self.csv = csv
        self._listeners: List[Callable[[dict], None]] = []
        self._stopping = False
        self._poll_busy = False
        self._poll_t0 = 0.0
        self._next_poll_t = 0.0
        self._wd_next: Optional[float] = None
        self._last_event_t: Optional[float] = None
        self.owner: Optional[str] = None
        self._st: Dict[str, Any] = self._blank_status()
        self._last_ok_t: Optional[float] = None
        self._first_poll = True
        self._skip_residual = False
        self._pending_off = False
        self._need_push = True
        self._marker: Optional[int] = None
        self._marker_mismatch = 0
        self._verify: Optional[dict] = None
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
        self._episode_notes: List[str] = []
        self._proc_ctx: Optional[str] = None
        self._run_ctx: Optional[str] = None
        self._fail_since: Optional[float] = None
        self._wd = 0
        self._wd_fail_log_t = -1e9
        self._st_seq = 0
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
        self._give_up_seq = 0
        self._give_up_text = ""
        c = self.cfg
        self.profiler = RampProfiler(self.status_raw, self._write_sv, self._write_ramp_counts,
                                     lambda m: self._log(m), c, clock=self._now)
        self.hold = HeaterHold(
            c.hold_mode, mv_limit=c.mv_limit, mv_min=c.mv_min,
            enter_tol_c=c.hold_enter_tol, enter_sec=c.hold_enter_sec,
            arrive_tol_c=c.hold_arrive_tol, drift_pv_c=c.hold_drift_pv, drift_mv=c.hold_drift_mv,
            clock=self._now, request_mv_limit=self._mv_requests.append,
            message=self._on_hold_msg, engaged=self._on_hold_engaged, give_up=self._on_hold_give_up)
        self.sat = HeaterSaturationGuard(
            mv_limit=c.mv_limit, mv_min=c.mv_min, sat_sec=c.sat_sec, arrive_tol_c=c.hold_arrive_tol, clock=self._now,
            request_mv_limit=self._mv_requests.append, message=self._on_hold_msg,
            saturated=self._on_saturated, stalled=self._on_stalled)
        self.resp = HeaterResponseGuard(
            enabled=c.noresp_enable, mv_high=c.noresp_mv, sec=c.noresp_sec, rise_c=c.noresp_rise_c,
            loss_sec=c.noresp_loss_sec, loss_below_c=c.noresp_loss_below_c,
            loss_fall_c=c.noresp_loss_fall_c, mv_limit=c.mv_limit, clock=self._now)
        self._link_init()

    # ═══════════════════════ 조회 ═══════════════════════
    def start(self) -> None:
        """조립이 붙인 뒤 1회 — v1 main._init_ch2_heater·attach_heater 로그(설정 경고 → 연동 준비 → 서비스 시작)."""
        for w in self.cfg_warns:
            self._log(f"⚠ 히터 설정: {w}")
        self._log("CH2 히터 공정 연동 준비(레시피 use_heater)")
        self._log("CH2 히터 서비스 시작")

    @property
    def writes_allowed(self) -> bool:
        return bool(self.drv.writes_allowed)

    def status(self) -> dict:
        """마지막 확정 상태(잠금 없이 — 어느 스레드·시계 대기 조건식에서도 부를 수 있다. 키 집합은 바뀌지 않는다)."""
        return {k: v for k, v in dict(self._st).items() if not k.startswith("_")}

    def status_raw(self) -> dict:
        return self._st

    def add_listener(self, cb: Callable[[dict], None]) -> None:
        if cb not in self._listeners:
            self._listeners.append(cb)

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

    @property
    def busy(self) -> bool:
        """설정 적용 idle 판단용 — RUN·램프·보류 OFF·공정 연동(대기·감시) 중."""
        return bool(self._st.get("run") or self.profiler.is_active() or self._pending_off
                    or self._job is not None or self._watch or self.owner == "process")

    def snapshot_fields(self) -> Dict[str, Any]:
        """1초 행 엔진 열(drivers/heater.py _ENGINE)."""
        b = f"{self.chamber}.heater"
        ok, _why = self.is_ready()                 # 읽기만(잠금 없이 — tick 스레드가 worker 쓰기를 기다리지 않게)
        return {f"{b}.owner": self.owner, f"{b}.target": self.final_target(),
                f"{b}.ramp_active": self.profiler.is_active(),
                f"{b}.ramp_set_rate": self.profiler.rate() if self.profiler.is_active() else None,
                f"{b}.hold": self.hold.state, f"{b}.hold_value": self.hold.value if self.hold.is_holding() else None,
                f"{b}.sat_clamp": self.sat.clamped, f"{b}.phase": self.phase(), f"{b}.ready": ok,
                f"{b}.stale": bool(self._st.get("stale"))}

    # ═══════════════════════ 내부: I/O ═══════════════════════
    def _link_up(self) -> bool:
        return self.drv.connected

    def _w_reg(self, addr: int, value: int) -> None:
        self.drv.w_reg(self._s, addr, value)

    def _w_coil(self, addr: int, on: bool) -> None:
        self.drv.w_coil(self._s, addr, on)

    def _write_sv(self, temp_c: float, force_log: bool = False) -> None:
        raw = self.cfg.c_to_raw(temp_c)
        self._w_reg(H.REG_SV, raw)
        now = self._now()
        if (force_log or self._sv_log_c is None or abs(float(temp_c) - self._sv_log_c) >= SV_LOG_MIN_DELTA_C
                or (now - self._sv_log_t) >= SV_LOG_MIN_INTERVAL_S):
            self._sv_log_c = float(temp_c)
            self._sv_log_t = now
            self._log(f"목표(SV) {float(temp_c):.1f}°C 기록 (D112={raw})")

    def _write_ramp_counts(self, counts: int) -> None:
        self._w_reg(H.REG_RAMP_RATE, max(1, min(100, int(counts))))

    def _write_mv_limit(self, v: int) -> None:
        self._w_reg(H.REG_MV_LIMIT, max(self.cfg.mv_min + 20, min(self.cfg.mv_limit, int(v))))

    def _ramp_c_per_min(self) -> float:
        return float(self.cfg.rate_counts(self.cfg.ramp_rate) * 6)

    def _kick_wd(self) -> None:
        if not self._link_up():
            return
        self._wd = (self._wd + 1) & 0x7FFF
        try:
            self._w_reg(H.REG_WD, self._wd)
        except Exception as e:
            now = self._now()
            if (now - self._wd_fail_log_t) >= WD_FAIL_LOG_INTERVAL_S:
                self._wd_fail_log_t = now
                self._log(f"워치독(D114) 쓰기 실패 — 다음 주기에 재시도: {e!r}")

    def _run_mv_requests(self, fail_word: str = "쓰기") -> None:
        for v in list(self._mv_requests):
            try:
                self._write_mv_limit(v)
            except Exception as e:
                self._log(f"⚠ DAC 상한(D118) {fail_word} 실패: {e!r}")
        self._mv_requests.clear()

    def _flush(self) -> None:
        while self._after:
            fn = self._after.pop(0)
            try:
                fn()
            except Exception as e:
                self._log(f"⚠ 후처리 예외: {e!r}")

    # ═══════════════════════ 내부: 상태 ═══════════════════════
    @staticmethod
    def _blank_status() -> dict:
        return {"ok": None, "pv": None, "sv": None, "sv_limit": None, "cur_sv": None, "pid_err": None,
                "mv": None, "mv_pct": 0.0, "mv_limit": None, "sv_ramp": None, "ramp_rate": None,
                "holdback": None, "ot_limit": None, "slow_zone": None, "slow_rate": None,
                "kp_act": None, "ti_act": None, "marker": None,
                "run": False, "itl": False, "fault": False, "ot": False, "tc_err": False, "wd_err": False,
                "pid_run": False, "dac_en": False, "tc_ok": False, "da_ok": False,
                "water_ok": False, "vac_ok": False, "rst": False,
                "output_dead": False, "noresp": None, "stale": True, "age_s": None, "t": None}

    def _publish(self) -> None:
        snap = {k: v for k, v in self._st.items() if not k.startswith("_")}
        for cb in list(self._listeners):
            try:
                cb(dict(snap))
            except Exception as e:
                self._log(f"상태 리스너 예외: {e!r}")
        self._proc_on_status(dict(snap))

    def _next_interval(self) -> float:
        st = self._st
        busy = (st.get("run") or self.profiler.is_active() or self._pending_off
                or (self._need_push and not st.get("stale") and self._now() >= self._push_retry_at))
        return float(self.cfg.poll_interval_s if busy else self.cfg.poll_idle_s)

    # ═══════════════════════ tick ═══════════════════════
    def on_tick_start(self, tick=None) -> None:
        """워치독(heater_service.py:727-736) — WD_PERIOD 마다 RUN 이고 링크가 살아 있을 때만 D114 +1."""
        now = self._now()
        with self._lock:
            if self._stopping:
                return
            if self._wd_next is None:
                self._wd_next = now + float(self.cfg.wd_period_s)
                return
            if now + 1e-6 < self._wd_next:
                return
            self._wd_next = now + float(self.cfg.wd_period_s)
            if not (self._st.get("run") and self._link_up()):
                return
        self.drv.submit("wd", self._wd_fn, on_done=lambda r: None)

    def _wd_fn(self, s) -> None:
        with self._lock:
            self._s = s
            try:
                if self._st.get("run"):
                    self._kick_wd()
            finally:
                self._s = None

    def on_row(self, view=None) -> None:
        """1초 행 — v1 폴링 주기가 됐으면 폴링 1회."""
        now = self._now()
        with self._lock:
            if self._stopping or self._poll_busy or now + 1e-6 < self._next_poll_t:
                return
            self._poll_t0 = now
            self.poll_once()
        self._flush()

    def poll_once(self) -> None:
        """폴링 1회(v1 _poll_once) — 미연결·읽기 실패는 여기서, 성공 읽기는 PLC worker 요청으로 처리."""
        with self._lock:
            now = self._now()
            if not self._link_up():
                self._on_fail("PLC 미연결", quiet=self._last_ok_t is None)
                self._poll_finished()
                return
            rd = self.drv.take()
            if rd is None:
                if self._last_event_t is not None and now - self._last_event_t < NO_READ_FAIL_S:
                    return                   # 이번 tick 읽기가 아직 안 옴 — 다음 tick 에 다시(실패로 세지 않음)
                rd = HeaterReadFail("PLC 히터 읽기 결과 없음", now)
            self._last_event_t = now
            if isinstance(rd, HeaterReadFail):
                self._on_fail(rd.why)
                self._poll_finished()
                return
            self._poll_busy = True
        self.drv.submit("poll", lambda s: self._poll_fn(s, rd), on_done=self._poll_done)

    def _poll_finished(self) -> None:
        self._next_poll_t = self._poll_t0 + self._next_interval() - 0.5

    def _poll_done(self, res) -> None:
        with self._lock:
            self._poll_busy = False
            if res.status != Status.OK:
                if res.status == Status.FAIL and res.error:
                    self._log(f"⚠ 히터 폴링 처리 예외: {res.error}")
                elif res.status in (Status.LINK_DOWN,):
                    self._on_fail("PLC 미연결", quiet=self._last_ok_t is None)
            self._poll_finished()
        self._flush()

    def _poll_fn(self, s, rd: HeaterRead):
        with self._lock:
            self._s = s
            try:
                now = self._now()
                if self._fail_since is not None:
                    self._log(f"히터 상태 읽기 복구 (끊긴 {now - self._fail_since:.0f}초)")
                    self._fail_since = None
                st = decode_block(rd.regs, rd.bits, self.cfg, now)
                if self.drv.write_seq != rd.seq:
                    return "discard"         # 읽은 뒤 켜기·끄기·목표 변경 같은 쓰기가 끼었다 — 쓰기 전 값
                if not self._process(st, now):
                    return "retry"           # 잔존 RUN 정리 실패 — 발행하지 않고 다음 폴링에 재시도
                self._st_seq = self.drv.write_seq
                self._last_ok_t = now
                self._publish()
                return "ok"
            finally:
                self._s = None

    def _on_fail(self, why: str, quiet: bool = False) -> None:
        """v1 _on_fail(heater_service.py:738-754)."""
        now = self._now()
        if self._fail_since is None and not quiet:
            self._fail_since = now
            self._log(f"⚠ 히터 상태 읽기 실패: {why}")
        st = self._st
        age = None if self._last_ok_t is None else now - self._last_ok_t
        st["age_s"] = age
        st["stale"] = age is None or age > float(self.cfg.stale_sec)
        if not st["stale"] and self.drv.write_seq != self._st_seq:
            return
        self._publish()

    # ═══════════════════════ 처리(worker 안) ═══════════════════════
    def _process(self, st: dict, now: float) -> bool:
        first = self._first_poll
        if first and st["run"] and not self._skip_residual and self.writes_allowed:
            if not self._clear_residual_run(st):
                return False
        if self._pending_off and self.writes_allowed:
            try:
                self._w_coil(H.COIL_RUN, False)
                self._pending_off = False
                self._log("보류했던 히터 OFF 적용")
            except Exception as e:
                self._log(f"⚠ 보류 OFF 적용 실패 — 다음 폴링에 재시도: {e!r}")
        self._marker_and_push(st)
        self._check_program(st)
        self._fault_edges(st, first)
        self._rst_check(st)
        if self._reset_pending:
            self._reset_pending = False
            if st["fault"]:
                self._log(f"⚠ 리셋 후에도 히터 이상 유지 — {self._fault_text(st)}: 원인이 남아 있습니다")
        rise = bool(st["run"]) and not self._run_prev
        fall = (not st["run"]) and self._run_prev
        if rise:
            self._run_since = now
        self._output_dead(st, now)
        self._st = st
        if rise:
            self._on_run_rise(st)
        elif fall:
            self._on_run_fall(st, now)
        self._run_prev = bool(st["run"])
        if self.profiler.is_active():
            self.profiler.tick()
        self._mv_requests.clear()
        self.sat.tick(st, self.hold.is_holding(), "dac", final_target=self.final_target(),
                      ramp_active=self.profiler.is_active(), defer_clamp=self.resp.loss_wait())
        self.hold.tick(st, self.final_target())
        if self.writes_allowed:
            self._run_mv_requests()
        self._mv_requests.clear()
        trip = self.resp.tick(st, self.final_target(), holding=self.hold.is_holding(),
                              clamped=self.sat.clamped is not None)
        if trip is not None:
            self._on_no_response(st, trip)
        if st["run"] and self.cfg.log_enabled and self.csv is not None and (now - self._csv_last_t) >= float(self.cfg.log_period_s):
            self._csv_last_t = now
            self.csv.write_row(st, owner=OWNER_LABEL.get(self.owner, self.owner or ""),
                               hold=self.hold.log_tuple(), sat_clamp=self.sat.clamped,
                               ramp_target=self.profiler.target())
        self._first_poll = False
        return True
