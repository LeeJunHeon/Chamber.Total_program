"""챔버12 스텝 실행기 — v1 controller/process_controller.py 의 실행 부분(ProcessController 러너)을 다시 쓴 것.

v1 근거(기준 커밋 66efc7b): start_process 628-715, request_stop 717-753, 장치 콜백 on_* 835-1134, _runner 1139-1237,
_execute_step 1239-1334, 기대 토큰 _send_and_collect_tokens 1336-1486, THICKNESS_ZERO/WAIT 1488-1613, 대기·카운트다운
1633-1708, ETA 1710-1780(eta.EtaClock), 폴링 1809-1866, 실패·종료 1869-2119, 토큰 소유권 3026-3061.

v1 은 asyncio 한 루프였다. v2 는 엔진의 러너 스레드 하나가 이 실행기를 돌리고(clock.wait_until 로만 기다림), 장치 결과는 worker
스레드에서 match·failed 로 들어온다(잠금 = clock.cond). 그래서 한 가지 순서만 바꿨다: 기대 토큰을 '보내기 전에' 건다
(v1 은 보낸 뒤 걸었지만 한 루프라 결과가 먼저 올 수 없었다 — 스레드에서는 빠른 응답을 놓치지 않으려면 먼저 걸어야 한다).
그 밖의 순서·문구·타이밍·판정은 v1 그대로:
- 평시 단계 대기에는 시간 제한이 없다 — RGA(예산 rga_step_timeout_ms)·THICKNESS_ZERO(확인 초 + 30)·THICKNESS_WAIT(상한)만.
- 종료 절차(shutdown) 대기: 전원 OFF 단계 max(PC_POWER_OFF_TIMEOUT_MS 240000, 2500) ms(RF 연속은 램프다운 + 70 s), 그 밖
  SHUTDOWN_STEP_TIMEOUT_MS 2500, 단계 사이 SHUTDOWN_STEP_GAP_MS 500, 그동안 abort 를 보지 않는다.
- 실패: 첫 실패만 대표 원인, 종료 절차 중 실패는 기록하고 계속(전원 OFF 실패가 다른 전원 단계 것이면 대기 유지).
v2 추가(사용자 지시 3장 (3) 공정 중 자유 제어 — 실행기 쪽 몫): 일시정지(대기 타이머 정지 — 남은 시간 유지), 남은 시간 수정
(set_remaining), 블록 건너뛰기(skip_to — 지금 대기를 끊고 그 블록 마지막 스텝 뒤로). 정지 순서 중에는 셋 다 받지 않는다.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from v2.ch12.eta import EtaClock
from v2.ch12.process_steps import ProcessStep

from v2.ch12.process_tokens import (HARD_WAIT, SOURCE_TOKENS, ZERO_STEP_SLACK_S, Countdown, Expect, RunResult,  # noqa: F401
                                    StepFailure, _cfg, _ms, _norm_err, delay_countdown, fmt_hms,
                                    rf_power_stop_timeout_ms, tokens_for)


class StepRunner:
    """한 공정(v1 컨트롤러 런). host(엔진)가 주는 것: clock·lk(clock.cond)·cfg·n(챔버 번호)·log(src, msg)·state(text)·
    actions(ProcessActions)·on_step(idx, step) — 스텝 시작 알림(블록 추적·기록)."""

    def __init__(self, host) -> None:
        self.h = host
        self.clock, self.lk, self.cfg, self.n = host.clock, host.lk, host.cfg, int(host.n)
        self.eta = EtaClock(self.cfg, self.n, self.clock.monotonic)
        self.is_running = False
        self._reset()

    # ═══════════════════════ 상태 ═══════════════════════
    def _reset(self) -> None:
        self.params: Dict[str, Any] = {}
        self.steps: List[ProcessStep] = []
        self.idx = -1
        self.test_mode = False
        self.stop_requested = self.shutdown = self.shutdown_error = self.failed = False
        self.failures: List[str] = []
        self.err = {"code": None, "source": None, "detail": "", "meta": {}}
        self.expect: Optional[Expect] = None
        self.owner: Dict[Tuple[str, Any], int] = {}
        self.abort_gen = 0
        self.skip_to: Optional[int] = None
        self.cd: Optional[Countdown] = None
        self.pause_req = False
        self.actual_sd_ms = 0
        self.actual_pt_ms = 0
        self.rga_abandoned = False
        self.thk_wait = False
        self.thk_end: Optional[Any] = None
        self._last: Optional[RunResult] = None
        self.trace: List[tuple] = []             # (시각, 스텝 번호, action, 문구, 종료 절차) — 실행한 순서(시험·기록)
        self.last_poll_active: Optional[bool] = None
        self.last_poll_targets: Optional[dict] = None
        self.t0 = self.clock.monotonic()
        self.eta.reset()

    @property
    def current(self) -> Optional[ProcessStep]:
        return self.steps[self.idx] if 0 <= self.idx < len(self.steps) else None

    def eta_remaining_s(self) -> Optional[int]:
        return self.eta.remaining_s(running=self.is_running,
                                    invalid=self.shutdown or self.stop_requested or self.failed)

    def _log(self, msg: str, src: str = "Process") -> None:
        self.h.log(src, msg)

    def _notify(self) -> None:
        with self.lk:
            self.lk.notify_all()

    def _wait(self, pred, timeout_s: Optional[float] = None) -> bool:
        """pred 가 참이면 True, 시간이 다 되면 False(timeout None = 끝없이)."""
        t = None if timeout_s is None else self.clock.monotonic() + max(0.0, float(timeout_s))
        return self.clock.wait_until(t, pred)

    def _sleep(self, sec: float) -> None:
        self.clock.wait_until(self.clock.monotonic() + max(0.0, float(sec)), None)

    # ═══════════════════════ 시작·정지(v1 start_process·request_stop) ═══════════════════════
    def run(self, params: Dict[str, Any], steps: List[ProcessStep]) -> RunResult:
        """러너 스레드에서 끝까지(블록) — 결과는 finish detail(v1 'finished' 이벤트 payload)."""
        with self.lk:
            if self.is_running:
                self._log("오류: 이미 다른 공정이 실행 중입니다.")
                return RunResult(False, {"result": "FAIL", "reason": "busy"})
            self._reset()
            self.params = params
            pname = str(params.get("process_name") or params.get("process_note") or params.get("Process_name")
                        or "Untitled").strip() or "Untitled"
            for k in ("process_name", "process_note", "Process_name"):
                params[k] = pname
            self.test_mode = bool(params.get("test_mode", False))
            self.steps = list(steps)
            self.is_running = True
            self.t0 = self.clock.monotonic()
        self._log(f"=== '{pname}' 공정 시작 (총 {len(self.steps)}단계) ===")
        try:
            self._loop()
        except StepFailure as e:
            self._log(f"러너 예외: {e}")
            return self._finish(False)
        except Exception as e:                       # v1 _runner except → finish(False)
            self._log(f"러너 예외: {e}")
            return self._finish(False)
        return self._last

    def request_stop(self) -> None:
        with self.lk:
            if not self.is_running:
                self._log("정지 요청: 실행 중 공정 없음(이미 종료됨) → 무시")
                return
            if self.stop_requested or self.shutdown:
                self._log("정지 요청: 이미 정지 처리 중입니다.")
                return
            self.stop_requested = True
            self._log("정지 요청을 받았습니다.")
            self.abort_gen += 1
            self.lk.notify_all()
            if self.test_mode:
                self._log("[TEST MODE] STOP: 장비 종료 절차 스킵 (딜레이만 취소)")
                return
            self._start_shutdown()

    # ═══════════════════════ 장치 결과(v1 on_*) ═══════════════════════
    def match(self, kind: str, spec: Any = None) -> None:
        with self.lk:
            if self.is_running and self.expect is not None:
                self.expect.match(kind, spec)
                self.lk.notify_all()

    def needs(self, kind: str, spec: Any = None) -> bool:
        with self.lk:
            return bool(self.expect is not None and not self.expect.over and self.expect.needs(kind, spec))

    def mfc_failed(self, cmd: str, why: Any, code: Optional[str] = None, meta: Optional[dict] = None) -> None:
        """MFC 실패는 지금 기다리는 MFC 명령일 때만 중단(838-860)."""
        m = {"cmd": cmd, **(meta or {})}
        with self.lk:
            if self.needs("MFC", cmd):
                self.step_failed("MFC", why, code=code, meta=m)
                return
        c, d, _ = _norm_err(why, code, m)
        self._log(f"경고(무시): {cmd}: {c} - {d} (현재 스텝과 무관)" if c else f"경고(무시): {cmd}: {d} (현재 스텝과 무관)",
                  "MFC")

    def power_off_failed(self, source: str, off_kind: str, why: Any, code: Optional[str] = None,
                         meta: Optional[dict] = None) -> None:
        """on_power_off_failed(962-982) — 종료 절차 중이면 그 전원 단계만 끝낸다."""
        with self.lk:
            if not self.is_running:
                return
            if self.shutdown:
                self.step_failed(source, why, code=code, meta=meta, cancel_wait=self.needs(off_kind))
                return
            self.step_failed(source, why, code=code, meta=meta)

    def rga_finished(self) -> None:
        with self.lk:
            if self.rga_abandoned:
                self._log("[RGA] 예산 초과 후 도착한 finished 무시(stale)")
                return
        self.match("RGA_OK")

    def oes_failed(self, src: str, why: Any, code: Optional[str] = None, meta: Optional[dict] = None) -> None:
        c, d, _ = _norm_err(why, code, meta)
        self._log(f"오류 무시하고 계속: {c} - {d}" if c else f"오류 무시하고 계속: {d}", src or "OES")

    def step_failed(self, source: str, reason: Any, *, code: Optional[str] = None, meta: Optional[dict] = None,
                    cancel_wait: bool = True) -> None:
        """_step_failed(1979-2038)."""
        with self.lk:
            if not self.is_running:
                return
            c, d, m = _norm_err(reason, code, meta)
            full = f"[{source} - {c}: {d}]" if c else f"[{source} - {d}]"
            oi = self._owner_of(source)
            if oi is not None and 0 <= oi < len(self.steps):
                no, act = oi + 1, self.steps[oi].action
            else:
                cur = self.current
                no, act = self.idx + 1, (cur.action if cur else "UNKNOWN")
            if not (self.err["source"] or self.err["detail"]):
                self.err = {"code": str(c).strip().upper() if c else None, "source": str(source).strip() or "UNKNOWN",
                            "detail": str(d).strip(), "meta": dict(m or {})}
            if self.shutdown:
                self.shutdown_error = True
                self.failures.append(f"Step {no} {act}: {full}")
                if not cancel_wait:
                    cur = self.current
                    self._log(f"경고: 종료 중 {source} 실패 기록 — 현재 단계({cur.action if cur else 'UNKNOWN'}) "
                              f"대기는 유지 (사유: {full})")
                    return
                self._log(f"경고: 종료 중 단계 실패 → 계속 진행 ({act}, 사유: {full})")
                if self.expect is not None:
                    self.expect.cancel("failure-during-shutdown")
                    self.expect = None
                self.lk.notify_all()
                return
            self.failed = True
            self.failures.append(f"Step {no} {act}: {full}")
            self._log(f"오류 발생: {full}. 종료 절차를 시작합니다.")
            self._start_shutdown()

    def _owner_of(self, source: str) -> Optional[int]:
        kinds = SOURCE_TOKENS.get(str(source).strip(), ())
        for (k, _s), i in reversed(list(self.owner.items())):
            if k in kinds:
                return i
        return None

    # ═══════════════════════ 종료 절차 ═══════════════════════
    def _start_shutdown(self) -> None:
        """_start_normal_shutdown(1926-1977). 종료 절차 = host.shutdown_steps()(지금 구간 장비 — blocks.segment_uses)."""
        with self.lk:
            if not self.is_running:
                self._log("종료 절차 무시: 실행 중 공정 없음(이미 종료됨)")
                return
            if self.shutdown:
                self._log("종료 절차 무시: 이미 종료 절차 진행 중입니다.")
                return
            self.shutdown = True
            self.eta.reset()
            self.pause_req = False
            self.skip_to = None
            self._log("정지 요청 - 안전한 종료 절차를 시작합니다.")
            self._apply_polling(False)
            self.abort_gen += 1
            self.lk.notify_all()
        self.h.actions.cancel_waits()                     # IG 대기·히터 대기·두께 판정(v1 _cancel_ig·_cancel_heater·_cancel_thickness)
        with self.lk:
            self._cancel_countdown()
            if self.expect is not None:
                self.expect.cancel("shutdown")
                self.expect = None
            try:
                steps = self.h.shutdown_steps()
                if steps:
                    self._log(f"종료 절차 생성 완료: {len(steps)}단계")
                    self.steps = list(steps)
                    self.idx = -1
                else:
                    self._log("종료 절차가 없어서 즉시 완료합니다.")
                    self.steps, self.idx = [], 0
            except Exception as e:
                self._log(f"종료 절차 시작 오류: {e}")
                self.steps, self.idx = [], 0
            self.lk.notify_all()

    # ═══════════════════════ 러너(v1 _runner) ═══════════════════════
    def _loop(self) -> None:
        while True:
            with self.lk:
                if self.stop_requested and not self.shutdown and not self.test_mode:
                    self._log("정지 요청 감지 - 종료 절차를 시작합니다.")
                    self._start_shutdown()
                elif self.stop_requested and not self.shutdown and self.test_mode:
                    self._log("[TEST MODE] 정지 요청 감지 - 종료 절차 스킵")
                if self.skip_to is not None and not self.shutdown:
                    self.idx = max(self.idx, int(self.skip_to))
                self.skip_to = None
                self.idx += 1
                if self.idx >= len(self.steps):
                    base_ok = not self.stop_requested
                    ok = base_ok and not self.failed and not (self.shutdown and self.shutdown_error)
                    self._last = self._finish(ok)
                    return
                step = self.steps[self.idx]
                if not self.shutdown:
                    self.eta.try_confirm(self.steps, self.idx, blocked=self.shutdown, log=self.h.log)
                self.h.state(step.message)
                self._log(f"[{'종료절차' if self.shutdown else '공정'} {self.idx + 1}/{len(self.steps)}] {step.message}")
                self.h.on_step(self.idx, step, self.shutdown)
                par: List[ProcessStep] = []
                if step.parallel:
                    t = self.idx
                    while t < len(self.steps) and self.steps[t].parallel:
                        par.append(self.steps[t])
                        t += 1
                    first = self.idx
                    self.idx = t - 1
                now = self.clock.monotonic()
                for j, s_ in enumerate(par or [step]):
                    self.trace.append((now, (first if par else self.idx) + j, s_.action, s_.message, self.shutdown))
            self._pause_gate(step)
            if par:
                self._run_parallel(par, first)
            else:
                self._apply_polling(step.polling)
                self._execute(step)

    def _pause_gate(self, step: ProcessStep) -> None:
        """일시정지(파워 켜기 등 타이머가 아닌 블록) — 아직 안 보낸 명령을 재개까지 보내지 않는다. 대기 스텝은 카운트다운이 멈춘다."""
        if step.action in ("DELAY", "ROW_DELAY", "THICKNESS_WAIT"):
            return
        with self.lk:
            g = self.abort_gen
            if not self.pause_req or self.shutdown:
                return
        self._log(f"[PAUSE] '{step.message}' — 재개할 때까지 보내지 않음")
        self._wait(lambda: not self.pause_req or self.shutdown or self.abort_gen != g or self.skip_to is not None)

    def _run_parallel(self, par: List[ProcessStep], first: int) -> None:
        self._apply_polling(any(s.polling for s in par))
        toks: List[Tuple[str, Any]] = []
        with self.lk:
            for j, s in enumerate(par):
                tk = tokens_for(s, self.h.actions.norm_plc)
                toks += tk
                for t in tk:
                    self.owner[t] = first + j
            ex = self._set_expect(toks)
            g = self.abort_gen
            sd = self.shutdown
        for s in par:
            self.h.actions.send(s, self)
        if ex is None:
            return
        if sd:
            if not self._wait(lambda: ex.over, _ms(self.cfg, "SHUTDOWN_STEP_TIMEOUT_MS", 2500) / 1000.0):
                self._log("종료(병렬) 스텝 확인 시간 초과 → 다음으로")
            elif ex.cancelled is not None:
                return                                   # v1: 취소된 기대(CancelledError) → 간격 없이 다음
            self._sleep(_ms(self.cfg, "SHUTDOWN_STEP_GAP_MS", 500) / 1000.0)
            return
        self._wait(lambda: ex.over or self.abort_gen != g or self.skip_to is not None)
        with self.lk:
            if not ex.over and self.expect is ex:
                ex.cancel("abort")
                self.expect = None

    def _set_expect(self, tokens) -> Optional[Expect]:
        if self.expect is not None:
            self.expect.cancel("replaced")
            self.expect = None
        if not tokens:
            return None
        self.expect = Expect(tokens)
        return self.expect

    def _execute(self, step: ProcessStep) -> None:
        a = step.action
        if a == "THICKNESS_WAIT":
            return self._thickness_wait(step)
        if a == "THICKNESS_ZERO":
            return self._thickness_zero(step)
        if a == "DELAY":
            if step.polling:
                self._apply_polling(True)
            return self._countdown(step.duration or 100, step.message)
        if a == "ROW_DELAY":
            return self._row_delay(step)
        with self.lk:
            tks = tokens_for(step, self.h.actions.norm_plc)
            for t in tks:
                self.owner[t] = self.idx
            ex = None if (step.no_wait or not tks) else self._set_expect(tks)
            g, sd = self.abort_gen, self.shutdown
        self.h.actions.send(step, self)
        if ex is None:
            return
        if sd:
            if a in HARD_WAIT:
                lim = max(float(_cfg(self.cfg, self.n, "PC_POWER_OFF_TIMEOUT_MS", 240000)),
                          _ms(self.cfg, "SHUTDOWN_STEP_TIMEOUT_MS", 2500))
                if a == "RF_POWER_STOP":
                    lim = rf_power_stop_timeout_ms(self.cfg, self.n, lim)
                got = self._wait(lambda: ex.over, lim / 1000.0)
                if not got:
                    self.step_failed("Shutdown", f"{a} timeout ({lim / 1000:.0f}s)")
            else:
                got = self._wait(lambda: ex.over, _ms(self.cfg, "SHUTDOWN_STEP_TIMEOUT_MS", 2500) / 1000.0)
                if not got:
                    self._log("종료 스텝 확인 시간 초과 → 다음 스텝 진행")
            if got and ex.cancelled is not None:
                return                                   # v1: 취소된 기대(CancelledError) → 간격 없이 return
            self._sleep(_ms(self.cfg, "SHUTDOWN_STEP_GAP_MS", 500) / 1000.0)
            return
        if a == "RGA_SCAN":
            from v2.ch12.drivers.rga_link import step_timeout_ms
            budget = step_timeout_ms(self.cfg)
            if not self._wait(lambda: ex.over or self.abort_gen != g or self.skip_to is not None, budget / 1000.0):
                self._log(f"RGA 스캔 대기 시간 초과(예산 {budget / 1000.0:g}s) → 그래프 스킵, 다음 단계 진행")
                with self.lk:
                    self.rga_abandoned = True
                    if self.expect is ex:
                        ex.cancel("rga-timeout")
                        self.expect = None
                return
        else:
            self._wait(lambda: ex.over or self.abort_gen != g or self.skip_to is not None)
        with self.lk:
            if not ex.over and self.expect is ex:
                ex.cancel("abort")
                self.expect = None

    # ═══════════════════════ 대기·카운트다운(1633-1708) ═══════════════════════
    def _countdown(self, duration_ms: int, msg: str) -> None:
        with self.lk:
            self._cancel_countdown(account=False)
            self.cd = Countdown(self.clock.monotonic(), int(duration_ms), msg)
            if self.pause_req and not self.shutdown:
                self.cd.paused_at = self.cd.start
            allow = not self.shutdown
            g = self.abort_gen
        self._log(f"{msg} 시작 ({int((duration_ms + 999) // 1000)}초 대기)")
        aborted = False
        while True:
            with self.lk:
                cd = self.cd
                if cd is None:                       # 정지 순서가 카운트다운을 먼저 정리함(v1 _cancel_countdown — 문구가 비어 있음)
                    aborted = True
                    break
                now = self.clock.monotonic()
                rem = cd.remaining_ms(now)
                s = (rem + 999) // 1000
                m_, s_ = divmod(s, 60)
                self.h.state(f"{cd.msg} (남은 시간: {m_}분 {s_}초)" if m_ > 0 else f"{cd.msg} (남은 시간: {s_}초)")
                if rem <= 0:
                    break
                if allow and (self.abort_gen != g or self.skip_to is not None):
                    aborted = True
                    break
                nxt = now + min(1.0, rem / 1000.0) if cd.paused_at is None else now + 1.0
            g0 = (self.abort_gen, self.skip_to, self.pause_req)
            self.clock.wait_until(nxt, lambda: (allow and (self.abort_gen != g0[0] or self.skip_to is not None))
                                  or self.pause_req != g0[2] or self.cd is None)
        with self.lk:
            m = self.cd.msg if self.cd else ""       # v1: 정지 순서가 먼저 지웠으면 ' 중단됨'(빈 문구) — 그대로
            self._log(f"{m} 중단됨" if aborted else f"{m} 완료")
            self._cancel_countdown()

    def _cancel_countdown(self, account: bool = True) -> None:
        """실제 흐른 시간(계획 상한)을 'Shutter Delay'·'메인 공정 진행' 누적에 더하고 정리."""
        cd = self.cd
        if cd is not None and account and cd.total_ms > 0:
            el = max(0, min(int(cd.elapsed_s(self.clock.monotonic()) * 1000.0), cd.total_ms))
            mm = (cd.msg or "").strip()
            if mm.startswith("Shutter Delay"):
                self.actual_sd_ms += el
            elif mm.startswith("메인 공정 진행"):
                self.actual_pt_ms += el
        self.cd = None

    def set_pause(self, on: bool) -> None:
        """대기 타이머 정지/재개 — 정지한 만큼 ETA 도 늦춘다."""
        with self.lk:
            self.pause_req = bool(on)
            cd, now = self.cd, self.clock.monotonic()
            if cd is not None:
                if on and cd.paused_at is None:
                    cd.paused_at = now
                elif not on and cd.paused_at is not None:
                    d = now - cd.paused_at
                    cd.paused_s += d
                    cd.paused_at = None
                    self.eta.shift(d)
            self.lk.notify_all()

    def set_remaining(self, sec: float) -> Optional[float]:
        """지금 DELAY 의 남은 시간 = sec(초). 이전 남은 초를 돌려준다(DELAY 가 아니면 None)."""
        with self.lk:
            cd = self.cd
            if cd is None:
                return None
            now = self.clock.monotonic()
            old = cd.remaining_ms(now) / 1000.0
            cd.total_ms = int(cd.elapsed_s(now) * 1000.0 + max(0.0, float(sec)) * 1000.0)
            self.eta.shift(float(sec) - old)
            self.lk.notify_all()
            return old

    def remaining_s(self) -> Optional[float]:
        with self.lk:
            if self.cd is not None:
                return self.cd.remaining_ms(self.clock.monotonic()) / 1000.0
            return None

    def request_skip(self, last_idx: int) -> None:
        with self.lk:
            self.skip_to = int(last_idx)
            self.lk.notify_all()

    def _row_delay(self, step: ProcessStep) -> None:
        """블록 레시피 안의 delay 행 블록 — v1 러너 대기(chamber_runtime.py:5127-5145)와 같은 문구·int(초) 회."""
        label, disp = step.params
        with self.lk:
            g = self.abort_gen
        self._log(f"[Runner] '{disp}' 단계: {label} 대기 시작")
        ok = delay_countdown(self.clock, step.duration / 1000.0,
                             lambda r: self.h.state(f"지연 대기 중: {label} · 남은 시간 {fmt_hms(r)}"),
                             lambda: self.abort_gen != g or self.skip_to is not None, lambda: self.pause_req)
        if ok:
            self._log(f"[Runner] '{disp}' 지연 완료 → 다음 스텝")

    # ═══════════════════════ 두께(1488-1613) ═══════════════════════
    def _thickness_zero(self, step: ProcessStep) -> None:
        confirm_s = float(step.params[0]) if step.params else 5.0
        th = self.h.actions.thickness
        if th is None:
            self._log("⚠ 두께 영점 콜백 없음 → 영점 없이 진행")
            return
        with self.lk:
            self.owner[("THICKNESS_ZERO", None)] = self.idx
            ex = self._set_expect([("THICKNESS_ZERO", None)])
            g = self.abort_gen
        try:
            th.zero_start(confirm_s, lambda d: self.match("THICKNESS_ZERO"))
        except Exception as e:
            self._log(f"⚠ 두께 영점 시작 실패({e!r}) → 영점 없이 진행")
            with self.lk:
                if self.expect is ex:
                    ex.cancel("thickness-zero-error")
                    self.expect = None
            return
        # 한도(확인 초 + 30)는 두께 엔진이 본다(기다림만 끊고 on_done) — 여기는 그보다 조금 길게 안전망
        self._wait(lambda: ex.over or self.abort_gen != g or self.skip_to is not None, confirm_s + ZERO_STEP_SLACK_S + 5.0)
        with self.lk:
            aborted = self.abort_gen != g
            if self.expect is ex:
                ex.cancel("thickness-zero-end")
                self.expect = None
        if aborted:
            self._log("두께 영점·확인 중단됨")

    def _thickness_wait(self, step: ProcessStep) -> None:
        target = float(step.value)
        exp_s, lim_s = (float(x) for x in step.params)
        if step.polling:
            self._apply_polling(True)
        th = self.h.actions.thickness
        with self.lk:
            self._cancel_countdown(account=False)
            self.cd = Countdown(self.clock.monotonic(), int(lim_s * 1000.0), step.message)
            self.thk_wait, self.thk_end = True, None
            self.owner[("THICKNESS_DONE", None)] = self.idx
            g = self.abort_gen
        if th is None:
            # 두께 서비스 없음 → 예상 시간으로 시간 대체(v1 cb_thickness_start 의 svc None)
            self._log(f"{step.message} 시작 — 목표 {target:g} nm, 예상 {exp_s:.0f}초, 최대 {lim_s:.0f}초 "
                      "(목표 도달 시 종료, 이상 시 시간 대체, 상한 도달 시 정상 종료 절차)")
            self._wait(lambda: self.abort_gen != g or self.skip_to is not None, exp_s)
            end = "중단" if self.abort_gen != g else "시간대체"
            res = None
        else:
            def on_end(r):
                with self.lk:
                    self.thk_end = r
                    self.lk.notify_all()
            th.wait_start(target, exp_s, lim_s, on_end, step.message,
                          dep_rate=self.params.get("dep_rate") or None)
            self._wait(lambda: self.thk_end is not None or self.abort_gen != g or self.skip_to is not None)
            if self.thk_end is None:
                th.abort()                                     # STOP·실패·건너뛰기 — 두께 엔진이 '중단'으로 끝낸다
                self._wait(lambda: self.thk_end is not None, 5.0)
            res = self.thk_end
            end = getattr(res, "end", "중단")
        with self.lk:
            self.thk_wait = False
            self._cancel_countdown()
        if res is not None and getattr(res, "code", None) == "E848":
            self.step_failed(res.source or "THICKNESS", res.detail, code="E848", meta=res.meta)
        return end

    # ═══════════════════════ 폴링(1809-1866) ═══════════════════════
    def _apply_polling(self, active: bool) -> None:
        active = bool(active)
        targets = self.h.actions.polling_targets(active, self.params)
        with self.lk:
            changed = self.last_poll_active != active
            if changed:
                if active:
                    self._log("폴링 시작")
                elif self.last_poll_active is not None:
                    self._log("폴링 중지")
            if not (changed or self.last_poll_targets != targets):
                return
            self.last_poll_active, self.last_poll_targets = active, dict(targets)
        self.h.actions.apply_polling(active, targets)

    # ═══════════════════════ 끝(_finish 2040-2119) ═══════════════════════
    def _finish(self, ok: bool) -> RunResult:
        with self.lk:
            stopped = bool(self.stop_requested)
            result = "SUCCESS" if ok else ("STOP" if stopped else "FAIL")
            pname = str(self.params.get("process_name") or "Untitled").strip() or "Untitled"
            detail = {"process_name": pname, "result": result, "stopped": stopped, "test_mode": self.test_mode,
                      "aborting": False, "errors": list(self.failures), "error_code": self.err["code"],
                      "error_source": self.err["source"], "error_detail": self.err["detail"],
                      "error_meta": dict(self.err["meta"] or {}),
                      "actual_shutter_delay_min": None, "actual_process_time_min": None}
            self._cancel_countdown()
            detail["actual_shutter_delay_min"] = round(self.actual_sd_ms / 60000.0, 3)
            detail["actual_process_time_min"] = round(self.actual_pt_ms / 60000.0, 3)
            self.is_running = False
            if self.expect is not None:
                self.expect.cancel("finish")
                self.expect = None
            self.shutdown = self.stop_requested = False
            self.last_poll_active = self.last_poll_targets = None
            self.lk.notify_all()
        self.h.actions.cancel_waits(final=True)
        self._log("=== 공정이 성공적으로 완료되었습니다 ===" if ok else "=== 공정이 중단되었습니다 ===")
        if not ok and detail["errors"]:
            self._log(f"[종료 중 실패 요약] 총 {len(detail['errors'])}건")
            for item in detail["errors"]:
                self._log(f" - {item}")
        self.h.state("공정 완료" if ok else "공정 중단됨")
        self._last = RunResult(bool(ok), detail)
        return self._last
