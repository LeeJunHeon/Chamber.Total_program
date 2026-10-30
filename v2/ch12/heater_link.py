"""CH2 히터 엔진 — 공정 연동. v1 runtime/chamber_runtime.py 의 히터 부분(1463-1556 cb_heater_set/wait/hold/cancel,
6946-7184 _on_heater_status·_heater_judge·_heater_record·_heater_hold_failed·_heater_after_row·종료 사유, 4086-4090, 7745-7749).

공정 엔진이 부르는 것(모두 즉시 돌아온다 — 결과는 공정 결과로)
- heater_set(target, rate)            HEATER_SET  → ("set_ok") / ("failed", 문구, E830)
- heater_wait(target, soak_s=None)    HEATER_WAIT → ("ready") / ("failed", 문구, E830·E831·E832)
- heater_hold()                       HEATER_HOLD → ("hold_ok") / ("failed", 문구, E833·E830)
- heater_cancel()                     STOP·실패·종료 절차 시작 때(대기·감시 해제, 세대 +1 — 진행 중 설정의 결과는 버린다)
- after_row(ok, stopped, detail, queue_mode, next_uses_heater) / heater_off(reason) / queue_end()   공정 1행 끝·큐 끝 정리
- process_started(uses_heater), set_process_flags(running, polling, log_open), set_context(text)
공정 결과는 on_process(kind, ok, 문구, 코드) 콜백(있으면, 잠금 밖)과 process_results(deque)로 — kind ∈ set_ok·ready·hold_ok·failed.
판정은 상태 발행마다(v1 상태 리스너): 출력 무응답·끊김(noresp)이 오면 대기·감시를 끝내고 E832(일반 'RUN 꺼짐'보다 먼저).
판정 코드가 예외를 내면 대기·감시를 끝내고 E830(대기 중)·E832(감시 중) — traceback 은 HEATER 로그에 1번.
"""
from __future__ import annotations

import traceback
from collections import deque
from typing import Any, Callable, Dict, List, Mapping, Optional

from v2.ch12.heater_notify import OWNER_LABEL
from v2.ch12.heater_process import HeaterHoldWaiter, HeaterSoakWaiter, heater_wait_timeout_s, itl_missing_text


def next_row_uses_heater(rows: List[Mapping[str, Any]], index: int) -> bool:
    """큐에서 다음 '공정' 행이 히터를 쓰는가 — delay 행은 건너뛰고, TEST 행은 안 쓰는 것으로 본다(chamber_runtime.py:7109-7121)."""
    from v2.ch12.recipe import TF_TRUE, delay_of
    q = list(rows or [])
    i = int(index) + 1
    while i < len(q):
        row = q[i] or {}
        if delay_of(row)[0] > 0:
            i += 1
            continue
        if str(row.get("#") or "").strip().lower() == "test":
            return False
        return str(row.get("use_heater", "") or "").strip().upper() in TF_TRUE
    return False


def process_context(name: str, index: int, total: int) -> str:
    """카드 부제 맥락 — 공정 "이름", 연속 실행이면 " (현재/전체)"(chamber_runtime.py:7123-7131)."""
    ctx = f'공정 "{str(name or "").strip()}"'
    if int(total) > 1:
        ctx += f" ({int(index) + 1}/{int(total)})"
    return ctx


class HeaterProcessLink:
    def _link_init(self) -> None:
        self._gen = 0
        self._job = None                     # ("wait", HeaterSoakWaiter) | ("hold", HeaterHoldWaiter)
        self._watch = False
        self._rate: Optional[float] = None
        self._hold_pending = False           # HEATER_HOLD 무장 쓰기를 보냈고 아직 대기 시작 전
        self.on_process: Optional[Callable[[str, bool, str, Optional[str]], None]] = None
        self.process_results: "deque" = deque(maxlen=200)
        self.next_step_fn: Optional[Callable[[], str]] = None
        self.on_sample: Optional[Callable[[Any, Any, Any, bool], None]] = None
        self.proc_running = False
        self.proc_polling = False
        self.proc_log_open = False
        self._line_t = -1e9
        self._record_warned = False
        self.stop_user = True                # STOP 출처(사용자/원격) — 종료 사유 문구

    # ── 결과 ──
    def _proc_result(self, kind: str, msg: str = "", code: Optional[str] = None) -> None:
        item = (kind, kind != "failed", str(msg), code)
        self.process_results.append(item)
        cb = self.on_process
        if cb is not None:
            self._after.append(lambda: cb(*item))

    def take_process_results(self) -> list:
        out = []
        while self.process_results:
            out.append(self.process_results.popleft())
        return out

    def phase(self) -> str:
        if self._job is not None:
            kind, w = self._job
            if kind == "wait":
                return "soak" if getattr(w, "hold_t0", 0.0) else "wait"
            return "hold"
        return "watch" if self._watch else ""

    def set_process_flags(self, running: bool, polling: bool = False, log_open: bool = False) -> None:
        with self._lock:
            self.proc_running, self.proc_polling, self.proc_log_open = bool(running), bool(polling), bool(log_open)

    # ═══════════════════════ HEATER_SET / WAIT / HOLD ═══════════════════════
    def heater_set(self, target: float, rate: Optional[float] = None, context: Optional[str] = None) -> None:
        """HEATER_SET(cb_heater_set) — 준비 확인 → 공정이 넘겨받음·맥락 → 유지·포화 클램프 금지 → 운전 중이면 목표 변경, 아니면 켜기."""
        gen = self._gen
        posted = [False]

        def post(kind: str, msg: str = "", code: Optional[str] = None) -> None:
            posted[0] = True
            self._proc_result(kind, msg, code)
        with self._lock:
            ok, why = self.is_ready()
            if not ok:
                post("failed", f"히터 사용 불가 — {why}", "E830")
        if not ok:
            self._flush()
            return

        def locked():
            ok2, why2 = self.is_ready()
            if not ok2:
                if gen == self._gen:
                    post("failed", f"히터 사용 불가 — {why2}", "E830")
                return False, why2
            r = float(rate) if rate else float(self.cfg.ramp_rate)
            running = bool(self._st.get("run"))
            self.take_over("process")
            if context is not None:
                self.set_context(context)
            try:
                self._set_hold_armable_locked(False, "공정이 작업 조건에서 유지값을 다시 잰다")
                if running:
                    ok3, why3 = self._apply_target_locked(float(target), r, "process")
                else:
                    ok3, why3 = self._turn_on_locked(float(target), r, "process")
                    if ok3:
                        self._kick_wd()
            except Exception as e:
                ok3, why3 = False, repr(e)
            if gen != self._gen:
                posted[0] = True             # 그 사이 정지·취소 — 공정은 건드리지 않음(정리는 after_row·안전망)
                return ok3, why3
            if not ok3:
                if not self._st.get("run"):
                    self.release("process")
                post("failed", f"히터 설정 실패 — {why3}", "E830")
                return ok3, why3
            self._rate = r
            self._watch = True
            self._log(f"공정 히터 설정 — 목표 {float(target):.1f}°C · {r:g}°C/min "
                      f"({'운전 중 목표 변경' if running else '운전 시작'})")
            post("set_ok")
            return True, ""

        def done(ok_: bool, why_: str) -> None:
            if posted[0] or gen != self._gen:
                return
            with self._lock:
                if not self._st.get("run") and self.owner == "process":
                    self.owner = None
                post("failed", f"히터 설정 실패 — {why_}", "E830")
            self._flush()
        self._cmd("heater_set", locked, done)

    def heater_wait(self, target: float, soak_s: Optional[float] = None) -> None:
        """승온 대기 시작. soak_s = 레시피 heater_soak(초) — 도달 확인 뒤 시간만 세는 SOAK(None = 없음)."""
        with self._lock:
            c = self.cfg
            rate = float(self._rate or c.ramp_rate)
            hold = 0.0 if soak_s is None else float(soak_s)
            to, why = heater_wait_timeout_s(self._st.get("pv"), float(target), rate,
                                            base_timeout_s=c.wait_timeout_s, approach_zone=c.approach_zone,
                                            approach_min_rate=c.approach_min_rate, soak_sec=c.soak_time_s)
            if soak_s is None:
                self._log(f"히터 승온 대기 — 목표 {float(target):.1f}°C ±{c.soak_tol:g}°C 를 "
                          f"{c.soak_time_s:.0f}초 유지하면 진행, 한도 {to / 60:.0f}분({why})")
            else:
                self._log(f"히터 승온 대기 — 목표 {float(target):.1f}°C ±{c.soak_tol:g}°C 를 "
                          f"{c.soak_time_s:.0f}초 유지하면 도달, 그 뒤 SOAK {hold / 60:g}분(레시피 heater_soak, "
                          f"시간만 셈) 후 진행. 도달 한도 {to / 60:.0f}분({why})")
            self._job = ("wait", HeaterSoakWaiter(float(target), tol=c.soak_tol, soak_sec=c.soak_time_s,
                                                  timeout_s=to, hold_sec=hold, clock=self._now, log=self._log))

    def heater_hold(self) -> None:
        gen = self._gen

        def locked():
            self._set_hold_armable_locked(True, "")
            if gen != self._gen:
                return True, ""
            snap = self.hold_snapshot()
            if snap.get("mode") == "off":
                self._log("유지 모드 꺼짐(HEATER_HOLD_MODE=off) — 포화 감시만 다시 켜고 진행")
                self._proc_result("hold_ok")
                return True, ""
            c = self.cfg
            self._log(f"히터 유지 모드 측정 시작(작업 압력·플라즈마 조건) — 최대 {c.hold_wait_s:.0f}초")
            self._job = ("hold", HeaterHoldWaiter(timeout_s=c.hold_wait_s, give_up_seq0=int(snap.get("give_up_seq") or 0),
                                                  clock=self._now, log=self._log))
            return True, ""
        self._cmd("heater_hold", locked, None, offline=locked)

    def heater_cancel(self) -> None:
        with self._lock:
            self._gen += 1
            self._job = None
            self._watch = False

    # ═══════════════════════ 판정(상태 발행마다) ═══════════════════════
    def _proc_on_status(self, st: dict) -> None:
        self._heater_record(st)
        job, watch = self._job, self._watch
        try:
            self._heater_judge(st)
        except Exception as e:
            self._heater_judge_error(e, job, watch)

    def _heater_judge(self, st: dict) -> None:
        trip = st.get("noresp")
        if trip and (self._job is not None or self._watch):
            self._job = None
            self._watch = False
            self._proc_result("failed", f"공정 중 히터 {trip} — 공정을 중단합니다", "E832")
            return
        job = self._job
        if job is not None:
            kind, w = job
            r = w.tick(st, self.profiler.is_active()) if kind == "wait" else w.tick(st, self.hold_snapshot())
            if r is None:
                return
            self._job = None
            ok, msg, code = r
            if ok:
                self._log(msg)
                if kind == "wait":
                    self._heater_reached_card(w, st)
                self._proc_result("ready" if kind == "wait" else "hold_ok")
                return
            if kind == "hold" and code == "E833":
                self._heater_hold_failed(getattr(w, "fail_kind", ""), msg)
                return
            self._watch = False
            self._proc_result("failed", msg, code)
            return
        if self._watch and (st.get("fault") or (not st.get("stale") and not st.get("run"))):
            self._watch = False
            if st.get("fault"):
                why = "히터 이상(FAULT)"
            elif not st.get("itl"):
                why = f"히터 인터락 해제({itl_missing_text(st)})"
            else:
                why = "히터가 꺼짐(외부 OFF 또는 순간 인터락 해제)"
            self._proc_result("failed", f"공정 중 {why} — 공정을 중단합니다", "E832")

    def _heater_judge_error(self, e: Exception, job, watch: bool) -> None:
        self._job = None
        self._watch = False
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__)).rstrip()
        self._log(f"⚠ 히터 공정 판정 예외{' — 공정을 중단합니다' if (job is not None or watch) else ''}: {tb}")
        if job is not None:
            self._proc_result("failed", f"히터 {'도달' if job[0] == 'wait' else '유지'} 대기 판정 중 프로그램 오류({e!r}) — "
                                        f"공정을 중단합니다", "E830")
        elif watch:
            self._proc_result("failed", f"공정 중 히터 감시 중 프로그램 오류({e!r}) — 공정을 중단합니다", "E832")

    def _heater_hold_failed(self, kind: str, msg: str) -> None:
        """유지 측정 실패 — abort: E833 / 포기(출력 상한): 경고 후 진행 / 시간 초과: 강제 진입 후 진행(chamber_runtime.py:7082-7107)."""
        if self.cfg.hold_fail_action == "abort":
            self._watch = False
            self._proc_result("failed", f"{msg} (HEATER_HOLD_FAIL_ACTION=abort)", "E833")
            return
        if kind == "give_up":
            self._log(f"⚠ {msg} — 출력이 이미 상한(D118={self._st.get('mv_limit')})이라 "
                      f"상한이 곧 출력 제한 역할을 합니다. 공정을 계속합니다(HEATER_MV_LIMIT 여유 확인)")
            self._proc_result("hold_ok")
            return
        gen = self._gen
        if self._s is None:
            ok, why = False, "PLC 세션 없음"
        else:
            ok, why = self._force_hold_locked(msg)
        if gen != self._gen:
            return
        if ok:
            self._log(f"⚠ {msg} → 유지 모드 강제 진입: {why}")
            self._proc_result("hold_ok")
        else:
            self._watch = False
            self._proc_result("failed", f"{msg} — 강제 진입 실패: {why}", "E833")

    def _heater_reached_card(self, w, st: dict) -> None:
        try:
            if self.owner != "process":
                return
            nxt = ""
            if callable(self.next_step_fn):
                try:
                    nxt = str(self.next_step_fn() or "")
                except Exception:
                    nxt = ""
            took = float(self._now()) - float(getattr(w, "t0", self._now()))
            hold = float(getattr(w, "hold_sec", 0.0) or 0.0)
            self.notify_reached(getattr(w, "target", self.final_target()), st.get("pv"), took, nxt,
                                soak_sec=hold, soak_pv=w.pv_range_text() if hold > 0 else "")
        except Exception as e:
            self._log(f"⚠ 히터 도달 카드 실패: {e!r}")

    # ═══════════════════════ 공정 기록 ═══════════════════════
    def _heater_record(self, st: dict) -> None:
        try:
            if st.get("stale") or not self.proc_running:
                return
            run = bool(st.get("run"))
            if self.proc_polling and self.on_sample is not None:
                pv = st.get("pv") if st.get("tc_ok") else None
                self.on_sample(pv, self.final_target(), st.get("mv"), run)
            period = float(self.cfg.run_log_period_s or 0.0)
            if run and period > 0 and self.proc_log_open:
                now = self._now()
                if now - self._line_t >= period:
                    self._line_t = now
                    self._log(self.status_line(st))
        except Exception as e:
            if not self._record_warned:
                self._record_warned = True
                self._log(f"⚠ 히터 공정 기록 실패(이후 생략): {e!r}")

    def status_line(self, st: dict) -> str:
        """공정 로그용 히터 상태 한 줄(chamber_runtime.py:7044-7071)."""
        def f1(v) -> str:
            return "--.-" if v is None else f"{float(v):.1f}"

        def i0(v) -> str:
            return "-" if v is None else f"{int(v)}"
        pv = st.get("pv") if st.get("tc_ok") else None
        hs = self.hold_snapshot()
        if hs.get("holding"):
            mode = f"유지(DAC 상한 {i0(hs.get('value'))})"
        elif self.profiler.is_active():
            mode = "램프"
        else:
            mode = "제어"
        owner = OWNER_LABEL.get(str(self.owner or ""), "-")
        return (f"히터 상태 — PV {f1(pv)}°C · SV {f1(st.get('sv_ramp'))}°C · 목표 {f1(self.final_target())}°C · "
                f"MV {i0(st.get('mv'))}/{i0(st.get('mv_limit'))} · {mode} · {owner}")

    # ═══════════════════════ 공정 끝·큐 ═══════════════════════
    def process_started(self, uses_heater: bool) -> None:
        if not uses_heater and self._st.get("run"):
            self._log("히터가 운전 중이지만 이 공정은 히터를 쓰지 않습니다(use_heater=F) — 히터는 그대로 둡니다")

    def end_reason(self, ok: bool, stopped: bool, detail: Optional[dict] = None) -> str:
        """공정이 끌 때의 종료 카드 사유(chamber_runtime.py:7151-7164)."""
        d = detail or {}
        result = str(d.get("result") or ("SUCCESS" if ok else ("STOP" if stopped else "FAIL"))).upper()
        if result == "SUCCESS":
            return "공정 종료"
        if result == "STOP":
            return "공정 중단 — 사용자 STOP" if self.stop_user else "공정 중단 — 원격 STOP"
        code = str(d.get("error_code") or "").strip().upper()
        if code:
            from v2.ch12.errors import get_error_title
            return f"공정 중단 — 오류 ({code} {get_error_title(code)})"
        txt = " ".join(str(d.get("error_detail") or "").split())
        return "공정 중단 — 오류" + (f" ({txt[:40]})" if txt else "")

    def after_row(self, ok: bool, stopped: bool, detail: Optional[dict] = None, queue_mode: bool = False,
                  next_uses_heater: bool = False, on_done: Optional[Callable[[], None]] = None) -> None:
        """공정 1행이 끝난 뒤: 다음 공정도 히터를 쓰면 유지, 아니면 끈다(공정 소유 히터만)."""
        if self.owner != "process":
            if on_done:
                on_done()
            return
        if queue_mode and ok and not stopped and next_uses_heater:
            self._log("다음 공정도 히터를 씀 → 히터 유지(다음 공정이 목표를 다시 설정)")
            if on_done:
                on_done()
            return
        self.heater_off(self.end_reason(ok, stopped, detail), on_done=on_done)

    def heater_off(self, reason: str, owner: str = "process", on_done: Optional[Callable[[], None]] = None) -> None:
        """정지 순서·공정 끝에서 — 공정 소유 히터만: RUN 이면 끄기(사유), 아니면 주체만 놓기."""
        if self.owner != owner:
            if on_done:
                on_done()
            return
        if self._st.get("run"):
            def done(ok: bool, why: str) -> None:
                self._log(f"공정이 히터를 끕니다 — {reason}" + ("" if ok else f" (실패: {why})"))
                if on_done:
                    on_done()
            self.turn_off(reason, owner=owner, on_done=done)
        else:
            self.release(owner)
            if on_done:
                on_done()

    def queue_end(self) -> None:
        """큐 종료 안전망 — 공정이 히터를 쥐고 있으면 끈다(실패·정지·검증 실패·시작 실패 경로 공통)."""
        if self.owner == "process":
            self.heater_off("공정 큐 종료")
