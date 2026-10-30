"""CH2 히터 엔진 — 명령. v1 controller/heater_service.py:189-466(turn_on·turn_off·apply_target·reset_fault·take_over·
set_hold_armable·force_hold·shutdown·claim·release) + ui/heater_panel.py:325-401 의 패널 명령 의미.

v1 은 명령마다 asyncio.Lock 을 잡고 PLC 쓰기를 차례로 await 했다. v2 는 명령 하나 = PLC worker 요청 하나(폴링 처리와 같은 worker
— 섞이지 않는다): 요청 안에서 v1 과 같은 순서로 검사·쓰기를 하고 결과를 on_done(ok, why) 로 돌려준다.
링크가 없으면 요청을 보내지 않고 v1 의 '링크 없음' 갈래를 바로 실행한다(끄기는 보류 OFF, 켜기·적용·리셋은 거부).
v1 이상 동작 그대로(결정 D144): 패널 [적용] 은 owner 없이 apply_target 을 불러 공정 소유 운전 중에도 목표가 바뀐다.
"""
from __future__ import annotations

from typing import Callable, Optional, Tuple

from v2.common.worker import Status
from v2.ch12.drivers import heater as H
from v2.ch12.heater_notify import OWNER_LABEL

Done = Optional[Callable[[bool, str], None]]


class HeaterCommands:
    # ═══════════════════════ 실행 틀 ═══════════════════════
    def _reply(self, on_done: Done, r: Tuple[bool, str]) -> None:
        if on_done is not None:
            try:
                on_done(bool(r[0]), str(r[1] or ""))
            except Exception as e:
                self._log(f"⚠ 명령 결과 콜백 예외: {e!r}")

    def _cmd(self, name: str, fn_locked: Callable[[], Tuple[bool, str]], on_done: Done,
             offline: Optional[Callable[[], Tuple[bool, str]]] = None, after_ok: Optional[Callable[[], None]] = None) -> None:
        """fn_locked 를 PLC worker 안(세션·잠금)에서. 링크가 없으면 offline()(잠금, 세션 없음) 또는 'PLC 링크 없음'."""
        if not self.writes_allowed:
            self._reply(on_done, (False, "BENCH_READ_ONLY — 읽기 전용 모드에서는 히터 명령을 보내지 않습니다"))
            return
        if not self._link_up():
            with self._lock:
                r = offline() if offline is not None else (False, "PLC 링크 없음")
            self._flush()
            self._reply(on_done, r)
            return

        def fn(s):
            with self._lock:
                self._s = s
                try:
                    r = fn_locked()
                    if r[0] and after_ok is not None:
                        after_ok()
                    return r
                finally:
                    self._s = None

        def done(res) -> None:
            self._flush()
            if res.status == Status.OK and isinstance(res.value, tuple):
                self._reply(on_done, res.value)
            elif res.status == Status.LINK_DOWN and offline is not None:
                with self._lock:
                    r = offline()
                self._flush()
                self._reply(on_done, r)
            else:
                self._reply(on_done, (False, f"PLC 쓰기 실패: {res.reason or res.status}"
                                             + (f" {res.error}" if res.error else "")))
        self.drv.submit(name, fn, on_done=done)

    # ═══════════════════════ 소유 ═══════════════════════
    def claim(self, owner: str) -> bool:
        """v1 claim — v1 안에서 부르는 곳 없음(HEAT-121), 시험용으로만 남긴다."""
        with self._lock:
            if self.owner in (None, owner):
                self.owner = owner
                return True
            return False

    def release(self, owner: str) -> None:
        with self._lock:
            if self.owner == owner:
                self.owner = None
                if owner == "process":
                    self._proc_ctx = None

    def take_over(self, owner: str) -> None:
        """공정이 수동·외부 운전 중인 히터를 넘겨받는다."""
        with self._lock:
            if self.owner not in (None, owner):
                self._log(f"{OWNER_LABEL.get(self.owner, self.owner)} 운전 중인 히터를 {OWNER_LABEL.get(owner, owner)}이(가) 넘겨받음")
            self.owner = owner

    def _process_locked_out(self, owner: Optional[str]) -> bool:
        return (owner is not None and owner != "process" and self.owner == "process"
                and bool(self._st.get("run")))

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

    # ═══════════════════════ 켜기·끄기·목표 ═══════════════════════
    def turn_on(self, target_c: float, rate: Optional[float] = None, owner: str = "manual", on_done: Done = None) -> None:
        self._cmd("turn_on", lambda: self._turn_on_locked(target_c, rate, owner), on_done, after_ok=self._kick_wd)

    def _turn_on_locked(self, target_c, rate, owner) -> Tuple[bool, str]:
        ok, why = self.is_ready()
        if not ok:
            return False, why
        if self._st.get("run"):
            return False, "이미 운전 중입니다 — 목표를 바꾸려면 [적용]을 쓰십시오"
        ok, why = self._target_ok(target_c)
        if not ok:
            return False, why
        if self.owner not in (None, owner):
            return False, f"제어 주체 충돌 — 현재 {OWNER_LABEL.get(self.owner, self.owner)}"
        target = float(target_c)
        r = float(rate) if rate else float(self.cfg.ramp_rate)
        try:
            started = self.profiler.start(target, r, OWNER_LABEL.get(owner, owner))
            if not started:
                self._write_sv(target, force_log=True)
            self._w_coil(H.COIL_RUN, True)
        except Exception as e:
            self.profiler.stop(restore_rate=True)
            self._log(f"⚠ 히터 ON 실패: {e!r}")
            return False, f"PLC 쓰기 실패: {e}"
        self.owner = owner
        self._end_reason = None
        self._log(f"운전 ON 요청 — 목표 {target:.1f}°C ({OWNER_LABEL.get(owner, owner)})")
        return True, ""

    def turn_off(self, reason: str = "정지", owner: Optional[str] = None, on_done: Done = None) -> None:
        with self._lock:
            if self._process_locked_out(owner):
                r = (False, "공정이 히터를 제어 중입니다")
            else:
                r = None
        if r is not None:
            self._reply(on_done, r)
            return

        def locked():
            if self._process_locked_out(owner):
                return False, "공정이 히터를 제어 중입니다"
            return self._turn_off_locked(reason)
        self._cmd("turn_off", locked, on_done, offline=locked)

    def _turn_off_locked(self, reason: str) -> Tuple[bool, str]:
        self._end_reason = reason
        io = self._s is not None and self._link_up()
        if self.profiler.is_active():
            self.profiler.stop(restore_rate=True)
        elif self._st.get("ok") is not None and self._st.get("ramp_rate") not in (None, self._ramp_c_per_min()):
            if io:
                try:
                    self._write_ramp_counts(self.cfg.rate_counts(self.cfg.ramp_rate))
                except Exception:
                    pass
        if not io:
            self._pending_off = True
            self._log("링크 다운 — 히터 OFF 를 보류(복구 시 1회 적용)")
            return True, "링크 다운 — OFF 보류(복구 시 적용)"
        try:
            self._w_coil(H.COIL_RUN, False)
            self._pending_off = False
            self._log(f"운전 OFF ({reason})")
            return True, ""
        except Exception as e:
            self._pending_off = True
            self._log(f"⚠ 히터 OFF 쓰기 실패 → 보류(다음 폴링에 재시도): {e!r}")
            return True, "쓰기 실패 — OFF 보류(다음 폴링에 재시도)"

    def apply_target(self, target_c: float, rate: Optional[float] = None, owner: Optional[str] = None,
                     on_done: Done = None) -> None:
        def offline():
            if self._process_locked_out(owner):
                return False, "공정이 히터를 제어 중입니다"
            return False, "PLC 링크 없음"
        self._cmd("apply_target", lambda: self._apply_target_locked(target_c, rate, owner), on_done, offline=offline)

    def _apply_target_locked(self, target_c, rate, owner) -> Tuple[bool, str]:
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
                self.profiler.stop(restore_rate=False)
                if not self.profiler.start(target, rate, "공정" if owner == "process" else "적용"):
                    self._write_sv(target, force_log=True)
            else:
                self._write_sv(target, force_log=True)
        except Exception as e:
            return False, f"PLC 쓰기 실패: {e}"
        return True, ""

    def reset_fault(self, on_done: Done = None) -> None:
        def offline():
            if self._st.get("run"):
                return False, "히터 RUN 이 켜져 있습니다 — 먼저 OFF 한 뒤 리셋하십시오"
            return False, "PLC 링크 없음"

        def locked():
            if self._st.get("run"):
                return False, "히터 RUN 이 켜져 있습니다 — 먼저 OFF 한 뒤 리셋하십시오"
            try:
                self._w_coil(H.COIL_RST, True)
            except Exception as e:
                return False, f"PLC 쓰기 실패: {e}"
            self._log("사용자가 히터 이상 리셋을 요청했습니다(RST)")
            self._reset_pending = True
            return True, ""
        self._cmd("reset_fault", locked, on_done, offline=offline)

    # ═══════════════════════ 유지·포화(공정 전용) ═══════════════════════
    def set_hold_armable(self, on: bool, why: str = "", on_done: Done = None) -> None:
        self._cmd("hold_armable", lambda: self._set_hold_armable_locked(on, why), on_done,
                  offline=lambda: self._set_hold_armable_locked(on, why))

    def _set_hold_armable_locked(self, on: bool, why: str = "") -> Tuple[bool, str]:
        """금지: holding·클램프를 풀고 D118 을 MV_LIMIT 로 바로 되돌린다. 허용: 지금 조건에서 새로 측정·다시 무장."""
        self._mv_requests.clear()
        if not on and self.hold.is_holding():
            self.hold.release(why or "공정이 유지 재측정 요청")
        self.sat.set_clamp_enabled(on, why)
        self.hold.set_armable(on)
        self._run_mv_requests("원복")
        return True, ""

    def force_hold(self, why: str, on_done: Done = None) -> None:
        self._cmd("force_hold", lambda: self._force_hold_locked(why), on_done,
                  offline=lambda: (False, "히터가 꺼져 있거나 PLC 응답 없음"))

    def _force_hold_locked(self, why: str) -> Tuple[bool, str]:
        st = self._st
        if not st.get("run") or st.get("stale"):
            return False, "히터가 꺼져 있거나 PLC 응답 없음"
        if self.hold.is_holding():
            return True, f"이미 유지 중(DAC 상한 {self.hold.value})"
        recent = self.sat.recent_mv_avg()
        self._mv_requests.clear()
        if not self.hold.force_dac_hold(recent if recent is not None else st.get("mv"), prefer_current=False):
            return False, self.hold.last_force_reason or "강제 진입 실패"
        self._run_mv_requests()
        src = self.hold.last_force_source
        self._event("유지 모드 강제 진입")
        self._card("유지 모드 강제 진입", f"{self.context()} · {why} — DAC 상한 {self.hold.value} 고정({src})", "FAIL",
                   {"PV": self._fmt(st.get("pv")), "MV": str(st.get("mv"))})
        return True, f"DAC 상한 {self.hold.value} 고정({src})"

    # ═══════════════════════ 종료 ═══════════════════════
    def shutdown(self, on_done: Optional[Callable[[], None]] = None) -> None:
        """프로그램 종료(v1 shutdown, heater_service.py:433-466): 폴링·워치독 정지 → RUN 이고 링크 있으면 RUN=0 1회 → CSV 닫기.
        RUN=0 은 안전 요청(앞의 쓰기 대기열을 버리고 맨 앞)."""
        with self._lock:
            self._stopping = True
            run = bool(self._st.get("run"))

        def finish(done: bool) -> None:
            with self._lock:
                if run and not done:
                    self._log("⚠ 종료 시 히터 OFF 를 보내지 못함 — PLC 워치독이 30초 뒤 차단")
                try:
                    if self.csv is not None and self.csv.is_open():
                        self.csv.write_row(self._st, owner=self.owner or "", note="프로그램 종료")
                    if self.csv is not None:
                        self.csv.stop()
                except Exception:
                    pass
            if on_done is not None:
                on_done()
        if not run or not self._link_up() or not self.writes_allowed:
            finish(False)
            return

        def fn(s):
            with self._lock:
                try:
                    self.drv.w_coil(s, H.COIL_RUN, False)
                    self._log("프로그램 종료 — 히터 RUN OFF")
                    return True
                except Exception as e:
                    self._log(f"⚠ 종료 시 히터 OFF 실패: {e!r}")
                    return False
        self.drv.submit("shutdown_off", fn, kind="safety",
                        on_done=lambda res: finish(res.status == Status.OK and bool(res.value)))

    # ═══════════════════════ 패널(ui/heater_panel.py:325-401) ═══════════════════════
    def panel_target(self, text) -> Tuple[Optional[float], str]:
        """패널 목표 칸 검사(_target_value) — (값, '') 또는 (None, 사유)."""
        txt = ("" if text is None else str(text)).strip()
        if not txt:
            return None, "목표 온도를 입력하십시오"
        try:
            t = float(txt)
        except Exception:
            return None, "목표 온도가 숫자가 아닙니다"
        lim = float(self.cfg.max_temp)
        svl = self._st.get("sv_limit")
        if svl:
            lim = min(lim, float(svl))
        if not (0.0 < t <= lim):
            return None, f"목표 범위 밖 — 0 초과 {lim:.1f}°C 이하"
        return t, ""

    def panel_onoff(self, target_text, on_done: Done = None) -> None:
        """[ON/OFF] — RUN 이면 끄기('정지(패널)', owner manual), 아니면 목표 검사 후 켜기(owner manual, 속도 = 설정 기본)."""
        if self._st.get("run"):
            self.turn_off("정지(패널)", owner="manual", on_done=on_done)
            return
        t, why = self.panel_target(target_text)
        if t is None:
            self._reply(on_done, (False, why))
            return
        self.turn_on(t, owner="manual", on_done=on_done)

    def panel_apply(self, target_text, on_done: Done = None) -> None:
        """[적용] — v1 그대로 owner 없이(공정 잠금 안 걸림, 결정 D144)."""
        t, why = self.panel_target(target_text)
        if t is None:
            self._reply(on_done, (False, why))
            return
        self.apply_target(t, on_done=on_done)

    def panel_reset(self, on_done: Done = None) -> None:
        """[리셋] — RUN 이면 '리셋 불가'(확인창은 화면 몫)."""
        if self._st.get("run"):
            self._reply(on_done, (False, "히터 RUN 이 켜져 있습니다.\n먼저 히터를 OFF 한 뒤 리셋하십시오."))
            return
        self.reset_fault(on_done=on_done)

    # 짧은 이름(웹·로컬 명령 표가 쓰는 이름)
    def start_manual(self, target_c: float, rate: Optional[float] = None, on_done: Done = None) -> None:
        self.turn_on(target_c, rate=rate, owner="manual", on_done=on_done)

    def stop_manual(self, on_done: Done = None) -> None:
        self.turn_off("정지(패널)", owner="manual", on_done=on_done)
