"""플라즈마 클리닝 정지·정리 — v1 runtime/plasma_cleaning_runtime.py 의 _safe_rf_stop·_force_rf_zero_then_set_off·
_force_finalize_rf_stop·_note_rf_off_unconfirmed·_shutdown_rest_devices·_release_mfcs_and_finalize·_final_cleanup(PC-071~078).

RF 안전 정지(_safe_rf_stop, 1790-1832) — 모든 종료 경로 공통
  공정 타이머 끔 → rf.cleanup()(= RfPower.stop) PC_RF_CLEANUP_TIMEOUT_S 5 s → wait_power_off PC_RF_WAIT_POWER_OFF_TIMEOUT_S 15 s
  → 0 W 확인 완료 신호가 없으면: RF 안쪽 동작(램프다운·보정·측정) 먼저 멈춤 → '강제 0W/SET OFF' → RF 상태 강제 초기화 → 나머지 장치.
  v1 cleanup() 은 kick 램프다운(목표 > PC_RF_RAMP_KICK_THRESHOLD_W)이면 램프다운을 띄우고 바로 돌아오고, 그 밖(직접 OFF)은 0 W 확인까지
  안에서 기다린다 — 그래서 '5 s 초과' 문구는 직접 OFF 경로에서만 나온다. 완료 신호는 0 W 가 확인됐을 때만(미확인이면 15 s 를 다 기다림).
강제 0W(1721-1757): DCV_WRITE_1 ← 0 W 를 0.5 s 간격으로, POWER_OFF_ZERO_DEADLINE_S(10 s) 를 넘기면 미확인 알림(런당 1회)·SET OFF 안 함.
  0 W 확인 뒤 DCV_SET_1 OFF 최대 3회 0.2 s 간격, 끝내 실패면 'RF SET OFF 실패 — 0W 는 확인됨 (…)' 로그만. 쓰기는 worker 의 safety
  우선순위(램프다운 쓰기와 같은 줄 — 넣은 순서대로라 0 W 뒤에 램프다운 쓰기가 끼지 않는다, TEST-109).
나머지 장치(2069-2097): 선택 가스 FLOW OFF → SP4 쪽 MFC VALVE OPEN → MFC 소유권 해제(가스 OFF '뒤') → 게이트 밸브는 닫지 않음.
MFC 해제(2018-2066): 런 사용 해제 먼저 → MFC 마다 폴링 사용자에서 빼고 ① 남은 폴링 0·다른 런 없음 → 마스크 원복 + on_process_cleanup
  ② 폴링 0·다른 런 있음 → 마스크 원복·폴링 끔·release_owner·'대기열/상태 유지' ③ 다른 폴링 사용자 → release_owner·'폴링 유지'.
_final_cleanup(1837-1898): 런당 1회. TEST 면 장비 정리 없이 로그만. IG cancel_wait 2 s → RF 안전 정지 → (못 탔으면) MFC 해제 →
  0.05 s → 이벤트 펌프 내림 3 s → (disconnect_on_finish — v1 main 이 넘기지 않아 늘 꺼짐, PC-079) → 공정 로그 닫기 → 런 사용 해제.
의도적 종료(power_only — 엔진 program_exit, S32): IG·RF·MFC 장비 명령 없이 소유·폴링 표시·이벤트 펌프·공정 로그만 정리(PC RF 는
  안전 규칙이 rf_zero_off 로 끄고, 가스 유량·SP4 밸브는 그대로 — 챔버K 결정 4 'MFC 그대로').
[이상] 프리플라이트 GV 인터락 FALSE·읽기 실패도 이 정리를 탄다 — 켠 것이 없어도 RF 0 W·VALVE OPEN 등을 보냄(PC-096, D104 v1 그대로).
"""
from __future__ import annotations

from typing import Optional

from v2.common.worker import Status
from v2.ch12.pc_steps import Box, err_of

ZERO_RETRY_S = 0.5
SET_OFF_TRIES = 3
SET_OFF_RETRY_S = 0.2


def apply_rf_off_unconfirmed(ok: bool, stopped: bool, reason: Optional[str], off_reason: Optional[str]):
    """3335-3343 — RF OFF 미확인이고 아직 성공인 런만 실패로(이미 실패·STOP 이면 그대로)."""
    if off_reason and ok:
        return False, False, str(off_reason)
    return ok, stopped, reason


class StopIO:
    """엔진이 섞어 쓴다 — self.clock·self.cfg·self.dev·self.log·self.ch·self.run_state·self.pc …"""

    def _cfgf(self, key: str, default: float) -> float:
        try:
            v = self.cfg.get(key, default)
            return float(default if v is None else v)
        except Exception:
            return float(default)

    # ───────── RF ─────────
    def note_rf_off_unconfirmed(self, reason: str) -> None:
        if self.rf_off_unconfirmed_reason:
            return
        self.rf_off_unconfirmed_reason = str(reason)
        alert = (f"⚠️ CH{self.ch} Plasma Cleaning RF Power OFF 실패 - 출력 상태 미확인 "
                 f"(켜져 있을 가능성 있음). RF 전원 출력과 PLC D/A(DCV_SET_1/DCV_WRITE_1)를 즉시 확인하세요.")
        self.log("RF", alert)
        self.card("notify_error_with_src", src="PC", text=alert, level="error", code="RF_OFF_UNCONFIRMED")

    def stop_rf_internal(self) -> None:
        """_stop_rf_internal_tasks — 램프다운·보정·측정을 멈춘다(이미 worker 에 들어간 쓰기는 넣은 순서대로 끝난다)."""
        rf = self.dev.rf
        if rf is None:
            return
        rf.ramping_down = False
        rf.polling, rf._due = False, None
        rf._gen += 1

    def _plc_call(self, submit) -> Optional[str]:
        """PLC 요청 하나 — 성공이면 None, 실패면 문구."""
        box = Box(self.clock)
        submit(box.set)
        self.clock.wait_until(None, lambda: box.done)
        res = box.value
        return None if res.status == Status.OK else (err_of(res) or "error")

    def force_rf_zero_then_set_off(self) -> bool:
        plc = self.dev.plc
        if plc is None:
            self.note_rf_off_unconfirmed("PLC 없음 — 강제 0W 를 보낼 수 없음")
            return False
        deadline = self._cfgf("POWER_OFF_ZERO_DEADLINE_S", 10.0) or 10.0
        t0 = self.clock.monotonic()
        n = 0
        while True:
            n += 1
            err = self._plc_call(lambda cb: plc.power_write(0.0, family="DCV", write_idx=1, kind="safety", on_done=cb))
            if err is None:
                break
            el = self.clock.monotonic() - t0
            if el >= deadline:
                self.note_rf_off_unconfirmed(f"강제 0W 쓰기 실패 ({el:.1f}초, {n}회, {err})")
                return False
            self.clock.wait_until(self.clock.monotonic() + ZERO_RETRY_S)
        for i in range(SET_OFF_TRIES):
            err = self._plc_call(lambda cb: plc.power_enable(False, family="DCV", set_idx=1, kind="safety", on_done=cb))
            if err is None:
                return True
            if i >= SET_OFF_TRIES - 1:
                self.log("RF", f"RF SET OFF 실패 — 0W 는 확인됨 ({err})")
                return True
            self.clock.wait_until(self.clock.monotonic() + SET_OFF_RETRY_S)
        return True

    def force_finalize_rf_stop(self) -> None:
        rf = self.dev.rf
        if rf is None:
            return
        self.stop_rf_internal()
        rf.ramping_down = rf.running = rf.enabled = False
        rf.state = "IDLE"
        rf.target_power = rf.current_step = 0.0
        rf.last_sent = 0.0
        done, rf._stop_done = list(rf._stop_done), []          # v1 _power_off_evt.set() — 기다리는 쪽을 풀어 준다
        for f in done:
            try:
                f(None)
            except Exception:
                pass

    def safe_rf_stop(self) -> None:
        self.process_timer_active = False
        rf = self.dev.rf
        if rf is None:
            self.shutdown_rest_devices()
            return
        cleanup_to = self._cfgf("PC_RF_CLEANUP_TIMEOUT_S", 5.0)
        wait_to = self._cfgf("PC_RF_WAIT_POWER_OFF_TIMEOUT_S", 15.0)
        early = rf.ramping_down and not rf.running             # v1 cleanup 첫 줄 — 이미 내리는 중이면 바로 돌아옴
        inline = (not early) and rf.direct and rf.target_power <= rf.p("kick_th")
        box = Box(self.clock)
        rf.stop(on_done=box.set)
        ok_done = lambda: box.done and box.value is None        # noqa: E731 — 0 W 확인 완료 신호
        if inline:
            self.clock.wait_until(self.clock.monotonic() + cleanup_to, lambda: box.done)
            if not box.done:
                self.log("RF", f"cleanup timeout({cleanup_to:.1f}s) → 계속 진행")
        self.clock.wait_until(self.clock.monotonic() + wait_to, ok_done)
        if not ok_done():
            self.stop_rf_internal()
            if self.dev.plc is not None:
                self.log("RF", "ramp-down 완료 신호 timeout → 강제 0W/SET OFF")
                self.force_rf_zero_then_set_off()
            else:
                self.note_rf_off_unconfirmed("PLC 없음 — 강제 0W 를 보낼 수 없음")
            self.force_finalize_rf_stop()
        self.shutdown_rest_devices()

    # ───────── MFC·나머지 장치 ─────────
    def _mfc_op(self, start) -> None:
        box = Box(self.clock)
        start(lambda k, r, d: box.set((k, r, d)))
        self.clock.wait_until(None, lambda: box.done)

    def shutdown_rest_devices(self) -> None:
        owner = self.mfc_owner()
        g, pr = self.dev.mfc_gas, self.dev.mfc_pressure
        if g is not None:
            try:
                self.log("STEP", "종료: MFC GAS OFF(sel)")
                self._mfc_op(lambda cb: g.flow_off_selected(owner=owner, on_done=cb))
                self.log("STEP", "종료: MFC GAS OFF OK")
            except Exception:
                pass
        if pr is not None:
            try:
                self.log("STEP", "종료: MFC(SP4) VALVE OPEN")
                self._mfc_op(lambda cb: pr.valve_open(owner=owner, on_done=cb))
                self.log("STEP", "종료: MFC(SP4) VALVE OPEN OK")
            except Exception:
                pass
        try:
            self.release_mfcs_and_finalize()
        except Exception:
            pass
        self.log("STEP", f"종료: GateValve CH{self.ch} 유지(OPEN)")

    def mfc_targets(self) -> list:
        out = []
        for m in (self.dev.mfc_gas, self.dev.mfc_pressure):
            if m is not None and all(m is not x for x in out):
                out.append(m)
        return out

    def acquire_mfcs(self) -> None:
        owner = self.mfc_owner()
        for m in self.mfc_targets():
            m.poll_users.add(owner)                  # v1 runtime_state.acquire_shared — 사용자 집합만(마스크·폴링은 따로)
            self.mfc_claims[m.resource_key] = owner

    def end_mfc_run_use(self, clear_owner: bool = False) -> None:
        if self.run_owner:
            self.run_state.end_run_use(self.run_owner)
        if clear_owner:
            self.run_owner = None

    def release_mfcs_and_finalize(self) -> None:
        self.end_mfc_run_use()
        for m in self.mfc_targets():
            key = m.resource_key
            owner = self.mfc_claims.pop(key, self.mfc_owner())
            m.poll_users.discard(owner)
            left = len(m.poll_users)
            others_run = set(self.run_state.run_users(key)) - {owner}
            if left == 0 and not others_run:
                m.set_poll_mask(gas=True, pressure=True)
                m.on_process_cleanup()
            elif left == 0:
                m.set_poll_mask(gas=True, pressure=True)
                m.set_process_status(False)
                m.release_owner(owner, reason="plasma cleaning finished")
                self.log("MFC", f"{key} 대기열/상태 유지 — 다른 공정 진행 중: {', '.join(sorted(others_run))} "
                                f"(폴링만 중지, PC 몫만 정리)")
            else:
                m.release_owner(owner, reason="plasma cleaning finished")
                self.log("MFC", f"{key} 폴링 유지 — 사용 중: {', '.join(sorted(m.poll_users))}")
        self.mfc_release_done = True

    # ───────── 런 끝 정리 ─────────
    def cleanup_tasks_only(self, reason: str = "") -> None:
        if reason:
            self.log("MAIN", f"[CLEANUP_TASKS_ONLY] {reason}")
        self.pumps_off()
        self.run_log.close()

    def final_cleanup(self) -> None:
        if self.cleanup_started:
            return
        self.cleanup_started = True
        self.phase = "stopping"
        self.log("MAIN", "[CLEANUP] begin")
        try:
            if self.test_mode_active:
                self.log("MAIN", "[CLEANUP] TEST MODE: 장비 정리(RF/MFC/PLC) 스킵, 태스크/로그만 정리")
                self.pumps_off()
                self.run_log.close()
                return
            if getattr(self, "power_only", False):        # 의도적 종료(S32) — 장비 명령 없이 기록·소유만 정리
                self.log("MAIN", "[CLEANUP] 의도적 종료 — RF 안전 정지·가스 OFF·VALVE OPEN 생략(PC RF 는 S32 가 즉시 끔, MFC 그대로)")
                self.process_timer_active = False
                if not self.mfc_release_done:
                    try:
                        self.release_mfcs_and_finalize()     # 소유·폴링 표시만(장비 쓰기 없음 — 유량은 그대로)
                    except Exception:
                        pass
                self.pumps_off()
                self.run_log.close()
                return
            ig = self.dev.ig
            if ig is not None:
                box = Box(self.clock)
                ig.cancel_wait(reason="cleanup", on_done=lambda k, r, d: box.set(r))
                self.clock.wait_until(self.clock.monotonic() + 2.0, lambda: box.done)
            try:
                self.safe_rf_stop()
            except Exception:                           # v1 suppress(Exception) — 로그 없음
                pass
            if not self.mfc_release_done:
                try:
                    self.release_mfcs_and_finalize()
                except Exception:
                    pass
            self.clock.wait_until(self.clock.monotonic() + 0.05)
            self.pumps_off()
            self.run_log.close()
        finally:
            self.end_mfc_run_use(clear_owner=True)
            self.log("MAIN", "[CLEANUP] end")
