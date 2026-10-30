"""RF Pulse 정지·1초 읽기(챔버12) — RfPulseDriver mixin. v1 device/rf_pulse.py stop_process·stop_unused·
_power_off_sequence(772-886·888-976)와 _poll_loop(1487-1601)를 다시 쓴 것.

정지: 확인형 RF OFF 는 RFPULSE_OFF_DEADLINE_S(20 s) 안에서 RF_OFF(재시도 2)를 되풀이 — CSR 0 이면 확인, CSR ≠ 0 이면 162 로
  출력 꺼짐 확인, 아니면 0.5 s 뒤 다시. 끝내 못 하면 command_failed(RF_OFF, '출력 상태 미확인'). 켠 적 없는 출력은 RF_OFF 1회만
  (RFPULSE_UNUSED_OFF_WAIT_S 3 s) → power_off_finished(경고 없음), 기다리는 사이 RF ON 이 나갔으면 확인형으로.
1초 읽기: 162 → 165 → 166(질의 사이 CMD_GAP_MS), 166 뒤 POLL_INTERVAL_MS 쉼. tick 마다 마감 안에 보낼 수 있는 다음 질의 하나.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from v2.ch12.drivers.pulse_port import IO, Sleep, WaitLink
from v2.common import aebus

RF_OFF, Q_STATUS, Q_FWD, Q_REF = 1, 162, 165, 166
CSR_CODES = {0: "Command accepted", 1: "Wrong control mode (not HOST)", 2: "RF output is ON (cannot change this setting now)",
             4: "Data out of range / bad value", 5: "User Port RF signal OFF / interlock (model dependent)",
             7: "Active faults exist", 9: "Data byte count incorrect", 19: "Recipe mode active",
             50: "Frequency out of range", 51: "Duty out of range", 99: "Command not implemented"}

def parse_status(d: Optional[bytes]) -> Optional[Dict[str, Any]]:
    """162 4 B: b1 bit5 출력·bit6 요청·bit7 목표 미도달, b2 bit7 인터록 열림·bit3 과열, b4 bit0 전류 제한·bit5 확장 fault·bit7 CEX."""
    if not d or len(d) < 4:
        return None
    b1, b2, b4 = d[0], d[1], d[3]
    return {"on": bool(b1 & 0x20), "req": bool(b1 & 0x40), "sp_miss": bool(b1 & 0x80), "ilock": bool(b2 & 0x80),
            "ot": bool(b2 & 0x08), "limI": bool(b4 & 0x01), "xflt": bool(b4 & 0x20), "cex": bool(b4 & 0x80), "raw": bytes(d[:4])}


def status_str(st: Dict[str, Any]) -> str:
    return " ".join(f"{k}={int(st[k])}" for k in ("on", "req", "sp_miss", "ilock", "ot", "limI", "xflt", "cex"))




class RfpStopPollMixin:
    # ───────── 정지(v1 stop_process·stop_unused·_power_off_sequence) ─────────
    def off_needs_confirm(self) -> bool:
        return bool(self._output_maybe_on or self.output_off_unconfirmed)

    def _begin_stop(self, on_done) -> bool:
        self._stop_requested = True
        self.polling = False
        if on_done:
            self._off_waiters.append(on_done)
        return self._off_op is not None                 # OFF 는 런당 하나(두 번 눌러도 결과 1회)

    def stop_process(self, on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """확인형 RF OFF — RFPULSE_OFF_DEADLINE_S(20 s) 안에 power_off_finished 또는 command_failed(RF_OFF) 정확히 하나."""
        if self._begin_stop(on_done):
            return

        def gen():
            yield IO(lambda s: None, 0.5, kind="safety", purge=True, confirms_link=False, name="purge")
            return (yield from self._g_off_sequence())
        self._run("off", gen(), None, off=True)

    def stop_unused(self, on_done: Optional[Callable[[Optional[str], bool], None]] = None) -> None:
        """켠 적 없는 출력의 정지 — RF_OFF 1회(재시도 없음), RFPULSE_UNUSED_OFF_WAIT_S(3 s)까지만 → power_off_finished."""
        if self._begin_stop(on_done):
            return

        def gen():
            wait_s = max(0.5, float(self._c("RFPULSE_UNUSED_OFF_WAIT_S", 3.0)))
            t_end = self.clock.monotonic() + wait_s
            yield IO(lambda s: None, 0.5, kind="safety", purge=True, confirms_link=False, name="purge")
            res = None
            if (yield WaitLink(t_end - self.clock.monotonic())):
                ok, res = yield from self._g_exec(RF_OFF, b"", "[RF OFF 1회]", max(int(self._c("ACK_TIMEOUT_MS", 2000)), 2500),
                                                  0, "safety", until=t_end)
            if self.off_needs_confirm():
                self._emit("status", "RF OFF 1회 대기 중 RF ON 이 나감 → 확인형 RF OFF 로 전환")
                return (yield from self._g_off_sequence())
            how = ("CSR 0" if res[0] == 0 else f"CSR {res[0]}: {CSR_CODES.get(res[0], 'Unknown')}") if res else "응답 없음"
            self._emit("status", f"이번 런에서 RF 출력을 켠 적 없음 → RF OFF 1회만 보냄({how}) — 확인 대기 없이 정지 단계 완료")
            self._emit("power_off_finished")
            return True
        self._run("off_unused", gen(), None, off=True)

    def _g_off_sequence(self):
        t0 = self.clock.monotonic()
        deadline = t0 + float(self._c("RFPULSE_OFF_DEADLINE_S", 20.0))
        onoff_ms = max(int(self._c("ACK_TIMEOUT_MS", 2000)), 2500)
        attempts, confirmed = 0, False
        if not self.worker.connected:
            yield WaitLink(max(0.0, deadline - self.clock.monotonic()))
        while not confirmed and deadline - self.clock.monotonic() > 0:
            attempts += 1
            tag = "[RF OFF]" if attempts == 1 else f"[RF OFF #{attempts}]"
            ok, res = yield from self._g_exec(RF_OFF, b"", tag, onoff_ms, 2, "safety", until=deadline)
            if ok:
                confirmed = True
                break
            if res:
                self._emit("status", f"RF OFF 거부 (CSR {res[0]}: {CSR_CODES.get(res[0], 'Unknown')}) → STATUS 로 출력 확인")
                q = int(self._c("QUERY_TIMEOUT_MS", 4500))
                st = parse_status((yield from self._g_query(Q_STATUS, "[OFF STATUS]", q, 1, "safety",
                                                            wait_s=min(q / 1000.0 + 1.0, max(0.05, deadline - self.clock.monotonic())))))
                if st is not None and not st["on"]:
                    confirmed = True
                    break
            else:
                self._emit("status", f"RF OFF 미확인 → 재시도 (남은 {max(0.0, deadline - self.clock.monotonic()):.0f}초)")
            if deadline - self.clock.monotonic() <= 0:
                break
            yield Sleep(0.5)
        if confirmed:
            self._output_maybe_on = False
            self.output_off_unconfirmed = False
            self._emit("power_off_finished")
            return True
        self.output_off_unconfirmed = True
        el = max(0.0, self.clock.monotonic() - t0)
        self._emit("status", f"RF OFF 확인 실패 — {el:.0f}초 동안 {attempts}회 시도, 출력 상태 미확인 "
                             f"(연결={'O' if self.worker is not None and self.worker.connected else 'X'})")
        self._failed("RF_OFF", f"RF Pulse OFF 미확인 — {el:.0f}초 동안 {attempts}회 시도, 출력 상태 미확인")
        return False

    # ───────── 1초 읽기(v1 _poll_loop 1487-1601 의 순서·간격) ─────────
    def _start_polling(self) -> None:
        self.polling = True
        self._poll_step = 0
        self._cycle_next = -1e9

    def _state(self) -> Dict[str, Any]:
        b = self.block
        return {f"{b}.set_w": self._target_w, f"{b}.freq_hz": self.freq_hz,
                f"{b}.duty_pct": self.duty_pct, f"{b}.polling": self.polling}

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        out = self._state()
        if not self.polling or self._op is not None or self._off_op is not None:
            return out
        due = max(self._ready_at, self._cycle_next) if self._poll_step == 0 else self._ready_at
        if max(due, self.clock.monotonic()) + 0.1 > session.deadline:
            return out                                          # 이번 tick 에는 보낼 차례가 아님(간격·쉼)
        if due > self.clock.monotonic():
            self.clock.sleep_until(due)
        step = self._poll_step
        cmd, tag = ((Q_STATUS, "[POLL WAKE]"), (Q_FWD, "[POLL FWD]"), (Q_REF, "[POLL REF]"))[step]
        q_s = self._ms("POLL_QUERY_TIMEOUT_MS", int(self._c("QUERY_TIMEOUT_MS", 4500)))
        b = self.block
        try:
            data = self._transact(session, cmd, b"", q_s, session.deadline, tag, False)
        except Exception:
            self._poll_advance(step)
            if step == 2:
                self._judge()                                   # v1: 이번 바퀴가 실패해도 마지막 값으로 감시
            raise
        self._poll_advance(step)
        if step == 0:
            st = parse_status(data)
            if st is None:
                self._emit("status", "STATUS payload too short")
            else:
                self.last_status = st
                out.update({f"{b}.out_on": st["on"], f"{b}.itl_open": st["ilock"], f"{b}.status_raw": st["raw"].hex()})
                self._emit("rf_status", status_str(st), data=dict(st))
                for flag, msg in (("ilock", "STATUS: Interlock OPEN detected"), ("ot", "STATUS: Over-Temperature detected"),
                                  ("xflt", "STATUS: Extended fault present")):
                    if st[flag]:
                        self._emit("status", msg)
                if st["req"] and not st["on"]:
                    self._emit("status", "STATUS: RF requested but output not ON yet")
                self._emit("status", f"STATUS {status_str(st)}")
        elif step == 1:
            self.last_fwd = float(aebus.u16(data) if len(data) >= 2 else 0.0)
            out[f"{b}.fwd_w"] = self.last_fwd
        else:
            self.last_ref = float(aebus.u16(data) if len(data) >= 2 else 0.0)
            out[f"{b}.ref_w"] = self.last_ref
            self._judge()
        return out

    def _poll_advance(self, step: int) -> None:
        self._ready_at = self.clock.monotonic() + self._gap_s()
        self._poll_step = (step + 1) % 3
        if step == 2:
            self._cycle_next = self.clock.monotonic() + self._ms("POLL_INTERVAL_MS", 1000)

    def _judge(self) -> None:
        if self._target_w > 0.0 and self.last_fwd is not None and self.last_ref is not None and not self._stop_requested:
            pct, n_f = float(self._c("RFPULSE_FORP_TOLERANCE_PERCENT", 5.0)), int(self._c("RFPULSE_FORP_CONSECUTIVE_LIMIT", 3))
            lim_w, n_r = float(self._c("RFPULSE_REFP_LIMIT_WATTS", 20.0)), int(self._c("RFPULSE_REFP_CONSECUTIVE_LIMIT", 3))
            warn_w = float(self._c("RFPULSE_REFP_WARN_WATTS", 0.0))
            tol = self._target_w * (pct / 100.0)
            self._forp_n = self._forp_n + 1 if abs(self.last_fwd - self._target_w) >= tol else 0
            if self._forp_n >= n_f:
                self._emit("status", f"FORP setpoint 이탈: meas={self.last_fwd:.1f}W, target={self._target_w:.1f}W, "
                                     f"허용오차=±{pct:.1f}% ({self._forp_n}회 연속)")
                self._emit("command_failed", f"FORP가 setpoint에서 {pct:.1f}% 이상 이탈({n_f}회 연속)",
                           reason="RFP_FORP", cmd="FORP_MONITOR")
                self._forp_n = 0
            self._refp_n = self._refp_n + 1 if self.last_ref >= lim_w else 0
            if self._refp_n >= n_r:
                self._emit("status", f"REFP 과다 반사: meas={self.last_ref:.1f}W, limit={lim_w:.1f}W ({self._refp_n}회 연속)")
                self._emit("command_failed", f"REFP가 {lim_w:.1f}W 이상 ({n_r}회 연속)", reason="RFP_REFP", cmd="REFP_MONITOR")
                self._refp_n = 0
            if warn_w > 0.0:
                if self.last_ref >= warn_w and not self._refp_warn_active:
                    self._emit("status", f"⚠ REFP_WARN: REFP={self.last_ref:.1f}W ≥ {warn_w:.1f}W (공정은 계속 진행)")
                    self._refp_warn_active = True
                elif self.last_ref < warn_w and self._refp_warn_active:
                    self._emit("status", f"REFP 정상 복귀: REFP={self.last_ref:.1f}W < {warn_w:.1f}W")
                    self._refp_warn_active = False
        if self.last_fwd is not None and self.last_ref is not None:
            self._emit("power", f"fwd={self.last_fwd:.0f} W ref={self.last_ref:.0f} W",
                       data={"forward": self.last_fwd, "reflected": self.last_ref})

