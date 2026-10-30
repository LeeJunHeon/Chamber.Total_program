"""CH2 히터 엔진 — 잔존 RUN 정리·FAULT/RST 엣지·출력 죽음·출력 무응답 처리·RUN 상승/하강(카드·CSV·종료 사유).
v1 controller/heater_service.py:558-572·859-885·1047-1188 그대로(HeaterEngine 이 섞어 씀).
"""
from __future__ import annotations

from v2.ch12.drivers import heater as H
from v2.ch12.heater_notify import NORMAL_END_REASONS, OWNER_LABEL

OUT_DEAD_GRACE_SEC = 5.0
OUT_DEAD_COUNT = 3
RESIDUAL_FAIL_LOG_INTERVAL_S = 10.0
RST_STUCK_POLLS = 2


class HeaterEdges:
    def _clear_residual_run(self, st: dict) -> bool:
        try:
            self._w_coil(H.COIL_RUN, False)
            if st["pv"] is not None:
                self._w_reg(H.REG_SV, max(0, min(32767, int(st["_raw_pv"]))))
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

    def _fault_edges(self, st: dict, first: bool) -> None:
        if st["fault"] and not self._fault_prev:
            why = self._fault_text(st)
            if first:
                why = f"{why} — 기존 래치(프로그램 시작 시 확인)"
            self._fault_why = why
            if st["run"] and not self._fault_off_sent:
                self._fault_off_sent = True
                try:
                    self._w_coil(H.COIL_RUN, False)
                except Exception as e:
                    self._pending_off = True
                    self._log(f"⚠ 이상 시 RUN OFF 쓰기 실패 → 보류: {e!r}")
            if self.profiler.is_active():
                self.profiler.stop(restore_rate=False)
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

    def _rst_check(self, st: dict) -> None:
        if not st.get("rst"):
            self._rst_n = 0
            return
        self._rst_n += 1
        if self._rst_n < RST_STUCK_POLLS or not self.writes_allowed:
            return
        try:
            self._w_coil(H.COIL_RST, False)
            self._rst_n = 0
            self._log("⚠ 히터 리셋 코일(M00403)이 1 로 남아 있어 0 으로 되돌림 — 래더의 RST 자기 리셋 확인 필요")
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

    def _on_no_response(self, st: dict, info: dict) -> None:
        """출력 무응답·끊김 판정 — ❌ 카드 → 히터 OFF(종료 사유 = 라벨). 이번 발행 상태 noresp 에 원인 문구(공정 E832 용)."""
        label = str(info.get("label") or "출력 무응답")
        detail = str(info.get("text") or "")
        hint = str(info.get("hint") or "")
        st["noresp"] = f"{label} — {detail}"
        self._event(label)
        self._log(f"⚠ 히터 {label}: {detail} → 히터를 끕니다 ({hint})")
        self._card(label, f"{self.context()} · {label} — 배선·VARITAP·차단기 확인", "FAIL",
                   {"판정": detail, "PV": self._fmt(info.get("pv")),
                    "MV": f"{float(info.get('mv') or 0):.0f}/{float(info.get('limit') or 0):.0f}", "확인": hint})
        self._turn_off_locked(label)

    def _on_run_rise(self, st: dict) -> None:
        if self.owner is None:
            self.owner = "external"
        c = self.cfg
        path = None
        if c.log_enabled and self.csv is not None:
            r = self.csv.start()
            path = r or getattr(self.csv, "path", None) or "기록 시작"
        self._csv_last_t = -1e9
        tgt = self.profiler.target()
        tgt_txt = f"{tgt:.1f}°C(램프)" if tgt is not None else self._fmt(st.get("sv"))
        ramp_txt = f"{self.profiler.rate():g}°C/min" if self.profiler.is_active() else "없음(래더 램프만)"
        self._log(f"운전 시작 ({OWNER_LABEL.get(self.owner, self.owner)}) — 목표 {tgt_txt} · 램프 {ramp_txt}"
                  f" · 래더 램프 {st.get('ramp_rate')}°C/min · 홀드백 {self._fmt(st.get('holdback'))}"
                  f" · SV상한 {self._fmt(st.get('sv_limit'))} · OT {self._fmt(st.get('ot_limit'))}"
                  f" · DAC상한 {st.get('mv_limit')} · Kp/Ti 적용 {self._num(st.get('kp_act'))}/{self._num(st.get('ti_act'))}"
                  f" · 유지 모드 {c.hold_mode} · CSV {path if path else '없음'}")
        self._episode_notes = []
        self._run_ctx = (self._proc_ctx if (self.owner == "process" and self._proc_ctx)
                         else OWNER_LABEL.get(self.owner, self.owner))
        fields = {"목표": self._fmt(self.final_target()), "램프": ramp_txt, "현재 PV": self._fmt(st.get("pv"))}
        if c.hold_mode == "dac":
            fields["유지 모드"] = "dac"
        self._card("시작", self._run_ctx, "INFO", fields)

    def _on_run_fall(self, st: dict, now: float) -> None:
        try:
            if self.csv is not None and self.csv.is_open():
                self.csv.write_row(st, owner=OWNER_LABEL.get(self.owner, self.owner or ""),
                                   hold=self.hold.log_tuple(), sat_clamp=self.sat.clamped, note="운전 종료")
            if self.csv is not None:
                self.csv.stop()
        except Exception:
            pass
        if self.profiler.is_active():
            self.profiler.stop(restore_rate=True)
        elif st.get("ramp_rate") is not None and float(st["ramp_rate"]) != self._ramp_c_per_min() and self.writes_allowed:
            try:
                self._write_ramp_counts(self.cfg.rate_counts(self.cfg.ramp_rate))
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
            reason = f"인터락 해제({self._itl_missing(st)}) — PLC 가 RUN 을 끔"
        else:
            reason = "외부 OFF 또는 순간 인터락 해제(이 프로그램이 끄지 않음)"
        if reason not in NORMAL_END_REASONS:
            self._event(reason)
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
