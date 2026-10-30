"""DC Pulse 출력 중 감시·아크 계수(챔버12) — v1 device/dc_pulse.py _poll_loop·_check_overcurrent·아크 함수를 다시 쓴 것.

DcPulseDriver 가 섞어 쓰는 mixin(I/O 없음 — 값만 받아 판정하고 이벤트를 낸다. 장비 I/O·OFF 는 드라이버가 한다).

판정 주기는 v1 그대로(1초 읽기와 따로): OUTPUT_ON 뒤 DCP_ARC_IGN_WINDOW_S(30 s) 점화 창 안에서는
  max(0.2, min(DCP_POLL_INTERVAL_S, DCP_ARC_FAST_INTERVAL_S)) = 1 s, 그 밖 DCP_POLL_INTERVAL_S = 5 s 마다 한 번(dc_pulse.py:1586-1602).
  1초 읽기 값은 매 tick 행에 들어가지만, 연속 N회 판정은 이 주기의 표본으로만 센다(1 s 마다 세면 AUTO-STOP 이 5배
  빨라진다 — v1 주석 dc_pulse.py:2721-2725 의 금지 사항).
한 주기(dc_pulse.py:2668-2985)
  ⓪ 과전류(DC_OVERCURRENT_*, 공통 키만 — v1 _OC_KEY_PREFIX): I > 한계(설정 W 기준) 를 STREAK_N 회 연속 → AUTO_STOP
     (점화 창에서도 판정). ① 저전류 I ≤ DCP_I_LOW_THRESH_A 를 DCP_I_LOW_COUNT_MAX_N 회 → AUTO_STOP "low_current: …"
  ② 세트포인트 |P − 설정| > max(TOL_W, 설정 × TOL_PCT) 를 DCP_P_SET_DEVIATE_MAX_N 회 → AUTO_STOP "target_failed"
     (①② 는 점화 창 안에서 멈춤 — 세지도 0 으로 되돌리지도 않음). ②-b 런당 1회 P 대 V×I 정합성(경고만). ③ telemetry 이벤트.
  아크: SAT(0x96)·ANT(0x99) 누적 − 기준값 = 런 누적, 직전 표본과의 증가분/경과로 계산 rate, 점화 창이 닫힌 뒤 첫 표본을
     점화 누적으로 확정, 경고(rate ≥ RATE_PER_S 를 RATE_N 회 연속 / 점화 ≥ IGN_WARN / 증착 ≥ DEPO_WARN)는 런당 1회.
  PIV 읽기 실패가 5회 연속이면 경고 1회, 회복하면 정보 1회.
"""
from __future__ import annotations

from typing import Optional

from v2.ch12.drivers import dcpulse_proto as P


class DcpMonitorMixin:
    # 드라이버가 채우는 것: self.clock, self._c(key, default), self.cfg, self._emit(kind, detail, **kw), 상태 필드

    def _mon_init(self) -> None:
        self._spdev_n = self._low_curr_n = self._overcurr_n = 0
        self._poll_nak_n = 0
        self._soft_arc_total = self._hard_arc_total = 0
        self._arc_alert_sent = False
        self._arc_baseline_soft = self._arc_baseline_hard = 0
        self._arc_rate_hit_n = 0
        self._arc_reset_ok, self._arc_reset_state = False, "unknown"
        self._arc_on_ref: tuple = (None, None)
        self._arc_run_start_ts = 0.0
        self._arc_ign_soft: Optional[int] = None
        self._arc_ign_hard: Optional[int] = None
        self._arc_final_soft: Optional[int] = None
        self._arc_final_hard: Optional[int] = None
        self._arc_prev_raw_s: Optional[int] = None
        self._arc_prev_raw_h: Optional[int] = None
        self._arc_prev_ts = 0.0
        self._arc_zero_reset_logged = False
        self._piv_scale_checked = False

    # ───────── 주기 ─────────
    def _in_arc_fast_window(self, now: Optional[float] = None) -> bool:
        t0 = float(self._arc_run_start_ts or 0.0)
        if t0 <= 0.0:
            return False
        win = max(0.0, float(self._c("DCP_ARC_IGN_WINDOW_S", 30.0)))
        if win <= 0.0:
            return False
        now = self.clock.monotonic() if now is None else now
        return (now - t0) < win

    def _effective_poll_period(self, now: Optional[float] = None) -> float:
        poll = max(0.1, float(self._c("DCP_POLL_INTERVAL_S", 5.0)))
        if self._in_arc_fast_window(now):
            return max(0.2, min(poll, float(self._c("DCP_ARC_FAST_INTERVAL_S", 1.0))))
        return poll

    # ───────── 과전류 ─────────
    def _oc(self, key: str, default):
        return self.cfg.get(key, default)                # DC_OVERCURRENT_* 는 공통 키만(v1 dc_pulse.py:404-411)

    def _oc_limit(self) -> float:
        ref_w = float(self._last_ref_power_w or 0.0)
        return P.overcurrent_limit_a(ref_w if ref_w > 0.0 else None, self._oc("DC_OVERCURRENT_P1_W", 300.0),
                                     self._oc("DC_OVERCURRENT_I1_A", 1.0), self._oc("DC_OVERCURRENT_P2_W", 600.0),
                                     self._oc("DC_OVERCURRENT_I2_A", 2.0), bool(self._oc("DC_OVERCURRENT_EXTRAPOLATE", True)))

    def _overcurrent_arm_status(self) -> str:
        if not self._oc("DC_OVERCURRENT_ENABLE", True):
            return ("[overcurrent] ⚠ 인터락 OFF (settings.json common.DC_OVERCURRENT_ENABLE=false) — 과전류로 정지하지 않음")
        ref_w = float(self._last_ref_power_w or 0.0)
        streak_n = max(1, int(self._oc("DC_OVERCURRENT_STREAK_N", 1)))
        armed = "ON" if self._oc("DC_OVERCURRENT_ARM_ON_START", True) else "OFF"
        return (f"[overcurrent] 인터락 ON — 한계 {self._oc_limit():.2f}A (설정 {ref_w:.0f}W, 연속 {streak_n}회, 점화 감시 {armed})")

    def _mon_overcurrent(self, p: float, v: float, i: float, raw: Optional[dict]) -> Optional[str]:
        """과전류면 AUTO_STOP 사유 문구(fault 덧붙이기 전), 아니면 None. 경고 문구는 여기서 낸다."""
        if not self._oc("DC_OVERCURRENT_ENABLE", True):
            return None
        try:
            i = float(i)
        except Exception:
            return None
        if i != i or i in (float("inf"), float("-inf")):
            return None
        limit = self._oc_limit()
        if i > limit:
            self._overcurr_n += 1
        else:
            self._overcurr_n = 0
            return None
        streak_n = max(1, int(self._oc("DC_OVERCURRENT_STREAK_N", 1)))
        if self._overcurr_n < streak_n:
            self._emit("status", f"[WARN] 과전류 감지: I={i:.3f} A > {limit:.3f} A ({self._overcurr_n}/{streak_n})")
            return None
        ref_w = float(self._last_ref_power_w or 0.0)
        raw_s = f" | raw P/I/V={raw.get('P')}/{raw.get('I')}/{raw.get('V')}" if raw else ""
        self._emit("status", f"[overcurrent] I={i:.3f}A > 한계 {limit:.3f}A | 설정={ref_w:.0f}W | P={p:.1f}W V={v:.1f}V{raw_s}")
        return f"overcurrent: I={i:.3f}A > {limit:.3f}A (설정 {ref_w:.0f}W 기준, {self._overcurr_n}회 연속)"

    # ───────── 한 주기 판정(과전류 뒤) ─────────
    def _mon_cycle(self, res: Optional[dict], now: float) -> Optional[tuple]:
        """PIV 결과 res({"raw","eng"} 또는 None)로 ⓪~③. AUTO_STOP 이면 (사유, 상태 문구, p, v, i, eng) — I/O 는 호출자."""
        if res is not None:
            if self._poll_nak_n >= 5:
                self._emit("status", "[INFO] 텔레메트리 읽기 회복 (NAK 연속 종료)")
            self._poll_nak_n = 0
        else:
            self._poll_nak_n += 1
            if self._poll_nak_n == 5:
                self._emit("status", "[WARN] 텔레메트리 연속 5회 NAK — DCP 시리얼 라인 노이즈 의심 (저전류/이탈 감시 공백 중)")
            return None
        eng = res["eng"]
        p, v, i = float(eng["P_W"]), float(eng["V_V"]), float(eng["I_A"])
        why = self._mon_overcurrent(p, v, i, res.get("raw"))
        if why:
            return (why, "[AUTO-STOP] 과전류 → OUTPUT_OFF & stop polling", p, v, i, eng)
        fast = self._in_arc_fast_window(now)
        ref = float(self._last_ref_power_w or 0.0)
        th, nmax = float(self._c("DCP_I_LOW_THRESH_A", 0.05)), max(1, int(self._c("DCP_I_LOW_COUNT_MAX_N", 3)))
        if fast:
            pass
        elif ref > 0.0:
            if i <= th:
                self._low_curr_n += 1
                self._emit("status", f"[WARN] 저전류 감지: I={i:.3f} A ({self._low_curr_n}/{nmax})")
                if self._low_curr_n >= nmax:
                    return (f"low_current: I <= {th:.3f}A ({self._low_curr_n}회 연속)",
                            "[AUTO-STOP] 저전류가 연속 발생 → OUTPUT_OFF & stop polling", p, v, i, eng)
            else:
                self._low_curr_n = 0
        else:
            self._low_curr_n = 0
        tol_w, tol_pct = float(self._c("DCP_P_SET_TOL_W", 15.0)), float(self._c("DCP_P_SET_TOL_PCT", 0.05))
        dmax = max(1, int(self._c("DCP_P_SET_DEVIATE_MAX_N", 3)))
        if fast:
            pass
        elif ref > 0.0:
            tol = max(tol_w, abs(ref) * tol_pct)
            if abs(p - ref) > tol:
                self._spdev_n += 1
                self._emit("status", f"[WARN] 현재 P={p:.1f} W, Set={ref:.1f} W, Tol=±{tol:.1f} W — 세트포인트 이탈 "
                                     f"({self._spdev_n}/{dmax})")
                if self._spdev_n >= dmax:
                    return ("target_failed", "[AUTO-STOP] 세트포인트 이탈이 연속 발생 → OUTPUT_OFF & stop polling",
                            p, v, i, eng)
            else:
                self._spdev_n = 0
        else:
            self._spdev_n = 0
        if not fast and not self._piv_scale_checked:
            c = P.piv_scale_check(p, i, v)
            if c is not None:
                self._piv_scale_checked = True
                ok, err = c
                rr = res.get("raw") or {}
                if ok:
                    self._emit("status", f"[piv] 스케일 정합 OK (P={p:.1f} W, VxI={v * i:.1f} W, 오차 {err:.1f}%)")
                else:
                    self._emit("status", f"[piv] 스케일 의심 — P={p:.1f} W vs VxI={v * i:.1f} W (오차 {err:.1f}%) | "
                                         f"raw P/I/V={rr.get('P')}/{rr.get('I')}/{rr.get('V')} | "
                                         f"상수 W/LSB={self._c('DCP_P_MEAS_W_PER_LSB', 10.0)} "
                                         f"A/LSB={self._c('DCP_I_MEAS_A_PER_LSB', 0.01)} V/LSB={self._c('DCP_V_MEAS_V_PER_LSB', 1.0)}")
        self._emit("telemetry", f"[telemetry] P={p:.1f} W, V={v:.2f} V, I={i:.3f} A", data=dict(eng, raw=res.get("raw")))
        return None

    # ───────── 아크 ─────────
    def _mon_arc(self, soft: Optional[int], hard: Optional[int], s_rate: Optional[int], h_rate: Optional[int],
                 now: float) -> None:
        if soft is None and hard is None:
            return
        raw_s = soft if soft is not None else self._soft_arc_total
        raw_h = hard if hard is not None else self._hard_arc_total
        b_s0, b_h0 = int(self._arc_baseline_soft), int(self._arc_baseline_hard)
        zr = False
        if soft is not None and soft < b_s0:
            zr, self._arc_baseline_soft = True, 0
        if hard is not None and hard < b_h0:
            zr, self._arc_baseline_hard = True, 0
        if zr and not self._arc_zero_reset_logged:
            self._arc_zero_reset_logged = True
            self._emit("status", f"[arc] 장비 카운터 감소 SAT {b_s0}→{soft if soft is not None else '-'}, "
                                 f"ANT {b_h0}→{hard if hard is not None else '-'} — 장비가 카운터를 다시 셈 → 기준값을 0 으로 변경")
        run_s = max(0, int(raw_s) - int(self._arc_baseline_soft))
        run_h = max(0, int(raw_h) - int(self._arc_baseline_hard))
        self._soft_arc_total, self._hard_arc_total = run_s, run_h
        calc_rate: Optional[float] = None
        if self._arc_prev_ts > 0.0 and now > self._arc_prev_ts:
            d = max(0, int(raw_s) - int(self._arc_prev_raw_s or 0)) + max(0, int(raw_h) - int(self._arc_prev_raw_h or 0))
            calc_rate = d / (now - self._arc_prev_ts)
        self._arc_prev_raw_s, self._arc_prev_raw_h, self._arc_prev_ts = int(raw_s), int(raw_h), now
        win_s = float(self._c("DCP_ARC_IGN_WINDOW_S", 30.0))
        if self._arc_ign_soft is None and self._arc_run_start_ts > 0.0 and not self._in_arc_fast_window(now):
            self._arc_ign_soft, self._arc_ign_hard = run_s, run_h
            self._emit("status", f"[arc] 점화 구간 종료(+{win_s:.0f}s): 점화 soft={run_s} hard={run_h} 합계={run_s + run_h}")
        sp = P.arc_split(run_s, run_h, self._arc_ign_soft, self._arc_ign_hard)
        rate_sum = int(s_rate or 0) + int(h_rate or 0)
        rv = (lambda x: str(x) if x is not None else "-")
        self._emit("status", f"[arc] 장비 SAT={rv(soft)} ANT={rv(hard)} AE={rv(s_rate)} AF={rv(h_rate)} | "
                             f"런 soft={run_s} hard={run_h} (기준 SAT={int(self._arc_baseline_soft)} "
                             f"ANT={int(self._arc_baseline_hard)}) | 점화={sp['ign_total']} 증착={sp['depo_total']} | "
                             f"계산 {f'{calc_rate:.1f}/s' if calc_rate is not None else '-'}"
                             f"{' | fast' if self._in_arc_fast_window(now) else ''}")
        rate_lim, rate_n = float(self._c("DCP_ARC_ALERT_RATE_PER_S", 20.0)), int(self._c("DCP_ARC_ALERT_RATE_N", 2))
        ign_lim, depo_lim = int(self._c("DCP_ARC_IGN_WARN", 3000)), int(self._c("DCP_ARC_DEPO_WARN", 500))
        run_lim = int(self._c("DCP_ARC_ALERT_RUN_TOTAL", 1000))
        rate_obs = float(calc_rate) if calc_rate is not None else 0.0
        self._arc_rate_hit_n = self._arc_rate_hit_n + 1 if (rate_lim > 0 and rate_obs >= rate_lim) else 0
        by_rate = rate_n > 0 and self._arc_rate_hit_n >= rate_n
        by_ign = ign_lim > 0 and sp["ign_closed"] and sp["ign_total"] >= ign_lim
        by_depo = depo_lim > 0 and sp["depo_total"] >= depo_lim
        if (by_rate or by_ign or by_depo) and not self._arc_alert_sent:
            self._arc_alert_sent = True
            elapsed = max(0.0, now - (self._arc_run_start_ts or now))
            self._emit("arc_threshold_reached",
                       f"Arc 경고 (런 누적 Soft={run_s}, Hard={run_h}, 합계={sp['run_total']} | 점화={sp['ign_total']}"
                       f" 증착={sp['depo_total']}, 최근 {rate_obs:.1f}/s)",
                       data={"run_soft": run_s, "run_hard": run_h, "rate": rate_sum, "rate_soft": s_rate, "rate_hard": h_rate,
                             "raw_soft": int(raw_s), "raw_hard": int(raw_h), "elapsed_s": round(elapsed, 1),
                             "by_rate": by_rate, "by_total": False, "by_ign": by_ign, "by_depo": by_depo,
                             "ign_soft": sp["ign_soft"], "ign_hard": sp["ign_hard"], "ign_total": sp["ign_total"],
                             "depo_soft": sp["depo_soft"], "depo_hard": sp["depo_hard"], "depo_total": sp["depo_total"],
                             "calc_rate": round(calc_rate, 2) if calc_rate is not None else None, "rate_limit": rate_lim,
                             "run_total_limit": run_lim, "ign_limit": ign_lim, "depo_limit": depo_lim})

    def _arc_after_on(self, s_now: Optional[int], h_now: Optional[int]) -> None:
        """OUTPUT_ON 직후 SAT/ANT 1회 기록 + 런 기준값 결정(v1 _log_arc_after_on·_decide_arc_baseline_after_on)."""
        a, b = (tuple(self._arc_on_ref) + (None, None))[:2]
        f = (lambda x: str(int(x)) if x is not None else "-")
        self._emit("status", f"[arc] ON 직후 장비 SAT={f(s_now)} ANT={f(h_now)} (ON 전 기준 SAT={f(a)} ANT={f(b)})")
        base_s, base_h = int(self._arc_baseline_soft or 0), int(self._arc_baseline_hard or 0)
        res = {}
        for key, now, base in (("s", s_now, base_s), ("h", h_now, base_h)):
            res[key] = "fail" if now is None else ("reset" if (base == 0 or int(now) < base) else "kept")
        if res["s"] == "reset":
            self._arc_baseline_soft = 0
        if res["h"] == "reset":
            self._arc_baseline_hard = 0
        if "kept" in res.values():
            self._emit("status", f"[arc] ⚠ ON 직후 카운터가 초기화되지 않음 SAT {base_s}→{f(s_now)}, ANT {base_h}→{f(h_now)} "
                                 f"— 기준값 유지(기존 방식)")
            how = "초기화 안 됨"
        elif "fail" in res.values():
            how = "확인 실패 → 기존 방식"
        else:
            how = "ON 직후 초기화 확인"
        self._emit("status", f"[arc] 런 기준값: SAT={int(self._arc_baseline_soft)} ANT={int(self._arc_baseline_hard)} ({how})")

    def _arc_final(self, fs: Optional[int], fh: Optional[int]) -> None:
        """OUTPUT_OFF 직전 종료값 확정(v1 _capture_arc_final)."""
        if fs is None and fh is None:
            return
        raw_s = int(fs) if fs is not None else int(self._soft_arc_total) + int(self._arc_baseline_soft)
        raw_h = int(fh) if fh is not None else int(self._hard_arc_total) + int(self._arc_baseline_hard)
        run_s = max(max(0, raw_s - int(self._arc_baseline_soft)), int(self._soft_arc_total))
        run_h = max(max(0, raw_h - int(self._arc_baseline_hard)), int(self._hard_arc_total))
        self._arc_final_soft = self._soft_arc_total = run_s
        self._arc_final_hard = self._hard_arc_total = run_h
        d = P.arc_split(run_s, run_h, self._arc_ign_soft, self._arc_ign_hard)
        self._emit("status", f"[arc] 종료 확정: run soft={run_s} hard={run_h} 합계={d['run_total']} | 점화={d['ign_total']} "
                             f"증착={d['depo_total']} | raw SAT={raw_s} ANT={raw_h} (baseline {int(self._arc_baseline_soft)}/"
                             f"{int(self._arc_baseline_hard)}) | 0x8C 동작={P.ARC_RESET_LABEL.get(self._arc_reset_state, '확인불가')}")

    @property
    def arc_counts(self) -> tuple:
        if self._arc_final_soft is not None and self._arc_final_hard is not None:
            return int(self._arc_final_soft), int(self._arc_final_hard)
        return self._soft_arc_total, self._hard_arc_total

    @property
    def arc_summary(self) -> dict:
        s_, h_ = self.arc_counts
        d = P.arc_split(s_, h_, self._arc_ign_soft, self._arc_ign_hard)
        d.update(reset_ok=bool(self._arc_reset_ok), reset_state=str(self._arc_reset_state or "unknown"),
                 final_read=self._arc_final_soft is not None and self._arc_final_hard is not None,
                 baseline_soft=int(self._arc_baseline_soft), baseline_hard=int(self._arc_baseline_hard),
                 ign_window_s=float(self._c("DCP_ARC_IGN_WINDOW_S", 30.0)), ran=float(self._arc_run_start_ts or 0.0) > 0.0)
        return d

    def reset_arc_counts(self) -> None:
        """공정 시작 시 — 아크 누적·기준선 초기화(v1 reset_arc_counts)."""
        keep = (self._spdev_n, self._low_curr_n, self._overcurr_n, self._poll_nak_n, self._piv_scale_checked)
        self._mon_init()
        self._spdev_n, self._low_curr_n, self._overcurr_n, self._poll_nak_n, self._piv_scale_checked = keep
