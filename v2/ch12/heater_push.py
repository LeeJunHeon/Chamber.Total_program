"""CH2 히터 엔진 — 세션 마커·설정 push·되읽기 확인. v1 controller/heater_service.py:887-1045 그대로(HeaterEngine 이 섞어 씀).

push 순서: D113 → D126 → D118 → D120 → D121 → D127 → D128 → Kp(D143 상위 먼저 → D142) → Ti(D145 → D144), Kp/Ti 는 범위 밖이면
생략·적용값이 이미 같으면(상대 1e-5) 다시 쓰지 않음. 첫 실패에서 멈추고 30 s 백오프(로그 60 s 에 1줄). 성공하면 세션 마커
(0x1000~0xFFFE 난수 → D160, 직접 쓰기). 폴링마다 마커를 비교해 2회 연속 불일치면 PLC 재기동으로 보고 주체를 지우고 즉시 다시 push.
"""
from __future__ import annotations

from typing import Dict

from v2.ch12.drivers import heater as H
from v2.ch12.drivers.heater import float_to_words

MARKER_MISMATCH_N = 2
PUSH_RETRY_SEC = 30.0
PUSH_FAIL_LOG_INTERVAL_S = 60.0


class HeaterPush:
    def _marker_and_push(self, st: dict) -> None:
        mreg = int(self.cfg.session_mark_reg)
        if self._verify is not None:
            self._check_verify(st)
        if not self.writes_allowed:
            return                           # bench — 설정·마커를 쓰지 않는다(ON 도 거부된 채)
        if self._need_push or (mreg and self._marker is None):
            if self._now() < self._push_retry_at:
                return
            self._push_and_mark(st)
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
                self.profiler.stop(restore_rate=False)
            if self._run_prev:
                self._end_reason = "PLC 재기동"
            self.owner = None
            self._need_push = True
            self._push_retry_at = 0.0
            self._push_and_mark(st)

    def _push_and_mark(self, st: dict) -> None:
        c = self.cfg
        if c.push_config and not self._push_config(st):
            return
        mreg = int(c.session_mark_reg)
        if mreg:
            m = self.rng.randint(0x1000, 0xFFFE)
            try:
                self.drv.w_marker(self._s, mreg, m)
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
        now = self._now()
        self._push_fail_n += 1
        self._push_retry_at = now + PUSH_RETRY_SEC
        if (now - self._push_fail_log_t) >= PUSH_FAIL_LOG_INTERVAL_S:
            self._push_fail_log_t = now
            self._log(f"⚠ 히터 설정 쓰기 실패({self._push_fail_n}회째) — {PUSH_RETRY_SEC:.0f}초 뒤 재시도: {what}")

    def _push_config(self, st: dict) -> bool:
        """D113 → D126 → D118 → D120 → D121 → D127 → D128 → Kp(D143→D142) → Ti(D145→D144). 2워드는 상위 먼저."""
        c = self.cfg
        items = [(H.REG_SV_LIMIT, c.c_to_raw(c.sv_limit_c), "SV상한"), (H.REG_OT_LIMIT, c.c_to_raw(c.ot_limit_c), "OT"),
                 (H.REG_MV_LIMIT, int(c.mv_limit), "DAC상한"), (H.REG_RAMP_RATE, c.rate_counts(c.ramp_rate), "램프"),
                 (H.REG_HOLDBACK, c.c_to_raw(c.holdback_c), "홀드백"), (H.REG_SLOW_ZONE, c.c_to_raw(c.slow_zone_c), "감속구간"),
                 (H.REG_SLOW_RATE, c.rate_counts(c.slow_rate), "감속램프")]
        expect: Dict[int, int] = {}
        for addr, raw, name in items:
            try:
                self._w_reg(addr, raw)
                expect[addr] = raw
            except Exception as e:
                self._push_failed(f"{name}(D{addr}): {e!r}")
                return False
        pid = {}
        for valid, val, lo, hi, act_key, name in ((c.kp_valid, c.kp, H.REG_KP_LO, H.REG_KP_HI, "kp_act", "Kp"),
                                                   (c.ti_valid, c.ti_s, H.REG_TI_LO, H.REG_TI_HI, "ti_act", "Ti")):
            if not valid:
                continue
            pid[act_key] = float(val)
            act = st.get(act_key)
            if act is not None and abs(float(act) - float(val)) <= max(1e-6, abs(float(val)) * 1e-5):
                continue
            w_lo, w_hi = float_to_words(val)
            try:
                self._w_reg(hi, w_hi)
                self._w_reg(lo, w_lo)
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
                got = int(regs[addr - H.REG_BLOCK_START])
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
