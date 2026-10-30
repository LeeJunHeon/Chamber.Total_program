"""CH2 히터 목표 도달 후 유지 모드 — DAC 상한 고정("dac") 전용. v1 controller/heater_hold.py(HeaterHold) 그대로 옮김.

CH2 는 TC 가 1개라 TC2 추종(tc2)·강등 경로는 없다(v1 과 같음).
도달 판정: 램프 종료 → |PV−final| ≤ ARRIVE_TOL 에 한 번 들어와야 잰다(도달 래치) → |PV−SV| ≤ ENTER_TOL 이 ENTER_SEC 연속·
  표본 4개 이상 → 창 전·후반 평균 드리프트(PV/MV) 안이면 캡처. 평균 MV ≥ 0.98×상한이면 고정하지 않고 give_up,
  ≤ MV_MIN+20 이면 고정하지 않는다(바닥). 통과하면 D118 ← 창 평균 MV([MV_MIN+20, MV_LIMIT]) → holding.
해제: RUN OFF / FAULT / 목표 변경 → D118 = MV_LIMIT 원복. holding 중 D118 이 되돌아가 있으면 5초 뒤 재적용.
PLC 를 직접 쓰지 않는다 — request_mv_limit(int) 콜백으로 요청만 한다(엔진이 같은 폴링 안에서 순서대로 쓴다).
engaged_relaxed(강제 진입이면 True)는 v1 처럼 대입만 하고 읽는 곳이 없다(HEAT-123).
"""
from __future__ import annotations

import time
from typing import Callable

# 진입 게이트의 유효 허용치 — config 값은 하한, 아래 비율이 실제 판정 폭(챔버K 2026-09-21/22 실측 근거)
DRIFT_PV_REL = 0.5    # ENTER_TOL 의 절반이면 정착으로 본다
DRIFT_MV_REL = 0.05   # 상한의 5%
MV_SAT_REL = 0.98     # 창 평균 MV 가 상한의 98% 이상이면 유지할 여유가 없다(give_up)
REAPPLY_SEC = 5.0     # 유지 중 값이 되돌아가 있으면 이만큼 지난 뒤 재적용


def _noop(*_a, **_k):
    return None


class HeaterHold:
    def __init__(self, mode: str, *, mv_limit: int, mv_min: int,
                 enter_tol_c: float, enter_sec: float, arrive_tol_c: float,
                 drift_pv_c: float, drift_mv: float,
                 clock: Callable[[], float] = time.monotonic,
                 request_mv_limit: Callable[[int], None] = _noop,
                 message: Callable[[str, str], None] = _noop,
                 engaged: Callable[[str], None] = _noop,
                 give_up: Callable[[str], None] = _noop):
        self.mode = str(mode)
        self.mv_limit = int(mv_limit)
        self.mv_min = int(mv_min)
        self.enter_tol = float(enter_tol_c)
        self.enter_sec = float(enter_sec)
        self.arrive_tol = float(arrive_tol_c)
        self.drift_pv = float(drift_pv_c)
        self.drift_mv = float(drift_mv)
        self._now = clock
        self._req = request_mv_limit
        self._message = message
        self._engaged = engaged
        self._give_up = give_up
        self._h = self._fresh()
        self.armable = True          # False 면 도달·진입 판정을 하지 않는다(공정이 작업 압력·플라즈마 조건에서 잡기 전까지 막아 둠)
        self.engaged_relaxed = False
        self.last_force_reason = ""
        self.last_force_source = ""

    # ───────── 상태 ─────────
    @staticmethod
    def _fresh() -> dict:
        return {'state': 'idle', 'samples': [], 't0': 0.0, 'sv': None, 'value': None,
                'last_push': 0.0, 'arrived': False, 'arr_sv': None, 'floor_warned': False,
                'limit_warned': False, 'drift_log_t': 0.0, 'last_window_mv': None,
                'last_sv': None, 'gave_up': None}

    @property
    def state(self) -> str:
        return self._h['state']

    @property
    def value(self):
        return self._h['value']

    @property
    def arrived(self) -> bool:
        return bool(self._h['arrived'])

    @property
    def gave_up(self):
        return self._h['gave_up']

    @property
    def last_window_mv(self):
        """마지막으로 잰 창의 평균 MV(드리프트로 탈락한 창 포함) — 2단계 강제 진입용."""
        return self._h['last_window_mv']

    def is_holding(self) -> bool:
        return self._h['state'] == 'holding'

    def set_armable(self, on: bool) -> None:
        """유지 진입 허용/금지(공정 전용). holding 이면 호출부가 release() 로 먼저 푼다.
        바꿀 때마다 측정 상태와 '마지막 측정 창 평균'을 버린다 — 이후 측정·강제 진입은 새 조건의 값만 쓴다."""
        self.armable = bool(on)
        if self._h['state'] != 'holding':
            self._reset(keep_arrived=False)
            self._h['last_window_mv'] = None

    def effective_gates(self):
        """(eff_pv, eff_mv) — config 하한과 비율 허용치 중 큰 쪽."""
        return (max(self.drift_pv, DRIFT_PV_REL * self.enter_tol),
                max(self.drift_mv, DRIFT_MV_REL * self.mv_limit))

    def log_tuple(self):
        """CSV (hold, hold_mv)."""
        h = self._h
        return (1 if h['state'] == 'holding' else 0, h['value'] if h['state'] == 'holding' else None)

    def _msg(self, level: str, text: str) -> None:
        try:
            self._message(level, text)
        except Exception:
            pass

    def _request(self, v: int) -> None:
        try:
            self._req(int(v))
        except Exception:
            pass

    def _reset(self, keep_arrived: bool) -> None:
        h = self._h
        arrived, arr_sv = h['arrived'], h['arr_sv']
        limit_w, floor_w, drift_t = h['limit_warned'], h['floor_warned'], h['drift_log_t']
        lw, lsv = h['last_window_mv'], h['last_sv']
        self._h = self._fresh()
        self._h.update(last_window_mv=lw, last_sv=lsv)
        if keep_arrived:
            self._h.update(arrived=arrived, arr_sv=arr_sv, limit_warned=limit_w,
                           floor_warned=floor_w, drift_log_t=drift_t)

    # ───────── 2단계용 강제 진입 ─────────
    def force_dac_hold(self, mv_value=None, prefer_current: bool = False) -> bool:
        """dac 유지 강제 진입. prefer_current=False 면 마지막 창 평균 → 없으면 인자(현재 MV)."""
        h = self._h
        if h['state'] == 'holding':
            return True
        if self.mode == 'off':
            self.last_force_reason = "유지 모드 꺼짐(HEATER_HOLD_MODE=off)"
            return False
        cur = None
        if mv_value is not None:
            try:
                cur = float(mv_value)
            except Exception:
                cur = None
        if prefer_current:
            src, self.last_force_source = ((cur, "현재 MV") if cur is not None
                                           else (h['last_window_mv'], "마지막 측정 창 평균"))
        else:
            src, self.last_force_source = ((h['last_window_mv'], "마지막 측정 창 평균")
                                           if h['last_window_mv'] is not None else (cur, "현재 MV"))
        if src is None:
            self.last_force_reason = "측정 창 평균도 현재 MV 도 없음"
            return False
        value = max(self.mv_min + 20, min(self.mv_limit, int(round(src))))
        h.update(state='holding', sv=h['last_sv'], value=value, last_push=self._now(),
                 samples=[], t0=0.0, gave_up=None)
        self.engaged_relaxed = True
        self._request(value)
        self._msg("경고", f"유지 모드 강제 진입 — DAC 상한 {value} 고정 ({self.last_force_source})")
        try:
            self._engaged('dac')
        except Exception:
            pass
        return True

    # ───────── 해제 ─────────
    def release(self, why: str) -> None:
        h = self._h
        if h['state'] == 'holding':
            self._request(self.mv_limit)
            self._msg("정보", f"DAC 상한 고정 해제 → {self.mv_limit} 원복 ({why})")
        elif h['samples']:
            self._msg("정보", f"유지 모드 대기 취소 ({why})")
        self._reset(keep_arrived=False)

    # ───────── tick ─────────
    def tick(self, st: dict, final_target=None) -> None:
        try:
            self._tick(st, final_target)
        except Exception as e:
            self._msg("경고", f"유지 모드 처리 예외: {e!r}")

    def _tick(self, st: dict, final_target) -> None:
        h = self._h
        now = self._now()
        if self.mode == 'off':
            if h['state'] == 'holding':
                self.release("기능 꺼짐")
            elif h['state'] != 'idle' or h['samples'] or h['arrived']:
                self._reset(keep_arrived=False)
            return
        run = bool(st.get('run'))
        fault = bool(st.get('fault'))
        sv = st.get('sv'); pv = st.get('pv'); svr = st.get('sv_ramp'); mv = st.get('mv')
        if sv is not None:
            h['last_sv'] = float(sv)
        if not run:
            if h['state'] != 'idle' or h['samples']:
                self.release("운전 OFF")
            h['last_window_mv'] = None
            return
        if fault:
            if h['state'] != 'idle' or h['samples']:
                self.release("히터 이상")
            return
        try:
            final = float(final_target) if final_target is not None else (float(sv) if sv is not None else None)
        except Exception:
            final = float(sv) if sv is not None else None

        if h['state'] == 'holding':
            self._tick_holding(st, now, sv)
            return
        if not self.armable:
            if h['samples'] or h['state'] != 'idle' or h['arrived']:
                self._reset(keep_arrived=False)
            return

        base_ok = (bool(st.get('ok')) and pv is not None and sv is not None and svr is not None
                   and mv is not None and float(sv) > 0)
        ramp_done = (base_ok and final is not None
                     and abs(float(svr) - float(sv)) <= 0.05 and abs(float(sv) - final) <= 0.05)
        if (base_ok and final is not None and h['arrived'] and h['arr_sv'] is not None
                and abs(final - float(h['arr_sv'])) > 0.05):
            h.update(arrived=False, arr_sv=None, samples=[], t0=0.0, state='idle')
        if ramp_done and not h['arrived'] and abs(float(pv) - final) <= self.arrive_tol:
            h['arrived'] = True
            h['arr_sv'] = final
            self._msg("정보", f"목표 도달 확인 — 유지 모드 측정 시작 (PV {float(pv):.1f} / SV {final:.1f})")

        ok = ramp_done and bool(h['arrived']) and abs(float(pv) - float(sv)) <= self.enter_tol
        if not ok:
            if h['samples']:
                h['samples'] = []; h['t0'] = 0.0; h['state'] = 'idle'
            h['limit_warned'] = False; h['floor_warned'] = False
            return
        if not h['samples']:
            h['t0'] = now; h['state'] = 'arming'; h['gave_up'] = None
        h['samples'].append((float(mv), float(pv)))
        if (now - h['t0']) < self.enter_sec:
            return
        n = len(h['samples'])
        if n < 4:
            return
        half = n // 2
        first, second = h['samples'][:half], h['samples'][half:]
        d_mv = sum(s[0] for s in second) / len(second) - sum(s[0] for s in first) / len(first)
        d_pv = sum(s[1] for s in second) / len(second) - sum(s[1] for s in first) / len(first)
        eff_pv, eff_mv = self.effective_gates()
        h['last_window_mv'] = sum(s[0] for s in h['samples']) / n
        if abs(d_pv) > eff_pv or abs(d_mv) > eff_mv:
            h['samples'] = []; h['t0'] = now            # 처음부터 다시 잰다(arrived 유지)
            if (now - h['drift_log_t']) >= 30.0:
                h['drift_log_t'] = now
                self._msg("정보", f"목표 근처지만 아직 정착 전 — 재측정 (PV 드리프트 {d_pv:+.2f}°C(허용 {eff_pv:.1f}) / "
                                  f"MV 드리프트 {d_mv:+.1f}(허용 {eff_mv:.0f}))")
            return
        value = int(round(sum(s[0] for s in h['samples']) / n))
        if value >= MV_SAT_REL * float(self.mv_limit):
            why = (f"도달 시점 출력이 상한에 붙어 있어 고정하지 않음 (평균 MV {value} ≥ {self.mv_limit}×{MV_SAT_REL:g}) "
                   f"— 히터가 목표를 유지할 여유가 없습니다")
            if not h['limit_warned']:
                h['limit_warned'] = True
                self._msg("경고", why)
                h['gave_up'] = why
                try:
                    self._give_up(why)
                except Exception:
                    pass
            h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)
            return
        if value <= self.mv_min + 20:
            if not h['floor_warned']:
                h['floor_warned'] = True
                self._msg("경고", f"도달 시점 출력이 최소치라 고정하지 않음 (평균 MV {value} ≤ {self.mv_min + 20}) "
                                  f"— PID 가 출력을 내지 않는 상태입니다")
            h.update(state='idle', samples=[], t0=0.0, sv=None, value=None, last_push=0.0)
            return
        h['floor_warned'] = False
        h['limit_warned'] = False
        self._capture(h, now, value, pv, sv)

    def _capture(self, h: dict, now: float, value: int, pv, sv) -> None:
        raw = value
        value = max(self.mv_min + 20, min(self.mv_limit, value))
        if value != raw:
            self._msg("정보", f"DAC 상한 고정값 클램프 {raw} → {value}")
        h.update(state='holding', sv=float(sv), value=value, last_push=now, samples=[], t0=0.0)
        self.engaged_relaxed = False
        self._request(value)
        self._msg("정보", f"유지 모드 — DAC 상한 {value} 고정 (PV {float(pv):.1f} / SV {float(sv):.1f})")
        try:
            self._engaged('dac')
        except Exception:
            pass

    def _tick_holding(self, st: dict, now: float, sv) -> None:
        h = self._h
        if sv is None or h['sv'] is None or abs(float(sv) - float(h['sv'])) > 0.05:
            self.release(f"목표 변경 {h['sv']} → {sv}")
            return
        try:
            cur = int(st.get('mv_limit')) if st.get('mv_limit') is not None else None
        except Exception:
            cur = None
        if cur is not None and cur != int(h['value']) and (now - h['last_push']) >= REAPPLY_SEC:
            h['last_push'] = now
            self._request(int(h['value']))
            self._msg("정보", f"DAC 상한 재적용 {h['value']} (읽힌 D118={cur})")
