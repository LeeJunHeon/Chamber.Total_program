"""공정 1회 요약 값 모으기 — v1 controller/data_logger.py 의 수집 버퍼와 같은 규칙(값만 받는다).

Calib CSV(calib_csv.build_row)와 엑셀(excel_log.build_rows)이 같은 RunStats 를 읽는다(v1 도 DataLogger 하나를 둘이 읽었다).
부르는 쪽: 엔진이 1초 스냅샷 행·공정 이벤트에서 add_* 를 부른다(v1 은 장치 신호마다 불렀다 — 표본 수는 주기만큼 다를 수
있다. 수집 구간·형식·최솟값/평균 규칙은 같다).

규칙(LOG-032~040)
- start(params): params 사본 보관, 시작 시각 = t0_wall → started_at → now(datetime·ISO 문자열·epoch 초). 버퍼 비움.
  use_ms=False 면 처음부터 수집, True 면 Main Shutter 열릴 때(shutter_open)부터.
- shutter_open(): IG 를 뺀 모든 평균 버퍼(DC·DC2·DCP·RF·RFP·MFC flow·pressure·히터)를 비우고 수집 시작, shutter_close(): 중단.
- IG 는 셔터와 무관하게 세션 전체(숫자 또는 문자열의 첫 숫자). MFC 압력도 문자열('V+100.00' 'P=..')에서 첫 숫자.
- MFC 유량 가스명 AR/O2/N2(대소문자 무시) 만.
- 히터: pv 는 TC 정상일 때만(부르는 쪽이 None 으로), sv·mv 는 히터 RUN 일 때만.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional

_NUM = re.compile(r"([-+]?\d+(?:\.\d+)?)")


class RunStats:
    def __init__(self, now: Callable[[], datetime] = datetime.now) -> None:
        self.now = now
        self.params: Dict[str, Any] = {}
        self.session_started_at: Optional[datetime] = None
        self.ig: List[float] = []
        self.dc_p: List[float] = []
        self.dc_v: List[float] = []
        self.dc_i: List[float] = []
        self.dc2_p: List[float] = []
        self.dc2_v: List[float] = []
        self.dc2_i: List[float] = []
        self.dcp_p: List[float] = []
        self.dcp_v: List[float] = []
        self.dcp_i: List[float] = []
        self.rf_f: List[float] = []
        self.rf_r: List[float] = []
        self.rfp_f: List[float] = []
        self.rfp_r: List[float] = []
        self.flow: Dict[str, List[float]] = {"Ar": [], "O2": [], "N2": []}
        self.pressure: List[float] = []
        self.heater_pv: List[float] = []
        self.heater_mv: List[float] = []
        self.heater_sv: Optional[float] = None
        self.thickness_record: Dict[str, Any] = {}
        self.collecting = False

    # ── 세션 ──
    def start(self, params: Mapping[str, Any]) -> None:
        self.params = dict(params)
        sa = self.params.get("t0_wall") or self.params.get("started_at")
        if isinstance(sa, datetime):
            self.session_started_at = sa
        elif isinstance(sa, str):
            try:
                self.session_started_at = datetime.fromisoformat(sa)
            except Exception:
                self.session_started_at = self.now()
        elif isinstance(sa, (int, float)):
            self.session_started_at = datetime.fromtimestamp(float(sa))
        else:
            self.session_started_at = self.now()
        self.ig.clear()
        self._clear_avg()
        self.thickness_record = {}
        self.collecting = not params.get("use_ms", False)

    def _clear_avg(self) -> None:
        for lst in (self.dc_p, self.dc_v, self.dc_i, self.dc2_p, self.dc2_v, self.dc2_i, self.dcp_p, self.dcp_v,
                    self.dcp_i, self.rf_f, self.rf_r, self.rfp_f, self.rfp_r, self.pressure):
            lst.clear()
        for g in self.flow:
            self.flow[g].clear()
        self.heater_pv.clear()
        self.heater_mv.clear()
        self.heater_sv = None

    def shutter_open(self) -> None:
        self._clear_avg()
        self.collecting = True

    def shutter_close(self) -> None:
        self.collecting = False

    def end_update(self, *, process_name: str = "", result: str = "", stopped: bool = False,
                   actual_shutter_delay_min: Optional[float] = None, actual_process_time_min: Optional[float] = None,
                   soft_arc_count: Optional[int] = None, hard_arc_count: Optional[int] = None) -> None:
        """공정 종료 때 런타임이 덮어쓰던 값(chamber_runtime.py:1841-1863, 1882-1886)."""
        pp = self.params
        pname = str(process_name or "").strip()
        if pname:
            pp["process_name"] = pname
            pp["Process_name"] = pname
            pp["process_note"] = pname
        pp["result"] = result
        pp["stopped"] = bool(stopped)
        if actual_shutter_delay_min is not None:
            pp["shutter_delay"] = float(actual_shutter_delay_min)
        if actual_process_time_min is not None:
            pp["process_time"] = float(actual_process_time_min)
        if soft_arc_count is not None:
            pp["soft_arc_count"] = soft_arc_count
        if hard_arc_count is not None:
            pp["hard_arc_count"] = hard_arc_count

    # ── 표본 ──
    @staticmethod
    def _num(v: Any) -> Optional[float]:
        if isinstance(v, (int, float)):
            return float(v)
        m = _NUM.search(str(v))
        return float(m.group(1)) if m else None

    def add_ig(self, pressure: Any) -> None:
        try:
            v = self._num(pressure)
            if v is not None:
                self.ig.append(v)
        except Exception:
            pass

    def _pvi(self, lp, lv, li, p, v, i) -> None:
        if not self.collecting:
            return
        lp.append(float(p))
        lv.append(float(v))
        li.append(float(i))

    def add_dc(self, power: float, voltage: float, current: float) -> None:
        self._pvi(self.dc_p, self.dc_v, self.dc_i, power, voltage, current)

    def add_dc2(self, power: float, voltage: float, current: float) -> None:
        self._pvi(self.dc2_p, self.dc2_v, self.dc2_i, power, voltage, current)

    def add_dcpulse(self, power: float, voltage: float, current: float) -> None:
        self._pvi(self.dcp_p, self.dcp_v, self.dcp_i, power, voltage, current)

    def add_rf(self, for_p: float, ref_p: float) -> None:
        if self.collecting:
            self.rf_f.append(float(for_p))
            self.rf_r.append(float(ref_p))

    def add_rfpulse(self, for_p: float, ref_p: float) -> None:
        if self.collecting:
            self.rfp_f.append(float(for_p))
            self.rfp_r.append(float(ref_p))

    def add_mfc_flow(self, gas_name: str, flow: float) -> None:
        if not self.collecting:
            return
        key = {"AR": "Ar", "O2": "O2", "N2": "N2"}.get(str(gas_name).strip().upper())
        if key:
            self.flow[key].append(float(flow))

    def add_mfc_pressure(self, pressure: Any) -> None:
        if not self.collecting:
            return
        try:
            v = self._num(pressure)
            if v is not None:
                self.pressure.append(v)
        except Exception:
            pass

    def add_heater(self, pv: Optional[float], sv: Optional[float], mv: Optional[float], running: bool) -> None:
        if not self.collecting:
            return
        try:
            if pv is not None:
                self.heater_pv.append(float(pv))
            if running:
                if mv is not None:
                    self.heater_mv.append(float(mv))
                if sv is not None:
                    self.heater_sv = float(sv)
        except (TypeError, ValueError):
            pass

    def set_thickness(self, rec: Optional[Mapping[str, Any]]) -> None:
        self.thickness_record = dict(rec or {})

    # ── 요약 ──
    def heater_stats(self) -> Optional[Dict[str, Optional[float]]]:
        pv, mv = self.heater_pv, self.heater_mv
        if not pv and not mv:
            return None
        return {
            "sv": self.heater_sv,
            "pv_avg": (sum(pv) / len(pv)) if pv else None,
            "pv_min": min(pv) if pv else None,
            "pv_max": max(pv) if pv else None,
            "mv_avg": (sum(mv) / len(mv)) if mv else None,
            "n": float(len(pv)),
        }

    def heater_summary_text(self) -> str:
        """공정 txt 히터 요약 한 줄(data_logger.py:455-466). 표본이 없으면 빈 문자열."""
        s = self.heater_stats()
        if not s:
            return ""
        parts = [f"목표 {s['sv']:.1f}°C" if s["sv"] is not None else "히터 꺼짐"]
        if s["pv_avg"] is not None:
            parts.append(f"PV 평균 {s['pv_avg']:.1f}°C (최소 {s['pv_min']:.1f} · 최대 {s['pv_max']:.1f})")
        if s["mv_avg"] is not None:
            parts.append(f"MV 평균 {s['mv_avg']:.0f}")
        parts.append(f"표본 {int(s['n'] or 0)}개")
        return "히터 요약(메인 공정 구간) — " + " · ".join(parts)

    def apply_events(self, events) -> None:
        """시험·재생용: [("start", params), ("ig", v), ("dc", p, v, i), …] 를 차례로 적용."""
        fn = {"start": self.start, "ig": self.add_ig, "shutter_open": self.shutter_open,
              "shutter_close": self.shutter_close, "dc": self.add_dc, "dc2": self.add_dc2,
              "dcpulse": self.add_dcpulse, "rf": self.add_rf, "rfpulse": self.add_rfpulse,
              "flow": self.add_mfc_flow, "pressure": self.add_mfc_pressure, "heater": self.add_heater,
              "thickness": self.set_thickness, "end": lambda d: self.end_update(**d)}
        for ev in events:
            fn[ev[0]](*ev[1:])
