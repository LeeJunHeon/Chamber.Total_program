"""챔버12 두께 — 이번 런 기록(종료 방식·최종 유효두께·평균 증착률·적용값·큰 튐·대체 사유)과 패널 진행 영역 값.

v1 근거(읽고 다시 씀): runtime/chamber_runtime.py:6744-6764(_thk_run_started), 6787-6804(_thk_record — Time 모드 메인 공정 창),
6845-6879(_thk_on_end), 6881-6901(_thk_values — 메인 공정 전 종료 = '중단'), 6903-6923(_thk_finalize_record — 두께 요약 로그),
6932-6943(_thk_card_fields), 6599-6610(패널 진행 영역 cb(mode, target, pct, text)).
ThicknessEngine(thickness.py)이 잠금 안에서 부른다 — 이 모듈은 스레드를 모른다.
값은 Calib CSV·엑셀 두께 열(outputs)과 종료 카드(hub)가 같은 것을 쓴다(values()).
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.ch12.thickness_judge import END_ABORT, END_FALLBACK, END_LIMIT, END_THICKNESS, END_TIME

IDLE_VIEW = {"mode": "", "target": "", "pct": None, "text": "공정 없음"}   # ui/thickness_panel.py:276-303


class RunRecord:
    def __init__(self, log: Callable[[str, str], None]) -> None:
        self._log = log
        self.rec: Optional[Dict[str, Any]] = None      # None = 두께 모니터 없음·공정 시작 전(v1 _thk_rec 없음)
        self.warnings: List[str] = []                  # 이번 런 경고 목록(영점·수명·대체 → 종료 카드 warnings)
        self.view: Dict[str, Any] = dict(IDLE_VIEW)    # 패널 진행 영역 — 공정이 끝나도 다음 공정 전까지 유지

    def set_view(self, mode: str, target: str, pct, text: str) -> None:
        self.view = {"mode": mode, "target": target, "pct": pct, "text": text}

    # ── 공정 시작(_thk_run_started) ──
    def start(self, params: Mapping[str, Any], pre: Optional[Mapping[str, Any]], supported: bool) -> None:
        if not supported:
            self.rec = {}
            return
        thk_mode = bool((params or {}).get("thickness_mode"))
        rec = {"end": END_THICKNESS if thk_mode else END_TIME, "mode": "Thickness" if thk_mode else "Time",
               "target": (params or {}).get("thickness") if thk_mode else None,
               "first": None, "last": None, "seq": None}
        if thk_mode and isinstance(pre, Mapping):
            rec.update(pre)
        self.rec = rec
        if thk_mode:
            self.set_view("Thickness", f"{float(rec['target']):.1f} nm", 0.0, "공정 시작 — 메인 공정 대기")
        else:
            self.set_view("Time", "—", None, "Time 모드 공정(두께는 기록만)")

    # ── 메인 공정 창 두께(_thk_record) ──
    def sample(self, st: Mapping[str, Any], now: float, main_polling: bool, running: bool) -> None:
        try:
            rec = self.rec
            if not rec or st.get("stale") or not main_polling or not running:
                return
            seq, s = st.get("seq"), st.get("thickness_nm")
            if s is None or seq == rec.get("seq"):
                return
            rec["seq"] = seq
            pt = (float(s), float(now))
            if rec.get("first") is None:
                rec["first"] = pt
            rec["last"] = pt
        except Exception:
            pass

    # ── THICKNESS_WAIT 끝(_thk_on_end) ──
    def on_wait_end(self, kind: str, judge) -> Optional[str]:
        """종료 방식 기록·경고 목록. 상한이면 '유효 x / 목표 y nm' 를 돌려준다(E848 실패 문구 — 경고 목록에는 넣지 않음)."""
        rec = self.rec
        if rec is None:
            return None
        rec["end"] = kind
        rec["wait_ended"] = True
        target = float(rec.get("target") or 0.0)
        eff = None
        if judge is not None:
            eff = float(judge.eff)
            rec["final_nm"] = eff
            rec["avg_rate"] = judge.avg_rate()
            rec["jumps"] = int(judge.jump_count)
            rec["fallback_reason"] = judge.fallback_reason
        eff_txt = "--" if eff is None else f"{eff:.1f}"
        what = f"유효 {eff_txt} / 목표 {target:.1f} nm"
        warn = None
        if kind == END_LIMIT:
            self._log("THICKNESS", f"⚠ 최대 시간 상한 도달 — {what} → 공정 실패(E848)")
        elif kind == END_FALLBACK:
            warn = f"시간 대체로 종료({rec.get('fallback_reason') or '-'}) — 마지막 확인 {what}"
            self._log("THICKNESS", warn)
        else:
            self._log("THICKNESS", f"메인 공정 종료 — 종료 방식 {kind}, {what}")
            if kind == END_THICKNESS and judge is not None and judge.fallback_count:
                warn = f"공정 중 시간 대체 {judge.fallback_count}회({judge.fallback_reason}) — 복귀 뒤 두께로 종료"
        if warn:
            self.warnings.append(warn)
        pct = (eff / target * 100.0) if (eff is not None and target > 0) else None
        self.set_view(kind, f"{target:.1f} nm", pct, f"종료({kind}) — {eff_txt} nm")
        return what if kind == END_LIMIT else None

    # ── 기록 값(_thk_values) ──
    def values(self) -> dict:
        rec = dict(self.rec or {})
        if not rec:
            return {}
        if rec.get("mode") == "Thickness" and not rec.get("wait_ended"):
            rec["end"] = END_ABORT                    # 메인 공정 전에 끝남(Shutter Delay·영점 중 정지 등)
            rec["final_nm"] = None
            rec["avg_rate"] = None
            rec["before_main"] = True
            return rec
        if rec.get("mode") == "Time":
            f, l = rec.get("first"), rec.get("last")
            if f is None or l is None:
                return {"end": END_TIME}
            final = l[0] - f[0]
            span = l[1] - f[1]
            rec["final_nm"] = final
            rec["avg_rate"] = (final / span) if span > 0 else None
        return rec

    # ── 공정 끝(_thk_finalize_record) ──
    def finalize(self) -> dict:
        """기록 값(Calib CSV·엑셀 두께 열 입력)을 돌려주고 패널 마지막 결과·'두께 요약' 로그를 남긴다."""
        if self.rec is None:
            return {}
        try:
            v = self.values()
            if v.get("end") == END_TIME:
                fin = v.get("final_nm")
                self.set_view(END_TIME, "—", None, "종료(Time)" + (f" — {float(fin):.1f} nm" if fin is not None else ""))
            elif v.get("before_main"):
                tgt = v.get("target")
                self.set_view(END_ABORT, f"{float(tgt):.1f} nm" if tgt else "—", None, f"종료({END_ABORT}) — 메인 공정 전")
            if v.get("final_nm") is not None:
                ar = v.get("avg_rate")
                self._log("THICKNESS", f"두께 요약 — 종료 방식 {v.get('end')} · 최종 {float(v['final_nm']):.2f} nm"
                                       + (f" / 목표 {float(v['target']):g} nm" if v.get("target") else "")
                                       + (f" · 평균 {float(ar):.4f} nm/s" if ar is not None else "")
                                       + (f" · 큰 튐 {v.get('jumps')}회(기록만)" if v.get("jumps") else ""))
            return v
        except Exception as e:
            self._log("THICKNESS", f"⚠ 두께 기록 실패(무시): {e!r}")
            return {}

    # ── 종료 카드(_thk_card_fields) ──
    def card_fields(self, payload: dict) -> None:
        if self.rec is None:
            return
        try:
            v = self.values()
            if v.get("final_nm") is None and v.get("mode") != "Thickness":
                return
            payload.setdefault("thickness_end", str(v.get("end") or ""))
            fin = "--" if v.get("final_nm") is None else f"{float(v['final_nm']):.1f}"
            payload.setdefault("thickness_final", f"{fin} / {float(v['target']):g} nm" if v.get("target") else f"{fin} nm")
            if v.get("jumps"):
                payload.setdefault("thickness_jumps", f"{int(v['jumps'])}회(기록만)")
        except Exception:
            pass

    # ── 저널(v2) — 시각은 저장 시점 기준 나이(초) ──
    def to_state(self, now: float) -> dict:
        rec = None
        if self.rec is not None:
            rec = dict(self.rec)
            for k in ("first", "last"):
                if rec.get(k) is not None:
                    rec[k] = [rec[k][0], now - rec[k][1]]
        return {"rec": rec, "warnings": list(self.warnings), "view": dict(self.view)}

    def from_state(self, d: Mapping[str, Any], now: float) -> None:
        rec = d.get("rec")
        if rec is not None:
            rec = dict(rec)
            for k in ("first", "last"):
                if rec.get(k) is not None:
                    rec[k] = (float(rec[k][0]), now - float(rec[k][1]))
            rec["seq"] = None                         # 새 드라이버는 seq 를 처음부터 센다
        self.rec = rec
        self.warnings = list(d.get("warnings") or [])
        self.view = dict(d.get("view") or IDLE_VIEW)


# ═══════════════════════ 1초 행 블록(ThicknessEngine.snapshot_fields 의 키) ═══════════════════════
def blocks(chamber: str = "ch2") -> List[DeviceBlock]:
    """'<chamber>.thk' — 공정 쪽 두께 판정 값(장비 값 자체는 '<chamber>.stm100' 블록). 값이 없으면 None(채우지 않음)."""
    return [DeviceBlock(f"{chamber}.thk", f"{chamber.upper()} 두께 판정", (
        FieldDef("target_nm", FType.FLOAT, "nm", "목표 두께", "{:g}", source="recipe thickness"),
        FieldDef("eff_nm", FType.FLOAT, "nm", "유효 두께", "{:.2f}", source="judge", desc="장비 두께 − 기준점(영점 확인)"),
        FieldDef("remaining_nm", FType.FLOAT, "nm", "남은 두께", "{:.2f}", source="judge"),
        FieldDef("mode", FType.STR, "", "종료 방식", source="engine", desc="Thickness / 시간 대체 / Time / 상한 / 시간대체 / 중단"),
        FieldDef("phase", FType.STR, "", "단계", source="engine", detail=True, desc="idle / zero / wait"),
        FieldDef("done_n", FType.INT, "", "목표 이상 연속 횟수", source="judge", desc="THICKNESS_DONE_CONSECUTIVE 에 닿으면 종료"),
        FieldDef("done_need", FType.INT, "", "필요 연속 횟수", source="cfg", detail=True),
        FieldDef("judge", FType.STR, "", "판정 상태", source="judge", detail=True, desc="thickness / fallback"),
        FieldDef("permanent", FType.BOOL, "", "영구 시간 대체", source="judge", detail=True),
        FieldDef("pct", FType.FLOAT, "%", "진행률", "{:.0f}", source="judge"),
        FieldDef("text", FType.STR, "", "패널 문구", source="engine", detail=True),
    ), desc="두께 모니터 공정 연동(ThicknessEngine) — thickness_nm·rate_nm_s 는 stm100 블록")]


BLOCK_FIELDS = ("target_nm", "eff_nm", "remaining_nm", "mode", "phase", "done_n", "done_need", "judge", "permanent", "pct",
                "text")

