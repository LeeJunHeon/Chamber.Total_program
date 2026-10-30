"""CH2 히터 엔진 — 로그·구글챗 카드·이상 사건(특이사항)·문구 도우미. v1 controller/heater_service.py:523-643 그대로.

v1 은 로그를 CH2 런타임 append_log("HEATER", 문구)로, 카드를 CH2 챗(_post_card('[CH2] 히터 {제목}', 부제, 상태, 필드, urgent=True))
으로 보냈다. v2 는 같은 문구를 이벤트로 낸다 — HeaterEvent("log", 문구, data={"source": "HEATER"}),
HeaterEvent("card", 부제, data={"title", "subtitle", "status", "fields", "urgent"}) — 카드는 포털이 같은 계기·같은 내용으로 보낸다
(지시 3장 (7)). 카드는 챗 객체 유무와 상관없이 늘 이벤트로 낸다(v1: 챗 없으면 생략 — 받는 쪽이 정한다).
"""
from __future__ import annotations

from typing import List, Optional

from v2.ch12.drivers.heater import HeaterEvent
from v2.ch12.heater_process import itl_missing_text

OWNER_LABEL = {"manual": "수동", "process": "공정", "external": "외부"}
# 종료 카드 ✅ 사유(이 밖의 사유·이상 사건·PLC 이상 플래그가 있으면 ❌) — controller/heater_service.py:73-74
NORMAL_END_REASONS = frozenset({"정지(패널)", "공정 종료", "공정 중단 — 사용자 STOP", "공정 중단 — 원격 STOP"})
EVENTS_SHOWN_MAX = 4


class HeaterNotify:
    """HeaterEngine 이 섞어 쓴다(self.on_event·self._st·self._episode_notes·self.owner·self.hold·self.sat …)."""

    # ── 내보내기 ──
    def _emit(self, ev: HeaterEvent) -> None:
        self.events.append(ev)
        if self.on_event is not None:
            try:
                self.on_event(ev)
            except Exception:
                pass

    def _log(self, msg: str) -> None:
        self._emit(HeaterEvent("log", str(msg), data={"source": "HEATER"}))

    def _card(self, title: str, subtitle: str, status: str, fields: Optional[dict] = None) -> None:
        self._emit(HeaterEvent("card", str(subtitle), data={"title": f"[CH2] 히터 {title}", "subtitle": str(subtitle),
                                                             "status": status, "fields": dict(fields or {}),
                                                             "urgent": True}))

    def cards(self, word: str = "") -> List[HeaterEvent]:
        return [e for e in self.events if e.kind == "card" and word in e.data.get("title", "")]

    def logs(self, word: str = "") -> List[str]:
        return [e.detail for e in self.events if e.kind == "log" and word in e.detail]

    # ── 유지·포화 모듈 콜백(heater_service.py:538-582) ──
    def _on_hold_msg(self, level: str, text: str) -> None:
        self._log(("⚠ " if level == "경고" else "") + text)

    def _on_hold_engaged(self, kind: str) -> None:
        self._log(f"유지 진입 DAC 상한 {self.hold.value}")

    def _on_hold_give_up(self, why: str) -> None:
        self._give_up_seq += 1
        self._give_up_text = str(why)
        self._event("유지 모드 포기")
        self._card("유지 모드 포기", f"{self.context()} · {why}", "FAIL", {"PV": self._fmt(self._st.get("pv"))})

    def _on_saturated(self, info: dict) -> None:
        clamp = info.get("clamp")
        self._event(f"DAC 포화{'(클램프 ' + str(clamp) + ')' if clamp is not None else ''}")
        self._card("DAC 포화", f"{self.context()} · 도달 후 {float(info.get('sec') or 0):.0f}초째 상한({info.get('limit')})에 물림 → D118 {clamp}",
                   "FAIL", {"PV": self._fmt(info.get("pv")), "MV": str(info.get("mv")),
                            "클램프": "-" if clamp is None else str(clamp)})

    def _on_stalled(self, info: dict) -> None:
        self._event("승온 정체")
        eta = info.get("eta_min")
        self._card("승온 정체",
                   f"{self.context()} · 목표 {float(info.get('target') or 0):.1f}°C · 출력 상한 {info.get('limit')} 에서 상승 "
                   f"{float(info.get('rate') or 0):.2f}°C/min", "FAIL",
                   {"PV": self._fmt(info.get("pv")), "MV": f"{info.get('mv'):.0f}/{info.get('limit')}",
                    "현재 속도로": "도달 안 됨" if eta is None else f"약 {float(eta):.0f}분",
                    "확인": "HEATER_MV_LIMIT(출력 상한)"})

    # ── 문구 ──
    @staticmethod
    def _fmt(v) -> str:
        return "--.-" if v is None else f"{float(v):.1f}°C"

    @staticmethod
    def _num(v) -> str:
        return "-" if v is None else f"{float(v):g}"

    @staticmethod
    def _fault_label(st: dict) -> str:
        if st.get("ot"):
            return "이상 — 과온"
        if st.get("tc_err"):
            return "이상 — 온도센서"
        if st.get("wd_err"):
            return "이상 — 통신 워치독"
        return "이상"

    @staticmethod
    def _fault_text(st: dict) -> str:
        if st.get("ot"):
            return "과온 트립"
        if st.get("tc_err"):
            return "온도센서 이상(단선/모듈)"
        if st.get("wd_err"):
            return "통신 워치독 타임아웃"
        return f"히터 이상 (PID err={st.get('pid_err')})"

    @staticmethod
    def _itl_missing(st: dict) -> str:
        return itl_missing_text(st)

    @staticmethod
    def _fmt_duration(sec: float) -> str:
        m = int(max(0.0, float(sec)) // 60)
        return f"{m // 60}시간 {m % 60}분" if m >= 60 else f"{m}분"

    def _event(self, label: str) -> None:
        """이번 운전의 이상 사건(같은 라벨은 한 번) — 종료·도달 카드의 특이사항."""
        if label and label not in self._episode_notes:
            self._episode_notes.append(label)

    def _events_text(self) -> str:
        ev = list(self._episode_notes)
        if not ev:
            return "없음"
        if len(ev) > EVENTS_SHOWN_MAX:
            return " · ".join(ev[:EVENTS_SHOWN_MAX]) + f" 외 {len(ev) - EVENTS_SHOWN_MAX}건"
        return " · ".join(ev)

    # ── 맥락(heater_service.py:195-231) ──
    def set_context(self, text: Optional[str]) -> None:
        """카드 부제 맥락(공정 "이름" (현재/전체)). None 이면 수동/외부 기본값. 운전 중에 바뀌면 이번 운전 맥락도 따라감."""
        with self._lock:
            self._proc_ctx = str(text) if text else None
            if self._proc_ctx and self._run_prev:
                self._run_ctx = self._proc_ctx

    def context(self) -> str:
        if self._run_prev and self._run_ctx:
            return self._run_ctx
        if self.owner == "process" and self._proc_ctx:
            return self._proc_ctx
        return OWNER_LABEL.get(self.owner or "manual", self.owner or "수동")

    def notify_reached(self, target, pv, took_sec: float, next_step: str = "",
                       soak_sec: float = 0.0, soak_pv: str = "") -> None:
        """공정 소유 히터의 승온 대기 통과 카드 1장 — 이상 사건이 없으면 ✅, 있으면 ❌."""
        took = int(max(0.0, float(took_sec or 0.0)))
        soak = int(round(max(0.0, float(soak_sec or 0.0))))
        ramp = max(0, took - soak)
        fields = {"목표": self._fmt(target), "도달 PV": self._fmt(pv), "승온 소요": f"{ramp // 60}분 {ramp % 60}초"}
        if soak > 0:
            fields["SOAK"] = f"{soak // 60}분 {soak % 60}초"
            fields["SOAK 중 PV"] = str(soak_pv)
        if next_step:
            fields["다음 단계"] = str(next_step)
        if self._episode_notes:
            fields["특이사항"] = self._events_text()
        self._card("도달", self.context(), "FAIL" if self._episode_notes else "SUCCESS", fields)
