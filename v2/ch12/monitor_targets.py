"""챔버12 공정 모니터 기준 = 지금 장비에 확정된 목표(2026-11-19 D423) — 엔진 쪽 연결(MonitorTargetMixin).

공정 모니터(process_monitor — 가스 유량·작업 압력 편차 알림)는 v1 처럼 run_start 의 레시피 값으로 기준을 한 번 정했다. 그래서 공정 중
값 바꾸기·가스 단계(규칙 C)·뒤 블록 예약 값이 바뀌어도 기준은 레시피 값이라 사람이 값을 바꿀 때마다 거짓 편차 카드(MONITOR)가 나갔다.
여기서 기준을 '지금 장비에 확정된 목표'로 따라가게 한다 — 어느 경로든 같은 규칙:
- 러너 스텝(레시피·예약한 공정 중 값·시퀀스 편집·재시작 뒤 다시 펼친 스텝): 가스 묶음의 FLOW_SET 이 시작되면 그 가스 판정을 쉬고
  (중간 단계 포함), 목표 FLOW_SET 뒤의 FLOW_ON 이 도달(연속 N 회)로 끝나면 기준 = 그 값. SP1_SET 이 시작되면 압력 판정을 쉬고,
  끝나면 기준 = 그 값. 러너가 다음 스텝으로 넘어갔다 = 앞 스텝이 확정(실패면 공정이 멈추므로 기준을 바꾸지 않음). 셔터 열기 전이면
  v1 처럼 바로 판정, 판정 중(셔터 열림)이면 새 기준에 처음 들어올 때부터(process_monitor — 옮겨 가는 동안 미룸).
- 공정 중 바로 바꾸기(process_adjust._apply_live — 가스 FLOW_SET·작업 압력 SP1_SET): 보낼 때 판정 쉼 → 확정되면 기준 = 새 값
  (실제 값이 새 기준에 처음 들어올 때까지 미룸 — 옮겨 가는 동안, 셔터 열기 전이라도) / 실패면 기준 그대로.
- 가스 단계 규칙 C(process_gas_steps): 시작부터 끝까지 판정 쉼 → 마지막 단계 도달 = 기준 새 값 / 실패면 기준 그대로(공정은 중단).
- 재시작 뒤 이어가기: 모니터를 params 로 다시 켜고 기준 = 이번 런에서 확정한 값(monitor_baseline — 실행한 스텝 + 공정 중 값),
  Main Shutter 가 열린 채였으면 감시도 다시(process_monitor.EngineMonitor.restore_after_resume).
- 가스 유량을 바꾸면(공정 중 바로 바꾸기·가스 단계는 언제나, 러너 스텝은 판정 중일 때) 확정까지 압력 판정도 쉬고, 확정·실패 뒤
  압력은 기준 그대로 처음 기준 안에 들어올 때부터(유량이 바뀌면 압력도 잠깐 흔들린다).
항목 키: 가스 'Ar'·'O2'·'N2'(모니터가 쓰는 이름), 작업 압력 'pressure'. 모니터가 없거나 그 항목을 감시하지 않으면 아무 일도 없음.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


def gas_key(name: str) -> str:
    """엔진 가스 이름('AR'·'O2'·'N2') → 모니터 항목('Ar'·'O2'·'N2')."""
    return str(name).capitalize()


class MonitorTargetMixin:
    def _mon(self):
        return getattr(self, "monitor", None)

    def mon_key_of_channel(self, c: int) -> Optional[str]:
        for g, info in (self.sc.gas_info or {}).items():
            if int(info.get("channel", -1)) == int(c):
                return gas_key(g)
        return None

    def mon_hold(self, key: Optional[str], why: str = "", *, live: bool = True) -> None:
        """그 항목 판정을 쉼. 가스 유량이면 압력 판정도 쉼(유량이 바뀌면 압력도 잠깐 흔들림) — 공정 중 바로 바꾸기(live)는 언제나,
        러너 스텝은 판정 중(셔터 열림)일 때만."""
        m = self._mon()
        if m is None or not key:
            return
        m.hold(key, why)
        if key != "pressure":
            m.hold("pressure", f"{key} 유량 바꾸는 중", by=f"gas:{key}", only_active=not live)

    def mon_release(self, key: Optional[str]) -> None:
        """바꾸기 실패 — 기준 그대로 판정을 다시(가스면 압력도 기준 그대로 다시 — 처음 기준 안에 들어올 때부터)."""
        m = self._mon()
        if m is None or not key:
            return
        m.release(key)
        if key != "pressure":
            m.rearm("pressure", by=f"gas:{key}", live=True)

    def mon_set(self, key: Optional[str], value: Any, why: str, *, live: bool = True) -> None:
        """확정 — 기준 = 새 값. 공정 중 바로 바꾼 값(live)·판정 중이면 새 기준에 처음 들어올 때부터 판정(옮겨 가는 동안 미룸).
        가스면 쉬게 했던 압력도 기준 그대로 같은 방식으로 다시."""
        m = self._mon()
        if m is None or not key or value is None:
            return
        m.set_target(key, float(value), why, live=live)
        if key != "pressure":
            m.rearm("pressure", by=f"gas:{key}", live=live)

    # ── 러너 스텝(on_step 이 부른다 — 러너 잠금 안) ──
    def mon_on_step(self, idx: int, step, shutdown: bool) -> None:
        """새 스텝이 시작됨 = 앞 스텝 확정. 앞 스텝이 목표 FLOW_SET 의 FLOW_ON(도달)·SP1_SET 이면 기준 = 그 값,
        지금 스텝이 FLOW_SET·SP1_SET 이면 그 항목 판정을 쉼. 정지 순서(종료 절차) 중에는 기준을 바꾸지 않음."""
        if shutdown or self._mon() is None:
            return
        steps = self.runner.steps
        prev = steps[idx - 1] if 0 < idx <= len(steps) else None
        if prev is not None and prev.action == "MFC_CMD":
            cmd, args = prev.params
            if cmd == "FLOW_ON":
                c = int(args.get("channel", -1))
                j = idx - 2
                while j >= 0 and not (steps[j].action == "MFC_CMD" and steps[j].params[0] == "FLOW_SET"
                                      and int(steps[j].params[1].get("channel", -1)) == c):
                    j -= 1
                if j >= 0 and steps[j].ref:                     # 목표 FLOW_SET 뒤 도달 — 중간 단계면 계속 쉼
                    why = "공정 중 값 바꾸기(예약)" if steps[j].ref in self.overrides else "공정 진행"
                    self.mon_set(self.mon_key_of_channel(c), steps[j].params[1].get("value"), why, live=False)
            elif cmd == "SP1_SET":
                why = "공정 중 값 바꾸기(예약)" if prev.ref in self.overrides else "공정 진행"
                self.mon_set("pressure", args.get("value"), why, live=False)
        if step is not None and step.action == "MFC_CMD":
            cmd, args = step.params
            if cmd == "FLOW_SET":
                self.mon_hold(self.mon_key_of_channel(int(args.get("channel", -1))), "가스 설정 중", live=False)
            elif cmd == "SP1_SET":
                self.mon_hold("pressure", "작업 압력 설정 중", live=False)

    # ── 재시작 뒤 이어가기 ──
    def monitor_baseline(self) -> Dict[str, Any]:
        """이번 런에서 이미 확정한 목표(실행한 스텝 + 공정 중 값 — _writes 가 지난 스텝에 수정값을 얹어 줌)와 Main Shutter 상태."""
        w = self._writes() if hasattr(self, "_writes") else {}
        out: Dict[str, float] = {}
        for ch_s, v in (w.get("flow") or {}).items():
            k = self.mon_key_of_channel(int(ch_s))
            if k:
                out[k] = float(v)
        sp1 = (w.get("sp") or {}).get("1")
        if sp1 is not None:
            out["pressure"] = float(sp1)
        r = self.runner
        ms = None
        for s in r.steps[:max(0, r.idx)]:
            if s.action == "PLC_CMD" and s.params and str(s.params[0]).upper() == "MS":
                ms = bool(s.params[1])
        return {"targets": out, "shutter_open": bool(ms)}
