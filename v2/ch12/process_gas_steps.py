"""챔버12 가스 단계 규칙 C — 단계 설정(PROCESS_GAS_STEP_LEVELS)이 있는 가스는 레시피 시작이든 공정 중 변경이든 '올라갈 때 중간값을 작은
순서대로 거친다, 내릴 때는 바로'(사용자 결정 2026-11-19, D420 — D417 ③ 의 '중간값 구성이 바뀌면 거부'를 대신한다).

공정 중 값 바꾸기(PROCESS_ADJUST mfc.<가스>_flow)에서
- 아직 안 한 가스 주입(그 가스 묶음의 첫 스텝이 아직 안 옴): 거부하지 않고 새 목표 기준으로 묶음(FLOW_SET→FLOW_ON 단계들)을 다시 펼친다
  (process_steps.restep_gas — 레시피에 그 값이 있었던 것과 같은 결과). 예 10 → 9.2 면 [9.0] → 9.2, 10 → 12 면 9.0 → 9.5 → 12, 10 → 8 이면 8.
  다시 펼친 묶음은 런마다 gas_plan[(블록 순번, 항목)] = 값으로 기억한다 — 시퀀스 편집·재시작 뒤 다시 펼칠 때 지난 묶음 모양을 그대로 만들기 위해.
- 이미 흐르는 중(그 가스 묶음이 지남): 지금 설정값 < 중간값 < 새 값 인 중간값을 작은 순서대로 FLOW_SET → FLOW_ON(도달 판정 — 가스 주입과
  같은 MFC_FLOW_STABLE_COUNT 회 연속·같은 시간 초과) → … → 마지막 새 값도 같은 판정. 예 8 → 10 = 9.0 → 9.5 → 10, 9.2 → 10 = 9.5 → 10.
  중간값이 없으면(10 → 12) 또는 내리면(10 → 8) 예전처럼 바로(FLOW_SET 확인 — 단계 없는 가스와 같음).
  단계 명령은 MFC 에 공정 소유자와 다른 이름(STEP_OWNER_SUFFIX)으로 보낸다 — 확인 사건이 러너가 기다리는 다른 가스의 FLOW_SET/ON 과
  섞이지 않게(MFC 이벤트는 소유자가 같을 때만 러너로 감). 공정 끝 정리에서 그 이름도 함께 푼다(process_actions.cleanup_devices).
- 실패: 중간 단계든 마지막이든 도달 못 하면 가스 주입 때와 같이 공정 중단(러너 step_failed('MFC', 드라이버 문구) — '[MFC - GAS 안정화 시간 초과]').
  정지·이상 중단·끝 정지가 시작되면 단계를 멈춘다(그때 온 취소 결과는 실패로 보지 않음).
- 단계 진행 중 같은 가스의 새 변경, 그 가스 주입 묶음을 러너가 돌리는 중의 변경은 거부(ADJUST_GAS_STEP — 'O2 단계 진행 중 — 끝난 뒤 다시').
- 보이기: 단계마다 공정 진행 줄(process adjust, result stepping — 'O2 9.0 → 9.5 → 10 진행 중 2/3'), 화면 '공정 중 값 바꾸기' 항목에
  stepping {levels, i, n, text}. 끝나면 confirmed, 실패면 failed.
겹칠 때(결정 D420): 일시정지 중에도 단계는 계속(사람이 준 값 바꾸기 — 일시정지는 블록 타이머·파워 명령만 멈춤), 건너뛰기는 가스 주입 블록이
건너뛸 수 없는 블록이라 묶음 도중에 끊기지 않음(다른 블록 건너뛰기는 단계와 무관), 시퀀스 편집은 지난 묶음 = gas_plan, 남은 묶음 = 수정값으로
다시 펼침, 재시작 뒤 이어가기는 단계 진행 중이었으면 이어가지 않고 안전 정지(resume 확인 gas_step — 값 확인 없이 이어가면 중간값에 멈춘 채
계속될 수 있다), 단계가 없던 때의 수정값은 지금처럼 다시 펼친 스텝에 그대로.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from v2.ch12 import process_steps as PS

STEP_OWNER_SUFFIX = ":step"


def fmt_level(x: float, configured: bool) -> str:
    """단계 글 — 설정 중간값은 소수 한 자리('9.0'), 목표는 그대로(:g — '10', '9.2')."""
    return f"{float(x):.1f}" if configured else f"{float(x):g}"


def steps_text(gas: str, levels: List[float], i: int, n_mid: int) -> str:
    """'O2 9.0 → 9.5 → 10 진행 중 2/3'(i = 지금 단계 1부터)."""
    path = " → ".join(fmt_level(x, k < n_mid) for k, x in enumerate(levels))
    return f"{gas} {path} 진행 중 {i}/{len(levels)}"


class GasStepMixin:
    def _init_gas_steps(self) -> None:
        self.gas_steps: Dict[str, dict] = {}            # 흐르는 중 단계 진행(가스 이름 → 상태)
        self.gas_plan: Dict[tuple, float] = {}          # 이번 런에서 다시 펼친 묶음 (블록 순번, 항목 키) → 목표값

    # ───────── 공통 ─────────
    def _sc_now(self):
        """지금 설정으로 만든 StepCfg(가스 단계 설정은 설정 적용 뒤 바뀔 수 있음) — 실패하면 엔진이 처음 만든 것."""
        try:
            return PS.step_cfg(self.cfg, self.n)
        except Exception:
            return self.sc

    def gas_levels_of(self, key: str) -> tuple:
        """(가스, 채널, 단계 중간값 tuple) — 가스 유량 항목이 아니거나 단계 설정이 없으면 단계는 ()."""
        sc = self._sc_now()
        gc = PS.gas_of_key(sc, key)
        if gc is None:
            return None, None, ()
        return gc[0], gc[1], tuple(sc.gas_steps.get(gc[0], ()))

    def gas_step_owner(self) -> str:
        return f"{self.actions.owner}{STEP_OWNER_SUFFIX}"

    def _gas_busy(self, gas: str, c: int) -> str:
        """같은 가스의 단계 진행 중·그 가스 주입 묶음을 러너가 돌리는 중이면 거부 사유 글, 아니면 ''."""
        st = self.gas_steps.get(gas)
        if st is not None:
            return f"{gas} 단계 진행 중({st['text']}) — 끝난 뒤 다시"
        cur = self.runner.current if self._active_run() else None
        if cur is not None and cur.action == "MFC_CMD" and cur.params[0] in ("FLOW_SET", "FLOW_ON") \
                and int(cur.params[1].get("channel", -1)) == int(c):
            return f"{gas} 가스 주입 진행 중 — 끝난 뒤 다시"
        return ""

    # ───────── 아직 안 한 가스 주입: 다시 펼치기 ─────────
    def gas_restep_future(self, key: str, value: float) -> List[int]:
        """지금 런의 아직 시작 안 한 묶음을 새 목표로 다시 펼치고 gas_plan 에 기억 — 다시 펼친 블록 순번들."""
        sc = self._sc_now()
        with self.lk:
            r = self.runner
            start = r.idx + 1
            before = {id(s) for s in r.steps}
            new, n = PS.restep_gas(r.steps, self.cur_insts, sc, key, float(value), start)
            if not n:
                return []
            blocks = sorted({s.block for s in new[start:] if id(s) not in before and s.ref == key})
            for b in blocks:
                self.gas_plan[(b, key)] = float(value)
            r.steps = new
            self.cur_steps = new
        return blocks

    def apply_gas_plan(self, steps: list, insts: list, plan: Dict[tuple, float], below_block: Optional[int] = None) -> list:
        """다시 펼칠 때(시퀀스 편집·재시작) 기억한 묶음 모양을 그대로 — below_block 이 있으면 그보다 앞 블록만."""
        sc = self._sc_now()
        out = steps
        for (b, key), v in sorted(plan.items(), key=lambda x: (x[0][0], x[0][1])):
            if below_block is not None and b >= below_block:
                continue
            out = PS.restep_gas_block(out, insts, sc, key, float(v), b)
        return out

    def gas_plan_record(self) -> list:
        return [{"block": b, "key": k, "value": v} for (b, k), v in sorted(self.gas_plan.items())]

    @staticmethod
    def gas_plan_from(rec) -> Dict[tuple, float]:
        out: Dict[tuple, float] = {}
        for x in rec or []:
            try:
                out[(int(x["block"]), str(x["key"]))] = float(x["value"])
            except Exception:
                continue
        return out

    # ───────── 이미 흐르는 중: 단계 진행 ─────────
    def gas_steps_start(self, ai, gas: str, c: int, ups: List[float], final: float, old: Any, source: str,
                        by: Optional[str]) -> None:
        levels = list(ups) + [float(final)]
        st = {"gas": gas, "channel": int(c), "levels": levels, "n_mid": len(ups), "i": 0, "key": ai.key, "device": ai.device,
              "item": ai.item, "old": old, "new": float(final), "source": source, "by": by, "text": ""}
        st["text"] = steps_text(gas, levels, 1, len(ups))
        self.gas_steps[gas] = st
        self._gas_step_next(st)

    def _gas_step_emit(self, st: dict, result: str, reason: Optional[str] = None) -> None:
        self.emit("adjust", st["text"] if result == "stepping" else f"{st['device']}.{st['item']} {st['old']!r} → {st['new']!r} ({result})",
                  reason, level="warn" if result == "failed" else "info", device=st["device"], item=st["item"], old=st["old"],
                  new=st["new"], source=st["source"], by=st["by"], result=result, reason=reason, run_id=self.run_id,
                  steps=list(st["levels"]), step=st["i"] + (1 if result == "stepping" else 0), text=st["text"])

    def _gas_step_next(self, st: dict) -> None:
        gas = st["gas"]
        if self.gas_steps.get(gas) is not st:
            return
        if not self._active_run():
            self.gas_steps.pop(gas, None)
            self.log("Process", f"[ADJUST] {gas} 단계 멈춤 — 공정 정지·끝 정지 시작({st['text']})")
            return
        i = st["i"]
        if i >= len(st["levels"]):
            self.gas_steps.pop(gas, None)
            self._rec(st["device"], st["item"], st["old"], st["new"], st["source"], "confirmed", None, st["by"])
            return
        lv = st["levels"][i]
        st["text"] = steps_text(gas, st["levels"], i + 1, st["n_mid"])
        self.log("Process", f"[ADJUST] {st['text']} — FLOW_SET {lv:g} sccm → 도달 대기")
        self._gas_step_emit(st, "stepping")
        a, owner, c = self.actions, self.gas_step_owner(), st["channel"]

        def after_on(_k, reason, detail) -> None:
            if reason is None:
                st["i"] += 1
                self._gas_step_next(st)
            else:
                self._gas_step_fail(st, "FLOW_ON", reason, detail)

        def after_set(_k, reason, detail) -> None:
            if self.gas_steps.get(gas) is not st:
                return
            if reason is not None:
                self._gas_step_fail(st, "FLOW_SET", reason, detail)
            elif not self._active_run():
                self._gas_step_next(st)                      # 정지가 시작됨 — FLOW_ON 을 보내지 않고 멈춤
            else:
                a.mfc.flow_on(c, owner=owner, on_done=after_on)
        a.mfc.set_flow(c, float(lv), owner=owner, on_done=after_set)

    def _gas_step_fail(self, st: dict, cmd: str, reason: str, detail: str) -> None:
        gas = st["gas"]
        if self.gas_steps.get(gas) is not st:
            return
        self.gas_steps.pop(gas, None)
        if not self._active_run():                           # 정지·끝 정지가 안정화를 취소한 것 — 실패로 보지 않음
            self.log("Process", f"[ADJUST] {gas} 단계 멈춤 — {cmd} {detail or reason}(공정 정지 중)")
            return
        self._gas_step_emit(st, "failed", reason)
        self.log("Process", f"[ADJUST] {st['text']} — {cmd} 실패: {detail or reason} → 공정 중단(가스 주입 실패와 같이)")
        self.runner.step_failed("MFC", detail or reason or "unknown", code=None,
                                meta={"cmd": cmd, "gas": gas, "kind": "command_failed", "ch": self.n, "adjust": True})

    def gas_stepping_state(self, key: str) -> Optional[dict]:
        for st in self.gas_steps.values():
            if st["key"] == key:
                return {"levels": list(st["levels"]), "i": st["i"] + 1, "n": len(st["levels"]), "text": st["text"]}
        return None
