"""챔버12 공정 중 자유 제어 4종(사용자 지시 3장 (3) — 웹·로컬 화면에서, 로봇 제외. 출처 검사는 core 몫).

챔버K v2/chk/process_engine.py 의 adjust 와 같은 방식: 검사(공정 중·값 범위·이번 공정에서 쓰는 항목) → 지금 블록이 쓰는 중이면
드라이버에 바로 쓰고 되읽어 확인(applied → confirmed/failed), 아직 안 온 블록이 쓰면 그 스텝 값을 바꿔 둔다(scheduled) → 기록
(ProcEvent "adjust": 장비·항목·이전 → 새 값·출처·결과·사유). 항목 표는 blocks.ADJUST_ITEMS(스텝 ref 로 찾는다).
- 지금 쓰기: MFC 유량 FLOW_SET(그 채널 — 확인은 v1 FLOW_SET 확인 경로)·SP1_SET, DC·DC2·RF 연속 새 목표(드라이버 목표값 —
  보정·램프가 따라간다), DC Pulse·RF Pulse set_reference_power(REF_POWER 확인), 히터 apply_target(공정 소유 유지),
  메인 남은 시간(지금 DELAY 남은 시간 = 값), 지금 대기 끝내기(skip_wait = 그 블록 건너뛰기와 같은 정리).
- 블록 건너뛰기: 블록 정의 skippable 일 때만(아니면 SKIP_NOT_ALLOWED) — 정리(skip_cleanup) 후 그 블록 마지막 스텝 뒤로.
- 일시정지·재개: 블록 정의 pausable 일 때만(PAUSE_NOT_ALLOWED). 대기류 = 타이머 정지(남은 시간 유지, ETA 같이 늦춤),
  파워 켜기 = 아직 안 보낸 파워 명령을 보내지 않고 지금 출력 유지(재개하면 이어서). 히터 SOAK 정지는 히터 엔진 쪽 기능이 없어
  PAUSE_NOT_AVAILABLE(결정 후보).
- 시퀀스 편집: blocks.SequenceState.edit(아직 시작 안 한 블록만, 판 +1, 전체 재검사) → 런 다시 나눔 → 지금 런은 다시 펼쳐 남은
  스텝만 바꾼다(지난 스텝은 같은 블록이라 그대로). 원래 레시피·편집 기록(record())·실제 실행 순서(executed)를 run_end 에 싣는다.
어떤 편집도 정지 순서(STOP·이상 중단은 고정 — 지금 구간 장비), 런 끝 정지(런마다 펼쳐 자동 끝 정지), 인터락·두절 감시(안전 규칙
4단계 — 엔진 밖)를 빼지 못한다. 정지 순서 중에는 넷 다 PROCESS_NOT_RUNNING.
"""
from __future__ import annotations

import copy
from typing import Any, Dict, Optional

from v2.ch12 import blocks as BK
from v2.ch12.process_list import USE_KEYS, derive_params, plan_runs

_CH = {"ar_flow": 1, "o2_flow": 2, "n2_flow": 3}
_TIMER_BLOCKS = ("wait", "main", "test", "row_delay")


class AdjustMixin:
    def _init_adjust(self) -> None:
        self.overrides: Dict[str, Any] = {}
        self.adjusted: Dict[str, dict] = {}
        self.reject_detail = ""
        self._abort_code: Optional[str] = None
        self._stop_user = True

    # ───────── 공통 ─────────
    def _active_run(self) -> bool:
        r = self.runner
        return bool(r.is_running and not r.shutdown and not r.stop_requested)

    def _rec(self, device: str, item: str, old: Any, new: Any, source: str, result: str, reason: Optional[str],
             by: Optional[str]) -> None:
        self.emit("adjust", f"{device}.{item} {old!r} → {new!r} ({result})", reason,
                  level="warn" if result in ("rejected", "failed") else "info",
                  device=device, item=item, old=old, new=new, source=source, by=by, result=result, reason=reason,
                  run_id=self.run_id)
        self.log("Process", f"[ADJUST] {device}.{item}: {old!r} → {new!r} ({result}{', ' + reason if reason else ''}, "
                            f"출처 {source})")

    def _ref_steps(self, key: str):
        """(지난 스텝들, 남은 스텝들) — 이 항목(ref)을 쓰는 지금 런의 스텝."""
        r = self.runner
        steps = r.steps if r.is_running and not r.shutdown else []
        past = [s for i, s in enumerate(steps) if s.ref == key and i <= r.idx]
        fut = [s for i, s in enumerate(steps) if s.ref == key and i > r.idx]
        return past, fut

    @staticmethod
    def _step_value(s) -> Any:
        if s.action == "MFC_CMD":
            return s.params[1].get("value")
        if s.action == "DELAY":
            return (s.duration or 0) / 1000.0
        return s.value

    def _set_step_value(self, s, v: float) -> None:
        if s.action == "MFC_CMD":
            args = dict(s.params[1])
            args["value"] = float(v)
            s.params = (s.params[0], args)
        elif s.action == "DELAY":
            s.duration = int(round(float(v) * 1000.0))
        else:
            s.value = float(v)

    # ═══════════════════════ PROCESS_ADJUST ═══════════════════════
    def adjust(self, device: str, item: str, value: Any, source: str = "web", by: Optional[str] = None) -> Optional[str]:
        ai = BK.adjust_item(str(device), str(item))
        if ai is None:
            self.reject_detail = f"{device}.{item} — 모르는 항목"
            self._rec(str(device), str(item), None, value, source, "rejected", "BAD_ARGS", by)
            return "BAD_ARGS"
        if not self._active_run():
            self._rec(device, item, None, value, source, "rejected", "PROCESS_NOT_RUNNING", by)
            return "PROCESS_NOT_RUNNING"
        e = BK._check_param(ai.param, value)
        if e:
            self.reject_detail = e
            why = "BAD_ARGS" if ("숫자" in e or "true/false" in e) else "ADJUST_OUT_OF_RANGE"
            self._rec(device, item, None, value, source, "rejected", why, by)
            return why
        if ai.key == "process.skip_wait":
            if value is not True:
                return "BAD_ARGS"
            inst = self.cur_inst
            if inst is None or inst.type not in _TIMER_BLOCKS + ("base_wait", "pressure_ctrl", "pressure_boost",
                                                                  "heater_wait", "heater_hold", "rga_scan"):
                self._rec(device, item, None, True, source, "rejected", "ADJUST_NOT_APPLICABLE", by)
                return "ADJUST_NOT_APPLICABLE"
            why = self.skip_block(source, by)
            self._rec(device, item, None, True, source, "rejected" if why else "applied", why, by)
            return why
        past, fut = self._ref_steps(ai.key)
        if not past and not fut:
            self._rec(device, item, None, value, source, "rejected", "ADJUST_NOT_USED", by)
            return "ADJUST_NOT_USED"
        v = float(value)
        old = self.overrides.get(ai.key)
        if old is None:
            old = self._step_value((past or fut)[-1] if past else fut[0])
        for s in fut:
            self._set_step_value(s, v)
        self.overrides[ai.key] = v
        self.adjusted[ai.key] = {"by": by, "t": self.clock.wall(), "source": source}
        live = bool(past) and self._apply_live(ai, v, old, source, by)
        self._rec(device, item, old, v, source, "applied" if live else "scheduled", None, by)
        return None

    def _apply_live(self, ai, v: float, old: Any, source: str, by: Optional[str]) -> bool:
        a, key = self.actions, ai.key

        def confirm(reason: Optional[str]) -> None:
            self._rec(ai.device, ai.item, old, v, source, "confirmed" if reason is None else "failed", reason, by)
        if ai.device == "mfc":
            if key == "mfc.wp":
                a.mfc.sp_set(1, v, owner=a.owner, on_done=lambda k, r, d: confirm(r))
            else:
                a.mfc.set_flow(_CH[ai.item], v, owner=a.owner, on_done=lambda k, r, d: confirm(r))
            return True
        if key in ("dc.power", "dc2.power", "rf.power"):
            d = a.dc.get(1 if key == "dc.power" else 2) if key != "rf.power" else a.rf
            if d is None or not getattr(d, "running", False):
                return False
            d.target_power = float(v)
            if hasattr(d, "sent_target_reached"):
                d.sent_target_reached = False
            return True
        if key in ("dcp.power", "rfp.power"):
            d = a.dcp if key == "dcp.power" else a.rfp
            if d is None or d.worker is None:
                return False
            d.set_reference_power(v, pause_polling=True, on_done=lambda r, ok: confirm(None if ok else (r or "FAILED")))
            return True
        if key == "heater.target":
            h = self.heater
            if h is None:
                return False
            h.apply_target(v, owner="process", on_done=lambda ok, why="": confirm(None if ok else (why or "FAILED")))
            return True
        if key == "process.main_remaining":
            cur = self.runner.current
            if cur is not None and cur.ref == key and self.runner.cd is not None:
                self.runner.set_remaining(v)
                return True
        return False

    def adjust_state(self) -> Optional[list]:
        """항목마다 {device, item, value, recipe, adjusted, scope} — 공정 밖이면 None."""
        if not self._active_run():
            return None
        out = []
        for ai in BK.ADJUST_ITEMS:
            past, fut = self._ref_steps(ai.key)
            scope = "live" if past and (ai.device != "process") else "future" if fut else ("done" if past else "unused")
            if ai.key == "process.main_remaining":
                cur = self.runner.current
                scope = "live" if cur is not None and cur.ref == ai.key else ("future" if fut else "unused")
            recipe = self._step_value((past or fut)[-1] if past else fut[0]) if (past or fut) else None
            val = self.runner.remaining_s() if scope == "live" and ai.key == "process.main_remaining" else \
                self.overrides.get(ai.key, recipe)
            out.append({"device": ai.device, "item": ai.item, "value": val, "recipe": recipe, "scope": scope,
                        "adjusted": dict(self.adjusted[ai.key]) if ai.key in self.adjusted else None})
        return out

    # ═══════════════════════ 블록 건너뛰기 ═══════════════════════
    def _cur_def(self):
        inst = self.cur_inst
        return (inst, BK.by_type()[inst.type]) if inst is not None else (None, None)

    def skip_block(self, source: str = "web", by: Optional[str] = None) -> Optional[str]:
        if self.state_name == "COOLDOWN" and self.cur_inst is None and isinstance(self.cur_block, int) and \
                0 <= self.cur_block < len(self.blocks) and self.blocks[self.cur_block]["type"] == "row_delay":
            return "SKIP_NOT_ALLOWED"                       # delay 행(러너 대기)은 STOP 으로만(v1)
        if not self._active_run():
            return "PROCESS_NOT_RUNNING"
        inst, d = self._cur_def()
        if inst is None or d is None or inst.auto or not d.skippable:
            self.reject_detail = f"{inst.type if inst else '-'}: 건너뛸 수 없는 블록"
            self.emit("skip", self.reject_detail, "SKIP_NOT_ALLOWED", level="warn", source=source, by=by)
            return "SKIP_NOT_ALLOWED"
        a = self.actions
        t = inst.type
        if t in ("heater_wait", "heater_hold") and self.heater is not None:
            self.heater.heater_cancel()
        if t == "rga_scan":
            self.runner.rga_abandoned = True
        if t in ("main", "thickness_main"):
            a.oes_stop()
        last = inst.first_step + inst.n_steps - 1
        self.runner.request_skip(last)                     # 지금 기대를 먼저 내린다(취소 결과가 실패로 오지 않게)
        with self.lk:
            ex = self.runner.expect
            if ex is not None:
                ex.cancel("skip")
                self.runner.expect = None
        if t == "base_wait":
            a.ig.cancel_wait("skip")
        if t in ("pressure_ctrl", "pressure_boost"):
            def cancel_wait_pressure() -> None:
                for op in list(a.mfc.runner.ops):
                    if not op.done and getattr(op, "name", "") == "WAIT_PRESSURE":
                        a.mfc.runner.cancel(op, "SKIPPED", "블록 건너뛰기")
            a.mfc.runner.call_in_worker(cancel_wait_pressure, "mfc:skip")
        self.log("Process", f"[SKIP] 블록 {self.block_no()} {t} 건너뛰기 — 정리: {', '.join(d.skip_cleanup)} (출처 {source})")
        self._block_end("skipped")
        self.emit("skip", f"{self.block_no()} {t}", data={"block_no": self.block_no(), "type": t, "source": source,
                                                         "by": by, "cleanup": list(d.skip_cleanup), "run_id": self.run_id})
        return None

    # ═══════════════════════ 일시정지·재개 ═══════════════════════
    def pause(self, source: str = "web", by: Optional[str] = None) -> Optional[str]:
        if self.paused:
            return None
        inst, d = self._cur_def()
        delay_row = self.state_name == "COOLDOWN" and self.cur_inst is None and isinstance(self.cur_block, int)
        if not delay_row:
            if not self._active_run():
                return "PROCESS_NOT_RUNNING"
            if inst is None or not d.pausable:
                self.reject_detail = f"{inst.type if inst else '-'}: 멈출 수 없는 블록"
                return "PAUSE_NOT_ALLOWED"
            if inst.type == "heater_wait":
                self.reject_detail = "히터 SOAK 타이머 정지는 히터 엔진에 아직 없음"
                return "PAUSE_NOT_AVAILABLE"
            self.pause_kind = "timer" if inst.type in _TIMER_BLOCKS else "power_hold"
            self.runner.set_pause(True)
        else:
            self.pause_kind = "timer"
        self.paused = True
        self.log("Process", f"[PAUSE] 일시정지 — {d.pause if d else '카운트다운 정지'} (출처 {source})")
        self.emit("pause", "paused", data={"on": True, "kind": self.pause_kind, "source": source, "by": by,
                                           "block_no": self.block_no()})
        return None

    def resume_pause(self, source: str = "web", by: Optional[str] = None) -> Optional[str]:
        if not self.paused:
            return "NOT_PAUSED"
        self.paused = False
        self.runner.set_pause(False)
        self.log("Process", f"[PAUSE] 재개 (출처 {source})")
        self.emit("pause", "resumed", data={"on": False, "source": source, "by": by, "block_no": self.block_no()})
        return None

    # ═══════════════════════ 시퀀스 편집 ═══════════════════════
    def started_upto(self) -> int:
        if not self.busy or self.run_idx < 0:
            return 0
        run = self.runs[self.run_idx]
        if self.cur_block is None:
            return run.last + 1
        if self.cur_block < 0:
            return run.first
        return self.cur_block + 1

    def edit_sequence(self, op: Dict[str, Any], source: str = "web", by: Optional[str] = None) -> Optional[str]:
        if self.seq is None:
            return "NO_RECIPE"
        if self.runner.is_running and (self.runner.shutdown or self.runner.stop_requested):
            return "PROCESS_NOT_RUNNING"
        with self.lk:
            k = self.started_upto()
            res = self.seq.edit(op, k, self.cfg, self.n, caps=self.caps, origin=source)
            if not res.ok:
                self.reject_detail = "; ".join(res.errors)[:500]
                self.emit("edit", self.reject_detail, res.reason, level="warn", op=dict(op), rev=self.seq.rev,
                          source=source, by=by)
                return res.reason
            self.blocks = copy.deepcopy(self.seq.blocks)
            self.runs = plan_runs(self.blocks, self.rts, self.rows) if self.runs else []
            if self.runner.is_running and 0 <= self.run_idx < len(self.runs):
                run = self.runs[self.run_idx]
                bl = run.blocks(self.blocks)
                steps, insts = BK.expand(bl, self.cfg, self.n, caps=self.caps)
                r = self.runner
                for s, old in zip(steps[:r.idx + 1], r.steps[:r.idx + 1]):
                    s.value, s.params, s.duration = old.value, old.params, old.duration   # 지난 스텝·수정값 유지
                for s in steps[r.idx + 1:]:
                    if s.ref in self.overrides and s.ref != "process.main_remaining":
                        self._set_step_value(s, self.overrides[s.ref])
                r.steps = steps
                self.cur_steps, self.cur_insts = steps, insts
                if self.cur_inst is not None and not self.cur_inst.auto:
                    self.cur_inst = insts[self.cur_inst.index]
                d = derive_params(bl, self.sc)
                self.params.update({x: d[x] for x in USE_KEYS if x in d})
        self.log("Process", f"[EDIT] 레시피 판 {self.seq.rev} — {op.get('op')} (출처 {source})")
        self.emit("edit", f"rev {self.seq.rev}", None, op=dict(op), rev=self.seq.rev, source=source, by=by,
                  blocks=len(self.blocks), record=self.seq.record())
        return None
