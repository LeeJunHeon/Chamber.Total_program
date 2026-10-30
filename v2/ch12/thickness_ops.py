"""챔버12 두께 — ThicknessEngine 의 명령(패널 [영점]·Factor 창·재료 목록 창)과 저널·재시작 이어가기(v2).

v1 근거(읽고 다시 씀): ui/thickness_panel.py:318-357([영점] — 공정 중 경고·확인창 뒤 금지 사유 다시 확인·zero('B', why='패널')),
ui/thickness_factor_dialog.py:252-344([장비에 쓰기]·[저장]), ui/thickness_material_dialog.py:101-163([행 삭제] 확인·[저장]),
controller/thickness_service.py:195-219(zero·set_material 문구). 검사·문구는 thickness_factors·thickness_materials.
확인창(예/아니오)은 화면 몫 — 재료 목록 저장은 Factor 에서 쓰는 재료가 빠지면 confirm 문구를 먼저 돌려주고 confirmed=True 로
다시 부르면 저장한다(v1 은 [행 삭제] 때 물었다 — 같은 문구).
v1 은 Factor 창을 닫으면 진행 중인 [장비에 쓰기] 요청을 취소했다 — v2 는 화면과 장비 쪽이 나뉘어 요청은 끝까지 간다(결과만 버려도 됨).

저널·이어가기(지시 3장 (6) — 두께는 '메인 증착'(THICKNESS_WAIT) 중에만 이어간다)
- state_for_journal(): phase idle / zero / wait, 대기 경과·시간 대체 남은 초, 판정기 상태(t0 기준 상대 시각), 기록(나이)
- resume_checks(state): zero → 이어갈 수 없음(장비 명령 도중). wait → 응답·XTAL·리셋 플래그 없음·최근 값·장비 두께가 줄지 않음
  (영구 시간 대체·판정 없음이면 시간으로만 — 확인 없음)
- resume(state, gap_s, on_end): 꺼져 있던 시간도 경과에 넣는다(상한·시간 대체 마감도 같이 당겨진다)
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Sequence, Tuple

from v2.ch12 import thickness_factors as TF
from v2.ch12 import thickness_materials as TM
from v2.ch12.thickness_judge import ThicknessEndJudge

if TYPE_CHECKING:                                   # 순환 import 없이 형 표기만
    from v2.ch12.thickness import WaitEnd

_LABELS = ("응답 없음", "통신 실패", "체크섬 오류", "프레임 오류", "명령 거부", "값 없음", "되읽기 불일치", "응답 형식 오류")


def _exc_text(detail: str) -> str:
    """드라이버 실패 문구('종류 — 내용', error_text) → v1 이 보이던 예외 문구(str(e)). 종류가 없으면 그대로."""
    d = str(detail or "")
    for lab in _LABELS:
        if d.startswith(lab + " — "):
            return d[len(lab) + 3:]
    return d


class ThicknessOpsMixin:
    """ThicknessEngine 이 상속한다(속성은 엔진 것 — _lock·driver·record·_wait·_judge …)."""

    # ═══════════════════════ 저널·이어가기(v2) ═══════════════════════
    def state_for_journal(self) -> dict:
        """1초 저널용 — 시각은 상대(초). phase: idle / zero(이어갈 수 없음 — 장비 명령 도중) / wait."""
        st = self.status()
        with self._lock:
            now = self.clock.monotonic()
            w, j = self._wait, self._judge
            d: Dict[str, Any] = {"phase": "wait" if w is not None else ("zero" if self._zero is not None else "idle"),
                                 "params": {"dep_rate": self._params.get("dep_rate")}, "zero_info": self._zero_info,
                                 "pre": self._pre, "running": self._running, "main_polling": self._main_polling,
                                 "record": self.record.to_state(now), "last_device_nm": st.get("thickness_nm")}
            if w is not None:
                d["wait"] = {k: w[k] for k in ("target", "expected_s", "limit_s", "message")}
                d["wait"]["elapsed_s"] = now - w["start_t"]
                d["wait"]["fb_left_s"] = None if w["fb_t"] is None else w["fb_t"] - now
                d["judge"] = None if j is None else dict(j.to_state(), t0_age_s=now - j.t0)
            return d

    def resume_checks(self, state: Mapping[str, Any]) -> List[Dict[str, Any]]:
        """이어가기 확인 목록 [{name, ok, detail}] — 하나라도 ok=False 면 엔진은 안전 정지(PROGRAM_RESTART)."""
        phase = state.get("phase")
        if phase == "zero":
            return [{"name": "thickness.phase", "ok": False, "detail": "두께 영점·확인 중(장비 명령 도중) — 이어갈 수 없음"}]
        if phase != "wait":
            return []
        j = state.get("judge")
        if j is None or j.get("permanent"):
            return [{"name": "thickness.time_only", "ok": True, "detail": "판정 없음·영구 시간 대체 — 시간으로만 진행"}]
        st, jp = self.status(), self.params()
        age = st.get("age_s")
        last = state.get("last_device_nm")
        out = [
            {"name": "thickness.link", "ok": bool(st.get("connected") and st.get("link_ok")), "detail": "두께 모니터 응답"},
            {"name": "thickness.crystal", "ok": st.get("crystal_ok") is not False, "detail": "XTAL FAIL 아님"},
            {"name": "thickness.reset", "ok": int(st.get("reset_seen") or 0) == 0, "detail": "전원 리셋 플래그 없음"},
            {"name": "thickness.fresh", "ok": age is not None and age <= jp.ready_fresh_s,
             "detail": f"최근 {jp.ready_fresh_s:g}초 안 정상 두께 값"},
            {"name": "thickness.counter", "ok": last is None or (st.get("thickness_nm") is not None
                                                                  and float(st["thickness_nm"]) >= float(last) - jp.zero_confirm_nm),
             "detail": f"장비 두께가 저널 값({last}) 뒤로 줄지 않음(영점·리셋 없음)"},
        ]
        return out

    def resume(self, state: Mapping[str, Any], gap_s: float, on_end: Callable[[WaitEnd], None]) -> None:
        """저널 상태로 THICKNESS_WAIT 를 이어간다 — 꺼져 있던 시간(gap_s)도 경과에 넣는다(장비는 계속 증착)."""
        st = self.status()
        with self._lock:
            now = self.clock.monotonic()
            gap = max(0.0, float(gap_s))
            self._params = dict(state.get("params") or {})
            self._zero_info, self._pre = state.get("zero_info"), state.get("pre")
            self._running, self._main_polling = bool(state.get("running", True)), bool(state.get("main_polling"))
            self.record.from_state(state.get("record") or {}, now - gap)
            w = state.get("wait")
            if w is None:
                return
            start = now - float(w["elapsed_s"]) - gap
            fb = w.get("fb_left_s")
            self._wait = {"target": float(w["target"]), "expected_s": float(w["expected_s"]),
                          "limit_s": float(w["limit_s"]), "message": w.get("message") or "메인 공정 진행",
                          "on_end": on_end, "start_t": start, "limit_t": start + float(w["limit_s"]),
                          "fb_t": None if fb is None else now + float(fb) - gap}
            js = state.get("judge")
            if js is not None:
                t0 = now - float(js["t0_age_s"]) - gap
                self._judge = self._last_judge = ThicknessEndJudge.from_state(
                    js, t0=t0, seq0=-1, reset0=int(st.get("reset_seen") or 0), log=lambda m: self._log("THICKNESS", m))
                if js.get("permanent"):
                    self._judge = None
            self._log("THICKNESS", f"두께 판정 이어감 — 공백 {gap:.0f}초, 경과 {now - start:.0f}초 / 상한 {float(w['limit_s']):.0f}초")
            self._check_deadlines(now)
        self._flush()

    # ═══════════════════════ 명령(패널·Factor·재료 창) ═══════════════════════
    def cmd_zero(self, process_running: bool, on_done: Callable[[bool, str], None]) -> None:
        """패널 [영점](ui/thickness_panel.py:318-357) — 확인창은 화면 몫, 여기서 다시 금지 사유를 본다."""
        st = self.status()
        if process_running or st.get("process_running"):
            on_done(False, "공정 중에는 영점을 할 수 없습니다.")
            return
        block = TF.device_write_block(st, process_running) if self.driver is not None else "두께 모니터 미연결"
        if block or self._cmd_busy:
            on_done(False, f"{block} — 영점을 보내지 않았습니다." if block else "")
            return
        self._cmd_busy = True

        def done(d) -> None:
            self._cmd_busy = False
            on_done(bool(d.ok), "" if d.ok else f"영점 실패: {_exc_text(d.detail)}")
        self.driver.zero("B", on_done=done, why="패널")

    def factor_window(self) -> dict:
        mats, mw = TM.load_materials(self.materials_path)
        return TF.factor_window(self.factor_path, mats, mw, ch=self.n, materials_path=str(self.materials_path))

    def factor_save(self, tooling_cells, material_texts) -> Tuple[bool, str, List[str]]:
        mats, _w = TM.load_materials(self.materials_path)
        ok, text, bad = TF.save_window(self.factor_path, tooling_cells, material_texts, mats)
        if ok:
            self.reload_factors()
        return ok, text, bad

    def factor_write(self, chuck: str, gun: str, tooling_text, material_text, process_running: bool,
                     on_done: Callable[[dict], None]) -> None:
        """Factor 창 [장비에 쓰기] — 창의 값(저장 무관)을 장비에 바로 쓰고 되읽어 확인. 저장 파일은 건드리지 않는다."""
        mats, _w = TM.load_materials(self.materials_path)
        tooling, mat, errs, bad = TF.write_values(chuck, gun, tooling_text, material_text, mats)
        if errs:
            on_done({"ok": False, "title": "장비에 쓰기", "bad": bad,
                     "text": "고칠 칸이 있어 장비에 쓰지 않았습니다:\n\n" + "\n".join(errs)})
            return
        why = TF.device_write_block(self.status(), process_running) if self.driver is not None else "두께 모니터 미연결"
        if why:
            on_done({"ok": False, "title": "장비에 쓰기", "bad": [], "text": f"{why} — 장비에 쓰지 않았습니다."})
            return

        def done(d) -> None:
            if not d.ok:
                on_done({"ok": False, "title": "장비에 쓰기", "bad": [],
                         "text": f"장비에 쓰지 못했습니다(되읽기 확인 포함): {_exc_text(d.detail)}"})
                return
            g = d.value or {}
            on_done({"ok": True, "title": "장비에 쓰기", "bad": [],
                     "text": f"장비에 적용됨 — {mat.name} · density {float(g.get('density')):g} · "
                             f"Z {float(g.get('z_factor')):.3f} · tooling {float(g.get('tooling')):g}%"})
        self.driver.set_material(float(mat.density), float(mat.z_factor), float(tooling), material=mat.name,
                                 on_done=done, why="Factor 창 수동")

    def materials_window(self) -> dict:
        return TM.materials_window(self.materials_path)

    def materials_save(self, rows: Sequence[Sequence], used_names: Sequence[str] = (),
                       confirmed: bool = False) -> Dict[str, Any]:
        """재료 목록 [저장] — Factor 에서 쓰는 재료가 빠지면 먼저 확인 문구(confirmed=True 로 다시 부르면 저장)."""
        old, _w = TM.load_materials(self.materials_path)
        keep = {str(r[0]).strip().casefold() for r in rows if r and str(r[0] or "").strip()}
        gone = [m.name for m in old if m.name.casefold() not in keep]
        ask = TM.delete_confirm_text(gone, used_names)
        if ask and not confirmed:
            return {"ok": False, "confirm": ask, "text": "", "bad": []}
        ok, text, bad = TM.save_rows(self.materials_path, rows)
        if ok:
            self.reload_factors()
        return {"ok": ok, "confirm": "", "text": text, "bad": bad}
