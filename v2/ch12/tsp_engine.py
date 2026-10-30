"""TSP 운전 엔진 — v1 runtime/tsp_runtime.py(TSPPageController 의 시작·_run·정지) + controller/tsp_controller.py
(TSPProcessController.run) 를 같은 순서·문구·타이밍으로. 장비는 drivers.tsp.TspDriver(on·off·205 확인 0.05 s·1.5 s 창)와
TSP 게이지(system.SharedGauge — D67: CH1 IG worker 공유, 설정은 CH2 값)를 쓴다. 실행은 엔진 스레드 하나(clock.start_thread —
v1 asyncio 태스크 하나와 같은 자리). 기다림은 모두 시계 대기(가상 시계 시험).

시작(start — v1 on_start_clicked, tsp_runtime.py:394-454, 가드 순서 그대로)
  ① 이미 실행 중 → 로그 '이미 실행 중입니다.'(PROCESS_RUNNING) ② CH1 챔버·CH1 PC 실행 중 → 팝업 '실행 오류'/'CH1는 이미 다른
  공정이 실행 중입니다.'(CH1_PROCESS_RUNNING — CH2 공정은 막지 않음) ③ TSP 실행 중(run_state) → '실행 오류'/'다른 공정이 실행
  중입니다.' ④ 쿨다운 max(CH1 챔버·CH1 PC·TSP 종료 뒤 남은 시간, TSP_COOLDOWN_S 60) → '대기 필요'/'이전 공정 종료 후 {cd}초 대기
  필요합니다.\\n{secs}초 후에 시작하십시오.'(COOLDOWN) ⑤ 입력 파싱(target float·cycles int ≥1) → '[ERROR] 입력 파싱 실패: ..'
  (BAD_ARGS). 통과하면 표시 초기화(현재 사이클 '0'·압력 '') → 실행 스레드 → busy. 거절 사유 문구는 결과 message(v1
  _start_block_reason). 팝업은 이벤트 popup(title·text — 화면이 띄운다).
실행(_run — tsp_runtime.py:509-705)
  설정 다시 읽기 → 직전 재확인(②③ — 걸리면 팝업만·카드 없음, ⚠ finally 의 mark_finished 로 TSP 쿨다운 60 s 시작: PC-148, D110)
  → run_state.mark_started('tsp') → 게이지 잡기·TSP begin_use → 로그 파일(outputs.tsp_txt) → '=== TSP 공정 시작 === …' → 시작 카드
  → 컨트롤러(_ctrl_run) 를 전체 상한 cycles×(TSP_ON_SEC+TSP_OFF_SEC)+TSP_TOTAL_TIMEOUT_MARGIN_S 안에서 → 결과 로그·종료 카드.
  finally: mark_finished('tsp')(CH1 챔버·CH1 PC·TSP 쿨다운 시작 — run_state.chamber_block_reason·pc_block_reason 이 본다) →
  TSP OFF → 연결 닫기(end_use) → 로그 기록기 멈춤(경로는 남김 — PC-149) → IG ensure_off → IG cleanup → 게이지 놓기 → 화면 기본값.
컨트롤러(_ctrl_run — tsp_controller.py:108-194)
  상태 prepare·'[TSP] 공정 시작: …' → ig_on·SIG 1·'[IG] ensure_on 완료' → TSP_FIRST_CHECK_DELAY_SEC(5 s) → P0 1회(≤ 목표 →
  initial_below_target) → i=1..N: tsp_on·'[TSP] ON (cycle i/N)'·TSP ON(확인) → TSP_ON_SEC(승화 120 s) 동안 TSP_POLL_SEC(10 s)
  간격 RDI(≤ 목표 → reached_target_on) → tsp_off·'[TSP] OFF (cycle i/N)'·TSP OFF → TSP_OFF_SEC(사이클 간격 150 s) 동안
  폴링(reached_target_off) → cycles_done=i·'[TSP] cycle i/N 완료' → 다 돌면 cycles_exhausted. 예외 '[TSP] 예외: {e!r}'·
  exception:{e!r}. finally: cleanup·TSP OFF·IG OFF·finished(TSP OFF·SIG 0 은 runtime finally 에서 한 번 더 — v1 그대로).
결과·카드(tsp_runtime.py:598-666 — 카드는 이벤트 card, 포털이 보낸다)
  성공 → normal_completed '정상 종료' ok; cycles_exhausted → cycles_completed '목표 사이클 완료' ok(오류 아님 — ⚠ v1 카드는
  status_label 을 쓰지 않아 '정상 종료'로 보인다: D175·CHAT-047, 받는 쪽이 v1 그대로 그림); 그 밖 → error '오류로 공정 종료'
  errors=[reason]. 사용자 STOP → '[사용자 중단]'·카드 stopped. 예외 → '[ERROR] 실행 실패: {e!r}'·카드 errors=[str(e)].
STOP·전체 상한(PC-150 — v1 은 Python 버전마다 다름. v2 는 3.11 동작으로 정함 — 근거: 장비 노트북 대상 Python 3.11(plan.py 2장
  8), 같은 구조를 3.11·3.12·3.13 에서 실제로 돌려 확인(D112)): 둘 다 컨트롤러가 취소를 결과 'cancelled' 로 삼키고('[TSP] run()
  취소됨' → cleanup·OFF·finished) — STOP 은 그 뒤 '[사용자 중단]'·stopped 카드(3.11), 상한 초과는 'TSP run timed out' 이 아니라
  '=== RESULT === ok=False … reason=cancelled'·'오류로 공정 종료' 카드(errors ['cancelled'], 모든 버전 같음).
로그(outputs.tsp_txt): '[TSP]' 접두어 중복 그대로('[TSP] [TSP] ON …' — PC-151, D113). 이벤트 log 로도 낸다.
오류 코드(D17): TSP on/off 실패의 v1 코드 E804~E808 을 결과·이벤트 code 에 싣는다(로그 문구는 v1 예외 repr 그대로 —
  TSPError('상태 확인 시간초과(target=on, last=2)') 등. 연결 실패의 v1 원문(OSError 계열)은 미확인 — ConnectionError(문구)).
나눔: 사이클(컨트롤러)·기다림·v1 예외 모양·단계 표는 tsp_cycle(TspCycle — 섞어 씀), 로그 파일은 outputs/tsp_txt(TspTxtLog).
v2 에서 더한 것: abort(reason)(안전 규칙 — 결과는 '[ERROR] 실행 실패: RuntimeError('abort: …')' 와 같은 예외 경로), 저널
  (state_for_journal — TSP 는 이어가기 안 함: 어느 단계도 상태 유지 블록이 아니다, PHASES), 1초 행 tsp.proc.*·tsp.set.base_torr.
  판정은 v1 주기의 자기 RDI 로(1초 행 값은 판정에 쓰지 않는다). 매일 예약(PC-143~146)은 tsp_schedule(engine.schedule).
"""
from __future__ import annotations

import collections
import math
import threading
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from v2.ch12.outputs.tsp_txt import TspTxtLog
from v2.ch12.tsp_cycle import (CLOSE_WAIT_S, OWNER, OWNERS, PHASES, TspCycle, TspEngineEvent, TspRunResult, _UserStop, _txt,
                               parse_cycles, parse_target)


class TspEngine(TspCycle):
    def __init__(self, clock, cfg: Dict[str, Any], tsp, gauge, run_state, *,
                 on_event: Optional[Callable[[TspEngineEvent], None]] = None, writer=None,
                 now: Optional[Callable[[], datetime]] = None) -> None:
        self.clock, self.cfg, self.tsp, self.gauge, self.rs = clock, cfg, tsp, gauge, run_state
        self.on_event = on_event
        self.events: "collections.deque" = collections.deque(maxlen=20000)
        self.now = now or (lambda: datetime.fromtimestamp(clock.wall()))
        self.txt = TspTxtLog(cfg, writer=writer, now=self.now)
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._running = False             # v1 _busy(실행 태스크 있음 — finally 첫머리에서 풀림)
        self._cancel: Optional[tuple] = None   # ("stop"|"timeout"|"abort", 사유)
        self._deadline: Optional[float] = None
        self._in_ctrl = False
        self.phase = "idle"
        self.phase_t0 = clock.monotonic()
        self.phase_end: Optional[float] = None
        self.run_id: Optional[str] = None
        self.owner: Optional[str] = None
        self.by: Optional[str] = None
        self.target: Optional[float] = None
        self.cycles: Optional[int] = None
        self.cycle = 0
        self.cycles_done = 0
        self.last_pressure = math.nan
        self.started_t: Optional[float] = None
        self.start_block_reason: Optional[str] = None
        self.last_result: Optional[Dict[str, Any]] = None
        self.log_file: Optional[str] = None
        self.defaults = {"target": str(cfg.get("TSP_UI_DEFAULT_TARGET", "2.5e-07")),
                         "cycles": str(cfg.get("TSP_UI_DEFAULT_CYCLES", 10))}
        self.inputs = dict(self.defaults)
        self.display = {"now_cycle": "0", "base_pressure": ""}
        self._s: Dict[str, Any] = {}
        self._last_code: Optional[str] = None
        self.started_ctrl: Optional[float] = None
        self.schedule = None                 # tsp_schedule.TspSchedule(매일 예약 — attach 가 붙인다)
        self._refresh_from_config()

    # ═══════════════════════ 설정(시작·실행마다 다시 읽음 — tsp_runtime.py:138-160) ═══════════════════════
    def _refresh_from_config(self) -> None:
        c = self.cfg
        self._s = {"host": str(c.get("TSP_TCP_HOST", "192.168.1.50")), "tsp_port": int(c.get("TSP_TCP_PORT", 4004)),
                   "ig_port": int(c.get("TSP_IG_TCP_PORT", c.get("CH1_IG_TCP_PORT", 4001))),
                   "on": float(c.get("TSP_ON_SEC", 120.0)), "off": float(c.get("TSP_OFF_SEC", 150.0)),
                   "poll": float(c.get("TSP_POLL_SEC", 10.0)), "first": float(c.get("TSP_FIRST_CHECK_DELAY_SEC", 5.0)),
                   "verify": bool(c.get("TSP_VERIFY_WITH_STATUS", True)), "cooldown": float(c.get("TSP_COOLDOWN_S", 60.0)),
                   "margin": float(c.get("TSP_TOTAL_TIMEOUT_MARGIN_S", 300.0))}

    # ═══════════════════════ 내보내기 ═══════════════════════
    def _emit(self, ev: TspEngineEvent) -> None:
        self.events.append(ev)
        if self.on_event is not None:
            try:
                self.on_event(ev)
            except Exception:
                pass

    def _log(self, msg: str) -> None:
        ui = self.txt.log(msg)
        self._emit(TspEngineEvent("log", str(msg), data={"source": "TSP", "ui": ui}))

    def _popup(self, title: str, text: str) -> None:
        """v1 _post_warning(5 s 자동 닫힘 경고창) — 화면이 띄운다."""
        self._emit(TspEngineEvent("popup", text, level="warn", data={"title": title, "text": text, "auto_close_ms": 5000}))

    def _card(self, name: str, data: Dict[str, Any], level: str = "info") -> None:
        self._emit(TspEngineEvent("card", name, level=level, code=data.get("code"), data=dict(data, card=name)))

    def _state(self, s: str) -> None:
        """v1 state_cb — '[STATE] {s}'."""
        self._set_phase(s)
        self._log(f"[STATE] {s}")

    def _set_phase(self, s: str, end: Optional[float] = None) -> None:
        with self._lock:
            self.phase, self.phase_t0, self.phase_end = s, self.clock.monotonic(), end

    def _pressure(self, p: float) -> None:
        """v1 pressure_cb — '[IG] P=..' + 압력 표시."""
        self.last_pressure = p
        self._log(f"[IG] P={p:.3e}")
        self.display["base_pressure"] = f"{p:.3e}"

    def _cycle_done(self, cur: int, total: int) -> None:
        self._log(f"[TSP] cycle {cur}/{total} 완료")
        self.display["now_cycle"] = str(cur)

    # ═══════════════════════ 명령 API ═══════════════════════
    @property
    def busy(self) -> bool:
        """실행 스레드가 끝날 때까지(정리 포함) True — 설정 적용·프로그램 종료 판단용."""
        t = self._thread
        return bool(self._running or (t is not None and t.is_alive()))

    def _reject(self, reason: str, block: str) -> Dict[str, Any]:
        self.start_block_reason = block
        return {"ok": False, "reason": reason, "message": block}

    def start(self, params: Optional[Dict[str, Any]] = None, source: str = "local", by: Optional[str] = None) -> Dict[str, Any]:
        """TSP 시작(v1 on_start_clicked). params = {"target": 목표 Torr(글자 또는 수), "cycles": 반복 횟수}.
        → {"ok", "reason"(PROCESS_RUNNING·CH1_PROCESS_RUNNING·COOLDOWN·BAD_ARGS), "message"(v1 _start_block_reason), "run_id"}."""
        p = dict(params or {})
        with self._lock:
            self.start_block_reason = None
            claimed = not self.busy
            if claimed:
                self._running = True                   # 자리 잡기(같은 순간 두 번 시작 방지) — 거절이면 아래에서 푼다
        if not claimed:
            self._log("이미 실행 중입니다.")
            return self._reject("PROCESS_RUNNING", "TSP 공정이 이미 실행 중")
        try:
            rs = self.rs
            if rs.is_running("chamber", 1) or rs.is_running("pc", 1):
                self._popup("실행 오류", "CH1는 이미 다른 공정이 실행 중입니다.")
                return self._unclaim(self._reject("CH1_PROCESS_RUNNING", "CH1에서 다른 공정(Chamber/PC) 실행 중"))
            if rs.is_running("tsp"):
                self._popup("실행 오류", "다른 공정이 실행 중입니다.")
                return self._unclaim(self._reject("PROCESS_RUNNING", "다른 TSP 공정 실행 중"))
            self._refresh_from_config()
            cd = float(self._s["cooldown"])
            remain = max(rs.remaining_cooldown("chamber", 1, cooldown_s=cd), rs.remaining_cooldown("pc", 1, cooldown_s=cd),
                         rs.remaining_cooldown("tsp", 0, cooldown_s=cd))
            if remain > 0.0:
                secs, cd_s = int(remain + 0.999), int(float(cd) + 0.5)
                self._popup("대기 필요", f"이전 공정 종료 후 {cd_s}초 대기 필요합니다.\n{secs}초 후에 시작하십시오.")
                return self._unclaim(self._reject("COOLDOWN", f"이전 공정 종료 후 쿨다운 {cd_s}초 미경과(잔여 {secs}초)"))
            if "target" in p:
                self.inputs["target"] = _txt(p.get("target"))
            if "cycles" in p:
                self.inputs["cycles"] = _txt(p.get("cycles"))
            try:
                target = parse_target(self.inputs["target"])
                cycles = parse_cycles(self.inputs["cycles"])
            except Exception as e:
                self._log(f"[ERROR] 입력 파싱 실패: {e}")
                return self._unclaim(self._reject("BAD_ARGS", f"입력값 파싱 실패({e})"))
            self.display = {"now_cycle": "0", "base_pressure": ""}
            with self._lock:
                self.run_id = uuid.uuid4().hex[:12]
                self.owner = source if source in OWNERS else None
                self.by = by
                self.target, self.cycles, self.cycle, self.cycles_done = target, cycles, 0, 0
                self.last_pressure = math.nan
                self._cancel, self._deadline, self.last_result = None, None, None
                self.started_t = self.clock.monotonic()
            self._thread = self.clock.start_thread(lambda: self._run(target, cycles), "tsp-run")
            return {"ok": True, "reason": None, "message": "", "run_id": self.run_id}
        except BaseException:
            self._unclaim(None)
            raise

    def _unclaim(self, res):
        with self._lock:
            self._running = False
        return res

    def stop(self, source: str = "local") -> Dict[str, Any]:
        """STOP(v1 on_stop_clicked — '[TSP] 중단 요청'·태스크 취소). 예약은 건드리지 않는다."""
        t = self._thread
        if t is None or not t.is_alive() or not self._running:
            return {"ok": False, "reason": "PROCESS_NOT_RUNNING", "message": "실행 중 아님"}
        self._log("[TSP] 중단 요청")
        self._request_cancel("stop", f"USER_STOP({source})")
        return {"ok": True, "reason": None, "message": ""}

    def abort(self, reason: str) -> bool:
        """안전 규칙·ALL STOP 이 부르는 이상 중단(v2) — 컨트롤러는 STOP 과 같이 취소를 삼키고 정리, 결과는 예외 경로
        ('[ERROR] 실행 실패: RuntimeError('abort: {reason}')'·오류 카드). 실행 중이 아니면 False."""
        t = self._thread
        if t is None or not t.is_alive() or not self._running:
            return False
        self._log(f"[TSP] 이상 중단 요청: {reason}")
        self._request_cancel("abort", str(reason))
        return True

    def _request_cancel(self, kind: str, why: str) -> None:
        with self.clock.cond:
            if self._cancel is None:
                self._cancel = (kind, why)
            self.clock.cond.notify_all()

    def close(self) -> None:
        """조립 정지(system.stop) — 실행 중이면 이상 중단(PROGRAM_EXIT)하고 정리가 끝나기를 기다린다."""
        t = self._thread
        if t is not None and t.is_alive():
            if self._running:
                self._request_cancel("abort", "PROGRAM_EXIT")
            try:
                self.clock.wait_until(self.clock.monotonic() + CLOSE_WAIT_S, lambda: not t.is_alive())
            except RuntimeError:
                t.join(5.0)
        if self.schedule is not None:
            self.schedule.close()
        self.txt.stop_writer()

    def state(self) -> Dict[str, Any]:
        now = self.clock.monotonic()
        with self._lock:
            return {"phase": self.phase, "busy": self.busy, "running": self._running, "run_id": self.run_id,
                    "owner": self.owner, "by": self.by, "target": self.target, "cycles": self.cycles, "cycle": self.cycle,
                    "cycles_done": self.cycles_done, "inputs": dict(self.inputs), "display": dict(self.display),
                    "last_pressure": None if math.isnan(self.last_pressure) else self.last_pressure,
                    "remain_s": None if self.phase_end is None else max(0.0, self.phase_end - now),
                    "deadline_left_s": None if self._deadline is None else max(0.0, self._deadline - now),
                    "start_block_reason": self.start_block_reason, "result": self.last_result,
                    "log_file": self.log_file, "gauge_owner": getattr(self.gauge, "owner", None),
                    "schedule": None if self.schedule is None else self.schedule.state()}

    # ═══════════════════════ 1초 행·조립 훅 ═══════════════════════
    def on_tick_start(self, tick=None) -> None:
        """할 일 없음 — TSP 판정은 v1 주기(TSP_POLL_SEC)의 자기 RDI 로 한다."""

    def on_row(self, view=None) -> None:
        """할 일 없음(1초 행 값은 판정에 쓰지 않는다 — v1 과 같은 IG 읽기 횟수·간격)."""

    def snapshot_fields(self) -> Dict[str, Any]:
        now = self.clock.monotonic()
        with self._lock:
            if not self.busy:
                return {"tsp.proc.state": "idle"}
            ph = self.phase
            n = self.cycles or 0
            label = PHASES.get(ph, {}).get("label", ph)
            if ph in ("tsp_on", "tsp_off") and self.cycle:
                label = f"TSP {'ON' if ph == 'tsp_on' else 'OFF'} (cycle {self.cycle}/{n})"
            return {"tsp.proc.state": ph, "tsp.proc.run_id": self.run_id, "tsp.proc.owner": self.owner,
                    "tsp.proc.block_no": self.cycle or None, "tsp.proc.block_count": n or None,
                    "tsp.proc.block_type": ph, "tsp.proc.block_label": label,
                    "tsp.proc.block_elapsed_s": round(now - self.phase_t0, 1),
                    "tsp.proc.remain_s": None if self.phase_end is None else round(max(0.0, self.phase_end - now), 1),
                    "tsp.proc.paused": False, "tsp.set.base_torr": self.target}

    # ═══════════════════════ 저널·이어가기(5단계) ═══════════════════════
    def state_for_journal(self) -> Dict[str, Any]:
        st = self.state()
        return {"kind": "tsp", "busy": st["busy"], "phase": st["phase"], "run_id": st["run_id"], "owner": st["owner"],
                "target": st["target"], "cycles": st["cycles"], "cycle": st["cycle"], "cycles_done": st["cycles_done"],
                "elapsed_s": None if self.started_t is None or not st["busy"] else self.clock.monotonic() - self.started_t,
                "resumable": bool(PHASES.get(st["phase"], {}).get("resumable", False))}

    def resume_checks(self, state: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """이어가기 확인 [{name, ok, detail}] — TSP 운전 중이었으면 늘 ok=False(안전 정지: TSP OFF·IG OFF, PROGRAM_RESTART).
        TSP 의 어느 단계(IG 켜기·TSP ON/OFF·사이클 대기)도 상태 유지 블록이 아니다(PHASES — 정의)."""
        st = state or {}
        if not st.get("busy"):
            return []
        ph = str(st.get("phase") or "")
        return [{"name": "tsp.phase", "ok": bool(PHASES.get(ph, {}).get("resumable", False)),
                 "detail": f"TSP {PHASES.get(ph, {}).get('label', ph)} — 상태 유지 블록이 아님(이어가지 않음)"}]

    # ═══════════════════════ 실행(tsp_runtime.py:509-705) ═══════════════════════
    def _run(self, target: float, cycles: int) -> None:
        self._refresh_from_config()
        s = self._s
        self._last_code = None
        self.started_ctrl = None
        used_tsp = held_ig = False
        try:
            rs = self.rs
            if rs.is_running("chamber", 1) or rs.is_running("pc", 1):
                self._popup("실행 오류", "CH1는 이미 다른 공정이 실행 중입니다.")
                self._finish_record("blocked", False, "CH1_PROCESS_RUNNING")
                return
            if rs.is_running("tsp"):
                self._popup("실행 오류", "다른 공정이 실행 중입니다.")
                self._finish_record("blocked", False, "PROCESS_RUNNING")
                return
            rs.mark_started("tsp")
            ok, who = self.gauge.acquire(OWNER)
            if not ok:
                raise RuntimeError(f"TSP 게이지 사용 중({who})")
            held_ig = True
            self.tsp.begin_use()
            used_tsp = True
            path = self.txt.open_run(self._log)
            self.log_file = str(path)
            self._log("=== TSP 공정 시작 === "
                      f"host={s['host']} ig={s['ig_port']} tsp={s['tsp_port']} target={target} cycles={cycles} "
                      f"on={s['on']}s off={s['off']}s poll={s['poll']}s first_wait={s['first']}s")
            self._card("started", {"params": {"process_note": f"TSP run: target={target:.3e}, cycles={cycles}",
                                              "tsp_target": target, "tsp_cycles": cycles}, "run_id": self.run_id})
            total = cycles * (s["on"] + s["off"]) + float(s["margin"])
            self.started_ctrl = self.clock.monotonic()
            self._deadline = self.started_ctrl + total
            result = self._ctrl_run(target, cycles)
            kind = (self._cancel or (None, None))[0]
            if kind == "stop":
                raise _UserStop()
            if kind == "abort":
                raise RuntimeError(f"abort: {self._cancel[1]}")
            fp = result.final_pressure
            if fp == fp:
                self.display["base_pressure"] = f"{fp:.3e}"
            self._log(f"=== RESULT === ok={result.success} final_P={(f'{fp:.3e}' if fp == fp else 'NaN')} "
                      f"cycles_done={result.cycles_done} reason={result.reason}")
            self._result_card(result)
        except _UserStop:
            self._log("[사용자 중단]")
            self._card("finished", {"ok": False, "detail": {"process_name": "TSP", "stopped": True}, "run_id": self.run_id})
            self._finish_record("stopped", False, "USER_STOP")
        except Exception as e:
            self._log(f"[ERROR] 실행 실패: {e!r}")
            self._card("finished", {"ok": False, "detail": {"process_name": "TSP", "errors": [str(e)]}, "run_id": self.run_id,
                                    "code": self._last_code}, level="error")
            self._finish_record("error", False, str(e), code=self._last_code)
        finally:
            self._closing(used_tsp, held_ig)

    def _result_card(self, r: TspRunResult) -> None:
        reason_str = (r.reason or "").lower() if isinstance(r.reason, str) else str(r.reason)
        if r.success:
            status, label, ok = "normal_completed", "정상 종료", True
        elif "cycles_exhausted" in reason_str:
            status, label, ok = "cycles_completed", "목표 사이클 완료", True     # 오류 아님(카드 그림은 D175)
        else:
            status, label, ok = "error", "오류로 공정 종료", False
        fp = r.final_pressure
        detail: Dict[str, Any] = {"process_name": "TSP", "status": status, "status_label": label,
                                  "final_pressure": fp if fp == fp else None,     # NaN 은 JSON 에 못 실어 None(v1 NaN)
                                  "cycles_done": r.cycles_done, "reason": r.reason}
        code = r.code or (self._last_code if not ok else None)
        if not ok:
            detail["errors"] = [str(r.reason)] if r.reason else ["unknown_error"]
        self._card("finished", {"ok": ok, "detail": detail, "run_id": self.run_id, "code": code},
                   level="info" if ok else "error")
        self._finish_record(status, ok, r.reason, cycles_done=r.cycles_done, final_pressure=detail["final_pressure"],
                            code=code)

    def _finish_record(self, outcome: str, ok: bool, reason: Optional[str], **kw) -> None:
        rec = {"outcome": outcome, "ok": ok, "reason": reason, "run_id": self.run_id,
               "cycles_done": kw.get("cycles_done", self.cycles_done), "final_pressure": kw.get("final_pressure"),
               "code": kw.get("code")}
        self.last_result = rec
        self._emit(TspEngineEvent("result", f"{outcome}: {reason}", level="info" if ok else "warn", code=rec["code"],
                                  reason=None if ok else outcome.upper(), data=rec))

    def _closing(self, used_tsp: bool, held_ig: bool) -> None:
        """v1 finally(tsp_runtime.py:668-705)."""
        self._in_ctrl = False
        try:
            self.rs.mark_finished("tsp")
        except Exception:
            pass
        with self._lock:
            self._running = False
        self._set_phase("closing")
        if used_tsp:
            self._quiet(lambda: self._tsp_switch(False))
            self.tsp.end_use(close=False)
            self._quiet(lambda: self._call(lambda cb: self.tsp.close(on_done=cb), False))
        self.txt.close_run()
        if held_ig:
            self._quiet(lambda: self._ig_off())
            self._quiet(lambda: self._ig_off(cleanup=True))
            try:
                self.gauge.release(OWNER)
            except Exception:
                pass
        self.inputs = dict(self.defaults)
        self.display = {"now_cycle": "0", "base_pressure": ""}
        with self._lock:
            self.cycle = 0
            self._deadline = None
        self._set_phase("idle")


def attach(system, run_state, name: str = "tsp", settings_dir=None, schedule: bool = True) -> TspEngine:
    """조립(Ch12System)에 TSP 엔진을 꽂는다 — 이벤트는 조립 이벤트 길(출처 tsp_engine), 로그 파일은 v1 기록 출력 쓰기 스레드.
    schedule=True 면 매일 예약(tsp_schedule)을 붙이고 바로 복원한다(v1 은 TSP 페이지 생성 때 예약 등록) — 저장 폴더는
    settings_dir, 없으면 조립 설정 저장소 폴더(system.store.dir)."""
    from v2.ch12.tsp_schedule import TspSchedule
    eng = TspEngine(system.clock, system.cfg, system.tsp, system.shared.tsp_ig, run_state,
                    on_event=system._ev_for("tsp_engine"), writer=system.outputs.writer)
    system.register_engine(name, eng)
    if schedule:
        store = getattr(system, "store", None)
        d = settings_dir if settings_dir is not None else (store.dir if store is not None else None)
        eng.schedule = TspSchedule(eng, d)
        msg = eng.schedule.restore()
        if msg:
            system.emit("tsp_engine", "schedule_restore", msg)
    return eng
