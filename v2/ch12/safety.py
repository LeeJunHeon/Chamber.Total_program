"""챔버12 안전 규칙 실행 — safety_rules.py 표 중 안전 엔진 몫(S30 ALL STOP·S31 시작 때 남은 파워 정리·S32 의도적 종료 파워만)을
드라이버·엔진 호출로 잇는다. 나머지 규칙(v1 공정 중단 조건·시작 가드·정지 순서)은 엔진·드라이버가 v1 처럼 이미 처리하고, 표의 impl
(IMPLEMENTED_IN)에 위치를 적었다 — 여기서 같은 판정을 한 번 더 하지 않는다(판정이 두 곳에 생기지 않게, 새 규칙 금지).

연결(assembly.build 가 붙인다): system.safety = Ch12Safety — Ch12System 이 1초 행마다 on_row(RowView)(기록 다음·엔진 앞)를 부른다.
이벤트: SafetyEvent(kind fired·released·step·info, rule S번호, code, detail, level) → system.emit("safety", …, event=…) — 허브가
  ruleId 를 실어 웹·로컬 화면에(fired/released 는 alarms 에).

S30 ALL STOP(결정 D05): CH1 STOP + CH2 STOP + 플라즈마 클리닝 STOP + TSP STOP 을 한꺼번에 — 각 엔진의 v1 정지 순서 그대로(보내기만,
  서로 기다리지 않음). 넷 다 다시 idle 이 되면 released. 진행 중이면 stopping 에 보인다.
S31 시작 때 남은 파워(지시 3장 (6), 챔버K S51): 프로그램이 켜진 뒤 첫 정상 PLC 행(재연결 때는 다시 하지 않음)에서 공정·운전이 없는데
  PLC D/A 전원(CH2 DC1·DC2, CH2 RF 연속, PC RF)의 SET 래치가 켜져 있거나 WRITE 값이 0 이 아니면 그 전원만 끈다(드라이버 정지 경로 —
  0 W 확인 → SET OFF, RF 는 v1 STOP 과 같은 램프다운). 펌프·밸브·셔터·가스는 건드리지 않는다. 공정 중이면 알림만. bench 는 쓰지 않는다.
  DC Pulse·RF Pulse 는 포트를 공정이 고를 때만 열어(v1 GAPB-001) 시작 때 읽지 않는다 — 확인 못 함('사용자 확인 필요' 후보).
  히터 남은 RUN 은 히터 엔진 첫 폴링(S21)이 정리한다.
S32 의도적 종료(챔버K 결정 4): power_off_only(reason) — 챔버 공정을 멈추고(새 장비 명령을 보내지 않게 붙잡음 — 정지 순서를 돌리지 않음:
  돌리면 MV·가스 밸브를 닫는다, run_end error PROGRAM_EXIT), 확인을 기다리지 않는 OFF 를 먼저 다 보낸 뒤 deadline_s 까지 결과를 기다린다:
  DC1·DC2 OFF · RF(CH2·PC) 0 W → SET OFF(램프다운 없음) · 연결된 DC Pulse OUTPUT OFF · RF Pulse RF OFF · 히터 RUN=0 ·
  TSP 운전 중이면 이상 중단(TSP OFF). 플라즈마 클리닝이 돌고 있으면 챔버 공정처럼 먼저 붙잡고(PC 엔진 program_exit — 끝 정리의
  RF 안전 정지·가스 FLOW OFF·SP4 VALVE OPEN 을 보내지 않음, 결과 실패 PROGRAM_EXIT) PC RF 도 다른 RF 처럼 0 W → SET OFF
  (rf_zero_off — 붙잡은 뒤라 PC 램프다운 쓰기와 엇갈리지 않음). PLC 릴레이·MFC·IG 는 그대로('MFC 그대로' — 챔버K 결정 4).
"""
from __future__ import annotations

import collections
import threading
from typing import Any, Callable, Dict, List, Optional

from v2.common.safety_engine import SafetyEvent
from v2.ch12 import process_states as PS

FRESH = ("ok", "late")
FORCE_DEADLINE_S = 4.0            # 의도적 종료 — 콘솔 창 닫기는 약 5 s 안에 끝나야 한다(챔버K 와 같은 여유)
DC_UNITS = ((1, "ch2.dc.dc1_enable", "plc.dcv_write_0"), (2, "ch2.dc.dc2_enable", "plc.dcv_write_3"))
RF_UNITS = (("ch2", "ch2.rf.enable", "plc.dcv_write_2", "ch2.rf.fwd_w"), ("pc", "pc.rf.enable", "plc.dcv_write_1", "pc.rf.fwd_w"))


def rf_zero_off(drv, on_done: Callable[[Optional[str]], None]) -> None:
    """RF 연속(PLC D/A) 즉시 끄기 — 램프다운 없이 0 W 확인 → SET OFF(plc_power.ZeroThenOff, RfPower 의 끝 단계 그대로).
    의도적 종료에서만(창 닫기 5 s 안). 드라이버 안 상태(동작·폴링·램프)를 먼저 멈춰 뒤에 다른 쓰기가 끼지 않게 한다."""
    drv.running = False
    drv._gen += 1
    drv.polling, drv._due = False, None
    drv.enabled = True                       # 이번 런에 안 켰어도 SET OFF 까지(남은 래치)
    drv.ramping_down = True
    drv._stop_done.append(on_done)
    drv._finish_power_off(lambda ok: drv._end(ok, msg="RF 파워 즉시 OFF(프로그램 종료)", idle=True))


class Ch12Safety:
    def __init__(self, system, proc: Dict[str, Any], pc=None, tsp=None, heater=None) -> None:
        self.sys, self.clock, self.cfg = system, system.clock, system.cfg
        self.proc = dict(proc)                    # {"ch1": ProcessEngine, "ch2": …}
        self.pc, self.tsp, self.heater = pc, tsp, heater
        self.bench = not bool(system.policy.device_writes)
        self.events: "collections.deque[SafetyEvent]" = collections.deque(maxlen=5000)
        self.first_done = False
        self.residual: List[tuple] = []           # S31 조치 기록 (장비, 조치) — 실행당 한 번뿐
        self.all_stop_active = False
        self.all_stop_t: Optional[float] = None
        self.all_stops = 0
        self.forced: Optional[dict] = None
        self._forced_lk = threading.Lock()
        self._held: set = set()

    # ───────── 이벤트 ─────────
    def _emit(self, kind: str, rule: str, code: str, detail: str, level: str = "info") -> SafetyEvent:
        e = SafetyEvent(kind, rule, code, detail, level, self.clock.wall())
        self.events.append(e)
        try:
            self.sys.emit("safety", kind, detail, level=level, code=code, event=e)
        except Exception:
            pass
        return e

    # ───────── 상태 ─────────
    def engine_busy(self) -> Dict[str, bool]:
        out = {ch: bool(e.busy) for ch, e in self.proc.items() if e is not None}
        if self.pc is not None:
            out["pc"] = bool(self.pc.busy)
        if self.tsp is not None:
            out["tsp"] = bool(self.tsp.busy)
        return out

    def any_busy(self) -> bool:
        return any(self.engine_busy().values())

    @property
    def stopping(self) -> Optional[dict]:
        """지금 정지 순서 중인 것(허브 state.stopping) — 없으면 None."""
        out: Dict[str, Any] = {}
        for ch, e in self.proc.items():
            if e is not None and PS.chamber_state(e) == "STOPPING":
                out[ch] = "STOPPING"
        if self.pc is not None and getattr(self.pc, "phase", "") == "stopping":
            out["pc"] = "stopping"
        if self.tsp is not None and getattr(self.tsp, "phase", "") in ("cleanup", "closing"):
            out["tsp"] = self.tsp.phase
        if self.all_stop_active:
            out["allStop"] = True
        return out or None

    # ───────── S30 ALL STOP ─────────
    def all_stop(self, source: str = "web", by: Optional[str] = None) -> List[str]:
        """넷을 한꺼번에 STOP(보내기만). 보낸 대상 목록."""
        self.all_stops += 1
        self.all_stop_active = True
        self.all_stop_t = self.clock.monotonic()
        self._emit("fired", "S30", "ALL_STOP", f"ALL STOP({source}{'·' + by if by else ''}) — CH1·CH2·PC·TSP STOP", "warn")
        sent: List[str] = []
        for ch in ("ch1", "ch2"):
            e = self.proc.get(ch)
            if e is not None:
                self._safe(lambda e=e: e.stop(source=source, by=by, user=True))
                sent.append(ch)
        if self.pc is not None:
            self._safe(lambda: self.pc.stop(source=source))
            sent.append("pc")
        if self.tsp is not None:
            self._safe(lambda: self.tsp.stop(source=source))
            sent.append("tsp")
        self._emit("step", "S30", "ALL_STOP", "보냄: " + ", ".join(sent))
        return sent

    def _safe(self, fn) -> Any:
        try:
            return fn()
        except Exception as e:                    # 하나의 실패가 나머지 STOP 을 막지 않는다
            self._emit("error", "S30", "INTERNAL_ERROR", repr(e), "error")
            return None

    def _all_stop_tick(self) -> None:
        if self.all_stop_active and not self.any_busy():
            self.all_stop_active = False
            self._emit("released", "S30", "ALL_STOP", "ALL STOP 끝 — 모두 대기")

    # ───────── 1초 행 ─────────
    def on_row(self, view) -> None:
        if not self.first_done and view.q("plc") in FRESH:
            self.first_done = True
            self.first_read(view)
        self._all_stop_tick()

    # ───────── S31 시작 때 남은 파워 ─────────
    def residual_found(self, view) -> List[tuple]:
        """[(장비 이름, 드라이버, 종류 dc|rf, 값 설명, 측정 W)] — PLC 행으로 본 남은 D/A 전원."""
        out = []
        dc = getattr(self.sys, "dc", {}) or {}
        for unit, en_col, wr_col in DC_UNITS:
            d = dc.get(unit)
            if d is None:
                continue
            en, wr = view.get(en_col), view.get(wr_col) or 0
            if en or wr:
                out.append((f"DC{unit}", d, "dc", f"SET={bool(en)} WRITE={wr}", None))
        rf = getattr(self.sys, "rf", {}) or {}
        for mode, en_col, wr_col, fwd_col in RF_UNITS:
            d = rf.get(mode)
            if d is None:
                continue
            en, wr = view.get(en_col), view.get(wr_col) or 0
            if en or wr:
                out.append(("RF(CH2)" if mode == "ch2" else "RF(PC)", d, "rf", f"SET={bool(en)} WRITE={wr}", view.get(fwd_col)))
        return out

    def first_read(self, view) -> None:
        found = self.residual_found(view)
        if not found:
            return
        busy = [k for k, v in self.engine_busy().items() if v]
        for name, drv, kind, txt, fwd in found:
            if busy:
                self._emit("fired", "S31", "RESIDUAL_POWER_KEPT", f"{name} {txt} — 공정·운전 중({','.join(busy)})이라 끄지 않음",
                           "warn")
                continue
            if self.bench:
                self._emit("fired", "S31", "RESIDUAL_POWER_OFF", f"{name} {txt} — bench(읽기 전용)라 보내지 않음", "info")
                continue
            self._emit("fired", "S31", "RESIDUAL_POWER_OFF", f"시작 때 {name} 출력이 남아 있음({txt}) — 파워만 끔", "warn")
            self.residual.append((name, kind))
            done = (lambda r, n=name: self._emit("released", "S31", "RESIDUAL_POWER_OFF",
                                                  f"{n} 꺼짐" if r is None else f"{n} 끄기 확인 실패({r})",
                                                  "info" if r is None else "error"))
            try:
                drv.enabled = True                    # 이번 실행에 켜지 않았어도 0 W → SET OFF 까지
                if kind == "rf":
                    drv.last_sent = max(0.0, float(fwd or 0.0))   # v1 STOP 과 같은 램프다운(측정 W 부터)
                drv.stop(on_done=done)
            except Exception as e:
                self._emit("error", "S31", "INTERNAL_ERROR", f"{name}: {e!r}", "error")

    # ───────── S32 의도적 종료 — 파워 출력만 ─────────
    def hold_process(self, e, reason: str) -> bool:
        """챔버 엔진을 붙잡는다 — 정지 순서 없이 멈추고 run_end error PROGRAM_EXIT(엔진 program_exit — 5단계가 만든 길).
        새 장비 명령도 보내지 않게 actions.send 를 막는다(끄기 명령 뒤 다음 블록이 다시 켜지 않게). 공정 중이었으면 True."""
        was = bool(e.busy)
        if hasattr(e, "program_exit"):
            e.program_exit()
        elif was:
            e.emit("run_end", f"프로그램 종료 — 파워 출력만 끔({reason})", "PROGRAM_EXIT", level="error", result="error",
                   run_id=e.run_id)
            e.error = "PROGRAM_EXIT"
        e.actions.send = lambda step, runner: None
        with e.lk:
            e._closing = True
            e._cancel = True
            e.queue_mode = False
            e.lk.notify_all()
        self._held.add(e.ch)
        return was

    def power_off_only(self, reason: str = "PROGRAM_EXIT", deadline_s: float = FORCE_DEADLINE_S) -> Optional[dict]:
        """결정 4 — 파워 출력만 끈다(확인을 기다리지 않는 명령을 먼저 다 보냄). 두 번째 호출은 None."""
        if not self._forced_lk.acquire(blocking=False):
            return None
        clk = self.clock
        t0 = clk.monotonic()
        done: Dict[str, bool] = {}
        sent: List[str] = []
        was: List[str] = []

        def cb(name: str):
            done[name] = False
            sent.append(name)

            def fin(*a) -> None:
                done[name] = True
                clk.notify()
            return fin
        self._emit("fired", "S32", "POWER_OFF_ONLY", f"의도적 종료({reason}) — 파워 출력만 끔", "warn")
        for ch, e in self.proc.items():
            if e is not None and self.hold_process(e, reason):
                was.append(ch)
        pc_held = self._hold_pc(reason)                   # PC 도 붙잡는다(정지 정리 없이) — 뒤의 PC RF 즉시 끄기와 엇갈리지 않게
        if pc_held:
            was.append("pc")
        if not self.bench:
            s = self.sys
            for unit, d in sorted((getattr(s, "dc", {}) or {}).items()):
                if d is not None:
                    d.enabled = True
                    self._try(lambda d=d, u=unit: d.stop(on_done=cb(f"DC{u} OFF")))
            for mode, d in (getattr(s, "rf", {}) or {}).items():
                if d is None:
                    continue
                self._try(lambda d=d, m=mode: rf_zero_off(d, cb(f"RF({m}) 0 W·SET OFF")))
            for ch in ("ch1", "ch2"):
                dp = (getattr(s, "dcpulse", {}) or {}).get(ch)
                if dp is not None and dp.worker is not None:
                    self._try(lambda dp=dp, ch=ch: dp.output_off(on_done=cb(f"{ch} DC Pulse OFF")))
                rp = (getattr(s, "rfpulse", {}) or {}).get(ch)
                if rp is not None and rp.worker is not None:
                    self._try(lambda rp=rp, ch=ch: rp.stop_process(on_done=cb(f"{ch} RF Pulse OFF")))
            if self.heater is not None:
                self._try(lambda: self.heater.shutdown(on_done=cb("히터 RUN=0")))
            if pc_held:
                sent.append("플라즈마 클리닝 붙잡음(PC RF 만 끔 — 가스·밸브 그대로)")
            if self.tsp is not None and self.tsp.busy:
                self._try(lambda: self.tsp.abort("PROGRAM_EXIT"))
                sent.append("TSP 이상 중단(TSP OFF)")
                was.append("tsp")
        t_sent = clk.monotonic() - t0
        if done:
            clk.wait_until(t0 + float(deadline_s), lambda: all(done.values()))
        res = {"why": reason, "sent": sent, "done": [k for k, v in done.items() if v], "sent_s": round(t_sent, 3),
               "elapsed_s": round(clk.monotonic() - t0, 3), "process_was_running": was, "bench": self.bench}
        self.forced = res
        self._emit("released", "S32", "POWER_OFF_ONLY",
                   f"보낸 것 {len(sent)} · 끝난 것 {len(res['done'])} · {res['elapsed_s']:.2f} s", "info")
        return res

    def _hold_pc(self, reason: str) -> bool:
        """플라즈마 클리닝을 정지 순서 없이 붙잡는다(엔진 program_exit — 끝 정리에서 RF 안전 정지·가스 FLOW OFF·SP4 VALVE OPEN 을
        보내지 않음, 결과 실패 PROGRAM_EXIT). 그런 길이 없는 엔진이면 v1 RF 안전 정지(abort). 돌고 있었으면 True."""
        pc = self.pc
        if pc is None or not bool(getattr(pc, "busy", False)):
            return False
        f = getattr(pc, "program_exit", None)
        held = [False]

        def go() -> None:
            held[0] = bool(f(reason) if callable(f) else pc.abort(reason))
        self._try(go)
        return held[0]

    def _try(self, fn) -> None:
        try:
            fn()
        except Exception as e:
            self._emit("error", "S32", "INTERNAL_ERROR", repr(e), "error")

    # ───────── 보기 ─────────
    def public_state(self) -> dict:
        return {"stopping": self.stopping, "allStops": self.all_stops, "residual": [list(x) for x in self.residual],
                "forced": self.forced, "firstRead": self.first_done}
