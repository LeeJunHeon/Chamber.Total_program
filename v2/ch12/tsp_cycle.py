"""TSP 운전 엔진의 사이클 부분 — v1 controller/tsp_controller.py(TSPProcessController.run·_poll_until) 를 같은 순서·문구로,
그리고 엔진이 같이 쓰는 정의(단계 표 PHASES·v1 예외 모양·입력 파싱·이벤트·결과). 엔진(tsp_engine.TspEngine)이 섞어 쓴다.

기다림은 모두 시계 대기(clock.wait_until — 가상 시계 시험). 취소(STOP·전체 상한·abort)는 컨트롤러 안의 기다림에서만 걸린다
(_Cancelled — asyncio.CancelledError 처럼 except Exception 에 잡히지 않는다). 장비 호출은 넣은 요청을 그대로 두고 결과만 버린다
(v1 await 취소와 같다). 정리 단계(finally)의 장비 호출은 취소되지 않고 FINALLY_WAIT_S 상한으로 기다린다.
v1 예외 모양: drivers.tsp 의 실패(TspDone)를 v1 이 올렸을 예외로 바꿔 로그 문구('[TSP] 예외: TSPError(...)')를 v1 과 같게 —
  TspVerifyTimeout → TSPError, TspNackError → TSPNackError, TspWindowError → TSPWindowError, TspProtocolError·TspUnexpectedReply →
  TSPProtocolError, TspNoResponse → TimeoutError()(v1 asyncio.wait_for(readexactly)), TspLinkError → ConnectionError(문구 —
  v1 원문은 OSError 계열, 미확인), TspFrameError → ValueError. code 속성 = v1 E 코드(D17).
"""
from __future__ import annotations

import ast
import math
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

OWNER = "tsp"                        # 게이지 소유자 이름(shared.tsp_ig)
FINALLY_WAIT_S = 60.0                # 정리 단계 장비 호출 하나를 기다리는 상한(v1 은 바이트마다 제한 — 끝없이 기다리지 않게)
CLOSE_WAIT_S = 120.0
# 단계 정의 — 어느 것도 상태 유지 블록이 아니다(장비 켜기·끄기·사이클 도중) → 재시작 뒤 이어가지 않는다(안전 정지)
PHASES: Dict[str, Dict[str, Any]] = {
    "idle": {"label": "대기", "resumable": False},
    "prepare": {"label": "준비", "resumable": False},
    "ig_on": {"label": "IG ON·첫 확인", "resumable": False},
    "tsp_on": {"label": "TSP ON(승화)", "resumable": False},
    "tsp_off": {"label": "TSP OFF(사이클 간격)", "resumable": False},
    "cleanup": {"label": "정리", "resumable": False},
    "finished": {"label": "정리 끝", "resumable": False},
    "closing": {"label": "종료 정리(OFF·IG OFF)", "resumable": False},
}
OWNERS = ("robot", "web", "local")


# v1 예외(device/tsp.py:51-63) — 로그 문구가 v1 repr 과 같게
class TSPError(Exception):
    code: Optional[str] = None


class TSPNackError(TSPError):
    pass


class TSPProtocolError(TSPError):
    pass


class TSPWindowError(TSPError):
    pass


_V1_CLASS = {"TspVerifyTimeout": TSPError, "TspNackError": TSPNackError, "TspWindowError": TSPWindowError,
             "TspProtocolError": TSPProtocolError, "TspUnexpectedReply": TSPProtocolError,
             "TspLinkError": ConnectionError, "TspFrameError": ValueError}


def v1_exception(done) -> BaseException:
    """drivers.tsp.TspDone(실패) → v1 이 올렸을 예외(repr 이 같게). code 속성 = v1 E 코드."""
    text = str(getattr(done, "detail", "") or getattr(done, "reason", "") or "")
    m = re.match(r"^(\w+)\((.*)\)$", text, re.S)
    name, msg = (m.group(1), m.group(2)) if m else ("", text)
    if m:
        try:
            msg = ast.literal_eval(msg) if msg else ""
        except Exception:
            pass
    if name == "TspNoResponse":
        e: BaseException = TimeoutError()                     # v1 asyncio.wait_for(readexactly) 시간 초과
    elif name in _V1_CLASS:
        e = _V1_CLASS[name](msg)
    else:
        e = RuntimeError(text)
    try:
        e.code = getattr(done, "code", None)                  # type: ignore[attr-defined]
    except Exception:
        pass
    return e


class _Cancelled(BaseException):
    """취소(STOP·전체 상한·abort) — asyncio.CancelledError 자리(except Exception 에 잡히지 않음)."""


class _UserStop(BaseException):
    """v1(3.11) 바깥 태스크로 올라온 STOP."""


@dataclass(frozen=True)
class TspEngineEvent:
    kind: str                      # log / popup / card / state / result
    detail: str = ""
    level: str = "info"
    code: Optional[str] = None
    reason: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TspRunResult:
    """v1 TSPRunResult."""
    success: bool
    cycles_done: int
    final_pressure: float
    reason: Optional[str] = None
    code: Optional[str] = None


def _txt(v: Any) -> str:
    if v is None or isinstance(v, bool):
        return ""
    return str(v).strip()


def parse_target(v: Any) -> float:
    """v1 _read_target(tsp_runtime.py:239-243) — 예외 문구 그대로."""
    t = _txt(v)
    if not t:
        raise ValueError("TSP_targetPressure_edit 가 비어있습니다.")
    return float(t)


def parse_cycles(v: Any) -> int:
    """v1 _read_cycles(tsp_runtime.py:245-252)."""
    t = _txt(v)
    if not t:
        raise ValueError("TSP_setCycle_edit 가 비어있습니다.")
    n = int(t)
    if n < 1:
        raise ValueError("반복 횟수는 1 이상이어야 합니다.")
    return n



class TspCycle:
    """TspEngine 이 상속한다(속성은 엔진 것 — clock·gauge·tsp·_s·_cancel·_deadline·_in_ctrl·_lock …)."""

    # ═══════════════════════ 기다림(가상 시계) ═══════════════════════
    def _cancel_now(self) -> bool:
        if self._cancel is not None:
            return True
        if self._in_ctrl and self._deadline is not None and self.clock.monotonic() >= self._deadline:
            self._cancel = ("timeout", f"TSP run timed out after {self._deadline - (self.started_ctrl or 0):.0f}s")
            return True
        return False

    def _check_cancel(self) -> None:
        if self._in_ctrl and self._cancel_now():
            raise _Cancelled()

    def _sleep(self, seconds: float) -> None:
        """취소되는 대기(v1 asyncio.sleep)."""
        end = self.clock.monotonic() + max(0.0, float(seconds))
        tgt = end if self._deadline is None else min(end, self._deadline)
        while True:
            self.clock.wait_until(tgt, self._cancel_now)
            self._check_cancel()
            if self.clock.monotonic() >= end:
                return

    def _call(self, submit: Callable[[Callable], None], cancellable: bool) -> Any:
        """장비 호출 하나 — submit(on_done) 를 넣고 결과를 기다린다. 취소되면(컨트롤러 안) _Cancelled — 넣은 요청은 그대로
        간다(v1 await 취소와 같이 결과만 버림). 정리 단계(cancellable=False)는 FINALLY_WAIT_S 상한."""
        box: List[Any] = []

        def done(*a) -> None:
            box.append(a)
            self.clock.notify()
        submit(done)
        if cancellable and self._in_ctrl:
            tgt = self._deadline
            while not box:
                self.clock.wait_until(tgt, lambda: bool(box) or self._cancel_now())
                if box:
                    break
                self._check_cancel()
            return box[0]
        if not self.clock.wait_until(self.clock.monotonic() + FINALLY_WAIT_S, lambda: bool(box)):
            raise TimeoutError()
        return box[0]

    # ── 장비 동작(v1 예외 모양으로) ──
    def _ig(self):
        d = getattr(self.gauge, "driver", None)
        if d is None:
            raise RuntimeError("TSP 게이지 없음")
        return d

    def _ig_ensure_on(self) -> None:
        ig = self._ig()
        _k, reason, detail = self._call(lambda cb: ig.ensure_on(on_done=cb), True)
        if reason is not None:
            raise RuntimeError(detail or reason)

    def _ig_read(self) -> float:
        ig = self._ig()
        _k, reason, detail = self._call(lambda cb: ig.read_pressure(on_done=cb), True)
        if reason is not None:
            raise RuntimeError(detail or reason)
        return float(detail)

    def _ig_off(self, cleanup: bool = False) -> None:
        ig = self._ig()
        fn = ig.cleanup if cleanup else ig.ensure_off
        _k, reason, detail = self._call(lambda cb: fn(on_done=cb), self._in_ctrl)
        if reason is not None:
            raise RuntimeError(detail or reason)

    def _tsp_switch(self, on: bool) -> None:
        fn = self.tsp.on if on else self.tsp.off
        (done,) = self._call(lambda cb: fn(on_done=cb), self._in_ctrl)
        if not done.ok:
            self._last_code = getattr(done, "code", None) or self._last_code
            raise v1_exception(done)

    # ═══════════════════════ 컨트롤러(tsp_controller.py:108-194) ═══════════════════════
    def _poll_until(self, target: float, duration: float, poll_sec: float) -> bool:
        deadline = self.clock.monotonic() + max(0.0, duration)
        with self._lock:
            self.phase_end = deadline
        while True:
            try:
                p = self._ig_read()
                self._pressure(p)
                if p <= target:
                    return True
            except Exception as e:
                self._log(f"[IG] 압력 읽기 오류: {e!r}")
            now = self.clock.monotonic()
            if now >= deadline:
                return False
            self._sleep(min(max(0.1, float(poll_sec)), max(0.0, deadline - now)))

    def _ctrl_run(self, target: float, cycles: int) -> TspRunResult:
        s = self._s
        on_sec, off_sec = max(0.0, float(s["on"])), max(0.0, float(s["off"]))
        poll_sec, first = max(0.1, float(s["poll"])), max(0.0, float(s["first"]))
        cycles_done = 0
        self.last_pressure = math.nan
        if int(cycles) <= 0:
            return TspRunResult(False, 0, self.last_pressure, "invalid_cycles")
        self._in_ctrl = True
        try:
            self._state("prepare")
            self._log(f"[TSP] 공정 시작: target={target}, cycles={cycles}, on={on_sec}s, off={off_sec}s, "
                      f"poll={poll_sec}s, first_wait={first}s")
            self._state("ig_on")
            self._ig_ensure_on()
            self._log("[IG] ensure_on 완료")
            with self._lock:
                self.phase_end = self.clock.monotonic() + first
            self._sleep(first)
            try:
                p0 = self._ig_read()
                self._pressure(p0)
                self._log(f"[IG] 초기 판정 P0={p0:.3e}")
                if p0 <= target:
                    return TspRunResult(True, cycles_done, self.last_pressure, "initial_below_target")
            except Exception as e:
                self._log(f"[IG] 초기 판정 RDI 실패: {e!r}")
            for i in range(1, cycles + 1):
                self.cycle = i
                self._state("tsp_on")
                self._log(f"[TSP] ON (cycle {i}/{cycles})")
                self._tsp_switch(True)
                if self._poll_until(target, on_sec, poll_sec):
                    return TspRunResult(True, cycles_done, self.last_pressure, "reached_target_on")
                self._state("tsp_off")
                self._log(f"[TSP] OFF (cycle {i}/{cycles})")
                self._tsp_switch(False)
                if self._poll_until(target, off_sec, poll_sec):
                    return TspRunResult(True, cycles_done, self.last_pressure, "reached_target_off")
                cycles_done = i
                self.cycles_done = i
                self._cycle_done(cycles_done, cycles)
            return TspRunResult(False, cycles_done, self.last_pressure, "cycles_exhausted")
        except _Cancelled:
            self._in_ctrl = False
            self._log("[TSP] run() 취소됨")
            return TspRunResult(False, cycles_done, self.last_pressure, "cancelled")
        except Exception as e:
            self._in_ctrl = False
            self._log(f"[TSP] 예외: {e!r}")
            return TspRunResult(False, cycles_done, self.last_pressure, f"exception:{e!r}", getattr(e, "code", None))
        finally:
            self._in_ctrl = False
            self._state("cleanup")
            self._quiet(lambda: self._tsp_switch(False))
            self._quiet(lambda: self._ig_off())
            self._state("finished")

    @staticmethod
    def _quiet(fn: Callable[[], None]) -> None:
        try:
            fn()
        except Exception:                 # v1 contextlib.suppress(Exception)
            pass
