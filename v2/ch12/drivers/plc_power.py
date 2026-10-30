"""PLC D/A 전원(DC1·DC2·RF 연속·PC RF) 공통 — 이벤트 형식과 'OFF = 0 W 확인 뒤 SET OFF' 순서.

v1 근거: device/dc_power.py:431-511(_write_zero_confirmed·_set_off_after_zero), device/rf_power.py:863-925(_finish_power_off).
PLC D/A 는 "0 W 쓰기 성공"이 OFF 확인 기준이다 — 래더에서 DCV_SET_n 이 출력 허용과 WRITE MOV 를 같이 켜고 채널 출력 상태가
'이전값' 이라 0 W 가 들어가지 않은 채 SET 을 끄면 마지막 출력값이 유지될 수 있다(device/dc_power.py:114-117).
  0 W 쓰기: 실패하면 0.5 s 뒤 다시, POWER_OFF_ZERO_DEADLINE_S(10 s)가 지나면 실패(출력 상태 미확인).
  SET OFF: 0 W 가 확인된 뒤에만, 3회까지(0.2 s 간격) — 실패는 경고만(출력은 이미 0).
쓰기는 모두 worker 의 safety 요청(정지 경로 — purge 없음: 공유 PLC 의 다른 챔버 요청을 버리지 않는다).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from v2.common.worker import Result, Status

ZERO_RETRY_S = 0.5
SET_OFF_TRIES = 3
SET_OFF_RETRY_S = 0.2


@dataclass(frozen=True)
class PowerEvent:
    """v1 DCPowerEvent·RFPowerEvent 와 같은 kind: status / display / state_changed / target_reached / target_failed /
    power_off_finished / power_off_failed. detail = v1 message. data 에 power·voltage·current 또는 forward·reflected·running."""
    kind: str
    detail: str = ""
    reason: Optional[str] = None
    code: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


def err_text(res: Result) -> str:
    return res.error or res.reason or res.status


class ZeroThenOff:
    """0 W 확인 → SET OFF. write_zero(on_done, not_before)·set_off(on_done, not_before) 는 PLC 요청을 내는 함수."""

    def __init__(self, clock, deadline_s: float, write_zero: Callable, set_off: Optional[Callable],
                 enabled: bool, on_zero_fail: Callable[[int, float, str], None],
                 on_set_off: Callable[[bool, str], None], on_done: Callable[[bool], None]) -> None:
        self.clock = clock
        self.deadline_s = float(deadline_s) or 10.0
        self.write_zero, self.set_off, self.enabled = write_zero, set_off, enabled
        self.on_zero_fail, self.on_set_off, self.on_done = on_zero_fail, on_set_off, on_done
        self.t0 = clock.monotonic()
        self.n = 0
        self.tries = 0

    def start(self) -> None:
        self._zero(None)

    def _zero(self, not_before) -> None:
        self.n += 1
        self.write_zero(self._zero_done, not_before)

    def _zero_done(self, res: Result) -> None:
        if res.status == Status.OK:
            if self.set_off is not None and self.enabled:
                self._off(None)
            else:
                self.on_done(True)
            return
        el = self.clock.monotonic() - self.t0
        if el >= self.deadline_s:
            self.on_zero_fail(self.n, el, err_text(res))
            self.on_done(False)
            return
        self._zero(self.clock.monotonic() + ZERO_RETRY_S)

    def _off(self, not_before) -> None:
        self.tries += 1
        self.set_off(self._off_done, not_before)

    def _off_done(self, res: Result) -> None:
        if res.status == Status.OK:
            self.on_set_off(True, "")
            self.on_done(True)
        elif self.tries >= SET_OFF_TRIES:
            self.on_set_off(False, err_text(res))
            self.on_done(True)
        else:
            self._off(self.clock.monotonic() + SET_OFF_RETRY_S)
