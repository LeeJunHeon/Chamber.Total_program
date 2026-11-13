"""RF Pulse 설정 명령 쓰기 → 되읽기 확인 → 다시 쓰기 → VERIFY_MISMATCH(2026-11-09 E2 — D50 통일을 SETPOINT·MODE·PULSING 으로 넓힘).

되읽기 질의(AE Bus 보고 명령): SET_SETPOINT(8) → 164(셋포인트 W, u16 LE) · SET_PULSING(27) → 177(펄싱 모드 1 B) · SET_MODE(3) → 설정 키
RFPULSE_Q_MODE(기본 154 — 2026-11-13 D407: CESAR 1310 매뉴얼 Table 4-16 '154 Reports regulation mode, 1 byte: 6 Forward · 7 Load · 8 External(DC Bias), set with
command 3'. 0 = 끔). ⚠ 155 는 v1·v2 가 이미 'READ_ACTIVE_CTRL'(제어 모드 — HOST 확인, 값 2)로 쓰는 질의라 조절 모드(6 FWD)를 되읽을 수 없다(읽으면 늘 불일치).
164·177 도 사용자 지시 2026-11-09 의 번호이고 매뉴얼 쪽수는 미확인 — 응답이 오지 않으면(통신 실패) 검증을 생략하고 경고만(freq·duty 되읽기와 같은 규칙),
되읽은 값이 다르면 RFPULSE_REWRITE_MAX 회까지 다시 쓰고 그래도 다르면 command_failed VERIFY_MISMATCH.
RFPULSE_READBACK_ENABLED=false 면 v1 처럼 CSR 만.
"""
from __future__ import annotations

from typing import Callable, Optional, Tuple

from v2.ch12.drivers.pulse_port import Sleep
from v2.common import aebus

Q_SETPOINT, Q_PULSING = 164, 177


class RfpVerifyMixin:
    def _rb_cfg(self) -> Tuple[bool, int, float]:
        on = bool(self._c("RFPULSE_READBACK_ENABLED", True))
        return on, (max(0, int(self._c("RFPULSE_REWRITE_MAX", 3))) if on else 0), int(self._c("RFPULSE_REWRITE_GAP_MS", 100)) / 1000.0

    def _g_write_verified(self, cmd: int, data: bytes, tag: str, query: int, matches: Callable[[bytes], bool], what: str,
                          timeout_ms: Optional[int] = None):
        """(성공, CSR 바이트, 불일치 여부). 불일치면 command_failed(VERIFY_MISMATCH)를 이미 냈다 — 부른 쪽은 시작 시퀀스를 그냥 끝낸다."""
        ok, cb = yield from self._g_exec(cmd, data, tag, timeout_ms)
        if not ok:
            return False, cb, False
        rb_on, rew, gap = self._rb_cfg()
        if not rb_on or not query:
            return True, cb, False
        mism = ""
        for attempt in range(rew + 1):
            if gap > 0:
                yield Sleep(gap)
            got = yield from self._g_query(query, f"[READ {what}]")
            if got is None:
                self._emit("status", f"[VERIFY] RF Pulse {what} read-back 실패(통신) → 검증 생략")
                return True, cb, False
            if matches(got):
                self._emit("status", f"[VERIFY] RF Pulse {what} OK")
                return True, cb, False
            mism = f"{what} (요청 {data.hex()} ≠ 장비 {got.hex()})"
            if attempt < rew:
                self._emit("status", f"[VERIFY] RF Pulse {mism} 불일치 → 다시 쓰기 {attempt + 1}/{rew}")
                ok, cb = yield from self._g_exec(cmd, data, f"[RE-WRITE {what}]", timeout_ms)
                if not ok:
                    return False, cb, False
        self._emit("command_failed", f"RF Pulse {mism} — 다시 쓰기 {rew}회 뒤에도 되읽기 불일치", reason="VERIFY_MISMATCH", cmd=what)
        return False, cb, True

    @staticmethod
    def match_setpoint(sp: int) -> Callable[[bytes], bool]:
        return lambda b: len(b) >= 2 and aebus.u16(b) == int(sp)

    @staticmethod
    def match_byte(v: int) -> Callable[[bytes], bool]:
        return lambda b: len(b) >= 1 and b[0] == int(v)
