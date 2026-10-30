r"""OES 측정 루프 공용 — 연결 목록 감시(_LinkGuard)·연속 읽기 실패 복구·첫 프레임·daemon 재초기화.

v1 apps/oes_service/oes_api.py:1143-1357(_ResetCallTimeout·_acquire_named_mutex_async·_LinkGuard·_acquire_first_frame)과
1940-1963(_daemon_reset_device)을 그대로 옮긴 것. 장비 호출(spSetupGivenChannel 리셋·재초기화)의 순서·인자·간격은 v1 그대로.
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Optional

from v2.ch12.workers.oes_common import (
    _LINK_POLL_S, _LINK_SETTLE_S, _LINK_STATUS_EVERY_S, _LINK_WAIT_AT_START_S, _READ_FAIL_REINIT_AT,
    _READ_FAIL_RESET_AT, _REINIT_MUTEX_NAME, _REINIT_MUTEX_WAIT_S, _REINIT_RETRY_S, _RESET_CALL_TIMEOUT_S,
    _WinMutex, _env_float, _errlog, _errlog_exc, _list_cyusb_interfaces, _runlog, _status,
)
from v2.ch12.workers.oes_device import OESAsync


class _ResetCallTimeout(TimeoutError):
    """포트 리셋(spSetupGivenChannel) 호출 타임아웃 → 읽기 타임아웃과 같은 hard_abort 경로."""


async def _acquire_named_mutex_async(mtx: _WinMutex, max_wait_s: float) -> bool:
    # 뮤텍스는 잡은 스레드에서 풀어야 하므로 이벤트 루프 스레드에서 0ms 대기로 반복 확인한다
    loop = asyncio.get_running_loop()
    end = loop.time() + float(max_wait_s)
    while True:
        if mtx.acquire(timeout_ms=0):
            return True
        mtx.release()  # 소유하지 못한 핸들 닫기(ReleaseMutex 실패는 무시됨)
        if loop.time() >= end:
            return False
        await asyncio.sleep(_LINK_POLL_S)


class _LinkGuard:
    """
    측정 루프(daemon / one-shot) 공용: 분광기 연결 목록 감시 + 연속 읽기 실패 복구.
    - 목록이 기준과 다르면 DLL 을 부르지 않고 '연결 끊김'(일시 중지) 상태로 둔다.
    - 목록이 돌아오면 잠시 기다린 뒤 재초기화(뮤텍스)하고 측정을 재개한다.
    - 목록은 정상인데 읽기가 연속 실패하면 N번째에 포트 리셋, M번째에 재초기화.
    """

    def __init__(self, oes: OESAsync, *, ch: int, usb: int, integration_ms: int):
        self.oes = oes
        self.ch = int(ch)
        self.usb = int(usb)
        self.integration_ms = int(integration_ms)

        self.paused = False
        self._pause_t0 = 0.0
        self._last_wait_log = 0.0
        self._next_reinit_try = 0.0
        self._fail_streak = 0
        self._last_fail_reinit: Optional[float] = None

        # finished 페이로드용
        self.link_pause_count = 0     # 일시 중지(연결 끊김) 진입 횟수
        self.link_pause_s = 0.0       # 일시 중지 후 재개까지 걸린 시간 합(초)
        self.reinit_count = 0         # 재초기화 수행 횟수(뮤텍스 획득 후 시도 기준)
        self.reset_count = 0          # 포트 리셋(spSetupGivenChannel) 횟수

    def payload(self) -> dict:
        return {
            "link_pause_count": int(self.link_pause_count),
            "link_pause_s": round(float(self.link_pause_s), 1),
            "reinit_count": int(self.reinit_count),
            "reset_count": int(self.reset_count),
        }

    def _link_changed_msg(self) -> str:
        cur = _list_cyusb_interfaces()
        n = len(cur) if cur is not None else "?"
        m = len(self.oes._link_baseline or [])
        return f"[link] 분광기 연결 변화 감지(현재 {n}대/기준 {m}대) → 측정 일시 중지"

    def _enter_pause(self, msg: str) -> None:
        if self.paused:
            return
        now = time.time()
        self.paused = True
        self._pause_t0 = now
        self._last_wait_log = now
        self._next_reinit_try = 0.0
        self.link_pause_count += 1
        _status(msg)

    def _resume(self) -> None:
        dur = max(0.0, time.time() - self._pause_t0)
        self.paused = False
        self.link_pause_s += dur
        self._fail_streak = 0
        _status(f"[link] 분광기 재연결 → 재초기화 완료, 측정 재개(중단 {dur:.0f}초)")

    async def _reinit(self) -> bool:
        mtx = _WinMutex(_REINIT_MUTEX_NAME)
        if not await _acquire_named_mutex_async(mtx, _REINIT_MUTEX_WAIT_S):
            _status(f"[link] 재초기화 뮤텍스 대기 초과({_REINIT_MUTEX_WAIT_S:.0f}초) → 다음 기회에 재시도")
            return False
        try:
            self.reinit_count += 1
            ok = await _daemon_reset_device(self.oes, ch=self.ch, usb=self.usb)
            if not ok:
                _status(f"[link] 재초기화 실패: {self.oes._last_error or 'initialize_device failed'}")
                return False
            with contextlib.suppress(Exception):
                await self.oes._call(self.oes._apply_device_settings_blocking, int(self.oes.sChannel), self.integration_ms)
            return True
        finally:
            mtx.release()

    async def reset_port(self, msg: str) -> None:
        """spSetupGivenChannel 1회(포트 리셋). 호출 타임아웃이면 _ResetCallTimeout."""
        self.reset_count += 1
        _status(msg)
        try:
            await asyncio.wait_for(
                self.oes._call(self.oes._setup_channel_blocking, int(self.oes.sChannel)),
                timeout=_RESET_CALL_TIMEOUT_S,
            )
        except asyncio.TimeoutError as e:
            raise _ResetCallTimeout(
                f"OES reset(spSetupGivenChannel) timeout after {_RESET_CALL_TIMEOUT_S}s (ch={self.ch}, usb={self.usb})"
            ) from e
        except Exception as e:
            _runlog(f"[read] 포트 리셋 예외: {type(e).__name__}: {e}")

    async def ensure_at_start(self) -> None:
        """측정 시작(첫 프레임 전): 목록이 기준과 다르면 복귀를 기다렸다가 재초기화. 안 되면 RuntimeError."""
        if self.oes.link_ok():
            if self.oes.sp_dll is not None and int(self.oes.sChannel) >= 0:
                return
            # 이전 재초기화 실패로 장치가 닫혀 있음 → 목록은 정상이므로 재초기화부터
            if not await self._reinit():
                raise RuntimeError("[link] 측정 시작 전 분광기 재초기화 실패")
            return
        self._enter_pause(self._link_changed_msg())
        end = time.time() + _LINK_WAIT_AT_START_S
        while not self.oes.link_ok():
            if time.time() >= end:
                raise RuntimeError(
                    f"[link] 측정 시작 전 분광기 연결 목록이 기준과 다름({_LINK_WAIT_AT_START_S:.0f}초 대기 후에도 복귀 안 됨)"
                )
            await asyncio.sleep(_LINK_POLL_S)
        await asyncio.sleep(_LINK_SETTLE_S)
        if not self.oes.link_ok() or not await self._reinit():
            raise RuntimeError("[link] 측정 시작 전 분광기 재초기화 실패")
        self._resume()

    async def _tick_paused(self) -> None:
        now = time.time()
        if not self.oes.link_ok():
            if now - self._last_wait_log >= _LINK_STATUS_EVERY_S:
                self._last_wait_log = now
                _status(f"[link] 재연결 대기 중(경과 {now - self._pause_t0:.0f}초)")
            return
        if now < self._next_reinit_try:
            return
        await asyncio.sleep(_LINK_SETTLE_S)
        if not self.oes.link_ok():
            return
        if await self._reinit():
            self._resume()
        else:
            self._next_reinit_try = time.time() + _REINIT_RETRY_S
            _status(f"[link] {_REINIT_RETRY_S:.0f}초 뒤 재초기화 재시도")

    async def before_read(self) -> bool:
        """샘플 읽기 직전. False 면 DLL 호출 없이 이번 샘플을 건너뛴다."""
        if self.paused:
            await self._tick_paused()
            return not self.paused
        if not self.oes.link_ok():
            self._enter_pause(self._link_changed_msg())
            return False
        return True

    async def after_read(self, got: bool) -> bool:
        """샘플 읽기 직후. True 면 행을 기록한다(목록이 바뀌었으면 방금 샘플은 버린다)."""
        if not self.oes.link_ok():
            self._enter_pause(self._link_changed_msg())
            return False
        if got:
            self._fail_streak = 0
            return True

        self._fail_streak += 1
        n = self._fail_streak
        if n == _READ_FAIL_RESET_AT:
            await self.reset_port(f"[read] 연속 읽기 실패 {n}회 → 포트 리셋")
        if n >= _READ_FAIL_REINIT_AT:
            now = time.time()
            if self._last_fail_reinit is None or now - self._last_fail_reinit >= _REINIT_RETRY_S:
                self._last_fail_reinit = now
                _status(f"[read] 연속 읽기 실패 {n}회 → 재초기화")
                if await self._reinit():
                    self._fail_streak = 0
                    _status("[read] 재초기화 완료, 측정 계속")
                else:
                    # 장치가 닫힌 상태라 읽을 수 없음 → 일시 중지하고 끊김과 같은 규칙으로 재시도
                    self._enter_pause(f"[read] 재초기화 실패 → 측정 일시 중지, {_REINIT_RETRY_S:.0f}초 뒤 재시도")
                    self._next_reinit_try = time.time() + _REINIT_RETRY_S
        return False


async def _acquire_first_frame(oes: OESAsync, retries: int = 20, delay_s: float = 0.2, guard: Optional[_LinkGuard] = None):
    # ✅ 첫 프레임도 DLL hang 가능 → 타임아웃으로 끊기
    call_timeout_s = _env_float("OES_FIRST_FRAME_TIMEOUT_S", 2.0)

    last_err = None
    empty = 0
    for _ in range(max(1, retries)):
        try:
            x, y = await asyncio.wait_for(
                oes._call(oes._acquire_one_slice_avg),
                timeout=call_timeout_s,
            )
            if x is not None and y is not None:
                return x, y
        except asyncio.TimeoutError as e:
            # ✅ 이건 DLL 스레드 hang 가능성이 높으니 즉시 상위로 올려서 워커가 '자가 종료' 루트로 가게 함
            raise TimeoutError(f"first frame timeout after {call_timeout_s}s") from e
        except Exception as e:
            last_err = e
        else:
            # (None, None): 3번째 실패마다 포트 리셋 1회(타임아웃이면 TimeoutError 로 올라감)
            empty += 1
            if guard is not None and empty % _READ_FAIL_RESET_AT == 0:
                await guard.reset_port(f"[read] 첫 프레임 읽기 실패 {empty}회 → 포트 리셋")
        await asyncio.sleep(delay_s)

    raise RuntimeError(f"first frame failed: {last_err}")



async def _daemon_reset_device(oes: OESAsync, *, ch: int, usb: int) -> bool:
    """cleanup + initialize_device 재시도. (daemon 내부 복구용)"""
    cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)
    init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)

    # cleanup
    try:
        await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
    except asyncio.TimeoutError:
        _errlog(f"[daemon] reset cleanup timeout after {cleanup_timeout_s}s (ch={ch} usb={usb})")
        return False
    except Exception:
        pass

    # re-init (새로 객체를 만들지 않고 같은 객체 재사용)
    try:
        ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        return bool(ok)
    except asyncio.TimeoutError:
        _errlog(f"[daemon] reset init timeout after {init_timeout_s}s (ch={ch} usb={usb})")
        return False
    except Exception as e:
        _errlog_exc(f"[daemon] reset init exception: {type(e).__name__}: {e} (ch={ch} usb={usb})")
        return False
