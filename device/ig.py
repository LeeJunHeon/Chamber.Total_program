# -*- coding: utf-8 -*-
"""
ig.py — asyncio 기반 IG(이온/진공 게이지) 컨트롤러

의존성: pyserial-asyncio
    pip install pyserial-asyncio

설계 요점(기존 PyQt 버전과 동등 기능):
  - serial_asyncio + asyncio.Protocol
  - 단일 명령 큐(타임아웃/재시도/인터커맨드 gap)로 송수신 직렬화
  - 연결 워치독(지수 백오프) - 중간 단선/포트 오류 복구
  - 시퀀스: "SIG 1(IG ON) → 첫 읽기 지연 → RDI 1회 → 이후 폴링"
  - 폴링 중 'IG OFF' 응답 시 자동 재점등(백오프 2s/5s/10s)
  - base pressure 도달/실패 시 SIG 0(IG OFF) 후 정리

UI/Qt 의존성 없음. 상위는 `await`/`async for`로만 사용.
"""

from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Optional, Callable, Deque, AsyncGenerator, Literal
import asyncio
import time

try:
    import serial_asyncio
except Exception as e:
    raise RuntimeError(
        "pyserial-asyncio가 필요합니다. `pip install pyserial-asyncio` 후 다시 시도하세요."
    ) from e

from lib.config_ch2 import (
    IG_PORT, IG_BAUD, IG_WAIT_TIMEOUT,
    IG_TIMEOUT_MS, IG_GAP_MS,
    IG_POLLING_INTERVAL_MS, IG_WATCHDOG_INTERVAL_MS,
    IG_RECONNECT_BACKOFF_START_MS, IG_RECONNECT_BACKOFF_MAX_MS,
    IG_REIGNITE_MAX_ATTEMPTS, IG_REIGNITE_BACKOFF_MS,
    DEBUG_PRINT,
)

# =========================
# 이벤트 모델
# =========================
EventKind = Literal["status", "pressure", "base_reached", "base_failed"]

@dataclass
class IGEvent:
    kind: EventKind
    message: Optional[str] = None  # status/failed 사유
    pressure: Optional[float] = None  # pressure 이벤트일 때만 사용

# =========================
#  명령 큐에 넣는 레코드
# =========================
@dataclass
class Command:
    cmd_str: str                              # '\r' 포함 또는 자동 보정
    callback: Optional[Callable[[Optional[str]], None]]  # 응답 1줄 또는 None
    timeout_ms: int
    gap_ms: int
    tag: str
    retries_left: int
    allow_no_reply: bool

# =========================
#  라인 프레이밍 Protocol
# =========================
class _IGProtocol(asyncio.Protocol):
    """CR/LF로 라인 프레이밍해서 IG에 전달."""
    def __init__(self, owner: "AsyncIG"):
        self.owner = owner
        self.transport: Optional[asyncio.Transport] = None
        self._rx = bytearray()
        self._RX_MAX = 16 * 1024
        self._LINE_MAX = 512

    # --- asyncio.Protocol 콜백 ---
    def connection_made(self, transport: asyncio.BaseTransport):
        self.transport = transport  # type: ignore
        self.owner._on_connection_made(self.transport)

    def data_received(self, data: bytes):
        if not data:
            return
        self._rx.extend(data)
        # 과다 보호
        if len(self._rx) > self._RX_MAX:
            del self._rx[:-self._RX_MAX]
            self.owner._dbg("IG", f"수신 버퍼 과다(RX>{self._RX_MAX}); 최근 {self._RX_MAX}B만 보존.")

        # 라인 파싱: 한 콜백에서 가능한 모든 라인을 owner로 전달
        while True:
            i_cr = self._rx.find(b'\r')
            i_lf = self._rx.find(b'\n')
            if i_cr == -1 and i_lf == -1:
                break
            idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
            line_bytes = self._rx[:idx]

            # CRLF/LFCR 동시 처리
            drop = idx + 1
            if drop < len(self._rx):
                ch = self._rx[idx]
                nxt = self._rx[idx + 1]
                if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                    drop += 1
            del self._rx[:drop]

            if len(line_bytes) > self._LINE_MAX:
                self.owner._dbg("IG", f"Rx line too long (+{len(line_bytes)-self._LINE_MAX}B), truncating")
                line_bytes = line_bytes[:self._LINE_MAX]

            try:
                line = line_bytes.decode("ascii", errors="ignore").strip()
            except Exception:
                line = ""

            if line:
                self.owner._on_line_from_serial(line)

        # 선행 CR/LF 정리 (남은 경우만)
        while self._rx[:1] in (b'\r', b'\n'):
            del self._rx[0:1]

    def connection_lost(self, exc: Optional[Exception]):
        self.owner._on_connection_lost(exc)

# =========================
#  IG asyncio 컨트롤러
# =========================
class AsyncIG:
    def __init__(self):
        # 설정
        self.debug_print = DEBUG_PRINT
        # 연결/프로토콜
        self._transport: Optional[asyncio.Transport] = None
        self._protocol: Optional[_IGProtocol] = None
        self._connected: bool = False

        # 명령 큐/인플라이트
        self._cmd_q: Deque[Command] = deque()
        self._inflight: Optional[Command] = None

        # 응답 라인 큐(프로토콜 → 워커)
        self._line_q: asyncio.Queue[str] = asyncio.Queue(maxsize=1024)

        # 이벤트 큐(상위/UI 소비용)
        self._event_q: asyncio.Queue[IGEvent] = asyncio.Queue(maxsize=1024)

        # 태스크들
        self._want_connected: bool = False
        self._watchdog_task: Optional[asyncio.Task] = None
        self._cmd_worker_task: Optional[asyncio.Task] = None
        self._polling_task: Optional[asyncio.Task] = None

        # 재연결 백오프
        self._reconnect_backoff_ms = IG_RECONNECT_BACKOFF_START_MS

        # 베이스 압력 대기
        self._target_pressure = 0.0
        self._waiting_active = False
        self._wait_start_s = 0.0

        self._first_read_delay_ms = 5000  # IG ON OK 후 첫 RDI 전 지연(1회)

        # ✅ 재점등(자동 ON 재시도) 제어 플래그/카운터
        self._suspend_reignite: bool = False     # 종료/취소 중 재점등 금지
        self._total_reignite_attempts: int = 0   # 누적 재점등 횟수
        # 선택: 마지막 성공 여부 초기화(이미 사용 중이면 안전을 위해 추가)
        self._last_wait_success: bool = False

    # ---------------------------
    # 공용 API
    # ---------------------------
    async def start(self):
        """워치독/커맨드 워커 시작. (연결은 워치독이 담당)"""
        if self._watchdog_task or self._cmd_worker_task:
            return
        self._want_connected = True
        loop = asyncio.get_running_loop()
        self._watchdog_task = loop.create_task(self._watchdog_loop(), name="IGWatchdog")
        self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="IGCmdWorker")
        await self._emit_status(f"IG 워치독/워커 시작")

    async def cleanup(self):
        """모든 태스크 중지 및 포트 종료."""
        await self._emit_status("IG 종료 절차 시작")
        self._want_connected = False
        self._waiting_active = False
        self._suspend_reignite = True           # ✅ 재점등 금지

        # 폴링 태스크
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except Exception:
                pass
            self._polling_task = None

        # 명령 워커
        if self._cmd_worker_task:
            self._cmd_worker_task.cancel()
            try:
                await self._cmd_worker_task
            except Exception:
                pass
            self._cmd_worker_task = None

        # 워치독
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except Exception:
                pass
            self._watchdog_task = None

        # 큐/인플라이트 정리
        self._purge_pending("shutdown")

        # 연결 종료
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._transport = None
        self._protocol = None
        self._connected = False

        await self._emit_status("IG 연결 종료됨")

    def enqueue(
        self,
        cmd_str: str,
        on_reply: Optional[Callable[[Optional[str]], None]] = None,
        *,
        timeout_ms: int = IG_TIMEOUT_MS,
        gap_ms: int = IG_GAP_MS,
        tag: str = "",
        retries_left: int = 5,
        allow_no_reply: bool = False,
    ):
        """명령을 큐에 추가(단일 직렬 처리)."""
        if not cmd_str.endswith("\r"):
            cmd_str += "\r"
        self._cmd_q.append(
            Command(
                cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply
            )
        )

    async def wait_for_base_pressure(self, base_pressure: float, interval_ms: int = IG_POLLING_INTERVAL_MS) -> bool:
        """
        IG를 켜고(SIG 1) 목표 압력에 도달할 때까지 폴링(RDI) 후, 도달하면 SIG 0로 끄고 True를 반환.
        시간 초과 시 SIG 0 후 False.
        (진행 중 이벤트는 events() 제너레이터로도 전달)
        """
        # 워치독이 연결을 시도/유지
        if not self._watchdog_task:
            await self.start()

        self._target_pressure = float(base_pressure)
        self._wait_start_s = time.monotonic()
        self._waiting_active = True

        # ✅ 재점등 상태 초기화
        self._suspend_reignite = False
        self._total_reignite_attempts = 0
        self._last_wait_success = False

        await self._emit_status("Base Pressure 대기 시작")

        # (A) IG ON (응답 필수)
        ok = await self._send_and_expect_ok("SIG 1", tag="[IG ON]", retries=5)
        if not ok:
            await self._emit_failed("IG ON 실패")
            await self._sig0_off_ignore_reply()
            self._waiting_active = False
            return False

        # 첫 RDI 지연 후 1회 읽기
        await self._emit_status(f"IG ON OK → 첫 RDI를 {self._first_read_delay_ms}ms 후 수행")
        await asyncio.sleep(self._first_read_delay_ms / 1000.0)

        # 첫 RDI 1회
        line = await self._send_and_wait_line("RDI", tag="[FIRST READ AFTER ON]", timeout_ms=IG_TIMEOUT_MS)
        if not self._waiting_active:
            return False
        # 라인 파싱/처리 (도달/실패/IG OFF는 다음 루프에서 동일 처리)
        await self._handle_rdi_line(line)

        # 폴링 시작
        if self._waiting_active:
            self._polling_task = asyncio.create_task(self._poll_rdi_loop(interval_ms))

        # 완료를 이 함수에서 기다림: True/False 반환
        # 폴링 태스크가 set by self._waiting_active False when done
        try:
            while self._waiting_active:
                await asyncio.sleep(0.05)

            # 상태가 이미 처리되었고 SIG 0는 내부에서 처리
            # 최종 성공/실패는 이벤트에서 이미 방출됨
            # 여기서는 마지막 상태를 판단해서 반환
            # (성공 시점에서 self._waiting_active False가 되고 바로 SIG 0 후 종료되므로 True 반환)
            # 이를 추적하려면 플래그를 두자
            return getattr(self, "_last_wait_success", False)
        finally:
            # ✅ IG는 베이스 압력 판정이 끝나면 곧바로 연결과 태스크를 정리한다.
            await self.cleanup()

    def cancel_wait(self):
        """
        사용자 STOP 대응:
         - waiting 플래그 내리고,
         - 폴링 중지,
         - 큐/인플라이트 취소,
         - SIG 0 한 번 보내고 끝(응답 무시)
        """
        self._waiting_active = False
        self._suspend_reignite = True            # ✅ 재점등 금지
        self._total_reignite_attempts = 0
        if self._polling_task:
            self._polling_task.cancel()
            self._polling_task = None
        self._purge_pending("user cancel / stop")
        # SIG 0은 응답 무시
        self.enqueue("SIG 0", on_reply=None, timeout_ms=IG_TIMEOUT_MS, gap_ms=150,
                     tag="[IG OFF] SIG 0 (cancel)", retries_left=0, allow_no_reply=True)

    async def events(self) -> AsyncGenerator[IGEvent, None]:
        """
        상태/압력/성공/실패 이벤트를 비동기 제너레이터로 전달.
        """
        while True:
            ev = await self._event_q.get()
            yield ev

    # ---------------------------
    # 내부: 연결/워치독
    # ---------------------------
    async def _watchdog_loop(self):
        """포트가 닫혀 있고 연결 의도가 있으면 지수 백오프로 재연결."""
        backoff = self._reconnect_backoff_ms
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(IG_WATCHDOG_INTERVAL_MS / 1000.0)
                continue

            await self._emit_status(f"재연결 시도 예약... ({backoff} ms)")
            await asyncio.sleep(backoff / 1000.0)

            if not self._want_connected or self._connected:
                continue

            try:
                loop = asyncio.get_running_loop()
                transport, protocol = await serial_asyncio.create_serial_connection(
                    loop, lambda: _IGProtocol(self), IG_PORT, baudrate=IG_BAUD
                )
                # 성공
                self._transport = transport
                self._protocol = protocol  # type: ignore
                self._connected = True
                backoff = IG_RECONNECT_BACKOFF_START_MS
                await self._emit_status(f"{IG_PORT} 연결 성공 (asyncio)")
                # 포트 열리면 pending 명령 송신은 워커가 처리
            except Exception as e:
                await self._emit_status(f"{IG_PORT} 연결 실패: {e}")
                backoff = min(backoff * 2, IG_RECONNECT_BACKOFF_MAX_MS)

    def _on_connection_made(self, transport: asyncio.Transport):
        # 이미 watchdog에서 플래그 처리
        pass

    def _on_connection_lost(self, exc: Optional[Exception]):
        self._connected = False
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
        self._transport = None
        self._protocol = None
        self._dbg("IG", f"연결 끊김: {exc}")
        # 인플라이트 명령 재시도/취소 정책
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

    def _on_line_from_serial(self, line: str):
        # 워커가 소비
        try:
            self._line_q.put_nowait(line)
        except asyncio.QueueFull:
            self._dbg("IG", "라인 큐가 가득 찼습니다. 가장 오래된 라인을 폐기합니다.")
            try:
                _ = self._line_q.get_nowait()
            except Exception:
                pass
            try:
                self._line_q.put_nowait(line)
            except Exception:
                pass

    # ---------------------------
    # 내부: 명령 워커/송수신
    # ---------------------------
    async def _cmd_worker_loop(self):
        """단일 직렬 명령 처리 루프."""
        while True:
            # cancel 지원
            await asyncio.sleep(0)
            if not self._cmd_q:
                await asyncio.sleep(0.01)
                continue
            if not self._connected or not self._transport:
                # 연결될 때까지 대기
                await asyncio.sleep(0.05)
                continue

            cmd = self._cmd_q.popleft()
            self._inflight = cmd
            sent_txt = cmd.cmd_str.strip()
            self._dbg("IG", f"[SEND] {sent_txt} (tag={cmd.tag})")

            try:
                payload = cmd.cmd_str.encode("ascii")
                self._transport.write(payload)
                await self._transport.drain() if hasattr(self._transport, "drain") else None
            except Exception as e:
                # 전송 실패 → 재연결 유도
                self._dbg("IG", f"{cmd.tag} {sent_txt} 전송 오류: {e}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                # 연결 강제 종료 → 워치독 재연결
                if self._transport:
                    try: self._transport.close()
                    except Exception: pass
                self._connected = False
                continue

            # 응답 필요 없으면 gap만 지키고 다음
            if cmd.allow_no_reply:
                self._safe_callback(cmd.callback, None)
                self._inflight = None
                await asyncio.sleep(cmd.gap_ms / 1000.0)
                continue

            # 응답 대기 (에코 스킵)
            try:
                line = await self._read_one_line_skip_echo(sent_txt, cmd.timeout_ms / 1000.0)
            except asyncio.TimeoutError:
                # 타임아웃
                self._dbg("IG", f"[TIMEOUT] {cmd.tag} {sent_txt}")
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._dbg("IG", f"{cmd.tag} {sent_txt} 재시도 남은횟수={cmd.retries_left}")
                    self._cmd_q.appendleft(cmd)
                    # 재연결 유도
                    if self._transport:
                        try: self._transport.close()
                        except Exception: pass
                    self._connected = False
                else:
                    self._safe_callback(cmd.callback, None)
                    await asyncio.sleep(cmd.gap_ms / 1000.0)
                continue

            # 정상 수신
            recv_txt = (line or "").strip()
            self._dbg("IG < 응답", f"{cmd.tag} {sent_txt} ← {recv_txt}")
            self._safe_callback(cmd.callback, recv_txt)
            self._inflight = None
            await asyncio.sleep(cmd.gap_ms / 1000.0)

    async def _read_one_line_skip_echo(self, sent_no_cr: str, timeout_s: float) -> str:
        """라인 큐에서 다음 라인을 읽되, 전송 에코와 빈 라인은 스킵."""
        deadline = time.monotonic() + timeout_s
        while True:
            remain = max(0.0, deadline - time.monotonic())
            if remain <= 0:
                raise asyncio.TimeoutError()
            line = await asyncio.wait_for(self._line_q.get(), timeout=remain)
            if not line:
                continue
            # 공백/대소문자/양끝 제어문자 차이로 인한 오검출 방지
            if line.strip().upper() == sent_no_cr.strip().upper():
                continue
            return line

    # ---------------------------
    # 내부: Base Pressure 흐름
    # ---------------------------
    async def _poll_rdi_loop(self, interval_ms: int):
        """주기적 RDI 폴링."""
        try:
            while self._waiting_active:
                # 타임아웃은 개별 명령에 부여
                line = await self._send_and_wait_line("RDI", tag="[POLL RDI]", timeout_ms=IG_TIMEOUT_MS)
                if not self._waiting_active:
                    break
                await self._handle_rdi_line(line)

                # 전체 대기 시간 초과 판정
                if (time.monotonic() - self._wait_start_s) > float(IG_WAIT_TIMEOUT):
                    await self._emit_status(f"시간 초과({IG_WAIT_TIMEOUT}초): 목표 압력 미도달")
                    await self._emit_failed("Timeout")
                    self._waiting_active = False
                    # IG OFF 후 종료
                    await self._sig0_off_ignore_reply()  # 응답은 굳이 기다리지 않음
                    break

                await asyncio.sleep(max(0.0, interval_ms / 1000.0))
        except asyncio.CancelledError:
            pass

    async def _handle_rdi_line(self, line: Optional[str]):
        """RDI 응답 처리(IG OFF → 재점등, 파싱, 도달 판정)."""
        if not self._waiting_active:
            return

        s = (line or "").strip()
        if not s:
            return  # 빈 응답 → 다음 주기에 재시도

        if s.upper() == "IG OFF":
            # ✅ 종료/취소 중에는 재점등 금지
            if not self._waiting_active or self._suspend_reignite:
                return

            # ✅ 재점등 총 횟수 상한 (config에서 관리)
            if self._total_reignite_attempts >= int(IG_REIGNITE_MAX_ATTEMPTS):
                await self._emit_status(
                    f"IG OFF 응답 반복 → 자동 재점등 중단(상한 {IG_REIGNITE_MAX_ATTEMPTS}회 초과). 폴링만 유지"
                )
                return

            self._total_reignite_attempts += 1
            await self._emit_status(
                f"IG OFF 응답 감지 → 자동 재점등 시도({self._total_reignite_attempts}/{IG_REIGNITE_MAX_ATTEMPTS})"
            )

            # 폴링 태스크 루프 내에서는 다음 콜로 재개
            ok = await self._try_re_on_with_backoff()
            if ok:
                await self._emit_status("재점등 성공. 첫 RDI 후 폴링 재개")
                await asyncio.sleep(self._first_read_delay_ms / 1000.0)
                # 즉시 한 번 더 읽어 최신화
                line2 = await self._send_and_wait_line("RDI", tag="[AFTER RE-ON]", timeout_ms=IG_TIMEOUT_MS)
                await self._handle_rdi_line(line2)
            else:
                await self._emit_status("자동 재점등 실패(한도 도달). 폴링만 재개")
            return

        # 숫자 파싱 (예: '1.2x10e-5' → '1.2e-5')
        cleaned = s.lower().replace("x10e", "e")
        try:
            pressure = float(cleaned)
        except Exception:
            await self._emit_status(f"압력 읽기 실패(파싱): {repr(s)}")
            return

        await self._emit_pressure(pressure)

        if pressure <= self._target_pressure:
            await self._emit_status("목표 압력 도달")
            await self._emit_base_reached()
            self._waiting_active = False
            self._last_wait_success = True
            # IG OFF 후 종료
            await self._sig0_off_ignore_reply()

    async def _try_re_on_with_backoff(self) -> bool:
        """IG OFF → SIG 1 재점등; config의 IG_REIGNITE_BACKOFF_MS 적용."""
        # ✅ 종료/취소 중이면 즉시 중단
        if not self._waiting_active or self._suspend_reignite:
            return False

        # ms 리스트를 초로 변환
        delays_s = [max(0, int(ms)) / 1000.0 for ms in IG_REIGNITE_BACKOFF_MS] or [2.0, 5.0, 10.0]

        for sec in delays_s:
            if not self._waiting_active or self._suspend_reignite:
                return False
            ok = await self._send_and_expect_ok("SIG 1", tag="[IG RE-ON]", retries=3)
            if ok:
                return True
            await self._emit_status(f"재점등 실패 → {int(sec*1000)}ms 후 재시도")
            await asyncio.sleep(sec)

        return False

    async def _send_and_expect_ok(self, cmd: str, *, tag: str, retries: int) -> bool:
        """cmd 송신 후 'OK'로 시작하는 응답을 기대."""
        for i in range(max(1, int(retries))):
            line = await self._send_and_wait_line(cmd, tag=tag, timeout_ms=IG_TIMEOUT_MS, retries=0)
            if (line or "").strip().upper().startswith("OK"):
                return True
            # 'OK'가 아니면 포트 닫고 워치독이 재연결 시도
            if self._transport:
                try: self._transport.close()
                except Exception: pass
            self._connected = False
            # 다음 루프에서 워치독이 다시 연결하고 우리는 다시 시도
        return False

    async def _send_and_wait_line(self, cmd: str, *, tag: str, timeout_ms: int, retries: int = 1) -> Optional[str]:
        """
        큐를 통해 cmd를 보내고 한 줄 응답을 기다린다(에코 스킵).
        retries: 타임아웃 시 재시도 횟수(타임아웃에만 적용).
        """
        fut: asyncio.Future[Optional[str]] = asyncio.get_running_loop().create_future()

        def _cb(line: Optional[str]):
            if not fut.done():
                fut.set_result(line)

        self.enqueue(
            cmd, _cb, timeout_ms=timeout_ms, gap_ms=IG_GAP_MS, tag=tag,
            retries_left=max(0, int(retries)), allow_no_reply=False
        )
        try:
            return await asyncio.wait_for(fut, timeout=(timeout_ms / 1000.0) + 2.0)
        except asyncio.TimeoutError:
            return None

    async def _sig0_off_ignore_reply(self):
        """IG OFF(SIG 0)를 보내되 응답은 기다리지 않음."""
        self.enqueue("SIG 0", on_reply=None, timeout_ms=IG_TIMEOUT_MS, gap_ms=150,
                     tag="[IG OFF] SIG 0", retries_left=3, allow_no_reply=True)

    # ---------------------------
    # 내부: 큐/콜백/로깅/이벤트
    # ---------------------------
    def _purge_pending(self, reason: str = "") -> int:
        """인플라이트/대기 명령을 모두 취소하고 버퍼 큐를 비움."""
        purged = 0
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            purged += 1
            self._safe_callback(cmd.callback, None)

        while self._cmd_q:
            cmd = self._cmd_q.popleft()
            purged += 1
            self._safe_callback(cmd.callback, None)

        # 라인 큐 비우기
        try:
            while True:
                self._line_q.get_nowait()
        except Exception:
            pass

        self._dbg("IG", f"대기 중 명령 {purged}개 폐기 ({reason})")
        return purged

    def _safe_callback(self, cb: Optional[Callable[[Optional[str]], None]], arg: Optional[str]):
        if cb is None:
            return
        try:
            cb(arg)
        except Exception as e:
            self._dbg("IG", f"콜백 오류: {e}")

    async def _emit_status(self, msg: str):
        if self.debug_print:
            print(f"[IG][status] {msg}")
        try:
            await self._event_q.put(IGEvent(kind="status", message=msg))
        except Exception:
            pass

    async def _emit_pressure(self, p: float):
        if self.debug_print:
            print(f"[IG][pressure] {p:.3e}")
        try:
            await self._event_q.put(IGEvent(kind="pressure", pressure=p))
        except Exception:
            pass

    async def _emit_base_reached(self):
        try:
            await self._event_q.put(IGEvent(kind="base_reached"))
        except Exception:
            pass

    async def _emit_failed(self, why: str):
        try:
            await self._event_q.put(IGEvent(kind="base_failed", message=why))
        except Exception:
            pass

    def _dbg(self, src: str, msg: str):
        if self.debug_print:
            print(f"[{src}] {msg}")
