# -*- coding: utf-8 -*-
"""
ig.py — asyncio 기반 IG(이온/진공 게이지) 컨트롤러 (MOXA NPort TCP Server 직결)

의존성: 표준 라이브러리만 사용 (pyserial 불필요)

설계 요점:
  - asyncio Streams + TCP 라인 리더( CR/LF 프레이밍 )
  - 단일 명령 큐(타임아웃/재시도/인터커맨드 gap)
  - 워치독(지수 백오프) 자동 재연결
  - 시퀀스: SIG 1 → 첫 RDI 지연 → 1회 읽기 → 폴링
  - 'IG OFF' 응답 시 자동 재점등(백오프), Base 도달 시 SIG 0
"""


from __future__ import annotations
from dataclasses import dataclass
from collections import deque
from typing import Optional, Callable, Deque, AsyncGenerator, Literal
import asyncio, time, contextlib, socket

from lib.config_ch2 import (
    IG_TCP_HOST, IG_TCP_PORT, IG_TX_EOL, IG_SKIP_ECHO, IG_TIMEOUT_MS, IG_GAP_MS, IG_CONNECT_TIMEOUT_S,
    IG_POLLING_INTERVAL_MS, IG_WATCHDOG_INTERVAL_MS, IG_RECONNECT_BACKOFF_START_MS, 
    IG_RECONNECT_BACKOFF_MAX_MS, IG_REIGNITE_MAX_ATTEMPTS, IG_REIGNITE_BACKOFF_MS, IG_WAIT_TIMEOUT
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
#  IG asyncio 컨트롤러
# =========================
class AsyncIG:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None):
        # 채널별 오버라이드(없으면 config 기본 사용)
        self._override_host: Optional[str] = host
        self._override_port: Optional[int] = port

        # 연결/프로토콜
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._tx_eol: bytes = IG_TX_EOL
        self._tx_eol_str: str = IG_TX_EOL.decode("ascii", "ignore")  # ← 1회만 디코드
        self._skip_echo: bool = bool(IG_SKIP_ECHO)

        self._connected: bool = False         # ← 누락되어 있던 상태 플래그 추가
        self._ever_connected: bool = False

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

        # 베이스 압력 대기
        self._target_pressure = 0.0
        self._waiting_active = False
        self._wait_start_s = 0.0

        self._first_read_delay_ms = 5000  # IG ON OK 후 첫 RDI 전 지연(1회)

        # ✅ 재점등(자동 ON 재시도) 제어 플래그/카운터
        self._suspend_reignite: bool = False     # 종료/취소 중 재점등 금지
        self._total_reignite_attempts: int = 0   # 누적 재점등 횟수
        self._last_wait_success: bool = False

        # ✅ 최근 읽은 압력과 폴링 인터벌(로그용)
        self._last_pressure: Optional[float] = None
        self._poll_interval_ms: int = IG_POLLING_INTERVAL_MS

    # ---------------------------
    # 공용 API
    # ---------------------------
    async def start(self):
        """워치독/커맨드 워커 시작. (연결은 워치독이 담당)"""
        # 죽은 태스크 정리
        if self._watchdog_task and self._watchdog_task.done():
            self._watchdog_task = None
        if self._cmd_worker_task and self._cmd_worker_task.done():
            self._cmd_worker_task = None

        if self._watchdog_task and self._cmd_worker_task:
            return
        
        self._want_connected = True
        loop = asyncio.get_running_loop()
        if not self._watchdog_task:
            self._watchdog_task = loop.create_task(self._watchdog_loop(), name="IGWatchdog")
        if not self._cmd_worker_task:
            self._cmd_worker_task = loop.create_task(self._cmd_worker_loop(), name="IGCmdWorker")
        #await self._emit_status("IG 워치독/워커 시작")

    async def cleanup(self):
        # OFF 보내기 전에 재점등 금지/대기 해제만
        self._waiting_active = False
        self._suspend_reignite = True

        # 1) OFF direct-write 보장 + 짧은 드레인
        try:
            _ = await self._send_off_best_effort(wait_gap_ms=400)
            await self._drain_until_idle(timeout_ms=600)  # fallback enqueue 대비
        except Exception:
            pass

        # 이제 재연결 시도는 중단
        self._want_connected = False

        # 2) 폴링 태스크 중지
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except Exception:
                pass
            self._polling_task = None

        # 3) 커맨드 워커 중지
        if self._cmd_worker_task:
            self._cmd_worker_task.cancel()
            try:
                await self._cmd_worker_task
            except Exception:
                pass
            self._cmd_worker_task = None

        # 4) 워치독 중지
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except Exception:
                pass
            self._watchdog_task = None

        # 5) 큐/라인 비우기
        self._purge_pending("shutdown")

        # 6) TCP 종료
        await self._on_tcp_disconnected()

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
        if not cmd_str.endswith(self._tx_eol_str):
            cmd_str += self._tx_eol_str
        self._cmd_q.append(Command(cmd_str, on_reply, timeout_ms, gap_ms, tag, retries_left, allow_no_reply))

    async def wait_for_base_pressure(self, base_pressure: float, interval_ms: int = IG_POLLING_INTERVAL_MS) -> bool:
        """
        IG를 켜고(SIG 1) 목표 압력에 도달할 때까지 폴링(RDI) 후, 도달하면 SIG 0로 끄고 True를 반환.
        시간 초과 시 SIG 0 후 False.
        (진행 중 이벤트는 events() 제너레이터로도 전달)
        """
        if self._waiting_active:
            await self._emit_status("이미 Base Pressure 대기 중입니다.")
            return False

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
            await self._send_off_best_effort(wait_gap_ms=200)  # direct-write 경로 사용
            self._waiting_active = False
            return False

        # 첫 RDI 지연 후 1회 읽기
        await self._emit_status(f"IG ON OK → 첫 RDI를 {self._first_read_delay_ms}ms 후 수행")
        await asyncio.sleep(self._first_read_delay_ms / 1000.0)

        # ⬇️ 보냄/대기/타임아웃을 상태 로그로 노출
        await self._emit_status(f"[FIRST READ] RDI 송신 및 응답 대기(최대 {IG_TIMEOUT_MS}ms)")
        line = await self._send_and_wait_line("RDI", tag="[FIRST READ AFTER ON]", timeout_ms=IG_TIMEOUT_MS)


        if line is None:
            await self._emit_status("[FIRST READ] RDI 타임아웃 → 재연결 트리거")
            await self._on_tcp_disconnected()
            # 계속 진행(아래 handle은 None이면 조용히 리턴)
        if not self._waiting_active:
            return False

        await self._handle_rdi_line(line)

        # 폴링 시작 전, 인터벌 저장
        self._poll_interval_ms = int(interval_ms)

        # 폴링 시작
        if self._waiting_active:
            await self._log_wait_again(self._poll_interval_ms)
            self._polling_task = asyncio.create_task(self._poll_rdi_loop(interval_ms))

        try:
            # 하드 타임아웃: 내부 timeout + 첫 지연 + 여유
            try:
                limit_s = float(IG_WAIT_TIMEOUT)
            except Exception:
                limit_s = 120.0
            hard_deadline = self._wait_start_s + limit_s + (self._first_read_delay_ms/1000.0) + 5.0

            while self._waiting_active:
                # 폴링 태스크가 예기치 않게 종료했는지 감시
                if self._polling_task and self._polling_task.done() and self._waiting_active:
                    err = None
                    try:
                        err = self._polling_task.exception()
                    except Exception:
                        pass
                    await self._emit_status(f"폴링 태스크 조기 종료: {repr(err)}")
                    await self._emit_failed("PollingTaskExited")
                    self._waiting_active = False
                    try:
                        await self._send_off_best_effort(wait_gap_ms=200)
                    except Exception:
                        pass
                    break

                # 최종 안전망: 하드 타임아웃
                if time.monotonic() > hard_deadline:
                    await self._emit_status("하드 타임아웃: 상위 가드에 의해 종료")
                    await self._emit_failed("HardTimeout")
                    self._waiting_active = False
                    try:
                        await self._send_off_best_effort(wait_gap_ms=200)
                    except Exception:
                        pass
                    break

                await asyncio.sleep(0.05)

            return getattr(self, "_last_wait_success", False)
        finally:
            await self.cleanup()

    async def cancel_wait(self):
        self._waiting_active = False
        self._suspend_reignite = True
        self._total_reignite_attempts = 0
        if self._polling_task:
            self._polling_task.cancel()
            self._polling_task = None

        # 대기/인플라이트 명령은 정리
        self._purge_pending("user cancel / stop")

        # 연결 상태와 무관하게 여기서 '직접' OFF 보장 경로를 기다림
        try:
            await self._send_off_best_effort(wait_gap_ms=300)
        except asyncio.CancelledError:
            # 종료 중 취소되면 조용히 상위로 전파
            raise
        except Exception:
            # OFF 보장은 best-effort이므로 조용히 무시 가능(로그는 emit_status 안에서 남음)
            pass

    async def events(self) -> AsyncGenerator[IGEvent, None]:
        """
        상태/압력/성공/실패 이벤트를 비동기 제너레이터로 전달.
        """
        while True:
            ev = await self._event_q.get()
            yield ev

    def set_endpoint(self, host: str, port: int) -> None:
        """런타임에서 채널별 IG TCP 엔드포인트 지정."""
        self._override_host = str(host)
        self._override_port = int(port)

    def _resolve_endpoint(self) -> tuple[str, int]:
        """최종 접속 host/port 결정: override > config 기본값."""
        host = self._override_host if self._override_host else IG_TCP_HOST
        port = self._override_port if self._override_port else IG_TCP_PORT
        return str(host), int(port)


    # ---------------------------
    # 내부: 연결/워치독
    # ---------------------------
    async def _watchdog_loop(self):
        backoff = IG_RECONNECT_BACKOFF_START_MS
        while self._want_connected:
            if self._connected:
                await asyncio.sleep(IG_WATCHDOG_INTERVAL_MS / 1000.0)
                continue

            if self._ever_connected:
                await self._emit_status(f"재연결 시도 예약... ({backoff} ms)")
                await asyncio.sleep(backoff / 1000.0)

            if not self._want_connected:
                break

            try:
                host, port = self._resolve_endpoint()
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=max(0.5, float(IG_CONNECT_TIMEOUT_S))
                )
                self._reader, self._writer = reader, writer
                self._connected = True
                self._ever_connected = True
                backoff = IG_RECONNECT_BACKOFF_START_MS

                # (선택) TCP Keepalive 활성화
                try:
                    sock = writer.get_extra_info("socket")
                    if sock is not None:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                except Exception:
                    pass

                # 라인 수신 루프 시작
                if self._reader_task and not self._reader_task.done():
                    self._reader_task.cancel()
                    with contextlib.suppress(Exception):
                        await self._reader_task
                self._reader_task = asyncio.create_task(self._tcp_reader_loop(), name="IGTcpReader")

                await self._emit_status(f"{host}:{port} 연결 성공 (TCP)")
            except Exception as e:
                host, port = self._resolve_endpoint()
                await self._emit_status(f"{host}:{port} 연결 실패: {e}")
                backoff = min(backoff * 2, IG_RECONNECT_BACKOFF_MAX_MS)

    async def _tcp_reader_loop(self):
        assert self._reader is not None
        buf = bytearray()
        RX_MAX = 16 * 1024
        LINE_MAX = 512
        try:
            while self._connected and self._reader:
                chunk = await self._reader.read(128)
                if not chunk:
                    break
                buf.extend(chunk)
                if len(buf) > RX_MAX:
                    del buf[:-RX_MAX]
                    await self._emit_status(f"수신 버퍼 과다(RX>{RX_MAX}); 최근 {RX_MAX}B만 보존")

                # CR/LF 라인 파싱
                while True:
                    i_cr = buf.find(b"\r")
                    i_lf = buf.find(b"\n")
                    if i_cr == -1 and i_lf == -1:
                        break
                    idx = i_cr if i_lf == -1 else (i_lf if i_cr == -1 else min(i_cr, i_lf))
                    line_bytes = buf[:idx]
                    drop = idx + 1
                    if drop < len(buf):
                        ch = buf[idx]
                        nxt = buf[idx + 1]
                        if (ch == 13 and nxt == 10) or (ch == 10 and nxt == 13):
                            drop += 1
                    del buf[:drop]

                    if len(line_bytes) > LINE_MAX:
                        line_bytes = line_bytes[:LINE_MAX]

                    try:
                        line = line_bytes.decode("ascii", "ignore").strip()
                    except Exception:
                        line = ""
                    if line:
                        self._on_line_from_tcp(line)

                while buf[:1] in (b"\r", b"\n"):
                    del buf[0:1]
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self._emit_status(f"리더 루프 예외: {e!r}")
        finally:
            await self._on_tcp_disconnected()

    async def _on_tcp_disconnected(self):
        self._connected = False
        if self._reader_task:
            self._reader_task.cancel()
        self._reader_task = None
        if self._writer:
            with contextlib.suppress(Exception):
                self._writer.close()
                await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        # 라인 큐 비우기
        try:
            while True:
                self._line_q.get_nowait()
        except Exception:
            pass

        await self._emit_status("IG TCP 연결 끊김")

        # 인플라이트 재시도/취소
        if self._inflight is not None:
            cmd = self._inflight
            self._inflight = None
            if cmd.retries_left > 0:
                cmd.retries_left -= 1
                self._cmd_q.appendleft(cmd)
            else:
                self._safe_callback(cmd.callback, None)

    def _on_line_from_tcp(self, line: str):
        try:
            self._line_q.put_nowait(line)
        except asyncio.QueueFull:
            self._log_status_nowait("라인 큐 포화 → 가장 오래된 라인 폐기")
            with contextlib.suppress(Exception):
                _ = self._line_q.get_nowait()
            with contextlib.suppress(Exception):
                self._line_q.put_nowait(line)

    # ---------------------------
    # 내부: 명령 워커/송수신
    # ---------------------------
    async def _cmd_worker_loop(self):
        """단일 명령 처리 루프(TCP Streams)."""
        while True:
            # cancel 지원
            await asyncio.sleep(0)
            if not self._cmd_q:
                await asyncio.sleep(0.01)
                continue
            if not self._connected or not self._writer:
                await asyncio.sleep(0.05)
                continue

            cmd = self._cmd_q.popleft()
            self._inflight = cmd
            sent_txt = cmd.cmd_str.strip()
            await self._emit_status(f"[SEND] {sent_txt} (tag={cmd.tag})")

            try:
                await self._absorb_late_lines(30)
                payload = cmd.cmd_str.encode("ascii", "ignore")
                self._writer.write(payload)
                await self._writer.drain()
            except Exception as e:
                self._inflight = None
                await self._emit_status(f"[SEND-ERROR] {cmd.tag} {sent_txt} 전송 오류: {e!r}")
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    self._cmd_q.appendleft(cmd)
                else:
                    self._safe_callback(cmd.callback, None)
                await self._on_tcp_disconnected()
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
                self._inflight = None
                if cmd.retries_left > 0:
                    cmd.retries_left -= 1
                    await self._emit_status(f"[TIMEOUT] {cmd.tag} {sent_txt} / 재시도 남은횟수={cmd.retries_left}")
                    self._cmd_q.appendleft(cmd)
                    await self._on_tcp_disconnected()
                else:
                    await self._emit_status(f"[TIMEOUT] {cmd.tag} {sent_txt} / 재시도 소진")
                    self._safe_callback(cmd.callback, None)
                    await asyncio.sleep(cmd.gap_ms / 1000.0)
                continue

            # 정상 수신
            recv_txt = (line or "").strip()
            await self._emit_status(f"[RECV] {cmd.tag} {sent_txt} ← {recv_txt}")
            self._safe_callback(cmd.callback, recv_txt)
            self._inflight = None
            await asyncio.sleep(cmd.gap_ms / 1000.0)

    async def _read_one_line_skip_echo(self,sent_no_cr: str, timeout_s: float) -> str:
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
            if self._skip_echo and line.strip().upper() == sent_no_cr.strip().upper():
                continue
            return line

    # ---------------------------
    # 내부: Base Pressure 흐름
    # ---------------------------
    async def _poll_rdi_loop(self, interval_ms: int):
        """주기적 RDI 폴링(미도달 시 대기→재시도, 예외 안전)."""
        try:
            # IG_WAIT_TIMEOUT이 비정상이어도 안전하게 숫자로
            try:
                wait_limit_s = float(IG_WAIT_TIMEOUT)
            except Exception:
                wait_limit_s = 120.0  # 합리적 기본값

            while self._waiting_active:
                if not self._connected:
                    await asyncio.sleep(max(0.0, interval_ms / 1000.0))
                    continue

                # 1) RDI 1회 시도 (타임아웃은 per-command)
                line = await self._send_and_wait_line("RDI", tag="[POLL RDI]", timeout_ms=IG_TIMEOUT_MS)
                if not self._waiting_active:
                    break

                # 2) 처리: 도달/IG OFF/파싱 실패/미도달 모두 내부에서 결정
                await self._handle_rdi_line(line)
                if not self._waiting_active:
                    break  # 도달 또는 실패 처리로 종료된 경우

                # 미도달 안내 로그 추가
                await self._log_wait_again(interval_ms)

                # 3) 전체 대기 시간 초과 → 실패로 종료
                if (time.monotonic() - self._wait_start_s) > wait_limit_s:
                    await self._emit_status(f"시간 초과({wait_limit_s:.1f}s): 목표 압력 미도달")
                    await self._emit_failed("Timeout")
                    self._waiting_active = False
                    await self._send_off_best_effort(wait_gap_ms=200)
                    break

                # 4) 목표 미도달이면 interval 만큼 기다렸다 다음 RDI 반복
                await asyncio.sleep(max(0.0, interval_ms / 1000.0))

        except asyncio.CancelledError:
            # 정상 취소
            pass
        except Exception as e:
            # 조용히 죽지 않도록 실패 전환
            await self._emit_status(f"폴링 태스크 예외: {e!r}")
            await self._emit_failed("InternalError")
            self._waiting_active = False
            try:
                await self._send_off_best_effort(wait_gap_ms=200)
            except Exception:
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
            self._suspend_reignite = True
            # IG OFF 보장 후 종료
            await self._send_off_best_effort(wait_gap_ms=200)

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
            # 'OK'가 아니면 재연결 트리거
            await self._on_tcp_disconnected()
            # 다음 루프에서 워치독이 다시 연결하고 우리는 다시 시도
        return False

    async def _send_and_wait_line(self, cmd: str, *, tag: str, timeout_ms: int, retries: int = 1) -> Optional[str]:
        """
        cmd를 큐로 보내고 한 줄 응답을 기다린다(에코 스킵).
        재시도는 이 함수가 직접 수행하며, 워커에는 retries_left=0으로 넣는다.
        """
        attempts = max(0, int(retries)) + 1
        for attempt in range(attempts):
            fut: asyncio.Future[Optional[str]] = asyncio.get_running_loop().create_future()

            def _cb(line: Optional[str]):
                if not fut.done():
                    fut.set_result(line)

            # 워커 쪽 재시도는 끈다(retries_left=0)
            self.enqueue(
                cmd, _cb, timeout_ms=timeout_ms, gap_ms=IG_GAP_MS,
                tag=tag, retries_left=0, allow_no_reply=False
            )
            try:
                # 워커 타임아웃 + 약간의 마진(네고 가능)
                return await asyncio.wait_for(fut, timeout=(timeout_ms / 1000.0) + 0.5)
            except asyncio.TimeoutError:
                await self._emit_status(f"{tag} '{cmd}' 응답 타임아웃({timeout_ms}ms)")
                if attempt < attempts - 1:
                    await self._on_tcp_disconnected()
                    await asyncio.sleep(0)
                    continue
                return None

    async def _send_off_best_effort(self, wait_gap_ms: int = 300) -> bool:
        # 1) 온라인: 현재 writer 로 바로 송신
        if self._connected and self._writer:
            try:
                self._writer.write(b"SIG 0" + self._tx_eol)
                await self._writer.drain()
                await asyncio.sleep(max(0, wait_gap_ms) / 1000.0)
                return True
            except Exception as e:
                await self._emit_status(f"IG OFF 전송 실패(online): {e!r}")

        # 2) 오프라인: 임시 TCP one-shot
        try:
            # 2) 오프라인: 임시 TCP one-shot
            host, port = self._resolve_endpoint()
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=max(0.5, float(IG_CONNECT_TIMEOUT_S))
            )
            writer.write(b"SIG 0" + self._tx_eol)
            await writer.drain()
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            await asyncio.sleep(max(0, wait_gap_ms) / 1000.0)
            return True
        except Exception as e:
            await self._emit_status(f"직접 OFF 전송 실패({host}:{port} one-shot): {e!r}")
            return False

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

        # 동기 구간: 즉시 status 푸시
        self._log_status_nowait(f"대기 중 명령 {purged}개 폐기 ({reason})")
        return purged

    def _safe_callback(self, cb: Optional[Callable[[Optional[str]], None]], arg: Optional[str]):
        if cb is None:
            return
        try:
            cb(arg)
        except Exception:
            pass

    def _q_put_event_nowait(self, ev: IGEvent):
        """이벤트 큐가 가득 차면 가장 오래된 항목을 버리고 새 이벤트를 넣는다."""
        try:
            self._event_q.put_nowait(ev)
        except asyncio.QueueFull:
            try:
                _ = self._event_q.get_nowait()
            except Exception:
                pass
            try:
                self._event_q.put_nowait(ev)
            except Exception:
                pass

    def _log_status_nowait(self, msg: str) -> None:
        """await 불가한 동기 구간에서 status 이벤트 즉시 푸시"""
        self._q_put_event_nowait(IGEvent(kind="status", message=msg))

    async def _emit_status(self, msg: str):
        self._q_put_event_nowait(IGEvent(kind="status", message=msg))

    async def _emit_pressure(self, p: float):
        self._last_pressure = p
        self._q_put_event_nowait(IGEvent(kind="pressure", pressure=p))

    async def _emit_base_reached(self):
        self._q_put_event_nowait(IGEvent(kind="base_reached"))

    async def _emit_failed(self, why: str):
        self._q_put_event_nowait(IGEvent(kind="base_failed", message=why))

    # ---------------------------
    # 내부: 유틸
    # ---------------------------
    async def _drain_until_idle(self, timeout_ms: int = 300):
        """_inflight이 비고 큐가 빌 때까지 잠깐 대기(상한 시간 내)."""
        deadline = time.monotonic() + max(0, timeout_ms) / 1000.0
        while time.monotonic() < deadline:
            if self._inflight is None and not self._cmd_q:
                break
            await asyncio.sleep(0.01)

    async def _absorb_late_lines(self, budget_ms: int = 120):
        deadline = time.monotonic() + max(0, budget_ms) / 1000.0
        drained = 0
        while time.monotonic() < deadline:
            try:
                _ = self._line_q.get_nowait()
                drained += 1
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.005)
        if drained:
            await self._emit_status(f"이전 늦은 응답 {drained}개 흡수(다음 명령 오염 방지)")

    async def _log_wait_again(self, interval_ms: int):
        sec = max(0, int(interval_ms)) // 1000
        if self._last_pressure is not None:
            await self._emit_status(
                f"목표 미도달: 현재 {self._last_pressure:.3e} Torr > 목표 {self._target_pressure:.3e} Torr → {sec}초 후 재시도"
            )
        else:
            await self._emit_status(f"목표 미도달 → {sec}초 후 재시도")


    # === IGControllerLike 호환용 얇은 쉼(옵션) ===
    async def ensure_on(self) -> None:
        """
        IGControllerLike.ensure_on 구현:
        - 워치독/워커가 안 떠 있으면 start()
        - 'SIG 1'을 보내 'OK' 응답을 확인
        """
        # 워치독/워커 보장
        await self.start()

        ok = await self._send_and_expect_ok("SIG 1", tag="[ensure_on]", retries=5)
        if not ok:
            raise RuntimeError("SIG 1 failed (ensure_on)")

    async def ensure_off(self) -> None:
        """
        IGControllerLike.ensure_off 구현:
        - 응답을 기다리지 않고 best-effort로 SIG 0 보장
        """
        await self._send_off_best_effort(wait_gap_ms=300)

    async def read_pressure(self) -> float:
        """
        IGControllerLike.read_pressure 구현:
        - 'RDI' 1회 → 라인 파싱 → Torr(float) 반환
        """
        line = await self._send_and_wait_line("RDI", tag="[read_pressure]", timeout_ms=IG_TIMEOUT_MS)
        if not line:
            raise RuntimeError("RDI timeout")

        s = line.strip().lower().replace("x10e", "e")
        try:
            return float(s)
        except Exception:
            raise RuntimeError(f"parse error: {line!r}")



