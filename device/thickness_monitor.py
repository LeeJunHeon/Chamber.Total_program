# device/thickness_monitor.py
# -*- coding: utf-8 -*-
"""두께 모니터(Sycon STM-100/MF, QCM) asyncio 드라이버 — MOXA NPort TCP Server 경유 RS-232(9600-8-N-1). 표준 라이브러리만.

Sycon protocol(매뉴얼 5장):
  프레임 = STX(0x02) + LEN(1바이트, 데이터 길이) + ASCII 데이터 + CHK(데이터 바이트 합 & 0xFF). 응답도 같은 형식.
  장비가 먼저 보내는 메시지는 없다(질의-응답만). 체크섬/길이가 틀리면 장비는 아무 응답도 하지 않는다
  → 명령마다 타임아웃·재시도. 송신 전에 수신 버퍼를 비운다(늦게 온 이전 응답을 다음 명령의 응답으로 읽지 않게).
  응답 첫 글자 = 결과 코드: A 정상 / B 정상+전원리셋 플래그 / F,G 잘못된 명령 / H,I 잘못된 값 / J,K 잘못된 수식자
  (뒤쪽 글자가 리셋 플래그가 있는 쪽). 리셋 플래그가 보이면 'L' 로 확인하고 reset_seen 을 1 올린다.
  값 앞에 명령 글자가 다시 붙어 올 수 있다("M !", "U 5319234") → 떼고 해석한다.
  값 대신 빈값/"--------"/"    . " 가 올 수 있다(XTAL FAIL 중 S·T·U·V 등) → 통신 실패와 구분되는 '값 없음'
  (ThicknessValueUnavailable). 본문이 공백·'-'·'.' 만이면 값 없음, 숫자 일부만 빈칸('12 .4')이면 형식 오류.
  N/O/P(릴레이 상태)는 매뉴얼 표와 상세 설명이 서로 달라 쓰지 않는다.

연결: 워치독이 끊기면 지수 백오프로 다시 연결한다. quiet=True(서비스가 '장비 무응답' 으로 본 동안)면
연결·재연결·끊김 로그를 남기지 않는다(동작은 같음 — 몇 초마다 같은 줄이 쌓이지 않게). 수신은 전용 태스크가 버퍼에 쌓고(EOF 를 바로 알아챈다),
명령은 잠금 하나로 한 번에 하나만(FIFO) 주고받는다. 링크 유지(MOXA 60초 무통신 끊김)는 서비스의 상시 폴링이 맡는다.

예외 구분(서비스가 다르게 센다):
  ThicknessCommError        — 통신 실패(미연결·송신 실패·응답 없음). ThicknessTimeout 이 하위.
  ThicknessProtocolError    — 체크섬·길이·형식·모르는 코드. ThicknessFrameError(체크섬·길이·모르는 코드 — 프레임 자체가
                              틀림, ThicknessChecksumError 가 하위), ThicknessCommandRejected(F~K) 가 하위.
                              그 밖의 ThicknessProtocolError 는 프레임은 정상인데 내용 형식이 틀린 것.
  ThicknessValueUnavailable — 응답은 정상인데 값이 없음(빈값/"--------"/"    . ").
  ThicknessVerifyError      — set_material 되읽기 불일치(1회 재시도 뒤).
  범위 밖 값은 보내기 전에 ValueError.
"""
from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Callable, Optional

STX = 0x02
CMD_LEN_MAX = 10              # 송신 데이터 최대 길이(매뉴얼)
RX_LEN_MAX = 32               # 수신 LEN 허용 상한(매뉴얼은 10 — 펌웨어 차이에 너그럽게)
RX_BUF_MAX = 4096

OK_CODES = frozenset("AB")
RESET_CODES = frozenset("BGIK")   # 전원 리셋 플래그가 있는 쪽
CODE_MEANING = {
    "A": "정상", "B": "정상(전원 리셋 플래그)",
    "F": "잘못된 명령", "G": "잘못된 명령(전원 리셋 플래그)",
    "H": "잘못된 값", "I": "잘못된 값(전원 리셋 플래그)",
    "J": "잘못된 수식자", "K": "잘못된 수식자(전원 리셋 플래그)",
}

DENSITY_MIN, DENSITY_MAX = 0.500, 99.99
Z_MIN, Z_MAX = 0.100, 9.999
TOOLING_MIN, TOOLING_MAX = 10.0, 399.0
TOL_DENSITY, TOL_Z, TOL_TOOLING = 0.01, 0.002, 0.2

RETRY_GAP_S = 0.1             # 응답 없음 뒤 재송신 전 대기(늦게 오는 응답이 지나가게)
RESET_ACK_MIN_INTERVAL_S = 2.0

_INT_RE = re.compile(r"^[+-]?\d+$")
_NUM_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)$")


# ═══════════════════════ 예외 ═══════════════════════
class ThicknessError(Exception):
    """두께 모니터 오류 공통 부모."""


class ThicknessCommError(ThicknessError):
    """통신 실패 — 미연결·송신 실패·응답 없음."""


class ThicknessTimeout(ThicknessCommError):
    """명령 응답 없음(재시도 모두 소진)."""


class ThicknessProtocolError(ThicknessError):
    """프레임·체크섬·형식·코드 오류."""


class ThicknessFrameError(ThicknessProtocolError):
    """응답 프레임 자체가 틀림(체크섬·길이·모르는 결과 코드) — 통신 이상으로 센다."""


class ThicknessChecksumError(ThicknessFrameError):
    """수신 프레임 체크섬/길이 불일치 — 재시도 가능."""


class ThicknessCommandRejected(ThicknessProtocolError):
    """장비가 F~K(잘못된 명령/값/수식자)로 거부."""

    def __init__(self, code: str, cmd: str, body: str = ""):
        self.code, self.cmd, self.body = code, cmd, body
        super().__init__(f"{CODE_MEANING.get(code, '모르는 코드')}({code}) — 보낸 명령 {cmd!r}"
                         + (f", 응답 {body!r}" if body else ""))


class ThicknessValueUnavailable(ThicknessError):
    """응답은 정상인데 값이 없음(빈값/'--------'/'    . ')."""


class ThicknessVerifyError(ThicknessError):
    """set_material 되읽기 불일치."""


# ═══════════════════════ 프레임·파싱(순수 함수) ═══════════════════════
def checksum(data: bytes) -> int:
    return sum(data) & 0xFF


def build_frame(data_ascii: str) -> bytes:
    data = str(data_ascii).encode("ascii")
    if not (1 <= len(data) <= CMD_LEN_MAX):
        raise ValueError(f"명령 데이터는 1~{CMD_LEN_MAX}바이트여야 합니다: {data_ascii!r} ({len(data)}바이트)")
    return bytes([STX, len(data)]) + data + bytes([checksum(data)])


def extract_frame(buf: bytearray) -> Optional[str]:
    """buf 앞에서 프레임 하나를 꺼낸다(꺼낸 바이트는 buf 에서 지운다). 아직 덜 왔으면 None.
    STX 앞 쓰레기는 버린다. LEN 이 범위 밖이거나 체크섬이 틀리면 ThicknessChecksumError."""
    i = buf.find(bytes([STX]))
    if i < 0:
        buf.clear()
        return None
    if i > 0:
        del buf[:i]
    if len(buf) < 2:
        return None
    ln = buf[1]
    if not (1 <= ln <= RX_LEN_MAX):
        del buf[:1]
        raise ThicknessChecksumError(f"수신 LEN={ln} 범위 밖")
    need = 2 + ln + 1
    if len(buf) < need:
        return None
    data = bytes(buf[2:2 + ln])
    chk = buf[2 + ln]
    del buf[:need]
    if checksum(data) != chk:
        raise ThicknessChecksumError(f"체크섬 불일치 rx={data!r} chk={chk:#04x} 계산={checksum(data):#04x}")
    return data.decode("ascii", "replace")


def fmt_compact(x: float, decimals: int) -> str:
    """명령 데이터 10바이트 제한 — 1.230 → '1.23'."""
    s = f"{float(x):.{int(decimals)}f}".rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def strip_echo(body: str, token: str) -> str:
    """'U 5319234' → '5319234', 'M !' → '!'. 앞 글자가 명령 글자(영문)일 때만 뗀다."""
    s = (body or "").strip()
    t = (token or "")[:1]
    if s and t.isalpha() and s[:1].upper() == t.upper():
        s = s[1:].strip()
    return s


def _value_text(body: str, token: str) -> str:
    s = strip_echo(body, token)
    if set(s) <= {" ", "-", "."} or s.upper() in ("N/A", "NA"):     # 빈칸·'-'·'.' 만 = 값 없음(XTAL FAIL 등)
        raise ThicknessValueUnavailable(f"{token}: 값 없음({body!r})")
    return s


def parse_thickness_a(body: str) -> float:
    """S — 두께(Å, 부호 포함 정수)."""
    s = _value_text(body, "S")
    if not _INT_RE.fullmatch(s):
        raise ThicknessProtocolError(f"S: 두께 형식 오류 {body!r}")
    return float(int(s))


def parse_rate_a_s(body: str) -> float:
    """T — 증착률(Å/s, 부호+NNN.N)."""
    s = _value_text(body, "T")
    if not _NUM_RE.fullmatch(s):
        raise ThicknessProtocolError(f"T: 증착률 형식 오류 {body!r}")
    return float(s)


def parse_frequency_hz(body: str) -> int:
    """U — 센서 주파수(Hz)."""
    s = _value_text(body, "U")
    if not _INT_RE.fullmatch(s) or int(s) <= 0:
        raise ThicknessProtocolError(f"U: 주파수 형식 오류 {body!r}")
    return int(s)


def parse_life_pct(body: str) -> float:
    """V — 크리스탈 수명(%)."""
    s = _value_text(body, "V")
    if not _NUM_RE.fullmatch(s):
        raise ThicknessProtocolError(f"V: 수명 형식 오류 {body!r}")
    v = float(s)
    if not (0.0 <= v <= 100.0):
        raise ThicknessProtocolError(f"V: 수명 범위 밖 {body!r}")
    return v


def parse_crystal_ok(body: str) -> bool:
    """M — '@' 정상(True) / '!' FAIL(False)."""
    s = _value_text(body, "M")
    if s == "@":
        return True
    if s == "!":
        return False
    raise ThicknessProtocolError(f"M: 크리스탈 상태 형식 오류 {body!r}")


def parse_number(body: str, token: str) -> float:
    """E?/F?/J? 되읽기."""
    s = _value_text(body, token)
    if not _NUM_RE.fullmatch(s):
        raise ThicknessProtocolError(f"{token}?: 숫자 형식 오류 {body!r}")
    return float(s)


def check_material(density: float, z_factor: float, tooling: float):
    """보낼 문자열로 정규화하고 범위를 확인한다 — (d_str, z_str, t_str). 범위 밖이면 ValueError."""
    d_s, z_s, t_s = fmt_compact(density, 3), fmt_compact(z_factor, 3), fmt_compact(tooling, 1)
    for name, v, lo, hi in (("density", float(d_s), DENSITY_MIN, DENSITY_MAX),
                            ("Z-factor", float(z_s), Z_MIN, Z_MAX),
                            ("tooling", float(t_s), TOOLING_MIN, TOOLING_MAX)):
        if not (lo <= v <= hi):
            raise ValueError(f"{name}={v:g} 범위({lo:g}~{hi:g}) 밖 — 보내지 않음")
    return d_s, z_s, t_s


@dataclass
class Reply:
    code: str
    body: str
    raw: str

    @property
    def reset(self) -> bool:
        return self.code in RESET_CODES


# ═══════════════════════ 드라이버 ═══════════════════════
class AsyncThicknessMonitor:
    def __init__(self, host: str, port: int, *,
                 cmd_timeout_s: float = 1.0,
                 retries: int = 2,
                 connect_timeout_s: float = 3.0,
                 backoff_start_s: float = 1.0,
                 backoff_max_s: float = 30.0,
                 settle_s: float = 0.1,
                 log: Optional[Callable[[str], None]] = None,
                 clock: Callable[[], float] = time.monotonic):
        self.host, self.port = str(host), int(port)
        self.cmd_timeout_s = max(0.1, float(cmd_timeout_s))
        self.retries = max(0, int(retries))
        self.connect_timeout_s = max(0.5, float(connect_timeout_s))
        self.backoff_start_s = max(0.1, float(backoff_start_s))
        self.backoff_max_s = max(self.backoff_start_s, float(backoff_max_s))
        self.settle_s = max(0.0, float(settle_s))
        self._log_cb = log
        self._now = clock

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._rx = bytearray()
        self._rx_evt: Optional[asyncio.Event] = None
        self._rx_task: Optional[asyncio.Task] = None
        self._io_lock: Optional[asyncio.Lock] = None
        self._connected = False
        self._want = False
        self._wd_task: Optional[asyncio.Task] = None
        self._backoff = self.backoff_start_s
        self._next_try = 0.0
        self._conn_fail_logged = False
        self._ever_connected = False
        self.quiet = False                   # True 면 연결·재연결·끊김 로그 생략(서비스가 무응답 동안 켠다)

        self.reset_seen = 0                  # 전원 리셋 플래그를 새로 본 횟수(2단계: 공정 중 리셋 판정)
        self._reset_active = False
        self._reset_ack_t = -1e9
        self.last_rx_t: Optional[float] = None

    # ── 상태 ──
    @property
    def is_connected(self) -> bool:
        return bool(self._connected)

    def _log(self, msg: str) -> None:
        if self._log_cb:
            try:
                self._log_cb(msg)
            except Exception:
                pass

    def _lock(self) -> asyncio.Lock:
        if self._io_lock is None:
            self._io_lock = asyncio.Lock()
        return self._io_lock

    def _evt(self) -> asyncio.Event:
        if self._rx_evt is None:
            self._rx_evt = asyncio.Event()
        return self._rx_evt

    # ── 연결 ──
    async def start(self) -> None:
        """워치독 시작(연결은 워치독이 맡는다)."""
        self._want = True
        if self._wd_task is None or self._wd_task.done():
            self._wd_task = asyncio.get_running_loop().create_task(self._watchdog_loop(), name="ThicknessWatchdog")

    async def stop(self) -> None:
        self._want = False
        t = self._wd_task
        self._wd_task = None
        if t is not None and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(t, return_exceptions=True), timeout=1.0)
            except (Exception, asyncio.CancelledError):
                pass
        await self._disconnect()

    async def _watchdog_loop(self) -> None:
        while self._want:
            if not self._connected and self._now() >= self._next_try:
                try:
                    await self.connect()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass                     # connect 가 로그·백오프를 처리
            await asyncio.sleep(0.2)

    async def connect(self) -> None:
        if self._connected:
            return
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.connect_timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._next_try = self._now() + self._backoff
            if not self._conn_fail_logged:
                self._conn_fail_logged = True
                self._log(f"두께 모니터 연결 실패 {self.host}:{self.port} — 계속 재시도: {e!r}")
            self._backoff = min(self._backoff * 2.0, self.backoff_max_s)
            raise ThicknessCommError(f"연결 실패: {e!r}") from e
        self._reader, self._writer = reader, writer
        self._rx.clear()
        self._connected = True
        self._backoff = self.backoff_start_s
        self._conn_fail_logged = False
        self._ever_connected = True
        self.last_rx_t = None
        self._rx_task = asyncio.get_running_loop().create_task(self._rx_loop(reader), name="ThicknessRx")
        if not self.quiet:
            self._log(f"두께 모니터 연결 {self.host}:{self.port}")

    async def reconnect(self, why: str = "") -> None:
        """소켓을 닫는다 — 워치독이 백오프 뒤 다시 연결한다."""
        if self._connected and not self.quiet:
            self._log(f"두께 모니터 재연결: {why}" if why else "두께 모니터 재연결")
        await self._disconnect()
        self._next_try = self._now() + self._backoff
        self._backoff = min(self._backoff * 2.0, self.backoff_max_s)

    def _mark_lost(self) -> None:
        was = self._connected
        self._connected = False
        w = self._writer
        self._writer = self._reader = None
        if w is not None:
            try:
                w.transport.abort()
            except Exception:
                pass
        self._evt().set()                    # 기다리던 명령을 깨운다
        if was and self._want:
            self._next_try = self._now() + self._backoff
            if not self.quiet:
                self._log("두께 모니터 연결 끊김 — 재연결 대기")

    async def _disconnect(self) -> None:
        self._connected = False
        t, self._rx_task = self._rx_task, None
        if t is not None and not t.done() and t is not asyncio.current_task():
            t.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(t, return_exceptions=True), timeout=1.0)
            except (Exception, asyncio.CancelledError):
                pass
        w = self._writer
        self._writer = self._reader = None
        if w is not None:
            try:
                w.close()
                await asyncio.wait_for(w.wait_closed(), timeout=1.0)
            except (Exception, asyncio.CancelledError):
                try:
                    w.transport.abort()
                except Exception:
                    pass
        self._rx.clear()
        self._evt().set()

    async def _rx_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                chunk = await reader.read(256)
                if not chunk:
                    break
                self._rx.extend(chunk)
                if len(self._rx) > RX_BUF_MAX:
                    del self._rx[:-RX_BUF_MAX]
                self._evt().set()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        if reader is self._reader:
            self._mark_lost()

    # ── 주고받기 ──
    async def exchange(self, cmd: str, *, timeout_s: Optional[float] = None,
                       retries: Optional[int] = None) -> Reply:
        """명령 하나를 보내고 응답 프레임 하나를 받는다(한 번에 하나). 리셋 플래그가 보이면 L 로 확인한다."""
        frame = build_frame(cmd)
        async with self._lock():
            rep = await self._exchange_locked(cmd, frame, timeout_s, retries)
            await self._track_reset(rep, cmd)
            return rep

    async def _exchange_locked(self, cmd: str, frame: bytes, timeout_s: Optional[float],
                               retries: Optional[int]) -> Reply:
        tmo = self.cmd_timeout_s if timeout_s is None else max(0.05, float(timeout_s))
        attempts = 1 + (self.retries if retries is None else max(0, int(retries)))
        last: Optional[Exception] = None
        for i in range(attempts):
            if i > 0:
                await asyncio.sleep(RETRY_GAP_S)
            w = self._writer
            if not self._connected or w is None:
                raise ThicknessCommError("두께 모니터 미연결")
            self._rx.clear()
            self._evt().clear()
            try:
                w.write(frame)
                await asyncio.wait_for(w.drain(), timeout=tmo)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._mark_lost()
                raise ThicknessCommError(f"송신 실패 {cmd!r}: {e!r}") from e
            try:
                payload = await self._read_frame(tmo)
            except (ThicknessTimeout, ThicknessChecksumError) as e:
                last = e
                continue
            code = payload[:1]
            if code not in CODE_MEANING:
                last = ThicknessFrameError(f"모르는 응답 코드 {payload!r} (명령 {cmd!r})")
                continue
            return Reply(code=code, body=payload[1:], raw=payload)
        assert last is not None
        if isinstance(last, ThicknessTimeout):
            raise ThicknessTimeout(f"응답 없음 {cmd!r} ({attempts}회)")
        raise last

    async def _read_frame(self, timeout_s: float) -> str:
        deadline = self._now() + timeout_s
        evt = self._evt()
        while True:
            payload = extract_frame(self._rx)
            if payload is not None:
                self.last_rx_t = self._now()
                return payload
            if not self._connected:
                raise ThicknessCommError("수신 중 연결 끊김")
            remain = deadline - self._now()
            if remain <= 0:
                raise ThicknessTimeout("응답 없음")
            evt.clear()                      # 위 extract 와 여기 사이에는 await 가 없어 수신이 끼지 않는다
            try:
                await asyncio.wait_for(evt.wait(), timeout=remain)
            except asyncio.TimeoutError:
                pass

    async def _track_reset(self, rep: Reply, cmd: str) -> None:
        if not rep.reset:
            self._reset_active = False
            return
        if not self._reset_active:
            self._reset_active = True
            self.reset_seen += 1
            self._log(f"⚠ 두께 모니터 전원 리셋 플래그({rep.code}) — L 로 확인 (누적 {self.reset_seen})")
        if cmd == "L" or (self._now() - self._reset_ack_t) < RESET_ACK_MIN_INTERVAL_S:
            return
        self._reset_ack_t = self._now()
        try:
            await self._exchange_locked("L", build_frame("L"), None, None)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._log(f"⚠ 리셋 플래그 확인(L) 실패: {e!r}")

    async def query(self, cmd: str) -> str:
        """정상(A/B) 응답의 본문. F~K 면 ThicknessCommandRejected."""
        rep = await self.exchange(cmd)
        if rep.code not in OK_CODES:
            raise ThicknessCommandRejected(rep.code, cmd, rep.body)
        return rep.body

    # ── 읽기 ──
    async def read_thickness_a(self) -> float:
        return parse_thickness_a(await self.query("S"))

    async def read_rate_a_s(self) -> float:
        return parse_rate_a_s(await self.query("T"))

    async def read_frequency_hz(self) -> int:
        return parse_frequency_hz(await self.query("U"))

    async def read_crystal_life(self) -> float:
        return parse_life_pct(await self.query("V"))

    async def read_crystal_ok(self) -> bool:
        return parse_crystal_ok(await self.query("M"))

    async def read_reset_status(self) -> str:
        return strip_echo(await self.query("a"), "a")

    async def read_version(self) -> str:
        return (await self.query("@")).strip()

    async def read_density(self) -> float:
        return parse_number(await self.query("E?"), "E")

    async def read_z_factor(self) -> float:
        return parse_number(await self.query("F?"), "F")

    async def read_tooling(self) -> float:
        return parse_number(await self.query("J?"), "J")

    # ── 쓰기 ──
    async def zero(self, mode: str = "B") -> None:
        """B = 두께·타이머 영점, C = 두께만 영점."""
        m = str(mode or "").strip().upper()
        if m not in ("B", "C"):
            raise ValueError(f"영점 방식은 'B' 또는 'C': {mode!r}")
        await self.query(m)

    async def set_material(self, density: float, z_factor: float, tooling: float, *,
                           verify_retries: int = 1) -> dict:
        """E=/F=/J= 로 쓰고 E?/F?/J? 로 되읽어 허용오차 비교. 불일치면 1회 다시 쓰고, 그래도 다르면 ThicknessVerifyError.
        범위 밖 값은 보내기 전에 ValueError. 성공하면 되읽은 값 dict."""
        d_s, z_s, t_s = check_material(density, z_factor, tooling)
        want = (float(d_s), float(z_s), float(t_s))
        got = (None, None, None)
        for _attempt in range(1 + max(0, int(verify_retries))):
            await self.query("E=" + d_s)
            await self.query("F=" + z_s)
            await self.query("J=" + t_s)
            if self.settle_s:
                await asyncio.sleep(self.settle_s)
            got = (await self.read_density(), await self.read_z_factor(), await self.read_tooling())
            if (abs(got[0] - want[0]) <= TOL_DENSITY and abs(got[1] - want[1]) <= TOL_Z
                    and abs(got[2] - want[2]) <= TOL_TOOLING):
                return {"density": got[0], "z_factor": got[1], "tooling": got[2]}
        raise ThicknessVerifyError(
            f"되읽기 불일치 — density 보냄 {want[0]:g}/읽음 {got[0]}, Z 보냄 {want[1]:g}/읽음 {got[1]}, "
            f"tooling 보냄 {want[2]:g}/읽음 {got[2]}")

