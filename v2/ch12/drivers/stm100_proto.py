"""두께 모니터 Sycon STM-100/MF 프로토콜(순수 함수·예외) — v1 device/thickness_monitor.py 의 프레임·해석을 다시 쓴 것.

프레임 = STX(0x02) + LEN(데이터 길이 1바이트) + ASCII 데이터(1~10 B) + CHK(데이터 바이트 합 & 0xFF). 응답도 같은 형식.
응답 첫 글자 = 결과 코드: A 정상 / B 정상+전원리셋 플래그 / F,G 잘못된 명령 / H,I 잘못된 값 / J,K 잘못된 수식자
(뒤쪽 글자가 리셋 플래그가 있는 쪽). 값 앞에 명령 글자가 다시 붙어 올 수 있다("U 5319234") → 떼고 해석.
값 대신 빈값/"--------"/"    . " = 값 없음(XTAL FAIL 등 — StmValueUnavailable, 통신 실패와 구분).
예외 갈래는 v1 과 같다(서비스가 다르게 센다 — device/thickness_monitor.py:20-27):
  StmCommError(통신 실패, 하위 StmTimeout·StmLinkLost) / StmProtocolError(하위 StmFrameError ⊃ StmChecksumError, StmCommandRejected)
  / StmValueUnavailable / StmVerifyError. 범위 밖 재료값은 보내기 전에 ValueError.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple

STX = 0x02
CMD_LEN_MAX = 10              # 송신 데이터 최대 길이(매뉴얼)
RX_LEN_MAX = 32               # 수신 LEN 허용 상한(v1: 매뉴얼은 10 — 펌웨어 차이에 너그럽게)

OK_CODES = frozenset("AB")
RESET_CODES = frozenset("BGIK")
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

_INT_RE = re.compile(r"^[+-]?\d+$")
_NUM_RE = re.compile(r"^[+-]?(\d+(\.\d*)?|\.\d+)$")


# ═══════════════════════ 예외(v1 과 같은 갈래) ═══════════════════════
class StmError(Exception):
    """두께 모니터 오류 공통 부모."""


class StmCommError(StmError):
    """통신 실패 — 미연결·송신 실패·응답 없음."""


class StmTimeout(StmCommError):
    """명령 응답 없음(재시도 모두 소진)."""


class StmLinkLost(StmCommError):
    """송신 실패·수신 중 연결 끊김 — worker 가 연결을 끊고 백오프 뒤 다시 연다."""


class StmProtocolError(StmError):
    """프레임·체크섬·형식·코드 오류."""


class StmFrameError(StmProtocolError):
    """응답 프레임 자체가 틀림(체크섬·길이·모르는 결과 코드) — 통신 이상으로 센다."""


class StmChecksumError(StmFrameError):
    """수신 프레임 체크섬/길이 불일치 — 재시도 가능."""


class StmCommandRejected(StmProtocolError):
    """장비가 F~K(잘못된 명령/값/수식자)로 거부."""

    def __init__(self, code: str, cmd: str, body: str = ""):
        self.code, self.cmd, self.body = code, cmd, body
        super().__init__(f"{CODE_MEANING.get(code, '모르는 코드')}({code}) — 보낸 명령 {cmd!r}"
                         + (f", 응답 {body!r}" if body else ""))


class StmValueUnavailable(StmError):
    """응답은 정상인데 값이 없음(빈값/'--------'/'    . ')."""


class StmVerifyError(StmError):
    """set_material 되읽기 불일치."""


def error_text(e: Exception) -> str:
    """화면·로그용 한 줄(v1 controller/thickness_service.py:54-63 error_text 와 같은 분류·문구)."""
    for cls, label in ((StmTimeout, "응답 없음"), (StmCommError, "통신 실패"),
                       (StmChecksumError, "체크섬 오류"), (StmFrameError, "프레임 오류"),
                       (StmCommandRejected, "명령 거부"),
                       (StmValueUnavailable, "값 없음"), (StmVerifyError, "되읽기 불일치"),
                       (StmError, "응답 형식 오류")):
        if isinstance(e, cls):
            return f"{label} — {e}"
    return f"{type(e).__name__}: {e}"


# ═══════════════════════ 프레임·파싱 ═══════════════════════
def checksum(data: bytes) -> int:
    return sum(data) & 0xFF


def build_frame(data_ascii: str) -> bytes:
    data = str(data_ascii).encode("ascii")
    if not (1 <= len(data) <= CMD_LEN_MAX):
        raise ValueError(f"명령 데이터는 1~{CMD_LEN_MAX}바이트여야 합니다: {data_ascii!r} ({len(data)}바이트)")
    return bytes([STX, len(data)]) + data + bytes([checksum(data)])


def extract_frame(buf: bytearray) -> Optional[str]:
    """buf 앞에서 프레임 하나를 꺼낸다(꺼낸 바이트는 지운다). 덜 왔으면 None. STX 앞 쓰레기는 버린다.
    LEN 이 범위 밖이거나 체크섬이 틀리면 StmChecksumError(v1 extract_frame 과 같은 동작·문구)."""
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
        raise StmChecksumError(f"수신 LEN={ln} 범위 밖")
    need = 2 + ln + 1
    if len(buf) < need:
        return None
    data = bytes(buf[2:2 + ln])
    chk = buf[2 + ln]
    del buf[:need]
    if checksum(data) != chk:
        raise StmChecksumError(f"체크섬 불일치 rx={data!r} chk={chk:#04x} 계산={checksum(data):#04x}")
    return data.decode("ascii", "replace")


def fmt_compact(x: float, decimals: int) -> str:
    """명령 데이터 10바이트 제한 — 1.230 → '1.23'."""
    s = f"{float(x):.{int(decimals)}f}".rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def strip_echo(body: str, token: str) -> str:
    s = (body or "").strip()
    t = (token or "")[:1]
    if s and t.isalpha() and s[:1].upper() == t.upper():
        s = s[1:].strip()
    return s


def _value_text(body: str, token: str) -> str:
    s = strip_echo(body, token)
    if set(s) <= {" ", "-", "."} or s.upper() in ("N/A", "NA"):
        raise StmValueUnavailable(f"{token}: 값 없음({body!r})")
    return s


def parse_thickness_a(body: str) -> float:
    s = _value_text(body, "S")
    if not _INT_RE.fullmatch(s):
        raise StmProtocolError(f"S: 두께 형식 오류 {body!r}")
    return float(int(s))


def parse_rate_a_s(body: str) -> float:
    s = _value_text(body, "T")
    if not _NUM_RE.fullmatch(s):
        raise StmProtocolError(f"T: 증착률 형식 오류 {body!r}")
    return float(s)


def parse_frequency_hz(body: str) -> int:
    s = _value_text(body, "U")
    if not _INT_RE.fullmatch(s) or int(s) <= 0:
        raise StmProtocolError(f"U: 주파수 형식 오류 {body!r}")
    return int(s)


def parse_life_pct(body: str) -> float:
    s = _value_text(body, "V")
    if not _NUM_RE.fullmatch(s):
        raise StmProtocolError(f"V: 수명 형식 오류 {body!r}")
    v = float(s)
    if not (0.0 <= v <= 100.0):
        raise StmProtocolError(f"V: 수명 범위 밖 {body!r}")
    return v


def parse_crystal_ok(body: str) -> bool:
    s = _value_text(body, "M")
    if s == "@":
        return True
    if s == "!":
        return False
    raise StmProtocolError(f"M: 크리스탈 상태 형식 오류 {body!r}")


def parse_number(body: str, token: str) -> float:
    s = _value_text(body, token)
    if not _NUM_RE.fullmatch(s):
        raise StmProtocolError(f"{token}?: 숫자 형식 오류 {body!r}")
    return float(s)


def check_material(density: float, z_factor: float, tooling: float) -> Tuple[str, str, str]:
    """보낼 문자열로 정규화하고 범위를 확인한다 — (d, z, t). 범위 밖이면 ValueError(보내지 않음)."""
    d_s, z_s, t_s = fmt_compact(density, 3), fmt_compact(z_factor, 3), fmt_compact(tooling, 1)
    for name, v, lo, hi in (("density", float(d_s), DENSITY_MIN, DENSITY_MAX),
                            ("Z-factor", float(z_s), Z_MIN, Z_MAX),
                            ("tooling", float(t_s), TOOLING_MIN, TOOLING_MAX)):
        if not (lo <= v <= hi):
            raise ValueError(f"{name}={v:g} 범위({lo:g}~{hi:g}) 밖 — 보내지 않음")
    return d_s, z_s, t_s


def material_matches(want: Tuple[float, float, float], got: Tuple[float, float, float]) -> bool:
    return (abs(got[0] - want[0]) <= TOL_DENSITY and abs(got[1] - want[1]) <= TOL_Z
            and abs(got[2] - want[2]) <= TOL_TOOLING)


@dataclass
class Reply:
    code: str
    body: str
    raw: str

    @property
    def reset(self) -> bool:
        return self.code in RESET_CODES
