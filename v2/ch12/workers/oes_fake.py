r"""가짜 분광기 DLL(시험용, --fake) — SPdbUSBm.dll 과 같은 함수 이름·인자(ctypes 객체)를 받아 응답한다.

워커의 나머지 코드(초기화 순서·연결 감시·측정 루프·CSV·stop 플래그·JSONL)는 실제와 똑같이 돈다 — DLL 만 바꾼다.
호출마다 OES_FAKE_LOG 파일(있으면)에 JSON 한 줄 {"fn": 이름, "args": [...]} 을 붙인다 → 시험이 호출 순서·인자를 확인.

동작은 환경변수 OES_FAKE="키=값,키=값" 으로:
  devices=N            spTestAllChannels 가 돌려줄 대수(기본 2, 0 이면 '장치 스캔 실패')
  model=M              spInitGivenChannel 이 성공하는 모델 번호(기본 2=PDA → 1056 픽셀)
  serial=S             spDevInfo 일련번호(기본 FAKE{ch})
  hang_init=1          spTestAllChannels 가 멈춤(초기화 단계 시간 초과)
  hang_read_after=N    spReadDataEx N 회 뒤 멈춤(DLL 걸림 → OES_READ_TIMEOUT_S 뒤 hard_abort 125)
  fail_read_after=N    spReadDataEx N 회 뒤 rc=-1(연속 읽기 실패 → 리셋·재초기화)
  die_after=N          spReadDataEx N 회 뒤 프로세스가 죽음(os._exit(9) — 워커 크래시)
"""
from __future__ import annotations

import ctypes
import json
import os
import threading
import time
from typing import Any, Dict

from v2.ch12.workers import oes_common as C

_LOG_LOCK = threading.Lock()


def _opts() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in (os.environ.get("OES_FAKE", "") or "").split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _plain(a: Any) -> Any:
    if isinstance(a, ctypes.Array):
        return f"<buf {len(a)}>"
    return getattr(a, "value", a)


class _Fn:
    """ctypes 함수처럼 argtypes·restype 를 붙일 수 있는 호출 가능 객체."""

    def __init__(self, dll: "FakeDll", name: str, impl) -> None:
        self.dll, self.name, self.impl = dll, name, impl
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        self.dll._log(self.name, [_plain(a) for a in args])
        return self.impl(*args)


class FakeDll:
    def __init__(self, path: str) -> None:
        self.path = path
        self._handle = None
        o = _opts()
        self.devices = int(o.get("devices", "2"))
        self.model = int(o.get("model", "2"))
        self.serial = o.get("serial", "")
        self.hang_init = o.get("hang_init") == "1"
        self.hang_read_after = int(o.get("hang_read_after", "-1"))
        self.fail_read_after = int(o.get("fail_read_after", "-1"))
        self.die_after = int(o.get("die_after", "-1"))
        self.reads = 0
        self.frame = 0
        for name, impl in (
            ("spTestAllChannels", self._test_all), ("spSetupGivenChannel", self._setup),
            ("spInitGivenChannel", self._init), ("spReadDataEx", self._read), ("spCloseGivenChannel", self._ok),
            ("spAutoDark", self._ok), ("spSetTrgEx", self._ok), ("spSetTEC", self._ok), ("spSetDblIntEx", self._ok),
            ("spGetWLTable", self._wl), ("spDevInfo", self._dev_info),
        ):
            setattr(self, name, _Fn(self, name, impl))

    def _log(self, fn: str, args: list) -> None:
        path = os.environ.get("OES_FAKE_LOG")
        if not path:
            return
        with _LOG_LOCK:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps({"fn": fn, "args": args}) + "\n")

    @staticmethod
    def _ok(*_a) -> int:
        return 0

    def _test_all(self, _order) -> int:
        if self.hang_init:
            time.sleep(3600)
        return self.devices

    def _setup(self, ch) -> int:
        return 0 if 0 <= int(_plain(ch)) < self.devices else -1

    def _init(self, model, ch) -> int:
        return 0 if int(_plain(model)) == self.model and 0 <= int(_plain(ch)) < self.devices else -1

    def _read(self, buf, ch) -> int:
        self.reads += 1
        if 0 <= self.die_after < self.reads:
            os._exit(9)
        if 0 <= self.hang_read_after < self.reads:
            time.sleep(3600)
        if 0 <= self.fail_read_after < self.reads:
            return -1
        self.frame += 1
        base = 1000 + 10 * int(_plain(ch)) + (self.frame % 7)
        for i in range(len(buf)):
            buf[i] = base + (i % 50)
        return 0

    def _wl(self, buf, _ch) -> int:
        for i in range(len(buf)):
            buf[i] = 200.0 + 0.6 * i
        return 0

    def _dev_info(self, mb, sb, ch) -> int:
        mb.value = b"SM303-FAKE"
        sb.value = (self.serial or f"FAKE{int(_plain(ch))}").encode("ascii")
        return 0


def install() -> None:
    """워커가 --fake 로 시작할 때 — DLL 로드를 가짜로 바꾼다."""
    C.DLL_FACTORY = FakeDll
