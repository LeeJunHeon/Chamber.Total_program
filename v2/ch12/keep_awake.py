"""절전 막기(G — 사용자 결정 2026-11-26) — 노트북이 시스템 절전(대기 모드)에 들어가 로봇 요청·공정 기록이 끊기지 않게 한다.

윈도우 SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) — 부른 스레드가 살아 있는 동안만 유효하므로 이 모듈의 스레드 하나
(keep-awake)가 걸고 그 스레드가 푼다(ES_CONTINUOUS 만). 화면 꺼짐(ES_DISPLAY_REQUIRED)은 막지 않는다. 챔버K v2/chk/keep_awake.py 와 같은 방식.
설정 KEEP_AWAKE: always(기본 — 프로그램이 켜져 있는 동안. 로봇 요청을 늘 받아야 한다) · run(공정·Plasma Cleaning·TSP·히터 운전 중만 —
system.busy) · off. 윈도우가 아니면 아무것도 하지 않는다. 프로그램 종료 때 푼다. 걸 때·풀 때 상세 사건 한 줄씩(selfwatch keep_awake_on·
keep_awake_off). 시험은 가짜 API(api = 호출 기록 함수)로.

한계(W — 사용자 결정 2026-11-27, 챔버K W-25 A5 와 같게): SetThreadExecutionState 는 사용자가 직접 시킨 절전(덮개 닫기·전원 버튼)을 막지
못한다. 그래서 프로그램이 시작할 때 윈도우 전원 설정을 읽기만 한다(powrprof — 켜진 전원 구성의 덮개 닫기 동작 AC·배터리, AC 절전 시간,
GetSystemPowerStatus — 지금 AC 전원인지). 덮개 닫기가 '아무것도 안 함'이 아니거나 AC 절전 시간이 '없음'이 아니거나 배터리로 돌고
있으면 시작 줄 한 줄 + 경고 사건 1번(selfwatch power_settings, 사유 POWER_SETTINGS). 설정은 바꾸지 않는다. 윈도우가 아니거나 읽지 못하면
조용히 넘어간다. 시험은 가짜 읽기 함수(reader)로.
"""
from __future__ import annotations

import sys
import threading
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001
CHECK_S = 1.0
MODES = ("always", "run", "off")
WHY = {"always": "프로그램이 켜져 있는 동안", "run": "운전 중"}


def windows_api() -> Optional[Callable[[int], int]]:
    """윈도우면 kernel32.SetThreadExecutionState, 아니면 None."""
    if not sys.platform.startswith("win"):
        return None
    try:
        import ctypes
        f = ctypes.windll.kernel32.SetThreadExecutionState          # type: ignore[attr-defined]
        f.restype = ctypes.c_uint
        f.argtypes = [ctypes.c_uint]
        return f
    except Exception:
        return None


# ───────── 윈도우 전원 설정 읽기(W — 읽기만, 챔버K keep_awake.py 와 같은 이름·글) ─────────
SUB_BUTTONS = "4f971e89-eebd-4455-a8de-9e59040e7347"      # 전원 단추와 덮개
LIDACTION = "5ca83367-6e45-459f-a27b-476b1d01c936"        # 덮개 닫기 동작 0 아무것도 안 함 · 1 절전 · 2 최대 절전 · 3 시스템 종료
SUB_SLEEP = "238c9fa8-0aad-41ed-83f4-97be242c8f20"        # 절전
STANDBYIDLE = "29f6c1db-86da-48c5-9fdb-f2b67b1f44da"      # 다음 시간 후 절전(초, 0 = 없음)
LID_WORD = {1: "절전됩니다", 2: "최대 절전 모드로 들어갑니다", 3: "꺼집니다"}


@dataclass(frozen=True)
class PowerInfo:
    lid_ac: Optional[int] = None       # 덮개 닫기 동작(전원 연결 시)
    lid_dc: Optional[int] = None       # 덮개 닫기 동작(배터리 사용 시)
    sleep_ac_s: Optional[int] = None   # 절전까지 시간(전원 연결 시, 0 = 없음)
    on_ac: Optional[bool] = None       # 지금 전원 연결(AC)인가


def power_warning(info: Optional[PowerInfo]) -> Optional[str]:
    """절전될 수 있는 전원 설정이면 한 줄 글(없으면 None) — 챔버K 와 같은 글."""
    if info is None:
        return None
    parts = []
    lid = next((x for x in (info.lid_ac, info.lid_dc) if x not in (None, 0)), None)
    if lid is not None:
        when = "전원 연결 시" if info.lid_ac not in (None, 0) else "배터리 사용 시"
        parts.append(f"덮개를 닫으면 {LID_WORD.get(lid, '절전됩니다')}({when})")
    if info.sleep_ac_s not in (None, 0):
        m = max(1, round(int(info.sleep_ac_s) / 60))
        parts.append(f"전원 연결 시 {m}분 동안 쓰지 않으면 절전됩니다")
    if info.on_ac is False:
        parts.append("지금 배터리로 돌고 있습니다")
    if not parts:
        return None
    return ("장비 노트북 전원 설정: " + ". ".join(parts)
            + ". 윈도우 전원 설정에서 덮개 닫기는 '아무것도 안 함', 절전은 '없음'으로 바꾸고 전원을 연결하세요")


def check_power(reader: Optional[Callable[[], PowerInfo]], say: Callable[[str], None],
                on_event: Optional[Callable[[str, dict], None]] = None) -> Optional[str]:
    """시작 때 한 번 — 읽어서 위험하면 시작 줄 한 줄 + 경고 사건 1번(on_event(글, 값)). 윈도우가 아니면(reader None)·못 읽으면 조용히."""
    if reader is None:
        return None
    try:
        info = reader()
    except Exception:
        return None
    msg = power_warning(info)
    if msg:
        say(f"[주의] {msg}")
        if on_event:
            try:
                on_event(msg, {"lidAc": info.lid_ac, "lidDc": info.lid_dc, "sleepAcS": info.sleep_ac_s, "onAc": info.on_ac})
            except Exception:
                pass
    return msg


def windows_power() -> Optional[Callable[[], PowerInfo]]:
    """윈도우면 전원 설정을 읽는 함수(powrprof·GetSystemPowerStatus — 읽기만), 아니면 None."""
    if not sys.platform.startswith("win"):
        return None
    try:
        import ctypes
        import uuid
        pp = ctypes.windll.powrprof                                   # type: ignore[attr-defined]
        k32 = ctypes.windll.kernel32                                  # type: ignore[attr-defined]
    except Exception:
        return None

    class GUID(ctypes.Structure):
        _fields_ = [("Data1", ctypes.c_ulong), ("Data2", ctypes.c_ushort), ("Data3", ctypes.c_ushort), ("Data4", ctypes.c_ubyte * 8)]

    class SPS(ctypes.Structure):
        _fields_ = [("ACLineStatus", ctypes.c_ubyte), ("BatteryFlag", ctypes.c_ubyte), ("BatteryLifePercent", ctypes.c_ubyte),
                    ("SystemStatusFlag", ctypes.c_ubyte), ("BatteryLifeTime", ctypes.c_ulong), ("BatteryFullLifeTime", ctypes.c_ulong)]

    def guid(s: str) -> "GUID":
        return GUID.from_buffer_copy(uuid.UUID(s).bytes_le)

    def read() -> PowerInfo:
        scheme = ctypes.POINTER(GUID)()
        if pp.PowerGetActiveScheme(None, ctypes.byref(scheme)) != 0:
            raise OSError("PowerGetActiveScheme")
        try:
            def val(fn, sub: str, setting: str) -> Optional[int]:
                v = ctypes.c_ulong()
                rc = fn(None, scheme, ctypes.byref(guid(sub)), ctypes.byref(guid(setting)), ctypes.byref(v))
                return None if rc != 0 else int(v.value)
            lid_ac = val(pp.PowerReadACValueIndex, SUB_BUTTONS, LIDACTION)
            lid_dc = val(pp.PowerReadDCValueIndex, SUB_BUTTONS, LIDACTION)
            sleep_ac = val(pp.PowerReadACValueIndex, SUB_SLEEP, STANDBYIDLE)
        finally:
            k32.LocalFree(scheme)
        sps = SPS()
        on_ac = {0: False, 1: True}.get(int(sps.ACLineStatus)) if k32.GetSystemPowerStatus(ctypes.byref(sps)) else None
        return PowerInfo(lid_ac, lid_dc, sleep_ac, on_ac)
    return read


class KeepAwake:
    def __init__(self, cfg: Mapping[str, Any], busy: Callable[[], bool],
                 on_event: Optional[Callable[[str, str], None]] = None,
                 api: Optional[Callable[[int], int]] = None) -> None:
        self.cfg, self.busy, self.on_event = cfg, busy, on_event
        self.api = api                   # None = 윈도우가 아님(아무것도 하지 않음)
        self.held = False
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def mode(self) -> str:
        m = str(self.cfg.get("KEEP_AWAKE") or "always")
        return m if m in MODES else "always"

    def want(self) -> bool:
        m = self.mode()
        if m == "always":
            return True
        if m == "off":
            return False
        try:
            return bool(self.busy())
        except Exception:
            return self.held             # 판단 실패 — 지금 상태 그대로

    def _event(self, kind: str, detail: str) -> None:
        if self.on_event:
            try:
                self.on_event(kind, detail)
            except Exception:
                pass

    def tick(self) -> None:
        """한 번 판단 — keep-awake 스레드가 1초마다 부른다(시험은 직접)."""
        if self.api is None:
            return
        w = self.want()
        if w == self.held:
            return
        self.api(ES_CONTINUOUS | ES_SYSTEM_REQUIRED if w else ES_CONTINUOUS)
        self.held = w
        if w:
            self._event("keep_awake_on", f"윈도우 절전 막기 시작({WHY.get(self.mode(), '운전 중')})")
        else:
            self._event("keep_awake_off", "윈도우 절전 막기 끝")

    def release(self) -> None:
        """걸려 있으면 푼다 — 같은 스레드에서(_loop 끝)."""
        if self.held and self.api is not None:
            self.api(ES_CONTINUOUS)
            self.held = False
            self._event("keep_awake_off", "윈도우 절전 막기 끝(프로그램 종료)")

    def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                self.tick()
                self._stop.wait(CHECK_S)
        finally:
            self.release()

    def start(self) -> None:
        if self.api is None:
            return
        self._thread = threading.Thread(target=self._loop, name="keep-awake", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
