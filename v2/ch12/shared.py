"""공유 장비 명시적 소유권(사용자 지시 3장 (3) — CH1·CH2 는 실제로 동시에 돈다).

장비 하나를 여러 쓰는 곳이 나눠 쓸 때, 누가 지금 쓰는지를 이 객체 하나가 들고 있다(시스템 조립이 하나 만들어 엔진에 준다).
CH1 과 CH2 가 서로 다른 것을 잡을 때는 서로 막지 않는다 — 잠금은 자원마다 따로이고, 잡기·놓기는 기다리지 않는다
(성공/거부를 바로 돌려준다).

자원
- PLC 링크(PlcLink): 포트·worker 는 하나(drivers.plc — 모든 요청이 한 줄로). 쓰는 곳마다 출처 태그를 붙인 proxy
  ("CH1"/"CH2"/"PC"/"ROBOT"/"HEATER"/"TSP"/"LOCAL"/"WEB" — v1 _PLCProxy). 로봇 PLC 명령 중에는 하트비트를 쉰다
  (pause_watchdog, v1 host/handlers.py 의 로봇 명령 구간). 두 챔버의 로봇 명령이 겹치면 참여 수를 세어 마지막이 끝날 때만 다시 켠다.
- 카메라(CameraShare): 1대. 참여자 chamber1·chamber2·pc1·pc2(v1 util/camera_recorder.py:252-255) — 모드·승격 규칙은
  drivers.camera 가 이미 v1 그대로 가진다. 여기는 참여자 이름 검사와 '누가 참여 중인지'만.
- 펄스 포트(drivers.pulse_port.PulsePorts): CH1 4007·CH2 4005 — 같은 주소 다른 종류·다른 챔버 거부(v1 문구). 그대로 쓴다.
- OES·RGA(챔버마다 1대): 한 번에 한 소유자(Exclusive) — 같은 챔버 공정과 그 챔버 플라즈마 클리닝이 겹치지 않게.
  같은 소유자의 다시 잡기는 허용. (v1 은 공정 스텝이 순서대로 불러 겹칠 일이 없었다 — 명시만 한 것, 새 판정 없음.)
- MFC 폴링(MfcPoll): 드라이버 acquire_poll/release_poll(사용자 집합 — 마지막 사용자가 끌 때만 멈춤, v1
  runtime/chamber_runtime.py:6533-6558)을 그대로 부른다.
- TSP 게이지(OnDemand): TSP 를 쓸 때만 worker 를 만들어 붙이고 놓으면 멈춘다(v1 TSP 런타임이 실행마다 새 AsyncIG 를 만들던 것과
  같다 — runtime/tsp_runtime.py:522-523). 1초 행은 잡기 전 off.
snapshot() 은 지금 소유자 표(화면·기록·시험용).
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Optional, Tuple

PLC_SOURCES = ("CH1", "CH2", "PC", "ROBOT", "HEATER", "TSP", "LOCAL", "WEB")
CAMERA_OWNERS = ("chamber1", "chamber2", "pc1", "pc2")      # util/camera_recorder.py:252-255


class Exclusive:
    """한 번에 한 소유자. acquire(owner) → (성공, 지금 소유자). 같은 소유자 다시 잡기는 성공."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.holder: Optional[str] = None
        self._lock = threading.Lock()

    def acquire(self, owner: str) -> Tuple[bool, Optional[str]]:
        with self._lock:
            if self.holder is None or self.holder == owner:
                self.holder = owner
                return True, owner
            return False, self.holder

    def release(self, owner: Optional[str] = None) -> bool:
        """owner 를 주면 그 소유자일 때만. 놓았으면 True."""
        with self._lock:
            if self.holder is None or (owner is not None and self.holder != owner):
                return False
            self.holder = None
            return True


class PlcLink:
    """PLC 링크 하나를 여러 출처가 나눠 쓴다 — proxy(출처)·로봇 명령 구간(하트비트 쉼)."""

    def __init__(self, plc) -> None:
        self.plc = plc
        self._proxies: Dict[str, Any] = {}
        self._robot = 0
        self._lock = threading.Lock()

    def proxy(self, source: str):
        s = str(source).upper()
        if s not in PLC_SOURCES:
            raise ValueError(f"PLC 출처 {source!r} — {PLC_SOURCES} 중 하나")
        with self._lock:
            p = self._proxies.get(s)
            if p is None:
                p = self._proxies[s] = self.plc.proxy(s)
            return p

    @property
    def robot_active(self) -> int:
        return self._robot

    def begin_robot(self) -> int:
        """로봇 PLC 명령 시작 — 첫 참여자가 하트비트를 쉰다. 지금 참여 수."""
        with self._lock:
            self._robot += 1
            if self._robot == 1:
                self.plc.pause_watchdog()
            return self._robot

    def end_robot(self) -> int:
        """로봇 PLC 명령 끝 — 마지막 참여자가 하트비트를 다시 켠다. 남은 참여 수."""
        with self._lock:
            if self._robot <= 0:
                return 0
            self._robot -= 1
            if self._robot == 0:
                self.plc.resume_watchdog()
            return self._robot

    def robot(self):
        """with link.robot(): … — 구간이 예외로 끝나도 하트비트를 다시 켠다."""
        link = self

        class _Ctx:
            def __enter__(self_inner):
                link.begin_robot()
                return link.proxy("ROBOT")

            def __exit__(self_inner, *exc):
                link.end_robot()
                return False
        return _Ctx()


class CameraShare:
    """카메라 1대 — 참여자 이름 검사 후 드라이버(start(mode, owner)·stop(owner))로."""

    def __init__(self, camera) -> None:
        self.camera = camera

    def _check(self, owner: str) -> str:
        if owner not in CAMERA_OWNERS:
            raise ValueError(f"카메라 참여자 {owner!r} — {CAMERA_OWNERS} 중 하나")
        return owner

    def join(self, owner: str) -> bool:
        from v2.ch12.drivers.camera import OWNER_MODE
        return self.camera.start(OWNER_MODE[self._check(owner)], owner=owner)

    def leave(self, owner: str) -> bool:
        """남은 참여자가 있으면 촬영을 계속하고 False(drivers.camera.stop)."""
        return self.camera.stop(owner=self._check(owner))

    @property
    def users(self) -> set:
        return set(self.camera.current_users)


class MfcPoll:
    """MFC 폴링 사용자 — 드라이버의 사용자 집합을 그대로(마지막 사용자가 끌 때만 멈춤)."""

    def __init__(self, mfc) -> None:
        self.mfc = mfc

    def acquire(self, owner: str) -> None:
        self.mfc.acquire_poll(owner)

    def release(self, owner: str) -> int:
        return self.mfc.release_poll(owner)

    @property
    def users(self) -> set:
        return set(self.mfc.poll_users)


class OnDemand:
    """쓸 때만 worker 를 붙이는 장비(TSP 게이지). make() → (driver, worker) 를 잡을 때마다 새로 만들고 1초 소스에 붙인다.
    놓으면 worker 를 멈추고 소스 worker 를 None(1초 행 off). 한 번에 한 소유자(같은 소유자 다시 잡기는 지금 것 유지)."""

    def __init__(self, name: str, make: Callable[[], Tuple[Any, Any]], source=None) -> None:
        self.name = name
        self.make = make
        self.source = source
        self.owner: Optional[str] = None
        self.driver = None
        self.worker = None
        self._lock = threading.RLock()
        if source is not None:
            source.worker = None

    def acquire(self, owner: str) -> Tuple[bool, Optional[str]]:
        with self._lock:
            if self.owner is not None:
                return (True, owner) if self.owner == owner else (False, self.owner)
            drv, w = self.make()
            self.driver, self.worker, self.owner = drv, w, owner
            if self.source is not None:
                self.source.worker = w
            w.start()
            return True, owner

    def release(self, owner: Optional[str] = None) -> bool:
        with self._lock:
            if self.owner is None or (owner is not None and self.owner != owner):
                return False
            w = self.worker
            self.owner, self.worker = None, None
            if self.source is not None:
                self.source.worker = None
        if w is not None:
            if threading.current_thread() is getattr(w, "_thread", None):
                threading.Thread(target=w.stop, name=f"release-{self.name}", daemon=True).start()
            else:
                w.stop()
        return True

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        d = self.driver
        if d is None:
            raise RuntimeError(f"{self.name}: 잡지 않음")
        return d.source_read(session, seq)


class SharedDevices:
    """공유 장비 소유권 모음 — 조립(system)이 하나 만든다."""

    def __init__(self, plc, camera, pulse, mfc: Dict[str, Any], tsp_ig: Optional[OnDemand] = None) -> None:
        self.plc = PlcLink(plc)
        self.camera = CameraShare(camera)
        self.pulse = pulse                                       # drivers.pulse_port.PulsePorts
        self.mfc = {ch: MfcPoll(m) for ch, m in mfc.items()}
        self.oes = {ch: Exclusive(f"{ch}.oes") for ch in mfc}
        self.rga = {ch: Exclusive(f"{ch}.rga") for ch in mfc}
        self.tsp_ig = tsp_ig

    def release_all(self, owner: str) -> int:
        """그 소유자가 잡은 단독 자원(OES·RGA·TSP 게이지)을 모두 놓는다(공정 끝 정리). 놓은 수."""
        n = sum(1 for x in list(self.oes.values()) + list(self.rga.values()) if x.release(owner))
        if self.tsp_ig is not None and self.tsp_ig.release(owner):
            n += 1
        return n

    def snapshot(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"plc.robot": self.plc.robot_active, "camera": sorted(self.camera.users)}
        for ch, m in self.mfc.items():
            out[f"{ch}.mfc_poll"] = sorted(m.users)
        for ch in self.oes:
            out[f"{ch}.oes"] = self.oes[ch].holder
            out[f"{ch}.rga"] = self.rga[ch].holder
        if self.pulse is not None:
            for ep, p in sorted(self.pulse.ports.items()):
                out[f"pulse {ep}"] = None if p.owner is None else f"{p.owner[0]}.{p.owner[1]}"
        if self.tsp_ig is not None:
            out["tsp.ig"] = self.tsp_ig.owner
        return out
