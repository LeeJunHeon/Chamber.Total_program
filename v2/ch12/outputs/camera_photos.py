"""카메라 사진 저장 — v1 util/camera_recorder.py(_record_loop) 의 경로·파일명·저장 방식(LOG-071~078).

캡처(카메라 열기·1920x1080 요청·90° 시계 방향 회전·cv2.imencode('.jpg'))는 v2/ch12/drivers/camera.py 가 하고, 여기서는 받은
JPG 바이트를 Path.write_bytes 로 쓴다(cv2.imwrite 는 한글 경로에서 조용히 실패 — v1 과 같은 이유).

v1 그대로
- 경로 CAMERA_LOG_ROOT(C:\\VanaM_Logs\\Camera_Logs)\\{CH1|CH2|CLEANING|ALL}\\{YYYYmmdd_HHMMSS}\\{HHMMSS}_{NNNN}.jpg
  (NNNN = 1부터 4자리, 읽기 성공한 장 번호). 루트 생성 실패면 exe\\Logs_LocalFallback\\Camera_Logs.
  apps/rf_service rf_worker 가 이 폴더·세션 이름(^\\d{8}_\\d{6}$)·파일명으로 찾는다(inv_app 확인 항목).
- 디스크 가드: 세션 시작 때(카메라 열기 전) 여유 < CAMERA_MIN_FREE_GB 면 '디스크 여유 부족(x GB < 20.0GB) — 이번 공정
  영상 미기록', 촬영 중 300장마다 재검사해 '— 촬영 중단'. 여유 조회 실패는 inf. 부족 알림은 공정마다 1회(notify 콜백).
- 세션 폴더는 카메라를 연 뒤에 만든다(열기 실패면 폴더 없음 — 판독 워커가 빈 폴더를 영구 기록하지 않게).
  끝에 '완료 — 촬영 N장 | 폴더: ..', 0장이면 빈 폴더 삭제('촬영 0장 → 빈 폴더 삭제: <이름>').
- 모드: 참여자(chamber1·chamber2·pc1·pc2) 1명이면 CH1/CH2/CLEANING, 2명 이상이면 ALL(mode_for_owners). 소유권·재시작 판단은
  드라이버 몫.

부르는 법(drivers/camera.py 의 촬영 스레드에서)
  cp = CameraPhotos(cfg, writer=w, log=fn, notify=fn, now=clock_dt)
  if not cp.disk_ok(): return                      # 카메라 열기 전
  (카메라 열기 성공 후) d = cp.begin(mode)          # 세션 폴더 경로(쓰기 스레드가 만듦)
  cp.save(jpg_bytes)                                # 장마다 — 번호는 여기서 센다. 300장마다 cp.disk_ok(during=True)
  cp.end()                                          # '완료 …' 로그·0장이면 빈 폴더 삭제
"""
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
from v2.ch12.outputs.paths import Paths
from v2.ch12.params import outputs as P

MODE_FOLDER = {"CH1": "CH1", "CH2": "CH2", "CLEANING": "CLEANING", "ALL": "ALL"}
OWNER_MODE = {"chamber1": "CH1", "chamber2": "CH2", "pc1": "CLEANING", "pc2": "CLEANING"}
DISK_CHECK_EVERY = 300


def mode_for_owners(owners: Iterable[str]) -> str:
    """참여자 집합 → 촬영 모드(util/camera_recorder.py:706-718)."""
    users = set(owners)
    if len(users) == 1:
        return OWNER_MODE.get(next(iter(users)), "ALL")
    return "ALL"


def free_gb(path: Path) -> float:
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except Exception:
        return float("inf")


class CameraPhotos(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, writer: Optional[QueueWriter] = None,
                 log: Optional[Callable[[str], None]] = None, notify: Optional[Callable[[str], None]] = None,
                 now: Callable[[], datetime] = wall_now, free_fn: Callable[[Path], float] = free_gb) -> None:
        super().__init__(writer, "camera-photos")
        self.cfg = cfg if cfg is not None else {}
        self.paths = Paths(self.cfg)
        self._log_fn = log
        self._notify_fn = notify
        self._notified = False
        self.now = now
        self.free_fn = free_fn
        self.session_dir: Optional[Path] = None
        self.count = 0
        self.mode = ""

    def _log(self, msg: str) -> None:
        if self._log_fn:
            try:
                self._log_fn(msg)
            except Exception:
                pass

    def _notify(self, msg: str) -> None:
        if self._notify_fn is None or self._notified:
            return
        self._notified = True
        try:
            self._notify_fn(msg)
        except Exception:
            pass

    def new_process(self) -> None:
        """새 공정이 촬영을 시작하면 부족 알림을 다시 허용(v1 start 때 _notified=False)."""
        self._notified = False

    def root(self) -> Path:
        """설정 루트를 만들어 돌려준다. 실패하면 exe 기준 로컬 폴백(v1 _resolve_root)."""
        root = self.paths.camera_root()
        try:
            root.mkdir(parents=True, exist_ok=True)
            return root
        except Exception as e:
            fb = self.paths.camera_fallback()
            self._log(f"[CameraRecorder] 저장 루트 생성 실패({root}: {e}) → 폴백 경로 사용: {fb}")
            fb.mkdir(parents=True, exist_ok=True)
            return fb

    def disk_ok(self, during: bool = False) -> bool:
        thr = float(P.get(self.cfg, "CAMERA_MIN_FREE_GB"))
        free = self.free_fn(self.root())
        if free < thr:
            msg = f"디스크 여유 부족({free:.1f}GB < {thr:.1f}GB) — " + ("촬영 중단" if during else "이번 공정 영상 미기록")
            self._log(msg)
            self._notify(msg)
            return False
        return True

    def begin(self, mode: str) -> Path:
        self.mode = str(mode)
        ts = self.now().strftime("%Y%m%d_%H%M%S")
        d = self.root() / MODE_FOLDER.get(self.mode, "ALL") / ts
        self.session_dir = d
        self.count = 0
        self.writer.put(self._mkdir, d)
        self._log(f"카메라 시작 mode={self.mode} → {d}")
        return d

    def _mkdir(self, d: Path) -> None:
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._log(f"폴더 생성 실패: {e}")

    def save(self, jpg: Optional[bytes]) -> Optional[Path]:
        """한 장(인코딩한 JPG 바이트, 인코딩 실패면 None). 저장할 경로를 돌려준다(쓰기는 큐)."""
        if self.session_dir is None:
            return None
        self.count += 1
        p = self.session_dir / f"{self.now().strftime('%H%M%S')}_{self.count:04d}.jpg"
        if jpg is None:
            self._log("이미지 인코딩 실패")
            return None
        self.writer.put(self._write, p, bytes(jpg))
        return p

    def need_disk_check(self) -> bool:
        return self.count > 0 and self.count % DISK_CHECK_EVERY == 0

    def _write(self, p: Path, data: bytes) -> None:
        try:
            p.write_bytes(data)
        except Exception as e:
            self._log(f"이미지 저장 실패: {e}")

    def end(self) -> None:
        d = self.session_dir
        if d is None:
            return
        self._log(f"완료 — 촬영 {self.count}장 | 폴더: {d}")
        if self.count == 0:
            self.writer.put(self._rmdir_empty, d)
        self.session_dir = None

    def _rmdir_empty(self, d: Path) -> None:
        try:
            d.rmdir()
            self._log(f"촬영 0장 → 빈 폴더 삭제: {d.name}")
        except Exception:
            pass
