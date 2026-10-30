"""카메라 드라이버(공유 장비 1대) — v1 util/camera_recorder.py(CameraRecorder)의 장치 부분을 다시 쓴 것(import 하지 않음).

v1 이 지금 하는 일(OCR 은 이미 빠짐 — 판독은 별도 apps/rf_service): 공정 중 RF 매칭 패널을 1초마다 찍어 원본 JPG 로 저장.
- 장치: cv2.VideoCapture(index=1, v1 main.py:591) — 열기 실패면 이번 공정 미기록. 1920×1080 요청.
- 저장: {CAMERA_LOG_ROOT}/{모드 폴더}/{YYYYmmdd_HHMMSS}/{HHMMSS}_{번호:04d}.jpg — 90° 시계 방향 회전 뒤
  cv2.imencode('.jpg') + Path.write_bytes(한글 경로에서 cv2.imwrite 가 조용히 실패하는 문제 회피, 사용자 지시 4장).
  루트 만들기 실패 → <exe 또는 저장소 루트>/Logs_LocalFallback/Camera_Logs. 한 장도 못 찍으면 빈 폴더를 지운다.
- 디스크 여유: 카메라를 열기 전에 CAMERA_MIN_FREE_GB(20 GB) 미만이면 촬영 안 함(알림 1회), 촬영 중에는 300장마다 다시 —
  미만이면 중단(알림 1회). 여유 조회 실패는 무한대(가드로 촬영이 막히지 않게).
- 읽기 실패: 0.3 s 쉬고 다시, 연속 10번 넘으면 '카메라 읽기 반복 실패'로 끝.
- 공유 소유권(카메라 1대·프레임에 모든 디스플레이): 참여자(owner) 집합이 모드를 정한다 — 1명 = 그 owner 의 모드
  (chamber1→CH1, chamber2→CH2, pc1·pc2→CLEANING, 모르는 owner→ALL), 2명 이상 = ALL 로 승격(1회만 재시작), 강등 없음
  (진행 중인 ALL 은 유지). stop(owner) 는 남은 참여자가 있으면 촬영을 계속하고 False. owner 없이 부르면 v1 처럼 무조건.
- 알림(notify_cb, 구글챗)은 한 공정에 한 번 — 새 참여자가 start 하면 다시 한 번 허용.
cv2·numpy 는 이 파일 안에서만, 함수 안에서 늦게 import 한다(시험은 cv2 없이 가짜 장치 — CameraBackend).
촬영은 clock.start_thread 스레드에서(가상 시계 시험 가능), 기다림은 clock.wait_until — stop 은 바로 깨운다.
1초 행(공유 블록 "camera"): 촬영 중·모드·참여자·장 수·마지막 메시지. 포트에 닿지 않는다(cache_source).
"""
from __future__ import annotations

import datetime as _dt
import logging
import shutil
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from v2.common.snapshot import DeviceBlock, FieldDef, FType
from v2.common.snapshot_bus import Source
from v2.ch12.drivers.cache_source import CacheWorker

logger = logging.getLogger(__name__)

MODES = ("CH1", "CH2", "CLEANING", "ALL")
MODE_FOLDER = {"CH1": "CH1", "CH2": "CH2", "CLEANING": "CLEANING", "ALL": "ALL"}     # util/camera_recorder.py:245-250
OWNER_MODE = {"chamber1": "CH1", "chamber2": "CH2", "pc1": "CLEANING", "pc2": "CLEANING"}   # :252-255
FRAME_W, FRAME_H = 1920, 1080          # :865-866
READ_FAIL_MAX = 10                     # :897-900
READ_FAIL_PAUSE_S = 0.3                # :901
DISK_RECHECK_EVERY = 300               # :907
INTERVAL_MIN_S = 0.2                   # :653
RESTART_JOIN_S = 3.0                   # :791

BUDGET = dict(queries_per_tick=0, bytes_tx=0, bytes_rx=0, min_gap_ms=0, fits_1s=True, alternate=None,
              note="시리얼 포트 없음 — 1초 행은 촬영 스레드 상태만. 촬영은 CAMERA_INTERVAL_S(1 s)마다 한 장(USB 카메라, "
                   "JPG 한 장 약 0.3~1 MB — 미확인, bench 에서 실측)")


# ═══════════════════════ 장치(cv2 — 여기서만, 늦게 import) ═══════════════════════
class CameraBackend:
    """장치 열기·읽기·회전·JPG 인코딩. 기본은 cv2. 시험은 같은 메서드를 가진 가짜를 넘긴다."""

    def open(self, index: int) -> Optional[Any]:
        import cv2
        cap = cv2.VideoCapture(index)
        if not cap.isOpened():
            cap.release()
            return None
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, FRAME_W)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, FRAME_H)
        return cap

    def read(self, cap) -> Tuple[bool, Any]:
        return cap.read()

    def rotate(self, frame):
        import cv2
        return cv2.rotate(frame, cv2.ROTATE_90_CLOCKWISE)

    def encode_jpg(self, frame) -> Optional[bytes]:
        import cv2
        ok, buf = cv2.imencode(".jpg", frame)
        return buf.tobytes() if ok else None

    def release(self, cap) -> None:
        try:
            cap.release()
        except Exception:
            pass


def base_dir() -> Path:
    """v1 _local_fallback_root 의 기준 — 배포(frozen)는 exe 옆, 소스 실행은 저장소 루트."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).resolve().parents[3]


def local_fallback_root() -> Path:
    return base_dir() / "Logs_LocalFallback" / "Camera_Logs"


def free_gb(path: Path) -> float:
    """경로가 속한 디스크의 여유(GB). 조회 실패면 inf(util/camera_recorder.py:622-627)."""
    try:
        return shutil.disk_usage(path).free / (1024 ** 3)
    except Exception:
        return float("inf")


def blocks() -> List[DeviceBlock]:
    return [DeviceBlock("camera", "카메라", (
        FieldDef("running", FType.BOOL, "", "촬영 중", source="driver", summary="any"),
        FieldDef("mode", FType.STR, "", "모드", source="driver", enum_labels=tuple((m, m) for m in MODES)),
        FieldDef("users", FType.STR, "", "참여자", source="driver", detail=True, desc="쉼표로 묶은 owner"),
        FieldDef("frames", FType.INT, "", "이번 세션 장 수", source="driver", detail=True),
        FieldDef("last_msg", FType.STR, "", "마지막 메시지", source="driver", detail=True),
    ), desc="USB 카메라 1대(공유) — RF 매칭 패널 사진")]


@dataclass(frozen=True)
class CameraEvent:
    kind: str          # log / disk_low / open_failed / started / finished / promoted / read_failed
    detail: str


class _Session:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.stop = False
        self.thread: Optional[threading.Thread] = None
        self.frames = 0
        self.save_dir: Optional[Path] = None
        self.ended = False      # 촬영 함수가 끝남(스레드가 아직 정리 중이어도) — 시계 참여 해제보다 먼저 세운다

    def alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive() and not self.ended


class CameraDriver:
    def __init__(self, clock, cfg: Dict[str, Any], on_event: Optional[Callable[[CameraEvent], None]] = None,
                 backend: Optional[CameraBackend] = None, notify_cb: Optional[Callable[[str], None]] = None,
                 free_gb_fn: Callable[[Path], float] = free_gb, writes_allowed: bool = True) -> None:
        self.clock, self.cfg = clock, cfg
        self.on_event = on_event
        self.backend = backend or CameraBackend()
        self._notify_cb = notify_cb
        self._free_gb = free_gb_fn
        self.writes_allowed = writes_allowed            # 카메라는 장비에 쓰지 않는다 — bench 에서도 촬영(읽기)은 v1 과 같게 허용
        self._lock = threading.Lock()                   # 세션 잠금(v1 _lock)
        self._cb_lock = threading.Lock()                # 로그 콜백 잠금(v1 _cb_lock) — _lock 과 겹치지 않는다
        self._users: set = set()
        self._log_cb: Optional[Callable[[str], None]] = None
        self._log_cbs: Dict[str, Callable[[str], None]] = {}
        self._mode = "ALL"
        self._sess: Optional[_Session] = None
        self._notified = False
        self.last_msg = ""
        self._source = Source("camera", CacheWorker(clock, name="camera"), self.source_read, ("camera",), 900)

    # ───────── 설정 ─────────
    @property
    def camera_index(self) -> int:
        return int(self.cfg["CAMERA_INDEX"])

    @property
    def interval_s(self) -> float:
        return max(INTERVAL_MIN_S, float(self.cfg["CAMERA_INTERVAL_S"]))

    # ───────── 로그·알림(v1 _log·_notify) ─────────
    def set_log_callback(self, cb: Optional[Callable[[str], None]], owner: str = "") -> None:
        with self._cb_lock:
            if owner:
                if cb is None:
                    self._log_cbs.pop(owner, None)
                else:
                    self._log_cbs[owner] = cb
            else:
                self._log_cb = cb

    def _log(self, msg: str, kind: str = "log") -> None:
        self.last_msg = msg
        if self.on_event:
            try:
                self.on_event(CameraEvent(kind, msg))
            except Exception:
                pass
        with self._cb_lock:
            cbs = [self._log_cb] + list(self._log_cbs.values())
        cbs = [cb for cb in cbs if cb is not None]
        if not cbs:
            logger.info(msg)
            return
        for cb in cbs:
            try:
                cb(msg)
            except Exception:
                pass

    def _notify(self, msg: str) -> None:
        cb = self._notify_cb
        if cb is None or self._notified:
            return
        self._notified = True
        try:
            cb(msg)
        except Exception:
            pass

    # ───────── 상태 ─────────
    @property
    def current_mode(self) -> str:
        with self._lock:
            return self._mode

    @property
    def current_users(self) -> set:
        with self._lock:
            return set(self._users)

    @property
    def is_running(self) -> bool:
        s = self._sess
        return bool(s is not None and s.alive() and not s.stop)

    def _desired_mode(self) -> str:
        """참여자로 정한 모드(잠금 안에서). 0명 "", 1명 그 owner 의 모드, 2명 이상 ALL. 진행 중인 ALL 은 유지(강등 금지)."""
        s = self._sess
        if self._mode == "ALL" and self._users and s is not None and s.alive() and not s.stop:
            return "ALL"
        if not self._users:
            return ""
        if len(self._users) == 1:
            return OWNER_MODE.get(next(iter(self._users)), "ALL")
        return "ALL"

    # ───────── 시작·정지(v1 start·stop 과 같은 규칙) ─────────
    def start(self, mode: str = "ALL", owner: str = "") -> bool:
        """촬영 시작 — 바로 돌아온다. owner 가 있으면 mode 는 무시하고 참여자 집합이 정한다:
        이미 그 모드로 촬영 중이면 아무것도 하지 않고(재시작 금지), 모드가 바뀌면(둘째 참여자 → ALL) 1회만 재시작."""
        promote_from = ""
        with self._lock:
            want = str(mode).upper()
            if not owner or owner not in self._users:
                self._notified = False                  # 새 공정 → 알림 1회 다시 허용
            if owner:
                self._users.add(owner)
                want = self._desired_mode() or "ALL"
                s = self._sess
                alive = bool(s is not None and s.alive())
                if alive and self._mode == want:
                    return True
                if alive:
                    promote_from = self._mode
            old = self._sess
            if old is not None and old.alive():
                old.stop = True
                self.clock.notify()
                old.thread.join(RESTART_JOIN_S)
            self._mode = want if want in MODES else "ALL"
            sess = _Session(self._mode)
            self._sess = sess
            sess.thread = self.clock.start_thread(lambda: self._run_session(sess), f"camera-{self._mode}")
            users_now = sorted(self._users)
        if promote_from:
            self._log(f"카메라 모드 승격 {promote_from}→{want} (참여: {users_now})", "promoted")
        return True

    def stop(self, owner: str = "") -> bool:
        """정지 요청 — 바로 돌아온다. owner 가 있으면 참여자에서 빼고, 남은 참여자가 없을 때만 정지(True), 남으면 False."""
        with self._lock:
            if owner:
                self._users.discard(owner)
                if self._users:
                    return False
            else:
                self._users.clear()
            s = self._sess
        with self._cb_lock:
            self._log_cbs.clear()
        if s is not None:
            s.stop = True
            self.clock.notify()
        return True

    def join(self, timeout: float = 5.0) -> None:
        s = self._sess
        if s is not None and s.thread is not None:
            s.thread.join(timeout)

    # ───────── 1초 읽기 ─────────
    def source(self) -> Source:
        return self._source

    def source_read(self, session, seq: int) -> Dict[str, Any]:
        s = self._sess
        return {"camera.running": self.is_running, "camera.mode": self.current_mode if self.is_running else None,
                "camera.users": ",".join(sorted(self.current_users)) or None,
                "camera.frames": None if s is None else s.frames, "camera.last_msg": self.last_msg or None}

    # ───────── 촬영 스레드(v1 _record_loop) ─────────
    def _resolve_root(self) -> Path:
        root = Path(str(self.cfg["CAMERA_LOG_ROOT"]))
        try:
            root.mkdir(parents=True, exist_ok=True)
            return root
        except Exception as e:
            fb = local_fallback_root()
            logger.warning("[camera] 저장 루트 생성 실패(%s: %s) → 폴백 경로 사용: %s", root, e, fb)
            fb.mkdir(parents=True, exist_ok=True)
            return fb

    def _wait(self, sess: _Session, dt: float) -> None:
        self.clock.wait_until(self.clock.monotonic() + max(0.0, dt), lambda: sess.stop)

    def _now_str(self, fmt: str) -> str:
        return _dt.datetime.fromtimestamp(self.clock.wall()).strftime(fmt)

    def _run_session(self, sess: _Session) -> None:
        """촬영 스레드 본체 — 끝나면(어느 길로든) ended 를 먼저 세운다: 가상 시계는 스레드가 참여를 풀 때 시각을 옮기므로,
        OS 스레드가 아직 살아 있는 짧은 사이에 is_running 이 참으로 보이지 않게."""
        try:
            self._record_loop(sess)
        finally:
            sess.ended = True

    def _record_loop(self, sess: _Session) -> None:
        # 0) 디스크 여유 — 카메라를 열기 전에
        root = self._resolve_root()
        thr = float(self.cfg["CAMERA_MIN_FREE_GB"])
        free = self._free_gb(root)
        if free < thr:
            msg = f"디스크 여유 부족({free:.1f}GB < {thr:.1f}GB) — 이번 공정 영상 미기록"
            self._log(msg, "disk_low")
            self._notify(msg)
            return
        # 1) 장치 열기 — 저장 폴더보다 먼저(열기 실패로 빈 세션 폴더가 NAS 로 가면 판독 워커가 빈 것으로 영구 기록)
        try:
            cap = self.backend.open(self.camera_index)
        except Exception as e:                       # cv2 없음 등
            cap = None
            self._log(f"카메라 장치 오류: {e!r}", "open_failed")
        if cap is None:
            self._log(f"카메라 열기 실패 (index={self.camera_index}) — 이번 공정 영상 미기록", "open_failed")
            return
        # 2) 저장 폴더 {root}/{모드}/{YYYYmmdd_HHMMSS}/
        save_dir = root / MODE_FOLDER.get(sess.mode, "ALL") / self._now_str("%Y%m%d_%H%M%S")
        try:
            save_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.backend.release(cap)
            self._log(f"폴더 생성 실패: {e}")
            return
        sess.save_dir = save_dir
        self._log(f"카메라 시작 mode={sess.mode} → {save_dir}", "started")
        err_count = 0
        img_count = 0
        try:
            while not sess.stop:
                t0 = self.clock.monotonic()
                ret, frame = self.backend.read(cap)
                if not ret:
                    err_count += 1
                    if err_count > READ_FAIL_MAX:
                        self._log("카메라 읽기 반복 실패", "read_failed")
                        break
                    self._wait(sess, READ_FAIL_PAUSE_S)       # v1 sleep(0.3) — v2 는 정지 요청에 바로 깬다
                    continue
                err_count = 0
                img_count += 1
                sess.frames = img_count
                if img_count % DISK_RECHECK_EVERY == 0:
                    free = self._free_gb(root)
                    if free < thr:
                        msg = f"디스크 여유 부족({free:.1f}GB < {thr:.1f}GB) — 촬영 중단"
                        self._log(msg, "disk_low")
                        self._notify(msg)
                        break
                frame = self.backend.rotate(frame)       # 분석 쪽과 같은 방향(세로 1080×1920)
                try:
                    img_name = save_dir / f"{self._now_str('%H%M%S')}_{img_count:04d}.jpg"
                    data = self.backend.encode_jpg(frame)  # imencode + write_bytes — 한글 경로 안전
                    if data is not None:
                        img_name.write_bytes(data)
                    else:
                        self._log("이미지 인코딩 실패")
                except Exception as e:
                    self._log(f"이미지 저장 실패: {e}")
                elapsed = self.clock.monotonic() - t0
                self._wait(sess, self.interval_s - elapsed)
        except Exception as e:
            self._log(f"루프 오류: {e}")
        finally:
            self.backend.release(cap)
            self._log(f"완료 — 촬영 {img_count}장 | 폴더: {save_dir}", "finished")
            if img_count == 0:                           # 한 장도 못 찍었으면 빈 폴더를 남기지 않는다
                try:
                    save_dir.rmdir()
                    self._log(f"촬영 0장 → 빈 폴더 삭제: {save_dir.name}")
                except Exception:
                    pass
