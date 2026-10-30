"""v1 기록 출력의 경로 규칙(한 곳) + 로컬 먼저 → 사본(G:·NAS) 보내기 스레드(D07).

경로 규칙(v1 근거 — 위치·파일명은 v1 과 같다)
- 로그 뿌리 LOG_ROOT_DIR(C:\\VanaM_Logs\\CH1&2): 공정 txt CH{n}\\, SYSTEM\\, ERROR\\, RUNTIME_DUMP\\, selfwatch\\, CH2_Heater\\
  (lib/config_common.py:645, runtime/chamber_runtime.py:560, util/system_log.py:31-39, util/app_logging.py:33-35,
  util/runtime_dump.py:20-22, util/self_watch.py:41-47, lib/heater_logger.py:27-29)
- 로컬 폴백 뿌리 LOCAL_FALLBACK_ROOT = exe(소스 실행이면 저장소) 폴더\\Logs_LocalFallback, 하위 CH1·CH2·Server·PLC_Coil·
  Plasma_Cleaning·TSP·ERROR·RUNTIME_DUMP·RGA·SYSTEM(lib/config_common.py:8-31). cwd 는 쓰지 않는다.
- Sputter Calib CSV: SPUTTER_CALIB_DB_DIR\\Ch{n}_log.csv, pending LOCAL_FALLBACK_CH{n}_DIR\\Ch{n}_pending.csv
- 엑셀: G:/공유 드라이브/VanaM_Sputter/Process_log/CH{n}.xlsx(Main — GDRIVE_LOG_DIR 무시, v1 그대로),
  PC 시트는 GDRIVE_LOG_DIR. 실패 pending: LOCAL_FALLBACK_ROOT\\CH{n}_pending.xlsx / PC 는 <G: 폴더>\\LocalFallback
- PLC 코일 CSV: PLC_COIL_LOG_NAS_DIR\\YYYYMMDD.csv, 폴백 PLC_COIL_LOG_LOCAL_DIR(비면 LOCAL_FALLBACK_ROOT\\PLC_Coil)
- 두께: CHn_THICKNESS_LOG_DIR\\CH{n}\\Thickness_YYYYMMDD.csv(G: 면 기본값), 폴백 LOCAL_FALLBACK_ROOT\\Thickness\\CH{n}
- 히터: CH2_HEATER_LOG_DIR(비면 LOG_ROOT_DIR\\CH2_Heater)\\CH2_HEATER_*.csv, 폴백 LOCAL_FALLBACK_ROOT\\CH2_Heater
- 카메라: CAMERA_LOG_ROOT\\{CH1|CH2|CLEANING|ALL}\\{세션}\\{HHMMSS}_{NNNN}.jpg, 폴백 exe\\Logs_LocalFallback\\Camera_Logs
- Robot CSV: HOST_LOG_NAS_DIR\\Robot_YYYYMMDD.csv(+_lock\\Robot.lock), HOST_LOG_LOCAL_DIR\\(Robot_YYYYMMDD_sputter.csv·
  pending_sputter.csv·open_sputter.json)
- OES CSV(워커가 씀): OES_LOCAL_DIR(C:\\VanaM_Logs\\OES)\\CH{n}\\OES_Data_YYYYmmdd_HHMMSS.csv(device/oes.py:37-49,93,171)
  — 환경변수 OES_LOCAL_BASE 가 있으면 그 값(v1 그대로)
- RGA CSV(워커가 씀): CHn_RGA_CSV_PATH(G:\\공유 드라이브\\VanaM_Sputter\\RGA\\Ch.{n}\\RGA_spectrums.csv),
  폴백 LOCAL_FALLBACK_ROOT\\RGA\\Ch.{n}\\RGA_spectrums.csv(device/rga.py:133-153, lib/config_common.py:128-136)

D07 — 로컬 먼저, 사본만
- v1 이 G: 에 정본을 직접 쓰던 출력(Calib CSV·Main/PC 엑셀)은 OUTPUT_LOCAL_FIRST_DIR(로컬)에 같은 이름·형식의 정본을
  먼저 쓰고, G: 의 v1 파일에는 CopySender 가 사본(새 행)을 보낸다. G: 쪽 파일 결과는 v1 과 같다.
- CopySender: 이름 붙은 작업(돌려주는 값 True=끝, False=다음에 다시)을 스레드 하나에서 돌린다. 실패한 작업은
  OUTPUT_COPY_RETRY_S 마다 다시. 같은 이름 작업은 하나로 합친다(마지막 것). 정본은 로컬이라 사본이 밀려도 잃지 않는다.
- v1 이 이미 로컬에 쓰던 출력(공정 txt·PLC 코일·두께·히터·카메라·selfwatch·ERROR·SYSTEM·덤프)은 그대로 로컬에 쓰고
  NAS 반영은 v1 처럼 log_sync 워커(apps/log_sync_service — 독립 도구, LOG_SYNC_PAIRS 의 src 가 LOG_ROOT_DIR)가 한다.
"""
from __future__ import annotations

import os
import shutil
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional

from v2.ch12.params import outputs as P

V1_GDRIVE_MAIN_DIR = Path("G:/공유 드라이브/VanaM_Sputter/Process_log")    # util/gdrive_logger.py:30 (하드코딩)
V1_THICKNESS_DEFAULT_DIR = r"C:\VanaM_Logs\CH1&2\Thickness"               # lib/thickness_config.py DEFAULT_LOG_DIR
V1_OES_DEFAULT_DIR = r"C:\VanaM_Logs\OES"                                 # device/oes.py:39
V1_RGA_DEFAULT = r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.{n}\RGA_spectrums.csv"   # lib/config_common.py:128-131


def exe_base_dir() -> Path:
    """폴백 기준 폴더 — 묶은 exe 면 exe 폴더, 소스 실행이면 저장소 루트(v1 lib/config_common.py:8-13 과 같은 자리)."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[3]


class Paths:
    """cfg 사전에서 경로를 '부를 때' 계산한다(v1 DEC-033 — 호출 시점에 읽음)."""

    def __init__(self, cfg: Optional[Mapping[str, Any]] = None) -> None:
        self.cfg = cfg if cfg is not None else {}

    def _g(self, key: str) -> Any:
        return P.get(self.cfg, key)

    # ── 뿌리 ──
    def log_root(self) -> Path:
        return Path(str(self._g("LOG_ROOT_DIR")))

    def fallback_root(self) -> Path:
        v = str(self._g("LOCAL_FALLBACK_ROOT") or "")
        return Path(v) if v else exe_base_dir() / "Logs_LocalFallback"

    def fallback_ch(self, ch: int) -> Path:
        return self.fallback_root() / f"CH{int(ch)}"

    def local_first_root(self) -> Path:
        return Path(str(self._g("OUTPUT_LOCAL_FIRST_DIR")))

    # ── 공정 txt·SYSTEM·ERROR·덤프 ──
    def process_txt_dir(self, ch: int) -> Path:
        return self.log_root() / f"CH{int(ch)}"

    def system_dirs(self) -> "tuple[Path, Path]":
        return self.log_root() / "SYSTEM", self.fallback_root() / "SYSTEM"

    def error_dirs(self) -> "tuple[Path, Path]":
        return self.log_root() / "ERROR", self.fallback_root() / "ERROR"

    def dump_dirs(self) -> "tuple[Path, Path]":
        return self.log_root() / "RUNTIME_DUMP", self.fallback_root() / "RUNTIME_DUMP"

    def selfwatch_dir(self) -> Path:
        d = str(self._g("SELFWATCH_DIR") or "")
        return Path(d) if d else self.log_root() / "selfwatch"

    # ── Calib CSV ──
    def calib_file(self, ch: int) -> Path:
        return Path(str(self._g("SPUTTER_CALIB_DB_DIR"))) / f"Ch{int(ch)}_log.csv"

    def calib_pending(self, ch: int) -> Path:
        return self.fallback_ch(ch) / f"Ch{int(ch)}_pending.csv"

    def calib_local_first(self, ch: int) -> Path:
        return self.local_first_root() / "Calib" / f"Ch{int(ch)}_log.csv"

    # ── 엑셀 ──
    def excel_main_dir(self) -> Path:
        return V1_GDRIVE_MAIN_DIR

    def excel_pc_dir(self) -> Path:
        return Path(str(self._g("GDRIVE_LOG_DIR")))

    def excel_local_first(self, ch: int) -> Path:
        return self.local_first_root() / "Process_log" / f"CH{int(ch)}.xlsx"

    # ── PLC 코일 ──
    def plc_coil_dirs(self) -> "tuple[Path, Path]":
        loc = str(self._g("PLC_COIL_LOG_LOCAL_DIR") or "")
        return (Path(str(self._g("PLC_COIL_LOG_NAS_DIR"))),
                Path(loc) if loc else self.fallback_root() / "PLC_Coil")

    # ── 두께·히터 ──
    def thickness_root(self, ch: int) -> str:
        d = str(self._g(f"CH{int(ch)}_THICKNESS_LOG_DIR") or "").strip() or V1_THICKNESS_DEFAULT_DIR
        if d[:2].upper() == "G:":
            d = V1_THICKNESS_DEFAULT_DIR
        return d

    def thickness_fallback(self, ch: int) -> Path:
        return self.fallback_root() / "Thickness" / f"CH{int(ch)}"

    def heater_dir(self) -> Path:
        d = str(self._g("CH2_HEATER_LOG_DIR") or "")
        return Path(d) if d else self.log_root() / "CH2_Heater"

    def heater_fallback(self) -> Path:
        return self.fallback_root() / "CH2_Heater"

    # ── 카메라 ──
    def camera_root(self) -> Path:
        return Path(str(self._g("CAMERA_LOG_ROOT")))

    def camera_fallback(self) -> Path:
        return exe_base_dir() / "Logs_LocalFallback" / "Camera_Logs"

    # ── Robot CSV ──
    def robot_nas_dir(self) -> Path:
        return Path(str(self._g("HOST_LOG_NAS_DIR")))

    def robot_local_dir(self) -> Path:
        return Path(str(self._g("HOST_LOG_LOCAL_DIR")))

    # ── OES·RGA(워커가 씀 — 경로 규칙만) ──
    def oes_dir(self, ch: int) -> Path:
        env = (os.environ.get("OES_LOCAL_BASE") or "").strip()
        base = env or str(self.cfg.get("OES_LOCAL_DIR", "") or "").strip() or V1_OES_DEFAULT_DIR
        return Path(base) / f"CH{int(ch)}"

    @staticmethod
    def oes_filename(ts) -> str:
        return f"OES_Data_{ts.strftime('%Y%m%d_%H%M%S')}.csv"

    def rga_csv(self, ch: int) -> "tuple[str, str]":
        """(정본, 폴백) — v1 device/rga.py:_resolve_csv_paths 와 같은 규칙."""
        n = int(ch)
        primary = str(self.cfg.get(f"CH{n}_RGA_CSV_PATH", "") or V1_RGA_DEFAULT.format(n=n))
        fallback = str(self.fallback_root() / "RGA" / f"Ch.{n}" / "RGA_spectrums.csv")
        return primary, fallback


# ───────────────────────── 사본 보내기 스레드 ─────────────────────────
class CopySender:
    """로컬 정본 → G:·NAS 사본. 작업 = 돌려주는 값 True(끝)/False(다음에 다시). 예외도 '다음에 다시'."""

    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, name: str = "copy-sender",
                 retry_s: Optional[float] = None) -> None:
        self.cfg = cfg if cfg is not None else {}
        self._retry_s = retry_s
        self._jobs: Dict[str, Callable[[], bool]] = {}
        self._order: list = []
        self._lk = threading.Lock()
        self._kick = threading.Event()
        self._stop = threading.Event()
        self._busy = False
        self._rounds = 0
        self.sent = 0
        self.failures = 0
        self.last_error = ""
        self._thread = threading.Thread(target=self._loop, name=name, daemon=True)
        self._thread.start()

    def retry_s(self) -> float:
        if self._retry_s is not None:
            return float(self._retry_s)
        try:
            return max(1.0, float(P.get(self.cfg, "OUTPUT_COPY_RETRY_S")))
        except Exception:
            return 30.0

    def submit(self, name: str, fn: Callable[[], bool]) -> None:
        with self._lk:
            if name not in self._jobs:
                self._order.append(name)
            self._jobs[name] = fn
        self._kick.set()

    def kick(self) -> None:
        self._kick.set()

    def pending_names(self) -> list:
        with self._lk:
            return list(self._order)

    def _run_round(self) -> None:
        with self._lk:
            names = list(self._order)
            self._busy = True
        try:
            for name in names:
                with self._lk:
                    fn = self._jobs.get(name)
                if fn is None:
                    continue
                try:
                    ok = bool(fn())
                except Exception as e:
                    ok = False
                    self.last_error = f"{name}: {e!r}"
                with self._lk:
                    if ok:
                        self.sent += 1
                        if self._jobs.get(name) is fn:      # 그 사이 새로 들어온 같은 이름 작업은 남긴다
                            self._jobs.pop(name, None)
                            self._order.remove(name)
                    else:
                        self.failures += 1
        finally:
            with self._lk:
                self._busy = False
                self._rounds += 1

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._kick.clear()
            self._run_round()
            self._kick.wait(timeout=self.retry_s())

    def flush(self, timeout_s: float = 10.0) -> bool:
        """지금 있는 작업을 한 번 더 돌리고 기다린다. 남은(실패한) 작업이 없으면 True. 시험·종료용."""
        t_end = time.monotonic() + timeout_s
        with self._lk:
            need = self._rounds + (2 if self._busy else 1)
            self._kick.set()
        while time.monotonic() < t_end:
            with self._lk:
                if self._rounds >= need:
                    return not self._order
            time.sleep(0.002)
        return False

    def stop(self, timeout_s: float = 5.0) -> None:
        self._stop.set()
        self._kick.set()
        self._thread.join(timeout_s)


def copy_file_job(src: Path, dst: Path) -> Callable[[], bool]:
    """파일 통째 사본(임시 이름에 복사 → os.replace). 실패하면 False(다음에 다시)."""
    def _job() -> bool:
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(dst) + ".copying")
        shutil.copyfile(str(src), str(tmp))
        os.replace(str(tmp), str(dst))
        return True
    return _job
