"""플라즈마 클리닝 공정 로그 파일 PC_*.log — v1 runtime/plasma_cleaning_runtime.py 의 _open_run_log·append_log·_close_run_log 파일 출력.

v1 그대로(PC-033~037·039)
- 경로 {LOG_ROOT_DIR}\\Plasma_Cleaning\\PC_{YYYYmmdd_HHMMSS}_CH{n}.log(append, utf-8, 텍스트 모드 — 윈도우 줄 끝 변환 같음).
  폴더를 못 만들면 {LOCAL_FALLBACK_ROOT}\\Plasma_Cleaning, 파일을 못 열면 '런 로그 파일 열기 실패 → 파일 기록 없이 진행: …'(부르는 쪽
  로그) 후 공정은 계속.
- 머리 7줄('# ==== Plasma Cleaning Run ====' … '# ============================') — 2591-2602.
- 줄 '[HH:MM:SS] {출처}: {메시지}'. 파일이 열려 있지 않을 때의 줄은 SYSTEM 일자 로그 system_log_append('PC', '{출처}: {메시지}').
- '[poll]' 로 시작하는 MFC*/IG* 출처 줄은 화면에도 파일에도 남기지 않는다(2331-2337 — [이상] 주석은 '표시만', v1 그대로 D109).
v2 에서 다른 것: v1 은 1 s 타이머마다 버퍼를 한 번에 썼다(최대 2000줄). v2 는 줄마다 쓰기 스레드(QueueWriter) 큐에 넣는다 — 순서·내용 같음.
  닫기도 같은 큐 뒤에(남은 줄을 다 쓴 뒤 close — v1 '남은 버퍼 동기 기록 → close').
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.paths import Paths
from v2.ch12.outputs.system_log import SystemLog

DIR_NAME = "Plasma_Cleaning"
_TS = re.compile(r"^\[\d{2}:\d{2}:\d{2}\]\s*(.*)$")


def hidden(src: str, msg: str) -> bool:
    """2331-2337 — MFC*/IG* 의 '[poll]' 줄은 버린다."""
    try:
        return isinstance(msg, str) and msg.lstrip().startswith("[poll]") and isinstance(src, str) \
            and (src.startswith("MFC") or src.startswith("IG"))
    except Exception:
        return False


def header_lines(now: datetime, ch: int, gas_name: str, pressure_name: str, p) -> list:
    return ["# ==== Plasma Cleaning Run ====\n",
            f"# started_at = {now.isoformat()}\n",
            f"# chamber = CH{int(ch)}\n",
            f"# mfc_gas = {gas_name}\n",
            f"# mfc_pressure = {pressure_name}\n",
            (f"# params: gas_idx={p.gas_idx}, flow={p.gas_flow_sccm:.1f} sccm, "
             f"IG_target={p.target_pressure:.3e} Torr, "
             f"SP4={p.sp4_setpoint_mTorr:.2f} mTorr, RF={p.rf_power_w:.1f} W, "
             f"time={p.process_time_min:.2f} min\n"),
            "# ============================\n"]


class PcRunLog:
    def __init__(self, cfg: Mapping[str, Any], *, writer: Optional[QueueWriter] = None,
                 system_log: Optional[SystemLog] = None, now: Callable[[], datetime] = datetime.now) -> None:
        self.cfg = cfg
        self.paths = Paths(cfg)
        self._own_writer = writer is None
        self.writer = writer if writer is not None else QueueWriter("pc-runlog")
        self.system_log = system_log if system_log is not None else SystemLog(cfg, writer=self.writer, now=now)
        self.now = now
        self._fp = None
        self.path: Optional[Path] = None
        self.lines_written = 0

    @property
    def is_open(self) -> bool:
        return self._fp is not None

    def open(self, ch: int, p, gas_name: str, pressure_name: str) -> Optional[Path]:
        """파일을 열고 머리를 쓴다(부른 스레드에서 — v1 도 루프 스레드에서 동기로 열었다). 실패면 예외(부르는 쪽이 문구 로그)."""
        if self._fp is not None:
            self.close()
        ts = self.now().strftime("%Y%m%d_%H%M%S")
        fname = f"PC_{ts}_CH{int(ch)}.log"
        d = self.paths.log_root() / DIR_NAME
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            d = self.paths.fallback_root() / DIR_NAME
            try:
                d.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
        path = d / fname
        fp = open(path, "a", encoding="utf-8", buffering=1)
        for line in header_lines(self.now(), ch, gas_name, pressure_name, p):
            fp.write(line)
        fp.flush()
        self._fp, self.path = fp, path
        return path

    def line(self, line: str) -> None:
        """'[HH:MM:SS] 출처: 메시지' 한 줄 — 열려 있으면 파일, 아니면 SYSTEM 일자 로그(앞 시각을 뗌)."""
        fp = self._fp
        if fp is None:
            m = _TS.match(str(line))
            self.system_log.append("PC", m.group(1) if m else str(line))
            return
        self.writer.put(self._write, fp, str(line))

    def _write(self, fp, line: str) -> None:
        try:
            fp.write(line + "\n")
            fp.flush()
            self.lines_written += 1
        except Exception:
            pass

    def close(self) -> None:
        fp, self._fp = self._fp, None
        if fp is not None:
            self.writer.put(self._close_fp, fp)

    @staticmethod
    def _close_fp(fp) -> None:
        try:
            fp.close()
        except Exception:
            pass

    def flush(self, timeout_s: float = 10.0) -> bool:
        return self.writer.flush(timeout_s)

    def stop(self, timeout_s: float = 5.0) -> None:
        self.close()
        if self._own_writer:
            self.writer.stop(timeout_s)
        else:
            self.writer.flush(timeout_s)
