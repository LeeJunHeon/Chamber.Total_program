"""Sputter Calib CSV(Ch{n}_log.csv) — v1 controller/data_logger.py 의 파일 출력(LOG-023~040).

열·형식(v1 그대로)
- 머리글 HEADER 36열 + CH2 만 HEATER_COLUMNS 5열 + 두께 채널(SUPPORTS_THICKNESS) 만 THICKNESS_COLUMNS 13열(맨 뒤).
- utf-8-sig, csv.DictWriter(줄 끝 CRLF), 1공정 1행(성공·실패·STOP 모두). Result SUCCESS/STOP/FAIL.
- 숫자 형식: Base '%.2e', 유량 '%.2f'(표본 없으면 0.00), Working Pressure '%.4f', 파워 평균 '%.2f'(사용+표본 있을 때만),
  Shutter Delay '%.3f', Process Time = str(float)(포맷 없음 — v1 그대로), 히터 '%.1f'/'%.0f', 두께 '{:g}'/'%.2f'/'%.4f'.

파일 처리(v1 그대로 + D07)
- 기록할 때만 열고 쓰고 바로 닫는다(열어 두면 G: 가 올리지 않는다 — data_logger.py:157-163).
- 시작 때 한 번: 기존 파일 머리글이 새 머리글과 다르면 전 행을 새 머리글로 .tmp 에 다시 쓰고 os.replace(옛 'Process Note'
  값을 'Process Name' 으로). 실패하면 실제 파일 머리글 순서로 기록(새 열 값은 버려짐 — 'Sputter Calib CSV 머리글 갱신 실패 …').
- 로컬 pending(LOCAL_FALLBACK_CH{n}_DIR\\Ch{n}_pending.csv, 머리글 = 새 머리글)을 본파일에 합친 뒤 지움, 실패하면 1회 재시도.
- D07: v1 은 G: 본파일에 바로 썼다. v2 는 ① 로컬 정본(OUTPUT_LOCAL_FIRST_DIR\\Calib\\Ch{n}_log.csv)에 먼저 쓰고 ② 같은 행을 v1
  pending 파일에 넣은 뒤 ③ 사본 스레드(CopySender)가 v1 병합 규칙으로 G: 본파일에 보낸다(바로 깨움). 실패하면 pending 에 남아
  다음 회차·다음 기록 때 다시. G: 쪽 파일 바이트는 v1 과 같다(앞 행이 pending 에 남았을 때 v1 은 새 행을 먼저 쓸 수 있었지만
  v2 는 순서를 지킨다). 보낸 만큼만 pending 에서 지운다(중간 실패 때 v1 처럼 중복되지 않게).
- 로그 문구(CH 로그 'CSV' 출처로 log_func 에): v1 문구 그대로.

부르는 법
  c = CalibCsv(ch, cfg, thickness_columns=bool(cfg["CHn_SUPPORTS_THICKNESS"]), log_func=fn, writer=w, sender=s)
  c.record(stats: RunStats, was_successful)   # 공정 끝(엔진). was_successful=None 이면 기록 생략(v1 그대로)
  row = build_row(ch, stats, was_successful, thickness_columns)   # 순수 함수(dict)
"""
from __future__ import annotations

import csv
import os
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter
from v2.ch12.outputs.log_hub import FixedCsvDictAppender
from v2.ch12.outputs.paths import CopySender, Paths
from v2.ch12.outputs.run_stats import RunStats

HEADER: List[str] = [
    "Timestamp", "Process Name", "Result", "Base Pressure",
    "Main Shutter", "Shutter Delay", "Integration Time", "Power Select",
    "G1 Target", "G2 Target", "G3 Target",
    "Ar flow", "O2 flow", "N2 flow",
    "Working Pressure", "Process Time",
    "RF: For.P", "RF: Ref. P",
    "DC: V", "DC: I", "DC: P",
    "DC2: V", "DC2: I", "DC2: P",
    "RF Pulse: P", "RF Pulse: Freq", "RF Pulse: Duty Cycle",
    "DC Pulse: P", "DC Pulse: V", "DC Pulse: I", "DC Pulse: Freq", "DC Pulse: Duty Cycle",
    "DC Pulse: Off Time",
    "RF Pulse: For.P", "RF Pulse: Ref.P", "Chuck Position",
]
HEATER_COLUMNS: List[str] = ["Heater: SV", "Heater: PV Avg", "Heater: PV Min", "Heater: PV Max", "Heater: MV Avg"]
THICKNESS_COLUMNS: List[str] = [
    "Thickness: End Mode", "Thickness: Target nm", "Thickness: Final nm", "Thickness: Avg Rate nm/s",
    "Thickness: Tooling %", "Thickness: Density", "Thickness: Z", "Thickness: Chuck", "Thickness: Gun",
    "Thickness: Fallback Reason", "Thickness: Jump Count", "Thickness: Material", "Thickness: Factor Source",
]


def header_for(ch: int, thickness_columns: bool) -> List[str]:
    h = list(HEADER)
    if int(ch) == 2:
        h.extend(HEATER_COLUMNS)
    if thickness_columns:
        h.extend(THICKNESS_COLUMNS)
    return h


def _dcp_off_time_text(pp: Mapping[str, Any]) -> str:
    v = pp.get("dc_pulse_off_time_us", None)
    if v not in (None, ""):
        try:
            return f"{float(v):.1f}"
        except Exception:
            pass
    raw = pp.get("dc_pulse_off_time", None)
    if raw in (None, ""):
        return ""
    if isinstance(raw, str):
        return "DC" if raw.strip().upper() == "DC" else ""
    try:
        return f"{float(raw):.1f}"
    except Exception:
        return ""


def build_row(ch: int, st: RunStats, was_successful: bool, thickness_columns: bool) -> Dict[str, str]:
    """v1 finalize_and_write_log 의 행(data_logger.py:478-631)."""
    pp = st.params

    def _avg(seq: List[float]) -> float:
        return (sum(seq) / len(seq)) if seq else 0.0

    base_pressure = min(st.ig) if st.ig else float(pp.get("base_pressure", 0.0))
    use_rf = bool(pp.get("use_rf_power", False))
    use_dc = bool(pp.get("use_dc_power", False))
    use_dc2 = bool(pp.get("use_dc_power2", False))
    use_rfp = bool(pp.get("use_rf_pulse", False))
    use_dcp = bool(pp.get("use_dc_pulse", False))
    rfp_p, rfp_f, rfp_d = pp.get("rf_pulse_power", None), pp.get("rf_pulse_freq", None), pp.get("rf_pulse_duty", None)
    dcp_p, dcp_f, dcp_d = pp.get("dc_pulse_power", None), pp.get("dc_pulse_freq", None), pp.get("dc_pulse_duty", None)
    ts0 = st.session_started_at or st.now()
    use_ms = bool(pp.get("use_ms", False))
    use_ps = bool(pp.get("use_power_select", False))
    sd = pp.get("shutter_delay", None)
    it = pp.get("integration_time", None)
    proc_name = str(pp.get("process_name") or pp.get("Process_name") or pp.get("process_note") or "")
    stopped = bool(pp.get("stopped", False))
    result = "SUCCESS" if bool(was_successful) else ("STOP" if stopped else "FAIL")

    def _a2(use: bool, seq: List[float]) -> str:
        return f"{_avg(seq):.2f}" if (use and seq) else ""

    row: Dict[str, str] = {
        "Timestamp": ts0.strftime("%Y-%m-%d %H:%M:%S"),
        "Process Name": proc_name,
        "Result": result,
        "Base Pressure": f"{base_pressure:.2e}",
        "Main Shutter": "T" if use_ms else "F",
        "Shutter Delay": f"{float(sd):.3f}" if sd not in (None, "") else "",
        "Integration Time": str(int(it)) if it not in (None, "") else "",
        "Power Select": "T" if use_ps else "F",
        "G1 Target": str(pp.get("G1 Target", "")),
        "G2 Target": str(pp.get("G2 Target", "")),
        "G3 Target": str(pp.get("G3 Target", "")),
        "Ar flow": f"{_avg(st.flow['Ar']):.2f}",
        "O2 flow": f"{_avg(st.flow['O2']):.2f}",
        "N2 flow": f"{_avg(st.flow['N2']):.2f}",
        "Working Pressure": f"{_avg(st.pressure):.4f}",
        "Process Time": str(pp.get("process_time", 0.0)),
        "RF: For.P": _a2(use_rf, st.rf_f),
        "RF: Ref. P": _a2(use_rf, st.rf_r),
        "DC: V": _a2(use_dc, st.dc_v),
        "DC: I": _a2(use_dc, st.dc_i),
        "DC: P": _a2(use_dc, st.dc_p),
        "DC2: V": _a2(use_dc2, st.dc2_v),
        "DC2: I": _a2(use_dc2, st.dc2_i),
        "DC2: P": _a2(use_dc2, st.dc2_p),
        "RF Pulse: P": (f"{float(rfp_p):.2f}" if (use_rfp and rfp_p not in (None, "")) else ""),
        "RF Pulse: Freq": (str(int(rfp_f)) if (use_rfp and rfp_f not in (None, "")) else ""),
        "RF Pulse: Duty Cycle": (str(int(rfp_d)) if (use_rfp and rfp_d not in (None, "")) else ""),
        "DC Pulse: P": (f"{_avg(st.dcp_p):.2f}" if (use_dcp and st.dcp_p)
                        else (f"{float(dcp_p):.2f}" if (use_dcp and dcp_p not in (None, "")) else "")),
        "DC Pulse: V": _a2(use_dcp, st.dcp_v),
        "DC Pulse: I": _a2(use_dcp, st.dcp_i),
        "DC Pulse: Freq": (str(int(dcp_f)) if (use_dcp and dcp_f not in (None, "")) else ""),
        "DC Pulse: Duty Cycle": (str(int(dcp_d)) if (use_dcp and dcp_d not in (None, "")) else ""),
        "DC Pulse: Off Time": (_dcp_off_time_text(pp) if use_dcp else ""),
        "RF Pulse: For.P": _a2(use_rfp, st.rfp_f),
        "RF Pulse: Ref.P": _a2(use_rfp, st.rfp_r),
        "Chuck Position": str(pp.get("chuck_position") or ""),
    }
    if int(ch) == 2:
        hs = st.heater_stats() or {}

        def _hf(key: str, fmt: str) -> str:
            v = hs.get(key)
            return fmt.format(v) if v is not None else ""
        row.update({
            "Heater: SV": _hf("sv", "{:.1f}"), "Heater: PV Avg": _hf("pv_avg", "{:.1f}"),
            "Heater: PV Min": _hf("pv_min", "{:.1f}"), "Heater: PV Max": _hf("pv_max", "{:.1f}"),
            "Heater: MV Avg": _hf("mv_avg", "{:.0f}"),
        })
    if thickness_columns:
        tr = st.thickness_record or {}

        def _tf(key: str, fmt: str) -> str:
            v = tr.get(key)
            if v in (None, ""):
                return ""
            try:
                return fmt.format(float(v))
            except Exception:
                return ""
        row.update({
            "Thickness: End Mode": str(tr.get("end") or ""),
            "Thickness: Target nm": _tf("target", "{:g}"),
            "Thickness: Final nm": _tf("final_nm", "{:.2f}"),
            "Thickness: Avg Rate nm/s": _tf("avg_rate", "{:.4f}"),
            "Thickness: Tooling %": _tf("tooling", "{:g}"),
            "Thickness: Density": _tf("density", "{:g}"),
            "Thickness: Z": _tf("z_factor", "{:g}"),
            "Thickness: Chuck": str(tr.get("chuck") or ""),
            "Thickness: Gun": str(tr.get("gun") or ""),
            "Thickness: Fallback Reason": str(tr.get("fallback_reason") or ""),
            "Thickness: Jump Count": str(int(tr["jumps"])) if tr.get("jumps") is not None else "",
            "Thickness: Material": str(tr.get("material") or ""),
            "Thickness: Factor Source": str(tr.get("factor_source") or ""),
        })
    return row


# ───────────────────────── 파일 규칙(v1 그대로) ─────────────────────────
def ensure_header(path: Path, header: List[str]) -> None:
    """기존 CSV 가 옛 머리글이면 새 머리글로 다시 쓴다(data_logger.py:204-249)."""
    if not path.exists():
        return
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as rf:
            existing = next(csv.reader(rf), [])
    except Exception:
        return
    if existing == header:
        return
    temp_path = path.with_suffix(".tmp")
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as rf:
            rows = list(csv.DictReader(rf))
        with open(temp_path, "w", encoding="utf-8-sig", newline="") as wf:
            w = csv.DictWriter(wf, fieldnames=header)
            w.writeheader()
            for row in rows:
                new_row = {h: row.get(h, "") for h in header}
                if not new_row.get("Process Name"):
                    new_row["Process Name"] = row.get("Process Name") or row.get("Process Note") or ""
                w.writerow(new_row)
        os.replace(temp_path, path)
    except Exception:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except Exception:
            pass


def file_fields(path: Path, header: List[str], log: Callable[[str], None]) -> List[str]:
    """기록에 쓸 열 = 실제 파일 머리글(data_logger.py:188-202)."""
    try:
        if path.exists() and path.stat().st_size > 0:
            with open(path, "r", encoding="utf-8-sig", newline="") as rf:
                existing = next(csv.reader(rf), [])
            if existing and existing != header:
                log(f"Sputter Calib CSV 머리글 갱신 실패 — 기존 열 순서로 기록합니다(새 열은 다음 시작 때 추가): {path}")
                return list(existing)
    except Exception:
        pass
    return list(header)


class CalibCsv(Owned):
    def __init__(self, ch: int, cfg: Optional[Mapping[str, Any]] = None, *, thickness_columns: bool = False,
                 log_func: Optional[Callable[[str], None]] = None, writer: Optional[QueueWriter] = None,
                 sender: Optional[CopySender] = None) -> None:
        super().__init__(writer, f"calib-ch{int(ch)}")
        self.ch = int(ch)
        self.paths = Paths(cfg)
        self.thickness_columns = bool(thickness_columns)
        self._log_func = log_func
        self.header = header_for(self.ch, self.thickness_columns)
        self.nas_file = self.paths.calib_file(self.ch)               # G: 본파일(v1 위치) — 시작 때 고정(LOG-020)
        self.pending_file = self.paths.calib_pending(self.ch)
        self.local_file = self.paths.calib_local_first(self.ch)
        self._own_sender = sender is None
        self.sender = sender if sender is not None else CopySender(cfg, name=f"calib-copy-ch{self.ch}")
        self._lk = threading.Lock()               # pending 파일(쓰기 스레드 ↔ 사본 스레드)
        self._fields: Optional[List[str]] = None  # G: 본파일의 실제 열(첫 보내기 때 한 번 — v1 시작 때 한 번)
        self._failing = False                     # 실패 문구는 실패가 시작될 때 한 번(재시도마다 되풀이하지 않음)
        self.writer.put(self._startup_local)
        self.sender.submit(f"calib-ch{self.ch}", self._deliver)

    def _log(self, msg: str) -> None:
        if self._log_func:
            try:
                self._log_func(msg)
            except Exception:
                pass

    # ── 기록(엔진이 부름 — 큐에만) ──
    def record(self, stats: RunStats, was_successful: Optional[bool]) -> Optional[Dict[str, str]]:
        if was_successful is None:
            return None
        row = build_row(self.ch, stats, bool(was_successful), self.thickness_columns)
        self.writer.put(self._write_row, row)
        return row

    # ── 쓰기 스레드 ──
    def _startup_local(self) -> None:
        try:
            self.local_file.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        ensure_header(self.local_file, self.header)

    def _write_row(self, row: Dict[str, str]) -> None:
        # ① 로컬 정본
        try:
            app = FixedCsvDictAppender(self.local_file, file_fields(self.local_file, self.header, lambda m: None),
                                       encoding="utf-8-sig")
            try:
                app.open()
                app.append_row(row)
            finally:
                app.close()
        except Exception as e:
            self._log(f"Sputter Calib CSV 로컬 정본 기록 실패: {e!r}")
        # ② v1 pending(보낼 행) — 그때만 로컬 폴더 생성
        with self._lk:
            try:
                p = FixedCsvDictAppender(self.pending_file, self.header, encoding="utf-8-sig")
                try:
                    p.open()
                    p.append_row(row)
                finally:
                    p.close()
            except Exception as e2:
                self._log(f"Sputter Calib CSV pending 적재마저 실패: {e2!r}")
                return
        # ③ 사본 보내기
        self.sender.submit(f"calib-ch{self.ch}", self._deliver)

    # ── 사본 스레드: pending → G: 본파일(v1 _flush_pending_to_nas + 1회 재시도) ──
    def _read_pending(self) -> List[Dict[str, str]]:
        if not self.pending_file.exists():
            return []
        if self.pending_file.stat().st_size == 0:
            self.pending_file.unlink(missing_ok=True)
            return []
        with open(self.pending_file, "r", encoding="utf-8-sig", newline="") as rf:
            return list(csv.DictReader(rf))

    def _rewrite_pending(self, rows: List[Dict[str, str]]) -> None:
        if not rows:
            self.pending_file.unlink(missing_ok=True)
            return
        tmp = self.pending_file.with_suffix(".csv.tmp")
        with open(tmp, "w", encoding="utf-8-sig", newline="") as wf:
            w = csv.DictWriter(wf, fieldnames=self.header)
            w.writeheader()
            for r in rows:
                w.writerow({h: r.get(h, "") for h in self.header})
        os.replace(tmp, self.pending_file)

    def _deliver(self) -> bool:
        if self._fields is None:              # v1: 시작 때 머리글 업그레이드 + 실제 열(G: 본파일)
            try:
                self.nas_file.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self._log(f"Sputter Calib CSV NAS 폴더 생성 실패(초기화) → 기록 시마다 NAS 재시도 예정: {e!r}")
            ensure_header(self.nas_file, self.header)
            self._fields = file_fields(self.nas_file, self.header, self._log)
        with self._lk:
            try:
                rows = self._read_pending()
            except Exception as e:
                self._log(f"Sputter Calib CSV pending 병합 실패(다음에 재시도): {e!r}")
                return False
            if not rows:
                return True
            sent = 0
            err: Optional[Exception] = None
            for attempt in range(2):              # SMB 세션 타임아웃 대응: 핸들 해제 후 1회 재시도(v1)
                app = FixedCsvDictAppender(self.nas_file, self._fields, encoding="utf-8-sig")
                try:
                    app.open()
                    while sent < len(rows):
                        app.append_row({h: rows[sent].get(h, "") for h in self.header})
                        sent += 1
                    err = None
                except Exception as e:
                    err = e
                finally:
                    app.close()
                if err is None:
                    if attempt == 0:
                        self._log(f"Sputter Calib CSV {len(rows)}행 기록 완료 (NAS) → {self.nas_file}"
                                  if len(rows) > 1 else f"Sputter Calib CSV 1행 기록 완료 (NAS) → {self.nas_file}")
                    else:
                        self._log(f"Sputter Calib CSV NAS 재시도 성공 → {self.nas_file}")
                    break
            if sent:
                try:
                    self._rewrite_pending(rows[sent:])
                except Exception:
                    pass
            if err is not None:
                if not self._failing:
                    self._log(f"Sputter Calib CSV NAS 기록 실패 → pending 적재: {err!r}")
                    self._log(f"Sputter Calib CSV pending 적재 완료 → {self.pending_file}")
                self._failing = True
                return False
            if self._failing:
                self._log(f"Sputter Calib CSV pending → NAS 병합 완료 → {self.nas_file}")
            self._failing = False
            return True

    def stop(self, timeout_s: float = 5.0) -> None:
        self.stop_writer(timeout_s)
        if self._own_sender:
            self.sender.flush(timeout_s)
            self.sender.stop(timeout_s)
