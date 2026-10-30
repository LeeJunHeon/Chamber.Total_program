"""CH1/CH2 공정 엑셀(CH{n}.xlsx) — v1 util/gdrive_logger.py 의 파일 출력(LOG-041~056).

v1 그대로
- Main Process 시트: G:/공유 드라이브/VanaM_Sputter/Process_log/CH{n}.xlsx(하드코딩 — GDRIVE_LOG_DIR 무시, LOG-041),
  성공(ok) 공정만(STOP·실패는 Calib CSV 에만 — 부르는 쪽 몫), DEV_MODE 면 생략, 사용한 전원 소스마다 1행(DC Pulse → DC →
  DC2 → RF Pulse → RF, 없으면 1행), DC Pulse 행만 Soft/Hard Arc, RF/RF Pulse 행의 Avg Load/Tune 은 늘 빈칸(v1 이상 동작 —
  DataLogger 에 rf_load/tune 버퍼가 없었다, LOG-053), CH1 은 G2/G3 Target 공백, CH2 는 Heater 열, 두께 채널은 Thickness 열.
- Plasma Cleaning 시트: GDRIVE_LOG_DIR\\CH{n}.xlsx, PC 성공 때만 1행(날짜 = 저장 시각), 폴백 <G: 폴더>\\LocalFallback\\
  CH{n}_pending.xlsx(DEV_MODE 검사 없음 — v1 그대로).
- 서식·다음 행 찾기·broken·pending 은 excel_style.py(v1 코드 그대로). 저장 실패면 LOCAL_FALLBACK_ROOT\\CH{n}_pending.xlsx
  에 같은 행(나중에 합치는 코드 없음 — v1 그대로).

D07(로컬 먼저): 같은 행을 OUTPUT_LOCAL_FIRST_DIR\\Process_log\\CH{n}.xlsx(로컬 정본)에 먼저 쓰고, G: 의 v1 파일에는 사본
스레드(CopySender)가 v1 _save_sync 그대로 보낸다(G: 실패면 v1 처럼 pending.xlsx — 그 행은 다시 보내지 않는다).
v2 차이: 날짜는 RunStats.session_started_at(공정 시작)으로 고정 — v1 은 Calib CSV 기록 스레드가 시작 시각을 먼저 지우면
저장 시각이 될 수 있었다(LOG-058 경합). Arc 경고는 웹훅 대신 on_arc_alert(ch, 공정명, soft, hard) 콜백(챗은 포털 몫).

부르는 법
  x = ExcelLog(ch, cfg, thickness_columns=.., dev_mode=bool(cfg.get("DEV_MODE")), writer=w, sender=s, on_arc_alert=fn)
  x.save_main(stats, operator=pp.get("operator",""), substrate=.., note=.., arc_alert_sent=False)   # 성공 공정만
  x.save_pc(pc_params)   # {"process_name","time","base_pressure","sp_ar","avg_ar","sp_pressure","avg_pressure",
                         #  "sp_power","avg_forp","avg_refp","avg_load","avg_tune"}
  rows = build_rows(ch, stats, operator, substrate, note)   # 순수 함수
"""
from __future__ import annotations

import contextlib
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from v2.ch12.outputs import excel_style as X
from v2.ch12.outputs.base import Owned, QueueWriter, wall_now
from v2.ch12.outputs.paths import CopySender, Paths
from v2.ch12.outputs.run_stats import RunStats
from v2.ch12.params import outputs as P


PC_ARC_THRESH = 5        # util/gdrive_logger.py:31 _DEFAULT_ARC_THRESH — runtime/plasma_cleaning_runtime.py:1649 은 넘기지 않음
PC_REFP_WARN_W = 20.0


def _avg(seq: list) -> Optional[float]:
    if not seq:
        return None
    return round(sum(seq) / len(seq), 4)


def _dcp_off_time_value(pp: Mapping[str, Any]):
    v = pp.get("dc_pulse_off_time_us", None)
    if v not in (None, ""):
        try:
            return float(v)
        except Exception:
            pass
    raw = pp.get("dc_pulse_off_time", None)
    if raw in (None, "") or isinstance(raw, str):
        return None
    try:
        return float(raw)
    except Exception:
        return None


def build_rows(ch: int, st: RunStats, operator: str = "", substrate: str = "", note: str = "",
               now: Callable[[], datetime] = wall_now) -> List[Dict[str, Any]]:
    """v1 _build_data(util/gdrive_logger.py:557-756)."""
    pp = st.params
    ts = st.session_started_at or now()
    base_pres = min(st.ig) if st.ig else pp.get("base_pressure")
    base: Dict[str, Any] = {
        "timestamp": ts,
        "operator": operator or pp.get("operator", ""),
        "process_name": (pp.get("process_name") or pp.get("Process_name", "")),
        "note": note,
        "substrate": substrate,
        "main_shutter": "T" if pp.get("use_ms") else "F",
        "power_select": "T" if pp.get("use_power_select") else "F",
        "G1 Target": pp.get("G1 Target", ""),
        "G2 Target": pp.get("G2 Target", "") if ch != 1 else "",
        "G3 Target": pp.get("G3 Target", "") if ch != 1 else "",
        "chuck_position": pp.get("chuck_position", ""),
        "shutter_delay": pp.get("shutter_delay"),
        "process_time": pp.get("process_time"),
        "dep_rate": pp.get("dep_rate"),
        "thickness": pp.get("thickness"),
        "base_pressure": base_pres,
        "sp_ar": pp.get("Ar_flow") or pp.get("ar_flow"),
        "sp_n2": pp.get("N2_flow") or pp.get("n2_flow"),
        "sp_o2": pp.get("O2_flow") or pp.get("o2_flow"),
        "sp_pressure": pp.get("working_pressure") or pp.get("sp_pressure"),
        "avg_ar": _avg(st.flow.get("Ar", [])),
        "avg_n2": _avg(st.flow.get("N2", [])),
        "avg_o2": _avg(st.flow.get("O2", [])),
        "avg_pressure": _avg(st.pressure),
        "soft_arc": None,
        "hard_arc": None,
    }
    hs: Dict[str, Any] = {}
    with contextlib.suppress(Exception):
        hs = st.heater_stats() or {}

    def _r(key: str, nd: int):
        v = hs.get(key)
        return None if v is None else round(float(v), nd)
    tr: Dict[str, Any] = dict(st.thickness_record or {})

    def _t(key: str, nd: Optional[int]):
        v = tr.get(key)
        if v in (None, ""):
            return None
        if nd is None:
            return str(v)
        try:
            return round(float(v), nd)
        except Exception:
            return None
    base.update({
        "thk_end": _t("end", None), "thk_target": _t("target", 2), "thk_final": _t("final_nm", 2),
        "thk_avg_rate": _t("avg_rate", 4), "thk_tooling": _t("tooling", 1), "thk_density": _t("density", 3),
        "thk_z": _t("z_factor", 3), "thk_fallback_reason": _t("fallback_reason", None),
        "thk_jump_count": (int(tr["jumps"]) if tr.get("jumps") is not None else None),
        "thk_material": _t("material", None), "thk_factor_source": _t("factor_source", None),
    })
    base.update({
        "heater_sv": _r("sv", 1), "heater_pv_avg": _r("pv_avg", 1), "heater_pv_min": _r("pv_min", 1),
        "heater_pv_max": _r("pv_max", 1), "heater_mv_avg": _r("mv_avg", 0),
    })
    none5 = {"avg_forp": None, "avg_refp": None, "avg_load": None, "avg_tune": None}
    power_rows: List[Dict[str, Any]] = []
    if pp.get("use_dc_pulse"):
        power_rows.append({
            "power_source": "DC Pulse", "sp_power": pp.get("dc_pulse_power"), "avg_power": _avg(st.dcp_p), **none5,
            "avg_voltage": _avg(st.dcp_v), "avg_current": _avg(st.dcp_i),
            "duty_cycle": pp.get("dc_pulse_duty_cycle") or pp.get("dc_pulse_duty"),
            "frequency": pp.get("dc_pulse_freq"), "off_time": _dcp_off_time_value(pp),
            "soft_arc": int(pp.get("soft_arc_count") or 0), "hard_arc": int(pp.get("hard_arc_count") or 0),
        })
    if pp.get("use_dc_power"):
        power_rows.append({
            "power_source": "DC", "sp_power": pp.get("dc_power"), "avg_power": _avg(st.dc_p), **none5,
            "avg_voltage": _avg(st.dc_v), "avg_current": _avg(st.dc_i),
            "duty_cycle": None, "frequency": None, "off_time": None,
        })
    if pp.get("use_dc_power2"):
        power_rows.append({
            "power_source": "DC2", "sp_power": pp.get("dc_power2"), "avg_power": _avg(st.dc2_p), **none5,
            "avg_voltage": _avg(st.dc2_v), "avg_current": _avg(st.dc2_i),
            "duty_cycle": None, "frequency": None, "off_time": None,
        })
    if pp.get("use_rf_pulse"):
        power_rows.append({
            "power_source": "RF Pulse", "sp_power": pp.get("rf_pulse_power"),
            "avg_forp": _avg(st.rfp_f), "avg_refp": _avg(st.rfp_r),
            "avg_load": None, "avg_tune": None,          # v1: rf_load/tune 버퍼 없음 → 늘 빈칸(LOG-053)
            "avg_voltage": None, "avg_current": None,
            "duty_cycle": pp.get("rf_pulse_duty_cycle") or pp.get("rf_pulse_duty"),
            "frequency": pp.get("rf_pulse_freq"), "off_time": pp.get("rf_pulse_off_time_us"),
        })
    if pp.get("use_rf_power"):
        power_rows.append({
            "power_source": "RF", "sp_power": pp.get("rf_power"),
            "avg_forp": _avg(st.rf_f), "avg_refp": _avg(st.rf_r),
            "avg_load": None, "avg_tune": None,          # v1 그대로 늘 빈칸
            "avg_voltage": None, "avg_current": None, "duty_cycle": None, "frequency": None, "off_time": None,
        })
    if not power_rows:
        return [base]
    out = []
    for pr in power_rows:
        row = dict(base)
        row.update(pr)
        out.append(row)
    return out


def build_pc_rows(pc_params: Mapping[str, Any], now: Callable[[], datetime] = wall_now) -> List[Dict[str, Any]]:
    """v1 _build_pc_only_data(util/gdrive_logger.py:763-786)."""
    pc = pc_params or {}
    return [{
        "timestamp": now(), "operator": "", "process_name": pc.get("process_name", "Plasma Cleaning"),
        "note": "", "substrate": "",
        "pc_time": pc.get("time"), "pc_base_pressure": pc.get("base_pressure"),
        "pc_sp_ar": pc.get("sp_ar"), "pc_avg_ar": pc.get("avg_ar"),
        "pc_sp_pressure": pc.get("sp_pressure"), "pc_avg_pressure": pc.get("avg_pressure"),
        "pc_sp_power": pc.get("sp_power"), "pc_avg_forp": pc.get("avg_forp"), "pc_avg_refp": pc.get("avg_refp"),
        "pc_avg_load": pc.get("avg_load"), "pc_avg_tune": pc.get("avg_tune"),
    }]


def save_sync(ch: int, rows: List[Dict[str, Any]], log_dir: Path, arc_thresh: int, refp_warn: float,
              local_fallback_dir: Optional[Path], sheet_name: str, cols, grp_cols, grp_starts) -> Path:
    """v1 _save_sync 의 파일 부분(util/gdrive_logger.py:513-529). 쓴 파일 경로(폴백이면 폴백)."""
    def _do_save(target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        with X._write_lock:
            wb, ws = X._get_or_create_wb(target, sheet_name, cols, grp_cols, grp_starts)
            row_num = X._next_data_row(ws)
            for row_data in rows:
                X._write_data_row(ws, row_num, row_data, arc_thresh, refp_warn, cols)
                row_num += 1
            wb.save(str(target))

    main_path = Path(log_dir) / f"CH{ch}.xlsx"
    try:
        _do_save(main_path)
        return main_path
    except Exception:
        if local_fallback_dir is None:
            raise
        fb_path = Path(local_fallback_dir) / f"CH{ch}_pending.xlsx"
        _do_save(fb_path)
        return fb_path


def arc_over(rows: List[Dict[str, Any]], arc_thresh: int) -> "tuple[int, int, bool]":
    """(soft, hard, 임계 이상) — 전 행 최대값으로 판정(util/gdrive_logger.py:531-534)."""
    soft = max((int(r.get("soft_arc") or 0) for r in rows), default=0)
    hard = max((int(r.get("hard_arc") or 0) for r in rows), default=0)
    return soft, hard, (soft + hard) >= arc_thresh


class ExcelLog(Owned):
    def __init__(self, ch: int, cfg: Optional[Mapping[str, Any]] = None, *, thickness_columns: bool = False,
                 dev_mode: bool = False, writer: Optional[QueueWriter] = None, sender: Optional[CopySender] = None,
                 on_arc_alert: Optional[Callable[[int, str, int, int], None]] = None,
                 log_func: Optional[Callable[[str], None]] = None, now: Callable[[], datetime] = wall_now,
                 main_dir: Optional[Path] = None) -> None:
        super().__init__(writer, f"excel-ch{int(ch)}")
        self.ch = int(ch)
        self.cfg = cfg if cfg is not None else {}
        self.paths = Paths(self.cfg)
        self.thickness_columns = bool(thickness_columns)
        self.dev_mode = bool(dev_mode)
        self.on_arc_alert = on_arc_alert
        self._log_func = log_func
        self.now = now
        self.main_dir = Path(main_dir) if main_dir is not None else self.paths.excel_main_dir()
        self._own_sender = sender is None
        self.sender = sender if sender is not None else CopySender(self.cfg, name=f"excel-copy-ch{self.ch}")
        self._seq = 0

    def _log(self, msg: str) -> None:
        if self._log_func:
            try:
                self._log_func(msg)
            except Exception:
                pass

    def _thresh(self) -> "tuple[int, float]":
        try:
            a = int(P.get(self.cfg, "GDRIVE_ARC_ALERT_THRESH"))
        except Exception:
            a = 5
        try:
            r = float(P.get(self.cfg, "GDRIVE_REF_P_WARN_W"))
        except Exception:
            r = 20.0
        return a, r

    def save_main(self, stats: RunStats, *, operator: str = "", substrate: str = "", note: str = "",
                  arc_alert_sent: bool = False) -> Optional[List[Dict[str, Any]]]:
        """성공 공정 1회(부르는 쪽이 ok 일 때만). 행 목록(DEV_MODE 면 None)."""
        if self.dev_mode:
            return None
        try:
            rows = build_rows(self.ch, stats, operator, substrate, note, now=self.now)
        except Exception as e:
            self._log(f"GDrive 저장 준비 실패: {e!r}")
            return None
        arc, refp = self._thresh()
        cols = X._main_cols(self.ch, self.thickness_columns)
        self.writer.put(self._local_then_copy, rows, "Main Process", cols, self.paths.fallback_root(), arc, refp)
        soft, hard, over = arc_over(rows, arc)
        if over and not arc_alert_sent and self.on_arc_alert is not None:
            try:
                self.on_arc_alert(self.ch, str(rows[0].get("process_name", "")), soft, hard)
            except Exception:
                pass
        return rows

    def save_pc(self, pc_params: Mapping[str, Any]) -> List[Dict[str, Any]]:
        rows = build_pc_rows(pc_params, now=self.now)
        arc, refp = PC_ARC_THRESH, PC_REFP_WARN_W     # v1 save_pc_only 는 기본 인자(5, 20.0)로 불렸다
        pc_dir = self.paths.excel_pc_dir()
        cols = (X._COLS_PC, X._GRP_COLS_PC, X._GRP_STARTS_PC)
        self.writer.put(self._local_then_copy, rows, "Plasma Cleaning", cols, pc_dir / "LocalFallback", arc, refp,
                        pc_dir)
        return rows

    # ── 쓰기 스레드: 로컬 정본 → 사본 작업 ──
    def _local_then_copy(self, rows, sheet, cols, fb_dir, arc, refp, target_dir: Optional[Path] = None) -> None:
        local_dir = self.paths.local_first_root() / ("Process_log" if target_dir is None else "Process_log_PC")
        try:
            save_sync(self.ch, rows, local_dir, arc, refp, None, sheet, *cols)
        except Exception as e:
            self._log(f"GDrive 로컬 정본 저장 실패: {e!r}")
        tdir = self.main_dir if target_dir is None else Path(target_dir)
        self._seq += 1

        def _copy() -> bool:
            try:
                p = save_sync(self.ch, rows, tdir, arc, refp, fb_dir, sheet, *cols)
                self._log("GDrive 저장 완료" if p.parent == tdir else f"GDrive 저장 실패 → 로컬 보관: {p}")
            except Exception as e:
                self._log(f"GDrive 저장 실패: {e!r}")      # v1: 예외는 밖으로 내지 않음, 다시 보내지 않음
            return True
        self.sender.submit(f"excel-ch{self.ch}-{self._seq}", _copy)

    def stop(self, timeout_s: float = 10.0) -> None:
        self.stop_writer(timeout_s)
        if self._own_sender:
            self.sender.flush(timeout_s)
            self.sender.stop(timeout_s)
