# controller/data_logger.py
import csv
import os
import re
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from PySide6.QtCore import QObject, Slot


class DataLogger(QObject):
    """
    CH별로 공정 요약 데이터를 CSV에 1행 기록.
    - 두 채널 모두 같은 디렉토리(csv_dir)에 저장, 파일명만 Ch{ch}_log.csv로 구분
    - 파일이 없으면 자동 생성 + 헤더 기록
    - 최종 파일 쓰기는 asyncio.to_thread로 오프로딩(프리징 완화)
    """
    def __init__(
        self,
        parent: Optional[QObject] = None,
        *,
        ch: int,
        csv_dir: Optional[Path] = None,
    ):
        super().__init__(parent)
        self._ch = int(ch)  # ← 추가: 폴백 경로명/파일명에 사용

        # 공통 저장 경로 (인자 없으면 기존 NAS 기본값)
        log_directory = Path(csv_dir) if csv_dir else Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database")
        try:
            log_directory.mkdir(parents=True, exist_ok=True)
        except Exception:
            # 폴백
            log_directory = Path.cwd() / f"_CSV_local_CH{int(ch)}"
            log_directory.mkdir(parents=True, exist_ok=True)
            print(f"[DataLogger] NAS 접근 실패 → 로컬 폴백: {log_directory}")

        # 채널별 파일명만 다르게
        self.log_file = log_directory / f"Ch{int(ch)}_log.csv"

        # 수집 버퍼
        self.process_params: Dict = {}
        self.ig_pressure_readings: List[float] = []

        self.dc_power_readings: List[float] = []
        self.dc_voltage_readings: List[float] = []
        self.dc_current_readings: List[float] = []

        # DC Pulse 폴링값(평균용)
        self.dc_pulse_power_readings: List[float] = []
        self.dc_pulse_voltage_readings: List[float] = []
        self.dc_pulse_current_readings: List[float] = []

        self.rf_for_p_readings: List[float] = []
        self.rf_ref_p_readings: List[float] = []

        # RF Pulse 폴링값
        self.rf_pulse_for_p_readings: List[float] = []
        self.rf_pulse_ref_p_readings: List[float] = []

        self.mfc_flow_readings: Dict[str, List[float]] = {"Ar": [], "O2": [], "N2": []}
        self.mfc_pressure_readings: List[float] = []

        # ✅ 세션 시작 시각 저장용(내부에서 캡처)
        self._session_started_at: Optional[datetime] = None

        # 최종 헤더
        self.header: List[str] = [
            "Timestamp", "Process Note", "Base Pressure",
            "G1 Target", "G2 Target", "G3 Target",
            "Ar flow", "O2 flow", "N2 flow",
            "Working Pressure", "Process Time",
            "RF: For.P", "RF: Ref. P",
            "DC: V", "DC: I", "DC: P",
            "RF Pulse: P", "RF Pulse: Freq", "RF Pulse: Duty Cycle",
            "DC Pulse: P", "DC Pulse: V", "DC Pulse: I", "DC Pulse: Freq", "DC Pulse: Duty Cycle",
            "RF Pulse: For.P", "RF Pulse: Ref.P",
        ]

        # 기존 파일이 있으면 헤더 업그레이드(있던 행 보존, 새 컬럼은 공란)
        self._ensure_header()

    # ──────────────────────────────────────────────────────────────
    # 초기 헤더 정리
    # ──────────────────────────────────────────────────────────────
    def _ensure_header(self) -> None:
        """기존 CSV가 구헤더면 새 헤더로 업그레이드(기존 행 보존, 새 컬럼 공란)."""
        if not self.log_file.exists():
            return  # 없으면 첫 기록 때 새로 헤더 씀

        try:
            with open(self.log_file, "r", encoding="utf-8-sig", newline="") as rf:
                reader = csv.reader(rf)
                existing_header = next(reader, [])
        except Exception:
            # 읽기 실패시 건드리지 않음
            return

        if existing_header == self.header:
            return

        temp_path = self.log_file.with_suffix(".tmp")
        try:
            # 구헤더로 전체 읽기
            with open(self.log_file, "r", encoding="utf-8-sig", newline="") as rf:
                old_reader = csv.DictReader(rf)
                rows = list(old_reader)

            # 새 헤더로 임시 파일 작성
            with open(temp_path, "w", encoding="utf-8-sig", newline="") as wf:
                new_writer = csv.DictWriter(wf, fieldnames=self.header)
                new_writer.writeheader()
                for row in rows:
                    new_row = {h: row.get(h, "") for h in self.header}
                    new_writer.writerow(new_row)

            # 원자적 교체
            os.replace(temp_path, self.log_file)
        except Exception:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────
    # 세션 시작/수집 슬롯
    # ──────────────────────────────────────────────────────────────
    @Slot(dict)
    def start_new_log_session(self, params: dict) -> None:
        """새로운 공정 시작 시 데이터 저장소 초기화."""
        self.process_params = params.copy()

        # ✅ 우선순위: t0_wall → started_at → now()  (모두 tz 없이 취급)
        sa = self.process_params.get("t0_wall") or self.process_params.get("started_at")

        # ✅ 런타임에서 넘겨준 시작시각이 있으면 우선 사용, 없으면 now()
        if isinstance(sa, datetime):
            self._session_started_at = sa
        elif isinstance(sa, str):
            try:
                self._session_started_at = datetime.fromisoformat(sa)
            except Exception:
                self._session_started_at = datetime.now()
        elif isinstance(sa, (int, float)):
            self._session_started_at = datetime.fromtimestamp(float(sa))
        else:
            self._session_started_at = datetime.now()

        self.ig_pressure_readings.clear()

        self.dc_power_readings.clear()
        self.dc_voltage_readings.clear()
        self.dc_current_readings.clear()

        self.dc_pulse_power_readings.clear()
        self.dc_pulse_voltage_readings.clear()
        self.dc_pulse_current_readings.clear()

        self.rf_for_p_readings.clear()
        self.rf_ref_p_readings.clear()

        self.rf_pulse_for_p_readings.clear()
        self.rf_pulse_ref_p_readings.clear()

        for gas in self.mfc_flow_readings:
            self.mfc_flow_readings[gas].clear()
        self.mfc_pressure_readings.clear()

    @Slot(object)
    def log_ig_pressure(self, pressure: object) -> None:
        try:
            if isinstance(pressure, (int, float)):
                self.ig_pressure_readings.append(float(pressure)); return
            m = re.search(r"([-+]?\d+(?:\.\d+)?)", str(pressure))
            if m:
                self.ig_pressure_readings.append(float(m.group(1)))
        except Exception:
            pass

    @Slot(float, float, float)
    def log_dc_power(self, power: float, voltage: float, current: float) -> None:
        self.dc_power_readings.append(float(power))
        self.dc_voltage_readings.append(float(voltage))
        self.dc_current_readings.append(float(current))

    @Slot(float, float, float)
    def log_dcpulse_power(self, power: float, voltage: float, current: float) -> None:
        self.dc_pulse_power_readings.append(float(power))
        self.dc_pulse_voltage_readings.append(float(voltage))
        self.dc_pulse_current_readings.append(float(current))

    @Slot(float, float)
    def log_rf_power(self, for_p: float, ref_p: float) -> None:
        self.rf_for_p_readings.append(float(for_p))
        self.rf_ref_p_readings.append(float(ref_p))

    @Slot(float, float)
    def log_rfpulse_power(self, for_p: float, ref_p: float) -> None:
        self.rf_pulse_for_p_readings.append(float(for_p))
        self.rf_pulse_ref_p_readings.append(float(ref_p))

    @Slot(str, float)
    def log_mfc_flow(self, gas_name: str, flow_value: float) -> None:
        key = {"AR": "Ar", "O2": "O2", "N2": "N2"}.get(gas_name.strip().upper())
        if key:
            self.mfc_flow_readings[key].append(float(flow_value))

    @Slot(object)
    def log_mfc_pressure(self, pressure: object) -> None:
        """
        MFC 압력 신호를 수신.
        - 문자열 'V+100.00', '100.00', 'P=100.00' 등 → 숫자만 추출
        - 숫자형(float/int)도 그대로 수집
        """
        try:
            if isinstance(pressure, (int, float)):
                self.mfc_pressure_readings.append(float(pressure))
                return
            s = str(pressure)
            m = re.search(r"([-+]?\d+(?:\.\d+)?)", s)
            if m:
                self.mfc_pressure_readings.append(float(m.group(1)))
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────────
    # 종료/기록
    # ──────────────────────────────────────────────────────────────
    @Slot(object)
    def finalize_and_write_log(self, was_successful: Optional[bool] = True) -> None:
        """공정 종료 시 평균 계산 후 CSV 1행을 백그라운드에서 기록."""
        if not was_successful:
            return

        # 평균치 헬퍼
        def _avg(seq: List[float]) -> float:
            return (sum(seq) / len(seq)) if seq else 0.0

        # IG는 최소값
        base_pressure = min(self.ig_pressure_readings) if self.ig_pressure_readings \
                else float(self.process_params.get("base_pressure", 0.0))

        # 사용 플래그
        use_rf = bool(self.process_params.get("use_rf_power", False))
        use_dc = bool(self.process_params.get("use_dc_power", False))
        use_rfp = bool(self.process_params.get("use_rf_pulse", False))
        use_dcp = bool(self.process_params.get("use_dc_pulse", False))  # ← 추가

        # RF Pulse 설정
        rfp_p = self.process_params.get("rf_pulse_power", None)
        rfp_f = self.process_params.get("rf_pulse_freq", None)
        rfp_d = self.process_params.get("rf_pulse_duty", None)

        # DC Pulse 설정(있다면 setpoint 기록용)
        dcp_p = self.process_params.get("dc_pulse_power", None)         # ← 추가
        dcp_f = self.process_params.get("dc_pulse_freq", None)          # ← 추가
        dcp_d = self.process_params.get("dc_pulse_duty", None)          # ← 추가

        # ✅ 세션 시작시각을 우선 사용 (없으면 안전하게 now)
        ts0 = self._session_started_at or datetime.now()

        # 기록 데이터(문자열로 포맷)
        log_data: Dict[str, str] = {
            "Timestamp": ts0.strftime("%Y-%m-%d %H:%M:%S"),
            "Process Note": str(self.process_params.get("process_note", "")),
            "Base Pressure": f"{base_pressure:.2e}",
            "G1 Target": str(self.process_params.get("G1 Target", "")),
            "G2 Target": str(self.process_params.get("G2 Target", "")),
            "G3 Target": str(self.process_params.get("G3 Target", "")),
            "Ar flow": f"{_avg(self.mfc_flow_readings['Ar']):.2f}",
            "O2 flow": f"{_avg(self.mfc_flow_readings['O2']):.2f}",
            "N2 flow": f"{_avg(self.mfc_flow_readings['N2']):.2f}",
            "Working Pressure": f"{_avg(self.mfc_pressure_readings):.4f}",
            "Process Time": str(self.process_params.get("process_time", 0.0)),

            # 일반 RF
            "RF: For.P": f"{_avg(self.rf_for_p_readings):.2f}" if (use_rf and self.rf_for_p_readings) else "",
            "RF: Ref. P": f"{_avg(self.rf_ref_p_readings):.2f}" if (use_rf and self.rf_ref_p_readings) else "",

            # DC
            "DC: V": f"{_avg(self.dc_voltage_readings):.2f}" if (use_dc and self.dc_voltage_readings) else "",
            "DC: I": f"{_avg(self.dc_current_readings):.2f}" if (use_dc and self.dc_current_readings) else "",
            "DC: P": f"{_avg(self.dc_power_readings):.2f}"   if (use_dc and self.dc_power_readings) else "",

            # RF Pulse 설정값
            "RF Pulse: P":          (f"{float(rfp_p):.2f}" if (use_rfp and rfp_p not in (None, "")) else ""),
            "RF Pulse: Freq":       (str(int(rfp_f))        if (use_rfp and rfp_f not in (None, "")) else ""),
            "RF Pulse: Duty Cycle": (str(int(rfp_d))        if (use_rfp and rfp_d not in (None, "")) else ""),

            # DC Pulse 측정 평균(없으면 setpoint로 보강)
            "DC Pulse: P": (
                f"{_avg(self.dc_pulse_power_readings):.2f}" if (use_dcp and self.dc_pulse_power_readings)
                else (f"{float(dcp_p):.2f}" if (use_dcp and dcp_p not in (None, "")) else "")
            ),
            "DC Pulse: V": f"{_avg(self.dc_pulse_voltage_readings):.2f}" if (use_dcp and self.dc_pulse_voltage_readings) else "",
            "DC Pulse: I": f"{_avg(self.dc_pulse_current_readings):.2f}" if (use_dcp and self.dc_pulse_current_readings) else "",
            "DC Pulse: Freq":       (str(int(dcp_f))        if (use_dcp and dcp_f not in (None, "")) else ""),
            "DC Pulse: Duty Cycle": (str(int(dcp_d))        if (use_dcp and dcp_d not in (None, "")) else ""),

            # RF Pulse 폴링 평균
            "RF Pulse: For.P": f"{_avg(self.rf_pulse_for_p_readings):.2f}" if (use_rfp and self.rf_pulse_for_p_readings) else "",
            "RF Pulse: Ref.P": f"{_avg(self.rf_pulse_ref_p_readings):.2f}"  if (use_rfp and self.rf_pulse_ref_p_readings) else "",
        }

        # 비동기 백그라운드 파일쓰기로 프리징 최소화
        try:
            asyncio.get_running_loop().create_task(self._write_row_async(log_data))
        except RuntimeError:
            # 이벤트 루프가 없으면 동기 기록
            self._write_row_sync(log_data)

    async def _write_row_async(self, log_data: Dict[str, str]) -> None:
        await asyncio.to_thread(self._write_row_sync, log_data)

    def _write_row_sync(self, log_data: Dict[str, str]) -> None:
        try:
            # 부모 디렉터리 보장
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            file_exists = self.log_file.exists()
            with open(self.log_file, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=self.header)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(log_data)
            self._session_started_at = None
        except Exception as e:
            # NAS 실패 → 로컬 폴더로 재시도
            print(f"데이터 로그 파일 작성 실패(NAS): {e}")
            try:
                local_dir = Path.cwd() / f"_CSV_local_CH{self._ch}"
                local_dir.mkdir(parents=True, exist_ok=True)
                local_file = local_dir / f"Ch{self._ch}_log.csv"
                file_exists = local_file.exists()
                with open(local_file, "a", newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=self.header)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(log_data)
                # 이후 런부터는 로컬 파일을 기본으로 사용
                self.log_file = local_file
                print(f"[DataLogger] NAS 실패 → 로컬 폴백으로 기록: {local_file}")
            except Exception as e2:
                print(f"[DataLogger] 로컬 폴백마저 실패: {e2}")
