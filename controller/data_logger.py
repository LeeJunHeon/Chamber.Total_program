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
    각 장치로부터 데이터를 받아 메모리에 수집하고,
    공정 종료 시 평균값을 계산해 CSV에 1행 기록한다.
    - PySide6 기반
    - 최종 파일 쓰기는 asyncio.to_thread로 오프로딩(프리징 완화)
    """
    def __init__(self, parent: Optional[QObject] = None):
        super().__init__(parent)

        # 로그 파일 경로
        log_directory = Path(r"\\VanaM_NAS\VanaM_Sputter\Sputter\Calib\Database")
        log_directory.mkdir(parents=True, exist_ok=True)
        self.log_file = log_directory / "Ch2_log.csv"

        # 수집 버퍼
        self.process_params: Dict = {}
        self.ig_pressure_readings: List[float] = []

        self.dc_power_readings: List[float] = []
        self.dc_voltage_readings: List[float] = []
        self.dc_current_readings: List[float] = []

        self.rf_for_p_readings: List[float] = []
        self.rf_ref_p_readings: List[float] = []

        # RF Pulse 폴링값
        self.rf_pulse_for_p_readings: List[float] = []
        self.rf_pulse_ref_p_readings: List[float] = []

        self.mfc_flow_readings: Dict[str, List[float]] = {"Ar": [], "O2": [], "N2": []}
        self.mfc_pressure_readings: List[float] = []

        # 최종 헤더
        self.header: List[str] = [
            "Timestamp", "Process Note", "Base Pressure",
            "G1 Target", "G2 Target", "G3 Target",
            "Ar flow", "O2 flow", "N2 flow",
            "Working Pressure", "Process Time",
            "RF: For.P", "RF: Ref. P",
            "DC: V", "DC: I", "DC: P",
            "RF Pulse: P", "RF Pulse: Freq", "RF Pulse: Duty Cycle",
            "RF Pulse: For.P", "RF Pulse: Ref.P",
        ]

        # 기존 파일이 있으면 헤더 업그레이드 시도
        self._ensure_header()

    # ──────────────────────────────────────────────────────────────
    # 초기 헤더 정리
    # ──────────────────────────────────────────────────────────────
    def _ensure_header(self) -> None:
        """기존 Ch2_log.csv가 구헤더면 새 헤더로 업그레이드(기존 행 보존, 새 컬럼 공란)."""
        if not self.log_file.exists():
            return  # 파일 없으면 다음 기록 때 새 헤더로 생성

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
        self.ig_pressure_readings.clear()

        self.dc_power_readings.clear()
        self.dc_voltage_readings.clear()
        self.dc_current_readings.clear()

        self.rf_for_p_readings.clear()
        self.rf_ref_p_readings.clear()

        self.rf_pulse_for_p_readings.clear()
        self.rf_pulse_ref_p_readings.clear()

        for gas in self.mfc_flow_readings:
            self.mfc_flow_readings[gas].clear()
        self.mfc_pressure_readings.clear()

    @Slot(float)
    def log_ig_pressure(self, pressure: float) -> None:
        self.ig_pressure_readings.append(float(pressure))

    @Slot(float, float, float)
    def log_dc_power(self, power: float, voltage: float, current: float) -> None:
        self.dc_power_readings.append(float(power))
        self.dc_voltage_readings.append(float(voltage))
        self.dc_current_readings.append(float(current))

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
        if gas_name in self.mfc_flow_readings:
            self.mfc_flow_readings[gas_name].append(float(flow_value))

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
    @Slot(bool)
    def finalize_and_write_log(self, was_successful: bool) -> None:
        """공정 종료 시 평균 계산 후 CSV 1행을 백그라운드에서 기록."""
        if not was_successful:
            return

        # 평균치 헬퍼
        def _avg(seq: List[float]) -> float:
            return (sum(seq) / len(seq)) if seq else 0.0

        # IG는 첫 값(없으면 params 기본값)
        base_pressure = (
            self.ig_pressure_readings[0]
            if self.ig_pressure_readings
            else float(self.process_params.get("base_pressure", 0.0))
        )

        # 사용 플래그
        use_rf = bool(self.process_params.get("use_rf_power", False))
        use_dc = bool(self.process_params.get("use_dc_power", False))
        use_rfp = bool(self.process_params.get("use_rf_pulse", False))

        # RF Pulse 설정
        rfp_p = self.process_params.get("rf_pulse_power", None)
        rfp_f = self.process_params.get("rf_pulse_freq", None)
        rfp_d = self.process_params.get("rf_pulse_duty", None)

        # 기록 데이터(문자열로 포맷)
        log_data: Dict[str, str] = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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

            # RF Pulse 폴링 평균
            "RF Pulse: For.P": f"{_avg(self.rf_pulse_for_p_readings):.2f}" if (use_rfp and self.rf_pulse_for_p_readings) else "",
            "RF Pulse: Ref.P": f"{_avg(self.rf_pulse_ref_p_readings):.2f}"  if (use_rfp and self.rf_pulse_ref_p_readings) else "",
        }

        # 비동기 백그라운드 파일쓰기로 프리징 최소화
        try:
            asyncio.get_running_loop().create_task(self._write_row_async(log_data))
        except RuntimeError:
            # 이벤트 루프가 없으면 동기 기록 (테스트/비정상 종료 경로)
            self._write_row_sync(log_data)

    async def _write_row_async(self, log_data: Dict[str, str]) -> None:
        await asyncio.to_thread(self._write_row_sync, log_data)

    def _write_row_sync(self, log_data: Dict[str, str]) -> None:
        """실제 파일 쓰기(동기). to_thread로 호출됨."""
        try:
            file_exists = self.log_file.exists()
            with open(self.log_file, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=self.header)
                if not file_exists:
                    writer.writeheader()
                writer.writerow(log_data)
        except Exception as e:
            print(f"데이터 로그 파일 작성 실패: {e}")
