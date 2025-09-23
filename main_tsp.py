# main_tsp.py
from __future__ import annotations
import asyncio
from typing import Optional
from PySide6.QtWidgets import QMessageBox

from device.tsp import TSPLetterClientRFC2217, AsyncTSP
from controller.tsp_controller import TSPBurstRunner, TSPProcessConfig, IGControllerLike


class TSPPageController:
    """
    - UI는 main.py에서 로드된 ui 인스턴스를 그대로 받습니다.
    - 로그 출력은 Plasma Cleaning 페이지의 pc_logMessage_edit를 사용합니다.
    - IG(ion gauge)는 main.py에서 생성된 AsyncIG 인스턴스를 재사용합니다.
    """
    def __init__(self, ui, ig_obj: IGControllerLike,
                 tsp_host: str, tsp_port: int, tsp_baud: int = 9600, addr: int = 1):
        self.ui = ui
        self.ig = ig_obj

        self.client = TSPLetterClientRFC2217(tsp_host, tsp_port, tsp_baud, addr)
        self.client.open()
        self.tsp = AsyncTSP(self.client)

        self.runner: Optional[TSPBurstRunner] = None
        self.cancel_event: Optional[asyncio.Event] = None
        self._is_running: bool = False

        # 기본값
        self.ui.TSP_setCycle_edit.setPlainText("10")
        self.ui.TSP_nowCycle_edit.setPlainText("0")
        self.ui.TSP_basePressure_edit.setPlainText("")

        # 버튼 연결
        self.ui.TSP_Start_button.clicked.connect(self.on_start_clicked)
        self.ui.TSP_Stop_button.clicked.connect(self.on_stop_clicked)

        # 로그창 초기화(있으면)
        try:
            self.ui.pc_logMessage_edit.clear()
        except Exception:
            pass

    # ===== UI/로그 =====
    def append_log(self, src: str, msg: str) -> None:
        try:
            w = self.ui.pc_logMessage_edit
            w.appendPlainText(f"[{src}] {msg}")
            sb = w.verticalScrollBar()
            if sb is not None:
                sb.setValue(sb.maximum())
        except Exception:
            pass

    def set_state(self, text: str) -> None:
        self.append_log("STATE", text)

    def on_pressure(self, torr: float) -> None:
        try:
            self.ui.TSP_basePressure_edit.setPlainText(f"{torr:.2e}")
        except Exception:
            pass

    def on_cycle(self, cycle: int, max_cycles: int) -> None:
        try:
            self.ui.TSP_nowCycle_edit.setPlainText(str(cycle))
        except Exception:
            pass
        self.append_log("CYCLE", f"{cycle}/{max_cycles}")

    def _read_target_pressure(self) -> float:
        txt = self.ui.TSP_targetPressure_edit.toPlainText().strip().replace(",", "").lower()
        if not txt:
            return 1.0e-6
        try:
            return float(txt)
        except Exception:
            self.append_log("WARN", f"Target Pressure 파싱 실패({txt!r}) → 1e-6 사용")
            return 1.0e-6

    def _read_set_cycles(self) -> int:
        txt = self.ui.TSP_setCycle_edit.toPlainText().strip()
        try:
            n = int(txt)
        except Exception:
            self.append_log("WARN", f"#(총 사이클) 파싱 실패({txt!r}) → 10 사용")
            n = 10
        return max(1, n)

    # ===== 실행 =====
    async def _run_process(self) -> None:
        self._is_running = True
        self.cancel_event = asyncio.Event()

        cfg = TSPProcessConfig(
            target_pressure_torr=self._read_target_pressure(),
            tsp_on_seconds=75.0,      # 1분 15초
            off_wait_seconds=150.0,   # 2분 30초
            max_cycles=self._read_set_cycles(),
            ig_poll_interval=1.0,
        )

        self.runner = TSPBurstRunner(
            self.tsp, self.ig,
            log_cb=self.append_log,
            state_cb=self.set_state,
            pressure_cb=self.on_pressure,
            cycle_cb=self.on_cycle,
        )

        # 시작 전 UI 초기화
        self.ui.TSP_nowCycle_edit.setPlainText("0")
        self.append_log("RUN", f"시작: target={cfg.target_pressure_torr:.3e} Torr, "
                               f"on={cfg.tsp_on_seconds:.0f}s, off={cfg.off_wait_seconds:.0f}s, "
                               f"max_cycles={cfg.max_cycles}")

        # 실행
        res = await self.runner.run(cfg, cancel_event=self.cancel_event)

        self.append_log("RESULT", f"ok={res.ok}, used={res.cycles_used}, "
                                  f"lastP={res.last_pressure_torr:.3e}, reason={res.reason}")

        # 종료 후 총 사이클 입력란 복원
        self.ui.TSP_setCycle_edit.setPlainText("10")
        self._is_running = False

    def on_start_clicked(self) -> None:
        if self._is_running:
            QMessageBox.information(None, "TSP", "이미 공정이 진행 중입니다.")
            return
        # 필요한 경우 IG 워커가 떠있지 않아도 run()은 작동하지만,
        # 프로젝트 정책상 백그라운드 시작이 필요하면 main.py 쪽에서 보장해 주세요.
        asyncio.create_task(self._run_process())

    def on_stop_clicked(self) -> None:
        if self.cancel_event and not self.cancel_event.is_set():
            self.append_log("RUN", "사용자 STOP 요청")
            self.cancel_event.set()

    async def aclose(self) -> None:
        try:
            await self.tsp.aclose()
        except Exception:
            pass


# (선택) 단독 테스트용 런처 — 실제 운영에서는 사용하지 마세요.
if __name__ == "__main__":
    import qasync
    from PySide6.QtWidgets import QApplication, QWidget
    from PySide6.QtUiTools import QUiLoader
    from PySide6.QtCore import QFile

    from device.ig import AsyncIG  # 실제 장비 모듈
    UI_PATH = "CH1&2YFMTfB.ui"

    async def _run(app: QApplication):
        # UI 로드
        ui_file = QFile(UI_PATH); ui_file.open(QFile.ReadOnly)
        loader = QUiLoader()
        w: QWidget = loader.load(ui_file)
        ui = w
        w.show()

        ig = AsyncIG()
        ctrl = TSPPageController(ui, ig, tsp_host="192.168.0.50", tsp_port=4001)
        app.aboutToQuit.connect(lambda: asyncio.create_task(ctrl.aclose()))

        while True:
            await asyncio.sleep(3600)

    app = QApplication([])
    loop = qasync.QEventLoop(app); asyncio.set_event_loop(loop)
    with loop:
        loop.run_until_complete(_run(app))
