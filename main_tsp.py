# main_tsp.py
from __future__ import annotations
import asyncio
from typing import Optional

from PySide6.QtWidgets import QMessageBox
from PySide6.QtCore import QTimer

from lib.config_ch2 import TSP_PORT, TSP_BAUD  # URL/baud는 config에서
from device.tsp import TSPLetterClient, AsyncTSP
from controller.tsp_controller import TSPBurstRunner, TSPProcessConfig, IGControllerLike


class TSPPageController:
    """
    - main.py는 UI만 생성해 넘겨주고, TSP 연결/공정은 여기서 전담.
    - Start를 눌렀을 때만 RFC2217(URL)로 연결하고, 공정 종료 후 닫는다.
    - IG는 외부에서 주입할 수도 있고(권장), 없으면 내부에서 생성해 사용한다.
    """
    def __init__(
        self,
        ui,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        ig: Optional[IGControllerLike] = None,
        port: Optional[str] = None,      # e.g. "rfc2217://192.168.0.50:4001"
        baud: Optional[int] = None,      # 보통 9600
    ):
        self.ui = ui
        self._loop = loop or asyncio.get_event_loop()

        self.port = port or TSP_PORT
        self.baud = baud or TSP_BAUD

        self.client: Optional[TSPLetterClient] = None
        self.tsp: Optional[AsyncTSP] = None

        # IG: 외부 주입 시 재사용(권장). 없으면 내부 생성.
        self.ig: Optional[IGControllerLike] = ig
        self._owns_ig = ig is None
        self._ig_task: Optional[asyncio.Task] = None

        self.runner: Optional[TSPBurstRunner] = None
        self.cancel_event: Optional[asyncio.Event] = None
        self._is_running: bool = False
        self._connect_lock = asyncio.Lock()

        # === UI 초기화 & 버튼 연결 ===
        self._init_ui_defaults()
        self._connect_ui_signals()

        # 로그창 설정
        try:
            self.ui.pc_logMessage_edit.clear()
            self.ui.pc_logMessage_edit.setMaximumBlockCount(2000)
        except Exception:
            pass

    # ---------------- UI ----------------
    def _init_ui_defaults(self) -> None:
        """TSP 섹션 기본값 세팅."""
        try:
            self.ui.TSP_setCycle_edit.setPlainText("10")
            self.ui.TSP_nowCycle_edit.setPlainText("0")
            self.ui.TSP_basePressure_edit.setPlainText("")
            self.ui.TSP_targetPressure_edit.setPlainText("9e-6")  # 기본 9e-6
        except Exception:
            pass

    def _connect_ui_signals(self) -> None:
        """Start/Stop 버튼에 핸들러 연결."""
        try:
            self.ui.TSP_Start_button.clicked.connect(self.on_start_clicked)
            self.ui.TSP_Stop_button.clicked.connect(self.on_stop_clicked)
        except Exception:
            pass

    # ---------------- 로그/표시 ----------------
    def _log(self, msg: str) -> None:
        def _do():
            try:
                w = self.ui.pc_logMessage_edit
                w.appendPlainText(msg)
                sb = w.verticalScrollBar()
                if sb is not None:
                    sb.setValue(sb.maximum())
            except Exception:
                pass

        # 항상 생성자에서 받은 루프를 사용 + 스레드세이프 예약
        loop = self._loop
        try:
            loop.call_soon_threadsafe(_do)
        except Exception:
            # 루프가 아직 준비 안됐다면(초기화 시점), 최소한 블로킹 없이 예약
            QTimer.singleShot(0, _do)

    def append_log(self, src: str, msg: str) -> None:
        self._log(f"[{src}] {msg}")

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

    # ---------------- 입력 읽기 ----------------
    def _read_target_pressure(self) -> float:
        """
        UI: TSP_targetPressure_edit
        - 빈 값/파싱 실패면 9e-6 Torr 사용
        """
        try:
            txt = self.ui.TSP_targetPressure_edit.toPlainText().strip().replace(",", "").lower()
        except Exception:
            return 9.0e-6

        if not txt:
            return 9.0e-6
        try:
            return float(txt)
        except Exception:
            # ▼▼ 수정된 부분 ▼▼
            self.append_log("WARN", f"Target Pressure 파싱 실패({txt!r}) → 9e-6 사용")
            try:
                self.ui.TSP_targetPressure_edit.setPlainText("error")
            except Exception:
                pass
            return 9.0e-6

    def _read_set_cycles(self) -> int:
        """
        UI: TSP_setCycle_edit
        - 파싱 실패 시 10, 최소 1
        """
        try:
            txt = self.ui.TSP_setCycle_edit.toPlainText().strip()
            n = int(txt)
        except Exception:
            self.append_log("WARN", "총 사이클 파싱 실패 → 10 사용")
            n = 10
        return max(1, n)

    # ---------------- 내부 보장(연결/IG) ----------------
    async def _ensure_ig_started(self) -> None:
        if self.ig is None:
            from device.ig import AsyncIG
            self.ig = AsyncIG()
            self._owns_ig = True

        # IG 워커 기동(이미 떠 있으면 건너뜀)
        if self._ig_task is None or self._ig_task.done():
            try:
                self._ig_task = asyncio.create_task(self.ig.start())
            except TypeError:
                try:
                    self.ig.start()  # type: ignore[attr-defined]
                except Exception:
                    pass

    async def _ensure_tsp_open(self) -> None:
        """Start 버튼을 눌렀을 때에만 RFC2217 URL로 연결."""
        if self.tsp is not None and getattr(self.tsp, "is_connected", False):
            return
        async with self._connect_lock:
            if self.tsp is not None and getattr(self.tsp, "is_connected", False):
                return

            # RFC2217 URL 사용: e.g. "rfc2217://192.168.0.50:4001"
            self.client = TSPLetterClient(port=self.port, baudrate=self.baud)
            self.tsp = AsyncTSP(self.client)
            try:
                await self.tsp.ensure_open()
                self.append_log("TSP", f"연결됨: {self.port} @ {self.baud}")
            except Exception as e:
                self.append_log("TSP", f"연결 실패: {e!r}")
                self.client = None
                self.tsp = None
                raise

    # ---------------- 실행 ----------------
    async def _run_process(self) -> None:
        self._is_running = True
        self.cancel_event = asyncio.Event()

        try:
            # Start 클릭 시점에만 연결/IG 시작
            await self._ensure_tsp_open()
            await self._ensure_ig_started()

            cfg = TSPProcessConfig(
                target_pressure_torr=self._read_target_pressure(),
                tsp_on_seconds=75.0,        # 1분 15초
                off_wait_seconds=150.0,     # 2분 30초
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
            try:
                self.ui.TSP_nowCycle_edit.setPlainText("0")
            except Exception:
                pass
            self.append_log(
                "RUN",
                f"시작: target={cfg.target_pressure_torr:.3e} Torr, "
                f"on={cfg.tsp_on_seconds:.0f}s, off={cfg.off_wait_seconds:.0f}s, "
                f"max_cycles={cfg.max_cycles}"
            )

            # 실행
            res = await self.runner.run(cfg, cancel_event=self.cancel_event)
            self.append_log(
                "RESULT",
                f"ok={res.ok}, used={res.cycles_used}, "
                f"lastP={res.last_pressure_torr:.3e}, reason={res.reason}"
            )

        except Exception as e:
            try:
                QMessageBox.critical(None, "TSP", f"TSP 실행 실패:\n{e}")
            except Exception:
                pass
            self.append_log("ERR", f"TSP 실행 예외: {e!r}")

        finally:
            # 총 사이클 입력란 복원
            try:
                self.ui.TSP_setCycle_edit.setPlainText("10")
            except Exception:
                pass

            # “Start 때만 연결” 원칙 → 실행 종료 후 포트 닫음
            try:
                if self.tsp is not None:
                    await self.tsp.aclose()
                    self.append_log("TSP", "포트 닫힘")
            except Exception:
                pass
            finally:
                self.client = None
                self.tsp = None

            # 내부 소유 IG면 가볍게 정리(외부 주입이면 건드리지 않음)
            if self._owns_ig and self.ig is not None:
                try:
                    if hasattr(self.ig, "cleanup_quick"):
                        await self.ig.cleanup_quick()  # type: ignore[attr-defined]
                    elif hasattr(self.ig, "cleanup"):
                        await self.ig.cleanup()        # type: ignore[attr-defined]
                except Exception:
                    pass
                self.ig = None
                self._ig_task = None
                self._owns_ig = False

            self._is_running = False
            self.cancel_event = None
            self.runner = None

    # ---------------- 버튼 핸들러 ----------------
    def on_start_clicked(self) -> None:
        if self._is_running:
            try:
                QMessageBox.information(None, "TSP", "이미 공정이 진행 중입니다.")
            except Exception:
                pass
            return
        asyncio.create_task(self._run_process())

    def on_stop_clicked(self) -> None:
        if self.cancel_event and not self.cancel_event.is_set():
            self.append_log("RUN", "사용자 STOP 요청")
            self.cancel_event.set()

    # ---------------- 외부 주입/종료 ----------------
    def attach_ig(self, ig: IGControllerLike) -> None:
        """나중에 외부 IG를 붙이고 싶을 때 호출(내부 IG 소유권 해제)."""
        self.ig = ig
        self._owns_ig = False

    async def aclose(self) -> None:
        # 실행 중이면 중단 요청
        try:
            if self.cancel_event and not self.cancel_event.is_set():
                self.cancel_event.set()
        except Exception:
            pass

        await asyncio.sleep(0.1)
        try:
            if self.tsp is not None:
                await self.tsp.aclose()
                self.append_log("TSP", "포트 닫힘")
        except Exception:
            pass
        finally:
            self.client = None
            self.tsp = None

        if self._owns_ig and self.ig is not None:
            try:
                if hasattr(self.ig, "cleanup_quick"):
                    await self.ig.cleanup_quick()  # type: ignore[attr-defined]
                elif hasattr(self.ig, "cleanup"):
                    await self.ig.cleanup()        # type: ignore[attr-defined]
            except Exception:
                pass
            self.ig = None
            self._ig_task = None
            self._owns_ig = False
