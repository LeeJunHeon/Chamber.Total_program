# main_tsp.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, contextlib
from typing import Optional
from datetime import datetime

# 장비/컨트롤러
from device.ig import AsyncIG
from device.tsp import AsyncTSP
from controller.tsp_controller import TSPProcessController, TSPRunConfig

# 설정(기본값)
DEFAULT_HOST       = "192.168.1.50"
DEFAULT_IG_PORT    = 4001     # CH1 IG
DEFAULT_TSP_PORT   = 4004     # TSP
DWELL_SEC          = 150.0    # 2분 30초
POLL_SEC           = 5.0      # 5초
VERIFY_WITH_STATUS = True     # TSP on/off 후 205 확인

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

class TSPPageController:
    """
    UI 페이지용 컨트롤러:
      - 로그: ui.pc_logMessage_edit
      - 입력: ui.TSP_targetPressure_edit, ui.TSP_setCycle_edit
      - 버튼: ui.TSP_Start_button / ui.TSP_Stop_button
      - 표시: ui.TSP_nowCycle_edit(현재 사이클), ui.TSP_basePressure_edit(현재 압력)
    """
    def __init__(
        self,
        ui,
        *,
        host: str = DEFAULT_HOST,
        tcp_port: int = DEFAULT_TSP_PORT,
        addr: int = 0x01,  # main.py 호환용(미사용)
        loop: Optional[asyncio.AbstractEventLoop] = None,
        ig: Optional[AsyncIG] = None,  # 외부에서 IG 주입 가능
    ) -> None:
        self.ui = ui
        self.loop = loop or asyncio.get_event_loop()
        self.host = host
        self.tsp_port = int(tcp_port)
        self.ig_port = DEFAULT_IG_PORT

        self._ig_ext = ig is not None
        self.ig: Optional[AsyncIG] = ig
        self.tsp: Optional[AsyncTSP] = None
        self._task: Optional[asyncio.Task] = None
        self._busy = False

        self._connect_buttons()

    # ── UI 헬퍼 ─────────────────────────────────────────────
    def _log(self, msg: str) -> None:
        edit = getattr(self.ui, "pc_logMessage_edit", None)
        if edit is not None:
            try:
                edit.appendPlainText(f"[{_ts()}] {msg}")
                return
            except Exception:
                pass
        print(f"[{_ts()}] {msg}")

    def _get_plain(self, name: str) -> Optional[str]:
        w = getattr(self.ui, name, None)
        if w is None:
            return None
        try:
            return w.toPlainText().strip()
        except Exception:
            return None

    def _set_plain(self, name: str, value: str) -> None:
        w = getattr(self.ui, name, None)
        if w is None:
            return
        try:
            w.setPlainText(value)
        except Exception:
            pass

    def _read_target(self) -> float:
        txt = self._get_plain("TSP_targetPressure_edit")
        if not txt:
            raise ValueError("TSP_targetPressure_edit 가 비어있습니다.")
        return float(txt)

    def _read_cycles(self) -> int:
        txt = self._get_plain("TSP_setCycle_edit")
        if not txt:
            raise ValueError("TSP_setCycle_edit 가 비어있습니다.")
        v = int(txt)
        if v < 1:
            raise ValueError("반복 횟수는 1 이상이어야 합니다.")
        return v

    def _connect_buttons(self) -> None:
        start_btn = getattr(self.ui, "TSP_Start_button", None)
        if start_btn is not None:
            try:
                start_btn.clicked.connect(self.on_start_clicked)  # type: ignore[attr-defined]
            except Exception:
                pass
        stop_btn = getattr(self.ui, "TSP_Stop_button", None)
        if stop_btn is not None:
            try:
                stop_btn.clicked.connect(self.on_stop_clicked)    # type: ignore[attr-defined]
            except Exception:
                pass

    # ── Start/Stop 핸들러 ──────────────────────────────────
    def on_start_clicked(self) -> None:
        if self._busy:
            self._log("이미 실행 중입니다.")
            return
        try:
            target = self._read_target()
            cycles = self._read_cycles()
        except Exception as e:
            self._log(f"[ERROR] 입력 파싱 실패: {e}")
            return

        # 시작 시 표시 초기화
        self._set_plain("TSP_nowCycle_edit", "0")
        self._set_plain("TSP_basePressure_edit", "")

        self._busy = True
        self._task = asyncio.create_task(self._run(target, cycles))

    def on_stop_clicked(self) -> None:
        if self._task and not self._task.done():
            self._log("중단 요청")
            self._task.cancel()

    # ── 내부 실행 루틴 ─────────────────────────────────────
    async def _run(self, target: float, cycles: int) -> None:
        try:
            # 장비 인스턴스 준비
            if self.ig is None:
                self.ig = AsyncIG(host=self.host, port=self.ig_port)
            self.tsp = AsyncTSP(host=self.host, port=self.tsp_port)

            # 콜백
            def state_cb(s: str): self._log(f"[STATE] {s}")
            def pressure_cb(p: float):
                self._log(f"[IG] P={p:.3e}")
                self._set_plain("TSP_basePressure_edit", f"{p:.3e}")
            def cycle_cb(cur: int, total: int):
                self._log(f"[TSP] cycle {cur}/{total} 완료")
                self._set_plain("TSP_nowCycle_edit", str(cur))

            ctrl = TSPProcessController(
                tsp=self.tsp, ig=self.ig,
                log_cb=self._log, state_cb=state_cb,
                pressure_cb=pressure_cb, cycle_cb=cycle_cb,
                turn_off_ig_on_finish=True,  # 종료 시 IG OFF
            )

            cfg = TSPRunConfig(
                target_pressure=target,
                cycles=cycles,
                dwell_sec=DWELL_SEC,
                poll_sec=POLL_SEC,
                verify_with_status=VERIFY_WITH_STATUS,
            )

            self._log(f"=== TSP 공정 시작 === host={self.host} ig={self.ig_port} tsp={self.tsp_port} "
                      f"target={target} cycles={cycles} dwell={DWELL_SEC}s poll={POLL_SEC}s")
            result = await ctrl.run(cfg)

            self._log("=== 결과 ===")
            self._log(f" success       : {result.success}")
            if result.final_pressure == result.final_pressure:
                self._log(f" final_pressure: {result.final_pressure:.3e}")
                self._set_plain("TSP_basePressure_edit", f"{result.final_pressure:.3e}")
            else:
                self._log(" final_pressure: NaN")
            self._log(f" cycles_done   : {result.cycles_done}")
            self._log(f" reason        : {result.reason}")

        except asyncio.CancelledError:
            self._log("[사용자 중단]")
        except Exception as e:
            self._log(f"[ERROR] 실행 실패: {e!r}")
        finally:
            # 안전 정리: TSP OFF + 연결 닫기, IG OFF(내부 생성한 경우만)
            with contextlib.suppress(Exception):
                if self.tsp:
                    await self.tsp.off()
            with contextlib.suppress(Exception):
                if self.tsp:
                    await self.tsp.aclose()
            if not self._ig_ext:
                with contextlib.suppress(Exception):
                    if self.ig:
                        await self.ig.ensure_off()
                self.ig = None
            self.tsp = None
            self._busy = False
            self._task = None
