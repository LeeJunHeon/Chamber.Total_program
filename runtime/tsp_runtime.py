# main_tsp.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, contextlib
from typing import Optional
from datetime import datetime, timedelta

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

# ⬇ 매일 07:00 자동 예약 실행 설정
ENABLE_TSP_DAILY_7AM = True   # 자동 예약을 끄려면 False
DAILY_HH = 7
DAILY_MM = 0

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

        # ⬇ 예약 실행 상태
        self._schedule_task: Optional[asyncio.Task] = None
        self._schedule_repeat_daily: bool = False

        self._connect_buttons()

        self._defaults = {
            "target": self._get_plain("TSP_targetPressure_edit") or "2.5e-07",
            "cycles": self._get_plain("TSP_setCycle_edit") or "10",
        }
        
        # ⬇ 프로그램 기동 시 매일 07:00 예약 등록
        try:
            if ENABLE_TSP_DAILY_7AM:
                when = self._next_time_at(DAILY_HH, DAILY_MM)
                self.schedule_run_at(when, repeat_daily=True)
        except Exception as _e:
            self._log(f"[TSP] 예약 초기화 실패: {_e!r}")

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

    def _reset_ui_defaults(self) -> None:
        # 입력값: 프로그램 처음 켰을 때의 값을 복원
        self._set_plain("TSP_targetPressure_edit", self._defaults["target"])
        self._set_plain("TSP_setCycle_edit", self._defaults["cycles"])
        # 표시값: 공정 전 상태로 정리
        self._set_plain("TSP_nowCycle_edit", "0")
        self._set_plain("TSP_basePressure_edit", "")


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
        # 1) 실행 중이면 즉시 중단
        if self._task and not self._task.done():
            self._log("[TSP] 중단 요청")
            self._task.cancel()

        # 2) 실행 중이 아니더라도, 예약이 걸려 있으면 예약 취소
        if self._schedule_task and not self._schedule_task.done():
            self.cancel_schedule()  # 내부에서 "[TSP] 예약 실행 취소됨" 로그 출력


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
                on_sec=120.0,                 # 2분
                off_sec=150.0,                # 2분 30초
                poll_sec=10.0,                # IG 10초 간격 RDI
                first_check_delay_sec=5.0,    # IG ON 후 5초 대기
                verify_with_status=True,
            )

            self._log(
                "=== TSP 공정 시작 === "
                f"host={self.host} ig={self.ig_port} tsp={self.tsp_port} "
                f"target={target} cycles={cycles} "
                f"on={cfg.on_sec}s off={cfg.off_sec}s poll={cfg.poll_sec}s first_wait={cfg.first_check_delay_sec}s"
            )
            result = await ctrl.run(cfg)

            # UI 표시는 유지
            if result.final_pressure == result.final_pressure:
                self._set_plain("TSP_basePressure_edit", f"{result.final_pressure:.3e}")

            # 로그는 한 줄 요약
            self._log(
                f"=== RESULT === ok={result.success} "
                f"final_P={(f'{result.final_pressure:.3e}' if result.final_pressure == result.final_pressure else 'NaN')} "
                f"cycles_done={result.cycles_done} reason={result.reason}"
            )

        except asyncio.CancelledError:
            self._log("[사용자 중단]")
        except Exception as e:
            self._log(f"[ERROR] 실행 실패: {e!r}")
        finally:
            with contextlib.suppress(Exception):
                if self.tsp:
                    await self.tsp.off()
            with contextlib.suppress(Exception):
                if self.tsp:
                    await self.tsp.aclose()

            if not self._ig_ext:
                # 1) IG를 확실히 OFF
                with contextlib.suppress(Exception):
                    if self.ig:
                        await self.ig.ensure_off()
                # 2) ★ IG 백그라운드 태스크/세션 완전 정리 (핵심 수정)
                with contextlib.suppress(Exception):
                    if self.ig:
                        await self.ig.cleanup()

                self.ig = None

            self.tsp = None

            # 3) ★ UI를 초기 기본값으로 복원
            with contextlib.suppress(Exception):
                self._reset_ui_defaults()

            self._busy = False
            self._task = None

    # ─────────────────────────────────────────────────────
    # 예약 실행 유틸리티
    # ─────────────────────────────────────────────────────
    def _next_time_at(self, hh: int, mm: int) -> datetime:
        """오늘 hh:mm, 이미 지났으면 내일 같은 시각을 리턴."""
        now = datetime.now()
        cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if cand <= now:
            cand = cand + timedelta(days=1)
        return cand

    def schedule_run_at(self, when: datetime, *, repeat_daily: bool = False) -> None:
        """지정 시각(로컬)에 TSP 공정을 자동 시작."""
        self.cancel_schedule(silent=True)
        self._schedule_repeat_daily = bool(repeat_daily)

        # ✅ running 여부와 무관하게 안전: 현재 set된 이벤트 루프를 얻어서 create_task
        loop = asyncio.get_event_loop_policy().get_event_loop()
        self._schedule_task = loop.create_task(
            self._schedule_loop(when), name="TSPStartScheduler"
        )

        self._log(f"[TSP] 예약 실행 등록: {when.strftime('%Y-%m-%d %H:%M:%S')} (매일={self._schedule_repeat_daily})")

    def cancel_schedule(self, silent: bool = False) -> None:
        """예약 실행 취소."""
        if self._schedule_task and not self._schedule_task.done():
            self._schedule_task.cancel()
        self._schedule_task = None
        self._schedule_repeat_daily = False
        if not silent:
            self._log("[TSP] 예약 실행 취소됨")

    async def _schedule_loop(self, when: datetime) -> None:
        """예약: when 도달 → (바쁘면 대기) → on_start_clicked 트리거 → (옵션) 매일 반복."""
        try:
            while True:
                # ⬇ 필요한 만큼만 한 번 대기 (task.cancel()로 즉시 취소 가능)
                now = datetime.now()
                remain = (when - now).total_seconds()
                if remain > 0:
                    await asyncio.sleep(remain)

                # 실행 직전: 다른 공정이 돌고 있으면 비어질 때까지 대기
                # (로그는 60초에 한 번만 출력해 스팸 방지)
                last_log_ts = 0.0
                loop_time = asyncio.get_running_loop().time
                while self._busy:
                    t = loop_time()
                    if t - last_log_ts >= 60.0:
                        self._log("[TSP] 현재 공정 중… 예약 실행을 대기합니다.")
                        last_log_ts = t
                    await asyncio.sleep(5.0)

                # 실행 트리거
                try:
                    self.on_start_clicked()
                except Exception as e:
                    self._log(f"[TSP] 예약 시작 실패: {e!r}")

                # 반복 모드 아니면 종료
                if not self._schedule_repeat_daily:
                    self._log("[TSP] 1회 예약 실행 완료(반복 없음).")
                    break

                # 다음날 같은 시각으로 갱신
                when = when + timedelta(days=1)
                self._log(f"[TSP] 다음 반복 예약: {when.strftime('%Y-%m-%d %H:%M:%S')}")
        except asyncio.CancelledError:
            # 취소 시 조용히 종료
            pass
        finally:
            if not self._schedule_repeat_daily:
                self._schedule_task = None

