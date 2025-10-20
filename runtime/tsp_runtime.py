# main_tsp.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import asyncio, contextlib
from collections import deque
from typing import Optional, Deque, Any, Mapping
from datetime import datetime, timedelta

# 장비/컨트롤러
from device.ig import AsyncIG
from device.tsp import AsyncTSP
from controller.tsp_controller import TSPProcessController, TSPRunConfig
from controller.chat_notifier import ChatNotifier
from controller.runtime_state import runtime_state # CH 상태 조회용

# 설정(기본값)
DEFAULT_HOST       = "192.168.1.50"
DEFAULT_IG_PORT    = 4001     # CH1 IG
DEFAULT_TSP_PORT   = 4004     # TSP
DWELL_SEC          = 150.0    # 2분 30초
POLL_SEC           = 5.0      # 5초
VERIFY_WITH_STATUS = True     # TSP on/off 후 205 확인

# ⬇ 매일 08:00 자동 예약 실행 설정
ENABLE_TSP_DAILY_7AM = True   # 자동 예약을 끄려면 False
DAILY_HH = 7
DAILY_MM = 30

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
        chat: Optional[ChatNotifier] = None,
        log_dir: Path | str | None = None,
    ) -> None:
        self.ui = ui
        self.loop = loop or asyncio.get_event_loop()
        self.host = host
        self.tsp_port = int(tcp_port)
        self.ig_port = DEFAULT_IG_PORT

        self.chat = chat

        # ▼ NAS 로그 설정
        self._log_root = Path(log_dir) if log_dir else Path.cwd()
        self._log_dir = self._ensure_log_dir(self._log_root / "TSP")
        self._log_file_path: Path | None = None
        self._prestart_buf: Deque[str] = deque(maxlen=1000)
        self._log_q: asyncio.Queue[str] = asyncio.Queue(maxsize=4096)
        self._log_fp = None
        self._log_writer_task: asyncio.Task | None = None

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
        now_ui = datetime.now().strftime("%H:%M:%S")
        now_file = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line_ui = f"[{now_ui}] [TSP] {msg}"
        line_file = f"[{now_file}] [TSP] {msg}\n"

        # UI
        edit = getattr(self.ui, "pc_logMessage_edit", None)
        if edit is not None:
            with contextlib.suppress(Exception):
                edit.appendPlainText(line_ui)

        # 파일: 파일 준비 전이면 프리버퍼에, 준비 후엔 큐로
        if not self._log_file_path:
            self._prestart_buf.append(line_file)
        else:
            self._log_enqueue_nowait(line_file)

        # 콘솔 출력 제거 (UI+파일만 기록)
        # print(line_ui)

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
        
        # ⬇ CH1이 공정 중이면 시작 자체를 차단
        try:
            if runtime_state.is_running(1):
                self._log("[TSP] CH1이 공정 중이라서 TSP를 시작할 수 없습니다. CH1 종료 후 다시 시도하세요.")
                return
        except Exception:
            pass

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
            # 시작 직전 레이스 가드
            if runtime_state.is_running(1):
                self._log("[TSP] 시작 직전 CH1 공정이 감지되어 중단합니다.")
                return

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

            # ★ 로그 파일 준비
            now_local = datetime.now().astimezone()
            ts = now_local.strftime("%Y%m%d_%H%M%S")
            self._log_file_path = (self._log_dir / f"TSP_{ts}").with_suffix(".txt")
            if not self._log_writer_task or self._log_writer_task.done():
                self._log_writer_task = asyncio.create_task(self._log_writer_loop(), name="LogWriter.TSP")

            # ★ 프리버퍼 → 파일 큐로 플러시
            if self._prestart_buf:
                for line in list(self._prestart_buf):
                    self._log_enqueue_nowait(line)
                self._prestart_buf.clear()

            self._log(
                "=== TSP 공정 시작 === "
                f"host={self.host} ig={self.ig_port} tsp={self.tsp_port} "
                f"target={target} cycles={cycles} "
                f"on={cfg.on_sec}s off={cfg.off_sec}s poll={cfg.poll_sec}s first_wait={cfg.first_check_delay_sec}s"
            )

            # ← 추가: Google Chat 시작 카드
            if self.chat:
                params = {
                    "process_note": f"TSP run: target={target:.3e}, cycles={cycles}",
                    "tsp_target": target,
                    "tsp_cycles": cycles,
                }
                self.chat.notify_process_started(params)

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

            # ← 수정: Google Chat 종료 카드(상세, 상태 분류)
            if self.chat:
                # 1) 상태 분류
                reason_str = (result.reason or "").lower() if isinstance(result.reason, str) else str(result.reason)
                if result.success:
                    status = "normal_completed"
                    status_label = "정상 종료"
                    ok_for_chat = True
                elif "cycles_exhausted" in reason_str:
                    # 목표 압력은 못 미달했지만 설정한 사이클을 모두 수행 → 오류가 아님
                    status = "cycles_completed"
                    status_label = "목표 사이클 완료"
                    ok_for_chat = True
                else:
                    status = "error"
                    status_label = "오류로 공정 종료"
                    ok_for_chat = False

                # 2) 상세 데이터 조립
                detail = {
                    "process_name": "TSP",
                    "status": status,                 # 내부용 상태 코드
                    "status_label": status_label,     # 카드에 보여줄 문구
                    "final_pressure": result.final_pressure,
                    "cycles_done": result.cycles_done,
                    "reason": result.reason,
                }
                if not ok_for_chat:
                    # 진짜 오류일 때만 errors 채움
                    if result.reason:
                        detail["errors"] = [str(result.reason)]
                    else:
                        detail["errors"] = ["unknown_error"]

                # 3) 성공/중립(OK) 여부를 첫 번째 인자로 전달
                self.chat.notify_process_finished_detail(ok_for_chat, detail)

        except asyncio.CancelledError:
            self._log("[사용자 중단]")

            # ← 추가: 사용자 Stop 종료 카드
            if self.chat:
                self.chat.notify_process_finished_detail(False, {
                    "process_name": "TSP",
                    "stopped": True,
                })
        except Exception as e:
            self._log(f"[ERROR] 실행 실패: {e!r}")

            # ← 추가: 오류 종료 카드
            if self.chat:
                self.chat.notify_process_finished_detail(False, {
                    "process_name": "TSP",
                    "errors": [str(e)],
                })

        finally:
            with contextlib.suppress(Exception):
                if self.tsp:
                    await self.tsp.off()
            with contextlib.suppress(Exception):
                if self.tsp:
                    await self.tsp.aclose()
            # 라이터 종료
            with contextlib.suppress(Exception):
                if self._log_writer_task:
                    self._log_writer_task.cancel()
                    await self._log_writer_task
                self._log_writer_task = None

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
    # NAS 로그 유틸리티
    # ─────────────────────────────────────────────────────
    def _ensure_log_dir(self, root: Path) -> Path:
        nas = Path(root)
        local = Path.cwd() / "_Logs_local_TSP"
        try:
            nas.mkdir(parents=True, exist_ok=True)
            return nas
        except Exception:
            local.mkdir(parents=True, exist_ok=True)
            self._log("[Logger] NAS 폴더 접근 실패 → 로컬 폴백 사용")
            return local

    def _log_enqueue_nowait(self, line: str) -> None:
        try:
            self._log_q.put_nowait(line)
        except asyncio.QueueFull:
            with contextlib.suppress(Exception):
                _ = self._log_q.get_nowait()
                self._log_q.put_nowait(line)

    async def _log_writer_loop(self):
        try:
            while True:
                line = await self._log_q.get()
                if self._log_fp is None and self._log_file_path:
                    with contextlib.suppress(Exception):
                        self._log_fp = open(self._log_file_path, "a", encoding="utf-8", newline="")
                if not self._log_fp:
                    await asyncio.sleep(0.1)
                    self._log_enqueue_nowait(line)
                    continue
                with contextlib.suppress(Exception):
                    self._log_fp.write(line); self._log_fp.flush()
        except asyncio.CancelledError:
            pass
        finally:
            if self._log_fp:
                with contextlib.suppress(Exception):
                    self._log_fp.flush(); self._log_fp.close()
                self._log_fp = None        

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

                # 실행 직전: TSP 자체가 바쁘거나, CH1이 공정 중이면 대기
                # (로그는 60초에 한 번만 출력해 스팸 방지)
                last_log_ts = 0.0
                loop_time = asyncio.get_running_loop().time
                while self._busy or runtime_state.is_running(1):
                    t = loop_time()
                    if t - last_log_ts >= 60.0:
                        msg = "[TSP] 현재 공정 중… 예약 실행을 대기합니다." if self._busy \
                            else "[TSP] CH1 공정 중… 예약 실행을 대기합니다."
                        self._log(msg)
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

