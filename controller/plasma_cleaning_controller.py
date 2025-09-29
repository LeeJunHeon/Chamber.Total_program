# controller/plasma_cleaning_controller.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Mapping, Optional, TypedDict, Union, Literal

# ──────────────────────────────────────────────────────────────
# 파라미터/이벤트 타입
# ──────────────────────────────────────────────────────────────

class PCParams(TypedDict, total=False):
    # MFC/가스
    pc_gas_mfc_idx: int            # 기본: 3번 (CH1의 Plasma Cleaning 전용 라인)
    pc_gas_flow_sccm: float        # (선택) 초기 유량/상한 (SP1 제어와 병행할 때 사용)
    # 압력 제어(SP1)
    pc_target_pressure_mTorr: float  # 목표 압력(mTorr) → SP1 set 대상
    pc_pressure_tolerance: float     # 안정화 판정 허용오차(±mTorr)
    pc_pressure_timeout_s: float     # 목표 도달 타임아웃(초)
    pc_pressure_settle_s: float      # 목표 도달 후 추가 안정화 대기(초)
    # RF
    pc_rf_power_w: float             # RF 연속 파워(W)
    pc_rf_reach_timeout_s: float     # RF 목표 도달 확인 타임아웃(초, 옵션)
    # 유지시간
    pc_process_time_min: float       # 유지 시간(분)
    # 기타
    process_note: str

@dataclass
class PCEvent:
    kind: Literal[
        "log", "state", "status", "started", "finished", "aborted",
        "polling_targets", "polling"
    ]
    payload: Optional[Mapping[str, Any]] = None


# ──────────────────────────────────────────────────────────────
# 컨트롤러
# ──────────────────────────────────────────────────────────────

class PlasmaCleaningController:
    """
    Plasma Cleaning 전용 상태머신(IG 미사용).
    시퀀스:
      1) (선택) MFC #idx에 초기 유량 설정
      2) SP1 Set(목표 압력) → SP1 On
      3) 압력 목표 도달 + (선택) 안정화 대기
      4) RF 연속 파워 설정, 목표 도달(옵션) 확인
      5) 유지 타이머 진행
      6) 안전 종료(RF OFF → SP1 OFF → MFC 유량 0)

    콜백(런타임에서 주입):
      - send_mfc(cmd, args): MFC 제어(예: "set_flow", {"gas_idx":3, "flow":20.0, "ch":1})
      - sp1_set(mTorr): SP1 setpoint 를 설정
      - sp1_on(on: bool): SP1 제어 On/Off
      - send_rf_power(power_w): RF 연속 파워 목표 설정 시작
      - stop_rf_power(): RF 전원/목표 해제
      - wait_pressure_stable(target_mTorr, tol_mTorr, timeout_s, settle_s) -> bool :
            목표±tol에 들어오고 settle_s 동안 유지되면 True
      - await_rf_target(power_w, timeout_s) -> bool :
            RF 목표 도달 여부 대기(선택)
    """

    # ── 생성/설정 ────────────────────────────────────────────────
    def __init__(
        self,
        *,
        ch: int,
        send_mfc: Callable[[str, Mapping[str, Any]], None],
        sp1_set: Callable[[float], None],
        sp1_on: Callable[[bool], None],
        send_rf_power: Callable[[float], None],
        stop_rf_power: Callable[[], None],
        wait_pressure_stable: Optional[
            Callable[[float, float, float, float], Coroutine[Any, Any, bool]]
        ] = None,
        await_rf_target: Optional[
            Callable[[float, float], Coroutine[Any, Any, bool]]
        ] = None,
    ) -> None:
        self.ch = int(ch)
        self.event_q: asyncio.Queue[PCEvent] = asyncio.Queue()

        # 콜백
        self._send_mfc = send_mfc
        self._sp1_set = sp1_set
        self._sp1_on = sp1_on
        self._send_rf = send_rf_power
        self._stop_rf = stop_rf_power
        self._wait_pressure_stable = wait_pressure_stable
        self._await_rf_target = await_rf_target

        # 런 상태
        self._task: Optional[asyncio.Task] = None
        self._stop_evt = asyncio.Event()
        self._running = False
        self.current_params: PCParams = {}

    @property
    def is_running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    # ── 외부 API ────────────────────────────────────────────────
    def start(self, params: PCParams) -> None:
        if self.is_running:
            self._emit_log("PC", "이미 공정 실행 중입니다."); return
        self._stop_evt = asyncio.Event()
        self.current_params = self._normalize(params)
        self._task = asyncio.create_task(self._run(self.current_params), name=f"PCLEAN.CH{self.ch}")

    def request_stop(self) -> None:
        self._stop_evt.set()

    # ── 이벤트 도우미 ───────────────────────────────────────────
    def _emit(self, kind: PCEvent["kind"], payload: Optional[Mapping[str, Any]] = None) -> None:
        try:
            self.event_q.put_nowait(PCEvent(kind, payload))
        except asyncio.QueueFull:
            pass

    def _emit_log(self, src: str, msg: str) -> None:
        self._emit("log", {"src": f"{src}{self.ch}", "msg": msg})

    def _emit_state(self, text: str) -> None:
        self._emit("state", {"text": text})

    def _emit_status(self, running: bool) -> None:
        self._emit("status", {"running": running})

    def _emit_started(self, params: PCParams) -> None:
        self._emit("started", {"params": dict(params)})

    def _emit_finished(self, ok: bool, detail: Mapping[str, Any]) -> None:
        self._emit("finished", {"ok": ok, "detail": dict(detail)})

    def _emit_aborted(self) -> None:
        self._emit("aborted", {})

    def _set_polling(self, *, mfc: bool, rf: bool) -> None:
        self._emit("polling_targets", {"targets": {"mfc": mfc, "rf": rf}})

    # ── 실행 로직 ────────────────────────────────────────────────
    async def _run(self, p: PCParams) -> None:
        self._running = True
        ok = False
        detail: dict[str, Any] = {}
        self._emit_status(True)
        self._emit_started(p)
        self._set_polling(mfc=True, rf=True)

        note = p.get("process_note", f"PlasmaCleaning CH{self.ch}")
        try:
            self._emit_state(f"[PC] 시작: {note}")

            # (선택) 1) MFC 초기 유량/가스라인
            if p["pc_gas_flow_sccm"] > 0:
                self._emit_state(f"[PC] MFC#{p['pc_gas_mfc_idx']} 초기 유량 {p['pc_gas_flow_sccm']:.3f} sccm")
                self._send_mfc("set_flow", {
                    "gas_idx": int(p["pc_gas_mfc_idx"]),
                    "flow": float(p["pc_gas_flow_sccm"]),
                    "ch": self.ch,
                })
                self._emit_log("MFC", f"MFC#{p['pc_gas_mfc_idx']} -> {p['pc_gas_flow_sccm']:.3f} sccm")

            # 2) SP1 설정/On
            tgt = float(p["pc_target_pressure_mTorr"])
            self._emit_state(f"[PC] SP1 Set {tgt:g} mTorr")
            self._sp1_set(tgt)
            await asyncio.sleep(0.05)  # 전송 후 소폭 지연
            self._emit_state("[PC] SP1 On")
            self._sp1_on(True)

            # 3) 압력 안정화 대기
            tol = float(p["pc_pressure_tolerance"])
            tmo = float(p["pc_pressure_timeout_s"])
            settle = float(p["pc_pressure_settle_s"])
            self._emit_state(f"[PC] 압력대기: {tgt:g}±{tol:g} mTorr, timeout {tmo:.0f}s, settle {settle:.0f}s")
            pres_ok = await self._wait_pressure_or_fallback(tgt, tol, tmo, settle)
            if not pres_ok:
                self._emit_log("PC", "압력 안정화 타임아웃")
                detail.update(stage="pressure_stable_timeout", target_mTorr=tgt, tol_mTorr=tol)
                self._emit_finished(False, detail)
                return

            if self._stop_evt.is_set():
                self._emit_log("PC", "사용자 중단 요청 감지")
                self._emit_aborted()
                return

            # 4) RF 파워
            pw = float(p["pc_rf_power_w"])
            self._emit_state(f"[PC] RF 파워 설정 {pw:g} W")
            self._send_rf(pw)
            self._emit_log("RF", f"RF 목표 {pw:g} W 전송")

            if p["pc_rf_reach_timeout_s"] > 0 and self._await_rf_target:
                rf_ok = await self._wait_rf_or_fallback(pw, p["pc_rf_reach_timeout_s"])
                if not rf_ok:
                    self._emit_log("PC", "RF 목표 도달 확인 실패(무시하고 계속)")
                    detail.update(rf_reach_timeout=True)

            if self._stop_evt.is_set():
                self._emit_log("PC", "사용자 중단 요청 감지")
                self._emit_aborted()
                return

            # 5) 유지 타이머
            hold_s = max(0.0, float(p["pc_process_time_min"]) * 60.0)
            self._emit_state(f"[PC] 유지 {hold_s:.0f}초 진행")
            detail.update(hold_seconds=hold_s)
            ok = await self._hold_loop(hold_s)

        except asyncio.CancelledError:
            self._emit_log("PC", "태스크 취소")
            self._emit_aborted()
            return
        except Exception as e:
            self._emit_log("PC", f"예외: {e!r}")
            detail.update(error=str(e))
            self._emit_finished(False, detail)
            return
        finally:
            # 6) 안전 종료
            try:
                self._emit_state("[PC] 종료: RF OFF → SP1 OFF → MFC=0 sccm")
                try: self._stop_rf()
                except Exception: self._emit_log("RF", "stop 실패(무시)")
                try: self._sp1_on(False)
                except Exception: self._emit_log("PC", "SP1 Off 실패(무시)")
                try:
                    self._send_mfc("set_flow", {"gas_idx": int(p["pc_gas_mfc_idx"]), "flow": 0.0, "ch": self.ch})
                except Exception:
                    self._emit_log("MFC", "유량 0 명령 실패(무시)")
            finally:
                self._set_polling(mfc=False, rf=False)
                self._emit_status(False)
                self._running = False

        # 종료 리포트
        if ok:
            self._emit_state("[PC] 완료")
            self._emit_finished(True, detail)
        else:
            self._emit_state("[PC] 실패")
            self._emit_finished(False, detail)

    # ── 보조: 대기 루틴 ─────────────────────────────────────────
    async def _wait_pressure_or_fallback(self, target: float, tol: float, timeout_s: float, settle_s: float) -> bool:
        if self._wait_pressure_stable:
            try:
                return await asyncio.wait_for(
                    self._wait_pressure_stable(target, tol, timeout_s, settle_s),
                    timeout=timeout_s + settle_s + 2.0,
                )
            except asyncio.TimeoutError:
                return False
        # 폴백: 실제 압력 감시 콜백이 없으면 짧게 대기만(실사용에선 콜백 구현 권장)
        await asyncio.sleep(min(3.0, timeout_s))
        return True

    async def _wait_rf_or_fallback(self, target_w: float, timeout_s: float) -> bool:
        if self._await_rf_target:
            try:
                return await asyncio.wait_for(self._await_rf_target(target_w, timeout_s), timeout=timeout_s + 1.0)
            except asyncio.TimeoutError:
                return False
        await asyncio.sleep(min(2.0, timeout_s))
        return True

    async def _hold_loop(self, seconds: float) -> bool:
        remain = float(seconds)
        step = 0.5
        while remain > 0:
            if self._stop_evt.is_set():
                self._emit_log("PC", "사용자 중단으로 유지 단계 종료")
                self._emit_aborted()
                return False
            self._emit_state(f"[PC] 진행 중… 남은 {int(remain)} s")
            await asyncio.sleep(step)
            remain -= step
        return True

    # ── 정규화 ──────────────────────────────────────────────────
    def _normalize(self, raw: Mapping[str, Any]) -> PCParams:
        def f(v: Any, d: float) -> float:
            try: return float(v)
            except Exception: return float(d)
        def i(v: Any, d: int) -> int:
            try: return int(float(v))
            except Exception: return int(d)

        p: PCParams = {
            "pc_gas_mfc_idx":        i(raw.get("pc_gas_mfc_idx", 3), 3),
            "pc_gas_flow_sccm":      f(raw.get("pc_gas_flow_sccm", 0.0), 0.0),
            "pc_target_pressure_mTorr": f(raw.get("pc_target_pressure_mTorr", 2.0), 2.0),
            "pc_pressure_tolerance": f(raw.get("pc_pressure_tolerance", 0.2), 0.2),
            "pc_pressure_timeout_s": f(raw.get("pc_pressure_timeout_s", 90.0), 90.0),
            "pc_pressure_settle_s":  f(raw.get("pc_pressure_settle_s", 5.0), 5.0),
            "pc_rf_power_w":         f(raw.get("pc_rf_power_w", 100.0), 100.0),
            "pc_rf_reach_timeout_s": f(raw.get("pc_rf_reach_timeout_s", 0.0), 0.0),  # 0이면 생략
            "pc_process_time_min":   f(raw.get("pc_process_time_min", 1.0), 1.0),
            "process_note":          str(raw.get("process_note", f"PlasmaCleaning CH{self.ch}")),
        }

        # 음수/비정상 방어
        if p["pc_gas_flow_sccm"] < 0: p["pc_gas_flow_sccm"] = 0.0
        if p["pc_pressure_tolerance"] < 0: p["pc_pressure_tolerance"] = 0.0
        if p["pc_pressure_timeout_s"] < 0: p["pc_pressure_timeout_s"] = 0.0
        if p["pc_pressure_settle_s"] < 0: p["pc_pressure_settle_s"] = 0.0
        if p["pc_rf_power_w"] < 0: p["pc_rf_power_w"] = 0.0
        if p["pc_process_time_min"] < 0: p["pc_process_time_min"] = 0.0
        return p
