"""챔버12 공정 장비 동작 — v1 runtime/chamber_runtime.py 의 공정 콜백(_bind_process_controller 965-1681)과
장비 이벤트 펌프(_pump_mfc/ig/rga/dc/dc2/rf/rfpulse/dcpulse/oes_events 2195-2851)를 v2 드라이버로 다시 쓴 것.

- send(step, runner): 스텝 하나를 장비 호출로(v1 cb_plc·cb_mfc·cb_dc_power … 와 같은 경로·문구). 결과는 runner 로
  (match = 확인 토큰, step_failed·mfc_failed·power_off_failed·rga_finished·oes_failed). 기대 토큰은 runner 가 보내기 전에 건다.
- on_event(Logged): 조립(system.event_listeners)이 주는 드라이버 이벤트 중 이 챔버 장비 것만 — v1 펌프와 같은 처리
  (MFC 는 owner 가 다른 확인·실패를 넘기지 않음, DC target_failed 뒤 첫 OFF 완료 삼킴, DC Pulse OUTPUT_ON/REF_POWER/OUTPUT_OFF 확인,
  RF Pulse RF_OFF 실패는 OFF 실패 경로, 폴링 값 로그 문구 GAPB-006, RunStats 표본).
- PLC 명령 로그 '[CHn] 요청: NAME -> b (raw='…', ch=n)' · '[CHn] 완료: NAME -> b (N ms)'(GAPB-004, 시계 기준 ms).
- 실측 펄스 값 되읽기(GAPB-003): DC Pulse 시작 성공 뒤 freq·duty·off 를 기록 params 에 덮어쓰고 '[실측] …' 로그, RF Pulse 는 로그 없이.
- 폴링(polling_targets·apply_polling — v1 _compute_polling_targets 1838-1866·_apply_polling_targets 6519-6579): MFC 는 공유 사용자
  집합(shared.mfc — owner chamber{n}), IG 공정 상태, DC·DC2·RF 연속 폴링. 펄스 전원 폴링은 v2 드라이버가 출력 ON 동안 스스로 한다.
- 카메라(Main Shutter 열기 — RF 연속·RF Pulse 공정만, 참여자 chamber{n} — shared.camera).
v1 의 구글챗 카드·알림은 보내지 않는다(포털 몫) — 같은 계기에 엔진 이벤트(kind="chat")로 문구를 남긴다.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from v2.common.worker import Status
from v2.ch12.process_steps import common_info, norm_plc_name

_OFF_TOKEN = {"DC Power": "DC_OFF", "DC2 Power": "DC2_OFF", "RF Power": "RF_OFF"}


class ProcessActions:
    def __init__(self, eng) -> None:
        self.e = eng
        self.sys = eng.sys
        self.ch, self.n = eng.ch, eng.n
        self.owner = f"chamber{self.n}"
        self.plc = self.sys.shared.plc.proxy(self.ch.upper())
        self.mfc = self.sys.mfc[self.ch]
        self.ig = self.sys.ig[self.ch]
        self.dc = {1: self.sys.dc.get(1), 2: self.sys.dc.get(2)} if self.ch == "ch2" else {1: None, 2: None}
        self.rf = self.sys.rf.get("ch2") if self.ch == "ch2" else None
        self.dcp = self.sys.dcpulse.get(self.ch)
        self.rfp = self.sys.rfpulse.get(self.ch)
        self.oes = self.sys.oes.get(self.ch)
        self.rga = self.sys.rga.get(self.ch)
        self.heater = eng.heater
        self.thickness = eng.thickness
        self.norm_plc = norm_plc_name
        self.runner = None                       # 지금 공정의 StepRunner(엔진이 넣음)
        self.dc_failed = {1: False, 2: False}
        self.oes_active = False
        self.oes_initialized = False
        self.camera_joined = False
        self.sources = {f"{self.ch}.mfc", f"{self.ch}.ig", f"{self.ch}.dcpulse", f"{self.ch}.rfpulse",
                        f"{self.ch}.oes", f"{self.ch}.rga"} | ({"ch2.dc", "ch2.rf"} if self.ch == "ch2" else set())
        self.mfc_polling = False
        if self.heater is not None:
            self.heater.on_process = self._heater_result

    # ───────── 공통 ─────────
    def log(self, src: str, msg: str) -> None:
        self.e.log(src, msg)

    def _r(self):
        return self.runner

    def _match(self, kind: str, spec: Any = None) -> None:
        r = self.runner
        if r is not None:
            r.match(kind, spec)

    def _fail(self, source: str, why: Any, code: Optional[str] = None, meta: Optional[dict] = None) -> None:
        r = self.runner
        if r is not None:
            r.step_failed(source, why, code=code, meta=meta)

    def _ms_since(self, t0: float) -> float:
        return (self.e.clock.monotonic() - t0) * 1000.0

    # ═══════════════════════ 스텝 → 장비 ═══════════════════════
    def send(self, step, runner) -> None:
        a = step.action
        fn = getattr(self, f"_a_{a.lower()}", None)
        if fn is None:
            from v2.ch12.process_runner import StepFailure
            raise StepFailure("E213", f"알 수 없는 Action: {a}", {"action": a})
        fn(step)

    # PLC(cb_plc 968-1116)
    def _a_plc_cmd(self, step) -> None:
        name, on = step.params[0], step.params[1]
        raw, nname, onb = str(name), str(name).upper(), bool(on)
        t0 = self.e.clock.monotonic()
        self.log("PLC", f"[CH{self.n}] 요청: {nname} -> {onb} (raw='{raw}', ch={self.n})")
        if self.n == 1 and nname in ("G1", "G2", "G3", "N2"):
            why = "건 셔터 없음" if nname in ("G1", "G2", "G3") else "N2 라인 없음"
            self.log("PLC", f"[CH1] '{nname}' 명령은 무시({why}).")
            self._match("PLC", nname)
            return

        def failed(res_or_exc) -> None:
            err = getattr(res_or_exc, "error", None) or getattr(res_or_exc, "reason", None) or res_or_exc
            r = self.runner
            if r is not None:
                r.step_failed("PLC", err, code=getattr(res_or_exc, "code", None),
                              meta={"cmd": nname, "raw_cmd": raw, "ch": self.n, "requested_on": onb, "op": None,
                                    "addr": None})
            self.log("PLC", f"명령 실패: {raw} -> {onb}: {err!r}")

        def ok() -> None:
            self._match("PLC", nname)
            self.log("PLC", f"[CH{self.n}] 완료: {nname} -> {onb} ({self._ms_since(t0):.0f} ms)")

        def done(res) -> None:
            if res.status != Status.OK:
                failed(res)
                return
            if nname == "MS":
                self.e.on_main_shutter(onb)
                self._camera(onb)
            if nname == "SW_RF_SELECT" and onb:
                def rd(r2) -> None:
                    if r2.status == Status.OK:
                        actual = bool(r2.value)
                        self.e.stats.params["use_power_select"] = actual
                        self.log("PLC", f"[CH{self.n}] SW_RF_SELECT 실측 기록(use_power_select={actual})")
                    else:
                        val = self.e.stats.params.get("use_power_select")
                        self.log("PLC", f"[CH{self.n}] SW_RF_SELECT 읽기 실패({r2.error or r2.reason!r}) → "
                                        f"레시피 값 유지(use_power_select={val})")
                    ok()
                self.plc.read_bit("SW_RF_SELECT", on_done=rd)
                return
            ok()
        try:
            if nname == "MV":
                self.plc.write_switch(f"MAIN_{self.n}_GAS_SW", onb, on_done=done)
            elif nname in ("AR", "O2", "N2", "MAIN"):
                self.plc.gas(self.n, nname, on=onb, on_done=done)
            elif nname == "MS":
                self.plc.main_shutter(self.n, open=onb, on_done=done)
            elif nname in ("G1", "G2", "G3"):
                self.plc.write_switch(f"SHUTTER_{int(nname[1])}_SW", onb, on_done=done)
            else:
                self.plc.write_switch(raw, onb, on_done=done)
        except Exception as e:
            failed(e)

    def _camera(self, on: bool) -> None:
        """Main Shutter 기준 카메라(1012-1043) — RF 연속·RF Pulse 공정만. 시작은 이전 촬영 join 이 있을 수 있어 스레드에서."""
        cam = self.sys.shared.camera
        own = f"chamber{self.n}"
        p = self.e.params
        use_rf, use_rfp = bool(p.get("use_rf_power", False)), bool(p.get("use_rf_pulse", False))
        try:
            if on:
                if use_rf or use_rfp:
                    def go():
                        cam.join(own)
                        self.camera_joined = True
                        self.log("CAM", f"[CH{self.n}] 카메라 녹화 시작 (mode={cam.camera.current_mode}, "
                                        f"RF={'RF' if use_rf else ''}{'Pulse' if use_rfp else ''})")
                    self.e.clock.start_thread(go, f"cam-join-{self.ch}")
                else:
                    self.log("CAM", f"[CH{self.n}] RF 미사용 공정 → 카메라 건너뜀")
            else:
                self.camera_leave()
        except Exception as e:
            self.log("CAM", f"카메라 오류: {e!r}")

    def camera_leave(self) -> None:
        if not self.camera_joined:
            return
        self.camera_joined = False
        if self.sys.shared.camera.leave(f"chamber{self.n}"):
            self.log("CAM", f"[CH{self.n}] 카메라 녹화 정지")
        else:
            self.log("CAM", f"[CH{self.n}] 카메라 사용 종료 — 다른 공정이 계속 촬영 중")

    # MFC(cb_mfc 1118-1128) — 결과는 이벤트(owner)로
    def _a_mfc_cmd(self, step) -> None:
        cmd, args = step.params
        gas = str(args.get("gas", "")).upper() if isinstance(args, dict) else ""
        if self.n == 1 and gas == "N2":
            self.log("MFC", "[CH1] N2 요청 무시 (라인 없음)")
            self._match("MFC", cmd)
            return
        self.mfc.command(cmd, dict(args), owner=self.owner)

    # 연속 전원(1130-1158)
    def _dc_dev(self, unit: int):
        return self.dc.get(unit)

    def _a_dc_power_set(self, step) -> None:
        d = self._dc_dev(1)
        if not d:
            self.log("DCpower", "이 챔버는 DC 연속 파워를 지원하지 않습니다.")
            return
        d.start(float(step.value))

    def _a_dc2_power_set(self, step) -> None:
        d = self._dc_dev(2)
        if not d:
            self.log("DCpower2", "이 챔버는 DC 연속 파워 2호기를 지원하지 않습니다.")
            return
        d.start(float(step.value))

    def _a_dc_power_stop(self, step) -> None:
        if self._dc_dev(1):
            self._dc_dev(1).stop()

    def _a_dc2_power_stop(self, step) -> None:
        if self._dc_dev(2):
            self._dc_dev(2).stop()

    def _a_rf_power_set(self, step) -> None:
        if not self.rf:
            self.log("RFpower", "이 챔버는 RF 연속 파워를 지원하지 않습니다.")
            return
        self.rf.start(float(step.value))

    def _a_rf_power_stop(self, step) -> None:
        if self.rf:
            self.rf.stop()

    # DC Pulse(1160-1285)
    def _a_dc_pulse_start(self, step) -> None:
        d = self.dcp
        if not d:
            self.log("DCPulse", "DC-Pulse 미지원 챔버입니다.")
            return
        power = float(step.value)
        freq, off = step.params[0], step.params[1]

        def done(reason, ok) -> None:
            if ok:
                def got(_r, pr) -> None:
                    if pr:
                        pp = self.e.stats.params
                        pp["dc_pulse_freq"] = pr["freq_khz"]
                        pp["dc_pulse_duty"] = pp["dc_pulse_duty_cycle"] = pr["duty_pct"]
                        pp["dc_pulse_off_time_us"] = pr["off_time_us"]
                        pp["dc_pulse_off_time"] = "DC" if pr["off_time_us"] is None else pr["off_time_us"]
                        off_txt = "DC" if pr["off_time_us"] is None else f"{float(pr['off_time_us']):.1f}us"
                        self.log("DCPulse", f"[실측] f={pr['freq_khz']}kHz, off={off_txt}, duty={pr['duty_pct']}%")
                d.read_actual_pulse_params(got)
                return
            why = getattr(d, "last_failure", None) or "prepare_and_start failed"
            r = self.runner
            if r is not None:
                r.step_failed("DCPulse", why, meta={"stage": "prepare_and_start", "ch": self.n, "power": power,
                                                    "freq": freq, "off_time": off, "returned_ok": False})
        d.prepare_and_start(power, freq=freq, off_time_us=off, on_done=done)

    def _a_dc_pulse_set(self, step) -> None:
        d = self.dcp
        if not d:
            self.log("DCPulse", "DC-Pulse 미지원 챔버입니다.")
            return

        def done(reason, ok) -> None:
            if not ok:
                self.log("DCPulse", "set_reference_power returned False (will be handled by event pump)")
        d.set_reference_power(float(step.value), pause_polling=True, on_done=done)

    def _a_dc_pulse_stop(self, step) -> None:
        d = self.dcp
        if d:
            (d.output_off if d.off_needs_confirm() else d.output_off_unused)()

    # RF Pulse(1287-1372)
    def _a_rf_pulse_start(self, step) -> None:
        d = self.rfp
        if not d:
            self.log("RFPulse", "RF-Pulse 미지원 챔버입니다.")
            return

        def done(reason, ok) -> None:
            if ok:
                def got(_r, pr) -> None:
                    if pr:
                        pp = self.e.stats.params
                        pp["rf_pulse_freq"], pp["rf_pulse_duty_cycle"] = pr["freq_khz"], pr["duty_pct"]
                        pp["rf_pulse_off_time_us"] = pr["off_time_us"]
                d.read_actual_pulse_params(got)
        d.start_pulse_process(float(step.value), step.params[0], step.params[1], on_done=done)

    def _a_rf_pulse_set(self, step) -> None:
        d = self.rfp
        if not d:
            self.log("RFPulse", "RF-Pulse 미지원 챔버입니다.")
            return

        def done(reason, ok) -> None:
            if not ok:
                self.log("RFPulse", "set_reference_power returned False (will be handled by event pump)")
        d.set_reference_power(float(step.value), pause_polling=True, on_done=done)

    def _a_rf_pulse_stop(self, step) -> None:
        d = self.rfp
        if d:
            (d.stop_process if d.off_needs_confirm() else d.stop_unused)()

    # IG·RGA·OES(1374-1461)
    def _a_ig_cmd(self, step) -> None:
        iv = int(self.e.cget("IG_POLLING_INTERVAL_MS", 10000))
        self.ig.wait_for_base_pressure(float(step.value), interval_ms=iv,
                                       on_done=lambda *a: self.log("IG", f"wait_for_base_pressure returned: "
                                                                         f"{len(a) > 1 and a[1] is None}"))

    def _a_rga_scan(self, step) -> None:
        tag = f"RGA{self.n}"
        if not self.rga:
            self.log("RGA", "예외로 RGA 스캔 실패: RuntimeError('RGA 어댑터 없음') → 다음 단계")
            self._rga_done()
            return

        def done(res) -> None:
            if not res.ok:
                why = res.message or "RGA failed"
                self.log(tag, f"측정 실패: {why} → 다음 단계")
                fail = res.fail or {}
                for k in ("stderr", "stdout"):
                    t = str(fail.get(k) or "").strip()
                    if t:
                        self.log(tag, f"{k}: {t[-800:]}")
                self.e.chat(f"[{tag}] 측정 실패: {why} → 건너뜀")
            self._rga_done()
        try:
            self.rga.scan(float(self.e.cget("RGA_WORKER_TIMEOUT_S", 60.0)), on_done=done)
        except Exception as e:
            self.log("RGA", f"예외로 RGA 스캔 실패: {e!r} → 다음 단계")
            self._rga_done()

    def _rga_done(self) -> None:
        r = self.runner
        if r is not None:
            r.rga_finished()

    def _a_oes_run(self, step) -> None:
        dur, integ = float(step.params[0]), int(step.params[1])
        o = self.oes

        def fail(e) -> None:
            self.oes_active = False
            self.oes_initialized = False
            self.log("OES", f"OES 실패: {e!r}")
            self.e.chat(f"[OES] 실패: {e!r}")
            r = self.runner
            if r is not None:
                r.oes_failed("OES", e, meta={"stage": "run_measurement", "ch": self.n, "duration_sec": dur,
                                             "integration_ms": integ})
        if o is None:
            fail(RuntimeError("OES 링크 없음"))
            return

        def measured(res) -> None:
            if not self.oes_active:
                return
            self.oes_active = False
            if res.success:
                self.log(f"OES{self.n}", f"{res.message} (csv={res.out_csv})" if res.out_csv else res.message)
                self.log("OES", "OES 측정 종료(정상).")
            elif res.emitted:
                self.oes_initialized = False
                self.log(f"OES{self.n}", f"측정 실패: {res.message} → 종료 절차로 전환")
                r = self.runner
                if r is not None:
                    r.oes_failed("OES", res.message, meta={"success": False, "csv": res.out_csv, "ch": self.n})

        def start() -> None:
            self.oes_active = True
            o.measure(dur, integ, on_done=measured)

        if self.oes_initialized:
            start()
            return
        t = float(self.e.cget("CHAMBER_OES_INIT_TIMEOUT_S", 30.0))

        def inited(ok, err) -> None:
            if not ok:
                fail(RuntimeError(f"OES 초기화 실패: {err or 'unknown'}"))
                return
            self.oes_initialized = True
            start()
        o.initialize(timeout_s=t, force=False, on_done=inited)

    def oes_stop(self) -> bool:
        if self.oes is not None and self.oes_active:
            self.oes.stop(wait=False)
            return True
        return False

    # CH2 히터(1464-1556) — heater_link 가 판정·문구를 가진다
    def _a_heater_set(self, step) -> None:
        h = self.heater
        if h is None:
            self._fail("HEATER", "히터 기능이 꺼져 있습니다(SUPPORTS_HEATER=false 또는 히터 초기화 실패)", code="E830")
            return
        h.heater_set(float(step.value), step.params[0] if step.params else None, context=self.e.heater_context())

    def _a_heater_wait(self, step) -> None:
        h = self.heater
        if h is None:
            self._fail("HEATER", "히터 서비스 없음", code="E830")
            return
        soak = step.params[0] if step.params else None
        h.heater_wait(float(step.value), None if soak is None else float(soak))

    def _a_heater_hold(self, step) -> None:
        if self.heater is None:
            self._fail("HEATER", "히터 서비스 없음", code="E830")
            return
        self.heater.heater_hold()

    def _heater_result(self, kind: str, ok: bool, msg: str, code: Optional[str]) -> None:
        tok = {"set_ok": "HEATER_SET", "ready": "HEATER_READY", "hold_ok": "HEATER_HOLD"}.get(kind)
        if tok:
            self._match(tok)
        elif kind == "failed":
            self._fail("HEATER", msg, code=code)

    # ═══════════════════════ 취소(v1 _cancel_ig·_cancel_heater·_cancel_thickness) ═══════════════════════
    def cancel_waits(self, final: bool = False) -> None:
        if not final:
            try:
                self.ig.cancel_wait()
            except Exception:
                pass
        if self.heater is not None:
            try:
                self.heater.heater_cancel()
            except Exception:
                pass
        if self.thickness is not None:
            try:
                self.thickness.abort()
            except Exception:
                pass

    # ═══════════════════════ 폴링 ═══════════════════════
    def polling_targets(self, active: bool, params: Dict[str, Any]) -> Dict[str, bool]:
        if not active:
            return {"mfc": False, "dc": False, "dc2": False, "rf": False, "dc_pulse": False, "rf_pulse": False}
        info = common_info(params or {}, self.e.caps)
        return {"mfc": True, "dc_pulse": bool(info["use_dc_pulse"]), "rf_pulse": bool(info["use_rf_pulse"]),
                "dc": bool(info["use_dc"]), "dc2": bool(info["use_dc2"]), "rf": bool(info["use_rf"])}

    def apply_polling(self, active: bool, targets: Dict[str, bool]) -> None:
        """v1 'polling' 이벤트 처리(2141-2185) + _apply_polling_targets(6519-6579)."""
        self.e.polling_active = bool(active)
        p = self.e.params
        cap = self.e.caps
        base = {"mfc": active, "dc": active and cap.dc and bool(p.get("use_dc_power")),
                "dc2": active and cap.dc2 and bool(p.get("use_dc_power2")),
                "rf": active and cap.rf and bool(p.get("use_rf_power"))}
        t = {k: bool(base[k] and targets.get(k, False)) for k in base}
        mp = self.sys.shared.mfc[self.ch]
        if t["mfc"]:
            mp.acquire(self.owner)
            self.mfc_polling = True
        elif self.mfc_polling or self.owner in mp.users:
            mp.release(self.owner)
            self.mfc_polling = False
        for unit, k in ((1, "dc"), (2, "dc2")):
            if self.dc.get(unit):
                self.dc[unit].set_process_status(t[k])
        if self.rf:
            self.rf.set_process_status(t["rf"])
        if self.thickness is not None:
            try:
                self.thickness.set_main_polling(bool(active))
            except Exception:
                pass
        if self.heater is not None:
            try:
                self.heater.set_process_flags(True, bool(active), True)
            except Exception:
                pass

    # ═══════════════════════ 장비 이벤트(v1 펌프) ═══════════════════════
    def on_event(self, lg) -> None:
        src = lg.source
        if src not in self.sources:
            return
        ev = lg.event
        k = lg.kind
        try:
            if src.endswith(".mfc"):
                self._ev_mfc(k, ev)
            elif src.endswith(".ig"):
                self._ev_ig(k, ev)
            elif src == "ch2.dc":
                self._ev_dc(k, ev)
            elif src == "ch2.rf":
                self._ev_rf(k, ev)
            elif src.endswith(".dcpulse"):
                self._ev_dcp(k, ev)
            elif src.endswith(".rfpulse"):
                self._ev_rfp(k, ev)
        except Exception as e:
            self.log(f"MAIN{self.n}", f"장비 이벤트 처리 예외: {e!r} (src={src}, kind={k})")

    def _busy(self) -> bool:
        return bool(self.e.busy)

    def _ev_mfc(self, k: str, ev) -> None:
        if k in ("command_confirmed", "command_failed"):
            ow = getattr(ev, "owner", None)
            if ow is not None and ow != self.owner:
                return
        if k == "status":
            self.log(f"MFC{self.n}", ev.detail or "")
        elif k == "command_confirmed":
            self._match("MFC", ev.cmd or "")
        elif k == "command_failed":
            r = self.runner
            if r is not None:
                r.mfc_failed(ev.cmd or "", ev.detail or ev.reason or "unknown", code=getattr(ev, "code", None),
                             meta={"cmd": ev.cmd, "gas": ev.gas, "kind": k, "ch": self.n})
        elif k == "flow":
            gas, flow = ev.gas or "", float(ev.value or 0.0)
            self.e.stats.add_mfc_flow(gas, flow)
            self.log(f"MFC{self.n}", f"[poll] {gas}: {flow:.2f} sccm")
        elif k == "pressure":
            txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
            if self.mfc_polling:
                self.e.stats.add_mfc_pressure(txt)
            self.log(f"MFC{self.n}", f"[poll] ChamberP: {txt}")

    def _ev_ig(self, k: str, ev) -> None:
        if k in ("status", "pressure") and self.e.ig_in_use_by_pc():
            return
        if k == "status":
            self.log(f"IG{self.n}", ev.detail or "")
        elif k == "pressure":
            if ev.pressure is not None:
                self.e.stats.add_ig(float(ev.pressure))
        elif k == "base_reached":
            self._match("IG_OK")
        elif k == "base_failed":
            self._fail("IG", ev.detail or "unknown", code=getattr(ev, "code", None),
                       meta={"kind": k, "pressure": ev.pressure, "ch": self.n})

    def _warn_off(self, unit: str, label: str) -> None:
        alert = (f"⚠️ CH{self.n} {unit} Power OFF 실패 - 출력 상태 미확인 "
                 f"(켜져 있을 가능성 있음). 전원 장비 출력과 PLC D/A(SET/WRITE)를 즉시 확인하세요.")
        self.log(f"{unit}{self.n}" if unit == "RF" else unit, alert)
        self.e.run_warnings.append(f"{unit} Power OFF 실패(출력 상태 미확인)")
        self.e.chat(alert)

    def _ev_dc(self, k: str, ev) -> None:
        d = ev.data or {}
        unit = 2 if str(d.get("unit", "DC1")).upper().endswith("2") else 1
        lab, src = ("DC1", "DC Power") if unit == 1 else ("DC2", "DC2 Power")
        if k == "status":
            self.log(lab, ev.detail or "")
        elif k == "display":
            p, v, i = (float(d.get(x) or 0.0) for x in ("power", "voltage", "current"))
            (self.e.stats.add_dc if unit == 1 else self.e.stats.add_dc2)(p, v, i)
            self.log(lab, f"측정: {p:.1f} W, {v:.1f} V, {i:.3f} A")
        elif k == "target_reached":
            self._match("DC_TARGET" if unit == 1 else "DC2_TARGET")
        elif k == "target_failed":
            self.dc_failed[unit] = True
            msg = str(ev.detail or "")
            u = "DC Power" if unit == 1 else "DC2"
            head = (f"⚠️ CH{self.n} {u} 과전류 차단 → 전체 공정 중단" if ("과전류" in msg or "overcurrent" in msg.lower())
                    else f"CH{self.n} {u} 출력 이상 → 전체 공정 중단")
            self.e.chat(f"{head} | {msg}")
            self._fail(src, ev.detail or "low-power", code=getattr(ev, "code", None),
                       meta={"kind": k, "power": d.get("power"), "voltage": d.get("voltage"),
                             "current": d.get("current"), "ch": self.n, **({"unit": 2} if unit == 2 else {})})
        elif k == "power_off_failed":
            self._warn_off(lab if unit == 2 else "DC1", lab)
            self.dc_failed[unit] = False
            r = self.runner
            if r is not None:
                r.power_off_failed(src, _OFF_TOKEN[src], ev.detail or f"{lab} Power OFF 미확인",
                                   code=getattr(ev, "code", None),
                                   meta={"kind": k, "ch": self.n, "safety": "output_state_unconfirmed"})
        elif k == "power_off_finished":
            if not self.dc_failed[unit]:
                self._match("DC_OFF" if unit == 1 else "DC2_OFF")
            else:
                self.dc_failed[unit] = False

    def _ev_rf(self, k: str, ev) -> None:
        d = ev.data or {}
        if d.get("mode") not in (None, "ch2"):
            return
        if k == "status":
            self.log(f"RF{self.n}", ev.detail or "")
        elif k == "display":
            fwd, ref = float(d.get("forward") or 0.0), float(d.get("reflected") or 0.0)
            self.e.stats.add_rf(fwd, ref)
            self.log(f"RF{self.n}", f"[poll] fwd={fwd:.1f}W, ref={ref:.1f}W")
        elif k == "target_reached":
            self._match("RF_TARGET")
        elif k == "target_failed":
            self._fail("RF Power", ev.detail or "unknown", code=getattr(ev, "code", None),
                       meta={"kind": k, "forward": d.get("forward"), "reflected": d.get("reflected"), "ch": self.n})
        elif k == "power_off_failed":
            self._warn_off("RF", "RF")
            r = self.runner
            if r is not None:
                r.power_off_failed("RF Power", "RF_OFF", ev.detail or "RF Power OFF 미확인",
                                   code=getattr(ev, "code", None),
                                   meta={"kind": k, "ch": self.n, "safety": "output_state_unconfirmed"})
        elif k == "power_off_finished":
            self._match("RF_OFF")

    def _ev_rfp(self, k: str, ev) -> None:
        if k == "status":
            self.log(f"RFPulse{self.n}", ev.detail or "")
        elif k == "power":
            d = ev.data or {}
            self.e.stats.add_rfpulse(float(d.get("forward") or 0.0), float(d.get("reflected") or 0.0))
        elif k == "target_reached":
            self._match("RF_TARGET")
        elif k == "command_failed":
            cmd = str(ev.cmd or "")
            meta = {"cmd": ev.cmd, "kind": k, "ch": self.n}
            r = self.runner
            if cmd == "RF_OFF":
                alert = (f"⚠️ CH{self.n} RF Pulse OFF 실패 - 출력 상태 미확인 "
                         f"(켜져 있을 가능성 있음). RF Pulse 장비의 출력 상태를 즉시 확인하세요.")
                self.log(f"RFPulse{self.n}", alert)
                self.e.run_warnings.append("RF Pulse OFF 실패(출력 상태 미확인)")
                self.e.chat(alert)
                meta["safety"] = "output_state_unconfirmed"
                if r is not None:
                    r.power_off_failed("RFPulse", "RFPULSE_OFF", ev.detail or ev.reason or "unknown", meta=meta)
            elif r is not None:
                r.step_failed("RFPulse", ev.detail or ev.reason or "unknown", code=getattr(ev, "code", None), meta=meta)
        elif k == "power_off_finished":
            self._match("RFPULSE_OFF")

    def _ev_dcp(self, k: str, ev) -> None:
        tag = f"DCPulse{self.n}"
        if k == "status":
            self.log(tag, ev.detail or "")
        elif k == "telemetry":
            d = ev.data or {}
            p, v, i = (float(d.get(x) or 0.0) for x in ("P_W", "V_V", "I_A"))
            self.e.stats.add_dcpulse(p, v, i)
            raw = d.get("raw") or {}
            raw_s = f" (raw P/I/V={raw.get('P')}/{raw.get('I')}/{raw.get('V')})" if raw else ""
            self.log(tag, f"[telemetry] P={p:.1f} W, V={v:.2f} V, I={i:.3f} A{raw_s}")
        elif k == "arc_threshold_reached":
            self.log(tag, ev.detail or "")
        elif k == "command_confirmed":
            cmd = str(ev.cmd or "").upper()
            if cmd.startswith("OUTPUT_ON"):
                self._match("DC_PULSE_TARGET")
            elif cmd.startswith("REF_POWER"):
                self._match("DC_PULSE_SET")
            elif cmd.startswith("OUTPUT_OFF"):
                self._match("DCPULSE_OFF")
        elif k == "command_failed":
            why_raw = ev.detail or ev.reason or "unknown"
            why, cmd = str(why_raw).lower(), str(ev.cmd or "").upper()
            self.log(tag, f"CMD FAIL: {cmd} ({why_raw})")
            r = self.runner
            if cmd.startswith("OUTPUT_OFF"):
                alert = (f"⚠️ CH{self.n} DC Pulse OUTPUT OFF 실패 - 출력 상태 미확인 "
                         f"(켜져 있을 가능성 있음). 장비 패널의 HV/OUTPUT 상태를 즉시 확인하세요.")
                self.log(tag, alert)
                self.e.run_warnings.append("DC Pulse OUTPUT OFF 실패(출력 상태 미확인)")
                self.e.chat(alert)
                if r is not None:
                    r.power_off_failed("DCPulse", "DCPULSE_OFF", "OUTPUT_OFF 미확인(출력 상태 미확인)",
                                       meta={"cmd": cmd, "kind": k, "ch": self.n,
                                             "safety": "output_state_unconfirmed"})
                return
            if (ev.data or {}).get("phase") == "prepare" or cmd.startswith("READ_") or cmd == "FAULT_RESET":
                return
            if cmd == "AUTO_STOP" or "target_failed" in why:
                head = (f"⚠️ CH{self.n} DC Pulse 과전류 차단 → 전체 공정 중단" if "overcurrent" in why else
                        f"CH{self.n} DC Pulse 저전류 감지 → 전체 공정 중단" if "low_current" in why else
                        f"CH{self.n} DC Pulse 세트포인트 이탈(연속) 또는 P=0W 감지 → 전체 공정 중단")
                self.e.chat(f"{head} | {why_raw}")
            if r is not None:
                r.step_failed("DCPulse", why_raw, code=getattr(ev, "code", None),
                              meta={"cmd": cmd, "kind": k, "ch": self.n})

    # ═══════════════════════ 공정 뒤 장비 정리(_stop_device_watchdogs 5308-5535 의 v2 몫) ═══════════════════════
    def cleanup_devices(self) -> None:
        """MFC(공유 보호)·전원 폴링 OFF → IG 대기 취소(같은 CH PC 사용 중이면 생략) → OES 정지 요청(기다리지 않음) →
        펄스 OFF 안전망(cleanup — 끝날 때까지 최대 DCP_CLEANUP_OFF_WAIT_S) → 펄스 포트 해제 → 카메라 떠남.
        v2 는 MFC·IG 연결을 끊지 않는다(1초 읽기 — 상시 연결 D37) — 대기열·상태만 정리."""
        mp = self.sys.shared.mfc[self.ch]
        others = mp.users - {self.owner}
        if others:
            self.log("MFC", f"다른 공정이 MFC 사용 중({', '.join(sorted(others))}) → mfc 폴링/상태 리셋 생략(공유 자원 보호), "
                            f"이 챔버 몫만 정리")
            self.mfc.release_owner(self.owner, "cleanup")
        else:
            self.mfc.on_process_cleanup()
        if self.mfc_polling or self.owner in mp.users:
            mp.release(self.owner)
            self.mfc_polling = False
        for d in (self.dc.get(1), self.dc.get(2), self.rf):
            if d is not None:
                d.set_process_status(False)
        if self.e.ig_in_use_by_pc():
            self.log("IG", "같은 CH Plasma Cleaning 이 IG 사용 중 → IG 대기 취소/정리 생략 (공유 장치 보호)")
        else:
            try:
                self.ig.cancel_wait()
                self.ig.set_process_status(False)
            except Exception:
                pass
        if self.oes is not None:
            try:
                self.oes.stop(wait=False)
            except Exception:
                pass
            if not bool(getattr(self.oes, "daemon_enabled", False)):
                self.oes_initialized = False
        self.oes_active = False
        pending = []
        for d, nm in ((self.dcp, "DC Pulse"), (self.rfp, "RF Pulse")):
            if d is not None and d.worker is not None:
                box = {"done": False, "unconf": False}

                def fin(unconf, b=box) -> None:
                    b["done"], b["unconf"] = True, bool(unconf)
                    self.e.clock.notify()
                d.cleanup(on_done=fin)
                pending.append((nm, box))
        if pending:
            ok = self.e.clock.wait_until(self.e.clock.monotonic() + 15.0, lambda: all(b["done"] for _, b in pending))
            if not ok:
                self.log("MAIN", f"⚠ device cleanup timeout: {[nm for nm, b in pending if not b['done']]!r}")
            for nm, b in pending:
                if b["unconf"]:
                    w = f"⚠ 정리 중 {nm} 출력 OFF 미확인 — 장비 출력 상태 확인 필요"
                    self.log("MAIN", w)
                    self.e.chat(w)
        self.sys.shared.pulse.release_chamber(self.ch)
        self.camera_leave()
