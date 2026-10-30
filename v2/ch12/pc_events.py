"""플라즈마 클리닝 — 장비 이벤트 받기(v1 이벤트 펌프 268-431)·카메라 연동(1049-1065).

v1 은 프리플라이트에서 RF·MFC·IG 이벤트 펌프를 띄우고(_preflight_connect 512-578) 종료 정리(_final_cleanup D)에서 내렸다.
v2 는 조립의 이벤트 구독(system.event_listeners) 하나로 드라이버 이벤트(Logged.event)를 받아 '펌프가 떠 있는 동안'만 처리한다.
- MFC: owner 가 있고 PC 가 아닌 명령 결과는 무시(공유 MFC — 257-264). status → 로그, confirmed 'OK: cmd' + 컨트롤러,
  failed 'FAIL: cmd (사유)' + 컨트롤러(실패·중단 신호). flow '[poll] …'(화면·파일에 안 남음)·공정 타이머 중 gas MFC 의 선택 가스만 수집,
  pressure '[poll] ChamberP: …'·공정 타이머 중 pressure MFC 것만 수집(CH2 PC 평균 반토막 대책 — 297-330).
- IG(선택 챔버): status·'[poll] {p:.3e} Torr'(실행 중 전부 수집 — base_pressure = 마지막 값)·base_reached·base_failed.
- RF: display → FWD/REF 칸·'FWD=…, REF=… (W)'·공정 타이머 중 수집, status, target_reached/failed(실패 → 강제 실패·컨트롤러 FAIL),
  power_off_finished. 'log'(쓰기 뒤 되읽기 'RF READ FWD=…')는 v1 _rf_send 처럼 출처 'PLC'.
- 카메라: RF SET ON(state_changed running) 때 'pc{ch}' 로 참여(시작 때 owner 고정 — TEST-008), RF SET OFF 가 성공했을 때 같은 owner
  로 해제(남은 참여자가 있으면 '카메라 사용 종료 — 다른 공정이 계속 촬영 중'). [이상] v1 처럼 강제 SET OFF(정지 정리)는 카메라를
  풀지 않는다(_force_rf_zero_then_set_off 는 toggle_enable 을 거치지 않음).
"""
from __future__ import annotations

from v2.ch12.drivers import mfc as M


class EventIO:
    # ───────── 이벤트(펌프 — 268-431) ─────────
    def pumps_on_now(self) -> None:
        g, pr = self.dev.mfc_gas, self.dev.mfc_pressure
        sel = f"CH{self.ch}"
        self.pumps = {}
        if g is not None and g is pr:
            self.pumps[g.block] = f"MFC(SP4/GAS-{sel})"
        else:
            if g is not None:
                self.pumps[g.block] = "MFC(GAS)"
            if pr is not None:
                self.pumps[pr.block] = f"MFC(SP4-{sel})"
        self.ig_pump = (self.dev.ig.block, f"IG-{sel}") if self.dev.ig is not None else None
        self.rf_pump = self.dev.rf is not None

    def pumps_off(self) -> None:
        self.pumps = {}
        self.ig_pump = None
        self.rf_pump = False

    rf_pump = False

    def on_system_event(self, e) -> None:
        """system.event_listeners 에 건다 — 드라이버 이벤트(Logged.event)."""
        src, ev = getattr(e, "source", ""), getattr(e, "event", None)
        if ev is None or src == "pc":
            return
        try:
            if src == "pc.rf":
                self._on_rf(ev)
            elif src in self.pumps:
                self._on_mfc(src, ev)
            elif self.ig_pump is not None and src == self.ig_pump[0]:
                self._on_ig(ev)
        except Exception as ex:
            self.log("PC", f"이벤트 펌프 오류: {ex!r}")

    def _on_mfc(self, src: str, ev) -> None:
        label = self.pumps.get(src)
        if label is None:
            return
        k = getattr(ev, "kind", None)
        if k in ("command_confirmed", "command_failed"):
            ow = getattr(ev, "owner", None)
            if ow is not None and ow != self.mfc_owner():
                return
        if k in ("status", "io"):          # v1 은 [SEND]·[RECV] 도 status(device/mfc.py:1359·1440, ig.py:702·757)
            self.log(label, ev.detail or "")
        elif k == "command_confirmed":
            self.log(label, f"OK: {ev.cmd or ''}")
            self.pc.on_mfc_confirmed(ev.cmd or "")
        elif k == "command_failed":
            self.log(label, f"FAIL: {ev.cmd or ''} ({ev.detail or 'unknown'})")
            self.pc.on_mfc_failed(ev.cmd or "", ev.detail or "unknown", code=ev.code,
                                  meta={"kind": k, "gas": ev.gas, "label": label, "ch": self.ch})
        elif k == "flow":
            gas, flow = ev.gas or "", float(ev.value or 0.0)
            self.log(label, f"[poll] {gas}: {flow:.2f} sccm")
            g = self.dev.mfc_gas
            if self.process_timer_active and g is not None and src == g.block:
                sel = M.GAS_MAP.get(int(self.gas_idx or 3), "N2")
                if not sel or gas == sel:
                    self.readings["flow"].append(flow)
        elif k == "pressure":
            txt = ev.text or (f"{ev.value:.3g}" if ev.value is not None else "")
            self.log(label, f"[poll] ChamberP: {txt}")
            pr = self.dev.mfc_pressure
            if self.process_timer_active and isinstance(ev.value, (int, float)) and pr is not None and src == pr.block:
                self.readings["pressure"].append(float(ev.value))

    def _on_ig(self, ev) -> None:
        label = self.ig_pump[1]
        k = getattr(ev, "kind", None)
        if k in ("status", "io"):          # v1 은 [SEND]·[RECV] 도 status(device/mfc.py:1359·1440, ig.py:702·757)
            self.log(label, ev.detail or "")
        elif k == "pressure":
            pv = ev.pressure
            self.log(label, f"[poll] {pv:.3e} Torr" if isinstance(pv, (int, float)) else f"[poll] {ev.detail or ''}")
            if isinstance(pv, (int, float)) and self.running:
                self.readings["ig"].append(float(pv))
        elif k == "base_reached":
            self.log(label, "Base pressure reached")
        elif k == "base_failed":
            self.log(label, f"Base pressure failed: {ev.detail or ''}")

    def _on_rf(self, ev) -> None:
        k = getattr(ev, "kind", None)
        data = getattr(ev, "data", {}) or {}
        if k == "state_changed" and data.get("running") is True:
            self._camera_join()
        if k == "status" and ev.detail == "RF SET OFF":
            self._camera_leave()
        if not self.rf_pump:
            return
        if k == "display":
            fwd, ref = float(data.get("forward") or 0.0), float(data.get("reflected") or 0.0)
            self.ui_fwd, self.ui_ref = f"{fwd:.1f}", f"{ref:.1f}"
            self.log("RF", f"FWD={fwd:.1f}, REF={ref:.1f} (W)")
            if self.process_timer_active:
                self.readings["forp"].append(fwd)
                self.readings["refp"].append(ref)
        elif k == "status":
            self.log("RF", ev.detail or "")
        elif k == "log":
            self.log("PLC", ev.detail or "")
        elif k == "target_reached":
            self.log("RF", "목표 파워 도달")
            with self.clock.cond:
                self.rf_target_ok, self.rf_target_reason, self.rf_target_evt = True, "", True
                self.clock.cond.notify_all()
        elif k == "target_failed":
            reason = ev.detail or "RF 목표 파워 실패 (REF.P 과다 또는 저출력)"
            with self.clock.cond:                    # 기다리는 RF 시작이 '목표 실패'를 보고 깨도록 한 묶음으로(v1 이벤트 순서)
                self.log("RF", f"목표 파워 실패: {reason}")
                self.rf_target_ok, self.rf_target_reason = False, reason
                self.forced_fail, self.forced_fail_reason = True, reason
                self.pc.on_rf_failed(reason, code=ev.code, meta={"kind": k, "forward": self.dev.rf.forward_w,
                                                                  "reflected": self.dev.rf.reflected_w, "ch": self.ch})
                self.log("PC", "RF 실패 감지 → PC 컨트롤러에 FAIL 전달")
                self.rf_target_evt = True
                self.clock.cond.notify_all()
        elif k == "power_off_finished":
            self.log("RF", "Power OFF finished")

    # ───────── 카메라(1049-1065) ─────────
    def _camera_join(self) -> None:
        share, cam = self.dev.camera_share, self.dev.camera
        if share is None:
            return
        owner = self.cam_owner_active = f"pc{int(self.ch)}"

        def work():
            try:
                if cam is not None:
                    cam.set_log_callback(lambda m: self.log("CAM", m), owner=owner)
                share.join(owner)
                self.log("CAM", f"카메라 녹화 시작 (mode={cam.current_mode if cam is not None else '-'})")
            except Exception:
                pass
        self.clock.start_thread(work, "pc-camera-join")

    def _camera_leave(self) -> None:
        share = self.dev.camera_share
        if share is None:
            return
        owner = self.cam_owner_active or f"pc{int(self.ch)}"
        try:
            ok = share.leave(owner)
            self.cam_owner_active = ""
            if not ok:
                self.log("CAM", "카메라 사용 종료 — 다른 공정이 계속 촬영 중")
        except Exception:
            pass

