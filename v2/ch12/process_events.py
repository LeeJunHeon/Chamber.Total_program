"""챔버12 공정 장비 이벤트 — v1 runtime/chamber_runtime.py 의 장비 이벤트 펌프(_pump_mfc/ig/dc/dc2/rf/rfpulse/dcpulse_events
2195-2776)를 v2 드라이버 이벤트(system.event_listeners 의 Logged)로. ProcessActions 가 이 믹스인을 쓴다.

- MFC: owner 가 다른 확인·실패는 넘기지 않음(공유 MFC — owner 없는 이벤트는 넘김), flow·pressure 표본(RunStats — pressure 는 메인
  공정 폴링 구간만) + '[poll] {gas}: x.xx sccm'·'[poll] ChamberP: …'.
- IG: 같은 CH 플라즈마 클리닝이 IG 를 쓰는 동안 status·pressure 는 기록하지 않음, base_reached → IG_OK, base_failed → 실패.
- DC·DC2(PLC D/A): target_failed 뒤 첫 power_off_finished 는 OFF 확인으로 넘기지 않음(1회 플래그), power_off_failed 는 OFF 실패
  경로(종료 절차 중이면 그 단계만)·경고('⚠️ CHn … Power OFF 실패 - 출력 상태 미확인 …'), 측정 줄 '측정: P W, V V, I A'.
- RF 연속: '[poll] fwd=x.xW, ref=x.xW', target_reached → RF_TARGET.
- DC Pulse: command_confirmed OUTPUT_ON → DC_PULSE_TARGET, REF_POWER → DC_PULSE_SET, OUTPUT_OFF → DCPULSE_OFF;
  command_failed OUTPUT_OFF → OFF 실패 경로(phase 검사보다 먼저), phase=prepare·READ_*·FAULT_RESET 무시, AUTO_STOP 은 사유 문구.
  '[telemetry] P=… W, V=… V, I=… A (raw P/I/V=…)'(GAPB-006).
- RF Pulse: command_failed RF_OFF → OFF 실패 경로, 그 밖은 실패, power_off_finished → RFPULSE_OFF.
"""
from __future__ import annotations

_OFF_TOKEN = {"DC Power": "DC_OFF", "DC2 Power": "DC2_OFF", "RF Power": "RF_OFF"}


class ProcessEventsMixin:
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
