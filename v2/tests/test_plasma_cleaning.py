"""플라즈마 클리닝 엔진(v2/ch12/plasma_cleaning.py) — 가상 시계 + Ch12Rig(시뮬레이터 전부 + 실제 드라이버) 실제 실행.

기대값 근거는 v1 코드(파일:줄)와 v1 시험(tests/test_pc_*.py — v1 을 import 하지 않고 기대값만 옮김):
- 정상 흐름 CH1·CH2(장비 명령 순서·GV 유지·RF 0 W·SET OFF·MFC 정리·공정 로그·엑셀 시트·카드), 쿨다운, 채널 고정
- GV 인터락 FALSE → E302·읽기 실패 → E511(tests/test_pc_gv_interlock_code.py:93-135) — 둘 다 _final_cleanup 장비 명령(PC-096)
- RF 미도달(시간 초과 분류·'timeout 60s' 문구·목표 실패 이벤트), RF 0 W 미확인(tests/test_pc_rf_stop.py:126-274 — 알림 1회·SET OFF 안 함·실패)
- STOP(PC-052·067·100), 호스트 응답 코드(E205~E209·E227·E302·E700·E511 — host/handlers.py:950-1001), TEST 모드(PC-014~017·093·094)
"""
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import plasma_cleaning as PCM
from v2.ch12.drivers import plc_map as PM
from v2.ch12.pc_recipe import HostStartError
from v2.ch12.sim.rig import Ch12Rig

C = PM.COIL_MAP
UI = {"gas_flow": "10", "rf_power": "100", "process_time": "0.1", "working_pressure": "2"}


def make(tmp_path, cfg=None, hook=None):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    box = {}

    def h(t):
        if hook is not None and "eng" in box:
            hook(box["r"], box["eng"], t)
    r = Ch12Rig(c, tmp_path, cfg=cfg, scenario_hook=h)
    eng = PCM.attach(r)
    box.update(r=r, eng=eng)
    r.rows = []
    r.row_listeners.append(r.rows.append)
    assert r.start()
    return r, eng


def ticks(r, pred, limit=900):
    n = 0
    while not pred() and n < limit:
        r.loop.run(1)
        n += 1
    assert pred(), f"{limit} tick 안에 조건이 안 됨"
    return n


def run_to_end(r, eng, limit=900):
    ticks(r, lambda: eng.last_final is not None and not eng.running, limit)
    r.loop.run(1)


def logs(eng):
    return [ln.split("] ", 1)[1] for ln in eng.ui_log]


def cards(eng, api=None):
    return [e.data for e in eng.events if e.kind == "card" and (api is None or e.data.get("api") == api)]


def popups(eng):
    return [(e.data["kind"], e.data["title"], e.data["text"]) for e in eng.events if e.kind == "popup"]


def host(r, eng, recipe, **kw):
    box = eng.host_start(recipe, **kw)
    ticks(r, lambda: box.done, 200)
    return box.value


def sp4_override_hook(ch_name="ch2", torr=0.002):
    """CH2 PC: 가스는 MFC1(CH1 챔버 모델)로 들어가 CH2 모델 압력이 안 오른다 — SP4 대기 단계부터 CH2 압력을 고정(시험 대본)."""
    def hook(r, eng, t):
        if eng.pc.step in ("gas_select", "gas_flow", "sp4_set", "sp4_wait") and r.chambers[ch_name].pressure_override is None:
            r.chambers[ch_name].pressure_override = torr
    return hook


# ───────────────────────── 정상 흐름 ─────────────────────────
def test_normal_flow_ch1(tmp_path):
    r, eng = make(tmp_path)
    try:
        pre = []
        res = eng.start(params=UI, source="web", by="op", on_preflight=lambda *a: pre.append(a))
        assert res.accepted and r.run_state.is_running("pc", 1)
        ticks(r, lambda: eng.pc.step == "process")
        r.loop.run(2)
        row = r.rows[-1]
        assert row["pc.proc.state"] == "run" and row["pc.proc.block_type"] == "process" and row["pc.proc.owner"] == "web"
        assert row["pc.set.rf_w"] == 100.0 and row["pc.set.n2_sccm"] == 10.0 and row["pc.set.pressure_mtorr"] == 2.0
        assert row["pc.set.time_s"] == 6.0 and row["pc.proc.remain_s"] is not None and row["pc.proc.block_no"] == 10
        assert r.shared.camera.users == {"pc1"}                         # RF SET ON 때 참여(1049-1065)
        assert r.sims["plc"].coils[C["DCV_SET_1"]] is True
        run_to_end(r, eng)
        assert pre == [(True, "preflight OK", None)]
        assert eng.last_final == {"ok": True, "stopped": False, "reason": None, "ch": 1, "status": "success"}
        L = logs(eng)
        order = ["프리플라이트: 장비 연결 확인/시작", "GV 인터락 확인(프리플라이트)", "파일 로그 시작", "GV 인터락=True",
                 "GV OPEN_LAMP=True", "Base pressure reached", "GasFlow → 192.168.1.50:4003 ch3", "OK: FLOW_ON",
                 "SP4 작업압 안정화 완료", "RF Power 적용: 100.0 W", "목표 파워 안정 → 프로세스 타이머 시작 가능", "RF START OK",
                 "플라즈마 클리닝 종료", "Final notify ok=True, stopped=False, lr='success', reason=None", "[CLEANUP] begin",
                 "종료: MFC GAS OFF OK", "종료: MFC(SP4) VALVE OPEN OK", "종료: GateValve CH1 유지(OPEN)", "[CLEANUP] end",
                 "[FINALLY] idle UI 복구 완료"]
        idx = [next(i for i, ln in enumerate(L) if ln.endswith(o)) for o in order]
        assert idx == sorted(idx), [L[i] for i in idx]
        assert not any("[poll]" in ln for ln in L)                       # PC-037
        # 장비: MFC1 'Q3 10.0'·'L31'·'S4 0.2'·'D4'·'L30'·'O', 게이트는 열린 채(PC-077), RF D/A 0·SET OFF
        assert r.sims["ch1.mfc"].set_commands_seen()[-6:] == ["Q3 10.0", "L31", "S4 0.2", "D4", "L30", "O"]
        assert r.chambers["ch1"].valves["gate"] is True
        assert r.sims["plc"].regs[PM.REG_MAP["DCV_WRITE_1"]] == 0 and r.sims["plc"].coils[C["DCV_SET_1"]] is False
        assert r.shared.camera.users == set()
        m1 = r.mfc["ch1"]
        assert m1.poll_users == set() and not m1.polling and (m1.poll_gas, m1.poll_pressure) == (True, True)
        # 카운트다운 total+1 초(PC-052): 'RF START OK' 부터 '종료 시퀀스 진입' 까지 7 s
        t = {ln.split("] ", 1)[1]: ln[1:9] for ln in eng.ui_log}
        h1, h2 = t["RF: RF START OK"], t["STEP: 종료 시퀀스 진입"]
        sec = lambda s: int(s[:2]) * 3600 + int(s[3:5]) * 60 + int(s[6:8])   # noqa: E731
        assert sec(h2) - sec(h1) == 7
        # 공정 로그 PC_*.log 머리·본문(2591-2602), 닫은 뒤의 줄은 SYSTEM 일자 로그
        eng.run_log.flush()
        p = eng.run_log.path
        assert p.parent == Path(r.cfg["LOG_ROOT_DIR"]) / "Plasma_Cleaning" and p.name.startswith("PC_") and p.name.endswith("_CH1.log")
        text = p.read_text(encoding="utf-8").splitlines()
        assert text[0] == "# ==== Plasma Cleaning Run ====" and text[2] == "# chamber = CH1"
        assert text[3] == "# mfc_gas = 192.168.1.50:4003" and text[4] == "# mfc_pressure = 192.168.1.50:4003"
        assert text[5] == ("# params: gas_idx=3, flow=10.0 sccm, IG_target=5.000e-06 Torr, SP4=2.00 mTorr, RF=100.0 W, "
                           "time=0.10 min")
        assert text[6] == "# ============================" and text[7].endswith("PC: 파일 로그 시작")
        assert any(ln.endswith("STEP: 종료: GateValve CH1 유지(OPEN)") for ln in text)
        assert not any("[CLEANUP] end" in ln for ln in text)
        # 카드: 시작·종료(성공 — 실패 이유 텍스트 없음)
        st = cards(eng, "notify_process_started")[0]["payload"]
        assert st == {"process_note": "Plasma Cleaning", "process_time": 0.1, "use_rf_power": True, "rf_power": 100.0,
                      "prefix": "CH1 Plasma Cleaning", "ch": 1}
        fin = cards(eng, "notify_process_finished_detail")[0]
        assert fin["ok"] is True and fin["payload"] == {"process_name": "Plasma Cleaning", "prefix": "CH1 Plasma Cleaning",
                                                       "ch": 1, "status": "success", "stopped": False}
        assert cards(eng, "notify_text") == [] and popups(eng) == []
        # 쿨다운 시작(mark_finished) — 바로 다시 시작하면 거절(코드 없음 → 핸들러 E700)
        assert not r.run_state.is_running("pc", 1) and r.run_state.last_error("pc", 1) is None
        again = eng.start()
        assert not again.accepted and again.code is None and "1분 대기" in again.message
        assert r.internal_errors == 0 and r.unknown_fields == set()
    finally:
        r.stop()


def test_excel_plasma_cleaning_sheet_values(tmp_path):
    from v2.ch12.outputs import excel_style as X                    # openpyxl 은 outputs 에서만 import(test_isolation)
    r, eng = make(tmp_path)
    try:
        eng.start(params=dict(UI, process_name="PC-A"))
        run_to_end(r, eng)
        x = eng.excel[1]
        assert x.flush() and x.sender.flush()
        pp = eng.build_pc_params(eng.params)
        assert pp["process_name"] == "PC-A" and pp["time"] == 0.1 and pp["sp_ar"] == 10.0 and pp["sp_pressure"] == 2.0
        assert pp["base_pressure"] == eng.readings["ig"][-1]             # IG 마지막 값(2177-2179)
        assert pp["avg_forp"] == round(sum(eng.readings["forp"]) / len(eng.readings["forp"]), 4)
        assert pp["avg_load"] is None and pp["avg_tune"] is None
        assert len(eng.readings["forp"]) >= 6 and eng.readings["flow"] and eng.readings["pressure"]
        # FWD 평균: 공정 6 s 동안 v1 RF 제어(모자라면 +3 W fine-up, 유지 중 ±0.1 W/2회)가 목표 둘레 +3 W 안에서 움직인다 —
        # 어느 쪽에 머무는지는 1초 읽기 표본 시점(쓰기 뒤 몇 ms)에 달려 3.5 W 폭(PLC 나눠 읽기 뒤 102.8 W, 전 100.0 W)
        assert abs(pp["avg_forp"] - 100.0) < 3.5 and abs(pp["avg_ar"] - 10.0) < 0.5 and abs(pp["avg_pressure"] - 2.0) < 0.2
        for f in (Path(r.cfg["GDRIVE_LOG_DIR"]) / "CH1.xlsx",
                  Path(r.cfg["OUTPUT_LOCAL_FIRST_DIR"]) / "Process_log_PC" / "CH1.xlsx"):
            ws = X.load_workbook(str(f))["Plasma Cleaning"]
            vals = [c.value for c in list(ws.iter_rows(min_row=4, max_row=4))[0]]
            assert vals[2:] == [pp["process_name"], None, None, pp["time"], pp["base_pressure"], pp["sp_ar"],
                                pp["avg_ar"], pp["sp_pressure"], pp["avg_pressure"], pp["sp_power"], pp["avg_forp"],
                                pp["avg_refp"], None, None], f
    finally:
        r.stop()


def test_normal_flow_ch2_mask_split_and_bindings(tmp_path):
    r, eng = make(tmp_path, hook=sp4_override_hook())
    try:
        ok, _ = eng.select_channel(2)
        assert ok and eng.dev.mfc_gas is r.mfc["ch1"] and eng.dev.mfc_pressure is r.mfc["ch2"] and eng.dev.ig is r.ig["ch2"]
        L = logs(eng)
        assert L[-3:] == ["PC: Bind MFC: GAS=192.168.1.50:4003, SP4=192.168.1.50:4006", "PC: Selected Chamber → CH2",
                          "PC: [Plasma Cleaning] Use CH2 IG, SP4 → MFC2, GasFlow → MFC1 ch3"]
        assert eng.start(params=UI).accepted
        ticks(r, lambda: eng.pc.step == "process")
        assert (r.mfc["ch1"].poll_gas, r.mfc["ch1"].poll_pressure) == (True, False)          # GAS→R60만
        assert (r.mfc["ch2"].poll_gas, r.mfc["ch2"].poll_pressure) == (False, True)          # SP4→R5만
        assert r.shared.camera.users == {"pc2"}
        run_to_end(r, eng)
        assert eng.last_final["ok"] is True and eng.last_final["ch"] == 2
        L = logs(eng)
        assert "PC: MFC 폴링 마스크 분리: GAS→R60만, SP4→R5만" in L
        assert any(ln.startswith("IG-CH2:") for ln in L) and any(ln.startswith("MFC(GAS):") for ln in L)
        assert any(ln.startswith("MFC(SP4-CH2):") for ln in L)
        assert r.sims["ch1.mfc"].set_commands_seen()[-3:] == ["Q3 10.0", "L31", "L30"]       # 가스는 MFC1
        assert r.sims["ch2.mfc"].set_commands_seen()[-3:] == ["S4 0.2", "D4", "O"]           # SP4 는 MFC2
        assert r.chambers["ch2"].valves["gate"] is True and r.chambers["ch1"].valves["gate"] is False
        assert r.sims["plc"].coils[C["DCV_SET_1"]] is False                                  # RF 는 늘 DCV ch1
        for ch in ("ch1", "ch2"):
            assert (r.mfc[ch].poll_gas, r.mfc[ch].poll_pressure) == (True, True) and r.mfc[ch].poll_users == set()
        eng.run_log.flush()
        assert eng.run_log.path.name.endswith("_CH2.log")
        x = eng.excel[2]
        assert x.flush() and x.sender.flush() and (Path(r.cfg["GDRIVE_LOG_DIR"]) / "CH2.xlsx").exists()
    finally:
        r.stop()


def test_ch2_with_chamber1_on_mfc1_keeps_its_polling(tmp_path):
    r, eng = make(tmp_path, hook=sp4_override_hook())
    try:
        r.shared.mfc["ch1"].acquire("chamber1")
        r.shared.camera.join("chamber1")
        eng.select_channel(2)
        eng.start(params=UI)
        run_to_end(r, eng)
        L = logs(eng)
        assert "PC: MFC1 다른 공정 사용 중(chamber1) → MFC 폴링 마스크 분리 생략" in L
        assert "MFC: MFC1 폴링 유지 — 사용 중: chamber1" in L                                # 2018-2066 ③
        assert "CAM: 카메라 사용 종료 — 다른 공정이 계속 촬영 중" in L
        assert r.mfc["ch1"].poll_users == {"chamber1"} and r.mfc["ch1"].polling
        assert r.shared.camera.users == {"chamber1"}
    finally:
        r.stop()


# ───────────────────────── 프리플라이트 실패 ─────────────────────────
@pytest.mark.parametrize("ch", [1, 2])
def test_gv_interlock_false_is_e302_and_cleanup_sends_device_commands(tmp_path, ch):
    r, eng = make(tmp_path)
    try:
        r.sims["plc"].coils[C[f"G_V_{ch}_인터락"]] = False
        if ch == 2:
            eng.select_channel(2)
        before = list(r.sims[f"ch{ch}.mfc"].set_commands_seen())
        res = host(r, eng, "", origin_meta={"key": ""})
        msg = f"게이트밸브 인터락 FALSE (G_V_{ch}_인터락=FALSE) → LoadLock의 압력을 확인하십시오."
        assert (res.ok, res.code, res.raw_code, res.message) == (False, "E302", "E302", msg)
        ticks(r, lambda: not eng.running)
        assert ("critical", "게이트밸브 인터락", msg) in popups(eng)
        assert r.run_state.last_error("pc", ch) == msg and not r.run_state.is_running("pc", ch)
        # [이상] 켠 것이 없어도 _final_cleanup 장비 명령(PC-096): SP4 MFC 'O', RF 0 W, MFC 정리
        assert r.sims[f"ch{ch}.mfc"].set_commands_seen()[len(before):] == ["O"]
        L = logs(eng)
        assert "MAIN: [CLEANUP] begin" in L and "STEP: 종료: MFC(SP4) VALVE OPEN OK" in L and "RF: 정지 신호 수신됨." in L
        assert "STEP: 종료: MFC GAS OFF OK" not in L                                         # 선택 가스 없음 → 예외 삼킴
        assert r.chambers[f"ch{ch}"].valves["gate"] is False                                 # GV 를 열지 않음
        assert cards(eng) == []                                                              # PC-097 — 카드 없음
        # PC-101: 아무것도 안 돈 실패도 쿨다운
        ok_cool, remain, _ = r.run_state.pc_block_reason(ch)
        assert not ok_cool and remain > 50
    finally:
        r.stop()


def test_gv_interlock_read_failure_is_code_less_e511(tmp_path):
    r, eng = make(tmp_path)
    try:
        now = r.clock.monotonic()
        r.sims["plc"].add_fault("exception", now, now + 1.5)
        res = host(r, eng, "")
        assert res.ok is False and res.raw_code is None and res.code == "E511"
        assert res.message.startswith("게이트밸브 인터락 상태 확인 실패: ")
        ticks(r, lambda: not eng.running)
        assert popups(eng)[-1][1] == "GV 인터락 오류"
    finally:
        r.stop()


def test_gv_interlock_read_timeout_message(tmp_path):
    r, eng = make(tmp_path)
    try:
        now = r.clock.monotonic()
        r.sims["plc"].add_fault("no_response", now, now + 1.0)
        res = host(r, eng, "")
        assert (res.code, res.message) == ("E511", "게이트밸브 인터락 상태 확인 실패: GV 인터락 read timeout(0.6s): G_V_1_인터락")
    finally:
        r.stop()


def test_preflight_connect_failure_no_device_commands_and_preflight_timeout_e700(tmp_path):
    r, eng = make(tmp_path, cfg={"PC_HOST_START_WAIT_TIMEOUT_S": 1.0})
    try:
        now = r.clock.monotonic()
        r.sims["ch1.mfc"].add_fault("disconnect", now, now + 60)
        ticks(r, lambda: not r.mfc["ch1"].is_connected(), 30)
        before = list(r.sims["ch1.mfc"].set_commands_seen())
        res = host(r, eng, "")
        assert res.code == "E700" and res.raw_code is None
        assert res.message == "preflight timeout(1.0s) (쿨다운/가드 등으로 프리플라이트에 도달하지 못했습니다)"
        ticks(r, lambda: not eng.running)
        kind, title, text = popups(eng)[-1]
        assert (kind, title) == ("critical", "연결 실패") and "장비 연결 타임아웃: MFC(GAS), MFC(SP4)" in text
        assert r.sims["ch1.mfc"].set_commands_seen() == before                               # 장치 명령 없음
        assert "MAIN: [CLEANUP_TASKS_ONLY] preflight_connect failed" in logs(eng)
        assert not r.run_state.pc_block_reason(1)[0]                                         # 쿨다운(PC-101)
    finally:
        r.stop()


# ───────────────────────── RF ─────────────────────────
def _fwd_raw(w):
    return int(round((w + 4.0 - 3.0664228165) / 0.1503488383))


def test_rf_not_reached_timeout_message_and_forced_fail(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=UI)
        ticks(r, lambda: eng.pc.step == "rf_start")
        r.sims["plc"].forced_regs = {2: _fwd_raw(50.0)}
        t0 = r.clock.monotonic()
        run_to_end(r, eng, 400)
        L = logs(eng)
        why = next(ln for ln in L if ln.startswith("RF: RF Power 도달 실패"))
        assert "목표(100.0W) 미도달, timeout 60s (FWD=50.0W" in why                         # [이상] 문구 60s, 실제 120 s(PC-095)
        assert "STEP: STOP during RF START → cancel rf_start & shutdown" in L
        reason = why.split(": ", 1)[1]
        assert eng.last_final == {"ok": False, "stopped": False, "reason": reason, "ch": 1, "status": "failed"}
        assert r.clock.monotonic() - t0 > 120
        fin = cards(eng, "notify_process_finished_detail")[0]["payload"]
        assert fin["status"] == "failed" and fin["error_source"] == "RF" and fin["error_meta"]["stage"] == "_rf_start"
        assert cards(eng, "notify_text")[0]["text"] == f"❌ Plasma Cleaning 실패 이유: {reason}"
        assert r.run_state.last_error("pc", 1) == reason
        assert r.sims["plc"].coils[C["DCV_SET_1"]] is False
        assert not (Path(r.cfg["GDRIVE_LOG_DIR"]) / "CH1.xlsx").exists()                   # 성공만 엑셀
    finally:
        r.stop()


def test_rf_zero_output_reason(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=UI)
        ticks(r, lambda: eng.pc.step == "rf_start")
        r.sims["plc"].forced_regs = {2: 0, 3: 0}
        run_to_end(r, eng, 400)
        assert eng.last_final["reason"] == "RF Power 도달 실패: 장비 OFF/SET 미설정(0W 지속/응답 없음)"
    finally:
        r.stop()


def test_rf_target_failed_event_ref_over(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=UI)
        ticks(r, lambda: eng.pc.step == "rf_start")
        r.sims["plc"].forced_regs = {3: 523}                                                 # REF ≈ 80 W > 50 W
        run_to_end(r, eng, 300)
        L = logs(eng)
        reason = eng.last_final["reason"]
        assert reason.startswith("Ref.p(REF) 안정화 시간(60s) 초과") and eng.last_final["status"] == "failed"
        i = [L.index(f"RF: 목표 파워 실패: {reason}"), L.index("PC: RF 실패 감지 → PC 컨트롤러에 FAIL 전달"),
             L.index(f"RF: RF START 실패 확정: {reason}"), L.index("STEP: STOP during RF START → cancel rf_start & shutdown")]
        assert i == sorted(i)
    finally:
        r.stop()


def test_rf_off_unconfirmed_alert_once_no_set_off_and_fail(tmp_path):
    state = {"done": False}

    def hook(r, eng, t):
        if eng.phase == "stopping" and not state["done"]:
            state["done"] = True
            now = r.clock.monotonic()
            r.sims["plc"].add_fault("exception", now, now + 120)
    r, eng = make(tmp_path, hook=hook)
    try:
        eng.start(params=UI)
        run_to_end(r, eng, 400)
        L = logs(eng)
        assert "RF: cleanup timeout(5.0s) → 계속 진행" in L                                 # 직접 OFF(≤ 100 W) 경로
        assert "RF: ramp-down 완료 신호 timeout → 강제 0W/SET OFF" in L
        alerts = cards(eng, "notify_error_with_src")
        assert len(alerts) == 1 and alerts[0]["src"] == "PC" and "출력 상태 미확인" in alerts[0]["text"]
        assert eng.rf_off_unconfirmed_reason.startswith("강제 0W 쓰기 실패 (")
        assert eng.last_final == {"ok": False, "stopped": False, "reason": eng.rf_off_unconfirmed_reason, "ch": 1,
                                  "status": "failed"}                                         # [A2] 성공을 실패로
        assert r.sims["plc"].coils[C["DCV_SET_1"]] is True                                  # 0 W 미확인 → SET OFF 안 함
        assert not (Path(r.cfg["GDRIVE_LOG_DIR"]) / "CH1.xlsx").exists()
    finally:
        r.stop()


# ───────────────────────── STOP·중단 ─────────────────────────
def test_user_stop_during_countdown(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=dict(UI, process_time="1"))
        ticks(r, lambda: eng.pc.step == "process")
        r.loop.run(3)
        assert eng.stop() == (True, "")
        assert eng.stop() == (False, "이미 정지 요청됨")
        run_to_end(r, eng)
        L = logs(eng)
        assert "MAIN: [STOP] 사용자 정지 요청 수신" in L and "CTRL: [STOP] pc.request_stop() 전달" in L
        assert any(ln.startswith("STEP: STOP 이벤트 감지 → 남은 ") for ln in L)
        assert eng.last_final == {"ok": False, "stopped": True, "reason": "사용자 STOP", "ch": 1, "status": "stopped"}
        assert cards(eng, "notify_text")[0]["text"] == "❌ Plasma Cleaning 실패 이유: 사용자 STOP"     # PC-100
        assert r.run_state.last_error("pc", 1) is None and popups(eng) == []                    # STOP 은 오류 아님
        assert r.sims["plc"].coils[C["DCV_SET_1"]] is False and r.chambers["ch1"].valves["gate"] is True
        assert not (Path(r.cfg["GDRIVE_LOG_DIR"]) / "CH1.xlsx").exists()
        assert eng.stop() == (False, "실행 중이 아님")
    finally:
        r.stop()


def test_stop_during_sp4_wait_aborts_before_rf(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=UI)
        ticks(r, lambda: eng.pc.step == "sp4_wait")
        r.loop.run(2)
        eng.stop()
        run_to_end(r, eng)
        L = logs(eng)
        assert "STEP: STOP during SP4 pressure wait → abort before RF" in L
        assert not any("RF Power 적용" in ln for ln in L) and eng.last_final["status"] == "stopped"
    finally:
        r.stop()


def test_abort_from_safety_rule_keeps_reason(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=dict(UI, process_time="1"))
        ticks(r, lambda: eng.pc.step == "process")
        assert eng.abort("S99 시험 중단")
        run_to_end(r, eng)
        assert eng.last_final == {"ok": False, "stopped": True, "reason": "S99 시험 중단", "ch": 1, "status": "stopped"}
        assert eng.abort("x") is False
    finally:
        r.stop()


# ───────────────────────── 채널 고정·쿨다운·가드 ─────────────────────────
def test_channel_switch_rejected_while_running_and_host_start_e205_before_recipe(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=dict(UI, process_time="1"))
        ticks(r, lambda: eng.pc.step == "ig_base")
        assert eng.select_channel(2) == (False, "플라즈마 클리닝 실행 중에는 챔버 전환이 불가합니다.")
        assert eng.set_selected_ch(2) is False and eng.ch == 1
        assert logs(eng)[-1] == "PC: 실행 중 → 챔버 선택 변경 무시 (요청 CH2, 현재 CH1)"
        f = tmp_path / "pc_ch2.csv"
        f.write_text("use_ch,process_name,base_pressure,gas_flow,working_pressure,rf_power,process_time\r\n"
                     "2,PC-CH2,5e-6,20,3,100,5\r\n", encoding="utf-8-sig")
        ui_before = dict(eng.ui)
        res = host(r, eng, str(f))
        assert (res.ok, res.code, res.message) == (False, "E205", "이미 Plasma Cleaning이 실행 중입니다.")
        assert ("warning", "실행 중", "이미 Plasma Cleaning이 실행 중입니다.") in popups(eng)
        assert eng.ui == ui_before and eng.ch == 1 and eng.loaded_row is None                # TEST-110
        res = host(r, eng, str(tmp_path / "nope.csv"))
        assert res.code == "E207"                                                            # 파일 오류가 먼저
        eng.stop()
        run_to_end(r, eng)
        assert not r.run_state.is_running("pc", 1) and eng.last_final["ch"] == 1             # TEST-111
    finally:
        r.stop()


def test_cooldown_rules(tmp_path):
    r, eng = make(tmp_path)
    try:
        rs = r.run_state
        rs.mark_finished("pc", 2)                         # 앞선 PC 종료는 CH1·CH2 PC 모두 막음
        res = eng.start()
        assert (res.accepted, res.code, res.message) == (False, None,
                                                         "이전 공정 종료 후 1분 대기 필요합니다. 약 60초 후에 다시 시도하십시오.")
        assert ("warning", "대기 필요", "이전 공정 종료 후 1분 대기 필요합니다.\n60초 후에 시작하십시오.") in popups(eng)
        assert host(r, eng, "").code == "E700"
        r.loop.run(61)
        rs.mark_finished("tsp", 0)                        # TSP 종료는 CH1 PC 만
        assert not eng.start().accepted
        eng.select_channel(2)
        rs2 = rs.pc_block_reason(2)
        assert rs2[0] is True
        rs.mark_finished("chamber", 2)                    # 같은 CH 챔버 종료
        assert rs.pc_block_reason(2)[0] is False and rs.pc_block_reason(1)[0] is False   # CH1 은 TSP 때문
        r.loop.run(61)
        rs.mark_finished("chamber", 1)
        assert rs.pc_block_reason(2)[0] is True
        rs.mark_started("chamber", 2)
        res = eng.start()
        assert (res.accepted, res.code, res.message) == (False, "E205", "CH2는 이미 다른 공정이 실행 중입니다.")
        rs.mark_finished("chamber", 2)
        r.loop.run(61)
        eng.chamber_busy_probe = lambda ch: ch == 2       # 리스트 delay·행 사이 바쁨(PC-012)
        assert eng.start().message == "CH2는 이미 다른 공정이 실행 중입니다."
        eng.chamber_busy_probe = lambda ch: 1 / 0         # 예외면 허용
        rs.mark_started("pc", 2)
        assert eng.start().message == "CH2 Plasma Cleaning이 이미 실행 중입니다."
        assert not eng.running
    finally:
        r.stop()


# ───────────────────────── 호스트 응답 코드 ─────────────────────────
def test_host_recipe_file_codes(tmp_path):
    r, eng = make(tmp_path)
    try:
        assert host(r, eng, str(tmp_path / "x.txt")).code == "E209"
        assert host(r, eng, str(tmp_path / "x.txt")).message == "지원하지 않는 레시피 형식입니다. CSV 파일 경로만 허용됩니다."
        miss = tmp_path / "none.csv"
        res = host(r, eng, str(miss))
        assert (res.code, res.message) == ("E207", f"CSV 파일을 찾을 수 없습니다: {miss}")
        empty = tmp_path / "empty.csv"
        empty.write_text("process_name,rf_power\r\n,,\r\n", encoding="utf-8-sig")
        assert host(r, eng, str(empty)).code == "E208"
        bad = tmp_path / "cp949.csv"
        bad.write_bytes("공정,rf_power\r\n클리닝,100\r\n".encode("cp949"))
        res = host(r, eng, str(bad))
        assert res.code == "E209" and "CSV UTF-8(쉼표로 분리)" in res.message
        assert eng.running is False
    finally:
        r.stop()


def test_host_duplicate_start_is_e206_and_csv_applied(tmp_path):
    r, eng = make(tmp_path)
    try:
        f = tmp_path / "pc.csv"
        f.write_text("use_ch,process_name,base_pressure,gas_flow,working_pressure,rf_power,process_time\r\n"
                     ",PC-1,5.00E-06,10,2,100,0.1\r\n,,,,,,\r\n,PC-2,,,,,\r\n", encoding="utf-8-sig")
        b1 = eng.host_start(str(f), origin_meta={"key": ""})
        b2 = eng.host_start(str(f))
        assert b2.done and (b2.value.code, b2.value.message) == ("E206", "다른 Plasma Cleaning start 요청이 이미 처리 중입니다.")
        ticks(r, lambda: b1.done, 100)
        assert b1.value.ok and b1.value.message == "preflight OK"
        assert eng.ui["base_pressure"] == "5.00E-06" and eng.ui["process_name"] == "PC-1"    # 과학 표기 글자 그대로
        assert eng.host_recipe_rows == 2                                                     # 쉼표만 있는 행 제외
        run_to_end(r, eng)
        assert eng.last_final["ok"] and eng.process_name == "PC-1"
    finally:
        r.stop()


# ───────────────────────── TEST 모드 ─────────────────────────
def test_test_mode_no_device_commands(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.load_recipe_rows([{"#": "test", "time": "10s", "process_name": "T"}])
        mfc_before = list(r.sims["ch1.mfc"].set_commands_seen())
        res = host(r, eng, "")
        assert res.ok and res.message == "TEST MODE (skip preflight)"
        t0 = r.clock.monotonic()
        run_to_end(r, eng)
        assert 9.5 <= r.clock.monotonic() - t0 <= 13
        L = logs(eng)
        assert "MAIN: [TEST MODE] 10.0s 동안 시뮬레이션 실행 (장비 제어 없음)" in L
        assert "MAIN: [CLEANUP] TEST MODE: 장비 정리(RF/MFC/PLC) 스킵, 태스크/로그만 정리" in L
        assert "MAIN: [TEST MODE] 종료 및 UI 복구 완료" in L
        assert r.sims["ch1.mfc"].set_commands_seen() == mfc_before and r.chambers["ch1"].valves["gate"] is False
        st = cards(eng, "notify_process_started")[0]["payload"]
        assert st["process_note"] == "Plasma Cleaning (TEST)" and st["use_rf_power"] is False and st["process_time"] == 0.17
        assert eng.last_final["ok"] is True and eng.ui["process_time"] == "0.17"
        assert not r.run_state.pc_block_reason(1)[0]                                         # TEST 도 쿨다운
        # PC-094: 불러온 행이 남아 다음 시작도 TEST
        assert eng.state()["test_loaded"] is True
    finally:
        r.stop()


def test_test_mode_unitless_time_is_60s_and_stop(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.load_recipe_rows([{"#": "TEST", "Time": "5"}])
        eng.start()
        r.loop.run(3)
        assert "PC: 파일 로그 시작 (TEST MODE)" in logs(eng)
        assert any("[TEST MODE] 60.0s" in ln for ln in logs(eng))                            # PC-093
        eng.stop()
        run_to_end(r, eng)
        assert eng.last_final == {"ok": False, "stopped": True, "reason": "사용자 STOP", "ch": 1, "status": "stopped"}
        assert cards(eng, "notify_text")[0]["text"] == "❌ Plasma Cleaning 실패 이유: 사용자 STOP"
    finally:
        r.stop()


# ───────────────────────── 저널·이어가기 확인 ─────────────────────────
def test_journal_and_resume_checks(tmp_path):
    r, eng = make(tmp_path)
    try:
        eng.start(params=dict(UI, process_time="1"))
        ticks(r, lambda: eng.pc.step == "sp4_wait")
        st = eng.state_for_journal()
        assert st["step"] == "sp4_wait" and eng.resume_checks(st)[0][1] is False
        ticks(r, lambda: eng.pc.step == "process")
        r.loop.run(3)
        st = eng.state_for_journal()
        assert st["step"] == "process" and st["params"]["rf_power_w"] == 100.0 and st["countdown_left"] is not None
        chk = dict((k, ok) for k, ok, _ in eng.resume_checks(st))
        assert chk == {"resumable_step": True, "power_output": True, "gas": True, "pressure": True}, eng.resume_checks(st)
        eng.stop()
        run_to_end(r, eng)
    finally:
        r.stop()


def test_robot_adapter_shape_start_for_robot(tmp_path):
    """robot_engines.RobotEngines.start_pc 모양 — 적재 실패는 예외(.code), 가드 결과는 on_preflight(ok, code, detail)."""
    r, eng = make(tmp_path)
    try:
        with pytest.raises(HostStartError) as ei:
            eng.start_for_robot(str(tmp_path / "none.csv"), {"key": ""})
        assert ei.value.code == "E207"
        got = []
        r.sims["plc"].coils[C["G_V_1_인터락"]] = False
        eng.start_for_robot("", {"key": "", "request_id": "r1"}, lambda ok, code, detail: got.append((ok, code, detail)))
        ticks(r, lambda: bool(got) and not eng.running)
        assert got == [(False, "E302", "게이트밸브 인터락 FALSE (G_V_1_인터락=FALSE) → LoadLock의 압력을 확인하십시오.")]
        st, last = eng.pc_state_for_robot(), eng.last_pc_result()
        assert st == {"ready": True, "running": {1: False, 2: False}, "cleaning": False, "selected_ch": 1}
        assert last["error"] == {1: True, 2: False} and last["finished"][1] is not None and last["finished"][2] is None
        got.clear()
        eng.start_for_robot("", {}, lambda ok, code, detail: got.append((ok, code, detail)))
        assert got and got[0][0] is False and got[0][1] is None and "1분 대기" in got[0][2]      # 쿨다운 — 코드 없음
    finally:
        r.stop()
