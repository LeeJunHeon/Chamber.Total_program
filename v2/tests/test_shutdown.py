"""의도적 종료(resume.intentional_exit·app install_console_handlers·winshell) — 저널 표시 먼저, 파워 출력만 끔, 다시 켜면 이어가지 않음.

- 가상 시계 실제 조립: CH2 공정(DC1·RF Pulse·가스·셔터) 메인 중 의도적 종료 → 저널 모든 부분에 exit.intentional, run_end error
  PROGRAM_EXIT 하나, DC1 SET OFF·0 W·RF Pulse OFF, PLC 릴레이(가스 밸브·메인 밸브·셔터)·MFC 유량은 그대로(챔버K 결정 4).
  4단계 안전 규칙(S32 safety.power_off_only)이 있으면 그것을, 없으면 resume.power_off_only + 엔진 program_exit.
  그 뒤 같은 시뮬레이터로 다시 켜면 이어가지 않는다.
- 실제 하위 프로세스: Ctrl+C(SIGINT)·SIGTERM → 종료 코드 0, 저널 exit 표시.
- winshell: 윈도우가 아니면 아무것도 설치하지 않음, 처리는 한 번만. 윈도우 콘솔 닫기·로그오프는 리눅스에서 못 돌림(윈도우 확인 필요).
"""
import json
import signal
import sys

import pytest

from v2.ch12 import resume as RS
from v2.ch12 import winshell
from v2.ch12.drivers import plc_map as PM
from v2.tests import restart_rig as RR

P = dict(base_pressure=7e-6, working_pressure=5, process_time=1.0, shutter_delay=0.2, use_ar=True, ar_flow=20,
         use_dc_power=True, dc_power=100, use_rf_pulse=True, rf_pulse_power=100, rf_pulse_freq=1.0, rf_pulse_duty=50,
         use_ms=True, use_g1=True)


def _coil(rig, name):
    return rig.sims["plc"].coils[PM.resolve(name)]


@pytest.mark.parametrize("via", ["safety", "builtin"])
def test_intentional_exit_powers_off_only_and_no_resume(tmp_path, via):
    rig = RR.boot(tmp_path)
    e = rig.eng["ch2"]
    RR.drive(rig, lambda: rig.resumer.done, 30)
    assert e.start(params=dict(P, process_note="ex"), owner="local", source="local") is None
    RR.drive(rig, lambda: e.cur_inst is not None and e.cur_inst.type == "main", 600)
    assert _coil(rig, "DCV_SET_0") and rig.sims["ch2.rfpulse"].out and rig.sims["ch2.mfc"].on[0]
    if via == "builtin":
        rig.safety = None                                     # 안전 규칙이 없을 때의 대체 경로
    res = RS.intentional_exit(rig, rig.journal, "CTRL_CLOSE")
    assert res is not None and "ch2" in res["process_was_running"]
    assert RS.intentional_exit(rig, rig.journal, "CTRL_CLOSE") is None           # 두 번째는 아무것도 안 함
    RR.drive(rig, lambda: not e.busy, 60)
    for part in ("ch1", "ch2", "pc", "tsp", "shared"):
        d = json.loads((tmp_path / "data" / "journal" / f"{part}.json").read_text(encoding="utf-8"))
        assert d["exit"]["intentional"] is True and d["exit"]["why"] == "CTRL_CLOSE"
    assert RR.ends(e) == [("error", "PROGRAM_EXIT")]
    assert not _coil(rig, "DCV_SET_0") and rig.sims["plc"].regs[PM.REG_MAP["DCV_WRITE_0"]] == 0
    assert not rig.sims["ch2.rfpulse"].out
    # PLC 릴레이·MFC 는 그대로(결정 4)
    assert _coil(rig, "MAIN_2_GAS_SW") and _coil(rig, "Ar_2_GAS_SW") and _coil(rig, "MAIN_SHUTTER_2_SW")
    assert rig.sims["ch2.mfc"].on[0]
    # 다시 켜면 이어가지 않는다
    rig.journal.abandon()
    rig.clock.close()
    rig2 = RR.reboot(tmp_path, rig, 2.0)
    try:
        e2 = rig2.eng["ch2"]
        RR.drive(rig2, lambda: rig2.resumer.done and not e2.busy, 120)
        assert rig2.resumer.done and "ch2" not in rig2.resumer.decisions and not RR.resumed(e2) and not e2.busy
        assert RR.ends(e2) == []
    finally:
        RR.finish(rig2)


def test_exit_mark_written_before_power_off(tmp_path, monkeypatch):
    """저널 표시가 파워 끄기보다 먼저 — 끄는 도중 죽어도 다음 시작은 이어가지 않는다."""
    rig = RR.boot(tmp_path, start=False)
    order = []
    real_mark = rig.journal.mark_exit
    monkeypatch.setattr(rig.journal, "mark_exit", lambda why: (order.append("journal"), real_mark(why)))
    monkeypatch.setattr(rig.safety, "power_off_only", lambda why, dl=None: order.append("power") or {"sent": []})
    try:
        RS.intentional_exit(rig, rig.journal, "SIGTERM")
        assert order == ["journal", "power"]
    finally:
        RR.finish(rig)


def test_winshell_noop_off_windows_and_fires_once():
    got = []
    if sys.platform != "win32":
        assert winshell.install(got.append) is None and winshell.user32_loaded() is None
    sh = winshell.WinShell(got.append)
    sh.fire("CTRL_CLOSE")
    sh.fire("WM_ENDSESSION")
    assert got == ["CTRL_CLOSE"]
    assert winshell.CTRL_NAMES == {2: "CTRL_CLOSE", 5: "CTRL_LOGOFF", 6: "CTRL_SHUTDOWN"}


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX 신호로 시험 — 윈도우는 콘솔 이벤트(윈도우 확인 필요)")
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
def test_real_app_signal_marks_journal_and_exits_0(tmp_path, sig):
    import time
    from v2.tests import test_app as TA
    cfg = TA.config(tmp_path)
    pr = TA.Proc(cfg)
    assert pr.running.wait(60.0), pr.text
    time.sleep(2.5)
    pr.p.send_signal(sig)
    assert pr.wait(30.0) == 0, pr.text
    name = "SIGINT" if sig == signal.SIGINT else "SIGTERM"
    for part in ("ch1", "ch2", "shared"):
        d = json.loads((tmp_path / "root" / "data" / "journal" / f"{part}.json").read_text(encoding="utf-8"))
        assert d["exit"] == {"intentional": True, "why": name, "t_wall": d["exit"]["t_wall"]} and d["active"] is False
