"""N — 챔버K 와 이름·말 맞추기(사용자 결정 2026-11-27).

1 사유 코드 MFC_COMM_LOSS → MFC_COMM_LOST(설정 키 MFC_COMM_LOSS_ABORT_SEC 는 그대로).
2 화면·콘솔에 나가는 글의 '두절' → '끊김', 콘솔 'NAS 사본 끔' → 'NAS 사본 사용하지 않음'(주석은 그대로).
3 drivers/rf.py 의 고정값 UNCMD_GRACE_S(5 s) → 설정 RF_UNCOMMANDED_GRACE_SEC(기본 5, 0~30 — 챔버K 와 같은 이름).
"""
import json

from v2.ch12 import commands as C
from v2.ch12 import web_schema as W
from v2.ch12.drivers.plc import PlcPoll
from v2.ch12.params import table
from v2.ch12.sim.plc_rig import PlcRig

DEF = {d.key: d for d in table().params}


def test_mfc_reason_code_renamed_setting_key_kept():
    codes = C.reason_codes()
    assert "MFC_COMM_LOST" in codes and "MFC_COMM_LOSS" not in codes
    assert "MFC_COMM_LOSS_ABORT_SEC" in DEF
    r = {x["code"]: x for x in W.commands_part()["reasons"]}
    assert r["MFC_COMM_LOST"]["label"] == "MFC 통신 끊김"
    assert r["MFC_COMM_LOST"]["help"] == "MFC 와 통신이 끊겨 공정을 멈췄습니다. 케이블과 전원을 확인하세요."
    from v2.ch12 import safety_rules as SR
    s37 = next(x for x in SR.RULES if x.id == "S37")
    assert {a.args.get("reason") for a in s37.actions if a.kind == "PROCESS_ABORT"} == {"PLC_COMM_LOST", "MFC_COMM_LOST"}


def test_no_dujeol_in_exported_bundle():
    """웹·노트북 화면이 받는 묶음(설정 설명·사유·규칙 글·필드 글) 어디에도 '두절'이 없다."""
    text = W.to_json(W.bundle())
    assert "두절" not in text
    assert "오래 끊긴 뒤에도 이 간격" in json.dumps([p.desc for p in table().params], ensure_ascii=False)


def test_console_nas_line_says_not_used(tmp_path):
    from v2.ch12 import app as A
    from v2.tests.test_app_web import FakeWeb, _in_process
    web = FakeWeb()
    app, lines = _in_process(tmp_path, web, UPLINK_IN_SIM=False, EQUIPMENT_ID="CH12")
    try:
        assert app.start() == A.EXIT_OK
        out = "\n".join(lines)
        assert "NAS 사본 사용하지 않음" in out and "NAS 사본 끔" not in out
    finally:
        app.shutdown()


def test_rf_uncommanded_grace_setting():
    d = DEF["RF_UNCOMMANDED_GRACE_SEC"]
    assert (d.default, d.min, d.max, d.label) == (5.0, 0.0, 30.0, "RF 끈 뒤 명령 없는 출력 유예")
    import v2.ch12.drivers.rf as RFM
    assert not hasattr(RFM, "UNCMD_GRACE_S")                              # 고정값 없음 — 설정으로

    def run(grace):
        r = PlcRig(rf=True, cfg={"RF_UNCOMMANDED_GRACE_SEC": grace})
        ctl, t = r.rf, [100.0]

        def dec(latch, f_raw, dt=1.0):
            t[0] += dt
            return ctl._decode(PlcPoll(0.0, t[0], {82: latch}, {8: f_raw, 9: 0}, {}))
        ctl.last_sent = 50.0
        dec(True, 400)                                                    # 명령 출력 중
        ctl.last_sent = None
        got = [dec(False, 400, dt=1.0)["ch2.rf.uncommanded"] for _ in range(12)]   # 끈 뒤 1~12 s, Forward 59.6 W 남음
        r.done()
        return got
    g5, g0, g10 = run(5.0), run(0.0), run(10.0)
    assert g5[:4] == [False] * 4 and g5[4:] == [True] * 8                 # 기본 5 s — 예전 고정값과 같은 동작
    assert g0 == [True] * 12                                              # 0 = 끈 바로 다음 읽기부터 판정
    assert g10[:9] == [False] * 9 and g10[9:] == [True] * 3
