"""전수 목록 영역 chat(v2/ch12/spec/inv_chat.py) — 구글챗 카드의 '계기'가 되는 장비 이벤트를 v2 가 v1 문구 그대로 내는지.

카드 모양·문구·중복 억제·보내기는 포털(vanam-process lib/ops2/ch12/chatCards.ts·chat.ts — 골든 scripts/ops2-chat-ch12.mjs 317 경우,
scripts/ops2-ch12-ui-check.mjs)이 맡는다. 여기는 장비 쪽: 엔진이 내는 chat 이벤트(v1 notify_error_with_src·notify_text 에 넣던 글)
— 전원 OFF 미확인·RF/DC Pulse OFF 실패·정리 중 OFF 미확인·OES/RGA 측정 실패·실패 이유 글·보내지 않는 경우(연속 RF·IG).
v1 근거는 시험마다 'runtime/chamber_runtime.py:줄'. v1 을 import 하지 않는다(test_isolation).
"""
from __future__ import annotations

from types import SimpleNamespace

from v2.tests import proc_rig as R


# ───────────── 대역: ProcessEventsMixin 이 보는 엔진 자리(chat·run_warnings·stats) ─────────────
class _Eng:
    def __init__(self):
        self.chats, self.run_warnings, self.busy, self.run_id, self.params = [], [], True, "r1", {}

        class _St:
            def __getattr__(self, _n):
                return lambda *a, **k: None
        self.stats = _St()

    def chat(self, text):
        self.chats.append(text)

    def ig_in_use_by_pc(self):
        return False


class _Runner:
    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        return lambda *a, **k: self.calls.append((name, a, k))


def _host(n=2):
    from v2.ch12.process_events import ProcessEventsMixin

    class H(ProcessEventsMixin):
        pass
    h = H()
    h.n, h.e, h.runner, h.owner = n, _Eng(), _Runner(), f"chamber{n}"
    h.logs, h.fails, h.matches = [], [], []
    h.dc_failed = {1: False, 2: False}
    h.log = lambda src, msg: h.logs.append((src, msg))
    h._fail = lambda src, why, **k: h.fails.append((src, why, k))
    h._match = lambda *a: h.matches.append(a)
    return h


def _ev(**kw):
    base = dict(cmd=None, detail=None, reason=None, data={}, code=None, owner=None)
    base.update(kw)
    return SimpleNamespace(**base)


OFF_TAIL = "(켜져 있을 가능성 있음). 전원 장비 출력과 PLC D/A(SET/WRITE)를 즉시 확인하세요."


def test_chat_power_off_failed_texts_dc1_dc2_rf():
    """CHAT-023 — chamber_runtime.py:2390-2399(DC1)·2450-2459(DC2)·2494-2501(RF): '⚠️ CHn {unit} Power OFF 실패 - 출력 상태
    미확인 …' 카드 글 + 런 경고 '{unit} Power OFF 실패(출력 상태 미확인)' + 정지 순서에 OFF 실패로 넘김."""
    h = _host(2)
    h._ev_dc("power_off_failed", _ev(data={"unit": "DC1"}, detail="off timeout"))
    h._ev_dc("power_off_failed", _ev(data={"unit": "DC2"}))
    h._ev_rf("power_off_failed", _ev(data={"mode": "ch2"}))
    assert h.e.chats == [f"⚠️ CH2 DC1 Power OFF 실패 - 출력 상태 미확인 {OFF_TAIL}",
                         f"⚠️ CH2 DC2 Power OFF 실패 - 출력 상태 미확인 {OFF_TAIL}",
                         f"⚠️ CH2 RF Power OFF 실패 - 출력 상태 미확인 {OFF_TAIL}"]
    assert h.e.run_warnings == ["DC1 Power OFF 실패(출력 상태 미확인)", "DC2 Power OFF 실패(출력 상태 미확인)",
                                "RF Power OFF 실패(출력 상태 미확인)"]
    assert [(c[0], c[1][:2]) for c in h.runner.calls] == [("power_off_failed", ("DC Power", "DC_OFF")),
                                                         ("power_off_failed", ("DC2 Power", "DC2_OFF")),
                                                         ("power_off_failed", ("RF Power", "RF_OFF"))]


def test_chat_rf_pulse_off_failed_only_rf_off():
    """CHAT-024 — chamber_runtime.py:2561-2593: RF_OFF 명령 실패만 카드 글 + 런 경고, 다른 RF Pulse 명령 실패는 글 없이 공정 실패."""
    h = _host(1)
    h._ev_rfp("command_failed", _ev(cmd="RF_OFF", detail="no ack"))
    assert h.e.chats == ["⚠️ CH1 RF Pulse OFF 실패 - 출력 상태 미확인 (켜져 있을 가능성 있음). "
                         "RF Pulse 장비의 출력 상태를 즉시 확인하세요."]
    assert h.e.run_warnings == ["RF Pulse OFF 실패(출력 상태 미확인)"]
    assert h.runner.calls[-1][0] == "power_off_failed" and h.runner.calls[-1][1][:2] == ("RFPulse", "RFPULSE_OFF")
    h = _host(1)
    h._ev_rfp("command_failed", _ev(cmd="SET_POWER", detail="nak"))
    assert h.e.chats == [] and h.runner.calls[-1][0] == "step_failed"


def test_chat_dc_pulse_output_off_failed_before_phase_check():
    """CHAT-025 — chamber_runtime.py:2695-2733: OUTPUT_OFF 실패는 prepare 단계 검사보다 먼저 카드 글(+ 런 경고·OFF 실패),
    prepare 단계의 다른 실패·READ_*·FAULT_RESET 실패는 글 없음(CHAT-026 끝)."""
    h = _host(2)
    h._ev_dcp("command_failed", _ev(cmd="OUTPUT_OFF", detail="timeout", data={"phase": "prepare"}))
    assert h.e.chats == ["⚠️ CH2 DC Pulse OUTPUT OFF 실패 - 출력 상태 미확인 (켜져 있을 가능성 있음). "
                         "장비 패널의 HV/OUTPUT 상태를 즉시 확인하세요."]
    assert h.e.run_warnings == ["DC Pulse OUTPUT OFF 실패(출력 상태 미확인)"]
    assert h.runner.calls[-1][0] == "power_off_failed"
    h = _host(2)
    for cmd, data in (("REF_POWER", {"phase": "prepare"}), ("READ_P", {}), ("FAULT_RESET", {})):
        h._ev_dcp("command_failed", _ev(cmd=cmd, detail="x", data=data))
    assert h.e.chats == [] and h.runner.calls == []


def test_chat_no_card_for_rf_cont_target_failed_and_ig():
    """CHAT-022 — chamber_runtime.py:2483-2492·2285: 연속 RF(PLC DAC) target_failed 와 IG 오류는 카드 글을 내지 않고 공정 실패만."""
    h = _host(2)
    h._ev_rf("target_failed", _ev(data={"mode": "ch2", "forward": 0.0}, detail="RF 미도달"))
    assert h.e.chats == [] and h.fails and h.fails[-1][0] == "RF Power"
    h._ev_ig("base_failed", _ev(detail="IG 실패", pressure=None))
    assert h.e.chats == []


def test_chat_cleanup_off_unconfirmed_and_measure_failures(tmp_path):
    """CHAT-027 — chamber_runtime.py:5470-5486: 정리 중 OFF 가 한도로 끊긴 펄스 전원(v1 도 cleanup_off_unconfirmed 는 DC Pulse·
    RF Pulse 만 — device/dc_pulse.py:557·rf_pulse.py:356)마다 '⚠ 정리 중 {이름} 출력 OFF 미확인 — 장비 출력 상태 확인 필요'.
    CHAT-028 — 1411-1420·2326-2331: OES 측정 예외 '[OES] 실패: {repr}', RGA 최종 실패 '[RGAn] 측정 실패: {사유} → 건너뜀'."""
    rig = R.make(tmp_path, chambers=("ch1", "ch2"))
    try:
        assert rig.start()
        rig.loop.run(2)
        e2, e1 = rig.eng["ch2"], rig.eng["ch1"]
        a = e2.actions
        a.dcp.worker = object()                                  # 이번 런에 쓴 펄스 전원(포트를 잡은 상태)
        a.dcp.cleanup = lambda on_done: on_done(True)            # OFF 가 대기 한도로 끊김(결과 이벤트 없음)
        a.cleanup_devices()
        a.dcp.worker = None
        chats = [x.detail for x in e2.events if x.kind == "chat"]
        assert chats == ["⚠ 정리 중 DC Pulse 출력 OFF 미확인 — 장비 출력 상태 확인 필요"]
        assert R.has_log(e2, "⚠ 정리 중 DC Pulse 출력 OFF 미확인 — 장비 출력 상태 확인 필요")

        b = e1.actions
        b.oes = None
        b._a_oes_run(SimpleNamespace(params=(10.0, 60)))
        b.rga = SimpleNamespace(scan=lambda t, on_done: on_done(SimpleNamespace(ok=False, message="timeout", fail={})))
        b._a_rga_scan(SimpleNamespace())
        chats = [x.detail for x in e1.events if x.kind == "chat"]
        assert chats == ["[OES] 실패: RuntimeError('OES 링크 없음')", "[RGA1] 측정 실패: timeout → 건너뜀"]
        assert all(x.level == "warn" for x in e1.events if x.kind == "chat")
    finally:
        rig.stop()
        rig.clock.close()


def _run_test_row(tmp_path, at8):
    rig = R.make(tmp_path, chambers=("ch1",))
    e = rig.eng["ch1"]
    e.load([{"#": "test", "Process_name": "T1", "time": "60s"}], source="local", name="t")
    R.run(rig, R.idle(e), max_ticks=200, hooks=[R.at(2, lambda: e.start(owner="local")), R.at(8, lambda: at8(e))])
    return [(x.kind, x.detail) for x in e.events if x.kind in ("chat", "run_end")]


def test_chat_fail_reason_text_only_for_real_failure(tmp_path):
    """CHAT-019 — chamber_runtime.py:2006-2027: 진짜 실패(ok=False·stopped 아님)면 종료 뒤 '❌ CHn 공정 실패 이유: {reason}' 글,
    사용자 STOP 은 글 없음. 장비 이벤트 순서: chat(실패 이유) → run_end(포털이 글을 종료 카드 뒤로 3 s 늦춤 — 골든 ch2_error_run_errors)."""
    kinds = _run_test_row(tmp_path / "a", lambda e: e.abort("SAFETY_TRIP", "S9 DC 과전류"))
    fail = [d for k, d in kinds if k == "chat" and d.startswith("❌ CH1 공정 실패 이유: ")]
    assert len(fail) == 1, kinds
    assert [k for k, _ in kinds][-2:] == ["chat", "run_end"]
    kinds = _run_test_row(tmp_path / "b", lambda e: e.stop(source="local", user=True))
    assert kinds and kinds[-1][0] == "run_end" and not any(d.startswith("❌") for k, d in kinds if k == "chat"), kinds
