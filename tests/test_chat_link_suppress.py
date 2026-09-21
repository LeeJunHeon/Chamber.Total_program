# -*- coding: utf-8 -*-
"""PLC 링크 다운 중 연결계(E401/E402) 채팅 카드 억제 검증 (네트워크 없음, 웹훅 더미).

1) 링크 다운 상태에서 chat_host/chat_ch1 역할 notifier 가 E401 20 + E402 20 을 보내도 _post_card 0회
2) 같은 상태에서 link_event=True 의 E401 1건은 정상 전송
3) 같은 상태에서 E301(공정) 카드는 정상 전송
4) 1) 직후 consume_suppressed() == (40, {"E401":20,"E402":20}), 두 번째는 (0, {})
5) 끊김→복구 전체 시나리오에서 _post_card 총 2회(끊김 1 + 복구 1), 복구 subtitle 에 억제 건수 문구
6) CHAT_PLC_LINK_SUPPRESS=False 면 40건 모두 전송(기존 동작 보존)
"""
import os
import sys
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                       # noqa: E402
import controller.chat_notifier as CN               # noqa: E402
from lib import config_common as cfgc               # noqa: E402
from lib import link_state                          # noqa: E402


def _mk_notifier(monkeypatch, cards):
    n = CN.ChatNotifier(webhook_url=None)
    n.webhook_default = ""                       # config_local 의 실제 URL 무효화

    def _fake_post(title, subtitle="", status="INFO", fields=None, urgent=False, **kw):
        cards.append({"title": title, "subtitle": subtitle, "status": status, "urgent": urgent})
    monkeypatch.setattr(n, "_post_card", _fake_post)
    return n


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setattr(cfgc, "CHAT_PLC_LINK_SUPPRESS", True, raising=False)
    monkeypatch.setattr(cfgc, "CHAT_PLC_LINK_CODES", ["E401", "E402"], raising=False)
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 0, raising=False)   # 이 테스트는 링크 억제만 본다
    link_state.set_plc_link_down(False)
    link_state.consume_suppressed()
    yield monkeypatch
    link_state.set_plc_link_down(False)
    link_state.consume_suppressed()


def _spam(n_host, n_ch1):
    for i in range(20):
        n_host.notify_error_event("HOST", "E401", f"PLC 재연결 대기 중 (backoff) #{i}")
        n_ch1.notify_error_event("CH1", "E402", f"PLC 응답 없음 #{i}")


def test_1_link_down_blocks_e401_e402(env):
    host_cards, ch1_cards = [], []
    n_host = _mk_notifier(env, host_cards)
    n_ch1 = _mk_notifier(env, ch1_cards)
    link_state.set_plc_link_down(True)
    _spam(n_host, n_ch1)
    assert host_cards == [] and ch1_cards == []
    assert link_state.suppressed_total() == 40


def test_2_link_event_passes(env):
    cards = []
    n = _mk_notifier(env, cards)
    link_state.set_plc_link_down(True)
    n.notify_error_event("PLC", "E401", "[CH1&2] PLC 연결 끊김 60초 경과", link_event=True)
    assert len(cards) == 1
    assert cards[0]["subtitle"] == "[PLC] E401 | [CH1&2] PLC 연결 끊김 60초 경과"
    assert link_state.suppressed_total() == 0


def test_3_process_code_not_blocked(env):
    cards = []
    n = _mk_notifier(env, cards)
    link_state.set_plc_link_down(True)
    n.notify_error_event("CH1", "E301", "공정 중단: 압력 이탈")
    n.notify_error_event("CH1", "E403", "Modbus 프로토콜 오류")   # 목록 밖 코드
    assert len(cards) == 2
    assert link_state.suppressed_total() == 0


def test_4_consume_suppressed(env):
    n_host = _mk_notifier(env, [])
    n_ch1 = _mk_notifier(env, [])
    link_state.set_plc_link_down(True)
    _spam(n_host, n_ch1)
    assert link_state.consume_suppressed() == (40, {"E401": 20, "E402": 20})
    assert link_state.consume_suppressed() == (0, {})
    assert link_state.format_suppressed(40, {"E401": 20, "E402": 20}) == "E401 20건, E402 20건"
    assert link_state.format_suppressed(0, {}) == ""


def test_5_full_cycle_exactly_two_cards(env):
    """main._on_plc_conn_change 와 동일한 로직으로 끊김→복구 시나리오."""
    plc_cards, host_cards, ch1_cards = [], [], []
    chat_plc = _mk_notifier(env, plc_cards)
    n_host = _mk_notifier(env, host_cards)
    n_ch1 = _mk_notifier(env, ch1_cards)

    def _on_conn_change(connected, detail):
        if connected:
            total, by_code = link_state.consume_suppressed()
            sub = detail
            if total > 0:
                sub += (f" | 끊긴 동안 억제된 연결 알림 {total}건 "
                        f"({link_state.format_suppressed(total, by_code)})")
            chat_plc._post_card("PLC 재연결", subtitle=sub, status="SUCCESS", urgent=True)
        else:
            chat_plc.notify_error_event("PLC", "E401", detail, link_event=True)

    # 끊김 감지(plc._mark_conn_fail 첫 회) → 로봇 명령 실패 폭주 → 임계 도달 끊김 카드 → 폭주 → 복구
    link_state.set_plc_link_down(True)
    _spam(n_host, n_ch1)
    _on_conn_change(False, "[CH1&2] PLC 연결 끊김 60초 경과, 재연결 실패 (192.168.1.10:502)")
    _spam(n_host, n_ch1)
    link_state.set_plc_link_down(False)
    _on_conn_change(True, "[CH1&2] PLC 재연결 성공 (192.168.1.10:502)")

    assert host_cards == [] and ch1_cards == []
    assert len(plc_cards) == 2, plc_cards
    assert plc_cards[0]["status"] == "FAIL" and "연결 끊김" in plc_cards[0]["subtitle"]
    assert plc_cards[1]["title"] == "PLC 재연결" and plc_cards[1]["status"] == "SUCCESS"
    assert "억제된 연결 알림 80건 (E401 40건, E402 40건)" in plc_cards[1]["subtitle"], plc_cards[1]
    assert link_state.suppressed_total() == 0
    # 복구 후에는 다시 정상 전송
    n_host.notify_error_event("HOST", "E401", "x")
    assert len(host_cards) == 1


def test_6_suppress_off_keeps_legacy(env):
    env.setattr(cfgc, "CHAT_PLC_LINK_SUPPRESS", False, raising=False)
    host_cards, ch1_cards = [], []
    n_host = _mk_notifier(env, host_cards)
    n_ch1 = _mk_notifier(env, ch1_cards)
    link_state.set_plc_link_down(True)
    _spam(n_host, n_ch1)
    assert len(host_cards) == 20 and len(ch1_cards) == 20
    assert link_state.suppressed_total() == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
