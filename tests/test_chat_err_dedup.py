# -*- coding: utf-8 -*-
"""ChatNotifier.notify_error_event 일반 중복 억제 검증 (네트워크 없음, 웹훅 더미).

코드는 비(非)링크 코드(E501/E502)를 쓴다 — CHAT_PLC_LINK_CODES(E401/E402)는 2026-09-23 정책상
링크 상태와 무관하게 항상 억제되므로 일반 dedup 경로 검증에 쓸 수 없다.

1) 같은 (src, code, cause) 가 창 안에 5회 오면 _post_card 는 1회만 호출된다
2) 창 만료 후 같은 키가 오면 " 반복" 요약 문구가 포함된 카드가 나간다
3) code 나 cause 가 다르면 각각 즉시 전송된다
4) CHAT_ERR_DEDUP_S = 0 이면 5회 모두 전송된다(기존 동작 보존)
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


class _Clock:
    def __init__(self):
        self.t = 5000.0

    def __call__(self):
        return self.t


@pytest.fixture
def env(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(CN.time, "monotonic", clock)
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 60.0, raising=False)
    n = CN.ChatNotifier(webhook_url=None)
    n.webhook_default = ""                       # 실제 URL 이 config_local 에 있어도 무효화
    cards = []

    def _fake_post(title, subtitle="", status="INFO", fields=None, urgent=False, **kw):
        cards.append({"title": title, "subtitle": subtitle, "status": status,
                      "fields": fields, "urgent": urgent})
    monkeypatch.setattr(n, "_post_card", _fake_post)
    return clock, n, cards


def test_1_same_key_in_window_posts_once(env):
    clock, n, cards = env
    for _ in range(5):
        n.notify_error_event("PLC", "E501", "PLC 재연결 대기 중 (backoff)")
        clock.t += 1.0
    assert len(cards) == 1, cards
    assert cards[0]["urgent"] is True and cards[0]["status"] == "FAIL"
    assert cards[0]["subtitle"] == "[PLC] E501 | PLC 재연결 대기 중 (backoff)"


def test_2_summary_after_window(env):
    clock, n, cards = env
    for _ in range(5):
        n.notify_error_event("PLC", "E501", "PLC 재연결 대기 중 (backoff)")
        clock.t += 1.0
    clock.t += 60.0
    n.notify_error_event("PLC", "E501", "PLC 재연결 대기 중 (backoff)")
    assert len(cards) == 2, cards
    sub = cards[1]["subtitle"]
    assert sub.startswith("[PLC] E501 | PLC 재연결 대기 중 (backoff)")
    assert " 반복" in sub and "같은 오류 4회" in sub, sub
    # 새 사이클: 바로 다음 재발은 다시 억제
    clock.t += 1.0
    n.notify_error_event("PLC", "E501", "PLC 재연결 대기 중 (backoff)")
    assert len(cards) == 2


def test_2b_expired_other_key_flushed_on_next_call(env):
    clock, n, cards = env
    for _ in range(3):
        n.notify_error_event("PLC", "E501", "x")
    clock.t += 61.0
    n.notify_error_event("RGA", "E500", "y")          # 다른 키 호출 시점에 만료 키 요약이 먼저
    assert len(cards) == 3, cards
    assert "[PLC] E501 | x" in cards[1]["subtitle"] and "같은 오류 2회" in cards[1]["subtitle"]
    assert cards[2]["subtitle"] == "[RGA] E500 | y"


def test_3_different_code_or_cause_sent_immediately(env):
    clock, n, cards = env
    n.notify_error_event("PLC", "E501", "a")
    n.notify_error_event("PLC", "E502", "a")
    n.notify_error_event("PLC", "E501", "b")
    n.notify_error_event("MFC", "E501", "a")
    assert len(cards) == 4
    assert all(" 반복" not in c["subtitle"] for c in cards)


def test_4_window_zero_keeps_legacy_behaviour(env, monkeypatch):
    clock, n, cards = env
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 0, raising=False)
    for _ in range(5):
        n.notify_error_event("PLC", "E501", "PLC 재연결 대기 중 (backoff)")
    assert len(cards) == 5
    assert all(c["subtitle"] == "[PLC] E501 | PLC 재연결 대기 중 (backoff)" for c in cards)
    assert n._err_dedup == {}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
