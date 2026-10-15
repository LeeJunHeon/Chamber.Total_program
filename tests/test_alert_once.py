# -*- coding: utf-8 -*-
"""구글챗 알림 반복 방지 — 네트워크 없이 전송 직전(_schedule_post)을 가로채 확인한다.

1) 바로 보낸(flush) '장비 오류' 카드는 공정 종료 때 다시 나가지 않는다(종료 카드·실패 이유 텍스트는 그대로)
2) 공정 중 오류가 두 번 바로 나가면 두 번째 카드에는 새 오류만 실린다
3) 모아 두었다가(flush 없음) 종료 때 보내는 기존 방식은 그대로 한 장
4) 종료 카드 '원인'은 이미 보낸 오류로도 채운다(detail 에 errors 가 없을 때)
5) 프로그램 종료(shutdown) 때도 이미 보낸 오류를 다시 보내지 않는다 / 즉시 모드(set_defer(False))도 같다
6) 카메라 외부 알림은 한 공정에 한 번 — 새 공정(참여자)이 start 하면 다시 한 번 허용
"""
import os
import sys
import time
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                       # noqa: E402
from controller.chat_notifier import ChatNotifier   # noqa: E402
from util.camera_recorder import CameraRecorder     # noqa: E402

E1 = "⚠️ CH2 DC Power 과전류 차단 → 전체 공정 중단 | DC 과전류: meas=1.662A > 한계 1.000A"
E2 = "⚠ 정리 중 DC1 출력 OFF 미확인 — 장비 출력 상태 확인 필요"
CAUSE = "Step 14 DC_POWER_SET: [DC Power - DC 과전류]"


def _label(pl: dict) -> tuple:
    """보낸 내용 → ('TEXT', 글) 또는 (카드 제목, 본문, 원인)."""
    if "text" in pl:
        return ("TEXT", pl["text"])
    w = pl["cardsV2"][0]["card"]["sections"][0]["widgets"]
    title = w[0]["textParagraph"]["text"].replace("<b>", "").replace("</b>", "")
    body = w[1].get("textParagraph", {}).get("text", "") if len(w) > 1 else ""
    cause = [x["decoratedText"]["text"] for x in w
             if "decoratedText" in x and x["decoratedText"].get("topLabel") == "원인"]
    return (title, body, cause[0] if cause else "")


def _run(steps, defer=True):
    """공정 시작 → steps → 보낸 순서대로 라벨 목록(공정 시작 카드 제외)."""
    posts = []

    async def go():
        c = ChatNotifier("https://example.invalid/hook")
        c._schedule_post = lambda pl, wh, urgent=False: posts.append(_label(pl))
        if not defer:
            c.set_defer(False)
        c.notify_process_started({"process_name": "Sc pre", "ch": 2})
        c.flush()
        for s in steps:
            s(c)
    asyncio.run(go())
    return posts[1:]


def _err_now(e):
    """런타임 _pump_dc_events 와 같은 호출 — 바로 보냄."""
    return lambda c: (c.notify_error_with_src("DC1", e), c.flush())


def _finish(errors=True):
    """런타임 finished 핸들러와 같은 호출 순서 — 종료 카드 → flush → 실패 이유 텍스트 → flush."""
    detail = {"process_name": "Sc pre (1/2)", "ch": 2}
    if errors:
        detail["errors"] = [CAUSE]

    def _f(c):
        c.notify_process_finished_detail(False, detail)
        c.flush()
        c.notify_text(f"❌ CH2 공정 실패 이유: {CAUSE}")
        c.flush()
    return _f


def _errors(posts):
    return [p for p in posts if p[0].endswith("장비 오류")]


def test_immediate_error_card_not_resent_at_finish():
    posts = _run([_err_now(E1), _finish()])
    assert [p[0] for p in posts] == ["❌ 장비 오류", "❌ 공정 종료", "TEXT"]
    assert E1 in posts[0][1] and CAUSE in posts[1][2]


def test_second_immediate_error_carries_only_new_one():
    posts = _run([_err_now(E1), _err_now(E2), _finish()])
    errs = _errors(posts)
    assert len(errs) == 2
    assert E1 in errs[0][1] and E2 not in errs[0][1]
    assert E2 in errs[1][1] and E1 not in errs[1][1]


def test_deferred_errors_sent_once_with_finish():
    posts = _run([lambda c: c.notify_error_with_src("DC1", E1),
                  lambda c: c.notify_error_with_src("DC1", E2), _finish()])
    errs = _errors(posts)
    assert len(errs) == 1 and E1 in errs[0][1] and E2 in errs[0][1]


def test_finish_cause_falls_back_to_sent_errors():
    posts = _run([_err_now(E1), _finish(errors=False)])
    fin = [p for p in posts if p[0] == "❌ 공정 종료"][0]
    assert E1 in fin[2]
    assert len(_errors(posts)) == 1


def test_shutdown_does_not_resend():
    posts = _run([_err_now(E1), lambda c: c.shutdown()])
    assert len(_errors(posts)) == 1


def test_immediate_mode_notifier_sends_once():
    posts = _run([_err_now(E1), _finish()], defer=False)
    assert len(_errors(posts)) == 1


# ───────────────────────── 카메라 ─────────────────────────
def _fake_loop(self):
    while not self._stop_event.is_set():
        time.sleep(0.01)


@pytest.fixture
def rec(monkeypatch):
    monkeypatch.setattr(CameraRecorder, "_record_loop", _fake_loop)
    monkeypatch.setattr(CameraRecorder, "_load_config", lambda self: None)
    calls = []
    r = CameraRecorder(camera_index=99, notify_cb=calls.append)
    r.calls = calls
    yield r
    r.stop()
    if r._thread:
        r._thread.join(timeout=1.0)


def test_camera_notify_once_per_process(rec):
    rec.start(owner="chamber2")
    rec._notify("디스크 여유 부족(15.0GB < 20.0GB)")
    rec._notify("디스크 여유 부족(14.9GB < 20.0GB) — 촬영 중단")
    assert rec.calls == ["디스크 여유 부족(15.0GB < 20.0GB)"]
    rec.start(owner="chamber2")                       # 같은 공정이 다시 불러도 그대로
    rec._notify("again")
    assert len(rec.calls) == 1
    rec.start(owner="chamber1")                       # 다른 공정이 붙으면 그 공정 몫으로 한 번 더
    rec._notify("chamber1 공정")
    assert rec.calls[-1] == "chamber1 공정" and len(rec.calls) == 2
    rec.stop(owner="chamber1")
    rec.stop(owner="chamber2")
    rec.start(owner="chamber2")                       # 다음 공정
    rec._notify("다음 공정")
    assert rec.calls[-1] == "다음 공정" and len(rec.calls) == 3


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
