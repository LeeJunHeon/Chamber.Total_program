"""B — 정상 종료 끝 표시(챔버K W-25 C1 uplink.bye·app 과 같게).

정상 종료(Ctrl+C·SIGTERM·창 닫기) 때 메시지 없는 POST 에 "bye": true — 웹이 이 실행의 주인 자리를 바로 비워 다시 켠 실행이
'같은 장비 ID 의 다른 실행이 연결 중'(409)으로 기다리지 않는다. 보내던 POST 뒤에 가고, 보낸 뒤에는 다시 POST 하지 않는다.
창 닫기는 남은 시간 안에서만. 가짜 웹(fake_web)으로.
"""
import threading
import time

from v2.tests.fake_web import FakeWeb
from v2.tests.test_uplink import Rigs


def _wait(pred, limit=10.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < limit:
        if pred():
            return True
        time.sleep(0.05)
    return pred()


def test_bye_is_last_post_and_nothing_after(tmp_path):
    web = FakeWeb().up()
    r = Rigs()
    try:
        rig, up = r.make(tmp_path, web)
        up.start()
        assert _wait(lambda: len([p for p in web.posts if p["code"] == 200]) >= 2)
        up.stop()
        n0 = len(web.posts)
        assert up.bye() is True and up.bye_sent
        assert len(web.posts) == n0 + 1
        last = web.posts[-1]
        assert last["bye"] is True and last["n"] == 0 and last["code"] == 200 and last["instance"] == rig.hub.instance
        assert up.send_once() is False                                   # 보낸 뒤에는 POST 하지 않는다
        assert up.bye() is False                                         # 두 번 보내지 않는다
        time.sleep(1.0)
        assert len(web.posts) == n0 + 1
    finally:
        r.close()
        web.down()


def test_bye_goes_after_the_post_in_flight(tmp_path):
    web = FakeWeb().up()
    r = Rigs()
    try:
        rig, up = r.make(tmp_path, web)
        web.delay_s = 0.8                                                # 보내던 POST 가 0.8 s 걸림
        t = threading.Thread(target=lambda: up.send_once(), daemon=True)
        t.start()
        time.sleep(0.2)                                                  # POST 가 나가는 중
        t0 = time.monotonic()
        ok = up.bye(timeout=2.0)
        t.join(5)
        assert ok and time.monotonic() - t0 >= 0.5                      # 보내던 POST 가 끝난 뒤
        assert [p["bye"] for p in web.posts][-2:] == [False, True]
    finally:
        r.close()
        web.down()


def test_bye_failure_does_not_delay_exit(tmp_path):
    r = Rigs()
    web = FakeWeb().up()
    try:
        rig, up = r.make(tmp_path / "a", web)
        web.down()                                                       # 웹이 없음 — 바로 실패
        t0 = time.monotonic()
        assert up.bye(timeout=2.0) is False and time.monotonic() - t0 < 2.5
        web.up()
        web.delay_s = 3.0                                                # 웹이 답하지 않음 — timeout 안에서 포기
        rig2, up2 = r.make(tmp_path / "b", web)
        t0 = time.monotonic()
        assert up2.bye(timeout=1.0) is False and time.monotonic() - t0 < 2.0
        rig3, up3 = r.make(tmp_path / "c", web)
        assert up3._post_lock.acquire()                                  # 보내던 POST 가 끝나지 않음 — 기다리다 포기(POST 없음)
        t0 = time.monotonic()
        assert up3.bye(timeout=0.5) is False and time.monotonic() - t0 < 1.0 and not up3.bye_sent
        up3._post_lock.release()
        time.sleep(3.2)                                                  # 늦게 답한 가짜 웹 스레드가 끝나게
    finally:
        r.close()
        web.down()


def test_app_sends_bye_on_shutdown_only_when_uplink_is_used(tmp_path):
    from v2.ch12 import app as A
    from v2.tests.test_app_web import _in_process
    web = FakeWeb().up()
    try:
        app, _lines = _in_process(tmp_path / "on", web, UPLINK_IN_SIM=True)
        assert app.start() == A.EXIT_OK
        assert _wait(lambda: any(p["code"] == 200 for p in web.posts))
        t0 = time.monotonic()
        app.shutdown()
        assert time.monotonic() - t0 < 8.0
        assert web.posts[-1]["bye"] is True and sum(p["bye"] for p in web.posts) == 1
        n = len(web.posts)
        time.sleep(1.0)
        assert len(web.posts) == n                                       # 끝 표시 뒤 POST 없음
        web2 = FakeWeb().up()
        app2, _ = _in_process(tmp_path / "off", web2, UPLINK_IN_SIM=False, EQUIPMENT_ID="CH12")
        assert app2.start() == A.EXIT_OK and app2.uplink is None
        app2.shutdown()
        assert web2.posts == []                                          # 웹 연결을 쓰지 않으면 보내지 않는다
        web2.down()
    finally:
        web.down()


def test_console_close_sends_bye_within_budget():
    from v2.ch12 import app as A
    src = open(A.__file__, encoding="utf-8").read()
    i = src.index("def on_console(why: str)")
    body = src[i:src.index("self.shell = winshell.install", i)]
    assert "CONSOLE_EXIT_BUDGET_S - (time.monotonic() - t0)" in body and "if left > 0.3:" in body
    assert "self.uplink.bye(timeout=min(1.0, left))" in body and body.index("exit_now(why)") < body.index("uplink.bye")
    assert A.CONSOLE_EXIT_BUDGET_S == 5.0
