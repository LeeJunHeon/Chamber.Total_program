# -*- coding: utf-8 -*-
"""PLC 링크 챗 카드 = 끊김 1회당 정확히 2장 (2026-09-23 10:47 사건 재현 포함).

실제 코드 경로로 검증한다: device/plc.py 의 _run_io / _connect_sync / 하트비트,
lib/link_state.py 의 전이 판정, controller/chat_notifier.py 의 억제.
"""
import os
import sys
import time
import asyncio
import threading
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402
from lib import link_state                                       # noqa: E402
from lib import config_common as cfgc                            # noqa: E402
import device.plc as PLC                                         # noqa: E402
from device.plc import PLCError                                  # noqa: E402
from pymodbus.exceptions import ConnectionException, ModbusIOException   # noqa: E402
from tests.test_plc_link_policy import _mk, _reset_cfg, _set_cfg, FakeClient, _Resp   # noqa: E402


# ───────────────────────── 하네스 ─────────────────────────
class _Cards:
    """link_state 발송기 + ChatNotifier 를 실제로 엮은 수집기."""

    def __init__(self, monkeypatch):
        self.cards = []          # (title, subtitle, status)
        import controller.chat_notifier as CN
        n = CN.ChatNotifier(webhook_url=None)
        n.webhook_default = ""
        n._defer = False
        monkeypatch.setattr(
            n, "_post_card",
            lambda title, subtitle="", status="INFO", fields=None, urgent=False, **k:
                self.cards.append((title, subtitle, status)))
        self.notifier = n
        link_state.set_emitter(self._emit)

    def _emit(self, kind, info):
        # main.py 의 _emit_link_card 와 같은 내용(발송 스레드 검증은 test_6 에서 별도로)
        self.threads = getattr(self, "threads", [])
        self.threads.append(threading.get_ident())
        if kind == "down":
            sub = f"[CH1&2] PLC 연결 끊김 | 사유={info.get('reason')} | op={info.get('op')}"
            self.notifier._post_card("PLC 연결 끊김", subtitle=sub, status="FAIL", urgent=True)
        else:
            sub = f"{info.get('detail')} | 끊김 {info.get('elapsed_s'):.1f}초"
            tot = int(info.get("suppressed_total") or 0)
            if tot > 0:
                sub += (f" | 끊긴 동안 억제된 연결 알림 {tot}건 "
                        f"({link_state.format_suppressed(tot, info.get('suppressed_by_code') or {})})")
            self.notifier._post_card("PLC 재연결", subtitle=sub, status="SUCCESS", urgent=True)

    def titles(self):
        return [t for t, _, _ in self.cards]

    def host_error(self, code="E401", detail="Modbus TCP 연결 실패", src="HOST"):
        """errors/error_reporter 와 같은 진입점(notify_error_event)으로 호스트 오류 리포트."""
        self.notifier.notify_error_event(src, code, detail)


@pytest.fixture
def env(monkeypatch):
    _reset_cfg()
    _saved = {k: getattr(cfgc, k, None) for k in ("PLC_RECONNECT_DELAY_S", "PLC_RECONNECT_RETRY")}
    monkeypatch.setattr(cfgc, "CHAT_PLC_LINK_SUPPRESS", True, raising=False)
    monkeypatch.setattr(cfgc, "CHAT_PLC_LINK_CODES", ["E401", "E402"], raising=False)
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 0, raising=False)
    monkeypatch.setattr(cfgc, "DEV_MODE", False, raising=False)
    link_state.set_plc_link_down(False)
    link_state.consume_suppressed()
    c = _Cards(monkeypatch)
    yield c
    link_state.set_emitter(None)
    link_state.set_plc_link_down(False)
    link_state.consume_suppressed()
    for _k, _v in _saved.items():            # 다른 테스트로 새지 않게 원복
        if _v is not None:
            setattr(cfgc, _k, _v)
    _reset_cfg()


# ───────────────────────── 1. 10:47 재현 ─────────────────────────
def test_1_incident_20260923_1047(env):
    """저우선 RST → 고우선 소켓 재생성·접속 실패 → 호스트 E401 4건 → 재접속 성공 ⇒ 카드 2장."""
    p, logs = _mk()
    asyncio.run(p.read_coil(1))                       # 소켓 확보 (링크 up)
    assert env.cards == []

    # 10:47:00 코일 로거(저우선) 읽기 → RST 10054. 저우선은 감지만 하고 재접속하지 않는다
    cli = _cur = p._client
    connects = FakeClient.connect_calls
    FakeClient.script = [ConnectionResetError(10054, "기존 연결이 원격 호스트에 의해 강제로 끊겼습니다")]
    with pytest.raises(PLCError):
        asyncio.run(p.read_coils_block(0, 4, priority="low"))
    assert link_state.is_plc_link_down() is True, "첫 감지 즉시 down"
    assert env.titles() == ["PLC 연결 끊김"]
    assert p._client is cli and FakeClient.connect_calls == connects, "저우선은 재접속/close 안 함"

    # 10:47:00 로봇 GET_SPUTTER_STATUS → 고우선 read_coil: 소켓 재생성 + 재접속 실패
    FakeClient.script = [ConnectionResetError(10054, "reset")]
    FakeClient.connect_ok = False
    with pytest.raises(PLCError):
        asyncio.run(p.read_coil(1))
    assert any("소켓 재생성" in m for m in logs)

    # 10:47:02 / 10:47:06 호스트 응답 E401 4건 (진단중 1 + backoff 3)
    for detail in ("Modbus TCP 연결 실패 (192.168.1.2:502) (시도 1회, 분류=진단중)",
                   "PLC 재연결 대기 중 (backoff)",
                   "PLC 재연결 대기 중 (backoff)",
                   "PLC 재연결 대기 중 (backoff)"):
        env.host_error("E401", detail)
    assert env.titles() == ["PLC 연결 끊김"], "호스트 E401 개별 카드는 0장"
    assert link_state.suppressed_total() == 4

    # 10:47:05 하트비트의 링크 끊김 감지 — 이미 down 이므로 카드 추가 없음
    p._mark_conn_fail()
    assert env.titles() == ["PLC 연결 끊김"]

    # 10:47:11 재접속 성공 (하트비트 경로)
    FakeClient.connect_ok = True
    asyncio.run(p.read_coil(1))
    p._mark_conn_ok()
    assert env.titles() == ["PLC 연결 끊김", "PLC 재연결"]
    up = env.cards[1][1]
    assert "억제된 연결 알림 4건" in up and "E401 4건" in up, up
    assert env.cards[0][2] == "FAIL" and env.cards[1][2] == "SUCCESS"
    assert link_state.is_plc_link_down() is False and link_state.suppressed_total() == 0


# ───────────────────────── 2. 장기 장애 ─────────────────────────
def test_2_long_outage_is_still_two_cards(env):
    p, logs = _mk()
    asyncio.run(p.read_coil(1))
    # 재시도 지연 0 (_apply_cfg_from_config 가 connect() 마다 config 값을 다시 읽는다)
    _set_cfg(PLC_RECONNECT_BACKOFF_S=0.0, PLC_RECONNECT_DELAY_S=0.0, PLC_RECONNECT_RETRY=0)
    # 링크 상실(RST) 로 시작 → 이후 접속 실패가 계속된다
    FakeClient.script = [ConnectionResetError(10054, "reset")]
    FakeClient.connect_ok = False
    with contextlib.suppress(PLCError):
        asyncio.run(p.read_coil(1))
    assert env.titles() == ["PLC 연결 끊김"]
    # 27시간 장애를 흉내: 접속 실패 + 호스트 오류를 반복
    for i in range(200):
        with contextlib.suppress(PLCError):
            asyncio.run(p.connect())
        env.host_error("E401", "PLC 재연결 대기 중 (backoff)")
        if i % 3 == 0:
            p._mark_conn_fail()
    assert env.titles() == ["PLC 연결 끊김"], env.titles()
    FakeClient.connect_ok = True
    asyncio.run(p.connect())
    assert env.titles() == ["PLC 연결 끊김", "PLC 재연결"]
    assert "억제된 연결 알림 200건" in env.cards[1][1]
    _reset_cfg()


# ───────────────────────── 3. 플래핑 ─────────────────────────
def test_3_flapping_three_cycles(env):
    p, logs = _mk()
    asyncio.run(p.read_coil(1))
    for _ in range(3):
        FakeClient.script = [ConnectionResetError(10054, "reset")]
        FakeClient.connect_ok = False
        with contextlib.suppress(PLCError):
            asyncio.run(p.read_coil(1))
        FakeClient.connect_ok = True
        asyncio.run(p.read_coil(1))
    assert env.titles() == ["PLC 연결 끊김", "PLC 재연결"] * 3, env.titles()


# ───────────────────────── 4. 단발 E402 ─────────────────────────
def test_4_single_e402_no_card(env):
    p, logs = _mk()
    asyncio.run(p.read_coil(1))
    FakeClient.script = [ModbusIOException("no resp"), _Resp([True])]
    asyncio.run(p.read_coil(1))                       # 같은 소켓 재시도 성공
    assert env.cards == [] and link_state.is_plc_link_down() is False
    # 저우선 단발 타임아웃도 down 이 아니다
    FakeClient.script = [ModbusIOException("no resp")]
    with pytest.raises(PLCError):
        asyncio.run(p.read_coils_block(0, 4, priority="low"))
    assert env.cards == [] and link_state.is_plc_link_down() is False
    # 링크가 살아 있어도 E402 개별 카드는 억제된다(건수만)
    env.host_error("E402", "PLC 응답 없음")
    assert env.cards == [] and link_state.suppressed_total() == 1


# ───────────────────────── 5. 부팅/종료 ─────────────────────────
def test_5_boot_and_shutdown(env):
    # 부팅 성공 → 카드 없음
    p, logs = _mk()
    asyncio.run(p.connect())
    assert env.cards == []
    # 정상 종료(close) → 카드 없음
    asyncio.run(p._locked_thread(p._close_sync))
    assert env.cards == []
    # 부팅 실패 후 성공 → 2장
    p2, _ = _mk()
    FakeClient.connect_ok = False
    with contextlib.suppress(PLCError):
        asyncio.run(p2.connect())
    assert env.titles() == ["PLC 연결 끊김"]
    FakeClient.connect_ok = True
    asyncio.run(p2.connect())
    assert env.titles() == ["PLC 연결 끊김", "PLC 재연결"]


# ───────────────────────── 6. 워커 스레드 전이 → 루프 스레드 발송 ─────────────────────────
def test_6_worker_thread_transition_marshalled(monkeypatch):
    """main.py 의 발송기와 같은 구조(call_soon_threadsafe)로 마샬링되는지."""
    link_state.set_plc_link_down(False)
    link_state.consume_suppressed()
    sent = []

    async def _main():
        loop = asyncio.get_running_loop()
        loop_tid = threading.get_ident()

        def _emitter(kind, info):
            def _send():
                sent.append((kind, threading.get_ident()))
            try:
                loop.call_soon_threadsafe(_send)
            except Exception:
                _send()
        link_state.set_emitter(_emitter)
        wtid = {}

        def _worker():                          # _connect_sync 는 to_thread 에서 돈다
            wtid["id"] = threading.get_ident()
            link_state.note_link_down("RST/연결 끊김", "read_coil")
        t = threading.Thread(target=_worker, name="FakePLCWorker")
        t.start(); t.join(2.0)
        assert sent == [], "워커 스레드에서 직접 발송하면 안 된다"
        for _ in range(20):
            if sent:
                break
            await asyncio.sleep(0.01)
        assert len(sent) == 1 and sent[0] == ("down", loop_tid) and loop_tid != wtid["id"]
    try:
        asyncio.run(_main())
    finally:
        link_state.set_emitter(None)
        link_state.set_plc_link_down(False)


# ───────────────────────── 7. 공정 카드는 억제 안 됨 ─────────────────────────
def test_7_process_cards_not_suppressed(env):
    link_state.note_link_down("RST", "read_coil")     # 링크 down 상태에서도
    env.host_error("E301", "Gate 체크 실패 → 공정 시작 차단", src="CH1")
    env.host_error("E403", "Modbus 프로토콜 오류", src="CH2")
    titles = env.titles()
    assert titles.count("장비 오류") == 2, titles
    assert titles[0] == "PLC 연결 끊김"
    # 링크 코드만 억제
    env.host_error("E401", "연결 오류")
    assert env.titles().count("장비 오류") == 2 and link_state.suppressed_total() == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
