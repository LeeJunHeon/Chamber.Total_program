# tests/test_client_process_chat.py
# -*- coding: utf-8 -*-
"""
클라이언트(로봇) 요청 공정 전용 구글챗 방 — 시작 / 종료 / 에러 검증 (네트워크 없음).

A. util/host_process_log 기록 이벤트
   a1 시작 시각이 처음 기록될 때만 started 1회
   a2 finalize 는 기록키당 finished 1회(멱등 재호출은 통지 없음)
   a3 reject → finished(거절), 시작 시각 없음
   a4 startup_recover → finished(중단(재시작)) — 공정 중 / 시작 전 사유 구분
   a5 수신자가 예외를 내도 기록·반환값은 그대로
   a6 수신자는 락 밖에서 불린다(다른 스레드가 _lk 를 잡을 수 있다)
   a7 수신자 유무와 무관하게 CSV 줄은 똑같다
   a8 pending 기록 실패(메모리 큐 경로)에도 통지 1회, 재호출 시 중복 없음
   a9 수신자가 없으면 통지 상태를 쌓지 않는다
B. ChatNotifier.notify_client_process 카드
   b1 시작 카드  b2 정상 종료  b3 STOP 은 종료(⚠️)  b4 에러 4종
   b5 같은 에러는 창 안에서 1장, 창 뒤 재발 시 '반복' 칸  b6 공정명 규칙
   b7 이상한 입력에도 예외 없음  b8 urgent 전송(지연 버퍼를 타지 않음)·자기 웹훅으로만
   b9 _post_card 아이콘(기존 INFO/SUCCESS/FAIL 그대로 + WARN)
C. 연결
   c1 기록 → 카드 끝까지(요청 1건 = 시작+종료, 거절 = 에러 1장)
   c2 main.py 연결(별도 웹훅·이벤트 루프 마샬링·종료 정리 목록)

pytest 로도, `python tests/test_client_process_chat.py` 로도 실행된다.
"""
from __future__ import annotations

import contextlib
import csv
import os
import sys
import tempfile
import threading
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import controller.chat_notifier as CN                    # noqa: E402
from lib import config_common as cfgc                    # noqa: E402
from util.host_process_log import (                      # noqa: E402
    HostProcessLog, RESULT_SUCCESS, RESULT_FAIL, RESULT_STOP,
    RESULT_REJECT, RESULT_RESTART, RESULT_UNKNOWN,
)

_URL = "https://example.invalid/client-process"


# ─────────────────────────── 헬퍼 ───────────────────────────
def _setup(tmp: Path) -> HostProcessLog:
    """임시 폴더를 NAS/로컬로 지정한 새 인스턴스(워커 없음)."""
    cfgc.HOST_LOG_ENABLED = True
    cfgc.HOST_LOG_NAS_DIR = str(tmp / "nas")
    cfgc.HOST_LOG_LOCAL_DIR = str(tmp / "local")
    cfgc.HOST_LOG_RETRY_S = 30
    cfgc.HOST_LOG_LOCK_STALE_S = 120
    h = HostProcessLog()
    h._ensure_worker = lambda: None          # type: ignore[assignment]
    return h


def _pending_rows(tmp: Path):
    p = tmp / "local" / "pending_sputter.csv"
    if not p.exists():
        return []
    with open(p, "r", encoding="utf-8", newline="") as f:
        return [r for r in csv.reader(f) if r]


def _rec(h: HostProcessLog):
    events = []
    h.set_event_sink(lambda kind, info: events.append((kind, dict(info))))
    return events


class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _notifier(monkeypatch=None):
    """카드를 실제로 보내지 않고 (title, subtitle, status, fields, urgent) 로 모은다."""
    n = CN.ChatNotifier(webhook_url=_URL)
    cards = []

    def _fake_post(title, subtitle="", status="INFO", fields=None, urgent=False, **kw):
        cards.append({"title": title, "subtitle": subtitle, "status": status,
                      "fields": dict(fields or {}), "urgent": urgent})
    n._post_card = _fake_post                 # type: ignore[assignment]
    return n, cards


def _info(**kw):
    today = datetime.now().replace(microsecond=0)
    base = {
        "key": "sputter-1", "target": "CH2", "request_id": "rid-123", "peer": "1.2.3.4",
        "received_at": today.replace(hour=9, minute=54, second=5),
        "started_at": today.replace(hour=9, minute=54, second=7),
        "recipe_name": "STO_#16-3.csv", "row_count": "1", "process_names": "STO_#16-3",
        "log_files": ["CH2_STO.txt"],
    }
    base.update(kw)
    return base


# ─────────────────────────── A. 기록 이벤트 ───────────────────────────
def test_a1_started_once():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    ev = _rec(h)
    rx = datetime(2026, 10, 2, 9, 54, 5)
    k = h.request(target="CH2", request_id="rid-1", peer="p", received_at=rx, recipe_name="a.csv")
    h.update(k, row_count=2, process_names="A / B")
    assert ev == [], "요청 수신은 통지하지 않는다"
    h.mark_started(k, rx + timedelta(seconds=2), log_file="run1.txt")
    h.mark_started(k, rx + timedelta(seconds=99), log_file="run2.txt")   # 두 번째 행 — 시작 시각 그대로
    assert [e[0] for e in ev] == ["started"], ev
    info = ev[0][1]
    assert info["key"] == k and info["target"] == "CH2" and info["request_id"] == "rid-1"
    assert info["recipe_name"] == "a.csv" and info["row_count"] == "2"
    assert info["process_names"] == "A / B"
    assert info["received_at"] == rx and info["started_at"] == rx + timedelta(seconds=2)
    assert info["log_files"] == ["run1.txt"]


def test_a2_finished_once_and_idempotent():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    ev = _rec(h)
    rx = datetime(2026, 10, 2, 10, 0, 0)
    k = h.request(target="CH1", request_id="r", peer="p", received_at=rx)
    h.mark_started(k, rx + timedelta(seconds=5))
    fin = rx + timedelta(minutes=30)
    assert h.finalize(k, RESULT_SUCCESS, "", fin) is True
    assert h.finalize(k, RESULT_FAIL, "두 번째", fin) is False      # 멱등
    kinds = [e[0] for e in ev]
    assert kinds == ["started", "finished"], kinds
    info = ev[1][1]
    assert info["result"] == RESULT_SUCCESS and info["reason"] == ""
    assert info["finished_at"] == fin and info["started_at"] == rx + timedelta(seconds=5)


def test_a3_reject_event():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    ev = _rec(h)
    k = h.request(target="CH1", request_id="r", peer="p", received_at=datetime(2026, 10, 2, 11, 0, 0))
    assert h.reject(k, "E420 CH1는 이미 다른 공정이 실행 중입니다.") is True
    assert len(ev) == 1 and ev[0][0] == "finished"
    info = ev[0][1]
    assert info["result"] == RESULT_REJECT
    assert info["reason"] == "E420 CH1는 이미 다른 공정이 실행 중입니다."
    assert info["started_at"] is None and isinstance(info["finished_at"], datetime)


def test_a4_startup_recover_emits_restart():
    tmp = Path(tempfile.mkdtemp())
    h1 = _setup(tmp)                          # 이전 실행(수신자 없음)
    rx = datetime(2026, 10, 2, 12, 0, 0)
    k_run = h1.request(target="CH1", request_id="a", peer="p", received_at=rx)
    h1.mark_started(k_run, rx + timedelta(seconds=3))
    k_wait = h1.request(target="CH2", request_id="b", peer="p", received_at=rx + timedelta(seconds=1))
    assert len(h1._final_emitted) == 0

    h2 = _setup(tmp)                          # 재시작 후
    ev = _rec(h2)
    h2.startup_recover()
    fin = sorted((e for e in ev if e[0] == "finished"), key=lambda e: e[1]["target"])
    assert len(fin) == 2, ev
    a, b = fin[0][1], fin[1][1]
    assert a["key"] == k_run and a["result"] == RESULT_RESTART
    assert a["reason"] == "프로그램 재시작 (공정 중)" and a["finished_at"] is None
    assert b["key"] == k_wait and b["result"] == RESULT_RESTART
    assert b["reason"] == "프로그램 재시작 (시작 전)"
    assert len(_pending_rows(tmp)) == 2


def test_a5_sink_exception_does_not_break_logging():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)

    def _boom(kind, info):
        raise RuntimeError("sink down")
    h.set_event_sink(_boom)
    rx = datetime(2026, 10, 2, 13, 0, 0)
    k = h.request(target="CH1", request_id="r", peer="p", received_at=rx)
    h.mark_started(k, rx)
    assert h.finalize(k, RESULT_SUCCESS, "", rx + timedelta(minutes=1)) is True
    assert len(_pending_rows(tmp)) == 1
    assert k not in h._load_open()


def test_a6_sink_called_outside_lock():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    got = []

    def _sink(kind, info):
        res = {}

        def _try():
            ok = h._lk.acquire(timeout=1.0)
            res["ok"] = ok
            if ok:
                h._lk.release()
        t = threading.Thread(target=_try)
        t.start()
        t.join(2.0)
        got.append((kind, res.get("ok")))
    h.set_event_sink(_sink)
    rx = datetime(2026, 10, 2, 14, 0, 0)
    k = h.request(target="CH1", request_id="r", peer="p", received_at=rx)
    h.mark_started(k, rx)
    h.finalize(k, RESULT_SUCCESS, "", rx + timedelta(minutes=1))
    k2 = h.request(target="CH2", request_id="r2", peer="p", received_at=rx)
    h.reject(k2, "E420")
    assert got == [("started", True), ("finished", True), ("finished", True)], got


def test_a7_csv_rows_identical_with_and_without_sink():
    def _run(with_sink: bool):
        tmp = Path(tempfile.mkdtemp())
        h = _setup(tmp)
        if with_sink:
            _rec(h)
        rx = datetime(2026, 10, 2, 15, 0, 0)
        k = h.request(target="CH1", request_id="same", peer="p", received_at=rx, recipe_name="x.csv")
        h.update(k, row_count=3, process_names="A / B / C")
        h.mark_started(k, rx + timedelta(seconds=10), log_file="r.txt")
        h.finalize(k, RESULT_STOP, "사용자 STOP", rx + timedelta(minutes=7))
        k2 = h.request(target="CH2", request_id="same2", peer="p", received_at=rx + timedelta(seconds=1))
        h.reject(k2, "E230 레시피 값 오류")
        return _pending_rows(tmp)
    assert _run(False) == _run(True)


def test_a8_mem_queue_path_emits_once():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    ev = _rec(h)
    h._append_pending = lambda ymd, row: False            # type: ignore[assignment]
    rx = datetime(2026, 10, 2, 16, 0, 0)
    k = h.request(target="CH1", request_id="r", peer="p", received_at=rx)
    assert h.finalize(k, RESULT_FAIL, "IG 타임아웃", rx + timedelta(minutes=1)) is True
    assert h.finalize(k, RESULT_FAIL, "IG 타임아웃", rx + timedelta(minutes=1)) is True   # open 유지(기존 동작)
    fin = [e for e in ev if e[0] == "finished"]
    assert len(fin) == 1 and fin[0][1]["result"] == RESULT_FAIL, ev


def test_a9_no_sink_no_state():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    rx = datetime(2026, 10, 2, 17, 0, 0)
    for i in range(5):
        k = h.request(target="CH1", request_id=f"r{i}", peer="p", received_at=rx + timedelta(seconds=i))
        h.mark_started(k, rx)
        h.finalize(k, RESULT_SUCCESS, "", rx + timedelta(minutes=1))
    assert len(h._final_emitted) == 0
    assert len(_pending_rows(tmp)) == 5


# ─────────────────────────── B. 카드 ───────────────────────────
def test_b1_started_card():
    n, cards = _notifier()
    n.notify_client_process("started", _info())
    assert len(cards) == 1
    c = cards[0]
    assert c["title"] == "공정 시작" and c["status"] == "INFO" and c["urgent"] is True
    assert c["subtitle"] == "CH2 · STO_#16-3"
    f = c["fields"]
    assert f["대상"] == "CH2"
    assert f["레시피"] == "STO_#16-3.csv (공정 1개)"
    assert f["요청 시각"] == "09:54:05" and f["시작 시각"] == "09:54:07"
    assert f["request_id"] == "rid-123"


def test_b2_finished_success_card():
    n, cards = _notifier()
    st = datetime.now().replace(hour=9, minute=54, second=7, microsecond=0)
    fin = st + timedelta(minutes=411, seconds=23)
    n.notify_client_process("finished", _info(started_at=st, result=RESULT_SUCCESS, reason="",
                                              finished_at=fin))
    c = cards[-1]
    assert c["title"] == "공정 종료" and c["status"] == "SUCCESS"
    f = c["fields"]
    assert f["결과"] == "정상 완료"
    assert f["시작 ~ 종료"] == f"{st:%H:%M:%S} ~ {fin:%H:%M:%S}"
    assert f["소요"] == "411.4분"
    assert f["request_id"] == "rid-123"

    # 대기 행만 있는 레시피(시작 없이 성공) — 종료 시각만
    n.notify_client_process("finished", _info(started_at=None, result=RESULT_SUCCESS,
                                              reason="공정 행 없음(대기만)", finished_at=fin))
    f2 = cards[-1]["fields"]
    assert f2["결과"] == "정상 완료 · 공정 행 없음(대기만)"
    assert "종료 시각" in f2 and "소요" not in f2


def test_b3_stop_is_finished_with_warn():
    n, cards = _notifier()
    n.notify_client_process("finished", _info(result=RESULT_STOP, reason="사용자 STOP",
                                              finished_at=datetime.now()))
    c = cards[-1]
    assert c["title"] == "공정 종료" and c["status"] == "WARN"
    assert c["fields"]["결과"] == "STOP · 사용자 STOP"


def test_b4_error_kinds(monkeypatch):
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 60.0, raising=False)
    n, cards = _notifier()
    cases = [
        (dict(result=RESULT_REJECT, reason="E420 CH1는 이미 다른 공정이 실행 중입니다.", started_at=None),
         "요청 거절 (시작 전)"),
        (dict(result=RESULT_FAIL, reason="IG base pressure timeout"), "공정 실패"),
        (dict(result=RESULT_FAIL, reason="E511 사전 연결 점검 실패", started_at=None), "공정 실패 (시작 전)"),
        (dict(result=RESULT_RESTART, reason="프로그램 재시작 (공정 중)", finished_at=None),
         "프로그램 재시작으로 중단"),
        (dict(result=RESULT_UNKNOWN, reason="종료 경로 미확인"), "종료 미확인"),
    ]
    for kw, label in cases:
        n.notify_client_process("finished", _info(**kw))
        c = cards[-1]
        assert c["title"] == "공정 에러" and c["status"] == "FAIL", c
        assert c["fields"]["구분"] == label, c
        assert c["fields"]["사유"] == kw["reason"]
        assert "시각" in c["fields"]
        if kw.get("started_at", "x") is None:
            assert "시작 시각" not in c["fields"]
    assert len(cards) == len(cases)


def test_b5_error_dedup(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(CN.time, "monotonic", clock)
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 60.0, raising=False)
    n, cards = _notifier()
    rej = dict(result=RESULT_REJECT, reason="E401 PLC 연결 실패", started_at=None)
    for _ in range(4):                                      # 로봇 재시도
        n.notify_client_process("finished", _info(**rej))
        clock.t += 5.0
    assert len(cards) == 1
    n.notify_client_process("finished", _info(target="CH1", **rej))     # 다른 대상 → 바로
    assert len(cards) == 2
    clock.t += 61.0
    n.notify_client_process("finished", _info(**rej))       # 창 뒤 재발 → 반복 칸
    assert len(cards) == 3
    assert cards[-1]["fields"]["반복"].startswith("직전 ") and "3회 더" in cards[-1]["fields"]["반복"]
    # 종료/시작 카드는 억제 대상이 아님
    n.notify_client_process("started", _info())
    n.notify_client_process("started", _info())
    assert len(cards) == 5
    # 창 0 이면 항상 전송
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 0, raising=False)
    n.notify_client_process("finished", _info(**rej))
    n.notify_client_process("finished", _info(**rej))
    assert len(cards) == 7


def test_b6_name_rules():
    n, cards = _notifier()
    n.notify_client_process("started", _info(target="CH1", process_names="A / B / C"))
    assert cards[-1]["subtitle"] == "CH1 · A 외 2개"
    n.notify_client_process("started", _info(target="PC(CH1)", process_names="", recipe_name="pc_1.csv",
                                             row_count=""))
    assert cards[-1]["subtitle"] == "PC(CH1) · pc_1"
    assert cards[-1]["fields"]["레시피"] == "pc_1.csv"
    n.notify_client_process("started", _info(target="", process_names="", recipe_name="", row_count=""))
    assert cards[-1]["subtitle"] == "— · —"


def test_b7_garbage_never_raises():
    n, cards = _notifier()
    n.notify_client_process("finished", {"received_at": "not-a-date", "started_at": 123,
                                         "finished_at": object(), "result": None, "row_count": None})
    n.notify_client_process("", None)            # type: ignore[arg-type]
    n.notify_client_process("unknown", {"target": "CH1"})
    # 날짜 문자열(ISO)도 받는다
    n.notify_client_process("started", _info(received_at="2026-10-02T09:54:05",
                                             started_at="2026-10-02T09:54:07"))
    assert cards[-1]["title"] == "공정 시작"


def test_b8_urgent_and_own_webhook(monkeypatch):
    n = CN.ChatNotifier(webhook_url=_URL)
    assert n._defer is True                       # 기본값이 지연이어도
    sent = []
    monkeypatch.setattr(n, "_schedule_post",
                        lambda payload, url, urgent=False: sent.append((payload, url, urgent)))
    n.notify_client_process("started", _info())
    n.notify_client_process("finished", _info(result=RESULT_REJECT, reason="E230", started_at=None))
    assert len(sent) == 2 and n._buffer == []
    assert all(url == _URL and urgent is True for _, url, urgent in sent)
    txt = str(sent[0][0])
    assert "공정 시작" in txt and "ℹ️" in txt
    assert "공정 에러" in str(sent[1][0]) and "❌" in str(sent[1][0])


def test_b9_post_card_icons():
    n = CN.ChatNotifier(webhook_url=_URL)
    seen = []
    n._post_json = lambda payload, urgent=False, route_params=None: seen.append(payload)  # type: ignore
    for st in ("INFO", "SUCCESS", "FAIL", "WARN", "???"):
        n._post_card("t", "s", st)
    heads = [p["cardsV2"][0]["card"]["sections"][0]["widgets"][0]["textParagraph"]["text"] for p in seen]
    assert heads == ["<b>ℹ️ t</b>", "<b>✅ t</b>", "<b>❌ t</b>", "<b>⚠️ t</b>", "<b>ℹ️ t</b>"]


# ─────────────────────────── C. 연결 ───────────────────────────
def test_c1_log_to_card_end_to_end(monkeypatch):
    monkeypatch.setattr(cfgc, "CHAT_ERR_DEDUP_S", 60.0, raising=False)
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    n, cards = _notifier()
    h.set_event_sink(n.notify_client_process)
    rx = datetime.now().replace(microsecond=0)
    k = h.request(target="CH2", request_id="rid-9", peer="p", received_at=rx, recipe_name="STO.csv")
    h.update(k, row_count=1, process_names="STO")
    h.mark_started(k, rx + timedelta(seconds=2), log_file="a.txt")
    h.finalize(k, RESULT_SUCCESS, "", rx + timedelta(minutes=10))
    k2 = h.request(target="CH1", request_id="rid-10", peer="p", received_at=rx)
    h.reject(k2, "E420 CH1는 이미 다른 공정이 실행 중입니다.")
    assert [c["title"] for c in cards] == ["공정 시작", "공정 종료", "공정 에러"]
    assert cards[1]["fields"]["소요"] == "10.0분"
    assert cards[2]["subtitle"] == "CH1 · —" and cards[2]["fields"]["구분"] == "요청 거절 (시작 전)"


def test_c2_main_wiring_source():
    src = (_ROOT / "main.py").read_text(encoding="utf-8")
    i = src.index('getattr(cfgl, "CHAT_WEBHOOK_CLIENT_PROCESS_URL", "")')
    blk = src[i:i + 2500]
    assert "self.chat_client = ChatNotifier(_cp_url) if _cp_url else None" in blk
    assert "self.chat_client.set_defer(False)" in blk
    assert "self._loop.call_soon_threadsafe(_send)" in blk
    assert "notifier.notify_client_process(kind, info)" in blk
    assert "set_event_sink(_emit_client_process_card)" in blk
    # 기존 방 웹훅(CHAT_WEBHOOK_URL)로 대신 보내지 않는다 — URL 이 없으면 알림기 자체를 만들지 않는다
    assert "ChatNotifier(_cp_url)" in blk and "ChatNotifier(url)" not in blk
    # 종료 시 정리 목록
    j = src.index('getattr(self, "chat_tsp", None),\n            getattr(self, "chat_client", None),')
    assert j > 0


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
