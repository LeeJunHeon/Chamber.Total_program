# tests/test_host_process_log.py
# -*- coding: utf-8 -*-
"""
호스트 요청 공정 공유 CSV 로그(v3.1) 검증.

pytest 로도, `python tests/test_host_process_log.py` 로도 실행된다.
"""
from __future__ import annotations

import csv
import io
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from lib import config_common as cfgc                    # noqa: E402
from util.host_process_log import (                      # noqa: E402
    HostProcessLog, HEADER, N_COLS,
    RESULT_SUCCESS, RESULT_REJECT, RESULT_RESTART, RESULT_STOP,
)


# ─────────────────────────── 헬퍼 ───────────────────────────
def _setup(tmp: Path, *, worker: bool = True) -> HostProcessLog:
    """임시 폴더를 NAS/로컬로 지정한 새 인스턴스.

    worker=False 면 백그라운드 워커를 띄우지 않는다(_ensure_worker 를 no-op 으로).
    finalize 가 워커를 깨워 테스트가 상태를 만들기 전에 NAS 로 보내버리는
    경합을 없애고, 전송은 테스트가 _flush_once() 로만 수행한다.
    """
    cfgc.HOST_LOG_ENABLED = True
    cfgc.HOST_LOG_NAS_DIR = str(tmp / "nas")
    cfgc.HOST_LOG_LOCAL_DIR = str(tmp / "local")
    cfgc.HOST_LOG_RETRY_S = 30
    cfgc.HOST_LOG_LOCK_STALE_S = 120
    h = HostProcessLog()
    if not worker:
        h._ensure_worker = lambda: None          # type: ignore[assignment]
    return h


def _nas_rows(tmp: Path, ymd: str):
    p = Path(cfgc.HOST_LOG_NAS_DIR) / f"Robot_{ymd}.csv"
    if not p.exists():
        return None, []
    raw = p.read_bytes()
    txt = raw.decode("utf-8-sig")
    return raw, list(csv.reader(io.StringIO(txt)))


def _pending_rows(tmp: Path):
    p = Path(cfgc.HOST_LOG_LOCAL_DIR) / "pending_sputter.csv"
    if not p.exists():
        return []
    with open(p, "r", encoding="utf-8", newline="") as f:
        return [r for r in csv.reader(f) if r]


# ─────────────────────────── 1 ───────────────────────────
def test_1_basic_roundtrip_and_midnight():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    rx = datetime(2026, 9, 18, 23, 59, 30, 250000)      # 자정 직전 요청
    k = h.request(target="CH1", request_id="req-1", peer="10.0.0.5:5000",
                  received_at=rx, recipe_name="a.csv")
    h.update(k, row_count=3, process_names="A / B / C")
    h.mark_started(k, rx + timedelta(seconds=30), log_file="run1.txt")
    assert h.finalize(k, RESULT_SUCCESS, "", rx + timedelta(minutes=5, seconds=30)) is True
    assert len(_pending_rows(tmp)) == 1

    h._flush_once()
    raw, rows = _nas_rows(tmp, "20260918")               # ← 요청 날짜 파일
    assert raw[:3] == b"\xef\xbb\xbf", "파일 생성 시 UTF-8 BOM"
    assert b"\r\n" in raw, "CRLF"
    assert rows[0] == HEADER
    r = rows[1]
    assert len(r) == N_COLS == 15
    assert r[0] == "CH1"
    assert r[1] == "23:59:30" and r[2] == "00:00:00" and r[3] == "00:05:00"
    assert r[4] == "5.0", "소요(분) 소수 1자리"
    assert r[5] == RESULT_SUCCESS and r[6] == "", "성공이면 사유 빈칸"
    assert r[7] == "A / B / C" and r[8] == "a.csv" and r[9] == "3"
    assert r[10] == "req-1" and r[11] == "10.0.0.5:5000" and r[12] == "run1.txt"
    assert r[13] == "sputter"
    assert r[-1] == k, "기록키는 마지막 열"
    assert _pending_rows(tmp) == [], "전송 후 pending 비움"


# ─────────────────────────── 2 ───────────────────────────
def test_2_reject_has_no_times():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    rx = datetime(2026, 9, 18, 10, 0, 0)
    k = h.request(target="CH2", request_id="r2", peer="p:1", received_at=rx)
    assert h.reject(k, "E301 gate가 CLOSED가 아님") is True
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    r = rows[1]
    assert r[5] == RESULT_REJECT
    assert r[2] == "" and r[3] == "" and r[4] == "", "시작/종료/소요 빈칸"
    assert "E301" in r[6]


# ─────────────────────────── 3 ───────────────────────────
def test_3_finalize_is_idempotent():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    rx = datetime(2026, 9, 18, 11, 0, 0)
    k = h.request(target="CH1", request_id="r3", peer="p:1", received_at=rx)
    assert h.finalize(k, RESULT_SUCCESS) is True
    assert h.finalize(k, RESULT_SUCCESS) is False, "두 번째는 멱등"
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    assert len(rows) == 2, "헤더 + 1줄"

    # is_owned 아비트레이션: 런타임이 수락했으면 핸들러는 거절하지 않는다
    k2 = h.request(target="CH1", request_id="r3b", peer="p:1", received_at=rx)
    h.mark_owned(k2)
    if not h.is_owned(k2):
        h.reject(k2, "핸들러 거절")          # 실행되지 않아야 함
    h.finalize(k2, "실패", "런타임 실패")
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    assert len(rows) == 3
    assert rows[2][5] == "실패" and rows[2][6] == "런타임 실패"


# ─────────────────────────── 4 ───────────────────────────
def test_4_lock_fresh_and_stale():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp, worker=False)
    rx = datetime(2026, 9, 18, 12, 0, 0)
    k = h.request(target="CH1", request_id="r4", peer="p:1", received_at=rx)
    h.finalize(k, RESULT_SUCCESS)

    lock = Path(cfgc.HOST_LOG_NAS_DIR) / "_lock" / "Robot.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("other 999 now", encoding="utf-8")   # 신선한 잠금

    cfgc.HOST_LOG_LOCK_STALE_S = 120
    t0 = time.time()
    h._flush_once()
    assert time.time() - t0 < 20, "잠금 대기는 25회 x 0.2s 안쪽"
    assert len(_pending_rows(tmp)) == 1, "신선한 잠금 → pending 유지"
    assert not (Path(cfgc.HOST_LOG_NAS_DIR) / "Robot_20260918.csv").exists()

    # 오래된 잠금(mtime -180s) → 제거 후 기록 (기준 120초)
    old = time.time() - 180
    os.utime(lock, (old, old))
    h._flush_once()
    assert _pending_rows(tmp) == []
    _, rows = _nas_rows(tmp, "20260918")
    assert len(rows) == 2
    assert not lock.exists(), "잠금은 항상 해제"


# ─────────────────────────── 5 ───────────────────────────
def test_5_nas_down_then_recover_keeps_order():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    cfgc.HOST_LOG_NAS_DIR = str(tmp / "nas_missing" / "x" / "y")   # 만들 수 없는 경로가 아니므로
    # 실제로 '쓸 수 없는' 상황을 만들기 위해 파일을 폴더 자리에 둔다
    blocker = tmp / "nas_blocked"
    blocker.write_text("not a dir", encoding="utf-8")
    cfgc.HOST_LOG_NAS_DIR = str(blocker / "sub")

    rx = datetime(2026, 9, 18, 13, 0, 0)
    keys = []
    for i in range(3):
        k = h.request(target="CH1", request_id=f"r5-{i}", peer="p:1",
                      received_at=rx + timedelta(seconds=i))
        h.finalize(k, RESULT_SUCCESS)
        keys.append(k)
    assert len(_pending_rows(tmp)) == 3, "NAS 불가여도 pending 에 3줄"

    h._flush_once()                       # 실패해도 예외 없이 지나가야 한다
    assert len(_pending_rows(tmp)) == 3

    cfgc.HOST_LOG_NAS_DIR = str(tmp / "nas")     # 복구
    h._flush_once()
    assert _pending_rows(tmp) == []
    _, rows = _nas_rows(tmp, "20260918")
    assert [r[-1] for r in rows[1:]] == keys, "요청 순서 유지"


# ─────────────────────────── 6 ───────────────────────────
def test_6_no_duplicate_when_already_on_nas():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp, worker=False)
    rx = datetime(2026, 9, 18, 14, 0, 0)
    k = h.request(target="CH1", request_id="r6", peer="p:1", received_at=rx)
    h.finalize(k, RESULT_SUCCESS)
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    assert len(rows) == 2

    # 같은 줄을 pending 에 되돌려 놓고 재전송
    row = rows[1]
    with open(Path(cfgc.HOST_LOG_LOCAL_DIR) / "pending_sputter.csv", "a",
              encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\r\n")
        w.writerow(["20260918"] + row)
    assert len(_pending_rows(tmp)) == 1
    h._flush_once()
    _, rows2 = _nas_rows(tmp, "20260918")
    assert len(rows2) == 2, "중복 없음"
    assert _pending_rows(tmp) == [], "pending 에서는 제거"


# ─────────────────────────── 7 ───────────────────────────
def test_7_startup_recover():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp, worker=False)
    rx = datetime(2026, 9, 18, 15, 0, 0)

    # (a) pending 에 있고 open 에도 있는 키 → open 만 삭제
    k_done = h.request(target="CH1", request_id="done", peer="p:1", received_at=rx)
    h.finalize(k_done, RESULT_SUCCESS)             # pending 에 들어가고 open 에서 빠짐
    cur = h._load_open()
    cur[k_done] = {"key": k_done, "target": "CH1", "request_id": "done", "peer": "p:1",
                   "received_at": rx.isoformat(timespec="milliseconds"),
                   "started_at": "", "recipe_name": "", "row_count": "",
                   "process_names": "", "log_files": []}
    # (b) open 에만 있는 키 — 시작 전 / 공정 중
    k_pre = "sputter-20260918150001000-pre"
    k_run = "sputter-20260918150002000-run"
    cur[k_pre] = {"key": k_pre, "target": "CH1", "request_id": "pre", "peer": "p:1",
                  "received_at": rx.isoformat(timespec="milliseconds"), "started_at": "",
                  "recipe_name": "", "row_count": "", "process_names": "", "log_files": []}
    cur[k_run] = {"key": k_run, "target": "CH2", "request_id": "run", "peer": "p:1",
                  "received_at": rx.isoformat(timespec="milliseconds"),
                  "started_at": (rx + timedelta(minutes=1)).isoformat(timespec="milliseconds"),
                  "recipe_name": "", "row_count": "", "process_names": "", "log_files": []}
    h._save_open(cur)

    h.startup_recover()          # worker=False 이므로 전송은 일어나지 않는다
    h.close(0.1)
    assert h._load_open() == {}, "open 은 비워진다"

    # 전송은 하지 않았으므로 pending 만 본다(앞의 요청날짜 칸 제거)
    rows = [r[1:] for r in _pending_rows(tmp)]
    by_key = {}
    for r in rows:
        by_key.setdefault(r[-1], []).append(r)

    assert len(by_key.get(k_done, [])) == 1, \
        "(a) 이미 pending 에 있던 키는 중단 줄을 추가하지 않는다"
    assert by_key[k_done][0][5] != RESULT_RESTART, "(a) 중단 줄이 아니어야 한다"
    a = by_key[k_pre][0]
    b = by_key[k_run][0]
    assert a[5] == RESULT_RESTART and a[2] == "" and "시작 전" in a[6]
    assert b[5] == RESULT_RESTART and b[2] == "15:01:00" and "공정 중" in b[6]
    assert a[3] == "" and a[4] == "" and b[3] == "" and b[4] == "", "종료/소요 빈칸"


# ─────────────────────────── 8 ───────────────────────────
def test_8_key_collision_suffix():
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp)
    rx = datetime(2026, 9, 18, 16, 0, 0, 500000)
    k1 = h.request(target="CH1", request_id="same", peer="p:1", received_at=rx)
    k2 = h.request(target="CH1", request_id="same", peer="p:1", received_at=rx)
    k3 = h.request(target="CH1", request_id="same", peer="p:1", received_at=rx)
    assert k1.endswith("-same")
    assert k2 == k1 + "-2", k2
    assert k3 == k1 + "-3", k3

    # 정제: 허용 문자 외는 "_"
    k4 = h.request(target="CH1", request_id="a/b c:d", peer="p:1", received_at=rx)
    assert k4.endswith("-a_b_c_d"), k4


# ─────────────────────────── 9 ───────────────────────────
def _mp_worker(nas_dir: str, local_dir: str, tag: str, n: int):
    """별도 프로세스에서 n 줄을 무작위 간격으로 append (잠금 규약 사용)."""
    import random
    sys.path.insert(0, str(_ROOT))
    from lib import config_common as _c
    from util.host_process_log import HostProcessLog as _H
    _c.HOST_LOG_ENABLED = True
    _c.HOST_LOG_NAS_DIR = nas_dir
    _c.HOST_LOG_LOCAL_DIR = local_dir
    _c.HOST_LOG_LOCK_STALE_S = 30
    h = _H()
    base = datetime(2026, 9, 18, 17, 0, 0)
    for i in range(n):
        k = h.request(target="CH1", request_id=f"{tag}-{i}", peer="p:1",
                      received_at=base + timedelta(milliseconds=i))
        h.finalize(k, RESULT_SUCCESS)
        h._flush_once()
        time.sleep(random.uniform(0.0, 0.004))


def test_9_two_processes_no_interleaving():
    import multiprocessing as mp
    tmp = Path(tempfile.mkdtemp())
    _setup(tmp)
    nas = str(tmp / "nas")
    procs = []
    for tag in ("A", "B"):
        loc = str(tmp / f"local_{tag}")
        p = mp.Process(target=_mp_worker, args=(nas, loc, tag, 100))
        p.start()
        procs.append(p)
    for p in procs:
        p.join(timeout=180)
        assert p.exitcode == 0, f"worker exitcode={p.exitcode}"

    path = Path(nas) / "Robot_20260918.csv"
    raw = path.read_bytes()
    rows = list(csv.reader(io.StringIO(raw.decode("utf-8-sig"))))
    assert rows[0] == HEADER
    data = rows[1:]
    assert len(data) == 200, f"총 200줄이어야 함 (got {len(data)})"
    assert all(len(r) == N_COLS for r in data), "깨진 줄 없음"
    assert len({r[-1] for r in data}) == 200, "기록키 중복 없음"


# ─────────────────────────── 10 ───────────────────────────
def test_10_chamber_runtime_integration():
    from runtime.chamber_runtime import ChamberRuntime
    tmp = Path(tempfile.mkdtemp())
    h = _setup(tmp, worker=False)

    import util.host_process_log as HPL
    HPL._INSTANCE = h                      # 런타임이 같은 인스턴스를 보게 한다

    def mk():
        o = ChamberRuntime.__new__(ChamberRuntime)
        o.ch = 1
        o.logs = []
        o.append_log = lambda s_, m: o.logs.append(f"[{s_}] {m}")
        o._host_run = None
        o._run_origin = None
        o._run_origin_meta = {}
        o._log_file_path = None
        return o

    rx = datetime(2026, 9, 18, 18, 0, 0)

    # (a) 3행 레시피(대기 행 포함) 성공 → finalize 1회, 로그파일 2개
    k = h.request(target="CH1", request_id="m1", peer="p:1", received_at=rx)
    h.update(k, recipe_name="r.csv", row_count=3, process_names="A / delay 1m / B")
    o = mk()
    o._host_run_begin("host", {"key": k})
    assert h.is_owned(k)
    o._log_file_path = Path("CH1_A_1.txt"); o._host_run_mark_started()
    o._log_file_path = Path("CH1_B_2.txt"); o._host_run_mark_started()
    o._host_run_set_explicit("성공", "")
    o._clear_queue_and_reset_ui_stub = None
    o._host_run_finalize()
    assert o._host_run is None
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    r = [x for x in rows[1:] if x[-1] == k]
    assert len(r) == 1, "요청 1건 = 1줄"
    assert r[0][7] == "A / delay 1m / B"
    assert r[0][12] == "CH1_A_1.txt / CH1_B_2.txt", "로그파일 2개 ' / ' 연결"
    assert r[0][5] == RESULT_SUCCESS

    # (b) 2행에서 실패
    k2 = h.request(target="CH1", request_id="m2", peer="p:1", received_at=rx)
    o = mk()
    o._host_run_begin("host", {"key": k2})
    o._log_file_path = Path("x.txt"); o._host_run_mark_started()
    o._host_run["last_row"] = {"ok": False, "stopped": False, "result": "실패",
                               "reason": "IG 압력 미달"}
    o._host_run_finalize()
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    r = [x for x in rows[1:] if x[-1] == k2][0]
    assert r[5] == "실패" and r[6] == "IG 압력 미달"

    # (c) 시작 전 STOP
    k3 = h.request(target="CH1", request_id="m3", peer="p:1", received_at=rx)
    o = mk()
    o._host_run_begin("host", {"key": k3})
    o._host_run_set_explicit("STOP", "사용자 STOP (시작 전)")
    o._host_run_finalize()
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    r = [x for x in rows[1:] if x[-1] == k3][0]
    assert r[5] == RESULT_STOP and "시작 전" in r[6]
    assert r[2] == "", "시작시각 빈칸"

    # (d) 이전 런이 finalize 를 거치지 않고 남아 있으면, 새 요청 수락 시 먼저 닫는다
    k_dangling = h.request(target="CH1", request_id="m4a", peer="p:1", received_at=rx)
    k_next = h.request(target="CH1", request_id="m4b", peer="p:1", received_at=rx)
    o = mk()
    o._host_run_begin("host", {"key": k_dangling})
    o._log_file_path = Path("dangling.txt"); o._host_run_mark_started()
    # finalize 없이 곧바로 다음 요청 수락 (비정상 경로)
    o._host_run_begin("host", {"key": k_next})
    assert o._host_run["key"] == k_next, "새 런으로 교체"
    o._host_run_set_explicit("성공", "")
    o._host_run_finalize()
    h._flush_once()
    _, rows = _nas_rows(tmp, "20260918")
    rd = [x for x in rows[1:] if x[-1] == k_dangling]
    rn = [x for x in rows[1:] if x[-1] == k_next]
    assert len(rd) == 1 and rd[0][5] == "미확인", "이전 런은 '미확인' 으로 닫힌다"
    assert "종료 미확인" in rd[0][6]
    assert len(rn) == 1 and rn[0][5] == RESULT_SUCCESS, "새 런은 정상 기록"

    # (e) UI origin 은 아무 것도 기록하지 않는다
    before = len(_nas_rows(tmp, "20260918")[1])
    o = mk()
    o._host_run_begin("ui", None)
    assert o._host_run is None
    o._host_run_finalize()
    h._flush_once()
    assert len(_nas_rows(tmp, "20260918")[1]) == before

    HPL._INSTANCE = None


# ─────────────────────────── 11 ───────────────────────────
def test_11_server_writes_response_before_csv():
    """host/server.py 의 _handle 에서 write/drain 이 _cmd_csv.append 보다 먼저인지."""
    src = (_ROOT / "host" / "server.py").read_text(encoding="utf-8")
    body = src[src.index("async def _handle("):]
    i_drain = body.index("await writer.drain()\n\n                # ✅ 응답을 보낸 뒤에 명령 CSV 기록")
    i_append = body.index("await self._cmd_csv.append(_pending_cmd_row)")
    assert i_drain < i_append, "응답 write/drain 이 CSV append 보다 먼저여야 한다"
    # dispatch 앞에서 요청 컨텍스트를 세팅하는지
    assert body.index("set_request_ctx(") < body.index("await self.router.dispatch(")
    assert "reset_request_ctx(" in body


# ─────────────────────────── runner ───────────────────────────
def _run_all():
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f)]
    fails = []
    for n, f in fns:
        try:
            f()
            print(f"  ✅ {n}")
        except Exception as e:
            import traceback
            fails.append(n)
            print(f"  ❌ {n}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print("=" * 56)
    print(f"실패 {len(fails)}건" + (": " + ", ".join(fails) if fails else " — 전부 통과 ✅"))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(_run_all())
