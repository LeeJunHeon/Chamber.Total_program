"""TSP 실행 로그의 사용 중 필라멘트(671)·에러 코드(206) — v1 main 984556e(tests/test_tsp_filament_log.py)와 같은 기대 문구.
시작 1번(실패해도 시작을 막지 않음)·종료 1번(정상·중단·오류 모두, 시작을 읽은 런만)·읽기만(장비에 쓰지 않음)."""
import pytest

from v2.ch12.drivers import tsp as T
from v2.tests.test_tsp_engine import fil_logs, make, run_until, run_until_idle


@pytest.fixture
def env(tmp_path):
    c, rig, rs, eng = make(tmp_path)
    rig.chambers["ch1"].pressure_override = 1.0e-5
    yield c, rig, rs, eng
    rig.stop()


def _sim(rig):
    return rig.sims["tsp"]


def test_labels_and_strict_decode():
    assert [T.decode_ascii_strict(x) for x in ("000003", "0", " 2 ", "", None, "x1", "-1")] == [3, 0, 2, None, None, None, None]
    assert [T.filament_label(n) for n in (None, 0, 2, 7)] == ["읽기 실패", "Mini Ti-Ball", "필라멘트 2", "알 수 없음(7)"]
    assert [T.error_label(c) for c in (None, 0, 3, 4, 9)] == ["읽기 실패", "없음", "TSP 필라멘트 끊김", "TSP 필라멘트 전체 소진", "알 수 없음(9)"]


def test_normal_end_same_filament(env):
    c, rig, rs, eng = env
    _sim(rig).active_filament = 3
    assert eng.start({"target": "2.5e-7", "cycles": "1"}, "web", by="op")["ok"]
    run_until_idle(rig, eng)
    assert fil_logs(eng) == ["[FILAMENT] 시작 시 사용 중 필라멘트: 3번",
                             "[FILAMENT] 종료 시 사용 중 필라멘트: 3번 (에러 코드 0 = 없음)",
                             "[FILAMENT] 실행 중 필라멘트 변경 없음"]
    writes = [f[2:5] for f in _sim(rig).frames if len(f) > 8 and f[5:6] == b"1"]
    assert writes and set(writes) == {b"011"}                       # 쓰기는 ON/OFF(011)뿐 — 671·206 은 읽기만


def test_filament_changed_during_run_and_exhausted(env):
    c, rig, rs, eng = env
    sim = _sim(rig)
    sim.active_filament = 1
    assert eng.start({"target": "2.5e-7", "cycles": "2"}, "web", by="op")["ok"]
    run_until(rig, lambda: any("시작 시 사용 중" in d for d in fil_logs(eng)))
    sim.active_filament, sim.error_code = 2, 4
    run_until_idle(rig, eng)
    assert fil_logs(eng)[1:] == ["[FILAMENT] 종료 시 사용 중 필라멘트: 2번 (에러 코드 4 = TSP 필라멘트 전체 소진)",
                                 "[FILAMENT] 실행 중 필라멘트 변경: 1번 → 2번 (컨트롤러 자동 전환 추정, 에러 코드 4 = TSP 필라멘트 전체 소진)",
                                 "[FILAMENT] ⚠ TSP 카트리지 필라멘트 전체 소진 — 카트리지 교체 필요"]


def test_user_stop_still_logs_end(env):
    c, rig, rs, eng = env
    _sim(rig).active_filament = 2
    assert eng.start({"target": "2.5e-7", "cycles": "3"}, "web", by="op")["ok"]
    run_until(rig, lambda: any("시작 시 사용 중" in d for d in fil_logs(eng)))
    eng.stop("web")
    run_until_idle(rig, eng)
    assert fil_logs(eng)[1:] == ["[FILAMENT] 종료 시 사용 중 필라멘트: 2번 (에러 코드 0 = 없음)",
                                 "[FILAMENT] 실행 중 필라멘트 변경 없음"]


def test_read_failure_does_not_block_start(env):
    c, rig, rs, eng = env
    sim = _sim(rig)
    orig = sim.handle

    def handle(data, now):
        if len(data) > 8 and data[2:5] in (b"671", b"206"):
            return None                                           # 응답 없음
        return orig(data, now)
    sim.handle = handle
    assert eng.start({"target": "2.5e-7", "cycles": "1"}, "web", by="op")["ok"]
    run_until_idle(rig, eng)
    f = fil_logs(eng)
    assert f[0].startswith("[FILAMENT] 시작 시 필라멘트 번호 읽기 실패(") and f[1].startswith("[FILAMENT] 종료 시 필라멘트 번호 읽기 실패(")
    assert eng.last_result["ok"] is not None and eng.last_result["outcome"] != "error"
