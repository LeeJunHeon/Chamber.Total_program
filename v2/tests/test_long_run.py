"""오래 켜 두기 — 가상 시계 10 시간(v2/ch12/sim/soak.run_virtual — 기간은 인자, 최종 단계는 엔진을 꽂고 7일).

실행 프로그램과 같은 조립·같은 기록 크기(Ch12Rig + system.APP_RETENTION, 기본 설정, sim). 15 분마다 펄스 포트 점유·해제,
TSP 게이지 잡기·놓기, TSP 읽기 켜기·끄기, 카메라 참여·떠남(잡을 때마다 worker·스레드를 새로 만드는 곳).
시작 로컬 19:00 → 5 시간 뒤 자정(일별 SQLite 바뀜·지난 날 CSV 내보내기).
매시 gc 뒤: 메모리(8~10 시간 사이 증가 0.5 MB 안쪽 — 챔버K 와 같은 기준), 스레드 수(늘지 않음), 기록 큐·출력 큐(쌓이지 않음),
링 버퍼(이벤트·시뮬레이터 기록 — 상한 안), 놓친 tick 0. 환경 변수 CH12_LONGRUN_TABLE 이 있으면 시간별 표를 그 파일에(JSON).
"""
import collections
import json
import os

import pytest

from v2.ch12.sim import soak
from v2.ch12.system import APP_RETENTION

HOURS = 10


def check(res, hours):
    table, rig = res["table"], res["rig"]
    by = {r["h"]: r for r in table}
    assert sorted(by) == list(range(hours + 1))
    assert rig.loop.rows == hours * 3600 + 1 and rig.loop.missed == 0
    assert res["stats"]["close_ms_max"] < 900                                   # 행이 마감까지 기다리지 않음(읽기가 다 돌아옴)
    for r in table:
        assert r["rec_pending"] < 100 and r["rec_dropped"] == 0 and r["out_pending"] < 100, r
        assert r["events"] <= APP_RETENTION.events and r["sim_log_max"] <= APP_RETENTION.sim_log, r
    assert by[hours]["threads"] == by[1]["threads"], table                     # 잡기·놓기에서 스레드가 새지 않음
    assert list(rig.recorder.errors) == [] and list(rig.outputs.writer.errors) == [] and rig.internal_errors == 0
    cycles = (hours * 3600) // soak.PERIOD_S
    ev = collections.Counter((e.source, e.kind) for e in rig.events)
    if hours * 3600 < 1000:                                                    # 링 버퍼가 다 차지 않았을 때만 셀 수 있음
        assert ev[("ch1.dcpulse", "connected")] >= cycles
    return by


@pytest.mark.slow
def test_virtual_10h_app_retention_flat(tmp_path):
    res = soak.run_virtual(HOURS, tmp_path)
    if os.environ.get("CH12_LONGRUN_TABLE"):
        with open(os.environ["CH12_LONGRUN_TABLE"], "w", encoding="utf-8") as f:
            json.dump({"table": res["table"], "stats": res["stats"]}, f, ensure_ascii=False, indent=1)
    by = check(res, HOURS)
    assert by[HOURS]["private_mb"] - by[8]["private_mb"] < 0.5, res["table"]   # 8~10 시간 사이 0.5 MB 안쪽
    data = res["rig"].recorder.dir
    days = sorted(p.name for p in data.glob("snap_*.sqlite"))
    assert len(days) == 2 and (data / days[0].replace(".sqlite", ".csv")).exists()   # 자정 — 지난 날 CSV
    assert res["rig"].recorder.written == HOURS * 3600 + 1


def test_virtual_short_run_same_checks(tmp_path, monkeypatch):
    """같은 잡기·놓기를 짧게(빠른 시험) — 4 분, 1 분 주기 4회."""
    monkeypatch.setattr(soak, "PERIOD_S", 60)
    monkeypatch.setattr(soak, "HOLD_S", 10)
    res = soak.run_virtual(4 / 60, tmp_path)
    table = res["table"]
    assert [r["h"] for r in table] == [0] and res["rig"].loop.rows == 241 and res["rig"].loop.missed == 0
    ev = collections.Counter((e.source, e.kind) for e in res["rig"].events)
    assert ev[("ch1.dcpulse", "connected")] == 4 and ev[("tsp.ig", "connected")] == 4 and ev[("camera", "started")] == 4
    assert all(res["rig"].sources[n].worker is None for n in ("ch1.dcpulse", "ch2.rfpulse", "tsp.ig"))   # 놓은 뒤 off
    assert res["stats"]["close_ms_max"] < 900 and res["rig"].internal_errors == 0
