"""챔버12 시험대(v2/ch12/sim/rig.py → v2/ch12/system.py) — 시뮬레이터 전부 + 실제 드라이버 + 1초 묶기 + 기록 + v1 기록 출력.

가상 시계로 60 tick:
- 모든 블록의 품질·나이(켜진 장비 ok, 쓰지 않는 펄스·TSP·꺼진 장비 off), 1초 기록 SQLite 60 행, PLC 코일 CSV 5 s.
- 장비 끊김·재연결이 행에 맞게(그 장비만 fail·나이 증가, 다른 챔버·장비는 ok, 재연결 뒤 ok) + 이벤트.
- 설정에서 꺼진 장비는 포트를 열지 않고 off(열은 그대로).
- 엔진 꽂는 자리: on_tick_start·snapshot_fields·on_row 순서(기록 → 안전 → 엔진 → v1 출력 → 허브), 엔진 오류는 tick 을 멈추지 않음.
"""
import collections
import csv
import sqlite3
from pathlib import Path

import pytest

from v2.common.clock import VirtualClock
from v2.ch12 import snapshot_fields as SF
from v2.ch12.sim.rig import Ch12Rig

ON = {"plc", "ch1.mfc", "ch1.ig", "ch2.mfc", "ch2.ig", "ch2.dc", "ch2.rf", "ch2.stm100", "pc.rf", "camera"}
OFF = set(SF.BLOCK_ORDER) - ON


def make(tmp_path, cfg=None, **kw):
    c = VirtualClock(start_wall=1_760_000_000.0)
    c.register()
    r = Ch12Rig(c, tmp_path, cfg=cfg, **kw)
    r.rows = []
    r.row_listeners.append(r.rows.append)
    return r


def run(r, n):
    assert r.start()
    try:
        r.loop.run(n)
    finally:
        r.stop()
    return {x["seq"]: x for x in r.rows}


def test_60_ticks_all_blocks_quality_and_age(tmp_path):
    r = make(tmp_path)
    rows = run(r, 60)
    assert sorted(rows) == list(range(60)) and r.loop.missed == 0
    for seq, x in rows.items():
        for b in ON:
            assert (x[f"{b}.q"], x[f"{b}.age_s"]) == ("ok", 0.0), (seq, b)
        for b in OFF:
            assert (x[f"{b}.q"], x[f"{b}.age_s"]) == ("off", None), (seq, b)
            blk = next(bl for bl in SF.device_blocks() if bl.name == b)
            assert all(x[f"{b}.{f.name}"] is None for f in blk.fields)        # 꺼진 장비 값은 채우지 않음
    # MFC: 명령 간격 1 s 라 R60·R5 번갈아(자기 품질·나이)
    q = collections.Counter((x["ch1.mfc.ar_sccm.q"], x["ch1.mfc.pressure_mtorr.q"]) for x in rows.values())
    assert q[("ok", "stale")] > 10 and q[("stale", "ok")] > 10
    ages = collections.Counter(x["ch1.mfc.pressure_mtorr.age_s"] for x in rows.values() if x["ch1.mfc.pressure_mtorr.q"] == "stale")
    assert set(ages) <= {None, 1.0, 2.0} and ages[1.0] > 20 and ages[None] <= 1   # 2 = 간격을 못 맞춘 tick(약 10 tick 에 1번)
    # PLC 한 번 읽기 → plc·ch2.dc·ch2.rf·pc.rf, 시뮬레이터 초기 상태(펌핑 중)
    x = rows[30]
    assert x["plc.m_v_1_sw"] is True and x["plc.turbo_2_sw"] is True and x["plc.g_v_1_close_lamp"] is True
    assert x["ch2.dc.dc1_p"] == 0.0 and x["ch2.rf.state"] == "IDLE" and x["pc.rf.fwd_w"] == 0.0
    assert x["ch2.dc.dc2_p"] is None                                          # 히터 있음 → DC2 없음(D/A CH3 = 히터)
    assert x["ch2.stm100.crystal_ok"] is True and x["ch2.stm100.seq"] > 0
    assert x["ch1.ig.state"] == "off" and x["camera.running"] is False
    assert x["prog.threads"] >= 10 and x["prog.uptime_s"] > 0 and x["prog.rec_dropped"] == 0
    assert x["ch1.proc.run_id"] is None and x["ch2.set.dc_w"] is None          # 엔진 없음 — 비움
    # 1초 기록·v1 출력
    assert r.recorder.flush() and r.recorder.written == 60 and list(r.recorder.errors) == []
    db = sqlite3.connect(next(Path(r.cfg["LOCAL_DATA_DIR"]).glob("snap_*.sqlite")))
    assert db.execute("SELECT COUNT(*) FROM snapshot").fetchone()[0] == 60
    assert db.execute('SELECT "ch1.mfc.q", "ch1.oes.q" FROM snapshot WHERE seq = 5').fetchone() == ("ok", "off")
    db.close()
    coil = list(Path(r.cfg["PLC_COIL_LOG_NAS_DIR"]).glob("*.csv"))
    with open(coil[0], encoding="utf-8-sig", newline="") as f:
        lines = list(csv.reader(f))
    assert lines[0][0] == "Timestamp" and len(lines[0]) == 1 + 124 + 12 and 11 <= len(lines) - 1 <= 13   # 5 s 마다
    assert r.outputs.plc_coil.stats["ok"] >= 0 and list(r.outputs.writer.errors) == []
    assert r.internal_errors == 0
    assert r.device_table()["ch1.dcpulse"] == "on-demand" and r.device_table()["ch1.stm100"] == "off"


def test_disconnect_and_reconnect_show_in_rows(tmp_path):
    r = make(tmp_path)
    # seq n 의 tick = monotonic n + 1
    r.sims["ch1.mfc"].add_fault("disconnect", 10.5, 15.5)
    r.sims["plc"].add_fault("disconnect", 20.5, 23.5)
    r.sims["ch2.ig"].add_fault("no_response", 30.5, 33.5)
    r.sims["ch2.stm100"].add_fault("disconnect", 40.5, 44.5)
    rows = run(r, 55)
    q = {b: [rows[n][f"{b}.q"] for n in range(55)] for b in ("plc", "ch1.mfc", "ch2.mfc", "ch2.ig", "ch2.stm100", "ch2.dc")}
    # CH1 MFC: 끊김 → 백오프(1·2·4 s) 동안 fail(마지막 값의 나이가 늘어남) → 재연결 뒤 ok. CH2 MFC 는 그대로.
    assert all(v == "fail" for v in q["ch1.mfc"][10:15]) and q["ch1.mfc"][9] == "ok" and q["ch1.mfc"][-1] == "ok"
    first_ok = next(n for n in range(15, 55) if q["ch1.mfc"][n] == "ok")
    assert [rows[n]["ch1.mfc.age_s"] for n in range(10, first_ok)] == [float(k) for k in range(1, first_ok - 9)]
    assert set(q["ch2.mfc"]) == {"ok"}
    # PLC: 끊김 → plc 와 PLC 해독 블록(ch2.dc)이 같이 fail → 재연결(5 s 백오프) 뒤 ok
    assert q["plc"] == q["ch2.dc"] and "fail" in q["plc"][19:23] and q["plc"][30] == "ok"
    assert all(rows[n]["ch1.mfc.q"] == "ok" for n in range(19, 26))           # PLC 끊김이 다른 장비를 막지 않음
    # IG 무응답: 그 tick 은 응답 없음(stale·나이) → 늦은 실패(fail) → 다시 ok, 연결은 유지(v1 — 무응답으로 끊지 않음)
    assert "fail" in q["ch2.ig"][30:36] and q["ch2.ig"][40] == "ok"
    # 두께(1초 행은 드라이버 캐시): 링크가 끊긴 동안 fail
    assert "fail" in q["ch2.stm100"][40:47] and q["ch2.stm100"][-1] == "ok"
    ev = collections.Counter((e.source, e.kind) for e in r.events)
    assert ev[("ch1.mfc", "disconnected")] == 1 and ev[("ch1.mfc", "connected")] == 2
    assert ev[("plc", "disconnected")] == 1 and ev[("plc", "link_up")] >= 1
    assert ev[("ch2.ig", "disconnected")] == 0 and ev[("ch2.stm100", "disconnected")] == 1
    db = sqlite3.connect(next(Path(r.cfg["LOCAL_DATA_DIR"]).glob("snap_*.sqlite")))
    got = {(s, c, lv) for s, c, lv in db.execute("SELECT source, code, level FROM events")}
    db.close()
    assert ("ch1.mfc", "DISCONNECTED", "warn") in got and ("ch1.mfc", "CONNECTED", "info") in got
    st = r.outputs.plc_coil.stats                                             # PLC 끊긴 동안 코일 CSV 행 없음(v1)
    assert st["disconnected"] + st["empty_snapshot"] >= 1


def test_disabled_devices_do_not_open_ports_and_are_off(tmp_path):
    cfg = {"CH2_SUPPORTS_THICKNESS": False, "CH2_SUPPORTS_DCPULSE": False, "CH1_SUPPORTS_RFPULSE": False,
           "SUPPORTS_DC": False, "CH2_SUPPORTS_RF_CONT": False}
    r = make(tmp_path, cfg)
    other = make(tmp_path / "b")
    assert r.bus.columns == other.bus.columns                                 # 열은 설정과 무관
    other.stop()
    rows = run(r, 5)
    x = rows[4]
    for b in ("ch2.stm100", "ch2.dcpulse", "ch1.rfpulse", "ch2.dc", "ch2.rf"):
        assert x[f"{b}.q"] == "off", b
    assert x["pc.rf.q"] == "ok" and x["plc.q"] == "ok"                         # 같은 PLC 의 다른 블록은 그대로
    assert not r.enabled["ch2.stm100"] and len(r.sims["ch2.stm100"].cmds) == 0  # 포트에 닿지 않음
    assert r.dcpulse["ch2"] is None and r.rfpulse["ch1"] is None and r.dc[1] is None and r.rf["ch2"] is None
    with pytest.raises(KeyError):
        r.shared.pulse.claim("dcpulse", "ch2")


class _Engine:
    def __init__(self, log, name, fail=False):
        self.log, self.name, self.fail = log, name, fail
        self.events = collections.deque(maxlen=7)
        self.busy = False
        self.views = []

    def on_tick_start(self, t):
        self.log.append((t.seq, "tick", self.name))

    def snapshot_fields(self):
        return {"ch1.proc.run_id": "R1", "ch1.proc.block_no": 2, "ch1.set.ar_sccm": 20.0, "ch1.proc.eta_remaining_s": None,
                "nope.col": 1}

    def on_row(self, view):
        self.views.append(view)
        self.log.append((view.seq, "engine", self.name))
        if self.fail:
            raise RuntimeError("엔진 오류")


def test_engine_slot_order_fields_and_errors(tmp_path):
    r = make(tmp_path)
    log = []
    e1, e2 = _Engine(log, "ch1"), _Engine(log, "bad", fail=True)
    r.register_engine("ch1", e1)
    r.register_engine("bad", e2)
    with pytest.raises(ValueError):
        r.register_engine("ch1", e1)
    assert e1.events.maxlen == r.retention.events                              # 링 버퍼 크기는 한 곳에서

    class Safety:
        def on_row(self, view):
            log.append((view.seq, "safety", ""))
    r.safety = Safety()
    rec = r.loop.sinks[0]
    r.loop.sinks[0] = lambda row: (log.append((row["seq"], "record", "")), rec(row))
    out = r.outputs.on_row
    r.outputs.on_row = lambda row: (log.append((row["seq"], "outputs", "")), out(row))
    r.row_listeners.insert(0, lambda row: log.append((row["seq"], "hub", "")))
    assert not r.busy()
    e1.busy = True
    assert r.busy()
    rows = run(r, 4)
    order = [k for s, k, n in log if s == 2]
    assert order == ["tick", "tick", "record", "safety", "engine", "engine", "outputs", "hub"]
    x = rows[3]
    assert x["ch1.proc.run_id"] == "R1" and x["ch1.proc.block_no"] == 2 and x["ch1.set.ar_sccm"] == 20.0
    assert x["prog.engines"] == "ch1,bad" and "nope.col" not in x and r.unknown_fields == {"nope.col"}
    assert e1.views[-1].seq == 3 and e1.views[-1].value("ch1.mfc.ar_on") is False
    assert r.internal_errors == 4 and len(rows) == 4                           # 엔진 오류 4번 — tick 은 계속
    errs = [e for e in r.events if e.code == "INTERNAL_ERROR"]
    assert len(errs) == 4 and errs[0].source == "engine:bad" and errs[0].level == "error"
    assert r.unregister_engine("bad") is e2 and r.engine("ch1") is e1
