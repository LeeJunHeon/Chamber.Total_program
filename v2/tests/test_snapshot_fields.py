"""챔버12 1초 행 필드(v2/ch12/snapshot_fields.py) — 블록 순서·열·공정 맥락·목표값·프로그램 상태·RowView."""
import sys

import pytest

from v2.common.snapshot import TICK_COLUMNS
from v2.ch12 import snapshot_fields as SF


def test_blocks_in_order_from_driver_blocks():
    bl = SF.device_blocks()
    assert [b.name for b in bl] == list(SF.BLOCK_ORDER)
    assert len(bl) == 23
    assert {"plc", "camera", "tsp", "tsp.ig", "pc.rf", "ch2.dc", "ch2.rf", "ch2.heater", "ch2.thk"} <= set(SF.BLOCK_ORDER)
    for ch in ("ch1", "ch2"):
        assert {f"{ch}.{d}" for d in ("mfc", "ig", "dcpulse", "rfpulse", "stm100", "oes", "rga")} <= set(SF.BLOCK_ORDER)
    # PLC 한 번 읽기가 채우는 블록(품질 공유)
    assert {b for b, s in SF.BLOCK_SOURCE.items() if s == "plc"} == {"plc", "ch2.dc", "ch2.rf", "pc.rf", "ch2.heater"}
    assert SF.BLOCK_SOURCE["ch2.thk"] == "engine"                                # 두께 판정 열은 엔진이 채운다


def test_columns_unique_tick_then_extra_then_blocks():
    cols = SF.columns()
    assert len(cols) == len(set(cols))
    assert cols[:4] == [f.name for f in TICK_COLUMNS]
    n_extra = len(SF.EXTRA_FIELDS)
    assert cols[4:4 + n_extra] == [f.name for f in SF.EXTRA_FIELDS]
    assert cols[4 + n_extra] == "plc.q"
    for b in SF.BLOCK_ORDER:
        assert f"{b}.q" in cols and f"{b}.age_s" in cols


def test_context_and_target_fields():
    names = {f.name for f in SF.EXTRA_FIELDS}
    for c in ("ch1", "ch2", "pc", "tsp"):
        for k in ("state", "run_id", "owner", "recipe_rev", "block_no", "block_type", "block_label", "remain_s",
                  "eta_remaining_s", "min_total_s"):
            assert f"{c}.proc.{k}" in names
    assert "ch1.set.n2_sccm" not in names and "ch2.set.n2_sccm" in names          # CH1 가스는 Ar·O2 만
    assert {"ch2.set.heater_c", "ch2.set.dc_w", "ch1.set.dcpulse_w", "ch1.set.rfpulse_w", "pc.set.rf_w"} <= names
    from v2.ch12.drivers import heater as HD
    from v2.ch12 import thickness_record as TR
    assert SF.ENGINE_COLUMNS == ({n for n in names if not n.startswith("prog.")} | set(HD.engine_columns("ch2"))
                                 | {f"ch2.thk.{n}" for n in TR.BLOCK_FIELDS})


def test_program_state_reads_this_process():
    st = SF.program_state()
    assert set(st) >= {"prog.working_set_mb", "prog.private_mb", "prog.handles", "prog.threads", "prog.gc0", "prog.gc2"}
    assert st["prog.threads"] >= 1 and st["prog.gc0"] >= 0
    if sys.platform.startswith("linux"):
        assert st["prog.working_set_mb"] > 1 and st["prog.private_mb"] > 0 and st["prog.handles"] >= 3
        assert st["prog.os_threads"] >= 1
        assert st["prog.gdi"] is None and st["prog.user"] is None                 # 윈도우만 — 채우지 않음
    sw = SF.selfwatch_state(st)
    assert set(sw) == {"working_set_mb", "private_mb", "handles", "gdi", "user", "threads", "gc0", "gc1", "gc2"}


def test_rowview_quality_and_age():
    row = {"seq": 7, "ts": "2025-10-09T08:53:51",
           "ch1.mfc.q": "ok", "ch1.mfc.age_s": 0.0,
           "ch1.mfc.ar_sccm": 20.0, "ch1.mfc.ar_sccm.q": "ok", "ch1.mfc.ar_sccm.age_s": 0.0,
           "ch1.mfc.pressure_mtorr": 5.0, "ch1.mfc.pressure_mtorr.q": "stale", "ch1.mfc.pressure_mtorr.age_s": 1.0,
           "ch1.mfc.ar_on": True,
           "tsp.ig.q": "fail", "tsp.ig.age_s": 3.0, "tsp.ig.waiting": False,
           "plc.q": "late", "plc.age_s": 1.0, "plc.m_v_1_sw": True}
    v = SF.RowView(row)
    assert v.seq == 7 and v["ch1.mfc.ar_sccm"] == 20.0 and len(v) == len(row)
    assert v.fresh("ch1.mfc.ar_sccm") and v.value("ch1.mfc.ar_sccm") == 20.0
    assert not v.fresh("ch1.mfc.pressure_mtorr") and v.value("ch1.mfc.pressure_mtorr") is None
    assert v.last("ch1.mfc.pressure_mtorr") == 5.0 and v.age("ch1.mfc.pressure_mtorr") == 1.0
    assert v.fresh("ch1.mfc.ar_on")                                    # 자기 품질 없음 → 블록 품질 ok
    assert not v.fresh("tsp.ig.waiting") and v.age("tsp.ig.waiting") == 3.0   # 'tsp.ig' 블록(접두어 'tsp' 아님)
    assert v.fresh("plc.m_v_1_sw")                                     # late 도 이번 행에 처음 쓰이는 새 값
    assert SF.block_qualities(row, ("plc", "tsp.ig")) == {"plc": "late", "tsp.ig": "fail"}
    with pytest.raises(TypeError):
        v["x"] = 1
