"""v1 기록 출력 — CH1/CH2 공정 엑셀(CH{n}.xlsx) 골든 비교(zip 안 파일마다 바이트, core.xml 시각만 맞춤).

골든: v1 util/gdrive_logger.py(_build_data·_save_sync·save_pc_only)를 실제로 돌려 만든 파일. 전원 소스마다 1행,
다음 행은 아래에서 위로(max_row 함정 — 서식만 있는 행), 새 뒤쪽 열 머리글만 채우기, broken·pending 처리, PC 시트.
"""
from __future__ import annotations

import pytest

from v2.ch12.outputs import excel_style as X
from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.excel_log import ExcelLog, build_rows
from v2.ch12.outputs.paths import CopySender, Paths
from v2.ch12.outputs.run_stats import RunStats
from v2.tests.outputs_golden import GOLDEN, Clock, assert_same_tree, copy_tree, load, make_root, v2_cfg, xlsx_parts
from v2.tools import v1golden_outputs_scen as SC

_KEEP = lambda rel: not rel.startswith("로컬정본")   # noqa: E731


@pytest.mark.parametrize("name", sorted(SC.EXCEL))
def test_excel_golden(tmp_path, name):
    scen = load(name)
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG, scen.get("cfg"))
    clock = Clock(scen.get("t0", SC.T0))
    main_dir = root / scen["main_dir"]
    if (GOLDEN / name / "files_pre").exists():
        copy_tree(GOLDEN / name / "files_pre", root)
    if scen.get("pre") == "blocked":
        main_dir.parent.mkdir(parents=True, exist_ok=True)
        main_dir.write_bytes(b"blocker")
    w = QueueWriter("t")
    s = CopySender(cfg, retry_s=3600)
    alerts, logs = [], []
    x = ExcelLog(scen["ch"], cfg, thickness_columns=scen.get("thickness", False), writer=w, sender=s,
                 now=clock.now, main_dir=main_dir, on_arc_alert=lambda *a: alerts.append(a), log_func=logs.append)
    try:
        for rec in scen["records"]:
            clock.set(rec.get("t", scen.get("t0", SC.T0)))
            if rec.get("pc"):
                x.save_pc(rec["pc"])
            else:
                st = RunStats(now=clock.now)
                st.apply_events(rec["events"])
                x.save_main(st, operator=rec.get("operator", ""), substrate=rec.get("substrate", ""),
                            note=rec.get("note", ""))
            w.flush()
            s.flush()
        assert_same_tree(root, GOLDEN / name / "files", _KEEP)
    finally:
        w.stop()
        s.stop()
    if name == "excel_ch2_multi":
        assert alerts == [(2, "Arc 많음", 3000, 2500)]          # 웹훅 대신 콜백(챗은 포털 몫)
        local = root / "로컬정본" / "Process_log" / "CH2.xlsx"
        assert xlsx_parts(local)["xl/worksheets/sheet1.xml"] == \
            xlsx_parts(GOLDEN / name / "files" / scen["main_dir"] / "CH2.xlsx")["xl/worksheets/sheet1.xml"]
    if name == "excel_blocked":
        assert any("로컬 보관" in m for m in logs)


def test_excel_dev_mode_and_rules(tmp_path):
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    w = QueueWriter("t")
    s = CopySender(cfg, retry_s=3600)
    x = ExcelLog(1, cfg, dev_mode=True, writer=w, sender=s, main_dir=root / "G")
    st = RunStats()
    st.apply_events(SC.EV_X1)
    assert x.save_main(st) is None                      # DEV_MODE 면 생략(v1)
    w.stop()
    s.stop()
    assert not (root / "G").exists()
    rows = build_rows(1, st)
    assert rows[0]["G2 Target"] == "" and rows[0]["G3 Target"] == ""          # CH1 은 G2/G3 공백
    assert rows[0]["avg_load"] is None and rows[0]["avg_tune"] is None        # RF Avg Load/Tune 늘 빈칸(v1)
    assert str(Paths({"GDRIVE_LOG_DIR": "H:/x"}).excel_main_dir()) == str(X_main()) and \
        str(Paths({}).excel_pc_dir()) == "G:/공유 드라이브/VanaM_Sputter/Process_log"


def X_main():
    from v2.ch12.outputs.paths import V1_GDRIVE_MAIN_DIR
    return V1_GDRIVE_MAIN_DIR


def test_excel_next_row_skips_phantom():
    Workbook, PatternFill = X.Workbook, X.PatternFill        # openpyxl 은 outputs 에서만 import(test_isolation)
    wb = Workbook()
    ws = wb.active
    ws.cell(row=5, column=1).value = "x"
    for r in range(6, 40):
        ws.cell(row=r, column=3).fill = PatternFill("solid", fgColor="F2F3F4")
    assert ws.max_row == 39 and X._next_data_row(ws) == 6
