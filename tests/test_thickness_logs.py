# -*- coding: utf-8 -*-
"""두께 모니터 3차 — 공정 엑셀(Thickness 그룹: 새 파일·기존 파일 머리글 확장·CH1 무변화), Calib CSV 제외량 열,
구글챗 시작 카드(Thickness/Time), 런타임 기록·종료 카드의 큰 튐 횟수(기록만), 영점 성공 여부 전달."""
import asyncio
import contextlib
import csv
import os
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
sys.path.insert(0, _HERE)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                     # noqa: E402
from openpyxl import load_workbook                                # noqa: E402

from controller.data_logger import THICKNESS_COLUMNS, DataLogger  # noqa: E402
from controller.thickness_process import ThicknessEndJudge        # noqa: E402
from lib.thickness_config import ThicknessJudgeParams             # noqa: E402
from util import gdrive_logger as G                               # noqa: E402

REC = {"end": "Thickness", "target": 100.0, "final_nm": 100.04, "avg_rate": 0.0512, "tooling": 110.0,
       "density": 2.7, "z_factor": 1.08, "chuck": "mid", "gun": "G1", "jumps": 2,
       "fallback_reason": ""}
THK_NAMES = ["End Mode", "Target", "Final", "Avg Rate", "Tooling", "Density", "Z", "Fallback Reason", "Jump Count"]


def _dl(ch, tmp_path, thk):
    dl = DataLogger(ch=ch, csv_dir=tmp_path, thickness_columns=thk)
    dl.start_new_log_session({"use_ms": False, "process_name": "T1", "dep_rate": 0.05, "thickness": 100.0,
                              "process_time": 33.3})
    return dl


def _save(ch, dl, tmp_path, thk):
    rows = G._build_data(ch, dl, "", "", "", None)
    G._save_sync(ch, rows, tmp_path, 5, 20.0, False, "", tmp_path / "fb", "Main Process", *G._main_cols(ch, thk))


# ───────────────────────── Calib CSV ─────────────────────────
def test_calib_csv_jump_count_is_last(tmp_path):
    assert THICKNESS_COLUMNS[-1] == "Thickness: Jump Count"
    assert not [c for c in THICKNESS_COLUMNS if "Step" in c]          # 계단 튐 의심·제외 열 삭제
    dl = _dl(2, tmp_path, True)
    dl.set_thickness_record(REC)
    dl.finalize_and_write_log(True)
    with open(tmp_path / "Ch2_log.csv", encoding="utf-8-sig", newline="") as fp:
        rows = list(csv.DictReader(fp))
    assert rows[-1]["Thickness: Jump Count"] == "2"


def test_calib_csv_header_extends_from_phase2_header(tmp_path):
    """큰 튐 열이 없는 머리글 파일 — 머리글만 뒤로 늘리고 기존 행은 그대로."""
    old_hdr = DataLogger(ch=2, csv_dir=tmp_path / "x", thickness_columns=True).header[:-1]
    f = tmp_path / "Ch2_log.csv"
    with open(f, "w", encoding="utf-8-sig", newline="") as fp:
        w = csv.writer(fp)
        w.writerow(old_hdr)
        w.writerow(["2026-10-27 10:00:00", "old"] + [""] * (len(old_hdr) - 2))
    dl = DataLogger(ch=2, csv_dir=tmp_path, thickness_columns=True)
    rows = list(csv.DictReader(open(f, encoding="utf-8-sig")))
    assert dl.header[-1] == "Thickness: Jump Count" and rows[0]["Process Name"] == "old"
    assert rows[0]["Thickness: Jump Count"] == ""


# ───────────────────────── 공정 엑셀 ─────────────────────────
def test_excel_new_ch2_file_has_thickness_group_after_heater(tmp_path):
    dl = _dl(2, tmp_path, True)
    dl.set_thickness_record(REC)
    _save(2, dl, tmp_path, True)
    ws = load_workbook(tmp_path / "CH2.xlsx")["Main Process"]
    names = [ws.cell(2, c).value for c in range(1, ws.max_column + 1)]
    assert names[:43] == [c[0] for c in G._COLS_MAIN_CH2] and names[43:] == THK_NAMES
    assert ws.cell(1, 44).value == "Thickness" and "AR1:AZ1" in {str(r) for r in ws.merged_cells.ranges}
    vals = [ws.cell(4, c).value for c in range(44, 53)]
    assert vals == ["Thickness", 100.0, 100.04, 0.0512, 110.0, 2.7, 1.08, None, 2]
    # 기본 정보의 Dep.rate·Thickness 는 레시피 값, Process Time 은 그대로(실제 진행 시간이 들어간 값)
    col = {c[0]: i for i, c in enumerate(G._COLS_MAIN_CH2, 1)}
    assert ws.cell(4, col["Dep.rate"]).value == 0.05 and ws.cell(4, col["Thickness"]).value == 100.0


def test_excel_existing_ch2_heater_file_gets_thickness_headers(tmp_path):
    """히터까지만 있던 CH2.xlsx — 기존 머리글·행 그대로, 뒤에 Thickness 머리글만 붙이고 새 행에 값."""
    dl = _dl(2, tmp_path, False)
    _save(2, dl, tmp_path, False)                                  # 2단계까지의 배치(43열)
    ws = load_workbook(tmp_path / "CH2.xlsx")["Main Process"]
    assert ws.max_column == 43
    dl = _dl(2, tmp_path, True)
    dl.set_thickness_record(dict(REC, end="Time", target=None, jumps=None))
    _save(2, dl, tmp_path, True)
    ws = load_workbook(tmp_path / "CH2.xlsx")["Main Process"]
    assert [ws.cell(2, c).value for c in range(44, 53)] == THK_NAMES and ws.cell(3, 45).value == "nm"
    assert ws.cell(1, 44).value == "Thickness" and ws.cell(1, 39).value == "Heater"
    assert ws["C4"].value == "T1" and [ws.cell(4, c).value for c in range(44, 53)] == [None] * 9
    assert ws.cell(5, 44).value == "Time" and ws.cell(5, 45).value is None and ws.cell(5, 46).value == 100.04


def test_excel_ch1_unchanged(tmp_path):
    assert G._main_cols(1) == (G._COLS_MAIN, G._GRP_COLS_MAIN, G._GRP_STARTS_MAIN)
    assert G._main_cols(2) == (G._COLS_MAIN_CH2, G._GRP_COLS_MAIN_CH2, G._GRP_STARTS_MAIN_CH2)
    dl = _dl(1, tmp_path, False)
    dl.set_thickness_record(REC)                                   # 기록이 있어도 열이 없으면 쓰지 않는다
    _save(1, dl, tmp_path, False)
    ws = load_workbook(tmp_path / "CH1.xlsx")["Main Process"]
    assert ws.max_column == 38 and ws.cell(2, 39).value is None


def test_save_process_log_picks_columns_from_data_logger(tmp_path, monkeypatch):
    from lib import config_common as cc
    monkeypatch.setattr(cc, "DEV_MODE", False)
    dl = _dl(2, tmp_path, True)
    dl.set_thickness_record(REC)
    asyncio.run(G.save_process_log(2, dl, log_dir=tmp_path, local_fallback=tmp_path / "fb"))
    ws = load_workbook(tmp_path / "CH2.xlsx")["Main Process"]
    assert ws.max_column == 52 and ws.cell(4, 52).value == 2


# ───────────────────────── 구글챗 시작 카드 ─────────────────────────
class _Chat:
    def __new__(cls):
        from controller.chat_notifier import ChatNotifier

        class C(ChatNotifier):
            def __init__(self):
                super().__init__(webhook_url="")
                self.cards = []

            def _post_card(self, title, subtitle="", status="INFO", fields=None, urgent=False, **k):
                self.cards.append((title, dict(fields or {})))

            def _upsert_error_card(self):
                pass

            def flush(self):
                pass
        return C()


def test_start_card_thickness_and_time():
    base = {"process_name": "a", "ch": 2, "shutter_delay": 5, "process_time": 33.333, "thickness": 100.0,
            "dep_rate": 0.05, "use_g1": True}
    c = _Chat()
    c.notify_process_started(dict(base))
    time_card = c.cards[-1][1]
    assert time_card["Process Time"] == "33.3분" and "종료 방식" not in time_card
    c.notify_process_started(dict(base, thickness_mode=True, thickness_expected_s=2000.0, thickness_limit_s=4000.0))
    thk = c.cards[-1][1]
    assert thk["Process Time"] == "목표 두께 100 nm (예상 33.3분, 상한 66.7분)" and thk["종료 방식"] == "Thickness"
    assert list(thk)[:5] == list(time_card)[:5]                    # 칸 순서는 그대로, '종료 방식' 만 뒤에


def test_end_card_shows_jumps():
    c = _Chat()
    c.notify_process_finished_detail(True, {"process_name": "a", "ch": 2, "thickness_end": "Thickness",
                                            "thickness_final": "100.0 / 100 nm", "thickness_jumps": "2회(기록만)"})
    assert c.cards[-1][1]["큰 튐"] == "2회(기록만)" and "계단 튐 제외" not in c.cards[-1][1]


# ───────────────────────── 런타임 기록 ─────────────────────────
def test_runtime_record_and_card_include_jumps():
    from test_thickness_runtime import FakeSvc, _rt

    async def go():
        svc = FakeSvc()
        rt, pc, rec = _rt(svc)
        rt._thk_run_started({"thickness_mode": True, "thickness": 100.0})
        j = ThicknessEndJudge(100.0, 0.05, ThicknessJudgeParams(), start_t=0, base=0.0, seq0=0)
        j.eff, j.eff_t, j.jump_count = 100.1, 2000.0, 2
        rt._thk_last_judge = j
        pc._thickness_end("Thickness")
        assert rt._thk_values()["jumps"] == 2
        p = {}
        rt._thk_card_fields(p)
        assert p["thickness_jumps"] == "2회(기록만)" and p["thickness_final"] == "100.1 / 100 nm"
        logs = []
        rt.append_log = lambda s, m: logs.append(m)
        rt.data_logger = DataLogger(ch=2, csv_dir=Path(os.environ.get("TEMP", ".")) / "thk_logs_test", thickness_columns=True)
        rt._thk_finalize_record()
        assert any("큰 튐 2회(기록만)" in m for m in logs)
    asyncio.run(go())


# ───────────────────────── 6차: 구글챗 정리·상한 실패 ─────────────────────────
def test_no_chat_text_during_process():
    """공정 중 시간 대체 전환·복귀·판정 예외·상한 도달은 구글챗 별도 메시지를 보내지 않는다(공정 로그·종료 카드에만)."""
    from test_thickness_runtime import FakeSvc, _rt
    import types
    async def go():
        svc = FakeSvc()
        rt, pc, rec = _rt(svc)
        sent = []
        rt.chat = types.SimpleNamespace(notify_text=sent.append, flush=lambda: None)
        assert not hasattr(rt, "_thk_notify")
        rt._thk_run_started({"thickness_mode": True, "thickness": 1.0})
        j = ThicknessEndJudge(1.0, 0.1, ThicknessJudgeParams(), start_t=svc.t, base=0.0, seq0=7)
        rt._thk_judge = rt._thk_last_judge = j
        for _ in range(12):                                            # 새 값 없음 → 시간 대체
            svc.t += 1.0
            rt._on_thickness_status(svc.status())
        svc.t += 0.5
        svc.st.update(seq=8, thickness_nm=0.5)                         # 복귀
        rt._on_thickness_status(svc.status())

        def boom(*a):
            raise RuntimeError("x")
        j.tick = boom                                                  # 판정 예외
        rt._on_thickness_status(svc.status())
        assert pc._thickness_end("상한") is not None
        assert sent == []
        assert {"on_thickness_fallback", "on_thickness_resume"} <= set(rec.names())
    asyncio.run(go())


@pytest.mark.parametrize("ok", [True, False])
def test_end_card_has_thickness_and_warnings_on_success_and_failure(ok):
    """종료 카드(성공)·에러 카드(실패) 모두 같은 경로 — 종료 방식·최종 두께·경고 목록이 들어간다."""
    c = _Chat()
    detail = {"process_name": "a", "ch": 2, "thickness_end": "Thickness" if ok else "상한",
              "thickness_final": "100.0 / 100 nm" if ok else "62.3 / 100 nm",
              "warnings": ["영점 확인 안 됨 — 50.20 nm 를 기준점으로(5초 동안 ≤ 2 nm 값 없음)", "크리스탈 수명 4.0%"]}
    if not ok:
        detail["errors"] = ["Step 30 THICKNESS_WAIT: [THICKNESS - E848: 두께 목표 미도달 — 최대 시간 초과: 유효 62.3 / 목표 100.0 nm, "
                            "경과 66.7분(상한 66.7분)]"]
    c.notify_process_finished_detail(ok, detail)
    f = c.cards[-1][1]
    assert f["종료 방식"] == detail["thickness_end"] and f["최종 두께"] == detail["thickness_final"]
    assert "영점 확인 안 됨" in f["주의"] and "크리스탈 수명" in f["주의"]
    if not ok:
        assert "E848" in f["원인"] and "두께 목표 미도달" in f["원인"]


def test_limit_failure_still_writes_calib_csv(tmp_path):
    """상한 실패도 Calib CSV 에 기록(Result FAIL, 종료 방식 상한). 엑셀 공정 로그는 기존 규칙대로 성공만(런타임 _GDRIVE_OK and ok)."""
    dl = _dl(2, tmp_path, True)
    dl.set_thickness_record(dict(REC, end="상한", final_nm=62.3))
    dl.finalize_and_write_log(False)
    with open(tmp_path / "Ch2_log.csv", encoding="utf-8-sig", newline="") as fp:
        r = list(csv.DictReader(fp))[-1]
    assert r["Result"] == "FAIL" and r["Thickness: End Mode"] == "상한" and r["Thickness: Final nm"] == "62.30"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
