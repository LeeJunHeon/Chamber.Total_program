# -*- coding: utf-8 -*-
"""레시피 CSV 의 '쉼표만 있는 행'이 공정으로 큐에 들어가던 문제.

Pre-sputter 레시피(Recipe/CH1/Pre-sputter_RF (30+5)x4_2hr.csv)는 실제 7행 뒤에 쉼표만 있는 행이 4개 있다.
csv.DictReader 는 완전히 빈 줄만 건너뛰므로 그 4행이 '공정 8~11' 로 큐에 들어가 "(총 11개)" 가 되고,
마지막 Pre-sputter 뒤 '공정 8' 에서 "CSV 공정 파라미터 오류" 로 큐가 끝났다(CH1_공정_8_*.txt 로그 생성).
"""
import os
import sys
import csv
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                        # noqa: E402
import runtime.chamber_runtime as CR                                 # noqa: E402
from runtime.chamber_runtime import ChamberRuntime, _is_blank_recipe_row   # noqa: E402
from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime     # noqa: E402

HEADER = ["Process_name", "delay", "shutter_delay", "process_time",
          "use_rf_pulse", "rf_pulse_power", "rf_pulse_freq", "rf_pulse_duty_cycle",
          "base_pressure", "working_pressure", "Ar_flow"]


def _write_csv(path, rows, *, header=None, blank_rows=0, raw_blank=","):
    """utf-8-sig + CRLF 로 쓴다(실제 레시피와 같은 형식). blank_rows 는 쉼표만 있는 행."""
    hdr = header if header is not None else HEADER
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        f.write(",".join(hdr) + "\r\n")
        for r in rows:
            f.write(",".join(str(c) for c in r) + "\r\n")
        for _ in range(blank_rows):
            f.write(raw_blank * (len(hdr) - 1) + "\r\n")     # 쉼표만 있는 행


def _presputter_rows():
    """Pre-sputter + delay 5m 를 3번 반복하고 마지막 Pre-sputter — 실제 레시피와 같은 7행."""
    ps = ["Pre-sputter", "", "0", "30", "T", "100", "1000", "10", "5e-6", "3", "20"]
    dl = ["delay 5m", "5m", "", "", "", "", "", "", "", "", ""]
    return [ps, dl, ps, dl, ps, dl, ps]


# ───────────────────────── 챔버 하네스 ─────────────────────────
class _Cfg:
    def _get(self, name, default=None):
        return default


def _mk_ch(tmp_path):
    c = ChamberRuntime.__new__(ChamberRuntime)
    c.ch = 1
    c.cfg = _Cfg()
    c.logs = []
    c.warns = []
    c.append_log = lambda src, msg: c.logs.append((src, msg))
    c._post_warning = lambda title, text, **k: c.warns.append((title, text))
    c._update_ui_from_params = lambda p: None
    c.process_queue = []
    c.current_process_index = -1
    c._w_log = None
    c._host_start_future = None
    c._hostlog = lambda: None

    def _handle_start_clicked(_flag, *, origin="ui", origin_meta=None):
        fut = c._host_start_future
        if fut is not None and not fut.done():
            fut.set_result((True, "ok"))
    c._handle_start_clicked = _handle_start_clicked
    return c


def _names(c):
    return [str(r.get("Process_name", "")) for r in c.process_queue]


def _log_text(c):
    return "\n".join(f"{s}: {m}" for s, m in c.logs)


# ───────────────────────── (a) 순수 함수 ─────────────────────────
def test_a_is_blank_recipe_row():
    assert _is_blank_recipe_row({"a": "", "b": "   ", "c": None}) is True
    assert _is_blank_recipe_row({"a": "", "b": "delay 5m"}) is False
    assert _is_blank_recipe_row({"a": "", "b": "0"}) is False           # '0' 도 값
    assert _is_blank_recipe_row({"a": "", None: ["", "  ", None]}) is True    # restkey 리스트
    assert _is_blank_recipe_row({"a": "", None: ["", "x"]}) is False
    assert _is_blank_recipe_row(["", "  ", None]) is True               # csv.reader 행
    assert _is_blank_recipe_row(["", "delay 5m"]) is False
    assert _is_blank_recipe_row([]) is True
    assert _is_blank_recipe_row({}) is True
    assert _is_blank_recipe_row(None) is False                         # dict/list 가 아니면 판정하지 않는다
    print("  (a) _is_blank_recipe_row OK")


# ───────────────────────── (b) 호스트/Pre-Sputter 경로 ─────────────────────────
def test_b_host_presputter_path_skips_blank_rows(tmp_path):
    p = tmp_path / "Pre-sputter_RF (30+5)x4_2hr.csv"
    _write_csv(p, _presputter_rows(), blank_rows=4)

    async def _main():
        c = _mk_ch(tmp_path)
        await c.start_with_recipe_string(str(p), origin="presputter")
        assert len(c.process_queue) == 7, _names(c)
        assert _names(c) == ["Pre-sputter", "delay 5m"] * 3 + ["Pre-sputter"], _names(c)
        assert not any(n.startswith("공정 ") for n in _names(c)), _names(c)
        txt = _log_text(c)
        assert "빈 행 4개 건너뜀" in txt, txt
        assert "(총 7개)" in txt, txt
        print(f"  (b) 7개 / {_names(c)}")
    asyncio.run(_main())


# ───────────────────────── (c) UI 파일 선택 경로 ─────────────────────────
def test_c_ui_path_skips_blank_rows(tmp_path):
    p = tmp_path / "recipe.csv"
    _write_csv(p, _presputter_rows(), blank_rows=4)

    async def _main():
        c = _mk_ch(tmp_path)
        c._last_process_list_dir = str(tmp_path)

        async def _aopen_file(*a, **k):
            return str(p)
        c._aopen_file = _aopen_file
        await c._handle_process_list_clicked_async()
        assert len(c.process_queue) == 7, _names(c)
        txt = _log_text(c)
        assert "총 7개 공정 읽음." in txt, txt
        assert "빈 행 4개 건너뜀" in txt, txt
        assert c.warns == [], c.warns
        print(f"  (c) UI 경로 7개, 경고 없음")
    asyncio.run(_main())


# ───────────────────────── (d) 행 번호 보존 ─────────────────────────
def test_d_row_number_preserved_after_skipping_blank(tmp_path):
    """헤더 / 정상 / 쉼표만 / 잘못된 행 → 오류 메시지의 행 번호는 원래 4행."""
    p = tmp_path / "bad.csv"
    ok = ["Pre-sputter", "", "0", "30", "T", "100", "1000", "10", "5e-6", "3", "20"]
    bad = ["Pre-sputter", "", "0", "30", "T", "abc", "1000", "10", "5e-6", "3", "20"]
    with open(p, "w", encoding="utf-8-sig", newline="") as f:
        f.write(",".join(HEADER) + "\r\n")          # 1행
        f.write(",".join(ok) + "\r\n")              # 2행
        f.write("," * (len(HEADER) - 1) + "\r\n")   # 3행 (쉼표만)
        f.write(",".join(bad) + "\r\n")             # 4행

    async def _main():
        c = _mk_ch(tmp_path)
        with pytest.raises(RuntimeError) as ei:
            await c.start_with_recipe_string(str(p), origin="host")
        msg = str(ei.value)
        assert "4행('Pre-sputter') rf_pulse_power='abc'" in msg, msg
        print(f"  (d) {msg.splitlines()[-1].strip()}")
    asyncio.run(_main())


# ───────────────────────── (e) 빈 행 없는 파일 ─────────────────────────
def test_e_no_blank_rows_keeps_behaviour(tmp_path):
    p = tmp_path / "plain.csv"
    r1 = ["A", "", "0", "10", "T", "100", "1000", "10", "5e-6", "3", "20"]
    r2 = ["B", "", "0", "10", "T", "100", "1000", "10", "5e-6", "3", "20"]
    r3 = ["", "", "0", "10", "T", "100", "1000", "10", "5e-6", "3", "20"]   # 이름 없는 행
    _write_csv(p, [r1, r2, r3])

    async def _main():
        c = _mk_ch(tmp_path)
        await c.start_with_recipe_string(str(p), origin="host")
        assert _names(c) == ["A", "B", "공정 3"], _names(c)
        txt = _log_text(c)
        assert "빈 행" not in txt, txt
        assert "(총 3개)" in txt, txt
        print(f"  (e) {_names(c)} / '빈 행' 로그 없음")
    asyncio.run(_main())


# ───────────────────────── (f) PC 행 수 ─────────────────────────
def test_f_pc_row_count_excludes_blank(tmp_path):
    p = tmp_path / "pc.csv"
    with open(p, "w", encoding="utf-8-sig", newline="") as f:
        f.write("#,time,rf_power,working_pressure,Ar_flow\r\n")
        f.write("PC1,10m,100,3,20\r\n")
        f.write(",,,,\r\n")
        f.write(",,,,\r\n")
        f.write("\r\n")                               # 빈 줄

    async def _main():
        pc = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
        pc.logs = []
        pc.append_log = lambda s, m: pc.logs.append((s, m))
        pc._apply_recipe_row_to_ui = lambda row: None

        class _Cfgm:
            PC_HOST_START_WAIT_TIMEOUT_S = 5.0
        pc._cfg_mod = _Cfgm()
        pc._host_start_future = None

        async def _on_click_start(*, origin="ui", origin_meta=None):
            fut = getattr(pc, "_host_start_future", None)
            if fut is not None and not fut.done():
                fut.set_result((True, "ok"))
        pc._on_click_start = _on_click_start
        await pc.start_with_recipe_string(str(p), origin="host")
        assert pc._loaded_recipe_row.get("#") == "PC1", pc._loaded_recipe_row
        assert pc._host_recipe_rows == 1, pc._host_recipe_rows
        print(f"  (f) PC row_count={pc._host_recipe_rows}")
    asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q", "-s"]))
