"""전수 목록 gap A(v2/ch12/spec/inv_gap_a.py) — 기록 파일·두께 Factor/재료 파일의 경고 문구(v1 문구 그대로, 로거당 1회).

GAPA-001~012(장비 드라이버 문구)는 v2/tests/test_inv_dev.py 에 있다.
"""
import json
from datetime import datetime

from v2.ch12 import thickness_factors as TF
from v2.ch12 import thickness_materials as TM
from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.heater_csv import HeaterCsv
from v2.ch12.outputs.thickness_csv import ThicknessCsv
from v2.tests.outputs_golden import Clock, make_root, v2_cfg
from v2.tools import v1golden_outputs_scen as SC


def _heater(tmp_path, over):
    root = make_root(tmp_path)
    (root / "막힘").write_text("x", encoding="utf-8")
    cfg = v2_cfg(root, SC.BASE_CFG, over(root))
    warns = []
    clock = Clock("2026-10-30T10:00:00")
    w = QueueWriter("t")
    h = HeaterCsv(cfg, writer=w, warn=warns.append, monotonic=clock.monotonic, now=clock.now)
    h.start()
    h.write_row({"run": True, "pv": 25.0})
    h.start()                                    # 같은 로거 — 경고는 1번만
    h.stop()
    w.stop()
    return root, warns


def test_heater_csv_folder_fallback_and_none_warn_once(tmp_path):
    """GAPA-013 — 지정 폴더를 못 만들면 폴백에 쓰고 '히터 CSV 폴더(기본)를 만들 수 없어 폴백 에 저장합니다', 둘 다 실패면
    '히터 CSV 폴더를 만들 수 없어 기록하지 않습니다' — 로거 인스턴스당 1회, 공정을 막지 않음."""
    root, warns = _heater(tmp_path / "a", lambda r: {"CH2_HEATER_LOG_DIR": str(r / "막힘" / "H")})
    fb = root / "exe 폴더" / "Logs_LocalFallback" / "CH2_Heater"
    assert warns == [f"히터 CSV 폴더({root / '막힘' / 'H'})를 만들 수 없어 {fb} 에 저장합니다"]
    assert len(list(fb.glob("CH2_HEATER_*.csv"))) == 1
    root, warns = _heater(tmp_path / "b", lambda r: {"CH2_HEATER_LOG_DIR": str(r / "막힘" / "H"),
                                                     "LOCAL_FALLBACK_ROOT": str(r / "막힘" / "F")})
    assert warns == ["히터 CSV 폴더를 만들 수 없어 기록하지 않습니다"]


def test_heater_csv_open_and_write_failures(tmp_path, monkeypatch):
    """GAPA-013 — 파일 열기 실패 '히터 CSV 열기 실패: {e!r}', 행 쓰기 실패 '히터 CSV 기록 실패(이후 경고 생략): {e!r}'."""
    import builtins
    root = make_root(tmp_path)
    cfg = v2_cfg(root, SC.BASE_CFG)
    warns = []
    clock = Clock("2026-10-30T10:00:00")
    real_open = builtins.open

    def bad_open(path, *a, **k):
        if str(path).endswith(".csv") and "CH2_HEATER_" in str(path):
            raise PermissionError("잠김")
        return real_open(path, *a, **k)
    monkeypatch.setattr(builtins, "open", bad_open)
    w = QueueWriter("t")
    h = HeaterCsv(cfg, writer=w, warn=warns.append, monotonic=clock.monotonic, now=clock.now)
    h.start()
    h.stop()
    w.stop()
    monkeypatch.setattr(builtins, "open", real_open)
    assert warns == ["히터 CSV 열기 실패: PermissionError('잠김')"]
    warns2 = []
    w = QueueWriter("t")
    h = HeaterCsv(cfg, writer=w, warn=warns2.append, monotonic=clock.monotonic, now=clock.now)
    h.start()
    w.stop()
    h._writer = type("W", (), {"writerow": lambda self, row: (_ for _ in ()).throw(OSError("디스크"))})()
    h._do_write(["x"])
    h._do_write(["y"])
    assert warns2 == ["히터 CSV 기록 실패(이후 경고 생략): OSError('디스크')"]
    h._do_stop()


def test_thickness_csv_open_and_write_failures_retry_after_60s(tmp_path):
    """GAPA-014 — 지정·폴백 모두 실패면 '두께 기록 CSV 를 열 수 없어 기록하지 않습니다(60초 뒤 재시도): {e!r}',
    행 쓰기 실패면 '두께 기록 CSV 쓰기 실패(60초 뒤 다시 엶): {e!r}'(파일 닫고 60 s 뒤 다시 엶). 로거당 1회."""
    root = make_root(tmp_path)
    (root / "막힘").write_text("x", encoding="utf-8")
    cfg = v2_cfg(root, SC.BASE_CFG, {"THICKNESS_LOG_DIR": "@/막힘/T", "LOCAL_FALLBACK_ROOT": "@/막힘/F"})
    warns = []
    mono = [0.0]
    w = QueueWriter("t")
    t = ThicknessCsv(2, cfg, writer=w, warn=warns.append, monotonic=lambda: mono[0])
    t.set_recording(True, "공정", {"connected": True}, datetime(2026, 10, 30, 9, 0, 0))
    t.on_tick({"connected": True}, datetime(2026, 10, 30, 9, 0, 1))
    t.stop()
    w.stop()
    assert len(warns) == 1 and warns[0].startswith("두께 기록 CSV 를 열 수 없어 기록하지 않습니다(60초 뒤 재시도): ")
    root = make_root(tmp_path / "2")
    cfg = v2_cfg(root, SC.BASE_CFG, {"THICKNESS_LOG_DIR": "@/T"})
    warns = []
    w = QueueWriter("t")
    t = ThicknessCsv(2, cfg, writer=w, warn=warns.append, monotonic=lambda: mono[0])
    t.set_recording(True, "공정", {"connected": True}, datetime(2026, 10, 30, 9, 0, 0))
    w.stop()
    inner = t.file
    inner._writer = type("W", (), {"writerow": lambda self, row: (_ for _ in ()).throw(OSError("디스크"))})()
    inner.write_row({"connected": True}, datetime(2026, 10, 30, 9, 0, 2), 1.0)
    assert warns == ["두께 기록 CSV 쓰기 실패(60초 뒤 다시 엶): OSError('디스크')"]


def test_material_list_warnings_and_duplicate_save(tmp_path, monkeypatch):
    """GAPA-015 — 재료 목록 파일 생성 실패·읽기 실패(기본 목록 사용), 항목 오류·이름 중복 제외, 저장 때 이름 중복 ValueError."""
    blocked = tmp_path / "막힘"
    blocked.write_text("x", encoding="utf-8")
    mats, warns = TM.load_materials(blocked / "thickness_materials.json")
    assert mats and any(w.startswith(f"재료 목록 파일을 만들 수 없음({blocked / 'thickness_materials.json'}) — 기본 목록 사용: ")
                        for w in warns)
    p = tmp_path / "m.json"
    p.write_text("{깨짐", encoding="utf-8")
    mats, warns = TM.load_materials(p)
    assert mats and warns[0].startswith("재료 목록 파일을 읽을 수 없음(m.json) — 기본 목록 사용: ")
    p.write_text(json.dumps({"version": 1, "materials": [
        5, {"name": "", "symbol": "X", "density": 1.0, "z_factor": 1.0},
        {"name": "Al", "symbol": "Al", "density": "a", "z_factor": 1.0},
        {"name": "Cu", "symbol": "Cu", "density": 200.0, "z_factor": 1.0},
        {"name": "Ti", "symbol": "Ti", "density": 4.5, "z_factor": 0.628},
        {"name": "ti", "symbol": "Ti", "density": 4.5, "z_factor": 0.628}]}), encoding="utf-8")
    mats, warns = TM.load_materials(p)
    assert [m.name for m in mats] == ["Ti"]
    assert "1번째 항목이 표(객체)가 아님 — 제외" in warns
    assert "2번째 항목 '': 이름이 비어 있음 — 제외" in warns
    assert "3번째 항목 'Al': density 'a' 숫자가 아님 — 제외" in warns
    assert any(w.startswith("4번째 항목 'Cu': density 200 범위(") and w.endswith("밖 — 제외") for w in warns)
    assert "6번째 항목 'ti': 이름 중복 — 제외" in warns
    import pytest
    with pytest.raises(ValueError, match="^재료 이름 중복: "):
        TM.save_materials(tmp_path / "out.json", [mats[0], mats[0]])
    assert not (tmp_path / "out.json").exists()


def test_factor_file_warnings(tmp_path):
    """GAPA-016 — Factor 파일 읽기 실패·형식 오류(빈 표), tooling/material 표가 아님, 칸 숫자 아님·범위 밖(그 칸만 미설정)."""
    p = tmp_path / "thickness_factors_ch2.json"
    p.write_text("{깨짐", encoding="utf-8")
    fac, warns = TF.load_factors(p)
    assert warns and warns[0].startswith("Factor 파일을 읽을 수 없음(thickness_factors_ch2.json) — 빈 표로 시작: ")
    p.write_text("[1, 2]", encoding="utf-8")
    assert TF.load_factors(p)[1] == ["Factor 파일 형식 오류(thickness_factors_ch2.json) — 빈 표로 시작"]
    p.write_text(json.dumps({"version": 2, "tooling": 3, "material": 4}), encoding="utf-8")
    w = TF.load_factors(p)[1]
    assert "tooling 이 표(객체)가 아님 — 전부 미설정" in w and "material 이 표(객체)가 아님 — 전부 미선택" in w
    p.write_text(json.dumps({"version": 2, "tooling": {"up": 1, "mid": {"G1": "x", "G2": 500, "G3": 100}},
                             "material": {"G1": 7, "G2": {"material": "Ti"}}}), encoding="utf-8")
    fac, w = TF.load_factors(p)
    assert "tooling.up 가 표(객체)가 아님 — 그 줄 미설정" in w
    assert "tooling[mid][G1] 'x' 숫자가 아님 — 미설정" in w and "tooling[mid][G2] 500 범위(10~399%) 밖 — 미설정" in w
    assert "material.G1 가 표(객체)가 아님 — 미선택" in w
