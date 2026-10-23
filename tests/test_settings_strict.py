# -*- coding: utf-8 -*-
"""settings.json 엄격 검사 — 고칠 곳이 하나라도 있으면 값을 하나도 적용하지 않고, 무엇을 고칠지 알려 준다.

예전: BOM 파일·문법 오류는 통째로 무시({}), 오타 키는 debug 로그만, 타입 불일치는 그 값만 무시 →
사용자가 바꾼 값이 조용히 빠진 채 장비가 돌았다(로그 핸들러도 없어 아무 데도 안 남음).
지금: BOM 은 읽고, 나머지는 막고 목록(어디: 무엇 — 어떻게 고칠지)을 돌려준다. 장치 접근 없음.
"""
import os
import re
import sys
import contextlib
import importlib.util
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(_HERE)
sys.path.insert(0, ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from lib import config_common, config_ch1, config_ch2           # noqa: E402
from lib import _config_loader as L                             # noqa: E402

REPO_JSON = Path(ROOT) / "config" / "settings.json"


@pytest.fixture(autouse=True)
def restore_cfg():
    """검사기가 바꾼 config 모듈 값을 원래대로 돌린다."""
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


def _repo_text() -> str:
    return REPO_JSON.read_text(encoding="utf-8")


def _write(tmp_path, text, *, bom=False) -> Path:
    p = tmp_path / "settings.json"
    p.write_bytes((b"\xef\xbb\xbf" if bom else b"") + text.encode("utf-8"))
    return p


def _sub(text, key, val):
    """repo settings.json 에서 "key": 값 하나를 바꾼다(첫 번째)."""
    out, n = re.subn(rf'("{key}"\s*:\s*)[^,\n]+', lambda mt: mt.group(1) + val, text, count=1)
    assert n == 1, key
    return out


def _line_of(text, needle) -> int:
    return next(i for i, ln in enumerate(text.splitlines(), 1) if needle in ln)


# ── 정상 ──

def test_repo_settings_json_is_clean():
    res = L.check_and_load_settings(REPO_JSON)
    assert res.ok, res.problems
    assert res.applied > 100 and res.data.get("common")
    assert isinstance(config_common.MFC_SCALE_FACTORS, dict) and type(config_common.MFC_SCALE_FACTORS) is dict
    assert all(isinstance(k, int) for k in config_common.MFC_SCALE_FACTORS)   # "1" → 1 변환 유지


def test_bom_is_accepted(tmp_path):
    txt = _sub(_repo_text(), "SHUTDOWN_STEP_GAP_MS", "777")
    res = L.check_and_load_settings(_write(tmp_path, txt, bom=True))
    assert res.ok, res.problems
    assert config_common.SHUTDOWN_STEP_GAP_MS == 777


def test_missing_section_uses_py_values(tmp_path):
    res = L.check_and_load_settings(_write(tmp_path, '{ "common": { "SHUTDOWN_STEP_GAP_MS": 600 } }'))
    assert res.ok and config_common.SHUTDOWN_STEP_GAP_MS == 600


# ── 막는 경우 ──

def test_missing_file(tmp_path):
    p = tmp_path / "none" / "settings.json"
    res = L.check_and_load_settings(p)
    assert res.missing and len(res.problems) == 1 and "설정 파일이 없습니다" in res.problems[0]
    # 예전 호출 방식은 파일이 없으면 .py 값으로 진행
    assert L.load_settings(p) == {}
    assert L.check_and_load_settings(p, require_file=False).ok


def test_missing_comma_reports_line_col_and_text(tmp_path):
    txt = _repo_text()
    txt, n = re.subn(r'("SHUTDOWN_STEP_GAP_MS"\s*:\s*[^,\n]+),', r"\1", txt, count=1)
    assert n == 1
    gap_line = _line_of(txt, '"SHUTDOWN_STEP_GAP_MS"')
    res = L.check_and_load_settings(_write(tmp_path, txt))
    assert len(res.problems) == 1
    p = res.problems[0]
    m = re.match(r"줄 (\d+) 칸 (\d+): 쉼표\(,\)가 빠졌습니다", p)
    assert m, p
    err_line = int(m.group(1))
    # 오류는 쉼표가 빠진 줄 다음의 첫 항목(주석·빈 줄은 건너뜀) — 블록/줄 주석이 있어도 줄 번호가 원본과 같다
    assert err_line > gap_line
    nxt = next(ln for ln in txt.splitlines()[gap_line:] if ln.strip() and not ln.strip().startswith("//"))
    assert txt.splitlines()[err_line - 1] == nxt
    assert f"{err_line}| {nxt.rstrip()}" in p
    assert config_common.SHUTDOWN_STEP_GAP_MS == 500           # 아무것도 적용 안 됨


def test_block_comment_keeps_line_numbers(tmp_path):
    txt = '{\n  /* 여러 줄\n     주석\n  */\n  "common": { "A": 1 "B": 2 }\n}\n'
    res = L.check_and_load_settings(_write(tmp_path, txt))
    assert res.problems and res.problems[0].startswith("줄 5 칸 22: 쉼표"), res.problems


@pytest.mark.parametrize("body,expect", [
    ('{ "common": { "SHUTDOWN_STEP_GAP_MS": 500, } }', "마지막 항목 뒤의 쉼표"),
    ('{ "common": { "LOG_ROOT_DIR": "C:\\VanaM_Logs" } }', "역슬래시"),
    ('{ "common": { "SHUTDOWN_STEP_GAP_MS": True } }', "true/false/null 은 소문자"),
    ('{ "common": { "SHUTDOWN_STEP_GAP_MS" 500 } }', "콜론(:)"),
    ('{ "common": { "LOG_ROOT_DIR": "C:/x } }', "닫히지 않았습니다"),
    ('{ "common": {} } }', "파일 끝 }"),
])
def test_syntax_errors_have_korean_hints(tmp_path, body, expect):
    res = L.check_and_load_settings(_write(tmp_path, body))
    assert len(res.problems) == 1 and expect in res.problems[0], res.problems
    assert res.problems[0].startswith("줄 1 칸 ")


def test_not_utf8(tmp_path):
    p = tmp_path / "settings.json"
    p.write_bytes('{ "common": { "LOG_ROOT_DIR": "가" } }'.encode("cp949"))
    res = L.check_and_load_settings(p)
    assert len(res.problems) == 1 and "UTF-8" in res.problems[0]


def test_top_level_not_object(tmp_path):
    res = L.check_and_load_settings(_write(tmp_path, "[1, 2]"))
    assert len(res.problems) == 1 and "{ }" in res.problems[0]


def test_unknown_key_with_suggestion_and_nothing_applied(tmp_path):
    txt = _sub(_repo_text(), "SHUTDOWN_STEP_GAP_MS", "777")                 # 정상 변경
    txt = txt.replace('"SHUTDOWN_STEP_TIMEOUT_MS"', '"SHUTDOWN_STEP_TIMEOUT_M"', 1)   # 오타
    res = L.check_and_load_settings(_write(tmp_path, txt))
    assert res.problems == ["common.SHUTDOWN_STEP_TIMEOUT_M: 프로그램에 없는 설정 이름입니다 — "
                            "이름 오타를 확인하세요 (혹시 SHUTDOWN_STEP_TIMEOUT_MS ?)"]
    assert res.data == {} and res.applied == 0
    assert config_common.SHUTDOWN_STEP_GAP_MS == 500           # 정상 변경도 적용 안 됨(전부 아니면 전무)


def test_unknown_and_bad_sections(tmp_path):
    res = L.check_and_load_settings(_write(tmp_path, '{ "Ch2": {}, "ch1": 3, "__note": "x" }'))
    assert any(p.startswith('"Ch2": 알 수 없는 구역') and "혹시 ch2" in p for p in res.problems), res.problems
    assert any(p.startswith('"ch1": { ... } 묶음이어야') for p in res.problems), res.problems
    assert len(res.problems) == 2


@pytest.mark.parametrize("val,kind", [
    ('"abc"', '따옴표로 감싼 글자 "abc"'),
    ("null", "null null"),
    ("true", "true/false true"),
    ("[1]", "목록([ ... ]) [1]"),
])
def test_type_mismatch_for_number(tmp_path, val, kind):
    txt = _sub(_repo_text(), "SHUTDOWN_STEP_GAP_MS", val)
    res = L.check_and_load_settings(_write(tmp_path, txt))
    assert res.problems == [f"common.SHUTDOWN_STEP_GAP_MS: {kind} 가 들어 있습니다 — 숫자로 쓰세요 (기본값 예: 500)"]


def test_number_for_bool_is_rejected(tmp_path):
    txt = _sub(_repo_text(), "DCP_REQUIRE_HOST_MODE", "1")
    res = L.check_and_load_settings(_write(tmp_path, txt))
    assert len(res.problems) == 1 and res.problems[0].startswith(
        "common.DCP_REQUIRE_HOST_MODE: 숫자 1 가 들어 있습니다 — true/false로 쓰세요"), res.problems


def test_int_float_still_compatible(tmp_path):
    txt = _sub(_repo_text(), "SHUTDOWN_STEP_GAP_MS", "650.0")
    res = L.check_and_load_settings(_write(tmp_path, txt))
    assert res.ok and config_common.SHUTDOWN_STEP_GAP_MS == 650.0


def test_duplicate_keys(tmp_path):
    body = ('{ "common": { "SHUTDOWN_STEP_GAP_MS": 500, "SHUTDOWN_STEP_GAP_MS": 600,'
            ' "MFC_SCALE_FACTORS": {"1": 1.0, "1": 2.0} }, "ch1": {}, "ch1": {} }')
    res = L.check_and_load_settings(_write(tmp_path, body))
    assert '"ch1" 구역이 두 번 있습니다 — 하나로 합치세요' in res.problems
    assert any(p.startswith("common.SHUTDOWN_STEP_GAP_MS: 같은 이름이 두 번") for p in res.problems)
    assert any(p.startswith('common.MFC_SCALE_FACTORS 안의 "1": 같은 이름이 두 번') for p in res.problems)
    assert len(res.problems) == 3 and config_common.SHUTDOWN_STEP_GAP_MS == 500


def test_path_value_cannot_be_set(tmp_path):
    assert isinstance(config_common.LOCAL_FALLBACK_ROOT, Path)
    res = L.check_and_load_settings(_write(tmp_path, '{ "common": { "LOCAL_FALLBACK_ROOT": "D:/x" } }'))
    assert res.problems == ["common.LOCAL_FALLBACK_ROOT: settings.json 으로 바꿀 수 없는 항목입니다 — 이 줄을 지우세요"]


def test_all_problems_listed_at_once(tmp_path):
    txt = _repo_text()
    txt = _sub(txt, "SHUTDOWN_STEP_GAP_MS", '"x"')
    txt = _sub(txt, "DCP_REQUIRE_HOST_MODE", "0")
    txt = txt.replace('"SHUTDOWN_STEP_TIMEOUT_MS"', '"SHUTDOWN_STEP_TIMEOUT"', 1)
    res = L.check_and_load_settings(_write(tmp_path, txt))
    assert len(res.problems) == 3, res.problems
    text = L.format_problems(res)
    assert text.startswith("1. ") and "\n2. " in text and "\n3. " in text
    short = L.format_problems(res, limit=2)
    assert short.endswith("... 외 1건 (자세히 보기)") and "\n3. " not in short


def test_load_settings_legacy_applies_nothing_on_problem(tmp_path):
    txt = _sub(_sub(_repo_text(), "SHUTDOWN_STEP_GAP_MS", "777"), "SHUTDOWN_STEP_TIMEOUT_MS", '"x"')
    assert L.load_settings(_write(tmp_path, txt)) == {}
    assert config_common.SHUTDOWN_STEP_GAP_MS == 500


# ── 알림창 ──

def test_error_box_lists_what_to_fix(tmp_path):
    from PySide6.QtWidgets import QApplication, QMessageBox
    app = QApplication.instance() or QApplication([])          # noqa: F841
    from util.settings_error_box import settings_error_box, SHOW_MAX
    txt = _sub(_repo_text(), "SHUTDOWN_STEP_GAP_MS", '"x"')
    res = L.check_and_load_settings(_write(tmp_path, txt))
    box = settings_error_box(res)
    assert box.icon() == QMessageBox.Icon.Critical
    assert "프로그램을 시작하지 않습니다" in box.windowTitle()
    assert "고칠 곳이 1건" in box.text()
    assert str(res.path) in box.informativeText()
    assert "1. common.SHUTDOWN_STEP_GAP_MS: 따옴표로 감싼 글자 \"x\" 가 들어 있습니다 — 숫자로 쓰세요" in box.informativeText()
    assert box.detailedText() == ""
    many = L.SettingsResult(path=res.path, problems=[f"p{i}" for i in range(SHOW_MAX + 3)])
    box2 = settings_error_box(many)
    assert "외 3건" in box2.informativeText() and f"{SHOW_MAX + 3}. p{SHOW_MAX + 2}" in box2.detailedText()


def test_main_blocks_before_devices():
    """main(): 검사 → 로깅 → QApplication → 고칠 곳 있으면 알림창·return 1 이 QEventLoop/MainWindow 보다 먼저."""
    src = (Path(ROOT) / "main.py").read_text(encoding="utf-8")
    body = src[src.index("def main() -> int:"):]
    i_chk = body.index("cfg_res = check_and_load_settings()")
    i_log = body.index("setup_app_logging(")
    i_app = body.index("app = QApplication(sys.argv)")
    i_box = body.index("settings_error_box(cfg_res).exec()")
    i_ret = body.index("return 1", i_box)
    i_loop = body.index("loop = QEventLoop(app)")
    i_win = body.index("w = MainWindow(loop)")
    assert i_chk < i_log < i_app < i_box < i_ret < i_loop < i_win


# ── 워커 ──

def _load_log_sync():
    spec = importlib.util.spec_from_file_location(
        "log_sync_settings_strict_under_test", os.path.join(ROOT, "apps", "log_sync_service", "log_sync.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_log_sync_worker_exits_on_bad_settings(tmp_path, monkeypatch):
    LS = _load_log_sync()
    bad = _write(tmp_path, _sub(_repo_text(), "LOG_SYNC_ALERT_AFTER_FAILS", '"six"'))
    monkeypatch.setenv("LOG_SYNC_SETTINGS", str(bad))
    base = tmp_path / "exe"
    base.mkdir()
    monkeypatch.setattr(LS, "_worker_base_dir", lambda: base)
    sent = []
    monkeypatch.setattr(LS, "notify", lambda m: sent.append(m))
    monkeypatch.setattr(LS, "_load_notify_config", lambda: None)
    logs = []
    with pytest.raises(SystemExit) as ei:
        LS._load_config(lambda *x: logs.append(" ".join(map(str, x))))
    assert ei.value.code == 2
    assert "common.LOG_SYNC_ALERT_AFTER_FAILS: 따옴표로 감싼 글자 \"six\" 가 들어 있습니다" in logs[0]
    assert "LOG_SYNC_ALERT_AFTER_FAILS" in (base / "run_error.log").read_text(encoding="utf-8")
    assert sent and sent[0].startswith("settings.json 오류로 로그 동기화 워커 중지: common.LOG_SYNC_ALERT_AFTER_FAILS")


def test_log_sync_worker_ok_and_missing(tmp_path, monkeypatch):
    LS = _load_log_sync()
    good = _write(tmp_path, _sub(_repo_text(), "LOG_SYNC_ALERT_AFTER_FAILS", "9"))
    monkeypatch.setenv("LOG_SYNC_SETTINGS", str(good))
    cfg = LS._load_config(lambda *x: None)
    assert cfg["alert_after_fails"] == 9 and cfg["settings"] == good
    monkeypatch.setenv("LOG_SYNC_SETTINGS", str(tmp_path / "nope.json"))
    logs = []
    cfg = LS._load_config(lambda *x: logs.append(" ".join(map(str, x))))
    assert cfg["settings"] is None and "settings.json 없음" in logs[0]


def test_rf_worker_uses_same_strict_path():
    """rf_worker 는 import 가 무겁다(onnx·cv2) — 같은 구조인지 텍스트로 확인."""
    src = (Path(ROOT) / "apps" / "rf_service" / "rf_worker.py").read_text(encoding="utf-8")
    body = src[src.index("def _load_config(log):"):src.index("from lib import config_common as cfgc", src.index("def _load_config(log):"))]
    assert "check_and_load_settings(settings)" in body and "_settings_fail(res, log)" in body
    assert "load_settings(path=settings)" not in body and "except Exception" not in body
    fail = src[src.index("def _settings_fail(res, log)"):src.index("def _load_config(log):")]
    assert "sys.exit(2)" in fail and "run_error.log" in fail and "RF 워커 중지" in fail
