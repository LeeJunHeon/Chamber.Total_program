# -*- coding: utf-8 -*-
"""settings.json 워커 모드 — log_sync·rf_worker 는 자기가 쓰는 common 키만 엄격하게 보고, 메인에만 있는 새 키는 무시한다.

예전(10-28 장비): 두께 모니터 키가 들어간 새 settings.json 을 배치하자 예전 코드로 빌드된 log_sync exe 가
'ch1.SUPPORTS_THICKNESS: 프로그램에 없는 설정 이름 외 35건' 으로 10분마다 멈추고 구글챗을 보냈다.
지금: 문법 오류·중복 키·자기 키 오타/타입 오류만 멈추고, 모르는 키는 run.log 한 줄로 넘어간다. 메인 검사는 그대로.
"""
import ast
import contextlib
import importlib.util
import json
import os
import re
import sys
from pathlib import Path

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                    # noqa: E402

from lib import config_common, config_ch1, config_ch2           # noqa: E402
from lib import _config_loader as L                             # noqa: E402

LS_KEYS = ("LOG_SYNC_ENABLED", "LOG_SYNC_PAIRS", "LOG_SYNC_STATE_DIR", "LOG_SYNC_ALERT_AFTER_FAILS")


@pytest.fixture(autouse=True)
def restore_cfg():
    mods = (config_common, config_ch1, config_ch2)
    snap = [dict(vars(md)) for md in mods]
    yield
    for md, d in zip(mods, snap):
        for k in [k for k in vars(md) if k not in d]:
            delattr(md, k)
        for k, v in d.items():
            setattr(md, k, v)


def _write(tmp_path, text) -> Path:
    p = tmp_path / "settings.json"
    p.write_text(text, encoding="utf-8")
    return p


# 메인이 더 새 버전 — 워커 config 에 없는 키(ch1/ch2 의 새 두께 키, common 의 새 키, 새 구역)
NEWER = """{
  // 주석
  "common": {
    "LOG_SYNC_ENABLED": false,
    "LOG_SYNC_ALERT_AFTER_FAILS": 9,
    "LOG_ROOT_DIR": "D:/elsewhere",
    "THICKNESS_FUTURE_COMMON_KEY": 1.5
  },
  "ch1": {"SUPPORTS_THICKNESS_NEW_FLAG": true, "THICKNESS_FUTURE_NM": 3},
  "ch2": {"THICKNESS_FUTURE_NM": 4},
  "ch9": {"X": 1}
}"""


def test_worker_mode_ignores_unknown_keys_and_applies_only_own_keys(tmp_path):
    old_root = config_common.LOG_ROOT_DIR
    res = L.check_and_load_settings(_write(tmp_path, NEWER), worker_keys=LS_KEYS)
    assert res.problems == []
    assert config_common.LOG_SYNC_ENABLED is False and config_common.LOG_SYNC_ALERT_AFTER_FAILS == 9
    assert config_common.LOG_ROOT_DIR == old_root                       # 자기 키가 아니면 적용하지 않는다
    assert res.applied == 2
    assert sorted(res.ignored) == ["ch1.SUPPORTS_THICKNESS_NEW_FLAG", "ch1.THICKNESS_FUTURE_NM", "ch2.THICKNESS_FUTURE_NM",
                                   "ch9", "common.THICKNESS_FUTURE_COMMON_KEY"]


def test_main_mode_still_rejects_the_same_file(tmp_path):
    """메인 검사 결과는 그대로 — 모르는 이름·모르는 구역은 고칠 곳, 아무것도 적용하지 않음."""
    res = L.check_and_load_settings(_write(tmp_path, NEWER))
    assert len(res.problems) == 5 and res.ignored == []
    assert config_common.LOG_SYNC_ENABLED is True


@pytest.mark.parametrize("text,word", [
    ('{"common": {"LOG_SYNC_ENABLED": 1}}', "LOG_SYNC_ENABLED"),                          # 자기 키 타입(숫자 → bool 아님)
    ('{"common": {"LOG_SYNC_PAIRS": "C:/a"}}', "LOG_SYNC_PAIRS"),                          # 자기 키 타입(목록 자리에 글자)
    ('{"common": {"LOG_SYNC_PAIR": [["a", "b"]]}}', "혹시 LOG_SYNC_PAIRS"),               # 자기 키 오타 의심
    ('{"common": {"LOG_SYNC_ENABLE": true}}', "혹시 LOG_SYNC_ENABLED"),
    ('{"common": {"LOG_SYNC_ENABLED": true,}}', "줄"),                                     # JSON 문법 오류
    ('{"common": {"LOG_SYNC_ENABLED": true, "LOG_SYNC_ENABLED": false}}', "두 번"),        # 중복 키(자기 키)
    ('{"common": {}, "ch1": {"X_NEW": 1, "X_NEW": 2}}', "ch1.X_NEW"),                      # 중복 키(안 쓰는 구역도 — 파일이 깨짐)
    ('{"common": {}, "common": {}}', "두 번"),                                             # 중복 구역
    ('{"common": [1, 2]}', "묶음"),
    ('[1]', "{ }"),
])
def test_worker_mode_stops_on_broken_file_or_own_key_errors(tmp_path, text, word):
    res = L.check_and_load_settings(_write(tmp_path, text), worker_keys=LS_KEYS)
    assert res.problems and any(word in p for p in res.problems), res.problems
    assert config_common.LOG_SYNC_ENABLED is True                       # 아무것도 적용하지 않음


def test_similar_but_different_new_key_is_not_a_typo(tmp_path):
    """같은 접두어의 새 키(글자 여러 개 차이)는 오타로 보지 않는다 — 메인에 RF_READER_* 키가 늘어도 워커가 멈추지 않게."""
    keys = ("RF_READER_CSV_DIR", "RF_READER_NAS_DIR")
    res = L.check_and_load_settings(_write(tmp_path, '{"common": {"RF_READER_LOG_DIR": "x", "RF_READER_NAS_URL": "y"}}'),
                                    worker_keys=keys)
    assert res.problems == [] and len(res.ignored) == 2


def test_repo_settings_json_passes_worker_mode_for_both_workers():
    repo = Path(ROOT) / "config" / "settings.json"
    for keys in (LS_KEYS, _worker_keys("apps/rf_service/rf_worker.py")):
        res = L.check_and_load_settings(repo, worker_keys=keys)
        assert res.problems == [] and res.ignored == []


# ── 각 워커가 읽는 키 목록이 코드와 같은지(누락 방지) ──
def _src(rel):
    return (Path(ROOT) / rel).read_text(encoding="utf-8")


def _worker_keys(rel):
    tree = ast.parse(_src(rel))
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "SETTINGS_KEYS" for t in n.targets):
            return tuple(ast.literal_eval(n.value))
    raise AssertionError(f"{rel}: SETTINGS_KEYS 없음")


@pytest.mark.parametrize("rel", ["apps/log_sync_service/log_sync.py", "apps/rf_service/rf_worker.py"])
def test_worker_settings_keys_match_code(rel):
    src = _src(rel)
    used = set(re.findall(r'getattr\(\s*cfgc\s*,\s*"([A-Z0-9_]+)"', src))
    assert used and set(_worker_keys(rel)) == used
    assert not re.search(r"cfgc\.[A-Z]", src)                          # getattr 말고 직접 읽는 곳 없음
    assert "worker_keys=SETTINGS_KEYS" in src
    for k in used:
        assert hasattr(config_common, k), k                           # 모두 config_common 에 있는 이름


# ── log_sync 워커 끝까지(설정 읽기) ──
def _load_log_sync():
    spec = importlib.util.spec_from_file_location("log_sync_worker_mode_under_test",
                                                  os.path.join(ROOT, "apps", "log_sync_service", "log_sync.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_log_sync_newer_settings_logs_one_line_and_runs(tmp_path, monkeypatch):
    LS = _load_log_sync()
    sent = []
    monkeypatch.setattr(LS, "notify", sent.append)
    monkeypatch.setenv("LOG_SYNC_SETTINGS", str(_write(tmp_path, NEWER)))
    lines = []
    cfg = LS._load_config(lines.append)
    assert cfg["enabled"] is False and cfg["alert_after_fails"] == 9
    ign = [ln for ln in lines if "모르는 키" in ln]
    assert len(ign) == 1 and "5개" in ign[0] and "메인 프로그램이 더 새 버전일 수 있음" in ign[0]
    assert sent == []                                                  # 구글챗 없음


def test_log_sync_own_key_typo_stops_with_chat(tmp_path, monkeypatch):
    LS = _load_log_sync()
    sent = []
    monkeypatch.setattr(LS, "notify", sent.append)
    monkeypatch.setattr(LS, "_worker_base_dir", lambda: tmp_path)
    monkeypatch.setenv("LOG_SYNC_SETTINGS", str(_write(tmp_path, '{"common": {"LOG_SYNC_PAIR": []}}')))
    with pytest.raises(SystemExit) as ei:
        LS._load_config(lambda *_a: None)
    assert ei.value.code == 2 and len(sent) == 1 and "LOG_SYNC_PAIR" in sent[0]
    assert (tmp_path / "run_error.log").is_file()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
