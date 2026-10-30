"""v2 설정 → 독립 도구(apps/log_sync_service·apps/rf_service)가 읽는 v1 형식 config/settings.json (결정 D13).

두 독립 도구는 v1 그대로 쓰고 v1 형식 settings.json 을 '워커 모드'로 직접 읽는다(v1 lib/_config_loader.py:19-28·344-379 —
자기 common 키(SETTINGS_KEYS)만 엄격 검사·적용, ch1/ch2 구역·자기 키 아닌 common 키는 무시). v2 에서는 그 키들의 정본이 v2 설정
(params core·outputs 조각)이므로, 이 도구가 v2 값에서 그 키만 뽑아 v1 형식 파일로 내보낸다.

실행: cd <저장소 루트> && python3 -m v2.tools.export_worker_settings --config-dir <v2 설정 폴더> --out <폴더>/config/settings.json [--force]
      --config-dir 를 주지 않으면 v2 기본값(= v1 운영값). 도구는 settings.json 을 env(LOG_SYNC_SETTINGS 등) 또는 시작 폴더에서
      위로 config/settings.json 을 찾는다(apps/log_sync_service/log_sync.py:66-) — 그 자리에 둔다.

값 규칙
- 키 = 두 도구의 SETTINGS_KEYS(apps/log_sync_service/log_sync.py:55, apps/rf_service/rf_worker.py:203-204) — v2 키 이름이 같다.
- v2 의 JSON 목록 글자(LOG_SYNC_PAIRS)는 v1 목록 값으로 되돌린다. 숫자·bool·글자는 그대로.
- 형식은 v1 사실표(settings_v1facts KEYS)의 v1 형식과 맞아야 한다(워커 모드가 자기 키 형식 불일치면 멈춘다) — 아니면 오류로 쓰지 않음.
- 파일: {"common": {...}} 하나, UTF-8·들여쓰기 2·끝 줄바꿈, 임시 파일 뒤 교체. 이미 있으면 --force 가 없으면 멈춘다.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Optional

from v2.ch12.params import table as params_table
from v2.ch12.spec import settings_v1facts as facts
from v2.common.config_store import ConfigStore
from v2.common.paramdef import PType, defaults

# v1 apps/log_sync_service/log_sync.py:55 · apps/rf_service/rf_worker.py:203-204
WORKER_KEYS = {
    "log_sync": ("LOG_SYNC_ENABLED", "LOG_SYNC_PAIRS", "LOG_SYNC_STATE_DIR", "LOG_SYNC_ALERT_AFTER_FAILS"),
    "rf_worker": ("CAMERA_LOG_ROOT", "RF_READER_STATE_DIR", "RF_READER_CSV_DIR", "RF_READER_STALE_MIN",
                  "RF_READER_NAS_DIR", "RF_READER_RETENTION_DAYS", "RF_READER_MIN_FREE_GB", "RF_READER_UPLOAD_ALERT_DAYS"),
}
ALL_KEYS = tuple(k for keys in WORKER_KEYS.values() for k in keys)
_FACT = {k[0]: k for k in facts.KEYS}


def _v1_kind_ok(key: str, v: Any) -> bool:
    """v1 _type_compatible(기존 .py 값 형식, 새 값) — bool 은 bool 끼리, int↔float 호환, list↔tuple."""
    f = _FACT.get(f"common.{key}")
    if f is None:
        return False
    fmt = f[3]
    if fmt == "true/false":
        return isinstance(v, bool)
    if isinstance(v, bool):
        return False
    if fmt.startswith("숫자"):
        return isinstance(v, (int, float))
    if fmt == "글자":
        return isinstance(v, str)
    if fmt == "목록":
        return isinstance(v, (list, tuple))
    return False


def worker_values(values: Mapping[str, Any], table=None) -> dict:
    """v2 값 사전 → {키: v1 값}. 형식이 v1 과 맞지 않으면 ValueError(파일을 쓰지 않는다)."""
    t = table or params_table()
    by = t.by_key()
    out, bad = {}, []
    for k in ALL_KEYS:
        p = by[k]
        v = values.get(k, p.default)
        if p.type is PType.STR and _FACT.get(f"common.{k}", (None, None, None, ""))[3] == "목록":
            v = json.loads(v)
        if not _v1_kind_ok(k, v):
            bad.append(f"{k}={v!r}")
        out[k] = v
    if bad:
        raise ValueError("v1 형식과 맞지 않는 값(워커가 멈춘다): " + ", ".join(bad))
    return out


def build(values: Mapping[str, Any], table=None) -> dict:
    return {"common": worker_values(values, table)}


def load_values(config_dir: Optional[Path], table=None) -> dict:
    t = table or params_table()
    if config_dir is None:
        return defaults(t.params)
    return ConfigStore(Path(config_dir), t.params, t.rules, t.schema_hash()).load()


def write(obj: dict, out: Path, *, force: bool = False) -> Path:
    out = Path(out)
    if out.exists() and not force:
        raise FileExistsError(f"{out}: 이미 있다 — 덮어쓰려면 --force")
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    tmp.replace(out)
    return out


def main(argv: Optional[list] = None) -> int:
    ap = argparse.ArgumentParser(description="v2 설정 → 독립 도구(log_sync·rf_service)용 v1 형식 settings.json")
    ap.add_argument("--config-dir", default=None, help="v2 설정 폴더(config_active.json) — 없으면 v2 기본값")
    ap.add_argument("--out", required=True, help="쓸 settings.json 경로")
    ap.add_argument("--force", action="store_true", help="이미 있으면 덮어쓴다")
    a = ap.parse_args(argv)
    try:
        obj = build(load_values(Path(a.config_dir) if a.config_dir else None))
        p = write(obj, Path(a.out), force=a.force)
    except Exception as e:  # noqa: BLE001 — 사람이 읽는 한 줄로
        print(f"오류: {e}", file=sys.stderr)
        return 1
    print(f"{p}: {len(obj['common'])}개 키 ({', '.join(obj['common'])})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
