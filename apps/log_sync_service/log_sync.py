# apps/log_sync_service/log_sync.py
# -*- coding: utf-8 -*-
"""
로컬 로그 → NAS 단방향 증분 동기화 워커 (한 번 실행하고 끝나는 배치, 상주 루프 없음).

메인 프로그램은 로그를 로컬(C:)에만 쓴다. 이 워커가 별도 프로세스로 주기 실행(작업 스케줄러 등)되어
NAS 로 복사하므로, SMB 가 느리거나 멈춰도 메인 프로그램(qasync 단일 루프)에는 영향이 없다.

동작
----
- LOG_SYNC_PAIRS 의 각 [src, dst] 에 대해 src 를 재귀 순회.
- dst 의 같은 상대경로 파일과 크기·mtime 이 모두 같으면 건너뜀.
- 다르면 dst 에 "{이름}.tmp" 로 복사 → os.replace → 크기 검증 → copystat(mtime 맞춤).
- ★ dst 에서 파일을 지우지 않는다(미러 아님). ★ src 는 읽기 전용.
- 쓰는 중인 파일도 복사 대상(다음 실행에서 갱신됨). 잠겨서 읽기 실패하면 그 파일만 건너뜀.
- dst 루트 접근 불가면 그 쌍을 통째로 실패 집계. 예외로 프로세스를 죽이지 않는다.

상태/알림
---------
- LOG_SYNC_STATE_DIR/state.json : 연속 실패 횟수, 마지막 성공 시각
- LOG_SYNC_STATE_DIR/run.log
- 연속 실패가 LOG_SYNC_ALERT_AFTER_FAILS 에 도달하는 순간 1회만 구글챗 알림
  (exe 옆 log_sync_config.json 의 webhook_url / enabled — 번들하지 않음)

설정(settings.json common → lib.config_common; getattr 로 호출 시점 조회)
  LOG_SYNC_ENABLED, LOG_SYNC_PAIRS, LOG_SYNC_STATE_DIR, LOG_SYNC_ALERT_AFTER_FAILS
  환경변수 LOG_SYNC_SETTINGS 로 settings.json 경로 직접 지정 가능
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import ssl
import sys
import time
import traceback
import urllib.request
from datetime import datetime
from pathlib import Path

# ✅ 개발(py 실행)일 때만 저장소 루트를 sys.path 에 넣어 lib.* 를 찾게 한다
if not getattr(sys, "frozen", False):
    _ROOT = Path(__file__).resolve().parents[2]
    if str(_ROOT) not in sys.path:
        sys.path.insert(0, str(_ROOT))


# ===================== 경로 헬퍼 =====================
def _worker_base_dir() -> Path:
    """exe 옆(번들되지 않는 파일: log_sync_config.json) 기준 폴더."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _find_settings_json() -> Path | None:
    """1) env LOG_SYNC_SETTINGS  2) 시작 폴더에서 위로 최대 4단계 config/settings.json."""
    env = os.environ.get("LOG_SYNC_SETTINGS", "").strip()
    if env:
        return Path(env)
    if getattr(sys, "frozen", False):
        start = Path(sys.executable).parent
    else:
        start = Path(__file__).resolve().parent
    cur = start
    for _ in range(5):          # 시작 폴더 + 위로 4단계
        cand = cur / "config" / "settings.json"
        if cand.is_file():
            return cand
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def _load_config(log):
    """settings.json → config_common 적용 후 getattr 로 조회. 반환 dict."""
    settings = _find_settings_json()
    try:
        from lib import _config_loader
        if settings is not None and settings.is_file():
            _config_loader.load_settings(path=settings)
        else:
            log(f"  ! settings.json 없음 → config_common 기본값 사용 (탐색 결과: {settings})")
            settings = None
    except Exception as e:
        log(f"  ! settings.json 적용 실패({e}) → config_common 기본값 사용")
        settings = None
    from lib import config_common as cfgc
    pairs_raw = getattr(cfgc, "LOG_SYNC_PAIRS", [
        [r"C:\VanaM_Logs\CH1&2", r"\\VanaM_NAS\VanaM_Sputter\Sputter\Logs\CH1&2"],
    ])
    pairs = []
    for p in pairs_raw or []:
        try:
            src, dst = p[0], p[1]
            if src and dst:
                pairs.append((str(src), str(dst)))
        except Exception:
            log(f"  ! LOG_SYNC_PAIRS 항목 무시: {p!r}")
    return {
        "enabled":   bool(getattr(cfgc, "LOG_SYNC_ENABLED", True)),
        "pairs":     pairs,
        "state_dir": str(getattr(cfgc, "LOG_SYNC_STATE_DIR", r"C:\VanaM_Logs\_sync_state")),
        "alert_after_fails": int(getattr(cfgc, "LOG_SYNC_ALERT_AFTER_FAILS", 6)),
        "settings":  settings,
    }


# ===================== 구글챗 알림 (rf_worker 와 동일 방식) =====================
_NOTIFY_URL = ""
_NOTIFY_ENABLED = False


def _load_notify_config() -> None:
    global _NOTIFY_URL, _NOTIFY_ENABLED
    cfg_path = _worker_base_dir() / "log_sync_config.json"
    try:
        if not cfg_path.exists():
            return
        with open(cfg_path, "r", encoding="utf-8") as fp:
            cfg = json.loads(fp.read())
        _NOTIFY_ENABLED = bool(cfg.get("enabled", True))
        _NOTIFY_URL = str(cfg.get("webhook_url", "") or "").strip()
    except Exception:
        pass


def notify(msg: str) -> None:
    """Google Chat webhook (blocking, 실패 전부 무시). URL 없으면 건너뜀."""
    if not _NOTIFY_ENABLED or not _NOTIFY_URL:
        return
    try:
        payload = json.dumps({"text": f"[LogSync] {msg}"}).encode("utf-8")
        req = urllib.request.Request(_NOTIFY_URL, data=payload,
                                     headers={"Content-Type": "application/json"})
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=5, context=ctx) as resp:
            resp.read()
    except Exception:
        pass


# ===================== 상태 =====================
def load_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            st = json.load(f)
        if isinstance(st, dict):
            return st
    except Exception:
        pass
    return {"consecutive_fails": 0, "last_success": None, "alerted": False}


def save_state(path, st):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ===================== 동기화 =====================
def _same(src_st, dst_path):
    """크기와 mtime(정수 초) 이 모두 같으면 True."""
    try:
        d = os.stat(dst_path)
    except OSError:
        return False
    return d.st_size == src_st.st_size and int(d.st_mtime) == int(src_st.st_mtime)


def _copy_one(src, dst, log):
    """tmp 복사 → os.replace → 크기 검증 → copystat. 실패 시 tmp 정리 후 False."""
    tmp = dst + ".tmp"
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copyfile(src, tmp)           # src 는 읽기만
        os.replace(tmp, dst)
        if os.path.getsize(dst) != os.path.getsize(src):
            raise RuntimeError(f"크기 불일치 dst={os.path.getsize(dst)} src={os.path.getsize(src)}")
        try:
            shutil.copystat(src, dst)       # mtime 맞춤 → 다음 실행에서 건너뜀
        except Exception:
            pass
        return True
    except Exception as e:
        log(f"    실패 {src} → {dst}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False


def sync_pair(src_root, dst_root, dry_run, log):
    """한 쌍 동기화. 반환: dict(checked, copied, skipped, failed, pair_failed)."""
    r = {"checked": 0, "copied": 0, "skipped": 0, "failed": 0, "pair_failed": False}
    if not os.path.isdir(src_root):
        log(f"  ! src 없음: {src_root}")
        r["pair_failed"] = True
        return r
    if not dry_run:
        try:
            os.makedirs(dst_root, exist_ok=True)
            if not os.path.isdir(dst_root):
                raise RuntimeError("폴더 아님")
        except Exception as e:
            log(f"  ! dst 접근 불가: {dst_root}: {e} → 이 쌍 건너뜀")
            r["pair_failed"] = True
            return r

    for dirpath, dirnames, filenames in os.walk(src_root):
        rel_dir = os.path.relpath(dirpath, src_root)
        for fn in filenames:
            src = os.path.join(dirpath, fn)
            dst = os.path.join(dst_root, rel_dir, fn) if rel_dir != "." else os.path.join(dst_root, fn)
            r["checked"] += 1
            try:
                st = os.stat(src)
            except OSError as e:
                log(f"    stat 실패 {src}: {e}")
                r["failed"] += 1
                continue
            if _same(st, dst):
                r["skipped"] += 1
                continue
            if dry_run:
                log(f"    [dry-run] 복사 대상: {os.path.relpath(src, src_root)}")
                r["copied"] += 1
                continue
            if _copy_one(src, dst, log):
                r["copied"] += 1
            else:
                r["failed"] += 1
    return r


# ===================== main =====================
def main():
    early = []
    def _pre_log(*x):
        line = " ".join(str(v) for v in x)
        print(line, flush=True); early.append(line)

    cfg = _load_config(_pre_log)
    _load_notify_config()

    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", default=os.environ.get("LOG_SYNC_STATE_DIR", cfg["state_dir"]))
    ap.add_argument("--dry-run", action="store_true", help="복사하지 않고 대상만 집계")
    ap.add_argument("--once", action="store_true", help="한 번 실행(기본 동작, 명시용)")
    a = ap.parse_args()

    t0 = time.time()
    os.makedirs(a.state_dir, exist_ok=True)
    run_log = open(os.path.join(a.state_dir, "run.log"), "a", encoding="utf-8")
    for line in early:
        run_log.write(f"{_now()} {line}\n")

    def log(*x):
        line = " ".join(str(v) for v in x)
        print(line, flush=True)
        try:
            run_log.write(f"{_now()} {line}\n"); run_log.flush()
        except Exception:
            pass

    log(f"[log-sync] settings={cfg['settings'] or '-'} state_dir={a.state_dir} "
        f"enabled={cfg['enabled']} pairs={len(cfg['pairs'])} alert_after={cfg['alert_after_fails']} "
        f"notify={'on' if (_NOTIFY_ENABLED and _NOTIFY_URL) else 'off'} dry_run={a.dry_run}")

    if not cfg["enabled"]:
        log("[log-sync] LOG_SYNC_ENABLED=false → 종료")
        run_log.close()
        return

    tot = {"checked": 0, "copied": 0, "skipped": 0, "failed": 0}
    any_fail = False
    for src, dst in cfg["pairs"]:
        log(f"  {src} → {dst}")
        r = sync_pair(src, dst, a.dry_run, log)
        for k in tot:
            tot[k] += r[k]
        if r["pair_failed"] or r["failed"] > 0:
            any_fail = True
        log(f"    검사={r['checked']} 복사={r['copied']} 건너뜀={r['skipped']} 실패={r['failed']}"
            + (" (쌍 전체 실패)" if r["pair_failed"] else ""))

    # ── 상태 / 알림 (dry-run 은 상태를 건드리지 않음) ──
    st_path = os.path.join(a.state_dir, "state.json")
    if not a.dry_run:
        st = load_state(st_path)
        thr = cfg["alert_after_fails"]
        if any_fail:
            st["consecutive_fails"] = int(st.get("consecutive_fails", 0)) + 1
            if st["consecutive_fails"] >= thr and not st.get("alerted", False):
                msg = f"로그 NAS 동기화 연속 {st['consecutive_fails']}회 실패 (임계 {thr})"
                log(f"  ! {msg}")
                notify(msg)
                st["alerted"] = True          # 임계 도달 시점에 1회만
        else:
            st["consecutive_fails"] = 0
            st["alerted"] = False
            st["last_success"] = _now()
        st["last_run"] = _now()
        try:
            save_state(st_path, st)
        except Exception as e:
            log(f"  ! state 저장 실패: {e}")

    log(f"[log-sync] 완료 쌍={len(cfg['pairs'])} 검사={tot['checked']} 복사={tot['copied']} "
        f"건너뜀={tot['skipped']} 실패={tot['failed']} 소요={time.time()-t0:.1f}s")
    run_log.close()


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        print(tb, flush=True)
        try:
            try:
                st = os.environ.get("LOG_SYNC_STATE_DIR") or _load_config(lambda *x: None)["state_dir"]
                os.makedirs(st, exist_ok=True)
                lp = os.path.join(st, "run.log")
            except Exception:
                lp = str(_worker_base_dir() / "run_error.log")
            with open(lp, "a", encoding="utf-8") as f:
                f.write(f"{_now()} [log-sync] ! 예상 못 한 예외\n{tb}\n")
        except Exception:
            pass
        _load_notify_config()
        notify(f"워커 비정상 종료: {tb.strip().splitlines()[-1]}")
        sys.exit(1)
