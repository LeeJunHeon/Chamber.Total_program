r"""OES daemon(상주) 모드(워커 안) — v1 apps/oes_service/oes_api.py:1913-2376 을 옮긴 것.

1회 init(실패면 kind daemon ok=False 후 os._exit(2)) → status 'daemon READY ..' → stdin JSONL 명령 루프:
ping/health → pong, close/exit/quit → 정리 종료, reset → cleanup+init(kind reset), measure → 1회 측정
(실패·예외 뒤 자동 reset), 모르는 cmd 는 status, 빈 줄 무시, 잘못된 JSON 은 status 'invalid json'. EOF 면 정리 종료.
장비 호출·CSV·stop 플래그·finished 필드는 v1 그대로.
v2 연결부: 명령 줄의 "id" 를 그 명령이 만든 모든 출력 줄에 싣는다(oes_common.set_command_id), 측정 중 여부를
하트비트(oes_worker 의 kind hb)에 알린다(STATE). v1 메인이 보내던 줄({"cmd": ...})도 그대로 받는다(id 없음 → null).
"""
from __future__ import annotations

import asyncio
import contextlib
import csv
import json
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

from v2.ch12.workers import oes_common as C
from v2.ch12.workers.oes_common import (
    OES_AVG_COUNT, _WinMutex, _copy_csv_to_nas, _default_out_dir, _errlog, _errlog_exc, _find_ar_index,
    _make_default_filename, _mutex_timeout_ms, _post_anomaly_webhook, _report_anomaly, _print_json, _status, _stop_dir, _env_float,
)
from v2.ch12.workers.oes_device import OESAsync
from v2.ch12.workers.oes_guard import _LinkGuard, _ResetCallTimeout, _acquire_first_frame, _daemon_reset_device

STATE = {"state": "init"}      # v2: 하트비트에 싣는 워커 상태(init / idle / measuring)


async def _stdin_readline_async() -> Optional[str]:
    loop = asyncio.get_running_loop()
    # Windows에서도 가장 안정적인 방식: blocking readline을 executor로 보냄
    return await loop.run_in_executor(None, sys.stdin.readline)

async def _daemon_read_cmd() -> Optional[dict]:
    """stdin에서 JSON 한 줄을 읽어 dict로 반환. EOF면 None."""
    try:
        line = await _stdin_readline_async()
    except Exception as e:
        _errlog_exc(f"[daemon] stdin readline failed: {type(e).__name__}: {e}")
        return None

    if not line:
        return None  # EOF

    s = (line or "").strip()
    if not s:
        return {}

    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {"cmd": "_invalid", "raw": s[:500]}
    except Exception as e:
        _print_json({"kind": "status", "message": f"[daemon] invalid json: {type(e).__name__}: {e} raw={s[:200]}"})
        return {}


async def _daemon_measure_once(
    *,
    oes: OESAsync,
    ch: int,
    usb: int,
    duration_s: float,
    integration_ms: int,
    sample_interval_s: float,
    avg_count: int,
    out_dir: Optional[Path],
    out_csv: Optional[Path],
) -> int:
    """daemon에서 1회 측정. (OES 채널은 열어둔 채 CSV만 생성/복사)"""
    t0 = time.time()
    rows = 0
    f = None

    # ✅ (1) out_dir/out_csv 확정
    if out_csv:
        out_csv_final = Path(out_csv).expanduser().resolve()
        out_dir_final = out_csv_final.parent
    else:
        out_dir_final = Path(out_dir).expanduser().resolve() if out_dir else _default_out_dir(int(ch))
        out_csv_final = out_dir_final / _make_default_filename()

    out_dir_final.mkdir(parents=True, exist_ok=True)

    # ✅ (2) stop flag 경로 확정
    stop_flag_usb_local = out_dir_final / f".stop_usb{int(usb)}.flag"
    stop_flag_csv = Path(str(out_csv_final) + ".stop")
    stop_flag_usb_global = _stop_dir() / f".stop_usb{int(usb)}.flag"

    # ✅ (3) 이전 잔재 제거(스테일 stop 방지)
    # - global stop은 '실시간 STOP 신호'로도 쓰이므로 여기서 지우지 않는다.
    for p in (stop_flag_usb_local, stop_flag_csv):
        with contextlib.suppress(Exception):
            p.unlink()

    stopped = False
    stop_reason = None

    # ✅ (4) 장비 설정 적용(측정마다 integration/avg를 반영)
    try:
        oes._avg_count = int(max(1, int(avg_count)))  # type: ignore[attr-defined]
    except Exception:
        pass

    # ✅ 분광기 연결 감시/읽기 실패 복구: 시작 전 목록 확인(다르면 복귀 대기 → 재초기화, 안 되면 예외)
    #    목록 확인을 먼저 해서, 기준과 다를 때 설정 명령이 다른 챔버 분광기로 나가지 않게 한다
    guard = _LinkGuard(oes, ch=int(ch), usb=int(usb), integration_ms=int(integration_ms))
    await guard.ensure_at_start()

    with contextlib.suppress(Exception):
        await oes._call(oes._apply_device_settings_blocking, int(oes.sChannel), int(integration_ms))

    # ✅ (5) CSV open + header + 첫 프레임
    _print_json({"kind": "status", "message": f"[daemon] open csv: {out_csv_final}"})
    f = open(str(out_csv_final), "w", newline="", encoding="utf-8")
    w = csv.writer(f)

    x, y = await _acquire_first_frame(oes, guard=guard)
    x_list = x.tolist() if hasattr(x, "tolist") else list(x)
    y_list = y.tolist() if hasattr(y, "tolist") else list(y)

    w.writerow(["Time"] + [float(v) for v in x_list])
    f.flush()

    _print_json({
        "kind": "started",
        "ok": True,
        "ch": int(ch),
        "usb": int(usb),
        "resolved_usb": int(getattr(oes, "sChannel", -1)),
        "out_csv": str(out_csv_final),
        "cols": int(len(x_list)),
        "model": str(getattr(oes, "_model_name", "UNKNOWN")),
        "sample_interval_s": float(sample_interval_s),
        "avg_count": int(avg_count),
        "integration_ms": int(integration_ms),
    })

    now_s = datetime.now().strftime("%H:%M:%S")
    w.writerow([now_s] + [float(v) for v in y_list])
    rows += 1
    f.flush()

    # ── OES Anomaly: Ar 감시 초기화 (처음/마지막 N행 제외) ──
    _ar_idx = _find_ar_index(x_list) if C._ANOMALY_ENABLED else None
    _ar_sum = 0.0
    _ar_count = 0
    _ar_row_num = 0

    hard_abort = False
    hard_abort_error = None
    hard_abort_exit_code = 0

    deadline = time.time() + max(0.0, float(duration_s))
    _ar_cutoff = deadline - (C._ANOMALY_SKIP_LAST * float(sample_interval_s))
    while time.time() < deadline:
        # ✅ stop 요청 감지(USB 기반 / CSV 기반)
        if stop_flag_usb_global.exists() or stop_flag_usb_local.exists() or stop_flag_csv.exists():
            stopped = True
            stop_reason = "stop_flag"
            _status(
                f"[daemon] stop requested (usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb_local.exists()} csv_flag={stop_flag_csv.exists()})"
            )
            break

        await asyncio.sleep(float(sample_interval_s))

        # sleep 직후 한번 더(반응성)
        if stop_flag_usb_global.exists() or stop_flag_usb_local.exists() or stop_flag_csv.exists():
            stopped = True
            stop_reason = "stop_flag"
            _status(
                f"[daemon] stop requested (usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb_local.exists()} csv_flag={stop_flag_csv.exists()})"
            )
            break

        # ✅ 연결 목록이 기준과 다르면(또는 끊김 중이면) DLL 호출 없이 이번 샘플 건너뜀
        if not await guard.before_read():
            continue

        read_timeout_s = _env_float("OES_READ_TIMEOUT_S", 2.0)
        try:
            x2, y2 = await asyncio.wait_for(oes._call(oes._acquire_one_slice_avg), timeout=read_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES read timeout after {read_timeout_s}s (daemon ch={ch} usb={usb})"
            _errlog(msg)
            hard_abort = True
            hard_abort_error = msg
            hard_abort_exit_code = 125
            stopped = True
            stop_reason = "read_timeout"
            break

        # ✅ 읽은 직후 목록 재확인(바뀌었으면 버림) + 연속 실패 복구(리셋/재초기화)
        try:
            keep = await guard.after_read(x2 is not None and y2 is not None)
        except _ResetCallTimeout as e:
            msg = str(e)
            _errlog(msg)
            hard_abort = True
            hard_abort_error = msg
            hard_abort_exit_code = 125
            stopped = True
            stop_reason = "reset_timeout"
            break
        if not keep:
            continue

        if x2 is None or y2 is None:
            continue

        y2_list = y2.tolist() if hasattr(y2, "tolist") else list(y2)
        if len(y2_list) != len(x_list):
            continue

        now_s = datetime.now().strftime("%H:%M:%S")
        w.writerow([now_s] + [float(v) for v in y2_list])
        rows += 1
        f.flush()

        # ── OES Anomaly: Ar 파장 감시 (처음/마지막 N행 제외) ──
        if _ar_idx is not None:
            try:
                ar_val = float(y2_list[_ar_idx])
                _ar_row_num += 1
                if _ar_row_num <= C._ANOMALY_SKIP_FIRST:
                    pass  # 처음 N행 스킵
                elif time.time() > _ar_cutoff:
                    pass  # 마지막 N초 구간 스킵
                else:
                    if _ar_count == 0:
                        _ar_sum = ar_val
                        _ar_count = 1
                    else:
                        ar_mean = _ar_sum / _ar_count
                        if ar_mean != 0 and abs(ar_val - ar_mean) / abs(ar_mean) >= C._ANOMALY_THRESHOLD:
                            pct = ((ar_val - ar_mean) / ar_mean) * 100
                            direction = "급등" if pct > 0 else "급락"
                            alert_msg = (
                                f"⚠️ OES 이상 감지 (CH{ch})\n"
                                f"Ar {C._AR_WAVELENGTH_NM:.1f}nm {direction}: "
                                f"{ar_val:.1f} (평균 {ar_mean:.1f}, {pct:+.1f}%)\n"
                                f"측정 행: {rows}"
                            )
                            _status(f"[daemon] ANOMALY: {alert_msg}")
                            _report_anomaly(alert_msg)              # D28: 웹훅 대신 메인으로(포털이 카드)
                        else:
                            _ar_sum += ar_val
                            _ar_count += 1
            except Exception:
                pass

    elapsed = time.time() - t0

    # ✅ 파일 flush/close
    with contextlib.suppress(Exception):
        if f:
            f.flush()
            os.fsync(f.fileno())
            f.close()
            f = None

    # ✅ NAS 복사 (daemon은 USB mutex를 계속 잡고 있으므로, 여기서 mutex release는 하지 않음)
    nas_ok, nas_csv, nas_error, local_deleted = await _copy_csv_to_nas(out_csv_final, int(ch))

    ok_final = (not hard_abort)

    payload = {
        "kind": "finished",
        "ok": bool(ok_final),
        "stopped": bool(stopped),
        "stop_reason": stop_reason,
        "ch": int(ch),
        "usb": int(usb),
        "out_csv": str(out_csv_final),
        "nas_ok": bool(nas_ok),
        "nas_csv": str(nas_csv) if nas_csv else None,
        "nas_error": nas_error,
        "local_deleted": bool(local_deleted),
        "rows": int(rows),
        "elapsed_s": float(elapsed),
    }
    payload.update(guard.payload())
    if not ok_final and hard_abort_error:
        payload["error"] = hard_abort_error

    _print_json(payload)

    # ✅ hard_abort면 DLL hang 의심이므로 daemon 자체를 자가 종료시키는 게 안전 (메인이 재기동/폴백)
    if hard_abort and hard_abort_exit_code:
        for p in (stop_flag_usb_local, stop_flag_csv, stop_flag_usb_global):
            with contextlib.suppress(Exception):
                p.unlink()
        os._exit(int(hard_abort_exit_code))

    # stop flag 정리
    for p in (stop_flag_usb_local, stop_flag_csv, stop_flag_usb_global):
        with contextlib.suppress(Exception):
            p.unlink()

    return 0 if ok_final else 10

async def cmd_daemon(ch: int, usb: int, dll_path: Optional[str], out_dir: Optional[Path]) -> int:
    """OES를 1회 initialize 후 살아있는 상태로 유지하는 daemon."""
    mtx = _WinMutex(f"Local\\VanaM_OES_USB{int(usb)}")
    mutex_ms = _mutex_timeout_ms()

    _status(f"[worker] daemon: acquiring mutex name={mtx.name} timeout_ms={mutex_ms}")
    acquired = mtx.acquire(timeout_ms=mutex_ms)
    if not acquired:
        _errlog(f"cmd=daemon mutex timeout ch={ch} usb={usb} timeout_ms={mutex_ms}")
        _print_json({"kind": "daemon", "ok": False, "ch": int(ch), "usb": int(usb), "error": f"mutex timeout ({mutex_ms}ms)"})
        return 4

    oes = None
    try:
        # ✅ daemon 기본 출력 디렉토리(측정 명령에서 out_dir/out_csv 없을 때 사용)
        base_dir = Path(out_dir).expanduser().resolve() if out_dir else _default_out_dir(int(ch))
        base_dir.mkdir(parents=True, exist_ok=True)

        oes = OESAsync(
            chamber=int(ch),
            usb_index=int(usb),
            dll_path=dll_path,
            save_directory=str(base_dir),
            sample_interval_s=1.0,
            avg_count=int(OES_AVG_COUNT),
            debug_print=False,
        )

        init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)
        _status(f"[worker] daemon init begin ch={ch} usb={usb} dir={base_dir} dll_path={dll_path}")
        try:
            ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES init timeout after {init_timeout_s}s (cmd=daemon ch={ch} usb={usb})"
            _errlog(msg)
            _print_json({"kind": "daemon", "ok": False, "ch": int(ch), "usb": int(usb), "error": msg})
            os._exit(124)

        payload = {
            "kind": "daemon",
            "ok": bool(ok),
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "pixels": int(getattr(oes, "_npix", 0) or 0),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),
            "dll_resolved": str(getattr(oes, "_dll_path", "")),
            "dll_exists": bool(Path(getattr(oes, "_dll_path", "")).is_file()),
            "out_dir": str(base_dir),
            "serial": str(getattr(oes, "_serial", "") or ""),
        }
        if not ok:
            payload["error"] = str(getattr(oes, "_last_error", "")) or "initialize_device failed"
            payload["scan"] = getattr(oes, "_last_scan", {}) or {}

        _print_json(payload)

        if not ok or getattr(oes, "sChannel", -1) < 0:
            # ✅ init 실패 시 executor 스레드가 DLL 호출에 갇혀 있을 수 있음.
            #    정상 return이면 인터프리터 종료가 비데몬 스레드 join에서 무한 대기
            #    → 워커 좀비화 → 디바이스 핸들 점유 → 이후 모든 시도가 같은 지점에서 hang.
            #    measure / total-timeout 경로(os._exit)와 동일하게 하드 종료로 대칭화.
            with contextlib.suppress(Exception):
                mtx.release()
            os._exit(2)

        _status(f"[worker] daemon READY ch={ch} usb={usb} resolved={getattr(oes,'sChannel',-1)} pixels={getattr(oes,'_npix',0)} serial={getattr(oes,'_serial','') or '?'}")

        # ✅ daemon READY 직후: 이전 실행에서 남은 global stop 잔재만 1회 정리
        with contextlib.suppress(Exception):
            (_stop_dir() / f".stop_usb{int(usb)}.flag").unlink()

        # ✅ 측정 동시 실행 방지
        measure_lock = asyncio.Lock()

        STATE["state"] = "idle"
        while True:
            C.set_command_id(None)
            cmd_obj = await _daemon_read_cmd()
            if cmd_obj is None:
                _status("[daemon] stdin closed (EOF) → exit")
                break

            C.set_command_id(cmd_obj.get("id"))      # v2: 이 명령이 만드는 출력 줄마다 id
            cmd = str(cmd_obj.get("cmd") or "").strip().lower()
            if not cmd:
                continue

            if cmd in {"ping", "health"}:
                _print_json({"kind": "pong", "ok": True, "ch": int(ch), "usb": int(usb), "ts": time.time()})
                continue

            if cmd in {"close", "exit", "quit"}:
                _status("[daemon] close requested → cleanup & exit")
                break

            if cmd == "reset":
                _status("[daemon] reset requested")
                ok_reset = await _daemon_reset_device(oes, ch=int(ch), usb=int(usb))
                _print_json({"kind": "reset", "ok": bool(ok_reset), "ch": int(ch), "usb": int(usb)})
                continue

            if cmd == "measure":
                if measure_lock.locked():
                    _print_json({"kind": "finished", "ok": False, "ch": int(ch), "usb": int(usb), "error": "busy"})
                    continue

                # cmd 파라미터
                dur = float(cmd_obj.get("duration_s") or cmd_obj.get("duration") or 0.0)
                integ = int(cmd_obj.get("integration_ms") or 0)
                if integ <= 0:
                    integ = int(os.environ.get("OES_DEFAULT_INTEGRATION_MS", "50"))  # 기본 50ms
                si = float(cmd_obj.get("sample_interval_s") or 1.0)
                ac = int(cmd_obj.get("avg_count") or OES_AVG_COUNT)

                od = cmd_obj.get("out_dir")
                oc = cmd_obj.get("out_csv")

                od_p = Path(str(od)) if od else base_dir
                oc_p = Path(str(oc)) if oc else None

                async with measure_lock:
                    STATE["state"] = "measuring"
                    try:
                        rc = await _daemon_measure_once(
                            oes=oes,
                            ch=int(ch),
                            usb=int(usb),
                            duration_s=dur,
                            integration_ms=integ,
                            sample_interval_s=si,
                            avg_count=ac,
                            out_dir=od_p,
                            out_csv=oc_p,
                        )
                        # 측정 실패(비-fatal)면 다음 측정을 위해 자동 reset 시도
                        if rc != 0:
                            with contextlib.suppress(Exception):
                                ok_reset = await _daemon_reset_device(oes, ch=int(ch), usb=int(usb))
                                _status(f"[daemon] auto-reset after measure rc={rc} ok={ok_reset}")
                    except Exception as e:
                        _errlog_exc(f"[daemon] measure exception: {type(e).__name__}: {e}")
                        _print_json({"kind": "finished", "ok": False, "ch": int(ch), "usb": int(usb), "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()})
                        with contextlib.suppress(Exception):
                            ok_reset = await _daemon_reset_device(oes, ch=int(ch), usb=int(usb))
                            _status(f"[daemon] auto-reset after exception ok={ok_reset}")
                    finally:
                        STATE["state"] = "idle"
                continue

            _print_json({"kind": "status", "message": f"[daemon] unknown cmd={cmd} obj={cmd_obj}"})

        return 0

    except Exception as e:
        _errlog_exc(f"cmd=daemon fatal exception ch={ch} usb={usb}")
        _print_json({"kind": "fatal", "ok": False, "ch": int(ch), "usb": int(usb), "error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()})
        return 99

    finally:
        # ✅ daemon 종료 시에만 cleanup
        if oes is not None:
            cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)
            try:
                await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
            except asyncio.TimeoutError:
                _errlog(f"[daemon] cleanup timeout after {cleanup_timeout_s}s (exit ch={ch} usb={usb})")
            except Exception:
                pass

        with contextlib.suppress(Exception):
            mtx.release()    

