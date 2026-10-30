r"""OES 단발 명령(워커 안) — cmd_init·cmd_measure. v1 apps/oes_service/oes_api.py:1359-1911 을 옮긴 것.

동작은 v1 그대로: USB 뮤텍스 Local\VanaM_OES_USB{U}(OES_MUTEX_TIMEOUT_MS) → init(OES_INIT_TIMEOUT_S) → 연결 감시 →
측정 전 장비 설정 → CSV(머리글 'Time'+파장, 행 'HH:MM:SS'+세기, 매 행 flush, 끝 fsync) → stop 플래그 3개 확인 →
finished JSON → hard_abort(읽기·리셋·정리 시간 초과)면 os._exit(124|125|126). 종료 코드 init 0/2/3/4, measure 0/10/4.
v2 차이: 출력 줄에 v2 봉투(v·id — oes_common._print_json), Ar 이상 감지 설정은 oes_common 모듈 값(C.*)을 읽는다.
"""
from __future__ import annotations

import asyncio
import contextlib
import csv
import os
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional

from v2.ch12.workers import oes_common as C
from v2.ch12.workers.oes_common import (
    _WinMutex, _copy_csv_to_nas, _default_out_dir, _env_float, _errlog, _errlog_exc, _find_ar_index,
    _make_default_filename, _mutex_timeout_ms, _post_anomaly_webhook, _report_anomaly, _print_json, _status, _stop_dir,
)
from v2.ch12.workers.oes_device import OESAsync, _resolve_oes_dll_path
from v2.ch12.workers.oes_guard import _LinkGuard, _ResetCallTimeout, _acquire_first_frame


async def cmd_init(ch: int, usb: int, dll_path: Optional[str], out_dir: Optional[Path], out_csv: Optional[Path]) -> int:
    mtx = _WinMutex(f"Local\\VanaM_OES_USB{int(usb)}")
    mutex_ms = _mutex_timeout_ms()

    _status(f"[worker] init: acquiring mutex name={mtx.name} timeout_ms={mutex_ms}")
    acquired = mtx.acquire(timeout_ms=mutex_ms)

    try:
        if not acquired:
            _errlog(f"cmd=init mutex timeout ch={ch} usb={usb}")
            _print_json({"kind":"init","ok":False,"ch":int(ch),"usb":int(usb),"error":"mutex timeout"})
            return 4
        
        _status(f"[worker] init: mutex acquired name={mtx.name}")

        # ✅ out_dir/out_csv 반영 + dll 실제 resolve 정보까지 남김
        if out_csv:
            temp_dir = Path(out_csv).expanduser().resolve().parent
        elif out_dir:
            temp_dir = Path(out_dir).expanduser().resolve()
        else:
            temp_dir = _default_out_dir(ch)

        dll_resolved = _resolve_oes_dll_path(dll_path)
        dll_exists = Path(dll_resolved).is_file()

        _status(f"[worker] init begin ch={ch} usb={usb} dir={temp_dir} dll_arg={dll_path} dll_resolved={dll_resolved} dll_exists={dll_exists}")

        oes = OESAsync(chamber=int(ch), usb_index=int(usb), dll_path=dll_path, save_directory=str(temp_dir))
        init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)
        try:
            ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES init timeout after {init_timeout_s}s (ch={ch}, usb={usb})"
            _errlog(msg)
            _print_json({"kind":"init","ok":False,"ch":int(ch),"usb":int(usb),"error":msg})
            if acquired:
                with contextlib.suppress(Exception):
                    mtx.release()
            os._exit(124)

        payload = {
            "kind": "init",
            "ok": bool(ok),
            "ch": int(ch),
            "usb": int(usb),
            "resolved_usb": int(getattr(oes, "sChannel", -1)),
            "pixels": int(getattr(oes, "_npix", 0) or 0),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),   # ✅ 추가
            "dll_resolved": str(getattr(oes, "_dll_path", "")),
            "dll_exists": bool(Path(getattr(oes, "_dll_path", "")).is_file()),
            "serial": str(getattr(oes, "_serial", "") or ""),
        }

        if not ok:
            payload["error"] = str(getattr(oes, "_last_error", "")) or "initialize_device failed"
            payload["scan"] = getattr(oes, "_last_scan", {}) or {}

        # ✅ 1) 결과를 먼저 부모에게 알림(디버깅/상태판단에 유리)
        _print_json(payload)

        # ✅ 2) 그 다음 정리(정리가 hang이면 워커가 알아서 종료)
        cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)
        try:
            await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES cleanup timeout after {cleanup_timeout_s}s (cmd=init ch={ch} usb={usb})"
            _errlog(msg)
            with contextlib.suppress(Exception):
                mtx.release()
            os._exit(126)
        except Exception:
            pass

        return 0 if ok else 2

    except Exception as e:
        _errlog_exc(f"cmd=init exception ch={ch} usb={usb} dll_path={dll_path}")
        _print_json({
            "kind": "init",
            "ok": False,
            "ch": int(ch),
            "usb": int(usb),
            "error": f"{type(e).__name__}: {e}",
            "trace": traceback.format_exc(),
        })
        return 3

    finally:
        # ✅ acquired 성공했을 때만 release (혹은 release가 내부적으로 안전해도 가드는 두는 게 깔끔)
        if acquired:
            with contextlib.suppress(Exception):
                mtx.release()


async def cmd_measure(
    ch: int,
    usb: int,
    duration_s: float,
    integration_ms: int,
    sample_interval_s: float,
    avg_count: int,
    out_dir: Optional[Path],
    out_csv: Optional[Path],
    dll_path: Optional[str],
) -> int:
    mtx = _WinMutex(f"Local\\VanaM_OES_USB{int(usb)}")
    mutex_ms = _mutex_timeout_ms()

    _status(f"[worker] measure: acquiring mutex name={mtx.name} timeout_ms={mutex_ms}")
    acquired = mtx.acquire(timeout_ms=mutex_ms)

    t0 = time.time()
    rows = 0
    f = None
    oes = None

    # ✅ out_dir_final/out_csv는 아직 확정 전 → 먼저 None/기본값으로 선언
    out_dir_final: Optional[Path] = None
    out_csv_final: Optional[Path] = None

    # ✅ stop flag도 아직 확정 전
    stop_flag_usb: Optional[Path] = None
    stop_flag_csv: Optional[Path] = None
    stop_flag_usb_global: Optional[Path] = None  # ✅ 추가: finally에서 안전하게 쓰기 위해

    stopped = False
    stop_reason = None

    # ✅ DLL hang 의심 상황에서 워커가 최종적으로 '자가 종료(os._exit)'하도록 하는 플래그들
    hard_abort = False
    hard_abort_error: Optional[str] = None
    hard_abort_exit_code: int = 0

    # ✅ mutex를 "한 번만" 해제하기 위한 가드 (acquired=False면 해제하지 않음)
    mutex_released = False
    def _release_mutex_once() -> None:
        nonlocal mutex_released
        if mutex_released:
            return
        mutex_released = True
        if not acquired:
            return
        with contextlib.suppress(Exception):
            mtx.release()

    try:
        if not acquired:
            _errlog(f"cmd=measure mutex timeout ch={ch} usb={usb} timeout_ms={mutex_ms}")
            _print_json({"kind":"finished","ok":False,"ch":int(ch),"usb":int(usb),"error":f"mutex timeout ({mutex_ms}ms)"})
            return 4

        # ✅ (1) out_dir_final/out_csv 확정: out_csv 우선, 그다음 out_dir, 없으면 default
        if out_csv:
            out_csv_final = Path(out_csv).expanduser().resolve()
            out_dir_final = out_csv_final.parent
        else:
            out_dir_final = Path(out_dir).expanduser().resolve() if out_dir else _default_out_dir(int(ch))
            out_csv_final = out_dir_final / _make_default_filename()

        # ✅ 확정된 값을 이후 코드가 쓰도록 덮어쓰기(아래 코드 수정 최소화)
        out_csv = out_csv_final

        # ✅ (2) 이제 out_dir_final을 만들 수 있음
        out_dir_final.mkdir(parents=True, exist_ok=True)

        # ✅ (3) stop flag 경로 확정
        #  - local(기존 호환)
        stop_flag_usb = out_dir_final / f".stop_usb{int(usb)}.flag"
        stop_flag_csv = Path(str(out_csv) + ".stop")

        #  - global(신규): 메인이 out_dir 몰라도 stop 보낼 수 있도록 고정 경로 추가
        stop_dir = _stop_dir()
        stop_flag_usb_global = stop_dir / f".stop_usb{int(usb)}.flag"

        # ✅ 워커가 뜨기 전/초기화 직전에 들어온 stop도 살리기 (메인은 기다리지 않으니까 중요)
        if stop_flag_usb_global.exists():
            stopped = True
            stop_reason = "pre_stop"
            _status("[worker] stop already requested before init (global flag exists)")

            _print_json({
                "kind": "finished",
                "ok": True,
                "stopped": True,
                "stop_reason": stop_reason,
                "ch": int(ch),
                "usb": int(usb),
                "out_csv": str(out_csv) if out_csv else None,
                "rows": int(rows),
                "elapsed_s": float(time.time() - t0),
                "nas_ok": False,
                "nas_csv": None,
                "nas_error": "pre_stop before init (no data)",
                "local_deleted": False,
            })
            return 0

        # 이전 실행 잔재 제거(스테일 stop 방지)
        # ✅ local stop만 제거 (global stop은 메인이 먼저 보낸 stop이 씹히지 않도록 유지)
        for p in (stop_flag_usb, stop_flag_csv):
            with contextlib.suppress(Exception):
                p.unlink()

        oes = OESAsync(
            chamber=int(ch),
            usb_index=int(usb),
            dll_path=dll_path,
            save_directory=str(out_dir_final),
            sample_interval_s=float(sample_interval_s),
            avg_count=int(avg_count),
            debug_print=False,
        )

        _print_json({"kind": "status", "message": f"[worker] init start ch={ch} usb={usb} dll_path={dll_path}"})

        init_timeout_s = _env_float("OES_INIT_TIMEOUT_S", 25.0)
        try:
            ok = await asyncio.wait_for(oes.initialize_device(), timeout=init_timeout_s)
        except asyncio.TimeoutError:
            msg = f"OES initialize_device timeout after {init_timeout_s}s (ch={ch}, usb={usb})"
            _errlog(msg)

            # ✅ 메인은 절대 기다리지 않게: 워커가 스스로 '확실히' 종료해야 함
            _print_json({"kind": "finished", "ok": False, "ch": int(ch), "usb": int(usb), "out_csv": str(out_csv), "error": msg})

            # mutex/stop파일 정리 후 하드 종료(스레드 hang으로 sys.exit가 먹지 않을 수 있음)
            with contextlib.suppress(Exception):
                stop_flag_usb.unlink()
            with contextlib.suppress(Exception):
                stop_flag_csv.unlink()
            with contextlib.suppress(Exception):
                stop_flag_usb_global.unlink()
            _release_mutex_once()
            os._exit(124)

        _print_json({"kind": "status", "message": f"[worker] init done ok={ok} resolved_usb={getattr(oes,'sChannel',-1)} pixels={getattr(oes,'_npix',0)}"})

        if not ok or getattr(oes, "sChannel", -1) < 0:
            init_err = str(getattr(oes, "_last_error", "")) or "OES initialize_device() failed"
            raise RuntimeError(init_err)

        # ✅ 분광기 연결 감시/읽기 실패 복구(daemon 과 같은 규칙)
        #    목록 확인을 먼저 해서, 기준과 다를 때 설정 명령이 다른 챔버 분광기로 나가지 않게 한다
        guard = _LinkGuard(oes, ch=int(ch), usb=int(usb), integration_ms=int(integration_ms))
        await guard.ensure_at_start()

        with contextlib.suppress(Exception):
            await oes._call(oes._apply_device_settings_blocking, int(oes.sChannel), int(integration_ms))

        _print_json({"kind": "status", "message": f"[worker] open csv: {out_csv}"})
        f = open(str(out_csv), "w", newline="", encoding="utf-8")
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
            "out_csv": str(out_csv),
            "cols": int(len(x_list)),
            "model": str(getattr(oes, "_model_name", "UNKNOWN")),   # ✅ 추가
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
        _ar_cutoff = time.time() + max(0.0, float(duration_s)) - (C._ANOMALY_SKIP_LAST * float(sample_interval_s))

        deadline = time.time() + max(0.0, float(duration_s))
        while time.time() < deadline:
            # ✅ stop 요청 감지(USB 기반 / CSV 기반)
            if stop_flag_usb_global.exists() or stop_flag_usb.exists() or (stop_flag_csv and stop_flag_csv.exists()):
                stopped = True
                stop_reason = "stop_flag"
                _status(
                    f"[worker] stop requested "
                    f"(usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb.exists()} "
                    f"csv_flag={(stop_flag_csv.exists() if stop_flag_csv else None)})"
                )
                break

            await asyncio.sleep(float(sample_interval_s))

            # sleep 직후 한번 더(반응성)
            if stop_flag_usb_global.exists() or stop_flag_usb.exists() or (stop_flag_csv and stop_flag_csv.exists()):
                stopped = True
                stop_reason = "stop_flag"
                _status(
                    f"[worker] stop requested "
                    f"(usb_global={stop_flag_usb_global.exists()} usb_local={stop_flag_usb.exists()} "
                    f"csv_flag={(stop_flag_csv.exists() if stop_flag_csv else None)})"
                )
                break

            # ✅ 연결 목록이 기준과 다르면(또는 끊김 중이면) DLL 호출 없이 이번 샘플 건너뜀
            if not await guard.before_read():
                continue

            read_timeout_s = _env_float("OES_READ_TIMEOUT_S", 2.0)
            try:
                x2, y2 = await asyncio.wait_for(oes._call(oes._acquire_one_slice_avg), timeout=read_timeout_s)
            except asyncio.TimeoutError:
                msg = f"OES read timeout after {read_timeout_s}s (ch={ch}, usb={usb})"
                _errlog(msg)

                # ✅ DLL hang 의심: 이후 DLL cleanup await는 더 위험 → cleanup 스킵하고 파일/NAS 처리 후 자가 종료 루트로
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
                                _status(f"[worker] ANOMALY: {alert_msg}")
                                _report_anomaly(alert_msg)              # D28: 웹훅 대신 메인으로(포털이 카드)
                            else:
                                _ar_sum += ar_val
                                _ar_count += 1
                except Exception:
                    pass

        elapsed = time.time() - t0

        with contextlib.suppress(Exception):
            if f:
                f.flush()
                os.fsync(f.fileno())
                f.close()
                f = None

        cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)

        if oes:
            # ✅ read_timeout으로 hard_abort가 된 경우엔 cleanup 자체를 시도하지 않는 게 더 안전
            if not hard_abort:
                try:
                    await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
                except asyncio.TimeoutError:
                    msg = f"OES cleanup timeout after {cleanup_timeout_s}s (ch={ch}, usb={usb})"
                    _errlog(msg)
                    hard_abort = True
                    hard_abort_error = hard_abort_error or msg
                    hard_abort_exit_code = hard_abort_exit_code or 126
                    stop_reason = stop_reason or "cleanup_timeout"
                except Exception:
                    pass

            oes = None

        # ✅ 장비/DLL 정리 끝났으면 USB mutex는 즉시 해제 (NAS 복사는 mutex 없이)
        _release_mutex_once()

        nas_ok, nas_csv, nas_error, local_deleted = await _copy_csv_to_nas(out_csv, int(ch))

        ok_final = (not hard_abort)

        payload = {
            "kind": "finished",
            "ok": bool(ok_final),
            "stopped": bool(stopped),
            "stop_reason": stop_reason,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv),
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

        # ✅ hard_abort면 DLL 스레드가 걸렸을 수 있으니, stop 파일/뮤텍스 정리 후 워커 자가 종료로 끝낸다
        if hard_abort and hard_abort_exit_code:
            # stop 파일은 finally를 기대하면 안 됨(os._exit) → 여기서 직접 정리
            for p in (stop_flag_usb, stop_flag_csv, stop_flag_usb_global):
                if p is not None:
                    with contextlib.suppress(Exception):
                        p.unlink()
            _release_mutex_once()
            os._exit(int(hard_abort_exit_code))

        return 0 if ok_final else 10

    except Exception as e:
        elapsed = time.time() - t0

        # ✅ 1) 실패여도 파일/버퍼를 최대한 정리해서 'size mismatch' 확률 줄이기
        with contextlib.suppress(Exception):
            if f:
                f.flush()
                os.fsync(f.fileno())
                f.close()
                f = None

        # ✅ 2) DLL/채널 정리(실패했더라도 다음 런에 영향 최소화)
        cleanup_timeout_s = _env_float("OES_CLEANUP_TIMEOUT_S", 5.0)

        if oes:
            try:
                await asyncio.wait_for(oes.cleanup(), timeout=cleanup_timeout_s)
            except asyncio.TimeoutError:
                # 예외 경로에서 cleanup hang이면 워커가 계속 떠있을 수 있음 → 자가 종료 쪽으로
                msg = f"OES cleanup timeout after {cleanup_timeout_s}s (exception path ch={ch} usb={usb})"
                _errlog(msg)
                # stop 파일/뮤텍스 정리 후 종료(마찬가지로 finally 기대 X)
                for p in (stop_flag_usb, stop_flag_csv, stop_flag_usb_global):
                    if p is not None:
                        with contextlib.suppress(Exception):
                            p.unlink()
                _release_mutex_once()
                os._exit(126)
            except Exception:
                pass
            finally:
                oes = None

        # ✅ 3) 그 다음 mutex 해제(다음 측정 막지 않기)
        _release_mutex_once()

        # ✅ 4) 로컬 CSV가 있으면 NAS 복사 시도
        nas_ok = False
        nas_csv = None
        nas_error = None
        local_deleted = False
        try:
            if out_csv and Path(out_csv).exists():
                nas_ok, nas_csv, nas_error, local_deleted = await _copy_csv_to_nas(Path(out_csv), int(ch))
        except Exception as _e:
            nas_ok = False
            nas_error = f"nas copy exception: {type(_e).__name__}: {_e}"

        _print_json({
            "kind": "finished",
            "ok": False,
            "stopped": bool(stopped),
            "stop_reason": stop_reason,
            "ch": int(ch),
            "usb": int(usb),
            "out_csv": str(out_csv) if out_csv else None,
            "nas_ok": bool(nas_ok),
            "nas_csv": str(nas_csv) if nas_csv else None,
            "nas_error": nas_error,
            "local_deleted": bool(local_deleted),
            "rows": int(rows),
            "elapsed_s": float(elapsed),
            "error": f"{type(e).__name__}: {e}",
            "trace": traceback.format_exc(),
        })
        return 10

    finally:
        if stop_flag_usb is not None:
            with contextlib.suppress(Exception):
                stop_flag_usb.unlink()
        if stop_flag_csv is not None:
            with contextlib.suppress(Exception):
                stop_flag_csv.unlink()
        if stop_flag_usb_global is not None:
            with contextlib.suppress(Exception):
                stop_flag_usb_global.unlink()

        # ✅ 앞에서 이미 해제했을 수도 있으므로 1회 해제로 통일
        _release_mutex_once()


# ====== Daemon mode (persistent OES) ======
# - 워커 프로세스를 1회만 띄워 OES 채널을 열린 상태로 유지
# - stdin(JSONL)로 명령을 받아 측정만 수행 (프로그램 종료 시에만 cleanup)
#
# stdin JSONL 예시:
#   {"cmd":"ping"}
#   {"cmd":"measure","duration_s":10,"integration_ms":50,"sample_interval_s":1.0,"avg_count":3,"out_dir":"C:/.../OES/CH1"}
#   {"cmd":"reset"}
#   {"cmd":"close"}
#
# stdout JSON은 기존과 동일하게 kind=status/started/finished 등을 사용한다.
