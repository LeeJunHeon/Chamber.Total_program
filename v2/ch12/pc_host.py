"""플라즈마 클리닝 — 호스트(로봇) START 진입점·호스트 공정 로그. v1 runtime/plasma_cleaning_runtime.py:3190-3301·2688-2728.

host_start(recipe, origin_meta, on_result) — v1 start_with_recipe_string 그대로(E206 중복 → CSV E207·E209·E227·E208·형식 E209 →
  실행 중 E205(레시피 적용 전 — TEST-110) → 시작 → 프리플라이트 신호를 PC_HOST_START_WAIT_TIMEOUT_S 기다림, 넘으면
  'preflight timeout(…)' → E700). 결과 HostStartResult(ok, message, code=핸들러 분류 끝난 코드, raw_code=런타임이 실은 코드).
start_for_robot(recipe, request_meta, on_preflight) — robot_engines.RobotEngines.start_pc 모양(E206·결과 대기는 robot_ops 가 —
  v2/ch12/robot_status.py _wait_pc_start). 적재·검증 실패는 예외(.code), 시작 가드 결과는 on_preflight(ok, code, detail) 한 번.
pc_state_for_robot()·last_pc_result() — RobotEngines.pc_state·last_pc_result 의 '사실'.
_host_run_begin — 호스트 요청이면 Robot CSV 기록키 소유·레시피 정보·시작 표시([이상] log_file 늘 빈칸 — PC-098).
"""
from __future__ import annotations

import os
from typing import Any, Callable, Dict, Mapping, Optional

from v2.ch12 import pc_recipe as R
from v2.ch12.pc_recipe import HostStartError, HostStartResult
from v2.ch12.pc_steps import Box


class HostIO:
    # ───────── 호스트 START(3190-3301) ─────────
    def host_start(self, recipe: str, origin_meta: Optional[Mapping[str, Any]] = None,
                   on_result: Optional[Callable[[HostStartResult], None]] = None, source: str = "robot") -> Box:
        """로봇 START_PLASMA_CLEANING — 바로 돌아오고 결과는 on_result(HostStartResult)(반환한 Box 에도). 빈 recipe 는 E202,
        PC 없음 E204 는 핸들러(robot_ops) 몫. 여기서는 v1 start_with_recipe_string 그대로."""
        out = Box(self.clock)

        def finish(res: HostStartResult) -> None:
            out.set(res)
            if on_result is not None:
                try:
                    on_result(res)
                except Exception:
                    pass
        hp = self._host_pending
        if hp is not None and not hp.done:
            finish(self._host_fail(HostStartError("다른 Plasma Cleaning start 요청이 이미 처리 중입니다.", "E206")))
            return out
        self._host_pending = Box(self.clock)
        self._host_start_code = None
        self.clock.start_thread(lambda: finish(self._host_start_body(recipe, dict(origin_meta or {}), source)),
                                "pc-host-start")
        return out

    def _host_fail(self, e: BaseException) -> HostStartResult:
        raw = getattr(e, "code", None)
        return HostStartResult(False, str(e), R.host_error_code(str(e), raw), raw)

    def _reject_host_if_running(self) -> None:
        if self.running:
            msg = "이미 Plasma Cleaning이 실행 중입니다."
            self.popup("warning", "실행 중", msg)
            raise HostStartError(msg, "E205")

    def _prepare_host_recipe(self, s: str) -> None:
        """3201-3271 — 빈 값은 화면 값으로, '.csv' 는 적재(E227·E207·E209·E208) 뒤 실행 중 거절(E205) → 화면 적용, 그 밖 E209."""
        if not s:
            self._reject_host_if_running()
            self.host_recipe_path, self.host_recipe_rows = "", 1
        elif s.lower().endswith(".csv"):
            row, n = self._load_csv_with_timeout(s)
            if not row:
                raise HostStartError("CSV에 데이터 행이 없습니다.", "E208")
            self._reject_host_if_running()
            self.loaded_row = dict(row)
            self._apply_row(row)
            self.log("File", f"CSV 로드 완료: {s} → UI에 값 세팅")
            self.host_recipe_path, self.host_recipe_rows = s, int(n or 1)
        else:
            raise HostStartError("지원하지 않는 레시피 형식입니다. CSV 파일 경로만 허용됩니다.", "E209")

    def _host_start_body(self, recipe: str, meta: Dict[str, Any], source: str) -> HostStartResult:
        hp = self._host_pending
        try:
            self._prepare_host_recipe((recipe or "").strip())
            self._on_preflight = None
            self._click_start(source=source, by="", origin="host", origin_meta=meta)
            wait_s = self._cfgf("PC_HOST_START_WAIT_TIMEOUT_S", 10.0)
            self.clock.wait_until(self.clock.monotonic() + wait_s, lambda: hp.done)
            if not hp.done:
                raise RuntimeError(f"preflight timeout({wait_s:.1f}s) (쿨다운/가드 등으로 프리플라이트에 도달하지 못했습니다)")
            ok, reason = hp.value
            if not ok:
                code = self._host_start_code
                raise HostStartError(reason or "Plasma Cleaning start failed", code or None)
            return HostStartResult(True, reason, None, None)
        except Exception as e:
            return self._host_fail(e)
        finally:
            self._host_pending = None
            self._host_start_code = None

    def _load_csv_with_timeout(self, path: str):
        box = Box(self.clock)

        def work():
            try:
                box.set(("ok", R.load_host_csv(path, self.log)))
            except HostStartError as e:
                box.set(("coded", e))
            except RuntimeError as e:
                box.set(("rt", e))
            except Exception as e:
                box.set(("exc", e))
        self.clock.start_thread(work, "pc-csv-load")
        self.clock.wait_until(self.clock.monotonic() + R.CSV_LOAD_TIMEOUT_S, lambda: box.done)
        if not box.done:
            raise HostStartError(f"CSV 로드 30초 timeout (NAS 응답 지연): {path}", "E227")
        kind, v = box.value
        if kind == "ok":
            return v
        if kind in ("coded", "rt"):
            raise v
        raise R.e209_read_failed(path, v)


    # ───────── robot_engines.RobotEngines 어댑터 모양 ─────────
    def start_for_robot(self, recipe: str, request_meta: Optional[Mapping[str, Any]] = None,
                        on_preflight: Optional[Callable[[bool, Optional[str], str], None]] = None,
                        source: str = "robot") -> None:
        """RobotEngines.start_pc — 레시피 적재·실행 중 거절은 예외(HostStartError.code), 시작 가드 결과는
        on_preflight(ok, code, detail)(v1 _host_report_start 시점). E206·PC_HOST_START_WAIT_TIMEOUT_S 대기는 부르는 쪽."""
        s = (recipe or "").strip()
        self._prepare_host_recipe(s)

        def pre(ok: bool, reason: str, code: Optional[str]) -> None:
            if on_preflight is not None:
                on_preflight(bool(ok), code, str(reason))
        self._on_preflight = pre
        self._click_start(source=source, by="", origin="host", origin_meta=dict(request_meta or {}))

    def pc_state_for_robot(self) -> Dict[str, Any]:
        rs = self.run_state
        return {"ready": True, "running": {1: rs.is_running("pc", 1), 2: rs.is_running("pc", 2)},
                "cleaning": bool(self.running), "selected_ch": int(self.ch)}

    def last_pc_result(self) -> Dict[str, Any]:
        rs = self.run_state
        return {"finished": {1: rs.last_finished("pc", 1), 2: rs.last_finished("pc", 2)},
                "error": {1: rs.has_error("pc", 1), 2: rs.has_error("pc", 2)}}

    # ───────── 호스트 공정 로그(2688-2728) ─────────
    def _host_run_begin(self) -> None:
        self.host_key = None
        if str(self.origin or "ui") != "host":
            return
        key = str(self.origin_meta.get("key") or "")
        hl = self.host_log
        if hl is None or not key:
            return
        try:
            hl.mark_owned(key)
            pname = str((self.loaded_row or {}).get("Process_name") or (self.loaded_row or {}).get("process_name")
                        or "").strip()
            hl.update(key, recipe_name=os.path.basename(self.host_recipe_path) if self.host_recipe_path else "",
                      row_count=int(self.host_recipe_rows or 1), process_names=pname)
            hl.mark_started(key, self._now_dt(), log_file=None)         # [이상] log_file 늘 빈칸(PC-098)
        except Exception:
            pass
        self.host_key = key

