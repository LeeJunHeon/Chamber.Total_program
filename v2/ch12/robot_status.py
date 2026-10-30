"""로봇 상태 질의·공정 시작 명령 — v1 host/handlers.py:531-1003 동작 그대로.

GET_SPUTTER_STATUS(즉시 — 메모리 판정 + PLC L_ATM 1비트), GET_LOADING_1/2_SENSOR(1비트), GET_RECIPE(폴더 스캔 RECIPE_SCAN_TIMEOUT_S),
START_SPUTTER(인자·busy·lock_chN·게이트·Main Valve → 엔진 시작 → 시작 가드 신호까지 CHAMBER_HOST_START_WAIT_TIMEOUT_S),
START_PLASMA_CLEANING(엔진 시작 → PC_HOST_START_WAIT_TIMEOUT_S). 공정 시작 명령은 요청을 받자마자 Robot CSV 기록키를 발급하고
(v1 _hostlog_begin), 실패 응답이면서 엔진이 아직 소유하지 않은 키는 '거절'로 마감한다(v1 _hostlog_reject_if_unowned).

v1 에서 런타임 안에 있던 '시작 가드 결과 대기'(chamber_runtime.py:8108-8129, plasma_cleaning_runtime.py:3207-3214·3275-3297)는
여기로 왔다(엔진은 on_preflight 로 결과만 알린다) — 문구·시간 한도·E206(다른 PC 시작 대기 중)은 v1 그대로.
"""
from __future__ import annotations

import threading
from pathlib import Path
from typing import Optional, Tuple

from v2.ch12.errors import CodedRuntimeError
from v2.ch12.robot_base import Json, RobotBase, cfg_tuple
from v2.ch12.robot_engines import scan_recipe_folder


class _Preflight:
    """on_preflight 를 한 번 받는 상자(늦게 온 신호는 버린다)."""

    def __init__(self, clock) -> None:
        self.clock = clock
        self.result: Optional[Tuple[bool, Optional[str], str]] = None
        self.closed = False
        self._lk = threading.Lock()

    def __call__(self, ok: bool, code: Optional[str] = None, detail: str = "") -> None:
        with self._lk:
            if self.closed or self.result is not None:
                return
            self.result = (bool(ok), code, "" if detail is None else str(detail))
        self.clock.notify()

    def wait(self, timeout_s: float) -> Optional[Tuple[bool, Optional[str], str]]:
        self.clock.wait_until(self.clock.monotonic() + float(timeout_s), lambda: self.result is not None)
        with self._lk:
            self.closed = True
            return self.result


def _classify_start_error(e: RuntimeError, *, recipe_error: bool) -> Tuple[str, str]:
    """v1 START 의 RuntimeError 분류(handlers.py:915-929·990-1001): .code → '레시피 오류' 시작 E230(챔버만) →
    '1분 대기'/'cooldown'/'preflight timeout' E700 → E511."""
    msg = str(e)
    code = getattr(e, "code", None)
    if not code and recipe_error and msg.lstrip().startswith("레시피 오류"):
        code = "E230"
    if not code and ("1분 대기" in msg or "cooldown" in msg.lower() or "preflight timeout" in msg.lower()):
        code = "E700"
    return msg, code or "E511"


class StatusOps(RobotBase):
    # ───────── GET_SPUTTER_STATUS ─────────
    def get_sputter_status(self, payload: Json) -> Json:
        try:
            chamber_1 = self.chamber_status(1)
            chamber_2 = self.chamber_status(2)
            loadlock = self.loadlock_status()
            with self._plc_command("GET_SPUTTER_STATUS"):
                with self._plc_call():
                    atm = self.read_bit("L_ATM")
                vacuum = (not bool(atm))
                return self._ok(Chamber_1=chamber_1, Chamber_2=chamber_2, Loadlock_Chamber=loadlock, vacuum=vacuum,
                                eta=self.build_eta())
        except Exception as e:
            return self._fail(e, code=getattr(e, "code", None))

    # ───────── GET_RECIPE ─────────
    def get_recipe(self, data: Json) -> Json:
        raw_timeout = self.cfg.get("RECIPE_SCAN_TIMEOUT_S", 8.0)
        try:
            folder = str(data.get("folder") or "").strip().upper()
            allowed = cfg_tuple(self.cfg.get("ROBOT_RECIPE_FOLDERS", ("CH1", "CH2", "ALD")))
            if folder not in allowed:
                return self._fail(f"folder는 {allowed} 중 하나여야 합니다. (입력={folder!r})", code="E226")
            base_dir = Path(self.cfg["ROBOT_RECIPE_ROOT_DIR"])
            target_dir = base_dir / folder
            timeout_s = float(raw_timeout)
            scan = getattr(self.engines, "recipe_folder_scan", None) if self.engines is not None else None
            files = (scan or scan_recipe_folder)(str(target_dir), timeout_s)
            return self._ok("OK", base_dir=str(base_dir), folder=folder, files=list(files), count=len(files))
        except TimeoutError:
            return self._fail(f"GET_RECIPE timeout ({raw_timeout}s)", code="E227")
        except Exception as e:
            return self._fail(e)

    # ───────── GET_LOADING_n_SENSOR ─────────
    def get_loading_sensor(self, which: int, payload: Json) -> Json:
        try:
            key = "LOADING_1_SENSOR_LAMP" if int(which) == 1 else "LOADING_2_SENSOR_LAMP"
            try:
                with self._plc_call():
                    v = self.read_bit(key)
            except KeyError as e:
                return self._fail(f"PLC 주소맵에 {key}가 없습니다: {e}", code="E411")
            except Exception as e:
                return self._fail(e, code=getattr(e, "code", None) or "E412")
            with self._plc_command(f"GET_LOADING_{which}_SENSOR"):
                pass
            return self._ok("OK", value=bool(v))
        except Exception as e:
            return self._fail(e)

    # ───────── 게이트·Main Valve 상태(START_SPUTTER·VACUUM·게이트 공용, handlers.py:1006-1078) ─────────
    def _read_gate_state(self, ch: int) -> Json:
        if ch not in (1, 2):
            raise ValueError(f"지원하지 않는 CH: {ch}")
        with self._plc_call():
            open_lamp = bool(self.read_bit(f"G_V_{ch}_OPEN_LAMP"))
            close_lamp = bool(self.read_bit(f"G_V_{ch}_CLOSE_LAMP"))
        if close_lamp and not open_lamp:
            state = "closed"
        elif open_lamp and not close_lamp:
            state = "open"
        elif not open_lamp and not close_lamp:
            state = "moving_or_unknown"
        else:
            state = "invalid_both_true"
        return {"ch": ch, "state": state, "open_lamp": open_lamp, "close_lamp": close_lamp}

    def _read_main_valve_state(self, ch: int) -> Json:
        if ch not in (1, 2):
            raise ValueError(f"지원하지 않는 CH: {ch}")
        with self._plc_call():
            sw = bool(self.read_bit(f"M_V_{ch}_SW"))
            interlock = bool(self.read_bit(f"M_V_{ch}_인터락"))
        if sw and interlock:
            state = "open"
        elif not interlock:
            state = "interlock_off"
        else:
            state = "closed"
        return {"ch": ch, "state": state, "sw": sw, "interlock": interlock}

    # ───────── START_SPUTTER ─────────
    def start_sputter(self, data: Json) -> Json:
        try:
            _ch = int(data.get("ch") or 0)
        except Exception:
            _ch = 0
        key = self._hostlog_begin(f"CH{_ch}" if _ch in (1, 2) else "CH?", str(data.get("recipe") or ""))
        try:
            res = self._start_sputter_impl(data, key)
        except BaseException as e:
            self._hostlog_reject_if_unowned(key, f"핸들러 예외: {e!r}")
            raise
        if isinstance(res, dict) and res.get("result") == "fail":
            self._hostlog_reject_if_unowned(key, self._hostlog_reason_from_res(res))
        return res

    def _start_sputter_impl(self, data: Json, key: str) -> Json:
        ch = int(data.get("ch") or 0)
        recipe = str(data.get("recipe") or "").strip()
        if ch not in (1, 2):
            return self._fail("ch는 1 또는 2만 허용합니다.", code="E201")
        if not recipe:
            return self._fail("recipe가 비어 있습니다. (CSV 경로 또는 레시피 문자열 필요)", code="E202")
        ready = False
        try:
            f = self.ch_facts(ch)
            ready = bool(f and f.get("ready", True))
        except Exception:
            ready = True                     # v1: 런타임 객체는 있음(상태 조회 예외는 busy 검사가 통과시킴)
        if not ready:
            return self._fail(f"Chamber CH{ch} runtime not ready", code="E203")
        busy = self._fail_if_ch_busy(ch, f"START_SPUTTER_CH{ch}")
        if busy is not None:
            return busy
        with self.lock_ch[ch]:
            busy = self._fail_if_ch_busy(ch, f"START_SPUTTER_CH{ch}")
            if busy is not None:
                return busy
            with self._plc_command(f"START_SPUTTER_CH{ch}"):
                try:
                    st = self._read_gate_state(ch)
                except KeyError as e:
                    return self._fail(f"PLC 주소맵에 gate lamp 키가 없습니다: {e}", code="E411")
                except Exception as e:
                    return self._fail(e, code=getattr(e, "code", None) or "E412")
                if st["state"] != "closed":
                    return self._fail(f"START_SPUTTER 불가 — CH{ch} gate가 CLOSED가 아님({st['state']})", code="E301")
                try:
                    mv = self._read_main_valve_state(ch)
                except KeyError as e:
                    return self._fail(f"PLC 주소맵에 Main Valve 키가 없습니다: {e}", code="E411")
                except Exception as e:
                    return self._fail(e, code=getattr(e, "code", None) or "E412")
                if mv["state"] != "open":
                    return self._fail(f"START_SPUTTER 불가 — CH{ch} Main Valve가 열려있지 않음"
                                      f"(state={mv['state']}, sw={mv['sw']}, interlock={mv['interlock']})", code="E301")
                try:
                    self._wait_chamber_start(ch, recipe, key)
                    return self._ok("SPUTTER START OK", ch=ch, min_total_s=self._min_total_s(ch))
                except RuntimeError as e:
                    msg, code = _classify_start_error(e, recipe_error=True)
                    return self._fail(msg, code=code)
                except Exception as e:
                    return self._fail(e)

    def _wait_chamber_start(self, ch: int, recipe: str, key: str) -> None:
        """엔진 시작 → 시작 가드 결과 대기(v1 chamber_runtime.py:8108-8129). 실패는 RuntimeError(문구 그대로)."""
        box = _Preflight(self.clock)
        self.engines.start_sputter(ch, recipe, self.request_meta(key), box)
        t = float(self.cfg.get("CHAMBER_HOST_START_WAIT_TIMEOUT_S", 10.0))
        got = box.wait(t)
        if got is None:
            raise RuntimeError(f"preflight timeout ({t:.1f}s) "
                               "(start guard 또는 내부 대기로 인해 프리플라이트 미도달)")
        ok, code, reason = got
        if not ok:
            if code:
                raise CodedRuntimeError(reason, code)
            raise RuntimeError(reason)

    def _min_total_s(self, ch: int) -> Optional[int]:
        try:
            return self.engines.min_total_s(ch)
        except Exception:
            return None

    # ───────── START_PLASMA_CLEANING ─────────
    def start_plasma_cleaning(self, data: Json) -> Json:
        sel = 1
        try:
            sel = int((self.engines.pc_state() or {}).get("selected_ch", 1) or 1) if self.engines is not None else 1
        except Exception:
            sel = 1
        key = self._hostlog_begin(f"PC(CH{sel})", str(data.get("recipe") or ""))
        try:
            res = self._start_pc_impl(data, key, sel)
        except BaseException as e:
            self._hostlog_reject_if_unowned(key, f"핸들러 예외: {e!r}")
            raise
        if isinstance(res, dict) and res.get("result") == "fail":
            self._hostlog_reject_if_unowned(key, self._hostlog_reason_from_res(res))
        return res

    def _start_pc_impl(self, data: Json, key: str, sel: int) -> Json:
        recipe = str(data.get("recipe") or "").strip()
        if not recipe:
            return self._fail("recipe가 비어 있습니다. (CSV 경로 또는 레시피 문자열 필요)", code="E202")
        ready = False
        if self.engines is not None:
            try:
                ready = bool((self.engines.pc_state() or {}).get("ready", True))
            except Exception:
                ready = True
        if not ready:
            return self._fail("Plasma Cleaning runtime not ready", code="E204")
        with self._plc_command("START_PLASMA_CLEANING"):
            try:
                self._wait_pc_start(sel, recipe, key)
                return self._ok("PLASMA CLEANING START OK")
            except RuntimeError as e:
                msg, code = _classify_start_error(e, recipe_error=False)
                return self._fail(msg, code=code)
            except Exception as e:
                return self._fail(e)

    def _wait_pc_start(self, sel: int, recipe: str, key: str) -> None:
        """v1 PlasmaCleaningRuntime.start_with_recipe_string 의 E206·결과 대기(plasma_cleaning_runtime.py:3207-3214·3275-3301)."""
        if self._pc_start_pending:
            raise CodedRuntimeError("다른 Plasma Cleaning start 요청이 이미 처리 중입니다.", "E206")
        self._pc_start_pending = True
        try:
            box = _Preflight(self.clock)
            self.engines.start_pc(sel, recipe, self.request_meta(key), box)
            t = float(self.cfg.get("PC_HOST_START_WAIT_TIMEOUT_S", 10.0))
            got = box.wait(t)
            if got is None:
                raise RuntimeError(f"preflight timeout({t:.1f}s) "
                                   "(쿨다운/가드 등으로 프리플라이트에 도달하지 못했습니다)")
            ok, code, reason = got
            if not ok:
                if code:
                    raise CodedRuntimeError(reason or "Plasma Cleaning start failed", code)
                raise RuntimeError(reason or "Plasma Cleaning start failed")
        finally:
            self._pc_start_pending = False

