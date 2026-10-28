# controller/thickness_service.py
# -*- coding: utf-8 -*-
"""두께 모니터(STM-100) 채널별 상시 읽기 서비스 — asyncio 단일 루프, Qt 없음(HeaterService 패턴).

프로그램이 켜져 있는 동안 공정 여부·레시피 사용 여부와 무관하게 항상 읽는다(패널 표시). 연속 기록 CSV 는 공정 중에만 —
런타임이 공정 시작·종료 때 set_recording(on, 공정명) 으로 알린다(서비스는 런타임 상태를 읽지 않는다).
  S·T 는 THICKNESS_POLL_INTERVAL_S, U·V·M 은 THICKNESS_SLOW_POLL_S, 적용값 E?/F?/J? 는 연결 직후와 MATERIAL_READ_S 마다.
명령 요청(zero, set_material)은 폴링과 같은 루프에서 순서대로 실행한다(요청이 들어오면 다음 폴링 전에 먼저).
장비 Å → nm 변환은 여기 한 곳(_A_TO_NM)에서만 한다.

실패 정책(Evaporator stm_service 와 같은 방향): 한두 번 실패로 끊김을 만들지 않는다.
  재연결은 통신이 안 될 때만 — 통신 실패(응답 없음·송신 실패)·프레임 오류(체크섬·길이·모르는 코드)가
  MAX_FAIL_BEFORE_RECONNECT 회 연속이면. 프레임은 정상인데 내용이 이상한 '형식 오류'·'값 없음'(XTAL FAIL 중 빈칸 응답)은
  재연결 사유가 아니다(오류 줄 기록, 형식 오류 로그는 같은 문구 60초에 1번).
  stale 은 정상 두께 값 기준(마지막 정상 S 뒤 THICKNESS_STALE_S 초과),
  link_ok 는 통신 기준(최근 THICKNESS_STALE_S 안에 정상 프레임 응답 — 값 유무와 무관).
  S 가 값 없음이 되면 M(크리스탈 상태)을 바로 읽는다(XTAL FAIL 을 곧바로 반영).
리스너 예외는 잡아서 로그만(같은 문구는 60초에 1번) 남기고 폴링은 계속한다.

status():
  connected, ok_ts(마지막 정상 S 단조시간), age_s, stale, link_ok, link_age_s(마지막 정상 프레임 뒤 초), thickness_nm, rate_nm_s, freq_hz, life_pct,
  crystal_ok, reset_seen(전원 리셋 플래그 누적), seq(새 S 값마다 +1 — 같은 값 중복 판정 방지),
  density, z_factor, tooling(장비에 적용된 값), material(마지막으로 적용한 재료 이름), last_error, process_running, process_name, t
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime
import time
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from device.thickness_monitor import (AsyncThicknessMonitor, ThicknessChecksumError, ThicknessCommandRejected,
                                      ThicknessCommError, ThicknessError, ThicknessFrameError, ThicknessTimeout,
                                      ThicknessValueUnavailable, ThicknessVerifyError)
from lib.thickness_config import ThicknessConfig
from lib.thickness_factors import ThicknessFactors, load_factors
from lib.thickness_materials import Material, default_materials_file, find_material, load_materials
from lib.thickness_logger import ThicknessCsvLogger

_A_TO_NM = 0.1                    # 1 Å = 0.1 nm
MAX_FAIL_BEFORE_RECONNECT = 6     # 통신 실패·프레임 오류 연속 횟수
FORMAT_LOG_INTERVAL_S = 60.0      # 같은 형식 오류 로그 간격
MATERIAL_READ_S = 60.0            # 적용값(E?/F?/J?) 다시 읽는 간격
CSV_PERIOD_S = 1.0                # 연속 기록 1초 1행
LISTENER_LOG_INTERVAL_S = 60.0
LOOP_ERR_LOG_INTERVAL_S = 60.0
JOB_TIMEOUT_S = 15.0              # 요청 1건 상한(set_material 은 명령 6~12개)


def error_text(e: Exception) -> str:
    """화면·로그용 한 줄 — 종류(응답 없음/통신 실패/체크섬/거부/형식/값 없음) + 내용."""
    for cls, label in ((ThicknessTimeout, "응답 없음"), (ThicknessCommError, "통신 실패"),
                       (ThicknessChecksumError, "체크섬 오류"), (ThicknessFrameError, "프레임 오류"),
                       (ThicknessCommandRejected, "명령 거부"),
                       (ThicknessValueUnavailable, "값 없음"), (ThicknessVerifyError, "되읽기 불일치"),
                       (ThicknessError, "응답 형식 오류")):
        if isinstance(e, cls):
            return f"{label} — {e}"
    return f"{type(e).__name__}: {e}"


class ThicknessService:
    def __init__(self, cfg: ThicknessConfig, *,
                 log: Callable[[str, str], None],
                 device: Any = None,
                 process_info: Optional[Callable[[], Tuple[bool, str]]] = None,
                 materials_file=None,
                 clock: Callable[[], float] = time.monotonic):
        self.cfg = cfg
        self.ch = int(cfg.ch)
        self._log_cb = log
        self._now = clock
        self._process_info = process_info
        self.materials_file = str(materials_file or default_materials_file())   # 재료 목록(채널 공용)
        self.dev = device if device is not None else AsyncThicknessMonitor(
            cfg.host, cfg.port, cmd_timeout_s=cfg.cmd_timeout_s, log=self._log, clock=clock)
        self._listeners: List[Callable[[dict], None]] = []
        self._jobs: Deque[Tuple[Callable[[], Awaitable[Any]], asyncio.Future, str]] = deque()
        self._wake: Optional[asyncio.Event] = None
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

        self._seq = 0
        self._ok_ts: Optional[float] = None
        self._link_ts: Optional[float] = None    # 마지막 정상 프레임 응답(값 없음·형식 오류 포함)
        self._fail_n = 0
        self._unavail_n = 0
        self._was_connected = False
        self._next_slow = 0.0
        self._next_material = 0.0
        self._csv_last = -1e9
        self._recording = False              # 연속 기록 CSV — 공정 중에만(set_recording)
        self._rec_name = ""
        self._throttle: Dict[str, float] = {}
        self._st: Dict[str, Any] = self._blank_status()

        self.factors, warns = load_factors(cfg.factor_file)
        for w in warns:
            self._log(f"⚠ Factor 파일: {w}")
        self.materials: List[Material] = []
        self._load_materials()
        self.csv = ThicknessCsvLogger(cfg.log_dir, self.ch, warn=lambda m: self._log(f"⚠ {m}"), clock=clock)

    # ═══════════════════════ 공개 API ═══════════════════════
    def start(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        """폴링 태스크를 띄운다. loop 를 주면 그 루프에(MainWindow 는 루프가 돌기 전에 생성된다)."""
        lp = loop
        if lp is None:
            try:
                lp = asyncio.get_running_loop()
            except RuntimeError:
                lp = asyncio.get_event_loop()
        if self._task is None or self._task.done():
            self._stopping = False
            self._task = lp.create_task(self._run(), name=f"CH{self.ch}ThicknessPoll")

    async def shutdown(self, timeout: float = 2.0) -> None:
        """루프 정지 → 대기 중 요청 실패 처리 → 연결 닫기 → CSV 닫기."""
        self._stopping = True
        t, self._task = self._task, None
        if t is not None and not t.done():
            t.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(t, return_exceptions=True), timeout=float(timeout))
            except (Exception, asyncio.CancelledError):
                pass
        while self._jobs:
            _f, fut, _n = self._jobs.popleft()
            if not fut.done():
                fut.set_exception(ThicknessError("프로그램 종료 — 요청 취소"))
        try:
            await asyncio.wait_for(self.dev.stop(), timeout=float(timeout))
        except (Exception, asyncio.CancelledError):
            pass
        with contextlib.suppress(Exception):
            self.csv.close()

    def status(self) -> dict:
        return dict(self._st)

    def add_listener(self, cb: Callable[[dict], None]) -> None:
        if cb not in self._listeners:
            self._listeners.append(cb)

    def remove_listener(self, cb: Callable[[dict], None]) -> None:
        with contextlib.suppress(ValueError):
            self._listeners.remove(cb)

    @property
    def clock(self) -> Callable[[], float]:
        return self._now

    def set_recording(self, on: bool, process_name: str = "") -> None:
        """공정 시작(on=True, 공정명)·종료(on=False) — 켜져 있는 동안만 연속 기록 CSV 에 1초 1행, 끄면 파일을 닫는다."""
        if on:
            self._rec_name = str(process_name or "")
            self._csv_last = -1e9            # 시작 즉시 1행
            self._recording = True
            return
        self._recording = False
        self._rec_name = ""
        with contextlib.suppress(Exception):
            self.csv.close()

    @property
    def recording(self) -> bool:
        return self._recording

    def _load_materials(self) -> List[str]:
        mats, warns = load_materials(self.materials_file)
        self.materials = mats
        for w in warns:
            self._log(f"⚠ 재료 목록: {w}")
        return warns

    def find_material(self, name: str) -> Optional[Material]:
        """재료 목록에서 이름(대소문자 무시)으로 찾는다."""
        return find_material(self.materials, name)

    def reload_factors(self) -> List[str]:
        """Factor 파일·재료 목록을 다시 읽는다(설정창 저장 뒤 — 진행 중인 공정에는 영향 없고 다음 공정 시작 때 적용). 경고 목록."""
        fac, warns = load_factors(self.cfg.factor_file)
        self.factors = fac
        warns = list(warns) + self._load_materials()
        self._log("Factor 파일·재료 목록 다시 읽음" + (f" — 경고 {len(warns)}건" if warns else ""))
        for w in warns:
            self._log(f"⚠ Factor 파일: {w}")
        return warns

    async def zero(self, mode: str = "B", why: str = "") -> Tuple[bool, str]:
        """영점(B=두께·타이머, C=두께만). 폴링과 같은 순서로 실행. (성공, 사유)."""
        m = str(mode or "").strip().upper()
        try:
            await self._submit(lambda: self.dev.zero(m), f"영점({m})")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._log(f"⚠ 두께 영점({m}) 실패{(' — ' + why) if why else ''}: {e}")
            return False, f"영점 실패: {e}"
        self._log(f"두께 영점({m}){(' — ' + why) if why else ''}")
        return True, ""

    async def set_material(self, density: float, z_factor: float, tooling: float, material: str = "") -> dict:
        """장비에 density/Z/tooling 을 쓰고 되읽어 확인한다(드라이버 set_material). 실패하면 예외. material = 재료 이름(표시용)."""
        res = await self._submit(lambda: self.dev.set_material(density, z_factor, tooling), "재료값 적용")
        self._st.update(density=res.get("density"), z_factor=res.get("z_factor"), tooling=res.get("tooling"),
                        material=str(material or ""))
        self._log(f"두께 모니터 재료값 적용{(' ' + material) if material else ''} density {res.get('density')} · "
                  f"Z {res.get('z_factor')} · tooling {res.get('tooling')}%")
        self._publish()
        return res

    # ═══════════════════════ 내부: 요청 큐 ═══════════════════════
    def _evt(self) -> asyncio.Event:
        if self._wake is None:
            self._wake = asyncio.Event()
        return self._wake

    async def _submit(self, factory: Callable[[], Awaitable[Any]], name: str) -> Any:
        if self._stopping or self._task is None or self._task.done():
            raise ThicknessError("두께 서비스가 동작 중이 아님")
        fut = asyncio.get_running_loop().create_future()
        self._jobs.append((factory, fut, name))
        self._evt().set()
        return await asyncio.wait_for(asyncio.shield(fut), timeout=JOB_TIMEOUT_S + 5.0)

    async def _run_jobs(self) -> None:
        while self._jobs:
            factory, fut, name = self._jobs.popleft()
            if fut.done():
                continue
            try:
                res = await asyncio.wait_for(factory(), timeout=JOB_TIMEOUT_S)
            except asyncio.CancelledError:
                if not fut.done():
                    fut.set_exception(ThicknessError(f"{name} 취소"))
                raise
            except asyncio.TimeoutError:
                if not fut.done():
                    fut.set_exception(ThicknessError(f"{name} 시간 초과({JOB_TIMEOUT_S:g}초)"))
            except Exception as e:
                if not fut.done():
                    fut.set_exception(e)
            else:
                if not fut.done():
                    fut.set_result(res)

    # ═══════════════════════ 내부: 루프 ═══════════════════════
    async def _run(self) -> None:
        try:
            await self.dev.start()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._log(f"⚠ 두께 모니터 시작 실패: {e!r}")
        while not self._stopping:
            t0 = self._now()
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log_throttled("loop", f"⚠ 두께 폴링 처리 예외: {e!r}", LOOP_ERR_LOG_INTERVAL_S)
            wait = max(0.05, float(self.cfg.poll_interval_s) - (self._now() - t0))
            evt = self._evt()
            if not self._jobs:
                evt.clear()
                try:
                    await asyncio.wait_for(evt.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass

    async def poll_once(self) -> None:
        """요청 실행 → (연결돼 있으면) S·T, 때가 되면 U·V·M·적용값 → 상태 갱신·발행 → (공정 중이면) CSV."""
        await self._run_jobs()
        conn = bool(self.dev.is_connected)
        if conn and not self._was_connected:
            self._fail_n = self._unavail_n = 0
            self._next_slow = self._next_material = 0.0     # 연결 직후 바로 읽는다
        self._was_connected = conn
        if conn:
            await self._poll_fast()
            now = self._now()
            if self.dev.is_connected and now >= self._next_slow:
                self._next_slow = now + float(self.cfg.slow_poll_s)
                await self._poll_slow()
            if self.dev.is_connected and now >= self._next_material:
                self._next_material = now + MATERIAL_READ_S
                await self._read_material()
        self._refresh()
        self._publish()
        now = self._now()
        if self._recording and now - self._csv_last >= CSV_PERIOD_S:
            self._csv_last = now
            self.csv.write_row(dict(self._st, process_running=True, process_name=self._rec_name))

    async def _poll_fast(self) -> None:
        try:
            a = await self.dev.read_thickness_a()
        except asyncio.CancelledError:
            raise
        except ThicknessValueUnavailable as e:
            self._on_unavailable(e)
            if self._unavail_n == 1 and self._st.get("crystal_ok") is not False:
                self._next_slow = 0.0        # 값이 막 없어졌다 — 이번 폴링에서 M 을 바로 읽는다(XTAL FAIL 확인)
        except ThicknessError as e:
            if await self._on_error(e):
                return                       # 통신 실패면 T 는 건너뛴다
        else:
            self._link_seen()
            self._unavail_n = 0
            self._seq += 1
            self._ok_ts = self._now()
            self._st["thickness_nm"] = a * _A_TO_NM
            self._st["seq"] = self._seq
        try:
            r = await self.dev.read_rate_a_s()
        except asyncio.CancelledError:
            raise
        except ThicknessValueUnavailable:
            self._link_seen()
            self._st["rate_nm_s"] = None
        except ThicknessError as e:
            await self._on_error(e)
        else:
            self._link_seen()
            self._st["rate_nm_s"] = r * _A_TO_NM

    async def _poll_slow(self) -> None:
        for key, fn in (("freq_hz", self.dev.read_frequency_hz),
                        ("life_pct", self.dev.read_crystal_life),
                        ("crystal_ok", self.dev.read_crystal_ok)):
            if not self.dev.is_connected:
                return
            try:
                self._st[key] = await fn()
            except asyncio.CancelledError:
                raise
            except ThicknessValueUnavailable:
                self._link_seen()
                self._st[key] = None         # XTAL FAIL 이면 U/V 가 빈값으로 온다 — 실패로 세지 않는다
            except ThicknessError as e:
                await self._on_error(e)
            else:
                self._link_seen()

    async def _read_material(self) -> None:
        for key, fn in (("density", self.dev.read_density),
                        ("z_factor", self.dev.read_z_factor),
                        ("tooling", self.dev.read_tooling)):
            if not self.dev.is_connected:
                return
            try:
                self._st[key] = await fn()
            except asyncio.CancelledError:
                raise
            except ThicknessValueUnavailable:
                self._link_seen()
                self._st[key] = None
            except ThicknessError as e:
                await self._on_error(e)
            else:
                self._link_seen()

    def _link_seen(self) -> None:
        """정상 프레임 응답을 받았다 — 통신은 된다(값 유무와 무관). 통신 실패 연속 횟수 초기화."""
        self._link_ts = self._now()
        self._fail_n = 0

    def _on_unavailable(self, e: Exception) -> None:
        """S 값 없음 — 통신은 정상(재연결 사유 아님)."""
        self._link_seen()
        self._unavail_n += 1
        self._set_error(f"값 없음 {self._unavail_n}회 연속")

    async def _on_error(self, e: Exception) -> bool:
        """읽기 오류 분류 — 통신 실패·프레임 오류면 실패로 세고(재연결 정책) True, 프레임은 정상인데 내용이 틀린
        형식 오류·명령 거부면 기록만 하고 False."""
        if isinstance(e, (ThicknessCommError, ThicknessFrameError)):
            await self._on_fail(e)
            return True
        self._link_seen()
        txt = error_text(e)
        self._set_error(txt)
        self._log_throttled(f"fmt:{txt}", f"⚠ 두께 모니터 {txt}", FORMAT_LOG_INTERVAL_S)
        return False

    def _set_error(self, text: str) -> None:
        """마지막 오류 한 줄(벽시계 시각 포함 — 복구 뒤에도 남아 언제 난 오류인지 보이게)."""
        self._st["last_error"] = f"{datetime.datetime.now():%H:%M:%S} {text}"

    async def _on_fail(self, e: Exception) -> None:
        self._fail_n += 1
        self._set_error(error_text(e))
        if self._fail_n >= MAX_FAIL_BEFORE_RECONNECT:
            self._fail_n = self._unavail_n = 0
            self._log(f"⚠ 두께 모니터 연속 실패 {MAX_FAIL_BEFORE_RECONNECT}회 — 재연결: {e}")
            try:
                await self.dev.reconnect(f"연속 실패: {e}")
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    # ═══════════════════════ 내부: 상태·발행 ═══════════════════════
    @staticmethod
    def _blank_status() -> dict:
        return {
            "connected": False, "ok_ts": None, "age_s": None, "stale": True, "link_ok": False, "link_age_s": None,
            "thickness_nm": None, "rate_nm_s": None,
            "freq_hz": None, "life_pct": None, "crystal_ok": None,
            "reset_seen": 0, "seq": 0,
            "density": None, "z_factor": None, "tooling": None, "material": "",
            "last_error": "", "process_running": False, "process_name": "", "t": None,
        }

    def _refresh(self) -> None:
        now = self._now()
        st = self._st
        st["t"] = now
        st["connected"] = bool(self.dev.is_connected)
        st["ok_ts"] = self._ok_ts
        st["age_s"] = None if self._ok_ts is None else now - self._ok_ts
        st["stale"] = self._ok_ts is None or (now - self._ok_ts) > float(self.cfg.stale_s)
        st["link_age_s"] = None if self._link_ts is None else now - self._link_ts
        st["link_ok"] = (st["connected"] and self._link_ts is not None
                         and (now - self._link_ts) <= float(self.cfg.stale_s))
        st["reset_seen"] = int(getattr(self.dev, "reset_seen", 0) or 0)   # 드라이버가 감지·L 확인·로그를 맡는다
        running, name = False, ""
        if self._process_info is not None:
            try:
                running, name = self._process_info()
            except Exception:
                running, name = False, ""
        st["process_running"] = bool(running)
        st["process_name"] = str(name or "") if running else ""

    def _publish(self) -> None:
        snap = dict(self._st)
        for cb in list(self._listeners):
            try:
                cb(dict(snap))
            except Exception as e:
                self._log_throttled(f"listener:{e!r}", f"두께 상태 리스너 예외: {e!r}", LISTENER_LOG_INTERVAL_S)

    def _log(self, msg: str) -> None:
        try:
            self._log_cb("THICKNESS", msg)
        except Exception:
            pass

    def _log_throttled(self, key: str, msg: str, interval: float) -> None:
        now = self._now()
        if now - self._throttle.get(key, -1e9) >= interval:
            self._throttle[key] = now
            self._log(msg)
