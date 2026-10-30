"""챔버12 프리스퍼터 예약 — v1 runtime/pre_sputter_runtime.py(PreSputterRuntime) + main.py:499-512·1630-1805(전용 레시피·예약 저장).

v1 그대로(챔버마다 하나 — main 이 ch1=…/ch2=None, parallel=False 로 둘을 만든다)
- 예약(start_daily): 기존 예약을 조용히 취소하고 매일 HH:MM(지났으면 내일), 상태 '예약됨 · 매일 HH:MM', 남은 시간 'HH:MM:SS',
  로그 '[PreSputter] 예약 등록: YYYY-mm-dd HH:MM:SS (매일 반복)'. 취소(stop) '예약 취소됨'(실제로 취소했을 때만).
- 그 시각: 챔버가 바쁘면(is_busy — 프리플라이트·Chuck·큐 행 사이 포함) 그날 PASS('해당 챔버가 이미 공정 중 → 이번 예약 PASS',
  상태 'PASS(공정 중) · 다음 HH:MM', 다음날 재예약). 아니면 1회 실행:
  · 사용자 레시피 큐를 비우고 보관(snapshot_recipe_queue) → 전용 레시피 경로가 있으면 그 파일로(start_with_recipe — origin
    presputter), 없으면 화면 현재값으로(start_ui) → 시작 신호(엔진 busy)를 최대 START_CONFIRM_TIMEOUT_S(60 s) 기다림
    ('{라벨} 시작 신호 미확인 (timeout)') → 끝날 때까지 tick 마다 감시 → 큐 되돌림(아직 바쁘면 보관분 파기 — 엔진 쪽 문구).
  · 상태 '완료 · 다음 HH:MM' / '실행 실패 · 다음 HH:MM', 한 회차 예외는 '실행 중 예외: … — 이번 회차 실패, 다음 예약 유지'.
- 알려진 이상 동작(UI-143) 그대로: 화면 현재값 시작은 시작 가드(쿨다운·입력 검사 등)가 거절해도 True(v1 start_presputter_from_ui 는
  공정 중일 때만 False) → 60 s 동안 시작 신호를 기다린 뒤 '시작 신호 미확인 (timeout)' → 실행 실패.
- 로그 '[HH:MM:SS] [PreSputter] {CH1|CH2} {msg}' 는 PC 로그창(log_sink)에만 — 구글챗 없음(v1 chat=None).
- 저장(v1 QSettings VanaM/CH1_2_Program 의 presputter/recipe_chN·reserved_chN·reserve_hhmm_chN) → v2 설정 폴더 presputter.json
  같은 키 이름. 복원(restore): 예약이면 시각 검사(형식 'HH:MM'·범위) 후 start_daily + 'PreSputter CHn 예약 복원: 매일 HH:MM',
  레시피 파일이 없어도 경로는 유지(표시 '(파일 없음)').
시각은 clock.wall()(로컬) — 가상 시계 시험 가능. 대기는 러너 스레드 하나(clock.start_thread)에서 clock.wait_until.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Optional

START_CONFIRM_TIMEOUT_S = 60.0
SETTINGS_FILE = "presputter.json"


def next_time_at(now: datetime, hh: int, mm: int) -> datetime:
    """오늘 hh:mm, 이미 지났으면(같은 시각 포함) 내일(_next_time_at)."""
    cand = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    return cand + timedelta(days=1) if cand <= now else cand


def fmt_hms(seconds: float) -> str:
    s = int(max(0, seconds))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def parse_hhmm(raw: str):
    """preSputter_SetTime_edit — 'HH:MM' 또는 '8시30분'. 잘못되면 (None, None)."""
    m = re.match(r"^\s*(\d{1,2})\s*(?::|시)\s*(\d{1,2})", str(raw or ""))
    if not m:
        return None, None
    hh, mm = int(m.group(1)), int(m.group(2))
    return (hh, mm) if (0 <= hh <= 23 and 0 <= mm <= 59) else (None, None)


class PreSputter:
    def __init__(self, clock, engine, *, settings_dir, start_ui: Optional[Callable[[], bool]] = None,
                 ui_params: Optional[Callable[[], dict]] = None, log_sink: Optional[Callable[[str], None]] = None,
                 hh: int = 6, mm: int = 0, tick_s: float = 1.0,
                 start_confirm_timeout_s: float = START_CONFIRM_TIMEOUT_S) -> None:
        self.clock, self.e = clock, engine
        self.label = engine.ch.upper()
        self.settings_path = Path(settings_dir) / SETTINGS_FILE
        self.ui_params = ui_params
        self._start_ui = start_ui
        self.log_sink = log_sink
        self.lines: list = []
        self.hh, self.mm = int(hh), int(mm)
        self.tick_s = float(tick_s) if tick_s > 0 else 1.0
        self.start_confirm_timeout_s = float(start_confirm_timeout_s)
        self.recipe_path: Optional[str] = None
        self.status_text, self.left_text = "예약 없음", "--:--:--"
        self._gen = 0
        self._active = False
        self._thread = None
        self._repeat = True
        self.when: Optional[datetime] = None

    # ───────── 표시·로그 ─────────
    def _now(self) -> datetime:
        return datetime.fromtimestamp(self.clock.wall())

    def _hhmm(self) -> str:
        return f"{self.hh:02d}:{self.mm:02d}"

    def _log(self, msg: str) -> None:
        line = f"[{self._now():%H:%M:%S}] [PreSputter] {self.label} {msg}"
        self.lines.append(line)
        if self.log_sink:
            try:
                self.log_sink(line)
            except Exception:
                pass

    @property
    def is_reserved(self) -> bool:
        return self._thread is not None and self._thread.is_alive() and self._active

    # ───────── 예약 ─────────
    def start_daily(self) -> None:
        self.stop(silent=True)
        self._repeat = True
        self.when = next_time_at(self._now(), self.hh, self.mm)
        self.status_text = f"예약됨 · 매일 {self._hhmm()}"
        self.left_text = fmt_hms((self.when - self._now()).total_seconds())
        self._gen += 1
        self._active = True
        g = self._gen
        self._thread = self.clock.start_thread(lambda: self._loop(g), f"PreSputter.{self.label}")
        self._log(f"[PreSputter] 예약 등록: {self.when:%Y-%m-%d %H:%M:%S} (매일 반복)")
        self.save(reserved=True)

    def schedule(self, text: str) -> bool:
        """schedule_from_ui — 'HH:MM' 으로 매일 예약 갱신."""
        hh, mm = parse_hhmm(text)
        if hh is None:
            self._log("[설정] 잘못된 시간 형식(HH:MM)")
            self.status_text = "시간 형식 오류 (HH:MM)"
            return False
        self.hh, self.mm = hh, mm
        self.start_daily()
        return True

    def stop(self, silent: bool = False) -> None:
        had = self.is_reserved
        self._gen += 1
        self._active = False
        self._repeat = False
        self.clock.notify()
        self.status_text, self.left_text = "예약 없음", "--:--:--"
        if had and not silent:
            self._log("예약 취소됨")
            self.save(reserved=False)

    def set_recipe_path(self, path: Optional[str]) -> None:
        self.recipe_path = path or None
        self.save()

    def recipe_display(self) -> str:
        p = self.recipe_path
        if not p:
            return "UI 현재값 사용"
        return os.path.basename(p) if os.path.exists(p) else f"{os.path.basename(p)} (파일 없음)"

    # ───────── 저장·복원(v1 QSettings 키 이름) ─────────
    def _read(self) -> Dict[str, Any]:
        try:
            return json.loads(self.settings_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save(self, reserved: Optional[bool] = None) -> None:
        n = self.e.n
        d = self._read()
        d[f"presputter/recipe_ch{n}"] = self.recipe_path or ""
        if reserved is not None:
            d[f"presputter/reserved_ch{n}"] = bool(reserved)
            if reserved:
                d[f"presputter/reserve_hhmm_ch{n}"] = self._hhmm()
        try:
            self.settings_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.settings_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
            os.replace(tmp, self.settings_path)
        except Exception:
            pass

    def restore(self) -> Optional[str]:
        """부팅 때 — 레시피 경로·예약 복원. 돌려주는 값 = 남긴 문구(시험·로그)."""
        n = self.e.n
        d = self._read()
        path = d.get(f"presputter/recipe_ch{n}") or ""
        if path:
            self.recipe_path = path
        if not d.get(f"presputter/reserved_ch{n}", False):
            return None
        hhmm = str(d.get(f"presputter/reserve_hhmm_ch{n}", "") or "")
        m = re.match(r"^\s*(\d{1,2})\s*:\s*(\d{1,2})\s*$", hhmm)
        if not m:
            return f"PreSputter CH{n} 예약 복원 실패: 시각 형식 오류({hhmm!r})"
        hh, mm = int(m.group(1)), int(m.group(2))
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return f"PreSputter CH{n} 예약 복원 실패: 범위 초과({hhmm!r})"
        self.hh, self.mm = hh, mm
        self.start_daily()
        return f"PreSputter CH{n} 예약 복원: 매일 {hh:02d}:{mm:02d}"

    # ───────── 실행 ─────────
    def _alive(self, g: int) -> bool:
        return self._gen == g

    def _loop(self, g: int) -> None:
        when = self.when
        while self._alive(g):
            try:
                while self._alive(g):
                    rem = (when - self._now()).total_seconds()
                    if rem <= 0:
                        break
                    self.left_text = fmt_hms(rem)
                    self.clock.wait_until(self.clock.monotonic() + min(self.tick_s, rem), lambda: not self._alive(g))
                if not self._alive(g):
                    return
                self.left_text = "00:00:00"
                if self.e.busy:
                    self._log("[PreSputter] 해당 챔버가 이미 공정 중 → 이번 예약 PASS")
                    when = when + timedelta(days=1)
                    self.when = when
                    self.status_text = f"PASS(공정 중) · 다음 {self._hhmm()}"
                    self.left_text = fmt_hms((when - self._now()).total_seconds())
                    self._log(f"[PreSputter] 다음 반복 예약: {when:%Y-%m-%d %H:%M:%S}")
                    continue
                self._run_one(g)
            except Exception as e:
                self._log(f"[PreSputter] 실행 중 예외: {e!r} — 이번 회차 실패, 다음 예약 유지")
                self.status_text = f"실행 오류 · 다음 {self._hhmm()}"
            if not self._alive(g):
                return
            if not self._repeat:
                self._log("[PreSputter] 1회 예약 실행 완료(반복 없음).")
                return
            when = when + timedelta(days=1)
            self.when = when
            self._log(f"[PreSputter] 다음 반복 예약: {when:%Y-%m-%d %H:%M:%S}")

    def run_once_now(self) -> None:
        """start_once_now(디버그·수동) — 부른 스레드에서 끝까지."""
        self._run_one(self._gen)

    def _start(self) -> bool:
        if self.recipe_path:
            from v2.ch12.recipe import load_recipe_file
            self._log(f"[PreSputter] {self.label} 레시피 실행: {os.path.basename(self.recipe_path)}")
            r = load_recipe_file(self.recipe_path, origin="host")
            why = r.message if not r.ok else (self.e.load(r.rows, source="local", name=os.path.basename(self.recipe_path))
                                              or self.e.start(source="local", owner="local", origin="presputter"))
            if why:
                self._log(f"[PreSputter] {self.label} 레시피 시작 실패: {why!r}")
                return False
            return True
        self._log(f"[PreSputter] {self.label} UI 현재값으로 실행")
        if self._start_ui is not None:
            ok = bool(self._start_ui())
        elif self.e.is_running:
            self.e.log("MAIN", f"[CH{self.e.n}] PreSputter: 이미 공정 중입니다.")
            ok = False
        else:
            self.e.start(source="local", owner="local", origin="presputter",
                         params=dict(self.ui_params() if self.ui_params else {}))
            self.e.log("MAIN", f"[CH{self.e.n}] PreSputter 자동 시작 (UI 현재값)")
            ok = True                                       # UI-143: 시작 가드가 거절해도 True(v1 그대로)
        if not ok:
            self._log(f"[PreSputter] {self.label} 시작 실패")
        return ok

    def _run_one(self, g: int) -> None:
        if self.e.busy:
            self._log(f"[PreSputter] {self.label} 이미 실행 중 → 건너뜀")
            return
        self.status_text, self.left_text = "Pre-Sputter 실행 중", "00:00:00"
        failed = False
        snap = self.e.snapshot_recipe_queue()
        try:
            if not self._start():
                failed = True
                return
            c = self.clock
            if not c.wait_until(c.monotonic() + self.start_confirm_timeout_s, lambda: self.e.busy or not self._alive(g)):
                self._log(f"[PreSputter] {self.label} 시작 신호 미확인 (timeout)")
                failed = True
                return
            while self.e.busy and self._alive(g):
                c.wait_until(c.monotonic() + self.tick_s, lambda: not self._alive(g))
        finally:
            self.e.restore_recipe_queue(snap)
            if self._alive(g):
                self.status_text = f"실행 실패 · 다음 {self._hhmm()}" if failed else f"완료 · 다음 {self._hhmm()}"
                if self.when is not None:
                    self.left_text = fmt_hms((next_time_at(self._now(), self.hh, self.mm) - self._now()).total_seconds())
        if not failed:
            self._log(f"[PreSputter] {self.label} 완료")

    def close(self) -> None:
        self._gen += 1
        self._active = False
        self.clock.notify()
