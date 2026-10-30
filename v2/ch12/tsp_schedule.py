"""TSP 매일 예약 — v1 runtime/tsp_runtime.py 의 예약 부분(__init__ 111-135·251-307·460-505·751-843, PC-143~146) 그대로.

v1 그대로
- 예약 시각 입력(TSP_SetTime_edit — v2 time_text): 비어 있으면 TSP_DAILY_HH:MM('05:00')으로 채움. 시각 = 입력이 'HH:MM'
  (또는 '5시30분' — '^\\s*(\\d{1,2})\\s*(?::|시)\\s*(\\d{1,2})', 0~23·0~59)이면 그 값, 아니면 설정값(입력이 비어 있지 않았으면
  '예약 시각 형식 오류(예: 05:00) → 기본값 HH:MM 사용').
- 기동 때 TSP_DAILY_ENABLE 이면 매일 예약 등록(실패 '[TSP] 예약 초기화 실패: ..'). 체크박스(toggle): 켜면 설정 다시 읽기·시각
  결정·입력창 HH:MM 정규화·매일 예약(실패 '[TSP] 예약 등록 실패: ..'), 끄면 예약 취소.
- 등록 '[TSP] 예약 실행 등록: YYYY-mm-dd HH:MM:SS (매일=True|False)'(기존 예약은 조용히 취소), 취소 '[TSP] 예약 실행 취소됨'.
  다음 시각 = 오늘 HH:MM, 이미 지났으면(같은 시각 포함) 내일 — 놓친 회차는 따라잡지 않는다.
- 그 시각: TSP 실행 중(엔진 running)이거나 CH1 챔버·CH1 PC·TSP 실행 중(run_state)이면 기다리지 않고
  '[TSP] 예약 시각 도달했지만 {TSP|리소스(Chamber/PC/TSP)} 사용 중 → 오늘 예약 취소' → (1회 예약이면 '[TSP] 1회 예약이므로 더 이상
  재시도하지 않습니다.' 끝) → 다음날. 아니면 시작(엔진 start — 화면 입력값), 못 하면 '[TSP] ⚠ 예약(mm-dd HH:MM) 실행 못함 →
  오늘 건너뜀 (사유: {거절 사유 또는 사유 불명})'. 반복이면 '[TSP] 다음 반복 예약: ..', 아니면 '[TSP] 1회 예약 실행 완료(반복 없음).'.
  STOP 은 예약을 건드리지 않는다. 표시 'Daily HH:MM / Next mm-dd' 또는 'No schedule', 체크 = 예약 살아 있음.
- 로그는 엔진 로그 길(outputs.tsp_txt — '[TSP] [TSP] 예약 …' 접두어 중복 PC-151, 실행 사이 줄은 다음 실행 파일 앞에 PC-149).
v2 에서 다른 것(사용자 지시 — 프리스퍼터 예약과 같은 저장)
- 예약 상태를 v2 설정 폴더 tsp_schedule.json 에 저장(tsp/reserved·tsp/reserve_hhmm·tsp/repeat_daily, 원자적 교체). 기동 때
  restore(): 저장이 있으면 그것(예약이면 그 시각으로 다시 등록 + 'TSP 예약 복원: 매일 HH:MM', 꺼 두었으면 예약 없음), 저장이
  없으면 v1 처럼 TSP_DAILY_ENABLE·설정 시각. v1 은 저장하지 않아 재시작하면 늘 설정값(TSP_DAILY_ENABLE·05:00)으로 돌아갔다.
- 프로그램 종료(close)는 저장을 바꾸지 않는다(다음 기동에 복원). 기다림은 예약 스레드 하나에서 clock.wait_until(가상 시계 시험).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from v2.ch12.pre_sputter import next_time_at, parse_hhmm

SETTINGS_FILE = "tsp_schedule.json"
WAIT_CHUNK_S = 60.0                # 벽시계를 다시 보는 간격(가상 시계에서도 같은 결과 — 남은 시간이 더 짧으면 그만큼)


class TspSchedule:
    def __init__(self, engine, settings_dir: Optional[Path] = None) -> None:
        self.e = engine
        self.clock = engine.clock
        self.settings_path = Path(settings_dir) / SETTINGS_FILE if settings_dir is not None else None
        self.repeat_daily = False
        self.when: Optional[datetime] = None
        self.text = "No schedule"
        self.checked = False
        self._gen = 0
        self._live: Optional[int] = None         # 살아 있는 예약 루프의 세대(None = 예약 없음)
        self._thread = None
        self._refresh_cfg()
        self.time_text = ""
        if not self.time_text:                                   # tsp_runtime.py:114-117
            self.time_text = f"{int(self.hh):02d}:{int(self.mm):02d}"

    # ───────── 설정·시각 ─────────
    def _refresh_cfg(self) -> None:
        c = self.e.cfg
        self.enable = bool(c.get("TSP_DAILY_ENABLE", True))
        self.hh = int(c.get("TSP_DAILY_HH", 5))
        self.mm = int(c.get("TSP_DAILY_MM", 0))

    def _now(self) -> datetime:
        return datetime.fromtimestamp(self.clock.wall())

    def _log(self, msg: str) -> None:
        self.e._log(msg)

    def effective_time(self) -> Tuple[int, int]:
        """_effective_schedule_time(tsp_runtime.py:297-306)."""
        hh, mm = parse_hhmm(self.time_text)
        if hh is not None:
            return hh, mm
        if (self.time_text or "").strip():
            self._log(f"예약 시각 형식 오류(예: 05:00) → 기본값 {self.hh:02d}:{self.mm:02d} 사용")
        return self.hh, self.mm

    def set_time_text(self, text: str) -> None:
        """예약 시각 입력창(TSP_SetTime_edit) 글자 — 예약은 toggle 로 다시 등록할 때 반영(v1 그대로)."""
        self.time_text = str(text or "").strip()

    @property
    def alive(self) -> bool:
        return self._live is not None

    def _refresh_ui(self, when: Optional[datetime] = None) -> None:
        """_refresh_schedule_ui(tsp_runtime.py:482-505)."""
        alive = self._live is not None
        if when is not None:
            self.when = when
        elif not alive:
            self.when = None
        self.checked = alive
        self.text = self.when.strftime("Daily %H:%M / Next %m-%d") if (alive and self.when is not None) else "No schedule"

    # ───────── 등록·취소(tsp_runtime.py:751-786) ─────────
    def schedule_run_at(self, when: datetime, repeat_daily: bool = False, save: bool = True) -> None:
        self.cancel(silent=True, save=False)
        self.repeat_daily = bool(repeat_daily)
        self._gen += 1
        g = self._live = self._gen
        self._thread = self.clock.start_thread(lambda: self._loop(g, when), "TSPStartScheduler")
        self._log(f"[TSP] 예약 실행 등록: {when.strftime('%Y-%m-%d %H:%M:%S')} (매일={self.repeat_daily})")
        self._refresh_ui(when)
        if save:
            self._save(True)

    def cancel(self, silent: bool = False, save: bool = True) -> None:
        """cancel_schedule — silent 이 아니면 '[TSP] 예약 실행 취소됨'."""
        self._gen += 1
        self._live = None
        self.clock.notify()
        self.repeat_daily = False
        self.when = None
        self._refresh_ui()
        if not silent:
            self._log("[TSP] 예약 실행 취소됨")
        if save and not silent:
            self._save(False)

    def toggle(self, checked: bool) -> bool:
        """on_schedule_toggled(tsp_runtime.py:463-480). 지금 예약이 살아 있는지 돌려준다."""
        if checked:
            try:
                self._refresh_cfg()
                hh, mm = self.effective_time()
                self.hh, self.mm = hh, mm
                self.time_text = f"{int(hh):02d}:{int(mm):02d}"
                self.schedule_run_at(next_time_at(self._now(), hh, mm), repeat_daily=True)
            except Exception as e:
                self._log(f"[TSP] 예약 등록 실패: {e!r}")
                self._refresh_ui()
        else:
            if self._live is not None:
                self.cancel()
            else:
                self._refresh_ui()
        return self._live is not None

    def init_daily(self) -> None:
        """기동 때(tsp_runtime.py:124-132) — TSP_DAILY_ENABLE 이면 매일 예약."""
        try:
            if self.enable:
                hh, mm = self.effective_time()
                self.hh, self.mm = hh, mm
                self.schedule_run_at(next_time_at(self._now(), hh, mm), repeat_daily=True, save=False)
        except Exception as e:
            self._log(f"[TSP] 예약 초기화 실패: {e!r}")
        self._refresh_ui(self.when)

    # ───────── 저장·복원(v2) ─────────
    def _read(self) -> Dict[str, Any]:
        if self.settings_path is None:
            return {}
        try:
            return json.loads(self.settings_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save(self, reserved: bool) -> None:
        if self.settings_path is None:
            return
        d = self._read()
        d["tsp/reserved"] = bool(reserved)
        if reserved:
            d["tsp/reserve_hhmm"] = f"{int(self.hh):02d}:{int(self.mm):02d}"
            d["tsp/repeat_daily"] = bool(self.repeat_daily)
        try:
            self.settings_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.settings_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
            os.replace(tmp, self.settings_path)
        except Exception:
            pass

    def restore(self) -> Optional[str]:
        """기동 때 — 저장이 있으면 그 상태, 없으면 v1 기동(init_daily). 남긴 문구(시험·로그)."""
        d = self._read()
        if "tsp/reserved" not in d:
            self.init_daily()
            return None
        if not d.get("tsp/reserved"):
            self._refresh_ui()
            return "TSP 예약 복원: 예약 없음"
        hhmm = str(d.get("tsp/reserve_hhmm", "") or "")
        hh, mm = parse_hhmm(hhmm)
        if hh is None:
            self.init_daily()
            return f"TSP 예약 복원 실패: 시각 형식 오류({hhmm!r}) — 설정값으로"
        self.hh, self.mm = hh, mm
        self.time_text = f"{hh:02d}:{mm:02d}"
        rep = bool(d.get("tsp/repeat_daily", True))
        self.schedule_run_at(next_time_at(self._now(), hh, mm), repeat_daily=rep, save=False)
        return f"TSP 예약 복원: {'매일 ' if rep else ''}{hh:02d}:{mm:02d}"

    def close(self) -> None:
        """프로그램 종료 — 예약 스레드만 끝낸다(저장은 그대로 — 다음 기동에 복원)."""
        self._gen += 1
        self._live = None
        self.clock.notify()

    def state(self) -> Dict[str, Any]:
        return {"checked": self.checked, "text": self.text, "time_text": self.time_text,
                "when": None if self.when is None else self.when.strftime("%Y-%m-%d %H:%M:%S"),
                "repeat_daily": self.repeat_daily}

    # ───────── 예약 루프(tsp_runtime.py:788-843) ─────────
    def _ok(self, g: int) -> bool:
        return self._live == g

    def _loop(self, g: int, when: datetime) -> None:
        ended = False
        try:
            while self._ok(g):
                while self._ok(g):
                    rem = (when - self._now()).total_seconds()
                    if rem <= 0:
                        break
                    self.clock.wait_until(self.clock.monotonic() + min(rem, WAIT_CHUNK_S), lambda: not self._ok(g))
                if not self._ok(g):
                    return
                rs = self.e.rs
                if (self.e._running or rs.is_running("chamber", 1) or rs.is_running("pc", 1) or rs.is_running("tsp")):
                    who = "TSP" if self.e._running else "리소스(Chamber/PC/TSP)"
                    self._log(f"[TSP] 예약 시각 도달했지만 {who} 사용 중 → 오늘 예약 취소")
                    if not self.repeat_daily:
                        self._log("[TSP] 1회 예약이므로 더 이상 재시도하지 않습니다.")
                        break
                    when = when + timedelta(days=1)
                    self._log(f"[TSP] 다음 반복 예약: {when.strftime('%Y-%m-%d %H:%M:%S')}")
                    self._refresh_ui(when)
                    continue
                started = False
                try:
                    started = bool(self.e.start(None, "local", by="schedule").get("ok"))
                except Exception as e:
                    self._log(f"[TSP] 예약 시작 실패: {e!r}")
                if not started:
                    why = self.e.start_block_reason or "사유 불명"
                    self._log(f"[TSP] ⚠ 예약({when.strftime('%m-%d %H:%M')}) 실행 못함 → 오늘 건너뜀 (사유: {why})")
                if not self.repeat_daily:
                    self._log("[TSP] 1회 예약 실행 완료(반복 없음).")
                    break
                when = when + timedelta(days=1)
                self._log(f"[TSP] 다음 반복 예약: {when.strftime('%Y-%m-%d %H:%M:%S')}")
                self._refresh_ui(when)
            ended = True
        finally:
            if ended and self._live == g:        # 1회 예약이 끝남 — 예약 없음(취소·종료로 끝난 루프는 건드리지 않음)
                self._live = None
                self._refresh_ui()
                self._save(False)
