"""1초 행 → v1 기록 출력(조립이 부른다 — 사용자 지시 3장 (2) '1초 스냅샷에서 만들어도 됨').

행이 닫힐 때마다 on_row(row) 하나로 아래에 값을 넘긴다(각 모듈이 자기 주기·엣지를 v1 그대로 가진다 — 여기는 값 옮기기만).
- PLC 코일 CSV(plc_coil_csv.PlcCoilCsv, 5 s): 행의 plc.<코일>·plc.dcv_* → v1 PLC_COIL_MAP 이름. PLC 품질이 ok 가 아닌 행은
  코일 스냅샷 없음(v1 '빈 스냅샷'), worker 가 끊겼으면 disconnected, 코일 값이 모두 비면(드라이버가 코일 로그를 쉰 tick) backoff,
  레지스터가 비면 reg_failed(행은 쓰고 칸만 빈칸). 요약 줄의 plan 은 행의 plc.coil_plan.
- selfwatch(selfwatch.SelfWatchLog, 30 s): 행의 prog.*(snapshot_fields.program_state).
- 두께 CSV(thickness_csv.ThicknessCsv, 챔버마다, 공정 중에만): 드라이버 status()(v1 thickness logger 와 같은 원천).
  기록 시작·끝(set_recording)은 공정 엔진이 부른다 — thickness[ch].
- CH2 히터 CSV(heater_csv.HeaterCsv): 히터 상태를 주는 함수(heater_state — 히터 드라이버·엔진이 꽂는다)가 있을 때만.
  heater_state() → None 또는 (st, {"owner_label", "hold", "sat_clamp", "ramp_target"}).
쓰기는 모두 QueueWriter 하나(쓰기 스레드) — tick 은 기다리지 않는다. 기록 오류는 출력 모듈이 삼킨다(v1 과 같음).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Mapping, Optional

from v2.ch12.drivers import plc_map as PM
from v2.ch12.outputs.base import QueueWriter
from v2.ch12.outputs.heater_csv import HeaterCsv
from v2.ch12.outputs.plc_coil_csv import REG_KEYS, PlcCoilCsv, v1_coil_keys
from v2.ch12.outputs.selfwatch import SelfWatchLog
from v2.ch12.outputs.thickness_csv import ThicknessCsv

_COIL_COLS = tuple((k, f"plc.{PM.field_name(k)}") for k in v1_coil_keys())
_REG_COLS = tuple((k, f"plc.{PM.field_name(k)}") for k in REG_KEYS)


def row_time(row: Mapping[str, Any]) -> datetime:
    return datetime.fromisoformat(str(row["ts"]))


def plc_coil_args(row: Mapping[str, Any], connected: bool) -> Dict[str, Any]:
    """1초 행 → PlcCoilCsv.on_tick 인자."""
    if not connected:
        return dict(connected=False)
    if row.get("plc.q") != "ok":
        return dict(connected=True, coils=None)
    coils = {k: row.get(c) for k, c in _COIL_COLS}
    if all(v is None for v in coils.values()):
        return dict(connected=True, backoff=True)
    regs = {k: row.get(c) for k, c in _REG_COLS}
    failed = any(v is None for v in regs.values())
    plan = row.get("plc.coil_plan")
    return dict(connected=True, coils={k: bool(v) for k, v in coils.items() if v is not None},
                regs={k: v for k, v in regs.items() if v is not None}, reg_failed=failed,
                plan=-1 if plan is None else int(plan))


class RowFeed:
    def __init__(self, cfg: Mapping[str, Any], clock, *, plc_connected: Callable[[], bool],
                 thickness: Optional[Dict[str, Any]] = None, log: Optional[Callable[[str, str], None]] = None,
                 on_selfwatch: Optional[Callable[[str, str, str], None]] = None) -> None:
        self.cfg, self.clock = cfg, clock
        self.writer = QueueWriter("v1-outputs")
        self._log = log or (lambda src, msg: None)
        self.plc_connected = plc_connected
        now = lambda: datetime.fromtimestamp(clock.wall())                      # noqa: E731
        self.plc_coil = PlcCoilCsv(cfg, writer=self.writer, log=lambda m: self._log("plc_coil_csv", m),
                                   monotonic=clock.monotonic)
        self.selfwatch = SelfWatchLog(cfg, writer=self.writer, log=lambda m: self._log("selfwatch", m),
                                      on_event=on_selfwatch, now=now, monotonic=clock.monotonic, wall_time=clock.wall)
        self.stm = dict(thickness or {})                                         # ch → Stm100Driver(켜진 것만)
        self.thickness = {ch: ThicknessCsv(1 if ch == "ch1" else 2, cfg, writer=self.writer,
                                           warn=lambda m, ch=ch: self._log(f"{ch}.thickness_csv", m),
                                           monotonic=clock.monotonic) for ch in self.stm}
        self.heater = HeaterCsv(cfg, writer=self.writer, warn=lambda m: self._log("ch2.heater_csv", m),
                                monotonic=clock.monotonic, now=now)
        self.heater_state: Optional[Callable[[], Optional[tuple]]] = None
        self.rows = 0
        self.errors = 0

    def start(self) -> None:
        self.selfwatch.start()

    def on_row(self, row: Mapping[str, Any]) -> None:
        self.rows += 1
        ts = row_time(row)
        mono = self.clock.monotonic()
        for name, fn in (("plc_coil", lambda: self.plc_coil.on_tick(ts, mono=mono, **plc_coil_args(row, self.plc_connected()))),
                         ("selfwatch", lambda: self.selfwatch.on_tick(_selfwatch_state(row))),
                         ("thickness", lambda: self._thickness(ts)),
                         ("heater", self._heater)):
            try:
                fn()
            except Exception as e:                  # 출력 하나의 오류가 다른 출력·tick 을 막지 않는다
                self.errors += 1
                self._log("outputs", f"{name}: {e!r}")

    def _thickness(self, ts: datetime) -> None:
        for ch, t in self.thickness.items():
            if t.recording:
                t.on_tick(self.stm[ch].status(), ts)

    def _heater(self) -> None:
        f = self.heater_state
        if f is None:
            return
        got = f()
        if got:
            st, kw = got
            self.heater.on_tick(st, **(kw or {}))

    @property
    def pending(self) -> int:
        return self.writer.pending

    def stop(self, timeout_s: float = 5.0) -> None:
        self.plc_coil.writer.put(self.plc_coil.appender.close)
        for t in self.thickness.values():
            self.writer.put(t.file.close)
        if self.heater.is_open():
            self.heater.stop()
        self.writer.stop(timeout_s)


def _selfwatch_state(row: Mapping[str, Any]) -> Dict[str, Any]:
    from v2.ch12.snapshot_fields import selfwatch_state
    return selfwatch_state(row)
