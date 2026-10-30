"""PLC 코일 CSV(일별) — v1 device/plc.py 의 _plc_coil_log_loop 파일 출력(LOG-059~063).

v1 그대로
- 경로 PLC_COIL_LOG_NAS_DIR(C:\\VanaM_Logs\\CH1&2\\CH1&2_PLC)\\YYYYMMDD.csv, 폴백 PLC_COIL_LOG_LOCAL_DIR(비면
  LOCAL_FALLBACK_ROOT\\PLC_Coil), utf-8-sig, 핸들 유지·매 행 flush, primary 재시도 10 s(log_hub.DailyCsvListAppender).
- 열: Timestamp(isoformat 초까지), PLC_COIL_MAP 키 전부(정의 순, 'TRUE'/'FALSE' — 없는 키는 FALSE), 레지스터 12개
  DCV_READ_0~3, DCV_WRITE_0~3, DCV_READ_4~7(정수, 실패면 빈칸).
- 주기 PLC_COIL_LOG_INTERVAL_S(5.0 s, 최소 0.5 s). PLC 끊김·백오프(연속 타임아웃 진행 중·저우선 연속 실패) 중이면 행 없음,
  코일 스냅샷이 비면 행 없음, 레지스터 실패는 행은 쓰고 칸만 빈칸.
- PLC_COIL_LOG_SUMMARY_S(60 s)마다 건너뜀이나 reg_failed 가 있으면 'WARN PLC COIL LOG summary(60s): …' 한 줄,
  NAS→LOCAL 전환 때 'PLC COIL LOG: NAS write failed -> switched to LOCAL (keep-handle)'.
v2: PLC 를 따로 읽지 않는다 — 1초 스냅샷 행에서 이 주기마다 뽑아 쓴다(on_tick 을 1초마다 부름). 블록 계획(plan)·블록 수는
드라이버 몫이라 요약 줄의 plan·blocks 는 부르는 쪽이 주는 값(없으면 -1).

부르는 법(1초 tick 마다)
  pc = PlcCoilCsv(cfg, coil_keys=None(=v1 PLC_COIL_MAP 순서), writer=w, log=fn)
  pc.on_tick(ts: datetime, connected=bool, backoff=bool, coils={키: bool}|None, regs={키: int}|None,
             reg_failed=bool, mono=초(가상 시계 monotonic))
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

from v2.ch12.outputs.base import Owned, QueueWriter
from v2.ch12.outputs.log_hub import DailyCsvListAppender
from v2.ch12.outputs.paths import Paths
from v2.ch12.params import outputs as P

REG_KEYS: List[str] = [
    "DCV_READ_0", "DCV_READ_1", "DCV_READ_2", "DCV_READ_3",
    "DCV_WRITE_0", "DCV_WRITE_1", "DCV_WRITE_2", "DCV_WRITE_3",
    "DCV_READ_4", "DCV_READ_5", "DCV_READ_6", "DCV_READ_7",
]


def v1_coil_keys() -> List[str]:
    """v1 PLC_COIL_MAP 키(정의 순) — v2/ch12/spec/devices_v1facts.py(device/plc.py 를 ast 로 옮긴 표)."""
    from v2.ch12.spec import devices_v1facts as F
    for d in (F.FACTS.values() if isinstance(F.FACTS, dict) else F.FACTS):
        cm = d.get("coil_map") if isinstance(d, dict) else None
        if cm:
            return list(cm.keys())
    raise RuntimeError("devices_v1facts 에 coil_map 이 없음")


def build_row(ts: datetime, coil_keys: Iterable[str], coils: Mapping[str, Any],
              reg_keys: Iterable[str], regs: Mapping[str, Any]) -> List[str]:
    row = [ts.isoformat(timespec="seconds")]
    for k in coil_keys:
        row.append("TRUE" if coils.get(k, False) else "FALSE")
    for k in reg_keys:
        row.append(str(regs.get(k, "")))
    return row


class PlcCoilCsv(Owned):
    def __init__(self, cfg: Optional[Mapping[str, Any]] = None, *, coil_keys: Optional[Iterable[str]] = None,
                 reg_keys: Optional[Iterable[str]] = None, writer: Optional[QueueWriter] = None,
                 log: Optional[Callable[[str], None]] = None,
                 monotonic: Optional[Callable[[], float]] = None) -> None:
        super().__init__(writer, "plc-coil-csv")
        self.cfg = cfg if cfg is not None else {}
        self.paths = Paths(self.cfg)
        self.coil_keys = list(coil_keys) if coil_keys is not None else v1_coil_keys()
        self.reg_keys = list(reg_keys) if reg_keys is not None else list(REG_KEYS)
        self.header = ["Timestamp", *self.coil_keys, *self.reg_keys]
        self._log_fn = log
        self.interval = max(0.5, float(P.get(self.cfg, "PLC_COIL_LOG_INTERVAL_S")))
        try:
            self.summary_s = float(P.get(self.cfg, "PLC_COIL_LOG_SUMMARY_S"))
        except Exception:
            self.summary_s = 60.0
        nas, local = self.paths.plc_coil_dirs()
        self._mono_now = 0.0
        self.appender = DailyCsvListAppender(primary_dir=nas, fallback_dir=local,
                                             filename_builder=lambda dt: f"{dt.strftime('%Y%m%d')}.csv",
                                             encoding="utf-8-sig", retry_primary_every_s=10.0,
                                             monotonic=monotonic or (lambda: self._mono_now))
        self._next_due: Optional[float] = None
        self._last_summary: Optional[float] = None
        self.stats = dict(ok=0, disconnected=0, empty_snapshot=0, budget_timeout=0, plc_error=0, write_failed=0,
                          backoff=0, reg_failed=0, low_fail=0)
        self.rows_queued = 0

    def _log(self, msg: str) -> None:
        if self._log_fn:
            try:
                self._log_fn(msg)
            except Exception:
                pass

    def on_tick(self, ts: datetime, *, connected: bool, backoff: bool = False,
                coils: Optional[Mapping[str, Any]] = None, regs: Optional[Mapping[str, Any]] = None,
                reg_failed: bool = False, mono: Optional[float] = None, budget_timeout: bool = False,
                plan: int = -1, blocks: int = -1) -> bool:
        """1초 tick. 이번에 행을 큐에 넣었으면 True."""
        now = float(mono) if mono is not None else ts.timestamp()
        if self._last_summary is None:
            self._last_summary = now
        if self._next_due is not None and now < self._next_due - 1e-6:
            return False
        self._next_due = now + self.interval
        wrote = False
        if not connected:
            self.stats["disconnected"] += 1
        elif backoff:
            self.stats["backoff"] += 1
        elif not coils:
            self.stats["budget_timeout" if budget_timeout else "empty_snapshot"] += 1
        else:
            if reg_failed:
                self.stats["reg_failed"] += 1
                self.stats["low_fail"] += 1
                regs = {}
            row = build_row(ts, self.coil_keys, coils, self.reg_keys, regs or {})
            self.writer.put(self._append, ts, now, row)
            self.stats["ok"] += 1           # v1 은 쓰기 뒤에 셌다 — 요약 줄이 쓰기 스레드를 기다리지 않게 넣을 때 센다
            self.rows_queued += 1
            wrote = True
        self._emit_summary(now, plan, blocks)
        return wrote

    def _append(self, ts: datetime, mono: float, row: List[str]) -> None:
        self._mono_now = mono
        try:
            self.appender.append_row(dt=ts, header=self.header, row=row)
            if self.appender.consume_switched_flag():
                self._log("PLC COIL LOG: NAS write failed -> switched to LOCAL (keep-handle)")
        except Exception as e:
            self.stats["ok"] = max(0, self.stats["ok"] - 1)
            self.stats["write_failed"] += 1
            self._log(f"PLC COIL LOG: write failed (ignored): {e!r}")

    def _emit_summary(self, now: float, plan: int, blocks: int) -> None:
        if (now - float(self._last_summary or 0.0)) < max(1.0, self.summary_s):
            return
        self._last_summary = now
        st = self.stats
        skips = sum(v for k, v in st.items() if k not in ("ok", "reg_failed", "low_fail"))
        if skips > 0 or st["reg_failed"] > 0:
            self._log("WARN PLC COIL LOG summary(%.0fs): ok=%d disconnected=%d empty=%d "
                      "budget_timeout=%d plc_error=%d write_failed=%d backoff=%d (plan=%d, blocks=%d) "
                      "reg_failed=%d low_fail=%d" % (
                          max(1.0, self.summary_s), st["ok"], st["disconnected"], st["empty_snapshot"],
                          st["budget_timeout"], st["plc_error"], st["write_failed"], st["backoff"],
                          plan, blocks, st["reg_failed"], st["low_fail"]))
        for k in st:
            st[k] = 0

    def stop(self, timeout_s: float = 5.0) -> None:
        self.writer.put(self.appender.close)
        self.stop_writer(timeout_s)
