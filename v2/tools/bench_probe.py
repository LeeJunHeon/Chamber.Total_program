r"""챔버12 bench 실측 도구 — 장비 노트북에서 v1 을 끈 상태로, 실제 장비를 읽기 전용으로 붙여 설정을 전부 읽고 1초 수집·응답 시간을 잰다.

    python -m v2.tools.bench_probe --minutes 10
    (빼기: --skip ch2.ig(여러 번), 펄스 장비: --ch1-pulse rfpulse|dcpulse --ch2-pulse …(기본 none — 연결 안 함),
     설정 바꾸기: --set KEY=VALUE(예 --set PLC_TCP_HOST=192.168.1.2), 기록 폴더: --out <폴더>)

챔버K v2/tools/bench_probe.py(de8e946)와 같은 방식.
- RUN_MODE=bench 고정(모드 선택지 없음): 장비에 아무것도 쓰지 않는다(삼중 보호) — worker 가 쓰기·안전 요청을 BENCH_READ_ONLY 로
  거부하고, 드라이버도 writes_allowed=False(PLC Modbus 클라이언트 읽기 전용·MFC R 질의만 …), 포트 바로 위의 통신층 가드
  (v2.ch12.cmd_guard, bench 목록)가 질의 목록 밖 바이트를 포트에 닿기 전에 막는다. 펄스 장비는 드라이버가 출력 ON 때만 읽으므로
  이 도구가 질의만 직접 보낸다(가드 bench 목록 안).
- 장비: PLC(192.168.1.2:502), MFC·IG CH1·CH2, TSP, CH2 두께 모니터(CH2_SUPPORTS_THICKNESS 일 때), 고른 펄스 장비.
  --skip 과 꺼진 장비는 연결하지 않고 요약에 '제외'. TSP 운전 IG 는 CH1 IG 와 같은 포트라 따로 열지 않는다.
- 시작 전 안전 확인 — 걸리면 아무것도 쓰지 않고 끝낸다:
  · 연결을 열 수 없으면(v1 이 켜져 있거나 주소가 다름) 다시 시도하지 않고 종료(3)
  · PLC 를 읽을 수 없으면 종료(2) — 출력 상태를 확인할 수 없다
  · 출력 켜짐 흔적이면 종료(2): CH2 히터 RUN(M00400 — bench 는 워치독 D114 를 쓰지 않아 30 s 뒤 래더가 WD_ERR 로 끈다),
    D/A 설정 D4~D7 > 0, DC 측정 ≥ 5 W, RF 순방향 ≥ 5 W, 가스 유량 > 0.1 sccm, DC Pulse HV On, RF Pulse 출력 ON, TSP 승화(3·4·5)
  · DCV_SET 래치·RF 릴레이·가스 밸브·IG 점등만 켜진 것은 [참고] 로 남기고 계속(출력 없음)
- 설정 전부 읽기(안전 확인 뒤): 장비별 bench 질의를 한 번씩(장비마다 동시에, 장비 안에서는 드라이버 간격대로, 무응답이면 0.3 s 쉬고
  다음) + PLC 첫 읽기의 코일·레지스터 전부 → 요약 표 + inventory.json(원문 그대로 — 해석은 드라이버 파서로 '뜻'에만).
- N 분 1초 수집(SnapshotBus·TickLoop — 실행 프로그램과 같은 읽기) → 응답 시간(p50·p99·최대)·보낸 간격(최소·p50)·무응답·품질 통계.
- 기록: 바탕화면\v2_bench_YYYYMMDD_HHMMSS — events.jsonl(처음부터), inventory.json, 스냅샷(SQLite·CSV), 통신 녹화 rec_<장비>.jsonl,
  summary.txt. NAS·G:·운영 폴더(LOG_ROOT_DIR·LOCAL_DATA_DIR 등)에는 쓰지 않는다(그 아래를 --out 으로 주면 거부 — 종료 4).
  Ctrl+C 로 언제든 멈출 수 있고, 멈춰도 요약은 남긴다.
종료 코드: 0 정상, 2 안전 확인에서 멈춤, 3 연결 실패, 4 기록 폴더 거부.
"""
from __future__ import annotations

import argparse
import collections
import datetime as _dt
import json
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from v2.common.clock import RealClock
from v2.common.paramdef import defaults
from v2.common.recorder import Recorder
from v2.common.snapshot_bus import SnapshotBus, Source, TickLoop, iso_local
from v2.common.transport import TransportError, save_exchanges
from v2.common.worker import Request, Status
from v2.ch12 import cmd_guard as G
from v2.ch12.drivers import plc_map as PM
from v2.ch12.drivers.plc_conv import dc_scale, rf_scale
from v2.ch12.params import PARAMS
from v2.tools import bench_probe_dev as BD

NO_REPLY_PAUSE_S = 0.3
POWER_TRACE_W = 5.0
FLOW_TRACE_SCCM = 0.1
FORBIDDEN_OUT_KEYS = ("LOG_ROOT_DIR", "LOCAL_DATA_DIR", "OUTPUT_LOCAL_FIRST_DIR", "CAMERA_LOG_ROOT", "HOST_LOG_LOCAL_DIR",
                      "HOST_LOG_NAS_DIR", "OES_LOCAL_DIR", "LOG_SYNC_STATE_DIR", "GDRIVE_LOG_DIR", "PLC_COIL_LOG_NAS_DIR",
                      "NAS_COPY_DIR", "SPUTTER_CALIB_DB_DIR")
BANNER = """
==================================================================
  챔버12 v2 bench — 읽기 전용(RUN_MODE=bench)
  장비에 아무것도 쓰지 않습니다(하트비트 읽기·질의만, 설정·출력 쓰기 없음).
  v1 프로그램을 끈 상태에서 실행하세요. 멈추려면 Ctrl+C.
==================================================================""".strip("\n")


def _pct(xs: List[float], p: float) -> float:
    s = sorted(xs)
    if not s:
        return float("nan")
    return s[min(len(s) - 1, max(0, int(round(p / 100.0 * (len(s) - 1)))))]


def _ev_json(e) -> dict:
    out = {"type": type(e).__name__}
    for k in ("device", "kind", "detail", "reason", "code", "cmd", "t"):
        v = getattr(e, k, None)
        if v is not None:
            out[k] = v if isinstance(v, (str, int, float, bool)) else repr(v)
    return out


def _norm(p: str) -> str:
    return str(p).replace("/", "\\").rstrip("\\").lower()


def out_refused(out: Path, cfg: Dict[str, Any]) -> Optional[str]:
    """기록 폴더가 NAS·G:·운영 폴더 아래면 사유. bench 기록은 따로 둔다(운영 기록·동기화와 섞이지 않게)."""
    s = _norm(str(out))
    if s.startswith("\\\\"):
        return f"NAS(UNC) 경로: {out}"
    if s.startswith("g:"):
        return f"G: 드라이브: {out}"
    for k in FORBIDDEN_OUT_KEYS:
        root = _norm(str(cfg.get(k) or ""))
        if root and (s == root or s.startswith(root + "\\")):
            return f"운영 폴더 {k}={cfg.get(k)} 아래"
    return None


class BenchProbe:
    def __init__(self, out_root: Path, print_fn: Callable[[str], None] = print, cfg_over: Optional[Dict[str, Any]] = None,
                 skip: tuple = (), pulse: Optional[Dict[str, str]] = None) -> None:
        self.clock = RealClock()
        self.cfg = dict(defaults(PARAMS), **(cfg_over or {}))
        self.cfg["RUN_MODE"] = "bench"
        self.p = print_fn
        self.lines: List[str] = []
        self.events: List[Any] = []
        self.notes: List[str] = []
        self.inv: List[dict] = []
        self.rows: List[Dict[str, Any]] = []
        self.loop: Optional[TickLoop] = None
        self.recorder: Optional[Recorder] = None
        self.started = False
        self.stop_mono: Optional[float] = None
        self.t0 = self.clock.monotonic()
        self.pulse = dict(pulse or {})
        self.out = Path(out_root) / _dt.datetime.now().strftime("v2_bench_%Y%m%d_%H%M%S")
        self._events_f = None
        self.excluded: Dict[str, str] = {}
        eps = BD.endpoints(self.cfg, self.pulse)
        for k in BD.PROBES:
            if k in skip:
                self.excluded[k] = "제외(--skip)"
            elif k.endswith(".pulse") and k not in eps:
                self.excluded[k] = f"제외(--{k.split('.')[0]}-pulse none — 4007/4005 에 꽂힌 장비를 골라야 연결)"
            elif k == "ch2.stm100" and not self.cfg.get("CH2_SUPPORTS_THICKNESS"):
                self.excluded[k] = "제외(설정 꺼짐 — CH2_SUPPORTS_THICKNESS=false)"
        self.probes: Dict[str, BD.Probe] = {}
        for k in BD.PROBES:
            if k not in self.excluded:
                pr = BD.build(self.clock, self.cfg, k, eps[k], self._ev)
                self.probes[pr.name] = pr
        seen: Dict[str, str] = {}
        for pr in self.probes.values():
            if pr.endpoint in seen:
                raise ValueError(f"{pr.name} 와 {seen[pr.endpoint]} 가 같은 주소 {pr.endpoint} — 하나만 여세요(--skip)")
            seen[pr.endpoint] = pr.name

    def open_records(self) -> None:
        self.out.mkdir(parents=True, exist_ok=True)
        self._events_f = open(self.out / "events.jsonl", "a", encoding="utf-8", newline="\n")
        for e in self.events:
            self._write_ev(e)

    # ───────── 이벤트·출력 ─────────
    def _write_ev(self, e) -> None:
        if self._events_f is None:
            return
        try:
            j = _ev_json(e)
            j["t_s"] = round(self.clock.monotonic() - self.t0, 3)
            self._events_f.write(json.dumps(j, ensure_ascii=False) + "\n")
            self._events_f.flush()
        except Exception:
            pass

    def _ev(self, e) -> None:
        self.events.append(e)
        self._write_ev(e)
        if self.recorder is not None:
            try:
                self.recorder.put_event(iso_local(self.clock.wall()), "warn" if getattr(e, "reason", None) else "info",
                                        str(getattr(e, "reason", None) or getattr(e, "kind", "?")).upper(),
                                        str(getattr(e, "device", type(e).__name__)), str(getattr(e, "detail", "")))
            except Exception:
                pass

    def say(self, s: str = "") -> None:
        self.lines.append(s)
        self.p(s)

    # ───────── 단계 ─────────
    def open_ports(self) -> bool:
        opened = []
        for pr in self.probes.values():
            try:
                BD.open_link(pr)
                opened.append(pr)
            except TransportError as e:
                self.say(f"[중지] {pr.name} {pr.endpoint} 에 연결할 수 없음: {e}")
                self.say("        v1 프로그램이 켜져 있거나 주소·포트가 다를 수 있습니다. 다시 시도하지 않고 끝냅니다.")
                for o in opened:
                    o.outer.close()
                return False
        self.say("연결: " + ", ".join(f"{p.name}={p.endpoint}" for p in self.probes.values())
                 + "".join(f" · {k} {why}" for k, why in self.excluded.items()))
        return True

    def start_workers(self) -> Dict[str, bool]:
        for pr in self.probes.values():
            pr.worker.start()
        self.started = True
        self.clock.wait_until(self.clock.monotonic() + 8.0, lambda: all(p.worker.ready for p in self.probes.values()))
        return {k: p.worker.ready for k, p in self.probes.items()}

    def _read_once(self, pr: BD.Probe, fn, wait_s: float = 6.0):
        """(값, 상태). 무응답·실패면 값 None."""
        box: list = []
        pr.worker.submit(Request("read", fn, deadline=self.clock.monotonic() + wait_s - 0.5, name="bench",
                                 on_done=lambda r: (box.append(r), self.clock.notify())))
        self.clock.wait_until(self.clock.monotonic() + wait_s, lambda: bool(box))
        if box and box[0].status == Status.OK:
            return box[0].value, Status.OK
        return None, (box[0].reason or box[0].status) if box else "timeout"

    def safety_check(self, ready: Dict[str, bool]) -> Optional[str]:
        """멈춰야 하면 이유, 아니면 None. 읽기만 한다."""
        plc = self.probes.get("plc")
        if plc is None or not ready.get("plc"):
            return "PLC 를 읽을 수 없음(연결 안 됨·제외) — 출력 상태를 확인할 수 없어 멈춥니다"
        v, st = self._read_once(plc, lambda s: plc.driver.source_read(s, 0))
        poll = plc.driver.last_poll
        if v is None or poll is None:
            return f"PLC 를 읽을 수 없음({st}) — 출력 상태를 확인할 수 없어 멈춥니다"
        self.first_poll = poll
        c, r = self.cfg, poll.regs
        dac = {i: r.get(PM.REG_MAP[f"DCV_WRITE_{i}"]) for i in range(4)}
        p1 = dc_scale(c, r.get(0, 0), r.get(1, 0))[0]
        p2 = dc_scale(c, r.get(10, 0), r.get(11, 0))[0]
        rf2 = rf_scale(c, r.get(8, 0), r.get(9, 0), 2)
        rf1 = rf_scale(c, r.get(2, 0), r.get(3, 0), 1)
        self.say(f"PLC: D/A D4~D7 = {[dac[i] for i in range(4)]} · DC1 {p1:.1f} W · DC2 {p2:.1f} W · "
                 f"RF(CH2) 순방향 {rf2['forward']} W · RF(PC) 순방향 {rf1['forward']} W")
        if c.get("CH2_SUPPORTS_HEATER") and poll.coils.get(640):
            return ("CH2 히터 RUN(M00400)이 켜져 있음 — bench 는 워치독 D114 를 쓰지 않아 30 s 뒤 래더가 WD_ERR 로 히터를 끕니다. "
                    "히터를 끈 뒤 다시 실행하세요")
        on = [f"D{4 + i}={dac[i]}" for i in range(4) if (dac[i] or 0) > 0]
        if on:
            return f"D/A 출력 설정이 남아 있음({', '.join(on)}) — 전원 출력 중으로 보입니다"
        if max(p1, p2) >= POWER_TRACE_W or max(rf1["forward"], rf2["forward"]) >= POWER_TRACE_W:
            return f"전원 출력이 측정됨(DC {max(p1, p2):.1f} W · RF {max(rf1['forward'], rf2['forward'])} W) — 공정 진행 중으로 보입니다"
        for n, a in PM.COIL_MAP.items():
            if poll.coils.get(a) and (n.startswith("DCV_SET_") or n in ("SW_RF_SELECT", "SW_POWER_SELECT") or n.endswith("_GAS_SW")):
                self.notes.append(f"[참고] {n}(코일 {a}) 켜짐 — 출력 없음(v1 정지 흔적일 수 있음)")
        for n in self.notes:
            self.say(n)
        for pr in self.probes.values():
            if not ready.get(pr.name):
                continue
            why = self._device_trace(pr)
            if why:
                return why
        return None

    def _device_trace(self, pr: BD.Probe) -> Optional[str]:
        from v2.ch12.drivers.mfc import parse_r60
        from v2.ch12.drivers.ig import parse_rdi
        from v2.ch12.drivers.rfpulse_poll import parse_status
        k = pr.kind
        if k == "mfc":
            line, _ = self._read_once(pr, BD.q_mfc(pr, "R60"))
            vals = parse_r60(line) or []
            flows = [pr.driver.hw_to_ui(i + 1, x) for i, x in enumerate(vals[:3])]
            if flows and max(flows) > FLOW_TRACE_SCCM:
                return f"{pr.name} 가스 유량이 있음({', '.join(f'{x:g}' for x in flows)} sccm) — 공정 진행 중으로 보입니다"
        elif k == "ig":
            line, _ = self._read_once(pr, BD.q_ig(pr))
            if parse_rdi(line)[1] == "ok":
                self.say(f"[참고] {pr.name} 점등 중({line!r}) — 끄지 않고 읽기만 합니다")
        elif k == "dcpulse":
            h, _ = self._read_once(pr, BD.q_dcp(pr, 0x90))
            if h and len(bytes.fromhex(h)) >= 2 and bytes.fromhex(h)[-1] & 1:
                return f"{pr.name} HV On(0x90 bit0) — 출력이 켜져 있습니다"
        elif k == "rfpulse":
            h, _ = self._read_once(pr, BD.q_rfp(pr, 162))
            st = parse_status(bytes.fromhex(h)) if h else None
            if st and st["on"]:
                return f"{pr.name} 출력 ON(162 byte1 bit5) — 출력이 켜져 있습니다"
        elif k == "tsp":
            v, _ = self._read_once(pr, BD.q_tsp(pr))
            if v in (3, 4, 5):
                return f"TSP 상태 {v}(RAMP·WAIT_SUBLIM·SUBLIM) — 승화 중입니다"
        return None

    # ───────── 설정 전부 읽기 ─────────
    def _inventory_dev(self, pr: BD.Probe) -> None:
        for q, fn, mean in BD.queries(pr):
            raw, st = self._read_once(pr, fn, wait_s=12.0)
            if raw is None:
                self.clock.wait_until(self.clock.monotonic() + NO_REPLY_PAUSE_S)
            self.inv.append(dict(device=pr.name, query=q, raw=None if raw is None else str(raw), meaning=mean,
                                 status=str(st)))

    def inventory(self, ready: Dict[str, bool]) -> None:
        ths = [threading.Thread(target=self._inventory_dev, args=(pr,), daemon=True)
               for pr in self.probes.values() if ready.get(pr.name) and pr.kind != "plc"]
        for t in ths:
            t.start()
        for t in ths:
            t.join(300.0)
        fp = getattr(self, "first_poll", None)
        if fp is not None:
            for n, a in list(PM.COIL_MAP.items()) + list(PM.P_MIRROR_MAP.items()):
                if a in fp.coils:
                    self.inv.append(dict(device="plc", query=f"FC1 {a}", raw=str(int(fp.coils[a])), meaning=n, status="ok"))
            for a in sorted(fp.regs):
                names = [n for n, x in PM.REG_MAP.items() if x == a]
                self.inv.append(dict(device="plc", query=f"FC3 D{a}", raw=str(fp.regs[a]),
                                     meaning=names[0] if names else ("히터 영역" if a >= 110 else ""), status="ok"))
        order = {k: i for i, k in enumerate(["plc"] + [p for p in self.probes if p != "plc"])}
        self.inv.sort(key=lambda i: order.get(i["device"], 99))
        (self.out / "inventory.json").write_text(json.dumps({"items": self.inv}, ensure_ascii=False, indent=1), encoding="utf-8")

    def print_inventory(self) -> None:
        self.say("---------------- 장비 설정(읽은 값 — 원문 그대로) ----------------")
        for dev in self.probes:
            items = [i for i in self.inv if i["device"] == dev]
            if not items:
                continue
            self.say(f"[{dev}] {len(items)}개")
            for i in items[:200]:
                raw = "(무응답)" if i["raw"] is None else repr(i["raw"])
                self.say(f"  {i['query']:<10} {raw:<34} {i['meaning']}")

    # ───────── 1초 수집 ─────────
    def run_ticks(self, seconds: int) -> None:
        blocks, sources, hooks = [], [], []
        for pr in self.probes.values():
            blocks += list(pr.blocks)
            names = tuple(b.name for b in pr.blocks)
            if pr.kind == "tsp":
                pr.driver.begin_use()
                sources.append(pr.driver.source())
            elif pr.kind == "stm100":
                pr.driver.start()
                sources.append(pr.driver.source())
            else:
                fn, dl = pr.source
                sources.append(Source(pr.name, pr.worker, fn, names, dl))
            if pr.kind == "plc":
                hooks.append(pr.driver.on_tick)
        bus = SnapshotBus(self.clock, blocks, sources, int(self.cfg["TICK_DEADLINE_MS"]))
        self.recorder = Recorder(self.out, bus.columns, retention_days=int(self.cfg["SNAPSHOT_RETENTION_DAYS"]))
        self.recorder.start()
        self.loop = TickLoop(self.clock, bus, [self.recorder.put_row, self.rows.append],
                             tick_hooks=[lambda t, h=h: h(t) for h in hooks])
        self.say(f"수집 시작 — {seconds} s (Ctrl+C 로 멈춤)")
        try:
            self.loop.run(seconds)
        except KeyboardInterrupt:
            self.say("Ctrl+C — 멈춤. 요약을 남깁니다")

    def stop(self) -> None:
        self.stop_mono = self.clock.monotonic()
        for pr in self.probes.values():
            if pr.kind == "stm100":
                pr.driver.stop()
        if self.started:
            for pr in self.probes.values():
                pr.worker.stop()
        for pr in self.probes.values():
            try:
                pr.outer.close()
            except Exception:
                pass
        if self.recorder is not None:
            self.recorder.stop()
            for day in sorted({r["ts"][:10].replace("-", "") for r in self.rows}):
                try:
                    self.recorder.export_csv(day)
                except Exception as e:
                    self.say(f"CSV 내보내기 실패 {day}: {e!r}")

    def _cut_at_stop(self, pr: BD.Probe) -> int:
        """멈추기 시작한 때 아직 응답을 기다리던 마지막 요청(종료 때 취소) 개수 — 무응답으로 세지 않는다."""
        rec = pr.rec
        if self.stop_mono is None or rec._t0 is None:
            return 0
        stop_rel = self.stop_mono - rec._t0
        n = 0
        for e in reversed(rec.exchanges):
            if e.rx or e.t_s < stop_rel - 3.5:
                break
            n += 1
        return n

    def _dump(self, pr: BD.Probe, n: int = 8) -> None:
        self.say(f"  녹화 앞부분 {pr.name}(최대 {n}개): 보낸 시각 · 보낸 것 → 받은 것 · 지연")
        for e in pr.rec.exchanges[:n]:
            if pr.kind in BD.TEXT_KINDS:
                tx, rx = repr(e.tx.decode("ascii", errors="replace")), repr(e.rx.decode("ascii", errors="replace"))
            else:
                tx, rx = e.tx.hex(" "), (e.rx.hex(" ") or "(없음)")
            self.say(f"    {e.t_s:8.3f}s  {tx}  →  {rx}  {e.delay_s * 1000:.1f} ms")

    # ───────── 요약 ─────────
    def summary(self, status: str) -> None:
        self.say("")
        self.say("================ 요약 ================")
        self.say(f"결과: {status}")
        self.say(f"기록 폴더: {self.out}")
        problem = []
        for k, why in self.excluded.items():
            self.say(f"응답 {k:12s}: {why}")
        for pr in self.probes.values():
            pr.rec._flush()
            ex = pr.rec.exchanges
            if self.out.exists():
                save_exchanges(self.out / f"rec_{pr.name}.jsonl", ex)
            d = [e.delay_s * 1000 for e in ex if e.rx]
            cut = self._cut_at_stop(pr)
            none = sum(1 for e in ex if not e.rx) - cut
            gaps = [(b.t_s - a.t_s) * 1000 for a, b in zip(ex, ex[1:])]
            if none > 0 or not pr.worker.ready:
                problem.append(pr)
            if d:
                self.say(f"응답 {pr.name:12s}: {len(d)}회  p50 {_pct(d, 50):.1f} ms  p99 {_pct(d, 99):.1f} ms  최대 {max(d):.1f} ms  "
                         f"무응답 {none}  간격 최소 {min(gaps, default=0):.0f}·p50 {_pct(gaps, 50) if gaps else 0:.0f} ms"
                         + (f"  (종료 때 끊김 {cut} — 무응답 아님)" if cut else ""))
            else:
                self.say(f"응답 {pr.name:12s}: 응답 없음(보낸 {len(ex)}회)")
        blocked = [e for e in self.events if type(e).__name__ == "GuardEvent"]
        self.say(f"가드가 막은 명령: {len(blocked)}건" + "".join(f"\n  {e.device} {e.detail}" for e in blocked[:20]))
        if self.loop is not None:
            self.say(f"tick: {self.loop.rows}행, 놓친 tick {self.loop.missed}, jitter 최대 {max(self.loop.jitters, default=0):.1f} ms")
            for pr in self.probes.values():
                for b in pr.blocks:
                    cnt = collections.Counter(r.get(f"{b.name}.q") for r in self.rows)
                    self.say(f"품질 {b.name:12s}: " + ", ".join(f"{q}={n}" for q, n in sorted(cnt.items(), key=str)))
        if self.inv:
            self.print_inventory()
        for n in self.notes:
            self.say(n)
        self.say("---------------- 연결 기록 ----------------")
        conn = [e for e in self.events if type(e).__name__ in ("WorkerEvent", "GuardEvent")]
        for e in conn[:40]:
            self.say(f"  {getattr(e, 'device', '?'):12s} {e.kind:<16} {getattr(e, 'detail', '')}")
        if len(conn) > 40:
            self.say(f"  … {len(conn) - 40}건 더(events.jsonl)")
        for pr in problem:
            self._dump(pr)
        self.say("---------------- 확인할 것(v2/ch12/spec/devices.UNVERIFIED) ----------------")
        from v2.ch12.spec import devices as DV
        for u in DV.UNVERIFIED:
            self.say(f"[미확인] {u.device}: {u.item} — 지금 {u.v1} · 확인 {u.how}")
        if self.out.exists():
            (self.out / "summary.txt").write_text("\n".join(self.lines) + "\n", encoding="utf-8")
        if self._events_f is not None:
            try:
                self._events_f.close()
            except Exception:
                pass


def run(seconds: int, out_root: Path, print_fn=print, cfg_over: Optional[Dict[str, Any]] = None, skip: tuple = (),
        pulse: Optional[Dict[str, str]] = None) -> int:
    print_fn(BANNER)
    b = BenchProbe(out_root, print_fn, cfg_over, skip, pulse)
    why = out_refused(b.out, b.cfg)
    if why:
        print_fn(f"[중지] 기록 폴더 거부 — {why}. bench 기록은 NAS·운영 폴더에 쓰지 않습니다(--out 으로 다른 곳)")
        return 4
    b.open_records()
    cnt = G.allowed_counts()
    b.say("읽기 전용 가드 켜짐(bench) — 허용 질의 수: " + " · ".join(
        f"{p.name} {'FC1·FC3' if p.kind == 'plc' else cnt[p.kind][0]}" for p in b.probes.values()))
    code, status = 0, "정상 종료"
    try:
        if not b.open_ports():
            code, status = 3, "연결 실패 — 장비에 아무것도 쓰지 않고 종료"
        else:
            ready = b.start_workers()
            not_ready = [k for k, v in ready.items() if not v]
            if not_ready:
                b.say("연결 안 된 장비(응답 없음): " + ", ".join(not_ready))
            why = b.safety_check(ready)
            if why:
                b.say(f"[중지] {why}")
                code, status = 2, "안전 확인에서 멈춤 — 장비에 아무것도 쓰지 않고 종료"
            else:
                b.say("안전 확인 통과 — 히터 RUN 꺼짐, 전원·가스·펄스·TSP 출력 없음")
                b.say("장비 설정 읽기 …")
                b.inventory(ready)
                b.run_ticks(seconds)
    except KeyboardInterrupt:
        status = "Ctrl+C 로 멈춤"
    finally:
        b.stop()
        b.summary(status)
    return code


def _kv(s: str):
    if "=" not in s:
        raise argparse.ArgumentTypeError(f"KEY=VALUE 형식: {s}")
    k, v = s.split("=", 1)
    try:
        return k, json.loads(v)
    except ValueError:
        return k, v


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="챔버12 v2 bench 실측(읽기 전용 — RUN_MODE=bench 고정)")
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--seconds", type=int, default=None, help="--minutes 대신 초 단위")
    ap.add_argument("--out", default=str(Path.home() / "Desktop"), help="기록 폴더를 만들 곳(기본: 바탕화면)")
    ap.add_argument("--skip", action="append", default=[], choices=[p for p in BD.PROBES if not p.endswith(".pulse")],
                    help="이 장비는 연결하지 않는다(여러 번 가능)")
    ap.add_argument("--ch1-pulse", default="none", choices=("none", "dcpulse", "rfpulse"), help="CH1 4007 에 꽂힌 펄스 장비")
    ap.add_argument("--ch2-pulse", default="none", choices=("none", "dcpulse", "rfpulse"), help="CH2 4005 에 꽂힌 펄스 장비")
    ap.add_argument("--set", action="append", default=[], type=_kv, metavar="KEY=VALUE", help="설정 값 바꾸기(RUN_MODE 제외)")
    a = ap.parse_args(argv)
    over = dict(a.set)
    if "RUN_MODE" in over:
        ap.error("RUN_MODE 는 bench 고정 — 바꿀 수 없음")
    seconds = a.seconds if a.seconds is not None else int(round(a.minutes * 60))
    return run(seconds, Path(a.out), cfg_over=over, skip=tuple(a.skip), pulse={"ch1": a.ch1_pulse, "ch2": a.ch2_pulse})


if __name__ == "__main__":
    sys.exit(main())
