"""챔버12 재시작 뒤 이어가기/안전 정지 판단과 의도적 종료(사용자 지시 3장 (6) — 합의된 규칙 그대로). 저널은 journal.py.

다시 켜졌을 때(ResumeManager — app.start_journal 이 조립 뒤·worker 시작 전에 prepare, 시작 뒤 별도 스레드에서 run)
- prepare: 옛 저널을 읽는다. '공정 중'(active)인 부분은 hold(판단 전 덮어쓰지 않음)하고 그 파일의 재시작 횟수를 바로 +1 해 둔다
  (판단 도중 또 죽으면 다음 시작은 '첫 재시작'이 아니게). CH2 공정이 히터를 쥐고 있었으면 히터 엔진 adopt_journal(첫 폴링의 잔존
  RUN 정리를 하지 않음 — 이어가지 않으면 정지 순서가 끈다). 처리한 명령 id 기억(웹·로봇)을 되살린다(journal.restore_commands).
  그 챔버 엔진은 판단이 끝날 때까지 새 시작을 받지 않는다(_restart_pending → start_guard RESTART_PENDING).
- run(1초 행 RESUME_ROWS 개를 다시 읽은 뒤): 챔버마다 아래가 모두 맞을 때만 이어간다(resume_from), 하나라도 아니면 STOP 과 같은 정지
  순서로 끄고 run_end error PROGRAM_RESTART(stop_from_journal).
  ① gap — 꺼져 있던 시간 ≤ RESUME_MAX_GAP_S(기본 30 s). 재부팅(벽시계−단조 시계가 다름 — BSOD·정전)은 늘 아님.
  ② first_restart — 이 공정에서 첫 재시작(저널 restarts == 0).
  ③ 장비 상태가 저널의 기대 상태와 일치 — 블록 종류별 확인 목록(blocks.RESUME_CHECKS, 저널 state.resume_checks):
     shutter·gas·power_select = 저널 때 1초 행의 PLC 코일과 같음(gas 는 + FLOW ON 이던 가스의 잰 유량이 설정 ±10 %,
     최소 1 sccm), pressure = 지금 압력이 저널 때 압력과 SP 목표 사이(압력 제어가
     목표로 가는 중) 또는 둘 중 하나의 MFC 허용 범위 안(mfc_ops.within — D79 와 같은 폭; 압력 제어 전·압력 도달 대기 스텝 중이면 확인 없음), power_output = 켜져 있던 전원이 켜져 있고 목표 ±10 %(최소 1 W — D79 와 같은 폭), 끈 것은 꺼짐,
     heater_run·watchdog = 히터 엔진 resume_checks(RUN·목표·WD_ERR·FAULT), thickness = 두께 엔진 resume_checks.
  ④ resumable — 지금 블록이 상태 유지 블록(블록 정의 resumable ∧ 지금 스텝 holding — 대기·가스 안정화·프리스퍼터·메인 증착·
     히터 유지/도달 대기·TEST·delay 행). 진공·벤트·게이트·척·핀(로봇 — 블록 아님)·장비 설정 쓰기 도중·정지 순서·STOP 요청 중은 아님.
     의도적 종료 표시가 있으면 아님.
  ⑤ enabled — RESUME_ENABLED.
  이어갈 때 엔진이: 설정값 다시 쓰기·되읽기 확인(실패면 안전 정지) → 이벤트 source process·kind=resumed {run_id, gap_s, name,
  block, checks} '복구됨(공백 N s)'(공정 txt·웹 — 카드 1장은 포털) → 남은 만큼 이어감. 1초 기록의 빈 구간은 채우지 않는다.
  이어가지 않으면 따로 이벤트 없이 run_end error PROGRAM_RESTART 하나(포털 카드 1장 — '❌ 공정 실패 이유' chat 은 내지 않는다).
  플라즈마 클리닝·TSP 는 늘 안전 정지(PC: RF 0 W → SET OFF → 가스 OFF → MFC VALVE OPEN, TSP: TSP OFF → 연결 닫기 → CH1 IG OFF —
  CH1 공정을 이어가면 IG 는 둠) — 상태 유지 블록 목록(④)에 없다(결정 후보).
- 드라이버 넘겨받기(adopt_outputs): 새 프로그램의 드라이버는 출력을 켠 적이 없다고 알아서 정지 순서의 끄기를 건너뛴다 — 저널·다시
  읽은 값으로 DC·RF 연속의 SET 래치·마지막 전송값, 펄스 '켜졌을 수 있음'(포트도 다시 잡음)을 먼저 넣는다(드라이버 API 없음 — D88 방식).
- 공정이 없을 때 켜졌는데 파워가 남아 있으면(1초 행 PLC dcv_set_*·dcv_write_*) 파워만 끈다 — 4단계 안전 규칙(system.safety)에
  power_off_only 가 있으면 그것을, 첫 읽기 정리(first_read — 챔버K S51)가 있으면 그쪽에 맡기고, 없으면 이 모듈의 power_off_only.
  펄스 전원은 포트를 잡아야 읽을 수 있어 '공정 없음' 시작에서는 보지 않는다(공정 중이었으면 정지 순서가 끈다).

의도적 종료(intentional_exit — 콘솔 닫기·Ctrl+C·로그오프·윈도우 종료·SIGTERM)
  저널에 먼저 '의도적 종료'를 적고(이어가지 않게) → 공정 중이면 공정을 멈추고(run_end error PROGRAM_EXIT — 챔버: program_exit,
  PC·TSP: abort PROGRAM_EXIT) → 챔버K 결정 4 처럼 파워 출력만 끈다(power_off_only: DC·DC2·RF·PC RF 0 W·SET OFF, DC Pulse·RF Pulse
  확인형 OFF, 히터 RUN=0 — PLC 릴레이·밸브·MFC 는 그대로) → 저널 마지막 쓰기. 확인을 기다리지 않는 명령을 먼저 보내고
  FORCE_DEADLINE_S 안에 끝낸다(창 닫기 한도 약 5초).
"""
from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from v2.ch12 import journal as J
from v2.ch12.drivers import mfc_ops

Check = Tuple[str, bool, str]                 # (확인 id, 맞음, 문구)

RESUME_ROWS = 3                               # 판단 전에 다시 읽을 1초 행 수
RESUME_WAIT_S = 20.0                          # 장비 읽기를 기다리는 상한(펄스 포트 연결 포함)
RESUME_FRESH_S = 5.0                          # 판단에 쓰는 값의 나이 상한(번갈아 읽는 값 포함)
POWER_TOL_REL, POWER_TOL_ABS = 0.10, 1.0      # 파워 출력 확인 폭(D79 PC FWD ±10 % 와 같게, 최소 1 W)
FORCE_DEADLINE_S = 3.5                        # 의도적 종료 — 파워 끄기 결과를 기다리는 상한(챔버K FORCE_DEADLINE_S)
CHAMBER_PARTS = ("ch1", "ch2")
DCV_NAMES = {0: "CH2 DC1", 1: "PC RF", 2: "CH2 RF", 3: "CH2 DC2"}


# ───────────── 확인 목록 ─────────────
def norm_checks(items: Iterable[Any]) -> List[Check]:
    """엔진마다 모양이 다른 확인 결과([(id, ok, 문구)] · [{name, ok, detail}])를 하나로."""
    out: List[Check] = []
    for x in items or ():
        if isinstance(x, dict):
            out.append((str(x.get("name") or x.get("id")), bool(x.get("ok")), str(x.get("detail") or "")))
        else:
            out.append((str(x[0]), bool(x[1]), str(x[2]) if len(x) > 2 else ""))
    return out


def _tol_ok(measured: Any, target: Any) -> bool:
    try:
        m, t = float(measured), float(target)
    except (TypeError, ValueError):
        return False
    return abs(m - t) <= max(POWER_TOL_ABS, POWER_TOL_REL * abs(t))


def bool_check(cid: str, expect: Dict[str, Any], now: Callable[[str], Any]) -> Check:
    """PLC 코일(장비 값)이 저널 때와 같은가. MFC 의 *_on·*_set_sccm 은 드라이버 표시(장비 값 아님)라 여기서 보지 않는다."""
    bad = []
    for col, v in sorted(expect.items()):
        if not isinstance(v, bool) or not col.startswith("plc."):
            continue
        got = now(col)
        if got is None or bool(got) != v:
            bad.append(f"{col} 기대 {v} · 읽음 {got}")
    return (cid, not bad, "저널과 같음" if not bad else "; ".join(bad)[:400])


def flow_checks(ch: str, expect: Dict[str, Any], now: Callable[[str], Any]) -> List[Check]:
    """저널 때 FLOW ON 이던 가스 — 지금 잰 유량(R60)이 그때 설정 유량 ±10 %(최소 1 sccm) 안."""
    out: List[Check] = []
    for g in ("ar", "o2", "n2"):
        b = f"{ch}.mfc.{g}_"
        if not expect.get(b + "on"):
            continue
        tgt, got = expect.get(b + "set_sccm"), now(b + "sccm")
        ok = got is not None and tgt is not None and abs(float(got) - float(tgt)) <= max(1.0, POWER_TOL_REL * float(tgt))
        out.append((f"gas.{g}", bool(ok), f"{g.upper()} 유량 {got} sccm (설정 {tgt} sccm ±{POWER_TOL_REL:.0%}, 최소 1 sccm)"))
    return out


def power_checks(ch: str, expect: Dict[str, Any], now: Callable[[str], Any]) -> List[Check]:
    out: List[Check] = []
    groups = []
    if ch == "ch2":
        groups += [("DC1", "ch2.dc.dc1_enable", "ch2.dc.dc1_p", "ch2.dc.dc1_target_w"),
                   ("DC2", "ch2.dc.dc2_enable", "ch2.dc.dc2_p", "ch2.dc.dc2_target_w"),
                   ("RF", "ch2.rf.enable", "ch2.rf.fwd_w", "ch2.rf.target_w")]
    groups += [("DC Pulse", f"{ch}.dcpulse.out_on", f"{ch}.dcpulse.p_w", f"{ch}.dcpulse.set_w"),
               ("RF Pulse", f"{ch}.rfpulse.out_on", f"{ch}.rfpulse.fwd_w", f"{ch}.rfpulse.set_w")]
    for lab, en, p, t in groups:
        want = expect.get(en)
        if want is None:
            continue
        if not want:
            if lab in ("DC1", "DC2", "RF"):
                got = now(en)
                out.append((f"power_output.{lab}", got is not None and not got, f"{lab} 꺼짐 기대 · 읽음 {got}"))
            continue
        tgt = expect.get(t)
        got_p = now(p)
        ok = _tol_ok(got_p, tgt)
        if lab != "DC Pulse":                              # DC Pulse 의 out_on 은 드라이버 표시(장비 값 아님) — 전력으로만
            ok = ok and bool(now(en))
        out.append((f"power_output.{lab}", ok, f"{lab} 켜짐 · {got_p} W (목표 {tgt} W ±{POWER_TOL_REL:.0%}, 최소 "
                                               f"{POWER_TOL_ABS:g} W)"))
    return out or [("power_output", True, "켜진 전원 없음")]


# ───────────── 드라이버 넘겨받기 ─────────────
def adopt_outputs(system, ch: str, st: Dict[str, Any], expect: Dict[str, Any]) -> List[str]:
    """새 드라이버에 '켜져 있던 출력'을 넣는다(정지 순서·이어가기가 실제로 끄고 다시 쓰게). 넘겨받은 것 목록."""
    out = st.get("outputs") or {}
    pw = (st.get("writes") or {}).get("power") or {}
    pe = expect.get("power_output") or {}
    done: List[str] = []
    if ch == "ch2":
        for u in (1, 2):
            d = system.dc.get(u)
            o = out.get(f"dc{u}") or {}
            if d is None:
                continue
            if o.get("enabled") or (o.get("last_sent") or 0) > 0 or pe.get(f"ch2.dc.dc{u}_enable") or f"dc{u}" in pw:
                d.enabled = True
                d.last_sent = float(o.get("last_sent") or pe.get(f"ch2.dc.dc{u}_set_w") or pw.get(f"dc{u}") or 1.0)
                d.target_power = float(o.get("target") or pw.get(f"dc{u}") or 0.0)
                done.append(f"DC{u}")
        rf = system.rf.get("ch2")
        o = out.get("rf") or {}
        if rf is not None and (o.get("enabled") or (o.get("last_sent") or 0) > 0 or pe.get("ch2.rf.enable") or "rf" in pw):
            rf.enabled = True
            rf.last_sent = float(o.get("last_sent") or pe.get("ch2.rf.write_w") or pw.get("rf") or 1.0)
            rf.current_step = rf.last_sent
            rf.target_power = float(o.get("target") or pw.get("rf") or 0.0)
            done.append("RF")
    for kind, key in (("dcpulse", "dcp"), ("rfpulse", "rfp")):
        o = out.get(key) or {}
        d = getattr(system, kind, {}).get(ch)
        if d is None or not (o.get("bound") or o.get("maybe_on") or key in pw):
            continue
        if d.worker is None:
            ok, msg, _ = system.shared.pulse.claim(kind, ch)
            if not ok:
                done.append(f"{kind} 포트 못 잡음({msg})")
                continue
        d._output_maybe_on = True                        # 확인형 OFF 가 되게
        d.polling = True
        if kind == "dcpulse":
            d._out_on = True                               # 1초 읽기(전력)가 돌게 — 실제 출력은 다시 읽은 전력으로 확인
        done.append(kind)
    return done


# ───────────── 파워만 끄기(챔버K 결정 4) ─────────────
def power_indices(system) -> List[int]:
    """PLC D/A 전원 채널 — CH2 DC1(0)·PC RF(1)·CH2 RF 연속(2)·CH2 DC2(3, 히터가 없을 때만 — D/A CH3 = 히터 출력)."""
    idx = [1]
    if system.dc.get(1) is not None:
        idx.append(0)
    if system.rf.get("ch2") is not None:
        idx.append(2)
    if system.dc.get(2) is not None and not bool(system.cfg.get("CH2_SUPPORTS_HEATER", False)):
        idx.append(3)
    return sorted(idx)


def residual_indices(system, now: Callable[[str], Any]) -> List[int]:
    """1초 행에서 남은 D/A 출력(SET 래치 켜짐 또는 WRITE > 0)."""
    out = []
    for k in power_indices(system):
        s, w = now(f"plc.dcv_set_{k}"), now(f"plc.dcv_write_{k}")
        if s or (isinstance(w, (int, float)) and w > 0):
            out.append(k)
    return out


def power_off_only(system, why: str = "PROGRAM_EXIT", wait_s: float = FORCE_DEADLINE_S, *, heater: bool = True,
                   plc_idx: Optional[Iterable[int]] = None, pulses: bool = True,
                   log: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """파워 출력만 끈다 — PLC D/A 0 W → SET OFF(safety 요청), 잡혀 있는 DC Pulse·RF Pulse 확인형 OFF, 히터 RUN=0.
    PLC 릴레이(펌프·밸브·가스 차단밸브·셔터)·MFC 는 건드리지 않는다. 엔진은 건드리지 않는다(공정 중단은 부르는 쪽).
    의도적 종료는 4단계 안전 규칙(S32 safety.power_off_only)이 있으면 그것을 쓴다(intentional_exit) — 이 함수는 그 대체·
    재시작 정지를 되살리지 못한 챔버의 비상 끄기·시작 때 남은 파워(안전 규칙이 없을 때)용."""
    clk = system.clock
    t0 = clk.monotonic()
    done: Dict[str, bool] = {}
    sent: List[str] = []

    def cb(name):
        done[name] = False
        sent.append(name)

        def fin(*a):
            done[name] = True
            clk.notify()
        return fin
    for d in list(system.dc.values()) + [system.rf.get("ch2"), system.rf.get("pc")]:
        if d is not None:                                  # 제어 루프(보정·램프)가 다시 쓰지 않게
            d._gen += 1
            d.running = d.polling = False
            if hasattr(d, "ramping_down"):
                d.ramping_down = False
    for k in (power_indices(system) if plc_idx is None else list(plc_idx)):
        nm = DCV_NAMES.get(k, f"DCV{k}")
        system.plc.power_write(0.0, family="DCV", write_idx=k, kind="safety", source="SYS", on_done=cb(f"{nm} 0 W"))
        system.plc.power_enable(False, family="DCV", set_idx=k, kind="safety", source="SYS", on_done=cb(f"{nm} SET OFF"))
    if pulses:
        for ch in CHAMBER_PARTS:
            dp, rp = system.dcpulse.get(ch), system.rfpulse.get(ch)
            if dp is not None and dp.worker is not None:
                dp.output_off(on_done=cb(f"{ch.upper()} DC Pulse OFF"))
            if rp is not None and rp.worker is not None:
                rp.stop_process(on_done=cb(f"{ch.upper()} RF Pulse OFF"))
    h = J.heater_of(system) if heater else None
    if h is not None and hasattr(h, "shutdown"):
        h.shutdown(on_done=cb("히터 RUN=0"))
    t_sent = clk.monotonic() - t0
    clk.wait_until(t0 + float(wait_s), lambda: all(done.values()))
    res = {"why": why, "sent": sent, "done": [k for k, v in done.items() if v], "sent_s": round(t_sent, 3),
           "elapsed_s": round(clk.monotonic() - t0, 3)}
    if log is not None:
        log(f"파워만 끔({why}) — 보냄 {sent}, 끝남 {res['done']}, {res['elapsed_s']:.2f} s")
    return res


# ───────────── 재시작 판단 ─────────────
class _Closed(Exception):
    pass


class _Closer:
    """조립 정지(system.stop → 엔진 close) 때 저널 쓰기 스레드·재시작 판단을 끝낸다(엔진 이름 journal)."""

    def __init__(self, journal, manager) -> None:
        self.j, self.m = journal, manager

    def close(self) -> None:
        self.m.close()
        self.j.stop()


class ResumeManager:
    def __init__(self, system, journal: J.Journal, log: Optional[Callable[[str], None]] = None) -> None:
        self.sys, self.j, self.clock, self.cfg = system, journal, system.clock, system.cfg
        self.say = log or (lambda m: None)
        self.old: Dict[str, Dict[str, Any]] = {}
        self.active: Dict[str, dict] = {}
        self.decisions: Dict[str, Dict[str, Any]] = {}
        self.residual: Optional[Dict[str, Any]] = None
        self.rows = 0
        self.done = False
        self.closed = False
        self.error: Optional[str] = None
        self._thread = None

    # ── 시작 전(조립 뒤·worker 시작 전) ──
    def prepare(self) -> "ResumeManager":
        self.old = J.load(self.j.dir)
        sh = self.old.get("shared", {}).get("doc") or {}
        self.j.restore_commands(sh.get("commands"))
        for part in ("ch1", "ch2", "pc", "tsp"):
            doc = self.old.get(part, {}).get("doc")
            if not doc or not doc.get("active"):
                continue
            n = int(doc.get("restarts") or 0) + 1
            self.active[part] = doc
            self.j.hold([part])
            self.j.set_restarts(part, doc.get("run_id"), n)
            try:
                self.j.write_part(part, dict(doc, restarts=n, resume_pending=True))
            except OSError as e:
                self.say(f"저널 재시작 횟수 쓰기 실패({part}): {e!r}")
            eng = self.sys.engine(J.ENGINE_OF[part])
            if eng is not None and part in CHAMBER_PARTS:
                eng._restart_pending = True
        h = J.heater_of(self.sys)
        hs = sh.get("heater")
        if h is not None and hs and "ch2" in self.active and hs.get("owner") == "process" and hasattr(h, "adopt_journal"):
            h.adopt_journal(hs)
        self.sys.row_listeners.append(self._on_row)
        return self

    def start(self):
        self._thread = self.clock.start_thread(self.run, "resume")
        return self._thread

    def _on_row(self, row) -> None:
        self.rows += 1
        self.clock.notify()

    def _now_fn(self, t_from: float) -> Callable[[str], Any]:
        cache, clk = self.j.cache, self.clock

        def now(col: str) -> Any:
            x = cache.v.get(col)
            if x is None or x[1] < t_from or clk.monotonic() - x[1] > RESUME_FRESH_S:
                return None
            return x[0]
        return now

    def close(self) -> None:
        """조립 정지 — 판단을 기다리던 중이면 그만둔다(공정이 있던 부분은 다음 시작이 다시 판단 — 저널을 덮어쓰지 않았다)."""
        self.closed = True
        self.clock.notify()

    def _wait_rows(self, n: int, extra: Callable[[], bool] = lambda: True) -> bool:
        goal = self.rows + n
        self.clock.wait_until(self.clock.monotonic() + RESUME_WAIT_S,
                              lambda: self.closed or (self.sys.started and self.rows >= goal and extra()))
        if self.closed:
            raise _Closed()
        return True

    # ── 판단·실행(별도 스레드) ──
    def run(self) -> None:
        try:
            self._run()
        except _Closed:
            self.say("조립 정지 — 재시작 판단 그만둠")
        except Exception as e:                              # 판단 오류 — 공정이 있던 챔버는 안전 정지 쪽으로
            self.error = repr(e)
            self.say(f"재시작 판단 오류: {e!r}")
            for part in list(self.active):
                if part not in self.decisions:
                    self._fallback(part, f"판단 오류: {e!r}")
        finally:
            for part in list(self.active):
                if part in self.decisions:                 # 판단하지 못한 부분은 옛 저널 그대로(다음 시작이 다시 판단)
                    self.j.release(part)
                eng = self.sys.engine(J.ENGINE_OF[part])
                if eng is not None:
                    eng._restart_pending = False
            self.sys.row_listeners[:] = [f for f in self.sys.row_listeners if f != self._on_row]
            self.done = True
            self.clock.notify()

    def _run(self) -> None:
        self._wait_rows(RESUME_ROWS)
        adopted: Dict[str, List[str]] = {}
        for ch in CHAMBER_PARTS:
            doc = self.active.get(ch)
            if doc and doc.get("state"):
                adopted[ch] = adopt_outputs(self.sys, ch, doc["state"], doc.get("expect") or {})
        t_from = self.clock.monotonic()
        pulses = [getattr(self.sys, k).get(ch) for ch in adopted for k in ("dcpulse", "rfpulse") if k in adopted[ch]]
        self._wait_rows(RESUME_ROWS, lambda: all(d.worker is not None and d.worker.connected for d in pulses))
        now = self._now_fn(t_from)
        for ch in CHAMBER_PARTS:
            if ch in self.active:
                self._chamber(ch, self.active[ch], now, adopted.get(ch, []))
        if "pc" in self.active:
            self._stop_pc(self.active["pc"])
        if "tsp" in self.active:
            self._stop_tsp(self.active["tsp"])
        if not self.active:
            self._residual(now)

    def conditions(self, part: str, doc: dict) -> Tuple[float, List[Check]]:
        """①②④⑤ — (공백, 확인 목록)."""
        gap, reboot = J.gap_of(doc, self.clock)
        mx = float(self.cfg.get("RESUME_MAX_GAP_S", 30.0))
        st = doc.get("state") or {}
        ex = doc.get("exit") or {}
        out: List[Check] = [
            ("gap", (not reboot) and gap <= mx, f"꺼져 있던 시간 {gap:.1f} s (한도 {mx:g} s)" + (" — 재부팅" if reboot else "")),
            ("first_restart", int(doc.get("restarts") or 0) == 0, f"이 공정 재시작 {int(doc.get('restarts') or 0) + 1}번째"),
            ("resumable", bool(st.get("resumable")) and not ex.get("intentional"),
             f"블록 {st.get('block_no')} {st.get('block_type')} · 스텝 {st.get('step_action')}"
             f"{' holding' if st.get('step_holding') else ''}" + (" · 정지 순서 중" if st.get("shutdown") else "")
             + (" · 의도적 종료 표시" if ex.get("intentional") else "")),
            ("enabled", bool(self.cfg.get("RESUME_ENABLED", True)), "RESUME_ENABLED"),
        ]
        return gap, out

    def device_checks(self, ch: str, doc: dict, now: Callable[[str], Any]) -> List[Check]:
        """③ — 블록 종류별 확인 목록(저널 state.resume_checks)."""
        st, exp = doc.get("state") or {}, doc.get("expect") or {}
        eng = self.sys.engine(J.ENGINE_OF[ch])
        out: List[Check] = []
        ids = list(st.get("resume_checks") or [])
        for cid in ids:
            if cid in ("shutter", "gas", "power_select"):
                out.append(bool_check(cid, exp.get(cid) or {}, now))
                if cid == "gas":
                    out += flow_checks(ch, exp.get("gas") or {}, now)
            elif cid == "pressure":
                sp = (st.get("writes") or {}).get("sp_target")
                pm = now(f"{ch}.mfc.pressure_mtorr")
                if sp is None:
                    out.append(("pressure", True, "압력 제어 전 — 확인 없음"))
                elif str(st.get("step_message") or "").startswith("압력 도달 대기"):
                    out.append(("pressure", True, f"압력 도달 대기 중(지금 {pm} mTorr) — 이어 가는 대기 스텝이 도달을 판정"))
                else:
                    was = (exp.get("pressure") or {}).get(f"{ch}.mfc.pressure_mtorr")
                    ref = float(was) if was is not None else float(sp)
                    m = self.sys.mfc[ch]
                    lo, hi = min(ref, float(sp)), max(ref, float(sp))
                    ok = pm is not None and (lo <= float(pm) <= hi or mfc_ops.within(m, ref, float(pm))
                                             or mfc_ops.within(m, float(sp), float(pm)))
                    out.append(("pressure", bool(ok), f"압력 {pm} mTorr · 저널 {was} mTorr(목표 {sp}, MFC 허용 범위)"))
            elif cid == "power_output":
                out += power_checks(ch, exp.get("power_output") or {}, now)
            elif cid == "thickness":
                th = getattr(eng, "thickness", None)
                if th is None or not doc.get("thickness"):
                    out.append(("thickness", True, "두께 판정 없음"))
                else:
                    out += norm_checks(th.resume_checks(doc["thickness"]))
        if "heater_run" in ids or "watchdog" in ids:
            h = getattr(eng, "heater", None)
            hs = (self.old.get("shared", {}).get("doc") or {}).get("heater")
            if h is None or hs is None:
                out.append(("heater_run", True, "히터 없음"))
            else:
                out += norm_checks(h.resume_checks(hs))
        return out

    def _chamber(self, ch: str, doc: dict, now: Callable[[str], Any], adopted: List[str]) -> None:
        eng = self.sys.engine(J.ENGINE_OF[ch])
        st = doc.get("state") or {}
        gap, checks = self.conditions(ch, doc)
        if all(c[1] for c in checks):
            checks += self.device_checks(ch, doc, now)
        ok = all(c[1] for c in checks)
        sh = self.old.get("shared", {}).get("doc") or {}
        plan = {"reasons": checks, "expect": doc.get("expect"), "heater": sh.get("heater"), "thickness": doc.get("thickness"),
                "adopted": adopted}
        dec = {"part": ch, "run_id": doc.get("run_id"), "gap_s": gap, "checks": checks, "resume": ok, "adopted": adopted}
        self.decisions[ch] = dec
        if eng is None:
            dec.update(resume=False, result="no_engine")
            self._fallback(ch, "엔진 없음")
            return
        eng._restart_pending = False
        gap_now, _ = J.gap_of(doc, self.clock)
        why = eng.resume_from(st, gap_now, plan) if ok else None
        if ok and why is None:
            dec["result"] = "resumed"
            self.say(f"{ch} 이어감 — 공백 {gap_now:.1f} s")
            return
        if why:
            plan["reasons"] = checks + [("restore", False, f"{why}: {getattr(eng, 'reject_detail', '')}")]
            dec["checks"] = plan["reasons"]
            dec["resume"] = False
        try:
            why2 = eng.stop_from_journal(st, gap_now, "PROGRAM_RESTART", plan)
        except Exception as e:
            why2 = repr(e)
        dec["result"] = "stopped" if why2 is None else "fallback"
        self.say(f"{ch} 이어가지 않음 — " + "; ".join(f"{c[0]}: {c[2]}" for c in plan["reasons"] if not c[1]))
        if why2 is not None:
            self._fallback(ch, f"정지 순서를 되살리지 못함: {why2}")

    def _fallback(self, part: str, why: str) -> None:
        """엔진으로 정지 순서를 되살리지 못함 — 파워만 끄고 run_end error PROGRAM_RESTART 를 직접 낸다."""
        doc = self.active.get(part) or {}
        self.decisions.setdefault(part, {"part": part, "run_id": doc.get("run_id"), "resume": False})["result"] = "fallback"
        power_off_only(self.sys, "PROGRAM_RESTART", heater=(part == "ch2"), log=self.say)
        self._emit_end(part, doc.get("run_id"), f"재시작 — {why}")

    def _emit_end(self, part: str, run_id: Optional[str], text: str, gap: Optional[float] = None) -> None:
        hub = getattr(self.sys, "hub", None)
        if hub is not None and run_id:
            hub._open_runs[part] = run_id
        self.sys.emit(f"engine:{part}", "run_end", text, level="error", code="PROGRAM_RESTART",
                      event={"run_id": run_id, "result": "error", "reason": "PROGRAM_RESTART",
                             "gap_s": None if gap is None else round(gap, 1)})

    # ── 플라즈마 클리닝·TSP — 늘 안전 정지 ──
    def _stop_pc(self, doc: dict) -> None:
        st = doc.get("state") or {}
        gap, checks = self.conditions("pc", doc)
        self.decisions["pc"] = {"part": "pc", "run_id": doc.get("run_id"), "gap_s": gap, "resume": False,
                                "checks": checks + [("pc", False, "플라즈마 클리닝은 이어가지 않음(상태 유지 블록 아님)")],
                                "result": "stopped"}
        eng = self.sys.engine("pc")
        if eng is None:
            return self._fallback("pc", "PC 엔진 없음")
        try:
            ch = 2 if int(st.get("ch") or 1) == 2 else 1
            eng.set_selected_ch(ch)
            eng.log("MAIN", f"[RESTART] 재시작(공백 {gap:.0f} s) — 이어가지 않고 안전 정지(PROGRAM_RESTART)")
            eng.force_rf_zero_then_set_off()
            eng.force_finalize_rf_stop()
            gi = st.get("gas_idx")
            if gi and eng.dev.mfc_gas is not None:
                eng.dev.mfc_gas.gas_select(int(gi), owner=eng.mfc_owner())
                eng.shutdown_rest_devices()
        except Exception as e:
            self.say(f"PC 재시작 정지 오류: {e!r}")
            power_off_only(self.sys, "PROGRAM_RESTART", heater=False, pulses=False, plc_idx=[1], log=self.say)
        self._emit_end("pc", doc.get("run_id"), f"재시작(공백 {gap:.0f} s) — 플라즈마 클리닝 안전 정지", gap)

    def _stop_tsp(self, doc: dict) -> None:
        gap, checks = self.conditions("tsp", doc)
        self.decisions["tsp"] = {"part": "tsp", "run_id": doc.get("run_id"), "gap_s": gap, "resume": False,
                                 "checks": checks + [("tsp", False, "TSP 는 이어가지 않음(상태 유지 블록 아님)")],
                                 "result": "stopped"}
        tsp = getattr(self.sys, "tsp", None)

        def call(submit, t=10.0):
            box: List[Any] = []
            submit(lambda *a: (box.append(a), self.clock.notify()))
            self.clock.wait_until(self.clock.monotonic() + t, lambda: bool(box))
            return box[0] if box else None
        if tsp is not None:
            try:
                tsp.begin_use()
                call(lambda cb: tsp.off(on_done=cb))
                tsp.end_use(close=False)
                call(lambda cb: tsp.close(on_done=cb))
            except Exception as e:
                self.say(f"TSP 재시작 정지 오류: {e!r}")
        if (self.decisions.get("ch1") or {}).get("result") != "resumed":
            ig = self.sys.ig.get("ch1")
            if ig is not None:
                call(lambda cb: ig.ensure_off(on_done=cb))
        self._emit_end("tsp", doc.get("run_id"), f"재시작(공백 {gap:.0f} s) — TSP 안전 정지(TSP OFF·IG OFF)", gap)

    # ── 공정이 없을 때 남은 파워 ──
    def _residual(self, now: Callable[[str], Any]) -> None:
        sf = getattr(self.sys, "safety", None)
        if sf is not None and (hasattr(sf, "power_off_only") or hasattr(sf, "first_read")):
            if hasattr(sf, "first_read") and not hasattr(sf, "power_off_only"):
                return                                     # 안전 규칙의 첫 읽기 정리(S51 류)가 한다 — 겹치지 않게
        idx = residual_indices(self.sys, now)
        if idx:
            self.say(f"공정 없음 · 남은 파워 {[DCV_NAMES[k] for k in idx]} — 파워만 끔")
            self.residual = power_off_only(self.sys, "RESIDUAL_POWER", heater=False, plc_idx=idx, pulses=False, log=self.say)
            self.sys.emit("resume", "residual_power_off", f"시작 때 남은 파워 출력 {[DCV_NAMES[k] for k in idx]} — 파워만 끔",
                          level="warn", code="RESIDUAL_POWER_OFF", event={"sent": self.residual.get("sent")})


# ───────────── 조립·의도적 종료 ─────────────
def attach(system, directory=None, log: Optional[Callable[[str], None]] = None) -> Tuple[J.Journal, ResumeManager]:
    """저널 + 재시작 판단을 조립에 붙인다(worker 시작 전에 — prepare 가 옛 저널을 먼저 읽는다). 시작 뒤 manager.start()."""
    j = J.Journal(system, directory, on_error=(lambda m: log(f"저널: {m}")) if log else None)
    m = ResumeManager(system, j, log=log).prepare()
    j.start()
    system.journal, system.resumer = j, m
    system.register_engine("journal", _Closer(j, m))
    return j, m


_EXIT_LOCK = threading.Lock()


def intentional_exit(system, journal: Optional[J.Journal], why: str, wait_s: float = FORCE_DEADLINE_S,
                     log: Optional[Callable[[str], None]] = None) -> Optional[Dict[str, Any]]:
    """의도적 종료 — 저널 표시 먼저 → 공정 중단(PROGRAM_EXIT) → 파워만 끔 → 저널 마지막 쓰기. 두 번째 부름은 None."""
    if not _EXIT_LOCK.acquire(blocking=False):
        return None
    try:
        if journal is not None and journal.exit is not None:
            return None
        if journal is not None:
            journal.mark_exit(why)
        f = getattr(getattr(system, "safety", None), "power_off_only", None)
        if callable(f):                                    # 4단계 S32 — 공정 붙잡기(PROGRAM_EXIT)·파워만 끄기를 한 곳에서
            res = dict(f(why, wait_s) or {}, by="safety")
            if journal is not None:
                journal.write_final()
            if log is not None:
                log(f"의도적 종료({why}) — S32 {res.get('sent')} · 끝남 {res.get('done')}")
            return res
        was: List[str] = []
        for ch in CHAMBER_PARTS:
            e = system.engine(J.ENGINE_OF[ch])
            if e is not None and getattr(e, "busy", False) and e.program_exit():
                was.append(ch)
        for name in ("pc", "tsp"):
            e = system.engine(name)
            if e is not None and getattr(e, "busy", False) and hasattr(e, "abort") and e.abort("PROGRAM_EXIT"):
                was.append(name)
        res = power_off_only(system, why, wait_s, log=log)
        res["process_was_running"] = was
        if journal is not None:
            journal.write_final()
        return res
    finally:
        _EXIT_LOCK.release()
