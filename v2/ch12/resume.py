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
  ③ 장비의 켜짐/꺼짐 상태가 저널의 기대 상태와 같음(사용자 결정 2026-11-02 D79·D93 — 값의 허용 폭은 보지 않는다: 파워·유량·압력은
     맞춰지는 중일 수 있다): 파워 출력 ON/OFF·셔터·가스 밸브 코일·Power Select 릴레이·히터 RUN·인터락·장비 오류 없음(히터 워치독·
     두께 모니터 응답). 목표값은 저널대로 다시 쓰고(쓰기→되읽기 확인) 그 뒤는 블록의 원래 로직(도달 대기·이탈 감시)이 판단한다.
  ④ 이어가기 방식(사용자 결정 2026-11-02 D94 — CH1·CH2·플라즈마 클리닝·TSP 같은 규칙, 엔진 state_for_journal 의 resume_mode):
     stop_sequence = 정지 순서 도중이었다 → 정지 순서를 처음부터 끝까지(STOP 과 같음, 사유 PROGRAM_RESTART) ·
     hold = 기다리는 중(대기·가스 안정화·메인 증착·히터 유지/도달 대기·TEST·delay 행·클리닝 시간·TSP 사이클 간격) → 남은 만큼 이어감 ·
     rewrite = 장비 설정 쓰기 도중 → 그 블록(단계)의 설정을 처음부터 다시 씀(③ 은 그 블록이 쓰는 장비를 뺀 나머지만) ·
     none = 시작 전 단계·의도적 종료 표시 → 이어가지 않음. 예외는 로봇 이동 명령(진공·벤트·게이트·척·핀) 뿐 — 자동으로 다시 하지 않고
     로봇에는 오류(D08 E205)로 끝낸다(robot_ops 는 저널에 없고 로봇 서버가 제어 프로그램 응답 없음 → E205).
  ⑤ enabled — RESUME_ENABLED_CH1·CH2·PC·TSP.
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

Check = Tuple[str, bool, str]                 # (확인 id, 맞음, 문구)

RESUME_ROWS = 3                               # 판단 전에 다시 읽을 1초 행 수
RESUME_WAIT_S = 20.0                          # 장비 읽기를 기다리는 상한(펄스 포트 연결 포함)
RESUME_FRESH_S = 5.0                          # 판단에 쓰는 값의 나이 상한(번갈아 읽는 값 포함)
FORCE_DEADLINE_S = 3.5                        # 의도적 종료 — 파워 끄기 결과를 기다리는 상한(챔버K FORCE_DEADLINE_S)
CHAMBER_PARTS = ("ch1", "ch2")
DCV_NAMES = {0: "CH2 DC1", 1: "PC RF", 2: "CH2 RF", 3: "CH2 DC2"}
ENABLED_KEY = {"ch1": "RESUME_ENABLED_CH1", "ch2": "RESUME_ENABLED_CH2", "pc": "RESUME_ENABLED_PC", "tsp": "RESUME_ENABLED_TSP"}
# 켜짐/꺼짐 확인 항목 전체 — 설정 쓰기 도중(rewrite)이면 그 블록이 쓰는 항목은 뺀다(블록이 처음부터 다시 쓴다)
ALL_CHECKS = ("power_output", "shutter", "gas", "power_select", "heater_run", "watchdog", "interlock", "thickness")
BLOCK_WRITES: Dict[str, tuple] = {
    "heater_set": ("heater_run",), "gas_prep": ("gas", "interlock"), "gas_intro": ("gas", "interlock"),
    "pressure_ctrl": ("gas", "interlock"), "pressure_boost": ("gas", "interlock"), "pressure_set": (),
    "shutter": ("shutter",), "power_select": ("power_select",), "power_on": ("power_output", "power_select"),
    "pulse_set": ("power_output",), "main": ("power_output",), "thickness_zero": ("thickness",),
}   # end_stop 은 없다 — 끝 정지 도중 재시작은 설정 쓰기 도중이 아니라 정지 순서 이어가기(process_view._resume_mode, 11-09 C1)


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


def interlock_check(ch: str, expect: Dict[str, Any], now: Callable[[str], Any]) -> Check:
    """인터락·장비 오류 없음 — 켜져 있던 가스 밸브의 인터락 코일이 FALSE 로 바뀌지 않았고 PLC 를 읽을 수 있다."""
    n = 1 if ch == "ch1" else 2
    gas = expect or {}
    bad = []
    for col in (f"plc.main_{n}_gas_sw", f"plc.ar_{n}_gas_sw", f"plc.o2_{n}_gas_sw", f"plc.n2_{n}_gas_sw"):
        if gas.get(col) is True:
            il = col.replace("_gas_sw", "_gas_인터락")
            if now(il) is False:
                bad.append(f"{il} FALSE")
    if now(f"plc.main_shutter_{n}_sw") is None:
        bad.append("PLC 읽기 없음")
    return ("interlock", not bad, "인터락 이상·장비 오류 없음" if not bad else "; ".join(bad))


def power_checks(ch: str, expect: Dict[str, Any], now: Callable[[str], Any]) -> List[Check]:
    """파워 출력 켜짐/꺼짐(값은 보지 않는다 — 램프·점화 중일 수 있음). 켜져 있던 것은 켜짐(RF Pulse·DC1·DC2·RF 는 장비 enable,
    DC Pulse 는 드라이버 표시라 전력 > 0), 끈 연속 전원은 꺼짐."""
    out: List[Check] = []
    groups = []
    if ch == "ch2":
        groups += [("DC1", "ch2.dc.dc1_enable", "ch2.dc.dc1_p"), ("DC2", "ch2.dc.dc2_enable", "ch2.dc.dc2_p"),
                   ("RF", "ch2.rf.enable", "ch2.rf.fwd_w")]
    groups += [("DC Pulse", f"{ch}.dcpulse.out_on", f"{ch}.dcpulse.p_w"), ("RF Pulse", f"{ch}.rfpulse.out_on", f"{ch}.rfpulse.fwd_w")]
    for lab, en, p in groups:
        want = expect.get(en)
        if want is None:
            continue
        if not want:
            if lab in ("DC1", "DC2", "RF"):
                got = now(en)
                out.append((f"power_output.{lab}", got is not None and not got, f"{lab} 꺼짐 기대 · 읽음 {got}"))
            continue
        got_p = now(p)
        if lab == "DC Pulse":                              # out_on 은 드라이버 표시(장비 값 아님) — 전력으로만
            ok = got_p is not None and float(got_p) > 0.0
        else:
            ok = bool(now(en))
        out.append((f"power_output.{lab}", ok, f"{lab} 켜짐 기대 · 읽음 {'켜짐' if ok else '꺼짐/읽기 없음'} ({got_p} W)"))
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


def rf_adopt(rf, clock, power_w: float, last_sent: Optional[float], cb) -> None:
    """RF 연속(CH2 RF·플라즈마 클리닝 RF) — 램프를 다시 하지 않고 유지 상태로 넘겨받아 마지막 전송값(없으면 목표)을 다시 쓴다
    (검증 쓰기). cb(None=성공 | 사유)."""
    now = clock.monotonic()
    rf._gen += 1
    rf.enabled = rf.running = True
    rf.state, rf.target_power = "MAINTAINING", float(power_w)
    rf.ref_armed, rf.blind_start_ts, rf.blind_reach_ts = True, now, now
    rf.low_power_n = rf.maintain_count = 0
    rf.polling, rf._due = True, now
    rf.current_step = float(last_sent if last_sent is not None else power_w)
    rf.last_sent = None
    if not rf._send(rf.current_step, after=lambda ok: cb(None if ok else "RF_WRITE_FAILED")):
        cb(None)


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
        self.stop_all: Optional[str] = None            # 판단 중에 ALL STOP 이 왔다(safety.all_stop) — 사유(출처)
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
            if eng is not None:
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

    def request_stop_all(self, source: str = "web", by: Optional[str] = None) -> None:
        """ALL STOP(S30, D05)이 재시작 판단 중에 왔다 — 아직 판단하지 않은 부분은 이어가지 않고 정지 순서로, 이미 이어간 부분은 판단이
        끝나는 대로 STOP. (판단 중에는 엔진이 아직 돌지 않아 STOP 이 닿지 않으므로.)"""
        self.stop_all = f"{source}{'·' + by if by else ''}"
        self.clock.notify()

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
        for part in ("pc", "tsp"):
            if part in self.active:
                self._pc_tsp(part, self.active[part], now)
        if not self.active:
            self._residual(now)
        self._stop_resumed_if_all_stop()

    def _stop_resumed_if_all_stop(self) -> None:
        """판단 도중 ALL STOP 이 왔는데 이미 이어간 부분은 지금 STOP(엔진이 돌고 있다)."""
        if not self.stop_all:
            return
        src = self.stop_all
        for part, dec in list(self.decisions.items()):
            if dec.get("result") != "resumed":
                continue
            eng = self.sys.engine(J.ENGINE_OF[part])
            try:
                if part in CHAMBER_PARTS:
                    eng.stop(source="web", by=src, user=True)
                else:
                    eng.stop(source="web")
            except Exception as e:
                self.say(f"ALL STOP — {part} STOP 실패: {e!r}")

    def conditions(self, part: str, doc: dict) -> Tuple[float, List[Check]]:
        """①②④⑤ — (공백, 확인 목록). 네 부분(ch1·ch2·pc·tsp) 같은 규칙."""
        gap, reboot = J.gap_of(doc, self.clock)
        mx = float(self.cfg.get("RESUME_MAX_GAP_S", 30.0))
        st = doc.get("state") or {}
        ex = doc.get("exit") or {}
        mode = st.get("resume_mode")
        out: List[Check] = [
            ("gap", (not reboot) and gap <= mx, f"꺼져 있던 시간 {gap:.1f} s (한도 {mx:g} s)" + (" — 재부팅" if reboot else "")),
            ("first_restart", int(doc.get("restarts") or 0) == 0, f"이 공정 재시작 {int(doc.get('restarts') or 0) + 1}번째"),
        ]
        if mode == "stop_sequence":
            out.append(("stop_sequence", False, "정지 순서 도중이었음 — 정지 순서를 이어서 끝까지(처음부터 다시)"))
        out.append(("resumable", mode in ("hold", "rewrite") and not ex.get("intentional"),
                    f"방식 {mode or '-'} · 블록 {st.get('block_no')} {st.get('block_type')} · 스텝 {st.get('step_action')}"
                    + (" · 기다리는 중" if mode == "hold" else (" · 설정 쓰기 도중 — 블록 설정을 처음부터 다시" if mode == "rewrite" else ""))
                    + (" · 의도적 종료 표시" if ex.get("intentional") else "")))
        out.append(("enabled", bool(self.cfg.get(ENABLED_KEY[part], True)), ENABLED_KEY[part]))
        return gap, out

    def device_checks(self, ch: str, doc: dict, now: Callable[[str], Any]) -> List[Check]:
        """③ — 켜짐/꺼짐 확인(값 허용 폭 없음). 기다리는 중이면 블록 종류별 목록(저널 state.resume_checks) + 인터락,
        설정 쓰기 도중이면 그 블록이 쓰는 항목을 뺀 전체."""
        st, exp = doc.get("state") or {}, doc.get("expect") or {}
        eng = self.sys.engine(J.ENGINE_OF[ch])
        if st.get("resume_mode") == "rewrite":
            skip = set(BLOCK_WRITES.get(str(st.get("block_type")), ALL_CHECKS))
            ids = [c for c in ALL_CHECKS if c not in skip]
        else:
            ids = [c for c in (list(st.get("resume_checks") or []) + ["interlock"]) if c != "pressure"]   # 압력은 값 확인 없음
        # 지난 저널 뒤에 쓰기를 한 블록이 쓰는 항목은 뺀다 — 기대 상태(1초 행)가 그 쓰기를 아직 못 담았을 수 있다
        # (기다리는 중이면 지금 블록은 이미 쓰기를 마쳤으므로 지금 블록 자신은 빼지 않는다)
        cur_t = st.get("block_type")
        win = [t for t in (st.get("window_types") or []) if st.get("resume_mode") == "rewrite" or t != cur_t]
        unreliable = {c for t in win for c in BLOCK_WRITES.get(t, ALL_CHECKS)}
        ids = [c for c in ids if c not in unreliable or c == "interlock"]
        out: List[Check] = []
        for cid in ids:
            if cid in ("shutter", "gas", "power_select"):
                out.append(bool_check(cid, exp.get(cid) or {}, now))
            elif cid == "interlock":
                out.append(interlock_check(ch, exp.get("gas") or {}, now))
            elif cid == "power_output":
                out += power_checks(ch, exp.get("power_output") or {}, now)
            elif cid == "thickness":
                th = getattr(eng, "thickness", None)
                if th is None or not doc.get("thickness"):
                    out.append(("thickness", True, "두께 판정 없음"))
                else:
                    out += norm_checks(th.resume_checks(doc["thickness"]))
        want = {c for c in ("heater_run", "watchdog") if c in ids}
        if want:
            h = getattr(eng, "heater", None)
            hs = (self.old.get("shared", {}).get("doc") or {}).get("heater")
            if h is None or hs is None:
                out.append(("heater_run", True, "히터 없음"))
            else:
                out += [c for c in norm_checks(h.resume_checks(hs)) if c[0] in want]      # 목표 온도 허용 폭 확인은 하지 않음
        return out

    def _chamber(self, ch: str, doc: dict, now: Callable[[str], Any], adopted: List[str]) -> None:
        eng = self.sys.engine(J.ENGINE_OF[ch])
        st = doc.get("state") or {}
        gap, checks = self.conditions(ch, doc)
        if self.stop_all:
            checks.append(("all_stop", False, f"ALL STOP({self.stop_all}) — 이어가지 않고 정지 순서"))
        if all(c[1] for c in checks):
            checks += self.device_checks(ch, doc, now)
        ok = all(c[1] for c in checks)
        sh = self.old.get("shared", {}).get("doc") or {}
        plan = {"reasons": checks, "expect": doc.get("expect"), "heater": sh.get("heater"), "thickness": doc.get("thickness"),
                "adopted": adopted, "rewrite_block": st.get("resume_mode") == "rewrite"}
        dec = {"part": ch, "run_id": doc.get("run_id"), "gap_s": gap, "checks": checks, "resume": ok, "adopted": adopted,
               "mode": st.get("resume_mode")}
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
            self.say(f"{ch} 이어감({st.get('resume_mode')}) — 공백 {gap_now:.1f} s")
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

    # ── 플라즈마 클리닝·TSP — CH1·CH2 와 같은 규칙(D94) ──
    def _pc_tsp(self, part: str, doc: dict, now: Callable[[str], Any]) -> None:
        """조건(공백·첫 재시작·방식·RESUME_ENABLED_*) + 켜짐/꺼짐 확인이 맞으면 엔진 resume_from, 아니면 안전 정지(정지 순서)."""
        st = doc.get("state") or {}
        gap, checks = self.conditions(part, doc)
        if self.stop_all:
            checks.append(("all_stop", False, f"ALL STOP({self.stop_all}) — 이어가지 않고 정지 순서"))
        if all(c[1] for c in checks):
            checks += self.pc_checks(doc, now) if part == "pc" else self.tsp_checks(doc)
        ok = all(c[1] for c in checks)
        dec = {"part": part, "run_id": doc.get("run_id"), "gap_s": gap, "checks": checks, "resume": ok,
               "mode": st.get("resume_mode")}
        self.decisions[part] = dec
        eng = self.sys.engine(part)
        if ok and eng is not None:
            gap_now, _ = J.gap_of(doc, self.clock)
            hub = getattr(self.sys, "hub", None)
            if hub is not None and doc.get("run_id"):
                hub._open_runs[part] = doc["run_id"]          # 지난 실행이 보낸 run_start — run_end 를 run_end 로 내게
            why = eng.resume_from(st, gap_now, {"reasons": checks})
            if why is None:
                dec["result"] = "resumed"
                if part == "tsp":
                    self.sys.emit("engine:tsp", "resumed", f"복구됨(공백 {gap_now:.0f} s)", level="warn", code="RESUMED",
                                  event={"run_id": doc.get("run_id"), "gap_s": round(gap_now, 1), "name": "tsp",
                                         "block": {"type": st.get("phase")}, "checks": [list(c) for c in checks]})
                self.say(f"{part} 이어감({st.get('resume_mode')}) — 공백 {gap_now:.1f} s")
                return
            checks = checks + [("restore", False, f"{why}")]
            dec.update(checks=checks, resume=False)
        dec["result"] = "stopped"
        (self._stop_pc if part == "pc" else self._stop_tsp)(doc, checks)

    def pc_checks(self, doc: dict, now: Callable[[str], Any]) -> List[Check]:
        """플라즈마 클리닝 켜짐/꺼짐 확인 — GV 인터락·장비 오류 없음, 클리닝 시간 대기 중이면 RF SET 켜짐(값 허용 폭 없음)."""
        st = doc.get("state") or {}
        ch = 2 if int(st.get("ch") or 1) == 2 else 1
        if st.get("test_mode"):
            return [("device", True, "TEST — 장비 확인 없음")]
        begin = st.get("resume_begin")
        bad = []
        if now("plc.main_shutter_1_sw") is None:
            bad.append("PLC 읽기 없음")
        if begin != "gv_interlock" and now(f"plc.g_v_{ch}_인터락") is False:
            bad.append(f"GV{ch} 인터락 FALSE")
        out: List[Check] = [("interlock", not bad, "GV 인터락·장비 오류 없음" if not bad else "; ".join(bad))]
        if st.get("resume_mode") == "hold":
            en = now("pc.rf.enable")
            out.append(("power_output.RF", en is True, f"PC RF SET 켜짐 기대 · 읽음 {en}"))
        return out

    def tsp_checks(self, doc: dict) -> List[Check]:
        eng = self.sys.engine("tsp")
        return norm_checks(eng.resume_checks(doc.get("state"))) if eng is not None else [("tsp", False, "TSP 엔진 없음")]

    def _stop_pc(self, doc: dict, checks: Optional[List[Check]] = None) -> None:
        st = doc.get("state") or {}
        gap, base = self.conditions("pc", doc)
        if checks is None:
            checks = base + [("pc", False, "플라즈마 클리닝을 이어가지 않음")]
        self.decisions["pc"] = {"part": "pc", "run_id": doc.get("run_id"), "gap_s": gap, "resume": False,
                                "checks": checks, "result": "stopped", "mode": st.get("resume_mode")}
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

    def _stop_tsp(self, doc: dict, checks: Optional[List[Check]] = None) -> None:
        gap, base = self.conditions("tsp", doc)
        if checks is None:
            checks = base + [("tsp", False, "TSP 를 이어가지 않음")]
        self.decisions["tsp"] = {"part": "tsp", "run_id": doc.get("run_id"), "gap_s": gap, "resume": False,
                                 "checks": checks, "result": "stopped", "mode": (doc.get("state") or {}).get("resume_mode")}
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
        if sf is not None and hasattr(sf, "first_read"):
            return                                         # 안전 규칙 S31(첫 정상 PLC 행 — 공정·운전이 없으면 그 전원만 끔)이 한다 —
            #                                               하나로(11-09 C5: 이전엔 안전 규칙이 있어도 여기서 한 번 더 돌아 두 번 껐다)
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
