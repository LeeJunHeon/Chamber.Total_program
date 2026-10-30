"""챔버12 MFC 동작 생성기 — v1 device/mfc.py 의 고수준 동작(set_flow·flow_on·flow_off·밸브·SP·영점·WAIT_PRESSURE·안정화·정리)을
같은 순서·문구·타이밍으로. 생성기는 line_ops 항목(Send·Query·Sleep·Until)을 내고 OpRunner 가 worker 위에서 돌린다.

실패는 OpFailed(사유 코드, v1 command_failed reason 문구) — 문구는 v1 그대로(예 'ch1 FLOW_ON 30초 안에 전송되지 못해 대기열에서 철회').
'확정 시점 = max(등록 + min_delay, 전송 + after_send)' 는 v1 _await_sent(device/mfc.py:2058-2063) 그대로.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from v2.ch12.drivers.line_ops import OpFailed, Sleep, Until

PENDING = "_PENDING"                 # FLOW_ON: 결과는 안정화 작업이 낸다
STAB_MAX_ATTEMPTS = 180              # device/mfc.py:1659 — 가스 안정화 최대 3분(채널별, 코드 고정값)
SP_READ_TRIES = 5                    # device/mfc.py:1088 — WAIT_PRESSURE SP 읽기 5회
SP_READ_RETRY_S = 0.5                # device/mfc.py:1101


@dataclass
class StabJob:
    channel: int
    target_hw: float
    owner: Optional[str]
    created: float
    attempts: int = 0
    on_done: Optional[Callable] = None


def _mk(d, key, **kw) -> str:
    from v2.ch12.drivers.mfc import mk_cmd
    return mk_cmd(key, **kw)


def _min_delay(d) -> float:
    return max(float(d.c("MFC_DELAY_MS", 1000)), 200.0) / 1000.0       # device/mfc.py:470·515


def noreply(d, key: str, cmd: str, tag: str, owner, channel, gap_ms=None, min_delay_s: float = 0.0,
            after_send_s: float = 0.0, withdraw: bool = True):
    """v1 _send_noreply_confirmed / _confirm_sent_or_fail — 등록 → 실제 전송 확인 → 확정(실패 문구 '{tag} {사유}')."""
    t_enq = d.clock.monotonic()
    res = yield d.send(cmd, tag, gap_ms, withdraw)
    if res[0] != "sent":
        raise OpFailed(res[2], f"{tag} {res[1]}")
    yield Until(max(t_enq + max(0.0, min_delay_s), res[1] + max(0.0, after_send_s)))


# ───────── 유량 ─────────
def set_flow(d, ch: int, ui: float, owner):
    """FLOW_SET(device/mfc.py:419-448, 검증 꺼짐) — 'Q{ch} {str(float)}' 실제 전송 뒤 확정, last_setpoints 갱신."""
    scaled = d.ui_to_hw(ch, ui)
    d.status(f"Ch{ch} GAS 스케일: {ui:.2f}sccm → 장비 {scaled:.2f}%FS")
    if ch in d.ch_owner:
        d.ch_owner[ch] = owner
    res = yield d.send(_mk(d, "FLOW_SET", channel=ch, value=scaled), f"[SET ch{ch}]")
    if res[0] != "sent":
        raise OpFailed(res[2], f"Ch{ch} FLOW_SET {res[1]}")
    d.last_setpoints[ch] = scaled


def flow_on(d, ch: int, owner, on_done):
    """FLOW_ON(device/mfc.py:450-486) — 같은 채널 이전 안정화만 정리 → 'L{ch}1' → 전송 + max(MFC_DELAY_MS, 200) ms
    → ON 플래그 → (목표 > 0) 채널 안정화 작업, 아니면 확정."""
    prev = stab_owner(d, ch)
    cancel_stab_job(d, ch, "같은 채널 FLOW_ON 재요청", notify=(prev is not None and prev != owner))
    target = mask_set(d, ch, True)
    if ch in d.ch_owner:
        d.ch_owner[ch] = owner
    t_enq = d.clock.monotonic()
    item = d.send(_mk(d, "FLOW_ON", channel=ch), f"[FLOW_ON ch{ch}]")
    d.mask_shadow = target
    res = yield item
    if res[0] != "sent":
        raise OpFailed(res[2], f"ch{ch} FLOW_ON {res[1]}")
    yield Until(max(t_enq + _min_delay(d), res[1]))
    d.flow_on_flags[ch] = True
    if d.stab_enabled:
        tgt = float(d.last_setpoints.get(ch, 0.0))
        if tgt > 0:
            start_stab_job(d, ch, tgt, owner, on_done)
            d.status(f"FLOW_ON: ch{ch} 안정화 시작 (목표 HW {tgt:.2f})")
            return (PENDING, "")


def flow_off(d, ch: int, owner):
    """FLOW_OFF(device/mfc.py:488-523) — 안정화 취소 → 목표·카운터·ON 0 → 'L{ch}0'(철회하지 않음) → 전송 + max(1000,200) ms."""
    prev = stab_owner(d, ch)
    if cancel_stab_job(d, ch, "FLOW_OFF 요청", notify=(prev is not None and prev != owner)):
        d.status(f"FLOW_OFF 요청: ch{ch} 안정화 취소")
    d.last_setpoints[ch] = 0.0
    d.flow_error_counters[ch] = 0
    d.flow_on_flags[ch] = False
    target = mask_set(d, ch, False)
    if d.shutdown:
        raise OpFailed("SHUTDOWN", "MFC not ready (shutdown)")
    t_enq = d.clock.monotonic()
    item = d.send(_mk(d, "FLOW_OFF", channel=ch), f"[FLOW_OFF ch{ch}]", withdraw=False)
    d.mask_shadow = target
    res = yield item
    if res[0] != "sent":
        raise OpFailed(res[2], f"ch{ch} FLOW_OFF {res[1]}")
    yield Until(max(t_enq + _min_delay(d), res[1]))


def flow_set_on(d, ch: int, ui: float, owner, on_done):
    """플라즈마 클리닝 선택 채널 FLOW_SET → FLOW_ON(device/mfc.py:541-548). FLOW_SET 실패면 FLOW_ON 도 실패."""
    try:
        yield from set_flow(d, ch, ui, owner)
    except OpFailed as e:
        d.emit("command_failed", e.detail, reason=e.reason, cmd="FLOW_SET", owner=owner, channel=ch)
        raise OpFailed(e.reason, f"ch{ch} FLOW_SET 실패로 FLOW_ON 생략")
    d.emit("command_confirmed", cmd="FLOW_SET", owner=owner, channel=ch)
    r = yield from flow_on(d, ch, owner, on_done)
    return r


def mask_set(d, ch: int, on: bool) -> str:
    """섀도 마스크(device/mfc.py:2297-2303) — 내부 상태만(L0 는 보내지 않음)."""
    bits = list((d.mask_shadow or "0000").ljust(4, "0")[:4])
    if 1 <= ch <= 4:
        bits[ch - 1] = "1" if on else "0"
    return "".join(bits)


# ───────── 밸브·SP·영점 ─────────
def valve(d, key: str, owner):
    """VALVE_OPEN·VALVE_CLOSE(device/mfc.py:639-663) — 'O'/'C', 전송 + MFC_DELAY_MS_VALVE(5000) 뒤 확정.
    OPEN 은 안전 방향이라 30 s 를 넘어도 철회하지 않는다."""
    if d.shutdown:
        raise OpFailed("SHUTDOWN", "MFC not ready (shutdown)")
    yield from noreply(d, key, _mk(d, key), f"[{key}]", owner, None,
                       min_delay_s=float(d.c("MFC_DELAY_MS_VALVE", 5000)) / 1000.0, withdraw=(key == "VALVE_CLOSE"))


def sp_set(d, n: int, ui: float, owner):
    """SPn_SET(device/mfc.py:675-738) — hw = round(UI × 0.1, 3), 'S{n} {str(float)}', 전송 + MFC_GAP_MS 뒤 확정."""
    scale = float(d.c("MFC_PRESSURE_SCALE", 0.1))
    dec = int(d.c("MFC_PRESSURE_DECIMALS", 3))
    hw = round(float(ui) * scale, dec)
    d.status(f"SP{n} 스케일: UI {ui:.2f} → 장비 {hw:.{dec}f}")
    key = f"SP{n}_SET"
    yield from noreply(d, key, _mk(d, key, value=hw), f"[{key}]", owner, None,
                       min_delay_s=float(d.c("MFC_GAP_MS", 1000)) / 1000.0)


def zeroing(d, ch: Optional[int], owner):
    """PS_ZEROING 'Z1' / MFC_ZEROING(ch) 'L{4+ch}1'(device/mfc.py:1026-1039) — gap·확정 = MFC_ZEROING_GAP_MS."""
    zgap = float(d.c("MFC_ZEROING_GAP_MS", d.c("MFC_GAP_MS", 1000)))
    if ch is None:
        d.status("압력 센서 Zeroing 명령 전송")
        cmd, tag = _mk(d, "PS_ZEROING"), "[PS_ZEROING]"
    else:
        d.status(f"Ch{ch} MFC Zeroing 명령 전송")
        cmd, tag = _mk(d, "MFC_ZEROING", channel=ch), f"[MFC_ZEROING ch{ch}]"
    yield from noreply(d, "", cmd, tag, owner, ch, gap_ms=zgap, after_send_s=zgap / 1000.0)


# ───────── 읽기 ─────────
def read_pressure(d, emit_fail: bool, tag: str, owner):
    """v1 read_pressure(device/mfc.py:878-892) — R5 → pressure 이벤트 + UI 값(없으면 None)."""
    line = yield d.query("R5", ("P",), tag)
    if not (line and line.strip()):
        if emit_fail:
            d.emit("command_failed", "응답 없음", reason="DEVICE_NO_RESPONSE", cmd="READ_PRESSURE", owner=owner)
        else:
            d.status("[READ_PRESSURE] 응답 없음 (non-fatal, will retry)")
        return None
    return d.emit_pressure(line.strip())


def read_pressure_cmd(d, owner):
    yield from read_pressure(d, True, "[READ_PRESSURE]", owner)


def read_flow_all(d, owner):
    """READ_FLOW_ALL(device/mfc.py:864-876) — R60 → 채널별 flow 이벤트 + 유량 감시."""
    from v2.ch12.drivers.mfc import GAS_MAP, parse_r60
    line = yield d.query("R60", ("Q0",), "[READ R60]")
    vals = parse_r60(line or "")
    if not vals:
        d.emit("command_failed", "R60 파싱 실패", reason="DEVICE_REJECTED", cmd="READ_FLOW", owner=owner)
        return
    for ch, name in GAS_MAP.items():
        if ch - 1 < len(vals):
            d.emit("flow", gas=name, value=d.hw_to_ui(ch, float(vals[ch - 1])))
            d.monitor_flow(ch, float(vals[ch - 1]))


def _read_sp_ui(d, idx: int):
    """_read_sp_setpoint_ui(device/mfc.py:750-802) — R{n} 'S{n}…' → UI(= hw / 0.1, 소수 3자리)."""
    from v2.ch12.drivers.mfc import parse_pressure
    line = yield d.query(f"R{idx}", (f"S{idx}",), f"[READ_SP{idx}]")
    if not (line and line.strip()):
        d.status(f"[READ_SP{idx}] 응답 없음")
        return None
    hw = parse_pressure(line.strip())
    if hw is None:
        d.status(f"[READ_SP{idx}] 파싱 실패: {line!r}")
        return None
    scale = float(d.c("MFC_PRESSURE_SCALE", 0.1))
    dec = int(d.c("MFC_PRESSURE_DECIMALS", 3))
    ui = round(float(hw) / scale, dec)
    d.status(f"[READ_SP{idx}] 현재 setpoint (UI) = {ui:.{dec}f}")
    return ui


def within(d, target: float, actual: float) -> bool:
    """pressure_within_tolerance(device/mfc.py:807-822)."""
    if target <= 0:
        return False
    diff = abs(actual - target)
    return diff <= float(d.c("MFC_PRESSURE_TOL_ABS", 0.02)) or diff <= abs(target) * float(d.c("MFC_PRESSURE_TOL_REL", 0.05))


def wait_pressure(d, target: float, timeout_sec, use_sp_target: bool, sp_index, owner):
    """WAIT_PRESSURE(device/mfc.py:1066-1134 + wait_for_pressure_reached 894-982). 경과는 v1 처럼 '확인 간격 합'으로 센다
    (읽는 데 걸린 시간은 넣지 않음 — v1 과 같게)."""
    timeout = float(d.c("MFC_PRESSURE_TIMEOUT_SEC", 60.0) if timeout_sec is None else timeout_sec)
    if use_sp_target and sp_index is not None:
        try:
            sp_idx = int(sp_index)
        except Exception:
            sp_idx = None
        if sp_idx in (1, 2, 3, 4):
            sp_target = None
            for attempt in range(1, SP_READ_TRIES + 1):
                sp_target = yield from _read_sp_ui(d, sp_idx)
                if sp_target is not None and sp_target > 0:
                    break
                d.status(f"[WAIT_PRESSURE] SP{sp_idx} setpoint 읽기 실패 (재시도 {attempt}/5)")
                if attempt < SP_READ_TRIES:
                    yield Sleep(SP_READ_RETRY_S)
            if sp_target is None or sp_target <= 0:
                raise OpFailed("SP_READ_FAILED", f"SP{sp_idx} setpoint를 5회 시도 후에도 읽지 못함 → 압력 대기 불가")
            d.status(f"[WAIT_PRESSURE] SP{sp_idx} setpoint {sp_target:.3g} 기준으로 압력 도달 대기")
            target = sp_target
        else:
            d.status(f"[WAIT_PRESSURE] 잘못된 sp_index={sp_index!r} → UI target 사용")
    interval = float(d.c("MFC_PRESSURE_CHECK_INTERVAL_SEC", 1.0))
    ok, last = yield from _wait_reached(d, target, timeout, interval, owner)
    if not ok:
        raise OpFailed("PRESSURE_NOT_STABLE", f"압력 안정화 실패: target={target:.3g}, last={last:.3g}")


def _wait_reached(d, target: float, timeout: float, interval: float, owner):
    tol_abs = float(d.c("MFC_PRESSURE_TOL_ABS", 0.02))
    tol_rel = float(d.c("MFC_PRESSURE_TOL_REL", 0.05))
    need = int(d.c("MFC_PRESSURE_STABLE_COUNT", 3))
    fail_max = int(d.c("MFC_PRESSURE_READ_FAIL_STREAK_MAX", 3))
    if target <= 0:
        d.status(f"[PRESSURE] target <= 0 이라서 압력 대기를 건너뜁니다. target={target}")
        return False, 0.0
    stable, elapsed, last, fails = 0, 0.0, 0.0, 0
    d.status(f"[PRESSURE] 목표압 {target:.3g} (tol_abs={tol_abs}, tol_rel={tol_rel * 100:.1f}%) 도달까지 대기 시작")
    while elapsed < timeout:
        cur = yield from read_pressure(d, False, "[READ_PRESSURE]", owner)
        if cur is None:
            fails += 1
            stable = 0
            if fails >= fail_max:
                d.status(f"[PRESSURE] READ_PRESSURE 연속 {fails}회 실패 → 통신불가로 중단")
                return False, last
            yield Sleep(interval)
            elapsed += interval
            continue
        fails = 0
        last = cur
        if within(d, target, cur):
            stable += 1
            d.status(f"[PRESSURE] OK ({stable}/{need}) target={target:.3g}, current={cur:.3g}")
            if stable >= need:
                d.status(f"[PRESSURE] 목표압 도달 및 안정: target={target:.3g}, current={cur:.3g}")
                return True, cur
        else:
            stable = 0
            d.status(f"[PRESSURE] 아직 목표 미달: target={target:.3g}, current={cur:.3g}")
        yield Sleep(interval)
        elapsed += interval
    d.status(f"[PRESSURE] 타임아웃({timeout:.1f}s) - target={target:.3g}, last={last:.3g}")
    return False, last


# ───────── 안정화(채널별 작업, 감시 op 하나) ─────────
def stab_owner(d, ch: int):
    job = d.stab_jobs.get(ch)
    return job.owner if job is not None else None


def _job_end(d, job: StabJob, reason: Optional[str], detail: str, emit: bool = True) -> None:
    if emit:
        if reason is None:
            d.emit("command_confirmed", cmd="FLOW_ON", owner=job.owner, channel=job.channel)
        else:
            d.emit("command_failed", detail, reason=reason, cmd="FLOW_ON", owner=job.owner, channel=job.channel)
    if job.on_done:
        job.on_done("FLOW_ON", reason, detail)


def start_stab_job(d, ch: int, target_hw: float, owner, on_done) -> None:
    """device/mfc.py:1594-1606 — 같은 채널 이전 작업은 교체(다른 주체였으면 그 주체에 실패)."""
    prev = d.stab_jobs.get(ch)
    if prev is not None and prev.owner != owner:
        _job_end(d, prev, "STAB_CANCELLED", f"ch{ch} 안정화 중단 — 다른 주체({owner or '-'})가 같은 채널을 다시 켬")
    d.stab_jobs[ch] = StabJob(ch, float(target_hw), owner, d.clock.monotonic(), on_done=on_done)
    if d.stab_op is None or d.stab_op.done:
        d.stab_op = d.runner.start("stab", stab_loop(d), None, kind="write")


def cancel_stab_job(d, ch: int, reason: str, notify: bool) -> bool:
    """device/mfc.py:1608-1622 — notify 면 그 작업 주체에게 FLOW_ON 실패 'ch{n} 안정화 중단 (사유)'."""
    job = d.stab_jobs.pop(ch, None)
    if job is None:
        return False
    _job_end(d, job, "STAB_CANCELLED", f"ch{ch} 안정화 중단 ({reason})", emit=notify)
    return True


def cancel_all_stab(d, reason: str, notify: bool) -> int:
    return sum(1 for ch in list(d.stab_jobs) if cancel_stab_job(d, ch, reason, notify))


def stab_loop(d):
    """_stabilization_loop(device/mfc.py:1631-1669) — R60 한 번으로 등록된 채널을 함께 판정, MFC_STABILIZATION_INTERVAL_MS 마다."""
    from v2.ch12.drivers.mfc import parse_r60
    while d.stab_jobs:
        chs = sorted(d.stab_jobs)
        t_read = d.clock.monotonic()
        line = yield d.query("R60", ("Q0",), f"[STAB R60 ch{','.join(str(c) for c in chs)}]")
        vals = parse_r60(line or "")
        tol_ratio = float(d.c("FLOW_ERROR_TOLERANCE", 0.05))
        for ch in chs:
            job = d.stab_jobs.get(ch)
            if job is None or job.created > t_read:
                continue
            actual_hw = float(vals[ch - 1]) if vals and (ch - 1) < len(vals) else None
            actual_ui = None if actual_hw is None else d.hw_to_ui(ch, actual_hw)
            d.status(f"GAS 확인... ch{ch} (목표: {d.hw_to_ui(ch, job.target_hw):.2f}sccm, "
                     f"현재: {(-1 if actual_ui is None else actual_ui):.2f}sccm)", owner=job.owner, channel=ch)
            if d.stab_jobs.get(ch) is not job:
                continue
            if actual_hw is not None and abs(actual_hw - job.target_hw) <= job.target_hw * tol_ratio:
                d.stab_jobs.pop(ch, None)
                _job_end(d, job, None, "")
                continue
            job.attempts += 1
            if job.attempts >= STAB_MAX_ATTEMPTS:
                d.stab_jobs.pop(ch, None)
                _job_end(d, job, "STAB_TIMEOUT", "GAS 안정화 시간 초과")
        if not d.stab_jobs:
            break
        yield Sleep(float(d.c("MFC_STABILIZATION_INTERVAL_MS", 1000)) / 1000.0)


# ───────── 정리 ─────────
def _purge(d, label: str) -> int:
    tags = d.runner.purge(keep=lambda op: op is d.stab_op)
    if tags:
        d.status(f"대기 중 명령 {len(tags)}개 폐기 ({label}) tags={tags!r}")
    else:
        d.status(f"대기 중 명령 {len(tags)}개 폐기 ({label})")
    return len(tags)


def finish_all(d, label: str) -> None:
    """on_process_finished(device/mfc.py:1170-1189)."""
    d.set_process_status(False)
    cancel_all_stab(d, f"정리({label})로 안정화 중단", True)
    _purge(d, label)
    d.last_setpoints = {1: 0.0, 2: 0.0, 3: 0.0}
    d.flow_error_counters = {1: 0, 2: 0, 3: 0}
    d.flow_on_flags = {1: False, 2: False, 3: False}
    d.ch_owner = {1: None, 2: None, 3: None}
    d.selected_ch = d.selected_owner = None


def release_owner(d, owner: str, reason: str) -> None:
    """release_owner(device/mfc.py:1191-1216) — owner 몫만(대기열 유지, 실패 통지 없음)."""
    n_stab = 0
    for ch, job in list(d.stab_jobs.items()):
        if job.owner == owner and cancel_stab_job(d, ch, f"{owner} 사용 종료", notify=False):
            n_stab += 1
    chs = [ch for ch, o in d.ch_owner.items() if o == owner]
    for ch in chs:
        d.last_setpoints[ch] = 0.0
        d.flow_on_flags[ch] = False
        d.flow_error_counters[ch] = 0
        d.ch_owner[ch] = None
    if d.selected_owner == owner:
        d.selected_ch = d.selected_owner = None
    tail = f", {reason}" if reason else ""
    d.status(f"[{owner}] 사용 종료 — 다른 사용자가 있어 대기열 유지, 이 주체 몫만 정리 "
             f"(채널 {chs or '-'}, 안정화 취소 {n_stab}건{tail})", owner=owner)


def cleanup(d) -> None:
    """cleanup(device/mfc.py:313-372)의 상태 부분."""
    d.status("MFC 종료 절차 시작")
    d.polling, d._poll_stage = False, None
    cancel_all_stab(d, "MFC 연결 종료", True)
    _purge(d, "shutdown")
    d.status("MFC 연결 종료됨")


# ───────── 문자열 명령 라우터(handle_command) ─────────
def route(d, cmd: str, args: dict, owner, on_done) -> None:
    key = (cmd or "").strip().upper()

    def req(name: str, cast: Callable[[Any], Any] = float):
        if name not in args:
            raise KeyError(f"'{name}' is required for {key}")
        try:
            return cast(args[name])
        except Exception as e:
            raise ValueError(f"invalid {name} for {key}: {args[name]!r}") from e

    try:
        if key == "FLOW_SET":
            d.set_flow(req("channel", int), req("value", float), owner, on_done)
        elif key == "FLOW_ON":
            d.flow_on(req("channel", int), owner, on_done)
        elif key == "FLOW_OFF":
            d.flow_off(req("channel", int), owner, on_done)
        elif key == "VALVE_OPEN":
            d.valve_open(owner, on_done)
        elif key == "VALVE_CLOSE":
            d.valve_close(owner, on_done)
        elif key == "PS_ZEROING":
            d.ps_zeroing(owner, on_done)
        elif key == "MFC_ZEROING":
            d.mfc_zeroing(req("channel", int), owner, on_done)
        elif key in ("SP1_ON", "SP2_ON", "SP3_ON", "SP4_ON"):
            d.sp_on(int(key[2]), owner, on_done)
        elif key in ("SP1_SET", "SP2_SET", "SP4_SET"):
            d.sp_set(int(key[2]), req("value", float), owner, on_done)
        elif key == "WAIT_PRESSURE":
            target = req("target", float)
            timeout = float(args.get("timeout_sec", d.c("MFC_PRESSURE_TIMEOUT_SEC", 60.0)))
            d.wait_pressure(target, timeout, bool(args.get("use_sp_target", False)), args.get("sp_index", None),
                            owner, on_done)
        elif key in ("READ_FLOW_ALL", "READ_FLOW"):
            d.read_flow_all(owner, on_done)
        elif key == "READ_PRESSURE":
            d.read_pressure(owner, on_done)
        else:
            d.emit("command_failed", "지원되지 않는 MFC 명령", reason="UNKNOWN_COMMAND", cmd=key, owner=owner)
            if on_done:
                on_done(key, "UNKNOWN_COMMAND", "지원되지 않는 MFC 명령")
    except Exception as e:
        d.emit("command_failed", f"예외: {e}", reason="BAD_ARGS", cmd=key, owner=owner)
        if on_done:
            on_done(key, "BAD_ARGS", f"예외: {e}")
