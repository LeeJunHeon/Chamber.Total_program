"""챔버12 IG 동작 생성기 — v1 device/ig.py 의 베이스 압력 대기·재점등·SIG 0 최선 전송·정리·공정 중 폴링을 같은 순서·문구·타이밍으로.

v1 은 대기 루프(50 ms 감시)·폴링 태스크·정리 태스크가 따로 돌았다. v2 는 대기 op 하나(SIG 1 → 첫 RDI → 폴링 → 정리)와
하드 한도 감시 op 하나(device/ig.py:369-378)로 같은 결과를 낸다 — 정리(cleanup)는 v1 의 _cleaning 가드처럼 한 번만.
base_failed 문구는 v1 그대로(꼬리 조건 불일치 포함 — D181).
"""
from __future__ import annotations

from typing import Optional

from v2.ch12.drivers.line_ops import CALLER_TIMEOUT, Disconnect, OpFailed, Sleep, Until

SILENT = "_SILENT"                    # on_done 을 부르지 않음(대기 op 의 결과는 wait_cb 가 따로)
_FAIL_CODE = {"IG ON 실패": "IG_ON_FAILED", "목표 압력 도달 실패": "BASE_TIMEOUT", "ReigniteLimit": "REIGNITE_LIMIT",
              "HardTimeout": "HARD_TIMEOUT", "InternalError": "INTERNAL_ERROR", "PollingTaskExited": "POLL_EXITED"}


# ───────── 기본 입출력 ─────────
def send_and_wait(g, cmd: str, tag: str, retries: int = 1):
    """_send_and_wait_line(device/ig.py:941-972) — 호출 쪽 한도를 넘기면 status 후 (남았으면) 연결을 끊고 다시."""
    attempts = max(0, int(retries)) + 1
    for attempt in range(attempts):
        line = yield g.query(cmd, tag)
        if line is CALLER_TIMEOUT:
            g.status(f"{tag} '{cmd}' 응답 타임아웃({int(g.c('IG_TIMEOUT_MS', 3000))}ms)")
            if attempt < attempts - 1:
                yield Disconnect(f"{tag} 응답 타임아웃 — 다시")
                continue
            return None
        return line
    return None


def expect_ok(g, cmd: str, tag: str, retries: int):
    """_send_and_expect_ok(device/ig.py:929-939) — 'OK' 로 시작하지 않으면 연결을 끊고 다음 시도."""
    for _ in range(max(1, int(retries))):
        line = yield from send_and_wait(g, cmd, tag, retries=0)
        if (line or "").strip().upper().startswith("OK"):
            return True
        yield Disconnect(f"{cmd}: 'OK' 아님 — 재연결")
    return False


def off_best_effort(g, wait_gap_ms: int):
    """_send_off_best_effort(device/ig.py:974-1030) — SIG 0 을 큐로, 보낼 때까지 max(600, wait_gap+300) ms 기다린 뒤 wait_gap 쉼.
    못 보냈으면 명령은 남겨 두고(링크가 돌아오면 보냄 — v1 의 직접 쓰기·임시 연결 1회 대신) True."""
    yield g.send_off(wait_gap_ms)
    yield Sleep(max(0, int(wait_gap_ms)) / 1000.0, io=False)
    return True


def emit_failed(g, why: str) -> None:
    """_emit_failed(device/ig.py:1096-1106) — '목표 도달 실패' 일 때만 꼬리. 실제 시간 초과 문구는 '목표 압력 도달 실패'라 안 붙는다(D181)."""
    msg = why
    if why == "목표 도달 실패" and g.last_pressure is not None:
        msg = f"{why} (최종 {g.last_pressure:.3e} Torr, 목표 {g.target_pressure:.3e} Torr)"
    code = _FAIL_CODE.get(why, "INTERNAL_ERROR")
    g.last_fail = (code, msg)
    g.emit("base_failed", msg, reason=code)


def emit_pressure(g, p: float) -> None:
    g.last_pressure = p
    g.emit("pressure", pressure=p)


def _purge(g, reason: str) -> int:
    tags = g.runner.purge()
    g.status(f"대기 중 명령 {len(tags)}개 폐기 ({reason})")
    return len(tags)


def _stop_bg(g) -> None:
    if g.bg_op is not None and not g.bg_op.done:
        g.runner.cancel(g.bg_op, SILENT, "")
    g.bg_op = None


# ───────── 정리 ─────────
def cleanup(g):
    """cleanup(device/ig.py:192-254) — 중복 가드, 대기 해제·재점등 금지 → SIG 0(400) → 공정 중 폴링 중지 → 대기열 폐기 → '종료됨'."""
    if g.cleaning:
        return
    g.cleaning = True
    try:
        g.waiting_active = False
        g.suspend_reignite = True
        yield from off_best_effort(g, 400)
        _stop_bg(g)
        _purge(g, "shutdown")
        g.status("IG 연결 종료됨")
    finally:
        g.cleaning = False


def _cancel_body(g, reason: str):
    """cancel_wait(device/ig.py:400-425)."""
    g.waiting_active = False
    g.suspend_reignite = True
    g.total_reignite = 0
    _stop_bg(g)
    _purge(g, reason or "user cancel / stop")
    yield from off_best_effort(g, 300)


def _end_wait_with_cleanup(g, reason: str, detail: str):
    """진행 중 대기를 멈추고(대기 op 취소) v1 finally 의 정리를 한 뒤 대기 결과를 알린다."""
    op, cb = g.wait_op, g.wait_cb
    g.wait_op = g.wait_cb = None
    if op is not None and not op.done:
        g.runner.cancel(op, SILENT, "")
    yield from cleanup(g)
    if cb:
        cb("WAIT_FOR_BASE", reason, detail)


def cancel_wait(g, reason: str):
    active = g.wait_op is not None and not g.wait_op.done
    if active:
        g.runner.cancel(g.wait_op, SILENT, "")
    yield from _cancel_body(g, reason)
    if active:
        yield from _end_wait_with_cleanup(g, "CANCELLED", "")


# ───────── 베이스 압력 대기 ─────────
def start_wait(g, target: float, interval_ms: Optional[int], on_done) -> None:
    """worker 스레드에서. 이전 대기가 있으면 그 대기는 결과 SUPERSEDED(이벤트 없음 — v1 은 False 를 돌려줄 뿐)."""
    prev_op, prev_cb = g.wait_op, g.wait_cb
    prev = g.waiting_active and prev_op is not None and not prev_op.done
    if prev:
        g.runner.cancel(prev_op, SILENT, "")
        if prev_cb:
            prev_cb("WAIT_FOR_BASE", "SUPERSEDED", "")
    token = object()
    g.wait_token, g.wait_cb = token, on_done

    def fin(op, reason, detail):
        if reason == SILENT or g.wait_token is not token:
            return
        cb, g.wait_cb, g.wait_op = g.wait_cb, None, None
        if cb:
            cb("WAIT_FOR_BASE", reason, detail)
    g.wait_op = g.runner.start("WAIT_FOR_BASE", wait_gen(g, target, interval_ms, token, prev), fin)


def _result(g):
    if g.last_wait_success:
        return None, ""
    return getattr(g, "last_fail", None) or ("CANCELLED", "")


def wait_gen(g, target: float, interval_ms: Optional[int], token, prev: bool):
    if interval_ms is None:
        interval_ms = int(g.c("IG_POLLING_INTERVAL_MS", 10000))
    if prev:
        g.status("이미 Base Pressure 대기 중입니다. 이전 대기를 정리하고 재시작합니다.")
        yield from _cancel_body(g, "user cancel / stop")
        yield from cleanup(g)
    g.target_pressure = float(target)
    g.wait_start = g.clock.monotonic()
    g.waiting_active = True
    g.last_pressure = None
    g.suspend_reignite = False
    g.total_reignite = 0
    g.last_wait_success = False
    g.last_fail = None
    g.status("Base Pressure 대기 시작")
    ok = yield from expect_ok(g, "SIG 1", "[IG ON]", 5)
    if not ok:
        # v1: 이 return 은 try/finally 밖(device/ig.py:321-326) — 정리(cleanup)를 하지 않아 SIG 0 은 한 번뿐(골든 ig_on_failed)
        emit_failed(g, "IG ON 실패")
        yield from off_best_effort(g, 200)
        g.waiting_active = False
        return _result(g)
    first_ms = int(g.c("IG_FIRST_READ_DELAY_MS", 5000))
    timeout_ms = int(g.c("IG_TIMEOUT_MS", 3000))
    g.status(f"IG ON OK → 첫 RDI를 {first_ms}ms 후 수행")
    g.block_reads(first_ms / 1000.0 + 2 * (timeout_ms / 1000.0 + 0.5) + 2.0)
    yield Sleep(first_ms / 1000.0)
    g.status(f"[FIRST READ] RDI 송신 및 응답 대기 (최대 {timeout_ms}ms)")
    line = yield from send_and_wait(g, "RDI", "[FIRST READ AFTER ON]", retries=1)
    g.read_block_until = g.clock.monotonic()
    g.worker.suppress(g.clock.monotonic(), "")
    limit_s = float(g.c("IG_WAIT_TIMEOUT", 600))
    if line is None:
        g.status("[FIRST READ] RDI 타임아웃 → 재연결 트리거")
        yield Disconnect("[FIRST READ] RDI 타임아웃")
    if not g.waiting_active:                      # v1: try 밖 return — 정리 없음
        return SILENT, ""
    yield from handle_rdi(g, line)
    hard = g.wait_start + limit_s + first_ms / 1000.0 + 5.0
    guard = g.runner.start("IG_HARD_GUARD", hard_guard(g, token, hard), None)
    try:
        while g.waiting_active:
            if not g.worker.connected:
                yield Sleep(max(0.0, interval_ms / 1000.0))
                continue
            line = yield from send_and_wait(g, "RDI", "[POLL RDI]", retries=0)
            if not g.waiting_active:
                break
            yield from handle_rdi(g, line)
            if not g.waiting_active:
                break
            log_wait_again(g, interval_ms)
            if g.clock.monotonic() - g.wait_start > limit_s:
                g.status(f"시간 초과({limit_s:.1f}s): 목표 압력 미도달")
                emit_failed(g, "목표 압력 도달 실패")
                g.waiting_active = False
                yield from off_best_effort(g, 200)
                break
            yield Sleep(max(0.0, interval_ms / 1000.0))
    except Exception as e:                        # device/ig.py:821-832
        g.status(f"폴링 태스크 예외: {e!r}")
        emit_failed(g, "InternalError")
        g.waiting_active = False
        yield from off_best_effort(g, 200)
    g.runner.cancel(guard, SILENT, "")
    if g.wait_token is token:
        yield from cleanup(g)
    return _result(g)


def hard_guard(g, token, deadline: float):
    """하드 한도(device/ig.py:369-378) — 대기가 아직이면 'HardTimeout' + SIG 0 + 정리."""
    yield Until(deadline, io=False)
    if g.wait_token is not token or not g.waiting_active:
        return SILENT, ""
    g.status("하드 타임아웃: 상위 가드에 의해 종료")
    emit_failed(g, "HardTimeout")
    g.waiting_active = False
    op = g.wait_op
    if op is not None and not op.done:
        g.runner.cancel(op, SILENT, "")
    yield from off_best_effort(g, 200)
    yield from _end_wait_with_cleanup(g, "HARD_TIMEOUT", g.last_fail[1])
    return SILENT, ""


def log_wait_again(g, interval_ms: int) -> None:
    sec = max(0, int(interval_ms)) // 1000
    if g.last_pressure is not None:
        g.status(f"목표 미도달: 현재 {g.last_pressure:.3e} Torr > 목표 {g.target_pressure:.3e} Torr → {sec}초 후 재시도")
    else:
        g.status(f"목표 미도달 → {sec}초 후 재시도")


def handle_rdi(g, line: Optional[str]):
    """_handle_rdi_line(device/ig.py:834-906) — IG OFF → 재점등, 숫자 → pressure 이벤트·도달 판정."""
    if not g.waiting_active:
        return
    s = (line or "").strip()
    if not s:
        return
    if s.upper() == "IG OFF":
        if not g.waiting_active or g.suspend_reignite:
            return
        max_attempts = int(g.c("IG_REIGNITE_MAX_ATTEMPTS", 3))
        if g.total_reignite >= max_attempts:
            g.status(f"IG OFF 응답 반복 → 자동 재점등 중단(상한 {max_attempts}회 도달). 공정 실패로 종료")
            g.waiting_active = False
            g.suspend_reignite = True
            g.last_wait_success = False
            emit_failed(g, "ReigniteLimit")
            return
        g.total_reignite += 1
        g.status(f"IG OFF 응답 감지 → 자동 재점등 시도({g.total_reignite}/{max_attempts})")
        ok = yield from try_re_on(g)
        if ok:
            g.status("재점등 성공. 첫 RDI 후 폴링 재개")
            first = int(g.c("IG_FIRST_READ_DELAY_MS", 5000)) / 1000.0
            g.block_reads(first + float(g.c("IG_TIMEOUT_MS", 3000)) / 1000.0 + 2.0)
            yield Sleep(first)
            line2 = yield from send_and_wait(g, "RDI", "[AFTER RE-ON]", retries=0)
            g.read_block_until = g.clock.monotonic()
            g.worker.suppress(g.clock.monotonic(), "")
            yield from handle_rdi(g, line2)
        else:
            g.status("자동 재점등 실패(한도 도달). 폴링만 재개")
        return
    try:
        pressure = float(s.lower().replace("x10e", "e"))
    except Exception:
        g.status(f"압력 읽기 실패(파싱): {s!r}")
        return
    emit_pressure(g, pressure)
    if pressure <= g.target_pressure:
        g.status("목표 압력 도달")
        g.emit("base_reached")
        g.waiting_active = False
        g.last_wait_success = True
        g.suspend_reignite = True


def try_re_on(g):
    """_try_re_on_with_backoff(device/ig.py:908-927) — SIG 1(3회)을 IG_REIGNITE_BACKOFF_MS 순서로."""
    if not g.waiting_active or g.suspend_reignite:
        return False
    raw = str(g.c("IG_REIGNITE_BACKOFF_MS", "[2000, 5000, 10000]"))
    try:
        import json
        backoffs = json.loads(raw) if raw.strip().startswith("[") else [int(x) for x in raw.split(",") if x.strip()]
    except Exception:
        backoffs = [2000, 5000, 10000]
    delays = [max(0, int(ms)) / 1000.0 for ms in backoffs] or [2.0, 5.0, 10.0]
    for sec in delays:
        if not g.waiting_active or g.suspend_reignite:
            return False
        ok = yield from expect_ok(g, "SIG 1", "[IG RE-ON]", 3)
        if ok:
            return True
        g.status(f"재점등 실패 → {int(sec * 1000)}ms 후 재시도")
        yield Sleep(sec)
    return False


# ───────── 공정 중 폴링·단발 ─────────
def set_process_status(g, should_poll: bool) -> None:
    """device/ig.py:1248-1274 (worker 스레드)."""
    if should_poll:
        if g.bg_op is None or g.bg_op.done:
            g.bg_op = g.runner.start("IG_BG_POLL", bg_loop(g), None, kind="read")
        return
    _stop_bg(g)
    was_waiting = g.wait_op is not None and not g.wait_op.done and g.waiting_active
    g.waiting_active = False
    g.suspend_reignite = True
    _purge(g, "process finished")
    if was_waiting:                        # v1: 대기 루프가 waiting 해제를 보고 끝남 → finally 정리
        g.runner.start("IG_END_WAIT", _end_wait_with_cleanup(g, "CANCELLED", ""), None)


def bg_loop(g):
    """_bg_poll_loop(device/ig.py:1276-1302) — 베이스 대기 중이면 0.2 s 쉼, 아니면 RDI → pressure 이벤트, IG_POLLING_INTERVAL_MS."""
    while True:
        interval = int(g.c("IG_POLLING_INTERVAL_MS", 10000)) / 1000.0
        if g.waiting_active:
            yield Sleep(0.2, io=False)
            continue
        if not g.worker.connected:
            yield Sleep(interval)
            continue
        line = yield from send_and_wait(g, "RDI", "[BG-POLL RDI]", retries=0)
        if line:
            try:
                emit_pressure(g, float(line.strip().lower().replace("x10e", "e")))
            except Exception:
                pass
        yield Sleep(interval)


def ensure_off(g):
    yield from off_best_effort(g, 300)


def ensure_on(g):
    """ensure_on(device/ig.py:1142-1153) — SIG 1(5회), 실패 'SIG 1 failed (ensure_on)'."""
    ok = yield from expect_ok(g, "SIG 1", "[ensure_on]", 5)
    if not ok:
        raise OpFailed("IG_ON_FAILED", "SIG 1 failed (ensure_on)")


def read_pressure(g):
    line = yield from send_and_wait(g, "RDI", "[read_pressure]", retries=0)
    if not line:
        raise OpFailed("DEVICE_NO_RESPONSE", "RDI timeout")
    try:
        p = float(line.strip().lower().replace("x10e", "e"))
    except Exception:
        raise OpFailed("DEVICE_REJECTED", f"parse error: {line!r}")
    g.last_read = p
    return None, repr(p)


def route(g, cmd: str, kw: dict, on_done) -> None:
    key = (cmd or "").strip().upper()
    try:
        if key == "ENSURE_ON":
            g.ensure_on(on_done)
        elif key == "ENSURE_OFF":
            g.ensure_off(on_done)
        elif key == "READ_PRESSURE":
            def done(k, r, d):
                if r is None:
                    emit_pressure(g, float(d))
                else:
                    g.status(f"[command] 예외: RuntimeError({d!r})")
                if on_done:
                    on_done(k, r, d)
            g.read_pressure(done)
        elif key == "WAIT_FOR_BASE":
            g.wait_for_base_pressure(float(kw.get("target")), int(kw.get("interval_ms", int(g.c("IG_POLLING_INTERVAL_MS", 10000)))),
                                     on_done)
        elif key == "CANCEL_WAIT":
            g.cancel_wait(on_done=on_done)
        elif key == "SET_ENDPOINT":
            g.status("[command] SET_ENDPOINT 는 v2 조립(설정 IG 포트 + 재시작)에서 — 무시")
        else:
            g.status(f"[command] 지원하지 않는 IG 명령: {cmd!r}")
    except Exception as e:
        g.status(f"[command] 예외: {e!r}")
