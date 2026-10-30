"""챔버12 공정 시작 전 단계 — v1 runtime/chamber_runtime.py 의 _start_after_preflight(3799-4119) 중 장비 부분.

순서(v1 그대로): 이번 런 펄스 선택(_run_select — use_*_pulse ∧ 지원만, 값으로 추정하지 않음) → 펄스 주소 점유(재연결·장비
기동보다 먼저 — 같은 주소를 다른 챔버나 같은 챔버의 다른 종류가 쓰면 거부) → 장비 기동 순서 PLC → MFC → IG → DC Pulse → RF Pulse
(GAPB-001 — v2 는 PLC·MFC·IG 가 상시 연결이라 '이미 연결' 이면 'already connected → skip', 펄스는 점유가 곧 연결) →
연결 확인(PLC·MFC·IG + 고른 펄스, 0.2 s 간격, 8 s·펄스 10 s, 1초마다 '연결 대기 중: …') → Gate(닫힘일 때만) → Main Valve
(SW ∧ 인터락) → [두께 기본 점검] → Chuck 위치(실패해도 계속 — 경고 목록) → [두께 재료값] → 시작.
Gate·Main Valve 판정(4단계 안전 규칙 표의 S 항목이 될 것)과 Chuck 이동(6단계 robot_ops 와 같은 동작)은 v1 코드를 그대로 여기
두었다 — 4·6단계가 같은 함수를 부르거나 엔진의 hooks 로 바꿔 끼운다(engine.preflight_hooks).
모든 대기는 엔진 러너 스레드에서 clock.wait_until — 취소(STOP)는 cancelled() 로 바로 끝난다(StageCancelled).
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

from v2.common.worker import Status


class StageCancelled(Exception):
    """STOP·새 시작으로 단계가 취소됨(v1 asyncio CancelledError)."""


PLC_QUEUE_ALLOW_S = 2.0      # v2: PLC worker 줄 서기(1초 묶음 읽기·명령 간격 150 ms·다른 챔버 요청) 여유 — 결정 후보


def plc_call(eng, submit: Callable[[Callable], Any], timeout_s: float):
    """PLC 요청 하나를 보내고 결과(Result)를 기다린다. 시간 초과 → TimeoutError, 취소 → StageCancelled.
    v1 읽기 한도(Gate·Main Valve 0.6 s)는 v1 이 PLC 를 거의 비워 두던 때의 값이라, v2 는 줄 서기 여유(PLC_QUEUE_ALLOW_S)를 더한다."""
    box: dict = {}

    def done(res) -> None:
        box["r"] = res
        eng.clock.notify()
    submit(done)
    ok = eng.clock.wait_until(eng.clock.monotonic() + timeout_s + PLC_QUEUE_ALLOW_S,
                              lambda: "r" in box or eng.stage_cancelled())
    if eng.stage_cancelled():
        raise StageCancelled()
    if not ok:
        raise TimeoutError(f"PLC 응답 없음({timeout_s:.1f}s)")
    return box["r"]


def read_bit(eng, name: str, timeout_s: float = 3.0) -> bool:
    from v2.ch12.drivers import plc_map as PM
    if name not in PM.COIL_MAP:
        raise KeyError(name)
    r = plc_call(eng, lambda cb: eng.plc.read_bit(name, on_done=cb), timeout_s)
    if r.status != Status.OK:
        raise RuntimeError(r.error or r.reason or r.status)
    return bool(r.value)


def write_switch(eng, name: str, on: bool, timeout_s: float = 3.0) -> None:
    r = plc_call(eng, lambda cb: eng.plc.write_switch(name, on, on_done=cb), timeout_s)
    if r.status != Status.OK:
        raise RuntimeError(r.error or r.reason or r.status)


def sleep(eng, sec: float) -> None:
    eng.clock.wait_until(eng.clock.monotonic() + max(0.0, sec), eng.stage_cancelled)
    if eng.stage_cancelled():
        raise StageCancelled()


# ───────────── 펄스 점유·장비 기동 ─────────────
KIND_LABEL = {"dcpulse": "DC-Pulse", "rfpulse": "RF-Pulse"}


def claim_pulses(eng, sel: dict) -> Optional[str]:
    """고른 펄스 장비의 주소 점유(3864-3894). 거부 문구 또는 None. 부분 점유는 부르는 쪽이 release_chamber 로 푼다."""
    for kind in ("dcpulse", "rfpulse"):
        if not sel.get(kind):
            continue
        ok, msg, _owner = eng.sys.shared.pulse.claim(kind, eng.ch)
        if not ok:
            return msg
    return None


def start_devices(eng, sel: dict) -> None:
    """_start_devices_task(2974-3048) 의 순서·로그. PLC 로그는 PLC 소유 챔버만(_owns_plc — CH1)."""
    a = eng.actions
    items = [("PLC", lambda: eng.sys.workers["plc"].connected, eng.n == 1),
             ("MFC", a.mfc.is_connected, True), ("IG", a.ig.is_connected, True)]
    if sel.get("dcpulse") and a.dcp is not None:
        items.append(("DCPulse", lambda: bool(a.dcp.worker and a.dcp.worker.connected), True))
    if sel.get("rfpulse") and a.rfp is not None:
        items.append(("RFPulse", lambda: bool(a.rfp.worker and a.rfp.worker.connected), True))
    for label, conn, log in items:
        if conn() and log:
            eng.log(label, "already connected → skip")
        elif log:
            eng.log(label, "start 호출 완료")          # v2: worker 가 이미 연결을 시도 중(상시 재연결)


def wait_connected(eng, sel: dict, timeout_s: float) -> List[str]:
    """_preflight_connect(4175-4208) — 실패한 장비 이름 목록."""
    a = eng.actions
    need: List[Tuple[str, Callable[[], bool]]] = [("PLC", lambda: eng.sys.workers["plc"].connected),
                                                  ("MFC", a.mfc.is_connected), ("IG", a.ig.is_connected)]
    if sel.get("dcpulse") and a.dcp is not None:
        need.append(("DC-Pulse", lambda: bool(a.dcp.worker and a.dcp.worker.connected)))
    if sel.get("rfpulse") and a.rfp is not None:
        need.append(("RF-Pulse", lambda: bool(a.rfp.worker and a.rfp.worker.connected)))
    c = eng.clock
    t0 = c.monotonic()
    nxt_log = t0
    while True:
        now = c.monotonic()
        missing = [n for n, f in need if not f()]
        if now >= nxt_log:
            eng.log("MAIN", f"연결 대기 중: {', '.join(missing) if missing else '모두 연결됨'}")
            nxt_log += 1.0
        if not missing:
            return []
        if now - t0 >= timeout_s:
            for n in missing:
                eng.log(n, f"연결 확인 실패(타임아웃 {timeout_s:.1f}s)")
            return missing
        sleep(eng, 0.2)


# ───────────── Gate·Main Valve(3622-3758) ─────────────
def gate_check(eng) -> Tuple[bool, str]:
    n, cg = eng.n, eng.cget
    tries, rd_t, iv = int(cg("CHAMBER_GATE_RECHECK_COUNT", 5)), float(cg("CHAMBER_GATE_READ_TIMEOUT_S", 0.6)), \
        float(cg("CHAMBER_GATE_RECHECK_INTERVAL_S", 0.2))
    for _ in range(tries):
        try:
            op = read_bit(eng, f"G_V_{n}_OPEN_LAMP", rd_t)
            cl = read_bit(eng, f"G_V_{n}_CLOSE_LAMP", rd_t)
        except KeyError as e:
            eng.log("MAIN", f"[CH{n}] PLC 주소맵에 gate lamp 키 없음: {e} → 시작 차단")
            return False, f"PLC 주소맵 키 없음 ({e})"
        except StageCancelled:
            raise
        except Exception as e:
            eng.log("MAIN", f"[CH{n}] Gate lamp 읽기 실패: {type(e).__name__}: {e} → 재시도 중")
            sleep(eng, iv)
            continue
        if cl and not op:
            return True, ""
        if op and not cl:
            eng.log("MAIN", f"[CH{n}] Gate 상태=OPEN (open={op}, close={cl})")
            return False, "Gate OPEN 상태"
        if op and cl:
            eng.log("MAIN", f"[CH{n}] Gate lamp 이상(OPEN/CLOSE 모두 TRUE) (open={op}, close={cl})")
            return False, "Gate lamp 이상 (OPEN/CLOSE 모두 True)"
        sleep(eng, iv)
    eng.log("MAIN", f"[CH{n}] Gate 상태=moving_or_unknown (OPEN/CLOSE 모두 FALSE) → 시작 차단")
    return False, "Gate 이동 중 / 상태 불명 (재시도 초과)"


def main_valve_check(eng) -> Tuple[bool, str]:
    n, cg = eng.n, eng.cget
    tries, rd_t, iv = int(cg("CHAMBER_MAINVALVE_RECHECK_COUNT", 5)), float(cg("CHAMBER_MAINVALVE_READ_TIMEOUT_S", 0.6)), \
        float(cg("CHAMBER_MAINVALVE_RECHECK_INTERVAL_S", 0.2))
    last_sw = last_it = False
    for _ in range(tries):
        try:
            sw = read_bit(eng, f"M_V_{n}_SW", rd_t)
            it = read_bit(eng, f"M_V_{n}_인터락", rd_t)
        except KeyError as e:
            eng.log("MAIN", f"[CH{n}] PLC 주소맵에 Main Valve 키 없음: {e} → 시작 차단")
            return False, f"PLC 주소맵 키 없음 ({e})"
        except StageCancelled:
            raise
        except Exception as e:
            eng.log("MAIN", f"[CH{n}] Main Valve 읽기 실패: {type(e).__name__}: {e} → 재시도 중")
            sleep(eng, iv)
            continue
        if sw and it:
            return True, ""
        last_sw, last_it = sw, it
        sleep(eng, iv)
    if not last_it:
        why = "Main Valve 인터락 불충족 (벤트 상태이거나 터보/러프펌프/압력 조건 미충족)"
    elif not last_sw:
        why = "Main Valve가 열려있지 않음 (인터락 충족, 밸브 미개방)"
    else:
        why = "Main Valve 상태 불명"
    eng.log("MAIN", f"[CH{n}] Main Valve 미개방 (sw={last_sw}, interlock={last_it}) → 시작 차단")
    return False, why


# ───────────── Chuck(4210-4354) ─────────────
def read_chuck_pos(eng) -> str:
    n = eng.n
    try:
        up, mid, dn = (read_bit(eng, f"Z{n}_{x}_LOCATION") for x in ("UP", "MID", "DOWN"))
        if int(up) + int(mid) + int(dn) == 1:
            return "up" if up else ("mid" if mid else "down")
    except StageCancelled:
        raise
    except Exception:
        pass
    return "unknown"


def chuck_move(eng, params: dict) -> bool:
    """레시피 chuck_position 이 있을 때만 1회. 실패·타임아웃이면 params chuck_position = 실측, False(공정은 계속)."""
    pos = str(params.get("chuck_position") or "").strip().lower()
    if not pos:
        return True
    n = eng.n
    if pos not in ("up", "mid", "down"):
        eng.log("PLC", f"[CH{n}] 알 수 없는 chuck_position='{pos}' → 스킵")
        return True
    power_sw = f"Z_M_P_{n}_SW"
    move_sw = {"up": f"Z_M_P_{n}_CW_SW", "mid": f"Z_M_P_{n}_MID_SW", "down": f"Z_M_P_{n}_CCW_SW"}[pos]
    lamp = f"Z{n}_{pos.upper()}_LOCATION"
    timeout_s = float(eng.cget("CHAMBER_CHUCK_MOVE_TIMEOUT_S", 60.0))
    settle_s = float(eng.cget("CHAMBER_CHUCK_POWER_ON_SETTLE_S", 0.2))
    poll_s = float(eng.cget("CHAMBER_CHUCK_POLL_INTERVAL_S", 0.3))

    def off() -> None:
        for sw in (move_sw, power_sw):
            try:
                write_switch(eng, sw, False)
            except StageCancelled:
                raise
            except Exception:
                pass
    try:
        try:
            already = read_bit(eng, lamp)
        except StageCancelled:
            raise
        except Exception:
            already = False
        if already:
            eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이미 목표 위치 ({lamp}=True) → 이동 생략")
            return True
        eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이동 시작: {power_sw} → {move_sw} → {lamp} 폴링")
        eng.state(f"Chuck {pos.upper()} 이동 중…")
        write_switch(eng, power_sw, True)
        sleep(eng, settle_s)
        write_switch(eng, move_sw, True)
        deadline = eng.clock.monotonic() + timeout_s
        while eng.clock.monotonic() < deadline:
            try:
                ok = read_bit(eng, lamp)
            except StageCancelled:
                raise
            except Exception:
                ok = False
            if ok:
                off()
                eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이동 성공 ({lamp}=True)")
                return True
            sleep(eng, poll_s)
        off()
        actual = read_chuck_pos(eng)
        eng.log("PLC", f"[CH{n}] Chuck '{pos}' 타임아웃({int(timeout_s)}s) — 실측 위치: {actual}")
        params["chuck_position"] = actual
        return False
    except StageCancelled:
        try:
            eng.plc.write_switch(move_sw, False)
            eng.plc.write_switch(power_sw, False)
        except Exception:
            pass
        raise
    except Exception as e:
        off()
        actual = read_chuck_pos(eng)
        eng.log("PLC", f"[CH{n}] Chuck '{pos}' 이동 중 예외: {e!r} — 실측 위치: {actual}")
        params["chuck_position"] = actual
        return False
