# -*- coding: utf-8 -*-
"""DC Pulse Arc 계측 재설계 검증.

t1  baseline 은 OUTPUT_ON '이전' 에 확정된다 (읽기 순서)
t2  0x8C 리셋 동작 여부를 pre/post 비교로 판정하고 로그에 남긴다
t3  점화 창 안에서는 폴링 주기가 고속으로 바뀐다
t4  [필수] 고속 창에서 AUTO-STOP 카운터가 증가하지도 리셋되지도 않는다
t5  output_off() 가 0x80=2 전송 '이전' 에 0x96/0x99 를 읽어 종료값을 확정한다
t6  _arc_split 의 점화/증착 분해 (순수 함수)
t7  장비 자체 리셋(raw < baseline) 시 baseline 0 보정 + 런당 1회 로그
"""
import os
import sys
import asyncio
import contextlib

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from device.dc_pulse import AsyncDCPulse, _arc_split   # noqa: E402


# ───────────────────────── 공용 ─────────────────────────
def _mk(**cfg):
    """실제 통신 없이 상태만 가진 인스턴스."""
    d = AsyncDCPulse.__new__(AsyncDCPulse)
    d._poll_period_s = 5.0
    d._cfg = dict(cfg)
    d._soft_arc_total = 0
    d._hard_arc_total = 0
    d._arc_baseline_soft = 0
    d._arc_baseline_hard = 0
    d._arc_ign_soft = None
    d._arc_ign_hard = None
    d._arc_final_soft = None
    d._arc_final_hard = None
    d._arc_run_start_ts = 0.0
    d._arc_reset_ok = False
    d._arc_prev_raw_s = None
    d._arc_prev_raw_h = None
    d._arc_prev_ts = 0.0
    d._arc_zero_reset_logged = False

    def _cf(key, default):
        return float(cfg.get(key, default))

    def _ci(key, default):
        return int(cfg.get(key, default))

    d._cfg_float = _cf
    d._cfg_int = _ci
    return d


# ───────────────────────── t1 / t2 ─────────────────────────
def test_1_baseline_before_output_on():
    """SAT/ANT 읽기 2쌍이 모두 OUTPUT_ON 전송 '이전' 에 일어난다."""
    import device.dc_pulse as M
    src = open(M.__file__, encoding="utf-8").read()

    i_reset = src.index("4-b) Hard/Soft Arc Count Reset")
    i_post = src.index("4-c)")
    i_on = src.index("ok2 = await self.output_on()")
    i_pre = src.index("4-a)")

    assert i_pre < i_reset < i_post < i_on, (i_pre, i_reset, i_post, i_on)
    # HV-On 직후 baseline 읽기 블록은 삭제됐다
    assert "[arc] baseline SAT=" not in src, "옛 baseline 로그가 남아 있다"
    assert src.count("_bs = await self.read_soft_arc_total()") == 0


def test_2_reset_verification_log():
    """0x8C 동작 여부 판정 규칙: post==0 이거나 post<pre 면 '예'."""
    import device.dc_pulse as M
    src = open(M.__file__, encoding="utf-8").read()
    assert "[arc] reset 확인:" in src
    assert "0x8C 동작=" in src
    assert "self._arc_reset_ok" in src

    # 판정 함수와 동일한 규칙을 그대로 재현해 검증
    def shrunk(a, b):
        if b is None:
            return False
        return int(b) == 0 or (a is not None and int(b) < int(a))

    assert shrunk(15050, 0) is True            # 완전 리셋
    assert shrunk(15050, 12) is True           # 리셋 후 바로 몇 개 발생
    assert shrunk(15050, 15050) is False       # 안 먹었다
    assert shrunk(None, 0) is True
    assert shrunk(15050, None) is False        # 읽기 실패 → 판정 불가 = 아니오


# ───────────────────────── t3 ─────────────────────────
def test_3_fast_window_period():
    import time
    d = _mk(DCP_ARC_IGN_WINDOW_S=30.0, DCP_ARC_FAST_INTERVAL_S=1.0)

    # 출력 ON 전 (_arc_run_start_ts=0) → 고속 아님
    assert d._in_arc_fast_window() is False
    assert d._effective_poll_period() == 5.0

    d._arc_run_start_ts = time.monotonic()
    assert d._in_arc_fast_window() is True
    assert d._effective_poll_period() == 1.0

    d._arc_run_start_ts = time.monotonic() - 31.0
    assert d._in_arc_fast_window() is False
    assert d._effective_poll_period() == 5.0

    # 고속 주기가 기본 주기보다 크게 설정돼도 기본 주기를 넘기지 않는다
    d2 = _mk(DCP_ARC_IGN_WINDOW_S=30.0, DCP_ARC_FAST_INTERVAL_S=99.0)
    d2._arc_run_start_ts = time.monotonic()
    assert d2._effective_poll_period() == 5.0

    # 창 0 이면 고속 비활성
    d3 = _mk(DCP_ARC_IGN_WINDOW_S=0.0)
    d3._arc_run_start_ts = time.monotonic()
    assert d3._in_arc_fast_window() is False


# ───────────────────────── t4 (필수) ─────────────────────────
def test_4_autostop_frozen_in_fast_window():
    """고속 창 안에서는 저전류/이탈 카운터를 증가시키지도 0 으로 리셋하지도 않는다.

    (1초 주기에서 연속 N회 기준을 그대로 쓰면 AUTO-STOP 이 5배 빨라진다 → 금지)
    """
    import device.dc_pulse as M
    src = open(M.__file__, encoding="utf-8").read()

    i_fast = src.index("_fast = self._in_arc_fast_window()")
    i_low = src.index("# ① 저전류 감시")
    i_sp = src.index("# ② 세트포인트 근접 확인")
    i_tel = src.index("# ③ 텔레메트리 이벤트 전송")
    assert i_fast < i_low < i_sp < i_tel

    # 두 판정 블록 모두 _fast 게이트를 먼저 통과한다
    low_block = src[i_low:i_sp]
    sp_block = src[i_sp:i_tel]
    for name, blk in (("저전류", low_block), ("세트포인트", sp_block)):
        head = blk[:blk.index("elif ref > 0.0:")]
        assert "if _fast:" in head and "pass" in head, name

    # 텔레메트리 이벤트는 게이트 밖이다(평소대로 남는다)
    assert "_fast" not in src[i_tel:i_tel + 400]

    # 고속 창 판정 자체는 AUTO-STOP 임계값을 바꾸지 않는다
    assert "_i_low_count_max_n" not in src[i_fast:i_low]
    assert "_p_set_deviate_max_n" not in src[i_fast:i_low]


# ───────────────────────── t5 ─────────────────────────
def test_5_output_off_captures_final():
    d = _mk()
    order = []
    d._out_on = True
    d._stop_guard = False
    d._arc_baseline_soft = 100
    d._arc_baseline_hard = 50
    d._soft_arc_total = 200
    d._hard_arc_total = 80

    async def _rs():
        order.append("read_soft")
        return 1000

    async def _rh():
        order.append("read_hard")
        return 500

    async def _emit(msg):
        order.append(("status", msg))

    def _drain():
        order.append("drain")

    async def _w(cmd, data, n, label=""):
        order.append(("write", hex(cmd), data, label))
        return True

    d.read_soft_arc_total = _rs
    d.read_hard_arc_total = _rh
    d._emit_status = _emit
    d._drain_rx_frames = _drain
    d._write_cmd_data = _w

    ok = asyncio.run(d.output_off())
    assert ok is True

    i_read = order.index("read_soft")
    i_write = [i for i, x in enumerate(order)
               if isinstance(x, tuple) and x[0] == "write"][0]
    assert i_read < i_write, f"종료값 읽기가 OUTPUT_OFF 이후다: {order}"
    assert order[i_write] == ("write", "0x80", 0x0002, "OUTPUT_OFF")

    # 확정값 = raw - baseline
    assert d._arc_final_soft == 900
    assert d._arc_final_hard == 450
    assert d.arc_counts == (900, 450)
    assert any(isinstance(x, tuple) and x[0] == "status" and "[arc] 종료 확정:" in x[1]
               for x in order), order


def test_5b_final_read_failure_is_harmless():
    """종료값 읽기가 실패/지연해도 OUTPUT_OFF 는 그대로 전송된다."""
    d = _mk()
    d._out_on = True
    d._arc_baseline_soft = 0
    d._arc_baseline_hard = 0
    d._soft_arc_total = 7
    d._hard_arc_total = 3
    sent = []

    async def _hang():
        await asyncio.sleep(10.0)
        return 1

    async def _emit(msg):
        pass

    async def _w(cmd, data, n, label=""):
        sent.append(label)
        return True

    d.read_soft_arc_total = _hang
    d.read_hard_arc_total = _hang
    d._emit_status = _emit
    d._drain_rx_frames = lambda: None
    d._write_cmd_data = _w

    async def _run():
        t0 = asyncio.get_running_loop().time()
        ok = await d.output_off()
        return ok, asyncio.get_running_loop().time() - t0

    ok, dt = asyncio.run(_run())
    assert ok is True and sent == ["OUTPUT_OFF"]
    assert dt < 3.0, f"OUTPUT_OFF 가 {dt:.1f}초 지연됐다"
    # 확정 실패 → 폴링 마지막 값 유지
    assert d._arc_final_soft is None
    assert d.arc_counts == (7, 3)


# ───────────────────────── t6 ─────────────────────────
def test_6_arc_split():
    # 점화 창이 아직 안 닫혔다 → 전부 점화
    d = _arc_split(120, 40, None, None)
    assert (d["ign_total"], d["depo_total"]) == (160, 0)
    assert d["ign_closed"] is False

    # 정상 분해
    d = _arc_split(3200, 900, 3000, 800)
    assert d["ign_soft"] == 3000 and d["ign_hard"] == 800
    assert d["depo_soft"] == 200 and d["depo_hard"] == 100
    assert d["run_total"] == 4100 and d["ign_total"] == 3800 and d["depo_total"] == 300
    assert d["ign_closed"] is True

    # 역전(장비 자체 리셋 등) → 0 클램프, 음수 없음
    d = _arc_split(10, 5, 3000, 800)
    assert d["depo_total"] == 0
    assert d["ign_soft"] == 10 and d["ign_hard"] == 5

    d = _arc_split(-5, -5, -1, -1)
    assert all(v >= 0 for k, v in d.items() if isinstance(v, int))


# ───────────────────────── t7 ─────────────────────────
def test_7_device_self_reset_guard():
    import device.dc_pulse as M
    src = open(M.__file__, encoding="utf-8").read()
    assert "[arc] 장비 자체 리셋 감지" in src
    assert "_arc_zero_reset_logged" in src

    # 런 시작 시 플래그가 초기화된다(런당 1회 보장)
    i_on = src.index("self._arc_run_start_ts = time.monotonic()")
    assert "self._arc_zero_reset_logged = False" in src[i_on:i_on + 900]

    # 로그는 플래그가 꺼져 있을 때만 (런당 1회)
    i_log = src.index("[arc] 장비 자체 리셋 감지")
    guard = src[i_log - 400:i_log]
    assert 'not getattr(self, "_arc_zero_reset_logged", False)' in guard


def test_8_alert_policy_uses_ign_depo():
    """런 누적 단독 기준은 판정에서 빠지고 점화/증착 기준이 들어왔다."""
    import device.dc_pulse as M
    src = open(M.__file__, encoding="utf-8").read()
    i = src.index("# ── 알림 정책 ──")
    blk = src[i:i + 5200]
    assert "_by_ign" in blk and "_by_depo" in blk
    assert "_by_rate or _by_ign or _by_depo" in blk
    assert "_by_total = False" in blk, "런 누적 단독 판정은 폐지돼야 한다"
    # 하위 호환 키는 유지
    assert '"by_total": _by_total' in blk
    assert '"run_total_limit"' in blk


def _run_all():
    fs = [v for k, v in sorted(globals().items())
          if k.startswith("test_") and callable(v)]
    for f in fs:
        f()
        print(f"  OK  {f.__name__}")
    print(f"\n{len(fs)} tests passed")


if __name__ == "__main__":
    _run_all()
