# -*- coding: utf-8 -*-
"""HeaterService + 램프 + 유지 + 포화 감시 통합 — 래더·열 모델 에뮬레이터(tests/heater_ladder_sim.py) 시나리오."""
import os
import sys
import csv
import asyncio

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))
sys.path.insert(0, _HERE)

import pytest                                   # noqa: E402
import heater_ladder_sim as sim                 # noqa: E402


def _cards(chat, word):
    """제목에 word 가 든 카드."""
    return [c for c in chat.cards if word in c[1]]


def _logs(logs, word):
    """word 가 든 로그 문구."""
    return [m for t, m in logs if word in m]


def test_reachable_200_hold_no_false_saturation():
    """200°C(도달 가능) — dac 유지 진입 후 MV 가 유지값에 붙어도 포화 경고·카드가 없어야 한다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(200.0)
        assert ok, why
        await sim.run(svc, plc, clk, 3600)
        assert svc.hold.is_holding()
        assert plc.D[118] == svc.hold.value and 480 <= svc.hold.value <= 510
        assert not _cards(chat, "포화") and not _cards(chat, "정체")
        assert not _logs(logs, "DAC 출력 포화") and not _logs(logs, "승온 정체")
        assert abs(plc.T - 200.0) <= 2.0
    asyncio.run(go())


def test_unreachable_300_no_clamp_one_stall_card():
    """300°C(MV 520 으로 도달 불가) — 도달 전이므로 클램프 없이 온도 유지 + 승온 정체 카드 1장."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        await svc.turn_on(300.0)
        seen = {"d118": set(), "pv_max": 0.0}

        def step(s, p):
            seen["d118"].add(p.D[118])
            seen["pv_max"] = max(seen["pv_max"], p.D[110] / 10.0)
        await sim.run(svc, plc, clk, 5400, on_step=step)
        assert seen["d118"] == {520}                                  # 클램프 없음
        assert plc.D[110] / 10.0 >= seen["pv_max"] - 1.0              # 온도가 내려가지 않음
        assert len(_cards(chat, "정체")) == 1 and not _cards(chat, "포화")
    asyncio.run(go())


def test_external_run_ladder_ramp_no_clamp():
    """외부 RUN(파이썬 램프 없음, SV 고정) — 래더 램프로 오르는 동안 클램프하지 않는다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        plc.D[112] = 2200
        plc.M[640] = True                                             # XG5000/HMI 에서 켬
        seen = set()
        await sim.run(svc, plc, clk, 2400, on_step=lambda s, p: seen.add(p.D[118]))
        assert seen == {520} and not _cards(chat, "포화")
        assert plc.D[110] / 10.0 >= 214.0
    asyncio.run(go())


def test_post_arrival_windup_still_clamped_when_hold_off():
    """유지 모드 off · 도달 후 TC 가 낮게 읽힘(와인드업) — 챔버K 사고 보호(직전 평균 클램프)는 그대로 동작."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(hold_mode="off")
        await sim.run(svc, plc, clk, 5)
        await svc.turn_on(200.0)
        await sim.run(svc, plc, clk, 2400)
        plc.tc_offset = -30.0
        await sim.run(svc, plc, clk, 1200)
        assert len(_cards(chat, "포화")) == 1
        assert 485 <= plc.D[118] <= 505
        assert abs(plc.T - 200.0) <= 5.0
    asyncio.run(go())


def test_barely_reachable_248_no_clamp():
    """248°C(간신히 도달) — 도달 시점부터 상한이면 포화로 세지 않는다(유지 모드 포기 카드만)."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        await svc.turn_on(248.0)
        seen = set()
        await sim.run(svc, plc, clk, 7200, on_step=lambda s, p: seen.add(p.D[118]))
        assert seen == {520} and not _cards(chat, "포화")
        assert plc.T >= 247.0
        assert len(_cards(chat, "유지 모드 포기")) == 1
    asyncio.run(go())


def test_dac_hold_shutter_offset_info_only():
    """dac 유지 중 TC 표시만 80°C 낮아짐(셔터 등) — 카드 없이 정보 로그 1줄, 실제 온도 유지."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        await svc.turn_on(200.0)
        await sim.run(svc, plc, clk, 2200)
        assert svc.hold.is_holding()
        n_cards = len(chat.cards)
        plc.tc_offset = -80.0
        await sim.run(svc, plc, clk, 1800)
        assert svc.hold.is_holding() and len(chat.cards) == n_cards
        assert len(_logs(logs, "유지 중 PV")) == 1
        assert abs(plc.T - 200.0) <= 3.0
    asyncio.run(go())


def test_plc_restart_end_reason():
    """운전 중 PLC 재기동(메모리 초기화) — 종료 사유가 '외부'가 아니라 재기동 의심, 설정·마커 재적용."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        await svc.turn_on(150.0)
        await sim.run(svc, plc, clk, 300)
        plc.connected = False
        await sim.run(svc, plc, clk, 20)
        temp = plc.T
        plc.__init__()
        plc.T = temp
        await sim.run(svc, plc, clk, 10)
        ends = _cards(chat, "히터 종료")
        assert len(ends) == 1 and "재기동" in ends[0][2]
        assert plc.D[160] == svc._marker and plc.D[113] == 5000
    asyncio.run(go())


@pytest.mark.parametrize("attr,label", [("water_ok", "냉각수"), ("vac_ok", "진공(메인밸브 인터락)")])
def test_interlock_loss_stops_run_with_cause(attr, label):
    """운전 중 냉각수·진공(메인밸브 인터락)이 끊기면 래더가 RUN 을 끈다(H9) — 종료 사유·카드·준비 판정에 원인이 나오고,
    조건이 돌아와도 저절로 다시 켜지지 않는다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        st = svc.status()
        assert st["water_ok"] and st["vac_ok"] and st["itl"]
        ok, why = await svc.turn_on(150.0)
        assert ok, why
        await sim.run(svc, plc, clk, 60)
        assert plc.M[640] and svc.status()["run"]
        setattr(plc, attr, False)
        await sim.run(svc, plc, clk, 3)
        st = svc.status()
        assert not plc.M[640] and not st["run"] and not st["itl"] and not st[attr]
        assert plc.D[141] == 0 and not plc.M[652]                          # 출력 차단
        ends = _cards(chat, "히터 종료")
        assert len(ends) == 1 and ends[0][2] == f"수동 · 인터락 해제({label}) — PLC 가 RUN 을 끔" and ends[0][3] == "WARN"
        assert _logs(logs, f"운전 종료 — 사유 인터락 해제({label}) — PLC 가 RUN 을 끔")
        assert svc.owner is None
        assert svc.is_ready() == (False, f"인터락 미충족: {label}")
        setattr(plc, attr, True)
        await sim.run(svc, plc, clk, 10)
        assert svc.status()["itl"] and not plc.M[640] and not svc.status()["run"]   # 다시 켜지지 않음
        assert svc.is_ready()[0] and len(_cards(chat, "히터 종료")) == 1
    asyncio.run(go())


def test_interlock_loss_recorded_in_csv(tmp_path):
    """히터 CSV 에 냉각수·진공 조건 열 — 운전 종료 행에 어떤 조건이 끊겼는지 남는다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make(log_enabled=True, log_dir=str(tmp_path))
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0)
        assert ok, why
        await sim.run(svc, plc, clk, 30)
        plc.vac_ok = False
        await sim.run(svc, plc, clk, 3)
    asyncio.run(go())
    files = sorted(tmp_path.glob("CH2_HEATER_*"))
    assert len(files) == 1
    with open(files[0], encoding="utf-8-sig", newline="") as fp:
        rows = list(csv.DictReader(fp))
    assert rows[0]["water_ok"] == "1" and rows[0]["vac_ok"] == "1"
    last = rows[-1]
    assert last["note"] == "운전 종료" and last["itl"] == "0" and last["water_ok"] == "1" and last["vac_ok"] == "0"


def test_brief_interlock_drop_between_polls():
    """폴링 사이에 한 스캔만 풀렸다 돌아온 인터락 — RUN 은 꺼진 채이고, 사유는 '외부 OFF 또는 순간 인터락 해제'."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0)
        assert ok, why
        await sim.run(svc, plc, clk, 30)
        plc.vac_ok = False
        plc.scan(0.0, pulse=False)
        plc.vac_ok = True
        await sim.run(svc, plc, clk, 3)
        st = svc.status()
        assert not st["run"] and st["itl"]
        ends = _cards(chat, "히터 종료")
        assert len(ends) == 1 and ends[0][2] == "수동 · 외부 OFF 또는 순간 인터락 해제(이 프로그램이 끄지 않음)"
        assert ends[0][3] == "INFO"
    asyncio.run(go())


def test_interlock_off_before_start_blocks_turn_on():
    """진공(메인밸브 인터락)이 없으면 켜지 않는다 — 거절 사유에 빠진 조건이 나온다."""
    async def go():
        svc, plc, clk, logs, chat = sim.make()
        plc.vac_ok = False
        plc.water_ok = False
        await sim.run(svc, plc, clk, 5)
        ok, why = await svc.turn_on(150.0)
        assert not ok and "냉각수, 진공(메인밸브 인터락)" in why
        assert not plc.M[640]
    asyncio.run(go())


def test_ladder_h0_accepts_900_950_limits():
    """래더 H0 클램프(D113 9000·D126 9500) — 상한 그대로 쓰면 클램프되지 않는다."""
    async def go():
        plc = sim.Ladder()
        await plc.write_reg(113, 9000)
        await plc.write_reg(126, 9500)
        assert (plc.D[113], plc.D[126]) == (9000, 9500)
    asyncio.run(go())


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
