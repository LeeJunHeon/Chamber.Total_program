"""챔버12 장비 사실 표(v2/ch12/spec/devices.py) 시험 — 주소·포트가 params 기본값과 같은지, 모든 드라이버에 BUDGET 이 있는지,
빈 칸·근거 없음이 없는지, 1초 예산·PC 사망 표·미확인 목록의 모양."""
import importlib
import pkgutil
import re

from v2.common.paramdef import defaults
from v2.ch12 import cmd_guard as G
from v2.ch12 import drivers as DRV
from v2.ch12.params import PARAMS
from v2.ch12.spec import devices as D
from v2.ch12.spec.devices_v1facts import FACTS

CFG = defaults(PARAMS)
SRC = re.compile(r"[\w/]+\.(py|json):\d+|plan\.py|챔버K")


def test_endpoints_match_params_defaults():
    for e in D.ENDPOINTS:
        assert CFG[e.host_key] == e.host, (e.device, e.chamber, e.host_key)
        assert int(CFG[e.port_key]) == e.port, (e.device, e.chamber, e.port_key)
        assert e.protocol and e.idle_60s and e.keepalive and SRC.search(e.source), e
        assert e.via_nport == (e.host == "192.168.1.50")
    keys = [(e.device, e.chamber) for e in D.ENDPOINTS]
    assert len(keys) == len(set(keys))
    assert D.endpoint("plc", "shared").port == 502 and D.endpoint("plc", "shared").host == "192.168.1.2"


def test_endpoints_match_v1_facts_with_corrections():
    """devices_v1facts 와 같은 주소·포트(정정 사항 제외) — CH1 RF/DC Pulse 4007 공유, TSP keepalive false(D20)."""
    for dev in ("mfc", "ig", "dcpulse", "rfpulse"):
        ports = FACTS[dev]["connection"]["port"]
        for ch in ("ch1", "ch2"):
            assert D.endpoint(dev, ch).port == ports[ch], (dev, ch)
    assert D.endpoint("ig", "tsp").port == FACTS["ig"]["connection"]["port"]["tsp"] == D.endpoint("ig", "ch1").port
    assert D.endpoint("dcpulse", "ch1").port == D.endpoint("rfpulse", "ch1").port == 4007
    assert D.endpoint("tsp", "shared").port == FACTS["tsp"]["connection"]["port"]
    assert "false" in D.endpoint("tsp", "shared").keepalive and CFG["TSP_TCP_KEEPALIVE"] is False
    assert D.endpoint("stm100", "ch2").port == FACTS["stm100"]["connection"]["port"]["ch2"]
    assert {c[3] for c in D.CORRECTIONS} >= {"D20", "D192·D140"}
    assert D.LAN["nport_ports"][4007].startswith("CH1 DC Pulse/RF Pulse")


def _driver_modules():
    out = []
    for m in pkgutil.iter_modules(DRV.__path__):
        mod = importlib.import_module(f"v2.ch12.drivers.{m.name}")
        if callable(getattr(mod, "blocks", None)):             # 1초 행 블록을 내는 드라이버 모듈
            out.append(mod)
    return out


def test_every_driver_has_budget_and_is_in_tick_plan():
    mods = _driver_modules()
    assert len(mods) >= 12
    planned = {t.module for t in D.TICK_PLAN}
    for mod in mods:
        b = getattr(mod, "BUDGET", None)
        assert isinstance(b, dict), mod.__name__
        for k in D.BUDGET_KEYS:
            assert k in b, (mod.__name__, k)
        assert isinstance(b["fits_1s"], bool) and b["note"]
        assert mod.__name__ in planned, mod.__name__
    for t in D.TICK_PLAN:
        mod = importlib.import_module(t.module)
        assert t.budget is mod.BUDGET and t.basis
    assert len({t.device for t in D.TICK_PLAN}) == len(D.TICK_PLAN)


def test_alternate_for_devices_not_fitting_1s():
    for t in D.TICK_PLAN:
        if not t.budget["fits_1s"]:
            assert D.ALTERNATE.get(t.device), t.device
    # 사용자 결정 2026-11-02 D36·D50 — MFC·IG·RF Pulse·DC Pulse 는 모두 매 tick 전부 읽어 번갈아 읽기·1초 밖 목록에 없다
    assert not ({"mfc", "ig", "rfpulse", "dcpulse"} & set(D.ALTERNATE))
    by = {t.device: t for t in D.TICK_PLAN}
    assert all(by[d].budget["fits_1s"] and not by[d].budget.get("alternate") for d in ("mfc", "ig", "rfpulse", "dcpulse"))
    assert (by["mfc"].budget["queries_per_tick"], by["rfpulse"].budget["queries_per_tick"],
            by["dcpulse"].budget["queries_per_tick"]) == (9, 3, 5)
    assert all(len(r) == 7 for r in D.budget_table())


def test_devices_commands_have_v1_sources():
    keys = [d.key for d in D.DEVICES]
    assert len(keys) == len(set(keys))
    assert set(G.DEVICES) <= set(keys) and {"dc", "rf"} <= set(keys)
    for d in D.DEVICES:
        assert d.name and d.model and d.gaps and d.timeouts and d.commands
        for c in d.commands:
            assert c.name and c.wire and c.reply and c.kind in ("read", "write")
            assert SRC.search(c.source), (d.key, c.name, c.source)


def test_pc_dead_table():
    by = {p.target: p for p in D.PC_DEAD}
    assert all(p.who_turns_off in ("software", "plc_ladder", "device") for p in D.PC_DEAD)
    for t in ("RF 연속", "DC 연속", "DC Pulse", "RF Pulse"):
        p = next(v for k, v in by.items() if k.startswith(t))
        assert p.who_turns_off == "software", t                 # RF/DC 파워에는 PLC 인터락 없음
    gas = next(v for k, v in by.items() if k.startswith("가스"))
    assert gas.who_turns_off == "plc_ladder" and "PLC" in gas.on_pc_dead
    heater = next(v for k, v in by.items() if "히터" in k)
    assert heater.confirmed and "D114" in heater.on_pc_dead and "30 s" in heater.on_pc_dead
    assert len(D.SOFTWARE_MUST_OFF) == 4


def test_unverified_list():
    assert len(D.UNVERIFIED) >= 10
    for u in D.UNVERIFIED:
        assert u.device and u.item and u.v1 and u.how
    assert any("4008" in u.item for u in D.UNVERIFIED) and any("4007" in u.item for u in D.UNVERIFIED)


def test_intervals_match_params_and_have_manual_basis():
    """사용자 결정 2026-11-02 D36·D50 — 장비마다 간격 설정 키·기본값(params 와 같음)·매뉴얼 근거(문서·쪽)·매 tick 읽는 질의."""
    by = {i.device: i for i in D.INTERVALS}
    assert set(by) == {"mfc", "ig", "rfpulse", "dcpulse"}
    for i in D.INTERVALS:
        assert CFG[i.key] == i.default_ms, i.key
        assert i.basis and i.reads
    assert (by["mfc"].default_ms, by["ig"].default_ms, by["rfpulse"].default_ms, by["dcpulse"].default_ms) == (50, 200, 0, 150)
    assert "p.72" in by["mfc"].basis and "4-48" in by["rfpulse"].basis and "근거 문서 없음" in by["ig"].basis
    for d in D.DEVICES:                                            # 장비 사실 표의 간격 문구에도 새 값
        if d.key in by:
            assert str(by[d.key].default_ms) in d.gaps, d.key


def test_readback_table_matches_params_and_lists_commands_without_readback():
    """값을 바꾸는 명령 통일 — 되읽기 질의가 있는 명령은 설정 키(READBACK_ENABLED·REWRITE_MAX·REWRITE_GAP_MS)가 params 에 있고,
    없는 명령은 이유와 함께 READBACK_NONE 에 있다."""
    assert {r.device for r in D.READBACK} == {"mfc", "ig", "dcpulse", "rfpulse", "plc"}
    for r in D.READBACK:
        for k in re.split(r"[·,]", r.keys):
            assert k.strip() in CFG, (r.device, k)
        assert r.command and r.query and r.compare and r.source
    assert {n[0] for n in D.READBACK_NONE} >= {"mfc", "ig", "dcpulse", "rfpulse", "plc"}
    assert all(len(n) == 3 and n[2] for n in D.READBACK_NONE)
