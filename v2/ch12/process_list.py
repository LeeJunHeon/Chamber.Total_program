"""챔버12 공정 리스트 — 블록 목록을 v1 의 '행 하나 = 컨트롤러 런 하나'로 나눈다(사용자 지시 3장 (3)).

v1(runtime/chamber_runtime.py:5044-5244 _runner_stage_advance_queue): 레시피 파일의 행마다 delay 행이면 러너 대기(장비·새 로그 없음),
TEST 행이면 장비 제어 없는 DELAY 1단계, 공정 행이면 정규화·값 검사 → 새 공정 txt → 쿨다운(첫 행 빼고) → 프리플라이트 → 컨트롤러 런.
v2 는 레시피가 블록 목록이고 v1 행은 blocks_expand.template_from_rows 로 행마다 블록이 된다(블록의 "row" = 행 순번).
- plan_runs: 연속한 같은 row 의 블록 = 런 하나(row 없는 블록은 앞 런에 붙는다 — 공정 중 편집으로 넣은 블록). 블록 레시피(row 없음)는
  런 하나. 런 종류: delay(row_delay 블록 하나) · test(test 블록 하나) · error(행 변환 실패 — E844) · process.
- 런마다 따로 펼친다(blocks.expand) — 그래서 런 끝에는 늘 정지 순서가 있다(끝 정지 블록이 없거나 편집으로 지워져도 자동 끝 정지).
- derive_params: 블록에서 v1 공정 params 의 사용 플래그·값(폴링 타깃·카메라·기록 열이 본다). v1 행은 행 params 를 그대로 쓰고
  공정 중 편집으로 그 런이 바뀌면 블록에서 다시 만든 값을 덮는다.
- stop_uses: STOP·이상 중단의 정지 순서가 끌 장비 = 지금 블록이 속한 구간(앞 끝 정지 다음 ~ 다음 끝 정지 전)의 블록이 쓰는 것
  (v1 은 행 params 전체 — _create_shutdown_sequence(current_params)).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional

from v2.ch12.blocks_expand import resolved_params, segment_uses


@dataclass
class RunPlan:
    kind: str                          # process · test · delay · error
    first: int                         # 블록 목록에서 첫 블록 위치(0 부터)
    last: int                          # 마지막 블록 위치(포함)
    row: Optional[int] = None
    params: Dict[str, Any] = field(default_factory=dict)
    raw: Optional[dict] = None         # v1 원본 행(ETA min_total·값 검사)
    logs: List[tuple] = field(default_factory=list)
    error: str = ""
    delay_s: float = 0.0
    label: str = ""
    disp: str = ""

    def blocks(self, blocks: List[dict]) -> List[dict]:
        return blocks[self.first:self.last + 1]


def plan_runs(blocks: List[dict], rts: Optional[list] = None, rows: Optional[list] = None) -> List[RunPlan]:
    """블록 → 런 목록. rts = template_from_rows 의 RowTemplate(행 params·로그·오류), rows = v1 원본 행."""
    by_row = {rt.row: rt for rt in (rts or [])}
    out: List[RunPlan] = []
    cur: Optional[RunPlan] = None
    for i, b in enumerate(blocks):
        r = b.get("row")
        if cur is None or (r is not None and r != cur.row):
            cur = RunPlan("process", i, i, r)
            out.append(cur)
        else:
            cur.last = i
    for p in out:
        bl = p.blocks(blocks)
        rt = by_row.get(p.row)
        if rt is not None:
            p.params = dict(rt.params or {})
            p.logs = list(rt.logs or [])
            p.raw = dict(rows[p.row - 1]) if rows and 0 < p.row <= len(rows) else None
        if rt is not None and rt.kind == "error":
            p.kind, p.error = "error", rt.error
        elif len(bl) == 1 and bl[0]["type"] == "row_delay":
            q = bl[0]["params"]
            p.kind, p.delay_s, p.label, p.disp = "delay", float(q["sec"]), str(q["label"]), str(q["name"])
        elif len(bl) == 1 and bl[0]["type"] == "test":
            p.kind = "test"
            if not p.params:
                sec = float(bl[0]["params"]["sec"])
                p.params = {"test_mode": True, "test_duration_sec": sec, "process_time": round(sec / 60.0, 3)}
            p.params["test_mode"] = True
        if not p.params and p.kind == "process":
            p.params = {}
    return out


def derive_params(blocks: List[dict], sc) -> Dict[str, Any]:
    """블록 → v1 공정 params 의 사용 플래그·값(없는 값은 넣지 않음)."""
    out: Dict[str, Any] = {"use_ms": False, "use_power_select": False, "use_heater": False}
    for g in ("ar", "o2", "n2"):
        out[f"use_{g}"] = False
    for g in ("g1", "g2", "g3"):
        out[f"use_{g}"] = False
    for b in blocks:
        t, p = b["type"], resolved_params(b, sc)
        if t in ("gas_prep", "gas_intro"):
            for g in ("ar", "o2", "n2"):
                if p.get(g) is True:
                    out[f"use_{g}"] = True
                    if t == "gas_intro" and isinstance(p.get(g + "_flow"), (int, float)):
                        out[f"{g}_flow"] = float(p[g + "_flow"])
        elif t == "shutter" and p["action"] == "open":
            for x in p["targets"] or ():
                out["use_ms" if x == "ms" else f"use_{x}"] = True
        elif t == "power_select":
            out["use_power_select"] = True
        elif t == "power_on":
            for k, uk, vk in (("dc_power", "use_dc_power", "dc_power"), ("dc2_power", "use_dc_power2", "dc_power2"),
                              ("rf_power", "use_rf_power", "rf_power"), ("dcp_power", "use_dc_pulse", "dc_pulse_power"),
                              ("rfp_power", "use_rf_pulse", "rf_pulse_power")):
                if isinstance(p.get(k), (int, float)) and p[k] > 0:
                    out[uk], out[vk] = True, float(p[k])
            if p.get("dcp_freq_khz") is not None:
                out["dc_pulse_freq"] = p["dcp_freq_khz"]
            if p.get("dcp_off_us") is not None:
                out["dc_pulse_off_time"] = p["dcp_off_us"]
            if p.get("rfp_freq_khz") is not None:
                out["rf_pulse_freq"] = p["rfp_freq_khz"]
            if p.get("rfp_duty") is not None:
                out["rf_pulse_duty_cycle"] = p["rfp_duty"]
        elif t == "base_wait":
            out["base_pressure"] = p["target_torr"]
        elif t == "pressure_set":
            out["working_pressure"] = p["value"]
        elif t == "main":
            out["process_time"] = float(p["min"])
        elif t == "thickness_main":
            out["thickness"], out["dep_rate"], out["thickness_mode"] = p["target_nm"], p["dep_rate"], True
        elif t == "heater_set":
            out["use_heater"], out["heater_temp"] = True, p["target_c"]
            if p.get("ramp_c_per_min") is not None:
                out["heater_ramp"] = p["ramp_c_per_min"]
        elif t == "heater_wait" and p.get("soak_min") is not None:
            out["heater_soak"] = p["soak_min"]
    return out


USE_KEYS = ("use_ms", "use_power_select", "use_heater", "use_ar", "use_o2", "use_n2", "use_g1", "use_g2", "use_g3",
            "use_dc_power", "use_dc_power2", "use_rf_power", "use_dc_pulse", "use_rf_pulse", "dc_power", "dc_power2",
            "rf_power", "dc_pulse_power", "rf_pulse_power")


def stop_uses(blocks: List[dict], cur: int, sc):
    """지금 블록(cur — 런 안 위치)이 속한 구간의 장비. cur 가 끝 정지면 그 앞 구간."""
    lo = 0
    for i in range(min(cur, len(blocks)) - 1, -1, -1):
        if blocks[i]["type"] == "end_stop":
            lo = i + 1
            break
    if 0 <= cur < len(blocks) and blocks[cur]["type"] == "end_stop":
        return segment_uses(blocks[lo:cur], sc)
    hi = len(blocks)
    for i in range(max(cur, 0), len(blocks)):
        if blocks[i]["type"] == "end_stop":
            hi = i
            break
    return segment_uses(blocks[lo:hi], sc)


def next_process_uses_heater(runs: List[RunPlan], idx: int) -> bool:
    """_next_queue_row_uses_heater(7109-7121) — 다음 '공정' 런(delay 건너뜀, TEST 는 안 씀)이 use_heater=T 인가."""
    for p in runs[idx + 1:]:
        if p.kind == "delay":
            continue
        if p.kind == "test":
            return False
        return bool(p.params.get("use_heater", False))
    return False


def first_process_index(runs: List[RunPlan]) -> int:
    return next((i for i, p in enumerate(runs) if p.kind != "delay"), -1)


def raw_rows_from(runs: List[RunPlan], start: int) -> List[Mapping[str, Any]]:
    """ETA min_total 용 원본 행(rows[현재 인덱스:]) — 블록 레시피(원본 없음)면 빈 목록."""
    return [p.raw for p in runs[max(0, start):] if p.raw is not None]
