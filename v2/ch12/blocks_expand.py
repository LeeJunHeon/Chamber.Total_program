"""챔버12 블록 — 기본 템플릿(v1 행 → 블록)과 펼치기(블록 → 스텝). 정의·검사는 blocks.py.

template_blocks(p, chamber, cfg): v1 공정 params(recipe.normalize_row 결과 = v1 ProcessController 입력)
  → v1 _create_process_sequence(controller/process_controller.py:2153-2735) 와 같은 순서·조건의 블록 목록 + v1 로그.
  펼치면 process_sequence.build_sequence 와 같은 스텝이 나온다(같은 스텝 조각 함수를 쓴다 — 골든 시험).
template_from_rows(rows, chamber, cfg): v1 레시피 파일 행 전부 → 블록 목록(행마다 "row" = 행 순번).
  delay 행 → row_delay, TEST 행 → test, 공정 행 → 템플릿(끝 정지 포함).

expand(blocks, cfg, chamber): 블록 → (스텝, BlockInst 목록). 스텝마다 block(블록 순번)·ref·holding.
  end_stop 은 그 구간(앞 end_stop 다음부터) 블록이 쓴 장비를 끈다(segment_uses). 목록 끝이 end_stop 이 아니고 그 뒤 구간에
  장비를 건드리는 블록이 있으면 자동 끝 정지를 덧붙인다(auto).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional

from v2.ch12 import process_steps as ps
from v2.ch12.blocks import AUTO_END_STOP_LABEL, by_type
from v2.ch12.process_steps import Caps, ProcessStep, StepError, Uses

# "cfg:키" → StepCfg 필드(채널 키 CHn_키 를 v1 기본값과 함께 읽은 값)
_CFG_FIELDS = {"PROCESS_DEFAULT_BASE_PRESSURE": "base_pressure", "PC_PRESSURE_WAIT_TIMEOUT_S": "pressure_wait_timeout_s",
               "PC_WORKING_PRESSURE_BOOST_TARGET": "boost_target",
               "PROCESS_DEFAULT_OES_INTEGRATION_MS": "integration_ms"}


@dataclass
class BlockInst:
    index: int
    no: str
    type: str
    label: str
    params: Dict[str, Any]
    row: Optional[int] = None
    auto: bool = False
    first_step: int = 0
    n_steps: int = 0

    def record(self) -> dict:
        return {"no": self.no, "type": self.type, "label": self.label, "params": dict(self.params), "row": self.row,
                "auto": self.auto}


def _empty(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def resolved_params(b: Mapping[str, Any], sc: ps.StepCfg) -> Dict[str, Any]:
    """비운 값은 정의 기본값, "cfg:키" 는 지금 설정값."""
    d = by_type()[b["type"]]
    out = {}
    for bp in d.params:
        v = (b.get("params") or {}).get(bp.name)
        if _empty(v) or (bp.type == "parts" and v == []):
            v = bp.default
        if isinstance(v, str) and v.startswith("cfg:"):
            v = getattr(sc, _CFG_FIELDS[v[4:]])
        out[bp.name] = v
    return out


def segment_uses(blocks: List[dict], sc: ps.StepCfg) -> Uses:
    """블록 구간이 쓴 장비 — 끝 정지·STOP 이 끌 것(v1 은 행 params: _create_shutdown_sequence 2737-2745)."""
    u = Uses()
    for b in blocks:
        t, p = b["type"], resolved_params(b, sc)
        if t == "power_on":
            u.dc |= _num(p["dc_power"]) and p["dc_power"] > 0
            u.dc2 |= _num(p["dc2_power"]) and p["dc2_power"] > 0
            u.rf |= _num(p["rf_power"]) and p["rf_power"] > 0
            u.dcp |= _num(p["dcp_power"]) and p["dcp_power"] > 0
            u.rfp |= _num(p["rfp_power"]) and p["rfp_power"] > 0
        elif t in ("gas_prep", "gas_intro"):
            for g in ("ar", "o2", "n2"):
                if p.get(g) is True:
                    u.gases[g.upper()] = True
        elif t == "shutter" and p["action"] == "open":
            for g in p["targets"] or ():
                if g != "ms":
                    u.guns[g.upper()] = True
        elif t == "power_select":
            u.power_select = True
    return u


def _block_steps(t: str, p: Dict[str, Any], label: str, sc: ps.StepCfg, ctx: dict, seg: List[dict]) -> List[ProcessStep]:
    if t == "heater_set":
        ctx["heater_target"] = float(p["target_c"])
        return ps.heater_set_steps(float(p["target_c"]), p["ramp_c_per_min"])
    if t == "heater_wait":
        temp = p["target_c"] if _num(p["target_c"]) else ctx.get("heater_target")
        if temp is None:
            raise ValueError("히터 도달 대기: 목표 없음(앞에 히터 설정 블록이 없고 target_c 도 비었다)")
        return ps.heater_wait_steps(float(temp), p["soak_min"])
    if t == "base_wait":
        return ps.base_wait_steps(float(p["target_torr"]))
    if t == "rga_scan":
        return ps.rga_steps()
    if t in ("gas_prep", "gas_intro"):
        used = {g.upper(): p[g] is True for g in ("ar", "o2", "n2")}
        if t == "gas_prep":
            return ps.gas_prep_steps(sc, used)
        flows = {g.upper(): (p[g + "_flow"] if _num(p[g + "_flow"]) else 0) for g in ("ar", "o2", "n2")}
        return ps.gas_intro_steps(sc, used, flows)
    if t == "pressure_ctrl":
        return ps.pressure_ctrl_steps(sc, float(p["target"]), float(p["timeout_sec"]))
    if t == "pressure_boost":
        return ps.boost_steps(float(p["value"]), float(p["timeout_sec"]))
    if t == "pressure_set":
        return ps.sp1_steps(float(p["value"]))
    if t == "shutter":
        return ps.shutter_steps(sc, p["action"], list(p["targets"] or ()))
    if t == "power_select":
        return ps.power_select_steps()
    if t == "power_on":
        return ps.power_on_steps(sc, dc=p["dc_power"], dc2=p["dc2_power"], dcp=p["dcp_power"], dcp_freq=p["dcp_freq_khz"],
                                 dcp_off=p["dcp_off_us"], rfp=p["rfp_power"], rfp_freq_khz=p["rfp_freq_khz"],
                                 rfp_duty=p["rfp_duty"], rf=p["rf_power"])
    if t == "wait":
        return ps.delay_steps(float(p["sec"]), label or f"대기 {float(p['sec']):g}s")
    if t == "pulse_set":
        return ps.pulse_set_steps(p["kind"], float(p["power"]), label or ps.pulse_set_message(p["kind"], p["power"]),
                                  bool(p["polling"]))
    if t == "thickness_zero":
        return ps.thickness_zero_steps(sc)
    if t == "main":
        ch = None
        if _num(p["change_after_sec"]):
            ch = {"after_sec": float(p["change_after_sec"]), "kind": p["change_kind"], "power": float(p["change_power"]),
                  "message": p["change_label"] or ps.pulse_set_message(p["change_kind"], p["change_power"])}
        return ps.main_steps(float(p["min"]), int(p["integration_ms"]), ch)
    if t == "thickness_main":
        return ps.thickness_main_steps(sc, float(p["target_nm"]), float(p["dep_rate"]), int(p["integration_ms"]))
    if t == "heater_hold":
        return ps.heater_hold_steps()
    if t == "test":
        return ps.row_test_steps(float(p["sec"]))
    if t == "row_delay":
        return [ProcessStep("ROW_DELAY", f"'{p['name']}' 단계: {p['label']} 대기", duration=int(float(p["sec"])) * 1000,
                            params=(p["label"], p["name"]), holding=True)]
    if t == "end_stop":
        return ps.shutdown_steps(sc, segment_uses(seg, sc), skip=tuple(p["skip"] or ()))
    raise ValueError(f"모르는 블록 {t!r}")


def expand(blocks: List[dict], cfg: Mapping[str, Any], chamber: int, *, caps: Optional[Caps] = None) -> tuple:
    sc = ps.step_cfg(cfg, chamber)
    defs = by_type()
    steps: List[ProcessStep] = []
    insts: List[BlockInst] = []
    ctx: dict = {}
    seg: List[dict] = []

    def one(b: Mapping[str, Any], no: str, auto: bool = False) -> None:
        p = resolved_params(b, sc)
        out = _block_steps(b["type"], p, b.get("label") or "", sc, ctx, seg)
        inst = BlockInst(len(insts), no, b["type"], b.get("label") or "", p, b.get("row"), auto, len(steps), len(out))
        for s in out:
            s.block = inst.index
        insts.append(inst)
        steps.extend(out)

    for i, b in enumerate(blocks, 1):
        one(b, str(i))
        if b["type"] == "end_stop":
            seg = []
        else:
            seg.append(b)
    if (not blocks or blocks[-1]["type"] != "end_stop") and any(defs[b["type"]].touches for b in seg):
        one({"type": "end_stop", "params": {}, "label": AUTO_END_STOP_LABEL}, str(len(blocks) + 1), auto=True)
    return steps, insts


# ───────────── 기본 템플릿(v1 행) ─────────────
def _B(t: str, label: str = "", **params) -> dict:
    return {"type": t, "params": params, "label": label}


def template_blocks(p: Mapping[str, Any], chamber: int, cfg: Mapping[str, Any], *, caps: Optional[Caps] = None) -> tuple:
    """(블록 목록, 로그 [(src, msg)]) — process_sequence.build_sequence 와 같은 순서·조건·로그."""
    caps = caps or ps.caps_from_cfg(cfg, chamber)
    sc = ps.step_cfg(cfg, chamber)
    log: list = []
    if bool(p.get("test_mode", False)):
        from v2.ch12.process_sequence import row_test_duration_sec
        dur = row_test_duration_sec(p)
        log.append(("Process", f"[TEST MODE] 장비제어 스킵 / 시뮬레이션 {dur:.1f}s 진행"))
        return [_B("test", sec=dur)], log
    from v2.ch12.process_sequence import mid_change_plan, thickness_conflict
    info = ps.common_info(p, caps)
    bp = float(p.get("base_pressure", sc.base_pressure))
    wp = float(p.get("working_pressure", 0))
    pt_min = float(p.get("process_time", 0))
    sd_min = float(p.get("shutter_delay", 0))
    sd_sec, pt_sec = sd_min * 60.0, pt_min * 60.0
    thk = bool(p.get("thickness_mode", False))
    if thk:
        c = thickness_conflict(p, caps)
        if c:
            raise StepError("E844", c)
    try:
        ims = int(float(p.get("integration_time", sc.integration_ms)))
    except Exception:
        ims = sc.integration_ms
    mid = mid_change_plan(p, info, caps, log)
    out: List[dict] = []
    log.append(("Process", "공정 시작"))
    heater = bool(p.get("use_heater", False))
    if heater:
        temp = float(p.get("heater_temp") or 0.0)
        _hr, _hs = p.get("heater_ramp"), p.get("heater_soak")
        ramp = float(_hr) if _hr not in (None, "") else None
        soak = float(_hs) if _hs not in (None, "") else None
        log.append(("Process", ps.heater_text(temp, ramp, soak)))
        out += [_B("heater_set", target_c=temp, ramp_c_per_min=ramp), _B("heater_wait", target_c=None, soak_min=soak)]
    out += [_B("base_wait", target_torr=bp), _B("rga_scan")]
    g = {k: bool(p.get(f"use_{k}", False)) for k in ("ar", "o2", "n2")}
    out.append(_B("gas_prep", **g))
    out.append(_B("gas_intro", ar=g["ar"], ar_flow=float(p.get("ar_flow", 0)) if g["ar"] else None,
                  o2=g["o2"], o2_flow=float(p.get("o2_flow", 0)) if g["o2"] else None,
                  n2=g["n2"], n2_flow=float(p.get("n2_flow", 0)) if g["n2"] else None))
    out.append(_B("pressure_ctrl", target=wp, timeout_sec=sc.pressure_wait_timeout_s))
    guns = [x.lower() for x in sc.gun_shutters if p.get(f"use_{x.lower()}", False)]
    if guns:
        out.append(_B("shutter", "Gun Shutter 열기", action="open", targets=guns))
    if bool(p.get("use_power_select", False)) and sc.chamber == 2:
        out.append(_B("power_select"))
        ms = sc.power_select_settle_ms
        if ms > 0:
            out.append(_B("wait", f"Power Select 안정화 대기 ({ms / 1000:.0f}초)", sec=ms / 1000.0))
    log += [("Process", m) for m in ps.unsupported_logs(info)]
    from v2.ch12.process_sequence import power_kwargs
    kw = power_kwargs(p, info)
    if any(_num(kw[k]) and kw[k] > 0 for k in ("dc", "dc2", "dcp", "rfp", "rf")):
        out.append(_B("power_on", dc_power=kw["dc"], dc2_power=kw["dc2"], dcp_power=kw["dcp"],
                      dcp_freq_khz=kw["dcp_freq"] if info["use_dc_pulse"] else None,
                      dcp_off_us=kw["dcp_off"] if info["use_dc_pulse"] else None, rfp_power=kw["rfp"],
                      rfp_freq_khz=(float(kw["rfp_freq_khz"]) if kw["rfp_freq_khz"] is not None else None)
                      if info["use_rf_pulse"] else None,
                      rfp_duty=kw["rfp_duty"] if info["use_rf_pulse"] else None, rf_power=kw["rf"]))
    if info["use_rf_pulse"]:
        ms = sc.rf_pulse_post_on_delay_ms
        out.append(_B("wait", f'Power Delay {ms / 1000.0:.0f}초', sec=ms / 1000.0))
    if wp < sc.boost_target:
        out.append(_B("pressure_boost", value=sc.boost_target, timeout_sec=sc.pressure_wait_timeout_s))
    out.append(_B("pressure_set", value=wp))
    if heater:
        out.append(_B("heater_hold"))
    if sd_sec > 0:
        if mid and mid["t"] < sd_sec:
            p1, p2 = float(mid["t"]), float(sd_sec - mid["t"])
            if p1 > 0:
                out.append(_B("wait", f'Shutter Delay (변경 전) {p1 / 60.0:.2f}분', sec=p1))
            out.append(_B("pulse_set", ps.pulse_set_message(mid["kind"], mid["power"], mid["t"], "ShutterDelay"),
                          kind=mid["kind"], power=mid["power"], polling=False))
            if p2 > 0:
                out.append(_B("wait", f'Shutter Delay (변경 후) {p2 / 60.0:.2f}분', sec=p2))
        else:
            out.append(_B("wait", f'Shutter Delay {sd_min}분', sec=sd_sec))
    if thk:
        out.append(_B("thickness_zero"))
    if info["use_ms"]:
        out.append(_B("shutter", "Main Shutter 열기", action="open", targets=["ms"]))
    if thk:
        out.append(_B("thickness_main", target_nm=float(p.get("thickness") or 0.0), dep_rate=float(p.get("dep_rate") or 0.0),
                      integration_ms=ims))
    elif pt_sec > 0:
        change = {}
        if mid and sd_sec <= mid["t"] < sd_sec + pt_sec:
            change = dict(change_after_sec=float(mid["t"] - sd_sec), change_kind=mid["kind"], change_power=mid["power"],
                          change_label=ps.pulse_set_message(mid["kind"], mid["power"], mid["t"], "Main"))
        out.append(_B("main", min=pt_min, integration_ms=ims, **change))
    out.append(_B("end_stop", skip=None))
    log.append(("Process", ps.SHUTDOWN_LOG))
    return out, log


@dataclass
class RowTemplate:
    row: int                          # 1 부터(파일의 공정 행 순서)
    kind: str                         # delay · test · process · error
    blocks: List[dict] = field(default_factory=list)
    logs: List[tuple] = field(default_factory=list)
    params: Optional[dict] = None     # 정규화한 공정 params(process·test)
    error: str = ""                   # E844 등


def template_from_row(raw: Mapping[str, Any], chamber: int, cfg: Mapping[str, Any], *, caps: Optional[Caps] = None,
                      row_no: int = 1, ui_targets: Optional[Mapping[str, str]] = None) -> RowTemplate:
    """v1 레시피 한 행 → 블록(delay 판정 → TEST 표시 → 정규화 → Thickness 모드 판정 → 템플릿)."""
    from v2.ch12.recipe import normalize_row, plan_row, thickness_mode_of
    caps = caps or ps.caps_from_cfg(cfg, chamber)
    plan = plan_row(raw)
    if plan.kind == "delay":
        return RowTemplate(row_no, "delay", [{"type": "row_delay", "params": {"sec": plan.delay_s, "label": plan.label,
                                                                              "name": plan.disp}, "label": "", "row": row_no}])
    logs: list = []
    p = normalize_row(plan.raw, chamber, caps, ui_targets=ui_targets, log=logs)
    if not plan.test and thickness_mode_of(p.get("thickness"), caps.thickness):
        p["thickness_mode"] = True
    try:
        blocks, tlog = template_blocks(p, chamber, cfg, caps=caps)
    except StepError as e:
        return RowTemplate(row_no, "error", [], logs, p, f"{e.code}: {e.detail}")
    for b in blocks:
        b["row"] = row_no
    return RowTemplate(row_no, "test" if plan.test else "process", blocks, logs + tlog, p)


def template_from_rows(rows: List[Mapping[str, Any]], chamber: int, cfg: Mapping[str, Any], *,
                       caps: Optional[Caps] = None) -> tuple:
    """v1 레시피 파일 행 전부 → (블록 목록, 행별 RowTemplate). 블록의 row 로 행 경계(새 로그·쿨다운 — 엔진)를 안다."""
    rts = [template_from_row(r, chamber, cfg, caps=caps, row_no=i) for i, r in enumerate(rows, 1)]
    return [b for rt in rts for b in rt.blocks], rts
