"""챔버12 웹 검사 골든 만들기 — 포털(vanam-process lib/ops2/ch12 TS)이 장비와 같은 검사 결과를 내는지 확인용(챔버K W-4b 방식).

python -m v2.tools.make_web_golden_ch12                → v2/tests/golden/web_ch12_*.json 세 개를 다시 쓴다(손으로 고치지 않음).
python -m v2.tools.make_web_golden_ch12 --copy <포털>   → 위 파일을 <포털>/lib/ops2/ch12/golden/ 에 복사(머리 _출처 = 이 저장소
                                                         커밋 해시·작업 트리 깨끗한지·정의 해시). 커밋한 뒤 다시 복사하면 해시가 맞다.
모든 파일 공통 머리: {"_설명", "defs"(web_blocks.definitions() — 웹이 검사에 쓰는 정의), "defsHash", "cfg"(기준 설정 = v2 파라미터
  표 기본값 중 검사가 읽은 키)}. 경우마다 "cfgOver"(기준 설정에 덮어쓸 값 — null 은 키 없음과 같다).

1) web_ch12_blocks.json — 블록 검사(blocks.check_blocks)·시퀀스 편집(blocks.edit_sequence)
   blockCases: {name, covers(규칙 꼬리표), ch, blocks(입력 — 형식이 틀린 것도 그대로), cfgOver,
                clean(정리된 블록 또는 null), errors, warnings(문구 그대로), autoEndStop(펼치면 자동 끝 정지가 붙는가 — clean 이 있을 때)}
   editCases:  {name, covers, ch, blocks, startedUpto, op, rev, cfgOver, ok, result(blocks 또는 null), rev2(결과 판 번호), reason,
                errors, warnings}
2) web_ch12_template.json — v1 행 → 기본 템플릿 블록(blocks_expand.template_from_row; caps 는 설정에서 caps_from_cfg)
   cases: {name, ch, raw(v1 행 — 값은 CSV 처럼 글), cfgOver, kind(delay·test·process·error), params(정규화 — delay 행 null),
           logs([src, msg]), blocks, error("E844: …" 또는 ""), normErrors(recipe_check.check_norm_params — delay 행 null),
           exception(파이썬 예외 이름 — 있으면 나머지는 null)}
   열 조합은 v2/tests/golden/process_combos.json(v1 을 실제로 돌린 435 경우)의 입력을 그대로 쓰고 caps 바꿈은 설정으로 옮긴다.
3) web_ch12_recipe_check.json — E230 행 값 검사(recipe_check.check_raw_rows·e230_message·e230_popup)·시작 전 값 검사
   rawCases:  {name, rows, rowNumbers, errors, message, popup}
   normCases: {name, ch, caps, params, errors}(recipe_check.json 의 51 경우 + 추가)
시험 v2/tests/test_web_golden_ch12.py 가 파일 = 지금 코드로 만든 것인지·꼬리표·규칙 id·블록 종류가 다 나오는지 본다.
"""
from __future__ import annotations

import copy
import dataclasses
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from v2.ch12 import blocks as BK
from v2.ch12 import process_steps as PS
from v2.ch12 import recipe_check as CK
from v2.ch12 import web_blocks as WB
from v2.ch12.blocks_expand import template_from_row
from v2.ch12.params import table

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "v2" / "tests" / "golden"
FILES = ("web_ch12_blocks.json", "web_ch12_template.json", "web_ch12_recipe_check.json")
PORTAL_DIR = Path("lib") / "ops2" / "ch12" / "golden"
# 골든 결과를 정하는 파일(포털 사본 머리 dirty 판단)
SOURCES: tuple = ("v2/ch12/blocks.py", "v2/ch12/blocks_expand.py", "v2/ch12/recipe.py", "v2/ch12/recipe_check.py",
                  "v2/ch12/process_steps.py", "v2/ch12/process_sequence.py", "v2/ch12/web_blocks.py", "v2/ch12/params",
                  "v2/tools/make_web_golden_ch12.py", "v2/tests/golden/process_combos.json",
                  "v2/tests/golden/recipe_check.json", *(f"v2/tests/golden/{f}" for f in FILES))


class _Track(dict):
    """읽은 키를 적는 설정(기준 설정에 실을 키 고르기)."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.read: set = set()

    def get(self, k, d=None):
        self.read.add(k)
        return super().get(k, d)

    def __getitem__(self, k):
        self.read.add(k)
        return super().__getitem__(k)


def _defaults() -> Dict[str, Any]:
    return {p.key: p.default for p in table().params}


def _cfg(over: Dict[str, Any], read: Optional[set] = None) -> _Track:
    c = _Track(_defaults())
    for k, v in over.items():
        if v is None:
            c.pop(k, None)
        else:
            c[k] = v
    c.read = set()
    return c


def B(t: Any, lab: Any = None, rowno: Any = None, **params) -> Dict[str, Any]:
    """블록 — 둘째·셋째 인자는 블록 label·row(params 의 label 과 겹치지 않게 이름을 달리 둔다)."""
    b: Dict[str, Any] = {"type": t, "params": params}
    if lab is not None:
        b["label"] = lab
    if rowno is not None:
        b["row"] = rowno
    return b


NOHEAT = {"CH2_SUPPORTS_HEATER": False}
NOTHK = {"CH2_SUPPORTS_THICKNESS": False}
_FULL2 = [
    B("heater_set", target_c=200.0, ramp_c_per_min=6.0), B("heater_wait", soak_min=10), B("base_wait", target_torr=5e-6),
    B("rga_scan"), B("gas_prep", ar=True, o2=True), B("gas_intro", ar=True, ar_flow=20.0, o2=True, o2_flow=2.5),
    B("pressure_ctrl", target=3.0), B("shutter", "Gun Shutter 열기", action="open", targets=["g1", "g2"]),
    B("power_select"), B("wait", "Power Select 안정화", sec=7), B("power_on", dc_power=100.0, rfp_power=50.0,
                                                                   rfp_freq_khz=10.0, rfp_duty=50),
    B("wait", sec=30.0), B("pressure_boost"), B("pressure_set", value=3.0), B("heater_hold"),
    B("pulse_set", kind="rf", power=60.0), B("thickness_zero"), B("shutter", action="open", targets=["ms"]),
    B("main", min=10, change_after_sec=60, change_kind="rf", change_power=80.0, change_label="중간 변경"),
    B("thickness_main", target_nm=100.0, dep_rate=0.5), B("test", sec=5), B("row_delay", sec=60, label="1분", name="쉬기"),
    B("end_stop", skip=["flow_off", "valve_open"]),
]
_FULL1 = [
    B("base_wait"), B("rga_scan"), B("gas_prep", ar=True), B("gas_intro", ar=True, ar_flow=20.0), B("pressure_ctrl", target=2.0),
    B("power_on", dcp_power=80.0, dcp_freq_khz=20, dcp_off_us=10.0), B("pressure_boost"), B("pressure_set", value=2.0),
    B("wait", sec=300.0), B("shutter", action="open", targets=["ms"]), B("main", min=10.0), B("end_stop"),
]

# (이름, 챔버, 블록 목록, 설정 바꿈, 꼬리표)
BLOCK_CASES: List[tuple] = [
    # ── 통과 ──
    ("ch2_full_all_types", 2, _FULL2, {}, ["ok", "auto.no"]),
    ("ch1_full", 1, _FULL1, {}, ["ok", "auto.no"]),
    ("auto_end_stop_added", 2, [B("gas_intro", ar=True, ar_flow=10.0)], {}, ["auto.yes"]),
    ("auto_end_stop_after_end_stop", 2, [B("end_stop"), B("power_select")], {}, ["auto.yes"]),
    ("no_auto_only_waits", 2, [B("wait", sec=1), B("test", sec=2), B("row_delay", sec=1, label="a", name="b")], {}, ["auto.no"]),
    ("no_auto_waits_after_end_stop", 2, [B("power_select"), B("end_stop"), B("wait", sec=5)], {}, ["auto.no"]),
    ("defaults_cfg_empty", 2, [B("base_wait"), B("pressure_ctrl", target=3), B("pressure_boost"), B("main", min=1)], {},
     ["defaults"]),
    ("defaults_plain", 2, [B("heater_set", target_c=100), B("heater_wait"), B("gas_prep"), B("gas_intro"),
                           B("power_on"), B("pulse_set", kind="dc", power=10), B("end_stop")], {}, ["defaults"]),
    ("int_float_types", 2, [B("wait", sec=5), B("power_on", dcp_power=100, dcp_freq_khz=50.0, rfp_duty=None),
                            B("main", min=1, integration_ms=100.0)], {}, ["format.asType"]),
    ("labels_rows", 2, [B("wait", "", sec=1), B("wait", "   ", sec=1), B("wait", "L" * BK.LABEL_MAX, sec=1),
                        B("wait", "x", 3, sec=1), B("end_stop", None, 1)], {}, ["label", "row", "limits.labelMax"]),
    ("params_falsy_is_empty", 2, [{"type": "rga_scan", "params": []}, {"type": "power_select", "params": ""},
                                  {"type": "thickness_zero", "params": 0}, {"type": "heater_hold", "params": None},
                                  {"type": "rga_scan"}], {}, ["format.params"]),
    ("end_stop_skip_all_allowed", 2, [B("end_stop", skip=list(PS.SHUTDOWN_SKIPPABLE))], {}, ["stop.skip"]),
    ("end_stop_skip_empty", 2, [B("end_stop", skip=[])], {}, ["stop.skip", "defaults"]),
    ("off_dc_text", 1, [B("power_on", dcp_power=10.0, dcp_off_us="dc"), B("power_on", dcp_power=10.0, dcp_off_us=" DC ")], {},
     ["dcp_off_range"]),
    ("off_ok_ranges", 1, [B("power_on", dcp_power=10.0, dcp_freq_khz=100, dcp_off_us=4.0),
                          B("power_on", dcp_power=10.0, dcp_off_us=10.04), B("power_on", dcp_power=10.0, dcp_off_us=0.96)],
     {}, ["dcp_off_range"]),
    ("rfp_freq_ok_edges", 2, [B("power_on", rfp_power=10.0, rfp_freq_khz=0.001), B("power_on", rfp_power=10.0, rfp_freq_khz=30)],
     {}, ["rfp_freq_range"]),
    ("gun_shutters_cfg_text", 2, [B("shutter", action="close", targets=["g3", "ms"])],
     {"CH2_PROCESS_GUN_SHUTTERS": '["G1", "G3"]'}, ["shutter_gun_exists"]),
    ("gun_shutters_common_key", 2, [B("shutter", action="open", targets=["g2"])],
     {"CH2_PROCESS_GUN_SHUTTERS": None, "PROCESS_GUN_SHUTTERS": ["G2"]}, ["shutter_gun_exists", "cfg.common"]),
    ("gun_shutters_code_default", 2, [B("shutter", action="open", targets=["g1", "g2", "g3"])],
     {"CH2_PROCESS_GUN_SHUTTERS": None}, ["shutter_gun_exists", "cfg.default"]),
    ("heater_limit_ok", 2, [B("heater_set", target_c=900.0)], {}, ["heater_limit"]),
    ("heater_wait_after_set_later_segment", 2, [B("heater_set", target_c=100.0), B("end_stop"), B("heater_wait")], {},
     ["heater_wait_target"]),
    ("main_change_all", 2, [B("main", min=1.5, change_after_sec=0, change_kind="dc", change_power=10)], {},
     ["main_change_together"]),
    ("gas_flow_blank_warn", 2, [B("gas_intro", ar=True, o2=True, o2_flow="  ", n2=True, n2_flow=None)], {},
     ["gas_flow_blank"]),
    ("gas_flow_blank_warn_ch1", 1, [B("gas_intro", ar=True, o2=True, o2_flow=1.0)], {}, ["gas_flow_blank"]),
    ("power_cap_dc2_on", 2, [B("power_on", dc2_power=10.0)], dict(NOHEAT, CH2_SUPPORTS_DC2=True), ["power_cap", "caps"]),
    ("needs_thickness_on_ch1", 1, [B("thickness_zero"), B("thickness_main", target_nm=10, dep_rate=1)],
     {"CH1_SUPPORTS_THICKNESS": True}, ["needs_cap", "caps"]),
    ("caps_common_keys", 1, [B("power_on", dc_power=10.0, rf_power=10.0)],
     {"CH1_SUPPORTS_DC": None, "SUPPORTS_DC": True, "SUPPORTS_RF_CONT": True}, ["power_cap", "caps", "cfg.common"]),
    ("caps_dc_cont_first", 2, [B("power_on", dc_power=10.0)], {"SUPPORTS_DC_CONT": False}, ["power_cap", "caps"]),
    ("caps_truthy_text", 1, [B("power_on", dc_power=10.0)], {"CH1_SUPPORTS_DC": "false"}, ["power_cap", "caps"]),
    ("int_big_value", 2, [B("main", min=1, integration_ms=1e20)], {}, ["format.asType"]),
    ("max_blocks_ok", 2, [B("wait", sec=1)] * BK.MAX_BLOCKS, {}, ["limits.maxBlocks"]),
    # ── 목록 ──
    ("err_empty_list", 2, [], {}, ["list.empty"]),
    ("err_not_list", 2, {"type": "wait"}, {}, ["list.empty"]),
    ("err_null", 2, None, {}, ["list.empty"]),
    ("err_too_many", 2, [B("wait", sec=1)] * (BK.MAX_BLOCKS + 1), {}, ["limits.maxBlocks"]),
    ("err_block_not_object", 2, [5, "wait", None, [B("wait", sec=1)], B("wait", sec=1)], {}, ["format.notObject"]),
    ("err_type", 2, [B("nope"), {"params": {}}, B(" "), B(""), {"type": ["wait"]}, {"type": 5}, {"type": 2.5},
                     {"type": True}, {"type": {"a": 1}}, {"type": None}], {}, ["format.type"]),
    ("err_unknown_block_key", 2, [dict(B("wait", sec=1), foo=1, bar=None), dict(B("rga_scan"), stage="X")], {},
     ["format.unknownKey"]),
    ("err_params_not_object", 2, [{"type": "wait", "params": "sec=1"}, {"type": "wait", "params": [1]},
                                  {"type": "wait", "params": 5, "label": 7}], {}, ["format.params"]),
    ("err_unknown_param", 2, [B("wait", sec=1, secs=2, Sec=3), B("rga_scan", x=None)], {}, ["format.unknownKey"]),
    # ── 필수 ──
    ("err_required_missing", 2, [B("heater_set"), B("pressure_ctrl"), B("pressure_set"), B("shutter"), B("power_select"),
                                 B("wait"), B("pulse_set"), B("main"), B("thickness_main"), B("test"), B("row_delay")], {},
     ["required"]),
    ("err_required_blank", 2, [B("wait", sec="  "), B("row_delay", sec=1, label="", name="\t"),
                               B("shutter", action="open", targets=[]), B("pulse_set", kind=None, power=1.0)], {},
     ["required", "format.list"]),
    # ── 형식 ──
    ("err_str_number", 2, [B("wait", sec="5"), B("power_on", dc_power="100"), B("main", min="1", integration_ms="60")], {},
     ["format.number"]),
    ("err_bool_number", 2, [B("wait", sec=True), B("power_on", dcp_freq_khz=False, dcp_power=10.0), B("test", sec=False)], {},
     ["format.number"]),
    ("err_not_bool", 2, [B("gas_prep", ar="true", o2=1, n2=0), B("pulse_set", kind="dc", power=1.0, polling="F")], {},
     ["format.bool"]),
    ("err_not_int", 1, [B("power_on", dcp_power=10.0, dcp_freq_khz=20.5), B("power_on", rfp_power=10.0, rfp_duty=33.3),
                        B("main", min=1, integration_ms=59.9)], {}, ["format.int"]),
    ("err_not_str", 2, [B("row_delay", sec=1, label=5, name=["x"]), B("main", min=1, change_after_sec=1, change_kind="dc",
                                                                         change_power=1.0, change_label=3)], {},
     ["format.str"]),
    ("err_enum", 2, [B("shutter", action="toggle", targets=["g1"]), B("shutter", action=1, targets=["g1"]),
                     B("pulse_set", kind="DC", power=1.0), B("main", min=1, change_after_sec=1, change_kind="x",
                                                             change_power=1.0)], {}, ["format.enum"]),
    ("err_list", 2, [B("shutter", action="open", targets="g1"), B("shutter", action="open", targets=["g4"]),
                     B("shutter", action="close", targets=["g1", "g1"]), B("shutter", action="close", targets=[1]),
                     B("shutter", action="close", targets=[["g1"]]), B("shutter", action="close", targets={"g1": 1})], {},
     ["format.list"]),
    ("err_off", 1, [B("power_on", dcp_power=10.0, dcp_off_us="x"), B("power_on", dcp_power=10.0, dcp_off_us=True),
                    B("power_on", dcp_power=10.0, dcp_off_us=[5])], {}, ["format.off"]),
    ("err_parts", 2, [B("end_stop", skip=["power"]), B("end_stop", skip=["relay", "ms_close", "zzz"]),
                      B("end_stop", skip=["ms_close", "ms_close"]), B("end_stop", skip=[1, "power"]),
                      B("end_stop", skip="ms_close"), B("end_stop", skip=[None])], {}, ["stop.skip", "format.parts"]),
    ("err_label_row", 2, [B("wait", "L" * (BK.LABEL_MAX + 1), sec=1), B("wait", 5, sec=1), B("wait", ["a"], sec=1),
                          B("wait", 0, sec=1), B("wait", False, sec=1), B("wait", None, 0, sec=1), B("wait", None, -1, sec=1),
                          B("wait", None, True, sec=1), B("wait", None, "2", sec=1)], {}, ["label", "row", "limits.labelMax"]),
    # ── 범위 ──
    ("err_range_min", 2, [B("wait", sec=-1), B("heater_wait", target_c=10, soak_min=-0.5), B("gas_intro", ar=True, ar_flow=-1),
                          B("test", sec=0.5), B("power_on", rfp_power=1.0, rfp_duty=0)], {}, ["range.min"]),
    ("err_range_min_exclusive", 2, [B("heater_set", target_c=0), B("heater_set", target_c=1, ramp_c_per_min=0.0),
                                    B("pressure_ctrl", target=1, timeout_sec=0), B("power_on", dc_power=0.0),
                                    B("main", min=0), B("thickness_main", target_nm=0, dep_rate=0),
                                    B("row_delay", sec=0, label="a", name="b"), B("pulse_set", kind="dc", power=-1)], {},
     ["range.minExclusive"]),
    ("err_range_max", 2, [B("heater_set", target_c=100, ramp_c_per_min=60.5), B("heater_wait", target_c=1, soak_min=481),
                          B("power_on", rfp_power=1.0, rfp_duty=100)], {}, ["range.max"]),
    ("err_range_dcp_freq", 1, [B("power_on", dcp_power=10.0, dcp_freq_khz=19), B("power_on", dcp_power=10.0, dcp_freq_khz=151)],
     {}, ["range.min", "range.max"]),
    ("err_range_tiny_numbers", 2, [B("wait", sec=-1e-300), B("heater_wait", target_c=1, soak_min=480.00000000000006),
                                   B("pressure_ctrl", target=1, timeout_sec=-0.0)], {}, ["range.min", "format.repr"]),
    # ── 값 사이·장비 지원 ──
    ("err_chamber_only", 1, [B("heater_set", target_c=100), B("heater_wait", target_c=100), B("power_select"),
                             B("heater_hold")], {}, ["chamber_only", "needs_cap"]),
    ("err_needs_heater_off", 2, [B("heater_set", target_c=100), B("heater_wait", target_c=1), B("heater_hold")], NOHEAT,
     ["needs_cap"]),
    ("err_needs_thickness_off", 2, [B("thickness_zero"), B("thickness_main", target_nm=10, dep_rate=1)], NOTHK, ["needs_cap"]),
    ("err_ch1_n2", 1, [B("gas_prep", n2=True), B("gas_intro", n2=True, n2_flow=1.0), B("gas_intro", n2=False)], {},
     ["ch1_no_n2"]),
    ("err_shutter_gun_missing", 1, [B("shutter", action="open", targets=["g1", "ms"]),
                                    B("shutter", action="close", targets=["g2", "g3"])], {}, ["shutter_gun_exists"]),
    ("err_shutter_gun_missing_ch2", 2, [B("shutter", action="open", targets=["g1", "g2", "g3"])],
     {"CH2_PROCESS_GUN_SHUTTERS": '["G1"]'}, ["shutter_gun_exists"]),
    ("err_power_cap_ch1", 1, [B("power_on", dc_power=10.0, dc2_power=5.0, rf_power=1.0)], {}, ["power_cap"]),
    ("err_power_cap_ch2", 2, [B("power_on", dc2_power=5.0), B("power_on", dcp_power=1.0, rfp_power=2.0)],
     {"CH2_SUPPORTS_DCPULSE": False, "CH2_SUPPORTS_RFPULSE": False}, ["power_cap", "pulse_exclusive"]),
    ("err_pulse_exclusive", 2, [B("power_on", dcp_power=1.0, rfp_power=2.0)], {}, ["pulse_exclusive"]),
    ("err_rfp_freq", 2, [B("power_on", rfp_power=1.0, rfp_freq_khz=0.0005), B("power_on", rfp_power=1.0, rfp_freq_khz=30.5),
                         B("power_on", rfp_freq_khz=-1)], {}, ["rfp_freq_range"]),
    ("err_rfp_freq_cfg", 2, [B("power_on", rfp_power=1.0, rfp_freq_khz=25)], {"CH2_RFPULSE_FREQ_MAX_HZ": 20000.5},
     ["rfp_freq_range", "cfg"]),
    ("err_rfp_freq_cfg_bad", 2, [B("power_on", rfp_power=1.0, rfp_freq_khz=31)], {"CH2_RFPULSE_FREQ_MAX_HZ": "abc"},
     ["rfp_freq_range", "cfg"]),
    ("err_rfp_freq_cfg_zero", 2, [B("power_on", rfp_power=1.0, rfp_freq_khz=31)],
     {"CH2_RFPULSE_FREQ_MAX_HZ": None, "RFPULSE_FREQ_MAX_HZ": 0}, ["rfp_freq_range", "cfg", "cfg.common"]),
    ("err_rfp_freq_cfg_common", 2, [B("power_on", rfp_power=1.0, rfp_freq_khz=12.5)],
     {"CH2_RFPULSE_FREQ_MAX_HZ": None, "RFPULSE_FREQ_MAX_HZ": 12000}, ["rfp_freq_range", "cfg.common"]),
    ("err_dcp_off", 1, [B("power_on", dcp_power=10.0, dcp_freq_khz=100, dcp_off_us=4.5),
                        B("power_on", dcp_power=10.0, dcp_off_us=0.5), B("power_on", dcp_power=10.0, dcp_off_us=10.06),
                        B("power_on", dcp_power=10.0, dcp_freq_khz=150, dcp_off_us=2.7),
                        B("power_on", dcp_power=10.0, dcp_freq_khz=30, dcp_off_us=13.37),
                        B("power_on", dcp_freq_khz=200, dcp_off_us=9.0), B("power_on", dcp_freq_khz=20.5, dcp_off_us=9.0),
                        B("power_on", dcp_off_us=-3)], {}, ["dcp_off_range", "range.max", "format.int"]),
    ("err_dcp_off_bad_freq_skips", 1, [B("power_on", dcp_power=10.0, dcp_freq_khz="x", dcp_off_us=12.0)], {},
     ["dcp_off_range"]),
    ("err_heater_limit", 2, [B("heater_set", target_c=900.5), B("heater_set", target_c=450.0)], {"CH2_HEATER_MAX_TEMP": 400.0},
     ["heater_limit", "cfg"]),
    ("err_heater_limit_sv", 2, [B("heater_set", target_c=600.0)], {"CH2_HEATER_SV_LIMIT_C": 0, "CH2_HEATER_MAX_TEMP": 700},
     ["heater_limit", "cfg"]),
    ("err_heater_limit_max_over_sv", 2, [B("heater_set", target_c=850.0)],
     {"CH2_HEATER_SV_LIMIT_C": 800.0, "CH2_HEATER_MAX_TEMP": 1000.0}, ["heater_limit", "cfg"]),
    ("err_main_change", 2, [B("main", min=1, change_after_sec=10), B("main", min=1, change_kind="dc"),
                            B("main", min=1, change_after_sec=1, change_kind="rf"), B("main", min=1, change_power=5.0),
                            B("main", min=1, change_after_sec="", change_kind=" ", change_power=None)], {},
     ["main_change_together"]),
    ("err_heater_wait_no_target", 2, [B("heater_wait"), B("heater_set", target_c=100), B("heater_wait")], {},
     ["heater_wait_target"]),
    ("err_heater_wait_twice", 2, [B("heater_wait"), B("heater_wait")], {}, ["heater_wait_target"]),
    ("err_heater_wait_hidden_by_other_error", 2, [B("heater_wait"), B("wait", sec=-1)], {}, ["heater_wait_target"]),
    ("warn_with_error", 2, [B("gas_intro", ar=True), B("gas_intro", o2=True, o2_flow="x")], {}, ["gas_flow_blank", "warn"]),
    ("err_mixed_order", 1, [dict(B("gas_intro", ar=True, n2=True, x=1, o2="y"), zz=1, aa=2), B("heater_set", target_c=1000),
                            B("power_on", dc_power=-1, rfp_power=10.0, dcp_power=5.0, rfp_freq_khz=99)], {},
     ["order", "chamber_only", "ch1_no_n2", "power_cap"]),
    ("err_unicode_type", 2, [B("대기"), B("wait​"), B("it's")], {}, ["format.type", "format.repr"]),
]

# 시퀀스 편집 (이름, 챔버, 블록, started_upto, op, rev, 설정 바꿈, 꼬리표)
_SEQ = [B("gas_intro", ar=True, ar_flow=10.0), B("wait", sec=10), B("main", min=1), B("end_stop")]
EDIT_CASES: List[tuple] = [
    ("insert_ok", 2, _SEQ, 1, {"op": "insert", "at": 2, "block": B("wait", sec=5)}, 0, {}, ["edit.ok"]),
    ("insert_at_end", 2, _SEQ, 0, {"op": "insert", "at": 4, "block": B("power_select")}, 3, {}, ["edit.ok", "auto.yes"]),
    ("insert_at_started", 2, _SEQ, 2, {"op": "insert", "at": 2, "block": B("wait", sec=5)}, 0, {}, ["edit.ok"]),
    ("insert_before_started", 2, _SEQ, 2, {"op": "insert", "at": 1, "block": B("wait", sec=5)}, 4, {}, ["edit.started"]),
    ("insert_bad_at", 2, _SEQ, 0, {"op": "insert", "at": 5, "block": B("wait", sec=5)}, 0, {}, ["edit.badOp"]),
    ("insert_bool_at", 2, _SEQ, 0, {"op": "insert", "at": True, "block": B("wait", sec=5)}, 0, {}, ["edit.badOp"]),
    ("insert_no_block", 2, _SEQ, 0, {"op": "insert", "at": 1}, 0, {}, ["edit.badOp"]),
    ("insert_block_not_dict", 2, _SEQ, 0, {"op": "insert", "at": 1, "block": [1]}, 0, {}, ["edit.badOp"]),
    ("insert_invalid", 2, _SEQ, 1, {"op": "insert", "at": 3, "block": B("wait", sec=-5)}, 2, {}, ["edit.invalid"]),
    ("insert_warn", 2, _SEQ, 1, {"op": "insert", "at": 3, "block": B("gas_intro", o2=True)}, 2, {}, ["edit.ok", "warn"]),
    ("delete_ok", 2, _SEQ, 1, {"op": "delete", "at": 3}, 0, {}, ["edit.ok", "auto.yes"]),
    ("delete_started", 2, _SEQ, 2, {"op": "delete", "at": 1}, 0, {}, ["edit.started"]),
    ("delete_bad_at", 2, _SEQ, 0, {"op": "delete", "at": 4}, 0, {}, ["edit.badOp"]),
    ("delete_neg_at", 2, _SEQ, 0, {"op": "delete", "at": -1}, 0, {}, ["edit.badOp"]),
    ("delete_float_at", 2, _SEQ, 0, {"op": "delete", "at": 1.5}, 0, {}, ["edit.badOp"]),
    ("move_ok", 2, _SEQ, 1, {"op": "move", "from": 3, "to": 1}, 7, {}, ["edit.ok"]),
    ("move_same", 2, _SEQ, 0, {"op": "move", "from": 2, "to": 2}, 0, {}, ["edit.ok"]),
    ("move_started_from", 2, _SEQ, 2, {"op": "move", "from": 1, "to": 3}, 0, {}, ["edit.started"]),
    ("move_started_to", 2, _SEQ, 2, {"op": "move", "from": 3, "to": 0}, 0, {}, ["edit.started"]),
    ("move_bad", 2, _SEQ, 0, {"op": "move", "from": 0}, 0, {}, ["edit.badOp"]),
    ("move_makes_invalid", 2, [B("heater_set", target_c=100), B("heater_wait"), B("end_stop")], 0,
     {"op": "move", "from": 1, "to": 0}, 0, {}, ["edit.invalid", "heater_wait_target"]),
    ("bad_op_name", 2, _SEQ, 0, {"op": "replace", "at": 1}, 0, {}, ["edit.badOp"]),
    ("bad_op_not_mapping", 2, _SEQ, 0, ["insert"], 0, {}, ["edit.badOp"]),
    ("bad_started_upto", 2, _SEQ, 5, {"op": "delete", "at": 3}, 0, {}, ["edit.badOp"]),
    ("bad_started_upto_neg", 2, _SEQ, -1, {"op": "delete", "at": 3}, 0, {}, ["edit.badOp"]),
    ("edit_ch1_device", 1, [B("wait", sec=1)], 0, {"op": "insert", "at": 1, "block": B("heater_hold")}, 0, {},
     ["edit.invalid", "chamber_only"]),
    ("edit_empty_result", 2, [B("wait", sec=1)], 0, {"op": "delete", "at": 0}, 0, {}, ["edit.invalid", "list.empty"]),
]

# 블록 꼬리표 — 시험이 모두 한 번 이상 나오는지 본다(규칙 id 는 VALUE_RULES 로 따로)
TAGS: tuple = (
    "ok", "auto.yes", "auto.no", "defaults", "format.asType", "label", "row", "limits.labelMax", "limits.maxBlocks",
    "format.params", "stop.skip", "list.empty", "format.notObject", "format.type", "format.unknownKey", "required",
    "format.list", "format.number", "format.bool", "format.int", "format.str", "format.enum", "format.off", "format.parts",
    "range.min", "range.max", "range.minExclusive", "format.repr", "caps", "cfg", "cfg.common", "cfg.default", "warn",
    "order", "edit.ok", "edit.started", "edit.badOp", "edit.invalid",
)


# ───────────── 1) 블록 ─────────────
def _auto(clean: Optional[list], cfg, ch: int) -> Optional[bool]:
    if clean is None:
        return None
    _steps, insts = BK.expand(clean, cfg, ch)
    return any(i.auto for i in insts)


def block_case(name, ch, blocks, over, tags, track: Optional[set] = None) -> Dict[str, Any]:
    cfg = _cfg(over)
    clean, errs, warns = BK.check_blocks(copy.deepcopy(blocks), cfg, ch)
    auto = _auto(clean, cfg, ch)
    if track is not None:
        track |= cfg.read
    return {"name": name, "covers": sorted(set(tags)), "ch": ch, "blocks": blocks, "cfgOver": over, "clean": clean,
            "errors": list(errs), "warnings": list(warns), "autoEndStop": auto}


def edit_case(name, ch, blocks, k, op, rev, over, tags, track: Optional[set] = None) -> Dict[str, Any]:
    cfg = _cfg(over)
    r = BK.edit_sequence(copy.deepcopy(blocks), k, copy.deepcopy(op), cfg, ch, rev=rev)
    auto = _auto(r.blocks, cfg, ch)
    if track is not None:
        track |= cfg.read
    return {"name": name, "covers": sorted(set(tags)), "ch": ch, "blocks": blocks, "startedUpto": k, "op": op, "rev": rev,
            "cfgOver": over, "ok": r.ok, "result": r.blocks, "rev2": r.rev, "reason": r.reason, "errors": list(r.errors),
            "warnings": list(r.warns), "autoEndStop": auto}


# ───────────── 2) 기본 템플릿 ─────────────
_CAPS_CFG = {"dc": "SUPPORTS_DC", "dc2": "SUPPORTS_DC2", "rf": "SUPPORTS_RF_CONT", "dcp": "SUPPORTS_DCPULSE",
             "rfp": "SUPPORTS_RFPULSE", "heater": "SUPPORTS_HEATER", "thickness": "SUPPORTS_THICKNESS"}


def _combo_over(case: Dict[str, Any]) -> Dict[str, Any]:
    """process_combos 의 caps 바꿈(caps_override·thk)을 설정 바꿈으로."""
    ch = case["ch"]
    over = {f"CH{ch}_{_CAPS_CFG[k]}": v for k, v in case["caps_override"].items()}
    over[f"CH{ch}_SUPPORTS_THICKNESS"] = bool(case["thk"])
    want = dataclasses.replace(PS.caps_from_cfg(_defaults(), ch), **case["caps_override"], thickness=bool(case["thk"]))
    assert PS.caps_from_cfg(_cfg(over), ch) == want, (case["name"], over)
    return over


def _row(**kw) -> Dict[str, str]:
    return {k: ("" if v is None else v) for k, v in kw.items()}


_P = dict(Process_name="P", Ar="T", Ar_flow="20", working_pressure="3", process_time="1", use_dc_power="T", dc_power="50",
          gun1="T")
# (이름, 챔버, 행, 설정 바꿈) — process_combos 에 없는 행 모양
EXTRA_TEMPLATE: List[tuple] = [
    ("delay_minutes", 2, _row(Process_name="쉬기", delay="5"), {}),
    ("delay_minutes_float", 2, _row(Process_name="", delay="1.5"), {}),
    ("delay_duration", 2, _row(Process_name="d", delay="1h30m"), {}),
    ("delay_duration_no_unit_tail", 2, _row(Process_name="d", delay="1h30"), {}),
    ("delay_text_zero", 2, _row(Process_name="d", delay="abc", **{k: v for k, v in _P.items() if k != "Process_name"}), {}),
    ("delay_zero", 1, _row(Process_name="z", delay="0", Ar="T", Ar_flow="5", use_dc_pulse="T", dc_pulse_power="10"), {}),
    ("delay_negative", 1, _row(Process_name="z", delay="-2", Ar="T", use_rf_pulse="T", rf_pulse_power="10"), {}),
    ("delay_spaces", 2, _row(Process_name="d", delay=" 30 s "), {}),
    ("delay_unicode_digits", 2, _row(Process_name="d", delay="５"), {}),
    ("delay_underscore", 2, _row(Process_name="d", delay="1_0"), {}),
    ("delay_name_default_unit", 2, _row(Process_name="delay 10"), {}),
    ("delay_name_units", 2, _row(Process_name="  DELAY 2H "), {}),
    ("delay_name_seconds", 1, {"Process_name": "delay30s"}, {}),
    ("delay_name_days", 2, _row(Process_name="Delay 1 d"), {}),
    ("delay_name_bad_unit", 2, dict(_P, Process_name="delay 3x"), {}),
    ("delay_name_float", 2, dict(_P, Process_name="delay 1.5m"), {}),
    ("delay_from_process_note", 2, {"process_note": "delay 2s"}, {}),
    ("test_row_time_seconds", 2, _row(**{"#": "TEST", "Process_name": "t", "time": "30s"}), {}),
    ("test_row_time_number", 2, _row(**{"#": "test", "Process_name": "t", "time": "30"}), {}),
    ("test_row_process_time", 2, {"#": " Test ", "Process_name": "t", "process_time": "2"}, {}),
    ("test_row_no_time", 1, {"#": "test"}, {}),
    ("test_row_minutes_round", 2, {"#": "test", "time": "1m40s"}, {}),
    ("test_row_hms", 1, {"#": "test", "Process_name": "x", "time": "1h", "process_time": ""}, {}),
    ("values_not_strings", 2, {"Process_name": "n", "Ar": True, "Ar_flow": 20, "working_pressure": 3, "process_time": "2",
                               "use_dc_power": "t", "dc_power": 50, "gun1": "yes", "use_g2": "F", "main_shutter": None,
                               "base_pressure": None, "integration_time": "100.9"}, {}),
    ("number_shapes", 2, dict(_P, Ar_flow=" 1_0.5 ", working_pressure="+2.", process_time=".5", dc_power="1e2",
                              base_pressure="7.00E-06", shutter_delay="-0", integration_time="1e2"), {}),
    ("number_unicode", 2, dict(_P, Ar_flow="２０", dc_power="1٠٠"), {}),
    ("number_bad_text", 2, dict(_P, Ar_flow="20sccm", working_pressure="abc", process_time="1__0", integration_time="inf",
                                dc_power="_5"), {}),
    ("thickness_auto_time", 2, dict(_P, process_time="", thickness="100", dep_rate="0.5"), NOTHK),
    ("thickness_dep_dot", 2, dict(_P, process_time="0", thickness="60", **{"dep.rate": "2"}), NOTHK),
    ("thickness_dep_bad_then_dot", 2, dict(_P, process_time="0", thickness="60", dep_rate="x", **{"dep.rate": "3"}), NOTHK),
    ("thickness_dep_zero", 2, dict(_P, process_time="0", thickness="60", dep_rate="0", **{"dep.rate": "3"}), NOTHK),
    ("thickness_mode", 2, dict(_P, thickness="100", dep_rate="0.25", main_shutter="T"), {}),
    ("thickness_mode_e844", 2, dict(_P, thickness="100", dep_rate="1", use_dc_pulse="T", dc_pulse_power="50",
                                    power_change_time="2m", change_power_value="60.5"), {}),
    ("thickness_mode_change_before_main", 2, dict(_P, thickness="100", dep_rate="1", shutter_delay="5", use_rf_pulse="T",
                                                  rf_pulse_power="50", power_change_time="30s", change_power_value="60"), {}),
    ("g_targets", 2, dict(_P, gun2="T", **{"G1 Target": " Ti ", "G2_target_name": "Cu", "G3 Target": "Al"}), {}),
    ("g_targets_ch1", 1, _row(Process_name="t", Ar="T", use_dc_pulse="T", dc_pulse_power="10", **{"G1 Target": "", "G1_target_name": "W",
                                                                                                  "G2 Target": "X"}), {}),
    ("use_g_truthy", 2, dict(_P, gun1="F", use_g1="F", use_g3="0"), {}),
    ("chuck_positions", 2, dict(_P, chuck_position=" MID "), {}),
    ("chuck_position_bad", 2, dict(_P, chuck_position="top"), {}),
    ("heater_off_values", 2, dict(_P, use_heater="F", heater_temp="300", heater_soak="5"), {}),
    ("heater_on_full", 2, dict(_P, use_heater="T", heater_temp="300.5", heater_ramp="7.25", heater_soak="0"), {}),
    ("heater_on_bad_values", 2, dict(_P, use_heater="T", heater_temp="x", heater_ramp="61", heater_soak="-1"), {}),
    ("heater_on_ch1", 1, _row(Process_name="h", Ar="T", use_dc_pulse="T", dc_pulse_power="10", use_heater="T",
                              heater_temp="100"), {}),
    ("dcp_legacy_duty", 1, _row(Process_name="d", Ar="T", use_dc_pulse="T", dc_pulse_power="10", dc_pulse_freq="40",
                                dc_pulse_duty_cycle="75"), {}),
    ("dcp_legacy_duty_no_freq", 1, _row(Process_name="d", Ar="T", use_dc_pulse="T", dc_pulse_power="10",
                                        dc_pulse_duty_cycle="75"), {}),
    ("dcp_off_round", 1, _row(Process_name="d", Ar="T", use_dc_pulse="T", dc_pulse_power="10.25", dc_pulse_freq="33.9",
                              dc_pulse_off_time="2.25"), {}),
    ("dcp_off_bad", 1, _row(Process_name="d", Ar="T", use_dc_pulse="T", dc_pulse_power="10", dc_pulse_off_time="dcx"), {}),
    ("dcp_off_dc_lower", 1, _row(Process_name="d", Ar="T", use_dc_pulse="T", dc_pulse_power="10", dc_pulse_off_time=" dc "), {}),
    ("dcp_freq_inf", 1, _row(Process_name="d", Ar="T", use_dc_pulse="T", dc_pulse_power="10", dc_pulse_freq="inf"), {}),
    ("pulse_unused_values", 2, dict(_P, use_dc_pulse="F", dc_pulse_freq="40", dc_pulse_off_time="5", use_rf_pulse="",
                                    rf_pulse_duty_cycle="50"), {}),
    ("pulse_unused_power_only", 2, dict(_P, dc_pulse_power="-5", rf_pulse_power="0.0"), {}),
    ("pulse_unsupported", 2, dict(_P, use_rf_pulse="T", rf_pulse_power="10", rf_pulse_freq="5"), {"CH2_SUPPORTS_RFPULSE": False}),
    ("rfp_freq_overflow", 2, dict(_P, use_rf_pulse="T", rf_pulse_power="10", rf_pulse_freq="1e306"), {}),
    ("rfp_freq_round", 2, dict(_P, use_rf_pulse="T", rf_pulse_power="10", rf_pulse_freq="0.0125", rf_pulse_duty_cycle="33.7"),
     {}),
    ("mid_change_both_pulses", 1, _row(Process_name="m", Ar="T", use_dc_pulse="T", dc_pulse_power="10", use_rf_pulse="T",
                                       rf_pulse_power="20", shutter_delay="1", process_time="2", power_change_time="90",
                                       change_power_value="15"), {}),
    ("mid_change_ignored", 1, _row(Process_name="m", Ar="T", use_rf_pulse="T", rf_pulse_power="20", process_time="1",
                                   power_change_time="2m", change_power_value="15"), {}),
    ("mid_change_bad_power", 1, _row(Process_name="m", Ar="T", use_dc_pulse="T", dc_pulse_power="20", process_time="1",
                                     power_change_time="30", change_power_value="x"), {}),
    ("mid_change_in_shutter_delay", 1, _row(Process_name="m", Ar="T", use_dc_pulse="T", dc_pulse_power="20", shutter_delay="0.5",
                                            process_time="1", power_change_time="0.5m", change_power_value="25"), {}),
    ("mid_change_at_shutter_end", 1, _row(Process_name="m", Ar="T", use_dc_pulse="T", dc_pulse_power="20", shutter_delay="0.5",
                                          process_time="1", power_change_time="30s", change_power_value="25"), {}),
    ("power_select_ch1_ignored", 1, _row(Process_name="p", Ar="T", use_dc_pulse="T", dc_pulse_power="20", power_select="T"), {}),
    ("power_select_settle_cfg", 2, dict(_P, power_select="T"), {"PC_POWER_SELECT_SETTLE_MS": 2500}),
    ("power_select_settle_zero", 2, dict(_P, power_select="T"), {"PC_POWER_SELECT_SETTLE_MS": 0}),
    ("boost_cfg", 2, dict(_P, working_pressure="12"), {"CH2_PC_WORKING_PRESSURE_BOOST_TARGET": None,
                                                       "PC_WORKING_PRESSURE_BOOST_TARGET": 15.5}),
    ("gun_shutters_cfg", 2, dict(_P, gun2="T", gun3="T"), {"CH2_PROCESS_GUN_SHUTTERS": '["G3", "G1"]'}),
    ("ch1_n2", 1, _row(Process_name="n", N2="T", N2_flow="5", use_dc_pulse="T", dc_pulse_power="10"), {}),
    ("ch1_n2_flow_only", 1, _row(Process_name="n", Ar="T", N2_flow="5", use_dc_pulse="T", dc_pulse_power="10"), {}),
    ("no_gas_no_power", 2, _row(Process_name="e"), {}),
    ("all_blank_names", 2, {"#": "7"}, {}),
    ("process_note_passthrough", 2, {"process_note": "메모", "Ar": "T", "use_rf_power": "T", "rf_power": "40", "gun1": "T"}, {}),
]

# 템플릿 경우 이름 앞머리 — 시험이 다 나오는지 본다
TEMPLATE_KINDS = ("delay", "test", "process", "error")


def _j(x):
    return json.loads(json.dumps(x, ensure_ascii=False, allow_nan=False))


def template_case(name, ch, raw, over, track: Optional[set] = None) -> Dict[str, Any]:
    cfg = _cfg(over)
    caps = PS.caps_from_cfg(cfg, ch)
    out: Dict[str, Any] = {"name": name, "ch": ch, "raw": raw, "cfgOver": over}
    try:
        rt = template_from_row(dict(raw), ch, cfg, caps=caps)
    except Exception as e:                                  # v1 처럼 예외가 그대로 올라오는 행(rf 주파수 넘침 등)
        out.update(kind=None, params=None, logs=None, blocks=None, error=None, normErrors=None, exception=type(e).__name__)
    else:
        norm = None
        if rt.params is not None:
            p = copy.deepcopy(rt.params)
            p.pop("thickness_mode", None)
            norm = CK.check_norm_params(p, ch, caps, cfg)
        out.update(kind=rt.kind, params=rt.params, logs=[list(x) for x in rt.logs], blocks=rt.blocks, error=rt.error,
                   normErrors=norm, exception=None)
    if track is not None:
        track |= cfg.read
    return _j(out)


def _drop_blank(name: str, ch: int, raw: Dict[str, Any], over: Dict[str, Any]) -> Dict[str, Any]:
    """빈 글 칸을 뺀 행 — 결과가 같을 때만(파일 크기). 다르면 원래 행."""
    short = {k: v for k, v in raw.items() if v != ""}
    a, b = template_case(name, ch, raw, over), template_case(name, ch, short, over)
    a.pop("raw"), b.pop("raw")
    return short if a == b else raw


def template_inputs() -> List[tuple]:
    """process_combos.json 435 경우(빈 칸을 뺀 행) + EXTRA_TEMPLATE."""
    combo = json.loads((GOLD / "process_combos.json").read_text(encoding="utf-8"))
    rows = []
    for c in combo["cases"]:
        over = _combo_over(c)
        rows.append((c["name"], c["ch"], _drop_blank(c["name"], c["ch"], c["raw"], over), over))
    return rows + EXTRA_TEMPLATE


# ───────────── 3) 레시피 행 값 검사(E230)·시작 전 값 검사 ─────────────
def _r(name="", **kw) -> Dict[str, Any]:
    d = {"Process_name": name}
    d.update(kw)
    return d


EXTRA_RAW: List[tuple] = [
    ("empty_rows", [], None),
    ("non_dict_rows", [5, None, _r("a", base_pressure="x"), ["base_pressure", "y"]], None),
    ("row_numbers_short", [_r("a", base_pressure="x"), _r("b", base_pressure="y"), _r("c", base_pressure="z")], [10, 12]),
    ("float_shapes_ok", [_r("f", base_pressure="1e-5", working_pressure=" 2.5 ", process_time="1_000", shutter_delay="+.5",
                            thickness="5.", dep_rate="inf", dc_power="-Infinity", rf_power="nan", Ar_flow="２０",
                            O2_flow=" 3　", N2_flow="1E+3")], None),
    ("float_shapes_bad", [_r("f", base_pressure="1e", working_pressure="1__0", process_time="_1", shutter_delay="1_",
                             thickness="0x10", dep_rate="1,5", dc_power="5W", rf_power="infinit", Ar_flow="--1",
                             O2_flow=".", N2_flow="1.2.3")], None),
    ("float_whitespace_kinds", [_r("w", base_pressure="\x1c5\x1f", working_pressure="﻿5", process_time="​5")], None),
    ("ranges_edges", [_r("r", tooling="10", density="0.5", z_factor="0.1"), _r("r2", tooling="9.999", density="100",
                                                                              z_factor="nan"),
                      _r("r3", tooling="inf", density="-0", z_factor="9.9990001")], [3, 4, 5]),
    ("int_keys", [_r("i", integration_time="60.7", dc_pulse_freq="1e3", dc_pulse_duty_cycle="inf", rf_pulse_duty_cycle="nan"),
                  _r("i2", integration_time="1e400", dc_pulse_freq="x", rf_pulse_duty_cycle=" 5 ")], None),
    ("tf_keys", [_r("t", gun1="t", gun2="yes", gun3="On", use_dc_pulse="0", use_rf_pulse="no", Ar="TRUE ", O2=" n",
                    N2="2", main_shutter="Y", power_select="false", use_heater="ｔ")], None),
    ("tf_non_strings", [_r("b", gun1=True, gun2=False, gun3=1, Ar=0, O2=None, N2=["T"], use_heater=2)], None),
    ("enum_chuck", [_r("c", chuck_position="UP"), _r("c2", chuck_position=" Mid "), _r("c3", chuck_position="top"),
                    _r("c4", chuck_position=False)], None),
    ("off_time", [_r("o", dc_pulse_off_time="dc"), _r("o2", dc_pulse_off_time=" DC "), _r("o3", dc_pulse_off_time="1e1"),
                  _r("o4", dc_pulse_off_time="DCX"), _r("o5", dc_pulse_off_time="inf")], None),
    ("durations", [_r("d", power_change_time="30", change_power_value="5"), _r("d2", power_change_time="1.5h",
                                                                             change_power_value="5"),
                   _r("d3", power_change_time="1h30m10.5s", change_power_value="5"),
                   _r("d4", power_change_time="0", change_power_value="5"), _r("d5", power_change_time="h", change_power_value="5"),
                   _r("d6", power_change_time="5 m", change_power_value="5"), _r("d7", power_change_time="５m",
                                                                                 change_power_value="5"),
                   _r("d8", power_change_time="0.0s1", change_power_value="5"), _r("d9", power_change_time="2M",
                                                                                   change_power_value="x")], None),
    ("pair_rule", [_r("p", change_power_value="5"), _r("p2", power_change_time="5s"), _r("p3", change_power_value=" ",
                                                                                         power_change_time=" ")], None),
    ("names", [_r(" 이름 ", base_pressure="x"), {"Process_name": None, "base_pressure": "x"}, {"base_pressure": "x"},
               {"Process_name": 0, "base_pressure": "x"}, {"Process_name": 5, "base_pressure": "x"},
               {"Process_name": "it's", "base_pressure": "x"}], None),
    ("non_string_values", [_r("v", base_pressure=5, working_pressure=0, process_time=True, shutter_delay=False,
                              integration_time=2.5, dc_pulse_off_time=7, tooling=500, density=[1])], None),
    ("overflow_exact_20", [_r(f"x{i}", base_pressure="bad") for i in range(20)], None),
    ("overflow_21", [_r(f"x{i}", base_pressure="bad") for i in range(21)], None),
    ("overflow_many", [_r(f"y{i}", base_pressure="a", working_pressure="b", gun1="c") for i in range(15)],
     list(range(100, 115))),
]
EXTRA_NORM: List[tuple] = [
    ("flow_text_numbers", 2, dict(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True),
     {"use_ar": True, "ar_flow": " 5 ", "o2_flow": "NONE", "n2_flow": "", "use_g1": True, "use_rf_power": True}),
    ("flow_bool_and_list", 2, dict(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True),
     {"use_o2": True, "ar_flow": True, "o2_flow": [1], "n2_flow": {"a": 1}, "use_g2": True, "use_dc_power": True,
      "dc_power": 1.0}),
    ("flow_missing_keys", 1, dict(dc=False, dc2=False, rf=False, dcp=True, rfp=True, heater=False, thickness=False),
     {"use_n2": True, "use_rf_pulse": True, "rf_pulse_power": 5.0, "rf_pulse_freq": 0.001, "rf_pulse_duty": 1}),
    ("dcp_freq_given_off_ok", 1, dict(dc=False, dc2=False, rf=False, dcp=True, rfp=True, heater=False, thickness=False),
     {"use_ar": True, "use_dc_pulse": True, "dc_pulse_power": 5.0, "dc_pulse_freq": 150, "dc_pulse_off_time": 2.7}),
    ("dcp_freq_zero", 1, dict(dc=False, dc2=False, rf=False, dcp=True, rfp=True, heater=False, thickness=False),
     {"use_ar": True, "use_dc_pulse": True, "dc_pulse_power": 5.0, "dc_pulse_freq": 0, "dc_pulse_off_time": 10.5}),
    ("dcp_off_dc_text", 2, dict(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True),
     {"use_ar": True, "use_g1": True, "use_dc_pulse": True, "dc_pulse_power": 5.0, "dc_pulse_off_time": " dc "}),
    ("heater_limits_edges", 2, dict(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True),
     {"use_ar": True, "use_g1": True, "use_g2": True, "use_rf_power": True, "rf_power": 1.0, "use_heater": True,
      "heater_temp": 0.5, "heater_ramp": None, "heater_soak": 0.0}),
    ("heater_ramp_zero", 2, dict(dc=True, dc2=False, rf=True, dcp=True, rfp=True, heater=True, thickness=True),
     {"use_ar": True, "use_g1": True, "use_rf_power": True, "use_heater": True, "heater_temp": 1e-7, "heater_ramp": 0.0,
      "heater_soak": 480.1}),
]


def raw_case(name, rows, nos) -> Dict[str, Any]:
    errs = CK.check_raw_rows(copy.deepcopy(rows), row_numbers=nos)
    return {"name": name, "rows": rows, "rowNumbers": nos, "errors": errs, "message": CK.e230_message(errs) if errs else "",
            "popup": CK.e230_popup(errs) if errs else ""}


def norm_case(name, ch, caps, params, cfg) -> Dict[str, Any]:
    errs = CK.check_norm_params(copy.deepcopy(params), ch, PS.Caps(**caps), cfg)
    return {"name": name, "ch": ch, "caps": caps, "params": params, "errors": errs}


# ───────────── 묶기 ─────────────
def _head(read: set, what: str) -> Dict[str, Any]:
    full = _defaults()
    defs = WB.definitions()
    return {"_설명": f"챔버12 웹 검사 골든 — {what}(v2.tools.make_web_golden_ch12 가 만든다 — 손으로 고치지 않음). 모양은 생성기 머리말",
            "defsVersion": WB.WEB_BLOCKS_VERSION, "defsHash": WB.content_hash(defs), "defs": defs,
            "cfg": {k: full[k] for k in sorted(read) if k in full}}


def build(track: Optional[Dict[str, set]] = None) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    r1: set = set()
    bc = [block_case(*c, track=r1) for c in BLOCK_CASES]
    ec = [edit_case(*c, track=r1) for c in EDIT_CASES]
    out[FILES[0]] = {**_head(r1, "블록 검사·시퀀스 편집"), "blockCases": bc, "editCases": ec}
    r2: set = set()
    tc = [template_case(*c, track=r2) for c in template_inputs()]
    out[FILES[1]] = {**_head(r2, "v1 행 → 기본 템플릿 블록"), "cases": tc}
    check = json.loads((GOLD / "recipe_check.json").read_text(encoding="utf-8"))
    raw = [raw_case(n, g["rows"], g["row_numbers"]) for n, g in sorted(check["raw"].items())]
    raw += [raw_case(*c) for c in EXTRA_RAW]
    full = _Track(_defaults())
    norm = [norm_case(f"v1_{i:02d}", c["ch"], c["caps"], c["params"], full) for i, c in enumerate(check["norm"])]
    norm += [norm_case(n, ch, caps, p, full) for n, ch, caps, p in EXTRA_NORM]
    out[FILES[2]] = {**_head(full.read, "E230 행 값 검사·시작 전 값 검사"), "rawCases": raw, "normCases": norm}
    if track is not None:
        track.update({FILES[0]: r1, FILES[1]: r2, FILES[2]: set(full.read)})
    return out


def to_json(d: Dict[str, Any]) -> str:
    """머리는 들여쓰기, 경우는 한 줄씩(파일 크기)."""
    parts = []
    for k, v in d.items():
        if isinstance(v, list) and k.endswith(("Cases", "cases")):
            body = ",\n".join("  " + json.dumps(x, ensure_ascii=False, allow_nan=False) for x in v)
            parts.append(f" {json.dumps(k, ensure_ascii=False)}: [\n{body}\n ]")
        else:
            parts.append(f" {json.dumps(k, ensure_ascii=False)}: "
                         + json.dumps(v, ensure_ascii=False, allow_nan=False, indent=1).replace("\n", "\n "))
    return "{\n" + ",\n".join(parts) + "\n}\n"


def _git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return ""


def source_head(name: str) -> Dict[str, Any]:
    """포털 사본 머리 — 이 저장소 커밋·작업 트리 상태·정의 해시."""
    full = _git("rev-parse", "HEAD")
    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    dirty = bool(_git("status", "--porcelain", "--", *SOURCES))
    return {"Chamber.Total_program": f"{branch} {full[:7]} ({full})", "dirty": dirty, "file": f"v2/tests/golden/{name}",
            "defsVersion": WB.WEB_BLOCKS_VERSION, "defsHash": WB.content_hash(),
            "설명": "_출처 밖은 장비 파일 그대로(손으로 고치지 않음). dirty=true 면 커밋 전 작업 트리에서 만든 것 — 커밋 뒤 "
                  "python -m v2.tools.make_web_golden_ch12 --copy <포털> 로 다시 복사. npm run ops2:golden 이 확인"}


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    GOLD.mkdir(parents=True, exist_ok=True)
    data = build()
    for name, d in data.items():
        (GOLD / name).write_text(to_json(d), encoding="utf-8", newline="\n")
        n = sum(len(v) for k, v in d.items() if isinstance(v, list) and k.endswith(("Cases", "cases")))
        print(f"{GOLD / name} — {n} 경우")
    if "--copy" in argv:
        dest = Path(argv[argv.index("--copy") + 1]) / PORTAL_DIR
        dest.mkdir(parents=True, exist_ok=True)
        for name, d in data.items():
            (dest / name).write_text(to_json({"_출처": source_head(name), **d}), encoding="utf-8", newline="\n")
            print(f"  → {dest / name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
