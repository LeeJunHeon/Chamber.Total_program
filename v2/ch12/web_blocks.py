"""챔버12 웹 검사 정의 — 포털(vanam-process lib/ops2/ch12)이 블록·v1 행 검사를 장비와 같게 계산하는 데 쓰는 정의를 JSON 으로.

챔버K v2/chk/web_schema.py definitions() 의 blocks 부분을 따른다(나중에 CH12 web_schema 가 이 definitions() 를 묶음에 싣는다).
웹은 값 범위·형식·기본값·장비 지원 판단에 쓰는 표를 여기서 읽고, 값 사이 규칙(VALUE_RULES)은 id 로 알아본다
— 웹 코드가 모르는 id 가 있으면 웹 검사를 하지 않는다(장비가 마지막 판단). 문구 틀·해석 순서는 웹 코드에 옮겨 있다.

- blocks: 블록 정의(blocks.BLOCK_DEFS.public — 종류·값 형식·범위·enum·기본값("cfg:키" = 펼칠 때 설정)·필수·챔버 전용·
  필요 지원(Caps)·장비를 건드림(자동 끝 정지 판단)·건너뛰기·일시정지·이어가기), 블록 키·한도·자동 끝 정지 이름,
  값 사이 규칙(VALUE_RULES), 끝 정지 부분(뺄 수 있는 것), 시퀀스 편집, 공정 중 수정 항목, 이어가기 확인 항목.
- cfg: 검사·템플릿이 읽는 설정 키와 코드 기본값 — 채널 키 CHn_키 → 공통 키 → 기본값(process_steps._cget).
  stepCfgDefaults 는 step_cfg({}, n) 그대로(설정이 비었을 때), caps 는 caps_from_cfg 규칙 설명.
- recipe: v1 행 열(TEMPLATE_COLUMNS·EXTRA_COLUMNS·COLUMN_DOC), E230 행 값 검사 표(recipe_check RAW_*·T/F 글자·최대 20건),
  시작 전 값 검사 한도(히터 램프·SOAK·DC Pulse), 기간·delay 행 패턴.
골든: v2/tools/make_web_golden_ch12.py(웹 TS 가 같은 결과를 내는지 — npm run ops2:golden).
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any, Dict, Optional

from v2.ch12 import blocks as BK
from v2.ch12 import process_steps as PS
from v2.ch12 import recipe as RC
from v2.ch12 import recipe_check as CK

# 정의 모양이 바뀌면 올린다 — 웹(lib/ops2/ch12/blocks.ts WEB_BLOCKS_VERSION)이 다르면 웹 검사를 하지 않는다
WEB_BLOCKS_VERSION = 1

# 값 사이·장비 지원 규칙(blocks._check_one 의 코드 순서 그대로) — 웹은 id 를 알아보고 같은 계산을 한다.
# kind: error(오류) · warn(경고). msg: 문구 틀({pre} = "블록 {번호}({종류})").
VALUE_RULES: tuple = (
    dict(id="chamber_only", blocks=["*"], kind="error", when="정의 chamber 가 있고 지금 챔버와 다름",
         msg="{pre}: CH{chamber} 에서는 쓸 수 없음(CH{def.chamber} 전용)"),
    dict(id="needs_cap", blocks=["*"], kind="error", when="정의 needs(Caps 필드)가 꺼져 있음",
         msg="{pre}: 이 챔버 설정에서 꺼져 있음(SUPPORTS_{NEEDS})"),
    dict(id="gas_flow_blank", blocks=["gas_intro"], kind="warn", when="가스 true · 그 유량 값이 형식 오류 아님 · 유량 비움",
         msg="{pre}: {g}_flow 비움 → 0 sccm(v1 공란 = 0)"),
    dict(id="ch1_no_n2", blocks=["gas_prep", "gas_intro"], kind="error", when="CH1 · n2 true",
         msg="{pre}: n2: CH1 은 N2 라인 없음"),
    dict(id="shutter_gun_exists", blocks=["shutter"], kind="error",
         when="targets 형식 오류 아님 · ms 가 아닌 대상이 설정 PROCESS_GUN_SHUTTERS(소문자)에 없음",
         msg="{pre}: targets: 이 챔버에 없는 건 셔터 {miss}(PROCESS_GUN_SHUTTERS)"),
    dict(id="power_cap", blocks=["power_on"], kind="error",
         when="dc_power·dc2_power·rf_power·dcp_power·rfp_power 순서로 — 숫자인데 Caps(dc·dc2·rf·dcp·rfp)가 꺼짐",
         msg="{pre}: {k}: 이 챔버는 {lab} 미지원"),
    dict(id="pulse_exclusive", blocks=["power_on"], kind="error", when="dcp_power·rfp_power 둘 다 숫자",
         msg="{pre}: RF Pulse와 DC Pulse는 동시에 사용할 수 없습니다 (같은 시리얼 포트)"),
    dict(id="rfp_freq_range", blocks=["power_on"], kind="error",
         when="rfp_freq_khz 숫자 · 0.001 ≤ 값 ≤ RFPULSE_FREQ_MAX_HZ/1000(0 이하·오류면 30) 아님",
         msg="{pre}: rfp_freq_khz: 허용 범위 0.001~{fmax:g}kHz 밖"),
    dict(id="dcp_off_range", blocks=["power_on"], kind="error",
         when="dcp_off_us 형식 오류 아님 · 비우지 않음 — recipe_check.check_dc_pulse(freq = dcp_freq_khz 숫자면, off = 'DC' 글 또는 "
              "round(값, 1)) 의 'DC Pulse Freq' 로 시작하지 않는 오류",
         msg="{pre}: {check_dc_pulse 문구}"),
    dict(id="heater_limit", blocks=["heater_set"], kind="error",
         when="target_c 숫자 · 값 > min(HEATER_MAX_TEMP, HEATER_SV_LIMIT_C)(recipe_check.heater_limit_c)",
         msg="{pre}: target_c: {v:g} > {lim:g}°C"),
    dict(id="main_change_together", blocks=["main"], kind="error",
         when="change_after_sec·change_kind·change_power 중 일부만 비우지 않음",
         msg="{pre}: change_after_sec·change_kind·change_power 는 함께 넣어야 함"),
    dict(id="heater_wait_target", blocks=["heater_wait"], kind="error",
         when="(오류 없을 때 펼치기) target_c 비움 · 목록에서 앞에 heater_set 이 없음 — 첫 번째 것만",
         msg="펼치기 실패: 히터 도달 대기: 목표 없음(앞에 히터 설정 블록이 없고 target_c 도 비었다)"),
)

# 검사·템플릿이 읽는 설정 키(채널 키 CHn_키 → 공통 키 → 기본값)
CFG_KEYS: tuple = (
    dict(key="PROCESS_GUN_SHUTTERS", field="gun_shutters", type="list", use="셔터 블록 대상 검사·기본 템플릿 건 셔터 순서"),
    dict(key="PROCESS_DEFAULT_BASE_PRESSURE", field="base_pressure", type="float", use="base_wait target_torr 기본(cfg:)"),
    dict(key="PROCESS_DEFAULT_OES_INTEGRATION_MS", field="integration_ms", type="int", use="main·thickness_main 적분 기본(cfg:)"),
    dict(key="PC_PRESSURE_WAIT_TIMEOUT_S", field="pressure_wait_timeout_s", type="float", use="압력 대기 최대(cfg:)"),
    dict(key="PC_RF_PULSE_POST_ON_DELAY_MS", field="rf_pulse_post_on_delay_ms", type="int", use="기본 템플릿 Power Delay"),
    dict(key="PC_WORKING_PRESSURE_BOOST_TARGET", field="boost_target", type="float", use="SP2 부스트 목표(cfg:)·부스트 여부"),
    dict(key="PC_POWER_SELECT_SETTLE_MS", field="power_select_settle_ms", type="int", use="기본 템플릿 Power Select 안정화"),
    dict(key="RFPULSE_FREQ_MAX_HZ", field="", type="float", use="RF Pulse 주파수 상한(Hz, 0 이하·오류면 30000)"),
    dict(key="SUPPORTS_DC_CONT", field="", type="bool", use="caps.dc(없으면 SUPPORTS_DC, 그것도 없으면 CH2)"),
    dict(key="SUPPORTS_DC", field="", type="bool", use="caps.dc"),
    dict(key="SUPPORTS_DC_CONT2", field="", type="bool", use="caps.dc2(없으면 SUPPORTS_DC2, 없으면 false) — CH2 · 히터 꺼짐일 때만"),
    dict(key="SUPPORTS_DC2", field="", type="bool", use="caps.dc2"),
    dict(key="SUPPORTS_RF_CONT", field="", type="bool", use="caps.rf(CH2 는 늘 true, CH1 은 없으면 false)"),
    dict(key="SUPPORTS_DC_PULSE", field="", type="bool", use="caps.dcp(없으면 SUPPORTS_DCPULSE, 없으면 CH1)"),
    dict(key="SUPPORTS_DCPULSE", field="", type="bool", use="caps.dcp"),
    dict(key="SUPPORTS_RFPULSE", field="", type="bool", use="caps.rfp(없으면 SUPPORTS_RF_PULSE, 없으면 CH2)"),
    dict(key="SUPPORTS_RF_PULSE", field="", type="bool", use="caps.rfp"),
    dict(key="SUPPORTS_HEATER", field="", type="bool", use="caps.heater(CH2 만)"),
    dict(key="SUPPORTS_THICKNESS", field="", type="bool", use="caps.thickness"),
)
# 채널 키 규칙 없이 그대로 읽는 키(heater_limit_c — 기본 900·900, SV ≤ 0 → 500, MAX ≤ 0 또는 > SV → SV)
CFG_PLAIN_KEYS: tuple = (
    dict(key="CH2_HEATER_SV_LIMIT_C", default=900.0, use="히터 목표 상한"),
    dict(key="CH2_HEATER_MAX_TEMP", default=900.0, use="히터 목표 상한"),
)


def _step_cfg_defaults(ch: int) -> Dict[str, Any]:
    d = dataclasses.asdict(PS.step_cfg({}, ch))
    d["gun_shutters"] = list(d["gun_shutters"])
    return d


def definitions() -> Dict[str, Any]:
    """웹 검사 정의 전부(현재값 없음 — content_hash 대상)."""
    return {
        "version": WEB_BLOCKS_VERSION,
        "blocks": {
            "defs": [d.public() for d in BK.BLOCK_DEFS],
            "blockKeys": list(BK.BLOCK_KEYS),
            "limits": {"labelMax": BK.LABEL_MAX, "maxBlocks": BK.MAX_BLOCKS},
            "autoEndStopLabel": BK.AUTO_END_STOP_LABEL,
            "empty": "빈 값 = null 또는 공백뿐인 문자열(숫자 0 은 빈 값 아님). parts 형식은 [] 도 빈 값. "
                     "params 가 없거나 거짓 값([]·\"\"·0·false)이면 {}. label 빈 값 = \"\"",
            "rules": "값 범위·형식·장비 지원·같은 블록 안 값의 짝만 — 순서·조합 규칙 없음(v1 행 조합 검사는 v1 행에만)",
            "valueRules": [dict(r) for r in VALUE_RULES],
            "shutdownParts": [{"id": i, "label": lab, "skippable": sk} for i, lab, sk in PS.SHUTDOWN_PARTS],
            "endStopMustKeep": list(PS.SHUTDOWN_MUST_KEEP),
            "resumeChecks": dict(BK.RESUME_CHECKS),
            "edit": {"ops": list(BK.EDIT_OPS), "reasons": ["EDIT_STARTED_BLOCK", "EDIT_BAD_OP", "EDIT_INVALID"],
                     "desc": "아직 시작 안 한 블록만 추가·삭제·순서 변경(started_upto = 시작한 맨 위 블록 수), 편집마다 전체 재검사 "
                             "· 통과하면 판 번호 +1"},
            "adjust": {"command": "PROCESS_ADJUST", "items": [a.public() for a in BK.ADJUST_ITEMS]},
        },
        "cfg": {
            "keys": [dict(k) for k in CFG_KEYS],
            "plainKeys": [dict(k) for k in CFG_PLAIN_KEYS],
            "channelRule": "CH{n}_키 값이 null 이 아니면 그것, 아니면 키 값, 둘 다 null 이면 기본값. 목록·사전 기본값인 키는 글이면 JSON",
            "stepCfgDefaults": {str(ch): _step_cfg_defaults(ch) for ch in (1, 2)},
            "rfpFreqMaxHzDefault": 30000.0,
        },
        "recipe": {
            "templateColumns": list(RC.TEMPLATE_COLUMNS),
            "extraColumns": list(RC.EXTRA_COLUMNS),
            "columnDoc": {k: {"desc": v[0], "unit": v[1], "blank": v[2], "key": v[3]} for k, v in RC.COLUMN_DOC.items()},
            "tfTrue": list(RC.TF_TRUE), "tfFalse": list(RC.TF_FALSE),
            "raw": {"code": CK.E230, "prefix": CK.RECIPE_ERROR_PREFIX, "maxShown": CK.MAX_SHOWN,
                    "floatKeys": list(CK.RAW_FLOAT_KEYS),
                    "rangeKeys": {k: list(v) for k, v in CK.RAW_RANGE_KEYS.items()},
                    "intKeys": list(CK.RAW_INT_KEYS), "offTimeKeys": list(CK.RAW_OFF_TIME_KEYS),
                    "tfKeys": list(CK.RAW_TF_KEYS), "tfTrue": list(CK.TF_TRUE_TOKENS), "tfFalse": list(CK.TF_FALSE_TOKENS),
                    "enumKeys": {k: list(v) for k, v in CK.RAW_ENUM_KEYS.items()},
                    "durationKeys": list(CK.RAW_DURATION_KEYS),
                    "pair": ["change_power_value", "power_change_time"]},
            "norm": {"heaterRampMax": CK.HEATER_RAMP_MAX_C_PER_MIN, "heaterSoakMax": CK.HEATER_SOAK_MAX_MIN,
                     "dcpFreq": [CK.DCP_FREQ_MIN_KHZ, CK.DCP_FREQ_MAX_KHZ],
                     "dcpOff": [CK.DCP_OFF_MIN_US, CK.DCP_OFF_ABS_MAX_US]},
            "durationPattern": CK._HMS_RE.pattern,
            "delayNamePattern": RC._DELAY_NAME_RE.pattern,
        },
    }


def content_hash(defs: Optional[Dict[str, Any]] = None) -> str:
    d = definitions() if defs is None else defs
    return hashlib.sha256(json.dumps(d, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
