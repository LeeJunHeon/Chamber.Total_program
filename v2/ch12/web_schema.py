"""챔버12 웹 설정 내보내기 묶음(사용자 지시 3장 (7)) — 웹이 이 묶음 하나로 설정·명령·레시피·블록·스냅샷 화면을 그린다.
챔버K v2/chk/web_schema.py 와 같은 모양·규칙(필드 의미·hash 규칙·EXPORT_VERSION 히스토리) — 챔버12 에만 있는 것은 chambers.

묶음(JSON 하나, bundle()):
  version · hash(정의 내용의 sha256 — 현재값 제외) · paramSchemaHash(params.table().schema_hash — CONFIG_APPLY 의 schemaHash) ·
  configVersion · overriddenKeys(노트북 덮어쓰기가 정한 키, 비밀값 제외)
  params        파라미터 정의(키·이름·묶음·단위·형식·범위·허용값·기본값·현재값 current(실행 값 — 덮어쓰기 반영)·stored(저장 값 —
                config_active, 웹이 첫 설정 버전·CONFIG_APPLY 에 쓰는 값)·overridden·pending·안전 등급·적용 시점·범위(scope)·역할·
                v1 근거·설명·webLocked(웹에서 못 바꾸는 키 — RUN_MODE·EQUIPMENT_ID·UPLINK_IN_SIM)). 비밀값은 default·current·
                stored·pending 모두 null. float 형식 설정값은 늘 float.
  crossRules    키 사이 규칙 설명 · dropped v2 에서 뺀 v1 키(이유·v1 근거·대체 키)
  commands      명령(종류·출처·역할·허용 상태·인자·benchOk …) · preconditions · reasons(사유 코드) · plcButtons
                — 4단계 commands.py 의 public()(또는 COMMANDS 각 항목의 public()). 그 모듈이 아직 없으면 빈 목록.
  recipe        v1 행 레시피 열·E230 행 값 검사 표(web_blocks.definitions().recipe)
  blocks        블록 정의·값 사이 규칙·끝 정지 부분·시퀀스 편집·이어가기 확인(web_blocks.definitions().blocks) + version
                (WEB_BLOCKS_VERSION — 웹 TS 와 다르면 웹 검사를 하지 않음) + cfg(검사·템플릿이 읽는 설정 키·채널 규칙)
  adjust        공정 중 수정 항목(PROCESS_ADJUST — blocks.ADJUST_ITEMS)
  stopSequence  STOP·이상 중단·끝 정지 순서 — 4단계 safety_rules.py 의 public()(없으면 {"fixed": true, "steps": []})
  states        공정 상태 이름(process_states.STATES — 3단계, 없으면 []) · events 웹이 받는 공정 이벤트(종류·data 키)
  eventKinds    event 메시지의 (context.source, context.kind) → 화면 이름·level(main 평소 로그 / detail 상세에서만).
                warn·error 는 표와 상관없이 늘 보이고, 표에 없는 짝은 detail 로 본다. 챔버는 context.chamber(ch1·ch2·pc·tsp·shared)
  paramGroups   설정(params[].group) 묶음 key → 화면 이름(모든 group 이 있다 — 시험)
  snapshot      1초 스냅샷 열 정의 — tick·process(공정 맥락 <c>.proc.*·목표값 <c>.set.*·프로그램 상태 prog.*)·장비 블록(이름·라벨·
                worker(그 블록을 채우는 1초 읽기 소스 — ch2.dc·ch2.rf·pc.rf 는 plc)·품질 열 <블록>.q·<블록>.age_s) → 필드(column·
                형식 fmt 는 파이썬 format spec 그대로·enumLabels·stateColumn). 열 집합(columns) = hello.snapshotColumns.
  chambers      CH1·CH2·플라즈마 클리닝·TSP 구성 — 블록(1초 행)·가스·목표값 열·공정 맥락 열 접두어, 지원 장비 규칙(supportKeys —
                채널 키 CHn_키 → 공통 키, process_steps.caps_from_cfg)과 지금 값 supports(bundle 만 — 정의에서는 null)·히터·두께.
범위가 파라미터 값에 묶인 항목(paramMin·paramMax)은 웹이 params 의 현재값으로 계산한다.
hash 는 정의만으로 계산한다(현재값·대기값·supports 제외) — 내용이 바뀌면 시험(test_web_schema)의 PINNED 가 달라져 실패한다 —
EXPORT_VERSION 을 올리고 아래 히스토리에 무엇이 바뀌었는지 적는다. 4단계 commands·safety_rules, 3단계 process_states 가 생기면
내용이 바뀌므로 그때 올린다.

버전 히스토리
  1  첫 판(7단계 — 챔버K 묶음 버전 12 의 필드 의미 그대로 + chambers, 챔버별 공정 맥락)
  2  4단계 — commands(명령 표 58+·portalOnly·webOnly·target·PLC 버튼 51·사유 표)·stopSequence(safety_rules.public — 챔버 정지 순서·
     끝 정지 금지 단계·PC/TSP 정지·allStopSequence·powerOffOnly·규칙 S1~S36)·states(process_states.STATES), 1초 행 블록
     ch2.heater(PLC 히터 해독 + 엔진 열)·ch2.thk(두께 판정 — 엔진 열) 추가(chambers.ch2.blocks 에도)
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from typing import Any, Dict, Mapping, Optional

from v2.common.paramdef import Scope
from v2.common.snapshot import TICK_COLUMNS
from v2.ch12 import snapshot_fields as SF
from v2.ch12 import web_blocks as WB
from v2.ch12.hub import optional_module

EXPORT_VERSION = 2

# 웹이 받는 공정 이벤트(엔진이 system.emit(source="process:<c>", kind, event=data) 로 내는 것 — hub 가 run_* 메시지로 바꾼다)
EVENTS: tuple = (
    ("run_start", "행 시작 → run_start 메시지(chamber)", ("run_id", "recipe_name", "by", "name", "step_no", "params", "blocks")),
    ("run_end", "행 끝 → run_end 메시지(chamber)", ("run_id", "status", "reason", "summary", "name")),
)

# 설정(params[].group) → 화면 이름
PARAM_GROUPS: tuple = (
    ("install", "설치·장비"), ("secret", "비밀값"), ("tick", "1초 읽기"), ("plc", "PLC"), ("chamber", "챔버 공통"),
    ("mfc", "MFC"), ("ig", "이온 게이지"), ("dc", "DC 전원"), ("rf", "RF 전원"), ("dcpulse", "DC Pulse"),
    ("rfpulse", "RF Pulse"), ("tsp", "TSP 장비"), ("tsp_run", "TSP 운전"), ("stm100", "두께 모니터(STM-100)"),
    ("thickness", "두께"), ("heater", "히터"), ("heater_plc", "히터 PLC 설정"), ("heater_ramp", "히터 승온"),
    ("heater_hold", "히터 유지 모드"), ("heater_noresp", "히터 무응답"), ("process", "공정"), ("pc", "플라즈마 클리닝"),
    ("eta", "남은 시간(ETA)"), ("stop", "정지 순서"), ("resume", "재시작 이어가기"), ("command", "명령"),
    ("robot", "로봇"), ("robot_server", "로봇 서버"), ("camera", "카메라"), ("oes", "OES"), ("rga", "RGA"),
    ("outputs", "v1 기록 출력"), ("record", "기록"), ("uplink", "웹 연결"), ("chat", "구글챗"), ("monitor", "감시"),
    ("apps", "보조 프로그램"), ("debug", "디버그"),
)

# event 메시지 (context.source, context.kind) → (화면 이름, level). hub.origin_of 가 정하는 source:
#   process(엔진) · safety · heater · uplink · 장비 이름(plc·mfc·ig·dcpulse·rfpulse·dc·rf·tsp·stm100·camera·oes·rga·pulse …).
#   main = 사람이 공정 흐름을 따라가는 데 필요한 것, detail = 되읽기·자기 진단·주기 값 등(상세에서만).
_M, _D = "main", "detail"
_WORKERS = ("plc", "mfc", "ig", "dcpulse", "rfpulse", "tsp", "stm100")
EVENT_KINDS: tuple = (
    ("process", "run_start", "공정 시작", _M), ("process", "run_end", "공정 끝", _M),
    ("process", "internal_error", "내부 오류", _M), ("process", "log", "공정 기록", _D),
    ("safety", "fired", "안전 규칙 발동", _M), ("safety", "released", "안전 규칙 해제", _M),
    ("heater", "log", "히터 기록", _D), ("heater", "card", "히터 알림 카드", _M),
    ("plc", "link_up", "PLC 연결 복구", _M), ("plc", "link_down", "PLC 연결 끊김", _M),
    ("plc", "conn_change", "PLC 연결 상태", _D), ("plc", "coil_plan", "코일 읽기 계획", _D), ("plc", "log", "PLC 기록", _D),
    ("plc", "read", "PLC 읽기", _D),
    ("mfc", "command_confirmed", "명령 확인", _D), ("mfc", "command_failed", "명령 실패", _M), ("mfc", "flow", "유량", _D),
    ("mfc", "pressure", "압력", _D), ("mfc", "read", "읽기", _D), ("mfc", "status", "상태", _D), ("mfc", "io", "입출력", _D),
    ("ig", "base_reached", "베이스 압력 도달", _M), ("ig", "base_failed", "베이스 압력 실패", _M),
    ("ig", "pressure", "압력", _D), ("ig", "read", "읽기", _D), ("ig", "status", "상태", _D), ("ig", "io", "입출력", _D),
    ("dc", "target_reached", "DC 목표 도달", _M), ("dc", "target_failed", "DC 목표 실패", _M),
    ("dc", "power_off_finished", "DC 꺼짐", _M), ("dc", "power_off_failed", "DC 끄기 실패", _M),
    ("dc", "state_changed", "DC 상태", _D), ("dc", "display", "DC 표시", _D), ("dc", "status", "DC 상태", _D),
    ("rf", "target_reached", "RF 목표 도달", _M), ("rf", "target_failed", "RF 목표 실패", _M),
    ("rf", "power_off_finished", "RF 꺼짐", _M), ("rf", "power_off_failed", "RF 끄기 실패", _M),
    ("rf", "state_changed", "RF 상태", _D), ("rf", "display", "RF 표시", _D), ("rf", "status", "RF 상태", _D),
    ("dcpulse", "command_confirmed", "명령 확인", _D), ("dcpulse", "command_failed", "명령 실패", _M),
    ("dcpulse", "arc_threshold_reached", "아크 기준 넘음", _M), ("dcpulse", "telemetry", "측정값", _D),
    ("dcpulse", "status", "상태", _D),
    ("rfpulse", "target_reached", "목표 도달", _M), ("rfpulse", "command_failed", "명령 실패", _M),
    ("rfpulse", "power_off_finished", "꺼짐", _M), ("rfpulse", "rf_status", "RF 상태", _D), ("rfpulse", "power", "파워", _D),
    ("rfpulse", "status", "상태", _D),
    ("tsp", "command_result", "TSP 명령 결과", _M), ("tsp", "link_closed", "TSP 연결 닫음", _D),
    ("stm100", "zero", "두께 0 맞춤", _M), ("stm100", "material_applied", "재료 적용", _M),
    ("stm100", "unresponsive", "두께 모니터 무응답", _M),
    ("stm100", "job_failed", "두께 작업 실패", _M), ("stm100", "config_warning", "두께 설정 경고", _M),
    ("stm100", "unavailable", "두께 모니터 없음", _M), ("stm100", "reset_flag", "리셋 표시", _D),
    ("stm100", "reset_ack_failed", "리셋 확인 실패", _D),
    ("oes", "finished", "OES 측정 끝", _M), ("oes", "rejected", "OES 거부", _M), ("oes", "worker_exit", "OES 워커 종료", _M),
    ("oes", "status", "OES 상태", _D),
    ("rga", "finished", "RGA 측정 끝", _M), ("rga", "failed", "RGA 실패", _M), ("rga", "rejected", "RGA 거부", _M),
    ("rga", "data", "RGA 값", _D), ("rga", "status", "RGA 상태", _D),
    ("selfwatch", "selfwatch", "프로그램 자기 감시", _M), ("outputs", "internal_error", "기록 출력 오류", _M),
    ("uplink", "uplink_down", "웹 연결 끊김", _M), ("uplink", "uplink_up", "웹 연결 다시 됨", _M),
    ("uplink", "uplink_auth_failed", "웹 토큰 거부", _M), ("uplink", "uplink_conflict", "웹이 이 실행을 받지 않음", _M),
    ("uplink", "uplink_too_large", "웹이 받지 않는 큰 메시지", _M), ("uplink", "uplink_spool_full", "웹 보관함 가득", _M),
    ("uplink", "uplink_gap", "웹으로 올릴 메시지 놓침", _M), ("uplink", "uplink_spool_loaded", "지난 실행 메시지 이어 올림", _D),
    ("uplink", "uplink_bad_command", "웹 명령 형식 오류", _M), ("uplink", "uplink_spool_error", "보관함 쓰기 실패", _M),
) + tuple(x for w in _WORKERS for x in (
    (w, "connected", "장비 연결됨", _D), (w, "connect_failed", "연결 실패", _D), (w, "disconnected", "장비 연결 끊김", _M),
    (w, "long_outage", "장비 연결 오래 끊김", _M), (w, "timeout", "응답 없음", _D),
    (w, "recovered", "다시 응답", _M if w == "stm100" else _D),
    (w, "COMMAND_BLOCKED", "명령 막힘", _M),
))

# 챔버 구성(정의) — 지원 장비 지금 값(supports)은 bundle 에서 caps_from_cfg 로
_SUPPORT_KEYS = {"dc": ("SUPPORTS_DC_CONT", "SUPPORTS_DC"), "dc2": ("SUPPORTS_DC_CONT2", "SUPPORTS_DC2"),
                 "rf": ("SUPPORTS_RF_CONT",), "dcp": ("SUPPORTS_DC_PULSE", "SUPPORTS_DCPULSE"),
                 "rfp": ("SUPPORTS_RFPULSE", "SUPPORTS_RF_PULSE"), "heater": ("SUPPORTS_HEATER",),
                 "thickness": ("SUPPORTS_THICKNESS",)}
CHAMBERS: tuple = (
    dict(id="ch1", label="CH1", kind="chamber", channel=1, gases=["ar", "o2"],
         blocks=["ch1.mfc", "ch1.ig", "ch1.dcpulse", "ch1.rfpulse", "ch1.stm100", "ch1.oes", "ch1.rga"],
         shared=["plc", "camera"], heater=False, note="CH1 가스는 Ar·O2 만(N2 라인 없음)"),
    dict(id="ch2", label="CH2", kind="chamber", channel=2, gases=["ar", "o2", "n2"],
         blocks=["ch2.mfc", "ch2.ig", "ch2.dcpulse", "ch2.rfpulse", "ch2.dc", "ch2.rf", "ch2.heater", "ch2.stm100", "ch2.thk",
                 "ch2.oes", "ch2.rga"],
         shared=["plc", "camera"], heater=True,
         note="CH2 히터(VARITAP VSCD-30 + PLC PID, 래더 워치독) · DC·RF 연속은 PLC D/A(ch2.dc·ch2.rf) · DC2 는 히터 없을 때만"),
    dict(id="pc", label="플라즈마 클리닝", kind="pc", channel=None, gases=["ar", "o2", "n2"], blocks=["pc.rf"],
         shared=["plc"], heater=False, note="PC RF 는 PLC D/A(pc.rf) — 채널(CH1·CH2)은 공정이 고른다"),
    dict(id="tsp", label="TSP", kind="tsp", channel=None, gases=[], blocks=["tsp", "tsp.ig"], shared=["ch1.ig"],
         heater=False, note="TSP 게이지는 CH1 IG worker 공유(D67)"),
)


def _param(p, values: Optional[Mapping[str, Any]], pending: Mapping[str, Any],
           stored: Optional[Mapping[str, Any]] = None, overridden: frozenset = frozenset()) -> dict:
    secret = p.scope is Scope.SECRET
    if secret or values is None:
        st = ov = None
    else:
        st = stored.get(p.key, p.default) if stored is not None else values.get(p.key)
        ov = p.key in overridden
    return {"key": p.key, "label": p.label, "group": p.group, "unit": p.unit, "type": p.type.value,
            "min": p.min, "max": p.max, "enum": list(p.enum) if p.enum else None, "pattern": p.pattern,
            "default": None if secret else p.default,
            "current": None if (secret or values is None) else values.get(p.key),
            "stored": st, "overridden": ov,
            "pending": None if secret else pending.get(p.key),
            "safety": p.safety.value, "apply": p.apply.value, "scope": p.scope.value, "role": p.role.value,
            "v1Key": p.v1_key, "v1Source": p.v1_source, "v1Default": p.v1_default, "pendingConfirm": p.pending_confirm,
            "webLocked": p.web_locked, "desc": p.desc, "note": p.note}


def _field(f, column: str) -> dict:
    return {"column": column, "name": f.name, "type": f.type.value, "unit": f.unit, "label": f.label, "fmt": f.fmt,
            "summary": f.summary, "ownQuality": f.own_quality, "detail": f.detail, "desc": f.desc,
            "enumLabels": dict(f.enum_labels) if f.enum_labels else None,
            "stateColumn": f"{column.rsplit('.', 1)[0]}.{f.state}" if f.state else None}


def snapshot() -> Dict[str, Any]:
    return {"tick": [_field(f, f.name) for f in TICK_COLUMNS],
            "process": [_field(f, f.name) for f in SF.EXTRA_FIELDS],
            "blocks": [{"name": b.name, "label": b.label, "desc": b.desc, "worker": SF.BLOCK_SOURCE[b.name],
                        "quality": [f"{b.name}.q", f"{b.name}.age_s"],
                        "fields": [_field(f, f"{b.name}.{f.name}") for f in b.fields]} for b in SF.device_blocks()],
            "columns": SF.columns()}


def _public(mod, name: str) -> Any:
    """모듈의 public() 또는 표 이름 각 항목의 public()."""
    f = getattr(mod, "public", None)
    if callable(f):
        return f()
    return [x.public() if hasattr(x, "public") else (dataclasses.asdict(x) if dataclasses.is_dataclass(x) else x)
            for x in getattr(mod, name, ())]


def commands_part() -> Dict[str, Any]:
    out: Dict[str, Any] = {"commands": [], "preconditions": [], "reasons": [], "plcButtons": []}
    C = optional_module("v2.ch12.commands")
    if C is None:
        return out
    got = _public(C, "COMMANDS")
    if isinstance(got, dict):
        out.update({k: list(got.get(k) or []) for k in out})
    else:
        out["commands"] = list(got)
    return out


def stop_part() -> Dict[str, Any]:
    SR = optional_module("v2.ch12.safety_rules")
    if SR is None:
        return {"fixed": True, "steps": []}
    got = _public(SR, "STOP_SEQUENCE")
    return got if isinstance(got, dict) else {"fixed": True, "steps": list(got)}


def states_part() -> list:
    PS = optional_module("v2.ch12.process_states")
    if PS is None:
        return []
    return [{"name": s.name, "label": s.label, "kind": getattr(getattr(s, "kind", None), "value", getattr(s, "kind", None))}
            for s in getattr(PS, "STATES", ())]


def chambers(values: Optional[Mapping[str, Any]] = None) -> list:
    from v2.ch12.process_steps import caps_from_cfg
    out = []
    for c in CHAMBERS:
        d = dict(c, procPrefix=f"{c['id']}.proc.", targets=[f"{c['id']}.set.{n}" for n in SF.TARGETS[c["id"]]],
                 supportKeys={k: list(v) for k, v in _SUPPORT_KEYS.items()} if c["kind"] == "chamber" else {},
                 supports=None)
        if values is not None and c["kind"] == "chamber":
            caps = dataclasses.asdict(caps_from_cfg(values, c["channel"]))
            d["supports"] = {k: bool(caps[k]) for k in _SUPPORT_KEYS}
        out.append(d)
    return out


def definitions() -> Dict[str, Any]:
    """현재값을 뺀 정의 전부(hash 대상)."""
    from v2.ch12.params import table
    t = table()
    wb = WB.definitions()
    cmd = commands_part()
    return {
        "params": [_param(p, None, {}) for p in t.params],
        "crossRules": [{"id": r.id, "keys": list(r.keys), "desc": r.desc} for r in t.rules],
        "dropped": [{"key": d.key, "reason": d.reason, "v1Source": d.v1_source, "replacedBy": d.replaced_by}
                    for d in t.dropped],
        "commands": cmd["commands"], "preconditions": cmd["preconditions"], "reasons": cmd["reasons"],
        "plcButtons": cmd["plcButtons"],
        "recipe": wb["recipe"],
        "blocks": dict(wb["blocks"], version=wb["version"], cfg=wb["cfg"]),
        "adjust": wb["blocks"]["adjust"],
        "stopSequence": stop_part(),
        "states": states_part(),
        "events": [{"kind": k, "label": lab, "data": list(keys)} for k, lab, keys in EVENTS],
        "eventKinds": [{"source": s, "kind": k, "label": lab, "level": lv} for s, k, lab, lv in EVENT_KINDS],
        "paramGroups": [{"key": k, "label": lab} for k, lab in PARAM_GROUPS],
        "snapshot": snapshot(),
        "chambers": chambers(None),
    }


def content_hash(defs: Optional[Dict[str, Any]] = None) -> str:
    raw = json.dumps(defs if defs is not None else definitions(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def bundle(values: Optional[Mapping[str, Any]] = None, pending: Optional[Mapping[str, Any]] = None,
           config_version: Optional[int] = None, stored: Optional[Mapping[str, Any]] = None,
           overridden=()) -> Dict[str, Any]:
    """웹으로 보내는 묶음. values = 지금 실행 값(ConfigStore.values), pending = 대기 중 값(idle·restart),
    stored = 저장 값(ConfigStore.stored — 없으면 실행 값과 같다고 봄), overridden = 덮어쓰기가 정한 키."""
    from v2.ch12.params import table
    t = table()
    d = definitions()
    h = content_hash(d)
    ov = frozenset(overridden or ())
    d["params"] = [_param(p, values, dict(pending or {}), stored, ov) for p in t.params]
    d["chambers"] = chambers(values)
    return dict({"version": EXPORT_VERSION, "hash": h, "paramSchemaHash": t.schema_hash(),
                 "configVersion": config_version, "overriddenKeys": sorted(ov)}, **d)


def bundle_for(values: Mapping[str, Any], store, config_version: Optional[int]) -> Dict[str, Any]:
    """실행 값 + ConfigStore(대기 값·저장 값·덮어쓰기 키)로 묶음 — 로컬 /api/v1/schema·hello webSchema 가 같이 쓴다."""
    if store is None:
        return bundle(values, {}, config_version)
    return bundle(values, dict(store.pending_idle, **store.pending_restart), config_version, store.stored(),
                  store.overridden())


def to_json(b: Dict[str, Any]) -> str:
    return json.dumps(b, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
