"""화면 글 용어표(2026-11-24 묶음 16 — 챔버K 묶음 21 과 같은 용어). 장비가 묶음으로 내보내는 이름·설명이 용어표를 따르는지 본다.

- 옛 말(용어표 괄호 안의 말)이 이름(label)·운전자용 설명(help)·값 이름(enum)·사건 이름·출처·사유 이름/설명에 없다
- 같은 대상은 같은 이름: PLC 버튼 이름 = short = 그 코일의 스냅샷 필드 이름, 상태 이름 = 그 상태를 만드는 블록 이름(같은 것만)
- 이름 길이: 필드 16·상태 14·사건 18·명령 20·사유 26, help 70자. 챔버 구분 'Loadlock'·'Plasma Cleaning' 은 'CH1' 처럼 세 글자로 센다
  (챔버 이름을 줄여 새 줄임말을 만들지 않으려고 — 예외 이유)
- 이름·help 에 괄호(SP1 같은 짧은 구분만 허용)·개발 표기(코일·설정 키·레지스터·v1·결정/규칙 번호·파일 이름)·줄표·'안 함·못 함·안 됨·켬·끔' 없음
- 평소 보기 필드는 help 가 있거나 FIELD_NAME_ONLY 에 있다, 거부·중단·실패·정보 사유는 모두 help 가 있다
- 설정(params)은 이번에 용어표 낱말만 바꿨다(이름의 PLC 주소·v1 등은 다음 차례 — desc 와 함께) → 옛 말 검사만 한다
- 진행 단계 화면 이름(step_labels): 골든의 모든 스텝 문구에 이름이 생기고 그 이름에 옛 말·괄호가 없다, 스텝 문구는 그대로
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from types import SimpleNamespace

from v2.ch12 import commands as C
from v2.ch12 import plc_labels as PL
from v2.ch12 import snapshot_fields as SF
from v2.ch12 import web_schema as W
from v2.ch12.drivers import plc_map as PM
from v2.ch12.step_labels import step_label

V2 = Path(__file__).resolve().parents[1]
GOLDEN = V2 / "tests" / "golden"

# 용어표의 옛 말(괄호 안) — 정규식. '런' 은 '런타임' 과 다르게 낱말일 때만, 'IG'·'PC' 는 낱말일 때만, O2·N2 는 O₂·N₂ 로
OLD_TERMS = (
    r"로터리", r"터보", r"메인 밸브", r"러핑", r"포어라인", r"벤트", r"게이트 밸브", r"(?<![가-힣])도어", r"로드락", r"4핀", r"척 모터",
    r"이온 게이지", r"\bIG\b", r"순방향", r"반사", r"Ref\.?P", r"플라즈마 클리닝", r"\bPC\b", r"냉각수", r"메인 셔터", r"건 셔터",
    r"\bAR 밸브", r"인터록", r"\bPV\b", r"\bSV\b", r"실측", r"되읽기", r"설정값", r"\bON\b", r"\bOFF\b", r"\bOPEN\b", r"\bCLOSED\b",
    r"(?<![가-힣])(켬|끔)(?![가-힣])", r"미연결", r"무응답", r"(?<![가-힣])런(?![타가-힣])", r"스텝", r"\bO2\b", r"\bN2\b",
    r"(?<![가-힣])로컬", r"(?<![가-힣])묶음",
)
# 개발 표기(이름·help 에 넣지 않음 — desc 로)
DEV_MARKS = (r"[A-Z][A-Z0-9]*_[A-Z0-9_]+", r"\b[a-z]+_[a-z_]+\b", r"\.py\b", r"\bv1\b", r"\b[MD]0\d{3,4}\b", r"\b[SDE]\d{2,3}\b",
             r"\bcfg:", r"\bR\d{1,2}\b")
WORDING = re.compile(r"—|안 ?(함|됨)(?![가-힣])|못 ?함(?![가-힣])|(?<![가-힣])(켬|끔)(?![가-힣])")
SHORT_PARENS = re.compile(r"\((SP\d|DC\d|G\d)\)")       # 허용하는 짧은 구분 괄호
# 괄호를 그대로 두는 곳(이유): 블록 정의 이름 '압력 부스트(SP2)'·'작업 압력(SP1)' 은 SHORT_PARENS 로 이미 허용 —
# 그 밖에 괄호가 남은 곳은 없다(빈 튜플). 생기면 이유와 함께 여기에
PAREN_OK: tuple = ()
# 한 줄에 같은 낱말을 두 번 쓰지 않는다(글 규칙 4) — 조사가 붙은 흔한 꼴은 뺀다
REPEAT_OK = {"있으면", "입니다", "합니다", "켜짐입니다"}


def _w(text: str) -> int:
    """이름 길이 — 챔버 구분 'Loadlock'·'Plasma Cleaning' 은 'CH1' 처럼 세 글자로."""
    return len((text or "").replace(PL.PC, "CH0").replace("Loadlock", "CH0"))


def _bundle():
    return W.bundle()


def _named(b) -> list:
    """(어디, 글) — 이름·설명 전부(평소 보기 화면)."""
    out = []
    for bl in b["snapshot"]["blocks"]:
        out.append((f"block {bl['name']}", bl["label"]))
        for f in bl["fields"]:
            out += [(f"field {f['column']}", f["label"]), (f"fieldhelp {f['column']}", f.get("help") or "")]
            out += [(f"fieldenum {f['column']}", v) for v in (f.get("enumLabels") or {}).values()]
    for f in b["snapshot"]["process"] + b["snapshot"]["tick"]:
        out += [(f"field {f['column']}", f["label"]), (f"fieldhelp {f['column']}", f.get("help") or "")]
        out += [(f"fieldenum {f['column']}", v) for v in (f.get("enumLabels") or {}).values()]
    out += [(f"button {x['target']}", x["label"]) for x in b["plcButtons"]]
    out += [(f"buttonshort {x['target']}", x["short"]) for x in b["plcButtons"]]
    out += [(f"command {c['kind']}", c["label"]) for c in b["commands"]]
    out += [(f"state {s['name']}", s["label"]) for s in b["states"]]
    out += [(f"event {e['source']}.{e['kind']}", e["label"]) for e in b["eventKinds"]]
    out += [(f"source {e['source']}", e["label"]) for e in b["eventSources"]]
    out += [(f"reason {r['code']}", r["label"]) for r in b["reasons"]]
    out += [(f"reasonhelp {r['code']}", r.get("help") or "") for r in b["reasons"]]
    out += [(f"group {g['key']}", g["label"]) for g in b["paramGroups"]]
    out += [(f"hazard {k}", v) for k, v in b["plcHazardText"].items()]
    for d in b["blocks"]["defs"]:
        out.append((f"blockdef {d['type']}", d["label"]))
        out += [(f"blockparam {d['type']}.{p['name']}", p.get("label") or "") for p in d["params"]]
    out += [(f"adjust {a['device']}.{a['item']}", a.get("label") or "") for a in b["adjust"]["items"]]
    ss = b["stopSequence"]
    for k in ("steps", "pcSteps", "tspSteps", "allStopSequence", "powerOffOnly"):
        out += [(f"stop {k}.{s['no']}", s["label"]) for s in ss.get(k) or []]
    out += [(f"chamber {c['id']}", c["label"]) for c in b["chambers"]]
    return out


def test_old_terms_not_in_screen_names():
    b = _bundle()
    texts = _named(b) + [(f"param {p['key']}", p["label"]) for p in b["params"]]
    bad = [(w, pat, t) for w, t in texts for pat in OLD_TERMS if re.search(pat, t or "")]
    assert bad == [], bad[:15]


def test_plc_buttons_same_name_as_fields():
    b = _bundle()
    fields = {f["column"]: f for bl in b["snapshot"]["blocks"] for f in bl["fields"]}
    for x in b["plcButtons"]:
        f = fields[f"plc.{PM.field_name(x['name'])}"]
        assert x["label"] == x["short"] == f["label"], (x["target"], x["label"], x["short"], f["label"])
        assert x["name"] in x["desc"] and x["name"] in f["desc"]                # 래더 이름은 desc 로
    # PLC 필드 142개 모두 래더 이름이 아닌 용어표 이름(래더 이름은 desc 맨 앞)
    plc = [f for c, f in fields.items() if c.startswith("plc.")]
    assert len(plc) == 142
    assert [f["label"] for f in plc if re.fullmatch(r"[A-Z0-9_가-힣a-z]+", f["label"]) and "_" in f["label"]] == []
    names = set(PM.COIL_MAP) | set(PM.P_MIRROR_MAP) | set(PM.REG_MAP)
    assert names == set(PL.COIL_LABEL)


def test_states_same_name_as_blocks():
    b = _bundle()
    st = {s["name"]: s["label"] for s in b["states"]}
    bl = {d["type"]: d["label"] for d in b["blocks"]["defs"]}
    assert st["pc.test"] == bl["test"] == "TEST 모드"
    assert st["heater.hold"] != bl["heater_hold"]          # 다른 것(유지 모드 대기 ≠ 유지 측정 대기 블록) — 같은 이름을 쓰지 않음


def test_name_lengths_and_shapes():
    b = _bundle()
    bad = []
    for bl in b["snapshot"]["blocks"]:
        bad += [("field", f["label"]) for f in bl["fields"] if _w(f["label"]) > 16]
    bad += [("field", f["label"]) for f in b["snapshot"]["process"] if _w(f["label"]) > 16]
    bad += [("state", s["label"]) for s in b["states"] if _w(s["label"]) > 14]
    bad += [("event", e["label"]) for e in b["eventKinds"] if _w(e["label"]) > 18]
    bad += [("command", c["label"]) for c in b["commands"] if _w(c["label"]) > 20 or "(" in c["label"]]
    bad += [("reason", r["label"]) for r in b["reasons"] if _w(r["label"]) > 26 or "(" in r["label"]]
    helps = [("fieldhelp", f.get("help") or "") for bl in b["snapshot"]["blocks"] for f in bl["fields"]]
    helps += [("fieldhelp", f.get("help") or "") for f in b["snapshot"]["process"]]
    helps += [("reasonhelp", r.get("help") or "") for r in b["reasons"]]
    bad += [h for h in helps if len(h[1]) > 70]
    assert bad == [], bad[:10]


def test_no_parens_dev_marks_or_banned_wording():
    b = _bundle()
    bad = []
    for where, t in _named(b):
        if not t:
            continue
        rest = SHORT_PARENS.sub("", t)
        if "(" in rest and not where.startswith(PAREN_OK):
            bad.append((where, t, "괄호"))
        for pat in DEV_MARKS:
            if re.search(pat, t):
                bad.append((where, t, pat))
        if WORDING.search(t):
            bad.append((where, t, "줄표·안 함·켬/끔"))
    assert bad == [], bad[:15]


def test_everyday_fields_have_help_and_reasons_have_help():
    b = _bundle()
    fields = [f for bl in b["snapshot"]["blocks"] for f in bl["fields"]] + list(b["snapshot"]["process"])
    miss = [f["column"] for f in fields if not f.get("detail") and not f.get("help") and f["column"] not in SF.FIELD_NAME_ONLY]
    assert miss == []
    assert [r["code"] for r in b["reasons"] if not r.get("help")] == []
    assert {"help"} <= set(b["reasons"][0]) and "help" in b["snapshot"]["blocks"][1]["fields"][0]
    assert all(h.endswith(("니다.", "세요.")) for h in (f.get("help") for f in fields) if h)
    assert all(r["help"].endswith(("니다.", "세요.")) for r in b["reasons"])


def test_event_sources_cover_event_kinds():
    b = _bundle()
    src = {e["source"]: e["label"] for e in b["eventSources"]}
    assert {e["source"] for e in b["eventKinds"]} <= set(src)
    assert src["ig"] == "Ion Gauge" and src["dc"] == "DC Power" and src["rf"] == "RF Power"


def _golden_steps():
    d = json.loads((GOLDEN / "process_combos.json").read_text(encoding="utf-8"))
    out = {}
    for case in d["cases"]:
        for key in ("steps", "emergency"):
            for s in case["v1"].get(key) or []:
                p = ast.literal_eval(s[3]) if s[3] not in ("None", None) else None
                out[(s[0], s[1])] = SimpleNamespace(action=s[0], message=s[1], params=p)
    return out


def test_step_screen_names_for_every_golden_step():
    steps = _golden_steps()
    assert len(steps) > 150
    bad = []
    for (a, m), st in steps.items():
        lab = step_label(st)
        if lab == m:
            bad.append((a, m, "이름을 만들지 못함"))
        if "(" in lab or WORDING.search(lab) or any(re.search(p, lab) for p in OLD_TERMS + DEV_MARKS):
            bad.append((a, m, lab))
        assert st.message == m                                    # 스텝 문구는 그대로(이름만 따로)
    assert bad == [], bad[:10]
    pick = {m: step_label(st) for (a, m), st in steps.items()}
    assert pick["메인 밸브 열기"] == "MV 열기" and pick["Power Select OFF (SW_POWER_SELECT)"] == "Power Select 끄기"
    assert pick["Power_select: Power Select ON (SW_RF_SELECT)"] == "RF Select 켜기"
    assert pick["[긴급] 메인 밸브 즉시 닫기"] == "긴급 MV 닫기"


def test_same_word_not_twice_in_one_line():
    """글 규칙 4 — 이름·help 한 줄에 같은 낱말(2글자 이상)을 두 번 쓰지 않는다. 확인창 문구(plcHazardText)는 여러 문장이라 뺀다."""
    b = _bundle()
    bad = []
    for where, t in _named(b):
        if not t or where.startswith("hazard"):
            continue
        ws = [w for w in re.findall(r"[A-Za-z0-9₂]+|[가-힣]+", t) if len(w) >= 2 and w not in REPEAT_OK]
        twice = sorted({w for w in ws if ws.count(w) > 1})
        if twice:
            bad.append((where, t, twice))
    assert bad == [], bad[:10]


def test_rules_catch_old_examples():
    """검사 규칙이 옛 이름(묶음 15)을 실제로 잡는다."""
    def old(t):
        return any(re.search(p, t) for p in OLD_TERMS)

    def dev(t):
        return any(re.search(p, t) for p in DEV_MARKS)
    for t in ("메인 밸브 열기", "IG 상태", "진공도(IG)", "반사", "RF 반사 감시 임계", "로드락 4핀 올림", "CH1 벤트", "도어 열기",
              "터보 꺼짐 지속", "플라즈마 클리닝 STOP", "히터 PV", "인터록 열림", "장비 무응답", "O2 유량", "로컬 화면 포트",
              "PLC 버튼 · CH1 벤트 켬", "되읽기 값 불일치", "Ar ON", "TSP OFF(사이클 간격)", "런 Soft 아크"):
        assert old(t), t
    for t in ("M_V_1_SW", "히터 SV(D112)", "R6 읽은 뒤 나이", "v1 기록 출력 큐", "게이트 밸브 CLOSED 아님(v1 E301)",
              "공정이 없어 파워만 끔(S51)", "cfg:PC_X"):
        assert dev(t) or old(t), t
    assert WORDING.search("유효 시간이 지난 웹 명령 — 실행하지 않음") and WORDING.search("파워 출력만 끔")
    for t in ("런타임 덤프", "CH1 MV", "Ion Gauge 압력", "Reflected", "Loadlock 4Pin 올림", "켜짐", "끄기", "O₂ 유량"):
        assert not old(t) and not dev(t), t
    assert _w("Loadlock Rotary 끄기 대기") == 16 and _w(f"{PL.PC} RF Power 목표") == 15
