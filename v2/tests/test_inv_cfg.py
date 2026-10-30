"""전수 목록 cfg(v2/ch12/spec/inv_cfg.py) 항목 시험 — v1 설정 키마다 v2 키·기본값·쓰는 곳을 실제로 확인한다.

test_key[<v1 경로>] (경로 = settings_v1facts KEYS 의 "common.X"·"ch1.X"·"ch2.X"·"구역.X.하위"·"(code).X")
  1) v2 대응: 변환 도구(v2/tools/convert_v1_config.Converter)와 같은 색인으로 그 v1 키를 옮기는 v2 파라미터(이름 규칙
     common.X → X, chN.X → CHN_X, 또는 ParamDef.v1_key)가 있거나, 조각 DROPPED 에 근거가 있다.
  2) 기본값: v2 기본값 = v1 실효값 — json 값, 단 ⚠ 'common 값은 효과 없음'(채널 모듈이 import 때 복사한 .py 값을 씀 — D133)
     과 settings.json 에 없는 선언 상수는 .py 값, '이 채널 값은 읽히지 않음'은 common 값(D134).
  3) 쓰는 곳: v2 코드(v2/ch12·v2/common·v2/robot_server·v2/supervisor — params·spec 조각 제외, 독스트링 제외)의 글자 상수나
     f-글자 틀에 그 키 이름(CH1_/CH2_ 접두어·f"{prefix}X" 포함)이 있다. 이름을 동적으로 만드는 곳·포털이 읽는 키는 USE 표에
     (파일, 글자) 로 적고 그 파일에 그 글자가 있는지 본다(포털 vanam-process 가 같은 상위 폴더에 없으면 포털 쪽만 건너뜀).
     버린 키(DROPPED)는 대신하는 키(replaced_by)가 같은 방식으로 쓰이는지, 또는 FIXED 표(v2 코드 상수 = v1 값)로 본다.
  na 항목(v1 에서도 읽지 않던 키): v1 사실표에 '읽는 곳 없음'이고 v2 코드에서도 쓰지 않음을 확인한다(test_unread_key).
"""
from __future__ import annotations

import ast
import json
import re
from functools import lru_cache
from pathlib import Path

import pytest

from v2.ch12.params import ownership as own
from v2.ch12.params import table as params_table
from v2.ch12.spec import inv_cfg
from v2.ch12.spec import settings_v1facts as facts
from v2.tools import convert_v1_config as conv

ROOT = Path(__file__).resolve().parents[2]
PORTAL = ROOT.parent / "vanam-process"
SCAN_DIRS = ("v2/ch12", "v2/common", "v2/robot_server", "v2/supervisor")
SKIP_DIRS = ("v2/ch12/params/", "v2/ch12/spec/", "v2/ch12/sim/")      # 시뮬레이터·시험대 설정 사전은 쓰는 곳이 아니다
FACT = {k[0]: k for k in facts.KEYS}
T = params_table()
CONV = conv.Converter(T)
_ID = re.compile(r"[A-Za-z0-9_]+")


# ───────────── v2 코드 색인(글자 상수·f-글자 틀) ─────────────
@lru_cache(maxsize=1)
def _index():
    tokens: dict = {}             # 글자 상수 안의 이름 조각 → 파일들
    templates: list = []          # (정규식, 파일) — f"{pre}_TCP_HOST" 처럼 이름을 만드는 틀
    for d in SCAN_DIRS:
        for f in sorted((ROOT / d).rglob("*.py")):
            rel = f.relative_to(ROOT).as_posix()
            if rel.startswith(SKIP_DIRS):
                continue
            tree = ast.parse(f.read_text(encoding="utf-8"))
            skip = set()
            for n in ast.walk(tree):
                if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str):
                    skip.add(id(n.value))                       # 독스트링·떠 있는 글자는 쓰는 곳이 아니다
                if isinstance(n, ast.JoinedStr):
                    parts, lits = [], []
                    for v in n.values:
                        if isinstance(v, ast.Constant):
                            skip.add(id(v))
                            parts.append(re.escape(str(v.value)))
                            lits.append(str(v.value))
                        else:
                            parts.append("[A-Za-z0-9_]*")
                    if (len(n.values) > 1 and lits and all(_ID.fullmatch(x) for x in lits)
                            and max(len(x.strip("_")) for x in lits) >= 6):     # 이름 조각이 있는 틀만(CH{n}_{x} 같은 것 제외)
                        templates.append((re.compile("".join(parts)), rel))
            for n in ast.walk(tree):
                if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in skip:
                    for tok in _ID.findall(n.value):
                        tokens.setdefault(tok, set()).add(rel)
    return tokens, templates


def uses(name: str) -> set:
    """v2 코드에서 이름 name(v1 이름 — CH1_/CH2_ 붙은 꼴·f-글자 틀 포함)을 쓰는 파일."""
    tokens, templates = _index()
    cands = (name, "_" + name, "CH1_" + name, "CH2_" + name)
    out = set()
    for c in cands:
        out |= tokens.get(c, set())
    for rx, rel in templates:
        if any(rx.fullmatch(c) for c in cands):
            out.add(rel)
    return out


def _where_ok(where) -> list:
    """[(파일, 글자)] 중 확인 못 한 것(포털 없으면 포털 항목은 건너뜀)."""
    bad = []
    for path, snippet in where:
        if path.startswith("vanam-process/"):
            p = PORTAL / path[len("vanam-process/"):]
            if not PORTAL.exists():
                continue
        else:
            p = ROOT / path
        if not p.is_file() or snippet not in p.read_text(encoding="utf-8", errors="replace"):
            bad.append((path, snippet))
    return bad


# ───────────── v1 실효값 ─────────────
def v1_effective(path: str):
    f = FACT.get(path)
    if f is None:                                    # ch1 구역에 없는 채널 키 — 채널 모듈 복사값(.py)
        sec, rest = path.split(".", 1)
        f = FACT.get("ch2." + rest) or FACT.get("common." + rest)
        return f[2]
    eff = conv._effect(path)
    if eff == "dead_channel":                        # 채널 값은 읽히지 않음 — common 값
        sec, name, sub = own.split_path(path)
        return v1_effective(f"common.{name}" + (f".{sub}" if sub else ""))
    if path.startswith("(code).") or eff == "dead_common" or f[1] is None:
        return f[2]
    return f[1]


def _default_of(p, want=None):
    """v2 기본값(목록·묶음 값을 JSON 글자로 담는 STR 키는 풀어서)."""
    if isinstance(p.default, str) and (p.pattern == r"\[.*\]" or isinstance(want, (list, tuple, dict))):
        return json.loads(p.default)
    return p.default


# ───────────── inv_cfg 항목 → v1 경로 ─────────────
_PATH_RX = re.compile(r"(?:설정 ((?:common|ch1|ch2)\.\S+)|코드 상수 config_common\.(\S+))")


def _item_path(it: dict):
    m = _PATH_RX.match(it["desc"])
    if not m:
        return None
    return m.group(1) or f"{own.CODE}.{m.group(2)}"


KEY_ITEMS = {p: it for it in inv_cfg.ITEMS for p in [_item_path(it)] if p}
KEY_PATHS = sorted(KEY_ITEMS)


def _name(path: str) -> str:
    sec, name, sub = own.split_path(path)
    return name if sub is None else f"{name}_{sub.upper()}"


def _base(key: str) -> str:
    return key[4:] if key.startswith(("CH1_", "CH2_")) else key


_CHAT = "vanam-process/lib/ops2/ch12/chatCards.ts"
_EXPORT = "v2/tools/export_worker_settings.py"

# 이름을 동적으로 만들거나 v2 코드 밖(포털·독립 도구 내보내기)에서 쓰는 키 — (파일, 그 파일에 있어야 하는 글자)
USE = {
    "common.CHAT_ERR_DEDUP_S": [(_CHAT, "v.CHAT_ERR_DEDUP_S")],             # 포털이 카드 중복 억제(v1 chat_notifier)
    "common.CHAT_PLC_LINK_SUPPRESS": [(_CHAT, "v.CHAT_PLC_LINK_SUPPRESS")],
    "common.CHAT_PLC_LINK_CODES": [(_CHAT, "v.CHAT_PLC_LINK_CODES")],
    # 독립 도구(log_sync·rf_worker)가 v1 settings.json 으로 읽는 키 — v2 값에서 내보낸다(D13)
    **{f"common.{k}": [(_EXPORT, f'"{k}"')] for k in (
        "LOG_SYNC_ENABLED", "LOG_SYNC_PAIRS", "LOG_SYNC_STATE_DIR", "LOG_SYNC_ALERT_AFTER_FAILS", "CAMERA_LOG_ROOT",
        "RF_READER_STATE_DIR", "RF_READER_CSV_DIR", "RF_READER_STALE_MIN", "RF_READER_NAS_DIR", "RF_READER_RETENTION_DAYS",
        "RF_READER_MIN_FREE_GB", "RF_READER_UPLOAD_ALERT_DAYS")},
}

# v2 기본값이 v1 값과 글자로 다른 키(뜻은 같다) — 경로 → (v2 기본값, 이유). 같은 뜻인지는 FIXED 가 실제로 확인한다.
DEFAULT = {
    "ch1.THICKNESS_FACTOR_FILE": ("", "빈 값 = 설정 폴더의 thickness_factors_ch1.json(v1 config/ 의 같은 이름 — D12)"),
    "ch2.THICKNESS_FACTOR_FILE": ("", "빈 값 = 설정 폴더의 thickness_factors_ch2.json(v1 config/ 의 같은 이름 — D12)"),
    "(code).LOCAL_FALLBACK_ROOT": ("", "빈 값 = exe(소스 실행이면 저장소) 옆 Logs_LocalFallback — v1 과 같은 자리"),
    "(code).PLC_COIL_LOG_LOCAL_DIR": ("", "빈 값 = LOCAL_FALLBACK_ROOT\\PLC_Coil — v1 과 같은 자리"),
    "(code).RGA_NET": ("", "장비 계정은 비밀값(장비 로컬 파일) — 코드에 두지 않음(D23)"),
}


def _fb():
    from v2.ch12.outputs import paths as OP
    return OP, OP.Paths({}), OP.exe_base_dir() / "Logs_LocalFallback"


def _fixed_fallback_root():
    OP, P_, fb = _fb()
    assert P_.fallback_root() == fb


def _fixed_fallback_sub(sub: str, fn):
    def check():
        OP, P_, fb = _fb()
        assert fn(P_) == fb / sub
    return check


def _fixed_server_dir():
    from v2.robot_server import log as RL
    from v2.ch12.outputs import paths as OP
    assert RL.default_fallback_dir() == OP.exe_base_dir() / "Logs_LocalFallback" / "Server"


def _fixed_rga_dir():
    src = (ROOT / "v2/ch12/drivers/rga_link.py").read_text(encoding="utf-8")
    assert '"LOCAL_FALLBACK_RGA_DIR", str(wp.REPO_ROOT / "Logs_LocalFallback" / "RGA")' in src


def _fixed_rga_net():
    from v2.common.paramdef import Scope
    assert T.by_key()["RGA_NET"].scope is Scope.SECRET


def _fixed_thickness_factor_file(ch: int):
    def check():
        from v2.ch12.thickness_factors import factor_file
        d = Path("/설정")
        assert factor_file({f"CH{ch}_THICKNESS_FACTOR_FILE": ""}, f"ch{ch}", d) == d / f"thickness_factors_ch{ch}.json"
    return check


def _fixed_gas_info():
    from v2.ch12 import process_steps as PS
    assert PS.GAS_INFO_DEFAULT == FACT["(code).PROCESS_GAS_INFO"][2] == {"AR": {"channel": 1}, "O2": {"channel": 2},
                                                                          "N2": {"channel": 3}}


def _fixed_rga_xlsx():
    by = T.by_key()
    v1 = FACT["(code).RGA_XLSX_PATH"][2]
    assert by["CH1_RGA_CSV_PATH"].default == v1["ch1"] and by["CH2_RGA_CSV_PATH"].default == v1["ch2"]


def _fixed_eol(path: str, snippet: str):
    def check():
        assert snippet in (ROOT / path).read_text(encoding="utf-8")
    return check


def _fixed_mfc_commands():
    from v2.ch12.drivers import mfc as M
    # v1 lib/config_common.py:300-330 의 명령 글자(대표값) — 바이트 전체는 test_mfc 골든
    assert M.mk_cmd("SET_ONOFF_MASK", "101") == "L0101" and M.mk_cmd("FLOW_ON", 2) == "L21"
    assert M.mk_cmd("FLOW_OFF", 3) == "L30" and M.mk_cmd("MFC_ZEROING", 1) == "L51"
    assert M.mk_cmd("FLOW_SET", 1, "10.0") == "Q1 10.0" and M.mk_cmd("READ_FLOW_SET", 2) == "R66"
    assert [M.mk_cmd(k) for k in ("READ_FLOW_ALL", "READ_MFC_ON_OFF_STATUS", "READ_PRESSURE", "READ_VALVE_POSITION",
                                  "READ_SYSTEM_STATUS", "VALVE_OPEN", "VALVE_CLOSE", "PS_ZEROING", "SP4_ON")] == \
        ["R60", "R69", "R5", "R6", "R7", "O", "C", "Z1", "D4"]
    assert M.mk_cmd("SP4_SET", "2.5") == "S4 2.5"
    assert sorted(M.MFC_COMMANDS) == sorted(               # v1 표의 이름 27개(lib/config_common.py:300-330)
        "FLOW_OFF FLOW_ON FLOW_SET MFC_ZEROING PS_ZEROING READ_FLOW READ_FLOW_ALL READ_FLOW_SET READ_MFC_ON_OFF_STATUS "
        "READ_PRESSURE READ_SP1_VALUE READ_SP2_VALUE READ_SP3_VALUE READ_SP4_VALUE READ_SYSTEM_STATUS READ_VALVE_POSITION "
        "SET_ONOFF_MASK SP1_ON SP1_SET SP2_ON SP2_SET SP3_ON SP3_SET SP4_ON SP4_SET VALVE_CLOSE VALVE_OPEN".split())


def _fixed_keepalive_on():
    """D39 — 공용 TcpTransport 는 SO_KEEPALIVE 를 늘 켠다(v1 MFC·IG false)."""
    assert "socket.SO_KEEPALIVE, 1" in (ROOT / "v2/common/transport.py").read_text(encoding="utf-8")


def _fixed_zero(path: str):
    """v1 실효값이 0(그 기능 꺼짐) — v2 는 그 기능을 두지 않아 같은 동작."""
    def check():
        assert FACT[path][2] == 0 and (FACT[path][1] in (0, None))
    return check


FIXED = {
    "(code).LOCAL_FALLBACK_ROOT": _fixed_fallback_root,
    "ch1.THICKNESS_FACTOR_FILE": _fixed_thickness_factor_file(1),
    "ch2.THICKNESS_FACTOR_FILE": _fixed_thickness_factor_file(2),
    "(code).PLC_COIL_LOG_LOCAL_DIR": _fixed_fallback_sub("PLC_Coil", lambda p: p.plc_coil_dirs()[1]),
    "(code).LOCAL_FALLBACK_CH1_DIR": _fixed_fallback_sub("CH1", lambda p: p.fallback_ch(1)),
    "(code).LOCAL_FALLBACK_CH2_DIR": _fixed_fallback_sub("CH2", lambda p: p.fallback_ch(2)),
    "(code).LOCAL_FALLBACK_SYSTEM_DIR": _fixed_fallback_sub("SYSTEM", lambda p: p.system_dirs()[1]),
    "(code).LOCAL_FALLBACK_SERVER_DIR": _fixed_server_dir,
    "(code).LOCAL_FALLBACK_RGA_DIR": _fixed_rga_dir,
    "(code).RGA_NET": _fixed_rga_net,
    "(code).PROCESS_GAS_INFO": _fixed_gas_info,
    "(code).RGA_XLSX_PATH": _fixed_rga_xlsx,
    "(code).IG_TX_EOL": _fixed_eol("v2/ch12/drivers/ig.py", 'session.write((cmd + "\\r").encode("ascii", "ignore"))'),
    "(code).MFC_TX_EOL": _fixed_eol("v2/ch12/drivers/mfc.py", '"\\r"'),
    "(code).MFC_COMMANDS": _fixed_mfc_commands,
    "common.IG_TCP_KEEPALIVE": _fixed_keepalive_on,
    "common.MFC_TCP_KEEPALIVE": _fixed_keepalive_on,
    "common.IG_INACTIVITY_REOPEN_S": _fixed_zero("common.IG_INACTIVITY_REOPEN_S"),
    "common.MFC_INACTIVITY_REOPEN_S": _fixed_zero("common.MFC_INACTIVITY_REOPEN_S"),
    "common.DCP_INACTIVITY_REOPEN_S": _fixed_zero("common.DCP_INACTIVITY_REOPEN_S"),
    "common.DCP_WRITE_WORKER_RETRIES": _fixed_zero("common.DCP_WRITE_WORKER_RETRIES"),
}

# v1 에서 읽기는 하지만 그 값이 쓰이는 길이 없는 키(죽은 코드) — 경로 → 근거
V1_DEAD = {
    "common.DCP_CMD_MAX_RETRIES": "device/dc_pulse.py:476 에서 읽지만 그 값을 쓰는 _write_simple 이 어디서도 불리지 않음",
}

# v2 키는 있지만 v2 코드가 아직/일부러 쓰지 않는 키 — 경로 → 이유(inv_cfg 항목 note 와 같은 뜻, 보고 대상)
V2_UNUSED = {
    "common.DEBUG_PRINT": "v1 개발용 콘솔 디버그 출력 스위치(운영값 false — 출력 없음). v2 드라이버는 디버그 출력이 없다(false 와 같음)",
    "common.OES_PLOT_MIN_INTERVAL_S": "화면 OES 그래프 쓰로틀 — 그래프는 포털 몫(UI-098·PROC-232), 아직 포털에 OES 그래프 없음",
    "common.CHAT_POST_CONCURRENCY": "v1 chat_notifier 전송 폭주 보호 — v2 는 포털이 카드를 보낸다(CHAT-005 담당)",
    "common.CHAT_PENDING_MAX": "v1 chat_notifier 대기 상한 — v2 는 포털이 카드를 보낸다(CHAT-005 담당)",
    "common.CHAT_PENDING_HARD_MAX": "v1 chat_notifier urgent 상한 — v2 는 포털이 카드를 보낸다(CHAT-005 담당)",
    "common.PROCESS_LIST_START_DIR": "레시피 파일 열기 대화상자 시작 폴더 — v2 는 포털 레시피 적재로 대체(UI-080·PC-087)",
    "common.CHAMBER_PULSE_RECONNECT_TIMEOUT_S": "v1 시작 전 펄스 set_endpoint_reconnect 한도 — v2 펄스 포트는 점유(claim) 뒤 worker 연결 "
                                                "대기(CHAMBER_PREFLIGHT_TIMEOUT_WITH_PULSE_S)로 대신(PROC-095 담당)",
    "common.PC_PREFLIGHT_DEVICE_CONNECT_TIMEOUT_S": "v1 장비 connect() 호출 한도 — v2 worker 는 늘 연결을 유지해 connect 는 호출 로그만"
                                                    "(D37, v2/ch12/pc_preflight.py) — 이 한도를 걸 호출이 없다",
}


def _v1_unread(path: str) -> bool:
    """v1 사실표·전수 목록이 '읽는 곳 없음'(전역 grep)이라 한 키, 또는 V1_DEAD."""
    f = FACT.get(path)
    return (conv._effect(path) == "unread" or path in V1_DEAD or (f is not None and "읽는 곳 없음" in f[5])
            or "읽는 곳 없음" in KEY_ITEMS[path]["note"])


def _changed(v):
    if isinstance(v, bool):
        return not v
    if isinstance(v, (int, float)):
        return v + 1
    if isinstance(v, str):
        return v + "_x"
    return None


def _convert_one(path: str, v):
    sec, name, sub = own.split_path(path)
    res = conv.Result()
    data = {sec: {name: v if sub is None else {sub: v}}}
    CONV._entry(res, path, v, data)
    return res


@pytest.mark.parametrize("path", KEY_PATHS)
def test_key(path):
    sec, name, sub = own.split_path(path)
    p, d = CONV.param_of.get(path), CONV.dropped_of.get(path)
    eff = conv._effect(path)
    want = v1_effective(path) if (path in FACT or path.startswith(("ch1.", "ch2."))) else None
    assert p is not None or d is not None, f"{path}: v2 키도 버림 근거도 없다"
    if p is not None:
        # 1) 이름 규칙(또는 v1_key)
        assert p.key == own.v2_key_for(path) or path in own.v1_refs(p.key, p.v1_key) or \
            own.v2_key_for(path) not in T.by_key(), (path, p.key)
        # 2) 기본값 = v1 실효값
        if path in DEFAULT:
            assert p.default == DEFAULT[path][0], (path, p.default)
        else:
            assert conv._same(_default_of(p, want), want), (path, p.default, want)
    else:
        assert d.reason.strip(), path
    if path in FIXED:
        FIXED[path]()
    # 3) 쓰는 곳
    if _v1_unread(path):                               # v1 에서도 쓰지 않던 키 — 변환 도구는 받아 버리거나 키만 둔다
        return
    if path in V2_UNUSED:
        return
    if p is not None:
        files = uses(_base(p.key))
        bad = _where_ok(USE.get(path, []))
        assert (files or path in USE) and not bad, (path, p.key, "v2 코드에서 쓰는 곳 없음", bad)
        return
    # 버린 키: 대신하는 키가 쓰이거나(FIXED·replaced_by), v1 값 그대로면 버리고 다르면(v1 에서 쓰이던 키) 멈춘다
    for k in ([] if path in FIXED else
              [x for x in (d.replaced_by or "").replace("·", " ").split() if re.fullmatch(r"[A-Z0-9_]+", x)]):
        assert k in T.by_key(), (path, k)
        assert uses(_base(k)) or _where_ok(USE.get(path, [])) == [], (path, k)
    if path.startswith(own.CODE) or not isinstance(want, (bool, int, float, str)):
        assert path in FIXED or d.replaced_by or uses(name), (path, "대신하는 v2 동작 확인 없음")
        return
    res = _convert_one(path, want)
    assert not res.errors and path in {x["path"] for x in res.dropped}, (path, res.errors)
    if eff == "used" and _changed(want) is not None:
        res2 = _convert_one(path, _changed(want))
        assert any("옮길 곳이 없다" in e for e in res2.errors), (path, res2.errors)


# ───────────── 독립 도구용 settings.json 내보내기(D13 — CFG-546·547 등) ─────────────
def test_export_worker_settings_v1_format(tmp_path):
    from v2.common.config_store import ConfigStore
    from v2.common.paramdef import defaults
    from v2.tools import export_worker_settings as W

    assert W.WORKER_KEYS["log_sync"] == ("LOG_SYNC_ENABLED", "LOG_SYNC_PAIRS", "LOG_SYNC_STATE_DIR",
                                         "LOG_SYNC_ALERT_AFTER_FAILS")                       # log_sync.py:55
    assert W.WORKER_KEYS["rf_worker"] == ("CAMERA_LOG_ROOT", "RF_READER_STATE_DIR", "RF_READER_CSV_DIR",
                                          "RF_READER_STALE_MIN", "RF_READER_NAS_DIR", "RF_READER_RETENTION_DAYS",
                                          "RF_READER_MIN_FREE_GB", "RF_READER_UPLOAD_ALERT_DAYS")  # rf_worker.py:203
    obj = W.build(defaults(T.params))
    assert list(obj) == ["common"] and list(obj["common"]) == list(W.ALL_KEYS)
    for k, v in obj["common"].items():                       # 기본값 = v1 운영값(json), 형식도 v1 과 같다
        assert conv._same(v, FACT[f"common.{k}"][1]), k
        assert conv._v1_compatible(conv._kind_of(FACT[f"common.{k}"][3]), v), k
    out = tmp_path / "cfgw" / "config" / "settings.json"
    assert W.main(["--out", str(out)]) == 0
    back = conv.read_json_v1(out)                            # v1 과 같은 읽기(BOM·JSONC·중복)로 다시 읽힌다
    res = conv.Result()
    CONV.convert_settings(back, res)
    assert res.ok and all(conv._same(conv.json.loads(res.values[k]) if k == "LOG_SYNC_PAIRS" else res.values[k],
                                     obj["common"][k]) for k in W.ALL_KEYS), res.errors
    assert W.main(["--out", str(out)]) == 1                  # 이미 있으면 --force 없이는 멈춤
    # v2 설정 폴더 값을 내보낸다
    cdir = tmp_path / "v2cfg"
    st = ConfigStore(cdir, T.params, T.rules, T.schema_hash())
    st.load()
    vals = dict(st.stored())
    vals["LOG_SYNC_ALERT_AFTER_FAILS"] = 9
    vals["LOG_SYNC_PAIRS"] = json.dumps([["C:\\a", "\\\\nas\\a"]])
    assert st.apply(1, vals, T.schema_hash(), busy=False).status != "rejected"
    assert W.main(["--config-dir", str(cdir), "--out", str(out), "--force"]) == 0
    got = json.loads(out.read_text(encoding="utf-8"))["common"]
    assert got["LOG_SYNC_ALERT_AFTER_FAILS"] == 9 and got["LOG_SYNC_PAIRS"] == [["C:\\a", "\\\\nas\\a"]]
    with pytest.raises(ValueError, match="v1 형식"):
        W.worker_values(dict(defaults(T.params), RF_READER_STALE_MIN=True))   # bool 은 숫자 자리에 못 간다(워커가 멈춤)


# ───────────── v1 로더 규칙(변환 도구 — CFG-531~545) ─────────────
def _write(tmp_path, raw: bytes | str) -> Path:
    p = tmp_path / "config" / "settings.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(raw, str):
        p.write_text(raw, encoding="utf-8")
    else:
        p.write_bytes(raw)
    return p


def test_loader_messages_same_as_v1(tmp_path):
    with pytest.raises(ValueError, match="설정 파일이 없습니다: .* — 프로그램 폴더의 config 폴더에 settings.json 을 넣으세요"):
        conv.read_json_v1(tmp_path / "없음" / "settings.json")
    with pytest.raises(ValueError, match="UTF-8 로 저장된 파일이 아닙니다 — 메모장에서 '다른 이름으로 저장'"):
        conv.read_json_v1(_write(tmp_path, '{"common": {"CH_NAME": "가"}}'.encode("cp949")))
    with pytest.raises(ValueError) as ei:
        conv.read_json_v1(_write(tmp_path, '{\n  "common": {\n    "A": 1\n    "B": 2\n  }\n}\n'))
    msg = str(ei.value)
    assert "줄 4 칸 5: 쉼표(,)가 빠졌습니다 — 바로 앞 항목(보통 앞 줄) 끝에 쉼표를 넣으세요" in msg
    assert msg.endswith('\n      4|     "B": 2')                                  # 원본 줄(v1 _json_problem)
    with pytest.raises(ValueError, match="Extra data|파일 끝 } 뒤에 글자가 더 있습니다"):
        conv.read_json_v1(_write(tmp_path, '{"common": {}} x'))
    res = conv.Result()
    CONV.convert_settings({"comon": {}, "ch1": {"MFC_TCP_PRT": 1}}, res)
    assert any('"comon": 알 수 없는 구역' in e and "(혹시 common ?)" in e for e in res.errors)
    assert any("ch1.MFC_TCP_PRT: 모르는 키" in e and "(혹시 MFC_TCP_PORT ?)" in e for e in res.errors)


def test_nested_dup_dict_int_keys_and_all_or_nothing(tmp_path):
    raw = '{"ch1": {"MFC_SCALE_FACTORS": {"1": 1, "1": 2}}}'
    res = conv.convert(_write(tmp_path / "a", raw), tmp_path / "a")
    assert any("ch1.MFC_SCALE_FACTORS 안의 \"1\": 같은 이름이 두 번" in e for e in res.errors)
    res = conv.convert(_write(tmp_path / "b", '{"ch1": {"MFC_SCALE_FACTORS": {"1": 1, "2": 10, "3": 1}}}'), tmp_path / "b")
    assert res.ok and [res.values[f"CH1_MFC_SCALE_FACTORS_{i}"] for i in (1, 2, 3)] == [1.0, 10.0, 1.0]
    # 전부 아니면 전무 — 하나라도 오류면 아무 파일도 쓰지 않는다
    bad = conv.convert(_write(tmp_path / "c", '{"common": {"SHUTDOWN_STEP_GAP_MS": 500, "NOPE_KEY": 1}}'), tmp_path / "c")
    out = tmp_path / "out"
    conv.write(bad, out)
    assert not bad.ok and not (out / "config_active.json").exists() and not (out / conv.REPORT).exists()


def test_store_reload_is_idempotent(tmp_path):
    """v1 은 시작 때와 MainWindow 에서 같은 파일을 두 번 적용했다(main.py:150-158) — v2 는 한 번 읽고 다시 읽어도 같다."""
    from v2.common.config_store import ConfigStore
    st = ConfigStore(tmp_path, T.params, T.rules, T.schema_hash())
    a = st.load()
    assert st.load() == a


# ───────────── 채널 값·어댑터(CFG-554~556) ─────────────
def test_channel_lookup_like_v1_adapter():
    from v2.ch12.drivers.pulse_port import chan_get
    from v2.ch12.outputs.paths import Paths, V1_RGA_DEFAULT
    cfg = {"CH1_X": 1, "X": 2}
    assert chan_get(cfg, "ch1", "X") == 1 and chan_get(cfg, "ch2", "X") == 2 and chan_get({}, "ch1", "X", 7) == 7
    p = Paths({"CH1_RGA_CSV_PATH": r"D:\a.csv"})
    assert p.rga_csv(1)[0] == r"D:\a.csv" and p.rga_csv(2)[0] == V1_RGA_DEFAULT.format(n=2)


def test_channel_fixed_values():
    by = T.by_key()
    want = {"CH1_IG_TCP_PORT": 4001, "CH1_MFC_TCP_PORT": 4003, "CH1_RFPULSE_TCP_PORT": 4007,
            "CH2_IG_TCP_PORT": 4002, "CH2_MFC_TCP_PORT": 4006, "CH2_DCPULSE_TCP_PORT": 4005,
            "CH2_RFPULSE_TCP_PORT": FACT["ch2.RFPULSE_TCP_PORT"][1],
            "CH1_RGA_CSV_PATH": r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.1\RGA_spectrums.csv",
            "CH2_RGA_CSV_PATH": r"G:\공유 드라이브\VanaM_Sputter\RGA\Ch.2\RGA_spectrums.csv",
            "CH2_MFC_SCALE_FACTORS_2": 10.0}
    for k, v in want.items():
        assert by[k].default == v, (k, by[k].default)


def test_rga_step_timeout_formula():
    from v2.ch12.drivers.rga_link import step_timeout_ms
    from v2.common.paramdef import defaults
    d = defaults(T.params)
    n, t, dl = max(1, d["RGA_MAX_ATTEMPTS"]), d["RGA_WORKER_TIMEOUT_S"], max(0.0, d["RGA_RETRY_DELAY_S"])
    assert step_timeout_ms(d) == int(n * t * 1000 + (n - 1) * dl * 1000 + 5000)
    assert step_timeout_ms({"RGA_MAX_ATTEMPTS": 0, "RGA_WORKER_TIMEOUT_S": 10, "RGA_RETRY_DELAY_S": -5}) == 15000
