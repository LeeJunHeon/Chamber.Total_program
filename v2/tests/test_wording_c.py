"""C — 콘솔·화면 글의 '끔·켬·안 함·못 함'(사용자 지시 2026-11-27, 챔버K W-25 와 같은 말).

콘솔 '웹 연결 끔(sim)' → '웹 연결 사용하지 않음(sim)'. 웹·노트북 화면이 받는 묶음(설정 이름·설명·메모, 키 사이 규칙, 명령 설명, 블록 글,
레시피 열 설명, 안전 규칙 글, 뺀 설정 사유)과 콘솔·사건 글에 이 네 말을 쓰지 않는다('끄기'·'사용하지 않음'·'꺼짐'·'하지 않음' 등으로).
주석·독스트링과 공정 txt 의 v1 문구는 그대로. 그대로 둔 글은 KEEP 에 이유와 함께.
"""
import ast
import json
import re
from pathlib import Path

from v2.ch12 import web_schema as W

BAD = re.compile(r"안 ?함(?![가-힣])|못 함(?![가-힣])|(?<![가-힣])(켬|끔)(?![가-힣])|[가-힣](켬|끔)(?![가-힣])")
ROOT = Path(__file__).resolve().parents[1]

# 그대로 두는 글 — (파일, 글 조각): 이유
KEEP = {
    ("v2/ch12/drivers/line_ops.py", "초 안에 전송 확인 못 함"): "v1 device/mfc.py MFC 명령 실패 사유 — 공정 txt·v1 대조 시험(test_mfc)",
    ("v2/ch12/drivers/mfc_ops.py", "가 같은 채널을 다시 켬"): "v1 device/mfc.py 안정화 중단 사유 — 공정 txt·v1 대조 시험(test_inv_dev)",
    ("v2/ch12/heater_edges.py", "PLC 가 RUN 을 끔"): "v1 heater_service 운전 종료 사유 — 히터 운전 기록·v1 대조 시험(test_heater_sim)",
    ("v2/ch12/heater_process.py", "PLC 가 히터를 끔"): "v1 heater_process 공정 실패 사유(E830) — 공정 txt·v1 대조 시험",
    ("v2/ch12/keep_awake.py", "'아무것도 안 함'"): "윈도우 전원 설정의 옵션 이름 그대로(챔버K 와 같은 글)",
    ("v2/ch12/thickness_factors.py", "(선택 안 함)"): "v1 두께 창 목록 첫 줄 — 웹 문구 검사도 같은 예외",
}


def _strings(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docs = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.body:
            b = n.body[0]
            if isinstance(b, ast.Expr) and isinstance(getattr(b, "value", None), ast.Constant):
                docs.add(id(b.value))
    for n in ast.walk(tree):
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docs:
            yield n.lineno, n.value


def test_bundle_has_none_of_the_four_words():
    hits = []

    def walk(x, at):
        if isinstance(x, dict):
            for k, v in x.items():
                walk(v, f"{at}/{k}")
        elif isinstance(x, list):
            for i, v in enumerate(x):
                walk(v, f"{at}/{i}")
        elif isinstance(x, str) and BAD.search(x):
            hits.append((at, x[:120]))
    walk(json.loads(W.to_json(W.bundle())), "")
    assert hits == [], hits[:10]


def test_console_and_event_texts_have_none_except_kept():
    hits, used = [], set()
    for p in sorted((ROOT / "ch12").rglob("*.py")):
        rel = p.relative_to(ROOT.parent).as_posix()
        if "/spec/" in rel:
            continue
        for line, s in _strings(p):
            if not BAD.search(s):
                continue
            keep = [k for k in KEEP if k[0] == rel and k[1] in s]
            if keep:
                used.update(keep)
            else:
                hits.append(f"{rel}:{line} {s[:100]!r}")
    assert hits == [], hits[:10]
    assert used == set(KEEP)                                     # 그대로 둔 글이 실제로 남아 있다(지운 뒤 목록만 남지 않게)


def test_console_lines(tmp_path):
    from v2.ch12 import app as A
    from v2.tests.test_app_web import FakeWeb, _in_process
    app, lines = _in_process(tmp_path, FakeWeb(), UPLINK_IN_SIM=False, EQUIPMENT_ID="CH12")
    try:
        assert app.start() == A.EXIT_OK
        assert "웹 연결 사용하지 않음(sim)" in lines
        assert not [ln for ln in lines if BAD.search(ln)], [ln for ln in lines if BAD.search(ln)]
    finally:
        app.shutdown()
