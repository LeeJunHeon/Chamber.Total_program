"""전수 목록 완료 검사(사용자 지시 0단계) — 모든 항목이 done 또는 na 일 때만 통과. 마지막에 반드시 통과해야 한다.

done 항목: v2 칸의 파일이 실제로 있고(여러 개면 ; 로 — 하나 이상), test 칸의 시험이 실제로 있다
  ("v2/tests/test_x.py::test_y" — 파일이 있고 그 이름의 시험 함수(또는 클래스 안 메서드)가 있다; 여러 개면 ; 로 — 모두).
  포털 쪽 시험은 "vanam-process/scripts/ops2-golden.mjs::<그 파일 안의 이름>" 처럼(파일이 있고 이름이 파일 안에 있음).
na 항목: note 에 이유가 있다.
포털 쪽 담당(v2 칸이 "vanam-process ..." 로 시작)은 vanam-process 저장소 경로가 있는지(같은 상위 폴더에 있으면) 본다.
"""
import ast
from pathlib import Path

from v2.ch12.spec import inventory as INV

ROOT = Path(__file__).resolve().parents[2]
PORTAL = ROOT.parent / "vanam-process"


def _test_names(path: Path) -> set:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name.startswith("test"):
            out.add(n.name)
    return out


def _v2_ok(v2: str) -> bool:
    for part in [p.strip() for p in v2.split(";") if p.strip()]:
        if part.startswith("vanam-process"):
            rel = part.split(None, 1)[1].split()[0] if " " in part else ""
            if not PORTAL.exists() or (rel and (PORTAL / rel).exists()):
                return True
            continue
        path = part.split()[0].split("::")[0]
        if (ROOT / path).exists():
            return True
    return False


def _test_ok(test: str, cache: dict) -> bool:
    parts = [p.strip() for p in test.split(";") if p.strip()]
    if not parts:
        return False
    for t in parts:
        f, _, name = t.partition("::")
        if f.startswith("vanam-process/"):          # 포털 쪽 시험(스크립트) — 파일이 있고 이름이 그 안에 있음
            if not PORTAL.exists():
                continue
            p = PORTAL / f[len("vanam-process/"):]
            if not p.is_file() or (name and name not in p.read_text(encoding="utf-8", errors="replace")):
                return False
            continue
        p = ROOT / f
        if not p.is_file():
            return False
        if name:
            names = cache.setdefault(f, _test_names(p))
            if name.split("[")[0] not in names:
                return False
    return True


def problems():
    bad, cache = [], {}
    seen = set()
    for it in INV.all_items():
        if it["id"] in seen:
            bad.append(f"{it['id']}: id 중복")
        seen.add(it["id"])
        st = it["status"]
        if st == "na":
            if not it["note"].strip():
                bad.append(f"{it['id']}: na 인데 이유 없음")
        elif st == "done":
            if not _v2_ok(it["v2"]):
                bad.append(f"{it['id']}: v2 담당 파일 없음 {it['v2']!r}")
            if not _test_ok(it["test"], cache):
                bad.append(f"{it['id']}: 시험 없음 {it['test']!r}")
        else:
            bad.append(f"{it['id']}: {st}")
    return bad


def test_inventory_complete():
    bad = problems()
    s = INV.summary()
    assert not bad, f"전수 목록 미완료 {len(bad)}건 (요약 {s}) — 처음 30건:\n" + "\n".join(bad[:30])
