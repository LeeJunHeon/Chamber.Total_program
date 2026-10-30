"""v1 기록 출력 골든 비교 도우미(v2/tests/test_outputs_*.py 가 쓴다 — v1 을 import 하지 않는다).

골든: v2/tests/golden/outputs/<이름>/scenario.json + files[_단계]/… (만든 법: v2/tools/v1golden_outputs.py — v1 실제 실행).
- 경로 '@/…' 는 시험마다 새 임시 뿌리(한글 '기록 시험') 기준.
- 바이트 비교. xlsx 는 zip 안 파일마다 바이트 비교(docProps/core.xml 의 만든·고친 시각만 맞춤 — openpyxl 이 저장 시각을 씀).
"""
from __future__ import annotations

import json
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional

GOLDEN = Path(__file__).resolve().parent / "golden" / "outputs"


def load(name: str) -> dict:
    return json.loads((GOLDEN / name / "scenario.json").read_text(encoding="utf-8"))


def make_root(tmp_path: Path) -> Path:
    r = Path(tmp_path) / "기록 시험"
    r.mkdir(parents=True, exist_ok=True)
    return r


def resolve(root: Path, v):
    if isinstance(v, str) and v.startswith("@/"):
        return str(root / v[2:])
    return v


def v2_cfg(root: Path, *cfgs: dict) -> dict:
    """scenario cfg → v2 cfg(채널 구역 키 THICKNESS_LOG_DIR → CH1_/CH2_ 접두, 로컬 정본은 임시 뿌리 아래)."""
    out = {"OUTPUT_LOCAL_FIRST_DIR": str(root / "로컬정본")}
    for c in cfgs:
        for k, v in (c or {}).items():
            v = resolve(root, v)
            if k == "THICKNESS_LOG_DIR":
                out["CH1_THICKNESS_LOG_DIR"] = v
                out["CH2_THICKNESS_LOG_DIR"] = v
            elif k == "LOCAL_FALLBACK_SYSTEM_DIR":
                continue
            else:
                out[k] = v
    return out


class Clock:
    """고정 시계 — now()·monotonic()·wall()."""

    def __init__(self, iso: str) -> None:
        self.t = datetime.fromisoformat(iso)
        self.mono = 1000.0

    def now(self) -> datetime:
        return self.t

    def set(self, iso: str) -> None:
        self.t = datetime.fromisoformat(iso)

    def advance(self, s: float) -> None:
        self.t = self.t + timedelta(seconds=s)
        self.mono += s

    def monotonic(self) -> float:
        return self.mono

    def time(self) -> float:
        return self.t.timestamp()


def write_pre(root: Path, pre: dict) -> None:
    for rel, text in (pre or {}).items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        if text is None:
            p.mkdir(parents=True, exist_ok=True)
        elif isinstance(text, dict):
            p.write_bytes(text["text"].encode("utf-8"))
        else:
            p.write_bytes(text.encode("utf-8"))


def copy_tree(src: Path, dst: Path) -> None:
    for p in src.rglob("*"):
        if p.is_file():
            t = dst / p.relative_to(src)
            t.parent.mkdir(parents=True, exist_ok=True)
            t.write_bytes(p.read_bytes())


_CORE_TIME = re.compile(rb"(<dcterms:(?:created|modified)[^>]*>)[^<]*(</dcterms:)")


def xlsx_parts(path: Path) -> dict:
    with zipfile.ZipFile(path) as z:
        out = {}
        for n in z.namelist():
            b = z.read(n)
            if n == "docProps/core.xml":
                b = _CORE_TIME.sub(rb"\1T\2", b)
            out[n] = b
        return out


def files_under(root: Path, keep: Callable[[str], bool] = lambda rel: True) -> dict:
    out = {}
    for p in sorted(Path(root).rglob("*")):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            if keep(rel):
                out[rel] = p
    return out


def assert_same_tree(root: Path, golden_dir: Path, keep: Callable[[str], bool] = lambda rel: True,
                     normalize: Optional[Callable[[str, bytes], bytes]] = None) -> None:
    got = files_under(root, keep)
    want = files_under(golden_dir, keep)
    assert sorted(got) == sorted(want), f"파일 목록 다름\n v2: {sorted(got)}\n v1: {sorted(want)}"
    for rel, wp in want.items():
        gp = got[rel]
        if rel.endswith(".xlsx") and zipfile.is_zipfile(wp):
            a, b = xlsx_parts(gp), xlsx_parts(wp)
            assert sorted(a) == sorted(b), f"{rel}: xlsx 안 파일 목록 다름"
            for n in b:
                assert a[n] == b[n], f"{rel}!{n} 바이트 다름\n v2: {a[n][:600]!r}\n v1: {b[n][:600]!r}"
            continue
        gb, wb = gp.read_bytes(), wp.read_bytes()
        if normalize is not None:
            gb, wb = normalize(rel, gb), normalize(rel, wb)
        assert gb == wb, f"{rel} 바이트 다름\n v2: {gb[:800]!r}\n v1: {wb[:800]!r}"


def norm_logs(root: Path, lines: Iterable[str]) -> list:
    return [str(x).replace(str(root), "@") for x in lines]
