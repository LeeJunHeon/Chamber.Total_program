"""꾸러미 — 장비 노트북에 옮길 v2 만 담은 zip(2026-11-23 D429).

    python -m v2.tools.pack [--out 폴더] [--ref HEAD]

- 담는 것: 그 커밋(--ref, 기본 HEAD)에서 git 이 추적하는 v2/ 파일 — 작업 폴더가 아니라 커밋 내용 그대로(git archive).
  빼는 것: 시험(v2/tests — 골든 포함), v1 골든을 만드는 도구(v2/tools/v1golden_*·robot_golden_cases·make_web_golden_*).
- 맨 위에 VERSION: 첫 줄 '브랜치@커밋7'(앱이 .git 없을 때 이것을 버전으로 보인다 — app.git_version), 둘째 줄 전체 커밋, 셋째 줄 만든 시각.
- zip 안 모든 경로는 윈도우에서 만들 수 있는 이름(v2/common/winnames — 어기면 만들지 않음).
- 이름 ch12_v2_<브랜치(/→-)>_<커밋7>.zip, 기본 폴더 = 저장소의 상위 폴더(저장소 안에 만들지 않는다 — D427).
- 커밋하지 않은 v2 변경은 담지 않는다(있으면 알림만). git 이 없는 곳(꾸러미를 푼 폴더)에서는 만들지 않는다.
장비 노트북: zip 을 새 폴더(예 C:\\CH12_v2\\app_<커밋7>)에 풀고 그 폴더의 v2\\run_ch12_sim.cmd 로 띄운다.
필요한 파이썬 패키지는 v2/requirements.txt(pyserial·openpyxl·lxml·numpy — C:\\CH12_v2\\venv 에 한 번 설치).
"""
from __future__ import annotations

import argparse
import io
import subprocess
import sys
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from v2.common.winnames import problems   # noqa: E402

EXCLUDE_DIRS = ("v2/tests/",)
EXCLUDE_TOOLS = ("v2/tools/v1golden_", "v2/tools/robot_golden_cases", "v2/tools/make_web_golden_")
VERSION_NAME = "VERSION"


class PackError(RuntimeError):
    pass


def _git(args: List[str], root: Path) -> bytes:
    try:
        r = subprocess.run(["git", *args], cwd=root, capture_output=True, timeout=300)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise PackError(f"git 을 실행할 수 없음: {e}") from e
    if r.returncode != 0:
        raise PackError(f"git {' '.join(args)} 실패: {r.stderr.decode('utf-8', 'replace').strip()}")
    return r.stdout


def included(path: str) -> bool:
    """zip 에 넣는 경로인가(v2/ 아래, 시험·골든 도구·__pycache__ 제외)."""
    if not path.startswith("v2/") or path.startswith(EXCLUDE_DIRS) or path.startswith(EXCLUDE_TOOLS):
        return False
    parts = path.split("/")
    return "__pycache__" not in parts and not path.endswith((".pyc", ".pyo"))


def version_of(root: Path, ref: str = "HEAD") -> Tuple[str, str]:
    """('브랜치@커밋7', 전체 커밋). 분리 HEAD 면 'detached@커밋7'."""
    sha = _git(["rev-parse", f"{ref}^{{commit}}"], root).decode().strip()
    name = _git(["rev-parse", "--abbrev-ref", ref], root).decode().strip()
    if not name or name == "HEAD":
        name = "detached"
    return f"{name}@{sha[:7]}", sha


def dirty_v2(root: Path) -> List[str]:
    out = _git(["status", "--porcelain", "--untracked-files=no", "--", "v2"], root).decode("utf-8", "replace")
    return [ln for ln in out.splitlines() if ln.strip()]


def build(root: Path = ROOT, out_dir: Optional[Path] = None, ref: str = "HEAD",
          now: Optional[datetime] = None) -> Tuple[Path, str, int]:
    """zip 을 만들고 (경로, 버전, 파일 수)."""
    root = Path(root)
    if not (root / ".git").exists():
        raise PackError(f"{root} 는 git 저장소가 아님 — 꾸러미는 저장소에서 만든다(어느 커밋인지 알아야 함)")
    ver, sha = version_of(root, ref)
    raw = _git(["archive", "--format=tar", sha, "--", "v2"], root)
    entries: List[Tuple[str, bytes, tuple]] = []
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as tf:
        for m in tf.getmembers():
            if not m.isfile() or not included(m.name):
                continue
            f = tf.extractfile(m)
            data = f.read() if f is not None else b""
            entries.append((m.name, data, datetime.fromtimestamp(m.mtime).timetuple()[:6]))
    if not entries:
        raise PackError(f"{ver}: v2 파일이 없음")
    bad = {p: problems(p) for p, _, _ in entries if problems(p)}
    if bad:
        raise PackError(f"윈도우에서 풀 수 없는 경로 {len(bad)}개: {list(bad.items())[:5]}")
    stamp = (now or datetime.now()).isoformat(timespec="seconds")
    version_text = f"{ver}\n{sha}\n만든 시각 {stamp}\n"
    out_dir = Path(out_dir) if out_dir is not None else root.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    safe = ver.replace("/", "-").replace("@", "_")
    dest = out_dir / f"ch12_v2_{safe}.zip"
    tmp = dest.with_name(dest.name + ".part")
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        zi = zipfile.ZipInfo(VERSION_NAME, date_time=entries[0][2])
        zi.compress_type = zipfile.ZIP_DEFLATED
        z.writestr(zi, version_text.encode("utf-8"))
        for name, data, dt in sorted(entries):
            zi = zipfile.ZipInfo(name, date_time=dt)
            zi.compress_type = zipfile.ZIP_DEFLATED
            zi.external_attr = 0o644 << 16
            zi.flag_bits |= 0x800                      # 이름 UTF-8(윈도우 탐색기·파이썬 모두 한글 이름 그대로)
            z.writestr(zi, data)
    tmp.replace(dest)
    return dest, ver, len(entries)


def main(argv: Optional[Sequence[str]] = None) -> int:
    for st in (sys.stdout, sys.stderr):
        try:
            st.reconfigure(errors="replace")
        except Exception:
            pass
    ap = argparse.ArgumentParser(prog="python -m v2.tools.pack", description="챔버12 v2 꾸러미(zip) — v2 만, 시험·골든 제외, VERSION 포함")
    ap.add_argument("--out", help="zip 을 둘 폴더(기본: 저장소의 상위 폴더)")
    ap.add_argument("--ref", default="HEAD", help="담을 커밋(기본 HEAD)")
    a = ap.parse_args(argv)
    try:
        dirty = dirty_v2(ROOT) if a.ref == "HEAD" else []
        dest, ver, n = build(ROOT, Path(a.out) if a.out else None, a.ref)
    except PackError as e:
        print(f"[중지] {e}", flush=True)
        return 1
    if dirty:
        print(f"[알림] 커밋하지 않은 v2 변경 {len(dirty)}개는 담지 않았습니다(꾸러미 = 커밋 {ver} 그대로):", flush=True)
        for ln in dirty[:10]:
            print(f"  {ln}", flush=True)
    print(f"꾸러미 {dest} — 버전 {ver} · 파일 {n}개 + VERSION", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
