"""v2 는 v1 모듈을 import 하지 않고, 표준 라이브러리 + pyserial + pytest 만 쓴다(정해진 곳만 예외).

예외(필요한 곳에서만, 함수 안 지연 import 권장):
- openpyxl : v1 CH1/CH2 엑셀 기록 출력(v2/ch12/outputs)
- cv2·numpy: 카메라 사진(v2/ch12/drivers/camera.py, v2/ch12/outputs) — v1 util/camera_recorder.py 와 같은 라이브러리
- 업체 라이브러리: OES·RGA 워커(v2/ch12/workers — 별도 프로세스)
또 Python 3.11 문법까지만(장비 노트북) — 모든 파일을 feature_version=(3, 11) 로 파싱한다.
"""
import ast
import sys
from pathlib import Path

V2_DIR = Path(__file__).resolve().parents[1]
V1_TOP = {"lib", "core", "device", "controller", "runtime", "host", "util", "ui", "errors", "apps", "main",
          "config", "recipes", "tests", "integrations", "dll"}
BASE_ALLOWED = set(sys.stdlib_module_names) | {"v2", "pytest", "__future__", "serial"}
EXTRA = {"ch12/outputs": {"openpyxl", "cv2", "numpy"}, "ch12/drivers/camera.py": {"cv2", "numpy"},
         "ch12/workers": {"cv2", "numpy", "pyvisa", "usb", "clr", "pythoncom", "win32com", "serial"}}


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                yield a.name.split(".")[0], node.lineno
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                yield node.module.split(".")[0], node.lineno


def _py_files():
    return [p for p in V2_DIR.rglob("*.py") if "node_modules" not in p.parts]


def test_no_v1_imports():
    bad = [f"{py.relative_to(V2_DIR)}:{line} imports {top}"
           for py in _py_files() for top, line in _imports(py) if top in V1_TOP]
    assert not bad, bad


def test_stdlib_only_except_listed():
    bad = []
    for py in _py_files():
        rel = py.relative_to(V2_DIR).as_posix()
        allowed = set(BASE_ALLOWED)
        for k, extra in EXTRA.items():
            if rel.startswith(k):
                allowed |= extra
        for top, line in _imports(py):
            if top not in allowed:
                bad.append(f"{rel}:{line} imports {top}")
    assert not bad, bad


def test_python_311_syntax():
    bad = []
    for py in _py_files():
        try:
            ast.parse(py.read_text(encoding="utf-8"), filename=str(py), feature_version=(3, 11))
        except SyntaxError as e:
            bad.append(f"{py.relative_to(V2_DIR)}:{e.lineno} {e.msg}")
    assert not bad, bad
