# -*- coding: utf-8 -*-
"""공용 비동기 파일 열기(util/file_pick.py) 검증 — 2026-10-12 00:26:52 크래시(Qt6Gui QDrag::dragCursor) 대책.
실제 QFileDialog(offscreen) + qasync 이벤트 루프로 돌린다. 네트워크·장치 접근 없음.

1) 비네이티브·ReadOnly·WindowModal, WA_DeleteOnClose 없음, 창 안 모든 아이템 뷰 드래그 앤 드롭 꺼짐
2) 선택(Accepted) → 경로 반환. 마우스 버튼이 눌린 동안에는 결과를 돌려준 뒤에도 지우지 않고(숨김),
   버튼이 떨어지면 그때 1회 삭제 — finished 처리·WA_DeleteOnClose 같은 다른 삭제 경로가 없다는 뜻
3) 취소(Rejected) → "" 반환 + 같은 경로로 삭제
4) 코루틴 취소(CancelledError) → 창 숨김 + 같은 경로로 삭제
5) ReadOnly 모델은 끌어 옮기기(MoveAction drop)를 거부 — 파일이 그대로 있다
6) 호출부 3곳(main.py Pre-Sputter, chamber/plasma_cleaning _aopen_file)이 공용 함수를 쓰고(함수 안에
   QFileDialog 생성·WA_DeleteOnClose 없음), 저장소에서 QFileDialog 를 직접 만드는 곳은 util/file_pick.py 뿐이다
7) 실제 호출부 3곳을 그대로 불러 선택 → 경로(Pre-Sputter 는 기존 후처리 순서 그대로), 취소 → 변경 없음
"""
import os
import sys
import ast
import asyncio
import contextlib
import subprocess
import tempfile
import textwrap
from pathlib import Path

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import pytest                                                     # noqa: E402
from PySide6.QtCore import QMimeData, QUrl, Qt                    # noqa: E402
from PySide6.QtWidgets import (QAbstractItemView, QApplication,   # noqa: E402
                               QFileDialog, QFileSystemModel, QLineEdit, QWidget)
from shiboken6 import isValid                                     # noqa: E402

import util.file_pick as fp                                       # noqa: E402

_APP = QApplication.instance() or QApplication(sys.argv)


def _run(coro_fn):
    """qasync 이벤트 루프(장비 프로그램과 같은 방식)로 코루틴 1개를 끝까지 돌린다."""
    import qasync
    loop = qasync.QEventLoop(_APP)
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro_fn())
    finally:
        asyncio.set_event_loop(None)
        with contextlib.suppress(Exception):
            loop.close()   # GC 때 닫히면 'Signal source has been deleted' 경고가 남으므로 여기서 닫는다


def _tmp_tree() -> Path:
    tmp = Path(tempfile.mkdtemp())
    (tmp / "a.csv").write_text("Process_name\nx\n", encoding="utf-8")
    (tmp / "sub").mkdir()
    return tmp


async def _wait_dialog(parent: QWidget, timeout_s: float = 5.0) -> QFileDialog:
    """parent 아래에서 '보이는' 파일 다이얼로그를 찾는다(앞서 숨겨져 삭제 대기 중인 창은 건너뜀)."""
    for _ in range(int(timeout_s / 0.02)):
        for dlg in parent.findChildren(QFileDialog):
            if isValid(dlg) and dlg.isVisible():
                return dlg
        await asyncio.sleep(0.02)
    raise AssertionError("다이얼로그가 열리지 않음")


async def _wait_until(pred, timeout_s: float = 5.0) -> bool:
    for _ in range(int(timeout_s / 0.02)):
        if pred():
            return True
        await asyncio.sleep(0.02)
    return pred()


# ───────────────────────── 1 ─────────────────────────
def test_1_dialog_flags_and_no_drag_drop(monkeypatch):
    monkeypatch.setattr(fp, "_mouse_buttons_down", lambda: False)
    tmp = _tmp_tree()
    parent = QWidget()
    seen = {}

    async def _main():
        t = asyncio.ensure_future(fp.pick_open_file(parent, "t", str(tmp), "CSV Files (*.csv)"))
        dlg = await _wait_dialog(parent)
        seen["native_off"] = dlg.testOption(QFileDialog.Option.DontUseNativeDialog)
        seen["readonly"] = dlg.testOption(QFileDialog.Option.ReadOnly)
        seen["model_ro"] = dlg.findChild(QFileSystemModel).isReadOnly()
        seen["delete_on_close"] = dlg.testAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        seen["modality"] = dlg.windowModality()
        seen["views"] = [(v.objectName(), v.dragDropMode(), v.dragEnabled(), v.acceptDrops(),
                          v.viewport().acceptDrops()) for v in dlg.findChildren(QAbstractItemView)]
        dlg.reject()
        return await t

    assert _run(_main) == ""
    assert seen["native_off"] is True
    assert seen["readonly"] is True and seen["model_ro"] is True
    assert seen["delete_on_close"] is False
    assert seen["modality"] == Qt.WindowModality.WindowModal
    names = {n for n, *_ in seen["views"]}
    assert {"listView", "treeView", "sidebar"} <= names          # 원래 드래그가 켜져 있던 뷰들
    for name, mode, drag, acc, vp_acc in seen["views"]:
        assert mode == QAbstractItemView.DragDropMode.NoDragDrop, name
        assert drag is False and acc is False and vp_acc is False, name


# ───────────────────────── 2 ─────────────────────────
def test_2_accept_returns_path_and_single_deferred_delete(monkeypatch):
    pressed = {"v": True}                                        # 버튼이 눌린 상태로 시작
    monkeypatch.setattr(fp, "_mouse_buttons_down", lambda: pressed["v"])
    monkeypatch.setattr(fp, "DISPOSE_POLL_MS", 50)
    tmp = _tmp_tree()
    parent = QWidget()
    seen = {}

    async def _main():
        t = asyncio.ensure_future(fp.pick_open_file(parent, "t", str(tmp), "CSV Files (*.csv)"))
        dlg = await _wait_dialog(parent)
        dlg.findChild(QLineEdit, "fileNameEdit").setText((tmp / "a.csv").as_posix())
        dlg.accept()
        seen["path"] = await t
        await asyncio.sleep(0.5)                                 # 버튼이 눌린 동안: 지우면 안 된다
        seen["alive_while_pressed"] = isValid(dlg)
        seen["hidden_while_pressed"] = isValid(dlg) and not dlg.isVisible()
        pressed["v"] = False                                     # 버튼이 떨어지면
        seen["deleted_after_release"] = await _wait_until(lambda: not isValid(dlg))

    _run(_main)
    assert seen["path"] and os.path.samefile(seen["path"], tmp / "a.csv")
    assert seen["alive_while_pressed"] is True                   # finished 안 삭제·WA_DeleteOnClose 경로 없음
    assert seen["hidden_while_pressed"] is True
    assert seen["deleted_after_release"] is True                 # 삭제 경로는 하나(버튼이 떨어진 뒤)


# ───────────────────────── 3 ─────────────────────────
def test_3_reject_returns_empty_and_deletes(monkeypatch):
    monkeypatch.setattr(fp, "_mouse_buttons_down", lambda: False)
    tmp = _tmp_tree()
    parent = QWidget()
    seen = {}

    async def _main():
        t = asyncio.ensure_future(fp.pick_open_file(parent, "t", str(tmp), "CSV Files (*.csv)"))
        dlg = await _wait_dialog(parent)
        dlg.reject()
        seen["path"] = await t
        seen["deleted"] = await _wait_until(lambda: not isValid(dlg))

    _run(_main)
    assert seen["path"] == ""
    assert seen["deleted"] is True


# ───────────────────────── 4 ─────────────────────────
def test_4_cancelled_task_hides_and_deletes(monkeypatch):
    monkeypatch.setattr(fp, "_mouse_buttons_down", lambda: False)
    tmp = _tmp_tree()
    parent = QWidget()
    seen = {}

    async def _main():
        t = asyncio.ensure_future(fp.pick_open_file(parent, "t", str(tmp), "CSV Files (*.csv)"))
        dlg = await _wait_dialog(parent)
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t
        seen["cancelled"] = t.cancelled()
        seen["hidden"] = (not isValid(dlg)) or (not dlg.isVisible())
        seen["deleted"] = await _wait_until(lambda: not isValid(dlg))

    _run(_main)
    assert seen["cancelled"] is True
    assert seen["hidden"] is True
    assert seen["deleted"] is True


# ───────────────────────── 5 ─────────────────────────
def test_5_readonly_model_refuses_drag_move(monkeypatch):
    monkeypatch.setattr(fp, "_mouse_buttons_down", lambda: False)
    tmp = _tmp_tree()
    parent = QWidget()
    seen = {}

    async def _main():
        t = asyncio.ensure_future(fp.pick_open_file(parent, "t", str(tmp), "All Files (*)"))
        dlg = await _wait_dialog(parent)
        model = dlg.findChild(QFileSystemModel)
        sub = (tmp / "sub").as_posix()
        seen["sub_loaded"] = await _wait_until(lambda: model.index(sub).isValid())  # 폴더가 모델에 올라온 뒤 검사
        sub_idx = model.index(sub)
        mime = QMimeData()
        mime.setUrls([QUrl.fromLocalFile(str(tmp / "a.csv"))])
        seen["drop_ok"] = model.dropMimeData(mime, Qt.DropAction.MoveAction, -1, -1, sub_idx)
        seen["sub_flags_drop"] = bool(model.flags(sub_idx) & Qt.ItemFlag.ItemIsDropEnabled)
        dlg.reject()
        await t

    _run(_main)
    assert seen["sub_loaded"] is True
    assert seen["drop_ok"] is False
    assert seen["sub_flags_drop"] is False
    assert (tmp / "a.csv").exists() and not (tmp / "sub" / "a.csv").exists()


def _tracked_py(root: Path) -> list:
    """git 이 추적하는 .py 상대경로(가상환경·빌드 폴더 제외 효과). git 이 없으면 소스 폴더만 훑는다."""
    with contextlib.suppress(Exception):
        out = subprocess.run(["git", "ls-files", "-z", "--", "*.py"], cwd=str(root),
                             capture_output=True, timeout=30)
        if out.returncode == 0 and out.stdout:
            return [x for x in out.stdout.decode("utf-8", "replace").split("\0") if x]
    rels = ["main.py"]
    for d in ("apps", "controller", "device", "host", "lib", "runtime", "ui", "util"):
        rels += [p.relative_to(root).as_posix() for p in (root / d).rglob("*.py")]
    return rels


def _load_method(path: Path, cls: str, name: str, glb: dict):
    """클래스 메서드 하나의 '실제 소스'만 떼어 실행 가능한 함수로 만든다.
    (main.py 는 import 할 때 Windows 이벤트 루프 정책을 바꾸므로 테스트에서 import 하지 않는다)"""
    code = textwrap.dedent(_func_src(path, cls, name))
    ns = dict(glb)
    exec(compile(code, str(path), "exec"), ns)
    return ns[name]


# ───────────────────────── 6 ─────────────────────────
def _func_src(path: Path, cls: str, name: str) -> str:
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for f in node.body:
                if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef)) and f.name == name:
                    return ast.get_source_segment(src, f)
    raise AssertionError(f"{path.name}: {cls}.{name} 없음")


def test_6_call_sites_use_shared_picker():
    root = Path(_ROOT)
    cases = [
        (root / "main.py", "MainWindow", "_pick_presputter_recipe"),
        (root / "runtime" / "chamber_runtime.py", "ChamberRuntime", "_aopen_file"),
        (root / "runtime" / "plasma_cleaning_runtime.py", "PlasmaCleaningRuntime", "_aopen_file"),
    ]
    for path, cls, name in cases:
        body = _func_src(path, cls, name)
        assert "await pick_open_file(" in body, (path.name, name)
        assert "QFileDialog(" not in body and "WA_DeleteOnClose" not in body, (path.name, name)
        assert "from util.file_pick import pick_open_file" in path.read_text(encoding="utf-8")

    # 저장소(git 이 추적하는 .py, tests 제외)에서 QFileDialog 를 직접 만드는 곳은 util/file_pick.py 하나뿐이다
    hits = []
    for rel in _tracked_py(root):
        if rel.startswith("tests/") or rel == "util/file_pick.py":
            continue
        s = (root / rel).read_text(encoding="utf-8", errors="replace")
        if "QFileDialog(" in s:
            hits.append(rel)
    assert hits == []


# ───────────────────────── 7 ─────────────────────────
def test_7_call_sites_behave_as_before(monkeypatch):
    """실제 호출부 3곳을 그대로 불러 선택·취소 결과가 기존과 같은지(선택 → 경로, 취소 → 변경 없음)."""
    monkeypatch.setattr(fp, "_mouse_buttons_down", lambda: False)
    from runtime.chamber_runtime import ChamberRuntime
    from runtime.plasma_cleaning_runtime import PlasmaCleaningRuntime
    pick_presputter = _load_method(Path(_ROOT) / "main.py", "MainWindow", "_pick_presputter_recipe",
                                   {"os": os, "pick_open_file": fp.pick_open_file})

    tmp = _tmp_tree()
    target = tmp / "a.csv"
    parent = QWidget()
    got, calls = {}, []

    async def _answer(where: QWidget, path=None):
        dlg = await _wait_dialog(where)
        if path is None:
            dlg.reject()
        else:
            dlg.findChild(QLineEdit, "fileNameEdit").setText(Path(path).as_posix())
            dlg.accept()

    class _RT:
        recipe_path = str(tmp / "old.csv")

        def set_recipe_path(self, p):
            calls.append(("set_recipe_path", p))

    async def _main():
        # 챔버 _aopen_file
        c = ChamberRuntime.__new__(ChamberRuntime)
        c._has_ui = lambda: True
        c._parent_widget = lambda: parent
        c.append_log = lambda *a, **k: None
        ans = asyncio.ensure_future(_answer(parent, target))
        got["chamber"] = await c._aopen_file(caption="t", start_dir=str(tmp),
                                            name_filter="CSV Files (*.csv);;All Files (*.*)")
        await ans

        # 플라즈마 클리닝 _aopen_file (취소)
        p = PlasmaCleaningRuntime.__new__(PlasmaCleaningRuntime)
        p._has_ui = lambda: True
        p._parent_widget = lambda: parent
        ans = asyncio.ensure_future(_answer(parent, None))
        got["pc_cancel"] = await p._aopen_file(caption="t", start_dir=str(tmp),
                                              name_filter="CSV Files (*.csv);;All Files (*)")
        await ans
        ans = asyncio.ensure_future(_answer(parent, target))
        got["pc"] = await p._aopen_file(caption="t", start_dir=str(tmp),
                                       name_filter="CSV Files (*.csv);;All Files (*)")
        await ans

        # main.py Pre-Sputter 레시피 — self 가 다이얼로그 부모이므로 QWidget 하나를 self 로 쓴다
        me = QWidget()
        me._update_presputter_recipe_cache = lambda ch, path: calls.append(("cache", ch, path))
        me._save_presputter_recipe_path = lambda ch, path: calls.append(("save", ch, path))
        me._broadcast_log = lambda src, msg: calls.append(("log", src, msg))
        me._refresh_presputter_ui = lambda: calls.append(("refresh",))
        ans = asyncio.ensure_future(_answer(me, None))                # 취소 → 아무것도 안 바뀜
        await pick_presputter(me, 2, _RT())
        await ans
        got["calls_after_cancel"] = list(calls)
        ans = asyncio.ensure_future(_answer(me, target))              # 선택 → 기존과 같은 후처리
        await pick_presputter(me, 2, _RT())
        await ans
        got["all_deleted"] = await _wait_until(
            lambda: not parent.findChildren(QFileDialog) and not me.findChildren(QFileDialog))

    _run(_main)
    assert got["chamber"] and os.path.samefile(got["chamber"], target)
    assert got["pc_cancel"] == "" and got["pc"] and os.path.samefile(got["pc"], target)
    assert got["calls_after_cancel"] == []
    names = [c[0] for c in calls]
    assert names == ["set_recipe_path", "cache", "save", "log", "refresh"]
    assert os.path.samefile(calls[0][1], target)
    assert calls[3][1] == "Auto" and calls[3][2].startswith("PreSputter CH2 레시피 지정: ")
    assert got["all_deleted"] is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
