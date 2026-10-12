# util/file_pick.py
# -*- coding: utf-8 -*-
"""비동기(qasync) 파일 열기 다이얼로그 — 레시피 파일 선택 공용.

쓰는 곳: main.py(Pre-Sputter 레시피), runtime/chamber_runtime.py·runtime/plasma_cleaning_runtime.py(_aopen_file).

배경 — 2026-10-12 00:26:52 프로그램 종료(Qt6Gui.dll+0x431326 = QDrag::dragCursor 첫 줄, 접근 위반):
  Qt 자체(비네이티브) QFileDialog 는 파일 목록·상세 트리가 InternalMove, 사이드바가 DragDrop 이고 모델도 쓰기 가능이다.
  파일을 더블클릭할 때 마우스가 조금만 움직여도 드래그가 시작되는데, 드래그가 도는 동안에도 qasync 콜백이 돌아
  선택 처리와 창 삭제(WA_DeleteOnClose·finished 안 deleteLater·대기 뒤 close+deleteLater)가 실행됐다.
  창(= 드래그를 시작한 뷰의 부모)이 지워지면 QDrag 도 함께 지워지고, Qt(Windows) GiveFeedback 이
  null 검사 없이 currentDrag()->dragCursor() 를 불러 종료된다. Qt 6.9.1 ~ dev 브랜치 모두 같은 코드라
  Qt 를 올려도 해결되지 않는다 → 이쪽에서 막는다.

원칙
  1) 창 안의 드래그 앤 드롭을 전부 끈다(파일 목록·상세 트리·사이드바). ReadOnly 로 열어
     이름 바꾸기·삭제·새 폴더·끌어 옮기기(NAS 레시피 파일 이동)도 막는다. 파일 선택 동작은 그대로다.
  2) 창 삭제 경로는 하나다. finished 처리 안에서는 결과만 읽고 지우지 않으며 WA_DeleteOnClose 도 쓰지 않는다.
     결과를 받은 뒤 숨기고, 마우스 버튼이 모두 떨어진 다음 deleteLater 를 한 번만 부른다.
  3) 네이티브 다이얼로그 우회(Windows 쉘/COM 으로 UI 가 멈추는 문제)·WindowModal·시작 폴더 50ms 지연 적용은 그대로다.
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Optional

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import QAbstractItemView, QDialog, QFileDialog, QWidget

try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:  # pragma: no cover
    def _qt_is_valid(obj: Any) -> bool:
        return obj is not None


# 결과를 받은 뒤 마우스 버튼이 눌려 있으면 이 간격으로 다시 확인한다.
DISPOSE_POLL_MS = 200
# 이만큼 기다려도 버튼이 눌려 있으면 지우지 않고 숨긴 채로 둔다(부모가 정리할 때 함께 정리됨).
DISPOSE_MAX_WAIT_MS = 10 * 60 * 1000


def _mouse_buttons_down() -> bool:
    """Qt 가 아는 마우스 버튼 상태 — 하나라도 눌려 있으면 True. (테스트에서 바꿔 끼울 수 있게 함수로 둔다)"""
    try:
        return QGuiApplication.mouseButtons() != Qt.MouseButton.NoButton
    except Exception:
        return False


def _disable_drag_drop(dlg: QFileDialog) -> None:
    """다이얼로그 안 모든 아이템 뷰(파일 목록·상세 트리·사이드바 등)의 드래그 앤 드롭을 끈다.
    ※ DontUseNativeDialog 를 켠 뒤(=위젯이 만들어진 뒤)에 불러야 한다."""
    for view in dlg.findChildren(QAbstractItemView):
        with contextlib.suppress(Exception):
            view.setDragDropMode(QAbstractItemView.DragDropMode.NoDragDrop)
            view.setDragEnabled(False)
            view.setAcceptDrops(False)
            vp = view.viewport()
            if vp is not None:
                vp.setAcceptDrops(False)


def _dispose_when_idle(dlg: QFileDialog) -> None:
    """유일한 삭제 경로: 숨기고, 마우스 버튼이 모두 떨어진 뒤 deleteLater 1회."""
    if not _qt_is_valid(dlg):
        return
    with contextlib.suppress(Exception):
        if dlg.isVisible():
            dlg.hide()  # WA_DeleteOnClose 를 쓰지 않으므로 close() 대신 hide()
    waited = 0

    def _try_delete() -> None:
        nonlocal waited
        if not _qt_is_valid(dlg):
            return
        if _mouse_buttons_down():
            waited += DISPOSE_POLL_MS
            if waited < DISPOSE_MAX_WAIT_MS:
                QTimer.singleShot(DISPOSE_POLL_MS, _try_delete)
            return
        with contextlib.suppress(Exception):
            dlg.deleteLater()

    # 지금 이 자리(코루틴 재개 지점)에서 바로 지우지 않고 이벤트 루프로 넘긴다.
    QTimer.singleShot(0, _try_delete)


async def pick_open_file(parent: Optional[QWidget], caption: str, start_dir: str = "",
                         name_filter: str = "All Files (*)") -> str:
    """기존 파일 1개를 고른다. 취소하면 "" (호출부는 기존처럼 빈 문자열 = 변경 없음)."""
    dlg = QFileDialog(parent, caption, "", name_filter)
    dlg.setFileMode(QFileDialog.FileMode.ExistingFile)
    # 순서 중요: 비네이티브로 먼저 바꿔야 위젯·모델이 만들어져 ReadOnly 가 모델에 적용되고 뷰를 찾을 수 있다.
    dlg.setOption(QFileDialog.Option.DontUseNativeDialog, True)  # Windows 쉘/COM 우회(UI 멈춤 방지)
    dlg.setOption(QFileDialog.Option.ReadOnly, True)             # 이름 바꾸기·삭제·새 폴더·끌어 옮기기 차단
    _disable_drag_drop(dlg)
    dlg.setWindowModality(Qt.WindowModality.WindowModal)

    loop = asyncio.get_running_loop()
    fut: asyncio.Future[str] = loop.create_future()

    def _on_finished(result: int) -> None:
        # 결과만 읽는다 — 여기서 창을 닫거나 지우지 않는다(드래그·중첩 루프 안에서 불릴 수 있음).
        if fut.done():
            return
        path = ""
        with contextlib.suppress(Exception):
            if int(result) == int(QDialog.DialogCode.Accepted):
                files = dlg.selectedFiles()
                if files:
                    path = str(files[0])
        fut.set_result(path)

    dlg.finished.connect(_on_finished)
    dlg.open()
    _disable_drag_drop(dlg)  # 열린 뒤 한 번 더(늦게 만들어지는 뷰가 있어도 빠지지 않게 — 중복 호출 무해)
    if start_dir:
        QTimer.singleShot(50, lambda: dlg.setDirectory(start_dir) if _qt_is_valid(dlg) else None)

    try:
        return await fut
    finally:
        # 취소(CancelledError)여도 같은 경로로 정리한다.
        with contextlib.suppress(Exception):
            dlg.finished.disconnect(_on_finished)
        _dispose_when_idle(dlg)
