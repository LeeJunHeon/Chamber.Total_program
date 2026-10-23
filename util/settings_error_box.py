# util/settings_error_box.py
# -*- coding: utf-8 -*-
"""settings.json 에 고칠 곳이 있을 때 시작 전에 띄우는 알림창(자동으로 닫지 않는다)."""
from __future__ import annotations

from PySide6.QtWidgets import QMessageBox

from lib._config_loader import SettingsResult, format_problems

SHOW_MAX = 15        # 알림창 본문에 바로 보여 줄 개수(나머지는 '자세히 보기')


def settings_error_box(res: SettingsResult) -> QMessageBox:
    n = len(res.problems)
    box = QMessageBox()
    box.setIcon(QMessageBox.Icon.Critical)
    box.setWindowTitle("설정 파일 오류 — 프로그램을 시작하지 않습니다")
    box.setText(
        f"settings.json 에 고칠 곳이 {n}건 있습니다.\n"
        "아래 내용을 고친 뒤 프로그램을 다시 실행하세요.\n"
        "(장비에는 아직 연결하지 않았습니다)"
    )
    box.setInformativeText(f"파일: {res.path}\n\n" + format_problems(res, limit=SHOW_MAX))
    if n > SHOW_MAX:
        box.setDetailedText(format_problems(res))
    box.setStandardButtons(QMessageBox.StandardButton.Ok)
    return box
