# ui/thickness_material_dialog.py
# -*- coding: utf-8 -*-
"""두께 모니터 재료 목록 편집 — 표(이름·기호·density·Z·note), 행 추가/삭제/수정.

저장 때 칸마다 범위(density 0.500~99.99, Z 0.100~9.999)·이름 빈칸·이름 중복(대소문자 무시)을 검사하고 틀린 칸은 빨갛게, 저장하지 않는다.
어떤 건에서 쓰는 재료(used_names)를 지우려 하면 확인창. 저장은 원자적 교체, 저장 뒤 on_saved() — Factor 설정창이 드롭다운을 갱신한다.
진행 중인 공정에는 영향 없고 다음 공정 시작 때 적용된다.
"""
from __future__ import annotations

from typing import Callable, Iterable, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (QDialog, QHBoxLayout, QHeaderView, QLabel, QMessageBox, QPushButton, QTableWidget,
                               QTableWidgetItem, QVBoxLayout, QWidget)

from lib.thickness_materials import Material, check_material_fields, load_materials, save_materials

COLS = ("이름", "기호", "density (g/cm3)", "Z-factor", "note")
_BAD_BG = QColor("#ffcdd2")
NEXT_RUN_NOTE = "저장하면 다음 공정 시작 때 적용됩니다(진행 중인 공정에는 영향 없음)."


def _num_text(v) -> str:
    return "" if v is None else f"{float(v):g}"


class ThicknessMaterialDialog(QDialog):
    def __init__(self, path: str, *, used_names: Iterable[str] = (), on_saved: Optional[Callable[[], object]] = None,
                 parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._path = path
        self._used = {str(n).casefold(): str(n) for n in used_names if n}
        self._on_saved = on_saved
        self.confirm: Callable[[str], bool] = self._ask           # 테스트에서 바꿔 끼운다
        self.setWindowTitle("두께 모니터 재료 목록")
        self.setModal(True)
        self.resize(640, 560)

        mats, warns = load_materials(path)
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("재료 목록 — 이름(재료를 구별하는 키, 중복 불가)·기호·density 0.500~99.99·Z-factor 0.100~9.999"))
        self.table = QTableWidget(0, len(COLS), self)
        self.table.setHorizontalHeaderLabels(list(COLS))
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.setColumnWidth(0, 170)
        self.table.setColumnWidth(4, 170)
        for m in mats:
            self._append_row(m)
        lay.addWidget(self.table)
        if warns:
            w = QLabel("⚠ 읽기 경고(그 항목은 빠짐):\n" + "\n".join(warns[:6])
                       + (f"\n… 외 {len(warns) - 6}건" if len(warns) > 6 else ""), self)
            w.setWordWrap(True)
            w.setStyleSheet("color: #ef6c00; font-size: 8pt;")
            lay.addWidget(w)
        note = QLabel(f"파일: {path}\n{NEXT_RUN_NOTE}", self)
        note.setWordWrap(True)
        note.setStyleSheet("color: #6b7280; font-size: 8pt;")
        lay.addWidget(note)

        row = QHBoxLayout()
        self.add_button = QPushButton("행 추가", self)
        self.del_button = QPushButton("행 삭제", self)
        row.addWidget(self.add_button)
        row.addWidget(self.del_button)
        row.addStretch(1)
        self.save_button = QPushButton("저장", self)
        self.cancel_button = QPushButton("취소", self)
        row.addWidget(self.save_button)
        row.addWidget(self.cancel_button)
        lay.addLayout(row)
        self.add_button.clicked.connect(self._on_add)
        self.del_button.clicked.connect(self._on_delete)
        self.save_button.clicked.connect(self._on_save)
        self.cancel_button.clicked.connect(self.reject)

    # ── 표 ──
    def _append_row(self, m: Optional[Material] = None) -> int:
        r = self.table.rowCount()
        self.table.insertRow(r)
        vals = ("", "", "", "", "") if m is None else (m.name, m.symbol, _num_text(m.density), _num_text(m.z_factor), m.note)
        for c, v in enumerate(vals):
            self.table.setItem(r, c, QTableWidgetItem(v))
        return r

    def _text(self, r: int, c: int) -> str:
        it = self.table.item(r, c)
        return it.text().strip() if it is not None else ""

    def _on_add(self, *_a) -> None:
        r = self._append_row()
        self.table.setCurrentCell(r, 0)
        self.table.editItem(self.table.item(r, 0))

    def _ask(self, text: str) -> bool:
        return QMessageBox.question(self, "재료 삭제", text, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                    QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes

    def delete_rows(self, rows: List[int]) -> bool:
        """행 삭제 — 건에서 쓰는 재료가 있으면 확인창(아니오면 지우지 않음). 지웠으면 True."""
        rows = sorted(set(r for r in rows if 0 <= r < self.table.rowCount()), reverse=True)
        if not rows:
            return False
        used = [self._text(r, 0) for r in rows if self._text(r, 0).casefold() in self._used]
        if used and not self.confirm(f"Factor 설정에서 쓰는 재료입니다: {', '.join(used)}\n"
                                     "지우면 그 건은 '재료 목록에 없음'이 되어 Thickness 모드 시작이 거부됩니다(E845).\n지울까요?"):
            return False
        for r in rows:
            self.table.removeRow(r)
        return True

    def _on_delete(self, *_a) -> None:
        self.delete_rows([i.row() for i in self.table.selectedIndexes()] or [self.table.currentRow()])

    # ── 검증·저장 ──
    def collect(self):
        """(재료 목록, 오류 목록). 틀린 칸은 빨갛게."""
        out: List[Material] = []
        errs: List[str] = []
        seen = {}
        for r in range(self.table.rowCount()):
            for c in range(len(COLS)):
                it = self.table.item(r, c)
                if it is not None:
                    it.setBackground(QColor(Qt.GlobalColor.white))
            vals = [self._text(r, c) for c in range(len(COLS))]
            if not any(vals):
                continue                                         # 완전히 빈 행은 무시
            m, e = check_material_fields(vals[0], vals[1], vals[2], vals[3])
            if e:
                errs.append(f"{r + 1}행 '{vals[0]}': {', '.join(e)}")
                for c, key in ((0, "이름"), (2, "density"), (3, "z_factor")):
                    if any(x.startswith(key) for x in e):
                        self.table.item(r, c).setBackground(_BAD_BG)
                continue
            k = m.name.casefold()
            if k in seen:
                errs.append(f"{r + 1}행 '{m.name}': 이름 중복({seen[k] + 1}행)")
                self.table.item(r, 0).setBackground(_BAD_BG)
                continue
            seen[k] = r
            m.note = vals[4]
            out.append(m)
        return out, errs

    def _on_save(self, *_a) -> None:
        mats, errs = self.collect()
        if errs:
            QMessageBox.warning(self, "재료 목록", "고칠 칸이 있어 저장하지 않았습니다:\n\n" + "\n".join(errs[:15]))
            return
        try:
            save_materials(self._path, mats)
        except Exception as e:
            QMessageBox.warning(self, "재료 목록", f"저장 실패(파일은 바뀌지 않음):\n{e!r}")
            return
        if self._on_saved is not None:
            try:
                self._on_saved()
            except Exception:
                pass
        self.accept()
