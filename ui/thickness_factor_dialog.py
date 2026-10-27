# ui/thickness_factor_dialog.py
# -*- coding: utf-8 -*-
"""두께 모니터 Factor 설정창 — 3×3 tooling 표(chuck up/mid/down × 건 G1/G2/G3) + 건별 density/Z/메모.

빈 칸 = 미설정. 저장 때 칸마다 범위를 검사하고(틀린 칸은 빨갛게, 저장하지 않음), 맞으면 파일을 원자적으로 바꾼 뒤
on_saved() 로 서비스에 다시 읽기를 알린다. 장비에 값을 쓰지는 않는다(공정 시작 때 2단계가 쓴다).
"""
from __future__ import annotations

from typing import Callable, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (QDialog, QHBoxLayout, QLabel, QMessageBox, QPushButton, QTableWidget,
                               QTableWidgetItem, QVBoxLayout, QWidget)

from lib.thickness_factors import (CHUCKS, GUNS, MEMO_MAX, ThicknessFactors, check_value, load_factors,
                                   save_factors)

_CHUCK_TEXT = {"up": "Up", "mid": "Mid", "down": "Down"}
_BAD_BG = QColor("#ffcdd2")


def _cell_text(v) -> str:
    return "" if v is None else f"{float(v):g}"


class ThicknessFactorDialog(QDialog):
    def __init__(self, path: str, *, ch: int = 2, on_saved: Optional[Callable[[], object]] = None,
                 parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._path = path
        self._on_saved = on_saved
        self.setWindowTitle(f"CH{int(ch)} 두께 모니터 Factor 설정")
        self.setModal(True)
        self.resize(460, 420)

        fac, warns = load_factors(path)
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("Tooling (%, 10.0~399) — chuck 위치 × 건, 빈 칸 = 미설정"))
        self.tool_table = QTableWidget(len(CHUCKS), len(GUNS), self)
        self.tool_table.setHorizontalHeaderLabels(list(GUNS))
        self.tool_table.setVerticalHeaderLabels([_CHUCK_TEXT[c] for c in CHUCKS])
        for r, c in enumerate(CHUCKS):
            for k, g in enumerate(GUNS):
                self.tool_table.setItem(r, k, QTableWidgetItem(_cell_text(fac.tooling[c][g])))
        self.tool_table.setFixedHeight(126)
        lay.addWidget(self.tool_table)

        lay.addWidget(QLabel("재료값 — 건별 (density 0.500~99.99 g/cm3, Z-factor 0.100~9.999, 메모 = 타깃 이름)"))
        self.mat_table = QTableWidget(len(GUNS), 3, self)
        self.mat_table.setHorizontalHeaderLabels(["density", "Z-factor", "메모"])
        self.mat_table.setVerticalHeaderLabels(list(GUNS))
        for r, g in enumerate(GUNS):
            m = fac.material[g]
            self.mat_table.setItem(r, 0, QTableWidgetItem(_cell_text(m.get("density"))))
            self.mat_table.setItem(r, 1, QTableWidgetItem(_cell_text(m.get("z_factor"))))
            self.mat_table.setItem(r, 2, QTableWidgetItem(str(m.get("memo") or "")))
        self.mat_table.setFixedHeight(126)
        lay.addWidget(self.mat_table)

        self.info = QLabel(f"파일: {path}", self)
        self.info.setWordWrap(True)
        self.info.setStyleSheet("color: #6b7280; font-size: 8pt;")
        lay.addWidget(self.info)
        if warns:
            w = QLabel("⚠ 읽기 경고(그 칸은 미설정으로 표시):\n" + "\n".join(warns[:8])
                       + (f"\n… 외 {len(warns) - 8}건" if len(warns) > 8 else ""), self)
            w.setWordWrap(True)
            w.setStyleSheet("color: #ef6c00; font-size: 8pt;")
            lay.addWidget(w)

        row = QHBoxLayout()
        row.addStretch(1)
        self.save_button = QPushButton("저장", self)
        self.cancel_button = QPushButton("취소", self)
        row.addWidget(self.save_button)
        row.addWidget(self.cancel_button)
        lay.addLayout(row)
        self.save_button.clicked.connect(self._on_save)
        self.cancel_button.clicked.connect(self.reject)

    # ── 검증 ──
    def collect(self):
        """(ThicknessFactors, 오류 목록). 틀린 칸은 빨갛게 칠한다."""
        fac = ThicknessFactors()
        errs: List[str] = []

        def _get(table: QTableWidget, r: int, c: int, kind: str, where: str):
            it = table.item(r, c)
            txt = it.text().strip() if it is not None else ""
            v, err = check_value(kind, txt)
            if it is not None:
                it.setBackground(_BAD_BG if err else QColor(Qt.GlobalColor.white))
            if err:
                errs.append(f"{where}: {err}")
            return v

        for r, c in enumerate(CHUCKS):
            for k, g in enumerate(GUNS):
                fac.tooling[c][g] = _get(self.tool_table, r, k, "tooling", f"tooling {_CHUCK_TEXT[c]}/{g}")
        for r, g in enumerate(GUNS):
            fac.material[g]["density"] = _get(self.mat_table, r, 0, "density", f"{g} density")
            fac.material[g]["z_factor"] = _get(self.mat_table, r, 1, "z_factor", f"{g} Z-factor")
            it = self.mat_table.item(r, 2)
            memo = it.text().strip() if it is not None else ""
            if len(memo) > MEMO_MAX:
                errs.append(f"{g} 메모: {MEMO_MAX}자 넘음")
            fac.material[g]["memo"] = memo[:MEMO_MAX]
        return fac, errs

    def _on_save(self) -> None:
        fac, errs = self.collect()
        if errs:
            QMessageBox.warning(self, "Factor 설정", "고칠 칸이 있어 저장하지 않았습니다:\n\n" + "\n".join(errs))
            return
        try:
            save_factors(self._path, fac)
        except Exception as e:
            QMessageBox.warning(self, "Factor 설정", f"저장 실패(파일은 바뀌지 않음):\n{e!r}")
            return
        if self._on_saved is not None:
            try:
                self._on_saved()
            except Exception:
                pass
        self.accept()
