# ui/thickness_factor_dialog.py
# -*- coding: utf-8 -*-
"""두께 모니터 Factor 설정창 — 3×3 tooling 표(chuck up/mid/down × 건 G1/G2/G3) + 건별 재료 드롭다운.

재료는 재료 목록(config/thickness_materials.json)에서 고른다 — 맨 위 '(선택 안 함)', 항목 표시 "기호 — 이름", 입력해서 찾기 가능.
옆에 고른 재료의 density·Z 를 읽기 전용으로 보여 준다. [재료 목록 편집] 으로 목록을 고치면 드롭다운·표시값이 바로 갱신된다.
빈 칸 = 미설정. 저장 때 tooling 범위·재료 이름을 검사하고(틀린 칸은 빨갛게, 저장하지 않음), 맞으면 파일을 원자적으로 바꾼 뒤
on_saved() 로 서비스에 다시 읽기를 알린다. 저장은 장비에 값을 쓰지 않는다(공정 시작 때 쓴다) — 진행 중인 공정에는 영향 없음.
[장비에 쓰기](service 가 있을 때만): 고른 chuck×건의 tooling 칸 + 그 건 재료 density·Z 를 지금 창의 값(저장 여부 무관)으로
장비에 바로 쓰고 되읽어 확인한다. 공정 중·미연결·응답 없음이면 쓰지 않는다(누르는 시점에 검사). 저장 파일은 건드리지 않는다.
"""
from __future__ import annotations

import asyncio
import contextlib
from typing import Callable, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (QComboBox, QCompleter, QDialog, QGridLayout, QHBoxLayout, QLabel, QMessageBox,
                               QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget)

from lib.thickness_factors import CHUCKS, GUNS, ThicknessFactors, check_tooling, load_factors, save_factors
from lib.thickness_materials import Material, default_materials_file, find_material, load_materials
from ui.thickness_material_dialog import NEXT_RUN_NOTE, ThicknessMaterialDialog

_CHUCK_TEXT = {"up": "Up", "mid": "Mid", "down": "Down"}
_BAD_BG = QColor("#ffcdd2")
NONE_TEXT = "(선택 안 함)"
WRITE_NOTE = "장비에 쓰기 = 지금 창의 값을 장비에 바로 적용(저장과 별개). Thickness 공정은 시작 때 그 공정 값으로 다시 씀."


def _cell_text(v) -> str:
    return "" if v is None else f"{float(v):g}"


class ThicknessFactorDialog(QDialog):
    def __init__(self, path: str, *, ch: int = 2, on_saved: Optional[Callable[[], object]] = None,
                 materials_path: Optional[str] = None, parent: Optional[QWidget] = None,
                 service=None, process_running: Optional[Callable[[], bool]] = None):
        super().__init__(parent)
        self._svc = service
        self._process_running = process_running
        self._tasks: set = set()
        self._msg_boxes: list = []
        self._writing = False
        self.last_message = ("", "")            # (제목, 문구) — 마지막 알림(테스트·로그용)
        self._path = path
        self._materials_path = str(materials_path or default_materials_file())
        self._on_saved = on_saved
        self._materials: List[Material] = []
        self._mat_dialog = None
        self.setWindowTitle(f"CH{int(ch)} Thickness Monitor — Factor 설정")
        self.setModal(True)
        self.resize(500, 440)

        fac, warns = load_factors(path)
        mats, mwarns = load_materials(self._materials_path)
        self._materials = mats
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

        lay.addWidget(QLabel("재료 — 건별(재료 목록에서 고름, density·Z 는 목록 값)"))
        grid = QGridLayout()
        self.combos: Dict[str, QComboBox] = {}
        self.values: Dict[str, QLabel] = {}
        for r, g in enumerate(GUNS):
            grid.addWidget(QLabel(g, self), r, 0)
            cb = QComboBox(self)
            cb.setEditable(True)
            cb.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
            cb.setMinimumWidth(240)
            self.combos[g] = cb
            grid.addWidget(cb, r, 1)
            lab = QLabel("", self)
            lab.setStyleSheet("color: #37474f;")
            self.values[g] = lab
            grid.addWidget(lab, r, 2)
        lay.addLayout(grid)
        self._fill_combos({g: fac.material[g] for g in GUNS})
        for g, cb in self.combos.items():
            cb.currentIndexChanged.connect(lambda _i, gg=g: self._show_values(gg))
            cb.editTextChanged.connect(lambda _t, gg=g: self._show_values(gg))

        self.info = QLabel(f"파일: {path}\n재료 목록: {self._materials_path}\n{NEXT_RUN_NOTE}", self)
        self.info.setWordWrap(True)
        self.info.setStyleSheet("color: #6b7280; font-size: 8pt;")
        lay.addWidget(self.info)
        all_warns = warns + [f"재료 목록: {w}" for w in mwarns]
        missing = [f"{g} 재료 '{fac.material[g]}' 가 재료 목록에 없음 — 다시 고르십시오"
                   for g in GUNS if fac.material[g] and find_material(self._materials, fac.material[g]) is None]
        all_warns += missing
        if all_warns:
            w = QLabel("⚠ 읽기 경고:\n" + "\n".join(all_warns[:8])
                       + (f"\n… 외 {len(all_warns) - 8}건" if len(all_warns) > 8 else ""), self)
            w.setWordWrap(True)
            w.setStyleSheet("color: #ef6c00; font-size: 8pt;")
            lay.addWidget(w)

        # ── 장비에 바로 쓰기(service 가 있을 때만) ──
        wrow = QHBoxLayout()
        self.write_label = QLabel("장비에 바로 쓰기", self)
        self.write_chuck = QComboBox(self)
        for c in CHUCKS:
            self.write_chuck.addItem(_CHUCK_TEXT[c], c)
        self.write_gun = QComboBox(self)
        for g in GUNS:
            self.write_gun.addItem(g, g)
        self.write_button = QPushButton("장비에 쓰기", self)
        self.write_button.setToolTip("고른 chuck×건의 tooling 과 그 건 재료의 density·Z 를 지금 창의 값으로 장비에 바로 씀")
        for w in (self.write_label, self.write_chuck, self.write_gun, self.write_button):
            wrow.addWidget(w)
        wrow.addStretch(1)
        lay.addLayout(wrow)
        self.write_note = QLabel(WRITE_NOTE, self)
        self.write_note.setWordWrap(True)
        self.write_note.setStyleSheet("color: #6b7280; font-size: 8pt;")
        lay.addWidget(self.write_note)
        has_svc = service is not None
        for w in (self.write_label, self.write_chuck, self.write_gun, self.write_button, self.write_note):
            w.setVisible(has_svc)
        self.write_button.setEnabled(has_svc)
        self.write_button.clicked.connect(self._on_write_clicked)

        row = QHBoxLayout()
        self.edit_materials_button = QPushButton("재료 목록 편집", self)
        row.addWidget(self.edit_materials_button)
        row.addStretch(1)
        self.save_button = QPushButton("저장", self)
        self.cancel_button = QPushButton("취소", self)
        row.addWidget(self.save_button)
        row.addWidget(self.cancel_button)
        lay.addLayout(row)
        self.edit_materials_button.clicked.connect(self._on_edit_materials)
        self.save_button.clicked.connect(self._on_save)
        self.cancel_button.clicked.connect(self.reject)

    # ── 재료 드롭다운 ──
    def _fill_combos(self, selected: Dict[str, Optional[str]]) -> None:
        """드롭다운을 재료 목록으로 다시 채우고 selected(건 → 이름)를 고른다. 목록에 없는 이름은 입력 칸에 그대로 둔다."""
        labels = [NONE_TEXT] + [m.label for m in self._materials]
        for g, cb in self.combos.items():
            cb.blockSignals(True)
            cb.clear()
            cb.addItem(NONE_TEXT, "")
            for m in self._materials:
                cb.addItem(m.label, m.name)
            comp = QCompleter(labels, cb)
            comp.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
            comp.setFilterMode(Qt.MatchFlag.MatchContains)
            cb.setCompleter(comp)
            name = selected.get(g)
            m = find_material(self._materials, name) if name else None
            if m is not None:
                cb.setCurrentIndex(cb.findData(m.name))
            elif name:
                cb.setCurrentIndex(-1)
                cb.setEditText(name)
            else:
                cb.setCurrentIndex(0)
            cb.blockSignals(False)
            self._show_values(g)

    def _chosen(self, g: str):
        """(재료 | None, 오류문구). 맨 위 항목·빈 칸 = 미선택(None, '')."""
        cb = self.combos[g]
        text = cb.currentText().strip()
        if not text or text == NONE_TEXT:
            return None, ""
        for m in self._materials:
            if text in (m.label, m.name) or text.casefold() == m.name.casefold():
                return m, ""
        return None, f"{g}: 재료 '{text}' 가 재료 목록에 없음"

    def _show_values(self, g: str) -> None:
        m, err = self._chosen(g)
        lab = self.values[g]
        if m is not None:
            lab.setText(f"ρ {m.density:g} · Z {m.z_factor:.3f}")
            lab.setToolTip(m.note)
        else:
            lab.setText("목록에 없음" if err else "—")
            lab.setToolTip("")

    def selected_names(self) -> Dict[str, Optional[str]]:
        out = {}
        for g in GUNS:
            m, err = self._chosen(g)
            out[g] = m.name if m is not None else (self.combos[g].currentText().strip() if err else None)
        return out

    def reload_materials(self) -> None:
        """재료 목록을 다시 읽어 드롭다운·표시값을 갱신한다(고른 이름은 유지)."""
        keep = self.selected_names()
        self._materials, _w = load_materials(self._materials_path)
        self._fill_combos(keep)

    def _on_edit_materials(self, *_a) -> None:
        used = [n for n in self.selected_names().values() if n]
        dlg = ThicknessMaterialDialog(self._materials_path, used_names=used, on_saved=self._materials_saved, parent=self)
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        self._mat_dialog = dlg
        dlg.open()

    def _materials_saved(self) -> None:
        self.reload_materials()
        if self._on_saved is not None:              # 서비스도 재료 목록을 다시 읽는다
            try:
                self._on_saved()
            except Exception:
                pass

    # ── 장비에 바로 쓰기 ──
    def _notify(self, icon, title: str, text: str) -> None:
        """비모달 알림(창 흐름을 막지 않는다)."""
        self.last_message = (title, text)
        with contextlib.suppress(Exception):
            box = QMessageBox(icon, title, text, QMessageBox.StandardButton.Ok, self)
            box.setWindowModality(Qt.WindowModality.NonModal)
            box.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
            self._msg_boxes.append(box)
            box.finished.connect(lambda *_a, b=box: self._msg_boxes.remove(b) if b in self._msg_boxes else None)
            box.show()

    def _write_values(self):
        """(tooling, 재료, 오류 목록) — 고른 chuck×건 칸과 그 건 재료. 틀린 칸은 빨갛게(저장 검사와 같은 방식)."""
        chuck, gun = self.write_chuck.currentData(), self.write_gun.currentData()
        errs: List[str] = []
        r, k = CHUCKS.index(chuck), GUNS.index(gun)
        it = self.tool_table.item(r, k)
        if it is None:
            it = QTableWidgetItem("")
            self.tool_table.setItem(r, k, it)
        tooling, err = check_tooling(it.text().strip())
        if tooling is None:
            errs.append(f"tooling {_CHUCK_TEXT[chuck]}/{gun}: {err or '빈 칸'}")
        it.setBackground(_BAD_BG if tooling is None else QColor(Qt.GlobalColor.white))
        mat, merr = self._chosen(gun)
        if mat is None:
            errs.append(merr or f"{gun}: 재료 미선택")
        self.combos[gun].setStyleSheet("QComboBox {background: #ffcdd2;}" if mat is None else "")
        return tooling, mat, errs

    def _on_write_clicked(self, *_a) -> None:
        if self._svc is None or self._writing:
            return
        tooling, mat, errs = self._write_values()
        if errs:
            self._notify(QMessageBox.Icon.Warning, "장비에 쓰기", "고칠 칸이 있어 장비에 쓰지 않았습니다:\n\n" + "\n".join(errs))
            return
        from ui.thickness_panel import device_write_block
        try:
            st = dict(self._svc.status() or {})
        except Exception:
            st = {}
        if self._process_running is not None:
            with contextlib.suppress(Exception):
                st["process_running"] = bool(st.get("process_running")) or bool(self._process_running())
        why = device_write_block(st)
        if why:
            self._notify(QMessageBox.Icon.Warning, "장비에 쓰기", f"{why} — 장비에 쓰지 않았습니다.")
            return
        self._writing = True
        self.write_button.setEnabled(False)

        async def _go():
            try:
                got = await self._svc.set_material(float(mat.density), float(mat.z_factor), float(tooling),
                                                   material=mat.name, why="Factor 창 수동")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._notify(QMessageBox.Icon.Warning, "장비에 쓰기", f"장비에 쓰지 못했습니다(되읽기 확인 포함): {e}")
            else:
                d, z, t = got.get("density"), got.get("z_factor"), got.get("tooling")
                self._notify(QMessageBox.Icon.Information, "장비에 쓰기",
                             f"장비에 적용됨 — {mat.name} · density {float(d):g} · Z {float(z):.3f} · tooling {float(t):g}%")
            finally:
                self._writing = False
                with contextlib.suppress(Exception):
                    self.write_button.setEnabled(True)

        try:
            t = asyncio.get_event_loop().create_task(_go())
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)
        except Exception as e:
            self._writing = False
            self.write_button.setEnabled(True)
            self._notify(QMessageBox.Icon.Warning, "장비에 쓰기", f"명령을 실행할 수 없습니다: {e!r}")

    def done(self, r: int) -> None:
        for t in list(self._tasks):                 # 창이 닫히면 진행 중인 쓰기 요청을 취소
            with contextlib.suppress(Exception):
                t.cancel()
        super().done(r)

    # ── 검증 ──
    def collect(self):
        """(ThicknessFactors, 오류 목록). 틀린 칸은 빨갛게."""
        fac = ThicknessFactors()
        errs: List[str] = []
        for r, c in enumerate(CHUCKS):
            for k, g in enumerate(GUNS):
                it = self.tool_table.item(r, k)
                txt = it.text().strip() if it is not None else ""
                v, err = check_tooling(txt)
                if it is not None:
                    it.setBackground(_BAD_BG if err else QColor(Qt.GlobalColor.white))
                if err:
                    errs.append(f"tooling {_CHUCK_TEXT[c]}/{g}: {err}")
                fac.tooling[c][g] = v
        for g in GUNS:
            m, err = self._chosen(g)
            self.combos[g].setStyleSheet("QComboBox {background: #ffcdd2;}" if err else "")
            if err:
                errs.append(err)
            fac.material[g] = m.name if m is not None else None
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
