# ui/config_dialog.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import contextlib
from typing import Any, Dict, Tuple

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QTabWidget,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QPushButton,
    QMessageBox,
    QLabel,
    QAbstractItemView,
    QHeaderView,
)

from lib import user_config


def _json_one_line(v: Any) -> str:
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return "" if v is None else str(v)


def _parse_value(text: str, original: Any) -> Tuple[bool, Any, str]:
    s = (text or "").strip()

    # ✅ 빈칸은 "기본값(original)로 되돌리기"로 처리 (입력 편의)
    if s == "":
        return True, original, ""

    if isinstance(original, bool):
        sl = s.lower()
        if sl in ("1", "true", "t", "yes", "y", "on"):
            return True, True, ""
        if sl in ("0", "false", "f", "no", "n", "off"):
            return True, False, ""
        return False, original, "bool 값은 true/false 또는 1/0 형태로 입력하세요."

    if isinstance(original, int) and not isinstance(original, bool):
        try:
            return True, int(s), ""
        except Exception:
            return False, original, "int 값으로 변환 실패"

    if isinstance(original, float):
        try:
            return True, float(s), ""
        except Exception:
            return False, original, "float 값으로 변환 실패"

    if isinstance(original, (dict, list)):
        try:
            return True, json.loads(s), ""
        except Exception:
            return False, original, "dict/list는 JSON 형식(JSON)으로 입력해야 합니다."

    return True, s, ""


class _ParamTable(QTableWidget):
    """
    요구사항 반영:
    - 파란 하이라이트(선택/현재셀/호버/휠 스크롤) 완전 제거
    - 컬럼은 창 폭에 맞춰 꽉 차게(가로 늘어남 방지)
    - 긴 텍스트는 ... (ElideRight)
    - 편집 시에는 전체 텍스트를 보여주되(기본 QLineEdit),
      "칸 안에서" 좌우 스크롤 가능 (라인에디터 기본 기능)
    """
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)

        self.setColumnCount(2)
        self.setHorizontalHeaderLabels(["KEY", "VALUE"])
        self.verticalHeader().setVisible(False)
        self.setAlternatingRowColors(True)
        self.setWordWrap(False)

        # ✅ 편집 트리거
        self.setEditTriggers(
            QAbstractItemView.DoubleClicked
            | QAbstractItemView.EditKeyPressed
            | QAbstractItemView.AnyKeyPressed
        )

        # ✅ 파란 하이라이트 완전 제거 핵심 1) 선택 자체 OFF
        self.setSelectionMode(QAbstractItemView.NoSelection)
        self.setSelectionBehavior(QAbstractItemView.SelectItems)

        # ✅ 파란 하이라이트 완전 제거 핵심 2) 뷰가 포커스를 가지지 않게(휠 스크롤로 current 생성 방지)
        self.setFocusPolicy(Qt.NoFocus)

        # ✅ 긴 텍스트는 ... 로
        self.setTextElideMode(Qt.ElideRight)

        # ✅ 가로폭 고정 정책: KEY는 적당히, VALUE는 나머지 전체 폭
        hh = self.horizontalHeader()
        hh.setDefaultAlignment(Qt.AlignCenter)
        hh.setStretchLastSection(True)
        hh.setSectionResizeMode(0, QHeaderView.Interactive)  # KEY
        hh.setSectionResizeMode(1, QHeaderView.Stretch)      # VALUE
        self.setColumnWidth(0, 260)  # KEY 기본 폭(필요하면 숫자만 조절)

        # ✅ 절대 "내용 길이로 컬럼이 커지지 않게" (가로 스크롤바도 OFF)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        # ✅ selected / current / hover / focus 스타일 제거 (OS 테마가 current를 칠하는 경우까지 커버)
        self.setStyleSheet("""
        QTableWidget::item:selected { background: transparent; color: black; }
        QTableWidget::item:hover { background: transparent; }
        QTableWidget::item:focus { outline: none; }
        QTableWidget { outline: none; }
        """)

        self._base: Dict[str, Any] = {}
        self._tooltips: Dict[str, str] = {}

    # ✅ 휠 스크롤 시 current cell이 생기면서 파랗게 보이는 현상 방지
    def wheelEvent(self, event):
        super().wheelEvent(event)
        if self.state() != QAbstractItemView.EditingState:
            self.clearSelection()
            self.setCurrentCell(-1, -1)

    def set_params(self, base_params: Dict[str, Any], current_params: Dict[str, Any], tooltips: Dict[str, str]) -> None:
        self._base = dict(base_params or {})
        self._tooltips = dict(tooltips or {})

        keys = sorted(self._base.keys())
        self.setRowCount(len(keys))

        for r, k in enumerate(keys):
            base_v = self._base.get(k)
            cur_v = (current_params or {}).get(k, base_v)

            # KEY (읽기 전용)
            it_k = QTableWidgetItem(k)
            it_k.setFlags(it_k.flags() & ~Qt.ItemIsEditable)
            it_k.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)

            tip = self._tooltips.get(k, "")
            if tip:
                tip2 = f"{tip}\n\n기본값: {base_v!r}\n타입: {type(base_v).__name__}"
            else:
                tip2 = f"기본값: {base_v!r}\n타입: {type(base_v).__name__}"
            it_k.setToolTip(tip2)

            # VALUE (편집 가능)
            it_v = QTableWidgetItem(_json_one_line(cur_v))
            it_v.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
            # 툴팁은 “의미 설명” 유지 (요청: 수정할 때만 전체 내용 확인)
            it_v.setToolTip(tip2)

            self.setItem(r, 0, it_k)
            self.setItem(r, 1, it_v)

        # ✅ 여기서 resizeColumnsToContents() 절대 호출 금지
        #    (긴 데이터가 있으면 컬럼이 창보다 커져버림)

        self.clearSelection()
        self.setCurrentCell(-1, -1)

    def collect_full_values(self) -> Tuple[bool, Dict[str, Any], str]:
        out: Dict[str, Any] = {}

        for r in range(self.rowCount()):
            k_item = self.item(r, 0)
            v_item = self.item(r, 1)
            if not k_item:
                continue

            key = (k_item.text() or "").strip()
            if not key:
                continue

            base_v = self._base.get(key)
            text = v_item.text() if v_item else ""

            ok, parsed, err = _parse_value(text, base_v)
            if not ok:
                return False, {}, f"[{key}] {err}"

            out[key] = parsed

        return True, out, ""


class ConfigDialog(QDialog):
    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWindowTitle("Config")
        self.setModal(True)
        self.resize(980, 700)

        self._defaults = user_config.build_defaults()
        self._cfg = user_config.load()
        self._tips = user_config.build_tooltips()

        self._tab = QTabWidget(self)

        self._tbl_comm = _ParamTable()
        self._tbl_tsp = _ParamTable()
        self._tbl_pc = _ParamTable()
        self._tbl_ch1 = _ParamTable()
        self._tbl_ch2 = _ParamTable()

        self._tab.addTab(self._wrap(self._tbl_comm), "Communication")
        self._tab.addTab(self._wrap(self._tbl_tsp), "TSP")
        self._tab.addTab(self._wrap(self._tbl_pc), "Plasma cleaning")
        self._tab.addTab(self._wrap(self._tbl_ch1), "CH1")
        self._tab.addTab(self._wrap(self._tbl_ch2), "CH2")

        self._hint = QLabel(
            "※ Save: config/user_config.json에 저장\n"
            "※ Apply(Runtime): 저장된 변경값 중 현재 적용 가능한 항목만 즉시 적용합니다.\n"
            "   공정 중이거나 현재 상태에서 적용 불가능한 항목은 적용되지 않으며 안내 메시지가 표시됩니다."
        )
        self._hint.setWordWrap(True)

        btn_reload = QPushButton("Reload")
        btn_save = QPushButton("Save")
        btn_apply = QPushButton("Apply (Runtime)")
        btn_close = QPushButton("Close")

        btn_reload.clicked.connect(self._on_reload)
        btn_save.clicked.connect(self._on_save)
        btn_apply.clicked.connect(self._on_apply)
        btn_close.clicked.connect(self.close)

        bottom = QHBoxLayout()
        bottom.addWidget(btn_reload)
        bottom.addStretch(1)
        bottom.addWidget(btn_save)
        bottom.addWidget(btn_apply)
        bottom.addWidget(btn_close)

        lay = QVBoxLayout(self)
        lay.addWidget(self._tab)
        lay.addWidget(self._hint)
        lay.addLayout(bottom)

        self._refresh()

    def _wrap(self, widget: QWidget) -> QWidget:
        w = QWidget()
        v = QVBoxLayout(w)
        v.setContentsMargins(6, 6, 6, 6)
        v.addWidget(widget)
        return w

    def _refresh(self) -> None:
        d0 = self._defaults
        d1 = self._cfg
        tips = self._tips

        self._tbl_comm.set_params(d0.get("communication", {}), d1.get("communication", {}), tips.get("communication", {}))
        self._tbl_tsp.set_params(d0.get("tsp", {}), d1.get("tsp", {}), tips.get("tsp", {}))
        self._tbl_pc.set_params(d0.get("plasma_cleaning", {}), d1.get("plasma_cleaning", {}), tips.get("plasma_cleaning", {}))
        self._tbl_ch1.set_params(d0.get("ch1", {}), d1.get("ch1", {}), tips.get("ch1", {}))
        self._tbl_ch2.set_params(d0.get("ch2", {}), d1.get("ch2", {}), tips.get("ch2", {}))

    def _collect(self) -> Tuple[bool, Dict[str, Any], str]:
        ok, comm, err = self._tbl_comm.collect_full_values()
        if not ok:
            return False, {}, err
        ok, tsp, err = self._tbl_tsp.collect_full_values()
        if not ok:
            return False, {}, err
        ok, pc, err = self._tbl_pc.collect_full_values()
        if not ok:
            return False, {}, err
        ok, ch1, err = self._tbl_ch1.collect_full_values()
        if not ok:
            return False, {}, err
        ok, ch2, err = self._tbl_ch2.collect_full_values()
        if not ok:
            return False, {}, err

        data = {
            "__meta__": dict((self._cfg or {}).get("__meta__", {})),
            "communication": comm,
            "tsp": tsp,
            "plasma_cleaning": pc,
            "ch1": ch1,
            "ch2": ch2,
        }
        return True, data, ""

    def _on_reload(self) -> None:
        self._defaults = user_config.build_defaults()
        self._cfg = user_config.load()
        self._tips = user_config.build_tooltips()
        self._refresh()
        QMessageBox.information(self, "Config", "Reloaded.")

    def _on_save(self) -> None:
        ok, data, err = self._collect()
        if not ok:
            QMessageBox.warning(self, "Config", f"저장 실패:\n{err}")
            return

        ok2, path, msg = user_config.save(data)
        if not ok2:
            QMessageBox.warning(self, "Config", f"저장 실패:\n{msg}")
            return

        self._cfg = user_config.load(path)
        self._refresh()
        QMessageBox.information(self, "Config", f"저장 완료:\n{path}\n{msg}")

    def _on_apply(self) -> None:
        ok, data, err = self._collect()
        if not ok:
            QMessageBox.warning(self, "Config", f"적용 실패:\n{err}")
            return

        ok2, path, msg = user_config.save(data)
        if not ok2:
            QMessageBox.warning(self, "Config", f"저장 실패:\n{msg}")
            return

        merged = user_config.load(path)

        self._cfg = merged
        self._refresh()

        # ✅ 실제 적용 판단은 main.py / ConfigApplyController가 담당
        with contextlib.suppress(Exception):
            p = self.parent()
            if p is not None and hasattr(p, "on_config_applied"):
                p.on_config_applied()