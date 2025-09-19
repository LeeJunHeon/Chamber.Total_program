# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Sequence, List, Tuple
from PySide6.QtCore import Qt, QMargins, QPointF, QRect
from PySide6.QtGui import QFont, QPen, QPainter
from PySide6.QtWidgets import QWidget, QVBoxLayout, QSizePolicy
from PySide6.QtCharts import (
    QChart, QChartView, QLineSeries, QScatterSeries,
    QValueAxis, QLogValueAxis
)

# ===== 통일 설정 (여기만 바꾸면 전체가 같이 바뀜) =====
LABEL_FONT_PT = 9     # 축 눈금/제목 모두 동일 포인트 크기
# LABEL_FONT_FAMILY = "Malgun Gothic"  # 특정 폰트를 강제하고 싶으면 주석 해제

def make_font() -> QFont:
    f = QFont()
    f.setPointSize(LABEL_FONT_PT)
    # if LABEL_FONT_FAMILY: f.setFamily(LABEL_FONT_FAMILY)
    return f


# ─────────────────────────────────────────────────────────────
# plotArea를 컨테이너에 '딱' 맞추되, 축/라벨이 들어갈 외곽 여백은 충분히 남김
# ─────────────────────────────────────────────────────────────
class TightChartView(QChartView):
    def __init__(self, chart: QChart, *, left=64, top=12, right=28, bottom=48, parent=None):
        super().__init__(chart, parent)
        self._margins = (left, top, right, bottom)
        self.setRenderHint(QPainter.Antialiasing, True)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        l, t, r, b = self._margins
        rect: QRect = self.viewport().rect().adjusted(l, t, -r, -b)
        if rect.width() > 50 and rect.height() > 50:
            self.chart().setPlotArea(rect)


class GraphController:
    """QtCharts 버전 컨트롤러 (RGA=로그축 Stem+Marker, OES=라인)"""

    # 기본 범위
    RGA_X_RANGE: Tuple[float, float] = (0.0, 65.0)
    RGA_Y_RANGE_TORR: Tuple[float, float] = (1e-11, 1e-5)

    OES_X_RANGE: Tuple[float, float] = (100.0, 1200.0)
    OES_Y_RANGE: Tuple[float, float] = (0.0, 16000.0)

    def __init__(self, rga_widget: QWidget, oes_widget: QWidget):
        tick_font = make_font()
        title_font = make_font()   # 요청: 축 제목/눈금 동일 폰트·크기

        # ========== RGA ==========
        self.rga_chart = QChart()
        self.rga_chart.legend().hide()
        self.rga_chart.setBackgroundRoundness(0)
        self.rga_chart.setMargins(QMargins(6, 6, 6, 6))

        self.rga_axis_x = QValueAxis()
        self.rga_axis_x.setRange(*self.RGA_X_RANGE)
        self.rga_axis_x.setTickCount(14)
        self.rga_axis_x.setLabelFormat("%d")
        self._style_value_axis(self.rga_axis_x, "Atomic Mass", tick_font, title_font)

        self.rga_axis_y = QLogValueAxis()
        self.rga_axis_y.setBase(10.0)
        self.rga_axis_y.setRange(*self.RGA_Y_RANGE_TORR)
        self.rga_axis_y.setLabelFormat("%.0e")
        self.rga_axis_y.setMinorTickCount(0)
        self.rga_axis_y.setMinorGridLineVisible(False)
        self._style_log_axis(self.rga_axis_y, "Partial Pressure", tick_font, title_font)

        self.rga_scatter = QScatterSeries()
        self.rga_scatter.setMarkerSize(6.0)
        self.rga_scatter.setColor(Qt.red)
        self.rga_scatter.setBorderColor(Qt.red)

        self._rga_stem_series: List[QLineSeries] = []

        self.rga_chart.addAxis(self.rga_axis_x, Qt.AlignmentFlag.AlignBottom)
        self.rga_chart.addAxis(self.rga_axis_y, Qt.AlignmentFlag.AlignLeft)
        self.rga_chart.addSeries(self.rga_scatter)
        self.rga_scatter.attachAxis(self.rga_axis_x)
        self.rga_scatter.attachAxis(self.rga_axis_y)

        self._apply_axis_pens(self.rga_axis_x)
        self._apply_axis_pens(self.rga_axis_y)

        # 살짝 더 작은 플롯 영역(요청) → 마진을 이전보다 조금 키웠음
        self.rga_view = TightChartView(self.rga_chart, left=75, top=30, right=28, bottom=60)
        self._mount_chart_view(rga_widget, self.rga_view)

        # ========== OES ==========
        self.oes_chart = QChart()
        self.oes_chart.legend().hide()
        self.oes_chart.setBackgroundRoundness(0)
        self.oes_chart.setMargins(QMargins(6, 6, 6, 6))

        self.oes_axis_x = QValueAxis()
        self._setup_oes_x_axis_base(self.oes_axis_x)  # 100 단위 고정
        self._style_value_axis(self.oes_axis_x, "Wavelength (nm)", tick_font, title_font)  # ← y축이랑 동일 스타일 적용

        self.oes_axis_y = QValueAxis()
        self.oes_axis_y.setRange(*self.OES_Y_RANGE)
        self.oes_axis_y.setTickCount(9)
        self.oes_axis_y.setLabelFormat("%d")
        self.oes_axis_y.setMinorTickCount(0)
        self.oes_axis_y.setMinorGridLineVisible(False)
        self._style_value_axis(self.oes_axis_y, "Intensity", tick_font, title_font)

        self.oes_series = QLineSeries()
        pen = QPen(Qt.red); pen.setWidth(1)
        self.oes_series.setPen(pen)

        self.oes_chart.addAxis(self.oes_axis_x, Qt.AlignmentFlag.AlignBottom)
        self.oes_chart.addAxis(self.oes_axis_y, Qt.AlignmentFlag.AlignLeft)
        self.oes_chart.addSeries(self.oes_series)
        self.oes_series.attachAxis(self.oes_axis_x)
        self.oes_series.attachAxis(self.oes_axis_y)

        self._apply_axis_pens(self.oes_axis_x)
        self._apply_axis_pens(self.oes_axis_y)

        # OES도 플롯 영역을 "조금" 줄이기 → bottom/left 마진 상향
        self.oes_view = TightChartView(self.oes_chart, left=75, top=30, right=28, bottom=60)
        self._mount_chart_view(oes_widget, self.oes_view)

    # ─────────────────────── public API ───────────────────────
    def update_rga_plot(self, x_data: Sequence[float], y_torr: Sequence[float]) -> None:
        for s in self._rga_stem_series:
            self.rga_chart.removeSeries(s)
        self._rga_stem_series.clear()
        self.rga_scatter.clear()

        if not x_data or not y_torr:
            return

        pts: List[Tuple[float, float]] = []
        for x, y in zip(x_data, y_torr):
            try:
                xf = float(x); yf = float(y)
            except Exception:
                continue
            if xf < self.RGA_X_RANGE[0] or xf > self.RGA_X_RANGE[1]:
                continue
            if yf <= 0.0:
                continue
            pts.append((xf, yf))

        if not pts:
            return

        ymin = self.RGA_Y_RANGE_TORR[0]
        stem_pen = QPen(Qt.red); stem_pen.setWidth(1)

        for xf, yf in pts:
            self.rga_scatter.append(QPointF(xf, yf))
            line = QLineSeries()
            line.setPen(stem_pen)
            line.append(xf, ymin)
            line.append(xf, yf)
            self.rga_chart.addSeries(line)
            line.attachAxis(self.rga_axis_x)
            line.attachAxis(self.rga_axis_y)
            self._rga_stem_series.append(line)

        self.rga_axis_x.setRange(*self.RGA_X_RANGE)
        self.rga_axis_y.setRange(*self.RGA_Y_RANGE_TORR)

    def update_oes_plot(self, x_data: Sequence[float], y_data: Sequence[float]) -> None:
        """OES: 선 그래프 (x는 100~1200 범위로 클리핑, x축 눈금 100 단위 고정)"""
        self.oes_series.clear()
        if not x_data or not y_data:
            return

        xmin, xmax = self.OES_X_RANGE
        pairs: List[Tuple[float, float]] = []
        for x, y in zip(x_data, y_data):
            try:
                xf = float(x); yf = float(y)
            except Exception:
                continue
            if xf < xmin or xf > xmax:
                continue
            pairs.append((xf, yf))

        if len(pairs) < 2:
            return
        pairs.sort(key=lambda p: p[0])

        for x, y in pairs:
            self.oes_series.append(QPointF(x, y))

        # x축: 항상 100 간격 고정
        self._setup_oes_x_axis_base(self.oes_axis_x)
        # y축: 기본 범위 유지
        self.oes_axis_y.setRange(*self.OES_Y_RANGE)

    def clear_rga_plot(self) -> None:
        self.rga_scatter.clear()
        for s in self._rga_stem_series:
            self.rga_chart.removeSeries(s)
        self._rga_stem_series.clear()
        self.rga_axis_x.setRange(*self.RGA_X_RANGE)
        self.rga_axis_y.setRange(*self.RGA_Y_RANGE_TORR)

    def clear_oes_plot(self) -> None:
        self.oes_series.clear()
        self._setup_oes_x_axis_base(self.oes_axis_x)
        self.oes_axis_y.setRange(*self.OES_Y_RANGE)

    def reset(self) -> None:
        self.clear_rga_plot()
        self.clear_oes_plot()

    # ─────────────────────── helpers ───────────────────────
    def _setup_oes_x_axis_base(self, axis: QValueAxis,) -> None:
        axis.setRange(*self.OES_X_RANGE)
        # 100, 200, ..., 1200 고정
        axis.setTickType(QValueAxis.TickType.TicksDynamic)
        axis.setTickAnchor(100.0)
        axis.setTickInterval(100.0)
        axis.setLabelFormat("%d")
        axis.setMinorTickCount(0)
        axis.setMinorGridLineVisible(False)
        self._apply_axis_fonts(axis)

    def _mount_chart_view(self, container: QWidget, view: QChartView) -> None:
        layout = container.layout()
        if layout is None:
            layout = QVBoxLayout(container)
            container.setLayout(layout)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        if layout.indexOf(view) == -1:
            layout.addWidget(view)
        container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        container.setMinimumSize(120, 100)

    def _apply_axis_fonts(self, axis) -> None:
        f = make_font()
        axis.setTitleFont(f)
        axis.setLabelsFont(f)

    def _style_value_axis(self, axis: QValueAxis, title: str, tick_font: QFont, title_font: QFont) -> None:
        axis.setTitleText(title)
        axis.setLabelsVisible(True)
        axis.setGridLineVisible(True)
        axis.setMinorGridLineVisible(False)
        axis.setTitleFont(title_font)
        axis.setLabelsFont(tick_font)

    def _style_log_axis(self, axis: QLogValueAxis, title: str, tick_font: QFont, title_font: QFont) -> None:
        axis.setTitleText(title)
        axis.setLabelsVisible(True)
        axis.setGridLineVisible(True)
        axis.setMinorGridLineVisible(False)
        axis.setTitleFont(title_font)
        axis.setLabelsFont(tick_font)

    def _apply_axis_pens(self, axis) -> None:
        axis.setLinePen(QPen(Qt.black, 1))
        axis.setGridLinePen(QPen(Qt.gray, 1))
        axis.setMinorGridLinePen(QPen(Qt.lightGray, 1))
