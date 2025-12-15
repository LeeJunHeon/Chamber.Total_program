from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QMessageBox

def show_autoclose_popup(parent, title: str, text: str,
                         timeout_ms: int = 5000,
                         icon=QMessageBox.Warning) -> QMessageBox:
    """
    - 5초(기본) 뒤 자동으로 닫히는 비블로킹 팝업
    - exec() 사용 금지 (UI 멈춤/팝업 고정 방지)
    """
    box = QMessageBox(parent)
    box.setWindowTitle(title)
    box.setText(text)
    box.setIcon(icon)

    # 사용자가 바로 닫을 수도 있게 OK는 유지
    box.setStandardButtons(QMessageBox.Ok)

    # 비모달/비블로킹
    box.setModal(False)
    box.show()

    # 5초 후 자동 종료
    QTimer.singleShot(timeout_ms, box.close)

    # GC 방지: parent에 참조 보관(중요)
    if parent is not None:
        if not hasattr(parent, "_active_autoclose_popups"):
            parent._active_autoclose_popups = []
        parent._active_autoclose_popups.append(box)
        box.finished.connect(lambda _: parent._active_autoclose_popups.remove(box))

    return box
