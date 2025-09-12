# controller/chat_notifier.py
from PySide6.QtCore import QObject, Slot
import asyncio
import json
import ssl
import urllib.request
from typing import Optional, List, Dict, Any


class ChatNotifier(QObject):
    """
    Google Chat 웹훅 알림 (asyncio 버전)
    - QThread 제거. 모든 HTTP는 asyncio.to_thread로 오프로딩(메인/GUI 프리징 없음)
    - Qt 신호와 자연스럽게 연결할 수 있도록 QObject/Slot 유지
    - main.py의 기존 인터페이스(start/shutdown/notify_*) 호환
    """

    def __init__(self, webhook_url: Optional[str], parent=None):
        super().__init__(parent)
        self.webhook_url = (webhook_url or "").strip()

        # 지연 전송 & 버퍼
        self._defer: bool = True
        self._buffer: List[dict] = []

        # 실행 컨텍스트
        self._last_started_params: Optional[dict] = None
        self._errors: List[str] = []
        self._finished_sent: bool = False

        # 내부 상태
        self._ctx = ssl.create_default_context()
        self._pending: set[asyncio.Task] = set()
        self._started: bool = False

    # ---------- 라이프사이클 ----------
    def start(self):
        """호환용(스레드 없음)."""
        self._started = True

    def shutdown(self):
        """앱 종료 직전 호출. 버퍼를 스케줄하고 남은 태스크는 이벤트 루프가 처리."""
        self.flush()
        try:
            loop = asyncio.get_running_loop()
            if self._pending:
                async def _drain():
                    # 너무 오래 붙잡지 않도록 타임아웃
                    await asyncio.wait(self._pending, timeout=2.0)
                loop.create_task(_drain())
        except RuntimeError:
            # 루프가 이미 내려간 상황이면 그냥 무시
            pass

    # ---------- 내부 전송 ----------
    async def _post_async(self, payload: dict):
        if not self.webhook_url or not payload:
            return
        data = json.dumps(payload).encode("utf-8")

        def _blocking_post():
            req = urllib.request.Request(
                self.webhook_url, data=data, headers={"Content-Type": "application/json"}
            )
            try:
                with urllib.request.urlopen(req, timeout=3, context=self._ctx) as resp:
                    resp.read()
            except Exception:
                # 실패는 조용히 무시(원하면 여기서 파일로그 등 추가)
                pass

        # 표준 라이브러리만 사용 → 블로킹 I/O는 스레드풀에 위임
        await asyncio.to_thread(_blocking_post)

    def _schedule_post(self, payload: dict):
        """비동기 전송 태스크를 스케줄하고 참조를 보관(가비지 방지)."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 아직 루프가 없으면 버퍼에 보관
            self._buffer.append(payload)
            return

        task = loop.create_task(self._post_async(payload))
        self._pending.add(task)
        task.add_done_callback(lambda t: self._pending.discard(t))

    # ---------- 지연/버퍼 ----------
    def set_defer(self, on: bool):
        self._defer = bool(on)

    def flush(self):
        """버퍼에 쌓인 메시지를 모두 비동기 전송으로 스케줄(비차단)."""
        if not self._buffer:
            return
        pending = self._buffer[:]
        self._buffer.clear()
        for pl in pending:
            self._schedule_post(pl)

    def _post_json(self, payload: dict, urgent: bool = False):
        if self._defer and not urgent:
            self._buffer.append(payload)
            return
        self._schedule_post(payload)

    # ---------- 텍스트/카드 ----------
    def _post_text(self, text: str, urgent: bool = False):
        if text:
            self._post_json({"text": text}, urgent=urgent)

    def _post_card(self, title: str, subtitle: str = "", status: str = "INFO",
                   fields: Optional[Dict[str, Any]] = None, urgent: bool = False):
        icon = {"INFO": "ℹ️", "SUCCESS": "✅", "FAIL": "❌"}.get(status, "ℹ️")
        widgets = [{"textParagraph": {"text": f"<b>{icon} {title}</b>"}}]
        if subtitle:
            widgets.append({"textParagraph": {"text": subtitle}})
        if fields:
            for k, v in fields.items():
                widgets.append({"decoratedText": {"topLabel": str(k), "text": str(v)}})

        payload = {
            "cardsV2": [
                {
                    "cardId": "notify-card",
                    "card": {
                        "header": {"title": "Sputter Controller", "subtitle": "Status Notification"},
                        "sections": [{"widgets": widgets}],
                    },
                }
            ]
        }
        self._post_json(payload, urgent=urgent)

    # ---------- 포맷 헬퍼 ----------
    def _b(self, params: dict, key: str) -> bool:
        v = params.get(key, False)
        if isinstance(v, str):
            v = v.strip().lower()
            return v in ("1", "t", "true", "y", "yes")
        return bool(v)

    def _fmt_min(self, v) -> str:
        try:
            f = float(v)
        except Exception:
            return "—"
        if f <= 0:
            return "—"
        return f"{int(f)}분" if abs(f - int(f)) < 1e-6 else f"{f:.1f}분"

    def _guns_and_targets(self, p: dict) -> str:
        out = []
        for gun, use_key, name_key in (("G1", "use_g1", "G1_target_name"),
                                       ("G2", "use_g2", "G2_target_name"),
                                       ("G3", "use_g3", "G3_target_name")):
            if self._b(p, use_key):
                name = (p.get(name_key) or "").strip() or "-"
                out.append(f"{gun}: {name}")
        return ", ".join(out) if out else "—"

    def _power_summary(self, p: dict) -> str:
        items = []
        if self._b(p, "use_dc_power"):
            items.append(f"DC {p.get('dc_power', 0)} W")
        if self._b(p, "use_rf_power"):
            items.append(f"RF {p.get('rf_power', 0)} W")
        if self._b(p, "use_rf_pulse"):
            f = p.get("rf_pulse_freq")
            d = p.get("rf_pulse_duty")
            freq_txt = f"{int(f)} Hz" if isinstance(f, (int, float)) and f is not None else "keep"
            duty_txt = f"{int(d)} %" if isinstance(d, (int, float)) and d is not None else "keep"
            items.append(f"RF Pulse {p.get('rf_pulse_power', 0)} W @ {freq_txt}, {duty_txt}")
        return " / ".join(items) if items else "—"

    # ========== Qt 슬롯들 ==========
    @Slot(dict)
    def notify_process_started(self, params: dict):
        # 실행 컨텍스트 초기화
        self._last_started_params = dict(params) if params else None
        self._errors.clear()
        self._finished_sent = False
        self._buffer.clear()

        name = (params or {}).get("process_note") or (params or {}).get("Process_name") or "Untitled"
        guns = self._guns_and_targets(params or {})
        pwr  = self._power_summary(params or {})
        sh_delay = self._fmt_min((params or {}).get("shutter_delay", 0))
        proc_time = self._fmt_min((params or {}).get("process_time", 0))

        self._post_card(
            title="공정 시작",
            subtitle=name,
            status="INFO",
            fields={
                "사용 Guns / 타겟": guns,
                "파워": pwr,
                "Shutter Delay": sh_delay,
                "Process Time": proc_time,
            },
            urgent=True  # 시작 알림은 즉시
        )

    @Slot(bool, dict)
    def notify_process_finished_detail(self, ok: bool, detail: dict):
        name = (detail or {}).get("process_name") or (self._last_started_params or {}).get("process_note") or "Untitled"
        stopped  = bool((detail or {}).get("stopped"))
        aborting = bool((detail or {}).get("aborting"))
        errs: List[str] = list((detail or {}).get("errors") or [])
        if not errs and self._errors:
            errs = list(self._errors)

        if ok:
            subtitle = "정상 종료"
            status = "SUCCESS"
            fields = {"공정 이름": name}
        else:
            status = "FAIL"
            if stopped:
                subtitle = "사용자 Stop으로 종료"
                fields = {"공정 이름": name}
            elif aborting:
                subtitle = "긴급 중단으로 종료"
                fields = {"공정 이름": name}
            else:
                subtitle = "오류로 종료"
                if errs:
                    preview = " • " + "\n • ".join(errs[:3])
                    if len(errs) > 3:
                        preview += f"\n(+{len(errs)-3}건 더)"
                    fields = {"공정 이름": name, "원인": preview}
                else:
                    fields = {"공정 이름": name, "원인": "알 수 없음"}

        self._post_card("공정 종료", subtitle, status, fields)
        self.flush()
        self._finished_sent = True
        self._errors.clear()

    @Slot(bool)
    def notify_process_finished(self, ok: bool):
        # 상세 카드가 이미 나갔다면 중복 방지
        if self._finished_sent:
            self.flush()
            return
        self._post_card(
            "공정 종료", "성공" if ok else "실패",
            "SUCCESS" if ok else "FAIL",
            fields={"공정 이름": (self._last_started_params or {}).get("process_note", "Untitled")}
        )
        self.flush()
        self._finished_sent = True

    @Slot(str)
    def notify_text(self, text: str):
        self._post_text(text)

    @Slot(str)
    def notify_error(self, reason: str):
        pretty = reason or "unknown"
        self._errors.append(pretty)
        self._post_card("장비 오류", pretty, "FAIL", urgent=False)

    @Slot(str, str)
    def notify_error_with_src(self, src: str, reason: str):
        pretty = f"[{src}] {reason}" if src else (reason or "unknown")
        self._errors.append(pretty)
        self._post_card("장비 오류", pretty, "FAIL", urgent=False)
