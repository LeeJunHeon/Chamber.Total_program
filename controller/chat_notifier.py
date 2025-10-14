# controller/chat_notifier.py
from PySide6.QtCore import QObject, Slot
import asyncio
import json
import ssl
import urllib.request
from typing import Optional, List, Dict, Any, Set, Tuple

# ── 채널별 웹훅은 여기서 읽음 ────────────────────────────────────────────────
try:
    import lib.config_local  # config_local.py에 CH1_CHAT_WEBHOOK_URL / CH2_CHAT_WEBHOOK_URL 정의
except Exception:
    config_local = None


class ChatNotifier(QObject):
    """
    Google Chat 웹훅 알림 (asyncio 버전)
    - CH1/CH2를 구분해 서로 다른 Webhook URL로 라우팅
    - QThread 제거. 모든 HTTP는 asyncio.to_thread로 오프로딩(메인/GUI 프리징 없음)
    - Qt 신호와 자연스럽게 연결할 수 있도록 QObject/Slot 유지
    - main.py의 기존 인터페이스(start/shutdown/notify_*) 호환
    """

    def __init__(self, webhook_url: Optional[str], parent=None):
        super().__init__(parent)

        # 기본(폴백) 웹훅
        self.webhook_default = (webhook_url or "").strip()

        # 채널별 웹훅 (config_local 우선)
        self.webhook_ch1 = ""
        self.webhook_ch2 = ""
        if config_local is not None:
            self.webhook_ch1 = getattr(config_local, "CH1_CHAT_WEBHOOK_URL", "") or ""
            self.webhook_ch2 = getattr(config_local, "CH2_CHAT_WEBHOOK_URL", "") or ""
        self.webhook_ch1 = self.webhook_ch1.strip()
        self.webhook_ch2 = self.webhook_ch2.strip()

        # 지연 전송 & 버퍼 (payload, webhook_url) 튜플로 저장
        self._defer: bool = True
        self._buffer: List[Tuple[dict, Optional[str]]] = []

        # 실행 컨텍스트
        self._last_started_params: Optional[dict] = None
        self._last_started_ch: Optional[int] = None  # 1/2/None
        self._errors: List[str] = []          # 누적 오류(집계용)
        self._error_seen: Set[str] = set()    # 종료 리포트 중복 방지
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
        # 오류 집계 카드를 최신 상태로 반영해둔다
        self._upsert_error_card()
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

    # ---------- 유틸/라우팅 ----------
    def _num(self, v, default=None):
        try:
            if v is None:
                return default
            return float(v)
        except Exception:
            try:
                import re
                m = re.search(r'[-+]?\d+(\.\d+)?', str(v))
                return float(m.group(0)) if m else default
            except Exception:
                return default

    def _which_chamber(self, params: Optional[dict]) -> Optional[int]:
        p = params or {}
        for k in ("ch", "channel", "chamber", "ui_ch", "ui_channel"):
            if k in p:
                v = str(p[k]).strip().upper()
                if v in ("1", "CH1"):
                    return 1
                if v in ("2", "CH2"):
                    return 2
        keys = [str(k).lower() for k in p.keys()]
        if any(k.startswith("ch1_") for k in keys):
            return 1
        if any(k.startswith("ch2_") for k in keys):
            return 2
        return None

    def _resolve_webhook(self, params_for_routing: Optional[dict]) -> Optional[str]:
        """메시지 보낼 웹훅 URL 결정."""
        ch = self._which_chamber(params_for_routing)
        if ch is None:
            ch = self._last_started_ch
        if ch == 1 and self.webhook_ch1:
            return self.webhook_ch1
        if ch == 2 and self.webhook_ch2:
            return self.webhook_ch2
        # 폴백: 기본→CH1→CH2 순
        return self.webhook_default or self.webhook_ch1 or self.webhook_ch2 or None

    # ---------- 내부 전송 ----------
    async def _post_async(self, payload: dict, webhook_url: Optional[str]):
        if not webhook_url or not payload:
            return
        data = json.dumps(payload).encode("utf-8")

        def _blocking_post():
            req = urllib.request.Request(
                webhook_url, data=data, headers={"Content-Type": "application/json"}
            )
            try:
                with urllib.request.urlopen(req, timeout=3, context=self._ctx) as resp:
                    resp.read()
            except Exception:
                # 실패는 조용히 무시(원하면 여기서 파일로그 등 추가)
                pass

        # 표준 라이브러리만 사용 → 블로킹 I/O는 스레드풀에 위임
        await asyncio.to_thread(_blocking_post)

    def _schedule_post(self, payload: dict, webhook_url: Optional[str]):
        """비동기 전송 태스크를 스케줄하고 참조를 보관(가비지 방지)."""
        if not webhook_url:
            return  # 보낼 곳이 없으면 무시
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 아직 루프가 없으면 버퍼에 보관
            self._buffer.append((payload, webhook_url))
            return

        task = loop.create_task(self._post_async(payload, webhook_url))
        self._pending.add(task)
        task.add_done_callback(lambda t: self._pending.discard(t))

    # ---------- 지연/버퍼 ----------
    def set_defer(self, on: bool):
        self._defer = bool(on)

    def flush(self):
        """버퍼에 쌓인 메시지를 모두 비동기 전송으로 스케줄(비차단)."""
        if self._errors:
            # flush 직전에 집계 카드 1장으로 정리
            self._upsert_error_card()

        if not self._buffer:
            return

        pending = self._buffer[:]
        self._buffer.clear()
        for pl, wh in pending:
            self._schedule_post(pl, wh)

    def _post_json(self, payload: dict, urgent: bool = False, route_params: Optional[dict] = None):
        webhook = self._resolve_webhook(route_params)
        if self._defer and not urgent:
            self._buffer.append((payload, webhook))
            return
        self._schedule_post(payload, webhook)

    # ---------- 텍스트/카드 ----------
    def _post_text(self, text: str, urgent: bool = False, route_params: Optional[dict] = None):
        if text:
            self._post_json({"text": text}, urgent=urgent, route_params=route_params)

    def _post_card(self, title: str, subtitle: str = "", status: str = "INFO",
                   fields: Optional[Dict[str, Any]] = None, urgent: bool = False,
                   route_params: Optional[dict] = None):
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
        self._post_json(payload, urgent=urgent, route_params=route_params)

    # ----- 오류 집계 카드 전용 -----
    def _remove_error_card_from_buffer(self):
        """버퍼에서 기존 오류 집계 카드 제거."""
        kept: List[Tuple[dict, Optional[str]]] = []
        for pl, wh in self._buffer:
            try:
                cid = pl["cardsV2"][0].get("cardId")
            except Exception:
                cid = None
            if cid != "error-aggregate":
                kept.append((pl, wh))
        self._buffer = kept

    def _upsert_error_card(self):
        """누적된 self._errors를 한 장의 '장비 오류' 카드로 버퍼에 유지."""
        # 기존 집계 카드는 제거
        self._remove_error_card_from_buffer()
        if not self._errors:
            return

        bullets = "<br>".join(f"• {e}" for e in self._errors)
        widgets = [
            {"textParagraph": {"text": "<b>❌ 장비 오류</b>"}},
            {"textParagraph": {"text": f"{bullets}"}}
        ]
        payload = {
            "cardsV2": [
                {
                    "cardId": "error-aggregate",   # ← 버퍼에서 식별/치환용
                    "card": {
                        "header": {"title": "Sputter Controller", "subtitle": "Status Notification"},
                        "sections": [{"widgets": widgets}],
                    },
                }
            ]
        }
        webhook = self._resolve_webhook(self._last_started_params)
        if self._defer:
            self._buffer.append((payload, webhook))
        else:
            self._schedule_post(payload, webhook)

    # ---------- 포맷 헬퍼 ----------
    def _b(self, params: dict, key: str) -> bool:
        v = params.get(key, False)
        if isinstance(v, str):
            v = v.strip().lower()
            return v in ("1", "t", "true", "y", "yes", "on", "checked")
        return bool(v)

    def _fmt_min(self, v) -> str:
        try:
            f = float(v)
        except Exception:
            return "—"
        if f <= 0:
            return "—"
        return f"{int(f)}분" if abs(f - int(f)) < 1e-6 else f"{f:.1f}분"

    # --- CH1/CH2 분기 전용 포맷터 (간결 버전) ---
    def _guns_and_targets_ch1(self, p: dict) -> str:
        name = (p.get("ch1_gunTarget_name") or p.get("gunTarget_name") or
                p.get("target_name") or p.get("gun_name") or "").strip()
        return f"G1: {name}" if name else "—"

    def _power_summary_ch1(self, p: dict) -> str:
        pw = (p.get("ch1_dcPulsePower") or p.get("ch1_dcPulsePower_edit") or
              p.get("dcPulsePower")      or p.get("dc_pulse_power"))
        fq = (p.get("ch1_dcPulseFreq") or p.get("ch1_dcPulseFreq_edit") or
              p.get("dcPulseFreq")      or p.get("dc_pulse_freq"))
        dy = (p.get("ch1_dcPulseDutyCycle") or p.get("ch1_dcPulseDutyCycle_edit") or
              p.get("dcPulseDuty")          or p.get("dc_pulse_duty"))
        pwf = self._num(pw); fqf = self._num(fq); dyf = self._num(dy)
        if pwf is not None or fqf is not None or dyf is not None:
            pw_txt = f"{int(pwf)} W" if pwf is not None else "—"
            fq_txt = f"{int(fqf)} Hz" if fqf is not None else "—"
            dy_txt = f"{int(dyf)} %" if dyf is not None else "—"
            return f"DC Pulse {pw_txt} @ {fq_txt}, {dy_txt}"
        dc = p.get("ch1_Power") or p.get("ch1_Power_edit") or p.get("dc_power")
        dcf = self._num(dc)
        if dcf is not None:
            return f"DC {int(dcf)} W"
        return "—"

    def _guns_and_targets_ch2(self, p: dict) -> str:
        out = []
        for gun, ck, nk in (("G1","ch2_G1_checkbox","ch2_g1Target_name"),
                            ("G2","ch2_G2_checkbox","ch2_g2Target_name"),
                            ("G3","ch2_G3_checkbox","ch2_g3Target_name")):
            if self._b(p, ck) or (p.get(nk) not in (None, "")):
                name = (p.get(nk) or "").strip() or "-"
                out.append(f"{gun}: {name}")
        return ", ".join(out) if out else "—"

    def _power_summary_ch2(self, p: dict) -> str:
        items = []
        use_dc = self._b(p, "ch2_dcPower_checkbox")
        dc = p.get("ch2_dcPower_edit")
        dcf = self._num(dc)
        if use_dc or dcf is not None:
            items.append(f"DC {int(dcf)} W" if dcf is not None else "DC — W")

        use_rfp = self._b(p, "ch2_rfPulsePower_checkbox")
        rfpw = p.get("ch2_rfPulsePower_edit")
        rffq = p.get("ch2_rfPulseFreq_edit")
        rfdt = p.get("ch2_rfPulseDutyCycle_edit")
        pwf = self._num(rfpw); fqf = self._num(rffq); dyf = self._num(rfdt)
        if use_rfp or (pwf is not None or fqf is not None or dyf is not None):
            pw_txt = f"{int(pwf)} W" if pwf is not None else "—"
            fq_txt = f"{int(fqf)} Hz" if fqf is not None else "keep"
            dy_txt = f"{int(dyf)} %" if dyf is not None else "keep"
            items.append(f"RF Pulse {pw_txt} @ {fq_txt}, {dy_txt}")

        return " / ".join(items) if items else "—"

    # ========== Qt 슬롯들 ==========
    @Slot(dict)
    def notify_process_started(self, params: dict):
        # 실행 컨텍스트 초기화
        self._last_started_params = dict(params) if params else None
        self._last_started_ch = self._which_chamber(params)
        self._errors.clear()
        self._error_seen.clear()
        self._finished_sent = False
        self._buffer.clear()  # 이전 집계 카드 등 모두 제거

        name = (params or {}).get("process_note") or (params or {}).get("Process_name") or "Untitled"

        # CH 분기 포맷
        if self._last_started_ch == 1:
            guns = self._guns_and_targets_ch1(params or {})
            pwr  = self._power_summary_ch1(params or {})
        elif self._last_started_ch == 2:
            guns = self._guns_and_targets_ch2(params or {})
            pwr  = self._power_summary_ch2(params or {})
        else:
            # 못찾으면 CH2 규격 먼저 시도, 없으면 CH1 폴백
            guns = self._guns_and_targets_ch2(params or {}) or self._guns_and_targets_ch1(params or {})
            pwr  = self._power_summary_ch2(params or {}) or self._power_summary_ch1(params or {})

        sh_delay = self._fmt_min((params or {}).get("shutter_delay", 0))
        proc_time = self._fmt_min((params or {}).get("process_time", 0))

        self._post_card(
            title="공정 시작",
            subtitle=name,
            status="INFO",
            fields={
                "사용 Guns / 타겟": guns if guns else "—",
                "파워": pwr if pwr else "—",
                "Shutter Delay": sh_delay,
                "Process Time": proc_time,
            },
            urgent=True,                # 시작 알림은 즉시
            route_params=params         # 🔸 라우팅: 이 공정의 챔버로 보냄
        )

    @Slot(bool, dict)
    def notify_process_finished_detail(self, ok: bool, detail: dict):
        # detail에 ch 힌트가 있으면 갱신
        ch = self._which_chamber(detail)
        if ch is not None:
            self._last_started_ch = ch

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

        self._post_card("공정 종료", subtitle, status, fields, route_params=detail)

        # 종료 카드와 함께 누적 오류 집계 카드 1장도 같이 나가도록 반영
        self._upsert_error_card()
        self.flush()
        self._finished_sent = True
        self._errors.clear()
        self._error_seen.clear()

    @Slot(bool)
    def notify_process_finished(self, ok: bool):
        # 상세 카드가 이미 나갔다면 중복 방지
        if self._finished_sent:
            self._upsert_error_card()
            self.flush()
            return
        self._post_card(
            "공정 종료", "성공" if ok else "실패",
            "SUCCESS" if ok else "FAIL",
            fields={"공정 이름": (self._last_started_params or {}).get("process_note", "Untitled")},
            route_params=self._last_started_params
        )
        self._upsert_error_card()
        self.flush()
        self._finished_sent = True
        self._errors.clear()
        self._error_seen.clear()

    @Slot(str)
    def notify_text(self, text: str):
        # 최근 시작된 챔버 기준으로 라우팅
        self._post_text(text, route_params=self._last_started_params)

    @Slot(str)
    def notify_error(self, reason: str):
        """개별 카드 생성 X → 내부 누적만, 집계 카드 1장으로 관리"""
        pretty = (reason or "unknown").strip()
        self._errors.append(pretty)
        self._error_seen.add(pretty)
        # 버퍼에 집계 카드 1장을 업데이트(치환)만 해둔다
        self._upsert_error_card()

    @Slot(str, str)
    def notify_error_with_src(self, src: str, reason: str):
        """개별 카드 생성 X → 내부 누적만, 집계 카드 1장으로 관리"""
        base = (reason or "unknown").strip()
        pretty = f"[{src}] {base}" if src else base
        self._errors.append(pretty)
        self._error_seen.add(pretty)
        self._upsert_error_card()
