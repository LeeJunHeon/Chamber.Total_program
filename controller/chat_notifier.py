# controller/chat_notifier.py
# -*- coding: utf-8 -*-

from PySide6.QtCore import QObject, Slot
import asyncio
import json
import ssl
import urllib.request
from typing import Optional, List, Dict, Any, Set, Tuple

# ── 단일 웹훅: lib/config_local.py 의 CHAT_WEBHOOK_URL 사용 ───────────────
try:
    from lib import config_local as _cfg  # lib/config_local.py
except Exception:
    _cfg = None


class ChatNotifier(QObject):
    """
    Google Chat 웹훅 알림 (asyncio)
    - 단일 웹훅 URL만 사용
    - 공정 시작 카드에 '사용 Guns/타겟, 파워, Shutter Delay, Process Time' 포함
    - 네트워크 I/O는 asyncio.to_thread로 오프로딩 → UI 프리징 방지

    ※ 키 매핑(업로드한 코드 기준):
      - 건/타겟:
        * NormParams: use_g1/use_g2/use_g3, G{n}_target_name, "G{n} Target"
        * CH1 레거시 UI 폴백: gunTarget_name
      - 파워:
        * DC 연속: use_dc_power, dc_power
        * DC-Pulse: use_dc_pulse, dc_pulse_power, dc_pulse_freq, dc_pulse_duty(또는 dc_pulse_duty_cycle)
        * RF-Pulse: use_rf_pulse, rf_pulse_power, rf_pulse_freq, rf_pulse_duty(또는 rf_pulse_duty_cycle)
        * (옵션) RF 연속: use_rf_power, rf_power  ← 값이 들어오면 표시
      - 시간:
        * shutter_delay, process_time (기존 표시와 동일하게 '분' 단위 요약)
    """

    def __init__(self, webhook_url: Optional[str] = None, parent=None):
        super().__init__(parent)

        # 단일 웹훅 URL
        cfg_url = ""
        if _cfg is not None:
            cfg_url = (getattr(_cfg, "CHAT_WEBHOOK_URL", "") or "").strip()
        self.webhook_default = (webhook_url or cfg_url).strip()

        # 지연 전송 & 버퍼 (payload, webhook_url) 튜플로 저장
        self._defer: bool = True
        self._buffer: List[Tuple[dict, Optional[str]]] = []

        # 실행 컨텍스트
        self._last_started_params: Optional[dict] = None
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
                    await asyncio.wait(self._pending, timeout=2.0)
                loop.create_task(_drain())
        except RuntimeError:
            pass

    # ---------- 숫자/포맷 유틸 ----------
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

    def _b(self, params: dict, key: str) -> bool:
        v = params.get(key, False)
        if isinstance(v, str):
            v = v.strip().lower()
            return v in ("1", "t", "true", "y", "yes", "on", "checked")
        return bool(v)

    def _fmt_min(self, v) -> str:
        # 기존과 동일하게 '분' 단위로 요약(0 이하 → '—')
        try:
            f = float(v)
        except Exception:
            return "—"
        if f <= 0:
            return "—"
        return f"{int(f)}분" if abs(f - int(f)) < 1e-6 else f"{f:.1f}분"

    # ---------- 내부 전송 ----------
    def _resolve_webhook(self, _route_params_ignored: Optional[dict] = None) -> Optional[str]:
        """단일 웹훅만 사용."""
        return self.webhook_default or None

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
                # 실패는 조용히 무시(원하면 파일로그 등 추가)
                pass

        await asyncio.to_thread(_blocking_post)

    def _schedule_post(self, payload: dict, webhook_url: Optional[str]):
        """비동기 전송 태스크를 스케줄하고 참조를 보관(가비지 방지)."""
        if not webhook_url:
            return
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
                    "cardId": "error-aggregate",
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

    # ---------- 내용 포맷 ----------
    def _guns_and_targets(self, p: dict) -> str:
        """
        NormParams/RawParams 키를 모두 지원하여 건/타겟 요약을 만든다.
        - NormParams: use_g{n}, G{n}_target_name, "G{n} Target"
        - RawParams : "G{n} Target"
        - CH1 레거시: gunTarget_name (있으면 G1로 표기)
        """
        items = []
        for i in (1, 2, 3):
            used_flag = f"use_g{i}"
            name_keys = (f"G{i}_target_name", f"G{i} Target")
            name = (p.get(name_keys[0]) or p.get(name_keys[1]) or "").strip()

            # 선택키가 없거나 False여도 이름만 있으면 노출 (CH1은 셔터 없으므로 이름만 저장될 수 있음)
            used = (used_flag in p and self._b(p, used_flag))
            if used or name:
                items.append(f"G{i}: {name or '-'}")

        if not items:
            # CH1 레거시 폴백
            legacy = (p.get("gunTarget_name") or "").strip()
            if legacy:
                items.append(f"G1: {legacy}")

        return ", ".join(items) if items else "—"

    def _power_summary(self, p: dict) -> str:
        """
        파워 요약:
          - DC-Pulse: use_dc_pulse, dc_pulse_power, dc_pulse_freq, dc_pulse_duty | dc_pulse_duty_cycle
          - RF-Pulse: use_rf_pulse, rf_pulse_power, rf_pulse_freq, rf_pulse_duty | rf_pulse_duty_cycle
          - DC(연속): use_dc_power, dc_power
          - (옵션) RF(연속): use_rf_power, rf_power  → 값이 오면 표시
        """
        items = []

        # DC Pulse
        use_dcp = self._b(p, "use_dc_pulse")
        dcpw = self._num(p.get("dc_pulse_power"))
        dcfq = self._num(p.get("dc_pulse_freq"))
        dcdt = self._num(p.get("dc_pulse_duty", p.get("dc_pulse_duty_cycle")))
        if use_dcp or (dcpw is not None or dcfq is not None or dcdt is not None):
            pw_txt = f"{int(dcpw)} W" if dcpw is not None else "—"
            fq_txt = f"{int(dcfq)} Hz" if dcfq is not None else "keep"
            dt_txt = f"{int(dcdt)} %" if dcdt is not None else "keep"
            items.append(f"DC Pulse {pw_txt} @ {fq_txt}, {dt_txt}")

        # RF Pulse
        use_rfp = self._b(p, "use_rf_pulse")
        rfpw = self._num(p.get("rf_pulse_power"))
        rffq = self._num(p.get("rf_pulse_freq"))
        rfdt = self._num(p.get("rf_pulse_duty", p.get("rf_pulse_duty_cycle")))
        if use_rfp or (rfpw is not None or rffq is not None or rfdt is not None):
            pw_txt = f"{int(rfpw)} W" if rfpw is not None else "—"
            fq_txt = f"{int(rffq)} Hz" if rffq is not None else "keep"
            dt_txt = f"{int(rfdt)} %" if rfdt is not None else "keep"
            items.append(f"RF Pulse {pw_txt} @ {fq_txt}, {dt_txt}")

        # DC(연속)
        use_dc = self._b(p, "use_dc_power")
        dcp = self._num(p.get("dc_power"))
        if use_dc or dcp is not None:
            items.append(f"DC {int(dcp)} W" if dcp is not None else "DC — W")

        # RF(연속) — 장비가 지원하지 않아도 값이 주어지면 묵시적으로 표기(안 들어오면 생략)
        use_rf = self._b(p, "use_rf_power")
        rfp = self._num(p.get("rf_power"))
        if use_rf or rfp is not None:
            items.append(f"RF {int(rfp)} W" if rfp is not None else "RF — W")

        return " / ".join(items) if items else "—"

    # ========== Qt 슬롯들 ==========
    @Slot(dict)
    def notify_process_started(self, params: dict):
        # 실행 컨텍스트 초기화
        self._last_started_params = dict(params) if params else None
        self._errors.clear()
        self._error_seen.clear()
        self._finished_sent = False
        self._buffer.clear()  # 이전 집계 카드 등 모두 제거

        p = params or {}

        # 공정 이름
        name = p.get("process_note") or p.get("Process_name") or "Untitled"

        # 챔버 정보 (ch 또는 prefix에서 추출)
        ch_txt = ""
        ch_val = p.get("ch")
        if ch_val not in (None, ""):
            try:
                ch_txt = f"CH{int(ch_val)}"
            except Exception:
                ch_txt = str(ch_val).strip() or ""

        if not ch_txt:
            prefix = str(p.get("prefix", "")).strip()
            if prefix:
                head = prefix.split()[0]  # "CH1 Sputter" → "CH1"
                if head.upper().startswith("CH"):
                    ch_txt = head

        # 서브타이틀: "CH1 · Plasma Cleaning" / "CH2 · 공정명" 이런 식으로 표시
        subtitle = f"{ch_txt} · {name}" if ch_txt else name

        guns = self._guns_and_targets(p)
        pwr  = self._power_summary(p)
        sh_delay  = self._fmt_min(p.get("shutter_delay", 0))
        proc_time = self._fmt_min(p.get("process_time", 0))

        # 필드에 Chamber도 추가
        fields = {
            "사용 Guns / 타겟": guns if guns else "—",
            "파워": pwr if pwr else "—",
            "Shutter Delay": sh_delay,
            "Process Time": proc_time,
        }
        if ch_txt:
            # 맨 위에 Chamber를 보이게 하고 싶으면 이렇게 앞에 넣어도 됨
            fields = {"Chamber": ch_txt, **fields}

        self._post_card(
            title="공정 시작",
            subtitle=subtitle,
            status="INFO",
            fields=fields,
            urgent=True,
            route_params=params,
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

        self._post_card("공정 종료", subtitle, status, fields, route_params=detail)

        # 종료 카드와 함께 누적 오류 집계 카드 1장도 같이 나가도록
        self._upsert_error_card()
        self.flush()
        self._finished_sent = True
        self._errors.clear()
        self._error_seen.clear()

    @Slot(bool)
    def notify_process_finished(self, ok: bool):
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
        self._post_text(text, route_params=self._last_started_params)

    @Slot(str)
    def notify_error(self, reason: str):
        pretty = (reason or "unknown").strip()
        self._errors.append(pretty)
        self._error_seen.add(pretty)
        self._upsert_error_card()

    @Slot(str, str)
    def notify_error_with_src(self, src: str, reason: str):
        base = (reason or "unknown").strip()
        pretty = f"[{src}] {base}" if src else base
        self._errors.append(pretty)
        self._error_seen.add(pretty)
        self._upsert_error_card()

    # ------------------------------------------------------------------
    # ✅ HOST/통신 계열 오류: '끝'이 없어서 집계하지 말고 발생 즉시 전송
    #    - util/error_reporter.notify_all() 이 이 경로를 우선 호출함
    #    - 기존 notify_error_with_src() 는 (공정 중) 누적 집계 카드 용도로 유지
    # ------------------------------------------------------------------
    @Slot(str, str, str)
    def notify_error_event(self, src: str, error_code: str, message: str):
        """통신/서버 계열 오류를 **발생 즉시** Google Chat에 전송한다.

        주의:
        - 여기서는 self._errors(집계용)에 넣지 않는다. (모아보내기 방지)
        - message는 이미 1줄로 정리된 상태를 기대하지만,
        여기서도 한 번 더 '\\n'/'\\r'을 제거해 안전하게 보정한다.
        """

        src = (src or "HOST").strip() or "HOST"
        code = (error_code or "").strip().upper()

        # message 정규화(줄바꿈 제거)
        msg = (message or "").replace("\r", "\n")
        msg = " ".join(msg.splitlines()).strip()

        # 메시지에 코드가 이미 들어있는 경우(예: "E401 | ...") 중복 제거
        if code and msg.upper().startswith(code):
            tail = msg[len(code):].lstrip()
            if tail.startswith("|"):
                tail = tail[1:].lstrip()
            elif tail.startswith(":"):
                tail = tail[1:].lstrip()
            msg = tail or msg

        # cause / fix 분리(표시를 깔끔하게)
        cause = msg
        fix = ""
        if "해결방법:" in msg:
            a, b = msg.split("해결방법:", 1)
            cause = a.strip()
            fix = b.strip()

        subtitle = f"[{src}] {code} | {cause}" if code else f"[{src}] {cause}"
        fields = {"해결방법": fix} if fix else None

        # urgent=True: 버퍼에 쌓지 말고 즉시 전송(루프가 없으면 flush 때 전송)
        self._post_card("장비 오류", subtitle=subtitle, status="FAIL", fields=fields, urgent=True)

