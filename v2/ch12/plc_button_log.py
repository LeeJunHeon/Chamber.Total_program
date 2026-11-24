"""PLC 버튼 기록(2026-11-20 D425) — 누를 때마다 사건 하나: 버튼·켬/끔·누가·웹/노트북·결과·그 챔버 공정 상태.

사용자 결정(2026-11-20): PLC 버튼은 공정 중에도 누를 수 있게 둔다(잠그지 않음) — 대신 확인창(화면)과 기록을 강화한다.
- 사건: source 'command:<챔버>'(공통은 'command')·kind 'plc_button'·event {"chamber", "data"} — 허브가 context.chamber(그 챔버 탭)·context.data 로 낸다.
  챔버 = 버튼의 공정 챔버(commands.PlcButton.proc_chamber — 챔버 버튼은 그 챔버, 건 셔터·Power Select 는 CH2), 없으면 shared.
  data = {target, name(코일), short(이름), chamber(버튼 자리), proc_chamber, on, by(계정 — 웹 requestedBy·노트북 'local'), by_name,
  origin('web'·'local'·…), who(화면 글 — '홍길동(웹)'·'노트북'), result('done'·'failed'·'rejected'), reason, detail,
  hazard('person'·'equipment'·None), process({chamber, busy, state, block_no, block, recipe} — 누른 순간)}.
  수준·코드: 실행됨 info(공정 중 PLC_MANUAL_IN_PROCESS, 아니면 PLC_BUTTON) · 쓰기 실패 warn PLC_BUTTON_FAILED · 거부 warn 거부 사유.
- 결과: 받은 명령은 PLC 쓰기가 끝날 때(on_done — 성공·실패·시간 초과) 한 번, 받지 않은 명령(인자·상태·bench·PLC 끊김)은 그 자리에서.
- 장비 로그(SYSTEM 일자 로그, 출처 'PLC')에 늘 한 줄, 공정 중이면 그 챔버 공정 로그(txt·화면 로그)에도 한 줄('PLC' 출처).
  글: 'PLC 버튼 · CH1 Vent 켜기 · 홍길동(웹, hong@…) (CH1 공정 중 · 시간 기준 메인 공정 · 레시피 R1)' + 실패·거부면 ' — 거부: 이유'.
  화면 글(사건 data 로 포털이 만든다)은 계정 없이 이름만: 'PLC 버튼 · CH1 Vent 켜기 · 홍길동(웹) (CH1 공정 중 · 시간 기준 메인 공정)'.
  (묶음 16 — '켬/끔' 대신 '켜기/끄기', 버튼 이름은 필드 이름과 같은 용어표 이름)
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from v2.common.commands import Source
from v2.ch12 import blocks as BK
from v2.ch12 import commands as C

ORIGIN = {Source.WEB: "web", Source.HMI: "local", Source.ROBOT: "robot"}
ORIGIN_WORD = {"web": "웹", "local": "노트북", "robot": "로봇"}
RESULT_WORD = {"failed": "실패", "rejected": "거부"}


def origin_of(src: Any) -> str:
    return ORIGIN.get(src, str(getattr(src, "value", src) or "internal"))


def who_of(origin: str, by: str, by_name: Optional[str], *, account: bool = False) -> str:
    """'홍길동(웹)'(account 면 '홍길동(웹, hong@…)') · '노트북' · '로봇'. 웹인데 이름이 없으면 계정(이메일이면 @ 앞)."""
    if origin == "local":
        return "노트북"
    word = ORIGIN_WORD.get(origin, origin)
    if origin != "web":
        return word
    by = str(by or "")
    name = str(by_name or "").strip() or (by.split("@", 1)[0] if "@" in by else by) or "알 수 없음"
    return f"{name}({word}, {by})" if account and by and by != name else f"{name}({word})"


def process_of(engine: Any, ch: str) -> Dict[str, Any]:
    """그 챔버 공정 상태(누른 순간) — busy·상태·지금 블록·레시피 이름."""
    if engine is None:
        return {"chamber": ch, "busy": False, "state": None, "block_no": None, "block": None, "recipe": None}
    busy = bool(getattr(engine, "busy", False))
    inst = getattr(engine, "cur_inst", None) if busy else None
    block = None
    if inst is not None:
        d = BK.by_type().get(getattr(inst, "type", None))
        block = getattr(inst, "label", None) or (d.label if d else getattr(inst, "type", None))
    params = getattr(engine, "params", None) or {}
    recipe = (str(params.get("process_name") or getattr(engine, "recipe_name", "") or "") or None) if busy else None
    try:
        no = engine.block_no() if busy and inst is not None else None
    except Exception:
        no = None
    return {"chamber": ch, "busy": busy, "state": str(getattr(engine, "state_text", "") or "") or None,
            "block_no": no, "block": block, "recipe": recipe}


def line(data: Dict[str, Any], *, account: bool) -> str:
    """기록 한 줄(account = 계정까지 — 장비 로그·공정 로그·사건 문구). 화면 글은 포털이 같은 규칙으로(계정 없이) 만든다."""
    who = who_of(data["origin"], data["by"], data.get("by_name"), account=account)
    t = f"PLC 버튼 · {data['short']} {'켜기' if data['on'] else '끄기'} · {who}"
    p = data.get("process") or {}
    if p.get("busy"):
        what = " · ".join(x for x in (p.get("block"), f"레시피 {p['recipe']}" if account and p.get("recipe") else None) if x)
        t += f" ({str(p['chamber']).upper()} 공정 중{' · ' + what if what else ''})"
    if data["result"] in RESULT_WORD:
        why = data.get("detail") or data.get("reason") or ""
        t += f" — {RESULT_WORD[data['result']]}{': ' + why if why else ''}"
    return t


class PlcButtonLog:
    """core 가 부른다 — begin(누른 순간: 대상·누가·공정 상태) → finish(결과). 사건·장비 로그·공정 로그를 한 번에."""

    def __init__(self, system, proc: Dict[str, Any]) -> None:
        self.sys = system
        self.proc = proc
        self.count = 0

    def begin(self, args: Dict[str, Any], src: Any, by: str) -> Dict[str, Any]:
        t = str(args.get("target") or "")
        b = C.PLC_BY_TARGET.get(t)
        on = args.get("on") is True
        pch = b.proc_chamber if b else None
        bn = args.get("by_name") if isinstance(args.get("by_name"), str) else None
        return {"target": t, "name": b.name if b else None, "short": b.short if b else (t or "알 수 없는 버튼"),
                "chamber": b.chamber if b else None, "proc_chamber": pch, "on": on, "by": str(by or ""),
                "by_name": (bn or "").strip()[:60] or None, "origin": origin_of(src),
                "hazard": b.hazard(on) if b else None,
                "process": process_of(self.proc.get(pch), pch) if pch else None,
                "result": None, "reason": None, "detail": None}

    def finish(self, data: Dict[str, Any], result: str, reason: Optional[str] = None, detail: str = "") -> None:
        data = dict(data, result=result, reason=reason, detail=(str(detail or "") or None))
        data["who"] = who_of(data["origin"], data["by"], data.get("by_name"))
        text = line(data, account=True)
        p = data.get("process") or {}
        busy = bool(p.get("busy"))
        level = "info" if result == "done" else "warn"
        code = ("PLC_MANUAL_IN_PROCESS" if busy else "PLC_BUTTON") if result == "done" else \
            ("PLC_BUTTON_FAILED" if result == "failed" else (reason or "REJECTED"))
        ch = data.get("proc_chamber") or "shared"
        self.count += 1
        try:
            self.sys.emit("command" if ch == "shared" else f"command:{ch}", "plc_button", text, level=level, code=code,
                          event={"chamber": ch, "data": data})
        except Exception:
            pass
        self._system_log(text)
        e = self.proc.get(data.get("proc_chamber") or "")
        if busy and e is not None:
            try:
                e.log("PLC", text)                         # 그 챔버 공정 로그(txt·화면 로그) — 공정 중일 때만(파일이 열려 있음)
            except Exception:
                pass

    def _system_log(self, text: str) -> None:
        """SYSTEM 일자 로그(장비 로그) — 엔진 공정 기록이 쓰는 같은 SystemLog(같은 쓰기 스레드)."""
        for e in self.proc.values():
            sl = getattr(getattr(e, "txt", None), "system_log", None)
            if sl is not None:
                try:
                    sl.append("PLC", text)
                except Exception:
                    pass
                return
