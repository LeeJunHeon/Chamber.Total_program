"""장비↔웹 메시지 형식과 연결 계약(장비 공통, 결정 8 — 2026-10-20). 웹 단계는 이 독스트링을 계약으로 그대로 쓴다.

연결 — 장비가 먼저 연결한다(웹은 장비로 접속하지 않는다). UPLINK_URL = 웹의 장비 연결 기준 주소.
  두 길 모두 헤더 Authorization: Bearer <UPLINK_TOKEN>(장비별). https 는 인증서를 확인한다.

  POST {UPLINK_URL}/ingest — 장비 → 웹, UPLINK_INTERVAL_MS(기본 1 s)마다. Content-Type: application/json
    요청 {"equipment": "CHK2", "instance": "<이 실행 id>", "schemaHash": "<파라미터 정의 해시>",
          "messages": [Envelope, ...]}
    응답 200 {"acked": ["<웹이 저장한 envelope id>", ...], "commands": [Command, ...]?, "needWebSchema": true?}
    - messages 순서: (연결·다시 연결 뒤 첫 POST 면) hello → 보관함의 메시지(오래된 것부터) → 최신 state 하나.
      보관함에는 지난 실행이 남긴 메시지도 있다 — 각 envelope 의 instance 로 구분(seq 는 실행 안에서만 이어진다).
    - 주기 밖 POST: HIGH 메시지(cmd_result·config_ack·run_*·warn/error event)가 생기면 직전 POST 가 성공한 상태이고 그 뒤 200 ms 이상
      지났을 때 바로 POST 한다(명령 결과가 1 s 주기를 기다리지 않게). 실패 뒤 대기(401·409·백오프) 중에는 하지 않는다.
    - 본문은 표준 JSON — NaN·Infinity 는 null 로 바꿔 보낸다(보관함 줄·로컬 서버도 같음).
    - 웹은 id 로 한 번만 저장하고(같은 id 가 다시 와도 저장하지 않되 acked 에는 넣는다), 저장한 id 를 acked 로 돌려준다.
      장비는 acked 에 있는 id 만 보관함에서 지운다 — 끊겨도·프로그램을 다시 켜도 잃지 않는다. state·hello 는 보관하지 않는다.
    - needWebSchema: 웹에 이 장비의 webSchemaHash 묶음이 없으면 true — 장비는 다음 POST 에 webSchema 를 실은 hello 를 한 번 더.
    - commands: SSE 와 같은 명령(웹이 SSE 로 못 보냈을 때 등). 처리 규칙은 같다(같은 id 는 한 번만).

  GET {UPLINK_URL}/stream?equipment=<ID>&instance=<실행 id> — 웹 → 장비 SSE(text/event-stream). 명령의 주 경로.
    event: command / data: Command JSON. 웹은 15 s 마다 keep-alive 주석(": ka"). 장비는 45 s 동안 아무것도 없으면 끊고 다시 붙는다.

  응답 코드별 장비 동작(두 길 같음)
    401 토큰 틀림           → UPLINK_AUTH_FAILED 경고(한 번), 60 s 마다만 다시
    409 이 실행을 받지 않음  → UPLINK_CONFLICT 경고(한 번) — 같은 장비 ID 의 다른 실행이 연결 중, 10 s 마다 다시
    413 너무 큼(ingest)     → 묶음을 반으로 나눠 바로 다시(메시지 하나도 413 이면 그 메시지를 버리고 UPLINK_TOO_LARGE 경고)
    그 밖 오류·시간 초과     → 1→2→4…→30 s 로 늘려 다시(±20 % 흔들림). 시간 제한 UPLINK_TIMEOUT_SEC
    실패가 UPLINK_DOWN_SEC 넘게 이어지면 UPLINK_DOWN 경고, 다시 성공하면 UPLINK_UP(info). 끊겨도 공정·안전은 노트북에서 그대로.

명령(Command) {"id", "kind", "args", "requestedBy", "requestedAt", "expiresAt"}
  - 같은 id 는 한 번만 실행 — 장비는 최근 id 와 결과를 기억해, 다시 오면(SSE·POST 응답 어느 쪽이든) 실행하지 않고 같은 cmd_result 를 다시 보낸다.
  - expiresAt(UTC ISO)이 장비 시계로 UPLINK_CLOCK_SKEW_SEC 보다 더 지났으면 실행하지 않고 cmd_result rejected COMMAND_EXPIRED.
  - 실행은 장비 명령 처리(ChkCore.command, 출처 web, by = requestedBy) — 결과 cmd_result(accepted/rejected)는 보관함에 들어가
    반드시 웹에 닿는다. CONFIG_APPLY·CONFIG_ROLLBACK 도 같은 길(결과는 config_ack 도).
  - bench 는 장비 쓰기 명령을 BENCH_READ_ONLY 로 거부한다(상태·기록은 올린다).

보관함(장비 LOCAL_DATA_DIR\\uplink)
  - state 를 뺀 모든 메시지를 받자마자 디스크에 넣는다. 상한 UPLINK_SPOOL_MAX_MB — 차면 info event·snapshot_summary 부터 오래된 것을
    버리고(run_*·cmd_result·config_ack·warn/error event 는 마지막까지) 버린 수를 UPLINK_SPOOL_FULL 경고로.
  - 한 POST 에 최대 UPLINK_BATCH_MAX 개·UPLINK_BATCH_BYTES 바이트(v2.chk.uplink).
  - 장비 안에서 메시지를 놓치면(느린 소비자로 끊긴 뒤 링 버퍼보다 많이 지남) UPLINK_GAP 경고(놓친 seq 범위).

실행 모드: prod·bench 는 연결, sim 은 연결하지 않음. UPLINK_IN_SIM(설치 키, 웹 개발용)이면 sim 도 연결 — 이때 EQUIPMENT_ID 는 -SIM 으로
  끝나야 한다(아니면 시작 거부). 웹은 hello 의 runMode 로 sim 장비를 구분한다(sim 장비로는 구글챗을 보내지 않는다 — 웹 할 일).
비밀값: UPLINK_URL·UPLINK_TOKEN 은 로그·콘솔·메시지 어디에도 그대로 쓰지 않는다(주소는 호스트만).

Envelope: {"id"(uuid hex), "instance"(실행 id), "seq"(실행 안 증가), "ts"(UTC ISO, ms, Z), "type", "data"}
로컬 서버(브라우저 API v1, v2.chk.local_server)도 같은 envelope 을 SSE 로 보낸다.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MField:
    name: str
    type: str          # str / int / float / bool / obj / list / iso / uuid
    required: bool
    desc: str


@dataclass(frozen=True)
class MessageDef:
    type: str
    direction: str     # "up"(장비→웹) / "down"(웹→장비)
    desc: str
    fields: tuple


def _f(name, type_, required, desc):
    return MField(name, type_, required, desc)


ENVELOPE: tuple = (
    _f("id", "uuid", True, "메시지 id(웹은 이것으로 한 번만 저장)"),
    _f("instance", "str", True, "실행 id(프로그램을 켤 때마다 새로)"),
    _f("seq", "int", True, "실행 안에서 1씩 증가"),
    _f("ts", "iso", True, "UTC 시각(장비 시계)"),
    _f("type", "str", True, "메시지 종류"),
    _f("data", "obj", True, "본문"),
)

MESSAGES: tuple = (
    MessageDef("hello", "up", "연결·다시 연결 때마다 POST 맨 앞. 웹은 같은 장비의 이전 실행 대기 명령을 만료시킨다", (
        _f("equipment", "str", True, "장비 ID"),
        _f("instance", "str", True, "실행 id"),
        _f("program", "str", True, "프로그램 이름"),
        _f("version", "str", True, "브랜치@SHA7"),
        _f("runMode", "str", True, "sim / bench / prod — 웹은 sim 장비를 구분한다"),
        _f("schemaHash", "str", True, "파라미터 정의 해시(sha256 — CONFIG_APPLY 의 schemaHash)"),
        _f("configVersion", "int", True, "장비가 쓰고 있는 설정 버전(0 = 기본값)"),
        _f("snapshotColumns", "list", True, "1초 스냅샷 열 이름"),
        _f("commandKinds", "list", True, "로컬 화면에서 받는 명령 종류"),
        _f("writesAllowed", "bool", True, "장비 쓰기 허용(bench 는 false)"),
        _f("deviceUtc", "iso", True, "장비 UTC 시각 — 웹이 시계 차를 본다"),
        _f("webSchemaHash", "str", True, "웹 설정 내보내기 묶음 해시"),
        _f("overriddenKeys", "list", True, "노트북 덮어쓰기(local_override.json)가 정한 키(비밀값 제외) — 그 키의 실행 값은 "
           "덮어쓰기 값, 저장 값은 묶음 params[].stored"),
        _f("webSchema", "obj", False, "묶음(chk.web_schema.bundle) — 웹이 needWebSchema 로 달라고 할 때만"),
    )),
    MessageDef("state", "up", "현재 상태 1개 — POST 마다 최신 하나(보관하지 않음)", (
        _f("procState", "str", True, "공정 상태 기계 상태"),
        _f("runId", "str", False, "진행 중 공정 id"),
        _f("step", "obj", False, "현재 블록(번호·총수·종류·이름·남은 초)"),
        _f("recipe", "obj", False, "적재된 레시피(이름·행)"),
        _f("heaterRecipe", "obj", False, "히터 레시피 진행 {name, state(RAMPING·SOAKING), step(1부터), steps, "
           "held(일시정지 중 — HEATER_RECIPE_HOLD on=true 뒤 true, on=false 뒤 false)} — 돌고 있지 않으면 null"),
        _f("adjust", "list", False, "공정 중 수정 항목의 지금 값 — 공정 중(PROCESS_ADJUST 를 받는 상태)이면 blocks.ADJUST_ITEMS 마다 "
           "{device, item, value(이번 공정이 지금 쓰는 값 — 항목 단위, RF Pulse 주파수 kHz, main_remaining 은 남은 초), "
           "recipe(레시피 원래 값 — 쓰는 중·지난 블록이면 그 블록, 아니면 다음 블록), adjusted(마지막으로 받아들인 수정 "
           "{by, t(장비 시계 epoch 초), source} 또는 null), scope(live 지금 블록이 쓰는 중 · future 앞으로 올 블록 · "
           "done 쓰던 블록이 지났고 남은 블록도 안 씀 · unused 이번 공정에서 안 씀), busy(RF Pulse START 중 — ADJUST_BUSY)}. "
           "skip_wait 는 value·recipe 가 null. 공정 밖이면 null"),
        _f("heater", "obj", True, "히터 소유·넘김·가스 상태"),
        _f("snapshot", "obj", True, "가장 최근 1초 행(열 이름 → 값)"),
        _f("alarms", "list", True, "활성 알람(규칙 id·사유 코드·시작 시각)"),
        _f("links", "obj", True, "장비별 통신 상태(품질·나이)"),
        _f("configVersion", "int", True, "적용 중 설정 버전"),
        _f("configPending", "obj", False, "대기 중 설정(idle/restart 대기)"),
        _f("left", "obj", True, "지난 공정이 남긴 출력"),
        _f("stopping", "str", False, "진행 중 정지 순서"),
        _f("uplink", "obj", True, "웹 연결 {enabled, connected(SSE), lastOkAt, lastError, queued, spoolBytes, dropped}"),
    )),
    MessageDef("snapshot_summary", "up", "UPLINK_SUMMARY_PERIOD_SEC(정각 맞춤) 요약 — 공정 중에만(구간 안 행에 proc.run_id 가 하나라도 "
               "있을 때만 낸다, 공정 밖 구간은 내지 않음)", (
        _f("runId", "str", True, "구간의 마지막 공정 id(proc.run_id)"),
        _f("from", "iso", True, "구간 시작(정각, UTC)"),
        _f("to", "iso", True, "구간 끝"),
        _f("rows", "int", True, "구간 행 수(누락 확인용)"),
        _f("values", "obj", True, "숫자 열 이름 → {last, mean, min, max}"),
        _f("quality", "obj", True, "장비별 {okRatio, maxAge}"),
    )),
    MessageDef("event", "up", "알람·상태 변화·정보(의미 있는 사건만)", (
        _f("level", "str", True, "error / warn / info"),
        _f("code", "str", True, "사유 코드 또는 이벤트 코드(UPLINK_* 포함)"),
        _f("ruleId", "str", False, "안전 규칙 id(S..)"),
        _f("message", "str", True, "사람이 읽는 문구(500자)"),
        _f("context", "obj", False, "출처(source)·종류(kind)·값"),
    )),
    MessageDef("run_start", "up", "공정 시작(행 단위)", (
        _f("runId", "str", True, "공정 id"),
        _f("recipeName", "str", False, "레시피 이름"),
        _f("by", "str", False, "시작한 사람(requestedBy / local)"),
        _f("name", "str", False, "행 이름"),
        _f("stepNo", "int", True, "행 번호"),
        _f("params", "obj", True, "이 행 파라미터(검사 뒤 값)"),
        _f("blocks", "list", False, "블록 목록"),
        _f("configVersion", "int", True, "설정 버전"),
    )),
    MessageDef("run_end", "up", "공정 종료 — run_start 를 보낸 공정에만", (
        _f("runId", "str", True, "공정 id"),
        _f("result", "str", True, "done / aborted / error"),
        _f("reasonCode", "str", False, "사유 코드"),
        _f("summary", "obj", True, "공정 요약"),
    )),
    MessageDef("cmd_result", "up", "명령 결과(accepted/rejected) — 같은 명령 id 가 다시 오면 같은 내용으로 다시", (
        _f("cmdId", "str", False, "명령 id(웹 명령은 늘 있음 — id 없이 보낸 로컬 화면 명령은 null)"),
        _f("kind", "str", True, "명령 종류"),
        _f("status", "str", True, "accepted / rejected"),
        _f("reasonCode", "str", False, "사유 코드(COMMAND_EXPIRED·BENCH_READ_ONLY …)"),
        _f("source", "str", True, "web / hmi"),
        _f("by", "str", False, "요청자(requestedBy / local)"),
        _f("detail", "str", False, "거부 이유 문구(300자, 예: \"value 5000 — 범위 0 ~ 700 °C (HEATER_MAX_TEMP)\") — 없으면 빈 "
                                    "문자열(kind·source·by·reasonCode 는 각 칸에)"),
    )),
    MessageDef("config_ack", "up", "CONFIG_APPLY·CONFIG_ROLLBACK·idle 적용 결과", (
        _f("version", "int", True, "요청 버전"),
        _f("status", "str", True, "applied / pending_idle / pending_restart / rejected"),
        _f("reasonCode", "str", False, "사유 코드"),
        _f("errors", "list", False, "검사 오류 목록"),
        _f("appliedKeys", "list", False, "바로 적용한 키"),
        _f("pendingKeys", "list", False, "대기 중 키"),
        _f("overriddenKeys", "list", False, "노트북 덮어쓰기가 이긴 키"),
        _f("kind", "str", False, "apply / rollback / idle"),
        _f("by", "str", False, "요청자"),
        _f("changes", "list", False, "[{key, old, new}] — 설정 변경 이력"),
        _f("values", "obj", False, "rollback 이면 새 버전의 값 전체(웹 DB 를 맞춘다)"),
    )),
    MessageDef("command", "down", "웹 → 장비 명령(SSE event: command, 또는 POST 응답 commands[])", (
        _f("id", "str", True, "명령 id(같은 id 는 한 번만 실행)"),
        _f("kind", "str", True, "명령 종류"),
        _f("args", "obj", True, "인자"),
        _f("requestedBy", "str", True, "요청자"),
        _f("requestedAt", "iso", True, "요청 시각"),
        _f("expiresAt", "iso", True, "유효 시간(장비 시계 기준, 허용 차 UPLINK_CLOCK_SKEW_SEC)"),
    )),
)


def _finite(o: Any) -> Any:
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: _finite(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_finite(v) for v in o]
    return o


def dumps_json(obj: Any) -> bytes:
    """표준 JSON(UTF-8, 공백 없음) — NaN·Infinity 는 null 로(웹 JSON.parse 는 NaN 을 못 읽어 400 → 보관함이 멈춘다)."""
    try:
        raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str, allow_nan=False)
    except ValueError:
        raw = json.dumps(_finite(json.loads(json.dumps(obj, default=str))), ensure_ascii=False,
                         separators=(",", ":"), allow_nan=False)
    return raw.encode("utf-8")
