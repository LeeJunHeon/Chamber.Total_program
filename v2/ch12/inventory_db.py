"""재고 DB(재고관리 웹앱 API) Gun Target 불러오기 — v1 util/inventory_client.py + runtime/chamber_runtime.py
_load_gun_targets(8925-8950)·큐 행 시작 대기(5080-5098) (PROC-034·035).

v1 그대로
- GET INVENTORY_API_URL(기본 https://vanam.synology.me/inventory/api/chamber-slots), Accept application/json, 시간 한도 5 s,
  https 는 인증서 검사 끔(CERT_NONE — v1 그대로). 전용 1-worker 풀(InventoryIO)에서 — 다른 일을 막지 않는다.
- 응답 = 슬롯 목록. locationName 'Chamber N - Gun M' → (N, 'GM Target'), 파싱 안 되면 건너뜀, 다른 챔버는 건너뜀.
  값 = 'itemName (targetUnitId)'(unit id 가 없으면 itemName). 결과는 늘 {'G1 Target','G2 Target','G3 Target'}(없으면 '').
- 네트워크 실패(연결·시간 초과·HTTP 오류·JSON 아님)는 빈 값 — 공정 무영향(로그 없음). 응답 모양이 틀려 읽다 예외가 나면
  TARGET 로그 '[CH{n}] 타겟 정보 불러오기 실패: {e!r}' 뒤 빈 값(v1 _load_gun_targets 의 except).
- 큐(파일 레시피) 행마다 — delay·TEST 행 포함(v1 은 delay 판정보다 먼저 검사) — 행의 G1~G3 Target(또는 G*_target_name)이 모두
  비면 불러오기를 최대 3 s 기다린다: 넘으면 TARGET '[CH{n}] Gun Target DB 3초 초과 — 빈 값으로 진행 (공정 진행)',
  예외면 '[CH{n}] Gun Target DB 실패: {e!r} — 빈 값으로 진행'. 늦게 온 결과는 이 행에 쓰지 않는다(v1 wait_for 취소).
- 채운 값의 쓰임(v1 _normalize_params_for_process 6167-6188 — 비면 화면 칸 = DB 값): CH1 은 G1 Target 만, CH2 는 use_gN 인 건만.
  v1 _load_gun_targets 는 비어 있는 칸만 채운다 — 이 행의 칸은 비어 있으므로(그래서 불렀다) 받은 값이 그대로 들어간다.

v2 방식
- 외부 접속은 실행 프로그램(prod)에서만. sim·bench·시험은 127.0.0.1(localhost) URL 만 쓴다 — 설정 URL 이 밖이면 sim 은 가짜 서버
  (v2/ch12/sim/inventory_sim.FakeInventoryServer)를 띄워 그 URL 로, bench 는 끔(불러오기 없이 빈 값 — 기다리지 않음).
- 화면 칸이 없으므로 받은 값은 InventoryDb.latest(n) 에 남긴다(화면·웹이 'DB 타겟'으로 보여 줄 값 — v1 공정 끝 뒤 다시 불러오기는
  refresh(n) — 기다리지 않음).
- socket.setdefaulttimeout(전역)은 바꾸지 않는다(v2 는 worker 스레드가 같은 프로세스에 있다) — 대기는 3 s 한도가 끊는다.

붙이는 법(조립): db = make_inventory(cfg, mode); attach(engine, db) → engine.gun_targets(run) 를 엔진 러너가 큐 행마다 부른다.
  끝낼 때 db.close().
"""
from __future__ import annotations

import concurrent.futures as cf
import json
import ssl
import threading
import urllib.request
from typing import Any, Callable, Dict, Mapping, Optional
from urllib.parse import urlparse

DEFAULT_URL = "https://vanam.synology.me/inventory/api/chamber-slots"    # util/inventory_client.py:10
HTTP_TIMEOUT_S = 5.0                                                     # util/inventory_client.py:11
START_WAIT_S = 3.0                                                       # chamber_runtime.py:5088
LOOPBACK = ("127.0.0.1", "localhost", "::1")
KEYS = ("G1 Target", "G2 Target", "G3 Target")


class FetchFailed(Exception):
    """네트워크 실패(v1: 빈 값으로 — 공정 무영향)."""


def parse_slot(loc_name: str) -> Optional[tuple]:
    """'Chamber 1 - Gun 1' → (1, 'G1 Target'), 파싱 실패 None(inventory_client.py:16-32)."""
    try:
        left, right = loc_name.split("-")
        ch_num = int(left.strip().split()[-1])
        gun_num = int(right.strip().split()[-1])
        return ch_num, f"G{gun_num} Target"
    except Exception:
        return None


def targets_from_slots(slots: Any, ch: int) -> Dict[str, str]:
    """슬롯 목록 → 챔버 ch 의 {'G1 Target': ..}(inventory_client.py:68-80). 모양이 틀리면 예외(v1 과 같은 곳에서)."""
    result: Dict[str, str] = {k: "" for k in KEYS}
    for slot in slots:
        loc_name = slot.get("locationName") or ""
        parsed = parse_slot(loc_name)
        if parsed is None:
            continue
        ch_num, gun_key = parsed
        if ch_num != ch:
            continue
        item_name = slot.get("itemName") or ""
        unit_id = slot.get("targetUnitId")
        result[gun_key] = f"{item_name} ({unit_id})" if unit_id is not None else item_name
    return result


def http_get_slots(url: str, timeout_s: float = HTTP_TIMEOUT_S) -> Any:
    """_fetch_slots_sync(inventory_client.py:35-52) — 실패는 FetchFailed."""
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        if is_loopback(url):                      # 127.0.0.1(sim·시험) — 프록시를 거치지 않는다
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(req, timeout=timeout_s) as resp:
                return json.loads(resp.read().decode())
        kw = {"context": ctx} if url.lower().startswith("https:") else {}
        with urllib.request.urlopen(req, timeout=timeout_s, **kw) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        raise FetchFailed(repr(e)) from e


def is_loopback(url: str) -> bool:
    try:
        return (urlparse(url).hostname or "") in LOOPBACK
    except Exception:
        return False


class InventoryDb:
    """재고 DB 클라이언트 하나(두 챔버가 같이 쓴다 — v1 _INV_EXEC 1 worker)."""

    def __init__(self, url: Optional[str], *, fetch: Callable[[str], Any] = http_get_slots,
                 wait_s: float = START_WAIT_S, fake=None) -> None:
        self.url = url
        self.fetch = fetch
        self.wait_s = float(wait_s)
        self.fake = fake                    # sim 가짜 서버(close 때 멈춤)
        self._pool = cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="InventoryIO")
        self._lk = threading.Lock()
        self._latest: Dict[int, Dict[str, str]] = {}
        self.fetches = 0

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    def latest(self, n: int) -> Dict[str, str]:
        with self._lk:
            return dict(self._latest.get(int(n)) or {k: "" for k in KEYS})

    def _load(self, n: int, log: Callable[[str, str], None]) -> Optional[Dict[str, str]]:
        """_load_gun_targets 의 불러오기 부분(풀 스레드에서). 받은 값(실패면 빈 값) 또는 읽기 예외면 None."""
        self.fetches += 1
        try:
            slots = self.fetch(self.url)
        except FetchFailed:
            got = {k: "" for k in KEYS}               # 네트워크 실패 → 빈 값(로그 없음)
        else:
            try:
                got = targets_from_slots(slots, n)
            except Exception as e:
                log("TARGET", f"[CH{n}] 타겟 정보 불러오기 실패: {e!r}")
                return None
        with self._lk:
            self._latest[n] = dict(got)
        return got

    def request(self, n: int, log: Callable[[str, str], None]) -> "cf.Future":
        return self._pool.submit(self._load, int(n), log)

    def refresh(self, n: int, log: Optional[Callable[[str, str], None]] = None) -> Optional["cf.Future"]:
        """기다리지 않는 다시 불러오기(v1 asyncio.ensure_future(self._load_gun_targets()) — 시작·공정 끝 뒤)."""
        if not self.enabled:
            return None
        return self.request(n, log or (lambda s, m: None))

    def wait_targets(self, n: int, log: Callable[[str, str], None]) -> Optional[Dict[str, str]]:
        """큐 행 시작 대기(chamber_runtime.py:5086-5098) — 받은 값, 늦음·실패·끔이면 None."""
        if not self.enabled:
            return None
        try:
            fut = self.request(n, log)
            return fut.result(timeout=self.wait_s)
        except cf.TimeoutError:
            log("TARGET", f"[CH{n}] Gun Target DB 3초 초과 — 빈 값으로 진행 (공정 진행)")
        except Exception as e:
            log("TARGET", f"[CH{n}] Gun Target DB 실패: {e!r} — 빈 값으로 진행")
        return None

    def close(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)
        if self.fake is not None:
            try:
                self.fake.stop()
            except Exception:
                pass
            self.fake = None


def make_inventory(cfg: Mapping[str, Any], mode: str, **kw: Any) -> InventoryDb:
    """모드별 재고 DB — prod 만 설정 URL(밖) 접속, sim 은 가짜 서버(127.0.0.1), bench·그 밖은 127.0.0.1 URL 이 아니면 끔."""
    url = str(cfg.get("INVENTORY_API_URL") or DEFAULT_URL).strip() or DEFAULT_URL
    if str(mode) == "prod" or is_loopback(url):
        return InventoryDb(url, **kw)
    if str(mode) == "sim":
        from v2.ch12.sim.inventory_sim import FakeInventoryServer
        fake = FakeInventoryServer().start()
        return InventoryDb(fake.url, fake=fake, **kw)
    return InventoryDb(None, **kw)


def row_targets_empty(src: Optional[Mapping[str, Any]]) -> bool:
    """행의 G1~G3 Target 이 모두 비었나(chamber_runtime.py:5081-5085 — 'G* Target' 또는 'G*_target_name')."""
    s = src or {}
    return not any(str(s.get(f"G{i} Target", "") or s.get(f"G{i}_target_name", "")).strip() for i in (1, 2, 3))


def apply_targets(params: Dict[str, Any], got: Mapping[str, str], n: int) -> None:
    """받은 값을 런 params 에(normalize_row 의 G* 규칙 — CH1 은 G1 만, CH2 는 use_gN 인 건만). 이미 값이 있는 칸은 두지 않는다."""
    for i in (1, 2, 3):
        if n == 1 and i != 1:
            continue
        if n != 1 and not params.get(f"use_g{i}", False):
            continue
        if str(params.get(f"G{i} Target", "") or "").strip():
            continue
        v = str(got.get(f"G{i} Target", "") or "")
        params[f"G{i} Target"] = params[f"G{i}_target_name"] = v


def attach(engine, db: InventoryDb) -> Callable[[Any], None]:
    """엔진 gun_targets 자리에 꽂는다 — engine.gun_targets(run) 를 러너가 큐 행마다(delay·TEST 포함) 부른다."""
    n = int(engine.n)

    def gun_targets(run) -> None:
        try:
            raw = run.raw if getattr(run, "raw", None) is not None else (run.params or {})
            if not row_targets_empty(raw):
                return
            got = db.wait_targets(n, engine.log)
            if got and run.params is not None:
                apply_targets(run.params, got, n)
        except Exception as e:                 # 러너를 멈추지 않는다(v1 except Exception — 빈 값으로 진행)
            engine.log("TARGET", f"[CH{n}] Gun Target DB 실패: {e!r} — 빈 값으로 진행")

    engine.gun_targets = gun_targets
    return gun_targets
