"""챔버12 1초 저널(사용자 지시 3장 (6) — 합의된 이어가기 규칙의 '저널' 부분). 이어가기 판단은 resume.py.

무엇을(파일 하나 = 부분 하나, LOCAL_DATA_DIR\\journal\\<부분>.json)
- ch1·ch2(챔버 공정 엔진 proc.ch1·proc.ch2), pc(플라즈마 클리닝), tsp(TSP), shared(공유 — CH2 히터 상태·처리한 명령 id 와 결과).
- 부분마다 공통 머리: v·part·seq(쓴 횟수)·saved_wall(벽시계)·saved_mono(단조 시계)·boot(= 벽시계 − 단조 시계, 재부팅 알아보기)·
  pid·program·version·active(공정 중)·run_id·restarts(이 공정에서 재시작 횟수)·exit(의도적 종료 표시 — {intentional, why, t_wall}).
- 챔버: state = 엔진 state_for_journal()(run id·소유자·레시피 전체·판 번호·지금 블록·블록/메인 경과·수정값·실행 순서·런 목록·
  스텝·카운트다운 …), thickness = 두께 엔진 state_for_journal(), expect = 지금 장비 상태(1초 행에서 — 이어가기 ③ 의 '기대 상태').
- pc·tsp: state = 엔진 state_for_journal().
- shared: heater = 히터 엔진 state_for_journal(), commands = {"web": uplink.to_state(), "robot": robot_api.to_state()}.

어떻게
- 1초 행이 닫힐 때(system.row_listeners — tick 스레드) 문서를 만들고 JSON 으로 바꿔 쓰기 스레드에 넘긴다(최신 것만). 디스크 쓰기
  (임시 파일 → flush → fsync → os.replace)는 쓰기 스레드에서만 — tick 을 막지 않는다. 바뀌지 않은 부분도 1초마다 다시 쓴다
  (saved_wall 이 '마지막으로 살아 있던 때' — 꺼져 있던 시간 계산의 기준).
- hold(부분): 재시작 판단이 끝날 때까지 그 부분의 옛 저널을 덮어쓰지 않는다(판단 도중 또 죽어도 옛 공정 정보가 남게).
- 처리한 명령 id 기억(웹 uplink·로봇 robot_api): 시작할 때 옛 저널 값을 넣어 두고(restore_commands), 그 객체가 조립에 나타나면
  (system.uplink, system.robot_api 또는 엔진 조립 assembly.robot_api — 나중에 붙어도) to_state 로 저장하기 전에 먼저 from_state 로
  되살린다(bind_commands — app 은 웹 연결을 만든 직후에도 부른다).
- mark_exit(why): 의도적 종료 — 모든 부분에 exit 를 적어 바로(동기) 쓰고, 그 뒤로는 1초 쓰기를 멈춘다(write_final 만).
- 읽기(load): 없는 파일 = missing, JSON 이 아니거나 형식이 다르면 corrupt(파일은 <이름>.corrupt 로 남겨 둔다).
- 시험용 abandon(): 쓰기 스레드만 멈춘다(마지막 저널은 디스크 그대로 — '프로그램이 죽음' 흉내).
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from v2.ch12 import snapshot_fields as SF

VERSION = 1
PARTS = ("ch1", "ch2", "pc", "tsp", "shared")
ENGINE_OF = {"ch1": "proc.ch1", "ch2": "proc.ch2", "pc": "pc", "tsp": "tsp", "heater": "heater"}
PERIOD_S = 1.0                      # 1초 저널
BOOT_TOL_S = 5.0                    # 같은 부팅으로 볼 벽시계−단조 시계 차이(시계 맞춤 등)
EXPECT_FRESH_S = 10.0               # 기대 상태에 넣을 값의 나이 상한(번갈아 읽는 값 — MFC R60/R5, RF Pulse 3단 회전)
REPLACE_TRIES = 5                   # os.replace 가 PermissionError(윈도우 — 백신·검색 색인이 잠깐 엶)면 다시
CMD_STORES = ("web", "robot")       # 명령 기억 이름 → system 속성
CMD_ATTR = {"web": "uplink", "robot": "robot_api"}


def heater_of(system) -> Any:
    """히터 엔진 — 조립은 HeaterHost(eng) 로 꽂는다(엔진 이름 heater)."""
    h = system.engine("heater") if hasattr(system, "engine") else None
    return getattr(h, "eng", h)


def journal_dir(cfg) -> Path:
    return Path(cfg["LOCAL_DATA_DIR"]) / "journal"


def part_path(directory: Path, part: str) -> Path:
    return Path(directory) / f"{part}.json"


# ───────────── 원자적 쓰기·읽기 ─────────────
def atomic_write(path: Path, data: bytes) -> None:
    """임시 파일에 다 쓰고 fsync 한 뒤 os.replace — 중간에 꺼져도 옛 파일 아니면 새 파일(반쯤 쓴 파일 없음)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    for i in range(REPLACE_TRIES):
        try:
            os.replace(tmp, path)
            break
        except PermissionError:
            if i == REPLACE_TRIES - 1:
                raise
            time.sleep(0.05)
    if os.name == "posix":                       # 폴더 항목까지(이름 바꾸기가 정전에 살아남게) — 윈도우는 없음
        try:
            fd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass


def read_part(path: Path) -> Tuple[Optional[dict], Optional[str]]:
    """(문서, 오류). 오류: None · 'missing' · 'corrupt: …'(깨진 파일은 .corrupt 로 남김)."""
    path = Path(path)
    if not path.is_file():
        return None, "missing"
    try:
        raw = path.read_bytes()
        d = json.loads(raw.decode("utf-8"))
        if not isinstance(d, dict) or d.get("v") != VERSION or "saved_wall" not in d:
            raise ValueError(f"형식이 다름(v={d.get('v') if isinstance(d, dict) else type(d).__name__})")
        return d, None
    except Exception as e:
        try:
            os.replace(path, path.with_name(path.name + ".corrupt"))
        except OSError:
            pass
        return None, f"corrupt: {type(e).__name__}: {e}"[:300]


def load(directory: Path, parts: Iterable[str] = PARTS) -> Dict[str, Dict[str, Any]]:
    """{부분: {"doc": 문서 또는 None, "error": 오류 또는 None}}."""
    return {p: dict(zip(("doc", "error"), read_part(part_path(directory, p)))) for p in parts}


def boot_mark(clock) -> float:
    return float(clock.wall()) - float(clock.monotonic())


def gap_of(doc: dict, clock) -> Tuple[float, bool]:
    """(꺼져 있던 시간 s, 재부팅). 같은 부팅이면 단조 시계 차이, 아니면(재부팅·단조 시계가 뒤로) 벽시계 차이 + 재부팅=True."""
    wall, mono = float(clock.wall()), float(clock.monotonic())
    sw, sm, sb = float(doc.get("saved_wall") or 0.0), doc.get("saved_mono"), doc.get("boot")
    if sm is not None and sb is not None and abs((wall - mono) - float(sb)) <= BOOT_TOL_S and mono >= float(sm):
        return mono - float(sm), False
    return (wall - sw if wall >= sw else float("inf")), True


# ───────────── 기대 상태(이어가기 ③) ─────────────
def expect_columns(chamber: str) -> Dict[str, tuple]:
    """확인 id → 1초 행 열. 블록 종류별 확인 목록(blocks.RESUME_CHECKS) 중 1초 행으로 보는 것."""
    n = 1 if chamber == "ch1" else 2
    m = f"{chamber}.mfc."
    gas = (f"plc.main_{n}_gas_sw", f"plc.ar_{n}_gas_sw", f"plc.o2_{n}_gas_sw") + (("plc.n2_2_gas_sw",) if n == 2 else ()) + \
        tuple(f"{m}{g}_{k}" for g in ("ar", "o2", "n2") for k in ("on", "set_sccm", "sccm"))
    shutter = (f"plc.main_shutter_{n}_sw",) + (("plc.shutter_1_sw", "plc.shutter_2_sw", "plc.shutter_3_sw") if n == 2 else ())
    power = tuple(f"{chamber}.dcpulse.{k}" for k in ("out_on", "p_w", "set_w")) + \
        tuple(f"{chamber}.rfpulse.{k}" for k in ("out_on", "fwd_w", "set_w"))
    if n == 2:
        power = tuple(f"ch2.dc.dc{u}_{k}" for u in (1, 2) for k in ("enable", "p", "target_w", "set_w")) + \
            tuple(f"ch2.rf.{k}" for k in ("enable", "fwd_w", "target_w", "write_w")) + power
    return {"shutter": shutter, "gas": gas, "pressure": (m + "pressure_mtorr",), "power_output": power,
            "power_select": ("plc.sw_rf_select", "plc.sw_power_select") if n == 2 else ()}


class FreshCache:
    """열마다 마지막 '새로 받은 값'과 그 단조 시각 — 번갈아 읽는 값도 기대 상태·확인에 쓰게."""

    def __init__(self) -> None:
        self.v: Dict[str, Tuple[Any, float]] = {}

    def feed(self, view: SF.RowView, cols: Iterable[str], now: float) -> None:
        for c in cols:
            if c in view.row and view.fresh(c) and view.row.get(c) is not None:
                self.v[c] = (view.row[c], now)

    def get(self, col: str, now: float, max_age: float = EXPECT_FRESH_S) -> Any:
        x = self.v.get(col)
        if x is None or now - x[1] > max_age:
            return None
        return x[0]


def _json_default(o: Any) -> Any:
    if isinstance(o, (set, frozenset, tuple)):
        return list(o)
    if isinstance(o, Path):
        return str(o)
    if hasattr(o, "isoformat"):
        return o.isoformat()
    return repr(o)


def dumps(doc: dict) -> bytes:
    return json.dumps(doc, ensure_ascii=False, default=_json_default, separators=(",", ":")).encode("utf-8")


def _active(part: str, st: Optional[dict]) -> bool:
    if not st:
        return False
    if part in ("ch1", "ch2"):
        return bool(st.get("running") or (st.get("state") not in (None, "IDLE")))
    if part == "pc":
        return bool(st.get("running"))
    if part == "tsp":
        return bool(st.get("busy"))
    return False


# ───────────── 저널 ─────────────
class Journal:
    def __init__(self, system, directory: Optional[Path] = None, period_s: float = PERIOD_S,
                 on_error: Optional[Callable[[str], None]] = None) -> None:
        self.sys = system
        self.clock = system.clock
        self.dir = Path(directory) if directory is not None else journal_dir(system.cfg)
        self.period_s = float(period_s)
        self.on_error = on_error
        self.held: set = set()                      # 판단이 끝날 때까지 덮어쓰지 않는 부분
        self.restarts: Dict[str, Tuple[Optional[str], int]] = {}    # 부분 → (run_id, 재시작 횟수)
        self.exit: Optional[dict] = None
        self.frozen = False                         # mark_exit 뒤 — 1초 쓰기 멈춤
        self.cache = FreshCache()
        self.cols = {ch: expect_columns(ch) for ch in ("ch1", "ch2")}
        self._all_cols = sorted({c for d in self.cols.values() for cs in d.values() for c in cs})
        self.seq = 0
        self.writes = 0
        self.errors = 0
        self.last_error: Optional[str] = None
        self.last_view: Optional[SF.RowView] = None
        self._cmd_saved: Dict[str, Any] = {}        # 옛 저널의 명령 기억(아직 되살리지 않은 것)
        self._cmd_bound: Dict[str, Any] = {}        # 되살린 객체(같은 객체면 다시 하지 않음)
        self._pending: Dict[str, bytes] = {}
        self._lk = threading.Lock()                 # 대기 문서·명령 기억
        self._io = threading.Lock()                 # 디스크 쓰기 한 번에 하나
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._attached = False

    # ── 생명주기 ──
    def start(self) -> "Journal":
        if not self._attached:
            self.sys.row_listeners.append(self.on_row)
            self._attached = True
        if self._thread is None:
            self._thread = threading.Thread(target=self._writer, name="journal", daemon=True)
            self._thread.start()
        return self

    def stop(self, timeout_s: float = 3.0) -> None:
        """남은 것을 쓰고 쓰기 스레드를 멈춘다."""
        self._detach()
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout_s)
            self._thread = None
        self._write_pending()

    def abandon(self) -> None:
        """시험: '프로그램이 죽음' — 남은 것을 쓰지 않고 멈춘다(디스크의 마지막 저널 그대로)."""
        self._detach()
        with self._lk:
            self._pending.clear()
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(3.0)
            self._thread = None

    def _detach(self) -> None:
        if self._attached:
            try:
                self.sys.row_listeners.remove(self.on_row)
            except ValueError:
                pass
            self._attached = False

    # ── 판단 쪽이 쓰는 것 ──
    def hold(self, parts: Iterable[str]) -> None:
        with self._lk:
            self.held |= set(parts)
            for p in parts:
                self._pending.pop(p, None)

    def release(self, part: str) -> None:
        with self._lk:
            self.held.discard(part)

    def set_restarts(self, part: str, run_id: Optional[str], n: int) -> None:
        with self._lk:
            self.restarts[part] = (run_id, int(n))

    def restore_commands(self, saved: Optional[dict]) -> None:
        """옛 저널 shared.commands — 지금 있는 기억 객체에는 바로, 나중에 붙는 것은 붙을 때(bind_commands)."""
        if isinstance(saved, dict):
            with self._lk:
                for name in CMD_STORES:
                    if saved.get(name) is not None:
                        self._cmd_saved[name] = saved[name]
        self.bind_commands()

    def cmd_store(self, name: str) -> Any:
        obj = getattr(self.sys, CMD_ATTR[name], None)
        if obj is None and hasattr(self.sys, "engine"):
            obj = getattr(self.sys.engine("assembly"), CMD_ATTR[name], None)
        return obj

    def bind_commands(self) -> Dict[str, int]:
        """조립에 붙은 명령 기억 객체(system.uplink·system.robot_api)에 옛 기억을 넣는다 — 객체마다 한 번. {이름: 넣은 수}."""
        out: Dict[str, int] = {}
        for name in CMD_STORES:
            obj = self.cmd_store(name)
            if obj is None or not hasattr(obj, "from_state"):
                continue
            with self._lk:
                if self._cmd_bound.get(name) is obj:
                    continue
                self._cmd_bound[name] = obj
                saved = self._cmd_saved.get(name)
            if saved is not None:
                try:
                    out[name] = int(obj.from_state(saved) or 0)
                except Exception as e:
                    self._err(f"명령 기억 되살리기 실패({name}): {e!r}")
        return out

    # ── 문서 만들기 ──
    def _head(self, part: str, now_wall: float, now_mono: float) -> dict:
        return {"v": VERSION, "part": part, "seq": self.seq, "saved_wall": now_wall, "saved_mono": now_mono,
                "boot": now_wall - now_mono, "pid": os.getpid(), "program": getattr(self.sys, "program", ""),
                "version": getattr(self.sys, "version", ""), "exit": dict(self.exit) if self.exit else None}

    def _engine_state(self, name: str) -> Optional[dict]:
        e = heater_of(self.sys) if name == "heater" else (self.sys.engine(ENGINE_OF[name]) if hasattr(self.sys, "engine")
                                                           else None)
        f = getattr(e, "state_for_journal", None)
        if f is None:
            return None
        try:
            return f()
        except Exception as ex:
            self._err(f"{name} state_for_journal 실패: {ex!r}")
            return {"error": repr(ex)}

    def expected(self, chamber: str, now: float) -> Dict[str, Dict[str, Any]]:
        return {cid: {c: self.cache.get(c, now) for c in cols} for cid, cols in self.cols[chamber].items()}

    def snapshot(self) -> Dict[str, dict]:
        """지금 문서 전부(부분 → 문서)."""
        now_wall, now_mono = float(self.clock.wall()), float(self.clock.monotonic())
        self.seq += 1
        self.bind_commands()
        out: Dict[str, dict] = {}
        for part in ("ch1", "ch2", "pc", "tsp"):
            d = self._head(part, now_wall, now_mono)
            st = self._engine_state(part)
            d["state"] = st
            d["active"] = _active(part, st)
            rid = (st or {}).get("run_id")
            d["run_id"] = rid if d["active"] else None
            prev = self.restarts.get(part)
            d["restarts"] = prev[1] if (prev and d["active"] and prev[0] == rid) else 0
            if part in ("ch1", "ch2"):
                eng = self.sys.engine(ENGINE_OF[part])
                th = getattr(eng, "thickness", None)
                d["thickness"] = None
                if th is not None and hasattr(th, "state_for_journal"):
                    try:
                        d["thickness"] = th.state_for_journal()
                    except Exception as ex:
                        self._err(f"{part} 두께 state_for_journal 실패: {ex!r}")
                d["expect"] = self.expected(part, now_mono)
            out[part] = d
        sh = self._head("shared", now_wall, now_mono)
        sh["heater"] = self._engine_state("heater")
        cmds: Dict[str, Any] = {}
        for name in CMD_STORES:
            obj = self.cmd_store(name)
            if obj is not None and hasattr(obj, "to_state"):
                try:
                    cmds[name] = obj.to_state()
                except Exception as ex:
                    self._err(f"명령 기억 저장 실패({name}): {ex!r}")
            else:
                with self._lk:
                    cmds[name] = self._cmd_saved.get(name)       # 아직 객체가 없으면 옛 기억을 그대로 넘긴다
        sh["commands"] = cmds
        sh["active"] = False
        out["shared"] = sh
        return out

    # ── 1초 행(tick 스레드) ──
    def on_row(self, row: Dict[str, Any]) -> None:
        view = SF.RowView(row)
        self.last_view = view
        self.cache.feed(view, self._all_cols, float(self.clock.monotonic()))
        if self.frozen:
            return
        self._queue(self.snapshot())

    def _queue(self, docs: Dict[str, dict]) -> None:
        enc = {}
        for p, d in docs.items():
            try:
                enc[p] = dumps(d)
            except Exception as e:
                self._err(f"{p} JSON 만들기 실패: {e!r}")
        with self._lk:
            for p, b in enc.items():
                if p not in self.held:
                    self._pending[p] = b
        self._wake.set()

    def _writer(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(self.period_s)
            self._wake.clear()
            if self._stop.is_set():
                return
            self._write_pending()

    def _write_pending(self) -> int:
        with self._io:
            with self._lk:
                items, self._pending = self._pending, {}
            n = 0
            for p, b in items.items():
                try:
                    atomic_write(part_path(self.dir, p), b)
                    n += 1
                    self.writes += 1
                except Exception as e:
                    self._err(f"{p} 저널 쓰기 실패: {e!r}")
            return n

    def flush(self) -> int:
        """지금 상태를 바로(부른 스레드에서) 쓴다 — 시험·종료."""
        self._queue(self.snapshot())
        return self._write_pending()

    def write_part(self, part: str, doc: dict) -> None:
        """한 부분을 그대로 바로 쓴다(재시작 판단 — 재시작 횟수 올림 등). hold 와 무관."""
        with self._io:
            atomic_write(part_path(self.dir, part), dumps(doc))
            self.writes += 1

    # ── 의도적 종료 ──
    def mark_exit(self, why: str) -> None:
        """의도적 종료 — 모든 부분에 exit 를 먼저 적어 바로 쓴다. 그 뒤 1초 쓰기 멈춤(write_final 만)."""
        self.exit = {"intentional": True, "why": str(why), "t_wall": float(self.clock.wall())}
        self.frozen = True
        with self._lk:
            self.held.clear()
        self._queue(self.snapshot())
        self._write_pending()

    def write_final(self) -> None:
        """종료 정리 뒤 마지막 한 번(엔진이 멈춘 상태 + exit)."""
        with self._lk:
            self.held.clear()
        self._queue(self.snapshot())
        self._write_pending()

    def _err(self, msg: str) -> None:
        self.errors += 1
        self.last_error = msg
        if self.on_error is not None:
            try:
                self.on_error(msg)
            except Exception:
                pass
