"""RGA 링크(v2/ch12/drivers/rga_link.py) ↔ 실제 하위 프로세스 워커(v2/ch12/workers/rga_worker --fake).

가짜 RGA100(rga_fake) — 워커의 측정 순서·CSV·응답 JSON 은 실제 코드. 확인: 스캔·CSV(머리글·형식·붙여 쓰기)·srsinst 호출 순서,
재시도(3회·1 s)·실패 문구, 워커 죽음(JSON 없음)·시간 초과 kill·재시작, 계정 없음, bench 거부, 취소, 1초 행,
v1 골든(명령 인자 = v1 명령줄, 이벤트 문구).
"""
from __future__ import annotations

import json
import re
import threading
import time
from pathlib import Path

import pytest

from v2.common.clock import RealClock
from v2.ch12.drivers import rga_link as rl
from v2.ch12.drivers import worker_proc as wp

GOLDEN = json.loads((Path(__file__).parent / "golden" / "dev_rga.json").read_text(encoding="utf-8"))
NET = json.dumps({"ch1": {"user": "tester", "password": "pw-not-real"}, "ch2": {"user": "tester", "password": "pw-not-real"}})


class Rig:
    def __init__(self, tmp: Path, monkeypatch, fake: str = "", writes_allowed: bool = True, chamber: str = "ch1", **cfg):
        monkeypatch.setenv("RGA_FAKE", fake)
        self.calls = tmp / "rga_calls.jsonl"
        monkeypatch.setenv("RGA_FAKE_LOG", str(self.calls))
        self.tmp = tmp
        n = 1 if chamber == "ch1" else 2
        self.cfg = {"RGA_WORKER_FAKE": True, f"CH{n}_RGA_CSV_PATH": str(tmp / "G" / f"Ch.{n}" / "RGA_spectrums.csv"),
                    "LOCAL_FALLBACK_RGA_DIR": str(tmp / "fallback"), "RGA_NET": NET, **cfg}
        self.events = []
        self.link = rl.RgaLink(RealClock(), None, self.cfg, writes_allowed, on_event=self.events.append, chamber=chamber)

    def scan(self, timeout_s=None, wait=60):
        box, ev = {}, threading.Event()
        self.link.scan(timeout_s, on_done=lambda r: (box.update(r=r), ev.set()))
        assert ev.wait(wait), "스캔 결과가 오지 않음"
        return box["r"]

    def ev(self):
        return [(e.kind, e.detail) for e in self.events]

    def calls_list(self):
        return [json.loads(x)["call"] for x in self.calls.read_text(encoding="utf-8").splitlines()] if self.calls.exists() else []


@pytest.fixture
def rigs(tmp_path, monkeypatch):
    made = []

    def make(**kw):
        r = Rig(tmp_path, monkeypatch, **kw)
        made.append(r)
        return r
    yield make
    for r in made:
        r.link.close()


def _norm_ev(kind: str, msg: str, base: str, primary: str) -> tuple:
    msg = msg.replace(primary, "<PRIMARY>").replace(base, "<BASE>")
    msg = re.sub(r"^(RGA worker attempt \d+/\d+ start): .*$", r"\1", msg)
    return kind, msg


def _golden_ev(name: str) -> list:
    g = GOLDEN["events"][name]
    prim = GOLDEN["settings"]["RGA_CSV_PATH"]["ch1"]
    return [_norm_ev(e["kind"], e["message"], "<ROOT>/Logs_LocalFallback/RGA", prim) for e in g]


def _v2_ev(r: Rig) -> list:
    prim, fb = r.link.csv_paths()
    return [_norm_ev(k, d, str(Path(fb).parent.parent), prim) for k, d in r.ev()]


# ───────── 스캔 ─────────
def test_scan_ok_csv_and_srsinst_call_order(rigs):
    r = rigs()
    res = r.scan()
    assert res.ok and res.message == "RGA scan finished (attempt 1/3)" and len(res.pressures) == 65
    assert res.mass_axis[:3] == [1.0, 2.0, 3.0] and res.meta["ip"] == "192.168.1.20" and res.meta["csv_ok"]
    assert r.calls_list() == ["RGA100", "filament.turn_on", "scan.get_histogram_scan", "filament.turn_off",
                              "scan.get_partial_pressure_corrected_spectrum", "scan.get_mass_axis",
                              "filament.turn_off", "disconnect"]                      # v1 그대로(필라멘트 OFF 두 번)
    first = json.loads(r.calls.read_text(encoding="utf-8").splitlines()[0])
    assert first["args"] == ["tcpip", "192.168.1.20", "tester", "***"]
    mass_call = json.loads(r.calls.read_text(encoding="utf-8").splitlines()[5])
    assert mass_call["args"] == [{"for_analog_scan": False}]
    assert r.scan().ok                                                                  # 같은 워커로 두 번째
    raw = (r.tmp / "G" / "Ch.1" / "RGA_spectrums.csv").read_bytes()
    assert raw.startswith(b"\xef\xbb\xbf")                                             # utf-8-sig
    lines = raw.decode("utf-8-sig").splitlines()
    assert lines[0] == "Time," + ",".join(f"Mass {i}" for i in range(1, 66))
    assert len(lines) == 3 and re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", lines[1].split(",")[0])
    assert lines[1].split(",")[1:3] == ["1.00E-09", "2.00E-09"]                    # %.2E
    assert len(r.link.spawned) == 1                                                     # 상주 워커 하나
    assert _v2_ev(r)[:4] == _golden_ev("ok.ch1")
    assert all("pw-not-real" not in json.dumps(m) for m in r.link.sent)                # 비밀번호는 기록에 남기지 않음
    row = r.link.source_read(None, 0)
    assert row["ch1.rga.running"] is True and row["ch1.rga.measuring"] is False and row["ch1.rga.sample_age_s"] is not None


def test_fail_retries_three_times_like_v1(rigs):
    r = rigs(fake="fail=scan")
    t0 = time.monotonic()
    res = r.scan()
    assert not res.ok and res.reason == "RGA_FAILED" and res.message == "TimeoutError: fake scan timeout"
    assert 2.0 <= time.monotonic() - t0 < 20                                           # 재시도 사이 1 s × 2
    assert _v2_ev(r) == _golden_ev("fail.ch1")
    failed = [e for e in r.events if e.kind == "failed"][0]
    assert sorted(failed.data) == [k for k in GOLDEN["events"]["fail.ch1"] if k["kind"] == "failed"][0]["keys"]
    assert r.calls_list().count("filament.turn_off") == 3                              # 예외에도 finally 에서 OFF


def test_worker_death_no_json_respawns(rigs):
    r = rigs(fake="die=1")
    res = r.scan()
    assert not res.ok and res.message == "RGA worker stdout JSON parse failed"
    assert _v2_ev(r) == _golden_ev("nojson.ch1")
    assert len(r.link.spawned) == 3                                                     # 시도마다 새로 띄움
    failed = [e for e in r.events if e.kind == "failed"][0]
    assert sorted(failed.data) == [k for k in GOLDEN["events"]["nojson.ch1"] if k["kind"] == "failed"][0]["keys"]


def test_timeout_kills_worker_and_next_attempt_restarts(rigs):
    r = rigs(fake="hang=1", RGA_MAX_ATTEMPTS=2, RGA_RETRY_DELAY_S=0.0)
    t0 = time.monotonic()
    res = r.scan(timeout_s=1.0)
    assert not res.ok and res.message == "RGA worker timeout (1.0s)"
    assert time.monotonic() - t0 < 15
    assert len(r.link.spawned) == 2 and r.link.source_read(None, 0)["ch1.rga.running"] is False


def test_csv_fallback_and_csv_fail_keep_measurement(rigs, tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("x")
    r = rigs(CH1_RGA_CSV_PATH=str(blocker / "RGA_spectrums.csv"))                      # 정본 폴더를 만들 수 없음
    res = r.scan()
    assert res.ok and res.meta["csv_fallback_used"]
    fb = r.link.csv_paths()[1]
    assert ("status", f"[RGA] CSV 정본 저장 실패 → 로컬 폴백에 저장: {fb}") in r.ev()
    assert Path(fb).exists()
    r2 = rigs(CH1_RGA_CSV_PATH=str(blocker / "a.csv"), LOCAL_FALLBACK_RGA_DIR=str(blocker / "fb"))
    res2 = r2.scan()
    assert res2.ok and not res2.meta["csv_ok"]
    msg = [d for k, d in r2.ev() if k == "status" and d.startswith("[RGA] CSV 저장 실패")]
    assert msg and msg[0].startswith(f"[RGA] CSV 저장 실패(primary={blocker / 'a.csv'}): primary(") and msg[0].endswith("→ fallback=실패")


def test_no_credentials_fails_without_touching_device(rigs):
    r = rigs(RGA_NET="", RGA_MAX_ATTEMPTS=1)
    res = r.scan()
    assert not res.ok and "RGA 계정 설정 없음" in res.message
    assert r.calls_list() == []


def test_bench_rejects_and_cancel(rigs):
    b = rigs(writes_allowed=False)
    res = b.scan()
    assert not res.ok and res.reason == "BENCH_READ_ONLY" and b.link.spawned == []
    r = rigs(fake="hang=1")
    box, ev = {}, threading.Event()
    r.link.scan(30.0, on_done=lambda x: (box.update(r=x), ev.set()))
    assert _wait(lambda: r.link.scanning and r.link._h is not None and r.link._h.alive(), 10)
    time.sleep(0.5)
    r.link.cancel()
    assert ev.wait(10) and box["r"].reason == "CANCELLED"
    assert not any(k in ("failed", "finished") for k, _ in r.ev())                     # v1: 취소는 이벤트 없이 끝
    assert r.link.source_read(None, 0)["ch1.rga.running"] is False


def _wait(pred, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


# ───────── v1 골든: 명령 인자 ─────────
def test_golden_scan_arguments_equal_v1_argv():
    for ch in (1, 2):
        link = rl.RgaLink(RealClock(), None, {}, True, chamber=f"ch{ch}")
        pri, fb = link.csv_paths()
        cmd = link.scan_command(7, float(GOLDEN["settings"]["RGA_WORKER_TIMEOUT_S"]), pri, fb)
        v1 = [s["argv"] for s in GOLDEN["spawns"] if s["mode"] == "ok" and s["argv"][1] == str(ch)][0]
        v1d = dict(zip(v1[0::2], v1[1::2]))
        v2d = {"--ch": str(cmd["ch"]), "--timeout": str(cmd["timeout"]), "--csv": cmd["csv"],
               "--csv_fallback": cmd["csv_fallback"].replace(str(wp.REPO_ROOT), "<ROOT>")}
        assert v2d == v1d
        assert cmd["user"] == "" and cmd["password"] == ""                             # 비밀값은 설정에만(기본 없음)
    assert rl.step_timeout_ms({}) == GOLDEN["step_timeout_ms"] == 187000
    assert rl.blocks("ch1")[0].columns() == ["ch1.rga.q", "ch1.rga.age_s", "ch1.rga.running", "ch1.rga.measuring",
                                             "ch1.rga.sample_age_s"]
