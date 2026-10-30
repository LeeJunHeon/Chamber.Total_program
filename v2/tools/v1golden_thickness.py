"""v1 두께 판정·Factor·재료 파일 코드를 실제로 실행해 골든을 만든다 → v2/tests/golden/thickness.json.

실행: cd <저장소 루트> && python3 v2/tools/v1golden_thickness.py

- v1 모듈은 importlib 로 동적으로 부른다(v2 안에서 v1 을 import 문으로 쓰지 않는다 — test_isolation).
  controller/thickness_process.py(ZeroConfirm·ThicknessEndJudge), lib/thickness_config.py(ThicknessJudgeParams),
  lib/thickness_factors.py(check_tooling·load_factors), lib/thickness_materials.py(default_materials·check_material_fields·
  load_materials), 배포 파일 config/thickness_materials.json·thickness_factors_ch2.json(내용 그대로).
- judge : 시나리오(입력 = 상태 dict 와 시각 목록 — 골든에 함께 저장)마다 tick 결과(이벤트)·로그·끝 상태.
- zero  : ZeroConfirm 시나리오 — 결과가 나온 tick 번호와 (기준점, 로그, 경고).
- files : 파일 읽기 규칙 — 입력 글자 → 표·경고.
시험(v2/tests/test_thickness_files.py·test_thickness_judge_golden 부분)은 v1 을 import 하지 않고 이 골든만 읽는다.
"""
from __future__ import annotations

import dataclasses
import importlib
import json
import os
import random
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "v2" / "tests" / "golden" / "thickness.json"


def _load_v1():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    return (importlib.import_module("controller.thickness_process"), importlib.import_module("lib.thickness_config"),
            importlib.import_module("lib.thickness_factors"), importlib.import_module("lib.thickness_materials"))


def _st(seq, s, **kw):
    d = {"seq": seq, "thickness_nm": s, "rate_nm_s": None, "crystal_ok": True, "reset_seen": 0, "stale": False,
         "link_ok": True}
    d.update(kw)
    return d


def _ramp(rate, n, dt=0.5, t0=0.0, s0=0.0, seq0=0, **kw):
    out, t, s, seq = [], t0, s0, seq0
    for _ in range(n):
        t += dt
        s += rate * dt
        seq += 1
        out.append([round(t, 6), _st(seq, round(s, 6), rate_nm_s=rate, **kw)])
    return out


def _scenarios():
    sc = []
    # 1) 2번 연속 — 늦은 응답(주파수)이 두께로 읽혀 한 값이 튐 → 종료 안 함, 그 뒤 2번 연속 목표 이상 → 종료
    ticks = _ramp(0.5, 60)                                          # 30 nm @ 30 s
    t, seq = ticks[-1][0], 60
    ticks.append([t + 0.5, _st(seq + 1, 592207.9, rate_nm_s=0.5)])
    ticks += _ramp(0.5, 90, t0=t + 0.5, s0=30.25, seq0=seq + 1)        # 52.75 nm 까지
    sc.append(dict(name="consecutive2_spike", target=50.0, dep=0.5, base=0.0, p={"done_consecutive": 2}, ticks=ticks))
    # 2) 새 값 없음 → 시간 대체 → 복귀(첫 값 목표 이상 — 다음 값으로 확인) → 종료
    ticks = _ramp(0.1, 10)
    t = ticks[-1][0]
    ticks += [[t + k, _st(10, 1.0)] for k in range(1, 13)]
    ticks += [[t + 12.5, _st(11, 2.6)], [t + 13.0, _st(12, 2.7)]]
    for c in (1, 2):
        sc.append(dict(name=f"stale_resume_c{c}", target=2.5, dep=0.1, base=0.0, p={"done_consecutive": c}, ticks=ticks))
    # 3) XTAL FAIL(값 없음·link_ok) → 영구, 4) 리셋 플래그 → 영구
    ticks = _ramp(0.2, 8) + [[4.5, _st(8, 1.6, stale=True, crystal_ok=False)], [5.0, _st(9, 3.0)]]
    sc.append(dict(name="xtal_fail", target=2.0, dep=0.2, base=0.0, p={}, ticks=ticks))
    ticks = _ramp(0.2, 8) + [[4.5, _st(9, 1.8, reset_seen=1)], [5.0, _st(10, 3.0, reset_seen=1)]]
    sc.append(dict(name="reset_flag", target=2.0, dep=0.2, base=0.0, p={}, ticks=ticks))
    # 5) 40 s 실측 뒤 끊김 — 실측 증착률로 남은 시간, 6) 실측이 레시피 3배 → 레시피
    for name, dep in (("measured_rate", 0.2), ("rate_out_of_range", 0.05)):
        ticks = _ramp(0.2, 80)
        t = ticks[-1][0]
        ticks += [[t + k, _st(80, 16.0)] for k in range(1, 13)]
        sc.append(dict(name=name, target=40.0, dep=dep, base=1.5, p={}, ticks=[[a, dict(b, thickness_nm=b["thickness_nm"] + 1.5)]
                                                                            for a, b in ticks]))
    # 7) 기록만 경고 — 정체 65 s, 급등, 큰 튐, 음수
    ticks = [[0.5 * (i + 1), _st(i + 1, 0.0, rate_nm_s=0.0)] for i in range(130)]
    ticks += [[65.5, _st(131, 0.05, rate_nm_s=1.0)], [66.0, _st(132, 2.05, rate_nm_s=0.1)], [66.5, _st(133, -3.0)]]
    sc.append(dict(name="warnings", target=100.0, dep=0.1, base=0.0, p={}, ticks=ticks))
    # 8) 기준점 없음 — 처음부터 영구 시간 대체
    sc.append(dict(name="no_base", target=100.0, dep=0.5, base=None, p={}, ticks=_ramp(1.0, 4)))
    # 9) 잡음(분해능 0.1 nm, 간격 0.5±0.1 s) — 입력을 그대로 저장
    rng = random.Random(1234)
    ticks, t, seq = [], 0.0, 0
    while t < 110:
        t += 0.5 + rng.uniform(-0.1, 0.1)
        seq += 1
        ticks.append([round(t, 6), _st(seq, round(round((1.0 * t + rng.gauss(0, 0.2)) / 0.1) * 0.1, 6), rate_nm_s=1.0)])
    sc.append(dict(name="noise_c2", target=100.0, dep=1.0, base=0.0, p={"done_consecutive": 2}, ticks=ticks))
    return sc


def _zero_scenarios():
    def vals(v, dt=0.5, n=14, gap_after=None):
        out, seq = [], 0
        for i in range(n):
            t = round(dt * (i + 1), 6)
            if gap_after is not None and seq >= gap_after:
                out.append([t, {"seq": seq, "thickness_nm": None}])
            else:
                seq += 1
                out.append([t, {"seq": seq, "thickness_nm": v[min(seq - 1, len(v) - 1)]}])
        return out
    return [dict(name="immediate", zero_ok=True, ticks=vals([0.0])),
            dict(name="old_then_zero", zero_ok=True, ticks=vals([350.0, 350.0, 0.1])),
            dict(name="negative", zero_ok=True, ticks=vals([-3.0])),
            dict(name="not_confirmed", zero_ok=True, ticks=vals([50.0, 50.1, 49.9, 50.2])),
            dict(name="cmd_failed", zero_ok=False, ticks=vals([350.0, 0.0])),
            dict(name="no_values", zero_ok=True, ticks=vals([0.0], gap_after=0)),
            dict(name="lost_after_one", zero_ok=True, ticks=vals([350.0], gap_after=1)),
            dict(name="xtal_fail", zero_ok=True, ticks=[[0.1, {"seq": 0, "thickness_nm": None, "crystal_ok": False,
                                                               "link_ok": True, "stale": True}]])]


def _ev(e):
    if e is None:
        return None
    return {"kind": e.kind, "reason": e.reason, "remaining_s": e.remaining_s, "permanent": e.permanent,
            "eff_nm": e.eff_nm, "has_detail": bool(e.detail)}


def main() -> None:
    tp, tc, tf, tm = _load_v1()
    P = tc.ThicknessJudgeParams()
    out = {"source": "v1 실행 — v2/tools/v1golden_thickness.py", "judge": [], "zero": [], "files": {}}
    for sc in _scenarios():
        p = dataclasses.replace(P, **sc["p"])
        j = tp.ThicknessEndJudge(sc["target"], sc["dep"], p, start_t=0.0, base=sc["base"], seq0=0, reset0=0)
        start = _ev(j.start_event())
        evs = [_ev(j.tick(st, t)) for t, st in sc["ticks"]]
        out["judge"].append(dict(sc, start=start, events=evs, logs=list(j.events),
                                 final={"eff": j.eff, "done_n": j.done_n, "jump_count": j.jump_count, "mode": j.mode,
                                        "permanent": j.permanent, "fallback_count": j.fallback_count,
                                        "avg_rate": j.avg_rate(), "use_rate": list(j.use_rate())}))
    for sc in _zero_scenarios():
        zc = tp.ZeroConfirm(P, start_t=0.0, seq0=0, zero_ok=sc["zero_ok"])
        res = None
        for i, (t, st) in enumerate(sc["ticks"]):
            r = zc.tick(st, t)
            if r is not None:
                res = {"index": i, "base": r.base, "log": r.log, "warning": r.warning}
                break
        out["zero"].append(dict(sc, result=res))
    f = out["files"]
    f["materials_default"] = [dataclasses.asdict(m) for m in tm.default_materials()]
    f["materials_shipped"] = json.loads((ROOT / "config" / "thickness_materials.json").read_text(encoding="utf-8-sig"))
    f["factors_ch2_shipped"] = json.loads((ROOT / "config" / "thickness_factors_ch2.json").read_text(encoding="utf-8-sig"))
    f["check_tooling"] = [[repr(v), list(tf.check_tooling(v))] for v in (None, "", " ", "120", 120.5, 5, 400, "abc", True,
                                                                          10, 399, "10.0", float("nan"))]
    f["check_material"] = []
    for args in (("Al", "Al", 2.73, 1.08), ("", "X", 2.0, 1.0), ("L", "L", 0.1, 1.0), ("Z", "Z", 2.0, 12.0),
                 ("N", "N", "abc", 1.0), ("B", "B", True, None), (" Cu ", " Cu ", "8.93", "0.437")):
        m, e = tm.check_material_fields(*args)
        f["check_material"].append([list(map(repr, args)), None if m is None else dataclasses.asdict(m), e])
    cases = {
        "invalid": json.dumps({"tooling": {"up": {"G1": 120.5, "G2": 5, "G3": "abc"}, "mid": {"G1": 400}, "down": "x"},
                               "material": {"G1": {"material": "Aluminum Oxide"},
                                            "G2": {"density": 2.7, "z_factor": 1.08, "memo": "Al"}, "G3": "x"}}),
        "broken": "{ broken", "list": "[1, 2]", "tooling_not_obj": json.dumps({"tooling": 3, "material": []}),
    }
    f["load_factors"] = {}
    with tempfile.TemporaryDirectory() as tmp:
        for k, text in cases.items():
            pth = Path(tmp) / f"{k}.json"
            pth.write_text(text, encoding="utf-8")
            fac, w = tf.load_factors(pth)
            f["load_factors"][k] = {"text": text, "tooling": fac.tooling, "material": fac.material,
                                    "warns": [x.replace(tmp, "<tmp>") for x in w]}
        mcases = {"invalid": json.dumps({"materials": [
            {"name": "Good", "symbol": "G", "density": 2.0, "z_factor": 1.0, "note": "ok"},
            {"name": "", "symbol": "X", "density": 2.0, "z_factor": 1.0},
            {"name": "LowDensity", "symbol": "L", "density": 0.1, "z_factor": 1.0},
            {"name": "BigZ", "symbol": "Z", "density": 2.0, "z_factor": 12.0},
            {"name": "NotNum", "symbol": "N", "density": "abc", "z_factor": 1.0},
            {"name": "good", "symbol": "g", "density": 3.0, "z_factor": 1.0}, "string"]}),
            "broken": "{ broken", "no_list": json.dumps({"materials": 5})}
        f["load_materials"] = {}
        for k, text in mcases.items():
            pth = Path(tmp) / f"m_{k}.json"
            pth.write_text(text, encoding="utf-8")
            mats, w = tm.load_materials(pth)
            f["load_materials"][k] = {"text": text, "names": [m.name for m in mats], "notes": [m.note for m in mats],
                                      "warns": [x.replace(tmp, "<tmp>") for x in w]}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print("written", OUT, f"judge {len(out['judge'])} · zero {len(out['zero'])}")


if __name__ == "__main__":
    main()
