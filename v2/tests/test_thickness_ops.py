"""ThicknessEngine 명령(패널 [영점]·Factor 창 [장비에 쓰기]·[저장]·재료 목록 [저장])과 저널·재시작 이어가기(v2).

명령 기대값은 v1 시험 값(tests/test_thickness_round14.py:183-220 [영점] 재검사, test_thickness_panel.py:277-330 [장비에 쓰기]).
이어가기는 v2 기능(지시 3장 (6)) — THICKNESS_WAIT 중에만, 확인 목록은 thickness_ops.resume_checks.
도우미(Rig·가짜 드라이버·시계)는 test_thickness.py 것을 쓴다.
"""
import pytest

from v2.tests.test_thickness import Rig, _waiting


# ═══════════════════════ 명령(v1 round14:183-220, panel:277-330) ═══════════════════════
def test_cmd_zero_rechecks_block(tmp_path):
    r = Rig(tmp_path)
    out = []
    r.e.cmd_zero(True, lambda ok, t: out.append((ok, t)))
    assert out == [(False, "공정 중에는 영점을 할 수 없습니다.")] and r.d.zero_calls == []
    r.d.st["stale"] = True
    r.e.cmd_zero(False, lambda ok, t: out.append((ok, t)))
    assert out[-1] == (False, "두께 모니터 응답 없음 — 영점을 보내지 않았습니다.")
    r.d.st["stale"] = False
    r.e.cmd_zero(False, lambda ok, t: out.append((ok, t)))
    assert out[-1] == (True, "") and r.d.zero_calls == [("B", "패널")]
    r.d.zero_ok = False
    r.e.cmd_zero(False, lambda ok, t: out.append((ok, t)))
    assert out[-1] == (False, "영점 실패: 응답 없음 'B' (3회)")


def test_factor_write_uses_window_values_without_saving(tmp_path):
    r = Rig(tmp_path)
    fpath = tmp_path / "thickness_factors_ch2.json"
    before = fpath.read_bytes()
    out = []
    r.e.factor_write("mid", "G2", "120", "Titanium", False, out.append)
    assert r.d.mat_calls == [(4.5, 0.628, 120.0, "Titanium", "Factor 창 수동")] and fpath.read_bytes() == before
    assert out[-1]["ok"] and out[-1]["text"] == "장비에 적용됨 — Titanium · density 4.5 · Z 0.628 · tooling 120%"
    r.e.factor_write("mid", "G2", "5", "Titanium", False, out.append)
    assert not out[-1]["ok"] and "장비에 쓰지 않았습니다" in out[-1]["text"] and out[-1]["bad"] == ["tooling.mid.G2"]
    for st, running, word in ((dict(r.d.st), True, "공정 중"), (dict(r.d.st, connected=False), False, "미연결"),
                              (dict(r.d.st, stale=True), False, "응답 없음")):
        r.d.st = st
        r.e.factor_write("mid", "G2", "120", "Titanium", running, out.append)
        assert word in out[-1]["text"] and len(r.d.mat_calls) == 1
    r.d.st.update(connected=True, stale=False)
    r.d.mat_error = "되읽기 불일치 — J 되읽기 불일치 120 ≠ 121"
    r.e.factor_write("mid", "G2", "120", "Titanium", False, out.append)
    assert out[-1]["text"] == "장비에 쓰지 못했습니다(되읽기 확인 포함): J 되읽기 불일치 120 ≠ 121"


def test_factor_and_material_save_reload(tmp_path):
    r = Rig(tmp_path)
    cells = {c: {g: "" for g in ("G1", "G2", "G3")} for c in ("up", "mid", "down")}
    cells["up"]["G2"] = "150"
    ok, text, bad = r.e.factor_save(cells, {"G1": "Titanium", "G2": "", "G3": ""})
    assert ok and r.e.factors.tooling["up"]["G2"] == 150.0 and r.e.factors.material["G1"] == "Titanium"
    assert ("THICKNESS", "Factor 파일·재료 목록 다시 읽음") in r.side
    rows = [["Aluminum", "Al", "2.73", "1.08", ""]]                     # Titanium 을 뺌 — Factor G1 이 쓰는 재료
    res = r.e.materials_save(rows, used_names=["Titanium"])
    assert not res["ok"] and "Titanium" in res["confirm"] and len(r.e.materials) == 2
    res = r.e.materials_save(rows, used_names=["Titanium"], confirmed=True)
    assert res["ok"] and [m.name for m in r.e.materials] == ["Aluminum"]
    assert r.e.factor_window()["values"]["G1"] == "목록에 없음"
    assert r.e.materials_window()["rows"] == [["Aluminum", "Al", "2.73", "1.08", ""]]


def test_load_warnings_logged_at_start(tmp_path):
    (tmp_path / "thickness_factors_ch2.json").write_text("{ broken", encoding="utf-8")
    r = Rig(tmp_path, factors=False, cfg={"CH2_THICKNESS_DONE_CONSECUTIVE": 0})
    side = [m for _s, m in r.side]
    assert side[0] == "⚠ Factor 파일: Factor 파일을 읽을 수 없음(thickness_factors_ch2.json) — 빈 표로 시작: " \
                      "JSONDecodeError('Expecting property name enclosed in double quotes: line 1 column 3 (char 2)')"
    assert "⚠ 두께 판정 설정: THICKNESS_DONE_CONSECUTIVE=0 범위(1~10) 밖 → 1" in side
    assert (tmp_path / "thickness_materials.json").exists() and len(r.e.materials) == 99   # 없으면 v1 기본 99종 생성


# ═══════════════════════ 저널·이어가기(v2) ═══════════════════════
def test_journal_resume_continues_wait_with_gap(tmp_path):
    r = _waiting(tmp_path, cfg={"CH2_THICKNESS_DONE_CONSECUTIVE": 2})
    r.e.run_started({"thickness_mode": True, "thickness": 10.0, "dep_rate": 0.05})
    r.e.wait_start(10.0, 200.0, 500.0, r.ends.append)
    for i in range(20):
        r.feed(0.2 * (i + 1))                                   # 4.0 nm @ 10 s
    st = r.e.state_for_journal()
    assert st["phase"] == "wait" and st["wait"]["elapsed_s"] == pytest.approx(10.0) and st["judge"]["eff"] == pytest.approx(4.0)
    # 재시작: 새 시계·새 드라이버(seq 처음부터), 공백 20 s 동안 장비는 계속 증착
    r2 = _waiting(tmp_path, cfg={"CH2_THICKNESS_DONE_CONSECUTIVE": 2})
    r2.c.t = 5.0
    r2.d.st.update(seq=1, thickness_nm=8.1, ok_ts=5.0, age_s=0.2)
    checks = r2.e.resume_checks(st)
    assert all(c["ok"] for c in checks) and {c["name"] for c in checks} >= {"thickness.link", "thickness.counter"}
    r2.e.resume(st, 20.0, r2.ends.append)
    w = r2.e._wait
    assert w["start_t"] == pytest.approx(5.0 - 30.0) and w["limit_t"] == pytest.approx(5.0 - 30.0 + 500.0)
    r2.feed(10.0)
    r2.feed(10.1)
    assert [e.end for e in r2.ends] == ["Thickness"] and r2.ends[0].elapsed_s == pytest.approx(31.0)
    assert any(m.startswith("두께 판정 이어감 — 공백 20초") for m in r2.msgs("THICKNESS"))


@pytest.mark.parametrize("mut,name", [
    (lambda s: s.update(link_ok=False), "thickness.link"), (lambda s: s.update(crystal_ok=False), "thickness.crystal"),
    (lambda s: s.update(reset_seen=1), "thickness.reset"), (lambda s: s.update(age_s=9.0), "thickness.fresh"),
    (lambda s: s.update(thickness_nm=0.5), "thickness.counter"),
])
def test_resume_checks_fail(tmp_path, mut, name):
    r = _waiting(tmp_path)
    r.e.wait_start(10.0, 200.0, 500.0, r.ends.append)
    r.feed(4.0)
    st = r.e.state_for_journal()
    mut(r.d.st)
    bad = [c["name"] for c in r.e.resume_checks(st) if not c["ok"]]
    assert bad == [name]


def test_resume_checks_zero_phase_and_time_only(tmp_path):
    r = Rig(tmp_path)
    r.d.hold = True
    r.zero(5.0)
    assert [c["ok"] for c in r.e.resume_checks(r.e.state_for_journal())] == [False]
    n = Rig(tmp_path, driver=False)
    n.e.wait_start(10.0, 50.0, 100.0, n.ends.append)
    s = n.e.state_for_journal()
    assert s["judge"] is None and all(c["ok"] for c in n.e.resume_checks(s))
    n2 = Rig(tmp_path, driver=False)
    n2.e.resume(s, 10.0, n2.ends.append)
    n2.c.t += 40.0
    n2.e.poll()
    assert [e.end for e in n2.ends] == ["시간대체"] and n2.ends[0].elapsed_s == pytest.approx(50.0)
