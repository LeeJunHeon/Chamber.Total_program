"""챔버12 두께 모니터 Factor 파일 — v2 설정 폴더의 데이터 파일 thickness_factors_ch{N}.json(v1 형식 version 2 그대로, 결정 D12).

v1 근거(읽고 다시 씀, import 안 함): lib/thickness_factors.py(전부), lib/thickness_config.py:25-26·149-154(파일 위치),
ui/thickness_factor_dialog.py(Factor 창 검사·문구), ui/thickness_panel.py:37-47(device_write_block).

구조:
  tooling[chuck][gun]  — chuck ∈ up/mid/down, gun ∈ G1/G2/G3, 단위 %(10.0~399). null = 미설정.
  material[gun]        — {"material": 재료 이름} — 재료 목록(thickness_materials)의 name. 빈 문자열 = 미선택.
파일이 없으면 빈 표(경고 없음). 읽기 실패·형식 오류는 빈 표+경고, 틀린 칸은 그 칸만 미설정+경고, 이전 형식(건별 density·z_factor
숫자)은 경고 후 무시. 저장은 원자적(같은 폴더 임시 파일 → fsync → os.replace).
파일 위치: CHn_THICKNESS_FACTOR_FILE 이 비면 설정 폴더/thickness_factors_ch{N}.json, 상대 경로는 설정 폴더 기준, 절대 경로는 그대로
(v1 은 exe 옆 config 폴더 기준 — 변환 도구가 파일을 설정 폴더로 옮긴다, v2/tools/convert_v1_config.py).

Factor 창 명령(v1 ThicknessFactorDialog 와 같은 검사·문구 — 화면 그리기는 포털·hmi 몫)
- factor_window(path, materials)          : tooling 9칸 글자·건별 재료(이름·표시·ρ/Z)·읽기 경고(최대 8줄)·안내
- material_choices(materials)             : 드롭다운 항목 [("(선택 안 함)", ""), ("기호 — 이름", 이름), …]
- chosen(text, materials, gun)            : 고른 글자 → (재료|None, 오류) — 맨 위·빈 칸 = 미선택
- collect(tooling_cells, material_texts, materials) : 저장 검사 → (ThicknessFactors, 오류, 빨간 칸)
- save_window(path, tooling_cells, material_texts, materials) : 저장(장비에 쓰지 않음) → (ok, 문구, 빨간 칸)
- write_values(chuck, gun, tooling_text, material_text, materials) : [장비에 쓰기] 값 검사 → (tooling, 재료, 오류, 빨간 칸)
- device_write_block(st, process_running) : 영점·장비에 쓰기 금지 사유(공정 중·미연결·응답 없음) 또는 ""
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from v2.ch12.drivers.stm100_proto import TOOLING_MAX, TOOLING_MIN
from v2.ch12.thickness_materials import NEXT_RUN_NOTE, Material, atomic_write_json, find_material

CHUCKS = ("up", "mid", "down")
GUNS = ("G1", "G2", "G3")
FILE_VERSION = 2                                       # lib/thickness_factors.py:24
CHUCK_TEXT = {"up": "Up", "mid": "Mid", "down": "Down"}  # ui/thickness_factor_dialog.py:27
NONE_TEXT = "(선택 안 함)"                             # :29
WRITE_NOTE = "장비에 쓰기 = 지금 창의 값을 장비에 바로 적용(저장과 별개). Thickness 공정은 시작 때 그 공정 값으로 다시 씀."  # :30
TOOLING_HEADER = "Tooling (%, 10.0~399) — chuck 위치 × 건, 빈 칸 = 미설정"                   # :61
MATERIAL_HEADER = "재료 — 건별(재료 목록에서 고름, density·Z 는 목록 값)"                     # :71
TITLE_FMT = "CH{ch} Thickness Monitor — Factor 설정"


def _blank_tooling() -> Dict[str, Dict[str, Optional[float]]]:
    return {c: {g: None for g in GUNS} for c in CHUCKS}


def _blank_material() -> Dict[str, Optional[str]]:
    return {g: None for g in GUNS}


@dataclass
class ThicknessFactors:
    tooling: Dict[str, Dict[str, Optional[float]]] = field(default_factory=_blank_tooling)
    material: Dict[str, Optional[str]] = field(default_factory=_blank_material)   # 건 → 재료 이름(None = 미선택)

    def tooling_for(self, chuck: str, gun: str) -> Optional[float]:
        return (self.tooling.get(str(chuck).lower()) or {}).get(str(gun).upper())

    def material_name(self, gun: str) -> Optional[str]:
        return self.material.get(str(gun).upper())

    def to_json(self) -> dict:
        return {
            "version": FILE_VERSION,
            "tooling": {c: {g: self.tooling[c][g] for g in GUNS} for c in CHUCKS},
            "material": {g: {"material": self.material[g] or ""} for g in GUNS},
        }


def factor_file(cfg: Mapping[str, Any], chamber: str, settings_dir) -> Path:
    """CHn_THICKNESS_FACTOR_FILE("" = 설정 폴더/thickness_factors_chN.json, 상대 경로 = 설정 폴더 기준)."""
    n = int(str(chamber).lower().replace("ch", ""))
    raw = str(cfg.get(f"CH{n}_THICKNESS_FACTOR_FILE") or "").strip()
    if not raw:
        return Path(settings_dir) / f"thickness_factors_ch{n}.json"
    p = Path(raw)
    return p if p.is_absolute() else Path(settings_dir) / p


def check_tooling(v) -> Tuple[Optional[float], str]:
    """tooling 한 칸 — (값, 오류문구). 빈값/None 은 (None, ''). lib/thickness_factors.py:55-67."""
    if v is None or (isinstance(v, str) and not v.strip()):
        return None, ""
    if isinstance(v, bool):
        return None, f"{v!r} 숫자가 아님"
    try:
        f = float(v)
    except Exception:
        return None, f"{v!r} 숫자가 아님"
    if f != f or not (TOOLING_MIN <= f <= TOOLING_MAX):
        return None, f"{f:g} 범위({TOOLING_MIN:g}~{TOOLING_MAX:g}%) 밖"
    return f, ""


def load_factors(path) -> Tuple[ThicknessFactors, List[str]]:
    """(ThicknessFactors, 경고 목록) — 예외를 던지지 않는다. lib/thickness_factors.py:70-115."""
    fac = ThicknessFactors()
    warns: List[str] = []
    p = Path(path)
    if not p.exists():
        return fac, warns
    try:
        data = json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception as e:
        warns.append(f"Factor 파일을 읽을 수 없음({p.name}) — 빈 표로 시작: {e!r}")
        return fac, warns
    if not isinstance(data, dict):
        warns.append(f"Factor 파일 형식 오류({p.name}) — 빈 표로 시작")
        return fac, warns
    tool = data.get("tooling") or {}
    if not isinstance(tool, dict):
        warns.append("tooling 이 표(객체)가 아님 — 전부 미설정")
        tool = {}
    for c in CHUCKS:
        row = tool.get(c) or {}
        if not isinstance(row, dict):
            warns.append(f"tooling.{c} 가 표(객체)가 아님 — 그 줄 미설정")
            continue
        for g in GUNS:
            v, err = check_tooling(row.get(g))
            if err:
                warns.append(f"tooling[{c}][{g}] {err} — 미설정")
            fac.tooling[c][g] = v
    mat = data.get("material") or {}
    if not isinstance(mat, dict):
        warns.append("material 이 표(객체)가 아님 — 전부 미선택")
        mat = {}
    for g in GUNS:
        m = mat.get(g) or {}
        if not isinstance(m, dict):
            warns.append(f"material.{g} 가 표(객체)가 아님 — 미선택")
            continue
        if any(m.get(k) not in (None, "") for k in ("density", "z_factor")) and not m.get("material"):
            warns.append(f"material.{g}: 이전 형식(density·Z 숫자)은 쓰지 않음 — [Factor 설정]에서 재료를 다시 고르십시오")
        name = m.get("material")
        fac.material[g] = str(name).strip() if isinstance(name, str) and name.strip() else None
    return fac, warns


def save_factors(path, fac: ThicknessFactors) -> Path:
    """원자적 저장. 실패하면 예외(원본은 그대로)."""
    return atomic_write_json(path, fac.to_json())


# ═══════════════════════ 장비 쓰기 금지 사유(ui/thickness_panel.py:37-47) ═══════════════════════
def device_write_block(st: Optional[Mapping[str, Any]], process_running: bool = False) -> str:
    """영점·재료값 쓰기를 보낼 수 없는 사유 — 공정 중·미연결·응답 없음(stale). 보낼 수 있으면 ''.
    v1 은 상태 dict 의 process_running 을 봤다(v2 드라이버 상태에는 없어 부르는 쪽이 넘긴다 — 둘 중 하나면 공정 중)."""
    st = st or {}
    if st.get("process_running") or process_running:
        return "공정 중"
    if not st.get("connected"):
        return "두께 모니터 미연결"
    if st.get("stale"):
        return "두께 모니터 응답 없음"
    return ""


# ═══════════════════════ Factor 창(ui/thickness_factor_dialog.py) ═══════════════════════
def _cell_text(v) -> str:
    return "" if v is None else f"{float(v):g}"


def material_choices(materials: Sequence[Material]) -> List[Tuple[str, str]]:
    """드롭다운 항목(표시, 이름) — 맨 위 '(선택 안 함)'. 입력 찾기는 표시에 포함·대소문자 무시(화면 몫)."""
    return [(NONE_TEXT, "")] + [(m.label, m.name) for m in materials]


def chosen(text: Optional[str], materials: Sequence[Material], gun: str) -> Tuple[Optional[Material], str]:
    """(재료 | None, 오류문구). 맨 위 항목·빈 칸 = 미선택(None, ''). ThicknessFactorDialog._chosen."""
    t = str(text or "").strip()
    if not t or t == NONE_TEXT:
        return None, ""
    for m in materials:
        if t in (m.label, m.name) or t.casefold() == m.name.casefold():
            return m, ""
    return None, f"{gun}: 재료 '{t}' 가 재료 목록에 없음"


def value_text(text: Optional[str], materials: Sequence[Material], gun: str) -> str:
    """고른 재료 옆 읽기 전용 표시 — 'ρ d · Z z' / '목록에 없음' / '—'(_show_values)."""
    m, err = chosen(text, materials, gun)
    if m is not None:
        return f"ρ {m.density:g} · Z {m.z_factor:.3f}"
    return "목록에 없음" if err else "—"


def factor_window(path, materials: Sequence[Material], material_warnings: Sequence[str] = (), *, ch: int = 2,
                  materials_path: str = "") -> dict:
    """창을 열 때 보일 것 — tooling 칸 글자(chuck × 건), 건별 재료 글자(목록에 없는 저장 이름은 그대로)·표시값·읽기 경고."""
    fac, warns = load_factors(path)
    cells = {c: {g: _cell_text(fac.tooling[c][g]) for g in GUNS} for c in CHUCKS}
    sel: Dict[str, str] = {}
    for g in GUNS:
        name = fac.material[g]
        m = find_material(materials, name) if name else None
        sel[g] = m.name if m is not None else (name or "")
    all_warns = list(warns) + [f"재료 목록: {w}" for w in material_warnings]
    all_warns += [f"{g} 재료 '{fac.material[g]}' 가 재료 목록에 없음 — 다시 고르십시오"
                  for g in GUNS if fac.material[g] and find_material(materials, fac.material[g]) is None]
    warn_text = ""
    if all_warns:
        warn_text = ("⚠ 읽기 경고:\n" + "\n".join(all_warns[:8])
                     + (f"\n… 외 {len(all_warns) - 8}건" if len(all_warns) > 8 else ""))
    return {"title": TITLE_FMT.format(ch=int(ch)), "tooling_header": TOOLING_HEADER, "material_header": MATERIAL_HEADER,
            "tooling": cells, "material": sel,
            "values": {g: value_text(sel[g], materials, g) for g in GUNS},
            "choices": material_choices(materials), "warnings": all_warns, "warning_text": warn_text,
            "info": f"파일: {path}\n재료 목록: {materials_path}\n{NEXT_RUN_NOTE}", "write_note": WRITE_NOTE}


def collect(tooling_cells: Mapping[str, Mapping[str, Any]], material_texts: Mapping[str, Any],
            materials: Sequence[Material]) -> Tuple[ThicknessFactors, List[str], List[str]]:
    """저장 검사(ThicknessFactorDialog.collect) — (Factor, 오류 목록, 빨간 칸 이름 'tooling.mid.G2'·'material.G3')."""
    fac = ThicknessFactors()
    errs: List[str] = []
    bad: List[str] = []
    for c in CHUCKS:
        row = tooling_cells.get(c) or {}
        for g in GUNS:
            raw = row.get(g)
            txt = str(raw).strip() if raw is not None else ""
            v, err = check_tooling(txt)
            if err:
                errs.append(f"tooling {CHUCK_TEXT[c]}/{g}: {err}")
                bad.append(f"tooling.{c}.{g}")
            fac.tooling[c][g] = v
    for g in GUNS:
        m, err = chosen(material_texts.get(g), materials, g)
        if err:
            errs.append(err)
            bad.append(f"material.{g}")
        fac.material[g] = m.name if m is not None else None
    return fac, errs, bad


def save_window(path, tooling_cells, material_texts, materials) -> Tuple[bool, str, List[str]]:
    """[저장](ThicknessFactorDialog._on_save) — 장비에 쓰지 않음(공정 시작 때 씀). (저장했나, 알림, 빨간 칸).
    성공하면 부르는 쪽이 ThicknessEngine.reload_factors() 를 부른다(v1 on_saved)."""
    fac, errs, bad = collect(tooling_cells, material_texts, materials)
    if errs:
        return False, "고칠 칸이 있어 저장하지 않았습니다:\n\n" + "\n".join(errs), bad
    try:
        save_factors(path, fac)
    except Exception as e:
        return False, f"저장 실패(파일은 바뀌지 않음):\n{e!r}", []
    return True, "", []


def write_values(chuck: str, gun: str, tooling_text: Any, material_text: Any,
                 materials: Sequence[Material]) -> Tuple[Optional[float], Optional[Material], List[str], List[str]]:
    """[장비에 쓰기] 값(_write_values) — 고른 chuck×건 칸과 그 건 재료. (tooling, 재료, 오류, 빨간 칸)."""
    errs: List[str] = []
    bad: List[str] = []
    c, g = str(chuck).lower(), str(gun).upper()
    tooling, err = check_tooling("" if tooling_text is None else str(tooling_text).strip())
    if tooling is None:
        errs.append(f"tooling {CHUCK_TEXT.get(c, c)}/{g}: {err or '빈 칸'}")
        bad.append(f"tooling.{c}.{g}")
    mat, merr = chosen(material_text, materials, g)
    if mat is None:
        errs.append(merr or f"{g}: 재료 미선택")
        bad.append(f"material.{g}")
    return tooling, mat, errs, bad
