"""챔버12 두께 모니터 재료 목록 — v2 설정 폴더의 데이터 파일 thickness_materials.json(v1 형식 version 1 그대로, 결정 D12).

v1 근거(읽고 다시 씀, import 안 함): lib/thickness_materials.py(전부), ui/thickness_material_dialog.py(재료 목록 창 검사·문구).

항목: name(영문 이름, 재료를 구별하는 키 — 대소문자 무시 중복 금지), symbol(화학식), density(g/cm3, 0.500~99.99),
      z_factor(0.100~9.999), note(선택). 화면 표시 = "기호 — 이름".
처음 내용은 STM-100 매뉴얼 4.3 Material Reference Table 그대로 99종(Z 가 '-' 인 18종은 1.000, note 에 그 사실 — v1 _MANUAL_TABLE).
파일이 없으면 기본 목록으로 새로 만든다. 읽을 수 없으면 기본 목록 + 경고(파일은 건드리지 않음). 잘못된 항목·이름 중복(뒤쪽)은
경고 후 그 항목만 뺀다. 저장은 같은 폴더 임시 파일(utf-8, LF, indent 2, ensure_ascii=False) → fsync → os.replace(원자적).

재료 목록 창 명령(v1 ThicknessMaterialDialog 와 같은 검사·문구 — 화면 그리기는 포털·hmi 몫)
- materials_window(path)            : 표 행·읽기 경고(최대 6줄 + '… 외 n건')·안내 문구
- collect_rows(rows)                 : 저장 검사(완전히 빈 행 무시, 범위·이름 빈칸·이름 중복) → (재료 목록, 오류, 빨간 칸)
- delete_confirm_text(names, used)   : Factor 에서 쓰는 재료를 지울 때 확인 문구(없으면 "")
- save_rows(path, rows)              : 검사 통과면 원자적 저장 → (ok, 문구, 빨간 칸)
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from v2.ch12.drivers.stm100_proto import DENSITY_MAX, DENSITY_MIN, Z_MAX, Z_MIN

FILE_NAME = "thickness_materials.json"
FILE_VERSION = 1                                   # lib/thickness_materials.py:23
Z_MISSING_NOTE = "매뉴얼 Z 미기재 — 1.000 사용"    # lib/thickness_materials.py:24
COLS = ("이름", "기호", "density (g/cm3)", "Z-factor", "note")                    # ui/thickness_material_dialog.py:20
NEXT_RUN_NOTE = "저장하면 다음 공정 시작 때 적용됩니다(진행 중인 공정에는 영향 없음)."   # ui/thickness_material_dialog.py:22
HEADER_TEXT = "재료 목록 — 이름(재료를 구별하는 키, 중복 불가)·기호·density 0.500~99.99·Z-factor 0.100~9.999"  # :43
TITLE = "Thickness Monitor — 재료 목록"

# STM-100 매뉴얼 4.3 Material Reference Table — 이름|기호|density|Z ('-' = 매뉴얼 Z 미기재). lib/thickness_materials.py:27-54 그대로
_MANUAL_TABLE = """
Aluminum|Al|2.73|1.080; Aluminum Oxide|Al2O3|3.97|-; Antimony|Sb|6.62|0.768; Arsenic|As|5.73|0.966;
Barium|Ba|3.50|2.100; Beryllium|Be|1.85|0.543; Bismuth|Bi|9.80|0.790; Bismuth Oxide|Bi2O3|8.90|-;
Boron|B|2.54|0.389; Cadmium|Cd|8.64|0.682; Cadmium Selenide|CdSe|5.81|-; Cadmium Sulfide|CdS|4.83|1.020;
Cadmium Telluride|CdTe|5.85|0.980; Calcium|Ca|1.55|2.620; Calcium Fluoride|CaF2|3.18|0.775;
Carbon (Diamond)|C|3.52|0.220; Carbon (Graphite)|C|2.25|3.260; Cerium (III) Fluoride|CeF3|6.16|-;
Cerium (IV) Oxide|CeO2|7.13|-; Chromium|Cr|7.20|0.305; Chromium (III) Oxide|Cr2O3|5.21|-; Cobalt|Co|8.71|0.343;
Copper|Cu|8.93|0.437; Copper (I) Sulfide (A)|Cu2S (A)|5.60|0.690; Copper (I) Sulfide (B)|Cu2S (B)|5.80|0.670;
Copper (III) Sulfide|CuS|4.60|0.820; Dysprosium|Dy|8.54|0.600; Erbium|Er|9.05|0.740; Gadolinium|Gd|7.89|0.670;
Gallium|Ga|5.93|0.593; Gallium Arsenide|GaAs|5.31|1.590; Germanium|Ge|5.35|0.516; Gold|Au|19.3|0.381;
Hafnium|Hf|13.1|0.360; Hafnium Oxide|HfO2|9.63|-; Holmium|Ho|8.80|0.580; Indium|In|7.30|0.841;
Indium Antimonide|InSb|5.76|0.769; Indium Oxide|In2O3|7.18|-; Iridium|Ir|22.4|0.129; Iron|Fe|7.86|0.349;
Lanthanum|La|6.17|0.920; Lanthanum Fluoride|LaF3|5.94|-; Lanthanum Oxide|LaO3|6.51|-; Lead|Pb|11.3|1.130;
Lead Sulfide|PbS|7.50|0.566; Lithium|Li|0.53|5.900; Lithium Fluoride|LiF|2.64|0.774; Magnesium|Mg|1.74|1.610;
Magnesium Fluoride|MgF2|3.00|-; Magnesium Oxide|MgO|3.58|0.411; Manganese|Mn|7.20|0.377; Molybdenum|Mo|10.2|0.257;
Neodymium Fluoride|NdF3|6.506|-; Neodymium Oxide|Nd2O3|7.24|-; Nickel|Ni|8.91|0.331; Niobium|Nb|8.57|0.493;
Niobium (V) Oxide|Nb2O5|4.47|-; Palladium|Pd|12.0|0.357; Platinum|Pt|21.4|0.245; Potassium Chloride|KCl|1.98|2.050;
Rhenium|Re|21.04|0.150; Rhodium|Rh|12.41|0.210; Rubidium|Rb|1.53|2.540; Samarium|Sm|7.54|0.890;
Scandium|Sc|3.00|0.910; Selenium|Se|4.82|0.864; Silicon|Si|2.32|0.712; Silicon (II) Oxide|SiO|2.13|0.870;
Silicon Dioxide|SiO2|2.20|1.070; Silver|Ag|10.5|0.529; Silver Bromide|AgBr|6.47|1.180; Silver Chloride|AgCl|5.56|1.320;
Sodium|Na|0.97|4.800; Sodium Chloride|NaCl|2.17|1.570; Sulfur|S|2.07|2.290; Tantalum|Ta|16.6|0.262;
Tantalum (V) Oxide|Ta2O5|8.20|0.300; Tellurium|Te|6.25|0.900; Terbium|Tb|8.27|0.660; Thallium|Tl|11.85|1.550;
Thorium (IV) Fluoride|ThF4|6.32|-; Tin|Sn|7.30|0.724; Titanium|Ti|4.50|0.628; Titanium (IV) Oxide|TiO2|4.26|0.400;
Titanium Oxide|TiO|4.90|-; Tungsten|W|19.3|0.163; Tungsten Carbide|WC|15.6|0.151; Uranium|U|18.7|0.238;
Vanadium|V|5.96|0.530; Ytterbium|Yb|6.98|1.130; Yttrium|Y|4.34|0.835; Yttrium Oxide|Y2O3|5.01|-; Zinc|Zn|7.04|0.514;
Zinc Oxide|ZnO|5.61|0.556; Zinc Selenide|ZnSe|5.26|0.722; Zinc Sulfide|ZnS|4.09|0.775; Zirconium|Zr|6.51|0.600;
Zirconium Oxide|ZrO2|5.6|-
"""


@dataclass
class Material:
    name: str
    symbol: str
    density: float
    z_factor: float
    note: str = ""

    @property
    def label(self) -> str:
        """화면 표시 — "기호 — 이름"."""
        return f"{self.symbol} — {self.name}" if self.symbol else self.name

    def to_json(self) -> dict:
        d = {"name": self.name, "symbol": self.symbol, "density": self.density, "z_factor": self.z_factor}
        if self.note:
            d["note"] = self.note
        return d


def materials_file(settings_dir) -> Path:
    """재료 목록 파일(채널 공용) — v2 설정 폴더/thickness_materials.json(v1: exe 옆 config/thickness_materials.json)."""
    return Path(settings_dir) / FILE_NAME


def default_materials() -> List[Material]:
    out: List[Material] = []
    for item in _MANUAL_TABLE.replace("\n", " ").split(";"):
        item = item.strip()
        if not item:
            continue
        name, symbol, dens, z = (x.strip() for x in item.split("|"))
        if z == "-":
            out.append(Material(name, symbol, float(dens), 1.0, Z_MISSING_NOTE))
        else:
            out.append(Material(name, symbol, float(dens), float(z)))
    return out


def check_material_fields(name, symbol, density, z_factor) -> Tuple[Optional[Material], List[str]]:
    """한 항목 검사 — (Material | None, 오류 목록). lib/thickness_materials.py:95-116."""
    errs: List[str] = []
    nm = str(name or "").strip()
    if not nm:
        errs.append("이름이 비어 있음")
    vals = {}
    for key, v, lo, hi in (("density", density, DENSITY_MIN, DENSITY_MAX), ("z_factor", z_factor, Z_MIN, Z_MAX)):
        try:
            if isinstance(v, bool) or v is None or (isinstance(v, str) and not v.strip()):
                raise ValueError
            f = float(v)
        except Exception:
            errs.append(f"{key} {v!r} 숫자가 아님")
            continue
        if f != f or not (lo <= f <= hi):
            errs.append(f"{key} {f:g} 범위({lo:g}~{hi:g}) 밖")
            continue
        vals[key] = f
    if errs:
        return None, errs
    return Material(nm, str(symbol or "").strip(), vals["density"], vals["z_factor"]), []


def duplicate_names(names) -> List[str]:
    seen, dup = set(), []
    for n in names:
        k = str(n or "").strip().casefold()
        if not k:
            continue
        if k in seen and n not in dup:
            dup.append(n)
        seen.add(k)
    return dup


def atomic_write_json(path, data) -> Path:
    """같은 폴더 임시 파일에 쓰고 fsync → os.replace. 실패하면 임시 파일을 지우고 예외(원본은 그대로).
    lib/thickness_factors.py:118-136·thickness_materials.py:131-153 과 같은 방식."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    fd, tmp = tempfile.mkstemp(prefix=p.name + ".", suffix=".tmp", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fp:
            fp.write(text)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp, str(p))
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return p


def save_materials(path, materials: List[Material]) -> Path:
    """이름 중복이면 ValueError(원본은 그대로). 아니면 원자적 저장."""
    dup = duplicate_names([m.name for m in materials])
    if dup:
        raise ValueError(f"재료 이름 중복: {', '.join(dup)}")
    return atomic_write_json(path, {"version": FILE_VERSION, "materials": [m.to_json() for m in materials]})


def load_materials(path, *, create: bool = True) -> Tuple[List[Material], List[str]]:
    """(재료 목록, 경고) — 예외를 던지지 않는다. lib/thickness_materials.py:156-193."""
    p = Path(path)
    warns: List[str] = []
    if not p.exists():
        mats = default_materials()
        if create:
            try:
                save_materials(p, mats)
            except Exception as e:
                warns.append(f"재료 목록 파일을 만들 수 없음({p}) — 기본 목록 사용: {e!r}")
        return mats, warns
    try:
        data = json.loads(p.read_text(encoding="utf-8-sig"))
        items = data.get("materials") if isinstance(data, dict) else None
        if not isinstance(items, list):
            raise ValueError("materials 목록이 없음")
    except Exception as e:
        warns.append(f"재료 목록 파일을 읽을 수 없음({p.name}) — 기본 목록 사용: {e!r}")
        return default_materials(), warns
    out: List[Material] = []
    seen = set()
    for i, it in enumerate(items, 1):
        if not isinstance(it, dict):
            warns.append(f"{i}번째 항목이 표(객체)가 아님 — 제외")
            continue
        m, errs = check_material_fields(it.get("name"), it.get("symbol"), it.get("density"), it.get("z_factor"))
        if m is None:
            warns.append(f"{i}번째 항목 '{it.get('name')}': {', '.join(errs)} — 제외")
            continue
        if m.name.casefold() in seen:
            warns.append(f"{i}번째 항목 '{m.name}': 이름 중복 — 제외")
            continue
        seen.add(m.name.casefold())
        m.note = str(it.get("note") or "").strip()
        out.append(m)
    return out, warns


def find_material(materials: Sequence[Material], name: str) -> Optional[Material]:
    k = str(name or "").strip().casefold()
    if not k:
        return None
    for m in materials:
        if m.name.casefold() == k:
            return m
    return None


def materials_by_name(materials: Sequence[Material]) -> Dict[str, Material]:
    """lib/thickness_materials.py:206-207(v1 에서 쓰는 곳 없음 — THK-093)."""
    return {m.name: m for m in materials}


# ═══════════════════════ 재료 목록 창 명령(ui/thickness_material_dialog.py) ═══════════════════════
def _num_text(v) -> str:
    return "" if v is None else f"{float(v):g}"


def materials_window(path) -> dict:
    """창을 열 때 보일 것 — 행(이름·기호·density·Z·note 글자), 읽기 경고(그 항목은 빠짐, 최대 6줄), 안내."""
    mats, warns = load_materials(path)
    rows = [[m.name, m.symbol, _num_text(m.density), _num_text(m.z_factor), m.note] for m in mats]
    warn_text = ""
    if warns:
        warn_text = ("⚠ 읽기 경고(그 항목은 빠짐):\n" + "\n".join(warns[:6])
                     + (f"\n… 외 {len(warns) - 6}건" if len(warns) > 6 else ""))
    return {"title": TITLE, "header": HEADER_TEXT, "columns": list(COLS), "rows": rows, "warnings": warns,
            "warning_text": warn_text, "note": f"파일: {path}\n{NEXT_RUN_NOTE}"}


def collect_rows(rows: Iterable[Sequence]) -> Tuple[List[Material], List[str], List[Tuple[int, int]]]:
    """저장 검사(ThicknessMaterialDialog.collect) — (재료 목록, 오류 목록, 빨간 칸 (행, 열) 목록). 행 번호는 0부터."""
    out: List[Material] = []
    errs: List[str] = []
    bad: List[Tuple[int, int]] = []
    seen: Dict[str, int] = {}
    for r, row in enumerate(rows):
        vals = [str(row[c]).strip() if c < len(row) and row[c] is not None else "" for c in range(len(COLS))]
        if not any(vals):
            continue                                         # 완전히 빈 행은 무시
        m, e = check_material_fields(vals[0], vals[1], vals[2], vals[3])
        if e:
            errs.append(f"{r + 1}행 '{vals[0]}': {', '.join(e)}")
            for c, key in ((0, "이름"), (2, "density"), (3, "z_factor")):
                if any(x.startswith(key) for x in e):
                    bad.append((r, c))
            continue
        k = m.name.casefold()
        if k in seen:
            errs.append(f"{r + 1}행 '{m.name}': 이름 중복({seen[k] + 1}행)")
            bad.append((r, 0))
            continue
        seen[k] = r
        m.note = vals[4]
        out.append(m)
    return out, errs, bad


def delete_confirm_text(names: Iterable[str], used_names: Iterable[str]) -> str:
    """지우려는 재료 중 Factor 에서 쓰는 것이 있으면 확인창 문구(ThicknessMaterialDialog.delete_rows), 없으면 ""."""
    used = {str(n).casefold() for n in used_names if n}
    hit = [str(n).strip() for n in names if str(n or "").strip().casefold() in used]
    if not hit:
        return ""
    return (f"Factor 설정에서 쓰는 재료입니다: {', '.join(hit)}\n"
            "지우면 그 건은 '재료 목록에 없음'이 되어 Thickness 모드 시작이 거부됩니다(E845).\n지울까요?")


def save_rows(path, rows: Iterable[Sequence]) -> Tuple[bool, str, List[Tuple[int, int]]]:
    """[저장](ThicknessMaterialDialog._on_save) — (저장했나, 알림 문구(성공이면 ""), 빨간 칸)."""
    mats, errs, bad = collect_rows(rows)
    if errs:
        return False, "고칠 칸이 있어 저장하지 않았습니다:\n\n" + "\n".join(errs[:15]), bad
    try:
        save_materials(path, mats)
    except Exception as e:
        return False, f"저장 실패(파일은 바뀌지 않음):\n{e!r}", []
    return True, "", []
