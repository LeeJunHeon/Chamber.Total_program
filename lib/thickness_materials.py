# lib/thickness_materials.py
# -*- coding: utf-8 -*-
"""두께 모니터 재료 목록(config/thickness_materials.json) — 읽기·검증·원자적 저장. Qt 없음.

항목: name(영문 이름, 재료를 구별하는 키 — 대소문자 무시 중복 금지), symbol(화학식), density(g/cm3, 0.500~99.99),
      z_factor(0.100~9.999), note(선택). 화면 표시 = "기호 — 이름".
처음 내용은 STM-100 매뉴얼 4.3 Material Reference Table 그대로(Z 가 '-' 인 재료는 1.000, note 에 그 사실).
exe 옆 config 폴더에 두어 재빌드 없이 고친다. 파일이 없으면 기본 목록으로 새로 만든다. 잘못된 항목은 경고 후 그 항목만 뺀다.
Factor 파일(thickness_factors_ch{N}.json)은 건별로 재료 이름만 저장하고, 공정 시작 때 이 목록에서 density·Z 를 찾는다.
"""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from device.thickness_monitor import DENSITY_MAX, DENSITY_MIN, Z_MAX, Z_MIN
from lib.thickness_config import base_dir

FILE_VERSION = 1
Z_MISSING_NOTE = "매뉴얼 Z 미기재 — 1.000 사용"

# STM-100 매뉴얼 4.3 Material Reference Table — 이름|기호|density|Z ('-' = 매뉴얼 Z 미기재)
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


def default_materials_file() -> Path:
    return base_dir() / "config" / "thickness_materials.json"


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
    """한 항목 검사 — (Material | None, 오류 목록)."""
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


def save_materials(path, materials: List[Material]) -> Path:
    """같은 폴더 임시 파일에 쓰고 os.replace 로 교체한다. 이름 중복이면 ValueError(원본은 그대로)."""
    dup = duplicate_names([m.name for m in materials])
    if dup:
        raise ValueError(f"재료 이름 중복: {', '.join(dup)}")
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps({"version": FILE_VERSION, "materials": [m.to_json() for m in materials]},
                      ensure_ascii=False, indent=2) + "\n"
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


def load_materials(path=None, *, create: bool = True) -> Tuple[List[Material], List[str]]:
    """(재료 목록, 경고) — 예외를 던지지 않는다. 파일이 없으면 기본 목록(create 면 파일도 만든다).
    잘못된 항목·중복 이름(뒤쪽)은 경고 후 그 항목만 뺀다. 파일을 읽을 수 없으면 기본 목록 + 경고(파일은 건드리지 않음)."""
    p = Path(path) if path else default_materials_file()
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


def find_material(materials: List[Material], name: str) -> Optional[Material]:
    k = str(name or "").strip().casefold()
    if not k:
        return None
    for m in materials:
        if m.name.casefold() == k:
            return m
    return None


def materials_by_name(materials: List[Material]) -> Dict[str, Material]:
    return {m.name: m for m in materials}
