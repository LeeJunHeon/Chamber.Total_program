"""레시피 XLSX 읽기 — 표준 라이브러리(zipfile + xml)만으로 v1 openpyxl 읽기와 같은 값.

왜 직접 읽나
- v2 에서 openpyxl 은 v2/ch12/outputs(엑셀 기록 출력)에서만 허용된다(v2/tests/test_isolation.py). 레시피 읽기는 제어 쪽이라
  의존성을 늘리지 않고 표준 라이브러리로 읽는다. v1 이 쓰는 openpyxl 기능은 '첫 시트·값만(data_only)·읽기 전용' 뿐이다.
- 결과는 v1 과 같아야 한다 — v1 runtime/chamber_runtime.py:3395-3416(UI)·8025-8045(호스트):
    wb = load_workbook(path, read_only=True, data_only=True); ws = wb.worksheets[0]; ws.iter_rows(values_only=True)
  v2/tools/v1golden_process_steps.py 가 openpyxl 로 읽은 값(골든)과 이 모듈이 읽은 값이 같은지 시험이 확인한다.

openpyxl 3.1(읽기 전용) 동작을 그대로 옮긴 것 (openpyxl 소스: worksheet/_read_only.py·_reader.py·styles/stylesheet.py·
styles/numbers.py·utils/datetime.py)
- 시트: workbook.xml <sheets> 순서의 첫 워크시트(차트 시트 제외 — 미확인: 차트 시트가 맨 앞인 파일은 시험 안 함).
- 행: 1행부터 시작한다(min_row=1). <dimension ref> 가 있으면 그 max_row 까지, 빠진 행은 빈 행으로 채워 낸다
  (그래서 v1 의 행 번호 enumerate(start=2) 가 엑셀 행 번호와 맞는다). 칸 수는 dimension 의 max_col(없으면 그 행 마지막 칸).
  ⚠ max_row 함정: 서식만 있는 행도 <row> 로 남아 max_row 를 키운다 — 값이 없으니 v1 은 빈 행으로 건너뛴다(여기도 같다).
- 값: t="s" 공유 문자열(si 의 t + r/t 이어 붙임, rPh 제외), t="inlineStr", t="b" → bool, t="e"·"str" → 문자열,
  t="d" → ISO 날짜, 숫자는 '.'·'E'·'e' 가 있으면 float 아니면 int. 숫자 칸의 셀 서식이 날짜 형식이면 datetime/date/time,
  [h]:mm 같은 경과 시간 형식이면 timedelta(from_excel). <v> 가 비면 None.
"""
from __future__ import annotations

import datetime as _dt
import posixpath
import re
import zipfile
from typing import Any, Iterator, List, Optional, Tuple
from xml.etree import ElementTree as ET

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
RNS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
PNS = "{http://schemas.openxmlformats.org/package/2006/relationships}"
T_WORKSHEET = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
T_SST = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings"
T_STYLES = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles"

# openpyxl styles/numbers.py BUILTIN_FORMATS 중 날짜·시간 판정에 걸리는 것만이 아니라 전부(판정은 is_date_format 이 한다)
BUILTIN_FORMATS = {
    0: 'General', 1: '0', 2: '0.00', 3: '#,##0', 4: '#,##0.00', 5: '"$"#,##0_);("$"#,##0)', 6: '"$"#,##0_);[Red]("$"#,##0)',
    7: '"$"#,##0.00_);("$"#,##0.00)', 8: '"$"#,##0.00_);[Red]("$"#,##0.00)', 9: '0%', 10: '0.00%', 11: '0.00E+00',
    12: '# ?/?', 13: '# ??/??', 14: 'mm-dd-yy', 15: 'd-mmm-yy', 16: 'd-mmm', 17: 'mmm-yy', 18: 'h:mm AM/PM',
    19: 'h:mm:ss AM/PM', 20: 'h:mm', 21: 'h:mm:ss', 22: 'm/d/yy h:mm', 37: '#,##0_);(#,##0)', 38: '#,##0_);[Red](#,##0)',
    39: '#,##0.00_);(#,##0.00)', 40: '#,##0.00_);[Red](#,##0.00)', 41: r'_(* #,##0_);_(* \(#,##0\);_(* "-"_);_(@_)',
    42: r'_("$"* #,##0_);_("$"* \(#,##0\);_("$"* "-"_);_(@_)', 43: r'_(* #,##0.00_);_(* \(#,##0.00\);_(* "-"??_);_(@_)',
    44: r'_("$"* #,##0.00_)_("$"* \(#,##0.00\)_("$"* "-"??_)_(@_)', 45: 'mm:ss', 46: '[h]:mm:ss', 47: 'mmss.0',
    48: '##0.0E+0', 49: '@',
}
_STRIP_RE = re.compile(r'".*?"|\[(?!hh?\]|mm?\]|ss?\])[^\]]*\]')
_TIMEDELTA_RE = re.compile(r'\[hh?\](:mm(:ss(\.0*)?)?)?|\[mm?\](:ss(\.0*)?)?|\[ss?\](\.0*)?', re.I)
_COORD_RE = re.compile(r"^\$?([A-Za-z]{1,3})\$?(\d+)$")
_RANGE_RE = re.compile(r"^\$?([A-Za-z]{1,3})?\$?(\d+)?(?::\$?([A-Za-z]{1,3})?\$?(\d+)?)?$")
WINDOWS_EPOCH = _dt.datetime(1899, 12, 30)
MAC_EPOCH = _dt.datetime(1904, 1, 1)


def is_date_format(fmt: Optional[str]) -> bool:
    if fmt is None:
        return False
    fmt = _STRIP_RE.sub("", fmt.split(";")[0])
    return re.search(r"(?<![_\\])[dmhysDMHYS]", fmt) is not None


def is_timedelta_format(fmt: Optional[str]) -> bool:
    if fmt is None:
        return False
    return _TIMEDELTA_RE.search(fmt.split(";")[0]) is not None


def _col_index(letters: str) -> int:
    n = 0
    for ch in letters.upper():
        n = n * 26 + (ord(ch) - 64)
    return n


def _cast_number(v: str):
    if "." in v or "E" in v or "e" in v:
        return float(v)
    return int(v)


def from_excel(value: float, epoch: _dt.datetime = WINDOWS_EPOCH, timedelta: bool = False):
    """openpyxl utils/datetime.py:from_excel."""
    if timedelta:
        td = _dt.timedelta(days=value)
        if td.microseconds:
            td = _dt.timedelta(seconds=td.total_seconds() // 1, microseconds=round(td.microseconds, -3))
        return td
    day, fraction = divmod(value, 1)
    diff = _dt.timedelta(milliseconds=round(fraction * 86400 * 1000))
    if 0 <= value < 1 and diff.days == 0:
        mins, seconds = divmod(diff.seconds, 60)
        hours, mins = divmod(mins, 60)
        return _dt.time(hours, mins, seconds, diff.microseconds)
    if 0 < value < 60 and epoch == WINDOWS_EPOCH:
        day += 1
    return epoch + _dt.timedelta(days=day) + diff


def _iso(v: str):
    """openpyxl from_ISO8601 의 흔한 꼴(날짜·날짜시각·시각)만 — t="d" 칸은 엑셀이 거의 만들지 않는다(미확인)."""
    v = v.strip()
    for f in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            d = _dt.datetime.strptime(v.rstrip("Z"), f)
            return d.date() if f == "%Y-%m-%d" else d
        except ValueError:
            pass
    for f in ("%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
        try:
            return _dt.datetime.strptime(v, f).time()
        except ValueError:
            pass
    return v


def _text(si) -> str:
    """openpyxl cell/text.py Text.content — 바로 아래 t 와 r/t 를 이어 붙인다(rPh 발음 표기는 뺀다)."""
    parts = []
    t = si.find(f"{NS}t")
    if t is not None:
        parts.append(t.text or "")
    for r in si.findall(f"{NS}r"):
        rt = r.find(f"{NS}t")
        if rt is not None and rt.text is not None:
            parts.append(rt.text)
    return "".join(parts)


def _rels(zf: zipfile.ZipFile, path: str) -> dict:
    d, b = posixpath.split(path)
    rp = posixpath.join(d, "_rels", b + ".rels")
    out = {}
    if rp in zf.namelist():
        for r in ET.fromstring(zf.read(rp)).findall(f"{PNS}Relationship"):
            tgt = r.get("Target", "")
            full = tgt.lstrip("/") if tgt.startswith("/") else posixpath.normpath(posixpath.join(d, tgt))
            out[r.get("Id")] = (r.get("Type"), full)
    return out


class _Book:
    def __init__(self, zf: zipfile.ZipFile):
        self.zf = zf
        wb_path = "xl/workbook.xml"
        for r in ET.fromstring(zf.read("_rels/.rels")).findall(f"{PNS}Relationship"):
            if r.get("Type", "").endswith("/officeDocument"):
                wb_path = r.get("Target", wb_path).lstrip("/")
        root = ET.fromstring(zf.read(wb_path))
        rels = _rels(zf, wb_path)
        pr = root.find(f"{NS}workbookPr")
        self.epoch = MAC_EPOCH if pr is not None and pr.get("date1904") in ("1", "true") else WINDOWS_EPOCH
        self.sheets: List[str] = []
        for sh in root.iter(f"{NS}sheet"):
            typ, full = rels.get(sh.get(f"{RNS}id"), ("", ""))
            if typ == T_WORKSHEET:
                self.sheets.append(full)
        self.sst: List[str] = []
        self.date_formats: set = set()
        self.timedelta_formats: set = set()
        for typ, full in rels.values():
            if typ == T_SST and full in zf.namelist():
                self.sst = [_text(si) for si in ET.fromstring(zf.read(full)).findall(f"{NS}si")]
            elif typ == T_STYLES and full in zf.namelist():
                self._styles(ET.fromstring(zf.read(full)))

    def _styles(self, root) -> None:
        custom = {}
        nf = root.find(f"{NS}numFmts")
        if nf is not None:
            for f in nf.findall(f"{NS}numFmt"):
                custom[int(f.get("numFmtId"))] = f.get("formatCode")
        xfs = root.find(f"{NS}cellXfs")
        for idx, xf in enumerate(xfs.findall(f"{NS}xf") if xfs is not None else []):
            fid = int(xf.get("numFmtId", 0))
            fmt = custom[fid] if fid in custom else BUILTIN_FORMATS.get(fid)
            if is_date_format(fmt):
                self.date_formats.add(idx)
            if is_timedelta_format(fmt):
                self.timedelta_formats.add(idx)

    def cell(self, c) -> Tuple[Optional[int], Any]:
        t = c.get("t", "n")
        sid = int(c.get("s", 0) or 0)
        col = None
        r = c.get("r")
        if r:
            m = _COORD_RE.match(r)
            if m:
                col = _col_index(m.group(1))
        if t == "inlineStr":
            node = c.find(f"{NS}is")
            return col, (_text(node) if node is not None else None)
        v = c.findtext(f"{NS}v", None) or None
        if v is None:
            return col, None
        if t == "n":
            num = _cast_number(v)
            if sid in self.date_formats:
                try:
                    return col, from_excel(num, self.epoch, timedelta=sid in self.timedelta_formats)
                except (OverflowError, ValueError):
                    return col, "#VALUE!"
            return col, num
        if t == "s":
            return col, self.sst[int(v)]
        if t == "b":
            return col, bool(int(v))
        if t == "d":
            return col, _iso(v)
        return col, v                                  # "str"(수식 문자열 결과)·"e"(오류 글자)


def iter_first_sheet(path: str) -> Iterator[tuple]:
    """ws.iter_rows(values_only=True) 와 같은 튜플들(첫 워크시트)."""
    with zipfile.ZipFile(path) as zf:
        book = _Book(zf)
        if not book.sheets:
            raise IndexError("list index out of range")      # v1: wb.worksheets[0] 이 IndexError
        root = ET.fromstring(zf.read(book.sheets[0]))
    max_col = max_row = None
    dim = root.find(f"{NS}dimension")
    if dim is not None:
        m = _RANGE_RE.match(dim.get("ref", ""))
        if m:
            c1, r1, c2, r2 = m.groups()
            if m.group(0).find(":") < 0:
                c2, r2 = c1, r1
            max_col = _col_index(c2) if c2 else None
            max_row = int(r2) if r2 else None
    empty_row = (None,) * max_col if max_col is not None else ()
    data = root.find(f"{NS}sheetData")
    counter, row_counter, idx = 1, 0, 1
    for row in (data.findall(f"{NS}row") if data is not None else []):
        rr = row.get("r")
        row_counter = int(float(rr)) if rr else row_counter + 1
        idx = row_counter
        if max_row is not None and idx > max_row:
            break
        cells, col_counter = [], 0
        for c in row.findall(f"{NS}c"):
            col, val = book.cell(c)
            col_counter = col if col is not None else col_counter + 1
            cells.append((col_counter, val))
        while counter < idx:
            counter += 1
            yield empty_row
        if counter <= idx:
            counter += 1
            mc = max_col or (cells[-1][0] if cells else None)
            if not cells and not max_col:
                yield ()
                continue
            new = [None] * mc
            for cc, val in cells:
                if 1 <= cc <= mc:
                    new[cc - 1] = val
            yield tuple(new)
    if max_row is not None and max_row < idx:          # openpyxl 그대로: 중간에 끊긴 경우만 남은 빈 행을 낸다
        while counter <= max_row:
            counter += 1
            yield empty_row
