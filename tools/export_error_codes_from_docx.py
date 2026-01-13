# tools/export_error_codes_from_docx.py
from docx import Document
import re
from pathlib import Path

# ✅ DOCX 경로는 raw string 또는 / 로 작성
DOCX = Path(r"C:\Users\wnsgj\Desktop\Sputter\Sputter Error Code.docx")

# ✅ 이 스크립트 위치 기준으로 프로젝트 루트/출력 경로 고정
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "util" / "error_codes_default.py"
OUT.parent.mkdir(parents=True, exist_ok=True)

code_re = re.compile(r"^(E\d{3})\s*:\s*(.+)$")

doc = Document(str(DOCX))
paras = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]

codes = {}
cur = None
for t in paras:
    m = code_re.match(t)
    if m:
        code = m.group(1)
        cause = m.group(2).strip()
        codes[code] = {"cause": cause, "fix": ""}
        cur = code
    else:
        if cur and not t.startswith("["):
            if not codes[cur]["fix"]:
                codes[cur]["fix"] = t
            else:
                codes[cur]["fix"] += "\n" + t

import pprint

with OUT.open("w", encoding="utf-8") as f:
    f.write("# Auto-generated from Sputter Error Code.docx\n")
    f.write("DEFAULT_CODES = ")
    pprint.pprint(codes, stream=f, width=120, sort_dicts=True)
    f.write("\n")

print("written:", OUT)

