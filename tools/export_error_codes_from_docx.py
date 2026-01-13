# tools/export_error_codes_from_docx.py
from docx import Document
import re
from pathlib import Path

DOCX = Path("C:\Users\wnsgj\Desktop\Sputter\Sputter Error Code.docx")  # 메뉴얼 파일명
OUT  = Path("util/error_codes_default.py")

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

OUT.write_text("DEFAULT_CODES = " + repr(codes), encoding="utf-8")
print("written:", OUT)
