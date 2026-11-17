"""Dung file .docx cho mot chuong tu ban thao markdown, dung chuong_3.docx lam template.

Cach dung:
    python build_chapter_docx.py <template.docx> <input.md> <output.docx>
"""
import re
import sys

import docx
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from docx.oxml import parse_xml
from docx.oxml.ns import qn
from docx.shared import Cm, Pt

W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
M_NS = "http://schemas.openxmlformats.org/officeDocument/2006/math"
TEXT_WIDTH_DXA = 9070  # 21 cm - le trai 3 cm - le phai 2 cm = 16 cm

# ---------------------------------------------------------------- OMML ----

def m_run(text, plain=False):
    sty = '<m:rPr><m:sty m:val="p"/></m:rPr>' if plain else ""
    font = '<w:rPr><w:rFonts w:ascii="Cambria Math" w:hAnsi="Cambria Math"/></w:rPr>'
    return f"<m:r>{sty}{font}<m:t xml:space=\"preserve\">{text}</m:t></m:r>"


def m_sub(base_xml, sub_text):
    return (f"<m:sSub><m:e>{base_xml}</m:e>"
            f"<m:sub>{m_run(sub_text, plain=True)}</m:sub></m:sSub>")


def m_abs(inner_xml):
    return ('<m:d><m:dPr><m:begChr m:val="|"/><m:endChr m:val="|"/></m:dPr>'
            f"<m:e>{inner_xml}</m:e></m:d>")


def m_frac(num_xml, den_xml):
    return f"<m:f><m:num>{num_xml}</m:num><m:den>{den_xml}</m:den></m:f>"


def eq_vwap_error():
    vwap = lambda s: m_sub(m_run("VWAP", plain=True), s)
    return (m_sub(m_run("ε"), "VWAP") + m_run("=")
            + m_abs(vwap("speed") + m_run("−") + vwap("batch")))


def eq_volume_drift():
    v = lambda s: m_sub(m_run("V"), s)
    num = m_abs(v("speed") + m_run("−") + v("batch"))
    return (m_sub(m_run("ΔV"), "%") + m_run("=")
            + m_frac(num, v("batch")) + m_run("×100%", plain=True))


DISPLAY_MATH = {
    r"\varepsilon_{\text{VWAP}}": eq_vwap_error,
    r"\Delta V_{\%}": eq_volume_drift,
}


def add_display_math(doc, latex):
    """Cong thuc dong rieng: LaTeX -> MathML -> OMML (tex2omml.py, MML2OMML.XSL cua Office)."""
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from tex2omml import tex_to_omml
    para = doc.add_paragraph(style="Body Text")
    para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    para.paragraph_format.first_line_indent = Cm(0)
    para._p.append(tex_to_omml(latex))

# -------------------------------------------------------------- inline ----

INLINE_RE = re.compile(r"(`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*|~[^~]+~|\^[^^\s]+\^)")


def add_inline(para, text, size=None, bold=None, italic=None, code_style=True):
    """code_style=False: dinh danh trong `...` giu phong Times New Roman (dung cho o bang)."""
    for token in INLINE_RE.split(text):
        if not token:
            continue
        run_bold, run_italic, sub, code = bold, italic, False, False
        if token.startswith("`"):
            token, code = token[1:-1], code_style
        elif token.startswith("**"):
            token, run_bold = token[2:-2], True
        elif token.startswith("*") and len(token) > 2:
            token, run_italic = token[1:-1], True
        elif token.startswith("~") and len(token) > 2:
            token, sub = token[1:-1], True
        sup = False
        if token.startswith("^") and token.endswith("^") and len(token) > 2:
            token, sup = token[1:-1], True
        if not code:
            # Dau cach ngan cach hang nghin (50 000) khong duoc xuong dong.
            token = re.sub(r"(?<=\d) (?=\d{3}\b)", " ", token)
        run = para.add_run(token)
        if sup:
            run.font.superscript = True
        if code:
            run.style = para.part.document.styles["Verbatim Char"]
        if run_bold:
            run.bold = True
        if run_italic:
            run.italic = True
        if sub:
            run.font.subscript = True
        if size:
            run.font.size = Pt(size)

# --------------------------------------------------------------- table ----

def set_cell_width(cell, dxa):
    tc_pr = cell._tc.get_or_add_tcPr()
    for old in tc_pr.findall(qn("w:tcW")):
        tc_pr.remove(old)
    tc_pr.append(parse_xml(f'<w:tcW xmlns:w="{W_NS}" w:type="dxa" w:w="{dxa}"/>'))


def _token_dxa(token):
    """Uoc luong do rong (dxa) cua mot tu khong the ngat, Times New Roman co 12."""
    text = re.sub(r"[`*~^]", "", token)
    # Chu in hoa Times New Roman dam rong hon chu thuong khoang 1,4 lan
    return sum(160 if ch.isupper() else 108 for ch in text)


def column_widths(rows):
    """Moi cot du rong cho tu dai nhat; phan con lai chia theo do dai noi dung."""
    ncol = len(rows[0])
    padding = 260
    min_w, pref = [], []
    for c in range(ncol):
        tokens = [t for r in rows for t in re.findall(r"`[^`]+`|\S+", r[c])]
        min_w.append(max(_token_dxa(t) for t in tokens) + padding)
        cell_lengths = [sum(_token_dxa(t) for t in re.findall(r"`[^`]+`|\S+", r[c]))
                        for r in rows[1:] or rows]
        pref.append(sum(cell_lengths) / len(cell_lengths) + padding)
    total_min = sum(min_w)
    if total_min >= TEXT_WIDTH_DXA:
        widths = [int(TEXT_WIDTH_DXA * w / total_min) for w in min_w]
    else:
        spare = TEXT_WIDTH_DXA - total_min
        extra = [max(p - m, 0) for p, m in zip(pref, min_w)]
        total_extra = sum(extra) or 1
        widths = [int(m + spare * e / total_extra) for m, e in zip(min_w, extra)]
    widths[-1] += TEXT_WIDTH_DXA - sum(widths)
    return widths


def add_table(doc, rows):
    table = doc.add_table(rows=len(rows), cols=len(rows[0]))
    table.style = doc.styles["Table"]
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    tbl_pr = table._tbl.tblPr
    tbl_pr.append(parse_xml(
        f'<w:tblBorders xmlns:w="{W_NS}">'
        + "".join(f'<w:{side} w:val="single" w:sz="4" w:space="0" w:color="000000"/>'
                  for side in ("top", "left", "bottom", "right", "insideH", "insideV"))
        + "</w:tblBorders>"))
    tbl_pr.append(parse_xml(f'<w:tblLayout xmlns:w="{W_NS}" w:type="fixed"/>'))
    widths = column_widths(rows)
    grid = table._tbl.tblGrid
    for col, width in zip(grid.findall(qn("w:gridCol")), widths):
        col.set(qn("w:w"), str(width))
    for r_idx, row in enumerate(rows):
        if r_idx == 0:
            tr_pr = table.rows[0]._tr.get_or_add_trPr()
            tr_pr.append(parse_xml(f'<w:tblHeader xmlns:w="{W_NS}"/>'))
        for c_idx, text in enumerate(row):
            cell = table.cell(r_idx, c_idx)
            set_cell_width(cell, widths[c_idx])
            para = cell.paragraphs[0]
            para.style = doc.styles["Compact"]
            para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            fmt = para.paragraph_format
            fmt.first_line_indent = Cm(0)
            fmt.line_spacing = 1.0
            fmt.space_before = Pt(2)
            fmt.space_after = Pt(2)
            add_inline(para, text, size=12, bold=True if r_idx == 0 else None, code_style=False)


def parse_table_row(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]

# ---------------------------------------------------------------- main ----

def reset_body(doc):
    body = doc.element.body
    for child in list(body):
        if child.tag != qn("w:sectPr"):
            body.remove(child)
    for r_id, rel in list(doc.part.rels.items()):
        if rel.reltype == RT.IMAGE:
            del doc.part.rels[r_id]


def apply_format_rules(doc):
    """Ap quy chuan Phan 7.1 (00_global_constants.md)."""
    h3 = doc.styles["Heading 3"]
    h3.font.italic = False  # 1.1.1: chu thuong, dam, KHONG nghieng
    h3.font.bold = True
    h3.font.size = Pt(13)
    h2 = doc.styles["Heading 2"]
    h2.font.size = Pt(13)
    h2.font.bold = True
    h1 = doc.styles["Heading 1"]
    h1.font.size = Pt(14)
    h1.font.bold = True
    footer = doc.sections[0].footer
    footer.is_linked_to_previous = False
    para = footer.paragraphs[0] if footer.paragraphs else footer.add_paragraph()
    for child in list(para._p):
        if child.tag != qn("w:pPr"):
            para._p.remove(child)
    para.alignment = WD_ALIGN_PARAGRAPH.CENTER
    para._p.append(parse_xml(
        f'<w:fldSimple xmlns:w="{W_NS}" w:instr="PAGE"><w:r>'
        '<w:rPr><w:rFonts w:ascii="Times New Roman" w:hAnsi="Times New Roman"/>'
        '<w:sz w:val="26"/></w:rPr><w:t>1</w:t></w:r></w:fldSimple>'))


def build(template, src, out):
    doc = docx.Document(template)
    reset_body(doc)
    apply_format_rules(doc)

    lines = open(src, encoding="utf-8").read().splitlines()
    i, after_heading = 0, False
    while i < len(lines):
        line = lines[i].rstrip()
        if not line:
            i += 1
            continue
        if line.startswith("# "):
            doc.add_paragraph(line[2:], style="Heading 1")
            doc.core_properties.title = line[2:]
            after_heading = True
        elif line.startswith("## "):
            doc.add_paragraph(line[3:], style="Heading 2")
            after_heading = True
        elif line.startswith("### "):
            doc.add_paragraph(line[4:], style="Heading 3")
            after_heading = True
        elif line.startswith("!["):
            # ![Hình x.y: chú thích](duong/dan.png) -> hinh can giua + ten hinh DUOI hinh (co 11, dam)
            import os
            m = re.match(r"!\[(.+)\]\((.+)\)", line)
            caption, rel = m.group(1), m.group(2)
            pic_para = doc.add_paragraph(style="Body Text")
            pic_para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            pic_para.paragraph_format.first_line_indent = Cm(0)
            pic_para.paragraph_format.keep_with_next = True
            pic_para.add_run().add_picture(os.path.join(os.path.dirname(os.path.abspath(src)), rel), width=Cm(15.5))
            cap = doc.add_paragraph(style="Body Text")
            cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
            cap.paragraph_format.first_line_indent = Cm(0)
            add_inline(cap, caption, size=11, bold=True)
        elif line.startswith("$$"):
            add_display_math(doc, line.strip("$").strip())
        elif line.startswith("```"):
            code = []
            i += 1
            while not lines[i].startswith("```"):
                code.append(lines[i])
                i += 1
            para = doc.add_paragraph(style="Source Code")
            para.paragraph_format.space_before = Pt(6)
            para.paragraph_format.space_after = Pt(6)
            for n, code_line in enumerate(code):
                run = para.add_run(code_line)
                if n < len(code) - 1:
                    run.add_break()
        elif line.startswith("|"):
            rows = []
            while i < len(lines) and lines[i].startswith("|"):
                if not re.match(r"^\|\s*:?-", lines[i]):
                    rows.append(parse_table_row(lines[i]))
                i += 1
            add_table(doc, rows)
            continue
        elif line.startswith("**Bảng") and line.endswith("**"):
            para = doc.add_paragraph(style="Body Text")
            para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            para.paragraph_format.first_line_indent = Cm(0)
            para.paragraph_format.keep_with_next = True
            add_inline(para, line[2:-2], size=11, bold=True)
        elif line.startswith("*Nguồn") and line.endswith("*"):
            para = doc.add_paragraph(style="Body Text")
            para.alignment = WD_ALIGN_PARAGRAPH.CENTER
            para.paragraph_format.first_line_indent = Cm(0)
            add_inline(para, line[1:-1], size=10, italic=True)
        elif line.startswith("- ") or re.match(r"^\d+\. ", line):
            marker, text = ("•", line[2:]) if line.startswith("- ") else line.split(" ", 1)
            para = doc.add_paragraph(style="Compact")
            starts_with_code = text.startswith("`")
            para.alignment = WD_ALIGN_PARAGRAPH.LEFT if starts_with_code else WD_ALIGN_PARAGRAPH.JUSTIFY
            para.paragraph_format.left_indent = Cm(1.0)
            para.paragraph_format.first_line_indent = Cm(-0.6)
            add_inline(para, f"{marker} {text}" if marker == "•" else f"{marker} {text}")
        else:
            style = "First Paragraph" if after_heading else "Body Text"
            add_inline(doc.add_paragraph(style=style), line)
            after_heading = False
            i += 1
            continue
        if not line.startswith(("# ", "## ", "### ")):
            after_heading = False
        i += 1
    doc.save(out)


if __name__ == "__main__":
    build(*sys.argv[1:4])
