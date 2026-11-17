"""Chuyen mot tap con LaTeX (du cho cong thuc cua luan van) sang OMML cua Word.

LaTeX -> MathML (bo phan tich viet tay ben duoi) -> OMML (MML2OMML.XSL cua Office).
Ho tro: \\frac, \\sqrt, _, ^, \\sum, \\text, \\max, \\min, \\left, \\right, |, \\lfloor,
\\rfloor, \\times, \\cdot, \\le, \\ge, \\approx, \\neq, \\in, \\to, chu Hy Lap, \\%, \\,.
"""
import re
from xml.sax.saxutils import escape

from lxml import etree

MML_NS = "http://www.w3.org/1998/Math/MathML"
XSL_PATH = r"C:\Program Files\Microsoft Office\root\Office16\MML2OMML.XSL"
_XSLT = None

SYMBOLS = {
    r"\times": "×", r"\cdot": "·", r"\le": "≤", r"\leq": "≤", r"\ge": "≥", r"\geq": "≥",
    r"\approx": "≈", r"\neq": "≠", r"\in": "∈", r"\to": "→", r"\rightarrow": "→",
    r"\lfloor": "⌊", r"\rfloor": "⌋", r"\lceil": "⌈", r"\rceil": "⌉", r"\pm": "±",
    r"\infty": "∞", r"\%": "%", r"\{": "{", r"\}": "}", r"\mid": "|", r"\cup": "∪",
    r"\cap": "∩", r"\emptyset": "∅", r"\ldots": "…", r"\dots": "…", r"\langle": "⟨",
    r"\rangle": "⟩", r"\forall": "∀", r"\neg": "¬", r"\land": "∧", r"\lor": "∨",
}
GREEK = {
    r"\alpha": "α", r"\beta": "β", r"\gamma": "γ", r"\delta": "δ", r"\Delta": "Δ",
    r"\varepsilon": "ε", r"\epsilon": "ϵ", r"\lambda": "λ", r"\mu": "μ", r"\sigma": "σ",
    r"\Sigma": "Σ", r"\Phi": "Φ", r"\phi": "φ", r"\tau": "τ", r"\theta": "θ", r"\pi": "π", r"\omega": "ω",
}
FUNCS = {r"\max": "max", r"\min": "min", r"\log": "log", r"\arg": "arg", r"\exp": "exp"}
OPERATORS = set("+-=()[],.;:/<>!|'")

TOKEN_RE = re.compile(r"\\[A-Za-z]+|\\[%{}, ;!_]|[{}_^]|\d+(?:\.\d+)?|[A-Za-z]|\s+|.")


class _Parser:
    def __init__(self, tex):
        # Giu ca token khoang trang de \text{...} giu duoc dau cach; che do thuong bo qua chung.
        self.toks = TOKEN_RE.findall(tex)
        self.i = 0

    def _skip_ws(self):
        while self.i < len(self.toks) and self.toks[self.i].isspace():
            self.i += 1

    def peek(self):
        self._skip_ws()
        return self.toks[self.i] if self.i < len(self.toks) else None

    def take(self):
        tok = self.peek()
        self.i += 1
        return tok

    def take_raw(self):
        """Lay token ke tiep, ke ca khoang trang (dung trong \\text{...})."""
        tok = self.toks[self.i] if self.i < len(self.toks) else None
        self.i += 1
        return tok

    def group(self):
        """Doc mot nhom {...} hoac mot nguyen to don le."""
        if self.peek() == "{":
            self.take()
            items = self.sequence(stop="}")
            self.take()
            return "<mrow>" + "".join(items) + "</mrow>"
        return self.atom()

    def raw_braced(self):
        assert self.take() == "{"
        depth, out = 1, []
        while True:
            tok = self.take_raw()
            if tok == "{":
                depth += 1
            elif tok == "}":
                depth -= 1
                if depth == 0:
                    break
            out.append(tok)
        text = ""
        for tok in out:
            if tok == r"\%":
                text += "%"
            elif tok == r"\_":
                text += "_"
            elif tok in (r"\,", r"\ "):
                text += " "
            elif tok.startswith("\\") and tok in SYMBOLS:
                text += SYMBOLS[tok]
            else:
                text += tok
        return text

    def atom(self):
        tok = self.take()
        if tok is None:
            return ""
        if tok == r"\frac":
            return f"<mfrac>{self.group()}{self.group()}</mfrac>"
        if tok == r"\sqrt":
            return f"<msqrt>{self.group()}</msqrt>"
        if tok in (r"\text", r"\mathrm", r"\operatorname"):
            return f"<mtext>{escape(self.raw_braced())}</mtext>"
        if tok == r"\sum":
            return "<mo>∑</mo>"
        if tok in (r"\left", r"\right"):
            delim = self.take()
            if delim == ".":
                return ""
            return f"<mo>{escape(SYMBOLS.get(delim, delim))}</mo>"
        if tok in (r"\,", r"\;", r"\!", r"\ "):
            return "<mspace width=\"0.17em\"/>"
        if tok in (r"\quad", r"\qquad"):
            return "<mtext>   </mtext>" if tok == r"\qquad" else "<mtext> </mtext>"
        if tok == r"\_":
            return "<mo>_</mo>"
        if tok in FUNCS:
            return f"<mi mathvariant=\"normal\">{FUNCS[tok]}</mi>"
        if tok in GREEK:
            return f"<mi>{GREEK[tok]}</mi>"
        if tok in SYMBOLS:
            return f"<mo>{escape(SYMBOLS[tok])}</mo>"
        if re.fullmatch(r"\d+(?:\.\d+)?", tok):
            return f"<mn mathvariant=\"normal\">{tok}</mn>"
        if re.fullmatch(r"[A-Za-z]", tok):
            return f"<mi>{tok}</mi>"
        if tok == "-":
            return "<mo>−</mo>"  # dau tru toan hoc U+2212, khong phai gach noi
        if tok in OPERATORS or tok in "−×":
            return f"<mo>{escape(tok)}</mo>"
        raise ValueError(f"Ky hieu LaTeX chua ho tro: {tok}")

    def sequence(self, stop=None):
        items = []
        while self.peek() is not None and self.peek() != stop:
            base_tok = self.peek()
            base = self.atom()
            sub = sup = None
            while self.peek() in ("_", "^"):
                kind = self.take()
                if kind == "_":
                    sub = self.group()
                else:
                    sup = self.group()
            if base_tok == r"\sum" and (sub or sup):
                if sub and sup:
                    base = f"<munderover>{base}{sub}{sup}</munderover>"
                elif sub:
                    base = f"<munder>{base}{sub}</munder>"
                else:
                    base = f"<mover>{base}{sup}</mover>"
            elif sub and sup:
                base = f"<msubsup>{base}{sub}{sup}</msubsup>"
            elif sub:
                base = f"<msub>{base}{sub}</msub>"
            elif sup:
                base = f"<msup>{base}{sup}</msup>"
            items.append(base)
        return items


def tex_to_mathml(tex):
    body = "".join(_Parser(tex).sequence())
    return f'<math xmlns="{MML_NS}" display="block"><mrow>{body}</mrow></math>'


def tex_to_omml(tex):
    """Tra ve phan tu lxml <m:oMath> cho cong thuc LaTeX."""
    global _XSLT
    if _XSLT is None:
        _XSLT = etree.XSLT(etree.parse(XSL_PATH))
    result = _XSLT(etree.fromstring(tex_to_mathml(tex)))
    return result.getroot()


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    for tex in sys.argv[1:]:
        print(etree.tostring(tex_to_omml(tex), encoding="unicode")[:400])
