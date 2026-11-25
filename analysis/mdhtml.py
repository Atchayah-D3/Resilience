"""Minimal Markdown -> standalone HTML for the RRB reports (no extra packages).

Handles what the reports use: # / ## / ### headings, paragraphs, "- " lists, pipe tables
(with ---: alignment), **bold** and `code`. Table cells starting with "pass" are green,
"FAIL" red, so a run's verdict is visible at a glance.
"""
import html
import re

CSS = """
body{font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:2rem auto;max-width:1400px;
     padding:0 1.5rem;color:#1f2328;line-height:1.45}
h1{border-bottom:2px solid #d0d7de;padding-bottom:.3rem}
h2{margin-top:2rem;border-bottom:1px solid #d0d7de;padding-bottom:.2rem}
h3{margin-top:1.5rem}
table{border-collapse:collapse;margin:.6rem 0 1.2rem;font-size:.86rem;display:block;overflow-x:auto}
th,td{border:1px solid #d0d7de;padding:.3rem .55rem;vertical-align:top}
th{background:#f6f8fa;position:sticky;top:0}
tr:nth-child(even) td{background:#fbfcfd}
td.r,th.r{text-align:right;white-space:nowrap}
td.pass{background:#dafbe1 !important;color:#116329;font-weight:600}
td.fail{background:#ffebe9 !important;color:#a40e26;font-weight:600}
td.nm{background:#fff8c5 !important;color:#7d4e00;font-style:italic}
td.na{background:#f6f8fa !important;color:#57606a;font-style:italic}
code{background:#f6f8fa;border-radius:4px;padding:.1rem .3rem;font-size:.85em}
td code{white-space:normal;word-break:break-word}
.meta{color:#656d76;font-size:.85rem}
"""


def _inline(t):
    t = html.escape(t, quote=False)
    t = re.sub(r"`([^`]+)`", r"<code>\1</code>", t)
    t = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", t)
    return t


def _cells(line):
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|") and not line.endswith("\\|"):
        line = line[:-1]
    parts = re.split(r"(?<!\\)\|", line)
    return [p.strip().replace("\\|", "|") for p in parts]


def md_to_html(md, title, footer=""):
    """Standalone page from Markdown."""
    return page(title, md_fragment(md), footer)


def page(title, body, footer="", extra_css=""):
    foot = f'<p class="meta">{_inline(footer)}</p>' if footer else ""
    return (f"<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
            f"<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
            f"<title>{html.escape(title)}</title><style>{CSS}{extra_css}</style></head>\n<body>\n"
            + body + f"\n{foot}\n</body></html>\n")


def md_fragment(md):
    """Markdown -> HTML body fragment (no <html> wrapper)."""
    lines = md.splitlines()
    out, i, para, in_list = [], 0, [], False

    def flush():
        nonlocal para, in_list
        if para:
            out.append("<p>" + " ".join(_inline(p) for p in para) + "</p>")
            para = []
        if in_list:
            out.append("</ul>")
            in_list = False

    while i < len(lines):
        ln = lines[i]
        s = ln.strip()
        if not s:
            flush()
        elif s.startswith("#"):
            flush()
            lvl = len(s) - len(s.lstrip("#"))
            out.append(f"<h{lvl}>{_inline(s[lvl:].strip())}</h{lvl}>")
        elif s.startswith("|") and i + 1 < len(lines) and re.match(r"^\|?\s*:?-{3,}", lines[i + 1].strip()):
            flush()
            head = _cells(s)
            align = ["r" if c.strip().endswith(":") else "" for c in _cells(lines[i + 1])]
            out.append("<table><thead><tr>" + "".join(
                f'<th class="{align[k] if k < len(align) else ""}">{_inline(c)}</th>' for k, c in enumerate(head))
                + "</tr></thead><tbody>")
            i += 2
            while i < len(lines) and lines[i].strip().startswith("|"):
                row = _cells(lines[i])
                tds = []
                for k, c in enumerate(row):
                    cls = align[k] if k < len(align) else ""
                    if c.lower() == "pass" or c.lower().startswith("pass"):
                        cls += " pass"
                    elif c.startswith("FAIL"):
                        cls += " fail"
                    elif c.startswith("not measured"):
                        cls += " nm"
                    elif c in ("not at target", "not applicable", "info"):
                        cls += " na"
                    tds.append(f'<td class="{cls.strip()}">{_inline(c)}</td>')
                out.append("<tr>" + "".join(tds) + "</tr>")
                i += 1
            out.append("</tbody></table>")
            continue
        elif s.startswith("- "):
            if para:
                out.append("<p>" + " ".join(_inline(p) for p in para) + "</p>")
                para = []
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline(s[2:])}</li>")
        else:
            if in_list:
                out.append("</ul>")
                in_list = False
            para.append(s)
        i += 1
    flush()
    return "\n".join(out)
