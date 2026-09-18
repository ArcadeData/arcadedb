#!/usr/bin/env python3
"""Reusable builder for the animated e2e-ha workflow diagrams.

Every diagram is a looping, self-contained SVG: no scripts, no external fonts, SMIL only, so it renders
inside GitHub markdown, an IDE preview and a plain browser alike.

A diagram is declared as a list of scenes. Each scene carries a step label (left rail), a caption
(bottom bar) and a list of stage primitives visible only while that scene is on screen. Primitives are
plain SVG strings drawn in the order given, so put cards first and arrows last.
"""

from dataclasses import dataclass, field
from typing import List

W, H = 1000, 580

BG = "#0b1220"
PANEL = "#141f36"
PANEL_EDGE = "#25344f"
TEXT = "#e7eefc"
MUTED = "#8ea3c4"
BLUE = "#41b6f7"
GREEN = "#54d98c"
AMBER = "#f4b942"
PURPLE = "#b98cf5"
RED = "#f4736f"
GREY = "#5b6b87"
FONT = "ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif"
MONO = "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace"

FADE = 0.28  # seconds of cross-fade at each scene boundary

# ---------------------------------------------------------------------------- stage geometry
CLIENT = (372, 246, 118, 76)          # x, y, w, h of the JUnit box
PROXY_X = 573                         # midpoint of the client -> node run, where the proxy pills sit
NODE_X, NODE_W = 660, 214
_LAYOUT = {2: (112, [116, 336]), 3: (100, [100, 238, 376])}

_nodes = _LAYOUT[2][1]
_node_h = _LAYOUT[2][0]
_proxy = False


def layout(n, proxy=False):
    """Selects the 2-node or 3-node stage, and whether a toxiproxy column sits in front of the nodes."""
    global _nodes, _node_h, _proxy
    _node_h, _nodes = _LAYOUT[n][0], _LAYOUT[n][1]
    _proxy = proxy


def node_count():
    return len(_nodes)


def node_box(i):
    return NODE_X, _nodes[i], NODE_W, _node_h


def node_center(i):
    return NODE_X + NODE_W / 2, _nodes[i] + _node_h / 2


def client_anchor(i):
    """Where an arrow to node i leaves the JUnit box."""
    cx, cy, cw, ch = CLIENT
    n = len(_nodes)
    span = 56 if n == 3 else 40
    return cx + cw, cy + ch / 2 + (i - (n - 1) / 2) * (span / max(n - 1, 1))


@dataclass
class Scene:
    step: str                       # left-rail label
    caption: str                    # bottom bar, line 1
    detail: str = ""                # bottom bar, line 2 (monospace)
    dur: float = 3.4
    body: List[str] = field(default_factory=list)   # stage primitives


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def txt(x, y, s, size=13, fill=TEXT, anchor="start", weight="400", font=FONT, opacity=1.0):
    return (f'<text x="{x}" y="{y}" font-family="{font}" font-size="{size}" fill="{fill}" '
            f'text-anchor="{anchor}" font-weight="{weight}" opacity="{opacity}">{esc(s)}</text>')


def rect(x, y, w, h, fill="none", stroke=PANEL_EDGE, r=10, sw=1.5, dash=None, opacity=1.0):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" '
            f'stroke-width="{sw}"{d} opacity="{opacity}"/>')


def chip(x, y, label, color=BLUE, w=None, size=11):
    w = w or (len(label) * 6.6 + 18)
    return (rect(x, y, w, 21, fill=color + "22", stroke=color + "88", r=10, sw=1) +
            txt(x + w / 2, y + 15, label, size=size, fill=color, anchor="middle", font=MONO))


def arrow(x1, y1, x2, y2, color=BLUE, label="", dash=None, packet=False, curve=0.0, width=2.0):
    """Quadratic arrow; `curve` bends it perpendicular to the segment. Optional travelling packet."""
    mx, my = (x1 + x2) / 2, (y1 + y2) / 2
    dx, dy = x2 - x1, y2 - y1
    ln = max((dx * dx + dy * dy) ** 0.5, 1e-6)
    cx, cy = mx - dy / ln * curve, my + dx / ln * curve
    path = f"M {x1} {y1} Q {cx} {cy} {x2} {y2}"
    d = f' stroke-dasharray="{dash}"' if dash else ""
    out = [f'<path d="{path}" fill="none" stroke="{color}" stroke-width="{width}"{d} '
           f'marker-end="url(#ah-{color[1:]})" opacity="0.95"/>']
    if label:
        lx, ly = (x1 + 2 * cx + x2) / 4, (y1 + 2 * cy + y2) / 4
        dy_label = 0 if abs(dy) > abs(dx) else -11
        out.append(f'<rect x="{lx - len(label) * 3.3 - 6}" y="{ly - 9 + dy_label}" '
                   f'width="{len(label) * 6.6 + 12}" height="18" rx="9" fill="{BG}" opacity="0.92"/>')
        out.append(txt(lx, ly + 4 + dy_label, label, size=11, fill=color, anchor="middle", font=MONO))
    if packet:
        out.append(f'<circle r="4.5" fill="{color}"><animateMotion dur="1.4s" repeatCount="indefinite" '
                   f'path="{path}"/></circle>')
    return "".join(out)


def client_to(i, label="", color=BLUE, packet=False, dash=None):
    """Arrow from the JUnit box to node i.

    In a toxiproxy diagram the link carries the proxy pill at its midpoint, so the label is dropped
    rather than drawn on top of it; the caption says what the call is.
    """
    x1, y1 = client_anchor(i)
    x2, y2 = NODE_X - 4, node_center(i)[1]
    curve = -22 if y2 < y1 else (22 if y2 > y1 else 0)
    return arrow(x1, y1, x2, y2, color=color, label="" if _proxy else label, packet=packet,
                 curve=curve, dash=dash)


def _link_mid(i):
    """Midpoint of the client -> node i link, where the proxy and toxic pills are drawn."""
    _, y1 = client_anchor(i)
    return PROXY_X, (y1 + node_center(i)[1]) / 2


def raft_link(i, j, label="", color=PURPLE, packet=True, dash=None, width=2.2):
    """Arrow along the Raft backbone from node i to node j.

    Adjacent nodes are joined straight down the backbone; a link that spans a node bows out to the
    left instead, so neither the line nor its label is drawn on top of the node it skips.
    """
    x = NODE_X + NODE_W / 2
    yi, yj = _nodes[i], _nodes[j]
    y1 = yi + _node_h + 4 if yj > yi else yi - 4
    y2 = yj - 6 if yj > yi else yj + _node_h + 6
    curve = 300 if abs(i - j) > 1 else 0
    if yj < yi:
        curve = -curve
    return arrow(x, y1, x, y2, color=color, label=label, packet=packet, dash=dash, width=width,
                 curve=curve)


def cut(i, j, label="cut"):
    """A red X across the Raft backbone between two adjacent nodes."""
    x = NODE_X + NODE_W / 2
    lo, hi = (i, j) if _nodes[i] < _nodes[j] else (j, i)
    y = (_nodes[lo] + _node_h + _nodes[hi]) / 2
    return (f'<path d="M {x - 11} {y - 11} l 22 22 M {x + 11} {y - 11} l -22 22" stroke="{RED}" '
            f'stroke-width="3" stroke-linecap="round"/>' +
            txt(x - 20, y + 5, label, size=11, fill=RED, anchor="end", font=MONO))


def node_state(i, state, color, sub=""):
    """Coloured node frame plus a state badge: LEADER / FOLLOWER / STARTING / PARTITIONED ..."""
    x, y, w, h = node_box(i)
    out = [rect(x, y, w, h, fill=color + "14", stroke=color, r=14, sw=2.2),
           chip(x + w - 102, y + 10, state, color, w=90)]
    if sub:
        out.append(txt(x + 16, y + h - 13, sub, size=11, fill=color, font=MONO))
    return "".join(out)


def node_off(i, state="STOPPED", sub=""):
    """A node that is not running: dimmed frame, dashed border."""
    x, y, w, h = node_box(i)
    return (rect(x, y, w, h, fill="#00000055", stroke=GREY, r=14, sw=2, dash="6 5") +
            chip(x + w - 102, y + 10, state, GREY, w=90) +
            (txt(x + 16, y + h - 13, sub, size=11, fill=GREY, font=MONO) if sub else ""))


def isolated(i, sub="no quorum"):
    """A node cut off from the Docker network."""
    x, y, w, h = node_box(i)
    return (node_state(i, "ISOLATED", RED, sub) +
            rect(x - 8, y - 8, w + 16, h + 16, stroke=RED + "88", r=18, sw=1.6, dash="7 6"))


def node_progress(i, frac, color=GREEN, label=""):
    x, y, w, h = node_box(i)
    bw = w - 32
    out = [rect(x + 16, y + h - 33, bw, 8, fill="#ffffff10", stroke="none", r=4, sw=0),
           f'<rect x="{x + 16}" y="{y + h - 33}" width="{bw * frac}" height="8" rx="4" fill="{color}"/>']
    if label:
        out.append(txt(x + 16, y + h - 41, label, size=11, fill=color, font=MONO))
    return "".join(out)


def stamp(i, text, color=GREEN):
    """A tick + short verdict pinned to the right of a node."""
    x, y, w, h = node_box(i)
    cx, cy = x + w + 44, y + h / 2
    return (f'<circle cx="{cx}" cy="{cy - 12}" r="13" fill="{color}22" stroke="{color}" stroke-width="2"/>'
            f'<path d="M {cx - 6} {cy - 12} l 4 5 l 8 -10" fill="none" stroke="{color}" stroke-width="2.4" '
            f'stroke-linecap="round" stroke-linejoin="round"/>' +
            txt(cx, cy + 16, text, size=10.5, fill=color, anchor="middle", font=MONO))


def cross_stamp(i, text, color=RED):
    """A cross + short verdict pinned to the right of a node."""
    x, y, w, h = node_box(i)
    cx, cy = x + w + 44, y + h / 2
    return (f'<circle cx="{cx}" cy="{cy - 12}" r="13" fill="{color}22" stroke="{color}" stroke-width="2"/>'
            f'<path d="M {cx - 5} {cy - 17} l 10 10 M {cx + 5} {cy - 17} l -10 10" stroke="{color}" '
            f'stroke-width="2.4" stroke-linecap="round"/>' +
            txt(cx, cy + 16, text, size=10.5, fill=color, anchor="middle", font=MONO))


def log_strip(i, cells, committed=0, color=BLUE, uncommitted_color=AMBER):
    """The Raft log held by node i: one cell per entry, filled while committed, outlined while not.

    `cells` are the term numbers (or any short label) in index order; `committed` is how many
    leading cells the node considers committed.
    """
    x, y, w, h = node_box(i)
    cw, gap = 23, 3
    x0, y0, ch = x + 16, y + h - 44, 18
    out = []
    for k, cell in enumerate(cells):
        cx = x0 + k * (cw + gap)
        done = k < committed
        c = color if done else uncommitted_color
        out.append(rect(cx, y0, cw, ch, fill=c + ("44" if done else "12"), stroke=c, r=4, sw=1.2,
                        dash=None if done else "3 2"))
        out.append(txt(cx + cw / 2, y0 + 13, str(cell), size=10, fill=c, anchor="middle", font=MONO))
    return "".join(out)


def node_to_client(i, label="", color=GREEN, packet=False, dash=None):
    """Reply arrow from node i back to the JUnit / client box."""
    x2, y2 = client_anchor(i)
    x1, y1 = NODE_X - 4, node_center(i)[1]
    curve = 22 if y1 < y2 else (-22 if y1 > y2 else 0)
    return arrow(x1, y1, x2, y2, color=color, label="" if _proxy else label, packet=packet,
                 curve=curve, dash=dash)


def toxic(i, label, color=RED):
    """A toxic applied to node i's proxy, drawn on the link just under the proxy pill."""
    x, y = _link_mid(i)
    w = len(label) * 6.6 + 18
    return chip(x - w / 2, y + 16, label, color, w=w)


def code_card(y, lines, title="", color=PURPLE, x=352, w=286):
    h = 26 + len(lines) * 16 + (16 if title else 0)
    out = [rect(x, y, w, h, fill="#0a1425", stroke=color + "77", r=10, sw=1.4)]
    yy = y + 20
    if title:
        out.append(txt(x + 12, yy, title, size=11, fill=color, weight="600", font=MONO))
        yy += 18
    for ln in lines:
        out.append(txt(x + 12, yy, ln, size=11, fill=MUTED, font=MONO))
        yy += 16
    return "".join(out)


def variants_card(lines, title="sibling @Test methods", color=AMBER):
    """The other test methods of the same class, listed where they would otherwise be invisible."""
    return code_card(92, lines, title=title, color=color)


# Text that does not fit the panel it is drawn in is a silent defect in a generated image: nothing
# fails, the sentence just runs under the next element. These are the widths that fit.
LIMITS = {"caption": 118, "detail": 142, "step": 40, "subtitle": 118}


def _lint(scenes, subtitle, title):
    for field, cap in (("subtitle", LIMITS["subtitle"]),):
        if len(subtitle) > cap:
            print(f"    ! {title}: {field} is {len(subtitle)} chars (max {cap})")
    for sc in scenes:
        for field in ("caption", "detail", "step"):
            v = getattr(sc, field)
            if len(v) > LIMITS[field]:
                print(f"    ! {title} / {sc.step}: {field} is {len(v)} chars (max {LIMITS[field]})")


def build(out_path, title, subtitle, scenes, footer="", proxy=None):
    _lint(scenes, subtitle, title)
    total = sum(s.dur for s in scenes)
    use_proxy = _proxy if proxy is None else proxy

    def anim(start, dur, vis="1", hid="0"):
        f = FADE / total
        t0, t1 = start / total, (start + dur) / total
        kt = [0.0, max(t0 - f, 0.0), min(t0 + f, 1.0), max(t1 - f, 0.0), min(t1 + f, 1.0), 1.0]
        kt = [min(max(v, 0.0), 1.0) for v in kt]
        kt = [round(max(kt[i], kt[i - 1] if i else 0.0), 5) for i in range(len(kt))]
        vals = [hid, hid, vis, vis, hid, hid]
        return (f'<animate attributeName="opacity" dur="{total}s" repeatCount="indefinite" '
                f'keyTimes="{";".join(str(v) for v in kt)}" values="{";".join(vals)}"/>')

    markers = "".join(
        f'<marker id="ah-{c[1:]}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        f'orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="{c}"/></marker>'
        for c in (BLUE, GREEN, AMBER, PURPLE, RED, GREY, MUTED))

    s = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
         f'font-family="{FONT}" role="img" aria-label="{esc(title)}">',
         f'<defs>{markers}</defs>',
         rect(0, 0, W, H, fill=BG, stroke="none", r=0, sw=0),
         txt(28, 34, title, size=19, weight="700"),
         txt(28, 55, subtitle, size=12, fill=MUTED)]

    # ---- left rail: one row per scene, the active one highlighted
    rail_x, rail_y, rail_w = 24, 76, 300
    s.append(rect(rail_x, rail_y, rail_w, H - 156, fill=PANEL, r=14))
    row_h = (H - 156 - 20) / len(scenes)
    start = 0.0
    for i, sc in enumerate(scenes):
        ry = rail_y + 10 + i * row_h
        size = 12 if len(sc.step) <= 33 else 10.5
        s.append(f'<g opacity="1">{txt(rail_x + 42, ry + row_h / 2 + 4, sc.step, size=size, fill=MUTED)}'
                 f'<circle cx="{rail_x + 24}" cy="{ry + row_h / 2}" r="6" fill="none" stroke="{PANEL_EDGE}" '
                 f'stroke-width="1.5"/>{anim(start, sc.dur, vis="0", hid="1")}</g>')
        s.append(f'<g opacity="0">'
                 f'{rect(rail_x + 6, ry + 2, rail_w - 12, row_h - 4, fill=BLUE + "1f", stroke=BLUE + "66", r=10, sw=1.2)}'
                 f'<circle cx="{rail_x + 24}" cy="{ry + row_h / 2}" r="6" fill="{BLUE}"/>'
                 f'{txt(rail_x + 42, ry + row_h / 2 + 4, sc.step, size=size, fill=TEXT, weight="600")}'
                 f'{anim(start, sc.dur)}</g>')
        start += sc.dur

    # ---- stage chrome: JUnit box, optional proxy column, node boxes, Raft backbone
    st_x, st_y, st_w, st_h = 348, 76, W - 348 - 24, H - 156
    s.append(rect(st_x, st_y, st_w, st_h, fill=PANEL, r=14))

    cx_, cy_, cw, ch = CLIENT
    s.append(rect(cx_, cy_, cw, ch, fill="#ffffff08", stroke=PANEL_EDGE, r=12))
    s.append(txt(cx_ + cw / 2, cy_ + 30, "JUnit", size=13, fill=TEXT, anchor="middle", weight="600"))
    s.append(txt(cx_ + cw / 2, cy_ + 48, "test host", size=11, fill=MUTED, anchor="middle"))
    s.append(txt(cx_ + cw / 2, cy_ + 64, "Testcontainers", size=10, fill=MUTED, anchor="middle", font=MONO))

    if use_proxy:
        for i in range(len(_nodes)):
            mx, my = _link_mid(i)
            label = f"toxiproxy 866{i}/867{i}"
            wpx = len(label) * 6.6 + 18
            s.append(rect(mx - wpx / 2, my - 11, wpx, 22, fill=BG, stroke=PANEL_EDGE, r=11, sw=1.2,
                          dash="4 3"))
            s.append(txt(mx, my + 4, label, size=10.5, fill=MUTED, anchor="middle", font=MONO))

    x = NODE_X + NODE_W / 2
    s.append(f'<path d="M {x} {_nodes[0] + _node_h} L {x} {_nodes[-1]}" stroke="{PANEL_EDGE}" '
             f'stroke-width="1.5" stroke-dasharray="4 4"/>')
    s.append(txt(x, _nodes[-1] + _node_h + 22, "raft backbone 2434", size=10.5, fill=MUTED,
                 anchor="middle", font=MONO))

    for i in range(len(_nodes)):
        bx, by, bw_, bh = node_box(i)
        s.append(rect(bx, by, bw_, bh, fill="#ffffff06", stroke=PANEL_EDGE, r=14))
        s.append(txt(bx + 16, by + 25, f"arcadedb-{i}", size=13, weight="600"))
        s.append(txt(bx + 16, by + 42, "http 2480 - raft 2434", size=10.5, fill=MUTED, font=MONO))

    start = 0.0
    for sc in scenes:
        s.append(f'<g opacity="0">{"".join(sc.body)}{anim(start, sc.dur)}</g>')
        start += sc.dur

    # ---- caption bar
    cap_y = H - 72
    s.append(rect(24, cap_y, W - 48, 48, fill=PANEL, r=12))
    start = 0.0
    for sc in scenes:
        g = [txt(44, cap_y + 21, sc.caption, size=12.5, fill=TEXT)]
        if sc.detail:
            g.append(txt(44, cap_y + 38, sc.detail, size=10.5, fill=MUTED, font=MONO))
        s.append(f'<g opacity="0">{"".join(g)}{anim(start, sc.dur)}</g>')
        start += sc.dur
    if footer:
        s.append(txt(W - 44, cap_y + 30, footer, size=11, fill=MUTED, anchor="end", font=MONO))

    s.append("</svg>")
    with open(out_path, "w") as f:
        f.write("".join(s) + "\n")
    return total
