#!/usr/bin/env python3
"""Registry of generated diagrams, and the index page that navigates them.

Both generators (build_all.py for the test workflows, raft_protocol.py for the protocol itself)
register through emit(), so the index page stays in step with whatever was actually built.
"""

import json
import os

from workflow_svg import build, layout

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = "../../src/test/java/com/arcadedb/containers/ha"

DIAGRAMS = []


def emit(slug, title, subtitle, scenes, blurb, group="Test workflows", java=None, methods=None,
         source=None, n=3, proxy=False):
    """Renders one diagram and records it for the index page."""
    layout(n, proxy)
    total = build(os.path.join(HERE, slug + ".svg"), title, subtitle, scenes,
                  footer="loop: %.0fs" % sum(s.dur for s in scenes))
    DIAGRAMS.append({"slug": slug, "title": title, "group": group, "java": java,
                     "methods": methods or [], "source": source, "blurb": blurb,
                     "scenes": len(scenes), "loop": round(total)})
    print(f"  {slug}.svg  ({total:.0f}s, {len(scenes)} scenes)")


INDEX_CSS = """
:root{--bg:#0b1220;--panel:#141f36;--edge:#25344f;--text:#e7eefc;--muted:#8ea3c4;--blue:#41b6f7}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
 font-family:ui-sans-serif,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
header{padding:28px 32px 14px}
h1{margin:0 0 6px;font-size:22px}
header p{margin:0;color:var(--muted);font-size:13.5px;max-width:880px;line-height:1.55}
main{display:grid;grid-template-columns:320px 1fr;gap:18px;padding:14px 32px 32px;align-items:start}
nav{background:var(--panel);border:1px solid var(--edge);border-radius:14px;padding:8px;
 position:sticky;top:14px}
nav h3{margin:12px 12px 6px;font-size:10.5px;letter-spacing:.12em;text-transform:uppercase;
 color:var(--muted);font-weight:600}
nav h3:first-child{margin-top:6px}
nav button{display:block;width:100%;text-align:left;background:none;border:0;color:var(--muted);
 font:inherit;font-size:13px;padding:9px 12px;border-radius:9px;cursor:pointer;line-height:1.35}
nav button:hover{background:#ffffff0d;color:var(--text)}
nav button.on{background:#41b6f71f;border:1px solid #41b6f766;color:var(--text);font-weight:600}
nav button small{display:block;color:var(--muted);font-weight:400;font-size:11px;margin-top:2px;
 font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
section{background:var(--panel);border:1px solid var(--edge);border-radius:14px;padding:18px}
section h2{margin:0 0 4px;font-size:17px}
section p.blurb{margin:0 0 12px;color:var(--muted);font-size:13px;line-height:1.5}
img{width:100%;height:auto;border-radius:12px;display:block;background:var(--bg)}
ul.meta{list-style:none;display:flex;flex-wrap:wrap;gap:8px;padding:0;margin:12px 0 0}
ul.meta li{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:11px;
 color:var(--muted);border:1px solid var(--edge);border-radius:9px;padding:4px 9px}
a{color:var(--blue)}
@media (max-width:900px){main{grid-template-columns:1fr}nav{position:static}}
"""


def write_index():
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ArcadeDB HA: Raft protocol and e2e-ha test workflows</title>
<style>{INDEX_CSS}</style>
</head>
<body>
<header>
  <h1>ArcadeDB HA - Raft protocol and e2e-ha test workflows</h1>
  <p>Looping animations, two groups. <strong>Raft protocol</strong> explains the consensus algorithm as
     this codebase implements it, with the real entry types, settings and endpoints.
     <strong>Test workflows</strong> gives one animation per integration test in <code>e2e-ha</code>.
     In every diagram the left rail is the sequence, the stage is the cluster, and the bottom bar
     carries the contract. Regenerate with <code>python3 build_all.py</code>.</p>
</header>
<main>
  <nav id="nav"></nav>
  <section>
    <h2 id="title"></h2>
    <p class="blurb" id="blurb"></p>
    <img id="svg" alt="">
    <ul class="meta" id="meta"></ul>
  </section>
</main>
<script>
const DIAGRAMS = {json.dumps(DIAGRAMS, indent=2)};
const SRC = {json.dumps(SRC)};
const nav = document.getElementById('nav');
const buttons = [];
function show(i) {{
  const d = DIAGRAMS[i];
  document.getElementById('title').textContent = d.title;
  document.getElementById('blurb').textContent = d.blurb;
  const img = document.getElementById('svg');
  img.src = d.slug + '.svg?' + Date.now();   // force the animation to restart from scene 1
  img.alt = d.title;
  const meta = ['<li>' + d.scenes + ' scenes</li>', '<li>' + d.loop + 's loop</li>'];
  if (d.methods.length)
    meta.push('<li>' + d.methods.length + ' @Test method' + (d.methods.length > 1 ? 's' : '') + '</li>');
  if (d.java) meta.push('<li><a href="' + SRC + '/' + d.java + '">' + d.java + '</a></li>');
  if (d.source) meta.push('<li>' + d.source + '</li>');
  document.getElementById('meta').innerHTML = meta.join('');
  buttons.forEach((b, j) => b.classList.toggle('on', i === j));
  location.hash = d.slug;
}}
let group = null;
DIAGRAMS.forEach((d, i) => {{
  if (d.group !== group) {{
    group = d.group;
    const h = document.createElement('h3');
    h.textContent = group;
    nav.appendChild(h);
  }}
  const b = document.createElement('button');
  const parts = d.title.split(' - ');
  b.innerHTML = parts[0] + '<small>' + parts.slice(1).join(' - ') + '</small>';
  b.onclick = () => show(i);
  nav.appendChild(b);
  buttons.push(b);
}});
const from = DIAGRAMS.findIndex(d => d.slug === location.hash.slice(1));
show(from >= 0 ? from : 0);
</script>
</body>
</html>
"""
    with open(os.path.join(HERE, "index.html"), "w") as f:
        f.write(html)
    print(f"  index.html  ({len(DIAGRAMS)} diagrams)")
