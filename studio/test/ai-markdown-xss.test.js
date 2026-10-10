/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

// Issue #9626: the AI Assistant's answer and the profiler's AI analysis are markdown written by a model that quotes the
// database, delivered by a third-party portal. Both are untrusted, and both used to go through `marked`, which copies raw
// HTML into its output, straight into the page. They now go through the escape-first renderer of
// studio-support-markdown.js. The scripts are loaded whole into a VM context, as the browser loads them, with a `marked`
// global that behaves like the real library (raw HTML passes through): a sink that still reaches for it fails here.
//
//     node --test studio/test/ai-markdown-xss.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const STATIC = path.join(__dirname, "..", "src", "main", "resources", "static");
const JS = path.join(STATIC, "js");

function escapeHtml(unsafe) {
  if (unsafe == null) return null;
  if (typeof unsafe === "object") unsafe = JSON.stringify(unsafe);
  else unsafe = unsafe.toString();
  return unsafe.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

/** A stand-in for `marked`: like the real one, it does not sanitize, so raw HTML in the source reaches the output. */
function rawMarked() {
  function Renderer() {}
  return { Renderer: Renderer, parse: (text) => "<p>" + text + "</p>" };
}

function load() {
  const rendered = {};
  const jQuery = (selector) => ({ html: (h) => (rendered[selector] = h) });
  const context = { String, Array, Object, Math, RegExp, Number, JSON, Date, console, escapeHtml, marked: rawMarked(), jQuery, $: jQuery, rendered,
    globalStorageLoad: (key, def) => def };
  vm.createContext(context);
  for (const file of ["studio-support-markdown.js", "studio-ai.js", "studio-profiler.js"])
    vm.runInContext(fs.readFileSync(path.join(JS, file), "utf8"), context, { filename: file });
  return context;
}

const ctx = load();

/** The HTML the AI Assistant puts in the page for one answer, through the whole message renderer. */
function assistant(text) {
  return ctx.aiRenderAssistantMessage({ role: "assistant", content: text }, 0);
}

/** The HTML the profiler puts in #profilerAiContent for one analysis. */
function profiler(text) {
  ctx.profilerRenderAiResponse({ response: text });
  return ctx.rendered["#profilerAiContent"];
}

/** No executable markup: no tag outside a fixed allow-list, no event handler, no script-capable URL. */
function assertInert(html, input) {
  assert.doesNotMatch(html, /<(script|img|svg|iframe|object|embed|style|link|meta|form|input|video|audio|math|base)\b/i, input);
  // Every attribute written is one Studio owns; none is an inline handler that came from the input
  const tags = html.match(/<[a-zA-Z][^>]*>/g) || [];
  for (const tag of tags) {
    // The only handlers Studio itself writes in these renderers, matched whole: anything else is a finding
    if (/\sonclick="(aiCopyCode\(this, 'aiMdCode_\d+'\)|aiDeleteMessage\(\d+\)|aiExecuteAll\(this, \d+, \d+\)|profilerAiExecuteAll\(this\))"/.test(tag)) {
      assert.equal((tag.match(/\son[a-z]+\s*=/gi) || []).length, 1, "extra handler in " + tag + " for input " + input);
      continue;
    }
    assert.doesNotMatch(tag, /\son[a-z]+\s*=/i, "event handler in " + tag + " for input " + input);
  }
  assert.doesNotMatch(html, /href\s*=\s*"?\s*(javascript|data|vbscript):/i, input);
}

// The payloads of the issue, verbatim, plus the block-level and attribute-breaking shapes
const PAYLOADS = [
  "Here is the value: <img src=x onerror=alert(1)>",
  "Row: <script>alert(document.cookie)</script>",
  "A link: [click](javascript:alert(1))",
  'inline <b onmouseover="alert(1)">hover</b>',
  "The type name is `<svg onload=alert(1)>`",
  "<div>\n<img src=x onerror=alert(1)>\n</div>",
  "Widget <img src=x onerror=\"fetch('/api/v1/server',{headers:{Authorization:globalCredentials}})\">",
  "<javascript:alert(1)>",
  "## <img src=x onerror=alert(1)>",
  "> <script>alert(1)</script>",
  "| name |\n| --- |\n| <img src=x onerror=alert(1)> |",
  "- <svg onload=alert(1)>",
  "![x](javascript:alert(1))",
  '[ok](https://ok.example/" onmouseover="alert(1))',
  "```<img src=x onerror=alert(1)>\ncode\n```",
  "```sql\" onclick=\"alert(1)\nSELECT 1\n```",
];

test("the AI Assistant answer never puts the model's or the database's HTML into the page", () => {
  for (const p of PAYLOADS) assertInert(assistant(p), p);
});

test("the profiler's AI analysis never puts the model's HTML into the page", () => {
  for (const p of PAYLOADS) assertInert(profiler(p), p);
});

test("the injected markup survives as visible text, not as nothing", () => {
  const html = assistant("Here is the value: <img src=x onerror=alert(1)>");
  assert.match(html, /Here is the value: &lt;img src=x onerror=alert\(1\)&gt;/);
  assert.match(profiler("Row: <script>x</script>"), /Row: &lt;script&gt;x&lt;\/script&gt;/);
});

test("the answer still renders the markdown a model writes", () => {
  const html = assistant("## Products\n\nThere are **3** *items*:\n\n- one\n- two\n\n| name | n |\n| --- | --- |\n| Widget | 1 |\n\n> note\n\n---\n\nSee [docs](https://docs.arcadedb.com/).");
  assert.match(html, /<h2>Products<\/h2>/);
  assert.match(html, /<strong>3<\/strong>/);
  assert.match(html, /<em>items<\/em>/);
  assert.match(html, /<ul><li>one<\/li><li>two<\/li><\/ul>/);
  assert.match(html, /<td>Widget<\/td>/);
  assert.match(html, /<blockquote><p>note<\/p><\/blockquote>/);
  assert.match(html, /<hr>/);
  assert.match(html, /<a href="https:\/\/docs\.arcadedb\.com\/" target="_blank" rel="noopener noreferrer">docs<\/a>/);
});

test("a fenced block keeps its language badge and copy button, with the code escaped", () => {
  const html = assistant("```sql\nSELECT * FROM T WHERE a < 3\n```");
  assert.match(html, /<span class="badge"[^>]*>SQL<\/span>/);
  assert.match(html, /<pre id="aiMdCode_\d+"[^>]*>SELECT \* FROM T WHERE a &lt; 3<\/pre>/);
  assert.match(html, /onclick="aiCopyCode\(this, 'aiMdCode_\d+'\)"/);
});

test("a long answer is not cut at the size of a support comment", () => {
  const long = "x".repeat(30000) + "\n\nTHE-END";
  assert.match(assistant(long), /THE-END/);
});

test("no Studio script parses markdown with marked, and the page no longer loads it", () => {
  for (const file of fs.readdirSync(JS).filter((f) => f.endsWith(".js"))) {
    const source = fs.readFileSync(path.join(JS, file), "utf8");
    assert.ok(!/\bmarked\s*\.\s*(parse|parseInline|Renderer|marked)\b/.test(source), file + " renders markdown through marked");
  }
  const index = fs.readFileSync(path.join(STATIC, "index.html"), "utf8");
  assert.ok(!/marked(\.min)?\.js/.test(index), "index.html still loads marked");
});
