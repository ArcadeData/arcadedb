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

// The markdown-lite renderer for comment bodies in the Support tab (studio-support-markdown.js): what it draws (paragraphs, bold,
// italic, code, tables, lists, http(s) links) and, above all, that nothing else gets through: every character of the input is
// escaped before any markup is added. Run with:
//
//     node --test studio/test/support-markdown.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const STATIC = path.join(__dirname, "..", "src", "main", "resources", "static");

function md(text, options) {
  const context = { String, Array, Object, Math, RegExp };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(path.join(STATIC, "js", "studio-support-markdown.js"), "utf8"), context, { filename: "studio-support-markdown.js" });
  return vm.runInContext("supportMarkdownHtml", context)(text, options);
}

/** No tag other than the ones the renderer owns, and no attribute other than the ones it writes. */
function assertSafe(html) {
  const tags = html.match(/<\/?[a-zA-Z][^>]*>/g) || [];
  for (const tag of tags) {
    assert.match(
      tag,
      /^<\/?(p|br|hr|h[1-6]|blockquote|strong|em|code|pre|ul|ol|li|table|thead|tbody|tr|th|td|div)( class="[a-z -]*")?>$|^<a href="https?:\/\/[^"<>]*" target="_blank" rel="noopener noreferrer">$|^<\/a>$/,
      "unexpected tag: " + tag
    );
  }
  // Whatever is left once the owned tags are removed is text: it holds no angle bracket (they are all &lt; and &gt;)
  const text = html.replace(/<\/?[a-zA-Z][^>]*>/g, "");
  assert.doesNotMatch(text, /[<>]/, "markup left in the text: " + text);
}

test("the comment Studio posts for a support request answer renders as bold text and a table", () => {
  const html = md("**How many records Brewery holds**\n\n| records |\n| --- |\n| 1414 |");
  assert.match(html, /<p><strong>How many records Brewery holds<\/strong><\/p>/);
  assert.match(html, /<table class="table table-sm[^"]*">/);
  assert.match(html, /<th>records<\/th>/);
  assert.match(html, /<td>1414<\/td>/);
  assert.doesNotMatch(html, /\*\*/);
  assert.doesNotMatch(html, /\| ---/);
  assertSafe(html);
});

test("paragraphs and line breaks", () => {
  assert.equal(md("one\ntwo\n\nthree"), "<p>one<br>two</p><p>three</p>");
  assert.equal(md(""), "");
  assert.equal(md(null), "");
  assert.equal(md(undefined), "");
});

test("bold, italic and inline code, and nothing is formatted inside code", () => {
  assert.equal(md("a **b** c *d* e `f`"), "<p>a <strong>b</strong> c <em>d</em> e <code>f</code></p>");
  assert.equal(md("`**not bold** <b>`"), "<p><code>**not bold** &lt;b&gt;</code></p>");
  assert.equal(md("2 * 3 * 4"), "<p>2 * 3 * 4</p>");
});

test("unclosed markers stay as plain text", () => {
  assert.equal(md("**never closed"), "<p>**never closed</p>");
  assert.equal(md("*also never"), "<p>*also never</p>");
  assert.equal(md("a `tick"), "<p>a `tick</p>");
  assert.equal(md("```\nno end <b>"), "<pre><code>no end &lt;b&gt;</code></pre>");
});

test("fenced code blocks keep their text exactly, escaped", () => {
  const html = md("before\n```sql\nSELECT * FROM a WHERE x < 3 AND y > 1\n  **keep**\n```\nafter");
  assert.match(html, /<pre><code>SELECT \* FROM a WHERE x &lt; 3 AND y &gt; 1\n  \*\*keep\*\*<\/code><\/pre>/);
  assert.match(html, /<p>before<\/p>/);
  assert.match(html, /<p>after<\/p>/);
  assertSafe(html);
});

test("bullet and numbered lists", () => {
  assert.equal(md("- a\n- **b**\n* c"), "<ul><li>a</li><li><strong>b</strong></li><li>c</li></ul>");
  assert.equal(md("1. one\n2) two"), "<ol><li>one</li><li>two</li></ol>");
  assert.equal(md("text\n- a\n\nmore"), "<p>text</p><ul><li>a</li></ul><p>more</p>");
});

test("pipe tables: header, separator, rows, alignment markers, ragged rows, escaped pipes", () => {
  const html = md("| a | b |\n|:--|--:|\n| 1 | 2 |\n| 3 |\n| x \\| y | 4 | extra |");
  assert.match(html, /<thead><tr><th>a<\/th><th>b<\/th><\/tr><\/thead>/);
  assert.match(html, /<tr><td>1<\/td><td>2<\/td><\/tr>/);
  assert.match(html, /<tr><td>3<\/td><td><\/td><\/tr>/);
  assert.match(html, /<tr><td>x \| y<\/td><td>4<\/td><\/tr>/);
  assert.doesNotMatch(html, /extra/);
  assertSafe(html);
});

test("a line with a pipe but no separator row is not a table", () => {
  const html = md("a | b\nc | d");
  assert.doesNotMatch(html, /<table/);
  assert.equal(html, "<p>a | b<br>c | d</p>");
});

test("links: only http and https, with a safe target", () => {
  assert.equal(
    md("see [the docs](https://docs.arcadedb.com/a?b=1&c=2) now"),
    '<p>see <a href="https://docs.arcadedb.com/a?b=1&amp;c=2" target="_blank" rel="noopener noreferrer">the docs</a> now</p>'
  );
  assert.match(md("[x](HTTP://example.com)"), /<a href="HTTP:\/\/example.com"/);
  for (const bad of ["javascript:alert(1)", "data:text/html,<script>1</script>", "vbscript:x", "ftp://x.y", "//evil.example", "mailto:a@b.c", " javascript:alert(1)"]) {
    const html = md("[click](" + bad + ")");
    assert.doesNotMatch(html, /<a /, bad);
    assertSafe(html);
  }
});

test("an image is a plain link and a bare URL is left as text", () => {
  const html = md("![pic](https://x.y/a.png) and https://x.y/b");
  assert.doesNotMatch(html, /<img/);
  assert.equal((html.match(/<a /g) || []).length, 1);
  assert.match(html, /^<p><a href="https:\/\/x\.y\/a\.png"[^>]*>pic<\/a> and https:\/\/x\.y\/b<\/p>$/);
  assertSafe(html);
});

test("XSS attempts in every position come out as text", () => {
  const attacks = [
    "<script>alert(1)</script>",
    "<img src=x onerror=alert(1)>",
    '"><svg onload=alert(1)>',
    "<a href=\"javascript:alert(1)\">x</a>",
  ];
  for (const a of attacks) {
    const inputs = [
      a,
      "**" + a + "**",
      "*" + a + "*",
      "`" + a + "`",
      "```\n" + a + "\n```",
      "- " + a,
      "1. " + a,
      "| " + a + " |\n| --- |\n| " + a + " |",
      "[" + a + "](https://ok.example/)",
      "[ok](https://ok.example/" + a + ")",
      "[ok](https://ok.example/\" onmouseover=\"alert(1))",
    ];
    for (const input of inputs) {
      const html = md(input);
      assertSafe(html);
      assert.doesNotMatch(html, /<script|<img|<svg/i, input);
    }
  }
});

test("headings, block quotes and rules (issue #9626: what an AI answer writes)", () => {
  assert.equal(md("# One\n### Three **b** ###"), "<h1>One</h1><h3>Three <strong>b</strong></h3>");
  assert.equal(md("#not a heading"), "<p>#not a heading</p>");
  assert.equal(md("####### seven"), "<p>####### seven</p>");
  assert.equal(md("> a\n> *b*\n\nafter"), "<blockquote><p>a<br><em>b</em></p></blockquote><p>after</p>");
  assert.equal(md("a\n\n---\n\n* * *\nb"), "<p>a</p><hr><hr><p>b</p>");
  // A rule never steals the separator row of a table
  assert.match(md("| a |\n| --- |\n| 1 |"), /<table/);
});

test("XSS attempts in headings and block quotes come out as text", () => {
  for (const a of ["<script>alert(1)</script>", "<img src=x onerror=alert(1)>", "[x](javascript:alert(1))"]) {
    for (const input of ["# " + a, "###### " + a + " ##", "> " + a, ">> " + a]) {
      const html = md(input);
      assertSafe(html);
      assert.doesNotMatch(html, /<script|<img|<a /i, input);
    }
  }
});

test("a caller draws fenced blocks itself and raises the bounds", () => {
  const seen = [];
  const html = md("```sql\nSELECT <1>\n```\n```\nplain\n```\n```sql\" onclick=\"x\nz\n```", {
    codeBlock: (code, lang) => {
      seen.push([code, lang]);
      return "<pre>X</pre>";
    },
  });
  assert.equal(html, "<pre>X</pre><pre>X</pre><pre>X</pre>");
  // The callback receives the raw code (it escapes it), and the language only when it is a plain word
  assert.deepEqual(seen, [["SELECT <1>", "sql"], ["plain", ""], ["z", ""]]);
  const long = "x".repeat(25000) + "\n\nEND";
  assert.doesNotMatch(md(long), /END/);
  assert.match(md(long, { maxChars: 30000 }), /END/);
  const rows = "| a |\n| --- |\n" + "| x |\n".repeat(300);
  assert.equal((md(rows).match(/<tr>/g) || []).length, 201);
  assert.equal((md(rows, { maxRows: 1000 }).match(/<tr>/g) || []).length, 301);
});

test("a quote in a link target cannot leave the attribute", () => {
  const html = md('[x](https://a.example/" onclick="alert(1))');
  assertSafe(html);
  assert.doesNotMatch(html, /" onclick=/);
});

test("pathological input is bounded and still safe", () => {
  const started = Date.now();
  const big = "**a ".repeat(50000) + "\n" + "| a |\n| --- |\n".repeat(1) + "| x |\n".repeat(5000) + "- i\n".repeat(5000) + "`".repeat(40000);
  const html = md(big);
  // A heading whose closing hashes follow a long run of spaces backtracks quadratically through a naive regex (#9626)
  const headings = md("# x" + " ".repeat(60000) + "#a\n## y" + " ".repeat(3990) + "#a\n" + "- ".repeat(3000) + "x", { maxChars: 200000 });
  assertSafe(headings);
  assertSafe(html);
  assert.ok(Date.now() - started < 2000, "took " + (Date.now() - started) + " ms");
  assert.ok(html.length < 200000, "output too large: " + html.length);
  const rows = (md("| a |\n| --- |\n" + "| x |\n".repeat(5000)).match(/<tr>/g) || []).length;
  assert.ok(rows <= 201, "rows: " + rows);
  const wide = md("|" + " c |".repeat(100) + "\n|" + " --- |".repeat(100) + "\n|" + " v |".repeat(100));
  assert.ok((wide.match(/<th>/g) || []).length <= 20);
  assert.equal(md("\u0000<b>"), "<p>&lt;b&gt;</p>");
});

test("Studio draws comment bodies through the renderer", () => {
  const source = fs.readFileSync(path.join(STATIC, "js", "studio-support.js"), "utf8");
  assert.match(source, /support-md[^]*supportMarkdownHtml\(text\)/);
  assert.match(source, /supportMarkdownHtml\(issue\.body \|\| issue\.description\)/);
  const index = fs.readFileSync(path.join(STATIC, "index.html"), "utf8");
  assert.match(index, /js\/studio-support-markdown\.js/);
});
