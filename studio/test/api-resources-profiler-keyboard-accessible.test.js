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
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

// Regression test for issue #9508 (follow-up of #9375 and #8740): the API tab endpoint list, its "Try It" tag and
// response-headers toggle, the Resources tab TOC headers and the Profiler query rows were click-only div/span/tr
// elements, unreachable from the keyboard and not announced as buttons. Collapsible headers also lacked aria-expanded.
//
// Run with:
//
//     node --test studio/test/api-resources-profiler-keyboard-accessible.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const read = (...p) => fs.readFileSync(path.join(STATIC_DIR, ...p), "utf8");
const apiHtml = read("api.html");
const apiMarkup = apiHtml.substring(0, apiHtml.indexOf("<script"));
const resourcesHtml = read("resources.html");
const profilerJs = read("js", "studio-profiler.js");
const css = read("css", "studio.css").replace(/\/\*[\s\S]*?\*\//g, "");

function extractFn(source, name) {
  const start = source.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  let i = source.indexOf("{", start) + 1;
  let depth = 1;
  while (i < source.length && depth > 0) {
    if (source[i] === "{") depth++;
    else if (source[i] === "}") depth--;
    i++;
  }
  return source.substring(start, i);
}

function fakeElement() {
  const classes = new Set();
  const attrs = {};
  return {
    classes,
    classList: {
      toggle: (c) => {
        if (classes.has(c)) classes.delete(c);
        else classes.add(c);
        return classes.has(c);
      },
    },
    setAttribute: (n, v) => (attrs[n] = String(v)),
    getAttribute: (n) => attrs[n],
  };
}

function openingTags(html, cssClass) {
  const re = new RegExp("<(\\w+)[^>]*class=[\"'](?:[^\"']* )?" + cssClass + "(?: [^\"']*)?[\"'][^>]*>", "g");
  const out = [];
  let m;
  while ((m = re.exec(html)) !== null) out.push(m);
  assert.ok(out.length > 0, "no element with class " + cssClass);
  return out;
}

function buttonBodies(html) {
  const out = [];
  const re = /<button[^>]*>([\s\S]*?)<\/button>/g;
  let m;
  while ((m = re.exec(html)) !== null) out.push(m[1]);
  return out;
}

test("every API endpoint row is a real button", () => {
  const tags = openingTags(apiMarkup, "api-endpoint");
  assert.ok(tags.length >= 21);
  for (const tag of tags) {
    assert.equal(tag[1], "button", "an endpoint row must be a <button>: " + tag[0]);
    assert.match(tag[0], /type=["']button["']/);
  }
  assert.ok(!/<(div|span|tr)[^>]*onclick/.test(apiMarkup), "click-only element left in the static markup");
});

test("API endpoint buttons only hold phrasing content", () => {
  for (const b of buttonBodies(apiMarkup)) assert.ok(!/<(div|p|ul|li|h\d|button|a|input)[\s>]/.test(b), "invalid content in a <button>: " + b);
});

test("the Try It tag is a button", () => {
  assert.match(apiHtml, /<button type=['"]button['"] class=['"]api-detail-tag api-detail-tag-tryit['"] onclick=/);
  assert.ok(!/<span[^>]*api-detail-tag-tryit/.test(apiHtml));
});

test("the response headers toggle is a button reporting its state", () => {
  const tag = /<button[^>]*api-playground-resp-headers-toggle[^>]*>/.exec(apiHtml);
  assert.ok(tag, "the toggle must be a <button>");
  assert.match(tag[0], /aria-expanded=["']false["']/);
  const el = fakeElement();
  const parent = fakeElement();
  el.parentNode = parent;
  eval(extractFn(apiHtml, "toggleApiRespHeaders"));
  toggleApiRespHeaders(el);
  assert.ok(parent.classes.has("open"));
  assert.equal(el.getAttribute("aria-expanded"), "true");
  toggleApiRespHeaders(el);
  assert.ok(!parent.classes.has("open"));
  assert.equal(el.getAttribute("aria-expanded"), "false");
});

test("every Resources TOC header is a button reporting its open state", () => {
  const re = /<(\w+)[^>]*class="docs-toc-header"[^>]*>\s*<span>[\s\S]*?<i class="fa fa-chevron-right( open)?"/g;
  let m;
  let n = 0;
  while ((m = re.exec(resourcesHtml)) !== null) {
    n++;
    assert.equal(m[1], "button", "a TOC header must be a <button>");
    assert.match(m[0], /type="button"/);
    assert.match(m[0], new RegExp('aria-expanded="' + (m[2] ? "true" : "false") + '"'), "aria-expanded must match the initial state");
  }
  assert.equal(n, 7);
});

test("toggling a TOC section flips aria-expanded", () => {
  const header = fakeElement();
  const chevron = fakeElement();
  const body = fakeElement();
  header.querySelector = () => chevron;
  header.nextElementSibling = body;
  eval(extractFn(resourcesHtml, "toggleTocSection"));
  toggleTocSection(header);
  assert.equal(header.getAttribute("aria-expanded"), "true");
  assert.ok(body.classes.has("open"));
  toggleTocSection(header);
  assert.equal(header.getAttribute("aria-expanded"), "false");
});

test("profiler query rows hold a keyboard-reachable button", () => {
  const escapeHtml = (s) => String(s);
  eval(extractFn(profilerJs, "profilerRenderQueryRow"));
  const row = profilerRenderQueryRow({ queryText: "SELECT 1", language: "sql", database: "d", executionCount: 1, totalTimeMs: 1, avgTimeMs: 1 }, 3);
  assert.match(row, /<td><button type="button" class="profiler-query-button" title="SELECT 1"/);
  assert.match(row, /onclick="profilerShowDetail\(3\)"/);
});

test("the new buttons lose the native chrome and show a focus ring", () => {
  const rulesOf = (sel) => css.split("}").filter((r) => r.substring(0, r.indexOf("{")).split(",").some((s) => s.trim() === sel));
  for (const sel of [".api-endpoint", ".docs-toc-header", ".api-playground-resp-headers-toggle", ".api-detail-tag-tryit", ".profiler-query-button"]) {
    assert.ok(rulesOf(sel).some((r) => /border:\s*0/.test(r)), sel + " keeps the native border");
    assert.ok(rulesOf(sel + ":focus-visible").some((r) => /outline:\s*2px solid/.test(r)), "no visible focus ring for " + sel);
  }
});
