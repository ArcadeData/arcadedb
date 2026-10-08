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

// Regression test for issue #9375 (follow-up of #8740): the Query sidebar's Reference panel (collapsible section
// headers, example snippets, Function Reference entries) and the sample-database cards of the quick-start panel and of
// the import-dataset modal were div elements with an inline onclick, so they could not be reached or triggered from the
// keyboard and screen readers did not announce them as buttons. The section headers did not report their open state.
//
// These tests check the rendered markup and the stylesheet. The keyboard behaviour itself (Tab order, Enter/Space
// dispatching a click) needs a real browser; the e2e-studio Playwright suite runs against a released Docker image, so a
// spec there could not see this change before it ships.
//
// Run with:
//
//     node --test studio/test/reference-quickstart-keyboard-accessible.test.js

const { test, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const src = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-database.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-utils.js"), "utf8");
const css = fs.readFileSync(path.join(STATIC_DIR, "css", "studio.css"), "utf8");

function extractFn(source, name) {
  const start = source.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  let i = source.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < source.length && depth > 0) {
    const c = source[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  return source.substring(start, i);
}

// --- Browser stubs -------------------------------------------------------------------------------------------------

let panelHtml;
let globalFunctionReference;

// A MINIMAL ELEMENT MODEL FOR toggleReferenceSection / filterFunctionReference: ENOUGH CLASS AND ATTRIBUTE STATE TO
// OBSERVE WHAT THEY WRITE, WITH NO DOM LIBRARY
function fakeElement() {
  const classes = new Set();
  const attrs = {};
  return {
    classes,
    attrs,
    addClass(c) {
      classes.add(c);
      return this;
    },
    toggleClass(c) {
      if (classes.has(c)) classes.delete(c);
      else classes.add(c);
      return this;
    },
    hasClass(c) {
      return classes.has(c);
    },
    attr(name, value) {
      if (value === undefined) return attrs[name];
      attrs[name] = String(value);
      return this;
    },
  };
}

let header, body, icon, filterTargets;

function $(selector) {
  if (selector === "#sidebarPanelReference")
    return {
      html: function (h) {
        panelHtml = h;
      },
    };
  if (selector === header)
    return {
      next: () => body,
      find: () => icon,
      attr: header.attr.bind(header),
    };
  if (filterTargets && filterTargets[selector]) return filterTargets[selector];
  throw new Error("unexpected selector in test: " + selector);
}

eval(extractFn(utilsSrc, "escapeHtml"));
eval(extractFn(src, "formatNumber"));
eval(extractFn(src, "buildDatasetCardHtml"));
eval(extractFn(src, "populateReferencePanel"));
eval(extractFn(src, "toggleReferenceSection"));
eval(extractFn(src, "filterFunctionReference"));

beforeEach(() => {
  panelHtml = null;
  globalFunctionReference = {
    categories: {
      "SQL Functions": {
        Math: [
          { name: "abs", syntax: "abs(<value>)", description: "Absolute value" },
          { name: "max", syntax: "max(<a>, <b>)", description: "" },
        ],
      },
      "Cypher Functions": {
        String: [{ name: "toupper", syntax: "toUpper(\"x\")", description: "Upper 'case'" }],
      },
    },
  };
  header = fakeElement();
  header.attr("aria-expanded", "false");
  body = fakeElement();
  icon = fakeElement();
  filterTargets = null;
});

// Returns every opening tag of the elements carrying the given class (as one of their classes)
function openingTags(html, cssClass) {
  const re = new RegExp("<(\\w+)[^>]*class=['\"](?:[^'\"]* )?" + cssClass + "(?: [^'\"]*)?['\"][^>]*>", "g");
  const out = [];
  let m;
  while ((m = re.exec(html)) !== null) out.push(m);
  assert.ok(out.length > 0, "no element with class " + cssClass);
  return out;
}

// Returns the inner markup of every <button> element in the html
function buttonBodies(html) {
  const out = [];
  const re = /<button[^>]*>([\s\S]*?)<\/button>/g;
  let m;
  while ((m = re.exec(html)) !== null) out.push(m[1]);
  return out;
}

// --- Reference panel -----------------------------------------------------------------------------------------------

test("every reference section header is a real button reporting its collapsed state", () => {
  populateReferencePanel();
  const tags = openingTags(panelHtml, "reference-section-header");
  // 4 LANGUAGE SECTIONS + 2 FUNCTION REFERENCE SECTIONS
  assert.equal(tags.length, 6);
  for (const tag of tags) {
    assert.equal(tag[1], "button", "a section header must be a <button>: " + tag[0]);
    assert.match(tag[0], /type='button'/);
    assert.match(tag[0], /aria-expanded='false'/, "a collapsed section must say so: " + tag[0]);
    assert.match(tag[0], /onclick='toggleReferenceSection\(this\)'/);
  }
  assert.match(panelHtml, /<i class='fa fa-chevron-right' aria-hidden='true'><\/i>/, "the chevron is decorative");
});

test("every example snippet is a real button that pastes it", () => {
  populateReferencePanel();
  const tags = openingTags(panelHtml, "reference-example").filter((t) => !t[0].includes("fn-ref-item"));
  assert.equal(tags.length, 16 + 5 + 6 + 1);
  for (const tag of tags) {
    assert.equal(tag[1], "button", "an example must be a <button>: " + tag[0]);
    assert.match(tag[0], /type='button'/);
    assert.match(tag[0], /onclick='pasteReferenceExample\(/);
  }
  assert.match(panelHtml, /<button type='button' class='reference-example' title='SELECT' onclick='pasteReferenceExample\("SELECT \* FROM MyType", "sql"\)'>/);
});

test("every Function Reference entry is a real button that inserts its syntax", () => {
  populateReferencePanel();
  const tags = openingTags(panelHtml, "fn-ref-item");
  assert.equal(tags.length, 3);
  for (const tag of tags) {
    assert.equal(tag[1], "button", "a function entry must be a <button>: " + tag[0]);
    assert.match(tag[0], /type='button'/);
    assert.match(tag[0], /onclick='insertFunctionSyntax\(/);
  }
  // THE FILTER MATCHES ON data-name, WHICH MUST SURVIVE THE CHANGE
  assert.match(tags[0][0], /data-name='abs'/);
});

test("no click-only div or span remains in the reference panel", () => {
  populateReferencePanel();
  assert.ok(!/<(div|span|small|code)[^>]*onclick/.test(panelHtml), "click-only element left: " + /<(div|span|small|code)[^>]*onclick[^>]*>/.exec(panelHtml));
});

test("the reference buttons only hold phrasing content, so the browser does not re-parent them", () => {
  populateReferencePanel();
  for (const b of buttonBodies(panelHtml)) assert.ok(!/<(div|p|ul|li|h\d|button|a|input)[\s>]/.test(b), "invalid content in a <button>: " + b);
});

test("toggling a section flips aria-expanded along with the open class", () => {
  toggleReferenceSection(header);
  assert.equal(header.attr("aria-expanded"), "true");
  assert.ok(body.hasClass("open"));
  assert.ok(icon.hasClass("open"));
  toggleReferenceSection(header);
  assert.equal(header.attr("aria-expanded"), "false");
  assert.ok(!body.hasClass("open"));
  assert.ok(!icon.hasClass("open"));
});

test("filtering the function reference marks the sections it auto-expands as expanded", () => {
  const sectionBody = fakeElement();
  const sectionIcon = fakeElement();
  const sectionHeader = fakeElement();
  sectionHeader.attr("aria-expanded", "false");
  const each = () => ({ each: () => {} });
  filterTargets = {
    ".fn-ref-item": each(),
    ".fn-ref-category": each(),
    ".fn-ref-section .reference-section-body": sectionBody,
    ".fn-ref-section .reference-section-header i": sectionIcon,
    ".fn-ref-section .reference-section-header": sectionHeader,
  };
  filterFunctionReference("abs");
  assert.ok(sectionBody.hasClass("open"));
  assert.equal(sectionHeader.attr("aria-expanded"), "true");
});

// --- Sample database cards -----------------------------------------------------------------------------------------

const DATASET = {
  name: "Movies",
  path: "movies/movies.tgz",
  format: "arcadedb",
  icon: "fa-film",
  description: "A small movie graph",
  vertices: 171,
  edges: 253,
  fileSizeMB: 0.5,
  url: "https://example.com/movies",
};

for (const fromModal of [false, true]) {
  const where = fromModal ? "import-dataset modal" : "quick-start panel";

  test("the " + where + " card is imported through a real button", () => {
    const html = buildDatasetCardHtml(DATASET, fromModal);
    const tag = openingTags(html, "quick-start-card-button")[0];
    assert.equal(tag[1], "button");
    assert.match(tag[0], /type="button"/);
    const expected = "importSampleDatabase('Movies', 'movies/movies.tgz', 'arcadedb'" + (fromModal ? ", true" : "") + ")";
    assert.ok(tag[0].includes('onclick="' + expected + '"'), "wrong handler: " + tag[0]);
    assert.match(tag[0], /aria-label="Import sample database Movies"/);
    assert.ok(!/<div[^>]*onclick/.test(html), "the card itself must not stay a click-only div");
  });

  test("the " + where + " card button holds no interactive content, so the info link stays outside it", () => {
    const html = buildDatasetCardHtml(DATASET, fromModal);
    const bodies = buttonBodies(html);
    assert.equal(bodies.length, 1);
    assert.ok(!/<(a|button|input|div)[\s>]/.test(bodies[0]), "invalid content in the card button: " + bodies[0]);
    // THE INFO LINK IS STILL THERE, AND STILL DOES NOT TRIGGER THE IMPORT
    assert.match(html, /<a class="quick-start-card-info" href="https:\/\/example.com\/movies" target="_blank"[^>]*onclick="event.stopPropagation\(\);"/);
    // THE DESCRIPTION AND STATS ARE STILL RENDERED
    assert.ok(html.includes("A small movie graph"));
    assert.ok(html.includes("171 vertices &middot; 253 edges &middot; 500 KB"));
  });
}

test("a card without an info url renders no link", () => {
  const html = buildDatasetCardHtml(Object.assign({}, DATASET, { url: null }), false);
  assert.ok(!html.includes("<a "));
  openingTags(html, "quick-start-card-button");
});

// --- Stylesheet ----------------------------------------------------------------------------------------------------

function ruleFor(selectorPart) {
  const rules = css.split("}");
  return rules.filter((r) => r.substring(0, r.indexOf("{")).includes(selectorPart));
}

test("the new buttons show a visible focus ring", () => {
  for (const sel of [".reference-section-header:focus-visible", ".reference-example:focus-visible", ".quick-start-card-button:focus-visible"]) {
    const rules = ruleFor(sel);
    assert.ok(rules.length > 0, "no focus style for " + sel);
    assert.ok(rules.some((r) => /outline:\s*2px solid/.test(r)), "no visible outline for " + sel);
  }
});

test("the reference buttons lose the native button chrome", () => {
  for (const sel of [".reference-section-header", ".reference-example"]) {
    const rules = ruleFor(sel).filter((r) => /border:\s*0/.test(r));
    assert.ok(rules.length > 0, sel + " keeps the native button border");
    assert.ok(rules.some((r) => /width:\s*100%/.test(r)), sel + " must still span the panel width");
  }
});

test("the card button covers the whole card, with the info link above it", () => {
  assert.ok(ruleFor(".quick-start-card-button::after").some((r) => /position:\s*absolute/.test(r) && /inset:\s*0/.test(r)));
  assert.ok(ruleFor(".quick-start-card-info").some((r) => /z-index:\s*2/.test(r)));
});

test("a focused card is highlighted like a hovered one", () => {
  assert.ok(ruleFor(".quick-start-card:focus-within").some((r) => /border-color/.test(r)));
});
