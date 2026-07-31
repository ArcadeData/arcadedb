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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

// Regression test for issue #5580: schema object names must not be rendered into an inline onclick
// attribute. There the name lands in three nested contexts at once - HTML attribute, JS string
// literal, and finally the handler argument - while only the first one is escaped, so a name holding
// a double quote decodes back to `"` and breaks out of the JS string. The remedy is a data-* attribute
// (one HTML-escape) read back through dataset (a plain string, no further interpretation).
//
// Run with:
//
//     node --test studio/test/schema-action-attributes.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_JS = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const src = fs.readFileSync(path.join(STATIC_JS, "studio-database.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(STATIC_JS, "studio-utils.js"), "utf8");

function extractFrom(source, name) {
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

const extractFn = (name) => extractFrom(src, name);

eval(extractFrom(utilsSrc, "escapeHtml"));
eval(extractFn("schemaActionAttrs"));
eval(extractFn("renderTypeLink"));
eval(extractFn("renderTypeSidebarBadge"));
eval(extractFn("renderTypeQuickActions"));
eval(extractFn("renderMaterializedViewQuickActions"));
eval(extractFn("findTypeInResult"));
eval(extractFn("renderProperties"));
eval(extractFn("renderIndexes"));
eval(extractFn("renderMaterializedViewsSidebarBadges"));
eval(extractFn("mvStatusDotClass"));
eval(extractFn("renderGavSidebarBadges"));

// sidebarBadgeColors is a top-level var in studio-database.js, needed by the MV badge renderer.
const sidebarBadgeColors = { materializedView: ["#a855f7", "#9333ea"] };

// Names that break the inline-onclick pattern. Every one of them is creatable through
// `CREATE DOCUMENT TYPE` with a back-quoted name, so they can genuinely reach these renderers.
const HOSTILE_NAMES = [
  'a"b',
  "a'b",
  "a`b",
  "a\\b",
  "a<script>alert(1)</script>b",
  "a&b",
  'x"); alert(1); //',
  "it's a \"type\"",
];

/** Reverses escapeHtml, i.e. what the browser does when it hands the value back through dataset. */
function htmlDecode(value) {
  return value
    .replace(/&#039;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

/** Reads every single-quoted `data-<attr>` value out of a rendered HTML fragment, decoded. */
function readDataAttrs(html, attr) {
  const out = [];
  const re = new RegExp("data-" + attr + "='([^']*)'", "g");
  let m;
  while ((m = re.exec(html)) !== null) out.push(htmlDecode(m[1]));
  return out;
}

/**
 * An inline handler is only safe when it carries no argument at all. The section headers keep theirs
 * ("createMaterializedView(); return false;") because they pass nothing; the moment a name becomes an
 * argument it enters the JS-string-inside-an-HTML-attribute context that issue #5580 is about.
 */
function assertNoArgumentCarryingInlineHandler(html) {
  const re = /onclick='([^']*)'/g;
  let m;
  while ((m = re.exec(html)) !== null)
    assert.ok(/^[A-Za-z_$][\w$]*\(\);(\s*return false;)?\s*$/.test(m[1]), "inline handler must not carry an argument: " + m[1]);
}

/** A name is safe in an attribute only if it can never terminate it: no raw quote, `<` or `&`. */
function assertNoAttributeBreakout(html) {
  // Everything between the delimiters of a data-* attribute must be free of raw single quotes.
  const re = /data-[a-z-]+='([^']*)'/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    assert.ok(!/[<>&](?!(amp|quot|lt|gt|#039);)/.test(m[1]), "attribute value holds an unencoded markup character: " + m[1]);
  }
}

test("schemaActionAttrs escapes the name exactly once and round-trips through dataset", () => {
  for (const name of HOSTILE_NAMES) {
    const attrs = schemaActionAttrs("drop-type", name);
    assert.ok(attrs.includes("data-action='drop-type'"), "action must be emitted");
    assert.deepStrictEqual(readDataAttrs(attrs, "name"), [name], "name must survive one HTML-escape unchanged: " + name);
    assertNoAttributeBreakout(attrs);
  }
});

test("schemaActionAttrs carries the owning type in data-parent when given", () => {
  const attrs = schemaActionAttrs("drop-property", 'prop"1', 'Type"A');
  assert.deepStrictEqual(readDataAttrs(attrs, "name"), ['prop"1']);
  assert.deepStrictEqual(readDataAttrs(attrs, "parent"), ['Type"A']);
});

test("schemaActionAttrs omits data-parent when no owner is given", () => {
  assert.ok(!schemaActionAttrs("drop-type", "T").includes("data-parent"), "no spurious empty data-parent attribute");
});

test("type links carry the name in a data attribute, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const html = renderTypeLink(name);
    assert.ok(!html.includes("onclick"), "type link must not use an inline handler: " + html);
    assert.deepStrictEqual(readDataAttrs(html, "name"), [name]);
    assertNoAttributeBreakout(html);
  }
});

test("sidebar type badges carry the name in a data attribute, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const html = renderTypeSidebarBadge({ name: name, records: 7 }, "#3b82f6", "show-type-detail");
    assert.ok(!html.includes("onclick"), "sidebar badge must not use an inline handler: " + html);
    assert.deepStrictEqual(readDataAttrs(html, "name"), [name]);
    assert.ok(html.includes("data-action='show-type-detail'"), "badge must declare the requested action");
    assertNoAttributeBreakout(html);
  }
});

test("sidebar type badges honour the action they are rendered for", () => {
  const html = renderTypeSidebarBadge({ name: "Person", records: 1 }, "#3b82f6", "browse-type");
  assert.ok(html.includes("data-action='browse-type'"));
});

test("type quick actions carry the name in a data attribute, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const html = renderTypeQuickActions({ name: name, type: "vertex" });
    assert.ok(!html.includes("onclick"), "quick actions must not use inline handlers: " + html);
    for (const decoded of readDataAttrs(html, "name")) assert.strictEqual(decoded, name);
    assertNoAttributeBreakout(html);
  }
});

test("type quick actions cover browse, count and drop, plus connections for vertices only", () => {
  const vertex = renderTypeQuickActions({ name: "Person", type: "vertex" });
  assert.ok(vertex.includes("data-action='browse-records'"));
  assert.ok(vertex.includes("data-action='browse-records-with-connections'"));
  assert.ok(vertex.includes("data-action='count-records'"));
  assert.ok(vertex.includes("data-action='drop-type'"));

  const doc = renderTypeQuickActions({ name: "Log", type: "document" });
  assert.ok(!doc.includes("browse-records-with-connections"), "non-vertex types have no connections action");
  assert.ok(doc.includes("data-action='browse-records'"));
});

test("materialized view quick actions carry the name in a data attribute, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const html = renderMaterializedViewQuickActions({ name: name });
    assert.ok(!html.includes("onclick"), "MV quick actions must not use inline handlers: " + html);
    for (const decoded of readDataAttrs(html, "name")) assert.strictEqual(decoded, name);
    assertNoAttributeBreakout(html);
  }
  const html = renderMaterializedViewQuickActions({ name: "SalesMv" });
  assert.ok(html.includes("data-action='refresh-materialized-view'"));
  assert.ok(html.includes("data-action='browse-records'"));
  assert.ok(html.includes("data-action='alter-materialized-view'"));
  assert.ok(html.includes("data-action='drop-materialized-view'"));
});

test("property rows drop-property through data attributes, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const row = { name: name, parentTypes: "", properties: [{ name: name + "-prop", type: "STRING" }], indexes: [] };
    const html = renderProperties(row, [row]);
    assert.ok(!html.includes("onclick"), "property row must not use an inline handler: " + html);
    assert.deepStrictEqual(readDataAttrs(html, "name"), [name + "-prop"], "data-name is the property being dropped");
    assert.deepStrictEqual(readDataAttrs(html, "parent"), [name], "data-parent is the owning type");
    assertNoAttributeBreakout(html);
  }
});

test("index rows drop-index through data attributes, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const row = {
      name: name,
      parentTypes: [],
      indexes: [{ name: name + "[k]", typeName: name, properties: ["k"], type: "LSM_TREE", unique: false, automatic: true }],
    };
    const html = renderIndexes(row, [row]);
    assert.ok(!html.includes("onclick"), "index row must not use an inline handler: " + html);
    assert.deepStrictEqual(readDataAttrs(html, "name"), [name + "[k]"], "data-name is the index being dropped");
    assert.deepStrictEqual(readDataAttrs(html, "parent"), [name], "data-parent is the owning type");
    assertNoAttributeBreakout(html);
  }
});

test("materialized view sidebar badges carry the name in a data attribute, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const views = [{ name: name, status: "VALID" }];

    const schemaSidebar = renderMaterializedViewsSidebarBadges(views, false);
    assertNoArgumentCarryingInlineHandler(schemaSidebar);
    assert.deepStrictEqual(readDataAttrs(schemaSidebar, "name"), [name]);
    assert.ok(schemaSidebar.includes("data-action='show-materialized-view-detail'"));

    const querySidebar = renderMaterializedViewsSidebarBadges(views, true);
    assertNoArgumentCarryingInlineHandler(querySidebar);
    assert.deepStrictEqual(readDataAttrs(querySidebar, "name"), [name]);
    assert.ok(querySidebar.includes("data-action='browse-records'"));
  }
});

test("graph analytical view sidebar badges carry the name in a data attribute, not an onclick", () => {
  for (const name of HOSTILE_NAMES) {
    const html = renderGavSidebarBadges([{ name: name, status: "VALID" }], false);
    assertNoArgumentCarryingInlineHandler(html);
    assert.deepStrictEqual(readDataAttrs(html, "name"), [name]);
    assert.ok(html.includes("data-action='show-gav-detail'"));
    assertNoAttributeBreakout(html);
  }
});

test("graph analytical view badges stay inert in the query sidebar", () => {
  const html = renderGavSidebarBadges([{ name: "Ranks", status: "VALID" }], true);
  assert.ok(!html.includes("data-action="), "the query sidebar renders GAV badges without an action");
});

/**
 * Every action string the renderers can emit. Actions reach an attribute either as a literal argument to
 * schemaActionAttrs() - including through a ternary - or as the `action` parameter of
 * renderTypeSidebarBadge(), so both call shapes are scanned.
 */
function emittedActions() {
  const actions = new Set();
  for (const call of ["schemaActionAttrs(", "renderTypeSidebarBadge("]) {
    let i = src.indexOf(call);
    while (i >= 0) {
      let j = i + call.length;
      let depth = 1;
      while (j < src.length && depth > 0) {
        if (src[j] === "(") depth++;
        else if (src[j] === ")") depth--;
        j++;
      }
      const args = src.substring(i + call.length, j - 1);
      const re = /"([a-z][a-z0-9]*(?:-[a-z0-9]+)+)"/g;
      let m;
      while ((m = re.exec(args)) !== null) actions.add(m[1]);
      i = src.indexOf(call, j);
    }
  }
  return actions;
}

/** Collects the action keys of a `var <name> = { ... };` registry literal in studio-database.js. */
function registryKeys(registryName) {
  const start = src.indexOf("var " + registryName + " = {");
  assert.notStrictEqual(start, -1, "registry not found: " + registryName);
  const end = src.indexOf("\n};", start);
  assert.notStrictEqual(end, -1, "registry not terminated: " + registryName);
  const body = src.substring(start, end);
  const keys = new Set();
  const re = /"([a-z0-9-]+)":\s*function/g;
  let m;
  while ((m = re.exec(body)) !== null) keys.add(m[1]);
  return keys;
}

// Source-level guard: every action string emitted through schemaActionAttrs must be present in one of the
// dispatch registries, otherwise the control renders but silently does nothing when clicked. A typo on
// either side is invisible in the browser, so it is caught here instead.
test("every emitted data-action is covered by a dispatch registry", () => {
  const emitted = emittedActions();
  assert.ok(emitted.size >= 15, "expected the schema renderers to emit the full action set, found " + emitted.size);

  const handled = new Set([...registryKeys("schemaClickActions"), ...registryKeys("schemaChangeActions")]);
  for (const action of emitted) assert.ok(handled.has(action), "no delegated handler for data-action='" + action + "'");
});

test("no dispatch registry entry is dead weight", () => {
  const emitted = emittedActions();
  for (const action of [...registryKeys("schemaClickActions"), ...registryKeys("schemaChangeActions")])
    assert.ok(emitted.has(action), "registry handles data-action='" + action + "' but nothing emits it");
});

// The names reaching these renderers are schema identifiers, so a value that looks like a number or a
// boolean is a legitimate type name. jQuery's .data() coerces those ("123" -> 123, "true" -> true);
// dataset does not. The handlers must therefore read dataset, or a type named `123` reaches
// quoteSqlName() as a number.
test("delegated handlers read dataset rather than jQuery .data()", () => {
  const start = src.indexOf("Delegated handlers for the schema actions");
  assert.notStrictEqual(start, -1, "dispatcher block not found");
  const block = src.substring(start, src.indexOf("function displaySchema("));
  assert.ok(block.includes("this.dataset.action"), "the dispatcher must key off dataset.action");
  assert.ok(!/\$\(this\)\.data\(/.test(block), "jQuery .data() coerces numeric and boolean-looking names, use dataset");
});
