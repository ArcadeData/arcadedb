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

// Issue #6550: a schema with more than 1200 types is a wall of badges in both type sidebars (Database > Schema and the
// Query page's overview), with no way to find one type. Each sidebar now has a filter box. The sidebars are rendered by
// the real displaySchema() / populateQuerySidebar() into a small DOM built from the markup they emit, so what is tested
// is the badges Studio actually renders, read back the way a browser reads them. Run with:
//
//     node --test studio/test/type-sidebar-filter.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const src = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-database.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-utils.js"), "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-database.js: " + name);
  let i = src.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  return src.substring(start, i);
}

// A declaration `var NAME = {...};` (or `const`), rebound as `var` so it lands in this module's scope.
function extractDeclaration(name) {
  const re = new RegExp("(?:var|const|let) " + name + " = ");
  const m = re.exec(src);
  if (m == null) throw new Error("declaration not found in studio-database.js: " + name);
  const end = src.indexOf(";\n", m.index);
  return "var " + src.substring(m.index + m[0].indexOf(name), end + 1);
}

// ---------------------------------------------------------------------------------------------------------------------
// A minimal DOM: enough of Element for the markup the sidebars emit, parsed strictly so that markup the renderers did not
// escape shows up as an element of its own instead of being silently read as text.
// ---------------------------------------------------------------------------------------------------------------------

const VOID_TAGS = new Set(["input", "br", "img", "hr"]);

function decodeEntities(text) {
  return text.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#0?39;/g, "'").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&");
}

class TextNode {
  constructor(text) {
    this.nodeType = 3;
    this.text = text;
  }

  get textContent() {
    return this.text;
  }
}

class Element {
  constructor(tagName, attributes) {
    this.nodeType = 1;
    this.tagName = tagName.toLowerCase();
    this.attributes = attributes || {};
    this.childNodes = [];
    this.value = "";
    const self = this;
    this.classList = {
      contains: (c) => self.classes().includes(c),
      add: (c) => {
        if (!self.classes().includes(c)) self.attributes["class"] = self.classes().concat([c]).join(" ");
      },
      remove: (c) => {
        self.attributes["class"] = self.classes().filter((x) => x !== c).join(" ");
      },
      toggle: (c, force) => {
        const want = force === undefined ? !self.classes().includes(c) : !!force;
        if (want) self.classList.add(c);
        else self.classList.remove(c);
        return want;
      }
    };
  }

  classes() {
    const value = this.attributes["class"];
    return value == null || value === "" ? [] : value.split(/\s+/).filter((x) => x !== "");
  }

  get id() {
    return this.attributes.id;
  }

  get children() {
    return this.childNodes.filter((n) => n.nodeType === 1);
  }

  get textContent() {
    return this.childNodes.map((n) => n.textContent).join("");
  }

  set textContent(text) {
    this.childNodes = [new TextNode(String(text))];
  }

  set innerHTML(html) {
    this.childNodes = [];
    parseInto(this, html);
  }

  descendants() {
    const out = [];
    const walk = (el) => {
      for (const child of el.children) {
        out.push(child);
        walk(child);
      }
    };
    walk(this);
    return out;
  }

  // Single class selectors only, which is all the code under test uses.
  querySelectorAll(selector) {
    assert.match(selector, /^\.[\w-]+$/, "the fake DOM only answers single class selectors: " + selector);
    const cls = selector.substring(1);
    return this.descendants().filter((el) => el.classList.contains(cls));
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }
}

function parseAttributes(text) {
  const out = {};
  const re = /([^\s=]+)(?:\s*=\s*(?:'([^']*)'|"([^"]*)"|([^\s'">]+)))?/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const raw = m[2] !== undefined ? m[2] : m[3] !== undefined ? m[3] : m[4] !== undefined ? m[4] : "";
    out[m[1]] = decodeEntities(raw);
  }
  return out;
}

function parseInto(parent, html) {
  const stack = [parent];
  const re = /<(\/?)([a-zA-Z][a-zA-Z0-9-]*)((?:\s+[^\s=>\/]+(?:\s*=\s*(?:'[^']*'|"[^"]*"|[^\s'">]+))?)*)\s*(\/?)>|([^<]+)/g;
  let pos = 0;
  let m;
  while ((m = re.exec(html)) !== null) {
    assert.equal(m.index, pos, "unparseable markup at " + pos + ": " + html.substring(pos, pos + 60));
    pos = re.lastIndex;
    const top = stack[stack.length - 1];
    if (m[5] !== undefined) {
      top.childNodes.push(new TextNode(decodeEntities(m[5])));
    } else if (m[1] === "/") {
      assert.equal(top.tagName, m[2].toLowerCase(), "mismatched closing tag </" + m[2] + ">");
      stack.pop();
    } else {
      const el = new Element(m[2], parseAttributes(m[3]));
      top.childNodes.push(el);
      if (!VOID_TAGS.has(el.tagName) && m[4] !== "/") stack.push(el);
    }
  }
  assert.equal(pos, html.length, "unparseable markup at " + pos + ": " + html.substring(pos, pos + 60));
  assert.equal(stack.length, 1, "unclosed element <" + stack[stack.length - 1].tagName + ">");
}

// The page around the two sidebars: what database.html and query.html hold (their wiring is checked against the real
// pages by the last test).
const page = new Element("body");
page.innerHTML =
  "<input id='dbTypeFilter'><div id='dbTypeFilterStatus'></div><div id='dbTypeBadges'></div><div id='dbTypeDetail'></div>" +
  "<input id='queryTypeFilter'><div id='queryTypeFilterStatus'></div><div id='queryOverviewTypes'></div>";

const document = {
  getElementById: (id) => page.descendants().find((el) => el.id === id) || null
};
const window = {};

function $(selector) {
  assert.match(selector, /^#[\w-]+$/, "the jQuery stub only answers id selectors: " + selector);
  const el = document.getElementById(selector.substring(1));
  return {
    length: el == null ? 0 : 1,
    html: (markup) => {
      if (el != null) el.innerHTML = markup;
    }
  };
}

// What the renderers call besides the functions under test.
let schemaTypes = [];
let materializedViews = [];
let graphAnalyticalViews = [];
let globalSchemaTypes = null;
const globalSidebarTypeColors = {};
function stopMaterializedViewAutoRefresh() {}
function fetchSchemaTypes(callback) {
  callback(schemaTypes);
}
function fetchMaterializedViews(callback) {
  callback(materializedViews);
}
function fetchGraphAnalyticalViews(callback) {
  callback(graphAnalyticalViews);
}
function studioAction() {
  return "";
}

eval(utilsSrc.substring(utilsSrc.indexOf("function escapeHtml("), utilsSrc.indexOf("\n}", utilsSrc.indexOf("function escapeHtml(")) + 2));
eval(extractDeclaration("LIGHTWEIGHT_EDGE_HINT"));
eval(extractDeclaration("sidebarBadgeColors"));
eval(extractDeclaration("TYPE_SIDEBAR_FILTERS"));
for (const fn of [
  "schemaActionAttrs", "formatSectionTotal", "renderTypeSidebarBadge", "renderMaterializedViewsSidebarBadges", "renderGavSidebarBadges",
  "mvStatusDotClass", "displaySchema", "populateQuerySidebar", "filterTypeSidebar", "normalizeTypeSidebarFilter", "typeNameMatchesFilter",
  "applyTypeSidebarFilter", "typeSidebarFilterStatus"
])
  eval(extractFn(fn));

// ---------------------------------------------------------------------------------------------------------------------

const HIDDEN = "sidebar-filter-hidden";

// The names of the badges a browser would show: a badge counts only when neither it nor any element above it is hidden.
function shownBadgeNames(containerId) {
  const out = [];
  const walk = (el, visible) => {
    for (const child of el.children) {
      const v = visible && !child.classList.contains(HIDDEN);
      if (child.classList.contains("sidebar-badge") && v) out.push(child.querySelector(".sidebar-badge-name").textContent);
      walk(child, v);
    }
  };
  walk(document.getElementById(containerId), true);
  return out;
}

function sectionByLabel(containerId, label) {
  const section = document
    .getElementById(containerId)
    .querySelectorAll(".sidebar-section")
    .find((s) => s.querySelector(".sidebar-section-header").textContent.includes(label));
  assert.ok(section != null, "no section '" + label + "' in #" + containerId);
  return section;
}

function subsectionHeader(containerId, label) {
  const header = document
    .getElementById(containerId)
    .querySelectorAll(".sidebar-subsection-header")
    .find((h) => h.textContent.includes(label));
  assert.ok(header != null, "no group '" + label + "' in #" + containerId);
  return header;
}

function typeFilter(inputId, value) {
  document.getElementById(inputId).value = value;
}

function statusText(statusId) {
  return document.getElementById(statusId).textContent;
}

const TYPES = [
  { name: "Person", type: "vertex", records: 10 },
  { name: "PersonAddress", type: "vertex", records: 3 },
  { name: "Company", type: "vertex", records: 12 },
  { name: "Knows", type: "edge", records: 40 },
  { name: "WorksFor", type: "edge", records: 0, lightweight: true },
  { name: "Invoice", type: "document", records: 7 }
];

function loadSchema(types, views, gavs) {
  schemaTypes = types;
  globalSchemaTypes = types;
  materializedViews = views || [];
  graphAnalyticalViews = gavs || [];
}

function resetFilters() {
  typeFilter("dbTypeFilter", "");
  typeFilter("queryTypeFilter", "");
}

test("the schema sidebar keeps only the types whose name contains the text, whatever its case", () => {
  resetFilters();
  loadSchema(TYPES);
  displaySchema();
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), ["Person", "PersonAddress", "Company", "Knows", "WorksFor", "Invoice"]);

  typeFilter("dbTypeFilter", "PERS");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), ["Person", "PersonAddress"]);
  assert.equal(statusText("dbTypeFilterStatus"), "2 of 6 match");

  typeFilter("dbTypeFilter", "  address ");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), ["PersonAddress"], "surrounding blanks are not part of the filter");
});

test("only the name is matched, not the record count the badge also shows", () => {
  resetFilters();
  loadSchema(TYPES);
  displaySchema();

  typeFilter("dbTypeFilter", "12");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), [], "Company holds 12 records, which must not make it match '12'");
  assert.equal(statusText("dbTypeFilterStatus"), "No match");

  typeFilter("dbTypeFilter", "lightweight");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), [], "the 'lightweight' label of an edge type is not its name");
});

test("a section with no match is hidden, and every section comes back when the filter is cleared", () => {
  resetFilters();
  loadSchema(TYPES);
  displaySchema();

  typeFilter("dbTypeFilter", "knows");
  filterTypeSidebar("dbTypeBadges");
  assert.ok(!sectionByLabel("dbTypeBadges", "Edges").classList.contains(HIDDEN), "the section holding the match stays");
  for (const label of ["Vertices", "Documents", "TimeSeries", "OLAP"])
    assert.ok(sectionByLabel("dbTypeBadges", label).classList.contains(HIDDEN), "section " + label + " has no match and must be hidden");

  typeFilter("dbTypeFilter", "");
  filterTypeSidebar("dbTypeBadges");
  assert.equal(document.getElementById("dbTypeBadges").descendants().filter((el) => el.classList.contains(HIDDEN)).length, 0,
    "an empty filter hides nothing, the empty TimeSeries section with its create button included");
  assert.equal(statusText("dbTypeFilterStatus"), "", "no count is shown while nothing is filtered");
});

test("materialized views and graph analytical views are filtered too, each group header with its own badges", () => {
  resetFilters();
  loadSchema(TYPES, [{ name: "PersonStats", status: "VALID" }], [{ name: "SocialGraph", status: "READY" }]);
  displaySchema();

  typeFilter("dbTypeFilter", "stats");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), ["PersonStats"]);
  assert.ok(!sectionByLabel("dbTypeBadges", "OLAP").classList.contains(HIDDEN));
  assert.ok(!subsectionHeader("dbTypeBadges", "Materialized Views").classList.contains(HIDDEN));
  assert.ok(subsectionHeader("dbTypeBadges", "Graph Analytical Views").classList.contains(HIDDEN), "a group with no match loses its header");

  typeFilter("dbTypeFilter", "social");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), ["SocialGraph"]);
  assert.ok(subsectionHeader("dbTypeBadges", "Materialized Views").classList.contains(HIDDEN));
  assert.ok(!subsectionHeader("dbTypeBadges", "Graph Analytical Views").classList.contains(HIDDEN));
  assert.equal(statusText("dbTypeFilterStatus"), "1 of 8 match", "views count among the badges the filter went through");
});

test("what was typed survives the sidebar being rebuilt, and applies to the types the rebuild brings", () => {
  resetFilters();
  loadSchema(TYPES);
  displaySchema();
  typeFilter("dbTypeFilter", "comp");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), ["Company"]);

  // Refresh, or a type created from the sidebar: the badges are rendered again from a new schema listing.
  loadSchema(TYPES.concat([{ name: "CompanyBranch", type: "vertex", records: 1 }, { name: "Order", type: "document", records: 1 }]));
  displaySchema();
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), ["Company", "CompanyBranch"]);
  assert.equal(statusText("dbTypeFilterStatus"), "2 of 8 match");
});

test("the Query page overview filters its badges the same way and keeps the filter across a refresh", () => {
  resetFilters();
  loadSchema(TYPES, [{ name: "PersonStats", status: "VALID" }], []);
  populateQuerySidebar();
  assert.deepEqual(shownBadgeNames("queryOverviewTypes"), ["Person", "PersonAddress", "Company", "Knows", "WorksFor", "Invoice", "PersonStats"]);

  typeFilter("queryTypeFilter", "person");
  filterTypeSidebar("queryOverviewTypes");
  assert.deepEqual(shownBadgeNames("queryOverviewTypes"), ["Person", "PersonAddress", "PersonStats"]);
  assert.ok(sectionByLabel("queryOverviewTypes", "Edges").classList.contains(HIDDEN));
  assert.equal(statusText("queryTypeFilterStatus"), "3 of 7 match");

  loadSchema(TYPES.concat([{ name: "PersonNote", type: "document", records: 2 }]), [], []);
  populateQuerySidebar();
  assert.deepEqual(shownBadgeNames("queryOverviewTypes"), ["Person", "PersonAddress", "PersonNote"]);

  // A database with no type at all: the empty notice replaces the badges and the count goes with them.
  loadSchema([], [], []);
  populateQuerySidebar();
  assert.equal(statusText("queryTypeFilterStatus"), "No match");
  assert.ok(document.getElementById("queryOverviewTypes").textContent.includes("No types defined"));

  // The two sidebars filter independently.
  typeFilter("dbTypeFilter", "");
  loadSchema(TYPES);
  displaySchema();
  assert.equal(shownBadgeNames("dbTypeBadges").length, 6, "the Query page's filter does not reach the schema sidebar");
});

test("a type name carrying markup or quotes is matched as the name it is and never becomes markup", () => {
  resetFilters();
  const hostile = "<img src=x onerror=alert(1)>";
  const quoted = "O'Brien \"Q\" & Co";
  loadSchema(TYPES.concat([{ name: hostile, type: "vertex", records: 1 }, { name: quoted, type: "document", records: 1 }]));
  displaySchema();
  assert.equal(document.getElementById("dbTypeBadges").descendants().filter((el) => el.tagName === "img").length, 0,
    "the type name was injected into the sidebar as markup");

  typeFilter("dbTypeFilter", "<IMG");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), [hostile], "the name must be matched as text, not as its escaped markup");

  typeFilter("dbTypeFilter", "o'brien \"q\" &");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), [quoted]);

  typeFilter("dbTypeFilter", "&lt;");
  filterTypeSidebar("dbTypeBadges");
  assert.deepEqual(shownBadgeNames("dbTypeBadges"), [], "the escaped form of a name is not what the user sees, so it must not match");

  typeFilter("dbTypeFilter", "<b>bold</b>");
  filterTypeSidebar("dbTypeBadges");
  const status = document.getElementById("dbTypeFilterStatus");
  assert.equal(status.children.length, 0, "the status line is text only");
  assert.ok(!status.textContent.includes("<b>"), "what was typed is never echoed into the page");
});

test("a schema of more than a thousand types is filtered in place and counted with grouping separators", () => {
  resetFilters();
  const many = [];
  for (let i = 0; i < 1500; i++) many.push({ name: "Type" + String(i).padStart(4, "0"), type: i % 3 === 0 ? "edge" : "vertex", records: i });
  loadSchema(many);
  displaySchema();

  typeFilter("dbTypeFilter", "type01");
  filterTypeSidebar("dbTypeBadges");
  assert.equal(shownBadgeNames("dbTypeBadges").length, 100);
  assert.equal(statusText("dbTypeFilterStatus"), "100 of 1,500 match");

  // The badges are the same nodes, only shown or hidden: no re-render per keystroke.
  const before = document.getElementById("dbTypeBadges").querySelectorAll(".sidebar-badge");
  typeFilter("dbTypeFilter", "type014");
  filterTypeSidebar("dbTypeBadges");
  const after = document.getElementById("dbTypeBadges").querySelectorAll(".sidebar-badge");
  assert.equal(after.length, 1500);
  for (let i = 0; i < before.length; i++) assert.equal(after[i], before[i], "badge " + i + " was replaced while filtering");
  assert.equal(shownBadgeNames("dbTypeBadges").length, 10);
});

test("an unknown sidebar and a missing container are ignored", () => {
  assert.doesNotThrow(() => filterTypeSidebar("noSuchSidebar"));
  assert.deepEqual(applyTypeSidebarFilter(null, "x"), { matched: 0, total: 0 });
  assert.equal(typeSidebarFilterStatus({ matched: 0, total: 0 }, "   "), "", "a blank filter is no filter");
});

// The authority is TYPE_SIDEBAR_FILTERS: every sidebar it names must have its filter box wired to it in a Studio page,
// with the status line it names, and the box must name the action the CSP dispatcher calls.
test("every filtered sidebar has its filter box and status line in a Studio page", () => {
  const pages = ["database.html", "query.html"].map((f) => ({ file: f, html: fs.readFileSync(path.join(STATIC_DIR, f), "utf8") }));
  const keys = Object.keys(TYPE_SIDEBAR_FILTERS);
  assert.ok(keys.includes("dbTypeBadges") && keys.includes("queryOverviewTypes"),
    "TYPE_SIDEBAR_FILTERS no longer reaches the two type sidebars: " + keys.join(", "));

  for (const containerId of keys) {
    const config = TYPE_SIDEBAR_FILTERS[containerId];
    const host = pages.find((p) => p.html.includes('id="' + containerId + '"'));
    assert.ok(host != null, "no Studio page holds the sidebar #" + containerId + " that TYPE_SIDEBAR_FILTERS names");

    const inputTag = new RegExp("<input[^>]*\\bid=\"" + config.input + "\"[^>]*>").exec(host.html);
    assert.ok(inputTag != null, host.file + " has no filter box #" + config.input + " for #" + containerId);
    const input = parseAttributes(inputTag[0].substring("<input".length, inputTag[0].length - 1));
    assert.equal(input["data-on-input"], "filterTypeSidebar", "#" + config.input + " must call filterTypeSidebar on input");
    assert.deepEqual(JSON.parse(input["data-args"]), [containerId], "#" + config.input + " must filter #" + containerId);
    assert.ok(host.html.includes('id="' + config.status + '"'), host.file + " has no status line #" + config.status);
  }
});
