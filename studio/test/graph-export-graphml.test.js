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

// Export > GraphML is written by Studio's own buildGraphML(), which replaced the cytoscape-graphml plugin (unmaintained
// since 2019, flagged by Meterian). The plugin declared no <key> elements, gave node data a made-up type="data", and
// wrote the record's properties map as the single value "[object Object]", so the file carried no property at all. It
// also called $.isArray, which jQuery 4 removed (issue #10101), and kept a compatibility shim alive for it.
//
// The file follows the layout of the server's GraphML exporter (gremlin's ArcadeGraphMLWriter): TinkerPop's labelV /
// labelE keys, keys declared with the GraphML type every value of them shares, and a value GraphML has no type for
// written as text with an arcadedb:type attribute, so ArcadeGraphMLReader imports the Studio file back with its types.
//
// The end-to-end half is the GraphML case of e2e-studio/tests/graph-export.spec.ts, which downloads the file through the
// real Export menu.
//
// Run with:
//
//     node --test studio/test/graph-export-graphml.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STUDIO = path.join(__dirname, "..");
const STATIC = path.join(STUDIO, "src", "main", "resources", "static");
const src = fs.readFileSync(path.join(STATIC, "js", "studio-graph.js"), "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-graph.js: " + name);
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

eval(extractFn("graphMLEscape"));
eval(extractFn("graphMLValueType"));
eval(extractFn("graphMLMergeType"));
eval(extractFn("graphMLKeyIds"));
eval(extractFn("buildGraphML"));

// Node has no XML parser built in: this checks what a parser would refuse. Every tag closes in order, attribute values
// are quoted, and neither text nor an attribute holds a raw "<" or a "&" that does not start an entity.
function assertWellFormed(xml) {
  assert.match(xml, /^<\?xml version="1\.0" encoding="UTF-8"\?>\n/);
  const body = xml.replace(/^<\?xml[^?]*\?>\n/, "");
  const entity = /&(?!(amp|lt|gt|quot|apos|#\d+);)/;
  const tag = /<(\/?)([A-Za-z][\w.:-]*)((?:\s+[\w.:-]+="[^"<]*")*)\s*(\/?)>/y;
  const stack = [];
  let roots = 0;
  let i = 0;
  while (i < body.length) {
    const lt = body.indexOf("<", i);
    const text = body.substring(i, lt < 0 ? body.length : lt);
    assert.doesNotMatch(text, entity, "a raw & in the text: " + text);
    if (stack.length === 0) assert.match(text, /^\s*$/, "text outside the root element");
    if (lt < 0) break;

    tag.lastIndex = lt;
    const m = tag.exec(body);
    assert.ok(m, "malformed tag at: " + body.substring(lt, lt + 60));
    assert.doesNotMatch(m[3], entity, "a raw & in an attribute: " + m[0]);
    if (m[1] === "/") assert.equal(stack.pop(), m[2], "closing tag out of order: " + m[0]);
    else if (m[4] !== "/") {
      if (stack.length === 0) roots++;
      stack.push(m[2]);
    }
    i = tag.lastIndex;
  }
  assert.deepEqual(stack, [], "unclosed elements");
  assert.equal(roots, 1, "one root element");
}

function unescape(text) {
  return text.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, "&");
}

// The <key> declarations as { id: { for, name, type } }
function keysOf(xml) {
  const keys = {};
  for (const m of xml.matchAll(/<key id="([^"]*)" for="([^"]*)" attr\.name="([^"]*)" attr\.type="([^"]*)"\/>/g))
    keys[unescape(m[1])] = { for: m[2], name: unescape(m[3]), type: m[4] };
  return keys;
}

// The elements of a kind as [{ attributes, data: { attrName: { text, hint } } }], resolved through the keys
function elementsOf(xml, kind) {
  const keys = keysOf(xml);
  const elements = [];
  for (const m of xml.matchAll(new RegExp("<" + kind + "((?: [\\w.:-]+=\"[^\"]*\")*)>([\\s\\S]*?)</" + kind + ">", "g"))) {
    const attributes = {};
    for (const a of m[1].matchAll(/ ([\w.:-]+)="([^"]*)"/g)) attributes[a[1]] = unescape(a[2]);
    const data = {};
    for (const d of m[2].matchAll(/<data key="([^"]*)"(?: arcadedb:type="([^"]*)")?>([\s\S]*?)<\/data>/g)) {
      const key = keys[unescape(d[1])];
      assert.ok(key, "a <data> whose key is not declared: " + d[0]);
      assert.equal(key.for, kind, "a <data> under a key declared for another element: " + d[0]);
      data[key.name] = { text: unescape(d[3]), hint: d[2] == null ? null : unescape(d[2]) };
    }
    elements.push({ attributes, data });
  }
  return elements;
}

const NODES = [
  { id: "#1:0", label: "Alice", fullLabel: "Alice", size: 50, type: "Person", weight: 1, properties: { name: "Alice", age: 30, active: true } },
  { id: "#1:1", label: "Bob", fullLabel: "Bob", size: 44, type: "Person", weight: 1, properties: { name: "Bob", age: 41, active: false } },
];
const EDGES = [{ id: "#9:0", label: "", type: "Knows", source: "#1:0", target: "#1:1", properties: { since: 2010 } }];

test("the document is well-formed GraphML with every key declared before the graph", () => {
  const xml = buildGraphML(NODES, EDGES);
  assertWellFormed(xml);

  assert.match(xml, /<graphml xmlns="http:\/\/graphml\.graphdrawing\.org\/xmlns" /);
  assert.match(xml, / xmlns:arcadedb="https:\/\/arcadedb\.com\/xmlns\/graphml"/);
  assert.match(xml, /<graph id="G" edgedefault="directed">/);
  assert.ok(xml.lastIndexOf("<key ") < xml.indexOf("<graph "), "the keys come before the graph");
});

test("nodes and edges carry their RID, their type and every property of the record", () => {
  const xml = buildGraphML(NODES, EDGES);

  const nodes = elementsOf(xml, "node");
  assert.deepEqual(nodes.map((n) => n.attributes.id), ["#1:0", "#1:1"]);
  assert.deepEqual(nodes[0].data, {
    labelV: { text: "Person", hint: null },
    name: { text: "Alice", hint: null },
    age: { text: "30", hint: null },
    active: { text: "true", hint: null },
  });

  const edges = elementsOf(xml, "edge");
  assert.deepEqual(edges.map((e) => e.attributes), [{ id: "#9:0", source: "#1:0", target: "#1:1" }]);
  assert.deepEqual(edges[0].data, { labelE: { text: "Knows", hint: null }, since: { text: "2010", hint: null } });
});

test("the display-only fields Studio adds to an element are not exported as properties", () => {
  // label, fullLabel, size and weight are what the graph view computed to draw the element, not data of the record
  const names = Object.values(keysOf(buildGraphML(NODES, EDGES))).map((k) => k.name);
  for (const name of ["label", "fullLabel", "size", "weight", "properties", "type", "source", "target"])
    assert.ok(!names.includes(name), name + " is not a key");
});

test("the properties map is never written as [object Object] (what the cytoscape-graphml plugin produced)", () => {
  const xml = buildGraphML(NODES, EDGES);
  assert.ok(!xml.includes("[object Object]"));
  assert.ok(!xml.includes('type="data"'));
});

test("each key is declared with the GraphML type its values share", () => {
  const nodes = [
    { id: "#1:0", type: "V", properties: { i: 1, l: 3000000000, d: 1.5, b: true, s: "x", widened: 1, mixed: 1 } },
    { id: "#1:1", type: "V", properties: { i: -2147483648, l: 1, d: 2, b: false, s: "y", widened: 2.5, mixed: "one" } },
  ];
  const byName = {};
  for (const key of Object.values(keysOf(buildGraphML(nodes, [])))) byName[key.name] = key.type;

  assert.deepEqual(byName, {
    labelV: "string",
    labelE: "string",
    i: "int",
    l: "long",
    d: "double",
    b: "boolean",
    s: "string",
    widened: "double",
    mixed: "string",
  });
});

test("a value of another type than its key's keeps its own type in arcadedb:type", () => {
  const nodes = [
    { id: "#1:0", type: "V", properties: { mixed: 7, flag: true, ratio: 0.5, big: 3000000000 } },
    { id: "#1:1", type: "V", properties: { mixed: "seven", flag: "yes", ratio: "half", big: "big" } },
  ];
  const [first, second] = elementsOf(buildGraphML(nodes, []), "node");

  assert.deepEqual(first.data.mixed, { text: "7", hint: "INTEGER" });
  assert.deepEqual(first.data.flag, { text: "true", hint: "BOOLEAN" });
  assert.deepEqual(first.data.ratio, { text: "0.5", hint: "DOUBLE" });
  assert.deepEqual(first.data.big, { text: "3000000000", hint: "LONG" });
  assert.deepEqual(second.data.mixed, { text: "seven", hint: null });
});

test("an integer under a long or double key is plain GraphML, readable by any reader", () => {
  const nodes = [
    { id: "#1:0", type: "V", properties: { l: 1, d: 1 } },
    { id: "#1:1", type: "V", properties: { l: 3000000000, d: 0.25 } },
  ];
  const [first] = elementsOf(buildGraphML(nodes, []), "node");
  assert.deepEqual(first.data.l, { text: "1", hint: null });
  assert.deepEqual(first.data.d, { text: "1", hint: null });
});

test("a map or a list is written as its JSON text, marked JSON so ArcadeDB reads it back as a map or a list", () => {
  const address = { city: "Rome", zip: ["00100", 1] };
  const nodes = [{ id: "#1:0", type: "V", properties: { address: address, tags: ["a", "b"] } }];
  const [node] = elementsOf(buildGraphML(nodes, []), "node");

  assert.equal(node.data.address.hint, "JSON");
  assert.deepEqual(JSON.parse(node.data.address.text), address);
  assert.equal(node.data.tags.hint, "JSON");
  assert.deepEqual(JSON.parse(node.data.tags.text), ["a", "b"]);
});

test("markup and characters XML refuses are escaped or dropped, in values, names and ids", () => {
  const nasty = 'a < b && c > "d" \'e\'';
  const nodes = [{ id: "#1:0", type: "T<&>", properties: { [nasty]: nasty, ctl: "x\u0000y\u0007z\u001F\ttab" } }];
  const edges = [{ id: "#9:0", type: "E", source: "#1:0", target: "#1:0", properties: { note: "</edge><node id=\"x\">" } }];
  const xml = buildGraphML(nodes, edges);
  assertWellFormed(xml);

  const [node] = elementsOf(xml, "node");
  assert.deepEqual(node.data.labelV, { text: "T<&>", hint: null });
  assert.deepEqual(node.data[nasty], { text: nasty, hint: null });
  assert.deepEqual(node.data.ctl, { text: "xyz\ttab", hint: null });
  assert.equal(elementsOf(xml, "node").length, 1);
  assert.equal(elementsOf(xml, "edge")[0].data.note.text, '</edge><node id="x">');
});

test("key ids are unique: a name nodes and edges share, or a label key's name, gets the element's suffix", () => {
  const nodes = [{ id: "#1:0", type: "V", properties: { name: "n", labelV: "own", labelE: "own too" } }];
  const edges = [{ id: "#9:0", type: "E", source: "#1:0", target: "#1:0", properties: { name: 1, w: 2 } }];
  const xml = buildGraphML(nodes, edges);

  const keys = keysOf(xml);
  assert.deepEqual(Object.keys(keys).sort(), ["labelE", "labelEV", "labelV", "labelVV", "nameE", "nameV", "w"].sort());
  assert.deepEqual(keys.nameV, { for: "node", name: "name", type: "string" });
  assert.deepEqual(keys.nameE, { for: "edge", name: "name", type: "int" });

  // A property called labelV is written under its own key id, the type under the label key
  assert.equal(xml.match(/<data key="labelV">V<\/data>/g).length, 1);
  assert.equal(xml.match(/<data key="labelVV">own<\/data>/g).length, 1);
  assert.equal(xml.match(/<data key="labelEV">own too<\/data>/g).length, 1);
});

test("a null property writes no <data>, and a property null everywhere declares no key", () => {
  const nodes = [
    { id: "#1:0", type: "V", properties: { maybe: null, gone: null } },
    { id: "#1:1", type: "V", properties: { maybe: 5 } },
  ];
  const xml = buildGraphML(nodes, []);

  const names = Object.values(keysOf(xml)).map((k) => k.name);
  assert.ok(names.includes("maybe"));
  assert.ok(!names.includes("gone"));
  const [first, second] = elementsOf(xml, "node");
  assert.equal(first.data.maybe, undefined);
  assert.deepEqual(second.data.maybe, { text: "5", hint: null });
});

test("an empty graph and an element with no properties still make a valid document", () => {
  assertWellFormed(buildGraphML([], []));

  const xml = buildGraphML([{ id: "#1:0", type: "V" }], []);
  assertWellFormed(xml);
  assert.deepEqual(elementsOf(xml, "node")[0].data, { labelV: { text: "V", hint: null } });
});

test("the cytoscape-graphml plugin and the jQuery shim it needed are gone", () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(STUDIO, "package.json"), "utf8"));
  assert.equal((pkg.dependencies || {})["cytoscape-graphml"], undefined);
  assert.ok(!fs.readFileSync(path.join(STUDIO, "webpack.config.js"), "utf8").includes("cytoscape-graphml"));
  assert.ok(!fs.readFileSync(path.join(STATIC, "index.html"), "utf8").includes("cytoscape-graphml"));
  assert.ok(!src.includes("globalCy.graphml("), "the export no longer goes through the plugin");
  assert.ok(!fs.readFileSync(path.join(STATIC, "js", "studio-notify.js"), "utf8").includes("$.isArray"));
});
