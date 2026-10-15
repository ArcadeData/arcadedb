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

// Regression test for issues #10082 and #10083: the record editor computed the UPDATE of a Save from two different
// baselines. The SET half compared each field against a `data-original` attribute read back through jQuery `.data()`,
// which COERCES ("12345" -> 12345, "true" -> true, "null" -> null, "{}" -> {}) while `.val()` is always a String, so
// pressing Save without touching anything rewrote those properties and retyped them (#10082). And that attribute was
// regenerated from the LIVE property map on every render, so adding or removing a property re-baselined every edit
// still on screen: the edit was either wiped (Add Property) or silently left out of the statement (remove, #10083).
//
// The statement now derives from ONE baseline, the record as loaded (originalProperties), against the form as it is.
// The invariant pinned here: the statement a Save produces does not depend on how many times the form was re-rendered
// in between, and a Save that follows no edit produces none.
//
// The real studio-record-editor.js is loaded, and the DOM is a stub whose `.data()` is jQuery 4.0.0's getData()
// verbatim, so the coercion the defect rode on is reproduced rather than assumed away.
//
// @author Luca Garulli (l.garulli@arcadedata.com)
//
// Run with:
//
//     node --test studio/test/record-editor-save.test.js

const { test, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_DIR = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const editorSrc = fs.readFileSync(path.join(JS_DIR, "studio-record-editor.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(JS_DIR, "studio-utils.js"), "utf8");

function extractFn(source, name) {
  const start = source.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  let i = source.indexOf("{", start) + 1;
  let depth = 1;
  while (i < source.length && depth > 0) {
    const c = source[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  return source.substring(start, i);
}

// --- Browser stubs -------------------------------------------------------------------------------------------------

// jQuery 4.0.0 src/data.js getData(), verbatim: what `.data("x")` returns for the string held by a data-x attribute
const rbrace = /^(?:\{[\w\W]*\}|\[[\w\W]*\])$/;
function getData(data) {
  if (data === "true") return true;
  if (data === "false") return false;
  if (data === "null") return null;
  if (data === +data + "") return +data;
  if (rbrace.test(data)) return JSON.parse(data);
  return data;
}

function unescapeHtml(s) {
  return s.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&amp;/g, "&");
}

// What the browser builds out of the markup the editor renders: one entry per .record-editor-field
function parseFields(html) {
  const fields = [];
  const re = /<input\b([^>]*)\/>|<textarea\b([^>]*)>([\s\S]*?)<\/textarea>/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    const textarea = m[2] !== undefined;
    const attrText = textarea ? m[2] : m[1];
    if (!/\brecord-editor-field\b/.test(attrText)) continue;
    const attrs = {};
    const attrRe = /([\w-]+)='([^']*)'/g;
    let a;
    while ((a = attrRe.exec(attrText)) !== null) attrs[a[1]] = a[2];
    // <input type=text> strips line breaks from its value. A <textarea> keeps them, but the parser normalizes CRLF and a lone CR
    // to LF and drops ONE line feed that directly follows the start tag
    let raw;
    if (textarea) {
      raw = unescapeHtml(m[3]).replace(/\r\n?/g, "\n");
      if (raw.startsWith("\n")) raw = raw.substring(1);
    } else raw = unescapeHtml(attrs.value || "").replace(/[\r\n]/g, "");
    fields.push({ attrs, disabled: /\sdisabled(\s|\/|$)/.test(attrText), value: raw });
  }
  return fields;
}

function wrap(field) {
  return {
    prop: (name) => (name === "disabled" ? field.disabled : undefined),
    val(v) {
      if (arguments.length === 0) return field.value;
      field.value = v;
      return this;
    },
    data: (name) => (field.attrs["data-" + name] === undefined ? undefined : getData(unescapeHtml(field.attrs["data-" + name]))),
    attr: (name) => (field.attrs[name] === undefined ? undefined : unescapeHtml(field.attrs[name])),
    focus() {
      field.focused = true;
      return this;
    },
    __field: field,
  };
}

function collection(fields) {
  const wrappers = fields.map(wrap);
  return {
    length: wrappers.length,
    each(fn) {
      wrappers.forEach((w, i) => fn.call(w, i, w));
      return this;
    },
    filter(fn) {
      return collection(wrappers.filter((w, i) => fn.call(w, i, w)).map((w) => w.__field));
    },
    focus() {
      wrappers.forEach((w) => w.focus());
      return this;
    },
  };
}

// --- Harness -------------------------------------------------------------------------------------------------------

const RID = "#1:0";
let ctx;
let fields;
let statements;
let notifications;
let promptCallback;

function newContext() {
  fields = [];
  statements = [];
  notifications = [];
  promptCallback = null;

  const noop = new Proxy(function () {}, { get: (t, p) => (p === "then" ? undefined : noop), apply: () => noop });
  const $ = (sel) => {
    if (typeof sel === "object") return sel;
    const fieldSel = /\.record-editor-field(?:\[data-prop='(.*)'\])?$/.exec(sel);
    if (fieldSel) {
      // Sizzle refuses an unescaped quote inside a quoted attribute value
      if (fieldSel[1] !== undefined && fieldSel[1].includes("'")) throw new Error("Syntax error, unrecognized expression: " + sel);
      const all = fields.filter((f) => fieldSel[1] === undefined || unescapeHtml(f.attrs["data-prop"]) === fieldSel[1]);
      return collection(all);
    }
    if (sel === "#tableRecordEditorContent" || sel === "#graphRecordEditorContent")
      return {
        html(h) {
          fields = parseFields(h);
          return this;
        },
      };
    return noop;
  };
  $.ajax = (opts) => {
    statements.push(JSON.parse(opts.data).command);
    const request = {
      done(fn) {
        fn({});
        return request;
      },
      fail: () => request,
    };
    return request;
  };

  ctx = vm.createContext({
    $,
    globalNotify: (title, message, kind) => notifications.push({ title, message, kind }),
    globalPrompt: (title, html, label, callback) => (promptCallback = callback),
    studioAction: (name) => " data-on-click='" + name + "'",
    getCurrentDatabase: () => "db",
    encodeDatabaseName: (d) => d,
    globalCredentials: "Basic x",
    globalCy: null,
    globalGraphPropertiesPerType: {},
    JSON,
  });
  vm.runInContext(extractFn(utilsSrc, "escapeHtml"), ctx);
  vm.runInContext(extractFn(utilsSrc, "quoteSqlName"), ctx);
  vm.runInContext(editorSrc, ctx);
}

function open(properties) {
  ctx.openRecordEditor(RID, "Beer", properties, "table");
}

// the user types into the field of a property
function type(prop, text) {
  const f = fields.find((x) => unescapeHtml(x.attrs["data-prop"]) === prop);
  assert.ok(f, `the form has a field for ${prop}`);
  f.value = text;
}

function valueOf(prop) {
  return fields.find((x) => unescapeHtml(x.attrs["data-prop"]) === prop).value;
}

function addProperty(name) {
  ctx.addPropertyToRecord();
  promptCallback({ newPropertyName: name });
}

function save() {
  ctx.saveRecordEditor();
  return statements.length === 0 ? null : statements[statements.length - 1];
}

beforeEach(newContext);

// --- The simple edit keeps working ---------------------------------------------------------------------------------

test("a single edited text property is written as a quoted string", () => {
  open({ name: "Lager", style: "Pale" });
  type("name", "Stout");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'Stout'");
});

test("quotes and backslashes in an edited text are escaped", () => {
  open({ name: "Lager" });
  type("name", "O'Brien \\ Co");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'O\\'Brien \\\\ Co'");
});

// --- #10082: a Save that follows no edit produces no statement -------------------------------------------------------

test("Save without any edit writes nothing, whatever the stored strings look like", () => {
  open({ name: "Lager", zip: "12345", active: "true", off: "false", nothing: "null", map: "{}", list: "[1,2]", pi: "3.14", lead: "01234" });
  assert.equal(save(), null);
  assert.equal(statements.length, 0);
  assert.deepEqual(notifications.map((n) => n.message), ["No changes to save"]);
});

test("Save without any edit writes nothing for typed values of every shape", () => {
  open({
    count: 5,
    ratio: 0.5,
    active: true,
    off: false,
    gone: null,
    nested: { a: 1, b: [1, 2, 3] },
    tags: ["x", "y"],
    empty: {},
    longText: "x".repeat(200),
    multiline: "line one\nline two",
  });
  assert.equal(save(), null);
  assert.equal(statements.length, 0);
});

test("Save without any edit writes nothing for values the browser rewrites on the way into a field", () => {
  open({
    crlf: "line one\r\nline two",
    lone: "line one\rline two",
    leading: "\nstarts with a line break",
    leadingCrlf: "\r\nstarts with a line break",
    trailing: "ends with a line break\n",
    blank: "\n\n",
  });
  assert.equal(save(), null);
  assert.equal(statements.length, 0);
});

test("editing one property of a multi-line record does not drag its line breaks into the statement", () => {
  open({ name: "Lager", notes: "one\r\ntwo\r\nthree", leading: "\nfirst" });
  type("name", "Stout");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'Stout'");
});

test("editing a multi-line value writes it with the line breaks the field holds", () => {
  open({ notes: "one\r\ntwo" });
  type("notes", "one\ntwo\nthree");
  assert.equal(save(), "UPDATE #1:0 SET `notes` = 'one\ntwo\nthree'");
});

test("editing one property does not drag the untouched ones into the statement", () => {
  open({ name: "Lager", zip: "12345", active: "true", count: 5, nested: { a: 1 } });
  type("name", "Stout");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'Stout'");
});

test("an edited text property stays text: its digits are not retyped, and a leading zero survives", () => {
  open({ zip: "12345", code: "01234", flag: "true" });
  type("zip", "12346");
  type("code", "01235");
  type("flag", "false");
  assert.equal(save(), "UPDATE #1:0 SET `zip` = '12346', `code` = '01235', `flag` = 'false'");
});

test("an edited number, boolean or object keeps being written as such", () => {
  open({ count: 5, active: true, nested: { a: 1 } });
  type("count", "6");
  type("active", "false");
  type("nested", '{"a":2}');
  assert.equal(save(), "UPDATE #1:0 SET `count` = 6, `active` = false, `nested` = {\"a\":2}");
});

test("only a decimal number is written bare: Infinity, NaN and hex are text, not SQL literals", () => {
  open({ a: 1, b: 1, c: 1, d: 1, e: 1, f: 1, g: 1, h: 1, i: 1 });
  type("a", "Infinity");
  type("b", "NaN");
  type("c", "0x10");
  type("d", "1e3");
  type("e", "-2.5");
  type("f", ".5");
  type("g", " 7 ");
  type("h", "+5");
  type("i", "12abc");
  assert.equal(
    save(),
    "UPDATE #1:0 SET `a` = 'Infinity', `b` = 'NaN', `c` = '0x10', `d` = 1e3, `e` = -2.5, `f` = .5, `g` = 7, `h` = '+5', `i` = '12abc'"
  );
});

test("clearing a property writes null, and so does typing null", () => {
  open({ name: "Lager", style: "Pale", count: 5 });
  type("name", "");
  type("style", "null");
  assert.equal(save(), "UPDATE #1:0 SET `name` = null, `style` = null");
});

test("a null property is an empty field that stays unchanged, and takes a value like a new one", () => {
  open({ name: "Lager", abv: null });
  assert.equal(valueOf("abv"), "");
  assert.equal(save(), null);
  type("abv", "5.5");
  assert.equal(save(), "UPDATE #1:0 SET `abv` = 5.5");
});

test("a property named like a number or a boolean is addressed by its name", () => {
  open({ 123: "a", true: "b" });
  type("123", "c");
  type("true", "d");
  assert.equal(save(), "UPDATE #1:0 SET `123` = 'c', `true` = 'd'");
});

// --- #10083: one baseline, whatever was re-rendered in between -------------------------------------------------------

test("removing a property keeps the edit typed before it", () => {
  open({ name: "Lager", style: "Pale" });
  type("name", "Stout");
  ctx.deletePropertyFromRecord("style");
  assert.equal(valueOf("name"), "Stout");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'Stout' REMOVE `style`");
});

test("adding a property keeps the edit typed before it", () => {
  open({ name: "Lager", style: "Pale" });
  type("name", "Stout");
  addProperty("abv");
  assert.equal(valueOf("name"), "Stout", "the form must not fall back to the stored value");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'Stout'");
});

test("a property added and then filled in is written", () => {
  open({ name: "Lager" });
  type("name", "Stout");
  addProperty("abv");
  type("abv", "5.5");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'Stout', `abv` = 5.5");
});

test("a property added and left empty is not written", () => {
  open({ name: "Lager" });
  addProperty("abv");
  assert.equal(save(), null);
});

test("the statement does not depend on how many times the form was re-rendered", () => {
  const expected = "UPDATE #1:0 SET `name` = 'Stout', `ibu` = 40 REMOVE `style`";
  open({ name: "Lager", style: "Pale", country: "IT" });
  type("name", "Stout");
  ctx.deletePropertyFromRecord("style");
  addProperty("ibu");
  type("ibu", "40");
  addProperty("tmp");
  ctx.deletePropertyFromRecord("tmp");
  ctx.deletePropertyFromRecord("country");
  addProperty("country");
  type("country", "IT");
  assert.equal(save(), expected);
});

test("re-rendering never turns a stored object into a statement", () => {
  open({ name: "Lager", style: "Pale", meta: { a: 1 }, tags: ["x"] });
  ctx.deletePropertyFromRecord("style");
  addProperty("abv");
  assert.equal(save(), "UPDATE #1:0 REMOVE `style`");
});

test("a rendered object stays a multi-line field after a re-render", () => {
  open({ name: "Lager", meta: { a: 1 } });
  addProperty("abv");
  assert.equal(valueOf("meta"), JSON.stringify({ a: 1 }, null, 2));
});

test("adding a property whose name holds a quote focuses its field without throwing", () => {
  open({ name: "Lager" });
  addProperty("o'neil");
  assert.equal(fields.find((f) => unescapeHtml(f.attrs["data-prop"]) === "o'neil").focused, true);
});

// --- Metadata and binary properties are never written ------------------------------------------------------------------

test("metadata and binary properties are never part of the statement", () => {
  open({ "@in": "x", name: "Lager", blob: "[B@1a2b3c" });
  type("name", "Stout");
  assert.equal(save(), "UPDATE #1:0 SET `name` = 'Stout'");
});
