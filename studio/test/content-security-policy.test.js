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

// Regression test for issue #9681: the server now sends Studio with a Content-Security-Policy whose script-src has no
// 'unsafe-inline', so the browser refuses inline on*="..." attributes, javascript: URLs and inline <script> blocks.
// Any of those left in Studio is a control that silently stops working. Handlers are data-on-* attributes dispatched by
// js/studio-actions.js, and only to the functions its STUDIO_ACTIONS allow-list names.
//
// Run with:
//
//     node --test studio/test/content-security-policy.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const JS_DIR = path.join(STATIC_DIR, "js");

const htmlFiles = fs.readdirSync(STATIC_DIR).filter((f) => f.endsWith(".html"));
const jsFiles = fs.readdirSync(JS_DIR).filter((f) => f.endsWith(".js"));
const read = (...p) => fs.readFileSync(path.join(...p), "utf8");
const html = Object.fromEntries(htmlFiles.map((f) => [f, read(STATIC_DIR, f)]));
const js = Object.fromEntries(jsFiles.map((f) => [f, read(JS_DIR, f)]));

const INLINE_EVENTS =
  "click|dblclick|change|input|submit|reset|keydown|keyup|keypress|mouse[a-z]+|pointer[a-z]+|focus|focusin|focusout|blur|" +
  "load|error|scroll|wheel|contextmenu|drag[a-z]*|drop|toggle|select";

// Repeats until nothing changes, so a comment that removing another one brings together is removed too
function stripHtmlComments(text) {
  let previous;
  do {
    previous = text;
    text = text.replace(/<!--[\s\S]*?-->/g, "");
  } while (text !== previous);
  return text;
}

// Removes block comments and line comments (a "//" that starts a line or follows whitespace, so "https://" survives)
function stripJsComments(text) {
  return text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|\s)\/\/.*$/gm, "$1");
}

function linesMatching(text, regex) {
  const out = [];
  text.split("\n").forEach((line, i) => {
    if (regex.test(line)) out.push(i + 1 + ": " + line.trim().substring(0, 160));
  });
  return out;
}

// ---- the source carries nothing the policy refuses ----

test("no Studio page or script carries an inline on*= event handler", () => {
  // HTML attribute names ignore case. In a script, markup is spelled lowercase, and onError is a variable
  const htmlAttr = new RegExp("\\son(?:" + INLINE_EVENTS + ")\\s*=", "i");
  const jsAttr = new RegExp("\\son(?:" + INLINE_EVENTS + ")\\s*=");
  const offenders = [];
  for (const [f, text] of Object.entries(html)) for (const l of linesMatching(stripHtmlComments(text), htmlAttr)) offenders.push(f + ":" + l);
  for (const [f, text] of Object.entries(js)) for (const l of linesMatching(stripJsComments(text), jsAttr)) offenders.push("js/" + f + ":" + l);
  assert.deepEqual(offenders, [], "inline handlers are refused by script-src 'self':\n" + offenders.join("\n"));
});

test("no Studio script sets or looks up an on* attribute at runtime", () => {
  const dynamic = new RegExp("(?:attr|setAttribute|getAttribute|removeAttr)\\(\\s*[\"']on(?:" + INLINE_EVENTS + ")[\"']|\\[on(?:" + INLINE_EVENTS + ")[*^$]?=", "i");
  const offenders = [];
  for (const [f, text] of Object.entries(js)) for (const l of linesMatching(stripJsComments(text), dynamic)) offenders.push("js/" + f + ":" + l);
  assert.deepEqual(offenders, [], offenders.join("\n"));
});

test("no Studio link navigates to a javascript: URL", () => {
  const url = /href\s*=\s*\\?["']\s*javascript:/i;
  const offenders = [];
  for (const [f, text] of Object.entries(html)) for (const l of linesMatching(stripHtmlComments(text), url)) offenders.push(f + ":" + l);
  for (const [f, text] of Object.entries(js)) for (const l of linesMatching(stripJsComments(text), url)) offenders.push("js/" + f + ":" + l);
  assert.deepEqual(offenders, [], offenders.join("\n"));
});

test("no Studio page carries an inline <script> block", () => {
  const offenders = [];
  for (const [f, text] of Object.entries(html)) {
    const re = /<script\b([^>]*)>/gi;
    let m;
    while ((m = re.exec(stripHtmlComments(text))) !== null) if (!/\bsrc\s*=/.test(m[1])) offenders.push(f + ": " + m[0]);
  }
  assert.deepEqual(offenders, [], offenders.join("\n"));
});

test("every script a page loads exists, and the dispatcher loads before Studio's own scripts", () => {
  const missing = [];
  for (const [f, text] of Object.entries(html)) {
    const re = /<script\s+src="(js\/[^"]+)"/g;
    let m;
    while ((m = re.exec(text)) !== null) if (!fs.existsSync(path.join(STATIC_DIR, m[1]))) missing.push(f + " -> " + m[1]);
  }
  assert.deepEqual(missing, []);

  const index = html["index.html"];
  const actions = index.indexOf('src="js/studio-actions.js"');
  assert.ok(actions > 0, "index.html loads js/studio-actions.js");
  const firstStudioScript = index.search(/src="js\/studio-(?!actions\.js|theme-init\.js)/);
  assert.ok(actions < firstStudioScript, "studio-actions.js is loaded before the other Studio scripts");
});

// ---- the allow-list ----

function loadActions(document) {
  const sandbox = { document, console: { error: () => {} }, JSON, Array, Object, Set, String, Error };
  sandbox.window = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(js["studio-actions.js"], sandbox);
  return sandbox;
}

function referencedActions() {
  const names = new Map();
  const add = (name, where) => {
    if (!names.has(name)) names.set(name, where);
  };
  for (const [f, text] of Object.entries(html)) {
    const re = /data-on-(?:click|change|input|keydown|enter)="([^"]*)"/g;
    let m;
    while ((m = re.exec(text)) !== null) add(m[1], f);
  }
  for (const [f, text] of Object.entries(js)) {
    if (f === "studio-actions.js") continue;
    const code = stripJsComments(text);
    let re = /studioAction\(\s*"([^"]*)"/g;
    let m;
    while ((m = re.exec(code)) !== null) add(m[1], "js/" + f);
    re = /attr\(\s*"data-on-(?:click|change|input|keydown|enter)"\s*,\s*"([^"]*)"/g;
    while ((m = re.exec(code)) !== null) add(m[1], "js/" + f);
    re = /data-on-(?:click|change|input|keydown|enter)=(?:\\?["'])(\w+)/g;
    while ((m = re.exec(code)) !== null) add(m[1], "js/" + f);
  }
  return names;
}

function topLevelFunctions() {
  const defined = new Set();
  for (const text of Object.values(js)) {
    const re = /^(?:async\s+)?function\s+([A-Za-z_$][\w$]*)\s*\(/gm;
    let m;
    while ((m = re.exec(text)) !== null) defined.add(m[1]);
  }
  return defined;
}

test("every action the markup names is allow-listed and is a global function", () => {
  const allowed = loadActions({ addEventListener: () => {} }).STUDIO_ACTIONS;
  const defined = topLevelFunctions();
  const referenced = referencedActions();
  assert.ok(referenced.size > 100, "the scan found the actions (" + referenced.size + ")");

  const problems = [];
  for (const [name, where] of referenced) {
    if (!allowed.has(name)) problems.push(where + ": '" + name + "' is not in STUDIO_ACTIONS");
    if (!defined.has(name)) problems.push(where + ": '" + name + "' is not a top-level function of a Studio script");
  }
  for (const name of allowed) if (!defined.has(name)) problems.push("STUDIO_ACTIONS names '" + name + "', which no Studio script defines");
  assert.deepEqual(problems, [], problems.join("\n"));
});

// ---- the dispatcher ----

function fakeDom() {
  const listeners = {};
  const capture = {};
  const byId = {};
  const document = {
    addEventListener: (type, fn, useCapture) => {
      (listeners[type] = listeners[type] || []).push(fn);
      capture[type] = useCapture;
    },
    getElementById: (id) => byId[id] || null,
  };
  function element(attrs, parent, extra) {
    const el = Object.assign(
      {
        nodeType: 1,
        parentElement: parent || null,
        attributes: Object.assign({}, attrs),
        getAttribute(n) {
          return Object.prototype.hasOwnProperty.call(this.attributes, n) ? this.attributes[n] : null;
        },
        hasAttribute(n) {
          return Object.prototype.hasOwnProperty.call(this.attributes, n);
        },
      },
      extra || {}
    );
    if (attrs && attrs.id) byId[attrs.id] = el;
    return el;
  }
  function fire(type, target, extra) {
    const event = Object.assign(
      {
        type,
        target,
        defaultPrevented: false,
        propagationStopped: false,
        preventDefault() {
          this.defaultPrevented = true;
        },
        stopPropagation() {
          this.propagationStopped = true;
        },
      },
      extra || {}
    );
    for (const fn of listeners[type] || []) fn(event);
    return event;
  }
  return { document, element, fire, listeners, capture };
}

function setup() {
  const dom = fakeDom();
  const ctx = loadActions(dom.document);
  const calls = [];
  for (const name of ctx.STUDIO_ACTIONS) ctx[name] = (...args) => calls.push([name, args]);
  return { dom, ctx, calls };
}

test("the dispatcher listens in the capture phase, so a handler that stops an event cannot hide it", () => {
  // .dropdown-menu stops every click inside a menu (studio-query.js): a bubbling listener on the document never sees a
  // click on a menu item, while the inline handler it replaces ran at the item itself
  const { dom } = setup();
  for (const type of ["click", "change", "input", "keydown"]) {
    assert.equal(dom.listeners[type].length, 1, type);
    assert.equal(dom.capture[type], true, type + " is dispatched in the capture phase");
  }
});

test("a click calls the named action with the JSON arguments, the element, the event, value and checked", () => {
  const { dom, ctx, calls } = setup();
  const el = dom.element({ "data-on-click": "aiCopyCode", "data-args": '[{"$":"this"},"blk_1",{"$":"event"},{"$":"value"},{"$":"checked"},7,null,true]' }, null, { value: "v", checked: true });
  const event = dom.fire("click", el);
  assert.equal(calls.length, 1);
  const [name, args] = calls[0];
  assert.equal(name, "aiCopyCode");
  assert.equal(args[0], el);
  assert.equal(args[1], "blk_1");
  assert.equal(args[2], event);
  assert.deepEqual(args.slice(3), ["v", true, 7, null, true]);
  assert.ok(ctx.STUDIO_ACTION_EVENTS.includes("click"));
});

test("a click on a child reaches the handlers of the element and of its ancestors, innermost first", () => {
  const { dom, calls } = setup();
  const row = dom.element({ "data-on-click": "loadSavedQuery", "data-args": "[3]" });
  const label = dom.element({}, row);
  const icon = dom.element({}, label);
  dom.fire("click", icon);
  assert.deepEqual(calls.map((c) => c[0] + ":" + JSON.stringify(c[1])), ["loadSavedQuery:[3]"]);

  const run = dom.element({ "data-on-click": "executeSavedQuery", "data-args": "[3]" }, row);
  calls.length = 0;
  dom.fire("click", run);
  assert.deepEqual(calls.map((c) => c[0]), ["executeSavedQuery", "loadSavedQuery"]);
});

test("data-stop-propagation keeps a click from reaching the ancestors' handlers", () => {
  const { dom, calls } = setup();
  const row = dom.element({ "data-on-click": "loadSavedQuery", "data-args": "[3]" });
  const run = dom.element({ "data-on-click": "executeSavedQuery", "data-args": "[3]", "data-stop-propagation": "" }, row);
  const event = dom.fire("click", run);
  assert.deepEqual(calls.map((c) => c[0]), ["executeSavedQuery"]);
  // the dispatcher listens in the capture phase: stopping the event there would keep it from the clicked element itself
  assert.equal(event.propagationStopped, false);

  // an element that only stops the click, with no action of its own
  const check = dom.element({ "data-stop-propagation": "" }, row);
  calls.length = 0;
  dom.fire("click", check);
  assert.deepEqual(calls, []);
});

test("data-prevent-default cancels the click's default action", () => {
  const { dom } = setup();
  const link = dom.element({ "data-on-click": "logout", "data-prevent-default": "" });
  assert.equal(dom.fire("click", link).defaultPrevented, true);
  const plain = dom.element({ "data-on-click": "logout" });
  assert.equal(dom.fire("click", plain).defaultPrevented, false);
});

test("an action outside the allow-list is never called, even when a global function has that name", () => {
  const { dom, ctx, calls } = setup();
  let ran = false;
  ctx.eval = () => (ran = true);
  ctx.notAnAction = () => (ran = true);
  dom.fire("click", dom.element({ "data-on-click": "eval", "data-args": '["alert(1)"]' }));
  dom.fire("click", dom.element({ "data-on-click": "notAnAction" }));
  dom.fire("click", dom.element({ "data-on-click": "constructor" }));
  assert.equal(ran, false);
  assert.deepEqual(calls, []);
  assert.throws(() => ctx.studioAction("eval", []), /Not a Studio action/);
});

test("arguments that are not a JSON array call nothing", () => {
  const { dom, calls } = setup();
  dom.fire("click", dom.element({ "data-on-click": "logout", "data-args": "logout()" }));
  dom.fire("click", dom.element({ "data-on-click": "logout", "data-args": '{"a":1}' }));
  assert.deepEqual(calls, []);
});

test("an object argument is only a placeholder when it is exactly one of the four", () => {
  const { dom, calls } = setup();
  dom.fire("click", dom.element({ "data-on-click": "aiLoadChat", "data-args": '[{"$":"this","x":1},{"$":"window"},"$this"]' }));
  assert.deepEqual(JSON.parse(JSON.stringify(calls[0][1])), [{ $: "this", x: 1 }, { $: "window" }, "$this"]);
});

test("change, input and keydown dispatch to their own attribute only", () => {
  const { dom, calls } = setup();
  const select = dom.element({ "data-on-change": "applyDefaultLimit", "data-args": '[{"$":"value"}]' }, null, { value: "500" });
  dom.fire("click", select);
  dom.fire("input", select);
  assert.deepEqual(calls, []);
  dom.fire("change", select);
  assert.deepEqual(calls[0], ["applyDefaultLimit", ["500"]]);

  const filter = dom.element({ "data-on-input": "filterHistoryEntries" });
  dom.fire("input", filter);
  assert.equal(calls[1][0], "filterHistoryEntries");

  const box = dom.element({ "data-on-keydown": "tsQueryEditorKeydown", "data-args": '[{"$":"event"}]' });
  const event = dom.fire("keydown", box, { key: "a" });
  assert.equal(calls[2][1][0], event);
});

test("Enter clicks or focuses the element data-enter-click / data-enter-focus names, and runs data-on-enter", () => {
  const { dom, calls } = setup();
  let clicked = 0;
  let focused = 0;
  dom.element({ id: "globalModalConfirmBtn" }, null, { click: () => clicked++ });
  dom.element({ id: "addPeerAddress" }, null, { focus: () => focused++ });
  const name = dom.element({ "data-enter-click": "globalModalConfirmBtn" });
  const peer = dom.element({ "data-enter-focus": "addPeerAddress" });
  const key = dom.element({ "data-on-enter": "aiActivate" });

  dom.fire("keydown", name, { key: "a" });
  dom.fire("keydown", peer, { key: "Tab" });
  dom.fire("keydown", key, { key: "x" });
  assert.deepEqual([clicked, focused, calls.length], [0, 0, 0]);

  dom.fire("keydown", name, { key: "Enter" });
  dom.fire("keydown", peer, { key: "Enter" });
  dom.fire("keydown", key, { key: "Enter" });
  assert.deepEqual([clicked, focused, calls.map((c) => c[0])], [1, 1, ["aiActivate"]]);
});

test("an action runs with `this` bound as an inline handler's call ran it, not to the element", () => {
  const dom = fakeDom();
  const ctx = loadActions(dom.document);
  let self = "unset";
  ctx.logout = function () {
    self = this;
  };
  dom.fire("click", dom.element({ "data-on-click": "logout" }));
  assert.equal(self, vm.runInContext("window", ctx));
});

test("studioAction builds attributes that survive any quoting and any argument text", () => {
  const { ctx } = setup();
  const tricky = ["O'Brien \"x\" <b>&amp;</b>", "\\n", "#12:3", 42, true, null];
  const attrs = ctx.studioAction("pasteReferenceExample", tricky, "click");
  assert.ok(!/[<>"']/.test(attrs.replace(/="[^"]*"/g, "")), "only the attribute delimiters are quotes: " + attrs);

  // parse the attributes the way the browser does, whatever quote the surrounding tag uses
  const decode = (s) => s.replace(/&quot;/g, '"').replace(/&#039;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");
  const parsed = {};
  attrs.replace(/ ([\w-]+)="([^"]*)"/g, (all, n, v) => (parsed[n] = decode(v)));
  assert.equal(parsed["data-on-click"], "pasteReferenceExample");
  assert.deepEqual(JSON.parse(parsed["data-args"]), tricky);

  assert.equal(ctx.studioAction("logout"), ' data-on-click="logout"');
  assert.equal(ctx.studioAction("filterHistoryEntries", null, "input"), ' data-on-input="filterHistoryEntries"');
  assert.equal(
    ctx.studioAction("aiCopyCode", [ctx.STUDIO_ARG_THIS, "b"]),
    ' data-on-click="aiCopyCode" data-args="[{&quot;$&quot;:&quot;this&quot;},&quot;b&quot;]"'
  );
});
