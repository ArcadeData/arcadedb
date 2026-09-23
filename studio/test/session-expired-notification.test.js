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

// Regression test for issue #8231: the "Session Expired" toast is a Bootstrap "warning" toast, which
// index.html deliberately never auto-hides (so the user has time to read it). Nothing ever dismissed it
// again, so once shown it stayed on screen even after the user logged back in and it was no longer true.
//
// The fix tags the toast ("session-expired") and adds dismissNotification(tag), which login() now calls
// on a successful re-login. Run with:
//
//     node --test studio/test/session-expired-notification.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const STATIC_DIR = path.join(__dirname, "..", "src", "main", "resources", "static");
const htmlSrc = fs.readFileSync(path.join(STATIC_DIR, "index.html"), "utf8");
const dbSrc = fs.readFileSync(path.join(STATIC_DIR, "js", "studio-database.js"), "utf8");

// Pulls one top-level `function name(...) {...}` out of a Studio source file. See
// create-index-command.test.js for why the extracted text is required to parse on its own.
function extractFn(src, name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  let i = src.indexOf("{", start);
  let depth = 1;
  i++;
  while (i < src.length && depth > 0) {
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") depth--;
    i++;
  }
  if (depth !== 0) throw new Error("unbalanced braces while extracting " + name + ": reached end of file");

  const source = src.substring(start, i);
  try {
    new Function("return (" + source + ")");
  } catch (e) {
    throw new Error(
      "the extracted source of " + name + " does not parse, which happens when that function contains a brace " +
        "inside a string literal, a regex or a comment: " + e.message
    );
  }
  return source;
}

// --- minimal DOM/Bootstrap stand-ins for the toast machinery in index.html -------------------------

function makeToastEl() {
  const attrs = {};
  const listeners = {};
  let headerNode = null;
  let bodyNode = null;
  let parent = null;

  const el = {
    className: "",
    setAttribute: (k, v) => { attrs[k] = v; },
    getAttribute: (k) => attrs[k],
    querySelector: (sel) => {
      if (sel === ".toast-header strong") return headerNode || (headerNode = { textContent: "" });
      if (sel === ".toast-body") return bodyNode || (bodyNode = { textContent: "" });
      return null;
    },
    addEventListener: (ev, fn) => { (listeners[ev] = listeners[ev] || []).push(fn); },
    remove: () => {
      if (!parent) return;
      const idx = parent.children.indexOf(el);
      if (idx >= 0) parent.children.splice(idx, 1);
    },
    _setParent: (p) => { parent = p; },
    _fire: (ev) => (listeners[ev] || []).forEach((fn) => fn()),
    _header: () => headerNode,
    _body: () => bodyNode,
  };
  return el;
}

function makeContainer() {
  return {
    children: [],
    appendChild(el) {
      this.children.push(el);
      el._setParent(this);
    },
    querySelectorAll(sel) {
      // Production code only ever queries the fixed class selector '.studio-toast'; mirror that,
      // rather than resurrecting the string-interpolated-selector approach the fix removed.
      if (sel !== ".studio-toast") throw new Error("unexpected selector in test double: " + sel);
      return this.children.filter((el) => (el.className || "").indexOf("studio-toast") !== -1);
    },
  };
}

function setupToastEnv() {
  const container = makeContainer();
  const document = { getElementById: (id) => (id === "toastContainer" ? container : null), createElement: () => makeToastEl() };
  const bootstrap = {
    Toast: function (el, options) {
      this.el = el;
      this.options = options;
      el._toastInstance = this;
    },
  };
  bootstrap.Toast.prototype.show = function () { this.shown = true; };
  bootstrap.Toast.prototype.hide = function () { this.el._fire("hidden.bs.toast"); };
  bootstrap.Toast.getInstance = (el) => el._toastInstance;

  var isToastReady = true;
  var notificationQueue = [];

  eval(extractFn(htmlSrc, "processNotificationQueue"));
  eval(extractFn(htmlSrc, "showNotification"));
  eval(extractFn(htmlSrc, "globalNotify"));
  eval(extractFn(htmlSrc, "dismissNotification"));

  return { container, globalNotify, dismissNotification, get queue() { return notificationQueue; } };
}

test("the Session Expired toast is tagged and does not auto-hide", () => {
  const env = setupToastEnv();
  env.globalNotify("Session Expired", "Your session has expired. Please log in again.", "warning", null, "session-expired");

  assert.equal(env.container.children.length, 1, "one toast should be showing");
  const toastEl = env.container.children[0];
  assert.equal(toastEl.getAttribute("data-notify-tag"), "session-expired");
  assert.equal(toastEl._toastInstance.options.autohide, false, "a warning toast must stay until dismissed");
});

test("dismissNotification removes a visible tagged toast", () => {
  const env = setupToastEnv();
  env.globalNotify("Session Expired", "Your session has expired. Please log in again.", "warning", null, "session-expired");
  assert.equal(env.container.children.length, 1);

  env.dismissNotification("session-expired");

  assert.equal(env.container.children.length, 0, "the stale Session Expired toast must be gone after re-login");
});

test("dismissNotification does not touch toasts with a different or no tag", () => {
  const env = setupToastEnv();
  env.globalNotify("Session Expired", "...", "warning", null, "session-expired");
  env.globalNotify("Success", "Database created", "success");

  env.dismissNotification("session-expired");

  assert.equal(env.container.children.length, 1, "the unrelated toast must survive");
  assert.equal(env.container.children[0]._header().textContent, "Success");
});

test("dismissNotification matches a tag containing a double quote", () => {
  // Regression case for the CodeRabbit finding on PR #8263: the original implementation built a CSS
  // attribute selector by interpolating `tag` directly (`'[data-notify-tag="' + tag + '"]'`), which broke
  // for a tag containing a `"`. The fix compares the attribute value instead of building a selector.
  const env = setupToastEnv();
  const oddTag = 'job:"42"';
  env.globalNotify("Job Finished", "...", "success", null, oddTag);
  assert.equal(env.container.children.length, 1);

  env.dismissNotification(oddTag);

  assert.equal(env.container.children.length, 0, "a toast tagged with a quote-bearing tag must still be dismissed");
});

test("dismissNotification also drops a still-queued Session Expired toast", () => {
  const env = setupToastEnv();
  // Nothing has been shown yet: queue it manually the way globalNotify would before DOMContentLoaded.
  env.queue.push({ title: "Session Expired", message: "...", type: "warning", tag: "session-expired" });

  env.dismissNotification("session-expired");

  assert.equal(env.queue.length, 0, "the queued Session Expired notification must not fire later");
});

// --- login() must clear a leftover Session Expired toast on a successful re-login ------------------

test("a successful login dismisses a leftover Session Expired notification", () => {
  let dismissedTag = null;
  let storedToken = null;
  let updateDatabasesCalled = false;

  const inputs = { "#inputUserName": "root", "#inputUserPassword": "playwithdata" };
  const $ = (sel) => ({
    val: () => inputs[sel] || "",
    show: () => {},
    hide: () => {},
  });

  let doneCallback = null;
  const ajaxHandle = {
    done: (fn) => { doneCallback = fn; return ajaxHandle; },
    fail: () => ajaxHandle,
    always: () => ajaxHandle,
  };
  const jQuery = { ajax: (opts) => { if (opts.beforeSend) opts.beforeSend({ setRequestHeader: () => {} }); return ajaxHandle; } };

  const console = { log: () => {}, warn: () => {}, error: () => {} };
  const make_base_auth = () => "Basic dGVzdA==";
  const storeSession = (token) => { storedToken = token; };
  const updateDatabases = (cb) => { updateDatabasesCalled = true; cb(); };
  const initQuery = () => {};
  const dismissNotification = (tag) => { dismissedTag = tag; };
  let globalCredentials = null, globalBasicAuth = null, globalUsername = null;

  eval(extractFn(dbSrc, "login"));

  login();
  assert.notEqual(doneCallback, null, "login() must have called jQuery.ajax");
  doneCallback({ token: "tok-123", user: "root" });

  assert.equal(storedToken, "tok-123");
  assert.equal(updateDatabasesCalled, true);
  assert.equal(dismissedTag, "session-expired", "a successful login must dismiss the stale Session Expired toast");
});
