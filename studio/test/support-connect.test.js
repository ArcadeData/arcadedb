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

// "Connect to ArcadeDB Portal" in the Support tab (studio-support-connect.js): that the portal address is only ever a plain http(s)
// link, that everything the portal says is escaped, what is shown for each way a connection ends, that the new tab is opened
// inside the click and without an opener, and that the daily re-registration is due at most once a day. Run with:
//
//     node --test studio/test/support-connect.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-support-connect.js");

function load(opts = {}) {
  const calls = [];
  const rendered = {};
  const notes = [];
  const storage = opts.storage || new Map();
  const tab = { opener: "x", location: { href: "" }, closed: false, close() { this.closed = true; } };
  const answers = opts.answers || {};
  const supportApi = (method, p, body) => {
    calls.push({ method, path: p, body });
    const answer = answers[method + " " + p];
    const chain = {
      done(fn) {
        if (answer && !answer.fail) fn(answer.text === undefined ? "" : answer.text);
        return chain;
      },
      fail(fn) {
        if (answer && answer.fail) fn(answer.fail);
        return chain;
      },
      always(fn) {
        fn();
        return chain;
      },
    };
    return chain;
  };
  const context = {
    $: (selector) => ({
      html(v) {
        if (v !== undefined) rendered[selector] = v;
        return this;
      },
      on() {},
    }),
    document: {},
    window: {
      open: () => (opts.blocked ? null : tab),
      localStorage: { getItem: (k) => (storage.has(k) ? storage.get(k) : null), setItem: (k, v) => storage.set(k, v) },
    },
    setTimeout: () => 1,
    clearTimeout() {},
    Date, Math, parseInt, String, Array, Object, JSON, Number,
    globalNotify: (t, m) => notes.push(m),
    loadSupport: () => calls.push({ method: "loadSupport" }),
    supportRegisterInstallation: (auto) => calls.push({ method: "sync", auto }),
    supportApi,
    supportParse: (t) => (t ? JSON.parse(t) : null),
    supportError: (x) => ({ message: x.message || "failed", code: x.code || "" }),
    supportAlertHtml: (e) => "<alert>" + e.message + "</alert>",
    supportSpinner: (t) => "<spin>" + t + "</spin>",
    supportEsc: (v) => String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"),
    supportStatus: { instanceId: "adb-1" },
    supportInstallation: null,
    supportInstallationError: null,
    supportIssues: [],
    console,
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(JS, "utf8"), context, { filename: "studio-support-connect.js" });
  return { run: (code) => vm.runInContext(code, context), context, calls, rendered, notes, storage, tab };
}

test("the portal address is followed only when it is a plain http(s) link", () => {
  const { run } = load();
  assert.equal(run('supportConnectSafeUrl("https://portal.arcadedb.com/#/connect?code=ABCD-EFGH")'), "https://portal.arcadedb.com/#/connect?code=ABCD-EFGH");
  assert.equal(run('supportConnectSafeUrl("http://localhost:3000/#/connect")'), "http://localhost:3000/#/connect");
  for (const bad of ["javascript:alert(1)", "data:text/html,x", "//evil.example", "https://a b", "", null, 5])
    assert.equal(run("supportConnectSafeUrl(" + JSON.stringify(bad) + ")"), null, String(bad));
});

test("the code is shown escaped, with the link only when it is safe, and a Cancel", () => {
  const { run, context } = load();
  context.supportConnect = { status: "pending", userCode: "<b>WDJB</b>", verifyUrl: 'https://portal.arcadedb.com/x"onmouseover="1' };
  let html = run("supportConnectHtml()");
  assert.ok(html.includes("&lt;b&gt;WDJB&lt;/b&gt;") && !html.includes("<b>WDJB"));
  assert.ok(html.includes("Waiting for approval") && html.includes("supportConnectCancelBtn"));
  assert.ok(!html.includes('"onmouseover="1'), "the attribute is escaped");
  context.supportConnect = { status: "pending", userCode: "WDJB-MJHT", verifyUrl: "javascript:alert(1)" };
  html = run("supportConnectHtml()");
  assert.ok(html.includes("WDJB-MJHT") && !html.includes("javascript:") && !html.includes("supportConnectLink"));
});

test("each way a connection ends is said, and the button comes back", () => {
  const { run, context } = load();
  for (const [status, text] of [["denied", "denied"], ["expired", "expired"]]) {
    context.supportConnect = { status };
    const html = run("supportConnectHtml()");
    assert.ok(html.includes(text) && html.includes("supportConnectBtn"), status);
  }
  context.supportConnect = { status: "error", error: { error: "key_limit", message: "25 active keys <x>" } };
  const html = run("supportConnectHtml()");
  assert.ok(html.includes("25 active keys &lt;x&gt;") && html.includes("supportConnectBtn"));
});

test("what was registered is told in words, and the fields that differ are counted", () => {
  const { run } = load();
  assert.equal(run('supportConnectRegistrationText({status:"created",name:"arcadedb_0",filled:[],differs:[]})'), "Registered as installation arcadedb_0.");
  assert.equal(run('supportConnectRegistrationText({status:"unchanged",name:"prod",differs:["version","os"]})'),
    "Already registered as installation prod. 2 fields differ from the portal and were left as they are: version, os.");
  assert.equal(run('supportConnectRegistrationText({status:"updated",name:"p",differs:["os"]})'),
    "Your installation was completed p. 1 field differs from the portal and was left as it is: os.");
  assert.ok(run('supportConnectRegistrationText({error:"instance_id.taken",message:"held elsewhere."})').includes("held elsewhere."));
  assert.equal(run("supportConnectRegistrationText(null)"), "");
});

test("starting opens the tab inside the click, cuts its opener and points it at the portal; the key is never asked for", () => {
  const { run, context, calls, tab } = load({
    answers: { "POST /connect": { text: JSON.stringify({ userCode: "WDJB-MJHT", verifyUrl: "https://portal.arcadedb.com/#/connect?code=WDJB-MJHT", expiresIn: 600 }) } },
  });
  run("supportConnectStart()");
  assert.equal(tab.opener, null);
  assert.equal(tab.location.href, "https://portal.arcadedb.com/#/connect?code=WDJB-MJHT");
  assert.equal(context.supportConnect.status, "pending");
  assert.equal(context.supportConnect.userCode, "WDJB-MJHT");
  assert.deepEqual(calls.map((c) => c.method + " " + c.path), ["POST /connect"]);
});

test("a blocked tab shows the link instead, and an unsafe address is never opened", () => {
  let r = load({
    blocked: true,
    answers: { "POST /connect": { text: JSON.stringify({ userCode: "A", verifyUrl: "https://portal.arcadedb.com/x", expiresIn: 600 }) } },
  });
  r.run("supportConnectStart()");
  assert.ok(r.run("supportConnectHtml()").includes("The new tab was blocked") && r.run("supportConnectHtml()").includes("supportConnectLink"));

  r = load({ answers: { "POST /connect": { text: JSON.stringify({ userCode: "A", verifyUrl: "javascript:alert(1)", expiresIn: 600 }) } } });
  r.run("supportConnectStart()");
  assert.equal(r.tab.location.href, "");
  assert.equal(r.tab.closed, true);
});

test("a refusal to start closes the empty tab and shows the server's message", () => {
  const { run, context, tab } = load({ answers: { "POST /connect": { fail: { message: "A connection is already waiting", code: "connect_in_progress" } } } });
  run("supportConnectStart()");
  assert.equal(tab.closed, true);
  assert.equal(context.supportConnectError.code, "connect_in_progress");
  assert.equal(context.supportConnect, null);
});

test("when the server says connected, the outcome is kept, the registration is reloaded and the daily call is not made again", () => {
  const { run, context, calls, notes, storage } = load();
  context.supportConnect = { status: "pending", userCode: "A" };
  run('supportConnectApply({status:"connected",workspaceName:"Acme",registration:{status:"created",name:"arcadedb_0",filled:[],differs:[]}})');
  assert.equal(context.supportInstallation.name, "arcadedb_0");
  assert.equal(context.supportInstallationError, null);
  assert.ok(calls.some((c) => c.method === "loadSupport"));
  assert.equal(notes.length, 1);
  assert.ok(storage.has("arcadedb.support.installationSyncedAt.adb-1"));

  // a refused registration of the installation is kept as an error and does not undo the connection
  const second = load();
  second.context.supportConnect = { status: "pending", userCode: "A" };
  second.run('supportConnectApply({status:"connected",registration:{error:"instance_id.taken",message:"held"}})');
  assert.equal(second.context.supportInstallationError.code, "instance_id.taken");
  assert.equal(second.context.supportInstallation, null);
});

test("a state that is not pending stops the polling; 'none' clears it", () => {
  const { run, context } = load();
  context.supportConnect = { status: "pending", userCode: "A" };
  run('supportConnectApply({status:"denied"})');
  assert.equal(context.supportConnect.status, "denied");
  run('supportConnectApply({status:"none"})');
  assert.equal(context.supportConnect, null);
});

test("the installation is completed automatically at most once a day, and never without storage", () => {
  const day = 24 * 3600 * 1000;
  const { run, calls, storage } = load();
  assert.equal(run("supportAutoSyncDue(null, 1000)"), true);
  assert.equal(run("supportAutoSyncDue(1000, 1000 + 1000)"), false);
  assert.equal(run(`supportAutoSyncDue(1000, 1000 + ${day})`), true);
  assert.equal(run("supportAutoSyncDue(5000, 1000)"), true, "a clock that went back");

  assert.equal(run('supportAutoSync("adb-9")'), true);
  assert.deepEqual(calls.filter((c) => c.method === "sync"), [{ method: "sync", auto: true }]);
  assert.ok(storage.has("arcadedb.support.installationSyncedAt.adb-9"));
  assert.equal(run('supportAutoSync("adb-9")'), false, "the second time within the day does nothing");
  assert.equal(calls.filter((c) => c.method === "sync").length, 1);

  const broken = load({ storage: { has: () => false, get: () => { throw new Error("denied"); }, set: () => { throw new Error("denied"); } } });
  assert.equal(broken.run('supportAutoSync("adb-9")'), false);
});
