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

// Screenshots in the Support page (studio-support-screenshots.js): what counts as a picture (by its bytes, never its name or
// declared type), what is taken from a paste or a drop, and that a picture is staged on the server, shown back, removable, and
// never sent by this code. Run with:
//
//     node --test studio/test/support-screenshots.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-support-screenshots.js");

const PNG = [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, 0x49, 0x48, 0x44, 0x52];
const bytes = (...b) => Uint8Array.from(b);
const text = (s) => new TextEncoder().encode(s.padEnd(16, " "));

function load() {
  const calls = [];
  const alerts = {};
  const lists = {};
  const revoked = [];
  let nextId = 1;
  let failWith = null;
  // a jQuery-like thenable: done/fail run synchronously, which is all the code needs
  const supportApi = (method, p, body) => {
    calls.push({ method, path: p, body });
    const chain = {
      done(fn) {
        if (!failWith && method === "POST") fn(JSON.stringify({ id: "shot_" + nextId++, type: "image/png", size: 1 }));
        else if (!failWith) fn("");
        return chain;
      },
      fail(fn) {
        if (failWith) fn(failWith);
        return chain;
      },
    };
    return chain;
  };
  const context = {
    $: (selector) => ({
      on() {},
      html(v) {
        if (v !== undefined) {
          if (/ShotsAlert$/.test(selector)) alerts[selector] = v;
          if (/ShotsList$/.test(selector)) lists[selector] = v;
        }
        return this;
      },
      val() {},
      attr() {},
    }),
    document: {},
    supportApi,
    supportParse: (t) => (t ? JSON.parse(t) : null),
    supportError: (x) => ({ message: x.message || "failed", code: x.code || "" }),
    supportEsc: (v) => String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"),
    URL: { createObjectURL: () => "blob:test/" + calls.length, revokeObjectURL: (u) => revoked.push(u) },
    Blob: class { constructor(parts, opts) { this.parts = parts; this.type = opts && opts.type; } },
    btoa: (s) => Buffer.from(s, "latin1").toString("base64"),
    Uint8Array, String, Array, Object, JSON, Promise, TextEncoder, Buffer, console,
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(JS, "utf8"), context, { filename: "studio-support-screenshots.js" });
  return { run: (code) => vm.runInContext(code, context), context, calls, alerts, lists, revoked, fail: (e) => (failWith = e) };
}

const blobOf = (content, size) => ({ size: size === undefined ? content.length : size, type: "image/png", arrayBuffer: () => Promise.resolve(content.buffer.slice(content.byteOffset, content.byteOffset + content.byteLength)) });
const settle = () => new Promise((r) => setImmediate(r));

test("a picture is recognised by its first bytes: PNG, JPEG, GIF, WebP; nothing else, whatever it is called", () => {
  const { run, context } = load();
  context.png = bytes(...PNG);
  assert.equal(run("supportShotType(png)"), "image/png");
  context.jpg = bytes(0xff, 0xd8, 0xff, 0xe0, 0, 16, 0x4a, 0x46, 0x49, 0x46, 0, 1);
  assert.equal(run("supportShotType(jpg)"), "image/jpeg");
  context.gif = text("GIF89a");
  assert.equal(run("supportShotType(gif)"), "image/gif");
  context.webp = bytes(0x52, 0x49, 0x46, 0x46, 1, 0, 0, 0, 0x57, 0x45, 0x42, 0x50);
  assert.equal(run("supportShotType(webp)"), "image/webp");
  for (const s of ['<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>', "<!doctype html><html>", "#!/bin/sh\nrm -rf /", "PK\u0003\u0004 zip"]) {
    context.bad = text(s);
    assert.equal(run("supportShotType(bad)"), null, s);
  }
  context.wav = bytes(0x52, 0x49, 0x46, 0x46, 1, 0, 0, 0, 0x57, 0x41, 0x56, 0x45);
  assert.equal(run("supportShotType(wav)"), null);
  context.short = bytes(0x89, 0x50, 0x4e, 0x47);
  assert.equal(run("supportShotType(short)"), null);
  assert.equal(run("supportShotType(null)"), null);
});

test("base64 is exact for a picture larger than one slice", () => {
  const { run, context } = load();
  const big = new Uint8Array(100_000).map((_, i) => (i * 31) & 0xff);
  context.big = big;
  assert.equal(run("supportShotBase64(big)"), Buffer.from(big).toString("base64"));
});

test("only picture files are taken from a clipboard or a drop; text and other files are ignored", () => {
  const { run, context } = load();
  const file = (type) => ({ type, name: "x" });
  context.items = [
    { kind: "string", type: "text/plain", getAsFile: () => null },
    { kind: "file", type: "image/png", getAsFile: () => file("image/png") },
    { kind: "file", type: "application/pdf", getAsFile: () => file("application/pdf") },
    { kind: "file", type: "image/svg+xml", getAsFile: () => file("image/svg+xml") },
  ];
  // the type is only a first filter: the bytes decide in supportShotsAdd (an SVG is refused there)
  assert.deepEqual(JSON.parse(run("JSON.stringify(supportShotsFrom(items).map(function (f) { return f.type; }))")), ["image/png", "image/svg+xml"]);
  context.files = [file("image/jpeg"), file("text/plain")];
  assert.equal(run("supportShotsFrom(files).length"), 1);
  assert.equal(run("supportShotsFrom(null).length + supportShotsFrom([]).length"), 0);
});

test("a picture is staged on the server and shown back; nothing is sent to support by this code", async () => {
  const { run, context, calls, lists } = load();
  context.pic = blobOf(bytes(...PNG));
  run('supportShotsAdd("spReply", pic)');
  await settle();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.equal(calls[0].path, "/screenshots");
  assert.equal(calls[0].body.data, Buffer.from(PNG).toString("base64"));
  assert.deepEqual(JSON.parse(run('JSON.stringify(supportShotsIds("spReply"))')), ["shot_1"]);
  assert.match(lists["#spReplyShotsList"], /<img src="blob:test/);
  assert.match(lists["#spReplyShotsList"], /data-id="shot_1"/);
  // another form is independent
  assert.deepEqual(JSON.parse(run('JSON.stringify(supportShotsIds("spIssue"))')), []);
});

test("not a picture, too large, or too many: refused here, with a sentence, before anything is staged", async () => {
  const { run, context, calls, alerts } = load();
  context.svg = blobOf(text('<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'));
  run('supportShotsAdd("spReply", svg)');
  await settle();
  assert.match(alerts["#spReplyShotsAlert"], /not a PNG, JPEG, GIF or WebP/);
  context.huge = blobOf(bytes(...PNG), 5 * 1024 * 1024 + 1);
  run('supportShotsAdd("spReply", huge)');
  assert.match(alerts["#spReplyShotsAlert"], /at most 5 MB/);
  assert.equal(calls.length, 0);

  for (let i = 0; i < 5; i++) {
    context.p = blobOf(bytes(...PNG));
    run('supportShotsAdd("spIssue", p)');
    await settle();
  }
  assert.equal(calls.length, 5);
  context.p = blobOf(bytes(...PNG));
  run('supportShotsAdd("spIssue", p)');
  assert.match(alerts["#spIssueShotsAlert"], /At most 5 screenshots/);
  assert.equal(calls.length, 5);
});

test("a refusal from the server is shown and nothing is kept; removing and clearing tell the server and release the picture", async () => {
  const { run, context, calls, alerts, revoked, fail } = load();
  fail({ message: "A screenshot must be a PNG, JPEG, GIF or WebP image" });
  context.pic = blobOf(bytes(...PNG));
  run('supportShotsAdd("spReply", pic)');
  await settle();
  assert.match(alerts["#spReplyShotsAlert"], /must be a PNG/);
  assert.deepEqual(JSON.parse(run('JSON.stringify(supportShotsIds("spReply"))')), []);
  assert.equal(revoked.length, 1, "the thumbnail of a refused picture is released");
  fail(null);

  run('supportShotsAdd("spReply", pic)');
  await settle();
  run('supportShotsAdd("spReply", pic)');
  await settle();
  const ids = JSON.parse(run('JSON.stringify(supportShotsIds("spReply"))'));
  assert.equal(ids.length, 2);
  context.removeId = ids[0];
  // the remove button's handler reads data attributes: exercise the state change it performs through clear instead
  run('supportShotsClear("spReply")');
  assert.deepEqual(JSON.parse(run('JSON.stringify(supportShotsIds("spReply"))')), []);
  const deletes = calls.filter((c) => c.method === "DELETE").map((c) => c.path);
  assert.deepEqual(deletes.sort(), ids.map((i) => "/screenshots/" + i).sort());

  // a SENT form keeps the server's copies (the send consumed them) and only releases the thumbnails
  run('supportShotsAdd("spIssue", pic)');
  await settle();
  const before = calls.filter((c) => c.method === "DELETE").length;
  run('supportShotsClear("spIssue", true)');
  assert.equal(calls.filter((c) => c.method === "DELETE").length, before);
});
