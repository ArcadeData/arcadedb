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

// Regression test for issue #10239: "Copy to clipboard" on the JSON result tab answered "Error. Failed to copy to
// clipboard" and copied nothing.
//
// Studio called navigator.clipboard.writeText() directly. The async Clipboard API exists only in a secure context
// (https, or http on localhost): on a plain-http page served from any other host, navigator.clipboard is undefined and
// the call throws a TypeError before anything is copied. Four of the other copy buttons (API token, MCP config, API
// playground response, AI code block) had no catch at all, so they failed silently.
//
// copyTextToClipboard() is the one place that talks to the clipboard. It uses the async API when it is there and falls
// back to a hidden textarea + document.execCommand("copy") when it is not (or when it rejects). Run with:
//
//     node --test studio/test/clipboard-copy-fallback.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const JS_DIR = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const utilsSrc = fs.readFileSync(path.join(JS_DIR, "studio-utils.js"), "utf8");

function extractFn(src, name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found: " + name);
  return blockFrom(src, start);
}

// Source from `start` through the brace that closes the first block opened after it. It counts braces only: a "{" or
// "}" inside a string, regex or comment in the extracted code would throw it off, so keep those out of the helper.
function blockFrom(src, start) {
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

// A just-enough DOM: records what the fallback appends, selects and copies, and what is left behind afterwards.
function fakeDom({ execCopy = "ok", modals = 0 } = {}) {
  const log = { copied: null, execCalls: 0, selected: false, focusRestored: false, areas: [] };
  const makeHost = () => ({
    children: [],
    appendChild(el) {
      el.parentNode = this;
      this.children.push(el);
    },
    removeChild(el) {
      this.children.splice(this.children.indexOf(el), 1);
      el.parentNode = null;
    },
  });
  const body = makeHost();
  const modalEls = Array.from({ length: modals }, makeHost);
  const topModal = modalEls.length > 0 ? modalEls[modalEls.length - 1] : null;
  const previouslyFocused = { focus: () => (log.focusRestored = true) };
  const document = {
    body,
    activeElement: previouslyFocused,
    querySelectorAll: (selector) => (selector === ".modal.show" ? modalEls : []),
    createElement: (tag) => {
      const el = {
        tag,
        style: {},
        attrs: {},
        value: "",
        setAttribute(k, v) {
          this.attrs[k] = v;
        },
        focus() {},
        select() {
          log.selected = true;
          this.selectedValue = this.value;
        },
        setSelectionRange() {},
      };
      log.areas.push(el);
      return el;
    },
    execCommand(cmd) {
      assert.equal(cmd, "copy");
      log.execCalls++;
      if (execCopy === "throw") throw new Error("execCommand blocked");
      if (execCopy === "fail") return false;
      const host = topModal || body;
      const area = host.children.find((c) => c.tag === "textarea");
      assert.ok(area, "the textarea must be attached to the document while copying");
      assert.equal(area.selectedValue, area.value, "the whole text must be selected before copying");
      log.copied = area.value;
      return true;
    },
  };
  return { document, log, body, modalEls };
}

function loadHelper({ navigator, document }) {
  // new Function keeps the fake navigator/document out of the test process' globals (Node 21+ has a real navigator)
  return new Function("navigator", "document", extractFn(utilsSrc, "copyTextToClipboard") + "\nreturn copyTextToClipboard;")(
    navigator,
    document
  );
}

test("insecure context (navigator.clipboard undefined) falls back to execCommand - the #10239 case", async () => {
  const dom = fakeDom();
  const copy = loadHelper({ navigator: {}, document: dom.document });

  await copy('{"result":[{"name":"Jay"}]}');

  assert.equal(dom.log.copied, '{"result":[{"name":"Jay"}]}');
  assert.equal(dom.log.execCalls, 1);
  assert.equal(dom.body.children.length, 0, "the temporary textarea must be removed");
  assert.ok(dom.log.focusRestored, "focus must go back to the element that had it");
});

test("uses the async Clipboard API when available and does not touch the DOM", async () => {
  const dom = fakeDom();
  let written = null;
  const navigator = {
    clipboard: {
      writeText: async (t) => {
        written = t;
      },
    },
  };
  const copy = loadHelper({ navigator, document: dom.document });

  await copy("hello");

  assert.equal(written, "hello");
  assert.equal(dom.log.execCalls, 0);
});

test("falls back to execCommand when the async Clipboard API rejects (permissions policy, no focus)", async () => {
  const dom = fakeDom();
  const navigator = {
    clipboard: {
      writeText: () => Promise.reject(new Error("NotAllowedError")),
    },
  };
  const copy = loadHelper({ navigator, document: dom.document });

  await copy("hello");

  assert.equal(dom.log.copied, "hello");
  assert.equal(dom.body.children.length, 0);
});

test("falls back to execCommand when the async Clipboard API throws synchronously", async () => {
  const dom = fakeDom();
  const navigator = {
    clipboard: {
      writeText: () => {
        throw new TypeError("synchronous failure");
      },
    },
  };
  const copy = loadHelper({ navigator, document: dom.document });

  await copy("hello");

  assert.equal(dom.log.copied, "hello");
});

test("rejects when neither mechanism can copy, and still cleans up", async () => {
  for (const execCopy of ["fail", "throw"]) {
    const dom = fakeDom({ execCopy });
    const copy = loadHelper({ navigator: {}, document: dom.document });

    await assert.rejects(copy("hello"), /copy/i, "execCopy=" + execCopy);

    assert.equal(dom.body.children.length, 0, "the temporary textarea must be removed (execCopy=" + execCopy + ")");
  }
});

test("inside an open Bootstrap modal the textarea goes into the modal, so its focus trap does not steal the selection", async () => {
  const dom = fakeDom({ modals: 1 });
  const copy = loadHelper({ navigator: {}, document: dom.document });

  await copy("settings");

  assert.equal(dom.log.copied, "settings");
  assert.equal(dom.modalEls[0].children.length, 0);
  assert.equal(dom.body.children.length, 0);
});

test("with stacked modals the textarea goes into the one on top (the last open)", async () => {
  const dom = fakeDom({ modals: 2 });
  const hosts = [];
  dom.modalEls.forEach((m) => {
    const append = m.appendChild.bind(m);
    m.appendChild = (el) => {
      hosts.push(m);
      append(el);
    };
  });
  const copy = loadHelper({ navigator: {}, document: dom.document });

  await copy("settings");

  assert.equal(dom.log.copied, "settings");
  assert.deepEqual(hosts, [dom.modalEls[1]], "only the topmost modal may host the textarea");
  assert.equal(dom.body.children.length, 0);
});

test("the temporary textarea is not aria-hidden (it takes focus) and has a 16px font (no iOS zoom on focus)", async () => {
  const dom = fakeDom();
  const copy = loadHelper({ navigator: {}, document: dom.document });

  await copy("hello");

  assert.equal(dom.log.areas.length, 1);
  const area = dom.log.areas[0];
  assert.equal(area.attrs["aria-hidden"], undefined, "Chrome warns about a focused aria-hidden element");
  assert.equal(area.attrs.readonly, "", "readonly keeps the on-screen keyboard away on mobile");
  assert.equal(area.style.fontSize, "16px");
});

test("copies an empty or missing value as an empty string instead of throwing", async () => {
  const dom = fakeDom();
  const copy = loadHelper({ navigator: {}, document: dom.document });

  await copy(null);

  assert.equal(dom.log.copied, "");
});

// Guard: a new copy button that calls navigator.clipboard directly brings the bug back for every plain-http install.
test("no Studio script calls navigator.clipboard.writeText except the shared helper", () => {
  const offenders = [];
  for (const file of fs.readdirSync(JS_DIR).filter((f) => f.endsWith(".js"))) {
    const src = fs.readFileSync(path.join(JS_DIR, file), "utf8");
    // the helper itself lives in studio-utils.js; drop its body before looking for stragglers
    const scan = file === "studio-utils.js" ? src.replace(extractFn(src, "copyTextToClipboard"), "") : src;
    if (/navigator\.clipboard\.writeText/.test(scan)) offenders.push(file);
  }
  assert.deepEqual(offenders, [], "use copyTextToClipboard() instead of navigator.clipboard.writeText()");
});

test("every copy button reports a failed copy instead of failing silently", () => {
  const sites = [
    ["studio-query.js", "$(\"#jsonToClipboard\")"],
    ["studio-graph.js", "$(\"#popupClipboard\")"],
    ["studio-api.js", "function copyApiResponse("],
    ["studio-security.js", "function copyCreatedToken("],
    ["studio-ai.js", "function aiCopyCode("],
    ["studio-server.js", "function copyMCPConfig("],
  ];
  for (const [file, anchor] of sites) {
    const src = fs.readFileSync(path.join(JS_DIR, file), "utf8");
    const at = src.indexOf(anchor);
    assert.ok(at >= 0, file + ": anchor not found: " + anchor);
    const body = blockFrom(src, at);
    assert.match(body, /copyTextToClipboard\(/, file + " must go through copyTextToClipboard()");
    assert.match(body, /\.catch\(|catch *\(/, file + " must handle a failed copy");
  }
});
