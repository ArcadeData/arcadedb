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

// Regression test for issue #4893: logging out and back in the Studio query interface
// used to add a new CodeMirror wrapper div for every login, because initQuery() ->
// renderCodeEditor() runs on each login and called CodeMirror.fromTextArea() again
// without disposing the previous instance. renderCodeEditor() now calls
// editor.toTextArea() first, so only a single CodeMirror wrapper ever exists. Run with:
//
//     node --test studio/test/relogin-codemirror.test.js
//
// renderCodeEditor() and getEditorMode() live in query.html (there is no bundler for the
// application JS), so we extract them from the HTML and exercise them against a mock
// CodeMirror that models the fromTextArea/toTextArea DOM lifecycle.

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const HTML_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "query.html");
const html = fs.readFileSync(HTML_PATH, "utf8");

function extractFn(src, name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in query.html: " + name);
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

test("re-login never stacks more than one CodeMirror editor wrapper", () => {
  // The fake DOM: a list of CodeMirror wrapper divs currently attached to the page.
  const wrappers = [];

  // Minimal CodeMirror mock reproducing the real fromTextArea/toTextArea contract:
  // fromTextArea inserts a new wrapper div next to the textarea; toTextArea removes it.
  const CodeMirror = {
    fromTextArea: function () {
      const wrapper = { class: "CodeMirror cm-s-neo CodeMirror-wrap" };
      wrappers.push(wrapper);
      return {
        _wrapper: wrapper,
        addKeyMap: function () {},
        on: function () {},
        refresh: function () {},
        toTextArea: function () {
          const idx = wrappers.indexOf(this._wrapper);
          if (idx >= 0) wrappers.splice(idx, 1);
        },
      };
    },
    showHint: function () {},
  };

  // Other globals referenced by renderCodeEditor()/getEditorMode() at call time.
  const document = { getElementById: function () { return {}; } };
  const $ = function () { return { val: function () { return "sql"; } }; };
  const setTimeout = function () {}; // avoid firing the deferred editor.refresh()
  var editor = null;

  eval(extractFn(html, "getEditorMode"));
  eval(extractFn(html, "renderCodeEditor"));

  // First page load with a stored session.
  renderCodeEditor();
  assert.strictEqual(wrappers.length, 1, "one editor after the first login");

  // Log out then back in repeatedly (initQuery -> renderCodeEditor each time).
  for (let i = 0; i < 5; i++)
    renderCodeEditor();

  assert.strictEqual(wrappers.length, 1, "still exactly one editor wrapper after several re-logins");
  assert.notStrictEqual(editor, null, "a live editor instance remains bound after re-login");
});
