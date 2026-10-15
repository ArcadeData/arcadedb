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

// Regression guard for issue #10101: an end-to-end test exists to FAIL when the feature it names is absent. The three
// Studio graph-export tests probed for the feature ("is there an Export button?", "is there a graph.graphml()?") and ran
// their assertions only when the probe succeeded, so the feature being absent was exactly what made the assertions not
// run; the same shape was found in seven more tests. No test called Studio's exportGraph(), and replacing its body with
// `return;` left every one of the 97 tests green.
//
// This is the mechanical half of the fix: a test(...) in e2e-studio/tests whose body holds no expect(...) at its OWN
// nesting level (not inside an if, try, catch, loop or callback) is a probe-then-skip test, and it is refused here, so the
// shape cannot be added back. The suite itself needs Docker and a browser, this check needs neither.
//
// A helper that asserts on the test's behalf does not count: the expect() has to be written in the test body, so a test
// that delegates all of its assertions to a helper adds one of its own (the filename, the content) after calling it.
//
// @author Luca Garulli (l.garulli@arcadedata.com)
//
// Run with:
//
//     node --test studio/test/e2e-tests-assert-unconditionally.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SPEC_DIR = path.join(__dirname, "..", "..", "e2e-studio", "tests");

// Replaces the content of comments, strings, template literals and regular expressions with spaces (line breaks kept),
// so the braces, parentheses and keywords left in the text are all code and every offset still maps to the source
function maskNonCode(src) {
  const out = src.split("");
  const blank = (from, to) => {
    for (let k = from; k < to; k++) if (out[k] !== "\n") out[k] = " ";
  };
  // Template literals nest: a "${" opens code again, closed by its matching "}"
  const templates = []; // brace depth at which each open template's "${" resumed
  let braces = 0;
  let prevSignificant = "";
  let i = 0;

  const readTemplateChars = (start) => {
    let k = start;
    while (k < src.length) {
      if (src[k] === "\\") k += 2;
      else if (src[k] === "`") {
        blank(start, k);
        return k + 1;
      } else if (src[k] === "$" && src[k + 1] === "{") {
        blank(start, k);
        templates.push(braces);
        braces++;
        prevSignificant = "{";
        return k + 2;
      } else k++;
    }
    blank(start, src.length);
    return src.length;
  };

  while (i < src.length) {
    const c = src[i];
    const next = src[i + 1];
    if (c === "/" && next === "/") {
      const end = src.indexOf("\n", i);
      const stop = end < 0 ? src.length : end;
      blank(i, stop);
      i = stop;
    } else if (c === "/" && next === "*") {
      const end = src.indexOf("*/", i + 2);
      const stop = end < 0 ? src.length : end + 2;
      blank(i, stop);
      i = stop;
    } else if (c === "'" || c === '"') {
      let k = i + 1;
      while (k < src.length && src[k] !== c && src[k] !== "\n") k += src[k] === "\\" ? 2 : 1;
      blank(i + 1, k);
      i = k + 1;
      prevSignificant = c;
    } else if (c === "`") {
      i = readTemplateChars(i + 1);
      prevSignificant = "`";
    } else if (c === "/" && /[(,=:[!&|?{};]|^$/.test(prevSignificant)) {
      // a regular expression literal: the character before it cannot end an operand
      let k = i + 1;
      let inClass = false;
      while (k < src.length && src[k] !== "\n") {
        if (src[k] === "\\") k++;
        else if (src[k] === "[") inClass = true;
        else if (src[k] === "]") inClass = false;
        else if (src[k] === "/" && !inClass) break;
        k++;
      }
      blank(i + 1, k);
      i = k + 1;
      prevSignificant = "/";
    } else {
      if (c === "{") braces++;
      else if (c === "}") {
        braces--;
        if (templates.length > 0 && templates[templates.length - 1] === braces) {
          templates.pop();
          i = readTemplateChars(i + 1);
          prevSignificant = "`";
          continue;
        }
      }
      if (!/\s/.test(c)) prevSignificant = c;
      i++;
    }
  }
  return out.join("");
}

function matching(masked, open, openChar, closeChar) {
  let depth = 0;
  for (let k = open; k < masked.length; k++) {
    if (masked[k] === openChar) depth++;
    else if (masked[k] === closeChar && --depth === 0) return k;
  }
  return -1;
}

const lineOf = (src, offset) => src.substring(0, offset).split("\n").length;

// Every test(...) / test.only(...) / test.fixme(...) body (test.skip has none to run): its title, line, and the number of
// expect(...) calls sitting at the body's own nesting level
function scanTests(src) {
  const masked = maskNonCode(src);
  const tests = [];
  const header = /(?<![\w.])test(?:\.(?:only|fixme|fail|slow))?\s*\(/g;
  let m;
  while ((m = header.exec(masked)) !== null) {
    const openParen = m.index + m[0].length - 1;
    const closeParen = matching(masked, openParen, "(", ")");
    assert.ok(closeParen > 0, `unbalanced parentheses in the test declared at line ${lineOf(src, m.index)}`);

    // the callback's "=>" sits directly in the call's own parentheses, not inside its parameter list
    let depth = 0;
    let arrow = -1;
    for (let k = openParen; k < closeParen; k++) {
      if (masked[k] === "(") depth++;
      else if (masked[k] === ")") depth--;
      else if (masked[k] === "=" && masked[k + 1] === ">" && depth === 1) {
        arrow = k;
        break;
      }
    }
    const title = (/^\s*(['"`])((?:\\.|(?!\1).)*)\1/.exec(src.substring(openParen + 1)) || [])[2] || "(unnamed)";
    if (arrow < 0) continue;
    const bodyOpen = masked.indexOf("{", arrow);
    if (bodyOpen < 0 || bodyOpen > closeParen || masked.substring(arrow + 2, bodyOpen).trim() !== "") continue;
    const bodyClose = matching(masked, bodyOpen, "{", "}");

    let level = 0;
    let own = 0;
    for (let k = bodyOpen + 1; k < bodyClose; k++) {
      if (masked[k] === "{") level++;
      else if (masked[k] === "}") level--;
      else if (level === 0 && masked[k] === "e" && /^expect(?:\.(?:soft|poll))?\s*\(/.test(masked.substring(k, k + 14)) && !/[\w.$]/.test(masked[k - 1]))
        own++;
    }
    tests.push({ title, line: lineOf(src, m.index), assertions: own });
    header.lastIndex = bodyClose;
  }
  return tests;
}

// --- The scanner ---------------------------------------------------------------------------------------------------

test("a test whose only assertions sit behind a probe is refused", () => {
  const src = `
    test('probe then skip', async ({ page }) => {
      const button = page.locator('#export');
      if (await button.isVisible()) {
        expect(await button.textContent()).toBe('Export');
      } else {
        console.log('no button');
      }
    });
    test('try and catch', async ({ page }) => {
      try {
        await expect(page.locator('#x')).toBeVisible();
      } catch (e) {
        expect(e).toBeDefined();
      }
    });
    test('a loop that may run zero times', async ({ page }) => {
      for (const n of []) { expect(n).toBe(1); }
    });
    test('a callback is not the test body', async ({ page }) => {
      [1].forEach(n => { expect(n).toBe(1); });
    });
  `;
  assert.deepEqual(scanTests(src).map((t) => [t.title, t.assertions]), [
    ["probe then skip", 0],
    ["try and catch", 0],
    ["a loop that may run zero times", 0],
    ["a callback is not the test body", 0],
  ]);
});

test("a test that asserts at its own level is accepted, whatever follows", () => {
  const src = `
    test('plain', async ({ page }) => {
      await expect(page.locator('#x')).toBeVisible();
    });
    test("after a conditional", async ({ page }) => {
      if (await page.locator('#a').isVisible()) { console.log('a'); }
      expect(await page.title()).toContain('Arcade');
    });
    test('soft and poll', async ({ page }) => {
      expect.soft(1).toBe(1);
      await expect.poll(async () => { return 1; }).toBe(1);
    });
    test('braces in strings, regexes and templates do not count', async ({ page }) => {
      const s = "}}}" + '{{{' + \`\${1 + 1} } {\`;
      const r = /}{/.test(s);
      // } { expect(1).toBe(1) in a comment
      await page.evaluate(() => { return 1; });
      expect(r).toBe(false);
    });
  `;
  assert.deepEqual(scanTests(src).map((t) => [t.title, t.assertions]), [
    ["plain", 1],
    ["after a conditional", 1],
    ["soft and poll", 2],
    ["braces in strings, regexes and templates do not count", 1],
  ]);
});

test("a declaration without a body, test.skip and describe blocks are not tests", () => {
  const src = `
    test.describe('suite', () => {
      test.skip('skipped', async ({ page }) => { });
      test('real', async ({ page }) => { expect(1).toBe(1); });
    });
  `;
  assert.deepEqual(scanTests(src).map((t) => t.title), ["real"]);
});

// --- The suite -----------------------------------------------------------------------------------------------------

test("every end-to-end test asserts unconditionally", () => {
  const specs = fs.readdirSync(SPEC_DIR).filter((f) => f.endsWith(".spec.ts")).sort();
  assert.ok(specs.length > 0, "the e2e specs are read");

  let total = 0;
  const offenders = [];
  for (const spec of specs) {
    const src = fs.readFileSync(path.join(SPEC_DIR, spec), "utf8");
    for (const t of scanTests(src)) {
      total++;
      if (t.assertions === 0) offenders.push(`e2e-studio/tests/${spec}:${t.line}  ${t.title}`);
    }
  }
  assert.ok(total > 50, `the scan finds the suite's tests (found ${total})`);
  assert.deepEqual(offenders, [], "these tests run no expect() at their own nesting level: an assertion behind an if, try, catch, "
    + "loop or callback is skipped exactly when the feature under test is missing, so the test cannot fail. Drive the real control "
    + "and assert unconditionally, or delete the probe and let the missing control fail the test.");
});
