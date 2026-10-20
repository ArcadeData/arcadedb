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
// "Own level" is a position in the syntax, not a brace depth (issue #10389): the guard counts an expect() only when it is
// the first thing of a statement that nothing governs, so `if (c) expect(...)`, `for (...) expect(...)`, `ok && expect(...)`,
// `ok ? expect(...) : log()` and `list.forEach(n => expect(...))` are refused exactly like their braced spellings. A test
// declared with a `function` callback or a concise arrow body is scanned like any other, and a declaration whose body cannot
// be read at all is reported as an offender rather than skipped, so the census cannot shrink without failing the build.
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

// Words that take the place of the "(" of a loop or conditional head: the ")" that closes that head leaves the statement
// WITHOUT its body yet, so whatever follows is governed by it
const HEAD_KEYWORDS = new Set(["if", "for", "while", "with"]);
// Words that do not move the statement position: `await expect(...)` and `return expect(...)` assert wherever the bare
// `expect(...)` would
const TRANSPARENT_KEYWORDS = new Set(["await", "return"]);
// Words that continue an expression or open a governed statement, so an expect() right behind them is not a statement of
// its own
const OPERATOR_KEYWORDS = new Set(["else", "do", "typeof", "void", "new", "in", "of", "instanceof", "case", "default", "yield", "delete", "throw"]);
const EXPECT_CALL = /expect(?:\s*\.\s*(?:soft|poll))?\s*\(/y;

// Whether the word at `at` opens a call to expect, expect.soft or expect.poll, however the call is spaced
function callsExpectAt(masked, at) {
  EXPECT_CALL.lastIndex = at;
  return EXPECT_CALL.test(masked);
}

// Counts the expect(...) calls in masked[from, to) that run on EVERY execution of that range: the range read as a statement
// list, an expect() counts only when it is the first thing of a statement that nothing governs. That is a question about
// syntax, not about braces: the statement after `if (c)`, `else`, `do`, a loop head, behind `&&`, `||`, `??`, `?`, `:` or
// inside any bracket (a callback, an argument, a block) is the same assertion that skips when the condition does not hold,
// however it is spelled. Anything this cannot place with confidence is NOT counted: the guard errs towards refusing.
function countOwnAssertions(masked, from, to) {
  const nesting = []; // one entry per open bracket: "(", "[", "{" or "head" for the "(" of an if/for/while
  let own = 0;
  // where the next token sits: "start" (a statement begins), "operand" (an expression just ended, so a statement begins
  // only after a line break: automatic semicolon insertion), "operator" (an expression or a governed statement continues)
  // or "head" (a loop or conditional head just closed)
  let state = "start";
  let newline = false;
  let head = false; // an if/for/while/with was just read: its "(" opens a head
  let member = false; // the last token was ".": the word that follows is a property name, never a keyword
  let doOpen = 0; // `do` statements whose closing `while (...)` has not been read yet
  let k = from;
  while (k < to) {
    const c = masked[k];
    if (c === "\n") {
      newline = true;
      k++;
    } else if (/\s/.test(c)) k++;
    else if (/[\w$]/.test(c)) {
      let end = k + 1;
      while (end < to && /[\w$]/.test(masked[end])) end++;
      const word = masked.substring(k, end);
      const startsStatement = nesting.length === 0 && (state === "start" || (state === "operand" && newline));
      const isAssertion = !member && word === "expect" && callsExpectAt(masked, k);
      if (isAssertion && startsStatement) {
        own++;
        state = "operand";
        newline = false;
      } else if (!member && TRANSPARENT_KEYWORDS.has(word)) {
        // the position is unchanged, and so is the line break seen so far
      } else {
        // the `while` right behind the body of a `do` closes that statement, it does not open a head of its own
        const doTail = !member && word === "while" && doOpen > 0 && nesting.length === 0 && state === "start";
        if (doTail) doOpen--;
        else if (!member && word === "do" && nesting.length === 0) doOpen++;
        head = !member && !doTail && HEAD_KEYWORDS.has(word);
        state = !member && (OPERATOR_KEYWORDS.has(word) || head) ? "operator" : "operand";
        newline = false;
      }
      member = false;
      k = end;
    } else {
      if (c === "(" || c === "[" || c === "{") {
        nesting.push(c === "(" && head ? "head" : c);
        state = "operator";
      } else if (c === ")" || c === "]" || c === "}") {
        const closed = nesting.pop();
        // a block ends a statement, a head leaves its body to come, any other bracket ends an operand
        state = closed === "head" ? "head" : closed === "{" ? "start" : "operand";
      } else state = c === ";" && nesting.length === 0 ? "start" : c === "'" || c === '"' || c === "`" ? "operand" : "operator";
      head = false;
      member = c === ".";
      newline = false;
      k++;
    }
  }
  return own;
}

// Offset where the argument that starts at `from` ends: the next comma or closing parenthesis outside any bracket
function argumentEnd(masked, from, closeParen) {
  let depth = 0;
  for (let k = from; k < closeParen; k++) {
    if (masked[k] === "(" || masked[k] === "[" || masked[k] === "{") depth++;
    else if (masked[k] === ")" || masked[k] === "]" || masked[k] === "}") depth--;
    else if (masked[k] === "," && depth === 0) return k;
  }
  return closeParen;
}

// The range [from, to) of the inline function a test(...) call declares as its body, looked for among the call's own
// arguments only (not inside a parameter list or a details object): `=> {...}`, `=> expression`, `function (...) {...}`,
// plus the offset of the "=>" or "function" that introduces it; null when the call carries none
function callbackBody(masked, openParen, closeParen) {
  let depth = 0;
  for (let k = openParen + 1; k < closeParen; k++) {
    const c = masked[k];
    if (c === "(" || c === "[" || c === "{") depth++;
    else if (c === ")" || c === "]" || c === "}") depth--;
    else if (depth === 0 && c === "=" && masked[k + 1] === ">") {
      let body = k + 2;
      while (body < closeParen && /\s/.test(masked[body])) body++;
      if (masked[body] === "{") return [body + 1, matching(masked, body, "{", "}"), k];
      return [body, argumentEnd(masked, body, closeParen), k];
    } else if (depth === 0 && c === "f" && /^function\b/.test(masked.substring(k, k + 9)) && !/[\w.$]/.test(masked[k - 1])) {
      const params = masked.indexOf("(", k);
      const paramsEnd = params < 0 ? -1 : matching(masked, params, "(", ")");
      const body = paramsEnd < 0 ? -1 : masked.indexOf("{", paramsEnd);
      if (body < 0 || body > closeParen) return null;
      return [body + 1, matching(masked, body, "{", "}"), k];
    }
  }
  return null;
}

// A test(...) declaration opens with a literal title; test.fixme(cond, "why"), test.fail(cond) and test.slow() inside a test
// or a describe annotate, they do not declare
const DECLARATION_HEAD = /^\s*['"`]/;

// Every test(...) / test.only(...) / test.fixme(...) declaration (test.skip has no body to run): its title, line, the number
// of expect(...) calls its body runs unconditionally, whether the body could not be analysed at all, and whether the title
// is a literal. A declaration is never dropped: a body the scanner cannot read is reported as `unanalysable`, so the suite
// fails instead of shrinking
function scanTests(src) {
  const masked = maskNonCode(src);
  const tests = [];
  const header = /(?<![\w.])test(?:\s*\.\s*(?:only|fixme|fail|slow))?\s*\(/g;
  let m;
  while ((m = header.exec(masked)) !== null) {
    const openParen = m.index + m[0].length - 1;
    const closeParen = matching(masked, openParen, "(", ")");
    assert.ok(closeParen > 0, `unbalanced parentheses in the test declared at line ${lineOf(src, m.index)}`);

    const title = (/^\s*(['"`])((?:\\.|(?!\1).)*)\1/.exec(src.substring(openParen + 1)) || [])[2] || "(unnamed)";
    const line = lineOf(src, m.index);
    const body = callbackBody(masked, openParen, closeParen);
    const literalTitle = DECLARATION_HEAD.test(masked.substring(openParen + 1, closeParen));
    // `test(` and `test.only(` only ever declare. test.fixme / test.fail / test.slow declare when they open with a title or
    // carry their body after the first argument (a computed title: test.fail(name, async () => {...})); they annotate
    // otherwise, even when the condition is a function of the fixtures: test.fixme(({ browserName }) => ..., "why")
    const annotates = /\.\s*(?:fixme|fail|slow)\s*\($/.test(m[0])
      && !literalTitle
      && (body === null || body[2] < argumentEnd(masked, openParen + 1, closeParen));
    if (annotates) {
      header.lastIndex = closeParen;
      continue;
    }
    if (body === null) {
      tests.push({ title, line, assertions: 0, unanalysable: true, literalTitle });
      header.lastIndex = closeParen;
      continue;
    }
    tests.push({ title, line, assertions: countOwnAssertions(masked, body[0], body[1]), unanalysable: false, literalTitle });
    // a test(...) declared inside this one is not visited (Playwright refuses it at run time); countDeclarations sees it, so
    // the suite's cross-check reports the gap
    header.lastIndex = closeParen;
  }
  return tests;
}

// How many tests the source declares, counted by a different method than scanTests so that one can check the other: every
// test(...) / test.only(...) / test.fixme(...) that opens with a literal title, wherever it sits and across line breaks. It
// does not step over a test body the way the scan does, so a declaration the scan jumps past (one nested in another test's
// body) shows up here as a gap. It is still a one-way check: a declaration with a computed title (`test(name, ...)`) is not
// counted, so the scan, not the census, is what catches that one (it reports it as unanalysable when it has no inline body)
function countDeclarations(src) {
  return (maskNonCode(src).match(/(?<![\w.])test(?:\s*\.\s*(?:only|fixme|fail|slow))?\s*\(\s*['"`]/g) || []).length;
}

// The two counts of one source, compared like for like: the declarations the scan returned with a literal title (a
// computed-title test is in the scan but cannot be in the census) against the census. They are compared per source, never
// summed over several: an extra computed-title entry in one spec must not cancel a declaration missed in another
function censusGap(src) {
  return { scanned: scanTests(src).filter((t) => t.literalTitle).length, declared: countDeclarations(src) };
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

// Every spelling that makes an expect() run only SOMETIMES must be refused, braced or not: the guard counts the position
// of the assertion in the syntax, so dropping the braces (or writing a `&&`, a `?:`, a callback) must not move it to the
// test's own level. Each body holds exactly one expect(); the verdict is the same for all of them.
const CONDITIONAL_SHAPES = [
  ["braced if", "if (await page.locator('#x').isVisible()) { expect(1).toBe(1); }"],
  ["braceless if", "if (await page.locator('#x').isVisible()) expect(1).toBe(1);"],
  ["braceless if, body on the next line", "if (ok)\n  expect(1).toBe(1);"],
  ["braced else", "if (ok) { log(); } else { expect(1).toBe(1); }"],
  ["braceless else", "if (ok) log(); else expect(1).toBe(1);"],
  ["braceless else if", "if (a) log(); else if (b) expect(1).toBe(1);"],
  ["nested braceless ifs", "if (a) if (b) expect(1).toBe(1);"],
  ["braceless if returning the assertion", "if (ok) return expect(1).toBe(1);"],
  ["braced for", "for (const n of []) { expect(n).toBe(1); }"],
  ["braceless for", "for (const n of []) expect(n).toBe(1);"],
  ["braceless for await", "for await (const n of []) expect(n).toBe(1);"],
  ["braced while", "while (ok) { expect(1).toBe(1); }"],
  ["braceless while", "while (ok) expect(1).toBe(1);"],
  ["braceless do", "do expect(1).toBe(1); while (ok);"],
  ["braceless while right after a do-while", "do { log(); } while (a);\nwhile (b) expect(1).toBe(1);"],
  ["try", "try { expect(1).toBe(1); } catch (e) { log(e); }"],
  ["catch", "try { log(); } catch (e) { expect(e).toBeDefined(); }"],
  ["finally", "try { log(); } finally { expect(1).toBe(1); }"],
  ["switch", "switch (ok) { case 1: expect(1).toBe(1); }"],
  ["&&", "ok && expect(ok).toBe(1);"],
  ["&& before an await", "ok && await expect(ok).toBe(1);"],
  ["&& at the start of the next line", "ok\n  && expect(ok).toBe(1);"],
  ["||", "ok || expect(ok).toBe(1);"],
  ["??", "ok ?? expect(ok).toBe(1);"],
  ["ternary, consequent", "ok ? expect(ok).toBe(1) : log();"],
  ["ternary, alternate", "ok ? log() : expect(ok).toBe(1);"],
  ["forEach with a concise arrow", "[].forEach(n => expect(n).toBe(1));"],
  ["forEach with a block arrow", "[].forEach(n => { expect(n).toBe(1); });"],
  ["forEach with a function", "[].forEach(function (n) { expect(n).toBe(1); });"],
  ["map over promises", "await Promise.all(xs.map(async (n) => expect(n).toBe(1)));"],
  ["test.step", "await test.step('s', async () => { expect(1).toBe(1); });"],
];

const UNCONDITIONAL_SHAPES = [
  ["plain", "expect(1).toBe(1);"],
  ["awaited", "await expect(page.locator('#x')).toBeVisible();"],
  ["returned", "return expect(1).toBe(1);"],
  ["soft", "expect.soft(1).toBe(1);"],
  ["poll", "await expect.poll(async () => 1).toBe(1);"],
  ["poll, call split over lines", "await expect.poll\n  (async () => 1).toBe(1);"],
  ["soft, spaced member access", "expect . soft (1).toBe(1);"],
  ["after a braced conditional", "if (ok) { log(); }\nexpect(1).toBe(1);"],
  ["after a braceless conditional", "if (ok) log();\nexpect(1).toBe(1);"],
  ["after a braceless else", "if (ok) log(); else other();\nexpect(1).toBe(1);"],
  ["after a braceless loop", "for (const n of []) log(n);\nexpect(1).toBe(1);"],
  ["after a call, no semicolon", "log(1)\nexpect(1).toBe(1)"],
  ["after a declaration, no semicolon", "const n = [1].length\nawait expect(n).toBe(1)"],
  ["after a typed declaration", "const errors: string[] = [];\nexpect(errors).toEqual([]);"],
  ["after a cast, no semicolon", "const s = (window as any).x\nexpect(s).toBe(1)"],
  ["after a do-while, no semicolon", "do { log(); } while (ok)\nexpect(1).toBe(1)"],
  ["after a do-while", "do { log(); } while (ok);\nexpect(1).toBe(1);"],
  ["after a braceless do-while", "do log(); while (ok);\nexpect(1).toBe(1);"],
  ["after a property named default", "const v = opts.default\nexpect(v).toBe(1)"],
  ["after a property named of", "const w = x.of\nexpect(w).toBe(1)"],
  ["after a property named in", "const w = x?.in\nexpect(w).toBe(1)"],
  ["after a property named if", "const f = x.if\nexpect(f).toBe(1)"],
  ["after a property named return", "const r = x.return\nexpect(r).toBe(1)"],
];

const wrap = (body) => `test('t', async ({ page }) => {\n${body}\n});`;

test("every conditional spelling of an assertion is refused, braced or not", () => {
  for (const [label, body] of CONDITIONAL_SHAPES) {
    const found = scanTests(wrap(body));
    assert.equal(found.length, 1, `${label}: the test is scanned`);
    assert.equal(found[0].assertions, 0, `${label}: the assertion is conditional, so it is not the test's own`);
  }
});

test("every unconditional spelling of an assertion is accepted", () => {
  for (const [label, body] of UNCONDITIONAL_SHAPES) {
    const found = scanTests(wrap(body));
    assert.equal(found.length, 1, `${label}: the test is scanned`);
    assert.equal(found[0].assertions, 1, `${label}: the assertion runs on every execution`);
  }
});

test("the original probe-then-skip shape stays refused when its braces are dropped", () => {
  const src = `
    test('export', async ({ page }) => {
      const button = page.locator('#export');
      if (await button.isVisible()) expect(await button.textContent()).toBe('Export');
    });
    test('graph export', async ({ page }) => {
      const hasGraph = await page.evaluate(() => typeof graph.graphml === 'function');
      hasGraph && expect(hasGraph).toBe(true);
    });
  `;
  assert.deepEqual(scanTests(src).map((t) => [t.title, t.assertions]), [["export", 0], ["graph export", 0]]);
});

test("function callbacks and concise arrow bodies are scanned, not skipped", () => {
  const src = `
    test('function callback', async function ({ page }) {
      expect(1).toBe(1);
    });
    test('named function callback', async function body({ page }) {
      if (ok) expect(1).toBe(1);
    });
    test('concise arrow', async ({ page }) => expect(1).toBe(1));
    test('concise arrow with await', async ({ page }) => await expect(page.locator('#x')).toBeVisible());
    test('concise arrow behind a guard', async ({ page }) => ok && expect(1).toBe(1));
    test('details argument', { tag: '@slow' }, async ({ page }) => {
      expect(1).toBe(1);
    });
    test.only('only', function () { expect(1).toBe(1); });
    test.fixme('fixme', async () => { expect(1).toBe(1); });
  `;
  assert.deepEqual(scanTests(src).map((t) => [t.title, t.assertions, t.unanalysable]), [
    ["function callback", 1, false],
    ["named function callback", 0, false],
    ["concise arrow", 1, false],
    ["concise arrow with await", 1, false],
    ["concise arrow behind a guard", 0, false],
    ["details argument", 1, false],
    ["only", 1, false],
    ["fixme", 1, false],
  ]);
});

test("a declaration whose body cannot be analysed is reported, not skipped", () => {
  const src = `
    test('named handler', handler);
    test.only('named handler, only', handler);
    test.fixme('named handler, fixme', handler);
    test('analysable', async () => { expect(1).toBe(1); });
  `;
  assert.deepEqual(scanTests(src).map((t) => [t.title, t.assertions, t.unanalysable]), [
    ["named handler", 0, true],
    ["named handler, only", 0, true],
    ["named handler, fixme", 0, true],
    ["analysable", 1, false],
  ]);
});

test("test.fixme, test.fail and test.slow annotations are not declarations", () => {
  const src = `
    test.describe('suite', () => {
      test.fixme(browserName === 'firefox', 'not on firefox');
      test.fail(ok, 'known to fail');
      test.slow();
      test.fixme(({ browserName }) => browserName === 'webkit', 'not on webkit');
      test.slow(({ browserName }) => browserName === 'firefox');
      test('real', async ({ page }) => { expect(1).toBe(1); });
    });
  `;
  assert.deepEqual(scanTests(src).map((t) => t.title), ["real"]);
});

test("test.fixme, test.fail and test.slow with a computed title and an inline body declare a test", () => {
  const src = `
    test.fail(title, async () => { expect(1).toBe(1); });
    test.fixme(name, { tag: '@x' }, async () => { if (ok) expect(1).toBe(1); });
    test.slow(({ browserName }) => browserName === 'firefox');
    test.fixme(({ browserName }) => browserName === 'webkit', 'why');
    test.fixme(ok, 'why');
  `;
  assert.deepEqual(scanTests(src).map((t) => [t.assertions, t.unanalysable]), [[1, false], [0, false]]);
});

test("member access spaced from test is still a declaration", () => {
  const src = `
    test .only('spaced', async () => { expect(1).toBe(1); });
    test
      .fixme('split', async () => { if (ok) expect(1).toBe(1); });
    test . describe ('suite', () => { });
  `;
  assert.deepEqual(scanTests(src).map((t) => [t.title, t.assertions]), [["spaced", 1], ["split", 0]]);
  assert.equal(countDeclarations(src), 2);
});

test("the declaration census counts without going through the scan", () => {
  const src = `
    test('arrow', async () => { expect(1).toBe(1); });
    test('function', async function () { expect(1).toBe(1); });
    test('concise', async () => expect(1).toBe(1));
    test('handler', handler);
    test.only('only', async () => { expect(1).toBe(1); });
    test.skip('skipped', async () => { });
    // test('commented out', async () => { });
    test.describe('suite', () => {
      test.fixme(ok, 'annotation');
    });
  `;
  assert.equal(countDeclarations(src), 5);
  assert.equal(scanTests(src).length, 5);
});

test("the census counts a declaration the scan steps over, so the suite's cross-check can fail", () => {
  // the scan resumes after the outer call, so the nested declaration is never visited; the census does not jump
  const nested = `
    test('outer', async () => {
      expect(1).toBe(1);
      test('inner', async () => { expect(1).toBe(1); });
    });
  `;
  assert.equal(scanTests(nested).length, 1);
  assert.equal(countDeclarations(nested), 2);

  // a title on the line after the opening parenthesis, and a declaration that follows other code on its line
  const spread = `
    test(
      'multi-line',
      async () => { expect(1).toBe(1); },
    );
    const each = [1]; test('after code', async () => { expect(1).toBe(1); });
  `;
  assert.equal(countDeclarations(spread), 2);
  assert.equal(scanTests(spread).length, 2);
});

test("a computed-title test cannot cancel a literal-title declaration the scan missed", () => {
  // the census counts literal titles only, so the two sides are compared on literal titles, one source at a time: summed
  // over the suite, the extra entry of a computed-title test would hide the nested declaration the scan never visited
  const src = `
    test(name, async () => { expect(1).toBe(1); });
    test('outer', async () => {
      expect(1).toBe(1);
      test('inner', async () => { expect(1).toBe(1); });
    });
  `;
  assert.equal(scanTests(src).length, 2);
  assert.deepEqual(censusGap(src), { scanned: 1, declared: 2 });

  const agreeing = `
    test(name, async () => { expect(1).toBe(1); });
    test('plain', async () => { expect(1).toBe(1); });
    test.fail(title, async () => { expect(1).toBe(1); });
  `;
  assert.deepEqual(censusGap(agreeing), { scanned: 1, declared: 1 });
});

// --- The suite -----------------------------------------------------------------------------------------------------

test("every end-to-end test asserts unconditionally", () => {
  const specs = fs.readdirSync(SPEC_DIR).filter((f) => f.endsWith(".spec.ts")).sort();
  assert.ok(specs.length > 0, "the e2e specs are read");

  let declared = 0;
  const offenders = [];
  for (const spec of specs) {
    const src = fs.readFileSync(path.join(SPEC_DIR, spec), "utf8");
    const gap = censusGap(src);
    declared += gap.declared;
    assert.equal(gap.scanned, gap.declared, `e2e-studio/tests/${spec}: the scan saw ${gap.scanned} tests with a literal title, the census counts `
      + `${gap.declared}. A declaration nested inside another test's body is skipped by the scan, and a title the scanner cannot see `
      + "would shrink the suite it checks without failing the build.");
    for (const t of scanTests(src)) {
      if (t.unanalysable) offenders.push(`e2e-studio/tests/${spec}:${t.line}  ${t.title}  (the body is not an inline function, so it cannot be analysed)`);
      else if (t.assertions === 0) offenders.push(`e2e-studio/tests/${spec}:${t.line}  ${t.title}`);
    }
  }
  assert.ok(declared > 0, "the census finds the suite's tests");
  assert.deepEqual(offenders, [],"these tests run no expect() at their own nesting level: an assertion behind an if, try, catch, "
    + "loop, callback, &&, || or ?: (braces or not) is skipped exactly when the feature under test is missing, so the test cannot fail. "
    + "Drive the real control and assert unconditionally, or delete the probe and let the missing control fail the test.");
});
