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

// Regression test for issue #6830. The graph widget ran the database name through escapeHtml() before
// concatenating it into an `api/v1/command/` URL. escapeHtml() is an HTML-entity encoder, not a URL
// encoder: it turns `a&b` into the different database name `a&amp;b`, which the server answers with a
// 404 - so "add node from record" failed for such a database while every other Studio panel worked.
// Several other call sites had the mirror-image problem, concatenating the name raw so a `/`, `#` or a
// space changed the shape of the URL.
//
// Both halves are checked here: encodeDatabaseName() itself, and the invariant that no api/v1 URL in
// the Studio builds its path segment any other way. Run with:
//
//     node --test studio/test/database-name-url-encoding.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const JS_DIR = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const utilsSrc = fs.readFileSync(path.join(JS_DIR, "studio-utils.js"), "utf8");

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
  if (depth !== 0) throw new Error("unbalanced braces while extracting " + name);
  return src.substring(start, i);
}

eval(extractFn(utilsSrc, "encodeDatabaseName"));
eval(extractFn(utilsSrc, "escapeHtml"));

test("a name carrying URL metacharacters survives as one path segment", () => {
  // The reported case: escapeHtml() produced `a&amp;b`, a different database name.
  assert.equal(encodeDatabaseName("a&b"), "a%26b");
  assert.notEqual(encodeDatabaseName("a&b"), escapeHtml("a&b"));

  // The mirror-image problem at the call sites that concatenated the name raw.
  assert.equal(encodeDatabaseName("a/b"), "a%2Fb");
  assert.equal(encodeDatabaseName("a#b"), "a%23b");
  assert.equal(encodeDatabaseName("a b"), "a%20b");
  assert.equal(encodeDatabaseName("a?b"), "a%3Fb");
  assert.equal(encodeDatabaseName("a'b"), "a'b");
  assert.equal(encodeDatabaseName('a"b'), "a%22b");
});

test("an ordinary name is untouched, so no existing URL changes", () => {
  assert.equal(encodeDatabaseName("mydb"), "mydb");
  assert.equal(encodeDatabaseName("Beer-Db_2024.1"), "Beer-Db_2024.1");
});

test("a missing name yields an empty segment rather than the string 'null'", () => {
  // getCurrentDatabase() returns null when nothing is selected; the request must fail as a malformed
  // URL instead of quietly addressing a database literally named "null".
  assert.equal(encodeDatabaseName(null), "");
  assert.equal(encodeDatabaseName(undefined), "");
});

test("every api/v1 URL in the Studio builds its database segment with encodeDatabaseName", () => {
  // Match the endpoint prefix in ANY form - string concatenation, a template literal, a parenthesized
  // expression - and require the helper on the same line, rather than matching only the `"..." + identifier`
  // shape that happens to be written today. The point of this test is the call sites nobody has written yet.
  const offenders = [];
  for (const file of fs.readdirSync(JS_DIR).filter((f) => f.endsWith(".js"))) {
    const src = fs.readFileSync(path.join(JS_DIR, file), "utf8");
    src.split("\n").forEach((line, i) => {
      if (/api\/v1\/(?:command|query|progress)\//.test(line) && !line.includes("encodeDatabaseName"))
        offenders.push(file + ":" + (i + 1) + " -> " + line.trim());
    });
  }
  assert.deepEqual(offenders, [], "these call sites build the URL path segment without encodeDatabaseName");
});

test("no admin command is sent as a hand-built JSON string", () => {
  // A database name may contain a single quote - the server rejects only / \\ NUL and .. - and the
  // `create/drop database` payloads used to be assembled as "{ 'command': '... " + name + "' }". Such a name
  // made the body malformed, so the database could not be created, dropped or reset at all. JSON.stringify
  // escapes it; nothing else here needs to, since the server reads the name as the verbatim rest of the command.
  const offenders = [];
  for (const file of fs.readdirSync(JS_DIR).filter((f) => f.endsWith(".js"))) {
    const src = fs.readFileSync(path.join(JS_DIR, file), "utf8");
    src.split("\n").forEach((line, i) => {
      if (/data:\s*"\{/.test(line)) offenders.push(file + ":" + (i + 1) + " -> " + line.trim());
    });
  }
  assert.deepEqual(offenders, [], "build the request body with JSON.stringify, not by concatenating JSON text");

  // And the payload a quote-carrying name produces is valid JSON that round-trips to the original name.
  const body = JSON.stringify({ command: "drop database " + "bob's db" });
  assert.equal(JSON.parse(body).command, "drop database bob's db");
});

test("a database name is never URL-encoded on its way into a command payload", () => {
  // The mirror image of the reported bug: createDatabase() ran the typed name through encodeURI(), so a name
  // typed as `my db` created a database actually named `my%20db`.
  const dbSrc = fs.readFileSync(path.join(JS_DIR, "studio-database.js"), "utf8");
  // Match an assignment rather than the bare call, so the comment explaining this rule is not itself a hit.
  assert.ok(
    !/=\s*encodeURI\((?!Component)/.test(dbSrc),
    "encodeURI() encodes for a URL, not for a command payload - it changes the name that reaches the server"
  );
});

test("no database name is HTML-escaped on its way into a URL or a SQL command", () => {
  const offenders = [];
  for (const file of fs.readdirSync(JS_DIR).filter((f) => f.endsWith(".js"))) {
    const src = fs.readFileSync(path.join(JS_DIR, file), "utf8");
    src.split("\n").forEach((line, i) => {
      if (line.includes("escapeHtml(getCurrentDatabase())")) offenders.push(file + ":" + (i + 1) + " -> " + line.trim());
    });
  }
  assert.deepEqual(offenders, [], "hold the raw name and escape at the HTML sink, not at the source");
});
