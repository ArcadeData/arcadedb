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
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */

// Regression test for issue #7804, second of the three questions #7372 left open: what Studio shows
// when the server refuses to mint an API token over an unprotected transport.
//
// The refusal is a 412 whose body explains the transport precondition and names the server setting.
// Studio used to hand that body to globalNotifyError(), which renders json.error as the toast TITLE
// and "Error on execution of the command" as the body - so the one sentence that tells the operator
// what to do ended up as a title, with a generic message underneath, and nothing said that Studio's
// own connection is the one being refused.
//
// apiTokenTransportRefusal() maps that one status onto a title and a message; everything else returns
// null so the existing globalNotifyError() path is untouched. Run with:
//
//     node --test studio/test/api-token-transport-refusal.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const SRC_PATH = path.join(__dirname, "..", "src", "main", "resources", "static", "js", "studio-security.js");
const src = fs.readFileSync(SRC_PATH, "utf8");

function extractFn(name) {
  const start = src.indexOf("function " + name + "(");
  if (start < 0) throw new Error("function not found in studio-security.js: " + name);
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

eval(extractFn("apiTokenTransportRefusal"));

// The body PostApiTokenHandler.checkTransport() writes for a 412.
const SERVER_REFUSAL = JSON.stringify({
  error:
    "API tokens can only be minted over HTTPS or from a loopback client. Connect over TLS, or set " +
    "arcadedb.server.apiTokenRequireSecureTransport=false to allow it",
});

test("a 412 is turned into the server's own explanation, not a generic failure", () => {
  const refusal = apiTokenTransportRefusal({ status: 412, responseText: SERVER_REFUSAL });

  assert.ok(refusal, "a 412 must be recognised as the transport refusal");
  assert.ok(refusal.title.length > 0, "the toast needs a title of its own");
  assert.notEqual(refusal.message, "Error on execution of the command", "the generic body is what this replaces");
  assert.ok(
    refusal.message.includes("arcadedb.server.apiTokenRequireSecureTransport"),
    "the operator has to be told which setting produced the refusal"
  );
});

test("the message says it is Studio's own connection that is being refused", () => {
  const refusal = apiTokenTransportRefusal({ status: 412, responseText: SERVER_REFUSAL });

  assert.match(
    refusal.message,
    /HTTPS/i,
    "the actionable half is that this Studio has to be reached over HTTPS (or from the server host)"
  );
});

test("a 412 with an unparseable body still explains the transport precondition", () => {
  // A proxy, not the server, can answer 412 with an HTML error page. Studio must not render 'undefined'.
  const refusal = apiTokenTransportRefusal({ status: 412, responseText: "<html>Precondition Failed</html>" });

  assert.ok(refusal, "the status alone identifies the refusal");
  assert.ok(refusal.message.length > 0);
  assert.ok(!refusal.message.includes("undefined"), "a body that is not JSON must not leak 'undefined' into the toast");
  assert.ok(!refusal.message.includes("<html>"), "an HTML error page is not an explanation to paste into a toast");
});

test("a 412 with a JSON body that has no 'error' field does not leak 'undefined'", () => {
  const refusal = apiTokenTransportRefusal({ status: 412, responseText: JSON.stringify({ detail: "nope" }) });

  assert.ok(refusal);
  assert.ok(!refusal.message.includes("undefined"));
});

test("every other status is left to the existing error handler", () => {
  // Returning null is what keeps 409 (duplicate name), 400 (bad permissions) and 403 rendering exactly
  // as they did before this change.
  for (const status of [400, 401, 403, 404, 409, 500, 0]) {
    assert.equal(apiTokenTransportRefusal({ status: status, responseText: SERVER_REFUSAL }), null, "status " + status);
  }
});

test("a missing or malformed jqXHR is not a refusal", () => {
  assert.equal(apiTokenTransportRefusal(null), null);
  assert.equal(apiTokenTransportRefusal(undefined), null);
  assert.equal(apiTokenTransportRefusal({}), null);
});

test("the mint call site actually routes 412 through this function", () => {
  // The function is only worth anything if the POST .fail handler calls it: a helper nothing invokes is
  // the same bug as no helper at all.
  const mintCall = src.indexOf('url: "api/v1/server/api-tokens"', src.indexOf("function createApiToken"));
  assert.ok(mintCall > 0, "the mint POST must still live in createApiToken()");

  const failHandler = src.slice(mintCall, src.indexOf("}", src.indexOf(".fail(function", mintCall)));
  assert.ok(
    failHandler.includes("apiTokenTransportRefusal"),
    "the mint .fail handler must consult apiTokenTransportRefusal() before falling back to globalNotifyError"
  );
});
