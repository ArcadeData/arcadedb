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

// Every endpoint whose path template carries a `{database}` segment. The list is NOT maintained by hand: it is
// read off the server's own route declarations, so a route that gains a `{database}` segment is guarded the
// moment it is declared. #6830 fixed the `command`/`query` sites, #6947 found that `ts` - a prefix nobody had
// listed - was still concatenating the name raw in studio-timeseries.js, and #10000 found that `vector`,
// `cluster/accept-copy` and `cluster/accept-diverged` had gone unlisted the same way, which a hand-kept list
// can only ever repeat. The two sources scanned are exhaustive by construction:
//
//   - HttpServer.setupRoutes(): the routes the server itself registers, `.post("/vector/{database}/search", ...)`.
//   - the openapi/*ApiSpec.java path literals, `"/api/v1/cluster/accept-copy/{database}"`: the plugin routes
//     (RaftHAPlugin.registerAPI() registers them with addPrefixPath, so the `{database}` never appears in the
//     registration) are declared there because the plugin module cannot declare its own OpenAPI paths, and
//     RaftHAPluginRegisteredRoutesMatchApiSpecTest fails the build when registerAPI() and that list disagree.
//
// Scanning source text breaks silently if a route is ever declared in a shape the scanner does not read, which
// would turn this guard into the vacuous test it replaced. So every non-comment line that mentions
// `{database}` must be accounted for (declaredDatabaseSegmentEndpoints().unparsed) or the test below fails.
const SERVER_JAVA_DIR = path.join(__dirname, "..", "..", "server", "src", "main", "java", "com", "arcadedb", "server", "http");
const HTTP_SERVER_SRC = path.join(SERVER_JAVA_DIR, "HttpServer.java");
const API_SPEC_DIR = path.join(SERVER_JAVA_DIR, "handler", "openapi");

// `.post("/vector/{database}/search", ...)`, `.get("/ts/{database}/prom/api/v1/query", ...)`, `.add("HEAD", "/x/{database}", ...)`
const ROUTE_REGISTRATION_RE = /\.(?:get|post|put|delete|add)\(\s*(?:"[A-Z]+",\s*)?"\/([a-z][a-z0-9\/-]*)\/\{database\}/;
// `"/api/v1/cluster/accept-copy/{database}"`, `"/api/v1/ha/snapshot/{database}/checksums"`
const SPEC_PATH_RE = /"\/api\/v1\/([a-z][a-z0-9\/-]*)\/\{database\}/g;

// Reads the route declarations out of the given Java sources. `unparsed` collects every code line that names a
// `{database}` segment in a shape neither pattern recognises, so the scanner cannot go blind without saying so.
function declaredDatabaseSegmentEndpoints(httpServerSrc, specSrcs) {
  const endpoints = new Set();
  const unparsed = [];
  httpServerSrc.split("\n").forEach((line, i) => {
    if (isComment(line) || !line.includes("{database}")) return;
    const m = ROUTE_REGISTRATION_RE.exec(line);
    if (m) endpoints.add(m[1]);
    else unparsed.push("HttpServer.java:" + (i + 1) + " -> " + line.trim());
  });
  for (const [file, src] of Object.entries(specSrcs))
    src.split("\n").forEach((line, i) => {
      if (isComment(line) || !line.includes("{database}")) return;
      let found = false;
      for (const m of line.matchAll(SPEC_PATH_RE)) {
        endpoints.add(m[1]);
        found = true;
      }
      // Javadoc and prose in a spec's description text (`through /api/v1/command/{database}.`) mention routes
      // without declaring one; only a quoted path literal can, and one of those that does not parse is reported.
      if (!found && /"\/api\/v1\/[^"]*\{database\}/.test(line)) unparsed.push(file + ":" + (i + 1) + " -> " + line.trim());
    });
  return { endpoints: [...endpoints].sort(), unparsed };
}

function readServerRouteDeclarations() {
  const specSrcs = {};
  for (const f of fs.readdirSync(API_SPEC_DIR).filter((n) => n.endsWith("ApiSpec.java")).sort())
    specSrcs[f] = fs.readFileSync(path.join(API_SPEC_DIR, f), "utf8");
  return declaredDatabaseSegmentEndpoints(fs.readFileSync(HTTP_SERVER_SRC, "utf8"), specSrcs);
}

const DATABASE_SEGMENT_ENDPOINTS = readServerRouteDeclarations().endpoints;
// The trailing slash is required: it is what keeps a hypothetical `api/v1/tsdb/` from matching `ts`.
const DATABASE_SEGMENT_RE = new RegExp("api/v1/(?:" + DATABASE_SEGMENT_ENDPOINTS.join("|") + ")/");

// A line that only talks about an endpoint - a comment naming `POST /api/v1/cluster/verify/{db}` - builds
// no URL, so it cannot carry the defect and must not be reported as one.
function isComment(line) {
  const t = line.trim();
  return t.startsWith("//") || t.startsWith("*") || t.startsWith("/*");
}

// The API tab's endpoint reference spells each route as a template, `/api/v1/query/{database}`: a placeholder the
// playground fills with encodeURIComponent, not a name concatenated into a URL. Only a segment that is NOT the literal
// `{database}` placeholder builds a URL from a name.
function buildsDatabaseSegment(line) {
  for (const m of line.matchAll(new RegExp(DATABASE_SEGMENT_RE.source, "g")))
    if (!line.startsWith("{database}", m.index + m[0].length)) return true;
  return false;
}

// The lines of one Studio source that build a `{database}` path segment without the helper.
function offendersIn(file, src) {
  const offenders = [];
  src.split("\n").forEach((line, i) => {
    if (buildsDatabaseSegment(line) && !isComment(line) && !line.includes("encodeDatabaseName"))
      offenders.push(file + ":" + (i + 1) + " -> " + line.trim());
  });
  return offenders;
}

test("the endpoint list is read off the server's route declarations, not kept by hand (#10000)", () => {
  const { endpoints, unparsed } = readServerRouteDeclarations();

  // A route declared in a shape the scanner cannot read would silently shrink the guard back to a stale list.
  assert.deepEqual(unparsed, [], "a route names {database} in a shape declaredDatabaseSegmentEndpoints() does not read");

  // The three prefixes #10000 found missing, plus one from each source, so the scanner reading nothing - a renamed
  // file, a reworded registration - fails here instead of passing the call-site test below against an empty list.
  for (const expected of ["vector", "cluster/accept-copy", "cluster/accept-diverged", "command", "query", "ts", "cluster/resync",
    "cluster/verify", "ha/snapshot"])
    assert.ok(endpoints.includes(expected), "the scanner no longer finds the `" + expected + "` route; found: " + endpoints.join(", "));
});

test("a route gaining a {database} segment is guarded without anyone editing a list (#10000)", () => {
  // A route nobody has heard of, declared the way HttpServer declares its own, and a plugin route declared the way
  // PluginApiSpec declares the ones RaftHAPlugin registers.
  const httpServer = '        .post("/brandnew/{database}/search", new Handler(this))\n        .get("/other", new Other(this))\n';
  const spec = '    openAPI.getPaths().addPathItem("/api/v1/cluster/brand-new-action/{database}", createPath());\n';
  const { endpoints, unparsed } = declaredDatabaseSegmentEndpoints(httpServer, { "PluginApiSpec.java": spec });
  assert.deepEqual(unparsed, []);
  assert.deepEqual(endpoints, ["brandnew", "cluster/brand-new-action"]);

  // A declaration in a shape the scanner does not read is reported rather than skipped.
  const odd = declaredDatabaseSegmentEndpoints('        .post(PREFIX + "/{database}/x", new Handler(this))\n', {});
  assert.equal(odd.unparsed.length, 1);

  // Prose that merely mentions a route is not a declaration.
  const prose = declaredDatabaseSegmentEndpoints("", { "Spec.java": '    through /api/v1/command/{database}.""");\n     * POST /api/v1/x/{database}\n' });
  assert.deepEqual(prose, { endpoints: [], unparsed: [] });
});

test("the call sites #10000 found unguarded are caught: vector, cluster/accept-copy, cluster/accept-diverged", () => {
  // The mutation the issue reports: studio-vector.js concatenating the raw database name. Before the list was read
  // off the server this produced no offender, because `vector` was not on it.
  assert.equal(offendersIn("studio-vector.js", 'url: "api/v1/vector/" + database + "/" + request.path,').length, 1);
  assert.equal(offendersIn("x.js", 'url: "api/v1/cluster/accept-copy/" + database,').length, 1);
  assert.equal(offendersIn("x.js", "url: `api/v1/cluster/accept-diverged/${database}`,").length, 1);

  // The correct form, as the real call sites write it, is not reported.
  assert.deepEqual(offendersIn("studio-vector.js", 'url: "api/v1/vector/" + encodeDatabaseName(database) + "/" + request.path,'), []);
  assert.deepEqual(offendersIn("x.js", 'url: "api/v1/cluster/accept-copy/" + encodeDatabaseName(database),'), []);
});

test("every api/v1 URL in the Studio builds its database segment with encodeDatabaseName", () => {
  // Match the endpoint prefix in ANY form - string concatenation, a template literal, a parenthesized
  // expression - and require the helper on the same line, rather than matching only the `"..." + identifier`
  // shape that happens to be written today. The point of this test is the call sites nobody has written yet.
  const offenders = [];
  for (const file of fs.readdirSync(JS_DIR).filter((f) => f.endsWith(".js")))
    offenders.push(...offendersIn(file, fs.readFileSync(path.join(JS_DIR, file), "utf8")));
  assert.deepEqual(offenders, [], "these call sites build the URL path segment without encodeDatabaseName");
});

test("the time series URLs keep a metacharacter-carrying name inside one path segment", () => {
  // #6947: `"api/v1/ts/" + db + "/query"` sent `api/v1/ts/a#b/query` for a database named `a#b`, which the
  // browser truncates at the fragment, and `a/b` invented a path segment the route cannot match. The server
  // reads `{database}` as a single decoded segment, so the name has to be percent-encoded going in.
  assert.equal("api/v1/ts/" + encodeDatabaseName("a#b") + "/query", "api/v1/ts/a%23b/query");
  assert.equal("api/v1/ts/" + encodeDatabaseName("a b") + "/latest", "api/v1/ts/a%20b/latest");
  assert.equal("api/v1/ts/" + encodeDatabaseName("a/b") + "/prom/api/v1/query_range", "api/v1/ts/a%2Fb/prom/api/v1/query_range");

  // The raw concatenation the fix replaced produced a different resource for each of those names.
  assert.notEqual("api/v1/ts/" + "a#b" + "/query", "api/v1/ts/" + encodeDatabaseName("a#b") + "/query");
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
