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

// Regression test for issue #5607: the "Add Index" dialog emitted `CREATE INDEX ... LSM_VECTOR`
// with no METADATA clause, which the engine refuses ("requires a METADATA clause with at least
// 'dimensions'"), and the dialog had no input the four named settings could have come from.
//
// The SQL assembly lives in the pure buildCreateIndexCommand()/validateCreateIndexOptions() pair so
// it can be exercised without a DOM. The engine side of the same contract is pinned by
// engine/src/test/java/com/arcadedb/index/vector/Issue5607VectorIndexMetadataTest.java. Run with:
//
//     node --test studio/test/create-index-command.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const JS_DIR = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const dbSrc = fs.readFileSync(path.join(JS_DIR, "studio-database.js"), "utf8");
const utilsSrc = fs.readFileSync(path.join(JS_DIR, "studio-utils.js"), "utf8");

// Pulls one top-level `function name(...) {...}` out of a Studio source file.
//
// The brace matcher counts every { and } it sees, including any inside a string literal, a regular
// expression or a comment, so a function extracted this way must not contain one. That constraint is
// checked rather than assumed: the extracted text is parsed here, so violating it fails with a message
// naming the function instead of a bare SyntaxError from the eval below.
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

eval(extractFn(utilsSrc, "quoteSqlName"));
eval(extractFn(utilsSrc, "escapeHtml"));
eval(extractFn(dbSrc, "buildCreateIndexCommand"));
eval(extractFn(dbSrc, "validateCreateIndexOptions"));

// --- minimal browser stand-ins, so createIndex() itself can be driven end to end -----------------
// They shadow the globals the extracted function closes over; nothing here touches a real DOM.

let dialog = null; // { title, html, callback } captured from globalPrompt
let sentCommand = null; // the SQL that reached jQuery.ajax
let notified = null; // the last globalNotify(..., "danger") message
let inputs = {}; // selector -> value, driven per test

const setTimeout = function () {}; // createIndex() defers its visibility wiring; irrelevant here
const document = {
  getElementById: function (id) {
    return Object.prototype.hasOwnProperty.call(inputs, "#" + id) ? { id: id } : null;
  }
};
const $ = function (selector) {
  return {
    val: function () {
      return inputs[selector];
    },
    prop: function (name) {
      return inputs[selector + ":" + name] === true;
    },
    on: function () {}
  };
};
const chainable = { done: function () { return chainable; }, fail: function () { return chainable; } };
const jQuery = {
  ajax: function (options) {
    sentCommand = JSON.parse(options.data).command;
    return chainable;
  }
};
const globalCredentials = "Bearer test";
const globalNotify = function (title, message, level) {
  if (level === "danger") notified = message;
};
const globalNotifyError = function () {};
const refreshSchemaAndShowType = function () {};
const getCurrentDatabase = function () {
  return "testdb";
};
let typeProperties = [];
const collectTypeProperties = function () {
  return typeProperties;
};
const globalPrompt = function (title, html, buttonLabel, callback) {
  dialog = { title: title, html: html, callback: callback };
};

eval(extractFn(dbSrc, "createIndex"));

/** Opens the dialog for a type exposing the given properties and returns the captured HTML. */
function openDialog(properties) {
  typeProperties = properties;
  dialog = null;
  sentCommand = null;
  notified = null;
  createIndex("Doc");
  return dialog.html;
}

/** Submits the open dialog with the given input values and returns the SQL that was sent, if any. */
function submitDialog(values) {
  inputs = values;
  sentCommand = null;
  notified = null;
  dialog.callback();
  return sentCommand;
}

test("LSM_VECTOR emits the METADATA clause the engine requires", () => {
  const command = buildCreateIndexCommand({
    typeName: "Doc",
    algorithm: "LSM_VECTOR",
    properties: ["embedding"],
    metadata: { dimensions: 384, similarity: "COSINE", maxConnections: 32, beamWidth: 100 }
  });

  assert.equal(
    command,
    "CREATE INDEX ON `Doc` (`embedding`) LSM_VECTOR METADATA " +
      '{"dimensions":384,"similarity":"COSINE","maxConnections":32,"beamWidth":100}'
  );
  // The bug: a METADATA-less LSM_VECTOR statement must never leave the dialog.
  assert.ok(command.includes("METADATA"), "LSM_VECTOR statement must carry a METADATA clause");
  assert.ok(command.includes('"dimensions"'), "METADATA must carry 'dimensions'");
});

test("LSM_VECTOR carries the optional quantization when the user picks one", () => {
  const command = buildCreateIndexCommand({
    typeName: "Doc",
    algorithm: "LSM_VECTOR",
    properties: ["embedding"],
    metadata: { dimensions: 8, similarity: "EUCLIDEAN", maxConnections: 16, beamWidth: 50, quantization: "INT8" }
  });

  assert.ok(command.includes('"quantization":"INT8"'));
});

test("LSM_VECTOR without dimensions is rejected client-side instead of failing on the server", () => {
  const base = { typeName: "Doc", algorithm: "LSM_VECTOR", properties: ["embedding"] };

  assert.match(validateCreateIndexOptions(Object.assign({}, base, { metadata: null })), /[Dd]imensions/);
  assert.match(validateCreateIndexOptions(Object.assign({}, base, { metadata: {} })), /[Dd]imensions/);
  assert.match(
    validateCreateIndexOptions(Object.assign({}, base, { metadata: { dimensions: 0 } })),
    /[Dd]imensions/
  );
  assert.equal(validateCreateIndexOptions(Object.assign({}, base, { metadata: { dimensions: 384 } })), null);
});

test("LSM_VECTOR accepts exactly one property", () => {
  assert.match(
    validateCreateIndexOptions({
      typeName: "Doc",
      algorithm: "LSM_VECTOR",
      properties: ["a", "b"],
      metadata: { dimensions: 4 }
    }),
    /one property/
  );

  // The algorithm's own rule wins over the generic "at least one property is required".
  assert.match(
    validateCreateIndexOptions({ typeName: "Doc", algorithm: "LSM_VECTOR", properties: [], metadata: { dimensions: 4 } }),
    /one property/
  );
  assert.match(
    validateCreateIndexOptions({ typeName: "Doc", algorithm: "LSM_SPARSE_VECTOR", properties: [], metadata: {} }),
    /indices property and a weights property/
  );
});

test("LSM_SPARSE_VECTOR keeps emitting its optional metadata and requires both properties", () => {
  const command = buildCreateIndexCommand({
    typeName: "Doc",
    algorithm: "LSM_SPARSE_VECTOR",
    properties: ["dims", "weights"],
    metadata: { dimensions: 105000, modifier: "IDF", weightQuantization: "FP16" }
  });

  assert.equal(
    command,
    "CREATE INDEX ON `Doc` (`dims`, `weights`) LSM_SPARSE_VECTOR METADATA " +
      '{"dimensions":105000,"modifier":"IDF","weightQuantization":"FP16"}'
  );

  // Every sparse setting has a server-side default, so an empty metadata must drop the clause entirely.
  assert.equal(
    buildCreateIndexCommand({
      typeName: "Doc",
      algorithm: "LSM_SPARSE_VECTOR",
      properties: ["dims", "weights"],
      metadata: {}
    }),
    "CREATE INDEX ON `Doc` (`dims`, `weights`) LSM_SPARSE_VECTOR"
  );

  assert.match(
    validateCreateIndexOptions({ typeName: "Doc", algorithm: "LSM_SPARSE_VECTOR", properties: ["dims"], metadata: {} }),
    /indices property and a weights property/
  );
});

test("the non-vector algorithms are unchanged", () => {
  assert.equal(
    buildCreateIndexCommand({
      typeName: "Doc",
      algorithm: "LSM_TREE",
      properties: ["name", "age"],
      unique: true,
      nullStrategy: "SKIP",
      ifNotExists: true
    }),
    "CREATE INDEX IF NOT EXISTS ON `Doc` (`name`, `age`) UNIQUE NULL_STRATEGY SKIP"
  );

  assert.equal(
    buildCreateIndexCommand({ typeName: "Doc", algorithm: "LSM_TREE", properties: ["name"] }),
    "CREATE INDEX ON `Doc` (`name`) NOTUNIQUE"
  );

  assert.equal(
    buildCreateIndexCommand({ typeName: "Doc", algorithm: "HASH", properties: ["name"], unique: true }),
    "CREATE INDEX ON `Doc` (`name`) UNIQUE_HASH"
  );

  assert.equal(
    buildCreateIndexCommand({ typeName: "Doc", algorithm: "HASH", properties: ["name"], nullStrategy: "INDEX" }),
    "CREATE INDEX ON `Doc` (`name`) NOTUNIQUE_HASH NULL_STRATEGY INDEX"
  );

  assert.equal(
    buildCreateIndexCommand({ typeName: "Doc", algorithm: "FULL_TEXT", properties: ["text"] }),
    "CREATE INDEX ON `Doc` (`text`) FULL_TEXT"
  );

  // NULL_STRATEGY is only valid on LSM_TREE / HASH: it must not leak onto the other algorithms.
  assert.equal(
    buildCreateIndexCommand({ typeName: "Doc", algorithm: "FULL_TEXT", properties: ["text"], nullStrategy: "SKIP" }),
    "CREATE INDEX ON `Doc` (`text`) FULL_TEXT"
  );

  assert.match(validateCreateIndexOptions({ typeName: "Doc", algorithm: "LSM_TREE", properties: [] }), /property/);
});

test("type and property names are quoted, so reserved words and odd characters survive", () => {
  assert.equal(
    buildCreateIndexCommand({ typeName: "Order", algorithm: "LSM_TREE", properties: ["from"] }),
    "CREATE INDEX ON `Order` (`from`) NOTUNIQUE"
  );
});

// --- the dialog itself ---------------------------------------------------------------------------

test("the Add Index dialog exposes an input for every mandatory vector setting", () => {
  const html = openDialog([{ name: "embedding", type: "ARRAY_OF_FLOATS" }]);

  // The bug: LSM_VECTOR was offered in the algorithm list with nowhere to enter its settings.
  assert.ok(html.includes("value='LSM_VECTOR'"), "LSM_VECTOR must be offered");
  assert.ok(html.includes("id='inputCreateIdxVectorDimensions'"), "dimensions input is missing");
  assert.ok(html.includes("id='inputCreateIdxVectorSimilarity'"), "similarity input is missing");
  assert.ok(html.includes("id='inputCreateIdxVectorMaxConnections'"), "maxConnections input is missing");
  assert.ok(html.includes("id='inputCreateIdxVectorBeamWidth'"), "beamWidth input is missing");

  // Balanced markup: an unclosed <div> would silently swallow the rest of the dialog.
  assert.equal((html.match(/<div/g) || []).length, (html.match(/<\/div>/g) || []).length, "unbalanced <div> in the dialog");
});

test("submitting the LSM_VECTOR branch sends a statement carrying METADATA", () => {
  openDialog([{ name: "embedding", type: "ARRAY_OF_FLOATS" }]);

  const command = submitDialog({
    "#inputCreateIdxAlgorithm": "LSM_VECTOR",
    "#inputCreateIdxPropsVector": "embedding",
    "#inputCreateIdxVectorDimensions": "384",
    "#inputCreateIdxVectorSimilarity": "COSINE",
    "#inputCreateIdxVectorMaxConnections": "32",
    "#inputCreateIdxVectorBeamWidth": "100",
    "#inputCreateIdxVectorQuantization": ""
  });

  assert.equal(
    command,
    "CREATE INDEX ON `Doc` (`embedding`) LSM_VECTOR METADATA " +
      '{"dimensions":384,"similarity":"COSINE","maxConnections":32,"beamWidth":100}'
  );
});

test("submitting the LSM_VECTOR branch with no dimensions never reaches the server", () => {
  openDialog([{ name: "embedding", type: "ARRAY_OF_FLOATS" }]);

  const command = submitDialog({
    "#inputCreateIdxAlgorithm": "LSM_VECTOR",
    "#inputCreateIdxPropsVector": "embedding",
    "#inputCreateIdxVectorDimensions": "",
    "#inputCreateIdxVectorSimilarity": "COSINE",
    "#inputCreateIdxVectorMaxConnections": "32",
    "#inputCreateIdxVectorBeamWidth": "100",
    "#inputCreateIdxVectorQuantization": ""
  });

  assert.equal(command, null, "no statement must be sent");
  assert.match(notified, /[Dd]imensions/);
});

test("submitting the LSM_TREE branch is unchanged, free-text property fallback included", () => {
  // No properties collected for the type: the dialog falls back to the comma-separated text input.
  openDialog([]);

  const command = submitDialog({
    "#inputCreateIdxAlgorithm": "LSM_TREE",
    "#inputCreateIdxPropsText": " name , age ",
    "#inputCreateIdxNullStrategy": "SKIP",
    "#inputCreateIdxUnique:checked": true,
    "#inputCreateIdxIfNotExists:checked": true
  });

  assert.equal(command, "CREATE INDEX IF NOT EXISTS ON `Doc` (`name`, `age`) UNIQUE NULL_STRATEGY SKIP");
});

test("submitting the LSM_SPARSE_VECTOR branch carries its optional metadata", () => {
  openDialog([
    { name: "dims", type: "ARRAY_OF_INTEGERS" },
    { name: "weights", type: "ARRAY_OF_FLOATS" }
  ]);

  const command = submitDialog({
    "#inputCreateIdxAlgorithm": "LSM_SPARSE_VECTOR",
    "#inputCreateIdxPropsSparseIdx": "dims",
    "#inputCreateIdxPropsSparseWeights": "weights",
    "#inputCreateIdxSparseDimensions": "105000",
    "#inputCreateIdxSparseModifier": "IDF",
    "#inputCreateIdxSparseWeightQuantization": ""
  });

  assert.equal(
    command,
    "CREATE INDEX ON `Doc` (`dims`, `weights`) LSM_SPARSE_VECTOR METADATA " + '{"dimensions":105000,"modifier":"IDF"}'
  );
});
