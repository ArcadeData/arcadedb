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

// Issue #7312: Studio's Vector tab, a front end for POST /api/v1/vector/{database}/search, /hybrid and /fulltext.
//
// The request assembly, the pre-flight checks and the response description are pure functions in studio-vector.js.
// The file is loaded whole into a VM context, so the tests see exactly what the browser loads; the one top-level
// statement that touches jQuery (the databaseChanged listener) meets a stub. The browser path is covered by
// e2e-studio/tests/vector-search.spec.ts. Run with:
//
//     node --test studio/test/vector-search-panel.test.js

const { test } = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const JS_DIR = path.join(__dirname, "..", "src", "main", "resources", "static", "js");
const vectorSrc = fs.readFileSync(path.join(JS_DIR, "studio-vector.js"), "utf8");

const jqueryStub = function () {
  return { on: function () {} };
};
const ctx = vm.createContext({ $: jqueryStub, jQuery: jqueryStub, document: {} });
vm.runInContext(vectorSrc, ctx);

// Assertions compare against plain objects: a value built inside the VM context has that context's Object prototype,
// which deepStrictEqual would otherwise report as a difference.
const plain = (v) => JSON.parse(JSON.stringify(v));

// The shape GetOpenApiHandler serves for the three request schemas (see VectorApiSpec), trimmed to what the panel
// reads. The numbers are deliberately NOT the production constants: the panel must use whatever the document says.
const SPEC = {
  components: {
    schemas: {
      VectorSearchRequest: {
        properties: {
          k: { type: "integer", minimum: 1, maximum: 50, default: 7 },
          efSearch: { type: "integer", minimum: 1, maximum: 400 }
        }
      },
      HybridSearchRequest: {
        properties: {
          k: { type: "integer", minimum: 1, maximum: 60, default: 7 },
          efSearch: { type: "integer", minimum: 1, maximum: 400 },
          fusionStrategy: { type: "string", enum: ["RRF", "DBSF", "LINEAR"] },
          expand: {
            type: "object",
            properties: {
              direction: { type: "string", enum: ["out", "in", "both"] },
              maxDepth: { type: "integer", minimum: 1, maximum: 3, default: 1 }
            }
          }
        }
      },
      FullTextSearchRequest: {
        properties: { limit: { type: "integer", minimum: 1, maximum: 90, default: 10 } }
      }
    }
  }
};

// Rows of `SELECT FROM schema:types`, carrying the dimensions/scoring the engine now lists for vector indexes.
const TYPES = [
  {
    name: "Doc",
    indexes: [
      { name: "Doc[embedding]", typeName: "Doc", type: "LSM_VECTOR", properties: ["embedding"], dimensions: 3, scoring: "distance_lower_is_better:COSINE" },
      { name: "Doc[body]", typeName: "Doc", type: "FULL_TEXT", properties: ["body"] },
      { name: "Doc[title]", typeName: "Doc", type: "LSM_TREE", properties: ["title"] }
    ]
  },
  {
    name: "Sparse",
    indexes: [
      { name: "Sparse[dims,weights]", typeName: "Sparse", type: "LSM_SPARSE_VECTOR", properties: ["dims", "weights"], dimensions: 8, scoring: "score_higher_is_better:dot_product" }
    ]
  }
];

const INDEXES = ctx.vecSearchableIndexes(TYPES);
const BOUNDS = ctx.vecBoundsFromOpenApi(SPEC);

test("the index list keeps only searchable indexes, with the dimensions and scoring schema:types reports", () => {
  assert.deepEqual(plain(INDEXES.vector.map((i) => [i.name, i.sparse, i.dimensions, i.scoring])), [
    ["Doc[embedding]", false, 3, "distance_lower_is_better:COSINE"],
    ["Sparse[dims,weights]", true, 8, "score_higher_is_better:dot_product"]
  ]);
  assert.deepEqual(plain(INDEXES.fulltext.map((i) => i.name)), ["Doc[body]"]);
});

test("the bounds are read from the OpenAPI document, not restated", () => {
  assert.deepEqual(plain(BOUNDS.search.k), { min: 1, max: 50, def: 7 });
  assert.deepEqual(plain(BOUNDS.search.efSearch), { min: 1, max: 400, def: null });
  assert.deepEqual(plain(BOUNDS.hybrid.maxDepth), { min: 1, max: 3, def: 1 });
  assert.deepEqual(plain(BOUNDS.hybrid.fusionStrategies), ["RRF", "DBSF", "LINEAR"]);
  assert.deepEqual(plain(BOUNDS.hybrid.directions), ["out", "in", "both"]);
  assert.deepEqual(plain(BOUNDS.fulltext.limit), { min: 1, max: 90, def: 10 });
});

test("a document without the vector schemas yields no bounds rather than guessed ones", () => {
  const none = ctx.vecBoundsFromOpenApi({ components: { schemas: {} } });
  assert.equal(none.search.k, null);
  assert.equal(none.hybrid.maxDepth, null);
  assert.deepEqual(plain(none.hybrid.fusionStrategies), []);
  // ...and then the form checks nothing: a k far beyond any real cap goes through for the server to judge
  const r = ctx.vecBuildRequest("search", { indexName: "Doc[embedding]", queryVector: "1,0,0", k: "999999" }, INDEXES, null);
  assert.equal(r.error, undefined);
  assert.equal(r.body.k, 999999);
});

test("a dense query vector of the wrong length is refused before the round trip", () => {
  const r = ctx.vecBuildRequest("search", { indexName: "Doc[embedding]", queryVector: "[1, 0]" }, INDEXES, BOUNDS);
  assert.match(r.error, /has 2 dimensions, but index 'Doc\[embedding\]' has 3/);
});

test("a well-formed dense search builds the request the endpoint expects", () => {
  const r = ctx.vecBuildRequest(
    "search",
    { indexName: "Doc[embedding]", queryVector: "[0.5, -1, 2e-1]", k: "5", efSearch: "64", filter: "  lang = 'en'  " },
    INDEXES,
    BOUNDS
  );
  assert.equal(r.path, "search");
  assert.deepEqual(plain(r.body), { indexName: "Doc[embedding]", k: 5, queryVector: [0.5, -1, 0.2], efSearch: 64, filter: "lang = 'en'" });
});

test("k and efSearch are held to the bounds the server advertises", () => {
  const base = { indexName: "Doc[embedding]", queryVector: "1 0 0" };
  assert.equal(ctx.vecBuildRequest("search", Object.assign({}, base, { k: "51" }), INDEXES, BOUNDS).error, "'k' must be at most 50");
  assert.equal(ctx.vecBuildRequest("search", Object.assign({}, base, { k: "0" }), INDEXES, BOUNDS).error, "'k' must be at least 1");
  assert.equal(ctx.vecBuildRequest("search", Object.assign({}, base, { k: "2.5" }), INDEXES, BOUNDS).error, "'k' must be an integer");
  assert.equal(ctx.vecBuildRequest("search", Object.assign({}, base, { efSearch: "401" }), INDEXES, BOUNDS).error, "'efSearch' must be at most 400");
  // an omitted k is omitted from the request, so the server applies its own default
  assert.equal(ctx.vecBuildRequest("search", base, INDEXES, BOUNDS).body.k, undefined);
});

test("the sparse path sends sparse=true, checks the indices, and refuses efSearch", () => {
  const ok = ctx.vecBuildRequest("search", { indexName: "Sparse[dims,weights]", queryVector: "0.5, 0.25", queryIndices: "1, 5" }, INDEXES, BOUNDS);
  assert.deepEqual(plain(ok.body), { indexName: "Sparse[dims,weights]", sparse: true, queryIndices: [1, 5], queryVector: [0.5, 0.25] });

  const outside = ctx.vecBuildRequest("search", { indexName: "Sparse[dims,weights]", queryVector: "0.5", queryIndices: "8" }, INDEXES, BOUNDS);
  assert.equal(outside.error, "Sparse dimension 8 is outside index dimensions 0-7");

  const mismatched = ctx.vecBuildRequest("search", { indexName: "Sparse[dims,weights]", queryVector: "0.5, 0.2", queryIndices: "1" }, INDEXES, BOUNDS);
  assert.match(mismatched.error, /'queryIndices' has 1 entries but 'queryVector' has 2/);

  const ef = ctx.vecBuildRequest("search", { indexName: "Sparse[dims,weights]", queryVector: "0.5", queryIndices: "1", efSearch: "10" }, INDEXES, BOUNDS);
  assert.equal(ef.error, "'efSearch' applies only to a dense index");

  // without indices a sparse vector is positional, so its length must match the index
  const positional = ctx.vecBuildRequest("search", { indexName: "Sparse[dims,weights]", queryVector: "1, 0" }, INDEXES, BOUNDS);
  assert.match(positional.error, /has 2 dimensions, but index 'Sparse\[dims,weights\]' has 8/);
});

test("a sparse index declared without dimensions (listed as 0) checks neither the vector length nor the indices", () => {
  const inferred = ctx.vecSearchableIndexes([
    { name: "Open", indexes: [{ name: "Open[d,w]", typeName: "Open", type: "LSM_SPARSE_VECTOR", properties: ["d", "w"], dimensions: 0, scoring: "score_higher_is_better:dot_product" }] }
  ]);
  assert.equal(inferred.vector[0].dimensions, 0);

  // positional weights of any length go through: the server infers the dimensions from the data and bounds nothing
  const positional = ctx.vecBuildRequest("search", { indexName: "Open[d,w]", queryVector: "0, 0.5, 0, 0, 0.25" }, inferred, BOUNDS);
  assert.equal(positional.error, undefined);
  assert.deepEqual(plain(positional.body), { indexName: "Open[d,w]", sparse: true, queryVector: [0, 0.5, 0, 0, 0.25] });

  // and so does a dimension id far beyond anything a bounded index would accept
  const farIndex = ctx.vecBuildRequest("search", { indexName: "Open[d,w]", queryVector: "1", queryIndices: "100000" }, inferred, BOUNDS);
  assert.equal(farIndex.error, undefined);
  assert.deepEqual(plain(farIndex.body.queryIndices), [100000]);
});

test("an unparseable vector names the offending element", () => {
  const r = ctx.vecBuildRequest("search", { indexName: "Doc[embedding]", queryVector: "1, x, 0" }, INDEXES, BOUNDS);
  assert.equal(r.error, "'queryVector' element 2 is not a number: x");
  assert.equal(ctx.vecBuildRequest("search", { indexName: "", queryVector: "1,0,0" }, INDEXES, BOUNDS).error, "Select a vector index");
});

test("hybrid: half a full-text leg is refused, and weights go only to legs that run", () => {
  const base = { indexName: "Doc[embedding]", queryVector: "1,0,0" };
  const half = ctx.vecBuildRequest("hybrid", Object.assign({}, base, { fulltextQuery: "java" }), INDEXES, BOUNDS);
  assert.equal(half.error, "The full-text leg needs both a full-text index and a query, or neither");

  const full = ctx.vecBuildRequest(
    "hybrid",
    Object.assign({}, base, {
      k: "4",
      fulltextIndexName: "Doc[body]",
      fulltextQuery: "java",
      fulltextWeight: "0.5",
      vectorWeight: "2",
      expandWeight: "9",
      fusionStrategy: "RRF",
      expand: false
    }),
    INDEXES,
    BOUNDS
  );
  assert.equal(full.path, "hybrid");
  assert.deepEqual(plain(full.body), {
    vectorIndexName: "Doc[embedding]",
    k: 4,
    queryVector: [1, 0, 0],
    fulltextQuery: "java",
    fulltextIndexName: "Doc[body]",
    fusionStrategy: "RRF",
    weights: { vector: 2, fulltext: 0.5 }
  });
});

test("hybrid: the expansion leg carries its edge types, direction and a bounded depth", () => {
  const base = { indexName: "Doc[embedding]", queryVector: "1,0,0", expand: true, edgeTypes: " Cites, , Links ", direction: "both" };
  const ok = ctx.vecBuildRequest("hybrid", Object.assign({}, base, { maxDepth: "2", expandWeight: "0.25" }), INDEXES, BOUNDS);
  assert.deepEqual(plain(ok.body.expand), { edgeTypes: ["Cites", "Links"], direction: "both", maxDepth: 2 });
  assert.deepEqual(plain(ok.body.weights), { expand: 0.25 });

  const deep = ctx.vecBuildRequest("hybrid", Object.assign({}, base, { maxDepth: "4" }), INDEXES, BOUNDS);
  assert.equal(deep.error, "'maxDepth' must be at most 3");

  const negative = ctx.vecBuildRequest("hybrid", Object.assign({}, base, { vectorWeight: "-1" }), INDEXES, BOUNDS);
  assert.equal(negative.error, "The vector weight must be a number that is not negative");
});

test("full-text: the query and index are required, and limit is bounded", () => {
  assert.equal(ctx.vecBuildRequest("fulltext", { indexName: "Doc[body]", queryText: "  " }, INDEXES, BOUNDS).error, "'queryText' is required");
  assert.equal(ctx.vecBuildRequest("fulltext", { indexName: "Doc[body]", queryText: "java", limit: "91" }, INDEXES, BOUNDS).error, "'limit' must be at most 90");
  const ok = ctx.vecBuildRequest("fulltext", { indexName: "Doc[body]", queryText: "+java -python", limit: "3" }, INDEXES, BOUNDS);
  assert.equal(ok.path, "fulltext");
  assert.deepEqual(plain(ok.body), { queryText: "+java -python", indexName: "Doc[body]", limit: 3 });
});

test("the response summary surfaces the truncated flag and the ranking direction", () => {
  const filled = ctx.vecDescribeResponse("search", {
    indexName: "Doc[embedding]",
    sparse: false,
    scoring: "distance_lower_is_better:COSINE",
    candidateLimit: 2,
    truncated: true,
    count: 2,
    results: [
      { rid: "#1:0", distance: 0, properties: {} },
      { rid: "#1:1", distance: 0.3, properties: {} }
    ]
  });
  assert.equal(filled.truncated, true);
  assert.equal(filled.scoreLabel, "distance");
  assert.equal(filled.scoring, "distance, lower is better (COSINE)");
  assert.equal(filled.candidateLimit, 2);

  const short = ctx.vecDescribeResponse("search", { sparse: true, scoring: "score_higher_is_better:idf_weighted_dot_product", truncated: false, count: 0, results: [] });
  assert.equal(short.truncated, false);
  assert.equal(short.scoreLabel, "score");
  assert.equal(short.scoring, "score, higher is better (idf weighted dot product)");

  // the full-text endpoint reports no truncation, so the panel must not invent one
  const ft = ctx.vecDescribeResponse("fulltext", { indexName: "Doc[body]", similarity: "BM25", count: 1, results: [{ rid: "#2:0", score: 1.5 }] });
  assert.equal(ft.truncated, null);
  assert.equal(ft.scoreLabel, "score");
});

test("an unfused hybrid response says so, and per-leg counts and caps are reported", () => {
  const unfused = ctx.vecDescribeResponse("hybrid", {
    vectorIndexName: "Doc[embedding]",
    scoring: "distance_lower_is_better:COSINE",
    fused: false,
    truncated: false,
    count: 1,
    legs: { vector: { count: 1 } },
    results: [{ rid: "#1:0", distance: 0.1, sources: ["vector"] }]
  });
  assert.equal(unfused.indexName, "Doc[embedding]");
  assert.equal(unfused.fused, false);
  assert.match(unfused.notes.join("|"), /Not fused/);

  const fused = ctx.vecDescribeResponse("hybrid", {
    fused: true,
    fusionStrategy: "RRF",
    truncated: true,
    legs: { vector: { count: 3 }, fulltext: { count: 2 }, expand: { count: 4, truncated: true, seedsTruncated: false } },
    results: [{ rid: "#1:0", fusedScore: 0.03, sources: ["vector", "expand"] }]
  });
  assert.equal(fused.scoreLabel, "fusedScore");
  assert.deepEqual(plain(fused.notes), [
    "Rows per leg: vector 3, full-text 2, expand 4",
    "Fused with RRF",
    "The graph expansion hit its fan-out cap"
  ]);
});

test("an unfused hybrid response mixing distance and score names both in the column header", () => {
  const mixed = ctx.vecDescribeResponse("hybrid", {
    fused: false,
    results: [
      { rid: "#2:0", score: 1.2, sources: ["fulltext"] },
      { rid: "#1:0", distance: 0.1, sources: ["vector"] },
      { rid: "#2:1", score: 0.7, sources: ["fulltext"] }
    ]
  });
  assert.equal(mixed.scoreLabel, "score / distance");
});

test("each hit shows exactly the ranking value it carries", () => {
  assert.deepEqual(plain(ctx.vecHitScore({ distance: 0.2 })), { label: "distance", value: 0.2 });
  assert.deepEqual(plain(ctx.vecHitScore({ score: 3 })), { label: "score", value: 3 });
  assert.deepEqual(plain(ctx.vecHitScore({ fusedScore: 0.01 })), { label: "fusedScore", value: 0.01 });
  // 0 is a perfectly good distance and must not be mistaken for "absent"
  assert.deepEqual(plain(ctx.vecHitScore({ distance: 0 })), { label: "distance", value: 0 });
});

test("the properties excerpt drops record metadata and is bounded", () => {
  assert.equal(ctx.vecPropertiesExcerpt({ "@rid": "#1:0", "@type": "Doc", title: "a" }, 200), '{"title":"a"}');
  const long = ctx.vecPropertiesExcerpt({ text: "x".repeat(500) }, 50);
  assert.equal(long.length, 50);
  assert.ok(long.endsWith("..."));
});
