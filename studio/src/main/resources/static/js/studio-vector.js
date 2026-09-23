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

// Vector tab (issue #7312): a front end for POST /api/v1/vector/{database}/search, /hybrid and /fulltext.
//
// Two things here are deliberately NOT written down in this file:
// - the argument bounds (k, efSearch, limit, maxDepth) and the closed value sets (fusionStrategy, expand.direction)
//   are read from the server's own OpenAPI document, which builds them from the constants the search services
//   enforce. A bound restated here would drift from the server the first time either side changed it.
// - the dimensions and scoring of each vector index are read from schema:types, which takes them from the same
//   place a search takes the 'scoring' it reports.
// When the OpenAPI document cannot be read, the form checks no bound at all and leaves the server to refuse.
//
// The request assembly, the pre-flight checks and the response description are pure functions of plain values, so
// studio/test/vector-search-panel.test.js can drive them without a DOM. The jQuery handlers only collect the inputs.

var vecIndexes = { vector: [], fulltext: [] };
var vecBounds = null;
var vecMode = "search";
var vecLastResponse = null;

// The route suffix of each mode: POST api/v1/vector/{database}/<path>.
var VEC_MODES = {
  search: { path: "search" },
  hybrid: { path: "hybrid" },
  fulltext: { path: "fulltext" }
};

// ===== Pure functions =====

/**
 * Splits the rows of `SELECT FROM schema:types` into the indexes each endpoint can search: LSM_VECTOR and
 * LSM_SPARSE_VECTOR for the vector leg, FULL_TEXT for the full-text endpoint and the hybrid full-text leg.
 */
function vecSearchableIndexes(types) {
  var vector = [];
  var fulltext = [];
  for (var i = 0; i < (types || []).length; i++) {
    var indexes = types[i].indexes || [];
    for (var j = 0; j < indexes.length; j++) {
      var idx = indexes[j];
      var entry = {
        name: idx.name,
        typeName: idx.typeName || types[i].name,
        properties: idx.properties || [],
        indexType: idx.type
      };
      if (idx.type === "LSM_VECTOR" || idx.type === "LSM_SPARSE_VECTOR") {
        entry.sparse = idx.type === "LSM_SPARSE_VECTOR";
        entry.dimensions = typeof idx.dimensions === "number" ? idx.dimensions : null;
        entry.scoring = idx.scoring || null;
        vector.push(entry);
      } else if (idx.type === "FULL_TEXT") {
        fulltext.push(entry);
      }
    }
  }
  var byName = function (a, b) {
    return a.name < b.name ? -1 : a.name > b.name ? 1 : 0;
  };
  vector.sort(byName);
  fulltext.sort(byName);
  return { vector: vector, fulltext: fulltext };
}

/** Reads one integer property's advertised range out of a request schema, or null when the spec carries none. */
function vecIntegerBound(schema) {
  if (!schema) return null;
  var bound = {
    min: typeof schema.minimum === "number" ? schema.minimum : null,
    max: typeof schema.maximum === "number" ? schema.maximum : null,
    def: typeof schema["default"] === "number" ? schema["default"] : null
  };
  return bound.min == null && bound.max == null && bound.def == null ? null : bound;
}

/**
 * Extracts, from the server's OpenAPI document, every bound and value set this panel checks or offers. A missing
 * schema yields a missing bound rather than a guessed one: the form then checks nothing and the server decides.
 */
function vecBoundsFromOpenApi(spec) {
  var schemas = (spec && spec.components && spec.components.schemas) || {};
  var props = function (name) {
    return (schemas[name] && schemas[name].properties) || {};
  };
  var search = props("VectorSearchRequest");
  var hybrid = props("HybridSearchRequest");
  var fulltext = props("FullTextSearchRequest");
  var expand = (hybrid.expand && hybrid.expand.properties) || {};
  return {
    search: {
      k: vecIntegerBound(search.k),
      efSearch: vecIntegerBound(search.efSearch)
    },
    hybrid: {
      k: vecIntegerBound(hybrid.k),
      efSearch: vecIntegerBound(hybrid.efSearch),
      maxDepth: vecIntegerBound(expand.maxDepth),
      fusionStrategies: (hybrid.fusionStrategy && hybrid.fusionStrategy["enum"]) || [],
      directions: (expand.direction && expand.direction["enum"]) || []
    },
    fulltext: {
      limit: vecIntegerBound(fulltext.limit)
    }
  };
}

/**
 * Parses a list of numbers typed as a JSON array (`[0.1, 0.2]`) or as a comma/space separated list (`0.1, 0.2`).
 * Returns `{ values: [...] }` or `{ error: "..." }`; an empty input returns an empty list.
 */
function vecParseNumberList(text, field) {
  var raw = text == null ? "" : String(text).trim();
  if (raw === "") return { values: [] };
  if (raw.charAt(0) === "[") {
    if (raw.charAt(raw.length - 1) !== "]") return { error: "'" + field + "' is not a valid JSON array" };
    raw = raw.substring(1, raw.length - 1).trim();
    if (raw === "") return { values: [] };
  }
  var parts = raw.split(/[\s,]+/);
  var values = [];
  for (var i = 0; i < parts.length; i++) {
    if (parts[i] === "") continue;
    var n = Number(parts[i]);
    if (!isFinite(n)) return { error: "'" + field + "' element " + (values.length + 1) + " is not a number: " + parts[i] };
    values.push(n);
  }
  return { values: values };
}

/** Parses an optional integer input. Returns `{ value }` (null when blank) or `{ error }`. */
function vecParseInteger(text, field) {
  var raw = text == null ? "" : String(text).trim();
  if (raw === "") return { value: null };
  var n = Number(raw);
  if (!isFinite(n) || Math.floor(n) !== n) return { error: "'" + field + "' must be an integer" };
  return { value: n };
}

/** Checks a value against a bound read from the server. No bound, or no value, means nothing to check. */
function vecCheckBound(value, bound, field) {
  if (value == null || bound == null) return null;
  if (bound.min != null && value < bound.min) return "'" + field + "' must be at least " + bound.min;
  if (bound.max != null && value > bound.max) return "'" + field + "' must be at most " + bound.max;
  return null;
}

/**
 * The vector-leg arguments shared by /search and /hybrid: the query vector, the sparse indices and efSearch, checked
 * against the index's own dimension count so a vector of the wrong length never makes the round trip.
 */
function vecBuildVectorLeg(form, index, bounds, body) {
  var vector = vecParseNumberList(form.queryVector, "queryVector");
  if (vector.error) return vector.error;
  if (vector.values.length === 0) return "'queryVector' is required";

  var dims = index.dimensions;
  if (index.sparse) {
    body.sparse = true;
    var indices = vecParseNumberList(form.queryIndices, "queryIndices");
    if (indices.error) return indices.error;
    if (indices.values.length > 0) {
      if (indices.values.length !== vector.values.length)
        return "'queryIndices' has " + indices.values.length + " entries but 'queryVector' has " + vector.values.length;
      for (var i = 0; i < indices.values.length; i++) {
        var d = indices.values[i];
        if (Math.floor(d) !== d || d < 0) return "'queryIndices' must contain only non-negative integers";
        if (dims > 0 && d >= dims) return "Sparse dimension " + d + " is outside index dimensions 0-" + (dims - 1);
      }
      body.queryIndices = indices.values;
    } else if (dims > 0 && vector.values.length !== dims) {
      return "'queryVector' has " + vector.values.length + " dimensions, but index '" + index.name + "' has " + dims +
        ". Fill 'queryIndices' to send compact sparse weights instead";
    }
  } else {
    if (form.queryIndices != null && String(form.queryIndices).trim() !== "")
      return "'queryIndices' applies only to a sparse index";
    if (dims > 0 && vector.values.length !== dims)
      return "'queryVector' has " + vector.values.length + " dimensions, but index '" + index.name + "' has " + dims;
  }
  body.queryVector = vector.values;

  var ef = vecParseInteger(form.efSearch, "efSearch");
  if (ef.error) return ef.error;
  if (ef.value != null) {
    if (index.sparse) return "'efSearch' applies only to a dense index";
    var efError = vecCheckBound(ef.value, bounds && bounds.efSearch, "efSearch");
    if (efError) return efError;
    body.efSearch = ef.value;
  }

  var filter = form.filter == null ? "" : String(form.filter).trim();
  if (filter !== "") body.filter = filter;
  return null;
}

/** Parses an optional non-negative leg weight. Returns `{ value }` (null when blank) or `{ error }`. */
function vecParseWeight(text, leg) {
  var raw = text == null ? "" : String(text).trim();
  if (raw === "") return { value: null };
  var n = Number(raw);
  if (!isFinite(n) || n < 0) return { error: "The " + leg + " weight must be a number that is not negative" };
  return { value: n };
}

/**
 * Builds the request for one of the three endpoints from the form's raw string values. Returns
 * `{ path, body }` on success and `{ error }` for anything the form can already tell the server would refuse.
 *
 * @param mode    "search" | "hybrid" | "fulltext"
 * @param form    raw input values, keyed by request field name
 * @param indexes the output of vecSearchableIndexes()
 * @param bounds  the output of vecBoundsFromOpenApi(), or null when it could not be read
 */
function vecBuildRequest(mode, form, indexes, bounds) {
  var modeBounds = (bounds && bounds[mode]) || null;
  var body = {};
  var err;

  if (mode === "fulltext") {
    var queryText = form.queryText == null ? "" : String(form.queryText).trim();
    if (queryText === "") return { error: "'queryText' is required" };
    if (!form.indexName) return { error: "Select a full-text index" };
    body.queryText = queryText;
    body.indexName = form.indexName;
    var limit = vecParseInteger(form.limit, "limit");
    if (limit.error) return { error: limit.error };
    if (limit.value != null) {
      err = vecCheckBound(limit.value, modeBounds && modeBounds.limit, "limit");
      if (err) return { error: err };
      body.limit = limit.value;
    }
    return { path: VEC_MODES.fulltext.path, body: body };
  }

  if (mode !== "search" && mode !== "hybrid") return { error: "Unknown search mode: " + mode };

  var index = null;
  for (var i = 0; i < indexes.vector.length; i++)
    if (indexes.vector[i].name === form.indexName) index = indexes.vector[i];
  if (!index) return { error: "Select a vector index" };

  body[mode === "hybrid" ? "vectorIndexName" : "indexName"] = index.name;

  var k = vecParseInteger(form.k, "k");
  if (k.error) return { error: k.error };
  if (k.value != null) {
    err = vecCheckBound(k.value, modeBounds && modeBounds.k, "k");
    if (err) return { error: err };
    body.k = k.value;
  }

  err = vecBuildVectorLeg(form, index, modeBounds, body);
  if (err) return { error: err };

  if (mode === "search") return { path: VEC_MODES.search.path, body: body };

  // Hybrid: the full-text and expansion legs are each all-or-nothing, and a weight is sent only for a leg that runs,
  // because the server refuses a weight for a leg the request does not ask for.
  var weights = {};
  var w = vecParseWeight(form.vectorWeight, "vector");
  if (w.error) return { error: w.error };
  if (w.value != null) weights.vector = w.value;

  var ftQuery = form.fulltextQuery == null ? "" : String(form.fulltextQuery).trim();
  if (ftQuery !== "" || form.fulltextIndexName) {
    if (ftQuery === "" || !form.fulltextIndexName)
      return { error: "The full-text leg needs both a full-text index and a query, or neither" };
    body.fulltextQuery = ftQuery;
    body.fulltextIndexName = form.fulltextIndexName;
    w = vecParseWeight(form.fulltextWeight, "full-text");
    if (w.error) return { error: w.error };
    if (w.value != null) weights.fulltext = w.value;
  }

  if (form.expand) {
    var expand = {};
    var edgeTypes = form.edgeTypes == null ? "" : String(form.edgeTypes).trim();
    if (edgeTypes !== "")
      expand.edgeTypes = edgeTypes.split(",").map(function (s) {
        return s.trim();
      }).filter(function (s) {
        return s !== "";
      });
    if (form.direction) expand.direction = form.direction;
    var depth = vecParseInteger(form.maxDepth, "maxDepth");
    if (depth.error) return { error: depth.error };
    if (depth.value != null) {
      err = vecCheckBound(depth.value, modeBounds && modeBounds.maxDepth, "maxDepth");
      if (err) return { error: err };
      expand.maxDepth = depth.value;
    }
    body.expand = expand;
    w = vecParseWeight(form.expandWeight, "expansion");
    if (w.error) return { error: w.error };
    if (w.value != null) weights.expand = w.value;
  }

  if (form.fusionStrategy) body.fusionStrategy = form.fusionStrategy;
  if (Object.keys(weights).length > 0) body.weights = weights;

  return { path: VEC_MODES.hybrid.path, body: body };
}

/**
 * Turns the server's 'scoring' string ("distance_lower_is_better:COSINE", "score_higher_is_better:dot_product") into
 * a label a person can read. An unrecognized string is shown as it came.
 */
function vecDescribeScoring(scoring) {
  if (!scoring) return "";
  var sep = scoring.indexOf(":");
  var direction = sep >= 0 ? scoring.substring(0, sep) : scoring;
  var how = sep >= 0 ? scoring.substring(sep + 1) : "";
  if (direction === "distance_lower_is_better") return "distance, lower is better" + (how ? " (" + how + ")" : "");
  if (direction === "score_higher_is_better") return "score, higher is better" + (how ? " (" + how.replace(/_/g, " ") + ")" : "");
  return scoring;
}

/**
 * The ranking value one hit carries and what to call it. A dense hit carries 'distance', a sparse or full-text hit
 * 'score', a fused hybrid hit 'fusedScore' - never more than one.
 */
function vecHitScore(hit) {
  if (hit == null) return { label: "", value: null };
  if (typeof hit.fusedScore === "number") return { label: "fusedScore", value: hit.fusedScore };
  if (typeof hit.distance === "number") return { label: "distance", value: hit.distance };
  if (typeof hit.score === "number") return { label: "score", value: hit.score };
  return { label: "", value: null };
}

/**
 * Summarizes a response for the header above the results: which index answered, how it ranks, how many hits, and
 * whether the result window was filled. 'truncated' is the signal that further matches may exist and 'k' should be
 * raised; the full-text endpoint reports no such flag, so it is null there rather than a guessed false.
 */
function vecDescribeResponse(mode, response) {
  var r = response || {};
  var results = r.results || [];
  // An unfused hybrid response can mix labels (a vector-leg hit carries 'distance', a full-text-leg hit 'score'), so
  // the column is named after every label present, in order of first appearance, rather than after the first row.
  var labels = [];
  for (var i = 0; i < results.length; i++) {
    var label = vecHitScore(results[i]).label;
    if (label && labels.indexOf(label) < 0) labels.push(label);
  }
  var scoreLabel = labels.join(" / ");

  var summary = {
    indexName: mode === "hybrid" ? r.vectorIndexName : r.indexName,
    scoring: mode === "fulltext" ? (r.similarity ? "score, higher is better (" + r.similarity + ")" : "") : vecDescribeScoring(r.scoring),
    count: typeof r.count === "number" ? r.count : results.length,
    truncated: typeof r.truncated === "boolean" ? r.truncated : null,
    candidateLimit: typeof r.candidateLimit === "number" ? r.candidateLimit : null,
    scoreLabel: scoreLabel || (mode === "search" ? (r.sparse ? "score" : "distance") : "score"),
    notes: []
  };

  if (mode === "hybrid") {
    summary.fused = r.fused === true;
    if (r.fused === false) summary.notes.push("Not fused: only one leg produced rows, so each hit keeps that leg's own " + summary.scoreLabel);
    else if (r.fusionStrategy) summary.notes.push("Fused with " + r.fusionStrategy);
    var legs = r.legs || {};
    var parts = [];
    if (legs.vector) parts.push("vector " + legs.vector.count);
    if (legs.fulltext) parts.push("full-text " + legs.fulltext.count);
    if (legs.expand) {
      parts.push("expand " + legs.expand.count);
      if (legs.expand.truncated) summary.notes.push("The graph expansion hit its fan-out cap");
      if (legs.expand.seedsTruncated) summary.notes.push("The graph expansion hit its seed cap");
    }
    if (parts.length > 0) summary.notes.unshift("Rows per leg: " + parts.join(", "));
  }
  return summary;
}

/** A short one-line excerpt of a hit's record, for the results table. Record metadata is left out. */
function vecPropertiesExcerpt(properties, maxLength) {
  var props = properties || {};
  var copy = {};
  for (var key in props)
    if (Object.prototype.hasOwnProperty.call(props, key) && key.charAt(0) !== "@") copy[key] = props[key];
  var text = JSON.stringify(copy);
  var max = maxLength || 200;
  return text.length > max ? text.substring(0, max - 3) + "..." : text;
}

// ===== DOM handlers =====

function initVector() {
  if (!$("#vecDbSelectContainer").children().length) initSearchableDbSelect("vecDbSelectContainer");
  vecSetMode(vecMode);
  vecLoadBounds();
  vecLoadIndexes();
}

function vecLoadBounds() {
  if (vecBounds != null) return;
  jQuery
    .ajax({
      type: "GET",
      url: "api/v1/openapi.json",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      }
    })
    .done(function (spec) {
      if (typeof spec === "string") spec = JSON.parse(spec);
      vecBounds = vecBoundsFromOpenApi(spec);
      vecApplyBounds();
    })
    .fail(function () {
      // Without the document the form checks no bound; the server still enforces every one of them.
      $("#vecBoundsNote").text("Argument bounds unavailable: the server will validate them.");
    });
}

function vecApplyBounds() {
  if (!vecBounds) return;
  var setBound = function ($input, bound) {
    if (!bound) return;
    if (bound.min != null) $input.attr("min", bound.min);
    if (bound.max != null) $input.attr("max", bound.max);
    if (bound.def != null) $input.attr("placeholder", bound.def);
    var range = (bound.min != null ? bound.min : "") + " - " + (bound.max != null ? bound.max : "");
    $input.attr("title", "Accepted by the server: " + range);
  };
  var modeBounds = vecBounds[vecMode] || {};
  setBound($("#vecK"), modeBounds.k);
  setBound($("#vecEfSearch"), modeBounds.efSearch);
  setBound($("#vecLimit"), vecBounds.fulltext.limit);
  setBound($("#vecMaxDepth"), vecBounds.hybrid.maxDepth);

  var fillSelect = function ($select, values, emptyLabel) {
    var current = $select.val();
    $select.empty().append($("<option>").val("").text(emptyLabel));
    for (var i = 0; i < values.length; i++) $select.append($("<option>").val(values[i]).text(values[i]));
    if (current) $select.val(current);
  };
  fillSelect($("#vecFusionStrategy"), vecBounds.hybrid.fusionStrategies, "(server default)");
  fillSelect($("#vecDirection"), vecBounds.hybrid.directions, "(server default)");
}

function vecLoadIndexes() {
  var db = getCurrentDatabase();
  if (!db) return;
  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/query/" + encodeDatabaseName(db),
      data: JSON.stringify({ language: "sql", command: "SELECT FROM schema:types" }),
      contentType: "application/json",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      }
    })
    .done(function (data) {
      vecIndexes = vecSearchableIndexes(data.result || []);
      vecRenderIndexes();
    })
    .fail(function (jqXHR) {
      globalNotifyError(jqXHR.responseText);
    });
}

function vecRenderIndexes() {
  var $tbody = $("#vecIndexesTable tbody").empty();
  var all = vecIndexes.vector.concat(vecIndexes.fulltext);
  $("#vecIndexesEmpty").toggle(all.length === 0);
  $("#vecIndexesTable").toggle(all.length > 0);
  for (var i = 0; i < all.length; i++) {
    var idx = all[i];
    var kind = idx.indexType === "FULL_TEXT" ? "full-text" : idx.sparse ? "sparse" : "dense";
    var dims = idx.dimensions == null ? "-" : idx.dimensions === 0 && idx.sparse ? "inferred" : idx.dimensions;
    var scoring = idx.indexType === "FULL_TEXT" ? "score, higher is better" : vecDescribeScoring(idx.scoring);
    $tbody.append(
      "<tr data-index='" + escapeHtml(idx.name) + "'>" +
        "<td class='vec-index-name'>" + escapeHtml(idx.name) + "</td>" +
        "<td>" + escapeHtml(idx.typeName) + "</td>" +
        "<td>" + escapeHtml((idx.properties || []).join(", ")) + "</td>" +
        "<td>" + kind + "</td>" +
        "<td class='vec-index-dims'>" + escapeHtml(dims) + "</td>" +
        "<td>" + escapeHtml(scoring) + "</td>" +
      "</tr>"
    );
  }

  var fillSelect = function ($select, list) {
    var current = $select.val();
    $select.empty().append($("<option>").val("").text("-- select index --"));
    for (var j = 0; j < list.length; j++) $select.append($("<option>").val(list[j].name).text(list[j].name));
    if (current) $select.val(current);
  };
  fillSelect($("#vecIndexName"), vecIndexes.vector);
  fillSelect($("#vecFulltextIndexName"), vecIndexes.fulltext);
  fillSelect($("#vecFtIndexName"), vecIndexes.fulltext);
  vecIndexChanged();
}

function vecSetMode(mode) {
  vecMode = mode;
  $(".vec-mode-btn").removeClass("active");
  $(".vec-mode-btn[data-mode='" + mode + "']").addClass("active");
  $(".vec-only-vector").toggle(mode !== "fulltext");
  $(".vec-only-hybrid").toggle(mode === "hybrid");
  $(".vec-only-fulltext").toggle(mode === "fulltext");
  vecApplyBounds();
  vecIndexChanged();
}

/** Shows the selected index's dimension count next to the vector input, and the inputs that apply to it. */
function vecIndexChanged() {
  var index = vecSelectedVectorIndex();
  var hint = "";
  if (index) {
    hint = index.sparse ? "sparse" : "dense";
    if (index.dimensions > 0) hint += ", " + index.dimensions + " dimensions";
    if (index.scoring) hint += ", " + vecDescribeScoring(index.scoring);
  }
  $("#vecIndexHint").text(hint);
  $(".vec-only-sparse").toggle(index != null && index.sparse === true && vecMode !== "fulltext");
  $(".vec-only-dense").toggle(index != null && index.sparse !== true && vecMode !== "fulltext");
}

/** The selected vector index's entry from vecIndexes, or null. */
function vecSelectedVectorIndex() {
  var name = $("#vecIndexName").val();
  for (var i = 0; i < vecIndexes.vector.length; i++) if (vecIndexes.vector[i].name === name) return vecIndexes.vector[i];
  return null;
}

function vecCollectForm() {
  // queryIndices and efSearch are collected by the selected index's kind, not by which input happens to be visible,
  // so a hidden input left filled from a previously selected index can never reach the request.
  var index = vecSelectedVectorIndex();
  var sparse = index != null && index.sparse === true;
  return {
    indexName: vecMode === "fulltext" ? $("#vecFtIndexName").val() : $("#vecIndexName").val(),
    queryVector: $("#vecQueryVector").val(),
    queryIndices: sparse ? $("#vecQueryIndices").val() : "",
    k: $("#vecK").val(),
    efSearch: index != null && !sparse ? $("#vecEfSearch").val() : "",
    filter: $("#vecFilter").val(),
    fulltextIndexName: $("#vecFulltextIndexName").val(),
    fulltextQuery: $("#vecFulltextQuery").val(),
    fusionStrategy: $("#vecFusionStrategy").val(),
    vectorWeight: $("#vecVectorWeight").val(),
    fulltextWeight: $("#vecFulltextWeight").val(),
    expand: $("#vecExpand").is(":checked"),
    edgeTypes: $("#vecEdgeTypes").val(),
    direction: $("#vecDirection").val(),
    maxDepth: $("#vecMaxDepth").val(),
    expandWeight: $("#vecExpandWeight").val(),
    queryText: $("#vecQueryText").val(),
    limit: $("#vecLimit").val()
  };
}

function vecShowError(message) {
  $("#vecError").text(message).toggle(!!message);
}

function vecExecute() {
  var db = getCurrentDatabase();
  if (!db) return;
  vecShowError("");
  var request = vecBuildRequest(vecMode, vecCollectForm(), vecIndexes, vecBounds);
  if (request.error) {
    vecShowError(request.error);
    return;
  }

  var mode = vecMode;
  var started = Date.now();
  $("#vecExecuteBtn").prop("disabled", true);
  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/vector/" + encodeDatabaseName(db) + "/" + request.path,
      data: JSON.stringify(request.body),
      contentType: "application/json",
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      }
    })
    .done(function (data) {
      vecLastResponse = data;
      vecRenderResults(mode, data, Date.now() - started);
    })
    .fail(function (jqXHR) {
      var message = jqXHR.responseText;
      try {
        var json = JSON.parse(jqXHR.responseText);
        // 'detail' carries the reason, but only outside production mode; 'error' alone is then all there is.
        message = json.detail ? json.detail : json.error || message;
      } catch (e) {
        // not JSON: show the raw body
      }
      vecShowError(message || "Search failed");
    })
    .always(function () {
      $("#vecExecuteBtn").prop("disabled", false);
    });
}

function vecRenderResults(mode, response, elapsedMs) {
  var summary = vecDescribeResponse(mode, response);
  var head = [];
  if (summary.indexName) head.push("<span>Index <b>" + escapeHtml(summary.indexName) + "</b></span>");
  if (summary.scoring) head.push("<span class='vec-scoring'>" + escapeHtml(summary.scoring) + "</span>");
  head.push("<span id='vecResultCount'>" + summary.count + " result" + (summary.count === 1 ? "" : "s") + "</span>");
  if (summary.candidateLimit != null) head.push("<span>candidate window " + summary.candidateLimit + "</span>");
  head.push("<span>" + elapsedMs + " ms</span>");
  $("#vecSummary").html(head.join(" &middot; "));

  var $truncated = $("#vecTruncated");
  if (summary.truncated === true) {
    $truncated
      .removeClass("text-bg-secondary")
      .addClass("text-bg-warning")
      .text("Truncated: the result window was filled, so more matches may exist. Raise 'k' to see them.")
      .show();
  } else if (summary.truncated === false) {
    $truncated
      .removeClass("text-bg-warning")
      .addClass("text-bg-secondary")
      .text("Complete: every match the search found is shown.")
      .show();
  } else {
    $truncated.hide();
  }

  var $notes = $("#vecNotes").empty();
  for (var n = 0; n < summary.notes.length; n++) $notes.append($("<div>").text(summary.notes[n]));

  var hybrid = mode === "hybrid";
  var $thead = $("#vecResultsTable thead").empty();
  $thead.append(
    "<tr><th>#</th><th>RID</th><th class='vec-score-header'>" + escapeHtml(summary.scoreLabel) + "</th>" +
      (hybrid ? "<th>Sources</th><th>Depth</th>" : "") +
      "<th>Type</th><th>Properties</th></tr>"
  );
  var $tbody = $("#vecResultsTable tbody").empty();
  var results = response.results || [];
  for (var i = 0; i < results.length; i++) {
    var hit = results[i];
    var score = vecHitScore(hit);
    var props = hit.properties || {};
    $tbody.append(
      "<tr class='vec-result-row'>" +
        "<td>" + (i + 1) + "</td>" +
        "<td class='vec-rid'>" + escapeHtml(hit.rid) + "</td>" +
        "<td class='vec-score' data-score-label='" + escapeHtml(score.label) + "'>" + (score.value == null ? "" : escapeHtml(score.value)) + "</td>" +
        (hybrid
          ? "<td>" + escapeHtml((hit.sources || []).join(", ")) + "</td><td>" + (hit.depth == null ? "" : escapeHtml(hit.depth)) + "</td>"
          : "") +
        "<td>" + escapeHtml(props["@type"] || "") + "</td>" +
        "<td class='vec-props'><code>" + escapeHtml(vecPropertiesExcerpt(props, 200)) + "</code></td>" +
      "</tr>"
    );
  }
  $("#vecResultsPanel").show();
}

$(document).on("databaseChanged", function () {
  if (typeof studioCurrentTab !== "undefined" && studioCurrentTab === "vector") vecLoadIndexes();
});
