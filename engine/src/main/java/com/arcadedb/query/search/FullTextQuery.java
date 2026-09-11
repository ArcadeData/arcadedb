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
package com.arcadedb.query.search;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Full-text (Lucene-syntax) retrieval over an ArcadeDB {@code FULL_TEXT} index.
 * <p>
 * Protocol-independent on purpose: the HTTP {@code POST /api/v1/vector/{database}/fulltext} route, the gRPC
 * {@code FullTextSearch} RPC and the MCP {@code full_text_search} tool all resolve their own database and their
 * own principal and then call {@link #search(Database, JSONObject)}, so the {@code limit} bound below is the
 * only one any of them enforces.
 */
public final class FullTextQuery {

  public static final int DEFAULT_LIMIT = 10;
  /**
   * Upper bound on the requested window (issue #6837). The per-bucket push-down bounds the index scan, but every
   * surviving hit is then loaded with {@code lookupByRID}, serialized in full and accumulated into the reply's
   * JSONArray, so an unbounded 'limit' still turns one call into "return every matching document". The cap matches
   * the ceiling the sibling search services already use for a candidate window ({@link VectorLeg#MAX_K},
   * {@link HybridSearch#MAX_LEG_CANDIDATES}), which keeps one number to reason about across the search surface.
   */
  public static final int MAX_LIMIT = 1_000;

  private FullTextQuery() {
  }

  /**
   * Checks every argument that needs no schema access. Kept separate from {@link #search} so a caller can run it
   * before resolving the database - the order the protocol surfaces use, so an argument fault is reported as an
   * argument fault regardless of whether the database also resolves, and a rejected request does no I/O.
   *
   * @throws IllegalArgumentException when an argument is missing, malformed or outside its bound
   */
  public static void validateArguments(final JSONObject args) {
    final String queryText = args.getString("queryText", null);
    // A blank query reaches the Lucene parser as an empty clause set and surfaces as IndexException("Invalid search
    // query: "), which names no cause. Reject it here so the caller learns what is actually wrong.
    if (queryText == null || queryText.isBlank())
      throw new IllegalArgumentException("'queryText' must not be blank. Provide at least one term, for example 'java' "
          + "or '+java -python'.");

    // The declared JSON-Schema window is advisory - the client is the one that would enforce it - so re-check it
    // here, before the index is resolved, so an out-of-range limit is reported as a limit fault rather than as
    // whatever addressing error the same call would also have produced.
    final int limit = args.getInt("limit", DEFAULT_LIMIT);
    if (limit < 1 || limit > MAX_LIMIT)
      throw new IllegalArgumentException("'limit' must be between 1 and " + MAX_LIMIT + ", got " + limit);
  }

  /**
   * Runs the search described by {@code args} against an already-resolved, already-authorized database.
   *
   * @param args {@code queryText} is required, plus either {@code indexName} or {@code typeName} (with optional
   *             {@code properties}) to address the index; {@code limit} is optional and bounded by
   *             {@link #MAX_LIMIT}
   *
   * @throws IllegalArgumentException when any argument is missing, malformed or outside its bound
   */
  public static JSONObject search(final Database database, final JSONObject args) {
    validateArguments(args);
    final String queryText = args.getString("queryText");
    final int limit = args.getInt("limit", DEFAULT_LIMIT);

    final TypeIndex typeIndex = resolveIndex(database, args);
    final String indexName = typeIndex.getName();

    // The limit is pushed down per bucket: each bucket keeps only its own top-'limit' matches by score (a bounded
    // min-heap on the BM25 path, a sort-and-truncate on CLASSIC), so this merges at most (bucket count * limit)
    // entries instead of every match in the index.
    final Map<RID, Float> hits;
    try {
      hits = FullTextSearch.search(typeIndex, queryText, limit);
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      // Lucene syntax the parser rejects is the caller's mistake and must be answered as one - HTTP 400, gRPC
      // INVALID_ARGUMENT - rather than as an internal error with a stack trace in the server log (issue #7393).
      final String detail = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
      throw new IllegalArgumentException("Invalid full-text query: " + detail, e);
    }

    final List<Map.Entry<RID, Float>> ranked = new ArrayList<>(hits.entrySet());
    // Score descending, tie-broken by RID so tied hits have a stable, deterministic order instead of depending on
    // HashMap iteration order (which varies with RID hashing and bucket layout).
    ranked.sort(Map.Entry.<RID, Float>comparingByValue().reversed().thenComparing(Map.Entry::getKey));

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final JSONArray results = new JSONArray();
    for (final Map.Entry<RID, Float> hit : ranked) {
      if (results.length() >= limit)
        break;

      // The index scan and this lookup are separate read windows (no explicit transaction is open), so a hit can
      // reference a record deleted concurrently after the scan; lookupByRID then throws RecordNotFoundException for
      // a dangling or concurrently-deleted RID. Skip that hit rather than failing the whole search, exactly as index
      // scans do. lookupByRID also returns Record, whose interface has no asDocument(); pattern-match instead, which
      // also skips any non-document record. Because the limit is pushed down per bucket, a skipped hit here cannot
      // be back-filled from beyond that bucket's top-K the way an unbounded search could: a bounded search can
      // legitimately return fewer than 'limit' results (e.g. limit - 1 for a single-bucket type) when the missing
      // hit was concurrently deleted. That is accepted best-effort behavior, not a bug.
      final Record record;
      try {
        record = database.lookupByRID(hit.getKey(), true);
      } catch (final RecordNotFoundException e) {
        continue;
      }
      if (!(record instanceof final Document document))
        continue;

      results.put(new JSONObject()
          .put("rid", hit.getKey().toString())
          .put("score", hit.getValue())
          .put("properties", serializer.serializeDocument(document)));
    }

    return new JSONObject()
        .put("indexName", indexName)
        .put("similarity", FullTextSearch.getSimilarity(typeIndex))
        .put("count", results.length())
        .put("results", results);
  }

  /**
   * Resolves the target index from 'indexName', or from 'typeName' with optional 'properties'. 'indexName' wins
   * when both addressing forms are supplied. Resolution happens exactly once here; the returned TypeIndex is passed
   * directly to {@link FullTextSearch#search(TypeIndex, String, int)} rather than re-resolved by name.
   */
  private static TypeIndex resolveIndex(final Database database, final JSONObject args) {
    final String indexName = args.getString("indexName", null);

    if (indexName != null && !indexName.isBlank())
      return validateFullTextIndex(database, indexName);

    final String typeName = args.getString("typeName", null);
    if (typeName == null || typeName.isBlank())
      throw new IllegalArgumentException(
          "Provide either 'indexName', or 'typeName' with optional 'properties'. " + describeAvailable(database));

    final JSONArray properties = args.getJSONArray("properties", null);
    if (properties != null && properties.length() > 0) {
      final StringBuilder derived = new StringBuilder(typeName).append('[');
      for (int i = 0; i < properties.length(); i++) {
        if (i > 0)
          derived.append(',');
        derived.append(properties.getString(i));
      }
      derived.append(']');
      // The schema derives an index name as typeName + Arrays.toString(propertyNames) with every space stripped from the
      // result, so strip spaces here too. Otherwise a property name containing a space would derive a name that can never
      // match the one the schema registered.
      return validateFullTextIndex(database, derived.toString().replace(" ", ""));
    }

    // 'typeName' alone is usable only when the type carries exactly one full-text index. An index declared on a
    // supertype is named for the supertype, so a subtype name resolves nothing here even though the index applies
    // to its records too; the error from describeAvailable() points the caller at the supertype's index name.
    final String prefix = typeName + "[";
    final List<String> allIndexes = FullTextSearch.listFullTextIndexes(database);
    final List<String> candidates = new ArrayList<>();
    for (final String name : allIndexes)
      if (name.startsWith(prefix))
        candidates.add(name);

    if (candidates.isEmpty())
      throw new IllegalArgumentException(
          "No full-text index found on type '" + typeName + "'. " + describeAvailable(database, allIndexes));

    if (candidates.size() > 1)
      throw new IllegalArgumentException("Type '" + typeName + "' has several full-text indexes: " + candidates
          + ". Pass 'indexName', or narrow with 'properties'.");

    return validateFullTextIndex(database, candidates.get(0));
  }

  /**
   * Validates that the named index exists and is a full-text index, and returns the resolved TypeIndex so the caller
   * can search it directly instead of resolving the name a second time. On the success path this costs a single
   * index lookup, not a schema-wide scan: it relies on the exceptions FullTextSearch.resolveFullTextIndex already
   * throws for an unknown or non-full-text name, and only enumerates every full-text index in the database (the cost
   * describeAvailable pays) when building the error message.
   */
  private static TypeIndex validateFullTextIndex(final Database database, final String indexName) {
    try {
      return FullTextSearch.resolveFullTextIndex(database, indexName);
    } catch (final SchemaException e) {
      throw new IllegalArgumentException(
          "Full-text index '" + indexName + "' does not exist. " + describeAvailable(database), e);
    } catch (final CommandExecutionException e) {
      throw new IllegalArgumentException(
          "Index '" + indexName + "' is not a full-text index. " + describeAvailable(database), e);
    }
  }

  /**
   * Builds the recovery hint appended to every addressing error, so the caller can self-correct without a further round-trip.
   */
  private static String describeAvailable(final Database database) {
    return describeAvailable(database, FullTextSearch.listFullTextIndexes(database));
  }

  /**
   * Same recovery hint as {@link #describeAvailable(Database)}, but reuses an already-materialized index list
   * instead of walking the schema again when the caller has one on hand.
   */
  private static String describeAvailable(final Database database, final List<String> indexes) {
    if (indexes.isEmpty())
      return "Database '" + database.getName() + "' has no full-text indexes. Create one with: "
          + "CREATE INDEX ON <Type> (<property>) FULL_TEXT";

    return "Available full-text indexes in '" + database.getName() + "': " + indexes
        + ". An index declared on a supertype is named for the supertype.";
  }
}
