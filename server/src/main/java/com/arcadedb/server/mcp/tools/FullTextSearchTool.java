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
package com.arcadedb.server.mcp.tools;

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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FullTextSearchTool {

  private static final int DEFAULT_LIMIT = 10;

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "full_text_search")
        .put("description",
            """
            Search a full-text index and return the matching records ranked by relevance score. \
            Address the index either by 'indexName' (e.g. 'Article[content]') or by 'typeName' plus optional 'properties'. \
            If both are given, 'indexName' wins. Query syntax: '+a +b' requires both terms, 'a -b' excludes b, 'a b' matches \
            either, '"exact phrase"' requires all terms in the same record (term order is NOT enforced), 'pre*' matches a \
            prefix, 'term~' is a fuzzy match, 'field:term' restricts to one property of a multi-property index, and 'term^2' \
            boosts a term. The returned 'similarity' says whether scores are BM25 or legacy CLASSIC coordination counts.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to search"))
                .put("indexName", new JSONObject()
                    .put("type", "string")
                    .put("description", "Name of the full-text index, e.g. 'Article[content]' or 'Article[title,body]'"))
                .put("typeName", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Type carrying the full-text index, e.g. 'Article'. An index declared on a supertype is named for the supertype"))
                .put("properties", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "string"))
                    .put("description",
                        "Indexed properties, used with 'typeName' to identify the index, e.g. ['content']. Must be given "
                            + "in the same order the index declares them, e.g. ['title','content'] for an index declared as "
                            + "Article[title,content]"))
                .put("queryText", new JSONObject()
                    .put("type", "string")
                    .put("description", "The full-text query"))
                .put("limit", new JSONObject()
                    .put("type", "integer")
                    .put("default", DEFAULT_LIMIT)
                    .put("description", "Maximum number of results to return (default: 10)")))
            .put("required", new JSONArray().put("database").put("queryText")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = args.getString("database");
    final String queryText = args.getString("queryText");
    final int limit = args.getInt("limit", DEFAULT_LIMIT);
    if (limit < 1)
      throw new IllegalArgumentException("'limit' must be at least 1, got " + limit);

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    final TypeIndex typeIndex = resolveIndex(database, args);
    final String indexName = typeIndex.getName();

    // The limit is pushed down per bucket: each bucket keeps only its own top-'limit' matches by score (a bounded
    // min-heap on the BM25 path, a sort-and-truncate on CLASSIC), so this merges at most (bucket count * limit)
    // entries instead of every match in the index.
    final Map<RID, Float> hits = FullTextSearch.search(typeIndex, queryText, limit);

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
      return validateFullTextIndex(database, derived.append(']').toString());
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
