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
                    .put("description", "Indexed properties, used with 'typeName' to identify the index, e.g. ['content']"))
                .put("queryText", new JSONObject()
                    .put("type", "string")
                    .put("description", "The full-text query"))
                .put("limit", new JSONObject()
                    .put("type", "integer")
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

    final Database database = MCPToolUtils.resolveDatabase(server, user, databaseName);

    final String indexName = resolveIndexName(database, args);
    final TypeIndex typeIndex = FullTextSearch.resolveFullTextIndex(database, indexName);

    final Map<RID, Float> hits = FullTextSearch.search(database, indexName, queryText);

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
      // also skips any non-document record.
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
   * when both addressing forms are supplied.
   */
  private static String resolveIndexName(final Database database, final JSONObject args) {
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
    final List<String> candidates = new ArrayList<>();
    for (final String name : FullTextSearch.listFullTextIndexes(database))
      if (name.startsWith(prefix))
        candidates.add(name);

    if (candidates.isEmpty())
      throw new IllegalArgumentException(
          "No full-text index found on type '" + typeName + "'. " + describeAvailable(database));

    if (candidates.size() > 1)
      throw new IllegalArgumentException("Type '" + typeName + "' has several full-text indexes: " + candidates
          + ". Pass 'indexName', or narrow with 'properties'.");

    return candidates.get(0);
  }

  /**
   * Validates that the named index exists and is a full-text index. On the success path this costs a single index
   * lookup, not a schema-wide scan: it relies on the exceptions FullTextSearch.resolveFullTextIndex already throws
   * for an unknown or non-full-text name, and only enumerates every full-text index in the database (the cost
   * describeAvailable pays) when building the error message.
   */
  private static String validateFullTextIndex(final Database database, final String indexName) {
    try {
      FullTextSearch.resolveFullTextIndex(database, indexName);
    } catch (final SchemaException e) {
      throw new IllegalArgumentException("Full-text index '" + indexName + "' does not exist. " + describeAvailable(database));
    } catch (final CommandExecutionException e) {
      throw new IllegalArgumentException("Index '" + indexName + "' is not a full-text index. " + describeAvailable(database));
    }

    return indexName;
  }

  /**
   * Builds the recovery hint appended to every addressing error, so the caller can self-correct without a further round-trip.
   */
  private static String describeAvailable(final Database database) {
    final List<String> indexes = FullTextSearch.listFullTextIndexes(database);

    if (indexes.isEmpty())
      return "Database '" + database.getName() + "' has no full-text indexes. Create one with: "
          + "CREATE INDEX ON <Type> (<property>) FULL_TEXT";

    return "Available full-text indexes in '" + database.getName() + "': " + indexes
        + ". An index declared on a supertype is named for the supertype.";
  }
}
