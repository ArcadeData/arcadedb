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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Fuses a vector retrieval leg, a full-text retrieval leg, and a depth-limited graph expansion leg
 * into one ranked result list.
 * <p>
 * The expansion leg is seeded from the union of the retrieval legs and carries no score of its own:
 * its breadth-first discovery order is its rank. Fusion scoring is performed by the engine's
 * {@code vector.fuse}, never re-implemented here.
 * <p>
 * A request naming only the vector leg cannot be fused, because fusion needs at least two sources.
 * Such a response reports {@code fused=false} and returns the leg's native distance or score rather
 * than a fabricated fused score.
 */
public class HybridSearchTool {
  public static final int LEG_OVERFETCH      = 4;
  public static final int MAX_LEG_CANDIDATES = 1_000;
  public static final int MAX_SEEDS          = 256;
  public static final int MAX_EXPANSION      = 2_000;
  public static final int MAX_DEPTH          = 3;

  /**
   * One row of one retrieval leg. A null score marks a rank-only leg, whose position in the list is
   * its only ranking signal.
   */
  public record LegRow(RID rid, Double score) {
  }

  /**
   * One fusion source: its display name, its rows in rank order, and the weight applied to every
   * rank contribution it makes.
   */
  public record Leg(String name, List<LegRow> rows, float weight) {
  }

  /**
   * Provenance for a row the expansion leg contributed: how many hops from its seed, and the RID
   * path back to that seed including the seed itself.
   */
  public record ExpansionInfo(int depth, List<RID> path) {
  }

  public static JSONObject getDefinition() {
    return new JSONObject()
        .put("name", "hybrid_search")
        .put("description",
            """
            Retrieve records by fusing a vector search, an optional full-text search, and an optional \
            graph expansion into one ranked list. The expansion walks outward from the records the other \
            legs found, so a neighbor of a strong match can itself rank. Supply 'fulltextIndexName' with \
            'fulltextQuery' to add the full-text leg, and 'expand' to add the graph leg; with neither, \
            this returns a plain vector search and reports fused=false. Graph expansion is rank-only, so \
            it requires fusionStrategy RRF. Depth is capped at 3 hops server-side. Each result names the \
            legs it came from in 'sources', and expansion-sourced results carry 'depth' and the 'path' \
            back to their seed. Embedding generation is not performed by ArcadeDB.""")
        .put("inputSchema", new JSONObject()
            .put("type", "object")
            .put("properties", new JSONObject()
                .put("database", new JSONObject()
                    .put("type", "string")
                    .put("description", "The name of the database to search"))
                .put("vectorIndexName", new JSONObject()
                    .put("type", "string")
                    .put("description", "Name of an LSM_VECTOR or LSM_SPARSE_VECTOR index"))
                .put("queryVector", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "number"))
                    .put("description",
                        "Dense query vector, or sparse weights corresponding to queryIndices when sparse=true"))
                .put("queryIndices", new JSONObject()
                    .put("type", "array")
                    .put("items", new JSONObject().put("type", "integer").put("minimum", 0))
                    .put("description",
                        "Sparse dimension ids corresponding to queryVector weights; omit to use queryVector positions"))
                .put("k", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("maximum", MCPVectorLeg.MAX_K)
                    .put("default", MCPVectorLeg.DEFAULT_K)
                    .put("description", "Maximum number of results to return after fusion"))
                .put("efSearch", new JSONObject()
                    .put("type", "integer")
                    .put("minimum", 1)
                    .put("description", "Dense-index search beam width; higher values improve recall at higher cost"))
                .put("filter", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Optional read-only SQL WHERE predicate applied to the vector leg's bounded candidate set"))
                .put("sparse", new JSONObject()
                    .put("type", "boolean")
                    .put("default", false)
                    .put("description", "Use vector.sparseNeighbors against an LSM_SPARSE_VECTOR index"))
                .put("fulltextIndexName", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Name of the full-text index, e.g. 'Article[content]'. Must be given together with fulltextQuery"))
                .put("fulltextQuery", new JSONObject()
                    .put("type", "string")
                    .put("description",
                        "Full-text query for the second leg. Must be given together with fulltextIndexName"))
                .put("expand", new JSONObject()
                    .put("type", "object")
                    .put("description", "Graph expansion leg, seeded from the records the other legs found")
                    .put("properties", new JSONObject()
                        .put("edgeTypes", new JSONObject()
                            .put("type", "array")
                            .put("items", new JSONObject().put("type", "string"))
                            .put("description", "Edge types to traverse; omit to traverse every edge type"))
                        .put("direction", new JSONObject()
                            .put("type", "string")
                            .put("enum", new JSONArray().put("out").put("in").put("both"))
                            .put("default", "out")
                            .put("description", "Traversal direction"))
                        .put("maxDepth", new JSONObject()
                            .put("type", "integer")
                            .put("minimum", 1)
                            .put("maximum", MAX_DEPTH)
                            .put("default", 1)
                            .put("description", "Hops to walk; capped at " + MAX_DEPTH + " server-side"))))
                .put("fusionStrategy", new JSONObject()
                    .put("type", "string")
                    .put("enum", new JSONArray().put("RRF").put("DBSF").put("LINEAR"))
                    .put("default", "RRF")
                    .put("description",
                        "Fusion strategy. DBSF and LINEAR need a score on every row, so neither can be combined "
                            + "with the rank-only expansion leg"))
                .put("weights", new JSONObject()
                    .put("type", "object")
                    .put("description",
                        "Per-leg fusion weights. The expansion leg defaults to 0.5 because an arbitrary neighbor "
                            + "should not outrank a direct match")
                    .put("properties", new JSONObject()
                        .put("vector", new JSONObject().put("type", "number").put("default", 1.0))
                        .put("fulltext", new JSONObject().put("type", "number").put("default", 1.0))
                        .put("expand", new JSONObject().put("type", "number").put("default", 0.5)))))
            .put("required", new JSONArray().put("database").put("vectorIndexName").put("queryVector").put("k")));
  }

  public static JSONObject execute(final ArcadeDBServer server, final ServerSecurityUser user, final JSONObject args,
      final MCPConfiguration config) {
    if (!config.isAllowReads())
      throw new SecurityException("Read operations are not allowed by MCP configuration");

    final String databaseName = MCPToolUtils.requireString(args, "database");
    final int k = args.getInt("k", MCPVectorLeg.DEFAULT_K);
    if (k < 1 || k > MCPVectorLeg.MAX_K)
      throw new IllegalArgumentException("'k' must be between 1 and " + MCPVectorLeg.MAX_K);

    // Argument faults are reported before the database is resolved, so a malformed request reads the
    // same whether or not the database also resolves.
    MCPVectorLeg.validateArguments(args, "vectorIndexName");

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    final MCPVectorLeg.VectorLegQuery vectorQuery = MCPVectorLeg.build(database, args, "vectorIndexName", legLimit(k));
    final List<LegRow> vectorLeg = runVectorLeg(database, vectorQuery);

    final JSONObject legs = new JSONObject()
        .put("vector", new JSONObject().put("count", vectorLeg.size()));

    final String strategy = strategyOf(args);
    // The expansion leg ranks by traversal order and carries no score, which the score-normalizing
    // strategies cannot consume. Rejecting here names the conflict; letting it through would surface
    // as a parse-level complaint about a source the caller never wrote.
    if (args.getJSONObject("expand", null) != null && !"RRF".equals(strategy))
      throw new IllegalArgumentException("fusionStrategy " + strategy + " needs a score on every row, but the graph "
          + "expansion leg is ranked by traversal order and has none. Use RRF, or drop 'expand'.");
    final List<LegRow> fullTextLeg = runFullTextLeg(database, args, legLimit(k), legs);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final List<Leg> legList = new ArrayList<>(3);
    legList.add(new Leg("vector", vectorLeg, weightOf(args, "vector", 1.0f)));
    if (!fullTextLeg.isEmpty())
      legList.add(new Leg("fulltext", fullTextLeg, weightOf(args, "fulltext", 1.0f)));

    final JSONObject expandArgs = args.getJSONObject("expand", null);
    Map<RID, ExpansionInfo> expansionInfo = Map.of();
    if (expandArgs != null) {
      requireVertexType(database, vectorQuery.index().typeIndex().getTypeName());
      final Expansion expansion = runExpansionLeg(database, expandArgs, collectSeeds(vectorLeg, fullTextLeg));
      expansionInfo = expansion.info();
      legs.put("expand", new JSONObject()
          .put("direction", expandArgs.getString("direction", "out"))
          .put("edgeTypes", expandArgs.getJSONArray("edgeTypes", new JSONArray()))
          .put("maxDepth", expandArgs.getInt("maxDepth", 1))
          .put("truncated", expansion.truncated())
          .put("count", expansion.rows().size()));
      if (!expansion.rows().isEmpty())
        legList.add(new Leg("expand", expansion.rows(), weightOf(args, "expand", 0.5f)));
    }

    final JSONObject response = new JSONObject()
        .put("vectorIndexName", vectorQuery.index().typeIndex().getName())
        .put("sparse", vectorQuery.sparse())
        .put("scoring", vectorQuery.index().scoring())
        .put("legs", legs);

    final JSONArray results;
    if (legList.size() < 2) {
      // Fusion needs at least two sources. Rather than fabricate a fused score from one leg, report the
      // leg's own score under its native key and say plainly that no fusion happened.
      results = new JSONArray();
      for (final LegRow row : vectorLeg) {
        if (results.length() >= k)
          break;
        final Document document = lookup(database, row.rid());
        if (document == null)
          continue;
        results.put(new JSONObject()
            .put("rid", row.rid().toString())
            .put(vectorQuery.sparse() ? "score" : "distance", row.score())
            .put("sources", new JSONArray().put("vector"))
            .put("properties", serializer.serializeDocument(document)));
      }
      response.put("fused", false);
    } else {
      results = fuse(database, legList, strategy, k, serializer, expansionInfo);
      response.put("fused", true).put("fusionStrategy", strategy);
      if (legs.has("fulltext"))
        response.put("fulltextIndexName", legs.getJSONObject("fulltext").getString("indexName"));
    }

    return response
        .put("truncated", results.length() >= k)
        .put("count", results.length())
        .put("results", results);
  }

  /**
   * Rows each retrieval leg fetches before fusion. Fusing lists that are only as long as the requested
   * result count would leave fusion nothing to reorder, so each leg over-fetches.
   */
  public static int legLimit(final int k) {
    return (int) Math.min((long) k * LEG_OVERFETCH, MAX_LEG_CANDIDATES);
  }

  /**
   * Result of the graph expansion leg: the ranked rows in breadth-first discovery order, the depth
   * and path for each of them, and whether the fan-out cap cut the walk short.
   */
  public record Expansion(List<LegRow> rows, Map<RID, ExpansionInfo> info, boolean truncated) {
  }

  /**
   * Walks outward from the seeds and returns the nodes found, ranked by breadth-first discovery order.
   * <p>
   * The rows carry no score. Breadth-first order already encodes "closer to a retrieved record ranks
   * higher", which is the only ranking signal a traversal produces, and inventing a numeric score for
   * it would put a second scoring model next to the engine's.
   * <p>
   * Seeds are dropped from the result: each is already ranked by the leg that found it, and letting it
   * rank again here would count its own presence twice.
   */
  private static Expansion runExpansionLeg(final Database database, final JSONObject expandArgs,
      final List<RID> seeds) {
    final int maxDepth = expandArgs.getInt("maxDepth", 1);
    if (maxDepth < 1 || maxDepth > MAX_DEPTH)
      throw new IllegalArgumentException(
          "expand.maxDepth must be between 1 and " + MAX_DEPTH + ", got " + maxDepth);

    final String direction = expandArgs.getString("direction", "out");
    if (!"out".equals(direction) && !"in".equals(direction) && !"both".equals(direction))
      throw new IllegalArgumentException(
          "expand.direction must be one of out, in, both, got '" + direction + "'");

    final List<String> edgeTypes = validatedEdgeTypes(database, expandArgs.getJSONArray("edgeTypes", null));

    if (seeds.isEmpty())
      return new Expansion(List.of(), Map.of(), false);

    final StringBuilder sql = new StringBuilder(
        "SELECT @rid AS rid, $depth AS depth, $path AS path FROM (TRAVERSE ");
    sql.append(direction).append('(');
    for (int i = 0; i < edgeTypes.size(); i++) {
      if (i > 0)
        sql.append(", ");
      sql.append('\'').append(edgeTypes.get(i)).append('\'');
    }
    sql.append(") FROM [");
    for (int i = 0; i < seeds.size(); i++) {
      if (i > 0)
        sql.append(',');
      sql.append(seeds.get(i).toString());
    }
    // The LIMIT applies inside the traversal, before the outer depth filter removes the seeds, so the
    // seeds consume slots and the budget must cover them or the last expanded rows are lost.
    sql.append("] MAXDEPTH ").append(maxDepth)
        .append(" LIMIT ").append(seeds.size() + MAX_EXPANSION)
        .append(" STRATEGY BREADTH_FIRST) WHERE $depth > 0");

    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(sql.toString());
    } catch (final RuntimeException e) {
      throw invalidExpression("expansion leg", e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated graph expansion is not read-only");

    final List<LegRow> rows = new ArrayList<>();
    final Map<RID, ExpansionInfo> info = new HashMap<>();
    final Set<RID> seedSet = new HashSet<>(seeds);
    try {
      final ResultSet analyzedResultSet = analyzed.execute(Map.of());
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", sql.toString())) {
        while (resultSet.hasNext() && rows.size() < MAX_EXPANSION) {
          final Result row = resultSet.next();
          final RID rid = MCPVectorLeg.toRID(row.getProperty("rid"));
          if (rid == null || seedSet.contains(rid) || info.containsKey(rid))
            continue;
          if (!(row.getProperty("depth") instanceof final Number depth))
            continue;
          rows.add(new LegRow(rid, null));
          info.put(rid, new ExpansionInfo(depth.intValue(), readPath(row.getProperty("path"), rid)));
        }
        return new Expansion(rows, info, rows.size() >= MAX_EXPANSION && resultSet.hasNext());
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression("expansion leg", e);
    }
  }

  /**
   * Reads the traversal's path metadata. Falls back to the row's own RID when the path is missing, so
   * a row always reports where it is even if the engine did not carry how it was reached.
   */
  private static List<RID> readPath(final Object raw, final RID rid) {
    if (!(raw instanceof final List<?> steps))
      return List.of(rid);
    final List<RID> path = new ArrayList<>(steps.size());
    for (final Object step : steps) {
      final RID stepRid = MCPVectorLeg.toRID(step);
      if (stepRid != null)
        path.add(stepRid);
    }
    return path.isEmpty() ? List.of(rid) : path;
  }

  /**
   * Validates every requested edge type against the schema. An unknown name is rejected rather than
   * passed through, because a traversal over a name that matches nothing returns an empty neighborhood
   * and raises no error: an unvalidated typo would silently downgrade the search with no way for the
   * caller to notice.
   */
  private static List<String> validatedEdgeTypes(final Database database, final JSONArray requested) {
    if (requested == null || requested.length() == 0)
      return List.of();

    final List<String> names = new ArrayList<>(requested.length());
    for (int i = 0; i < requested.length(); i++) {
      final Object raw = requested.get(i);
      if (!(raw instanceof final String name) || name.isBlank())
        throw new IllegalArgumentException("expand.edgeTypes must contain only non-blank type names");
      if (name.indexOf('\'') >= 0 || name.indexOf('`') >= 0 || name.indexOf('\\') >= 0)
        throw new IllegalArgumentException(
            "expand.edgeTypes entry '" + name + "' contains a quote or backslash, which is not supported");
      if (!database.getSchema().existsType(name) || !(database.getSchema().getType(name) instanceof EdgeType))
        throw new IllegalArgumentException("Edge type '" + name + "' does not exist. "
            + describeAvailableEdgeTypes(database));
      names.add(name);
    }
    return names;
  }

  private static String describeAvailableEdgeTypes(final Database database) {
    final Set<String> names = new TreeSet<>();
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof EdgeType)
        names.add(type.getName());
    return "Available edge types: " + names;
  }

  /**
   * Picks the records the expansion walks out from: every retrieval hit, in rank order, vector leg
   * first, capped so a wide retrieval cannot turn into an unbounded traversal.
   */
  private static List<RID> collectSeeds(final List<LegRow> vectorLeg, final List<LegRow> fullTextLeg) {
    final Set<RID> seeds = new LinkedHashSet<>();
    for (final LegRow row : vectorLeg) {
      if (seeds.size() >= MAX_SEEDS)
        break;
      seeds.add(row.rid());
    }
    for (final LegRow row : fullTextLeg) {
      if (seeds.size() >= MAX_SEEDS)
        break;
      seeds.add(row.rid());
    }
    return new ArrayList<>(seeds);
  }

  /**
   * Graph expansion is only meaningful over vertices. Checking the index's type once gives one clear
   * error instead of a silently empty neighborhood.
   */
  private static void requireVertexType(final Database database, final String typeName) {
    if (!database.getSchema().existsType(typeName)
        || !(database.getSchema().getType(typeName) instanceof VertexType))
      throw new IllegalArgumentException("Graph expansion requires a vertex type, but '" + typeName
          + "' is not one. Drop 'expand', or search an index declared on a vertex type.");
  }

  private static List<LegRow> runVectorLeg(final Database database, final MCPVectorLeg.VectorLegQuery query) {
    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(query.sql());
    } catch (final RuntimeException e) {
      throw invalidExpression("vector leg", e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated vector leg is not read-only");

    final List<LegRow> rows = new ArrayList<>();
    final Set<RID> seen = new LinkedHashSet<>();
    try {
      final ResultSet analyzedResultSet = analyzed.execute(query.parameters());
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", query.sql(), query.parameters())) {
        while (resultSet.hasNext()) {
          final Result row = resultSet.next();
          RID rid = MCPVectorLeg.toRID(row.getProperty("@rid"));
          if (rid == null)
            rid = row.getIdentity().orElse(null);
          if (rid == null || !seen.add(rid))
            continue;
          final Object raw = row.getProperty(query.sparse() ? "score" : "distance");
          if (!(raw instanceof final Number score))
            continue;
          rows.add(new LegRow(rid, score.doubleValue()));
        }
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression("vector leg", e);
    }
    return rows;
  }

  private static List<LegRow> runFullTextLeg(final Database database, final JSONObject args, final int limit,
      final JSONObject legs) {
    final String indexName = args.getString("fulltextIndexName", null);
    final String queryText = args.getString("fulltextQuery", null);
    final boolean hasIndex = indexName != null && !indexName.isBlank();
    final boolean hasQuery = queryText != null && !queryText.isBlank();

    if (!hasIndex && !hasQuery)
      return List.of();
    // Half a leg is always a mistake, and silently dropping it would return a plausible-looking result
    // set that quietly ignored what the caller asked for.
    if (hasIndex != hasQuery)
      throw new IllegalArgumentException(
          "'fulltextIndexName' and 'fulltextQuery' must be supplied together. Give both to add the full-text leg, "
              + "or neither to search without it.");

    final TypeIndex typeIndex;
    try {
      typeIndex = FullTextSearch.resolveFullTextIndex(database, indexName);
    } catch (final SchemaException e) {
      throw new IllegalArgumentException("Full-text index '" + indexName + "' does not exist. Available full-text "
          + "indexes in '" + database.getName() + "': " + FullTextSearch.listFullTextIndexes(database), e);
    } catch (final CommandExecutionException e) {
      throw new IllegalArgumentException("Index '" + indexName + "' is not a full-text index. Available full-text "
          + "indexes in '" + database.getName() + "': " + FullTextSearch.listFullTextIndexes(database), e);
    }

    final Map<RID, Float> hits = FullTextSearch.search(typeIndex, queryText, limit);
    final List<Map.Entry<RID, Float>> ranked = new ArrayList<>(hits.entrySet());
    // Score descending, tie-broken by RID so tied hits rank deterministically rather than by hash order.
    ranked.sort(Map.Entry.<RID, Float>comparingByValue().reversed().thenComparing(Map.Entry::getKey));

    final List<LegRow> rows = new ArrayList<>(ranked.size());
    for (final Map.Entry<RID, Float> hit : ranked)
      rows.add(new LegRow(hit.getKey(), hit.getValue().doubleValue()));

    legs.put("fulltext", new JSONObject()
        .put("indexName", typeIndex.getName())
        .put("similarity", FullTextSearch.getSimilarity(typeIndex))
        .put("count", rows.size()));
    return rows;
  }

  private static float weightOf(final JSONObject args, final String legName, final float fallback) {
    final JSONObject weights = args.getJSONObject("weights", null);
    if (weights == null)
      return fallback;
    final double value = weights.getDouble(legName, fallback);
    if (!Double.isFinite(value) || value < 0.0)
      throw new IllegalArgumentException("weights." + legName + " must be a finite number that is not negative");
    return (float) value;
  }

  /**
   * Hands the materialized legs to the engine's fusion function and shapes its output. Fusion scoring
   * lives entirely in {@code vector.fuse}; nothing here re-derives a rank contribution.
   */
  private static JSONArray fuse(final Database database, final List<Leg> legList, final String strategy, final int k,
      final JsonSerializer serializer, final Map<RID, ExpansionInfo> expansion) {
    final Map<String, Object> parameters = new LinkedHashMap<>();
    final StringBuilder sql = new StringBuilder("SELECT expand(`vector.fuse`(");
    final List<Float> weights = new ArrayList<>(legList.size());

    for (int i = 0; i < legList.size(); i++) {
      final Leg leg = legList.get(i);
      final List<Map<String, Object>> rows = new ArrayList<>(leg.rows().size());
      for (final LegRow row : leg.rows()) {
        final Map<String, Object> entry = new LinkedHashMap<>(2);
        // The RID must go in as a RID: the fusion function reads @rid as a RID or a record reference
        // and silently drops any row whose @rid is a string.
        entry.put("@rid", row.rid());
        if (row.score() != null)
          entry.put("score", row.score());
        rows.add(entry);
      }
      final String name = "l" + i;
      parameters.put(name, rows);
      weights.add(leg.weight());
      if (i > 0)
        sql.append(", ");
      sql.append(':').append(name);
    }

    final Map<String, Object> options = new LinkedHashMap<>();
    options.put("fusion", strategy);
    options.put("weights", weights);
    options.put("limit", k);
    parameters.put("opts", options);
    sql.append(", :opts))");

    final Map<RID, Set<String>> sources = new HashMap<>();
    for (final Leg leg : legList)
      for (final LegRow row : leg.rows())
        sources.computeIfAbsent(row.rid(), r -> new LinkedHashSet<>()).add(leg.name());

    final QueryEngine.AnalyzedQuery analyzed;
    try {
      analyzed = database.getQueryEngine("sql").analyze(sql.toString());
    } catch (final RuntimeException e) {
      throw invalidExpression("fusion", e);
    }
    if (!analyzed.isIdempotent())
      throw new SecurityException("Generated hybrid fusion is not read-only");

    final JSONArray results = new JSONArray();
    try {
      final ResultSet analyzedResultSet = analyzed.execute(parameters);
      try (final ResultSet resultSet = analyzedResultSet != null
          ? analyzedResultSet
          : database.query("sql", sql.toString(), parameters)) {
        while (resultSet.hasNext() && results.length() < k) {
          final Result row = resultSet.next();
          final RID rid = MCPVectorLeg.toRID(row.getProperty("@rid"));
          if (rid == null)
            continue;
          if (!(row.getProperty("score") instanceof final Number score))
            continue;
          final Object record = row.getProperty("record");
          final Document document = record instanceof final Document candidate ? candidate : lookup(database, rid);
          if (document == null)
            continue;

          final JSONArray sourceNames = new JSONArray();
          for (final String name : sources.getOrDefault(rid, Set.of()))
            sourceNames.put(name);

          final JSONObject result = new JSONObject()
              .put("rid", rid.toString())
              .put("fusedScore", score)
              .put("sources", sourceNames)
              .put("properties", serializer.serializeDocument(document));

          final ExpansionInfo info = expansion.get(rid);
          if (info != null) {
            result.put("depth", info.depth());
            final JSONArray path = new JSONArray();
            for (final RID step : info.path())
              path.put(step.toString());
            result.put("path", path);
          }
          results.put(result);
        }
      }
    } catch (final SecurityException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw invalidExpression("fusion", e);
    }
    return results;
  }

  private static String strategyOf(final JSONObject args) {
    final String raw = args.getString("fusionStrategy", "RRF");
    final String strategy = raw.toUpperCase(Locale.ROOT);
    if (!"RRF".equals(strategy) && !"DBSF".equals(strategy) && !"LINEAR".equals(strategy))
      throw new IllegalArgumentException(
          "Unknown fusionStrategy '" + raw + "'. Allowed: RRF, DBSF, LINEAR");
    return strategy;
  }

  private static Document lookup(final Database database, final RID rid) {
    try {
      return database.lookupByRID(rid, true) instanceof final Document document ? document : null;
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  private static IllegalArgumentException invalidExpression(final String stage, final RuntimeException cause) {
    final String detail = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
    return new IllegalArgumentException("Invalid hybrid search " + stage + ": " + detail, cause);
  }
}
