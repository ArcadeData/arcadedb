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
package com.arcadedb.mcp.tools;

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
import com.arcadedb.mcp.MCPConfiguration;
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
   * One fusion source: its display name, its rows in rank order, the weight applied to every rank
   * contribution it makes, and the key its score must be emitted under. The engine reads a value
   * under 'score' as a similarity and only sign-flips one read under 'distance', so a leg whose
   * native value is a distance must say so or its ranking inverts under the score-normalizing
   * strategies.
   */
  public record Leg(String name, List<LegRow> rows, float weight, String scoreKey) {
  }

  /**
   * Provenance for a row the expansion leg contributed: how many hops from its seed, and the RID
   * path back to that seed including the seed itself.
   */
  public record ExpansionInfo(int depth, List<RID> path) {
  }

  /** The full-text leg's ranked rows together with the type its index is declared on. */
  private record FullTextLeg(List<LegRow> rows, String typeName) {
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
                            + "should not outrank a direct match. A weight naming a leg the request does not ask for "
                            + "is rejected; a weight for a leg that runs but matches nothing has no effect, and that "
                            + "leg's zero row count is reported under 'legs'.")
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
    // same whether or not the database also resolves, and a rejected request does no I/O.
    MCPVectorLeg.validateArguments(args, "vectorIndexName");

    final String strategy = strategyOf(args);
    final JSONObject expandArgs = requireExpandObject(args);
    // The expansion leg ranks by traversal order and carries no score, which the score-normalizing
    // strategies cannot consume. Rejecting here names the conflict; letting it through would surface
    // as a parse-level complaint about a source the caller never wrote.
    if (expandArgs != null && !"RRF".equals(strategy))
      throw new IllegalArgumentException("fusionStrategy " + strategy + " needs a score on every row, but the graph "
          + "expansion leg is ranked by traversal order and has none. Use RRF, or drop 'expand'.");
    // The remaining expand and weights checks need no schema, so they are reported here too, before
    // the vector leg spends a search on a request that was going to be rejected anyway.
    if (expandArgs != null)
      validateExpand(expandArgs);
    validateWeights(args);

    final MCPToolUtils.DatabaseAccess access = MCPToolUtils.resolveDatabase(
        server, user, databaseName, config, MCPToolUtils.RequiredAccess.READ);
    final Database database = access.database();

    final MCPVectorLeg.VectorLegQuery vectorQuery = MCPVectorLeg.build(database, args, "vectorIndexName", legLimit(k));
    final List<LegRow> vectorLeg = runVectorLeg(database, vectorQuery);

    final JSONObject legs = new JSONObject()
        .put("vector", new JSONObject().put("count", vectorLeg.size()));

    final FullTextLeg fullTextLeg = runFullTextLeg(database, args, legLimit(k), legs);

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer()
        .setIncludeVertexEdges(false)
        .setUseCollectionSize(false)
        .setUseCollectionSizeForEdges(false);

    final List<Leg> legList = new ArrayList<>(3);
    legList.add(new Leg("vector", vectorLeg, weightOf(args, "vector", 1.0f), vectorQuery.sparse() ? "score" : "distance"));
    if (!fullTextLeg.rows().isEmpty())
      legList.add(new Leg("fulltext", fullTextLeg.rows(), weightOf(args, "fulltext", 1.0f), "score"));

    Map<RID, ExpansionInfo> expansionInfo = Map.of();
    if (expandArgs != null) {
      requireVertexType(database, vectorQuery.index().typeIndex().getTypeName());
      if (!fullTextLeg.rows().isEmpty())
        requireVertexType(database, fullTextLeg.typeName());
      final List<RID> seeds = collectSeeds(vectorLeg, fullTextLeg.rows());
      final Expansion expansion = runExpansionLeg(database, expandArgs, seeds);
      expansionInfo = expansion.info();
      legs.put("expand", new JSONObject()
          .put("direction", expandArgs.getString("direction", "out"))
          .put("edgeTypes", expandArgs.getJSONArray("edgeTypes", new JSONArray()))
          .put("maxDepth", expandArgs.getInt("maxDepth", 1))
          .put("truncated", expansion.truncated())
          // The seed budget is reported alongside the fan-out budget. A thin neighborhood otherwise
          // reads the same whether the graph was sparse or the retrieval legs simply out-ran the cap.
          .put("seedCount", seeds.size())
          .put("seedsTruncated", seeds.size() >= MAX_SEEDS)
          .put("count", expansion.rows().size()));
      if (!expansion.rows().isEmpty())
        legList.add(new Leg("expand", expansion.rows(), weightOf(args, "expand", 0.5f), "score"));
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
    }

    // The index name is reported whenever the full-text leg ran, including when it matched nothing and
    // so could not become a fusion source. A caller comparing a fused and an unfused response would
    // otherwise see the field appear and disappear for a reason unrelated to which index was searched.
    if (legs.has("fulltext"))
      response.put("fulltextIndexName", legs.getJSONObject("fulltext").getString("indexName"));

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
   * Reads 'expand' as a JSON object, or null when the caller omitted it. A present but non-object
   * value (e.g. an array) would otherwise reach the underlying JSON library as an unguarded cast and
   * surface as an opaque parser error instead of naming the argument that is wrong.
   */
  private static JSONObject requireExpandObject(final JSONObject args) {
    final Object raw = args.opt("expand");
    if (raw == null)
      return null;
    if (!(raw instanceof final JSONObject expand))
      throw new IllegalArgumentException("'expand' must be an object with optional edgeTypes, direction, and maxDepth");
    return expand;
  }

  /**
   * Validates the expand fields that need no schema access: maxDepth within the server cap and a
   * recognized direction. The single source both the early, pre-database check and the leg itself
   * call, so the rule can never drift between the two call sites.
   */
  private static void validateExpand(final JSONObject expandArgs) {
    final int maxDepth = expandArgs.getInt("maxDepth", 1);
    if (maxDepth < 1 || maxDepth > MAX_DEPTH)
      throw new IllegalArgumentException(
          "expand.maxDepth must be between 1 and " + MAX_DEPTH + ", got " + maxDepth);

    final String direction = expandArgs.getString("direction", "out");
    if (!"out".equals(direction) && !"in".equals(direction) && !"both".equals(direction))
      throw new IllegalArgumentException(
          "expand.direction must be one of out, in, both, got '" + direction + "'");
  }

  /**
   * Validates the weights map before any I/O: every key must name one of the three legs, and every
   * supplied value must be finite and not negative. An unrecognized key would otherwise be silently
   * ignored and the request would run at default weights with no sign the caller's override never
   * took effect.
   */
  private static void validateWeights(final JSONObject args) {
    final JSONObject weights = args.getJSONObject("weights", null);
    if (weights == null)
      return;
    for (final String key : weights.keySet())
      if (!"vector".equals(key) && !"fulltext".equals(key) && !"expand".equals(key))
        throw new IllegalArgumentException("Unknown weights key '" + key + "'. Allowed: vector, fulltext, expand");

    // A weight for a leg the request never asks for is rejected rather than ignored. It is read only
    // when its leg becomes a fusion source, so accepting it would leave the caller believing an
    // override took effect that nothing ever consumed - the same silent no-op an unknown key would be.
    // The full-text leg counts as requested when either of its arguments is present, so an incomplete
    // leg still reports the more specific error naming both fields.
    if (weights.has("fulltext") && !args.has("fulltextIndexName") && !args.has("fulltextQuery"))
      throw new IllegalArgumentException(
          "weights.fulltext was supplied but the request has no full-text leg. Add 'fulltextIndexName' and "
              + "'fulltextQuery', or drop the weight.");
    if (weights.has("expand") && !args.has("expand"))
      throw new IllegalArgumentException(
          "weights.expand was supplied but the request has no graph expansion leg. Add 'expand', or drop the weight.");
    for (final String legName : List.of("vector", "fulltext", "expand")) {
      if (!weights.has(legName))
        continue;
      // The type is checked before the value is read: reading a non-numeric weight straight through
      // surfaces the JSON layer's own parse failure, which names neither the argument nor the rule.
      if (!(weights.opt(legName) instanceof final Number number) || !Double.isFinite(number.doubleValue())
          || number.doubleValue() < 0.0)
        throw new IllegalArgumentException("weights." + legName + " must be a finite number that is not negative");
    }
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
    validateExpand(expandArgs);
    final int maxDepth = expandArgs.getInt("maxDepth", 1);
    final String direction = expandArgs.getString("direction", "out");
    final List<String> edgeTypes = validatedEdgeTypes(database, expandArgs.getJSONArray("edgeTypes", null));

    if (seeds.isEmpty())
      return new Expansion(List.of(), Map.of(), false);

    final StringBuilder sql = new StringBuilder(
        "SELECT @rid AS rid, $depth AS depth, $path AS path FROM (TRAVERSE ");
    sql.append(direction).append('(');
    // TRUST BOUNDARY: these names are the only caller-supplied text in this statement, and they are
    // safe to inline solely because validatedEdgeTypes rejected anything that is not an existing edge
    // type or that carries a quote or backslash. Weakening that method reopens injection here.
    for (int i = 0; i < edgeTypes.size(); i++) {
      if (i > 0)
        sql.append(", ");
      sql.append('\'').append(edgeTypes.get(i)).append('\'');
    }
    sql.append(") FROM [");
    // Seed RIDs are engine-derived, never caller text: they come from the retrieval legs' own result
    // rows via MCPVectorLeg.toRID, and RID.toString renders only #<bucket>:<position>.
    for (int i = 0; i < seeds.size(); i++) {
      if (i > 0)
        sql.append(',');
      sql.append(seeds.get(i).toString());
    }
    // The LIMIT applies inside the traversal, before the outer depth filter removes the seeds, so the
    // budget must cover the seeds that filter discards or the last expanded rows are lost.
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
        // Truncation reports a filled window, not a probe of what lies beyond it: rows the loop skips
        // as duplicates or seeds still consume the stream, so asking whether anything remains can
        // report false at the boundary. A filled window is the only state in which more neighbors may
        // exist, which is the same convention the top-level flag and vector_search already use.
        return new Expansion(rows, info, rows.size() >= MAX_EXPANSION);
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
   * Picks the records the expansion walks out from. The two retrieval legs are interleaved rather
   * than concatenated: at a large enough k the vector leg alone would otherwise fill the seed cap
   * and the full-text leg would never seed a traversal at all.
   */
  static List<RID> collectSeeds(final List<LegRow> vectorLeg, final List<LegRow> fullTextLeg) {
    final Set<RID> seeds = new LinkedHashSet<>();
    final int longest = Math.max(vectorLeg.size(), fullTextLeg.size());
    for (int i = 0; i < longest && seeds.size() < MAX_SEEDS; i++) {
      if (i < vectorLeg.size())
        seeds.add(vectorLeg.get(i).rid());
      if (seeds.size() < MAX_SEEDS && i < fullTextLeg.size())
        seeds.add(fullTextLeg.get(i).rid());
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

  private static FullTextLeg runFullTextLeg(final Database database, final JSONObject args, final int limit,
      final JSONObject legs) {
    final String indexName = args.getString("fulltextIndexName", null);
    final String queryText = args.getString("fulltextQuery", null);
    final boolean hasIndex = indexName != null && !indexName.isBlank();
    final boolean hasQuery = queryText != null && !queryText.isBlank();

    if (!hasIndex && !hasQuery)
      return new FullTextLeg(List.of(), null);
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
    return new FullTextLeg(rows, typeIndex.getTypeName());
  }

  /**
   * Reads a leg's weight. {@link #validateWeights} has already rejected an unknown key or an invalid
   * value by the time this runs, so this is a plain lookup.
   */
  private static float weightOf(final JSONObject args, final String legName, final float fallback) {
    final JSONObject weights = args.getJSONObject("weights", null);
    if (weights == null)
      return fallback;
    return (float) weights.getDouble(legName, fallback);
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
          entry.put(leg.scoreKey(), row.score());
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
