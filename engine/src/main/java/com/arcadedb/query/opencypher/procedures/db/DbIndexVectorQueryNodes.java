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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.sql.FunctionOptions;
import com.arcadedb.function.sql.vector.RecordPredicateRidSet;
import com.arcadedb.function.sql.vector.SQLFunctionVectorAbstract;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.NumberUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RidHashSet;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: db.index.vector.queryNodes(indexName, k, vector[, options])
 * <p>
 * Performs an approximate nearest neighbor search using a vector index.
 * This provides compatibility with Neo4j's vector search API.
 * </p>
 * <p>
 * The index can be specified as:
 * <ul>
 *   <li>ArcadeDB format: {@code 'Type[property]'} (e.g., {@code 'Document[embedding]'})</li>
 *   <li>Index name: the name of the vector index as returned by the schema</li>
 * </ul>
 * </p>
 * <p>
 * Example (Neo4j-compatible):
 * <pre>
 * CALL db.index.vector.queryNodes('Document[embedding]', 10, $vec)
 * YIELD node, score
 * RETURN node.title AS title, score
 * </pre>
 * </p>
 * <p>
 * The three-argument form has no filter: a {@code WHERE} after the {@code YIELD} applies to the k rows the search answered,
 * so when the nearest neighbours belong to records it rejects, fewer than k rows come back, or none. The optional fourth
 * argument, an ArcadeDB extension, is the pre-filter {@code vector.neighbors} takes (issue #10685): the candidates are
 * restricted during the search, and k rows that satisfy it are answered whenever the index holds k of them.
 * <ul>
 *   <li>{@code where}: a map of property to value ({@code {tenant: $tenant}}) or a SQL condition that may use named
 *   parameters ({@code 'tenant = :tenant'})</li>
 *   <li>{@code filter}: a list of nodes or RIDs the search may answer</li>
 * </ul>
 * Both together answer only the records that satisfy both:
 * <pre>
 * CALL db.index.vector.queryNodes('Document[embedding]', 10, $vec, {where: {tenant: $tenant}})
 * YIELD node, score
 * RETURN node.title AS title, score
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DbIndexVectorQueryNodes implements CypherProcedure {
  public static final String NAME = "db.index.vector.querynodes";
  /** {@link #NAME} is lower-cased for registry lookup; error messages quote the call the way the caller wrote it. */
  private static final String DISPLAY_NAME = "db.index.vector.queryNodes";
  private static final Set<String> OPTIONS = Set.of("where", "filter");

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 3;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getDescription() {
    return "Performs approximate nearest neighbor vector search using an HNSW index (Neo4j-compatible), optionally pre-filtered "
        + "by an options map {where: <map or condition>, filter: [<node or rid>, ...]}";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("node", "score");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    validateArgs(args);
    return search(args[0].toString(), args[1], args[2], args.length > 3 ? args[3] : null, null, context,
        "db.index.vector.queryNodes()").stream();
  }

  /**
   * The k-NN search behind both {@code db.index.vector.queryNodes} and the Cypher 25 {@code SEARCH} clause (issue #10776):
   * the {@code k} nearest live records of the index, nearest first, each as a row of {@code node} and {@code score}.
   *
   * @param indexSpec the index, as {@code 'Type[property]'} or as its schema name
   * @param kArg      the rows to answer, a number or a numeric string
   * @param vectorArg the query vector, or the key of the record whose vector it is
   * @param filter    a predicate every answered record must pass, tested by the index as it meets each candidate rather than
   *                  on its top k (issue #10782): the walk skips what it rejects and keeps expanding until it holds
   *                  {@code k} matches or has met every vector it can reach, so {@code k} matching rows come back whenever
   *                  the index holds them, however far they are. Each record is tested once per call. {@code null} for none
   * @param caller    the name of the entry point, for error messages and the work guard
   */
  public static List<Result> search(final String indexSpec, final Object kArg, final Object vectorArg,
      final Predicate<Document> filter, final CommandContext context, final String caller) {
    return search(indexSpec, kArg, vectorArg, null, filter, context, caller);
  }

  /**
   * The search with both pre-filters: the procedure's options map (issue #10685) and the {@code SEARCH} clause's predicate
   * (issue #10782). Each becomes the allow-list the indexes test every candidate against; given together, a candidate must
   * satisfy both.
   *
   * @param options the {@code {where, filter}} map of the procedure's fourth argument, {@code null} for none
   */
  private static List<Result> search(final String indexSpec, final Object kArg, Object vectorArg, final Object options,
      final Predicate<Document> filter, final CommandContext context, final String caller) {
    final int limit = extractLimit(kArg, caller);

    if (vectorArg == null)
      throw new CommandSQLParsingException("Vector parameter is null");
    if (vectorArg instanceof List<?> list)
      vectorArg = list.toArray();

    // Resolve the vector index
    final List<LSMVectorIndex> vectorIndexes = resolveVectorIndexes(indexSpec, context);

    // Resolved after the index, so a bad index spec stays loud. A filter that matched nothing is not "no filter": no record can
    // satisfy it (issue #8959)
    final Set<RID> optionRIDs = parseOptions(options, context);
    if (optionRIDs != null && optionRIDs.isEmpty())
      return List.of();

    // Get the query vector
    final float[] queryVector = extractQueryVector(vectorArg, vectorIndexes.getFirst(), context);

    // The filter as the indexes' allow-list (issue #10782), the plan the where option of vector.neighbors takes (#10169).
    // Skipping a rejected candidate here and widening the search instead stopped at MAX_FETCH_CANDIDATES, so a filter that
    // kept fewer than k of the 100,000 nearest answered short although the index held matches further away, and every
    // widening reloaded and re-tested the candidates of the pass before it
    final Set<RID> allowedRIDs;
    if (filter == null)
      allowedRIDs = optionRIDs;
    else if (optionRIDs == null)
      allowedRIDs = RecordPredicateRidSet.matching(filter, context);
    else
      allowedRIDs = RecordPredicateRidSet.matching(record -> optionRIDs.contains(record.getIdentity()) && filter.test(record),
          context);

    // A record deleted by a commit that overlaps the search can still be in an index's answer (issue #9858): it is skipped,
    // and the search widened until it answers k live rows, exactly as vector.neighbors does (#9368).
    // The command's deadline and the termination of its statement, honoured inside each search and while it waits for a
    // graph build (issue #9896)
    final WorkGuard guard = WorkGuard.forCommand(context, caller);
    return VectorUtils.searchWithStaleTopUp(limit,
        (fetchK, staleSkipped, saturated) -> searchAndLoad(vectorIndexes, queryVector, limit, fetchK, allowedRIDs, guard,
            staleSkipped, saturated));
  }

  /**
   * Reads the optional options map into the allow-list the index tests each candidate against, exactly as
   * {@code vector.neighbors} reads its own {@code where} and {@code filter} (issue #10685). {@code null} or an empty map is
   * the three-argument form.
   */
  private static Set<RID> parseOptions(final Object options, final CommandContext context) {
    if (options == null)
      return null;
    if (!(options instanceof Map<?, ?> map))
      throw new CommandSQLParsingException(
          DISPLAY_NAME + "(): options must be a map such as {where: {tenant: $tenant}}, got " + options.getClass().getSimpleName());
    final FunctionOptions opts = new FunctionOptions(DISPLAY_NAME, map, OPTIONS);
    return SQLFunctionVectorAbstract.parsePreFilter(opts.getList("filter"), opts.get("where"), DISPLAY_NAME, context);
  }

  /**
   * One pass of the search: asks every index for {@code fetchK} candidates and loads the nearest {@code limit} distinct live
   * records, counting in {@code staleSkipped} the candidates whose record no longer exists.
   */
  private static List<Result> searchAndLoad(final List<LSMVectorIndex> vectorIndexes, final float[] queryVector, final int limit,
      final int fetchK, final Set<RID> allowedRIDs, final WorkGuard guard, final int[] staleSkipped, final boolean[] saturated) {
    // Search across all indexes and collect results
    final List<Pair<RID, Float>> allNeighbors = new ArrayList<>();
    for (final LSMVectorIndex lsmIndex : vectorIndexes) {
      // The search vector.neighbors takes by default: a PRODUCT index walks on its PQ codes and re-ranks as its rerankOversample
      // says (issue #10514), any other quantization walks exactly
      final List<Pair<RID, Float>> neighbors = lsmIndex.findNeighbors(queryVector, fetchK, -1, -1, allowedRIDs, guard);
      allNeighbors.addAll(neighbors);
      if (neighbors.size() >= fetchK)
        saturated[0] = true;
    }

    // Sort by distance (ascending - closer is better)
    allNeighbors.sort(Comparator.comparing(Pair::getSecond));

    // Take top k results, converting distance to score (similarity).
    // EUCLIDEAN distance is L2² in [0, ∞); map it back to a (0, 1] similarity so callers receive a comparable score.
    final VectorSimilarityFunction similarityFunction = vectorIndexes.getFirst().getSimilarityFunction();
    final int resultCount = Math.min(limit, allNeighbors.size());
    final List<Result> results = new ArrayList<>(resultCount);

    // One node, one row: the Neo4j procedure this mirrors promises k rows carrying k DISTINCT nodes, and every
    // consumer taking the top k relies on it. Nothing upstream can promise it on its own - this list is the
    // concatenation of one search per resolved index, and a single record can be offered by more than one of them
    // (issue #7057). Deduplicating here, after the sort, keeps each node at its NEAREST distance and lets the walk
    // continue past a repeat, so a k that was silently halved comes back full instead of merely unduplicated.
    final RidHashSet emitted = new RidHashSet(resultCount);

    for (final Pair<RID, Float> neighbor : allNeighbors) {
      if (results.size() == resultCount)
        break;
      if (!emitted.add(neighbor.getFirst()))
        continue;
      // The indexes answer only what the allow-list accepted. Asserted rather than skipped (PR #10834 review): a search path
      // that ignored the allow-list would otherwise be hidden by a silent widening, and Surefire runs with -ea
      assert allowedRIDs == null || allowedRIDs.contains(neighbor.getFirst()) :
          "the vector index answered " + neighbor.getFirst() + ", which the pre-filter rejected";

      final Document record;
      try {
        record = neighbor.getFirst().asDocument();
      } catch (final RecordNotFoundException e) {
        // Deleted between the index search and this load: a candidate lost, not a failed query (issue #9858)
        staleSkipped[0]++;
        continue;
      }
      final float distance = neighbor.getSecond();

      final float score = toNeo4jScore(similarityFunction, distance);

      final ResultInternal r = new ResultInternal();
      r.setProperty("node", record);
      r.setProperty("score", score);
      results.add(r);
    }

    return results;
  }

  /**
   * Maps the index's distance back to the score Neo4j's vector index reports, in [0, 1], higher is closer (issue #8963):
   * {@code 1 / (1 + d^2)} for EUCLIDEAN (the distance is already the squared L2), {@code (1 + cosine) / 2} for COSINE
   * (the distance is {@code 1 - cosine}) and {@code (1 + dot) / 2} for DOT_PRODUCT (the distance is the negated
   * similarity). DOT_PRODUCT is documented to require unit vectors; the clamp keeps the score in range for data that is not.
   */
  static float toNeo4jScore(final VectorSimilarityFunction similarityFunction, final float distance) {
    return switch (similarityFunction) {
      case EUCLIDEAN -> 1.0f / (1.0f + distance);
      case COSINE -> Math.max(0.0f, Math.min(1.0f, 1.0f - distance / 2.0f));
      case DOT_PRODUCT -> Math.max(0.0f, Math.min(1.0f, -distance));
    };
  }

  /**
   * Narrows the {@code k} argument to an {@code int}, saturating at the int bounds instead of
   * failing, whether it arrives as a {@link Number} or as a numeric string.
   * <p>
   * A result-count bound has a sensible "as many as exist" reading, so a value above
   * {@link Integer#MAX_VALUE} saturates rather than being rejected. The string fallback used to go
   * through {@link Integer#parseInt}, which threw a bare {@code NumberFormatException} for exactly
   * the same magnitudes the {@link Number} branch next to it accepted - the two branches now agree.
   * </p>
   * <p>
   * A negative {@code k} has no such reading and is rejected by name, mirroring
   * {@code AbstractAlgoProcedure.extractCount()}. {@code LSMVectorIndex.findNeighborsFromVector}
   * clamps with {@code Math.min(k, candidateCount)}, which leaves a negative value negative, so
   * without this check it reaches {@code new ArrayList<>(k)} and surfaces as a bare
   * {@code IllegalArgumentException: Illegal Capacity} that never mentions {@code k}.
   * </p>
   */
  private static int extractLimit(final Object arg, final String caller) {
    if (arg == null)
      throw new CommandSQLParsingException(caller + ": k must be a number, got null");
    final int limit;
    if (arg instanceof Number n)
      limit = NumberUtils.saturateToInt(n);
    else {
      final String text = arg.toString().trim();
      try {
        limit = NumberUtils.saturateToInt(new BigDecimal(text));
      } catch (final NumberFormatException e) {
        throw new CommandSQLParsingException(caller + ": k must be a number, got '" + text + "'", e);
      }
    }

    if (limit < 0)
      throw new CommandSQLParsingException(caller + ": k must not be negative, got " + limit);

    return limit;
  }

  /**
   * Resolves the index specification to a list of LSMVectorIndex instances.
   * Supports both ArcadeDB format (Type[property]) and direct index name.
   */
  private static List<LSMVectorIndex> resolveVectorIndexes(final String indexSpec, final CommandContext context) {
    final int bracketStart = indexSpec.indexOf('[');
    if (bracketStart > 0 && indexSpec.endsWith("]")) {
      // ArcadeDB format: Type[property]
      final String typeName = indexSpec.substring(0, bracketStart);
      final String propertyName = indexSpec.substring(bracketStart + 1, indexSpec.length() - 1);

      final DocumentType type = context.getDatabase().getSchema().getType(typeName);
      final TypeIndex typeIndex = type.getPolymorphicIndexByProperties(propertyName);

      if (typeIndex == null)
        throw new CommandSQLParsingException(
            "No vector index found on property '" + propertyName + "' for type '" + typeName + "'");

      final Set<Integer> allowedBucketIds = new HashSet<>();
      for (final Bucket bucket : type.getBuckets(true))
        allowedBucketIds.add(bucket.getFileId());

      return filterVectorIndexes(typeIndex, allowedBucketIds);
    }

    // Direct index name lookup
    final Index directIndex = context.getDatabase().getSchema().getIndexByName(indexSpec);
    if (directIndex instanceof TypeIndex typeIndex)
      return filterVectorIndexes(typeIndex, null);

    throw new CommandSQLParsingException(
        "Index '" + indexSpec + "' is not a vector index (found: " +
            (directIndex != null ? directIndex.getClass().getSimpleName() : "null") + ")");
  }

  /**
   * Resolves the vector sub-indexes to search. See
   * {@link VectorUtils#collectSubIndexesOnce(IndexInternal[], Class, java.util.function.IntPredicate)} for why the same
   * sub-index can be listed twice and why the guard is on identity rather than on the bucket id (issue #7057).
   */
  private static List<LSMVectorIndex> filterVectorIndexes(final TypeIndex typeIndex, final Set<Integer> allowedBucketIds) {
    final IndexInternal[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes == null || bucketIndexes.length == 0)
      throw new CommandSQLParsingException("Index '" + typeIndex.getName() + "' has no bucket indexes");

    final List<LSMVectorIndex> vectorIndexes = VectorUtils.collectSubIndexesOnce(bucketIndexes, LSMVectorIndex.class,
        allowedBucketIds == null ? null : allowedBucketIds::contains);

    if (vectorIndexes.isEmpty())
      throw new CommandSQLParsingException("Index '" + typeIndex.getName() + "' is not a vector index");

    return vectorIndexes;
  }

  /**
   * Extracts a float[] query vector from the key parameter.
   */
  private static float[] extractQueryVector(final Object key, final LSMVectorIndex lsmIndex, final CommandContext context) {
    if (key instanceof float[] floatArray)
      return floatArray;

    if (key instanceof long[] src) {
      // Issue #4148: integer-only JSON arrays now arrive as long[] from the HTTP path.
      final float[] queryVector = new float[src.length];
      for (int i = 0; i < src.length; i++)
        queryVector[i] = src[i];
      return queryVector;
    }

    if (key instanceof double[] src) {
      final float[] queryVector = new float[src.length];
      for (int i = 0; i < src.length; i++)
        queryVector[i] = (float) src[i];
      return queryVector;
    }

    if (key instanceof Object[] objArray && objArray.length > 0 && objArray[0] instanceof Number) {
      final float[] queryVector = new float[objArray.length];
      for (int i = 0; i < objArray.length; i++)
        queryVector[i] = ((Number) objArray[i]).floatValue();
      return queryVector;
    }

    // Key is a vertex identifier - fetch the vertex and get its vector
    final String keyStr = key.toString();
    final String typeName = lsmIndex.getTypeName();
    final String vectorProperty = lsmIndex.getPropertyNames().getFirst();
    final String idProperty = lsmIndex.getIdPropertyName();

    // Shared with vector.neighbors, the SQL entry point onto this same lookup (issue #8097).
    try (final var rs = context.getDatabase().query("sql",
        SQLFunctionVectorAbstract.buildVectorByIdLookup(typeName, vectorProperty, idProperty), keyStr)) {
      if (rs.hasNext()) {
        final float[] queryVector = rs.next().getProperty(vectorProperty);
        if (queryVector != null)
          return queryVector;
      }
    }

    throw new CommandSQLParsingException("Could not find vertex with key '" + keyStr + "' or extract vector");
  }
}
