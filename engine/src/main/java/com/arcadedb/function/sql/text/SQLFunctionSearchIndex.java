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
package com.arcadedb.function.sql.text;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextQueryExecutor;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL function to search a full-text index.
 * <p>
 * Usage: SEARCH_INDEX('indexName', 'query')
 * Example: SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'java database')
 * <p>
 * Implements {@link IndexableSQLFunction} so the query planner can use index-based fetching
 * instead of a full type scan when SEARCH_INDEX is used in a WHERE clause.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionSearchIndex extends SQLFunctionAbstract implements IndexableSQLFunction {
  public static final String NAME  = "fulltext.searchIndex";
  public static final String ALIAS = "search_index";

  public SQLFunctionSearchIndex() {
    super(NAME);
  }

  @Override
  public String getAlias() {
    return ALIAS;
  }

  @Override
  public Object execute(final Object iThis, final Identifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final CommandContext iContext) {

    if (iParams.length < 2)
      throw new CommandExecutionException("SEARCH_INDEX() requires 2 parameters: indexName and query");

    if (iParams[0] == null || iParams[1] == null)
      throw new CommandExecutionException("SEARCH_INDEX() parameters cannot be null");

    final String indexName = iParams[0].toString();
    final String queryString = iParams[1].toString();

    // Cache key for this specific search. The query string is opaque data, so the cache lives in the command context's opaque
    // cache (getCachedValue/setCachedValue) rather than the variable namespace: getVariable would interpret a '.' in the key as
    // a nested $var.field path and reject query strings with two or more dots (issue #4734).
    final String cacheKey = "search_index:" + indexName + ":" + queryString;

    // Try to get cached results (Map of RID -> score). Scores are floats so BM25 relevance is preserved; CLASSIC coordination
    // scores widen to float losslessly.
    @SuppressWarnings("unchecked")
    Map<RID, Float> allResults = (Map<RID, Float>) iContext.getCachedValue(cacheKey);

    if (allResults == null) {
      // First execution - perform the actual search
      allResults = performSearch(indexName, queryString, iContext.getDatabase());

      // Cache the results
      iContext.setCachedValue(cacheKey, allResults);
    }

    // Check if current record matches
    if (iCurrentRecord != null) {
      final RID rid = iCurrentRecord.getIdentity();
      final boolean matches = allResults.containsKey(rid);

      if (matches) {
        // Store the score for this record in the context variable $score so $score projection works in SELECT. getOrDefault keeps
        // $score non-null even if the entry vanished between containsKey and get (cannot happen on the single-threaded SQL path
        // today, but makes the intent explicit).
        iContext.setVariable("$score", allResults.getOrDefault(rid, 0f));
      } else {
        // Clear the score for non-matching records
        iContext.setVariable("$score", 0f);
      }

      return matches;
    }

    // Return cursor with all results (used when called without a current record)
    final List<IndexCursorEntry> entries = new ArrayList<>();
    for (final Map.Entry<RID, Float> entry : allResults.entrySet()) {
      entries.add(new IndexCursorEntry(new Object[] { queryString }, entry.getKey(), entry.getValue().floatValue()));
    }
    entries.sort((a, b) -> Float.compare(b.floatScore, a.floatScore));

    return new TempIndexCursor(entries);
  }

  // ---- IndexableSQLFunction implementation ----

  @Override
  public boolean shouldExecuteAfterSearch(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Must still execute per-row after the indexed fetch to set $score in the context
    return true;
  }

  @Override
  public boolean allowsIndexedExecution(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    if (oExpressions == null || oExpressions.length < 2)
      return false;

    // Verify the named index exists and is a full-text type index
    final String indexName = resolveStringParam(oExpressions[0], context);
    if (indexName == null)
      return false;

    final Schema schema = context.getDatabase().getSchema();
    final Index index = schema.getIndexByName(indexName);
    return index instanceof TypeIndex;
  }

  @Override
  public boolean canExecuteInline(final FromClause target, final BinaryCompareOperator operator, final Object right,
      final CommandContext context, final Expression[] oExpressions) {
    // Can always fall back to full scan with per-row evaluation
    return true;
  }

  @Override
  public long estimate(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    return -1;
  }

  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions) {
    if (oExpressions == null || oExpressions.length < 2)
      return List.of();

    final String indexName = resolveStringParam(oExpressions[0], context);
    final String queryString = resolveStringParam(oExpressions[1], context);
    if (indexName == null || queryString == null)
      return List.of();

    // Perform the search and cache results (opaque cache key, see execute() for the rationale - issue #4734)
    final String cacheKey = "search_index:" + indexName + ":" + queryString;
    @SuppressWarnings("unchecked")
    Map<RID, Float> allResults = (Map<RID, Float>) context.getCachedValue(cacheKey);

    if (allResults == null) {
      allResults = performSearch(indexName, queryString, context.getDatabase());
      context.setCachedValue(cacheKey, allResults);
    }

    // Sort RIDs by score descending for optimal ORDER BY $score performance
    final List<Map.Entry<RID, Float>> sorted = new ArrayList<>(allResults.entrySet());
    sorted.sort((a, b) -> Float.compare(b.getValue(), a.getValue()));

    // Fetch records by RID
    final DatabaseInternal db = (DatabaseInternal) context.getDatabase();
    final List<Record> records = new ArrayList<>(sorted.size());
    for (final Map.Entry<RID, Float> entry : sorted) {
      try {
        final Record record = db.lookupByRID(entry.getKey(), true);
        if (record != null)
          records.add(record);
      } catch (final RecordNotFoundException e) {
        // Record was deleted since the index was last updated, skip it
      }
    }
    return records;
  }

  @Override
  public Object getScoringExplain(final FromClause target, final BinaryCompareOperator operator, final Object rightValue,
      final CommandContext context, final Expression[] oExpressions) {
    if (oExpressions == null || oExpressions.length < 2)
      return null;
    final String indexName = resolveStringParam(oExpressions[0], context);
    final String queryString = resolveStringParam(oExpressions[1], context);
    if (indexName == null || queryString == null)
      return null;

    final Index index = context.getDatabase().getSchema().getIndexByName(indexName);
    if (!(index instanceof final TypeIndex typeIndex))
      return null;

    // BM25 is scored per bucket (per-shard), so document frequency / IDF differ across buckets. EXPLAIN/PROFILE reports a single
    // representative bucket's statistics (the first full-text bucket index) rather than a global view - enough to understand the
    // similarity, parameters and relative term weights, but not a type-wide IDF.
    final Index[] buckets = typeIndex.getIndexesOnBuckets();
    LSMTreeFullTextIndex firstFt = null;
    int ftBucketCount = 0; // all full-text bucket indexes of a type share one similarity, so they are all BM25 or all CLASSIC
    for (final Index bucketIndex : buckets)
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex) {
        if (firstFt == null)
          firstFt = ftIndex;
        ++ftBucketCount;
      }
    // Only BM25 indexes carry a scoring explanation; CLASSIC has none.
    if (firstFt == null || !firstFt.isBM25())
      return null;

    final JSONObject explain = new FullTextQueryExecutor(firstFt).explainScoring(queryString);
    // Make it explicit that, on a multi-bucket type, these are the FIRST bucket's statistics so a reader is not misled into
    // treating the df/IDF as type-wide.
    if (ftBucketCount > 1)
      explain.put("note", "statistics shown are for the FIRST of " + ftBucketCount
          + " buckets; BM25 is scored per bucket, so df/IDF differ across buckets");
    return explain;
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }

  // ---- Private helpers ----

  /**
   * Performs the full-text search across all bucket indexes and returns a map of RID to score.
   */
  private Map<RID, Float> performSearch(final String indexName, final String queryString, final Database database) {
    final Map<RID, Float> allResults = new HashMap<>();

    final Index index = database.getSchema().getIndexByName(indexName);

    if (index == null)
      throw new CommandExecutionException("Index '" + indexName + "' not found");

    if (!(index instanceof final TypeIndex typeIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a type index");

    // Get the underlying full-text index
    final Index[] bucketIndexes = typeIndex.getIndexesOnBuckets();
    if (bucketIndexes.length == 0 || !(bucketIndexes[0] instanceof LSMTreeFullTextIndex))
      throw new CommandExecutionException("Index '" + indexName + "' is not a full-text index");

    // Execute search across all bucket indexes
    for (final Index bucketIndex : bucketIndexes) {
      if (bucketIndex instanceof final LSMTreeFullTextIndex ftIndex) {
        final FullTextQueryExecutor executor = new FullTextQueryExecutor(ftIndex);
        final IndexCursor cursor = executor.search(queryString, -1);

        while (cursor.hasNext()) {
          final Identifiable match = cursor.next();
          final float score = cursor.getFloatScore();
          // Float::sum across buckets: a given RID lives in exactly one bucket, so a RID is produced by at most one bucket index
          // and the merge is effectively an insert (no real summing). For CLASSIC the additive semantics would also be correct;
          // for BM25 the per-bucket scoping relies on this one-bucket-per-RID invariant - if it ever broke, scores would be
          // double-counted here rather than failing loudly.
          allResults.merge(match.getIdentity(), score, Float::sum);
        }
      }
    }

    return allResults;
  }

  /**
   * Resolves a function expression parameter to a string value.
   */
  private static String resolveStringParam(final Expression expr, final CommandContext context) {
    if (expr == null)
      return null;
    final Object value = expr.execute((Identifiable) null, context);
    return value != null ? value.toString() : null;
  }
}
