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
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexableSQLFunction;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

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
    final String cacheKey = cacheKey(indexName, queryString);

    // Try to get cached results (Map of RID -> score). Scores are floats so BM25 relevance is preserved; CLASSIC coordination
    // scores widen to float losslessly.
    @SuppressWarnings("unchecked")
    Map<RID, Float> allResults = (Map<RID, Float>) iContext.getCachedValue(cacheKey);

    if (allResults == null && iCurrentRecord != null) {
      // The indexed fetch of a statement with a LIMIT computed only the best matches (issue #9895), and it is the rows it
      // returned that the plan evaluates this condition on again: a record among them matches, with the same score the
      // complete search gives it. Only a record outside them needs the complete search.
      @SuppressWarnings("unchecked")
      final Map<RID, Float> best = (Map<RID, Float>) iContext.getCachedValue(bestCacheKey(indexName, queryString));
      final Float score = best != null ? best.get(iCurrentRecord.getIdentity()) : null;
      if (score != null) {
        iContext.setVariable("$score", score);
        return true;
      }
    }

    if (allResults == null) {
      // First execution - perform the actual search
      allResults = performSearch(indexName, queryString, iContext);

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
    return searchFromTarget(target, operator, rightValue, context, oExpressions, -1);
  }

  /**
   * Answers the matching records best first - descending score, then ascending RID so that equal scores come back in a
   * stable order - and LAZILY: each record is loaded only when the plan pulls its row, so a {@code LIMIT}, or a consumer
   * that stops early, stops the loading too (issue #9895). The order is a heap built in linear time and popped one row at
   * a time, so a statement that reads k rows of n matches pays O(n + k log n) rather than a full sort.
   * <p>
   * With a {@code limit} the search itself keeps only the best {@code limit} matches, which bounds what scoring retains
   * and what the cache holds. Those are the rows the plan consumes unless one of their records is gone: then the order
   * continues where they end, from the complete search, so the answer never comes up short. The limit is honored only
   * when every record the index can return is of the target type, because the type filter after the fetch would drop the
   * others after the limit counted them.
   */
  @Override
  public Iterable<Record> searchFromTarget(final FromClause target, final BinaryCompareOperator operator,
      final Object rightValue, final CommandContext context, final Expression[] oExpressions, final int limit) {
    if (oExpressions == null || oExpressions.length < 2)
      return List.of();

    final String indexName = resolveStringParam(oExpressions[0], context);
    final String queryString = resolveStringParam(oExpressions[1], context);
    if (indexName == null || queryString == null || limit == 0)
      // LIMIT 0 consumes no row: nothing to search
      return List.of();

    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    // The complete search, cached for the statement (opaque cache key, see execute() for the rationale - issue #4734)
    final String cacheKey = cacheKey(indexName, queryString);
    final Supplier<Map<RID, Float>> complete = () -> {
      @SuppressWarnings("unchecked")
      Map<RID, Float> allResults = (Map<RID, Float>) context.getCachedValue(cacheKey);
      if (allResults == null) {
        allResults = performSearch(indexName, queryString, context);
        context.setCachedValue(cacheKey, allResults);
      }
      return allResults;
    };

    if (limit < 1 || context.getCachedValue(cacheKey) != null || !returnsOnlyTargetRecords(indexName, target, context)) {
      final Map<RID, Float> allResults = complete.get();
      return () -> new BestFirstRecords(database, allResults, -1, null, guard);
    }

    final Map<RID, Float> best = FullTextSearch.search(FullTextSearch.resolveFullTextIndex(database, indexName), queryString,
        limit, guard);
    // One cache entry per statement: only a single AND block with this one function is handed a limit. The map holds each
    // bucket's best matches, so up to buckets x limit entries: their union contains the global best `limit`, which is the
    // prefix the iteration trusts
    context.setCachedValue(bestCacheKey(indexName, queryString), best);
    // Fewer matches than the limit: no bucket was cut short, so they are all the matches there are
    final boolean truncated = best.size() >= limit;
    return () -> new BestFirstRecords(database, best, truncated ? limit : -1, truncated ? complete : null, guard);
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

    return FullTextSearch.explainScoring(typeIndex, queryString);
  }

  @Override
  public String getSyntax() {
    return "SEARCH_INDEX(<index-name>, <query>)";
  }

  // ---- Private helpers ----

  /**
   * Performs the full-text search across all bucket indexes and returns a map of RID to score, stopping when the statement is
   * terminated or past its deadline (issue #9894).
   */
  private static Map<RID, Float> performSearch(final String indexName, final String queryString, final CommandContext context) {
    final Database database = context.getDatabase();
    return FullTextSearch.search(FullTextSearch.resolveFullTextIndex(database, indexName), queryString, -1,
        WorkGuard.forCommandDeadline(context));
  }

  private static String cacheKey(final String indexName, final String queryString) {
    return "search_index:" + indexName + ":" + queryString;
  }

  /** Where the indexed fetch keeps the best matches it computed for a statement with a LIMIT (issue #9895). */
  private static String bestCacheKey(final String indexName, final String queryString) {
    return "search_index_best:" + indexName + ":" + queryString;
  }

  /**
   * Whether every record the index can return passes the type filter the plan applies after the fetch: the index belongs
   * to the target type or to one of its subtypes.
   */
  private static boolean returnsOnlyTargetRecords(final String indexName, final FromClause target, final CommandContext context) {
    final Identifier targetType = target != null && target.getItem() != null ? target.getItem().getIdentifier() : null;
    if (targetType == null)
      return false;
    final Schema schema = context.getDatabase().getSchema();
    if (!(schema.getIndexByName(indexName) instanceof final TypeIndex typeIndex))
      return false;
    final DocumentType indexType = schema.getType(typeIndex.getTypeName());
    return indexType != null && indexType.isSubTypeOf(targetType.getStringValue());
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

  /**
   * The matches of a search as records, best first, each loaded only when it is pulled (issue #9895). The order is a binary
   * max-heap over two parallel arrays rather than a sorted list of map entries: building it is linear, each row pulled costs
   * a logarithmic pop, and a match is held as one reference and one primitive.
   * <p>
   * When the matches are only the best ones of a search with a limit, the first {@code trustedPrefix} of them are the first
   * of the complete order. Past that point the iteration switches to the complete search and skips the rows already served,
   * which the shared order (score, then RID) makes exactly the same rows.
   */
  private static final class BestFirstRecords implements Iterator<Record> {
    private final DatabaseInternal          database;
    private final WorkGuard                 guard;
    private       Supplier<Map<RID, Float>> complete;
    private       int                       trustedPrefix;
    private       RID[]                     rids;
    private       float[]                   scores;
    private       int                       size;
    private       int                       served;
    private       int                       ticks;
    private       Record                    next;

    private BestFirstRecords(final DatabaseInternal database, final Map<RID, Float> matches, final int trustedPrefix,
        final Supplier<Map<RID, Float>> complete, final WorkGuard guard) {
      this.database = database;
      this.guard = guard;
      this.trustedPrefix = trustedPrefix;
      this.complete = complete;
      buildHeap(matches);
    }

    @Override
    public boolean hasNext() {
      if (next == null)
        next = advance();
      return next != null;
    }

    @Override
    public Record next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final Record record = next;
      next = null;
      return record;
    }

    private Record advance() {
      while (true) {
        if (served == trustedPrefix && complete != null)
          continueWithCompleteSearch();
        if (size == 0)
          return null;

        final RID rid = pop();
        ++served;
        // A load can be I/O: check before every one, as the fetch steps do between rows (issue #9894)
        guard.check();
        try {
          final Record record = database.lookupByRID(rid, true);
          if (record != null)
            return record;
        } catch (final RecordNotFoundException e) {
          // Record was deleted since the index was last updated, skip it
        }
      }
    }

    /** The best matches are used up and more rows are pulled: continue in the order of the complete search. */
    private void continueWithCompleteSearch() {
      final Supplier<Map<RID, Float>> search = complete;
      complete = null;
      trustedPrefix = -1;
      buildHeap(search.get());
      for (int i = 0; i < served && size > 0; i++) {
        guard.checkPeriodically(++ticks);
        pop();
      }
    }

    private void buildHeap(final Map<RID, Float> matches) {
      size = matches.size();
      rids = new RID[size];
      scores = new float[size];
      int i = 0;
      for (final Map.Entry<RID, Float> entry : matches.entrySet()) {
        guard.checkPeriodically(++ticks);
        rids[i] = entry.getKey();
        scores[i] = entry.getValue();
        ++i;
      }
      for (int parent = size / 2 - 1; parent >= 0; parent--) {
        guard.checkPeriodically(++ticks);
        siftDown(parent);
      }
    }

    private RID pop() {
      final RID best = rids[0];
      --size;
      rids[0] = rids[size];
      scores[0] = scores[size];
      rids[size] = null;
      if (size > 1)
        siftDown(0);
      return best;
    }

    private void siftDown(int position) {
      while (true) {
        final int left = 2 * position + 1;
        if (left >= size)
          return;
        final int right = left + 1;
        final int better = right < size && precedes(right, left) ? right : left;
        if (!precedes(better, position))
          return;
        final RID rid = rids[position];
        rids[position] = rids[better];
        rids[better] = rid;
        final float score = scores[position];
        scores[position] = scores[better];
        scores[better] = score;
        position = better;
      }
    }

    /** Whether entry {@code a} comes before entry {@code b}: higher score first, then lower RID. */
    private boolean precedes(final int a, final int b) {
      final int byScore = Float.compare(scores[a], scores[b]);
      if (byScore != 0)
        return byScore > 0;
      return rids[a].compareTo(rids[b]) < 0;
    }
  }
}
