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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IndexedFunctionPushdown;
import com.arcadedb.query.sql.executor.RIDFilter;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * One full-text search of a {@code SEARCH_INDEX} or {@code SEARCH_FIELDS} call - an index, a query, and the fields the
 * query's unqualified clauses are restricted to - answered both ways the SQL engine asks for it: per record, as the
 * condition's boolean with its {@code $score}, and as the indexed fetch of the records that match (issues #9895, #9909,
 * #9910). Both functions share it, so a field-restricted search is the same search, with the same caches and the same
 * pushdowns, as one through the index's name.
 * <p>
 * The matches are cached in the command context for the statement, under a key the query string is part of: the opaque
 * cache rather than the variable namespace, which reads a '.' in a key as a nested {@code $var.field} path (issue #4734).
 * A second key holds a SUBSET of the matches with the scores the complete search gives them: the best ones a pushed-down
 * limit kept, or those a RID filter let through. A record among them matches without running the complete search.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FullTextFunctionSearch {
  private final TypeIndex    index;
  private final String       query;
  private final List<String> restrictedFields;
  private final String       cacheKey;
  private final String       subsetCacheKey;

  /**
   * @param restrictedFields the properties the query's unqualified clauses are searched in, or null for every property of
   *                         the index
   */
  FullTextFunctionSearch(final TypeIndex index, final String query, final List<String> restrictedFields) {
    this.index = index;
    this.query = query;
    this.restrictedFields = restrictedFields;
    if (restrictedFields == null) {
      this.cacheKey = "search_index:" + index.getName() + ":" + query;
      this.subsetCacheKey = "search_index_best:" + index.getName() + ":" + query;
    } else {
      this.cacheKey = "search_fields:" + index.getName() + ":" + restrictedFields + ":" + query;
      this.subsetCacheKey = "search_fields_best:" + index.getName() + ":" + restrictedFields + ":" + query;
    }
  }

  /**
   * The condition evaluated on {@code currentRecord}: whether it matches, with its score in {@code $score}. Without a
   * record, a cursor over every match, best first.
   */
  Object evaluate(final Identifiable currentRecord, final CommandContext context) {
    @SuppressWarnings("unchecked")
    Map<RID, Float> allResults = (Map<RID, Float>) context.getCachedValue(cacheKey);

    if (allResults == null && currentRecord != null) {
      // The indexed fetch computed only some of the matches - the best ones a LIMIT needs (issue #9895), or those its
      // @rid conditions let through (issue #9909) - and it is the rows it returned that the plan evaluates this condition
      // on again: a record among them matches, with the score the complete search gives it. Only a record outside them
      // needs the complete search.
      @SuppressWarnings("unchecked")
      final Map<RID, Float> subset = (Map<RID, Float>) context.getCachedValue(subsetCacheKey);
      final Float score = subset != null ? subset.get(currentRecord.getIdentity()) : null;
      if (score != null) {
        context.setVariable("$score", score);
        return true;
      }
    }

    if (allResults == null)
      allResults = completeSearch(context);

    if (currentRecord != null) {
      final Float score = allResults.get(currentRecord.getIdentity());
      // Store the score in $score so that a $score projection works in SELECT, and clear it for a record that does not match
      context.setVariable("$score", score != null ? score : 0f);
      return score != null;
    }

    final List<IndexCursorEntry> entries = new ArrayList<>(allResults.size());
    for (final Map.Entry<RID, Float> entry : allResults.entrySet())
      entries.add(new IndexCursorEntry(new Object[] { query }, entry.getKey(), entry.getValue()));
    entries.sort((a, b) -> Float.compare(b.floatScore, a.floatScore));
    return new TempIndexCursor(entries);
  }

  /**
   * The matching records in the order the plan asks for - best first (descending score, then ascending RID, so that equal
   * scores come back in a stable order) or RID order - and LAZILY: each record is loaded only when the plan pulls its
   * row, so a {@code LIMIT}, or a consumer that stops early, stops the loading too (issue #9895). The order is a heap built
   * in linear time and popped one row at a time, so a statement that reads k rows of n matches pays O(n + k log n) rather
   * than a full sort.
   * <p>
   * A RID filter is applied to the matches before any of them is scored or loaded (issue #9909).
   * <p>
   * With a limit, best first, the search itself keeps only the best {@code limit} matches, which bounds what scoring
   * retains and what the cache holds. Those are the rows the plan consumes unless one of their records is gone: then the
   * order continues where they end, from the complete search, so the answer never comes up short. The limit is honored
   * only when every record the index can return is of the target type, because the type filter after the fetch would drop
   * the others after the limit counted them. A RID order needs every match to know which come first, so it is never
   * handed the limit; the heap still loads only the rows pulled.
   */
  Iterable<Record> search(final FromClause target, final CommandContext context, final IndexedFunctionPushdown pushdown) {
    if (pushdown.limit() == 0)
      // LIMIT 0 consumes no row: nothing to search
      return List.of();

    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final RIDFilter ridFilter = pushdown.ridFilter();
    final IndexedFunctionPushdown.Order order = pushdown.order();
    final int limit = order == IndexedFunctionPushdown.Order.FUNCTION ? pushdown.limit() : -1;
    final Supplier<Map<RID, Float>> complete = () -> {
      @SuppressWarnings("unchecked")
      final Map<RID, Float> allResults = (Map<RID, Float>) context.getCachedValue(cacheKey);
      return allResults != null ? allResults : completeSearch(context);
    };

    // The Iterable builds a fresh heap on every iterator() call: a plan pulls it once, and re-iterating it re-orders the same
    // matches rather than searching again
    @SuppressWarnings("unchecked")
    final Map<RID, Float> cached = (Map<RID, Float>) context.getCachedValue(cacheKey);
    if (cached != null)
      return () -> new MatchingRecords(database, cached, ridFilter, -1, null, ridFilter, order, guard);

    if (limit < 1 || !returnsOnlyTargetRecords(target, context)) {
      if (ridFilter == null) {
        final Map<RID, Float> allResults = completeSearch(context);
        return () -> new MatchingRecords(database, allResults, null, -1, null, null, order, guard);
      }
      final Map<RID, Float> accepted = FullTextSearch.search(index, query, -1, guard, restrictedFields, ridFilter);
      rememberSubset(context, accepted);
      return () -> new MatchingRecords(database, accepted, null, -1, null, null, order, guard);
    }

    final Map<RID, Float> best = FullTextSearch.search(index, query, limit, guard, restrictedFields, ridFilter);
    // The map holds each bucket's best matches, so up to buckets x limit entries: their union contains the global best
    // `limit`, which is the prefix the iteration trusts
    rememberSubset(context, best);
    // Fewer matches than the limit: no bucket was cut short, so they are all the matches there are
    final boolean truncated = best.size() >= limit;
    return () -> new MatchingRecords(database, best, null, truncated ? limit : -1, truncated ? complete : null, ridFilter, order,
        guard);
  }

  Object explainScoring() {
    return FullTextSearch.explainScoring(index, query, restrictedFields);
  }

  /** Every match with its score, cached for the statement. Stops when the statement is terminated or past its deadline. */
  private Map<RID, Float> completeSearch(final CommandContext context) {
    final Map<RID, Float> allResults = FullTextSearch.search(index, query, -1, WorkGuard.forCommandDeadline(context),
        restrictedFields, null);
    context.setCachedValue(cacheKey, allResults);
    return allResults;
  }

  /**
   * Remembers some of the matches with their complete-search scores. The contract {@link #evaluate} relies on: every entry
   * is a match of the complete search, with the score the complete search gives it, since a record found here is answered
   * {@code true} without the complete search. The pushed-down searches only ever drop matches (past the limit, or rejected
   * by the RID filter) and score with corpus-wide statistics, so their results keep it. Two fetches of the same search in one statement (the
   * branches of an OR, each with its own RID filter) each add theirs: their union is still such a subset.
   */
  private void rememberSubset(final CommandContext context, final Map<RID, Float> subset) {
    @SuppressWarnings("unchecked")
    final Map<RID, Float> previous = (Map<RID, Float>) context.getCachedValue(subsetCacheKey);
    if (previous == null || previous.isEmpty())
      context.setCachedValue(subsetCacheKey, subset);
    else {
      final Map<RID, Float> union = new HashMap<>(previous);
      union.putAll(subset);
      context.setCachedValue(subsetCacheKey, union);
    }
  }

  /**
   * Whether every record the index can return passes the type filter the plan applies after the fetch: the index belongs
   * to the target type or to one of its subtypes.
   */
  private boolean returnsOnlyTargetRecords(final FromClause target, final CommandContext context) {
    final Identifier targetType = target != null && target.getItem() != null ? target.getItem().getIdentifier() : null;
    if (targetType == null)
      return false;
    final DocumentType indexType = context.getDatabase().getSchema().getType(index.getTypeName());
    return indexType != null && indexType.isSubTypeOf(targetType.getStringValue());
  }

  /**
   * The matches of a search as records, in the plan's order, each loaded only when it is pulled (issue #9895). The order is
   * a binary heap over parallel arrays rather than a sorted list of map entries: building it is linear, each row pulled
   * costs a logarithmic pop, and a match is held as one reference and, best first, one primitive.
   * <p>
   * When the matches are only the best ones of a search with a limit, the first {@code trustedPrefix} of them are the first
   * of the complete order. Past that point the iteration switches to the complete search and skips the rows already served,
   * which the shared order (score, then RID) makes exactly the same rows.
   */
  private static final class MatchingRecords implements Iterator<Record> {
    private final DatabaseInternal              database;
    private final WorkGuard                     guard;
    private final IndexedFunctionPushdown.Order order;
    /** Applied to the complete search the iteration continues with; it built its first heap from accepted matches. */
    private final RIDFilter                     continuationFilter;
    private       Supplier<Map<RID, Float>>     complete;
    private       int                           trustedPrefix;
    private       RID[]                         rids;
    private       float[]                       scores;
    private       int                           size;
    private       int                           served;
    private       int                           ticks;
    private       Record                        next;

    /**
     * @param filter the RID filter to apply to {@code matches}, or null when they passed it already
     */
    private MatchingRecords(final DatabaseInternal database, final Map<RID, Float> matches, final RIDFilter filter,
        final int trustedPrefix, final Supplier<Map<RID, Float>> complete, final RIDFilter continuationFilter,
        final IndexedFunctionPushdown.Order order, final WorkGuard guard) {
      this.database = database;
      this.guard = guard;
      this.order = order;
      this.trustedPrefix = trustedPrefix;
      this.complete = complete;
      this.continuationFilter = continuationFilter;
      buildHeap(matches, filter);
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

    /**
     * The best matches are used up and more rows are pulled: continue in the order of the complete search. Skipping the
     * {@code served} first rows of it is exact only because the order is total - equal scores are broken by RID - so the
     * best matches' heap and the complete one put every shared row in the same position.
     */
    private void continueWithCompleteSearch() {
      final Supplier<Map<RID, Float>> search = complete;
      complete = null;
      trustedPrefix = -1;
      buildHeap(search.get(), continuationFilter);
      for (int i = 0; i < served && size > 0; i++) {
        guard.checkPeriodically(++ticks);
        pop();
      }
    }

    private void buildHeap(final Map<RID, Float> matches, final RIDFilter filter) {
      final boolean byScore = order == IndexedFunctionPushdown.Order.FUNCTION;
      rids = new RID[matches.size()];
      scores = byScore ? new float[rids.length] : null;
      size = 0;
      for (final Map.Entry<RID, Float> entry : matches.entrySet()) {
        guard.checkPeriodically(++ticks);
        final RID rid = entry.getKey();
        if (filter != null && !filter.accepts(rid))
          continue;
        rids[size] = rid;
        if (byScore)
          scores[size] = entry.getValue();
        ++size;
      }
      for (int parent = size / 2 - 1; parent >= 0; parent--) {
        guard.checkPeriodically(++ticks);
        siftDown(parent);
      }
    }

    private RID pop() {
      final RID first = rids[0];
      --size;
      rids[0] = rids[size];
      rids[size] = null;
      if (scores != null)
        scores[0] = scores[size];
      if (size > 1)
        siftDown(0);
      return first;
    }

    private void siftDown(int position) {
      while (true) {
        final int left = 2 * position + 1;
        if (left >= size)
          return;
        final int right = left + 1;
        final int first = right < size && precedes(right, left) ? right : left;
        if (!precedes(first, position))
          return;
        final RID rid = rids[position];
        rids[position] = rids[first];
        rids[first] = rid;
        if (scores != null) {
          final float score = scores[position];
          scores[position] = scores[first];
          scores[first] = score;
        }
        position = first;
      }
    }

    /** Whether entry {@code a} comes before entry {@code b}: best first by score then RID, or in RID order. */
    private boolean precedes(final int a, final int b) {
      return switch (order) {
        case RID_ASC -> rids[a].compareTo(rids[b]) < 0;
        case RID_DESC -> rids[a].compareTo(rids[b]) > 0;
        case FUNCTION -> {
          final int byScore = Float.compare(scores[a], scores[b]);
          yield byScore != 0 ? byScore > 0 : rids[a].compareTo(rids[b]) < 0;
        }
      };
    }
  }
}
