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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.facet.FacetExecutionStats;
import com.arcadedb.query.facet.FacetExecutor;
import com.arcadedb.query.facet.FacetFilterEvaluator;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetSpec;
import com.arcadedb.query.facet.FacetSpecCompiler;
import com.arcadedb.query.facet.MatchSet;
import com.arcadedb.query.sql.parser.FacetItem;
import com.arcadedb.query.sql.parser.FacetsClause;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.utility.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.IntPredicate;

/**
 * Collects the matches of a FACETS query and computes its facets (D7). Pass-through: no row is buffered. Each row the WHERE produced
 * is recorded by RID in a {@link MatchSet} and forwarded unless a facet FILTER refuses it, so a downstream ORDER BY keeps its bounded
 * heap and a LIMIT keeps stopping early. The facets are computed once: when the upstream is exhausted, when a LIMIT downstream is
 * reached (before it releases the upstream) or when {@link #facets()} is asked. The matches the rows did not reach are then read
 * without being forwarded.
 * <p>
 * The matches come from one of these sources, decided when the step opens:
 * <ul>
 *   <li>{@code rows}: every row is recorded;</li>
 *   <li>{@code index}: the fetch below is an {@link IndexedMatchSource} the whole WHERE was handed to, which adds every match by RID
 *       without loading a record;</li>
 *   <li>{@code projection}: no WHERE and no FILTER over a type or buckets: {@link MatchSet#fromProjection} holds every record;
 *       {@code projection+scan} when it does not cover every bucket (no usable projection, or changed by the caller's open
 *       transaction): the records of those buckets are read by a scan of them when the step opens.</li>
 * </ul>
 * The index and the projection hold the records of every bucket; a bucket the current user may not read is left out of the match set
 * when it is built, so its records reach no count, no memory reservation and no PROFILE figure (the executor skips such a bucket too).
 * <p>
 * {@link ParallelRowPipeline#of} stops at this step, so an aggregation above it stays sequential: intended, it must see the rows the
 * facets were collected from.
 */
public class FacetCollectStep extends AbstractExecutionStep {
  private static final String ROWS       = "rows";
  private static final String INDEX      = "index";
  private static final String PROJECTION = "projection";
  private static final String SCAN       = "scan";

  private final FacetsClause               clause;
  private final int[]                      filterIndexByFacet;
  /** Whether a facet carries a FILTER: the rows must then be masked one by one, so neither the index nor the projection serves. */
  private final boolean                    anyFilter;
  private final List<FacetFilterEvaluator> evaluators;
  private final String                     targetType;
  private final int[]                      projectionBuckets;

  /** Compiled when the step opens, against the context of the execution (a cached plan is copied without one). */
  private List<FacetSpec>  specs;
  private DatabaseInternal database;
  private MatchSet.Builder builder;
  private MatchSet         projection;
  private boolean          recordRows;
  private String           matchSource;
  private ResultSet        prevResult;
  private boolean          upstreamDone;
  private boolean          opened;
  private boolean          closed;
  private long             matchCount;
  private FacetResults     facets;

  /**
   * @param clause             the FACETS clause, a copy owned by the step: printed by EXPLAIN, compiled into the facets of each
   *                           execution when the step opens (its input parameters and INCLUDE expressions read that context)
   * @param filterIndexByFacet per facet, the index of its FILTER in {@code evaluators}, -1 for none
   * @param evaluators         one per FILTER; bit {@code i} of a row's fail mask is set when evaluator {@code i} refuses it
   * @param targetType         the type the query selects from, null when it is not one (a subquery)
   * @param projectionBuckets  the buckets the match set may be taken from {@link MatchSet#fromProjection} (no WHERE, no FILTER, a type
   *                           or buckets as target), null when the matches must come from the rows
   */
  public FacetCollectStep(final FacetsClause clause, final int[] filterIndexByFacet, final List<FacetFilterEvaluator> evaluators,
      final String targetType, final int[] projectionBuckets, final CommandContext context) {
    super(context);
    this.clause = clause;
    this.filterIndexByFacet = filterIndexByFacet;
    boolean filter = false;
    for (final int filterIndex : filterIndexByFacet)
      filter |= filterIndex >= 0;
    this.anyFilter = filter;
    this.evaluators = evaluators;
    this.targetType = targetType;
    this.projectionBuckets = projectionBuckets;
  }

  /** Per facet, the index of its FILTER among the facets that carry one, -1 for none. */
  public static int[] filterIndexByFacet(final List<FacetSpec> specs) {
    final int[] result = new int[specs.size()];
    int filters = 0;
    for (int i = 0; i < result.length; i++)
      result[i] = specs.get(i).filter() != null ? filters++ : -1;
    return result;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious("FACETS requires a previous step");
    open(context);
    // the step the timeout step cannot cover: rows refused by a FILTER are consumed inside one hasNext()
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private Result nextItem;
      private int    fetched;

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords)
          return false;
        if (nextItem == null)
          nextItem = pull(prevStep, context, nRecords, guard, true);
        return nextItem != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        FacetCollectStep.this.close();
      }
    };
  }

  /**
   * The next row to forward, recording every row it reads; null once the upstream is exhausted, which computes the facets.
   *
   * @param forward false to drain: every remaining row is recorded and none is returned
   */
  private Result pull(final ExecutionStepInternal prevStep, final CommandContext context, final int nRecords, final WorkGuard guard,
      final boolean forward) {
    while (!upstreamDone) {
      if (prevResult == null || !prevResult.hasNext()) {
        prevResult = prevStep.syncPull(context, nRecords);
        if (!prevResult.hasNext()) {
          upstreamDone = true;
          computeFacets(context);
          return null;
        }
      }
      final Result row = prevResult.next();
      guard.check();
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      final int failMask;
      try {
        failMask = admit(row, context);
      } finally {
        if (context.isProfiling())
          cost += System.nanoTime() - begin;
      }
      if (forward && failMask == 0)
        return row;
    }
    return null;
  }

  /** Records the row when its matches are collected from the rows, and returns its fail mask. */
  private int admit(final Result row, final CommandContext context) {
    if (!recordRows && evaluators.isEmpty())
      return 0;
    final RID rid = row.getIdentity().orElseThrow(() -> new CommandExecutionException("FACETS needs rows that are records: " + row));
    int failMask = 0;
    for (int i = 0; i < evaluators.size(); i++)
      if (!evaluators.get(i).passes(rid, row, context))
        failMask |= 1 << i;
    if (recordRows)
      // refused once over the memory budget: the rows keep flowing, the facets are unavailable
      builder.add(rid, failMask);
    return failMask;
  }

  private void open(final CommandContext context) {
    if (opened)
      return;
    opened = true;
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      database = (DatabaseInternal) context.getDatabase();
      specs = FacetSpecCompiler.compile(clause, context);
      final long budgetWaitMs = GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong();
      for (final FacetFilterEvaluator evaluator : evaluators)
        evaluator.open(context);
      builder = MatchSet.builder(database, evaluators.size(), budgetWaitMs);
      recordRows = true;
      matchSource = ROWS;
      if (anyFilter)
        return;

      final List<ExecutionStepInternal> bucketFilters = new ArrayList<>();
      final IndexedMatchSource indexed = indexedSource(bucketFilters);
      if (indexed != null && indexed.collectAllMatches(context, builder, acceptedBuckets(bucketFilters))) {
        recordRows = false;
        matchSource = INDEX;
      } else if (projectionBuckets != null)
        openProjection(budgetWaitMs, context);
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * The fetch below when what stands between it and this step cannot drop one of its matches but by bucket (a bucket or type filter,
   * collected into {@code bucketFilters}) or is its own condition evaluated again: its matches are then the rows' matches.
   */
  private IndexedMatchSource indexedSource(final List<ExecutionStepInternal> bucketFilters) {
    final List<WhereClause> conditions = new ArrayList<>();
    ExecutionStepInternal step = prev;
    while (true) {
      if (step instanceof FilterByClustersStep || step instanceof FilterByTypeStep)
        bucketFilters.add(step);
      else if (step instanceof FilterStep filter)
        conditions.add(filter.getWhereClause());
      else
        break;
      step = ((AbstractExecutionStep) step).getPrev();
    }
    if (!(step instanceof IndexedMatchSource source))
      return null;
    for (final WhereClause condition : conditions)
      if (!source.isOwnCondition(condition))
        return null;
    return source;
  }

  /** The buckets whose matches the index adds: those the filters between it and this step pass and the current user may read. */
  private IntPredicate acceptedBuckets(final List<ExecutionStepInternal> bucketFilters) {
    // resolved once: the predicate is asked for every RID of the index's answer
    final SecurityDatabaseUser user = SecurityHelper.currentUser(database);
    return bucketId -> {
      if (bucketId < 0 || user != null && !user.requestAccessOnFile(bucketId, SecurityDatabaseUser.ACCESS.READ_RECORD))
        return false;
      for (final ExecutionStepInternal filter : bucketFilters)
        if (filter instanceof FilterByClustersStep clusters ? !clusters.acceptsBucket(bucketId)
            : !acceptsType(((FilterByTypeStep) filter).getTypeName(), bucketId))
          return false;
      return true;
    };
  }

  /** Whether the records of the bucket are instances of the type, as {@link FilterByTypeStep} decides for a row. */
  private boolean acceptsType(final String typeName, final int bucketId) {
    final DocumentType type = database.getSchema().getTypeByBucketId(bucketId);
    return type != null && type.isSubTypeOf(typeName);
  }

  /**
   * Takes the matches of the buckets the caller's open transaction did not change from the projection; the records of the others (and
   * of those without a usable projection) are read by a scan of them. A bucket the current user may not read is left out of both: its
   * records are no match of the facets.
   */
  private void openProjection(final long budgetWaitMs, final CommandContext context) {
    final int[] readable = readable(projectionBuckets);
    int[] eligible = readable;
    if (database.isTransactionActive()) {
      final TransactionContext transaction = database.getTransaction();
      final int[] unchanged = new int[eligible.length];
      int count = 0;
      for (final int bucketId : eligible) {
        final IntHashSet single = new IntHashSet(1);
        single.add(bucketId);
        if (!transaction.hasChangesInFiles(single))
          unchanged[count++] = bucketId;
      }
      eligible = count == eligible.length ? eligible : Arrays.copyOf(unchanged, count);
    }
    // with a bucket hidden from the user the rows are no source: draining them would read the hidden bucket
    final boolean hidden = readable.length < projectionBuckets.length;
    if (eligible.length == 0 && !hidden)
      return;

    if (eligible.length > 0) {
      projection = MatchSet.fromProjection(database, eligible, budgetWaitMs);
      if (projection.isOverBudget()) {
        // the facets are unavailable: recording the rows would only spend more of the budget
        recordRows = false;
        matchSource = PROJECTION;
        return;
      }
    }
    final int[] coveredIds = projection != null ? projection.bucketIds() : new int[0];
    if (coveredIds.length == 0 && !hidden)
      return;
    final boolean[] covered = new boolean[coveredIds.length == 0 ? 0 : coveredIds[coveredIds.length - 1] + 1];
    for (final int bucketId : coveredIds)
      covered[bucketId] = true;
    recordRows = false;
    if (coveredIds.length == readable.length)
      matchSource = PROJECTION;
    else {
      // the other buckets are read here, by themselves: drained from the rows, they would cost a read of every covered record too
      scanUncovered(readable, covered, context);
      matchSource = PROJECTION + "+" + SCAN;
    }
  }

  /** The buckets of {@code bucketIds} the current user may read records of. */
  private int[] readable(final int[] bucketIds) {
    final SecurityDatabaseUser user = SecurityHelper.currentUser(database);
    if (user == null)
      return bucketIds;
    final int[] readable = new int[bucketIds.length];
    int count = 0;
    for (final int bucketId : bucketIds)
      if (user.requestAccessOnFile(bucketId, SecurityDatabaseUser.ACCESS.READ_RECORD))
        readable[count++] = bucketId;
    return count == readable.length ? bucketIds : Arrays.copyOf(readable, count);
  }

  /**
   * Records every record of {@code bucketIds} the projection match set does not hold, as the caller's transaction sees them. The bucket
   * iterator loads each record to hand out its RID, so this costs a full read of those buckets (counted in {@code readRecord}), never of
   * the covered ones.
   */
  private void scanUncovered(final int[] bucketIds, final boolean[] covered, final CommandContext context) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    int ticks = 0;
    for (final int bucketId : bucketIds) {
      if (bucketId < covered.length && covered[bucketId])
        continue;
      final Iterator<Record> records = database.iterateBucket(database.getSchema().getBucketById(bucketId).getName());
      while (records.hasNext()) {
        guard.checkPeriodically(++ticks);
        // refused once over the memory budget: the facets are unavailable
        if (!builder.add(records.next().getIdentity(), 0))
          return;
      }
    }
  }

  /**
   * The facets of the query: computed once, memoized. When the upstream is not exhausted yet and the matches come from the rows, the
   * remaining rows are read (recorded, not forwarded): the rows end there. A step the rows never reached (LIMIT 0) is opened here.
   * Null when the step was closed before the facets were computed.
   */
  public FacetResults facets() {
    if (facets != null || closed)
      return facets;
    open(context);
    if (recordRows && !upstreamDone) {
      final ExecutionStepInternal prevStep = checkForPrevious("FACETS requires a previous step");
      final WorkGuard guard = WorkGuard.forCommandDeadline(context);
      pull(prevStep, context, DEFAULT_FETCH_RECORDS_PER_PULL, guard, false);
      // the rows ended with the drain: what a LIMIT downstream left open goes now
      prevStep.close();
    } else
      computeFacets(context);
    return facets;
  }

  /**
   * For a step downstream that is done with the rows (a LIMIT reached) and is about to close the chain ending at {@code step}: a collect
   * step in that chain computes its facets first (from the rows, by reading the matches past them), so the close releases everything
   * as it does for any plan and the computed facets survive it. A collect step the rows never reached (LIMIT 0) holds nothing yet: it
   * is left to {@link #facets()}, and the caller must not close the chain.
   *
   * @return false when the caller closes the chain as usual
   */
  static boolean completeBeforeRelease(final ExecutionStepInternal step) {
    for (ExecutionStepInternal current = step; current instanceof AbstractExecutionStep abstractStep; current = abstractStep.getPrev())
      if (current instanceof FacetCollectStep collect) {
        if (collect.closed || collect.facets != null)
          return false;
        if (!collect.opened)
          return true;
        collect.facets();
        return false;
      }
    return false;
  }

  private void computeFacets(final CommandContext context) {
    // built once: a computation that failed is not attempted again on the same matches
    if (facets != null || builder == null)
      return;
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    MatchSet matches = null;
    try {
      matches = builder.build();
      builder = null;
      if (projection != null) {
        matches = MatchSet.union(projection, matches);
        projection = null;
      }
      matchCount = matches.size();
      matches.setFacetFilterIndex(filterIndexByFacet);
      final DocumentType type = targetType != null && database.getSchema().existsType(targetType) ? database.getSchema().getType(
          targetType) : null;
      facets = FacetExecutor.execute(database, matches, specs, context, type);
    } finally {
      if (matches != null)
        matches.close();
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      releaseResources();
    }
    super.close();
  }

  @Override
  public void reset() {
    releaseResources();
    builder = null;
    prevResult = null;
    upstreamDone = false;
    opened = false;
    closed = false;
    matchCount = 0;
    facets = null;
  }

  /** Releases the match set (budget reservations, projection snapshots) and every evaluator, each whatever the others do. */
  private void releaseResources() {
    try {
      if (builder != null)
        builder.close();
    } finally {
      try {
        if (projection != null) {
          projection.close();
          projection = null;
        }
      } finally {
        RuntimeException failure = null;
        for (final FacetFilterEvaluator evaluator : evaluators)
          try {
            evaluator.close();
          } catch (final RuntimeException e) {
            if (failure == null)
              failure = e;
            else
              failure.addSuppressed(e);
          }
        if (failure != null)
          throw failure;
      }
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces).append("+ COLLECT FACET MATCHES");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    final Map<String, FacetExecutionStats> stats = facets != null ? facets.executionStats() : Map.of();
    for (int i = 0; i < clause.items.size(); i++) {
      final FacetItem item = clause.items.get(i);
      result.append("\n").append(spaces).append("  ").append(item);
      final int filterIndex = filterIndexByFacet[i];
      if (filterIndex >= 0 && filterIndex < evaluators.size()) {
        final FacetFilterEvaluator evaluator = evaluators.get(filterIndex);
        result.append(" [").append(evaluator.kind());
        if (context.isProfiling() && opened && evaluator instanceof FacetFilterEvaluator.Projection projectionFilter)
          result.append(" column=").append(projectionFilter.columnAnswers()).append(" record=").append(projectionFilter.recordAnswers());
        else if (context.isProfiling() && opened && evaluator instanceof FacetFilterEvaluator.Index indexFilter
            && indexFilter.fallback() != null)
          result.append(" fallback=RECORD (").append(indexFilter.fallback()).append(")");
        result.append("]");
      }
      final FacetExecutionStats facetStats = specs != null && i < specs.size() ? stats.get(specs.get(i).alias()) : null;
      if (facetStats != null)
        result.append("\n").append(spaces).append("    source=").append(facetStats.source()).append(" segments=")
            .append(facetStats.segmentsScanned()).append(" skipped=").append(facetStats.segmentsSkipped()).append(" counting=")
            .append(facetStats.countingMode()).append(" budget=").append(facetStats.budgetBytes()).append(" cardinality=")
            .append(facetStats.cardinality());
    }
    if (context.isProfiling() && opened) {
      result.append("\n").append(spaces).append("  matchSource=").append(matchSource).append(" matches=").append(matchCount);
      if (facets != null)
        result.append(" exact=").append(facets.exact()).append(" patched=").append(facets.patched()).append(" lagMs=")
            .append(facets.lagMs());
    }
    return result.toString();
  }

  @Override
  public Result toResult() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("name", getName());
    result.setProperty("type", getType());
    result.setProperty(InternalExecutionPlan.JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("description", getDescription());
    if (facets != null)
      result.setProperty("facets", facets.toJSON().toMap());
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  /**
   * A step for another execution, with its own copy of the clause; nothing is compiled here, since the plan cache copies without a
   * context ({@code copy(null)}): the copy compiles its facets when it opens, against the context it runs in.
   */
  @Override
  public ExecutionStep copy(final CommandContext context) {
    final List<FacetFilterEvaluator> evaluatorCopies = new ArrayList<>(evaluators.size());
    for (final FacetFilterEvaluator evaluator : evaluators)
      evaluatorCopies.add(evaluator.copy());
    return new FacetCollectStep(clause.copy(), filterIndexByFacet, evaluatorCopies, targetType, projectionBuckets, context);
  }
}
