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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.facet.FacetExecutionStats;
import com.arcadedb.query.facet.FacetExecutor;
import com.arcadedb.query.facet.FacetFallbackScanner;
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

import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.IntPredicate;
import java.util.logging.Level;

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
 *   <li>{@code projection}: no WHERE over a type or buckets, and no FILTER but those an evaluator answers by RID (INDEX, PROJECTION):
 *       {@link MatchSet#fromProjection} holds every record, and each gets its fail mask by RID ({@link MatchSet#withFailMasks});
 *       {@code projection+scan} when it does not cover every bucket (no usable projection, or changed by the caller's open
 *       transaction): the records of those buckets are read by a scan of them. Both are taken when the facets are computed, after the
 *       rows (D20): the projection snapshots are not held while the application pages through the rows, and a bucket the application's
 *       transaction writes meanwhile is scanned, as the transaction reads it. When no bucket has a projection the transaction did not
 *       change already when the step opens, the rows are the source.</li>
 * </ul>
 * The index and the projection hold the records of every bucket; a bucket the current user may not read is left out of the match set
 * when it is built, so its records reach no count, no memory reservation and no PROFILE figure (the executor skips such a bucket too).
 * Neither holds what a read event does to a record (final review B-I1, {@link FacetFallbackScanner#firesReadEvents}): a bucket whose
 * records fire read events is never taken from the projection (it is scanned, or the rows are the source), and the index is not the
 * source when a bucket of the target fires them, so a record a BEFORE READ trigger hides is no match and reaches no figure.
 * <p>
 * A result set dropped before it was read to its end and never closed gives back what the step holds (memory reservations, the
 * snapshots of PROJECTION filters) once it is unreachable: a {@link Cleaner} releases them and logs a warning.
 * <p>
 * {@link ParallelRowPipeline#of} stops at this step, so an aggregation above it stays sequential: intended, it must see the rows the
 * facets were collected from.
 */
public class FacetCollectStep extends AbstractExecutionStep {
  private static final String ROWS       = "rows";
  private static final String INDEX      = "index";
  private static final String PROJECTION = "projection";
  private static final String SCAN       = "scan";

  /** Releases what an abandoned step still holds, once it is unreachable (the same approach as the query heap budget). */
  private static final Cleaner CLEANER = Cleaner.create(runnable -> {
    final Thread thread = new Thread(runnable, "ArcadeDB-FacetQuery-Cleaner");
    thread.setDaemon(true);
    return thread;
  });

  /**
   * What the step holds across rows: the match set builder (its memory reservations) and the filter evaluators (their snapshots and
   * reservations). Kept apart from the step, and never referring to it, so the {@link Cleaner} can release it once the step is
   * unreachable. Synchronized: the cleaner runs on its own thread.
   */
  private static final class Held implements Runnable {
    private final List<FacetFilterEvaluator> evaluators;
    private       MatchSet.Builder           builder;
    private       boolean                    open;

    private Held(final List<FacetFilterEvaluator> evaluators) {
      this.evaluators = evaluators;
    }

    private synchronized void opened(final MatchSet.Builder builder) {
      this.builder = builder;
      open = true;
    }

    /** The builder, which the caller now owns. */
    private synchronized MatchSet.Builder takeBuilder() {
      final MatchSet.Builder taken = builder;
      builder = null;
      return taken;
    }

    /** By the cleaner (the step became unreachable without being closed), or by {@link #close()} once it released everything. */
    @Override
    public void run() {
      if (releaseIfOpen())
        LogManager.instance().log(this, Level.WARNING,
            "A FACETS result set was not closed: what it held (memory reservations, projection snapshots) was released only once it was "
                + "garbage collected. Close every result set");
    }

    /** Releases what is held unless it was released already, in one step: a close racing the cleaner releases once, and never warns. */
    private synchronized boolean releaseIfOpen() {
      if (!open)
        return false;
      release();
      return true;
    }

    /** Releases the builder's reservations and every evaluator, each whatever the others do; idempotent. */
    private synchronized void release() {
      open = false;
      try {
        if (builder != null) {
          builder.close();
          builder = null;
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

  private final FacetsClause               clause;
  private final int[]                      filterIndexByFacet;
  /** Whether a facet carries a FILTER: every match needs its fail mask, which the index source does not give. */
  private final boolean                    anyFilter;
  private final List<FacetFilterEvaluator> evaluators;
  private final String                     targetType;
  private final int[]                      projectionBuckets;

  private final Held                       held;
  /** Registered when the step first opens: a plan-cache template or copy that never runs holds nothing and costs no registration. */
  private       Cleaner.Cleanable          cleanable;

  /** Compiled when the step opens, against the context of the execution (a cached plan is copied without one). */
  private List<FacetSpec>  specs;
  private DatabaseInternal database;
  /** The builder {@link #held} holds, read here on every row without its lock; null once the match set is built. */
  private MatchSet.Builder builder;
  /** The buckets of {@code projectionBuckets} the user may read, when the projection is the source (taken at facet time); else null. */
  private int[]            projectionReadable;
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
   * @param projectionBuckets  the buckets the match set may be taken from {@link MatchSet#fromProjection} (no WHERE, a type or buckets
   *                           as target, no FILTER that only the row answers), null when the matches must come from the rows
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
    this.held = new Held(evaluators);
  }

  /** Whether the step is registered with the cleaner: from its first open to its close. */
  boolean registeredWithCleaner() {
    return cleanable != null;
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
    final int failMask = failMask(rid, row, context);
    if (recordRows)
      // refused once over the memory budget: the rows keep flowing, the facets are unavailable
      builder.add(rid, failMask);
    return failMask;
  }

  /**
   * Bit {@code i} set when evaluator {@code i} refuses the record.
   *
   * @param row the record's row, null to have it answered by RID (an evaluator that cannot loads the record)
   */
  private int failMask(final RID rid, final Result row, final CommandContext context) {
    int failMask = 0;
    for (int i = 0; i < evaluators.size(); i++)
      if (!evaluators.get(i).passes(rid, row, context))
        failMask |= 1 << i;
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
      builder = MatchSet.builder(database, evaluators.size(), budgetWaitMs);
      // held before any evaluator opens: what one acquires is released by close(), or by the cleaner for an abandoned step
      held.opened(builder);
      if (cleanable == null)
        cleanable = CLEANER.register(this, held);
      for (final FacetFilterEvaluator evaluator : evaluators)
        evaluator.open(context);
      recordRows = true;
      matchSource = ROWS;

      // the index adds its matches without a fail mask: a FILTER needs the rows (or, without WHERE, the projection)
      final List<ExecutionStepInternal> bucketFilters = new ArrayList<>();
      final IndexedMatchSource indexed = anyFilter || targetFiresReadEvents() ? null : indexedSource(bucketFilters);
      if (indexed != null && indexed.collectAllMatches(context, builder, acceptedBuckets(bucketFilters))) {
        recordRows = false;
        matchSource = INDEX;
      } else if (projectionBuckets != null)
        openProjection();
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
   * Decides, when the step opens, whether the projection is the source: it is when a bucket the user may read has a projection and the
   * caller's open transaction did not change it yet, or when a bucket is hidden from the user (draining the rows would read it). The
   * match set itself is taken by {@link #projectionMatches} once the rows are done. Otherwise the rows are the source, as they cost
   * nothing more than reading them.
   */
  private void openProjection() {
    final int[] readable = readable(projectionBuckets);
    final boolean hidden = readable.length < projectionBuckets.length;
    if (!hidden && unchanged(projectionsOf(readable)).length == 0)
      return;
    projectionReadable = readable;
    recordRows = false;
    matchSource = PROJECTION;
  }

  /**
   * The match set of the projection source, taken when the facets are computed (D20): the buckets the caller's open transaction did
   * not change by now are taken from their projection, the records of the others (and of those without a usable projection) are read
   * by a scan of them, as the transaction reads them, into the builder. A bucket the current user may not read is left out of both:
   * its records are no match of the facets.
   *
   * @return the projection's match set, null when no bucket is taken from a projection; over budget when the budget refused it (the
   *     facets are then unavailable and nothing is scanned)
   */
  private MatchSet projectionMatches(final CommandContext context) {
    final int[] eligible = unchanged(projectionsOf(projectionReadable));
    final MatchSet projection = eligible.length == 0 ? null :
        MatchSet.fromProjection(database, eligible, GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong());
    if (projection != null && projection.isOverBudget())
      return projection;
    try {
      final int[] coveredIds = projection != null ? projection.bucketIds() : new int[0];
      if (coveredIds.length == projectionReadable.length)
        return projection;
      final boolean[] covered = new boolean[coveredIds.length == 0 ? 0 : coveredIds[coveredIds.length - 1] + 1];
      for (final int bucketId : coveredIds)
        covered[bucketId] = true;
      // the other buckets are read here, by themselves: drained from the rows, they would cost a read of every covered record too
      scanUncovered(projectionReadable, covered, context);
      matchSource = PROJECTION + "+" + SCAN;
      return projection;
    } catch (final RuntimeException | Error e) {
      if (projection != null)
        projection.close();
      throw e;
    }
  }

  /**
   * Whether a record of the target fires read events: the index, which adds every match by RID without reading a record, is then not
   * the source (the rows, read as any read does, are). A query whose target is no type asks the database's own listeners only.
   */
  private boolean targetFiresReadEvents() {
    if (targetType == null || !database.getSchema().existsType(targetType))
      return FacetFallbackScanner.firesReadEvents(database, null);
    for (final Bucket bucket : database.getSchema().getType(targetType).getBuckets(true))
      if (FacetFallbackScanner.firesReadEvents(database, database.getSchema().getTypeByBucketId(bucket.getFileId())))
        return true;
    return false;
  }

  /** The buckets of {@code bucketIds} that have a columnar projection and whose records fire no read event. */
  private int[] projectionsOf(final int[] bucketIds) {
    final ColumnarIndexManager manager = ((LocalDatabase) database.getEmbedded()).getColumnarIndexManager();
    final int[] withProjection = new int[bucketIds.length];
    int count = 0;
    for (final int bucketId : bucketIds)
      if (manager.projection(bucketId) != null && !FacetFallbackScanner.firesReadEvents(database,
          database.getSchema().getTypeByBucketId(bucketId)))
        withProjection[count++] = bucketId;
    return count == bucketIds.length ? bucketIds : Arrays.copyOf(withProjection, count);
  }

  /** The buckets of {@code bucketIds} the caller's open transaction did not change (all of them without a transaction). */
  private int[] unchanged(final int[] bucketIds) {
    if (bucketIds.length == 0 || !database.isTransactionActive())
      return bucketIds;
    final TransactionContext transaction = database.getTransaction();
    final IntHashSet single = new IntHashSet(1);
    final int[] unchanged = new int[bucketIds.length];
    int count = 0;
    for (final int bucketId : bucketIds) {
      single.clear();
      single.add(bucketId);
      if (!transaction.hasChangesInFiles(single))
        unchanged[count++] = bucketId;
    }
    return count == bucketIds.length ? bucketIds : Arrays.copyOf(unchanged, count);
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
        final Record record = records.next();
        final RID rid = record.getIdentity();
        // the record is loaded already: a FILTER is answered on it
        final int failMask = evaluators.isEmpty() ? 0 : failMask(rid, new ResultInternal(record), context);
        // refused once over the memory budget: the facets are unavailable
        if (!builder.add(rid, failMask))
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
    MatchSet projection = null;
    try {
      if (projectionReadable != null)
        projection = projectionMatches(context);
      if (projection != null && projection.isOverBudget()) {
        // the facets are unavailable: nothing was scanned into the builder
        matches = projection;
        projection = null;
      } else if (projection != null && !evaluators.isEmpty()) {
        // each of the projection's matches gets its fail mask by RID (final review C-I4): no record is read where the evaluators can tell
        final MatchSet masked = MatchSet.withFailMasks(projection, builder, rid -> failMask(rid, null, context),
            WorkGuard.forCommandDeadline(context));
        projection = null;
        matches = masked;
      } else {
        matches = builder.build();
        if (projection != null) {
          matches = MatchSet.union(projection, matches);
          projection = null;
        }
      }
      matchCount = matches.size();
      matches.setFacetFilterIndex(filterIndexByFacet);
      final DocumentType type = targetType != null && database.getSchema().existsType(targetType) ? database.getSchema().getType(
          targetType) : null;
      facets = FacetExecutor.execute(database, matches, specs, context, type);
    } finally {
      // built or not, the builder is done: closing it releases its reservations unless the match set owns them now
      builder = null;
      final MatchSet.Builder taken = held.takeBuilder();
      try {
        if (taken != null)
          taken.close();
      } finally {
        try {
          if (projection != null)
            projection.close();
        } finally {
          if (matches != null)
            matches.close();
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }
    }
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      releaseResources();
      if (cleanable != null) {
        // released above: the cleaner's action finds nothing held, and the registration goes
        cleanable.clean();
        cleanable = null;
      }
    }
    super.close();
    // reachable until here, so the cleaner never runs while close() releases
    Reference.reachabilityFence(this);
  }

  @Override
  public void reset() {
    releaseResources();
    prevResult = null;
    upstreamDone = false;
    opened = false;
    closed = false;
    projectionReadable = null;
    matchCount = 0;
    facets = null;
  }

  /** Releases the builder (budget reservations) and every evaluator, each whatever the others do. */
  private void releaseResources() {
    builder = null;
    held.release();
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
        else if (context.isProfiling() && opened && evaluator instanceof FacetFilterEvaluator.Index indexFilter) {
          if (indexFilter.fallback() != null)
            result.append(" fallback=RECORD (").append(indexFilter.fallback()).append(")");
          else
            result.append(" rids=").append(indexFilter.rids());
        }
        result.append("]");
      }
      final FacetExecutionStats facetStats = specs != null && i < specs.size() ? stats.get(specs.get(i).alias()) : null;
      if (facetStats != null) {
        result.append("\n").append(spaces).append("    source=").append(facetStats.source()).append(" segments=")
            .append(facetStats.segmentsScanned()).append(" skipped=").append(facetStats.segmentsSkipped()).append(" counting=")
            .append(facetStats.countingMode()).append(" budget=").append(facetStats.budgetBytes()).append(" cardinality=")
            .append(facetStats.cardinality());
        if (facetStats.nodeVisits() > 0)
          result.append(" nodeVisits=").append(facetStats.nodeVisits());
      }
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
    // the server's query profiler keeps this result for every plan it records: the figures of the facets, no value of the data
    if (facets != null)
      result.setProperty("facets", facets.toProfileJSON().toMap());
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
