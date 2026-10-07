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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Loads the records the index entries of the previous step point to.
 * <p>
 * Built with a {@link ScanFallback}, the step does not trust the planner's choice of the index blindly (issue #8333).
 * The planner picks an index whenever its condition matches, but fetching most of a type through an index is one
 * random page access per record: cheap while the type fits the page cache, far slower than a plain scan once it does
 * not. So before loading anything the step reads the index entries alone - no record is touched - through a
 * {@link PhysicalOrderRidFetcher}, and:
 * <ul>
 *   <li>once more of them match than {@link com.arcadedb.GlobalConfiguration#QUERY_INDEX_MAX_SELECTIVITY} of the
 *   records the target buckets hold, it drops the index and serves the rows from a scan of the type filtered by the
 *   index condition, which is exactly the plan the statement gets without the index;</li>
 *   <li>otherwise it loads the matching records in physical order (bucket, then position), so the pages are swept
 *   forward once instead of visited in key order.</li>
 * </ul>
 * The decision is taken per execution, with the parameters bound, so a plan cached for {@code WHERE x > ?} serves a
 * selective value with the index and a non-selective one with the scan. The planner only builds the fallback when the
 * index order is not what the statement relies on and the scan is guaranteed to answer the same rows.
 * <p>
 * Both branches run in parallel where a scan would (issue #8333 on top of #8523): the scan is the parallel scan of the
 * type, and the physical-order load is cut in slices of the sorted record addresses that the workers of a parallel scan
 * load each, through their bucket, every page read once. An aggregation downstream runs in those workers too, with
 * the conditions the index does not answer, see {@link ParallelAggregationSource}. The threshold falls with the
 * workers the scan would run on, since the index entries are read by one thread whatever the parallelism.
 *
 * Created by luigidellaquila on 16/03/17.
 */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep implements ParallelAggregationSource {
  /**
   * What the step needs to replace the index search with a scan of the type.
   *
   * @param typeName    the type the planner targets (scanned polymorphically, like the index buckets are)
   * @param bucketNames the buckets the planner narrowed the target to, or null for all the type's buckets
   * @param keyFilter   the conditions the index search answers, to evaluate on every scanned record instead
   */
  public record ScanFallback(String typeName, Set<String> bucketNames, WhereClause keyFilter) {
    ScanFallback copy() {
      return new ScanFallback(typeName, bucketNames, keyFilter.copy());
    }
  }

  enum Strategy {INDEX_ORDER, PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED, SCAN}

  private final List<Integer> filterBucketIds;
  private final ScanFallback  scanFallback;

  // runtime: plain fields, unlike the per-thread state of Cypher's NodeIndexRangeScan, because a cached SQL plan is
  // copied for every execution (ExecutionPlanCache.get()) while a cached Cypher operator is shared by all of them
  private ResultSet                   prevResult = null;
  // The rows of a point lookup, loaded against one committed state (#9369), and the entries read past what that takes
  private ArrayDeque<Result>          consistentRows;
  private boolean                     consistentTried;
  private final ArrayDeque<Result>    pendingEntries = new ArrayDeque<>();
  private Strategy                    strategy;
  private PhysicalOrderRidFetcher     fetcher;
  private FetchFromTypeWithFilterStep scanStep;
  private long                        scanThreshold;
  private long                        matchedEntries;
  // A physical-order load shared by the workers of a parallel scan, one round per chunk of the range: decided once
  // per execution, like the strategy
  private boolean                     parallelDecided;
  private ParallelTypeScan            parallelRound;
  private int                         parallelRounds;

  /**
   * @param context         the execution context
   * @param filterBucketIds only extract values from these clusters. Pass null if no filtering is needed
   */
  public GetValueFromIndexEntryStep(final CommandContext context, final List<Integer> filterBucketIds) {
    this(context, filterBucketIds, null);
  }

  /**
   * @param scanFallback when not null, the step may serve the rows from a scan or in physical order instead of in index
   *                     order, see the class comment. Requires the previous step to be a {@link FetchFromIndexStep}
   */
  public GetValueFromIndexEntryStep(final CommandContext context, final List<Integer> filterBucketIds,
      final ScanFallback scanFallback) {
    super(context);
    this.filterBucketIds = filterBucketIds;
    this.scanFallback = scanFallback;
  }

  /**
   * Read-only access to the bucket-id constraint applied to index entries (or {@code null}
   * when no constraint is active and every value passes through). Surfaced for tests that
   * need to verify partition-aware bucket pruning narrowed the per-bucket sub-index set.
   */
  public List<Integer> getFilterBucketIds() {
    return filterBucketIds;
  }

  public ScanFallback getScanFallback() {
    return scanFallback;
  }

  /**
   * How the last execution served the rows, or null before it started. Surfaced for tests.
   */
  Strategy getStrategy() {
    return strategy;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();

    if (strategy == null)
      chooseStrategy(context, prevStep);

    return switch (strategy) {
      case SCAN -> scanStep.syncPull(context, nRecords);
      case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> {
        if (!parallelDecided) {
          parallelDecided = true;
          parallelRound = planRound(context, true);
        }
        // Once parallel, always: after the last round the fetcher still holds the last chunk, which a sequential
        // page would serve a second time
        yield parallelRounds > 0 ? parallelPhysicalOrderResultSet(context, nRecords) : physicalOrderResultSet(context, nRecords);
      }
      case INDEX_ORDER -> indexOrderResultSet(context, prevStep, nRecords);
    };
  }

  /**
   * Plans the parallel aggregation of this execution's rows (issue #8333 on top of #8523): the index entries are read
   * first, as for a sequential execution, then a range served by a scan hands the aggregation to the parallel scan of
   * the type, and one served in physical order has its records loaded by the workers of a parallel scan, a slice of
   * the sorted addresses each, chunk after chunk when the range is too large to hold at once.
   */
  @Override
  public ParallelTypeScan planParallelAggregation(final CommandContext context) {
    if (strategy != null || scanFallback == null || !ParallelTypeScan.isAllowed(context.getDatabase()))
      return null;

    chooseStrategy(context, checkForPrevious());
    parallelDecided = true;
    return switch (strategy) {
      case SCAN -> scanStep.planParallelAggregation(context);
      case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> planRound(context, true);
      case INDEX_ORDER -> null;
    };
  }

  @Override
  public ParallelTypeScan nextParallelAggregationRound(final CommandContext context) {
    return strategy == Strategy.PHYSICAL_ORDER_CHUNKED && fetcher.nextChunk() ? planRound(context, false) : null;
  }

  /**
   * Whether an execution starting now would run in parallel: never known before the index entries are read, so an
   * EXPLAIN does not claim it; a PROFILE shows how the execution went.
   */
  @Override
  public boolean wouldRunInParallel(final CommandContext context) {
    return false;
  }

  /**
   * Plans the parallel load of the records of the fetcher's current chunk, in physical order: the sorted addresses of
   * every bucket are cut in slices a worker loads each.
   *
   * @param first whether this is the first chunk, which decides for the whole execution: it goes parallel only where
   *              parallel scans are allowed and when it is large enough to share. A later chunk always does, the
   *              execution being parallel already: its entries are loaded, and answering null would drop them
   *
   * @return the scan, or null when the load stays sequential: parallel scans are not allowed here, or the first chunk
   * is too small to share
   */
  private ParallelTypeScan planRound(final CommandContext context, final boolean first) {
    final DatabaseInternal database = context.getDatabase();
    final int entriesPerUnit = ParallelTypeScan.entriesPerUnit(database, fetcher.getBufferedRids());
    // Counted before the units are built: building them hands the entries that are not record addresses over, which
    // a sequential load then would not serve
    if (first && (!ParallelTypeScan.isAllowed(database)
        || fetcher.slices(entriesPerUnit, null) + (fetcher.hasPassThrough() ? 1 : 0) < 2))
      return null;

    final ParallelTypeScan round = ParallelTypeScan.ofUnits(context, scanFallback.typeName(),
        unitsOfCurrentChunk(context, entriesPerUnit));
    if (round != null)
      ++parallelRounds;
    return round;
  }

  /**
   * The units the fetcher's current chunk is loaded in by a parallel scan: a slice of at most {@code entriesPerUnit}
   * sorted positions of one bucket each, in physical order, then the entries that are not record addresses.
   */
  private List<PhysicalOrderSliceStep> unitsOfCurrentChunk(final CommandContext context, final int entriesPerUnit) {
    final List<PhysicalOrderSliceStep> units = new ArrayList<>();
    fetcher.slices(entriesPerUnit,
        (bucketId, positions, from, to) -> units.add(PhysicalOrderSliceStep.ofPositions(context, bucketId, positions, from, to)));
    final List<Object> passThrough = fetcher.takePassThrough();
    if (passThrough != null)
      units.add(PhysicalOrderSliceStep.ofEntries(context, passThrough, GetValueFromIndexEntryStep::toResult));
    return units;
  }

  /**
   * The rows of a physical-order load shared by the workers of a parallel scan, in the order the sequential load serves
   * them: round after round, each one the parallel scan of a chunk of the range.
   */
  private ResultSet parallelPhysicalOrderResultSet(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      ResultSet page    = parallelRound != null ? parallelRound.pull(context, nRecords) : null;
      int       fetched = 0;

      @Override
      public boolean hasNext() {
        while (fetched < nRecords && parallelRound != null) {
          if (page.hasNext())
            return true;
          // The page stopped short of what was asked: this round has no row left. Every unit of it has ended, so the
          // next chunk can take over the arrays its slices read
          parallelRound.close();
          parallelRound = fetcher.nextChunk() ? planRound(context, false) : null;
          if (parallelRound != null)
            page = parallelRound.pull(context, nRecords - fetched);
        }
        return false;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        fetched++;
        return page.next();
      }
    };
  }

  private ResultSet indexOrderResultSet(final CommandContext context, final ExecutionStepInternal prevStep, final int nRecords) {
    return new ResultSet() {

      public boolean finished = false;

      Result nextItem = null;
      int    fetched  = 0;

      // Rows recovered by reconcile() for an entry whose record could not be loaded (#9369), served before the next entry
      final ArrayDeque<Result> recovered = new ArrayDeque<>();
      // The RIDs served for the key being read, kept for a non-unique index only so a recovery does not serve one twice
      final Set<RID> servedForKey     = new HashSet<>();
      Object         currentKey       = null;
      boolean        trackingOverflow = false;

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords || finished)
          return false;

        if (nextItem == null)
          fetchNextItem();

        return nextItem != null;

      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished)
          throw new NoSuchElementException();

        if (nextItem == null)
          fetchNextItem();

        if (nextItem == null)
          throw new NoSuchElementException();

        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      private void trackKey(final Object key) {
        if (!sameKey(key, currentKey)) {
          currentKey = key;
          servedForKey.clear();
          trackingOverflow = false;
        }
      }

      private void trackServed(final RID served) {
        if (servedForKey.size() >= MAX_TRACKED_RIDS_PER_KEY)
          trackingOverflow = true;
        else
          servedForKey.add(served);
      }

      private void fetchNextItem() {
        nextItem = null;
        if (finished)
          return;

        if (!recovered.isEmpty()) {
          nextItem = recovered.pollFirst();
          return;
        }

        if (!consistentTried && prevStep instanceof FetchFromIndexStep indexStep && indexStep.isPointLookup()) {
          consistentTried = true;
          consistentRows = loadPointLookup(context, indexStep, nRecords);
        }
        if (consistentRows != null) {
          nextItem = consistentRows.pollFirst();
          if (nextItem == null)
            finished = true;
          return;
        }

        if (prevResult == null) {
          prevResult = prevStep.syncPull(context, nRecords);
          if (!prevResult.hasNext() && pendingEntries.isEmpty()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          final Result val;
          if (!pendingEntries.isEmpty())
            val = pendingEntries.pollFirst();
          else {
            while (!prevResult.hasNext()) {
              prevResult = prevStep.syncPull(context, nRecords);
              if (!prevResult.hasNext()) {
                finished = true;
                return;
              }
            }
            val = prevResult.next();
          }
          final long begin = context.isProfiling() ? System.nanoTime() : 0;

          try {
            final Object finalVal = val.getProperty("rid");
            if (!passesBucketFilter(finalVal))
              continue;

            if (prevStep instanceof FetchFromIndexStep indexStep && finalVal instanceof RID rid) {
              final Object key = val.getProperty("key");
              final boolean unique = indexStep.isIndexUnique();
              if (!unique) {
                trackKey(key);
                // already served by a reconciliation of this key
                if (!trackingOverflow && servedForKey.contains(rid))
                  continue;
              }
              nextItem = toResult(finalVal, context);
              if (nextItem == null || overlappedACommit(context, indexStep)) {
                // The entry names a record that is gone, or the lookup that produced it overlapped a commit (#9369)
                recovered.addAll(reconcile(indexStep, key, rid, unique ? null : trackingOverflow ? NOT_TRACKED : servedForKey, context));
                nextItem = recovered.pollFirst();
                if (!unique && !trackingOverflow) {
                  for (final Result row : recovered)
                    servedForKey.add(row.getIdentity().orElseThrow());
                  if (nextItem != null)
                    servedForKey.add(nextItem.getIdentity().orElseThrow());
                }
              } else if (!unique && !trackingOverflow)
                trackServed(rid);
            } else
              nextItem = toResult(finalVal, context);
            if (nextItem != null)
              break;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }
    };
  }

  private ResultSet physicalOrderResultSet(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    return new ResultSet() {
      Result nextItem = null;
      int    fetched  = 0;

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords)
          return false;
        if (nextItem == null)
          nextItem = nextInPhysicalOrder(context, guard);
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
    };
  }

  private Result nextInPhysicalOrder(final CommandContext context, final WorkGuard guard) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      Object entry;
      while ((entry = fetcher.nextRecord(context.getDatabase())) != null) {
        guard.checkPeriodically((int) ++rowCount);
        final Result result = entry instanceof Record record ? new ResultInternal(record) : toResult(entry, context);
        if (result != null)
          return result;
      }
      return null;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * Reads the index entries alone, without loading a record, until either they run out or more of them match than
   * the scan threshold. See the class comment.
   */
  private void chooseStrategy(final CommandContext context, final ExecutionStepInternal prevStep) {
    // The threshold is a share of the records the target buckets hold, so it needs them named: the planner always does
    // when it builds a fallback, and a step without buckets keeps the index order
    if (scanFallback == null || !(prevStep instanceof FetchFromIndexStep indexStep) || filterBucketIds == null) {
      strategy = Strategy.INDEX_ORDER;
      return;
    }

    // The threshold falls with the workers the scan would run on: the index entries are read by one thread whatever
    // the parallelism, so the more the scan is split the sooner it wins
    final long scanThreshold = PhysicalOrderRidFetcher.scanThreshold(context.getDatabase(), filterBucketIds,
        ParallelTypeScan.plannedWorkers(context.getDatabase(), filterBucketIds));
    if (scanThreshold < 0 || !keysAreScalars(context)) {
      strategy = Strategy.INDEX_ORDER;
      return;
    }

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final boolean[] firstPass = { true };
      fetcher = new PhysicalOrderRidFetcher(() -> {
        // The first pass reads the plan's own index step; a second one, for a range too large to buffer, a fresh copy
        final FetchFromIndexStep step;
        if (firstPass[0]) {
          firstPass[0] = false;
          step = indexStep;
        } else
          step = (FetchFromIndexStep) indexStep.copy(context);
        return new IndexStepSource(step, context);
      }, scanThreshold);

      switch (fetcher.start(WorkGuard.forCommandDeadline(context))) {
      case SCAN -> {
        this.scanThreshold = scanThreshold;
        fetcher = null;
        scanStep = new FetchFromTypeWithFilterStep(scanFallback.typeName(), scanFallback.bucketNames(),
            scanFallback.keyFilter(), context, null);
        strategy = Strategy.SCAN;
      }
      case PHYSICAL_ORDER -> strategy = Strategy.PHYSICAL_ORDER;
      case PHYSICAL_ORDER_CHUNKED -> strategy = Strategy.PHYSICAL_ORDER_CHUNKED;
      }
      // Kept on the step: PROFILE is rendered after close() released the fetcher
      matchedEntries = fetcher != null ? fetcher.getMatched() : 0;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * Whether every value the index key is compared with is a single value. The index search expands a collection -
   * {@code tag = :p} with {@code :p = ['a', 'b']} seeks each element - where evaluating the same condition on a
   * record compares the value with the collection as a whole, so a search over collections keeps the index.
   */
  private boolean keysAreScalars(final CommandContext context) {
    final BooleanExpression keys = scanFallback.keyFilter().getBaseExpression();
    if (!(keys instanceof AndBlock and))
      return false;
    for (final BooleanExpression block : and.getSubBlocks()) {
      if (block instanceof BinaryCondition condition) {
        if (MultiValue.isMultiValue(condition.getRight().execute((Result) null, context)))
          return false;
      } else if (block instanceof BetweenCondition between) {
        if (MultiValue.isMultiValue(between.getSecond().execute((Result) null, context))
            || MultiValue.isMultiValue(between.getThird().execute((Result) null, context)))
          return false;
      } else
        return false;
    }
    return true;
  }

  /** One pass over the entries an index step returns, restricted to the target buckets. */
  private final class IndexStepSource implements PhysicalOrderRidFetcher.Source {
    private final FetchFromIndexStep step;
    private final CommandContext     context;

    private IndexStepSource(final FetchFromIndexStep step, final CommandContext context) {
      this.step = step;
      this.context = context;
    }

    @Override
    public Object next() {
      Identifiable value;
      while ((value = step.nextIdentifiable(context)) != null)
        if (passesBucketFilter(value))
          return value;
      return null;
    }

    @Override
    public void close() {
      step.releaseCursors();
    }
  }

  private boolean passesBucketFilter(final Object value) {
    if (filterBucketIds == null)
      return true;
    if (!(value instanceof Identifiable identifiable))
      return false;
    final int bucketId = identifiable.getIdentity().getBucketId();
    if (bucketId < 0)
      return true;
    for (final int filterClusterId : filterBucketIds)
      if (filterClusterId == bucketId)
        return true;
    return false;
  }

  private static final int      MAX_TRACKED_RIDS_PER_KEY = 256;
  private static final int      MAX_RECONCILE_ATTEMPTS   = 8;
  private static final int      MAX_POINT_LOOKUP_ENTRIES = 1024;
  private static final Set<RID> NOT_TRACKED              = Set.of();

  private static boolean sameKey(final Object a, final Object b) {
    if (a instanceof Object[] left && b instanceof Object[] right)
      return Arrays.equals(left, right);
    return Objects.equals(a, b);
  }

  /**
   * Loads the rows of a point lookup - an equality or an {@code IN} - against ONE committed state (#9369). A commit puts its
   * pages in the read cache one at a time and gives a freed record slot to the next record it creates, so entries read
   * before a commit and records loaded after it do not belong together: a row is lost when its record is gone, or served as
   * another key's when the slot was reused, and a row found in a later state can then collide with one served earlier from
   * an older one. The entries are read and every record loaded between two samples of the page manager's publication
   * sequence that are equal and even, which no commit can have interleaved with; otherwise the lookup starts again, and
   * after {@link #MAX_RECONCILE_ATTEMPTS} attempts runs under the publication lock, which keeps every commit out. Costs
   * two volatile reads when no commit overlaps.
   * <p>
   * The rows are held until all are loaded, so a lookup of more than {@link #MAX_POINT_LOOKUP_ENTRIES} entries is not
   * held: it returns null, leaving the entries read in {@link #pendingEntries} for the streaming path, which reconciles
   * them one by one.
   */
  @SuppressWarnings("unchecked")
  private ArrayDeque<Result> loadPointLookup(final CommandContext context, final FetchFromIndexStep indexStep, final int nRecords) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final PageManager pageManager = database.getPageManager();
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      for (int attempt = 0; attempt < MAX_RECONCILE_ATTEMPTS; attempt++) {
        final ArrayDeque<Result> rows = readAndLoad(context, indexStep, nRecords);
        if (rows == null)
          return null;
        final long baseline = indexStep.entriesSequence();
        if ((baseline & 1) == 0 && pageManager.getPublicationSequence() == baseline)
          return rows;
        restartLookup(database, indexStep);
      }
      final ArrayDeque<Result> rows = (ArrayDeque<Result>) pageManager.executeInLock(() -> {
        restartLookup(database, indexStep);
        return readAndLoad(context, indexStep, nRecords);
      });
      if (rows != null)
        restartLookup(database, indexStep);
      return rows;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /** The rows of every entry the index step returns, or null once there are more entries than are held. */
  private ArrayDeque<Result> readAndLoad(final CommandContext context, final FetchFromIndexStep indexStep, final int nRecords) {
    final List<Result> entries = new ArrayList<>();
    unpinRepeatableReadPages((DatabaseInternal) context.getDatabase(), indexStep);
    prevResult = indexStep.syncPull(context, nRecords);
    while (true) {
      while (prevResult.hasNext()) {
        entries.add(prevResult.next());
        if (entries.size() > MAX_POINT_LOOKUP_ENTRIES) {
          pendingEntries.addAll(entries);
          return null;
        }
      }
      prevResult = indexStep.syncPull(context, nRecords);
      if (!prevResult.hasNext())
        break;
    }
    final ArrayDeque<Result> rows = new ArrayDeque<>(entries.size());
    for (final Result entry : entries) {
      final Object value = entry.getProperty("rid");
      if (!passesBucketFilter(value))
        continue;
      final Result row = toResult(value, context);
      if (row != null)
        rows.add(row);
    }
    return rows;
  }

  private void restartLookup(final DatabaseInternal database, final FetchFromIndexStep indexStep) {
    indexStep.reset();
    prevResult = null;
    pendingEntries.clear();
  }

  /** Under REPEATABLE_READ the pages this transaction pinned may be the very two states that disagree. */
  private void unpinRepeatableReadPages(final DatabaseInternal database, final FetchFromIndexStep indexStep) {
    if (!database.isTransactionActive() || database.getTransactionIsolationLevel() != Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ)
      return;
    final List<Integer> files = new ArrayList<>(indexStep.indexFileIds());
    if (filterBucketIds != null)
      files.addAll(filterBucketIds);
    database.getTransaction().unpinFiles(files);
  }

  /**
   * Whether a commit published its pages since this step's point lookup read its index entries (#9369). A commit puts its
   * pages in the read cache one at a time, so an entry read before it can name a record that the commit has already
   * deleted - or whose slot it has already given to another record - although the key exists in the state before the commit
   * and in the state after it. Only a lookup that reads its entries when it starts is judged: a range scan reads them as it
   * goes, and reconciles the entries whose record is gone only.
   */
  private boolean overlappedACommit(final CommandContext context, final FetchFromIndexStep indexStep) {
    return indexStep.isPointLookup()
        && ((DatabaseInternal) context.getDatabase()).getPageManager().getPublicationSequence() != indexStep.entriesSequence();
  }

  /**
   * Looks an index key up again and loads its records against one committed state, for an entry that could not be served
   * as read (#9369). The lookup and the loads are repeated until no commit published in between, which the page manager's
   * publication sequence tells without a lock, and as a last resort run under the publication lock itself, which keeps any
   * commit out. Costs nothing unless an entry is dropped or a commit overlapped the lookup.
   *
   * @param served the RIDs already served for this key, so a non-unique index does not serve one twice; null for a unique
   *               index, where a key has one entry, and {@link #NOT_TRACKED} when a key held too many to remember, which
   *               leaves the entry itself to retry
   *
   * @return the rows now found for the key, empty when the key is gone or the entry was genuinely dangling
   */
  @SuppressWarnings("unchecked")
  private List<Result> reconcile(final FetchFromIndexStep indexStep, final Object key, final RID missing, final Set<RID> served,
      final CommandContext context) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final PageManager pageManager = database.getPageManager();
    for (int attempt = 0; attempt < MAX_RECONCILE_ATTEMPTS; attempt++) {
      final long before = pageManager.getPublicationSequence();
      if ((before & 1) != 0) {
        // a commit is publishing: wait for it to let go of the lock
        pageManager.executeInLock(() -> null);
        continue;
      }
      final List<Result> found = loadKeyAgain(database, indexStep, key, missing, served, context);
      if (pageManager.getPublicationSequence() == before)
        return found;
    }
    return (List<Result>) pageManager.executeInLock(() -> loadKeyAgain(database, indexStep, key, missing, served, context));
  }

  private List<Result> loadKeyAgain(final DatabaseInternal database, final FetchFromIndexStep indexStep, final Object key,
      final RID missing, final Set<RID> served, final CommandContext context) {
    if (database.isTransactionActive() && database.getTransactionIsolationLevel() == Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ) {
      // the pages this transaction pinned may be the very two states that disagree
      final List<Integer> files = new ArrayList<>(indexStep.indexFileIds());
      files.add(missing.getBucketId());
      database.getTransaction().unpinFiles(files);
    }
    final List<Result> found = new ArrayList<>(2);
    final List<RID> fresh = served == NOT_TRACKED ? null : indexStep.lookupKeyAgain(key);
    final List<RID> candidates = fresh != null ? fresh : List.of(missing);
    for (final RID rid : candidates) {
      if ((served != null && served != NOT_TRACKED && served.contains(rid)) || !passesBucketFilter(rid))
        continue;
      final Result row = toResult(rid, context, false);
      if (row != null)
        found.add(row);
    }
    return found;
  }

  /**
   * The row of an index entry, loaded through the context's database, or null when there is none. Static: the workers
   * of a parallel load call it too.
   */
  private static Result toResult(final Object value, final CommandContext context) {
    return toResult(value, context, true);
  }

  private static Result toResult(final Object value, final CommandContext context, final boolean warnIfMissing) {
    if (value instanceof RID rid) {
      try {
        // A DatabaseRID carries its origin database, so asDocument() resolves directly. For bare RIDs, route through the query's command-context
        // database instead of rid.asDocument() — the latter falls back to the thread-local active database, which is ambiguous and can pick the wrong
        // schema when multiple databases are open on the same thread.
        return new ResultInternal(
            rid instanceof DatabaseRID ? rid.asDocument() : (Document) context.getDatabase().lookupByRID(rid, true));
      } catch (final RecordNotFoundException e) {
        if (warnIfMissing)
          LogManager.instance()
              .log(GetValueFromIndexEntryStep.class, Level.WARNING, "Record %s not found. Skip it from the result set", null, value);
        return null;
      }
    } else if (value instanceof Document document)
      return new ResultInternal(document);
    else if (value instanceof Result result)
      return result;
    return null;
  }

  @Override
  public void reset() {
    prevResult = null;
    consistentRows = null;
    consistentTried = false;
    pendingEntries.clear();
    strategy = null;
    matchedEntries = 0;
    parallelDecided = false;
    parallelRounds = 0;
    releaseRuntimeSteps();
  }

  @Override
  public void close() {
    releaseRuntimeSteps();
    super.close();
  }

  private void releaseRuntimeSteps() {
    if (parallelRound != null) {
      parallelRound.close();
      parallelRound = null;
    }
    if (fetcher != null) {
      fetcher.close();
      fetcher = null;
    }
    if (scanStep != null) {
      scanStep.close();
      scanStep = null;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder(spaces).append("+ EXTRACT VALUE FROM INDEX ENTRY");

    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");

    if (filterBucketIds != null) {
      result.append("\n").append(spaces).append("  filtering buckets [");
      result.append(filterBucketIds.stream().map(x -> "" + x).collect(Collectors.joining(",")));
      result.append("]");
    }

    if (scanFallback != null) {
      result.append("\n").append(spaces).append("  in physical order, or by a full scan of ").append(scanFallback.typeName())
          .append(" when the range holds a large share of it");
      if (strategy != null) {
        result.append("\n").append(spaces).append("  served by ");
        switch (strategy) {
        case INDEX_ORDER -> result.append("index order (the share could not be estimated)");
        case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> {
          result.append("physical order (").append(matchedEntries).append(" entries matched)");
          if (parallelRounds > 0)
            result.append(", loaded in parallel");
        }
        case SCAN -> result.append("full scan (more than ").append(scanThreshold).append(" entries matched)");
        }
        if (strategy == Strategy.SCAN && scanStep != null)
          result.append("\n").append(scanStep.prettyPrint(depth + 1, indent));
      }
    }

    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new GetValueFromIndexEntryStep(context, this.filterBucketIds, scanFallback == null ? null : scanFallback.copy());
  }
}
