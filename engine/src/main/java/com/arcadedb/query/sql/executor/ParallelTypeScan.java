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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.ParallelScanProducerPool;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * One parallel execution of a type scan (issue #8523), shared by the plain scan ({@link FetchFromTypeExecutionStep})
 * and the scan with a pushed-down filter ({@link FetchFromTypeWithFilterStep}), so the filter is evaluated in the
 * workers instead of making the scan sequential.
 * <p>
 * <b>Units.</b> The work is cut in units, each one a range of pages of one bucket, scanned by the bucket step of the
 * owning fetch step (a copy restricted to the range, or the step itself when the bucket is not split). A bucket too
 * small to split is one unit; a large one is cut in ranges, so a type with a single bucket - the default - is scanned
 * in parallel too. Workers, never more than the producer pool has threads, take the units in order from a shared
 * counter. An index range loaded in physical order (issue #8333) is cut in units too, each one a slice of the sorted
 * record addresses of one bucket, see {@link #ofUnits}.
 * <p>
 * <b>Rows, in the sequential order.</b> {@link #pull} hands the rows out exactly in the order the sequential scan
 * would: every unit has its own bounded channel and the consumer drains them unit after unit. So parallelism changes
 * neither which rows a LIMIT keeps nor the order an unordered query returns. It cannot wedge: the unit the consumer
 * waits on was taken before every later one, so the worker that holds it is running and never waits on a channel the
 * consumer has not reached.
 * <p>
 * <b>Progress under a saturated pool (#8594).</b> The producer pool is JVM-wide, and the producers of a result set
 * read in part and left open stay parked on their full channels until it is closed or abandoned. A query must not wait
 * for them: the consumer never waits on a unit no worker has taken, it takes and scans that unit itself, and the
 * partial aggregation and the merge run on the caller too, which never waits for a helper the pool has not started. So
 * a saturated pool only makes a query less parallel, never stalls it.
 * <p>
 * <b>Partial aggregation.</b> {@link #aggregate} runs the rows of every unit through a per-worker partial state
 * instead of handing them out, and returns the partials for the caller to merge: the projections and the aggregation
 * run in the workers rather than on the one consumer thread.
 * <p>
 * Every producer runs on the dedicated {@link ParallelScanProducerPool}, never on the shared query pool (#4948), each
 * in its own copy of the context (#4949), and fetches its rows under the database read lock one batch at a time,
 * never across a blocking hand-off. A failed worker fails the query instead of shrinking its result (#4951), and a
 * result set nobody drains nor closes releases its workers after
 * {@link GlobalConfiguration#PARALLEL_SCAN_ABANDONED_TIMEOUT}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ParallelTypeScan {
  // How many rows a producer fetches under one database read-lock acquisition before releasing it to hand them over.
  // Small enough to keep DDL/close waits bounded, large enough to amortize the uncontended read lock to noise; and
  // batches, not single rows (#8265): one hand-off per batch instead of one per row.
  static final         int SCAN_BATCH_SIZE    = 256;
  // Channel capacity in batches per unit: the 4096-row look-ahead the single queue of the unordered scan used to have.
  private static final int UNIT_QUEUE_BATCHES = (4096 + SCAN_BATCH_SIZE - 1) / SCAN_BATCH_SIZE;
  // Units per worker a large type is cut in: more than one, so a worker that drew a slow range does not leave the
  // others idle at the end, not so many that the per-unit set-up shows.
  private static final int UNITS_PER_WORKER   = 4;
  // Entries of an index range loaded in physical order per page of the configured unit (#8333): a unit of 32 pages
  // holds at least 1024 of them. Loading a record by its address costs more than reading it in a page scan, so a unit
  // of entries is smaller than the records of a unit of pages.
  private static final int ENTRIES_PER_UNIT_PAGE = 32;
  // HOW LONG THE CALLER WAITS FOR A WORKER TO START A UNIT BEFORE IT TAKES THE UNIT FOR A DEDICATED READER (#8775)
  private static final long DEDICATED_READER_GRACE_MS = 50;
  private static final AtomicLong READER_IDS = new AtomicLong();
  // A LIST OF ITS OWN, COMPARED BY IDENTITY: List.of() IS A SHARED SINGLETON
  private static final List<Result> END_OF_UNIT = new ArrayList<>(0);

  private final DatabaseInternal     database;
  private final String               typeName;
  private final List<Unit>           units;
  private final int                  workers;
  // THE USER THE QUERY RUNS AS, BOUND ON EVERY WORKER: THE PER-BUCKET READ CHECK RUNS WHERE THE BUCKET IS OPENED, ON THE
  // WORKER, AND A THREAD WITH NO USER BOUND IS NOT CHECKED AT ALL
  private final SecurityDatabaseUser user;

  // ROW MODE
  private          UnitChannel[]           channels;
  private          List<Future<?>>         futures;
  private volatile Throwable               failure;
  // The consumer's last sign of life. Producers use it to recognize an abandoned (opened, never drained nor closed)
  // result set and free their pool thread instead of parking on a full channel forever. Starts at submission: under a
  // saturated pool a consumer slow to reach its first poll is already on the clock.
  private volatile long                    lastConsumed;
  private          int                     consumerUnit;
  // WHEN THE CONSUMER STARTED WAITING FOR A UNIT NO WORKER HAD STARTED, 0 WHEN IT IS NOT (#8775). ONLY THE CONSUMER THREAD TOUCHES IT
  // (THE ONE THAT PULLS THE RESULT SET), SO IT NEEDS NO VOLATILE
  private          long                    unitWaitSince;
  private          List<Result>            consumerBatch;
  private          int                     consumerBatchIndex;
  // THE UNIT THE CONSUMER SCANS ITSELF, BECAUSE NO WORKER HAD TAKEN IT WHEN IT GOT THERE (#8594), OR NULL
  private          AbstractExecutionStep   consumerStep;
  private          ResultSet[]             consumerCursor;
  private          CommandContext          consumerContext;
  private final    AtomicInteger           nextUnit = new AtomicInteger();
  // THE THREADS READING THE UNITS THE CALLER CLAIMED INSIDE A TRANSACTION (#8775): close() STOPS THEM
  private          BlockingQueue<Integer>  readerUnits;
  private volatile Thread                  dedicatedReader;

  private ParallelTypeScan(final DatabaseInternal database, final String typeName, final List<Unit> units) {
    this.database = database;
    this.typeName = typeName;
    this.units = units;
    this.workers = Math.min(units.size(), ParallelScanProducerPool.getInstance().getMaxParallelism());
    final DatabaseContext.DatabaseContextTL callerContext = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    this.user = callerContext != null ? callerContext.getCurrentUser() : null;
  }

  /** Binds this thread to the database as the caller: a worker reads with the caller's permissions, never with none. */
  private void initWorkerThread() {
    DatabaseContext.INSTANCE.init(database).setCurrentUser(user);
  }

  /**
   * The pages {@code [fromPage, toPage)} of the bucket {@code template} scans, or the whole of it with -1/-1. A
   * template that is not a bucket step is always one unit, run as it is.
   */
  record Unit(AbstractExecutionStep template, int fromPage, int toPage) {
    boolean isWholeTemplate() {
      return fromPage < 0;
    }
  }

  /** Consumes, in a worker, the rows of the units that worker scans. */
  @FunctionalInterface
  interface PartialSink<P> {
    /**
     * @param position where the row is in the sequential scan: its unit in the high 32 bits, its ordinal in the unit
     *                 in the low 32, so positions compare as the sequential scan orders the rows
     */
    void accept(P partial, Result row, long position, CommandContext workerContext);
  }

  /**
   * Plans a parallel execution of the bucket steps {@code bucketSteps}, in their order, or returns {@code null} when
   * this execution must stay sequential: parallel scans are disabled, the caller runs inside a transaction that has
   * written something or pins the pages it reads (the workers do not see its changes, issue #8775), on an async
   * executor thread or on a scan producer thread (a producer must never consume a nested parallel scan, #4948), or
   * there is not enough to share.
   * <p>
   * The decision is taken once, at the first pull. A transaction that writes while it drains the result does not feed
   * those writes back into a scan already running, whose workers read committed pages. This is not a statement
   * snapshot: units are read incrementally, so a commit from another thread can still reach pages read later. Rows the
   * transaction deleted after that pull can still be returned. Inside a transaction the caller never reads a unit
   * itself: one no worker has started (#8594) is read by a thread of its own, from committed pages, into the unit's
   * bounded channel, so the scan progresses on a saturated pool and never mixes two views.
   */
  static ParallelTypeScan plan(final CommandContext context, final String typeName, final List<ExecutionStep> bucketSteps) {
    final DatabaseInternal db = context.getDatabase();
    if (!isAllowed(db))
      return null;

    final List<Unit> units = planUnits(db, bucketSteps);
    if (units.size() < 2)
      return null;

    // A type with fewer buckets than the threshold goes parallel only when a bucket of it is large enough to split
    final int minBuckets = db.getConfiguration().getValueAsInteger(GlobalConfiguration.QUERY_PARALLEL_SCAN_MIN_BUCKETS);
    if (bucketSteps.size() < minBuckets && units.size() == bucketSteps.size())
      return null;

    return new ParallelTypeScan(db, typeName, units);
  }

  /**
   * A parallel execution of {@code unitSteps}, each one a unit run as it is, in their order: the slices of an index
   * range loaded in physical order (issue #8333). The caller decides whether the execution may run in parallel
   * ({@link #isAllowed}), once for all its rounds: a round of an execution already parallel must not fall back to
   * nothing because a setting changed in between.
   *
   * @return the scan, or {@code null} when there is no unit
   */
  static ParallelTypeScan ofUnits(final CommandContext context, final String typeName,
      final List<? extends AbstractExecutionStep> unitSteps) {
    final DatabaseInternal db = context.getDatabase();
    if (unitSteps.isEmpty())
      return null;

    final List<Unit> units = new ArrayList<>(unitSteps.size());
    for (final AbstractExecutionStep step : unitSteps)
      units.add(new Unit(step, -1, -1));
    return new ParallelTypeScan(db, typeName, units);
  }

  /**
   * How many of the {@code entries} of an index range loaded in physical order one unit holds (issue #8333): at least
   * {@link #ENTRIES_PER_UNIT_PAGE} per page of {@link GlobalConfiguration#QUERY_PARALLEL_SCAN_PAGES_PER_UNIT}, and more
   * on a large range, so it is cut in about {@link #UNITS_PER_WORKER} units per worker. {@link Integer#MAX_VALUE} when
   * the split is disabled: every bucket is then one unit, as in a scan.
   */
  static int entriesPerUnit(final DatabaseInternal db, final long entries) {
    final int pagesPerUnit = db.getConfiguration().getValueAsInteger(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT);
    if (pagesPerUnit <= 0)
      return Integer.MAX_VALUE;
    final long unitsWanted = (long) ParallelScanProducerPool.getInstance().getMaxParallelism() * UNITS_PER_WORKER;
    return (int) Math.min(Integer.MAX_VALUE,
        Math.max((long) pagesPerUnit * ENTRIES_PER_UNIT_PAGE, (entries + unitsWanted - 1) / unitsWanted));
  }

  static boolean isAllowed(final DatabaseInternal db) {
    return db.getConfiguration().getValueAsBoolean(GlobalConfiguration.QUERY_PARALLEL_SCAN)
        && !(Thread.currentThread() instanceof DatabaseAsyncExecutorImpl.AsyncThread)
        && !(Thread.currentThread() instanceof ParallelScanProducerPool.ProducerThread)
        && (!db.isTransactionActive() || db.getTransaction().isReadOnlyView());
  }

  /**
   * How many workers a parallel scan of the buckets {@code bucketIds}, planned now, would run on: 1 when it would stay
   * sequential. What an index range weighs a scan of the type by (issue #8333).
   */
  static int plannedWorkers(final DatabaseInternal db, final List<Integer> bucketIds) {
    if (!isAllowed(db))
      return 1;

    final int[] pages = new int[bucketIds.size()];
    for (int i = 0; i < pages.length; i++)
      pages[i] = pagesOf(db, bucketIds.get(i));
    final long pagesPerUnit = pagesPerUnit(db, pages);

    long units = 0;
    for (final int bucketPages : pages)
      units += unitsOf(bucketPages, pagesPerUnit);
    // THE SAME RULES AS plan()
    final int minBuckets = db.getConfiguration().getValueAsInteger(GlobalConfiguration.QUERY_PARALLEL_SCAN_MIN_BUCKETS);
    if (units < 2 || (pages.length < minBuckets && units == pages.length))
      return 1;
    return (int) Math.min(units, ParallelScanProducerPool.getInstance().getMaxParallelism());
  }

  private static int pagesOf(final DatabaseInternal db, final int bucketId) {
    final Bucket bucket = bucketId > -1 ? db.getSchema().getBucketByIdIfExists(bucketId) : null;
    return bucket instanceof LocalBucket localBucket ? localBucket.getTotalPages() : 0;
  }

  /**
   * The pages of a unit: at least the configured size, and larger on a large type, so it is cut in about
   * UNITS_PER_WORKER units per worker rather than in thousands of small ones. {@link Long#MAX_VALUE} when the split is
   * disabled.
   */
  private static long pagesPerUnit(final DatabaseInternal db, final int[] pages) {
    final int configuredPagesPerUnit = db.getConfiguration().getValueAsInteger(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT);
    if (configuredPagesPerUnit <= 0)
      return Long.MAX_VALUE;
    long totalPages = 0;
    for (final int bucketPages : pages)
      totalPages += bucketPages;
    final long unitsWanted = (long) ParallelScanProducerPool.getInstance().getMaxParallelism() * UNITS_PER_WORKER;
    return Math.max(configuredPagesPerUnit, (totalPages + unitsWanted - 1) / unitsWanted);
  }

  /** The units a bucket of {@code pages} pages is cut in. */
  private static long unitsOf(final int pages, final long pagesPerUnit) {
    // pages / 2, NOT 2 * pagesPerUnit: THE LATTER OVERFLOWS WHEN THE SPLIT IS DISABLED (Long.MAX_VALUE)
    return pages / 2 < pagesPerUnit ? 1 : (pages + pagesPerUnit - 1) / pagesPerUnit;
  }

  private static List<Unit> planUnits(final DatabaseInternal db, final List<ExecutionStep> bucketSteps) {
    final int[] pages = new int[bucketSteps.size()];
    for (int i = 0; i < bucketSteps.size(); i++)
      pages[i] = pagesOf(db, bucketIdOf(bucketSteps.get(i)));
    final long pagesPerUnit = pagesPerUnit(db, pages);

    final List<Unit> units = new ArrayList<>();
    for (int i = 0; i < bucketSteps.size(); i++) {
      final AbstractExecutionStep template = (AbstractExecutionStep) bucketSteps.get(i);
      if (unitsOf(pages[i], pagesPerUnit) == 1) {
        units.add(new Unit(template, -1, -1));
        continue;
      }
      for (long from = 0; from < pages[i]; from += pagesPerUnit) {
        // THE LAST RANGE IS OPEN-ENDED: IT ALSO TAKES THE PAGES THE BUCKET GROWS BY BEFORE ITS ITERATOR OPENS, AS THE
        // SEQUENTIAL SCAN WOULD
        final boolean last = from + pagesPerUnit >= pages[i];
        units.add(new Unit(template, (int) from, last ? -1 : (int) (from + pagesPerUnit)));
        if (last)
          break;
      }
    }
    return units;
  }

  private static int bucketIdOf(final ExecutionStep step) {
    if (step instanceof FetchFromClusterExecutionStep fetch)
      return fetch.getBucketId();
    if (step instanceof ScanWithFilterStep scan)
      return scan.getBucketId();
    if (step instanceof MappedScanStep scan)
      return scan.getBucketId();
    return -1;
  }

  int getUnitCount() {
    return units.size();
  }

  int getWorkerCount() {
    return workers;
  }

  Throwable getFailure() {
    return failure;
  }

  /**
   * The step that produces the rows of {@code unit}: the template itself when the unit is the whole of it, otherwise
   * a copy of it - with its own copy of the filter, so no two workers share an AST - restricted to the unit's pages.
   */
  private static AbstractExecutionStep stepFor(final Unit unit, final CommandContext workerContext) {
    if (unit.isWholeTemplate())
      return unit.template();

    final AbstractExecutionStep copy = (AbstractExecutionStep) unit.template().copy(workerContext);
    if (copy instanceof FetchFromClusterExecutionStep fetch)
      fetch.setPageRange(unit.fromPage(), unit.toPage());
    else if (copy instanceof ScanWithFilterStep scan)
      scan.setPageRange(unit.fromPage(), unit.toPage());
    else if (copy instanceof MappedScanStep scan)
      scan.setPageRange(unit.fromPage(), unit.toPage());
    return copy;
  }

  /** A split unit's copy ran on its own: charge what it measured to the template the profile shows. */
  private static void chargeProfile(final Unit unit, final AbstractExecutionStep step, final CommandContext workerContext) {
    if (step == unit.template() || !workerContext.isProfiling() || step.cost <= 0)
      return;
    final AbstractExecutionStep template = unit.template();
    synchronized (template) {
      template.cost = Math.max(template.cost, 0) + step.cost;
    }
  }

  /**
   * A context for one worker. The copy shares the database and the input parameters but owns its variables, so the
   * {@code $current} each worker writes never reaches the consumer's (#4949).
   */
  private CommandContext workerContext(final CommandContext context) {
    final CommandContext workerContext = context.copy();
    if (workerContext instanceof BasicCommandContext basic)
      basic.setDatabase(database);
    // THE INPUT-PARAMETERS MAP IS DELIBERATELY SHARED: IT IS READ-ONLY DURING EXECUTION (THE ONLY MUTATION IS
    // setInputParameters' OWN $profileExecution STRIP, A NO-OP HERE BECAUSE THE CALLER CONSUMED THE KEY AT PARSE TIME)
    workerContext.setInputParameters(context.getInputParameters());
    // setInputParameters RECOMPUTES THE PROFILING FLAG FROM THE STRIPPED MAP: RESTORE IT
    workerContext.setProfiling(context.isProfiling());
    return workerContext;
  }

  /**
   * Fetches, under one database read lock, the next batch of rows of a unit: up to {@link #SCAN_BATCH_SIZE} rows
   * examined, and fewer when they exceed the byte bound. Every row's content is loaded here, on the worker (#8265).
   *
   * @return the batch, empty when the unit has no row left, or {@code null} once it is exhausted and the batch empty
   */
  private List<Result> fetchBatch(final AbstractExecutionStep step, final CommandContext workerContext, final ResultSet[] cursor,
      final long maxBatchBytes) {
    final List<Result> batch = new ArrayList<>(SCAN_BATCH_SIZE);
    final long[] batchBytes = new long[1];
    final boolean more = database.executeInReadLock(() -> {
      ResultSet rs = cursor[0];
      if (rs == null)
        rs = cursor[0] = step.syncPull(workerContext, Integer.MAX_VALUE);
      // THE BYTE BOUND NEVER BLOCKS PROGRESS: IT IS CHECKED BEFORE ADDING, SO A SINGLE RECORD LARGER THAN maxBatchBytes
      // STILL GOES INTO ITS OWN BATCH. examined BOUNDS THE ROWS READ UNDER ONE LOCK ACQUISITION, NOT ONLY THE ROWS KEPT:
      // A FILTER OR AN AFTER-READ LISTENER REJECTING MOST ROWS WOULD OTHERWISE HOLD THE LOCK FOR A WHOLE BUCKET
      int examined = 0;
      while (examined < SCAN_BATCH_SIZE && batch.size() < SCAN_BATCH_SIZE && batchBytes[0] < maxBatchBytes) {
        if (!rs.hasNext()) {
          rs = cursor[0] = step.syncPull(workerContext, Integer.MAX_VALUE);
          if (!rs.hasNext())
            return false;
        }
        final Result r = rs.next();
        ++examined;
        if (loadContent(r)) {
          batch.add(r);
          batchBytes[0] += loadedContentBytes(r);
        }
      }
      return true;
    });
    return more || !batch.isEmpty() ? batch : null;
  }

  private long maxBatchBytes() {
    // 0 OR NEGATIVE DISABLES THE BYTE BOUND (ROW COUNT ONLY): TAKEN AS A LIMIT IT WOULD NEVER ADMIT A ROW
    final long configured = database.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_PARALLEL_SCAN_MAX_BATCH_BYTES);
    return configured > 0 ? configured : Long.MAX_VALUE;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // ROW MODE
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * One unit's rows on their way to the consumer, closed by {@link #END_OF_UNIT}: the marker, not a flag, is what
   * wakes a consumer waiting on the channel the moment the unit ends - with a flag it would sleep out its poll first,
   * and a scan whose filter keeps few rows ends most of its units with nothing else to deliver.
   */
  private static final class UnitChannel {
    final LinkedBlockingQueue<List<Result>> queue = new LinkedBlockingQueue<>(UNIT_QUEUE_BATCHES);
  }


  /**
   * Returns the next page of at most {@code nRecords} rows, in the sequential scan's order, starting the workers on
   * the first call. Every returned row is also written as {@code $current} on the consumer's context.
   */
  ResultSet pull(final CommandContext context, final int nRecords) {
    if (channels == null)
      startProducers(context);
    final long maxBatchBytes = maxBatchBytes();

    return new ResultSet() {
      int    dispatched = 0;
      Result nextItem   = null;

      @Override
      public boolean hasNext() {
        // A FAILED PRODUCER FAILS THE QUERY: FEWER ROWS REPORTED AS SUCCESS IS THE ANTI-PATTERN #4951 REMOVED
        if (failure != null)
          throw new CommandExecutionException("Parallel scan failed", failure);

        if (dispatched >= nRecords) {
          // PAGE BOUNDARY: THE CONSUMER IS ALIVE, SO A PAUSE OF THE UPSTREAM STEPS BETWEEN PAGES IS NOT ABANDONMENT
          lastConsumed = System.currentTimeMillis();
          return false;
        }
        if (nextItem != null)
          return true;

        while (nextItem == null) {
          final List<Result> batch = consumerBatch;
          if (batch != null && consumerBatchIndex < batch.size()) {
            lastConsumed = System.currentTimeMillis();
            nextItem = batch.get(consumerBatchIndex);
            batch.set(consumerBatchIndex++, null); // EARLY CLEANSE FOR GC
            break;
          }
          consumerBatch = null;

          if (consumerUnit >= channels.length) {
            stopDedicatedReader();
            return false;
          }

          if (consumerStep != null) {
            // A UNIT THE CONSUMER SCANS ITSELF: ITS ROWS NEED NO CHANNEL. AN EMPTY BATCH (ALL FILTERED AWAY) LOOPS
            lastConsumed = System.currentTimeMillis();
            final List<Result> fetched = fetchBatch(consumerStep, consumerContext, consumerCursor, maxBatchBytes);
            if (fetched != null) {
              consumerBatch = fetched;
              consumerBatchIndex = 0;
            } else {
              chargeProfile(units.get(consumerUnit), consumerStep, consumerContext);
              consumerStep = null;
              consumerCursor = null;
              ++consumerUnit;
            }
            continue;
          }

          // NO WORKER HAS TAKEN THE UNIT THE CONSUMER NEEDS: NONE OF THEM IS RUNNING, THEY ARE STILL QUEUED BEHIND THE
          // PRODUCERS OF OTHER QUERIES, WHICH A RESULT SET LEFT OPEN CAN PARK FOR THE WHOLE ABANDONMENT TIMEOUT. THE
          // CONSUMER TAKES IT AND SCANS IT ITSELF RATHER THAN WAIT FOR ROWS NOBODY IS PRODUCING (#8594)
          if (nextUnit.get() == consumerUnit && callerMayClaimNow() && nextUnit.compareAndSet(consumerUnit, consumerUnit + 1)) {
            unitWaitSince = 0;
            if (database.isTransactionActive()) {
              // INSIDE A TRANSACTION THE CALLER DOES NOT READ THE UNIT: IT WOULD SEE THE TRANSACTION'S WRITES, AND THE WORKERS'
              // UNITS NEVER DO (#8775). A THREAD OF ITS OWN READS IT FROM COMMITTED PAGES, INTO THE UNIT'S BOUNDED CHANNEL
              startDedicatedReader(context, consumerUnit);
            } else {
              channels[consumerUnit] = null;
              if (consumerContext == null)
                consumerContext = workerContext(context);
              consumerStep = stepFor(units.get(consumerUnit), consumerContext);
              consumerCursor = new ResultSet[1];
              continue;
            }
          }

          final UnitChannel channel = channels[consumerUnit];
          lastConsumed = System.currentTimeMillis();
          final List<Result> polled;
          try {
            polled = channel.queue.poll(10, TimeUnit.MILLISECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
          }
          if (failure != null)
            throw new CommandExecutionException("Parallel scan failed", failure);
          if (polled == END_OF_UNIT) {
            channels[consumerUnit] = null;
            unitWaitSince = 0;
            ++consumerUnit;
          } else if (polled != null) {
            consumerBatch = polled;
            consumerBatchIndex = 0;
          }
        }
        return true;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextItem;
        nextItem = null;
        dispatched++;
        context.setVariable("current", result);
        return result;
      }

      @Override
      public void close() {
        ParallelTypeScan.this.close();
      }
    };
  }

  /**
   * Hands the unit {@code unitIndex}, which the caller claimed because no worker had started it (#8594), to a reader
   * thread of this scan's own: a thread with no transaction reads committed pages only, as the workers do, and the rows
   * go through the unit's bounded channel like a worker's. Not the producer pool, which may be held by other result
   * sets for good and is the very thing this path must not depend on. One reader serves every unit the caller claims,
   * one after the other (it claims the next only once it has consumed the previous one), and it is created on the first.
   */
  private void startDedicatedReader(final CommandContext context, final int unitIndex) {
    if (readerUnits == null) {
      readerUnits = new LinkedBlockingQueue<>();
      final CommandContext readerContext = workerContext(context);
      final long maxBatchBytes = maxBatchBytes();
      final long abandonedTimeoutMs = database.getConfiguration().getValueAsLong(GlobalConfiguration.PARALLEL_SCAN_ABANDONED_TIMEOUT);
      final BlockingQueue<Integer> claimed = readerUnits;
      final Thread reader = new Thread(() -> {
        try {
          initWorkerThread();
          while (true) {
            // A BOUNDED WAIT, NOT A take(): A RESULT SET ABANDONED WHILE THE READER IS IDLE WOULD OTHERWISE KEEP ITS THREAD FOR EVER
            final Integer next = claimed.poll(1, TimeUnit.SECONDS);
            if (next == null) {
              if (abandonedTimeoutMs > 0 && System.currentTimeMillis() - lastConsumed > abandonedTimeoutMs)
                return;
              continue;
            }
            final int index = next;
            final Unit unit = units.get(index);
            final AbstractExecutionStep step = stepFor(unit, readerContext);
            try {
              if (!produceUnit(step, readerContext, channels[index], maxBatchBytes, abandonedTimeoutMs))
                return;
            } finally {
              chargeProfile(unit, step, readerContext);
            }
          }
        } catch (final InterruptedException e) {
          // THE SCAN IS OVER OR CLOSED: EXPECTED
          Thread.currentThread().interrupt();
        } catch (final Throwable e) {
          recordFailure(e);
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        }
      }, "ArcadeDB-parallel-scan-unit-reader-" + READER_IDS.incrementAndGet());
      reader.setDaemon(true);
      dedicatedReader = reader;
      reader.start();
    }
    readerUnits.add(unitIndex);
  }

  /**
   * Whether the caller may claim the unit it needs now. Outside a transaction it scans the unit itself, which costs
   * nothing, so at once. Inside one it costs a reader thread, so it first gives the workers a moment to start: right
   * after the scan is submitted none has run yet, and that is not saturation.
   */
  private boolean callerMayClaimNow() {
    // ONCE THE READER EXISTS SATURATION IS ESTABLISHED: THE CALLER WAITS NO MORE FOR THE UNITS AFTER THE FIRST
    if (!database.isTransactionActive() || readerUnits != null)
      return true;
    final long now = System.currentTimeMillis();
    if (unitWaitSince == 0) {
      unitWaitSince = now;
      return false;
    }
    return now - unitWaitSince >= DEDICATED_READER_GRACE_MS;
  }

  private void stopDedicatedReader() {
    final Thread reader = dedicatedReader;
    if (reader != null)
      reader.interrupt();
  }

  private void startProducers(final CommandContext context) {
    channels = new UnitChannel[units.size()];
    for (int i = 0; i < channels.length; i++)
      channels[i] = new UnitChannel();
    lastConsumed = System.currentTimeMillis();

    final long abandonedTimeoutMs = database.getConfiguration().getValueAsLong(GlobalConfiguration.PARALLEL_SCAN_ABANDONED_TIMEOUT);
    final long maxBatchBytes = maxBatchBytes();
    final ExecutorService executor = ParallelScanProducerPool.getInstance().getExecutorService();

    futures = new ArrayList<>(workers);
    for (int w = 0; w < workers; w++) {
      final CommandContext workerContext = workerContext(context);
      futures.add(executor.submit(() -> {
        initWorkerThread();
        try {
          while (true) {
            final int unitIndex = nextUnit.getAndIncrement();
            if (unitIndex >= units.size())
              return;
            final Unit unit = units.get(unitIndex);
            final AbstractExecutionStep step = stepFor(unit, workerContext);
            try {
              if (!produceUnit(step, workerContext, channels[unitIndex], maxBatchBytes, abandonedTimeoutMs))
                return;
            } finally {
              chargeProfile(unit, step, workerContext);
            }
          }
        } catch (final Throwable e) {
          // Throwable, not Exception: an Error that killed this producer must fail the query too, or the consumer
          // would take the missing unit for an empty one
          recordFailure(e);
        } finally {
          DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        }
      }));
    }
  }

  /** @return {@code false} when the producer must stop: cancelled, or its consumer is gone */
  private boolean produceUnit(final AbstractExecutionStep step, final CommandContext workerContext, final UnitChannel channel,
      final long maxBatchBytes, final long abandonedTimeoutMs) {
    final ResultSet[] cursor = new ResultSet[1];
    while (true) {
      // CANCELLATION IS SEEN HERE TOO, NOT ONLY IN THE BLOCKING offer(): A UNIT WHOSE ROWS ARE ALL FILTERED AWAY NEVER
      // OFFERS ANYTHING
      if (Thread.currentThread().isInterrupted())
        return false;

      final List<Result> fetched = fetchBatch(step, workerContext, cursor, maxBatchBytes);
      if (fetched != null && fetched.isEmpty())
        continue;
      // THE UNIT IS OVER: THE MARKER GOES THROUGH THE SAME BOUNDED OFFER AS A BATCH
      final List<Result> batch = fetched != null ? fetched : END_OF_UNIT;

      // A BOUNDED OFFER, NOT A put(): A RESULT SET OPENED BUT NEVER DRAINED NOR CLOSED WOULD OTHERWISE PARK THIS
      // PRODUCER, AND ITS POOL THREAD, FOR EVER
      while (true) {
        try {
          if (channel.queue.offer(batch, 1, TimeUnit.SECONDS))
            break;
        } catch (final InterruptedException e) {
          // CANCELLATION BY close(): EXPECTED
          Thread.currentThread().interrupt();
          return false;
        }
        if (abandonedTimeoutMs > 0 && System.currentTimeMillis() - lastConsumed > abandonedTimeoutMs) {
          recordFailure(new CommandExecutionException(
              "Parallel scan abandoned: the result set was not consumed nor closed for more than " + abandonedTimeoutMs + " ms (see "
                  + GlobalConfiguration.PARALLEL_SCAN_ABANDONED_TIMEOUT.getKey() + ", 0 disables the timeout)"));
          LogManager.instance()
              .log(this, Level.WARNING, "Parallel scan of type '%s' abandoned by its consumer: releasing the producer thread", null,
                  typeName);
          return false;
        }
      }
      if (batch == END_OF_UNIT)
        return true;
    }
  }

  private void recordFailure(final Throwable e) {
    if (failure == null)
      failure = e;
    LogManager.instance().log(this, e instanceof Error ? Level.SEVERE : Level.WARNING, "Error during parallel scan of type '%s'", e,
        typeName);
  }

  /** Stops every worker still running and drops what the consumer holds. Idempotent. */
  void close() {
    if (futures != null)
      for (final Future<?> f : futures)
        f.cancel(true);
    stopDedicatedReader();

    // A UNIT THE CONSUMER WAS SCANNING ITSELF: A COPY IS THIS SCAN'S TO CLOSE, A WHOLE TEMPLATE IS ITS OWNING STEP'S
    final AbstractExecutionStep step = consumerStep;
    if (step != null && step != units.get(consumerUnit).template())
      step.close();
    consumerStep = null;
    consumerCursor = null;
    consumerBatch = null;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // PARTIAL AGGREGATION MODE
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * Runs every row of the scan, in the workers, through a partial state each worker creates with
   * {@code partialFactory} in its own context, and returns the partials - at most one per worker, the caller's first,
   * since the caller is one of the workers - once every row has gone through one. Blocks the caller; a worker's failure
   * cancels the others and is rethrown as it was thrown when it is unchecked.
   *
   * @param onWait called on the caller's thread after every batch it scans and about every 50ms while it waits, e.g. to
   *               enforce a timeout
   */
  <P> List<P> aggregate(final CommandContext context, final Function<CommandContext, P> partialFactory, final PartialSink<P> sink,
      final Runnable onWait) {
    final long maxBatchBytes = maxBatchBytes();

    // THE CALLER IS ONE OF THE WORKERS: IT WOULD ONLY WAIT OTHERWISE, AND ON A SATURATED POOL IT SCANS EVERY UNIT
    return runAssisted(workers - 1, true, onCaller -> {
      final CommandContext workerContext = workerContext(context);
      final P partial = partialFactory.apply(workerContext);
      while (true) {
        final int unitIndex = nextUnit.getAndIncrement();
        if (unitIndex >= units.size())
          return partial;

        final Unit unit = units.get(unitIndex);
        final AbstractExecutionStep step = stepFor(unit, workerContext);
        try {
          final ResultSet[] cursor = new ResultSet[1];
          final long unitPosition = ((long) unitIndex) << 32;
          long ordinal = 0;
          List<Result> batch;
          while ((batch = fetchBatch(step, workerContext, cursor, maxBatchBytes)) != null) {
            if (Thread.currentThread().isInterrupted()) {
              if (onCaller)
                throw new CommandExecutionException("Parallel aggregation of type '" + typeName + "' interrupted");
              throw new CancellationException();
            }
            if (onCaller)
              onWait.run();
            for (final Result row : batch) {
              workerContext.setVariable("current", row);
              sink.accept(partial, row, unitPosition | ordinal++, workerContext);
            }
          }
        } finally {
          chargeProfile(unit, step, workerContext);
        }
      }
    }, onWait);
  }

  /** One run of the work {@link #runAssisted} shares between the caller and the pool. */
  @FunctionalInterface
  private interface AssistedRun<T> {
    /** @param onCaller whether this run is the caller's own, not a helper's on a pool thread */
    T run(boolean onCaller);
  }

  /**
   * Runs {@code body} on the caller and on up to {@code helpers} threads of the producer pool, every run taking its
   * work from a shared counter until none is left, and returns what the runs returned - the caller's first - once every
   * one that started is over. A helper the pool has not started by the time the caller runs out of work is skipped,
   * never waited for: there is nothing left for it, and under a pool saturated by the parked producers of result sets
   * left open the wait could last their whole abandonment timeout (#8594). A failed run cancels the others and is
   * rethrown as it was thrown when it is unchecked.
   *
   * @param bindDatabase whether a helper binds its thread to the database as the caller
   * @param onWait       called on the caller's thread about every 50ms while it waits for the helpers
   */
  private <T> List<T> runAssisted(final int helpers, final boolean bindDatabase, final AssistedRun<T> body, final Runnable onWait) {
    final ExecutorService executor = ParallelScanProducerPool.getInstance().getExecutorService();
    // PER HELPER: 0 = QUEUED, 1 = STARTED, 2 = SKIPPED. A HELPER STARTS ONLY BY WINNING 0 -> 1, SO ONE THE CALLER SKIPPED
    // RETURNS AT ONCE WHENEVER THE POOL GETS TO IT
    final AtomicIntegerArray states = new AtomicIntegerArray(helpers);
    final List<Future<T>> futures = new ArrayList<>(helpers);
    try {
      for (int h = 0; h < helpers; h++) {
        final int helper = h;
        futures.add(executor.submit(() -> {
          if (!states.compareAndSet(helper, 0, 1))
            return null;
          if (bindDatabase)
            initWorkerThread();
          try {
            return body.run(false);
          } finally {
            if (bindDatabase)
              DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
          }
        }));
      }

      final List<T> results = new ArrayList<>(helpers + 1);
      results.add(body.run(true));

      for (int h = 0; h < helpers; h++)
        states.compareAndSet(h, 0, 2);

      // WAITS ON ALL THE STARTED HELPERS AT ONCE, NOT IN SUBMISSION ORDER: A FAILED ONE STOPS THE OTHERS AS SOON AS IT FAILS
      while (true) {
        Future<T> pending = null;
        for (int h = 0; h < helpers; h++) {
          if (states.get(h) != 1)
            continue;
          final Future<T> f = futures.get(h);
          if (!f.isDone()) {
            if (pending == null)
              pending = f;
          } else
            // THROWS NOW IF THIS HELPER FAILED
            f.get();
        }
        if (pending == null)
          break;
        try {
          pending.get(50, TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
          onWait.run();
        }
      }
      for (int h = 0; h < helpers; h++)
        if (states.get(h) == 1)
          results.add(futures.get(h).get());
      return results;

    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CommandExecutionException("Parallel aggregation of type '" + typeName + "' interrupted", e);
    } catch (final ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime)
        throw runtime;
      if (cause instanceof Error error)
        throw error;
      throw new CommandExecutionException("Parallel aggregation of type '" + typeName + "' failed", cause);
    } finally {
      // A NO-OP ON SUCCESS; ON A FAILURE (OR A TIMEOUT onWait RAISED) IT STOPS THE HELPERS STILL RUNNING
      for (final Future<T> f : futures)
        f.cancel(true);
    }
  }

  /**
   * Runs independent CPU-bound tasks - the merge of partial aggregations - on the caller and, when {@code inParallel},
   * on the producer pool too, and returns once all are done. The producer pool rather than one of its own: the merge
   * runs right after this query's producers have finished, so it takes the threads they released, and it never blocks,
   * so it keeps the pool's progress guarantee (#4948). A dedicated pool would only add threads competing for the same
   * cores. The caller takes tasks too, so a pool saturated by other queries delays nothing (#8594).
   */
  void run(final List<Runnable> tasks, final boolean inParallel) {
    if (!inParallel || tasks.size() < 2) {
      for (final Runnable task : tasks)
        task.run();
      return;
    }

    final AtomicInteger nextTask = new AtomicInteger();
    runAssisted(Math.min(tasks.size(), ParallelScanProducerPool.getInstance().getMaxParallelism()) - 1, false, onCaller -> {
      int i;
      while ((i = nextTask.getAndIncrement()) < tasks.size())
        tasks.get(i).run();
      return null;
    }, () -> {
    });
  }

  // ---------------------------------------------------------------------------------------------------------------

  /**
   * Loads the content of a scanned record on the worker (#8265): left lazy, the first property access would load it
   * on whichever thread reads it first. Every record a bucket scan produces is an {@link ImmutableDocument}.
   *
   * @return {@code false} to drop a record the load found gone - deleted concurrently between the scan reading its
   * slot and this load, or filtered away by an {@code AfterRecordReadListener}, whose contract this must honor or a
   * filtered record leaks whenever nobody reads one of its properties (e.g. a bare {@code count(*)})
   */
  private static boolean loadContent(final Result result) {
    if (result instanceof ResultInternal internal && internal.element instanceof ImmutableDocument document) {
      try {
        return document.loadContent();
      } catch (final RecordNotFoundException e) {
        return false;
      }
    }
    return true;
  }

  /**
   * The size {@link #loadContent(Result)} just brought into memory, for the byte half of the batch bound: a row count
   * alone does not cap what a batch retains once the content is loaded eagerly.
   */
  private static long loadedContentBytes(final Result result) {
    if (result instanceof ResultInternal internal && internal.element instanceof ImmutableDocument document) {
      final Binary buffer = document.getBuffer();
      if (buffer != null)
        return buffer.size();
    }
    return 0L;
  }
}
