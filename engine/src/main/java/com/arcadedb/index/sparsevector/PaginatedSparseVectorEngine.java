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
package com.arcadedb.index.sparsevector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.LongHashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.logging.Level;

/**
 * Page-component-backed orchestrator for the {@code LSM_SPARSE_VECTOR} storage backend. Each sealed
 * segment lives as a {@link SparseSegmentComponent} owned by ArcadeDB's {@code FileManager};
 * flushes and compactions run inside {@code database.transaction(...)} so the page WAL captures
 * every byte of the new segment alongside the regular transaction record - no separate fsync,
 * no flush-on-commit hook, no sparse-vector-specific recovery code.
 * <p>
 * <b>Concurrency.</b> Writes ({@link #put}, {@link #remove}) are lock-free against the memtable.
 * Reads ({@link #topK}) take an atomic snapshot of the current memtable + segment set; the
 * snapshot is stable for the duration of the query even if a flush or compaction commits a new
 * publication mid-query. Flush, compaction, and engine close serialize on a single mutator lock
 * to keep the segment-set publication ordering well-defined.
 * <p>
 * <b>Tombstone semantics: whole-document deletes only.</b> Both the BMW DAAT scorer
 * ({@link BmwScorer}) and the test-only brute-force reference scorer treat any tombstone
 * seen on an aligned dim cursor as a delete of the entire RID, not just of that one dim. A
 * workload that wants to drop only one dim from a multi-dim document while keeping the others
 * live must remove all of that document's postings and re-insert the survivors in the same
 * write batch - otherwise the document disappears from any query that mentions the dim that was
 * tombstoned. This is intentional for the document-as-sparse-vector use case (and what the
 * {@code LSMSparseVectorIndex} put / remove path exposes today, where a vertex/document delete
 * tombstones the document's whole posting set), but it is a constraint partial-dim writers must
 * be aware of. See the per-method notes on {@link #put(int, com.arcadedb.database.RID, float)}
 * and {@link #remove(int, com.arcadedb.database.RID)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class PaginatedSparseVectorEngine implements AutoCloseable {

  /**
   * Memtable posting count above which {@link #maybeFlush()} flushes to a sealed segment. Picked
   * so a memtable-heavy phase consumes O(100 MiB) heap rather than scaling unbounded with insert
   * volume; large enough that small individual commits don't each spawn their own segment file.
   */
  static final long DEFAULT_MEMTABLE_FLUSH_THRESHOLD = 1_000_000L;

  /**
   * Backpressure factor: once {@code memtable.totalPostings() >= memtableFlushThreshold * factor},
   * {@link #put} briefly waits on {@link #mutatorLock} so any in-progress flush has a chance to
   * swap the memtable out before we add another posting to it. <b>Soft block, not a rejection</b>:
   * the put always eventually proceeds; the wait is bounded by the in-progress flush duration (or
   * is essentially free when no flush is running). This is the backpressure described in
   * {@code docs/sparse-vector-storage-design.md} ("Risks - Memtable pressure under high write
   * rate"): with the soft trigger ({@code maybeFlush}) firing per commit, sustained write rate
   * exceeding flush rate would otherwise let the memtable grow unbounded between flushes and
   * eventually OOM. We deliberately do <i>not</i> call {@link #flush} from {@code put}: the
   * commit-replay path runs inside an outer {@code database.transaction(...)}, and a nested
   * transaction in the flush path would deadlock or corrupt state. Letting {@code maybeFlush}
   * (post-commit callback) drive flushes keeps the call site safe; the soft-block here just
   * ensures puts don't outrun those flushes.
   * <p>
   * <b>No hard cap; OOM risk under maybeFlush starvation.</b> The check-then-lock dance is not
   * atomic: if a flush is in flight the put queues behind it, but if the lock is uncontended
   * (no flush running) the put proceeds even when the memtable has already crossed the
   * threshold. So a workload that writes faster than the post-commit {@code maybeFlush}
   * callback can drain - typically because the {@code DatabaseAsyncExecutor} pool is saturated
   * by other work and the callback is queued, or because flushes are themselves slow on a
   * write-amplifying compaction cycle - can grow the memtable to {@code k * flushThreshold} for
   * arbitrary {@code k}, and eventually OOM. There is no hard rejection path here. Operators
   * monitoring {@code memtablePostings} on the Studio Server tab should treat sustained values
   * past {@code 2 * flushThreshold} as a signal to investigate flush throughput, not as a
   * "backpressure absorbed it" event. A genuine hard cap (rejecting writes) would require
   * surfacing the rejection through the wrapper's commit pipeline; that is intentionally not in
   * this PR.
   */
  static final long MEMTABLE_BACKPRESSURE_FACTOR = 2L;

  /**
   * Size-tiered compaction parameters. After a successful {@link #flush()} the engine groups
   * active segments into geometric tiers by posting count and, if any tier has at least
   * {@link #DEFAULT_TIER_FANOUT} segments, merges that tier's oldest {@code fanout} segments
   * into one. The merged segment lives in the next tier up by virtue of its larger posting
   * count, so subsequent flushes at tier 0 don't keep re-merging it - this is the classic
   * size-tiered compaction strategy (STCS) ArcadeDB-flavoured for sparse vectors.
   * <p>
   * <b>Why size-tiered.</b> The earlier count-tiered policy ("after every flush, if total
   * segments &gt; T, merge the oldest M") capped the segment count but kept rewriting the
   * already-large compacted segment over and over - write amplification scaled with corpus
   * size. Size-tiered amortizes write amplification at {@code O(log_fanout(N/base))} per
   * posting and keeps query merge fan-in bounded: at steady state each tier holds at most
   * {@code fanout - 1} segments, so total active segments is roughly
   * {@code (fanout - 1) * log_fanout(N / base)}. For 10M postings, base = 1M, fanout = 4
   * that is ≈5 segments; for 1B postings ≈15.
   */
  static final int  DEFAULT_TIER_FANOUT        = 4;
  /**
   * Tier 0 boundary: any segment with this many or fewer postings lives in tier 0. Subsequent
   * tiers boundary geometrically by {@link #DEFAULT_TIER_FANOUT}. Aligned with
   * {@link #DEFAULT_MEMTABLE_FLUSH_THRESHOLD} so a default-shaped flush always lands in tier 0.
   */
  static final long DEFAULT_TIER_BASE_POSTINGS = DEFAULT_MEMTABLE_FLUSH_THRESHOLD;

  /**
   * Tombstone-ratio threshold for the secondary compaction trigger (Tier 2 follow-up to #4068).
   * If size-tiered compaction has not picked up a segment but at least one segment carries
   * {@code tombstones / totalPostings >= TOMBSTONE_RATIO_TRIGGER}, the gate falls through to a
   * file-count-bounded compaction that includes the high-tombstone segment plus enough older
   * neighbors (up to {@link #DEFAULT_TIER_FANOUT}) to merge them into one. This is what
   * eventually drains delete-heavy indexes whose segments would otherwise never grow into the
   * next tier and accumulate tombstones forever. The merge runs with
   * {@code dropAllTombstones=false} so the user-visible state is preserved exactly: tombstones
   * with a matching insert in an OLDER segment outside the input set still shadow that insert
   * after the merge. The win is purely in segment count (multi-segment-with-tombstones collapse
   * into one, BMW DAAT pays a smaller per-query merge cost).
   * <p>
   * <b>Legacy segments.</b> Segments built before the manifest gained a
   * {@code totalTombstones} slot read 0L for their tombstone count and are therefore never
   * picked by this trigger - even if their actual on-disk tombstone ratio is high. There is no
   * automatic migration: rebuilding the count would require an O(dims) trailer scan per legacy
   * segment on first open, which is not free for high-vocab corpora. Operators on a database
   * that pre-dates this commit and that historically saw a delete-heavy workload should run
   * {@code REBUILD INDEX <name>} once to compact every segment into a fresh size-tiered shape;
   * subsequent flushes write the new manifest slot and the trigger applies normally.
   */
  static final double TOMBSTONE_RATIO_TRIGGER = 0.30;

  /**
   * Blocks a parallel range must hold, at minimum, for the split to be worth making (issue #4085).
   * The cuts are taken at block boundaries of the query's widest dim, so a dim with fewer than
   * {@code partitions * MIN_BLOCKS_PER_PARTITION} blocks cannot be carved into ranges that are both
   * distinct and balanced; the partition count is clipped to fit, and falls back to serial when even
   * two ranges do not.
   */
  static final int MIN_BLOCKS_PER_PARTITION = 4;

  /**
   * Ceiling on how far an explicit {@code sparseVectorScoringMaxPartitions} may oversubscribe the
   * scoring pool. The setting exists to opt out of the load gate, not out of arithmetic: each range
   * costs a full cursor stack, and past a small multiple of the pool there is no thread free to run
   * it, so the extra ranges buy queueing and memory rather than parallelism.
   */
  static final int EXPLICIT_PARTITION_OVERSUBSCRIPTION = 4;

  /**
   * A decided split: where the cuts go, and how many workers were actually claimed for it.
   * <p>
   * The two travel together because they are not derivable from each other. An explicit partition
   * count is honoured in full but its claim saturates at the pool ceiling, so the reservation can be
   * smaller than {@code boundaries.length} - and releasing the wrong number is not a visible failure,
   * it is a counter that drifts until splitting stops happening for reasons nobody can see. Carrying
   * the figure the pool actually recorded is what makes the reserve and the release the same number
   * by construction rather than by two places computing it alike.
   */
  private record PartitionPlan(RID[] boundaries, int reservedWorkers) {
  }

  /**
   * Queries that took the partitioned path (issue #4085). Whether a query is split depends on how
   * busy the scoring pool was at that instant, so this is the only way to tell after the fact which
   * shape actually ran - results are identical either way by design, which is exactly what makes the
   * choice invisible without a counter.
   */
  private final AtomicLong partitionedQueries = new AtomicLong();

  private final DatabaseInternal  database;
  private final String            indexName;
  private final SegmentParameters params;
  private final long              memtableFlushThreshold;
  private final long              memtableBackpressureThreshold;
  private final int               tierFanout;
  private final long              tierBasePostings;

  private final AtomicReference<Memtable>                  memtable     = new AtomicReference<>(new Memtable());
  private final AtomicReference<PaginatedSegmentReader[]>  segments     = new AtomicReference<>(new PaginatedSegmentReader[0]);
  private final AtomicLong                                 nextSegmentId = new AtomicLong(1L);
  private final ReentrantLock                              mutatorLock  = new ReentrantLock();
  // Content fingerprint of the FileManager's view of <i>this index's</i> sparse-segment files,
  // captured at the end of the last successful {@link #refreshSegmentsFromFileManager}. Lets the
  // next refresh short-circuit when nothing has changed (the common case under steady-state
  // querying) without paying the full reconcile cost on every {@code topK}.
  // <p>
  // Stored as a {@code long} packed as {@code (count << 32) | sumOfFileIds}: count alone
  // (the previous heuristic) missed the HA-compaction case where a SCHEMA_ENTRY adds a merged
  // segment and retires its inputs in one step, leaving the file count unchanged. Adding the
  // sum-of-ids catches that: any add or remove changes the sum even when the count is balanced.
  // Initialized to a sentinel that no real fingerprint can equal so the first call always runs
  // the full reconcile.
  private final AtomicLong lastObservedFileFingerprint = new AtomicLong(Long.MIN_VALUE);
  // Snapshot of {@link FileManager#getModificationCount()} at the last successful refresh. The
  // refresh hot path checks this O(1) counter <i>before</i> the per-file fingerprint walk: when
  // the FileManager has not added or removed a file since this engine last observed it, the
  // sparse-segment subset cannot have changed either, so we can return without paying the
  // O(total registered files) walk. Without this guard, every {@code topK} on a database with N
  // indexes pays an N-scaled cost just to confirm the segment set is stable.
  private final AtomicLong lastObservedFileManagerMods = new AtomicLong(Long.MIN_VALUE);

  private volatile boolean closed;

  public PaginatedSparseVectorEngine(final DatabaseInternal database, final String indexName, final SegmentParameters params) {
    this(database, indexName, params, DEFAULT_MEMTABLE_FLUSH_THRESHOLD,
        DEFAULT_TIER_FANOUT, DEFAULT_TIER_BASE_POSTINGS);
  }

  PaginatedSparseVectorEngine(final DatabaseInternal database, final String indexName, final SegmentParameters params,
      final long memtableFlushThreshold) {
    this(database, indexName, params, memtableFlushThreshold,
        DEFAULT_TIER_FANOUT, DEFAULT_TIER_BASE_POSTINGS);
  }

  PaginatedSparseVectorEngine(final DatabaseInternal database, final String indexName, final SegmentParameters params,
      final long memtableFlushThreshold, final int tierFanout, final long tierBasePostings) {
    if (tierFanout < 2)
      throw new IllegalArgumentException("tierFanout must be >= 2 (compaction merges at least two segments)");
    if (tierBasePostings < 1L)
      throw new IllegalArgumentException("tierBasePostings must be >= 1");
    this.database = database;
    this.indexName = indexName;
    this.params = params;
    this.memtableFlushThreshold = memtableFlushThreshold;
    this.memtableBackpressureThreshold = Math.multiplyExact(memtableFlushThreshold, MEMTABLE_BACKPRESSURE_FACTOR);
    this.tierFanout = tierFanout;
    this.tierBasePostings = tierBasePostings;
    loadExistingSegments();
  }

  /**
   * The encoding parameters new segments are written with (posting-weight quantization, block/skip
   * layout, RID compression). Frozen at engine-open time from the index metadata.
   */
  public SegmentParameters parameters() {
    return params;
  }

  /**
   * Flush the memtable iff its posting count is at or above the configured threshold; cheap no-op
   * otherwise. Called from the wrapper's post-commit callback so a long bulk-load amortizes
   * memtable cost into a few sealed segments instead of growing unbounded toward OOM.
   */
  public void maybeFlush() {
    if (memtable.get().totalPostings() >= memtableFlushThreshold)
      flush();
  }

  // --- writes ---------------------------------------------------------------

  /**
   * Adds (or updates) a posting for {@code (dim, rid)} with {@code weight}.
   * <p>
   * <b>Tombstone semantics.</b> The engine's only delete primitive is the per-(dim, rid)
   * tombstone produced by {@link #remove(int, RID)}. The BMW DAAT scorer
   * ({@link BmwScorer}) and the test-only brute-force reference scorer both treat any
   * tombstone seen on an aligned dim cursor as a delete of the entire document - they skip the
   * RID for the rest of the query, regardless of how many other dims still have live postings
   * under that RID. <b>Partial-dim updates are not supported.</b> A workload that needs to
   * "remove dim 2 from doc X while keeping dims 1 and 3 live" must remove all of doc X's
   * postings and re-insert dim 1 and dim 3 within the same write batch, otherwise doc X will
   * disappear from any query that mentions dim 2. The whole-document delete is the supported
   * use case (and what the SQL/Studio surface produces today, since the index is built per
   * document and reflects document-level deletes).
   */
  public void put(final int dim, final RID rid, final float weight) {
    ensureOpen();
    // Advisory backpressure - yields to a concurrent flush so the put lands in the swapped-out
    // memtable when one is in flight, but does NOT block until the memtable drops below the
    // threshold. After the lock release, the put adds to the (still possibly oversized) current
    // memtable. The advisory shape is intentional: a hard block here would risk a nested-transaction
    // deadlock (the commit-replay path runs inside an outer transaction). See
    // {@link #applyBackpressureIfNeeded} for the full design rationale.
    applyBackpressureIfNeeded();
    memtable.get().put(dim, rid, weight);
  }

  /**
   * Tombstones the posting for {@code (dim, rid)}. See the tombstone-semantics note on
   * {@link #put(int, RID, float)}: this is a whole-document delete signal in the scorer's view,
   * not a partial-dim update.
   * <p>
   * <b>Caller contract.</b> The supported call site is the wrapper's
   * {@link LSMSparseVectorIndex#remove(Object[], com.arcadedb.database.Identifiable)} expansion,
   * which always tombstones <i>all</i> dims of a document together (one scalar
   * {@code (dim, rid)} per non-zero dim of the original sparse vector). A caller that
   * tombstones <i>some</i> dims of a multi-dim document and not others will silently make that
   * document disappear from any query mentioning a tombstoned dim - the scorer treats any
   * tombstone-aligned cursor as a whole-doc delete, regardless of whether other dims still hold
   * live postings. There is no runtime guard against this misuse (we don't have cross-transaction
   * doc-level state at this level); if you are calling this directly, document why the
   * partial-dim semantic is acceptable for your call site.
   */
  public void remove(final int dim, final RID rid) {
    ensureOpen();
    // Advisory backpressure - same contract as in {@link #put}: yields to a concurrent flush but
    // does not hard-block.
    applyBackpressureIfNeeded();
    memtable.get().remove(dim, rid);
  }

  /**
   * Backpressure hook for {@link #put} and {@link #remove}. When the memtable has accumulated
   * more than {@link #memtableBackpressureThreshold} live entries and a flush is in flight (i.e.
   * another thread holds {@link #mutatorLock}), the calling thread briefly joins the lock queue
   * so its write happens after the in-progress flush has swapped the memtable.
   * <p>
   * <b>Advisory only - no-op when no flush is running.</b> The lock take/release is essentially
   * free when uncontended, so a put past the threshold proceeds immediately if no flush is in
   * flight. The mechanism only resists writes that race a concurrent flush; it does not block
   * writes that simply outpace the per-commit {@link #maybeFlush} callback (e.g. when the
   * post-commit callback is delayed by {@code DatabaseAsyncExecutor} pool saturation).
   * <p>
   * <b>Capacity planning.</b> The threshold is {@code MEMTABLE_BACKPRESSURE_FACTOR (=2) *
   * memtableFlushThreshold}. In an extreme write burst where flush latency exceeds the time to
   * add another {@code memtableFlushThreshold} postings, the memtable can transiently exceed
   * {@code 2 * memtableFlushThreshold} entries before the next post-commit callback drains it.
   * Sized your {@code memtableFlushThreshold} so that {@code 2x * postingBytes} fits comfortably
   * in heap with the worst-case concurrent transaction count.
   * <p>
   * Without this, sustained write rate exceeding flush rate would let the memtable grow
   * unbounded between {@link #maybeFlush} calls, and eventually OOM. See
   * {@link #MEMTABLE_BACKPRESSURE_FACTOR} for the design rationale.
   */
  private void applyBackpressureIfNeeded() {
    if (memtable.get().totalPostings() < memtableBackpressureThreshold)
      return;
    // Wait for any in-progress flush (or the next one to take the lock if maybeFlush is queued)
    // to publish its memtable swap. Re-entrant: if this thread is already inside a flush() on
    // the same engine (e.g. a future caller invokes put() from a flush sub-callback), the
    // ReentrantLock just bumps the hold count and immediately decrements it on unlock; the outer
    // flush retains the lock throughout - no deadlock, no unintended early release.
    mutatorLock.lock();
    mutatorLock.unlock();
  }

  // --- reads ----------------------------------------------------------------

  public List<RidScore> topK(final int[] queryDims, final float[] queryWeights, final int k) throws IOException {
    ensureOpen();
    if (k <= 0)
      return List.of();
    if (queryDims.length != queryWeights.length)
      throw new IllegalArgumentException("queryDims and queryWeights must have equal length");

    refreshSegmentsFromFileManager();

    final Memtable mtSnapshot = memtable.get();
    final PaginatedSegmentReader[] segSnapshot = segments.get();

    // Splitting off globally: return before touching the pool at all. getInstance() would build a
    // ThreadPoolExecutor that this JVM has just been told it will never use, which an embedded
    // deployment that disabled the feature should not be paying for.
    if (GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger() == 1)
      return rangeTopK(queryDims, queryWeights, k, mtSnapshot, segSnapshot, null, null);

    // Registered for the whole call, split or not: the split decision is made against how many
    // queries are running, and a query that stays serial still occupies its caller's thread.
    final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
    pool.queryStarted();
    try {
      final PartitionPlan plan = planPartitionBoundaries(queryDims, segSnapshot);
      if (plan != null) {
        partitionedQueries.incrementAndGet();
        pool.querySplit();
        try {
          return parallelTopK(queryDims, queryWeights, k, mtSnapshot, segSnapshot, plan.boundaries());
        } finally {
          // Hand back exactly what the pool recorded, not what the split asked for: leaking it would
          // permanently shrink what later queries believe is free, and over-releasing would let them
          // over-claim. Either way the damage is a silent drift in a number nothing else checks.
          pool.releaseWorkers(plan.reservedWorkers());
        }
      }

      return rangeTopK(queryDims, queryWeights, k, mtSnapshot, segSnapshot, null, null);
    } finally {
      pool.queryFinished();
    }
  }

  /**
   * One range of a (possibly partitioned) top-K: opens a cursor stack of its own and scores
   * {@code [startInclusive, endExclusive)}. With both bounds null this is the whole query, which is
   * the serial path.
   * <p>
   * Every piece of per-query state lives in the cursor stack built here, so this method is safe to
   * run concurrently on the scoring pool for disjoint ranges: the segment readers it reads through
   * cache their per-dim metadata behind a CAS and their pages behind the shared page cache, and the
   * memtable is a {@code ConcurrentSkipListMap}. Nothing else is shared.
   */
  private List<RidScore> rangeTopK(final int[] queryDims, final float[] queryWeights, final int k, final Memtable mtSnapshot,
      final PaginatedSegmentReader[] segSnapshot, final RID startInclusive, final RID endExclusive) throws IOException {
    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = openMergedCursor(queryDims[i], mtSnapshot, segSnapshot);
      return BmwScorer.topK(queryDims, queryWeights, cursors, k, startInclusive, endExclusive);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
    }
  }

  /** Number of {@link #topK} calls that were split into parallel RID ranges. */
  public long partitionedQueryCount() {
    return partitionedQueries.get();
  }

  /**
   * True when the calling thread sits in a transaction that has already modified pages.
   * <p>
   * Such a query must not be split. A worker resolves its page reads through its own transaction
   * context, so it sees committed pages only - it cannot see what the caller's open transaction has
   * written but not committed. That is invisible for the usual read-only query, and wrong for the
   * one that matters: a {@code put} heavy enough to trigger a memtable flush writes a whole new
   * segment inside the caller's transaction, and a query issued later in that same transaction has
   * to score it. Staying serial in that case costs a query that was already paying for a flush
   * nothing measurable, and removes the whole class of "sees stale data inside its own transaction"
   * bug.
   */
  private boolean callerHoldsUncommittedChanges() {
    final DatabaseContext.DatabaseContextTL ctx = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath());
    if (ctx == null)
      return false;
    // Every transaction on the stack, not just the innermost. begin() on an already-active
    // transaction pushes a nested one, so a caller can sit in a fresh inner transaction with no
    // changes of its own while an outer one holds modified pages. Asking only the innermost would
    // report "clean" and let the query split, and the workers - reading committed pages through a
    // context of their own - would silently not see the outer transaction's writes. That is the
    // exact class of bug this guard exists to remove, so it has to look at all of them.
    for (int i = 0; i < ctx.transactions.size(); i++) {
      final TransactionContext tx = ctx.transactions.get(i);
      if (tx != null && tx.isActive() && tx.hasChanges())
        return true;
    }
    return false;
  }

  /**
   * Decide whether to split this query into parallel RID ranges, and where the cuts go (issue #4085).
   * Returns the interior boundaries - {@code partitions - 1} of them - or {@code null} to stay on the
   * caller thread.
   * <p>
   * <b>Why this is gated rather than always on.</b> Each range runs its own traversal with its own
   * top-K watermark. A range sees a fraction of the corpus, so its watermark rises more slowly than
   * the serial scan's and it prunes less: the partitioned query is faster in wall-clock but burns
   * more CPU in total (measured at ~1.16x for a 2-way split and ~1.89x for 8-way on a learned-sparse
   * corpus). Spending that on an idle machine is free latency; spending it on a machine already
   * running other queries takes throughput away from them.
   * <p>
   * Five things turn it off:
   * <ul>
   *   <li>an explicit {@code maxPartitions == 1};</li>
   *   <li>the caller is already a scoring-pool worker - a nested fan-out on a pool with a bounded
   *       queue deadlocks, since the outer tasks hold every worker while waiting on inner tasks that
   *       nothing is left to run;</li>
   *   <li>the caller holds uncommitted page changes, which a worker's own transaction cannot see;</li>
   *   <li>the query is too small for the fan-out to pay for itself;</li>
   *   <li>no worker can be claimed - either another query holds them all, or enough queries are
   *       already in flight to keep the pool's worth of threads busy without any help. That last one
   *       is the load signal that matters and the one the pool's own counters miss, since a query
   *       runs on its caller's thread and only touches the pool if it decides to split.</li>
   * </ul>
   * Boundaries are taken from the block index of the query's widest dim, so each range holds roughly
   * the same number of that dim's postings. That is a better proxy for "same amount of work" than
   * cutting the RID space into equal spans, which goes lopsided as soon as a range of RIDs has been
   * deleted or was never dense. Reading them touches in-memory block metadata only - no page reads.
   * <p>
   * It is a balance heuristic, not a guarantee, and it reads one dim of one segment. On an index with
   * several live segments - a freshly loaded one, before compaction has merged them - that segment's
   * layout need not represent the global RID distribution, and the ranges can come out uneven.
   * Correctness does not depend on it: the ranges still partition the RID space disjointly and every
   * document is still scored exactly once. Only the speedup suffers, and it shows up as one range
   * finishing long after its siblings.
   */
  private PartitionPlan planPartitionBoundaries(final int[] queryDims, final PaginatedSegmentReader[] segSnapshot)
      throws IOException {
    final int configured = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger();
    if (configured == 1 || segSnapshot.length == 0)
      return null;
    if (SparseVectorScoringPool.isPoolThread())
      return null;
    if (callerHoldsUncommittedChanges())
      return null;

    long totalPostings = 0L;
    PaginatedDimMetadata widest = null;
    for (final PaginatedSegmentReader r : segSnapshot) {
      for (final int dim : queryDims) {
        final PaginatedDimMetadata md = r.dimMetadata(dim);
        if (md == null)
          continue;
        totalPostings += md.postingCount();
        if (widest == null || md.postingCount() > widest.postingCount())
          widest = md;
      }
    }
    if (widest == null)
      return null;
    if (totalPostings < GlobalConfiguration.SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING.getValueAsLong())
      return null;

    final SparseVectorScoringPool pool = SparseVectorScoringPool.getInstance();
    // How many ranges the block layout can carry: below MIN_BLOCKS_PER_PARTITION blocks each, the
    // cuts stop being distinct RIDs and the ranges stop being balanced enough to be worth making.
    final int byLayout = widest.blockCount() / MIN_BLOCKS_PER_PARTITION;
    if (byLayout < 2)
      return null;

    // The caller scores one range itself, so only the other ranges need a worker. Claiming them up
    // front - rather than sizing the split from how idle the pool looks - is what keeps concurrent
    // queries from all deciding to split against the same instantaneously-free capacity.
    final int partitions;
    final int reserved;
    if (configured > 1) {
      // An explicit setting is an operator opting into the CPU cost, so the split itself is granted
      // whether or not the pool is busy - it never yields. Clamped for sanity all the same: the knob
      // is "do not throttle me", not "let me open a thousand cursor stacks", and past a small
      // multiple of the pool there are no threads left to run them anyway.
      partitions = Math.min(Math.min(configured, byLayout), pool.getMaxParallelism() * EXPLICIT_PARTITION_OVERSUBSCRIPTION);
      // The claim, unlike the split, is capped at what the pool can hold, and may come back smaller
      // than asked or zero. Claiming more would suppress splitting for every concurrent query for
      // this one's whole duration, on the strength of ranges that are queued rather than running.
      reserved = pool.reserveWorkers(partitions - 1);
      pool.warnExplicitSplitUnderLoad(partitions);
    } else {
      final int wanted = Math.min(pool.getMaxParallelism(), byLayout) - 1;
      final int granted = pool.tryReserveWorkers(wanted);
      if (granted < 1) {
        // Nothing free: every worker is already promised to another query. Staying serial here is the
        // point of the reservation - splitting anyway is what costs throughput under load.
        return null;
      }
      partitions = granted + 1;
      reserved = granted;
    }

    // Everything past the claim hands it back unless it reaches the caller. The boundary walk
    // touches in-memory block metadata only and cannot currently throw, but this method reads
    // segment metadata and is declared to throw, so a future edit that adds a page read here would
    // otherwise leak the claim permanently - silently shrinking what every later query believes is
    // free, with nothing failing to point at it.
    //
    // Written as a handed-off flag rather than a catch clause on purpose. Enumerating exception
    // types is how this guard fails quietly: the obvious "catch (RuntimeException | Error)" does not
    // intercept the IOException that the page read it was written for would actually throw. A
    // finally covers every abrupt exit - checked, unchecked, Error - and any future early return
    // that forgets about the claim.
    boolean handedOff = false;
    try {
      final RID[] boundaries = new RID[partitions - 1];
      final int blocks = widest.blockCount();
      for (int i = 1; i < partitions; i++) {
        final int b = (int) ((long) blocks * i / partitions);
        boundaries[i - 1] = new RID(widest.blockFirstBucketId(b), widest.blockFirstPosition(b));
      }
      handedOff = true;
      return new PartitionPlan(boundaries, reserved);
    } finally {
      if (!handedOff)
        pool.releaseWorkers(reserved);
    }
  }

  /**
   * Score every range concurrently on {@link SparseVectorScoringPool} and merge the per-range top-K
   * lists into the global one.
   * <p>
   * The merge is exact, not approximate: the ranges partition the RID space, so every document is
   * scored exactly once and by the same query, and a document's score does not depend on which range
   * it landed in. Only pruning differs between the two shapes, and pruning never changes a score - it
   * only decides which documents are cheap enough to skip.
   * <p>
   * Drains every future even when one fails, so a failed query does not leave siblings running and
   * competing for page reads behind it, and shares one deadline across the whole fan-out so N wedged
   * ranges cannot cost N timeouts. Same shape as the per-bucket fan-out in
   * {@code SQLFunctionVectorSparseNeighbors}.
   */
  private List<RidScore> parallelTopK(final int[] queryDims, final float[] queryWeights, final int k,
      final Memtable mtSnapshot, final PaginatedSegmentReader[] segSnapshot, final RID[] boundaries) throws IOException {
    final int partitions = boundaries.length + 1;
    final ExecutorService pool = SparseVectorScoringPool.getInstance().getExecutorService();
    // Ranges [1, partitions) go to workers; the caller scores range 0 itself rather than blocking on
    // them all. That is one more range running concurrently for the same claimed capacity, and it
    // keeps a thread that would otherwise be parked doing the work it came to do.
    final List<Future<List<RidScore>>> futures = new ArrayList<>(partitions - 1);
    for (int i = 1; i < partitions; i++) {
      final RID start = boundaries[i - 1];
      final RID end = i == partitions - 1 ? null : boundaries[i];
      futures.add(pool.submit(() -> {
        // A worker thread carries no database context of its own, and the block decoder borrows its
        // scratch buffer from one, so without this the first page read fails with "Transaction
        // context not found on current thread". Establishing it explicitly rather than relying on
        // LocalDatabase.checkDatabaseIsOpen, which creates one as a side effect for any thread that
        // reaches a checked database method first: that is what keeps the per-bucket fan-out working
        // today, and it is luck rather than design - the scoring path does not otherwise need it.
        //
        // Create-only-if-absent, and tear down only what this task created, exactly as
        // checkDatabaseIsOpen does it. This task does NOT always run on a worker: the pool's queue is
        // bounded and its rejection policy is caller-runs, so once the queue fills, a submitted range
        // executes inline on the submitting thread - which is the user's, inside the user's
        // transaction. An unconditional init() there takes DatabaseContext.init's "ROLLBACK PREVIOUS
        // TXS" branch and silently rolls the caller's transaction back, and the removeContext below
        // would then wipe the context the rest of the query still needs. The identical init/remove
        // pair in FetchFromTypeExecutionStep is safe only because its pool has an unbounded queue and
        // never runs a task on the caller.
        //
        // The context stays a fresh one rather than the caller's: TransactionContext caches the pages
        // it hands out in plain HashMaps, so several workers sharing one would corrupt it. Reading
        // committed pages through a fresh context is equivalent here because a caller holding
        // uncommitted changes is not allowed to fan out at all - see planPartitionBoundaries.
        final boolean contextCreated = DatabaseContext.INSTANCE.getContextIfExists(database.getDatabasePath()) == null;
        if (contextCreated)
          DatabaseContext.INSTANCE.init(database);
        try {
          return rangeTopK(queryDims, queryWeights, k, mtSnapshot, segSnapshot, start, end);
        } finally {
          if (contextCreated)
            DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());
        }
      }));
    }

    final int timeoutSeconds = GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.getValueAsInteger();
    final long deadlineNs = timeoutSeconds > 0 ? System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds) : Long.MAX_VALUE;
    final List<List<RidScore>> ranges = new ArrayList<>(partitions);
    IOException failure = null;

    // The caller's own range, scored while the workers run. Its failure is handled with the others so
    // one bad range never leaves siblings running behind a caller that has already given up.
    try {
      ranges.add(rangeTopK(queryDims, queryWeights, k, mtSnapshot, segSnapshot, null, boundaries[0]));
    } catch (final IOException | RuntimeException e) {
      failure = e instanceof IOException io ? io : new IOException("sparse-vector range scoring failed", e);
    }

    for (final Future<List<RidScore>> f : futures) {
      try {
        // A range that has already finished is always harvested, deadline or not. The caller scores
        // its own range before getting here, so it can legitimately consume the whole budget on work
        // that succeeded; failing the query then, while every worker range sits complete and waiting
        // to be read, would be a timeout reported for nothing that is actually outstanding. The
        // deadline governs waiting, not collecting.
        if (timeoutSeconds <= 0 || f.isDone()) {
          ranges.add(f.get());
        } else {
          final long remainingNs = deadlineNs - System.nanoTime();
          if (remainingNs <= 0L)
            throw new TimeoutException("deadline elapsed before draining range");
          ranges.add(f.get(remainingNs, TimeUnit.NANOSECONDS));
        }
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        for (final Future<?> other : futures)
          other.cancel(true);
        throw new IndexException("Interrupted during sparse-vector top-K range fan-out", ie);
      } catch (final TimeoutException te) {
        for (final Future<?> other : futures)
          other.cancel(true);
        throw new IndexException("Sparse-vector top-K range fan-out timed out after " + timeoutSeconds + "s (configurable via "
            + GlobalConfiguration.SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS.getKey() + ")", te);
      } catch (final ExecutionException ee) {
        final Throwable cause = ee.getCause();
        if (failure == null)
          failure = cause instanceof IOException io ? io : new IOException("sparse-vector range scoring failed", cause);
        else
          failure.addSuppressed(cause);
      }
    }
    if (failure != null)
      throw failure;

    return BmwScorer.mergeRanges(ranges, k);
  }

  /**
   * Top-K with traversal-integrated {@code groupBy} / {@code groupSize} (issue #4071). Replaces the
   * MVP's {@code topK} + over-fetch + post-filter pattern with a per-group min-heap inside the BMW
   * DAAT loop.
   * <p>
   * <b>Not range-split</b>, unlike {@link #topK} (issue #4085). Merging grouped results is not the
   * same problem as merging plain ones: the group budget is global, so two ranges that each filled
   * their {@code limit} groups can hold more distinct keys between them than the query asked for, and
   * picking which to drop needs the per-group worst scores rather than a flat best-k. That merge
   * already exists a layer up, in the per-bucket fan-out, and pushing it down here is a separate
   * piece of work rather than a line of this one. {@code groupKeyResolver} is consulted once per scored document; {@code allowedRIDs}
   * is applied inline so callers no longer need to over-fetch to compensate for selective filters.
   *
   * @see BmwScorer#topKGrouped
   */
  public List<RidScore> topKGrouped(final int[] queryDims, final float[] queryWeights, final int limit,
      final int groupSize, final Function<RID, Object> groupKeyResolver, final Set<RID> allowedRIDs) throws IOException {
    ensureOpen();
    if (limit <= 0 || groupSize <= 0)
      return List.of();
    if (queryDims.length != queryWeights.length)
      throw new IllegalArgumentException("queryDims and queryWeights must have equal length");

    refreshSegmentsFromFileManager();

    final Memtable mtSnapshot = memtable.get();
    final PaginatedSegmentReader[] segSnapshot = segments.get();

    // Grouped queries never split, but they are still load: they run a full traversal on their
    // caller's thread and compete for the same cores. Leaving them unregistered would let a plain
    // topK arriving alongside heavy grouped traffic see an idle gate, split, and take throughput
    // from them - which is precisely what the gate exists to prevent, merely from a source it could
    // not see. Registered only when splitting is enabled at all, for the same reason as topK.
    final boolean registerLoad = GlobalConfiguration.SPARSE_VECTOR_SCORING_MAX_PARTITIONS.getValueAsInteger() != 1;
    final SparseVectorScoringPool pool = registerLoad ? SparseVectorScoringPool.getInstance() : null;
    if (pool != null)
      pool.queryStarted();

    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = openMergedCursor(queryDims[i], mtSnapshot, segSnapshot);
      return BmwScorer.topKGrouped(queryDims, queryWeights, cursors, limit, groupSize, groupKeyResolver, allowedRIDs);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
      if (pool != null)
        pool.queryFinished();
    }
  }

  /**
   * Number of distinct live (non-tombstone) postings under one dim across the memtable + active
   * segments, after newest-source-wins merging. Used to compute IDF document frequency.
   */
  public long countDim(final int dim) throws IOException {
    final DimCursor c = openMergedCursor(dim, memtable.get(), segments.get());
    if (c == null)
      return 0L;
    long df = 0L;
    try {
      c.start();
      while (!c.isExhausted()) {
        if (!c.isTombstone())
          df++;
        if (!c.advance())
          break;
      }
    } finally {
      c.close();
    }
    return df;
  }

  // --- maintenance ----------------------------------------------------------

  /**
   * Flush the current memtable to a new sealed segment. Returns the new segment id, or
   * {@code -1L} if the memtable was empty (or this server is a Raft follower - followers receive
   * segments from the leader via the standard component-shipping path).
   * <p>
   * <b>Tombstone-only memtables are persisted, not dropped.</b> Unlike
   * {@link #compactInputs} - which drops a fully-tombstoned merged segment because the inputs
   * still hold the masking tombstones - flush is the only place a memtable's tombstones land on
   * disk. Skipping a tombstone-only flush would silently lose the masks, and the older segments'
   * live postings under those RIDs would resurface on the next query. So a positive return here
   * does not imply "live data was added"; it means "a segment was sealed and registered" and the
   * segment is allowed to contain only tombstones. {@code wroteAnything}-shape suppression is
   * intentionally <i>not</i> applied. Callers that need to distinguish "real new data" from
   * "tombstones-only" should consult {@link Memtable#tombstoneCount} on the snapshot before the
   * flush, not the return value.
   * <p>
   * The build runs inside {@link DatabaseInternal#runWithCompactionReplication}: the file
   * registration and page allocations are captured by the file-manager recording session so that
   * a {@code SCHEMA_ENTRY} carrying the new component metadata + a synthetic WAL of its pages is
   * shipped to followers atomically with the leader's local commit. On a standalone (non-HA)
   * database the override is a no-op wrapper and the inner transaction is the durability point.
   * <p>
   * <b>Lock acquisition.</b> {@code mutatorLock.lock()} blocks indefinitely. In normal operation
   * the lock is held only for the duration of one flush or compaction (microseconds for
   * {@code ensureOpen} checks, seconds at most for a 1M-posting flush write), so the wait is
   * bounded. The pathological case is a stalled compaction thread or a deadlocked HA recording
   * session; those would warrant an interruptible wait or a timeout, but the cost is path-specific
   * (post-commit callback wants to retry, close-time flush wants to give up, explicit flush wants
   * to throw) and no current caller has a workload where that pathology has been observed. If
   * profiling or a real incident surfaces the case, switch to {@link ReentrantLock#tryLock(long,
   * java.util.concurrent.TimeUnit)} with a path-specific fallback.
   */
  public long flush() {
    ensureOpen();
    mutatorLock.lock();
    try {
      final Memtable old = memtable.getAndSet(new Memtable());
      if (old.isEmpty())
        return -1L;
      final long segmentId = nextSegmentId.getAndIncrement();
      final SparseSegmentComponent[] componentRef = new SparseSegmentComponent[1];
      final boolean ranOnLeader;
      try {
        ranOnLeader = database.getWrappedDatabaseInstance().runWithCompactionReplication(() -> {
          componentRef[0] = buildSegmentComponent(segmentId, old);
          // Open the reader and publish it under {@link #mutatorLock} (held by the caller) AND
          // inside the recording session, so:
          //   - the segments-array publication is a single set() with no observable gap where a
          //     concurrent {@link #topK} could see a missing segment;
          //   - {@link #refreshSegmentsFromFileManager} won't race the publication: at this point
          //     the file is on disk + registered, and the publication below puts its id into
          //     knownIds, so the refresh path's "skip if knownIds contains this id" guard prevents
          //     the duplicate-reader bug that otherwise lands on the next query.
          try {
            appendSegment(new PaginatedSegmentReader(componentRef[0]));
          } catch (final IOException e) {
            // Build succeeded (component is registered with the FileManager) but reader open
            // failed - drop the orphan so it does not leak into the next refresh scan and
            // surface as a phantom segment whose header fails validation. Best-effort drop;
            // if it also fails, attach to the suppressed chain rather than masking the real
            // cause.
            try {
              dropComponent(componentRef[0]);
            } catch (final RuntimeException dropFailure) {
              e.addSuppressed(dropFailure);
            }
            throw new IndexException("Failed to open freshly-flushed sparse segment '" + indexName + "_seg" + segmentId + "'",
                e);
          }
          return Boolean.TRUE;
        });
      } catch (final InterruptedException e) {
        // Restore the interrupt flag so callers higher up the stack can detect cancellation.
        Thread.currentThread().interrupt();
        throw new IndexException("Failed to flush sparse vector engine '" + indexName + "'", e);
      } catch (final IOException e) {
        throw new IndexException("Failed to flush sparse vector engine '" + indexName + "'", e);
      }
      if (!ranOnLeader)
        return -1L;
      // Size-tiered auto-compaction gate. Run synchronously under {@code mutatorLock} (which
      // {@link #compactInputs} reacquires reentrantly): a long bulk-load that fires back-to-back
      // flushes would otherwise leave the engine with one segment per flush, and BMW DAAT would
      // pay a per-segment merge cost on every query. Cascade in case the merged segment promotes
      // its tier to also-overflowing - the loop terminates because each pass moves at least one
      // segment up a tier and tiers are bounded by the corpus size.
      while (compactSizeTiered() != -1L)
        ;
      return segmentId;
    } finally {
      mutatorLock.unlock();
    }
  }

  /**
   * Force a full compaction of every active segment into one. Returns the new segment id, or
   * {@code -1L} if there is nothing to compact (zero or one segment, the merge produced an
   * empty result, or this server is a Raft follower - followers receive the merged segment from
   * the leader instead of compacting independently).
   */
  public long compactAll() {
    return compactInputs(/* dropAllTombstones */ true, active -> active.length < 2 ? null
        : sortedCopy(active));
  }

  /** Compact the {@code count} oldest segments into one. */
  public long compactOldest(final int count) {
    if (count < 2)
      return -1L;
    return compactInputs(/* dropAllTombstones */ false, active -> {
      if (active.length < count)
        return null;
      final PaginatedSegmentReader[] sortedAll = sortedCopy(active);
      return Arrays.copyOf(sortedAll, count);
    });
  }

  /**
   * One pass of size-tiered compaction. Groups active segments by geometric tier on posting
   * count and, if any tier holds at least {@link #tierFanout} segments, merges that tier's
   * oldest {@code tierFanout} into one. Returns the new merged segment id, or {@code -1L} if
   * no tier overflowed (so the caller's cascading loop can stop).
   * <p>
   * <b>Sentinel conflation, intentional.</b> Three "stop the cascade" cases all return
   * {@code -1L}: (a) no tier overflowed, (b) this server is a Raft follower (followers receive
   * segments via Raft replication instead of compacting locally), and (c) the merged segment
   * was empty. The {@code while (compactSizeTiered() != -1L)} loop in {@link #flush}
   * intentionally collapses them - in all three cases there is no productive work this engine
   * can do on this pass. Case (c) is virtually unreachable from this entry point because
   * {@code compactSizeTiered} passes {@code dropAllTombstones=false}, so any input dim with at
   * least one posting (live or tombstone) emits at least one entry into the merged segment;
   * if a future variant of the policy drops tombstones, the cascade may want a richer return
   * type so an empty merge in tier {@code N} doesn't stop the cascade from inspecting tiers
   * {@code N+1, N+2, ...}.
   * <p>
   * Tier assignment is purely a function of {@code totalPostings()}, so no on-disk metadata
   * change is needed; the merged segment naturally promotes itself into the next tier up by
   * virtue of having more postings than its inputs.
   */
  public long compactSizeTiered() {
    return compactInputs(/* dropAllTombstones */ false, active -> {
      if (active.length < tierFanout)
        return null;
      // Bucket by tier. Within each tier, sort by segment id (oldest first) so the merge picks
      // a contiguous run of older segments and leaves any tier-mate that arrived more recently
      // for the next pass.
      // <p>
      // FUTURE: this is called inside the {@code while (compactSizeTiered() != -1L)} cascade in
      // {@link #flush}, so on a bulk-load it allocates a fresh {@code HashMap} + per-tier
      // {@code ArrayList}s on every cascade tick. Tier count is bounded by
      // {@code log_fanout(maxPostings)} (~20 worst case), so a pre-allocated fixed-size arrays
      // approach (e.g. {@code IntObjectMap}-style with the max plausible tier count) would
      // eliminate these allocations on the hot flush path. Not done yet because the current
      // numbers are dominated by the merge itself, not the bookkeeping; revisit if a profile
      // shows otherwise.
      final Map<Integer, List<PaginatedSegmentReader>> byTier = new HashMap<>();
      for (final PaginatedSegmentReader r : active) {
        final int t = tierOf(r.totalPostings());
        byTier.computeIfAbsent(t, k -> new ArrayList<>()).add(r);
      }
      // Primary trigger: pick the lowest-tier overflow first so write amplification stays minimal.
      final int[] sortedTiers = byTier.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
      for (final int t : sortedTiers) {
        final List<PaginatedSegmentReader> sameTier = byTier.get(t);
        if (sameTier.size() < tierFanout)
          continue;
        sameTier.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
        return sameTier.subList(0, tierFanout).toArray(new PaginatedSegmentReader[0]);
      }
      // Secondary trigger: tombstone-ratio cleanup. A delete-heavy index where segments never
      // grow into the next tier would otherwise accumulate small tombstone-rich segments that BMW
      // DAAT must still walk on every query. Once the corpus has at least {@code tierFanout}
      // segments AND at least one of them is past {@link #TOMBSTONE_RATIO_TRIGGER}, pick that
      // segment plus its (fanout - 1) oldest neighbors so the merge collapses file count.
      // Gating on {@code active.length >= tierFanout} (already enforced by the early return above)
      // prevents the 2-segment ping-pong where every flush re-merges the same pair.
      PaginatedSegmentReader heaviestTombstoneSeg = null;
      double heaviestRatio = 0.0;
      for (final PaginatedSegmentReader r : active) {
        final long total = r.totalPostings();
        if (total <= 0L)
          continue;
        final double ratio = (double) r.tombstoneCount() / (double) total;
        if (ratio >= TOMBSTONE_RATIO_TRIGGER && ratio > heaviestRatio) {
          heaviestTombstoneSeg = r;
          heaviestRatio = ratio;
        }
      }
      if (heaviestTombstoneSeg == null)
        return null;
      // Pair the high-tombstone segment with the (fanout - 1) oldest neighbors so newest-wins
      // precedence inside mergeIntoBuilder lines up with on-disk order.
      // <p>
      // Tier mixing here is intentional: the trigger's goal is file-count reduction (drain the
      // delete-heavy index of small tombstone-rich segments BMW DAAT must walk on every query),
      // not the write-amplification minimisation that the primary size-tiered branch optimises
      // for. Pulling the oldest neighbors regardless of tier is a single-pass collapse; the next
      // flush will rebalance the tier distribution naturally.
      final List<PaginatedSegmentReader> all = new ArrayList<>(active.length);
      all.addAll(Arrays.asList(active));
      all.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
      final List<PaginatedSegmentReader> picked = new ArrayList<>(tierFanout);
      final long heavyId = heaviestTombstoneSeg.segmentId();
      picked.add(heaviestTombstoneSeg);
      for (final PaginatedSegmentReader r : all) {
        if (picked.size() >= tierFanout)
          break;
        if (r.segmentId() == heavyId)
          continue;
        picked.add(r);
      }
      picked.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
      return picked.toArray(new PaginatedSegmentReader[0]);
    });
  }

  private int tierOf(final long postings) {
    if (postings <= tierBasePostings)
      return 0;
    // log_fanout(postings / base). Floor by integer division on the log values.
    return (int) (Math.log((double) postings / (double) tierBasePostings) / Math.log(tierFanout));
  }

  private static PaginatedSegmentReader[] sortedCopy(final PaginatedSegmentReader[] in) {
    final PaginatedSegmentReader[] out = Arrays.copyOf(in, in.length);
    Arrays.sort(out, Comparator.comparingLong(PaginatedSegmentReader::segmentId));
    return out;
  }

  private long compactInputs(final boolean dropAllTombstones,
      final Function<PaginatedSegmentReader[], PaginatedSegmentReader[]> pickInputs) {
    ensureOpen();
    mutatorLock.lock();
    try {
      final PaginatedSegmentReader[] active = segments.get();
      final PaginatedSegmentReader[] inputs = pickInputs.apply(active);
      if (inputs == null || inputs.length < 2)
        return -1L;

      final long newId = nextSegmentId.getAndIncrement();
      final SparseSegmentComponent[] componentRef = new SparseSegmentComponent[1];
      final boolean[] wroteAnything = { false };
      final boolean ranOnLeader;
      try {
        ranOnLeader = database.getWrappedDatabaseInstance().runWithCompactionReplication(() -> {
          componentRef[0] = createComponent(newId);
          try {
            // No enclosing database.transaction(): the builder streams merged pages straight to
            // disk in RAM-bounded chunks (issue #5189) so a compacted segment past the WALFile
            // 2 GB per-transaction ceiling - the FP32-at-10M case - compacts fine. HA replicates
            // the finished file via serializeFilePagesAsWal reading its on-disk pages.
            try (final SparseSegmentBuilder b = new SparseSegmentBuilder(componentRef[0], params, segmentBuildChunkBytes())) {
              b.setSegmentId(newId);
              final long[] parentIds = new long[inputs.length];
              for (int i = 0; i < inputs.length; i++)
                parentIds[i] = inputs[i].segmentId();
              b.setParentSegments(parentIds);
              try {
                wroteAnything[0] = mergeIntoBuilder(b, inputs, dropAllTombstones);
              } catch (final IOException e) {
                throw new IndexException("Failed to merge sparse segments during compaction", e);
              }
              b.finish();
            }
          } catch (final RuntimeException buildFailure) {
            // Same orphan-protection as flush(): drop the partial component so the next
            // refreshSegmentsFromFileManager scan doesn't try to open an empty file.
            try {
              dropComponent(componentRef[0]);
            } catch (final RuntimeException dropFailure) {
              buildFailure.addSuppressed(dropFailure);
            }
            throw buildFailure;
          }
          // Build succeeded; from here on any throw needs to drop {@code componentRef[0]} so the
          // partial-but-registered component does not leak into the FileManager. Track ownership
          // with a {@code componentHandled} flag: it flips to {@code true} when the component is
          // either explicitly disposed (empty-merge path drops it; reader-open failure drops it)
          // or successfully transferred to the segments array via {@link #replaceSegments}. The
          // outer {@code finally} block reads the flag and drops {@code componentRef[0]} only if
          // nothing else has - covering the previously-unguarded gap of "drain throws" or
          // "replaceSegments throws".
          final boolean[] componentHandled = { false };
          try {
            // Drain the page cache's async writer so the synthetic WAL HA ships in this same
            // recording session sees the final on-disk pages instead of zeros. If the bounded wait gives up
            // (#4928), shipping would send zeros to followers: abort, the flush retries later.
            if (!database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database))
              throw new IOException("Sparse-vector flush aborted: pages are still pending flush after the no-progress timeout");
            // Open the reader and swap segments under {@link #mutatorLock} (held by the caller) AND
            // inside the recording session, in a single CAS. Doing the retire-old + add-new step in
            // one {@link #replaceSegments} call closes the ghost-window where a concurrent
            // {@link #topK} could otherwise see neither the inputs nor the merged segment, and stops
            // {@link #refreshSegmentsFromFileManager} from racing in to open the new component
            // before this thread publishes it (which would land a duplicate reader for the same
            // segment id and double-count scores).
            if (!wroteAnything[0]) {
              // Empty merge (everything was tombstoned): drop the empty new component and the
              // inputs together. The recording session sees the create+delete pair on the new
              // component as a wash, and the inputs go away cleanly on followers too.
              dropComponent(componentRef[0]);
              componentHandled[0] = true;
              replaceSegments(inputs, /* maybeNew */ null);
            } else {
              final PaginatedSegmentReader newReader;
              try {
                newReader = new PaginatedSegmentReader(componentRef[0]);
              } catch (final IOException e) {
                // Defensive: a successful build + drain should never produce a reader that fails
                // header validation, but if it does, drop the orphan and surface the real cause.
                dropComponent(componentRef[0]);
                componentHandled[0] = true;
                throw new IndexException(
                    "Failed to open freshly-compacted sparse segment '" + indexName + "_seg" + newId + "'", e);
              }
              replaceSegments(inputs, newReader);
              componentHandled[0] = true;
            }
          } finally {
            if (!componentHandled[0]) {
              try {
                dropComponent(componentRef[0]);
              } catch (final RuntimeException ignored) {
                // best-effort: an in-flight throwable will already be carrying the primary cause
              }
            }
          }
          return Boolean.TRUE;
        });
      } catch (final InterruptedException e) {
        // Restore the interrupt flag so callers higher up the stack can detect cancellation.
        Thread.currentThread().interrupt();
        throw new IndexException("Failed to compact sparse vector engine '" + indexName + "'", e);
      } catch (final IOException e) {
        throw new IndexException("Failed to compact sparse vector engine '" + indexName + "'", e);
      }
      if (!ranOnLeader)
        return -1L;
      if (!wroteAnything[0])
        return -1L;
      return newId;
    } finally {
      mutatorLock.unlock();
    }
  }

  // --- introspection --------------------------------------------------------

  public long memtablePostings() {
    return memtable.get().totalPostings();
  }

  /**
   * <b>Total entries across the memtable and all sealed segments, including tombstones.</b>
   * This is the on-disk + in-memory entry count, not the live-document count - a value
   * suitable for sizing/operational metrics ("how big is this index"), not for telling a user
   * how many documents would match a query that hits this index. Live count would require
   * either summing per-dim {@code df} across every dim of every segment (an O(total dims) walk)
   * or persisting a per-segment live aggregate in the segment header (a format change). Neither
   * has been needed yet; if a caller wants to distinguish, expose {@link #memtableTombstones()}
   * for the in-memory portion and treat segment {@code totalPostings} as an upper bound until a
   * persisted live aggregate lands.
   */
  public long totalPostings() {
    long total = memtable.get().totalPostings();
    for (final PaginatedSegmentReader r : segments.get())
      total += r.totalPostings();
    return total;
  }

  /**
   * Number of tombstones currently held in the memtable. Tracked exactly (not estimated) by
   * {@link Memtable} on every {@link Memtable#put} / {@link Memtable#remove}, so this is cheap
   * and accurate at the memtable level. Sealed segments do not surface their tombstone count
   * directly - it is stored per-dim in the trailer (as {@code postingCount - df}) but not
   * aggregated in the segment header.
   */
  public long memtableTombstones() {
    return memtable.get().tombstoneCount();
  }

  public int segmentCount() {
    return segments.get().length;
  }

  public long[] segmentIds() {
    final PaginatedSegmentReader[] active = segments.get();
    final long[] out = new long[active.length];
    for (int i = 0; i < active.length; i++)
      out[i] = active[i].segmentId();
    Arrays.sort(out);
    return out;
  }

  /**
   * Test-only accessor for the engine's mutator lock. Used by the backpressure regression test
   * to exercise the soft-block path: the test takes the lock from a worker thread to simulate an
   * in-flight flush, then verifies that a put past the threshold blocks until release. Reflection
   * was the previous workaround; a typed package-private accessor keeps the field name from
   * leaking into test code that would otherwise silently break on a rename. The lock is fully
   * encapsulated for production: nothing on the public API exposes it.
   */
  ReentrantLock mutatorLockForTest() {
    return mutatorLock;
  }

  // --- lifecycle ------------------------------------------------------------

  @Override
  public void close() {
    if (closed)
      return;
    mutatorLock.lock();
    try {
      if (closed)
        return;
      // Final flush so writes since the last flush are durable. Routes through the same
      // {@code runWithCompactionReplication} hook the regular {@link #flush} uses, so a
      // close-time memtable on the leader gets replicated to followers via the standard
      // SCHEMA_ENTRY pipeline (HA replication smoke test would otherwise show the leader
      // permanently ahead of followers if a database was closed without an explicit prior
      // flush). On standalone the override is a no-op wrapper, so the inner transaction is
      // the durability point as before.
      // <p>
      // Wrapped in a top-level try/catch: by the time {@code close()} runs, the database may
      // already have torn down enough of the transaction pipeline that a fresh
      // {@code database.transaction(...)} would throw. We swallow that into a {@code SEVERE}
      // log instead of letting it abort the close - the data in the unflushed memtable is
      // already lost from the engine's perspective, and an exception here would leave other
      // components' close() unrun.
      final Memtable old = memtable.getAndSet(new Memtable());
      if (!old.isEmpty()) {
        try {
          final long segmentId = nextSegmentId.getAndIncrement();
          // close() does not need to publish a reader (the engine is being torn down) - we just
          // want the memtable to be sealed durably. {@link #buildSegmentComponent} registers the
          // component with the FileManager and drains the page cache; on a subsequent reopen,
          // {@link #loadExistingSegments} will pick the file up from disk. The return value
          // (the registered component) is intentionally unused here for that reason.
          database.getWrappedDatabaseInstance().runWithCompactionReplication(() -> {
            buildSegmentComponent(segmentId, old);
            return Boolean.TRUE;
          });
        } catch (final InterruptedException e) {
          // Restore the interrupt flag for the close-time path too. We still swallow the throw
          // into a SEVERE because close() must keep going for the rest of the shutdown sequence;
          // the interrupt status is what lets the caller's later blocking calls notice.
          Thread.currentThread().interrupt();
          LogManager.instance().log(this, Level.SEVERE,
              "Close-time flush of sparse vector engine '%s' interrupted; %d memtable postings discarded: %s",
              null, indexName, old.totalPostings(), e);
        } catch (final IOException | RuntimeException e) {
          // Database may be mid-teardown; tolerate it but make the loss loud.
          LogManager.instance().log(this, Level.SEVERE,
              "Close-time flush of sparse vector engine '%s' failed; %d memtable postings discarded: %s",
              null, indexName, old.totalPostings(), e);
        }
      }
      // Component lifetime is owned by FileManager; nothing else to release here.
      segments.set(new PaginatedSegmentReader[0]);
      closed = true;
    } finally {
      mutatorLock.unlock();
    }
  }

  /**
   * Drop every sealed segment component owned by this engine and clear the in-memory state.
   * Called from {@link LSMSparseVectorIndex#drop()} so dropping the index also reclaims the
   * {@code .sparseseg} files; without this the FileManager would keep the components and the
   * files would leak on disk once the wrapping LSM-Tree shell is dropped.
   * <p>
   * Discards the memtable too: a drop is a permanent destruction, the postings have nowhere
   * useful to land. After this call the engine is closed.
   */
  public void dropAll() {
    if (closed)
      return;
    mutatorLock.lock();
    try {
      if (closed)
        return;
      // Throw away unsealed memtable state - a drop voids any pending writes.
      memtable.set(new Memtable());
      // Drop every active segment via the FileManager so the on-disk file is reclaimed and the
      // schema's files list no longer references the component.
      for (final PaginatedSegmentReader r : segments.get()) {
        try {
          dropComponent(r.component());
        } catch (final RuntimeException ignored) {
          // best-effort: swallow per-segment drop failures so a single bad file doesn't strand
          // the rest of the cleanup; subsequent reopen will skip them via header validation.
        }
      }
      // Pick up any orphan components missed because they were registered but never made it into
      // {@code segments} (e.g. a partial flush that crashed before swap). Walk the FileManager
      // for sparseseg files matching this index's strict {@code <name>_seg<digits>} pattern
      // and drop those too. The strict pattern matters here: we don't want to delete files
      // belonging to a sibling index whose name happens to be a prefix of ours.
      for (final var componentFile : database.getFileManager().getFiles()) {
        if (!isOurSegmentFile(componentFile))
          continue;
        final var component = database.getSchema().getFileByIdIfExists(componentFile.getFileId());
        if (component instanceof SparseSegmentComponent ssc) {
          try {
            dropComponent(ssc);
          } catch (final RuntimeException ignored) {
            // best-effort: see comment above
          }
        }
      }
      segments.set(new PaginatedSegmentReader[0]);
      closed = true;
    } finally {
      mutatorLock.unlock();
    }
  }

  // --- internals ------------------------------------------------------------

  private void ensureOpen() {
    if (closed)
      throw new IllegalStateException("engine is closed");
  }

  /** Component name pattern: {@code <indexName>_seg<segmentId>}. */
  private String segmentComponentName(final long segmentId) {
    return indexName + "_seg" + segmentId;
  }

  /**
   * Allocate a fresh {@link SparseSegmentComponent} for the given segment id, register it with
   * the schema's file manager, and return it. Caller is responsible for the surrounding
   * transaction (the component's pages must be allocated inside one).
   */
  private SparseSegmentComponent createComponent(final long segmentId) {
    final String name = segmentComponentName(segmentId);
    final String filePath = database.getDatabasePath() + "/" + name;
    try {
      final SparseSegmentComponent c = new SparseSegmentComponent(database, name, filePath, ComponentFile.MODE.READ_WRITE,
          params.pageSize());
      ((LocalSchema) database.getSchema().getEmbedded()).registerFile(c);
      return c;
    } catch (final IOException e) {
      throw new IndexException("Failed to allocate sparse segment component '" + name + "'", e);
    }
  }

  private void dropComponent(final SparseSegmentComponent component) {
    // Null-safe: callers in error-cleanup paths (flush() / compactInputs()) capture the freshly
    // created component into a one-element {@code componentRef[0]} array and then drop in
    // catch/finally blocks. If {@link #createComponent} itself threw before the assignment ran,
    // {@code componentRef[0]} stays {@code null} - skipping silently here is what the cleanup
    // path expects, and it lets the cleanup site avoid a defensive null check at every site.
    if (component == null)
      return;
    try {
      database.getFileManager().dropFile(component.getFileId());
    } catch (final IOException e) {
      throw new IndexException("Failed to drop sparse segment component '" + component.getName() + "'", e);
    }
  }

  /**
   * Builds a sealed segment from {@code old} and persists it as a new {@link SparseSegmentComponent}.
   * Shared by {@link #flush} and {@link #close}: both run the same recipe (allocate the component,
   * write all dims through {@link SparseSegmentBuilder}, drain the page cache so the synthetic
   * WAL HA serializes after this step sees on-disk pages instead of zeros from the async write
   * cache, drop the component on builder failure to avoid orphan files). Caller is responsible
   * for executing this inside {@code runWithCompactionReplication} and for any post-build
   * publication step ({@code appendSegment} on flush, no-op on close).
   * <p>
   * Returns the registered component so the caller can open a {@link PaginatedSegmentReader}
   * over it under the same recording session.
   */
  /**
   * RAM budget (in bytes) for segment pages buffered before a batched disk flush during a
   * build/compaction. Reuses {@code INDEX_COMPACTION_RAM_MB} - the same knob the LSM compactor
   * uses - so both compactors share one operator-facing tuning point, and caps it at 30% of max
   * heap (also matching the LSM compactor) so an over-large configured value cannot itself OOM.
   */
  private long segmentBuildChunkBytes() {
    final long configured = database.getConfiguration().getValueAsLong(GlobalConfiguration.INDEX_COMPACTION_RAM_MB) * 1024L * 1024L;
    final long maxUsable = Runtime.getRuntime().maxMemory() * 30 / 100;
    return Math.max(params.pageSize(), Math.min(configured, maxUsable));
  }

  private SparseSegmentComponent buildSegmentComponent(final long segmentId, final Memtable old) {
    final SparseSegmentComponent component = createComponent(segmentId);
    try {
      // No enclosing database.transaction(): the builder streams pages straight to disk in
      // RAM-bounded chunks (issue #5189), so a segment larger than the WALFile 2 GB per-transaction
      // ceiling - or larger than heap - builds fine. HA still replicates it via
      // serializeFilePagesAsWal reading the finished on-disk file.
      try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, params, segmentBuildChunkBytes())) {
        b.setSegmentId(segmentId);
        for (final int dim : old.sortedDims()) {
          final Iterator<MemtablePosting> it = old.iterateDim(dim);
          if (!it.hasNext())
            continue;
          b.startDim(dim);
          while (it.hasNext()) {
            final MemtablePosting p = it.next();
            if (p.tombstone())
              b.appendTombstone(p.rid());
            else
              b.appendPosting(p.rid(), p.weight());
          }
          b.endDim();
        }
        b.finish();
      }
    } catch (final RuntimeException buildFailure) {
      // The build aborted (e.g. dim_index page overflow when a single segment has more unique
      // dims than fit in one page). createComponent already registered the segment file with the
      // FileManager, so leaving it would expose an empty file to the next
      // refreshSegmentsFromFileManager scan and crash queries. Drop it before propagating.
      try {
        dropComponent(component);
      } catch (final RuntimeException dropFailure) {
        buildFailure.addSuppressed(dropFailure);
      }
      throw buildFailure;
    }
    // Drain the page cache so the synthetic WAL HA's runWithCompactionReplication ships in this
    // recording session sees on-disk pages instead of zeros from the async writer. If the bounded wait
    // gives up (#4928), shipping would send zeros to followers: abort, compaction retries later.
    if (!database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database))
      throw new IndexException("Sparse-vector compaction aborted: pages are still pending flush after the no-progress timeout");
    return component;
  }

  /**
   * Lightweight resync of the in-memory segments snapshot against the FileManager. On a Raft
   * leader the engine's {@code segments} array is populated by {@link #appendSegment} after each
   * flush; on a follower, {@code SparseSegmentComponent} files arrive via {@code SCHEMA_ENTRY}
   * replication and are registered in the FileManager + {@link com.arcadedb.schema.LocalSchema}'s
   * {@code files} list, but no code path updates this engine's snapshot. Calling this at the
   * start of each query keeps follower visibility correct without requiring a separate
   * notification path.
   * <p>
   * <b>Concurrency.</b> The whole reconcile-and-publish runs under {@link #mutatorLock} (the
   * same lock {@link #flush} and {@link #compactInputs} take to swap segment arrays). Without
   * the lock, a TOCTOU window opened between the initial {@code segments.get()} snapshot and
   * the final {@code segments.set(...)}: a concurrent flush or compaction that committed in
   * that window would be silently overwritten by this method's stale view, dropping the
   * just-published segment from the in-memory array until the next refresh re-discovered it
   * from disk. Holding the lock serializes refreshes against mutating operations and is cheap
   * in practice - the steady-state common case has no new files to open and the lock is held
   * for microseconds. The expensive {@link PaginatedSegmentReader} construction (page-0 read
   * for newly-discovered components) only happens when a follower actually receives a new
   * segment via replication, which is rare relative to query rate.
   */
  private void refreshSegmentsFromFileManager() {
    // Fastest path: the FileManager has a global modification counter that bumps on every
    // registerFile / dropFile. If it has not advanced since our last successful refresh, no file
    // - of any kind, for any index - has been added or removed, so our sparse-segment subset is
    // necessarily current and we can skip the per-file walk entirely. This is the common case on
    // a steady-state querying database; it turns refresh into one volatile read.
    final long observedMods = database.getFileManager().getModificationCount();
    if (observedMods == lastObservedFileManagerMods.get())
      return;
    // Slower fallback: compute a content fingerprint of the FileManager's view of THIS index's
    // sparse-segment files (count + sum of file IDs). The walk is O(total files) but does only
    // cheap operations (string compare on file extension and component-name prefix, int add) -
    // no schema lookups, no reader allocations. When the fingerprint matches the last successful
    // refresh, our snapshot is current and we can return without taking the lock. The fingerprint
    // catches the HA-compaction case where a SCHEMA_ENTRY adds a merged segment and retires its
    // inputs in one step (file count unchanged, but the sum of file IDs changes) - the previous
    // count-only fast path would have stalled visibility of the new segment until the next
    // change-of-count event.
    final long observedFingerprint = computeFileFingerprint();
    if (observedFingerprint == lastObservedFileFingerprint.get()) {
      // FileManager moved but its files do not concern us - cache the mod count so we do not
      // re-walk for unrelated FileManager activity until the next mutation.
      lastObservedFileManagerMods.set(observedMods);
      return;
    }

    mutatorLock.lock();
    try {
      // Re-check under lock to skip the reconcile when a concurrent refresh on another thread
      // computed the SAME fingerprint and finished writing it back before we acquired the lock.
      // This catches the common "two queries race to refresh after the same flush" pattern.
      // <p>
      // It does <i>not</i> catch the case where a concurrent flush() committed a NEW fingerprint
      // while we were waiting for the lock - {@code observedFingerprint} would be the old (stale)
      // value, not the new high-water mark, so this comparison is false and we fall through to a
      // (technically redundant) reconcile. The {@code knownIds} guard in the reconcile loop makes
      // that wasted-work path safe (no duplicate readers), so the worst case is a re-walk, not a
      // correctness bug.
      if (observedFingerprint == lastObservedFileFingerprint.get())
        return;

      final PaginatedSegmentReader[] current = segments.get();
      final LongHashSet knownIds = new LongHashSet(Math.max(8, current.length * 2));
      for (final PaginatedSegmentReader r : current)
        knownIds.add(r.segmentId());

      boolean changed = false;
      final List<PaginatedSegmentReader> updated = new ArrayList<>(current.length);
      for (final PaginatedSegmentReader r : current)
        updated.add(r);

      for (final var componentFile : database.getFileManager().getFiles()) {
        if (!isOurSegmentFile(componentFile))
          continue;
        final var component = database.getSchema().getFileByIdIfExists(componentFile.getFileId());
        if (!(component instanceof SparseSegmentComponent ssc))
          continue;
        // On followers the sparseseg file briefly exists between createNewFiles and the WAL apply
        // that fills its pages, so a freshly-arrived component can fail header validation for a
        // moment. Skip it; the next query will pick it up once pages are written. Log at FINE so
        // an operator troubleshooting "follower is missing data" can see the skip when they raise
        // log levels - the steady-state path silently produces no log lines.
        final PaginatedSegmentReader reader;
        try {
          reader = new PaginatedSegmentReader(ssc);
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.FINE,
              "Skipping unreadable sparse segment '%s' (file id %d) during refresh; will retry on the next query: %s",
              ssc.getName(), componentFile.getFileId(), e.getMessage());
          continue;
        }
        if (knownIds.contains(reader.segmentId()))
          continue;
        updated.add(reader);
        changed = true;
      }

      // Drop any segments that the FileManager no longer knows about (a follower may apply a
      // SCHEMA_ENTRY that retires segments via removeFiles).
      for (int i = updated.size() - 1; i >= 0; i--) {
        if (!fileManagerHasComponent(updated.get(i).component())) {
          updated.remove(i);
          changed = true;
        }
      }

      if (changed) {
        updated.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
        segments.set(updated.toArray(new PaginatedSegmentReader[0]));
        if (!updated.isEmpty()) {
          final long highest = updated.getLast().segmentId();
          if (nextSegmentId.get() <= highest)
            nextSegmentId.set(highest + 1L);
        }
      }
      // Re-compute the fingerprint under the lock and commit it as the new high-water mark; if a
      // concurrent flush() racing this refresh added a file mid-reconcile we capture that here.
      // The mod-count high-water mark is captured AFTER the fingerprint so a concurrent
      // registerFile that bumps the counter but lands a file matching our prefix is still picked
      // up by the next refresh (the fingerprint will differ).
      lastObservedFileFingerprint.set(computeFileFingerprint());
      lastObservedFileManagerMods.set(database.getFileManager().getModificationCount());
    } finally {
      mutatorLock.unlock();
    }
  }

  private boolean fileManagerHasComponent(final SparseSegmentComponent ssc) {
    try {
      return database.getFileManager().existsFile(ssc.getFileId());
    } catch (final RuntimeException ignored) {
      return false;
    }
  }

  /**
   * Walk the FileManager and accumulate a 64-bit content fingerprint over THIS index's segment
   * files. Each file id is run through a splitmix64 mixer and the mixed values are summed, then
   * the count is mixed in. This is order-invariant (multiset semantics on the file id set), is
   * not subject to XOR cancellation under balanced add/remove (which is why we replaced an
   * earlier XOR-of-ids), and does not silently overflow the way a plain {@code sum} could in a
   * pathological scenario where the running sum brushes {@code Long.MAX_VALUE}. Cheap per-file
   * operations only: extension compare and component-name prefix-and-digits match, no schema
   * lookups, no reader opens.
   */
  private long computeFileFingerprint() {
    // getFiles() returns a thread-safe snapshot of the FileManager's file list (since #4371),
    // so iteration here is safe even though this method is called without holding mutatorLock.
    // Files registered after the snapshot is taken are invisible to this pass; the next bump of
    // FileManager#getModificationCount will trigger a re-walk that picks them up.
    final var files = database.getFileManager().getFiles();
    final int size = files.size();
    long count = 0L;
    long mixedSum = 0L;
    for (int i = 0; i < size; i++) {
      final var componentFile = files.get(i);
      if (!isOurSegmentFile(componentFile))
        continue;
      count++;
      mixedSum += splitmix64(componentFile.getFileId());
    }
    return splitmix64(count) ^ mixedSum;
  }

  /** splitmix64 finalizer; see Steele/Lea/Flood (2014). Used as a 64-bit avalanche mixer. */
  private static long splitmix64(long x) {
    x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
    x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
    return x ^ (x >>> 31);
  }

  /**
   * True iff {@code componentFile} is one of THIS index's segment files. Anchors the match on the
   * canonical name pattern {@code <indexName>_seg<digits>} so an unrelated index whose name is a
   * prefix of ours (e.g. {@code "myIndex"} vs {@code "myIndexV2"}) cannot accidentally land in
   * our segment set: simply checking {@code startsWith(indexName + "_seg")} would let
   * {@code myIndexV2_seg42} match a {@code myIndex} engine because the suffix is non-empty.
   */
  private boolean isOurSegmentFile(final ComponentFile componentFile) {
    if (componentFile == null)
      return false;
    if (!SparseSegmentComponent.FILE_EXT.equals(componentFile.getFileExtension()))
      return false;
    final String name = componentFile.getComponentName();
    if (name == null)
      return false;
    final String prefix = indexName + "_seg";
    if (!name.startsWith(prefix))
      return false;
    if (name.length() == prefix.length())
      return false;
    for (int i = prefix.length(); i < name.length(); i++) {
      final char c = name.charAt(i);
      if (c < '0' || c > '9')
        return false;
    }
    return true;
  }

  /**
   * Discover existing components belonging to this index by name pattern, sort them by segment
   * id, and prime {@link #nextSegmentId} above the highest known id.
   */
  private void loadExistingSegments() {
    final List<PaginatedSegmentReader> readers = new ArrayList<>();
    // Highest id seen across BOTH readable segments and unreadable orphan files, used to prime
    // nextSegmentId so a fresh build never reuses an id whose file still exists on disk.
    long highestSegmentFileId = 0L;

    // Walk every registered file by id; sparse segment components whose name strictly matches
    // {@code <indexName>_seg<digits>} belong to this engine.
    for (final var componentFile : database.getFileManager().getFiles()) {
      if (!isOurSegmentFile(componentFile))
        continue;
      final long parsedId = parseSegmentId(componentFile.getComponentName());
      if (parsedId > highestSegmentFileId)
        highestSegmentFileId = parsedId;
      final var component = database.getSchema().getFileByIdIfExists(componentFile.getFileId());
      if (component instanceof SparseSegmentComponent ssc) {
        try {
          readers.add(new PaginatedSegmentReader(ssc));
        } catch (final IOException e) {
          // A partial/unreadable segment file is a never-published orphan from a hard crash
          // mid-build: the build streams pages to disk outside any transaction (issue #5189), so a
          // JVM kill can leave a file whose page-0 header was never back-patched. It was never part
          // of a queried segment set, so skipping it loses no committed data - and its id is kept
          // out of the reusable range (above) so a fresh build cannot collide with its file name.
          LogManager.instance().log(this, Level.WARNING,
              "Skipping unreadable sparse segment '%s' (likely a crash-interrupted build); it will be ignored: %s",
              null, ssc.getName(), e.getMessage());
        }
      }
    }
    readers.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
    if (!readers.isEmpty())
      segments.set(readers.toArray(new PaginatedSegmentReader[0]));
    final long highestReadable = readers.isEmpty() ? 0L : readers.getLast().segmentId();
    final long floor = Math.max(highestReadable, highestSegmentFileId);
    if (floor > 0L)
      nextSegmentId.set(floor + 1L);
  }

  /**
   * Parse the numeric segment id out of a {@code <indexName>_seg<digits>} component name.
   * {@link #isOurSegmentFile} has already validated the pattern, so the suffix is all digits;
   * returns {@code -1} defensively if it somehow isn't (never expected from a real segment file).
   */
  private long parseSegmentId(final String componentName) {
    if (componentName == null)
      return -1L;
    final int suffixStart = indexName.length() + "_seg".length();
    if (suffixStart >= componentName.length())
      return -1L;
    try {
      return Long.parseLong(componentName.substring(suffixStart));
    } catch (final NumberFormatException e) {
      return -1L;
    }
  }

  // Both writers below run under {@link #mutatorLock}; the only concurrent reader is {@link #topK}
  // which takes a lock-free snapshot via {@code segments.get()}. A plain {@code segments.set(...)}
  // is therefore enough - the AtomicReference still provides the safe-publication barrier we need
  // for readers without the misleading-CAS-loop suggestion of contention between writers.
  private void appendSegment(final PaginatedSegmentReader newSeg) {
    final PaginatedSegmentReader[] curr = segments.get();
    final PaginatedSegmentReader[] next = Arrays.copyOf(curr, curr.length + 1);
    next[curr.length] = newSeg;
    segments.set(next);
  }

  private void replaceSegments(final PaginatedSegmentReader[] toRemove, final PaginatedSegmentReader maybeNew) {
    final LongHashSet removeIds = new LongHashSet(Math.max(8, toRemove.length * 2));
    for (final PaginatedSegmentReader r : toRemove)
      removeIds.add(r.segmentId());

    final PaginatedSegmentReader[] curr = segments.get();
    final List<PaginatedSegmentReader> next = new ArrayList<>(curr.length);
    for (final PaginatedSegmentReader r : curr) {
      if (!removeIds.contains(r.segmentId()))
        next.add(r);
    }
    if (maybeNew != null)
      next.add(maybeNew);
    segments.set(next.toArray(new PaginatedSegmentReader[0]));

    // Drop the underlying component files (and FileManager refs) for retired segments.
    for (final PaginatedSegmentReader r : toRemove)
      dropComponent(r.component());
  }

  /**
   * Build a merged {@link DimCursor} from the memtable and segment snapshot for one dim.
   * <p>
   * Sources are added unstarted; {@link DimCursor#start} is responsible for starting every
   * source and marking exhausted ones as not-live. This keeps the lifecycle contract uniform
   * across source types - a previous version eagerly started the memtable cursor here so it
   * could check {@code isExhausted} before adding to the list, while segment cursors were left
   * for DimCursor to start, which had the same observable behaviour but was easy to misread as
   * a hidden ordering requirement.
   */
  private DimCursor openMergedCursor(final int dim, final Memtable mt, final PaginatedSegmentReader[] segSnapshot)
      throws IOException {
    final List<SourceCursor> sources = new ArrayList<>(segSnapshot.length + 1);
    for (final PaginatedSegmentReader r : segSnapshot) {
      final PaginatedSegmentDimCursor c = r.openCursor(dim);
      if (c != null)
        sources.add(c);
    }
    // Skip the memtable source entirely when the memtable has no entry (live or tombstone)
    // for this dim. {@link MemtableSourceCursor#start} would handle the empty case by marking
    // itself exhausted, so correctness is fine either way - but a non-contributing source still
    // costs one slot in {@link DimCursor#materializeMin}'s per-advance scan, and dims that are
    // not in the memtable are the common case (queries typically touch ~10 dims while the
    // memtable holds postings for thousands).
    if (mt != null && mt.containsDim(dim))
      sources.add(new MemtableSourceCursor(mt, dim));
    if (sources.isEmpty())
      return null;
    return new DimCursor(dim, sources);
  }

  /**
   * N-way merge across {@code inputs} (oldest-first), emitting per-dim postings into {@code b}.
   * Returns {@code true} if at least one posting was emitted.
   */
  private boolean mergeIntoBuilder(final SparseSegmentBuilder b, final PaginatedSegmentReader[] inputs,
      final boolean dropAllTombstones) throws IOException {
    final IntHashSet allDimsSet = new IntHashSet();
    for (final PaginatedSegmentReader r : inputs)
      for (final int d : r.dims())
        allDimsSet.add(d);
    final int[] allDims = allDimsSet.toArray();
    Arrays.sort(allDims);

    boolean wroteAnything = false;
    for (final int dim : allDims) {
      final List<DimSource> sources = new ArrayList<>(inputs.length);
      try {
        for (int i = 0; i < inputs.length; i++) {
          final PaginatedSegmentDimCursor c = inputs[i].openCursor(dim);
          if (c == null)
            continue;
          c.start();
          if (c.isExhausted()) {
            c.close();
            continue;
          }
          sources.add(new DimSource(c, i));
        }
        if (sources.isEmpty())
          continue;

        boolean dimOpened = false;
        // O(n * k) merge: each step scans the live source list twice (min-RID then newest-aligned).
        // For STCS-bounded {@code tierFanout} (default 4) the constant is negligible. A
        // {@code compactAll()} on a heavily fragmented index, or any future raise of
        // {@code tierFanout} past ~16, would be better served by a min-heap keyed by current RID
        // (O(log n) per step) - tracked as a future optimization, not done here because the STCS
        // ceiling caps the practical input width and the linear loop is cache-friendly enough at
        // those sizes that the heap's pointer-chasing tends to lose on small {@code n}.
        while (!sources.isEmpty()) {
          // Find the smallest currentRid across live sources.
          RID minRid = sources.get(0).cursor.currentRid();
          for (int i = 1; i < sources.size(); i++) {
            final RID r = sources.get(i).cursor.currentRid();
            if (SparseSegmentBuilder.compareRid(r, minRid) < 0)
              minRid = r;
          }

          // Pick the newest source aligned at minRid (newest = highest priority index).
          DimSource newest = null;
          for (final DimSource s : sources) {
            if (minRid.equals(s.cursor.currentRid())) {
              if (newest == null || s.priority > newest.priority)
                newest = s;
            }
          }

          final boolean tombstone = newest.cursor.isTombstone();
          if (tombstone && dropAllTombstones) {
            // skip
          } else {
            if (!dimOpened) {
              b.startDim(dim);
              dimOpened = true;
            }
            if (tombstone)
              b.appendTombstone(minRid);
            else
              b.appendPosting(minRid, newest.cursor.currentWeight());
          }

          // Advance every cursor aligned at minRid; drop those that exhaust. {@code it.remove()}
          // pulls the closed cursor out of {@code sources}, which is what makes the {@code finally}
          // block double-close-safe: only cursors still in the list reach it. (Even without
          // {@code it.remove()}, {@link PaginatedSegmentDimCursor#close} is idempotent - it only
          // sets {@code exhausted = true} and {@code currentRid = null} - so a redundant call is
          // harmless. The list mutation is for correctness during the next outer-loop iteration,
          // not for close safety.)
          for (final Iterator<DimSource> it = sources.iterator(); it.hasNext(); ) {
            final DimSource s = it.next();
            if (minRid.equals(s.cursor.currentRid())) {
              if (!s.cursor.advance()) {
                s.cursor.close();
                it.remove();
              }
            }
          }
        }

        if (dimOpened) {
          b.endDim();
          wroteAnything = true;
        }
      } finally {
        for (final DimSource s : sources)
          s.cursor.close();
      }
    }
    return wroteAnything;
  }

  private static final class DimSource {
    final PaginatedSegmentDimCursor cursor;
    final int                       priority; // higher = newer

    DimSource(final PaginatedSegmentDimCursor cursor, final int priority) {
      this.cursor = cursor;
      this.priority = priority;
    }
  }
}
