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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
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
 * ({@link BmwScorer}) and the brute-force scorer ({@link BruteForceScorer}) treat any tombstone
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

  private final DatabaseInternal  database;
  private final String            indexName;
  private final SegmentParameters params;
  private final long              memtableFlushThreshold;
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
    this.tierFanout = tierFanout;
    this.tierBasePostings = tierBasePostings;
    loadExistingSegments();
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
   * ({@link BmwScorer}) and the brute-force scorer ({@link BruteForceScorer}) both treat any
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
    memtable.get().put(dim, rid, weight);
  }

  /**
   * Tombstones the posting for {@code (dim, rid)}. See the tombstone-semantics note on
   * {@link #put(int, RID, float)}: this is a whole-document delete signal in the scorer's view,
   * not a partial-dim update.
   */
  public void remove(final int dim, final RID rid) {
    ensureOpen();
    memtable.get().remove(dim, rid);
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

    final DimCursor[] cursors = new DimCursor[queryDims.length];
    try {
      for (int i = 0; i < queryDims.length; i++)
        cursors[i] = openMergedCursor(queryDims[i], mtSnapshot, segSnapshot);
      return BmwScorer.topK(queryDims, queryWeights, cursors, k);
    } finally {
      for (final DimCursor c : cursors)
        if (c != null)
          c.close();
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
   * The build runs inside {@link DatabaseInternal#runWithCompactionReplication}: the file
   * registration and page allocations are captured by the file-manager recording session so that
   * a {@code SCHEMA_ENTRY} carrying the new component metadata + a synthetic WAL of its pages is
   * shipped to followers atomically with the leader's local commit. On a standalone (non-HA)
   * database the override is a no-op wrapper and the inner transaction is the durability point.
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
            throw new IndexException("Failed to open freshly-flushed sparse segment '" + indexName + "_seg" + segmentId + "'",
                e);
          }
          return Boolean.TRUE;
        });
      } catch (final IOException | InterruptedException e) {
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
      final Map<Integer, List<PaginatedSegmentReader>> byTier = new HashMap<>();
      for (final PaginatedSegmentReader r : active) {
        final int t = tierOf(r.totalPostings());
        byTier.computeIfAbsent(t, k -> new ArrayList<>()).add(r);
      }
      // Pick the lowest-tier overflow first so write amplification stays minimal: small inputs
      // means cheap merge.
      final int[] sortedTiers = byTier.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
      for (final int t : sortedTiers) {
        final List<PaginatedSegmentReader> sameTier = byTier.get(t);
        if (sameTier.size() < tierFanout)
          continue;
        sameTier.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
        return sameTier.subList(0, tierFanout).toArray(new PaginatedSegmentReader[0]);
      }
      return null;
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
            database.transaction(() -> {
              try (final SparseSegmentBuilder b = new SparseSegmentBuilder(componentRef[0], params)) {
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
            });
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
          // Drain the page cache's async writer so the synthetic WAL HA ships in this same
          // recording session sees the final on-disk pages instead of zeros.
          database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
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
            replaceSegments(inputs, /* maybeNew */ null);
          } else {
            final PaginatedSegmentReader newReader;
            try {
              newReader = new PaginatedSegmentReader(componentRef[0]);
            } catch (final IOException e) {
              // Defensive: a successful build + drain should never produce a reader that fails
              // header validation, but if it does, drop the orphan and surface the real cause.
              try {
                dropComponent(componentRef[0]);
              } catch (final RuntimeException ignored) {
                // best-effort cleanup
              }
              throw new IndexException(
                  "Failed to open freshly-compacted sparse segment '" + indexName + "_seg" + newId + "'", e);
            }
            replaceSegments(inputs, newReader);
          }
          return Boolean.TRUE;
        });
      } catch (final IOException | InterruptedException e) {
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

  public long totalPostings() {
    long total = memtable.get().totalPostings();
    for (final PaginatedSegmentReader r : segments.get())
      total += r.totalPostings();
    return total;
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
        } catch (final IOException | InterruptedException | RuntimeException e) {
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
  private SparseSegmentComponent buildSegmentComponent(final long segmentId, final Memtable old) {
    final SparseSegmentComponent component = createComponent(segmentId);
    try {
      database.transaction(() -> {
        try (final SparseSegmentBuilder b = new SparseSegmentBuilder(component, params)) {
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
      });
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
    // recording session sees on-disk pages instead of zeros from the async writer.
    database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
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
    // Fast path: compute a content fingerprint of the FileManager's view of THIS index's
    // sparse-segment files (count + sum of file IDs). The walk is O(total files) but does only
    // cheap operations (string compare on file extension and component-name prefix, int add) -
    // no schema lookups, no reader allocations. When the fingerprint matches the last successful
    // refresh, our snapshot is current and we can return without taking the lock. The fingerprint
    // catches the HA-compaction case where a SCHEMA_ENTRY adds a merged segment and retires its
    // inputs in one step (file count unchanged, but the sum of file IDs changes) - the previous
    // count-only fast path would have stalled visibility of the new segment until the next
    // change-of-count event.
    final long observedFingerprint = computeFileFingerprint();
    if (observedFingerprint == lastObservedFileFingerprint.get())
      return;

    mutatorLock.lock();
    try {
      // Re-check under lock so we don't redo work a concurrent refresh just finished.
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
        // moment. Skip it; the next query will pick it up once pages are written.
        final PaginatedSegmentReader reader;
        try {
          reader = new PaginatedSegmentReader(ssc);
        } catch (final IOException e) {
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
      lastObservedFileFingerprint.set(computeFileFingerprint());
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
   * Walk the FileManager and accumulate a content fingerprint over THIS index's segment files.
   * Returns {@code (count << 32) + sumOfFileIds} - a count alone would miss balanced add+remove
   * SCHEMA_ENTRYs from HA compaction, and the sum-of-file-ids closes that gap. Plain integer
   * addition is used instead of XOR so a swap that adds {@code A+C} and removes {@code A} while
   * also adding {@code B} and removing {@code B+C} (i.e. shifts a constant {@code C} between
   * two file ids) shows up in the sum: XOR can mask that pattern when the affected bit lanes
   * align, addition cannot. Cheap per-file operations only: extension compare and
   * component-name prefix-and-digits match, no schema lookups, no reader opens.
   */
  private long computeFileFingerprint() {
    // Snapshot the FileManager's view before walking. {@link FileManager#getFiles} returns an
    // unmodifiableList wrapper over a mutable backing list, so iterating it directly while a
    // concurrent flush adds a file would throw ConcurrentModificationException - which we'd
    // catch but pay an exception-cost for. {@code toArray()} on an ArrayList is a single
    // {@code Arrays.copyOf} call so it's effectively atomic against single-element appends.
    final var snapshot = database.getFileManager().getFiles().toArray(new com.arcadedb.engine.ComponentFile[0]);
    long count = 0L;
    long sumIds = 0L;
    for (final var componentFile : snapshot) {
      if (!isOurSegmentFile(componentFile))
        continue;
      count++;
      sumIds += componentFile.getFileId();
    }
    return (count << 32) + sumIds;
  }

  /**
   * True iff {@code componentFile} is one of THIS index's segment files. Anchors the match on the
   * canonical name pattern {@code <indexName>_seg<digits>} so an unrelated index whose name is a
   * prefix of ours (e.g. {@code "myIndex"} vs {@code "myIndexV2"}) cannot accidentally land in
   * our segment set: simply checking {@code startsWith(indexName + "_seg")} would let
   * {@code myIndexV2_seg42} match a {@code myIndex} engine because the suffix is non-empty.
   */
  private boolean isOurSegmentFile(final com.arcadedb.engine.ComponentFile componentFile) {
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

    // Walk every registered file by id; sparse segment components whose name strictly matches
    // {@code <indexName>_seg<digits>} belong to this engine.
    for (final var componentFile : database.getFileManager().getFiles()) {
      if (!isOurSegmentFile(componentFile))
        continue;
      final var component = database.getSchema().getFileByIdIfExists(componentFile.getFileId());
      if (component instanceof SparseSegmentComponent ssc) {
        try {
          readers.add(new PaginatedSegmentReader(ssc));
        } catch (final IOException e) {
          throw new IndexException("Failed to open sparse segment component '" + ssc.getName() + "'", e);
        }
      }
    }
    readers.sort(Comparator.comparingLong(PaginatedSegmentReader::segmentId));
    if (!readers.isEmpty()) {
      segments.set(readers.toArray(new PaginatedSegmentReader[0]));
      nextSegmentId.set(readers.getLast().segmentId() + 1L);
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

          // Advance every cursor aligned at minRid; drop those that exhaust.
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
