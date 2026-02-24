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
package com.arcadedb.engine.timeseries;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.LocalSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Pairs a mutable TimeSeriesBucket with a sealed TimeSeriesSealedStore.
 * Implements crash-safe compaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesShard implements AutoCloseable {

  private final int                    shardIndex;
  private final DatabaseInternal       database;
  private final List<ColumnDefinition> columns;
  private final long                   compactionBucketIntervalMs;
  private final TimeSeriesBucket       mutableBucket;
  private final TimeSeriesSealedStore  sealedStore;
  // Read lock: held by scan/iterate (concurrent reads allowed).
  // Write lock: held by compact() to prevent queries from seeing data twice
  // during the window where sealed blocks are written but mutable not yet cleared.
  private final ReadWriteLock          compactionLock  = new ReentrantReadWriteLock();
  // Prevents concurrent compact() calls on this shard (e.g. maintenance scheduler + explicit call).
  // Both writeTempCompactionFile() and truncateToBlockCount() use the same .ts.sealed.tmp path;
  // concurrent execution would corrupt each other's temp files.
  private final Lock                   compactionMutex = new ReentrantLock();

  public TimeSeriesShard(final DatabaseInternal database, final String baseName, final int shardIndex,
                         final List<ColumnDefinition> columns) throws IOException {
    this(database, baseName, shardIndex, columns, 0);
  }

  public TimeSeriesShard(final DatabaseInternal database, final String baseName, final int shardIndex,
                         final List<ColumnDefinition> columns, final long compactionBucketIntervalMs) throws IOException {
    this.shardIndex = shardIndex;
    this.database = database;
    this.columns = columns;
    this.compactionBucketIntervalMs = compactionBucketIntervalMs;

    final String shardName = baseName + "_shard_" + shardIndex;
    final String shardPath = database.getDatabasePath() + "/" + shardName;
    final LocalSchema schema = (LocalSchema) database.getSchema();

    // Check if the bucket was already loaded by the component factory (cold open)
    final com.arcadedb.engine.Component existing = schema.getFileByName(shardName);
    if (existing instanceof TimeSeriesBucket tsb) {
      this.mutableBucket = tsb;
      this.mutableBucket.setColumns(columns);
    } else {
      // First-time creation: register the file with the schema BEFORE initialising the header
      // page, so that the nested-TX commit in initHeaderPage() can resolve the file by its ID.
      this.mutableBucket = new TimeSeriesBucket(database, shardName, shardPath, columns);
      schema.registerFile(mutableBucket);
      // Initialise the header page in a self-contained nested transaction.  Using a nested TX
      // here ensures that page 0 is committed immediately and is NOT placed in any enclosing
      // transaction's dirty set.  This is required so that the nested TX used by appendSamples()
      // can later commit page 0 without conflicting with an enclosing transaction that was open
      // when this shard was created (a common test pattern).
      database.begin();
      try {
        mutableBucket.initHeaderPage();
        database.commit();
      } catch (final Exception e) {
        if (database.isTransactionActive())
          database.rollback();
        throw e instanceof IOException ? (IOException) e :
            new IOException("Failed to initialise header for shard " + shardIndex, e);
      }
    }

    // If sealedStore construction fails, close mutableBucket to avoid a resource leak
    final TimeSeriesSealedStore tempSealedStore;
    try {
      tempSealedStore = new TimeSeriesSealedStore(shardPath, columns);
    } catch (final IOException e) {
      try { this.mutableBucket.close(); } catch (final Exception ignored) {}
      throw e;
    }
    this.sealedStore = tempSealedStore;

    // Crash recovery: if a compaction was interrupted, truncate any partial sealed blocks
    database.begin();
    try {
      if (mutableBucket.isCompactionInProgress()) {
        final long watermark = mutableBucket.getCompactionWatermark();
        sealedStore.truncateToBlockCount(watermark);
        mutableBucket.setCompactionInProgress(false);
        database.commit();
      } else {
        database.rollback();
      }
    } catch (final Exception e) {
      if (database.isTransactionActive())
        database.rollback();
      // Close both stores to avoid resource leaks before propagating the error
      try { this.sealedStore.close(); } catch (final Exception ignored) {}
      try { this.mutableBucket.close(); } catch (final Exception ignored) {}
      throw e instanceof IOException ? (IOException) e :
          new IOException("Crash recovery failed for shard " + shardIndex, e);
    }
  }

  /**
   * Appends samples to the mutable bucket.
   * <p>
   * The read lock is held for the <em>entire</em> internal transaction lifecycle
   * (begin → write → commit), not just during the page writes.  This is the key invariant
   * that prevents MVCC conflicts with Phase 4:
   * <ul>
   *   <li>Phase 4 acquires the <em>write</em> lock to clear the mutable bucket.</li>
   *   <li>The write lock can only be granted after all read-lock holders have released.</li>
   *   <li>Because this method releases the read lock only <em>after</em> the commit, Phase 4
   *       is guaranteed that every in-flight append has already persisted its page-0 modifications
   *       before Phase 4 starts its own transaction.  Phase 4 always sees the latest page-0
   *       version and commits without conflict; insert transactions are never affected.</li>
   * </ul>
   * <p>
   * This method always manages its own transaction.  If the caller already has an active
   * transaction, ArcadeDB creates a nested transaction (a new {@code TransactionContext} pushed
   * onto the per-thread stack).  The nested transaction commits independently; the caller's outer
   * transaction remains unaffected because it holds none of the modified pages in its dirty set.
   */
  public void appendSamples(final long[] timestamps, final Object[]... columnValues) throws IOException {
    compactionLock.readLock().lock();
    try {
      database.begin();
      try {
        mutableBucket.appendSamples(timestamps, columnValues);
        database.commit();
      } catch (final ConcurrentModificationException cme) {
        // Roll back the nested TX only.  Do NOT touch the caller's outer transaction:
        // the Javadoc promises it remains unaffected, and the caller must decide whether
        // to rollback/retry their own transaction.
        if (database.isTransactionActive())
          database.rollback();
        throw cme; // propagate as-is so callers can catch and retry
      } catch (final Exception e) {
        if (database.isTransactionActive())
          database.rollback();
        throw e instanceof IOException ? (IOException) e : new IOException("Failed to append timeseries samples", e);
      }
    } finally {
      compactionLock.readLock().unlock();
    }
  }

  /**
   * Scans both sealed and mutable layers, merging results by timestamp.
   * Holds the read lock to prevent concurrent {@link #compact()} from clearing the
   * mutable bucket after sealing, which would cause double-counting.
   */
  public List<Object[]> scanRange(final long fromTs, final long toTs, final int[] columnIndices,
                                  final TagFilter tagFilter) throws IOException {
    compactionLock.readLock().lock();
    try {
      final List<Object[]> results = new ArrayList<>();

      // Sealed layer first (already filtered by tagFilter inside sealedStore)
      final List<Object[]> sealedResults = sealedStore.scanRange(fromTs, toTs, columnIndices, tagFilter);
      results.addAll(sealedResults);

      // Then mutable layer
      final List<Object[]> mutableResults = mutableBucket.scanRange(fromTs, toTs, columnIndices);
      addFiltered(results, mutableResults, tagFilter, columnIndices);

      return results;
    } finally {
      compactionLock.readLock().unlock();
    }
  }

  /**
   * Returns an iterator over both sealed and mutable layers.
   * Sealed data is iterated first, then mutable. Tag filter is applied inline.
   * Both iterators are eagerly materialized under the read lock to prevent
   * concurrent {@link #compact()} from clearing the mutable bucket and causing
   * stale reads after the lock is released.
   */
  public Iterator<Object[]> iterateRange(final long fromTs, final long toTs, final int[] columnIndices,
                                         final TagFilter tagFilter) throws IOException {
    final Iterator<Object[]> sealedIter;
    final Iterator<Object[]> mutableIter;
    compactionLock.readLock().lock();
    try {
      sealedIter = sealedStore.iterateRange(fromTs, toTs, columnIndices, tagFilter);
      // Eagerly materialize the mutable iterator under the lock.
      // A lazy iterator would risk reading stale (cleared) pages if compaction
      // acquires the write lock and clears the bucket before next() is called.
      final List<Object[]> mutableRows = mutableBucket.scanRange(fromTs, toTs, columnIndices);
      mutableIter = mutableRows.iterator();
    } finally {
      compactionLock.readLock().unlock();
    }

    // Chain sealed then mutable, with inline tag filtering.
    // The sealed iterator is fully materialised; the mutable iterator is lazy but its
    // MVCC snapshot was already established under the read lock above.
    return new Iterator<>() {
      private Iterator<Object[]> current           = sealedIter;
      private boolean            switchedToMutable = false;
      private Object[]           nextRow           = null;

      {
        advance();
      }

      private void advance() {
        nextRow = null;
        while (true) {
          if (current.hasNext()) {
            final Object[] row = current.next();
            // Use matchesMapped() so the filter works correctly when columnIndices is a subset.
            if (tagFilter == null || tagFilter.matchesMapped(row, columnIndices)) {
              nextRow = row;
              return;
            }
          } else if (!switchedToMutable) {
            current = mutableIter;
            switchedToMutable = true;
          } else
            return;
        }
      }

      @Override
      public boolean hasNext() {
        return nextRow != null;
      }

      @Override
      public Object[] next() {
        if (nextRow == null)
          throw new NoSuchElementException();
        final Object[] result = nextRow;
        advance();
        return result;
      }
    };
  }

  /**
   * Maximum number of samples per sealed block. Keeps decompression cost bounded.
   */
  static final int SEALED_BLOCK_SIZE = 65_536;

  /**
   * Compacts mutable data into sealed columnar storage.
   * Data is written in chunks of {@link #SEALED_BLOCK_SIZE} rows to keep
   * individual sealed blocks small for fast decompression during queries.
   * <p>
   * <b>LSMTree-style lock-free pattern</b> — matches how {@code LSMTreeIndexCompactor} works:
   * <ol>
   *   <li><b>Phase 0</b> (brief write lock + brief TX): snapshot the current data page count
   *       N and persist the crash-recovery flag.  The write lock guarantees that no concurrent
   *       {@link #appendSamples} can modify page-0 during this TX, so the commit always
   *       succeeds.</li>
   *   <li><b>Phase 1</b> (no lock, read-only TX then rollback): read full/immutable pages
   *       1..N-1 via a short read-only transaction that is immediately rolled back.  These pages
   *       are permanently full — {@link #appendSamples} always writes to the LAST page — so
   *       their content is stable and no MVCC conflict is possible.</li>
   *   <li><b>Phase 2</b> (lock-free, no TX): sort and compress the snapshot data.</li>
   *   <li><b>Phase 3</b> (lock-free, no TX): write all sealed blocks to {@code .ts.sealed.tmp}.
   *       Concurrent queries still read from the live sealed file; no double-counting.</li>
   *   <li><b>Phase 4</b> (brief write lock + brief TX): read any remaining partial pages,
   *       merge with Phase-2 spill if present, compress, atomically swap the temp file,
   *       clear the compacted mutable bucket, and reset the crash-recovery flag.  The write
   *       lock blocks concurrent {@link #appendSamples} so the TX commit cannot conflict.</li>
   * </ol>
   * <p>
   * The result is that {@link #appendSamples} is never blocked during the heavy I/O phases
   * (1–3), and is only briefly blocked (≤ a few milliseconds) during the two write-lock
   * windows (phases 0 and 4).
   * <p>
   * Crash-safe: the {@code compactionInProgress} flag and watermark are committed in Phase 0.
   * If the process crashes before Phase 4 clears the flag, the {@link #TimeSeriesShard}
   * constructor truncates the sealed store back to the watermark and the mutable pages are
   * left intact for re-compaction.
   */
  public void compact() throws IOException {
    // Serialize concurrent compact() calls: writeTempCompactionFile() and truncateToBlockCount()
    // both use the same .ts.sealed.tmp path; concurrent execution would corrupt each other's
    // temp files (e.g. maintenance scheduler and an explicit compactAll() running in parallel).
    compactionMutex.lock();
    try {
      compactInternal();
    } finally {
      compactionMutex.unlock();
    }
  }

  private void compactInternal() throws IOException {
    final long initialBlockCount = sealedStore.getBlockCount();

    // ── Phase 0 (brief writeLock + brief TX): snapshot page count, set crash flag ─────────
    // The write lock blocks concurrent appendSamples() so the TX modifying page-0 cannot
    // get an MVCC conflict from a concurrent insert.
    final int snapshotDataPageCount;
    compactionLock.writeLock().lock();
    try {
      database.begin();
      try {
        final int pageCount = mutableBucket.getDataPageCount();
        if (pageCount == 0) {
          database.rollback();
          return;
        }
        snapshotDataPageCount = pageCount;
        mutableBucket.setCompactionInProgress(true);
        mutableBucket.setCompactionWatermark(initialBlockCount);
        database.commit();
      } catch (final Exception e) {
        if (database.isTransactionActive())
          database.rollback();
        throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed in phase 0", e);
      }
    } finally {
      compactionLock.writeLock().unlock();
    }

    // Pages 1..lastFullPage are FULL (immutable): appendSamples() never writes to full pages
    // — it always appends to the LAST page.  These are safe to read without the write lock.
    // Page snapshotDataPageCount is the current partial last page; it will be read in Phase 4
    // under the write lock together with any new pages created between Phase 0 and Phase 4.
    final int lastFullPage = snapshotDataPageCount - 1;

    // Shared output lists for compressed blocks built in Phases 1+2.
    final List<byte[][]> allCompressedList = new ArrayList<>();
    final List<long[]> allMetaList = new ArrayList<>();
    final List<double[]> allMinsList = new ArrayList<>();
    final List<double[]> allMaxsList = new ArrayList<>();
    final List<double[]> allSumsList = new ArrayList<>();
    final List<String[][]> allTagDVList = new ArrayList<>();

    // ── Phase 1 (no lock, read-only TX): read full/immutable pages ───────────────────────────
    // Full pages are never written to by concurrent appendSamples(); the read-only snapshot TX
    // is always conflict-free.  Roll it back immediately — nothing is modified.
    // phase2Spill holds the raw samples of the last partial chunk from Phase 2 (if any).
    // These samples are merged with Phase 4's partial-page data to avoid splitting a single
    // sealed block across the Phase-2/Phase-4 page boundary.
    Object[] phase2Spill = null;
    if (lastFullPage > 0) {
      database.begin();
      try {
        final Object[] snapshotData = mutableBucket.readFullPagesForCompaction(lastFullPage);
        if (snapshotData != null)
          // ── Phase 2 (lock-free, no TX): sort + compress immutable page data ─────────────
          // returnSpill=true: the last partial chunk is returned as raw data instead of being
          // emitted as a block, so Phase 4 can merge it with the partial-page samples and
          // produce a single correctly-sized block.
          phase2Spill = buildCompressedBlocks(snapshotData, allCompressedList, allMetaList, allMinsList, allMaxsList,
              allSumsList, allTagDVList, true);
      } finally {
        database.rollback(); // read-only: rollback is always safe
      }
    }

    // ── Phase 3 (lock-free, no TX): write existing sealed + Phase 2 blocks to temp file ─────
    // Concurrent queries still read from the CURRENT sealed file — no double-counting.
    // The temp file is created even when allCompressedList is empty so that Phase 4 can
    // append the partial page's data and then swap atomically in one shot.
    final List<TimeSeriesSealedStore.BlockEntry> newBlockDirectory;
    try {
      newBlockDirectory = sealedStore.writeTempCompactionFile(
          allCompressedList, allMetaList, allMinsList, allMaxsList, allSumsList, allTagDVList);
    } catch (final Exception e) {
      sealedStore.deleteTempFileIfExists();
      clearCompactionFlagBestEffort();
      throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed writing temp file", e);
    }

    // ── Phase 4a (brief writeLock + read-only TX): snapshot remaining pages ──────────────
    // Grab the write lock just long enough to snapshot the current page count and read the
    // pages accumulated since Phase 0.  Release the lock immediately so appends can resume
    // while we sort + compress (Phase 4b).
    final int phase4aPageCount;
    Object[] phase4aData;
    compactionLock.writeLock().lock();
    try {
      database.begin();
      try {
        phase4aPageCount = mutableBucket.getDataPageCount();
        if (phase4aPageCount > lastFullPage)
          phase4aData = mutableBucket.readPagesRangeForCompaction(lastFullPage + 1, phase4aPageCount);
        else
          phase4aData = null;
      } finally {
        database.rollback(); // read-only snapshot
      }
    } finally {
      compactionLock.writeLock().unlock();
    }

    // ── Phase 4b (lock-free, no TX): compress remaining pages + write to temp file ────────
    // Merge Phase-2 spill with the Phase-4a snapshot, then compress and append to the temp
    // file.  Concurrent appends proceed freely during this CPU/IO-intensive step.
    final Object[] toCompress4b;
    if (phase2Spill != null && phase4aData != null)
      toCompress4b = mergeCompactionData(phase2Spill, phase4aData);
    else if (phase2Spill != null)
      toCompress4b = phase2Spill;
    else
      toCompress4b = phase4aData;

    // Use returnSpill=true so the last partial chunk is held back for Phase 4c,
    // where it can be merged with any tail pages created during Phase 4b.
    Object[] phase4bSpill = null;
    if (toCompress4b != null) {
      final List<byte[][]> remCompressed = new ArrayList<>();
      final List<long[]> remMeta = new ArrayList<>();
      final List<double[]> remMins = new ArrayList<>();
      final List<double[]> remMaxs = new ArrayList<>();
      final List<double[]> remSums = new ArrayList<>();
      final List<String[][]> remTagDV = new ArrayList<>();
      phase4bSpill = buildCompressedBlocks(toCompress4b, remCompressed, remMeta, remMins, remMaxs, remSums,
          remTagDV, true);
      if (!remCompressed.isEmpty())
        sealedStore.appendBlocksToTempFile(remCompressed, remMeta, remMins, remMaxs, remSums, remTagDV,
            newBlockDirectory);
    }

    // ── Phase 4c (brief writeLock + brief TX): read tail pages, swap + clear ──────────────
    // Only pages created DURING Phase 4b need to be processed under the lock.  This is
    // typically just 0-2 pages worth of data, keeping the lock hold time minimal.
    compactionLock.writeLock().lock();
    try {
      database.begin();
      try {
        final int finalPageCount = mutableBucket.getDataPageCount();

        // Read only the tail pages created after Phase 4a's snapshot.
        Object[] tailData = null;
        if (finalPageCount > phase4aPageCount)
          tailData = mutableBucket.readPagesRangeForCompaction(phase4aPageCount + 1, finalPageCount);

        // Merge Phase 4b spill with tail data
        final Object[] toCompressFinal;
        if (phase4bSpill != null && tailData != null)
          toCompressFinal = mergeCompactionData(phase4bSpill, tailData);
        else if (phase4bSpill != null)
          toCompressFinal = phase4bSpill;
        else
          toCompressFinal = tailData;

        if (toCompressFinal != null) {
          final List<byte[][]> tailCompressed = new ArrayList<>();
          final List<long[]> tailMeta = new ArrayList<>();
          final List<double[]> tailMins = new ArrayList<>();
          final List<double[]> tailMaxs = new ArrayList<>();
          final List<double[]> tailSums = new ArrayList<>();
          final List<String[][]> tailTagDV = new ArrayList<>();
          buildCompressedBlocks(toCompressFinal, tailCompressed, tailMeta, tailMins, tailMaxs, tailSums, tailTagDV,
              false);
          if (!tailCompressed.isEmpty())
            sealedStore.appendBlocksToTempFile(tailCompressed, tailMeta, tailMins, tailMaxs, tailSums, tailTagDV,
                newBlockDirectory);
        }

        // Atomically swap temp file into the sealed store; updates in-memory blockDirectory.
        sealedStore.commitTempCompactionFile(newBlockDirectory);

        // Clear the entire mutable bucket and persist the crash-recovery flag reset.
        // No MVCC conflict: writeLock blocks all concurrent appendSamples().
        mutableBucket.clearDataPages();
        mutableBucket.setCompactionInProgress(false);
        database.commit();
      } catch (final Exception e) {
        if (database.isTransactionActive())
          database.rollback();
        // Restore sealed store to initial state so the next run re-compacts cleanly.
        // (The crash-recovery flag remains set, so a restart also handles this correctly.)
        try {
          sealedStore.truncateToBlockCount(initialBlockCount);
        } catch (final IOException te) {
          throw new IOException("Compaction failed and sealed store rollback also failed: " + te.getMessage(), e);
        }
        throw e instanceof IOException ? (IOException) e : new IOException("Compaction failed in phase 4c", e);
      }
    } finally {
      compactionLock.writeLock().unlock();
    }
  }

  /**
   * Sorts the given snapshot data by timestamp and builds compressed sealed blocks,
   * appending the results to the supplied output lists.
   * Called from Phase 2 (lock-free) and Phase 4 (under writeLock).
   *
   * @param returnSpill when {@code true}, the last partial chunk is NOT emitted as a block but
   *                    is instead returned as raw compaction data (same format as {@code data}).
   *                    The caller (Phase 4) prepends this spill to the partial-page data so the
   *                    bucket/block boundary is never split across phases.  Pass {@code false}
   *                    in Phase 4 where all remaining data must become blocks.
   * @return the spill raw data array, or {@code null} when {@code returnSpill} is {@code false}
   * or there is no partial last chunk.
   */
  private Object[] buildCompressedBlocks(
      final Object[] data,
      final List<byte[][]> compressedOut, final List<long[]> metaOut,
      final List<double[]> minsOut, final List<double[]> maxsOut, final List<double[]> sumsOut,
      final List<String[][]> tagDVOut, final boolean returnSpill) {

    final long[] timestamps = (long[]) data[0];
    final int totalSamples = timestamps.length;
    if (totalSamples == 0)
      return null;

    final int[] sortedIndices = sortIndices(timestamps);
    final long[] sortedTs = applyOrder(timestamps, sortedIndices);

    final int colCount = columns.size();
    final Object[][] sortedColArrays = new Object[colCount][];
    int nonTsIdx = 0;
    for (int c = 0; c < colCount; c++) {
      if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        sortedColArrays[c] = null;
      else {
        sortedColArrays[c] = applyOrderObjects((Object[]) data[nonTsIdx + 1], sortedIndices);
        nonTsIdx++;
      }
    }

    int chunkStart = 0;
    while (chunkStart < totalSamples) {
      int chunkEnd;
      if (compactionBucketIntervalMs > 0) {
        final long bucketStart = (sortedTs[chunkStart] / compactionBucketIntervalMs) * compactionBucketIntervalMs;
        final long bucketEnd = bucketStart + compactionBucketIntervalMs;
        chunkEnd = chunkStart;
        final int maxEnd = Math.min(chunkStart + SEALED_BLOCK_SIZE, totalSamples);
        while (chunkEnd < maxEnd && sortedTs[chunkEnd] < bucketEnd)
          chunkEnd++;
        if (chunkEnd == chunkStart)
          chunkEnd = chunkStart + 1;
      } else {
        chunkEnd = Math.min(chunkStart + SEALED_BLOCK_SIZE, totalSamples);
      }

      chunkEnd = adjustChunkForDictionaryLimit(chunkStart, chunkEnd, colCount, sortedColArrays);

      // If this is the last chunk and it may be partial, return it as spill so Phase 4 can
      // merge it with partial-page data and produce a single correctly-bounded block.
      if (returnSpill && chunkEnd == totalSamples) {
        final boolean isPartial;
        if (compactionBucketIntervalMs > 0)
          // Bucket-aligned: always hold back the last chunk since Phase 4 may have more
          // samples in the same bucket.
          isPartial = true;
        else
          // Fixed-size: partial when the chunk is smaller than a full sealed block.
          isPartial = (chunkEnd - chunkStart) < SEALED_BLOCK_SIZE;
        if (isPartial)
          return extractSpillData(sortedTs, sortedColArrays, chunkStart, chunkEnd, colCount);
      }

      final int chunkLen = chunkEnd - chunkStart;
      final long[] chunkTs = Arrays.copyOfRange(sortedTs, chunkStart, chunkEnd);

      final double[] mins = new double[colCount];
      final double[] maxs = new double[colCount];
      final double[] sums = new double[colCount];
      Arrays.fill(mins, Double.NaN);
      Arrays.fill(maxs, Double.NaN);

      final byte[][] compressedCols = new byte[colCount][];
      for (int c = 0; c < colCount; c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
          compressedCols[c] = DeltaOfDeltaCodec.encode(chunkTs);
        } else {
          final Object[] chunkValues = Arrays.copyOfRange(sortedColArrays[c], chunkStart, chunkEnd);
          compressedCols[c] = TimeSeriesSealedStore.compressColumn(columns.get(c), chunkValues);

          final TimeSeriesCodec codec = columns.get(c).getCompressionHint();
          if (codec == TimeSeriesCodec.GORILLA_XOR || codec == TimeSeriesCodec.SIMPLE8B) {
            double min = Double.MAX_VALUE, max = -Double.MAX_VALUE, sum = 0;
            for (final Object v : chunkValues) {
              final double d = v != null ? ((Number) v).doubleValue() : 0.0;
              if (d < min)
                min = d;
              if (d > max)
                max = d;
              sum += d;
            }
            mins[c] = min;
            maxs[c] = max;
            sums[c] = sum;
          }
        }
      }

      final String[][] chunkTagDistinctValues = new String[colCount][];
      for (int c = 0; c < colCount; c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG && sortedColArrays[c] != null) {
          final LinkedHashSet<String> distinctSet = new LinkedHashSet<>();
          for (int i = chunkStart; i < chunkEnd; i++) {
            final Object val = sortedColArrays[c][i];
            distinctSet.add(val != null ? val.toString() : "");
          }
          chunkTagDistinctValues[c] = distinctSet.toArray(new String[0]);
        }
      }

      compressedOut.add(compressedCols);
      metaOut.add(new long[]{chunkTs[0], chunkTs[chunkLen - 1], chunkLen});
      minsOut.add(mins);
      maxsOut.add(maxs);
      sumsOut.add(sums);
      tagDVOut.add(chunkTagDistinctValues);
      chunkStart = chunkEnd;
    }
    return null; // no spill
  }

  /**
   * Extracts the raw samples in {@code [from, to)} from the sorted compaction arrays and
   * returns them in the same format as the {@code data} parameter of
   * {@link #buildCompressedBlocks}: element 0 is {@code long[]} timestamps, subsequent
   * elements are {@code Object[]} non-timestamp column arrays.
   */
  private Object[] extractSpillData(final long[] sortedTs, final Object[][] sortedColArrays,
                                    final int from, final int to, final int colCount) {
    final Object[] spill = new Object[colCount];
    spill[0] = Arrays.copyOfRange(sortedTs, from, to);
    int spillIdx = 1;
    for (int c = 0; c < colCount; c++) {
      if (sortedColArrays[c] == null)
        continue; // TIMESTAMP column — skip; its data lives in element 0
      spill[spillIdx++] = Arrays.copyOfRange(sortedColArrays[c], from, to);
    }
    return spill;
  }

  /**
   * Concatenates two compaction data arrays (same format as the {@code data} parameter of
   * {@link #buildCompressedBlocks}).  Used to merge Phase-2 spill with Phase-4 partial-page
   * data before compressing them together.
   */
  private static Object[] mergeCompactionData(final Object[] a, final Object[] b) {
    final long[] tsA = (long[]) a[0];
    final long[] tsB = (long[]) b[0];
    final long[] mergedTs = new long[tsA.length + tsB.length];
    System.arraycopy(tsA, 0, mergedTs, 0, tsA.length);
    System.arraycopy(tsB, 0, mergedTs, tsA.length, tsB.length);
    final Object[] result = new Object[a.length];
    result[0] = mergedTs;
    for (int i = 1; i < a.length; i++) {
      final Object[] colA = (Object[]) a[i];
      final Object[] colB = (Object[]) b[i];
      final Object[] merged = new Object[colA.length + colB.length];
      System.arraycopy(colA, 0, merged, 0, colA.length);
      System.arraycopy(colB, 0, merged, colA.length, colB.length);
      result[i] = merged;
    }
    return result;
  }

  /**
   * Best-effort: clear the compaction-in-progress flag after a non-crash error.
   */
  private void clearCompactionFlagBestEffort() {
    compactionLock.writeLock().lock();
    try {
      database.begin();
      mutableBucket.setCompactionInProgress(false);
      database.commit();
    } catch (final Exception ignored) {
      if (database.isTransactionActive())
        try {
          database.rollback();
        } catch (final Exception re) { /* ignored */ }
    } finally {
      compactionLock.writeLock().unlock();
    }
  }

  public TimeSeriesBucket getMutableBucket() {
    return mutableBucket;
  }

  public TimeSeriesSealedStore getSealedStore() {
    return sealedStore;
  }

  /**
   * Returns the compaction read/write lock.
   * Callers that aggregate both sealed and mutable data must hold the read lock for the
   * entire duration to prevent compaction from completing between the two reads (which
   * would cause the compacted mutable data to be invisible to the caller).
   */
  java.util.concurrent.locks.ReadWriteLock getCompactionLock() {
    return compactionLock;
  }

  public int getShardIndex() {
    return shardIndex;
  }

  @Override
  public void close() throws IOException {
    mutableBucket.close();
    sealedStore.close();
  }

  // --- Private helpers ---

  private static void addFiltered(final List<Object[]> results, final List<Object[]> source, final TagFilter filter,
                                  final int[] columnIndices) {
    if (filter == null)
      results.addAll(source);
    else
      for (final Object[] row : source)
        // Use matchesMapped() so the filter works correctly when columnIndices is a subset.
        if (filter.matchesMapped(row, columnIndices))
          results.add(row);
  }

  private static int[] sortIndices(final long[] timestamps) {
    final int n = timestamps.length;
    final int[] indices = new int[n];
    for (int i = 0; i < n; i++)
      indices[i] = i;
    // Merge sort on primitive int[] to avoid Integer boxing
    mergeSort(indices, new int[n], 0, n, timestamps);
    return indices;
  }

  private static void mergeSort(final int[] arr, final int[] temp, final int from, final int to, final long[] keys) {
    if (to - from <= 1)
      return;
    final int mid = (from + to) >>> 1;
    mergeSort(arr, temp, from, mid, keys);
    mergeSort(arr, temp, mid, to, keys);
    // Merge
    int i = from, j = mid, k = from;
    while (i < mid && j < to) {
      if (keys[arr[i]] <= keys[arr[j]])
        temp[k++] = arr[i++];
      else
        temp[k++] = arr[j++];
    }
    while (i < mid)
      temp[k++] = arr[i++];
    while (j < to)
      temp[k++] = arr[j++];
    System.arraycopy(temp, from, arr, from, to - from);
  }

  private static long[] applyOrder(final long[] data, final int[] indices) {
    final long[] result = new long[data.length];
    for (int i = 0; i < indices.length; i++)
      result[i] = data[indices[i]];
    return result;
  }

  private static Object[] applyOrderObjects(final Object[] data, final int[] indices) {
    final Object[] result = new Object[data.length];
    for (int i = 0; i < indices.length; i++)
      result[i] = data[indices[i]];
    return result;
  }

  /**
   * If any DICTIONARY column in [chunkStart, chunkEnd) exceeds {@link DictionaryCodec#MAX_DICTIONARY_SIZE},
   * shrinks chunkEnd until all dictionary columns fit. Guarantees at least one row per chunk.
   */
  private int adjustChunkForDictionaryLimit(final int chunkStart, int chunkEnd,
                                            final int colCount, final Object[][] sortedColArrays) {
    for (int c = 0; c < colCount; c++) {
      if (columns.get(c).getCompressionHint() != TimeSeriesCodec.DICTIONARY || sortedColArrays[c] == null)
        continue;
      final HashSet<Object> distinct = new HashSet<>();
      for (int i = chunkStart; i < chunkEnd; i++) {
        distinct.add(sortedColArrays[c][i] != null ? sortedColArrays[c][i] : "");
        if (distinct.size() > DictionaryCodec.MAX_DICTIONARY_SIZE) {
          // Shrink chunk to i (exclusive) — the last row that still fits
          chunkEnd = Math.max(chunkStart + 1, i);
          break;
        }
      }
    }
    return chunkEnd;
  }
}
