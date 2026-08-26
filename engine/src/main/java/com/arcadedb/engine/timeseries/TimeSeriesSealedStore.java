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

import com.arcadedb.log.LogManager;
import com.arcadedb.engine.timeseries.codec.DeltaOfDeltaCodec;
import com.arcadedb.engine.timeseries.codec.DictionaryCodec;
import com.arcadedb.engine.timeseries.codec.GorillaXORCodec;
import com.arcadedb.engine.timeseries.codec.Simple8bCodec;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.engine.timeseries.simd.TimeSeriesVectorOps;
import com.arcadedb.engine.timeseries.simd.TimeSeriesVectorOpsProvider;
import com.arcadedb.schema.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Immutable columnar storage for compacted TimeSeries data.
 * Uses FileChannel positioned reads for zero-overhead access.
 * <p>
 * Index file (.ts.sealed) layout — 27-byte header:
 * - [0..3]   magic "TSIX" (4 bytes)
 * - [4]      format version byte (always {@value #CURRENT_VERSION})
 * - [5..6]   column count (short)
 * - [7..10]  block count (int)
 * - [11..18] global min timestamp (long)
 * - [19..26] global max timestamp (long)
 * - [27..]   block entries (inline metadata + compressed column data)
 * <p>
 * Block entry layout:
 * - magic "TSBL" (4), minTs (8), maxTs (8), sampleCount (4), colSizes (4*colCount)
 * - numericColCount (4), [min (8) + max (8) + sum (8)] * numericColCount (schema order, no colIdx)
 * - tag metadata: tagColCount (2), per TAG column: distinctCount (2), per value: len (2) + UTF-8 bytes
 * - compressed column data bytes
 * - blockCRC32 (4) — CRC32 of everything from blockMagic to end of compressed data
 * <p>
 * <b>High-Availability / Replication note:</b>
 * Sealed store files ({@code .ts.sealed}) are written via {@link RandomAccessFile} and
 * {@link FileChannel} directly to the local filesystem, <em>bypassing</em> ArcadeDB's
 * page-level replication infrastructure. This is by design: compacted time-series data
 * is derived (it is produced by compacting the replicated mutable {@link TimeSeriesBucket}
 * pages) and therefore does not need to be replicated separately. Each HA node independently
 * performs its own compaction from its own replicated mutable buckets, eventually reaching
 * an equivalent sealed store. In-flight mutable data (the {@code .tstb} bucket files) is
 * fully replicated through the normal {@link com.arcadedb.engine.PaginatedComponent} path.
 * The consequence is that, immediately after a failover, a follower that has not yet
 * compacted may serve queries from the mutable bucket only until its maintenance scheduler
 * runs the next compaction cycle.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesSealedStore implements AutoCloseable {

  public static final  int CURRENT_VERSION  = 0;
  private static final int MAGIC_VALUE       = 0x54534958; // "TSIX"
  private static final int BLOCK_MAGIC_VALUE = 0x5453424C; // "TSBL"
  private static final int HEADER_SIZE       = 27;
  // Shared with DeltaOfDeltaCodec and GorillaXORCodec: all three validate/use the same limit
  private static final int MAX_BLOCK_SIZE    = DeltaOfDeltaCodec.MAX_BLOCK_SIZE;
  /**
   * How far a recomputed column sum may sit from the one a block declares before the DEEP check calls it a
   * disagreement (issue #6360). Relative rather than absolute: adding the same doubles back in the same order is
   * reproducible, but nothing in the format promises the order, so this absorbs the rounding of the accumulation
   * and nothing larger.
   */
  private static final double SUM_RELATIVE_TOLERANCE = 1e-9;

  private final String               basePath;
  private final List<ColumnDefinition> columns;
  private       RandomAccessFile     indexFile;
  private       FileChannel          indexChannel;

  enum BlockMatchResult { SKIP, FAST_PATH, SLOW_PATH }

  // In-memory block directory (loaded at open) — protected by directoryLock
  private final List<BlockEntry>    blockDirectory = new ArrayList<>();
  private final ReadWriteLock       directoryLock  = new ReentrantReadWriteLock();
  private volatile long             globalMinTs    = Long.MAX_VALUE;  // volatile: read without write lock
  private volatile long             globalMaxTs    = Long.MIN_VALUE;  // volatile: read without write lock
  private          boolean          headerDirty;
  // Counts how many times downsampleBlocks actually rewrote the sealed file (i.e., selected at least one
  // block to downsample). Used by tests to assert idempotency: a steady-state cycle must not rewrite.
  private          long             downsampleRewriteCount;

  static final class BlockEntry {
    final long     minTimestamp;
    final long     maxTimestamp;
    final int      sampleCount;
    final long[]   columnOffsets;
    final int[]    columnSizes;
    final double[] columnMins;   // per-column min (NaN for non-numeric)
    final double[] columnMaxs;   // per-column max
    final double[] columnSums;   // per-column sum
    String[][]     tagDistinctValues; // indexed by schema column index, null for non-TAG columns
    // Where this block begins in the file, and the CRC32 the writer stored immediately after its data. Both are
    // set by EVERY path that produces an entry - appendBlock, writeNewBlockToFile and loadDirectory - so they
    // describe the block whether this process wrote it or found it on disk (issue #6360 item 3). They used to be
    // assigned by loadDirectory ALONE, which left every entry this process wrote holding zero in both, and the
    // only thing keeping that from being read was crcValidated short-circuiting the two readers below.
    final long     blockStartOffset; // file offset where the block's metadata begins
    int            storedCRC;        // CRC32 over metadata + compressed columns, as stored after the data
    volatile boolean crcValidated;   // true once the CRC has been checked (volatile: read without lock)
    // Coarsest granularity (ms) this block has already been downsampled to; 0 = raw / never downsampled.
    // In-memory only (not persisted): it makes downsampling idempotent across maintenance cycles within a
    // running process. After a restart the marker resets to 0, so a single follow-up downsampling cycle may
    // re-touch an already-coarse block once before re-marking it; that is harmless and self-healing.
    long           downsampledGranularityMs;

    /**
     * {@code blockStartOffset} is a CONSTRUCTOR PARAMETER and not a field a caller may forget (#6360 item 3): every
     * one of the three sites that builds an entry - {@code appendBlock}, {@code writeNewBlockToFile} and
     * {@code loadDirectory} - knows the offset before it builds one, and the previous shape, where only the last of
     * them assigned it afterwards, is exactly how two of the three came to hold zero.
     * <p>
     * {@code crcValidated} starts FALSE for the same reason. A block this process wrote is byte-for-byte what it
     * computed the CRC over, so re-reading it buys nothing - but that shortcut is granted by
     * {@link #recordWrittenCRC(int)}, together with the CRC it is a shortcut for, so a write path that forgets to
     * record one cannot silently inherit the other.
     */
    BlockEntry(final long minTs, final long maxTs, final int sampleCount, final int columnCount,
        final double[] mins, final double[] maxs, final double[] sums, final long blockStartOffset) {
      this.minTimestamp = minTs;
      this.maxTimestamp = maxTs;
      this.sampleCount = sampleCount;
      this.columnOffsets = new long[columnCount];
      this.columnSizes = new int[columnCount];
      this.columnMins = mins;
      this.columnMaxs = maxs;
      this.columnSums = sums;
      this.blockStartOffset = blockStartOffset;
      this.crcValidated = false;
    }

    /**
     * Records the CRC32 this block was written with, and that it therefore needs no reading back to be trusted.
     * <p>
     * The two go together deliberately: setting the flag without the CRC is the #6360 defect (a reader that then
     * validates compares against zero), and setting the CRC without the flag only costs a redundant read. One call
     * does both, so there is no way to do half of it.
     */
    void recordWrittenCRC(final int crc) {
      this.storedCRC = crc;
      this.crcValidated = true;
    }
  }

  public TimeSeriesSealedStore(final String basePath, final List<ColumnDefinition> columns) throws IOException {
    this.basePath = basePath;
    this.columns = columns;

    // Clean up stale .tmp files left by interrupted shutdown or maintenance
    final File tmpFile = new File(basePath + ".ts.sealed.tmp");
    if (tmpFile.exists() && !tmpFile.delete())
      throw new IOException("Failed to delete stale temporary file: " + tmpFile.getAbsolutePath());

    // Clean up a stale .incoming file left by an HA sealed-blob install that crashed before the
    // atomic move (issue #4382). The live .ts.sealed below is authoritative; the Raft entry that
    // produced the .incoming will be re-applied on catch-up.
    final File incomingFile = new File(basePath + ".ts.sealed.incoming");
    if (incomingFile.exists() && !incomingFile.delete())
      throw new IOException("Failed to delete stale incoming file: " + incomingFile.getAbsolutePath());

    final File f = new File(basePath + ".ts.sealed");
    final boolean exists = f.exists();
    this.indexFile = new RandomAccessFile(f, "rw");
    this.indexChannel = indexFile.getChannel();

    try {
      if (exists && indexFile.length() >= HEADER_SIZE)
        loadDirectory();
      else
        writeEmptyHeader();
    } catch (final IOException e) {
      // Close handles to avoid leaking them if initialization fails
      try { indexChannel.close(); } catch (final IOException ignored) {}
      try { indexFile.close(); } catch (final IOException ignored) {}
      throw e;
    }
  }

  /**
   * Appends a block of compressed column data with per-column statistics.
   * Stats enable block-level aggregation without decompression.
   *
   * @param sampleCount       number of samples in the block
   * @param minTs             minimum timestamp
   * @param maxTs             maximum timestamp
   * @param compressedColumns compressed byte arrays, one per column
   * @param columnMins        per-column min (NaN for non-numeric columns)
   * @param columnMaxs        per-column max (NaN for non-numeric columns)
   * @param columnSums        per-column sum (NaN for non-numeric columns)
   */
  public void appendBlock(final int sampleCount, final long minTs, final long maxTs,
      final byte[][] compressedColumns,
      final double[] columnMins, final double[] columnMaxs, final double[] columnSums,
      final String[][] tagDistinctValues) throws IOException {
    directoryLock.writeLock().lock();
    try {
      final int colCount = columns.size();

      // Count numeric columns (those with non-NaN stats)
      int numericColCount = 0;
      for (int c = 0; c < colCount; c++)
        if (!Double.isNaN(columnMins[c]))
          numericColCount++;

      // Build tag metadata section
      final byte[] tagMeta = buildTagMetadata(tagDistinctValues, colCount);

      // Block header: magic(4) + minTs(8) + maxTs(8) + sampleCount(4) + colSizes(4*colCount)
      //              + numericColCount(4) + [min(8) + max(8) + sum(8)] * numericColCount
      //              + tag metadata
      final int statsSize = 4 + (8 + 8 + 8) * numericColCount;
      final int metaSize = 4 + 8 + 8 + 4 + 4 * colCount + statsSize + tagMeta.length;
      final ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
      metaBuf.putInt(BLOCK_MAGIC_VALUE);
      metaBuf.putLong(minTs);
      metaBuf.putLong(maxTs);
      metaBuf.putInt(sampleCount);
      for (final byte[] col : compressedColumns)
        metaBuf.putInt(col.length);

      // Write stats section (schema order, no colIdx — iterate columns, skip non-numeric)
      metaBuf.putInt(numericColCount);
      for (int c = 0; c < colCount; c++) {
        if (!Double.isNaN(columnMins[c])) {
          metaBuf.putDouble(columnMins[c]);
          metaBuf.putDouble(columnMaxs[c]);
          metaBuf.putDouble(columnSums[c]);
        }
      }

      // Write tag metadata
      metaBuf.put(tagMeta);
      metaBuf.flip();

      // Compute CRC32 over meta + compressed data
      // Use metaBuf.limit() (not .array().length) since the backing array may be larger after flip
      final CRC32 crc = new CRC32();
      crc.update(metaBuf.array(), 0, metaBuf.limit());

      final long blockStart = indexFile.length();
      long offset = blockStart;
      indexFile.seek(offset);
      indexFile.write(metaBuf.array(), 0, metaBuf.limit());
      offset += metaSize;

      // #6360 item 3: the offset is given to the entry rather than patched onto it afterwards, so the entry
      // describes where its block is no matter which side of a restart wrote it.
      final BlockEntry entry = new BlockEntry(minTs, maxTs, sampleCount, colCount, columnMins, columnMaxs, columnSums,
          blockStart);
      entry.tagDistinctValues = tagDistinctValues;
      // Write compressed column data
      for (int c = 0; c < colCount; c++) {
        entry.columnOffsets[c] = offset;
        entry.columnSizes[c] = compressedColumns[c].length;
        crc.update(compressedColumns[c]);
        indexFile.write(compressedColumns[c]);
        offset += compressedColumns[c].length;
      }

      // Write block CRC32
      entry.recordWrittenCRC((int) crc.getValue());
      final ByteBuffer crcBuf = ByteBuffer.allocate(4);
      crcBuf.putInt(entry.storedCRC);
      crcBuf.flip();
      indexFile.write(crcBuf.array());

      blockDirectory.add(entry);

      if (minTs < globalMinTs)
        globalMinTs = minTs;
      if (maxTs > globalMaxTs)
        globalMaxTs = maxTs;

      headerDirty = true;
    } finally {
      directoryLock.writeLock().unlock();
    }
  }

  /**
   * Flushes the header to disk if any blocks have been appended since the last flush.
   * Called automatically by {@link #close()}.
   */
  public void flushHeader() throws IOException {
    directoryLock.writeLock().lock();
    try {
      if (headerDirty) {
        rewriteHeader();
        headerDirty = false;
      }
    } finally {
      directoryLock.writeLock().unlock();
    }
  }

  /**
   * Scans blocks overlapping the given time range and returns decompressed data.
   */
  public List<Object[]> scanRange(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    // Hold the read lock for the entire scan including file I/O.
    // This prevents concurrent writers from closing/replacing the file channel
    // while reads are in progress (stale offset race).
    directoryLock.readLock().lock();
    try {
      final List<Object[]> results = new ArrayList<>();
      final int tsColIdx = findTimestampColumnIndex();

      for (final BlockEntry entry : blockDirectory) {
        if (entry.maxTimestamp < fromTs || entry.minTimestamp > toTs)
          continue;

        final BlockMatchResult tagMatch = tagFilter != null
            ? blockMatchesTagFilter(entry, tagFilter)
            : BlockMatchResult.FAST_PATH;
        if (tagMatch == BlockMatchResult.SKIP)
          continue;

        final long[] timestamps = decompressTimestamps(entry, tsColIdx);
        final Object[][] decompressedCols = decompressColumns(entry, columnIndices, tsColIdx);

        final int resultCols = decompressedCols.length + 1;
        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] < fromTs || timestamps[i] > toTs)
            continue;

          final Object[] row = new Object[resultCols];
          row[0] = timestamps[i];
          for (int c = 0; c < decompressedCols.length; c++)
            row[c + 1] = decompressedCols[c][i];

          // For SLOW_PATH blocks (mixed tag values), apply per-row filtering.
          // Use matchesMapped() so the filter works correctly when columnIndices is a subset.
          if (tagMatch == BlockMatchResult.SLOW_PATH && !tagFilter.matchesMapped(row, columnIndices))
            continue;

          results.add(row);
        }
      }
      return results;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Returns an iterator over sealed blocks overlapping the given time range.
   * Eagerly collects all matching rows under the read lock to prevent stale
   * file offsets after atomic file replacement by concurrent writers.
   * <p>
   * Optimizations:
   * - Binary search on block directory to skip to first matching block
   * - Early termination when blocks are past the time range (blocks are sorted)
   * - Timestamps decompressed first; value columns only if the block has matches
   * - Binary search within each block's sorted timestamps for the matching range
   *
   * @param fromTs        start timestamp (inclusive)
   * @param toTs          end timestamp (inclusive)
   * @param columnIndices which columns to return (null = all)
   *
   * @return iterator yielding Object[] { timestamp, col1, col2, ... }.
   *         <b>Note:</b> all matching rows are fully materialised into memory before the iterator
   *         is returned, because the read lock must be held for all file I/O and released before
   *         the caller iterates.  For very large time ranges consider using aggregation instead.
   */
  public Iterator<Object[]> iterateRange(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    // Hold the read lock for all file I/O to prevent stale offsets after
    // atomic file replacement by concurrent writers (truncate/downsample).
    directoryLock.readLock().lock();
    try {
      final List<Object[]> results = new ArrayList<>();
      final int tsColIdx = findTimestampColumnIndex();
      final int dirSize = blockDirectory.size();

      // Binary search: find first block whose maxTimestamp >= fromTs
      int startBlockIdx = 0;
      if (dirSize > 0) {
        int lo = 0, hi = dirSize - 1;
        while (lo < hi) {
          final int mid = (lo + hi) >>> 1;
          if (blockDirectory.get(mid).maxTimestamp < fromTs)
            lo = mid + 1;
          else
            hi = mid;
        }
        startBlockIdx = lo;
      }

      for (int blockIdx = startBlockIdx; blockIdx < dirSize; blockIdx++) {
        final BlockEntry entry = blockDirectory.get(blockIdx);

        // Early termination: blocks are sorted, so if minTs > toTs all remaining are past range
        if (entry.minTimestamp > toTs)
          break;

        if (entry.maxTimestamp < fromTs)
          continue;

        final BlockMatchResult tagMatch = tagFilter != null
            ? blockMatchesTagFilter(entry, tagFilter)
            : BlockMatchResult.FAST_PATH;
        if (tagMatch == BlockMatchResult.SKIP)
          continue;

        final long[] ts = decompressTimestamps(entry, tsColIdx);
        final int start = lowerBound(ts, fromTs);
        final int end = upperBound(ts, toTs);

        if (start >= end)
          continue;

        final Object[][] decompCols = decompressColumns(entry, columnIndices, tsColIdx);
        final int resultCols = decompCols.length + 1;

        for (int i = start; i < end; i++) {
          final Object[] row = new Object[resultCols];
          row[0] = ts[i];
          for (int c = 0; c < decompCols.length; c++)
            row[c + 1] = decompCols[c][i];
          // Use matchesMapped() so the filter works correctly when columnIndices is a subset.
          if (tagMatch == BlockMatchResult.SLOW_PATH && !tagFilter.matchesMapped(row, columnIndices))
            continue;
          results.add(row);
        }
      }
      return results.iterator();
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Scans sealed blocks <em>newest-first</em> and returns at most {@code limit} rows in descending
   * timestamp order (issue #5414).
   * <p>
   * This is the read path behind last-point queries ({@code ORDER BY <ts> DESC LIMIT n},
   * {@code ts.last()}). Because blocks are stored in ascending timestamp order, walking the block
   * directory backwards lets the scan stop as soon as {@code limit} rows have been produced and no
   * older block can still hold a newer row. An unbounded last-point query therefore costs
   * O(blocks touched) instead of O(series), without any extra persisted state.
   * <p>
   * Early termination is guarded by the timestamp of the oldest row currently retained, so it stays
   * correct even if two adjacent blocks happen to overlap in time.
   *
   * @param fromTs        start timestamp (inclusive)
   * @param toTs          end timestamp (inclusive)
   * @param columnIndices which columns to return (null = all)
   * @param tagFilter     optional tag filter, applied at block level when possible
   * @param limit         maximum number of rows to return; {@code <= 0} means unlimited
   * @param metrics       optional block-level counters, may be {@code null}
   *
   * @return rows sorted by descending timestamp, at most {@code limit} of them
   */
  public List<Object[]> scanRangeDescending(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter, final int limit, final AggregationMetrics metrics) throws IOException {
    final int need = limit > 0 ? limit : Integer.MAX_VALUE;

    directoryLock.readLock().lock();
    try {
      final List<Object[]> results = new ArrayList<>();
      final int tsColIdx = findTimestampColumnIndex();
      final int dirSize = blockDirectory.size();
      if (dirSize == 0)
        return results;

      // Binary search: find the last block whose minTimestamp <= toTs. Everything after it starts
      // beyond the requested upper bound.
      int lo = 0, hi = dirSize - 1;
      while (lo < hi) {
        final int mid = (lo + hi + 1) >>> 1;
        if (blockDirectory.get(mid).minTimestamp > toTs)
          hi = mid - 1;
        else
          lo = mid;
      }
      final int startBlockIdx = lo;

      // Timestamp of the oldest row retained so far: once `need` rows are held, any block whose
      // maxTimestamp is older than this cannot contribute and the walk stops.
      long cutoffTs = Long.MIN_VALUE;

      for (int blockIdx = startBlockIdx; blockIdx >= 0; blockIdx--) {
        final BlockEntry entry = blockDirectory.get(blockIdx);

        // Early termination: blocks are ordered by ascending timestamp, so once a block ends before
        // the requested lower bound no older block can be in range either.
        if (entry.maxTimestamp < fromTs)
          break;

        if (results.size() >= need && entry.maxTimestamp < cutoffTs)
          break;

        if (entry.minTimestamp > toTs)
          continue;

        final BlockMatchResult tagMatch = tagFilter != null
            ? blockMatchesTagFilter(entry, tagFilter)
            : BlockMatchResult.FAST_PATH;
        if (tagMatch == BlockMatchResult.SKIP) {
          if (metrics != null)
            metrics.addSkippedBlock();
          continue;
        }

        final long[] ts = decompressTimestamps(entry, tsColIdx);
        final int start = lowerBound(ts, fromTs);
        final int end = upperBound(ts, toTs);
        if (start >= end)
          continue;

        if (metrics != null) {
          if (tagMatch == BlockMatchResult.SLOW_PATH)
            metrics.addSlowPathBlock();
          else
            metrics.addFastPathBlock();
        }

        // Columns stay unboxed: only the rows that survive the tag filter and make the top-N are
        // materialised, instead of boxing every value of the block (issue #5416).
        final RawColumn[] rawCols = decompressColumnsRaw(entry, columnIndices, tsColIdx);
        final int resultCols = rawCols.length + 1;

        // Rows inside a block are ascending, so walking backwards yields descending order and the
        // first `need` matches found are the newest ones in this block.
        int taken = 0;
        for (int i = end - 1; i >= start && taken < need; i--) {
          if (tagMatch == BlockMatchResult.SLOW_PATH && !matchesRawColumns(rawCols, i, tagFilter, columnIndices))
            continue;
          final Object[] row = new Object[resultCols];
          row[0] = ts[i];
          for (int c = 0; c < rawCols.length; c++)
            row[c + 1] = rawCols[c].valueAt(i);
          results.add(row);
          taken++;
        }
        if (metrics != null)
          metrics.addMaterializedRows(taken);

        if (results.size() >= need) {
          trimToDescendingLimit(results, need);
          cutoffTs = (long) results.getLast()[0];
        }
      }

      trimToDescendingLimit(results, need);
      return results;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Sorts the rows by descending timestamp and drops everything past {@code need}.
   */
  static void trimToDescendingLimit(final List<Object[]> rows, final int need) {
    rows.sort((a, b) -> Long.compare((long) b[0], (long) a[0]));
    while (rows.size() > need)
      rows.removeLast();
  }

  /**
   * Finds the first index where ts[i] >= target (lower bound).
   */
  private static int lowerBound(final long[] ts, final long target) {
    int lo = 0, hi = ts.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (ts[mid] < target)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  /**
   * Finds the first index where ts[i] > target (upper bound).
   */
  private static int upperBound(final long[] ts, final long target) {
    int lo = 0, hi = ts.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (ts[mid] <= target)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  private static int lowerBound(final long[] ts, final int from, final int to, final long target) {
    int lo = from, hi = to;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (ts[mid] < target)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  private static int upperBound(final long[] ts, final int from, final int to, final long target) {
    int lo = from, hi = to;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (ts[mid] <= target)
        lo = mid + 1;
      else
        hi = mid;
    }
    return lo;
  }

  /**
   * Push-down aggregation on sealed blocks.
   */
  public AggregationResult aggregate(final long fromTs, final long toTs, final int columnIndex,
      final AggregationType type, final long bucketIntervalMs) throws IOException {
    final AggregationResult result = new AggregationResult();
    final int tsColIdx = findTimestampColumnIndex();
    final int targetColSchemaIdx = findNonTsColumnSchemaIndex(columnIndex);

    final long singleBucketTs = TimeSeriesEngine.singleBucketAnchor(fromTs);

    // Hold the read lock for the entire scan including file I/O to prevent stale offsets
    // after atomic file replacement by concurrent writers (truncate/downsample).
    directoryLock.readLock().lock();
    try {
      for (final BlockEntry entry : blockDirectory) {
        if (entry.maxTimestamp < fromTs || entry.minTimestamp > toTs)
          continue;

        final long[] timestamps = decompressTimestamps(entry, tsColIdx);
        final double[] values = decompressDoubleColumn(entry, targetColSchemaIdx);

        for (int i = 0; i < timestamps.length; i++) {
          if (timestamps[i] < fromTs || timestamps[i] > toTs)
            continue;

          final long bucketTs = bucketIntervalMs > 0
              ? Math.floorDiv(timestamps[i], bucketIntervalMs) * bucketIntervalMs
              : singleBucketTs;

          accumulateSample(result, bucketTs, values[i], type);
        }
      }
      return result;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Push-down multi-column aggregation on sealed blocks.
   * Processes compressed blocks directly without creating Object[] row arrays.
   * When a block fits entirely within a single time bucket, uses block-level
   * statistics (min/max/sum/count) to skip decompression entirely.
   */
  public void aggregateMultiBlocks(final long fromTs, final long toTs,
      final List<MultiColumnAggregationRequest> requests, final long bucketIntervalMs,
      final MultiColumnAggregationResult result, final AggregationMetrics metrics,
      final TagFilter tagFilter) throws IOException {
    final int tsColIdx = findTimestampColumnIndex();
    final int reqCount = requests.size();

    // Pre-compute schema column indices for each request
    final int[] schemaColIndices = new int[reqCount];
    final boolean[] isCount = new boolean[reqCount];
    for (int r = 0; r < reqCount; r++) {
      isCount[r] = requests.get(r).type() == AggregationType.COUNT;
      if (!isCount[r])
        schemaColIndices[r] = requests.get(r).columnIndex();
      else
        schemaColIndices[r] = -1;
    }

    final double[] rowValues = new double[reqCount];

    // Pre-allocate decode buffers reused across all blocks in this call
    final long[] reusableTsBuf = new long[MAX_BLOCK_SIZE];
    final double[] reusableValBuf = new double[MAX_BLOCK_SIZE];

    final long singleBucketTs = TimeSeriesEngine.singleBucketAnchor(fromTs);

    // Hold the read lock for the entire scan including file I/O to prevent stale offsets
    // after atomic file replacement by concurrent writers (truncate/downsample).
    directoryLock.readLock().lock();
    try {
      for (final BlockEntry entry : blockDirectory) {
        if (entry.maxTimestamp < fromTs || entry.minTimestamp > toTs) {
          if (metrics != null)
            metrics.addSkippedBlock();
          continue;
        }

        // Block-level tag filter: SKIP blocks that cannot contain matching rows
        final BlockMatchResult tagMatch = tagFilter != null
            ? blockMatchesTagFilter(entry, tagFilter)
            : BlockMatchResult.FAST_PATH;
        if (tagMatch == BlockMatchResult.SKIP) {
          if (metrics != null)
            metrics.addSkippedBlock();
          continue;
        }

        // Check if entire block falls within a single time bucket and is fully inside the query range
        // FAST_PATH: block is homogeneous for the filtered tag, so block-level stats are valid
        if (tagMatch == BlockMatchResult.FAST_PATH
            && bucketIntervalMs > 0 && entry.minTimestamp >= fromTs && entry.maxTimestamp <= toTs) {
          final long blockMinBucket = Math.floorDiv(entry.minTimestamp, bucketIntervalMs) * bucketIntervalMs;
          final long blockMaxBucket = Math.floorDiv(entry.maxTimestamp, bucketIntervalMs) * bucketIntervalMs;

          if (blockMinBucket == blockMaxBucket) {
            // FAST PATH: use block-level stats directly — no decompression needed
            if (metrics != null)
              metrics.addFastPathBlock();
            for (int r = 0; r < reqCount; r++) {
              if (isCount[r])
                rowValues[r] = entry.sampleCount;
              else {
                final int sci = schemaColIndices[r];
                rowValues[r] = switch (requests.get(r).type()) {
                  case MIN -> entry.columnMins[sci];
                  case MAX -> entry.columnMaxs[sci];
                  case SUM, AVG -> entry.columnSums[sci];
                  case COUNT -> entry.sampleCount;
                };
              }
            }
            result.accumulateBlockStats(blockMinBucket, rowValues, entry.sampleCount);
            continue;
          }
        }

        // SLOW PATH: decompress and iterate (boundary blocks spanning multiple buckets)
        if (metrics != null)
          metrics.addSlowPathBlock();

        // Coalesced I/O: read all column data in one pread call
        long t0 = metrics != null ? System.nanoTime() : 0;
        final byte[] blockData = readBlockData(entry);
        if (metrics != null)
          metrics.addIo(System.nanoTime() - t0);

        // Decode timestamps into reusable buffer
        t0 = metrics != null ? System.nanoTime() : 0;
        final int tsCount = DeltaOfDeltaCodec.decode(
            sliceColumn(blockData, entry, tsColIdx), reusableTsBuf);
        if (metrics != null)
          metrics.addDecompTs(System.nanoTime() - t0);

        // Decompress only the columns needed by the requests (deduplicated)
        // Use reusable buffer for the first column; allocate for additional distinct columns
        final double[][] decompressedCols = new double[columns.size()][];
        boolean reusableValBufferUsed = false;
        for (int r = 0; r < reqCount; r++) {
          if (!isCount[r] && decompressedCols[schemaColIndices[r]] == null) {
            t0 = metrics != null ? System.nanoTime() : 0;
            final byte[] colBytes = sliceColumn(blockData, entry, schemaColIndices[r]);
            final ColumnDefinition col = columns.get(schemaColIndices[r]);
            if (!reusableValBufferUsed && col.getCompressionHint() == TimeSeriesCodec.GORILLA_XOR) {
              // Decode into reusable buffer (only safe for one column at a time)
              GorillaXORCodec.decode(colBytes, reusableValBuf);
              decompressedCols[schemaColIndices[r]] = reusableValBuf;
              reusableValBufferUsed = true;
            } else {
              decompressedCols[schemaColIndices[r]] = decompressDoubleColumnFromBytes(colBytes, schemaColIndices[r]);
            }
            if (metrics != null)
              metrics.addDecompVal(System.nanoTime() - t0);
          }
        }

        // Decompress tag columns for SLOW_PATH tag filtering
        final boolean needRowTagFilter = tagFilter != null && tagMatch == BlockMatchResult.SLOW_PATH;
        String[][] tagCols = null;
        List<TagFilter.Condition> filterConditions = null;
        if (needRowTagFilter) {
          filterConditions = tagFilter.getConditions();
          tagCols = new String[filterConditions.size()][];
          for (int ci = 0; ci < filterConditions.size(); ci++) {
            final int schemaIdx = findNonTsColumnSchemaIndex(filterConditions.get(ci).columnIndex());
            final byte[] colBytes = sliceColumn(blockData, entry, schemaIdx);
            tagCols[ci] = DictionaryCodec.decode(colBytes);
          }
        }

        // Use tsCount (not array length) since reusableTsBuf may be larger than actual data
        final long[] timestamps = reusableTsBuf;

        // Aggregate using segment-based vectorized accumulation
        t0 = metrics != null ? System.nanoTime() : 0;

        // Clip to query range using binary search on sorted timestamps
        final int rangeStart = lowerBound(timestamps, 0, tsCount, fromTs);
        final int rangeEnd = upperBound(timestamps, 0, tsCount, toTs);

        if (bucketIntervalMs > 0) {
          if (needRowTagFilter) {
            // Per-row accumulation with tag filtering (cannot use SIMD on mixed-tag blocks)
            for (int i = rangeStart; i < rangeEnd; i++) {
              if (!matchesTagConditions(tagCols, filterConditions, i))
                continue;
              final long bucketTs = Math.floorDiv(timestamps[i], bucketIntervalMs) * bucketIntervalMs;
              for (int r = 0; r < reqCount; r++) {
                if (isCount[r])
                  result.accumulateSingleStat(bucketTs, r, 1.0, 1);
                else
                  result.accumulateSingleStat(bucketTs, r, decompressedCols[schemaColIndices[r]][i], 1);
              }
            }
          } else {
            // Vectorized path: find contiguous segments within each bucket and use SIMD ops
            final TimeSeriesVectorOps ops = TimeSeriesVectorOpsProvider.getInstance();

            int segStart = rangeStart;
            while (segStart < rangeEnd) {
              final long bucketTs = Math.floorDiv(timestamps[segStart], bucketIntervalMs) * bucketIntervalMs;
              final long nextBucketTs = bucketTs + bucketIntervalMs;

              // Find end of this bucket's segment
              int segEnd = segStart + 1;
              while (segEnd < rangeEnd && timestamps[segEnd] < nextBucketTs)
                segEnd++;

              final int segLen = segEnd - segStart;

              // Accumulate each request using vectorized ops on the segment
              for (int r = 0; r < reqCount; r++) {
                if (isCount[r]) {
                  result.accumulateSingleStat(bucketTs, r, segLen, segLen);
                } else {
                  final double[] colData = decompressedCols[schemaColIndices[r]];
                  final double val = switch (requests.get(r).type()) {
                    case SUM, AVG -> ops.sum(colData, segStart, segLen);
                    case MIN -> ops.min(colData, segStart, segLen);
                    case MAX -> ops.max(colData, segStart, segLen);
                    case COUNT -> segLen;
                  };
                  result.accumulateSingleStat(bucketTs, r, val, segLen);
                }
              }

              segStart = segEnd;
            }
          }
        } else {
          // No bucket interval — accumulate all into one bucket
          for (int i = 0; i < tsCount; i++) {
            final long ts = timestamps[i];
            if (ts < fromTs || ts > toTs)
              continue;

            if (needRowTagFilter && !matchesTagConditions(tagCols, filterConditions, i))
              continue;

            for (int r = 0; r < reqCount; r++)
              rowValues[r] = isCount[r] ? 1.0 : decompressedCols[schemaColIndices[r]][i];

            result.accumulateRow(singleBucketTs, rowValues);
          }
        }
        if (metrics != null)
          metrics.addAccum(System.nanoTime() - t0);
      }
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Removes all blocks with maxTimestamp < threshold.
   */
  public void truncateBefore(final long timestamp) throws IOException {
    directoryLock.writeLock().lock();
    try {
      final List<BlockEntry> retained = new ArrayList<>();
      for (final BlockEntry entry : blockDirectory)
        if (entry.maxTimestamp >= timestamp)
          retained.add(entry);

      if (retained.size() == blockDirectory.size())
        return; // Nothing to truncate

      // Rewrite the file with only retained blocks
      final int colCount = columns.size();
      final String tempPath = basePath + ".ts.sealed.tmp";

      // Build new directory in a local list — do NOT modify blockDirectory until after the
      // atomic file swap. If Files.move() fails, the live file and blockDirectory remain intact.
      final List<BlockEntry> newDirectory = new ArrayList<>();
      try (final RandomAccessFile tempFile = new RandomAccessFile(tempPath, "rw")) {
        final ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
        headerBuf.putInt(MAGIC_VALUE);
        headerBuf.put((byte) CURRENT_VERSION);
        headerBuf.putShort((short) colCount);
        headerBuf.putInt(0);
        headerBuf.putLong(Long.MAX_VALUE);
        headerBuf.putLong(Long.MIN_VALUE);
        headerBuf.flip();
        tempFile.getChannel().write(headerBuf);

        for (final BlockEntry oldEntry : retained)
          copyBlockToFile(tempFile, oldEntry, colCount, newDirectory);
      }

      // Atomic file swap: close handles first (required on Windows), then atomically replace.
      // If the move fails, reopen the original file so the store remains usable.
      indexChannel.close();
      indexFile.close();

      final File oldFile = new File(basePath + ".ts.sealed");
      final File tmpFile = new File(tempPath);
      try {
        Files.move(tmpFile.toPath(), oldFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final IOException moveEx) {
        // Move failed — original file is still in place; reopen handles so the store stays usable
        try {
          indexFile = new RandomAccessFile(oldFile, "rw");
          indexChannel = indexFile.getChannel();
        } catch (final IOException reopenEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to reopen sealed store after failed atomic move: %s", reopenEx, oldFile.getAbsolutePath());
        }
        throw moveEx;
      }

      // Only update in-memory state after the successful file swap
      blockDirectory.clear();
      blockDirectory.addAll(newDirectory);
      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;
      for (final BlockEntry e : blockDirectory) {
        if (e.minTimestamp < globalMinTs) globalMinTs = e.minTimestamp;
        if (e.maxTimestamp > globalMaxTs) globalMaxTs = e.maxTimestamp;
      }

      indexFile = new RandomAccessFile(oldFile, "rw");
      indexChannel = indexFile.getChannel();
      rewriteHeader();
    } finally {
      directoryLock.writeLock().unlock();
    }
  }

  /**
   * Downsamples blocks older than cutoffTs to the given granularity.
   * Blocks already at the target resolution or coarser are left untouched (idempotency).
   * Numeric fields are averaged per (bucketTs, tagKey) group; tag columns preserved.
   */
  public void downsampleBlocks(final long cutoffTs, final long granularityMs,
      final int tsColIdx, final List<Integer> tagColIndices, final List<Integer> numericColIndices) throws IOException {
    directoryLock.writeLock().lock();
    try {

    final List<BlockEntry> toDownsample = new ArrayList<>();
    final List<BlockEntry> toKeep = new ArrayList<>();

    for (final BlockEntry entry : blockDirectory) {
      if (entry.maxTimestamp >= cutoffTs) {
        toKeep.add(entry);
        continue;
      }
      // Check if block is already at target resolution and must be left untouched (idempotency).
      // 1. A single-sample block can never be reduced further.
      // 2. A block already downsampled to this granularity (or coarser) is skipped via its marker. This is
      //    the authoritative check: it also covers zero-span blocks (maxTs == minTs) produced when a prior
      //    downsampling collapsed every tag-group into a single bucket - those have an average spacing of 0
      //    and would otherwise be re-selected on every maintenance cycle (issue #4599).
      // 3. Fallback density heuristic for raw blocks: if the average sample spacing already meets the target
      //    granularity, downsampling would not reduce the block, so skip it. Expressed as a multiplication to
      //    avoid integer-division truncation at the boundary.
      if (entry.sampleCount <= 1
          || entry.downsampledGranularityMs >= granularityMs
          || (entry.maxTimestamp - entry.minTimestamp) >= granularityMs * (long) (entry.sampleCount - 1)) {
        toKeep.add(entry);
        continue;
      }
      toDownsample.add(entry);
    }

    if (toDownsample.isEmpty())
      return;

    // Decompress all qualifying blocks and aggregate per (bucketTs, tagKey)
    // Use List<String> as map key (not null-byte-joined String) since tag values may contain null bytes.
    final Map<List<String>, Map<Long, double[]>> groupedData = new HashMap<>(); // tagKey -> (bucketTs -> [sum0, count0, sum1, count1, ...])
    final int numFields = numericColIndices.size();
    final int accSize = numFields * 2; // sum + count per numeric field

    for (final BlockEntry entry : toDownsample) {
      final long[] timestamps = decompressTimestamps(entry, tsColIdx);

      // Decompress tag columns
      final Object[][] tagData = new Object[tagColIndices.size()][];
      for (int t = 0; t < tagColIndices.size(); t++) {
        final int ci = tagColIndices.get(t);
        final byte[] compressed = readBytes(entry.columnOffsets[ci], entry.columnSizes[ci]);
        tagData[t] = switch (columns.get(ci).getCompressionHint()) {
          case DICTIONARY -> {
            final String[] vals = DictionaryCodec.decode(compressed);
            final Object[] boxed = new Object[vals.length];
            System.arraycopy(vals, 0, boxed, 0, vals.length);
            yield boxed;
          }
          default -> new Object[entry.sampleCount];
        };
      }

      // Decompress numeric columns
      final double[][] numData = new double[numFields][];
      for (int n = 0; n < numFields; n++) {
        final int ci = numericColIndices.get(n);
        numData[n] = decompressDoubleColumn(entry, ci);
      }

      // Group samples by (tagValues list, bucketTs)
      for (int i = 0; i < timestamps.length; i++) {
        final long bucketTs = Math.floorDiv(timestamps[i], granularityMs) * granularityMs;

        // Build tag key as List<String> to avoid ambiguity with null bytes in tag values
        final List<String> tagKey = new ArrayList<>(tagData.length);
        for (final Object[] tagCol : tagData)
          tagKey.add(tagCol[i] != null ? tagCol[i].toString() : "");

        final Map<Long, double[]> buckets = groupedData.computeIfAbsent(tagKey, k -> new HashMap<>());
        final double[] acc = buckets.computeIfAbsent(bucketTs, k -> new double[accSize]);
        for (int n = 0; n < numFields; n++) {
          acc[n * 2] += numData[n][i];       // sum
          acc[n * 2 + 1] += 1.0;             // count
        }
      }
    }

    // Build new downsampled samples from grouped data
    final List<Object[]> newSamples = new ArrayList<>();
    for (final Map.Entry<List<String>, Map<Long, double[]>> tagEntry : groupedData.entrySet()) {
      final List<String> tagParts = tagEntry.getKey();
      for (final Map.Entry<Long, double[]> bucketEntry : tagEntry.getValue().entrySet()) {
        final long bucketTs = bucketEntry.getKey();
        final double[] acc = bucketEntry.getValue();

        // Build a full row: [timestamp, tag0, tag1, ..., field0, field1, ...]
        // ordered by column index
        final Object[] row = new Object[columns.size()];
        row[tsColIdx] = bucketTs;
        for (int t = 0; t < tagColIndices.size(); t++)
          row[tagColIndices.get(t)] = t < tagParts.size() ? tagParts.get(t) : "";
        for (int n = 0; n < numFields; n++) {
          final double count = acc[n * 2 + 1];
          row[numericColIndices.get(n)] = count > 0 ? acc[n * 2] / count : 0.0;
        }
        newSamples.add(row);
      }
    }

    // Sort by timestamp
    newSamples.sort(Comparator.comparingLong(row -> (long) row[tsColIdx]));

    // Build new sealed blocks from downsampled data
    final int colCount = columns.size();
    final List<byte[][]> newBlocksCompressed = new ArrayList<>();
    final List<long[]> newBlocksMeta = new ArrayList<>(); // [minTs, maxTs, sampleCount]
    final List<double[]> newBlocksMins = new ArrayList<>();
    final List<double[]> newBlocksMaxs = new ArrayList<>();
    final List<double[]> newBlocksSums = new ArrayList<>();
    final List<String[][]> newBlocksTagDV = new ArrayList<>();

    int chunkStart = 0;
    while (chunkStart < newSamples.size()) {
      final int chunkEnd = Math.min(chunkStart + MAX_BLOCK_SIZE, newSamples.size());
      final int chunkLen = chunkEnd - chunkStart;

      // Extract timestamps for this chunk
      final long[] chunkTs = new long[chunkLen];
      for (int i = 0; i < chunkLen; i++)
        chunkTs[i] = (long) newSamples.get(chunkStart + i)[tsColIdx];

      // Per-column stats
      final double[] mins = new double[colCount];
      final double[] maxs = new double[colCount];
      final double[] sums = new double[colCount];
      Arrays.fill(mins, Double.NaN);
      Arrays.fill(maxs, Double.NaN);

      final byte[][] compressedCols = new byte[colCount][];
      for (int c = 0; c < colCount; c++) {
        if (c == tsColIdx) {
          compressedCols[c] = DeltaOfDeltaCodec.encode(chunkTs);
        } else {
          final Object[] chunkValues = new Object[chunkLen];
          for (int i = 0; i < chunkLen; i++)
            chunkValues[i] = newSamples.get(chunkStart + i)[c];
          compressedCols[c] = compressColumn(columns.get(c), chunkValues);

          // Compute stats for numeric columns
          final TimeSeriesCodec codec = columns.get(c).getCompressionHint();
          if (codec == TimeSeriesCodec.GORILLA_XOR || codec == TimeSeriesCodec.SIMPLE8B) {
            final double[] stats = reduceNumericStats(chunkValues);
            mins[c] = stats[0];
            maxs[c] = stats[1];
            sums[c] = stats[2];
          }
        }
      }

      // Collect distinct tag values for this chunk
      final String[][] chunkTagDV = new String[colCount][];
      for (int c = 0; c < colCount; c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG) {
          final LinkedHashSet<String> distinctSet = new LinkedHashSet<>();
          for (int i = chunkStart; i < chunkEnd; i++) {
            final Object val = newSamples.get(i)[c];
            distinctSet.add(val != null ? val.toString() : "");
          }
          chunkTagDV[c] = distinctSet.toArray(new String[0]);
        }
      }

      newBlocksCompressed.add(compressedCols);
      newBlocksMeta.add(new long[] { chunkTs[0], chunkTs[chunkLen - 1], chunkLen });
      newBlocksMins.add(mins);
      newBlocksMaxs.add(maxs);
      newBlocksSums.add(sums);
      newBlocksTagDV.add(chunkTagDV);
      chunkStart = chunkEnd;
    }

    // Rewrite sealed file: toKeep blocks (raw copy) + new downsampled blocks
    downsampleRewriteCount++;
    rewriteWithBlocks(toKeep, newBlocksCompressed, newBlocksMeta, newBlocksMins, newBlocksMaxs, newBlocksSums,
        newBlocksTagDV, granularityMs);
    } finally {
      directoryLock.writeLock().unlock();
    }
  }

  /**
   * Rewrites the sealed file, copying retained blocks as raw bytes and appending new blocks.
   * Blocks are written in ascending minTimestamp order so that the on-disk layout matches
   * the in-memory block directory, preserving binary search correctness after a restart.
   * Uses atomic tmp-file rename.
   */
  private void rewriteWithBlocks(final List<BlockEntry> retained,
      final List<byte[][]> newCompressed, final List<long[]> newMeta,
      final List<double[]> newMins, final List<double[]> newMaxs, final List<double[]> newSums,
      final List<String[][]> newTagDistinctValues, final long newBlocksGranularityMs) throws IOException {

    final int colCount = columns.size();
    final String tempPath = basePath + ".ts.sealed.tmp";

    // Build a merged, minTimestamp-sorted write plan so that the on-disk layout is
    // always in ascending order (required by binary search in iterateRange/scanRange).
    // A negative index means a "new" (downsampled) block; non-negative means retained.
    record WriteSpec(long minTs, boolean retained, int idx) {}
    final List<WriteSpec> writeOrder = new ArrayList<>(retained.size() + newCompressed.size());
    for (int i = 0; i < retained.size(); i++)
      writeOrder.add(new WriteSpec(retained.get(i).minTimestamp, true, i));
    for (int b = 0; b < newCompressed.size(); b++)
      writeOrder.add(new WriteSpec(newMeta.get(b)[0], false, b));
    writeOrder.sort(Comparator.comparingLong(WriteSpec::minTs));

    // Build new directory in a local list — do NOT modify blockDirectory until after the
    // atomic file swap. If Files.move() fails, the live file and blockDirectory remain intact.
    final List<BlockEntry> newDirectory = new ArrayList<>();
    try (final RandomAccessFile tempFile = new RandomAccessFile(tempPath, "rw")) {
      tempFile.setLength(0);
      // Write placeholder header
      final ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
      headerBuf.putInt(MAGIC_VALUE);
      headerBuf.put((byte) CURRENT_VERSION);
      headerBuf.putShort((short) colCount);
      headerBuf.putInt(0);
      headerBuf.putLong(Long.MAX_VALUE);
      headerBuf.putLong(Long.MIN_VALUE);
      headerBuf.flip();
      tempFile.getChannel().write(headerBuf);

      // Write all blocks in ascending minTimestamp order
      for (final WriteSpec spec : writeOrder) {
        if (spec.retained()) {
          copyBlockToFile(tempFile, retained.get(spec.idx()), colCount, newDirectory);
        } else {
          final int b = spec.idx();
          final long[] meta = newMeta.get(b);
          final BlockEntry entry = writeNewBlockToFile(tempFile, (int) meta[2], meta[0], meta[1],
              newCompressed.get(b), newMins.get(b), newMaxs.get(b), newSums.get(b), colCount,
              newTagDistinctValues != null ? newTagDistinctValues.get(b) : null);
          // Mark the freshly downsampled block so future cycles at the same (or finer) granularity skip it.
          entry.downsampledGranularityMs = newBlocksGranularityMs;
          newDirectory.add(entry);
        }
      }
    }

    // Atomic file swap: close handles first (required on Windows), then atomically replace.
    // If the move fails, reopen the original file so the store remains usable.
    indexChannel.close();
    indexFile.close();

    final File oldFile = new File(basePath + ".ts.sealed");
    final File tmpFile = new File(tempPath);
    try {
      Files.move(tmpFile.toPath(), oldFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (final IOException moveEx) {
      // Move failed — original file is still in place; reopen handles so the store stays usable
      try {
        indexFile = new RandomAccessFile(oldFile, "rw");
        indexChannel = indexFile.getChannel();
      } catch (final IOException reopenEx) {
        LogManager.instance().log(this, Level.SEVERE,
            "Failed to reopen sealed store after failed atomic move: %s", reopenEx, oldFile.getAbsolutePath());
      }
      throw moveEx;
    }

    // Only update in-memory state after the successful file swap
    blockDirectory.clear();
    blockDirectory.addAll(newDirectory);
    globalMinTs = Long.MAX_VALUE;
    globalMaxTs = Long.MIN_VALUE;
    for (final BlockEntry e : blockDirectory) {
      if (e.minTimestamp < globalMinTs) globalMinTs = e.minTimestamp;
      if (e.maxTimestamp > globalMaxTs) globalMaxTs = e.maxTimestamp;
    }

    indexFile = new RandomAccessFile(oldFile, "rw");
    indexChannel = indexFile.getChannel();
    rewriteHeader();
  }

  private void copyBlockToFile(final RandomAccessFile tempFile, final BlockEntry oldEntry, final int colCount,
      final List<BlockEntry> target) throws IOException {
    final byte[][] compressedCols = new byte[colCount][];
    for (int c = 0; c < colCount; c++)
      compressedCols[c] = readBytes(oldEntry.columnOffsets[c], oldEntry.columnSizes[c]);

    final BlockEntry newEntry = writeNewBlockToFile(tempFile, oldEntry.sampleCount, oldEntry.minTimestamp,
        oldEntry.maxTimestamp, compressedCols, oldEntry.columnMins, oldEntry.columnMaxs, oldEntry.columnSums,
        colCount, oldEntry.tagDistinctValues);
    // Preserve the in-memory downsampling marker across the file rewrite so idempotency survives compaction.
    newEntry.downsampledGranularityMs = oldEntry.downsampledGranularityMs;
    target.add(newEntry);
  }

  /**
   * Writes a single block to a temp file and returns the resulting {@link BlockEntry}.
   * Does NOT modify {@link #blockDirectory} or the global min/max timestamps —
   * callers are responsible for those updates.
   */
  private BlockEntry writeNewBlockToFile(final RandomAccessFile tempFile, final int sampleCount,
      final long minTs, final long maxTs, final byte[][] compressedCols,
      final double[] columnMins, final double[] columnMaxs, final double[] columnSums,
      final int colCount, final String[][] tagDistinctValues) throws IOException {

    int numericColCount = 0;
    for (int c = 0; c < colCount; c++)
      if (!Double.isNaN(columnMins[c]))
        numericColCount++;

    final byte[] tagMeta = buildTagMetadata(tagDistinctValues, colCount);

    final int statsSize = 4 + (8 + 8 + 8) * numericColCount;
    final int metaSize = 4 + 8 + 8 + 4 + 4 * colCount + statsSize + tagMeta.length;
    final ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
    metaBuf.putInt(BLOCK_MAGIC_VALUE);
    metaBuf.putLong(minTs);
    metaBuf.putLong(maxTs);
    metaBuf.putInt(sampleCount);
    for (final byte[] col : compressedCols)
      metaBuf.putInt(col.length);
    metaBuf.putInt(numericColCount);
    for (int c = 0; c < colCount; c++) {
      if (!Double.isNaN(columnMins[c])) {
        metaBuf.putDouble(columnMins[c]);
        metaBuf.putDouble(columnMaxs[c]);
        metaBuf.putDouble(columnSums[c]);
      }
    }
    metaBuf.put(tagMeta);
    metaBuf.flip();

    // Use metaBuf.limit() (not .array().length) since the backing array may be larger after flip
    final CRC32 crc = new CRC32();
    crc.update(metaBuf.array(), 0, metaBuf.limit());

    final long blockStart = tempFile.length();
    long dataOffset = blockStart;
    tempFile.seek(dataOffset);
    tempFile.write(metaBuf.array(), 0, metaBuf.limit());
    dataOffset += metaSize;

    // #6360 item 3: the temp file becomes the live file by an atomic move, so an offset into it is the offset the
    // block will have - exactly as the column offsets set below already are.
    final BlockEntry newEntry = new BlockEntry(minTs, maxTs, sampleCount, colCount, columnMins, columnMaxs, columnSums,
        blockStart);
    newEntry.tagDistinctValues = tagDistinctValues;
    for (int c = 0; c < colCount; c++) {
      newEntry.columnOffsets[c] = dataOffset;
      newEntry.columnSizes[c] = compressedCols[c].length;
      crc.update(compressedCols[c]);
      tempFile.write(compressedCols[c]);
      dataOffset += compressedCols[c].length;
    }

    newEntry.recordWrittenCRC((int) crc.getValue());
    final ByteBuffer crcBuf = ByteBuffer.allocate(4);
    crcBuf.putInt(newEntry.storedCRC);
    crcBuf.flip();
    tempFile.write(crcBuf.array());

    return newEntry;
  }

  /**
   * Lock-free phase of compaction: snapshots existing sealed blocks (under a brief read lock),
   * then writes all of them plus the new compressed blocks to {@code .ts.sealed.tmp} — entirely
   * without holding any lock.
   * <p>
   * Call {@link #commitTempCompactionFile(List)} under the caller's write lock to atomically
   * swap the temp file for the live sealed file and install the returned block directory.
   *
   * @param newCompressed      compressed column bytes for each new block
   * @param newMeta            {@code [minTs, maxTs, sampleCount]} for each new block
   * @param newMins            per-column min stats for each new block
   * @param newMaxs            per-column max stats for each new block
   * @param newSums            per-column sum stats for each new block
   * @param newTagDistinctValues tag metadata for each new block (may be null)
   *
   * @return the new {@link BlockEntry} list to pass to {@link #commitTempCompactionFile(List)}
   */
  List<BlockEntry> writeTempCompactionFile(
      final List<byte[][]> newCompressed, final List<long[]> newMeta,
      final List<double[]> newMins, final List<double[]> newMaxs, final List<double[]> newSums,
      final List<String[][]> newTagDistinctValues) throws IOException {

    final int colCount = columns.size();
    final String tempPath = basePath + ".ts.sealed.tmp";

    // Snapshot the current block list and pre-read all retained block bytes under the read lock.
    // This guards against concurrent truncateBefore / downsampleBlocks closing the channel.
    final List<BlockEntry> retained;
    final List<byte[][]> retainedBytes;
    directoryLock.readLock().lock();
    try {
      retained = new ArrayList<>(blockDirectory);
      retainedBytes = new ArrayList<>(retained.size());
      for (final BlockEntry e : retained) {
        final byte[][] cols = new byte[colCount][];
        for (int c = 0; c < colCount; c++)
          cols[c] = readBytes(e.columnOffsets[c], e.columnSizes[c]);
        retainedBytes.add(cols);
      }
    } finally {
      directoryLock.readLock().unlock();
    }

    // Build merged, minTimestamp-sorted write plan (same ordering as rewriteWithBlocks).
    record WriteSpec(long minTs, boolean isRetained, int idx) {}
    final List<WriteSpec> writeOrder = new ArrayList<>(retained.size() + newCompressed.size());
    for (int i = 0; i < retained.size(); i++)
      writeOrder.add(new WriteSpec(retained.get(i).minTimestamp, true, i));
    for (int b = 0; b < newCompressed.size(); b++)
      writeOrder.add(new WriteSpec(newMeta.get(b)[0], false, b));
    writeOrder.sort(Comparator.comparingLong(WriteSpec::minTs));

    final List<BlockEntry> newDirectory = new ArrayList<>(writeOrder.size());

    // Write placeholder header + all blocks to the temp file (no lock held).
    // Truncate first so any leftover bytes from a previous partial write are cleared.
    try (final RandomAccessFile tempFile = new RandomAccessFile(tempPath, "rw")) {
      tempFile.setLength(0);
      final ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
      headerBuf.putInt(MAGIC_VALUE);
      headerBuf.put((byte) CURRENT_VERSION);
      headerBuf.putShort((short) colCount);
      headerBuf.putInt(0);
      headerBuf.putLong(Long.MAX_VALUE);
      headerBuf.putLong(Long.MIN_VALUE);
      headerBuf.flip();
      tempFile.getChannel().write(headerBuf);

      for (final WriteSpec spec : writeOrder) {
        final BlockEntry entry;
        if (spec.isRetained()) {
          final int i = spec.idx();
          final BlockEntry old = retained.get(i);
          entry = writeNewBlockToFile(tempFile, old.sampleCount, old.minTimestamp, old.maxTimestamp,
              retainedBytes.get(i), old.columnMins, old.columnMaxs, old.columnSums, colCount, old.tagDistinctValues);
          // Preserve the in-memory downsampling marker across compaction's file rewrite.
          entry.downsampledGranularityMs = old.downsampledGranularityMs;
        } else {
          final int b = spec.idx();
          final long[] meta = newMeta.get(b);
          entry = writeNewBlockToFile(tempFile, (int) meta[2], meta[0], meta[1],
              newCompressed.get(b), newMins.get(b), newMaxs.get(b), newSums.get(b), colCount,
              newTagDistinctValues != null ? newTagDistinctValues.get(b) : null);
        }
        newDirectory.add(entry);
      }
    }

    return newDirectory;
  }

  /**
   * Completes the compaction by atomically swapping {@code .ts.sealed.tmp} for the live
   * {@code .ts.sealed} file and installing the given block directory.
   * <p>
   * Must be called while the caller holds its own write lock (e.g.
   * {@code compactionLock.writeLock()} in {@link TimeSeriesShard}) to prevent concurrent
   * queries from reading the sealed store while the channel is being replaced.
   * This method also acquires {@link #directoryLock} internally for the in-memory updates.
   *
   * @param newBlockDirectory the block entries returned by {@link #writeTempCompactionFile}
   */
  void commitTempCompactionFile(final List<BlockEntry> newBlockDirectory) throws IOException {
    directoryLock.writeLock().lock();
    try {
      // Atomic file swap: close handles first (required on Windows), then atomically replace.
      // If the move fails, reopen the original file so the store remains usable.
      indexChannel.close();
      indexFile.close();

      final File sealedFile = new File(basePath + ".ts.sealed");
      final File tmpFile = new File(basePath + ".ts.sealed.tmp");
      try {
        Files.move(tmpFile.toPath(), sealedFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final IOException moveEx) {
        // Move failed — original file is still in place; reopen handles so the store stays usable
        try {
          indexFile = new RandomAccessFile(sealedFile, "rw");
          indexChannel = indexFile.getChannel();
        } catch (final IOException reopenEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to reopen sealed store after failed atomic move: %s", reopenEx, sealedFile.getAbsolutePath());
        }
        throw moveEx;
      }

      indexFile = new RandomAccessFile(sealedFile, "rw");
      indexChannel = indexFile.getChannel();

      blockDirectory.clear();
      blockDirectory.addAll(newBlockDirectory);

      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;
      for (final BlockEntry e : blockDirectory) {
        if (e.minTimestamp < globalMinTs)
          globalMinTs = e.minTimestamp;
        if (e.maxTimestamp > globalMaxTs)
          globalMaxTs = e.maxTimestamp;
      }

      rewriteHeader();
    } finally {
      directoryLock.writeLock().unlock();
    }
  }

  /**
   * Appends additional blocks to the existing {@code .ts.sealed.tmp} file that was
   * already written by {@link #writeTempCompactionFile}.
   * <p>
   * Used by Phase 4 of lock-free compaction (called under the caller's write lock) to
   * include the partial last page's data that was read after the lock was acquired.
   * Since the partial page is small (≤ one page's worth of samples), this is fast.
   *
   * @param newCompressed   compressed column bytes for each additional block
   * @param newMeta         {@code [minTs, maxTs, sampleCount]} for each additional block
   * @param newMins         per-column min stats for each block
   * @param newMaxs         per-column max stats for each block
   * @param newSums         per-column sum stats for each block
   * @param newTagDV        tag metadata for each block (may be null)
   * @param directory       block directory from {@link #writeTempCompactionFile}; new
   *                        entries are appended in-place
   */
  void appendBlocksToTempFile(
      final List<byte[][]> newCompressed, final List<long[]> newMeta,
      final List<double[]> newMins, final List<double[]> newMaxs, final List<double[]> newSums,
      final List<String[][]> newTagDV,
      final List<BlockEntry> directory) throws IOException {
    if (newCompressed.isEmpty())
      return;

    final String tempPath = basePath + ".ts.sealed.tmp";
    final int colCount = columns.size();

    try (final RandomAccessFile tempFile = new RandomAccessFile(tempPath, "rw")) {
      // Read the current header to get existing block count and global min/max timestamps
      final ByteBuffer hdrBuf = ByteBuffer.allocate(HEADER_SIZE);
      tempFile.seek(0);
      tempFile.getChannel().read(hdrBuf);
      hdrBuf.flip();
      hdrBuf.position(7); // skip magic(4) + version(1) + colCount(2)
      final int existingBlockCount = hdrBuf.getInt();
      long curGlobalMin = hdrBuf.getLong();
      long curGlobalMax = hdrBuf.getLong();

      // Append each new block; writeNewBlockToFile always seeks to tempFile.length()
      for (int b = 0; b < newCompressed.size(); b++) {
        final long[] meta = newMeta.get(b);
        final BlockEntry entry = writeNewBlockToFile(tempFile, (int) meta[2], meta[0], meta[1],
            newCompressed.get(b), newMins.get(b), newMaxs.get(b), newSums.get(b), colCount,
            newTagDV != null ? newTagDV.get(b) : null);
        directory.add(entry);
        if (meta[0] < curGlobalMin)
          curGlobalMin = meta[0];
        if (meta[1] > curGlobalMax)
          curGlobalMax = meta[1];
      }

      // Update the header: block count (offset 7) and global min/max (offsets 11 and 19)
      final ByteBuffer updateBuf = ByteBuffer.allocate(4 + 8 + 8);
      updateBuf.putInt(existingBlockCount + newCompressed.size());
      updateBuf.putLong(curGlobalMin);
      updateBuf.putLong(curGlobalMax);
      updateBuf.flip();
      tempFile.getChannel().write(updateBuf, 7);
    }
  }

  /**
   * Deletes the temp compaction file ({@code .ts.sealed.tmp}) if it exists.
   * Called from error-recovery paths to leave a clean state.
   */
  void deleteTempFileIfExists() {
    final File tmp = new File(basePath + ".ts.sealed.tmp");
    if (tmp.exists() && !tmp.delete())
      LogManager.instance().log(this, Level.WARNING,
          "Failed to delete stale compaction temp file '%s'; next compaction may fail or use stale data",
          null, tmp.getAbsolutePath());
  }

  static byte[] compressColumn(final ColumnDefinition col, final Object[] values) {
    final TimeSeriesCodec codec = col.getCompressionHint();
    return switch (codec) {
      case GORILLA_XOR -> {
        final double[] doubles = new double[values.length];
        for (int i = 0; i < values.length; i++)
          doubles[i] = ColumnDefinition.numericValueOf(values[i]);
        yield GorillaXORCodec.encode(doubles);
      }
      case SIMPLE8B -> {
        final long[] longs = new long[values.length];
        for (int i = 0; i < values.length; i++)
          longs[i] = ColumnDefinition.integerValueOf(values[i]);
        yield Simple8bCodec.encode(longs);
      }
      case DICTIONARY -> {
        final String[] strings = new String[values.length];
        for (int i = 0; i < values.length; i++)
          strings[i] = values[i] != null ? values[i].toString() : "";
        yield DictionaryCodec.encode(strings);
      }
      default -> throw new IllegalStateException("Unknown compression codec: " + codec);
    };
  }

  /**
   * Truncates the sealed store to exactly {@code targetBlockCount} blocks,
   * removing any blocks appended after the watermark during an interrupted compaction.
   */
  public void truncateToBlockCount(final long targetBlockCount) throws IOException {
    directoryLock.writeLock().lock();
    try {
      if (targetBlockCount >= blockDirectory.size())
        return; // nothing to truncate

      final List<BlockEntry> retained = new ArrayList<>(blockDirectory.subList(0, (int) targetBlockCount));
      final int colCount = columns.size();
      final String tempPath = basePath + ".ts.sealed.tmp";

      // Build new directory in a local list — do NOT modify blockDirectory until after the
      // atomic file swap. If Files.move() fails, the live file and blockDirectory remain intact.
      final List<BlockEntry> newDirectory = new ArrayList<>();
      try (final RandomAccessFile tempFile = new RandomAccessFile(tempPath, "rw")) {
        tempFile.setLength(0);
        final ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
        headerBuf.putInt(MAGIC_VALUE);
        headerBuf.put((byte) CURRENT_VERSION);
        headerBuf.putShort((short) colCount);
        headerBuf.putInt(0);
        headerBuf.putLong(Long.MAX_VALUE);
        headerBuf.putLong(Long.MIN_VALUE);
        headerBuf.flip();
        tempFile.getChannel().write(headerBuf);

        for (final BlockEntry entry : retained)
          copyBlockToFile(tempFile, entry, colCount, newDirectory);
      }

      // Atomic file swap: close handles first (required on Windows), then atomically replace.
      // If the move fails, reopen the original file so the store remains usable.
      indexChannel.close();
      indexFile.close();

      final File oldFile = new File(basePath + ".ts.sealed");
      final File tmpFile = new File(tempPath);
      try {
        Files.move(tmpFile.toPath(), oldFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final IOException moveEx) {
        // Move failed — original file is still in place; reopen handles so the store stays usable
        try {
          indexFile = new RandomAccessFile(oldFile, "rw");
          indexChannel = indexFile.getChannel();
        } catch (final IOException reopenEx) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to reopen sealed store after failed atomic move: %s", reopenEx, oldFile.getAbsolutePath());
        }
        throw moveEx;
      }

      // Only update in-memory state after the successful file swap
      blockDirectory.clear();
      blockDirectory.addAll(newDirectory);
      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;
      for (final BlockEntry e : blockDirectory) {
        if (e.minTimestamp < globalMinTs) globalMinTs = e.minTimestamp;
        if (e.maxTimestamp > globalMaxTs) globalMaxTs = e.maxTimestamp;
      }

      indexFile = new RandomAccessFile(oldFile, "rw");
      indexChannel = indexFile.getChannel();
      rewriteHeader();
    } finally {
      directoryLock.writeLock().unlock();
    }
  }

  /**
   * Clears the "this block's CRC has already been checked" flag on every entry, so the next read of each block
   * validates it against the entry's OWN {@code blockStartOffset} and {@code storedCRC} instead of short-circuiting
   * on the flag.
   * <p>
   * Package-private, and it exists for the #6360 regression test. Those two fields used to be assigned by
   * {@link #loadDirectory()} alone, so every entry this process wrote held zero in both; the constructor pre-setting
   * the flag on a newly written block is exactly what kept that from ever being read, and clearing the flag is the
   * only way to make a test look at what the flag was hiding.
   */
  void clearCRCValidationCache() {
    directoryLock.readLock().lock();
    try {
      for (final BlockEntry entry : blockDirectory)
        entry.crcValidated = false;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  public int getBlockCount() {
    directoryLock.readLock().lock();
    try {
      return blockDirectory.size();
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Validates everything this format asserts about itself and returns one line per problem, empty when there is
   * none, plus one line per repair actually applied (issue #6340, extended by #6360).
   * <p>
   * <b>This is what {@code CHECK DATABASE} had no way to look at.</b> The checker walks record buckets and indexes;
   * a sealed store is neither, so until now the whole of a TimeSeries type's compacted data - which is most of its
   * data - was outside the reach of the tool whose job is to answer "is this database intact".
   * <p>
   * <b>The default tier reads every byte, and that is deliberate.</b> Every block carries a CRC32 over its metadata
   * and its compressed columns, and it is checked lazily on the first read of that block, so a block never read is
   * a block never verified. Doing it here reads the whole file once, sequentially. #6360 item 2 asked whether that
   * should move behind an opt-in clause and the answer is no: it is the same cost class as the record scan
   * {@code checkBuckets} already runs over every bucket of every document type, {@code CHECK DATABASE} is an
   * explicitly-invoked maintenance operation, and a tier that skipped it would answer "clean" having read only the
   * directory - which is the misleading-clean-result failure mode this whole check exists to end.
   * <p>
   * <b>{@link TimeSeriesIntegrity.Options#deep()} is the tier above it</b>, and what it adds is not "more of the
   * same": a CRC says the bytes are the bytes that were written, and says nothing about whether they mean what the
   * block claims they mean. Three things every read path takes on trust are checked only there - sorted
   * timestamps, the declared per-column statistics, and the declared distinct tag values. See
   * {@link #checkBlockContent}.
   * <p>
   * <b>What {@code FIX} may do here</b>, which #6360 item 1 asked and this answers: repair DERIVED bookkeeping and
   * nothing else. Two things qualify and both are lossless. The header's block count and global timestamp bounds
   * are recomputable from the block directory, and a wrong global bound is not cosmetic - {@link #loadDirectory}
   * reads those two straight out of the header rather than recomputing them, so a query pruned against them can
   * silently miss data this file holds. And bytes trailing the last readable block are the tail of a write that
   * did not complete, and dropping them matters more than it sounds: {@code appendBlock} writes at the END of the
   * file, so a tail nothing can read makes every block appended after it unreadable too. Only a tail that STARTS
   * with a block magic is dropped - that one is a block {@link #loadDirectory} recognised and could not read to
   * the end of, so it is incomplete by its own evidence; a tail that does not could equally be a complete block
   * whose magic took a bit flip, and that one is reported and left where a hex editor can still reach it. A block
   * that fails its CRC or its content check is NEVER discarded, repacked or rewritten either - those blocks are
   * the only copy of the samples in them, so choosing which to throw away is an operator's decision and not a
   * checker's.
   * <p>
   * The on-disk header is compared against the scanned directory only when it is not dirty. {@code appendBlock}
   * marks it dirty and {@link #flushHeader} rewrites it, so between the two the header legitimately under-reports
   * the blocks the file already holds and comparing them would report a healthy store as damaged.
   */
  public TimeSeriesIntegrity.Outcome checkIntegrity(final TimeSeriesIntegrity.Options options) throws IOException {
    final List<String> problems = new ArrayList<>();
    final List<String> repairs = new ArrayList<>();

    // A repairing run takes the WRITE lock for the whole pass rather than upgrading at the end: the two repairs
    // below rewrite the header and truncate the file, and both are only correct with respect to the very walk that
    // decided to make them, so an append landing between the two would invalidate the decision. A reporting run
    // keeps the read lock it always had - every path that mutates this file takes the write lock, so the read lock
    // alone already excludes all of them.
    final Lock lock = options.fix() ? directoryLock.writeLock() : directoryLock.readLock();
    lock.lock();
    try {
      final long fileLength = indexFile.length();
      if (fileLength < HEADER_SIZE) {
        problems.add("the file is " + fileLength + " bytes, shorter than its " + HEADER_SIZE + "-byte header");
        return new TimeSeriesIntegrity.Outcome(problems, repairs);
      }

      // Through readBytes() rather than a bare indexChannel.read(): a FileChannel may legally return fewer bytes
      // than asked for - not merely theoretical on a network filesystem - and the buffer's untouched tail is
      // zeros. Anywhere else that costs a retry; HERE it would turn "the read came up short" into a header or a
      // CRC that does not match, which is a false accusation of corruption made by the very code whose job is to
      // tell corruption from health. readBytes() loops and throws at a real end of file.
      final ByteBuffer headerBuf = ByteBuffer.wrap(readBytes(0, HEADER_SIZE));

      final int magic = headerBuf.getInt();
      if (magic != MAGIC_VALUE) {
        problems.add("the header does not start with the 'TSIX' magic (found 0x" + Integer.toHexString(magic)
            + "): the file is not a sealed store, or its first bytes were overwritten");
        return new TimeSeriesIntegrity.Outcome(problems, repairs);
      }

      final int version = headerBuf.get() & 0xFF;
      if (version != CURRENT_VERSION)
        problems.add("the header declares format version " + version + ", but this build writes and reads version "
            + CURRENT_VERSION);

      final int colCount = headerBuf.getShort() & 0xFFFF;
      if (colCount != columns.size())
        problems.add("the header declares " + colCount + " column(s) but the schema declares " + columns.size());

      final int storedBlockCount = headerBuf.getInt();
      final long storedMinTs = headerBuf.getLong();
      final long storedMaxTs = headerBuf.getLong();

      boolean headerDisagrees = false;
      if (!headerDirty && storedBlockCount != blockDirectory.size()) {
        problems.add("the header declares " + storedBlockCount + " block(s) but scanning the file found "
            + blockDirectory.size());
        headerDisagrees = true;
      }

      final int tsColIdx = options.deep() ? findTimestampColumnIndex() : -1;

      long computedMinTs = Long.MAX_VALUE;
      long computedMaxTs = Long.MIN_VALUE;
      // Where the next block's metadata must start. Blocks are written back to back from the header onwards, so
      // this is derived from the file rather than taken from the directory - which is also what makes the entry's
      // own blockStartOffset checkable against it just below.
      long blockStart = HEADER_SIZE;
      // Whether the walk reached the end of the directory. The two totals below and the trailing-bytes check are
      // both statements about ALL the blocks, so a walk that stopped at block N cannot make either of them: what
      // it accumulated is a prefix, and reporting a prefix as the whole would turn one finding into three.
      boolean walkedEveryBlock = true;

      for (int i = 0; i < blockDirectory.size(); i++) {
        final BlockEntry entry = blockDirectory.get(i);

        if (entry.sampleCount <= 0)
          problems.add("block " + i + " (offset " + blockStart + ") declares " + entry.sampleCount + " sample(s)");
        if (entry.minTimestamp > entry.maxTimestamp)
          problems.add("block " + i + " (offset " + blockStart + ") declares min timestamp " + entry.minTimestamp
              + " above its max timestamp " + entry.maxTimestamp);

        boolean sizesUsable = true;
        for (int c = 0; c < entry.columnSizes.length; c++)
          if (entry.columnSizes[c] < 0) {
            problems.add("block " + i + " (offset " + blockStart + ") declares a negative size " + entry.columnSizes[c]
                + " for column " + c);
            sizesUsable = false;
          }

        // The metadata sits between the block's start and its first column, so a first column at or before the
        // start means the directory and the file disagree about where this block is - and every offset after it
        // is then meaningless, which is why the walk stops rather than carrying on.
        if (sizesUsable && entry.columnOffsets[0] <= blockStart) {
          problems.add("block " + i + " places its first column at " + entry.columnOffsets[0]
              + ", at or before the " + blockStart + " where the block itself starts");
          sizesUsable = false;
        }

        if (!sizesUsable) {
          walkedEveryBlock = false;
          break;
        }

        final long endOfData =
            entry.columnOffsets[entry.columnSizes.length - 1] + entry.columnSizes[entry.columnSizes.length - 1];
        if (endOfData + 4 > fileLength) {
          problems.add("block " + i + " (offset " + blockStart + ") ends at " + (endOfData + 4)
              + ", past the end of a file of " + fileLength + " bytes: it was truncated");
          walkedEveryBlock = false;
          break;
        }

        if (entry.minTimestamp < computedMinTs)
          computedMinTs = entry.minTimestamp;
        if (entry.maxTimestamp > computedMaxTs)
          computedMaxTs = entry.maxTimestamp;

        // Recomputed from the FILE, not delegated to validateBlockCRC(): that one returns early for a block whose
        // flag is already set, which is right for a read (a block is immutable once written, so paying twice buys
        // nothing) and wrong here - a second CHECK DATABASE in the same process would answer "clean" without
        // having read a byte. Both sides therefore come from the file: the bytes from blockStart, and the CRC from
        // where the writer put it, immediately after the data.
        final int blockSize = (int) (endOfData - blockStart);
        final int storedCRC = ByteBuffer.wrap(readBytes(endOfData, 4)).getInt();
        final byte[] blockBytes = readBytes(blockStart, blockSize);

        final CRC32 crc = new CRC32();
        crc.update(blockBytes);
        final boolean crcMatches = (int) crc.getValue() == storedCRC;
        if (!crcMatches)
          problems.add("block " + i + ": CRC mismatch at offset " + blockStart + " (stored=0x"
              + Integer.toHexString(storedCRC) + ", computed=0x" + Integer.toHexString((int) crc.getValue())
              + "): its metadata or its compressed columns are not the bytes that were written");

        // Memory against disk (#6360 item 3). These two fields used to be filled in by loadDirectory alone, so an
        // entry this process wrote carried zero in both and every reader of them would have gone to offset 0 -
        // latent only because crcValidated short-circuits them. Now that every write path fills them in, the
        // comparison is meaningful for every entry, and a mismatch means the directory in memory would read a
        // different block than the one the file holds here.
        final boolean offsetsAgree = entry.blockStartOffset == blockStart;
        if (!offsetsAgree)
          problems.add("block " + i + ": the directory in memory places it at offset " + entry.blockStartOffset
              + " but the file has it at " + blockStart);
        if (crcMatches && entry.storedCRC != storedCRC)
          problems.add("block " + i + " (offset " + blockStart + "): the directory in memory holds CRC 0x"
              + Integer.toHexString(entry.storedCRC) + " but the file stores 0x" + Integer.toHexString(storedCRC));

        // Two preconditions, and skipping the decode when either fails is what keeps ONE fault reading as one
        // finding. A block that already failed its CRC decodes to noise, every line of it a consequence of the
        // failure already reported. And a directory that disagrees about where the block STARTS disagrees about
        // where its columns are - checkBlockContent slices them at entry.columnOffsets[c] minus this blockStart -
        // so it would report wrong statistics, or an undecodable block, for a block whose real problem is the
        // offset mismatch reported two lines above.
        if (options.deep() && crcMatches && offsetsAgree)
          checkBlockContent(i, entry, blockStart, blockBytes, tsColIdx, problems);

        blockStart = endOfData + 4;
      }

      boolean boundsDisagree = false;
      if (walkedEveryBlock && !headerDirty && !blockDirectory.isEmpty()) {
        if (storedMinTs != computedMinTs) {
          problems.add("the header declares global min timestamp " + storedMinTs + " but the blocks hold "
              + computedMinTs);
          boundsDisagree = true;
        }
        if (storedMaxTs != computedMaxTs) {
          problems.add("the header declares global max timestamp " + storedMaxTs + " but the blocks hold "
              + computedMaxTs);
          boundsDisagree = true;
        }
      }

      final long orphanTailBytes = walkedEveryBlock ? fileLength - blockStart : 0;
      // Whether the tail is PROVABLY a block that was never finished. The directory scan stops at the first thing
      // that is not a block magic, so a tail that DOES start with one is a block loadDirectory recognised and then
      // could not read to the end of - incomplete by its own evidence, and safe to drop. A tail that does not is
      // ambiguous: it can be an append that died before its magic reached the disk, or a COMPLETE block whose
      // magic took a bit flip, and the second is a block a hex editor could still recover. FIX drops only the
      // first, which is the whole difference between reclaiming a partial write and destroying the last block.
      final boolean tailIsUnfinishedBlock = orphanTailBytes >= 4
          && ByteBuffer.wrap(readBytes(blockStart, 4)).getInt() == BLOCK_MAGIC_VALUE;

      if (orphanTailBytes > 0)
        // A partial block left by an interrupted append is invisible to every other reader: it is neither used
        // nor reported. Say so - and say so with teeth, because appendBlock writes at the END of the file, so
        // every block appended from here on lands PAST the tail and is invisible for the same reason.
        problems.add(orphanTailBytes + " byte(s) follow the last readable block and belong to no block: "
            + (tailIsUnfinishedBlock ?
            "a block whose write did not complete" :
            "the tail of a write that did not complete, or a complete block whose magic was overwritten")
            + ". They are ignored by every read path, and so is anything appended after them"
            + (tailIsUnfinishedBlock ? "" : ". FIX leaves them alone: dropping them would destroy the block if it "
                + "is the second of those"));

      if (options.fix() && walkedEveryBlock) {
        // Truncation first: it changes the file length the header is about to be written for, and it removes only
        // bytes the directory scan already refuses to read.
        if (tailIsUnfinishedBlock) {
          indexFile.setLength(blockStart);
          indexChannel.force(true);
          repairs.add("dropped " + orphanTailBytes + " byte(s) of an incomplete write trailing the last block");
        }

        if (headerDisagrees || boundsDisagree) {
          // The in-memory bounds were READ from the header at open and never recomputed, so they carry the same
          // wrong values the header does; the directory is the authority here, exactly as it is for the count.
          globalMinTs = blockDirectory.isEmpty() ? Long.MAX_VALUE : computedMinTs;
          globalMaxTs = blockDirectory.isEmpty() ? Long.MIN_VALUE : computedMaxTs;
          rewriteHeader();
          headerDirty = false;
          repairs.add("rewrote the header from the block directory: " + blockDirectory.size() + " block(s)"
              + (blockDirectory.isEmpty() ? "" : ", timestamps " + globalMinTs + ".." + globalMaxTs));
        }
      }
    } finally {
      lock.unlock();
    }

    return new TimeSeriesIntegrity.Outcome(problems, repairs);
  }

  /**
   * Report-only overload for callers that only want the default tier - the shape this had before #6360.
   */
  public List<String> checkIntegrity() throws IOException {
    return checkIntegrity(TimeSeriesIntegrity.Options.REPORT_ONLY).problems();
  }

  /**
   * Decodes one block and reconciles it against what its own metadata says about it - the {@code DEEP} tier
   * (issue #6360 item 1).
   * <p>
   * <b>Why decoding earns its cost.</b> The block's CRC already proves the bytes are the bytes that were written.
   * What it cannot prove is that they mean what the block claims, and three read paths answer queries from those
   * claims without ever looking at the values:
   * <ul>
   * <li>{@code iterateRange} binary-searches a block's timestamps with {@code lowerBound}/{@code upperBound}, so
   * timestamps that are not sorted make it silently return a subset of the matching rows;</li>
   * <li>the aggregation push-down answers {@code MIN}/{@code MAX}/{@code SUM}/{@code AVG}/{@code COUNT} straight
   * from the declared per-column statistics without decompressing anything, so a wrong statistic is a wrong query
   * result that no later read ever contradicts;</li>
   * <li>block-level tag pruning SKIPS a whole block whose declared distinct tag values do not contain the value
   * being filtered on, so a distinct set missing a value it actually holds hides those rows entirely.</li>
   * </ul>
   * Each of those is a wrong ANSWER rather than an error, which is the category of damage a checker exists for.
   * <p>
   * The bytes are the ones already read for the CRC, so this costs decompression and no extra I/O. Sums are
   * compared with a relative tolerance because a {@code double} accumulation is only reproducible to within its own
   * rounding; minima, maxima, counts and timestamps are compared exactly, because they are values taken from the
   * data rather than computed over it.
   */
  private void checkBlockContent(final int blockIndex, final BlockEntry entry, final long blockStart,
      final byte[] blockBytes, final int tsColIdx, final List<String> problems) {
    final String where = "block " + blockIndex + " (offset " + blockStart + ")";
    try {
      final long[] timestamps = DeltaOfDeltaCodec.decode(sliceBlockColumn(entry, blockStart, blockBytes, tsColIdx));

      if (timestamps.length != entry.sampleCount)
        problems.add(where + " declares " + entry.sampleCount + " sample(s) but its timestamp column decodes to "
            + timestamps.length);

      if (timestamps.length > 0) {
        long decodedMin = timestamps[0];
        long decodedMax = timestamps[0];
        int unsortedAt = -1;
        for (int i = 1; i < timestamps.length; i++) {
          if (timestamps[i] < decodedMin)
            decodedMin = timestamps[i];
          if (timestamps[i] > decodedMax)
            decodedMax = timestamps[i];
          if (unsortedAt < 0 && timestamps[i] < timestamps[i - 1])
            unsortedAt = i;
        }

        if (unsortedAt >= 0)
          problems.add(where + " holds timestamps out of order at position " + unsortedAt + " ("
              + timestamps[unsortedAt] + " follows " + timestamps[unsortedAt - 1]
              + "): the range iterator binary-searches them, so a query over this block silently returns a subset");
        if (decodedMin != entry.minTimestamp)
          problems.add(where + " declares min timestamp " + entry.minTimestamp + " but its timestamps start at "
              + decodedMin);
        if (decodedMax != entry.maxTimestamp)
          problems.add(where + " declares max timestamp " + entry.maxTimestamp + " but its timestamps end at "
              + decodedMax);
      }

      for (int c = 0; c < columns.size(); c++) {
        if (c == tsColIdx)
          continue;

        final ColumnDefinition column = columns.get(c);
        final byte[] compressed = sliceBlockColumn(entry, blockStart, blockBytes, c);

        switch (column.getCompressionHint()) {
        case GORILLA_XOR -> checkNumericColumn(where, entry, c, column, GorillaXORCodec.decode(compressed), problems);
        case SIMPLE8B -> {
          final long[] longs = Simple8bCodec.decode(compressed);
          final double[] doubles = new double[longs.length];
          for (int i = 0; i < longs.length; i++)
            doubles[i] = longs[i];
          checkNumericColumn(where, entry, c, column, doubles, problems);
        }
        case DICTIONARY -> checkDictionaryColumn(where, entry, c, column, DictionaryCodec.decode(compressed), problems);
        default -> {
          // NONE / DELTA_OF_DELTA on a non-timestamp column: nothing this tier can assert about it.
        }
        }
      }
    } catch (final Exception e) {
      // A block whose CRC matched and whose bytes will not decode is a finding in its own right - the format and
      // the codec disagree about what those bytes are - and never a reason to abandon the rest of the file.
      problems.add(where + " passed its CRC but could not be decoded: " + e.getMessage());
    }
  }

  /**
   * The bytes of one column of a block, sliced out of the block image the CRC pass already read.
   */
  private static byte[] sliceBlockColumn(final BlockEntry entry, final long blockStart, final byte[] blockBytes,
      final int colIdx) {
    final int from = (int) (entry.columnOffsets[colIdx] - blockStart);
    return Arrays.copyOfRange(blockBytes, from, from + entry.columnSizes[colIdx]);
  }

  /**
   * Reduces one numeric column to the {@code {min, max, sum}} triple a block declares for it.
   * <p>
   * ONE definition, shared by the two write paths that produce the triple ({@link #downsampleBlocks} and
   * {@code TimeSeriesShard}'s compaction) and by the DEEP check that verifies it. It used to be three copies of the
   * same loop, and the failure mode of letting them drift belongs squarely to the checker: a verification reducing
   * differently from the writer reports a healthy block as damaged, which is the one thing an integrity check must
   * never do.
   */
  static double[] reduceNumericStats(final double[] values) {
    double min = Double.MAX_VALUE;
    double max = -Double.MAX_VALUE;
    double sum = 0;
    for (final double d : values) {
      if (d < min)
        min = d;
      if (d > max)
        max = d;
      sum += d;
    }
    return new double[] { min, max, sum };
  }

  /**
   * {@link #reduceNumericStats(double[])} over the boxed values a write path holds, unboxed exactly the way
   * {@link #compressColumn} unboxes them so the statistics describe the bytes that get written.
   */
  static double[] reduceNumericStats(final Object[] values) {
    final double[] unboxed = new double[values.length];
    for (int i = 0; i < values.length; i++)
      unboxed[i] = ColumnDefinition.numericValueOf(values[i]);
    return reduceNumericStats(unboxed);
  }

  /**
   * Reconciles one numeric column's decoded values against the min/max/sum the block declares for it - the numbers
   * the aggregation push-down answers from without decompressing anything.
   */
  private void checkNumericColumn(final String where, final BlockEntry entry, final int colIdx,
      final ColumnDefinition column, final double[] values, final List<String> problems) {
    if (values.length != entry.sampleCount) {
      problems.add(where + " declares " + entry.sampleCount + " sample(s) but column '" + column.getName()
          + "' decodes to " + values.length);
      return;
    }

    // A NaN declared minimum is how the format says "this column carries no statistics", which is what a writer
    // records for every column whose codec is not numeric. Nothing to reconcile against.
    if (Double.isNaN(entry.columnMins[colIdx]) || values.length == 0)
      return;

    final double[] stats = reduceNumericStats(values);
    final double min = stats[0];
    final double max = stats[1];
    final double sum = stats[2];

    if (min != entry.columnMins[colIdx])
      problems.add(where + " declares min " + entry.columnMins[colIdx] + " for column '" + column.getName()
          + "' but its values start at " + min + ": an aggregation answered from this block would be wrong");
    if (max != entry.columnMaxs[colIdx])
      problems.add(where + " declares max " + entry.columnMaxs[colIdx] + " for column '" + column.getName()
          + "' but its values reach " + max + ": an aggregation answered from this block would be wrong");
    // Relative, because summing the same doubles is only reproducible to within the rounding of the accumulation
    // itself - an absolute bound would fire on a healthy block of large values and pass on a damaged block of
    // small ones.
    final double declaredSum = entry.columnSums[colIdx];
    if (Math.abs(sum - declaredSum) > SUM_RELATIVE_TOLERANCE * Math.max(1.0, Math.abs(sum)))
      problems.add(where + " declares sum " + declaredSum + " for column '" + column.getName()
          + "' but its values add up to " + sum + ": an aggregation answered from this block would be wrong");
  }

  /**
   * Checks that a TAG column's declared distinct values cover the values it actually holds. They are what
   * block-level pruning SKIPS a block on, so a value present in the data but missing from the declaration makes
   * every query filtering on it miss this block entirely.
   */
  private void checkDictionaryColumn(final String where, final BlockEntry entry, final int colIdx,
      final ColumnDefinition column, final String[] values, final List<String> problems) {
    if (values.length != entry.sampleCount) {
      problems.add(where + " declares " + entry.sampleCount + " sample(s) but column '" + column.getName()
          + "' decodes to " + values.length);
      return;
    }

    if (column.getRole() != ColumnDefinition.ColumnRole.TAG || entry.tagDistinctValues == null
        || entry.tagDistinctValues[colIdx] == null)
      return;

    final Set<String> declared = new HashSet<>(Arrays.asList(entry.tagDistinctValues[colIdx]));
    final Set<String> missing = new LinkedHashSet<>();
    for (final String value : values)
      if (!declared.contains(value != null ? value : ""))
        missing.add(String.valueOf(value));

    if (!missing.isEmpty())
      problems.add(where + " holds tag value(s) " + missing + " in column '" + column.getName()
          + "' that its declared distinct set does not list: block pruning skips this block for those values, so "
          + "the rows carrying them are unreachable");
  }

  /**
   * Returns how many times {@link #downsampleBlocks} actually rewrote the sealed file. A steady-state
   * maintenance cycle (nothing new old enough to reduce) must not increment this. Used by regression tests
   * to assert downsampling idempotency (issue #4599).
   */
  long getDownsampleRewriteCount() {
    directoryLock.readLock().lock();
    try {
      return downsampleRewriteCount;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Returns the current on-disk size of the sealed-store file in bytes. Used by the HA compaction
   * path to decide whether the rewritten sealed store would be too large to ship inline in a single
   * Raft entry (issue #4382).
   */
  public long getFileSizeBytes() throws IOException {
    directoryLock.readLock().lock();
    try {
      return indexFile.length();
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Returns the total number of samples across all sealed blocks.
   * O(blockCount), all data already in memory from the block directory.
   */
  public long getTotalSampleCount() {
    directoryLock.readLock().lock();
    try {
      long total = 0;
      for (final BlockEntry entry : blockDirectory)
        total += entry.sampleCount;
      return total;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  public long getGlobalMinTimestamp() {
    return globalMinTs;
  }

  public long getGlobalMaxTimestamp() {
    return globalMaxTs;
  }

  public long getBlockMinTimestamp(final int blockIndex) {
    directoryLock.readLock().lock();
    try {
      return blockDirectory.get(blockIndex).minTimestamp;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  public long getBlockMaxTimestamp(final int blockIndex) {
    directoryLock.readLock().lock();
    try {
      return blockDirectory.get(blockIndex).maxTimestamp;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  @Override
  public void close() throws IOException {
    flushHeader();
    if (indexChannel != null && indexChannel.isOpen())
      indexChannel.close();
    if (indexFile != null)
      indexFile.close();
  }

  /**
   * Returns the sealed-store file name (relative, e.g. {@code weather_shard_0.ts.sealed}). Used as the
   * blob key when shipping the sealed store to HA followers.
   */
  public String getSealedFileName() {
    return new File(basePath + ".ts.sealed").getName();
  }

  /**
   * Reads the entire sealed-store file into a byte array. Used on the HA leader to capture the
   * post-compaction (or post-retention/downsampling) sealed bytes so they can be shipped to followers.
   * The header is flushed first so the on-disk image is current.
   */
  public byte[] readWholeSealedFile() throws IOException {
    flushHeader();
    directoryLock.readLock().lock();
    try {
      final long len = indexFile.length();
      if (len > Integer.MAX_VALUE)
        throw new IOException("Sealed store file too large to ship: " + len + " bytes");
      final byte[] bytes = new byte[(int) len];
      indexFile.seek(0);
      indexFile.readFully(bytes);
      return bytes;
    } finally {
      directoryLock.readLock().unlock();
    }
  }

  /**
   * Atomically replaces this sealed store's file with the supplied bytes (received from the HA leader)
   * and reopens it, rebuilding the in-memory block directory. Holds the write lock so concurrent
   * scans/iterations never observe a half-swapped file or a stale {@code FileChannel}.
   * <p>
   * The existing file handle is closed before the swap so the replace succeeds on platforms (Windows)
   * that forbid replacing a file while it is open.
   */
  public void installSealedFileBytes(final byte[] bytes) throws IOException {
    directoryLock.writeLock().lock();
    try {
      // Close current handles before replacing the file (required on Windows).
      try {
        if (indexChannel != null && indexChannel.isOpen())
          indexChannel.close();
      } catch (final IOException ignored) {
      }
      try {
        if (indexFile != null)
          indexFile.close();
      } catch (final IOException ignored) {
      }

      final File target = new File(basePath + ".ts.sealed");
      final File incoming = new File(basePath + ".ts.sealed.incoming");
      try (final FileOutputStream fos = new FileOutputStream(incoming)) {
        fos.write(bytes);
        fos.getFD().sync();
      }
      Files.move(incoming.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);

      indexFile = new RandomAccessFile(target, "rw");
      indexChannel = indexFile.getChannel();

      blockDirectory.clear();
      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;
      headerDirty = false;
      if (indexFile.length() >= HEADER_SIZE)
        loadDirectory();
      else
        writeEmptyHeader();
    } finally {
      directoryLock.writeLock().unlock();
    }
  }

  /**
   * Closes file handles and deletes the sealed store file from disk.
   */
  public void drop() throws IOException {
    close();
    final File f = new File(basePath + ".ts.sealed");
    if (f.exists() && !f.delete())
      throw new IOException("Failed to delete sealed store file: " + f.getAbsolutePath());
  }

  // --- Private helpers ---

  /**
   * Returns SKIP if the block cannot contain matching rows, FAST_PATH if the block
   * is homogeneous for the filtered tag(s), or SLOW_PATH if per-row filtering is needed.
   */
  BlockMatchResult blockMatchesTagFilter(final BlockEntry entry, final TagFilter tagFilter) {
    if (tagFilter == null)
      return BlockMatchResult.FAST_PATH;

    if (entry.tagDistinctValues == null)
      return BlockMatchResult.SLOW_PATH;

    boolean allSingleMatch = true;
    for (final TagFilter.Condition cond : tagFilter.getConditions()) {
      final int schemaIdx = findNonTsColumnSchemaIndex(cond.columnIndex());
      if (schemaIdx < 0 || schemaIdx >= entry.tagDistinctValues.length || entry.tagDistinctValues[schemaIdx] == null)
        return BlockMatchResult.SLOW_PATH;

      final String[] distinctVals = entry.tagDistinctValues[schemaIdx];
      boolean anyMatch = false;
      for (final String dv : distinctVals) {
        if (cond.matchValues().contains(dv)) {
          anyMatch = true;
          break;
        }
      }
      if (!anyMatch)
        return BlockMatchResult.SKIP;

      if (distinctVals.length != 1)
        allSingleMatch = false;
    }
    return allSingleMatch ? BlockMatchResult.FAST_PATH : BlockMatchResult.SLOW_PATH;
  }

  /**
   * Checks if a row at index i matches all tag filter conditions.
   */
  private static boolean matchesTagConditions(final String[][] tagCols,
      final List<TagFilter.Condition> conditions, final int i) {
    for (int ci = 0; ci < tagCols.length; ci++)
      if (!conditions.get(ci).matchValues().contains(tagCols[ci][i]))
        return false;
    return true;
  }

  /**
   * Builds the tag metadata byte array for a block.
   */
  private byte[] buildTagMetadata(final String[][] tagDistinctValues, final int colCount) {
    short tagColCount = 0;
    if (tagDistinctValues != null)
      for (int c = 0; c < colCount; c++)
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG && tagDistinctValues[c] != null)
          tagColCount++;

    if (tagColCount == 0) {
      final ByteBuffer buf = ByteBuffer.allocate(2);
      buf.putShort((short) 0);
      return buf.array();
    }

    // Pre-compute UTF-8 bytes
    final byte[][][] utf8Values = new byte[colCount][][];
    int totalSize = 2; // tagColCount
    for (int c = 0; c < colCount; c++) {
      if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG
          && tagDistinctValues[c] != null) {
        utf8Values[c] = new byte[tagDistinctValues[c].length][];
        totalSize += 2; // distinctCount
        for (int v = 0; v < tagDistinctValues[c].length; v++) {
          utf8Values[c][v] = tagDistinctValues[c][v].getBytes(StandardCharsets.UTF_8);
          if (utf8Values[c][v].length > 32767)
            throw new IllegalArgumentException(
                "Tag value too long: UTF-8 encoding is " + utf8Values[c][v].length
                    + " bytes (max 32767): '" + tagDistinctValues[c][v].substring(0, Math.min(40, tagDistinctValues[c][v].length())) + "...'");
          totalSize += 2 + utf8Values[c][v].length;
        }
      }
    }

    final ByteBuffer buf = ByteBuffer.allocate(totalSize);
    buf.putShort(tagColCount);
    for (int c = 0; c < colCount; c++) {
      if (utf8Values[c] != null) {
        buf.putShort((short) utf8Values[c].length);
        for (final byte[] val : utf8Values[c]) {
          buf.putShort((short) val.length);
          buf.put(val);
        }
      }
    }
    return buf.array();
  }

  private void writeEmptyHeader() throws IOException {
    final ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
    buf.putInt(MAGIC_VALUE);
    buf.put((byte) CURRENT_VERSION);
    buf.putShort((short) columns.size());
    buf.putInt(0); // block count
    buf.putLong(Long.MAX_VALUE); // min ts
    buf.putLong(Long.MIN_VALUE); // max ts
    buf.flip();
    indexChannel.write(buf, 0);
    indexChannel.force(true);
  }

  private void rewriteHeader() throws IOException {
    final ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
    buf.putInt(MAGIC_VALUE);
    buf.put((byte) CURRENT_VERSION);
    buf.putShort((short) columns.size());
    buf.putInt(blockDirectory.size());
    buf.putLong(globalMinTs);
    buf.putLong(globalMaxTs);
    buf.flip();
    indexChannel.write(buf, 0);
    indexChannel.force(false);
  }

  private void loadDirectory() throws IOException {
    final ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
    indexChannel.read(headerBuf, 0);
    headerBuf.flip();

    final int magic = headerBuf.getInt();
    if (magic != MAGIC_VALUE)
      throw new IOException(
          "Invalid sealed store magic in '" + basePath + ".ts.sealed': " + Integer.toHexString(magic));

    final int version = headerBuf.get() & 0xFF;
    if (version != CURRENT_VERSION)
      throw new IOException(
          "Unsupported sealed store format version " + version + " (expected: " + CURRENT_VERSION + ") in '"
              + basePath + ".ts.sealed'");

    final int colCount = headerBuf.getShort() & 0xFFFF;
    if (colCount != columns.size())
      throw new IOException("Column count mismatch in sealed store header of '" + basePath + ".ts.sealed': file has "
          + colCount + " columns, schema has " + columns.size());
    final int blockCount = headerBuf.getInt();
    globalMinTs = headerBuf.getLong();
    globalMaxTs = headerBuf.getLong();

    // Rebuild block directory by scanning block metadata records
    blockDirectory.clear();
    final long fileLength = indexFile.length();
    long pos = HEADER_SIZE;

    final int baseMetaSize = 4 + 8 + 8 + 4 + 4 * colCount; // magic + minTs + maxTs + sampleCount + colSizes

    while (pos + baseMetaSize <= fileLength) {
      final ByteBuffer metaBuf = ByteBuffer.allocate(baseMetaSize);
      if (indexChannel.read(metaBuf, pos) < baseMetaSize)
        break;
      metaBuf.flip();

      final int blockMagic = metaBuf.getInt();
      if (blockMagic != BLOCK_MAGIC_VALUE)
        break; // not a valid block header — stop scanning

      final long minTs = metaBuf.getLong();
      final long maxTs = metaBuf.getLong();
      final int sampleCount = metaBuf.getInt();

      final int[] colSizes = new int[colCount];
      for (int c = 0; c < colCount; c++)
        colSizes[c] = metaBuf.getInt();

      // Read stats section: numericColCount(4) + [min(8) + max(8) + sum(8)] * numericColCount (schema order)
      long statsPos = pos + baseMetaSize;
      final ByteBuffer numBuf = ByteBuffer.allocate(4);
      if (indexChannel.read(numBuf, statsPos) < 4)
        break;
      numBuf.flip();
      final int numericColCount = numBuf.getInt();
      statsPos += 4;

      final double[] mins = new double[colCount];
      final double[] maxs = new double[colCount];
      final double[] sums = new double[colCount];
      Arrays.fill(mins, Double.NaN);
      Arrays.fill(maxs, Double.NaN);

      if (numericColCount > 0) {
        final int tripletSize = (8 + 8 + 8) * numericColCount;
        final ByteBuffer statsBuf = ByteBuffer.allocate(tripletSize);
        if (indexChannel.read(statsBuf, statsPos) < tripletSize)
          break;
        statsBuf.flip();
        // Stats are in schema order — iterate columns, populate non-NaN entries
        int numericIdx = 0;
        for (int c = 0; c < colCount && numericIdx < numericColCount; c++) {
          if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP
              || columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG)
            continue;
          mins[c] = statsBuf.getDouble();
          maxs[c] = statsBuf.getDouble();
          sums[c] = statsBuf.getDouble();
          numericIdx++;
        }
        statsPos += tripletSize;
      }

      // Read tag metadata section
      String[][] blockTagDistinctValues = null;
      long tagEndPos = statsPos;
      final ByteBuffer tagCountBuf = ByteBuffer.allocate(2);
      if (indexChannel.read(tagCountBuf, tagEndPos) < 2)
        break;
      tagCountBuf.flip();
      // Read as unsigned short for safety
      final int tagColCount = tagCountBuf.getShort() & 0xFFFF;
      tagEndPos += 2;

      if (tagColCount > 0) {
        blockTagDistinctValues = new String[colCount][];
        int tagIdx = 0;
        for (int c = 0; c < colCount && tagIdx < tagColCount; c++) {
          if (columns.get(c).getRole() != ColumnDefinition.ColumnRole.TAG)
            continue;
          final ByteBuffer dcBuf = ByteBuffer.allocate(2);
          indexChannel.read(dcBuf, tagEndPos);
          dcBuf.flip();
          // Read as unsigned short: values up to 65535 (MAX_DICTIONARY_SIZE) are valid
          final int distinctCount = dcBuf.getShort() & 0xFFFF;
          tagEndPos += 2;

          blockTagDistinctValues[c] = new String[distinctCount];
          for (int v = 0; v < distinctCount; v++) {
            final ByteBuffer lenBuf = ByteBuffer.allocate(2);
            indexChannel.read(lenBuf, tagEndPos);
            lenBuf.flip();
            // Read as unsigned short: tag values are bounded to 32767 bytes at write time
            final int valLen = lenBuf.getShort() & 0xFFFF;
            tagEndPos += 2;

            final byte[] valBytes = new byte[valLen];
            final ByteBuffer valBuf = ByteBuffer.wrap(valBytes);
            indexChannel.read(valBuf, tagEndPos);
            blockTagDistinctValues[c][v] = new String(valBytes, StandardCharsets.UTF_8);
            tagEndPos += valLen;
          }
          tagIdx++;
        }
      }

      final BlockEntry entry = new BlockEntry(minTs, maxTs, sampleCount, colCount, mins, maxs, sums, pos);
      entry.tagDistinctValues = blockTagDistinctValues;
      long dataPos = tagEndPos;
      for (int c = 0; c < colCount; c++) {
        entry.columnOffsets[c] = dataPos;
        entry.columnSizes[c] = colSizes[c];
        dataPos += colSizes[c];
      }

      // Read stored CRC32 (validate lazily on first block read)
      final ByteBuffer crcBuf = ByteBuffer.allocate(4);
      if (indexChannel.read(crcBuf, dataPos) < 4)
        throw new IOException("Unexpected end of sealed store: missing block CRC32");
      crcBuf.flip();
      // Read from the file rather than recorded from a write, so the flag stays false: this is the one path whose
      // block has to be verified before it is trusted.
      entry.storedCRC = crcBuf.getInt();

      dataPos += 4; // skip CRC

      blockDirectory.add(entry);
      pos = dataPos;
    }
  }

  private long[] decompressTimestamps(final BlockEntry entry, final int tsColIdx) throws IOException {
    validateBlockCRC(entry);
    final byte[] compressed = readBytes(entry.columnOffsets[tsColIdx], entry.columnSizes[tsColIdx]);
    return DeltaOfDeltaCodec.decode(compressed);
  }

  private double[] decompressDoubleColumn(final BlockEntry entry, final int schemaColIdx) throws IOException {
    final byte[] compressed = readBytes(entry.columnOffsets[schemaColIdx], entry.columnSizes[schemaColIdx]);
    final ColumnDefinition col = columns.get(schemaColIdx);

    if (col.getCompressionHint() == TimeSeriesCodec.GORILLA_XOR)
      return GorillaXORCodec.decode(compressed);

    // For SIMPLE8B encoded longs, convert to doubles
    if (col.getCompressionHint() == TimeSeriesCodec.SIMPLE8B) {
      final long[] longs = Simple8bCodec.decode(compressed);
      final double[] result = new double[longs.length];
      for (int i = 0; i < longs.length; i++)
        result[i] = longs[i];
      return result;
    }

    throw new IllegalArgumentException(
        "decompressDoubleColumn: codec " + col.getCompressionHint() + " is not a numeric codec (column " + schemaColIdx + ")");
  }

  /**
   * A decoded block column kept in its primitive form.
   * <p>
   * The codecs already hand back {@code double[]} / {@code long[]} / {@code String[]}, so keeping
   * them unboxed lets a top-N scan pay one boxing per value actually returned instead of one per
   * value stored in the block (issue #5416).
   */
  private static final class RawColumn {
    private final ColumnDefinition col;
    private final double[]         doubles;
    private final long[]           longs;
    private final String[]         strings;

    private RawColumn(final ColumnDefinition col, final double[] doubles, final long[] longs, final String[] strings) {
      this.col = col;
      this.doubles = doubles;
      this.longs = longs;
      this.strings = strings;
    }

    /**
     * Boxing goes through the column definition so a sealed block hands back the same Java type the
     * mutable row does for the same declared column (issue #5475).
     */
    Object valueAt(final int i) {
      if (doubles != null)
        return col.boxDouble(doubles[i]);
      if (longs != null)
        return col.boxRaw(longs[i]);
      if (strings != null)
        return col.boxString(strings[i]);
      return null;
    }
  }

  /**
   * Same column selection as {@link #decompressColumns} but without boxing the values.
   */
  private RawColumn[] decompressColumnsRaw(final BlockEntry entry, final int[] columnIndices, final int tsColIdx)
      throws IOException {
    final List<RawColumn> result = new ArrayList<>();

    final BitSet colIndexSet;
    if (columnIndices != null) {
      colIndexSet = new BitSet();
      for (final int idx : columnIndices)
        colIndexSet.set(idx);
    } else {
      colIndexSet = null;
    }

    int nonTsIdx = 0;
    for (int c = 0; c < columns.size(); c++) {
      if (c == tsColIdx)
        continue;

      if (colIndexSet != null && !colIndexSet.get(nonTsIdx)) {
        nonTsIdx++;
        continue;
      }

      final byte[] compressed = readBytes(entry.columnOffsets[c], entry.columnSizes[c]);
      final ColumnDefinition col = columns.get(c);

      final RawColumn decoded = switch (col.getCompressionHint()) {
        case GORILLA_XOR -> new RawColumn(col, GorillaXORCodec.decode(compressed), null, null);
        case SIMPLE8B -> new RawColumn(col, null, Simple8bCodec.decode(compressed), null);
        case DICTIONARY -> new RawColumn(col, null, null, DictionaryCodec.decode(compressed));
        default -> new RawColumn(col, null, null, null);
      };

      result.add(decoded);
      nonTsIdx++;
    }
    return result.toArray(new RawColumn[0]);
  }

  /**
   * Mirrors {@link TagFilter#matchesMapped(Object[], int[])} against the raw, still unboxed columns.
   */
  private static boolean matchesRawColumns(final RawColumn[] rawColumns, final int rowIdx, final TagFilter tagFilter,
      final int[] columnIndices) {
    for (final TagFilter.Condition cond : tagFilter.getConditions()) {
      int outPos = -1;
      if (columnIndices == null)
        outPos = cond.columnIndex();
      else
        for (int i = 0; i < columnIndices.length; i++)
          if (columnIndices[i] == cond.columnIndex()) {
            outPos = i;
            break;
          }

      if (outPos < 0 || outPos >= rawColumns.length)
        return false;
      if (!cond.matchValues().contains(rawColumns[outPos].valueAt(rowIdx)))
        return false;
    }
    return true;
  }

  private Object[][] decompressColumns(final BlockEntry entry, final int[] columnIndices, final int tsColIdx) throws IOException {
    final List<Object[]> result = new ArrayList<>();

    // Build a BitSet for O(1) column-index lookup in the hot path (avoids O(n) linear scan per column)
    final BitSet colIndexSet;
    if (columnIndices != null) {
      colIndexSet = new BitSet();
      for (final int idx : columnIndices)
        colIndexSet.set(idx);
    } else {
      colIndexSet = null;
    }

    int nonTsIdx = 0;
    for (int c = 0; c < columns.size(); c++) {
      if (c == tsColIdx)
        continue;

      if (colIndexSet != null && !colIndexSet.get(nonTsIdx)) {
        nonTsIdx++;
        continue;
      }

      final byte[] compressed = readBytes(entry.columnOffsets[c], entry.columnSizes[c]);
      final ColumnDefinition col = columns.get(c);

      // Boxing goes through the column definition so a sealed block hands back the same Java type the
      // mutable row does for the same declared column (issue #5475).
      final Object[] decompressed = switch (col.getCompressionHint()) {
        case GORILLA_XOR -> {
          final double[] vals = GorillaXORCodec.decode(compressed);
          final Object[] boxed = new Object[vals.length];
          for (int i = 0; i < vals.length; i++)
            boxed[i] = col.boxDouble(vals[i]);
          yield boxed;
        }
        case SIMPLE8B -> {
          final long[] vals = Simple8bCodec.decode(compressed);
          final Object[] boxed = new Object[vals.length];
          for (int i = 0; i < vals.length; i++)
            boxed[i] = col.boxRaw(vals[i]);
          yield boxed;
        }
        case DICTIONARY -> {
          final String[] vals = DictionaryCodec.decode(compressed);
          final Object[] boxed = new Object[vals.length];
          for (int i = 0; i < vals.length; i++)
            boxed[i] = col.boxString(vals[i]);
          yield boxed;
        }
        default -> new Object[entry.sampleCount];
      };

      result.add(decompressed);
      nonTsIdx++;
    }
    return result.toArray(new Object[0][]);
  }

  private byte[] readBytes(final long offset, final int size) throws IOException {
    final ByteBuffer buf = ByteBuffer.allocate(size);
    int totalRead = 0;
    while (totalRead < size) {
      final int read = indexChannel.read(buf, offset + totalRead);
      if (read == -1)
        throw new IOException("Unexpected end of sealed store at offset " + (offset + totalRead));
      totalRead += read;
    }
    return buf.array();
  }

  /**
   * Reads all column data for a block in a single I/O call.
   * Columns are contiguous on disk, so one pread covers all of them.
   */
  private byte[] readBlockData(final BlockEntry entry) throws IOException {
    final long dataStart = entry.columnOffsets[0];
    int totalDataSize = 0;
    for (final int s : entry.columnSizes)
      totalDataSize += s;
    final byte[] data = readBytes(dataStart, totalDataSize);
    if (!entry.crcValidated) {
      final int metaSize = (int) (dataStart - entry.blockStartOffset);
      final byte[] metaBytes = readBytes(entry.blockStartOffset, metaSize);
      final CRC32 crc = new CRC32();
      crc.update(metaBytes);
      crc.update(data);
      checkCRC(entry, crc);
    }
    return data;
  }

  /**
   * Validates block CRC32 on first access (used by scanRange/iterateRange paths).
   * Reads the entire block (meta + data) in one call to verify.
   */
  private void validateBlockCRC(final BlockEntry entry) throws IOException {
    if (entry.crcValidated)
      return;
    final long endOfData = entry.columnOffsets[entry.columnSizes.length - 1]
        + entry.columnSizes[entry.columnSizes.length - 1];
    final int blockSize = (int) (endOfData - entry.blockStartOffset);
    final byte[] blockBytes = readBytes(entry.blockStartOffset, blockSize);
    final CRC32 crc = new CRC32();
    crc.update(blockBytes);
    checkCRC(entry, crc);
  }

  private void checkCRC(final BlockEntry entry, final CRC32 crc) throws IOException {
    if ((int) crc.getValue() != entry.storedCRC)
      throw new IOException("CRC mismatch in sealed store block at offset " + entry.blockStartOffset
          + " (stored=0x" + Integer.toHexString(entry.storedCRC)
          + ", computed=0x" + Integer.toHexString((int) crc.getValue()) + ")");
    entry.crcValidated = true;
  }

  /**
   * Slices a single column's bytes from the coalesced block data.
   */
  private static byte[] sliceColumn(final byte[] blockData, final BlockEntry entry, final int colIdx) {
    final int offset = (int) (entry.columnOffsets[colIdx] - entry.columnOffsets[0]);
    return Arrays.copyOfRange(blockData, offset, offset + entry.columnSizes[colIdx]);
  }

  /**
   * Decompresses a double column from pre-read bytes (no I/O).
   */
  private double[] decompressDoubleColumnFromBytes(final byte[] compressed, final int schemaColIdx) throws IOException {
    final ColumnDefinition col = columns.get(schemaColIdx);

    if (col.getCompressionHint() == TimeSeriesCodec.GORILLA_XOR)
      return GorillaXORCodec.decode(compressed);

    if (col.getCompressionHint() == TimeSeriesCodec.SIMPLE8B) {
      final long[] longs = Simple8bCodec.decode(compressed);
      final double[] result = new double[longs.length];
      for (int i = 0; i < longs.length; i++)
        result[i] = longs[i];
      return result;
    }

    throw new IllegalArgumentException(
        "decompressDoubleColumnFromBytes: codec " + col.getCompressionHint() + " is not a numeric codec (column " + schemaColIdx + ")");
  }

  private int findTimestampColumnIndex() {
    for (int i = 0; i < columns.size(); i++)
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        return i;
    return 0;
  }

  private int findNonTsColumnSchemaIndex(final int nonTsIndex) {
    int count = 0;
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (count == nonTsIndex)
        return i;
      count++;
    }
    throw new IllegalArgumentException("Column index " + nonTsIndex + " out of range");
  }

  private void accumulateSample(final AggregationResult result, final long bucketTs, final double value,
      final AggregationType type) {
    final int idx = result.findBucketIndex(bucketTs);
    if (idx >= 0) {
      final double existing = result.getValue(idx);
      final long count = result.getCount(idx);
      final double merged = switch (type) {
        case SUM -> existing + value;
        case COUNT -> existing + 1;
        case AVG -> existing + value; // accumulate sum, divide by count later
        // NaN policy (issue #4596): NaN is treated as absent and skipped, so a real value always
        // wins over a NaN running value (consistent with the row-iter, merge and SIMD paths).
        case MIN -> Double.isNaN(value) ? existing : Double.isNaN(existing) ? value : Math.min(existing, value);
        case MAX -> Double.isNaN(value) ? existing : Double.isNaN(existing) ? value : Math.max(existing, value);
      };
      result.updateValue(idx, merged);
      result.updateCount(idx, count + 1);
    } else {
      result.addBucket(bucketTs, type == AggregationType.COUNT ? 1 : value, 1);
    }
  }

}
