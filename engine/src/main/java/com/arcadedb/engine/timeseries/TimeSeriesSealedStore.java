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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    long           blockStartOffset; // file offset where block meta begins (for lazy CRC)
    int            storedCRC;        // CRC32 stored on disk (-1 if written inline, not yet flushed)
    volatile boolean crcValidated;   // true after first successful CRC check (volatile: read without lock)

    BlockEntry(final long minTs, final long maxTs, final int sampleCount, final int columnCount,
        final double[] mins, final double[] maxs, final double[] sums) {
      this.minTimestamp = minTs;
      this.maxTimestamp = maxTs;
      this.sampleCount = sampleCount;
      this.columnOffsets = new long[columnCount];
      this.columnSizes = new int[columnCount];
      this.columnMins = mins;
      this.columnMaxs = maxs;
      this.columnSums = sums;
      this.crcValidated = true; // newly created blocks don't need validation
    }
  }

  public TimeSeriesSealedStore(final String basePath, final List<ColumnDefinition> columns) throws IOException {
    this.basePath = basePath;
    this.columns = columns;

    // Clean up stale .tmp files left by interrupted shutdown or maintenance
    final File tmpFile = new File(basePath + ".ts.sealed.tmp");
    if (tmpFile.exists() && !tmpFile.delete())
      throw new IOException("Failed to delete stale temporary file: " + tmpFile.getAbsolutePath());

    final File f = new File(basePath + ".ts.sealed");
    final boolean exists = f.exists();
    this.indexFile = new RandomAccessFile(f, "rw");
    this.indexChannel = indexFile.getChannel();

    if (exists && indexFile.length() >= HEADER_SIZE)
      loadDirectory();
    else
      writeEmptyHeader();
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

      long offset = indexFile.length();
      indexFile.seek(offset);
      indexFile.write(metaBuf.array(), 0, metaBuf.limit());
      offset += metaSize;

      final BlockEntry entry = new BlockEntry(minTs, maxTs, sampleCount, colCount, columnMins, columnMaxs, columnSums);
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
      final ByteBuffer crcBuf = ByteBuffer.allocate(4);
      crcBuf.putInt((int) crc.getValue());
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
   * @return iterator yielding Object[] { timestamp, col1, col2, ... }
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

          final long bucketTs = bucketIntervalMs > 0 ? (timestamps[i] / bucketIntervalMs) * bucketIntervalMs : fromTs;

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
        final long blockMinBucket = (entry.minTimestamp / bucketIntervalMs) * bucketIntervalMs;
        final long blockMaxBucket = (entry.maxTimestamp / bucketIntervalMs) * bucketIntervalMs;

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
            final long bucketTs = (timestamps[i] / bucketIntervalMs) * bucketIntervalMs;
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
            final long bucketTs = (timestamps[segStart] / bucketIntervalMs) * bucketIntervalMs;
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

          result.accumulateRow(fromTs, rowValues);
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

      // Atomic file swap — if move fails, the original file and blockDirectory remain intact
      indexChannel.close();
      indexFile.close();

      final File oldFile = new File(basePath + ".ts.sealed");
      final File tmpFile = new File(tempPath);
      Files.move(tmpFile.toPath(), oldFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

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
      // Check if block is already at target resolution (density check)
      if (entry.sampleCount <= 1 || (entry.sampleCount > 1
          && (entry.maxTimestamp - entry.minTimestamp) / (entry.sampleCount - 1) >= granularityMs)) {
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
        final long bucketTs = (timestamps[i] / granularityMs) * granularityMs;

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

      // Collect distinct tag values for this chunk
      final String[][] chunkTagDV = new String[colCount][];
      for (int c = 0; c < colCount; c++) {
        if (columns.get(c).getRole() == ColumnDefinition.ColumnRole.TAG) {
          final java.util.LinkedHashSet<String> distinctSet = new java.util.LinkedHashSet<>();
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
    rewriteWithBlocks(toKeep, newBlocksCompressed, newBlocksMeta, newBlocksMins, newBlocksMaxs, newBlocksSums,
        newBlocksTagDV);
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
      final List<String[][]> newTagDistinctValues) throws IOException {

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
          newDirectory.add(entry);
        }
      }
    }

    // Atomic file swap — if move fails, the original file and blockDirectory remain intact
    indexChannel.close();
    indexFile.close();

    final File oldFile = new File(basePath + ".ts.sealed");
    final File tmpFile = new File(tempPath);
    Files.move(tmpFile.toPath(), oldFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

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

    long dataOffset = tempFile.length();
    tempFile.seek(dataOffset);
    tempFile.write(metaBuf.array(), 0, metaBuf.limit());
    dataOffset += metaSize;

    final BlockEntry newEntry = new BlockEntry(minTs, maxTs, sampleCount, colCount, columnMins, columnMaxs, columnSums);
    newEntry.tagDistinctValues = tagDistinctValues;
    for (int c = 0; c < colCount; c++) {
      newEntry.columnOffsets[c] = dataOffset;
      newEntry.columnSizes[c] = compressedCols[c].length;
      crc.update(compressedCols[c]);
      tempFile.write(compressedCols[c]);
      dataOffset += compressedCols[c].length;
    }

    final ByteBuffer crcBuf = ByteBuffer.allocate(4);
    crcBuf.putInt((int) crc.getValue());
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
      indexChannel.close();
      indexFile.close();

      final File sealedFile = new File(basePath + ".ts.sealed");
      final File tmpFile = new File(basePath + ".ts.sealed.tmp");
      Files.move(tmpFile.toPath(), sealedFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

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
          doubles[i] = values[i] != null ? ((Number) values[i]).doubleValue() : 0.0;
        yield GorillaXORCodec.encode(doubles);
      }
      case SIMPLE8B -> {
        final long[] longs = new long[values.length];
        for (int i = 0; i < values.length; i++)
          longs[i] = values[i] != null ? ((Number) values[i]).longValue() : 0L;
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

      // Atomic file swap — if move fails, the original file and blockDirectory remain intact
      indexChannel.close();
      indexFile.close();

      final File oldFile = new File(basePath + ".ts.sealed");
      final File tmpFile = new File(tempPath);
      Files.move(tmpFile.toPath(), oldFile.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

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

  public int getBlockCount() {
    directoryLock.readLock().lock();
    try {
      return blockDirectory.size();
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
    return blockDirectory.get(blockIndex).minTimestamp;
  }

  public long getBlockMaxTimestamp(final int blockIndex) {
    return blockDirectory.get(blockIndex).maxTimestamp;
  }

  @Override
  public void close() throws IOException {
    flushHeader();
    if (indexChannel != null && indexChannel.isOpen())
      indexChannel.close();
    if (indexFile != null)
      indexFile.close();
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
        if (cond.values().contains(dv)) {
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
      if (!conditions.get(ci).values().contains(tagCols[ci][i]))
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
      throw new IOException("Invalid sealed store magic: " + Integer.toHexString(magic));

    final int version = headerBuf.get() & 0xFF;
    if (version != CURRENT_VERSION)
      throw new IOException(
          "Unsupported sealed store format version " + version + " (expected: " + CURRENT_VERSION + ")");

    final int colCount = headerBuf.getShort() & 0xFFFF;
    if (colCount != columns.size())
      throw new IOException("Column count mismatch in sealed store header: file has " + colCount
          + " columns, schema has " + columns.size());
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

      final BlockEntry entry = new BlockEntry(minTs, maxTs, sampleCount, colCount, mins, maxs, sums);
      entry.tagDistinctValues = blockTagDistinctValues;
      entry.blockStartOffset = pos;
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
      entry.storedCRC = crcBuf.getInt();
      entry.crcValidated = false;

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

  private Object[][] decompressColumns(final BlockEntry entry, final int[] columnIndices, final int tsColIdx) throws IOException {
    final List<Object[]> result = new ArrayList<>();

    int nonTsIdx = 0;
    for (int c = 0; c < columns.size(); c++) {
      if (c == tsColIdx)
        continue;

      if (columnIndices != null && !isInArray(nonTsIdx, columnIndices)) {
        nonTsIdx++;
        continue;
      }

      final byte[] compressed = readBytes(entry.columnOffsets[c], entry.columnSizes[c]);
      final ColumnDefinition col = columns.get(c);

      final Object[] decompressed = switch (col.getCompressionHint()) {
        case GORILLA_XOR -> {
          final double[] vals = GorillaXORCodec.decode(compressed);
          final Object[] boxed = new Object[vals.length];
          for (int i = 0; i < vals.length; i++)
            boxed[i] = vals[i];
          yield boxed;
        }
        case SIMPLE8B -> {
          final long[] vals = Simple8bCodec.decode(compressed);
          final Object[] boxed = new Object[vals.length];
          if (col.getDataType() == Type.INTEGER) {
            for (int i = 0; i < vals.length; i++)
              boxed[i] = (int) vals[i];
          } else {
            for (int i = 0; i < vals.length; i++)
              boxed[i] = vals[i];
          }
          yield boxed;
        }
        case DICTIONARY -> {
          final String[] vals = DictionaryCodec.decode(compressed);
          final Object[] boxed = new Object[vals.length];
          System.arraycopy(vals, 0, boxed, 0, vals.length);
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
        case MIN -> Math.min(existing, value);
        case MAX -> Math.max(existing, value);
      };
      result.updateValue(idx, merged);
      result.updateCount(idx, count + 1);
    } else {
      result.addBucket(bucketTs, type == AggregationType.COUNT ? 1 : value, 1);
    }
  }

  private static boolean isInArray(final int value, final int[] array) {
    for (final int v : array)
      if (v == value)
        return true;
    return false;
  }
}
