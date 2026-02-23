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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

/**
 * Immutable columnar storage for compacted TimeSeries data.
 * Uses FileChannel positioned reads for zero-overhead access.
 * <p>
 * Index file (.ts.sealed) layout — 27-byte header:
 * - [0..3]   magic "TSIX" (4 bytes)
 * - [4]      formatVersion (1 byte)
 * - [5..6]   column count (short)
 * - [7..10]  block count (int)
 * - [11..18] global min timestamp (long)
 * - [19..26] global max timestamp (long)
 * - [27..]   block entries (inline metadata + compressed column data)
 * <p>
 * Block entry layout:
 * - magic "TSBL" (4), minTs (8), maxTs (8), sampleCount (4), colSizes (4*colCount)
 * - numericColCount (4), [min (8) + max (8) + sum (8)] * numericColCount (schema order, no colIdx)
 * - compressed column data bytes
 * - blockCRC32 (4) — CRC32 of everything from blockMagic to end of compressed data
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesSealedStore implements AutoCloseable {

  static final         int CURRENT_VERSION  = 1;
  private static final int MAGIC_VALUE       = 0x54534958; // "TSIX"
  private static final int BLOCK_MAGIC_VALUE = 0x5453424C; // "TSBL"
  private static final int HEADER_SIZE       = 27;
  private static final int MAX_BLOCK_SIZE    = 65536;

  private final String               basePath;
  private final List<ColumnDefinition> columns;
  private       RandomAccessFile     indexFile;
  private       FileChannel          indexChannel;
  private       int                  fileVersion;

  enum BlockMatchResult { SKIP, FAST_PATH, SLOW_PATH }

  // In-memory block directory (loaded at open)
  private final List<BlockEntry> blockDirectory = new ArrayList<>();
  private       long             globalMinTs    = Long.MAX_VALUE;
  private       long             globalMaxTs    = Long.MIN_VALUE;

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
    boolean        crcValidated;     // true after first successful CRC check

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
  public synchronized void appendBlock(final int sampleCount, final long minTs, final long maxTs,
      final byte[][] compressedColumns,
      final double[] columnMins, final double[] columnMaxs, final double[] columnSums,
      final String[][] tagDistinctValues) throws IOException {
    // Upgrade version 0 files to version 1 format
    if (fileVersion < 1) {
      if (!blockDirectory.isEmpty())
        upgradeFileToVersion1();
      else
        fileVersion = 1;
    }

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
    final CRC32 crc = new CRC32();
    crc.update(metaBuf.array());

    long offset = indexFile.length();
    indexFile.seek(offset);
    indexFile.write(metaBuf.array());
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

    rewriteHeader();
  }

  /**
   * Scans blocks overlapping the given time range and returns decompressed data.
   */
  public List<Object[]> scanRange(final long fromTs, final long toTs, final int[] columnIndices,
      final TagFilter tagFilter) throws IOException {
    final List<Object[]> results = new ArrayList<>();

    for (final BlockEntry entry : blockDirectory) {
      if (entry.maxTimestamp < fromTs || entry.minTimestamp > toTs)
        continue;

      if (tagFilter != null && blockMatchesTagFilter(entry, tagFilter) == BlockMatchResult.SKIP)
        continue;

      // Decompress timestamp column (always column 0)
      final long[] timestamps = decompressTimestamps(entry, 0);

      // Decompress requested columns
      final int tsColIdx = findTimestampColumnIndex();
      final Object[][] decompressedCols = decompressColumns(entry, columnIndices, tsColIdx);

      // Filter by time range and build result rows
      for (int i = 0; i < timestamps.length; i++) {
        if (timestamps[i] < fromTs || timestamps[i] > toTs)
          continue;

        final int resultCols = decompressedCols.length + 1;
        final Object[] row = new Object[resultCols];
        row[0] = timestamps[i];
        for (int c = 0; c < decompressedCols.length; c++)
          row[c + 1] = decompressedCols[c][i];

        results.add(row);
      }
    }
    return results;
  }

  /**
   * Returns a lazy iterator over sealed blocks overlapping the given time range.
   * Decompresses one block at a time, yielding rows on demand.
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

    final int firstBlockIdx = startBlockIdx;

    return new Iterator<>() {
      private int        blockIdx    = firstBlockIdx;
      private long[]     timestamps  = null;
      private Object[][] decompCols  = null;
      private int        rowIdx      = 0;
      private int        rowEnd      = 0;  // exclusive upper bound within block
      private int        resultCols  = 0;
      private Object[]   nextRow     = null;

      {
        advance();
      }

      private void advance() {
        nextRow = null;
        try {
          while (true) {
            // Yield from current decompressed block
            if (timestamps != null) {
              if (rowIdx < rowEnd) {
                final Object[] row = new Object[resultCols];
                row[0] = timestamps[rowIdx];
                for (int c = 0; c < decompCols.length; c++)
                  row[c + 1] = decompCols[c][rowIdx];
                rowIdx++;
                nextRow = row;
                return;
              }
              // Current block exhausted
              timestamps = null;
              decompCols = null;
            }

            // Find next matching block
            if (blockIdx >= dirSize)
              return;

            final BlockEntry entry = blockDirectory.get(blockIdx);

            // Early termination: blocks are sorted, so if minTs > toTs all remaining are past range
            if (entry.minTimestamp > toTs)
              return;

            blockIdx++;

            if (entry.maxTimestamp < fromTs)
              continue;

            // Block-level tag filter: skip blocks that cannot contain matching rows
            if (tagFilter != null && blockMatchesTagFilter(entry, tagFilter) == BlockMatchResult.SKIP)
              continue;

            // Decompress timestamps first
            final long[] ts = decompressTimestamps(entry, tsColIdx);

            // Binary search for the matching range within sorted timestamps
            final int start = lowerBound(ts, fromTs);
            final int end = upperBound(ts, toTs);

            if (start >= end)
              continue;

            // Timestamps have matches — now decompress value columns
            timestamps = ts;
            decompCols = decompressColumns(entry, columnIndices, tsColIdx);
            rowIdx = start;
            rowEnd = end;
            resultCols = decompCols.length + 1;
          }
        } catch (final IOException e) {
          throw new RuntimeException("Error iterating sealed TimeSeries blocks", e);
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
  }

  /**
   * Removes all blocks with maxTimestamp < threshold.
   */
  public synchronized void truncateBefore(final long timestamp) throws IOException {
    final List<BlockEntry> retained = new ArrayList<>();
    for (final BlockEntry entry : blockDirectory)
      if (entry.maxTimestamp >= timestamp)
        retained.add(entry);

    if (retained.size() == blockDirectory.size())
      return; // Nothing to truncate

    // Rewrite the file with only retained blocks
    final int colCount = columns.size();
    final String tempPath = basePath + ".ts.sealed.tmp";
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

      blockDirectory.clear();
      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;

      for (final BlockEntry oldEntry : retained)
        copyBlockToFile(tempFile, oldEntry, colCount);
    }

    // Swap files
    indexChannel.close();
    indexFile.close();

    final File oldFile = new File(basePath + ".ts.sealed");
    final File tmpFile = new File(tempPath);
    if (!oldFile.delete() || !tmpFile.renameTo(oldFile))
      throw new IOException("Failed to swap sealed store files during truncation");

    indexFile = new RandomAccessFile(oldFile, "rw");
    indexChannel = indexFile.getChannel();
    fileVersion = CURRENT_VERSION;
    rewriteHeader();
  }

  /**
   * Downsamples blocks older than cutoffTs to the given granularity.
   * Blocks already at the target resolution or coarser are left untouched (idempotency).
   * Numeric fields are averaged per (bucketTs, tagKey) group; tag columns preserved.
   */
  public synchronized void downsampleBlocks(final long cutoffTs, final long granularityMs,
      final int tsColIdx, final List<Integer> tagColIndices, final List<Integer> numericColIndices) throws IOException {

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
    final Map<String, Map<Long, double[]>> groupedData = new HashMap<>(); // tagKey -> (bucketTs -> [sum0, count0, sum1, count1, ...])
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

      // Group samples by (tagKey, bucketTs)
      for (int i = 0; i < timestamps.length; i++) {
        final long bucketTs = (timestamps[i] / granularityMs) * granularityMs;

        // Build tag key
        final StringBuilder tagKeyBuilder = new StringBuilder();
        for (int t = 0; t < tagData.length; t++) {
          if (t > 0)
            tagKeyBuilder.append('\0');
          tagKeyBuilder.append(tagData[t][i] != null ? tagData[t][i].toString() : "");
        }
        final String tagKey = tagKeyBuilder.toString();

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
    for (final Map.Entry<String, Map<Long, double[]>> tagEntry : groupedData.entrySet()) {
      final String[] tagParts = tagEntry.getKey().split("\0", -1);
      for (final Map.Entry<Long, double[]> bucketEntry : tagEntry.getValue().entrySet()) {
        final long bucketTs = bucketEntry.getKey();
        final double[] acc = bucketEntry.getValue();

        // Build a full row: [timestamp, tag0, tag1, ..., field0, field1, ...]
        // ordered by column index
        final Object[] row = new Object[columns.size()];
        row[tsColIdx] = bucketTs;
        for (int t = 0; t < tagColIndices.size(); t++)
          row[tagColIndices.get(t)] = t < tagParts.length ? tagParts[t] : "";
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
  }

  /**
   * Rewrites the sealed file, copying retained blocks as raw bytes and appending new blocks.
   * Sorts the combined result by minTimestamp. Uses atomic tmp-file rename.
   */
  private void rewriteWithBlocks(final List<BlockEntry> retained,
      final List<byte[][]> newCompressed, final List<long[]> newMeta,
      final List<double[]> newMins, final List<double[]> newMaxs, final List<double[]> newSums,
      final List<String[][]> newTagDistinctValues) throws IOException {

    final int colCount = columns.size();
    final String tempPath = basePath + ".ts.sealed.tmp";

    try (final RandomAccessFile tempFile = new RandomAccessFile(tempPath, "rw")) {
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

      blockDirectory.clear();
      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;

      // Write retained blocks (raw copy)
      for (final BlockEntry oldEntry : retained)
        copyBlockToFile(tempFile, oldEntry, colCount);

      // Write new downsampled blocks
      for (int b = 0; b < newCompressed.size(); b++) {
        final long[] meta = newMeta.get(b);
        writeNewBlockToFile(tempFile, (int) meta[2], meta[0], meta[1],
            newCompressed.get(b), newMins.get(b), newMaxs.get(b), newSums.get(b), colCount,
            newTagDistinctValues != null ? newTagDistinctValues.get(b) : null);
      }

      // Sort block directory by minTimestamp
      blockDirectory.sort(Comparator.comparingLong(e -> e.minTimestamp));
    }

    // Swap files
    indexChannel.close();
    indexFile.close();

    final File oldFile = new File(basePath + ".ts.sealed");
    final File tmpFile = new File(tempPath);
    if (!oldFile.delete() || !tmpFile.renameTo(oldFile))
      throw new IOException("Failed to swap sealed store files during downsampling");

    indexFile = new RandomAccessFile(oldFile, "rw");
    indexChannel = indexFile.getChannel();
    fileVersion = CURRENT_VERSION;
    rewriteHeader();
  }

  private void copyBlockToFile(final RandomAccessFile tempFile, final BlockEntry oldEntry, final int colCount) throws IOException {
    final byte[][] compressedCols = new byte[colCount][];
    for (int c = 0; c < colCount; c++)
      compressedCols[c] = readBytes(oldEntry.columnOffsets[c], oldEntry.columnSizes[c]);

    writeNewBlockToFile(tempFile, oldEntry.sampleCount, oldEntry.minTimestamp, oldEntry.maxTimestamp,
        compressedCols, oldEntry.columnMins, oldEntry.columnMaxs, oldEntry.columnSums, colCount, oldEntry.tagDistinctValues);
  }

  private void writeNewBlockToFile(final RandomAccessFile tempFile, final int sampleCount,
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

    final CRC32 crc = new CRC32();
    crc.update(metaBuf.array());

    long dataOffset = tempFile.length();
    tempFile.seek(dataOffset);
    tempFile.write(metaBuf.array());
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

    blockDirectory.add(newEntry);

    if (minTs < globalMinTs)
      globalMinTs = minTs;
    if (maxTs > globalMaxTs)
      globalMaxTs = maxTs;
  }

  private static byte[] compressColumn(final ColumnDefinition col, final Object[] values) {
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
      default -> new byte[0];
    };
  }

  /**
   * Truncates the sealed store to exactly {@code targetBlockCount} blocks,
   * removing any blocks appended after the watermark during an interrupted compaction.
   */
  public synchronized void truncateToBlockCount(final long targetBlockCount) throws IOException {
    if (targetBlockCount >= blockDirectory.size())
      return; // nothing to truncate

    final List<BlockEntry> retained = new ArrayList<>(blockDirectory.subList(0, (int) targetBlockCount));
    final int colCount = columns.size();
    final String tempPath = basePath + ".ts.sealed.tmp";
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

      blockDirectory.clear();
      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;

      for (final BlockEntry entry : retained)
        copyBlockToFile(tempFile, entry, colCount);
    }

    // Swap files
    indexChannel.close();
    indexFile.close();

    final File oldFile = new File(basePath + ".ts.sealed");
    final File tmpFile = new File(tempPath);
    if (!oldFile.delete() || !tmpFile.renameTo(oldFile))
      throw new IOException("Failed to swap sealed store files during truncation");

    indexFile = new RandomAccessFile(oldFile, "rw");
    indexChannel = indexFile.getChannel();
    fileVersion = CURRENT_VERSION;
    rewriteHeader();
  }

  public int getBlockCount() {
    return blockDirectory.size();
  }

  /**
   * Returns the total number of samples across all sealed blocks.
   * O(blockCount), all data already in memory from the block directory.
   */
  public long getTotalSampleCount() {
    long total = 0;
    for (final BlockEntry entry : blockDirectory)
      total += entry.sampleCount;
    return total;
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

  /**
   * Upgrades a version 0 file to version 1 by rewriting all blocks with tag metadata sections.
   * Existing blocks get tagColCount=0 (empty tag metadata). One-time migration cost.
   */
  private void upgradeFileToVersion1() throws IOException {
    final List<BlockEntry> allBlocks = new ArrayList<>(blockDirectory);
    final int colCount = columns.size();
    final String tempPath = basePath + ".ts.sealed.tmp";

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

      blockDirectory.clear();
      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;

      for (final BlockEntry oldEntry : allBlocks)
        copyBlockToFile(tempFile, oldEntry, colCount);
    }

    indexChannel.close();
    indexFile.close();

    final File oldFile = new File(basePath + ".ts.sealed");
    final File tmpFile = new File(tempPath);
    if (!oldFile.delete() || !tmpFile.renameTo(oldFile))
      throw new IOException("Failed to upgrade sealed store to version 1");

    indexFile = new RandomAccessFile(oldFile, "rw");
    indexChannel = indexFile.getChannel();
    fileVersion = CURRENT_VERSION;
    rewriteHeader();
  }

  private void writeEmptyHeader() throws IOException {
    fileVersion = CURRENT_VERSION;
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
    if (version > CURRENT_VERSION)
      throw new IOException(
          "Unsupported sealed store format version " + version + " (max supported: " + CURRENT_VERSION + ")");
    this.fileVersion = version;

    final int colCount = headerBuf.getShort();
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

      // Read tag metadata section (version 1+)
      String[][] blockTagDistinctValues = null;
      long tagEndPos = statsPos;
      if (version >= 1) {
        final ByteBuffer tagCountBuf = ByteBuffer.allocate(2);
        if (indexChannel.read(tagCountBuf, tagEndPos) < 2)
          break;
        tagCountBuf.flip();
        final short tagColCount = tagCountBuf.getShort();
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
            final short distinctCount = dcBuf.getShort();
            tagEndPos += 2;

            blockTagDistinctValues[c] = new String[distinctCount];
            for (int v = 0; v < distinctCount; v++) {
              final ByteBuffer lenBuf = ByteBuffer.allocate(2);
              indexChannel.read(lenBuf, tagEndPos);
              lenBuf.flip();
              final short valLen = lenBuf.getShort();
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

    return GorillaXORCodec.decode(compressed);
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
  private double[] decompressDoubleColumnFromBytes(final byte[] compressed, final int schemaColIdx) {
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

    return GorillaXORCodec.decode(compressed);
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
