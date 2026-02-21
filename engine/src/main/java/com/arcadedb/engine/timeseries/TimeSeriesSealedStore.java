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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Immutable columnar storage for compacted TimeSeries data.
 * Uses FileChannel positioned reads for zero-overhead access.
 * <p>
 * Index file (.ts.sealed) layout:
 * - [0..3]   magic "TSIX" (4 bytes)
 * - [4..5]   column count (short)
 * - [6..9]   block count (int)
 * - [10..17] global min timestamp (long)
 * - [18..25] global max timestamp (long)
 * - [26..]   block entries (inline metadata + compressed column data)
 * <p>
 * Block entry layout:
 * - magic "TSBL" (4), minTs (8), maxTs (8), sampleCount (4), colSizes (4*colCount)
 * - numericColCount (4), [colIdx (4) + min (8) + max (8) + sum (8)] * numericColCount
 * - compressed column data bytes
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesSealedStore implements AutoCloseable {

  private static final int MAGIC_VALUE       = 0x54534958; // "TSIX"
  private static final int BLOCK_MAGIC_VALUE = 0x5453424C; // "TSBL"
  private static final int HEADER_SIZE       = 26;
  private static final int MAX_BLOCK_SIZE    = 65536;

  private final String               basePath;
  private final List<ColumnDefinition> columns;
  private       RandomAccessFile     indexFile;
  private       FileChannel          indexChannel;

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
      final double[] columnMins, final double[] columnMaxs, final double[] columnSums) throws IOException {
    final int colCount = columns.size();

    // Count numeric columns (those with non-NaN stats)
    int numericColCount = 0;
    for (int c = 0; c < colCount; c++)
      if (!Double.isNaN(columnMins[c]))
        numericColCount++;

    // Block header: magic(4) + minTs(8) + maxTs(8) + sampleCount(4) + colSizes(4*colCount)
    //              + numericColCount(4) + [colIdx(4) + min(8) + max(8) + sum(8)] * numericColCount
    final int statsSize = 4 + (4 + 8 + 8 + 8) * numericColCount;
    final int metaSize = 4 + 8 + 8 + 4 + 4 * colCount + statsSize;
    final ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
    metaBuf.putInt(BLOCK_MAGIC_VALUE);
    metaBuf.putLong(minTs);
    metaBuf.putLong(maxTs);
    metaBuf.putInt(sampleCount);
    for (final byte[] col : compressedColumns)
      metaBuf.putInt(col.length);

    // Write stats section
    metaBuf.putInt(numericColCount);
    for (int c = 0; c < colCount; c++) {
      if (!Double.isNaN(columnMins[c])) {
        metaBuf.putInt(c);
        metaBuf.putDouble(columnMins[c]);
        metaBuf.putDouble(columnMaxs[c]);
        metaBuf.putDouble(columnSums[c]);
      }
    }
    metaBuf.flip();

    long offset = indexFile.length();
    indexFile.seek(offset);
    indexFile.write(metaBuf.array());
    offset += metaSize;

    final BlockEntry entry = new BlockEntry(minTs, maxTs, sampleCount, colCount, columnMins, columnMaxs, columnSums);
    // Write compressed column data
    for (int c = 0; c < colCount; c++) {
      entry.columnOffsets[c] = offset;
      entry.columnSizes[c] = compressedColumns[c].length;
      indexFile.write(compressedColumns[c]);
      offset += compressedColumns[c].length;
    }

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
  public List<Object[]> scanRange(final long fromTs, final long toTs, final int[] columnIndices) throws IOException {
    final List<Object[]> results = new ArrayList<>();

    for (final BlockEntry entry : blockDirectory) {
      if (entry.maxTimestamp < fromTs || entry.minTimestamp > toTs)
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
  public Iterator<Object[]> iterateRange(final long fromTs, final long toTs, final int[] columnIndices) throws IOException {
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
      final AggregationType type, final long bucketIntervalNs) throws IOException {
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

        final long bucketTs = bucketIntervalNs > 0 ? (timestamps[i] / bucketIntervalNs) * bucketIntervalNs : fromTs;

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
      final MultiColumnAggregationResult result, final AggregationMetrics metrics) throws IOException {
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

      // Check if entire block falls within a single time bucket and is fully inside the query range
      if (bucketIntervalMs > 0 && entry.minTimestamp >= fromTs && entry.maxTimestamp <= toTs) {
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

      // Use tsCount (not array length) since reusableTsBuf may be larger than actual data
      final long[] timestamps = reusableTsBuf;

      // Aggregate using segment-based vectorized accumulation
      t0 = metrics != null ? System.nanoTime() : 0;

      if (bucketIntervalMs > 0) {
        // Vectorized path: find contiguous segments within each bucket and use SIMD ops
        final TimeSeriesVectorOps ops = TimeSeriesVectorOpsProvider.getInstance();
        final int tsLen = timestamps.length;

        // Clip to query range using binary search on sorted timestamps
        final int rangeStart = lowerBound(timestamps, 0, tsCount, fromTs);
        final int rangeEnd = upperBound(timestamps, 0, tsCount, toTs);

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
      } else {
        // No bucket interval — accumulate all into one bucket
        for (int i = 0; i < tsCount; i++) {
          final long ts = timestamps[i];
          if (ts < fromTs || ts > toTs)
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
    blockDirectory.clear();
    final String tempPath = basePath + ".ts.sealed.tmp";
    try (final RandomAccessFile tempFile = new RandomAccessFile(tempPath, "rw")) {
      // Write empty header first
      final ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
      headerBuf.putInt(MAGIC_VALUE);
      headerBuf.putShort((short) columns.size());
      headerBuf.putInt(0);
      headerBuf.putLong(Long.MAX_VALUE);
      headerBuf.putLong(Long.MIN_VALUE);
      headerBuf.flip();
      tempFile.getChannel().write(headerBuf);

      globalMinTs = Long.MAX_VALUE;
      globalMaxTs = Long.MIN_VALUE;

      final int colCount = columns.size();
      for (final BlockEntry oldEntry : retained) {
        // Read compressed data from old file
        final byte[][] compressedCols = new byte[colCount][];
        for (int c = 0; c < colCount; c++)
          compressedCols[c] = readBytes(oldEntry.columnOffsets[c], oldEntry.columnSizes[c]);

        long dataOffset = tempFile.length();
        tempFile.seek(dataOffset);

        // Write block header with stats
        int numericColCount = 0;
        for (int c = 0; c < colCount; c++)
          if (!Double.isNaN(oldEntry.columnMins[c]))
            numericColCount++;

        final int statsSize = 4 + (4 + 8 + 8 + 8) * numericColCount;
        final int metaSize = 4 + 8 + 8 + 4 + 4 * colCount + statsSize;
        final ByteBuffer metaBuf = ByteBuffer.allocate(metaSize);
        metaBuf.putInt(BLOCK_MAGIC_VALUE);
        metaBuf.putLong(oldEntry.minTimestamp);
        metaBuf.putLong(oldEntry.maxTimestamp);
        metaBuf.putInt(oldEntry.sampleCount);
        for (final byte[] col : compressedCols)
          metaBuf.putInt(col.length);
        metaBuf.putInt(numericColCount);
        for (int c = 0; c < colCount; c++) {
          if (!Double.isNaN(oldEntry.columnMins[c])) {
            metaBuf.putInt(c);
            metaBuf.putDouble(oldEntry.columnMins[c]);
            metaBuf.putDouble(oldEntry.columnMaxs[c]);
            metaBuf.putDouble(oldEntry.columnSums[c]);
          }
        }
        metaBuf.flip();
        tempFile.write(metaBuf.array());
        dataOffset += metaSize;

        // Write compressed column data
        final BlockEntry newEntry = new BlockEntry(oldEntry.minTimestamp, oldEntry.maxTimestamp,
            oldEntry.sampleCount, colCount, oldEntry.columnMins, oldEntry.columnMaxs, oldEntry.columnSums);
        for (int c = 0; c < colCount; c++) {
          newEntry.columnOffsets[c] = dataOffset;
          newEntry.columnSizes[c] = compressedCols[c].length;
          tempFile.write(compressedCols[c]);
          dataOffset += compressedCols[c].length;
        }
        blockDirectory.add(newEntry);

        if (oldEntry.minTimestamp < globalMinTs)
          globalMinTs = oldEntry.minTimestamp;
        if (oldEntry.maxTimestamp > globalMaxTs)
          globalMaxTs = oldEntry.maxTimestamp;
      }
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

  private void writeEmptyHeader() throws IOException {
    final ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
    buf.putInt(MAGIC_VALUE);
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

      // Read stats section: numericColCount(4) + [colIdx(4) + min(8) + max(8) + sum(8)] * numericColCount
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
        final int tripletSize = (4 + 8 + 8 + 8) * numericColCount;
        final ByteBuffer statsBuf = ByteBuffer.allocate(tripletSize);
        if (indexChannel.read(statsBuf, statsPos) < tripletSize)
          break;
        statsBuf.flip();
        for (int n = 0; n < numericColCount; n++) {
          final int colIdx = statsBuf.getInt();
          mins[colIdx] = statsBuf.getDouble();
          maxs[colIdx] = statsBuf.getDouble();
          sums[colIdx] = statsBuf.getDouble();
        }
        statsPos += tripletSize;
      }

      final BlockEntry entry = new BlockEntry(minTs, maxTs, sampleCount, colCount, mins, maxs, sums);
      long dataPos = statsPos;
      for (int c = 0; c < colCount; c++) {
        entry.columnOffsets[c] = dataPos;
        entry.columnSizes[c] = colSizes[c];
        dataPos += colSizes[c];
      }
      blockDirectory.add(entry);
      pos = dataPos;
    }
  }

  private long[] decompressTimestamps(final BlockEntry entry, final int tsColIdx) throws IOException {
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
    final long startOffset = entry.columnOffsets[0];
    int totalSize = 0;
    for (final int size : entry.columnSizes)
      totalSize += size;
    return readBytes(startOffset, totalSize);
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
