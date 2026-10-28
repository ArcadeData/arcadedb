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
package com.arcadedb.index.columnar;

import java.util.Arrays;

/**
 * Immutable rows of ONE bucket page whose live value lives in the overlay: the slots changed by an apply pass, or changed by an earlier
 * pass and still current. Slots unchanged against the segments are not stored. Rows are ordered by slot; row {@code r} is the
 * {@code r}-th set bit of {@link #liveStripe()}.
 * <p>
 * Never mutated after {@link #build} (R13): a published overlay view shares the block by reference with every snapshot that holds
 * it, so a change to the page (an apply pass, a dictionary remap) builds a NEW block. Safe to read from any thread once published.
 */
final class OverlayPageBlock {
  private final int                page;
  private final int                maxRecordsInPage;
  private final ColumnDefinition[] columns;
  private final long               pageVersion;
  private final int                rowCount;
  private final long[]             liveStripe;
  private final long[][]           presence;    // per column, bit r = row r has a value
  private final long[][]           scalars;     // per column: ORDINAL as int, LONG as is, DOUBLE as raw bits; null for lists
  private final int[][]            listOffsets; // per ORDINAL_LIST column, rowCount + 1 offsets into listValues
  private final int[][]            listValues;
  private final long[]             min;         // per column, for skipping: see ColumnarSegmentView.minLong
  private final long[]             max;

  private OverlayPageBlock(final int page, final int maxRecordsInPage, final ColumnDefinition[] columns, final long pageVersion,
      final int rowCount, final long[] liveStripe) {
    this.page = page;
    this.maxRecordsInPage = maxRecordsInPage;
    this.columns = columns;
    this.pageVersion = pageVersion;
    this.rowCount = rowCount;
    this.liveStripe = liveStripe;
    final int n = columns.length;
    presence = new long[n][(rowCount + 63) >>> 6];
    scalars = new long[n][];
    listOffsets = new int[n][];
    listValues = new int[n][];
    min = new long[n];
    max = new long[n];
  }

  /**
   * Copies the staged rows into a new block. {@code rows} null or empty gives an empty block, which removes the page from the overlay.
   *
   * @throws IllegalArgumentException when {@code rows} was staged for another page size or other columns
   */
  static OverlayPageBlock build(final int page, final int maxRecordsInPage, final ColumnDefinition[] columns, final long pageVersion,
      final RowBuffer rows) {
    if (page < 0)
      throw new IllegalArgumentException("Invalid page " + page);
    if (rows != null && (rows.maxRecordsInPage() != maxRecordsInPage || !Arrays.equals(rows.columnsView(), columns)))
      throw new IllegalArgumentException("Rows staged for " + rows.maxRecordsInPage() + " slots per page and columns " + Arrays.toString(
          rows.columnsView()) + " do not fit a block of " + maxRecordsInPage + " slots and columns " + Arrays.toString(columns));
    final long[] stripe = rows == null ? new long[maxRecordsInPage / 64] : rows.stagedView().clone();
    final int rowCount = rows == null ? 0 : rows.size();
    final OverlayPageBlock block = new OverlayPageBlock(page, maxRecordsInPage, columns.clone(), pageVersion, rowCount, stripe);
    for (int k = 0; k < columns.length; k++)
      block.copyColumn(k, rows);
    return block;
  }

  private void copyColumn(final int k, final RowBuffer rows) {
    final ColumnKind kind = columns[k].kind();
    final long[] bits = presence[k];
    long low = Long.MAX_VALUE;
    long high = Long.MIN_VALUE;
    double lowDouble = Double.POSITIVE_INFINITY;
    double highDouble = Double.NEGATIVE_INFINITY;
    int[] offsets = null;
    int[] values = null;
    int listSize = 0;
    long[] scalar = null;
    if (kind == ColumnKind.ORDINAL_LIST) {
      offsets = new int[rowCount + 1];
      int total = 0;
      if (rows != null)
        for (int w = 0; w < liveStripe.length; w++)
          for (long word = liveStripe[w]; word != 0; word &= word - 1)
            total += rows.listCount(k, (w << 6) + Long.numberOfTrailingZeros(word));
      values = new int[total];
    } else
      scalar = new long[rowCount];

    int row = 0;
    for (int w = 0; w < liveStripe.length; w++)
      for (long word = liveStripe[w]; word != 0; word &= word - 1, row++) {
        final int slot = (w << 6) + Long.numberOfTrailingZeros(word);
        final boolean has = rows.present(k, slot);
        if (has)
          bits[row >>> 6] |= 1L << row;
        if (kind == ColumnKind.ORDINAL_LIST) {
          offsets[row] = listSize;
          final int count = rows.listCount(k, slot);
          if (count > 0) {
            System.arraycopy(rows.list(k, slot), 0, values, listSize, count);
            low = Math.min(low, values[listSize]);
            high = Math.max(high, values[listSize + count - 1]);
            listSize += count;
          }
        } else {
          final long v = rows.scalar(k, slot);
          scalar[row] = v;
          if (!has)
            continue;
          if (kind == ColumnKind.DOUBLE) {
            // NaN is never a bound, as in SegmentWriter
            final double d = Double.longBitsToDouble(v);
            if (d < lowDouble)
              lowDouble = d;
            if (d > highDouble)
              highDouble = d;
          } else {
            low = Math.min(low, v);
            high = Math.max(high, v);
          }
        }
      }
    if (offsets != null)
      offsets[rowCount] = listSize;
    if (kind == ColumnKind.DOUBLE && lowDouble <= highDouble) {
      low = Double.doubleToLongBits(lowDouble);
      high = Double.doubleToLongBits(highDouble);
    }
    scalars[k] = scalar;
    listOffsets[k] = offsets;
    listValues[k] = values;
    min[k] = low;
    max[k] = high;
  }

  int page() {
    return page;
  }

  long pageVersion() {
    return pageVersion;
  }

  int rowCount() {
    return rowCount;
  }

  int maxRecordsInPage() {
    return maxRecordsInPage;
  }

  /** The block's columns; read-only. */
  ColumnDefinition[] columnsView() {
    return columns;
  }

  /** The stored slots ({@code maxRecordsInPage / 64} words), shared by reference with published views: never write it. */
  long[] liveStripe() {
    return liveStripe;
  }

  /** The row of the slot, -1 when the block does not store it. */
  int row(final int slot) {
    final int word = slot >>> 6;
    final long bits = liveStripe[word];
    if ((bits & (1L << slot)) == 0)
      return -1;
    int rank = Long.bitCount(bits & ((1L << slot) - 1));
    for (int w = 0; w < word; w++)
      rank += Long.bitCount(liveStripe[w]);
    return rank;
  }

  /** The bucket position of the row. */
  long position(final int row) {
    int remaining = row;
    for (int w = 0; w < liveStripe.length; w++) {
      final int bits = Long.bitCount(liveStripe[w]);
      if (remaining < bits) {
        long word = liveStripe[w];
        for (int i = 0; i < remaining; i++)
          word &= word - 1;
        return (long) page * maxRecordsInPage + (w << 6) + Long.numberOfTrailingZeros(word);
      }
      remaining -= bits;
    }
    throw new IndexOutOfBoundsException("Row " + row + " of a block of " + rowCount + " rows");
  }

  boolean present(final int k, final int row) {
    return (presence[k][row >>> 6] & (1L << row)) != 0;
  }

  /** ORDINAL as an int, LONG as is, DOUBLE as its raw long bits; 0 when the row has no value. */
  long scalar(final int k, final int row) {
    return scalars[k][row];
  }

  /** Copies the ordinals of an ORDINAL_LIST row into {@code out} (at most its length) and returns their count, 0 when missing. */
  int ordinals(final int k, final int row, final int[] out) {
    final int start = listOffsets[k][row];
    final int count = listOffsets[k][row + 1] - start;
    System.arraycopy(listValues[k], start, out, 0, Math.min(count, out.length));
    return count;
  }

  /** Visits the ordinals of an ORDINAL_LIST row, ascending. */
  void forEachListOrdinal(final int k, final int row, final long position, final OrdinalConsumer consumer) {
    final int[] values = listValues[k];
    for (int i = listOffsets[k][row], end = listOffsets[k][row + 1]; i < end; i++)
      consumer.accept(position, values[i]);
  }

  /** Fills {@code out} with the row's values, as a {@link SegmentWriter} row; list arrays of {@code out} are grown when too short. */
  void readRow(final int row, final RowValues out) {
    for (int k = 0; k < columns.length; k++) {
      final boolean has = present(k, row);
      out.present[k] = has;
      switch (columns[k].kind()) {
      case ORDINAL -> out.ordinal[k] = (int) scalars[k][row];
      case LONG -> out.longs[k] = scalars[k][row];
      case DOUBLE -> out.doubles[k] = Double.longBitsToDouble(scalars[k][row]);
      case ORDINAL_LIST -> {
        final int count = listOffsets[k][row + 1] - listOffsets[k][row];
        if (out.list[k] == null || out.list[k].length < count)
          out.list[k] = new int[Math.max(count, 4)];
        out.listCount[k] = ordinals(k, row, out.list[k]);
      }
      }
    }
  }

  long min(final int k) {
    return min[k];
  }

  long max(final int k) {
    return max[k];
  }
}
