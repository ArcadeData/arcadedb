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
 * Per-page staging of {@link RowValues}: the apply pass puts the rows of one bucket page whose live value is in the overlay, in any slot
 * order, then {@link OverlayPageBlock#build} copies them into an immutable block. Storage is indexed by slot and reused across pages,
 * so a pass allocates nothing per row once the list arrays reached their size. Owned by one thread.
 */
final class RowBuffer {
  private final int                maxRecordsInPage;
  private final ColumnDefinition[] columns;
  private final long[]             staged;
  private final boolean[][]        present;
  private final long[][]           scalars;  // ORDINAL as int, LONG as is, DOUBLE as raw long bits
  private final int[][][]          lists;
  private final int[][]            listCounts;
  private int                      size;
  private RowValues                seedRow;

  RowBuffer(final int maxRecordsInPage, final ColumnDefinition[] columns) {
    if (maxRecordsInPage <= 0 || maxRecordsInPage % 64 != 0)
      throw new IllegalArgumentException("maxRecordsInPage must be a positive multiple of 64, found " + maxRecordsInPage);
    this.maxRecordsInPage = maxRecordsInPage;
    this.columns = columns.clone();
    this.staged = new long[maxRecordsInPage / 64];
    final int n = columns.length;
    present = new boolean[n][maxRecordsInPage];
    scalars = new long[n][];
    lists = new int[n][][];
    listCounts = new int[n][];
    for (int k = 0; k < n; k++)
      if (columns[k].kind() == ColumnKind.ORDINAL_LIST) {
        lists[k] = new int[maxRecordsInPage][];
        listCounts[k] = new int[maxRecordsInPage];
      } else
        scalars[k] = new long[maxRecordsInPage];
  }

  /**
   * Stages the row of the slot, replacing a row staged there before. The values are copied, so the caller may refill {@code row}.
   *
   * @throws IllegalArgumentException when the slot is outside the page or a value breaks the {@link RowValues#check rules} of its kind
   */
  void put(final int slot, final RowValues row) {
    if (slot < 0 || slot >= maxRecordsInPage)
      throw new IllegalArgumentException("Slot " + slot + " is outside a page of " + maxRecordsInPage + " records");
    for (int k = 0; k < columns.length; k++)
      row.check(k, columns[k]);
    for (int k = 0; k < columns.length; k++) {
      final boolean has = row.present[k];
      present[k][slot] = has;
      switch (columns[k].kind()) {
      case ORDINAL -> scalars[k][slot] = has ? row.ordinal[k] : 0;
      case LONG -> scalars[k][slot] = has ? row.longs[k] : 0;
      case DOUBLE -> scalars[k][slot] = has ? Double.doubleToRawLongBits(row.doubles[k]) : 0;
      case ORDINAL_LIST -> {
        final int count = has ? row.listCount[k] : 0;
        int[] list = lists[k][slot];
        if (count > 0 && (list == null || list.length < count))
          lists[k][slot] = list = new int[Math.max(count, 4)];
        if (count > 0)
          System.arraycopy(row.list[k], 0, list, 0, count);
        listCounts[k][slot] = count;
      }
      }
    }
    final long bit = 1L << slot;
    if ((staged[slot >>> 6] & bit) == 0) {
      staged[slot >>> 6] |= bit;
      size++;
    }
  }

  /**
   * Stages every row of the block, replacing what was staged: the apply pass starts a page from its current overlay content, because
   * {@link ColumnarOverlay#replacePage} makes the block it builds the page's WHOLE content.
   *
   * @throws IllegalArgumentException when the block has another page size or other columns
   */
  void seed(final OverlayPageBlock block) {
    if (block.maxRecordsInPage() != maxRecordsInPage || !Arrays.equals(block.columnsView(), columns))
      throw new IllegalArgumentException("Block of page " + block.page() + " does not fit rows staged for " + maxRecordsInPage
          + " slots per page and columns " + Arrays.toString(columns));
    clear();
    if (seedRow == null)
      seedRow = new RowValues(columns.length);
    final long[] stripe = block.liveStripe();
    int row = 0;
    for (int w = 0; w < stripe.length; w++)
      for (long word = stripe[w]; word != 0; word &= word - 1, row++) {
        block.readRow(row, seedRow);
        put((w << 6) + Long.numberOfTrailingZeros(word), seedRow);
      }
  }

  /** Forgets the row staged at the slot, if any. */
  void remove(final int slot) {
    final long bit = 1L << slot;
    if ((staged[slot >>> 6] & bit) != 0) {
      staged[slot >>> 6] &= ~bit;
      size--;
    }
  }

  /** Forgets every staged row; the storage is kept for the next page. */
  void clear() {
    Arrays.fill(staged, 0L);
    size = 0;
  }

  /** Rows staged. */
  int size() {
    return size;
  }

  int maxRecordsInPage() {
    return maxRecordsInPage;
  }

  ColumnDefinition[] columnsView() {
    return columns;
  }

  /** The staged slots, bit {@code slot & 63} of word {@code slot >>> 6}; read-only. */
  long[] stagedView() {
    return staged;
  }

  boolean present(final int k, final int slot) {
    return present[k][slot];
  }

  /** ORDINAL as an int, LONG as is, DOUBLE as its raw long bits; 0 when missing. */
  long scalar(final int k, final int slot) {
    return scalars[k][slot];
  }

  int listCount(final int k, final int slot) {
    return listCounts[k][slot];
  }

  /** The staged list of the slot, valid for its first {@link #listCount} entries; read-only. */
  int[] list(final int k, final int slot) {
    return lists[k][slot];
  }
}
