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
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * The in-memory newest segment of one bucket's projection: one {@link OverlayPageBlock} per page whose live value is in the overlay,
 * sorted by page. Owned by the apply thread and not thread safe; readers only ever see {@link #publish() published} views, which are
 * immutable and share the blocks by reference (blocks are never mutated, R13). Within one page a position appears once, so at most
 * one live value per position holds across segments and overlay (spec 5.2).
 */
final class ColumnarOverlay {
  private static final int[]              NO_PAGES  = new int[0];
  private static final OverlayPageBlock[] NO_BLOCKS = new OverlayPageBlock[0];

  private final int                  maxRecordsInPage;
  private final ColumnDefinition[]   columns;
  private final Map<String, Integer> columnIndex;
  private final LongSupplier         clockMs;
  private int[]                      pages  = NO_PAGES;
  private OverlayPageBlock[]         blocks = NO_BLOCKS;
  private int                        size;
  private long                       rows;
  private long                       oldestRowMs;

  /**
   * @param maxRecordsInPage slots per page of the bucket, as every block must have
   * @param columns          the projection's columns, as every block must have
   * @param clockMs          the clock of {@link #oldestRowMs()}, {@code System::currentTimeMillis} in production
   */
  ColumnarOverlay(final int maxRecordsInPage, final ColumnDefinition[] columns, final LongSupplier clockMs) {
    this(maxRecordsInPage, columns.clone(), indexOf(columns), clockMs);
  }

  private ColumnarOverlay(final int maxRecordsInPage, final ColumnDefinition[] columns, final Map<String, Integer> columnIndex,
      final LongSupplier clockMs) {
    if (maxRecordsInPage <= 0 || maxRecordsInPage % 64 != 0)
      throw new IllegalArgumentException("maxRecordsInPage must be a positive multiple of 64, found " + maxRecordsInPage);
    this.maxRecordsInPage = maxRecordsInPage;
    this.columns = columns;
    this.columnIndex = columnIndex;
    this.clockMs = clockMs;
  }

  private static Map<String, Integer> indexOf(final ColumnDefinition[] columns) {
    final Map<String, Integer> index = new HashMap<>(columns.length * 2);
    for (int k = 0; k < columns.length; k++)
      index.put(columns[k].property(), k);
    return index;
  }

  /**
   * Makes the block the page's whole overlay content, superseding the previous block of the page; an empty block removes the page.
   *
   * @throws IllegalArgumentException when the block was built for another page size or other columns
   */
  void replacePage(final OverlayPageBlock block) {
    if (block.maxRecordsInPage() != maxRecordsInPage || !Arrays.equals(block.columnsView(), columns))
      throw new IllegalArgumentException("Block of page " + block.page() + " was built for " + block.maxRecordsInPage()
          + " slots per page and other columns than this overlay (" + maxRecordsInPage + " slots per page)");
    final long rowsBefore = rows;
    final int found = Arrays.binarySearch(pages, 0, size, block.page());
    if (found >= 0) {
      rows -= blocks[found].rowCount();
      if (block.rowCount() == 0)
        removeAt(found);
      else
        blocks[found] = block;
    } else if (block.rowCount() > 0)
      insertAt(-found - 1, block);
    rows += block.rowCount();

    if (rows == 0)
      oldestRowMs = 0;
    else if (rowsBefore == 0)
      oldestRowMs = clockMs.getAsLong();
  }

  /** The page's block, null when the overlay does not hold the page. */
  OverlayPageBlock page(final int page) {
    final int found = Arrays.binarySearch(pages, 0, size, page);
    return found < 0 ? null : blocks[found];
  }

  /** The pages the overlay holds, ascending. */
  int[] pages() {
    return Arrays.copyOf(pages, size);
  }

  /** Rows held, over every page. */
  long rows() {
    return rows;
  }

  /**
   * Clock time at which the overlay last went from no row to some rows, so the age of its oldest row (P39); 0 while it holds no row.
   * A {@link #freeze()} restarts it: the frozen overlay keeps the time, this one is empty.
   */
  long oldestRowMs() {
    return oldestRowMs;
  }

  /** An immutable view of the current content: the sorted pages and the block references are copied, never the blocks. */
  ColumnarSegmentView publish() {
    return new View(maxRecordsInPage, columns, columnIndex, Arrays.copyOf(pages, size), Arrays.copyOf(blocks, size));
  }

  /**
   * Hands the current content to a flush and starts empty. The returned overlay holds the content and is not written again; its
   * published view stays in the projection state until the flushed segment replaces it.
   */
  ColumnarOverlay freeze() {
    final ColumnarOverlay frozen = new ColumnarOverlay(maxRecordsInPage, columns, columnIndex, clockMs);
    frozen.pages = pages;
    frozen.blocks = blocks;
    frozen.size = size;
    frozen.rows = rows;
    frozen.oldestRowMs = oldestRowMs;
    pages = NO_PAGES;
    blocks = NO_BLOCKS;
    size = 0;
    rows = 0;
    oldestRowMs = 0;
    return frozen;
  }

  private void insertAt(final int index, final OverlayPageBlock block) {
    if (size == pages.length) {
      final int capacity = Math.max(16, size + (size >> 1));
      pages = Arrays.copyOf(pages, capacity);
      blocks = Arrays.copyOf(blocks, capacity);
    }
    System.arraycopy(pages, index, pages, index + 1, size - index);
    System.arraycopy(blocks, index, blocks, index + 1, size - index);
    pages[index] = block.page();
    blocks[index] = block;
    size++;
  }

  private void removeAt(final int index) {
    System.arraycopy(pages, index + 1, pages, index, size - index - 1);
    System.arraycopy(blocks, index + 1, blocks, index, size - index - 1);
    size--;
    blocks[size] = null;
  }

  /** A published overlay: immutable, every field final, so safe to hand to any thread. */
  private static final class View implements ColumnarSegmentView {
    private final int                  maxRecordsInPage;
    private final ColumnDefinition[]   columns;
    private final Map<String, Integer> columnIndex;
    private final int[]                pages;
    private final OverlayPageBlock[]   blocks;
    private final Column[]             readers;
    private volatile PositionBitmap    live;
    private volatile long[]            bounds; // min of column k at 2k, max at 2k + 1

    private View(final int maxRecordsInPage, final ColumnDefinition[] columns, final Map<String, Integer> columnIndex,
        final int[] pages, final OverlayPageBlock[] blocks) {
      this.maxRecordsInPage = maxRecordsInPage;
      this.columns = columns;
      this.columnIndex = columnIndex;
      this.pages = pages;
      this.blocks = blocks;
      this.readers = new Column[columns.length];
      for (int k = 0; k < readers.length; k++)
        readers[k] = new Column(k);
    }

    /** Wraps the blocks' stripes without copying them (V-M7); computed once and cached. */
    @Override
    public PositionBitmap live() {
      PositionBitmap result = live;
      if (result == null) {
        final long[][] stripes = new long[blocks.length][];
        for (int i = 0; i < blocks.length; i++)
          stripes[i] = blocks[i].liveStripe();
        result = PositionBitmap.wrapShared(maxRecordsInPage, pages, stripes, pages.length);
        live = result;
      }
      return result;
    }

    @Override
    public PositionBitmap live(final int fromPage, final int toPageExclusive) {
      final PositionBitmap result = new PositionBitmap(maxRecordsInPage);
      int i = fromPage <= 0 ? 0 : Arrays.binarySearch(pages, fromPage);
      if (i < 0)
        i = -i - 1;
      for (; i < pages.length && pages[i] < toPageExclusive; i++)
        result.orStripe(pages[i], blocks[i].liveStripe(), 0);
      return result;
    }

    @Override
    public ColumnReader column(final String property) {
      final Integer k = columnIndex.get(property);
      return k == null ? null : readers[k];
    }

    @Override
    public long minLong(final String property) {
      return bounds()[2 * columnOf(property)];
    }

    @Override
    public long maxLong(final String property) {
      return bounds()[2 * columnOf(property) + 1];
    }

    private int columnOf(final String property) {
      final Integer k = columnIndex.get(property);
      if (k == null)
        throw new IllegalArgumentException("Overlay has no column '" + property + "'");
      return k;
    }

    private long[] bounds() {
      long[] result = bounds;
      if (result == null) {
        result = new long[2 * columns.length];
        for (int k = 0; k < columns.length; k++) {
          final boolean isDouble = columns[k].kind() == ColumnKind.DOUBLE;
          long low = Long.MAX_VALUE;
          long high = Long.MIN_VALUE;
          for (final OverlayPageBlock block : blocks) {
            final long blockLow = block.min(k);
            final long blockHigh = block.max(k);
            if (blockLow > blockHigh)
              continue; // no value in the block
            if (low > high) {
              low = blockLow;
              high = blockHigh;
            } else if (isDouble) {
              if (Double.longBitsToDouble(blockLow) < Double.longBitsToDouble(low))
                low = blockLow;
              if (Double.longBitsToDouble(blockHigh) > Double.longBitsToDouble(high))
                high = blockHigh;
            } else {
              low = Math.min(low, blockLow);
              high = Math.max(high, blockHigh);
            }
          }
          result[2 * k] = low;
          result[2 * k + 1] = high;
        }
        bounds = result;
      }
      return result;
    }

    /** The index of the block holding the position, -1 when the view has no row there. */
    private int blockOf(final long position) {
      if (position < 0)
        return -1;
      final long page = position / maxRecordsInPage;
      return page > Integer.MAX_VALUE ? -1 : Arrays.binarySearch(pages, (int) page);
    }

    private final class Column implements ColumnReader {
      private final int        k;
      private final ColumnKind kind;

      private Column(final int k) {
        this.k = k;
        this.kind = columns[k].kind();
      }

      @Override
      public ColumnKind kind() {
        return kind;
      }

      /** The block's row of the position packed with the block index ({@code index << 32 | row}), -1 when not stored. */
      private long locate(final long position) {
        final int index = blockOf(position);
        if (index < 0)
          return -1;
        final int row = blocks[index].row((int) (position % maxRecordsInPage));
        return row < 0 ? -1 : (long) index << 32 | row;
      }

      private long presentRow(final long position) {
        final long located = locate(position);
        return located >= 0 && blocks[(int) (located >>> 32)].present(k, (int) located) ? located : -1;
      }

      @Override
      public boolean isPresent(final long position) {
        return presentRow(position) >= 0;
      }

      @Override
      public int ordinal(final long position) {
        requireKind(ColumnKind.ORDINAL);
        final long located = presentRow(position);
        return located < 0 ? -1 : (int) blocks[(int) (located >>> 32)].scalar(k, (int) located);
      }

      @Override
      public int ordinals(final long position, final int[] out) {
        requireKind(ColumnKind.ORDINAL_LIST);
        final long located = presentRow(position);
        return located < 0 ? 0 : blocks[(int) (located >>> 32)].ordinals(k, (int) located, out);
      }

      @Override
      public long longValue(final long position) {
        requireKind(ColumnKind.LONG);
        final long located = presentRow(position);
        return located < 0 ? 0 : blocks[(int) (located >>> 32)].scalar(k, (int) located);
      }

      @Override
      public double doubleValue(final long position) {
        requireKind(ColumnKind.DOUBLE);
        final long located = presentRow(position);
        return located < 0 ? 0.0 : Double.longBitsToDouble(blocks[(int) (located >>> 32)].scalar(k, (int) located));
      }

      /** Visits {@code matches AND live()} page by page, only the view's pages, positions ascending (R12); allocates nothing. */
      @Override
      public void forEachOrdinal(final PositionBitmap matches, final OrdinalConsumer consumer) {
        if (kind != ColumnKind.ORDINAL && kind != ColumnKind.ORDINAL_LIST)
          throw new IllegalStateException("Column '" + columns[k].property() + "' of kind " + kind + " has no ordinals");
        if (matches.maxRecordsInPage() != maxRecordsInPage)
          throw new IllegalArgumentException(
              "Matches of " + matches.maxRecordsInPage() + " slots per page for an overlay of " + maxRecordsInPage);
        for (int i = 0; i < pages.length; i++) {
          final long[] matched = matches.stripe(pages[i]);
          if (matched == null)
            continue;
          final OverlayPageBlock block = blocks[i];
          final long[] stored = block.liveStripe();
          final long pageBase = (long) pages[i] * maxRecordsInPage;
          int row = 0;
          for (int w = 0; w < stored.length; w++) {
            long word = stored[w];
            final long hits = word & matched[w];
            if (hits == 0) {
              row += Long.bitCount(word);
              continue;
            }
            for (; word != 0; word &= word - 1, row++) {
              final int bit = Long.numberOfTrailingZeros(word);
              if ((hits & (1L << bit)) == 0 || !block.present(k, row))
                continue;
              final long position = pageBase + ((long) w << 6) + bit;
              if (kind == ColumnKind.ORDINAL)
                consumer.accept(position, (int) block.scalar(k, row));
              else
                block.forEachListOrdinal(k, row, position, consumer);
            }
          }
        }
      }

      private void requireKind(final ColumnKind expected) {
        if (kind != expected)
          throw new IllegalStateException("Column '" + columns[k].property() + "' is " + kind + ", not " + expected);
      }
    }
  }
}
