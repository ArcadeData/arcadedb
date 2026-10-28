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

import com.arcadedb.engine.block.BlockHeader;
import com.arcadedb.engine.codec.BitPackedCodec;
import com.arcadedb.engine.codec.NumericCodecChooser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.zip.CRC32;

import static com.arcadedb.index.columnar.SegmentFormat.*;

/**
 * Writes one immutable segment file (layout in {@link SegmentFormat}). Rows arrive in strictly ascending position order; one row block
 * is buffered in primitive arrays and encoded and written when full, so memory is bounded by {@code blockRows} plus the presence
 * stripes of the covered pages. {@link #finish()} appends presence, page versions and the footer, rewrites the file header with the
 * final row count and fsyncs. Closing a writer that did not finish deletes the partial file. Not thread safe.
 */
final class SegmentWriter implements AutoCloseable {
  private static final byte[] EMPTY = new byte[0];

  private final Path               file;
  private final long               segmentId;
  private final int                maxRecordsInPage;
  private final int                wordsPerStripe;
  private final ColumnDefinition[] columns;
  private final ColumnKind[]       kinds;
  private final int                blockRows;
  private final byte[]             definitions;
  private final FileChannel        channel;
  private long                     writeOffset;

  // current row block, one slot per column
  private final byte[][]           valuePresence;
  private final int[]              presentInBlock;
  private final long[][]           values;
  private final long[][]           listValues;
  private final int[]              listSize;
  private final long[][]           listOffsets;
  private final long[]             blockFiller;
  private int                      rowsInBlock;

  // segment-wide state
  private final long[]             min;
  private final long[]             max;
  private final double[]           minDouble;
  private final double[]           maxDouble;
  private long                     rowCount;
  private long                     lastPosition = -1;
  private int[]                    pageNumbers  = new int[16];
  private int[]                    pageRowStart = new int[16];
  private long[]                   pageVersions = new long[16];
  private long[]                   stripes;
  private int                      pageCount;

  private int[]                    directory    = new int[64];
  private int                      directoryEntries;
  private boolean                  finished;
  private boolean                  closed;

  /**
   * @param maxRecordsInPage slots per page of the bucket, a positive multiple of 64
   * @param blockRows        rows per row block
   */
  SegmentWriter(final Path file, final long segmentId, final int maxRecordsInPage, final ColumnDefinition[] columns, final int blockRows)
      throws IOException {
    if (maxRecordsInPage <= 0 || maxRecordsInPage % 64 != 0)
      throw new IllegalArgumentException("maxRecordsInPage must be a positive multiple of 64, found " + maxRecordsInPage);
    if (blockRows <= 0)
      throw new IllegalArgumentException("blockRows must be positive, found " + blockRows);
    this.file = file;
    this.segmentId = segmentId;
    this.maxRecordsInPage = maxRecordsInPage;
    this.wordsPerStripe = maxRecordsInPage / 64;
    this.columns = columns.clone();
    this.blockRows = blockRows;
    this.stripes = new long[16 * wordsPerStripe];

    final int n = columns.length;
    kinds = new ColumnKind[n];
    valuePresence = new byte[n][];
    presentInBlock = new int[n];
    values = new long[n][];
    listValues = new long[n][];
    listSize = new int[n];
    listOffsets = new long[n][];
    blockFiller = new long[n];
    min = new long[n];
    max = new long[n];
    minDouble = new double[n];
    maxDouble = new double[n];
    final JSONArray json = new JSONArray();
    for (int k = 0; k < n; k++) {
      kinds[k] = columns[k].kind();
      json.put(columns[k].toJSON());
      valuePresence[k] = new byte[(blockRows + 7) >>> 3];
      if (kinds[k] == ColumnKind.ORDINAL_LIST) {
        listValues[k] = new long[Math.min(blockRows, 1024)];
        listOffsets[k] = new long[blockRows + 1];
      } else
        values[k] = new long[blockRows];
      min[k] = Long.MAX_VALUE;
      max[k] = Long.MIN_VALUE;
      minDouble[k] = Double.POSITIVE_INFINITY;
      maxDouble[k] = Double.NEGATIVE_INFINITY;
    }
    this.definitions = new JSONObject().put("columns", json).toString().getBytes(StandardCharsets.UTF_8);

    this.channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    try {
      writeOffset = writeFileHeader();
    } catch (final IOException | RuntimeException e) {
      close();
      throw e;
    }
  }

  /**
   * Appends the row of the position. Every row of a page should carry the same page version; the segment records the highest.
   *
   * @throws IllegalStateException    when the position is not above the previous one, or the writer is finished or closed
   * @throws IllegalArgumentException when a value is invalid (negative ordinal, list not strictly ascending)
   */
  void addRow(final long position, final long pageVersion, final RowValues row) throws IOException {
    if (finished || closed)
      throw new IllegalStateException("Segment writer of " + file + " is " + (finished ? "finished" : "closed"));
    if (position <= lastPosition)
      throw new IllegalStateException("Positions must be strictly ascending: " + position + " after " + lastPosition);
    if (position < 0 || position / maxRecordsInPage > Integer.MAX_VALUE)
      throw new IllegalArgumentException("Invalid position " + position);
    if (rowCount == Integer.MAX_VALUE)
      throw new IllegalStateException("Segment " + file + " is full: " + rowCount + " rows");
    for (int k = 0; k < columns.length; k++)
      row.check(k, columns[k]);

    final int page = (int) (position / maxRecordsInPage);
    final int slot = (int) (position % maxRecordsInPage);
    if (pageCount == 0 || pageNumbers[pageCount - 1] != page)
      appendPage(page, pageVersion);
    else if (pageVersion > pageVersions[pageCount - 1])
      pageVersions[pageCount - 1] = pageVersion;
    stripes[(pageCount - 1) * wordsPerStripe + (slot >>> 6)] |= 1L << slot;

    for (int k = 0; k < columns.length; k++)
      storeValue(k, row);
    lastPosition = position;
    rowCount++;
    if (++rowsInBlock == blockRows)
      writeRowBlock();
  }

  /** Writes the remaining sections, rewrites the file header with the row count, fsyncs and closes the file. */
  SegmentReader.Meta finish() throws IOException {
    if (finished || closed)
      throw new IllegalStateException("Segment writer of " + file + " is " + (finished ? "finished" : "closed"));
    if (rowsInBlock > 0)
      writeRowBlock();
    for (int from = 0; from < pageCount; from += PAGES_PER_PRESENCE_BLOCK)
      writePresenceBlocks(from, Math.min(pageCount, from + PAGES_PER_PRESENCE_BLOCK));

    final int minPage = pageCount == 0 ? -1 : pageNumbers[0];
    final int maxPage = pageCount == 0 ? -1 : pageNumbers[pageCount - 1];
    final ByteBuffer footer = ByteBuffer.allocate(FOOTER_FIXED_BYTES + directoryEntries * DIRECTORY_ENTRY + columns.length * 16);
    footer.putInt(minPage).putInt(maxPage).putInt(directoryEntries);
    for (int i = 0; i < directoryEntries * 4; i++)
      footer.putInt(directory[i]);
    for (int k = 0; k < columns.length; k++) {
      if (kinds[k] == ColumnKind.DOUBLE && minDouble[k] <= maxDouble[k])
        footer.putLong(Double.doubleToLongBits(minDouble[k])).putLong(Double.doubleToLongBits(maxDouble[k]));
      else
        footer.putLong(min[k]).putLong(max[k]);
    }
    final int footerOffset = checkedOffset(writeOffset);
    final CRC32 crc = new CRC32();
    crc.update(footer.array());
    final ByteBuffer trailer = ByteBuffer.allocate(TRAILER_BYTES).putInt(footerOffset).putInt((int) crc.getValue()).putInt(FILE_MAGIC);
    write(footer.flip());
    write(trailer.flip());
    checkedOffset(writeOffset);

    writeFileHeader();
    channel.force(true);
    channel.close();
    finished = true;
    return new SegmentReader.Meta(segmentId, rowCount, minPage, maxPage);
  }

  /** Closes the file; a writer that did not {@link #finish()} deletes its partial file. */
  @Override
  public void close() throws IOException {
    if (closed || finished)
      return;
    closed = true;
    channel.close();
    Files.deleteIfExists(file);
  }

  private void storeValue(final int k, final RowValues row) {
    final int r = rowsInBlock;
    final boolean present = row.present[k];
    if (present) {
      valuePresence[k][r >>> 3] |= (byte) (1 << (r & 7));
      presentInBlock[k]++;
    }
    switch (kinds[k]) {
    case ORDINAL, LONG -> {
      final long v = kinds[k] == ColumnKind.ORDINAL ? row.ordinal[k] : row.longs[k];
      if (present) {
        values[k][r] = v;
        trackRange(k, v, v);
      }
    }
    case DOUBLE -> {
      final double v = row.doubles[k];
      values[k][r] = present ? Double.doubleToRawLongBits(v) : 0L;
      if (present) {
        if (v < minDouble[k])
          minDouble[k] = v;
        if (v > maxDouble[k])
          maxDouble[k] = v;
      }
    }
    case ORDINAL_LIST -> {
      int size = listSize[k];
      listOffsets[k][r] = size;
      if (present && row.listCount[k] > 0) {
        final int count = row.listCount[k];
        if (size + count > listValues[k].length)
          listValues[k] = Arrays.copyOf(listValues[k], Math.max(size + count, listValues[k].length * 2));
        final int[] list = row.list[k];
        for (int i = 0; i < count; i++)
          listValues[k][size++] = list[i];
        trackRange(k, list[0], list[count - 1]);
      }
      listSize[k] = size;
      listOffsets[k][r + 1] = size;
    }
    }
  }

  private void trackRange(final int k, final long low, final long high) {
    if (presentInBlock[k] == 1)
      blockFiller[k] = low;
    if (low < min[k])
      min[k] = low;
    if (high > max[k])
      max[k] = high;
  }

  private void appendPage(final int page, final long pageVersion) {
    if (pageCount == pageNumbers.length) {
      final int capacity = pageCount * 2;
      pageNumbers = Arrays.copyOf(pageNumbers, capacity);
      pageRowStart = Arrays.copyOf(pageRowStart, capacity);
      pageVersions = Arrays.copyOf(pageVersions, capacity);
      stripes = Arrays.copyOf(stripes, capacity * wordsPerStripe);
    }
    pageNumbers[pageCount] = page;
    pageRowStart[pageCount] = (int) rowCount;
    pageVersions[pageCount] = pageVersion;
    pageCount++;
  }

  private void writeRowBlock() throws IOException {
    final int rows = rowsInBlock;
    final byte[][] sub = new byte[SUB_COLUMNS * columns.length][];
    for (int k = 0; k < columns.length; k++) {
      final boolean allPresent = presentInBlock[k] == rows;
      // all present is the common case and costs no bytes
      sub[k * SUB_COLUMNS] = allPresent ? EMPTY : Arrays.copyOf(valuePresence[k], (rows + 7) >>> 3);
      switch (kinds[k]) {
      case ORDINAL, LONG -> {
        final long[] v = values[k];
        if (!allPresent) {
          // a missing row takes a present value of the block, so it never widens the codec's range
          final byte[] bits = valuePresence[k];
          for (int r = 0; r < rows; r++)
            if ((bits[r >>> 3] & (1 << (r & 7))) == 0)
              v[r] = blockFiller[k];
        }
        sub[k * SUB_COLUMNS + 1] = NumericCodecChooser.encodeBest(v, 0, rows);
        sub[k * SUB_COLUMNS + 2] = EMPTY;
      }
      case DOUBLE -> {
        final ByteBuffer raw = ByteBuffer.allocate(rows * 8);
        for (int r = 0; r < rows; r++)
          raw.putLong(values[k][r]);
        sub[k * SUB_COLUMNS + 1] = raw.array();
        sub[k * SUB_COLUMNS + 2] = EMPTY;
      }
      case ORDINAL_LIST -> {
        sub[k * SUB_COLUMNS + 1] = BitPackedCodec.encode(listValues[k], 0, listSize[k]);
        sub[k * SUB_COLUMNS + 2] = NumericCodecChooser.encodeBest(listOffsets[k], 0, rows + 1);
        listSize[k] = 0;
      }
      }
      Arrays.fill(valuePresence[k], (byte) 0);
      presentInBlock[k] = 0;
    }
    writeBlock(KIND_ROWS, (int) (rowCount - rows), BlockHeader.forColumns(BLOCK_MAGIC, VERSION, rows, sub), sub);
    rowsInBlock = 0;
  }

  private void writePresenceBlocks(final int from, final int to) throws IOException {
    final int n = to - from;
    final ByteBuffer pages = ByteBuffer.allocate(n * 4);
    final ByteBuffer rowStarts = ByteBuffer.allocate(n * 4);
    final ByteBuffer words = ByteBuffer.allocate(n * wordsPerStripe * 8);
    final ByteBuffer versions = ByteBuffer.allocate(n * 8);
    for (int i = from; i < to; i++) {
      pages.putInt(pageNumbers[i]);
      rowStarts.putInt(pageRowStart[i]);
      versions.putLong(pageVersions[i]);
    }
    for (int w = from * wordsPerStripe; w < to * wordsPerStripe; w++)
      words.putLong(stripes[w]);
    final byte[][] presence = { pages.array(), rowStarts.array(), words.array() };
    writeBlock(KIND_PRESENCE, pageNumbers[from], BlockHeader.forColumns(BLOCK_MAGIC, VERSION, n, presence), presence);
    final byte[][] version = { versions.array() };
    writeBlock(KIND_PAGE_VERSIONS, pageNumbers[from], BlockHeader.forColumns(BLOCK_MAGIC, VERSION, n, version), version);
  }

  private void writeBlock(final int kind, final int first, final BlockHeader header, final byte[][] payload) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(header.totalSize());
    header.write(buffer);
    for (final byte[] column : payload)
      buffer.put(column);
    final int offset = checkedOffset(writeOffset);
    write(buffer.flip());
    if (directoryEntries * 4 == directory.length)
      directory = Arrays.copyOf(directory, directory.length * 2);
    final int at = directoryEntries++ * 4;
    directory[at] = kind;
    directory[at + 1] = first;
    directory[at + 2] = offset;
    directory[at + 3] = header.totalSize();
  }

  /** Writes the file header block at offset 0 and returns its size, the same before and after the row count is known. */
  private int writeFileHeader() throws IOException {
    final byte[] meta = ByteBuffer.allocate(HEADER_META_BYTES).putLong(segmentId).putInt(maxRecordsInPage).putInt(blockRows)
        .putLong(rowCount).array();
    final BlockHeader header = BlockHeader.forColumns(FILE_MAGIC, VERSION, (int) rowCount, meta, definitions);
    final ByteBuffer buffer = ByteBuffer.allocate(header.totalSize());
    header.write(buffer);
    buffer.put(meta).put(definitions).flip();
    long position = 0;
    while (buffer.hasRemaining())
      position += channel.write(buffer, position);
    return header.totalSize();
  }

  private void write(final ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining())
      writeOffset += channel.write(buffer, writeOffset);
  }

  private int checkedOffset(final long offset) throws IOException {
    if (offset > Integer.MAX_VALUE)
      throw new IOException("Segment file " + file + " exceeds " + Integer.MAX_VALUE + " bytes");
    return (int) offset;
  }
}
