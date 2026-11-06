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

import com.arcadedb.engine.block.BlockCache;
import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.engine.block.BlockHeader;
import com.arcadedb.engine.codec.BitPackedCodec;
import com.arcadedb.engine.codec.LinearCodec;
import com.arcadedb.engine.codec.NumericCodecChooser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.arcadedb.index.columnar.SegmentFormat.*;

/**
 * Reads one segment file (layout in {@link SegmentFormat}) through one dead set. The file header and the footer are parsed and kept on
 * heap at open; every other block is read on a cache miss with a positional read on a channel opened for that read and closed after,
 * so no file handle is held per segment. Blocks go through the JVM-wide {@link BlockCache} under the {@link SegmentFile}'s cache file
 * id: presence blocks under column id {@code -1}, page versions under {@code -2}, the verified encoded sub-columns of column {@code k}
 * of a row block under {@code k}, and its decoded form under {@code k + 3 * columns}. Point reads never decode; a dense
 * {@link ColumnReader#forEachOrdinal} pass decodes a row block only when its matches exceed 1/8 of its rows and the encoding is wider
 * than 8 bits per value, so a narrow column is never blown up in the shared cache.
 * <p>
 * Thread safe: the state is immutable except the lazily cached {@link #live()} and {@link #deadCount()}. Every corrupt byte surfaces
 * as {@link BlockCorruptionException}.
 */
final class SegmentReader implements ColumnarSegmentView, AutoCloseable {
  private static final int       COLUMN_PRESENCE      = -1;
  private static final int       COLUMN_PAGE_VERSIONS = -2;
  private static final int       NARROW_BITS          = 8;
  private static final int       MIN_BATCH            = 64;
  private static final VarHandle LONG_BE              = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  /** What a finished writer reports, the manifest's view of a segment. */
  record Meta(long segmentId, long rows, int minPage, int maxPage) {
  }

  /** The parsed file header and footer, shared by every reader of the same file. */
  private record Layout(long segmentId, int maxRecordsInPage, int wordsPerStripe, int blockRows, int rowCount, ColumnDefinition[] columns,
                        Map<String, Integer> columnIndex, long[] min, long[] max, int minPage, int maxPage, int[] presenceFirstPage,
                        int[] presenceLocation, int[] versionsLocation, int[] rowsLocation) {
  }

  /** A block kept by the point-read memo, keyed like its cache entry. */
  private record MemoSlot(long fileId, int block, int column, Object value) {
    boolean holds(final long fileId, final int block, final int column) {
      return this.fileId == fileId && this.block == block && this.column == column;
    }
  }

  /**
   * Per-thread memo of the last presence block and the last row block a point read used, so consecutive point reads on the same
   * blocks make no cache call (no key, no loader allocated). Pinned memory is bounded by threads x 2 blocks, whatever the number of
   * open readers; a reader holds no block itself. Cache file ids are never reused, so a slot can never answer for another file.
   */
  private static final class PointMemo {
    MemoSlot presence;
    MemoSlot encoded;
  }

  private static final ThreadLocal<PointMemo> POINT_MEMO = ThreadLocal.withInitial(PointMemo::new);

  /** One presence block: covered pages, the row of the first position of each, and their stripes ({@code wordsPerStripe} each). */
  private record Presence(int[] pages, int[] rowStart, long[] stripes) {
    long bytes() {
      return 8L * pages.length + 8L * stripes.length + 48;
    }
  }

  private final SegmentFile        file;
  private final Layout             layout;
  private final PositionBitmap     dead;
  private final Column[]           readers;
  private final AtomicBoolean      closed    = new AtomicBoolean();
  private volatile PositionBitmap  live;
  private volatile long            deadCount = -1;

  private SegmentReader(final SegmentFile file, final Layout layout, final PositionBitmap dead) {
    if (dead != null && dead.maxRecordsInPage() != layout.maxRecordsInPage)
      throw new IllegalArgumentException(
          "Dead set of " + dead.maxRecordsInPage() + " slots per page for a segment of " + layout.maxRecordsInPage);
    this.file = file;
    this.layout = layout;
    this.dead = dead == null || dead.isEmpty() ? null : dead;
    this.readers = new Column[layout.columns.length];
    for (int k = 0; k < readers.length; k++)
      readers[k] = new Column(k);
  }

  /**
   * Opens the file with its own new {@link SegmentFile}, whose first reference the returned reader owns.
   *
   * @param dead positions of the segment that are dead, null for none; kept by the reader, so the caller must not write it afterwards
   *
   * @throws BlockCorruptionException when the file header, footer or trailer fails its magic, CRC or bounds checks
   */
  static SegmentReader open(final Path path, final PositionBitmap dead) throws IOException {
    final SegmentFile segmentFile = SegmentFile.open(path);
    try {
      return new SegmentReader(segmentFile, readLayout(path), dead);
    } catch (final IOException | RuntimeException e) {
      segmentFile.release();
      throw e;
    }
  }

  /** A reader of the same file through another dead set: the {@link SegmentFile} is retained and its cached blocks stay valid. */
  SegmentReader withDead(final PositionBitmap dead) {
    if (closed.get())
      throw new IllegalStateException("Segment reader of " + file.path() + " is closed");
    return new SegmentReader(file.retain(), layout, dead);
  }

  /**
   * A RENAME (spec 6.4): this reader when every column the segment stores has the property name {@code columns} gives its index file
   * id (or {@code columns} has no column of the same id and kind), otherwise a reader of the same file and dead set whose columns
   * answer {@link #column(String)} under the names of {@code columns}. The file is retained, the cached blocks stay valid; the
   * caller closes this reader when it no longer needs it.
   */
  SegmentReader withColumnNames(final ColumnDefinition[] columns) {
    ColumnDefinition[] renamed = null;
    for (int k = 0; k < layout.columns.length; k++) {
      final ColumnDefinition stored = layout.columns[k];
      for (final ColumnDefinition column : columns)
        if (column.indexFileId() == stored.indexFileId() && column.kind() == stored.kind() && !column.property().equals(stored.property())) {
          if (renamed == null)
            renamed = layout.columns.clone();
          renamed[k] = new ColumnDefinition(stored.indexFileId(), column.property(), stored.propertyType(), stored.kind(), stored.byItem(),
              stored.precision());
        }
    }
    if (renamed == null)
      return this;
    if (closed.get())
      throw new IllegalStateException("Segment reader of " + file.path() + " is closed");
    final Layout l = layout;
    final Layout relabeled = new Layout(l.segmentId, l.maxRecordsInPage, l.wordsPerStripe, l.blockRows, l.rowCount, renamed,
        columnIndexOf(renamed), l.min, l.max, l.minPage, l.maxPage, l.presenceFirstPage, l.presenceLocation, l.versionsLocation,
        l.rowsLocation);
    return new SegmentReader(file.retain(), relabeled, dead);
  }

  private static Map<String, Integer> columnIndexOf(final ColumnDefinition[] columns) {
    final Map<String, Integer> columnIndex = new HashMap<>();
    for (int k = 0; k < columns.length; k++)
      columnIndex.put(columns[k].property(), k);
    return columnIndex;
  }

  SegmentFile file() {
    return file;
  }

  /** The dead set the reader was opened with, null when no row is dead; shared, never write it (copy it to derive a new one). */
  PositionBitmap deadSet() {
    return dead;
  }

  long segmentId() {
    return layout.segmentId;
  }

  long rowCount() {
    return layout.rowCount;
  }

  /** Rows of the segment that are dead, counted once over the dead set's pages and cached. */
  long deadCount() {
    long count = deadCount;
    if (count < 0) {
      count = 0;
      if (dead != null)
        for (final int page : dead.pages()) {
          final int block = presenceBlockOf(page);
          if (block < 0)
            continue;
          final Presence presence = presence(block);
          final int index = Arrays.binarySearch(presence.pages, page);
          if (index < 0)
            continue;
          final long[] deadStripe = dead.stripe(page);
          final int base = index * layout.wordsPerStripe;
          for (int w = 0; w < layout.wordsPerStripe; w++)
            count += Long.bitCount(presence.stripes[base + w] & deadStripe[w]);
        }
      deadCount = count;
    }
    return count;
  }

  /** First covered page, -1 for an empty segment. */
  int minPage() {
    return layout.minPage;
  }

  /** Last covered page, -1 for an empty segment. */
  int maxPage() {
    return layout.maxPage;
  }

  /** The version recorded for the page, -1 when the segment does not cover it. */
  long pageVersion(final int page) {
    final int block = presenceBlockOf(page);
    if (block < 0)
      return -1;
    final Presence presence = presence(block);
    final int index = Arrays.binarySearch(presence.pages, page);
    if (index < 0)
      return -1;
    final long[] versions = BlockCache.instance()
        .get(file.cacheFileId(), block, COLUMN_PAGE_VERSIONS, () -> loadVersions(block), v -> 8L * v.length + 16);
    if (versions.length != presence.pages.length)
      throw new BlockCorruptionException(
          "Page versions block " + block + " of " + file.path() + " holds " + versions.length + " pages, presence " + presence.pages.length);
    return versions[index];
  }

  /** The row of the position (its rank in the segment's position order), -1 when the segment holds no row there. */
  int row(final long position) {
    if (position < 0)
      return -1;
    final long pageNumber = position / layout.maxRecordsInPage;
    if (pageNumber > layout.maxPage)
      return -1;
    final int page = (int) pageNumber;
    final int block = presenceBlockOf(page);
    if (block < 0)
      return -1;
    final Presence presence = pointPresence(block);
    final int index = Arrays.binarySearch(presence.pages, page);
    if (index < 0)
      return -1;
    final int slot = (int) (position % layout.maxRecordsInPage);
    final int base = index * layout.wordsPerStripe;
    final int word = slot >>> 6;
    final long bits = presence.stripes[base + word];
    if ((bits & (1L << slot)) == 0)
      return -1;
    long rank = presence.rowStart[index];
    for (int w = 0; w < word; w++)
      rank += Long.bitCount(presence.stripes[base + w]);
    rank += Long.bitCount(bits & ((1L << slot) - 1));
    if (rank >= layout.rowCount)
      throw new BlockCorruptionException("Row " + rank + " of " + file.path() + " is outside the " + layout.rowCount + " rows");
    return (int) rank;
  }

  @Override
  public PositionBitmap live() {
    PositionBitmap result = live;
    if (result == null) {
      result = live(Integer.MIN_VALUE, Integer.MAX_VALUE);
      live = result;
    }
    return result;
  }

  @Override
  public PositionBitmap live(final int fromPage, final int toPageExclusive) {
    final PositionBitmap result = new PositionBitmap(layout.maxRecordsInPage);
    if (layout.rowCount == 0 || fromPage >= toPageExclusive)
      return result;
    final int words = layout.wordsPerStripe;
    final long[] scratch = dead == null ? null : new long[words];
    final int[] firstPages = layout.presenceFirstPage;
    int block = presenceBlockOf(Math.max(fromPage, layout.minPage));
    for (; block >= 0 && block < firstPages.length && firstPages[block] < toPageExclusive; block++) {
      final Presence presence = presence(block);
      for (int i = 0; i < presence.pages.length; i++) {
        final int page = presence.pages[i];
        if (page < fromPage)
          continue;
        if (page >= toPageExclusive)
          break;
        final long[] deadStripe = dead == null ? null : dead.stripe(page);
        if (deadStripe == null)
          result.orStripe(page, presence.stripes, i * words);
        else {
          for (int w = 0; w < words; w++)
            scratch[w] = presence.stripes[i * words + w] & ~deadStripe[w];
          result.orStripe(page, scratch, 0);
        }
      }
    }
    return result;
  }

  @Override
  public ColumnReader column(final String property) {
    final Integer index = layout.columnIndex.get(property);
    return index == null ? null : readers[index];
  }

  /** The reader of the column of the index file id (stable across a property RENAME), null when the segment has no such column. */
  ColumnReader columnByIndexFileId(final int indexFileId) {
    final ColumnDefinition[] columns = layout.columns;
    for (int k = 0; k < columns.length; k++)
      if (columns[k].indexFileId() == indexFileId)
        return readers[k];
    return null;
  }

  @Override
  public long minLong(final String property) {
    return layout.min[columnOf(property)];
  }

  @Override
  public long maxLong(final String property) {
    return layout.max[columnOf(property)];
  }

  /** Releases this reader's {@link SegmentFile} reference, once. Never invalidates the cache: the file's blocks stay valid. */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true))
      file.release();
  }

  boolean liveMaterializedForTesting() {
    return live != null;
  }

  int blockCountForTesting(final int kind) {
    return location(kind).length / 2;
  }

  /** {offset, size} of the index-th block of the kind. */
  int[] blockLocationForTesting(final int kind, final int index) {
    final int[] location = location(kind);
    return new int[] { location[index * 2], location[index * 2 + 1] };
  }

  private int[] location(final int kind) {
    return switch (kind) {
      case KIND_PRESENCE -> layout.presenceLocation;
      case KIND_PAGE_VERSIONS -> layout.versionsLocation;
      default -> layout.rowsLocation;
    };
  }

  private int columnOf(final String property) {
    final Integer index = layout.columnIndex.get(property);
    if (index == null)
      throw new IllegalArgumentException("Segment " + file.path() + " has no column '" + property + "'");
    return index;
  }

  /** The presence block that may cover the page, -1 when the page is outside the segment. */
  private int presenceBlockOf(final int page) {
    if (layout.rowCount == 0 || page < layout.minPage || page > layout.maxPage)
      return -1;
    final int found = Arrays.binarySearch(layout.presenceFirstPage, page);
    return found >= 0 ? found : -found - 2;
  }

  /** The presence block for a point read, through the thread's {@link PointMemo}. */
  private Presence pointPresence(final int block) {
    final PointMemo memo = POINT_MEMO.get();
    final long fileId = file.cacheFileId();
    final MemoSlot last = memo.presence;
    if (last != null && last.holds(fileId, block, COLUMN_PRESENCE))
      return (Presence) last.value;
    final Presence presence = presence(block);
    memo.presence = new MemoSlot(fileId, block, COLUMN_PRESENCE, presence);
    return presence;
  }

  /** The row block of column k for a point read, through the thread's {@link PointMemo}. */
  private EncodedColumn pointEncoded(final int block, final int k) {
    final PointMemo memo = POINT_MEMO.get();
    final long fileId = file.cacheFileId();
    final MemoSlot last = memo.encoded;
    if (last != null && last.holds(fileId, block, k))
      return (EncodedColumn) last.value;
    final EncodedColumn column = encoded(block, k);
    memo.encoded = new MemoSlot(fileId, block, k, column);
    return column;
  }

  /** {fileId of the presence slot, fileId of the encoded slot} of the calling thread's memo, 0 for an empty slot. */
  static long[] pointMemoFileIdsForTesting() {
    final PointMemo memo = POINT_MEMO.get();
    return new long[] { memo.presence == null ? 0 : memo.presence.fileId, memo.encoded == null ? 0 : memo.encoded.fileId };
  }

  private Presence presence(final int block) {
    return BlockCache.instance().get(file.cacheFileId(), block, COLUMN_PRESENCE, () -> loadPresence(block), Presence::bytes);
  }

  private EncodedColumn encoded(final int block, final int k) {
    return BlockCache.instance().get(file.cacheFileId(), block, k, () -> loadEncoded(block, k), EncodedColumn::bytes);
  }

  private int[] decoded(final int block, final int k, final EncodedColumn encoded) {
    return BlockCache.instance()
        .get(file.cacheFileId(), block, k + SUB_COLUMNS * layout.columns.length, () -> encoded.decode(), d -> 4L * d.length + 16);
  }

  // ---------------------------------------------------------------------------------------------------------------- loading

  private Presence loadPresence(final int block) {
    final ByteBuffer buffer = readBlock(layout.presenceLocation, block);
    final BlockHeader header = verifiedHeader(buffer, layout.presenceLocation[block * 2 + 1], 3, -1);
    final int n = header.rowCount();
    final int words = layout.wordsPerStripe;
    if (n <= 0 || n > PAGES_PER_PRESENCE_BLOCK || header.columnSize(0) != n * 4 || header.columnSize(1) != n * 4
        || header.columnSize(2) != n * words * 8)
      throw corrupt("presence block " + block + " has inconsistent sizes");
    final int[] pages = new int[n];
    final int[] rowStart = new int[n];
    final long[] stripes = new long[n * words];
    final int pagesAt = header.columnOffset(0);
    final int rowsAt = header.columnOffset(1);
    final int stripesAt = header.columnOffset(2);
    for (int i = 0; i < n; i++) {
      pages[i] = buffer.getInt(pagesAt + 4 * i);
      rowStart[i] = buffer.getInt(rowsAt + 4 * i);
    }
    for (int w = 0; w < stripes.length; w++)
      stripes[w] = buffer.getLong(stripesAt + 8 * w);

    final int nextFirst = block + 1 < layout.presenceFirstPage.length ? layout.presenceFirstPage[block + 1] : layout.maxPage + 1;
    if (pages[0] != layout.presenceFirstPage[block] || pages[n - 1] >= nextFirst)
      throw corrupt("presence block " + block + " does not cover the pages its directory entry names");
    for (int i = 0; i < n; i++) {
      long popcount = 0;
      for (int w = 0; w < words; w++)
        popcount += Long.bitCount(stripes[i * words + w]);
      if (popcount == 0 || i > 0 && pages[i] <= pages[i - 1] || rowStart[i] < 0 || rowStart[i] + popcount > layout.rowCount
          || i + 1 < n && rowStart[i + 1] != rowStart[i] + popcount)
        throw corrupt("presence block " + block + " has an inconsistent page " + pages[i]);
    }
    return new Presence(pages, rowStart, stripes);
  }

  private long[] loadVersions(final int block) {
    final ByteBuffer buffer = readBlock(layout.versionsLocation, block);
    final BlockHeader header = verifiedHeader(buffer, layout.versionsLocation[block * 2 + 1], 1, -1);
    final int n = header.rowCount();
    if (n <= 0 || n > PAGES_PER_PRESENCE_BLOCK || header.columnSize(0) != n * 8)
      throw corrupt("page versions block " + block + " has inconsistent sizes");
    final long[] versions = new long[n];
    for (int i = 0; i < n; i++)
      versions[i] = buffer.getLong(header.columnOffset(0) + 8 * i);
    return versions;
  }

  private EncodedColumn loadEncoded(final int block, final int k) {
    final int rows = Math.min(layout.blockRows, layout.rowCount - block * layout.blockRows);
    final ByteBuffer buffer = readBlock(layout.rowsLocation, block);
    final BlockHeader header = verifiedHeader(buffer, layout.rowsLocation[block * 2 + 1], SUB_COLUMNS * layout.columns.length, rows);
    final byte[] bits = slice(buffer, header, k * SUB_COLUMNS);
    final byte[] values = slice(buffer, header, k * SUB_COLUMNS + 1);
    final byte[] offsets = slice(buffer, header, k * SUB_COLUMNS + 2);
    final ColumnKind kind = layout.columns[k].kind();
    if (bits.length != 0 && bits.length != (rows + 7) >>> 3)
      throw corrupt("row block " + block + " column " + k + " has " + bits.length + " presence bytes for " + rows + " rows");
    try {
      final boolean wide = switch (kind) {
        case ORDINAL, LONG -> {
          if (NumericCodecChooser.count(values) != rows || offsets.length != 0)
            throw corrupt("row block " + block + " column " + k + " does not hold " + rows + " values");
          yield isWide(values, rows);
        }
        case DOUBLE -> {
          if (values.length != rows * 8 || offsets.length != 0)
            throw corrupt("row block " + block + " column " + k + " does not hold " + rows + " doubles");
          yield false;
        }
        case ORDINAL_LIST -> {
          final int total = BitPackedCodec.count(values);
          if (NumericCodecChooser.count(offsets) != rows + 1 || NumericCodecChooser.get(offsets, 0) != 0
              || NumericCodecChooser.get(offsets, rows) != total)
            throw corrupt("row block " + block + " column " + k + " has list offsets that do not match its " + total + " ordinals");
          // the values decide: they dominate the decode cost of a list block
          yield isWide(values, total);
        }
      };
      return new EncodedColumn(rows, bits, values, offsets, kind, wide);
    } catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
      throw corrupt("row block " + block + " column " + k + " has an invalid encoding: " + e.getMessage());
    }
  }

  /**
   * Whether the encoding spends more than {@link #NARROW_BITS} bits per value: the bit width of a bit-packed or linear encoding,
   * the average over the whole array for a blockwise one (its 512-value blocks each have their own width).
   */
  private static boolean isWide(final byte[] encoded, final int count) {
    if (count == 0)
      return false;
    return switch (encoded[0]) {
      case BitPackedCodec.CODEC_ID -> (encoded[1] & 0xFF) > NARROW_BITS;
      case LinearCodec.CODEC_ID -> (encoded[LinearCodec.HEADER_BYTES + 1] & 0xFF) > NARROW_BITS;
      default -> (long) encoded.length * 8 > (long) NARROW_BITS * count;
    };
  }

  private ByteBuffer readBlock(final int[] location, final int index) {
    final int offset = location[index * 2];
    final int size = location[index * 2 + 1];
    Path path = file.path();
    try {
      try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
        return readFully(channel, offset, size, path);
      } catch (final NoSuchFileException e) {
        // the directory was moved to a dropped projection's tombstone between the path read and the open: once, under the new path
        if (path.equals(file.path()))
          throw e;
        path = file.path();
        try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
          return readFully(channel, offset, size, path);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException("Cannot read segment " + path, e);
    }
  }

  private BlockHeader verifiedHeader(final ByteBuffer buffer, final int size, final int columns, final int rows) {
    final BlockHeader header = BlockHeader.read(buffer, BLOCK_MAGIC);
    if (header.version() != VERSION || header.totalSize() != size || header.columnCount() != columns
        || rows >= 0 && header.rowCount() != rows)
      throw corrupt("a block of " + size + " bytes does not match its directory entry");
    header.verifyPayload(buffer, 0);
    return header;
  }

  private static byte[] slice(final ByteBuffer buffer, final BlockHeader header, final int column) {
    final byte[] out = new byte[header.columnSize(column)];
    buffer.get(header.columnOffset(column), out);
    return out;
  }

  private BlockCorruptionException corrupt(final String message) {
    return new BlockCorruptionException("Segment " + file.path() + ": " + message);
  }

  private static ByteBuffer readFully(final FileChannel channel, final long offset, final int size, final Path path) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    while (buffer.hasRemaining()) {
      if (channel.read(buffer, offset + buffer.position()) < 0)
        throw new BlockCorruptionException("Segment " + path + " ends before byte " + (offset + size));
    }
    return buffer.flip();
  }

  /**
   * Parses and checks the file header, trailer and footer. The CRC-protected file header comes first, so the column count is known
   * before the footer is read: the trailer's footer offset and the footer's entry count must then describe exactly the bytes up to the
   * trailer before the footer is allocated, so a corrupt offset or count never sizes an allocation.
   */
  private static Layout readLayout(final Path path) throws IOException {
    try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      final long fileSize = channel.size();
      if (fileSize < BlockHeader.FIXED_BYTES + FOOTER_FIXED_BYTES + TRAILER_BYTES || fileSize > Integer.MAX_VALUE)
        throw new BlockCorruptionException("Segment " + path + " has an impossible size of " + fileSize + " bytes");

      // file header: the fixed part names the column count, so read exactly the header of a 2-column block first
      final ByteBuffer head = readFully(channel, 0, BlockHeader.FIXED_BYTES + 8, path);
      final BlockHeader header = BlockHeader.read(head, FILE_MAGIC);
      if (header.version() != VERSION || header.columnCount() != 2 || header.columnSize(0) != HEADER_META_BYTES
          || header.totalSize() > fileSize - FOOTER_FIXED_BYTES - TRAILER_BYTES)
        throw new BlockCorruptionException("Segment " + path + " has an invalid file header");
      final ByteBuffer headerBlock = readFully(channel, 0, header.totalSize(), path);
      header.verifyPayload(headerBlock, 0);
      final ColumnDefinition[] columns = parseColumns(path, header, headerBlock);

      final ByteBuffer trailer = readFully(channel, fileSize - TRAILER_BYTES, TRAILER_BYTES, path);
      final int footerOffset = trailer.getInt(0);
      final int footerCrc = trailer.getInt(4);
      if (trailer.getInt(8) != FILE_MAGIC)
        throw new BlockCorruptionException("Segment " + path + " has no segment trailer (truncated or not a segment)");
      if (footerOffset < header.totalSize() || (long) footerOffset + FOOTER_FIXED_BYTES + TRAILER_BYTES > fileSize)
        throw new BlockCorruptionException("Segment " + path + " has footer offset " + footerOffset + " for a file of " + fileSize);
      final int entries = readFully(channel, footerOffset, FOOTER_FIXED_BYTES, path).getInt(8);
      final long footerSize = FOOTER_FIXED_BYTES + (long) entries * DIRECTORY_ENTRY + 16L * columns.length;
      if (entries < 0 || entries > fileSize / BlockHeader.FIXED_BYTES || footerOffset + footerSize + TRAILER_BYTES != fileSize)
        throw new BlockCorruptionException(
            "Segment " + path + " footer layout does not match the file: offset " + footerOffset + ", " + entries + " entries, " + fileSize
                + " bytes");
      final ByteBuffer footer = readFully(channel, footerOffset, (int) footerSize, path);
      if (BlockHeader.crc32(footer, 0, (int) footerSize) != footerCrc)
        throw new BlockCorruptionException("Segment " + path + " footer CRC mismatch");
      return parseLayout(path, header, headerBlock, columns, footer, footerOffset, entries);
    }
  }

  private static ColumnDefinition[] parseColumns(final Path path, final BlockHeader header, final ByteBuffer headerBlock) {
    try {
      final byte[] json = new byte[header.columnSize(1)];
      headerBlock.get(header.columnOffset(1), json);
      final JSONArray definitions = new JSONObject(new String(json, StandardCharsets.UTF_8)).getJSONArray("columns");
      final ColumnDefinition[] columns = new ColumnDefinition[definitions.length()];
      for (int k = 0; k < columns.length; k++)
        columns[k] = ColumnDefinition.fromJSON(definitions.getJSONObject(k));
      return columns;
    } catch (final RuntimeException e) {
      throw new BlockCorruptionException("Segment " + path + " has unreadable column definitions: " + e.getMessage());
    }
  }

  private static Layout parseLayout(final Path path, final BlockHeader header, final ByteBuffer headerBlock, final ColumnDefinition[] columns,
      final ByteBuffer footer, final int footerOffset, final int entries) {
    final int metaAt = header.columnOffset(0);
    final long segmentId = headerBlock.getLong(metaAt);
    final int maxRecordsInPage = headerBlock.getInt(metaAt + 8);
    final int blockRows = headerBlock.getInt(metaAt + 12);
    final long rowCount = headerBlock.getLong(metaAt + 16);
    if (maxRecordsInPage <= 0 || maxRecordsInPage % 64 != 0 || blockRows <= 0 || rowCount < 0 || rowCount > Integer.MAX_VALUE
        || rowCount != header.rowCount())
      throw new BlockCorruptionException("Segment " + path + " has invalid header values");

    final Map<String, Integer> columnIndex = columnIndexOf(columns);

    final int minPage = footer.getInt(0);
    final int maxPage = footer.getInt(4);
    if (footer.getInt(8) != entries)
      throw new BlockCorruptionException("Segment " + path + " footer changed while it was read");
    final int[] counts = new int[3];
    for (int i = 0; i < entries; i++) {
      final int at = FOOTER_FIXED_BYTES + i * DIRECTORY_ENTRY;
      final int kind = footer.getInt(at);
      final long offset = footer.getInt(at + 8);
      final long size = footer.getInt(at + 12);
      if (kind < KIND_PRESENCE || kind > KIND_ROWS || offset < header.totalSize() || size < BlockHeader.FIXED_BYTES
          || offset + size > footerOffset)
        throw new BlockCorruptionException("Segment " + path + " directory entry " + i + " is outside the file");
      counts[kind]++;
    }
    final int[] firstPages = new int[counts[KIND_PRESENCE]];
    final int[] versionFirstPages = new int[counts[KIND_PAGE_VERSIONS]];
    final int[] presence = new int[counts[KIND_PRESENCE] * 2];
    final int[] versions = new int[counts[KIND_PAGE_VERSIONS] * 2];
    final int[] rows = new int[counts[KIND_ROWS] * 2];
    final int[] next = new int[3];
    for (int i = 0; i < entries; i++) {
      final int at = FOOTER_FIXED_BYTES + i * DIRECTORY_ENTRY;
      final int kind = footer.getInt(at);
      final int first = footer.getInt(at + 4);
      final int n = next[kind]++;
      final int[] location = kind == KIND_PRESENCE ? presence : kind == KIND_PAGE_VERSIONS ? versions : rows;
      location[n * 2] = footer.getInt(at + 8);
      location[n * 2 + 1] = footer.getInt(at + 12);
      if (kind == KIND_PRESENCE)
        firstPages[n] = first;
      else if (kind == KIND_PAGE_VERSIONS)
        versionFirstPages[n] = first;
      else if (first != (long) n * blockRows)
        throw new BlockCorruptionException("Segment " + path + " row block " + n + " starts at row " + first);
    }
    final long expectedRowBlocks = (rowCount + blockRows - 1) / blockRows;
    boolean consistent = counts[KIND_ROWS] == expectedRowBlocks && Arrays.equals(firstPages, versionFirstPages)
        && (rowCount == 0 ? firstPages.length == 0 && minPage == -1 && maxPage == -1
                          : firstPages.length > 0 && firstPages[0] == minPage && maxPage >= firstPages[firstPages.length - 1]);
    for (int i = 1; consistent && i < firstPages.length; i++)
      consistent = firstPages[i] > firstPages[i - 1];
    if (!consistent)
      throw new BlockCorruptionException("Segment " + path + " directory does not describe " + rowCount + " rows");

    final long[] min = new long[columns.length];
    final long[] max = new long[columns.length];
    final int statsAt = FOOTER_FIXED_BYTES + entries * DIRECTORY_ENTRY;
    for (int k = 0; k < columns.length; k++) {
      min[k] = footer.getLong(statsAt + 16 * k);
      max[k] = footer.getLong(statsAt + 16 * k + 8);
    }
    return new Layout(segmentId, maxRecordsInPage, maxRecordsInPage / 64, blockRows, (int) rowCount, columns, columnIndex, min, max,
        minPage, maxPage, firstPages, presence, versions, rows);
  }

  // ---------------------------------------------------------------------------------------------------------------- columns

  /** The verified encoded sub-columns of one column of one row block. Point reads only; {@link #decode()} builds the dense form. */
  private record EncodedColumn(int rows, byte[] bits, byte[] values, byte[] offsets, ColumnKind kind, boolean wide) {
    long bytes() {
      return bits.length + values.length + offsets.length + 64L;
    }

    boolean present(final int row) {
      return bits.length == 0 || (bits[row >>> 3] & (1 << (row & 7))) != 0;
    }

    int ordinal(final int row) {
      try {
        return toOrdinal(NumericCodecChooser.get(values, row));
      } catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
        throw new BlockCorruptionException("Invalid ordinal encoding: " + e.getMessage());
      }
    }

    long longValue(final int row) {
      try {
        return NumericCodecChooser.get(values, row);
      } catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
        throw new BlockCorruptionException("Invalid long encoding: " + e.getMessage());
      }
    }

    double doubleValue(final int row) {
      return Double.longBitsToDouble((long) LONG_BE.get(values, row * 8));
    }

    /** {start, end} of the row's ordinals, packed as {@code start << 32 | end}. */
    long listRange(final int row) {
      try {
        final long start = NumericCodecChooser.get(offsets, row);
        final long end = NumericCodecChooser.get(offsets, row + 1);
        if (start < 0 || end < start || end > Integer.MAX_VALUE)
          throw new BlockCorruptionException("Invalid list offsets " + start + ".." + end);
        return start << 32 | end;
      } catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
        throw new BlockCorruptionException("Invalid list offsets encoding: " + e.getMessage());
      }
    }

    int listOrdinal(final int index) {
      try {
        return toOrdinal(BitPackedCodec.get(values, index));
      } catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
        throw new BlockCorruptionException("Invalid list ordinal encoding: " + e.getMessage());
      }
    }

    /**
     * ORDINAL: the ordinal of every row. ORDINAL_LIST: {@code rows + 1} offsets followed by the ordinals, the ordinals of row
     * {@code r} at {@code [rows + 1 + out[r], rows + 1 + out[r + 1])}.
     */
    int[] decode() {
      try {
        if (kind == ColumnKind.ORDINAL) {
          final long[] decoded = new long[rows];
          NumericCodecChooser.decode(values, decoded);
          final int[] out = new int[rows];
          for (int r = 0; r < rows; r++)
            out[r] = toOrdinal(decoded[r]);
          return out;
        }
        final long[] decodedOffsets = new long[rows + 1];
        NumericCodecChooser.decode(offsets, decodedOffsets);
        final int total = BitPackedCodec.count(values);
        final long[] decodedValues = new long[total];
        BitPackedCodec.decode(values, decodedValues);
        final int[] out = new int[rows + 1 + total];
        for (int r = 0; r <= rows; r++) {
          final long offset = decodedOffsets[r];
          if (offset < 0 || offset > total || r > 0 && offset < out[r - 1])
            throw new BlockCorruptionException("Invalid list offset " + offset + " at row " + r);
          out[r] = (int) offset;
        }
        for (int i = 0; i < total; i++)
          out[rows + 1 + i] = toOrdinal(decodedValues[i]);
        return out;
      } catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
        throw new BlockCorruptionException("Invalid column encoding: " + e.getMessage());
      }
    }

    private static int toOrdinal(final long value) {
      if (value < 0 || value > Integer.MAX_VALUE)
        throw new BlockCorruptionException("Invalid ordinal " + value);
      return (int) value;
    }
  }

  private final class Column implements ColumnReader {
    private final int        k;
    private final ColumnKind kind;

    Column(final int k) {
      this.k = k;
      this.kind = layout.columns[k].kind();
    }

    @Override
    public ColumnKind kind() {
      return kind;
    }

    private EncodedColumn pointBlock(final int row) {
      return pointEncoded(row / layout.blockRows, k);
    }

    @Override
    public boolean isPresent(final long position) {
      final int row = row(position);
      if (row < 0)
        return false;
      return pointBlock(row).present(row % layout.blockRows);
    }

    @Override
    public int ordinal(final long position) {
      requireKind(ColumnKind.ORDINAL);
      final int row = row(position);
      if (row < 0)
        return -1;
      final EncodedColumn column = pointBlock(row);
      final int r = row % layout.blockRows;
      return column.present(r) ? column.ordinal(r) : -1;
    }

    @Override
    public int ordinals(final long position, final int[] out) {
      requireKind(ColumnKind.ORDINAL_LIST);
      final int row = row(position);
      if (row < 0)
        return 0;
      final EncodedColumn column = pointBlock(row);
      final int r = row % layout.blockRows;
      if (!column.present(r))
        return 0;
      final long range = column.listRange(r);
      final int start = (int) (range >>> 32);
      final int count = (int) range - start;
      final int copied = Math.min(count, out.length);
      for (int i = 0; i < copied; i++)
        out[i] = column.listOrdinal(start + i);
      return count;
    }

    @Override
    public long longValue(final long position) {
      requireKind(ColumnKind.LONG);
      final int row = row(position);
      if (row < 0)
        return 0;
      final EncodedColumn column = pointBlock(row);
      final int r = row % layout.blockRows;
      return column.present(r) ? column.longValue(r) : 0;
    }

    @Override
    public double doubleValue(final long position) {
      requireKind(ColumnKind.DOUBLE);
      final int row = row(position);
      if (row < 0)
        return 0.0;
      final EncodedColumn column = pointBlock(row);
      final int r = row % layout.blockRows;
      return column.present(r) ? column.doubleValue(r) : 0.0;
    }

    @Override
    public void forEachOrdinal(final PositionBitmap matches, final OrdinalConsumer consumer) {
      if (kind != ColumnKind.ORDINAL && kind != ColumnKind.ORDINAL_LIST)
        throw new IllegalStateException("Column '" + layout.columns[k].property() + "' of kind " + kind + " has no ordinals");
      if (matches.maxRecordsInPage() != layout.maxRecordsInPage)
        throw new IllegalArgumentException(
            "Matches of " + matches.maxRecordsInPage() + " slots per page for a segment of " + layout.maxRecordsInPage);
      if (layout.rowCount == 0)
        return;

      final int words = layout.wordsPerStripe;
      final long slotsPerPage = layout.maxRecordsInPage;
      // the matched rows of one row block, emitted together once the walk leaves the block (rows ascend with positions)
      long[] positions = new long[MIN_BATCH];
      int[] rows = new int[MIN_BATCH];
      int batched = 0;
      int batchBlock = -1;
      int presenceIndex = -1;
      Presence presence = null;
      for (final int page : matches.pages()) {
        if (page < layout.minPage)
          continue;
        if (page > layout.maxPage)
          break;
        final int block = presenceBlockOf(page);
        if (block != presenceIndex) {
          presence = presence(block);
          presenceIndex = block;
        }
        final int index = Arrays.binarySearch(presence.pages, page);
        if (index < 0)
          continue;
        final long[] match = matches.stripe(page);
        final long[] deadStripe = dead == null ? null : dead.stripe(page);
        final long pageBase = page * slotsPerPage;
        final int base = index * words;
        int rank = presence.rowStart[index];
        for (int w = 0; w < words; w++) {
          final long present = presence.stripes[base + w];
          long bits = match[w] & present;
          if (deadStripe != null)
            bits &= ~deadStripe[w];
          while (bits != 0) {
            final int bit = Long.numberOfTrailingZeros(bits);
            final int row = rank + Long.bitCount(present & ((1L << bit) - 1));
            final int rowBlock = row / layout.blockRows;
            if (rowBlock != batchBlock) {
              if (batched > 0)
                emit(batchBlock, positions, rows, batched, consumer);
              batched = 0;
              batchBlock = rowBlock;
            }
            if (batched == positions.length) {
              positions = Arrays.copyOf(positions, batched * 2);
              rows = Arrays.copyOf(rows, batched * 2);
            }
            positions[batched] = pageBase + ((long) w << 6) + bit;
            rows[batched++] = row - rowBlock * layout.blockRows;
            bits &= bits - 1;
          }
          rank += Long.bitCount(present);
        }
      }
      if (batched > 0)
        emit(batchBlock, positions, rows, batched, consumer);
    }

    /** Visits the batched rows of one row block: dense over the decoded form when worth it, point reads on the encoded bytes otherwise. */
    private void emit(final int block, final long[] positions, final int[] rows, final int count, final OrdinalConsumer consumer) {
      final EncodedColumn column = encoded(block, k);
      final boolean dense = column.wide() && (long) count * 8 > column.rows();
      final int[] decoded = dense ? decoded(block, k, column) : null;
      if (kind == ColumnKind.ORDINAL) {
        for (int i = 0; i < count; i++) {
          final int r = rows[i];
          if (column.present(r))
            consumer.accept(positions[i], dense ? decoded[r] : column.ordinal(r));
        }
        return;
      }
      final int valuesAt = column.rows() + 1;
      for (int i = 0; i < count; i++) {
        final int r = rows[i];
        if (!column.present(r))
          continue;
        if (dense) {
          for (int j = valuesAt + decoded[r], end = valuesAt + decoded[r + 1]; j < end; j++)
            consumer.accept(positions[i], decoded[j]);
        } else {
          final long range = column.listRange(r);
          for (int j = (int) (range >>> 32), end = (int) range; j < end; j++)
            consumer.accept(positions[i], column.listOrdinal(j));
        }
      }
    }

    private void requireKind(final ColumnKind expected) {
      if (kind != expected)
        throw new IllegalStateException("Column '" + layout.columns[k].property() + "' is " + kind + ", not " + expected);
    }
  }
}
