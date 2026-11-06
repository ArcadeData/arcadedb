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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentRoundTripTest {
  static final int MAX_RECORDS_IN_PAGE = 2048;
  static final int BLOCK_ROWS          = 16384;

  static final ColumnDefinition BRAND = new ColumnDefinition(11, "brand", Type.STRING, ColumnKind.ORDINAL, false, null);
  static final ColumnDefinition TAGS  = new ColumnDefinition(12, "tags", Type.LIST, ColumnKind.ORDINAL_LIST, true, null);
  static final ColumnDefinition SIZE  = new ColumnDefinition(13, "size", Type.LONG, ColumnKind.LONG, false, null);
  static final ColumnDefinition SCORE = new ColumnDefinition(14, "score", Type.DOUBLE, ColumnKind.DOUBLE, false, null);

  static final ColumnDefinition[] ALL = { BRAND, TAGS, SIZE, SCORE };

  @TempDir
  Path dir;

  /** Rows of a test segment in input order, one array per column (index = row), as the writer received them. */
  static final class Data {
    final long[]    positions;
    final boolean[] brandPresent;
    final int[]     brand;
    final boolean[] tagsPresent;
    final int[][]   tags;
    final boolean[] sizePresent;
    final long[]    size;
    final boolean[] scorePresent;
    final double[]  score;

    Data(final int rows, final long seed, final int brandCardinality) {
      final Random random = new Random(seed);
      positions = new long[rows];
      brandPresent = new boolean[rows];
      brand = new int[rows];
      tagsPresent = new boolean[rows];
      tags = new int[rows][];
      sizePresent = new boolean[rows];
      size = new long[rows];
      scorePresent = new boolean[rows];
      score = new double[rows];
      long position = random.nextInt(9);
      for (int i = 0; i < rows; i++) {
        positions[i] = position;
        position += 1 + random.nextInt(9);
        brandPresent[i] = random.nextInt(20) != 0;
        brand[i] = random.nextInt(brandCardinality);
        tagsPresent[i] = random.nextInt(10) != 0;
        final TreeSet<Integer> set = new TreeSet<>();
        final int n = random.nextInt(6);
        for (int j = 0; j < n; j++)
          set.add(random.nextInt(300));
        tags[i] = set.stream().mapToInt(Integer::intValue).toArray();
        sizePresent[i] = random.nextInt(30) != 0;
        size[i] = random.nextLong() >> random.nextInt(40);
        scorePresent[i] = random.nextInt(50) != 0;
        score[i] = random.nextGaussian() * 1000;
      }
    }

    static long pageVersion(final long position) {
      return (position / MAX_RECORDS_IN_PAGE) * 10 + 7;
    }

    SegmentReader.Meta write(final Path file, final ColumnDefinition[] columns, final int blockRows, final long segmentId)
        throws IOException {
      try (final SegmentWriter writer = new SegmentWriter(file, segmentId, MAX_RECORDS_IN_PAGE, columns, blockRows)) {
        final RowValues values = new RowValues(columns.length);
        for (int i = 0; i < positions.length; i++) {
          for (int k = 0; k < columns.length; k++)
            fill(values, k, columns[k], i);
          writer.addRow(positions[i], pageVersion(positions[i]), values);
        }
        return writer.finish();
      }
    }

    private void fill(final RowValues values, final int k, final ColumnDefinition column, final int i) {
      switch (column.kind()) {
      case ORDINAL -> {
        values.present[k] = brandPresent[i];
        values.ordinal[k] = brand[i];
      }
      case ORDINAL_LIST -> {
        values.present[k] = tagsPresent[i];
        values.list[k] = tags[i];
        values.listCount[k] = tags[i].length;
      }
      case LONG -> {
        values.present[k] = sizePresent[i];
        values.longs[k] = size[i];
      }
      case DOUBLE -> {
        values.present[k] = scorePresent[i];
        values.doubles[k] = score[i];
      }
      }
    }
  }

  /** Every received (position, ordinal) pair, in visit order. */
  static final class Pairs implements OrdinalConsumer {
    final List<long[]> pairs = new ArrayList<>();

    @Override
    public void accept(final long position, final int ordinal) {
      pairs.add(new long[] { position, ordinal });
    }

    List<String> asStrings() {
      final List<String> out = new ArrayList<>(pairs.size());
      for (final long[] p : pairs)
        out.add(p[0] + ":" + p[1]);
      return out;
    }
  }

  static PositionBitmap bitmapOf(final long... positions) {
    final PositionBitmap bitmap = new PositionBitmap(MAX_RECORDS_IN_PAGE);
    for (final long p : positions)
      bitmap.set(p);
    return bitmap;
  }

  static PositionBitmap everyNth(final Data data, final int n) {
    final PositionBitmap bitmap = new PositionBitmap(MAX_RECORDS_IN_PAGE);
    for (int i = 0; i < data.positions.length; i += n)
      bitmap.set(data.positions[i]);
    return bitmap;
  }

  /** Expected brand pairs of the given rows (present values only), ascending position. */
  static List<String> expectedBrand(final Data data, final PositionBitmap matches, final Set<Long> dead) {
    final List<String> out = new ArrayList<>();
    for (int i = 0; i < data.positions.length; i++)
      if (matches.get(data.positions[i]) && data.brandPresent[i] && !dead.contains(data.positions[i]))
        out.add(data.positions[i] + ":" + data.brand[i]);
    return out;
  }

  static List<String> expectedTags(final Data data, final PositionBitmap matches) {
    final List<String> out = new ArrayList<>();
    for (int i = 0; i < data.positions.length; i++)
      if (matches.get(data.positions[i]) && data.tagsPresent[i])
        for (final int t : data.tags[i])
          out.add(data.positions[i] + ":" + t);
    return out;
  }

  private Path segment(final Data data, final ColumnDefinition[] columns, final int blockRows, final String name) throws IOException {
    final Path file = dir.resolve(name);
    data.write(file, columns, blockRows, 42);
    return file;
  }

  @Test
  void allKindsRoundTrip() throws IOException {
    final Data data = new Data(50_000, 1, 1000);
    final Path file = dir.resolve(SegmentFormat.segmentFileName(42));
    final SegmentReader.Meta meta = data.write(file, ALL, BLOCK_ROWS, 42);
    assertThat(meta.segmentId()).isEqualTo(42);
    assertThat(meta.rows()).isEqualTo(50_000);
    assertThat(meta.minPage()).isEqualTo((int) (data.positions[0] / MAX_RECORDS_IN_PAGE));
    assertThat(meta.maxPage()).isEqualTo((int) (data.positions[49_999] / MAX_RECORDS_IN_PAGE));

    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      assertThat(reader.segmentId()).isEqualTo(42);
      assertThat(reader.rowCount()).isEqualTo(50_000);
      assertThat(reader.deadCount()).isZero();
      assertThat(reader.minPage()).isEqualTo(meta.minPage());
      assertThat(reader.maxPage()).isEqualTo(meta.maxPage());
      final ColumnReader brand = reader.column("brand");
      final ColumnReader tags = reader.column("tags");
      final ColumnReader size = reader.column("size");
      final ColumnReader score = reader.column("score");
      assertThat(brand.kind()).isEqualTo(ColumnKind.ORDINAL);
      assertThat(tags.kind()).isEqualTo(ColumnKind.ORDINAL_LIST);
      assertThat(size.kind()).isEqualTo(ColumnKind.LONG);
      assertThat(score.kind()).isEqualTo(ColumnKind.DOUBLE);
      assertThat(reader.column("nope")).isNull();

      final int[] out = new int[16];
      for (int i = 0; i < data.positions.length; i++) {
        final long p = data.positions[i];
        assertThat(brand.isPresent(p)).as("brand presence row %d", i).isEqualTo(data.brandPresent[i]);
        if (data.brandPresent[i])
          assertThat(brand.ordinal(p)).as("brand row %d", i).isEqualTo(data.brand[i]);
        assertThat(tags.isPresent(p)).isEqualTo(data.tagsPresent[i]);
        if (data.tagsPresent[i]) {
          final int count = tags.ordinals(p, out);
          assertThat(Arrays.copyOf(out, count)).as("tags row %d", i).isEqualTo(data.tags[i]);
        }
        assertThat(size.isPresent(p)).isEqualTo(data.sizePresent[i]);
        if (data.sizePresent[i])
          assertThat(size.longValue(p)).as("size row %d", i).isEqualTo(data.size[i]);
        assertThat(score.isPresent(p)).isEqualTo(data.scorePresent[i]);
        if (data.scorePresent[i])
          assertThat(Double.doubleToRawLongBits(score.doubleValue(p))).isEqualTo(Double.doubleToRawLongBits(data.score[i]));
      }
      // a position between two rows has no value in any column
      final long gap = data.positions[100] + 1;
      if (gap != data.positions[101]) {
        assertThat(brand.isPresent(gap)).isFalse();
        assertThat(tags.isPresent(gap)).isFalse();
      }
    }
  }

  @Test
  void rowByRankOverPresence() throws IOException {
    final Data data = new Data(50_000, 2, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "rank.acs");
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final Set<Long> present = new HashSet<>();
      for (int i = 0; i < data.positions.length; i++) {
        assertThat(reader.row(data.positions[i])).isEqualTo(i);
        present.add(data.positions[i]);
      }
      final long last = data.positions[data.positions.length - 1];
      for (long p = 0; p <= last + 3000; p += 7)
        if (!present.contains(p))
          assertThat(reader.row(p)).as("absent position %d", p).isEqualTo(-1);
      assertThat(reader.row(last + 1)).isEqualTo(-1);
      assertThat(reader.row((long) (reader.maxPage() + 5) * MAX_RECORDS_IN_PAGE)).isEqualTo(-1);
    }
  }

  @Test
  void minMaxPerColumnSkipping() throws IOException {
    final Data data = new Data(50_000, 3, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "minmax.acs");
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int minOrdinal = Integer.MAX_VALUE;
    int maxOrdinal = Integer.MIN_VALUE;
    for (int i = 0; i < data.positions.length; i++) {
      if (data.sizePresent[i]) {
        min = Math.min(min, data.size[i]);
        max = Math.max(max, data.size[i]);
      }
      if (data.brandPresent[i]) {
        minOrdinal = Math.min(minOrdinal, data.brand[i]);
        maxOrdinal = Math.max(maxOrdinal, data.brand[i]);
      }
    }
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      assertThat(reader.minLong("size")).isEqualTo(min);
      assertThat(reader.maxLong("size")).isEqualTo(max);
      assertThat(reader.minLong("brand")).isEqualTo(minOrdinal);
      assertThat(reader.maxLong("brand")).isEqualTo(maxOrdinal);
      assertThatThrownBy(() -> reader.minLong("nope")).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void forEachOrdinalDenseAndPointAgree() throws IOException {
    final Data data = new Data(50_000, 4, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "modes.acs");
    final PositionBitmap all = everyNth(data, 1);
    final PositionBitmap sparse = everyNth(data, 50);
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final long fileId = reader.file().cacheFileId();
      final Pairs dense = new Pairs();
      reader.column("brand").forEachOrdinal(all, dense);
      assertThat(dense.asStrings()).isEqualTo(expectedBrand(data, all, Set.of()));
      // the dense path really ran: the decoded form of the brand column (id 0 + 3 * 4) was cached
      assertThat(BlockCache.instance().containsForTesting(fileId, 0, 3 * ALL.length)).isTrue();

      final Pairs point = new Pairs();
      reader.column("brand").forEachOrdinal(sparse, point);
      assertThat(point.asStrings()).isEqualTo(expectedBrand(data, sparse, Set.of()));

      final Pairs denseTags = new Pairs();
      reader.column("tags").forEachOrdinal(all, denseTags);
      assertThat(denseTags.asStrings()).isEqualTo(expectedTags(data, all));
      // list values of 0..299 are 9 bits wide: the list column (id 1 + 3 * 4) was decoded, whatever the width of its offsets
      assertThat(BlockCache.instance().containsForTesting(fileId, 0, 1 + 3 * ALL.length)).isTrue();
      final Pairs pointTags = new Pairs();
      reader.column("tags").forEachOrdinal(sparse, pointTags);
      assertThat(pointTags.asStrings()).isEqualTo(expectedTags(data, sparse));

      assertThatThrownBy(() -> reader.column("size").forEachOrdinal(all, dense)).isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  void forEachOrdinalVisitsPositionsAscending() throws IOException {
    final Data data = new Data(50_000, 5, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "ascending.acs");
    // block 0 every row (dense), block 1 every 50th row (point), block 2 every row (dense)
    final PositionBitmap matches = new PositionBitmap(MAX_RECORDS_IN_PAGE);
    for (int i = 0; i < 3 * BLOCK_ROWS; i++)
      if (i / BLOCK_ROWS != 1 || i % 50 == 0)
        matches.set(data.positions[i]);
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final Pairs brand = new Pairs();
      reader.column("brand").forEachOrdinal(matches, brand);
      assertThat(brand.pairs).isNotEmpty();
      assertThat(brand.pairs.getLast()[0]).isGreaterThan(data.positions[2 * BLOCK_ROWS]);
      for (int i = 1; i < brand.pairs.size(); i++)
        assertThat(brand.pairs.get(i)[0]).isGreaterThan(brand.pairs.get(i - 1)[0]);

      final Pairs tags = new Pairs();
      reader.column("tags").forEachOrdinal(matches, tags);
      for (int i = 1; i < tags.pairs.size(); i++) {
        final long[] prev = tags.pairs.get(i - 1);
        final long[] cur = tags.pairs.get(i);
        assertThat(cur[0]).isGreaterThanOrEqualTo(prev[0]);
        if (cur[0] == prev[0])
          assertThat(cur[1]).isGreaterThan(prev[1]);
      }
      assertThat(tags.asStrings()).isEqualTo(expectedTags(data, matches));
    }
  }

  @Test
  void deadPositionsLeaveTheLiveView() throws IOException {
    final Data data = new Data(50_000, 6, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "dead.acs");
    final long p1 = data.positions[17];
    final long p2 = data.positions[40_001];
    try (final SegmentReader reader = SegmentReader.open(file, bitmapOf(p1, p2))) {
      final PositionBitmap live = reader.live();
      assertThat(live.get(p1)).isFalse();
      assertThat(live.get(p2)).isFalse();
      assertThat(live.get(data.positions[18])).isTrue();
      assertThat(live.cardinality()).isEqualTo(50_000 - 2);
      assertThat(reader.live()).isSameAs(live);
      assertThat(reader.deadCount()).isEqualTo(2);

      final int page = (int) (p2 / MAX_RECORDS_IN_PAGE);
      final PositionBitmap bounded = reader.live(page, page + 1);
      assertThat(bounded.pages()).containsExactly(page);
      assertThat(bounded.get(p2)).isFalse();
      long onPage = 0;
      for (final long p : data.positions)
        if (p / MAX_RECORDS_IN_PAGE == page)
          onPage++;
      assertThat(bounded.cardinality()).isEqualTo(onPage - 1);
    }
  }

  @Test
  void pageVersionsAreRecorded() throws IOException {
    final Data data = new Data(50_000, 7, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "versions.acs");
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      for (final long p : data.positions)
        assertThat(reader.pageVersion((int) (p / MAX_RECORDS_IN_PAGE))).isEqualTo(Data.pageVersion(p));
      assertThat(reader.pageVersion(reader.maxPage() + 1)).isEqualTo(-1);
    }

    // a hole between two covered pages
    final Path holes = dir.resolve("holes.acs");
    final ColumnDefinition[] columns = { SIZE };
    try (final SegmentWriter writer = new SegmentWriter(holes, 9, MAX_RECORDS_IN_PAGE, columns, 64)) {
      final RowValues values = new RowValues(1);
      values.present[0] = true;
      writer.addRow(3, 100, values);
      writer.addRow(5L * MAX_RECORDS_IN_PAGE + 1, 555, values);
      writer.finish();
    }
    try (final SegmentReader reader = SegmentReader.open(holes, null)) {
      assertThat(reader.pageVersion(0)).isEqualTo(100);
      assertThat(reader.pageVersion(3)).isEqualTo(-1);
      assertThat(reader.pageVersion(5)).isEqualTo(555);
      assertThat(reader.minPage()).isEqualTo(0);
      assertThat(reader.maxPage()).isEqualTo(5);
    }
  }

  @Test
  void decodedBlocksGoThroughTheBlockCache() throws IOException {
    final Data data = new Data(50_000, 8, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "cache.acs");
    final PositionBitmap all = everyNth(data, 1);
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final Pairs first = new Pairs();
      reader.column("brand").forEachOrdinal(all, first);
      final long hitsBefore = BlockCache.instance().hits();
      final Pairs second = new Pairs();
      reader.column("brand").forEachOrdinal(all, second);
      final int rowBlocks = (50_000 + BLOCK_ROWS - 1) / BLOCK_ROWS;
      assertThat(BlockCache.instance().hits() - hitsBefore).isGreaterThanOrEqualTo(rowBlocks);
      assertThat(second.pairs.size()).isEqualTo(first.pairs.size());
    }
  }

  @Test
  void noFileHandleIsHeldBetweenReads() throws IOException {
    final Data data = new Data(50_000, 9, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "handles.acs");
    final PositionBitmap all = everyNth(data, 1);
    final SegmentFile segmentFile;
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final Pairs first = new Pairs();
      reader.column("brand").forEachOrdinal(all, first);
      Files.delete(file);
      assertThat(Files.exists(file)).isFalse();
      final Pairs second = new Pairs();
      reader.column("brand").forEachOrdinal(all, second);
      assertThat(second.asStrings()).isEqualTo(first.asStrings());
      segmentFile = reader.file();
    }
    segmentFile.deleted();
  }

  @Test
  void pointReadsOnOneBlockMakeNoCacheCalls() throws IOException {
    final Data data = new Data(50_000, 15, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "pointmemo.acs");
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final ColumnReader brand = reader.column("brand");
      brand.isPresent(data.positions[0]);
      final long callsBefore = BlockCache.instance().hits() + BlockCache.instance().misses();
      final int reads = 10_000;
      for (int i = 0; i < reads; i++) {
        final long p = data.positions[i % BLOCK_ROWS];
        assertThat(brand.ordinal(p)).isEqualTo(data.brandPresent[i % BLOCK_ROWS] ? data.brand[i % BLOCK_ROWS] : -1);
      }
      // without the per-thread point memo every read costs two cache calls (presence and row block): 2 * reads
      assertThat(BlockCache.instance().hits() + BlockCache.instance().misses() - callsBefore).isLessThan(reads);
      // a read in another block still answers
      assertThat(brand.isPresent(data.positions[BLOCK_ROWS + 1])).isEqualTo(data.brandPresent[BLOCK_ROWS + 1]);
    }
  }

  @Test
  void pointReadMemoIsPerThreadNotPerReader() throws Exception {
    final Data data = new Data(2_000, 16, 1000);
    final ColumnDefinition[] columns = { BRAND };
    final List<SegmentReader> readers = new ArrayList<>();
    try {
      for (int i = 0; i < 50; i++) {
        final Path file = dir.resolve("many-" + i + ".acs");
        data.write(file, columns, 512, i);
        readers.add(SegmentReader.open(file, null));
      }
      for (int i = 0; i < readers.size(); i++)
        assertThat(readers.get(i).column("brand").isPresent(data.positions[i * 7])).isEqualTo(data.brandPresent[i * 7]);
      // the calling thread pins the last presence block and the last row block only, both of the last reader
      final long last = readers.getLast().file().cacheFileId();
      assertThat(SegmentReader.pointMemoFileIdsForTesting()).containsExactly(last, last);
      // another thread's memo is its own
      final long[][] other = new long[1][];
      final Thread thread = new Thread(() -> other[0] = SegmentReader.pointMemoFileIdsForTesting());
      thread.start();
      thread.join();
      assertThat(other[0]).containsExactly(0L, 0L);
      // no reader or column keeps a block of its own
      for (final Class<?> type : List.of(SegmentReader.class, readers.getFirst().column("brand").getClass()))
        for (final Field field : type.getDeclaredFields())
          if (!Modifier.isStatic(field.getModifiers()))
            assertThat(field.getType().getSimpleName()).as(type.getSimpleName() + "." + field.getName())
                .isNotIn("Presence", "EncodedColumn", "MemoSlot", "PointMemo");
    } finally {
      for (final SegmentReader reader : readers)
        reader.close();
    }
  }

  @Test
  void positionsMustAscend() throws IOException {
    try (final SegmentWriter writer = new SegmentWriter(dir.resolve("order.acs"), 1, MAX_RECORDS_IN_PAGE, new ColumnDefinition[] { SIZE },
        64)) {
      final RowValues values = new RowValues(1);
      writer.addRow(10, 1, values);
      assertThatThrownBy(() -> writer.addRow(5, 1, values)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> writer.addRow(10, 1, values)).isInstanceOf(IllegalStateException.class);
      writer.addRow(11, 1, values);
    }
    // closed without finish(): the partial file is removed
    assertThat(Files.exists(dir.resolve("order.acs"))).isFalse();
  }

  @Test
  void presenceIsStoredOncePerSegment() throws IOException {
    final Data data = new Data(20_000, 10, 1000);
    final Path narrow = segment(data, new ColumnDefinition[] { BRAND }, 4096, "one.acs");
    final Path wide = segment(data, ALL, 4096, "four.acs");
    try (final SegmentReader one = SegmentReader.open(narrow, null); final SegmentReader four = SegmentReader.open(wide, null)) {
      final int presenceBlocks = one.blockCountForTesting(SegmentFormat.KIND_PRESENCE);
      assertThat(presenceBlocks).isEqualTo(1);
      assertThat(four.blockCountForTesting(SegmentFormat.KIND_PRESENCE)).isEqualTo(presenceBlocks);
      assertThat(readBlock(narrow, one.blockLocationForTesting(SegmentFormat.KIND_PRESENCE, 0)))
          .isEqualTo(readBlock(wide, four.blockLocationForTesting(SegmentFormat.KIND_PRESENCE, 0)));
      // the 4-column row blocks carry 3 sub-columns per column, the presence is not repeated in them
      assertThat(four.blockCountForTesting(SegmentFormat.KIND_ROWS)).isEqualTo(one.blockCountForTesting(SegmentFormat.KIND_ROWS));
      assertThat(Files.size(wide)).isGreaterThan(Files.size(narrow));
    }
  }

  static byte[] readBlock(final Path file, final int[] location) throws IOException {
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      final ByteBuffer buffer = ByteBuffer.allocate(location[1]);
      channel.read(buffer, location[0]);
      return buffer.array();
    }
  }

  @Test
  void pointModeNeverCachesADecodedBlock() throws IOException {
    final Data data = new Data(50_000, 11, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "point.acs");
    final PositionBitmap sparse = everyNth(data, 500);
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final long fileId = reader.file().cacheFileId();
      reader.column("brand").forEachOrdinal(sparse, new Pairs());
      reader.column("tags").forEachOrdinal(sparse, new Pairs());
      final int rowBlocks = reader.blockCountForTesting(SegmentFormat.KIND_ROWS);
      for (int b = 0; b < rowBlocks; b++) {
        // the encoded forms are held, so the probe below would see a decoded one if it had been admitted
        assertThat(BlockCache.instance().containsForTesting(fileId, b, 0)).isTrue();
        assertThat(BlockCache.instance().containsForTesting(fileId, b, 1)).isTrue();
        for (int k = 0; k < ALL.length; k++)
          assertThat(BlockCache.instance().containsForTesting(fileId, b, k + 3 * ALL.length)).as("block %d column %d", b, k).isFalse();
      }
    }
  }

  @Test
  void narrowColumnIsNeverDecoded() throws IOException {
    final Data data = new Data(50_000, 12, 3);
    final ColumnDefinition[] columns = { BRAND };
    final Path file = segment(data, columns, BLOCK_ROWS, "narrow.acs");
    final PositionBitmap all = everyNth(data, 1);
    try (final SegmentReader reader = SegmentReader.open(file, null)) {
      final long fileId = reader.file().cacheFileId();
      final Pairs pairs = new Pairs();
      reader.column("brand").forEachOrdinal(all, pairs);
      assertThat(pairs.asStrings()).isEqualTo(expectedBrand(data, all, Set.of()));
      assertThat(BlockCache.instance().containsForTesting(fileId, 0, 0)).isTrue();
      for (int b = 0; b < reader.blockCountForTesting(SegmentFormat.KIND_ROWS); b++)
        assertThat(BlockCache.instance().containsForTesting(fileId, b, 3)).isFalse();
    }
  }

  @Test
  void forEachOrdinalSkipsDeadRows() throws IOException {
    final Data data = new Data(50_000, 13, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "skipdead.acs");
    // two rows with a brand value, so a visit of them would be observable
    int r1 = 5;
    while (!data.brandPresent[r1])
      r1++;
    int r2 = 30_000;
    while (!data.brandPresent[r2])
      r2++;
    final long p1 = data.positions[r1];
    final long p2 = data.positions[r2];
    final PositionBitmap all = everyNth(data, 1);
    try (final SegmentReader reader = SegmentReader.open(file, bitmapOf(p1, p2))) {
      final Pairs pairs = new Pairs();
      reader.column("brand").forEachOrdinal(all, pairs);
      for (final long[] pair : pairs.pairs)
        assertThat(pair[0]).isNotIn(p1, p2);
      assertThat(pairs.asStrings()).isEqualTo(expectedBrand(data, all, Set.of(p1, p2)));
      assertThat(reader.liveMaterializedForTesting()).isFalse();
    }
  }

  @Test
  void forEachOrdinalTouchesOnlyMatchedPages() throws IOException {
    // 2100 pages of 64 slots, 10 rows each: 3 presence blocks (1024 pages each), 21 row blocks of 1000 rows
    final int maxRecords = 64;
    final Path file = dir.resolve("pages.acs");
    final ColumnDefinition[] columns = { BRAND };
    try (final SegmentWriter writer = new SegmentWriter(file, 3, maxRecords, columns, 1000)) {
      final RowValues values = new RowValues(1);
      values.present[0] = true;
      for (int page = 0; page < 2100; page++)
        for (int slot = 0; slot < 10; slot++) {
          values.ordinal[0] = (page * 31 + slot) % 5000;
          writer.addRow((long) page * maxRecords + slot, page, values);
        }
      writer.finish();
    }
    final PositionBitmap matches = new PositionBitmap(maxRecords);
    for (int slot = 0; slot < 10; slot++) {
      matches.set(3L * maxRecords + slot);
      matches.set(2050L * maxRecords + slot);
    }
    final PositionBitmap dead = new PositionBitmap(maxRecords);
    dead.set(500L * maxRecords + 2);
    dead.set(2050L * maxRecords + 4);
    try (final SegmentReader reader = SegmentReader.open(file, dead)) {
      assertThat(reader.blockCountForTesting(SegmentFormat.KIND_PRESENCE)).isEqualTo(3);
      assertThat(reader.blockCountForTesting(SegmentFormat.KIND_ROWS)).isEqualTo(21);
      final long fileId = reader.file().cacheFileId();
      assertThat(BlockCache.instance().entriesForTesting(fileId)).isZero();
      final Pairs pairs = new Pairs();
      reader.column("brand").forEachOrdinal(matches, pairs);
      assertThat(pairs.pairs).hasSize(19);
      // presence blocks 0 and 2, row blocks 0 (rows 30..39) and 20 (rows 20500..20509), encoded forms only
      assertThat(BlockCache.instance().entriesForTesting(fileId)).isEqualTo(4);
      assertThat(BlockCache.instance().containsForTesting(fileId, 0, 0)).isTrue();
      assertThat(BlockCache.instance().containsForTesting(fileId, 20, 0)).isTrue();
      assertThat(reader.liveMaterializedForTesting()).isFalse();
    }
  }

  @Test
  void closeAndWithDeadNeverInvalidateTheCache() throws IOException {
    final Data data = new Data(50_000, 14, 1000);
    final Path file = segment(data, ALL, BLOCK_ROWS, "withdead.acs");
    final PositionBitmap all = everyNth(data, 1);
    final SegmentReader first = SegmentReader.open(file, null);
    final long fileId = first.file().cacheFileId();
    first.column("brand").forEachOrdinal(all, new Pairs());
    final long entries = BlockCache.instance().entriesForTesting(fileId);
    assertThat(entries).isGreaterThan(0);

    final long p1 = data.positions[123];
    final SegmentReader copy = first.withDead(bitmapOf(p1));
    assertThat(copy.file()).isSameAs(first.file());
    assertThat(copy.deadCount()).isEqualTo(1);
    assertThat(first.deadCount()).isZero();
    assertThat(first.file().referencesForTesting()).isEqualTo(2);
    first.close();
    first.close();
    assertThatThrownBy(() -> first.withDead(null)).isInstanceOf(IllegalStateException.class);
    assertThat(copy.file().referencesForTesting()).isEqualTo(1);
    assertThat(BlockCache.instance().entriesForTesting(fileId)).isEqualTo(entries);

    final long hitsBefore = BlockCache.instance().hits();
    final Pairs pairs = new Pairs();
    copy.column("brand").forEachOrdinal(all, pairs);
    assertThat(BlockCache.instance().hits() - hitsBefore).isGreaterThanOrEqualTo(4);
    assertThat(pairs.asStrings()).isEqualTo(expectedBrand(data, all, Set.of(p1)));
    assertThat(BlockCache.instance().entriesForTesting(fileId)).isEqualTo(entries);

    final SegmentFile segmentFile = copy.file();
    copy.close();
    assertThat(segmentFile.referencesForTesting()).isZero();
    assertThat(BlockCache.instance().entriesForTesting(fileId)).isEqualTo(entries);
    Files.delete(file);
    segmentFile.deleted();
    assertThat(BlockCache.instance().entriesForTesting(fileId)).isZero();
  }

  @Test
  void segmentFileRefCounting() {
    final SegmentFile file = SegmentFile.open(dir.resolve("x.acs"));
    assertThat(file.cacheFileId()).isPositive();
    assertThat(file.retain()).isSameAs(file);
    file.release();
    assertThatThrownBy(file::deleted).isInstanceOf(IllegalStateException.class);
    file.release();
    file.deleted();
    file.deleted();
    assertThatThrownBy(file::retain).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(file::release).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void fileNames() {
    assertThat(SegmentFormat.segmentFileName(0x1fL)).isEqualTo("seg-000000000000001f.acs");
    assertThat(SegmentFormat.deadFileName(0x1fL, 3)).isEqualTo("seg-000000000000001f.dead.3");
  }
}
