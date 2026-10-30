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
package com.arcadedb.index.columnar.spike;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnarSpikeCorrectnessTest extends TestHelper {

  @Test
  void segmentMatchesRecordsOfTheBucket() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 42L, 0.0);
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);

    assertThat(segment.rows()).isEqualTo(20_000L);
    assertThat(dictionary.size()).isBetween(150, SpikeDataset.BRAND_CARDINALITY);
    // ceil(log2(size + 1)) for 150..255 values (+1 reserves MISSING)
    assertThat(segment.bitWidth()).isEqualTo(8);
    database.iterateType(SpikeDataset.TYPE, false).forEachRemaining(r -> {
      final long position = r.getIdentity().getPosition();
      assertThat(segment.isPresent(position)).isTrue();
      assertThat(dictionary.valueOf(segment.ordinalAt(position))).isEqualTo(r.asDocument().getString(SpikeDataset.BRAND));
    });
  }

  @Test
  void segmentMatchesRecordsAfterDeletes() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 7L, 0.05);
    final long[] deleted = SpikeDataset.deleteEvery(database, 3);
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);

    assertThat(segment.rows()).isEqualTo(20_000L - deleted.length);
    for (final long position : deleted)
      assertThat(segment.isPresent(position)).isFalse();

    final int[] missing = { 0 };
    database.iterateType(SpikeDataset.TYPE, false).forEachRemaining(r -> {
      final long position = r.getIdentity().getPosition();
      assertThat(segment.isPresent(position)).isTrue();
      final String brand = r.asDocument().getString(SpikeDataset.BRAND);
      if (brand == null) {
        missing[0]++;
        assertThat(segment.ordinalAt(position)).isEqualTo(SpikeSegment.MISSING);
      } else
        assertThat(dictionary.valueOf(segment.ordinalAt(position))).isEqualTo(brand);
    });
    assertThat(missing[0]).isGreaterThan(0);
  }

  @Test
  void segmentStraddlingWordsMatchesRecords() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 11L, 0.05);
    SpikeDataset.deleteEvery(database, 4);
    final SpikeDictionary dictionary = new SpikeDictionary();
    for (int i = 0; i < 300; i++)
      dictionary.getOrAdd("pad-" + i);
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);

    // 9 bits does not divide 64, so some values straddle two words
    assertThat(segment.bitWidth()).isEqualTo(9);
    final int[] checked = { 0 };
    database.iterateType(SpikeDataset.TYPE, false).forEachRemaining(r -> {
      final long position = r.getIdentity().getPosition();
      final String brand = r.asDocument().getString(SpikeDataset.BRAND);
      if (brand == null)
        assertThat(segment.ordinalAt(position)).isEqualTo(SpikeSegment.MISSING);
      else
        assertThat(dictionary.valueOf(segment.ordinalAt(position))).isEqualTo(brand);
      checked[0]++;
    });
    assertThat((long) checked[0]).isEqualTo(segment.rows());
  }

  @ParameterizedTest
  @ValueSource(strings = { "fileSize < 10000", "status = 'published'", "status = 'archived' AND fileSize < 500000" })
  void countsEqualGroupByWithMissingValues(final String where) {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 11L, 0.02);
    SpikeDataset.deleteEvery(database, 5);
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);

    final long[] positions = SpikeTermsCounter.positions(database, where);
    final SpikeTermsCounter.Counts counts = SpikeTermsCounter.count(segment, SpikeTermsCounter.matchBitmap(segment, positions), positions,
        dictionary.size());
    final long[] missing = new long[1];

    assertThat(SpikeTermsCounter.topN(counts, dictionary, SpikeDataset.BRAND_CARDINALITY)).containsExactlyElementsOf(
        SpikeTermsCounter.groupBy(database, where, SpikeDataset.BRAND_CARDINALITY, missing));
    assertThat(counts.missing()).isEqualTo(missing[0]).isGreaterThan(0L);
    assertThat(counts.uncovered()).isZero();
  }

  @Test
  void topNBreaksTiesByValueAscending() {
    SpikeDataset.createSchema(database);
    // insertion order deliberately not value order
    database.transaction(() -> {
      for (final String brand : new String[] { "b", "b", "b", "b", "b", "a", "a", "a", "a", "a", "c", "c" }) {
        final MutableDocument doc = database.newDocument(SpikeDataset.TYPE);
        doc.set(SpikeDataset.BRAND, brand);
        doc.set(SpikeDataset.STATUS, "published");
        doc.set(SpikeDataset.FILE_SIZE, 1L);
        doc.save();
      }
    });
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);
    final long[] positions = SpikeTermsCounter.positions(database, "1 = 1");
    final SpikeTermsCounter.Counts counts = SpikeTermsCounter.count(segment, SpikeTermsCounter.matchBitmap(segment, positions), positions,
        dictionary.size());

    assertThat(SpikeTermsCounter.topN(counts, dictionary, 2)).containsExactly(new SpikeTermsCounter.Bucket("a", 5),
        new SpikeTermsCounter.Bucket("b", 5));
  }

  @Test
  void matchBeyondSegmentIsReportedNotCounted() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 2_000, 5L, 0.0);
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);
    SpikeDataset.populate(database, 50_000, 6L, 0.0);

    final long[] positions = SpikeTermsCounter.positions(database, "1 = 1");
    final SpikeTermsCounter.Counts counts = SpikeTermsCounter.count(segment, SpikeTermsCounter.matchBitmap(segment, positions), positions,
        dictionary.size());

    final long firstUncovered = (long) segment.pageCount() * segment.maxRecordsInPage();
    final long expectedUncovered = Arrays.stream(positions).filter(p -> p >= firstUncovered).count();
    assertThat(expectedUncovered).isGreaterThan(0L);
    assertThat(counts.uncovered()).isEqualTo(expectedUncovered);
    long covered = counts.missing();
    for (final int c : counts.byOrdinal())
      covered += c;
    assertThat(covered).isEqualTo(segment.rows()).isEqualTo(2_000L);
  }

  @Test
  void indexRangeMatchesSqlPositions() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 13L, 0.02);
    SpikeDataset.deleteEvery(database, 3);
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, new SpikeDictionary());

    final long[] uncovered = new long[1];
    final long[] bitmap = SpikeMatchSource.indexRange(database, segment, SpikeDataset.FILE_SIZE, 0L, 10_000L, uncovered);
    final long[] expected = SpikeTermsCounter.matchBitmap(segment, SpikeTermsCounter.positions(database, "fileSize < 10000"));

    assertThat(SpikeMatchSource.cardinality(expected)).isGreaterThan(0L);
    assertThat(bitmap).containsExactly(expected);
    assertThat(uncovered[0]).isZero();
  }

  @Test
  void indexRangeReportsPositionsBeyondTheSegment() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 2_000, 5L, 0.0);
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, new SpikeDictionary());
    SpikeDataset.populate(database, 50_000, 6L, 0.0);

    final long[] uncovered = new long[1];
    final long[] bitmap = SpikeMatchSource.indexRange(database, segment, SpikeDataset.FILE_SIZE, 0L, SpikeDataset.FILE_SIZE_MAX, uncovered);
    final long[] positions = SpikeTermsCounter.positions(database, "fileSize < " + SpikeDataset.FILE_SIZE_MAX);
    final long firstUncovered = (long) segment.pageCount() * segment.maxRecordsInPage();

    assertThat(uncovered[0]).isGreaterThan(0L).isEqualTo(Arrays.stream(positions).filter(p -> p >= firstUncovered).count());
    assertThat(bitmap).containsExactly(SpikeTermsCounter.matchBitmap(segment, positions));
  }

  @Test
  void columnEqualsMatchesSqlPositions() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 17L, 0.02);
    SpikeDataset.deleteEvery(database, 4);
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.STATUS, dictionary);

    final long[] expected = SpikeTermsCounter.matchBitmap(segment, SpikeTermsCounter.positions(database, "status = 'published'"));
    final long[] bitmap = SpikeMatchSource.columnEquals(segment, dictionary, "published");

    assertThat(SpikeMatchSource.cardinality(expected)).isGreaterThan(0L);
    assertThat(bitmap).containsExactly(expected);
    assertThat(SpikeMatchSource.cardinality(SpikeMatchSource.columnEquals(segment, dictionary, "no-such-status"))).isZero();
  }

  @Test
  void highCardinalityCountsEqualGroupBy() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 23L, 0.0);
    final String where = "fileSize < 500000";
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.AUTHOR, dictionary);

    final long[] positions = SpikeTermsCounter.positions(database, where);
    final SpikeTermsCounter.Counts counts = SpikeTermsCounter.count(segment, SpikeTermsCounter.matchBitmap(segment, positions), positions,
        dictionary.size());
    final long[] missing = new long[1];
    final List<SpikeTermsCounter.Bucket> oracle = SpikeTermsCounter.groupBy(database, where, SpikeDataset.AUTHOR,
        SpikeDataset.AUTHOR_CARDINALITY, missing);

    // far more distinct authors than the brand's 200, and nearly one group per match
    assertThat(dictionary.size()).isGreaterThan(5_000);
    assertThat(oracle.size()).isGreaterThan(5_000);
    assertThat(SpikeTermsCounter.topN(counts, dictionary, SpikeDataset.AUTHOR_CARDINALITY)).containsExactlyElementsOf(oracle);
    assertThat(counts.missing()).isEqualTo(missing[0]).isZero();
    assertThat(counts.uncovered()).isZero();
  }

  @Test
  void multiValuedCountsEqualUnwindGroupBy() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 29L, 0.0);
    // matching records without tags: one absent list, one empty list
    database.transaction(() -> {
      for (final List<String> tags : Arrays.asList(null, List.<String>of())) {
        final MutableDocument doc = database.newDocument(SpikeDataset.TYPE);
        doc.set(SpikeDataset.BRAND, "brand-000");
        doc.set(SpikeDataset.STATUS, "published");
        doc.set(SpikeDataset.FILE_SIZE, 1L);
        if (tags != null)
          doc.set(SpikeDataset.TAGS, tags);
        doc.save();
      }
    });
    final String where = "fileSize < 500000";
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeMultiSegment segment = SpikeMultiSegment.build(SpikeDataset.bucket(database), SpikeDataset.TAGS, dictionary);

    final long[] positions = SpikeTermsCounter.positions(database, where);
    final long[] bitmap = SpikeTermsCounter.matchBitmap(segment.presence().length, segment.maxRecordsInPage(), positions);
    final SpikeTermsCounter.Counts counts = SpikeTermsCounter.countMulti(segment, bitmap, dictionary.size());
    final List<SpikeTermsCounter.Bucket> oracle = SpikeTermsCounter.groupByUnwind(database, where, SpikeDataset.TAGS,
        SpikeDataset.TAG_CARDINALITY);

    assertThat(oracle.size()).isGreaterThan(500);
    assertThat(SpikeTermsCounter.topN(counts, dictionary, SpikeDataset.TAG_CARDINALITY)).containsExactlyElementsOf(oracle);
    assertThat(counts.missing()).isEqualTo(2L);
    assertThat(positions.length).isEqualTo(countWithTags(where) + 2);
  }

  private long countWithTags(final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM " + SpikeDataset.TYPE + " WHERE " + where
        + " AND tags IS NOT NULL AND tags.size() > 0")) {
      return ((Number) rs.next().getProperty("cnt")).longValue();
    }
  }

  @Test
  void recordsPerPageStayNearTheTarget() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 31L, 0.0);
    final double perPage = 20_000.0 / SpikeDataset.bucket(database).getTotalPages();
    assertThat(perPage).as("records per page " + perPage).isBetween(250.0, 300.0);
  }

  @ParameterizedTest
  @ValueSource(strings = { "fileSize < 10000", "status = 'published'", "status = 'archived' AND fileSize < 500000" })
  void blockSegmentCountsEqualHeapSegment(final String where) {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 50_000, 11L, 0.02);
    SpikeDataset.deleteEvery(database, 5);
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);
    final SpikeBlockSegment blocks = SpikeBlockSegment.build(segment);

    final long[] positions = SpikeTermsCounter.positions(database, where);
    final long[] bitmap = SpikeTermsCounter.matchBitmap(segment, positions);
    final SpikeTermsCounter.Counts expected = SpikeTermsCounter.count(segment, bitmap, positions, dictionary.size());
    final SpikeTermsCounter.Counts actual = SpikeTermsCounter.countBlocks(blocks, bitmap, dictionary.size());

    assertThat(blocks.blockCount()).isGreaterThan(1);
    assertThat(actual.byOrdinal()).containsExactly(expected.byOrdinal());
    assertThat(actual.missing()).isEqualTo(expected.missing());
    assertThat(actual.uncovered()).isEqualTo(expected.uncovered());
    assertThat(blocks.offHeapBytes()).isGreaterThan(0L);
  }

  @Test
  void blockOfOnlyMissingRowsDecodesToZeros() {
    SpikeDataset.createSchema(database);
    SpikeDataset.populate(database, 20_000, 5L, 1.0);
    final SpikeDictionary dictionary = new SpikeDictionary();
    final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);
    final SpikeBlockSegment blocks = SpikeBlockSegment.build(segment);
    final int[] scratch = new int[SpikeBlockSegment.BLOCK_ROWS];

    assertThat(dictionary.size()).isZero();
    assertThat(blocks.blockCount()).isEqualTo(2);
    final int first = blocks.decodeBlock(0, scratch);
    assertThat(first).isEqualTo(SpikeBlockSegment.BLOCK_ROWS);
    assertThat(Arrays.stream(scratch, 0, first).allMatch(v -> v == 0)).isTrue();
    final int last = blocks.decodeBlock(1, scratch);
    assertThat(last).isEqualTo((int) (segment.rows() - SpikeBlockSegment.BLOCK_ROWS));
    assertThat(Arrays.stream(scratch, 0, last).allMatch(v -> v == 0)).isTrue();
  }
}
