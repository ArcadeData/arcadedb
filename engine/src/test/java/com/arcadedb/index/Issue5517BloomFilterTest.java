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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5517: a point lookup must skip a compacted series that provably cannot hold the key, and must never skip one that
 * can.
 * <p>
 * The second half is the one that matters. A false positive costs the page read that would have happened anyway; a
 * false NEGATIVE hides a row - the lookup is skipped, the record is reported missing and a unique index lets a
 * duplicate through - with the index file intact and nothing to see in CHECK DATABASE. So every test here that reports
 * a saving also asserts, over the very same data, that not one key went missing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5517BloomFilterTest extends TestHelper {

  private static final String TYPE_NAME  = "Doc";
  private static final int    TOTAL_KEYS = 60_000;
  private static final int    PAGE_SIZE  = 256 * 1024;
  private static final String KEY_PAD    = "x".repeat(64);

  private static String key(final int i) {
    return "K" + KEY_PAD + String.format("%08d", i);
  }

  @Override
  protected void beginTest() {
    // The explicit compaction below must be the only one, not raced by a background round.
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Override
  protected void endTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.reset();
    GlobalConfiguration.INDEX_COMPACTION_RAM_MB.reset();
    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.reset();
  }

  /**
   * Builds a unique index over one bucket whose data lands in several compacted series, the layout a bulk load
   * produces and the only one where skipping a series is worth anything.
   */
  private TypeIndex buildMultiSeriesIndex(final int totalKeys) throws Exception {
    return buildMultiSeriesIndex(totalKeys, 3);
  }

  private TypeIndex buildMultiSeriesIndex(final int totalKeys, final int minSeries) throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("email", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(PAGE_SIZE).create();

    database.transaction(() -> {
      for (int i = 0; i < totalKeys; i++)
        database.newDocument(TYPE_NAME).set("email", key(i)).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").getFirst();
    assertThat(((IndexInternal) typeIndex).scheduleCompaction()).isTrue();
    assertThat(((IndexInternal) typeIndex).compact()).isTrue();

    assertThat(compactedOf(typeIndex).getSeriesCount())
        .as("test setup must produce multiple compacted series").isGreaterThanOrEqualTo(minSeries);

    return typeIndex;
  }

  private static LSMTreeIndexCompacted compactedOf(final TypeIndex typeIndex) {
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    return bucketIndex.getMutableIndex().getSubIndex();
  }

  /**
   * The gate. Every key written before the compaction has to come back after it, with the filters deciding which
   * series are read - the one failure mode a bloom filter must never have, asserted over every single key.
   */
  @Test
  void notOneKeyGoesMissingBehindTheFilters() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex(TOTAL_KEYS);
    final LSMTreeIndexCompacted compacted = compactedOf(typeIndex);

    assertThat(compacted.getBloomFilter()).as("the compaction must have written a filter file").isNotNull();
    assertThat(compacted.getBloomFilter().getPublishedFilters())
        .as("every series must carry a filter").isEqualTo(compacted.getSeriesCount());

    for (int i = 0; i < TOTAL_KEYS; i++) {
      final IndexCursor cursor = typeIndex.get(new Object[] { key(i) });
      assertThat(cursor.hasNext()).as("key %d must still be found", i).isTrue();
      cursor.close();
    }
  }

  /**
   * What the filters are for: a key that no series holds must not cost a single series read. With one filter per
   * series and a 1% target, all but a handful of the probes must come back negative.
   */
  @Test
  void anAbsentKeySkipsAlmostEverySeries() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex(TOTAL_KEYS);
    final LSMTreeIndexCompacted compacted = compactedOf(typeIndex);

    final long skippedBefore = compacted.getBloomSkippedSeries();
    final long probedBefore = compacted.getBloomProbedSeries();

    final int lookups = 2_000;
    for (int i = 0; i < lookups; i++) {
      // Inside the key range the series span - the root page alone cannot rule these out.
      final IndexCursor cursor = typeIndex.get(new Object[] { key(i) + "-absent" });
      assertThat(cursor.hasNext()).as("the key was never inserted").isFalse();
      cursor.close();
    }

    final long skipped = compacted.getBloomSkippedSeries() - skippedBefore;
    final long read = compacted.getBloomProbedSeries() - probedBefore;

    assertThat(skipped + read).as("every series must have been probed on every lookup")
        .isEqualTo((long) lookups * compacted.getSeriesCount());
    assertThat((double) read / (skipped + read))
        .as("false positives measured over %d lookups of %d series", lookups, compacted.getSeriesCount())
        .isLessThan(0.05);
  }

  /** A key that IS there must never be filtered out, however many series the lookup has to walk. */
  @Test
  void aPresentKeyIsNeverSkipped() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex(TOTAL_KEYS);
    final LSMTreeIndexCompacted compacted = compactedOf(typeIndex);

    final int seriesCount = compacted.getSeriesCount();
    final long skippedBefore = compacted.getBloomSkippedSeries();

    final int lookups = 500;
    for (int i = 0; i < lookups; i++) {
      final IndexCursor cursor = typeIndex.get(new Object[] { key(i * (TOTAL_KEYS / lookups)) });
      assertThat(cursor.hasNext()).isTrue();
      cursor.close();
    }

    // Each lookup skips at most every series but the one holding the key.
    assertThat(compacted.getBloomSkippedSeries() - skippedBefore)
        .isLessThanOrEqualTo((long) lookups * (seriesCount - 1));
  }

  /**
   * The filters live in a file of their own, so what a reopened database reads back from page 0 must decide exactly
   * what the compaction decided. A directory that came back subtly wrong - a series' page count, a block index, a
   * probe count - would filter keys the series really holds.
   */
  @Test
  void theFiltersSurviveAReopen() throws Exception {
    buildMultiSeriesIndex(TOTAL_KEYS);

    reopenDatabase();

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").getFirst();
    final LSMTreeIndexCompacted compacted = compactedOf(typeIndex);

    assertThat(compacted.getBloomFilter()).as("the filter file must be reattached at load").isNotNull();
    assertThat(compacted.getBloomFilter().getPublishedFilters()).isEqualTo(compacted.getSeriesCount());

    for (int i = 0; i < TOTAL_KEYS; i += 7) {
      final IndexCursor cursor = typeIndex.get(new Object[] { key(i) });
      assertThat(cursor.hasNext()).as("key %d must be found after a reopen", i).isTrue();
      cursor.close();
    }

    assertThat(compacted.getBloomSkippedSeries()).as("the reloaded filters must actually be skipping series")
        .isGreaterThan(0);
  }

  /**
   * The filters are an optimisation, so turning them off must change the cost of a lookup and nothing else - the same
   * database, the same files, the same answers. This is also what an older build sees: it does not know the
   * {@code .bfidx} extension, skips the file at load and reads every series.
   */
  @Test
  void turningTheFiltersOffChangesNothingButTheCost() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex(TOTAL_KEYS);
    final List<Boolean> withFilters = probeEveryKey(typeIndex, 11);
    assertThat(compactedOf(typeIndex).getBloomSkippedSeries()).isGreaterThan(0);

    GlobalConfiguration.INDEX_BLOOM_FILTER_RATE.setValue(0.0f);
    reopenDatabase();

    final TypeIndex reopenedIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").getFirst();
    assertThat(probeEveryKey(reopenedIndex, 11)).isEqualTo(withFilters);
    assertThat(compactedOf(reopenedIndex).getBloomSkippedSeries()).as("no series may be skipped with the filters off")
        .isZero();
  }

  private List<Boolean> probeEveryKey(final TypeIndex typeIndex, final int step) {
    final List<Boolean> found = new ArrayList<>();
    for (int i = 0; i < TOTAL_KEYS; i += step) {
      final IndexCursor present = typeIndex.get(new Object[] { key(i) });
      found.add(present.hasNext());
      present.close();

      final IndexCursor absent = typeIndex.get(new Object[] { key(i) + "-absent" });
      found.add(absent.hasNext());
      absent.close();
    }
    return found;
  }

  /**
   * A non-unique index maps one key to many RIDs, and a key with enough of them spans several leaf pages of a series.
   * The filter is built where keys are appended, so a key coming back for each of its chunks - and a key whose chunks
   * straddle two series - must be recorded in every series it reaches.
   */
  @Test
  void aNonUniqueKeySpanningPagesIsFoundInEverySeriesItReaches() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("city", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "city" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(65_536).create();

    final int cities = 40;
    final int perCity = 1_500;
    database.transaction(() -> {
      for (int i = 0; i < cities * perCity; i++)
        database.newDocument(TYPE_NAME).set("city", "city-" + String.format("%04d", i % cities)).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("city").getFirst();
    assertThat(((IndexInternal) typeIndex).scheduleCompaction()).isTrue();
    assertThat(((IndexInternal) typeIndex).compact()).isTrue();

    for (int c = 0; c < cities; c++) {
      int count = 0;
      final IndexCursor cursor = typeIndex.get(new Object[] { "city-" + String.format("%04d", c) });
      while (cursor.hasNext()) {
        cursor.next();
        ++count;
      }
      cursor.close();
      assertThat(count).as("every RID of city %d must be reachable", c).isEqualTo(perCity);
    }
  }

  /** Dropping the index must take its filter file with it, or the next open sees a file nothing can ever read. */
  @Test
  void droppingTheIndexRemovesTheFilterFile() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex(20_000, 1);
    final LSMTreeIndexBloomFilter filter = compactedOf(typeIndex).getBloomFilter();
    assertThat(filter).isNotNull();

    final File filterFile = filter.getOSFile();
    assertThat(filterFile).exists();

    database.getSchema().dropIndex(typeIndex.getName());

    assertThat(filterFile).as("the bloom filter file must go with the index").doesNotExist();
  }
}
