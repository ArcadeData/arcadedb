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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5214: bounded (range/partial) scans over an LSM index that compacted into several series must return every key
 * exactly once and in the requested order.
 * <p>
 * Several compacted series inside one file are produced by a RAM-bounded compaction ({@code INDEX_COMPACTION_RAM_MB}),
 * i.e. on large indexes whose immutable pages exceed the compaction budget. Two defects lived on that layout:
 * <ul>
 * <li>a series lying wholly inside {@code [fromKeys, toKeys]} (containing neither endpoint) produced no cursor in
 * {@code LSMTreeIndexCompacted.newIterators}, so every interior series was dropped and range scans returned partial
 * results;</li>
 * <li>{@code iterator(false, fromKeys, inclusive)} delegated to the direction-auto-detecting range with a {@code null}
 * begin key, which defaults to ascending, so a descending partial scan iterated ascending.</li>
 * </ul>
 * The existing partial-scan tests only asserted element counts, never completeness of the set or the ordering, so both
 * defects stayed latent. These tests assert both.
 */
@Tag("slow")
class Issue5214MultiSeriesRangeTest extends TestHelper {

  private static final String TYPE_NAME = "Doc";
  private static final int    TOTAL_KEYS = 80_000;
  private static final int    LO         = 20_000;
  private static final int    HI         = 60_000;
  private static final int    PAGE_SIZE  = 256 * 1024;
  private static final String KEY_PAD    = "x".repeat(64);

  private static String key(final int i) {
    return "K" + KEY_PAD + String.format("%08d", i);
  }

  @Override
  public void beforeTest() {
    // Disable AUTO compaction so the explicit compaction below is the only one and is not raced by a background one.
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  /**
   * Builds a non-unique LSM index over a single bucket whose data lives in more than one compacted series, all emitted
   * by a single RAM-bounded compaction round.
   */
  private TypeIndex buildMultiSeriesIndex() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("email", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "email" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_KEYS; i++)
        database.newDocument(TYPE_NAME).set("email", key(i)).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("email").getFirst();
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(((IndexInternal) typeIndex).scheduleCompaction()).as("compaction scheduled").isTrue();
    assertThat(((IndexInternal) typeIndex).compact()).as("compaction executed").isTrue();

    assertThat(bucketIndex.getMutableIndex().getSubIndex().newIterators(true, null, null))
        .as("test setup must produce multiple compacted series").hasSizeGreaterThan(2);

    return typeIndex;
  }

  /** Drains a cursor, asserting strict monotonic order in the given direction, and returns the emitted keys. */
  private static List<String> drain(final IndexCursor cursor, final boolean ascending) {
    final List<String> keys = new ArrayList<>();
    try {
      String previous = null;
      while (cursor.hasNext()) {
        cursor.next();
        final String current = (String) cursor.getKeys()[0];
        if (previous != null) {
          if (ascending)
            assertThat(current).as("keys must be strictly ascending").isGreaterThan(previous);
          else
            assertThat(current).as("keys must be strictly descending").isLessThan(previous);
        }
        previous = current;
        keys.add(current);
      }
    } finally {
      cursor.close();
    }
    return keys;
  }

  private static List<String> expected(final int fromInclusive, final int toInclusive, final boolean ascending) {
    final List<String> keys = new ArrayList<>();
    if (ascending)
      for (int i = fromInclusive; i <= toInclusive; i++)
        keys.add(key(i));
    else
      for (int i = toInclusive; i >= fromInclusive; i--)
        keys.add(key(i));
    return keys;
  }

  @Test
  void ascendingRangeWindowReturnsEveryKey() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex();
    // Window spanning several series; whole series fall strictly inside and must not be dropped.
    final IndexCursor cursor = typeIndex.range(true, new Object[] { key(LO) }, true, new Object[] { key(HI) }, true);
    assertThat(drain(cursor, true)).isEqualTo(expected(LO, HI, true));
  }

  @Test
  void descendingRangeWindowReturnsEveryKey() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex();
    // Descending window: begin=high bound, end=low bound.
    final IndexCursor cursor = typeIndex.range(false, new Object[] { key(HI) }, true, new Object[] { key(LO) }, true);
    assertThat(drain(cursor, false)).isEqualTo(expected(LO, HI, false));
  }

  @Test
  void ascendingPartialFromReturnsEveryKey() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex();
    final IndexCursor cursor = typeIndex.iterator(true, new Object[] { key(LO) }, true);
    assertThat(drain(cursor, true)).isEqualTo(expected(LO, TOTAL_KEYS - 1, true));
  }

  @Test
  void descendingPartialFromReturnsEveryKeyInDescendingOrder() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex();
    final IndexCursor cursor = typeIndex.iterator(false, new Object[] { key(HI) }, true);
    assertThat(drain(cursor, false)).isEqualTo(expected(0, HI, false));
  }

  @Test
  void exclusiveRangeWindowExcludesEndpoints() throws Exception {
    final TypeIndex typeIndex = buildMultiSeriesIndex();
    final IndexCursor asc = typeIndex.range(true, new Object[] { key(LO) }, false, new Object[] { key(HI) }, false);
    assertThat(drain(asc, true)).isEqualTo(expected(LO + 1, HI - 1, true));
    final IndexCursor desc = typeIndex.range(false, new Object[] { key(HI) }, false, new Object[] { key(LO) }, false);
    assertThat(drain(desc, false)).isEqualTo(expected(LO + 1, HI - 1, false));
  }

  @Test
  void sqlRangeCountIsComplete() throws Exception {
    buildMultiSeriesIndex();
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE email >= ? AND email <= ?", key(LO), key(HI))) {
      assertThat((Long) rs.next().getProperty("c")).isEqualTo((long) (HI - LO + 1));
    }
  }

  @Test
  void sqlOrderByDescIsCompleteAndOrdered() throws Exception {
    buildMultiSeriesIndex();
    // The planner satisfies ORDER BY ... DESC directly from the index descending scan (no explicit sort), so this
    // exercises the end-to-end descending range path over multiple compacted series.
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT email FROM " + TYPE_NAME + " WHERE email <= ? ORDER BY email DESC", key(HI))) {
      String previous = null;
      while (rs.hasNext()) {
        final String current = rs.next().getProperty("email");
        if (previous != null)
          assertThat(current).as("SQL ORDER BY DESC keys must be strictly descending").isLessThan(previous);
        previous = current;
        keys.add(current);
      }
    }
    assertThat(keys).hasSize(HI + 1);
    assertThat(keys.getFirst()).isEqualTo(key(HI));
    assertThat(keys.getLast()).isEqualTo(key(0));
  }
}
