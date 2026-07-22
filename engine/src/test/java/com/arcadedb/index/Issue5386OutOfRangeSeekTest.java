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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * #5386: a range seek whose from-key sorts OUTSIDE the indexed [min,max] keyspace threw an NPE
 * ("convertedKeys is null") on a NOTUNIQUE single-field STRING index that compacted into several series.
 * <p>
 * Root cause (present in 26.5.1 / 26.7.3): for an open-ended range ({@code path >= X}, no upper bound) whose from-key
 * fell outside a compacted series' key range, {@code LSMTreeIndexCompacted.newIterators} fell back to a second lookup
 * against {@code toKeys}, which is {@code null} for an open-ended range. {@code lookupInPage(..., null, ...)} then
 * dereferenced {@code convertedKeys.length}. The #5214 rewrite (commit b0171877b) removed that fallback in favour of an
 * explicit full-series cursor, fixing this path; #5214's own tests only exercised bounded windows (both from and to
 * keys), so this test pins the open-ended out-of-range case.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5386OutOfRangeSeekTest extends TestHelper {

  private static final String TYPE_NAME  = "FileDoc";
  private static final int    TOTAL_KEYS = 80_000;
  private static final int    PAGE_SIZE  = 256 * 1024;
  private static final String KEY_PAD    = "x".repeat(64);

  private static String key(final int i) {
    return "K" + KEY_PAD + String.format("%08d", i);
  }

  @Override
  public void beforeTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private TypeIndex buildMultiSeriesIndex() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_RAM_MB, 1L);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("path", String.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "path" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL_KEYS; i++)
        database.newDocument(TYPE_NAME).set("path", key(i)).save();
    });

    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("path").getFirst();
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(((IndexInternal) typeIndex).scheduleCompaction()).as("compaction scheduled").isTrue();
    assertThat(((IndexInternal) typeIndex).compact()).as("compaction executed").isTrue();

    assertThat(bucketIndex.getMutableIndex().getSubIndex().newIterators(true, null, null))
        .as("test setup must produce multiple compacted series").hasSizeGreaterThan(2);

    return typeIndex;
  }

  @Test
  void seekBelowMinDoesNotThrow() throws Exception {
    buildMultiSeriesIndex();

    // in range: works
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", key(0))) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((String) rs.next().getProperty("path")).isEqualTo(key(0));
      }
    }).doesNotThrowAnyException();

    // below min: must not NPE, must return the smallest key
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "A")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((String) rs.next().getProperty("path")).isEqualTo(key(0));
      }
    }).doesNotThrowAnyException();

    // empty string (below every non-empty key)
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((String) rs.next().getProperty("path")).isEqualTo(key(0));
      }
    }).doesNotThrowAnyException();
  }

  @Test
  void seekAboveMaxDoesNotThrow() throws Exception {
    buildMultiSeriesIndex();

    // beyond max: must not NPE, must return no rows
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "z")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }).doesNotThrowAnyException();
  }

  /**
   * Reproduces the reporter's observed layout: the compacted segment's min is HIGHER than a newer mutable segment's
   * min (empty-string keys live only in the newer mutable pages). A {@code >= ''} seek is in-range for the mutable
   * segment but out-of-range-below for every compacted series.
   */
  @Test
  void seekBelowCompactedMinWithLowerMutableKeys() throws Exception {
    buildMultiSeriesIndex();

    // Now add records with keys that sort BELOW the compacted min into the (newer) mutable segment.
    database.transaction(() -> {
      for (int i = 0; i < 98; i++)
        database.newDocument(TYPE_NAME).set("path", "").save();          // empty-string keys: global minimum
      database.newDocument(TYPE_NAME).set("path", "A").save();
    });

    // equality on the empty string must find the 98 rows
    try (final ResultSet rs = database.query("sql",
        "SELECT count(*) AS c FROM " + TYPE_NAME + " WHERE path = ''")) {
      assertThat((Long) rs.next().getProperty("c")).isEqualTo(98L);
    }

    // >= '' : in-range for mutable, out-of-range-below for compacted series. Must not NPE.
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((String) rs.next().getProperty("path")).isEqualTo("");
      }
    }).doesNotThrowAnyException();

    // >= 'A' : also below the compacted min.
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT path FROM " + TYPE_NAME + " WHERE path >= ? LIMIT 1", "A")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }).doesNotThrowAnyException();
  }
}
