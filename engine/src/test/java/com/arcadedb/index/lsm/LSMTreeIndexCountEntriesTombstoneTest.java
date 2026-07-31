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
package com.arcadedb.index.lsm;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #5601 (1): {@code countEntries()} used to count tombstones as live entries.
 * <p>
 * On any LSM index the count settled on a residual that only a full compaction cleared: deleting every record
 * of a type left {@code countEntries() == 1} with zero records in the database. The cause is not the tombstones
 * themselves - the cursor already skips dead keys - but the fact that {@link LSMTreeIndex#countEntries()}
 * incremented once per {@code next()} call while {@link LSMTreeIndexCursor#next()} legitimately answers
 * {@code null} after an optimistic {@code hasNext()} when a trailing run of tombstones leaves nothing to emit.
 * <p>
 * Deletions must be in SEPARATE transactions: within one transaction {@code TransactionIndexContext} collapses
 * a REMOVE followed by an ADD on the same (key, RID) pair, so no tombstone ever reaches the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexCountEntriesTombstoneTest extends TestHelper {
  private static final String   TYPE_NAME = "T";
  private static final String[] CITIES    = { "Rome", "Milan", "Naples", "Palermo", "Turin" };

  @Override
  public void beforeTest() {
    // keep compaction manual: a full compaction resolves and drops the tombstones, hiding the defect
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Test
  void countEntriesIgnoresTombstonesOnDeleteOneByOne() {
    createTypeAndIndex();

    final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[name]");
    assertThat(index.countEntries()).isEqualTo(CITIES.length);

    long expected = CITIES.length;
    for (final String city : CITIES) {
      database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE name = '" + city + "'"));
      --expected;
      assertThat(index.countEntries()).as("after deleting '%s'", city).isEqualTo(expected);
    }

    assertThat(database.countType(TYPE_NAME, true)).isZero();
    assertThat(index.countEntries()).isZero();
  }

  @Test
  void countEntriesIgnoresTombstonesOnBulkDelete() {
    createTypeAndIndex();

    final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[name]");
    assertThat(index.countEntries()).isEqualTo(CITIES.length);

    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME));

    assertThat(database.countType(TYPE_NAME, true)).isZero();
    assertThat(index.countEntries()).isZero();
  }

  @Test
  void countEntriesMatchesTheCursorOnAPartiallyDeletedIndex() {
    createTypeAndIndex();

    // delete a strict subset, leaving a live entry AFTER the tombstone run in key order ("Turin" sorts last)
    for (final String city : new String[] { "Milan", "Naples", "Palermo" })
      database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE name = '" + city + "'"));

    final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[name]");

    long browsed = 0;
    final IndexCursor cursor = ((TypeIndex) index).iterator(true);
    try {
      while (cursor.hasNext())
        if (cursor.next() != null)
          ++browsed;
    } finally {
      cursor.close();
    }

    assertThat(index.countEntries()).isEqualTo(browsed).isEqualTo(2);
  }

  @Test
  void countEntriesIsZeroAfterDeletingEveryRecordAndCompacting() throws Exception {
    createTypeAndIndex();

    database.transaction(() -> database.command("sql", "DELETE FROM " + TYPE_NAME));

    final Index index = database.getSchema().getIndexByName(TYPE_NAME + "[name]");
    for (final Index bucketIndex : ((TypeIndex) index).getIndexesOnBuckets()) {
      ((LSMTreeIndex) bucketIndex).scheduleCompaction();
      ((LSMTreeIndex) bucketIndex).compact();
    }

    assertThat(index.countEntries()).isZero();
  }

  private void createTypeAndIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME);
    database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".name STRING");
    database.command("sql", "CREATE INDEX ON " + TYPE_NAME + " (name) NOTUNIQUE");

    database.transaction(() -> {
      for (final String city : CITIES)
        database.command("sql", "INSERT INTO " + TYPE_NAME + " SET name = '" + city + "'");
    });
  }
}
