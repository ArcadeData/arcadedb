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
package com.arcadedb.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10267: {@code CREATE INDEX ... NOTUNIQUE} over a populated type took the record-at-a-time build whatever the size, and
 * 23% of its time went to the transaction bookkeeping of every key (the lock on the key, the ordered set of keys of the
 * transaction, the commit of them), which a bulk build over existing records does not need. The sorted build skips it and, when the
 * keys fit its memory budget, takes about two thirds of the time. A NOTUNIQUE LSM_TREE index over a type holding at least
 * {@link GlobalConfiguration#INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS} records now takes it, under the same conditions as the UNIQUE
 * one of #10334, and gives the same index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10267NonUniqueSortedBuildChoiceTest extends TestHelper {
  private static final int ROWS = 6_000;

  private final List<SortedIndexBuildMetrics> sortedBuilds = new ArrayList<>();

  @BeforeEach
  void captureSortedBuilds() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(sortedBuilds::add);
  }

  @AfterEach
  void releaseHook() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
  }

  @Test
  void typeAtTheThresholdTakesTheSortedBuildAndGivesTheIndexOfTheDefaultOne() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
    createPopulatedType("Sorted");
    createPopulatedType("Plain");

    database.command("sql", "CREATE INDEX ON Sorted (k) NOTUNIQUE").close();
    assertThat(sortedBuilds).hasSize(1);
    assertThat(sortedBuilds.getFirst().unique()).isFalse();
    assertThat(sortedBuilds.getFirst().logicalEntries()).isEqualTo(ROWS);

    // The same data, the same statement, the record-at-a-time build: the threshold is what tells them apart
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 0L);
    database.command("sql", "CREATE INDEX ON Plain (k) NOTUNIQUE").close();
    assertThat(sortedBuilds).hasSize(1);

    final List<String> sorted = entries(database.getSchema().getType("Sorted").getIndexByProperties("k"));
    final List<String> plain = entries(database.getSchema().getType("Plain").getIndexByProperties("k"));
    assertThat(sorted).hasSize(ROWS);
    // The order of the records of ONE key is not part of what an index promises, the order of the keys and the set of entries are
    assertThat(keysOnly(sorted)).as("key order").isEqualTo(keysOnly(plain));
    assertThat(sorted.stream().sorted().toList()).as("entries").isEqualTo(plain.stream().sorted().toList());

    reopenDatabase();

    final List<String> reopened = entries(database.getSchema().getType("Sorted").getIndexByProperties("k"));
    assertThat(keysOnly(reopened)).isEqualTo(keysOnly(plain));
    assertThat(reopened.stream().sorted().toList()).isEqualTo(plain.stream().sorted().toList());
    database.transaction(() -> database.newDocument("Sorted").set("k", "after-reopen").save());
    assertThat(count(database.getSchema().getType("Sorted").getIndexByProperties("k").get(new Object[] { "after-reopen" })))
        .isEqualTo(1);
  }

  @Test
  void typeBelowTheThresholdKeepsTheRecordAtATimeBuild() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, ROWS + 1L);
    createPopulatedType("Work");

    database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();

    assertThat(sortedBuilds).isEmpty();
    assertThat(database.getSchema().getType("Work").getIndexByProperties("k").countEntries()).isEqualTo(ROWS);
  }

  @Test
  void thresholdZeroDisablesTheSortedBuild() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 0L);
    createPopulatedType("Work");

    database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();

    assertThat(sortedBuilds).isEmpty();
    assertThat(database.getSchema().getType("Work").getIndexByProperties("k").countEntries()).isEqualTo(ROWS);
  }

  /** The two thresholds are independent: the one of UNIQUE indexes does not switch a NOTUNIQUE build, nor the other way round. */
  @Test
  void eachKindOfIndexFollowsItsOwnThreshold() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 1L);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 0L);
    createPopulatedType("Work");
    database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();
    assertThat(sortedBuilds).isEmpty();

    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS, 0L);
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, 1L);
    final DocumentType other = database.getSchema().buildDocumentType().withName("Other").withTotalBuckets(1).create();
    other.createProperty("k", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Other").set("k", "distinct-%06d".formatted(i)).save();
    });
    database.command("sql", "CREATE INDEX ON Other (k) UNIQUE").close();
    assertThat(sortedBuilds).isEmpty();
  }

  @Test
  void recordsOfSubTypesCountTowardsTheThreshold() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
    database.getSchema().buildDocumentType().withName("Work").withTotalBuckets(1).create().createProperty("k", Type.STRING);
    database.getSchema().buildDocumentType().withName("Article").withSuperType("Work").withTotalBuckets(1).create();
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument(i % 2 == 0 ? "Work" : "Article").set("k", key(i)).save();
    });

    database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();

    assertThat(sortedBuilds).hasSize(1);
    assertThat(database.getSchema().getType("Work").getIndexByProperties("k").countEntries()).isEqualTo(ROWS);
  }

  /** The shape of an HTTP command: CREATE INDEX inside a transaction that has written nothing yet is handed back usable. */
  @Test
  void emptyCallerTransactionIsHandedBackUsable() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
    createPopulatedType("Work");

    database.begin();
    database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();
    assertThat(sortedBuilds).hasSize(1);
    assertThat(database.isTransactionActive()).isTrue();
    database.newDocument("Work").set("k", "after-index").save();
    database.commit();

    final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("k");
    assertThat(count(index.get(new Object[] { "after-index" }))).isEqualTo(1);
    assertThat(index.countEntries()).isEqualTo(ROWS + 1L);
  }

  /** The sorted build cannot share the caller's transaction: the build has to see what that transaction wrote. */
  @Test
  void buildInsideATransactionWithPendingChangesKeepsTheRecordAtATimeBuild() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
    createPopulatedType("Work");

    database.transaction(() -> {
      database.newDocument("Work").set("k", "uncommitted").save();
      database.command("sql", "CREATE INDEX ON Work (k) NOTUNIQUE").close();
    });

    assertThat(sortedBuilds).isEmpty();
    final TypeIndex index = database.getSchema().getType("Work").getIndexByProperties("k");
    assertThat(index.countEntries()).isEqualTo(ROWS + 1L);
    assertThat(count(index.get(new Object[] { "uncommitted" }))).isEqualTo(1);
  }

  /** Repeated keys, many records on each: the shape of a low-cardinality column, the one the report measured. */
  @Test
  void lowCardinalityColumnKeepsEveryRecordOfEveryKey() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_BUILD_SORTED_NON_UNIQUE_MIN_RECORDS, (long) ROWS);
    final DocumentType type = database.getSchema().buildDocumentType().withName("Lines").withTotalBuckets(3).create();
    type.createProperty("k", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Lines").set("k", "1994-01-%02d".formatted(1 + i % 28)).save();
    });

    database.command("sql", "CREATE INDEX ON Lines (k) NOTUNIQUE").close();

    assertThat(sortedBuilds).hasSize(1);
    final TypeIndex index = database.getSchema().getType("Lines").getIndexByProperties("k");
    assertThat(index.countEntries()).isEqualTo(ROWS);
    for (int day = 1; day <= 28; day++)
      assertThat(count(index.get(new Object[] { "1994-01-%02d".formatted(day) }))).isEqualTo(ROWS / 28 + (day <= ROWS % 28 ? 1 : 0));
  }

  private void createPopulatedType(final String typeName) {
    // One bucket, so two types loaded in the same order give the same record positions
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(1).create();
    type.createProperty("k", Type.STRING);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument(typeName).set("k", key(i)).save();
    });
  }

  /** Scrambled, so the keys never arrive sorted, and each one repeated by a handful of records. */
  private static String key(final int i) {
    return "key-%06d".formatted(Math.floorMod(i * 7_919, ROWS / 3));
  }

  private static List<String> keysOnly(final List<String> entries) {
    return entries.stream().map(e -> e.substring(0, e.indexOf('@'))).toList();
  }

  private static List<String> entries(final TypeIndex index) {
    final List<String> out = new ArrayList<>();
    final IndexCursor cursor = index.iterator(true);
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      out.add(Arrays.toString(cursor.getKeys()) + "@" + rid.getPosition());
    }
    return out;
  }

  private static int count(final IndexCursor cursor) {
    int total = 0;
    while (cursor.hasNext()) {
      cursor.next();
      ++total;
    }
    return total;
  }
}
