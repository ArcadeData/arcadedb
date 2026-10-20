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

import com.arcadedb.TestHelper;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10267: the sorted build sized its runs at a fixed 3,072 bytes per entry, about twenty times what an entry of a short key
 * retains, so a table that fitted the budget many times over was still spilled and merged, and the sorted build gained nothing over
 * the default one (at 6,000,000 rows of TPC-H's {@code lineitem} it spilled three runs with a budget that holds the whole table in
 * one). A run is now sized by what its entries retain, measured from the keys, so a table that fits the budget is built in a single
 * pass, a table that does not is spilled in runs as large as the budget allows, and a key that is repeated or small costs less than
 * one that is long.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10267SortedBuildRunSizingTest extends TestHelper {
  private static final int  ROWS            = 50_000;
  private static final long ONE_PASS_BUDGET = 16L << 20;

  private final AtomicReference<SortedIndexBuildMetrics> captured = new AtomicReference<>();

  @BeforeEach
  void captureSortedBuild() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(captured::set);
  }

  @AfterEach
  void releaseHook() {
    TypeIndexBuilder.setSortedBuildMetricsTestHook(null);
  }

  /** The shape of the report: ship dates as 'YYYY-MM-DD' strings over a few thousand distinct values, many rows each. */
  @Test
  void aLowCardinalityKeyThatFitsTheBudgetIsBuiltInOnePass() {
    final LocalDate first = LocalDate.of(1992, 1, 2);
    createType("LineItem", 8, Type.STRING);
    insert("LineItem", ROWS * 3, i -> first.plusDays(Math.floorMod(i * 7919L, 2_526L)).toString());

    final TypeIndex index = buildSorted("LineItem", ONE_PASS_BUDGET);

    assertOnePass(ROWS * 3);
    assertThat(index.countEntries()).isEqualTo(ROWS * 3L);
    try (final ResultSet resultSet = database.query("sql",
        "SELECT count() AS c FROM LineItem WHERE k >= '1994-01-01' AND k < '1995-01-01'")) {
      assertThat(resultSet.next().<Number>getProperty("c").longValue()).isEqualTo(expectedInYear(ROWS * 3, first));
    }
  }

  /** Every key distinct: the entries carry their own key, and still a run is measured by what they retain, not by a fixed 3 KB each. */
  @Test
  void aHighCardinalityKeyThatFitsTheBudgetIsBuiltInOnePass() {
    createType("Distinct", 3, Type.STRING);
    insert("Distinct", ROWS, i -> "key-%08d".formatted(Math.floorMod(i * 12_371L, ROWS)));

    final TypeIndex index = buildSorted("Distinct", ONE_PASS_BUDGET);

    // Before: 16 MB / 3,072 bytes = 5,461 entries a run, ten runs and a merge for 50,000 short keys
    assertOnePass(ROWS);
    assertThat(index.countEntries()).isEqualTo(ROWS);
    assertThat(index.get(new Object[] { "key-%08d".formatted(ROWS / 2) }).hasNext()).isTrue();
  }

  /** A table larger than the budget spills, in a handful of runs as large as the budget allows rather than dozens of tiny ones. */
  @Test
  void aTableLargerThanTheBudgetSpillsInFewLargeRuns() {
    createType("Spilled", 3, Type.STRING);
    insert("Spilled", ROWS, i -> "key-%08d".formatted(Math.floorMod(i * 12_371L, ROWS)));

    final TypeIndex index = buildSorted("Spilled", 4L << 20);

    // About 9 MB of entries at 4 MB a run is three runs; at 3,072 bytes an entry the same budget made 1,365 entries a run and 37 runs
    assertThat(captured.get()).isNotNull();
    assertThat(captured.get().initialRuns()).isBetween(2, 5);
    assertThat(index.countEntries()).isEqualTo(ROWS);
    assertThat(index.get(new Object[] { "key-%08d".formatted(17) }).hasNext()).isTrue();
  }

  /** The size of a run follows the size of the keys: the same row count with long keys needs runs where short ones fit in one pass. */
  @Test
  void aLongKeyFillsARunSoonerThanAShortOne() {
    createType("ShortKeys", 3, Type.STRING);
    createType("LongKeys", 3, Type.STRING);
    final String padding = "x".repeat(400);
    insert("ShortKeys", 10_000, i -> "key-%08d".formatted(Math.floorMod(i * 7_919L, 10_000L)));
    insert("LongKeys", 10_000, i -> "key-%08d-".formatted(Math.floorMod(i * 7_919L, 10_000L)) + padding);

    buildSorted("ShortKeys", 4L << 20);
    final int shortRuns = captured.get().initialRuns();
    buildSorted("LongKeys", 4L << 20);
    final int longRuns = captured.get().initialRuns();

    // 10,000 short entries retain about 2.4 MB and fit; 10,000 keys of 400 characters retain about 6.4 MB
    assertThat(shortRuns).isZero();
    assertThat(longRuns).isGreaterThanOrEqualTo(2);
  }

  /**
   * Short keys first, long keys later in the scan: the size of a key is followed as it changes, not averaged over the whole build.
   * An average over every key seen so far lags far behind a late jump and charges the long keys at a fraction of what they take, so
   * the run would grow past the budget.
   */
  @Test
  void keysThatGrowLateInTheScanAreChargedByTheirOwnSize() {
    createType("Growing", 1, Type.STRING);
    final String padding = "x".repeat(400);
    // 4,000 short keys retain about 1 MB and 8,000 keys of 400 characters about 5 MB: more than the 4 MB budget holds
    insert("Growing", 12_000, i -> i < 4_000 ? "key-%08d".formatted(i) : "key-%08d-".formatted(i) + padding);

    final TypeIndex index = buildSorted("Growing", 4L << 20);

    assertThat(captured.get().initialRuns()).isGreaterThanOrEqualTo(2);
    assertThat(index.countEntries()).isEqualTo(12_000);
    assertThat(index.get(new Object[] { "key-%08d-".formatted(11_999) + padding }).hasNext()).isTrue();
  }

  private TypeIndex buildSorted(final String typeName, final long budget) {
    captured.set(null);
    return database.getSchema().buildTypeIndex(typeName, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withBuildMode(IndexBuildMode.SORTED).withBuildMemoryBudget(budget).withUnique(false).create();
  }

  private void assertOnePass(final long rows) {
    assertThat(captured.get()).as("the sorted build ran").isNotNull();
    assertThat(captured.get().initialRuns()).as("runs spilled").isZero();
    assertThat(captured.get().totalSpillBytes()).as("bytes spilled").isZero();
    assertThat(captured.get().logicalEntries()).isEqualTo(rows);
  }

  private void createType(final String typeName, final int buckets, final Type keyType) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
    type.createProperty("k", keyType);
  }

  private void insert(final String typeName, final int rows, final LongFunction<String> key) {
    for (int from = 0; from < rows; from += 10_000) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(rows, start + 10_000); i++)
          database.newDocument(typeName).set("k", key.apply(i)).save();
      });
    }
  }

  private static long expectedInYear(final int rows, final LocalDate first) {
    long count = 0;
    for (long i = 0; i < rows; i++) {
      final String date = first.plusDays(Math.floorMod(i * 7919L, 2_526L)).toString();
      if (date.compareTo("1994-01-01") >= 0 && date.compareTo("1995-01-01") < 0)
        count++;
    }
    return count;
  }
}
