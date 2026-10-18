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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10030: the conversion-and-refusal loop that turns the raw values of an ingestion batch into the grid
 * {@link TimeSeriesEngine#appendBatch(long[], Object[][])} takes lives in one engine-level helper,
 * {@link TimeSeriesStorableRows}, shared by {@link TimeSeriesGateway#write} (line protocol, gRPC) and the Prometheus remote-write
 * handler, so there is one implementation of the conversion contract instead of two.
 */
class Issue10030StorableRowsTest extends TestHelper {

  private static final List<ColumnDefinition> COLUMNS = List.of(//
      new ColumnDefinition("ts", Type.DATETIME, ColumnDefinition.ColumnRole.TIMESTAMP),//
      new ColumnDefinition("host", Type.INTEGER, ColumnDefinition.ColumnRole.TAG),//
      new ColumnDefinition("v", Type.INTEGER, ColumnDefinition.ColumnRole.FIELD),//
      new ColumnDefinition("w", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  @Test
  void cleanRowsAreConvertedIntoTheGridWithoutDroppingAnything() {
    final Object[][] raw = { { "7", 1, 1.5 }, { "8", "2", 2.5 } };
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20 }, 2, false,
        (row, column, def) -> raw[row][column], unreadable);

    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.refused()).isZero();
    assertThat(rows.sharedValueRefused()).isFalse();
    assertThat(rows.timestamps()).containsExactly(10, 20);
    assertThat(rows.columnValues()).hasDimensions(3, 2);
    assertThat(rows.columnValues()[0]).as("the INTEGER tag sent as text is stored as a number").containsExactly(7, 8);
    assertThat(rows.columnValues()[1]).containsExactly(1, 2);
    assertThat(rows.columnValues()[2]).containsExactly(1.5, 2.5);
    assertThat(unreadable).isEmpty();
  }

  @Test
  void aValueAColumnCannotHoldDropsOnlyItsRowAndIsNamed() {
    final Object[][] raw = { { "7", 1, 1.0 }, { "8", 1e12, 2.0 }, { "9", 3, 3.0 } };
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20, 30 }, 3,
        false, (row, column, def) -> raw[row][column], unreadable);

    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.refused()).isEqualTo(1);
    assertThat(rows.sharedValueRefused()).isFalse();
    assertThat(rows.timestamps()).as("the refused row's timestamp is compacted away").containsExactly(10, 30);
    assertThat(rows.columnValues()[0]).containsExactly(7, 9);
    assertThat(rows.columnValues()[1]).containsExactly(1, 3);
    assertThat(rows.columnValues()[2]).containsExactly(1.0, 3.0);
    assertThat(unreadable).singleElement().asString().startsWith("m.v (field): ");
  }

  @Test
  void aRowIsRefusedAtItsFirstUnreadableColumnAndTheRestOfItIsNotRead() {
    final AtomicInteger reads = new AtomicInteger();
    final Object[][] raw = { { "not-a-number", "also-not", 1.0 } };
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10 }, 1, false,
        (row, column, def) -> {
          reads.incrementAndGet();
          return raw[row][column];
        }, unreadable);

    assertThat(rows.size()).isZero();
    assertThat(rows.refused()).isEqualTo(1);
    assertThat(reads.get()).as("the columns after the refused one are not read").isEqualTo(1);
    assertThat(unreadable).singleElement().asString().startsWith("m.host (tag): ");
  }

  @Test
  void aSharedTagIsReadAndConvertedOnceAndBroadcastDownItsColumn() {
    final AtomicInteger tagReads = new AtomicInteger();
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20, 30 }, 3, true,
        (row, column, def) -> {
          if (def.getRole() == ColumnDefinition.ColumnRole.TAG) {
            tagReads.incrementAndGet();
            return "42";
          }
          return row;
        }, unreadable);

    assertThat(tagReads.get()).as("a shared tag is read once per batch, not once per row").isEqualTo(1);
    assertThat(rows.size()).isEqualTo(3);
    assertThat(rows.columnValues()[0]).containsExactly(42, 42, 42);
    assertThat(rows.columnValues()[1]).containsExactly(0, 1, 2);
  }

  @Test
  void anUnreadableSharedTagRefusesEveryRowOfTheBatch() {
    final AtomicInteger fieldReads = new AtomicInteger();
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20 }, 2, true,
        (row, column, def) -> {
          if (def.getRole() == ColumnDefinition.ColumnRole.TAG)
            return "not-a-number";
          fieldReads.incrementAndGet();
          return 1;
        }, unreadable);

    assertThat(rows.size()).isZero();
    assertThat(rows.refused()).isEqualTo(2);
    assertThat(rows.sharedValueRefused()).isTrue();
    assertThat(fieldReads.get()).as("no row is read once the batch is refused").isZero();
    assertThat(unreadable).singleElement().asString().startsWith("m.host (tag): ");
  }

  @Test
  void theReportedReasonsAreCapped() {
    final int count = TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES + 50;
    final Set<String> unreadable = new LinkedHashSet<>();
    final List<ColumnDefinition> columns = List.of(//
        new ColumnDefinition("ts", Type.DATETIME, ColumnDefinition.ColumnRole.TIMESTAMP),//
        new ColumnDefinition("v", Type.INTEGER, ColumnDefinition.ColumnRole.FIELD));

    // A distinct reason per row: the column is the same, so the message must differ through the measurement name
    for (int i = 0; i < count; i++)
      TimeSeriesStorableRows.convert(database, "m" + i, columns, null, new long[] { i }, 1, false, (row, column, def) -> "x", unreadable);

    assertThat(unreadable).hasSize(TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES);
  }

  @Test
  void aNullValueIsStoredAsAbsentAndRefusesNothing() {
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10 }, 1, false,
        (row, column, def) -> null, unreadable);

    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.columnValues()[0]).containsExactly((Object) null);
    assertThat(unreadable).isEmpty();
  }
}
