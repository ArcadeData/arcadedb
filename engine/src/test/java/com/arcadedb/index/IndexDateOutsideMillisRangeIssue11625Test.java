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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11625: a DATE index compared its keys through epoch milliseconds, so a date the millisecond range
 * cannot hold (before year -292275054 or after 292278993, the extremes Neo4j's ExtremeValuesLibrary tests) failed every
 * insert and range lookup on the index with "long overflow" once it was a key, and could not be inserted next to another key.
 */
class IndexDateOutsideMillisRangeIssue11625Test extends TestHelper {

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE", "UNIQUE" })
  void anIndexedDateOutsideTheMillisRangeIsOrderedAndComparedAsUnindexed(final String kind) {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE PROPERTY T.d DATE");
      database.command("sql", "CREATE INDEX ON T (d) " + kind);
    });
    // the extremes first, so they are keys before the ordinary dates arrive, then in the other order
    final List<String> dates = List.of("-999999999-01-01", "+999999999-12-31", "2024-01-01", "-292275055-01-01", "1970-01-01",
        "+292278994-01-01", "-0001-12-31");
    for (final String date : dates)
      database.transaction(() -> database.command("sql", "INSERT INTO T SET d = ?", LocalDate.parse(date)));

    final List<LocalDate> sorted = dates.stream().map(LocalDate::parse).sorted().toList();
    assertThat(dates("SELECT d FROM T ORDER BY d")).containsExactlyElementsOf(sorted);
    assertThat(dates("SELECT d FROM T WHERE d > '1900-01-01' ORDER BY d")).containsExactly(LocalDate.parse("1970-01-01"),
        LocalDate.parse("2024-01-01"), LocalDate.parse("+292278994-01-01"), LocalDate.parse("+999999999-12-31"));
    assertThat(dates("SELECT d FROM T WHERE d < ? ORDER BY d", LocalDate.parse("-292275055-01-01"))).containsExactly(
        LocalDate.parse("-999999999-01-01"));
    assertThat(dates("SELECT d FROM T WHERE d = ?", LocalDate.parse("-999999999-01-01"))).containsExactly(
        LocalDate.parse("-999999999-01-01"));
    assertThat(dates("SELECT d FROM T WHERE d = ?", LocalDate.parse("+292278994-01-01"))).containsExactly(
        LocalDate.parse("+292278994-01-01"));
  }

  private List<LocalDate> dates(final String query, final Object... args) {
    try (final ResultSet rs = database.query("sql", query, args)) {
      return rs.stream().map(r -> r.<LocalDate>getProperty("d")).toList();
    }
  }
}
