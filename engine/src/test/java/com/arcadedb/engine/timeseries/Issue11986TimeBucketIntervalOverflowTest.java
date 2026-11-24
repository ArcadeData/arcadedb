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
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #11986: the interval text of {@code ts.timeBucket} was turned into milliseconds with an unchecked
 * multiplication, and a positive wrap was used as the interval: {@code '30500568905w'} grouped by buckets of 34,448,384 ms
 * (about 9.6 hours) without an error, in the projection, in the aggregation push-down and in a continuous aggregate.
 */
class Issue11986TimeBucketIntervalOverflowTest extends TestHelper {

  @Test
  void anIntervalWhoseMillisecondsOverflowALongIsRefused() {
    // positive wraps (30500568905w, 2562047788015216h) and negative ones (15250284453w, 9223372036854776s)
    for (final String interval : new String[] { "30500568905w", "15250284453w", "2562047788015216h", "9223372036854776s" })
      assertThatThrownBy(() -> SQLFunctionTimeBucket.parseInterval(interval))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("too large");

    assertThatThrownBy(() -> SQLFunctionTimeBucket.parseInterval("99999999999999999999h"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not fit a long");

    // the largest amount of each unit still parses, exactly
    assertThat(SQLFunctionTimeBucket.parseInterval((Long.MAX_VALUE / 3_600_000L) + "h"))
        .isEqualTo(Long.MAX_VALUE / 3_600_000L * 3_600_000L);
    assertThat(SQLFunctionTimeBucket.parseInterval((Long.MAX_VALUE / 604_800_000L) + "w"))
        .isEqualTo(Long.MAX_VALUE / 604_800_000L * 604_800_000L);
    assertThat(SQLFunctionTimeBucket.parseInterval("1h")).isEqualTo(3_600_000L);
  }

  @Test
  void aQueryWithAnOverflowingIntervalFailsInsteadOfGroupingByAnotherWidth() {
    createType();

    // the push-down shape (GROUP BY the bucket alias) and the generic projection
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT ts.timeBucket('30500568905w', ts) AS b, count(*) AS c FROM M GROUP BY b")) {
        while (rs.hasNext())
          rs.next();
      }
    }).hasMessageContaining("too large");
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT ts.timeBucket('2562047788015216h', ts) AS b FROM M")) {
        while (rs.hasNext())
          rs.next();
      }
    }).hasMessageContaining("too large");
    // the offset goes through the same parser
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT ts.timeBucket('1h', ts, {'offset': '-30500568905w'}) AS b FROM M")) {
        while (rs.hasNext())
          rs.next();
      }
    }).hasMessageContaining("too large");
  }

  @Test
  void aContinuousAggregateWithAnOverflowingIntervalIsRefusedAndLeavesNothingBehind() {
    createType();

    assertThatThrownBy(() -> database.command("sql",
        "CREATE CONTINUOUS AGGREGATE CA AS SELECT ts.timeBucket('30500568905w', ts) AS b, count(*) AS c FROM M GROUP BY b"))
        .hasMessageContaining("too large");

    assertThat(database.getSchema().existsContinuousAggregate("CA")).isFalse();
    assertThat(database.getSchema().existsType("CA")).isFalse();
  }

  @Test
  void aContinuousAggregateWithAZeroIntervalIsRefusedEvenOverAnEmptyType() {
    // over an empty type the initial refresh evaluates no row, so nothing used to refuse '0s': the aggregate was created with a
    // query that ts.timeBucket refuses ('must be a positive amount of time') on the first row it evaluates
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE CONTINUOUS AGGREGATE CA AS SELECT ts.timeBucket('0s', ts) AS b, count(*) AS c FROM M GROUP BY b"))
        .hasMessageContaining("must be a positive amount of time");

    assertThat(database.getSchema().existsContinuousAggregate("CA")).isFalse();
    assertThat(database.getSchema().existsType("CA")).isFalse();
  }

  private void createType() {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO M SET ts = 0, host = 'a', v = 1.0");
      database.command("sql", "INSERT INTO M SET ts = 40000000, host = 'a', v = 2.0");
      database.command("sql", "INSERT INTO M SET ts = 80000000, host = 'a', v = 3.0");
    });
  }
}
