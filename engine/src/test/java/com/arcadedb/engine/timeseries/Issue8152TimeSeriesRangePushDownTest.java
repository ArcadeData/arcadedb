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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8152 (correlated finding)
 * <p>
 * {@code SelectExecutionPlanner.toEpochMs} is the fourth near-copy of the same date-to-millis conversion the
 * continuous-aggregate refresher got wrong, and the one that knew the fewest types: a {@link java.time.LocalDateTime},
 * which is the engine's own DATETIME representation and what {@code date()} answers by default, fell through to the
 * {@code Long.MIN_VALUE} "not a timestamp" sentinel.
 * <p>
 * On the binary-comparison path that sentinel was checked and the push-down declined, so the answer stayed right. On
 * the BETWEEN path it was not checked at all: it became the range's UPPER bound, the pushed-down scan range was
 * {@code [from, Long.MIN_VALUE]} - empty - and the query answered ZERO rows for a range that holds data. The
 * converter is now shared, and the sentinel is refused as a bound either way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8152TimeSeriesRangePushDownTest extends TestHelper {

  private static final long HOUR = 3_600_000L;

  @Override
  protected void beginTest() {
    database.command("sql",
        "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");
    database.transaction(() -> {
      for (int i = 0; i < 6; i++)
        database.command("sql", "INSERT INTO Reading SET ts = ?, sensor = 'A', value = ?", i * HOUR, (double) i);
    });
  }

  @Test
  void betweenWithDateTimeBoundsReturnsTheRowsInTheRange() {
    // 1970-01-01T01:00 .. 1970-01-01T03:00 inclusive: hours 1, 2 and 3.
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts BETWEEN date('1970-01-01 01:00:00') "
        + "AND date('1970-01-01 03:00:00')")).isEqualTo(3);
  }

  @Test
  void betweenWithEpochMillisBoundsIsUnchanged() {
    // The control: numeric bounds always worked, and must keep answering the same rows as the datetime ones.
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts BETWEEN " + HOUR + " AND " + (3 * HOUR)))
        .isEqualTo(3);
  }

  @Test
  void comparisonsWithDateTimeBoundsAgreeWithTheirNumericTwins() {
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts >= date('1970-01-01 02:00:00')"))
        .isEqualTo(count("SELECT count(*) AS c FROM Reading WHERE ts >= " + (2 * HOUR)))
        .isEqualTo(4);
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts < date('1970-01-01 02:00:00')"))
        .isEqualTo(count("SELECT count(*) AS c FROM Reading WHERE ts < " + (2 * HOUR)))
        .isEqualTo(2);
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE ts = date('1970-01-01 04:00:00')"))
        .isEqualTo(count("SELECT count(*) AS c FROM Reading WHERE ts = " + (4 * HOUR)))
        .isEqualTo(1);
  }

  @Test
  void aBetweenOnANonTimestampValueDoesNotEmptyTheResult() {
    // A bound the converter cannot read must decline the push-down, never become a bound of its own.
    assertThat(count("SELECT count(*) AS c FROM Reading WHERE value BETWEEN 1 AND 3")).isEqualTo(3);
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
