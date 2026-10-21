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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10528: {@code ts > <Long.MAX_VALUE>} was turned into the range {@code [value + 1, Long.MAX_VALUE]} with no overflow check, so
 * the bound wrapped to {@code Long.MIN_VALUE}, which the range union reads as "no lower bound": a predicate no sample satisfies
 * answered every sample, and since #10342 {@code DELETE} destroyed every sample of the type, outside any transaction.
 * <p>
 * The rule: the range a time predicate is pushed down as is EXACTLY that predicate, at the extremes of {@code long} as anywhere else.
 * Every comparison operator is checked against the extremes, written either way round and as a bound parameter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10528TimeSeriesBoundOverflowTest extends TestHelper {
  private static final long MAX = Long.MAX_VALUE;
  private static final long MIN = Long.MIN_VALUE;

  @BeforeEach
  void createSamples() {
    database.command("sql", "CREATE TIMESERIES TYPE D1 TIMESTAMP ts TAGS (t STRING) FIELDS (i INTEGER)");
    database.command("sql", "INSERT INTO D1 SET ts = 1000, t = 'h', i = 1");
    database.command("sql", "INSERT INTO D1 SET ts = 2000, t = 'h', i = 2");
    database.command("sql", "INSERT INTO D1 SET ts = 3000, t = 'h', i = 3");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "ts > 9223372036854775807",
      "9223372036854775807 < ts",
      "ts > 9223372036854775807 AND t = 'h'",
      "ts > 9223372036854775807 OR ts > 9223372036854775807",
      // a union of blocks: the empty one adds nothing
      "ts > 9223372036854775807 OR ts < 0",
      // both ends of the union form: the lower one is declined by the push-down, so the WHERE stays and judges the rows
      "ts > 9223372036854775807 OR ts < -9223372036854775808",
      "-9223372036854775808 > ts OR 9223372036854775807 < ts",
      "ts >= 9223372036854775807",
      "ts = 9223372036854775807",
      "ts < -9223372036854775808",
      "-9223372036854775808 > ts"
  })
  void aPredicateNoSampleSatisfiesSelectsNothing(final String where) {
    assertThat(count("SELECT FROM D1 WHERE " + where)).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "ts > 9223372036854775807",
      "9223372036854775807 < ts",
      "ts >= 9223372036854775807",
      "ts = 9223372036854775807"
  })
  void aPredicateNoSampleSatisfiesDeletesNothing(final String where) {
    database.command("sql", "DELETE FROM D1 WHERE " + where);

    assertThat(count("SELECT FROM D1")).isEqualTo(3L);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "ts < -9223372036854775808",
      "-9223372036854775808 > ts"
  })
  void aLowerBoundBelowEveryTimestampIsNeverAScopeAndNeverDeletesAnything(final String where) {
    // Long.MIN_VALUE is the sentinel for "not a timestamp", so the push-down declines it and the DELETE is refused as unsupported
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM D1 WHERE " + where)).isInstanceOf(CommandExecutionException.class);

    assertThat(count("SELECT FROM D1")).isEqualTo(3L);
  }

  @Test
  void aBoundParameterAtTheExtremeIsTheSamePredicate() {
    assertThat(count("SELECT FROM D1 WHERE ts > :max", Map.of("max", MAX))).isZero();
    assertThat(count("SELECT FROM D1 WHERE :max < ts", Map.of("max", MAX))).isZero();

    database.command("sql", "DELETE FROM D1 WHERE ts > :max", Map.of("max", MAX));

    assertThat(count("SELECT FROM D1")).isEqualTo(3L);
  }

  @Test
  void theNeighboursOfTheExtremeKeepTheirMeaning() {
    // the predicates the issue lists as behaving, and the ones the fix must not turn into an empty range
    assertThat(count("SELECT FROM D1 WHERE ts > 9223372036854775806")).isZero();
    assertThat(count("SELECT FROM D1 WHERE ts < 9223372036854775807")).isEqualTo(3L);
    assertThat(count("SELECT FROM D1 WHERE ts > -9223372036854775807")).isEqualTo(3L);
    assertThat(count("SELECT FROM D1 WHERE ts <= 9223372036854775807")).isEqualTo(3L);
    assertThat(count("SELECT FROM D1 WHERE ts >= 2000")).isEqualTo(2L);
    assertThat(count("SELECT FROM D1 WHERE ts > 2000")).isEqualTo(1L);
    assertThat(count("SELECT FROM D1 WHERE 2000 < ts")).isEqualTo(1L);
    assertThat(count("SELECT FROM D1 WHERE 2000 >= ts")).isEqualTo(2L);
    // the empty block of a union adds no sample and takes none away
    assertThat(count("SELECT FROM D1 WHERE ts > 9223372036854775807 OR ts >= 2000")).isEqualTo(2L);
    assertThat(count("SELECT FROM D1 WHERE ts >= 2000 OR 9223372036854775807 < ts")).isEqualTo(2L);
  }

  @Test
  void deleteAboveTheLastSampleStillRemovesTheSamplesAbove() {
    database.command("sql", "DELETE FROM D1 WHERE ts > 1000");

    assertThat(count("SELECT FROM D1")).isEqualTo(1L);
  }

  private long count(final String sql) {
    return count(sql, Map.of());
  }

  private long count(final String sql, final Map<String, Object> parameters) {
    long rows = 0;
    try (final ResultSet resultSet = database.query("sql", sql, parameters)) {
      while (resultSet.hasNext()) {
        final Result ignored = resultSet.next();
        rows++;
      }
    }
    return rows;
  }
}
