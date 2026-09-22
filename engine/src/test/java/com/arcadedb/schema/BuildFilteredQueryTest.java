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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link ContinuousAggregateRefresher#buildFilteredQuery}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BuildFilteredQueryTest {

  @Test
  void withGroupBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void withOrderByNoGroupBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, temp FROM SensorReading ORDER BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, temp FROM SensorReading WHERE `ts` >= 1000 ORDER BY sensor_id");
  }

  @Test
  void withOrderByAndGroupBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading GROUP BY sensor_id ORDER BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    // WHERE should be inserted before GROUP BY
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 GROUP BY sensor_id ORDER BY sensor_id");
  }

  @Test
  void withExistingWhere() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE active = true GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    // #8156: the caller's predicate is bracketed. It is redundant for a single comparison and mandatory the moment
    // the predicate contains an OR, so it is applied unconditionally rather than guessed at.
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 AND (active = true) GROUP BY sensor_id");
  }

  /**
   * #8156: the regression. {@code AND} binds tighter than {@code OR}, so without the bracket the second disjunct
   * escapes the watermark filter entirely and buckets older than the watermark are re-aggregated - on top of rows
   * the refresh's own DELETE did not touch, because that DELETE only covers buckets at or after the watermark.
   */
  @Test
  void orInExistingWhereStaysGrouped() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temp) AS avg_temp FROM SensorReading "
            + "WHERE temp > 100 OR sensor_id = 'A' GROUP BY sensor_id, hour");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 7200000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temp) AS avg_temp FROM SensorReading "
            + "WHERE `ts` >= 7200000 AND (temp > 100 OR sensor_id = 'A') GROUP BY sensor_id, hour");
  }

  @Test
  void orInExistingWhereWithNoTrailingClause() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id FROM SensorReading WHERE temp > 100 OR sensor_id = 'A'");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id FROM SensorReading WHERE `ts` >= 1000 AND (temp > 100 OR sensor_id = 'A')");
  }

  @Test
  void whereClauseEndsAtOrderByNotAtTheEndOfTheString() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, temp FROM SensorReading WHERE temp > 100 OR sensor_id = 'A' ORDER BY sensor_id LIMIT 10");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, temp FROM SensorReading WHERE `ts` >= 1000 AND (temp > 100 OR sensor_id = 'A') "
            + "ORDER BY sensor_id LIMIT 10");
  }

  /**
   * #8156: the clause scan that finds where the WHERE ends is quote-aware, so a clause keyword sitting inside a
   * string literal does not close the bracket in the middle of the predicate. The old {@code indexOf} scan used for
   * the no-WHERE branch was not.
   */
  @Test
  void clauseKeywordInsideAStringLiteralIsNotAClause() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id FROM SensorReading WHERE label = 'GROUP BY me' OR temp > 1 GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id FROM SensorReading WHERE `ts` >= 1000 AND (label = 'GROUP BY me' OR temp > 1) "
            + "GROUP BY sensor_id");
  }

  /**
   * #8156 (found in review): a clause keyword the scan cannot see is not merely missed - the bracket then closes at
   * the END OF THE QUERY and swallows the real clause into the predicate, which fails the refresh on invalid SQL.
   * A single space in a keyword therefore matches any run of whitespace.
   */
  @Test
  void aClauseKeywordWithIrregularWhitespaceIsStillAClause() {
    final ContinuousAggregateImpl twoSpaces = buildCA(
        "SELECT sensor_id, temp FROM SensorReading WHERE temp > 100 OR sensor_id = 'A' ORDER  BY sensor_id");
    assertThat(ContinuousAggregateRefresher.buildFilteredQuery(twoSpaces, 1000, true)).isEqualTo(
        "SELECT sensor_id, temp FROM SensorReading WHERE `ts` >= 1000 AND (temp > 100 OR sensor_id = 'A') "
            + "ORDER  BY sensor_id");

    final ContinuousAggregateImpl lineBreak = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE a = 1 OR b = 2 GROUP\nBY sensor_id");
    assertThat(ContinuousAggregateRefresher.buildFilteredQuery(lineBreak, 1000, true)).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 AND (a = 1 OR b = 2) GROUP\nBY sensor_id");

    // And the no-WHERE branch places the new clause before it just the same.
    final ContinuousAggregateImpl noWhere = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading GROUP   BY sensor_id");
    assertThat(ContinuousAggregateRefresher.buildFilteredQuery(noWhere, 1000, true)).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 GROUP   BY sensor_id");
  }

  /**
   * A keyword must still not match a longer word that merely starts with it, whitespace tolerance included.
   */
  @Test
  void aLongerWordStartingWithAClauseKeywordIsNotAClause() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id FROM SensorReading WHERE skipped = true OR limited = 1 GROUP BY sensor_id");
    assertThat(ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true)).isEqualTo(
        "SELECT sensor_id FROM SensorReading WHERE `ts` >= 1000 AND (skipped = true OR limited = 1) "
            + "GROUP BY sensor_id");
  }

  /**
   * #8152: a watermark of 0 that HAS been set is a real watermark - the epoch bucket - and must still be filtered
   * on. There is no 2-argument overload that could infer the flag from `watermark > 0`: inferring it is the defect.
   */
  @Test
  void watermarkOfZeroIsFilteredWhenExplicitlySet() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading GROUP BY sensor_id");
    assertThat(ContinuousAggregateRefresher.buildFilteredQuery(ca, 0, true)).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 0 GROUP BY sensor_id");
    assertThat(ContinuousAggregateRefresher.buildFilteredQuery(ca, 0, false)).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading GROUP BY sensor_id");
  }

  @Test
  void noKeywordsAppendsAtEnd() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000");
  }

  @Test
  void withLimitNoGroupByNoOrderBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, temp FROM SensorReading LIMIT 100");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, temp FROM SensorReading WHERE `ts` >= 1000 LIMIT 100");
  }

  @Test
  void whereConditionStartsWithParenthesis() {
    // Regression: WHERE(condition) without a space after WHERE caused "AND(condition)" — missing space.
    // #8156 adds the predicate bracket, so an already-bracketed predicate simply gains a redundant outer pair;
    // detecting that the existing pair spans the whole predicate is more code than it saves.
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE(active = true) GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 AND ((active = true)) GROUP BY sensor_id");
  }

  @Test
  void watermarkZeroReturnsOriginal() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id FROM SensorReading ORDER BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 0, false);
    assertThat(result).isEqualTo("SELECT sensor_id FROM SensorReading ORDER BY sensor_id");
  }

  @Test
  void blockCommentContainingWhereIsIgnored() {
    // Regression: block comment containing WHERE must not be matched as the top-level WHERE
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) /* WHERE not here */ FROM SensorReading GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) /* WHERE not here */ FROM SensorReading WHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void lineCommentContainingWhereIsIgnored() {
    // Regression: line comment containing WHERE must not be matched as the top-level WHERE
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading -- no WHERE needed\nGROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading -- no WHERE needed\nWHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void lineCommentWithWhereKeywordIsNotMatched() {
    // A -- comment containing WHERE should not be treated as a top-level WHERE clause
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT avg(temp) FROM SensorReading -- WHERE clause not needed\nGROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true);
    // Should insert before GROUP BY, not after comment's WHERE
    assertThat(result).isEqualTo(
        "SELECT avg(temp) FROM SensorReading -- WHERE clause not needed\nWHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void dotInTimestampColumnIsRejected() {
    // Regression: SAFE_COLUMN_NAME must not allow dots in column names (could allow injection)
    final ContinuousAggregateImpl ca = new ContinuousAggregateImpl(null, "test_ca",
        "SELECT avg(temp) FROM SensorReading GROUP BY sensor_id",
        "test_backing",
        "SensorReading", 3_600_000L, "hour",
        "outer.inner"); // dot in timestamp column name

    assertThatThrownBy(() -> ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000, true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsafe timestamp column name");
  }

  private static ContinuousAggregateImpl buildCA(final String query) {
    return new ContinuousAggregateImpl(null, "test_ca", query,
        "test_backing", "SensorReading", 3_600_000L, "hour", "ts");
  }
}
