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
  void testWithGroupBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void testWithOrderByNoGroupBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, temp FROM SensorReading ORDER BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, temp FROM SensorReading WHERE `ts` >= 1000 ORDER BY sensor_id");
  }

  @Test
  void testWithOrderByAndGroupBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading GROUP BY sensor_id ORDER BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    // WHERE should be inserted before GROUP BY
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 GROUP BY sensor_id ORDER BY sensor_id");
  }

  @Test
  void testWithExistingWhere() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE active = true GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 AND active = true GROUP BY sensor_id");
  }

  @Test
  void testNoKeywordsAppendsAtEnd() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000");
  }

  @Test
  void testWithLimitNoGroupByNoOrderBy() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, temp FROM SensorReading LIMIT 100");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, temp FROM SensorReading WHERE `ts` >= 1000 LIMIT 100");
  }

  @Test
  void testWhereConditionStartsWithParenthesis() {
    // Regression: WHERE(condition) without a space after WHERE caused "AND(condition)" — missing space
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE(active = true) GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading WHERE `ts` >= 1000 AND (active = true) GROUP BY sensor_id");
  }

  @Test
  void testWatermarkZeroReturnsOriginal() {
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id FROM SensorReading ORDER BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 0);
    assertThat(result).isEqualTo("SELECT sensor_id FROM SensorReading ORDER BY sensor_id");
  }

  @Test
  void testBlockCommentContainingWhereIsIgnored() {
    // Regression: block comment containing WHERE must not be matched as the top-level WHERE
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) /* WHERE not here */ FROM SensorReading GROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) /* WHERE not here */ FROM SensorReading WHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void testLineCommentContainingWhereIsIgnored() {
    // Regression: line comment containing WHERE must not be matched as the top-level WHERE
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT sensor_id, avg(temp) FROM SensorReading -- no WHERE needed\nGROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    assertThat(result).isEqualTo(
        "SELECT sensor_id, avg(temp) FROM SensorReading -- no WHERE needed\nWHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void testLineCommentWithWhereKeywordIsNotMatched() {
    // A -- comment containing WHERE should not be treated as a top-level WHERE clause
    final ContinuousAggregateImpl ca = buildCA(
        "SELECT avg(temp) FROM SensorReading -- WHERE clause not needed\nGROUP BY sensor_id");
    final String result = ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000);
    // Should insert before GROUP BY, not after comment's WHERE
    assertThat(result).isEqualTo(
        "SELECT avg(temp) FROM SensorReading -- WHERE clause not needed\nWHERE `ts` >= 1000 GROUP BY sensor_id");
  }

  @Test
  void testDotInTimestampColumnIsRejected() {
    // Regression: SAFE_COLUMN_NAME must not allow dots in column names (could allow injection)
    final ContinuousAggregateImpl ca = new ContinuousAggregateImpl(null, "test_ca",
        "SELECT avg(temp) FROM SensorReading GROUP BY sensor_id",
        "test_backing",
        "SensorReading", 3_600_000L, "hour",
        "outer.inner"); // dot in timestamp column name

    assertThatThrownBy(() -> ContinuousAggregateRefresher.buildFilteredQuery(ca, 1000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsafe timestamp column name");
  }

  private static ContinuousAggregateImpl buildCA(final String query) {
    return new ContinuousAggregateImpl(null, "test_ca", query,
        "test_backing", "SensorReading", 3_600_000L, "hour", "ts");
  }
}
