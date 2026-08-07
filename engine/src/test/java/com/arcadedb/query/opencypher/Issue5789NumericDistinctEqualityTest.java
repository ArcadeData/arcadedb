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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5789: ArcadeDB evaluates {@code 1 = 1.0} as {@code true}, but its duplicate-elimination
 * paths (UNION, RETURN DISTINCT, count(DISTINCT ...), collect(DISTINCT ...)) treated the same
 * finite, non-null numeric values as distinct when they had different boxed numeric types
 * (INTEGER vs FLOAT). This left UNION and DISTINCT out of sync with expression equality.
 */
class Issue5789NumericDistinctEqualityTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue5789-numeric-distinct").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void unionCollapsesNumericallyEqualCrossTypeValues() {
    try (final ResultSet rs = database.query("opencypher", "RETURN 1 AS x UNION RETURN 1.0 AS x")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void unionStillDeduplicatesSameTypeValues() {
    try (final ResultSet rs = database.query("opencypher", "RETURN 1 AS x UNION RETURN 1 AS x")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void unionAllStillPreservesCrossTypeDuplicates() {
    try (final ResultSet rs = database.query("opencypher", "RETURN 1 AS x UNION ALL RETURN 1.0 AS x")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void returnDistinctCollapsesNumericallyEqualCrossTypeValues() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0] AS x RETURN DISTINCT x")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void returnDistinctStillKeepsNumericallyDifferentValues() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 2.0] AS x RETURN DISTINCT x")) {
      assertThat(rs.stream().count()).isEqualTo(2);
    }
  }

  @Test
  void countDistinctCollapsesNumericallyEqualCrossTypeValues() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0] AS x RETURN count(DISTINCT x) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
    }
  }

  @Test
  void collectDistinctCollapsesNumericallyEqualCrossTypeValues() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0] AS x RETURN collect(DISTINCT x) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final List<Object> collected = (List<Object>) row.getProperty("c");
      assertThat(collected).hasSize(1);
    }
  }

  @Test
  void minDistinctAndMaxDistinctCollapseNumericallyEqualCrossTypeValues() {
    // A regression guard for the shared DistinctAggregationWrapper used by min/max/avg DISTINCT.
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [1, 1.0, 2] AS x RETURN min(DISTINCT x) AS mn, max(DISTINCT x) AS mx, avg(DISTINCT x) AS av")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("mn")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) row.getProperty("mx")).doubleValue()).isEqualTo(2.0);
      // Only two distinct values (1 and 2) should be averaged: (1 + 2) / 2 = 1.5
      assertThat(((Number) row.getProperty("av")).doubleValue()).isEqualTo(1.5);
    }
  }

  @Test
  void equalityOperatorAndDistinctPathsAgree() {
    try (final ResultSet rs = database.query("opencypher", "RETURN 1 = 1.0 AS eq")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("eq")).isTrue();
    }
  }
}
