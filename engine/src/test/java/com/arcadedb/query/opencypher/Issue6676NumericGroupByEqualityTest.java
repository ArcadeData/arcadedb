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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6676: Cypher {@code GROUP BY} (the implicit grouping {@link
 * com.arcadedb.query.opencypher.executor.steps.GroupByAggregationStep} performs when a RETURN/WITH clause mixes
 * aggregations with non-aggregated expressions) keyed its groups on the raw boxed grouping value instead of
 * canonicalizing numeric types the way its sibling DISTINCT paths do (issue #5789, {@link
 * com.arcadedb.function.DistinctNumericKey}). A value present as different numeric runtime types (Integer 1 vs Long 1
 * vs Double 1.0) therefore split into separate groups, even though {@code 1 = 1.0} evaluates to {@code true}.
 */
class Issue6676NumericGroupByEqualityTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue6676-numeric-groupby").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void singleKeyGroupByCollapsesNumericallyEqualCrossTypeValues() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0, 1] AS x RETURN x, count(*) AS c")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("x")).doubleValue()).isEqualTo(1.0);
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(3L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void singleKeyGroupByStillSeparatesNumericallyDifferentValues() {
    try (final ResultSet rs = database.query("opencypher", "UNWIND [1, 1.0, 2] AS x RETURN x, count(*) AS c ORDER BY c DESC")) {
      assertThat(rs.hasNext()).isTrue();
      final Result first = rs.next();
      assertThat(((Number) first.getProperty("c")).longValue()).isEqualTo(2L);
      assertThat(rs.hasNext()).isTrue();
      final Result second = rs.next();
      assertThat(((Number) second.getProperty("c")).longValue()).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void multiKeyGroupByCollapsesNumericallyEqualCrossTypeValues() {
    try (final ResultSet rs = database.query("opencypher",
        "UNWIND [[1, 'a'], [1.0, 'a'], [2, 'b']] AS pair RETURN pair[0] AS n, pair[1] AS label, count(*) AS c")) {
      long total = 0;
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        total += ((Number) row.getProperty("c")).longValue();
        rows++;
      }
      assertThat(rows).isEqualTo(2); // (1/1.0, 'a') collapses into one group, (2, 'b') is the other
      assertThat(total).isEqualTo(3);
    }
  }
}
