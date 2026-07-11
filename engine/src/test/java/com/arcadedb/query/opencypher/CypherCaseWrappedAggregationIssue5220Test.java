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
 * Regression test for issue #5220: an aggregation function (sum/avg/min/max/collect) wrapped inside a
 * CASE expression must accumulate over all input rows, not return the value produced by the first row.
 * ArcadeDB returned the aggregation evaluated against a single representative row (the first one)
 * instead of the pre-computed aggregated value, because the override-aware evaluator did not route
 * through CaseExpression's branches. Neo4j returns the fully accumulated value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CypherCaseWrappedAggregationIssue5220Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-case5220").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (r:Root {id:1}), (r)-[:R]->(:V {v:2}), (r)-[:R]->(:V {v:10})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void unwindSumWrappedInCase() {
    final ResultSet rs = database.query("opencypher",
        "UNWIND [2, 10] AS v RETURN sum(v) AS bare, CASE WHEN true THEN sum(v) ELSE 0 END AS wrapped");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("bare")).intValue()).isEqualTo(12);
    assertThat(((Number) row.getProperty("wrapped")).intValue()).isEqualTo(12);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void unwindSumWrappedInCaseReversedOrder() {
    final ResultSet rs = database.query("opencypher",
        "UNWIND [10, 2] AS v RETURN sum(v) AS bare, CASE WHEN true THEN sum(v) ELSE 0 END AS wrapped");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("bare")).intValue()).isEqualTo(12);
    assertThat(((Number) row.getProperty("wrapped")).intValue()).isEqualTo(12);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void graphSumWrappedInCase() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (:Root {id:1})-[:R]->(b) RETURN sum(b.v) AS bare, CASE WHEN true THEN sum(b.v) ELSE 0 END AS wrapped");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("bare")).intValue()).isEqualTo(12);
    assertThat(((Number) row.getProperty("wrapped")).intValue()).isEqualTo(12);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void caseFalseBranchSelectsElse() {
    // When the WHEN condition is false, the ELSE branch is used; the wrapped aggregation must not run.
    final ResultSet rs = database.query("opencypher",
        "UNWIND [2, 10] AS v RETURN CASE WHEN false THEN sum(v) ELSE -1 END AS wrapped");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("wrapped")).intValue()).isEqualTo(-1);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void otherAggregatesWrappedInCase() {
    final ResultSet rs = database.query("opencypher",
        "UNWIND [2, 10] AS v RETURN "
            + "CASE WHEN true THEN min(v) ELSE 0 END AS mn, "
            + "CASE WHEN true THEN max(v) ELSE 0 END AS mx, "
            + "CASE WHEN true THEN avg(v) ELSE 0 END AS av, "
            + "CASE WHEN true THEN count(v) ELSE 0 END AS cnt");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("mn")).intValue()).isEqualTo(2);
    assertThat(((Number) row.getProperty("mx")).intValue()).isEqualTo(10);
    assertThat(((Number) row.getProperty("av")).doubleValue()).isEqualTo(6.0);
    assertThat(((Number) row.getProperty("cnt")).intValue()).isEqualTo(2);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void caseWrappedAggregationWithGroupBy() {
    // Two groups by parity of v; each group's CASE-wrapped sum must accumulate within the group.
    final ResultSet rs = database.query("opencypher",
        "UNWIND [1, 2, 3, 4] AS v RETURN v % 2 AS parity, "
            + "CASE WHEN true THEN sum(v) ELSE 0 END AS wrapped ORDER BY parity");
    assertThat(rs.hasNext()).isTrue();
    final Result even = rs.next();
    assertThat(((Number) even.getProperty("parity")).intValue()).isEqualTo(0);
    assertThat(((Number) even.getProperty("wrapped")).intValue()).isEqualTo(6); // 2 + 4
    assertThat(rs.hasNext()).isTrue();
    final Result odd = rs.next();
    assertThat(((Number) odd.getProperty("parity")).intValue()).isEqualTo(1);
    assertThat(((Number) odd.getProperty("wrapped")).intValue()).isEqualTo(4); // 1 + 3
    assertThat(rs.hasNext()).isFalse();
  }
}
