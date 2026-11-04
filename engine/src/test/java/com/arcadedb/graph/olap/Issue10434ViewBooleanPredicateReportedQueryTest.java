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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10434, the report's own three-vertex chain: {@code WHERE b.flag} on the middle node of a two-hop chain over a Graph
 * Analytical View. The plan is asserted too, so the answers cannot come from a walk over the records: the chain is the fused one,
 * with the middle node bound (a predicate whose node went unseen deferred its load, the {@code b*} of the plan) and not given up on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10434ViewBooleanPredicateReportedQueryTest extends TestHelper {
  private static final String CHAIN = "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) ";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE N").close();
    database.command("sql", "CREATE PROPERTY N.flag BOOLEAN").close();
    database.command("sql", "CREATE EDGE TYPE R").close();
    database.transaction(() -> database.command("opencypher", "CREATE (a:N {flag: false})-[:R]->(b:N {flag: true})-[:R]->(c:N {flag: false})")
        .close());
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW g VERTEX TYPES (N) EDGE TYPES (R) PROPERTIES (flag) UPDATE MODE SYNCHRONOUS").close();
    assertThat(GraphAnalyticalViewRegistry.get(database, "g").awaitReady(30, TimeUnit.SECONDS)).isTrue();
  }

  private long count(final String where) {
    try (final ResultSet rs = database.query("opencypher", CHAIN + where + " RETURN count(*) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  @Test
  void theMiddleNodeIsBoundForABarePredicate() {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + CHAIN + "WHERE b.flag RETURN count(*) AS c")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).contains("GAVFusedChain").contains("(b)-[:R]->(c*)").doesNotContain("(b*)");
    }
  }

  @Test
  void theReportedQueryFindsTheMiddleNodeWithTheFlag() {
    assertThat(count("WHERE b.flag")).isEqualTo(1L);
    assertThat(count("WHERE b.flag = true")).isEqualTo(1L);
    assertThat(count("WHERE NOT b.flag")).isZero();
    assertThat(count("WHERE c.flag")).isZero();
    assertThat(count("WHERE NOT c.flag")).isEqualTo(1L);
    assertThat(count("WHERE coalesce(b.flag, false)")).isEqualTo(1L);
  }
}
