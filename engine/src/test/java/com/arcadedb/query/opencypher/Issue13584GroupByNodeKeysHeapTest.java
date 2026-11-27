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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.QueryHeapBudgetTestSupport;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13584: a GROUP BY with several grouping keys kept the first input row of every group, and charged it to the query heap
 * budget next to the keys, although only a complex aggregation item ({@code h{.v, c: count(*)}}) reads it. With nodes as the keys
 * every group was charged its keys twice, and 1.5M groups exhausted the 2 GB budget that Neo4j needs a second for.
 */
class Issue13584GroupByNodeKeysHeapTest extends TestHelper {
  private static final int PAIRS = 30_000;

  @BeforeEach
  void noReservationLeft() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
  }

  @AfterEach
  void resetBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void aGroupWithOnlySimpleAggregatesIsChargedItsKeysNotItsFirstRow() {
    database.getSchema().createVertexType("A");
    database.getSchema().createVertexType("B");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      for (int i = 0; i < PAIRS; i++)
        database.newVertex("A").set("id", i, "name", "a" + i).save().newEdge("R", database.newVertex("B").set("id", i, "name", "b" + i).save());
    });
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(BUDGET_MB);

    try (final ResultSet rows = database.query("opencypher", "MATCH (a:A)-[:R]->(b:B) WITH a, b, count(*) AS c RETURN count(*) AS groups")) {
      assertThat(rows.next().<Number>getProperty("groups").intValue()).isEqualTo(PAIRS);
    }
  }

  @Test
  void aComplexAggregationItemStillReadsTheFirstRowOfItsGroup() {
    database.getSchema().createVertexType("V");
    database.transaction(() -> {
      for (int i = 0; i < 6; i++)
        database.newVertex("V").set("g", i % 2, "n", i).save();
    });
    try (final ResultSet rows = database.query("opencypher",
        "MATCH (v:V) WITH v.g AS g, v.n AS n, v RETURN g, n, v{.n, c: count(*)} AS m ORDER BY n")) {
      int seen = 0;
      while (rows.hasNext()) {
        final var row = rows.next();
        assertThat(row.<java.util.Map<String, Object>>getProperty("m")).containsEntry("n", row.<Number>getProperty("n").intValue());
        ++seen;
      }
      assertThat(seen).isEqualTo(6);
    }
  }

  private static final long BUDGET_MB = 40;
}
