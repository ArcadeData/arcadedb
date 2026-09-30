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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8725: a Cypher {@code MATCH} with a filter on an unindexed property scanned the label on the calling thread,
 * while the same SQL query has filtered in the workers of a parallel scan since #8523. The label scan now hands a
 * predicate of nodes that read only the row to the same parallel scan, which returns the rows in the order the sequential
 * scan reads them, so a LIMIT keeps the same rows.
 * <p>
 * Every parallel answer is checked against the same query run with parallel scans disabled, rows and order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8725CypherParallelFilteredScanTest extends TestHelper {
  private static final int ROWS = 20_000;

  @Override
  protected void beginTest() {
    // Small units, so the fixture's few hundred pages are cut in many of them
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    createType("OneBucket", 1);
    createType("FourBuckets", 4);
  }

  private void createType(final String type, final int buckets) {
    database.getSchema().createVertexType(type, buckets);
    final Random rnd = new Random(1);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex(type).set("id", i, "grp", rnd.nextInt(100), "name", "n" + rnd.nextInt(50)).save();
    });
  }

  @Test
  void filteredLabelScanRunsInParallelInTheSequentialOrder() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = "MATCH (e:" + type + ") WHERE e.grp = 5 RETURN e.id AS id";
      assertThat(profile(query)).as(type).contains("NodeByLabelScan").contains("[parallel]");
      final List<Object> parallel = column(query, Map.of());
      assertThat(parallel).isNotEmpty().isEqualTo(sequential(query, Map.of()));
    }
  }

  @Test
  void aggregatesAndLimitsKeepTheirAnswers() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      for (final String query : new String[] {
          "MATCH (e:" + type + ") WHERE e.grp = 5 RETURN count(*) AS id",
          "MATCH (e:" + type + ") WHERE e.grp < 10 AND e.name <> 'n3' RETURN e.id AS id LIMIT 37",
          "MATCH (e:" + type + ") WHERE e.grp >= 90 OR e.name = 'n7' RETURN e.id AS id SKIP 100 LIMIT 50",
          "MATCH (e:" + type + ") WHERE e.grp IN [1, 2, 3] RETURN sum(e.id) AS id",
          "MATCH (e:" + type + ") WHERE e.name STARTS WITH 'n4' AND e.grp % 7 = 0 RETURN e.id AS id",
          "MATCH (e:" + type + ") WHERE NOT e.grp = 5 AND e.missing IS NULL RETURN count(*) AS id" }) {
        assertThat(column(query, Map.of())).as(query).isEqualTo(sequential(query, Map.of()));
      }
    }
  }

  @Test
  void parametersReachTheWorkers() {
    final String query = "MATCH (e:FourBuckets) WHERE e.grp = $g AND e.name = $n RETURN e.id AS id";
    final Map<String, Object> params = Map.of("g", 12, "n", "n5");
    assertThat(column(query, params)).isNotEmpty().isEqualTo(sequential(query, params));
    assertThat(profile(query, params)).contains("[parallel]");
  }

  @Test
  void aPredicateWithAFunctionStaysSequential() {
    final String query = "MATCH (e:FourBuckets) WHERE toString(e.grp) = '5' RETURN e.id AS id";
    assertThat(profile(query)).doesNotContain("[parallel]");
    assertThat(column(query, Map.of())).isNotEmpty().isEqualTo(sequential(query, Map.of()));
  }

  @Test
  void anExpansionAboveTheScanAnswersTheSame() {
    database.getSchema().createEdgeType("Knows");
    database.transaction(() -> {
      final var target = database.newVertex("OneBucket").set("id", -1, "grp", -1).save();
      for (final var rs = database.query("sql", "SELECT FROM FourBuckets WHERE grp = 5 LIMIT 5"); rs.hasNext(); )
        rs.next().getVertex().get().newEdge("Knows", target).save();
    });
    final String query = "MATCH (e:FourBuckets)-[:Knows]->(t) WHERE e.grp = 5 RETURN e.id AS id, t.id AS t";
    assertThat(column(query, Map.of())).hasSize(5).isEqualTo(sequential(query, Map.of()));
  }

  @Test
  void inATransactionTheScanStaysOnTheCallingThread() {
    database.transaction(() -> {
      database.newVertex("FourBuckets").set("id", -7, "grp", 5, "name", "tx").save();
      final List<Object> ids = column("MATCH (e:FourBuckets) WHERE e.grp = 5 AND e.id = -7 RETURN e.id AS id", Map.of());
      assertThat(ids).containsExactly(-7);
    });
  }

  private List<Object> sequential(final String query, final Map<String, Object> params) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      return column(query, params);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private List<Object> column(final String query, final Map<String, Object> params) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final var row = rs.next();
        values.add(row.getPropertyNames().size() == 1 ? row.getProperty(row.getPropertyNames().iterator().next()) : row.toMap().toString());
      }
    }
    return values;
  }

  private String profile(final String query) {
    return profile(query, Map.of());
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
