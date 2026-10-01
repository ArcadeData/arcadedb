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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8797: an openCypher aggregate over a label scan ran on one core, while the same aggregate in SQL had run in the
 * workers of a parallel scan since #8523. An unfiltered label scan never took the parallel path, and the aggregation stayed
 * on the consuming thread even when it did. The aggregates that merge partial states ({@code count}, {@code sum},
 * {@code avg}, {@code min}, {@code max}) now aggregate in the workers.
 * <p>
 * Every answer is checked against the same query run with parallel scans disabled - rows AND the order of the groups - and
 * the values are integers (stored as doubles too), so the sums are exact whatever order the workers add them in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8797CypherParallelAggregationTest extends TestHelper {
  private static final int ROWS = 20_000;

  @Override
  protected void beginTest() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES ARE CUT IN MANY OF THEM
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      database.getSchema().createVertexType(type, type.equals("OneBucket") ? 1 : 4);
      final Random rnd = new Random(3);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++) {
          final var vertex = database.newVertex(type).set("id", i, "grp", rnd.nextInt(100), "flag", rnd.nextInt(3), "x", (double) rnd.nextInt(1000),
              "n", (long) rnd.nextInt(50));
          // A FEW VERTICES WITHOUT x: NULLS ARE SKIPPED BY THE AGGREGATES
          if (i % 97 == 0)
            vertex.remove("x");
          vertex.save();
        }
      });
    }
  }

  @Test
  void wholeTypeAggregatesRunInTheWorkers() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      for (final String query : new String[] {
          "MATCH (n:%s) RETURN sum(n.x) AS s".formatted(type),
          "MATCH (n:%s) RETURN max(n.x) AS m".formatted(type),
          "MATCH (n:%s) RETURN min(n.x) AS m".formatted(type),
          "MATCH (n:%s) RETURN avg(n.x) AS a".formatted(type),
          "MATCH (n:%s) RETURN count(n.x) AS c".formatted(type),
          "MATCH (n:%s) RETURN count(*) AS c, sum(n.n) AS s, min(n.n) AS lo, max(n.n) AS hi".formatted(type),
          "MATCH (n:%s) WHERE n.x > 500 RETURN count(*) AS c, sum(n.x) AS s".formatted(type),
          "MATCH (n:%s) WHERE n.grp = 7 AND n.flag <> 1 RETURN count(n) AS c, avg(n.n) AS a".formatted(type) }) {
        final String plan = assertSameAsSequential(query);
        assertThat(plan).as(query).contains("(parallel:");
      }
    }
  }

  @Test
  void groupedAggregatesRunInTheWorkersAndKeepTheFirstOccurrenceOrder() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      for (final String query : new String[] {
          "MATCH (n:%s) RETURN n.grp AS grp, count(*) AS c, sum(n.x) AS s".formatted(type),
          "MATCH (n:%s) RETURN n.grp AS grp, n.flag AS flag, count(*) AS c, max(n.x) AS m, min(n.id) AS first".formatted(type),
          "MATCH (n:%s) WHERE n.x > 500 RETURN n.grp AS grp, count(*) AS c".formatted(type),
          "MATCH (n:%s) RETURN n.flag AS flag, avg(n.n) AS a, count(n.x) AS c".formatted(type),
          "MATCH (n:%s) RETURN n.grp AS grp, count(*) AS c ORDER BY c DESC, grp LIMIT 5".formatted(type) }) {
        final String plan = assertSameAsSequential(query);
        assertThat(plan).as(query).contains("(parallel:");
      }
    }
  }

  /** What stays on the consuming thread - a DISTINCT aggregate, collect(), a function in an argument - still answers the same. */
  @Test
  void whatCannotMergeStaysSequentialAndCorrect() {
    for (final String query : new String[] {
        "MATCH (n:OneBucket) RETURN count(DISTINCT n.grp) AS c",
        "MATCH (n:OneBucket) RETURN n.flag AS flag, collect(n.n)[0] AS first",
        "MATCH (n:OneBucket) RETURN sum(toInteger(n.x)) AS s",
        "MATCH (n:OneBucket) RETURN count(*) * 10 + 1 AS c",
        "MATCH (n:OneBucket) WHERE n.grp IN [1, 2, 3] RETURN count(*) AS c" }) {
      final String plan = assertSameAsSequential(query);
      if (!query.contains("IN ["))
        assertThat(plan).as(query).doesNotContain("(parallel:");
    }
  }

  /** A filter that matches nothing answers what the sequential aggregation answers: a row with a zero count, or no group at all. */
  @Test
  void emptyInputAnswersLikeTheSequentialAggregation() {
    assertSameAsSequential("MATCH (n:FourBuckets) WHERE n.x > 5000 RETURN count(*) AS c, sum(n.x) AS s, max(n.x) AS m, avg(n.x) AS a");
    assertSameAsSequential("MATCH (n:FourBuckets) WHERE n.x > 5000 RETURN n.grp AS grp, count(*) AS c");
    assertThat(rows("MATCH (n:FourBuckets) WHERE n.x > 5000 RETURN n.grp AS grp, count(*) AS c")).isEmpty();
  }

  /** Workers do not see a transaction's changes, so inside one the aggregation stays on the caller - and sees them. */
  @Test
  void insideATransactionTheAggregationStaysSequentialAndSeesItsChanges() {
    database.transaction(() -> {
      database.newVertex("OneBucket").set("id", -1, "grp", 1_000, "x", 1_000_000.0).save();
      final List<String> rows = new ArrayList<>();
      final String plan = rowsAndPlan("MATCH (n:OneBucket) RETURN max(n.x) AS m", rows);
      assertThat(plan).doesNotContain("(parallel:");
      assertThat(rows).containsExactly("m=1000000.0;");
    });
  }

  /** A parameter in the filter and in the key is read by the workers like by the caller. */
  @Test
  void parametersReachTheWorkers() {
    final Map<String, Object> params = Map.of("min", 900);
    final List<String> parallel = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:FourBuckets) WHERE n.x >= $min RETURN n.flag AS flag, count(*) AS c", params)) {
      while (rs.hasNext())
        parallel.add(render(rs.next()));
    }
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> sequential = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:FourBuckets) WHERE n.x >= $min RETURN n.flag AS flag, count(*) AS c", params)) {
      while (rs.hasNext())
        sequential.add(render(rs.next()));
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
    assertThat(parallel).hasSize(3).isEqualTo(sequential);
  }

  /** Runs the query in parallel and sequentially, asserts the same rows in the same order, and returns the parallel run's plan. */
  private String assertSameAsSequential(final String query) {
    final List<String> parallel = new ArrayList<>();
    final String plan = rowsAndPlan(query, parallel);

    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    final List<String> sequential = new ArrayList<>();
    try {
      assertThat(rowsAndPlan(query, sequential)).doesNotContain("(parallel");
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
    assertThat(parallel).as(query).isEqualTo(sequential);
    return plan;
  }

  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    rowsAndPlan(query, rows);
    return rows;
  }

  private String rowsAndPlan(final String query, final List<String> rows) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(render(rs.next()));
    }
    // PROFILE RUNS THE QUERY AGAIN AND REPORTS THE PLAN THAT RAN
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static String render(final Result row) {
    final StringBuilder sb = new StringBuilder();
    for (final String p : row.getPropertyNames())
      sb.append(p).append('=').append(row.<Object>getProperty(p)).append(';');
    return sb.toString();
  }
}
