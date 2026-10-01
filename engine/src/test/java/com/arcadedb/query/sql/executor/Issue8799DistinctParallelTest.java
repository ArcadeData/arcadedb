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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8799: SELECT DISTINCT over a type scan ran its projection and its dedup on the consuming thread (one core), while the
 * same answer as a GROUP BY aggregated in the parallel workers (13x faster). A plain DISTINCT is now planned as the GROUP BY
 * over its projected items; the rows and their first-occurrence order stay what the DISTINCT returned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8799DistinctParallelTest extends TestHelper {
  private static final int ROWS = 20_000;

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      database.getSchema().createVertexType(type, type.equals("OneBucket") ? 1 : 4);
      final Random rnd = new Random(7);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex(type).set("id", i, "grp", rnd.nextInt(100), "x", rnd.nextDouble(), "s", "s" + rnd.nextInt(7)).save();
      });
    }
  }

  @Test
  void distinctRunsInTheWorkersAndKeepsFirstOccurrenceOrder() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      final String query = "SELECT DISTINCT grp FROM " + type;
      assertThat(plan(query)).as(query).contains("CALCULATE AGGREGATE PROJECTIONS (parallel").doesNotContain("+ DISTINCT");
      assertThat(rows(query)).as(query).hasSize(100).isEqualTo(firstOccurrences("SELECT grp AS v FROM " + type));
    }
  }

  @Test
  void filteredAndMultiColumnDistinct() {
    for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
      String query = "SELECT DISTINCT grp FROM " + type + " WHERE x > 0.5";
      assertThat(rows(query)).as(query).isEqualTo(firstOccurrences("SELECT grp AS v FROM " + type + " WHERE x > 0.5"));

      query = "SELECT DISTINCT grp, s FROM " + type;
      assertThat(rows(query)).as(query).hasSize(700).isEqualTo(firstOccurrences("SELECT grp + ',' + s AS v FROM " + type));

      query = "SELECT DISTINCT grp % 10 AS mod10 FROM " + type;
      assertThat(rows(query)).as(query).hasSize(10).isEqualTo(firstOccurrences("SELECT grp % 10 AS v FROM " + type));

      query = "SELECT count(*) AS c FROM (SELECT DISTINCT grp FROM " + type + ")";
      assertThat(rows(query)).containsExactly("100");
    }
  }

  /** What the rewrite must leave alone: a LIMIT stops a DISTINCT early, an ORDER BY sorts before the dedup, an aggregate is not a plain DISTINCT. */
  @Test
  void distinctThatIsNotPlainStaysADistinct() {
    final String type = "FourBuckets";
    assertThat(plan("SELECT DISTINCT grp FROM " + type + " LIMIT 5")).contains("+ DISTINCT");
    assertThat(rows("SELECT DISTINCT grp FROM " + type + " LIMIT 5")).hasSize(5);

    assertThat(plan("SELECT DISTINCT grp FROM " + type + " ORDER BY grp")).contains("+ DISTINCT");
    final List<String> sorted = rows("SELECT DISTINCT grp FROM " + type + " ORDER BY grp");
    assertThat(sorted).hasSize(100).first().isEqualTo("0");

    assertThat(plan("SELECT DISTINCT * FROM " + type)).contains("+ DISTINCT");
    assertThat(rows("SELECT DISTINCT * FROM " + type)).hasSize(ROWS);

    assertThat(rows("SELECT DISTINCT grp FROM " + type + " SKIP 95")).hasSize(5);
  }

  /** Found on the way: a GROUP BY on a computed expression, with no aggregate to project, answered null for that column. */
  @Test
  void groupByComputedExpressionWithoutAggregate() {
    assertThat(rows("SELECT grp % 10 AS m FROM OneBucket GROUP BY grp % 10")).hasSize(10).doesNotContain("null");
    assertThat(rows("SELECT grp % 10 FROM OneBucket GROUP BY grp % 10")).hasSize(10).doesNotContain("null");
  }

  /** Inside a transaction the scan is sequential: the rewritten DISTINCT still answers, and sees the transaction's changes. */
  @Test
  void insideATransaction() {
    database.transaction(() -> {
      database.newVertex("OneBucket").set("id", -1, "grp", 1_000).save();
      assertThat(rows("SELECT DISTINCT grp FROM OneBucket")).hasSize(101).contains("1000");
    });
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  /** The single column of each row, as text. */
  private List<String> rows(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder sb = new StringBuilder();
        for (final String p : row.getPropertyNames())
          sb.append(sb.isEmpty() ? "" : ",").append(row.<Object>getProperty(p));
        rows.add(sb.toString());
      }
    }
    return rows;
  }

  /** The distinct values of the query's {@code v} column, in the order they are first met by a plain scan. */
  private List<String> firstOccurrences(final String query) {
    final Set<String> seen = new LinkedHashSet<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        seen.add(String.valueOf(rs.next().<Object>getProperty("v")));
    }
    return new ArrayList<>(seen);
  }
}
