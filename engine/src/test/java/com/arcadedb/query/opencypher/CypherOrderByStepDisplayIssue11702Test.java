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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11702: the sort step printed itself in EXPLAIN and PROFILE as the bare class name "OrderByStep", with no
 * "+ " prefix, no keys, no top-k limit and no time or rows, so a plan did not show what was sorted or what the sort cost.
 */
class CypherOrderByStepDisplayIssue11702Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Item");
      database.command("opencypher", "UNWIND range(1, 50) AS i CREATE (:Item {n: i, g: i % 5})");
    });
  }

  @Test
  void explainShowsTheSortKeysAndDirections() {
    final List<String> lines = planLines("EXPLAIN MATCH (x:Item) RETURN x.n AS n ORDER BY x.g DESC, n");
    assertThat(lines).contains("+ ORDER BY x.g DESC, n ASC");
    assertThat(lines).noneMatch(line -> line.trim().equals("OrderByStep"));
  }

  @Test
  void aSortFeedingALimitShowsTheTopK() {
    assertThat(planLines("EXPLAIN MATCH (x:Item) RETURN x.n AS n ORDER BY n DESC LIMIT 3")).contains("+ ORDER BY n DESC (top 3)");
  }

  /** The rows a sort read, on the full sort and on the top-k path: the counter used to count the one call that read them. */
  @Test
  void profileShowsTheSortsTimeAndRows() {
    assertThat(planLines("PROFILE MATCH (x:Item) WITH x ORDER BY x.n RETURN x.n AS n"))
        .anyMatch(line -> line.matches("\\+ ORDER BY x\\.n ASC \\([\\d,.]+μs, 50 rows\\)"));
    assertThat(planLines("PROFILE MATCH (x:Item) RETURN x.n AS n ORDER BY n DESC LIMIT 3"))
        .anyMatch(line -> line.matches("\\+ ORDER BY n DESC \\(top 3\\) \\([\\d,.]+μs, 50 rows\\)"));
  }

  private List<String> planLines(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      return Arrays.stream(plan.split("\n")).map(String::trim).toList();
    }
  }
}
