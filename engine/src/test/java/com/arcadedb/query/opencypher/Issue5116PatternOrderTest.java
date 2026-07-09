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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5116: reordering standalone (single-node) comma-separated pattern parts
 * inside a single MATCH must not change the amount of work the engine performs, even when a node
 * variable repeats across parts with different label constraints.
 * <p>
 * The reported query repeats {@code n0} once with a multi-label conjunction and once bare:
 * <pre>
 *   fast: MATCH (n0 :L1 :L5), (n0), (n1) ...
 *   slow: MATCH (n1), (n0), (n0 :L1 :L5) ...
 * </pre>
 * The multi-label conjunction {@code (n0:L1:L5)} disqualifies the cost-based optimizer, so the query
 * runs on the legacy step-chain path. That path chains comma-separated pattern parts in textual order
 * as a left-deep nested-loop Cartesian product. In the slow form the bare {@code (n0)} is chained
 * before the labeled {@code (n0:L1:L5)}, so {@code n0} is first bound to <em>every</em> node (full
 * scan) and only afterwards filtered by the labels; in the fast form the labeled occurrence binds
 * {@code n0} to the small {@code L1&amp;L5} class first. This produced the reported ~16x slowdown.
 * <p>
 * The fix orders the parts of each independent component so the most selective (labeled) occurrence of
 * a shared variable binds first, making both formulations do the same work. The test is deterministic
 * (it compares PROFILE row-count totals, not wall-clock time) and asserts both orderings return
 * identical results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5116PatternOrderTest {
  private Database database;

  private static final int L1L5_COUNT = 5;   // nodes carrying both L1 and L5
  private static final int PLAIN_COUNT = 60; // other nodes (no L1/L5)

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-issue5116").create();

    database.transaction(() -> {
      for (int i = 0; i < L1L5_COUNT; i++)
        database.command("opencypher", "CREATE (n:L1:L5 {v: " + i + "})");
      for (int i = 0; i < PLAIN_COUNT; i++)
        database.command("opencypher", "CREATE (n:Other {v: " + i + "})");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void repeatedVariableOrderDoesNotChangeWork() {
    final String fast = "MATCH (n0 :L1 :L5), (n0), (n1) RETURN n0.v AS a, n1.v AS b";
    final String slow = "MATCH (n1), (n0), (n0 :L1 :L5) RETURN n0.v AS a, n1.v AS b";

    // Both formulations must return the same result set.
    final List<String> fastRows = collectRows(fast);
    final List<String> slowRows = collectRows(slow);

    final int totalNodes = L1L5_COUNT + PLAIN_COUNT;
    final int expected = L1L5_COUNT * totalNodes; // n0 in L1&L5, n1 in all nodes
    assertThat(fastRows).hasSize(expected);
    assertThat(slowRows).containsExactlyInAnyOrderElementsOf(fastRows);

    // The engine must not do more work just because the bare occurrence of n0 was written first.
    final long fastWork = profileTotalRows(fast);
    final long slowWork = profileTotalRows(slow);

    assertThat(slowWork)
        .as("reordering repeated-variable pattern parts must not multiply the work "
            + "(fast=%d, slow=%d rows processed)", fastWork, slowWork)
        .isEqualTo(fastWork);
  }

  private List<String> collectRows(final String cypher) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("a") + "|" + r.getProperty("b"));
      }
      rs.close();
    });
    return rows;
  }

  /**
   * Runs the query under PROFILE and sums every "&lt;n&gt; rows" counter reported across the plan
   * steps. This is a deterministic proxy for the total work performed by the plan.
   */
  private long profileTotalRows(final String cypher) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });

    long total = 0;
    final Matcher m = Pattern.compile("([\\d,]+) rows").matcher(plan.toString());
    while (m.find())
      total += Long.parseLong(m.group(1).replace(",", ""));
    return total;
  }
}
