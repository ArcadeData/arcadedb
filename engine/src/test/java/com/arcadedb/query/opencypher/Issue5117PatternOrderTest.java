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
 * Regression test for issue #5117: reordering independent (disconnected) comma-separated pattern
 * parts inside a single MATCH must not change the amount of work the engine performs.
 * <p>
 * The reported query mixes a multi-hop connected pattern with a standalone node pattern:
 * <pre>
 *   MATCH (n1:L1)&lt;-[r0:T4]-(n2:L0:L1)&lt;-[r1:T1]-(n3), (n4:L3) WHERE ...
 * </pre>
 * The multi-label conjunction {@code (n2:L0:L1)} disqualifies the cost-based optimizer, so the query
 * runs on the legacy step-chain path. That path chained comma-separated pattern parts in textual
 * order as a left-deep nested-loop Cartesian product: the first-listed part is the outer loop and
 * every later part is re-executed once per outer row. Writing the cheap {@code (n4:L3)} scan first
 * therefore re-executed the expensive {@code n3->n2->n1} traversal once for every {@code L3} vertex,
 * producing the reported ~33x slowdown versus writing the traversal first.
 * <p>
 * The fix reorders independent components so the expensive (edge-bearing) component drives as the
 * outer loop regardless of the written order, making both formulations do the same work. The test is
 * deterministic (it compares the PROFILE row-count totals, not wall-clock time) and also asserts the
 * two orderings return identical results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5117PatternOrderTest {
  private Database database;

  private static final int L3_COUNT    = 30;
  private static final int CHAIN_COUNT = 5;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/test-issue5117").create();

    database.transaction(() -> {
      // Standalone, disconnected L3 nodes.
      for (int i = 0; i < L3_COUNT; i++)
        database.command("opencypher", "CREATE (n:L3 {v: " + i + "})");

      // Connected chains: (n3:L2)-[:T1]->(n2:L0:L1)-[:T4]->(n1:L1)
      // i.e. matching pattern (n1:L1)<-[r0:T4]-(n2:L0:L1)<-[r1:T1]-(n3).
      for (int j = 0; j < CHAIN_COUNT; j++) {
        database.command("opencypher",
            "CREATE (a:L1 {name: 'a" + j + "'}), (b:L0:L1 {name: 'b" + j + "'}), (c:L2 {name: 'c" + j + "'}), "
                + "(b)-[:T4 {id: " + (10 + j) + "}]->(a), "
                + "(c)-[:T1 {id: " + (500 + j) + ", k43: " + (1000 + j) + "}]->(b)");
      }
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
  void patternOrderDoesNotChangeWork() {
    final String where = " WHERE (((r1.k43) > -2019129302) AND ((r0.id) <> (r1.id)))";
    final String fast = "MATCH (n1 :L1)<-[r0 :T4]-(n2 :L0 :L1)<-[r1 :T1]-(n3), (n4 :L3)" + where
        + " RETURN n1.name AS a, n4.v AS v";
    final String slow = "MATCH (n4 :L3), (n1 :L1)<-[r0 :T4]-(n2 :L0 :L1)<-[r1 :T1]-(n3)" + where
        + " RETURN n1.name AS a, n4.v AS v";

    // Both formulations must return the same result set.
    final List<String> fastRows = collectRows(fast);
    final List<String> slowRows = collectRows(slow);

    final int expected = CHAIN_COUNT * L3_COUNT; // 5 chains x 30 L3 nodes
    assertThat(fastRows).hasSize(expected);
    assertThat(slowRows).containsExactlyInAnyOrderElementsOf(fastRows);

    // The engine must not do more work just because the disconnected node was written first.
    final long fastWork = profileTotalRows(fast);
    final long slowWork = profileTotalRows(slow);

    assertThat(slowWork)
        .as("reordering independent pattern parts must not multiply the work of the expensive "
            + "traversal (fast=%d, slow=%d rows processed)", fastWork, slowWork)
        .isEqualTo(fastWork);
  }

  private List<String> collectRows(final String cypher) {
    final List<String> rows = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher", cypher);
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("a") + "|" + r.getProperty("v"));
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
