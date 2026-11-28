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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PROFILE of a {@code CALL { ... }} subquery prints the time the step spent on the rows it was given. Whatever way the body is run
 * (streamed, collected because it can write, in batches of its own transactions), the time is a part of the query's own execution
 * time, and a step that ran its body for hundreds of rows spent some of it.
 */
class CypherSubqueryStepProfileTest extends TestHelper {
  private static final Pattern CALL_COST = Pattern.compile("\\+ (?:OPTIONAL )?CALL SUBQUERY \\{ \\.\\.\\. \\}[^\\n]*\\(([\\d,]+)μs\\)");
  private static final Pattern EXECUTION_TIME = Pattern.compile("Execution Time: ([\\d,.]+) ms");

  @Test
  void aStreamedBodyReportsItsTimeAsPartOfTheQuery() {
    assertCostIsPartOfTheQuery("PROFILE UNWIND range(1, 300) AS i CALL (i) { RETURN i * 2 AS v } RETURN i, v");
  }

  @Test
  void aBodyThatCanWriteReportsItsTimeAsPartOfTheQuery() {
    assertCostIsPartOfTheQuery("PROFILE UNWIND range(1, 300) AS i CALL (i) { CREATE (n:T {i: i}) RETURN n.i AS v } RETURN i, v");
  }

  @Test
  void anOptionalCallReportsItsTimeAsPartOfTheQuery() {
    assertCostIsPartOfTheQuery("PROFILE UNWIND range(1, 300) AS i OPTIONAL CALL (i) { MATCH (m:Nothing) RETURN m AS v } RETURN i, v");
  }

  @Test
  void aCallInTransactionsReportsItsTimeAsPartOfTheQuery() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 300) AS i CREATE (:S {i: i})"));
    final String profile = profile("PROFILE MATCH (s:S) CALL (s) { SET s.v = 1 } IN TRANSACTIONS OF 100 ROWS RETURN count(*) AS c");
    assertCost(profile);
    assertThat(profile).contains("IN TRANSACTIONS");
  }

  private void assertCostIsPartOfTheQuery(final String statement) {
    assertCost(profile(statement));
  }

  private String profile(final String statement) {
    // A CALL IN TRANSACTIONS is refused inside a caller's transaction: the statement runs in its own
    try (final ResultSet rs = database.command("opencypher", statement)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private static void assertCost(final String profile) {
    final Matcher cost = CALL_COST.matcher(profile);
    assertThat(cost.find()).as(profile).isTrue();
    final long costMicros = Long.parseLong(cost.group(1).replace(",", ""));
    final Matcher total = EXECUTION_TIME.matcher(profile);
    assertThat(total.find()).as(profile).isTrue();
    final double totalMicros = Double.parseDouble(total.group(1).replace(",", "")) * 1_000;

    assertThat(costMicros).as("the body ran for 300 rows: %s", profile).isGreaterThan(0);
    assertThat((double) costMicros).as("the step is part of the query: %s", profile).isLessThanOrEqualTo(totalMicros + 1);
  }
}
