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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11597: a RETURN or WITH item whose aggregate sits under AND, OR, XOR, NOT, IS NULL,
 * IS NOT NULL, IN, STARTS WITH, ENDS WITH, CONTAINS or =~ was taken for a grouping key: {@code RETURN 2 IN collect(n.x)}
 * gave one row per input row, each evaluated on that row alone, and no row at all over an empty input. With no grouping
 * key an aggregation answers exactly one row.
 */
class CypherAggregateInsideBooleanIssue11597Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (:A {x: 1, s: 'a', g: 1}), (:A {x: 2, s: 'b', g: 1}), (:A {x: 3, s: 'c', g: 2})"));
  }

  @Test
  void anAggregateUnderABooleanOperatorAggregatesTheWholeInput() {
    final Object[][] cases = {
        { "MATCH (n:A) RETURN 2 IN collect(n.x) AS c", true },
        { "MATCH (n:A) RETURN 9 IN collect(n.x) AS c", false },
        { "MATCH (n:A) RETURN max(n.x) IS NULL AS c", false },
        { "MATCH (n:A) RETURN head(collect(n.x)) IS NOT NULL AS c", true },
        { "MATCH (n:A) RETURN count(n) > 0 AND true AS c", true },
        { "MATCH (n:A) RETURN count(n) > 5 OR false AS c", false },
        { "MATCH (n:A) RETURN count(n) = 3 XOR false AS c", true },
        { "MATCH (n:A) RETURN NOT (count(n) > 0) AS c", false },
        { "MATCH (n:A) RETURN count(n) > 0 AND min(n.x) = 1 AS c", true },
        { "MATCH (n:A) RETURN max(n.s) STARTS WITH 'c' AS c", true },
        { "MATCH (n:A) RETURN min(n.s) ENDS WITH 'a' AS c", true },
        { "MATCH (n:A) RETURN max(n.s) CONTAINS 'b' AS c", false },
        { "MATCH (n:A) RETURN max(n.s) =~ 'c' AS c", true },
        { "MATCH (n:A) WITH 2 IN collect(n.x) AS c RETURN c", true },
        { "MATCH (n:A) WITH count(n) > 0 AND true AS c RETURN c", true },
        { "MATCH (n:Nope) RETURN max(n.x) IS NULL AS c", true },
        { "MATCH (n:Nope) RETURN 2 IN collect(n.x) AS c", false },
        { "MATCH (n:Nope) RETURN NOT (count(n) > 0) AS c", true } };
    for (final Object[] testCase : cases)
      assertThat(values((String) testCase[0], "c")).as((String) testCase[0]).containsExactly(testCase[1]);
  }

  @Test
  void anAggregateUnderABooleanOperatorAggregatesEachGroup() {
    assertThat(rows("MATCH (n:A) RETURN n.g AS g, 2 IN collect(n.x) AS c ORDER BY g")).containsExactly("1:true", "2:false");
    assertThat(rows("MATCH (n:A) RETURN n.g AS g, count(n) > 1 AND true AS c ORDER BY g")).containsExactly("1:true", "2:false");
    assertThat(rows("MATCH (n:A) RETURN n.g AS g, max(n.s) STARTS WITH 'c' AS c ORDER BY g"))
        .containsExactly("1:false", "2:true");
    assertThat(rows("MATCH (n:A) WITH n.g AS g, NOT (count(n) > 1) AS c RETURN g, c ORDER BY g"))
        .containsExactly("1:false", "2:true");
  }

  private List<Object> values(final String query, final String column) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        out.add(rs.next().getProperty(column));
    }
    return out;
  }

  private List<String> rows(final String query) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.<Number>getProperty("g").longValue() + ":" + r.getProperty("c"));
      }
    }
    return out;
  }
}
