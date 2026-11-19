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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for #11770: an aggregate inside a map projection, inside a map literal entry (wrapped in another
 * expression), inside reduce() (wrapped in another expression), inside the bounds of a list slice or under a chained
 * property access was not treated as an aggregation: the item became a grouping key evaluated once per input row, or (a map projection) the
 * column vanished from the row. With no grouping key an aggregation answers exactly one row.
 */
class CypherAggregateInsideMapAndReduceIssue11770Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (:P {v: 1, g: 1}), (:P {v: 2, g: 1}), (:P {v: 3, g: 2}),
               (a:Person {name: 'a'})-[:KNOWS]->(:Person {name: 'b'}), (a)-[:KNOWS]->(:Person {name: 'c'})"""));
  }

  @Test
  void anAggregateNestedInAMapLiteralEntryAggregatesTheWholeInput() {
    assertThat(values("MATCH (n:P) RETURN {c: size(collect(n.v))} AS m", "m")).containsExactly(Map.of("c", 3L));
    assertThat(values("MATCH (n:P) RETURN {c: count(*) > 0} AS m", "m")).containsExactly(Map.of("c", true));
    assertThat(values("MATCH (n:P) RETURN {c: toString(count(*))} AS m", "m")).containsExactly(Map.of("c", "3"));
    assertThat(values("MATCH (n:P) RETURN {c: NOT count(*) = 0} AS m", "m")).containsExactly(Map.of("c", true));
    assertThat(values("MATCH (n:P) WITH {c: size(collect(n.v))} AS m RETURN m", "m")).containsExactly(Map.of("c", 3L));
    // An empty input still answers the one aggregate row
    assertThat(values("MATCH (n:P) WHERE n.v > 99 RETURN {c: size(collect(n.v))} AS m", "m")).containsExactly(Map.of("c", 0L));
  }

  @Test
  void anAggregateNestedInReduceAggregatesTheWholeInput() {
    assertThat(values("MATCH (n:P) RETURN reduce(s = 0, x IN tail(collect(n.v)) | s + x) AS r", "r")).containsExactly(5L);
    assertThat(values("MATCH (n:P) RETURN reduce(s = count(*), x IN [1, 2] | s + x) AS r", "r")).containsExactly(6L);
    assertThat(values("MATCH (n:P) WITH reduce(s = 0, x IN tail(collect(n.v)) | s + x) AS r RETURN r", "r")).containsExactly(5L);
    // allReduce() shares the shape: the aggregate nested in the list operand
    assertThat(values("MATCH (n:P) RETURN allReduce(s = 0, x IN tail(collect(n.v)) | s + x, s < 10) AS r", "r")).containsExactly(true);
    assertThat(values("MATCH (n:P) RETURN allReduce(s = 0, x IN tail(collect(n.v)) | s + x, s < 3) AS r", "r")).containsExactly(false);
  }

  @Test
  void anAggregateInAMapProjectionIsProjected() {
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{.name, n: count(f)} AS m", "m"))
        .containsExactly(Map.of("name", "a", "n", 2L));
    assertThat(values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{.name, n: size(collect(f.name))} AS m", "m"))
        .containsExactly(Map.of("name", "a", "n", 2L));

    final List<Object> friends = values("MATCH (p:Person)-[:KNOWS]->(f) RETURN p{.name, friends: collect(f.name)} AS m", "m");
    assertThat(friends).hasSize(1);
    assertThat(((Map<?, ?>) friends.getFirst()).get("name")).isEqualTo("a");
    assertThat((List<Object>) ((Map<?, ?>) friends.getFirst()).get("friends")).containsExactlyInAnyOrder("b", "c");

    final List<Object> viaWith = values(
        "MATCH (p:Person)-[:KNOWS]->(f) WITH p{.name, friends: collect(f.name)} AS m RETURN m", "m");
    assertThat(viaWith).hasSize(1);
    assertThat((List<Object>) ((Map<?, ?>) viaWith.getFirst()).get("friends")).containsExactlyInAnyOrder("b", "c");
  }

  @Test
  void anAggregateInsideAMapOrReduceAggregatesEachGroup() {
    assertThat(rows("MATCH (n:P) RETURN n.g AS g, {c: size(collect(n.v))} AS c ORDER BY g")).containsExactly("1:{c=2}", "2:{c=1}");
    assertThat(rows("MATCH (n:P) RETURN n.g AS g, reduce(s = 0, x IN tail(collect(n.v)) | s + x) AS c ORDER BY g"))
        .containsExactly("1:2", "2:0");
  }

  @Test
  void anAggregateInsideOtherCompositeNodesAggregatesTheWholeInput() {
    final Object[][] cases = {
        // list slice bounds
        { "MATCH (n:P) RETURN [10, 20, 30, 40][0..count(*)] AS c", List.of(10L, 20L, 30L) },
        // chained property access over an aggregate
        { "MATCH (n:P) RETURN collect({v: n.v})[0].v + count(*) AS c", 4L } };
    for (final Object[] testCase : cases)
      assertThat(values((String) testCase[0], "c")).as((String) testCase[0]).containsExactly(testCase[1]);
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
