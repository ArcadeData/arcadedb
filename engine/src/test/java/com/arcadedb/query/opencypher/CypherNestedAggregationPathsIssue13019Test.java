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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13019: an aggregate nested in another aggregate was refused in most positions, but three positions were not walked by
 * the semantic validation, so the statement was answered with a number instead:
 * <ul>
 *   <li>under a property access on a map literal: {@code sum({a: count(n.x)}.a)};</li>
 *   <li>under an index access on a list literal: {@code sum([1, 2][count(n.x) - 1])};</li>
 *   <li>inside the body of a reduce(): {@code reduce(acc = 0, x IN [1, 2] | acc + count(n.x))}.</li>
 * </ul>
 * The same holds for every other expression that runs once per element of a list (the predicate of an allReduce, the WHERE
 * of a list comprehension, of all/any/none/single) and for an aggregate under IN, IS NULL and the other boolean operators.
 * The shapes that are legal stay legal: an aggregate in the initial value or in the list of a reduce is evaluated once per group,
 * a map literal or a map projection holding an aggregate is a projection, and two aggregates next to each other are not nested.
 */
class CypherNestedAggregationPathsIssue13019Test extends TestHelper {
  private static final String NESTED      = "NestedAggregation: Nested aggregation functions are not allowed";
  private static final String IN_REDUCE   = "InvalidAggregation: Aggregation functions are not allowed in reduce expressions";
  private static final String IN_LIST_OPS = "InvalidAggregation: Aggregation functions are not allowed in list comprehensions";
  private static final String IN_LIST_PREDICATES = "InvalidAggregation: Aggregation functions are not allowed in list predicates";

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {x: 1}), (:P {x: 2}), (:P {x: 3})"));
  }

  @Test
  void anAggregateUnderAPropertyAccessOnAMapLiteralIsRefused() {
    assertRefused("MATCH (n:P) RETURN sum({a: count(n.x)}.a)", NESTED);
  }

  @Test
  void anAggregateUnderAnIndexAccessOnAListLiteralIsRefused() {
    assertRefused("MATCH (n:P) RETURN sum([1, 2][count(n.x) - 1])", NESTED);
  }

  @Test
  void anAggregateUnderASliceIsRefused() {
    assertRefused("MATCH (n:P) RETURN sum(size([1, 2, 3][0..count(n.x)]))", NESTED);
    assertRefused("MATCH (n:P) RETURN sum(size([1, 2, 3][count(n.x)..]))", NESTED);
  }

  @Test
  void anAggregateUnderABooleanOperatorInsideAnAggregateIsRefused() {
    assertRefused("MATCH (n:P) RETURN sum(CASE WHEN count(n.x) IN [3] THEN 1 ELSE 0 END)", NESTED);
    assertRefused("MATCH (n:P) RETURN sum(CASE WHEN count(n.x) IS NULL THEN 1 ELSE 0 END)", NESTED);
    assertRefused("MATCH (n:P) RETURN collect(toString(count(n.x)) STARTS WITH '3')", NESTED);
  }

  @Test
  void anAggregateInTheBodyOfAReduceIsRefused() {
    assertRefused("MATCH (n:P) RETURN reduce(acc = 0, x IN [1, 2] | acc + count(n.x))", IN_REDUCE);

    // the same for the body and the predicate of an allReduce
    assertRefused("MATCH (n:P) RETURN allReduce(acc = 0, x IN [1, 2] | acc + count(n.x), acc > 0)", IN_REDUCE);
    assertRefused("MATCH (n:P) RETURN allReduce(acc = 0, x IN [1, 2] | acc + x, acc < count(n.x))", IN_REDUCE);

    // the list of the reduce is evaluated once per group: an aggregate there is legal
    assertThat(values("MATCH (n:P) RETURN reduce(acc = 0, x IN collect(n.x) | acc + x) AS s", "s")).containsExactly(6L);
  }

  @Test
  void anAggregateInTheWhereOfAListComprehensionIsRefused() {
    assertRefused("MATCH (n:P) RETURN [x IN [1, 2, 3, 4] WHERE x < count(n.x)] AS l", IN_LIST_OPS);
    assertRefused("MATCH (n:P) RETURN [x IN [1, 2, 3, 4] WHERE x < count(n.x) | x * 2] AS l", IN_LIST_OPS);
  }

  @Test
  void anAggregateInTheWhereOfAListPredicateIsRefused() {
    assertRefused("MATCH (n:P) RETURN any(x IN [1, 2] WHERE x < count(n.x)) AS b", IN_LIST_PREDICATES);
    assertRefused("MATCH (n:P) RETURN all(x IN [1, 2] WHERE x < count(n.x)) AS b", IN_LIST_PREDICATES);
    assertRefused("MATCH (n:P) RETURN none(x IN [1, 2] WHERE x < count(n.x)) AS b", IN_LIST_PREDICATES);
    assertRefused("MATCH (n:P) RETURN single(x IN [1, 2] WHERE x < count(n.x)) AS b", IN_LIST_PREDICATES);
    // inside an aggregate the inner aggregate is nested as well: the per-element scope is the more precise refusal
    assertRefused("MATCH (n:P) RETURN sum(CASE WHEN any(x IN [1] WHERE x < count(n.x)) THEN 1 ELSE 0 END)", IN_LIST_PREDICATES);

    // the list of a list predicate is evaluated once per group: an aggregate there is legal
    assertThat(values("MATCH (n:P) RETURN any(x IN collect(n.x) WHERE x > 2) AS b", "b")).containsExactly(true);
  }

  @Test
  void aPerElementScopeIsFoundUnderEveryExpression() {
    // the walk descends into a property access, an index access and a boolean operand to reach the per-element scope
    assertRefused("MATCH (n:P) RETURN {a: [x IN [1, 2] | count(n.x)]}.a AS l", IN_LIST_OPS);
    assertRefused("MATCH (n:P) RETURN [reduce(acc = 0, x IN [1] | acc + count(n.x))][0] AS l", IN_REDUCE);
    assertRefused("MATCH (n:P) RETURN 1 IN [x IN [1, 2] | count(n.x)] AS b", IN_LIST_OPS);
  }

  @Test
  void anAggregateHiddenInAWhereOrInAnOrderByAfterANonAggregatingReturnIsRefused() {
    // the WHERE and ORDER BY checks walked only function calls and arithmetic: the same positions hid the aggregate there
    assertRefused("MATCH (n:P) WHERE {a: count(n.x)}.a > 0 RETURN n.x", "InvalidAggregation: Aggregation functions are not allowed in WHERE");
    assertRefused("MATCH (n:P) WHERE [count(n.x)][0] > 0 RETURN n.x", "InvalidAggregation: Aggregation functions are not allowed in WHERE");
    assertRefused("MATCH (n:P) WHERE CASE WHEN count(n.x) > 0 THEN true ELSE false END RETURN n.x",
        "InvalidAggregation: Aggregation functions are not allowed in WHERE");
    assertRefused("MATCH (n:P) RETURN n.x ORDER BY [count(n.x)][0]",
        "InvalidAggregation: Aggregation functions are not allowed in ORDER BY after RETURN");
    assertRefused("MATCH (n:P) RETURN n.x ORDER BY {a: count(n.x)}.a",
        "InvalidAggregation: Aggregation functions are not allowed in ORDER BY after RETURN");

    // a WHERE and an ORDER BY without an aggregate keep working
    assertThat(values("MATCH (n:P) WHERE {a: n.x}.a > 1 RETURN n.x AS x ORDER BY [n.x][0]", "x")).containsExactly(2, 3);
  }

  @Test
  void theLegalShapesKeepWorking() {
    // a map literal holding an aggregate is a projection, not a nested aggregate
    assertThat(values("MATCH (n:P) RETURN {v: count(n.x)} AS m", "m")).hasSize(1);

    // ... and so is a property access over it (named by the issue as a shape that must keep working)
    assertThat(values("MATCH (n:P) RETURN {v: count(n.x)}.v AS v", "v")).containsExactly(3L);
    assertThat(values("MATCH (n:P) RETURN [count(n.x), 0][0] AS v", "v")).containsExactly(3L);

    // the aggregate of the initial value of a reduce is evaluated once per group
    assertThat(values("MATCH (n:P) RETURN reduce(m = count(n.x), x IN [] | m) AS m", "m")).containsExactly(3L);

    // two aggregates next to each other are not nested
    assertThat(values("MATCH (n:P) RETURN count(n.x) + count(*) AS c", "c")).containsExactly(6L);
    assertThat(values("MATCH (n:P) RETURN count(n.x) AS c ORDER BY count(n.x)", "c")).containsExactly(3L);

    // an aggregate over a boolean operator is a plain aggregate
    assertThat(values("MATCH (n:P) RETURN sum(CASE WHEN n.x IN [1, 3] THEN 1 ELSE 0 END) AS s", "s")).containsExactly(2L);

    // a slice and a chained access over a list and a map of plain values keep working
    assertThat(values("RETURN [1, 2, 3][0] AS v", "v")).containsExactly(1L);
    assertThat(values("RETURN {a: 1}.a AS v", "v")).containsExactly(1L);
    assertThat(values("RETURN [1, 2, 3][0..1] AS v", "v")).containsExactly(Arrays.asList(1L));
  }

  private void assertRefused(final String query, final String message) {
    assertThatThrownBy(() -> database.command("opencypher", query))//
        .as(query)//
        .isInstanceOf(CommandParsingException.class)//
        .hasMessageContaining(message);
  }

  private List<Object> values(final String query, final String column) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        values.add(rs.next().getProperty(column));
    }
    return values;
  }
}
