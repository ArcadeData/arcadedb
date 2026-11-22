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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13019: an aggregate nested in another aggregate was refused in most positions, but three positions were not walked by
 * the semantic validation, so the statement was answered with a number instead:
 * <ul>
 *   <li>under a property access on a map literal: {@code sum({a: count(n.x)}.a)};</li>
 *   <li>under an index access on a list literal: {@code sum([1, 2][count(n.x) - 1])};</li>
 *   <li>inside the body of a reduce(): {@code reduce(acc = 0, x IN [1, 2] | acc + count(n.x))}.</li>
 * </ul>
 * The shapes that are legal stay legal: an aggregate in the initial value or in the list of a reduce is evaluated once per group,
 * a map literal or a map projection holding an aggregate is a projection, and two aggregates next to each other are not nested.
 */
class CypherNestedAggregationPathsIssue13019Test extends TestHelper {

  @BeforeEach
  void createGraph() {
    database.transaction(() -> database.command("opencypher", "CREATE (:P {x: 1}), (:P {x: 2}), (:P {x: 3})"));
  }

  @Test
  void anAggregateUnderAPropertyAccessOnAMapLiteralIsRefused() {
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:P) RETURN sum({a: count(n.x)}.a)"))//
        .isInstanceOf(CommandParsingException.class)//
        .hasMessageContaining("NestedAggregation");
  }

  @Test
  void anAggregateUnderAnIndexAccessOnAListLiteralIsRefused() {
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:P) RETURN sum([1, 2][count(n.x) - 1])"))//
        .isInstanceOf(CommandParsingException.class)//
        .hasMessageContaining("NestedAggregation");
  }

  @Test
  void anAggregateInTheBodyOfAReduceIsRefused() {
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:P) RETURN reduce(acc = 0, x IN [1, 2] | acc + count(n.x))"))//
        .isInstanceOf(CommandParsingException.class)//
        .hasMessageContaining("InvalidAggregation");

    // the same for the body of an allReduce
    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:P) RETURN allReduce(acc = 0, x IN [1, 2] | acc + count(n.x), acc > 0)"))//
        .isInstanceOf(CommandParsingException.class)//
        .hasMessageContaining("InvalidAggregation");

    // the list of the reduce is evaluated once per group: an aggregate there is legal
    assertThatCode(() -> database.command("opencypher", "MATCH (n:P) RETURN reduce(acc = 0, x IN collect(n.x) | acc + x)"))//
        .doesNotThrowAnyException();
  }

  @Test
  void theLegalShapesKeepWorking() {
    // a map literal holding an aggregate is a projection, not a nested aggregate
    assertThat(values("MATCH (n:P) RETURN {v: count(n.x)} AS m", "m")).hasSize(1);

    // the aggregate of the initial value of a reduce is evaluated once per group
    assertThat(values("MATCH (n:P) RETURN reduce(m = count(n.x), x IN [] | m) AS m", "m")).containsExactly(3L);

    // two aggregates next to each other are not nested
    assertThat(values("MATCH (n:P) RETURN count(n.x) + count(*) AS c", "c")).containsExactly(6L);

    // a slice and a chained access over a list and a map of plain values keep working
    assertThat(values("RETURN [1, 2, 3][0] AS v", "v")).containsExactly(1L);
    assertThat(values("RETURN {a: 1}.a AS v", "v")).containsExactly(1L);
    assertThat(values("RETURN [1, 2, 3][0..1] AS v", "v")).containsExactly(Arrays.asList(1L));
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
