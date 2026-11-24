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
import com.arcadedb.query.opencypher.ast.AllReduceExpression;
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpressionWrapper;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.ListComprehensionExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.ListPredicateExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.ReduceExpression;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12723: a list comprehension, reduce(), allReduce(), the list predicates and a pattern comprehension evaluate their
 * expressions on the outer row with their own variables bound over it, instead of on a copy of every outer column made per
 * element. These tests pin what the expressions answer in the shapes the binding has to get right: a variable shadowing an
 * outer column, nesting, an accumulator, a wide row, null items under three-valued logic, a mapping that builds a value per
 * element, and the hops of a pattern comprehension.
 */
class CypherComprehensionRowCopyIssue12723Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:P {name: 'a'})-[:K {w: 1}]->(b:P {name: 'b'}), (a)-[:K {w: 2}]->(c:P {name: 'c'}), (b)-[:K {w: 3}]->(c)"""));
  }

  @Test
  void comprehensionVariableShadowsAnOuterColumnOnlyInsideTheComprehension() {
    final Result row = single("WITH 1 AS x, 10 AS y RETURN [x IN [5, 6] | x + y] AS r, x AS outer");
    assertThat(row.<List<Object>>getProperty("r")).containsExactly(15L, 16L);
    assertThat(row.<Object>getProperty("outer")).isEqualTo(1L);

    assertThat(value("WITH 1 AS x RETURN [x IN [5, 6, 7] WHERE x > 5 | x * 2] AS r")).isEqualTo(List.of(12L, 14L));
    assertThat(value("WITH 1 AS x RETURN [x IN [5, 6, 7] WHERE x > 5] AS r")).isEqualTo(List.of(6L, 7L));
    // The outer column stays readable next to the variable
    assertThat(value("WITH 2 AS t RETURN [x IN [1, 2, 3] WHERE x <> t | x + t] AS r")).isEqualTo(List.of(3L, 5L));
  }

  @Test
  void nestedComprehensionsSeeEveryEnclosingVariable() {
    assertThat(value("WITH 100 AS base RETURN [x IN [1, 2] | [y IN [10, 20] | base + x + y]] AS r"))
        .isEqualTo(List.of(List.of(111L, 121L), List.of(112L, 122L)));
    // An inner variable shadowing the outer one, whose list reads the outer one
    assertThat(value("RETURN [x IN [1, 2] | [x IN [x * 10, x * 100] | x + 1]] AS r"))
        .isEqualTo(List.of(List.of(11L, 101L), List.of(21L, 201L)));
    assertThat(value("RETURN [x IN [1, 2, 3] WHERE any(y IN [2, 3] WHERE y = x) | reduce(s = x, z IN [x, x] | s + z)] AS r"))
        .isEqualTo(List.of(6L, 9L));
  }

  @Test
  void mappingBuildsItsOwnValuePerElement() {
    // Each element keeps the value bound while it was built, so nothing built for one element sees a later element
    final List<Object> maps = (List<Object>) value("MATCH (n:P {name: 'a'}) RETURN [x IN [1, 2, 3] | n {.name, x: x}] AS r");
    assertThat(maps).containsExactly(Map.of("name", "a", "x", 1L), Map.of("name", "a", "x", 2L), Map.of("name", "a", "x", 3L));

    assertThat(value("RETURN [x IN [1, 2] | {x: x, inner: [y IN [x] | y * 10]}] AS r"))
        .isEqualTo(List.of(Map.of("x", 1L, "inner", List.of(10L)), Map.of("x", 2L, "inner", List.of(20L))));
  }

  @Test
  void reduceOverAWideRowReadsTheOuterColumnsAndItsAccumulator() {
    final String columns = "WITH 1 AS c1, 2 AS c2, 3 AS c3, 4 AS c4, 5 AS c5, 6 AS c6, 7 AS c7, 8 AS c8, 9 AS c9, 10 AS c10, "
        + "11 AS c11, 12 AS c12, 13 AS c13 ";
    // 1..20 sum to 210, plus (1 + 13) on each of the 20 steps
    assertThat(value(columns + "RETURN reduce(s = 0, x IN range(1, 20) | s + x + c1 + c13) AS r")).isEqualTo(490L);
    assertThat(value(columns + "RETURN reduce(s = '', x IN ['a', 'b'] | s + x + toString(c7)) AS r")).isEqualTo("a7b7");

    // The accumulator shadows an outer column of the same name only inside reduce()
    final Result row = single("WITH 1000 AS s RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS r, s AS outer");
    assertThat(row.<Object>getProperty("r")).isEqualTo(6L);
    assertThat(row.<Object>getProperty("outer")).isEqualTo(1000L);

    assertThat(value("RETURN reduce(s = 0, x IN [1, null, 2] | s + coalesce(x, 10)) AS r")).isEqualTo(13L);
    assertThat(value("RETURN reduce(s = [], x IN [1, 2, 3] | s + [x * x]) AS r")).isEqualTo(List.of(1L, 4L, 9L));
    assertThat(value("RETURN reduce(s = 0, x IN [] | s + x) AS r")).isEqualTo(0L);
    assertThat(value("RETURN reduce(s = 0, x IN null | s + x) AS r")).isNull();
  }

  @Test
  void allReduceChecksEveryAccumulatedValue() {
    assertThat(value("RETURN allReduce(acc = 0, x IN [1, 2, 3] | acc + x, acc < 10) AS r")).isEqualTo(true);
    assertThat(value("RETURN allReduce(acc = 0, x IN [5, 6] | acc + x, acc < 10) AS r")).isEqualTo(false);
    assertThat(value("WITH 4 AS limit RETURN allReduce(acc = 0, x IN [1, 2, 3] | acc + x, acc < limit + x) AS r")).isEqualTo(true);
    assertThat(value("WITH 100 AS acc RETURN allReduce(acc = 0, x IN [1, 2] | acc + x, acc < 5) AS r")).isEqualTo(true);
    assertThat(value("RETURN allReduce(acc = 0, x IN [] | acc + x, acc < 0) AS r")).isEqualTo(true);
  }

  @Test
  void listPredicatesKeepThreeValuedLogicOverNullItems() {
    assertThat(value("RETURN any(x IN [1, null, 3] WHERE x > 2) AS r")).isEqualTo(true);
    assertThat(value("RETURN any(x IN [1, null] WHERE x > 2) AS r")).isNull();
    assertThat(value("RETURN any(x IN [1, 2] WHERE x > 2) AS r")).isEqualTo(false);
    assertThat(value("RETURN all(x IN [3, 4] WHERE x > 2) AS r")).isEqualTo(true);
    assertThat(value("RETURN all(x IN [3, null] WHERE x > 2) AS r")).isNull();
    assertThat(value("RETURN all(x IN [1, null] WHERE x > 2) AS r")).isEqualTo(false);
    assertThat(value("RETURN none(x IN [1, 2] WHERE x > 2) AS r")).isEqualTo(true);
    assertThat(value("RETURN none(x IN [1, null] WHERE x > 2) AS r")).isNull();
    assertThat(value("RETURN none(x IN [3, null] WHERE x > 2) AS r")).isEqualTo(false);
    assertThat(value("RETURN single(x IN [3, 1] WHERE x > 2) AS r")).isEqualTo(true);
    assertThat(value("RETURN single(x IN [3, null] WHERE x > 2) AS r")).isNull();
    assertThat(value("RETURN single(x IN [3, 4] WHERE x > 2) AS r")).isEqualTo(false);
    assertThat(value("RETURN single(x IN [1, 2] WHERE x > 2) AS r")).isEqualTo(false);
    assertThat(value("RETURN any(x IN null WHERE x > 2) AS r")).isNull();
  }

  @Test
  void listPredicateVariableShadowsAnOuterColumnAndReadsTheOthers() {
    assertThat(value("WITH 5 AS x RETURN any(x IN [1, 2] WHERE x = 5) AS r")).isEqualTo(false);
    assertThat(value("WITH 2 AS t RETURN any(x IN [1, 2] WHERE x = t) AS r")).isEqualTo(true);
    assertThat(value("WITH 2 AS t RETURN all(x IN [1, 2] WHERE x <= t) AS r")).isEqualTo(true);
    // The same predicate filters rows, where the outer row is every column of the match
    assertThat(names("MATCH (n:P) WHERE any(x IN ['a', 'c'] WHERE x = n.name) RETURN n.name AS name ORDER BY name"))
        .containsExactly("a", "c");
    assertThat(names("MATCH (n:P) WHERE none(x IN ['a', 'c'] WHERE x = n.name) RETURN n.name AS name ORDER BY name"))
        .containsExactly("b");
  }

  @Test
  void patternComprehensionBindsItsHopsOverTheOuterRow() {
    assertThat(sorted(value("MATCH (a:P {name: 'a'}) RETURN [(a)-[r:K]->(m) | m.name + toString(r.w)] AS r")))
        .containsExactly("b1", "c2");
    // Two hops, an anonymous middle node and a path variable
    assertThat(value("MATCH (a:P {name: 'a'}) RETURN [p = (a)-->()-->(z) | z.name + toString(length(p))] AS r"))
        .isEqualTo(List.of("c2"));
    // An outer column read by the projection and by the filter
    assertThat(sorted(value("MATCH (a:P {name: 'a'}) WITH a, 2 AS minW RETURN [(a)-[r:K]->(m) WHERE r.w >= minW | m.name] AS r")))
        .containsExactly("c");
    // Uncorrelated start: every candidate is bound in turn
    assertThat(sorted(value("RETURN [(x:P)-[:K]->(y) | x.name + y.name] AS r"))).containsExactly("ab", "ac", "bc");
    // A list comprehension around a pattern comprehension, whose filter reads the comprehension's variable
    assertThat(value("MATCH (a:P {name: 'a'}) RETURN [w IN [1, 2] | [(a)-[r:K]->(m) WHERE r.w = w | m.name]] AS r"))
        .isEqualTo(List.of(List.of("b"), List.of("c")));
    // A pattern comprehension around a list comprehension reading the hop's variables
    assertThat(sorted(value("MATCH (a:P {name: 'a'}) RETURN [(a)-[r:K]->(m) | [x IN [r.w] | m.name + toString(x)][0]] AS r")))
        .containsExactly("b1", "c2");
  }

  @Test
  void comprehensionOverAnAggregateReadsTheGroupingColumn() {
    final Result row = single("UNWIND [1, 2, 3] AS v WITH 10 AS k, v RETURN k, [x IN collect(v) WHERE x > 1 | x + k] AS r");
    assertThat(row.<Object>getProperty("k")).isEqualTo(10L);
    assertThat(row.<List<Object>>getProperty("r")).containsExactly(12L, 13L);
  }

  @Test
  void subqueryExpressionsInsideAComprehensionReadEachElementBinding() {
    // The body of a COUNT/COLLECT/EXISTS is seeded with the row the comprehension rebinds per element, and its step chain is
    // run again for the next element: each run must read the element it was seeded with
    assertThat(value("RETURN [x IN [1, 2, 3] | COUNT { UNWIND range(1, x) AS y RETURN y }] AS r")).isEqualTo(List.of(1L, 2L, 3L));
    assertThat(value("RETURN [x IN [1, 2, 3] | COLLECT { UNWIND range(1, x) AS y RETURN y * 10 }] AS r"))
        .isEqualTo(List.of(List.of(10L), List.of(10L, 20L), List.of(10L, 20L, 30L)));
    assertThat(value("RETURN [x IN [0, 1, 2] WHERE EXISTS { UNWIND range(1, x) AS y RETURN y }] AS r")).isEqualTo(List.of(1L, 2L));
    assertThat(value("RETURN reduce(s = 0, x IN [1, 2, 3] | s + COUNT { UNWIND range(1, x) AS y RETURN y }) AS r")).isEqualTo(6L);
    assertThat(value("RETURN any(x IN [1, 2] WHERE COUNT { UNWIND range(1, x) AS y RETURN y } = 2) AS r")).isEqualTo(true);
    // A body that matches against an outer node and the element
    assertThat(value("MATCH (a:P {name: 'a'}) RETURN [n IN ['b', 'c', 'z'] | COUNT { MATCH (a)-[:K]->(m:P) WHERE m.name = n RETURN m }] AS r"))
        .isEqualTo(List.of(1L, 1L, 0L));
  }

  @Test
  void theOuterRowIsNotCopiedPerElement() {
    // The defect: every element of the list copied every column of the outer row, which enumerated the outer row's names once
    // per element. The row is now read through, so the names are not enumerated whatever the length of the list
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final ExpressionEvaluator evaluator = OpenCypherQueryEngine.getExpressionEvaluator();

    final Expression list = longList(ELEMENTS);
    final Expression x = new VariableExpression("x");
    final Expression c1 = new VariableExpression("c1");

    final ListComprehensionExpression comprehension = new ListComprehensionExpression("x", list,
        new ComparisonExpressionWrapper(x, ComparisonExpression.Operator.GREATER_THAN, new LiteralExpression(0L, "0")),
        new ArithmeticExpression(x, ArithmeticExpression.Operator.ADD, c1), "[x IN list WHERE x > 0 | x + c1]");
    CountingRow row = wideRow();
    final List<Object> expected = new ArrayList<>();
    for (long i = 1; i <= ELEMENTS; i++)
      expected.add(i + 1);
    assertThat(evaluator.evaluate(comprehension, row, context)).isEqualTo(expected);
    assertNotEnumeratedPerElement(row, "list comprehension");

    // The comprehension of an aggregating projection, which the evaluator runs itself
    row = wideRow();
    evaluator.setAggregationOverrides(Map.of());
    try {
      assertThat(evaluator.evaluate(comprehension, row, context)).isEqualTo(expected);
    } finally {
      evaluator.clearAggregationOverrides();
    }
    assertNotEnumeratedPerElement(row, "list comprehension under aggregation");

    final Expression s = new VariableExpression("s");
    row = wideRow();
    assertThat(evaluator.evaluate(new ReduceExpression("s", new LiteralExpression(0L, "0"), "x", list,
        new ArithmeticExpression(s, ArithmeticExpression.Operator.ADD, x), "reduce(s = 0, x IN list | s + x)"), row, context))
        .isEqualTo((long) ELEMENTS * (ELEMENTS + 1) / 2);
    assertNotEnumeratedPerElement(row, "reduce()");

    row = wideRow();
    assertThat(evaluator.evaluate(new AllReduceExpression("s", new LiteralExpression(0L, "0"), "x", list,
        new ArithmeticExpression(s, ArithmeticExpression.Operator.ADD, x),
        new ComparisonExpressionWrapper(s, ComparisonExpression.Operator.GREATER_THAN, new LiteralExpression(0L, "0")),
        "allReduce(s = 0, x IN list | s + x, s > 0)"), row, context)).isEqualTo(true);
    assertNotEnumeratedPerElement(row, "allReduce()");

    for (final ListPredicateExpression.PredicateType type : ListPredicateExpression.PredicateType.values()) {
      row = wideRow();
      // Always true for all() and never true for the others, so none of them stops before the last element
      final Expression where = type == ListPredicateExpression.PredicateType.ALL ?
          new ComparisonExpressionWrapper(x, ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, c1) :
          new ComparisonExpressionWrapper(x, ComparisonExpression.Operator.LESS_THAN, c1);
      evaluator.evaluate(new ListPredicateExpression(type, "x", list, where, type + "(x IN list WHERE ...)"), row, context);
      assertNotEnumeratedPerElement(row, type.name().toLowerCase() + "()");
    }
  }

  private static final int ELEMENTS = 50;

  private static void assertNotEnumeratedPerElement(final CountingRow row, final String what) {
    assertThat(row.nameListings).as("%s over %d elements enumerated the outer row's names %d times", what, ELEMENTS,
        row.nameListings).isLessThanOrEqualTo(1);
  }

  private static Expression longList(final int size) {
    final List<Expression> elements = new ArrayList<>(size);
    for (long i = 1; i <= size; i++)
      elements.add(new LiteralExpression(i, Long.toString(i)));
    return new ListExpression(elements, "list");
  }

  private static CountingRow wideRow() {
    final Map<String, Object> columns = new LinkedHashMap<>();
    for (int i = 1; i <= 13; i++)
      columns.put("c" + i, (long) i);
    return new CountingRow(columns);
  }

  /**
   * An outer row that counts how often its names are enumerated, which a copy of it does once per copy.
   */
  private static final class CountingRow extends ResultInternal {
    private int nameListings;

    private CountingRow(final Map<String, Object> columns) {
      super(columns);
    }

    @Override
    public Set<String> getPropertyNames() {
      ++nameListings;
      return super.getPropertyNames();
    }

    @Override
    public boolean listsProperty(final String name) {
      return content.containsKey(name);
    }
  }

  private Object value(final String query) {
    return single(query).getProperty("r");
  }

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final List<Result> rows = new ArrayList<>();
      rs.forEachRemaining(rows::add);
      assertThat(rows).as(query).hasSize(1);
      return rows.getFirst();
    }
  }

  private List<String> names(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final List<String> names = new ArrayList<>();
      rs.forEachRemaining(row -> names.add(row.getProperty("name")));
      return names;
    }
  }

  private static List<String> sorted(final Object list) {
    final String[] values = ((List<?>) list).stream().map(String::valueOf).toArray(String[]::new);
    Arrays.sort(values);
    return List.of(values);
  }
}
