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
import com.arcadedb.query.opencypher.ast.ArithmeticExpression;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drift guard for issue #6354: the operator semantics of Cypher arithmetic were written twice - once in
 * {@code ArithmeticExpression.evaluate} and once, line for line, in
 * {@code ExpressionEvaluator.evaluateArithmetic}. The second copy exists because the evaluator resolves operands
 * through itself so an inline aggregator sees the pre-computed overrides {@code AggregationStep} /
 * {@code GroupByAggregationStep} install (issue #4100); that difference is two lines, and the sixty-odd others -
 * {@code ||} strict typing (#5298), list concatenation and append (#4284), string concatenation, temporal
 * arithmetic, numeric promotion, overflow and division-by-zero reporting - were duplicated. Each of them was a
 * place where a fix could land on one path and not the other, which is exactly the arrangement that hid the
 * list-slice heap exhaustion until issue #6323.
 * <p>
 * There is a third writing of the same semantics, in {@code ConstantFolder.foldArithmetic}, which answers an
 * operation over two literals at parse time and replaces it with the result - so a fold that disagrees with
 * execution silently rewrites the query into a different one. It now calls the same method, and is checked here
 * as a third path.
 * <p>
 * Every expression below is therefore run three ways: as a WHERE predicate over UNWIND-bound operands, which the
 * AST node evaluates; as a projection over the same operands, which {@code ExpressionEvaluator} evaluates; and as
 * a WHERE predicate over the literals themselves, which the folder answers before execution begins. All three
 * must agree - the same value, the same type, or the same failure. Failing identically matters as much as succeeding: half the operand
 * matrix these issues established is about which error a bad operand produces.
 * <p>
 * {@link #theTwoPathsAreDistinct()} is what keeps this from being a test of one code path run three times.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherArithmeticEvaluatorParityTest {
  private static Database database;

  @BeforeAll
  static void setup() {
    database = new DatabaseFactory("./target/databases/cypherarithmeticparity").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:Sample {num: 7, txt: 'x'})"));
  }

  @AfterAll
  static void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * The operand matrix, one case per line as {@code LEFT @@ OPERATOR @@ RIGHT}. It is split rather than written
   * as one expression because the two paths are reached by two different query shapes, and the shape that reaches
   * the evaluator has to wrap each operand in an aggregate.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      // Numeric promotion and the integer/double split (issues #5163, #5164, #5602).
      "1 @@ + @@ 2", "7 @@ - @@ 9", "6 @@ * @@ 7", "7 @@ / @@ 2", "7 @@ % @@ 3", "2 @@ ^ @@ 10",
      "7.5 @@ / @@ 2", "7 @@ / @@ 2.0", "-3 @@ % @@ 2", "3 @@ % @@ -2", "0.0 @@ / @@ 0.0", "1.0 @@ / @@ 0.0",
      "1 @@ / @@ 0", "1 @@ % @@ 0", "9223372036854775807 @@ + @@ 1", "-9223372036854775808 @@ / @@ -1",
      "9223372036854775807 @@ * @@ 2", "2 @@ ^ @@ 0.5", "-1 @@ ^ @@ 0.5",
      // Null propagation on either side.
      "null @@ + @@ 1", "1 @@ + @@ null", "null @@ * @@ null", "null @@ || @@ 'a'", "'a' @@ || @@ null",
      // String concatenation with + and its coercion, which || deliberately does not do (issue #5298).
      "'a' @@ + @@ 'b'", "'a' @@ + @@ 1", "1 @@ + @@ 'a'", "'a' @@ + @@ true", "'a' @@ || @@ 'b'",
      "'a' @@ || @@ 1", "1 @@ || @@ 2",
      // List concatenation, append and prepend (issue #4284), and the || form that refuses to append.
      "[1, 2] @@ + @@ [3]", "[1, 2] @@ + @@ 3", "1 @@ + @@ [2, 3]", "[1] @@ + @@ []", "[1, 2] @@ || @@ [3]",
      "[1, 2] @@ || @@ 3", "['a'] @@ + @@ 'b'", "'b' @@ + @@ ['a']",
      // A lazy range is a list here too: concatenating one must give the same answer on both paths.
      "range(1, 3) @@ + @@ [4]", "[0] @@ + @@ range(1, 3)", "range(1, 3) @@ || @@ range(4, 5)",
      // Temporal arithmetic.
      "date('2020-01-31') @@ + @@ duration({months: 1})", "date('2020-03-31') @@ - @@ duration({months: 1})",
      "duration({days: 1}) @@ + @@ duration({hours: 2})", "duration({days: 1}) @@ * @@ 3",
      "3 @@ * @@ duration({days: 1})", "duration({days: 3}) @@ / @@ 2",
      "localdatetime('2020-01-01T00:00:00') @@ + @@ duration({seconds: 90})",
      "time('12:00:00Z') @@ + @@ duration({minutes: 30})", "datetime('2020-01-01T00:00:00Z') @@ - @@ duration({days: 1})",
      // Operands that are not arithmetic at all, which must fail the same way on both paths.
      "true @@ + @@ false", "true @@ * @@ 2", "date('2020-01-01') @@ * @@ 2", "{a: 1} @@ + @@ 1",
      "duration({days: 1}) @@ ^ @@ 2",
      // Nested arithmetic, so the recursion into sub-expressions is exercised on both paths too.
      "1 + 2 * 3 @@ - @@ 4 / 2", "(1 + 2) @@ * @@ (3 - 1)", "'a' @@ + @@ (1 + 2)", "[1] @@ + @@ (2 + 3)"
  })
  void bothPathsAnswerIdentically(final String testCase) {
    final String[] parts = testCase.split(" @@ ", 3);
    final String operation = "(lv " + parts[1] + " rv)";
    final String literalOperation = "(" + parts[0] + " " + parts[1] + " " + parts[2] + ")";
    // Both operands are bound by UNWIND, so the only thing that differs between the two queries below is WHERE
    // against RETURN - and that is exactly what selects the path. A projection is evaluated by
    // ExpressionEvaluator; a WHERE predicate is evaluated by the AST node itself. See theTwoPathsAreDistinct().
    final String unwind = "UNWIND [" + parts[0] + "] AS lv UNWIND [" + parts[2] + "] AS rv ";

    final String viaEvaluator = outcome(unwind + "RETURN " + operation + " AS r");
    final String viaAst = outcome(unwind + "WITH lv, rv WHERE " + operation + " IS NOT NULL RETURN 1 AS r");
    // The third evaluator of the same semantics: ConstantFolder answers an operation over two literals at parse
    // time and replaces it with the result, so the query never evaluates it at all. A fold that disagrees with
    // execution rewrites the query into a different one. It rewrites predicates, not projections, which is why
    // the literal operation is hosted in a WHERE here and in the agreement query below.
    final String viaFolder = outcome("UNWIND [1] AS x WITH x WHERE " + literalOperation + " IS NOT NULL RETURN 1 AS r");

    // Failing identically matters as much as succeeding: half the operand matrix these issues established is
    // about WHICH error a bad operand produces, and a bare ClassCastException on one path where the other says
    // what is wrong with the query is precisely the drift issue #6344 found in the slice bounds. The folder is
    // held to the same standard by its own contract: it keeps the expression unfolded when the operation fails,
    // so the failure still arrives from execution, unchanged.
    if (viaEvaluator.startsWith("ERROR") || viaAst.startsWith("ERROR") || viaFolder.startsWith("ERROR")) {
      assertThat(viaAst).as("`%s` raises the same failure on the AST path", testCase).isEqualTo(viaEvaluator);
      assertThat(viaFolder).as("`%s` raises the same failure when folded", testCase).isEqualTo(viaEvaluator);
      return;
    }

    // Nothing failed, so compare the values - in a single query, so nothing is marshalled through Java and lost
    // on the way. `projected` is the evaluator, the repetitions inside WHERE are the AST node, `folded` is the
    // constant folder, and valueType() pins the type as well as the value: 3 and 3.0 are equal under Cypher '='
    // but must not be produced by one path where another produces the integer.
    final long agreements = count(unwind
        + "WITH lv, rv, " + operation + " AS projected "
        + "WHERE valueType(" + operation + ") = valueType(projected) "
        + "  AND valueType(" + literalOperation + ") = valueType(projected) "
        // NaN is not equal to itself, so the paths agreeing on it has to be said separately.
        + "  AND ((" + operation + " IS NULL AND projected IS NULL) OR " + operation + " = projected"
        + "       OR (valueType(projected) = 'FLOAT NOT NULL' AND isNaN(" + operation + ") AND isNaN(projected))) "
        + "  AND ((" + literalOperation + " IS NULL AND projected IS NULL) OR " + literalOperation + " = projected"
        + "       OR (valueType(projected) = 'FLOAT NOT NULL' AND isNaN(" + literalOperation + ") AND isNaN(projected))) "
        + "RETURN 1 AS r");
    assertThat(agreements).as("`%s` gives the same value and type on all three paths", testCase).isEqualTo(1L);
  }

  /**
   * The premise of the test above: the two query shapes really are two code paths. A projection is handed to
   * {@code ExpressionEvaluator}, a WHERE predicate is evaluated by the AST node - which is why the arithmetic
   * semantics were written twice in the first place. Without this the matrix could be one path compared with
   * itself and would prove nothing (the vacuous-test trap).
   * <p>
   * The evaluator path is pinned here by the shape that ONLY it can serve: an aggregate is pre-computed and
   * handed to the evaluator as an override, so arithmetic that reads one cannot be answered by the AST node,
   * which would re-evaluate the aggregate against a single representative row (issue #4100).
   */
  @Test
  void theTwoPathsAreDistinct() {
    assertThat(single("UNWIND [1, 2, 3] AS v RETURN sum(v) * 2 + 1 AS r")).isEqualTo(13L);
    assertThat(single("UNWIND [1, 2, 3] AS v RETURN 'total=' + sum(v) AS r")).isEqualTo("total=6");
    assertThat(single("UNWIND [1, 2, 3] AS v RETURN [0] + sum(v) AS r")).isEqualTo(List.of(0L, 6L));
    assertThat(single("UNWIND ['a', 'b'] AS v RETURN collect(v) + ['c'] AS r")).isEqualTo(List.of("a", "b", "c"));
    assertThat(single("UNWIND ['a', 'b'] AS v RETURN collect(v) || ['c'] AS r")).isEqualTo(List.of("a", "b", "c"));
    // And the predicate shape answers from the operands of the row it filters, which is the AST path.
    assertThat(count("UNWIND [1, 2, 3] AS v WITH v WHERE v * 2 > 4 RETURN v AS r")).isEqualTo(1L);
  }

  /** The shared entry point is callable with already-evaluated operands, which is the whole point of it. */
  @Test
  void theSharedEntryPointCarriesTheSemantics() {
    assertThat(ArithmeticExpression.apply(ArithmeticExpression.Operator.ADD, 1, 2)).isEqualTo(3L);
    assertThat(ArithmeticExpression.apply(ArithmeticExpression.Operator.ADD, null, 2)).isNull();
    assertThat(ArithmeticExpression.apply(ArithmeticExpression.Operator.ADD, 2, null)).isNull();
    assertThat(ArithmeticExpression.apply(ArithmeticExpression.Operator.ADD, "a", 1)).isEqualTo("a1");
    assertThat(ArithmeticExpression.apply(ArithmeticExpression.Operator.ADD, List.of(1L), 2L)).isEqualTo(List.of(1L, 2L));
    assertThat(ArithmeticExpression.apply(ArithmeticExpression.Operator.DIVIDE, 7, 2)).isEqualTo(3L);
    assertThat(ArithmeticExpression.apply(ArithmeticExpression.Operator.POWER, 2, 3)).isEqualTo(8.0);
  }

  /**
   * The outcome of a query as a comparable string: how many rows it produced, or the failure it raised, reduced
   * to the root cause so that the different wrappings the two paths pick up on the way out do not hide whether
   * they agree.
   */
  private String outcome(final String query) {
    try {
      return "ROWS " + count(query);
    } catch (final Throwable e) {
      Throwable root = e;
      while (root.getCause() != null)
        root = root.getCause();
      return "ERROR " + root.getClass().getName() + ": " + root.getMessage();
    }
  }

  private long count(final String query) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }
}
