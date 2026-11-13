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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.procedures.control.DoWhen;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapBudgetTestSupport;
import com.arcadedb.query.sql.executor.QueryHeapTracker;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Expression evaluation: chained comparisons, short-circuit AND/OR, parenthesized operands, map literals, binary values and heap limits.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherExpressionTest {
  /**
   * Regression tests for issue #5284: Cypher chained comparisons (e.g. {@code a < b < c}) must be
   * evaluated as the conjunction of adjacent comparisons ({@code a < b AND b < c}), matching the
   * OpenCypher specification and Neo4j behavior. Previously only the first comparison in the chain
   * was evaluated, silently yielding wrong results.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5284ChainedComparison extends TestHelper {

    private boolean scalarBoolean(final String cypher) {
      final ResultSet rs = database.query("opencypher", cypher);
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      return r.<Boolean>getProperty("result");
    }

    @Test
    void scalarChainedComparisons() {
      assertThat(scalarBoolean("RETURN 1 < 2 > 3 AS result")).isFalse();
      assertThat(scalarBoolean("RETURN 1 < 2 < 1 AS result")).isFalse();
      assertThat(scalarBoolean("RETURN 3 > 2 > 3 AS result")).isFalse();

      // fully-true chain still works
      assertThat(scalarBoolean("RETURN 1 < 2 < 3 AS result")).isTrue();

      // explicit AND controls
      assertThat(scalarBoolean("RETURN 1 < 2 AND 2 > 3 AS result")).isFalse();
      assertThat(scalarBoolean("RETURN 3 > 2 AND 2 > 3 AS result")).isFalse();
    }

    @Test
    void whereChainedComparison() {
      database.command("opencypher", "CREATE (:BugA {value: 15}), (:BugA {value: 100})");
      try {
        final ResultSet rs = database.query("opencypher",
            "MATCH (n:BugA) WHERE 10 < n.value < 20 RETURN n.value AS value ORDER BY value");
        final List<Long> values = new ArrayList<>();
        while (rs.hasNext())
          values.add(rs.next().<Number>getProperty("value").longValue());

        assertThat(values).containsExactly(15L);
      } finally {
        database.command("opencypher", "MATCH (n:BugA) DETACH DELETE n");
      }
    }
  }

  /**
   * Regression for issue #5384: a parenthesized function comparison in a projection, e.g.
   * {@code (abs(n.val) = 10)}, returned the raw function value instead of the BOOLEAN
   * result of the comparison.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5384ParenthesizedFunctionComparison {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/testIssue5384");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.command("opencypher", "CREATE (:Node {id:1, val:10})");
      database.command("opencypher", "CREATE (:Node {id:2, val:-10})");
      database.command("opencypher", "CREATE (:Node {id:3, val:5})");
      database.command("opencypher", "CREATE (:Node {id:4})");
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void parenthesizedAbsComparison() {
      assertThat(project("MATCH (n:Node) RETURN n.id AS id, (abs(n.val) = 10) AS r ORDER BY id"))
          .containsExactly(true, true, false, null);
    }

    @Test
    void unparenthesizedAbsComparisonControl() {
      assertThat(project("MATCH (n:Node) RETURN n.id AS id, abs(n.val) = 10 AS r ORDER BY id"))
          .containsExactly(true, true, false, null);
    }

    @Test
    void parenthesizedCoalesceComparison() {
      assertThat(project("MATCH (n:Node) RETURN n.id AS id, (coalesce(n.val, 0) = 10) AS r ORDER BY id"))
          .containsExactly(true, false, false, false);
    }

    @Test
    void parenthesizedToStringComparison() {
      assertThat(project("MATCH (n:Node) RETURN n.id AS id, (toString(n.val) = '10') AS r ORDER BY id"))
          .containsExactly(true, false, false, null);
    }

    @Test
    void parenthesizedToIntegerComparison() {
      assertThat(project("MATCH (n:Node) RETURN n.id AS id, (toInteger(n.val) = 10) AS r ORDER BY id"))
          .containsExactly(true, false, false, null);
    }

    @Test
    void parenthesizedToFloatGreaterThan() {
      assertThat(project("MATCH (n:Node) RETURN n.id AS id, (toFloat(n.val) > 0.0) AS r ORDER BY id"))
          .containsExactly(true, false, true, null);
    }

    @Test
    void parenthesizedCoalesceGreaterThan() {
      assertThat(project("MATCH (n:Node) RETURN n.id AS id, (coalesce(n.val, 0) > 0) AS r ORDER BY id"))
          .containsExactly(true, false, true, false);
    }

    @Test
    void parenthesizedComparisonInWithProjection() {
      assertThat(project("MATCH (n:Node) WITH n.id AS id, (abs(n.val) = 10) AS r RETURN id, r ORDER BY id"))
          .containsExactly(true, true, false, null);
    }

    private List<Object> project(final String cypher) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          values.add(r.getProperty("r"));
        }
      }
      return values;
    }
  }

  /**
   * Regression tests for issue #5835: {@code AND} and {@code OR} evaluated their result-irrelevant
   * operand and surfaced its runtime error, even though the boolean result was already determined
   * by the other operand (e.g. {@code false AND E} or {@code true OR E}). An equivalent unreachable
   * expression inside {@code CASE} was correctly skipped, which made the same runtime expression
   * behave differently depending only on which construct wrapped it.
   * <p>
   * {@code AND}/{@code OR} are built with two independent AST node types depending on where they
   * appear: {@code TernaryLogicalExpression} for {@code RETURN}/{@code WITH} projections and
   * {@code LogicalExpression} for {@code WHERE} predicates. Both need the short-circuit fix.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5835AndOrShortCircuit extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:_Oracle {_id:'n1'}), (:_Oracle {_id:'n2'})");
    }

    // --- RETURN/WITH projections (TernaryLogicalExpression) ---

    @Test
    void andWithFalseLeftShortCircuitsStaticRightError() {
      assertThat(scalar("RETURN false AND (left('abc', -1) = 0) AS x")).isEqualTo(false);
    }

    @Test
    void orWithTrueLeftShortCircuitsStaticRightError() {
      assertThat(scalar("RETURN true OR (left('abc', -1) = 0) AS x")).isEqualTo(true);
    }

    @Test
    void andWithFalseLeftShortCircuitsRuntimeBoundRightError() {
      assertThat(scalar("WITH 2 AS k RETURN false AND (left('abc', 0-k) = 0) AS x")).isEqualTo(false);
    }

    @Test
    void andWithTrueLeftStillEvaluatesRightAndThrows() {
      // Control: the right operand's value is required to determine the result, so it must
      // still be evaluated and its error must still surface.
      assertThatThrownBy(() -> scalar("RETURN true AND (left('abc', -1) = 0) AS x"))
          .isInstanceOf(CommandSemanticException.class);
    }

    @Test
    void orWithFalseLeftStillEvaluatesRightAndThrows() {
      // Control: same reasoning for OR when the left operand is false.
      assertThatThrownBy(() -> scalar("RETURN false OR (left('abc', -1) = 0) AS x"))
          .isInstanceOf(CommandSemanticException.class);
    }

    @Test
    void unselectedCaseBranchStillSkipsRuntimeBoundError() {
      // Control: CASE already behaved correctly; must remain unaffected by the AND/OR fix.
      assertThat(scalar("""
          WITH 2 AS k \
          RETURN CASE \
            WHEN false THEN left('abc', 0-k) \
            ELSE 1 \
          END AS x""")).isEqualTo(1L);
    }

    // --- Three-valued (null) logic must be unaffected by the short-circuit fix ---

    @Test
    void nullAndFalseIsFalse() {
      assertThat(scalar("RETURN (null AND false) AS x")).isEqualTo(false);
    }

    @Test
    void trueAndNullIsNull() {
      assertThat(scalar("RETURN (true AND null) AS x")).isNull();
    }

    @Test
    void nullOrTrueIsTrue() {
      assertThat(scalar("RETURN (null OR true) AS x")).isEqualTo(true);
    }

    @Test
    void falseOrNullIsNull() {
      assertThat(scalar("RETURN (false OR null) AS x")).isNull();
    }

    // --- WHERE predicates (LogicalExpression) ---

    @Test
    void whereFalseAndShortCircuitsStaticRightError() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:_Oracle) WHERE false AND (left('abc', -1) = 0) RETURN n._id AS id")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void whereFalseAndShortCircuitsRuntimeBoundRightError() {
      // Left operand is a runtime-evaluated comparison (not a literal), so it cannot be caught
      // by any compile-time boolean-literal simplification: this exercises LogicalExpression's
      // own short-circuit, not just a rewrite pass.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:_Oracle) WHERE (n._id = 'does-not-exist') AND (left('abc', -1) = 0) RETURN n._id AS id")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void whereTrueOrShortCircuitsRuntimeBoundRightError() {
      // A WITH-bound (non-literal) true flag on the left of OR: cannot be caught by any
      // compile-time boolean-literal simplification either.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:_Oracle) WHERE n._id = 'n1' WITH n, true AS flag " +
              "WHERE flag OR (left('abc', -1) = 0) RETURN n._id AS id")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("id")).isEqualTo("n1");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void whereTrueOrShortCircuitsStaticRightError() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (n:_Oracle) WHERE true OR (left('abc', -1) = 0) RETURN n._id AS id ORDER BY id")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("id")).isEqualTo("n1");
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("id")).isEqualTo("n2");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void whereTrueAndStillEvaluatesRightAndThrows() {
      // Control: the right operand's value is required, so its error must still surface.
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("opencypher",
            "MATCH (n:_Oracle) WHERE true AND (left('abc', -1) = 0) RETURN n")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(CommandSemanticException.class);
    }

    private Object scalar(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        assertThat(rs.hasNext()).as("query returned no rows: %s", cypher).isTrue();
        final Result r = rs.next();
        return r.getProperty("x");
      }
    }
  }

  /**
   * Regression tests for issue #8098: a {@code BINARY} property ({@code byte[]}) is one opaque value to
   * {@code UNWIND} (issue #7923) but was exploded into individual bytes by list comprehension,
   * {@code reduce()}, {@code allReduce()}, {@code all}/{@code any}/{@code none}/{@code single}, {@code IN}
   * and bracket indexing/slicing - so the same property read two different ways inside one query
   * disagreed on whether it was a list. Every one of those clauses now applies the same rule UNWIND
   * already does ({@code MultiValue.isSequenceArray}): a {@code byte[]} is never a sequence, so treating
   * it as one raises the same "not iterable" error a plain scalar would.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8098BinaryOpaqueEverywhere extends TestHelper {
    @Override
    protected void beginTest() {
      database.begin();
      database.getSchema().createVertexType("Blob");
      database.newVertex("Blob").set("name", "b1").set("data", new byte[] { 1, 2, 3 }).save();
      database.commit();
    }

    @Test
    void unwindKeepsTreatingItAsOneOpaqueValue() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (b:Blob) UNWIND [b.data] AS d RETURN d")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Object>getProperty("d")).isInstanceOf(byte[].class);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void listComprehensionRejectsIt() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN [x IN b.data | x]").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("iterable");
    }

    @Test
    void reduceRejectsIt() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN reduce(s = 0, x IN b.data | s + x)").next())
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void allPredicateRejectsIt() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN all(x IN b.data WHERE x > 0)").next())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("iterable");
    }

    @Test
    void inRejectsIt() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN 1 IN b.data").next())
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void indexingRejectsIt() {
      // a type error, the caller's: Neo.ClientError.Statement.TypeError (issue #11211)
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN b.data[0]").next())
          .isInstanceOf(TypeMismatchException.class);
    }

    @Test
    void slicingRejectsIt() {
      assertThatThrownBy(() -> database.query("opencypher", "MATCH (b:Blob) RETURN b.data[0..1]").next())
          .isInstanceOf(IllegalArgumentException.class);
    }

    /** Control: an ordinary numeric array is unaffected and still iterates element by element. */
    @Test
    void ordinaryArrayStillExplodesInListComprehension() {
      database.begin();
      database.newVertex("Blob").set("name", "v1").set("data", new int[] { 1, 2, 3 }).save();
      database.commit();

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (b:Blob {name:'v1'}) RETURN [x IN b.data | x * 2] AS doubled")) {
        assertThat(rs.hasNext()).isTrue();
        final List<?> doubled = rs.next().getProperty("doubled");
        assertThat(doubled).hasSize(3);
        assertThat(doubled.stream().map(v -> ((Number) v).intValue())).containsExactly(2, 4, 6);
      }
    }
  }

  /**
   * Regression tests for #9542: {@code WHERE (flag)} with a Boolean variable was parsed as the single-node pattern
   * {@code (flag)} and silently filtered every row, while {@code WHERE flag} kept them. A parenthesized variable is a
   * pattern only if it names a graph entity; one bound to a value is just a parenthesized expression.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9542ParenthesizedBooleanVariable extends TestHelper {

    private List<Object> column(final String query, final String column) {
      final List<Object> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          out.add(r.getProperty(column));
        }
      }
      return out;
    }

    @Test
    void parenthesizedBooleanVariableAfterWith() {
      assertThat(column("WITH true AS flag WHERE (flag) RETURN flag", "flag")).containsExactly(true);
      assertThat(column("WITH false AS flag WHERE (flag) RETURN flag", "flag")).isEmpty();
      assertThat(column("WITH true AS flag WHERE flag RETURN flag", "flag")).containsExactly(true);
    }

    @Test
    void parenthesizedComputedBooleanVariable() {
      assertThat(column("WITH 3.14 AS lhs, 1 AS rhs WITH lhs, rhs, lhs > rhs AS result WHERE (result) RETURN lhs", "lhs"))
          .containsExactly(3.14);
      assertThat(column("WITH 0.5 AS lhs, 1 AS rhs WITH lhs, rhs, lhs > rhs AS result WHERE (result) RETURN lhs", "lhs"))
          .isEmpty();
    }

    @Test
    void parenthesizedVariableInsideLogicalOperators() {
      assertThat(column("WITH true AS a, false AS b WHERE (a) AND NOT (b) RETURN a", "a")).containsExactly(true);
      assertThat(column("WITH true AS a, false AS b WHERE (b) OR (a) RETURN a", "a")).containsExactly(true);
      assertThat(column("WITH true AS a, false AS b WHERE (a) AND (b) RETURN a", "a")).isEmpty();
      assertThat(column("WITH true AS a WHERE NOT (a) RETURN a", "a")).isEmpty();
      assertThat(column("WITH false AS a WHERE NOT (a) RETURN a", "a")).containsExactly(false);
    }

    @Test
    void parenthesizedNullVariableIsUnknown() {
      assertThat(column("WITH null AS a WHERE (a) RETURN 1 AS x", "x")).isEmpty();
      // NOT of unknown is unknown, so the row is filtered
      assertThat(column("WITH null AS a WHERE NOT (a) RETURN 1 AS x", "x")).isEmpty();
      assertThat(column("WITH null AS a WHERE (a) OR true RETURN 1 AS x", "x")).containsExactly(1L);
    }

    @Test
    void parenthesizedBooleanVariableInMatchWhere() {
      database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1, ok: true}), (:N {id: 2, ok: false})"));
      assertThat(column("MATCH (n:N) WITH n, n.ok AS ok WHERE (ok) RETURN n.id AS id", "id")).containsExactly(1);
      assertThat(column("MATCH (n:N) WITH n, n.ok AS ok WHERE NOT (ok) RETURN n.id AS id", "id")).containsExactly(2);
      assertThat(column("UNWIND [true, false] AS ok MATCH (n:N) WHERE (ok) AND n.id = 1 RETURN n.id AS id", "id"))
          .containsExactly(1);
    }

    @Test
    void parenthesizedNodeVariableIsStillRefused() {
      database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})-[:R]->(:N {id: 2})"));
      assertThatThrownBy(() -> column("MATCH (n:N) WHERE (n) RETURN n.id AS id", "id")).isInstanceOf(CommandParsingException.class);
      assertThatThrownBy(() -> column("MATCH (n:N) WITH n WHERE (n) RETURN n.id AS id", "id"))
          .isInstanceOf(CommandParsingException.class);
      assertThatThrownBy(() -> column("MATCH (n:N)-[r:R]->() WHERE (r) RETURN n.id AS id", "id"))
          .isInstanceOf(CommandParsingException.class);
      assertThatThrownBy(() -> column("MATCH p = (n:N)-[:R]->() WHERE (p) RETURN n.id AS id", "id"))
          .isInstanceOf(CommandParsingException.class);
    }

    @Test
    void parenthesizedUndefinedVariableIsRefused() {
      assertThatThrownBy(() -> column("MATCH (m) WHERE (nope) RETURN m", "m")).isInstanceOf(CommandSemanticException.class);
    }

    @Test
    void parenthesizedNonBooleanValueIsATypeError() {
      // a number is not a Boolean: it is reported, not silently turned into "no row" by the single-node pattern reading
      assertThatThrownBy(() -> column("WITH 1 AS x WHERE (x) RETURN x", "x")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("InvalidArgumentType");
    }

    @Test
    void labelledOrFilteredSingleNodeIsNeverReadAsABooleanVariable() {
      database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})"));
      // (n:N) is a label predicate, not a variable: it answers about the node
      assertThat(column("MATCH (n:N) WHERE (n:N) RETURN n.id AS id", "id")).containsExactly(1);
      assertThat(column("MATCH (n:N) WHERE (n:Other) RETURN n.id AS id", "id")).isEmpty();
    }

    @Test
    void parenthesizedNodeNotProvableAtValidationFailsWhenARowReachesIt() {
      database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})"));
      assertThatThrownBy(() -> column("MATCH (n:N) WITH collect(n) AS ns UNWIND ns AS x WITH x WHERE (x) RETURN 1 AS v", "v"))
          .hasMessageContaining("InvalidArgumentType");
    }

    @Test
    void parenthesizedBacktickedBooleanVariable() {
      assertThat(column("WITH true AS `my flag` WHERE (`my flag`) RETURN 1 AS x", "x")).containsExactly(1L);
      assertThat(column("WITH false AS `flag` WHERE (`flag`) RETURN 1 AS x", "x")).isEmpty();
      // a doubled backtick is one backtick of the name
      assertThat(column("WITH true AS `a``b` WHERE (`a``b`) RETURN 1 AS x", "x")).containsExactly(1L);
    }

    @Test
    void parenthesizedEntityVariableOverNoRowsIsNotAnError() {
      // refused when the kind is known statically, and otherwise only when a row reaches the predicate
      database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})"));
      try {
        assertThat(column("MATCH (n:N) WHERE n.id = 99 WITH n AS x WHERE (x) RETURN 1 AS v", "v")).isEmpty();
      } catch (final CommandParsingException e) {
        assertThat(e.getMessage()).contains("InvalidArgumentType");
      }
      assertThat(column("UNWIND [] AS x WITH x WHERE (x) RETURN 1 AS v", "v")).isEmpty();
    }
  }

  /**
   * Issue #10160: the openCypher expressions that build a list out of a sub-plan or a traversal - {@code COLLECT { ... }}, a pattern
   * comprehension - and the {@code apoc.do.when} procedure drained it into a list of their own, past the per-operation element cap
   * and the query heap budget the identical SQL subquery is refused by since #10080. Each now builds its list through
   * {@code OperationHeapLimit}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10160CypherExpressionHeapLimit extends TestHelper {
    private static final int  OUTER_ROWS = 100;
    private static final int  INNER_ROWS = 4_000;
    private static final long CAP        = 100L;

    /** Every statement holds all of InnerT (or every neighbour of the hub) in the list of one expression. */
    private static final List<String> OVER_CAP = List.of(//
        "MATCH (o:OuterT) WHERE o.k = 0 RETURN size(COLLECT { MATCH (m:InnerT) RETURN m.pad }) AS s",//
        "MATCH (o:OuterT) WHERE o.k = 0 RETURN size(COLLECT { MATCH (m:InnerT) RETURN m.k, m.pad }) AS s",//
        "MATCH (h:Hub) RETURN size([(h)-->(l:InnerT) | l.pad]) AS s",//
        "MATCH (h:Hub) RETURN size([(h)-[:Links*1..1]->(l) WHERE l.k >= 0 | l.pad]) AS s",//
        "CALL apoc.do.when(true, 'MATCH (m:InnerT) RETURN m.pad AS p', '') YIELD value RETURN count(value) AS s",//
        // OPTIONAL suppresses "no rows", not a refusal of the heap limits: answering a null row would read as an empty result
        "OPTIONAL CALL apoc.do.when(true, 'MATCH (m:InnerT) RETURN m.pad AS p', '') YIELD value RETURN count(value) AS s");

    @AfterEach
    void resetSettings() {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    }

    @Test
    void everyListBuildingExpressionFailsPastTheElementCap() {
      createData();
      setCap(CAP);
      for (final String cypher : OVER_CAP)
        assertThatThrownBy(() -> drain(cypher)).as(cypher)
            .isInstanceOf(HeapLimitExceededException.class)
            .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    }

    @Test
    void theSameStatementsRunBelowTheCap() {
      createData();
      setCap(100_000L);
      for (final String cypher : OVER_CAP)
        assertThat(single(cypher).<Number>getProperty("s").intValue()).as(cypher).isEqualTo(INNER_ROWS);
    }

    @Test
    void theFailureNamesTheConstruct() {
      createData();
      setCap(CAP);
      assertThatThrownBy(() -> drain(OVER_CAP.get(0))).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("COLLECT subquery");
      assertThatThrownBy(() -> drain(OVER_CAP.get(2))).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("pattern comprehension");
      assertThatThrownBy(() -> drain(OVER_CAP.get(4))).isInstanceOf(HeapLimitExceededException.class).hasMessageContaining("do.when()");
    }

    @Test
    void aCollectIsChargedToTheSharedBudgetAndReleased() {
      createData();
      setCap(-1L);
      // The verdict compares the query with the whole budget, so nothing else may hold a share of it here: a query another test left
      // holding it turns "more than the whole budget" into "the running queries hold too much"
      QueryHeapBudgetTestSupport.awaitNoReservationLeft();
      final long baseline = QueryHeapBudget.getReservedBytes();

      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
      for (final String cypher : OVER_CAP)
        // MORE THAN THE WHOLE BUDGET, WHICH NO RETRY CAN HELP: A LIMIT OF THE HEAP, AS THE ELEMENT CAP IS
        assertThatThrownBy(() -> drain(cypher)).as(cypher).isInstanceOf(HeapLimitExceededException.class)
            .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

      // with room, the same statements run, and give everything back
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
      for (final String cypher : List.of(OVER_CAP.get(0), OVER_CAP.get(2)))
        assertThat(single(cypher).<Number>getProperty("s").intValue()).as(cypher).isEqualTo(INNER_ROWS);
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
    }

    /** A list given back when it is returned: one small COLLECT per outer row never adds up to the whole of them. */
    @Test
    void aCollectEvaluatedPerRowDoesNotAccumulateItsCharge() {
      createData();
      setCap(-1L);
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);

      assertThat(drain("MATCH (o:OuterT) RETURN size(COLLECT { MATCH (m:InnerT) WHERE m.k = o.k RETURN m.pad }) AS s"))
          .isEqualTo(OUTER_ROWS);
      assertThat(drain("MATCH (o:OuterT) RETURN size([(o)-->(l) | l.pad]) AS s")).isEqualTo(OUTER_ROWS);
    }

    /**
     * The rows of a procedure outlive its call: a chained CALL holds the stream of every input row of a batch before it reads them.
     * So apoc.do.when keeps its rows charged to the query until the stream is closed, rather than releasing them as an expression does.
     */
    @Test
    void apocDoWhenKeepsItsRowsChargedUntilItsStreamIsClosed() {
      createData();
      setCap(-1L);
      database.transaction(() -> {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        final Stream<Result> rows = new DoWhen().execute(new Object[] { true, "MATCH (m:InnerT) RETURN m.pad AS p", "" }, null,
            context);
        final QueryHeapTracker tracker = context.getQueryHeapTracker();
        assertThat(tracker.getUsedBytes()).as("charged while the stream is open").isGreaterThan(0L);

        rows.close();
        assertThat(tracker.getUsedBytes()).as("given back when it is closed").isZero();
      });
    }

    /** A stream CALL abandons - a LIMIT stops reading it, a later step fails - still gives its rows back to the shared budget. */
    @Test
    void apocDoWhenGivesItsRowsBackWhenItsStreamIsAbandoned() {
      createData();
      setCap(-1L);
      final long baseline = QueryHeapBudget.getReservedBytes();
      final String call = "UNWIND range(1, 3) AS i CALL apoc.do.when(true, 'MATCH (m:InnerT) RETURN m.pad AS p', '') YIELD value ";

      assertThat(drain(call + "RETURN value LIMIT 1")).isEqualTo(1);
      assertThat(QueryHeapBudget.getReservedBytes()).as("after a LIMIT").isLessThanOrEqualTo(baseline);

      assertThatThrownBy(() -> drain(call + "RETURN 1 / (i - i) AS boom")).isInstanceOf(ArithmeticErrorException.class);
      assertThat(QueryHeapBudget.getReservedBytes()).as("after a later step failed").isLessThanOrEqualTo(baseline);
    }

    @Test
    void theValuesAreTheOnesTheBodyProjects() {
      createData();
      setCap(100_000L);
      final Result row = single("MATCH (o:OuterT) WHERE o.k = 3 RETURN COLLECT { MATCH (m:InnerT) WHERE m.k < 2 RETURN m.k ORDER BY m.k } AS one,"
          + " COLLECT { MATCH (m:InnerT) WHERE m.k < 2 RETURN m.k, m.k * 10 ORDER BY m.k } AS two");
      assertThat(row.<List<Object>>getProperty("one")).containsExactly(0, 1);
      assertThat(row.<List<Object>>getProperty("two")).containsExactly(List.of(0, 0L), List.of(1, 10L));
    }

    private void createData() {
      database.command("sql", "CREATE VERTEX TYPE OuterT");
      database.command("sql", "CREATE VERTEX TYPE InnerT");
      database.command("sql", "CREATE VERTEX TYPE Hub");
      database.command("sql", "CREATE VERTEX TYPE Leaf");
      database.command("sql", "CREATE EDGE TYPE Links");
      final String pad = "x".repeat(500);
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex("Hub").save();
        for (int i = 0; i < INNER_ROWS; i++) {
          final MutableVertex inner = database.newVertex("InnerT").set("k", i).set("pad", pad).save();
          hub.newEdge("Links", inner);
        }
        for (int i = 0; i < OUTER_ROWS; i++) {
          final MutableVertex outer = database.newVertex("OuterT").set("k", i).save();
          outer.newEdge("Links", database.newVertex("Leaf").set("pad", "y").save());
        }
      });
    }

    private void setCap(final long cap) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, cap);
    }

    private int drain(final String cypher) {
      final int[] rows = new int[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", cypher)) {
          while (rs.hasNext()) {
            rs.next();
            ++rows[0];
          }
        }
      });
      return rows[0];
    }

    private Result single(final String cypher) {
      final Result[] result = new Result[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", cypher)) {
          result[0] = rs.next();
          assertThat(rs.hasNext()).isFalse();
        }
      });
      return result[0];
    }
  }

  /**
   * Regression tests for #11112: a map literal whose value was a map projection collapsed to that projection, losing the
   * literal's own keys and every other entry ({@code { a: n { .k }, b: 1 }} answered {@code {k: 1}}). The expected values are
   * Neo4j 5.26's.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11112MapLiteralWithMapProjection extends TestHelper {

    @BeforeEach
    void createGraph() {
      database.transaction(() -> database.command("opencypher", "CREATE (a:Qa {k: 1})<-[:QR {y: 1}]-(:Qb {t: 'x'})"));
    }

    @Test
    void aMapLiteralKeepsItsKeyAroundAProjection() {
      assertThat(single("MATCH (n:Qa) RETURN { a: n { .k } } AS m")).isEqualTo(Map.of("a", Map.of("k", 1)));
    }

    @Test
    void aMapLiteralKeepsItsOtherEntries() {
      assertThat(single("MATCH (n:Qa) RETURN { a: n { .k }, b: 1 } AS m")).isEqualTo(Map.of("a", Map.of("k", 1), "b", 1L));
      assertThat(single("MATCH (n:Qa) RETURN { b: 1, a: n { .k } } AS m")).isEqualTo(Map.of("a", Map.of("k", 1), "b", 1L));
    }

    @Test
    void aProjectionInsideAMapLiteralInsideAProjection() {
      assertThat(single("MATCH (n:Qa) RETURN n { c: { e: n { .k } } } AS m")).isEqualTo(Map.of("c", Map.of("e", Map.of("k", 1))));
    }

    @Test
    void aConnectionShapeWithAPatternComprehension() {
      assertThat(single("MATCH (n:Qa) RETURN n { c: { edges: [(n)<-[e0:QR]-(n0:Qb) | { node: n0 { .t }, p: e0 { .y } }] } } AS m"))
          .isEqualTo(Map.of("c", Map.of("edges", List.of(Map.of("node", Map.of("t", "x"), "p", Map.of("y", 1))))));
    }

    @Test
    void aListOfProjectionsInsideAMapLiteral() {
      assertThat(single("MATCH (n:Qa) RETURN { list: [n { .k }, n { .k }] } AS m"))
          .isEqualTo(Map.of("list", List.of(Map.of("k", 1), Map.of("k", 1))));
    }

    @Test
    void aProjectionWithAComputedFieldInsideAMapLiteral() {
      assertThat(single("MATCH (n:Qa) RETURN { a: n { .k, doubled: n.k * 2, s: toString(n.k) } } AS m"))
          .isEqualTo(Map.of("a", Map.of("k", 1, "doubled", 2L, "s", "1")));
    }

    @Test
    void aMapLiteralInWithAndInAFunctionArgument() {
      assertThat(single("MATCH (n:Qa) WITH { a: n { .k }, b: 1 } AS m RETURN m")).isEqualTo(Map.of("a", Map.of("k", 1), "b", 1L));
      final Result row = row("MATCH (n:Qa) RETURN keys({ a: n { .k }, b: 1 }) AS m");
      assertThat(row.<List<String>>getProperty("m")).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void aProjectionNextToAFunctionCallAListAndAPropertyAccess() {
      assertThat(single("MATCH (n:Qa) RETURN { s: toString(n.k), l: [1, 2], a: n { .k } } AS m"))
          .isEqualTo(Map.of("s", "1", "l", List.of(1L, 2L), "a", Map.of("k", 1)));
      assertThat(single("MATCH (n:Qa) RETURN { a: n { .k } }.a AS m")).isEqualTo(Map.of("k", 1));
    }

    @Test
    void deeperNestingAndAListComprehension() {
      assertThat(single("MATCH (n:Qa) RETURN { l1: n { l2: { l3: n { .k, l4: { v: n.k } } } } } AS m"))
          .isEqualTo(Map.of("l1", Map.of("l2", Map.of("l3", Map.of("k", 1, "l4", Map.of("v", 1))))));
      assertThat(single("MATCH (n:Qa) RETURN { xs: [x IN [1, 2] | { i: x, p: n { .k } }] } AS m"))
          .isEqualTo(Map.of("xs", List.of(Map.of("i", 1L, "p", Map.of("k", 1)), Map.of("i", 2L, "p", Map.of("k", 1)))));
    }

    @Test
    void aProjectionThroughAVariableStillWorks() {
      assertThat(single("MATCH (n:Qa) WITH n { .k } AS p RETURN { a: p } AS m")).isEqualTo(Map.of("a", Map.of("k", 1)));
    }

    @Test
    void aTopLevelProjectionStillWorks() {
      assertThat(single("MATCH (n:Qa) RETURN n { .k, c: { e: 1 } } AS m")).isEqualTo(Map.of("k", 1, "c", Map.of("e", 1L)));
    }

    private Object single(final String query) {
      return row(query).getProperty("m");
    }

    private Result row(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).as(query).isTrue();
        final Result row = rs.next();
        assertThat(rs.hasNext()).as(query).isFalse();
        return row;
      }
    }
  }
}
