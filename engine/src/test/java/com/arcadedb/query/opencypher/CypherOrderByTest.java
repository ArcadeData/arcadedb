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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * ORDER BY over projected expressions, grouping keys, DISTINCT and binary values.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherOrderByTest {
  /**
   * Regression test for issue #5283: {@code ORDER BY} must be able to resolve an expression that is
   * itself one of the projected expressions of a {@code RETURN DISTINCT} / {@code WITH DISTINCT},
   * even when that projection carries an alias.
   * <p>
   * The reported query
   * <pre>
   *   MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active
   * </pre>
   * failed with {@code UndefinedVariable: Variable 'n' not defined}, while the equivalent
   * {@code ORDER BY active} worked.
   * <p>
   * A DISTINCT projection narrows the scope available to ORDER BY down to the projected columns, which
   * is why {@code ORDER BY <non-projected expression>} must keep failing (a dedup group has no single
   * well-defined value for it). But an ORDER BY expression that is <em>structurally identical</em> to a
   * projected expression is constant within every dedup group by construction, so it is well defined.
   * Neo4j, Memgraph and FalkorDB all accept it. Neo4j implements this by rewriting such an ORDER BY
   * expression to the projection's alias before the scope check; this test pins that same behaviour.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5283DistinctOrderByProjectedExpression {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5283");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.command("cypher", "CREATE (:BugA {active: true, age: 30, name: 'c'})");
        database.command("cypher", "CREATE (:BugA {active: false, age: 20, name: 'b'})");
        database.command("cypher", "CREATE (:BugA {active: true, age: 10, name: 'a'})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private List<Object> column(final String cypher, final String columnName) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("cypher", cypher)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty(columnName));
        }
      }
      return values;
    }

    /**
     * The exact query from the issue report. Must return the two distinct values ascending.
     */
    @Test
    void orderByAliasedProjectedExpression() {
      assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active", "active")) //
          .containsExactly(false, true);
    }

    /**
     * Control from the issue report: ordering by the alias already worked and must keep working.
     */
    @Test
    void orderByAliasStillWorks() {
      assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY active", "active")) //
          .containsExactly(false, true);
    }

    /**
     * The rewrite must honour the sort direction of the ORDER BY item, not the projection order.
     */
    @Test
    void orderByAliasedProjectedExpressionDescending() {
      assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.active DESC", "active")) //
          .containsExactly(true, false);
    }

    /**
     * Un-aliased projections resolve through the same path: the output column is the expression text.
     */
    @Test
    void orderByProjectedExpressionWithoutAlias() {
      assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.age ORDER BY n.age", "n.age")) //
          .containsExactly(10, 20, 30);
    }

    /**
     * Composite expressions must match structurally, so that whitespace differences between the
     * projection text and the ORDER BY text cannot cause a spurious UndefinedVariable.
     */
    @Test
    void orderByProjectedCompositeExpression() {
      // Cypher integer arithmetic widens to 64-bit, hence Long rather than Integer
      assertThat(column("MATCH (n:BugA) RETURN DISTINCT n.age + 1 AS bumped ORDER BY n.age+1", "bumped")) //
          .containsExactly(11L, 21L, 31L);
    }

    /**
     * Same rule applies to WITH DISTINCT, whose ORDER BY is subject to the same narrowed scope.
     */
    @Test
    void withDistinctOrderByProjectedExpression() {
      assertThat(column("MATCH (n:BugA) WITH DISTINCT n.age AS age ORDER BY n.age RETURN age", "age")) //
          .containsExactly(10, 20, 30);
    }

    /**
     * Neo4j-compatible negative case: {@code n.name} is not projected, so after DISTINCT collapses
     * {active:true} down to one row there is no well-defined {@code n.name} to sort on. This must stay
     * an error - relaxing the scope check must not become "let any pre-DISTINCT variable through".
     */
    @Test
    void orderByNonProjectedExpressionStillRejected() {
      assertThatThrownBy(() -> database.query("cypher", "MATCH (n:BugA) RETURN DISTINCT n.active AS active ORDER BY n.name").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("not defined");
    }

    /**
     * A projected expression only licenses that exact expression: sorting by a different property of
     * the same variable stays rejected.
     */
    @Test
    void orderByDifferentPropertyOfProjectedVariableStillRejected() {
      assertThatThrownBy(() -> database.query("cypher", "MATCH (n:BugA) RETURN DISTINCT n.age AS age ORDER BY n.active").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("not defined");
    }
  }

  /**
   * Regression test for issue #5286: ordering by a grouping-key expression that is projected under an
   * alias must sort, instead of being silently ignored.
   * <p>
   * <pre>
   *   MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age
   * </pre>
   * returned rows in storage order and raised no error. The semantic validator admits {@code n} into
   * the ORDER BY scope for an aggregating projection, but the aggregation step does not carry {@code n}
   * onto the output rows, so the sort key evaluated to null on every row, all rows compared equal and
   * the sort became a no-op. The aliased + aggregating combination was the only affected one: the
   * un-aliased form matched the output column by name and sorted correctly.
   * <p>
   * The grouping key has exactly one value per group, so this is well defined and Neo4j sorts it. The
   * ORDER BY expression is now resolved to the projected output column, as for DISTINCT (#5283).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5286AggregationOrderByGroupingKey {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5286");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      // inserted out of age order, so an unsorted result is distinguishable from a sorted one
      database.transaction(() -> {
        database.command("cypher", "CREATE (:P {active: true, age: 30, name: 'c'})");
        database.command("cypher", "CREATE (:P {active: false, age: 20, name: 'b'})");
        database.command("cypher", "CREATE (:P {active: true, age: 10, name: 'a'})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private List<Object> column(final String cypher, final String columnName) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("cypher", cypher)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty(columnName));
        }
      }
      return values;
    }

    /**
     * The reported query: the grouping key is aliased, and ORDER BY names the original expression.
     */
    @Test
    void orderByAliasedGroupingKey() {
      assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age", "a")) //
          .containsExactly(10, 20, 30);
    }

    @Test
    void orderByAliasedGroupingKeyDescending() {
      assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age DESC", "a")) //
          .containsExactly(30, 20, 10);
    }

    /**
     * Ordering by the alias always worked and must keep working.
     */
    @Test
    void orderByAliasStillWorks() {
      assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY a", "a")) //
          .containsExactly(10, 20, 30);
    }

    /**
     * The un-aliased form resolves by column name and must keep working.
     */
    @Test
    void orderByUnaliasedGroupingKeyStillWorks() {
      assertThat(column("MATCH (n:P) RETURN n.age, count(*) AS c ORDER BY n.age", "n.age")) //
          .containsExactly(10, 20, 30);
    }

    /**
     * A composite grouping key must match regardless of whitespace.
     */
    @Test
    void orderByAliasedCompositeGroupingKey() {
      assertThat(column("MATCH (n:P) RETURN n.age + 1 AS x, count(*) AS c ORDER BY n.age+1", "x")) //
          .containsExactly(11L, 21L, 31L);
    }

    /**
     * Ordering by a projected aggregate expression is well defined per group and must sort too.
     */
    @Test
    void orderByProjectedAggregate() {
      // count() is 64-bit, hence Long
      assertThat(column("MATCH (n:P) RETURN n.active AS a, count(*) AS c ORDER BY count(*)", "c")) //
          .containsExactly(1L, 2L);
      assertThat(column("MATCH (n:P) RETURN n.active AS a, count(*) AS c ORDER BY count(*) DESC", "c")) //
          .containsExactly(2L, 1L);
    }

    /**
     * The same rule applies to an aggregating WITH.
     */
    @Test
    void withAggregationOrderByAliasedGroupingKey() {
      assertThat(column("MATCH (n:P) WITH n.age AS a, count(*) AS c ORDER BY n.age RETURN a, c", "a")) //
          .containsExactly(10, 20, 30);
    }

    /**
     * Grouping by one key and ordering by another projected key must sort on the requested one.
     */
    @Test
    void orderByAliasedGroupingKeyAmongSeveral() {
      assertThat(column("MATCH (n:P) RETURN n.name AS nm, n.age AS a, count(*) AS c ORDER BY n.age", "nm")) //
          .containsExactly("a", "b", "c");
    }

    /**
     * An ORDER BY built out of a grouping key is well defined too: the grouping key is constant per
     * group, so the enclosing expression is. Neo4j sorts this; it is resolved by substituting the
     * projected part, i.e. sorting on {@code a + 1}.
     */
    @Test
    void orderByExpressionContainingGroupingKey() {
      assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age + 1", "a")) //
          .containsExactly(10, 20, 30);
      assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age + 1 DESC", "a")) //
          .containsExactly(30, 20, 10);
    }

    /**
     * The substitution reaches inside function-call arguments.
     */
    @Test
    void orderByFunctionOfGroupingKey() {
      assertThat(column("MATCH (n:P) RETURN n.name AS nm, count(*) AS c ORDER BY toUpper(n.name) DESC", "nm")) //
          .containsExactly("c", "b", "a");
    }

    /**
     * When the node itself is the grouping key, a property of it is well defined per group and must
     * sort: the read follows the variable to the column it was projected as.
     */
    @Test
    void orderByPropertyOfProjectedNode() {
      assertThat(column("MATCH (n:P) RETURN n AS node, count(*) AS c ORDER BY n.age", "c")) //
          .containsExactly(1L, 1L, 1L);
      final List<Object> ages = new ArrayList<>();
      try (final ResultSet rs = database.query("cypher", "MATCH (n:P) RETURN n AS node, count(*) AS c ORDER BY n.age")) {
        while (rs.hasNext())
          ages.add(((Document) rs.next().getProperty("node")).get("age"));
      }
      assertThat(ages).containsExactly(10, 20, 30);
    }

    /**
     * A reference the projection dropped has no value to sort on. Reporting it is what Neo4j does, and
     * it is the point of the fix: the query used to return unsorted rows and say nothing.
     */
    @Test
    void orderByNonGroupingKeyRejected() {
      assertThatThrownBy(() -> database.query("cypher", "MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.name").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("not defined");
    }

    /**
     * An ORDER BY item that mixes an aggregation with a complex non-aggregated part is ambiguous by
     * specification, even when that part is projected (openCypher ReturnOrderBy6 [5]). Resolving the
     * part to its output column would make the query compile, so the substitution must leave items
     * containing an aggregation alone. Covered by the TCK; pinned here because it is the one shape the
     * rewrite must deliberately not touch.
     */
    @Test
    void orderByComplexExpressionWithAggregationStaysAmbiguous() {
      assertThatThrownBy(() -> database
          .query("cypher", "MATCH (n:P) RETURN n.age + 1, count(*) AS c ORDER BY n.age + 1 + count(*)").close())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("AmbiguousAggregationExpression");
    }

    /**
     * By contrast a simple projected property read inside an aggregation-containing ORDER BY item is
     * legal and must keep compiling (openCypher ReturnOrderBy6 [3]).
     */
    @Test
    void orderBySimplePropertyWithAggregationStillCompiles() {
      assertThat(column("MATCH (n:P) RETURN n.age AS a, count(*) AS c ORDER BY n.age + count(*)", "a")) //
          .containsExactlyInAnyOrder(10, 20, 30);
    }
  }

  /**
   * Regression test for issue #5287: {@code WITH DISTINCT} must narrow the scope available to its
   * {@code ORDER BY} to the projected columns, exactly as {@code RETURN DISTINCT} already does.
   * <p>
   * {@code WITH DISTINCT n.active AS active ORDER BY n.name} used to be accepted. DISTINCT collapses
   * the two {@code active: true} rows into one, and that survivor keeps the {@code n} of whichever
   * duplicate arrived first, so the sort key came from an arbitrary group member and the result
   * depended on storage order. The equivalent RETURN DISTINCT form correctly raised an error; the two
   * clauses now agree, and both match Neo4j.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5287WithDistinctOrderByScope {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5287");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.command("cypher", "CREATE (:P {active: true, age: 30, name: 'c'})");
        database.command("cypher", "CREATE (:P {active: false, age: 20, name: 'b'})");
        database.command("cypher", "CREATE (:P {active: true, age: 10, name: 'a'})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private List<Object> column(final String cypher, final String columnName) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("cypher", cypher)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty(columnName));
        }
      }
      return values;
    }

    /**
     * The reported case: {@code n.name} survives DISTINCT only as an arbitrary group representative,
     * so ordering by it has no deterministic result and must be rejected.
     */
    @Test
    void withDistinctOrderByNonProjectedRejected() {
      assertThatThrownBy(
          () -> database.query("cypher", "MATCH (n:P) WITH DISTINCT n.active AS active ORDER BY n.name RETURN active").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("not defined");
    }

    /**
     * WITH DISTINCT must now agree with the RETURN DISTINCT form, which already rejected this.
     */
    @Test
    void withDistinctAndReturnDistinctAgree() {
      assertThatThrownBy(() -> database.query("cypher", "MATCH (n:P) WITH DISTINCT n.name AS name ORDER BY n.age RETURN name").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("not defined");
      assertThatThrownBy(() -> database.query("cypher", "MATCH (n:P) RETURN DISTINCT n.name AS name ORDER BY n.age").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("not defined");
    }

    /**
     * Ordering by a projected expression stays well defined and must keep working (issue #5283).
     */
    @Test
    void withDistinctOrderByProjectedExpressionStillWorks() {
      assertThat(column("MATCH (n:P) WITH DISTINCT n.age AS age ORDER BY n.age RETURN age", "age")) //
          .containsExactly(10, 20, 30);
    }

    /**
     * Ordering by the alias stays well defined and must keep working.
     */
    @Test
    void withDistinctOrderByAliasStillWorks() {
      assertThat(column("MATCH (n:P) WITH DISTINCT n.age AS age ORDER BY age DESC RETURN age", "age")) //
          .containsExactly(30, 20, 10);
    }

    /**
     * An un-aliased projection is named after its own expression text, so ordering by that text
     * resolves against the projected column and the narrowing must not mistake it for a pre-DISTINCT
     * reference. (The downstream clause cannot name {@code n} here, since an un-aliased WITH does not
     * carry it forward - hence the constant projection.)
     */
    @Test
    void withDistinctOrderByUnaliasedProjectionNotRejected() {
      // one row per distinct age; Cypher integer literals are 64-bit, hence Long
      assertThat(column("MATCH (n:P) WITH DISTINCT n.age ORDER BY n.age RETURN 1 AS one", "one")) //
          .containsExactly(1L, 1L, 1L);
    }

    /**
     * A non-DISTINCT WITH keeps the full incoming scope, so ordering by a non-projected expression
     * remains legal there: the narrowing must not leak onto plain projections.
     */
    @Test
    void nonDistinctWithKeepsFullScope() {
      assertThat(column("MATCH (n:P) WITH n.active AS active ORDER BY n.name RETURN active", "active")) //
          .containsExactly(true, false, true);
    }
  }

  /**
   * Regression test for issue #7426: the {@code ORDER BY} of a plain (neither {@code DISTINCT} nor
   * aggregating) {@code RETURN} must resolve its variables against the scope that reaches the
   * {@code RETURN}, plus the columns that {@code RETURN} projects.
   * <p>
   * {@code MATCH (f:Function) WITH f.Name AS BusinessFunction RETURN BusinessFunction ORDER BY f.Name}
   * used to be accepted and to return the rows <i>unsorted</i>: the projecting {@code WITH} drops
   * {@code f}, the sort key resolved to nothing, and the client got wrong results with no error. The
   * collapsing forms of the same clause ({@code RETURN DISTINCT}, an aggregating {@code RETURN}) and
   * every form of {@code WITH} already reported it; the plain {@code RETURN} was the one shape left
   * without the check.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue7426PlainReturnOrderByScope {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue7426");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.command("cypher", "CREATE (:Function {Name: 'Zeta'})");
        database.command("cypher", "CREATE (:Function {Name: 'Alpha'})");
        database.command("cypher", "CREATE (:Function {Name: 'Mid'})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private List<Object> column(final String cypher, final String columnName) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("cypher", cypher)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          values.add(row.getProperty(columnName));
        }
      }
      return values;
    }

    /**
     * The reported query: the projecting WITH drops {@code f}, so the trailing ORDER BY has no {@code f}
     * to sort on and must be reported rather than silently ignored.
     */
    @Test
    void plainReturnOrderByOnVariableDroppedByWithIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN BusinessFunction
          ORDER BY f.Name""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * Same shape with the whole variable rather than a property of it.
     */
    @Test
    void plainReturnOrderByOnBareDroppedVariableIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN BusinessFunction
          ORDER BY f""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * A variable that no clause ever bound reaches the same check: before the fix this too sorted on
     * nothing instead of failing.
     */
    @Test
    void plainReturnOrderByOnNeverBoundVariableIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          RETURN f.Name AS BusinessFunction
          ORDER BY ghost.Name""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'ghost'");
    }

    /**
     * The dropped variable buried inside a function call is the same violation and must be found by the
     * recursive walk, not only at the top of the ORDER BY expression.
     */
    @Test
    void plainReturnOrderByOnDroppedVariableInsideExpressionIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN BusinessFunction
          ORDER BY toLower(f.Name)""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * A UNION branch is validated as its own statement, so the same plain-RETURN shape inside one must
     * report too.
     */
    @Test
    void plainReturnOrderByInsideUnionBranchIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN BusinessFunction
          ORDER BY f.Name
          UNION
          MATCH (g:Function)
          RETURN g.Name AS BusinessFunction""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * A CALL subquery body is validated by the same scope walk, so its own plain RETURN + ORDER BY is
     * held to the same rule.
     */
    @Test
    void plainReturnOrderByInsideCallSubqueryIsRejected() {
      assertThatThrownBy(() -> column("""
          CALL {
            MATCH (f:Function)
            WITH f.Name AS BusinessFunction
            RETURN BusinessFunction
            ORDER BY f.Name
          }
          RETURN BusinessFunction""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * A {@code COLLECT { }} body is handed the outer row as a seed and validated against the outer scope,
     * so its own plain RETURN + ORDER BY is held to the same rule as the top-level one.
     */
    @Test
    void plainReturnOrderByInsideCollectSubqueryExpressionIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN BusinessFunction, COLLECT { MATCH (g:Function) RETURN g.Name ORDER BY f.Name } AS Names""",
          "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * {@code RETURN *} projects the whole scope, so an ORDER BY naming something outside it is the same
     * violation as the aliased form.
     */
    @Test
    void returnStarOrderByOnDroppedVariableIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN *
          ORDER BY f.Name""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'f'");
    }

    /**
     * {@code WITH *} narrows nothing, so the only way its ORDER BY can fail is by naming a variable no
     * clause ever bound. That branch returned before reaching any ORDER BY check, so it sorted on nothing
     * instead of reporting - the same defect as the reported one, found by the completeness sweep.
     */
    @Test
    void withStarOrderByOnNeverBoundVariableIsRejected() {
      assertThatThrownBy(() -> column("""
          MATCH (f:Function)
          WITH *
          ORDER BY ghost.Name
          RETURN f.Name AS BusinessFunction""", "BusinessFunction"))
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'ghost'");
    }

    // ---------------------------------------------------------------------------------------------
    // In-scope forms: all of these were already correct and must stay accepted and correctly sorted.
    // ---------------------------------------------------------------------------------------------

    @Test
    void orderByOnAVariableStillInScopeIsAccepted() {
      assertThat(column("MATCH (f:Function) RETURN f.Name AS BusinessFunction ORDER BY f.Name", "BusinessFunction"))
          .containsExactly("Alpha", "Mid", "Zeta");
    }

    @Test
    void orderByOnTheProjectedAliasIsAccepted() {
      assertThat(column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN BusinessFunction
          ORDER BY BusinessFunction""", "BusinessFunction"))
          .containsExactly("Alpha", "Mid", "Zeta");
    }

    @Test
    void orderByOnAnAliasIntroducedByTheReturnItselfIsAccepted() {
      assertThat(column("""
          MATCH (f:Function)
          WITH f.Name AS Name
          RETURN Name AS BusinessFunction
          ORDER BY BusinessFunction DESC""", "BusinessFunction"))
          .containsExactly("Zeta", "Mid", "Alpha");
    }

    @Test
    void orderByOnAVariableTheWithCarriedForwardIsAccepted() {
      assertThat(column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction, f
          RETURN BusinessFunction
          ORDER BY f.Name""", "BusinessFunction"))
          .containsExactly("Alpha", "Mid", "Zeta");
    }

    @Test
    void orderByOnAnExpressionOverInScopeVariablesIsAccepted() {
      assertThat(column("MATCH (f:Function) RETURN f.Name AS BusinessFunction ORDER BY toLower(f.Name) DESC", "BusinessFunction"))
          .containsExactly("Zeta", "Mid", "Alpha");
    }

    @Test
    void withStarOrderByOnAnInScopeVariableIsAccepted() {
      assertThat(column("""
          MATCH (f:Function)
          WITH *
          ORDER BY f.Name
          RETURN f.Name AS BusinessFunction""", "BusinessFunction"))
          .containsExactly("Alpha", "Mid", "Zeta");
    }

    @Test
    void withStarOrderByOnAnExtraAliasIsAccepted() {
      assertThat(column("""
          MATCH (f:Function)
          WITH *, f.Name AS BusinessFunction
          ORDER BY BusinessFunction DESC
          RETURN BusinessFunction""", "BusinessFunction"))
          .containsExactly("Zeta", "Mid", "Alpha");
    }

    @Test
    void returnStarOrderByOnAnInScopeVariableIsAccepted() {
      assertThat(column("""
          MATCH (f:Function)
          WITH f.Name AS BusinessFunction
          RETURN *
          ORDER BY BusinessFunction""", "BusinessFunction"))
          .containsExactly("Alpha", "Mid", "Zeta");
    }

    @Test
    void orderByAfterWithStarIsAccepted() {
      assertThat(column("""
          MATCH (f:Function)
          WITH *
          RETURN f.Name AS BusinessFunction
          ORDER BY f.Name""", "BusinessFunction"))
          .containsExactly("Alpha", "Mid", "Zeta");
    }

    @Test
    void orderByOnAnUnwoundVariableIsAccepted() {
      assertThat(column("UNWIND ['Zeta', 'Alpha', 'Mid'] AS x RETURN x AS BusinessFunction ORDER BY x", "BusinessFunction"))
          .containsExactly("Alpha", "Mid", "Zeta");
    }
  }

  /**
   * Issue #10195: Cypher {@code ORDER BY} over a BINARY property compared two {@code byte[]} by their identity
   * {@code toString()}, which changes with every load of the record, so the order changed from run to run. Two byte
   * arrays are now compared by their bytes, unsigned and lexicographically, which is the order SQL gives them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10195OrderByBinary extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("PV").createProperty("c_bin", Type.BINARY);
        database.getSchema().getType("PV").createProperty("id", Type.INTEGER);
      });
      database.transaction(() -> {
        database.newVertex("PV").set("id", 1).set("c_bin", new byte[] { 0, 1, (byte) 0xfe, (byte) 0xff }).save();
        database.newVertex("PV").set("id", 2).set("c_bin", new byte[] {}).save();
        database.newVertex("PV").set("id", 3).set("c_bin", new byte[] { 0x7f }).save();
        database.newVertex("PV").set("id", 4).set("c_bin", new byte[] { (byte) 0x80 }).save();
        database.newVertex("PV").set("id", 5).set("c_bin", new byte[] { 0, 1 }).save();
      });
    }

    @Test
    void orderByBinaryIsStableAndUnsigned() {
      // EMPTY FIRST, A PREFIX BEFORE ITS EXTENSIONS, 0x80 AFTER 0x7f (UNSIGNED)
      for (int i = 0; i < 20; i++) {
        assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin")).containsExactly(2, 5, 1, 3, 4);
        assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin DESC")).containsExactly(4, 3, 1, 5, 2);
      }
    }

    @Test
    void orderByBinaryMatchesSql() {
      assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin"))
          .containsExactlyElementsOf(ids("sql", "SELECT id FROM PV ORDER BY c_bin"));
      assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin DESC"))
          .containsExactlyElementsOf(ids("sql", "SELECT id FROM PV ORDER BY c_bin DESC"));
    }

    @Test
    void orderByBinaryWithLimitIsStable() {
      // A LIMIT AFTER ORDER BY TAKES THE TOP-K PATH
      for (int i = 0; i < 20; i++)
        assertThat(ids("opencypher", "MATCH (n:PV) RETURN n.id AS id ORDER BY n.c_bin LIMIT 2")).containsExactly(2, 5);
    }

    @Test
    void orderByBinaryInWithFeedsCollectInOrder() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:PV) WITH n.c_bin AS b ORDER BY b RETURN collect(b)[1] AS second")) {
        assertThat(rs.next().<byte[]>getProperty("second")).containsExactly(0, 1);
      }
    }

    private List<Integer> ids(final String language, final String query) {
      final List<Integer> out = new ArrayList<>();
      try (final ResultSet rs = database.query(language, query)) {
        while (rs.hasNext())
          out.add(rs.next().<Number>getProperty("id").intValue());
      }
      return out;
    }
  }
}
