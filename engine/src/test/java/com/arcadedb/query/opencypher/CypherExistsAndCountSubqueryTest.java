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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * EXISTS { } and COUNT { } subqueries: multi-label, relationship properties, correlation and scope.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherExistsAndCountSubqueryTest {
  /**
   * Reproducer for issue #4995: correlated EXISTS { ... } combined with AND NOT EXISTS { ... }
   * drops a matching row.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue4995ExistsAndNotExists {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue4995");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.command("opencypher", "CREATE "
            + "(u1:User {id: 1, active: true}), "
            + "(u2:User {id: 2, active: false}), "
            + "(u3:User {id: 3, active: true}), "
            + "(u1)-[:FRIEND]->(u2), "
            + "(u3)-[:FRIEND]->(u1)");
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    @Test
    void existsAndNotExists() {
      database.transaction(() -> {
        database.command("opencypher", "MATCH (u:User {active: true}) "
            + "WHERE EXISTS { MATCH (u)-[:FRIEND]->(:User) } "
            + "AND NOT EXISTS { MATCH (u)-[:FRIEND]->(f:User) WHERE f.active IS NULL OR f.active = true } "
            + "SET u.flagged = true");
      });

      final List<Integer> flagged = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (u:User) WHERE u.flagged = true RETURN u.id AS uid ORDER BY uid")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          flagged.add(((Number) r.getProperty("uid")).intValue());
        }
      }

      assertThat(flagged).containsExactly(1);
    }

    @Test
    void existsWithOrAndTrailingClause() {
      // The correlated EXISTS subquery has a top-level OR in its WHERE followed by a RETURN clause,
      // exercising the branch that injects the correlation before a subsequent clause keyword.
      final List<Integer> matched = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "MATCH (u:User {active: true}) "
          + "WHERE EXISTS { MATCH (u)-[:FRIEND]->(f:User) WHERE f.active IS NULL OR f.active = true RETURN f } "
          + "RETURN u.id AS uid ORDER BY uid")) {
        while (rs.hasNext())
          matched.add(((Number) rs.next().getProperty("uid")).intValue());
      }

      // Only u3 has a friend (u1) that is active; u1's only friend (u2) is inactive.
      assertThat(matched).containsExactly(3);
    }
  }

  /**
   * Regression test for GitHub issue #5095.
   * <p>
   * {@code exists((n:A:B)-->(:Node))} must enforce the label conjunction {@code A:B}
   * on the already-bound start variable {@code n}, not only the traversal existence.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5095ExistsMultiLabel {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5095-exists-multilabel").create();
      database.transaction(() -> {
        // n1: labels A+B, no outgoing edge
        // n2: labels A+B, has outgoing edge   -> should match
        // n3: label A only, has outgoing edge -> should NOT match
        // n4: label B only, has outgoing edge -> should NOT match
        database.command("opencypher", "CREATE "
            + "(n1:Node:A:B {id: 1}),"
            + "(n2:Node:A:B {id: 2})-[:LINK]->(n1),"
            + "(n3:Node:A {id: 3})-[:LINK]->(n1),"
            + "(n4:Node:B {id: 4})-[:LINK]->(n1)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private List<Integer> runIds(final String query) {
      final List<Integer> ids = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext()) {
        final Result r = rs.next();
        ids.add(((Number) r.getProperty("id")).intValue());
      }
      return ids;
    }

    @Test
    void existsWithMultiLabelBoundStartEnforcesConjunction() {
      final List<Integer> ids = runIds(
          "MATCH (n:Node) WHERE exists((n:A:B)-->(:Node)) RETURN n.id AS id ORDER BY id");
      assertThat(ids).containsExactly(2);
    }

    @Test
    void controlExplicitPreFilter() {
      final List<Integer> ids = runIds(
          "MATCH (n:Node:A:B) WHERE exists((n)-->(:Node)) RETURN n.id AS id ORDER BY id");
      assertThat(ids).containsExactly(2);
    }

    @Test
    void controlSimpleExistsWithoutLabelConstraint() {
      final List<Integer> ids = runIds(
          "MATCH (n:Node) WHERE exists((n)-->(:Node)) RETURN n.id AS id ORDER BY id");
      assertThat(ids).containsExactly(2, 3, 4);
    }

    @Test
    void existsWithSingleExtraLabelBoundStart() {
      // Only nodes that also have label A and an outgoing edge: n2 (A+B) and n3 (A only)
      final List<Integer> ids = runIds(
          "MATCH (n:Node) WHERE exists((n:A)-->(:Node)) RETURN n.id AS id ORDER BY id");
      assertThat(ids).containsExactly(2, 3);
    }

    @Test
    void notExistsWithMultiLabelBoundStart() {
      // NOT exists((n:A:B)-->) is true for nodes that do NOT have both labels with an outgoing edge.
      // n1 (A+B, no edge), n3 (A only), n4 (B only) satisfy NOT; n2 does not.
      final List<Integer> ids = runIds(
          "MATCH (n:Node) WHERE NOT exists((n:A:B)-->(:Node)) RETURN n.id AS id ORDER BY id");
      assertThat(ids).containsExactly(1, 3, 4);
    }
  }

  /**
   * Regression test for GitHub issue #5109.
   * <p>
   * {@code exists((a)-[:R {w: 999}]->(:X))} must enforce the inline relationship property
   * filter {@code {w: 999}}. A non-matching value must yield {@code false}, not {@code true}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5109ExistsRelProperty {
    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/issue-5109-exists-rel-property").create();
      database.transaction(() -> {
        // Alice -[:R {w: 3}]-> Bob, Alice -[:R {w: 5}]-> Dave. No w=999 anywhere.
        database.command("opencypher", "CREATE "
            + "(a:X {name: 'Alice'}),"
            + "(b:X {name: 'Bob'}),"
            + "(d:X {name: 'Dave'}),"
            + "(a)-[:R {w: 3}]->(b),"
            + "(a)-[:R {w: 5}]->(d)");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    private boolean existsBool(final String existsPattern) {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {name: 'Alice'}) RETURN exists(" + existsPattern + ") AS v");
      assertThat(rs.hasNext()).isTrue();
      final Result r = rs.next();
      return Boolean.TRUE.equals(r.getProperty("v"));
    }

    @Test
    void truthTableForInlineRelPropertyFilter() {
      assertThat(existsBool("(a)-[:R]->(:X)")).as("any_R").isTrue();
      assertThat(existsBool("(a)-[:R {w: 3}]->(:X)")).as("has_w3").isTrue();
      assertThat(existsBool("(a)-[:R {w: 5}]->(:X)")).as("has_w5").isTrue();
      assertThat(existsBool("(a)-[:R {w: 999}]->(:X)")).as("has_w999").isFalse();
    }

    @Test
    void whereExistsWithNonMatchingRelPropertyReturnsEmpty() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {name: 'Alice'}) WHERE exists((a)-[:R {w: 999}]->(:X)) RETURN a.name AS name");
      assertThat(rs.hasNext()).isFalse();
    }

    @Test
    void whereExistsWithMatchingRelPropertyReturnsRow() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {name: 'Alice'}) WHERE exists((a)-[:R {w: 3}]->(:X)) RETURN a.name AS name");
      assertThat(rs.hasNext()).isTrue();
      assertThat((String) rs.next().getProperty("name")).isEqualTo("Alice");
    }

    @Test
    void whereExistsWithBoundEndNodeEnforcesRelProperty() {
      // Bob is the end of the w=3 edge; a w=999 filter to a bound Bob must not match.
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {name: 'Alice'}), (b:X {name: 'Bob'}) "
              + "WHERE exists((a)-[:R {w: 999}]->(b)) RETURN a.name AS name");
      assertThat(rs.hasNext()).isFalse();

      final ResultSet rs2 = database.query("opencypher",
          "MATCH (a:X {name: 'Alice'}), (b:X {name: 'Bob'}) "
              + "WHERE exists((a)-[:R {w: 3}]->(b)) RETURN a.name AS name");
      assertThat(rs2.hasNext()).isTrue();
    }

    @Test
    void notExistsWithNonMatchingRelPropertyReturnsRow() {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:X {name: 'Alice'}) WHERE NOT exists((a)-[:R {w: 999}]->(:X)) RETURN a.name AS name");
      assertThat(rs.hasNext()).isTrue();
      assertThat((String) rs.next().getProperty("name")).isEqualTo("Alice");
    }
  }

  /**
   * Reproducer for issue #5138: {@code EXISTS { ... WHERE node IN collected_nodes }} never matches
   * against an outer-scope collected node list.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5138ExistsInCollectedNodes {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue5138");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.command("opencypher", "CREATE "
            + "(a:User {id: 1, active: true}), "
            + "(b:User {id: 2, active: true}), "
            + "(c:User {id: 3, active: false}), "
            + "(d:User {id: 4, active: false}), "
            + "(a)-[:FRIEND]->(c), "
            + "(b)-[:FRIEND]->(d)");
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    @Test
    void existsWithInCollectedNodeList() {
      final List<String> results = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (inactive:User {active: false}) "
              + "WITH collect(DISTINCT inactive) AS inactive_nodes "
              + "MATCH (u:User {active: true}) "
              + "RETURN u.id AS id, EXISTS { "
              + "  MATCH (u)-[:FRIEND]->(friend) "
              + "  WHERE friend IN inactive_nodes "
              + "} AS has_inactive_friend "
              + "ORDER BY id")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          results.add(r.getProperty("id") + "=" + r.getProperty("has_inactive_friend"));
        }
      }

      assertThat(results).containsExactly("1=true", "2=true");
    }

    @Test
    void writeGuardedByExistsInCollectedNodeList() {
      database.transaction(() -> {
        database.command("opencypher",
            "MATCH (inactive:User {active: false}) "
                + "WITH collect(DISTINCT inactive) AS inactive_nodes "
                + "MATCH (u:User {active: true}) "
                + "WHERE EXISTS { "
                + "  MATCH (u)-[:FRIEND]->(friend) "
                + "  WHERE friend IN inactive_nodes "
                + "} "
                + "SET u.flagged = true");
      });

      long cnt;
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (u:User) WHERE u.flagged = true RETURN count(u) AS cnt")) {
        cnt = ((Number) rs.next().getProperty("cnt")).longValue();
      }
      assertThat(cnt).isEqualTo(2L);
    }
  }

  /**
   * Regression test for issue #5165: a correlated {@code count { ... }} subquery whose inner
   * {@code WHERE} predicate combines {@code IS NULL} with another predicate using {@code OR}
   * (e.g. {@code WHERE f.active IS NULL OR f.active = true}) returned wrong counts.
   * <p>
   * The injected correlation condition ({@code id(u) = $__count_u AND ...}) was placed before the
   * OR-containing predicate without parentheses, so operator precedence turned it into
   * {@code (id(u) = ... AND f.active IS NULL) OR f.active = true}, decoupling the OR branch from the
   * correlated outer row and counting matches globally.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5165CountSubqueryOrPredicate {
    private Database database;

    @BeforeEach
    void setup() {
      database = new DatabaseFactory("./target/databases/issue5165count").create();
      database.transaction(() -> {
        database.command("opencypher",
            """
            CREATE
              (u1:User {id:1, active:true}),
              (u2:User {id:2, active:true}),
              (u3:User {id:3, active:true}),
              (u4:User {id:4, active:false}),
              (u5:User {id:5, active:false}),
              (u6:User {id:6, active:false}),
              (u1)-[:FRIEND]->(u4),
              (u1)-[:FRIEND]->(u5),
              (u2)-[:FRIEND]->(u4),
              (u2)-[:FRIEND]->(u1),
              (u3)-[:FRIEND]->(u5),
              (u3)-[:FRIEND]->(u6)""");
      });
    }

    @AfterEach
    void teardown() {
      if (database != null)
        database.drop();
    }

    /**
     * Failing query from the issue: only u2 has a friend with active=true; none has active IS NULL.
     * Expected: u1=0, u2=1, u3=0 (matches Neo4j 2026.05.0).
     */
    @Test
    void countSubqueryWithIsNullOrPredicate() {
      final List<Result> rows = collect(database.query("opencypher",
          """
          MATCH (u:User {active:true})
          RETURN u.id AS id,
            count {
              MATCH (u)-[:FRIEND]->(f:User)
              WHERE f.active IS NULL OR f.active = true
            } AS cnt
          ORDER BY u.id"""));

      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(0L);
      assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(1L);
      assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(0L);
    }

    /**
     * Control: same expression without OR must keep working.
     */
    @Test
    void countSubqueryWithSimplePredicate() {
      final List<Result> rows = collect(database.query("opencypher",
          """
          MATCH (u:User {active:true})
          RETURN u.id AS id,
            count {
              MATCH (u)-[:FRIEND]->(f:User)
              WHERE f.active = true
            } AS cnt
          ORDER BY u.id"""));

      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(0L);
      assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(1L);
      assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(0L);
    }

    /**
     * Control: IS NULL alone counts zero for every outer row.
     */
    @Test
    void countSubqueryWithIsNullOnly() {
      final List<Result> rows = collect(database.query("opencypher",
          """
          MATCH (u:User {active:true})
          RETURN u.id AS id,
            count {
              MATCH (u)-[:FRIEND]->(f:User)
              WHERE f.active IS NULL
            } AS cnt
          ORDER BY u.id"""));

      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(0L);
      assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(0L);
      assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(0L);
    }

    /**
     * Control: basic correlated count without WHERE counts all friends.
     */
    @Test
    void countSubqueryWithoutWhere() {
      final List<Result> rows = collect(database.query("opencypher",
          """
          MATCH (u:User {active:true})
          RETURN u.id AS id,
            count {
              MATCH (u)-[:FRIEND]->(:User)
            } AS cnt
          ORDER BY u.id"""));

      assertThat(rows).hasSize(3);
      assertThat(((Number) rows.get(0).getProperty("cnt")).longValue()).isEqualTo(2L);
      assertThat(((Number) rows.get(1).getProperty("cnt")).longValue()).isEqualTo(2L);
      assertThat(((Number) rows.get(2).getProperty("cnt")).longValue()).isEqualTo(2L);
    }

    private static List<Result> collect(final ResultSet rs) {
      final List<Result> list = new ArrayList<>();
      while (rs.hasNext())
        list.add(rs.next());
      return list;
    }
  }

  /**
   * Regression test for GitHub issue #5696.
   * <p>
   * A subquery body ({@code EXISTS { }}, {@code COUNT { }}) seeded directly from an outer row (no leading
   * {@code WITH}) honours a pre-bound outer <i>node</i> variable but not a pre-bound outer
   * <i>relationship</i> variable: a leading {@code MATCH} that re-mentions the outer relationship variable
   * treated it as fresh and unbound, so the correlation silently matched nothing. A {@code WITH} in front
   * of the {@code MATCH}, or a node-only correlation, took the working path - which is what isolates the
   * defect to the relationship binding specifically.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5696ExistsRelationshipVariableCorrelation extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("opencypher",
          "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})");
    }

    /** The issue's first reproducer: EXISTS{} re-matching the outer relationship variable. */
    @Test
    void existsCorrelatesOnTheOuterRelationshipVariable() {
      assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) } RETURN count(*) AS c"))
          .isEqualTo(2L);
    }

    /** The issue's second reproducer: COUNT{} re-matching the outer relationship variable, per row. */
    @Test
    void countSubqueryCorrelatesOnTheOuterRelationshipVariable() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person)-[r:KNOWS]->(q) RETURN COUNT { MATCH (p)-[r:KNOWS]->(x) } AS c ORDER BY p.name")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("Alice-Bob row").isEqualTo(1L);
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("Bob-Charlie row").isEqualTo(1L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /** {@code COLLECT { }} flows through the same {@code executeWithSeedRow} path as EXISTS/COUNT. */
    @Test
    void collectSubqueryCorrelatesOnTheOuterRelationshipVariable() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person)-[r:KNOWS]->(q) RETURN COLLECT { MATCH (p)-[r:KNOWS]->(x) RETURN x.name } AS names "
              + "ORDER BY p.name")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<List<Object>>getProperty("names")).as("Alice-Bob row").containsExactly("Bob");
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<List<Object>>getProperty("names")).as("Bob-Charlie row").containsExactly("Charlie");
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /** Control: a fresh (non-reused) relationship variable in the body must still behave as an unbound match. */
    @Test
    void aFreshRelationshipVariableInTheBodyIsUnaffected() {
      assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) WHERE EXISTS { MATCH (p)-[r2:KNOWS]->(x) } RETURN count(*) AS c"))
          .isEqualTo(2L);
    }

    /** Control: an explicit CALL{} with an importing WITH already worked before the fix - must keep working. */
    @Test
    void callSubqueryWithExplicitWithImportStillCorrelates() {
      assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) CALL { WITH p, r MATCH (p)-[r:KNOWS]->(x) RETURN x } "
          + "RETURN count(*) AS c")).isEqualTo(2L);
    }

    /** Control: a WITH-first EXISTS body already materialized the binding before the fix - must keep working. */
    @Test
    void existsWithLeadingWithStillCorrelates() {
      assertThat(count("MATCH (p:Person)-[r:KNOWS]->(q) WHERE EXISTS { WITH p, r MATCH (p)-[r:KNOWS]->(x) RETURN x } "
          + "RETURN count(*) AS c")).isEqualTo(2L);
    }

    /** A relationship-only correlation (no outer node re-mention) that cannot possibly be satisfied by another edge. */
    @Test
    void existsCorrelatesOnTheRelationshipEvenWhenTheEdgeTypeWouldOtherwiseMatchMoreThanOne() {
      database.command("opencypher", "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Charlie'}) "
          + "CREATE (a)-[:KNOWS]->(c)");

      // Alice now has two outgoing KNOWS edges (to Bob and to Charlie); the outer row for the
      // Alice->Bob edge must correlate to that specific edge, not to Alice's other KNOWS edge.
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person {name:'Alice'})-[r:KNOWS]->(q:Person {name:'Bob'}) "
              + "WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) WHERE x.name = 'Bob' } RETURN count(*) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
      }
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (p:Person {name:'Alice'})-[r:KNOWS]->(q:Person {name:'Bob'}) "
              + "WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) WHERE x.name = 'Charlie' } RETURN count(*) AS c")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(0L);
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).isTrue();
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }

  /**
   * Regression tests for issue #5825: a variable dropped from scope by a {@code WITH} clause is
   * correctly rejected when referenced directly (e.g. in {@code RETURN}), but was silently accepted
   * when referenced inside an {@code EXISTS { ... }}, {@code COUNT { ... }} or {@code COLLECT { ... }}
   * subquery, where it just resolved as missing/null instead of raising an undefined-variable error -
   * turning a scope mistake into a silently-empty or silently-wrong result instead of a client error.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5825ExistsSubqueryScope extends TestHelper {
    @Override
    protected void beginTest() {
      database.command("opencypher", "CREATE (:_Oracle {_id:'n1', p:0}), (:_Oracle {_id:'n2', p:1})");
    }

    /** The reporter's failing query: `v` is dropped by the second WITH, then referenced inside EXISTS. */
    @Test
    void existsSubqueryReferencingDroppedVariableThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          """
          UNWIND [0] AS v \
          WITH v \
          WITH 0 AS y \
          MATCH (n) \
          WHERE EXISTS { \
            MATCH (x) \
            WHERE x.p = v \
          } \
          RETURN n._id AS n, y""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'v'");
    }

    /** Same shape, but the out-of-scope reference is inside COUNT { ... }. */
    @Test
    void countSubqueryReferencingDroppedVariableThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          """
          UNWIND [0] AS v \
          WITH v \
          WITH 0 AS y \
          MATCH (n) \
          WHERE COUNT { \
            MATCH (x) \
            WHERE x.p = v \
          } > 0 \
          RETURN n._id AS n, y""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'v'");
    }

    /** Same shape, but the out-of-scope reference is inside COLLECT { ... }. */
    @Test
    void collectSubqueryReferencingDroppedVariableThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          """
          UNWIND [0] AS v \
          WITH v \
          WITH 0 AS y \
          MATCH (n) \
          RETURN n._id AS n, y, COLLECT { \
            MATCH (x) \
            WHERE x.p = v \
            RETURN x._id \
          } AS c""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'v'");
    }

    /** Control: the same out-of-scope reference is already rejected when it is a direct RETURN. */
    @Test
    void directReferenceToDroppedVariableThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          """
          UNWIND [0] AS v \
          WITH v \
          WITH 0 AS y \
          RETURN v""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'v'");
    }

    /** Same shape, but the out-of-scope reference is inside a UNION branch of an EXISTS subquery body. */
    @Test
    void existsSubqueryUnionBranchReferencingDroppedVariableThrows() {
      assertThatThrownBy(() -> database.query("opencypher",
          """
          UNWIND [0] AS v \
          WITH v \
          WITH 0 AS y \
          MATCH (n) \
          WHERE EXISTS { \
            MATCH (x) \
            WHERE x.p = v \
            RETURN x \
            UNION \
            MATCH (z) \
            WHERE z.p = 1 \
            RETURN z \
          } \
          RETURN n._id AS n, y""").close())
          .isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable")
          .hasMessageContaining("'v'");
    }

    /** Control: keeping `v` in scope makes the EXISTS query valid and produces the expected rows. */
    @Test
    void existsSubqueryReferencingPreservedVariableIsValid() {
      final ResultSet rs = database.query("opencypher",
          """
          UNWIND [0] AS v \
          WITH v \
          WITH v, 0 AS y \
          MATCH (n) \
          WHERE EXISTS { \
            MATCH (x) \
            WHERE x.p = v \
          } \
          RETURN n._id AS n, y \
          ORDER BY n""");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("n")).isEqualTo("n1");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("n")).isEqualTo("n2");
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
