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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cypher index seeks: anchor selection, composite and inherited indexes, OR and IN seeks, parameters and index use inside write statements.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherIndexSeekTest {
  /**
   * Regression test for issue #5107: a {@code MATCH (p:Person) WHERE p.id = <const>} predicate inside
   * a write statement ({@code MATCH ... CREATE ...}) must use the unique index on {@code Person.id}
   * for the leading MATCH instead of full-scanning the vertex type.
   * <p>
   * Before the fix the leading MATCH full-scanned because its WHERE-based index shortcut only fired when
   * an input row was present; the leading/seed MATCH has none. These tests assert the index is used via
   * the PROFILE plan text and that results are still correct.
   * <p>
   * Note: since issue #5136 relaxed the {@code createCount > 1} guard, the two-CREATE write now runs
   * through the cost-based optimizer (which emits {@code NodeIndexSeek}) rather than the legacy path
   * (which prints {@code [index: Person[id]]}). Both are index-backed, so these tests accept either
   * marker.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5107MatchCreateIndex extends TestHelper {
    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final var personType = database.getSchema().createVertexType("Person");
        personType.createProperty("id", Integer.class);
        personType.createProperty("name", String.class);
        personType.createProperty("age", Integer.class);
        personType.createProperty("city", String.class);

        final var knows = database.getSchema().createEdgeType("KNOWS");
        knows.createProperty("since", Integer.class);

        // Unique index on Person.id (the index the write's MATCH must use).
        personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      });

      // Populate 100 Person vertices with sequential ids.
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.command("opencypher",
              "CREATE (p:Person {name: 'Person" + i + "', id: " + i + ", age: " + (20 + (i % 50)) + ", city: 'c" + i + "'})");
      });
    }

    /**
     * The exact issue query: a leading MATCH with a constant WHERE equality followed by two CREATE
     * clauses. The MATCH must use the unique index, and the write side-effects must be correct.
     */
    @Test
    void matchCreateUsesIndexForConstantPredicate() {
      final String write = "MATCH (p:Person) WHERE p.id = 42 "
          + "CREATE (q:Person {id: 900001, name: 'w', age: 33, city: 'c'}) "
          + "CREATE (p)-[:KNOWS {since: 2026}]->(q)";

      // PROFILE surfaces the executed plan. Since #5136 the two-CREATE write runs through the optimizer
      // (NodeIndexSeek); the legacy path prints [index: Person[id]]. Either proves the index was used
      // instead of a full scan.
      final String plan = profilePlan(write);

      assertThat(plan)
          .as("leading MATCH with constant WHERE equality must use the unique index, not a full scan\n%s", plan)
          .containsAnyOf("NodeIndexSeek", "[index: Person[id]]");

      // Verify the write actually happened and connected the correct source vertex.
      database.transaction(() -> {
        final ResultSet check = database.query("opencypher",
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 42 RETURN p.id AS src, q.id AS dst, q.name AS name");
        assertThat(check.hasNext()).isTrue();
        final Result row = check.next();
        assertThat((Integer) row.getProperty("src")).isEqualTo(42);
        assertThat((Integer) row.getProperty("dst")).isEqualTo(900001);
        assertThat((String) row.getProperty("name")).isEqualTo("w");
        assertThat(check.hasNext()).isFalse();
        check.close();
      });
    }

    /**
     * Same shape, but the predicate value comes from a query parameter. Parameters resolve without an
     * input row, so the index must still be used.
     */
    @Test
    void matchCreateUsesIndexForParameterPredicate() {
      final Map<String, Object> params = new HashMap<>();
      params.put("id", 7);

      final String write = "MATCH (p:Person) WHERE p.id = $id "
          + "CREATE (q:Person {id: 900002, name: 'param', age: 40, city: 'x'}) "
          + "CREATE (p)-[:KNOWS {since: 2027}]->(q)";

      final String plan = profilePlan(write, params);

      assertThat(plan)
          .as("leading MATCH with parameter WHERE equality must use the unique index\n%s", plan)
          .containsAnyOf("NodeIndexSeek", "[index: Person[id]]");

      database.transaction(() -> {
        final ResultSet check = database.query("opencypher",
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 7 RETURN q.id AS dst", params);
        assertThat(check.hasNext()).isTrue();
        assertThat((Integer) check.next().getProperty("dst")).isEqualTo(900002);
        check.close();
      });
    }

    /**
     * A single CREATE keeps the cost-based optimizer path, which already emits NodeIndexSeek. Guard
     * that the common case is not regressed and stays index-backed.
     */
    @Test
    void matchSingleCreateStillUsesIndex() {
      final String write = "MATCH (p:Person) WHERE p.id = 13 "
          + "CREATE (q:Person {id: 900003, name: 'single', age: 21, city: 'y'})";

      final String plan = profilePlan(write);

      assertThat(plan)
          .as("single-CREATE write must use the index (optimizer NodeIndexSeek or legacy index)\n%s", plan)
          .containsAnyOf("NodeIndexSeek", "[index: Person[id]]");

      database.transaction(() -> {
        final ResultSet check = database.query("opencypher", "MATCH (q:Person) WHERE q.id = 900003 RETURN q.name AS name");
        assertThat(check.hasNext()).isTrue();
        assertThat((String) check.next().getProperty("name")).isEqualTo("single");
        check.close();
      });
    }

    /**
     * A leading MATCH whose WHERE predicate cannot use an index (no index on {@code age}) must still
     * execute correctly and must not throw - it simply falls back to a full scan + row-level filter.
     */
    @Test
    void matchCreateNonIndexedPredicateFallsBackGracefully() {
      // 'name' is not indexed; the constant equality on it cannot use an index and must fall back to a
      // full scan + row-level filter without throwing. 'Person5' matches exactly one vertex.
      final String write = "MATCH (p:Person) WHERE p.name = 'Person5' "
          + "CREATE (q:Person {id: 900004, name: 'fallback', age: 99, city: 'z'}) "
          + "CREATE (p)-[:KNOWS {since: 2028}]->(q)";

      // Must not throw and must not falsely claim an index (there is none on 'name').
      final String plan = profilePlan(write);
      assertThat(plan).doesNotContain("[index:");

      database.transaction(() -> {
        final ResultSet check = database.query("opencypher",
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE q.name = 'fallback' RETURN p.id AS src, p.name AS srcName");
        assertThat(check.hasNext()).isTrue();
        final Result row = check.next();
        assertThat((Integer) row.getProperty("src")).isEqualTo(5);
        assertThat((String) row.getProperty("srcName")).isEqualTo("Person5");
        assertThat(check.hasNext()).isFalse();
        check.close();
      });
    }

    private String profilePlan(final String cypher) {
      return profilePlan(cypher, null);
    }

    private String profilePlan(final String cypher, final Map<String, Object> params) {
      final StringBuilder plan = new StringBuilder();
      // The PROFILE run executes the write; wrap it in a transaction so the side-effects commit.
      database.transaction(() -> {
        final ResultSet rs = params != null
            ? database.command("opencypher", "PROFILE " + cypher, params)
            : database.command("opencypher", "PROFILE " + cypher);
        while (rs.hasNext())
          rs.next();
        plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
        rs.close();
      });
      return plan.toString();
    }
  }

  /**
   * Regression test for issue #5136 (follow-up of #5107): a {@code MATCH ... CREATE ... CREATE ...}
   * statement with more than one CREATE clause must be handled by the cost-based optimizer (which emits
   * {@code NodeIndexSeek} for the leading indexed MATCH) instead of bailing out to the legacy path.
   * <p>
   * The main risk being guarded is variable threading across the chained {@code CreateStep}s: a second
   * CREATE {@code (p)-[:R]->(q)} references {@code p} bound by the MATCH and {@code q} created by the
   * first CREATE. These tests assert both the optimizer plan is used AND the write side-effects are
   * correct (the created variable is propagated through the CreateStep chain).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5136MatchMultipleCreateOptimizer extends TestHelper {
    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final var personType = database.getSchema().createVertexType("Person");
        personType.createProperty("id", Integer.class);
        personType.createProperty("name", String.class);
        personType.createProperty("age", Integer.class);
        personType.createProperty("city", String.class);

        final var knows = database.getSchema().createEdgeType("KNOWS");
        knows.createProperty("since", Integer.class);

        // Unique index on Person.id: the leading MATCH must resolve through it via NodeIndexSeek.
        personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      });

      // Populate 100 Person vertices with sequential ids.
      database.transaction(() -> {
        for (int i = 0; i < 100; i++)
          database.command("opencypher",
              "CREATE (p:Person {name: 'Person" + i + "', id: " + i + ", age: " + (20 + (i % 50)) + ", city: 'c" + i + "'})");
      });
    }

    /**
     * The exact #5107/#5136 query: a leading indexed MATCH followed by two CREATE clauses, the second
     * one referencing both the matched node and the node created by the first CREATE. The optimizer
     * must now handle it (NodeIndexSeek) and the edge must connect the correct source to the new node.
     */
    @Test
    void twoCreateClausesUseOptimizerAndThreadCreatedVariable() {
      final String write = "MATCH (p:Person) WHERE p.id = 42 "
          + "CREATE (q:Person {id: 900001, name: 'w', age: 33, city: 'c'}) "
          + "CREATE (p)-[:KNOWS {since: 2026}]->(q)";

      final String plan = profilePlan(write);

      assertThat(plan)
          .as("leading MATCH before multiple CREATE must now use the optimizer index seek\n%s", plan)
          .contains("NodeIndexSeek");

      database.transaction(() -> {
        final ResultSet check = database.query("opencypher",
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 42 RETURN p.id AS src, q.id AS dst, q.name AS name");
        assertThat(check.hasNext()).isTrue();
        final Result row = check.next();
        assertThat((Integer) row.getProperty("src")).isEqualTo(42);
        assertThat((Integer) row.getProperty("dst")).isEqualTo(900001);
        assertThat((String) row.getProperty("name")).isEqualTo("w");
        assertThat(check.hasNext()).isFalse();
        check.close();
      });
    }

    /**
     * Same shape but the predicate value is a parameter. Parameters resolve without an input row so the
     * index seek must still be chosen.
     */
    @Test
    void twoCreateClausesWithParameterPredicate() {
      final Map<String, Object> params = new HashMap<>();
      params.put("id", 7);

      final String write = "MATCH (p:Person) WHERE p.id = $id "
          + "CREATE (q:Person {id: 900002, name: 'param', age: 40, city: 'x'}) "
          + "CREATE (p)-[:KNOWS {since: 2027}]->(q)";

      final String plan = profilePlan(write, params);
      assertThat(plan)
          .as("parameter predicate must still choose the optimizer index seek\n%s", plan)
          .contains("NodeIndexSeek");

      database.transaction(() -> {
        final ResultSet check = database.query("opencypher",
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 7 RETURN q.id AS dst", params);
        assertThat(check.hasNext()).isTrue();
        assertThat((Integer) check.next().getProperty("dst")).isEqualTo(900002);
        check.close();
      });
    }

    /**
     * Three CREATE clauses where each later CREATE references a variable created by an earlier one:
     * CREATE a, then CREATE b linked to a, then CREATE c linked to b. Verifies the created-variable
     * threading survives across a longer chain of CreateSteps.
     */
    @Test
    void threeCreateClausesEachReferencingPreviousCreatedVariable() {
      final String write = "MATCH (p:Person) WHERE p.id = 10 "
          + "CREATE (a:Person {id: 910001, name: 'a', age: 1, city: 'c'}) "
          + "CREATE (a)-[:KNOWS {since: 2001}]->(b:Person {id: 910002, name: 'b', age: 2, city: 'c'}) "
          + "CREATE (b)-[:KNOWS {since: 2002}]->(p)";

      final String plan = profilePlan(write);
      assertThat(plan).as("multi-CREATE chain must use the optimizer\n%s", plan).contains("NodeIndexSeek");

      database.transaction(() -> {
        // a -> b -> p(id=10) chain: verify every hop landed on the intended vertex.
        final ResultSet check = database.query("opencypher",
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(p:Person) "
                + "WHERE a.id = 910001 RETURN a.id AS a, b.id AS b, p.id AS p");
        assertThat(check.hasNext()).isTrue();
        final Result row = check.next();
        assertThat((Integer) row.getProperty("a")).isEqualTo(910001);
        assertThat((Integer) row.getProperty("b")).isEqualTo(910002);
        assertThat((Integer) row.getProperty("p")).isEqualTo(10);
        assertThat(check.hasNext()).isFalse();
        check.close();
      });
    }

    /**
     * MATCH + CREATE vertex + CREATE edge back to the matched node (edge direction into the matched
     * node). Confirms the matched variable stays bound through the CREATE chain.
     */
    @Test
    void createEdgeBackToMatchedNode() {
      final String write = "MATCH (p:Person) WHERE p.id = 20 "
          + "CREATE (q:Person {id: 920001, name: 'back', age: 5, city: 'c'}) "
          + "CREATE (q)-[:KNOWS {since: 1999}]->(p)";

      final String plan = profilePlan(write);
      assertThat(plan).contains("NodeIndexSeek");

      database.transaction(() -> {
        final ResultSet check = database.query("opencypher",
            "MATCH (q:Person)-[:KNOWS]->(p:Person) WHERE q.id = 920001 RETURN p.id AS dst");
        assertThat(check.hasNext()).isTrue();
        assertThat((Integer) check.next().getProperty("dst")).isEqualTo(20);
        assertThat(check.hasNext()).isFalse();
        check.close();
      });
    }

    /**
     * Mixed CREATE/SET/MERGE ordering after the MATCH. The clauses must be applied in textual order:
     * CREATE q, SET q.age, then MERGE the edge. A single MERGE keeps the optimizer eligible.
     */
    @Test
    void mixedCreateSetMergeOrdering() {
      final String write = "MATCH (p:Person) WHERE p.id = 30 "
          + "CREATE (q:Person {id: 930001, name: 'mix', age: 5, city: 'c'}) "
          + "SET q.age = 77 "
          + "MERGE (p)-[:KNOWS {since: 2030}]->(q)";

      final String plan = profilePlan(write);
      assertThat(plan).as("mixed CREATE/SET/MERGE must still use the optimizer\n%s", plan).contains("NodeIndexSeek");

      database.transaction(() -> {
        final ResultSet check = database.query("opencypher",
            "MATCH (p:Person)-[:KNOWS]->(q:Person) WHERE p.id = 30 RETURN q.id AS dst, q.age AS age");
        assertThat(check.hasNext()).isTrue();
        final Result row = check.next();
        assertThat((Integer) row.getProperty("dst")).isEqualTo(930001);
        // SET after CREATE must have overwritten the created age.
        assertThat((Integer) row.getProperty("age")).isEqualTo(77);
        assertThat(check.hasNext()).isFalse();
        check.close();
      });
    }

    /**
     * The multi-CREATE optimizer path must create exactly the expected number of vertices and edges
     * (no duplicate or dropped writes) across the whole Person type.
     */
    @Test
    void multiCreateProducesExactCounts() {
      final long personsBefore = countType("Person");

      database.transaction(() -> database.command("opencypher",
          "MATCH (p:Person) WHERE p.id = 50 "
              + "CREATE (q:Person {id: 940001, name: 'x', age: 5, city: 'c'}) "
              + "CREATE (p)-[:KNOWS {since: 2040}]->(q)"));

      // Exactly one new Person and one new KNOWS edge.
      assertThat(countType("Person")).isEqualTo(personsBefore + 1);
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c");
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
        rs.close();
      });
    }

    private long countType(final String type) {
      final long[] c = new long[1];
      database.transaction(() -> {
        final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM `" + type + "`");
        c[0] = ((Number) rs.next().getProperty("c")).longValue();
        rs.close();
      });
      return c[0];
    }

    private String profilePlan(final String cypher) {
      return profilePlan(cypher, null);
    }

    private String profilePlan(final String cypher, final Map<String, Object> params) {
      final StringBuilder plan = new StringBuilder();
      database.transaction(() -> {
        final ResultSet rs = params != null
            ? database.command("opencypher", "PROFILE " + cypher, params)
            : database.command("opencypher", "PROFILE " + cypher);
        while (rs.hasNext())
          rs.next();
        plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
        rs.close();
      });
      return plan.toString();
    }
  }

  /**
   * Regression test for issue #5362: a redundant equality predicate composed with AND
   * ({@code WHERE (n.id = 1) AND (n.id = 1)}) must not downgrade the plan from
   * {@code NodeIndexSeek} to a full {@code NodeByLabelScan}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5362DuplicatePredicateIndexSeek extends TestHelper {
    @Override
    protected void beginTest() {
      database.transaction(() -> {
        final var type = database.getSchema().createVertexType("Bench");
        type.createProperty("id", Integer.class);
        type.createProperty("name", String.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      });

      database.transaction(() -> {
        for (int i = 0; i < 2000; i++)
          database.command("opencypher", "CREATE (n:Bench {id: " + i + ", name: 'n" + (i % 10) + "'})");
      });
    }

    @Test
    void singleEqualityUsesIndexSeek() {
      final String plan = profilePlan("MATCH (n:Bench) WHERE n.id = 1 RETURN count(n) AS c");
      assertThat(plan).as("baseline\n%s", plan).contains("NodeIndexSeek");
    }

    @Test
    void duplicateEqualityStillUsesIndexSeek() {
      final String plan = profilePlan("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 1) RETURN count(n) AS c");
      assertThat(plan).as("redundant AND must not disable the index seek\n%s", plan)
          .contains("NodeIndexSeek")
          .doesNotContain("NodeByLabelScan");
    }

    @Test
    void duplicateEqualityReturnsSameResult() {
      assertThat(count("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 1) RETURN count(n) AS c")).isEqualTo(1L);
      assertThat(count("MATCH (n:Bench) WHERE n.id = 1 RETURN count(n) AS c")).isEqualTo(1L);
    }

    /**
     * Two different equality predicates on the same indexed property are contradictory: the result must
     * be empty, and picking one of them for the index seek must not lose the other filter.
     */
    @Test
    void contradictoryEqualitiesReturnNothing() {
      assertThat(count("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 2) RETURN count(n) AS c")).isEqualTo(0L);
    }

    /**
     * A redundant equality ANDed with a further restricting predicate must keep both filters applied.
     */
    @Test
    void duplicateEqualityWithExtraPredicate() {
      assertThat(count("MATCH (n:Bench) WHERE (n.id = 1) AND (n.id = 1) AND n.id > 5 RETURN count(n) AS c")).isEqualTo(0L);
      assertThat(count("MATCH (n:Bench) WHERE (n.id = 7) AND (n.id = 7) AND n.id > 5 RETURN count(n) AS c")).isEqualTo(1L);
    }

    /**
     * The same gap hit any conjunction, not just a duplicated one: an indexed equality ANDed with a
     * non-indexed predicate must still anchor on the index.
     */
    @Test
    void mixedConjunctionUsesIndexSeek() {
      final String plan = profilePlan("MATCH (n:Bench) WHERE n.id = 7 AND n.name = 'n7' RETURN count(n) AS c");
      assertThat(plan).as("indexed equality ANDed with a non-indexed one\n%s", plan).contains("NodeIndexSeek");
      assertThat(count("MATCH (n:Bench) WHERE n.id = 7 AND n.name = 'n7' RETURN count(n) AS c")).isEqualTo(1L);
      // Conjunction that no row satisfies: the seek must not swallow the second predicate.
      assertThat(count("MATCH (n:Bench) WHERE n.id = 7 AND n.name = 'n8' RETURN count(n) AS c")).isEqualTo(0L);
    }

    /**
     * A disjunction must not become a single-value seek, which would drop the other branch's rows. Since #8723 an OR of
     * equalities on one indexed property is the IN-list seek, which looks up both values.
     */
    @Test
    void disjunctionIsNotAnchoredOnTheIndex() {
      final String query = "MATCH (n:Bench) WHERE n.id = 1 OR n.id = 2 RETURN count(n) AS c";
      final String plan = profilePlan(query);
      assertThat(plan).as("OR must be seeked with both values\n%s", plan).contains("NodeIndexSeek").contains("IN [1, 2]");
      assertThat(count(query)).isEqualTo(2L);
    }

    /**
     * A negated equality must not seed the seek either.
     */
    @Test
    void negatedEqualityIsNotAnchoredOnTheIndex() {
      final String query = "MATCH (n:Bench) WHERE NOT (n.id = 1) RETURN count(n) AS c";
      final String plan = profilePlan(query);
      assertThat(plan).as("NOT must not be turned into a seek\n%s", plan).doesNotContain("NodeIndexSeek");
      assertThat(count(query)).isEqualTo(1999L);
    }

    /**
     * An equality nested in an OR, ANDed at the top level with an indexed equality: only the top-level
     * conjunct is anchorable and the OR branch must survive as a filter.
     */
    @Test
    void conjunctionOfIndexedEqualityAndDisjunction() {
      final String query = "MATCH (n:Bench) WHERE n.id = 7 AND (n.name = 'n7' OR n.name = 'n8') RETURN count(n) AS c";
      assertThat(profilePlan(query)).contains("NodeIndexSeek");
      assertThat(count(query)).isEqualTo(1L);
      assertThat(count("MATCH (n:Bench) WHERE n.id = 7 AND (n.name = 'n1' OR n.name = 'n8') RETURN count(n) AS c")).isEqualTo(0L);
    }

    private long count(final String cypher) {
      final long[] c = new long[1];
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher", cypher);
        c[0] = ((Number) rs.next().getProperty("c")).longValue();
        rs.close();
      });
      return c[0];
    }

    private String profilePlan(final String cypher) {
      final StringBuilder plan = new StringBuilder();
      database.transaction(() -> {
        final ResultSet rs = database.command("opencypher", "PROFILE " + cypher);
        while (rs.hasNext())
          rs.next();
        plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
        rs.close();
      });
      return plan.toString();
    }
  }

  /**
   * Issue #5444: the Cypher optimizer offered a composite index for a predicate on its leading
   * property, but the seek operator asked the index for a one-element key. The index is keyed by the
   * whole tuple, so the lookup found nothing and the query returned an empty result set, silently.
   * <p>
   * Every case is checked against the SQL engine over the same data, since SQL has always used
   * composite indexes correctly and is the reference for what the rows must be.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5444CompositeIndexSeek {
    private static final String DATABASE_PATH = "./target/databases/issue-5444-composite-index-seek";

    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory(DATABASE_PATH).create();

      final var product = database.getSchema().createVertexType("Product");
      product.createProperty("category", Type.STRING);
      product.createProperty("price", Type.INTEGER);
      product.createProperty("sku", Type.STRING);

      final var area = database.getSchema().createVertexType("Area");
      area.createProperty("id", Type.STRING);
      area.createProperty("zone", Type.STRING);

      database.transaction(() -> {
        newProduct("a", 10, "p1");
        newProduct("a", 20, "p2");
        newProduct("b", 10, "p3");
        for (int i = 0; i < 128; i++)
          newProduct("decoy", 1000 + i, "decoy-" + i);

        database.newVertex("Area").set("id", "country").set("zone", "EU").save();
        database.newVertex("Area").set("id", "region").set("zone", "EU").save();
      });

      database.transaction(() -> {
        // the only index on either type is composite
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Product", "category", "price");
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Area", "id", "zone");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null)
        database.drop();
    }

    @Test
    void equalityOnTheLeadingPropertyOfAUniqueCompositeIndex() {
      assertThat(cypher("MATCH (a:Area) WHERE a.id = 'country' RETURN a.id AS value"))
          .containsExactly("country");
      assertSqlParity("MATCH (a:Area) WHERE a.id = 'country' RETURN a.id AS value",
          "SELECT id AS value FROM Area WHERE id = 'country'");
    }

    @Test
    void equalityOnTheFullKeyOfAUniqueCompositeIndex() {
      assertThat(cypher("MATCH (a:Area) WHERE a.id = 'country' AND a.zone = 'EU' RETURN a.id AS value"))
          .containsExactly("country");
      assertSqlParity("MATCH (a:Area) WHERE a.id = 'country' AND a.zone = 'EU' RETURN a.id AS value",
          "SELECT id AS value FROM Area WHERE id = 'country' AND zone = 'EU'");
    }

    @Test
    void equalityOnTheLeadingPropertyOfANonUniqueCompositeIndex() {
      assertThat(cypher("MATCH (p:Product) WHERE p.category = 'a' RETURN p.sku AS value ORDER BY value"))
          .containsExactly("p1", "p2");
      assertSqlParity("MATCH (p:Product) WHERE p.category = 'a' RETURN p.sku AS value ORDER BY value",
          "SELECT sku AS value FROM Product WHERE category = 'a' ORDER BY sku");
    }

    @Test
    void equalityOnTheFullKeyOfANonUniqueCompositeIndex() {
      assertThat(cypher("MATCH (p:Product) WHERE p.category = 'a' AND p.price = 20 RETURN p.sku AS value"))
          .containsExactly("p2");
      assertSqlParity("MATCH (p:Product) WHERE p.category = 'a' AND p.price = 20 RETURN p.sku AS value",
          "SELECT sku AS value FROM Product WHERE category = 'a' AND price = 20");
    }

    @Test
    void equalityOnATrailingPropertyIsNotSeekableButStaysCorrect() {
      assertThat(cypher("MATCH (p:Product) WHERE p.price = 10 RETURN p.sku AS value ORDER BY value"))
          .containsExactly("p1", "p3");
      assertSqlParity("MATCH (p:Product) WHERE p.price = 10 RETURN p.sku AS value ORDER BY value",
          "SELECT sku AS value FROM Product WHERE price = 10 ORDER BY sku");
    }

    @Test
    void parameterizedEqualityOnTheLeadingProperty() {
      assertThat(cypher("MATCH (p:Product) WHERE p.category = $category RETURN p.sku AS value ORDER BY value",
          Map.of("category", "a"))).containsExactly("p1", "p2");
    }

    @Test
    void inListOnTheLeadingPropertyOfACompositeIndex() {
      assertThat(cypher("MATCH (p:Product) WHERE p.category IN ['a', 'b'] RETURN p.sku AS value ORDER BY value"))
          .containsExactly("p1", "p2", "p3");
      assertSqlParity("MATCH (p:Product) WHERE p.category IN ['a', 'b'] RETURN p.sku AS value ORDER BY value",
          "SELECT sku AS value FROM Product WHERE category IN ['a', 'b'] ORDER BY sku");
    }

    @Test
    void rangeOnTheLeadingPropertyOfACompositeIndex() {
      assertThat(cypher("MATCH (p:Product) WHERE p.category > 'a' AND p.category < 'c' RETURN p.sku AS value ORDER BY value"))
          .containsExactly("p3");
      assertSqlParity("MATCH (p:Product) WHERE p.category > 'a' AND p.category < 'c' RETURN p.sku AS value ORDER BY value",
          "SELECT sku AS value FROM Product WHERE category > 'a' AND category < 'c' ORDER BY sku");
    }

    @Test
    void equalityWithAValueOfAnotherTypeThanTheKeyReturnsNoRowsWithoutFailing() {
      // a cross-category comparison is never true in Cypher: the prefix seek must not push a value the
      // index key cannot represent and blow up (issue #5225 for the range-scan equivalent)
      assertThat(cypher("MATCH (p:Product) WHERE p.category = 42 RETURN p.sku AS value")).isEmpty();
      assertSqlParity("MATCH (p:Product) WHERE p.category = 42 RETURN p.sku AS value",
          "SELECT sku AS value FROM Product WHERE category = 42");
    }

    @Test
    void aSeekableCompositePredicateStillUsesTheIndex() {
      assertThat(planOf("MATCH (a:Area) WHERE a.id = 'country' RETURN a.id AS value"))
          .contains("NodeIndexSeek")
          .contains("Area[id,zone]");
      assertThat(planOf("MATCH (p:Product) WHERE p.category = 'a' AND p.price = 20 RETURN p.sku AS value"))
          .contains("NodeIndexSeek")
          .contains("Product[category,price]");
    }

    private void assertSqlParity(final String cypherQuery, final String sqlQuery) {
      assertThat(cypher(cypherQuery))
          .as("the Cypher engine must agree with SQL: %s", cypherQuery)
          .isEqualTo(values(database.query("sql", sqlQuery)));
    }

    private List<String> cypher(final String query) {
      return values(database.query("opencypher", query));
    }

    private List<String> cypher(final String query, final Map<String, Object> parameters) {
      return values(database.query("opencypher", query, parameters));
    }

    private String planOf(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
        while (resultSet.hasNext())
          resultSet.next();
        return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private static List<String> values(final ResultSet resultSet) {
      final List<String> values = new ArrayList<>();
      try (resultSet) {
        while (resultSet.hasNext())
          values.add(String.valueOf(resultSet.next().<Object>getProperty("value")));
      }
      return values;
    }

    private void newProduct(final String category, final int price, final String sku) {
      database.newVertex("Product").set("category", category).set("price", price).set("sku", sku).save();
    }
  }

  /**
   * Issue #7021: an index declared on a parent type is inherited by every child type - the schema creates a
   * sub-index for each child bucket - and SQL has always used it, planning
   * {@code FETCH FROM INDEX Node[id] / FILTER ITEMS BY TYPE Entity}. OpenCypher asked the type for
   * {@code getAllIndexes(false)}, which answers only with the indexes the type declares ITSELF, so a query on
   * the child type found no index and fell back to a full label scan. Since a child type cannot own a second
   * index on a property its parent already indexes, that left no way at all to get an index seek on the child.
   * <p>
   * The seek that replaces the scan reads a polymorphic index, so its cursor also carries the parent's own
   * records and every sibling child's: the label filter that keeps the answer right is what the SQL plan
   * spells as FILTER ITEMS BY TYPE. Every test below therefore checks the rows, not only the plan, and pins
   * them against the SQL engine over the same data.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue7021InheritedIndex {
    private static final String DATABASE_PATH = "./target/databases/issue-7021-inherited-index";

    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory(DATABASE_PATH).create();

      // Node <- Entity, and Node <- Other: a sibling child whose records live in the same polymorphic index
      // and must never leak into an Entity answer.
      database.getSchema().createVertexType("Node").createProperty("id", Type.STRING);
      database.getSchema().getType("Node").createProperty("code", Type.STRING);
      database.getSchema().createVertexType("Entity").addSuperType("Node");
      database.getSchema().createVertexType("Other").addSuperType("Node");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id");
      // Non-unique, so the same code can legitimately be carried by records of two different child types.
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Node", "code");

      // The same shape on the relationship side: LINK <- SUBLINK, with the index declared on the parent.
      database.getSchema().createEdgeType("LINK").createProperty("tag", Type.STRING);
      database.getSchema().createEdgeType("SUBLINK").addSuperType("LINK");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "LINK", "tag");

      database.transaction(() -> {
        database.newVertex("Entity").set("id", "foo-1").save();
        database.newVertex("Entity").set("id", "foo-2").save();
        // Same property, same index, different concrete types.
        database.newVertex("Node").set("id", "bar-1").save();
        database.newVertex("Other").set("id", "baz-1").set("code", "shared").save();
        for (int i = 0; i < 64; i++)
          database.newVertex("Entity").set("id", "decoy-" + i).save();
      });

      database.transaction(() -> {
        final MutableVertex from = (MutableVertex) database.lookupByKey("Entity", "id", "foo-1").next().asVertex().modify();
        final MutableVertex to = (MutableVertex) database.lookupByKey("Entity", "id", "foo-2").next().asVertex().modify();
        from.newEdge("SUBLINK", to, "tag", "sub").save();
        from.newEdge("LINK", to, "tag", "parent").save();
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null)
        database.drop();
    }

    @Test
    void anEqualityPredicateOnAChildTypeUsesTheInheritedIndex() {
      assertThat(cypher("MATCH (e:Entity) WHERE e.id = 'foo-1' RETURN e.id AS value")).containsExactly("foo-1");
      assertSqlParity("MATCH (e:Entity) WHERE e.id = 'foo-1' RETURN e.id AS value",
          "SELECT id AS value FROM Entity WHERE id = 'foo-1'");
      assertThat(planOf("MATCH (e:Entity) WHERE e.id = 'foo-1' RETURN e.id AS value"))
          .as("the inherited Node[id] index must be seekable from the child type")
          .contains("NodeIndexSeek");
    }

    @Test
    void anInlinePropertyOnAChildTypeUsesTheInheritedIndex() {
      assertThat(cypher("MATCH (e:Entity {id: 'foo-2'}) RETURN e.id AS value")).containsExactly("foo-2");
      assertThat(planOf("MATCH (e:Entity {id: 'foo-2'}) RETURN e.id AS value")).contains("NodeIndexSeek");
    }

    @Test
    void anInListPredicateOnAChildTypeUsesTheInheritedIndex() {
      assertThat(cypher("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'foo-2'] RETURN e.id AS value ORDER BY value"))
          .containsExactly("foo-1", "foo-2");
      assertSqlParity("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'foo-2'] RETURN e.id AS value ORDER BY value",
          "SELECT id AS value FROM Entity WHERE id IN ['foo-1', 'foo-2'] ORDER BY id");
      assertThat(planOf("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'foo-2'] RETURN e.id AS value")).contains("NodeIndexSeek");
    }

    @Test
    void aParameterizedInListOnAChildTypeUsesTheInheritedIndex() {
      assertThat(cypher("MATCH (e:Entity) WHERE e.id IN $ids RETURN e.id AS value ORDER BY value",
          Map.of("ids", List.of("foo-1", "foo-2")))).containsExactly("foo-1", "foo-2");
    }

    @Test
    void aSeekOnTheInheritedIndexNeverReturnsARecordOfAnotherType() {
      // bar-1 is a Node and baz-1 an Other: both are in the very index the Entity seek reads, and neither is
      // an Entity. This is the check the SQL plan spells as FILTER ITEMS BY TYPE.
      assertThat(cypher("MATCH (e:Entity) WHERE e.id = 'bar-1' RETURN e.id AS value")).isEmpty();
      assertThat(cypher("MATCH (e:Entity {id: 'bar-1'}) RETURN e.id AS value")).isEmpty();
      assertThat(cypher("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN e.id AS value ORDER BY value"))
          .containsExactly("foo-1");
      assertSqlParity("MATCH (e:Entity) WHERE e.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN e.id AS value ORDER BY value",
          "SELECT id AS value FROM Entity WHERE id IN ['foo-1', 'bar-1', 'baz-1'] ORDER BY id");
    }

    @Test
    void aSeekOnTheParentTypeStillSeesEveryChildRecord() {
      assertThat(cypher("MATCH (n:Node) WHERE n.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN n.id AS value ORDER BY value"))
          .as("the parent type is polymorphic: its own records and every child's satisfy (n:Node)")
          .containsExactly("bar-1", "baz-1", "foo-1");
      assertSqlParity("MATCH (n:Node) WHERE n.id IN ['foo-1', 'bar-1', 'baz-1'] RETURN n.id AS value ORDER BY value",
          "SELECT id AS value FROM Node WHERE id IN ['foo-1', 'bar-1', 'baz-1'] ORDER BY id");
    }

    @Test
    void aMergeOnAChildTypeUsesTheInheritedIndexWithoutMatchingAnotherType() {
      // code 'shared' is carried by an Other, not by an Entity, so MERGE must CREATE an Entity rather than
      // adopt it - which is what a polymorphic index seek without the label filter would have done.
      database.transaction(() -> query("opencypher", "MERGE (e:Entity {code: 'shared'})"));
      assertThat(cypher("MATCH (e:Entity) WHERE e.code = 'shared' RETURN e.code AS value")).containsExactly("shared");
      assertThat(cypher("MATCH (n:Node) WHERE n.code = 'shared' RETURN n.code AS value"))
          .as("the pre-existing Other and the newly merged Entity both answer (n:Node)")
          .containsExactly("shared", "shared");

      // A second MERGE finds the Entity the first one created and must not add another.
      database.transaction(() -> query("opencypher", "MERGE (e:Entity {code: 'shared'})"));
      assertThat(cypher("MATCH (e:Entity) WHERE e.code = 'shared' RETURN e.code AS value")).containsExactly("shared");
    }

    @Test
    void anEdgePredicateOnAChildEdgeTypeUsesTheInheritedIndex() {
      // A relationship pattern matches the type it names and its subtypes, never its ancestors: the LINK edge
      // tagged 'parent' lives in the very index the SUBLINK seek reads and must not answer (r:SUBLINK).
      assertThat(cypher("MATCH (a)-[r:SUBLINK]->(b) WHERE r.tag = 'sub' RETURN r.tag AS value")).containsExactly("sub");
      assertThat(cypher("MATCH (a)-[r:SUBLINK]->(b) WHERE r.tag = 'parent' RETURN r.tag AS value")).isEmpty();
      assertThat(cypher("MATCH (a)-[r:LINK]->(b) WHERE r.tag = 'sub' RETURN r.tag AS value"))
          .as("the parent edge type is polymorphic, so a SUBLINK edge answers (r:LINK)")
          .containsExactly("sub");
    }

    @Test
    void aChainedMatchOnAChildTypeUsesTheInheritedIndex() {
      // UNWIND ... MATCH routes through MatchNodeStep's WHERE-driven lookup rather than the optimizer's seek.
      assertThat(cypher("UNWIND ['foo-1', 'bar-1'] AS wanted MATCH (e:Entity) WHERE e.id = wanted RETURN e.id AS value"))
          .as("the row-driven lookup must use the inherited index and still reject the Node record")
          .containsExactly("foo-1");
    }

    private void query(final String language, final String command) {
      try (final ResultSet resultSet = database.command(language, command)) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    }

    private void assertSqlParity(final String cypherQuery, final String sqlQuery) {
      assertThat(cypher(cypherQuery))
          .as("the Cypher engine must agree with SQL: %s", cypherQuery)
          .isEqualTo(values(database.query("sql", sqlQuery)));
    }

    private List<String> cypher(final String query) {
      return values(database.query("opencypher", query));
    }

    private List<String> cypher(final String query, final Map<String, Object> parameters) {
      return values(database.query("opencypher", query, parameters));
    }

    private String planOf(final String query) {
      try (final ResultSet resultSet = database.query("opencypher", "PROFILE " + query)) {
        while (resultSet.hasNext())
          resultSet.next();
        return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private static List<String> values(final ResultSet resultSet) {
      final List<String> values = new ArrayList<>();
      try (resultSet) {
        while (resultSet.hasNext())
          values.add(String.valueOf(resultSet.next().<Object>getProperty("value")));
      }
      return values;
    }
  }

  /**
   * Issue #8333, the Cypher side: the optimizer anchors a pattern on an index range whenever a range predicate meets an
   * indexed property, with a fixed guess of how much of the label it holds. A range over most of the label was then
   * fetched one random page access per vertex. The range scan now reads the matching entries first and either loads the
   * vertices in physical order or gives way to a scan of the label, whose rows the pattern's WHERE filters the same way -
   * wherever the order the rows arrive in cannot show in the output: an aggregating RETURN, or an ORDER BY. A query
   * returning its rows as they come keeps the index order.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8333CypherIndexRangeSelectivity extends TestHelper {
    private static final int PERSONS = 1_000;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.age INTEGER");
      database.command("sql", "CREATE INDEX ON Person (age) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < PERSONS; i++)
          // Inserted in a shuffled age order, so key order and physical order differ
          database.newVertex("Person").set("id", i).set("age", (int) ((i * 7919L) % PERSONS)).save();
      });
    }

    @Test
    void nonSelectiveRangeIsServedByALabelScan() {
      final String query = "MATCH (p:Person) WHERE p.age >= 20 RETURN p.id AS id ORDER BY id";
      assertThat(ids(query, Map.of())).isEqualTo(expected(20, PERSONS));
      assertThat(profile(query, Map.of())).contains("NodeIndexRangeScan").contains("served by label scan");
    }

    @Test
    void selectiveRangeIsServedInPhysicalOrder() {
      final String query = "MATCH (p:Person) WHERE p.age >= 100 AND p.age < 150 RETURN p.id AS id ORDER BY id";
      assertThat(ids(query, Map.of())).isEqualTo(expected(100, 150));
      assertThat(profile(query, Map.of())).contains("served in physical order");
    }

    @Test
    void upperBoundAloneStopsInsideTheIndex() {
      final String query = "MATCH (p:Person) WHERE p.age < $max RETURN p.id AS id ORDER BY id";
      assertThat(ids(query, Map.of("max", 30))).isEqualTo(expected(0, 30));
      assertThat(profile(query, Map.of("max", 30))).contains("served in physical order");
      // The same plan with a bound that covers most of the label
      assertThat(ids(query, Map.of("max", 900))).isEqualTo(expected(0, 900));
      assertThat(profile(query, Map.of("max", 900))).contains("served by label scan");
    }

    @Test
    void rowsReturnedAsTheyComeKeepTheIndexOrder() {
      for (final String query : new String[] { "MATCH (p:Person) WHERE p.age >= 20 RETURN p.age AS age",
          "MATCH (p:Person) WHERE p.age >= 20 RETURN p.age AS age LIMIT 5" }) {
        int previous = -1;
        try (final ResultSet rs = database.query("opencypher", query)) {
          while (rs.hasNext()) {
            final int age = rs.next().<Integer>getProperty("age");
            assertThat(age).as(query).isGreaterThanOrEqualTo(previous);
            previous = age;
          }
        }
        assertThat(profile(query, Map.of())).as(query).contains("NodeIndexRangeScan").doesNotContain("served")
            .doesNotContain("physical order");
      }
    }

    @Test
    void anAggregationOverAWideRange() {
      // 70% of the label: above the share a label scan, which runs on one thread, gives way at
      final String query = "MATCH (p:Person) WHERE p.age > 299 RETURN sum(p.age) AS s";
      assertThat(profile(query, Map.of())).contains("served by label scan");
      try (final ResultSet rs = database.query("opencypher", query)) {
        long expected = 0;
        for (int age = 300; age < PERSONS; age++)
          expected += age;
        assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(expected);
      }
    }

    @Test
    void orderSensitiveAggregationsKeepTheIndexOrder() {
      for (final String query : new String[] { "MATCH (p:Person) WHERE p.age >= 20 RETURN collect(p.id) AS ids",
          "MATCH (p:Person) WHERE p.age >= 20 RETURN p.age % 3 AS k, count(*) AS n LIMIT 1" })
        assertThat(profile(query, Map.of())).as(query).contains("NodeIndexRangeScan").doesNotContain("served");
    }

    private TreeSet<Integer> ids(final String query, final Map<String, Object> params) {
      final TreeSet<Integer> ids = new TreeSet<>();
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          ids.add(row.<Integer>getProperty("id"));
        }
      }
      return ids;
    }

    private String profile(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    /** The ids of the persons whose age is in [from, to). */
    private static TreeSet<Integer> expected(final int from, final int to) {
      final TreeSet<Integer> ids = new TreeSet<>();
      for (int i = 0; i < PERSONS; i++) {
        final int age = (int) ((i * 7919L) % PERSONS);
        if (age >= from && age < to)
          ids.add(i);
      }
      return ids;
    }
  }

  /**
   * Issue #8723: {@code WHERE e.x = $p OR e.x = $q} over an indexed property read every vertex of the label, while the
   * equivalent {@code IN} answered from the index, and an OR across two indexed properties scanned the label where SQL
   * unions two index fetches. Both ORs are now answered from the indexes; the WHERE is still evaluated above the seek, so
   * the answers do not change.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8723CypherOrIndexSeek extends TestHelper {
    private static final int VERTICES = 2_000;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE E");
      database.command("sql", "CREATE PROPERTY E.id LONG");
      database.command("sql", "CREATE PROPERTY E.x INTEGER");
      database.command("sql", "CREATE PROPERTY E.s STRING");
      database.command("sql", "CREATE PROPERTY E.u STRING");
      database.command("sql", "CREATE INDEX ON E (x) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON E (s) NOTUNIQUE");
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          database.newVertex("E").set("id", (long) i).set("x", i % 500).set("s", "s" + (i % 400)).set("u", "u" + (i % 7)).save();
      });
    }

    @Test
    void orOfEqualitiesOnOnePropertyUsesTheIndex() {
      final String query = "MATCH (e:E) WHERE e.x = 3 OR e.x = 7 RETURN e.id AS id ORDER BY id";
      assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 3 || i % 500 == 7));
      assertThat(explain(query)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    }

    @Test
    void orOfEqualitiesWithParametersUsesTheIndex() {
      final String query = "MATCH (e:E) WHERE e.x = $p OR e.x = $q RETURN count(*) AS n";
      assertThat(column(query, Map.of("p", 3, "q", 7))).containsExactly((long) expectedIds(i -> i % 500 == 3 || i % 500 == 7).size());
      assertThat(explain(query)).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    }

    @Test
    void orOfTwoIndexedPropertiesIsAUnionOfSeeks() {
      final String query = "MATCH (e:E) WHERE e.x = 3 OR e.s = 's10' RETURN e.id AS id ORDER BY id";
      assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 3 || i % 400 == 10));
      assertThat(explain(query)).contains("NodeIndexUnionSeek").doesNotContain("NodeByLabelScan");
    }

    @Test
    void aVertexThatSatisfiesBothDisjunctsIsReturnedOnce() {
      // id 1003: x = 3 and s = 's203'; s 's203' also holds id 203 (x = 203)
      final String query = "MATCH (e:E) WHERE e.x = 3 OR e.s = $s RETURN count(*) AS n";
      final long expected = expectedIds(i -> i % 500 == 3 || i % 400 == 3).size();
      assertThat(column(query, Map.of("s", "s3"))).containsExactly(expected);
      assertThat(explain(query)).contains("NodeIndexUnionSeek");
    }

    @Test
    void anInListDisjunctAndAnAndDisjunct() {
      final String query = "MATCH (e:E) WHERE e.x IN [1, 2] OR (e.s = 's9' AND e.u = 'u2') RETURN e.id AS id ORDER BY id";
      assertThat(column(query, Map.of()))
          .containsExactlyElementsOf(expectedIds(i -> i % 500 == 1 || i % 500 == 2 || (i % 400 == 9 && i % 7 == 2)));
      assertThat(explain(query)).contains("NodeIndexUnionSeek");
    }

    @Test
    void orNestedUnderAndKeepsTheOtherConjunct() {
      final String query = "MATCH (e:E) WHERE e.id > 1000 AND (e.x = 3 OR e.s = 's10') RETURN e.id AS id ORDER BY id";
      assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i > 1000 && (i % 500 == 3 || i % 400 == 10)));
    }

    @Test
    void aDisjunctWithoutIndexKeepsTheScan() {
      final String query = "MATCH (e:E) WHERE e.x = 3 OR e.u = 'u1' RETURN e.id AS id ORDER BY id";
      assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 3 || i % 7 == 1));
      assertThat(explain(query)).contains("NodeByLabelScan").doesNotContain("NodeIndexUnionSeek");
    }

    @Test
    void aNullDisjunctDoesNotMatchAnything() {
      final String query = "MATCH (e:E) WHERE e.x = null OR e.x = 5 RETURN e.id AS id ORDER BY id";
      assertThat(column(query, Map.of())).containsExactlyElementsOf(expectedIds(i -> i % 500 == 5));
    }

    @Test
    void parametersOfAnotherNumericTypeStillMatch() {
      final String query = "MATCH (e:E) WHERE e.x = $p OR e.s = $q RETURN e.id AS id ORDER BY id";
      final List<Object> expected = expectedIds(i -> i % 500 == 3 || i % 400 == 10);
      assertThat(column(query, Map.of("p", 3L, "q", "s10"))).containsExactlyElementsOf(expected);
      assertThat(column(query, Map.of("p", 3.0, "q", "s10"))).containsExactlyElementsOf(expected);
      assertThat(column("MATCH (e:E) WHERE e.x = $p OR e.x = $q RETURN e.id AS id ORDER BY id", Map.of("p", 3.0, "q", 7L)))
          .containsExactlyElementsOf(expectedIds(i -> i % 500 == 3 || i % 500 == 7));
    }

    @Test
    void theWhereOnASecondMatchOrAnOptionalMatchStillSeeks() {
      final List<Object> expected = expectedIds(i -> i % 500 == 3 || i % 400 == 10);
      assertThat(column("MATCH (e:E) MATCH (e) WHERE e.x = 3 OR e.s = 's10' RETURN e.id AS id ORDER BY id", Map.of()))
          .containsExactlyElementsOf(expected);
      assertThat(column("OPTIONAL MATCH (e:E) WHERE e.x = 3 OR e.s = 's10' RETURN e.id AS id ORDER BY id", Map.of()))
          .containsExactlyElementsOf(expected);
    }

    @Test
    void aListParameterDisjunctAndAScalarParameterDisjunct() {
      final String query = "MATCH (e:E) WHERE e.x IN $xs OR e.s = $s RETURN e.id AS id ORDER BY id";
      assertThat(column(query, Map.of("xs", List.of(1, 2, 3), "s", "s10")))
          .containsExactlyElementsOf(expectedIds(i -> i % 500 == 1 || i % 500 == 2 || i % 500 == 3 || i % 400 == 10));
    }

    @Test
    void aCollectionParameterOfAnEqualityIsComparedWholeNotSplitIntoKeys() {
      for (final String type : new String[] { "Tagged", "TaggedPlain" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".tags LIST");
        database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
      }
      database.command("sql", "CREATE INDEX ON Tagged (tags) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Tagged (s) NOTUNIQUE");
      database.transaction(() -> {
        for (final String type : new String[] { "Tagged", "TaggedPlain" }) {
          database.newVertex(type).set("tags", List.of(1, 2)).set("s", "a").save();
          database.newVertex(type).set("tags", List.of(1)).set("s", "b").save();
          database.newVertex(type).set("tags", List.of(2, 1)).set("s", "c").save();
        }
      });
      final Map<String, Object> params = Map.of("p", List.of(1, 2), "q", "c");
      final List<Object> plain = column("MATCH (t:TaggedPlain) WHERE t.tags = $p OR t.s = $q RETURN t.s AS s ORDER BY s", params);
      assertThat(column("MATCH (t:Tagged) WHERE t.tags = $p OR t.s = $q RETURN t.s AS s ORDER BY s", params)).isEqualTo(plain);
    }

    @Test
    void aCompositeIndexAndASubTypeAreSeekedByAUnion() {
      database.command("sql", "CREATE VERTEX TYPE Comp");
      database.command("sql", "CREATE PROPERTY Comp.a INTEGER");
      database.command("sql", "CREATE PROPERTY Comp.b INTEGER");
      database.command("sql", "CREATE PROPERTY Comp.c STRING");
      database.command("sql", "CREATE INDEX ON Comp (a, b) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Comp (c) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE CompChild EXTENDS Comp");
      database.transaction(() -> {
        for (int i = 0; i < 300; i++)
          database.newVertex(i % 3 == 0 ? "CompChild" : "Comp").set("a", i % 10, "b", i % 7, "c", "c" + (i % 50)).save();
      });
      final String query = "MATCH (n:Comp) WHERE n.a = 3 OR n.c = 'c4' RETURN count(*) AS n";
      long expected = 0;
      for (int i = 0; i < 300; i++)
        if (i % 10 == 3 || i % 50 == 4)
          expected++;
      assertThat(column(query, Map.of())).containsExactly(expected);
      assertThat(explain(query)).contains("NodeIndexUnionSeek");
    }

    @Test
    void anOrOnOtherVariableDoesNotSeekTheAnchor() {
      final String query = "MATCH (e:E), (f:E) WHERE e.x = 3 OR f.x = 4 RETURN count(*) AS n";
      final long expected = (long) expectedIds(i -> i % 500 == 3).size() * VERTICES + (long) VERTICES * expectedIds(i -> i % 500 == 4).size()
          - (long) expectedIds(i -> i % 500 == 3).size() * expectedIds(i -> i % 500 == 4).size();
      assertThat(column(query, Map.of())).containsExactly(expected);
    }

    @Test
    void explainOfTheInSeekPrintsTheValues() {
      final String plan = explain("MATCH (e:E) WHERE e.x IN [1, 2] RETURN e.id");
      assertThat(plan).contains("IN [1, 2]").doesNotContain("LiteralExpression@");
    }

    private static List<Object> expectedIds(final IntPredicate predicate) {
      final List<Object> ids = new ArrayList<>();
      for (int i = 0; i < VERTICES; i++)
        if (predicate.test(i))
          ids.add((long) i);
      return ids;
    }

    private List<Object> column(final String query, final Map<String, Object> params) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        while (rs.hasNext())
          values.add(new HashMap<>(rs.next().toMap()).values().iterator().next());
      }
      return values;
    }

    private String explain(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, Map.of())) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }
  }

  /**
   * Regression guards for #8834 (a range on a parent type's index under a subtype label returned the parent's and siblings'
   * vertices) and #8835 (a range on a property with only a hash index threw instead of scanning).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8834Issue8835RangeIndex extends TestHelper {

    private List<Long> longs(final String query) {
      final List<Long> got = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          got.add(rs.next().<Number>getProperty("v").longValue());
      }
      return got;
    }

    @Test
    void subtypeRangeOnParentIndexOnlyReturnsTheSubtype() {
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE VERTEX TYPE Q EXTENDS P");
      database.command("sql", "CREATE VERTEX TYPE R EXTENDS P");
      database.command("sql", "CREATE PROPERTY P.a LONG");
      database.command("sql", "CREATE INDEX ON P (a) NOTUNIQUE");
      database.transaction(() -> {
        database.newVertex("P").set("a", 1000L).save();
        database.newVertex("Q").set("a", -1L).save();
        database.newVertex("R").set("a", 2000L).save();
      });
      assertThat(longs("MATCH (n:Q) WHERE n.a > 500 RETURN n.a AS v")).isEmpty();
      assertThat(longs("MATCH (n:R) WHERE n.a > 500 RETURN n.a AS v")).containsExactly(2000L);
      assertThat(longs("MATCH (n:Q) WHERE n.a < 500 RETURN n.a AS v")).containsExactly(-1L);
      assertThat(longs("MATCH (n:P) WHERE n.a > 500 RETURN n.a AS v ORDER BY v")).containsExactly(1000L, 2000L);
    }

    @Test
    void rangeOnHashIndexFallsBackToTheLabelScan() {
      database.command("sql", "CREATE VERTEX TYPE H");
      database.command("sql", "CREATE PROPERTY H.h LONG");
      database.command("sql", "CREATE PROPERTY H.u LONG");
      database.command("sql", "CREATE INDEX ON H (h) NOTUNIQUE_HASH");
      database.command("sql", "CREATE INDEX ON H (u) UNIQUE_HASH");
      database.transaction(() -> {
        for (long i = 0; i < 10; i++)
          database.newVertex("H").set("h", i, "u", i).save();
      });
      assertThat(longs("MATCH (n:H) WHERE n.h > 7 RETURN n.h AS v")).containsExactlyInAnyOrder(8L, 9L);
      assertThat(longs("MATCH (n:H) WHERE n.u <= 1 RETURN n.u AS v")).containsExactlyInAnyOrder(0L, 1L);
      assertThat(longs("MATCH (n:H) WHERE n.h = 7 RETURN n.h AS v")).containsExactly(7L);
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (n:H) WHERE n.h > 7 RETURN n.h AS v")) {
        assertThat(rs.next().toJSON().toString()).doesNotContain("NodeIndexRangeScan");
      }
    }

    @Test
    void subtypeRangeOnParentIndexHoldingNullKeysIsFilteredByLabel() {
      database.command("sql", "CREATE VERTEX TYPE NP");
      database.command("sql", "CREATE VERTEX TYPE NQ EXTENDS NP");
      database.command("sql", "CREATE VERTEX TYPE NR EXTENDS NP");
      database.command("sql", "CREATE PROPERTY NP.a LONG");
      database.command("sql", "CREATE INDEX ON NP (a) NOTUNIQUE NULL_STRATEGY INDEX");
      database.transaction(() -> {
        database.newVertex("NP").set("a", 10L).save();
        database.newVertex("NP").save();
        database.newVertex("NQ").set("a", 20L).save();
        database.newVertex("NQ").save();
        database.newVertex("NR").set("a", 30L).save();
        database.newVertex("NR").save();
      });
      assertThat(longs("MATCH (n:NQ) WHERE n.a > 5 RETURN n.a AS v ORDER BY v")).containsExactly(20L);
      assertThat(longs("MATCH (n:NQ) WHERE n.a < 25 RETURN n.a AS v ORDER BY v")).containsExactly(20L);
      assertThat(longs("MATCH (n:NR) WHERE n.a >= 0 RETURN n.a AS v")).containsExactly(30L);
      assertThat(longs("MATCH (n:NQ) WHERE n.a > 5 RETURN n.a AS v ORDER BY v DESC")).containsExactly(20L);
    }

    @Test
    void rangeUnderAMidLevelLabelOfADeeperHierarchy() {
      database.command("sql", "CREATE VERTEX TYPE GP");
      database.command("sql", "CREATE VERTEX TYPE GM EXTENDS GP");
      database.command("sql", "CREATE VERTEX TYPE GC EXTENDS GM");
      database.command("sql", "CREATE VERTEX TYPE GS EXTENDS GP");
      database.command("sql", "CREATE PROPERTY GP.a LONG");
      database.command("sql", "CREATE INDEX ON GP (a) NOTUNIQUE");
      database.transaction(() -> {
        database.newVertex("GP").set("a", 10L).save();
        database.newVertex("GM").set("a", 20L).save();
        database.newVertex("GC").set("a", 30L).save();
        database.newVertex("GS").set("a", 40L).save();
      });
      assertThat(longs("MATCH (n:GM) WHERE n.a > 0 RETURN n.a AS v ORDER BY v")).containsExactly(20L, 30L);
      assertThat(longs("MATCH (n:GC) WHERE n.a > 0 RETURN n.a AS v")).containsExactly(30L);
    }
  }

  /**
   * Issue #9236: an equality on the first property of a composite HASH index made the seek read a prefix range of an index that
   * cannot be read in key order, and threw "does not support ordered iterations".
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9236CompositeHashPrefix extends TestHelper {

    private List<Long> ps(final String query, final Map<String, Object> params) {
      final List<Long> got = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        while (rs.hasNext())
          got.add(rs.next().<Number>getProperty("p").longValue());
      }
      return got;
    }

    private void create(final String type, final String indexKind) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".p INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".q INTEGER");
      database.command("sql", "CREATE INDEX ON " + type + " (p, q) " + indexKind);
      database.transaction(() -> {
        database.newVertex(type).set("p", 1, "q", 5).save();
        database.newVertex(type).set("p", 2, "q", 6).save();
      });
    }

    @Test
    void prefixOfCompositeHashIndexFallsBackToTheScan() {
      for (final String[] t : new String[][] { { "HashT", "NOTUNIQUE_HASH" }, { "UniqueHashT", "UNIQUE_HASH" } }) {
        create(t[0], t[1]);
        assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p = 1 RETURN n.p AS p", Map.of())).containsExactly(1L);
        assertThat(ps("MATCH (n:" + t[0] + " {p: 1}) RETURN n.p AS p", Map.of())).containsExactly(1L);
        assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p IN [1, 2] RETURN n.p AS p ORDER BY p", Map.of())).containsExactly(1L, 2L);
        assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p = 1 OR n.p = 2 RETURN n.p AS p ORDER BY p", Map.of())).containsExactly(1L, 2L);
        assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p = 1 AND n.q = 5 RETURN n.p AS p", Map.of())).containsExactly(1L);
      }
    }

    @Test
    void everyColumnPinnedStillSeeksTheCompositeHashIndex() {
      create("HashAll", "NOTUNIQUE_HASH");
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (n:HashAll) WHERE n.p = 1 AND n.q = 5 RETURN n.p AS p")) {
        assertThat(rs.next().toJSON().toString()).contains("NodeIndexSeek");
      }
    }

    @Test
    void inListAndOrOverAHashIndexKeepTheIndexSeek() {
      database.command("sql", "CREATE VERTEX TYPE HashOne");
      database.command("sql", "CREATE PROPERTY HashOne.p INTEGER");
      database.command("sql", "CREATE INDEX ON HashOne (p) UNIQUE_HASH");
      database.transaction(() -> {
        database.newVertex("HashOne").set("p", 1).save();
        database.newVertex("HashOne").set("p", 2).save();
        database.newVertex("HashOne").set("p", 3).save();
      });
      for (final String where : new String[] { "n.p IN [1, 2]", "n.p = 1 OR n.p = 2" }) {
        final String query = "MATCH (n:HashOne) WHERE " + where + " RETURN n.p AS p ORDER BY p";
        assertThat(ps(query, Map.of())).as(where).containsExactly(1L, 2L);
        try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
          assertThat(rs.next().toJSON().toString()).as(where).contains("NodeIndexSeek");
        }
      }

      create("HashPair", "NOTUNIQUE_HASH");
      final String pair = "MATCH (n:HashPair) WHERE n.p IN [1, 2] AND n.q = 5 RETURN n.p AS p ORDER BY p";
      assertThat(ps(pair, Map.of())).containsExactly(1L);
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + pair)) {
        assertThat(rs.next().toJSON().toString()).contains("NodeIndexSeek");
      }
    }

    @Test
    void nullParameterForTheSecondPropertyMatchesNothing() {
      create("HashN", "NOTUNIQUE_HASH");
      final Map<String, Object> params = new HashMap<>();
      params.put("p", 1);
      params.put("q", null);
      assertThat(ps("MATCH (n:HashN) WHERE n.p = $p AND n.q = $q RETURN n.p AS p", params)).isEmpty();
    }

    @Test
    void nullParameterForTheSecondPropertyOfAUniqueHashIndexMatchesNothing() {
      create("UniqueHashN", "UNIQUE_HASH");
      final Map<String, Object> params = new HashMap<>();
      params.put("p", 1);
      params.put("q", null);
      assertThat(ps("MATCH (n:UniqueHashN) WHERE n.p = $p AND n.q = $q RETURN n.p AS p", params)).isEmpty();
      assertThat(ps("MATCH (n:UniqueHashN) WHERE n.p = $p RETURN n.p AS p", params)).containsExactly(1L);
    }

    @Test
    void nullParameterForTheSecondPropertyOfAnOrderedCompositeIndexMatchesNothing() {
      create("LsmN", "NOTUNIQUE");
      final Map<String, Object> params = new HashMap<>();
      params.put("p", 1);
      params.put("q", null);
      assertThat(ps("MATCH (n:LsmN) WHERE n.p = $p AND n.q = $q RETURN n.p AS p", params)).isEmpty();
    }

    @Test
    void orderedCompositeIndexStillAnswersAPrefix() {
      create("LsmT", "NOTUNIQUE");
      assertThat(ps("MATCH (n:LsmT) WHERE n.p = 1 RETURN n.p AS p", Map.of())).containsExactly(1L);
      assertThat(ps("MATCH (n:LsmT) WHERE n.p IN [1, 2] RETURN n.p AS p ORDER BY p", Map.of())).containsExactly(1L, 2L);
    }
  }

  /**
   * Issue #9397: the point-lookup fix of #9369 covered the SQL path only. An openCypher {@code MATCH (n:T) WHERE n.k IN $ids}
   * and {@code Database.lookupByKey()} still lost, doubled or failed on a key that one transaction deletes and re-creates, though
   * the key exists in every committed state: the index entry and the record were read at different commits, and the keys of an
   * {@code IN} list were read one after the other, so a record slot freed by one key and taken by another was served for both.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9397TornCommitIndexLookup extends TestHelper {
    private static final int FIRST_ID = 200;
    private static final int IDS      = 50;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Product.views INTEGER");
    }

    @Test
    @Tag("slow")
    void uniqueHashLookupNeverLosesAKeyOfAnAtomicReplace() throws Exception {
      run("UNIQUE_HASH");
    }

    @Test
    @Tag("slow")
    void uniqueLookupNeverLosesAKeyOfAnAtomicReplace() throws Exception {
      run("UNIQUE");
    }

    @Test
    @Tag("slow")
    void notUniqueLookupNeverLosesAKeyOfAnAtomicReplace() throws Exception {
      run("NOTUNIQUE");
    }

    @Test
    void inListOfCypherAnswersEveryKeyOnce() {
      database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
      fill(1000);
      final List<Long> ids = ids();
      for (int run = 0; run < 3; run++) {
        final List<Integer> pids = new ArrayList<>();
        try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN $ids RETURN n.pid AS pid ORDER BY pid",
            Map.of("ids", ids))) {
          rs.forEachRemaining(r -> pids.add(r.<Number>getProperty("pid").intValue()));
        }
        assertThat(pids).as("run " + run).hasSize(IDS).doesNotHaveDuplicates().first().isEqualTo(FIRST_ID);
      }
    }

    @Test
    void largeInListIsServedInFull() {
      database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
      final int count = 1500;
      fill(count);
      final List<Long> ids = new ArrayList<>();
      for (long i = 0; i < count; i++)
        ids.add(i);
      int rows = 0;
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN $ids RETURN n.pid AS pid", Map.of("ids", ids))) {
        while (rs.hasNext()) {
          rs.next();
          rows++;
        }
      }
      assertThat(rows).isEqualTo(count);
    }

    @Test
    void lookupByKeyReturnsTheRecordOfTheKey() {
      database.command("sql", "CREATE INDEX ON Product (pid) NOTUNIQUE");
      fill(100);
      database.transaction(() -> database.command("sql", "CREATE VERTEX Product SET pid = 7, views = 5").close());

      final IndexCursor cursor = database.lookupByKey("Product", "pid", 7L);
      int found = 0;
      while (cursor.hasNext()) {
        final Identifiable record = cursor.next();
        assertThat(record.asVertex().getInteger("pid")).isEqualTo(7);
        found++;
      }
      assertThat(found).isEqualTo(2);
      assertThat(database.lookupByKey("Product", "pid", 99_999L).hasNext()).isFalse();
    }

    @Test
    void keyDeletedInTheSameTransactionIsNotFound() {
      database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
      fill(10);
      database.begin();
      try {
        database.command("sql", "DELETE FROM Product WHERE pid = 3").close();
        assertThat(database.lookupByKey("Product", "pid", 3L).hasNext()).isFalse();
        assertThat(database.lookupByKey("Product", "pid", 4L).hasNext()).isTrue();
      } finally {
        database.rollback();
      }
      assertThat(database.lookupByKey("Product", "pid", 3L).hasNext()).isTrue();
    }

    @Test
    void repeatableReadTransactionStillLooksKeysUp() {
      database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
      fill(100);
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        assertThat(database.lookupByKey("Product", "pid", 5L).hasNext()).isTrue();
        int rows = 0;
        try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN [1, 2, 3] RETURN n.pid AS pid")) {
          while (rs.hasNext()) {
            rs.next();
            rows++;
          }
        }
        assertThat(rows).isEqualTo(3);
      } finally {
        database.rollback();
      }
    }

    private void fill(final int count) {
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 100", Map.of("p", (long) i)).close();
      });
    }

    private static List<Long> ids() {
      final List<Long> ids = new ArrayList<>();
      for (long i = FIRST_ID; i < FIRST_ID + IDS; i++)
        ids.add(i);
      return ids;
    }

    private void run(final String indexKind) throws Exception {
      database.command("sql", "CREATE INDEX ON Product (pid) " + indexKind);
      fill(1000);
      final List<Long> ids = ids();

      final String[] modes = { "Cypher outside any transaction", "Cypher inside a transaction", "lookupByKey" };
      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicLong[] reads = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
      final AtomicLong[] wrong = { new AtomicLong(), new AtomicLong(), new AtomicLong() };
      final AtomicReference<Throwable> crashed = new AtomicReference<>();
      final Thread[] readers = new Thread[3];
      for (int m = 0; m < 3; m++) {
        final int mode = m;
        readers[m] = new Thread(() -> {
          try {
            while (!stop.get()) {
              if (mode == 2) {
                for (final long x : ids) {
                  int found = 0;
                  try {
                    final IndexCursor cursor = database.lookupByKey("Product", "pid", x);
                    while (cursor.hasNext()) {
                      cursor.next().asDocument();
                      found++;
                    }
                  } catch (final RuntimeException e) {
                    found = -1;
                  }
                  reads[mode].incrementAndGet();
                  if (found != 1)
                    wrong[mode].incrementAndGet();
                }
                continue;
              }
              if (mode == 1)
                database.begin();
              int rows = 0;
              try (final ResultSet rs = database.query("opencypher", "MATCH (n:Product) WHERE n.pid IN $ids RETURN n.pid AS pid",
                  Map.of("ids", ids))) {
                while (rs.hasNext()) {
                  rs.next();
                  rows++;
                }
              } finally {
                if (mode == 1)
                  database.rollback();
              }
              reads[mode].incrementAndGet();
              if (rows != IDS)
                wrong[mode].incrementAndGet();
            }
          } catch (final Throwable e) {
            // a crashed reader must fail the test, not leave its counters at zero
            crashed.set(e);
          }
        }, "reader-" + mode);
        readers[m].setDaemon(true);
        readers[m].start();
      }

      final long deadline = System.currentTimeMillis() + 8_000;
      long commits = 0;
      while (System.currentTimeMillis() < deadline) {
        final long x = FIRST_ID + commits % IDS;
        database.begin();
        database.command("sql", "DELETE FROM Product WHERE pid = :p", Map.of("p", x)).close();
        database.command("sql", "CREATE VERTEX Product SET pid = :p, views = 100", Map.of("p", x)).close();
        database.commit();
        commits++;
      }
      stop.set(true);
      for (final Thread t : readers)
        t.join();

      assertThat(crashed.get()).as("a reader crashed").isNull();
      final StringBuilder report = new StringBuilder();
      for (int m = 0; m < 3; m++)
        if (wrong[m].get() > 0)
          report.append(indexKind).append(", ").append(modes[m]).append(": ").append(wrong[m]).append(" of ").append(reads[m])
              .append(" reads were wrong; ");
      assertThat(report.toString()).isEmpty();
    }
  }

  /**
   * Issue #10380: a BINARY or ARRAY_OF_* value is a Java array, which has the identity {@code equals()} / {@code hashCode()}, so
   * openCypher {@code DISTINCT}, {@code GROUP BY}, {@code min()} and {@code max()} kept two records holding the very bytes
   * {@code a.b = c.b} reports equal as two values, and {@code min()} / {@code max()} answered whichever row came first. The four
   * mechanisms (equality, distinct / group key, ordering of {@code ORDER BY}, ordering of {@code min()} / {@code max()}) answer one
   * rule now.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10380ArrayValuesAsKeys extends TestHelper {

    @Test
    void binaryValuesWithTheSameBytesAreOneDistinctValueAndOneGroup() {
      createType("T", Type.BINARY);
      insert("T", "x", new byte[] { 1, 2, 3 });
      insert("T", "y", new byte[] { 1, 2, 3 });
      insert("T", "z", new byte[] { 9 });

      assertThat(rows("MATCH (n:T) RETURN DISTINCT n.p AS k")).hasSize(2);
      assertThat(scalar("MATCH (n:T) RETURN count(DISTINCT n.p) AS v")).isEqualTo(2L);
      assertThat(scalar("MATCH (n:T) RETURN size(collect(DISTINCT n.p)) AS v")).isEqualTo(2L);
      assertThat(counts("MATCH (n:T) RETURN n.p AS k, count(*) AS v")).containsExactlyInAnyOrder(2L, 1L);
      assertThat(rows("MATCH (n:T) WITH n.p AS k, collect(n.s) AS s RETURN k, s")).hasSize(2);
      assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v")).isEqualTo(true);

      // SQL, the control: the same data, the same answers
      assertThat(sqlRows("SELECT DISTINCT p FROM T")).isEqualTo(2);
      assertThat(sqlRows("SELECT p, count(*) FROM T GROUP BY p")).isEqualTo(2);
    }

    @Test
    void unionDeduplicatesBinaryValues() {
      createType("T", Type.BINARY);
      insert("T", "x", new byte[] { 1, 2, 3 });
      insert("T", "y", new byte[] { 1, 2, 3 });

      assertThat(rows("MATCH (n:T) WHERE n.s = 'x' RETURN n.p AS k UNION MATCH (n:T) WHERE n.s = 'y' RETURN n.p AS k")).hasSize(1);
    }

    @Test
    void aBinaryValueInsideAListHasTheSameKey() {
      createType("T", Type.BINARY);
      insert("T", "x", new byte[] { 1, 2, 3 });
      insert("T", "y", new byte[] { 1, 2, 3 });
      insert("T", "z", new byte[] { 1, 2, 4 });

      assertThat(rows("MATCH (n:T) RETURN DISTINCT [n.p] AS k")).hasSize(2);
      assertThat(rows("MATCH (n:T) RETURN DISTINCT {b: n.p} AS k")).hasSize(2);
    }

    @Test
    void binaryKeysOfDifferentBytesStayApart() {
      createType("T", Type.BINARY);
      // same length, same sum, different content: a key that rendered only the length or a checksum would merge them
      insert("T", "x", new byte[] { 1, 2, 3 });
      insert("T", "y", new byte[] { 3, 2, 1 });
      insert("T", "z", new byte[] { 2, 2, 2 });

      assertThat(rows("MATCH (n:T) RETURN DISTINCT n.p AS k")).hasSize(3);
      assertThat(counts("MATCH (n:T) RETURN n.p AS k, count(*) AS v")).containsExactlyInAnyOrder(1L, 1L, 1L);
    }

    @Test
    void minAndMaxOfBinaryDoNotDependOnTheInsertionOrder() {
      createType("A", Type.BINARY);
      createType("B", Type.BINARY);
      insert("A", "first", new byte[] { 1, 2, 3 });
      insert("A", "second", new byte[] { 9 });
      insert("B", "first", new byte[] { 9 });
      insert("B", "second", new byte[] { 1, 2, 3 });

      for (final String type : new String[] { "A", "B" }) {
        final Result row = single("MATCH (n:" + type + ") RETURN min(n.p) AS lo, max(n.p) AS hi");
        assertThat(row.<byte[]>getProperty("lo")).containsExactly(1, 2, 3);
        assertThat(row.<byte[]>getProperty("hi")).containsExactly(9);

        // coll.min / coll.max take the same ordering
        final Result coll = single("MATCH (n:" + type + ") WITH collect(n.p) AS bs RETURN coll.min(bs) AS lo, coll.max(bs) AS hi");
        assertThat(coll.<byte[]>getProperty("lo")).containsExactly(1, 2, 3);
        assertThat(coll.<byte[]>getProperty("hi")).containsExactly(9);

        // and ORDER BY, which already did
        final List<Result> ordered = rows("MATCH (n:" + type + ") RETURN n.p AS k ORDER BY k");
        assertThat(ordered.get(0).<byte[]>getProperty("k")).containsExactly(1, 2, 3);
        assertThat(ordered.get(1).<byte[]>getProperty("k")).containsExactly(9);
      }
    }

    @Test
    void binaryIsOrderedUnsignedByMinAndMax() {
      createType("T", Type.BINARY);
      insert("T", "a", new byte[] { 0x7f });
      insert("T", "b", new byte[] { (byte) 0x80 });
      insert("T", "c", new byte[] {});

      final Result row = single("MATCH (n:T) RETURN min(n.p) AS lo, max(n.p) AS hi");
      assertThat(row.<byte[]>getProperty("lo")).isEmpty();
      assertThat(row.<byte[]>getProperty("hi")).containsExactly((byte) 0x80);
    }

    @Test
    void binaryIsOrderedUnsignedByOrderByToo() {
      createType("T", Type.BINARY);
      insert("T", "a", new byte[] { 0x7f });
      insert("T", "b", new byte[] { (byte) 0x80 });
      insert("T", "c", new byte[] {});

      final List<Result> ordered = rows("MATCH (n:T) RETURN n.s AS s ORDER BY n.p");
      assertThat(ordered.stream().map(r -> r.<String>getProperty("s")).toList()).containsExactly("c", "a", "b");
    }

    @Test
    void twoEmbeddedDocumentsCompareByContentUnderEquals() {
      // the comparison failed with a NullPointerException: an embedded document has no RID
      database.getSchema().createDocumentType("Emb");
      createType("T", Type.EMBEDDED);
      insert("T", "x", new HashMap<>(Map.of("@type", "Emb", "k", 1)));
      insert("T", "y", new HashMap<>(Map.of("@type", "Emb", "k", 1)));
      insert("T", "z", new HashMap<>(Map.of("@type", "Emb", "k", 2)));

      assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'z' RETURN a.p = c.p AS v")).isEqualTo(false);
      assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'z' RETURN a.p <> c.p AS v")).isEqualTo(true);
      // an embedded document is never a record
      assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c AS v")).isEqualTo(false);
    }

    @Test
    void aBinaryValueIsNotAListAndComparesByItsBytesUnderTheOperators() {
      createType("T", Type.BINARY);
      insert("T", "a", new byte[] { 1, 2 });
      insert("T", "b", new byte[] { 1, 2 });
      insert("T", "c", new byte[] { 0x7f });
      insert("T", "d", new byte[] { (byte) 0x80 });

      // the bytes of a BINARY value are not the elements of a list, so DISTINCT, GROUP BY and = answer one rule
      assertThat(scalar("MATCH (n:T) WHERE n.s = 'a' RETURN n.p = [1, 2] AS v")).isEqualTo(false);
      assertThat(scalar("MATCH (n:T) WHERE n.s = 'a' RETURN n.p <> [1, 2] AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'a' AND b.s = 'b' RETURN a.p = b.p AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'a' AND b.s = 'b' RETURN a.p <> b.p AS v")).isEqualTo(false);
      // and they order unsigned, as ORDER BY and SQL do: 0x7f < 0x80, where the signed bytes said the opposite
      assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'c' AND b.s = 'd' RETURN a.p < b.p AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (a:T), (b:T) WHERE a.s = 'c' AND b.s = 'd' RETURN a.p > b.p AS v")).isEqualTo(false);
      assertThat(scalar("MATCH (n:T) WHERE n.s = 'a' RETURN n.p IN [[1, 2]] AS v")).isEqualTo(false);
      assertThat(rows("MATCH (n:T) WHERE n.p = $bytes RETURN n.s AS s", Map.of("bytes", new byte[] { 1, 2 }))).hasSize(2);
    }

    @Test
    void floatAndDoubleArraysOfTheSameNumbersShareAKey() {
      createType("A", Type.ARRAY_OF_FLOATS);
      createType("B", Type.ARRAY_OF_DOUBLES);
      insert("A", "x", new float[] { 1.0f, -0.0f });
      insert("B", "y", new double[] { 1.0d, 0.0d });

      assertThat(scalar("MATCH (a:A), (b:B) RETURN count(DISTINCT [a.p, b.p]) AS v")).isEqualTo(1L);
      assertThat(scalar("MATCH (a:A), (b:B) RETURN a.p = b.p AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (n) WHERE n:A OR n:B RETURN count(DISTINCT n.p) AS v")).isEqualTo(1L);
    }

    @Test
    void aFloatKeysByItsDecimalFormAsEqualsReadsIt() {
      // 0.1f is 0.10000000149011612 as a double, and equal to 0.1 for =, so it is one value for DISTINCT and GROUP BY too
      createType("A", Type.ARRAY_OF_FLOATS);
      createType("B", Type.ARRAY_OF_DOUBLES);
      insert("A", "x", new float[] { 0.1f, 2.5f });
      insert("B", "y", new double[] { 0.1d, 2.5d });

      assertThat(scalar("MATCH (a:A), (b:B) RETURN a.p = b.p AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (n) WHERE n:A OR n:B RETURN count(DISTINCT n.p) AS v")).isEqualTo(1L);

      createType("S", Type.FLOAT);
      createType("D", Type.DOUBLE);
      insert("S", "x", 0.1f);
      insert("D", "y", 0.1d);
      assertThat(scalar("MATCH (a:S), (b:D) RETURN a.p = b.p AS v")).isEqualTo(true);
      assertThat(scalar("MATCH (n) WHERE n:S OR n:D RETURN count(DISTINCT n.p) AS v")).isEqualTo(1L);
      assertThat(counts("MATCH (n) WHERE n:S OR n:D RETURN n.p AS k, count(*) AS v")).containsExactly(2L);
      assertThat(counts("MATCH (n) WHERE n:A OR n:B RETURN n.p AS k, count(*) AS v")).containsExactly(2L);
    }

    @Test
    void arraysOfNumbersAreValuesToo() {
      final Object[][] cases = {
          { Type.ARRAY_OF_SHORTS, new short[] { 1, 2 }, new short[] { 9 } },
          { Type.ARRAY_OF_INTEGERS, new int[] { 1, 2 }, new int[] { 9 } },
          { Type.ARRAY_OF_LONGS, new long[] { 1, 2 }, new long[] { 9 } },
          { Type.ARRAY_OF_FLOATS, new float[] { 1.5f, 2f }, new float[] { 9f } },
          { Type.ARRAY_OF_DOUBLES, new double[] { 1.5d, 2d }, new double[] { 9d } } };
      for (final Object[] c : cases) {
        final String type = "T" + c[0];
        createType(type, (Type) c[0]);
        insert(type, "x", c[1]);
        insert(type, "y", copy(c[1]));
        insert(type, "z", c[2]);

        assertThat(scalar("MATCH (n:" + type + ") RETURN count(DISTINCT n.p) AS v")).as("%s distinct", c[0]).isEqualTo(2L);
        assertThat(rows("MATCH (n:" + type + ") RETURN DISTINCT n.p AS k")).as("%s rows", c[0]).hasSize(2);
        assertThat(counts("MATCH (n:" + type + ") RETURN n.p AS k, count(*) AS v")).as("%s groups", c[0])
            .containsExactlyInAnyOrder(2L, 1L);
        assertThat(scalar("MATCH (a:" + type + "), (c:" + type + ") WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v"))
            .as("%s equality", c[0]).isEqualTo(true);
        assertThat(sqlRows("SELECT DISTINCT p FROM " + type)).as("%s sql", c[0]).isEqualTo(2);

        // min()/max() and ORDER BY answer the same order, whatever came first
        final Result row = single("MATCH (n:" + type + ") RETURN min(n.p) AS lo, max(n.p) AS hi");
        assertThat(Objects.deepEquals(row.getProperty("lo"), c[1])).as("%s min", c[0]).isTrue();
        assertThat(Objects.deepEquals(row.getProperty("hi"), c[2])).as("%s max", c[0]).isTrue();
        final List<Result> ordered = rows("MATCH (n:" + type + ") RETURN n.p AS k ORDER BY k DESC");
        assertThat(Objects.deepEquals(ordered.get(0).getProperty("k"), c[2])).as("%s order desc", c[0]).isTrue();
      }
    }

    @Test
    void anArrayOfNumbersKeepsTheOrderOfTheListOfThoseNumbers() {
      createType("T", Type.ARRAY_OF_INTEGERS);
      insert("T", "a", new int[] { 1, 2, 3 });
      insert("T", "b", new int[] { 1, 2 });
      insert("T", "c", new int[] { 1, 10 });

      final List<Result> ordered = rows("MATCH (n:T) RETURN n.s AS s ORDER BY n.p");
      assertThat(ordered.stream().map(r -> r.<String>getProperty("s")).toList()).containsExactly("b", "a", "c");
      final Result row = single("MATCH (n:T) RETURN min(n.p) AS lo, max(n.p) AS hi");
      assertThat((int[]) row.getProperty("lo")).containsExactly(1, 2);
      assertThat((int[]) row.getProperty("hi")).containsExactly(1, 10);
    }

    /**
     * One enumeration of the engine's value types, so the type a later fix does not reach cannot slip by: two records holding equal
     * values of a type are one value to {@code =}, to {@code DISTINCT}, to {@code GROUP BY} and to {@code min()} / {@code max()}.
     */
    @ParameterizedTest
    @EnumSource(Type.class)
    void everyTypeEqualValuesAreOneValueInAllFourMechanisms(final Type type) {
      final Supplier<Object> value = sample(type);
      database.getSchema().createDocumentType("Emb");
      database.getSchema().createVertexType("T").createProperty("p", type);
      database.getSchema().getType("T").createProperty("s", Type.STRING);
      final Object first = value.get();
      final Object second = value.get();
      database.transaction(() -> {
        database.newVertex("T").set("s", "x").set("p", first).save();
        database.newVertex("T").set("s", "y").set("p", second).save();
      });

      assertThat(scalar("MATCH (a:T), (c:T) WHERE a.s = 'x' AND c.s = 'y' RETURN a.p = c.p AS v")).as("%s equality", type)
          .isEqualTo(true);
      assertThat(scalar("MATCH (n:T) RETURN count(DISTINCT n.p) AS v")).as("%s count distinct", type).isEqualTo(1L);
      assertThat(rows("MATCH (n:T) RETURN DISTINCT n.p AS k")).as("%s distinct", type).hasSize(1);
      assertThat(counts("MATCH (n:T) RETURN n.p AS k, count(*) AS v")).as("%s group by", type).containsExactly(2L);
      final Result row = single("MATCH (n:T) RETURN min(n.p) AS lo, max(n.p) AS hi");
      assertThat(Objects.deepEquals(row.getProperty("lo"), row.getProperty("hi"))).as("%s min = max", type).isTrue();
      assertThat(rows("MATCH (n:T) RETURN n.p AS k ORDER BY k")).as("%s order by", type).hasSize(2);
    }

    /** A fresh value of the type on every call, so the two records of a case never share an array instance. */
    private Supplier<Object> sample(final Type type) {
      return switch (type) {
        case BOOLEAN -> () -> true;
        case INTEGER -> () -> 7;
        case SHORT -> () -> (short) 7;
        case LONG -> () -> 7L;
        case FLOAT -> () -> 1.5f;
        case DOUBLE -> () -> 1.5d;
        case DATETIME -> () -> new Date(1_000_000L);
        case STRING -> () -> "text";
        case BINARY -> () -> new byte[] { 1, 2, 3 };
        case LIST -> () -> new ArrayList<>(List.of(1, 2));
        case MAP -> () -> Map.of("k", 1);
        case LINK -> {
          database.getSchema().createDocumentType("Target");
          final RID[] target = new RID[1];
          database.transaction(() -> target[0] = database.newDocument("Target").save().getIdentity());
          yield () -> target[0];
        }
        case BYTE -> () -> (byte) 3;
        case DATE -> () -> LocalDate.of(2020, 1, 2);
        case DECIMAL -> () -> new BigDecimal("1.50");
        case EMBEDDED -> () -> new HashMap<>(Map.of("@type", "Emb", "k", 1));
        case DATETIME_MICROS -> () -> LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000);
        case DATETIME_NANOS -> () -> LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_789);
        case DATETIME_SECOND -> () -> LocalDateTime.of(2020, 1, 2, 3, 4, 5);
        case ARRAY_OF_SHORTS -> () -> new short[] { 1, 2 };
        case ARRAY_OF_INTEGERS -> () -> new int[] { 1, 2 };
        case ARRAY_OF_LONGS -> () -> new long[] { 1, 2 };
        case ARRAY_OF_FLOATS -> () -> new float[] { 1.5f, 2f };
        case ARRAY_OF_DOUBLES -> () -> new double[] { 1.5d, 2d };
        case OFFSET_TIME -> () -> OffsetTime.of(10, 11, 12, 0, ZoneOffset.ofHours(1));
        case LOCAL_TIME -> () -> LocalTime.of(10, 11, 12);
        case ZONED_DATETIME -> () -> ZonedDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC);
        case DURATION -> () -> CypherDuration.parse("PT1H");
      };
    }

    private static Object copy(final Object array) {
      return switch (array) {
        case byte[] a -> a.clone();
        case short[] a -> a.clone();
        case int[] a -> a.clone();
        case long[] a -> a.clone();
        case float[] a -> a.clone();
        case double[] a -> a.clone();
        default -> throw new IllegalArgumentException("Not an array: " + array);
      };
    }

    private void createType(final String name, final Type type) {
      database.transaction(() -> {
        database.getSchema().createVertexType(name).createProperty("p", type);
        database.getSchema().getType(name).createProperty("s", Type.STRING);
      });
    }

    private void insert(final String type, final String label, final Object value) {
      database.transaction(() -> database.newVertex(type).set("s", label).set("p", value).save());
    }

    private List<Result> rows(final String query) {
      final List<Result> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          out.add(rs.next());
      }
      return out;
    }

    private List<Result> rows(final String query, final Map<String, Object> parameters) {
      final List<Result> out = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query, parameters)) {
        while (rs.hasNext())
          out.add(rs.next());
      }
      return out;
    }

    private Result single(final String query) {
      final List<Result> rows = rows(query);
      assertThat(rows).hasSize(1);
      return rows.get(0);
    }

    private Object scalar(final String query) {
      return single(query).getProperty("v");
    }

    /** The {@code v} column of every row of a group-by query, as longs. */
    private List<Long> counts(final String query) {
      return rows(query).stream().map(r -> r.<Number>getProperty("v").longValue()).toList();
    }

    private int sqlRows(final String query) {
      int n = 0;
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext()) {
          rs.next();
          n++;
        }
      }
      return n;
    }
  }

  /**
   * Issue #10775: with several indexed predicates on one node the openCypher planner anchored on the first indexed
   * property a {@code HashMap} iterated to, without comparing the costs it had computed, so which of two indexes was used
   * depended on the property names. The anchor is now the cheapest candidate access path of the node - an equality seek,
   * an IN-list seek, a seek per disjunct of an OR, or a range scan - and a tie goes to the predicate written first.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10775AnchorPicksCheapestIndex extends TestHelper {
    private static final int ROWS = 2_000;

    // {unique property, non-unique property}: every pair appears in both roles, so a name that wins by its hash fails one of them
    private static final String[][] PAIRS = {
        { "uid", "grp" }, { "grp", "uid" }, { "a_id", "z_part" }, { "z_part", "a_id" }, { "doc_id", "group_name" },
        { "group_name", "doc_id" } };

    @Override
    protected void beginTest() {
      for (int k = 0; k < PAIRS.length; k++)
        createType("T10775_" + k, PAIRS[k][0], PAIRS[k][1]);

      // A label disjunction whose two roots carry the same pair of indexes, in both roles
      createType("A10775", "uid", "grp");
      createType("B10775", "uid", "grp");
      createType("C10775", "grp", "uid");
      createType("D10775", "grp", "uid");

      // A composite UNIQUE index beside a single-property UNIQUE one: only its whole key finds one row
      database.transaction(() -> {
        final VertexType type = database.getSchema().createVertexType("E10775");
        type.createProperty("uid", String.class);
        type.createProperty("cat", String.class);
        type.createProperty("seq", Integer.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uid");
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "cat", "seq");
      });
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex("E10775").set("uid", "id" + i).set("cat", i % 2 == 0 ? "even" : "odd").set("seq", i).save();
      });

      // Two disjunction roots with a non-unique composite (a, b) and a non-unique single-property index on c
      for (final String typeName : new String[] { "F10775", "G10775" }) {
        database.transaction(() -> {
          final VertexType type = database.getSchema().createVertexType(typeName);
          type.createProperty("a", Integer.class);
          type.createProperty("b", Integer.class);
          type.createProperty("c", Integer.class);
          type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a", "b");
          type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "c");
        });
        database.transaction(() -> {
          for (int i = 0; i < ROWS; i++)
            database.newVertex(typeName).set("a", i % 10).set("b", i % 100).set("c", i % 5).save();
        });
      }
    }

    private void createType(final String typeName, final String uniqueProperty, final String plainProperty) {
      database.transaction(() -> {
        final VertexType type = database.getSchema().createVertexType(typeName);
        type.createProperty(uniqueProperty, String.class);
        type.createProperty(plainProperty, String.class);
        type.createProperty("score", Integer.class);
        type.createProperty("rank", Integer.class);
        type.createProperty("note", String.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, uniqueProperty);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, plainProperty);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "score");
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "rank");
      });
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex(typeName)
              .set(uniqueProperty, "id" + i)
              .set(plainProperty, i < ROWS / 2 ? "A" : "B")
              .set("score", i)
              .set("rank", i % 10)
              .set("note", i % 2 == 0 ? "even" : "odd")
              .save();
      });
    }

    @Test
    void inListOnBothPropertiesAnchorsOnTheUniqueIndexWhateverTheNames() {
      for (int k = 0; k < PAIRS.length; k++) {
        final String type = "T10775_" + k;
        final String u = PAIRS[k][0];
        final String n = PAIRS[k][1];
        final String query = "MATCH (r:" + type + ") WHERE r." + u + " IN $ids AND r." + n + " IN $groups RETURN r." + u + " AS e";
        final Map<String, Object> params = Map.of("ids", List.of("id0", "id1", "id2", "id3", "id4"), "groups", List.of("A"));

        assertThat(plan(query, params)).as("unique=%s non-unique=%s", u, n).contains("index=" + type + "[" + u + "]");
        assertThat(values(query, params)).containsExactlyInAnyOrder("id0", "id1", "id2", "id3", "id4");
      }
    }

    @Test
    void equalityOnBothPropertiesAnchorsOnTheUniqueIndexWhateverTheNames() {
      for (int k = 0; k < PAIRS.length; k++) {
        final String type = "T10775_" + k;
        final String u = PAIRS[k][0];
        final String n = PAIRS[k][1];
        final String query = "MATCH (r:" + type + ") WHERE r." + u + " = $u AND r." + n + " = $g RETURN r." + u + " AS e";
        final Map<String, Object> params = Map.of("u", "id1", "g", "A");

        final String plan = plan(query, params);
        assertThat(plan).as("unique=%s non-unique=%s", u, n).contains("index=" + type + "[" + u + "]");
        // A unique index resolved by its whole key finds one row at most
        assertThat(plan).as("unique=%s non-unique=%s", u, n).contains("rows=1]");
        assertThat(values(query, params)).containsExactly("id1");

        // The same predicates as an inline property map
        final String inline = "MATCH (r:" + type + " {" + n + ": 'A', " + u + ": 'id1'}) RETURN r." + u + " AS e";
        assertThat(plan(inline, Map.of())).as("inline unique=%s non-unique=%s", u, n).contains("index=" + type + "[" + u + "]");
        assertThat(values(inline, Map.of())).containsExactly("id1");
      }
    }

    @Test
    void anInListOnTheUniqueIndexBeatsAnEqualityOnTheNonUniqueOne() {
      final String query = "MATCH (r:T10775_0) WHERE r.grp = $g AND r.uid IN $ids RETURN r.uid AS e";
      final Map<String, Object> params = Map.of("g", "A", "ids", List.of("id1", "id2", "id1300"));

      assertThat(plan(query, params)).contains("index=T10775_0[uid]");
      assertThat(values(query, params)).containsExactlyInAnyOrder("id1", "id2");

      final String swapped = "MATCH (r:T10775_1) WHERE r.uid = $g AND r.grp IN $ids RETURN r.grp AS e";
      assertThat(plan(swapped, params)).contains("index=T10775_1[grp]");
      assertThat(values(swapped, params)).containsExactlyInAnyOrder("id1", "id2");
    }

    @Test
    void aPrefixOfACompositeUniqueKeyIsNotCostedAsOneRow() {
      // cat alone is a prefix of the unique (cat, seq) key: one value of it is half the type, not one row
      final String prefix = "MATCH (r:E10775) WHERE r.cat IN ['even'] AND r.uid IN ['id2', 'id4'] RETURN r.uid AS e";
      assertThat(plan(prefix, Map.of())).contains("index=E10775[uid]");
      assertThat(values(prefix, Map.of())).containsExactlyInAnyOrder("id2", "id4");

      // With seq bound too the composite key is whole, and one seek of it beats two of uid
      final String wholeKey = "MATCH (r:E10775) WHERE r.cat IN ['even'] AND r.seq = 2 AND r.uid IN ['id2', 'id4'] RETURN r.uid AS e";
      assertThat(plan(wholeKey, Map.of())).contains("index=E10775[cat,seq]");
      assertThat(values(wholeKey, Map.of())).containsExactly("id2");
    }

    @Test
    void anOrOfSeeksOnTheUniqueIndexBeatsAnEqualityOnTheNonUniqueOne() {
      // Two unique seeks against one non-unique seek estimated at 10% of the type: with no distinct-key statistics the
      // model rates them close on a small type, so the fixture is sized for the 10% to be clearly the dearer one
      final String query = "MATCH (r:T10775_0) WHERE (r.uid = 'id1' OR r.uid = 'id1500') AND r.grp = 'A' RETURN r.uid AS e";

      assertThat(plan(query, Map.of())).contains("index=T10775_0[uid]");
      assertThat(values(query, Map.of())).containsExactly("id1");
    }

    @Test
    void anIndexedRangeIsUsedBesideAnEqualityThatHasNoIndex() {
      final String query = "MATCH (r:T10775_0) WHERE r.note = 'even' AND r.score >= 10 AND r.score < 20 RETURN r.uid AS e";

      final String plan = plan(query, Map.of());
      assertThat(plan).contains("NodeIndexRangeScan").contains("index=T10775_0[score]");
      assertThat(values(query, Map.of())).containsExactlyInAnyOrder("id10", "id12", "id14", "id16", "id18");
    }

    @Test
    void anEqualitySeekBeatsARangeOnAnotherProperty() {
      final String query = "MATCH (r:T10775_0) WHERE r.score > 5 AND r.uid = 'id7' RETURN r.uid AS e";

      assertThat(plan(query, Map.of())).contains("NodeIndexSeek").contains("index=T10775_0[uid]");
      assertThat(values(query, Map.of())).containsExactly("id7");
    }

    @Test
    void aBoundedRangeBeatsAHalfOpenOneWhateverTheNames() {
      final String byScore = "MATCH (r:T10775_0) WHERE r.rank > 2 AND r.score >= 10 AND r.score < 20 RETURN r.uid AS e";
      assertThat(plan(byScore, Map.of())).contains("index=T10775_0[score]");
      assertThat(values(byScore, Map.of())).containsExactlyInAnyOrder("id13", "id14", "id15", "id16", "id17", "id18", "id19");

      final String byRank = "MATCH (r:T10775_0) WHERE r.score > 2 AND r.rank >= 1 AND r.rank < 2 RETURN r.uid AS e";
      assertThat(plan(byRank, Map.of())).contains("index=T10775_0[rank]");
      assertThat(values(byRank, Map.of())).hasSize(ROWS / 10 - 1);
    }

    @Test
    void equalCostSeeksAnchorOnThePredicateWrittenFirst() {
      // Two seeks of the same cost: the tie goes to the order of the query, never to the names
      assertThat(plan("MATCH (r:T10775_0) WHERE r.score = 5 AND r.rank = 5 RETURN r", Map.of())).contains("index=T10775_0[score]");
      assertThat(plan("MATCH (r:T10775_0) WHERE r.rank = 5 AND r.score = 5 RETURN r", Map.of())).contains("index=T10775_0[rank]");
    }

    @Test
    void aUniqueEqualityBeatsAUniqueInOverAParameterOfTheSameEstimate() {
      // Both are one seek of one row as far as the planner can tell ($ids is one list it cannot size): the tie goes to the
      // equality, which is one key for certain, whichever of the two is written first
      final Map<String, Object> params = Map.of("u", "id1", "ids", List.of("id1", "id2", "id3"));

      final String inFirst = "MATCH (r:T10775_0) WHERE r.uid IN $ids AND r.uid = $u RETURN r.uid AS e";
      assertThat(plan(inFirst, params)).contains("index=T10775_0[uid]").doesNotContain("uid=IN");
      assertThat(values(inFirst, params)).containsExactly("id1");

      final String bothUnique = "MATCH (r:E10775) WHERE r.uid IN $ids AND r.cat = 'odd' AND r.seq = $s RETURN r.uid AS e";
      final Map<String, Object> seqParams = Map.of("ids", List.of("id1", "id3"), "s", 1);
      assertThat(plan(bothUnique, seqParams)).contains("index=E10775[cat,seq]");
      assertThat(values(bothUnique, seqParams)).containsExactly("id1");
    }

    @Test
    void aLabelDisjunctionAnchorsOnTheUniqueIndexWhateverTheNames() {
      final String byUid = "MATCH (r:A10775|B10775) WHERE r.grp = 'A' AND r.uid = 'id1' RETURN r.uid AS e";
      final String uidPlan = plan(byUid, Map.of());
      assertThat(uidPlan).contains("NodeByLabelDisjunctionIndexSeek").contains("index=A10775[uid]").contains("index=B10775[uid]");
      assertThat(values(byUid, Map.of())).containsExactly("id1", "id1");

      final String byGrp = "MATCH (r:C10775|D10775) WHERE r.uid = 'A' AND r.grp = 'id1' RETURN r.grp AS e";
      final String grpPlan = plan(byGrp, Map.of());
      assertThat(grpPlan).contains("NodeByLabelDisjunctionIndexSeek").contains("index=C10775[grp]").contains("index=D10775[grp]");
      assertThat(values(byGrp, Map.of())).containsExactly("id1", "id1");
    }

    @Test
    void aLabelDisjunctionCostsEachSeekWithTheKeyPrefixItBinds() {
      // c is written first, but the (a, b) seek binds two key columns and reads a tenth of what the c seek reads
      final String query = "MATCH (r:F10775|G10775) WHERE r.c = 2 AND r.a = 7 AND r.b = 17 RETURN r.b AS e";
      final String plan = plan(query, Map.of());
      assertThat(plan).contains("NodeByLabelDisjunctionIndexSeek").contains("index=F10775[a,b]").contains("index=G10775[a,b]");
      // i % 10 = 7, i % 100 = 17 and i % 5 = 2: i = 17, 117, ... on each of the two roots
      assertThat(values(query, Map.of())).hasSize(2 * ROWS / 100).containsOnly(17);
    }

    private String plan(final String query, final Map<String, Object> params) {
      try (final ResultSet result = database.query("opencypher", "EXPLAIN " + query, params)) {
        return result.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private List<Object> values(final String query, final Map<String, Object> params) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet result = database.query("opencypher", query, params)) {
        while (result.hasNext()) {
          final Result row = result.next();
          values.add(row.getProperty("e"));
        }
      }
      return values;
    }
  }
}
