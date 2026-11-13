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
import com.arcadedb.database.RID;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WHERE predicate evaluation: evaluated-once guarantees, NOT pattern predicates, query rewriting and relationship-type checks.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherPredicateEvaluationTest {
  /**
   * Regression test for <a href="https://github.com/ArcadeData/arcadedb/issues/4405">issue #4405</a>:
   * Missed optimization - redundant AND-true OR-false branch raises ArcadeDB PROFILE rows.
   *
   * The predicate {@code base AND true OR false AND expr} should be simplified to {@code base}
   * at parse time before reaching the optimizer, so both the base and transformed queries
   * produce identical execution plans.
   */
  @Nested
  class Issue4405 {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue4405");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.getSchema().createVertexType("L1");

      database.transaction(() -> {
        // Create vertices with k5 values: one matching -1 (used as a proxy for -1862396709),
        // several non-matching; plus k6 and k10 properties for the UNWIND.
        database.command("opencypher", "CREATE (:L1 {k5: -1, k6: 10, k10: 20})");
        database.command("opencypher", "CREATE (:L1 {k5: -1, k6: 11, k10: 21})");
        database.command("opencypher", "CREATE (:L1 {k5: 0,  k6: 12, k10: 22})");
        database.command("opencypher", "CREATE (:L1 {k5: 1,  k6: 13, k10: 23})");
        database.command("opencypher", "CREATE (:L1 {k5: 2,  k6: 14, k10: 24})");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    /**
     * Both the base query and the semantically-equivalent transformed query
     * ({@code base AND true OR false AND expr}) must return identical result sets.
     */
    @Test
    void baseAndTransformedQueryReturnSameResults() {
      final String baseQuery = """
          MATCH (n0 :L1)
          WHERE (n0.k5) = -1
          UNWIND [(n0.k6), (n0.k10)] AS a0
          RETURN a0
          """;

      final String transformedQuery = """
          MATCH (n0 :L1)
          WHERE ((n0.k5) = -1) AND true OR false AND (n0.k5 IS NULL OR n0.k5 IS NOT NULL)
          UNWIND [(n0.k6), (n0.k10)] AS a0
          RETURN a0
          """;

      final List<Object> baseResults = collectResults(baseQuery, "a0");
      final List<Object> transformedResults = collectResults(transformedQuery, "a0");

      assertThat(transformedResults)
          .as("transformed query (base AND true OR false AND expr) should return same rows as base query")
          .containsExactlyInAnyOrderElementsOf(baseResults);
    }

    /**
     * WHERE clause with only redundant boolean constants that simplify to false
     * (false OR false) should return no results.
     */
    @Test
    void whereAlwaysFalseReturnsEmpty() {
      final String query = "MATCH (n0 :L1) WHERE false OR false RETURN n0";

      try (final ResultSet rs = database.query("opencypher", query)) {
        assertThat(rs.hasNext()).as("WHERE false OR false should return no rows").isFalse();
      }
    }

    /**
     * WHERE clause with only redundant boolean constants that simplify to true
     * (true AND true) should return all vertices.
     */
    @Test
    void whereAlwaysTrueReturnsAll() {
      final String query = "MATCH (n0 :L1) WHERE true AND true RETURN n0";
      final List<Object> results = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          results.add(rs.next().getProperty("n0"));
        }
      }
      assertThat(results).as("WHERE true AND true should return all 5 vertices").hasSize(5);
    }

    /**
     * WHERE predicate AND true should behave identically to the predicate alone.
     */
    @Test
    void predicateAndTrueEquivalentToPredicateAlone() {
      final String baseQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 RETURN n0";
      final String withTrueQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 AND true RETURN n0";

      final List<Object> base = collectResults(baseQuery, "n0");
      final List<Object> withTrue = collectResults(withTrueQuery, "n0");

      assertThat(withTrue)
          .as("predicate AND true must return same results as predicate alone")
          .containsExactlyInAnyOrderElementsOf(base);
      assertThat(withTrue).hasSize(2);
    }

    /**
     * WHERE predicate OR false should behave identically to the predicate alone.
     */
    @Test
    void predicateOrFalseEquivalentToPredicateAlone() {
      final String baseQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 RETURN n0";
      final String withFalseQuery = "MATCH (n0 :L1) WHERE n0.k5 = -1 OR false RETURN n0";

      final List<Object> base = collectResults(baseQuery, "n0");
      final List<Object> withFalse = collectResults(withFalseQuery, "n0");

      assertThat(withFalse)
          .as("predicate OR false must return same results as predicate alone")
          .containsExactlyInAnyOrderElementsOf(base);
      assertThat(withFalse).hasSize(2);
    }

    private List<Object> collectResults(final String query, final String propertyName) {
      final List<Object> results = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          results.add(row.getProperty(propertyName));
        }
      }
      return results;
    }
  }

  /**
   * Regression test for issue #9202: the WHERE of a MATCH planned by the cost-based optimizer was evaluated twice, once by the
   * Filter of the physical plan and once by the FilterPropertiesStep added after the optimized match. A predicate that draws
   * rand() keeps a row with probability 0.5 when it is evaluated once and 0.25 when it is evaluated twice.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9202WhereEvaluatedOnce {
    private static final String DB_PATH = "./target/databases/issue9202";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.command("sql", "CREATE VERTEX TYPE P");
      database.command("sql", "CREATE EDGE TYPE K");
      database.begin();
      final RID[] v = new RID[1000];
      for (int i = 0; i < v.length; i++) {
        final MutableVertex mv = database.newVertex("P").set("id", (long) i);
        mv.save();
        v[i] = mv.getIdentity();
      }
      final Random r = new Random(3);
      for (int i = 0; i < 10_000; i++)
        v[r.nextInt(1000)].asVertex().newEdge("K", v[r.nextInt(1000)]);
      database.commit();
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private String plan(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().map(p -> p.prettyPrint(0, 2)).orElse("");
      }
    }

    @Test
    void randPredicateKeepsHalfTheRows() {
      for (final String where : new String[] { "rand() < 0.5", "b.id >= 0 AND rand() < 0.5" }) {
        final String query = "MATCH (a:P)-[:K]->(b:P) WHERE " + where + " RETURN count(*) AS n";
        double sum = 0;
        for (int i = 0; i < 5; i++)
          sum += count(query) / 10_000.0;
        assertThat(sum / 5).as(where).isBetween(0.45, 0.55);
      }
    }

    @Test
    void planCarriesTheFilterOnce() {
      final String text = plan("MATCH (a:P)-[:K]->(b:P) WHERE b.id >= 0 AND rand() < 0.5 RETURN count(*) AS n");
      assertThat(text).contains("Filter [predicate=");
      assertThat(text).doesNotContain("FILTER WHERE");
    }

    @Test
    void secondMatchWhereIsAppliedOnce() {
      final String query = "MATCH (a:P) WHERE a.id < 100 MATCH (a)-[:K]->(b:P) WHERE rand() < 0.5 RETURN count(*) AS n";
      final long all = count("MATCH (a:P) WHERE a.id < 100 MATCH (a)-[:K]->(b:P) RETURN count(*) AS n");
      double sum = 0;
      for (int i = 0; i < 10; i++)
        sum += count(query) / (double) all;
      assertThat(sum / 10).isBetween(0.4, 0.6);
    }
  }

  /**
   * Issue #9390: a NOT pattern predicate over an edge type the Graph Analytical View lists answered as if the pair were connected when the
   * same MATCH also had a hop over an edge type the view does not list. The oracle is the same query without a view.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9390NotPatternPredicateUnlistedHop extends TestHelper {
    private static final String HOPS = "MATCH (t1:Tag)-[:LIKES]->(m:Message)<-[:REPLY_OF]-(c:Comment)<-[:HAS_TAG]-(t2:Tag) ";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Tag");
      database.command("sql", "CREATE VERTEX TYPE Message");
      database.command("sql", "CREATE VERTEX TYPE Comment EXTENDS Message");
      for (final String e : new String[] { "LIKES", "REPLY_OF", "HAS_TAG" })
        database.command("sql", "CREATE EDGE TYPE " + e);
      database.transaction(() -> {
        final RID t1 = database.newVertex("Tag").save().getIdentity();
        final RID t2 = database.newVertex("Tag").save().getIdentity();
        final RID m = database.newVertex("Message").save().getIdentity();
        final RID c = database.newVertex("Comment").save().getIdentity();
        t1.asVertex().newEdge("LIKES", m);
        c.asVertex().newEdge("REPLY_OF", m);
        t2.asVertex().newEdge("HAS_TAG", c);
      });
    }

    @Test
    void notPredicateOnListedTypeWithUnlistedHop() throws InterruptedException {
      assertAllAgree(HOPS + "WHERE NOT (c)<-[:HAS_TAG]-(t1) AND t1 <> t2 RETURN count(*) AS n", 1L);
      assertAllAgree(HOPS + "WHERE NOT (c)<-[:HAS_TAG]-(t1) RETURN count(*) AS n", 1L);
      assertAllAgree(HOPS + "WHERE NOT (t1)-[:HAS_TAG]->(c) AND t1 <> t2 RETURN count(*) AS n", 1L);
      assertAllAgree(HOPS + "WHERE NOT (c)<-[:HAS_TAG]-(t2) RETURN count(*) AS n", 0L);
      assertAllAgree(HOPS + "WHERE (c)<-[:HAS_TAG]-(t2) RETURN count(*) AS n", 1L);
      assertAllAgree(HOPS + "WHERE (c)<-[:HAS_TAG]-(t1) RETURN count(*) AS n", 0L);
    }

    private void assertAllAgree(final String query, final long expected) throws InterruptedException {
      assertThat(count(query)).as("no view").isEqualTo(expected);

      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW gav VERTEX TYPES (Tag, Message, Comment) EDGE TYPES (HAS_TAG, REPLY_OF) UPDATE MODE OFF");
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "gav");
      final long deadline = System.currentTimeMillis() + 60_000;
      while (!view.isReady() && System.currentTimeMillis() < deadline)
        Thread.sleep(20);
      assertThat(view.isReady()).isTrue();
      try {
        assertThat(count(query)).as("view without LIKES").isEqualTo(expected);
      } finally {
        database.command("sql", "DROP GRAPH ANALYTICAL VIEW gav");
      }
    }

    private long count(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }
  }

  /**
   * Issue #11102: on the traditional execution path a WHERE conjunct pushed into the {@code MatchNodeStep} that binds its
   * variable was evaluated a second time by the clause's {@code FilterPropertiesStep}, once per row that passed it. The
   * filter step now applies only the residual the node steps did not take, and is left out when nothing is left.
   * <p>
   * The work is counted, never timed: {@link #PROBE} counts its calls and answers true, so every call is a row the
   * predicate was asked about. A seeded {@code CALL () { }} body is planned on the traditional path, which is where the
   * double evaluation lived.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11102PushedWhereEvaluatedOnce {
    private static final String DB_PATH = "./target/databases/issue-11102";
    private static final String PROBE   = "issue11102.probe";
    private static final int    NODES   = 50;

    private static final AtomicLong PROBES = new AtomicLong();

    private Database database;

    @BeforeEach
    void setUp() {
      CypherFunctionRegistry.registerOrReplace(new StatelessFunction() {
        @Override
        public String getName() {
          return PROBE;
        }

        @Override
        public Object execute(final Object[] args, final CommandContext context) {
          PROBES.incrementAndGet();
          return true;
        }
      });

      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.command("sql", "CREATE VERTEX TYPE PwNode");
      database.command("sql", "CREATE VERTEX TYPE PwOther");
      database.command("sql", "CREATE EDGE TYPE PW_LINK");
      database.transaction(() -> {
        database.command("opencypher", "UNWIND range(1, $n) AS i CREATE (:PwNode {uid: i, grp: i % 5})", Map.of("n", NODES));
        database.command("opencypher", "UNWIND range(0, 4) AS i CREATE (:PwOther {grp: i})");
        database.command("opencypher", "MATCH (a:PwNode), (b:PwNode) WHERE b.uid = a.uid + 1 CREATE (a)-[:PW_LINK]->(b)");
      });
      PROBES.set(0);
    }

    @AfterEach
    void tearDown() {
      CypherFunctionRegistry.unregister(PROBE);
      if (database != null)
        database.drop();
    }

    @Test
    void theShapeOfTheReportCallsThePredicateOncePerRow() {
      // The report: 4 calls for 2 rows, 2 from the pushed filter and 2 from the clause filter
      assertThat(column("CALL () { MATCH (n:PwNode) WHERE " + PROBE + "(n) RETURN n.uid AS uid } RETURN uid LIMIT 2", "uid")).hasSize(2);
      assertThat(PROBES.get()).isEqualTo(2);

      PROBES.set(0);
      assertThat(column("CALL () { MATCH (n:PwNode) WHERE " + PROBE + "(n) RETURN n.uid AS uid } RETURN uid", "uid")).hasSize(NODES);
      assertThat(PROBES.get()).isEqualTo(NODES);
    }

    @Test
    void aPushedConjunctNextToAResidualOneIsEvaluatedOnce() {
      // probe(a) is pushed into the node step of a; a.grp = b.grp needs b, bound by the second pattern part, and stays
      final List<Object> rows = column("CALL () { MATCH (a:PwNode), (b:PwOther) WHERE " + PROBE
          + "(a) AND a.grp = b.grp RETURN a.uid AS uid } RETURN uid", "uid");
      assertThat(rows).hasSize(NODES);
      // a is scanned once and each a is asked once, whatever b it is later joined with
      assertThat(PROBES.get()).isEqualTo(NODES);
    }

    @Test
    void aConjunctOnTheFarEndOfAHopIsStillApplied() {
      // probe(a) is pushed into the source step; b.uid > 40 reads the hop's target, which no node step binds, and stays
      final List<Object> rows = column("CALL () { MATCH (a:PwNode)-[:PW_LINK]->(b:PwNode) WHERE " + PROBE
          + "(a) AND b.uid > 40 RETURN b.uid AS uid } RETURN uid ORDER BY uid", "uid");
      assertThat(rows).containsExactly(41L, 42L, 43L, 44L, 45L, 46L, 47L, 48L, 49L, 50L);
      // every source with an outgoing link is asked once (the last node has none, and is still asked once)
      assertThat(PROBES.get()).isEqualTo(NODES);
    }

    @Test
    void anOptionalMatchStillFiltersInsideItsChain() {
      final List<String> rows = rows("CALL () { MATCH (o:PwOther) OPTIONAL MATCH (n:PwNode) WHERE " + PROBE
          + "(n) AND n.grp = o.grp AND n.uid < 10 RETURN o.grp AS g, count(n) AS c } RETURN g, c ORDER BY g");
      assertThat(rows).containsExactly("0,1", "1,2", "2,2", "3,2", "4,2");
    }

    @Test
    void aShortestPathPredicateIsDecidedOnceThePathExists() {
      // length(p) reads the path, which the endpoints' node steps run before: it must stay with the clause's filter
      final String body = "MATCH p = shortestPath((a:PwNode {uid: 1})-[:PW_LINK*]->(b:PwNode {uid: 5})) WHERE length(p) > 2 "
          + "RETURN length(p) AS len";
      assertThat(column("CALL () { " + body + " } RETURN len", "len")).containsExactly(4L);
      assertThat(column(body, "len")).containsExactly(4L);
    }

    @Test
    void theAnswersMatchTheOptimizerPath() {
      // The same statements outside a CALL body take the optimizer path: both answers must agree
      final String[] bodies = {
          "MATCH (n:PwNode) WHERE n.uid > 45 RETURN n.uid AS uid",
          "MATCH (a:PwNode), (b:PwOther) WHERE a.uid < 8 AND a.grp = b.grp RETURN a.uid AS uid",
          "MATCH (a:PwNode)-[:PW_LINK]->(b:PwNode) WHERE a.uid > 44 AND b.uid < 49 RETURN b.uid AS uid",
          "MATCH (a:PwNode) WHERE a.uid = 3 OR a.uid = 7 RETURN a.uid AS uid",
          "MATCH (a:PwNode) WHERE NOT (a.grp = 1) AND a.uid <= 6 RETURN a.uid AS uid" };
      for (final String body : bodies) {
        final List<String> optimizer = sorted(rows(body));
        final List<String> traditional = sorted(rows("CALL () { " + body + " } RETURN uid"));
        assertThat(traditional).as(body).isNotEmpty().isEqualTo(optimizer);
      }
    }

    private static List<String> sorted(final List<String> rows) {
      final List<String> copy = new ArrayList<>(rows);
      copy.sort(null);
      return copy;
    }

    private List<Object> column(final String query, final String name) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          values.add(rs.next().getProperty(name));
      }
      return values;
    }

    private List<String> rows(final String query) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final StringBuilder sb = new StringBuilder();
          for (final String name : row.getPropertyNames()) {
            if (!sb.isEmpty())
              sb.append(',');
            sb.append((Object) row.getProperty(name));
          }
          rows.add(sb.toString());
        }
      }
      return rows;
    }
  }
}
