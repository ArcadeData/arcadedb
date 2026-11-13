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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.executor.DistinctSemiJoin;
import com.arcadedb.query.opencypher.parser.FunctionValidator;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Query planning behaviour: pattern order, demand-driven reads, semi-joins, parallel scans and edge loading.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherQueryPlanningTest {
  /**
   * Regression test for issue #5116: reordering standalone (single-node) comma-separated pattern parts
   * inside a single MATCH must not change the amount of work the engine performs, even when a node
   * variable repeats across parts with different label constraints.
   * <p>
   * The reported query repeats {@code n0} once with a multi-label conjunction and once bare:
   * <pre>
   *   fast: MATCH (n0 :L1 :L5), (n0), (n1) ...
   *   slow: MATCH (n1), (n0), (n0 :L1 :L5) ...
   * </pre>
   * The multi-label conjunction {@code (n0:L1:L5)} disqualifies the cost-based optimizer, so the query
   * runs on the legacy step-chain path. That path chains comma-separated pattern parts in textual order
   * as a left-deep nested-loop Cartesian product. In the slow form the bare {@code (n0)} is chained
   * before the labeled {@code (n0:L1:L5)}, so {@code n0} is first bound to <em>every</em> node (full
   * scan) and only afterwards filtered by the labels; in the fast form the labeled occurrence binds
   * {@code n0} to the small {@code L1&amp;L5} class first. This produced the reported ~16x slowdown.
   * <p>
   * The fix orders the parts of each independent component so the most selective (labeled) occurrence of
   * a shared variable binds first, making both formulations do the same work. The test is deterministic
   * (it compares PROFILE row-count totals, not wall-clock time) and asserts both orderings return
   * identical results.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5116PatternOrder {
    private Database database;

    private static final int L1L5_COUNT = 5;   // nodes carrying both L1 and L5
    private static final int PLAIN_COUNT = 60; // other nodes (no L1/L5)

    @BeforeEach
    void setup() {
      database = new DatabaseFactory("./target/databases/test-issue5116").create();

      database.transaction(() -> {
        for (int i = 0; i < L1L5_COUNT; i++)
          database.command("opencypher", "CREATE (n:L1:L5 {v: " + i + "})");
        for (int i = 0; i < PLAIN_COUNT; i++)
          database.command("opencypher", "CREATE (n:Other {v: " + i + "})");
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void repeatedVariableOrderDoesNotChangeWork() {
      final String fast = "MATCH (n0 :L1 :L5), (n0), (n1) RETURN n0.v AS a, n1.v AS b";
      final String slow = "MATCH (n1), (n0), (n0 :L1 :L5) RETURN n0.v AS a, n1.v AS b";

      // Both formulations must return the same result set.
      final List<String> fastRows = collectRows(fast);
      final List<String> slowRows = collectRows(slow);

      final int totalNodes = L1L5_COUNT + PLAIN_COUNT;
      final int expected = L1L5_COUNT * totalNodes; // n0 in L1&L5, n1 in all nodes
      assertThat(fastRows).hasSize(expected);
      assertThat(slowRows).containsExactlyInAnyOrderElementsOf(fastRows);

      // The engine must not do more work just because the bare occurrence of n0 was written first.
      final long fastWork = profileTotalRows(fast);
      final long slowWork = profileTotalRows(slow);

      assertThat(slowWork)
          .as("reordering repeated-variable pattern parts must not multiply the work "
              + "(fast=%d, slow=%d rows processed)", fastWork, slowWork)
          .isEqualTo(fastWork);
    }

    private List<String> collectRows(final String cypher) {
      final List<String> rows = new ArrayList<>();
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher", cypher);
        while (rs.hasNext()) {
          final Result r = rs.next();
          rows.add(r.getProperty("a") + "|" + r.getProperty("b"));
        }
        rs.close();
      });
      return rows;
    }

    /**
     * Runs the query under PROFILE and sums every "&lt;n&gt; rows" counter reported across the plan
     * steps. This is a deterministic proxy for the total work performed by the plan.
     */
    private long profileTotalRows(final String cypher) {
      final StringBuilder plan = new StringBuilder();
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher", "PROFILE " + cypher);
        while (rs.hasNext())
          rs.next();
        plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
        rs.close();
      });

      long total = 0;
      final Matcher m = Pattern.compile("([\\d,]+) rows").matcher(plan.toString());
      while (m.find())
        total += Long.parseLong(m.group(1).replace(",", ""));
      return total;
    }
  }

  /**
   * Regression test for issue #5117: reordering independent (disconnected) comma-separated pattern
   * parts inside a single MATCH must not change the amount of work the engine performs.
   * <p>
   * The reported query mixes a multi-hop connected pattern with a standalone node pattern:
   * <pre>
   *   MATCH (n1:L1)&lt;-[r0:T4]-(n2:L0:L1)&lt;-[r1:T1]-(n3), (n4:L3) WHERE ...
   * </pre>
   * The multi-label conjunction {@code (n2:L0:L1)} disqualifies the cost-based optimizer, so the query
   * runs on the legacy step-chain path. That path chained comma-separated pattern parts in textual
   * order as a left-deep nested-loop Cartesian product: the first-listed part is the outer loop and
   * every later part is re-executed once per outer row. Writing the cheap {@code (n4:L3)} scan first
   * therefore re-executed the expensive {@code n3->n2->n1} traversal once for every {@code L3} vertex,
   * producing the reported ~33x slowdown versus writing the traversal first.
   * <p>
   * The fix reorders independent components so the expensive (edge-bearing) component drives as the
   * outer loop regardless of the written order, making both formulations do the same work. The test is
   * deterministic (it compares the PROFILE row-count totals, not wall-clock time) and also asserts the
   * two orderings return identical results.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue5117PatternOrder {
    private Database database;

    private static final int L3_COUNT    = 30;
    private static final int CHAIN_COUNT = 5;

    @BeforeEach
    void setup() {
      database = new DatabaseFactory("./target/databases/test-issue5117").create();

      database.transaction(() -> {
        // Standalone, disconnected L3 nodes.
        for (int i = 0; i < L3_COUNT; i++)
          database.command("opencypher", "CREATE (n:L3 {v: " + i + "})");

        // Connected chains: (n3:L2)-[:T1]->(n2:L0:L1)-[:T4]->(n1:L1)
        // i.e. matching pattern (n1:L1)<-[r0:T4]-(n2:L0:L1)<-[r1:T1]-(n3).
        for (int j = 0; j < CHAIN_COUNT; j++) {
          database.command("opencypher",
              "CREATE (a:L1 {name: 'a" + j + "'}), (b:L0:L1 {name: 'b" + j + "'}), (c:L2 {name: 'c" + j + "'}), "
                  + "(b)-[:T4 {id: " + (10 + j) + "}]->(a), "
                  + "(c)-[:T1 {id: " + (500 + j) + ", k43: " + (1000 + j) + "}]->(b)");
        }
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void patternOrderDoesNotChangeWork() {
      final String where = " WHERE (((r1.k43) > -2019129302) AND ((r0.id) <> (r1.id)))";
      final String fast = "MATCH (n1 :L1)<-[r0 :T4]-(n2 :L0 :L1)<-[r1 :T1]-(n3), (n4 :L3)" + where
          + " RETURN n1.name AS a, n4.v AS v";
      final String slow = "MATCH (n4 :L3), (n1 :L1)<-[r0 :T4]-(n2 :L0 :L1)<-[r1 :T1]-(n3)" + where
          + " RETURN n1.name AS a, n4.v AS v";

      // Both formulations must return the same result set.
      final List<String> fastRows = collectRows(fast);
      final List<String> slowRows = collectRows(slow);

      final int expected = CHAIN_COUNT * L3_COUNT; // 5 chains x 30 L3 nodes
      assertThat(fastRows).hasSize(expected);
      assertThat(slowRows).containsExactlyInAnyOrderElementsOf(fastRows);

      // The engine must not do more work just because the disconnected node was written first.
      final long fastWork = profileTotalRows(fast);
      final long slowWork = profileTotalRows(slow);

      assertThat(slowWork)
          .as("reordering independent pattern parts must not multiply the work of the expensive "
              + "traversal (fast=%d, slow=%d rows processed)", fastWork, slowWork)
          .isEqualTo(fastWork);
    }

    private List<String> collectRows(final String cypher) {
      final List<String> rows = new ArrayList<>();
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher", cypher);
        while (rs.hasNext()) {
          final Result r = rs.next();
          rows.add(r.getProperty("a") + "|" + r.getProperty("v"));
        }
        rs.close();
      });
      return rows;
    }

    /**
     * Runs the query under PROFILE and sums every "&lt;n&gt; rows" counter reported across the plan
     * steps. This is a deterministic proxy for the total work performed by the plan.
     */
    private long profileTotalRows(final String cypher) {
      final StringBuilder plan = new StringBuilder();
      database.transaction(() -> {
        final ResultSet rs = database.query("opencypher", "PROFILE " + cypher);
        while (rs.hasNext())
          rs.next();
        plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
        rs.close();
      });

      long total = 0;
      final Matcher m = Pattern.compile("([\\d,]+) rows").matcher(plan.toString());
      while (m.find())
        total += Long.parseLong(m.group(1).replace(",", ""));
      return total;
    }
  }

  /**
   * Regression test for issue #8537: a multi-hop MATCH must not load an edge record to find the vertex at the other end
   * of the hop or to enforce relationship uniqueness. The edge segment already holds both the edge RID and the
   * neighbour RID; the edge record is read only when the query reads the edge itself.
   * <p>
   * Every answer is checked against a brute-force enumeration over the Java API with Cypher's relationship uniqueness
   * (no edge twice in one MATCH pattern), on a graph built to exercise the edge-reuse case {@code p->a->p->a}, parallel
   * edges, self-loops and all three directions.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8537ExpandWithoutEdgeLoad {
    private static final String DB_PATH = "./target/databases/issue8537";

    private Database        database;
    private final List<RID> people = new ArrayList<>();

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Person");
        database.command("sql", "CREATE PROPERTY Person.id LONG");
        database.command("sql", "CREATE PROPERTY Person.age INTEGER");
        database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
        database.command("sql", "CREATE VERTEX TYPE Robot");
        database.command("sql", "CREATE EDGE TYPE KNOWS");
        database.command("sql", "CREATE PROPERTY KNOWS.since INTEGER");
      });

      database.transaction(() -> {
        final List<MutableVertex> v = new ArrayList<>();
        for (int i = 0; i < 12; i++)
          v.add(database.newVertex("Person").set("id", (long) i, "age", 30 + i).save());
        final MutableVertex robot = database.newVertex("Robot").set("id", 100L).save();

        // A two-cycle (the p->a->p->a edge-reuse case), parallel edges, a self-loop and a small random-ish mesh
        final int[][] edges = { { 0, 1 }, { 1, 0 }, { 0, 1 }, { 1, 2 }, { 2, 3 }, { 3, 0 }, { 2, 2 }, { 0, 0 }, { 4, 5 }, { 5, 6 },
            { 6, 4 }, { 1, 4 }, { 4, 1 }, { 7, 8 }, { 8, 9 }, { 9, 10 }, { 10, 11 }, { 11, 7 }, { 3, 7 }, { 7, 3 }, { 5, 5 } };
        int since = 2000;
        for (final int[] e : edges)
          v.get(e[0]).newEdge("KNOWS", v.get(e[1]), "since", since++);
        v.get(2).newEdge("KNOWS", robot, "since", 1999);
        robot.newEdge("KNOWS", v.get(0), "since", 1998);

        for (final MutableVertex p : v)
          people.add(p.getIdentity());
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void threeHopCountsMatchBruteForceInEveryDirection() {
      for (final String arrow : new String[] { "->", "<-", "-" }) {
        final Vertex.DIRECTION direction = switch (arrow) {
          case "->" -> Vertex.DIRECTION.OUT;
          case "<-" -> Vertex.DIRECTION.IN;
          default -> Vertex.DIRECTION.BOTH;
        };
        final String hop = arrow.equals("<-") ? "<-[:KNOWS]-" : arrow.equals("->") ? "-[:KNOWS]->" : "-[:KNOWS]-";

        for (long id = 0; id < 12; id++) {
          final long expectedAll = bruteForce(id, direction, false, false);
          final long expectedAnyMiddle = bruteForce(id, direction, false, true);
          final long expectedFiltered = bruteForce(id, direction, true, false);

          // Optimizer path (every node labelled), the path counted and the distinct end nodes
          assertThat(count("MATCH (p:Person)" + hop + "(:Person)" + hop + "(:Person)" + hop + "(x:Person) WHERE p.id = $id RETURN count(*) AS n",
              id)).as("paths %s from %d", arrow, id).isEqualTo(expectedAll);
          assertThat(count("MATCH (p:Person)" + hop + "(:Person)" + hop + "(:Person)" + hop
              + "(x:Person) WHERE p.id = $id AND x.age > 36 RETURN count(*) AS n", id)).as("filtered paths %s from %d", arrow, id)
              .isEqualTo(expectedFiltered);

          // Step-based path (unlabelled middle nodes do not qualify for the optimizer)
          assertThat(count("MATCH (p:Person)" + hop + "()" + hop + "()" + hop + "(x:Person) WHERE p.id = $id RETURN count(*) AS n", id))
              .as("unlabelled paths %s from %d", arrow, id).isEqualTo(expectedAnyMiddle);

          // Variable length, same uniqueness rule
          assertThat(count("MATCH (p:Person)" + hop.replace("[:KNOWS]", "[:KNOWS*3..3]") + "(x:Person) WHERE p.id = $id RETURN count(*) AS n", id))
              .as("var-length paths %s from %d", arrow, id).isEqualTo(expectedAnyMiddle);

          // Named relationship variables that are read: the edge record is loaded and must be the right one
          assertThat(count("MATCH (p:Person)" + hop.replace("[:", "[r1:") + "(:Person)" + hop.replace("[:", "[r2:") + "(:Person)"
              + hop.replace("[:", "[r3:") + "(x:Person) WHERE p.id = $id AND r1.since > 0 AND r2.since > 0 AND r3.since > 0 RETURN count(*) AS n",
              id)).as("named paths %s from %d", arrow, id).isEqualTo(expectedAll);
        }
      }
    }

    @Test
    void anonymousMultiHopMatchLoadsNoEdgeRecord() {
      final AtomicInteger edgeReads = countReads("KNOWS");

      final String[] queries = {
          "MATCH (p:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id AND x.age > 36 RETURN count(DISTINCT x) AS n",
          "MATCH (p:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(:Person)-[:KNOWS]-(x:Person) WHERE p.id = $id RETURN count(DISTINCT x) AS n",
          // Unlabelled middle nodes: the step-based plan
          "MATCH (p:Person)-[:KNOWS]->()-[:KNOWS]->()-[:KNOWS]->(x:Person) WHERE p.id = $id RETURN count(DISTINCT x) AS n",
          "MATCH (p:Person)-[:KNOWS]-()-[:KNOWS]-()-[:KNOWS]-(x:Person) WHERE p.id = $id RETURN count(DISTINCT x) AS n",
          // Variable length
          "MATCH (p:Person)-[:KNOWS*3..3]->(x:Person) WHERE p.id = $id RETURN count(DISTINCT x) AS n",
          "MATCH (p:Person)-[:KNOWS*1..3]-(x:Person) WHERE p.id = $id RETURN count(DISTINCT x) AS n" };
      for (final String query : queries) {
        for (long id = 0; id < 12; id++)
          count(query, id);
        assertThat(edgeReads.get()).as("KNOWS records loaded by %s", query).isZero();
      }
      assertThat(edgeReads.get()).as("KNOWS records loaded by anonymous multi-hop MATCH").isZero();

      // Reading the relationship still loads it, and only then
      count("MATCH (p:Person)-[r:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id AND r.since > 0 RETURN count(x) AS n", 0L);
      assertThat(edgeReads.get()).isPositive();
    }

    @Test
    void edgeFromAdjacencyAnswersItsEndpointsWithoutLoading() {
      final AtomicInteger edgeReads = countReads("KNOWS");

      final Vertex p = people.get(0).asVertex();
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      int seen = 0;
      for (final Edge e : iterable(engine.getEdgesKnowingEndpoints((VertexInternal) p, Vertex.DIRECTION.BOTH, "KNOWS"))) {
        assertThat(e.getOut()).isNotNull();
        assertThat(e.getIn()).isNotNull();
        assertThat(e.getOutVertex().getIdentity()).isEqualTo(e.getOut());
        assertThat(e.getInVertex().getIdentity()).isEqualTo(e.getIn());
        assertThat(e.getOut().equals(p.getIdentity()) || e.getIn().equals(p.getIdentity())).isTrue();
        ++seen;
      }
      for (final Edge e : iterable(engine.getEdgesKnowingEndpoints((VertexInternal) p, Vertex.DIRECTION.OUT))) {
        assertThat(e.getOut()).isEqualTo(p.getIdentity());
        ++seen;
      }
      assertThat(seen).isGreaterThan(0);
      assertThat(edgeReads.get()).as("KNOWS records loaded to answer endpoints").isZero();

      // The endpoints agree with the record once it is loaded
      for (final Edge e : iterable(engine.getEdgesKnowingEndpoints((VertexInternal) p, Vertex.DIRECTION.IN, "KNOWS"))) {
        final RID out = e.getOut();
        final RID in = e.getIn();
        assertThat(e.getInteger("since")).isNotNull();
        assertThat(e.getOut()).isEqualTo(out);
        assertThat(e.getIn()).isEqualTo(in);
        final Edge loaded = e.getIdentity().asEdge(true);
        assertThat(loaded.getOut()).isEqualTo(out);
        assertThat(loaded.getIn()).isEqualTo(in);
      }
      assertThat(edgeReads.get()).isPositive();
    }

    /**
     * An undirected variable-length hop takes a self-loop once, as a fixed-length one does (openCypher TCK, "undirected
     * match in self-relationship graph"): walking both lists of the vertex used to count it twice.
     */
    @Test
    void undirectedVariableLengthTakesSelfLoopOnce() {
      for (long id = 0; id < 12; id++) {
        final long fixed = count("MATCH (p:Person)-[:KNOWS]-(x) WHERE p.id = $id RETURN count(*) AS n", id);
        assertThat(count("MATCH (p:Person)-[:KNOWS*1..1]-(x) WHERE p.id = $id RETURN count(*) AS n", id)).as("from %d", id)
            .isEqualTo(fixed);
        assertThat(count("MATCH (p:Person)-[r:KNOWS*1..1]-(x) WHERE p.id = $id RETURN count(*) AS n", id)).as("named, from %d", id)
            .isEqualTo(fixed);
      }
    }

    /**
     * An edge that knows its endpoints from the edge list still has record content: when that content cannot be read, it
     * must not be modified as if it had none, which would save it back without its properties.
     */
    @Test
    void edgeWithListEndpointsRefusesModifyWhenContentIsFilteredAway() {
      final AfterRecordReadListener filter = record -> null;
      final RecordEvents events = database.getSchema().getType("KNOWS").getEvents();
      events.registerListener(filter);
      try {
        database.transaction(() -> {
          final Edge edge = ((DatabaseInternal) database).getGraphEngine()
              .getEdgesKnowingEndpoints((VertexInternal) people.get(0).asVertex(), Vertex.DIRECTION.OUT, "KNOWS").next();
          assertThat(edge.getOut()).isEqualTo(people.get(0));
          assertThatThrownBy(edge::modify).isInstanceOf(RecordNotFoundException.class);
        });
      } finally {
        events.unregisterListener(filter);
      }
    }

    /**
     * The anchor predicate is still applied, with Cypher's equality semantics, when it is evaluated once per anchor row
     * instead of once per expanded path: a fraction matches no LONG key, a text with no LONG form matches nothing, and the
     * text form of a key finds the same rows as the key itself, since Person.id is declared LONG (#9794, #9935).
     */
    @Test
    void anchorPredicateKeepsCypherEqualitySemantics() {
      // isPositive(): the anchor must reach some paths, or matching the brute force would hold for a predicate matching nothing
      assertThat(count("MATCH (p:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id RETURN count(*) AS n", "1"))
          .isEqualTo(bruteForce2(1)).isPositive();
      assertThat(count("MATCH (p:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id RETURN count(*) AS n", "one")).isZero();
      assertThat(count("MATCH (p:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id RETURN count(*) AS n", 1.5d)).isZero();
      assertThat(count("MATCH (p:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id RETURN count(*) AS n", 1))
          .isEqualTo(bruteForce2(1));

      // The predicate moved to the seek: the plan evaluates it there instead of on every expanded row
      try (final ResultSet rs = database.query("opencypher",
          "EXPLAIN MATCH (p:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id AND x.age > 36 RETURN count(*) AS n",
          Map.of("id", 1L))) {
        final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
        assertThat(plan).contains("NodeIndexSeek");
        assertThat(plan).containsPattern("(?s)Filter.*x\\.age.*NodeIndexSeek[^\\n]*filter: p\\.id");
      }
    }

    private long bruteForce2(final long id) {
      long n = 0;
      final Vertex p = people.get((int) id).asVertex();
      for (final Edge e1 : p.getEdges(Vertex.DIRECTION.OUT, "KNOWS")) {
        final Vertex a = e1.getInVertex();
        if (!a.getTypeName().equals("Person"))
          continue;
        for (final Edge e2 : a.getEdges(Vertex.DIRECTION.OUT, "KNOWS"))
          if (!e2.getIdentity().equals(e1.getIdentity()) && e2.getInVertex().getTypeName().equals("Person"))
            ++n;
      }
      return n;
    }

    /**
     * Counts paths p-[e1]-a-[e2]-b-[e3]-x with distinct e1, e2, e3, p and x Persons, a and b Persons unless
     * {@code anyMiddle}.
     */
    private long bruteForce(final long id, final Vertex.DIRECTION direction, final boolean filterAge, final boolean anyMiddle) {
      final Vertex p = people.get((int) id).asVertex();
      long n = 0;
      for (final Object[] h1 : hops(p, direction, !anyMiddle))
        for (final Object[] h2 : hops((Vertex) h1[1], direction, !anyMiddle)) {
          if (h2[0].equals(h1[0]))
            continue;
          for (final Object[] h3 : hops((Vertex) h2[1], direction, true)) {
            if (h3[0].equals(h1[0]) || h3[0].equals(h2[0]))
              continue;
            final Vertex x = (Vertex) h3[1];
            if (filterAge && x.getInteger("age") <= 36)
              continue;
            ++n;
          }
        }
      return n;
    }

    /** (edge RID, neighbour) pairs of the neighbours reached through KNOWS, a self-loop once under BOTH. */
    private List<Object[]> hops(final Vertex from, final Vertex.DIRECTION direction, final boolean personOnly) {
      final List<Object[]> result = new ArrayList<>();
      final Set<RID> selfLoops = new HashSet<>();
      for (final Edge e : from.getEdges(direction, "KNOWS")) {
        final RID other = e.getOut().equals(from.getIdentity()) ? e.getIn() : e.getOut();
        if (direction == Vertex.DIRECTION.BOTH && e.getOut().equals(e.getIn()) && !selfLoops.add(e.getIdentity()))
          continue;
        final Vertex target = other.asVertex();
        if (personOnly && !target.getTypeName().equals("Person"))
          continue;
        result.add(new Object[] { e.getIdentity(), target });
      }
      return result;
    }

    private long count(final String query, final Object id) {
      try (final ResultSet rs = database.query("opencypher", query, Map.of("id", id))) {
        final Result row = rs.next();
        return ((Number) row.getProperty("n")).longValue();
      }
    }

    private static <T> Iterable<T> iterable(final Iterator<T> iterator) {
      return () -> iterator;
    }

    private AtomicInteger countReads(final String typeName) {
      final AtomicInteger reads = new AtomicInteger();
      final AfterRecordReadListener listener = record -> {
        reads.incrementAndGet();
        return record;
      };
      final RecordEvents events = database.getSchema().getType(typeName).getEvents();
      events.registerListener(listener);
      return reads;
    }
  }

  /**
   * Issue #8725: a Cypher {@code MATCH} with a filter on an unindexed property scanned the label on the calling thread,
   * while the same SQL query has filtered in the workers of a parallel scan since #8523. The label scan now hands a
   * predicate of nodes that read only the row to the same parallel scan, which returns the rows in the order the sequential
   * scan reads them, so a LIMIT keeps the same rows.
   * <p>
   * Every parallel answer is checked against the same query run with parallel scans disabled, rows and order.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8725CypherParallelFilteredScan extends TestHelper {
    private static final int ROWS = 20_000;

    @Override
    protected void beginTest() {
      // Small units, so the fixture's few hundred pages are cut in many of them
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
      createType("OneBucket", 1);
      createType("FourBuckets", 4);
    }

    private void createType(final String type, final int buckets) {
      database.getSchema().createVertexType(type, buckets);
      final Random rnd = new Random(1);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex(type).set("id", i, "grp", rnd.nextInt(100), "name", "n" + rnd.nextInt(50)).save();
      });
    }

    @Test
    void filteredLabelScanRunsInParallelInTheSequentialOrder() {
      for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
        final String query = "MATCH (e:" + type + ") WHERE e.grp = 5 RETURN e.id AS id";
        assertThat(profile(query)).as(type).contains("NodeByLabelScan").contains("[parallel]");
        final List<Object> parallel = column(query, Map.of());
        assertThat(parallel).isNotEmpty().isEqualTo(sequential(query, Map.of()));
      }
    }

    @Test
    void aggregatesAndLimitsKeepTheirAnswers() {
      for (final String type : new String[] { "OneBucket", "FourBuckets" }) {
        for (final String query : new String[] {
            "MATCH (e:" + type + ") WHERE e.grp = 5 RETURN count(*) AS id",
            "MATCH (e:" + type + ") WHERE e.grp < 10 AND e.name <> 'n3' RETURN e.id AS id LIMIT 37",
            "MATCH (e:" + type + ") WHERE e.grp >= 90 OR e.name = 'n7' RETURN e.id AS id SKIP 100 LIMIT 50",
            "MATCH (e:" + type + ") WHERE e.grp IN [1, 2, 3] RETURN sum(e.id) AS id",
            "MATCH (e:" + type + ") WHERE e.name STARTS WITH 'n4' AND e.grp % 7 = 0 RETURN e.id AS id",
            "MATCH (e:" + type + ") WHERE NOT e.grp = 5 AND e.missing IS NULL RETURN count(*) AS id" }) {
          assertThat(column(query, Map.of())).as(query).isEqualTo(sequential(query, Map.of()));
        }
      }
    }

    @Test
    void parametersReachTheWorkers() {
      final String query = "MATCH (e:FourBuckets) WHERE e.grp = $g AND e.name = $n RETURN e.id AS id";
      final Map<String, Object> params = Map.of("g", 12, "n", "n5");
      assertThat(column(query, params)).isNotEmpty().isEqualTo(sequential(query, params));
      assertThat(profile(query, params)).contains("[parallel]");
    }

    @Test
    void temporalAndParameterPredicatesAreThreadSafeAcrossWorkers() {
      database.getSchema().createVertexType("Dated", 4);
      database.transaction(() -> {
        for (int i = 0; i < ROWS; i++)
          database.newVertex("Dated").set("id", i, "day", LocalDate.of(2020, 1, 1).plusDays(i % 1000)).save();
      });
      final String query = "MATCH (d:Dated) WHERE d.day >= $from AND d.day < $to RETURN d.id AS id";
      final Map<String, Object> params = Map.of("from", LocalDate.of(2020, 3, 1), "to", LocalDate.of(2020, 6, 1));
      assertThat(profile(query, params)).contains("[parallel]");
      assertThat(column(query, params)).isNotEmpty().isEqualTo(sequential(query, params));
    }

    @Test
    void stringAndRegexPredicatesRunInParallelAndAgree() {
      for (final String predicate : new String[] { "e.name STARTS WITH 'n1'", "e.name ENDS WITH '7'", "e.name CONTAINS '4'" }) {
        final String query = "MATCH (e:FourBuckets) WHERE " + predicate + " RETURN e.id AS id";
        assertThat(profile(query)).as(query).contains("[parallel]");
        assertThat(column(query, Map.of())).as(query).isNotEmpty().isEqualTo(sequential(query, Map.of()));
      }
    }

    @Test
    void aWorkerFailureFailsTheQueryAndLeavesTheDatabaseUsable() {
      final String query = "MATCH (e:FourBuckets) WHERE e.id / 0 > 1 RETURN count(*) AS n";
      assertThatThrownBy(() -> column(query, Map.of())).isInstanceOf(RuntimeException.class);
      assertThat(column("MATCH (e:FourBuckets) WHERE e.grp = 5 RETURN count(*) AS n", Map.of())).hasSize(1);
    }

    @Test
    void aPredicateWithAFunctionStaysSequential() {
      final String query = "MATCH (e:FourBuckets) WHERE toString(e.grp) = '5' RETURN e.id AS id";
      assertThat(profile(query)).doesNotContain("[parallel]");
      assertThat(column(query, Map.of())).isNotEmpty().isEqualTo(sequential(query, Map.of()));
    }

    @Test
    void anExpansionAboveTheScanAnswersTheSame() {
      database.getSchema().createEdgeType("Knows");
      database.transaction(() -> {
        final var target = database.newVertex("OneBucket").set("id", -1, "grp", -1).save();
        for (final var rs = database.query("sql", "SELECT FROM FourBuckets WHERE grp = 5 LIMIT 5"); rs.hasNext(); )
          rs.next().getVertex().get().newEdge("Knows", target).save();
      });
      final String query = "MATCH (e:FourBuckets)-[:Knows]->(t) WHERE e.grp = 5 RETURN e.id AS id, t.id AS t";
      assertThat(column(query, Map.of())).hasSize(5).isEqualTo(sequential(query, Map.of()));
    }

    @Test
    void inATransactionTheScanStaysOnTheCallingThread() {
      database.transaction(() -> {
        database.newVertex("FourBuckets").set("id", -7, "grp", 5, "name", "tx").save();
        final List<Object> ids = column("MATCH (e:FourBuckets) WHERE e.grp = 5 AND e.id = -7 RETURN e.id AS id", Map.of());
        assertThat(ids).containsExactly(-7);
      });
    }

    private List<Object> sequential(final String query, final Map<String, Object> params) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
      try {
        return column(query, params);
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
      }
    }

    private List<Object> column(final String query, final Map<String, Object> params) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        while (rs.hasNext()) {
          final var row = rs.next();
          values.add(row.getPropertyNames().size() == 1 ? row.getProperty(row.getPropertyNames().iterator().next()) : row.toMap().toString());
        }
      }
      return values;
    }

    private String profile(final String query) {
      return profile(query, Map.of());
    }

    private String profile(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }
  }

  /**
   * Issue #11009: a read-only {@code RETURN DISTINCT} whose items read only variables bound early enumerated every
   * combination the later clauses produce, only for DISTINCT to keep one row of each. Those later clauses decide whether
   * a row survives and nothing else, so they now run as a semi-join: once per input row, stopping at their first row,
   * not at all for an input row whose returned values already qualified, and once per value of what they read when they
   * read none of the returned variables.
   * <p>
   * Every test compares against the full expansion, which is the same query without DISTINCT, deduplicated here.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11009DistinctSemiJoin {
    private static final String DB_PATH = "./target/databases/issue-11009";

    // The shape of the report: everything after the rule reads the rule and what it reaches, never the host
    private static final String RULE_QUERY = """
        MATCH (h:Host)
        OPTIONAL MATCH (h)-[:IN_RULE]-(r:Rule)
        WITH h, r WHERE r.direction = $direction
        OPTIONAL MATCH (r)-[:FROM]-(s:Source)
        WITH h, r, s
        OPTIONAL MATCH (r)-[:USES]-(svc:Service)
        WITH svc, h, r, s
        OPTIONAL MATCH (s)-[:MEMBER]-(m:Host)
        WITH svc, h, r, s, m WHERE $port IN svc.ports OR $port IN r.ports
        MATCH (m)-[:HAS]-(p:Port)
        WITH h, r, s, m, p WHERE p.public = $public
        """;

    private static final int HOSTS = 40;
    private static final int RULES = 8;

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> {
        for (final String type : List.of("Host", "Rule", "Source", "Service", "Port"))
          database.getSchema().createVertexType(type);
        for (final String type : List.of("IN_RULE", "FROM", "USES", "MEMBER", "HAS"))
          database.getSchema().createEdgeType(type);

        final List<MutableVertex> hosts = new ArrayList<>();
        for (int i = 0; i < HOSTS; i++) {
          final MutableVertex host = database.newVertex("Host").set("name", String.format("h%02d", i)).save();
          // Two ports on some hosts, so a member reaches the same answer more than once
          if (i % 3 != 0)
            host.newEdge("HAS", database.newVertex("Port").set("public", i % 2 == 0).save());
          if (i % 5 == 0)
            host.newEdge("HAS", database.newVertex("Port").set("public", true).save());
          hosts.add(host);
        }

        // Each rule applies to many hosts, and a host to several rules
        for (int i = 0; i < RULES; i++) {
          final MutableVertex rule = database.newVertex("Rule").set("direction", i % 4 == 3 ? "OUT" : "IN")
              .set("ports", i % 2 == 0 ? List.of(161) : List.of()).save();
          final MutableVertex source = database.newVertex("Source").save();
          rule.newEdge("FROM", source);
          if (i % 3 == 2)
            rule.newEdge("USES", database.newVertex("Service").set("ports", List.of(161, 162)).save());
          for (int h = i; h < HOSTS; h += 2)
            hosts.get(h).newEdge("IN_RULE", rule);
          for (int m = 0; m < HOSTS; m += i + 3)
            source.newEdge("MEMBER", hosts.get(m));
        }
        // Two hosts with no rule at all: their OPTIONAL MATCH answers a null rule
        for (int i = 0; i < 2; i++)
          database.newVertex("Host").set("name", "lonely" + i).save();
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void reportedShapeAnswersTheFullExpansionAndRunsTheChainOncePerRule() {
      final Map<String, Object> params = Map.of("direction", "IN", "port", 161, "public", true);
      final List<String> expected = sortedDistinct(column(RULE_QUERY + "RETURN h.name AS name", params, "name"));
      assertThat(expected).isNotEmpty().hasSizeLessThan(HOSTS);

      final String query = RULE_QUERY + "RETURN DISTINCT h AS host ORDER BY host.name SKIP 0 LIMIT 50";
      assertThat(explain(query, params)).contains("SEMI JOIN").contains("remembered per r");
      final List<String> hosts = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query, params)) {
        rs.forEachRemaining(r -> hosts.add(r.<Vertex>getProperty("host").getString("name")));
      }
      assertThat(hosts).isEqualTo(expected);

      // The chain reads the rule and never the host: it runs at most once per rule (plus once for the null rule), not
      // once per (host, rule) pair
      final String profile = profile(query, params);
      final long pairs = column(RULE_QUERY.substring(0, RULE_QUERY.indexOf("OPTIONAL MATCH (r)")) + "RETURN h.name AS name",
          params, "name").size();
      assertThat(pairs).isGreaterThan(RULES + 1);
      assertThat(chainRuns(profile)).isPositive().isLessThanOrEqualTo(RULES + 1);

      // A page of the result
      final String paged = RULE_QUERY + "RETURN DISTINCT h.name AS name ORDER BY name SKIP 1 LIMIT 3";
      assertThat(column(paged, params, "name")).isEqualTo(expected.subList(1, Math.min(4, expected.size())));

      // Other parameters, including one no row passes
      for (final Map<String, Object> other : List.of(Map.<String, Object>of("direction", "IN", "port", 162, "public", true),
          Map.<String, Object>of("direction", "IN", "port", 161, "public", false),
          Map.<String, Object>of("direction", "OUT", "port", 161, "public", true),
          Map.<String, Object>of("direction", "IN", "port", 7, "public", true)))
        assertThat(column(RULE_QUERY + "RETURN DISTINCT h.name AS name ORDER BY name", other, "name")).as(other.toString())
            .isEqualTo(sortedDistinct(column(RULE_QUERY + "RETURN h.name AS name", other, "name")));
    }

    @Test
    void keepsTheOrderOfFirstAppearanceWithoutOrderBy() {
      final Map<String, Object> params = Map.of("direction", "IN", "port", 161, "public", true);
      final String query = RULE_QUERY + "RETURN DISTINCT h.name AS name";
      assertThat(explain(query, params)).contains("SEMI JOIN");
      assertThat(column(query, params, "name")).isEqualTo(
          new ArrayList<>(new LinkedHashSet<>(column(RULE_QUERY + "RETURN h.name AS name", params, "name"))));
    }

    @Test
    void anInputRowWhoseReturnedValuesQualifiedIsNotTestedAgain() {
      // The chain reads the host, so nothing is remembered; a host that qualified through one rule skips the chain for
      // every other rule it has
      final String query = """
          MATCH (h:Host)-[:IN_RULE]-(r:Rule) WITH h, r
          MATCH (h)-[:HAS]-(p:Port) WHERE r.direction = 'IN'
          RETURN DISTINCT h.name AS name ORDER BY name""";
      assertThat(explain(query, Map.of())).contains("SEMI JOIN").doesNotContain("remembered per");
      assertThat(column(query, Map.of(), "name")).isEqualTo(sortedDistinct(column(query.replace("DISTINCT ", ""), Map.of(), "name")));

      final String profile = profile(query, Map.of());
      final long pairs = column("MATCH (h:Host)-[:IN_RULE]-(r:Rule) RETURN h.name AS name", Map.of(), "name").size();
      assertThat(qualifiedSkips(profile)).isPositive();
      assertThat(chainRuns(profile) + qualifiedSkips(profile)).isEqualTo(pairs);
    }

    @Test
    void optionalMatchAloneKeepsEveryRow() {
      final String query = "MATCH (h:Host) WITH h OPTIONAL MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS name ORDER BY name";
      assertThat(explain(query, Map.of())).contains("SEMI JOIN");
      assertThat(column(query, Map.of(), "name")).hasSize(HOSTS + 2);
    }

    @Test
    void unwindInTheChainFiltersPerRow() {
      final String query = """
          MATCH (h:Host) WITH h
          MATCH (h)-[:IN_RULE]-(r:Rule) UNWIND r.ports AS port WITH h, port WHERE port = 161
          RETURN DISTINCT h.name AS name ORDER BY name""";
      assertThat(explain(query, Map.of())).contains("SEMI JOIN");
      assertThat(column(query, Map.of(), "name")).isEqualTo(sortedDistinct(column(query.replace("DISTINCT ", ""), Map.of(), "name")));
    }

    @Test
    void keepsMatchingRowsAfterManyThatDoNotMatch() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Bulk");
        final MutableVertex rule = database.newVertex("Rule").set("direction", "BULK").save();
        final MutableVertex other = database.newVertex("Rule").set("direction", "BULK").save();
        for (int i = 0; i < 1_000; i++) {
          final MutableVertex bulk = database.newVertex("Bulk").set("i", i).save();
          // Only the last 50 reach the rule with a port: the first 950 fail, and many batches go by first
          bulk.newEdge("IN_RULE", i >= 950 ? rule : other);
          if (i >= 990)
            bulk.newEdge("HAS", database.newVertex("Port").set("public", true).save());
        }
      });

      final String perRow = "MATCH (b:Bulk) WITH b MATCH (b)-[:HAS]-(p:Port) RETURN DISTINCT b.i AS i";
      assertThat(explain(perRow, Map.of())).contains("SEMI JOIN");
      assertThat(column(perRow, Map.of(), "i")).hasSize(10);

      // Remembered per rule: the 950 rows of the rule without a port fail from memory, the 50 of the other pass
      final String remembered = """
          MATCH (b:Bulk) OPTIONAL MATCH (b)-[:IN_RULE]-(r:Rule) WITH b, r
          MATCH (r)-[:IN_RULE]-(o:Bulk)-[:HAS]-(p:Port) RETURN DISTINCT b.i AS i ORDER BY i""";
      assertThat(explain(remembered, Map.of())).contains("remembered per r");
      final List<String> expected = new ArrayList<>();
      for (int i = 950; i < 1_000; i++)
        expected.add(String.valueOf(i));
      assertThat(column(remembered, Map.of(), "i")).isEqualTo(expected);
      assertThat(chainRuns(profile(remembered, Map.of()))).isEqualTo(2);
    }

    @Test
    void notRememberedWhenTheChainCanAnswerDifferentlyForTheSameValues() {
      // rand() can answer differently each time, and a pattern predicate is not modelled as a function of the names
      for (final String filter : List.of("WHERE rand() < 2", "WHERE (s)<-[:FROM]-(:Rule)")) {
        final String query = "MATCH (h:Host) OPTIONAL MATCH (h)-[:IN_RULE]-(r:Rule) WITH h, r MATCH (r)-[:FROM]-(s:Source) "
            + filter + " RETURN DISTINCT h.name AS name ORDER BY name";
        assertThat(explain(query, Map.of())).as(filter).contains("SEMI JOIN").doesNotContain("remembered per");
        assertThat(column(query, Map.of(), "name")).as(filter).hasSize(HOSTS);
      }
    }

    /**
     * The no-argument temporal functions answer from one clock per statement, so they are functions of nothing in a
     * RETURN item and keep the semi-join: still one row per host.
     */
    @Test
    void aStatementClockInTheReturnKeepsOneRowPerHost() {
      final String query = BASE.replace("AS v", "AS v, datetime() AS t, date() AS d");
      assertThat(explain(query, Map.of())).contains("SEMI JOIN");
      assertThat(column(query, Map.of(), "v")).isEqualTo(
          new ArrayList<>(new LinkedHashSet<>(column(BASE.replace("DISTINCT ", ""), Map.of(), "v"))));
    }

    /**
     * A RETURN item calling a function that answers differently on every call keeps a row per combination, whichever
     * engine the function comes from: a Cypher built-in, an APOC-style registered function, or a bare SQL fallback.
     */
    @Test
    void aVaryingFunctionInTheReturnKeepsEveryRow() {
      final int pairs = column(BASE.replace("DISTINCT ", ""), Map.of(), "v").size();
      for (final String call : List.of("randomUUID()", "apoc.create.uuid()", "create.uuidBase64()",
          "text.random(8)", "sql.uuid()", "sql.date()", "date.currentTimestamp()")) {
        final String query = BASE.replace("AS v", "AS v, " + call + " AS x");
        assertThat(explain(query, Map.of())).as(call).doesNotContain("SEMI JOIN");
        // A clock read in milliseconds can repeat across rows, a random value cannot
        if (!call.contains("date"))
          assertThat(column(query, Map.of(), "v")).as(call).hasSize(pairs);
      }
    }

    /**
     * The allow-list of deterministic functions names only functions a query can call, and none that varies. A name not
     * on it is read as varying, so a function registered later costs the optimization, never a row.
     */
    @Test
    void theDeterministicFunctionsAreRegisteredAndNoneVaries() {
      final Set<String> registered = new TreeSet<>();
      for (final String name : FunctionValidator.getKnownFunctionNames())
        registered.add(name.toLowerCase(Locale.ROOT));
      for (final String name : CypherFunctionRegistry.getFunctionNames())
        registered.add(name.toLowerCase(Locale.ROOT));
      assertThat(registered).containsAll(DistinctSemiJoin.DETERMINISTIC_FUNCTIONS);

      for (final String name : List.of("abs", "toLower", "apoc.coll.sort", "coll.sort", "date", "datetime", "text.join"))
        assertThat(DistinctSemiJoin.isVaryingFunction(name)).as(name).isFalse();
      for (final String name : List.of("rand", "randomUUID", "timestamp", "file", "linenumber", "text.random", "create.uuid",
          "apoc.create.uuid", "create.uuidBase64", "create.vNode", "create.vRelationship", "date.currentTimestamp",
          "datetime.realtime", "sql.date", "sql.uuid", "sql.abs", "my.userFunction", "notRegisteredAnywhere"))
        assertThat(DistinctSemiJoin.isVaryingFunction(name)).as(name).isTrue();
    }

    /**
     * The allow-list is a review of the built-in: a function registered over an allowed name at run time is not known
     * to be pure, so the name stops keeping the semi-join until the built-in is back.
     */
    @Test
    void aFunctionRegisteredOverAnAllowedNameIsReadAsVarying() {
      final StatelessFunction original = CypherFunctionRegistry.get("math.sigmoidPrime");
      assertThat(original).isNotNull();
      assertThat(DistinctSemiJoin.isVaryingFunction("math.sigmoidPrime")).isFalse();
      try {
        CypherFunctionRegistry.registerOrReplace(new StatelessFunction() {
          @Override
          public String getName() {
            return "math.sigmoidPrime";
          }

          @Override
          public Object execute(final Object[] args, final CommandContext context) {
            return Math.random();
          }
        });
        assertThat(DistinctSemiJoin.isVaryingFunction("math.sigmoidPrime")).isTrue();
        assertThat(DistinctSemiJoin.isVaryingFunction("apoc.math.sigmoidPrime")).isTrue();
      } finally {
        CypherFunctionRegistry.registerOrReplace(original);
      }
      assertThat(DistinctSemiJoin.isVaryingFunction("math.sigmoidPrime")).isFalse();
    }

    /**
     * A temporal constructor given only a timezone answers from the statement clock, like its no-argument form: the same
     * instant on every row, so it keeps the semi-join and keeps one row per host.
     */
    @Test
    void aTimezoneTemporalConstructorAnswersTheStatementClock() {
      for (final String call : List.of("datetime('UTC')", "date('UTC')", "time('UTC')", "localdatetime('UTC')",
          "localtime('UTC')")) {
        final String perRow = "UNWIND range(1, 500) AS i RETURN count(DISTINCT toString(" + call + ")) AS n";
        assertThat(column(perRow, Map.of(), "n")).as(call).containsExactly("1");
      }
      assertThat(column("RETURN datetime('UTC').epochMillis = datetime().epochMillis AS t", Map.of(), "t")).containsExactly(
          "true");

      final String query = BASE.replace("AS v", "AS v, datetime('Europe/Rome') AS t");
      assertThat(explain(query, Map.of())).contains("SEMI JOIN");
      assertThat(column(query, Map.of(), "v")).isEqualTo(
          new ArrayList<>(new LinkedHashSet<>(column(BASE.replace("DISTINCT ", ""), Map.of(), "v"))));
    }

    @Test
    void aTwoClauseQueryHasNoChainToRun() {
      // MATCH ... RETURN DISTINCT has no clause between the binding and the RETURN: nothing to test per row
      assertThat(explain("MATCH (h:Host) RETURN DISTINCT h.name AS v", Map.of())).doesNotContain("SEMI JOIN");
    }

    @Test
    void anUncorrelatedChainRunsOnce() {
      // The chain reads nothing of its input row: its answer is the same for every row, and it is asked once
      final String query = "MATCH (h:Host) WITH h MATCH (x:Source) RETURN DISTINCT h.name AS name ORDER BY name";
      assertThat(explain(query, Map.of())).contains("remembered per query");
      assertThat(column(query, Map.of(), "name")).hasSize(HOSTS + 2);
      assertThat(chainRuns(profile(query, Map.of()))).isEqualTo(1);

      final String none = "MATCH (h:Host) WITH h MATCH (x:Source) WHERE x.missing = 1 RETURN DISTINCT h.name AS name";
      assertThat(column(none, Map.of(), "name")).isEmpty();
      assertThat(chainRuns(profile(none, Map.of()))).isEqualTo(1);
    }

    @Test
    void followsAVariableRenamedBeforeTheChain() {
      final String query = "MATCH (h:Host) WITH h AS x MATCH (x)-[:HAS]-(p:Port) RETURN DISTINCT x.name AS name ORDER BY name";
      assertThat(explain(query, Map.of())).contains("SEMI JOIN");
      assertThat(column(query, Map.of(), "name")).isEqualTo(
          sortedDistinct(column("MATCH (h:Host)-[:HAS]-(:Port) RETURN h.name AS name", Map.of(), "name")));
    }

    /**
     * Each query differs from {@link #BASE}, which takes the semi-join, by one thing that makes the later rows count.
     */
    @Test
    void notUsedWhenTheResultNeedsTheLaterRows() {
      assertThat(explain(BASE, Map.of())).contains("SEMI JOIN");

      final String[] queries = {
          // RETURN reads a variable the later MATCH binds
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT p.public AS v",
          // not DISTINCT: every row counts
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN h.name AS v",
          // an aggregation counts the rows
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS v, count(p) AS c",
          // a RETURN item that can answer differently for every row
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS v, rand() AS x",
          // a WITH that looks across rows
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH h, p LIMIT 3 RETURN DISTINCT h.name AS v",
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH DISTINCT h, p RETURN DISTINCT h.name AS v",
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH h, p ORDER BY p.public RETURN DISTINCT h.name AS v",
          // a WITH that rebinds the returned name
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) WITH p AS h RETURN DISTINCT h.public AS v",
          // RETURN *
          "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT *" };
      for (final String query : queries)
        assertThat(explain(query, Map.of())).as(query).doesNotContain("SEMI JOIN");

      // The one with rand() keeps a row per (host, port)
      assertThat(column(queries[3], Map.of(), "v")).hasSize(
          column("MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN h.name AS v", Map.of(), "v").size());
    }

    private static final String BASE = "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) RETURN DISTINCT h.name AS v";

    @Test
    void writeQueriesAreNotAffected() {
      database.transaction(() -> {
        final String write = "MATCH (h:Host) WITH h MATCH (h)-[:HAS]-(p:Port) SET h.seen = true RETURN DISTINCT h.name AS v";
        assertThat(column(write, Map.of(), "v")).hasSize(
            sortedDistinct(column(BASE.replace("DISTINCT ", ""), Map.of(), "v")).size());
      });
      assertThat(column("MATCH (h:Host {seen: true}) RETURN h.name AS v", Map.of(), "v")).hasSize(
          sortedDistinct(column(BASE.replace("DISTINCT ", ""), Map.of(), "v")).size());
    }

    private static List<String> sortedDistinct(final List<String> values) {
      return new ArrayList<>(new TreeSet<>(values));
    }

    private static long chainRuns(final String profile) {
      return counter(profile, "chain runs");
    }

    private static long qualifiedSkips(final String profile) {
      return counter(profile, "already qualified");
    }

    private static long counter(final String profile, final String name) {
      final Matcher matcher = Pattern.compile(name + ": (\\d+)").matcher(profile);
      assertThat(matcher.find()).as(profile).isTrue();
      return Long.parseLong(matcher.group(1));
    }

    private String explain(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, params)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private String profile(final String query, final Map<String, Object> params) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private List<String> column(final String query, final Map<String, Object> params, final String name) {
      final List<String> values = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        rs.forEachRemaining(r -> values.add(String.valueOf((Object) r.getProperty(name))));
      }
      return values;
    }
  }

  /**
   * Issues #11081, #11082 and #11083: three read-only shapes that did work their result could not depend on.
   * <ul>
   * <li>#11081: a {@code LIMIT 0} drained its whole input, which only a write behind it needs;</li>
   * <li>#11082: a {@code CALL { }} body, and a UNION inside it, collected every row before the first one was handed on, so
   * a {@code LIMIT} after the CALL still ran every branch to the end;</li>
   * <li>#11083: an {@code OPTIONAL MATCH} ran its expansion once per input row even when consecutive rows held the same
   * values for everything it reads, which after a fan-out of the same vertex repeated an identical walk of its whole
   * adjacency for every row.</li>
   * </ul>
   * The work is counted, never timed: {@link #PROBE} is a function that counts its calls, placed in a predicate of the part
   * that should not run, and the OPTIONAL MATCH reports its chain runs in the profile.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11081ReadOnlyBoundedDemand {
    private static final String DB_PATH = "./target/databases/issue-11081";
    private static final String PROBE   = "issue11081.probe";
    private static final int    HIGH    = 2_000;

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

      // The graph of the reports: one root, 2 low leaves, 3 witness leaves and many high leaves
      for (final String type : List.of("GspRoot", "GspLow", "GspHigh", "GspWitness")) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".uid LONG");
        database.command("sql", "CREATE INDEX ON " + type + " (uid) NOTUNIQUE");
      }
      for (final String type : List.of("GSP_LOW", "GSP_HIGH", "GSP_WITNESS"))
        database.command("sql", "CREATE EDGE TYPE " + type);

      database.transaction(() -> {
        database.command("opencypher", "CREATE (:GspRoot {uid:0})");
        database.command("opencypher",
            "UNWIND range(1,2) AS i MATCH (r:GspRoot {uid:0}) CREATE (r)-[:GSP_LOW]->(:GspLow {uid:i,keep:true})");
        database.command("opencypher",
            "UNWIND range(3,5) AS i MATCH (r:GspRoot {uid:0}) CREATE (r)-[:GSP_WITNESS]->(:GspWitness {uid:i,keep:true})");
        database.command("opencypher", "UNWIND range(6,$last) AS i MATCH (r:GspRoot {uid:0}) "
            + "CREATE (r)-[:GSP_HIGH]->(:GspHigh {uid:i,keep:true})", Map.of("last", 5 + HIGH));
      });
      PROBES.set(0);
    }

    @AfterEach
    void tearDown() {
      CypherFunctionRegistry.unregister(PROBE);
      if (database != null)
        database.drop();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // #11081: a read-only LIMIT 0 reads nothing
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void existsWithLimitZeroDoesNotRunItsBody() {
      // The shape of the report
      final String query = "RETURN EXISTS { MATCH (r:GspRoot)-[:GSP_HIGH]->(n:GspHigh) WHERE n.keep = true AND " + PROBE
          + "(n) RETURN 1 AS item LIMIT 0 } AS answer";
      assertThat(single(query, "answer")).isEqualTo(false);
      assertThat(PROBES.get()).isZero();

      // Without the LIMIT 0 the body runs, and answers true
      assertThat(single(query.replace(" LIMIT 0", ""), "answer")).isEqualTo(true);
      assertThat(PROBES.get()).isPositive();
    }

    @Test
    void statementLevelLimitZeroDoesNotRunWhatIsBehindIt() {
      assertThat(rows("MATCH (n:GspHigh) WHERE " + PROBE + "(n) RETURN n.uid AS uid LIMIT 0")).isEmpty();
      assertThat(rows("MATCH (n:GspHigh) WHERE " + PROBE + "(n) WITH n ORDER BY n.uid LIMIT 0 RETURN n.uid AS uid")).isEmpty();
      assertThat(rows("MATCH (n:GspHigh) WHERE " + PROBE + "(n) WITH count(n) AS c LIMIT 0 RETURN c")).isEmpty();
      assertThat(PROBES.get()).isZero();
    }

    @Test
    void writeStatementLimitZeroStillRunsItsWrites() {
      database.transaction(() -> assertThat(commandRows("MATCH (n:GspLow) SET n.touched = true RETURN n LIMIT 0")).isEmpty());
      assertThat(single("MATCH (n:GspLow) WHERE n.touched = true RETURN count(n) AS c", "c")).isEqualTo(2L);

      // The write is inside a CALL body: the statement can still write, so the LIMIT 0 still drains
      database.transaction(
          () -> assertThat(commandRows("UNWIND range(1, 3) AS i CALL (i) { CREATE (:GspLow {uid: 100 + i}) } RETURN i LIMIT 0")).isEmpty());
      assertThat(single("MATCH (n:GspLow) WHERE n.uid > 100 RETURN count(n) AS c", "c")).isEqualTo(3L);

      // The write is a procedure: the statement can write, so the LIMIT 0 still drains
      database.transaction(() -> assertThat(commandRows("UNWIND range(1, 2) AS i CALL merge.node(['GspLow'], {uid: 300 + i}, {}, "
          + "{}) YIELD node RETURN node LIMIT 0")).isEmpty());
      assertThat(single("MATCH (n:GspLow) WHERE n.uid > 300 RETURN count(n) AS c", "c")).isEqualTo(2L);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // #11082: a read-only CALL body streams, so a LIMIT after it stops the body
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    void callUnionAllStopsAtTheLimit() {
      // The shape of the report: the witness branch alone satisfies LIMIT 3
      final String query = """
          CALL () {
            MATCH (r:GspRoot)-[:GSP_WITNESS]->(w:GspWitness) WHERE w.keep = true RETURN 1 AS item
            UNION ALL
            MATCH (r:GspRoot)-[:GSP_HIGH]->(n:GspHigh) WHERE n.keep = true AND %s(n) RETURN 1 AS item
          }
          RETURN item AS answer LIMIT 3""".formatted(PROBE);
      assertThat(column(query, "answer")).containsExactly(1L, 1L, 1L);
      // The body is asked for the three rows the LIMIT wants: the second branch never starts
      assertThat(PROBES.get()).isZero();

      // Every row is still there when the consumer asks for all of them
      PROBES.set(0);
      assertThat(column(query.replace(" LIMIT 3", ""), "answer")).hasSize(3 + HIGH);
      assertThat(PROBES.get()).isEqualTo(HIGH);
    }

    @Test
    void callBodyStopsAtTheLimit() {
      // The probe is in the body's RETURN, so it counts the rows the body produced: the two the LIMIT wants, of 2,000
      final String query = "CALL () { MATCH (n:GspHigh) RETURN n.uid AS uid, " + PROBE + "(n) AS p } RETURN uid LIMIT 2";
      assertThat(column(query, "uid")).hasSize(2);
      assertThat(PROBES.get()).isEqualTo(2);

      // A body run per outer row: each one stops where the consumer does
      PROBES.set(0);
      assertThat(column("UNWIND range(1, 5) AS k CALL (k) { MATCH (n:GspHigh) RETURN n.uid AS uid, " + PROBE
          + "(n) AS p } RETURN k, uid LIMIT 3", "uid")).hasSize(3);
      assertThat(PROBES.get()).isEqualTo(3);
    }

    @Test
    void streamedCorrelatedAndOptionalCallsAnswerAsBefore() {
      // Correlated: one body run per outer row, each streamed
      assertThat(rows("UNWIND [0, 1] AS k CALL (k) { MATCH (l:GspLow) WHERE l.uid > k RETURN l.uid AS u } "
          + "RETURN k, u ORDER BY k, u")).containsExactly("0,1", "0,2", "1,2");
      // An OPTIONAL CALL whose body has no row keeps the outer row
      assertThat(rows("UNWIND [0, 99] AS k OPTIONAL CALL (k) { MATCH (l:GspLow) WHERE l.uid > k RETURN l.uid AS u } "
          + "RETURN k, u ORDER BY k, u")).containsExactly("0,1", "0,2", "99,null");
      // A plain CALL whose body has no row drops it
      assertThat(rows("UNWIND [0, 99] AS k CALL (k) { MATCH (l:GspLow) WHERE l.uid > k RETURN l.uid AS u } "
          + "RETURN k, u ORDER BY k, u")).containsExactly("0,1", "0,2");
      // A nested read-only CALL, partially consumed by an inner LIMIT
      assertThat(rows("UNWIND [1, 2] AS k CALL (k) { CALL () { MATCH (w:GspWitness) RETURN w.uid AS u } RETURN u ORDER BY u "
          + "LIMIT 2 } RETURN k, u ORDER BY k, u")).containsExactly("1,3", "1,4", "2,3", "2,4");
    }

    @Test
    void callUnionDeduplicatesLikeAStatementLevelUnion() {
      // A UNION inside a CALL body de-duplicated on the text of the values, where a statement-level UNION compares them:
      // 1 and 1.0 were two rows there and one here
      final List<String> statementLevel = rows("RETURN 1 AS x UNION RETURN 1.0 AS x UNION RETURN 1 AS x");
      final List<String> inCall = rows("CALL () { RETURN 1 AS x UNION RETURN 1.0 AS x UNION RETURN 1 AS x } RETURN x");
      assertThat(inCall).hasSameSizeAs(statementLevel);

      assertThat(rows("CALL () { MATCH (w:GspWitness) RETURN w.uid AS u UNION MATCH (w:GspWitness) RETURN w.uid AS u } "
          + "RETURN u ORDER BY u")).containsExactly("3", "4", "5");
      assertThat(rows("CALL () { MATCH (w:GspWitness) RETURN w.uid AS u UNION ALL MATCH (w:GspWitness) RETURN w.uid AS u } "
          + "RETURN u ORDER BY u")).containsExactly("3", "3", "4", "4", "5", "5");
    }

    @Test
    void writingCallUnionRunsEveryBranch() {
      database.transaction(() -> assertThat(commandRows("""
          CALL () {
            CREATE (a:GspLow {uid: 200}) RETURN a.uid AS u
            UNION ALL
            CREATE (b:GspLow {uid: 201}) RETURN b.uid AS u
          }
          RETURN u LIMIT 1""")).hasSize(1));
      assertThat(single("MATCH (n:GspLow) WHERE n.uid >= 200 RETURN count(n) AS c", "c")).isEqualTo(2L);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // #11083: an OPTIONAL MATCH remembers the matches of the last input row
    // ---------------------------------------------------------------------------------------------------------------

    private static final String LOW_THEN_HIGH = "MATCH (r:GspRoot {uid:0}) OPTIONAL MATCH (r)-[:GSP_LOW]->(l:GspLow) "
        + "OPTIONAL MATCH (r)-[:GSP_HIGH]->(h:GspHigh) RETURN count(h) AS answer";
    private static final String HIGH_THEN_LOW = "MATCH (r:GspRoot {uid:0}) OPTIONAL MATCH (r)-[:GSP_HIGH]->(h:GspHigh) "
        + "OPTIONAL MATCH (r)-[:GSP_LOW]->(l:GspLow) RETURN count(h) AS answer";

    @Test
    void swappedOptionalMatchesRunTheLowBranchOnce() {
      assertThat(single(LOW_THEN_HIGH, "answer")).isEqualTo(2L * HIGH);
      assertThat(single(HIGH_THEN_LOW, "answer")).isEqualTo(2L * HIGH);

      // The low branch follows the high fan-out: every one of its input rows holds the same root
      final String profile = profile(HIGH_THEN_LOW);
      assertThat(counter(profile, "reused per l, r]", "match runs")).isEqualTo(1);
      assertThat(counter(profile, "reused per l, r]", "reused")).isEqualTo(HIGH - 1);
    }

    @Test
    void rememberedMatchesKeepEveryRowOfEachInput() {
      final String query = "MATCH (r:GspRoot) OPTIONAL MATCH (r)-[:GSP_HIGH]->(h:GspHigh) WHERE h.uid <= 8 "
          + "OPTIONAL MATCH p = (r)-[e:GSP_LOW]->(l:GspLow) RETURN h.uid AS h, l.uid AS l, type(e) AS t, length(p) AS len "
          + "ORDER BY h, l";
      assertThat(rows(query)).containsExactly("6,1,GSP_LOW,1", "6,2,GSP_LOW,1", "7,1,GSP_LOW,1", "7,2,GSP_LOW,1", "8,1,GSP_LOW,1",
          "8,2,GSP_LOW,1");
      assertThat(counter(profile(query), "reused per e, l, p, r]", "match runs")).isEqualTo(1);
      assertThat(rows(query)).isEqualTo(rows(notRemembered(query)));
    }

    @Test
    void theKeyHoldsEveryNameTheClauseReads() {
      // Same root on every row, but the WHERE reads k too: each k has its own match
      final String where = "MATCH (r:GspRoot) UNWIND [3, 3, 4, 9, 5] AS k OPTIONAL MATCH (r)-[:GSP_WITNESS]->(w:GspWitness) "
          + "WHERE w.uid = k RETURN k, w.uid AS w";
      assertThat(rows(where)).containsExactly("3,3", "3,3", "4,4", "9,null", "5,5");
      assertThat(counter(profile(where), "reused per k, r, w]", "match runs")).isEqualTo(4);

      // The same through an inline property map
      final String inline = "MATCH (r:GspRoot) UNWIND [3, 3, 4, 9, 5] AS k "
          + "OPTIONAL MATCH (r)-[:GSP_WITNESS]->(w:GspWitness {uid: k}) RETURN k, w.uid AS w";
      assertThat(rows(inline)).containsExactly("3,3", "3,3", "4,4", "9,null", "5,5");
    }

    @Test
    void recordingPausesWhileNoKeyRepeatsAndResumesWhenOneDoes() {
      // 100 keys that never repeat, then one repeated three times
      final String query = "MATCH (r:GspRoot) UNWIND range(1, 100) + [4, 4, 4] AS k "
          + "OPTIONAL MATCH (r)-[:GSP_WITNESS]->(w:GspWitness) WHERE w.uid = k RETURN k, w.uid AS w";
      final List<String> rows = rows(query);
      assertThat(rows).hasSize(103);
      assertThat(rows.subList(100, 103)).containsExactly("4,4", "4,4", "4,4");
      assertThat(rows).isEqualTo(rows(notRemembered(query)));

      // Past the first 64 runs nothing is recorded; the first repeat turns recording back on, the second one is answered
      final String profile = profile(query);
      assertThat(counter(profile, "reused per k, r, w]", "match runs")).isEqualTo(102);
      assertThat(counter(profile, "reused per k, r, w]", "reused")).isEqualTo(1);
    }

    @Test
    void matchesPastTheCapAreStreamedWithoutBeingRemembered() {
      // 2,000 x 3 x 2 = 12,000 matches per input row, past the 10,000 the memo keeps: each row runs the chain again
      final String query = "UNWIND [1, 2] AS i OPTIONAL MATCH (a:GspHigh), (b:GspWitness), (c:GspLow) RETURN count(*) AS n";
      assertThat(single(query, "n")).isEqualTo(2L * HIGH * 3 * 2);
      final String profile = profile(query);
      assertThat(counter(profile, "reused per a, b, c]", "match runs")).isEqualTo(2);
      assertThat(counter(profile, "reused per a, b, c]", "reused")).isZero();
    }

    @Test
    void anInputOfWholeBatchesEndsWithItsLastRow() {
      // A step that fills its buffer no longer asks its input whether more follows: it learns it on the next pull
      assertThat(column("UNWIND range(1, 200) AS i RETURN i", "i")).hasSize(200);
      assertThat(column("UNWIND range(1, 300) AS i WITH i WHERE i % 3 = 0 RETURN i", "i")).hasSize(100);
      assertThat(column("UNWIND range(1, 200) AS i RETURN DISTINCT i % 100 AS i", "i")).hasSize(100);
      assertThat(column("MATCH (n:GspHigh) WHERE n.uid > 5 RETURN n.uid AS uid", "uid")).hasSize(HIGH);
    }

    @Test
    void anEmptyAnswerIsRememberedAsWell() {
      final String query = "UNWIND range(1, 4) AS i MATCH (r:GspRoot) OPTIONAL MATCH (r)<-[:GSP_LOW]-(x) RETURN i, x ORDER BY i";
      assertThat(rows(query)).containsExactly("1,null", "2,null", "3,null", "4,null");
      final String profile = profile(query);
      assertThat(counter(profile, "reused per r, x]", "match runs")).isEqualTo(1);
      assertThat(counter(profile, "reused per r, x]", "reused")).isEqualTo(3);
    }

    @Test
    void anUncorrelatedClauseRunsOnce() {
      final String query = "UNWIND range(1, 5) AS i OPTIONAL MATCH (w:GspWitness) RETURN i, w.uid AS w ORDER BY i, w";
      assertThat(rows(query)).hasSize(15);
      assertThat(counter(profile(query), "reused per w]", "match runs")).isEqualTo(1);
    }

    @Test
    void aNullOrAbsentKeyIsComparedAsSuch() {
      // a is bound to null by the first OPTIONAL MATCH: the second one matches nothing for it
      final String query = "UNWIND [1, 2] AS i OPTIONAL MATCH (a:GspRoot {uid: 99}) OPTIONAL MATCH (a)-[:GSP_LOW]->(l) "
          + "RETURN i, a, l ORDER BY i";
      assertThat(rows(query)).containsExactly("1,null,null", "2,null,null");
    }

    @Test
    void relationshipUniquenessIsPerClause() {
      // e2 may be the same edge as e1: uniqueness holds within a MATCH clause, not across clauses
      final String query = "MATCH (r:GspRoot)-[e1:GSP_LOW]->(l1) OPTIONAL MATCH (r)-[e2:GSP_LOW]->(l2) "
          + "RETURN l1.uid AS a, l2.uid AS b ORDER BY a, b";
      assertThat(rows(query)).containsExactly("1,1", "1,2", "2,1", "2,2");
      assertThat(rows(query)).isEqualTo(rows(notRemembered(query)));
    }

    @Test
    void notRememberedWhenTheAnswerCanVaryOrTheStatementWrites() {
      assertThat(explain(HIGH_THEN_LOW)).contains("matches reused per");
      // A function that can answer differently for the same arguments
      assertThat(explain(notRemembered(HIGH_THEN_LOW))).contains("reused per h, r]").doesNotContain("reused per l, r]");
      // A subquery expression, whose reads are not modelled as reads of the row
      assertThat(explain("MATCH (r:GspRoot) OPTIONAL MATCH (r)-[:GSP_LOW]->(l) WHERE EXISTS { (l)<--(r) } RETURN l"))
          .doesNotContain("matches reused per");
      // A statement that writes
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "EXPLAIN MATCH (r:GspRoot) OPTIONAL MATCH (r)-[:GSP_LOW]->(l) "
            + "SET l.seen = true RETURN l")) {
          assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("matches reused per");
        }
      });
    }

    /** The same query with a predicate that turns the memo off and keeps every row: the reference answer. */
    private static String notRemembered(final String query) {
      final int at = query.lastIndexOf("OPTIONAL MATCH");
      final int returnAt = query.indexOf(" RETURN ", at);
      final String clause = query.substring(at, returnAt);
      final String guarded = clause.contains(" WHERE ") ? clause + " AND rand() < 2" : clause + " WHERE rand() < 2";
      return query.substring(0, at) + guarded + query.substring(returnAt);
    }

    // ---------------------------------------------------------------------------------------------------------------

    private Object single(final String query, final String column) {
      final List<Object> values = column(query, column);
      assertThat(values).hasSize(1);
      return values.getFirst();
    }

    private List<Object> column(final String query, final String column) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext())
          values.add(rs.next().getProperty(column));
      }
      return values;
    }

    /** Every row as its values joined by commas, in the order of the RETURN items. */
    private List<String> rows(final String query) {
      try (final ResultSet rs = database.query("opencypher", query)) {
        return rows(rs);
      }
    }

    private List<String> commandRows(final String query) {
      try (final ResultSet rs = database.command("opencypher", query)) {
        return rows(rs);
      }
    }

    private static List<String> rows(final ResultSet rs) {
      final List<String> rows = new ArrayList<>();
      while (rs.hasNext()) {
        final Result row = rs.next();
        final List<String> values = new ArrayList<>();
        for (final String name : row.getPropertyNames())
          values.add(String.valueOf(row.<Object>getProperty(name)));
        rows.add(String.join(",", values));
      }
      return Collections.unmodifiableList(rows);
    }

    private String explain(final String query) {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private String profile(final String query) {
      try (final ResultSet rs = database.query("opencypher", "PROFILE " + query)) {
        while (rs.hasNext())
          rs.next();
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    /** A counter the OPTIONAL MATCH step whose header contains {@code step} reports in the profile. */
    private static long counter(final String profile, final String step, final String name) {
      for (final String line : profile.split("\n"))
        if (line.contains(step)) {
          final Matcher matcher = Pattern.compile(name + ": (\\d+)").matcher(line);
          assertThat(matcher.find()).as(line).isTrue();
          return Long.parseLong(matcher.group(1));
        }
      throw new AssertionError("No step " + step + " in\n" + profile);
    }
  }
}
