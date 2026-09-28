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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue8537ExpandWithoutEdgeLoadTest {
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
   * instead of once per expanded path: a STRING parameter the index would coerce to the LONG key must match nothing.
   */
  @Test
  void anchorPredicateKeepsCypherEqualitySemantics() {
    assertThat(count("MATCH (p:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(x:Person) WHERE p.id = $id RETURN count(*) AS n", "1")).isZero();
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
