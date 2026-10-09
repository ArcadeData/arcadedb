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
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9600: {@code MATCH ()-[e:KNOWS]->() RETURN count(e)} and {@code MATCH ()-[e]->() RETURN count(e)} walked every edge
 * of the graph, and {@code MATCH (n) RETURN count(n)} loaded every vertex. A relationship counted by its own variable is
 * {@code count(*)} when a non-optional MATCH binds it, one hop between two unconstrained nodes is the number of edges of its
 * types (read off a view's totals or one walk of the edge lists, never off the edge type's record counter, which a light edge
 * is not in), and an unlabelled node is every vertex (the sum of the vertex types' counters).
 * <p>
 * Every count is checked against the row pipeline, reached through a {@code sum} that no push-down takes, and against a
 * count taken off the vertices' edge lists by hand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9600EdgeCountPushDownTest extends TestHelper {
  private static final String EDGE_COUNT = "COUNT EDGES";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Student EXTENDS Person");
    database.command("sql", "CREATE VERTEX TYPE City");
    database.command("sql", "CREATE EDGE TYPE KNOWS");
    database.command("sql", "CREATE EDGE TYPE KNOWS_WELL EXTENDS KNOWS");
    database.command("sql", "CREATE EDGE TYPE LIVES_IN");
    database.command("sql", "CREATE EDGE TYPE LIKES LIGHTWEIGHT");
  }

  @Test
  void theReportedQueriesArePushedDownAndExact() {
    buildGraph(7, 300);
    assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
    assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
    assertVertexCount();
  }

  @Test
  void everyDirectedAndUndirectedSpellingCountsLikeThePipeline() {
    buildGraph(11, 250);
    for (final String count : new String[] { "count(e)", "count(*)" }) {
      assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN " + count + " AS n", "KNOWS", Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH (a)<-[e:KNOWS]-(b) RETURN " + count + " AS n", "KNOWS", Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e:KNOWS]-() RETURN " + count + " AS n", "KNOWS", Vertex.DIRECTION.BOTH);
      assertEdgeCount("MATCH ()-[e]-() RETURN " + count + " AS n", null, Vertex.DIRECTION.BOTH);
      assertEdgeCount("MATCH ()-[e:KNOWS_WELL]->() RETURN " + count + " AS n", "KNOWS_WELL", Vertex.DIRECTION.OUT);
      // a light edge has no record: the type's counter would miss every one of them
      assertEdgeCount("MATCH ()-[e:LIKES]->() RETURN " + count + " AS n", "LIKES", Vertex.DIRECTION.OUT);
    }
  }

  /**
   * A unidirectional type stores its edges on the outgoing side only, which is where a directed count reads them; its
   * incoming side is not stored, so an undirected hop over it is left to the row pipeline, which answers that side.
   */
  @Test
  void aUnidirectionalTypeIsCountedFromItsSources() {
    database.command("sql", "CREATE EDGE TYPE FOLLOWS UNIDIRECTIONAL");
    buildGraph(31, 200);
    final Random random = new Random(31);
    database.transaction(() -> {
      final List<Vertex> people = new ArrayList<>();
      for (final Iterator<Record> it = database.iterateType("Person", true); it.hasNext(); )
        people.add(it.next().asVertex());
      for (final Vertex person : people)
        for (int k = random.nextInt(3); k > 0; k--)
          person.modify().newEdge("FOLLOWS", people.get(random.nextInt(people.size())));
    });
    assertEdgeCount("MATCH ()-[e:FOLLOWS]->() RETURN count(e) AS n", "FOLLOWS", Vertex.DIRECTION.OUT);
    assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
    assertPipelineOnly("MATCH ()-[e:FOLLOWS]-() RETURN count(e) AS n", byHand("FOLLOWS", Vertex.DIRECTION.BOTH));
    assertPipelineOnly("MATCH ()-[e]-() RETURN count(e) AS n", byHand(null, Vertex.DIRECTION.BOTH));
  }

  @Test
  void alternativesCountEachEdgeOnce() {
    buildGraph(13, 200);
    final long knows = byHand("KNOWS", Vertex.DIRECTION.OUT);
    final long likes = byHand("LIKES", Vertex.DIRECTION.OUT);
    assertThat(pushedDown("MATCH ()-[e:KNOWS|LIKES]->() RETURN count(e) AS n")).isEqualTo(knows + likes);
    // KNOWS_WELL is a KNOWS: naming both is still the KNOWS family once
    assertThat(pushedDown("MATCH ()-[e:KNOWS|KNOWS_WELL]->() RETURN count(e) AS n")).isEqualTo(knows);
    assertThat(pushedDown("MATCH ()-[e:KNOWS_WELL|KNOWS|KNOWS]->() RETURN count(e) AS n")).isEqualTo(knows);
    // a name that is no edge type matches nothing
    assertThat(pushedDown("MATCH ()-[e:KNOWS|NOT_A_TYPE]->() RETURN count(e) AS n")).isEqualTo(knows);
  }

  @Test
  void shapesTheOperatorDoesNotCountStayExact() {
    buildGraph(17, 200);
    // a name that is no edge type, alone, and a vertex type written as a relationship type
    assertPipelineOnly("MATCH ()-[e:NOT_A_TYPE]->() RETURN count(e) AS n", 0L);
    assertPipelineOnly("MATCH ()-[e:Person]->() RETURN count(e) AS n", 0L);
    // a node named at both ends keeps the self loops only
    assertPipelineOnly("MATCH (a)-[e:KNOWS]->(a) RETURN count(e) AS n", null);
    // a labelled end is a filter
    assertPipelineOnly("MATCH (:Student)-[e:KNOWS]->() RETURN count(e) AS n", null);
    // a relationship counted DISTINCT, or bound by an OPTIONAL MATCH where it can be null, is not count(*)
    assertPipelineOnly("MATCH ()-[e:KNOWS]->() RETURN count(DISTINCT e) AS n", null);
    assertPipelineOnly("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS]->() RETURN count(e) AS n", null);
    assertPipelineOnly("OPTIONAL MATCH (n:Student {age: 99}) RETURN count(n) AS n", 0L);
    // a property filter on the relationship, as a map or as an inline WHERE that naming it allows
    assertPipelineOnly("MATCH ()-[e:KNOWS {since: 3}]->() RETURN count(e) AS n", null);
    assertPipelineOnly("MATCH ()-[e:KNOWS WHERE e.since = 3]->() RETURN count(e) AS n", null);
    assertThat(plan("MATCH (:Student)-[e:KNOWS WHERE e.since = 3]->() RETURN count(e) AS n")).doesNotContain("COUNT CHAIN");
    assertThat(count("MATCH (:Student)-[e:KNOWS WHERE e.since = 3]->() RETURN count(e) AS n"))
        .isEqualTo(count("MATCH (:Student)-[e:KNOWS WHERE e.since = 3]->() RETURN sum(1) AS n"));
  }

  @Test
  void uncommittedEdgesAndVerticesAreCounted() {
    buildGraph(19, 120);
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Person").save();
      final MutableVertex b = database.newVertex("Student").save();
      a.newEdge("KNOWS", b);
      b.newEdge("KNOWS_WELL", a);
      a.newEdge("KNOWS", a);
      a.newLightEdge("KNOWS", b);
      a.newLightEdge("LIKES", b);
      // and edges deleted in the same transaction
      final List<Record> doomed = new ArrayList<>();
      for (final Iterator<Record> it = database.iterateType("KNOWS", true); it.hasNext() && doomed.size() < 4; )
        doomed.add(it.next());
      for (final Record edge : doomed)
        edge.asEdge().delete();
      assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
      assertEdgeCount("MATCH ()-[e:KNOWS]-() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.BOTH);
      assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
      assertVertexCount();
    });
  }

  @Test
  void aViewAnswersTheSameCountsBeforeAndAfterItTakesChanges() {
    buildGraph(23, 250);
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("counts")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).build();
    try {
      assertThat(view.isReady()).isTrue();
      assertThat(view.hasPendingChanges()).isFalse();
      assertAllEdgeCounts();

      // the totals themselves: an unknown type holds nothing, a sub-type listed with its super-type is counted once
      assertThat(view.countAllEdges("NOT_A_TYPE")).isZero();
      assertThat(view.countAllEdges("KNOWS", "KNOWS_WELL")).isEqualTo(view.countAllEdges("KNOWS"));
      assertThat(view.countAllEdges("KNOWS")).isEqualTo(byHand("KNOWS", Vertex.DIRECTION.OUT));
      assertThat(view.countAllEdges()).isEqualTo(byHand(null, Vertex.DIRECTION.OUT));

      // committed after the build: the totals of the slices are no longer the graph, the per-node counts are. Record edges
      // only: a light edge created after the build never reaches the overlay, whatever reads the view (issue #9572)
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Person").save();
        final MutableVertex b = database.newVertex("Person").save();
        a.newEdge("KNOWS", b);
        a.newEdge("KNOWS", a);
        b.newEdge("KNOWS_WELL", a);
        b.newEdge("LIVES_IN", database.newVertex("City").save());
      });
      assertThat(view.hasPendingChanges()).isTrue();
      assertAllEdgeCounts();

      // and edges deleted after the build: a regular one, a sub-type one and a self loop
      database.transaction(() -> {
        final List<Record> edges = new ArrayList<>();
        for (final Iterator<Record> it = database.iterateType("KNOWS", true); it.hasNext() && edges.size() < 6; )
          edges.add(it.next());
        for (final Record edge : edges)
          edge.asEdge().delete();
      });
      assertAllEdgeCounts();
    } finally {
      view.drop();
    }
  }

  /**
   * A view that leaves a vertex type out does not hold the edges that reach it: the count is not read off it, and the walk of
   * the edge lists answers instead.
   */
  @Test
  void aViewOverSomeVertexTypesIsNotUsedForTheCount() {
    buildGraph(37, 200);
    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("people").withVertexTypes("Person", "Student")
        .build();
    try {
      assertThat(view.isReady()).isTrue();
      assertThat(view.coversVertexType(null)).isFalse();
      // LIVES_IN reaches the cities, which the view does not map
      assertEdgeCount("MATCH ()-[e:LIVES_IN]->() RETURN count(e) AS n", "LIVES_IN", Vertex.DIRECTION.OUT);
      assertAllEdgeCounts();
    } finally {
      view.drop();
    }
  }

  /** An edge type with two super-types belongs to two families, and an edge of it is still one relationship. */
  @Test
  void anEdgeTypeWithTwoSuperTypesIsCountedOnce() {
    database.command("sql", "CREATE EDGE TYPE MENTORS");
    database.command("sql", "CREATE EDGE TYPE COACHES EXTENDS KNOWS, MENTORS");
    buildGraph(41, 120);
    database.transaction(() -> {
      final List<Vertex> people = new ArrayList<>();
      for (final Iterator<Record> it = database.iterateType("Person", true); it.hasNext(); )
        people.add(it.next().asVertex());
      for (int i = 0; i + 1 < people.size(); i += 3) {
        people.get(i).modify().newEdge("COACHES", people.get(i + 1));
        people.get(i + 1).modify().newEdge("MENTORS", people.get(i));
      }
    });
    final long knows = byHand("KNOWS", Vertex.DIRECTION.OUT);
    final long mentors = byHand("MENTORS", Vertex.DIRECTION.OUT);
    final long coaches = byHand("COACHES", Vertex.DIRECTION.OUT);
    assertThat(coaches).isPositive();
    assertThat(pushedDown("MATCH ()-[e:KNOWS|MENTORS]->() RETURN count(e) AS n")).isEqualTo(knows + mentors - coaches);
    assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
    assertEdgeCount("MATCH ()-[e]-() RETURN count(e) AS n", null, Vertex.DIRECTION.BOTH);

    final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("coaching").build();
    try {
      assertThat(pushedDown("MATCH ()-[e:KNOWS|MENTORS]->() RETURN count(e) AS n")).isEqualTo(knows + mentors - coaches);
      assertThat(pushedDown("MATCH ()-[e:KNOWS|MENTORS]-() RETURN count(e) AS n"))
          .isEqualTo(count("MATCH ()-[e:KNOWS|MENTORS]-() RETURN sum(1) AS n"));
      assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
    } finally {
      view.drop();
    }
  }

  /** A name a non-optional MATCH binds is never null, even when an OPTIONAL MATCH writes it again. */
  @Test
  void aNameBoundByTheMandatoryMatchCountsEveryRow() {
    buildGraph(43, 150);
    final String query = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) OPTIONAL MATCH (q)-[:KNOWS]->(p) RETURN count(p) AS n";
    assertThat(count(query)).isEqualTo(count(pipeline(query)));
    assertThat(count(query.replace("count(p)", "count(q)"))).isEqualTo(count(pipeline(query.replace("count(p)", "count(q)"))));

    // a WITH that binds the counted name to what may be null makes count(p) a count of the non-null values again
    final String rebound = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) WITH q AS p RETURN count(p) AS n";
    assertThat(count(rebound)).isEqualTo(count(pipeline(rebound)))
        .isEqualTo(count("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(q:Person) RETURN sum(CASE WHEN q IS NULL THEN 0 ELSE 1 END) AS n"));
  }

  /** {@code MATCH (n)}: a subtype's vertices are a supertype's too, and are counted once. */
  @Test
  void anUnlabelledNodeCountsEveryVertexOnce() {
    buildGraph(29, 150);
    // documents and edges are records too, and no node
    database.command("sql", "CREATE DOCUMENT TYPE Note");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Note").save();
    });
    assertVertexCount();

    // created and deleted in the current transaction
    database.transaction(() -> {
      database.newVertex("Student").save();
      database.newVertex("City").save();
      final List<Record> doomed = new ArrayList<>();
      for (final Iterator<Record> it = database.iterateType("Person", true); it.hasNext() && doomed.size() < 3; )
        doomed.add(it.next());
      for (final Record vertex : doomed)
        vertex.asVertex().delete();
      assertVertexCount();
    });
    assertVertexCount();
    // with WHERE, a property map or a dynamic label it is a filter, not a count of the types
    assertThat(plan("MATCH (n) WHERE n.age > 3 RETURN count(n) AS n")).doesNotContain("TYPE COUNT");
    assertThat(plan("MATCH (n {age: 3}) RETURN count(n) AS n")).doesNotContain("TYPE COUNT");
    // a dynamic label names a type only when the query runs
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:$($label)) RETURN count(n) AS n", "label", "Student")) {
      assertThat(((Number) rs.next().getProperty("n")).longValue()).isEqualTo(database.countType("Student", true));
    }
    assertThat(count("MATCH (n) WHERE n.age > 3 RETURN count(n) AS n")).isEqualTo(count("MATCH (n) WHERE n.age > 3 RETURN sum(1) AS n"));
  }

  private void assertAllEdgeCounts() {
    assertEdgeCount("MATCH ()-[e:KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
    assertEdgeCount("MATCH ()-[e:KNOWS]-() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.BOTH);
    assertEdgeCount("MATCH ()-[e:LIKES]->() RETURN count(e) AS n", "LIKES", Vertex.DIRECTION.OUT);
    assertEdgeCount("MATCH ()-[e]->() RETURN count(e) AS n", null, Vertex.DIRECTION.OUT);
    assertEdgeCount("MATCH ()-[e]-() RETURN count(e) AS n", null, Vertex.DIRECTION.BOTH);
    // a sub-type alone, and listed with its super-type, which already counts it
    assertEdgeCount("MATCH ()-[e:KNOWS_WELL]-() RETURN count(e) AS n", "KNOWS_WELL", Vertex.DIRECTION.BOTH);
    assertEdgeCount("MATCH ()-[e:KNOWS|KNOWS_WELL]-() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.BOTH);
    assertEdgeCount("MATCH ()-[e:KNOWS_WELL|KNOWS]->() RETURN count(e) AS n", "KNOWS", Vertex.DIRECTION.OUT);
  }

  /**
   * People with random KNOWS (some of them KNOWS_WELL, some light, some self loops, some parallel), LIVES_IN to cities and
   * light LIKES.
   */
  private void buildGraph(final long seed, final int people) {
    final Random random = new Random(seed);
    database.transaction(() -> {
      final List<MutableVertex> persons = new ArrayList<>();
      final List<MutableVertex> cities = new ArrayList<>();
      for (int i = 0; i < people; i++)
        persons.add(database.newVertex(i % 4 == 0 ? "Student" : "Person").set("age", i % 7).save());
      for (int i = 0; i < people / 20 + 1; i++)
        cities.add(database.newVertex("City").save());

      for (final MutableVertex person : persons) {
        final int knows = random.nextInt(6);
        for (int k = 0; k < knows; k++) {
          final MutableVertex other = random.nextInt(15) == 0 ? person : persons.get(random.nextInt(persons.size()));
          switch (random.nextInt(5)) {
          case 0 -> person.newEdge("KNOWS_WELL", other);
          case 1 -> person.newLightEdge("KNOWS", other);
          default -> person.newEdge("KNOWS", other, "since", random.nextInt(5));
          }
        }
        if (random.nextBoolean())
          person.newEdge("LIVES_IN", cities.get(random.nextInt(cities.size())));
        for (int k = random.nextInt(4); k > 0; k--)
          person.newEdge("LIKES", persons.get(random.nextInt(persons.size())));
      }
    });
  }

  /** The query is answered by the edge count, and agrees with the row pipeline and with the edge lists. */
  private void assertEdgeCount(final String query, final String type, final Vertex.DIRECTION direction) {
    assertThat(plan(query)).as("plan of %s", query).contains(EDGE_COUNT);
    final long expected = byHand(type, direction);
    assertThat(count(query)).as(query).isEqualTo(expected);
    assertThat(count(pipeline(query))).as(pipeline(query)).isEqualTo(expected);
  }

  private long pushedDown(final String query) {
    assertThat(plan(query)).as("plan of %s", query).contains(EDGE_COUNT);
    final long value = count(query);
    assertThat(count(pipeline(query))).as(pipeline(query)).isEqualTo(value);
    return value;
  }

  /** The query is not answered by the edge count, and still agrees with the row pipeline (and with {@code expected}). */
  private void assertPipelineOnly(final String query, final Long expected) {
    assertThat(plan(query)).as("plan of %s", query).doesNotContain(EDGE_COUNT);
    final long value = count(query);
    assertThat(value).as(query).isEqualTo(count(pipeline(query)));
    if (expected != null)
      assertThat(value).as(query).isEqualTo(expected);
  }

  private void assertVertexCount() {
    final String query = "MATCH (n) RETURN count(n) AS n";
    assertThat(plan(query)).contains("TYPE COUNT OPTIMIZATION (all vertex types)");
    long expected = 0;
    for (final Iterator<Record> it = Labels.iterateMatchingVertices(database, null, false); it.hasNext(); it.next())
      ++expected;
    assertThat(count(query)).isEqualTo(expected);
    assertThat(count("MATCH (n) RETURN count(*) AS n")).isEqualTo(expected);
    assertThat(count("MATCH (n) RETURN sum(1) AS n")).isEqualTo(expected);
  }

  /** The relationships the hop matches, off the edge lists: undirected, each edge from both ends and a self loop once. */
  private long byHand(final String type, final Vertex.DIRECTION direction) {
    final String[] types = type == null ? new String[0] : new String[] { type };
    long total = 0;
    for (final Iterator<Record> it = Labels.iterateMatchingVertices(database, null, false); it.hasNext(); ) {
      final Vertex vertex = it.next().asVertex();
      for (final Vertex neighbor : vertex.getVertices(Vertex.DIRECTION.OUT, types)) {
        ++total;
        if (direction == Vertex.DIRECTION.BOTH && !neighbor.getIdentity().equals(vertex.getIdentity()))
          ++total;
      }
    }
    return total;
  }

  /**
   * The same count through the row pipeline: a sum is no count, so no push-down takes it, whatever the MATCH looks like. A
   * {@code count(x)} counts the rows where {@code x} is not null, {@code count(DISTINCT x)} the distinct values.
   */
  private static String pipeline(final String query) {
    final int returnAt = query.lastIndexOf(" RETURN count(");
    final String match = query.substring(0, returnAt);
    final String counted = query.substring(returnAt + " RETURN count(".length(), query.lastIndexOf(") AS n"));
    if (counted.equals("*"))
      return match + " RETURN sum(1) AS n";
    if (counted.startsWith("DISTINCT "))
      return match + " WITH DISTINCT " + counted.substring("DISTINCT ".length()) + " AS x RETURN sum(CASE WHEN x IS NULL THEN 0 ELSE 1 END) AS n";
    return match + " RETURN sum(CASE WHEN " + counted + " IS NULL THEN 0 ELSE 1 END) AS n";
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().map(plan -> plan.prettyPrint(0, 2)).orElse("");
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
