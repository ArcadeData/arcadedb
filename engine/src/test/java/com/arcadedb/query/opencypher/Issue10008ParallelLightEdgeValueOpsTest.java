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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.StripeDirectory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #10008: #9573 and #9983 taught the pattern matcher that two lightweight edges of one type over
 * the same ordered pair ("twins") are two relationships, but the value-level operations still compared a relationship
 * by its RID, which twins share: {@code =} / {@code <>}, {@code DISTINCT} and grouping keys, path equality and the dedup
 * of {@code DELETE}, which left twins the statement asked to delete in the graph.
 * <p>
 * Every query runs over the same fixture built twice, once with lightweight and once with record-backed edges: the
 * record-backed edges were never affected, so the two must agree.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10008ParallelLightEdgeValueOpsTest extends TestHelper {

  private enum Fixture {
    /** two parallel K edges a->b */
    TWO_PARALLEL,
    /** three parallel K edges a->b */
    THREE_PARALLEL,
    /** two parallel K self loops on a */
    TWO_LOOPS
  }

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
  }

  static Stream<Arguments> queries() {
    final List<Arguments> cases = new ArrayList<>();
    for (final boolean lightweight : new boolean[] { true, false }) {
      // = and <>
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P), (x)-[r2:K]->(y) WHERE r1 <> r2 RETURN r2", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (x)-[r2:K]->(y) WHERE r1 <> r2 RETURN r2", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (x)-[r2:K]->(y) WHERE r1 = r2 RETURN r2", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (x)<-[r2:K]-(y) WHERE r1 = r2 RETURN r2", 0);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 = r2 RETURN r2", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 <> r2 RETURN r2", 2);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 = r2 RETURN r2", 3);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[r1:K]->(y:P) MATCH (y)<-[r2:K]-(x) WHERE r1 <> r2 RETURN r2", 6);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[r1:K]->(x) MATCH (x)<-[r2:K]-(x) WHERE r1 = r2 RETURN r2", 2);
      // DISTINCT, grouping and the aggregations that dedup
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]->(y:P) RETURN DISTINCT r", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN DISTINCT r", 2);
      add(cases, Fixture.THREE_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN DISTINCT r", 3);
      add(cases, Fixture.TWO_LOOPS, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN DISTINCT r", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K*2]-(z:P) RETURN DISTINCT r", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH p = (x:P)-[:K*2]-(z:P) RETURN DISTINCT p", 4);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH p = (x:P)-[:K]->(z:P) RETURN DISTINCT p", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) WITH DISTINCT r RETURN r", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) RETURN r, count(*) AS n", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]->(y:P) RETURN r UNION MATCH (x:P)<-[r:K]-(y:P) RETURN r", 2);
      add(cases, Fixture.TWO_PARALLEL, lightweight, "MATCH (x:P)-[r:K]-(y:P) WITH collect(DISTINCT r) AS rs UNWIND rs AS r RETURN r",
          2);
    }
    return cases.stream();
  }

  private static void add(final List<Arguments> cases, final Fixture fixture, final boolean lightweight, final String query,
      final int expected) {
    cases.add(Arguments.of(fixture, lightweight, query, expected));
  }

  @ParameterizedTest(name = "{0} lightweight={1}: {2} -> {3}")
  @MethodSource("queries")
  void valueOperationsCountTwinsAsDistinctRelationships(final Fixture fixture, final boolean lightweight, final String query,
      final int expected) {
    build(fixture, lightweight);
    assertThat(rows(query)).as(query).isEqualTo(expected);
  }

  @ParameterizedTest(name = "lightweight={0}")
  @MethodSource("lightweightOrNot")
  void countDistinctCountsTwins(final boolean lightweight) {
    build(Fixture.THREE_PARALLEL, lightweight);
    assertThat(single("MATCH (x:P)-[r:K]-(y:P) RETURN count(DISTINCT r) AS n")).isEqualTo(3L);
    assertThat(single("MATCH (x:P)-[r:K]-(y:P) RETURN size(collect(DISTINCT r)) AS n")).isEqualTo(3L);
    assertThat(single("MATCH (x:P)-[r:K]->(y:P) WITH collect(r) AS rs RETURN size(rs) AS n")).isEqualTo(3L);
  }

  static Stream<Arguments> deletes() {
    final List<Arguments> cases = new ArrayList<>();
    for (final boolean lightweight : new boolean[] { true, false })
      for (final Fixture fixture : Fixture.values()) {
        final long edges = fixture == Fixture.THREE_PARALLEL ? 3 : 2;
        cases.add(Arguments.of(fixture, lightweight, "MATCH (:P)-[r:K]-(:P) DELETE r", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH (:P)-[r:K]->(:P) DELETE r", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH (:P)<-[r:K]-(:P) DELETE r", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH ()-[r:K]-() WITH r DELETE r", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH ()-[r:K]-() WITH collect(r) AS rs UNWIND rs AS r DELETE r", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH p = (:P)-[:K]-(:P) DELETE p", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH (n:P) DETACH DELETE n", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH (n:P)-[r:K]-() DELETE r, n", edges));
        cases.add(Arguments.of(fixture, lightweight, "MATCH (n:P) FOREACH (r IN [(n)-[k:K]-() | k] | DELETE r)", edges));
      }
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} lightweight={1}: {2}")
  @MethodSource("deletes")
  void deleteRemovesEveryTwinItWasAskedTo(final Fixture fixture, final boolean lightweight, final String statement,
      final long edges) {
    build(fixture, lightweight);
    final long[] reported = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement)) {
        while (rs.hasNext())
          rs.next();
        reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
      }
    });
    assertThat(edgesLeft()).as(statement).isZero();
    assertThat(reported[0]).as(statement).isEqualTo(edges);
  }

  @ParameterizedTest(name = "lightweight={0}")
  @MethodSource("lightweightOrNot")
  void deleteReportsOneRelationshipPerTwin(final boolean lightweight) {
    build(Fixture.THREE_PARALLEL, lightweight);
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (:P)-[r:K]-(:P) DELETE r RETURN count(*) AS n")) {
        assertThat(((Number) rs.next().getProperty("n")).longValue()).isEqualTo(6L);
      }
    });
    assertThat(edgesLeft()).isZero();
  }

  /** Deleting one of the twins leaves the other: a relationship bound by one row is deleted once, not once per twin. */
  @ParameterizedTest(name = "lightweight={0}")
  @MethodSource("lightweightOrNot")
  void deletingOneTwinLeavesTheOthers(final boolean lightweight) {
    build(Fixture.THREE_PARALLEL, lightweight);
    database.transaction(() -> database.command("opencypher", "MATCH (:P)-[r:K]->(:P) WITH r LIMIT 1 DELETE r").close());
    assertThat(edgesLeft()).isEqualTo(2L);
    database.transaction(() -> database.command("opencypher", "MATCH (:P)-[r:K]->(:P) WITH r LIMIT 1 DELETE r, r").close());
    assertThat(edgesLeft()).isEqualTo(1L);
  }

  /**
   * A query that creates a twin of the edges it is reading: the new one is no row of the MATCH, and the edges that are
   * keep their identities, so DISTINCT counts the relationships the MATCH read, twins apart.
   */
  @Test
  void creatingATwinWhileReadingKeepsTheRowsApart() {
    database.command("sql", "CREATE EDGE TYPE L LIGHTWEIGHT");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      for (int i = 0; i < 3; i++)
        a.newEdge("L", b);
    });
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (x:P)-[r:L]-(y:P) CREATE (x)-[:L]->(y) WITH DISTINCT r RETURN count(r) AS n")) {
        assertThat(((Number) rs.next().getProperty("n")).longValue()).isEqualTo(3L);
      }
    });
    // 3 edges read from both ends, one created per row
    assertThat(single("MATCH ()-[r:L]->() RETURN count(r) AS n")).isEqualTo(9L);
    assertThat(single("MATCH (:P)-[r:L]-(:P) RETURN count(DISTINCT r) AS n")).isEqualTo(9L);
  }

  /**
   * An explicit delete of a twin and a DETACH of its endpoint in one statement: the DETACH reads the list after the twin
   * is gone, so its survivors are numbered anew and must not be mistaken for the twins already deleted.
   */
  @ParameterizedTest(name = "{0} lightweight={1}: {2}")
  @MethodSource("explicitAndDetach")
  void anExplicitDeleteAndADetachOfItsEndpointCountEveryTwinOnce(final Fixture fixture, final boolean lightweight,
      final String statement, final long nodes) {
    build(fixture, lightweight);
    final long[] reported = new long[2];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement)) {
        while (rs.hasNext())
          rs.next();
        reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
        reported[1] = rs.getStatistics().orElseThrow().getNodesDeleted();
      }
    });
    assertThat(edgesLeft()).as(statement).isZero();
    assertThat(reported[0]).as(statement).isEqualTo(fixture == Fixture.THREE_PARALLEL ? 3L : 2L);
    assertThat(reported[1]).as(statement).isEqualTo(nodes);
  }

  static Stream<Arguments> explicitAndDetach() {
    final List<Arguments> cases = new ArrayList<>();
    for (final boolean lightweight : new boolean[] { true, false })
      for (final Fixture fixture : Fixture.values()) {
        cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]->(:P) DETACH DELETE r, a", 1L));
        cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]->(:P) DETACH DELETE a, r", 1L));
        if (fixture != Fixture.TWO_LOOPS)
          cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]->(b:P) DETACH DELETE r, b", 1L));
        cases.add(Arguments.of(fixture, lightweight,
            "MATCH (a:P {name: 'a'})-[r:K]->(:P) WITH a, collect(r) AS rs FOREACH (x IN rs | DETACH DELETE x, a)", 1L));
        // both endpoints of the twins detached by one row: the second DETACH finds the twins gone with the first
        cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'}), (b:P {name: 'b'}) DETACH DELETE a, b", 2L));
        cases.add(Arguments.of(fixture, lightweight, "MATCH (a:P {name: 'a'})-[r:K]-(b:P) DETACH DELETE r, a, b",
            fixture == Fixture.TWO_LOOPS ? 1L : 2L));
      }
    return cases.stream();
  }

  /**
   * Twins met only by the last rows of a streaming DELETE: the rows before them are deleted as they stream, the twins
   * read the rest in and are numbered before the first of them goes.
   */
  @ParameterizedTest(name = "lightweight={0}")
  @MethodSource("lightweightOrNot")
  void twinsMetByTheLastRowsOfAStreamingDelete(final boolean lightweight) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      // a list is read newest first: the twins, created first, come last
      for (int i = 0; i < 3; i++)
        if (lightweight)
          a.newLightEdge("K", b);
        else
          a.newEdge("K", b);
      for (int i = 0; i < 50; i++)
        if (lightweight)
          a.newLightEdge("K", database.newVertex("P").save());
        else
          a.newEdge("K", database.newVertex("P").save());
    });
    final long[] reported = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (:P {name: 'a'})-[r:K]->(:P) DELETE r")) {
        while (rs.hasNext())
          rs.next();
        reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
      }
    });
    assertThat(edgesLeft()).isZero();
    assertThat(reported[0]).isEqualTo(53L);
  }

  /**
   * The first twin reads the remaining rows in before it is deleted, under the per-operation heap limit. A limit too
   * small for them fails the statement, which rolls back whole: nothing is deleted, rather than the rows that fit.
   */
  @Test
  void aDeleteOfTwinsOverTheHeapLimitFailsWithoutDeletingAnything() {
    build(Fixture.THREE_PARALLEL, true);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 2);
    assertThatThrownBy(() -> database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (:P)-[r:K]-(:P) DELETE r")) {
        while (rs.hasNext())
          rs.next();
      }
    })).hasStackTraceContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP,
        GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getDefValue());
    assertThat(edgesLeft()).isEqualTo(3L);
  }

  /**
   * A super-node: its edge list is promoted to stripes past the threshold, so the occurrences of its twins are counted
   * across generations and stripes, and the rows of one expansion all come off its one list (resolved once, by the
   * census of that list, rather than once per row).
   */
  @ParameterizedTest(name = "lightweight={0}")
  @MethodSource("lightweightOrNot")
  void twinsOnAStripedSuperNode(final boolean lightweight) {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 8);
    final int leaves = 40;
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("P").set("name", "hub").save();
      final List<MutableVertex> leafs = new ArrayList<>();
      for (int i = 0; i < leaves; i++)
        leafs.add(database.newVertex("P").set("name", "leaf" + i).save());
      // twins spread over the list's life: some before the promotion, some after
      for (int round = 0; round < 3; round++)
        for (int i = 0; i < leaves; i++)
          if (round == 0 || (round == 1 && i < 5) || (round == 2 && i < 2))
            if (lightweight)
              hub.newLightEdge("K", leafs.get(i));
            else
              hub.newEdge("K", leafs.get(i));
    });
    final long relationships = leaves + 5 + 2;
    try (final ResultSet rs = database.query("opencypher", "MATCH (h:P {name: 'hub'}) RETURN h")) {
      final VertexInternal hub = (VertexInternal) rs.next().<Vertex>getProperty("h");
      assertThat(database.lookupByRID(hub.getOutEdgesHeadChunk(), true)).isInstanceOf(StripeDirectory.class);
    }

    assertThat(single("MATCH (h:P {name: 'hub'})-[r:K]-(:P) RETURN count(DISTINCT r) AS n")).isEqualTo(relationships);
    assertThat(single("MATCH (:P)-[r:K]-(:P) RETURN count(DISTINCT r) AS n")).isEqualTo(relationships);
    assertThat(single("MATCH (h:P {name: 'hub'})-[r1:K]->(l:P) MATCH (l)<-[r2:K]-(h) WHERE r1 = r2 RETURN count(*) AS n"))
        .isEqualTo(relationships);
    assertThat(rows("MATCH (:P)-[r:K]-(:P) RETURN DISTINCT r")).isEqualTo((int) relationships);

    final long[] reported = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (:P)-[r:K]-(:P) DELETE r")) {
        while (rs.hasNext())
          rs.next();
        reported[0] = rs.getStatistics().orElseThrow().getRelationshipsDeleted();
      }
    });
    assertThat(edgesLeft()).isZero();
    assertThat(reported[0]).isEqualTo(relationships);
  }

  static Stream<Arguments> lightweightOrNot() {
    return Stream.of(Arguments.of(true), Arguments.of(false));
  }

  private long edgesLeft() {
    return single("MATCH ()-[r:K]->() RETURN count(r) AS n");
  }

  private void build(final Fixture fixture, final boolean lightweight) {
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("P").set("name", "a").save();
      final MutableVertex b = database.newVertex("P").set("name", "b").save();
      final int copies = fixture == Fixture.THREE_PARALLEL ? 3 : 2;
      final MutableVertex to = fixture == Fixture.TWO_LOOPS ? a : b;
      for (int i = 0; i < copies; i++)
        if (lightweight)
          a.newLightEdge("K", to);
        else
          a.newEdge("K", to);
    });
  }

  private int rows(final String query) {
    int found = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        rs.next();
        ++found;
      }
    }
    return found;
  }

  private long single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      final Result row = rs.next();
      return ((Number) row.getProperty("n")).longValue();
    }
  }
}
