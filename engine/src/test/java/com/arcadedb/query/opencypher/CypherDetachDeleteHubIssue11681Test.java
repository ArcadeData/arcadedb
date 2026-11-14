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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.StripedEdgeList;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #11681: openCypher {@code DETACH DELETE} of a vertex deleted each relationship with
 * {@code edge.delete()}, which also removed it from the deleted vertex's own edge list. On a hub that list is a
 * {@link StripedEdgeList}, and every removal scanned a stripe's segments for the entry, so the statement grew faster than the
 * number of relationships (3.9 s against 0.34 s for SQL {@code DELETE} at 48,000). The lists of the vertex are dropped
 * wholesale a moment later by {@code GraphEngine.deleteVertex}, which since #5760 disconnects each edge from its far endpoint
 * only.
 * <p>
 * The hub here carries every shape DETACH DELETE has to count: regular relationships in both directions (enough of them to
 * promote its incoming list to the striped layout), lightweight ones, a self-loop, parallel lightweight twins (#10008) and
 * relationships of a unidirectional type that only their source stores (#8676).
 */
class CypherDetachDeleteHubIssue11681Test extends TestHelper {
  private static final int CHUNKS     = 600;
  private static final int LINKED     = 300;
  private static final int TWINS      = 3;
  private static final int UNI        = 5;
  // EVERY RELATIONSHIP OF THE HUB: OF_RECORD IN, LINK OUT, ITS EXTRA TWINS, THE SELF-LOOP, UNI IN
  private static final int HUB_DEGREE = CHUNKS + LINKED + TWINS + 1 + UNI;

  private int savedThreshold;
  private RID hub;

  @BeforeEach
  void promoteEarly() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(256);
  }

  @AfterEach
  void restoreThreshold() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
  }

  @Test
  void detachDeleteOfAHubLeavesNoReferenceAndCountsEachRelationshipOnce() {
    buildHub();

    final QueryStatistics stats = detachDeleteHub();
    assertThat(stats.getNodesDeleted()).isEqualTo(1);
    assertThat(stats.getRelationshipsDeleted()).isEqualTo(HUB_DEGREE);

    assertThat(count("MATCH (c:Chunk)-[:OF_RECORD]->() RETURN count(*) AS n")).isZero();
    assertThat(count("MATCH (c:Chunk)<-[:LINK]-() RETURN count(*) AS n")).isZero();
    assertThat(count("MATCH (c:Chunk)-[:UNI]->() RETURN count(*) AS n")).isZero();
    assertThat(count("MATCH (r:Record) RETURN count(*) AS n")).isZero();
    database.transaction(() -> {
      assertThat(database.countType("OF_RECORD", false)).isZero();
      assertThat(database.countType("UNI", false)).isZero();
      assertThat(database.existsRecord(hub)).isFalse();
      final Iterator<Vertex> chunks = database.iterateType("Chunk", false);
      while (chunks.hasNext()) {
        final Vertex chunk = chunks.next();
        assertThat(chunk.countEdges(Vertex.DIRECTION.BOTH)).as("edges left on " + chunk.getIdentity()).isZero();
      }
    });
    assertIntegrityClean();
  }

  /**
   * The cost check, without a clock: every relationship is reported to the delete listeners once, and when each of them is,
   * the hub still lists everything it listed before the statement. Removing each relationship from the hub's own list first
   * (the superlinear work of the issue) shrinks the hub's degree from one report to the next.
   */
  @Test
  void theHubsOwnListsAreNotEditedPerRelationship() {
    buildHub();
    final long[] degreeBefore = new long[1];
    database.transaction(() -> degreeBefore[0] = hubDegree());

    final List<Long> degreeSeenAtEachEdgeDelete = new ArrayList<>();
    final BeforeRecordDeleteListener probe = record -> {
      if (record instanceof Edge)
        degreeSeenAtEachEdgeDelete.add(hubDegree());
      return true;
    };
    database.getEvents().registerListener(probe);
    final QueryStatistics stats;
    try {
      stats = detachDeleteHub();
    } finally {
      database.getEvents().unregisterListener(probe);
    }

    assertThat(stats.getRelationshipsDeleted()).isEqualTo(HUB_DEGREE);
    assertThat(degreeSeenAtEachEdgeDelete).as("each relationship is deleted, and reported, exactly once").hasSize(HUB_DEGREE);
    assertThat(degreeSeenAtEachEdgeDelete).as("the hub's own lists are dropped whole, not edited per relationship")
        .containsOnly(degreeBefore[0]);
    assertIntegrityClean();
  }

  /**
   * One statement reaching the same relationships from both endpoints, or as a vertex's relationship and as a bound one: each
   * is counted once, the lightweight twins as many times as there are twins.
   */
  @Test
  void aRelationshipReachedTwiceInOneStatementIsCountedOnce() {
    createSchema();
    createPair();
    QueryStatistics stats = stats("MATCH (a:Record {id: 'a'})-[r]-(b:Chunk {n: -1}) DETACH DELETE a, b");
    assertThat(stats.getNodesDeleted()).isEqualTo(2);
    assertThat(stats.getRelationshipsDeleted()).isEqualTo(4);

    createPair();
    stats = stats("MATCH (a:Record {id: 'a'})-[r]-(b:Chunk {n: -1}) DETACH DELETE a, r");
    assertThat(stats.getNodesDeleted()).isEqualTo(1);
    assertThat(stats.getRelationshipsDeleted()).isEqualTo(4);

    createPair();
    stats = stats("MATCH (a:Record {id: 'a'})-[r]-(b:Chunk {n: -1}) DETACH DELETE r, b, a");
    assertThat(stats.getNodesDeleted()).isEqualTo(2);
    assertThat(stats.getRelationshipsDeleted()).isEqualTo(4);

    assertThat(count("MATCH ()-[r]->() RETURN count(r) AS n")).isZero();
    assertIntegrityClean();
  }

  private void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE Record");
    database.command("sql", "CREATE VERTEX TYPE Chunk");
    database.command("sql", "CREATE EDGE TYPE OF_RECORD");
    database.command("sql", "CREATE EDGE TYPE LINK LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE UNI UNIDIRECTIONAL");
  }

  /** a -[:OF_RECORD]-> b, and two lightweight twins a -[:LINK]-> b, plus a unidirectional b -[:UNI]-> a. */
  private void createPair() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (a:Record {id: 'a'}), (b:Chunk {n: -1})
        CREATE (a)-[:OF_RECORD]->(b), (a)-[:LINK]->(b), (a)-[:LINK]->(b), (b)-[:UNI]->(a)"""));
  }

  private void buildHub() {
    createSchema();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Record {id: 'r0'})");
      database.command("opencypher",
          "MATCH (r:Record {id: 'r0'}) UNWIND range(0, $last) AS i CREATE (:Chunk {n: i})-[:OF_RECORD]->(r)",
          Map.of("last", CHUNKS - 1));
      database.command("opencypher", "MATCH (r:Record {id: 'r0'}), (c:Chunk) WHERE c.n < $linked CREATE (r)-[:LINK]->(c)",
          Map.of("linked", LINKED));
      database.command("opencypher",
          "MATCH (r:Record {id: 'r0'}), (c:Chunk {n: 0}) UNWIND range(1, $twins) AS k CREATE (r)-[:LINK]->(c)",
          Map.of("twins", TWINS));
      database.command("opencypher", "MATCH (r:Record {id: 'r0'}) CREATE (r)-[:OF_RECORD]->(r)");
      database.command("opencypher", "MATCH (r:Record {id: 'r0'}), (c:Chunk) WHERE c.n < $uni CREATE (c)-[:UNI]->(r)",
          Map.of("uni", UNI));
    });
    database.transaction(() -> {
      try (final ResultSet rs = database.query("opencypher", "MATCH (r:Record {id: 'r0'}) RETURN r")) {
        hub = rs.next().<Vertex>getProperty("r").getIdentity();
      }
      // THE PRECONDITION: WITHOUT PROMOTION THE HUB'S LIST IS A PLAIN CHAIN, NOT THE STRIPED ONE OF THE ISSUE
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      assertThat(engine.getEdgeHeadChunk((VertexInternal) hub.asVertex(), Vertex.DIRECTION.IN)).isInstanceOf(
          StripedEdgeList.class);
      assertThat(count("MATCH (r:Record {id: 'r0'})-[:LINK]->(c:Chunk {n: 0}) RETURN count(*) AS n")).isEqualTo(TWINS + 1);
    });
  }

  private QueryStatistics detachDeleteHub() {
    return stats("MATCH (r:Record {id: 'r0'}) DETACH DELETE r");
  }

  private QueryStatistics stats(final String cypher) {
    final QueryStatistics[] stats = new QueryStatistics[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rs.next();
        stats[0] = rs.getStatistics().orElseThrow();
      }
    });
    return stats[0];
  }

  private long hubDegree() {
    return database.lookupByRID(hub, true).asVertex().countEdges(Vertex.DIRECTION.BOTH);
  }

  private long count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private void assertIntegrityClean() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "totalErrors")).as("check database: " + row.toJSON()).isZero();
        assertThat(longProperty(row, "totalWarnings")).as("check database: " + row.toJSON()).isZero();
      }
    }
  }

  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }
}
