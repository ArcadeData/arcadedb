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
package com.arcadedb.query.opencypher.procedures.path;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reachable-component procedures ({@code path.subgraphAll}, {@code path.subgraphNodes}) over a graph whose adjacency
 * entries greatly outnumber its vertices, which is where issue #7976 was reported: the walk used to resolve each
 * adjacency entry into an edge record AND a vertex record BEFORE testing whether the neighbour had already been
 * visited, so the records it read scaled with the edges of the component rather than with its vertices.
 * <p>
 * The bound is asserted on {@code readRecord} - the database's own count of record lookups - rather than on elapsed
 * time, so the assertion states the complexity claim itself and cannot be turned green by a faster machine or red by
 * a GC pause.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PathSubgraphComponentTest {
  private static final int VERTICES = 2_000;
  private static final int EDGES    = 20_000;

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-path-subgraph-component");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Entity");
    database.getSchema().createVertexType("Chunk");
    database.getSchema().createEdgeType("RELATES");
    database.getSchema().createEdgeType("MENTIONS");
    database.getSchema().createEdgeType("IGNORED");

    database.transaction(() -> {
      final MutableVertex[] entities = new MutableVertex[VERTICES];
      for (int i = 0; i < VERTICES; i++)
        entities[i] = database.newVertex("Entity").set("id", i).save();

      // ONE CONNECTED COMPONENT: A BACKBONE PLUS RANDOM CHORDS, SO EVERY VERTEX IS REACHED MANY TIMES OVER
      for (int i = 1; i < VERTICES; i++)
        entities[i - 1].newEdge("RELATES", entities[i], true, (Object[]) null).save();

      final Random random = new Random(7);
      for (int i = VERTICES - 1; i < EDGES; i++)
        entities[random.nextInt(VERTICES)].newEdge("MENTIONS", entities[random.nextInt(VERTICES)], true, (Object[]) null).save();

      // A CHUNK HANGING OFF THE FIRST ENTITY THROUGH AN EDGE TYPE THE FILTER LEAVES OUT
      final MutableVertex chunk = database.newVertex("Chunk").set("id", -1).save();
      entities[0].newEdge("IGNORED", chunk, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  private long readRecords() {
    return ((Number) ((LocalDatabase) database).getStats().get("readRecord")).longValue();
  }

  @Test
  void subgraphAllReachesTheWholeComponent() {
    final ResultSet rs = database.query("cypher", """
        MATCH (n:Entity {id: 0})
        CALL path.subgraphall(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD nodes, relationships
        RETURN nodes, relationships
        """);

    final Result result = rs.next();
    assertThat(((List<?>) result.getProperty("nodes")).size()).isEqualTo(VERTICES);
    assertThat(((List<?>) result.getProperty("relationships")).size()).isEqualTo(EDGES);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void subgraphAllStartsFromTheStartNode() {
    final Result result = database.query("cypher", """
        MATCH (n:Entity {id: 0})
        CALL path.subgraphall(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD nodes
        RETURN nodes
        """).next();

    final List<?> nodes = result.getProperty("nodes");
    assertThat(((com.arcadedb.graph.Vertex) nodes.get(0)).getInteger("id")).isEqualTo(0);
  }

  @Test
  void relationshipFilterKeepsOtherEdgeTypesOut() {
    final Result result = database.query("cypher", """
        MATCH (n:Entity {id: 0})
        CALL path.subgraphall(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD nodes
        RETURN [node IN nodes WHERE node.id < 0 | node.id] AS chunks
        """).next();

    assertThat(((List<?>) result.getProperty("chunks"))).isEmpty();
  }

  @Test
  void labelFilterKeepsOtherLabelsOut() {
    final Result result = database.query("cypher", """
        MATCH (n:Entity {id: 0})
        CALL path.subgraphall(n, {labelFilter: 'Entity'}) YIELD nodes
        RETURN size(nodes) AS total
        """).next();

    // THE CHUNK IS REACHABLE (NO relationshipFilter HERE) BUT ITS LABEL IS NOT ACCEPTED
    assertThat(((Number) result.getProperty("total")).intValue()).isEqualTo(VERTICES);
  }

  @Test
  void maxLevelBoundsTheWalk() {
    final Result result = database.query("cypher", """
        MATCH (n:Entity {id: 0})
        CALL path.subgraphall(n, {relationshipFilter: 'RELATES', maxLevel: 3}) YIELD nodes
        RETURN size(nodes) AS total
        """).next();

    // THE BACKBONE IS A CHAIN AND THE WALK IS UNDIRECTED, SO 3 HOPS FROM ITS FIRST LINK REACH 4 VERTICES
    assertThat(((Number) result.getProperty("total")).intValue()).isEqualTo(4);
  }

  @Test
  void subgraphNodesReachesTheWholeComponent() {
    final ResultSet rs = database.query("cypher", """
        MATCH (n:Entity {id: 0})
        CALL path.subgraphnodes(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD node
        RETURN count(node) AS total
        """);

    assertThat(((Number) rs.next().getProperty("total")).intValue()).isEqualTo(VERTICES);
  }

  /**
   * Issue #7976: a walk that reads a record per adjacency entry reads {@code EDGES} records or more; a walk that
   * deduplicates on the RID first reads a bounded handful per vertex of the component - the vertex itself plus the
   * head of each of its two edge lists. The bound below sits between those two, far enough above the second to
   * survive an extra chunk hop on a denser vertex and far enough below the first that the old walk cannot pass it.
   */
  @Test
  void yieldingOnlyNodesReadsRecordsPerVertexNotPerEdge() {
    // WARM UP: THE FIRST RUN ALSO PAYS FOR SCHEMA AND BUCKET SET-UP
    database.query("cypher",
        "MATCH (n:Entity {id: 0}) CALL path.subgraphall(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD nodes RETURN size(nodes) AS t")
        .next();

    final long before = readRecords();
    final Result result = database.query("cypher",
        "MATCH (n:Entity {id: 0}) CALL path.subgraphall(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD nodes RETURN size(nodes) AS t")
        .next();
    final long reads = readRecords() - before;

    assertThat(((Number) result.getProperty("t")).intValue()).isEqualTo(VERTICES);
    assertThat(reads).isLessThan(8L * VERTICES);
    assertThat(reads).isLessThan(EDGES);
  }

  /**
   * Issue #7976: {@code path.subgraphNodes} never yields relationships at all, so it must never read an edge record.
   */
  @Test
  void subgraphNodesReadsRecordsPerVertexNotPerEdge() {
    database.query("cypher",
        "MATCH (n:Entity {id: 0}) CALL path.subgraphnodes(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD node RETURN count(node) AS t")
        .next();

    final long before = readRecords();
    database.query("cypher",
        "MATCH (n:Entity {id: 0}) CALL path.subgraphnodes(n, {relationshipFilter: 'MENTIONS|RELATES'}) YIELD node RETURN count(node) AS t")
        .next();
    final long reads = readRecords() - before;

    assertThat(reads).isLessThan(8L * VERTICES);
    assertThat(reads).isLessThan(EDGES);
  }
}
