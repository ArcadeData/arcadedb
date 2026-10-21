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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10308: a walk over the edge list of a vertex that is still going on while the same transaction changes that list. Before
 * the Gremlin neighbor expansion went lazy, nothing walked a list while writing to it: the whole list was read first. A walk now
 * has to hand out the entries its segment held when it reached it, whatever the transaction does to the segment afterwards.
 * <p>
 * The segment a transaction has written is the transaction's own cached instance, which a lookup hands to a walk as it is: an
 * edge removed from it moved the entries behind the removed one back under the walk's position, so every second entry was never
 * reached (and one added at the head moved the entries forward, so some were reached twice).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10308EdgeListWalkWhileWritingTest {
  private static final String DB_PATH = "./target/databases/issue10308-walk-while-writing";
  private static final int    EDGES   = 3_000;

  private Database database;
  private RID      root;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen()) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  /** The root and its edges, created by the CURRENT transaction when it is called inside one. */
  private void createRootWithEdges() {
    final MutableVertex vertex = database.newVertex("V").set("name", "root").save();
    root = vertex.getIdentity();
    for (int i = 0; i < EDGES; i++)
      vertex.newEdge("E", database.newVertex("V").set("n", i).save());
  }

  private long edgesOfRoot() {
    long total = 0;
    for (final Edge ignored : root.asVertex().getEdges(Vertex.DIRECTION.OUT, "E"))
      ++total;
    return total;
  }

  @Test
  void deletingTheNeighborsOfAListTheTransactionCreatedWhileWalkingReachesEveryOne() {
    database.begin();
    createRootWithEdges();

    final long visited = deleteNeighborsWhileWalking();

    assertThat(visited).isEqualTo(EDGES);
    assertThat(edgesOfRoot()).isZero();
    database.commit();
    assertThat(edgesOfRoot()).isZero();
  }

  @Test
  void deletingTheEdgesOfAListTheTransactionCreatedWhileWalkingReachesEveryOne() {
    database.begin();
    createRootWithEdges();

    final long visited = deleteEdgesWhileWalking();

    assertThat(visited).isEqualTo(EDGES);
    assertThat(edgesOfRoot()).isZero();
    database.commit();
    assertThat(edgesOfRoot()).isZero();
  }

  @Test
  void deletingTheNeighborsOfACommittedListAfterTouchingItWhileWalkingReachesEveryOne() {
    database.transaction(this::createRootWithEdges);

    database.begin();
    // THE LIST IS THE TRANSACTION'S OWN FROM HERE: ONE MORE EDGE WRITES ITS HEAD SEGMENT, AND A DELETE EVERY OTHER SEGMENT IT HITS
    root.asVertex().modify().newEdge("E", database.newVertex("V").set("n", -1).save());

    final long visited = deleteNeighborsWhileWalking();

    assertThat(visited).isEqualTo(EDGES + 1);
    assertThat(edgesOfRoot()).isZero();
    database.commit();
    assertThat(edgesOfRoot()).isZero();
  }

  @Test
  void deletingTheNeighborsOfACommittedListWhileWalkingReachesEveryOne() {
    database.transaction(this::createRootWithEdges);

    database.begin();
    final long visited = deleteNeighborsWhileWalking();

    assertThat(visited).isEqualTo(EDGES);
    assertThat(edgesOfRoot()).isZero();
    database.commit();
    assertThat(edgesOfRoot()).isZero();
  }

  @Test
  void addingEdgesWhileWalkingAListTheTransactionCreatedReachesEveryOneOnce() {
    database.begin();
    createRootWithEdges();

    final MutableVertex vertex = root.asVertex().modify();
    final List<RID> seen = new ArrayList<>();
    final Iterator<Vertex> neighbors = vertex.getVertices(Vertex.DIRECTION.OUT, "E").iterator();
    int guard = 0;
    while (neighbors.hasNext() && ++guard < 10 * EDGES) {
      seen.add(neighbors.next().getIdentity());
      // A NEW ENTRY GOES IN AT THE BEGINNING OF THE HEAD SEGMENT, MOVING EVERY ENTRY OF IT FORWARD
      vertex.newEdge("E", database.newVertex("V").set("n", -1).save());
    }

    assertThat(seen).hasSize(EDGES);
    assertThat(seen.stream().distinct().count()).isEqualTo(EDGES);
    database.commit();
  }

  private long deleteNeighborsWhileWalking() {
    long visited = 0;
    final Iterator<Vertex> neighbors = root.asVertex().getVertices(Vertex.DIRECTION.OUT, "E").iterator();
    // THE GUARD ONLY BOUNDS A WALK THAT NEVER ENDS
    while (neighbors.hasNext() && visited < 10L * EDGES) {
      final Vertex neighbor = neighbors.next();
      ++visited;
      if (database.existsRecord(neighbor.getIdentity()))
        neighbor.delete();
    }
    return visited;
  }

  private long deleteEdgesWhileWalking() {
    long visited = 0;
    final Iterator<Edge> edges = root.asVertex().getEdges(Vertex.DIRECTION.OUT, "E").iterator();
    while (edges.hasNext() && visited < 10L * EDGES) {
      final Edge edge = edges.next();
      ++visited;
      if (database.existsRecord(edge.getIdentity()))
        edge.delete();
    }
    return visited;
  }
}
