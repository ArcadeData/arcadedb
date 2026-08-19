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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6278.
 * <p>
 * {@code EdgeVertexIterator.hasNext()} gained a guard against a chunk whose {@code previous} pointer names itself
 * in #6276 (issue #6062), because that iterator backs {@code EdgeLinkedList.entryIterator()}, which is what
 * {@code CHECK DATABASE}'s adjacency probe cache walks a vertex's own edge list through. The guard was added only
 * there - every other chunk-hopping iterator behind an ordinary graph traversal ({@link EdgeIterator},
 * {@link VertexIterator}, {@link RIDIterator}, and the {@code *IteratorFilter} family built on
 * {@link IteratorFilterBase}) still lacked it, so the same corruption that #6276 made {@code CHECK DATABASE}
 * survive still hung {@code Vertex.getEdges()}, {@code getVertices()} and {@code GraphEngine.getConnectedVertexRIDs()}
 * in an ordinary request thread.
 * <p>
 * The fix lifts the guard into {@code ResettableIteratorBase.moveToPreviousChunk()}, the one chunk-hop every one of
 * these iterators now goes through. Each test here plants a self-referencing head chunk - the same
 * {@code setPrevious(itself)} trick as {@code Issue6062AdjacencyProbeCacheTest
 * .aSelfReferencingChunkEndsTheWalkInsteadOfFeedingItForever} - and walks one iterator family under a HARD CAP
 * rather than a timeout: an unterminating walk does not fail a test, it hangs the build, and a regression has to
 * come back as a red test, not a red build.
 */
class Issue6278SelfReferencingChunkTraversalIteratorsTest extends TestHelper {
  private static final String VERTEX_TYPE = "Issue6278Node";
  private static final String EDGE_TYPE   = "Issue6278Link";
  private static final int    DEGREE      = 50;
  private static final int    CAP         = 8 * DEGREE;

  /**
   * Every test here deliberately plants a self-referencing chunk - that is the corruption under measurement - so
   * the blanket teardown check would only re-assert the same database with the opposite expectation.
   */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().buildEdgeType().withName(EDGE_TYPE).withBidirectional(true).create();
    });
  }

  @Test
  void edgeIteratorEndsTheWalkInsteadOfFeedingItForever() {
    final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

    final int[] walked = new int[1];
    database.transaction(() -> {
      final Iterator<Edge> edges = edgeLinkedListFor(hub).edgeIterator();
      while (walked[0] <= CAP && edges.hasNext()) {
        edges.next();
        ++walked[0];
      }
    });

    assertThat(walked[0])
        .as("EdgeIterator must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
        .isLessThanOrEqualTo(DEGREE);
  }

  @Test
  void vertexIteratorEndsTheWalkInsteadOfFeedingItForever() {
    final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

    final int[] walked = new int[1];
    database.transaction(() -> {
      final Iterator<Vertex> vertices = edgeLinkedListFor(hub).vertexIterator();
      while (walked[0] <= CAP && vertices.hasNext()) {
        vertices.next();
        ++walked[0];
      }
    });

    assertThat(walked[0])
        .as("VertexIterator must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
        .isLessThanOrEqualTo(DEGREE);
  }

  @Test
  void ridIteratorEndsTheWalkInsteadOfFeedingItForever() {
    final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

    final int[] walked = new int[1];
    database.transaction(() -> {
      final Iterator<RID> rids = edgeLinkedListFor(hub).ridIterator();
      while (walked[0] <= CAP && rids.hasNext()) {
        rids.next();
        ++walked[0];
      }
    });

    assertThat(walked[0])
        .as("RIDIterator must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
        .isLessThanOrEqualTo(DEGREE);
  }

  @Test
  void edgeIteratorFilterEndsTheWalkInsteadOfFeedingItForever() {
    final RID hub = createHubWithDistinctSpokesAndSelfLoopHead();

    final int[] walked = new int[1];
    database.transaction(() -> {
      // Passing an edge type routes EdgeLinkedList.edgeIterator() to EdgeIteratorFilter, which is what exercises
      // the shared chunk-hop through IteratorFilterBase rather than through EdgeIterator directly.
      final Iterator<Edge> edges = edgeLinkedListFor(hub).edgeIterator(EDGE_TYPE);
      while (walked[0] <= CAP && edges.hasNext()) {
        edges.next();
        ++walked[0];
      }
    });

    assertThat(walked[0])
        .as(
            "EdgeIteratorFilter (IteratorFilterBase) must stop on a self-referencing chunk; without the guard it yields the chunk's entries forever")
        .isLessThanOrEqualTo(DEGREE);
  }

  /** One hub with {@code DEGREE} distinct spokes, its IN head chunk pointing at itself. */
  private RID createHubWithDistinctSpokesAndSelfLoopHead() {
    final RID[] hub = new RID[1];
    database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
    database.transaction(() -> {
      final MutableVertex target = hub[0].asVertex(true).modify();
      for (int i = 0; i < DEGREE; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
    });

    // Point the hub's IN head chunk at itself.
    database.transaction(() -> {
      final RID head = ((VertexInternal) hub[0].asVertex(true)).getInEdgesHeadChunk();
      final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(head, true);
      segment.setPrevious(segment);
      ((DatabaseInternal) database).updateRecord(segment);
    });

    return hub[0];
  }

  private EdgeLinkedList edgeLinkedListFor(final RID hub) {
    return ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
  }
}
