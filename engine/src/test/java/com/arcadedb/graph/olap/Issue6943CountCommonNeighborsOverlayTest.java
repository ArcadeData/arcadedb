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
package com.arcadedb.graph.olap;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6943's second finding: {@code GraphAnalyticalView.countCommonNeighbors} was the one
 * pair accessor on this class that never consulted {@code snap.overlay} - both {@code isConnectedTo} and
 * {@code countEdgesBetween} already subtract overlay deletions and add overlay additions for the same kind of pair
 * question. It also omitted the {@code nodeA < snap.nodeMapping.size()} guard both siblings carry, so a node id the
 * overlay handed out for a newly added vertex (legal once {@link GraphAnalyticalView#getNodeIdUpperBound()} exceeds
 * the base node count) indexed the base CSR's offsets array straight past its end.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6943CountCommonNeighborsOverlayTest extends TestHelper {

  @Test
  void overlayDeletedCommonNeighbourIsExcluded() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    final MutableVertex x = database.newVertex("Person").set("name", "X").save();
    a.newEdge("KNOWS", x);
    b.newEdge("KNOWS", x);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("common-neighbors-overlay-delete")
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());

    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
        .as("precondition: X is a common OUT-neighbour of A and B before the overlay touches anything").isEqualTo(1);

    // Delete A -> X within the overlay window (no compaction in between).
    database.begin();
    a.getIdentity().asVertex().getEdges(Vertex.DIRECTION.OUT, "KNOWS").forEach(e -> {
      if (e.getIn().equals(x.getIdentity()))
        e.delete();
    });
    database.commit();

    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
        .as("an overlay-deleted common neighbour must no longer be counted (#6943)").isEqualTo(0);

    gav.drop();
  }

  @Test
  void overlayAddedCommonNeighbourIsIncluded() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex a = database.newVertex("Person").set("name", "A").save();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    final MutableVertex x = database.newVertex("Person").set("name", "X").save();
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("common-neighbors-overlay-add")
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    final int idA = gav.getNodeId(a.getIdentity());
    final int idB = gav.getNodeId(b.getIdentity());

    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
        .as("precondition: no common neighbour before the overlay adds anything").isEqualTo(0);

    // Add A -> X and B -> X within the overlay window (no compaction in between).
    database.begin();
    a.getIdentity().asVertex().modify().newEdge("KNOWS", x);
    b.getIdentity().asVertex().modify().newEdge("KNOWS", x);
    database.commit();

    assertThat(gav.countCommonNeighbors(idA, idB, Vertex.DIRECTION.OUT, "KNOWS"))
        .as("an overlay-added common neighbour must be counted (#6943)").isEqualTo(1);

    gav.drop();
  }

  @Test
  void overlayAddedNodeIdDoesNotThrowArrayIndexOutOfBounds() {
    database.getSchema().createVertexType("Person");
    database.getSchema().createEdgeType("KNOWS");

    database.begin();
    final MutableVertex b = database.newVertex("Person").set("name", "B").save();
    final MutableVertex x = database.newVertex("Person").set("name", "X").save();
    b.newEdge("KNOWS", x);
    database.commit();

    final GraphAnalyticalView gav = GraphAnalyticalView.builder(database)
        .withName("common-neighbors-overlay-new-node")
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS")
        .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS)
        .build();

    final int idB = gav.getNodeId(b.getIdentity());
    assertThat(gav.getNodeCount()).as("precondition").isEqualTo(2);

    // Add a brand-new vertex C within the overlay window: its node id is >= the base node count, the
    // "legal once getNodeIdUpperBound() exceeds getNodeCount()" shape #6943 describes.
    database.begin();
    final MutableVertex c = database.newVertex("Person").set("name", "C").save();
    c.newEdge("KNOWS", x);
    database.commit();

    final int idC = gav.getNodeId(c.getIdentity());
    assertThat(idC).as("precondition: C's node id must be an overlay-added id past the base node count")
        .isGreaterThanOrEqualTo(gav.getNodeCount() - 1);

    // Before the fix, nodeA=idC indexed the base CSR's offsets array without a bounds check and threw AIOOBE.
    assertThat(gav.countCommonNeighbors(idC, idB, Vertex.DIRECTION.OUT, "KNOWS"))
        .as("X is a common OUT-neighbour of the overlay-added C and the base node B (#6943)").isEqualTo(1);
    assertThat(gav.countCommonNeighbors(idB, idC, Vertex.DIRECTION.OUT, "KNOWS"))
        .as("the same question with the overlay-added id as nodeB").isEqualTo(1);

    gav.drop();
  }
}
