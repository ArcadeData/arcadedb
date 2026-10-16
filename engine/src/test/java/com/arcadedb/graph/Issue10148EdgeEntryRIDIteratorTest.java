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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10148: {@code GraphEngine.getConnectedVertexEntries} walks the adjacency entries of a vertex on the RIDs they hold, as
 * {@code getConnectedVertexRIDs} does, and names the edge of the entry it last returned without reading a record. Over every list
 * layout (a plain list, with and without an edge-type filter, and the striped layout of a promoted super-node) it must return
 * the vertices in the order {@code getConnectedVertexRIDs} does, each with the RID of the edge that links it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10148EdgeEntryRIDIteratorTest extends TestHelper {
  private int savedThreshold;
  private int savedStripes;

  @BeforeEach
  void saveConfig() {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Hub");
    database.getSchema().createVertexType("Src");
    database.getSchema().createEdgeType("LINK");
    database.getSchema().createEdgeType("OTHER");
  }

  @Test
  void aPlainListNamesTheEdgeOfEveryEntry() {
    final RID hubId = buildHub(40);

    database.transaction(() -> {
      assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT);
      assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "LINK");
      assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "OTHER");
      assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "LINK", "OTHER");
    });
  }

  @Test
  void theIncomingListNamesTheEdgeOfEveryEntry() {
    final RID hubId = buildHub(40);

    database.transaction(() -> {
      final Vertex hub = hubId.asVertex();
      final List<RID> neighbors = new ArrayList<>();
      for (final Edge edge : hub.getEdges(Vertex.DIRECTION.OUT, "LINK"))
        neighbors.add(edge.getIn());
      // the far end sees the same edge in its incoming list
      for (final RID neighbor : neighbors)
        assertEveryEntryNamesItsEdge(neighbor, Vertex.DIRECTION.IN, "LINK");
    });
  }

  @Test
  void aPromotedSuperNodeNamesTheEdgeOfEveryEntryInTheOrderOfTheRidWalk() {
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(4);
    final RID hubId = buildHub(600);

    // PRECONDITION: THE HUB MUST REALLY HAVE BEEN PROMOTED, OTHERWISE THE TEST PROVES NOTHING
    database.transaction(() -> {
      final RID head = ((VertexInternal) hubId.asVertex(true)).getOutEdgesHeadChunk();
      assertThat(database.lookupByRID(head, true)).isInstanceOf(StripeDirectory.class);
      assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT);
      assertEveryEntryNamesItsEdge(hubId, Vertex.DIRECTION.OUT, "LINK");
    });
  }

  @Test
  void aVertexWithoutEdgesHasNoEntries() {
    final RID[] lone = new RID[1];
    database.transaction(() -> lone[0] = database.newVertex("Src").save().getIdentity());
    final RID loneId = lone[0];

    database.transaction(() -> {
      final EdgeEntryRIDIterator entries = engine().getConnectedVertexEntries((VertexInternal) loneId.asVertex(), Vertex.DIRECTION.OUT);
      assertThat(entries.hasNext()).isFalse();
      assertThatThrownBy(entries::next).isInstanceOf(NoSuchElementException.class);
      assertThatThrownBy(entries::getEdgeRID).isInstanceOf(IllegalStateException.class);
    });
  }

  @Test
  void theEdgeIsAskedAfterAnEntryWasReturned() {
    final RID hubId = buildHub(3);

    database.transaction(() -> {
      final EdgeEntryRIDIterator entries = engine().getConnectedVertexEntries((VertexInternal) hubId.asVertex(), Vertex.DIRECTION.OUT);
      assertThatThrownBy(entries::getEdgeRID).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> engine().getConnectedVertexEntries((VertexInternal) hubId.asVertex(), Vertex.DIRECTION.BOTH))
          .isInstanceOf(IllegalArgumentException.class);
    });
  }

  /** The hub points to {@code count} vertices over LINK (with a property, so a record each) and to a few over OTHER. */
  private RID buildHub(final int count) {
    final RID[] hubId = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub").save();
      for (int i = 0; i < count; i++) {
        final MutableVertex src = database.newVertex("Src").set("i", i).save();
        hub.newEdge("LINK", src).set("n", i).save();
      }
      for (int i = 0; i < 5; i++)
        hub.newEdge("OTHER", database.newVertex("Src").set("i", -i).save()).set("n", -i).save();
      hubId[0] = hub.getIdentity();
    });
    return hubId[0];
  }

  /** The entries come in the order of the RID walk, and the edge each names is the edge that links the vertex it returned. */
  private void assertEveryEntryNamesItsEdge(final RID vertexId, final Vertex.DIRECTION direction, final String... types) {
    final VertexInternal vertex = (VertexInternal) vertexId.asVertex();
    final List<RID> expected = new ArrayList<>();
    for (final RID neighbor : engine().getConnectedVertexRIDs(vertex, direction, types))
      expected.add(neighbor);

    // The edges as the edge walk sees them: the far end each links
    final Map<RID, RID> edgeOfNeighbor = new HashMap<>();
    for (final Edge edge : vertex.getEdges(direction, types))
      edgeOfNeighbor.put(direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut(), edge.getIdentity());

    final List<RID> seen = new ArrayList<>();
    final EdgeEntryRIDIterator entries = engine().getConnectedVertexEntries(vertex, direction, types);
    while (entries.hasNext()) {
      final RID neighbor = entries.next();
      seen.add(neighbor);
      assertThat(entries.getEdgeRID()).as("the edge of the entry for %s", neighbor).isEqualTo(edgeOfNeighbor.get(neighbor));
      // asking twice is the same answer, and asking does not move the walk
      assertThat(entries.getEdgeRID()).isEqualTo(edgeOfNeighbor.get(neighbor));
    }

    assertThat(seen).as("%s %s", direction, List.of(types)).isEqualTo(expected);
    assertThat(seen).isNotEmpty();
  }

  private GraphEngine engine() {
    return ((DatabaseInternal) database).getGraphEngine();
  }
}
