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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.utility.ResettableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10151: the {@link ResettableIterator} contract, driven over EVERY adjacency iterator the engine builds rather than
 * over one class. A {@code hasNext()} peeks the next entry and keeps it; a {@code reset()} that forgets to drop it hands
 * it out once more before the rewound walk hands it out again, so the ordinary "is there any neighbour?" check, followed
 * by a loop over the same {@code Iterable} (whose {@code iterator()} resets every source), yields the first neighbour
 * twice. {@code IteratorFilterBase} and {@code RIDIteratorFilter} both had that hole, each in its own copy of the peek.
 * <p>
 * For every walk - vertices, edges, neighbour RIDs and the raw edge-list iterators, OUT/IN/BOTH, with and without a type
 * filter and a neighbour filter, on the classic and on the striped super-node layout - {@code peek; reset; drain} and
 * {@code take k; peek; reset; drain} must both equal a fresh drain.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10151AdjacencyIteratorResetContractTest {
  private static final String DB_PATH   = "./target/databases/issue10151-reset-contract";
  private static final int    RECORD_E  = 40;
  private static final int    RECORD_F  = 10;
  private static final int    LIGHT_L   = 10;
  private static final int    INCOMING  = 12;
  private static final int    THRESHOLD = 16;

  private Database database;
  private int      savedThreshold;
  private int      savedStripes;

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
  }

  @ParameterizedTest(name = "striped={0}")
  @ValueSource(booleans = { false, true })
  void peekResetDrainEqualsAFreshDrainOnEveryAdjacencyIterator(final boolean striped) {
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    // 0 DISABLES THE PROMOTION, SO THE CLASSIC RUN NEVER BECOMES A STRIPED ONE WHATEVER THE DEFAULT THRESHOLD IS
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(striped ? THRESHOLD : 0);
    GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(4);

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    final RID[] hubAndNeighbour = new RID[2];
    database.transaction(() -> {
      database.getSchema().createVertexType("V");
      database.getSchema().createEdgeType("E");
      database.getSchema().createEdgeType("F");
      database.getSchema().createEdgeType("L");

      final MutableVertex hub = database.newVertex("V").set("name", "hub").save();
      final MutableVertex neighbour = database.newVertex("V").set("name", "neighbour").save();
      hubAndNeighbour[0] = hub.getIdentity();
      hubAndNeighbour[1] = neighbour.getIdentity();
      for (int i = 0; i < RECORD_E; i++)
        hub.newEdge("E", i % 7 == 0 ? neighbour : database.newVertex("V").set("n", i).save());
      for (int i = 0; i < RECORD_F; i++)
        hub.newEdge("F", database.newVertex("V").set("f", i).save());
      for (int i = 0; i < LIGHT_L; i++)
        hub.newLightEdge("L", i % 3 == 0 ? neighbour : database.newVertex("V").set("l", i).save());
      for (int i = 0; i < INCOMING; i++)
        (i % 4 == 0 ? neighbour : database.newVertex("V").set("in", i).save()).newEdge(i % 2 == 0 ? "E" : "F", hub);
      for (int i = 0; i < 2; i++) {
        neighbour.newEdge("F", hub);
        neighbour.newLightEdge("L", hub);
      }
    });

    final VertexInternal hub = (VertexInternal) hubAndNeighbour[0].asVertex();
    final RID neighbour = hubAndNeighbour[1];
    final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();

    // PRECONDITION: THE LAYOUT UNDER TEST IS THE ONE ASKED FOR, OTHERWISE HALF OF THE MATRIX PROVES NOTHING
    assertThat(engine.getEdgeHeadChunk(hub, Vertex.DIRECTION.OUT) instanceof StripedEdgeList).isEqualTo(striped);

    final Map<String, Supplier<Iterator<?>>> walks = new LinkedHashMap<>();
    final String[][] filters = { {}, { "E" }, { "E", "L" }, { "F", "E" } };
    for (final Vertex.DIRECTION direction : Vertex.DIRECTION.values())
      for (final String[] types : filters) {
        final String label = direction + " " + String.join(",", types);
        walks.put("getVertices " + label, () -> hub.getVertices(direction, types).iterator());
        walks.put("getEdges " + label, () -> hub.getEdges(direction, types).iterator());
        walks.put("getConnectedVertexRIDs " + label, () -> engine.getConnectedVertexRIDs(hub, direction, types).iterator());
        walks.put("getEdgesConnectedTo " + label, () -> engine.getEdgesConnectedTo(hub, direction, neighbour, types));
        if (direction == Vertex.DIRECTION.BOTH)
          continue;
        final Supplier<EdgeLinkedList> list = () -> engine.getEdgeHeadChunk(hub, direction);
        walks.put("entryIterator " + label, () -> list.get().entryIterator(types));
        walks.put("edgeIterator " + label, () -> list.get().edgeIterator(types));
        walks.put("edgeIteratorKnowingEndpoints " + label, () -> list.get().edgeIteratorKnowingEndpoints(types));
        walks.put("edgeIteratorConnectedTo " + label, () -> list.get().edgeIteratorConnectedTo(neighbour, types));
        walks.put("vertexIterator " + label, () -> list.get().vertexIterator(types));
        walks.put("ridIterator " + label, () -> list.get().ridIterator(types));
        walks.put("entryRidIterator " + label, () -> list.get().entryRidIterator(types));
      }

    int resettable = 0;
    for (final Map.Entry<String, Supplier<Iterator<?>>> walk : walks.entrySet()) {
      final List<Object> fresh = drain(walk.getValue().get());
      // A WALK WITH NOTHING TO HAND OUT CANNOT SHOW A DUPLICATED PEEK: EVERY FILTER ABOVE MATCHES AT LEAST TWO ENTRIES
      assertThat(fresh).as(walk.getKey()).hasSizeGreaterThan(1);

      for (int taken = 0; taken <= 2; taken++) {
        final Iterator<?> it = walk.getValue().get();
        if (!(it instanceof ResettableIterator<?> rewindable))
          break;
        for (int i = 0; i < taken; i++)
          it.next();
        assertThat(it.hasNext()).as(walk.getKey()).isTrue();
        rewindable.reset();
        assertThat(drain(it)).as(walk.getKey() + ", " + taken + " taken, peeked, reset").isEqualTo(fresh);
        if (taken == 0)
          ++resettable;
      }
    }
    // THE CONTRACT IS ONLY WORTH SOMETHING IF IT REACHED THE ITERATORS: EVERY CLASSIC ONE IS RESETTABLE
    assertThat(resettable).isGreaterThanOrEqualTo(walks.size() * 3 / 4);
  }

  private static List<Object> drain(final Iterator<?> it) {
    final List<Object> out = new ArrayList<>();
    while (it.hasNext())
      out.add(keyOf(it.next()));
    return out;
  }

  private static Object keyOf(final Object element) {
    if (element instanceof Edge edge)
      // A LIGHTWEIGHT EDGE HAS NO RECORD IDENTITY OF ITS OWN: ITS ENDPOINTS TELL IT APART
      return edge.getIdentity() + ":" + edge.getOut() + "->" + edge.getIn();
    if (element instanceof Identifiable identifiable)
      return identifiable.getIdentity();
    return element;
  }
}
