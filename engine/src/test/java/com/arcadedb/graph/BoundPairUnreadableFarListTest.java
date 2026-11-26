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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link GraphEngine#getEdgesConnectedTo} reads the pair off the shorter edge list (issue #11699), but a far end whose
 * list cannot be read is not a far end with no edges: the pair is read off the source's list, as it was before the
 * shorter side was chosen. Answering from the unreadable list returned no edge although the source holds one, and a
 * MERGE deciding on that would create a duplicate. Here the far head chunk is deleted outright; under load the same
 * null comes from a head a concurrent commit has not published yet (see {@link GraphEngine#getEdgeHeadChunkForWrite}).
 */
class BoundPairUnreadableFarListTest extends TestHelper {
  // MORE ENTRIES THAN THE NEAR LIST WALKS ALONE BEFORE THE FAR LIST IS LOOKED AT
  private static final int FILLERS    = 200;
  // THE INCOMING RELATIONSHIPS OF A FAR END PROMOTED TO A SUPER-NODE. THE HUB HAS FILLERS MORE, SO THE FAR LIST WINS THE RACE
  private static final int FAR_DEGREE = 150;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // THE TEST DELETES AN EDGE CHUNK ON PURPOSE: CHECK DATABASE WOULD REPORT EXACTLY THAT DAMAGE
    return false;
  }

  @Test
  void anUnreadableFarListLeavesTheWalkOnTheSource() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE T");
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      final MutableVertex leaf = database.newVertex("N").save();
      for (int i = 0; i < FILLERS; i++)
        hub.newEdge("T", database.newVertex("N").save());
      hub.newEdge("T", leaf);
      ends[0] = hub.getIdentity();
      ends[1] = leaf.getIdentity();
    });
    final RID[] leafHead = new RID[1];
    database.transaction(() -> leafHead[0] = ((VertexInternal) ends[1].asVertex()).getInEdgesHeadChunk());
    assertThat(leafHead[0]).isNotNull();
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, leafHead[0]));

    database.transaction(() -> {
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      final VertexInternal hub = (VertexInternal) ends[0].asVertex();
      for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.BOTH }) {
        final List<Edge> edges = drain(engine.getEdgesConnectedTo(hub, direction, ends[1], "T"));
        assertThat(edges).as("%s", direction).hasSize(1);
        assertThat(edges.getFirst().getIn()).isEqualTo(ends[1]);
      }
    });
  }

  /**
   * The same on a far end promoted to a super-node (#5156), whose list is one chain per stripe. A read walk skips a stripe
   * chain whose head cannot be loaded, so the race took what was left for the whole far list, the shorter one, and the pair
   * read off it missed every edge that stripe holds: all of them, since the edges to one neighbour sit in one stripe of a
   * generation. Whether the skipped stripe made a longer list look shorter or not, the edges it holds were lost.
   */
  @Test
  void anUnreadableStripeOfAFarListLeavesTheWalkOnTheSource() {
    database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 64);
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE T");
    final RID[] ends = new RID[2];
    database.transaction(() -> {
      final MutableVertex leaf = database.newVertex("N").save();
      for (int i = 0; i < FAR_DEGREE; i++)
        database.newVertex("N").save().newEdge("T", leaf);
      ends[1] = leaf.getIdentity();
    });
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("N").save();
      for (int i = 0; i < FAR_DEGREE + FILLERS; i++)
        hub.newEdge("T", database.newVertex("N").save());
      hub.newEdge("T", ends[1].asVertex());
      ends[0] = hub.getIdentity();
    });

    // THE STRIPE OF THE NEWEST GENERATION THAT HOLDS THE HUB'S ENTRY IN THE LEAF'S INCOMING LIST
    final RID[] stripeHead = new RID[1];
    database.transaction(() -> {
      final RID head = ((VertexInternal) ends[1].asVertex()).getInEdgesHeadChunk();
      assertThat(database.lookupByRID(head, true)).as("the far end must be a promoted super-node")
          .isInstanceOf(StripeDirectory.class);
      final StripeDirectory directory = (StripeDirectory) database.lookupByRID(head, true);
      final int generation = directory.getGenerationCount() - 1;
      stripeHead[0] = directory.getHead(generation, StripeDirectory.stripeOf(ends[0], directory.getStripes(generation)));
      assertThat(stripeHead[0]).isNotNull();
    });
    database.transaction(() -> TestHelper.deleteRecordAtLowLevel(database, stripeHead[0]));

    database.transaction(() -> {
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      final VertexInternal hub = (VertexInternal) ends[0].asVertex();
      for (final Vertex.DIRECTION direction : new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.BOTH }) {
        final List<Edge> edges = drain(engine.getEdgesConnectedTo(hub, direction, ends[1], "T"));
        assertThat(edges).as("%s", direction).hasSize(1);
        assertThat(edges.getFirst().getIn()).isEqualTo(ends[1]);
      }
      // A LIST PART OF WHICH CANNOT BE READ IS NOT A SHORT ONE, AS A HEAD THAT CANNOT BE READ IS NOT (ISSUE #11715)
      assertThat(engine.hasMoreEdgeEntriesThan((VertexInternal) ends[1].asVertex(), Vertex.DIRECTION.IN, FAR_DEGREE * 2))
          .isTrue();
    });
  }

  private static List<Edge> drain(final Iterator<Edge> edges) {
    final List<Edge> result = new ArrayList<>();
    while (edges.hasNext())
      result.add(edges.next());
    return result;
  }
}
