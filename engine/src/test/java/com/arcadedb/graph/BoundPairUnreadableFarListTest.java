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
  private static final int FILLERS = 200;

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

  private static List<Edge> drain(final Iterator<Edge> edges) {
    final List<Edge> result = new ArrayList<>();
    while (edges.hasNext())
      result.add(edges.next());
    return result;
  }
}
