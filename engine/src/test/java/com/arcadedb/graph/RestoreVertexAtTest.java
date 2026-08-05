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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.RecordNotFoundException;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end regression test reproducing the production incident this feature was built for: a vertex with
 * outgoing and incoming edges is deleted out of band (a raw {@code bucket.deleteRecord}, exactly what
 * {@code CHECK DATABASE FIX}'s vertex arm and {@code LocalBucket.check(fix=true)} do - no graph cascade), leaving
 * its edges dangling. {@link GraphEngine#restoreVertexAt} must bring the vertex back at the SAME RID with all of
 * its edges reconnected, without touching any of the neighbour vertices' own edge lists (which were never broken).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RestoreVertexAtTest extends TestHelper {

  private static final String VERTEX_TYPE = "Person";
  private static final String EDGE_TYPE   = "Knows";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });
  }

  @Test
  void restoresARawDeletedHubVertexAndReconnectsAllItsEdges() {
    final DatabaseInternal db = (DatabaseInternal) database;

    final RID[] hub = new RID[1];
    final RID[] leaves = new RID[4];

    database.transaction(() -> {
      final MutableVertex h = database.newVertex(VERTEX_TYPE).set("name", "hub").save();
      hub[0] = h.getIdentity();
      for (int i = 0; i < leaves.length; i++)
        leaves[i] = database.newVertex(VERTEX_TYPE).set("name", "leaf" + i).save().getIdentity();

      // hub -> leaf0, hub -> leaf1 (outgoing from hub)
      h.newEdge(EDGE_TYPE, leaves[0]).save();
      h.newEdge(EDGE_TYPE, leaves[1]).save();
      // leaf2 -> hub, leaf3 -> hub (incoming to hub)
      database.lookupByRID(leaves[2], true).asVertex().newEdge(EDGE_TYPE, hub[0]).save();
      database.lookupByRID(leaves[3], true).asVertex().newEdge(EDGE_TYPE, hub[0]).save();
    });

    // Sanity: the hub really does have 2 out + 2 in edges before the incident.
    database.transaction(() -> {
      final Vertex h = database.lookupByRID(hub[0], true).asVertex();
      assertThat(count(h.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE))).isEqualTo(2);
      assertThat(count(h.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))).isEqualTo(2);
    });

    // THE INCIDENT: raw, no-cascade delete of the hub - only its own record's slot is freed, edges untouched.
    final LocalBucket hubBucket = (LocalBucket) db.getSchema().getBucketById(hub[0].getBucketId());
    database.transaction(() -> hubBucket.deleteRecord(hub[0]));

    database.transaction(() -> assertThatThrownBy(() -> database.lookupByRID(hub[0], true)).isInstanceOf(RecordNotFoundException.class));

    // THE REPAIR
    database.transaction(() -> db.getGraphEngine().restoreVertexAt(hub[0], VERTEX_TYPE));

    database.transaction(() -> {
      final Vertex h = database.lookupByRID(hub[0], true).asVertex();
      assertThat(h.getIdentity()).isEqualTo(hub[0]);

      final Set<RID> outNeighbours = new HashSet<>();
      for (final Vertex v : h.getVertices(Vertex.DIRECTION.OUT, EDGE_TYPE))
        outNeighbours.add(v.getIdentity());
      assertThat(outNeighbours).containsExactlyInAnyOrder(leaves[0], leaves[1]);

      final Set<RID> inNeighbours = new HashSet<>();
      for (final Vertex v : h.getVertices(Vertex.DIRECTION.IN, EDGE_TYPE))
        inNeighbours.add(v.getIdentity());
      assertThat(inNeighbours).containsExactlyInAnyOrder(leaves[2], leaves[3]);
    });

    // The neighbours' OWN edge lists were never broken by the incident and must be unaffected by the repair: leaf2
    // and leaf3 could already traverse to the hub before the restore ran (that's what "the edges point to a RID
    // that doesn't exist yet" means), and still can afterward.
    database.transaction(() -> {
      final Vertex leaf2 = database.lookupByRID(leaves[2], true).asVertex();
      final Set<RID> leaf2Out = new HashSet<>();
      for (final Vertex v : leaf2.getVertices(Vertex.DIRECTION.OUT, EDGE_TYPE))
        leaf2Out.add(v.getIdentity());
      assertThat(leaf2Out).containsExactly(hub[0]);
    });
  }

  private static int count(final Iterable<Edge> edges) {
    int n = 0;
    for (final Edge ignored : edges)
      n++;
    return n;
  }
}
