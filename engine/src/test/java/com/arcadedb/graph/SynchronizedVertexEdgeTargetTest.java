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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: creating an edge whose TARGET is a {@link SynchronizedVertex} (the wrapper used to share a cached
 * vertex across threads) must work. Before the fix, {@code from.newEdge(type, cachedSynchronizedVertex)} threw
 * {@code java.lang.ClassCastException: SynchronizedVertex cannot be cast to VertexInternal} in
 * {@link GraphEngine#connectIncomingEdge}, because {@code SynchronizedVertex.asVertex()} returns the wrapper (which is
 * a {@link com.arcadedb.graph.Vertex} but not a {@link VertexInternal}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SynchronizedVertexEdgeTargetTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("User");
      database.getSchema().createVertexType("Group");
      database.getSchema().createEdgeType("MemberOf");
    });
  }

  @Test
  void canCreateEdgeToSynchronizedVertexTarget() {
    final RID[] rids = new RID[2];

    database.transaction(() -> {
      final MutableVertex user = database.newVertex("User").set("name", "u1").save();
      final MutableVertex group = database.newVertex("Group").set("name", "g1").save();
      rids[0] = user.getIdentity();
      rids[1] = group.getIdentity();

      // Wrap the TARGET in a SynchronizedVertex and create an edge to it (mirrors a cached vertex passed to newEdge).
      final SynchronizedVertex synchronizedTarget = new SynchronizedVertex(group);
      user.newEdge("MemberOf", synchronizedTarget);
    });

    // Both directions of the edge must be present and traversable.
    database.transaction(() -> {
      final Vertex user = database.lookupByRID(rids[0], true).asVertex();
      final Vertex group = database.lookupByRID(rids[1], true).asVertex();

      assertThat(user.countEdges(Vertex.DIRECTION.OUT, "MemberOf")).isEqualTo(1L);
      assertThat(group.countEdges(Vertex.DIRECTION.IN, "MemberOf")).isEqualTo(1L);
      assertThat(user.getVertices(Vertex.DIRECTION.OUT, "MemberOf").iterator().next().getIdentity()).isEqualTo(rids[1]);
      assertThat(group.getVertices(Vertex.DIRECTION.IN, "MemberOf").iterator().next().getIdentity()).isEqualTo(rids[0]);
    });
  }

  @Test
  void canCreateEdgeBetweenTwoSynchronizedVertices() {
    final RID[] rids = new RID[2];

    database.transaction(() -> {
      final MutableVertex user = database.newVertex("User").set("name", "u2").save();
      final MutableVertex group = database.newVertex("Group").set("name", "g2").save();
      rids[0] = user.getIdentity();
      rids[1] = group.getIdentity();

      // Source is a SynchronizedVertex too: newEdge delegates to the real MutableVertex, target unwraps on connect.
      final SynchronizedVertex synchronizedSource = new SynchronizedVertex(user);
      final SynchronizedVertex synchronizedTarget = new SynchronizedVertex(group);
      synchronizedSource.newEdge("MemberOf", synchronizedTarget);
    });

    database.transaction(() -> {
      final Vertex group = database.lookupByRID(rids[1], true).asVertex();
      assertThat(group.countEdges(Vertex.DIRECTION.IN, "MemberOf")).isEqualTo(1L);
    });
  }
}
