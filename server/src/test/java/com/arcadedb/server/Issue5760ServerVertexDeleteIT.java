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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5760 on a WRAPPED database. Deleting a vertex now routes each of its edges through
 * {@code DatabaseInternal.deleteEdgeSkippingEndpoint} rather than {@code Record.delete()}, and it does so through
 * the handle the EDGE carries - which on a server is the {@link ServerDatabase} wrapper, because
 * {@code LocalDatabase} builds every record with its {@code wrappedDatabaseInstance}. So the delegation added to
 * that wrapper is on the live path, not a formality, and this pins it: the same delete, over a real server
 * database, still takes every back-reference with it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5760ServerVertexDeleteIT extends BaseGraphServerTest {

  private static final int EDGES = 120;

  @Test
  void deletingAVertexOnAServerDatabaseDisconnectsEveryNeighbour() {
    // getServerDatabase, NOT getDatabase: the latter hands back the raw embedded instance the fixture opened, which
    // would exercise the plain LocalDatabase path and prove nothing about the delegation this test exists for.
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThat(database).as("the test must run against the server wrapper, not the embedded database")
        .isInstanceOf(ServerDatabase.class);

    database.transaction(() -> {
      database.getSchema().createVertexType("Issue5760Hub", 1);
      database.getSchema().createVertexType("Issue5760Src", 4);
      database.getSchema().createEdgeType("Issue5760Link", 4);
    });

    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Issue5760Hub");
      hub.save();
      holder[0] = hub.getIdentity();
    });
    final RID hubRID = holder[0];

    final List<RID> sources = new ArrayList<>(EDGES);
    for (int i = 0; i < EDGES; i++) {
      final RID[] src = new RID[1];
      database.transaction(() -> {
        final MutableVertex s = database.newVertex("Issue5760Src");
        s.save();
        s.newEdge("Issue5760Link", hubRID);
        src[0] = s.getIdentity();
      });
      sources.add(src[0]);
    }

    // A self-loop too: both of its endpoints are the vertex being deleted, so both sides are skipped.
    database.transaction(() -> hubRID.asVertex().modify().newEdge("Issue5760Link", hubRID));

    database.transaction(() -> {
      assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "Issue5760Link")).isEqualTo(EDGES + 1);
      assertThat(database.countType("Issue5760Link", false)).isEqualTo(EDGES + 1L);
    });

    database.transaction(() -> hubRID.asVertex().delete());

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).isFalse();
      assertThat(database.countType("Issue5760Link", false)).as("every edge touched the hub").isEqualTo(0L);
      for (final RID source : sources)
        assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "Issue5760Link"))
            .as("source " + source + " must not keep a back-reference to a deleted edge").isEqualTo(0);
    });
  }
}
