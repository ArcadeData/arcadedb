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
import com.arcadedb.database.RID;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A {@link GAVVertex} answers from the view only for the edge types the view lists (issue #9377), and an unfiltered call
 * only when the view lists every edge type of the schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GAVVertexEdgeTypeCoverageTest extends TestHelper {

  private GAVVertex vertexOf(final GraphAnalyticalView view, final RID rid) {
    return new GAVVertex(rid, view.getNodeId(rid), view, database);
  }

  @Test
  void unlistedEdgeTypesAreAnsweredFromTheRecord() throws Exception {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.getSchema().createEdgeType("F");
    final RID[] v = new RID[2];
    database.transaction(() -> {
      final MutableVertex x = database.newVertex("V").save(), y = database.newVertex("V").save();
      x.newEdge("E", y).save();
      x.newEdge("F", y).save();
      v[0] = x.getIdentity();
      v[1] = y.getIdentity();
    });

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW partial9377 VERTEX TYPES (V) EDGE TYPES (E) UPDATE MODE OFF");
    try {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "partial9377");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      final GAVVertex x = vertexOf(view, v[0]);
      assertThat(x.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
      assertThat(x.countEdges(Vertex.DIRECTION.OUT, "F")).isEqualTo(1);
      assertThat(x.countEdges(Vertex.DIRECTION.OUT)).as("untyped over a partial view").isEqualTo(2);
      assertThat(x.getConnectedVertexRIDs(Vertex.DIRECTION.OUT, "F")).containsExactly(v[1]);
      assertThat(x.isConnectedTo(database.lookupByRID(v[1], true), Vertex.DIRECTION.OUT, "F")).isTrue();
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW partial9377");
    }

    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW full9377 VERTEX TYPES (V) EDGE TYPES (E, F) UPDATE MODE OFF");
    try {
      final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, "full9377");
      assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
      final GAVVertex x = vertexOf(view, v[0]);
      assertThat(x.countEdges(Vertex.DIRECTION.OUT)).as("untyped over a view of every edge type").isEqualTo(2);
      assertThat(x.countEdges(Vertex.DIRECTION.OUT, "F")).isEqualTo(1);
    } finally {
      database.command("sql", "DROP GRAPH ANALYTICAL VIEW full9377");
    }
  }
}
