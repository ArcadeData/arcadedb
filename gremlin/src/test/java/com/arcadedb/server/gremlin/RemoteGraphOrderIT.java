/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.gremlin;

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.server.BaseGraphServerTest;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteGraphOrderIT extends AbstractGremlinServerIT {

  @Test
  public void testOrder() throws Exception {
    testEachServer((serverIndex) -> {
      assertThat(
          new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
              getDatabaseName())).isTrue();

      try (final RemoteDatabase db = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        /*
         * (rootVtx) --<edgType0>--> (connectedVtx0)
         *    |--------<edgType1>--> (connectedVtx1)
         */
        //Create RootVtx Type
        db.command("sql", "CREATE VERTEX TYPE RootVtx IF NOT EXISTS");

        //Create ConnectedVtx Type
        db.command("sql", "CREATE VERTEX TYPE ConnectedVtx IF NOT EXISTS");

        //Create EdgType0 Type
        db.command("sql", "CREATE EDGE TYPE EdgType0 IF NOT EXISTS");

        //Create EdgType1 Type
        db.command("sql", "CREATE EDGE TYPE EdgType1 IF NOT EXISTS");

        Vertex rootVtx = db.command("sql", "CREATE VERTEX RootVtx").next().getVertex().get();
        Vertex connectedVtx0 = db.command("sql", "CREATE VERTEX ConnectedVtx").next().getVertex().get();
        Vertex connectedVtx1 = db.command("sql", "CREATE VERTEX ConnectedVtx").next().getVertex().get();

        //EdgType0 added first
        Edge edgType0 = rootVtx.newEdge("EdgType0", connectedVtx0);

        //EdgType1 added last
        Edge edgType1 = rootVtx.newEdge("EdgType1", connectedVtx1);

        //Correct result - Returns one vertex/edge
// Vertices with outgoing "EdgType0" edge
        Iterable<Vertex> connectedVertices = rootVtx.getVertices(Vertex.DIRECTION.OUT, "EdgType0");

        assertThat(connectedVertices)
            .extracting(Vertex::getTypeName) // extract type names of vertices
            .containsExactly("ConnectedVtx"); // assert all extracted names are "ConnectedVtx"

        // Edges with type "EdgType0"
        Iterable<Edge> outgoingEdges = rootVtx.getEdges(Vertex.DIRECTION.OUT, "EdgType0");

        assertThat(outgoingEdges)
            .extracting(Edge::getTypeName) // extract edge types
            .containsExactly("EdgType0"); // assert all extracted types are "EdgType0"

        // Incoming edge counts
        assertThat(rootVtx.countEdges(Vertex.DIRECTION.IN, "EdgType0")).isEqualTo(0);
        assertThat(rootVtx.countEdges(Vertex.DIRECTION.IN, "EdgType1")).isEqualTo(0);
        assertThat(rootVtx.countEdges(Vertex.DIRECTION.OUT, "EdgType0")).isEqualTo(1);
        assertThat(rootVtx.countEdges(Vertex.DIRECTION.OUT, "EdgType1")).isEqualTo(1);

        // Counting outgoing "EdgType1" edges
        Iterable<Vertex> vertices = rootVtx.getVertices(Vertex.DIRECTION.OUT, "EdgType1");

        assertThat(vertices).hasSize(1);
      }
    });
  }
}
