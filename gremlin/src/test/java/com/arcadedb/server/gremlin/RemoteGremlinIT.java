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

import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.server.BaseGraphServerTest;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.assertj.core.api.Assertions.assertThat;

public class RemoteGremlinIT extends AbstractGremlinServerIT {

  @Test
  public void insert() throws Exception {
    testEachServer((serverIndex) -> {
      assertThat(
          new RemoteServer("127.0.0.1", 2480 + serverIndex, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).exists(
              getDatabaseName())).isTrue();

      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      try (final ArcadeGraph graph = ArcadeGraph.open(database)) {
        graph.getDatabase().getSchema().createVertexType("inputstructure");

        for (int i = 0; i < 1_000; i++)
          graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "inputstructure", "json", "{\"name\": \"John\"}");

        try (final ResultSet list = graph.gremlin("g.V().hasLabel(\"inputstructure\")").execute()) {
          assertThat(list.stream().count()).isEqualTo(1_000);
        }

        try (final ResultSet list = graph.gremlin("g.V().hasLabel(\"inputstructure\").count()").execute()) {
          assertThat(list.nextIfAvailable().<Integer>getProperty("result")).isEqualTo(1_000);
        }

        graph.tx().commit();

        assertThat(graph.traversal().V().hasLabel("inputstructure").count().next()).isEqualTo(1_000L);

      }
    });
  }

  @Test
  public void dropVertex() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      try (final ArcadeGraph graph = ArcadeGraph.open(database)) {

        graph.traversal().addV("RG").//
            property("name", "r1").//
            addV("RG").//
            property("name", "r2").as("member2").//
            V().//
            has("name", "r1").//
            addE(":TEST").to("member2").//
            addV("RG").//
            property("name", "r3").as("member3").//
            V().//
            has("name", "r1").//
            addE(":TEST").to("member3").//
            addV("RG").//
            property("name", "r4").as("member4").//
            V().//
            has("name", "r2").//
            addE(":TEST").to("member4").//
            addV("RG").//
            property("name", "r5").as("member5").//
            V().//
            has("name", "r3").//
            addE(":TEST").to("member5").//
            V().//
            has("name", "r4").//
            addE(":TEST").to("member5").//
            addV("P").//
            property("name", "p1").as("member6").//
            V().//
            has("name", "r5").//
            addE(":TEST").from("member6").next();

        graph.traversal().V().//
            has("RG", "name", "r4").//
            out().//
            has("RG", "name", "r5").//
            as("deleteEntry").//
            select("deleteEntry").///
            sideEffect(in().hasLabel("P").drop()).//
            sideEffect(select("deleteEntry").drop()).//
            constant("deleted").next();
      }
    });
  }
}
