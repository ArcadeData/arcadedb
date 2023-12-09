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
import com.arcadedb.gremlin.ArcadeGraphFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

public class RemoteGremlinFactoryIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-factory";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void okPoolRelease() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      for (int i = 0; i < 1_000; i++) {
        final ArcadeGraph instance = pool.get();
        Assertions.assertNotNull(instance);
        instance.close();
      }

      Assertions.assertEquals(1, pool.getTotalInstancesCreated());
    }
  }

  @Test
  public void errorPoolRelease() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      for (int i = 0; i < pool.getMaxInstances(); i++) {
        final ArcadeGraph instance = pool.get();
        Assertions.assertNotNull(instance);
      }

      try {
        pool.get();
        Assertions.fail();
      } catch (IllegalArgumentException e) {
        // EXPECTED
      }

      Assertions.assertEquals(pool.getMaxInstances(), pool.getTotalInstancesCreated());
    }
  }

  @Test
  public void executeTraversal() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ArcadeGraph graph = pool.get()) {
        for (int i = 0; i < 1_000; i++)
          graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "inputstructure", "json", "{\"name\": \"Elon\"}");

        try (final ResultSet list = graph.gremlin("g.V().hasLabel(\"inputstructure\").count()").execute()) {
          Assertions.assertEquals(1_000, (Long) list.nextIfAvailable().getProperty("result"));
        }

        Assertions.assertEquals(1_000, graph.traversal().V().hasLabel("inputstructure").count().next());

        Assertions.assertEquals(1_000, graph.traversal().V().hasLabel("inputstructure").count().toList().get(0));
      }
    }
  }

  @Test
  public void executeTraversal2() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, DATABASE_NAME, "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ArcadeGraph arcadeGraph = pool.get()) {
        GraphTraversalSource g = arcadeGraph.traversal();
        try {
          g.tx().begin();
          g.addV("Country").property("id", 0).property("country", "USA").property("code", 11).iterate();
          g.tx().commit();
        } catch (Exception e) {
          g.tx().rollback();
        }

        Assertions.assertEquals(1, arcadeGraph.traversal().V().hasLabel("Country").count().toList().get(0));
      }
    }
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
