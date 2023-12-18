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
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RemoteGremlinFactoryIT extends AbstractGremlinServerIT {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void okPoolRelease() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
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
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
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
  public void executeTraversalSeparateTransactions() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ArcadeGraph graph = pool.get()) {
        for (int i = 0; i < 1_000; i++)
          graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "inputstructure", "json", "{\"name\": \"Elon\"}");

        // THIS IS IN THE SAME SCOPE, SO IT CAN SEE THE PENDING VERTICES ADDED EARLIER
        try (final ResultSet list = graph.gremlin("g.V().hasLabel(\"inputstructure\").count()").execute()) {
          Assertions.assertEquals(1_000, (Integer) list.nextIfAvailable().getProperty("result"));
        }

        graph.tx().commit(); // <-- WITHOUT THIS COMMIT THE NEXT 2 TRAVERSALS WOULD NOT SEE THE ADDED VERTICES

        Assertions.assertEquals(1_000, graph.traversal().V().hasLabel("inputstructure").count().next());
        Assertions.assertEquals(1_000, graph.traversal().V().hasLabel("inputstructure").count().toList().get(0));
      }
    }
  }

  @Test
  public void executeTraversalTxMgmt() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ArcadeGraph graph = pool.get()) {
        for (int i = 0; i < 1000; i++) {
          GraphTraversalSource g = graph.tx().begin();
          g.addV("Country").property("id", i).property("country", "USA").property("code", i).iterate();
          g.tx().commit();
        }

        Assertions.assertEquals(1000, graph.traversal().V().hasLabel("Country").count().toList().get(0));
      }
    }
  }

  @Test
  public void executeTraversalNoTxMgmt() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ArcadeGraph graph = pool.get()) {
        for (int i = 0; i < 1000; i++)
          graph.traversal().addV("Country").property("id", i).property("country", "USA").property("code", i).iterate();
      }

      try (final ArcadeGraph graph = pool.get()) {
        Assertions.assertEquals(1000, graph.traversal().V().hasLabel("Country").count().toList().get(0));
      }
    }
  }
}
