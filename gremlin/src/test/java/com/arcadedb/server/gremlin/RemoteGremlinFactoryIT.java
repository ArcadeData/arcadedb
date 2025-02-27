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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class RemoteGremlinFactoryIT extends AbstractGremlinServerIT {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void okPoolRelease() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      for (int i = 0; i < 1_000; i++) {
        final ArcadeGraph instance = pool.get();
        assertThat(instance).isNotNull();
        instance.close();
      }

      assertThat(pool.getTotalInstancesCreated()).isEqualTo(1);
    }
  }

  @Test
  public void errorPoolRelease() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      for (int i = 0; i < pool.getMaxInstances(); i++) {
        final ArcadeGraph instance = pool.get();
        assertThat(instance).isNotNull();
      }

      try {
        pool.get();
        fail("");
      } catch (IllegalArgumentException e) {
        // EXPECTED
      }

      assertThat(pool.getTotalInstancesCreated()).isEqualTo(pool.getMaxInstances());
    }
  }

  @Test
  public void executeTraversalSeparateTransactions() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {
      try (final ArcadeGraph graph = pool.get()) {
        for (int i = 0; i < 1_000; i++)
          graph.addVertex(org.apache.tinkerpop.gremlin.structure.T.label, "inputstructure", "json", "{\"name\": \"John\"}");

        // THIS IS IN THE SAME SCOPE, SO IT CAN SEE THE PENDING VERTICES ADDED EARLIER
        try (final ResultSet list = graph.gremlin("g.V().hasLabel(\"inputstructure\").count()").execute()) {
          assertThat((Integer) list.nextIfAvailable().getProperty("result")).isEqualTo(1_000);
        }

        graph.tx().commit(); // <-- WITHOUT THIS COMMIT THE NEXT 2 TRAVERSALS WOULD NOT SEE THE ADDED VERTICES

        assertThat(graph.traversal().V().hasLabel("inputstructure").count().next()).isEqualTo(1_000);
        assertThat(graph.traversal().V().hasLabel("inputstructure").count().toList().get(0)).isEqualTo(1_000);
      }
    }
  }

  @Test
  public void executeTraversalTxMgmtMultiThreads() throws InterruptedException {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {

      try (final ArcadeGraph graph = pool.get()) {
        graph.getDatabase().getSchema().createVertexType("Country");
      }

      final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

      for (int i = 0; i < 1000; i++) {
        final int id = i;
        executorService.submit(() -> {

          try (final ArcadeGraph graph = pool.get()) {

            Transaction tx = graph.tx();
            GraphTraversalSource gtx = tx.begin();
            gtx.addV("Country")
                .property("id", id)
                .property("country", "USA")
                .property("code", id)
                .iterate();
            tx.commit();

          } catch (Exception e) {
            //do nothing
          }
        });
      }

      executorService.shutdown();
      executorService.awaitTermination(60, TimeUnit.SECONDS);

      try (final ArcadeGraph graph = pool.get()) {
        Long country = graph.traversal().V().hasLabel("Country").count().toList().get(0);
        assertThat(country).isGreaterThan(800);
      }
    } catch (Exception e) {
      //do nothing
    }
  }

  @Test
  public void executeTraversalNoTxMgmtMultiThreads() throws InterruptedException {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {

      try (final ArcadeGraph graph = pool.get()) {
        graph.getDatabase().getSchema().createVertexType("Country");
      }

      final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
      for (int i = 0; i < 1000; i++) {
        final int id = i;
        executorService.submit(() -> {
          try (final ArcadeGraph graph = pool.get()) {
            graph.traversal().addV("Country").property("id", id).property("country", "USA").property("code", id).iterate();
          }
        });
      }
      executorService.shutdown();
      executorService.awaitTermination(60, TimeUnit.SECONDS);

      try (final ArcadeGraph graph = pool.get()) {
        assertThat(graph.traversal().V().hasLabel("Country").count().toList().get(0)).isGreaterThan(800);
      }
    }
  }

  @Test
  public void executeTraversalTxMgmtHttp() throws InterruptedException {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {

      for (int i = 0; i < 1000; i++) {
        final int id = i;
        try (final ArcadeGraph graph = pool.get()) {
          GraphTraversalSource g = graph.tx().begin();
          g.addV("Country").property("id", id).property("country", "USA").property("code", id).iterate();
          g.tx().commit();
        }
      }

      try (final ArcadeGraph graph = pool.get()) {
        assertThat(graph.traversal().V().hasLabel("Country").count().toList().get(0)).isEqualTo(1000);
      }
    }
  }

  @Test
  public void executeTraversalTxMgmt() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {

      for (int i = 0; i < 1000; i++) {
        final int id = i;
        try (final ArcadeGraph graph = pool.get()) {
          GraphTraversalSource g = graph.traversal();
          graph.tx().begin();
          g.addV("Country").property("id", id).property("country", "USA").property("code", id).iterate();
          graph.tx().commit();
        }
      }

      try (final ArcadeGraph graph = pool.get()) {
        assertThat(graph.traversal().V().hasLabel("Country").count().toList().get(0)).isEqualTo(1000);
      }
    }
  }

  @Test
  public void executeTraversalNoTxMgmt() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS)) {

      for (int i = 0; i < 1000; i++) {
        final int id = i;
        try (final ArcadeGraph graph = pool.get()) {
          graph.traversal().addV("Country").property("id", id).property("country", "USA").property("code", id).iterate();
        }
      }

      try (final ArcadeGraph graph = pool.get()) {
        assertThat(graph.traversal().V().hasLabel("Country").count().toList().get(0)).isEqualTo(1000);
      }
    }
  }
}
