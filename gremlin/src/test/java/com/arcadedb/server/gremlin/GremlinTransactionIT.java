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
package com.arcadedb.server.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.test.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Gremlin transactions via remote driver connection.
 * <p>
 * This test verifies that transactions work correctly when using the Gremlin
 * driver to connect to ArcadeDB's Gremlin Server.
 * <p>
 * Issue: https://github.com/ArcadeData/arcadedb/issues/1446
 */
class GremlinTransactionIT extends AbstractGremlinServerIT {

  @Test
  void transactionCommit() throws Exception {
    final Cluster cluster = createCluster();
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      // Start a transaction
      final Transaction tx = g.tx();
      final GraphTraversalSource gtx = tx.begin();

      try {
        // Create a vertex within the transaction
        gtx.addV("TestVertex").property("name", "test1").next();

        // Commit the transaction
        tx.commit();

        // Verify the vertex was created
        final long count = g.V().hasLabel("TestVertex").has("name", "test1").count().next();
        assertThat(count).isEqualTo(1L);
      } catch (final Exception e) {
        tx.rollback();
        throw e;
      } finally {
        g.close();
      }
    } finally {
      cluster.close();
    }
  }

  @Test
  void transactionRollback() throws Exception {
    final Cluster cluster = createCluster();
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      // Start a transaction
      final Transaction tx = g.tx();
      final GraphTraversalSource gtx = tx.begin();

      try {
        // Create a vertex within the transaction
        gtx.addV("TestVertex").property("name", "rollbackTest").next();

        // Rollback the transaction
        tx.rollback();

        // Verify the vertex was NOT created
        final long count = g.V().hasLabel("TestVertex").has("name", "rollbackTest").count().next();
        assertThat(count).isEqualTo(0L);
      } finally {
        g.close();
      }
    } finally {
      cluster.close();
    }
  }

  @Test
  void multipleOperationsInTransaction() throws Exception {
    final Cluster cluster = createCluster();
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      // Start a transaction
      final Transaction tx = g.tx();
      final GraphTraversalSource gtx = tx.begin();

      try {
        // Create multiple vertices within the same transaction
        gtx.addV("MultiOpVertex").property("name", "Alice").next();
        gtx.addV("MultiOpVertex").property("name", "Bob").next();

        // Commit the transaction
        tx.commit();

        // Verify all data was created
        final long vertexCount = g.V().hasLabel("MultiOpVertex").count().next();
        assertThat(vertexCount).isEqualTo(2L);
      } catch (final Exception e) {
        tx.rollback();
        throw e;
      } finally {
        g.close();
      }
    } finally {
      cluster.close();
    }
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Cluster createCluster() {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(8182)
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer).create();
  }
}
