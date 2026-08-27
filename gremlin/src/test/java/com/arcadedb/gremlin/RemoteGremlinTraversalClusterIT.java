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
package com.arcadedb.gremlin;

import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.gremlin.AbstractGremlinServerIT;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A remote {@link ArcadeGraph#traversal()} goes through a TinkerPop driver {@link Cluster}, which owns a Netty
 * event-loop group, a scheduled executor and a connection pool. Issue #6822: nothing ever closed it, so every graph
 * instance on which {@code traversal()} had been called leaked those for the life of the JVM.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RemoteGremlinTraversalClusterIT extends AbstractGremlinServerIT {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void closingTheGraphClosesTheDriverCluster() {
    final ArcadeGraph graph = ArcadeGraph.open(remoteDatabase());

    assertThat(graph.traversal().V().count().next()).isEqualTo(0L);

    final Cluster cluster = graph.getCluster();
    assertThat(cluster).as("a remote traversal must go through the Gremlin driver").isNotNull();
    assertThat(cluster.isClosed()).isFalse();

    graph.close();

    assertThat(cluster.isClosed()).as("ArcadeGraph.close() must shut down the driver cluster it created").isTrue();
    assertThat(graph.getCluster()).isNull();
  }

  @Test
  void droppingTheGraphClosesTheDriverCluster() {
    final ArcadeGraph graph = ArcadeGraph.open(remoteDatabase());

    assertThat(graph.traversal().V().count().next()).isEqualTo(0L);

    final Cluster cluster = graph.getCluster();
    assertThat(cluster).isNotNull();

    graph.drop();

    assertThat(cluster.isClosed()).as("ArcadeGraph.drop() must shut down the driver cluster it created").isTrue();
  }

  @Test
  void aPooledInstanceKeepsItsClusterUntilTheFactoryIsClosed() {
    final Cluster cluster;
    try (final ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

      final ArcadeGraph borrowed = pool.get();
      assertThat(borrowed.traversal().V().count().next()).isEqualTo(0L);
      cluster = ((ArcadeGraph) borrowed).getCluster();
      assertThat(cluster).isNotNull();
      borrowed.close();

      assertThat(cluster.isClosed())
          .as("releasing an instance back to the pool must not tear down the cluster it will reuse")
          .isFalse();

      final ArcadeGraph reborrowed = pool.get();
      assertThat(reborrowed).isSameAs(borrowed);
      assertThat(reborrowed.traversal().V().count().next()).isEqualTo(0L);
      reborrowed.close();
    }

    assertThat(cluster.isClosed()).as("closing the factory must dispose the pooled instances and their clusters").isTrue();
  }

  private RemoteDatabase remoteDatabase() {
    return new RemoteDatabase("127.0.0.1", 2480, getDatabaseName(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
  }
}
