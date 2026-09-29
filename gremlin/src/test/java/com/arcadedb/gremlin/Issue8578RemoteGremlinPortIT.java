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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.gremlin.AbstractGremlinServerIT;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8578: the remote {@link ArcadeGraph} hardcoded the Gremlin Server port 8182, so a server whose Gremlin plugin
 * listens elsewhere was unreachable from it (and two parallel builds of the test suite fought over 8182). The server now
 * advertises the port and the client uses it, unless the client setting {@code arcadedb.gremlin.client.port} names one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8578RemoteGremlinPortIT extends AbstractGremlinServerIT {

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void serverAdvertisesTheBoundGremlinPort() {
    assertThat(getGremlinPort()).isNotEqualTo(8182).isPositive();

    try (final RemoteDatabase database = remoteDatabase(new ContextConfiguration())) {
      assertThat(database.getAdvertisedPort("gremlin")).isEqualTo(getGremlinPort());
      assertThat(database.getAdvertisedPort("nothing")).isZero();
    }
  }

  @Test
  void remoteGraphConnectsToTheAdvertisedPort() {
    try (final RemoteDatabase database = remoteDatabase(new ContextConfiguration());
        final ArcadeGraph graph = ArcadeGraph.open(database)) {
      assertThat(graph.traversal().V().count().next()).isEqualTo(0L);

      final Cluster cluster = graph.getCluster();
      assertThat(cluster).as("the traversal must go through the Gremlin driver, not the embedded fallback").isNotNull();
      assertThat(cluster.getPort()).isEqualTo(getGremlinPort());
    }
  }

  @Test
  void clientSettingWinsOverTheAdvertisedPort() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.GREMLIN_CLIENT_PORT, 45678);

    try (final RemoteDatabase database = remoteDatabase(configuration);
        final ArcadeGraph graph = ArcadeGraph.open(database)) {
      // ONLY THE CLUSTER IS BUILT: NOTHING LISTENS ON THE CONFIGURED PORT AND NOTHING IS SENT TO IT
      graph.traversal();
      final Cluster cluster = graph.getCluster();
      assertThat(cluster).isNotNull();
      assertThat(cluster.getPort()).isEqualTo(45678);
    }
  }

  private RemoteDatabase remoteDatabase(final ContextConfiguration configuration) {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, configuration);
  }
}
