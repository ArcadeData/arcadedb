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
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9518 over Gremlin Server: a traversal or a script the query admission gate does not start is answered with an
 * error before anything of it runs, and every request gives its slot back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class QueryAdmissionGateGremlinIssue9518Test extends AbstractGremlinServerIT {
  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aRequestTheGateDoesNotStartIsAnsweredWithAnErrorAndEveryRequestGivesItsSlotBack() throws Exception {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);

    final Cluster cluster = createCluster();
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
      final Client client = cluster.connect();

      final long refusedBefore = gate.getRefused();
      try (final QueryAdmissionGate.Ticket ignored = gate.admit()) {
        assertThatThrownBy(() -> g.V().count().next()).as("a traversal");
        assertThatThrownBy(() -> client.submit("g.V().count()").all().get()).as("a script");
      }
      assertThat(gate.getRefused()).as("both refused by the gate").isEqualTo(refusedBefore + 2);

      // EVERY REQUEST GIVES ITS SLOT BACK: WITH ONE SLOT AND NO WAITING, A LEAKED ONE WOULD REFUSE THE NEXT
      for (int i = 0; i < 2; i++) {
        assertThat(g.V().count().next()).isNotNull();
        assertThat(client.submit("g.V().count()").all().get()).hasSize(1);
      }
      assertThat(gate.getRunning()).isZero();
    } finally {
      cluster.close();
    }
  }

  private Cluster createCluster() {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));
    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer).create();
  }
}
