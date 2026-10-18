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
import com.arcadedb.database.Database;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.server.AdmissionSlotConformance;
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

/**
 * Issue #10235 over Gremlin Server: a traversal or a script that gives its admission slot back before a wait finds the slot
 * on the thread that evaluates it. See {@link AdmissionSlotConformance}.
 */
class AdmissionSlotConformanceGremlinTest extends AbstractGremlinServerIT {
  private static final String TYPE = "Conformance10235";

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aTraversalAndAScriptGiveTheirSlotBackFromTheThreadThatEvaluatesThem() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().getOrCreateVertexType(TYPE);
    final long before = database.countType(TYPE, false);

    final Cluster cluster = Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)
        .serializer(new GraphBinaryMessageSerializerV1(new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()))).create();
    try (final GraphTraversalSource g = AnonymousTraversalSource.traversal()
        .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()))) {
      AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "Gremlin traversal",
          () -> g.addV(TYPE).property("id", 1).iterate());

      final Client client = cluster.connect();
      AdmissionSlotConformance.assertGivesItsSlotBack(database, TYPE, "Gremlin script",
          () -> client.submit("g.addV('" + TYPE + "').property('id', 2)").all().get());
    } finally {
      cluster.close();
    }
    assertThat(database.countType(TYPE, false)).isEqualTo(before + 2);
  }
}
