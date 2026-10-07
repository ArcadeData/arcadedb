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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9317, end to end: in production mode a duplicated key raised through the Gremlin Server's own port reaches the client
 * as the shared placeholder, never with the stored key values, and the pipeline wiring of the concealing channelizer works on a
 * real connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9317GremlinProductionErrorConcealmentIT extends AbstractGremlinServerIT {
  private static final String SECRET = "secret-key-9317";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_MODE, "production");
  }

  @Test
  void aDuplicatedKeyIsConcealedOnTheWire() throws Exception {
    final Database database = getServer(0).getDatabase(getDatabaseName());
    database.command("sql", "CREATE VERTEX TYPE Dup9317");
    database.command("sql", "CREATE PROPERTY Dup9317.k STRING");
    database.command("sql", "CREATE INDEX ON Dup9317 (k) UNIQUE");

    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));
    final Cluster cluster = Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer).create();
    try {
      final Client client = cluster.connect().alias(getDatabaseName());
      final String insert = "g.traversal().addV('Dup9317').property('k', '" + SECRET + "').count()";
      client.submit(insert).all().get();

      final Throwable failure = catchThrowable(() -> client.submit(insert).all().get());

      assertThat(failure).isNotNull();
      assertThat(failure.toString()).doesNotContain(SECRET).contains(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
      final Throwable cause = failure.getCause();
      if (cause instanceof ResponseException responseException)
        assertThat(responseException.getRemoteStackTrace()).as("no stack trace attribute").isEmpty();
    } finally {
      cluster.close();
    }
  }
}
