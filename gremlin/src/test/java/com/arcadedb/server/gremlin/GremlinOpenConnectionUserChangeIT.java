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

import com.arcadedb.database.Database;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A Gremlin connection authenticates once and lives on, so what an administrator does to its user afterwards has to reach the
 * next request on that same connection: a user that was deleted, lost the database grant or got a new password must not keep
 * the access the connection had when it logged in. A change that leaves the user as it was must not end the connection.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinOpenConnectionUserChangeIT extends AbstractGremlinServerIT {
  private static final String USER         = "gremlinChangingUser";
  private static final String PASSWORD     = "changingPassword1";
  private static final String NEW_PASSWORD = "changingPassword2";
  private static final String TYPE         = "ChangingDoc";

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security != null && security.getUser(USER) != null)
        security.dropUser(USER);
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    super.endTest();
  }

  @Test
  void changedPasswordIsRefusedOnTheOpenConnection() {
    openConnectionThenChange(security -> security.updateUser(
        new JSONObject().put("name", USER).put("password", security.encodePassword(NEW_PASSWORD)).put("databases", grants())));

    // the new password works on a new connection
    final Cluster cluster = createCluster(USER, NEW_PASSWORD);
    try {
      assertThat(source(cluster).V().hasLabel(TYPE).count().next()).isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  @Test
  void passwordSetThroughTheUserApiIsRefusedOnTheOpenConnection() {
    openConnectionThenChange(security -> security.setUserPassword(USER, NEW_PASSWORD));
  }

  @Test
  void deletedUserIsRefusedOnTheOpenConnection() {
    openConnectionThenChange(security -> security.dropUser(USER));
  }

  @Test
  void removedDatabaseGrantIsRefusedOnTheOpenConnection() {
    // the stored password is kept, so only the grants change
    openConnectionThenChange(security -> security.updateUser(
        new JSONObject().put("name", USER).put("password", security.getUser(USER).getPassword()).put("databases", new JSONObject())));
  }

  @Test
  void updateThatKeepsPasswordAndGrantKeepsTheOpenConnection() {
    seedData();
    createUser();

    final Cluster cluster = createCluster(USER, PASSWORD);
    try {
      final GraphTraversalSource g = source(cluster);
      assertThat(g.V().hasLabel(TYPE).count().next()).isEqualTo(1L);

      final ServerSecurity security = getServer(0).getSecurity();
      security.updateUser(
          new JSONObject().put("name", USER).put("password", security.getUser(USER).getPassword()).put("databases", grants()));

      assertThat(g.V().hasLabel(TYPE).count().next()).isEqualTo(1L);
      g.addV(TYPE).property("name", "after").iterate();
      assertThat(countDocuments()).isEqualTo(2L);
    } finally {
      cluster.close();
    }
  }

  private void openConnectionThenChange(final Consumer<ServerSecurity> change) {
    seedData();
    createUser();

    final Cluster cluster = createCluster(USER, PASSWORD);
    try {
      final GraphTraversalSource g = source(cluster);
      // the first request opens the connection and authenticates it
      assertThat(g.V().hasLabel(TYPE).count().next()).isEqualTo(1L);

      change.accept(getServer(0).getSecurity());

      assertThat(catchThrowable(() -> g.V().hasLabel(TYPE).count().next())).as("a read on the open connection must be refused")
          .isNotNull();
      assertThat(catchThrowable(() -> g.addV(TYPE).property("name", "after").iterate())).as(
          "a write on the open connection must be refused").isNotNull();
    } finally {
      cluster.close();
    }

    assertThat(countDocuments()).isEqualTo(1L);
  }

  private GraphTraversalSource source(final Cluster cluster) {
    return AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
  }

  private void seedData() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", "CREATE VERTEX TYPE " + TYPE + " IF NOT EXISTS;\nINSERT INTO " + TYPE + " SET name = 'before';");
  }

  private long countDocuments() {
    return getServerDatabase(0, getDatabaseName()).countType(TYPE, false);
  }

  private JSONObject grants() {
    return new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"));
  }

  private void createUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(
        new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD)).put("databases", grants()));
  }

  /**
   * One connection, so the request after the change travels on the connection that authenticated before it: with a pool of
   * several, a later request could open a new connection and authenticate it again, which proves nothing about the first.
   */
  private Cluster createCluster(final String user, final String password) {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort()).credentials(user, password)
        .serializer(serializer).minConnectionPoolSize(1).maxConnectionPoolSize(1).create();
  }
}
