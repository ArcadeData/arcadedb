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
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
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
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for GHSA-c287-v325-j5jx: the Gremlin wire protocol must enforce authorization
 * (canAccessToDatabase) - a valid credential with no grant on a database must not be able to read,
 * write, or drop it via a traversal-source alias.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinServerAuthorizationIT extends AbstractGremlinServerIT {

  private static final String LIMITED_USER      = "limitedGremlinUser";
  private static final String LIMITED_PASSWORD  = "limitedPassword1";
  private static final String READONLY_USER     = "readonlyGremlinUser";
  private static final String READONLY_PASSWORD = "readonlyPassword1";

  @Test
  void zeroGrantUserCannotReadDatabase() {
    seedVictimData();
    createZeroGrantUser();

    final Cluster cluster = createCluster(LIMITED_USER, LIMITED_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      final Throwable thrown = catchThrowable(() -> g.V().hasLabel("Victim").values("secret").toList());
      assertThat(thrown)
          .as("A zero-grant credential must not be able to read the '%s' database over Gremlin", getDatabaseName())
          .isNotNull();
    } finally {
      cluster.close();
    }
  }

  @Test
  void zeroGrantUserCannotWriteDatabase() {
    seedVictimData();
    createZeroGrantUser();

    final Cluster cluster = createCluster(LIMITED_USER, LIMITED_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      final Throwable thrown = catchThrowable(() -> g.addV("Victim").property("secret", "injected").iterate());
      assertThat(thrown)
          .as("A zero-grant credential must not be able to write the '%s' database over Gremlin", getDatabaseName())
          .isNotNull();
    } finally {
      cluster.close();
    }

    // The write must not have landed.
    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.query("sql", "SELECT count(*) as c FROM Victim").next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void zeroGrantUserCannotReadViaScript() {
    seedVictimData();
    createZeroGrantUser();

    final Cluster cluster = createCluster(LIMITED_USER, LIMITED_PASSWORD);
    try {
      final Client client = cluster.connect();
      // A bare script carries no traversal-source alias: 'g' is resolved from the server's global
      // bindings (the default database). This must still be rejected for a zero-grant user.
      final Throwable thrown = catchThrowable(() -> client.submit("g.V().hasLabel('Victim').values('secret')").all().get());
      assertThat(thrown)
          .as("A zero-grant credential must not be able to read via a bare Gremlin script")
          .isNotNull();
    } finally {
      cluster.close();
    }
  }

  @Test
  void readOnlyUserCanReadButCannotWrite() {
    seedVictimData();

    final ServerSecurity security = getServer(0).getSecurity();
    // Group that grants read but not create/update/delete on every type of the test database.
    security.saveGroup(getDatabaseName(), "gremlinReadOnly", new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.createUser(new JSONObject()
        .put("name", READONLY_USER)
        .put("password", security.encodePassword(READONLY_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("gremlinReadOnly"))));

    final Cluster cluster = createCluster(READONLY_USER, READONLY_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      // Read is permitted (the user is authorized for the database and has readRecord).
      assertThat(g.V().hasLabel("Victim").count().next()).isEqualTo(1L);

      // Write must be denied by the engine's per-type ACL now that the principal is bound.
      final Throwable thrown = catchThrowable(() -> g.addV("Victim").property("secret", "injected").iterate());
      assertThat(thrown)
          .as("A read-only user must not be able to write over Gremlin")
          .isNotNull();
    } finally {
      cluster.close();
    }

    // The write must not have landed.
    final Database db = getServerDatabase(0, getDatabaseName());
    assertThat(db.query("sql", "SELECT count(*) as c FROM Victim").next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void zeroGrantUserCannotReadViaSessionScript() {
    seedVictimData();
    createZeroGrantUser();

    final Cluster cluster = createCluster(LIMITED_USER, LIMITED_PASSWORD);
    try {
      // A session-bound client (cluster.connect(sessionId)) runs scripts on a per-session executor.
      final Client client = cluster.connect("gremlin-authz-session");
      final Throwable thrown = catchThrowable(() -> client.submit("g.V().hasLabel('Victim').values('secret')").all().get());
      assertThat(thrown)
          .as("A zero-grant credential must not be able to read via a session-bound Gremlin script")
          .isNotNull();
    } finally {
      cluster.close();
    }
  }

  @Test
  void authorizedUserStillWorks() {
    seedVictimData();

    // root has access to every database, so it must still be able to read.
    final Cluster cluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      assertThat(g.V().hasLabel("Victim").count().next()).isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  private void seedVictimData() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", "CREATE VERTEX TYPE Victim IF NOT EXISTS;\nINSERT INTO Victim SET secret = 'topsecret';");
  }

  private void createZeroGrantUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(LIMITED_USER) == null)
      security.createUser(new JSONObject()
          .put("name", LIMITED_USER)
          .put("password", security.encodePassword(LIMITED_PASSWORD))
          .put("databases", new JSONObject())); // no database grants at all
  }

  private Cluster createCluster(final String user, final String password) {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(8182)
        .credentials(user, password).serializer(serializer).create();
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security != null) {
        if (security.getUser(LIMITED_USER) != null)
          security.dropUser(LIMITED_USER);
        if (security.getUser(READONLY_USER) != null)
          security.dropUser(READONLY_USER);
      }
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    super.endTest();
  }
}
