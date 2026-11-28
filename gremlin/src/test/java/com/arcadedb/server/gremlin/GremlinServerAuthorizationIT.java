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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
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
  private static final String OTHER_USER        = "otherDatabaseGremlinUser";
  private static final String OTHER_PASSWORD    = "otherDatabasePassword1";
  private static final String OTHER_DATABASE    = "gremlinOtherDatabase";
  // THE ANSWER OF THE REQUEST CHECK, AS OPPOSED TO AN ERROR RAISED WHILE THE SCRIPT RUNS
  private static final String AUTHORIZATION_REFUSAL = "is not authorized to access database";

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
  void zeroGrantUserCannotReadViaGremlinLangScriptWithoutAlias() {
    seedVictimData();
    createZeroGrantUser();

    final Cluster cluster = createCluster(LIMITED_USER, LIMITED_PASSWORD);
    try {
      final Client client = cluster.connect();
      // gremlin-lang is the one script language open to every user. With no alias the script runs on 'g', the default
      // database: the request is refused for a user with no grant on it, before the script is evaluated.
      final Throwable thrown = catchThrowable(
          () -> client.submit("g.V().hasLabel('Victim').values('secret')", gremlinLang()).all().get());
      assertThat(thrown).as("A zero-grant credential must be refused by the request check")
          .hasStackTraceContaining(AUTHORIZATION_REFUSAL);
    } finally {
      cluster.close();
    }
  }

  @Test
  void indexedLookupsOfAZeroGrantUserAreRefusedByTheRequestCheck() {
    seedVictimData();
    createVictimIndex();
    createZeroGrantUser();

    final Cluster cluster = createCluster(LIMITED_USER, LIMITED_PASSWORD);
    try {
      final Client client = cluster.connect();
      final Throwable first = catchThrowable(
          () -> client.submit("g.V().has('Victim','secret','topsecret').count()", gremlinLang()).all().get());
      final Throwable second = catchThrowable(
          () -> client.submit("g.V().has('Victim','secret','othervalue').count()", gremlinLang()).all().get());

      assertThat(first).as("First lookup").hasStackTraceContaining(AUTHORIZATION_REFUSAL);
      assertThat(second).as("Second lookup").hasStackTraceContaining(AUTHORIZATION_REFUSAL);
    } finally {
      cluster.close();
    }
  }

  @Test
  void userGrantedAnotherDatabaseReachesItOnlyThroughAnAlias() throws Exception {
    seedVictimData();
    createOtherDatabaseUser();

    final Cluster cluster = createCluster(OTHER_USER, OTHER_PASSWORD);
    try {
      final Client client = cluster.connect();

      // No alias: 'g' is the default database, which this user has no grant on
      assertThat(catchThrowable(() -> client.submit("g.V().hasLabel('Victim').count()", gremlinLang()).all().get()))
          .as("A script with no alias runs on the default database").hasStackTraceContaining(AUTHORIZATION_REFUSAL);

      // An alias naming the default database is refused too
      assertThat(catchThrowable(() -> client.alias(getDatabaseName()).submit("g.V().hasLabel('Victim').count()", gremlinLang()).all().get()))
          .as("An alias naming the default database").hasStackTraceContaining(AUTHORIZATION_REFUSAL);

      // A traversal bound to the granted database keeps working
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, OTHER_DATABASE));
      assertThat(g.V().hasLabel("Other").count().next()).as("A traversal bound to the granted database").isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  @Test
  void authorizedUserStillReadsViaGremlinLangScript() throws Exception {
    seedVictimData();

    final Cluster cluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final Client client = cluster.connect();
      assertThat(client.submit("g.V().hasLabel('Victim').count()", gremlinLang()).all().get().get(0).getLong()).isEqualTo(1L);
      // An explicit alias of the default traversal source keeps working
      assertThat(client.alias("g").submit("g.V().hasLabel('Victim').count()", gremlinLang()).all().get().get(0).getLong()).isEqualTo(1L);
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

  private void createVictimIndex() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", "CREATE PROPERTY Victim.secret IF NOT EXISTS STRING;\nCREATE INDEX IF NOT EXISTS ON Victim (secret) NOTUNIQUE;");
  }

  private static RequestOptions gremlinLang() {
    return RequestOptions.build().language("gremlin-lang").create();
  }

  /** A user whose only grant is on a database other than the default one, with read access to every type of it. */
  private void createOtherDatabaseUser() {
    final Database other = getServer(0).getOrCreateDatabase(OTHER_DATABASE);
    other.command("sqlscript", "CREATE VERTEX TYPE Other IF NOT EXISTS;\nINSERT INTO Other SET name = 'other';");

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(OTHER_DATABASE, "gremlinOtherReaders", new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    if (security.getUser(OTHER_USER) == null)
      security.createUser(new JSONObject()
          .put("name", OTHER_USER)
          .put("password", security.encodePassword(OTHER_PASSWORD))
          .put("databases", new JSONObject().put(OTHER_DATABASE, new JSONArray().put("gremlinOtherReaders"))));
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

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
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
        if (security.getUser(OTHER_USER) != null)
          security.dropUser(OTHER_USER);
      }
      if (getServer(0).existsDatabase(OTHER_DATABASE)) {
        ((DatabaseInternal) getServer(0).getDatabaseForOperator(OTHER_DATABASE)).getEmbedded().drop();
        getServer(0).removeDatabase(OTHER_DATABASE);
      }
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    super.endTest();
  }
}
