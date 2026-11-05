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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DatabaseIdleCloser;
import com.arcadedb.server.ServerControlPlane;
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

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11068 over Gremlin: a database the server closed for idleness is served to the next traversal, which opens it again,
 * where the graph manager used to answer as if it did not exist. The traversal that opens it runs with its user's principal
 * bound on it: the pool binds the principal on the databases open when the request starts, so one opened by the request itself
 * would otherwise run with every per-type check skipped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11068GremlinIdleCloseIT extends AbstractGremlinServerIT {
  private static final String READONLY_USER     = "idleGremlinReader";
  private static final String READONLY_PASSWORD = "idleGremlinReader1";

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Driven by the test: the scheduled check waits an hour
    config.setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_CHECK_INTERVAL, TimeUnit.HOURS.toMillis(1));
  }

  @Test
  void aTraversalReopensADatabaseClosedForIdleness() throws Exception {
    seed();
    assertThat(closeIdle()).isEqualTo(1);

    final Cluster cluster = cluster("root", DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
      assertThat(g.V().hasLabel("IdleGremlin").count().next()).isEqualTo(1L);
      assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
    } finally {
      cluster.close();
    }
  }

  @Test
  void theTraversalThatReopensTheDatabaseRunsWithItsUsersGrants() throws Exception {
    seed();
    createReadOnlyUser();
    assertThat(closeIdle()).isEqualTo(1);

    final Cluster cluster = cluster(READONLY_USER, READONLY_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
      // The first request after the close opens the database: it must not be the one request that runs unchecked
      final Throwable thrown = catchThrowable(() -> g.addV("IdleGremlin").property("name", "injected").iterate());
      assertThat(thrown).as("a read-only user must not write through the request that reopens the database")
          .hasStackTraceContaining("not allowed to create records");
      assertThat(g.V().hasLabel("IdleGremlin").count().next()).isEqualTo(1L);
    } finally {
      cluster.close();
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.query("sql", "SELECT count(*) AS c FROM IdleGremlin").next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void theScriptThatReopensTheDatabaseRunsWithItsUsersGrants() throws Exception {
    seed();
    createReadOnlyUser();
    assertThat(closeIdle()).isEqualTo(1);

    final Cluster cluster = cluster(READONLY_USER, READONLY_PASSWORD);
    try {
      // A bare script resolves 'g' while it runs, on the pool thread, after the principal was bound on the open databases
      final Client client = cluster.connect();
      // gremlin-lang: a Groovy script is refused to a non-administrator before it reaches any database
      final Throwable thrown = catchThrowable(
          () -> client.submit("g.addV('IdleGremlin').property('name', 'injected')",
              RequestOptions.build().language("gremlin-lang").create()).all().get());
      assertThat(thrown).as("a read-only user must not write through the script that reopens the database")
          .hasStackTraceContaining("not allowed to create records");
    } finally {
      cluster.close();
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.query("sql", "SELECT count(*) AS c FROM IdleGremlin").next().<Long>getProperty("c")).isEqualTo(1L);
  }

  @Test
  void aScriptReopensADatabaseClosedForIdleness() throws Exception {
    seed();
    // 'g' bound once: the executor's copy of the bindings now holds a traversal source over this instance
    assertThat(scriptCount()).isEqualTo(1L);
    assertThat(closeIdle()).isEqualTo(1);

    // That copy is read ahead of the live bindings: a traversal source left in it answered with the closed instance's error
    assertThat(scriptCount()).isEqualTo(1L);
    assertThat(getServer(0).existsDatabase(getDatabaseName())).isTrue();
  }

  @Test
  void aScriptAfterACloseAndOpenDatabaseRunsOnTheReopenedInstance() throws Exception {
    // The same stale binding, through the operator's commands rather than the idle close
    seed();
    assertThat(scriptCount()).isEqualTo(1L);
    final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));
    controlPlane.closeDatabase(getDatabaseName());
    controlPlane.openDatabase(getDatabaseName());
    assertThat(scriptCount()).isEqualTo(1L);
  }

  private long scriptCount() throws Exception {
    final Cluster cluster = cluster("root", DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final Client client = cluster.connect();
      return client.submit("g.V().hasLabel('IdleGremlin').count()", RequestOptions.build().language("gremlin-lang").create())
          .all().get().get(0).getLong();
    } finally {
      cluster.close();
    }
  }

  private void createReadOnlyUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), "idleGremlinReadOnly", new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.createUser(new JSONObject()
        .put("name", READONLY_USER)
        .put("password", security.encodePassword(READONLY_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("idleGremlinReadOnly"))));
  }

  private void seed() {
    getServerDatabase(0, getDatabaseName()).command("sqlscript",
        "CREATE VERTEX TYPE IdleGremlin IF NOT EXISTS;\nINSERT INTO IdleGremlin SET name = 'kept';");
  }

  /**
   * Checks a little apart, until one closes something: the first samples the database, a later one finds it unused past a 1 ms
   * timeout. Several, so that background work landing between two of them delays the close instead of failing the test.
   */
  private int closeIdle() throws InterruptedException {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 1L);
    final DatabaseIdleCloser closer = getServer(0).getDatabaseIdleCloser();
    int closed = closer.check();
    for (int attempt = 0; attempt < 10 && closed == 0; attempt++) {
      Thread.sleep(20);
      closed += closer.check();
    }
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 0L);
    return closed;
  }

  private Cluster cluster(final String user, final String password) {
    final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(
        new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));
    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
        .credentials(user, password).serializer(serializer).create();
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      final ArcadeDBServer server = getServer(0);
      if (server != null && server.isStarted()) {
        server.getConfiguration().setValue(GlobalConfiguration.SERVER_DATABASE_IDLE_TIMEOUT, 0L);
        server.getDatabaseForOperator(getDatabaseName());
        final ServerSecurity security = server.getSecurity();
        if (security != null && security.getUser(READONLY_USER) != null)
          security.dropUser(READONLY_USER);
      }
    } finally {
      super.endTest();
    }
  }
}
