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
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A variable of a {@code gremlin-lang} script resolves to what the request supplied: the names of the databases the Gremlin
 * Server has registered are not variables, so a script naming one is answered exactly as one naming a variable that does not exist.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinLangScriptVariablesIT extends AbstractGremlinServerIT {
  private static final String USER         = "gremlinLangVariablesUser";
  private static final String PASSWORD     = "gremlinLangVariablesUser1";
  private static final String GROUP        = "gremlinLangVariablesReaders";
  private static final String OTHER_DB     = "otherRegisteredDb";
  private static final String REACHABLE_DB = "reachableRegisteredDb";
  private static final String MISSING      = "neverCreatedDb";
  private static final String UNBOUND      = "No variable found for ";

  @Test
  void scriptNamingARegisteredDatabaseIsAnsweredAsAnUnboundVariable() throws Exception {
    registerDatabase(OTHER_DB);
    createUser();

    final Cluster cluster = createCluster(USER, PASSWORD);
    try {
      final Client client = cluster.connect();

      final Throwable registered = catchThrowable(() -> client.submit(hasName(OTHER_DB), gremlinLang()).all().get());
      final Throwable missing = catchThrowable(() -> client.submit(hasName(MISSING), gremlinLang()).all().get());

      assertThat(registered).hasStackTraceContaining(UNBOUND + OTHER_DB);
      assertThat(missing).hasStackTraceContaining(UNBOUND + MISSING);
      assertThat(rootMessage(registered).replace(OTHER_DB, "<name>")).isEqualTo(rootMessage(missing).replace(MISSING, "<name>"));
    } finally {
      cluster.close();
    }
  }

  @Test
  void scriptNamingTheDatabaseOfAnArgumentWithATypeIsAnsweredAsAnUnboundVariable() throws Exception {
    registerDatabase(OTHER_DB);
    createUser();

    final Cluster cluster = createCluster(USER, PASSWORD);
    try {
      final Client client = cluster.connect();

      final Throwable registered = catchThrowable(() -> client.submit("g.V().hasLabel(" + OTHER_DB + ")", gremlinLang()).all().get());
      final Throwable missing = catchThrowable(() -> client.submit("g.V().hasLabel(" + MISSING + ")", gremlinLang()).all().get());

      assertThat(registered).hasStackTraceContaining(UNBOUND + OTHER_DB);
      assertThat(missing).hasStackTraceContaining(UNBOUND + MISSING);
      assertThat(rootMessage(registered).replace(OTHER_DB, "<name>")).isEqualTo(rootMessage(missing).replace(MISSING, "<name>"));
    } finally {
      cluster.close();
    }
  }

  @Test
  void scriptNamingADatabaseTheUserReachesIsAnsweredAsAnUnboundVariable() throws Exception {
    registerDatabase(REACHABLE_DB);
    createUser();

    final Cluster cluster = createCluster(USER, PASSWORD);
    try {
      final Client client = cluster.connect();

      assertThat(catchThrowable(() -> client.submit(hasName(REACHABLE_DB), gremlinLang()).all().get()))
          .hasStackTraceContaining(UNBOUND + REACHABLE_DB);
    } finally {
      cluster.close();
    }
  }

  @Test
  void scriptParametersStillResolve() throws Exception {
    registerDatabase(OTHER_DB);
    createUser();

    final Cluster cluster = createCluster(USER, PASSWORD);
    try {
      final Client client = cluster.connect();

      final List<Result> results = client.submit("g.V().has('name', x).count()",
          RequestOptions.build().language("gremlin-lang").addParameter("x", "probe").create()).all().get();
      assertThat(results).hasSize(1);
      assertThat(results.getFirst().getLong()).isEqualTo(1L);

      // A PARAMETER MAY CARRY THE NAME OF A DATABASE: THE VALUE THE REQUEST SENT IS WHAT THE SCRIPT READS
      final List<Result> named = client.submit(hasName(OTHER_DB) + ".count()",
          RequestOptions.build().language("gremlin-lang").addParameter(OTHER_DB, "probe").create()).all().get();
      assertThat(named.getFirst().getLong()).isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  @Test
  void scriptsWithoutVariablesStillRun() throws Exception {
    createUser();

    final Cluster cluster = createCluster(USER, PASSWORD);
    try {
      final Client client = cluster.connect();
      assertThat(client.submit("g.V().has('name', 'probe').count()", gremlinLang()).all().get().getFirst().getLong()).isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  private static String hasName(final String variable) {
    return "g.V().has('name', " + variable + ")";
  }

  /** The database is opened by a request of the server administrator, which registers it with the Gremlin Server. */
  private void registerDatabase(final String name) throws Exception {
    final RemoteServer remote = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (!remote.exists(name))
      remote.create(name);

    final Cluster rootCluster = createCluster("root", DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(rootCluster, name));
      assertThat(g.V().count().next()).isZero();
    } finally {
      rootCluster.close();
    }
  }

  private void createUser() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", "CREATE VERTEX TYPE Probe IF NOT EXISTS;\nINSERT INTO Probe SET name = 'probe';");

    final ServerSecurity security = getServer(0).getSecurity();
    final JSONObject group = new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))));
    security.saveGroup(getDatabaseName(), GROUP, group);

    final JSONObject databases = new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP));
    // THE USER REACHES ONE MORE DATABASE, BUT NOT OTHER_DB
    if (getServer(0).existsDatabase(REACHABLE_DB)) {
      security.saveGroup(REACHABLE_DB, GROUP, group);
      databases.put(REACHABLE_DB, new JSONArray().put(GROUP));
    }

    if (security.getUser(USER) == null)
      security.createUser(new JSONObject()
          .put("name", USER)
          .put("password", security.encodePassword(PASSWORD))
          .put("databases", databases));
  }

  private static RequestOptions gremlinLang() {
    return RequestOptions.build().language("gremlin-lang").create();
  }

  private static String rootMessage(final Throwable error) {
    Throwable cause = error;
    while (cause.getCause() != null && cause.getCause() != cause)
      cause = cause.getCause();
    return String.valueOf(cause.getMessage());
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
        if (security.getUser(USER) != null)
          security.dropUser(USER);
        security.deleteGroup(getDatabaseName(), GROUP);
        if (getServer(0).existsDatabase(REACHABLE_DB))
          security.deleteGroup(REACHABLE_DB, GROUP);
      }
      final RemoteServer remote = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      for (final String name : new String[] { OTHER_DB, REACHABLE_DB })
        if (remote.exists(name))
          remote.drop(name);
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    super.endTest();
  }
}
