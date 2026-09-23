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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Gremlin capabilities that reach outside the database - evaluating Groovy (arbitrary JVM code, from a script, from the
 * {@code auto} fallback or from a lambda carried in bytecode) and the {@code io()} step (reads and writes host files at
 * a caller-chosen path) - are reserved to the server root user, on both the HTTP command endpoint and the Gremlin
 * Server wire protocol. A database administrator is not enough: the reach of these capabilities is the whole host, not
 * one database. Plain traversals keep working for every authorized user, including through {@code gremlin-lang} string
 * scripts on the wire.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinScriptingAuthorizationIT extends AbstractGremlinServerIT {
  private static final String GROOVY_SCRIPT   = "[1, 2, 3].collect { it * 2 }";
  private static final String REFUSAL         = "reserved to the server administrator";
  private static final String READER_USER     = "gremlinScriptReader";
  private static final String READER_PASSWORD = "gremlinScriptReader1";
  private static final String DBADMIN_USER    = "gremlinScriptDbAdmin";
  private static final String DBADMIN_PASSWORD = "gremlinScriptDbAdmin1";

  private final File ioTarget = new File("./target/gremlin-io-authz-probe.xml");

  @Test
  void httpGroovyEngineIsRootOnly() throws Exception {
    createUsers();
    setGremlinEngine("groovy");

    assertRefused(READER_USER, READER_PASSWORD, GROOVY_SCRIPT);
    assertRefused(DBADMIN_USER, DBADMIN_PASSWORD, GROOVY_SCRIPT);
    assertThat(executeHttp("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, GROOVY_SCRIPT)).isEqualTo(200);

    // A plain traversal evaluated by the Groovy engine is still Groovy: the gate is on the engine, not on the text.
    assertRefused(READER_USER, READER_PASSWORD, "g.V().count()");
  }

  @Test
  void httpAutoEngineFallbackIsRootOnly() throws Exception {
    createUsers();
    setGremlinEngine("auto");

    assertRefused(DBADMIN_USER, DBADMIN_PASSWORD, GROOVY_SCRIPT);
    assertThat(executeHttp("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, GROOVY_SCRIPT)).isEqualTo(200);

    // What the secure gremlin-lang parser accepts never reaches Groovy, so it stays open to a reader.
    assertThat(executeHttp(READER_USER, READER_PASSWORD, "g.V().count()")).isEqualTo(200);
  }

  @Test
  void httpJavaEngineStillServesReaders() throws Exception {
    createUsers();
    setGremlinEngine("java");

    assertThat(executeHttp(READER_USER, READER_PASSWORD, "g.V().count()")).isEqualTo(200);
  }

  @Test
  void httpIoStepIsRootOnly() throws Exception {
    createUsers();
    setGremlinEngine("java");

    final String writeGraph = "g.io('" + ioTarget.getAbsolutePath().replace('\\', '/') + "').write()";

    assertRefused(DBADMIN_USER, DBADMIN_PASSWORD, writeGraph);
    assertThat(ioTarget).doesNotExist();

    assertThat(executeHttp("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS, writeGraph)).isEqualTo(200);
    assertThat(ioTarget).exists();
  }

  @Test
  void wireGroovyScriptIsRootOnly() throws Exception {
    createUsers();

    final Cluster readerCluster = createCluster(READER_USER, READER_PASSWORD);
    try {
      final Client client = readerCluster.connect().alias(getDatabaseName());
      assertThat(catchThrowable(() -> client.submit(GROOVY_SCRIPT).all().get()))
          .as("A non-root user must not evaluate a Groovy script over the Gremlin wire protocol").hasStackTraceContaining(REFUSAL);
    } finally {
      readerCluster.close();
    }

    final Cluster dbAdminCluster = createCluster(DBADMIN_USER, DBADMIN_PASSWORD);
    try {
      final Client client = dbAdminCluster.connect().alias(getDatabaseName());
      assertThat(catchThrowable(() -> client.submit(GROOVY_SCRIPT).all().get()))
          .as("A database administrator must not evaluate a Groovy script over the Gremlin wire protocol").hasStackTraceContaining(REFUSAL);
    } finally {
      dbAdminCluster.close();
    }

    final Cluster rootCluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final List<Result> results = rootCluster.connect().alias(getDatabaseName()).submit(GROOVY_SCRIPT).all().get();
      assertThat(results.stream().map(Result::getInt).toList()).containsExactly(2, 4, 6);
    } finally {
      rootCluster.close();
    }
  }

  @Test
  void wireGremlinLangScriptServesReaders() throws Exception {
    createUsers();

    final Cluster cluster = createCluster(READER_USER, READER_PASSWORD);
    try {
      // No alias: 'g' is the global traversal source of the default database, which is the test database.
      final Client client = cluster.connect();
      final List<Result> results = client.submit("g.V().hasLabel('Probe').count()",
          RequestOptions.build().language("gremlin-lang").create()).all().get();
      assertThat(results.getFirst().getLong()).isEqualTo(1L);
    } finally {
      cluster.close();
    }
  }

  @Test
  void wireBytecodeLambdaIsRootOnly() {
    createUsers();

    final Cluster cluster = createCluster(DBADMIN_USER, DBADMIN_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));

      // A traversal without lambdas keeps working.
      assertThat(g.V().hasLabel("Probe").count().next()).isEqualTo(1L);

      // A lambda travels in bytecode as Groovy source the server compiles and runs.
      assertThat(catchThrowable(() -> g.V().hasLabel("Probe").map(Lambda.function("it.get().label()")).toList()))
          .as("A non-root user must not run a Groovy lambda carried in bytecode").hasStackTraceContaining(REFUSAL);

      // Nested inside a child traversal it is the same Groovy source.
      assertThat(catchThrowable(() -> g.V().hasLabel("Probe").union(__.map(Lambda.function("it.get().label()"))).toList()))
          .as("A non-root user must not run a Groovy lambda nested in a child traversal").hasStackTraceContaining(REFUSAL);
    } finally {
      cluster.close();
    }

    final Cluster rootCluster = createCluster("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(rootCluster, getDatabaseName()));
      assertThat(g.V().hasLabel("Probe").map(Lambda.function("it.get().label()")).toList()).containsExactly("Probe");
    } finally {
      rootCluster.close();
    }
  }

  @Test
  void wireIoStepIsRootOnly() {
    createUsers();

    final Cluster cluster = createCluster(DBADMIN_USER, DBADMIN_PASSWORD);
    try {
      final GraphTraversalSource g = AnonymousTraversalSource.traversal()
          .withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
      assertThat(catchThrowable(() -> g.io(ioTarget.getAbsolutePath()).write().iterate()))
          .as("A non-root user must not write a host file through the io() step").hasStackTraceContaining(REFUSAL);
      assertThat(ioTarget).doesNotExist();
    } finally {
      cluster.close();
    }
  }

  private void createUsers() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sqlscript", "CREATE VERTEX TYPE Probe IF NOT EXISTS;\nINSERT INTO Probe SET name = 'probe';");

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), "gremlinScriptReaders", new JSONObject()
        .put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroup(getDatabaseName(), "gremlinScriptDbAdmins", new JSONObject()
        .put("access", new JSONArray().put("updateSecurity").put("updateSchema").put("updateDatabaseSettings"))
        .put("types", new JSONObject().put("*", new JSONObject().put("access",
            new JSONArray().put("createRecord").put("readRecord").put("updateRecord").put("deleteRecord")))));

    if (security.getUser(READER_USER) == null)
      security.createUser(new JSONObject()
          .put("name", READER_USER)
          .put("password", security.encodePassword(READER_PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("gremlinScriptReaders"))));
    if (security.getUser(DBADMIN_USER) == null)
      security.createUser(new JSONObject()
          .put("name", DBADMIN_USER)
          .put("password", security.encodePassword(DBADMIN_PASSWORD))
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("gremlinScriptDbAdmins"))));
  }

  private void setGremlinEngine(final String engine) {
    getServerDatabase(0, getDatabaseName()).getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, engine);
  }

  private void assertRefused(final String user, final String password, final String command) throws Exception {
    final HttpURLConnection connection = post(user, password, command);
    try {
      assertThat(connection.getResponseCode()).isEqualTo(403);
      assertThat(readError(connection)).contains(REFUSAL);
    } finally {
      connection.disconnect();
    }
  }

  private int executeHttp(final String user, final String password, final String command) throws Exception {
    final HttpURLConnection connection = post(user, password, command);
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection post(final String user, final String password, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) URI.create(
        "http://127.0.0.1:" + getServerHttpPort() + "/api/v1/command/" + getDatabaseName()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes()));
    connection.setDoOutput(true);
    try (final PrintWriter pw = new PrintWriter(new OutputStreamWriter(connection.getOutputStream()))) {
      pw.write(new JSONObject().put("language", "gremlin").put("command", command).toString());
    }
    return connection;
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
        if (security.getUser(READER_USER) != null)
          security.dropUser(READER_USER);
        if (security.getUser(DBADMIN_USER) != null)
          security.dropUser(DBADMIN_USER);
      }
    } catch (final Exception e) {
      // IGNORE: server may already be stopped
    }
    ioTarget.delete();
    super.endTest();
  }
}
