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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.grpc.InsertOptions;
import com.arcadedb.server.grpc.InsertOptions.ConflictMode;
import com.arcadedb.server.grpc.InsertOptions.TransactionMode;
import com.arcadedb.server.grpc.InsertSummary;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7320: a principal whose grants name real databases could not use gRPC at all. The client sent
 * no {@code x-arcade-database} metadata, so the server authenticated every call against the literal
 * name {@code "default"} - a database this user is not granted - and refused the first RPC with
 * {@code UNAUTHENTICATED: Invalid credentials}. Only a principal granted {@code "*"} could connect.
 * <p>
 * The users here are granted ONE named database each, which is the shape the issue reports. The test
 * drives both stubs {@link RemoteGrpcDatabase} builds - the blocking one (query and command) and the
 * async one (bidirectional ingestion) - because each takes its credentials from its own
 * {@code CallCredentials} instance, and it drives the refusal path too: a user granted some OTHER
 * database must still be refused, and the refusal must name the database rather than blame the
 * password.
 */
class Issue7320ScopedUserGrpcIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT    = 50051;
  private static final String SCOPED_USER  = "scoped7320";
  private static final String SCOPED_PWD   = "scoped7320password";
  private static final String FOREIGN_USER = "foreign7320";
  private static final String FOREIGN_PWD  = "foreign7320password";
  private static final String TYPE         = "Scoped7320";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (database != null) {
      database.close();
      database = null;
    }
    if (grpcServer != null) {
      grpcServer.close();
      grpcServer = null;
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  /**
   * The blocking stub: a user granted only this database can query it over gRPC. Before the fix this
   * failed on the first RPC with {@code UNAUTHENTICATED: Invalid credentials}, because the server
   * checked the grant against the name {@code "default"}.
   */
  @Test
  void scopedUserCanQueryOverGrpc() throws Exception {
    command(0, "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS");
    command(0, "INSERT INTO `" + TYPE + "` SET id = 'seed'");
    createUser(SCOPED_USER, SCOPED_PWD, getDatabaseName());

    final RemoteGrpcDatabase scoped = connect(SCOPED_USER, SCOPED_PWD);

    try (final ResultSet rs = scoped.query("sql", "SELECT count(*) AS c FROM `" + TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }
  }

  /**
   * The async stub, which carries its own {@code CallCredentials} instance and would keep sending no
   * database if only the blocking one had been fixed.
   */
  @Test
  void scopedUserCanIngestOverTheAsyncStub() throws Exception {
    command(0, "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS");
    command(0, "CREATE PROPERTY `" + TYPE + "`.id IF NOT EXISTS STRING");
    command(0, "CREATE INDEX IF NOT EXISTS ON `" + TYPE + "` (id) UNIQUE");
    createUser(SCOPED_USER, SCOPED_PWD, getDatabaseName());

    final RemoteGrpcDatabase scoped = connect(SCOPED_USER, SCOPED_PWD);

    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final Map<String, Object> row = new HashMap<>();
      row.put("id", "row-" + i);
      rows.add(row);
    }

    final InsertOptions options = InsertOptions.newBuilder()
        .setDatabase(getDatabaseName())
        .setTargetClass(TYPE)
        .addKeyColumns("id")
        .setConflictMode(ConflictMode.CONFLICT_ERROR)
        .setTransactionMode(TransactionMode.PER_BATCH)
        .setServerBatchSize(10)
        .setCredentials(scoped.buildCredentials())
        .build();

    final InsertSummary summary = scoped.ingestBidi(options, rows, 5, 5, 60_000);

    assertThat(summary.getInserted()).isEqualTo(10);
  }

  /**
   * The header is a grant check, not decoration: a user granted some other database is still refused
   * when it targets this one, and the refusal now says which database was checked instead of blaming
   * the password - the second half of the issue's complaint.
   */
  @Test
  void userGrantedAnotherDatabaseIsRefusedAndTheRefusalNamesIt() throws Exception {
    command(0, "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS");
    createUser(FOREIGN_USER, FOREIGN_PWD, "someotherdatabase7320");

    final RemoteGrpcDatabase foreign = connect(FOREIGN_USER, FOREIGN_PWD);

    assertThatThrownBy(() -> foreign.query("sql", "SELECT FROM `" + TYPE + "`"))
        .as("a user with no grant on this database must not reach it over gRPC")
        .hasMessageContaining(getDatabaseName());
  }

  private RemoteGrpcDatabase connect(final String user, final String password) {
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, user, password, true, List.of());
    // The port the test server actually bound: the configured range starts at 2480, but a server already
    // listening there pushes this one up.
    database = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT,
        getServer(0).getHttpServer().getPort(), getDatabaseName(), user, password);
    return database;
  }

  /**
   * Creates a principal granted {@code database} and nothing else - no {@code "*"} wildcard, which is
   * what every gRPC test needed before this fix. The group is {@code admin}, which the default group
   * configuration defines under the {@code "*"} database, so no group has to be added here.
   * <p>
   * The user goes in through the server's own {@code POST /api/v1/server/users} endpoint rather than
   * through {@code ServerSecurity.createUser}, because the handler is what encodes the password into
   * the form the authentication path expects. Same approach as {@code Issue7305TimeSeriesGrpcAclIT}.
   */
  private void createUser(final String name, final String password, final String database) throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(name))
      security.dropUser(name);

    final JSONObject payload = new JSONObject()
        .put("name", name)
        .put("password", password)
        .put("databases", new JSONObject().put(database, new JSONArray().put("admin")));

    final HttpURLConnection connection = (HttpURLConnection) URI.create(
            "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/server/users").toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final OutputStream out = connection.getOutputStream()) {
      out.write(payload.toString().getBytes(StandardCharsets.UTF_8));
    }
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
  }
}
