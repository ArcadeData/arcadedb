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
package com.arcadedb.server.security;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.remote.grpc.RemoteGrpcDatabase;
import com.arcadedb.remote.grpc.RemoteGrpcServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7374: a {@link RemoteGrpcServer} built for one account and a {@link RemoteGrpcDatabase}
 * built on it for another - the combination the database constructor invites, since it asks for a
 * user of its own - used to run every gRPC call as the SERVER's account. The database's user reached
 * the request body and nothing else, and the server prefers the metadata-authenticated principal.
 * <p>
 * Every test here builds the server as {@code root} and the database as a scoped user, and then asks
 * the server which one it saw. It answers in its own refusal messages: {@code LocalDatabase} names
 * the principal it refused, and {@code GrpcAuthInterceptor} names the database it checked the grant
 * against.
 */
class Issue7374GrpcPrincipalIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT       = 50051;
  private static final String SCOPED_USER     = "scoped7374";
  private static final String SCOPED_PWD      = "scoped7374password";
  private static final String FOREIGN_USER    = "foreign7374";
  private static final String FOREIGN_PWD     = "foreign7374password";
  private static final String GROUP           = "acl7374";
  private static final String OPEN_TYPE       = "Open7374";
  private static final String RESTRICTED_TYPE = "Restricted7374";

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
   * The authorization half: the database's user is the one the engine gates on. The scoped user is
   * denied {@code RESTRICTED_TYPE} and the {@link RemoteGrpcServer} is {@code root}, so before the fix
   * the query simply returned the row. The refusal names the principal, which is the whole question.
   */
  @Test
  void theDatabaseUserIsTheOneTheEngineAuthorizes() throws Exception {
    sql("CREATE VERTEX TYPE `" + RESTRICTED_TYPE + "` IF NOT EXISTS");
    sql("INSERT INTO `" + RESTRICTED_TYPE + "` SET id = 'secret'");
    createScopedUser();

    connect(SCOPED_USER, SCOPED_PWD);

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM `" + RESTRICTED_TYPE + "`")) {
        rs.hasNext();
      }
    }).as("the query must run as the database's user, not as the server's root")
        .hasMessageContaining(SCOPED_USER)
        .hasMessageContaining(RESTRICTED_TYPE);
  }

  /**
   * The same connection on a type the scoped user IS granted still works, so the fix refuses by grant
   * rather than by having broken the credentials.
   */
  @Test
  void theDatabaseUserStillReachesWhatItIsGranted() throws Exception {
    sql("CREATE VERTEX TYPE `" + OPEN_TYPE + "` IF NOT EXISTS");
    sql("CREATE VERTEX TYPE `" + RESTRICTED_TYPE + "` IF NOT EXISTS");
    sql("INSERT INTO `" + OPEN_TYPE + "` SET id = 'visible'");
    createScopedUser();

    connect(SCOPED_USER, SCOPED_PWD);

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM `" + OPEN_TYPE + "`")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1L);
    }
  }

  /**
   * The authentication half: a database user granted some OTHER database is refused, even though the
   * {@link RemoteGrpcServer} it shares a channel with is {@code root} and would have been let in. The
   * refusal names the database the grant was checked against (#7320's wording), which is how this test
   * tells "refused for the right reason" from "refused for the wrong one".
   */
  @Test
  void aForeignDatabaseUserIsRefusedEvenOnARootServer() throws Exception {
    sql("CREATE VERTEX TYPE `" + OPEN_TYPE + "` IF NOT EXISTS");
    createUserGrantedDatabase(FOREIGN_USER, FOREIGN_PWD, "someotherdatabase7374", "admin");

    connect(FOREIGN_USER, FOREIGN_PWD);

    assertThatThrownBy(() -> database.query("sql", "SELECT FROM `" + OPEN_TYPE + "`"))
        .as("a database user with no grant on this database must not ride in on the server's account")
        .hasMessageContaining(getDatabaseName());
  }

  /**
   * Runs DDL/DML on the server's own embedded database rather than through
   * {@code BaseGraphServerTest.command(0, ...)}, which posts to a hardcoded {@code 127.0.0.1:2480} - a port
   * this server does not necessarily own, since the configured range starts there and a server already
   * listening pushes this one up.
   */
  private void sql(final String statement) {
    getServer(0).getDatabase(getDatabaseName()).command("sql", statement).close();
  }

  private void connect(final String user, final String password) {
    // The server holds root; the database holds the scoped principal. That is the mismatch #7374 is about.
    grpcServer = new RemoteGrpcServer("localhost", GRPC_PORT, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    // The port the test server actually bound: the configured range starts at 2480, but a server already
    // listening there pushes this one up.
    database = new RemoteGrpcDatabase(grpcServer, "localhost", GRPC_PORT,
        getServer(0).getHttpServer().getPort(), getDatabaseName(), user, password);
  }

  /**
   * Grants the scoped user every access on every type except {@link #RESTRICTED_TYPE}, whose access
   * list is empty, on this database only.
   */
  private void createScopedUser() throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();

    security.getDatabaseGroupsConfiguration(getDatabaseName()).put(GROUP,
        new JSONObject().put("access", new JSONArray().put("updateSecurity").put("updateSchema"))
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access",
                    new JSONArray().put("readRecord").put("createRecord").put("updateRecord").put("deleteRecord")))
                .put(RESTRICTED_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    createUserGrantedDatabase(SCOPED_USER, SCOPED_PWD, getDatabaseName(), GROUP);
  }

  /**
   * Creates a principal granted {@code database} and nothing else - no {@code "*"} wildcard, so the
   * grant check is a real one.
   * <p>
   * The user goes in through the server's own {@code POST /api/v1/server/users} endpoint rather than
   * through {@code ServerSecurity.createUser}, because the handler is what encodes the password into
   * the form the authentication path expects. Same approach as {@code Issue7320ScopedUserGrpcIT}.
   */
  private void createUserGrantedDatabase(final String name, final String password, final String database,
      final String group) throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(name))
      security.dropUser(name);

    final JSONObject payload = new JSONObject()
        .put("name", name)
        .put("password", password)
        .put("databases", new JSONObject().put(database, new JSONArray().put(group)));

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
    // The endpoint answers 201 on a fresh user and 200 on a replaced one; both mean the user exists.
    assertThat(connection.getResponseCode()).isIn(200, 201);
    connection.disconnect();
  }
}
