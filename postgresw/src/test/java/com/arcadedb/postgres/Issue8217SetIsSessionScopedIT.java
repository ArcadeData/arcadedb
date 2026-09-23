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
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #8217: a PostgreSQL {@code SET} is session-scoped, and on this server it was not in
 * either direction.
 * <ul>
 *   <li>{@code SET datestyle = ISO} rewrote the schema's date-time format, shared by every session on every protocol,
 *   and needed {@code UPDATE_DATABASE_SETTINGS} to do it;</li>
 *   <li>every other {@code SET} was recorded and never read: {@code SHOW} answered from a hard-coded table.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8217SetIsSessionScopedIT extends PostgresWireProtocolTestBase {

  private static final String RESTRICTED_USER     = "issue8217noSettings";
  private static final String RESTRICTED_PASSWORD = "issue8217noSettingsPwd";
  private static final String RESTRICTED_GROUP    = "issue8217NoSettings";

  @Test
  @DisplayName("[#8217] SET datestyle = ISO leaves the database-wide date-time format alone")
  void setDatestyleDoesNotRewriteTheSchemaFormat() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String formatBefore = database.getSchema().getDateTimeFormat();

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, "root", DEFAULT_PASSWORD_FOR_TESTS);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET datestyle = 'ISO'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // Over the extended protocol too: the path #8135 moved the SET to.
        sendParse(out, "", "SET datestyle TO 'ISO'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        assertThat(show(out, in, "datestyle")).as("the session answers the style it set, in PostgreSQL's spelling")
            .isEqualTo("ISO, MDY");
      });
    }

    assertThat(database.getSchema().getDateTimeFormat()).as("a pg session's SET must not change the database-wide format")
        .isEqualTo(formatBefore);

    // What the schema write used to break for every other client: a datetime literal in the database's own format.
    database.command("sql", "CREATE DOCUMENT TYPE Issue8217 IF NOT EXISTS");
    database.command("sql", "CREATE PROPERTY Issue8217.created IF NOT EXISTS DATETIME");
    database.transaction(() -> database.command("sql", "INSERT INTO Issue8217 SET created = '2024-05-19 17:05:11'"));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Issue8217")) {
      assertThat(rs.next().<Long>getProperty("c")).isGreaterThanOrEqualTo(1L);
    }
  }

  @Test
  @DisplayName("[#8217] a user without UPDATE_DATABASE_SETTINGS can run SET datestyle, a purely per-session statement")
  void setDatestyleNeedsNoDatabasePermission() throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), RESTRICTED_GROUP, new JSONObject().put("access", new JSONArray())
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    security.createUser(new JSONObject().put("name", RESTRICTED_USER).put("password", security.encodePassword(RESTRICTED_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(new String[] { RESTRICTED_GROUP }))));

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, RESTRICTED_USER, RESTRICTED_PASSWORD);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET datestyle = 'ISO, DMY'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("simple protocol").doesNotContain('E');

        sendParse(out, "", "SET datestyle = 'ISO, YMD'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("extended protocol").doesNotContain('E');

        assertThat(show(out, in, "datestyle")).isEqualTo("ISO, YMD");
      });
    } finally {
      security.dropUser(RESTRICTED_USER);
      security.deleteGroup(getDatabaseName(), RESTRICTED_GROUP);
    }
  }

  @Test
  @DisplayName("[#8217] SHOW answers what this session SET, and another session does not see it")
  void showReadsTheSessionsOwnSettings() throws Exception {
    try (final Socket first = connect(); final Socket second = connect()) {
      final DataOutputStream out1 = new DataOutputStream(first.getOutputStream());
      final DataInputStream in1 = new DataInputStream(first.getInputStream());
      authenticate(out1, in1, "root", DEFAULT_PASSWORD_FOR_TESTS);
      final DataOutputStream out2 = new DataOutputStream(second.getOutputStream());
      final DataInputStream in2 = new DataInputStream(second.getInputStream());
      authenticate(out2, in2, "root", DEFAULT_PASSWORD_FOR_TESTS);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out1, "SET search_path TO x");
        assertThat(messageTypesOf(readUntilReadyForQuery(in1))).doesNotContain('E');
        sendSimpleQuery(out1, "SET datestyle = 'DMY'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in1))).doesNotContain('E');

        assertThat(show(out1, in1, "search_path")).as("SHOW reads back what SET stored").isEqualTo("x");
        assertThat(show(out1, in1, "SEARCH_PATH")).as("parameter names are case-insensitive").isEqualTo("x");
        assertThat(show(out1, in1, "datestyle")).isEqualTo("ISO, DMY");

        // SHOW over the extended protocol reads the same settings.
        sendParse(out1, "", "SHOW search_path");
        sendBind(out1, "", "");
        sendExecute(out1, "");
        sendSync(out1);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in1))).as("extended-protocol SHOW").isEqualTo("x");

        assertThat(show(out2, in2, "search_path")).as("another session keeps its own value").isEmpty();
        assertThat(show(out2, in2, "datestyle")).as("another session keeps its own value").isEqualTo("ISO, MDY");

        sendSimpleQuery(out1, "SET search_path TO DEFAULT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in1))).doesNotContain('E');
        assertThat(show(out1, in1, "search_path")).as("SET ... TO DEFAULT restores the default").isEmpty();
      });
    }
  }

  @Test
  @DisplayName("[#8217] a SET PostgreSQL refuses is refused, with its SQLSTATE, and changes nothing")
  void invalidAndReadOnlySetsAreRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in, "root", DEFAULT_PASSWORD_FOR_TESTS);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET server_version = '99'");
        assertThat(sqlStateOf(readUntilReadyForQuery(in))).as("cant_change_runtime_param").isEqualTo("55P02");
        assertThat(show(out, in, "server_version")).isEqualTo(PostgresNetworkExecutor.PG_SERVER_VERSION);

        sendSimpleQuery(out, "SET datestyle = 'nonsense'");
        assertThat(sqlStateOf(readUntilReadyForQuery(in))).as("invalid_parameter_value").isEqualTo("22023");
        assertThat(show(out, in, "datestyle")).isEqualTo("ISO, MDY");
      });
    }
  }

  @Test
  @DisplayName("[#8217] run-time parameters sent in the startup packet are what SHOW answers")
  void startupParametersAreShown() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      // What pgjdbc sends: DateStyle and TimeZone in PostgreSQL's mixed case.
      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      for (final String s : new String[] { "user", "root", "database", getDatabaseName(), "DateStyle", "ISO, DMY", "TimeZone",
          "Europe/Rome", "application_name", "issue8217" })
        writeCString(body, s);
      body.write(0);
      final byte[] bodyBytes = body.toByteArray();
      out.writeInt(4 + 4 + bodyBytes.length);
      out.writeInt(196608); // protocol version 3.0
      out.write(bodyBytes);
      out.flush();
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(show(out, in, "datestyle")).isEqualTo("ISO, DMY");
        assertThat(show(out, in, "timezone")).isEqualTo("Europe/Rome");
        assertThat(show(out, in, "application_name")).isEqualTo("issue8217");
        assertThat(show(out, in, "user")).as("user is a startup field, not a run-time parameter").isEmpty();
      });
    }
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in, final String user, final String password)
      throws Exception {
    sendStartupMessage(out, user, getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, password);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
