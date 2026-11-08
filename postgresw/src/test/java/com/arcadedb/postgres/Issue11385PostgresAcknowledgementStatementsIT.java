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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #11385: the #11312 fix answered every {@code DDLStatement} as PostgreSQL answers CREATE TABLE, and DEFINE FUNCTION, DELETE
 * FUNCTION and SLEEP, which are not {@code DDLStatement} subclasses, kept being answered like a query: a RowDescription, a DataRow and
 * a tag with no count on the simple path, "A result was returned when none was expected" from pgjdbc's {@code executeUpdate()}, and
 * after a Describe('S') answered NoData, a refusal with {@code 0A000} of a statement the engine had already run. They are answered
 * like a schema change now, on all four protocol paths, decided by {@code Statement.returnsAcknowledgementOnly()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11385PostgresAcknowledgementStatementsIT extends PostgresWireProtocolTestBase {

  private static final String[] PATHS = { "Simple", "DescS", "NoDesc", "DescP" };

  @Test
  @DisplayName("[#11385] DEFINE FUNCTION, DELETE FUNCTION and SLEEP are answered with no result set on all four paths, and run once")
  void everyPathAnswersAFunctionLibraryChangeAsASchemaChange() throws Exception {
    // the functions the DELETE FUNCTION of each path removes
    for (final String path : PATHS)
      database().command("sql", "DEFINE FUNCTION del" + path + ".fn 'SELECT 1 AS result' LANGUAGE sql");

    // %s is replaced by a different suffix on each path: defining or deleting a function is not repeatable
    final String[][] cases = {
        { "DEFINE FUNCTION def%s.fn 'SELECT 1 AS result' LANGUAGE sql", "DEFINE FUNCTION" },
        { "DELETE FUNCTION del%s.fn", "DELETE FUNCTION" },
        { "SLEEP 1", "SLEEP" } };

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
        for (final String[] c : cases) {
          final String expectedTag = c[1];

          final String simpleSql = c[0].formatted("Simple");
          sendSimpleQuery(out, simpleSql);
          final List<WireMessage> simple = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(simple)).as("simple: " + simpleSql + " -> " + describe(simple)).containsExactly('C', 'Z');
          assertThat(commandTagOf(simple)).as("simple: " + simpleSql).isEqualTo(expectedTag);

          // asyncpg, pgx and npgsql: Describe the statement, then Bind and Execute without describing the portal
          final String describeStatementSql = c[0].formatted("DescS");
          final String statementName = "S" + expectedTag.hashCode();
          sendParse(out, statementName, describeStatementSql);
          sendDescribe(out, 'S', statementName);
          sendBind(out, "", statementName);
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> describeStatement = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(describeStatement)).as("Describe('S'): " + describeStatementSql + " -> " + describe(describeStatement))
              .containsExactly('1', 't', 'n', '2', 'C', 'Z');
          assertThat(commandTagOf(describeStatement)).as("Describe('S'): " + describeStatementSql).isEqualTo(expectedTag);

          // libpq PQexecPrepared, pgjdbc once a statement is promoted: no Describe at all
          final String noDescribeSql = c[0].formatted("NoDesc");
          sendParse(out, "", noDescribeSql);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> noDescribe = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(noDescribe)).as("no Describe: " + noDescribeSql + " -> " + describe(noDescribe))
              .containsExactly('1', '2', 'C', 'Z');
          assertThat(commandTagOf(noDescribe)).as("no Describe: " + noDescribeSql).isEqualTo(expectedTag);

          final String describePortalSql = c[0].formatted("DescP");
          sendParse(out, "", describePortalSql);
          sendBind(out, "", "");
          sendDescribe(out, 'P', "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> describePortal = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(describePortal)).as("Describe('P'): " + describePortalSql + " -> " + describe(describePortal))
              .containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(describePortal)).as("Describe('P'): " + describePortalSql).isEqualTo(expectedTag);
        }

        // each path ran its statements exactly once
        for (final String path : PATHS) {
          assertThat(database().getSchema().hasFunctionLibrary("def" + path)).as(path).isTrue();
          assertThat(functionExists("del" + path, "fn")).as(path).isFalse();
        }
      });
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "extended", "simple" })
  @DisplayName("[#11385] pgjdbc's executeUpdate() takes DEFINE FUNCTION, DELETE FUNCTION and SLEEP")
  void jdbcExecuteUpdateTakesAFunctionLibraryChange(final String queryMode) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    props.setProperty("preferQueryMode", queryMode);
    final String library = "jdbc" + queryMode;
    try (final Connection conn = DriverManager.getConnection(getServerPostgresJdbcUrl(), props); final Statement st = conn.createStatement()) {
      assertThat(st.executeUpdate("DEFINE FUNCTION " + library + ".fn 'SELECT 1 AS result' LANGUAGE sql")).isZero();
      assertThat(st.executeUpdate("DEFINE FUNCTION " + library + ".other 'SELECT 2 AS result' LANGUAGE sql")).isZero();
      assertThat(st.executeUpdate("DELETE FUNCTION " + library + ".other")).isZero();
      assertThat(st.executeUpdate("SLEEP 1")).isZero();
      assertThat(st.execute("DELETE FUNCTION " + library + ".fn")).isFalse();
      assertThat(st.getUpdateCount()).isZero();
    }
    assertThat(functionExists(library, "other")).isFalse();
    assertThat(functionExists(library, "fn")).isFalse();
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'C')
        return new String(message.body(), 0, message.body().length - 1, StandardCharsets.UTF_8);
    throw new AssertionError("no CommandComplete among " + describe(messages));
  }

  /** The message types, with the fields of an ErrorResponse, so a refusal says why. */
  private static String describe(final List<WireMessage> messages) {
    final StringBuilder text = new StringBuilder();
    for (final WireMessage message : messages)
      text.append(message.type() == 'E' ? "E" + errorFields(message) : String.valueOf(message.type())).append(' ');
    return text.toString().trim();
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private boolean functionExists(final String library, final String function) {
    return database().getSchema().hasFunctionLibrary(library) && database().getSchema().getFunctionLibrary(library).hasFunction(function);
  }

  private Database database() {
    return getServerDatabase(0, getDatabaseName());
  }
}
