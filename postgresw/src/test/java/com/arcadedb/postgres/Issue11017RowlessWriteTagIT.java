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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
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
 * Regression test for issue #11017: a write with no RETURN clause was answered with a result set and tagged with the
 * result-row count instead of the affected-row count on three of the four protocol paths (simple query, extended with
 * Describe('P'), extended with no Describe). Only the path with a statement-level Describe('S') was right. The four
 * paths must agree: no RowDescription, no DataRow, and a CommandComplete carrying the number of records changed.
 */
class Issue11017RowlessWriteTagIT extends PostgresWireProtocolTestBase {

  private static final int SEED_ROWS = 4;

  @Test
  @DisplayName("[#11017] the same write is answered with no result set and the affected count on all four protocol paths")
  void everyPathAnswersARowlessWriteTheSame() throws Exception {
    final String[][] cases = {
        { "DELETE FROM W11017 WHERE id <= 2", "DELETE 2" },
        { "DELETE FROM W11017 WHERE id = 999", "DELETE 0" },
        { "UPDATE W11017 SET tag = 'z' WHERE id <= 3", "UPDATE 3" },
        { "UPDATE W11017 SET tag = 'z' WHERE id = 999", "UPDATE 0" },
        { "INSERT INTO W11017 SET id = 7", "INSERT 0 1" } };

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
        int statementId = 0;
        for (final String[] c : cases) {
          final String statementName = "S" + statementId++;
          final String sql = c[0];
          final String expectedTag = c[1];

          seed(out, in);
          sendSimpleQuery(out, sql);
          final List<WireMessage> simple = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(simple)).as("simple: " + sql).containsExactly('C', 'Z');
          assertThat(commandTagOf(simple)).as("simple: " + sql).isEqualTo(expectedTag);

          seed(out, in);
          sendParse(out, statementName, sql);
          sendDescribe(out, 'S', statementName);
          sendBind(out, "", statementName);
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> describeStatement = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(describeStatement)).as("Describe('S'): " + sql).containsExactly('1', 't', 'n', '2', 'C', 'Z');
          assertThat(commandTagOf(describeStatement)).as("Describe('S'): " + sql).isEqualTo(expectedTag);

          seed(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> noDescribe = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(noDescribe)).as("no Describe: " + sql).containsExactly('1', '2', 'C', 'Z');
          assertThat(commandTagOf(noDescribe)).as("no Describe: " + sql).isEqualTo(expectedTag);

          seed(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendDescribe(out, 'P', "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> describePortal = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(describePortal)).as("Describe('P'): " + sql).containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(commandTagOf(describePortal)).as("Describe('P'): " + sql).isEqualTo(expectedTag);
        }
      });
    }
  }

  @Test
  @DisplayName("[#11017] a write WITH a RETURN keeps its result set and is tagged with the rows it returned")
  void writeWithReturnKeepsItsRows() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        seed(out, in);
        sendSimpleQuery(out, "DELETE FROM W11017 RETURN BEFORE WHERE id <= 2");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).contains('T', 'D');
        assertThat(commandTagOf(response)).isEqualTo("DELETE 2");
      });
    }
  }

  @Test
  @DisplayName("[#11017] JDBC executeUpdate(), execute()+getUpdateCount() and executeBatch() see the affected count")
  void jdbcSeesTheAffectedCount() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    try (final Connection conn = DriverManager.getConnection(getServerPostgresJdbcUrl(), props); final Statement st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE J11017 IF NOT EXISTS");

      assertThat(st.executeUpdate("INSERT INTO J11017 SET id = 1")).isEqualTo(1);
      assertThat(st.executeUpdate("INSERT INTO J11017 SET id = 2")).isEqualTo(1);
      assertThat(st.executeUpdate("INSERT INTO J11017 SET id = 3")).isEqualTo(1);
      assertThat(st.executeUpdate("UPDATE J11017 SET tag = 'z' WHERE id <= 2")).isEqualTo(2);

      assertThat(st.execute("UPDATE J11017 SET tag = 'y' WHERE id <= 3")).isFalse();
      assertThat(st.getUpdateCount()).isEqualTo(3);

      try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM J11017 WHERE id <= ?")) {
        ps.setInt(1, 2);
        assertThat(ps.executeUpdate()).isEqualTo(2);
      }

      try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO J11017 SET id = ?")) {
        ps.setInt(1, 10);
        ps.addBatch();
        ps.setInt(1, 11);
        ps.addBatch();
        assertThat(ps.executeBatch()).containsExactly(1, 1);
      }

      assertThat(st.executeUpdate("DELETE FROM J11017 WHERE id = 999")).isZero();
    }
  }

  private void seed(final DataOutputStream out, final DataInputStream in) throws Exception {
    simple(out, in, "CREATE DOCUMENT TYPE W11017 IF NOT EXISTS");
    simple(out, in, "DELETE FROM W11017");
    for (int i = 1; i <= SEED_ROWS; i++)
      simple(out, in, "INSERT INTO W11017 SET id = " + i + ", tag = 'a'");
  }

  private List<WireMessage> simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    return readUntilReadyForQuery(in);
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'C')
        return new String(message.body(), 0, message.body().length - 1, StandardCharsets.UTF_8);
    throw new AssertionError("no CommandComplete among " + messageTypesOf(messages));
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
}
