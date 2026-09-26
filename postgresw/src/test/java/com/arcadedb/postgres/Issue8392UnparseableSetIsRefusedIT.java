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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #8392: a {@code SET} the server cannot parse, or cannot honour, was answered {@code CommandComplete SET} having
 * applied nothing, on both protocols. {@code SET SESSION AUTHORIZATION} and {@code SET ROLE} were the sharpest: a client
 * that dropped privileges was told it had, and kept running with the connected user's.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8392UnparseableSetIsRefusedIT extends PostgresWireProtocolTestBase {

  @ParameterizedTest(name = "[#8392] simple protocol: {0} -> {1}")
  @CsvSource(delimiter = '|', value = {
      "SET x|42601",
      "SET SESSION AUTHORIZATION 'someone'|0A000",
      "SET ROLE readonly|0A000",
      "SET role = 'readonly'|0A000",
      "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE|0A000",
      "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY|0A000",
      "SET CONSTRAINTS fk_a DEFERRED|42704" })
  void simpleProtocolRefusesWithAnError(final String query, final String sqlState) throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, query);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("an ErrorResponse, never the tag for having executed it")
            .containsExactly('E', 'Z');
        assertThat(sqlStateOf(response)).isEqualTo(sqlState);
        assertThat(readyForQueryStatusOf(response)).as("autocommit: the connection is idle and usable").isEqualTo('I');

        assertThat(show(out, in, "role")).isEqualTo("none");
        assertThat(show(out, in, "session_authorization")).isEqualTo("root");
      });
    }
  }

  @ParameterizedTest(name = "[#8392] extended protocol: {0} is refused at Parse")
  @CsvSource(delimiter = '|', value = { "SET x", "SET CONSTRAINTS ALL", "SET TRANSACTION BOGUS" })
  void extendedProtocolRefusesAnUnparseableSetAtParse(final String query) throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "", query);
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("no ParseComplete, no CommandComplete: only the ErrorResponse")
            .containsExactly('E', 'Z');
        assertThat(sqlStateOf(response)).isEqualTo("42601");
      });
    }
  }

  @Test
  @DisplayName("[#8392] extended protocol: SET ROLE parses, and its Execute is refused")
  void extendedProtocolRefusesSetRoleAtExecute() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "", "SET ROLE readonly");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).contains('E').doesNotContain('C');
        assertThat(sqlStateOf(response)).isEqualTo("0A000");
        assertThat(show(out, in, "role")).isEqualTo("none");
      });
    }
  }

  @ParameterizedTest(name = "[#8392] accepted, since it is true: {0}")
  @CsvSource(delimiter = '|', value = {
      "SET CONSTRAINTS ALL DEFERRED",
      "SET ROLE NONE",
      "RESET ROLE",
      "SET SESSION AUTHORIZATION DEFAULT",
      "RESET SESSION AUTHORIZATION",
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
      "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
      "SET TRANSACTION READ WRITE",
      "SET NAMES 'UTF8'" })
  void truthfulSetsAreStillAnsweredSet(final String query) throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, query);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

        sendParse(out, "", query);
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#8392] a SET refused inside a transaction block aborts the block, as any other error does")
  void refusedSetAbortsTheBlock() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SET SESSION AUTHORIZATION 'someone'");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(sqlStateOf(response)).isEqualTo("0A000");
        assertThat(readyForQueryStatusOf(response)).as("failed transaction block").isEqualTo('E');
        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#8392] pgjdbc: setTransactionIsolation works for the real level and is refused for any other")
  void jdbcTransactionIsolation() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      assertThat(connection.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);

      assertThatThrownBy(() -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE))
          .isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));

      try (final Statement st = connection.createStatement()) {
        assertThatThrownBy(() -> st.execute("SET SESSION AUTHORIZATION 'someone'"))
            .isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));
        assertThatThrownBy(() -> st.execute("SET x"))
            .isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("42601"));
        // THE CONNECTION IS STILL USABLE
        st.execute("SET application_name = 'issue8392'");
      }
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
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
