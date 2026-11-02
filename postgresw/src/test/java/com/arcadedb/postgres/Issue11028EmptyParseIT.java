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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #11028: a Parse of an empty query string was answered with EmptyQueryResponse, a reply that belongs to Execute, after the
 * statement name had been invalidated, so nothing was registered and every Bind/Describe/Execute pipelined behind it failed with
 * "prepared statement does not exist". PostgreSQL registers the empty statement like any other: ParseComplete, a Describe answers
 * NoData, an Execute answers EmptyQueryResponse in place of CommandComplete. Every Parse leaves exactly one ParseComplete or one
 * ErrorResponse.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11028EmptyParseIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#11028] Parse of an empty string answers ParseComplete")
  void parseOfAnEmptyStringAnswersParseComplete() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "pstmt_empty", "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#11028] the whole extended pipeline of an empty statement answers as PostgreSQL does")
  void theEmptyStatementPipelineAnswersOneReplyPerMessage() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // unnamed: PostgreSQL answers 1, 2, n (NoData), I (EmptyQueryResponse), Z
        sendParse(out, "", "");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'n', 'I', 'Z');

        // named, described as a statement: ParameterDescription and NoData
        sendParse(out, "pe2", "");
        sendDescribe(out, 'S', "pe2");
        sendBind(out, "", "pe2");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', '2', 'I', 'Z');

        // the statement stays registered: a statement cache binds and executes it again with no Parse in front
        sendBind(out, "", "pe2");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'I', 'Z');

        // text that normalizes to nothing is an empty statement too
        for (final String text : new String[] { "   ", ";", " ; " }) {
          sendParse(out, "", text);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("'%s'", text).containsExactly('1', '2', 'I', 'Z');
        }

        // and the connection is in step: an ordinary statement right after answers as usual
        sendParse(out, "", "SELECT 1 AS one");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'T', 'D', 'C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#11028] an empty statement inside a transaction block leaves the block open and healthy")
  void anEmptyStatementInsideATransactionBlockLeavesItOpen() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendParse(out, "", "");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('1', '2', 'I', 'Z');
        assertThat(readyForQueryStatusOf(response)).isEqualTo('T');

        sendSimpleQuery(out, "COMMIT");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#11028] pgjdbc prepareStatement(\"\") executes and returns no result set")
  void pgjdbcPreparesAndExecutesTheEmptyStatement() throws Exception {
    try (final Connection connection = jdbcConnection()) {
      try (final PreparedStatement statement = connection.prepareStatement("")) {
        assertThat(statement.execute()).isFalse();
        // executed again, as an application reuses a prepared statement
        assertThat(statement.execute()).isFalse();
      }
      try (final Statement statement = connection.createStatement(); final ResultSet rs = statement.executeQuery("SELECT 1 AS one")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(1);
      }
    }
  }

  private Connection jdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    // Parse/Bind/Describe/Execute for every statement, and named server-side statements from the first execution on
    props.setProperty("preferQueryMode", "extended");
    props.setProperty("prepareThreshold", "1");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
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
