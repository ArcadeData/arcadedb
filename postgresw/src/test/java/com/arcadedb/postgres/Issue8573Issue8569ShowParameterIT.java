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
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.firstColumnName;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for two Postgres-wire issues:
 * <ul>
 *   <li>#8573: {@code SET}, {@code SHOW} and {@code RESET} of a parameter PostgreSQL does not know succeeded, inventing
 *   it, where PostgreSQL answers {@code 42704};</li>
 *   <li>#8569: {@code SHOW TRANSACTION ISOLATION LEVEL} was answered by a branch of its own, from the database default
 *   instead of the open transaction's level and under a column named {@code LEVEL}, so it contradicted
 *   {@code SHOW transaction_isolation}.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8573Issue8569ShowParameterIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#8573] an unknown parameter is refused with 42704 by SET, SHOW and RESET, on both protocols")
  void unknownParameterIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String query : new String[] { "SET bogus_param = 1", "SHOW bogus_param", "SHOW nosuchparam", "RESET nosuchparam",
            "SET LOCAL bogus_param TO 2", "SHOW databases" }) {
          final List<WireMessage> answer = simple(out, in, query);
          assertThat(messageTypesOf(answer)).as(query).containsExactly('E', 'Z');
          assertThat(sqlStateOf(answer)).as(query).isEqualTo("42704");
        }

        // The extended protocol: SHOW is answered at Parse, SET/RESET at Execute
        for (final String query : new String[] { "SHOW bogus_param", "SET bogus_param = 1", "RESET bogus_param" }) {
          sendParse(out, "", query);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> answer = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(answer)).as(query).contains('E');
          assertThat(sqlStateOf(answer)).as(query).isEqualTo("42704");
        }

        // What still works: a parameter PostgreSQL knows, and a custom placeholder
        assertThat(messageTypesOf(simple(out, in, "SET statement_timeout = 5000"))).doesNotContain('E');
        assertThat(showValue(out, in, "statement_timeout")).isEqualTo("5000");
        assertThat(messageTypesOf(simple(out, in, "SET myapp.tenant = 'acme'"))).doesNotContain('E');
        assertThat(showValue(out, in, "myapp.tenant")).isEqualTo("acme");
        assertThat(messageTypesOf(simple(out, in, "RESET myapp.never_set"))).doesNotContain('E');

        // SHOW answers under PostgreSQL's spelling of the name
        assertThat(firstColumnName(simple(out, in, "SHOW datestyle"))).isEqualTo("DateStyle");

        // SHOW ALL: one name/setting/description row per parameter
        final List<WireMessage> all = simple(out, in, "SHOW ALL");
        assertThat(messageTypesOf(all)).doesNotContain('E').contains('D');
        assertThat(firstColumnName(all)).isEqualTo("name");
      });
    }
  }

  @Test
  @DisplayName("[#8569] SHOW TRANSACTION ISOLATION LEVEL answers the open transaction's level, as SHOW transaction_isolation")
  void showTransactionIsolationLevelReadsTheOpenTransaction() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        final List<WireMessage> idle = simple(out, in, "SHOW TRANSACTION ISOLATION LEVEL");
        assertThat(firstColumnName(idle)).isEqualTo("transaction_isolation");
        assertThat(firstDataRowValue(idle)).isEqualTo("read committed");

        assertThat(messageTypesOf(simple(out, in, "BEGIN ISOLATION REPEATABLE_READ"))).doesNotContain('E');
        final List<WireMessage> inTx = simple(out, in, "SHOW TRANSACTION ISOLATION LEVEL");
        assertThat(firstColumnName(inTx)).isEqualTo("transaction_isolation");
        assertThat(firstDataRowValue(inTx)).isEqualTo("repeatable read").isEqualTo(showValue(out, in, "transaction_isolation"));

        // The extended protocol reaches the same value
        sendParse(out, "", "SHOW TRANSACTION   ISOLATION LEVEL");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("repeatable read");

        assertThat(messageTypesOf(simple(out, in, "ROLLBACK"))).doesNotContain('E');

        // A prepared SHOW reads the value at each Execute, not the one current when it was parsed
        sendParse(out, "SHOWISO", "SHOW transaction_isolation");
        sendSync(out);
        readUntilReadyForQuery(in);
        assertThat(messageTypesOf(simple(out, in, "BEGIN ISOLATION REPEATABLE_READ"))).doesNotContain('E');
        sendBind(out, "", "SHOWISO");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("repeatable read");
        assertThat(messageTypesOf(simple(out, in, "ROLLBACK"))).doesNotContain('E');
        sendBind(out, "", "SHOWISO");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("read committed");
        assertThat(firstDataRowValue(simple(out, in, "SHOW TRANSACTION ISOLATION LEVEL"))).isEqualTo("read committed");
      });
    }
  }

  @Test
  @DisplayName("[#8569] pgjdbc's getTransactionIsolation() reads the level the open transaction runs at")
  void jdbcGetTransactionIsolation() throws Exception {
    try (final Connection conn = getConnection()) {
      assertThat(conn.getTransactionIsolation()).isEqualTo(Connection.TRANSACTION_READ_COMMITTED);
      try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("SHOW TRANSACTION ISOLATION LEVEL")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("transaction_isolation")).isEqualTo("read committed");
      }
    }
  }

  private Connection getConnection() throws Exception {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }

  private String showValue(final DataOutputStream out, final DataInputStream in, final String name) throws Exception {
    return firstDataRowValue(simple(out, in, "SHOW " + name));
  }

  private List<WireMessage> simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    return readUntilReadyForQuery(in);
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
