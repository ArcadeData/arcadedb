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
import java.time.Duration;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for two Postgres-wire issues:
 * <ul>
 *   <li>#8391: {@code SHOW TIME ZONE}, the read-back of {@code SET TIME ZONE}, answered an empty string while
 *   {@code SHOW timezone} answered the value, and {@code RESET TIME ZONE} was refused as malformed;</li>
 *   <li>#8379: a prepared statement whose {@code Describe('S')} answered {@code NoData} still sent DataRows from
 *   Execute, with no RowDescription anywhere in the exchange.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8391Issue8379ShowTimeZoneAndDescribeNoDataIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#8391] SHOW TIME ZONE and SHOW timezone read the same parameter, and RESET TIME ZONE resets it")
  void showTimeZoneReadsTheSameParameterAsShowTimezone() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        assertThat(show(out, in, "TIME ZONE")).isEqualTo(show(out, in, "timezone")).isEqualTo("UTC");

        assertThat(messageTypesOf(simple(out, in, "SET TIME ZONE 'Europe/Rome'"))).doesNotContain('E');
        assertThat(show(out, in, "timezone")).isEqualTo("Europe/Rome");
        assertThat(show(out, in, "TIME ZONE")).isEqualTo("Europe/Rome");
        assertThat(show(out, in, "time   zone")).isEqualTo("Europe/Rome");

        final List<WireMessage> reset = simple(out, in, "RESET TIME ZONE");
        assertThat(messageTypesOf(reset)).as("RESET TIME ZONE is PostgreSQL syntax, not a malformed RESET").doesNotContain('E');
        assertThat(show(out, in, "TIME ZONE")).isEqualTo("UTC");
        assertThat(show(out, in, "timezone")).isEqualTo("UTC");

        // The extended protocol takes its own SHOW arm, so it is checked too.
        sendParse(out, "", "SHOW TIME ZONE");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("UTC");
      });
    }
  }

  @Test
  @DisplayName("[#8379] an INSERT whose Describe('S') answered NoData is answered with CommandComplete only, as PostgreSQL does")
  void insertDescribedAsNoDataSendsNoDataRow() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE T8379 IF NOT EXISTS");

        // The shape asyncpg, npgsql and pgx use: the statement is described once, then only bound and executed.
        sendParse(out, "S1", "INSERT INTO T8379 SET name = 'a'");
        sendDescribe(out, 'S', "S1");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', 'Z');

        for (int i = 0; i < 2; i++) {
          sendBind(out, "", "S1");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> execute = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(execute)).as("NoData promised no result set: no DataRow").containsExactly('2', 'C', 'Z');
          assertThat(commandTagOf(execute)).isEqualTo("INSERT 0 1");
        }

        // The same exchange in one round trip, as the issue reported it.
        sendParse(out, "S2", "INSERT INTO T8379 SET name = 'b'");
        sendDescribe(out, 'S', "S2");
        sendBind(out, "P2", "S2");
        sendExecute(out, "P2");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', '2', 'C', 'Z');

        // The writes happened: the rows were only withheld, not the INSERT.
        assertThat(firstDataRowValue(simple(out, in, "SELECT count(*) AS c FROM T8379"))).isEqualTo("3");
      });
    }
  }

  @Test
  @DisplayName("[#8379] a DELETE is not described by the type its FROM names, and answers no DataRow")
  void deleteIsDescribedAsNoData() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE D8379 IF NOT EXISTS");
        simple(out, in, "INSERT INTO D8379 SET name = 'a'");

        sendParse(out, "S1", "DELETE FROM D8379 WHERE name = 'a'");
        sendDescribe(out, 'S', "S1");
        sendBind(out, "", "S1");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('1', 't', 'n', '2', 'C', 'Z');
        assertThat(commandTagOf(response)).startsWith("DELETE ");

        assertThat(firstDataRowValue(simple(out, in, "SELECT count(*) AS c FROM D8379"))).isEqualTo("0");
      });
    }
  }

  @Test
  @DisplayName("[#8379] a statement whose rows its Describe('S') could not name is refused, not answered with unannounced rows")
  void rowReturningStatementDescribedAsNoDataIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "C1", "{cypher} UNWIND [1, 2] AS x RETURN x");
        sendDescribe(out, 'S', "C1");
        sendBind(out, "", "C1");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("no DataRow without a RowDescription").doesNotContain('D').contains('n', 'E');
        assertThat(sqlStateOf(refused)).isEqualTo("0A000");

        // The session is healthy afterwards, and describing the PORTAL - what pgjdbc and libpq do - names the columns.
        sendBind(out, "", "C1");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'T', 'D', 'D', 'C', 'Z');

        // A statement that returns no rows keeps the NoData promise without any error.
        sendParse(out, "C2", "{cypher} UNWIND [] AS x RETURN x");
        sendDescribe(out, 'S', "C2");
        sendBind(out, "", "C2");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', '2', 'C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#8379] control: a SELECT Describe('S') can name keeps its RowDescription and DataRows")
  void resolvableSelectIsUnchanged() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE Q8379 IF NOT EXISTS");
        simple(out, in, "CREATE PROPERTY Q8379.name IF NOT EXISTS STRING");
        simple(out, in, "INSERT INTO Q8379 SET name = 'a'");

        sendParse(out, "S3", "SELECT name FROM Q8379");
        sendDescribe(out, 'S', "S3");
        sendBind(out, "P3", "S3");
        sendExecute(out, "P3");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'T', '2', 'D', 'C', 'Z');
      });
    }
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
