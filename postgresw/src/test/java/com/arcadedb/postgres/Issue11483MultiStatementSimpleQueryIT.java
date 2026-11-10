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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #11483: a simple query ('Q') holding several statements was handed to one statement as a whole,
 * so psqlodbc 16, whose connect-time setup is {@code SET DateStyle = 'ISO';SET extra_float_digits = 2;show
 * transaction_isolation} in one message, could not connect since a SET the server cannot parse is refused (#8392).
 * <p>
 * PostgreSQL runs the statements of one 'Q' in order, answers each with its own RowDescription/DataRows/CommandComplete,
 * stops at the first error and sends ONE ReadyForQuery at the end. Outside an explicit block they form one implicit
 * transaction block: an error rolls back every statement of the message before it, SETs included; a BEGIN inside the
 * message turns the block into an explicit one (the statements before it included), a COMMIT/ROLLBACK ends it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11483MultiStatementSimpleQueryIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#11483] psqlodbc 16's connect-time SET;SET;show runs all three statements and ends with one ReadyForQuery")
  void psqlodbc16ConnectString() throws Exception {
    withConnection((out, in) -> {
      sendSimpleQuery(out, "SET DateStyle = 'ISO';SET extra_float_digits = 2;show transaction_isolation");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("SET, SET, then the SHOW's row, one ReadyForQuery")
          .containsExactly('C', 'C', 'T', 'D', 'C', 'Z');
      assertThat(commandTagsOf(answers)).containsExactly("SET", "SET", "SHOW");
      assertThat(dataRowValuesOf(answers)).containsExactly("read committed");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      // Both SETs were applied, and committed with the message
      assertThat(show(out, in, "DateStyle")).startsWith("ISO");
      assertThat(show(out, in, "extra_float_digits")).isEqualTo("2");
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] RESET;SHOW and SET;SET run statement by statement")
  void resetThenShow() throws Exception {
    withConnection((out, in) -> {
      sendSimpleQuery(out, "SET DateStyle = 'ISO'; SET extra_float_digits = 3");
      List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('C', 'C', 'Z');

      sendSimpleQuery(out, "RESET extra_float_digits;SHOW extra_float_digits");
      answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('C', 'T', 'D', 'C', 'Z');
      assertThat(commandTagsOf(answers)).containsExactly("RESET", "SHOW");
      assertThat(dataRowValuesOf(answers)).as("the RESET took effect before the SHOW ran").doesNotContain("3");
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] two SELECTs in one message answer two result sets")
  void twoSelects() throws Exception {
    withConnection((out, in) -> {
      sendSimpleQuery(out, "SELECT 1 AS a;SELECT 'x;y' AS b /* ; */;");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('T', 'D', 'C', 'T', 'D', 'C', 'Z');
      assertThat(dataRowValuesOf(answers)).containsExactly("1", "x;y");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] the statements of a message commit together when they all succeed")
  void implicitBlockCommits() throws Exception {
    final String type = "Issue11483Commit";
    withConnection((out, in) -> {
      createType(out, in, type);
      sendSimpleQuery(out, "INSERT INTO " + type + " SET id = 1; INSERT INTO " + type + " SET id = 2; SELECT count(*) AS c FROM " + type);
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(commandTagsOf(answers)).containsExactly("INSERT 0 1", "INSERT 0 1", "SELECT 1");
      assertThat(dataRowValuesOf(answers)).as("the SELECT sees the writes of the statements before it").containsExactly("2");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
    assertThat(idsOf(type)).containsExactly(1, 2);
  }

  @Test
  @DisplayName("[#11483] the first error stops the message and rolls back the statements of its implicit block")
  void errorStopsAndRollsBack() throws Exception {
    final String type = "Issue11483Error";
    withConnection((out, in) -> {
      createType(out, in, type);
      sendSimpleQuery(out, "INSERT INTO " + type + " SET id = 1; SELEKT bogus; INSERT INTO " + type + " SET id = 3");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("the first INSERT, the error, nothing for the third statement")
          .containsExactly('C', 'E', 'Z');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
    assertThat(idsOf(type)).as("the failed message left nothing behind").isEmpty();
  }

  @Test
  @DisplayName("[#11483] a SET of a failed implicit block is rolled back with it")
  void setOfFailedBlockIsRolledBack() throws Exception {
    withConnection((out, in) -> {
      final String before = show(out, in, "extra_float_digits");
      sendSimpleQuery(out, "SET extra_float_digits = 3; SELEKT bogus");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('C', 'E', 'Z');
      assertThat(show(out, in, "extra_float_digits")).isEqualTo(before).isNotEqualTo("3");
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] SET LOCAL lasts until the end of the message's implicit block")
  void setLocalLastsForTheMessage() throws Exception {
    withConnection((out, in) -> {
      final String before = show(out, in, "extra_float_digits");
      sendSimpleQuery(out, "SET LOCAL extra_float_digits = 3; SHOW extra_float_digits");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(dataRowValuesOf(answers)).containsExactly("3");
      assertThat(show(out, in, "extra_float_digits")).as("the block ended with the message").isEqualTo(before);
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] a BEGIN inside the message turns its implicit block into an explicit one, earlier statements included")
  void beginInsideTheMessage() throws Exception {
    final String type = "Issue11483Begin";
    withConnection((out, in) -> {
      createType(out, in, type);
      sendSimpleQuery(out, "INSERT INTO " + type + " SET id = 1; BEGIN; INSERT INTO " + type + " SET id = 2");
      List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(commandTagsOf(answers)).containsExactly("INSERT 0 1", "BEGIN", "INSERT 0 1");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('T');

      sendSimpleQuery(out, "ROLLBACK");
      answers = readUntilReadyForQuery(in);
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
    assertThat(idsOf(type)).as("the ROLLBACK discarded the INSERT sent before the BEGIN too").isEmpty();
  }

  @Test
  @DisplayName("[#11483] a COMMIT inside the message ends its implicit block: a later error rolls back only what follows it")
  void commitInsideTheMessage() throws Exception {
    final String type = "Issue11483CommitInside";
    withConnection((out, in) -> {
      createType(out, in, type);
      sendSimpleQuery(out, "INSERT INTO " + type + " SET id = 1; COMMIT; INSERT INTO " + type + " SET id = 2; SELEKT bogus");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('C', 'C', 'C', 'E', 'Z');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
    assertThat(idsOf(type)).containsExactly(1);
  }

  @Test
  @DisplayName("[#11483] an error inside an explicit block aborts it and skips the rest of the message")
  void errorInsideExplicitBlock() throws Exception {
    final String type = "Issue11483Explicit";
    withConnection((out, in) -> {
      createType(out, in, type);
      sendSimpleQuery(out, "BEGIN; INSERT INTO " + type + " SET id = 1; SELEKT bogus; INSERT INTO " + type + " SET id = 3");
      List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('C', 'C', 'E', 'Z');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('E');

      // the way out of the aborted block, and a statement after it, in one message
      sendSimpleQuery(out, "ROLLBACK; SELECT 7 AS v");
      answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('C', 'T', 'D', 'C', 'Z');
      assertThat(commandTagsOf(answers).get(0)).isEqualTo("ROLLBACK");
      assertThat(dataRowValuesOf(answers)).containsExactly("7");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
    assertThat(idsOf(type)).isEmpty();
  }

  @Test
  @DisplayName("[#11483] a statement after the first, in an aborted block, is refused and stops the message")
  void abortedBlockRefusesTheFirstStatement() throws Exception {
    withConnection((out, in) -> {
      sendSimpleQuery(out, "BEGIN; SELEKT bogus");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

      sendSimpleQuery(out, "SELECT 1; ROLLBACK");
      List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).as("25P02 for the SELECT, the ROLLBACK is never reached").containsExactly('E', 'Z');
      assertThat(sqlStateOf(answers)).isEqualTo("25P02");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('E');

      sendSimpleQuery(out, "ROLLBACK");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] DISCARD ALL is refused inside a multi-statement message, which is a transaction block, and runs alone")
  void discardAllInsideAMultiStatementMessage() throws Exception {
    withConnection((out, in) -> {
      sendSimpleQuery(out, "SELECT 1 AS a; DISCARD ALL");
      List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('T', 'D', 'C', 'E', 'Z');
      assertThat(sqlStateOf(answers)).isEqualTo("25001");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');

      sendSimpleQuery(out, "DISCARD ALL;");
      answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('C', 'Z');
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] a message of only separators or comments is the empty query")
  void emptyMessages() throws Exception {
    withConnection((out, in) -> {
      for (final String text : new String[] { ";", " ; ;; ", "-- just a comment", "/* a; b */ ;" }) {
        sendSimpleQuery(out, text);
        final List<WireMessage> answers = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(answers)).as("EmptyQueryResponse for '%s'", text).containsExactly('I', 'Z');
      }
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] a language other than SQL gets the whole text: a SQL script keeps its own statements")
  void sqlScriptIsNotSplit() throws Exception {
    withConnection((out, in) -> {
      sendSimpleQuery(out, "{sqlscript} LET a = SELECT 41 + 1 AS v; RETURN $a");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).doesNotContain('E');
      assertThat(messageTypesOf(answers).stream().filter(t -> t == 'C').count()).as("one statement, one CommandComplete").isEqualTo(1);
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] a Parse of several statements is refused as PostgreSQL refuses it, and the pipeline recovers at Sync")
  void parseOfSeveralStatementsIsRefused() throws Exception {
    withConnection((out, in) -> {
      sendParse(out, "multi", "SELECT 1; SELECT 2");
      sendBind(out, "multi", "multi");
      sendExecute(out, "multi");
      sendSync(out);
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('E', 'Z');
      final WireMessage error = answers.get(0);
      assertThat(errorFields(error).get('C')).isEqualTo("42601");
      assertThat(errorFields(error).get('M')).contains("cannot insert multiple commands into a prepared statement");
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
  }

  @Test
  @DisplayName("[#11483] the JDBC driver in simple query mode runs a multi-statement text and reads every result")
  void jdbcSimpleQueryMode() throws Exception {
    final String type = "Issue11483Jdbc";
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("sslmode", "disable");
    props.setProperty("preferQueryMode", "simple");
    try (final Connection conn = DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
        final Statement st = conn.createStatement()) {
      st.execute("CREATE DOCUMENT TYPE " + type + " IF NOT EXISTS");
      assertThat(st.execute("INSERT INTO " + type + " SET id = 1; INSERT INTO " + type + " SET id = 2; SELECT count(*) AS c FROM " + type))
          .as("the first result is the INSERT's update count").isFalse();
      assertThat(st.getUpdateCount()).isEqualTo(1);
      assertThat(st.getMoreResults()).isFalse();
      assertThat(st.getUpdateCount()).isEqualTo(1);
      assertThat(st.getMoreResults()).isTrue();
      try (final ResultSet rs = st.getResultSet()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getLong(1)).isEqualTo(2L);
      }
    }
    assertThat(idsOf(type)).containsExactly(1, 2);
  }

  private static void assertStreamStillInStep(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "SELECT 1 AS probe");
    final List<WireMessage> probe = readUntilReadyForQuery(in);
    assertThat(messageTypesOf(probe)).containsExactly('T', 'D', 'C', 'Z');
    assertThat(in.available()).as("nothing is left unread behind the probe's ReadyForQuery").isZero();
  }

  private interface WireExchange {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void withConnection(final WireExchange exchange) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> exchange.run(out, in));
    }
  }

  private void createType(final DataOutputStream out, final DataInputStream in, final String typeName) throws Exception {
    sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + typeName + " IF NOT EXISTS");
    assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
  }

  private static List<String> commandTagsOf(final List<WireMessage> messages) {
    final List<String> tags = new ArrayList<>();
    for (final WireMessage message : messages) {
      if (message.type() != 'C')
        continue;
      final byte[] body = message.body();
      int end = 0;
      while (end < body.length && body[end] != 0)
        end++;
      tags.add(new String(body, 0, end, StandardCharsets.UTF_8));
    }
    return tags;
  }

  /** The first column of every DataRow among {@code messages}, as text. */
  private static List<String> dataRowValuesOf(final List<WireMessage> messages) {
    final List<String> values = new ArrayList<>();
    for (final WireMessage message : messages)
      if (message.type() == 'D') {
        final ByteBuffer row = ByteBuffer.wrap(message.body());
        row.getShort(); // column count
        final byte[] value = new byte[row.getInt()];
        row.get(value);
        values.add(new String(value, StandardCharsets.UTF_8));
      }
    return values;
  }

  private List<Integer> idsOf(final String typeName) {
    final Database database = getServerDatabase(0, getDatabaseName());
    final List<Integer> ids = new ArrayList<>();
    database.query("sql", "SELECT id FROM " + typeName + " ORDER BY id")
        .forEachRemaining(row -> ids.add(row.getProperty("id")));
    return ids;
  }
}
