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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for four Postgres-wire issues around {@code SET} and Execute:
 * <ul>
 *   <li>#8241: a {@code SET} of a reported parameter sent no {@code ParameterStatus}, and startup announced only three
 *   of them;</li>
 *   <li>#8242: a {@code SET} inside a rolled-back block was not undone, {@code SET LOCAL} outlived its transaction, and
 *   {@code RESET} was not recognized;</li>
 *   <li>#8244: Execute answered with a {@code RowDescription} the client never asked for, which broke every pgjdbc
 *   connection on the execution that promoted a prepared statement;</li>
 *   <li>#8306: a {@code SET} on the simple protocol answered a one-row "Setting ignored" table instead of the bare
 *   {@code CommandComplete SET}.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8241Issue8242Issue8244Issue8306SetAndExecuteIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#8241] startup announces every reported parameter, the startup packet's own values included")
  void startupReportsEveryReportedParameter() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      for (final String s : new String[] { "user", "root", "database", getDatabaseName(), "DateStyle", "ISO, DMY", "TimeZone",
          "Europe/Rome", "application_name", "issue8241" })
        writeCString(body, s);
      body.write(0);
      final byte[] bodyBytes = body.toByteArray();
      out.writeInt(4 + 4 + bodyBytes.length);
      out.writeInt(196608); // protocol version 3.0
      out.write(bodyBytes);
      out.flush();
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);

      final Map<String, String> reported = parameterStatusesOf(readUntilReadyForQuery(in));
      assertThat(reported).containsEntry("server_version", PostgresNetworkExecutor.PG_SERVER_VERSION)
          .containsEntry("server_encoding", "UTF8")
          .containsEntry("client_encoding", "UTF8")
          .containsEntry("DateStyle", "ISO, DMY")
          .containsEntry("TimeZone", "Europe/Rome")
          .containsEntry("application_name", "issue8241")
          .containsEntry("integer_datetimes", "on")
          .containsEntry("standard_conforming_strings", "on")
          .containsEntry("IntervalStyle", "postgres")
          .containsEntry("is_superuser", "on");
    }
  }

  @Test
  @DisplayName("[#8241] a SET of a reported parameter is answered with a ParameterStatus carrying what SHOW answers")
  void setOfAReportedParameterSendsParameterStatus() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET application_name = 'issue8241'");
        List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(parameterStatusesOf(response)).containsExactly(Map.entry("application_name", "issue8241"));
        assertThat(messageTypesOf(response)).as("ParameterStatus right before the ReadyForQuery").endsWith('S', 'Z');

        // The value SHOW answers, never the raw SET text: pgjdbc closes the connection on a DateStyle that does not
        // start with ISO.
        sendSimpleQuery(out, "SET DateStyle = 'SQL, DMY'");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).containsExactly(Map.entry("DateStyle", "ISO, DMY"));

        // Over the extended protocol: reported before the ReadyForQuery the Sync answers.
        sendParse(out, "", "SET TimeZone TO 'Europe/Rome'");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        response = readUntilReadyForQuery(in);
        assertThat(parameterStatusesOf(response)).containsExactly(Map.entry("TimeZone", "Europe/Rome"));
        assertThat(messageTypesOf(response)).containsExactly('1', '2', 'C', 'S', 'Z');

        // A value that does not change is not reported, and neither is a parameter PostgreSQL does not report.
        sendSimpleQuery(out, "SET client_encoding = 'LATIN1'");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).as("client_encoding still answers UTF8").isEmpty();
        sendSimpleQuery(out, "SET search_path TO x");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).isEmpty();
        sendSimpleQuery(out, "SET application_name = 'issue8241'");
        assertThat(parameterStatusesOf(readUntilReadyForQuery(in))).as("unchanged").isEmpty();
      });
    }
  }

  @Test
  @DisplayName("[#8306] a SET on the simple protocol answers the bare CommandComplete SET, like the extended protocol")
  void simpleQuerySetAnswersNoRows() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SET search_path TO x");
        List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("SET");

        sendSimpleQuery(out, "SET LOCAL search_path TO y");
        response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("SET");

        sendSimpleQuery(out, "RESET search_path");
        response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("RESET");
      });
    }
  }

  @Test
  @DisplayName("[#8242] a SET inside a rolled-back block is undone, and the ROLLBACK reports the restored value")
  void setInsideARolledBackBlockIsUndone() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "SET search_path TO before");
        simple(out, in, "BEGIN");
        simple(out, in, "SET search_path TO x");
        assertThat(parameterStatusesOf(simple(out, in, "SET application_name = 'inside'"))).containsEntry("application_name", "inside");
        assertThat(show(out, in, "search_path")).as("visible inside the block").isEqualTo("x");
        final List<WireMessage> rollback = simple(out, in, "ROLLBACK");
        assertThat(parameterStatusesOf(rollback)).as("the rollback restored a reported parameter")
            .containsExactly(Map.entry("application_name", ""));
        assertThat(show(out, in, "search_path")).isEqualTo("before");

        // A committed block keeps its SET.
        simple(out, in, "BEGIN");
        simple(out, in, "SET search_path TO committed");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "search_path")).isEqualTo("committed");

        // An aborted block: its SETs go with the ROLLBACK that ends it.
        simple(out, in, "BEGIN");
        simple(out, in, "SET search_path TO aborted");
        assertThat(messageTypesOf(simple(out, in, "SELECT FROM NoSuchTypeIssue8242"))).contains('E');
        simple(out, in, "ROLLBACK");
        assertThat(show(out, in, "search_path")).isEqualTo("committed");
      });
    }
  }

  @Test
  @DisplayName("[#8242] SET LOCAL lasts until the end of its transaction, committed or not")
  void setLocalEndsWithItsTransaction() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "BEGIN");
        simple(out, in, "SET LOCAL search_path TO y");
        assertThat(show(out, in, "search_path")).isEqualTo("y");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "search_path")).as("SET LOCAL outlived its transaction").isEmpty();

        // Outside a block the statement is its own transaction, so a SET LOCAL has no lasting effect.
        simple(out, in, "SET LOCAL search_path TO z");
        assertThat(show(out, in, "search_path")).isEmpty();

        // A SET after a SET LOCAL of the same parameter supersedes it and survives the commit...
        simple(out, in, "BEGIN");
        simple(out, in, "SET LOCAL search_path TO local");
        simple(out, in, "SET search_path TO session");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "search_path")).isEqualTo("session");

        // ...while a SET LOCAL after a SET masks it only until the end of the transaction.
        simple(out, in, "BEGIN");
        simple(out, in, "SET search_path TO second");
        simple(out, in, "SET LOCAL search_path TO local");
        assertThat(show(out, in, "search_path")).isEqualTo("local");
        simple(out, in, "COMMIT");
        assertThat(show(out, in, "search_path")).isEqualTo("second");

        // A reported parameter set LOCAL is reported when set and again when it ends.
        simple(out, in, "BEGIN");
        assertThat(parameterStatusesOf(simple(out, in, "SET LOCAL TimeZone TO 'Asia/Tokyo'"))).containsEntry("TimeZone", "Asia/Tokyo");
        assertThat(parameterStatusesOf(simple(out, in, "COMMIT"))).containsExactly(Map.entry("TimeZone", "UTC"));
      });
    }
  }

  @Test
  @DisplayName("[#8242] RESET <name> and RESET ALL restore the reset value, on both protocols")
  void resetIsRecognized() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "SET search_path TO x");
        assertThat(messageTypesOf(simple(out, in, "RESET search_path"))).doesNotContain('E');
        assertThat(show(out, in, "search_path")).isEmpty();

        simple(out, in, "SET search_path TO x");
        simple(out, in, "SET TIME ZONE 'Europe/Rome'");
        assertThat(show(out, in, "timezone")).isEqualTo("Europe/Rome");
        final List<WireMessage> resetAll = simple(out, in, "RESET ALL");
        assertThat(messageTypesOf(resetAll)).doesNotContain('E');
        assertThat(parameterStatusesOf(resetAll)).containsExactly(Map.entry("TimeZone", "UTC"));
        assertThat(show(out, in, "search_path")).isEmpty();
        assertThat(show(out, in, "timezone")).isEqualTo("UTC");

        simple(out, in, "SET search_path TO x");
        sendParse(out, "", "RESET search_path");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('1', '2', 'C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("RESET");
        assertThat(show(out, in, "search_path")).isEmpty();
      });
    }
  }

  @Test
  @DisplayName("[#8242] a SET in an extended-protocol pipeline that fails is discarded with it at the Sync")
  void setInAFailedPipelineIsDiscarded() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "", "SET search_path TO pipelined");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendParse(out, "", "SELECT FROM NoSuchTypeIssue8242");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('E');
        assertThat(show(out, in, "search_path")).isEmpty();

        sendParse(out, "", "SET search_path TO pipelined");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        assertThat(show(out, in, "search_path")).isEqualTo("pipelined");
      });
    }
  }

  @Test
  @DisplayName("[#8244] Bind/Execute/Sync with no Describe is answered with no RowDescription")
  void executeWithoutDescribeSendsNoRowDescription() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE Probe8244 IF NOT EXISTS");
        simple(out, in, "INSERT INTO Probe8244 SET tag = 'a'");

        sendParse(out, "Sx", "SELECT tag FROM Probe8244");
        sendBind(out, "", "Sx");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("Describe('P') is answered with the RowDescription")
            .containsExactly('1', '2', 'T', 'D', 'C', 'Z');

        for (int i = 0; i < 2; i++) {
          sendBind(out, "", "Sx");
          sendExecute(out, "");
          sendSync(out);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("no Describe, so no RowDescription")
              .containsExactly('2', 'D', 'C', 'Z');
        }

        // A statement described once, by Describe('S'), is not described again either.
        sendParse(out, "Sy", "SELECT tag FROM Probe8244");
        sendDescribe(out, 'S', "Sy");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'T', 'Z');
        sendBind(out, "", "Sy");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'D', 'C', 'Z');
      });
    }
  }

  @ParameterizedTest(name = "prepareThreshold={0}")
  @ValueSource(ints = { 1, 5 })
  @DisplayName("[#8244] pgjdbc runs a row-returning PreparedStatement past its prepareThreshold")
  void pgjdbcRunsAPreparedStatementPastItsPrepareThreshold(final int prepareThreshold) throws Exception {
    try (final Connection connection = openJdbcConnection(prepareThreshold)) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Jdbc8244 IF NOT EXISTS");
      }

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Jdbc8244 SET tag = ?")) {
        for (int i = 0; i < 8; i++) {
          insert.setString(1, "t" + i);
          insert.execute();
        }
      }

      try (final PreparedStatement select = connection.prepareStatement("SELECT tag FROM Jdbc8244 WHERE tag = ?")) {
        for (int i = 0; i < 8; i++) {
          select.setString(1, "t" + i);
          try (final ResultSet rs = select.executeQuery()) {
            assertThat(rs.next()).as("execution %d", i + 1).isTrue();
            assertThat(rs.getString("tag")).isEqualTo("t" + i);
          }
        }
      }

      // SET through pgjdbc: the ParameterStatus it reads back must satisfy its own DateStyle check.
      try (final Statement statement = connection.createStatement()) {
        statement.execute("SET DateStyle = 'ISO, DMY'");
        try (final ResultSet rs = statement.executeQuery("SHOW DateStyle")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString(1)).isEqualTo("ISO, DMY");
        }
      }
    }
  }

  private List<WireMessage> simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    return readUntilReadyForQuery(in);
  }

  private static Map<String, String> parameterStatusesOf(final List<WireMessage> messages) {
    final Map<String, String> statuses = new LinkedHashMap<>();
    for (final WireMessage message : messages)
      if (message.type() == 'S') {
        final byte[] body = message.body();
        int end = 0;
        while (body[end] != 0)
          end++;
        final String name = new String(body, 0, end, StandardCharsets.UTF_8);
        final int start = end + 1;
        end = start;
        while (body[end] != 0)
          end++;
        statuses.put(name, new String(body, start, end - start, StandardCharsets.UTF_8));
      }
    return statuses;
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'C')
        return new String(message.body(), 0, message.body().length - 1, StandardCharsets.UTF_8);
    throw new AssertionError("no CommandComplete among " + messageTypesOf(messages));
  }

  private Connection openJdbcConnection(final int prepareThreshold) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("prepareThreshold", String.valueOf(prepareThreshold));
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
