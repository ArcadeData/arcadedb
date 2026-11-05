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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
 * Issue #11321 (#11312, #11313): the CommandComplete tag was read off the statement's TEXT, and a schema change was answered with a
 * result set.
 * <ul>
 * <li>#11312: a schema change (CREATE TYPE, CREATE PROPERTY, CREATE INDEX, ALTER TYPE, DROP TYPE...) answered a row on every path
 * and an empty tag, so pgjdbc's {@code executeUpdate()} threw "A result was returned when none was expected", a statement
 * described with Describe('S') ran and was then refused with {@code 0A000}, and a client that described nothing received a
 * DataRow with no RowDescription in front of it. It is answered as PostgreSQL answers CREATE TABLE now, on all four paths.</li>
 * <li>#11313: a write with a leading comment, a line break or a doubled space between its keywords was tagged {@code ""}, and
 * pgjdbc reported 0 rows for rows it wrote. The tag comes from the parsed statement now.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11321PostgresCommandTagIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#11312] a schema change is answered with no result set and its own tag on all four protocol paths, and runs once")
  void everyPathAnswersASchemaChangeAsPostgresAnswersCreateTable() throws Exception {
    // %s is replaced by a different suffix on each path: a schema change is not repeatable
    final String[][] cases = {
        { "CREATE DOCUMENT TYPE Ddl%s", "CREATE DOCUMENT TYPE" },
        { "CREATE VERTEX TYPE Vtx%s", "CREATE VERTEX TYPE" },
        { "CREATE PROPERTY Ddl%s.name STRING", "CREATE PROPERTY" },
        { "CREATE INDEX ON Ddl%s (name) NOTUNIQUE", "CREATE INDEX" },
        { "ALTER TYPE Ddl%s CUSTOM owner = 'x'", "ALTER TYPE" },
        { "REBUILD INDEX `Ddl%s[name]`", "REBUILD INDEX" },
        { "DROP TYPE Vtx%s", "DROP TYPE" } };

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
          assertThat(messageTypesOf(noDescribe)).as("no Describe: " + noDescribeSql + " -> " + describe(noDescribe)).containsExactly('1', '2', 'C', 'Z');
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

        // each path ran its statement exactly once: the types it created are there, and the ones it dropped are not
        for (final String path : new String[] { "Simple", "DescS", "NoDesc", "DescP" }) {
          assertThat(database().getSchema().existsType("Ddl" + path)).as(path).isTrue();
          assertThat(database().getSchema().getType("Ddl" + path).getCustomValue("owner")).as(path).isEqualTo("x");
          assertThat(database().getSchema().getType("Ddl" + path).existsProperty("name")).as(path).isTrue();
          assertThat(database().getSchema().existsType("Vtx" + path)).as(path).isFalse();
        }
      });
    }
  }

  @Test
  @DisplayName("[#11313] a leading comment, a line break or a doubled space does not change the tag of a statement")
  void theTagDoesNotDependOnTheSpellingOfTheStatement() throws Exception {
    final String[][] cases = {
        { "INSERT INTO C11313 SET i = 100", "INSERT 0 1" },
        { "/* hint */ INSERT INTO C11313 SET i = 100", "INSERT 0 1" },
        { "-- c\nINSERT INTO C11313 SET i = 100", "INSERT 0 1" },
        { "INSERT  INTO C11313 SET i = 100", "INSERT 0 1" },
        { "INSERT\nINTO C11313 SET i = 100", "INSERT 0 1" },
        { "insert into C11313 set i = 100", "INSERT 0 1" },
        { "/* h */ CREATE VERTEX V11313 SET i = 1", "INSERT 0 1" },
        { "/* h */ UPDATE C11313 SET i = 7 WHERE i <= 2", "UPDATE 2" },
        { "/* h */ DELETE FROM C11313 WHERE i = 3", "DELETE 1" },
        { "/* h */ SELECT i FROM C11313 WHERE i >= 1", "SELECT 4" },
        { "/* h */\n\n  SELECT  i FROM C11313 WHERE i = 4", "SELECT 1" } };

    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
        for (final String[] c : cases) {
          final String sql = c[0];
          final String expectedTag = c[1];

          seed(out, in);
          sendSimpleQuery(out, sql);
          assertThat(commandTagOf(readUntilReadyForQuery(in))).as("simple: " + sql).isEqualTo(expectedTag);

          seed(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendDescribe(out, 'P', "");
          sendExecute(out, "");
          sendSync(out);
          assertThat(commandTagOf(readUntilReadyForQuery(in))).as("Describe('P'): " + sql).isEqualTo(expectedTag);

          seed(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          assertThat(commandTagOf(readUntilReadyForQuery(in))).as("no Describe: " + sql).isEqualTo(expectedTag);
        }
      });
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "extended", "simple" })
  @DisplayName("[#11312, #11313] pgjdbc's executeUpdate() takes a schema change, and counts the rows of a commented write")
  void jdbcSeesSchemaChangesAndCommentedWritesAsPostgresAnswersThem(final String queryMode) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    props.setProperty("preferQueryMode", queryMode);
    final String type = "J11321" + queryMode;
    try (final Connection conn = DriverManager.getConnection(getServerPostgresJdbcUrl(), props); final Statement st = conn.createStatement()) {
      assertThat(st.executeUpdate("CREATE DOCUMENT TYPE " + type)).isZero();
      assertThat(st.executeUpdate("CREATE PROPERTY " + type + ".id INTEGER")).isZero();
      assertThat(st.executeUpdate("CREATE INDEX ON " + type + " (id) NOTUNIQUE")).isZero();
      assertThat(st.executeUpdate("ALTER TYPE " + type + " CUSTOM owner = 'jdbc'")).isZero();
      assertThat(st.execute("CREATE DOCUMENT TYPE " + type + "Other")).isFalse();
      assertThat(st.getUpdateCount()).isZero();
      assertThat(st.executeUpdate("DROP TYPE " + type + "Other")).isZero();

      assertThat(st.executeUpdate("/* hint */ INSERT INTO " + type + " SET id = 1")).isEqualTo(1);
      assertThat(st.executeUpdate("-- c\nINSERT INTO " + type + " SET id = 2")).isEqualTo(1);
      assertThat(st.executeUpdate("INSERT  INTO " + type + " SET id = 3")).isEqualTo(1);
      assertThat(st.executeUpdate("/* h */ UPDATE " + type + " SET tag = 'x' WHERE id <= 3")).isEqualTo(3);

      // bound parameters on the extended protocol only: the simple mode inlines them as '10'::int4, a cast SQL does not parse
      if ("extended".equals(queryMode)) {
        try (final PreparedStatement ps = conn.prepareStatement("/* h */ INSERT INTO " + type + " SET id = ?")) {
          for (int i = 10; i < 13; i++) {
            ps.setInt(1, i);
            ps.addBatch();
          }
          assertThat(ps.executeBatch()).containsExactly(1, 1, 1);
        }
        try (final PreparedStatement ps = conn.prepareStatement("/* h */ UPDATE " + type + " SET tag = 'y' WHERE id >= ?")) {
          ps.setInt(1, 10);
          assertThat(ps.executeUpdate()).isEqualTo(3);
        }
      } else {
        st.addBatch("/* h */ INSERT INTO " + type + " SET id = 10");
        st.addBatch("-- c\nINSERT INTO " + type + " SET id = 11");
        st.addBatch("INSERT  INTO " + type + " SET id = 12");
        assertThat(st.executeBatch()).containsExactly(1, 1, 1);
      }
      try (final ResultSet rs = st.executeQuery("/* h */ SELECT count(*) AS n FROM " + type)) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getLong("n")).isEqualTo(6);
      }
    }
  }

  private void seed(final DataOutputStream out, final DataInputStream in) throws Exception {
    simple(out, in, "CREATE DOCUMENT TYPE C11313 IF NOT EXISTS");
    simple(out, in, "CREATE VERTEX TYPE V11313 IF NOT EXISTS");
    simple(out, in, "DELETE FROM C11313");
    for (int i = 1; i <= 4; i++)
      simple(out, in, "INSERT INTO C11313 SET i = " + i);
  }

  private void simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    final List<WireMessage> answer = readUntilReadyForQuery(in);
    assertThat(messageTypesOf(answer)).as(query + " -> " + describe(answer)).doesNotContain('E');
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

  private Database database() {
    return getServerDatabase(0, getDatabaseName());
  }
}
