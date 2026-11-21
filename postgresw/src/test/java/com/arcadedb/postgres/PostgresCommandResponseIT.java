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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.show;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * What the server answers a command with over the PostgreSQL wire protocol: the {@code CommandComplete} tag of a write
 * (with or without rows), DDL and acknowledgement-only statements on every Describe path, {@code RETURNING}, and a
 * simple query carrying several statements.
 */
class PostgresCommandResponseIT extends PostgresWireProtocolTestBase {

  /*
   * Regression test for issue #11017: a write with no RETURN clause was answered with a result set and tagged with the
   * result-row count instead of the affected-row count on three of the four protocol paths (simple query, extended with
   * Describe('P'), extended with no Describe). Only the path with a statement-level Describe('S') was right. The four
   * paths must agree: no RowDescription, no DataRow, and a CommandComplete carrying the number of records changed.
   */
  private static final int SEED_ROWS = 4;

  // Regression: #11017
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

          seedRowlessWrites(out, in);
          sendSimpleQuery(out, sql);
          final List<WireMessage> simple = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(simple)).as("simple: " + sql).containsExactly('C', 'Z');
          assertThat(rowlessWriteCommandTagOf(simple)).as("simple: " + sql).isEqualTo(expectedTag);

          seedRowlessWrites(out, in);
          sendParse(out, statementName, sql);
          sendDescribe(out, 'S', statementName);
          sendBind(out, "", statementName);
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> describeStatement = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(describeStatement)).as("Describe('S'): " + sql).containsExactly('1', 't', 'n', '2', 'C', 'Z');
          assertThat(rowlessWriteCommandTagOf(describeStatement)).as("Describe('S'): " + sql).isEqualTo(expectedTag);

          seedRowlessWrites(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> noDescribe = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(noDescribe)).as("no Describe: " + sql).containsExactly('1', '2', 'C', 'Z');
          assertThat(rowlessWriteCommandTagOf(noDescribe)).as("no Describe: " + sql).isEqualTo(expectedTag);

          seedRowlessWrites(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendDescribe(out, 'P', "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> describePortal = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(describePortal)).as("Describe('P'): " + sql).containsExactly('1', '2', 'n', 'C', 'Z');
          assertThat(rowlessWriteCommandTagOf(describePortal)).as("Describe('P'): " + sql).isEqualTo(expectedTag);
        }
      });
    }
  }

  // Regression: #11017
  @Test
  @DisplayName("[#11017] a write WITH a RETURN keeps its result set and is tagged with the rows it returned")
  void writeWithReturnKeepsItsRows() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        seedRowlessWrites(out, in);
        sendSimpleQuery(out, "DELETE FROM W11017 RETURN BEFORE WHERE id <= 2");
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).contains('T', 'D');
        assertThat(rowlessWriteCommandTagOf(response)).isEqualTo("DELETE 2");
      });
    }
  }

  // Regression: #11017
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

  private void seedRowlessWrites(final DataOutputStream out, final DataInputStream in) throws Exception {
    simple(out, in, "CREATE DOCUMENT TYPE W11017 IF NOT EXISTS");
    simple(out, in, "DELETE FROM W11017");
    for (int i = 1; i <= SEED_ROWS; i++)
      simple(out, in, "INSERT INTO W11017 SET id = " + i + ", tag = 'a'");
  }

  private List<WireMessage> simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    return readUntilReadyForQuery(in);
  }

  private static String rowlessWriteCommandTagOf(final List<WireMessage> messages) {
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

  /*
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
  // Regression: #11321
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

  // Regression: #11321
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

          seedCommandTags(out, in);
          sendSimpleQuery(out, sql);
          assertThat(commandTagOf(readUntilReadyForQuery(in))).as("simple: " + sql).isEqualTo(expectedTag);

          seedCommandTags(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendDescribe(out, 'P', "");
          sendExecute(out, "");
          sendSync(out);
          assertThat(commandTagOf(readUntilReadyForQuery(in))).as("Describe('P'): " + sql).isEqualTo(expectedTag);

          seedCommandTags(out, in);
          sendParse(out, "", sql);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          assertThat(commandTagOf(readUntilReadyForQuery(in))).as("no Describe: " + sql).isEqualTo(expectedTag);
        }
      });
    }
  }

  // Regression: #11321
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

  private void seedCommandTags(final DataOutputStream out, final DataInputStream in) throws Exception {
    simpleWithoutError(out, in, "CREATE DOCUMENT TYPE C11313 IF NOT EXISTS");
    simpleWithoutError(out, in, "CREATE VERTEX TYPE V11313 IF NOT EXISTS");
    simpleWithoutError(out, in, "DELETE FROM C11313");
    for (int i = 1; i <= 4; i++)
      simpleWithoutError(out, in, "INSERT INTO C11313 SET i = " + i);
  }

  private void simpleWithoutError(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
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

  private Database database() {
    return getServerDatabase(0, getDatabaseName());
  }

  /*
   * Issue #11385: the #11312 fix answered every {@code DDLStatement} as PostgreSQL answers CREATE TABLE, and DEFINE FUNCTION, DELETE
   * FUNCTION and SLEEP, which are not {@code DDLStatement} subclasses, kept being answered like a query: a RowDescription, a DataRow and
   * a tag with no count on the simple path, "A result was returned when none was expected" from pgjdbc's {@code executeUpdate()}, and
   * after a Describe('S') answered NoData, a refusal with {@code 0A000} of a statement the engine had already run. They are answered
   * like a schema change now, on all four protocol paths, decided by {@code Statement.returnsAcknowledgementOnly()}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String[] PATHS = { "Simple", "DescS", "NoDesc", "DescP" };

  // Regression: #11385
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

  // Regression: #11385
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

  private boolean functionExists(final String library, final String function) {
    return database().getSchema().hasFunctionLibrary(library) && database().getSchema().getFunctionLibrary(library).hasFunction(function);
  }

  /*
   * Issue #11392 through the PostgreSQL JDBC driver: {@code prepareStatement(sql, RETURN_GENERATED_KEYS)} and
   * {@code prepareStatement(sql, new String[] {...})} append {@code RETURNING *} / {@code RETURNING "col"} to the statement, which
   * the SQL grammar refused with {@code 42601}, so Hibernate IDENTITY, Spring {@code KeyHolder} and jOOQ could not insert. And an
   * index equivalent to one under another name answered {@code 22023} where the same-name duplicate answers {@code 42P07}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11392
  @Test
  void returnGeneratedKeysAnswersTheInsertedRow() throws Exception {
    try (final Connection connection = open()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Gk");
      }

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Gk (name, n) VALUES (?, ?)",
          Statement.RETURN_GENERATED_KEYS)) {
        insert.setString(1, "first");
        insert.setInt(2, 1);
        assertThat(insert.executeUpdate()).isEqualTo(1);
        try (final ResultSet keys = insert.getGeneratedKeys()) {
          assertThat(keys.next()).isTrue();
          assertThat(keys.getString("name")).isEqualTo("first");
        }
      }

      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Gk (name, n) VALUES (?, ?)",
          new String[] { "n" })) {
        insert.setString(1, "second");
        insert.setInt(2, 2);
        assertThat(insert.executeUpdate()).isEqualTo(1);
        try (final ResultSet keys = insert.getGeneratedKeys()) {
          assertThat(keys.next()).isTrue();
          assertThat(keys.getInt("n")).isEqualTo(2);
        }
      }

      // The identity ArcadeDB generates for a record is its RID: asked for by name, it is the RID of the record inserted
      final String rid;
      try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Gk (name, n) VALUES (?, ?)",
          new String[] { "@rid" })) {
        insert.setString(1, "third");
        insert.setInt(2, 3);
        assertThat(insert.executeUpdate()).isEqualTo(1);
        try (final ResultSet keys = insert.getGeneratedKeys()) {
          assertThat(keys.next()).isTrue();
          rid = keys.getString("@rid");
        }
      }
      assertThat(rid).startsWith("#");
      try (final Statement statement = connection.createStatement();
          final ResultSet byRid = statement.executeQuery("SELECT name FROM " + rid)) {
        assertThat(byRid.next()).isTrue();
        assertThat(byRid.getString("name")).isEqualTo("third");
      }

      // the same quoted attribute in a query is the attribute too, not the string "@rid"
      try (final Statement statement = connection.createStatement();
          final ResultSet byQuotedRid = statement.executeQuery("SELECT \"@rid\" FROM Gk WHERE name = 'third'")) {
        assertThat(byQuotedRid.next()).isTrue();
        assertThat(byQuotedRid.getString(1)).isEqualTo(rid);
      }

      try (final Statement statement = connection.createStatement();
          final ResultSet count = statement.executeQuery("SELECT count(*) AS c FROM Gk")) {
        assertThat(count.next()).isTrue();
        assertThat(count.getLong("c")).isEqualTo(3L);
      }
    }
  }

  // Regression: #11392
  @Test
  void anEquivalentIndexUnderAnotherNameIsDuplicateTable() throws Exception {
    try (final Connection connection = open(); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Eqv");
      statement.execute("CREATE PROPERTY Eqv.a STRING");
      statement.execute("CREATE INDEX eqvA ON Eqv (a) NOTUNIQUE");

      final SQLException refused = catchThrowableOfType(SQLException.class,
          () -> statement.execute("CREATE INDEX eqvB ON Eqv (a) NOTUNIQUE"));
      assertThat(refused != null).as("the equivalent index is refused").isTrue();
      assertThat(refused.getSQLState()).isEqualTo("42P07");
    }
  }

  private Connection open() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
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
  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a SELECT that writes through a library function is rolled back with the block too")
  void selectCallingAWritingFunctionIsRolledBack() throws Exception {
    final String type = "Issue11483Function";
    withConnection((out, in) -> {
      createType(out, in, type);
      getServerDatabase(0, getDatabaseName()).command("sql",
          "DEFINE FUNCTION fw11483.add \"INSERT INTO " + type + " SET id = 1\" LANGUAGE sql");

      // isIdempotent() reads the SELECT's own syntax and cannot see the write inside the function it calls
      sendSimpleQuery(out, "SELECT `fw11483.add`() AS r; SELEKT bogus");
      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('T', 'D', 'C', 'E', 'Z');
      assertThat(readyForQueryStatusOf(answers)).isEqualTo('I');
      assertStreamStillInStep(out, in);
    });
    assertThat(idsOf(type)).as("the function's write belonged to the failed message").isEmpty();
  }

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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

  // Regression: #11483
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
