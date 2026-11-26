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
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendParseWithTypes;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #11838: a prepared statement described the columns of schemaless properties as varchar (text[] for a list), so a
 * client that prepares a statement - psycopg from its 6th execution, pgjdbc from its 5th - read {@code true} as {@code 't'},
 * {@code 42} as {@code '42'} and {@code [1, 2, 3]} as {@code ['1', '2', '3']}, where the same statement unprepared returned the
 * typed values. And a uuid parameter could not be bound at all.
 */
class Issue11838PreparedSchemalessTypesIT extends PostgresWireProtocolTestBase {
  private static final String   TYPE  = "T11838";
  private static final String   QUERY = "SELECT v FROM " + TYPE + " WHERE name = $1";
  private static final String[] NAMES = { "bool", "int", "double", "list", "date" };
  private static final UUID     ID    = UUID.fromString("0f8fad5b-d9cb-469f-a165-70867728950e");

  @Test
  void aNamedStatementDescribesEachPortalAsTheUnnamedOneDoes() throws Exception {
    createRecords();
    withConnection((out, in) -> {
      // libpq's PQprepare: Parse and Sync, nothing else
      sendParse(out, "S1", QUERY);
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 'Z');

      for (final String name : NAMES) {
        final List<WireMessage> unprepared = describeAndExecute(out, in, "", name);
        // libpq's PQexecPrepared: Bind, Describe('P'), Execute and Sync, on every execution
        final List<WireMessage> prepared = describeAndExecute(out, in, "S1", name);

        assertThat(columnTypeOid(prepared)).as(name + " prepared").isEqualTo(columnTypeOid(unprepared))
            .isNotEqualTo(PostgresType.VARCHAR.code).isNotEqualTo(PostgresType.ARRAY_TEXT.code);
        assertThat(firstDataRowValue(prepared)).as(name + " prepared").isEqualTo(firstDataRowValue(unprepared));
      }

      assertThat(columnTypeOid(describeAndExecute(out, in, "S1", "bool"))).isEqualTo(PostgresType.BOOLEAN.code);
      assertThat(columnTypeOid(describeAndExecute(out, in, "S1", "int"))).isEqualTo(PostgresType.INTEGER.code);
      assertThat(columnTypeOid(describeAndExecute(out, in, "S1", "double"))).isEqualTo(PostgresType.DOUBLE.code);
      assertThat(columnTypeOid(describeAndExecute(out, in, "S1", "list"))).isEqualTo(PostgresType.ARRAY_INT.code);
      assertThat(firstDataRowValue(describeAndExecute(out, in, "S1", "bool"))).isEqualTo("t");
      assertThat(firstDataRowValue(describeAndExecute(out, in, "S1", "list"))).isEqualTo("{1,2,3}");
    });
  }

  @Test
  void aStatementPreparedWhileItRunsKeepsOneLayoutForRowsOfAnyType() throws Exception {
    createRecords();
    withConnection((out, in) -> {
      // pgjdbc from its prepareThreshold-th execution, Npgsql's auto-prepare and node-postgres: the statement is parsed in the
      // exchange that first runs it, and pgjdbc and Npgsql decode every later execution with the RowDescription of that first
      // portal, so that description must hold for whatever rows a later portal returns
      sendParse(out, "S1", QUERY);
      sendBind(out, "", "S1", "bool");
      sendDescribe(out, 'P', "");
      sendExecute(out, "");
      sendSync(out);
      List<WireMessage> messages = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(messages)).containsExactly('1', '2', 'T', 'D', 'C', 'Z');
      assertThat(columnTypeOid(messages)).isEqualTo(PostgresType.VARCHAR.code);
      assertThat(firstDataRowValue(messages)).isEqualTo("t");

      // later executions without a Describe of their own, rows of other types included: never refused (a 0A000 here failed
      // Npgsql, which does not prepare again), each value sent as the text the layout announced
      for (int round = 0; round < 2; round++)
        for (final String[] expected : new String[][] { { "int", "42" }, { "double", "2.5" }, { "bool", "t" } }) {
          messages = executeWithoutDescribe(out, in, expected[0]);
          assertThat(messageTypesOf(messages)).as(expected[0]).containsExactly('2', 'D', 'C', 'Z');
          assertThat(firstDataRowValue(messages)).as(expected[0]).isEqualTo(expected[1]);
        }
      messages = executeWithoutDescribe(out, in, "list");
      assertThat(messageTypesOf(messages)).containsExactly('2', 'D', 'C', 'Z');

      // a later portal described all the same is answered with the same layout: such a statement keeps one for life
      assertThat(columnTypeOid(describeAndExecute(out, in, "S1", "int"))).isEqualTo(PostgresType.VARCHAR.code);
    });
  }

  @Test
  void aStatementDescribedBeforeItRunsKeepsThatLayoutForRowsOfAnyType() throws Exception {
    createRecords();
    withConnection((out, in) -> {
      // pgx (QueryExecModeCacheStatement), Npgsql's Prepare() and asyncpg: Parse and Describe('S') once, then Bind and Execute
      // with no Describe, every execution decoded with the statement's description. The parameter is unbound when it is
      // described, so nothing tells which record, of which type, an execution will read: the description holds for all
      sendParse(out, "S2", QUERY);
      sendDescribe(out, 'S', "S2");
      sendSync(out);
      List<WireMessage> messages = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(messages)).containsExactly('1', 't', 'T', 'Z');
      assertThat(columnTypeOid(messages)).isEqualTo(PostgresType.VARCHAR.code);

      for (final String[] expected : new String[][] { { "bool", "t" }, { "int", "42" }, { "double", "2.5" }, { "bool", "t" } }) {
        sendBind(out, "", "S2", expected[0]);
        sendExecute(out, "");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).as(expected[0]).containsExactly('2', 'D', 'C', 'Z');
        assertThat(firstDataRowValue(messages)).as(expected[0]).isEqualTo(expected[1]);
      }

      // pgconn.ExecPrepared and libpq's PQexecPrepared describe every portal of it: each one is answered from its own rows
      assertThat(columnTypeOid(describeAndExecute(out, in, "S2", "int"))).isEqualTo(PostgresType.INTEGER.code);
      assertThat(columnTypeOid(describeAndExecute(out, in, "S2", "bool"))).isEqualTo(PostgresType.BOOLEAN.code);
    });
  }

  @Test
  void jdbcNeverFailsWhenAPreparedStatementsRowsChangeType() throws Exception {
    createRecords();
    for (final boolean autoCommit : new boolean[] { true, false })
      try (final Connection connection = openJdbcConnection();
          final PreparedStatement select = connection.prepareStatement("SELECT v FROM " + TYPE + " WHERE name = ?")) {
        // inside a transaction block pgjdbc cannot prepare a statement again after a 0A000, which it heals only in autocommit
        connection.setAutoCommit(autoCommit);
        // executions 1-4 unnamed, the 5th prepared on the server and described, 6+ decoded with that description
        for (int i = 0; i < 8; i++)
          for (final String[] expected : new String[][] { { "bool", "t" }, { "int", "42" }, { "double", "2.5" } })
            assertThat(selectString(select, expected[0])).as(expected[0] + " execution " + (i + 1) + " autocommit " + autoCommit)
                .isEqualTo(expected[1]);
        if (!autoCommit)
          connection.commit();
      }
  }

  @Test
  void aUuidParameterBinds() throws Exception {
    createRecords();
    try (final Connection connection = openJdbcConnection();
        final PreparedStatement select = connection.prepareStatement("SELECT name FROM " + TYPE + " WHERE u = ?")) {
      // pgjdbc sends a java.util.UUID as a binary uuid, from the first execution and from the prepared ones
      for (int i = 0; i < 6; i++) {
        select.setObject(1, ID);
        try (final ResultSet resultSet = select.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getString(1)).isEqualTo("uuid");
        }
      }
    }

    withConnection((out, in) -> {
      // psycopg sends a uuid.UUID as text, declared as uuid; PostgreSQL also reads upper case, braces and no hyphens
      sendParseWithTypes(out, "U1", "SELECT name FROM " + TYPE + " WHERE u = $1", 2950);
      sendSync(out);
      readUntilReadyForQuery(in);
      for (final String spelling : new String[] { ID.toString(), ID.toString().toUpperCase(), "{" + ID + "}", ID.toString().replace("-", "") }) {
        sendBind(out, "", "U1", spelling);
        sendExecute(out, "");
        sendSync(out);
        assertThat(firstDataRowValue(readUntilReadyForQuery(in))).as(spelling).isEqualTo("uuid");
      }

      sendBind(out, "", "U1", "not-a-uuid");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('E');
    });
  }

  private void createRecords() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      database.newDocument(TYPE).set("name", "bool").set("v", true).save();
      database.newDocument(TYPE).set("name", "int").set("v", 42).save();
      database.newDocument(TYPE).set("name", "double").set("v", 2.5).save();
      database.newDocument(TYPE).set("name", "list").set("v", List.of(1, 2, 3)).save();
      database.newDocument(TYPE).set("name", "date").set("v", LocalDateTime.of(2026, 1, 2, 0, 0)).save();
      database.newDocument(TYPE).set("name", "uuid").set("u", ID.toString()).save();
    });
  }

  private static String selectString(final PreparedStatement select, final String name) throws Exception {
    select.setString(1, name);
    try (final ResultSet resultSet = select.executeQuery()) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getString(1);
    }
  }

  private static List<WireMessage> describeAndExecute(final DataOutputStream out, final DataInputStream in, final String statement,
      final String name) throws Exception {
    if (statement.isEmpty())
      sendParse(out, "", QUERY);
    sendBind(out, "", statement, name);
    sendDescribe(out, 'P', "");
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  private static List<WireMessage> executeWithoutDescribe(final DataOutputStream out, final DataInputStream in, final String name)
      throws Exception {
    sendBind(out, "", "S1", name);
    sendExecute(out, "");
    sendSync(out);
    return readUntilReadyForQuery(in);
  }

  /** The type OID of the first column of the RowDescription among {@code messages}. */
  private static int columnTypeOid(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'T') {
        final ByteBuffer body = ByteBuffer.wrap(message.body());
        body.getShort(); // field count
        while (body.get() != 0) {
          // the column name
        }
        body.getInt(); // table OID
        body.getShort(); // attribute number
        return body.getInt();
      }
    throw new AssertionError("no RowDescription among " + messageTypesOf(messages));
  }

  private interface WireSession {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void withConnection(final WireSession session) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');
      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> session.run(out, in));
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
}
