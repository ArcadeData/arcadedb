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

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A value placed inside an ArcadeDB JSON object ({@code INSERT INTO T CONTENT {"v": ?}}) reaches the type exactly as the client
 * gave it, whichever way the driver sends it. With {@code preferQueryMode=simple} the driver composes the statement text itself
 * and writes the value as a PostgreSQL literal, following the {@code standard_conforming_strings} the server reported: a quote is
 * doubled and a backslash stays as it is. The server has to read that literal as PostgreSQL does, inside the braces too, both to
 * find where the literal ends and to find where the statement ends.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresJsonObjectLiteralIT extends PostgresWireProtocolTestBase {
  private static final String[] VALUES = { "plain", "a\\b", "C:\\", "it's", "\\'", "\\''", "x\\'}", "\\', \"w\": 1, \"v\": '",
      "};z;--", "\\'};z;--", "\\'}];z /* ", "{[;'\\", "a\\';b", "line\nbreak" };

  @Test
  void aValueInsideAJsonObjectIsStoredAsItIs() throws Exception {
    createHolder();

    for (final String mode : new String[] { "simple", "extended" })
      for (final boolean standardConformingStrings : new boolean[] { true, false })
        try (final Connection connection = connect(mode)) {
          try (final Statement statement = connection.createStatement()) {
            if (!standardConformingStrings)
              statement.execute("SET standard_conforming_strings = off");
            statement.execute("DELETE FROM Holder");
          }

          for (final String value : VALUES)
            try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Holder CONTENT {\"v\": ?}")) {
              insert.setString(1, value);
              insert.execute();
            }

          assertThat(storedValues(connection)).as("preferQueryMode=%s, standard_conforming_strings=%s", mode,
              standardConformingStrings).containsExactlyInAnyOrder(VALUES);
        }
  }

  @Test
  void aValueInsideACollectionOfAJsonObjectIsStoredAsItIs() throws Exception {
    createHolder();

    for (final String mode : new String[] { "simple", "extended" })
      try (final Connection connection = connect(mode)) {
        for (final String value : VALUES)
          try (final PreparedStatement insert = connection.prepareStatement("INSERT INTO Holder CONTENT {\"k\": 1, \"items\": [?, ?]}")) {
            insert.setString(1, value);
            insert.setString(2, value);
            insert.execute();
          }

        final List<String> stored = new ArrayList<>();
        try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
            "SELECT items.size() AS n, items[0] AS a, items[1] AS b FROM Holder")) {
          while (resultSet.next()) {
            assertThat(resultSet.getInt("n")).isEqualTo(2);
            assertThat(resultSet.getString("b")).isEqualTo(resultSet.getString("a"));
            stored.add(resultSet.getString("a"));
          }
        }
        assertThat(stored).as("preferQueryMode=%s", mode).containsExactlyInAnyOrder(VALUES);

        try (final Statement statement = connection.createStatement()) {
          statement.execute("DELETE FROM Holder");
        }
      }
  }

  /**
   * One simple-query message that holds two statements, the first of them with the value inside its JSON object, written the way
   * a client composing the text writes it: each statement runs, and the value is the one the client gave.
   */
  @Test
  void aSimpleQueryComposedByTheClientRunsEachOfItsStatements() throws Exception {
    createHolder();

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      socket.setSoTimeout(30_000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in);
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      for (final String value : VALUES) {
        sendSimpleQuery(out,
            "INSERT INTO Holder CONTENT {\"v\": " + clientLiteral(value) + "}; INSERT INTO Holder CONTENT {\"v\": 'tail'}");
        final List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).as("value %s", value).doesNotContain('E');
      }
    }

    final List<String> expected = new ArrayList<>(Arrays.asList(VALUES));
    for (int i = 0; i < VALUES.length; i++)
      expected.add("tail");
    try (final Connection connection = connect("simple")) {
      assertThat(storedValues(connection)).containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  /** The value written as a client composing the text writes it while {@code standard_conforming_strings} is on. */
  private static String clientLiteral(final String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private void createHolder() throws SQLException {
    try (final Connection connection = connect("simple"); final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Holder");
    }
  }

  private static List<String> storedValues(final Connection connection) throws SQLException {
    final List<String> stored = new ArrayList<>();
    try (final Statement statement = connection.createStatement(); final ResultSet resultSet = statement.executeQuery(
        "SELECT v FROM Holder")) {
      while (resultSet.next())
        stored.add(resultSet.getString("v"));
    }
    return stored;
  }

  private Connection connect(final String queryMode) throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("preferQueryMode", queryMode);
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }
}
