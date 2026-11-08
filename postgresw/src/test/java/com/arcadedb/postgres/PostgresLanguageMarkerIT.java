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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

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
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendParseWithTypes;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * The {@code {language}} marker that sends a statement received on the Postgres wire to an engine other than SQL is
 * only read as the first token of the statement, and only names a language permitted on this protocol
 * ({@code arcadedb.postgres.languages}).
 * <p>
 * A marker found anywhere in the text used to be honoured: a correctly escaped string literal carrying one - which is
 * what a driver that interpolates parameters on the client side (pgjdbc's simple query mode, psycopg2, PHP PDO's
 * emulated prepares) writes for a bound value - turned the rest of the statement into a statement of another engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresLanguageMarkerIT extends PostgresWireProtocolTestBase {

  @Test
  void aMarkerInsideABoundValueDoesNotChangeTheLanguage() throws Exception {
    try (final Connection conn = connect(true)) {
      try (final Statement st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE MarkerPerson IF NOT EXISTS");
        st.execute("INSERT INTO MarkerPerson SET name = 'alice'");
      }

      // The application binds the value: the driver escapes it into a string literal of a SELECT
      try (final PreparedStatement ps = conn.prepareStatement("SELECT FROM MarkerPerson WHERE name = ?")) {
        ps.setString(1, "{cypher}MATCH (n:MarkerPerson) DETACH DELETE n //");
        try (final ResultSet rs = ps.executeQuery()) {
          assertThat(rs.next()).isFalse();
        }
      }

      try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("SELECT count(*) AS c FROM MarkerPerson")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getLong("c")).isEqualTo(1L);
      }
    }
  }

  @Test
  void aMarkerAfterTheFirstTokenIsPartOfTheSqlStatement() throws Exception {
    try (final Connection conn = connect(false)) {
      try (final Statement st = conn.createStatement()) {
        st.execute("CREATE VERTEX TYPE MarkerDoc IF NOT EXISTS");
        st.execute("INSERT INTO MarkerDoc SET name = '{gremlin}g.V().drop()'");
      }
      // Extended protocol: the marker inside the statement text is a plain SQL literal and the row is found by it
      try (final PreparedStatement ps = conn.prepareStatement("SELECT name FROM MarkerDoc WHERE name = '{gremlin}g.V().drop()'");
          final ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("name")).isEqualTo("{gremlin}g.V().drop()");
      }
    }
  }

  @Test
  void aLeadingMarkerStillSelectsAPermittedLanguage() throws Exception {
    for (final boolean simple : new boolean[] { true, false })
      try (final Connection conn = connect(simple)) {
        try (final Statement st = conn.createStatement()) {
          st.execute("CREATE VERTEX TYPE MarkerMovie IF NOT EXISTS");
          st.execute("DELETE FROM MarkerMovie");
          st.execute("INSERT INTO MarkerMovie SET title = 'matrix'");
        }
        try (final Statement st = conn.createStatement();
            final ResultSet rs = st.executeQuery("  {cypher} MATCH (m:MarkerMovie) RETURN m.title AS title")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("title")).isEqualTo("matrix");
        }
        // A leading comment, as some BI tools prepend to every native query, is not the first token
        try (final Statement st = conn.createStatement();
            final ResultSet rs = st.executeQuery("-- tool:: user 1\n/* query */ {Cypher}MATCH (m:MarkerMovie) RETURN m.title AS title")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("title")).isEqualTo("matrix");
        }
        try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("{sql}SELECT title FROM MarkerMovie")) {
          assertThat(rs.next()).isTrue();
          assertThat(rs.getString("title")).isEqualTo("matrix");
        }
      }
  }

  @Test
  void aLanguageNotPermittedOnThisProtocolIsRefused() throws Exception {
    for (final boolean simple : new boolean[] { true, false })
      try (final Connection conn = connect(simple)) {
        for (final String statement : new String[] { "{java}com.arcadedb.Constants::getVersion", "{redis}PING", "{nosuchlanguage}x" })
          try (final Statement st = conn.createStatement()) {
            assertThatThrownBy(() -> st.executeQuery(statement))//
                .isInstanceOf(PSQLException.class)//
                .hasMessageContaining("not permitted on the PostgreSQL protocol")//
                .hasMessageContaining(GlobalConfiguration.POSTGRES_LANGUAGES.getKey())//
                .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo("0A000"));
          }

        // The connection is still usable afterwards
        try (final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery("SELECT 1 AS one")) {
          assertThat(rs.next()).isTrue();
        }
      }
  }

  /**
   * A refused Parse still consumes its whole message: the declared parameter types that follow the statement text must not
   * be read as the next message.
   */
  @Test
  void aRefusedParseWithDeclaredParameterTypesKeepsTheConnectionInStep() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParseWithTypes(out, "", "{java}com.arcadedb.Constants::getVersion", 23, 25, 1043);
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).containsExactly('E', 'Z');
        assertThat(sqlStateOf(refused)).isEqualTo("0A000");

        sendSimpleQuery(out, "SELECT 1 AS one");
        final List<WireMessage> answered = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(answered)).doesNotContain('E');
        assertThat(firstDataRowValue(answered)).isEqualTo("1");
      });
    }
  }

  private Connection connect(final boolean simpleQueryMode) throws SQLException {
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    // A hang detector: a reply the server never sends fails the test instead of stalling the build
    props.setProperty("socketTimeout", "30");
    if (simpleQueryMode)
      // pgjdbc interpolates the bound values into the statement text, escaped as string literals
      props.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }
}
