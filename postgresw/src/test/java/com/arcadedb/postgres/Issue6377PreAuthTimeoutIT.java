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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6377: the Postgres wire executor never bounded its pre-authentication phase,
 * so an unauthenticated client that connected and then said nothing pinned a connection thread and a file
 * descriptor for as long as it liked - the listener starts one thread per accept and caps neither. The two
 * sibling wire protocols had already been hardened against exactly this (Redis #5912, Bolt #5978).
 * <p>
 * There were two independent ways to hold the connection, and a socket timeout alone only closes one of
 * them: the blocking read of the startup message, and the poll loop that waits for the password message to
 * show up (nothing is blocked on the socket there, so no socket timeout can interrupt it). Both are covered
 * below. The third hole was the startup message itself, whose declared length was read and then ignored
 * while the parameter loop ran until the client chose to stop it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6377PreAuthTimeoutIT extends PostgresWireProtocolTestBase {

  private static final int POSTGRES_PORT = 5432;
  /** Lowered so an unbounded phase shows up as a hung test rather than a half-minute one. */
  private static final int HANDSHAKE_TIMEOUT_MS = 1_000;
  /**
   * Safety bound for the client side only, well above the lowered server-side timeout. If this fires
   * instead of a clean EOF, the server is still holding the idle connection open - which is the bug.
   */
  private static final int CLIENT_SAFETY_TIMEOUT_MS = 30_000;

  @Test
  @Tag("slow")
  void aConnectionThatNeverSendsAStartupMessageIsClosed() throws Exception {
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(HANDSHAKE_TIMEOUT_MS);
    try (final Socket socket = openSocket()) {
      // Never send anything at all: the first read of the startup message blocks on the socket.
      assertThat(socket.getInputStream().read()).isEqualTo(-1);
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }

    assertPostgresStillServesClients();
  }

  @Test
  @Tag("slow")
  void aConnectionThatNeverSendsThePasswordIsClosed() throws Exception {
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(HANDSHAKE_TIMEOUT_MS);
    try (final Socket socket = openSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());

      // The server answers with an AuthenticationCleartextPassword request and then waits. Never reply.
      readMessageOfType(in, 'R');

      assertThat(in.read()).isEqualTo(-1);
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }

    assertPostgresStillServesClients();
  }

  @Test
  void aStartupMessageLongerThanPostgresOwnLimitIsRejectedWithoutReadingIt() throws Exception {
    try (final Socket socket = openSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());

      // A declared length above PQ_STARTUP_MSG_LIMIT. Nothing else is sent: the connection must go away on
      // the strength of the header alone, without waiting for the body it would otherwise try to consume.
      out.writeInt(10 * 1024 * 1024);
      out.writeInt(196608); // protocol version 3.0
      out.flush();

      assertThat(socket.getInputStream().read()).isEqualTo(-1);
    }

    assertPostgresStillServesClients();
  }

  @Test
  void aStartupMessageShorterThanItsOwnHeaderIsRejected() throws Exception {
    try (final Socket socket = openSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());

      out.writeInt(4); // shorter than the 8 bytes the header alone occupies
      out.writeInt(196608);
      out.flush();

      assertThat(socket.getInputStream().read()).isEqualTo(-1);
    }

    assertPostgresStillServesClients();
  }

  @Test
  void startupParametersStopAtTheLengthTheClientDeclared() throws Exception {
    try (final Socket socket = openSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());

      // A well-formed startup message, immediately followed by a stream of further "k\0v\0" pairs that the
      // declared length does not cover. They used to be read and accumulated into the pre-auth connection
      // properties, because the loop ran until the client sent a terminator rather than until the message
      // ended; now they are left in the socket buffer and the handshake carries on to the password request.
      sendStartupMessage(out, "root", getDatabaseName());
      for (int i = 0; i < 1_000; i++)
        out.write(new byte[] { 'k', 0, 'v', 0 });
      out.flush();

      final DataInputStream in = new DataInputStream(socket.getInputStream());
      readMessageOfType(in, 'R');
    }

    assertPostgresStillServesClients();
  }

  @Test
  @Tag("slow")
  void anAuthenticatedConnectionIsNotClosedWhileIdle() throws Exception {
    // The pre-auth timeout must not keep applying once the database is open: a Postgres client is expected
    // to keep a long-lived, often idle connection open between statements.
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(HANDSHAKE_TIMEOUT_MS);
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      assertThat(readSingleInt(statement)).isEqualTo(1);

      Thread.sleep(HANDSHAKE_TIMEOUT_MS * 3L); // well over the lowered pre-auth timeout

      assertThat(readSingleInt(statement)).isEqualTo(1);
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }
  }

  private Socket openSocket() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", POSTGRES_PORT), 5_000);
    socket.setSoTimeout(CLIENT_SAFETY_TIMEOUT_MS);
    return socket;
  }

  private void assertPostgresStillServesClients() throws Exception {
    // The listener and its per-connection threads must be unharmed by the connections dropped above.
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      assertThat(readSingleInt(statement)).isEqualTo(1);
    }
  }

  private static int readSingleInt(final Statement statement) throws Exception {
    try (final ResultSet resultSet = statement.executeQuery("SELECT 1")) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getInt(1);
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + POSTGRES_PORT + "/" + getDatabaseName(), properties);
  }
}
