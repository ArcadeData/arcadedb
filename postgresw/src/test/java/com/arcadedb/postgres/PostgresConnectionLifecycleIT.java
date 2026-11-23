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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The life of a Postgres connection on the server: how its handshake is bounded before authentication, how an idle
 * authenticated connection waits for its client and how its thread retires when the connection goes away, and how a
 * connection is answered when the database it names, or the one it already uses, is held by a restore or closed.
 */
class PostgresConnectionLifecycleIT extends PostgresWireProtocolTestBase {

  /*
   * Regression tests for issue #6410: an idle authenticated Postgres connection used to wake its thread ten
   * times a second to ask {@code in.available()} whether anything had arrived, because
   * {@code PostgresNetworkExecutor.readMessage()} never blocked. A Postgres client pool is expected to hold
   * long-lived, mostly-idle connections, so N pooled connections cost 10N timer wakeups per second doing no
   * work at all.
   * <p>
   * The same non-blocking read had a second consequence: a client that simply went away left its connection
   * thread polling a socket that would never produce another byte, for the lifetime of the server, because
   * {@code available()} on a closed peer returns 0 exactly as it does on an idle one - the end of the stream
   * was never actually read.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String EXECUTOR_THREAD   = "ArcadeDB-postgres/";
  /** How long a thread is given to notice that its connection is gone. Generous: it should be immediate. */
  private static final long   RETIREMENT_TIMEOUT_MS = 10_000;

  // Regression: #6410
  @Test
  void anIdleConnectionBlocksOnItsSocketInsteadOfPollingIt() throws Exception {
    final Set<Thread> before = executorThreads();

    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      assertThat(readSingleInt(statement)).isEqualTo(1);

      final Thread executor = newExecutorThread(before);

      // Sample the connection thread while nothing is being asked of it. A polling loop spends most of its
      // life in Thread.sleep(), so it is caught in TIMED_WAITING within a handful of samples; a blocking
      // read never is.
      for (int i = 0; i < 40; i++) {
        assertThat(executor.getState())
            .as("an idle connection thread must be blocked on its socket, not sleeping between polls")
            .isNotEqualTo(Thread.State.TIMED_WAITING);
        assertThat(stackOf(executor))
            .as("an idle connection thread must not be inside a sleep")
            .doesNotContain("java.lang.Thread.sleep");
        Thread.sleep(25);
      }
    }
  }

  // Regression: #6410
  @Test
  void aClientDisconnectRetiresTheConnectionThread() throws Exception {
    final Set<Thread> before = executorThreads();

    final Thread executor;
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      assertThat(readSingleInt(statement)).isEqualTo(1);
      executor = newExecutorThread(before);
    }

    assertRetires(executor);
  }

  // Regression: #6410
  @Test
  void aClientThatVanishesWithoutSayingGoodbyeRetiresTheConnectionThread() throws Exception {
    final Set<Thread> before = executorThreads();

    final Thread executor;
    final Socket socket = openSocket();
    try {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessageOfType(in, 'R');
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      executor = newExecutorThread(before);
    } finally {
      // No Terminate message: the client simply goes away, which is what a killed process or a dropped
      // network does. The JDBC driver's polite 'X' used to be the only thing that ended the command loop -
      // without it the thread polled a socket that could never produce another byte, for the lifetime of
      // the server, because available() answers 0 on a closed peer exactly as it does on an idle one.
      socket.close();
    }

    assertRetires(executor);
  }

  // Regression: #6410
  @Test
  void aServerSideCloseRetiresTheConnectionThread() throws Exception {
    final Set<Thread> before = executorThreads();

    try (final Socket socket = openSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessageOfType(in, 'R');
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);

      // BackendKeyData carries the process id and the secret a CancelRequest has to quote.
      final long[] key = readBackendKeyData(in);

      final Thread executor = newExecutorThread(before);

      // pg_terminate_backend, run on a second connection, closes the executor of the session it names - this is the
      // server closing a connection from underneath a client that is not talking to it at that moment, and the only
      // thing that has to break a blocked read. (A CancelRequest used to do it, but a CancelRequest stops the running
      // statement and leaves the connection alone, as on PostgreSQL - issue #9689.)
      try (final Connection admin = openJdbcConnection(); final Statement st = admin.createStatement();
          final ResultSet rs = st.executeQuery("SELECT pg_terminate_backend(" + key[0] + ") AS terminated")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getBoolean("terminated")).isTrue();
      }

      assertRetires(executor);
    }
  }

  /**
   * Reads messages until BackendKeyData ('K') and returns its process id and secret.
   */
  private static long[] readBackendKeyData(final DataInputStream in) throws Exception {
    while (true) {
      final int type = in.readUnsignedByte();
      final int length = in.readInt();
      if (type == 'K') {
        final long pid = in.readInt() & 0xFFFFFFFFL;
        final long secret = in.readInt() & 0xFFFFFFFFL;
        return new long[] { pid, secret };
      }
      in.skipNBytes(length - 4);
    }
  }

  private void assertRetires(final Thread executor) throws Exception {
    final long deadline = System.currentTimeMillis() + RETIREMENT_TIMEOUT_MS;
    while (executor.isAlive() && System.currentTimeMillis() < deadline)
      Thread.sleep(50);

    assertThat(executor.isAlive())
        .as("the connection thread must notice that its connection is gone and terminate, not keep polling a dead socket")
        .isFalse();
  }

  private static String stackOf(final Thread thread) {
    final StringBuilder buffer = new StringBuilder();
    for (final StackTraceElement element : thread.getStackTrace())
      buffer.append(element.toString()).append('\n');
    return buffer.toString();
  }

  private static Set<Thread> executorThreads() {
    final Set<Thread> threads = new HashSet<>();
    for (final Thread thread : Thread.getAllStackTraces().keySet())
      if (thread.getName().startsWith(EXECUTOR_THREAD))
        threads.add(thread);
    return threads;
  }

  private static Thread newExecutorThread(final Set<Thread> before) {
    final Set<Thread> now = executorThreads();
    now.removeAll(before);
    assertThat(now).as("the connection must have its own executor thread on the server").hasSize(1);
    return now.iterator().next();
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
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
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
  /** Lowered so an unbounded phase shows up as a hung test rather than a half-minute one. */
  private static final int HANDSHAKE_TIMEOUT_MS = 1_000;
  /**
   * Safety bound for the client side only, well above the lowered server-side timeout. If this fires
   * instead of a clean EOF, the server is still holding the idle connection open - which is the bug.
   */
  private static final int CLIENT_SAFETY_TIMEOUT_MS = 30_000;

  // Regression: #6377
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

  // Regression: #6377
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

  // Regression: #6377
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

  // Regression: #6377
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

  // Regression: #6377
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

  // Regression: #6377
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
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 5_000);
    socket.setSoTimeout(CLIENT_SAFETY_TIMEOUT_MS);
    return socket;
  }

  private void assertPostgresStillServesClients() throws Exception {
    // The listener and its per-connection threads must be unharmed by the connections dropped above.
    try (final Connection connection = openJdbcConnection(); final Statement statement = connection.createStatement()) {
      assertThat(readSingleInt(statement)).isEqualTo(1);
    }
  }

  /*
   * Issue #10533, through a real connection: a Postgres client that names a database an {@code overwrite} restore holds, or that a
   * {@code close database} closed, is refused with {@code 57P03} cannot_connect_now - the database exists, so it is not told that it
   * does not - and a name that is not a database is {@code 3D000} invalid_catalog_name (issue #10795). Once nothing holds the name the
   * same connection opens it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String HELD = "pgheld10533";

  // Regression: #10533
  @Test
  void aConnectionToADatabaseARestoreHoldsIsRefusedAsNotAvailable() throws Exception {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(HELD);
    try {
      assertThatThrownBy(() -> openConnection(HELD).close()).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW))
          .hasMessageContaining("not available");
      assertThat(server.existsDatabase(HELD)).as("the connection opened nothing").isFalse();
    } finally {
      server.releaseDatabaseNameReservedForRestore(HELD);
    }

    try (final Connection connection = openConnection(HELD)) {
      assertThat(connection.isValid(5)).as("a released reservation holds nothing").isTrue();
    } finally {
      dropIfPresent(server);
    }
  }

  // Regression: #10533
  @Test
  void aConnectionToADatabaseACloseCommandClosedIsRefusedAsNotAvailable() throws Exception {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.markClosedByCommand(HELD);
    try {
      assertThatThrownBy(() -> openConnection(HELD).close()).isInstanceOf(SQLException.class)
          .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW));
    } finally {
      server.unmarkClosedByCommand(HELD);
      dropIfPresent(server);
    }
  }

  // Regression: #10533
  /** Issue #10795: PostgreSQL's own answer to a connection that names a database it does not have, 3D000 invalid_catalog_name. */
  @Test
  void aNameThatIsNotADatabaseIsInvalidCatalogName() {
    assertThatThrownBy(() -> openConnection("pgnothing10533").close()).isInstanceOf(SQLException.class)
        .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_INVALID_CATALOG_NAME))
        .hasMessageContaining("does not exist");
  }

  /** Leaves {@link #HELD} on disk, closed and deregistered: what an {@code overwrite} restore finds on its target. */
  private static void onDiskNotOpen(final ArcadeDBServer server) {
    final ServerDatabase created = server.createDatabase(HELD, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(HELD);
    assertThat(server.existsDatabase(HELD)).isFalse();
  }

  private static void dropIfPresent(final ArcadeDBServer server) {
    if (server.existsDatabase(HELD))
      server.dropLocalDatabase(server.getDatabaseForOperator(HELD).getEmbedded(), HELD);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), HELD));
  }

  private Connection openConnection(final String database) throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + getServerPostgresPort() + "/" + database, properties);
  }

  /*
   * Issue #10689, through a real connection: a Postgres session resolves its database once, when it connects, so a session opened
   * before an {@code overwrite} restore claimed the database never meets a check made at resolution. Its writes inside the restore
   * window were acknowledged and then destroyed by the swap. They are now refused with {@code 57P03} cannot_connect_now, the answer a
   * held database gets, while its reads go on until the swap.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String TYPE = "Issue10689Pg";

  // Regression: #10689
  @Test
  void aSessionOpenedBeforeTheRestoreReadsAndCannotWrite() throws Exception {
    final ArcadeDBServer server = getServer(0);
    server.getDatabaseForOperator(getDatabaseName()).getSchema().createDocumentType(TYPE);

    try (final Connection connection = openConnection(getDatabaseName()); final Statement statement = connection.createStatement()) {
      statement.execute("INSERT INTO " + TYPE + " SET v = 1");

      server.reserveDatabaseNameForRestore(getDatabaseName());
      try {
        assertThat(count(statement)).as("the session still reads the database the restore will replace").isEqualTo(1L);

        assertThatThrownBy(() -> statement.execute("INSERT INTO " + TYPE + " SET v = 2")).isInstanceOf(SQLException.class)
            .satisfies(e -> assertThat(((SQLException) e).getSQLState()).isEqualTo(PostgresNetworkExecutor.SQLSTATE_CANNOT_CONNECT_NOW))
            .hasMessageContaining("restore");
        assertThat(count(statement)).as("nothing was written").isEqualTo(1L);
      } finally {
        server.releaseDatabaseNameReservedForRestore(getDatabaseName());
      }

      statement.execute("INSERT INTO " + TYPE + " SET v = 2");
      assertThat(count(statement)).as("the end of the restore gives the writes back").isEqualTo(2L);
    }
  }

  private static long count(final Statement statement) throws SQLException {
    try (final ResultSet resultSet = statement.executeQuery("SELECT count(@rid) AS c FROM " + TYPE)) {
      assertThat(resultSet.next()).isTrue();
      return resultSet.getLong(1);
    }
  }
}
