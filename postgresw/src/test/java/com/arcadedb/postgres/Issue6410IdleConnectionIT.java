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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
public class Issue6410IdleConnectionIT extends PostgresWireProtocolTestBase {

  private static final int    POSTGRES_PORT     = 5432;
  private static final String EXECUTOR_THREAD   = "ArcadeDB-postgres/";
  /** How long a thread is given to notice that its connection is gone. Generous: it should be immediate. */
  private static final long   RETIREMENT_TIMEOUT_MS = 10_000;

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

      // The cancel request arrives on a second connection and closes the executor of the session it names -
      // this is the server closing a connection from underneath a client that is not talking to it at that
      // moment, and the only thing that has to break a blocked read.
      try (final Socket cancelSocket = openSocket()) {
        final DataOutputStream cancel = new DataOutputStream(cancelSocket.getOutputStream());
        cancel.writeInt(16);
        cancel.writeInt(80877102); // CancelRequest
        cancel.writeInt((int) key[0]);
        cancel.writeInt((int) key[1]);
        cancel.flush();
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

  private Socket openSocket() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", POSTGRES_PORT), 5_000);
    socket.setSoTimeout(30_000);
    return socket;
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
    return DriverManager.getConnection("jdbc:postgresql://localhost:" + POSTGRES_PORT + "/" + getDatabaseName(), properties);
  }
}
