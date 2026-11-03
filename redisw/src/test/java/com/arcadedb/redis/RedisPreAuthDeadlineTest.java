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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The phase before authentication is bounded in TIME, not only in idleness: the handshake read timeout closes a connection
 * that sends nothing, but one that keeps sending - a command it is answered NOAUTH for, an array whose first element is not a
 * command name - is never idle. Without a deadline such connections held their pre-authentication slot for ever, at a few
 * bytes per socket timeout, and enough of them locked every legitimate client out of the listener. The log of what
 * unauthenticated peers send is bounded too, across connections, not only within one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RedisPreAuthDeadlineTest extends BaseRedisServerTest {

  private static final long DEADLINE_MS = 2_000;

  private final AtomicInteger executorRecords = new AtomicInteger();

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    LogManager.instance().setLogger(countingLogger());
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
    GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.setValue(DEADLINE_MS);
    GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.setValue(2);
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      // process-wide: put the default back, or every later log-asserting test in this fork captures nothing
      LogManager.instance().setLogger(new DefaultLogger());
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.reset();
      GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.reset();
    } finally {
      super.endTest();
    }
  }

  @Test
  void aConnectionThatKeepsBeingRefusedIsClosedAtTheDeadline() throws Exception {
    try (final Socket socket = connect()) {
      assertClosedWhileSending(socket, "PING\r\n");
    }
  }

  @Test
  void aConnectionThatKeepsSendingAnArrayThatIsNotACommandIsClosedAtTheDeadline() throws Exception {
    // the 8-byte shape: an array whose first element is an integer, answered with a protocol error and kept open
    try (final Socket socket = connect()) {
      assertClosedWhileSending(socket, "*1\r\n:1\r\n");
    }
  }

  @Test
  void connectionsHoldingTheSlotsCannotLockOutALegitimateClient() throws Exception {
    // both pre-authentication slots taken by connections that never authenticate but are never idle either
    final List<Socket> holders = new ArrayList<>();
    try {
      for (int i = 0; i < 2; i++) {
        final Socket holder = connect();
        send(holder, "PING\r\n");
        assertThat(readLine(holder)).startsWith("-NOAUTH");
        holders.add(holder);
      }

      // a third client is refused while they hold them...
      try (final Socket refused = connect()) {
        assertThat(isClosedByServer(refused)).as("the listener is full").isTrue();
      }

      // ...and gets in once the deadline has reclaimed them, whatever they keep sending
      for (final Socket holder : holders)
        assertClosedWhileSending(holder, "PING\r\n");

      try (final Socket client = connect()) {
        send(client, "AUTH root " + DEFAULT_PASSWORD_FOR_TESTS + "\r\nPING\r\n");
        assertThat(readLine(client)).isEqualTo("+OK");
        assertThat(readLine(client)).isEqualTo("+PONG");
      }
    } finally {
      for (final Socket holder : holders)
        holder.close();
    }
  }

  @Test
  void anAuthenticatedConnectionOutlivesTheDeadline() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "AUTH root " + DEFAULT_PASSWORD_FOR_TESTS + "\r\n");
      assertThat(readLine(socket)).isEqualTo("+OK");

      Thread.sleep(DEADLINE_MS + 2_000);

      send(socket, "PING\r\n");
      assertThat(readLine(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void junkFromManyUnauthenticatedConnectionsWritesABoundedLog() throws Exception {
    // one record per refused connection let a peer write the log at the rate it can open connections
    final int before = executorRecords.get();
    for (int i = 0; i < 40; i++)
      try (final Socket socket = connect()) {
        send(socket, "*2\r\nZ");
        drain(socket.getInputStream());
      }
    assertThat(executorRecords.get() - before).as("records written for 40 refused connections").isLessThanOrEqualTo(1);
  }

  /** Keeps sending {@code payload} well inside the idle timeout until the server closes; the deadline is the only way out. */
  private static void assertClosedWhileSending(final Socket socket, final String payload) throws Exception {
    // a hang detector, not a latency bound: the deadline is 2 s, the reaper ticks every second
    final long giveUpAt = System.currentTimeMillis() + 30_000;
    socket.setSoTimeout(500);
    while (System.currentTimeMillis() < giveUpAt) {
      try {
        send(socket, payload);
      } catch (final SocketException e) {
        return;
      }
      if (isClosedByServer(socket))
        return;
    }
    fail("the server kept a connection that never authenticated open past its pre-authentication deadline");
  }

  /** Reads what the server sent within the socket timeout: true when it closed the connection. */
  private static boolean isClosedByServer(final Socket socket) throws IOException {
    final InputStream in = socket.getInputStream();
    final byte[] buffer = new byte[256];
    try {
      while (true) {
        final int n = in.read(buffer);
        if (n == -1)
          return true;
        if (in.available() == 0)
          // answered and still open: read again only once more input could have arrived
          return false;
      }
    } catch (final SocketTimeoutException e) {
      return false;
    } catch (final SocketException e) {
      return true;
    }
  }

  private static void drain(final InputStream in) throws IOException {
    final byte[] buffer = new byte[256];
    try {
      while (in.read(buffer) != -1) {
        // until the server closes
      }
    } catch (final SocketException e) {
      // a reset is a close too
    }
  }

  private Socket connect() throws IOException {
    final Socket socket = new Socket("localhost", getServerRedisPort());
    socket.setSoTimeout(10_000);
    return socket;
  }

  private static void send(final Socket socket, final String raw) throws IOException {
    socket.getOutputStream().write(raw.getBytes(StandardCharsets.ISO_8859_1));
    socket.getOutputStream().flush();
  }

  private static String readLine(final Socket socket) throws IOException {
    final StringBuilder sb = new StringBuilder();
    int prev = -1;
    while (true) {
      final int b = socket.getInputStream().read();
      if (b == -1)
        throw new IOException("Unexpected EOF while reading a line, read so far: '" + sb + "'");
      if (prev == '\r' && b == '\n') {
        sb.setLength(sb.length() - 1);
        return sb.toString();
      }
      sb.append((char) b);
      prev = b;
    }
  }

  private Logger countingLogger() {
    final Logger delegate = new DefaultLogger();
    return new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        count(requester, level);
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
            arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        count(requester, level);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public boolean isLoggable(final Object requester, final Level level) {
        return delegate.isLoggable(requester, level);
      }

      @Override
      public void flush() {
        delegate.flush();
      }

      private void count(final Object requester, final Level level) {
        if (requester instanceof RedisNetworkExecutor && level.intValue() >= Level.INFO.intValue())
          executorRecords.incrementAndGet();
      }
    };
  }
}
