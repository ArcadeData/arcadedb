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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10597: two arms of the RESP parser logged at SEVERE and carried on instead of answering and
 * closing - a byte that is not a RESP type marker, and any byte after a CR that is not LF. Neither needed
 * authentication, so 40 KB of junk wrote 6.2 MB of log over connections that stayed open. The first arm also meant an
 * inline command ({@code PING\r\n}, what {@code nc}, {@code telnet} and health probes send) got no answer at all.
 * <p>
 * Every malformed message must now get one {@code -ERR Protocol error} reply and a close, with a number of log records
 * that does not depend on how many junk bytes were sent; an inline command is parsed the way Redis parses it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10597RedisMalformedInputTest extends BaseRedisServerTest {

  private static final String PASSWORD = DEFAULT_PASSWORD_FOR_TESTS;

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
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      // process-wide: put the default back, or every later log-asserting test in this fork captures nothing
      LogManager.instance().setLogger(new DefaultLogger());
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    } finally {
      super.endTest();
    }
  }

  @Test
  void inlinePingIsAnsweredInsteadOfHanging() throws Exception {
    try (final Socket socket = connect()) {
      // unauthenticated: one NOAUTH reply, not silence
      send(socket, "PING\r\n");
      assertThat(readLine(socket)).startsWith("-NOAUTH");

      send(socket, "AUTH root " + PASSWORD + "\r\nPING\r\n");
      assertThat(readLine(socket)).isEqualTo("+OK");
      assertThat(readLine(socket)).isEqualTo("+PONG");

      // an empty line is ignored, as Redis does, and a bare LF terminates a line too
      send(socket, "\r\n\nPING\n");
      assertThat(readLine(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void inlineCommandHonoursQuotes() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "AUTH root " + PASSWORD + "\r\n");
      assertThat(readLine(socket)).isEqualTo("+OK");

      send(socket, "SET \"key 10597\" 'a value'\r\n");
      assertThat(readLine(socket)).isEqualTo("+OK");

      send(socket, "GET \"key\\x2010597\"\r\n");
      assertThat(readLine(socket)).isEqualTo("$7");
      assertThat(readLine(socket)).isEqualTo("a value");
    }
  }

  @Test
  void unbalancedQuotesInAnInlineCommandCloseWithOneError() throws Exception {
    assertOneErrorThenClosed("PING \"unterminated\r\n", "unbalanced quotes");
  }

  // The payloads of the tests asserting the reply end exactly at the byte the server refuses: a server that closes with
  // unread input answers with a TCP reset, which can drop the reply before the client reads it. The issue's large
  // repros, which do leave input unread, are checked by assertClosedWithAtMostOneRecord instead.

  @Test
  void junkBytesWithoutANewlineCloseOnceTheInlineBoundIsReached() throws Exception {
    // the issue's 'Z' * 20000 repro, past the inline bound so the server has to give up rather than wait for a line:
    // the server refuses the byte after the 64 KB it buffered, the last one sent
    final int before = executorRecords.get();
    assertOneErrorThenClosed("Z".repeat(64 * 1024 + 1), "too big inline request");
    assertThat(executorRecords.get() - before).isLessThanOrEqualTo(1);
  }

  @Test
  void unknownTypeMarkerInsideAnArrayClosesWithOneError() throws Exception {
    assertOneErrorThenClosed("*2\r\nZ", "expected '$'");
  }

  @Test
  void strayBytesAfterACarriageReturnCloseWithOneError() throws Exception {
    assertOneErrorThenClosed("$1\rY", "CRLF");
  }

  @Test
  void issueReprosCloseWithAtMostOneRecordWhateverTheirSize() throws Exception {
    // the issue's repros, which used to log one or two SEVERE records per junk byte and leave the connection open
    assertClosedWithAtMostOneRecord("*2\r\n" + "Z".repeat(20_000));
    assertClosedWithAtMostOneRecord("$1\r" + "Y".repeat(20_000) + "\n");
    assertClosedWithAtMostOneRecord("Z".repeat(70_000));
  }

  @Test
  void bulkStringNotFollowedByCrlfClosesWithOneError() throws Exception {
    assertOneErrorThenClosed("*1\r\n$4\r\nPINGx", "CRLF");
  }

  @Test
  void inlineCommandHonoursTheConfiguredArgumentLimit() throws Exception {
    // the inline form must not be a way around arcadedb.redis.maxMultiBulkLength, which bounds a RESP array
    GlobalConfiguration.REDIS_MAX_MULTIBULK_LENGTH.setValue(5);
    try {
      assertOneErrorThenClosed("a b c d e f\r\n", "maximum allowed is 5");

      try (final Socket socket = connect()) {
        send(socket, "AUTH root " + PASSWORD + "\r\n");
        assertThat(readLine(socket)).isEqualTo("+OK");
      }
    } finally {
      GlobalConfiguration.REDIS_MAX_MULTIBULK_LENGTH.reset();
    }
  }

  @Test
  void topLevelValueThatIsNotACommandClosesWithOneError() throws Exception {
    // a bare bulk string is not a command: used to be logged as "Invalid command" with no reply
    assertOneErrorThenClosed("$1\r\nx\r\n", "Protocol error");
  }

  @Test
  void logRecordCountDoesNotDependOnTheNumberOfJunkBytes() throws Exception {
    final int small = recordsFor("$1\r" + "Y".repeat(10) + "\n");
    final int large = recordsFor("$1\r" + "Y".repeat(20_000) + "\n");
    assertThat(small).isEqualTo(1);
    assertThat(large).isEqualTo(small);
  }

  private int recordsFor(final String payload) throws Exception {
    final int before = executorRecords.get();
    try (final Socket socket = connect()) {
      send(socket, payload);
      assertRefusedAndClosed(socket);
    }
    return executorRecords.get() - before;
  }

  private void assertClosedWithAtMostOneRecord(final String payload) throws Exception {
    assertThat(recordsFor(payload)).isLessThanOrEqualTo(1);
  }

  /**
   * The connection must be closed (a read timeout fails the test), and whatever arrived before the close is the one
   * error reply: a reset can drop the reply, never turn it into anything else.
   */
  private static void assertRefusedAndClosed(final Socket socket) throws IOException {
    final String received = readToEnd(socket.getInputStream());
    if (!received.isEmpty())
      assertThat(received).startsWith("-ERR").containsIgnoringCase("Protocol error").endsWith("\r\n");
  }

  private void assertOneErrorThenClosed(final String payload, final String expected) throws Exception {
    try (final Socket socket = connect()) {
      send(socket, payload);
      final String reply = readLine(socket);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase(expected);
      assertThat(readToEnd(socket.getInputStream())).isEmpty();
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

  /** Reads until the server closes the connection: a timeout here means it was left open. */
  private static String readToEnd(final InputStream in) throws IOException {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final byte[] buffer = new byte[1024];
    int n;
    try {
      while ((n = in.read(buffer)) != -1)
        out.write(buffer, 0, n);
    } catch (final SocketException e) {
      // a reset after the server closed with unread input still in its buffer is a close too
    }
    return out.toString(StandardCharsets.ISO_8859_1);
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
