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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The lengths a Redis client declares are checked before they size anything: a connection that has not authenticated yet is
 * held to small bounds, and a value is buffered as its bytes arrive rather than in one block of the declared size.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class RedisDeclaredLengthTest extends BaseRedisServerTest {

  private static final String USER     = "root";
  private static final String PASSWORD = DEFAULT_PASSWORD_FOR_TESTS;

  private static final int  BEFORE_AUTH_MAX_BULK_LENGTH = 16 * 1024;
  private static final long ALLOCATION_BUDGET           = 8L * 1024 * 1024;

  @Test
  void aBulkLengthOverTheLimitBeforeAuthenticationIsRefused() throws Exception {
    // the default limit of a bulk string is 512MB, which a connection that proved nothing must not be given
    try (final Socket socket = connect()) {
      send(socket, "*1\r\n$536870912\r\n");

      final String reply = readLine(socket.getInputStream());
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("bulk length");
      assertThat(isClosedByServer(socket)).isTrue();
    }

    assertServerStillServes();
  }

  @Test
  void aBulkLengthJustOverTheLimitBeforeAuthenticationIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "*3\r\n$4\r\nAUTH\r\n$4\r\nroot\r\n$" + (BEFORE_AUTH_MAX_BULK_LENGTH + 1) + "\r\n");

      final String reply = readLine(socket.getInputStream());
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("bulk length");
      assertThat(isClosedByServer(socket)).isTrue();
    }
  }

  @Test
  void aBulkLengthAtTheLimitBeforeAuthenticationIsRead() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "*3\r\n$4\r\nAUTH\r\n$4\r\nroot\r\n$" + BEFORE_AUTH_MAX_BULK_LENGTH + "\r\n" + "x".repeat(BEFORE_AUTH_MAX_BULK_LENGTH)
          + "\r\n");

      assertThat(readLine(socket.getInputStream())).startsWith("-WRONGPASS");
    }
  }

  @Test
  void aMultibulkLengthOverTheLimitBeforeAuthenticationIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "*11\r\n");

      final String reply = readLine(socket.getInputStream());
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("multibulk length");
      assertThat(isClosedByServer(socket)).isTrue();
    }

    assertServerStillServes();
  }

  @Test
  void aMultibulkLengthAtTheLimitBeforeAuthenticationIsRead() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "*10\r\n" + "$1\r\nx\r\n".repeat(10));

      assertThat(readLine(socket.getInputStream())).startsWith("-NOAUTH");
    }
  }

  @Test
  void anInlineCommandOverTheArgumentLimitBeforeAuthenticationIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "PING a b c d e f g h i j\r\n");

      final String reply = readLine(socket.getInputStream());
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("multibulk length");
    }

    try (final Socket socket = connect()) {
      send(socket, "PING a b c d e f g h i\r\n");

      assertThat(readLine(socket.getInputStream())).startsWith("-NOAUTH");
    }
  }

  @Test
  void theAuthenticationCommandsFitTheLimitsBeforeAuthentication() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "AUTH " + USER + " " + PASSWORD + "\r\n");
      assertThat(readLine(socket.getInputStream())).isEqualTo("+OK");
    }

    // HELLO with every option it accepts is 7 words
    try (final Socket socket = connect()) {
      send(socket,
          "*7\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$" + USER.length() + "\r\n" + USER + "\r\n$" + PASSWORD.length() + "\r\n" + PASSWORD
              + "\r\n$7\r\nSETNAME\r\n$1\r\nc\r\n");
      assertThat(readLine(socket.getInputStream())).isEqualTo("*14");
    }
  }

  @Test
  void aDeclaredLengthIsNotBufferedBeforeItsBytesArrive() throws Exception {
    try (final Socket socket = connect()) {
      send(socket, "AUTH " + USER + " " + PASSWORD + "\r\n");
      assertThat(readLine(socket.getInputStream())).isEqualTo("+OK");

      final Thread executor = await().atMost(Duration.ofSeconds(10)).until(RedisDeclaredLengthTest::findExecutorThread,
          thread -> thread != null);
      final long before = allocatedBytes(executor);

      // within the limit of 512MB, so it is accepted, and then not a single byte of the value is sent
      send(socket, "*2\r\n$3\r\nGET\r\n$300000000\r\n");

      // the connection thread is reading the value: whatever it set aside for it is set aside by now
      await().atMost(Duration.ofSeconds(10)).until(() -> isReadingAValue(executor));
      assertThat(allocatedBytes(executor) - before).as("bytes allocated for a value of which nothing arrived")
          .isLessThan(ALLOCATION_BUDGET);
    }
  }

  @Test
  void valuesOfEverySizeRoundTripAfterAuthentication() throws Exception {
    // around the size of the read buffer and of each step the buffer for a value grows by, and a few MB
    final int[] sizes = { 0, 1, 1_000, 32 * 1024 - 1, 32 * 1024, 32 * 1024 + 1, 64 * 1024, 100_000, 1_000_000, 3_000_000 };

    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");

      for (final int size : sizes) {
        final String value = pattern(size);
        assertThat(jedis.set("key" + size, value)).isEqualTo("OK");
        assertThat(jedis.get("key" + size)).as("value of %d bytes", size).isEqualTo(value);
      }
    }
  }

  @Test
  void aValueThatArrivesInPiecesRoundTrips() throws Exception {
    final int size = 100_000;
    final String value = pattern(size);

    try (final Socket socket = connect()) {
      socket.setTcpNoDelay(true);
      send(socket, "AUTH " + USER + " " + PASSWORD + "\r\n");
      assertThat(readLine(socket.getInputStream())).isEqualTo("+OK");

      send(socket, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$" + size + "\r\n");
      send(socket, value.substring(0, 10));
      send(socket, value.substring(10, 40_000));
      send(socket, value.substring(40_000, 99_999));
      send(socket, value.substring(99_999) + "\r\n");
      assertThat(readLine(socket.getInputStream())).isEqualTo("+OK");

      send(socket, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
      final InputStream in = socket.getInputStream();
      assertThat(readLine(in)).isEqualTo("$" + size);
      assertThat(new String(in.readNBytes(size), StandardCharsets.US_ASCII)).isEqualTo(value);
      assertThat(new String(in.readNBytes(2), StandardCharsets.US_ASCII)).isEqualTo("\r\n");
    }
  }

  private Socket connect() throws IOException {
    final Socket socket = new Socket("localhost", getServerRedisPort());
    // a hang detector: a reply that never comes fails the test instead of holding it
    socket.setSoTimeout(10_000);
    return socket;
  }

  private void assertServerStillServes() {
    try (final Jedis jedis = new Jedis("localhost", getServerRedisPort())) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  private static void send(final Socket socket, final String text) throws IOException {
    socket.getOutputStream().write(text.getBytes(StandardCharsets.US_ASCII));
    socket.getOutputStream().flush();
  }

  /** One reply line, without its CRLF. */
  private static String readLine(final InputStream in) throws IOException {
    final ByteArrayOutputStream line = new ByteArrayOutputStream();
    int b;
    while ((b = in.read()) != -1) {
      if (b == '\n')
        break;
      line.write(b);
    }
    final String text = line.toString(StandardCharsets.US_ASCII);
    return text.endsWith("\r") ? text.substring(0, text.length() - 1) : text;
  }

  /** True when the server ended the connection, with whatever it had still to say read first. */
  private static boolean isClosedByServer(final Socket socket) throws IOException {
    return socket.getInputStream().read() == -1;
  }

  private static String pattern(final int size) {
    final StringBuilder value = new StringBuilder(size);
    for (int i = 0; i < size; i++)
      value.append((char) ('a' + i % 26));
    return value.toString();
  }

  private static Thread findExecutorThread() {
    for (final Thread thread : Thread.getAllStackTraces().keySet())
      if (thread.getName().startsWith(Constants.PRODUCT + "-redis/"))
        return thread;
    return null;
  }

  /** True while the thread waits for the bytes of a bulk string: the value buffer of the declared length exists, or not, by then. */
  private static boolean isReadingAValue(final Thread executor) {
    boolean parsingTheValue = false;
    boolean waitingForInput = false;
    for (final StackTraceElement frame : executor.getStackTrace())
      if (frame.getClassName().equals(RedisNetworkExecutor.class.getName())) {
        parsingTheValue |= frame.getMethodName().equals("parseChars");
        waitingForInput |= frame.getMethodName().equals("fill");
      }
    return parsingTheValue && waitingForInput;
  }

  private static long allocatedBytes(final Thread thread) {
    return ((ThreadMXBean) ManagementFactory.getThreadMXBean()).getThreadAllocatedBytes(thread.threadId());
  }

  @Override
  protected void populateDatabase() {
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Redis Protocol:com.arcadedb.redis.RedisProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }
}
