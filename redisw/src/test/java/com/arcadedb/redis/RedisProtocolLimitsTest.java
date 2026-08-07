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
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5895: the Redis wire-protocol listener parsed RESP arrays with unbounded
 * recursion, so a small ({@code ~47 KB}) deeply-nested payload overflowed the connection thread's JVM
 * stack before authentication ran. A client-supplied array-length header was equally unbounded, letting a
 * single {@code *2000000000\r\n} header set up a two-billion-iteration parse loop. Both are reachable
 * pre-auth by anyone who can open the Redis port.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RedisProtocolLimitsTest extends BaseGraphServerTest {

  private static final int    DEF_PORT = GlobalConfiguration.REDIS_PORT.getValueAsInteger();
  private static final String USER     = "root";
  private static final String PASSWORD = DEFAULT_PASSWORD_FOR_TESTS;

  @Test
  void deeplyNestedArrayIsRejectedInsteadOfOverflowingTheStack() throws IOException {
    // Well beyond both the configured nesting cap and the depth that used to overflow the default JVM
    // stack (~11,861 levels / ~47 KB in the original report), so the fix is exercised either way.
    final int          depth   = 50_000;
    final StringBuilder payload = new StringBuilder();
    for (int i = 0; i < depth; i++)
      payload.append("*1\r\n");
    payload.append("$1\r\nx\r\n");

    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(payload.toString().getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      // The server must reject the oversized nesting and close the connection instead of dying with an
      // uncaught StackOverflowError: reading from the socket returns a diagnostic error reply (or at
      // least EOF), never a hang and never a raw crash.
      final byte[] buffer = new byte[512];
      final int    read   = socket.getInputStream().read(buffer);
      if (read > 0) {
        final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
        assertThat(reply).startsWith("-ERR");
      }
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void oversizedArrayLengthIsRejectedImmediately() throws IOException {
    // A header this large would previously start a two-billion-iteration parse loop, tying up the
    // connection thread for as long as the client trickles bytes.
    final String payload = "*2000000000\r\n";

    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(payload.getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      final byte[] buffer = new byte[512];
      final int    read   = socket.getInputStream().read(buffer);
      assertThat(read).isGreaterThan(0);
      final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("multibulk length");
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void oversizedBulkStringLengthIsRejectedImmediately() throws IOException {
    // Same DoS class as the array-length case above, but on the $ path every command actually uses (the
    // command name and every argument, including GET/SET's own payloads, are RESP bulk strings). A header
    // this large would previously tie up the connection thread indefinitely and, if the client actually sent
    // the declared bytes, grow the parse buffer without bound.
    final String payload = "$2000000000\r\n";

    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(payload.getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      final byte[] buffer = new byte[512];
      final int    read   = socket.getInputStream().read(buffer);
      assertThat(read).isGreaterThan(0);
      final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("bulk length");
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void malformedLengthClosesCleanlyInsteadOfCrashingTheThread() throws IOException {
    // A non-numeric length used to throw an uncaught NumberFormatException, killing the connection thread
    // outright instead of getting the same -ERR Protocol error + close treatment as the size-related cases.
    final String payload = "$abc\r\n";

    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(payload.getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      final byte[] buffer = new byte[512];
      final int    read   = socket.getInputStream().read(buffer);
      assertThat(read).isGreaterThan(0);
      final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("bulk length");
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void configuredDepthLimitIsHonored() throws IOException {
    // Confirms arcadedb.redis.maxMultiBulkDepth is actually wired end to end, rather than the tests above
    // only ever exercising the (also never-directly-asserted) built-in default.
    GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH.setValue(3);
    try {
      final StringBuilder payload = new StringBuilder();
      for (int i = 0; i < 5; i++)
        payload.append("*1\r\n");
      payload.append("$1\r\nx\r\n");

      try (final Socket socket = new Socket("localhost", DEF_PORT)) {
        socket.setSoTimeout(10_000);
        socket.getOutputStream().write(payload.toString().getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();

        final byte[] buffer = new byte[512];
        final int    read   = socket.getInputStream().read(buffer);
        assertThat(read).isGreaterThan(0);
        final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
        assertThat(reply).startsWith("-ERR");
        assertThat(reply).contains("maximum allowed depth (3)");
      }

      // A flat command (depth 1) must still be accepted at the lowered limit.
      try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
        assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
        assertThat(jedis.ping()).isEqualTo("PONG");
      }
    } finally {
      GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH.reset();
    }
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
