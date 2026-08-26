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
  void deeplyNestedArrayIsRejectedInsteadOfOverflowingTheStack() throws Exception {
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
  void oversizedArrayLengthIsRejectedImmediately() throws Exception {
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
  void oversizedBulkStringLengthIsRejectedImmediately() throws Exception {
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
  void malformedLengthClosesCleanlyInsteadOfCrashingTheThread() throws Exception {
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
  void malformedLengthReplyDoesNotEmbedRawNewline() throws Exception {
    // parseValueUntilLF() only treats \r as the start of the CRLF terminator, so a malformed length token
    // can itself contain a bare \n; that used to be echoed verbatim into the RESP -ERR reply instead of
    // going through respErrorMessage()'s \r/\n sanitization, breaking the single-line contract every other
    // RESP error reply relies on.
    final String payload = "$1\nA\r\n";

    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(payload.getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      final byte[] buffer = new byte[512];
      final int    read   = socket.getInputStream().read(buffer);
      assertThat(read).isGreaterThan(0);
      final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).endsWith("\r\n");
      final String body = reply.substring(0, reply.length() - 2);
      assertThat(body).doesNotContain("\r").doesNotContain("\n");
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void unterminatedTokenIsRejectedInsteadOfGrowingUnbounded() throws Exception {
    // The new size/depth checks only fire once parseValueUntilLF() has actually produced a token (it looks
    // for a terminating CRLF), so a client that never sends one - e.g. "$" followed by a very long run of
    // digits with no \r\n - grows that buffer unbounded and holds the thread before maxBulkLength ever gets
    // a value to check against. A real RESP length token is always short, so it must be rejected on its own.
    final String payload = "$" + "9".repeat(1000);

    try (final Socket socket = new Socket("localhost", DEF_PORT)) {
      socket.setSoTimeout(10_000);
      socket.getOutputStream().write(payload.getBytes(StandardCharsets.US_ASCII));
      socket.getOutputStream().flush();

      final byte[] buffer = new byte[512];
      final int    read   = socket.getInputStream().read(buffer);
      assertThat(read).isGreaterThan(0);
      final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
      assertThat(reply).startsWith("-ERR");
      assertThat(reply).containsIgnoringCase("maximum allowed length");
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally.
    try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
      assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  void configuredDepthLimitIsHonored() throws Exception {
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

  @Test
  void misconfiguredDepthLimitFallsBackToDefault() throws Exception {
    // Below sanitizedLimit's floor (2, since depth >= maxMultiBulkDepth would otherwise reject even a flat
    // command's single argument): must fall back to the built-in default (32) rather than locking every
    // connection out.
    GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH.setValue(1);
    try {
      final StringBuilder payload = new StringBuilder();
      for (int i = 0; i < 10; i++)
        payload.append("*1\r\n");
      payload.append("$1\r\nx\r\n");
      // Followed, on the SAME connection, by a normal PING - which needs depth 1 to parse its single
      // argument. If the broken value of 1 were used verbatim (no fallback), PING itself would be rejected
      // with "maximum allowed depth (1)"; if the fallback to 32 took effect, it parses normally and gets
      // as far as the pre-auth NOAUTH check.
      payload.append("*1\r\n$4\r\nPING\r\n");

      try (final Socket socket = new Socket("localhost", DEF_PORT)) {
        socket.setSoTimeout(10_000);
        socket.getOutputStream().write(payload.toString().getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();

        final byte[] buffer = new byte[512];
        final int    read   = socket.getInputStream().read(buffer);
        assertThat(read).isGreaterThan(0);
        final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
        assertThat(reply).doesNotContain("maximum allowed depth");
        assertThat(reply).contains("NOAUTH");
      }
    } finally {
      GlobalConfiguration.REDIS_MAX_MULTIBULK_DEPTH.reset();
    }
  }

  @Test
  void configuredMultiBulkLengthLimitIsHonored() throws Exception {
    // Confirms arcadedb.redis.maxMultiBulkLength is actually wired end to end, not just the built-in default.
    GlobalConfiguration.REDIS_MAX_MULTIBULK_LENGTH.setValue(5);
    try {
      final String payload = "*6\r\n";

      try (final Socket socket = new Socket("localhost", DEF_PORT)) {
        socket.setSoTimeout(10_000);
        socket.getOutputStream().write(payload.getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();

        final byte[] buffer = new byte[512];
        final int    read   = socket.getInputStream().read(buffer);
        assertThat(read).isGreaterThan(0);
        final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
        assertThat(reply).startsWith("-ERR");
        assertThat(reply).contains("maximum allowed is 5");
      }

      // Ordinary traffic (AUTH's 3 elements, PING's 1) is well under the lowered limit and must still work.
      try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
        assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
        assertThat(jedis.ping()).isEqualTo("PONG");
      }
    } finally {
      GlobalConfiguration.REDIS_MAX_MULTIBULK_LENGTH.reset();
    }
  }

  @Test
  void configuredBulkLengthLimitIsHonored() throws Exception {
    // Confirms arcadedb.redis.maxBulkLength is actually wired end to end, not just the built-in default.
    // 64 comfortably fits every bulk string AUTH/PING send (including the test password), while still being
    // a small, clearly non-default value to prove the setting is honored rather than ignored.
    GlobalConfiguration.REDIS_MAX_BULK_LENGTH.setValue(64);
    try {
      final String payload = "$100\r\n";

      try (final Socket socket = new Socket("localhost", DEF_PORT)) {
        socket.setSoTimeout(10_000);
        socket.getOutputStream().write(payload.getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();

        final byte[] buffer = new byte[512];
        final int    read   = socket.getInputStream().read(buffer);
        assertThat(read).isGreaterThan(0);
        final String reply = new String(buffer, 0, read, StandardCharsets.US_ASCII);
        assertThat(reply).startsWith("-ERR");
        assertThat(reply).contains("maximum allowed is 64");
      }

      // Ordinary traffic well under the lowered limit must still be accepted.
      try (final Jedis jedis = new Jedis("localhost", DEF_PORT)) {
        assertThat(jedis.auth(USER, PASSWORD)).isEqualTo("OK");
        assertThat(jedis.ping()).isEqualTo("PONG");
      }
    } finally {
      GlobalConfiguration.REDIS_MAX_BULK_LENGTH.reset();
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
