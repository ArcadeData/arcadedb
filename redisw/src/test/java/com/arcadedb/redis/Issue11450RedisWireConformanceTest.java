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
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11450, two RESP conformance gaps of the wire path:
 * <ol>
 *   <li>PING and SELECT were missing from {@link RedisCommandArity}: {@code SELECT} with no argument answered the Java text
 *   {@code Index 1 out of bounds for length 1}, {@code SELECT <db> extra} and {@code PING a b} were accepted. The query language
 *   shares the PING entry, so {@code PING a b} is refused there too.</li>
 *   <li>A request argument (not the command name, which #10845 fixed) that is an integer or a nested array reached the
 *   handler's {@code (String)} cast and was answered with the {@code ClassCastException} text.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11450RedisWireConformanceTest extends BaseRedisServerTest {

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

  @Test
  void selectAndPingAreRefusedWithAWrongArgumentCount() throws Exception {
    try (final Socket socket = connectAndAuthenticate()) {
      sendCommand(socket, "SELECT");
      assertThat(readReply(socket)).isEqualTo("-ERR wrong number of arguments for 'select' command");

      sendCommand(socket, "SELECT", getDatabaseName(), "extra");
      assertThat(readReply(socket)).isEqualTo("-ERR wrong number of arguments for 'select' command");

      sendCommand(socket, "PING", "a", "b");
      assertThat(readReply(socket)).isEqualTo("-ERR wrong number of arguments for 'ping' command");

      sendCommand(socket, "PING", "a", "b", "c");
      assertThat(readReply(socket)).isEqualTo("-ERR wrong number of arguments for 'ping' command");

      // the documented forms still run on the same connection
      sendCommand(socket, "PING");
      assertThat(readReply(socket)).isEqualTo("+PONG");
      sendCommand(socket, "PING", "hello");
      assertThat(readReply(socket)).isEqualTo("$5\r\nhello");
      sendCommand(socket, "SELECT", getDatabaseName());
      assertThat(readReply(socket)).isEqualTo("+OK");
    }
  }

  @Test
  void queryLanguagePingRefusesASurplusArgument() {
    final Database database = getServerDatabase(0, getDatabaseName());

    assertThatThrownBy(() -> database.command("redis", "PING a b").close())
        .hasMessageContaining("wrong number of arguments for 'ping' command");

    try (final ResultSet rs = database.command("redis", "PING 'a b'")) {
      assertThat(rs.next().<String>getProperty("value")).isEqualTo("a b");
    }
  }

  /**
   * The position of the non-string element is the parameter: the command name (#10845, the control) and each argument after it.
   */
  @ParameterizedTest
  @ValueSource(ints = { 0, 1, 2 })
  void aNonStringElementIsAProtocolErrorWhereverItIs(final int position) throws Exception {
    try (final Socket socket = connectAndAuthenticate()) {
      sendRaw(socket, withElementAt(position, ":1\r\n"));
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol error: expected '$', got ':'");

      sendRaw(socket, withElementAt(position, "*1\r\n$1\r\nk\r\n"));
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol error: expected '$', got '*'");

      // an empty nested array and one holding a null bulk string are arrays too
      sendRaw(socket, withElementAt(position, "*0\r\n"));
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol error: expected '$', got '*'");
      sendRaw(socket, withElementAt(position, "*1\r\n$-1\r\n"));
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol error: expected '$', got '*'");

      // the framing is intact: the connection goes on
      sendCommand(socket, "PING");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void aNonStringArgumentIsRefusedBeforeAuthenticationToo() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerRedisPort())) {
      socket.setSoTimeout(10_000);
      sendRaw(socket, "*3\r\n$4\r\nAUTH\r\n$4\r\nroot\r\n:1\r\n");
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol error: expected '$', got ':'");
    }
  }

  /** {@code SET k v} with the element at {@code position} replaced by {@code element}, a raw RESP value. */
  private static String withElementAt(final int position, final String element) {
    final String[] elements = { "$3\r\nSET\r\n", "$1\r\nk\r\n", "$1\r\nv\r\n" };
    elements[position] = element;
    return "*3\r\n" + String.join("", elements);
  }

  private Socket connectAndAuthenticate() throws IOException {
    final Socket socket = new Socket("localhost", getServerRedisPort());
    socket.setSoTimeout(10_000);
    sendCommand(socket, "AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS);
    assertThat(readReply(socket)).isEqualTo("+OK");
    return socket;
  }

  private static void sendCommand(final Socket socket, final String... args) throws IOException {
    final StringBuilder sb = new StringBuilder();
    sb.append('*').append(args.length).append("\r\n");
    for (final String arg : args)
      sb.append('$').append(arg.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(arg).append("\r\n");
    sendRaw(socket, sb.toString());
  }

  private static void sendRaw(final Socket socket, final String raw) throws IOException {
    socket.getOutputStream().write(raw.getBytes(StandardCharsets.UTF_8));
    socket.getOutputStream().flush();
  }

  /** One reply frame without its trailing CRLF: {@code +OK}, {@code -ERR ...} or {@code $5\r\nhello}. */
  private static String readReply(final Socket socket) throws IOException {
    final String first = readLine(socket);
    if (first.startsWith("$") && !first.equals("$-1"))
      return first + "\r\n" + readLine(socket);
    return first;
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
}
