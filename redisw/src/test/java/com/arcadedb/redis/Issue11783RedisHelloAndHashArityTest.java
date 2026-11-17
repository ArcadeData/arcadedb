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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11783: the arity census exempted HELLO, HGET and HEXISTS on the grounds that each checked its own shape, and none did.
 * <ul>
 *   <li>wire {@code HELLO} skipped a protover that was no number and read it as an option, answered an out-of-range or empty one
 *   with the text of a {@code NumberFormatException} (before authentication too), read {@code AUTH} at one position only and
 *   accepted any word after it or in its place;</li>
 *   <li>the query-language {@code HGET} and {@code HEXISTS} checked a minimum count only, so {@code HGET P[id] 1 2 3} answered
 *   key 1 and {@code HGET #rid extra junk} answered the record.</li>
 * </ul>
 * An unknown wire command answers {@code -ERR unknown command '<name>'}, as Redis does, where it answered
 * {@code -Command not found}, whose first word a client reads as the error kind.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11783RedisHelloAndHashArityTest extends BaseRedisServerTest {
  private static final int HELLO_MAP_ELEMENTS = 14;

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
  void helloRefusesABadProtocolVersionBeforeAuthentication() throws Exception {
    try (final Socket socket = connect()) {
      for (final String version : new String[] { "99999999999999999999", "-", "", "abc", "3.0", "+3", "03", "-0", "\u0663" }) {
        sendCommand(socket, "HELLO", version);
        assertThat(readReply(socket)).as(version).isEqualTo("-ERR Protocol version is not an integer or out of range");
      }
      for (final String version : new String[] { "99999999999", "1", "4", "-1", "0" }) {
        sendCommand(socket, "HELLO", version);
        assertThat(readReply(socket)).as(version).isEqualTo("-NOPROTO unsupported protocol version");
      }
      // a non-canonical version does not reach the credentials that follow it
      sendCommand(socket, "HELLO", "+3", "AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol version is not an integer or out of range");
      sendCommand(socket, "GET", "k");
      assertThat(readReply(socket)).startsWith("-NOAUTH");
      // a word that is no version is not read as an option either: the credentials after it authenticate nobody
      sendCommand(socket, "HELLO", "AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol version is not an integer or out of range");
      sendCommand(socket, "GET", "k");
      assertThat(readReply(socket)).startsWith("-NOAUTH");
    }
  }

  @Test
  void helloRefusesAnUnknownOrTrailingOptionAndAuthenticatesNobody() throws Exception {
    try (final Socket socket = connect()) {
      sendCommand(socket, "HELLO", "3", "AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS, "BOGUS", "extra");
      assertThat(readReply(socket)).isEqualTo("-ERR Syntax error in HELLO option 'BOGUS'");
      sendCommand(socket, "HELLO", "2", "junk1", "junk2", "junk3");
      assertThat(readReply(socket)).isEqualTo("-ERR Syntax error in HELLO option 'junk1'");
      // an option missing its values is not an option
      sendCommand(socket, "HELLO", "3", "AUTH", "root");
      assertThat(readReply(socket)).isEqualTo("-ERR Syntax error in HELLO option 'AUTH'");
      sendCommand(socket, "HELLO", "3", "SETNAME");
      assertThat(readReply(socket)).isEqualTo("-ERR Syntax error in HELLO option 'SETNAME'");
      // the echoed word stays on one line, so the reply framing holds
      sendCommand(socket, "HELLO", "3", "a\r\n+OK");
      assertThat(readReply(socket)).isEqualTo("-ERR Syntax error in HELLO option 'a  +OK'");

      // none of them authenticated the connection
      sendCommand(socket, "GET", "k");
      assertThat(readReply(socket)).startsWith("-NOAUTH");
    }
  }

  @Test
  void helloAcceptsTheDocumentedForms() throws Exception {
    try (final Socket socket = connect()) {
      // SETNAME, before or after AUTH, as redis-py and Lettuce send it
      sendCommand(socket, "HELLO", "3", "SETNAME", "myname", "AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS);
      assertHelloMap(socket);
      sendCommand(socket, "HELLO", "2", "auth", "root", DEFAULT_PASSWORD_FOR_TESTS, "setname", "other");
      assertHelloMap(socket);
      sendCommand(socket, "HELLO", "3");
      assertHelloMap(socket);
      sendCommand(socket, "HELLO");
      assertHelloMap(socket);

      sendCommand(socket, "HELLO", "3", "SETNAME", "my name");
      assertThat(readReply(socket)).isEqualTo("-ERR Client names cannot contain spaces, newlines or special characters.");
      sendCommand(socket, "HELLO", "abc");
      assertThat(readReply(socket)).isEqualTo("-ERR Protocol version is not an integer or out of range");

      sendCommand(socket, "PING");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void authRefusesAnyOtherCount() throws Exception {
    try (final Socket socket = connect()) {
      sendCommand(socket, "AUTH", "a", "b", "c");
      assertThat(readReply(socket)).isEqualTo("-ERR wrong number of arguments for 'auth' command");
      sendCommand(socket, "AUTH");
      assertThat(readReply(socket)).isEqualTo("-ERR wrong number of arguments for 'auth' command");
    }
  }

  @Test
  void unknownWireCommandAnswersAsRedisDoes() throws Exception {
    try (final Socket socket = connect()) {
      sendCommand(socket, "AUTH", "root", DEFAULT_PASSWORD_FOR_TESTS);
      assertThat(readReply(socket)).isEqualTo("+OK");
      sendCommand(socket, "foo", "bar");
      assertThat(readReply(socket)).isEqualTo("-ERR unknown command 'foo'");
      sendCommand(socket, "x\r\n+OK");
      assertThat(readReply(socket)).isEqualTo("-ERR unknown command 'x  +OK'");
      sendCommand(socket, "PING");
      assertThat(readReply(socket)).isEqualTo("+PONG");
    }
  }

  @Test
  void queryLanguageHGetAndHExistsTakeTheCountOfTheirForm() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "CREATE DOCUMENT TYPE P").close();
    database.command("sql", "CREATE PROPERTY P.id INTEGER").close();
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE").close();
    final MutableDocument document = database.newDocument("P").set("id", 1);
    database.transaction(() -> document.save());
    final String rid = document.getIdentity().toString();

    for (final String command : new String[] { "HGET " + rid + " extra junk", "HGET " + rid + " extra", "HGET P[id] 1 2 3",
        "HGET P[id] 1 2", "HGET P[id]", "HGET" })
      assertThatThrownBy(() -> database.command("redis", command).close()).as(command)
          .hasMessageContaining("wrong number of arguments for 'hget' command");
    for (final String command : new String[] { "HEXISTS " + rid + " extra", "HEXISTS P[id] 1 extra", "HEXISTS P[id]", "HEXISTS" })
      assertThatThrownBy(() -> database.command("redis", command).close()).as(command)
          .hasMessageContaining("wrong number of arguments for 'hexists' command");
    assertThatThrownBy(() -> database.command("redis", "HMGET P[id]").close())
        .hasMessageContaining("wrong number of arguments for 'hmget' command");

    // the documented forms still answer
    try (final ResultSet rs = database.command("redis", "HGET " + rid)) {
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
    }
    try (final ResultSet rs = database.command("redis", "HGET P[id] 1")) {
      assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(1);
    }
    try (final ResultSet rs = database.command("redis", "HEXISTS " + rid)) {
      assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(1);
    }
    try (final ResultSet rs = database.command("redis", "HEXISTS P[id] 2")) {
      assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(0);
    }
    try (final ResultSet rs = database.command("redis", "HMGET P[id] 1 2")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /** Reads a HELLO reply: the map header and every one of its elements, so the next reply starts in sync. */
  private static void assertHelloMap(final Socket socket) throws IOException {
    assertThat(readReply(socket)).isEqualTo("*" + HELLO_MAP_ELEMENTS);
    for (int i = 0; i < HELLO_MAP_ELEMENTS; i++)
      assertThat(readReply(socket)).doesNotStartWith("-");
  }

  private Socket connect() throws IOException {
    final Socket socket = new Socket("localhost", getServerRedisPort());
    socket.setSoTimeout(10_000);
    return socket;
  }

  private static void sendCommand(final Socket socket, final String... args) throws IOException {
    final StringBuilder sb = new StringBuilder();
    sb.append('*').append(args.length).append("\r\n");
    for (final String arg : args)
      sb.append('$').append(arg.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(arg).append("\r\n");
    socket.getOutputStream().write(sb.toString().getBytes(StandardCharsets.UTF_8));
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
