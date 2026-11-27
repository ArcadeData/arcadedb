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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readWireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendParseWithTypes;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The sizes a Bind or Parse message declares are held to the message that carries them: a parameter, or a count of parameters,
 * format codes or types, that does not fit in what the message has left - or leaves some of it unused - is refused before any
 * buffer is sized for it, and a value that does fit is read as its bytes arrive, so a value delivered in several pieces binds
 * exactly as one delivered at once.
 * <p>
 * A read that waits for bytes the message never carries is what these tests would hang on without the checks, so every socket has a
 * read timeout that turns the wait into a failure instead of a stuck build. The timeout is a hang detector, not a latency bound.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresBindMessageBoundsIT extends PostgresWireProtocolTestBase {
  private static final String TYPE_NAME          = "BindBoundsT";
  private static final int    UNSUPPORTED_TYPE   = 999999;
  private static final int    READ_TIMEOUT_MS    = 20_000;
  private static final int    DELIVERY_SLICE     = 40_000;
  private static final String SQLSTATE_PROTOCOL  = "08P01";

  @BeforeEach
  void createType() {
    getServerDatabase(0, getDatabaseName()).getSchema().getOrCreateDocumentType(TYPE_NAME);
  }

  /** The last two cases leave no room for the result format count the message ends with. */
  @ParameterizedTest(name = "declared {0} bytes, {1} supplied")
  @CsvSource({ "1048576, 0", "100, 10", "65537, 65536", "16777216, 1000", "10, 10", "0, 0" })
  void aParameterThatDoesNotFitInTheRestOfTheMessageIsRefused(final int declared, final int supplied) throws Exception {
    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendParse(out, "", "SELECT $1");
      readWireMessage(in); // ParseComplete

      final ByteArrayOutputStream body = bindHeader(0, 1);
      writeInt(body, declared);
      body.write(new byte[supplied]);
      sendRawBind(out, body.toByteArray());

      assertRefusedAndClosed(in, SQLSTATE_PROTOCOL);
    }

    assertServerStillServesANewClient();
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "parameter format codes", "parameters", "result format codes", "no result format count", "too few result format codes" })
  void aCountThatDoesNotFitInTheMessageIsRefused(final String counted) throws Exception {
    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendParse(out, "", "SELECT 1");
      readWireMessage(in); // ParseComplete

      final ByteArrayOutputStream body = switch (counted) {
        case "parameter format codes" -> {
          final ByteArrayOutputStream result = bindHeader(60_000, 0);
          writeShort(result, 0); // result format count
          yield result;
        }
        case "parameters" -> {
          final ByteArrayOutputStream result = bindHeader(0, 60_000);
          writeShort(result, 0); // result format count
          yield result;
        }
        case "result format codes" -> {
          final ByteArrayOutputStream result = bindHeader(0, 0);
          writeShort(result, 30_000);
          yield result;
        }
        case "too few result format codes" -> {
          final ByteArrayOutputStream result = bindHeader(0, 0);
          writeShort(result, 2);
          writeShort(result, 1); // one code of the two
          yield result;
        }
        // the message ends where the result format count should start
        default -> bindHeader(0, 0);
      };
      sendRawBind(out, body.toByteArray());

      assertRefusedAndClosed(in, SQLSTATE_PROTOCOL);
    }

    assertServerStillServesANewClient();
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "bind", "parse" })
  void aMessageWithBytesLeftAfterItsLastFieldIsRefused(final String message) throws Exception {
    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      if ("bind".equals(message)) {
        sendParse(out, "", "SELECT 1");
        readWireMessage(in); // ParseComplete

        final ByteArrayOutputStream body = bindHeader(0, 0);
        writeShort(body, 0); // result format count
        body.write(0); // one byte more than the message has fields for
        sendRawBind(out, body.toByteArray());
      } else {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        writeCString(body, ""); // statement name
        writeCString(body, "SELECT 1");
        writeShort(body, 0); // parameter types
        body.write(0); // one byte more than the message has fields for
        out.write(framed('P', body.toByteArray()));
        out.flush();
      }

      assertRefusedAndClosed(in, SQLSTATE_PROTOCOL);
    }

    assertServerStillServesANewClient();
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(strings = { "more parameter types than the message holds", "fewer parameter types than the message holds",
      "no room for the parameter type count" })
  void aParseWhoseSizesDoNotFitItsLengthIsRefused(final String shape) throws Exception {
    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      final ByteArrayOutputStream body = new ByteArrayOutputStream();
      writeCString(body, ""); // statement name
      writeCString(body, "SELECT 1");
      switch (shape) {
      case "more parameter types than the message holds" -> writeShort(body, 60_000); // none of which follows
      case "fewer parameter types than the message holds" -> {
        writeShort(body, 1);
        writeInt(body, 0);
        writeInt(body, 0); // a second type that was not declared
      }
      default -> {
        // the message ends before the count
      }
      }
      out.write(framed('P', body.toByteArray()));
      out.flush();

      assertRefusedAndClosed(in, SQLSTATE_PROTOCOL);
    }

    assertServerStillServesANewClient();
  }

  @Test
  void aResultFormatCountThatDoesNotFitIsRefusedAfterAParameterThatFailed() throws Exception {
    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendParseWithTypes(out, "", "SELECT $1", UNSUPPORTED_TYPE);
      readWireMessage(in); // ParseComplete

      // The parameter fails to convert, so the rest of the message is skipped: its result format count claims codes it does not hold
      final ByteArrayOutputStream body = bindHeader(0, 1);
      final byte[] value = "1".getBytes(StandardCharsets.UTF_8);
      writeInt(body, value.length);
      body.write(value);
      writeShort(body, 30_000);
      sendRawBind(out, body.toByteArray());

      assertRefusedAndClosed(in, null);
    }

    assertServerStillServesANewClient();
  }

  @ParameterizedTest(name = "{0} bytes")
  @ValueSource(ints = { 0, 1, 65_535, 65_536, 65_537, 131_072, 300_001 })
  void aParameterDeliveredInPiecesIsBoundWhole(final int size) throws Exception {
    final String value = valueOfSize(size);

    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendParse(out, "", "INSERT INTO " + TYPE_NAME + " SET k = " + size + ", s = $1");

      final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
      final ByteArrayOutputStream body = bindHeader(0, 1);
      writeInt(body, valueBytes.length);
      body.write(valueBytes);
      writeShort(body, 0); // no result format codes

      // The Bind goes out in slices, with a pause between them, so the server has to take the value as it arrives
      final byte[] message = framed('B', body.toByteArray());
      for (int offset = 0; offset < message.length; offset += DELIVERY_SLICE) {
        out.write(message, offset, Math.min(DELIVERY_SLICE, message.length - offset));
        out.flush();
        Thread.sleep(5);
      }
      sendExecute(out, "");
      sendSync(out);

      final List<Character> types = messageTypesOf(readUntilReadyForQuery(in));
      assertThat(types).as("the statement is parsed, bound and run without an error").contains('1', '2', 'Z').doesNotContain('E');
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet resultSet = database.query("sql", "SELECT s FROM " + TYPE_NAME + " WHERE k = ?", size)) {
      assertThat(resultSet.hasNext()).isTrue();
      assertThat(resultSet.next().<String>getProperty("s")).isEqualTo(value);
    }
  }

  @ParameterizedTest(name = "{0} bytes")
  @ValueSource(ints = { 70_000, 300_001 })
  void largeParametersBehindAParameterThatFailsAreSkippedAndTheConnectionStaysAligned(final int size) throws Exception {
    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      // The first parameter is declared with a type the server does not read, so the Bind fails after reading it and the values
      // behind it - the large ones - are still on the wire
      sendParseWithTypes(out, "", "INSERT INTO " + TYPE_NAME + " SET k = -1, a = $1, b = $2, c = $3", UNSUPPORTED_TYPE, 0, 0);
      readWireMessage(in); // ParseComplete

      final String large = valueOfSize(size);
      sendBind(out, "", "", "1", large, large + "x");
      sendSync(out);

      final List<WireMessage> answers = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answers)).containsExactly('E', 'Z');

      // The channel is aligned after the skipped values: the next statement is answered as usual
      sendSimpleQuery(out, "SELECT 1");
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE k = -1")) {
      assertThat(resultSet.hasNext()).isFalse();
    }
  }

  private Socket connectedSocket() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    socket.setSoTimeout(READ_TIMEOUT_MS);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  /**
   * The answer to a refused message is an ErrorResponse naming the protocol, and the end of the connection: an end of stream, or a
   * reset when the client's unread bytes were still queued on the server's side when it closed.
   */
  private static void assertRefusedAndClosed(final DataInputStream in, final String expectedSqlState) throws Exception {
    final WireMessage answer = readWireMessage(in);
    assertThat(answer.type()).isEqualTo('E');
    if (expectedSqlState != null)
      assertThat(errorFields(answer).get('C')).isEqualTo(expectedSqlState);
    assertThatThrownBy(in::readUnsignedByte).isInstanceOfAny(EOFException.class, SocketException.class);
  }

  /** A refused connection must not have wedged the listener: a new client is served right after. */
  private void assertServerStillServesANewClient() throws Exception {
    try (final Socket socket = connectedSocket()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);
      sendSimpleQuery(out, "SELECT 1");
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
    }
  }

  /** The portal and statement names, then the parameter format code count (no codes follow) and the parameter count. */
  private static ByteArrayOutputStream bindHeader(final int formatCodeCount, final int parameterCount) {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    writeCString(body, ""); // statement name
    writeShort(body, formatCodeCount);
    if (parameterCount >= 0)
      writeShort(body, parameterCount);
    return body;
  }

  /** A Bind whose length field covers exactly the bytes sent, which is what a client that stops mid-message looks like. */
  private static void sendRawBind(final DataOutputStream out, final byte[] body) throws Exception {
    out.write(framed('B', body));
    out.flush();
  }

  private static byte[] framed(final char type, final byte[] body) {
    final ByteArrayOutputStream message = new ByteArrayOutputStream(body.length + 5);
    message.write(type);
    writeInt(message, 4 + body.length);
    message.writeBytes(body);
    return message.toByteArray();
  }

  private static void writeShort(final ByteArrayOutputStream out, final int value) {
    out.write(value >>> 8);
    out.write(value);
  }

  private static void writeInt(final ByteArrayOutputStream out, final int value) {
    out.write(value >>> 24);
    out.write(value >>> 16);
    out.write(value >>> 8);
    out.write(value);
  }

  /** Letters in a pattern that drifts as the value grows, so a value that came back shifted or cut differs from the one sent. */
  private static String valueOfSize(final int size) {
    final StringBuilder value = new StringBuilder(size);
    for (int i = 0; i < size; i++)
      value.append((char) ('a' + (i + i / 26) % 26));
    return value.toString();
  }
}
