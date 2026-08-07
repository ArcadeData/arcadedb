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
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #5894: the Postgres wire-protocol Bind message reads a client-supplied,
 * unbounded parameter length and allocated a byte array sized by it with no upper bound. An authenticated
 * client declaring a multi-gigabyte parameter could OOM or thrash the server before any payload byte
 * arrives. {@code arcadedb.postgres.maxParamSize} now rejects an oversized declared length before the
 * allocation happens.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresBindParamBoundIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.POSTGRES_DEBUG.setValue("false");
    super.endTest();
  }

  @Override
  protected String getDatabaseName() {
    return "postgresdb";
  }

  @Test
  @DisplayName("[#5894] Oversized Bind parameter length is rejected before allocation, with a graceful error instead of an OOM'd thread")
  void oversizedBindParameterIsRejectedGracefully() throws Exception {
    try (final Socket attacker = new Socket()) {
      attacker.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(attacker.getOutputStream());
      final DataInputStream in = new DataInputStream(attacker.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword

      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      sendParse(out, "SELECT 1");
      readMessage(in); // ParseComplete

      // Bind message declaring a single parameter whose 32-bit length claims ~2GB. The attacker never
      // sends anything resembling a 2GB payload - only the declared length - mirroring the report's
      // "declare a huge length, don't follow through" shape.
      sendMaliciousBind(out, 0x7FFFFFFF);
      out.flush();

      // Without a bound check, the server allocates `new byte[0x7FFFFFFF]`, which throws OutOfMemoryError
      // (an Error, not an Exception) - it is never caught by bindCommand's `catch (Exception e)`, so the
      // connection thread dies silently and the client never gets a response, hanging here. With the
      // bound check, the value bytes were never read (so the channel cannot be safely resynchronized
      // without risking the same unbounded/blocking read this fix closes), so the server replies with a
      // normal ErrorResponse ('E') and then closes the connection outright, well within the timeout.
      final int responseType = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> readMessageType(in));
      org.assertj.core.api.Assertions.assertThat((char) responseType).isEqualTo('E');

      // The connection is not left half-aligned or hanging: the server closes it after the error, so a
      // further read reaches EOF (rather than blocking or returning garbage from a desynced channel).
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> in.readUnsignedByte())
            .isInstanceOf(java.io.EOFException.class);
      });
    }

    // The rejected connection must not have wedged the shared listener/thread pool: a fresh,
    // well-behaved client is still served promptly right after.
    assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
      try (final Socket client = new Socket()) {
        client.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
        final DataOutputStream out = new DataOutputStream(client.getOutputStream());
        final DataInputStream in = new DataInputStream(client.getInputStream());
        sendStartupMessage(out, "root", getDatabaseName());
        readMessage(in); // AuthenticationCleartextPassword
        sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
        readMessageOfType(in, 'Z'); // AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
      }
    });
  }

  private static void sendStartupMessage(final DataOutputStream out, final String user, final String database) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, "user");
    writeCString(body, user);
    writeCString(body, "database");
    writeCString(body, database);
    body.write(0);

    final byte[] bodyBytes = body.toByteArray();
    out.writeInt(4 + 4 + bodyBytes.length);
    out.writeInt(196608); // protocol version 3.0
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendPasswordMessage(final DataOutputStream out, final String password) throws Exception {
    final byte[] pwBytes = password.getBytes(StandardCharsets.UTF_8);
    out.writeByte('p');
    out.writeInt(4 + pwBytes.length + 1);
    out.write(pwBytes);
    out.writeByte(0);
    out.flush();
  }

  private static void sendParse(final DataOutputStream out, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // unnamed statement
    writeCString(body, query);
    body.write(0);
    body.write(0); // int16 numParamDataTypes = 0

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('P');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  /**
   * Bind message for the unnamed portal/statement, declaring one parameter with the given (attacker
   * controlled) 32-bit length, then stopping - no payload bytes follow.
   */
  private static void sendMaliciousBind(final DataOutputStream out, final int declaredParamLength) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    writeCString(body, ""); // statement name
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0
    body.write(0);
    body.write(1); // int16 numParamValues = 1
    body.write((declaredParamLength >>> 24) & 0xFF);
    body.write((declaredParamLength >>> 16) & 0xFF);
    body.write((declaredParamLength >>> 8) & 0xFF);
    body.write(declaredParamLength & 0xFF);
    // no payload follows

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('B');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void writeCString(final ByteArrayOutputStream out, final String s) {
    out.writeBytes(s.getBytes(StandardCharsets.UTF_8));
    out.write(0);
  }

  private static void readMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    in.skipNBytes(length - 4);
  }

  private static void readMessageOfType(final DataInputStream in, final char expectedType) throws Exception {
    while (true) {
      final int type = in.readUnsignedByte();
      final int length = in.readInt();
      in.skipNBytes(length - 4);
      if (type == expectedType)
        return;
    }
  }

  private static int readMessageType(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    in.skipNBytes(length - 4);
    return type;
  }
}
