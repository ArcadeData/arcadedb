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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #5923: {@code bindCommand()} only advanced {@code paramsConsumed} to
 * {@code i + 1} <b>after</b> {@code PostgresType.deserialize()} returned successfully, even though the
 * parameter's length-prefixed value bytes were already fully read off the wire beforehand. When
 * {@code deserialize()} throws (e.g. a parameter declared with a type code the server doesn't recognize),
 * the error-recovery drain in the {@code catch} block restarts at the stale {@code paramsConsumed} index,
 * re-reading bytes that were already consumed and misinterpreting whatever comes next on the wire (here,
 * the real {@code resultFormatCount} field and the following Sync message) as a bogus 32-bit length. That
 * either desynchronizes the channel so badly the connection thread blocks waiting for millions of bytes
 * that will never arrive, or - depending on the misread bytes - trips the {@code POSTGRES_MAX_PARAM_SIZE}
 * guard and forces a connection close, in both cases turning a single malformed Bind message into a dead
 * connection instead of a clean per-statement error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresBindDeserializeFailureRecoveryIT extends BaseGraphServerTest {

  // Deliberately not a real PostgreSQL OID (all built-in type codes are well under 10000): guarantees
  // PostgresType.deserialize() throws "Type with code ... not supported for deserializing".
  private static final int UNSUPPORTED_TYPE_OID = 999999;

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
  @DisplayName("[#5923] A Bind parameter that fails deserialization leaves the channel aligned for the next message")
  void bindDeserializeFailureDoesNotDesyncChannel() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword

      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      // Declare one parameter with a type code deserialize() cannot handle.
      sendParseWithExplicitParamType(out, "SELECT 1", UNSUPPORTED_TYPE_OID);
      readMessageOfType(in, '1'); // ParseComplete

      // A protocol-correct Bind: format codes, one text-format parameter value, then resultFormatCount.
      // No padding, no extra bytes - exactly what a real client sends. Immediately pipeline a Sync right
      // after it, as real clients commonly do.
      sendBindWithOneTextParam(out, "1");
      sendSync(out);
      out.flush();

      // With the channel correctly realigned, the very next message the server sends back must be the
      // ErrorResponse for the failed Bind, followed promptly by ReadyForQuery from the Sync. Before the
      // fix, the drain misreads the wire and the connection thread blocks (or the connection is dropped),
      // so no ErrorResponse/ReadyForQuery pair arrives within the timeout.
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        final int first = readMessageType(in);
        assertThat((char) first).as("ErrorResponse for the malformed Bind").isEqualTo('E');
        readMessageOfType(in, 'Z'); // ReadyForQuery from the pipelined Sync
      });

      // The connection must still be fully usable for a subsequent, unrelated statement - proof the
      // channel is realigned, not just that a single error happened to slip through.
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SELECT 1");
        final int responseType = readMessageType(in);
        assertThat((char) responseType).as("response to a well-formed query on the same connection")
            .isIn('T', 'C', 'D');
        readMessageOfType(in, 'Z');
      });
    }
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

  /**
   * Parse message for the unnamed statement, declaring one parameter with an explicit (caller-supplied)
   * type OID, rather than letting the server infer it.
   */
  private static void sendParseWithExplicitParamType(final DataOutputStream out, final String query, final int paramTypeOid)
      throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // unnamed statement
    writeCString(body, query);
    body.write(0);
    body.write(1); // int16 numParamDataTypes = 1
    body.write((paramTypeOid >>> 24) & 0xFF);
    body.write((paramTypeOid >>> 16) & 0xFF);
    body.write((paramTypeOid >>> 8) & 0xFF);
    body.write(paramTypeOid & 0xFF);

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('P');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  /**
   * Protocol-correct Bind message for the unnamed portal/statement: no format codes declared (defaults to
   * text), one text-format parameter, no declared result formats. Exactly what a well-behaved client sends
   * - no artificial padding.
   */
  private static void sendBindWithOneTextParam(final DataOutputStream out, final String paramValue) throws Exception {
    final byte[] valueBytes = paramValue.getBytes(StandardCharsets.UTF_8);

    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    writeCString(body, ""); // statement name
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0 (all text)
    body.write(0);
    body.write(1); // int16 numParamValues = 1
    body.write((valueBytes.length >>> 24) & 0xFF);
    body.write((valueBytes.length >>> 16) & 0xFF);
    body.write((valueBytes.length >>> 8) & 0xFF);
    body.write(valueBytes.length & 0xFF);
    body.write(valueBytes);
    body.write(0);
    body.write(0); // int16 numResultFormatCodes = 0

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('B');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  private static void sendSimpleQuery(final DataOutputStream out, final String query) throws Exception {
    final byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
    out.writeByte('Q');
    out.writeInt(4 + queryBytes.length + 1);
    out.write(queryBytes);
    out.writeByte(0);
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
