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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #6930: a {@code Describe('P')} on a portal bound from {@code SET}/{@code SAVEPOINT}/
 * {@code RELEASE}/{@code ROLLBACK TO} sent no reply at all (every {@code Describe} must be answered by exactly
 * one of {@code RowDescription} or {@code NoData}), and the matching {@code Execute} answered {@code NoData}
 * ({@code 'n'}, a Describe-only reply) instead of {@code CommandComplete} ({@code 'C'}).
 * <p>
 * These tests speak the wire protocol directly over a raw socket so the extended-query {@code Describe('P')}
 * path is actually exercised, and so the exact reply sequence and the {@code CommandComplete} tag content can
 * be inspected (a JDBC client that always describes the statement, rather than the portal, would never reach
 * the first bug, and pgjdbc historically tolerated the second by accident - see the issue for why).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6930DescribeExecuteSetSavepointIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#6930] Describe(P)+Execute on a SET portal answers NoData then CommandComplete 'SET'")
  void setPortalDescribeAndExecute() throws Exception {
    assertReplySequenceForIgnoredExecutionStatement("SET datestyle = 'ISO'", "SET");
  }

  @Test
  @DisplayName("[#6930] Describe(P)+Execute on a SAVEPOINT portal answers NoData then CommandComplete 'SAVEPOINT'")
  void savepointPortalDescribeAndExecute() throws Exception {
    assertReplySequenceForIgnoredExecutionStatement("SAVEPOINT test_savepoint", "SAVEPOINT");
  }

  @Test
  @DisplayName("[#6930] Describe(P)+Execute on a RELEASE portal answers NoData then CommandComplete 'RELEASE'")
  void releasePortalDescribeAndExecute() throws Exception {
    assertReplySequenceForIgnoredExecutionStatement("SAVEPOINT test_savepoint2", "SAVEPOINT");
    assertReplySequenceForIgnoredExecutionStatement("RELEASE test_savepoint2", "RELEASE");
  }

  @Test
  @DisplayName("[#6930] Describe(P)+Execute on a ROLLBACK TO portal answers NoData then CommandComplete 'ROLLBACK'")
  void rollbackToPortalDescribeAndExecute() throws Exception {
    assertReplySequenceForIgnoredExecutionStatement("SAVEPOINT test_savepoint3", "SAVEPOINT");
    assertReplySequenceForIgnoredExecutionStatement("ROLLBACK TO test_savepoint3", "ROLLBACK");
  }

  private void assertReplySequenceForIgnoredExecutionStatement(final String query, final String expectedTag) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendParse(out, query);
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);

        final WireMessage parseComplete = readWireMessage(in);
        assertThat(parseComplete.type()).isEqualTo('1');
        final WireMessage bindComplete = readWireMessage(in);
        assertThat(bindComplete.type()).isEqualTo('2');

        // Exactly one Describe reply, and no rows since these statements never produce any.
        final WireMessage describeReply = readWireMessage(in);
        assertThat(describeReply.type()).as("Describe('P') on a %s portal must answer NoData, not silence", query)
            .isEqualTo('n');

        // Execute must answer CommandComplete with the real command tag, never NoData.
        final WireMessage executeReply = readWireMessage(in);
        assertThat(executeReply.type()).as("Execute on a %s portal must answer CommandComplete, not NoData", query)
            .isEqualTo('C');
        assertThat(tagOf(executeReply)).isEqualTo(expectedTag);

        final WireMessage readyForQuery = readWireMessage(in);
        assertThat(readyForQuery.type()).isEqualTo('Z');
      });
    }
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
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

  private static void sendBind(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    writeCString(body, ""); // statement name
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0
    body.write(0);
    body.write(0); // int16 numParamValues = 0
    body.write(0);
    body.write(0); // int16 numResultFormatCodes = 0

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('B');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendDescribePortal(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('P'); // describe a portal, not a prepared statement
    writeCString(body, ""); // unnamed portal

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('D');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendExecute(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    body.write(0);
    body.write(0);
    body.write(0);
    body.write(0); // int32 limit = 0 (no limit)

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('E');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  /**
   * A {@code CommandComplete} body is a single null-terminated tag string with no other fields.
   */
  private static String tagOf(final WireMessage message) {
    final byte[] body = message.body();
    return new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
  }

  private record WireMessage(char type, byte[] body) {
  }

  private static WireMessage readWireMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    final byte[] body = new byte[length - 4];
    in.readFully(body);
    return new WireMessage((char) type, body);
  }
}
