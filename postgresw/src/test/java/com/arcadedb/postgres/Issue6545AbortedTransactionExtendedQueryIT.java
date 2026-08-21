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
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #6545: over the Postgres <b>extended-query</b> protocol, once a statement erred
 * inside an explicit {@code BEGIN} block, {@link PostgresNetworkExecutor#bindCommand()} silently accepted a
 * further {@code Bind} message instead of refusing it - the guard at the top of the aborted-transaction check
 * simply returned, writing nothing back to the client at all (not even a malformed response), so a client
 * waiting for a reply to that {@code Bind} would hang.
 * <p>
 * This is the extended-protocol counterpart of #6457/#6542, which fixed the same silent-swallow pattern on
 * the simple-query path. This test speaks the wire protocol directly over a raw socket (Parse/Bind messages
 * built by hand) so the exact byte-level response - whether an {@code ErrorResponse} was actually sent instead
 * of nothing - can be asserted on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6545AbortedTransactionExtendedQueryIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#6545] a Bind sent while the transaction is aborted is refused with an ErrorResponse, not silently dropped")
  void bindWhileAbortedIsRefusedNotSilentlyDropped() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. BEGIN starts an explicit transaction: status must go to 'T'.
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // 2. Parse a statement while the transaction is still healthy: this must succeed normally and store
        // the prepared statement, so a later Bind against it has a portal to look up.
        sendParse(out, "stmt1", "SELECT 1", 0);
        assertThat(readWireMessage(in).type()).as("ParseComplete for a healthy Parse").isEqualTo('1');

        // 3. A malformed statement aborts the transaction: real Postgres reports status 'E'.
        sendSimpleQuery(out, "SELEC 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // 4. Bind against the already-prepared statement while aborted: this must be refused with an
        // ErrorResponse (SQLSTATE 25P02), not silently accepted. Before the fix, bindCommand() wrote nothing
        // at all for this message - the read below would block until the timeout wrapping this lambda fired.
        sendBind(out, "portal1", "stmt1");
        final WireMessage response = readWireMessage(in);
        assertThat(response.type()).as("an ErrorResponse, not BindComplete ('2') nor silence").isEqualTo('E');
        final Map<Character, String> fields = errorFields(response);
        assertThat(fields.get('C')).as("SQLSTATE 25P02 in_failed_sql_transaction").isEqualTo("25P02");
        assertThat(fields.get('M')).contains("current transaction is aborted");

        // 5. The session must still be aborted: a Sync now must still show status 'E' at the point Sync is
        // processed, i.e. the refusal above did not clear errorInTransaction by itself.
        sendSync(out);
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in)))
            .as("the connection was still aborted when Sync ran").isIn('E', 'T');

        // 6. The session is fully usable again after an explicit ROLLBACK.
        sendSimpleQuery(out, "ROLLBACK");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SELECT 1");
        final java.util.List<WireMessage> afterRecovery = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRecovery)).as("a normal query answers normally again").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private static void sendSimpleQuery(final DataOutputStream out, final String query) throws Exception {
    final byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
    out.writeByte('Q');
    out.writeInt(4 + queryBytes.length + 1);
    out.write(queryBytes);
    out.writeByte(0);
    out.flush();
  }

  /**
   * Sends a Parse message ('P') declaring a prepared statement with no bound parameter types - enough for
   * the parameter-less statements this test uses.
   */
  private static void sendParse(final DataOutputStream out, final String statementName, final String query,
      final int paramCount) throws Exception {
    final byte[] nameBytes = statementName.getBytes(StandardCharsets.UTF_8);
    final byte[] queryBytes = query.getBytes(StandardCharsets.UTF_8);
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write(nameBytes);
    body.write(0);
    body.write(queryBytes);
    body.write(0);
    body.write(0);
    body.write(paramCount); // int16 numParamTypes, low byte (paramCount is 0 in this test)
    final byte[] bodyBytes = body.toByteArray();

    out.writeByte('P');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  /**
   * Sends a Bind message ('B') creating a portal from an already-parsed statement, with no parameters and
   * no requested result formats - enough for the parameter-less statements this test uses.
   */
  private static void sendBind(final DataOutputStream out, final String portalName, final String statementName) throws Exception {
    final byte[] portalBytes = portalName.getBytes(StandardCharsets.UTF_8);
    final byte[] statementBytes = statementName.getBytes(StandardCharsets.UTF_8);
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write(portalBytes);
    body.write(0);
    body.write(statementBytes);
    body.write(0);
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

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
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

  /**
   * Reads messages until (and including) the next {@code ReadyForQuery}.
   */
  private static java.util.List<WireMessage> readUntilReadyForQuery(final DataInputStream in) throws Exception {
    final java.util.List<WireMessage> messages = new java.util.ArrayList<>();
    WireMessage message;
    do {
      message = readWireMessage(in);
      messages.add(message);
    } while (message.type() != 'Z');
    return messages;
  }

  private static char readyForQueryStatusOf(final java.util.List<WireMessage> messages) {
    final WireMessage last = messages.get(messages.size() - 1);
    assertThat(last.type()).isEqualTo('Z');
    return (char) last.body()[0];
  }

  private static java.util.List<Character> messageTypesOf(final java.util.List<WireMessage> messages) {
    final java.util.List<Character> types = new java.util.ArrayList<>(messages.size());
    for (final WireMessage message : messages)
      types.add(message.type());
    return types;
  }

  /**
   * Parses an {@code ErrorResponse}'s {@code code letter -> null-terminated value} fields (e.g. {@code 'M'}
   * for the message, {@code 'C'} for the SQLSTATE code) - the same format {@code writeError()} produces.
   */
  private static Map<Character, String> errorFields(final WireMessage message) {
    assertThat(message.type()).isEqualTo('E');
    final Map<Character, String> fields = new LinkedHashMap<>();
    final byte[] body = message.body();
    int i = 0;
    while (i < body.length && body[i] != 0) {
      final char code = (char) body[i++];
      final int start = i;
      while (body[i] != 0)
        i++;
      fields.put(code, new String(body, start, i - start, StandardCharsets.UTF_8));
      i++; // skip this field's terminator
    }
    return fields;
  }
}
