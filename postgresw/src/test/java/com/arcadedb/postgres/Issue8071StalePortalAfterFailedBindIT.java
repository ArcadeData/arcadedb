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
 * Regression test for issue #8071, the Bind-side twin of #7906: {@code bindCommand()} registered its portal only
 * on the success path, and neither its catch arm (a parameter {@code PostgresType.deserialize()} refuses) nor its
 * aborted-block refusal touched {@code portals}. A Bind that failed therefore left the PREVIOUS portal bound under
 * the same name, and an Execute of that name in a later round trip found a portal already executed, with a
 * materialised result set, and answered the OLD statement's rows with a success tag.
 * <p>
 * PostgreSQL never leaves such a portal usable: the unnamed one is replaced by {@code CreatePortal} before any
 * parameter is converted, and any error aborts the transaction the old portal lived in. Each test runs the failed
 * Bind in its own round trip, so the skip-until-Sync discard of #7775 cannot explain the outcome.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8071StalePortalAfterFailedBindIT extends PostgresWireProtocolTestBase {

  private static final String TYPE = "Stale8071";

  // Deliberately not a real PostgreSQL OID: PostgresType.deserialize() throws for it (see #5923's test).
  private static final int UNSUPPORTED_TYPE_OID = 999999;

  @Test
  @DisplayName("[#8071] a Bind that fails deserializing a parameter does not leave the previous unnamed portal executable")
  void aFailedBindDestroysThePreviousUnnamedPortal() throws Exception {
    assertFailedBindDestroysPreviousPortal("");
  }

  @Test
  @DisplayName("[#8071] a Bind that fails deserializing a parameter does not leave the previous named portal executable")
  void aFailedBindDestroysThePreviousNamedPortal() throws Exception {
    assertFailedBindDestroysPreviousPortal("p8071");
  }

  @Test
  @DisplayName("[#8071] a Bind refused in an aborted block does not leave the previous portal executable after ROLLBACK")
  void aBindRefusedInAnAbortedBlockDestroysThePreviousPortal() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulate(out, in);

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // Round 1: statement A bound under the unnamed portal and executed to completion inside the block.
        sendParse(out, "sA", "SELECT id FROM " + TYPE, -1);
        sendBind(out, "", "sA", null);
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("statement A answers its rows").contains('1', '2', 'D', 'C');

        // A failing statement aborts the block.
        sendSimpleQuery(out, "SELEC 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // Round 2: a Bind under the same portal name, refused with 25P02 because the block is aborted.
        sendBind(out, "", "sA", null);
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("the Bind is refused").contains('E').doesNotContain('2');

        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        // Round 3: Execute the portal name the refused Bind targeted. It must not answer A's rows again.
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> stale = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(stale)).as("the portal the refused Bind replaced must not answer A's rows")
            .doesNotContain('D');
        assertThat(messageTypesOf(stale)).as("nor a success tag for it").doesNotContain('C');
      });
    }
  }

  private void assertFailedBindDestroysPreviousPortal(final String portalName) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulate(out, in);

        // Round 1: statement A, bound under the portal name and executed to completion.
        sendParse(out, "sA", "SELECT id FROM " + TYPE, -1);
        sendBind(out, portalName, "sA", null);
        sendExecute(out, portalName);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("statement A answers its rows").contains('1', '2', 'D', 'C');

        // Round 2: statement B declares a parameter the server cannot deserialize, and its Bind under the SAME
        // portal name fails mid-message. Only the error comes back.
        sendParse(out, "sB", "SELECT id FROM " + TYPE + " WHERE id = $1", UNSUPPORTED_TYPE_OID);
        sendBind(out, portalName, "sB", "1");
        sendSync(out);
        final List<WireMessage> failed = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(failed)).as("B parses").contains('1');
        assertThat(messageTypesOf(failed)).as("and its Bind fails").contains('E').doesNotContain('2');

        // Round 3: Execute the portal name. A's portal must be gone, so neither A's rows nor a success tag.
        sendExecute(out, portalName);
        sendSync(out);
        final List<WireMessage> stale = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(stale)).as("the failed Bind must not leave A's portal to answer its rows")
            .doesNotContain('D');
        assertThat(messageTypesOf(stale)).as("nor a success tag for a statement the client never bound").doesNotContain('C');

        // The connection stays usable: a fresh Bind of A under the same name answers normally.
        sendBind(out, portalName, "sA", null);
        sendExecute(out, portalName);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('2', 'D', 'C');
      });
    }
  }

  private void createAndPopulate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    readUntilReadyForQuery(in);
    sendSimpleQuery(out, "CREATE PROPERTY " + TYPE + ".id IF NOT EXISTS INTEGER");
    readUntilReadyForQuery(in);
    sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET id = 1");
    readUntilReadyForQuery(in);
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z');
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
   * @param paramTypeOid the OID of the single declared parameter, or a negative value to declare none
   */
  private static void sendParse(final DataOutputStream out, final String statementName, final String query, final int paramTypeOid)
      throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, statementName);
    writeCString(body, query);
    if (paramTypeOid < 0) {
      writeShort(body, 0);
    } else {
      writeShort(body, 1);
      writeInt(body, paramTypeOid);
    }
    writeMessage(out, 'P', body);
  }

  /**
   * @param textParam the single text-format parameter value, or null to send no parameter
   */
  private static void sendBind(final DataOutputStream out, final String portalName, final String statementName, final String textParam)
      throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    writeCString(body, statementName);
    writeShort(body, 0); // no parameter format codes: all text
    if (textParam == null) {
      writeShort(body, 0);
    } else {
      final byte[] value = textParam.getBytes(StandardCharsets.UTF_8);
      writeShort(body, 1);
      writeInt(body, value.length);
      body.writeBytes(value);
    }
    writeShort(body, 0); // no result format codes
    writeMessage(out, 'B', body);
  }

  private static void sendExecute(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    writeInt(body, 0); // no row limit
    writeMessage(out, 'E', body);
  }

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  private static void writeMessage(final DataOutputStream out, final char type, final ByteArrayOutputStream body) throws Exception {
    final byte[] bodyBytes = body.toByteArray();
    out.writeByte(type);
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void writeShort(final ByteArrayOutputStream out, final int value) {
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }

  private static void writeInt(final ByteArrayOutputStream out, final int value) {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
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

  private static List<WireMessage> readUntilReadyForQuery(final DataInputStream in) throws Exception {
    final List<WireMessage> messages = new ArrayList<>();
    WireMessage message;
    do {
      message = readWireMessage(in);
      messages.add(message);
    } while (message.type() != 'Z');
    return messages;
  }

  private static char readyForQueryStatusOf(final List<WireMessage> messages) {
    final WireMessage last = messages.getLast();
    return (char) last.body()[0];
  }

  private static List<Character> messageTypesOf(final List<WireMessage> messages) {
    final List<Character> types = new ArrayList<>(messages.size());
    for (final WireMessage message : messages)
      types.add(message.type());
    return types;
  }
}
