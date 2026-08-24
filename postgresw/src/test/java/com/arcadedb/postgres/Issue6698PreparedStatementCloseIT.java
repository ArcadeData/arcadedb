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
 * Regression tests for issue #6698: {@link PostgresNetworkExecutor#preparedStatements} map is never pruned
 * on {@code Close('S')} messages naming a prepared statement or unnamed statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6698PreparedStatementCloseIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#6698] Close('S') removes named prepared statement from preparedStatements map")
  void closeNamedPreparedStatementRemovesFromMap() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. Parse named prepared statement "S1"
        sendParse(out, "S1", "SELECT 1");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('1', 'Z'); // ParseComplete, ReadyForQuery

        // 2. Describe('S', "S1") before close: returns ParameterDescription ('t') + RowDescription ('T')
        sendDescribe(out, 'S', "S1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('t', 'T', 'Z');

        // 3. Close('S', "S1")
        sendClose(out, 'S', "S1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z'); // CloseComplete, ReadyForQuery

        // 4. Describe('S', "S1") after close: statement was pruned, so returns NoData ('n')
        sendDescribe(out, 'S', "S1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages))
            .as("Describe on closed statement must return NoData because preparedStatements was pruned")
            .containsExactly('n', 'Z');

        // 5. Bind from closed statement "S1": fails to find statement in preparedStatements
        sendBind(out, "P1", "S1");
        sendExecute(out, "P1", 0);
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        // BindComplete ('2'), then Execute on unbound portal returns NoData ('n'), then ReadyForQuery ('Z')
        assertThat(messageTypesOf(messages))
            .as("Execute on portal bound from closed statement must return NoData")
            .containsExactly('2', 'n', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#6698] Close('S') removes unnamed prepared statement from preparedStatements map")
  void closeUnnamedPreparedStatementRemovesFromMap() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. Parse unnamed prepared statement ""
        sendParse(out, "", "SELECT 1");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('1', 'Z');

        // 2. Describe('S', "") before close: returns ParameterDescription ('t') + RowDescription ('T')
        sendDescribe(out, 'S', "");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('t', 'T', 'Z');

        // 3. Close('S', "")
        sendClose(out, 'S', "");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');

        // 4. Describe('S', "") after close: returns NoData ('n')
        sendDescribe(out, 'S', "");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages))
            .as("Describe on closed unnamed statement must return NoData")
            .containsExactly('n', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#6698] Closing a prepared statement does not discard previously bound active portals")
  void closePreparedStatementPreservesExistingBoundPortals() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. Parse statement "S1"
        sendParse(out, "S1", "SELECT 42");
        // 2. Bind portal "P1" from "S1"
        sendBind(out, "P1", "S1");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('1', '2', 'Z'); // ParseComplete, BindComplete, ReadyForQuery

        // 3. Close statement "S1"
        sendClose(out, 'S', "S1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');

        // 4. Existing bound portal "P1" still executes successfully
        sendExecute(out, "P1", 0);
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        // RowDescription ('T'), DataRow ('D'), CommandComplete ('C'), ReadyForQuery ('Z')
        assertThat(messageTypesOf(messages)).containsExactly('T', 'D', 'C', 'Z');

        // 5. New portal "P2" bound from closed statement "S1" fails to execute
        sendBind(out, "P2", "S1");
        sendExecute(out, "P2", 0);
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('2', 'n', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#6698] Close('P') and Close('S') on non-existent targets return CloseComplete without error")
  void closeNonExistentTargetReturnsCloseComplete() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // Close non-existent statement
        sendClose(out, 'S', "nonexistent_stmt");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');

        // Close non-existent portal
        sendClose(out, 'P', "nonexistent_portal");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');
      });
    }
  }

  @Test
  @DisplayName("[#6698] Close('P') removes portal from portals map")
  void closePortalRemovesFromMap() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendParse(out, "S1", "SELECT 1");
        sendBind(out, "P1", "S1");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('1', '2', 'Z');

        // Close portal "P1"
        sendClose(out, 'P', "P1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');

        // Execute on closed portal returns NoData
        sendExecute(out, "P1", 0);
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('n', 'Z');
      });
    }
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  private static void sendParse(final DataOutputStream out, final String statementName, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, statementName);
    writeCString(body, query);
    body.write(0);
    body.write(0); // int16 numParamDataTypes = 0

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('P');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendDescribe(final DataOutputStream out, final char describeType, final String name) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write((byte) describeType);
    writeCString(body, name);

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('D');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendBind(final DataOutputStream out, final String portalName, final String statementName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    writeCString(body, statementName);
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

  private static void sendExecute(final DataOutputStream out, final String portalName, final int limit) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    body.write((limit >>> 24) & 0xFF);
    body.write((limit >>> 16) & 0xFF);
    body.write((limit >>> 8) & 0xFF);
    body.write(limit & 0xFF);

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('E');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendClose(final DataOutputStream out, final char closeType, final String name) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write((byte) closeType);
    writeCString(body, name);

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('C');
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
    final byte[] body = in.readNBytes(length - 4);
    return new WireMessage((char) type, body);
  }

  private static List<WireMessage> readUntilReadyForQuery(final DataInputStream in) throws Exception {
    final List<WireMessage> messages = new ArrayList<>();
    while (true) {
      final WireMessage msg = readWireMessage(in);
      messages.add(msg);
      if (msg.type() == 'Z')
        return messages;
    }
  }

  private static List<Character> messageTypesOf(final List<WireMessage> messages) {
    return messages.stream().map(WireMessage::type).toList();
  }
}
