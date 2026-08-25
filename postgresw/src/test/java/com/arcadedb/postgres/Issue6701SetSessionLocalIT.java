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
import com.arcadedb.utility.DateUtils;

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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #6701: two pre-existing gaps in
 * {@link PostgresNetworkExecutor#setConfiguration(String)} / {@code parseSetCommand()}, found by CodeRabbit
 * while reviewing PR #6697 (unrelated to that PR's own fix).
 * <ul>
 *   <li>{@code SET SESSION <name> = <value>} / {@code SET LOCAL <name> = <value>} were not recognized: the
 *   {@code SESSION }/{@code LOCAL } modifier became part of the parameter name (e.g.
 *   {@code "session datestyle"}), so the {@code datestyle} special case never fired.</li>
 *   <li>The extended query protocol's {@code Parse} dispatch passed {@code portal.query} to
 *   {@code setConfiguration()} without stripping a trailing {@code ;} first, unlike the simple-query
 *   protocol's {@code queryCommand()}, which already strips one from {@code queryText}.</li>
 * </ul>
 * These tests speak the wire protocol directly over a raw socket so the extended-query {@code Parse} path
 * is actually exercised (a JDBC client normally sends {@code SET} over the simple-query protocol, which
 * would not reach the second bug at all), and observe the {@code datestyle} special case's side effect
 * ({@link com.arcadedb.schema.Schema#setDateTimeFormat}) directly on the server-side database rather than
 * relying on {@code connectionProperties}, which this class never exposes back to the client.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6701SetSessionLocalIT extends PostgresWireProtocolTestBase {

  @AfterEach
  @Override
  public void endTest() {
    // Restore the schema-wide format so this test can't leak state into any other test class that shares
    // the same database.
    getServerDatabase(0, getDatabaseName()).getSchema().setDateTimeFormat(GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString());
    super.endTest();
  }

  @Test
  @DisplayName("[#6701] SET SESSION <name> = <value> over the simple-query protocol resolves to the plain parameter name")
  void setSessionModifierIsHonoredOverSimpleProtocol() throws Exception {
    resetDateTimeFormat();

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SET SESSION datestyle = 'ISO'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in)))
            .as("no ErrorResponse for a SET SESSION command").doesNotContain('E');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getDateTimeFormat())
        .as("SET SESSION datestyle = 'ISO' must resolve the parameter name to plain 'datestyle', "
            + "not 'session datestyle', so the datestyle special case actually fires")
        .isEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);
  }

  @Test
  @DisplayName("[#6701] SET LOCAL <name> = <value> over the simple-query protocol resolves to the plain parameter name")
  void setLocalModifierIsHonoredOverSimpleProtocol() throws Exception {
    resetDateTimeFormat();

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SET LOCAL datestyle = 'ISO'");
        assertThat(messageTypesOf(readUntilReadyForQuery(in)))
            .as("no ErrorResponse for a SET LOCAL command").doesNotContain('E');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getDateTimeFormat())
        .as("SET LOCAL datestyle = 'ISO' must resolve the parameter name to plain 'datestyle', "
            + "not 'local datestyle', so the datestyle special case actually fires")
        .isEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);
  }

  @Test
  @DisplayName("[#6701] a semicolon-terminated SET over the extended query protocol is honored, not rejected")
  void trailingSemicolonIsStrippedOverExtendedProtocol() throws Exception {
    resetDateTimeFormat();

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // Sent as its own Parse+Bind+Execute+Sync round trip, the extended-query dispatch in
        // parseCommand() (not queryCommand()'s simple-query dispatch, which already strips a trailing ';').
        final List<WireMessage> messages = runExtendedQuery(out, in, "SET datestyle = 'ISO';");
        assertThat(messageTypesOf(messages))
            .as("a trailing ';' must not make setConfiguration() reject the value").doesNotContain('E');
      });
    }

    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getDateTimeFormat())
        .as("SET datestyle = 'ISO'; over the extended protocol must still flip the format to ISO 8601, "
            + "not fail to parse because of the glued-on trailing ';'")
        .isEqualTo(DateUtils.DATE_TIME_ISO_8601_FORMAT);
  }

  private void resetDateTimeFormat() {
    getServerDatabase(0, getDatabaseName()).getSchema().setDateTimeFormat(GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString());
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
   * Runs one statement as its own Parse+Bind+Execute+Sync round trip over the extended query protocol (the
   * unnamed statement/portal, no parameters), then reads every message the server sends back up to and
   * including the {@code ReadyForQuery} the Sync produces.
   */
  private static List<WireMessage> runExtendedQuery(final DataOutputStream out, final DataInputStream in, final String query)
      throws Exception {
    sendParse(out, query);
    sendBind(out);
    sendExecute(out);
    sendSync(out);
    return readUntilReadyForQuery(in);
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
   * Bind for the unnamed portal/statement, no parameters, no declared result formats.
   */
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
   * Reads messages until (and including) the next {@code ReadyForQuery}, so a single request/response round
   * trip can be inspected in full.
   */
  private static List<WireMessage> readUntilReadyForQuery(final DataInputStream in) throws Exception {
    final List<WireMessage> messages = new ArrayList<>();
    WireMessage message;
    do {
      message = readWireMessage(in);
      messages.add(message);
    } while (message.type() != 'Z');
    return messages;
  }

  private static List<Character> messageTypesOf(final List<WireMessage> messages) {
    final List<Character> types = new ArrayList<>(messages.size());
    for (final WireMessage message : messages)
      types.add(message.type());
    return types;
  }
}
