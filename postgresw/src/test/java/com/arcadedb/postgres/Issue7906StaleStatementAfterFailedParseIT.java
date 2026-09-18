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
 * Regression test for issue #7906: {@code parseCommand()} registered its portal with
 * {@code preparedStatements.put(portalName, portal)} as the LAST thing it did, after every branch that can fail,
 * so a Parse that was REFUSED left whatever had been parsed under that name BEFORE it still registered. A Bind
 * afterwards resurrected it, and the client received - for the statement the server had just refused - a complete
 * successful answer built from a DIFFERENT statement: BindComplete, RowDescription, the old statement's rows, and
 * a CommandComplete with a success tag.
 * <p>
 * PostgreSQL destroys the unnamed prepared statement the moment a Parse names it as destination, successful or
 * not ({@code drop_unnamed_stmt()} at the top of {@code exec_parse_message}), so a Bind afterwards gets
 * {@code unnamed prepared statement does not exist}.
 * <p>
 * The interesting shape is the second one below - two separate round trips, each ended by its own Sync. The
 * skip-until-Sync discard of issue #7775 cannot explain it: by the time the third round trip arrives the failing
 * message block is two Syncs in the past. Any Parse failure reaches it, not only the ROLLBACK TO refusal of
 * issue #7846 - a plain syntax error does it too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7906StaleStatementAfterFailedParseIT extends PostgresWireProtocolTestBase {

  private static final String TYPE = "Stale7906";

  @Test
  @DisplayName("[#7906] a refused Parse in its own round trip does not leave the previous statement bindable")
  void aRefusedParseDestroysTheStatementRegisteredUnderTheSameName() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulate(out, in);

        // Round 1: a good unnamed statement, bound and executed to completion.
        sendParse(out, "SELECT id FROM " + TYPE);
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);
        final List<WireMessage> first = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(first)).as("the good statement answers rows").contains('1', '2', 'T', 'D', 'C');

        // Round 2: a Parse that fails, alone, ended by its own Sync. Only the error comes back.
        sendParse(out, "SELEKT bogus syntax");
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("the refused Parse answers an ErrorResponse").contains('E');
        assertThat(messageTypesOf(refused)).as("and no ParseComplete").doesNotContain('1');

        // Round 3: Bind/Describe/Execute the unnamed statement. It no longer exists, so the server must not
        // answer with round 1's rows under a SELECT tag.
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);
        final List<WireMessage> stale = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(stale))
            .as("the destroyed statement must not be resurrected: no RowDescription and no DataRow for it")
            .doesNotContain('T', 'D');
        assertThat(messageTypesOf(stale)).as("and no success tag for a statement that was never parsed")
            .doesNotContain('C');
      });
    }
  }

  /**
   * The same hole reached through the refusal that widened it (issue #7846), and in one pipelined request - the
   * shape {@code PQsendQueryParams} produces. Here the skip-until-Sync of #7775 also applies, so the whole
   * pipeline behind the error is discarded; what this pins is that the stale statement is not answered even
   * when it is.
   */
  @Test
  @DisplayName("[#7906] a refused ROLLBACK TO in a pipeline does not run the previously parsed statement")
  void aRefusedRollbackToInOnePipelineDoesNotRunThePreviousStatement() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulate(out, in);

        sendParse(out, "SELECT id FROM " + TYPE);
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('T', 'D', 'C');

        sendParse(out, "ROLLBACK TO sp1");
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("the ROLLBACK TO is refused").contains('E');
        assertThat(messageTypesOf(response))
            .as("and the previously parsed SELECT is not run in its place")
            .doesNotContain('T', 'D', 'C');

        // The connection is still usable afterwards: a fresh Parse installs a statement of its own.
        sendParse(out, "SELECT id FROM " + TYPE);
        sendBind(out);
        sendDescribePortal(out);
        sendExecute(out);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('1', '2', 'T', 'D', 'C');
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
    body.write('P');
    writeCString(body, "");

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('D');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendExecute(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, "");
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
