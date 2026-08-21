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
import com.arcadedb.database.Database;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #6457: over the Postgres <b>simple-query</b> protocol, once a statement erred
 * inside an explicit {@code BEGIN} block the session became permanently wedged.
 * <p>
 * Three defects, all in {@link PostgresNetworkExecutor}:
 * <ul>
 *   <li>{@code writeReadyForQueryMessage()} never reported the aborted status {@code 'E'} - a client had no
 *   wire-level signal the transaction had failed.</li>
 *   <li>{@code queryCommand()} recognised only {@code BEGIN}, not {@code COMMIT}/{@code ROLLBACK}/{@code END}
 *   - over the simple protocol there was no statement that could ever end an aborted block, and even without
 *   a prior error a plain {@code COMMIT}/{@code ROLLBACK} left {@code explicitTransactionStarted} stuck.</li>
 *   <li>while aborted, {@code queryCommand()} silently returned for every statement - no {@code ErrorResponse},
 *   no {@code CommandComplete} - so a client had no way to tell its statement never ran.</li>
 * </ul>
 * These tests speak the wire protocol directly over a raw socket (rather than through the JDBC driver) so the
 * exact byte-level behaviour - the {@code ReadyForQuery} status byte, whether an {@code ErrorResponse} was
 * actually sent - can be asserted on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6457AbortedTransactionSimpleQueryIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#6457] an aborted explicit transaction reports status 'E', refuses further statements, and ROLLBACK ends it")
  void abortedTransactionRejectsFurtherStatementsUntilRollback() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. BEGIN starts an explicit transaction: status must go to 'T'.
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // 2. A malformed statement errors: real Postgres reports the aborted status 'E', not 'T'.
        sendSimpleQuery(out, "SELEC 1");
        final List<WireMessage> afterBadStatement = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterBadStatement)).as("an ErrorResponse for the malformed statement").contains('E');
        assertThat(readyForQueryStatusOf(afterBadStatement))
            .as("ReadyForQuery must report the aborted transaction status").isEqualTo('E');

        // 3. Any further statement - even a harmless one - must be refused with an ErrorResponse, not
        // silently swallowed (the core of issue #6457): before the fix this response held only the
        // ReadyForQuery message and nothing else.
        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> whileAborted = readUntilReadyForQuery(in);
        assertThat(whileAborted).as("the aborted statement must not be silently swallowed").hasSizeGreaterThan(1);
        final WireMessage error = whileAborted.stream().filter(m -> m.type() == 'E').findFirst()
            .orElseThrow(() -> new AssertionError("expected an ErrorResponse while the transaction is aborted"));
        final Map<Character, String> fields = errorFields(error);
        assertThat(fields.get('C')).as("SQLSTATE 25P02 in_failed_sql_transaction").isEqualTo("25P02");
        assertThat(fields.get('M')).contains("current transaction is aborted");
        assertThat(readyForQueryStatusOf(whileAborted)).as("still aborted").isEqualTo('E');

        // 4. ROLLBACK, unrecognized before the fix, must now end the aborted block cleanly.
        sendSimpleQuery(out, "ROLLBACK");
        final List<WireMessage> afterRollback = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRollback)).as("no ErrorResponse for ROLLBACK itself").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRollback)).as("back to idle, outside any transaction").isEqualTo('I');

        // 5. The session is fully usable again.
        sendSimpleQuery(out, "SELECT 1");
        final List<WireMessage> afterRecovery = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterRecovery)).as("a normal query answers normally again").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#6457] COMMIT of an aborted transaction acts as ROLLBACK and discards the writes made before the error")
  void abortedTransactionCommitActsAsRollbackAndDiscardsPriorWrites() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE Issue6457Commit IF NOT EXISTS");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "INSERT INTO Issue6457Commit SET id = 1");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        sendSimpleQuery(out, "SELEC 1"); // aborts the transaction
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // COMMIT while aborted: Postgres treats it exactly like ROLLBACK (with a warning) rather than
        // trying to persist writes that already failed.
        sendSimpleQuery(out, "COMMIT");
        final List<WireMessage> afterCommit = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterCommit)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterCommit)).isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType("Issue6457Commit", true))
        .as("a COMMIT of an aborted transaction must not persist the writes made before the error")
        .isZero();
  }

  @Test
  @DisplayName("[#6457] a plain COMMIT with no prior error is recognized over the simple-query protocol and persists the writes")
  void plainCommitOverSimpleQueryProtocolEndsTheTransactionAndPersists() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "CREATE DOCUMENT TYPE Issue6457PlainCommit IF NOT EXISTS");
        readUntilReadyForQuery(in);

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendSimpleQuery(out, "INSERT INTO Issue6457PlainCommit SET id = 1");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');

        // Before the fix, COMMIT was not recognized at all here: it fell through to the SQL engine as an
        // ordinary (failing) statement and explicitTransactionStarted was never cleared, so status stayed 'T'.
        sendSimpleQuery(out, "COMMIT");
        final List<WireMessage> afterCommit = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(afterCommit)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterCommit)).as("COMMIT must end the explicit transaction").isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    assertThat(database.countType("Issue6457PlainCommit", true))
        .as("a plain COMMIT must persist the writes made in the transaction")
        .isEqualTo(1);
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
   * Reads messages until (and including) the next {@code ReadyForQuery}, so a single simple-query request/
   * response round trip can be inspected in full - every {@code ErrorResponse}, {@code CommandComplete}, row,
   * etc. the server sent back for it, not just the final status byte.
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

  private static char readyForQueryStatusOf(final List<WireMessage> messages) {
    final WireMessage last = messages.get(messages.size() - 1);
    assertThat(last.type()).isEqualTo('Z');
    return (char) last.body()[0];
  }

  private static List<Character> messageTypesOf(final List<WireMessage> messages) {
    final List<Character> types = new ArrayList<>(messages.size());
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
