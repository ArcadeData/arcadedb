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
 * Regression tests for issue #6548, a follow-up from the review on PR #6546 (fix for #6543):
 * {@link PostgresNetworkExecutor#parseCommand()}, {@code bindCommand()}, and {@code executeCommand()} (the
 * extended query protocol - Parse/Bind/Execute) all started with an unconditional
 * <pre>{@code
 * if (errorInTransaction)
 *   return;
 * }</pre>
 * before the query text was ever inspected. Once {@code errorInTransaction} was set - a statement erred
 * inside an explicit {@code BEGIN} block - a client sending an explicit {@code ROLLBACK} (or {@code COMMIT}/
 * {@code END}, which real Postgres treats identically while aborted) over the extended protocol to recover
 * never reached the BEGIN/COMMIT/ROLLBACK dispatch #6546 added to the simple-query path: {@code Parse} just
 * silently dropped the message.
 * <p>
 * When {@code Sync} eventually arrived, its {@code errorInTransaction} branch called {@code
 * database.rollback()} and cleared {@code errorInTransaction}, but never touched {@code
 * explicitTransactionStarted}. Since {@code writeReadyForQueryMessage()} checks {@code errorInTransaction}
 * first and {@code explicitTransactionStarted} second, the client ended up back at {@code ReadyForQuery}
 * status {@code 'T'} instead of {@code 'I'} - the same "wedged forever" symptom #6543 fixed, just reached via
 * the aborted-transaction path instead of a plain {@code ROLLBACK} with no prior error.
 * <p>
 * These tests speak the wire protocol directly over a raw socket so the exact sequence - a malformed
 * statement that aborts an explicit transaction, followed by a recovery statement sent as its own
 * Parse/Bind/Execute/Sync round trip - can be driven and the resulting {@code ReadyForQuery} status byte
 * asserted on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6548AbortedTransactionRollbackExtendedProtocolIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#6548] ROLLBACK sent as its own Parse/Bind/Execute while aborted clears both errorInTransaction and explicitTransactionStarted")
  void rollbackRecoversFromAbortedTransactionOverExtendedProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. BEGIN over the extended query protocol: status must go to 'T'.
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

        // 2. A malformed statement, sent as its own Parse, aborts the transaction without ever reaching
        // Sync - errorInTransaction is set synchronously inside parseCommand()'s own catch block.
        sendParse(out, "SELEC 1");
        final WireMessage parseError = readWireMessage(in);
        assertThat(parseError.type()).as("ErrorResponse for the malformed statement").isEqualTo('E');

        // 3. ROLLBACK sent as its own Parse/Bind/Execute/Sync must recover the session: no ErrorResponse
        // anywhere in the round trip, and status must be 'I' - not 'T' (explicitTransactionStarted must be
        // cleared, not just errorInTransaction) and not 'E' (the transaction must actually be resolved).
        final List<WireMessage> afterRollback = runExtendedQuery(out, in, "ROLLBACK");
        assertThat(messageTypesOf(afterRollback)).as("no ErrorResponse for the recovering ROLLBACK").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRollback))
            .as("ROLLBACK must clear both errorInTransaction and explicitTransactionStarted").isEqualTo('I');

        // 4. The session is fully usable again, not wedged in a permanent aborted/explicit-transaction state.
        final List<WireMessage> afterRecovery = runExtendedQuery(out, in, "SELECT 1");
        assertThat(messageTypesOf(afterRecovery)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#6548] a trailing ';' or surrounding whitespace on ROLLBACK does not defeat the aborted-transaction recovery match")
  void rollbackWithTrailingSemicolonOrWhitespaceRecoversFromAbortedTransaction() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // isCommitStatement()/isRollbackStatement() match by exact string equality, so the aborted-branch
        // dispatch must trim and strip a trailing ';' before comparing - the same normalization
        // queryCommand() applies to its own queryText - or a driver that appends the statement terminator
        // (many do) would fall through to the silent return and reproduce this issue's "wedged forever"
        // symptom via a trailing semicolon instead of via the missing dispatch.
        for (final String rollbackVariant : new String[] {"ROLLBACK;", " ROLLBACK ", " ROLLBACK; "}) {
          assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

          sendParse(out, "SELEC 1");
          assertThat(readWireMessage(in).type()).isEqualTo('E');

          final List<WireMessage> afterRollback = runExtendedQuery(out, in, rollbackVariant);
          assertThat(messageTypesOf(afterRollback))
              .as("'" + rollbackVariant + "' must not itself error").doesNotContain('E');
          assertThat(readyForQueryStatusOf(afterRollback))
              .as("'" + rollbackVariant + "' must clear both errorInTransaction and explicitTransactionStarted")
              .isEqualTo('I');
        }
      });
    }
  }

  @Test
  @DisplayName("[#6548] COMMIT/END sent while aborted over the extended query protocol recover the session and report the ROLLBACK tag")
  void commitAndEndRecoverFromAbortedTransactionOverExtendedProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        for (final String recoveryStatement : new String[] {"COMMIT", "END"}) {
          assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

          sendParse(out, "SELEC 1");
          assertThat(readWireMessage(in).type()).isEqualTo('E');

          // Real Postgres treats a COMMIT/END of an aborted transaction the same as ROLLBACK - there is
          // nothing left to commit - so the command tag must read ROLLBACK regardless of which keyword the
          // client sent, exactly like queryCommand()'s simple-query dispatch already does.
          final List<WireMessage> afterRecoveryAttempt = runExtendedQuery(out, in, recoveryStatement);
          assertThat(messageTypesOf(afterRecoveryAttempt))
              .as(recoveryStatement + " must not itself error").doesNotContain('E');
          assertThat(readyForQueryStatusOf(afterRecoveryAttempt))
              .as(recoveryStatement + " must clear both errorInTransaction and explicitTransactionStarted")
              .isEqualTo('I');
          assertThat(commandCompleteTagOf(afterRecoveryAttempt))
              .as(recoveryStatement + " while aborted must report the ROLLBACK tag, not " + recoveryStatement)
              .isEqualTo("ROLLBACK");
        }
      });
    }
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
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
   * Reads messages until (and including) the next {@code ReadyForQuery}, so a single Parse+Bind+Execute+Sync
   * round trip can be inspected in full.
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
   * Extracts the tag from the round trip's {@code CommandComplete} ('C') message, e.g. {@code "ROLLBACK"}.
   */
  private static String commandCompleteTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages) {
      if (message.type() == 'C') {
        final byte[] body = message.body();
        final int end = body.length > 0 && body[body.length - 1] == 0 ? body.length - 1 : body.length;
        return new String(body, 0, end, StandardCharsets.UTF_8);
      }
    }
    throw new AssertionError("No CommandComplete message found in " + messageTypesOf(messages));
  }
}
