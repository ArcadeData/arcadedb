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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #6543: over the Postgres <b>extended query</b> protocol (Parse/Bind/Execute), the
 * Parse-time dispatch in {@link PostgresNetworkExecutor#parseCommand()} that tracks explicit-transaction state
 * recognized only {@code BEGIN}/{@code BEGIN TRANSACTION} and {@code COMMIT}. There was no {@code ROLLBACK}
 * branch, and worse, {@code sqlEngine.parse()} was called <i>unconditionally</i> before that check ever ran: a
 * bare {@code ROLLBACK} sent as its own Parse/Bind/Execute (rather than folded into a simple-query {@code 'Q'}
 * message, the wire path already covered by issue #6457/#6542) parsed successfully as ArcadeDB SQL's own
 * {@code RollbackStatement} - which does roll the underlying transaction back - but left
 * {@code explicitTransactionStarted} set. Since {@code writeReadyForQueryMessage()} reports status {@code 'T'}
 * whenever that flag is set, and {@code syncCommand()}'s implicit-commit branch only runs when the flag is
 * clear, the wire-level session stayed reported as "inside an explicit transaction" forever after the
 * ROLLBACK, even though the underlying transaction had already been discarded.
 * <p>
 * The same unconditional parse also meant a {@code ROLLBACK WORK}/{@code ROLLBACK TRANSACTION}/
 * {@code COMMIT WORK} could never be recognized by merely widening the {@code isCommitStatement}/
 * {@code isRollbackStatement} helpers (the follow-up issue #6543 explicitly asked to fold in): ArcadeDB SQL's
 * {@code rollbackStatement}/{@code commitStatement} grammar productions accept only the bare keyword (plus
 * COMMIT's own {@code RETRY} clause), so parsing any of those forms as SQL throws a syntax error before the
 * widened check is ever reached. The fix checks BEGIN/COMMIT/ROLLBACK - in every recognized form - before
 * calling {@code sqlEngine.parse()} at all, the same ordering {@code queryCommand()}'s simple-query dispatch
 * already used for exactly this reason.
 * <p>
 * These tests speak the wire protocol directly over a raw socket, framing each statement as its own
 * Parse+Bind+Execute+Sync round trip, so the extended-query dispatch path is actually exercised (a JDBC/
 * psycopg2 client normally sends transaction-control statements over the simple-query protocol, which would
 * not reach this bug at all).
 * <p>
 * <b>Not covered here:</b> whether ROLLBACK actually discards a write made after an extended-query BEGIN.
 * Unlike {@code queryCommand()}'s BEGIN handling (which calls {@code database.begin()} directly),
 * {@code parseCommand()}'s BEGIN branch only sets {@code explicitTransactionStarted} and never starts a real
 * underlying transaction - a pre-existing gap, independent of this issue, that means a write made after an
 * extended-query BEGIN auto-commits on its own before any following ROLLBACK/COMMIT is reached. Asserting
 * data-discarding here would therefore pass or fail on that unrelated gap rather than on the fix this class
 * targets.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6543RollbackExtendedProtocolIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#6543] a bare ROLLBACK over the extended query protocol ends the explicit transaction cleanly")
  void rollbackOverExtendedProtocolEndsTransaction() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // BEGIN over the extended query protocol: status must go to 'T'.
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');

        // ROLLBACK, unrecognized in the Parse dispatch before the fix, must not error and must end the
        // transaction, reporting idle status - the core of issue #6543.
        final List<WireMessage> afterRollback = runExtendedQuery(out, in, "ROLLBACK");
        assertThat(messageTypesOf(afterRollback)).as("no ErrorResponse for ROLLBACK itself").doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRollback))
            .as("ROLLBACK must clear explicitTransactionStarted so status goes back to idle").isEqualTo('I');

        // The session is fully usable again, not wedged in a permanent 'T' state.
        final List<WireMessage> afterRecovery = runExtendedQuery(out, in, "SELECT 1");
        assertThat(messageTypesOf(afterRecovery)).doesNotContain('E');
        assertThat(readyForQueryStatusOf(afterRecovery)).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#6543] ROLLBACK WORK / ROLLBACK TRANSACTION are recognized as aliases for ROLLBACK over the extended query protocol")
  void rollbackAliasesAreRecognizedOverExtendedProtocol() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "ROLLBACK WORK")))
            .as("ROLLBACK WORK must be recognized just like a bare ROLLBACK").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN TRANSACTION"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "ROLLBACK TRANSACTION")))
            .as("ROLLBACK TRANSACTION must be recognized just like a bare ROLLBACK").isEqualTo('I');

        // BEGIN WORK: PostgreSQL's own grammar is BEGIN [ WORK | TRANSACTION ], added per review feedback on
        // this PR for symmetry with the COMMIT/ROLLBACK WORK forms above.
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN WORK")))
            .as("BEGIN WORK must be recognized just like a bare BEGIN").isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "ROLLBACK"))).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#6543] COMMIT WORK / END are recognized as aliases for COMMIT over the extended query protocol")
  void commitAliasesAreRecognizedOverExtendedProtocol() throws Exception {
    // Companion to the ROLLBACK-alias test above: isCommitStatement() was widened to COMMIT WORK/END/END
    // TRANSACTION/END WORK alongside the ROLLBACK fix (the extended-protocol COMMIT branch previously matched
    // only the exact literal "COMMIT"), and that widening only works because BEGIN/COMMIT/ROLLBACK are now
    // checked before sqlEngine.parse() is ever called - so this exercises both halves of the fix together.
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "COMMIT WORK")))
            .as("COMMIT WORK must be recognized just like a bare COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "COMMIT TRANSACTION")))
            .as("COMMIT TRANSACTION must be recognized just like a bare COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "END")))
            .as("END must be recognized as an alias for COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "END TRANSACTION")))
            .as("END TRANSACTION must be recognized as an alias for COMMIT").isEqualTo('I');

        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "BEGIN"))).isEqualTo('T');
        assertThat(readyForQueryStatusOf(runExtendedQuery(out, in, "END WORK")))
            .as("END WORK must be recognized as an alias for COMMIT").isEqualTo('I');
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
}
