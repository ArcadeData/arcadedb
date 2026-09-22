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
import java.util.List;
import java.util.Map;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for two extended-protocol defects around a name that holds nothing.
 * <p>
 * Issue #8211: a Bind from a prepared statement that does not exist answered {@code BindComplete}, and an Execute
 * (or a Describe) of a portal that does not exist answered {@code NoData}, where PostgreSQL answers an
 * ErrorResponse with SQLSTATE {@code 26000}/{@code 34000}.
 * <p>
 * Issue #8212: portals outlived the transaction that created them. PostgreSQL drops every non-holdable portal when
 * its transaction ends - at Sync for the implicit block of an autocommit pipeline, at COMMIT/ROLLBACK/END for an
 * explicit one - so an Execute in a later round trip of a portal bound before that point must be refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8211Issue8212PortalLifecycleIT extends PostgresWireProtocolTestBase {

  private static final String TYPE = "Portal8212";

  @Test
  @DisplayName("[#8211] Bind from a missing prepared statement is refused with 26000, not answered BindComplete")
  void bindFromMissingStatementIsRefused() throws Exception {
    runOnConnection((out, in) -> {
      sendBind(out, "", "missing8211");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> named = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(named)).as("ErrorResponse, then the Execute discarded").containsExactly('E', 'Z');
      assertError(named.getFirst(), "26000", "prepared statement \"missing8211\" does not exist");

      sendBind(out, "", "");
      sendSync(out);
      final List<WireMessage> unnamed = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(unnamed)).containsExactly('E', 'Z');
      assertError(unnamed.getFirst(), "26000", "unnamed prepared statement does not exist");
    });
  }

  @Test
  @DisplayName("[#8211] Execute and Describe of a missing portal or statement are refused, not answered NoData")
  void executeAndDescribeOfMissingNameAreRefused() throws Exception {
    runOnConnection((out, in) -> {
      sendExecute(out, "missing8211");
      sendSync(out);
      final List<WireMessage> execute = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(execute)).as("never NoData: that is a Describe-only reply").containsExactly('E', 'Z');
      assertError(execute.getFirst(), "34000", "portal \"missing8211\" does not exist");

      sendDescribe(out, 'P', "missing8211");
      sendSync(out);
      final List<WireMessage> describePortal = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(describePortal)).containsExactly('E', 'Z');
      assertError(describePortal.getFirst(), "34000", "portal \"missing8211\" does not exist");

      sendDescribe(out, 'S', "missing8211");
      sendSync(out);
      final List<WireMessage> describeStatement = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(describeStatement)).containsExactly('E', 'Z');
      assertError(describeStatement.getFirst(), "26000", "prepared statement \"missing8211\" does not exist");

      // The connection is healthy afterwards
      sendParse(out, "", "SELECT id FROM " + TYPE);
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'T', 'D', 'D', 'C', 'Z');
    });
  }

  @Test
  @DisplayName("[#8212] a portal is dropped with the aborted implicit block, so a Bind discarded by skip-until-Sync leaves nothing executable")
  void portalDoesNotOutliveTheAbortedImplicitBlock() throws Exception {
    runOnConnection((out, in) -> {
      // Round 1: statement A bound under the unnamed portal and executed.
      sendParse(out, "", "SELECT id FROM " + TYPE);
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('D', 'C');

      // Round 2: a failing Parse; the Bind and Execute behind it are discarded.
      sendParse(out, "", "SELEKT bogus");
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('E', 'Z');

      // Round 3: Execute with no new Bind. A's portal went with its transaction.
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> stale = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(stale)).as("A's portal must not answer again").containsExactly('E', 'Z');
      assertError(stale.getFirst(), "34000", "portal \"\" does not exist");
    });
  }

  @Test
  @DisplayName("[#8212] in autocommit a portal ends at the Sync that terminates its implicit block")
  void portalEndsAtSyncInAutocommit() throws Exception {
    runOnConnection((out, in) -> {
      sendParse(out, "s1", "SELECT id FROM " + TYPE + " ORDER BY id");
      sendBind(out, "p1", "s1");
      sendExecuteWithLimit(out, "p1", 1);
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("suspended after one row").containsExactly('1', '2', 'T', 'D', 's', 'Z');

      sendExecuteWithLimit(out, "p1", 1);
      sendSync(out);
      final List<WireMessage> resumed = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(resumed)).containsExactly('E', 'Z');
      assertError(resumed.getFirst(), "34000", "portal \"p1\" does not exist");

      // The prepared statement is session-scoped and survives: a new Bind from it works.
      sendBind(out, "p1", "s1");
      sendExecute(out, "p1");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'T', 'D', 'D', 'C', 'Z');
    });
  }

  @Test
  @DisplayName("[#8212] inside an explicit block a portal survives Sync and is dropped at COMMIT")
  void portalSurvivesSyncInsideExplicitBlockAndEndsAtCommit() throws Exception {
    runOnConnection((out, in) -> {
      sendSimpleQuery(out, "BEGIN");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

      sendParse(out, "s1", "SELECT id FROM " + TYPE + " ORDER BY id");
      sendBind(out, "p1", "s1");
      sendExecuteWithLimit(out, "p1", 1);
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'T', 'D', 's', 'Z');

      // Resumed across the Sync: the explicit block is still open (the pgjdbc fetch-size cursor shape).
      sendExecuteWithLimit(out, "p1", 1);
      sendSync(out);
      final List<WireMessage> resumed = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(resumed)).containsExactly('D', 'C', 'Z');
      assertThat(readyForQueryStatusOf(resumed)).isEqualTo('T');

      sendBind(out, "p2", "s1");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'Z');

      sendSimpleQuery(out, "COMMIT");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

      sendExecute(out, "p2");
      sendSync(out);
      final List<WireMessage> afterCommit = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(afterCommit)).as("the portal ended with the block it was bound in").containsExactly('E', 'Z');
      assertError(afterCommit.getFirst(), "34000", "portal \"p2\" does not exist");
    });
  }

  private interface ConnectionTest {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void runOnConnection(final ConnectionTest test) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String command : new String[] { "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS",
            "CREATE PROPERTY " + TYPE + ".id IF NOT EXISTS INTEGER", "DELETE FROM " + TYPE, "INSERT INTO " + TYPE + " SET id = 1",
            "INSERT INTO " + TYPE + " SET id = 2" }) {
          sendSimpleQuery(out, command);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('E');
        }
        test.run(out, in);
      });
    }
  }

  private static void assertError(final WireMessage message, final String sqlState, final String text) {
    final Map<Character, String> fields = errorFields(message);
    assertThat(fields.get('C')).isEqualTo(sqlState);
    assertThat(fields.get('M')).isEqualTo(text);
  }

  private static void sendExecuteWithLimit(final DataOutputStream out, final String portalName, final int limit) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('E');
    out.writeInt(4 + bodyBytes.length + 4);
    out.write(bodyBytes);
    out.writeInt(limit);
    out.flush();
  }
}
