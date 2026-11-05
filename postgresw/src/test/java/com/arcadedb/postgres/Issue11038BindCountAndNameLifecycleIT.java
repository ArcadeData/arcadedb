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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.errorFields;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendBindWithFormats;
import static com.arcadedb.postgres.PostgresWireMessages.sendClose;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendParseWithTypes;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #11038 (#11018, #11034): the extended protocol's Bind must supply exactly the parameters its
 * prepared statement requires, and a named statement or portal cannot be redefined while it exists.
 * <p>
 * #11018: a Bind with fewer values than the statement's {@code $N} left the unbound placeholder to read the property of that
 * name, so an UPDATE wrote NULL and answered success. PostgreSQL's {@code exec_bind_message()} refuses the mismatch with
 * {@code 08P01} before anything runs.
 * <p>
 * #11034: a Parse naming a live statement and a Bind naming a live portal replaced it silently, where PostgreSQL answers
 * {@code 42P05} / {@code 42P03} (only the unnamed statement and portal are replaced). The report's third case - Close('S')
 * and DEALLOCATE closing the portals bound from the statement - follows the protocol documentation's wording but not
 * PostgreSQL itself: {@code DropPreparedStatement()} only releases the plan source, every portal holds its own reference to
 * the plan, and its Execute keeps answering. That behaviour is pinned here so a client working against PostgreSQL keeps
 * working here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11038BindCountAndNameLifecycleIT extends PostgresWireProtocolTestBase {

  private static final String TYPE     = "Bind11038";
  private static final int    INT4_OID = 23;

  @Test
  @DisplayName("[#11018] a Bind supplying fewer or more values than the statement requires is refused with 08P01")
  void bindWithTheWrongParameterCountIsRefused() throws Exception {
    runOnConnection((out, in) -> {
      sendParseWithTypes(out, "s1", "SELECT id FROM " + TYPE + " WHERE id = $1", INT4_OID);
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 'Z');

      sendBind(out, "", "s1", new String[0]);
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> tooFew = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(tooFew)).as("refused, and the Execute behind it discarded").containsExactly('E', 'Z');
      assertError(tooFew.get(0), "08P01", "bind message supplies 0 parameters, but prepared statement \"s1\" requires 1");

      sendBind(out, "", "s1", "1", "2", "3");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> tooMany = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(tooMany)).containsExactly('E', 'Z');
      assertError(tooMany.get(0), "08P01", "bind message supplies 3 parameters, but prepared statement \"s1\" requires 1");

      // The exact count still answers
      sendBind(out, "", "s1", "1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> exact = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(exact)).containsExactly('2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(exact)).isEqualTo("1");
    });
  }

  @Test
  @DisplayName("[#11018] an UPDATE bound with no value for its $1 never runs, so it cannot write NULL")
  void shortBindNeverWritesNull() throws Exception {
    runOnConnection((out, in) -> {
      // No declared types: the count comes from the $N the text references
      sendParse(out, "up", "UPDATE " + TYPE + " SET id = $1 WHERE id = 1");
      sendBind(out, "", "up", new String[0]);
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(refused)).containsExactly('1', 'E', 'Z');
      assertError(refused.get(1), "08P01", "bind message supplies 0 parameters, but prepared statement \"up\" requires 1");

      sendSimpleQuery(out, "SELECT count(*) AS c FROM " + TYPE + " WHERE id IS NULL");
      assertThat(firstDataRowValue(readUntilReadyForQuery(in))).as("no row was written with a NULL id").isEqualTo("0");
      sendSimpleQuery(out, "SELECT count(*) AS c FROM " + TYPE + " WHERE id = 1");
      assertThat(firstDataRowValue(readUntilReadyForQuery(in))).as("the row the UPDATE targeted is untouched").isEqualTo("1");
    });
  }

  @Test
  @DisplayName("[#11018] the required count is the highest $N referenced, even past the declared types, and Describe reports it")
  void requiredCountIsTheHighestPlaceholderBeyondTheDeclaredTypes() throws Exception {
    runOnConnection((out, in) -> {
      // One type declared, two placeholders referenced: PostgreSQL infers the second and requires both
      sendParseWithTypes(out, "s2", "SELECT id FROM " + TYPE + " WHERE id = $1 OR id = $2 ORDER BY id", INT4_OID);
      sendDescribe(out, 'S', "s2");
      sendSync(out);
      final List<WireMessage> described = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(described)).startsWith('1', 't');
      assertThat(parameterCountOf(described.get(1))).isEqualTo(2);

      sendBind(out, "", "s2", "1");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertError(refused.get(0), "08P01", "bind message supplies 1 parameters, but prepared statement \"s2\" requires 2");

      sendBind(out, "", "s2", "1", "2");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'D', 'D', 'C', 'Z');
    });
  }

  @Test
  @DisplayName("[#11018] a $N inside a string literal, a quoted identifier or a comment is not a placeholder")
  void placeholdersInsideLiteralsAndCommentsAreNotCounted() throws Exception {
    runOnConnection((out, in) -> {
      sendParse(out, "s3", "SELECT id FROM " + TYPE + " WHERE id = $1 AND '$5' = '$5' /* $7 */");
      sendDescribe(out, 'S', "s3");
      sendSync(out);
      final List<WireMessage> described = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(described)).as(described.toString()).startsWith('1', 't');
      assertThat(parameterCountOf(described.get(1))).isEqualTo(1);

      sendBind(out, "", "s3", "2");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> answered = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answered)).containsExactly('2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(answered)).isEqualTo("2");

      // With standard_conforming_strings on (the default) 'x\' is a whole literal, its backslash an ordinary character: read
      // with backslash escapes, the literal would run on and swallow the $1 that follows it
      sendParse(out, "s3b", "SELECT id FROM " + TYPE + " WHERE 'x\\' <> 'y' AND id = $1");
      sendDescribe(out, 'S', "s3b");
      sendSync(out);
      final List<WireMessage> describedBackslash = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(describedBackslash)).as(describedBackslash.toString()).startsWith('1', 't');
      assertThat(parameterCountOf(describedBackslash.get(1))).isEqualTo(1);

      sendBind(out, "", "s3b", "1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> answeredBackslash = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(answeredBackslash)).containsExactly('2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(answeredBackslash)).isEqualTo("1");
    });
  }

  @Test
  @DisplayName("[#11029] a statement whose only $N is inside a literal declares no parameter when Parse declares no types")
  void literalDollarDigitsDeclareNoParameter() throws Exception {
    runOnConnection((out, in) -> {
      // The shape asyncpg and node-postgres send: zero declared types, so the count comes from the text alone. Each of
      // these holds a $N only inside a quoted construct, so PostgreSQL describes no parameter and binds none. A dollar-quoted
      // body ($$ $3 $$) is skipped by the same scan (Issue11018ParameterPlaceholderCountTest), but the SQL engine does not
      // parse that literal form, so it cannot be executed here
      final String[] statements = { "SELECT 'price is $1 today' AS v", "SELECT \"$2\" FROM " + TYPE + " LIMIT 1",
          "SELECT 'a' AS v /* $5 */", "SELECT 'a' AS v -- $6\n" };
      for (int i = 0; i < statements.length; i++) {
        final String name = "lit" + i;
        sendParse(out, name, statements[i]);
        sendDescribe(out, 'S', name);
        sendSync(out);
        final List<WireMessage> described = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(described)).as(statements[i] + " " + described).startsWith('1', 't');
        assertThat(parameterCountOf(described.get(1))).as(statements[i]).isEqualTo(0);

        sendBind(out, "", name, new String[0]);
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as(statements[i]).doesNotContain('E');
      }

      sendSimpleQuery(out, "SELECT 'price is $1 today' AS v");
      assertThat(firstDataRowValue(readUntilReadyForQuery(in))).isEqualTo("price is $1 today");
    });
  }

  @Test
  @DisplayName("[#11018] a Bind with several format codes that do not match its value count is refused with 08P01")
  void bindWithMismatchedFormatCodesIsRefused() throws Exception {
    runOnConnection((out, in) -> {
      sendParseWithTypes(out, "s4", "SELECT id FROM " + TYPE + " WHERE id = $1", INT4_OID);
      sendBindWithFormats(out, "", "s4", new int[] { 0, 0 }, "1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(refused)).containsExactly('1', 'E', 'Z');
      assertError(refused.get(1), "08P01", "bind message has 2 parameter formats but 1 parameters");

      // One format code applies to every value
      sendBindWithFormats(out, "", "s4", new int[] { 0 }, "2");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'D', 'C', 'Z');
    });
  }

  @Test
  @DisplayName("[#11034] a Parse naming a live statement is refused with 42P05 and the statement keeps its text; the unnamed one is replaced")
  void reParseOfALiveNamedStatementIsRefused() throws Exception {
    runOnConnection((out, in) -> {
      sendParse(out, "rp1", "SELECT id FROM " + TYPE + " WHERE id = 1");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 'Z');

      sendParse(out, "rp1", "SELECT id FROM " + TYPE + " WHERE id = 2");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(refused)).containsExactly('E', 'Z');
      assertError(refused.get(0), "42P05", "prepared statement \"rp1\" already exists");

      sendBind(out, "", "rp1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> original = readUntilReadyForQuery(in);
      assertThat(firstDataRowValue(original)).as("the statement still holds the first text").isEqualTo("1");

      // Once closed, the name can be defined again
      sendClose(out, 'S', "rp1");
      sendParse(out, "rp1", "SELECT id FROM " + TYPE + " WHERE id = 2");
      sendBind(out, "", "rp1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> redefined = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(redefined)).containsExactly('3', '1', '2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(redefined)).isEqualTo("2");

      // The unnamed statement is silently replaced, as in PostgreSQL
      sendParse(out, "", "SELECT id FROM " + TYPE + " WHERE id = 1");
      sendParse(out, "", "SELECT id FROM " + TYPE + " WHERE id = 2");
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> unnamed = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(unnamed)).containsExactly('1', '1', '2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(unnamed)).isEqualTo("2");
    });
  }

  @Test
  @DisplayName("[#11034] a Bind naming a live portal is refused with 42P03; the unnamed portal is replaced")
  void reBindOfALiveNamedPortalIsRefused() throws Exception {
    runOnConnection((out, in) -> {
      sendSimpleQuery(out, "BEGIN");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

      sendParse(out, "rb1", "SELECT id FROM " + TYPE + " ORDER BY id");
      sendBind(out, "rb1p", "rb1");
      sendBind(out, "rb1p", "rb1");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(refused)).containsExactly('1', '2', 'E', 'Z');
      assertError(refused.get(2), "42P03", "cursor \"rb1p\" already exists");

      sendSimpleQuery(out, "ROLLBACK");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

      sendSimpleQuery(out, "BEGIN");
      readUntilReadyForQuery(in);
      sendBind(out, "", "rb1");
      sendBind(out, "", "rb1");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the unnamed portal is replaced silently")
          .containsExactly('2', '2', 'D', 'D', 'C', 'Z');
      sendSimpleQuery(out, "COMMIT");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
    });
  }

  @Test
  @DisplayName("[#11034] Close('S') and DEALLOCATE leave the portals bound from the statement executable, as PostgreSQL does")
  void closingAStatementLeavesItsPortalsExecutable() throws Exception {
    runOnConnection((out, in) -> {
      sendSimpleQuery(out, "BEGIN");
      readUntilReadyForQuery(in);

      sendParse(out, "cs2", "SELECT id FROM " + TYPE + " ORDER BY id");
      sendBind(out, "cs2p", "cs2");
      sendClose(out, 'S', "cs2");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', '3', 'Z');

      sendExecute(out, "cs2p");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the portal holds its own plan").containsExactly('D', 'D', 'C', 'Z');

      sendParse(out, "cs3", "SELECT id FROM " + TYPE + " ORDER BY id");
      sendBind(out, "cs3p", "cs3");
      sendSync(out);
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "DEALLOCATE cs3");
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('C', 'Z');

      sendExecute(out, "cs3p");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('D', 'D', 'C', 'Z');

      sendSimpleQuery(out, "COMMIT");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
    });
  }

  private interface ConnectionTest {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void runOnConnection(final ConnectionTest test) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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

  private static int parameterCountOf(final WireMessage parameterDescription) {
    assertThat(parameterDescription.type()).isEqualTo('t');
    return ByteBuffer.wrap(parameterDescription.body()).getShort();
  }

  private static void assertError(final WireMessage message, final String sqlState, final String text) {
    final Map<Character, String> fields = errorFields(message);
    assertThat(fields.get('C')).isEqualTo(sqlState);
    assertThat(fields.get('M')).isEqualTo(text);
  }
}
