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
 * A transaction-control statement on the simple query protocol returns no rows, so PostgreSQL answers it with the
 * bare {@code CommandComplete} tag. A {@code RowDescription} in front of the tag, even with zero fields, makes libpq
 * classify the result as {@code PGRES_TUPLES_OK} instead of {@code PGRES_COMMAND_OK}: {@code psql} prints an empty
 * table after {@code BEGIN}, and a client that checks the status when it opens a transaction (the Arrow PostgreSQL
 * ADBC driver, whose DB-API connections start with autocommit off) fails with
 * {@code [libpq] Failed to begin transaction}.
 */
class TransactionControlNoRowDescriptionIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("BEGIN, COMMIT and ROLLBACK on the simple query protocol are answered with CommandComplete only")
  void transactionControlIsAnsweredWithTheBareCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        for (final String[] step : new String[][] { { "BEGIN", "BEGIN", "T" }, { "COMMIT", "COMMIT", "I" },
            { "BEGIN", "BEGIN", "T" }, { "ROLLBACK", "ROLLBACK", "I" } }) {
          sendSimpleQuery(out, step[0]);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no RowDescription").doesNotContain('T');
          assertThat(messageTypesOf(response)).as(step[0] + " returns no rows, so no DataRow").doesNotContain('D');
          assertThat(messageTypesOf(response)).as(step[0] + " is acknowledged by its command tag").contains('C');
          assertThat(commandTagOf(response)).isEqualTo(step[1]);
          assertThat(readyForQueryStatusOf(response)).isEqualTo(step[2].charAt(0));
        }

        // A statement that does return rows keeps its RowDescription, inside and outside a transaction.
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendSimpleQuery(out, "SELECT 1 AS one");
        final List<WireMessage> select = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(select)).contains('T', 'D', 'C');
        sendSimpleQuery(out, "COMMIT");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).doesNotContain('T');
      });
    }
  }

  @Test
  @DisplayName("SAVEPOINT, RELEASE and ROLLBACK TO are answered with CommandComplete only")
  void savepointStatementsAreAnsweredWithTheBareCommandTag() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        for (final String statement : new String[] { "SAVEPOINT sp1", "ROLLBACK TO sp1", "RELEASE sp1" }) {
          sendSimpleQuery(out, statement);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(statement + " returns no rows, so no RowDescription").doesNotContain('T');
          assertThat(messageTypesOf(response)).as(statement + " is acknowledged by its command tag").contains('C');
        }
        sendSimpleQuery(out, "COMMIT");
        readUntilReadyForQuery(in);
      });
    }
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    final WireMessage complete = messages.stream().filter(m -> m.type() == 'C').findFirst()
        .orElseThrow(() -> new AssertionError("expected a CommandComplete"));
    final byte[] body = complete.body();
    return new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
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
