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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
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
 * Regression test for issue #6679: the Postgres simple-query ('Q' message) protocol path materialized the
 * entire result set into an unbounded {@code List<Result>} before sending the first row, so a large {@code
 * SELECT} risked an {@code OutOfMemoryError}. The simple-query protocol has no client-driven cursor/max-rows
 * mechanism (unlike the extended protocol's portal {@code Execute}), so buffering could not simply be turned
 * into a silent {@code PortalSuspended} truncation without misreporting a partial result as the whole answer.
 * The fix instead bounds the buffer with {@link GlobalConfiguration#POSTGRES_SIMPLE_QUERY_MAX_ROWS} and refuses
 * a SELECT whose result exceeds it with a clear {@code ErrorResponse}, protecting server memory without
 * silently returning wrong (truncated) data.
 * <p>
 * This test speaks the wire protocol directly over a raw socket (rather than through the JDBC driver) so the
 * exact byte-level response - an {@code ErrorResponse} versus a normal {@code RowDescription}/{@code DataRow}
 * stream - can be asserted on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6679SimpleQueryMaxRowsIT extends PostgresWireProtocolTestBase {
  private static final int    ROW_CAP  = 5;
  private static final String TYPE     = "Issue6679Row";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.POSTGRES_SIMPLE_QUERY_MAX_ROWS.setValue(ROW_CAP);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.POSTGRES_SIMPLE_QUERY_MAX_ROWS.reset();
    super.endTest();
  }

  private void createRows(final int count) {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.transaction(() -> {
      if (!database.getSchema().existsType(TYPE))
        database.getSchema().createDocumentType(TYPE);
      for (int i = 0; i < count; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
  }

  @Test
  @DisplayName("[#6679] a simple-query SELECT whose result exceeds the configured cap is refused, not silently truncated")
  void resultOverTheCapIsRefusedWithAnErrorResponse() throws Exception {
    createRows(ROW_CAP + 10);

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SELECT FROM " + TYPE);
        final List<WireMessage> response = readUntilReadyForQuery(in);

        assertThat(messageTypesOf(response)).as("an ErrorResponse instead of a truncated result").contains('E');
        assertThat(messageTypesOf(response)).as("no CommandComplete for a refused query").doesNotContain('C');
        assertThat(messageTypesOf(response)).as("no DataRow for a refused query").doesNotContain('D');
        final WireMessage error = response.stream().filter(m -> m.type() == 'E').findFirst()
            .orElseThrow(() -> new AssertionError("expected an ErrorResponse"));
        assertThat(errorFields(error).get('M')).contains("exceeds the configured limit").contains(String.valueOf(ROW_CAP));

        // The session must stay usable afterwards: not left aborted/wedged by the refusal.
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');

        // Prove it, rather than just trusting the status byte: the same socket must still accept and answer a
        // normal query, not merely report idle while actually closed/wedged.
        sendSimpleQuery(out, "SELECT 1 AS one");
        final List<WireMessage> followUp = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(followUp)).as("a normal query after the refusal must succeed").doesNotContain('E');
        assertThat(messageTypesOf(followUp)).as("a normal query after the refusal must return its row").contains('D');
        assertThat(readyForQueryStatusOf(followUp)).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#6679] a simple-query SELECT within the configured cap still returns every row normally")
  void resultWithinTheCapIsReturnedInFull() throws Exception {
    createRows(ROW_CAP - 2);

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SELECT FROM " + TYPE);
        final List<WireMessage> response = readUntilReadyForQuery(in);

        assertThat(messageTypesOf(response)).as("no ErrorResponse for a result within the cap").doesNotContain('E');
        final long dataRows = response.stream().filter(m -> m.type() == 'D').count();
        assertThat(dataRows).isEqualTo(ROW_CAP - 2);
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
      });
    }
  }

  @Test
  @DisplayName("[#6679] a simple-query SELECT of exactly the configured cap is returned in full")
  void resultAtTheCapIsReturnedInFull() throws Exception {
    createRows(ROW_CAP);

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SELECT FROM " + TYPE);
        final List<WireMessage> response = readUntilReadyForQuery(in);

        assertThat(messageTypesOf(response)).as("a result of exactly the cap must not be refused").doesNotContain('E');
        final long dataRows = response.stream().filter(m -> m.type() == 'D').count();
        assertThat(dataRows).isEqualTo(ROW_CAP);
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
      });
    }
  }

  /**
   * The row cap must never leave a write statement partially executed: {@code UPDATE ... RETURN} still updates
   * every matching row even when its RETURN result exceeds the cap, and only the (now fully-applied) result is
   * refused.
   */
  @Test
  @DisplayName("[#6679] an UPDATE ... RETURN over the cap still updates every row; only the RETURN result is refused")
  void updateReturnOverTheCapStillAppliesEveryWrite() throws Exception {
    createRows(ROW_CAP + 10);

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "UPDATE " + TYPE + " SET touched = true RETURN AFTER");
        final List<WireMessage> response = readUntilReadyForQuery(in);

        assertThat(messageTypesOf(response)).as("the oversized RETURN result is refused").contains('E');
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
      });
    }

    final Database database = getServerDatabase(0, getDatabaseName());
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + TYPE + " WHERE touched = true")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).longValue())
          .as("every matching row must be updated even though the RETURN result was refused").isEqualTo(ROW_CAP + 10);
    }
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
