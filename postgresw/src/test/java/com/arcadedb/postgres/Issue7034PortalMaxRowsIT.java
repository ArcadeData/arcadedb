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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #7034 (follow-up to #6679): the extended-protocol portal materialized the entire
 * result set with no cap at all, at both sites that run a portal - {@code Describe('P')} and the first
 * {@code Execute} of a portal that was never Described - so the "whole result in memory before the first byte"
 * defect #6679 fixed on the simple-query path was still reachable, and the {@code Execute} row limit the client
 * chose bounded only what was sent, not what the server held. The same knob,
 * {@link GlobalConfiguration#POSTGRES_QUERY_MAX_ROWS}, now caps every wire path.
 * <p>
 * A refusal at {@code Describe('P')} also has to reach the client as an {@code ErrorResponse}: a failure of the
 * query at Describe time used to go to the server log only, leaving the client waiting for a RowDescription that
 * never came.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7034PortalMaxRowsIT extends PostgresWireProtocolTestBase {
  private static final int    ROW_CAP = 5;
  private static final String TYPE    = "Issue7034Row";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.POSTGRES_QUERY_MAX_ROWS.setValue(ROW_CAP);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.POSTGRES_QUERY_MAX_ROWS.reset();
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
  @DisplayName("[#7034] Describe('P') on a portal whose result exceeds the cap is refused with an ErrorResponse, even with a small Execute fetch size")
  void describedPortalOverTheCapIsRefused() throws Exception {
    createRows(ROW_CAP + 10);

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // The pgjdbc/psycopg shape: Parse/Bind/Describe('P')/Execute(max_rows=2)/Sync. The client asked for 2 rows;
        // the server would have held all 15.
        sendParse(out, "SELECT FROM " + TYPE);
        sendBind(out, "P1");
        sendDescribe(out, "P1");
        sendExecute(out, "P1", 2);
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);

        assertThat(messageTypesOf(response)).as("ParseComplete, BindComplete, then the refusal").startsWith('1', '2', 'E');
        assertThat(messageTypesOf(response)).as("no RowDescription for a refused portal").doesNotContain('T');
        assertThat(messageTypesOf(response)).as("no DataRow for a refused portal").doesNotContain('D');
        assertThat(messageTypesOf(response)).as("the pipelined Execute is discarded up to Sync, not answered")
            .doesNotContain('C', 's');
        final WireMessage error = response.stream().filter(m -> m.type() == 'E').findFirst()
            .orElseThrow(() -> new AssertionError("expected an ErrorResponse"));
        assertThat(errorFields(error).get('M')).contains("exceeds the configured limit").contains(String.valueOf(ROW_CAP))
            .contains(GlobalConfiguration.POSTGRES_QUERY_MAX_ROWS.getKey());
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');

        // The session must stay usable afterwards
        sendSimpleQuery(out, "SELECT 1 AS one");
        final List<WireMessage> followUp = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(followUp)).doesNotContain('E').contains('D');
      });
    }
  }

  @Test
  @DisplayName("[#7034] the first Execute of a never-Described portal whose result exceeds the cap is refused")
  void unDescribedPortalOverTheCapIsRefused() throws Exception {
    createRows(ROW_CAP + 10);

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendParse(out, "SELECT FROM " + TYPE);
        sendBind(out, "P1");
        sendExecute(out, "P1", 2);
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);

        assertThat(messageTypesOf(response)).startsWith('1', '2', 'E');
        assertThat(messageTypesOf(response)).doesNotContain('T', 'D', 'C', 's');
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');

        // A refused portal is not left looking like a drained one: executing it again refuses again rather than
        // answering an empty CommandComplete
        sendExecute(out, "P1", 2);
        sendSync(out);
        final List<WireMessage> again = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(again)).startsWith('E').doesNotContain('D', 'C');
      });
    }
  }

  @Test
  @DisplayName("[#7034] a portal within the cap is still paginated by the Execute row limit exactly as before")
  void portalWithinTheCapIsPaginatedNormally() throws Exception {
    createRows(ROW_CAP);

    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendParse(out, "SELECT FROM " + TYPE);
        sendBind(out, "P1");
        sendDescribe(out, "P1");
        sendExecute(out, "P1", 3);
        sendSync(out);
        final List<WireMessage> first = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(first)).as("exactly the cap is not refused").doesNotContain('E');
        assertThat(first.stream().filter(m -> m.type() == 'D').count()).isEqualTo(3);
        assertThat(messageTypesOf(first)).as("suspended with rows left").contains('s').doesNotContain('C');

        sendExecute(out, "P1", 3);
        sendSync(out);
        final List<WireMessage> second = readUntilReadyForQuery(in);
        assertThat(second.stream().filter(m -> m.type() == 'D').count()).isEqualTo(ROW_CAP - 3);
        assertThat(messageTypesOf(second)).as("drained").contains('C').doesNotContain('s', 'E');
      });
    }
  }

  @Test
  @DisplayName("[#7034] a query that fails at Describe('P') is reported to the client as an ErrorResponse, not only logged")
  void queryFailingAtDescribeIsReportedToTheClient() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // Parses fine (the SQL parser does not check the schema), fails when run at Describe time
        sendParse(out, "SELECT FROM TypeThatDoesNotExist7034");
        sendBind(out, "P1");
        sendDescribe(out, "P1");
        sendExecute(out, "P1", 0);
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);

        assertThat(messageTypesOf(response)).startsWith('1', '2', 'E');
        assertThat(messageTypesOf(response)).doesNotContain('T', 'D', 'C');
        assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
      });
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

  private static void sendParse(final DataOutputStream out, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // unnamed statement
    writeCString(body, query);
    body.write(0);
    body.write(0); // int16 numParamDataTypes = 0
    writeFramed(out, 'P', body);
  }

  private static void sendBind(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    writeCString(body, ""); // unnamed statement
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0
    body.write(0);
    body.write(0); // int16 numParamValues = 0
    body.write(0);
    body.write(0); // int16 numResultFormatCodes = 0 (all text)
    writeFramed(out, 'B', body);
  }

  private static void sendDescribe(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('P');
    writeCString(body, portalName);
    writeFramed(out, 'D', body);
  }

  private static void sendExecute(final DataOutputStream out, final String portalName, final int maxRows) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    body.write((maxRows >>> 24) & 0xFF);
    body.write((maxRows >>> 16) & 0xFF);
    body.write((maxRows >>> 8) & 0xFF);
    body.write(maxRows & 0xFF);
    writeFramed(out, 'E', body);
  }

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  private static void writeFramed(final DataOutputStream out, final char type, final ByteArrayOutputStream body) throws Exception {
    final byte[] bodyBytes = body.toByteArray();
    out.writeByte(type);
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
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
