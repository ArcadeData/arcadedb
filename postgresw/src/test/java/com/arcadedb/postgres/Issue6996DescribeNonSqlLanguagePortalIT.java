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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for issue #6996, the half of #6930 that stayed unanswered: a portal whose query names a
 * language other than {@code "sql"} - {@code {cypher} MATCH ...} and friends - reaches {@code Describe('P')}
 * with no parsed SQL statement and no columns, and used to be answered with NOTHING at all.
 * <p>
 * The protocol has no such reply. Every {@code Describe} is owed exactly one of {@code RowDescription} or
 * {@code NoData}, so a client that counts one reply per request (pgjdbc does) consumed the {@code Execute}'s
 * own {@code RowDescription} as the answer to its pending {@code Describe} and read every later reply on the
 * connection one message off.
 * <p>
 * {@code NoData} would have kept the stream aligned but lied: a {@code {cypher}} portal does produce rows.
 * The fix runs the portal at {@code Describe} time - which is exactly what the parsed-SQL form has always
 * done - and answers from the rows it actually produced, so the announced shape is the real one. The run is
 * shared with {@code Execute}, so the query is still executed exactly once per portal however the client
 * orders the two messages; {@code aWriteCommandDescribedThenExecutedRunsExactlyOnce} pins that.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6996DescribeNonSqlLanguagePortalIT extends PostgresWireProtocolTestBase {

  @Test
  @DisplayName("[#6996] Describe(P) on a {cypher} portal answers RowDescription with the real columns")
  void cypherPortalDescribeIsAnsweredWithTheColumnsItWillReturn() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996 IF NOT EXISTS");
      runSimpleQuery(out, in, "INSERT INTO V6996 SET name = 'alice'");

      // Sync BEFORE Execute, so the Describe's reply cannot be confused with the Execute's own
      // RowDescription: this is the exchange that makes the missing reply observable at all. Sent in one
      // message batch and read afterwards, the two are indistinguishable - '1','2','T','D','C','Z' either
      // way - which is exactly the accidental compensation the issue describes.
      sendParse(out, "{cypher} MATCH (n:V6996) RETURN n.name AS name");
      sendBind(out);
      sendDescribePortal(out);
      sendSync(out);

      assertThat(readWireMessage(in).type()).isEqualTo('1'); // ParseComplete
      assertThat(readWireMessage(in).type()).isEqualTo('2'); // BindComplete

      final WireMessage describeReply = readWireMessage(in);
      assertThat(describeReply.type())
          .as("Describe('P') on a {cypher} portal must answer RowDescription, not silence and not NoData")
          .isEqualTo('T');
      assertThat(fieldNamesOf(describeReply))
          .as("and the shape it announces must be the one the rows actually carry")
          .containsExactly("name");
      assertThat(readWireMessage(in).type()).isEqualTo('Z'); // ReadyForQuery

      // Execute must NOT re-announce the shape: the portal was already described.
      sendExecute(out);
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('D');
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');
    });
  }

  @Test
  @DisplayName("[#6996] A {cypher} portal returning no row still answers Describe(P) exactly once")
  void anEmptyCypherResultStillGetsExactlyOneDescribeReply() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996Empty IF NOT EXISTS");

      sendParse(out, "{cypher} MATCH (n:V6996Empty) RETURN n.name AS name");
      sendBind(out);
      sendDescribePortal(out);
      sendSync(out);

      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('2');

      final WireMessage describeReply = readWireMessage(in);
      assertThat(describeReply.type())
          .as("one reply, whatever the row count: silence is what desynchronizes the client")
          .isIn('T', 'n');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      // No rows, so Execute answers with the command tag alone, and the connection stays in sync.
      sendExecute(out);
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');
    });
  }

  @Test
  @DisplayName("[#6996] Describe(P)+Execute of a {cypher} write command runs it exactly once")
  void aWriteCommandDescribedThenExecutedRunsExactlyOnce() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996Write IF NOT EXISTS");

      sendParse(out, "{cypher} CREATE (n:V6996Write {name: 'once'}) RETURN n.name AS name");
      sendBind(out);
      sendDescribePortal(out);
      sendSync(out);

      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('2');
      assertThat(readWireMessage(in).type())
          .as("the write is run once, at Describe, and its own rows describe it")
          .isEqualTo('T');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      sendExecute(out);
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('D');
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      // The Execute that followed the Describe must have replayed the materialized result rather than
      // running the CREATE a second time.
      assertThat(rowCountOf(out, in, "SELECT count(*) AS total FROM V6996Write")).isEqualTo(1);
    });
  }

  /**
   * The simple-query ('Q') form of the same {@code {cypher}} query, which has always worked: the extended
   * protocol's answer above must carry the same column.
   */
  @Test
  @DisplayName("[#6996] The extended protocol announces the same columns the simple query does")
  void theSimpleQueryFormAgrees() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996Simple IF NOT EXISTS");
      runSimpleQuery(out, in, "INSERT INTO V6996Simple SET name = 'bob'");

      sendSimpleQuery(out, "{cypher} MATCH (n:V6996Simple) RETURN n.name AS name");
      WireMessage message = readWireMessage(in);
      while (message.type() != 'T' && message.type() != 'Z')
        message = readWireMessage(in);
      assertThat(message.type()).isEqualTo('T');
      assertThat(fieldNamesOf(message)).containsExactly("name");
      drainToReadyForQuery(in);
    });
  }

  private interface Exchange {
    void run(DataOutputStream out, DataInputStream in) throws Exception;
  }

  private void withConnection(final Exchange exchange) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> exchange.run(out, in));
    }
  }

  /**
   * Runs one simple query and drains its replies up to ReadyForQuery.
   */
  private static void runSimpleQuery(final DataOutputStream out, final DataInputStream in, final String query)
      throws Exception {
    sendSimpleQuery(out, query);
    drainToReadyForQuery(in);
  }

  /**
   * Runs a one-row, one-column count query over the simple protocol and returns the number it answered.
   */
  private static long rowCountOf(final DataOutputStream out, final DataInputStream in, final String query)
      throws Exception {
    sendSimpleQuery(out, query);
    long count = -1;
    while (true) {
      final WireMessage message = readWireMessage(in);
      if (message.type() == 'Z')
        break;
      if (message.type() == 'D')
        count = Long.parseLong(firstColumnOf(message));
    }
    return count;
  }

  private static void drainToReadyForQuery(final DataInputStream in) throws Exception {
    while (readWireMessage(in).type() != 'Z')
      ;
  }

  private static void sendSimpleQuery(final DataOutputStream out, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, query);

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('Q');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
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
    body.write('P'); // describe a portal, not a prepared statement
    writeCString(body, ""); // unnamed portal

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('D');
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

  /**
   * A {@code RowDescription} body is an int16 field count followed by one null-terminated name and six
   * fixed-width fields per column.
   */
  private static List<String> fieldNamesOf(final WireMessage message) {
    final ByteBuffer buffer = ByteBuffer.wrap(message.body());
    final int fields = buffer.getShort() & 0xFFFF;
    final List<String> names = new ArrayList<>(fields);
    for (int i = 0; i < fields; i++) {
      names.add(readCString(buffer));
      buffer.position(buffer.position() + 18); // tableOid, columnIndex, typeOid, typeSize, typeModifier, format
    }
    return names;
  }

  /**
   * A {@code DataRow} body is an int16 column count followed by, per column, an int32 length and its bytes.
   */
  private static String firstColumnOf(final WireMessage message) {
    final ByteBuffer buffer = ByteBuffer.wrap(message.body());
    buffer.getShort(); // column count
    final int length = buffer.getInt();
    final byte[] value = new byte[length];
    buffer.get(value);
    return new String(value, StandardCharsets.UTF_8);
  }

  private static String readCString(final ByteBuffer buffer) {
    final StringBuilder text = new StringBuilder();
    for (byte b = buffer.get(); b != 0; b = buffer.get())
      text.append((char) (b & 0xFF));
    return text.toString();
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
}
