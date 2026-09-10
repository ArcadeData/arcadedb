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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.util.PSQLException;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Issue #7188: {@code COPY (query) TO STDOUT} in text, CSV and binary format, on the simple-query protocol
 * ({@code psql \copy}, pgjdbc's {@code CopyManager}) and on the extended one the Arrow ADBC PostgreSQL driver uses
 * for every result set it reads.
 */
class Issue7188CopyToStdoutIT extends PostgresWireProtocolTestBase {

  private static final String TYPE = "copy7188";
  /**
   * Smaller than the table, so a plain SELECT of the whole table is refused: proof that a COPY whose columns the
   * schema names is streamed rather than held, which is the property a bulk export needs from it.
   */
  private static final int    ROW_CAP = 3;

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

  @BeforeEach
  void populate() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("id", Type.INTEGER);
    type.createProperty("name", Type.STRING);
    type.createProperty("price", Type.DOUBLE);
    type.createProperty("flag", Type.BOOLEAN);
    database.transaction(() -> {
      database.newDocument(TYPE).set("id", 1, "name", "plain", "price", 1.5, "flag", true).save();
      database.newDocument(TYPE).set("id", 2, "name", "tab\there", "price", 2.0, "flag", false).save();
      database.newDocument(TYPE).set("id", 3, "name", "back\\slash and \"quote\", comma", "price", 3.25, "flag", true).save();
      database.newDocument(TYPE).set("id", 4, "name", null, "price", null, "flag", null).save();
      database.newDocument(TYPE).set("id", 5, "name", "", "price", 0.0, "flag", false).save();
    });
  }

  private static final String ORDERED = "SELECT id, name, price, flag FROM " + TYPE + " ORDER BY id";

  @Test
  void textFormatIsWhatPsqlCopyReads() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final String text = copyOut(connection, "COPY (" + ORDERED + ") TO STDOUT");
      assertThat(text).isEqualTo("""
          1\tplain\t1.5\tt
          2\ttab\\there\t2.0\tf
          3\tback\\\\slash and "quote", comma\t3.25\tt
          4\t\\N\t\\N\t\\N
          5\t\t0.0\tf
          """);
    }
  }

  @Test
  void csvFormatWithHeaderAndOptions() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      assertThat(copyOut(connection, "COPY (" + ORDERED + ") TO STDOUT (FORMAT csv, HEADER)")).isEqualTo("""
          id,name,price,flag
          1,plain,1.5,t
          2,tab\there,2.0,f
          3,"back\\slash and ""quote"", comma",3.25,t
          4,,,
          5,"",0.0,f
          """);

      // The legacy keyword spelling, as a \\copy user types it.
      assertThat(copyOut(connection, "COPY (SELECT id, name FROM " + TYPE + " WHERE id < 3 ORDER BY id) TO STDOUT CSV HEADER DELIMITER ';' NULL 'nil'"))
          .isEqualTo("id;name\n1;plain\n2;tab\there\n");
    }
  }

  @Test
  void binaryFormatIsWhatTheArrowAdbcDriverDecodes() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      final long rows = copyManager(connection).copyOut("COPY (" + ORDERED + ") TO STDOUT (FORMAT binary)", bytes);
      assertThat(rows).isEqualTo(5);

      final List<List<Object>> tuples = decodeBinaryCopy(bytes.toByteArray(), new int[] { 23, 1043, 701, 16 });
      assertThat(tuples).hasSize(5);
      assertThat(tuples.get(0)).containsExactly(1, "plain", 1.5, true);
      assertThat(tuples.get(2)).containsExactly(3, "back\\slash and \"quote\", comma", 3.25, true);
      assertThat(tuples.get(3)).as("NULL is a field of length -1").containsExactly(4, null, null, null);
      assertThat(tuples.get(4)).containsExactly(5, "", 0.0, false);
    }
  }

  @Test
  void theTableFormIsTheSameStatementWithAnImplicitSelect() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      assertThat(copyOut(connection, "COPY " + TYPE + " (id, name) TO STDOUT"))
          .contains("1\tplain\n")
          .contains("4\t\\N\n");
      // pg_dump and psql qualify the name, and quote it: the rewriter turns the quotes into back-ticks.
      assertThat(copyOut(connection, "COPY public.\"" + TYPE + "\" (id) TO STDOUT")).hasLineCount(5);
    }
  }

  @Test
  void aCopyStreamsPastTheRowCapAPlainSelectIsRefusedBy() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      // The cap is real for a query: its rows have to be held to name its columns.
      try (final Statement statement = connection.createStatement()) {
        assertThatThrownBy(() -> statement.executeQuery(ORDERED))
            .isInstanceOf(PSQLException.class)
            .hasMessageContaining("exceeds the configured limit of " + ROW_CAP);
      }
      // A COPY of the same statement names its columns from the schema and streams every row.
      assertThat(copyOut(connection, "COPY (" + ORDERED + ") TO STDOUT")).hasLineCount(5);
    }
  }

  @Test
  void whatIsDeclinedIsDeclinedWithFeatureNotSupportedAndTheConnectionStaysUsable() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      assertThatThrownBy(() -> copyManager(connection).copyIn("COPY " + TYPE + " FROM STDIN", new java.io.ByteArrayInputStream(new byte[0])))
          .isInstanceOf(PSQLException.class)
          .hasMessageContaining("COPY ... FROM STDIN is not supported")
          .extracting(e -> ((PSQLException) e).getSQLState()).isEqualTo("0A000");

      assertThatThrownBy(() -> copyOut(connection, "COPY (" + ORDERED + ") TO '/tmp/leak.csv'"))
          .isInstanceOf(PSQLException.class)
          .extracting(e -> ((PSQLException) e).getSQLState()).isEqualTo("0A000");

      // A COPY that is well-formed but whose query is not: the engine's own syntax error, and no CopyOutResponse.
      assertThatThrownBy(() -> copyOut(connection, "COPY (SELEC id FROM " + TYPE + ") TO STDOUT"))
          .isInstanceOf(PSQLException.class)
          .extracting(e -> ((PSQLException) e).getSQLState()).isEqualTo("42601");

      // An option PostgreSQL itself refuses.
      assertThatThrownBy(() -> copyOut(connection, "COPY (" + ORDERED + ") TO STDOUT (FORMAT binary, HEADER)"))
          .isInstanceOf(PSQLException.class)
          .hasMessageContaining("HEADER in BINARY");

      // After every refusal the session answers the next statement.
      assertThat(copyOut(connection, "COPY (SELECT count(*) AS n FROM " + TYPE + ") TO STDOUT")).isEqualTo("5\n");
    }
  }

  /**
   * The exact exchange the Arrow ADBC PostgreSQL driver has with the server for one query: it prepares and
   * describes the statement to learn the column OIDs, then runs {@code COPY (statement) TO STDOUT (FORMAT binary)}
   * through {@code PQexecParams} - Parse, Bind, Describe(portal), Execute, Sync - and decodes the stream by those
   * OIDs. Every reply has to be the one the protocol defines, in order, or the driver desynchronizes.
   */
  @Test
  void theExtendedProtocolExchangeTheAdbcDriverMakes() throws Exception {
    final String query = "SELECT id, name FROM " + TYPE + " ORDER BY id";
    withConnection((out, in) -> {
      // 1. Describe the statement inside the COPY, as the driver does first.
      sendParse(out, query);
      sendDescribeStatement(out);
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('t');
      final WireMessage rowDescription = readWireMessage(in);
      assertThat(rowDescription.type()).isEqualTo('T');
      assertThat(columnOidsOf(rowDescription)).containsExactly(23, 1043);
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      // 2. Describe the COPY statement itself: parameters, then NoData - a COPY has no result set.
      sendParse(out, "COPY (" + query + ") TO STDOUT (FORMAT binary)");
      sendDescribeStatement(out);
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('t');
      assertThat(readWireMessage(in).type()).isEqualTo('n');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      // 3. PQexecParams of the COPY.
      sendParse(out, "COPY (" + query + ") TO STDOUT (FORMAT binary)");
      sendBind(out);
      sendDescribePortal(out);
      sendExecute(out);
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('2');
      assertThat(readWireMessage(in).type()).isEqualTo('n');

      final WireMessage copyOutResponse = readWireMessage(in);
      assertThat(copyOutResponse.type()).isEqualTo('H');
      final ByteBuffer h = ByteBuffer.wrap(copyOutResponse.body());
      assertThat(h.get()).as("overall format: binary").isEqualTo((byte) 1);
      assertThat(h.getShort()).as("column count").isEqualTo((short) 2);
      assertThat(h.getShort()).isEqualTo((short) 1);
      assertThat(h.getShort()).isEqualTo((short) 1);

      final ByteArrayOutputStream stream = new ByteArrayOutputStream();
      WireMessage message = readWireMessage(in);
      int copyDataMessages = 0;
      while (message.type() == 'd') {
        stream.write(message.body());
        copyDataMessages++;
        message = readWireMessage(in);
      }
      assertThat(copyDataMessages).as("header, five rows, trailer").isEqualTo(7);
      assertThat(message.type()).as("CopyDone").isEqualTo('c');

      final WireMessage complete = readWireMessage(in);
      assertThat(complete.type()).isEqualTo('C');
      assertThat(cString(complete.body())).isEqualTo("COPY 5");
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      final List<List<Object>> tuples = decodeBinaryCopy(stream.toByteArray(), new int[] { 23, 1043 });
      assertThat(tuples).hasSize(5);
      assertThat(tuples.get(1)).containsExactly(2, "tab\there");
      assertThat(tuples.get(3)).containsExactly(4, null);

      // 4. The session is intact: a plain statement after the COPY is answered as usual.
      sendParse(out, "SELECT count(*) AS n FROM " + TYPE);
      sendBind(out);
      sendExecute(out);
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('2');
      assertThat(readWireMessage(in).type()).isEqualTo('T');
      assertThat(readWireMessage(in).type()).isEqualTo('D');
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');
    });
  }

  @Test
  void aBrokenQueryInsideACopyIsRefusedAtParseOnTheExtendedProtocol() throws Exception {
    withConnection((out, in) -> {
      sendParse(out, "COPY (SELEC id FROM " + TYPE + ") TO STDOUT");
      sendSync(out);
      final WireMessage error = readWireMessage(in);
      assertThat(error.type()).isEqualTo('E');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      sendParse(out, "COPY " + TYPE + " FROM STDIN");
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('E');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');
    });
  }

  // ---- helpers ----

  private static String copyOut(final Connection connection, final String copy) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    copyManager(connection).copyOut(copy, bytes);
    return bytes.toString(StandardCharsets.UTF_8);
  }

  private static CopyManager copyManager(final Connection connection) throws Exception {
    return connection.unwrap(PGConnection.class).getCopyAPI();
  }

  /**
   * Decodes a binary COPY stream: the signature, flags and header extension, then per tuple an int16 field count
   * and per field an int32 length (-1 for NULL) followed by the value in the type's binary send format.
   */
  private static List<List<Object>> decodeBinaryCopy(final byte[] bytes, final int[] oids) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] signature = new byte[11];
    buffer.get(signature);
    assertThat(signature).isEqualTo(new byte[] { 'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xFF, '\r', '\n', 0 });
    assertThat(buffer.getInt()).as("flags").isZero();
    final int extensionLength = buffer.getInt();
    buffer.position(buffer.position() + extensionLength); // header extension

    final List<List<Object>> tuples = new ArrayList<>();
    while (true) {
      final short fields = buffer.getShort();
      if (fields == -1)
        break;
      assertThat(fields).isEqualTo((short) oids.length);
      final List<Object> tuple = new ArrayList<>();
      for (int i = 0; i < fields; i++) {
        final int length = buffer.getInt();
        if (length == -1) {
          tuple.add(null);
          continue;
        }
        final byte[] value = new byte[length];
        buffer.get(value);
        final ByteBuffer v = ByteBuffer.wrap(value);
        tuple.add(switch (oids[i]) {
          case 23 -> v.getInt();
          case 701 -> v.getDouble();
          case 16 -> value[0] == 1;
          default -> new String(value, StandardCharsets.UTF_8);
        });
      }
      tuples.add(tuple);
    }
    assertThat(buffer.hasRemaining()).as("nothing follows the trailer").isFalse();
    return tuples;
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    properties.setProperty("preferQueryMode", "simple");
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
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
      readMessageOfType(in, 'Z');

      // A hang detector, not a latency bound: a reply the server never sends leaves the client blocked in
      // readFully() forever, and only a preemptive timeout can end that.
      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> exchange.run(out, in));
    }
  }

  private record WireMessage(char type, byte[] body) {
  }

  private static WireMessage readWireMessage(final DataInputStream in) throws Exception {
    final char type = (char) in.readUnsignedByte();
    final int length = in.readInt();
    final byte[] body = new byte[length - 4];
    in.readFully(body);
    return new WireMessage(type, body);
  }

  private static String cString(final byte[] body) {
    int end = 0;
    while (end < body.length && body[end] != 0)
      end++;
    return new String(body, 0, end, StandardCharsets.UTF_8);
  }

  /**
   * The type OIDs in a {@code RowDescription}: int16 count, then per column a null-terminated name, int32 table
   * OID, int16 attribute number, int32 type OID, int16 size, int32 modifier, int16 format.
   */
  private static List<Integer> columnOidsOf(final WireMessage rowDescription) {
    final ByteBuffer buffer = ByteBuffer.wrap(rowDescription.body());
    final int count = buffer.getShort();
    final List<Integer> oids = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      while (buffer.get() != 0)
        ;
      buffer.getInt();
      buffer.getShort();
      oids.add(buffer.getInt());
      buffer.getShort();
      buffer.getInt();
      buffer.getShort();
    }
    return oids;
  }

  private static void sendParse(final DataOutputStream out, final String query) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, "");
    writeCString(body, query);
    body.write(0);
    body.write(0);
    send(out, 'P', body);
  }

  private static void sendBind(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, "");
    writeCString(body, "");
    body.write(0);
    body.write(0);
    body.write(0);
    body.write(0);
    body.write(0);
    body.write(0);
    send(out, 'B', body);
  }

  private static void sendDescribePortal(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('P');
    writeCString(body, "");
    send(out, 'D', body);
  }

  private static void sendDescribeStatement(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('S');
    writeCString(body, "");
    send(out, 'D', body);
  }

  private static void sendExecute(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, "");
    body.write(0);
    body.write(0);
    body.write(0);
    body.write(0);
    send(out, 'E', body);
  }

  private static void sendSync(final DataOutputStream out) throws Exception {
    out.writeByte('S');
    out.writeInt(4);
    out.flush();
  }

  private static void send(final DataOutputStream out, final char type, final ByteArrayOutputStream body) throws Exception {
    final byte[] bytes = body.toByteArray();
    out.writeByte(type);
    out.writeInt(4 + bytes.length);
    out.write(bytes);
    out.flush();
  }
}
