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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6725, caught by the ArcadeDB Python e2e suite's
 * {@code test_asyncpg.py::test_parameterized_insert} in CI (asyncpg.exceptions._base.ProtocolError /
 * AssertionError "insufficient data in buffer: requested 4 remaining 3").
 * <p>
 * asyncpg Parses and Describes('S') a given SQL text only the FIRST time it sees it on a connection, then
 * reuses that cached RowDescription (and the OIDs/format codes it negotiated from it) for every later
 * Bind+Execute of the same text with different parameters. {@code PostgresNetworkExecutor.describeCommand()}
 * mirrors this: Describe('S') for a plain "SELECT ... FROM &lt;Type&gt;" resolves column OIDs once, via
 * {@code getColumnsFromType()} sampling a single arbitrary row ({@code SELECT FROM <Type> LIMIT 1}), and
 * caches the result on the prepared-statement template - {@code PostgresPortal.bindFrom()} then copies that
 * cached {@code columns} map into every portal bound from it afterwards.
 * <p>
 * {@code AsyncpgTest} (and this test's {@code Types6725}) is a schemaless DOCUMENT TYPE: the same property
 * can hold an {@code INTEGER} in one row and a {@code VARCHAR} in another. Before this fix, when the sampled
 * row's "val" was an {@code INTEGER} (OID int4, a real fixed-width 4-byte binary encoding) but a later
 * Bind+Execute of the very same prepared statement returned a row whose "val" was a {@code VARCHAR},
 * {@code executeCommand()} would recompute {@code portal.columns} from that row's own actual type and send a
 * SECOND, unsolicited RowDescription mid-Execute - something real PostgreSQL never does once a statement has
 * been Described - serialized as VARCHAR (3 ASCII bytes for "300") even though the client, going by the
 * FIRST RowDescription, had already committed to decoding that column as binary int4 (a fixed 4 bytes),
 * desyncing its decoder. The fix: once Describe('S') already told the client a shape (portal.columns is
 * non-null), executeCommand() keeps serializing under that same shape - coercing the actual value into it,
 * exactly as it already did for any portal explicitly Describe('P')-ed - instead of silently swapping it out
 * and re-announcing it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6725DescribeSampleRowTypeMismatchIT extends PostgresWireProtocolTestBase {

  @Test
  void aSchemalessColumnsDescribedShapeSurvivesALaterExecuteOfADifferentlyTypedRow() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE Types6725 IF NOT EXISTS");
      // id=1's "val" is a real INTEGER (a literal, not a bind parameter) - the row DESCRIBE('S') below will
      // sample, since it is the only row in the type at that point.
      runSimpleQueryToCompletion(out, in, "INSERT INTO Types6725 SET id = 1, val = 100");

      // Parse once, then Describe('S') - exactly what asyncpg does the FIRST time it sees this SQL text.
      // This resolves and caches "val"'s OID from the id=1 sample above: int4.
      sendParse(out, "SELECT val FROM Types6725 WHERE id = $1");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');
      sendDescribeStatement(out);
      sendSync(out);
      assertThat(readOneMessage(in).type).as("ParameterDescription").isEqualTo('t');
      assertThat(readOneMessage(in).type).as("RowDescription, from sampling id=1's INTEGER val").isEqualTo('T');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // id=2's "val" is a literal String '300' - a different Postgres OID than id=1's "val" on the very same
      // schemaless property.
      runSimpleQueryToCompletion(out, in, "INSERT INTO Types6725 SET id = 2, val = '300'");

      // Bind+Execute the SAME prepared statement (no new Parse/Describe) for id=2, requesting BINARY format
      // for the result column - what asyncpg does once it has been told (by the id=1 RowDescription above)
      // that this column is a native scalar type with a binary codec.
      sendBindWithOneTextParamBinaryResult(out, "P1", "2");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');
      sendExecute(out, "P1", 0);
      sendSync(out);

      final Msg row = readOneMessage(in);
      assertThat(row.type)
          .as("DataRow for id=2 directly, no unsolicited second RowDescription squeezed in - the client was "
              + "already told this statement's shape by the Describe('S') above and isn't expecting another")
          .isEqualTo('D');
      final DataInputStream rowIn = new DataInputStream(new ByteArrayInputStream(row.payload));
      assertThat(rowIn.readUnsignedShort()).as("this query projects exactly one column").isEqualTo(1);
      final int declaredLength = rowIn.readInt();
      assertThat(declaredLength)
          .as("the DESCRIBE-promised int4 binary width (4 bytes) must be honored for every row this portal "
              + "returns, even one - like id=2's \"val\" - whose actual value is a String \"300\"")
          .isEqualTo(4);
      final byte[] valueBytes = new byte[declaredLength];
      rowIn.readFully(valueBytes);
      final int decoded = ((valueBytes[0] & 0xFF) << 24) | ((valueBytes[1] & 0xFF) << 16) | ((valueBytes[2] & 0xFF) << 8) | (valueBytes[3] & 0xFF);
      assertThat(decoded).as("the String \"300\" coerces cleanly into the promised int4 column, same as a real INTEGER value would").isEqualTo(300);
    }
  }

  private record Msg(char type, byte[] payload) {
  }

  private static Msg readOneMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    final byte[] payload = new byte[length - 4];
    in.readFully(payload);
    return new Msg((char) type, payload);
  }

  private static void runSimpleQueryToCompletion(final DataOutputStream out, final DataInputStream in, final String sql) throws Exception {
    final byte[] queryBytes = (sql + "\0").getBytes(StandardCharsets.UTF_8);
    out.writeByte('Q');
    out.writeInt(4 + queryBytes.length);
    out.write(queryBytes);
    out.flush();
    readMessageOfType(in, 'Z'); // drains RowDescription/CommandComplete/etc. through ReadyForQuery
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

  private static void sendDescribeStatement(final DataOutputStream out) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('S');
    writeCString(body, ""); // unnamed statement

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('D');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  /**
   * Binds the unnamed statement to the given portal with one text param, requesting BINARY format for the
   * result column - what asyncpg does once it has negotiated a native scalar OID for that column.
   */
  private static void sendBindWithOneTextParamBinaryResult(final DataOutputStream out, final String portalName, final String paramValue)
      throws Exception {
    final byte[] paramBytes = paramValue.getBytes(StandardCharsets.UTF_8);
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    writeCString(body, ""); // unnamed source statement
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0 (all text)
    body.write(0);
    body.write(1); // int16 numParamValues = 1
    body.write((paramBytes.length >>> 24) & 0xFF);
    body.write((paramBytes.length >>> 16) & 0xFF);
    body.write((paramBytes.length >>> 8) & 0xFF);
    body.write(paramBytes.length & 0xFF);
    body.write(paramBytes);
    body.write(0);
    body.write(1); // int16 numResultFormatCodes = 1
    body.write(0);
    body.write(1); // format code 1 = binary, applies to all columns

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('B');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private static void sendExecute(final DataOutputStream out, final String portalName, final int maxRows) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, portalName);
    body.write((maxRows >>> 24) & 0xFF);
    body.write((maxRows >>> 16) & 0xFF);
    body.write((maxRows >>> 8) & 0xFF);
    body.write(maxRows & 0xFF);

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
}
