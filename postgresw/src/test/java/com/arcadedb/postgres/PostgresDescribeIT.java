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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.firstDataRowValue;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readWireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.sendBind;
import static com.arcadedb.postgres.PostgresWireMessages.sendDescribe;
import static com.arcadedb.postgres.PostgresWireMessages.sendExecute;
import static com.arcadedb.postgres.PostgresWireMessages.sendParse;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireMessages.sendSync;
import static com.arcadedb.postgres.PostgresWireMessages.sqlStateOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Describe ('S' for a prepared statement, 'P' for a portal) over the PostgreSQL wire protocol: the RowDescription or NoData
 * it answers must be the one the following Execute keeps to, whatever the sampled row, the statement (a SET, a SAVEPOINT,
 * an INSERT, a computed column) or the query language of the portal.
 */
class PostgresDescribeIT extends PostgresWireProtocolTestBase {

  /*
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
  // Regression: #6725
  @Test
  void aSchemalessColumnsDescribedShapeSurvivesALaterExecuteOfADifferentlyTypedRow() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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
      sendParse(out, "", "SELECT val FROM Types6725 WHERE id = $1");
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
          .as("the promised column is varchar, because \"val\" is undeclared and only text holds every record's value "
              + "(issue #9009): the row's own String \"300\" is written as its 3 bytes, with no second layout")
          .isEqualTo(3);
      final byte[] valueBytes = new byte[declaredLength];
      rowIn.readFully(valueBytes);
      assertThat(new String(valueBytes, StandardCharsets.UTF_8)).isEqualTo("300");
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

  /*
   * Regression tests for issue #6930: a {@code Describe('P')} on a portal bound from {@code SET}/{@code SAVEPOINT}/
   * {@code RELEASE}/{@code ROLLBACK TO} sent no reply at all (every {@code Describe} must be answered by exactly
   * one of {@code RowDescription} or {@code NoData}), and the matching {@code Execute} answered {@code NoData}
   * ({@code 'n'}, a Describe-only reply) instead of {@code CommandComplete} ({@code 'C'}).
   * <p>
   * These tests speak the wire protocol directly over a raw socket so the extended-query {@code Describe('P')}
   * path is actually exercised, and so the exact reply sequence and the {@code CommandComplete} tag content can
   * be inspected (a JDBC client that always describes the statement, rather than the portal, would never reach
   * the first bug, and pgjdbc historically tolerated the second by accident - see the issue for why).
   * <p>
   * {@code ROLLBACK TO} no longer reaches Describe/Execute at all: issue #7846 made it fail at Parse instead of
   * being accepted as a no-op, since a silent {@code CommandComplete} for a rollback this server can't perform
   * would let a client believe writes made after the savepoint were undone when they are still pending.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6930
  @Test
  @DisplayName("[#6930] Describe(P)+Execute on a SET portal answers NoData then CommandComplete 'SET'")
  void setPortalDescribeAndExecute() throws Exception {
    assertReplySequenceForIgnoredExecutionStatement("SET datestyle = 'ISO'", "SET");
  }

  // Regression: #6930
  @Test
  @DisplayName("[#6930] Describe(P)+Execute on a SAVEPOINT portal answers NoData then CommandComplete 'SAVEPOINT'")
  void savepointPortalDescribeAndExecute() throws Exception {
    assertReplySequenceForIgnoredExecutionStatement("SAVEPOINT test_savepoint", "SAVEPOINT");
  }

  // Regression: #6930
  @Test
  @DisplayName("[#6930] Describe(P)+Execute on a RELEASE portal answers NoData then CommandComplete 'RELEASE'")
  void releasePortalDescribeAndExecute() throws Exception {
    assertReplySequenceForIgnoredExecutionStatement("SAVEPOINT test_savepoint2", "SAVEPOINT");
    assertReplySequenceForIgnoredExecutionStatement("RELEASE test_savepoint2", "RELEASE");
  }

  // Regression: #6930
  @Test
  @DisplayName("[#7846] Parse on a ROLLBACK TO statement answers ErrorResponse, not ParseComplete, and aborts the transaction")
  void rollbackToStatementFailsAtParse() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "BEGIN");
        readMessageOfType(in, 'Z');

        // A ROLLBACK TO this server can't honor must not be silently accepted the way SAVEPOINT/RELEASE are
        // above: this server has no savepoint checkpoint to roll back to, so claiming success here would let
        // the client believe writes made after the savepoint were undone when they are still pending.
        sendParse(out, "", "ROLLBACK TO test_savepoint4");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);

        boolean sawError = false;
        WireMessage message;
        do {
          message = readWireMessage(in);
          if (message.type() == 'E')
            sawError = true;
          // ParseComplete must not appear: Parse itself is what refuses the statement.
          assertThat(message.type()).as("ROLLBACK TO must not be accepted as ParseComplete").isNotEqualTo('1');
        } while (message.type() != 'Z');
        assertThat(sawError).as("ROLLBACK TO must be refused with an ErrorResponse").isTrue();
        // Sync's own error-recovery branch (syncCommand()) has rolled the underlying transaction back at this
        // point, but it does not END the transaction block: the session stays aborted and reports 'E' until the
        // client sends COMMIT/ROLLBACK/END, exactly as the simple query protocol already behaved and as real
        // PostgreSQL requires (issue #7851 - this used to report 'T', which told the client the transaction was
        // healthy and free to take further statements). The ROLLBACK below is that end of block.
        assertThat(message.body()[0]).as("Sync leaves the aborted transaction aborted").isEqualTo((byte) 'E');

        sendSimpleQuery(out, "ROLLBACK");
        readMessageOfType(in, 'Z');
      });
    }
  }

  private void assertReplySequenceForIgnoredExecutionStatement(final String query, final String expectedTag) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendParse(out, "", query);
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);

        final WireMessage parseComplete = readWireMessage(in);
        assertThat(parseComplete.type()).isEqualTo('1');
        final WireMessage bindComplete = readWireMessage(in);
        assertThat(bindComplete.type()).isEqualTo('2');

        // Exactly one Describe reply, and no rows since these statements never produce any.
        final WireMessage describeReply = readWireMessage(in);
        assertThat(describeReply.type()).as("Describe('P') on a %s portal must answer NoData, not silence", query)
            .isEqualTo('n');

        // Execute must answer CommandComplete with the real command tag, never NoData.
        final WireMessage executeReply = readWireMessage(in);
        assertThat(executeReply.type()).as("Execute on a %s portal must answer CommandComplete, not NoData", query)
            .isEqualTo('C');
        assertThat(tagOf(executeReply)).isEqualTo(expectedTag);

        final WireMessage readyForQuery = readWireMessage(in);
        assertThat(readyForQuery.type()).isEqualTo('Z');
      });
    }
  }

  /**
   * A {@code CommandComplete} body is a single null-terminated tag string with no other fields.
   */
  private static String tagOf(final WireMessage message) {
    final byte[] body = message.body();
    return new String(body, 0, body.length - 1, StandardCharsets.UTF_8);
  }

  /*
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
   * <p>
   * A portal that names no column at all is still announced as an (empty) {@code RowDescription} rather than
   * {@code NoData} - see {@code PostgresNetworkExecutor.answerWithColumns}. The two tests below that reach
   * that state, a write with nothing to return and a query that matched nothing, are indistinguishable from
   * each other on this side of the wire, and {@code NoData} is only correct for the first: it promises no
   * result set at all, which pgjdbc holds the server to.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6996
  @Test
  @DisplayName("[#6996] Describe(P) on a {cypher} portal answers RowDescription with the real columns")
  void cypherPortalDescribeIsAnsweredWithTheColumnsItWillReturn() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996 IF NOT EXISTS");
      runSimpleQuery(out, in, "INSERT INTO V6996 SET name = 'alice'");

      // Flush BEFORE Execute, so the Describe's reply cannot be confused with the Execute's own
      // RowDescription: this is the exchange that makes the missing reply observable at all. Sent in one
      // message batch and read afterwards, the two are indistinguishable - '1','2','T','D','C','Z' either
      // way - which is exactly the accidental compensation the issue describes. A Flush and not a Sync: a Sync
      // ends the implicit transaction block, and the portal with it (issue #8212).
      sendParse(out, "", "{cypher} MATCH (n:V6996) RETURN n.name AS name");
      sendBind(out, "", "");
      sendDescribe(out, 'P', "");
      sendFlush(out);

      assertThat(readWireMessage(in).type()).isEqualTo('1'); // ParseComplete
      assertThat(readWireMessage(in).type()).isEqualTo('2'); // BindComplete

      final WireMessage describeReply = readWireMessage(in);
      assertThat(describeReply.type())
          .as("Describe('P') on a {cypher} portal must answer RowDescription, not silence and not NoData")
          .isEqualTo('T');
      assertThat(fieldNamesOf(describeReply))
          .as("and the shape it announces must be the one the rows actually carry")
          .containsExactly("name");

      // Execute must NOT re-announce the shape: the portal was already described.
      sendExecute(out, "");
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('D');
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');
    });
  }

  // Regression: #6996
  @Test
  @DisplayName("[#6996] A {cypher} portal returning no row still answers Describe(P) exactly once")
  void anEmptyCypherResultStillGetsExactlyOneDescribeReply() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996Empty IF NOT EXISTS");

      sendParse(out, "", "{cypher} MATCH (n:V6996Empty) RETURN n.name AS name");
      sendBind(out, "", "");
      sendDescribe(out, 'P', "");
      sendFlush(out);

      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('2');

      final WireMessage describeReply = readWireMessage(in);
      assertThat(describeReply.type())
          .as("one reply, whatever the row count: silence is what desynchronizes the client")
          .isEqualTo('T');
      assertThat(fieldNamesOf(describeReply))
          .as("a query that matched nothing names no column: the columns come from the rows, and the schema "
              + "fallback behind them (issues #6156/#6185) reads the SQL parser, so it cannot answer for {cypher}")
          .isEmpty();

      // No rows, so Execute answers with the command tag alone, and the connection stays in sync.
      sendExecute(out, "");
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');
    });
  }

  // Regression: #6996
  @Test
  @DisplayName("[#6996] Describe(P)+Execute of a {cypher} write command runs it exactly once")
  void aWriteCommandDescribedThenExecutedRunsExactlyOnce() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996Write IF NOT EXISTS");

      sendParse(out, "", "{cypher} CREATE (n:V6996Write {name: 'once'}) RETURN n.name AS name");
      sendBind(out, "", "");
      sendDescribe(out, 'P', "");
      sendFlush(out);

      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('2');
      assertThat(readWireMessage(in).type())
          .as("the write is run once, at Describe, and its own rows describe it")
          .isEqualTo('T');

      sendExecute(out, "");
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('D');
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      // The Execute that followed the Describe must have replayed the materialized result rather than
      // running the CREATE a second time.
      assertThat(rowCountOf(out, in, "SELECT count(*) AS total FROM V6996Write")).isEqualTo(1);
    });
  }

  // Regression: #6996
  /**
   * A {@code {cypher}} write with no {@code RETURN} produces no row, so the Describe has no column to name.
   * It is still answered with an (empty) {@code RowDescription} rather than {@code NoData} - see
   * {@code PostgresNetworkExecutor.answerWithColumns} for why: {@code NoData} promises no result set at all,
   * and nothing distinguishes this portal from a query whose rows merely did not match, which pgjdbc's
   * {@code executeQuery()} would then reject outright.
   */
  @Test
  @DisplayName("[#6996] Describe(P) on a {cypher} write with no RETURN is still answered exactly once")
  void aWriteWithNothingToReturnIsStillAnsweredExactlyOnce() throws Exception {
    withConnection((out, in) -> {
      runSimpleQuery(out, in, "CREATE VERTEX TYPE V6996NoReturn IF NOT EXISTS");

      sendParse(out, "", "{cypher} CREATE (n:V6996NoReturn {name: 'silent'})");
      sendBind(out, "", "");
      sendDescribe(out, 'P', "");
      sendFlush(out);

      assertThat(readWireMessage(in).type()).isEqualTo('1');
      assertThat(readWireMessage(in).type()).isEqualTo('2');
      final WireMessage describeReply = readWireMessage(in);
      assertThat(describeReply.type())
          .as("a write with nothing to return is owed a reply like any other portal")
          .isEqualTo('T');
      assertThat(fieldNamesOf(describeReply))
          .as("and it names no column, because the command returns none")
          .isEmpty();

      // The Execute that follows answers CommandComplete alone: no rows, and no second RowDescription.
      sendExecute(out, "");
      sendSync(out);
      assertThat(readWireMessage(in).type()).isEqualTo('C');
      assertThat(readWireMessage(in).type()).isEqualTo('Z');

      assertThat(rowCountOf(out, in, "SELECT count(*) AS total FROM V6996NoReturn"))
          .as("and the write happened exactly once, at Describe")
          .isEqualTo(1);
    });
  }

  // Regression: #6996
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
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      // A hang detector, not a latency bound: the bug under test is a reply the server never sends, which
      // leaves the client blocked in readFully() forever. Only a preemptive timeout can end that - a
      // stopwatch cannot interrupt a blocked socket read, and neither can @Timeout's default thread mode -
      // so it is sized generously (a stall inside it cannot turn a passing run red) rather than tightly.
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

  private static void sendFlush(final DataOutputStream out) throws Exception {
    out.writeByte('H');
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

  /*
   * Issue #8285: a client that describes a statement before running it (Parse + {@code Describe('S')}) got every
   * projected column that is not a plain schema property announced as {@code varchar} (OID 1043) - {@code count(*)},
   * {@code sum(x)}, {@code max(n)}, arithmetic - even though the RowDescription of an executed query reports the
   * real type. Because a statement Describe is the contract later Executes of that statement honor (issue #6725),
   * pgjdbc's {@link PreparedStatement#getMetaData()}, called before the statement ever runs, is exactly that
   * pre-execution Describe, and it returned these aggregates and expressions as text for good.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #8285
  @Test
  void countStarDescribesAsInt8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT count(*) AS c FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Long.class);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void sumOfADoublePropertyDescribesAsFloat8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT sum(x) AS s FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("float8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Double.class);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void maxOfALongPropertyDescribesAsInt8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT max(n) AS m FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Long.class);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void arithmeticOverALongPropertyDescribesAsInt8BeforeExecution() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT n * 2 AS d FROM Items8285 WHERE n = 1")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(Long.class);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void aModifierAppliedToAnAggregateIsNotDescribedAsTheAggregatesOwnType() throws Exception {
    // count(*) is int8, but .asString() runs AFTER it and turns the result into a String - inferring from the
    // un-modified count() alone would describe the modifier's input, not its actual output (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT count(*).asString() AS c FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isNotEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(String.class);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void nonExactDivisionIsNotDescribedAsAnIntegerType() throws Exception {
    // MathExpression.Operator.SLASH returns a Double whenever the division isn't exact - which value ends up in
    // the row depends on n's value, not on n's declared type, so this can never be decided before execution
    // (review of #8285). A statement Describe pins the contract later Executes of it honor (#6725), so once this
    // is correctly left undecided (varchar), the value round-trips as text rather than a native binary type -
    // safe, since text always decodes correctly, unlike a wrong binary int8 would have.
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT n / 2 AS d FROM Items8285 WHERE n = 1")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isNotEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getDouble(1)).isEqualTo(0.5);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void sumOfTwoIntegerPropertiesThatOverflowsDescribesAsInt8NotInt4BeforeExecution() throws Exception {
    // MathExpression.Operator.PLUS.apply(Integer, Integer) silently widens to a Long on overflow (no exception,
    // unlike the Long,Long overload), so an INTEGER + INTEGER whose sum does not fit in int4 executes to a Long
    // regardless of the declared operand types - the same "depends on the row's values" problem SLASH has, so
    // describing this as int4 would make a binary-format client decode the real Long as a truncated int4
    // (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Items8285Overflow IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Items8285Overflow.a IF NOT EXISTS INTEGER");
        statement.execute("CREATE PROPERTY Items8285Overflow.b IF NOT EXISTS INTEGER");
        statement.execute("INSERT INTO Items8285Overflow SET a = " + Integer.MAX_VALUE + ", b = " + Integer.MAX_VALUE);
      }

      try (final PreparedStatement statement = connection.prepareStatement("SELECT a + b AS s FROM Items8285Overflow")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getLong(1)).isEqualTo(2L * Integer.MAX_VALUE);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void nullCoalescingIsNotDescribedAsTheOtherOperandsWiderType() throws Exception {
    // MathExpression.Operator.NULL_COALESCING (??) returns whichever operand is non-null UNCHANGED - never
    // widened to a common type - so `n ?? x` (n LONG, x DOUBLE) returns n's own Long when n is non-null, not a
    // Double. Describing it as the wider operand type (float8) would make binary encoding call doubleValue() on
    // that Long and risk losing precision (review of #8285, CodeRabbit).
    //
    // Sent over the raw wire protocol rather than through pgjdbc: a PreparedStatement's client-side parameter
    // parsing treats "??" as an escaped literal "?" and rewrites it away before the query ever reaches the
    // server ("mismatched input '?'"), so this shape cannot be reproduced through pgjdbc's own API.
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      socket.setSoTimeout(30_000); // a stalled response fails the test instead of blocking it indefinitely
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      sendSimpleQuery(out, "CREATE DOCUMENT TYPE Items8285NullCoalescing IF NOT EXISTS");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "CREATE PROPERTY Items8285NullCoalescing.n IF NOT EXISTS LONG");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "CREATE PROPERTY Items8285NullCoalescing.x IF NOT EXISTS DOUBLE");
      readUntilReadyForQuery(in);
      sendSimpleQuery(out, "INSERT INTO Items8285NullCoalescing SET n = 1, x = 2.5");
      readUntilReadyForQuery(in);

      sendParse(out, "s", "SELECT n ?? x AS c FROM Items8285NullCoalescing WHERE n = 1");
      sendDescribe(out, 'S', "s");
      sendSync(out);
      final List<WireMessage> described = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(described)).as("Parse and Describe both succeed").doesNotContain('E');

      final WireMessage rowDescription = described.stream().filter(m -> m.type() == 'T').findFirst()
          .orElseThrow(() -> new AssertionError("no RowDescription among " + messageTypesOf(described)));
      assertThat(firstColumnTypeOid(rowDescription))
          .as("must not commit to float8 (OID 701), the wider operand's type, before execution")
          .isNotEqualTo(701);

      sendBind(out, "p", "s");
      sendExecute(out, "p");
      sendSync(out);
      final List<WireMessage> executed = readUntilReadyForQuery(in);
      assertThat(firstDataRowValue(executed)).as("n is non-null, so the value is n's own Long, unchanged")
          .isEqualTo("1");
    }
  }

  /**
   * The data type OID of a RowDescription's first (and, for these tests, only) column.
   */
  private static int firstColumnTypeOid(final WireMessage rowDescription) {
    final ByteBuffer buffer = ByteBuffer.wrap(rowDescription.body());
    buffer.getShort(); // field count
    while (buffer.get() != 0) {
      // skip the null-terminated field name
    }
    buffer.getInt(); // table OID
    buffer.getShort(); // column attribute number
    return buffer.getInt(); // data type OID
  }

  // Regression: #8285
  @Test
  void sumOfALongPropertyThatOverflowsDescribesAsNumericAndKeepsTheValue() throws Exception {
    // a LONG sum past Long.MAX_VALUE is widened to a BigDecimal (#8974): an int8 column would wrap it
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Items8974Long IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Items8974Long.n IF NOT EXISTS LONG");
        for (int i = 0; i < 3; i++)
          statement.execute("INSERT INTO Items8974Long SET n = 4000000000000000000");
      }

      try (final PreparedStatement statement = connection.prepareStatement("SELECT sum(n) AS s FROM Items8974Long")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("numeric");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getBigDecimal(1)).isEqualByComparingTo("12000000000000000000");
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void sumOfALongPropertyWithoutOverflowDescribesAsNumericAndEncodesTheLong() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT sum(n) AS s FROM Items8285")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("numeric");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getBigDecimal(1)).isEqualByComparingTo("3");
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void sumOfADecimalPropertyDescribesAsNumericNotFloat8BeforeExecution() throws Exception {
    // SQLFunctionSum/Type#increment keep a DECIMAL accumulator as BigDecimal: describing it as float8 would make
    // binary encoding call doubleValue() and lose precision (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Items8285Decimal IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Items8285Decimal.amount IF NOT EXISTS DECIMAL");
        statement.execute("INSERT INTO Items8285Decimal SET amount = 1.1");
        statement.execute("INSERT INTO Items8285Decimal SET amount = 2.2");
      }

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT sum(amount) AS s FROM Items8285Decimal")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("numeric");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(BigDecimal.class);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void maxOfADecimalPropertyDescribesAsNumericBeforeExecution() throws Exception {
    // min()/max() pass the operand's own type through unchanged (unlike sum(), which widens): symmetry check for
    // the sum(DECIMAL) case above, since a NUMERIC-typed min()/max() had no coverage (review of #8285).
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement statement = connection.createStatement()) {
        statement.execute("CREATE DOCUMENT TYPE Items8285DecimalMinMax IF NOT EXISTS");
        statement.execute("CREATE PROPERTY Items8285DecimalMinMax.amount IF NOT EXISTS DECIMAL");
        statement.execute("INSERT INTO Items8285DecimalMinMax SET amount = 1.1");
        statement.execute("INSERT INTO Items8285DecimalMinMax SET amount = 2.2");
      }

      try (final PreparedStatement statement = connection.prepareStatement(
          "SELECT max(amount) AS m FROM Items8285DecimalMinMax")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("numeric");

        try (final ResultSet resultSet = statement.executeQuery()) {
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getObject(1)).isInstanceOf(BigDecimal.class);
        }
      }
    }
  }

  // Regression: #8285
  @Test
  void aPlainDeclaredPropertyIsUnaffected() throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      createAndPopulateTestType(connection);

      try (final PreparedStatement statement = connection.prepareStatement("SELECT n FROM Items8285 WHERE n = 1")) {
        assertThat(statement.getMetaData().getColumnTypeName(1)).isEqualTo("int8");
      }
    }
  }

  private void createAndPopulateTestType(final Connection connection) throws Exception {
    try (final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DOCUMENT TYPE Items8285 IF NOT EXISTS");
      statement.execute("CREATE PROPERTY Items8285.n IF NOT EXISTS LONG");
      statement.execute("CREATE PROPERTY Items8285.x IF NOT EXISTS DOUBLE");
      for (int i = 0; i < 3; i++)
        statement.execute("INSERT INTO Items8285 SET n = " + i + ", x = " + (i * 0.5));
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), properties);
  }

  /*
   * Regression tests for two Postgres-wire issues:
   * <ul>
   *   <li>#8391: {@code SHOW TIME ZONE}, the read-back of {@code SET TIME ZONE}, answered an empty string while
   *   {@code SHOW timezone} answered the value, and {@code RESET TIME ZONE} was refused as malformed (its test is in
   *   {@code PostgresSessionSettingsIT});</li>
   *   <li>#8379: a prepared statement whose {@code Describe('S')} answered {@code NoData} still sent DataRows from
   *   Execute, with no RowDescription anywhere in the exchange.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #8379
  @Test
  @DisplayName("[#8379] an INSERT whose Describe('S') answered NoData is answered with CommandComplete only, as PostgreSQL does")
  void insertDescribedAsNoDataSendsNoDataRow() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE T8379 IF NOT EXISTS");

        // The shape asyncpg, npgsql and pgx use: the statement is described once, then only bound and executed.
        sendParse(out, "S1", "INSERT INTO T8379 SET name = 'a'");
        sendDescribe(out, 'S', "S1");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', 'Z');

        for (int i = 0; i < 2; i++) {
          sendBind(out, "", "S1");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> execute = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(execute)).as("NoData promised no result set: no DataRow").containsExactly('2', 'C', 'Z');
          assertThat(commandTagOf(execute)).isEqualTo("INSERT 0 1");
        }

        // The same exchange in one round trip, as the issue reported it.
        sendParse(out, "S2", "INSERT INTO T8379 SET name = 'b'");
        sendDescribe(out, 'S', "S2");
        sendBind(out, "P2", "S2");
        sendExecute(out, "P2");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', '2', 'C', 'Z');

        // The writes happened: the rows were only withheld, not the INSERT.
        assertThat(firstDataRowValue(simple(out, in, "SELECT count(*) AS c FROM T8379"))).isEqualTo("3");
      });
    }
  }

  // Regression: #8379
  @Test
  @DisplayName("[#8379] a portal bound BEFORE its statement was described with NoData is held to that NoData too")
  void portalBoundBeforeStatementDescribeKeepsTheNoDataPromise() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE B8379 IF NOT EXISTS");

        sendParse(out, "S1", "INSERT INTO B8379 SET name = 'a'");
        sendBind(out, "P1", "S1");
        sendDescribe(out, 'S', "S1");
        sendExecute(out, "P1");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('1', '2', 't', 'n', 'C', 'Z');
        assertThat(commandTagOf(response)).isEqualTo("INSERT 0 1");
      });
    }
  }

  // Regression: #8379
  @Test
  @DisplayName("[#8379] an UPDATE described as NoData is tagged with the records it changed, zero included")
  void updateDescribedAsNoDataIsTaggedWithTheAffectedCount() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE U8379 IF NOT EXISTS");
        simple(out, in, "INSERT INTO U8379 SET name = 'a'");
        simple(out, in, "INSERT INTO U8379 SET name = 'a'");

        for (final String[] c : new String[][] { { "UPDATE U8379 SET name = 'b' WHERE name = 'a'", "UPDATE 2" },
            { "UPDATE U8379 SET name = 'c' WHERE name = 'none'", "UPDATE 0" }, { "DELETE FROM U8379 WHERE name = 'b'", "DELETE 2" } }) {
          sendParse(out, "", c[0]);
          sendDescribe(out, 'S', "");
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> response = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(response)).as(c[0]).containsExactly('1', 't', 'n', '2', 'C', 'Z');
          assertThat(commandTagOf(response)).as(c[0]).isEqualTo(c[1]);
        }
      });
    }
  }

  // Regression: #8379
  @Test
  @DisplayName("[#8379] a DELETE is not described by the type its FROM names, and answers no DataRow")
  void deleteIsDescribedAsNoData() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE D8379 IF NOT EXISTS");
        simple(out, in, "INSERT INTO D8379 SET name = 'a'");

        sendParse(out, "S1", "DELETE FROM D8379 WHERE name = 'a'");
        sendDescribe(out, 'S', "S1");
        sendBind(out, "", "S1");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('1', 't', 'n', '2', 'C', 'Z');
        assertThat(commandTagOf(response)).startsWith("DELETE ");

        assertThat(firstDataRowValue(simple(out, in, "SELECT count(*) AS c FROM D8379"))).isEqualTo("0");
      });
    }
  }

  // Regression: #8379
  @Test
  @DisplayName("[#8379] a statement whose rows its Describe('S') could not name is refused, not answered with unannounced rows")
  void rowReturningStatementDescribedAsNoDataIsRefused() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "C1", "{cypher} UNWIND [1, 2] AS x RETURN x");
        sendDescribe(out, 'S', "C1");
        sendBind(out, "", "C1");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("no DataRow without a RowDescription").doesNotContain('D').contains('n', 'E');
        assertThat(sqlStateOf(refused)).isEqualTo("0A000");

        // The session is healthy afterwards, and describing the PORTAL - what pgjdbc and libpq do - names the columns.
        sendBind(out, "", "C1");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'T', 'D', 'D', 'C', 'Z');

        // A statement that returns no rows keeps the NoData promise without any error.
        sendParse(out, "C2", "{cypher} UNWIND [] AS x RETURN x");
        sendDescribe(out, 'S', "C2");
        sendBind(out, "", "C2");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', '2', 'C', 'Z');
      });
    }
  }

  // Regression: #8379
  @Test
  @DisplayName("[#8379] control: a SELECT Describe('S') can name keeps its RowDescription and DataRows")
  void resolvableSelectIsUnchanged() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE Q8379 IF NOT EXISTS");
        simple(out, in, "CREATE PROPERTY Q8379.name IF NOT EXISTS STRING");
        simple(out, in, "INSERT INTO Q8379 SET name = 'a'");

        sendParse(out, "S3", "SELECT name FROM Q8379");
        sendDescribe(out, 'S', "S3");
        sendBind(out, "P3", "S3");
        sendExecute(out, "P3");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'T', '2', 'D', 'C', 'Z');
      });
    }
  }

  private List<WireMessage> simple(final DataOutputStream out, final DataInputStream in, final String query) throws Exception {
    sendSimpleQuery(out, query);
    return readUntilReadyForQuery(in);
  }

  private static String commandTagOf(final List<WireMessage> messages) {
    for (final WireMessage message : messages)
      if (message.type() == 'C')
        return new String(message.body(), 0, message.body().length - 1, StandardCharsets.UTF_8);
    throw new AssertionError("no CommandComplete among " + messageTypesOf(messages));
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }

  /*
   * A {@code Describe('S')} must name the columns of a statement that returns rows whenever they can be named, instead of
   * promising {@code NoData} and then having the Execute refused with {@code 0A000} (issue #8562): a write with a
   * {@code RETURN} clause, and a catalog query whose filters are bound parameters.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #8562
  @Test
  @DisplayName("[#8562] INSERT ... RETURN and UPDATE ... RETURN AFTER are described with a RowDescription and executed with their rows")
  void writeWithReturnIsDescribedAndExecuted() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        simple(out, in, "CREATE DOCUMENT TYPE Article8562 IF NOT EXISTS");
        simple(out, in, "CREATE PROPERTY Article8562.id IF NOT EXISTS INTEGER");

        final String[][] statements = {
            { "S1", "INSERT INTO Article8562 SET id = 1 RETURN @rid" },
            { "S2", "UPDATE Article8562 SET id = 9 RETURN AFTER" },
            { "S3", "INSERT INTO Article8562 SET id = 3 RETURN id" },
            { "S4", "UPDATE Article8562 SET id = 9 RETURN AFTER @rid" },
            { "S5", "DELETE FROM Article8562 RETURN BEFORE WHERE id >= 0" },
            // a sub-query yields a list of rows: [0].m takes the scalar an INTEGER property can hold, which a bare sub-query
            // is not (it used to be stored as null without an error, refused since #9110)
            { "S6", "INSERT INTO Article8562 SET id = (SELECT max(id) AS m FROM Article8562)[0].m RETURN id" } };

        for (final String[] statement : statements) {
          sendParse(out, statement[0], statement[1]);
          sendDescribe(out, 'S', statement[0]);
          sendSync(out);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as(statement[1]).containsExactly('1', 't', 'T', 'Z');

          sendBind(out, "", statement[0]);
          sendExecute(out, "");
          sendSync(out);
          final List<WireMessage> execute = readUntilReadyForQuery(in);
          assertThat(messageTypesOf(execute)).as(statement[1]).contains('D').doesNotContain('E');
        }
      });
    }
  }

  // Regression: #8562
  @Test
  @DisplayName("[#8562] a pg_type lookup with a bound parameter is described with its columns and executed")
  void catalogQueryWithBoundParameterIsDescribedAndExecuted() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "C1", "SELECT typname AS name, oid, typarray AS array_oid, oid::regtype::text AS regtype, typdelim AS delimiter "
            + "FROM pg_type t WHERE t.oid = to_regtype($1) ORDER BY t.oid");
        sendDescribe(out, 'S', "C1");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'T', 'Z');

        sendBind(out, "", "C1", "int4");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> execute = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(execute)).contains('D').doesNotContain('E');
      });
    }
  }
}
