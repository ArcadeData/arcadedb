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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6458: the extended-query protocol's Execute with a non-zero max-row count
 * (portal suspension) sent {@code PortalSuspended} before the data rows instead of after, sent it together
 * with {@code CommandComplete} (the protocol allows exactly one terminator), and removed the portal on the
 * very Execute that suspended it - so a follow-up Execute meant to continue the fetch found nothing and the
 * result was silently truncated to the first batch.
 * <p>
 * The pgjdbc driver drives exactly this path when {@code autoCommit(false)} and {@code setFetchSize(n)} are
 * both in play: it opens a server-side cursor (a portal bound once) and issues repeated Execute messages with
 * a max-row count of {@code n} on that same portal as the application consumes the {@link ResultSet}, relying
 * on {@code PortalSuspended} vs {@code CommandComplete} to know whether to ask for more. pgjdbc always sends a
 * Describe('P') before its first Execute, though, which - because {@code describeCommand()} already has to run
 * the statement in full to discover the portal's columns - means those tests exercise the fix's pagination
 * behaviour rather than the exact wire defects the issue reports; {@link
 * #executeMaxRowsSendsRowsBeforeSuspendedNeverBothTerminatorsAndThePortalSurvivesToContinue} crafts the wire
 * messages directly (no Describe) to reach the code path the report describes and reproduces all three
 * defects verbatim - confirmed by running it against the pre-fix code, where it fails with
 * {@code expected: 'D' but was: 's'} on the very first row.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6458PortalSuspensionIT extends PostgresWireProtocolTestBase {

  /**
   * Reproduces the exact defects the issue reports, at the wire level, on a client that never sends a
   * Describe('P') for the portal - unlike pgjdbc, which always does and so never actually reaches the buggy
   * code path the other tests in this class exercise only indirectly (through its end result, not the wire
   * sequence itself). Crafts Parse/Bind/Execute/Execute directly, the shape the issue's own report describes.
   */
  @Test
  void executeMaxRowsSendsRowsBeforeSuspendedNeverBothTerminatorsAndThePortalSurvivesToContinue() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      // Schema and data setup over the simple query protocol - irrelevant to what this test is checking.
      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE RawSuspend6458 IF NOT EXISTS");
      runSimpleQueryToCompletion(out, in, "CREATE PROPERTY RawSuspend6458.id IF NOT EXISTS INTEGER");
      for (int i = 0; i < 10; i++)
        runSimpleQueryToCompletion(out, in, "INSERT INTO RawSuspend6458 SET id = " + i);

      // Parse + Bind only - deliberately NO Describe('P'), so the portal reaches Execute with
      // portal.executed == false and this exercises PostgresNetworkExecutor.executeCommand()'s own
      // first-execution branch directly, exactly as issue #6458 describes.
      sendParse(out, "SELECT id FROM RawSuspend6458 ORDER BY id");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');
      sendBind(out, "P1");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');

      // First Execute: limit=4 with 10 rows available - must suspend with 4 rows left unread. No Describe
      // means the client has not seen the columns yet, so this Execute must lead with its own RowDescription
      // before any data - PostgresNetworkExecutor.executeCommand()'s own !rowDescriptionSent branch.
      sendExecute(out, "P1", 4);
      sendSync(out);
      assertThat(readOneMessage(in).type).as("RowDescription, since no Describe('P') preceded this Execute").isEqualTo('T');
      assertThat(readNextBatchOfIds(in, 4))
          .as("first batch: DataRow x4 in id order, then exactly one terminator (checked by readNextBatchOfIds)")
          .containsExactly(0, 1, 2, 3);
      assertThat(readOneMessage(in).type).as("PortalSuspended, not CommandComplete - 6 rows remain unread").isEqualTo('s');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // Second Execute on the SAME portal name, with no Bind or Describe in between: the defect under test
      // removed the portal on the first Execute, so this would get NoData ('n') instead of the next batch.
      sendExecute(out, "P1", 4);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 4))
          .as("second batch continues exactly where the first stopped - the portal was not removed nor re-run")
          .containsExactly(4, 5, 6, 7);
      assertThat(readOneMessage(in).type).as("PortalSuspended again - 2 rows remain unread").isEqualTo('s');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // Third Execute drains the remaining 2 rows: this time exhausted, so CommandComplete - not
      // PortalSuspended, and not both.
      sendExecute(out, "P1", 4);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 2))
          .as("final batch: the last 2 rows, in order")
          .containsExactly(8, 9);
      assertThat(readOneMessage(in).type).as("CommandComplete - the portal is now fully drained").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');
    }
  }

  /**
   * The counterpart to {@link #executeMaxRowsSendsRowsBeforeSuspendedNeverBothTerminatorsAndThePortalSurvivesToContinue}:
   * this client DOES send a Describe('P') before its first Execute, like pgjdbc always does - so
   * {@code describeCommand()} is the one that materializes {@code fullResultSet} and sends the RowDescription,
   * and {@code executeCommand()} reaches its already-executed branch straight away. The pgjdbc-driven tests in
   * this class exercise this path indirectly through a JDBC {@link ResultSet}, which cannot observe the wire
   * sequence itself; this test asserts directly, at the wire level, that a small Execute limit after Describe('P')
   * still returns only that many rows with exactly one terminator - the more consequential of the two defects
   * #6458 reports, since {@code describeCommand()} used to run before the row-limit fix and read the query's
   * result in full regardless of what the first Execute would go on to ask for.
   */
  @Test
  void describePortalThenExecuteWithASmallLimitReturnsOnlyThatManyRowsThenSuspends() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", GlobalConfiguration.POSTGRES_PORT.getValueAsInteger()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      // Schema and data setup over the simple query protocol - irrelevant to what this test is checking.
      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE DescribeThenSuspend6458 IF NOT EXISTS");
      runSimpleQueryToCompletion(out, in, "CREATE PROPERTY DescribeThenSuspend6458.id IF NOT EXISTS INTEGER");
      for (int i = 0; i < 10; i++)
        runSimpleQueryToCompletion(out, in, "INSERT INTO DescribeThenSuspend6458 SET id = " + i);

      sendParse(out, "SELECT id FROM DescribeThenSuspend6458 ORDER BY id");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');
      sendBind(out, "P2");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');

      // Describe('P') before any Execute: this is describeCommand()'s branch, not executeCommand()'s - it runs
      // the statement, materializes portal.fullResultSet in full, and sends the RowDescription on its own.
      sendDescribe(out, "P2");
      sendSync(out);
      assertThat(readOneMessage(in).type).as("RowDescription, sent by describeCommand()").isEqualTo('T');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // First Execute: limit=4 with 10 rows available and portal.executed already true from Describe - this is
      // executeCommand()'s already-executed branch, which must still slice fullResultSet down to just 4 rows
      // rather than returning everything Describe had to read.
      sendExecute(out, "P2", 4);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 4))
          .as("first batch after Describe('P'): exactly 4 rows, not the full 10 describeCommand() read")
          .containsExactly(0, 1, 2, 3);
      assertThat(readOneMessage(in).type).as("PortalSuspended, not CommandComplete - 6 rows remain unread").isEqualTo('s');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // Second Execute on the same portal, no further Bind/Describe: continues exactly where the first left off.
      sendExecute(out, "P2", 4);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 4))
          .as("second batch continues from row 4")
          .containsExactly(4, 5, 6, 7);
      assertThat(readOneMessage(in).type).as("PortalSuspended again - 2 rows remain unread").isEqualTo('s');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // Third Execute drains the remaining 2 rows.
      sendExecute(out, "P2", 4);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 2))
          .as("final batch: the last 2 rows, in order")
          .containsExactly(8, 9);
      assertThat(readOneMessage(in).type).as("CommandComplete - the portal is now fully drained").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');
    }
  }

  @Test
  void aFetchSizeSmallerThanTheResultReturnsEveryRowAcrossSeveralSuspendedBatches() throws Exception {
    // 23 rows with a fetch size of 7 batches as 7, 7, 7, 2 - the last batch stops short of the limit, so it
    // exercises both the "limit hit, rows remain" slice (the first three batches) and the "naturally
    // exhausted before the limit" one (the last) in the pagination step of executeCommand().
    assertFetchSizeReturnsEveryRow(23, 7);
  }

  @Test
  void aFetchSizeThatDividesTheResultExactlyStillReturnsEveryRow() throws Exception {
    // 20 rows with a fetch size of 5 - every batch, including the last, lands exactly on the limit. This is
    // the boundary the pagination step has to get right: the final slice must still be reported as exhausted
    // (CommandComplete), not suspended, even though its size equals the limit - end == total, not end < total.
    assertFetchSizeReturnsEveryRow(20, 5);
  }

  private void assertFetchSizeReturnsEveryRow(final int totalRows, final int fetchSize) throws Exception {
    try (final Connection connection = openJdbcConnection()) {
      // Schema and data setup runs with the default autoCommit(true), so each statement commits itself.
      try (final Statement create = connection.createStatement()) {
        create.execute("CREATE DOCUMENT TYPE Suspend6458 IF NOT EXISTS");
        create.execute("CREATE PROPERTY Suspend6458.id IF NOT EXISTS INTEGER");
        create.execute("CREATE INDEX IF NOT EXISTS ON Suspend6458 (id) UNIQUE");
      }

      // A plain Statement per row (simple query protocol, not Parse/Bind/Execute) - deliberately not a single
      // reused PreparedStatement here: pgjdbc promotes a repeatedly-executed PreparedStatement to a named
      // server-side statement after its default prepareThreshold (5) executions, which would exercise the
      // portal-reuse-on-rebind path rather than the portal-suspension one this test targets.
      try (final Statement insert = connection.createStatement()) {
        for (int i = 0; i < totalRows; i++)
          insert.execute("INSERT INTO Suspend6458 SET id = " + i);
      }

      // A server-side cursor requires autoCommit off, matching the JDBC fetch-size-with-Execute-max-rows path
      // issue #6458 describes.
      connection.setAutoCommit(false);
      // pgjdbc piggybacks its own implicit BEGIN on the very first statement after autoCommit turns off. When
      // that first statement is a PreparedStatement, the BEGIN (simple query protocol) and the Parse/Bind/
      // Describe/Execute (extended protocol) go out in one pipelined flush - and a plain Statement here first
      // gives pgjdbc's implicit BEGIN its own round trip, keeping the SELECT below the only thing on the wire
      // when Execute runs. Unrelated to this issue: pgjdbc reads it back fine either way once the BEGIN gets
      // its own trip, and CI has no coverage either way of the mixed-flush case's own wire correctness.
      try (final Statement warmUp = connection.createStatement()) {
        warmUp.execute("begin");
      }
      final List<Integer> fetchedIds = new ArrayList<>();
      try (final PreparedStatement select = connection.prepareStatement("SELECT id FROM Suspend6458 ORDER BY id")) {
        select.setFetchSize(fetchSize);
        try (final ResultSet resultSet = select.executeQuery()) {
          while (resultSet.next())
            fetchedIds.add(resultSet.getInt("id"));
        }
      }
      connection.commit();
      connection.setAutoCommit(true);

      assertThat(fetchedIds)
          .as("every row must come back exactly once, in order, across every suspended batch - not just the first")
          .hasSize(totalRows)
          .containsExactlyElementsOf(java.util.stream.IntStream.range(0, totalRows).boxed().toList());
    }
  }

  @Test
  void aPortalThatSuspendsCanStillBeClosedExplicitlyWithoutBreakingTheConnection() throws Exception {
    // Regression guard for the portal-removal half of the fix (issue #6458's third defect): a portal
    // suspended mid-fetch (more rows available than were consumed) must be closeable - the client's normal
    // try-with-resources teardown of its ResultSet/PreparedStatement sends a Close ('C') message for the
    // portal - without the connection or a later statement on it hanging or erroring.
    try (final Connection connection = openJdbcConnection()) {
      try (final Statement create = connection.createStatement()) {
        create.execute("CREATE DOCUMENT TYPE Suspend6458b IF NOT EXISTS");
        create.execute("CREATE PROPERTY Suspend6458b.id IF NOT EXISTS INTEGER");
      }

      try (final Statement insert = connection.createStatement()) {
        for (int i = 0; i < 10; i++)
          insert.execute("INSERT INTO Suspend6458b SET id = " + i);
      }

      connection.setAutoCommit(false);
      // See the comment on the same pattern in assertFetchSizeReturnsEveryRow: keeps pgjdbc's implicit BEGIN
      // off the same pipelined flush as the PreparedStatement below, which is unrelated to this issue.
      try (final Statement warmUp = connection.createStatement()) {
        warmUp.execute("begin");
      }
      try (final PreparedStatement select = connection.prepareStatement("SELECT id FROM Suspend6458b ORDER BY id")) {
        select.setFetchSize(3);
        try (final ResultSet resultSet = select.executeQuery()) {
          // Consume only the first batch, leaving the portal suspended with rows still unread - the
          // try-with-resources close() below sends a Close ('C') message for the portal while it is in that
          // state.
          assertThat(resultSet.next()).isTrue();
          assertThat(resultSet.getInt("id")).isZero();
        }
      }
      connection.commit();
      connection.setAutoCommit(true);

      // The connection must still be usable afterwards - proof that closing a suspended portal did not wedge
      // the session or leak the ResultSet in a way that poisons subsequent statements.
      try (final Statement count = connection.createStatement();
          final ResultSet countResult = count.executeQuery("SELECT count() as total FROM Suspend6458b")) {
        assertThat(countResult.next()).isTrue();
        assertThat(countResult.getLong("total")).isEqualTo(10L);
      }
    }
  }

  private Connection openJdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    properties.setProperty("ssl", "false");
    properties.setProperty("sslMode", "disable");
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + getDatabaseName(), properties);
  }

  // ---- raw wire-protocol helpers for executeMaxRowsSendsRowsBeforeSuspendedNeverBothTerminatorsAndThePortalSurvivesToContinue ----

  private record Msg(char type, byte[] payload) {
  }

  private static Msg readOneMessage(final DataInputStream in) throws Exception {
    final int type = in.readUnsignedByte();
    final int length = in.readInt();
    final byte[] payload = new byte[length - 4];
    in.readFully(payload);
    return new Msg((char) type, payload);
  }

  /**
   * Reads exactly {@code expectedCount} DataRow messages and decodes each one's single INTEGER column
   * (text format - Bind below requests no result-format override), failing immediately, with the offending
   * message's type, on anything else. The terminator that follows (PortalSuspended or CommandComplete) is
   * read separately by the caller, so its type is asserted right next to the expectation that explains it.
   */
  private static List<Integer> readNextBatchOfIds(final DataInputStream in, final int expectedCount) throws Exception {
    final List<Integer> ids = new ArrayList<>(expectedCount);
    for (int i = 0; i < expectedCount; i++) {
      final Msg m = readOneMessage(in);
      assertThat(m.type).as("row %d of this batch must be a DataRow ('D'), sent before any terminator", i).isEqualTo('D');
      ids.add(parseSingleIntColumnDataRow(m.payload));
    }
    return ids;
  }

  private static int parseSingleIntColumnDataRow(final byte[] payload) throws Exception {
    final DataInputStream p = new DataInputStream(new ByteArrayInputStream(payload));
    final int fieldCount = p.readUnsignedShort();
    assertThat(fieldCount).as("this query projects exactly one column").isEqualTo(1);
    final int len = p.readInt();
    final byte[] valueBytes = new byte[len];
    p.readFully(valueBytes);
    return Integer.parseInt(new String(valueBytes, StandardCharsets.UTF_8));
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

  /**
   * Binds the unnamed statement to the given named portal, with no parameters and no result-format override
   * (every column comes back as text) - deliberately sends no Describe('P') afterward, so the portal reaches
   * Execute exactly as issue #6458's report describes.
   */
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

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('B');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  /**
   * Sends Describe('P') for the given portal name, the message pgjdbc always sends before its first Execute.
   */
  private static void sendDescribe(final DataOutputStream out, final String portalName) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    body.write('P');
    writeCString(body, portalName);

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('D');
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
