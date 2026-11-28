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
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

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
 * The extended-query protocol's prepared statements and portals over the PostgreSQL wire protocol: what a Parse, a Bind,
 * an Execute with a row limit, a Describe and a Close create, suspend, replace and drop, what is left (and must not be
 * left) after one of them fails, an empty Parse, the parameter count a Bind is checked against, and a Bind whose
 * parameters are malformed or oversized.
 */
class PostgresPortalIT extends PostgresWireProtocolTestBase {

  /*
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
  // Regression: #6458
  /**
   * Reproduces the exact defects the issue reports, at the wire level, on a client that never sends a
   * Describe('P') for the portal - unlike pgjdbc, which always does and so never actually reaches the buggy
   * code path the other tests in this class exercise only indirectly (through its end result, not the wire
   * sequence itself). Crafts Parse/Bind/Execute/Execute directly, the shape the issue's own report describes.
   */
  @Test
  void executeMaxRowsSendsRowsBeforeSuspendedNeverBothTerminatorsAndThePortalSurvivesToContinue() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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
      // Resuming a suspended portal across Syncs needs an explicit block, as in PostgreSQL: a portal is dropped with
      // the transaction it was bound in, and in autocommit that ends at the first Sync (issue #8212). pgjdbc only
      // opens a fetch-size cursor with autoCommit(false) for exactly this reason.
      runSimpleQueryToCompletion(out, in, "BEGIN");
      sendParse(out, "", "SELECT id FROM RawSuspend6458 ORDER BY id");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');
      sendBind(out, "P1", "");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');

      // First Execute: limit=4 with 10 rows available - must suspend with 4 rows left unread. No Describe was
      // sent, and Execute never answers with a RowDescription (issue #8244): the data rows come first.
      sendExecute(out, "P1", 4);
      sendSync(out);
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

  // Regression: #6458
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
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
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

      // Resuming a suspended portal across Syncs needs an explicit block, as in PostgreSQL: a portal is dropped with
      // the transaction it was bound in, and in autocommit that ends at the first Sync (issue #8212). pgjdbc only
      // opens a fetch-size cursor with autoCommit(false) for exactly this reason.
      runSimpleQueryToCompletion(out, in, "BEGIN");
      sendParse(out, "", "SELECT id FROM DescribeThenSuspend6458 ORDER BY id");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');
      sendBind(out, "P2", "");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');

      // Describe('P') before any Execute: this is describeCommand()'s branch, not executeCommand()'s - it runs
      // the statement, materializes portal.fullResultSet in full, and sends the RowDescription on its own.
      sendDescribe(out, 'P', "P2");
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

  // Regression: #6458
  /**
   * Regression test for issue #6660, surfaced by #6458 itself: keeping a portal alive after it completes
   * (rather than removing it, which #6458 needed for suspension to work) means a second Bind on the same
   * portal name with no new Parse in between - what asyncpg's statement cache and pgjdbc's server-side
   * prepared-statement promotion both do for a repeated identical query - used to reuse the exact same {@link
   * PostgresPortal} instance from the first run. Before the fix, that instance's {@code executed}/{@code
   * fullResultSet}/{@code resultCursor} from the first, already-exhausted run survived into the second Bind,
   * so the second Execute served an empty slice of the old result instead of re-running the query - confirmed
   * failing pre-fix with the second run's Execute jumping straight to an empty CommandComplete instead of the
   * RowDescription + 5 rows the first run got. Fixed via {@link PostgresPortal#bindFrom}: every Bind now
   * creates its own fresh portal from the (never mutated) prepared-statement template, so the second run
   * starts as clean as the first - including its own RowDescription, since a brand new portal has never sent
   * one before.
   */
  @Test
  void reBindingAnAlreadyExecutedPortalWithNoNewParseReRunsInsteadOfServingTheExhaustedFirstRun() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE Rebind6458 IF NOT EXISTS");
      runSimpleQueryToCompletion(out, in, "CREATE PROPERTY Rebind6458.id IF NOT EXISTS INTEGER");
      for (int i = 0; i < 5; i++)
        runSimpleQueryToCompletion(out, in, "INSERT INTO Rebind6458 SET id = " + i);

      // Parse once. A max-rows of 0 (unlimited) drains the whole portal in a single Execute, so this run
      // completes with CommandComplete rather than suspending - the state the second Bind below reuses.
      sendParse(out, "", "SELECT id FROM Rebind6458 ORDER BY id");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');

      sendBind(out, "PR1", "");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');
      sendExecute(out, "PR1", 0);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 5)).as("first run reads every row").containsExactly(0, 1, 2, 3, 4);
      assertThat(readOneMessage(in).type).as("CommandComplete - fully drained").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // Second Bind on the SAME portal name, no new Parse: must re-run the query, not replay the first run's
      // now-exhausted fullResultSet/resultCursor.
      sendBind(out, "PR1", "");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');
      sendExecute(out, "PR1", 0);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 5))
          .as("second run re-executes and reads every row again, not an empty slice of the exhausted first run")
          .containsExactly(0, 1, 2, 3, 4);
      assertThat(readOneMessage(in).type).as("CommandComplete - fully drained").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');
    }
  }

  // Regression: #6458
  /**
   * Regression test for a review finding on PR #6658 (2026-08-24 cycle): {@code bindCommand()} used to have a
   * "backwards-compatible fallback" - reached when a Bind's own {@code sourcePreparedStatement} name is not a
   * real, Parsed prepared statement - that fell back to treating {@code portalName} itself as the template
   * ({@code portals.get(portalName)}), which could pick up an already-executed, real portal as that template
   * and serve its stale result instead of re-executing.
   * <p>
   * That fallback no longer exists at all as of #6698/#6707 (d38315e267): an unresolvable
   * {@code sourcePreparedStatement} now binds a throwaway, do-nothing portal (draining Bind's wire bytes
   * without executing anything) and, per {@code bindCommand()}'s own comment, "without resurrecting any
   * previously bound portal" - the old portal under {@code portalName}, if any, is explicitly removed rather
   * than reused as a fallback template. This is a strictly stronger fix than the reset this test originally
   * pinned: there is no stale state to serve because nothing is ever resurrected. Confirmed failing (with
   * {@code 'T'} instead of {@code 'n'}) against the pre-#6707 code, where the fallback still exists and this
   * Bind would resurrect and re-run the old portal instead of finding it gone.
   */
  @Test
  void bindOnAnUnknownSourceStatementClearsAnyExistingPortalInsteadOfServingItsStaleResult() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE FallbackBind6458 IF NOT EXISTS");
      runSimpleQueryToCompletion(out, in, "CREATE PROPERTY FallbackBind6458.id IF NOT EXISTS INTEGER");
      for (int i = 0; i < 5; i++)
        runSimpleQueryToCompletion(out, in, "INSERT INTO FallbackBind6458 SET id = " + i);

      sendParse(out, "", "SELECT id FROM FallbackBind6458 ORDER BY id");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');

      // First Bind+Execute: a normal Bind naming the real (unnamed) statement, fully drained.
      sendBind(out, "PF1", "");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');
      sendExecute(out, "PF1", 0);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 5)).as("first run reads every row").containsExactly(0, 1, 2, 3, 4);
      assertThat(readOneMessage(in).type).as("CommandComplete - fully drained").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // Data changes between the two Binds, so a resurrected-and-stale vs. correctly-cleared portal is observable.
      runSimpleQueryToCompletion(out, in, "INSERT INTO FallbackBind6458 SET id = 5");
      runSimpleQueryToCompletion(out, in, "INSERT INTO FallbackBind6458 SET id = 6");
      runSimpleQueryToCompletion(out, in, "INSERT INTO FallbackBind6458 SET id = 7");

      // Second Bind on the SAME portal name, naming a source prepared statement that was never Parsed -
      // bindCommand()'s preparedStatements.get(...) lookup misses, so it binds a throwaway portal and removes
      // the already-executed PF1 portal from above instead of resurrecting it.
      sendBind(out, "PF1", "never-parsed-statement-name");
      sendExecute(out, "PF1", 0);
      sendSync(out);
      assertMissingStatementThenPortal(out, in, "PF1");
    }
  }

  // Regression: #6458
  /**
   * Regression test for a review finding on PR #6658 (2026-08-24, cycle 5): the same fallback as {@link
   * #bindOnAnUnknownSourceStatementClearsAnyExistingPortalInsteadOfServingItsStaleResult}, but for a DEFERRED
   * catalog query ({@code portal.catalogQuery = true}, set by {@code parseCommand()} when the query names an
   * emulated catalog relation - {@code pg_class} here - but its filter is a bound parameter whose value only
   * arrives with Bind) rather than plain SQL.
   * <p>
   * Like the plain-SQL case, the fallback this originally guarded no longer exists as of #6698/#6707
   * (d38315e267): an unresolvable {@code sourcePreparedStatement} removes the old portal outright instead of
   * resurrecting it as a template, so there is nothing left that could serve a stale catalog answer. Confirmed
   * failing (with {@code 'T'} instead of {@code 'n'}) against the pre-#6707 code.
   */
  @Test
  void bindOnAnUnknownSourceStatementClearsAnExistingDeferredCatalogPortalToo() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE CatalogFB6458First IF NOT EXISTS");

      // $1 is detected from the query text alone (parseCommand()'s placeholder-detection fallback, since this
      // Parse declares paramCount=0 like every real client using the simple wire helpers here) and typed
      // VARCHAR, which is what makes parseCommand() defer this to portal.catalogQuery = true instead of
      // resolving it immediately: a query naming an emulated catalog relation (pg_class) whose filter is a
      // bound parameter, mirroring the shape pgjdbc's DatabaseMetaData table/column lookups actually send.
      sendParse(out, "", "SELECT relname FROM pg_class WHERE relname LIKE $1");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');

      // First Bind+Execute: a normal Bind naming the real (unnamed) statement, filtered to just the one type
      // created so far.
      sendBind(out, "PCQ1", "", "CatalogFB6458%");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');
      sendExecute(out, "PCQ1", 0);
      sendSync(out);
      assertThat(readNextBatchOfStrings(in, 1)).as("first run matches only the one type created so far")
          .containsExactly("CatalogFB6458First");
      assertThat(readOneMessage(in).type).as("CommandComplete - fully drained").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // A second matching type appears between the two Binds, so a resurrected-and-stale vs. correctly-cleared
      // catalog portal is observable.
      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE CatalogFB6458Second IF NOT EXISTS");

      // Second Bind on the SAME portal name, naming a source prepared statement that was never Parsed -
      // bindCommand()'s preparedStatements.get(...) lookup misses, so it binds a throwaway portal and removes
      // the already-executed PCQ1 catalog-query portal from above instead of resurrecting it.
      sendBind(out, "PCQ1", "never-parsed-statement-name", "CatalogFB6458%");
      sendExecute(out, "PCQ1", 0);
      sendSync(out);
      assertMissingStatementThenPortal(out, in, "PCQ1");
    }
  }

  // Regression: #6458
  /**
   * Regression test for the CodeRabbit review finding on PR #6658: {@code bindCommand()} used to look up the
   * portal to bind by the PREPARED STATEMENT's name and store that exact object under the new portal name too
   * - so two portal names bound from the same statement were not independent portals at all, just two names
   * for the same mutable object. Binding a second portal ({@code P2}) from a statement that already had a
   * SUSPENDED portal ({@code P1}, mid-fetch) would silently reset/overwrite {@code P1}'s progress, and a
   * later {@code Execute(P1)} would resume (or lose) {@code P2}'s state instead of its own. Fixed via {@link
   * PostgresPortal#bindFrom}: every Bind creates its own fresh portal from the statement's read-only template,
   * so {@code P1} and {@code P2} never share state no matter how many times the same statement is re-bound.
   */
  @Test
  void twoPortalsFromOneStatementDoNotShareStateEvenWhenOneIsSuspended() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      runSimpleQueryToCompletion(out, in, "CREATE DOCUMENT TYPE AliasedPortals6458 IF NOT EXISTS");
      runSimpleQueryToCompletion(out, in, "CREATE PROPERTY AliasedPortals6458.id IF NOT EXISTS INTEGER");
      for (int i = 0; i < 10; i++)
        runSimpleQueryToCompletion(out, in, "INSERT INTO AliasedPortals6458 SET id = " + i);

      // Resuming a suspended portal across Syncs needs an explicit block, as in PostgreSQL: a portal is dropped with
      // the transaction it was bound in, and in autocommit that ends at the first Sync (issue #8212). pgjdbc only
      // opens a fetch-size cursor with autoCommit(false) for exactly this reason.
      runSimpleQueryToCompletion(out, in, "BEGIN");
      sendParse(out, "", "SELECT id FROM AliasedPortals6458 ORDER BY id");
      assertThat(readOneMessage(in).type).as("ParseComplete").isEqualTo('1');

      // P1: bind and partially fetch, leaving it suspended with 6 rows unread.
      sendBind(out, "P1", "");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');
      sendExecute(out, "P1", 4);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 4)).as("P1 first batch").containsExactly(0, 1, 2, 3);
      assertThat(readOneMessage(in).type).as("P1 suspended - 6 rows remain unread").isEqualTo('s');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // P2: bind from the SAME statement (no new Parse) and drain it fully. Before the fix this reused P1's
      // own object, so this Bind+Execute would corrupt P1's suspended progress.
      sendBind(out, "P2", "");
      assertThat(readOneMessage(in).type).as("BindComplete").isEqualTo('2');
      sendExecute(out, "P2", 0);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 10)).as("P2 reads every row, independently of P1's in-progress fetch").containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
      assertThat(readOneMessage(in).type).as("P2 fully drained").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');

      // P1 must still be resumable from exactly where it left off (row 4) - not empty, not restarted, and not
      // showing P2's already-exhausted cursor.
      sendExecute(out, "P1", 10);
      sendSync(out);
      assertThat(readNextBatchOfIds(in, 6)).as("P1 continues from row 4, unaffected by P2's Bind+Execute").containsExactly(4, 5, 6, 7, 8, 9);
      assertThat(readOneMessage(in).type).as("P1 now fully drained too").isEqualTo('C');
      assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');
    }
  }

  // Regression: #6458
  @Test
  void aFetchSizeSmallerThanTheResultReturnsEveryRowAcrossSeveralSuspendedBatches() throws Exception {
    // 23 rows with a fetch size of 7 batches as 7, 7, 7, 2 - the last batch stops short of the limit, so it
    // exercises both the "limit hit, rows remain" slice (the first three batches) and the "naturally
    // exhausted before the limit" one (the last) in the pagination step of executeCommand().
    assertFetchSizeReturnsEveryRow(23, 7);
  }

  // Regression: #6458
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
          .containsExactlyElementsOf(IntStream.range(0, totalRows).boxed().toList());
    }
  }

  // Regression: #6458
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
    return DriverManager.getConnection(
        getServerPostgresJdbcUrl(), properties);
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
    return Integer.parseInt(parseSingleStringColumnDataRow(payload));
  }

  /**
   * Same shape as {@link #readNextBatchOfIds} but for a single VARCHAR column (the catalog-query test's
   * {@code relname}), read as text rather than parsed into an int.
   */
  private static List<String> readNextBatchOfStrings(final DataInputStream in, final int expectedCount) throws Exception {
    final List<String> values = new ArrayList<>(expectedCount);
    for (int i = 0; i < expectedCount; i++) {
      final Msg m = readOneMessage(in);
      assertThat(m.type).as("row %d of this batch must be a DataRow ('D'), sent before any terminator", i).isEqualTo('D');
      values.add(parseSingleStringColumnDataRow(m.payload));
    }
    return values;
  }

  private static String parseSingleStringColumnDataRow(final byte[] payload) throws Exception {
    final DataInputStream p = new DataInputStream(new ByteArrayInputStream(payload));
    final int fieldCount = p.readUnsignedShort();
    assertThat(fieldCount).as("this query projects exactly one column").isEqualTo(1);
    final int len = p.readInt();
    final byte[] valueBytes = new byte[len];
    p.readFully(valueBytes);
    return new String(valueBytes, StandardCharsets.UTF_8);
  }

  /**
   * The Bind naming a statement that was never parsed is refused with {@code 26000} (issue #8211), the Execute
   * pipelined behind it is discarded, and an Execute of the portal name in the next round trip finds nothing to
   * resurrect: {@code 34000}.
   */
  private static void assertMissingStatementThenPortal(final DataOutputStream out, final DataInputStream in, final String portalName)
      throws Exception {
    assertThat(readOneMessage(in).type).as("the Bind is refused, not answered BindComplete").isEqualTo('E');
    assertThat(readOneMessage(in).type).as("the Execute behind it is discarded until Sync").isEqualTo('Z');

    sendExecute(out, portalName, 0);
    sendSync(out);
    assertThat(readOneMessage(in).type)
        .as("%s was removed, not resurrected with its stale first-run result", portalName)
        .isEqualTo('E');
    assertThat(readOneMessage(in).type).as("ReadyForQuery closes this Sync").isEqualTo('Z');
  }

  private static void runSimpleQueryToCompletion(final DataOutputStream out, final DataInputStream in, final String sql) throws Exception {
    final byte[] queryBytes = (sql + "\0").getBytes(StandardCharsets.UTF_8);
    out.writeByte('Q');
    out.writeInt(4 + queryBytes.length);
    out.write(queryBytes);
    out.flush();
    readMessageOfType(in, 'Z'); // drains RowDescription/CommandComplete/etc. through ReadyForQuery
  }

  /*
   * Regression tests for issue #6698: {@link PostgresNetworkExecutor#preparedStatements} map is never pruned
   * on {@code Close('S')} messages naming a prepared statement or unnamed statement.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #6698
  @Test
  @DisplayName("[#6698] Close('S') removes named prepared statement from preparedStatements map")
  void closeNamedPreparedStatementRemovesFromMap() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. Parse named prepared statement "S1"
        sendParse(out, "S1", "SELECT 1");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('1', 'Z'); // ParseComplete, ReadyForQuery

        // 2. Describe('S', "S1") before close: returns ParameterDescription ('t') + RowDescription ('T')
        sendDescribe(out, 'S', "S1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('t', 'T', 'Z');

        // 3. Close('S', "S1")
        sendClose(out, 'S', "S1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z'); // CloseComplete, ReadyForQuery

        // 4. Describe('S', "S1") after close: statement was pruned, so it is refused with 26000 (issue #8211)
        sendDescribe(out, 'S', "S1");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages))
            .as("Describe on closed statement must be refused because preparedStatements was pruned")
            .containsExactly('E', 'Z');

        // 5. Bind from closed statement "S1": fails to find statement in preparedStatements
        sendBind(out, "P1", "S1");
        sendExecute(out, "P1", 0);
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        // ErrorResponse for the Bind ('E'), the Execute discarded until Sync, then ReadyForQuery ('Z')
        assertThat(messageTypesOf(messages))
            .as("Bind from a closed statement must be refused, not answered BindComplete")
            .containsExactly('E', 'Z');
      });
    }
  }

  // Regression: #6698
  @Test
  @DisplayName("[#6698] Close('S') removes unnamed prepared statement from preparedStatements map")
  void closeUnnamedPreparedStatementRemovesFromMap() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // 1. Parse unnamed prepared statement ""
        sendParse(out, "", "SELECT 1");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('1', 'Z');

        // 2. Describe('S', "") before close: returns ParameterDescription ('t') + RowDescription ('T')
        sendDescribe(out, 'S', "");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('t', 'T', 'Z');

        // 3. Close('S', "")
        sendClose(out, 'S', "");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');

        // 4. Describe('S', "") after close: refused with 26000 (issue #8211)
        sendDescribe(out, 'S', "");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages))
            .as("Describe on closed unnamed statement must be refused")
            .containsExactly('E', 'Z');
      });
    }
  }

  // Regression: #6698
  @Test
  @DisplayName("[#6698] Closing a prepared statement does not discard previously bound active portals")
  void closePreparedStatementPreservesExistingBoundPortals() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // One pipeline, since a portal lives only as long as the implicit block a Sync ends (issue #8212):
        // 1. Parse statement "S1", 2. bind portal "P1" from it, 3. close statement "S1",
        // 4. the existing bound portal "P1" still executes successfully
        sendParse(out, "S1", "SELECT 42");
        sendBind(out, "P1", "S1");
        sendClose(out, 'S', "S1");
        sendExecute(out, "P1", 0);
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        // ParseComplete, BindComplete, CloseComplete, DataRow (no RowDescription: nothing was Described, issue #8244), CommandComplete, ReadyForQuery
        assertThat(messageTypesOf(messages)).containsExactly('1', '2', '3', 'D', 'C', 'Z');

        // 5. New portal "P2" bound from closed statement "S1" is refused (issue #8211)
        sendBind(out, "P2", "S1");
        sendExecute(out, "P2", 0);
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('E', 'Z');
      });
    }
  }

  // Regression: #6698
  @Test
  @DisplayName("[#6698] Close('P') and Close('S') on non-existent targets return CloseComplete without error")
  void closeNonExistentTargetReturnsCloseComplete() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // Close non-existent statement
        sendClose(out, 'S', "nonexistent_stmt");
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');

        // Close non-existent portal
        sendClose(out, 'P', "nonexistent_portal");
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('3', 'Z');
      });
    }
  }

  // Regression: #6698
  @Test
  @DisplayName("[#6698] Close('P') removes portal from portals map")
  void closePortalRemovesFromMap() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // One pipeline: the Sync would drop the portal on its own (issue #8212), which would hide what Close does
        sendParse(out, "S1", "SELECT 1");
        sendBind(out, "P1", "S1");
        sendClose(out, 'P', "P1");
        // Execute on closed portal is refused with 34000 (issue #8211)
        sendExecute(out, "P1", 0);
        sendSync(out);
        final List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('1', '2', '3', 'E', 'Z');
      });
    }
  }

  // Regression: #6698
  @Test
  @DisplayName("[#6698] Rebinding an existing portal name from a closed prepared statement invalidates portal")
  void rebindPortalFromClosedStatementClearsExistingPortalAndReturnsNoData() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        // One pipeline, since a portal lives only as long as the implicit block a Sync ends (issue #8212):
        // 1. Parse statement "S1", 2. bind portal "P1" from it, 3. execute "P1" - returns result,
        // 4. close statement "S1", 5. rebind portal "P1" naming closed statement "S1": refused (issue #8211), and the
        // Execute behind it discarded rather than answered by the existing P1
        sendParse(out, "S1", "SELECT 42");
        sendBind(out, "P1", "S1");
        sendExecute(out, "P1", 0);
        sendClose(out, 'S', "S1");
        sendBind(out, "P1", "S1");
        sendExecute(out, "P1", 0);
        sendSync(out);
        List<WireMessage> messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages))
            .as("the rebind from a closed statement is refused, and P1 does not answer again")
            .containsExactly('1', '2', 'D', 'C', '3', 'E', 'Z');

        // 6. P1 is gone: the refused Bind removed it, and the aborted block would have dropped it anyway
        sendExecute(out, "P1", 0);
        sendSync(out);
        messages = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(messages)).containsExactly('E', 'Z');
      });
    }
  }

  /*
   * Regression test for issue #7906: {@code parseCommand()} registered its portal with
   * {@code preparedStatements.put(portalName, portal)} as the LAST thing it did, after every branch that can fail,
   * so a Parse that was REFUSED left whatever had been parsed under that name BEFORE it still registered. A Bind
   * afterwards resurrected it, and the client received - for the statement the server had just refused - a complete
   * successful answer built from a DIFFERENT statement: BindComplete, RowDescription, the old statement's rows, and
   * a CommandComplete with a success tag.
   * <p>
   * PostgreSQL destroys the unnamed prepared statement the moment a Parse names it as destination, successful or
   * not ({@code drop_unnamed_stmt()} at the top of {@code exec_parse_message}), so a Bind afterwards gets
   * {@code unnamed prepared statement does not exist}.
   * <p>
   * The interesting shape is the second one below - two separate round trips, each ended by its own Sync. The
   * skip-until-Sync discard of issue #7775 cannot explain it: by the time the third round trip arrives the failing
   * message block is two Syncs in the past. Any Parse failure reaches it, not only the ROLLBACK TO refusal of
   * issue #7846 - a plain syntax error does it too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String TYPE = "Stale7906";

  // Regression: #7906
  @Test
  @DisplayName("[#7906] a refused Parse in its own round trip does not leave the previous statement bindable")
  void aRefusedParseDestroysTheStatementRegisteredUnderTheSameName() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulate(out, in);

        // Round 1: a good unnamed statement, bound and executed to completion.
        sendParse(out, "", "SELECT id FROM " + TYPE);
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> first = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(first)).as("the good statement answers rows").contains('1', '2', 'T', 'D', 'C');

        // Round 2: a Parse that fails, alone, ended by its own Sync. Only the error comes back.
        sendParse(out, "", "SELEKT bogus syntax");
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("the refused Parse answers an ErrorResponse").contains('E');
        assertThat(messageTypesOf(refused)).as("and no ParseComplete").doesNotContain('1');

        // Round 3: Bind/Describe/Execute the unnamed statement. It no longer exists, so the server must not
        // answer with round 1's rows under a SELECT tag.
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> stale = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(stale))
            .as("the destroyed statement must not be resurrected: no RowDescription and no DataRow for it")
            .doesNotContain('T', 'D');
        assertThat(messageTypesOf(stale)).as("and no success tag for a statement that was never parsed")
            .doesNotContain('C');
      });
    }
  }

  // Regression: #7906
  /**
   * The same hole reached through the refusal that widened it (issue #7846), and in one pipelined request - the
   * shape {@code PQsendQueryParams} produces. Here the skip-until-Sync of #7775 also applies, so the whole
   * pipeline behind the error is discarded; what this pins is that the stale statement is not answered even
   * when it is.
   */
  @Test
  @DisplayName("[#7906] a refused ROLLBACK TO in a pipeline does not run the previously parsed statement")
  void aRefusedRollbackToInOnePipelineDoesNotRunThePreviousStatement() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulate(out, in);

        sendParse(out, "", "SELECT id FROM " + TYPE);
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('T', 'D', 'C');

        sendParse(out, "", "ROLLBACK TO sp1");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).as("the ROLLBACK TO is refused").contains('E');
        assertThat(messageTypesOf(response))
            .as("and the previously parsed SELECT is not run in its place")
            .doesNotContain('T', 'D', 'C');

        // The connection is still usable afterwards: a fresh Parse installs a statement of its own.
        sendParse(out, "", "SELECT id FROM " + TYPE);
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('1', '2', 'T', 'D', 'C');
      });
    }
  }

  private void createAndPopulate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    readUntilReadyForQuery(in);
    sendSimpleQuery(out, "CREATE PROPERTY " + TYPE + ".id IF NOT EXISTS INTEGER");
    readUntilReadyForQuery(in);
    sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET id = 1");
    readUntilReadyForQuery(in);
  }

  /*
   * Regression test for issue #8071, the Bind-side twin of #7906: {@code bindCommand()} registered its portal only
   * on the success path, and neither its catch arm (a parameter {@code PostgresType.deserialize()} refuses) nor its
   * aborted-block refusal touched {@code portals}. A Bind that failed therefore left the PREVIOUS portal bound under
   * the same name, and an Execute of that name in a later round trip found a portal already executed, with a
   * materialised result set, and answered the OLD statement's rows with a success tag.
   * <p>
   * PostgreSQL never leaves such a portal usable: the unnamed one is replaced by {@code CreatePortal} before any
   * parameter is converted, and any error aborts the transaction the old portal lived in. Each test runs the failed
   * Bind in its own round trip, so the skip-until-Sync discard of #7775 cannot explain the outcome.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final String STALE_BIND_TYPE = "Stale8071";

  // Deliberately not a real PostgreSQL OID: PostgresType.deserialize() throws for it (see #5923's test).
  private static final int UNSUPPORTED_TYPE_OID = 999999;

  // Regression: #8071
  @Test
  @DisplayName("[#8071] a Bind that fails deserializing a parameter does not leave the previous unnamed portal executable")
  void aFailedBindDestroysThePreviousUnnamedPortal() throws Exception {
    assertFailedBindDestroysPreviousPortal("");
  }

  // Regression: #8071
  @Test
  @DisplayName("[#8071] a Bind that fails deserializing a parameter does not leave the previous named portal executable")
  void aFailedBindDestroysThePreviousNamedPortal() throws Exception {
    assertFailedBindDestroysPreviousPortal("p8071");
  }

  // Regression: #8071
  @Test
  @DisplayName("[#8071] a Bind refused in an aborted block does not leave the previous portal executable after ROLLBACK")
  void aBindRefusedInAnAbortedBlockDestroysThePreviousPortal() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulateStaleBind(out, in);

        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        // Round 1: statement A bound under the unnamed portal and executed to completion inside the block.
        sendParse(out, "sA", "SELECT id FROM " + STALE_BIND_TYPE);
        sendBind(out, "", "sA");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("statement A answers its rows").contains('1', '2', 'D', 'C');

        // A failing statement aborts the block.
        sendSimpleQuery(out, "SELEC 1");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('E');

        // Round 2: a Bind under the same portal name, refused with 25P02 because the block is aborted.
        sendBind(out, "", "sA");
        sendSync(out);
        final List<WireMessage> refused = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(refused)).as("the Bind is refused").contains('E').doesNotContain('2');

        sendSimpleQuery(out, "ROLLBACK");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');

        // Round 3: Execute the portal name the refused Bind targeted. It must not answer A's rows again.
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> stale = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(stale)).as("the portal the refused Bind replaced must not answer A's rows")
            .doesNotContain('D');
        assertThat(messageTypesOf(stale)).as("nor a success tag for it").doesNotContain('C');
      });
    }
  }

  private void assertFailedBindDestroysPreviousPortal(final String portalName) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
        createAndPopulateStaleBind(out, in);

        // Round 1: statement A, bound under the portal name and executed to completion.
        sendParse(out, "sA", "SELECT id FROM " + STALE_BIND_TYPE);
        sendBind(out, portalName, "sA");
        sendExecute(out, portalName);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("statement A answers its rows").contains('1', '2', 'D', 'C');

        // Round 2: statement B declares a parameter the server cannot deserialize, and its Bind under the SAME
        // portal name fails mid-message. Only the error comes back.
        sendParseWithTypes(out, "sB", "SELECT id FROM " + STALE_BIND_TYPE + " WHERE id = $1", UNSUPPORTED_TYPE_OID);
        sendBind(out, portalName, "sB", "1");
        sendSync(out);
        final List<WireMessage> failed = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(failed)).as("B parses").contains('1');
        assertThat(messageTypesOf(failed)).as("and its Bind fails").contains('E').doesNotContain('2');

        // Round 3: Execute the portal name. A's portal must be gone, so neither A's rows nor a success tag.
        sendExecute(out, portalName);
        sendSync(out);
        final List<WireMessage> stale = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(stale)).as("the failed Bind must not leave A's portal to answer its rows")
            .doesNotContain('D');
        assertThat(messageTypesOf(stale)).as("nor a success tag for a statement the client never bound").doesNotContain('C');

        // The connection stays usable: a fresh Bind of A under the same name answers normally.
        sendBind(out, portalName, "sA");
        sendExecute(out, portalName);
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).contains('2', 'D', 'C');
      });
    }
  }

  private void createAndPopulateStaleBind(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendSimpleQuery(out, "CREATE DOCUMENT TYPE " + STALE_BIND_TYPE + " IF NOT EXISTS");
    readUntilReadyForQuery(in);
    sendSimpleQuery(out, "CREATE PROPERTY " + STALE_BIND_TYPE + ".id IF NOT EXISTS INTEGER");
    readUntilReadyForQuery(in);
    sendSimpleQuery(out, "INSERT INTO " + STALE_BIND_TYPE + " SET id = 1");
    readUntilReadyForQuery(in);
  }

  /*
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
  private static final String LIFECYCLE_TYPE = "Portal8212";

  // Regression: #8211
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

  // Regression: #8211
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
      sendParse(out, "", "SELECT id FROM " + LIFECYCLE_TYPE);
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'D', 'D', 'C', 'Z');
    });
  }

  // Regression: #8212
  @Test
  @DisplayName("[#8212] a portal is dropped with the aborted implicit block, so a Bind discarded by skip-until-Sync leaves nothing executable")
  void portalDoesNotOutliveTheAbortedImplicitBlock() throws Exception {
    runOnConnection((out, in) -> {
      // Round 1: statement A bound under the unnamed portal and executed.
      sendParse(out, "", "SELECT id FROM " + LIFECYCLE_TYPE);
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

  // Regression: #8212
  @Test
  @DisplayName("[#8212] in autocommit a portal ends at the Sync that terminates its implicit block")
  void portalEndsAtSyncInAutocommit() throws Exception {
    runOnConnection((out, in) -> {
      sendParse(out, "s1", "SELECT id FROM " + LIFECYCLE_TYPE + " ORDER BY id");
      sendBind(out, "p1", "s1");
      sendExecuteWithLimit(out, "p1", 1);
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("suspended after one row").containsExactly('1', '2', 'D', 's', 'Z');

      sendExecuteWithLimit(out, "p1", 1);
      sendSync(out);
      final List<WireMessage> resumed = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(resumed)).containsExactly('E', 'Z');
      assertError(resumed.getFirst(), "34000", "portal \"p1\" does not exist");

      // The prepared statement is session-scoped and survives: a new Bind from it works.
      sendBind(out, "p1", "s1");
      sendExecute(out, "p1");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'D', 'D', 'C', 'Z');
    });
  }

  // Regression: #8212
  @Test
  @DisplayName("[#8212] inside an explicit block a portal survives Sync and is dropped at COMMIT")
  void portalSurvivesSyncInsideExplicitBlockAndEndsAtCommit() throws Exception {
    runOnConnection((out, in) -> {
      sendSimpleQuery(out, "BEGIN");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

      sendParse(out, "s1", "SELECT id FROM " + LIFECYCLE_TYPE + " ORDER BY id");
      sendBind(out, "p1", "s1");
      sendExecuteWithLimit(out, "p1", 1);
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'D', 's', 'Z');

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
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String command : new String[] { "CREATE DOCUMENT TYPE " + LIFECYCLE_TYPE + " IF NOT EXISTS",
            "CREATE PROPERTY " + LIFECYCLE_TYPE + ".id IF NOT EXISTS INTEGER", "DELETE FROM " + LIFECYCLE_TYPE, "INSERT INTO " + LIFECYCLE_TYPE + " SET id = 1",
            "INSERT INTO " + LIFECYCLE_TYPE + " SET id = 2" }) {
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

  /*
   * Issue #8708: a portal whose answer is materialized at Parse (SHOW, and a catalog query whose filters are literals)
   * skipped the Execute row-limit slice, so a fetch-size cursor over SHOW ALL got the whole result and a
   * {@code CommandComplete} instead of {@code PortalSuspended}, and a second Execute re-sent everything.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final int LIMIT = 3;

  // Regression: #8708
  @Test
  @DisplayName("[#8708] SHOW ALL honours the Execute row limit and resumes where it stopped")
  void showAllIsSliced() throws Exception {
    assertSliced("SHOW ALL");
  }

  // Regression: #8708
  @Test
  @DisplayName("[#8708] a catalog query with literal filters honours the Execute row limit")
  void literalCatalogQueryIsSliced() throws Exception {
    assertSliced("SELECT typname FROM pg_catalog.pg_type");
  }

  // Regression: #8708
  @Test
  @DisplayName("[#8708] a partially fetched SHOW ALL keeps its slice when a SET runs before the next Execute")
  void partiallyFetchedShowAllIsNotRestartedBySet() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "SHOW ALL");
        final int total = count(readUntilReadyForQuery(in), 'D');

        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);
        sendParse(out, "", "SHOW ALL");
        sendBind(out, "P1", "");
        sendExecute(out, "P1", LIMIT);
        sendSync(out);
        assertThat(count(readUntilReadyForQuery(in), 'D')).isEqualTo(LIMIT);

        sendSimpleQuery(out, "SET statement_timeout = 7000");
        readUntilReadyForQuery(in);

        // The cursor resumes after the first slice: it neither restarts nor re-reads a different answer
        sendExecute(out, "P1", 0);
        sendSync(out);
        assertThat(count(readUntilReadyForQuery(in), 'D')).isEqualTo(total - LIMIT);
      });
    }
  }

  private void assertSliced(final String query) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, query);
        final int total = count(readUntilReadyForQuery(in), 'D');
        assertThat(total).as("the query must return more rows than the limit").isGreaterThan(LIMIT);

        // A portal lives in a transaction, so the cursor needs an explicit block
        sendSimpleQuery(out, "BEGIN");
        readUntilReadyForQuery(in);

        sendParse(out, "", query);
        sendBind(out, "P1", "");
        sendExecute(out, "P1", LIMIT);
        sendSync(out);
        final List<WireMessage> first = readUntilReadyForQuery(in);
        assertThat(count(first, 'D')).as("first slice").isEqualTo(LIMIT);
        assertThat(messageTypesOf(first)).as("suspended, not complete").contains('s').doesNotContain('C');

        int received = LIMIT;
        while (true) {
          sendExecute(out, "P1", LIMIT);
          sendSync(out);
          final List<WireMessage> next = readUntilReadyForQuery(in);
          final int rows = count(next, 'D');
          assertThat(rows).isLessThanOrEqualTo(LIMIT);
          received += rows;
          if (messageTypesOf(next).contains('C'))
            break;
          assertThat(messageTypesOf(next)).contains('s');
        }
        assertThat(received).as("every row exactly once").isEqualTo(total);

        // A drained portal answers an empty slice, it does not re-send the answer
        sendExecute(out, "P1", LIMIT);
        sendSync(out);
        assertThat(count(readUntilReadyForQuery(in), 'D')).isZero();
      });
    }
  }

  private static int count(final List<WireMessage> messages, final char type) {
    return (int) messageTypesOf(messages).stream().filter(t -> t == type).count();
  }

  /*
   * Issue #11028: a Parse of an empty query string was answered with EmptyQueryResponse, a reply that belongs to Execute, after the
   * statement name had been invalidated, so nothing was registered and every Bind/Describe/Execute pipelined behind it failed with
   * "prepared statement does not exist". PostgreSQL registers the empty statement like any other: ParseComplete, a Describe answers
   * NoData, an Execute answers EmptyQueryResponse in place of CommandComplete. Every Parse leaves exactly one ParseComplete or one
   * ErrorResponse.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11028
  @Test
  @DisplayName("[#11028] Parse of an empty string answers ParseComplete")
  void parseOfAnEmptyStringAnswersParseComplete() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendParse(out, "pstmt_empty", "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 'Z');
      });
    }
  }

  // Regression: #11028
  @Test
  @DisplayName("[#11028] the whole extended pipeline of an empty statement answers as PostgreSQL does")
  void theEmptyStatementPipelineAnswersOneReplyPerMessage() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        // unnamed: PostgreSQL answers 1, 2, n (NoData), I (EmptyQueryResponse), Z
        sendParse(out, "", "");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'n', 'I', 'Z');

        // named, described as a statement: ParameterDescription and NoData
        sendParse(out, "pe2", "");
        sendDescribe(out, 'S', "pe2");
        sendBind(out, "", "pe2");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', '2', 'I', 'Z');

        // the statement stays registered: a statement cache binds and executes it again with no Parse in front
        sendBind(out, "", "pe2");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'I', 'Z');

        // text that normalizes to nothing is an empty statement too
        for (final String text : new String[] { "   ", ";", " ; ", ";;", " ; \n ; ;" }) {
          sendParse(out, "", text);
          sendBind(out, "", "");
          sendExecute(out, "");
          sendSync(out);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("'%s'", text).containsExactly('1', '2', 'I', 'Z');
        }

        // and the connection is in step: an ordinary statement right after answers as usual
        sendParse(out, "", "SELECT 1 AS one");
        sendBind(out, "", "");
        sendDescribe(out, 'P', "");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', 'T', 'D', 'C', 'Z');
      });
    }
  }

  // Regression: #11028
  @Test
  @DisplayName("[#11028] a simple query of semicolons only is the empty query too")
  void aSimpleQueryOfSemicolonsOnlyIsTheEmptyQuery() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String text : new String[] { "", ";", ";;", " ; ; " }) {
          sendSimpleQuery(out, text);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("'%s'", text).containsExactly('I', 'Z');
        }
      });
    }
  }

  // Regression: #11028
  @Test
  @DisplayName("[#11028] an empty statement inside a transaction block leaves the block open and healthy")
  void anEmptyStatementInsideATransactionBlockLeavesItOpen() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendParse(out, "", "");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> response = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(response)).containsExactly('1', '2', 'I', 'Z');
        assertThat(readyForQueryStatusOf(response)).isEqualTo('T');

        sendSimpleQuery(out, "COMMIT");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
  }

  // Regression: #11028
  @Test
  @DisplayName("[#11028] pgjdbc prepareStatement(\"\") executes and returns no result set")
  void pgjdbcPreparesAndExecutesTheEmptyStatement() throws Exception {
    try (final Connection connection = jdbcConnection()) {
      try (final PreparedStatement statement = connection.prepareStatement("")) {
        assertThat(statement.execute()).isFalse();
        // executed again, as an application reuses a prepared statement
        assertThat(statement.execute()).isFalse();
      }
      try (final Statement statement = connection.createStatement(); final ResultSet rs = statement.executeQuery("SELECT 1 AS one")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isEqualTo(1);
      }
    }
  }

  /*
   * Issue #13380: a text that holds only comments is the empty query as well as one that holds only ';' and blanks: PostgreSQL answers both
   * EmptyQueryResponse, and a client or a pooler pings a connection with {@code -- ping}. The simple protocol already answered it so
   * (its statement splitter drops the comments); a Parse of it was answered with the engine's syntax error. The engine now answers
   * such a text with an empty result (EmptyStatement) and the wire maps that to EmptyQueryResponse at Parse too. The lexer reads
   * {@code --} as a comment only when a space follows it, so the texts here carry the space.
   */
  @Test
  @DisplayName("a simple query that holds only comments is the empty query (unchanged)")
  void aSimpleQueryOfCommentsOnlyIsTheEmptyQuery() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String text : new String[] { "-- ping", "-- ping\n", "/* x */", "/* x */;", "/* a */ /* b */ ;", "-- a\n/* b */\n-- c" }) {
          sendSimpleQuery(out, text);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("'%s'", text).containsExactly('I', 'Z');
        }

        // the connection is in step: an ordinary statement, with a comment in front of it, answers as usual
        sendSimpleQuery(out, "-- ping\nSELECT 1 AS one");
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('T', 'D', 'C', 'Z');
      });
    }
  }

  @Test
  @DisplayName("the extended pipeline of a statement that holds only comments answers as the empty statement does")
  void theExtendedPipelineOfACommentOnlyStatementIsTheEmptyStatement() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String text : new String[] { "-- ping", "/* x */;", "/* a */ -- b" }) {
          // unnamed: ParseComplete, BindComplete, NoData, EmptyQueryResponse
          sendParse(out, "", text);
          sendBind(out, "", "");
          sendDescribe(out, 'P', "");
          sendExecute(out, "");
          sendSync(out);
          assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("'%s'", text).containsExactly('1', '2', 'n', 'I', 'Z');
        }

        // named: the statement stays registered and is executed again with no Parse in front
        sendParse(out, "pc1", "-- ping");
        sendDescribe(out, 'S', "pc1");
        sendBind(out, "", "pc1");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 't', 'n', '2', 'I', 'Z');
        sendBind(out, "", "pc1");
        sendExecute(out, "");
        sendSync(out);
        assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'I', 'Z');
      });
    }
  }

  @Test
  @DisplayName("a statement that holds only comments inside a transaction block leaves the block open")
  void aCommentOnlyStatementInsideATransactionBlockLeavesItOpen() throws Exception {
    try (final Socket socket = connect()) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        sendSimpleQuery(out, "BEGIN");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

        sendSimpleQuery(out, "-- ping");
        final List<WireMessage> simple = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(simple)).containsExactly('I', 'Z');
        assertThat(readyForQueryStatusOf(simple)).isEqualTo('T');

        sendParse(out, "", "/* x */");
        sendBind(out, "", "");
        sendExecute(out, "");
        sendSync(out);
        final List<WireMessage> extended = readUntilReadyForQuery(in);
        assertThat(messageTypesOf(extended)).containsExactly('1', '2', 'I', 'Z');
        assertThat(readyForQueryStatusOf(extended)).isEqualTo('T');

        sendSimpleQuery(out, "COMMIT");
        assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('I');
      });
    }
  }

  private Connection jdbcConnection() throws Exception {
    Class.forName("org.postgresql.Driver");
    final Properties props = new Properties();
    props.setProperty("user", "root");
    props.setProperty("password", DEFAULT_PASSWORD_FOR_TESTS);
    props.setProperty("ssl", "false");
    // Parse/Bind/Describe/Execute for every statement, and named server-side statements from the first execution on
    props.setProperty("preferQueryMode", "extended");
    props.setProperty("prepareThreshold", "1");
    return DriverManager.getConnection(getServerPostgresJdbcUrl(), props);
  }

  private Socket connect() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
    return socket;
  }

  /*
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
  private static final String BIND_COUNT_TYPE     = "Bind11038";
  private static final int    INT4_OID = 23;

  // Regression: #11038
  @Test
  @DisplayName("[#11018] a Bind supplying fewer or more values than the statement requires is refused with 08P01")
  void bindWithTheWrongParameterCountIsRefused() throws Exception {
    runOnBindCountConnection((out, in) -> {
      sendParseWithTypes(out, "s1", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = $1", INT4_OID);
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 'Z');

      sendBind(out, "", "s1", new String[0]);
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> tooFew = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(tooFew)).as("refused, and the Execute behind it discarded").containsExactly('E', 'Z');
      assertError(tooFew.getFirst(), "08P01", "bind message supplies 0 parameters, but prepared statement \"s1\" requires 1");

      sendBind(out, "", "s1", "1", "2", "3");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> tooMany = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(tooMany)).containsExactly('E', 'Z');
      assertError(tooMany.getFirst(), "08P01", "bind message supplies 3 parameters, but prepared statement \"s1\" requires 1");

      // The exact count still answers
      sendBind(out, "", "s1", "1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> exact = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(exact)).containsExactly('2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(exact)).isEqualTo("1");
    });
  }

  // Regression: #11038
  @Test
  @DisplayName("[#11018] an UPDATE bound with no value for its $1 never runs, so it cannot write NULL")
  void shortBindNeverWritesNull() throws Exception {
    runOnBindCountConnection((out, in) -> {
      // No declared types: the count comes from the $N the text references
      sendParse(out, "up", "UPDATE " + BIND_COUNT_TYPE + " SET id = $1 WHERE id = 1");
      sendBind(out, "", "up", new String[0]);
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(refused)).containsExactly('1', 'E', 'Z');
      assertError(refused.get(1), "08P01", "bind message supplies 0 parameters, but prepared statement \"up\" requires 1");

      sendSimpleQuery(out, "SELECT count(*) AS c FROM " + BIND_COUNT_TYPE + " WHERE id IS NULL");
      assertThat(firstDataRowValue(readUntilReadyForQuery(in))).as("no row was written with a NULL id").isEqualTo("0");
      sendSimpleQuery(out, "SELECT count(*) AS c FROM " + BIND_COUNT_TYPE + " WHERE id = 1");
      assertThat(firstDataRowValue(readUntilReadyForQuery(in))).as("the row the UPDATE targeted is untouched").isEqualTo("1");
    });
  }

  // Regression: #11038
  @Test
  @DisplayName("[#11018] the required count is the highest $N referenced, even past the declared types, and Describe reports it")
  void requiredCountIsTheHighestPlaceholderBeyondTheDeclaredTypes() throws Exception {
    runOnBindCountConnection((out, in) -> {
      // One type declared, two placeholders referenced: PostgreSQL infers the second and requires both
      sendParseWithTypes(out, "s2", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = $1 OR id = $2 ORDER BY id", INT4_OID);
      sendDescribe(out, 'S', "s2");
      sendSync(out);
      final List<WireMessage> described = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(described)).startsWith('1', 't');
      assertThat(parameterCountOf(described.get(1))).isEqualTo(2);

      sendBind(out, "", "s2", "1");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertError(refused.getFirst(), "08P01", "bind message supplies 1 parameters, but prepared statement \"s2\" requires 2");

      sendBind(out, "", "s2", "1", "2");
      sendExecute(out, "");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('2', 'D', 'D', 'C', 'Z');
    });
  }

  // Regression: #11038
  @Test
  @DisplayName("[#11018] a $N inside a string literal, a quoted identifier or a comment is not a placeholder")
  void placeholdersInsideLiteralsAndCommentsAreNotCounted() throws Exception {
    runOnBindCountConnection((out, in) -> {
      sendParse(out, "s3", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = $1 AND '$5' = '$5' /* $7 */");
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
      sendParse(out, "s3b", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE 'x\\' <> 'y' AND id = $1");
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

  // Regression: #11038
  @Test
  @DisplayName("[#11029] a statement whose only $N is inside a literal declares no parameter when Parse declares no types")
  void literalDollarDigitsDeclareNoParameter() throws Exception {
    runOnBindCountConnection((out, in) -> {
      // The shape asyncpg and node-postgres send: zero declared types, so the count comes from the text alone. Each of
      // these holds a $N only inside a quoted construct, so PostgreSQL describes no parameter and binds none. A dollar-quoted
      // body ($$ $3 $$) is skipped by the same scan (PostgresParameterBindingTest), but the SQL engine does not
      // parse that literal form, so it cannot be executed here
      final String[] statements = { "SELECT 'price is $1 today' AS v", "SELECT \"$2\" FROM " + BIND_COUNT_TYPE + " LIMIT 1",
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

  // Regression: #11038
  @Test
  @DisplayName("[#11018] a Bind with several format codes that do not match its value count is refused with 08P01")
  void bindWithMismatchedFormatCodesIsRefused() throws Exception {
    runOnBindCountConnection((out, in) -> {
      sendParseWithTypes(out, "s4", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = $1", INT4_OID);
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

  // Regression: #11038
  @Test
  @DisplayName("[#11034] a Parse naming a live statement is refused with 42P05 and the statement keeps its text; the unnamed one is replaced")
  void reParseOfALiveNamedStatementIsRefused() throws Exception {
    runOnBindCountConnection((out, in) -> {
      sendParse(out, "rp1", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = 1");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', 'Z');

      sendParse(out, "rp1", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = 2");
      sendSync(out);
      final List<WireMessage> refused = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(refused)).containsExactly('E', 'Z');
      assertError(refused.getFirst(), "42P05", "prepared statement \"rp1\" already exists");

      sendBind(out, "", "rp1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> original = readUntilReadyForQuery(in);
      assertThat(firstDataRowValue(original)).as("the statement still holds the first text").isEqualTo("1");

      // Once closed, the name can be defined again
      sendClose(out, 'S', "rp1");
      sendParse(out, "rp1", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = 2");
      sendBind(out, "", "rp1");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> redefined = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(redefined)).containsExactly('3', '1', '2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(redefined)).isEqualTo("2");

      // The unnamed statement is silently replaced, as in PostgreSQL
      sendParse(out, "", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = 1");
      sendParse(out, "", "SELECT id FROM " + BIND_COUNT_TYPE + " WHERE id = 2");
      sendBind(out, "", "");
      sendExecute(out, "");
      sendSync(out);
      final List<WireMessage> unnamed = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(unnamed)).containsExactly('1', '1', '2', 'D', 'C', 'Z');
      assertThat(firstDataRowValue(unnamed)).isEqualTo("2");
    });
  }

  // Regression: #11038
  @Test
  @DisplayName("[#11034] a Bind naming a live portal is refused with 42P03; the unnamed portal is replaced")
  void reBindOfALiveNamedPortalIsRefused() throws Exception {
    runOnBindCountConnection((out, in) -> {
      sendSimpleQuery(out, "BEGIN");
      assertThat(readyForQueryStatusOf(readUntilReadyForQuery(in))).isEqualTo('T');

      sendParse(out, "rb1", "SELECT id FROM " + BIND_COUNT_TYPE + " ORDER BY id");
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

  // Regression: #11038
  @Test
  @DisplayName("[#11034] Close('S') and DEALLOCATE leave the portals bound from the statement executable, as PostgreSQL does")
  void closingAStatementLeavesItsPortalsExecutable() throws Exception {
    runOnBindCountConnection((out, in) -> {
      sendSimpleQuery(out, "BEGIN");
      readUntilReadyForQuery(in);

      sendParse(out, "cs2", "SELECT id FROM " + BIND_COUNT_TYPE + " ORDER BY id");
      sendBind(out, "cs2p", "cs2");
      sendClose(out, 'S', "cs2");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).containsExactly('1', '2', '3', 'Z');

      sendExecute(out, "cs2p");
      sendSync(out);
      assertThat(messageTypesOf(readUntilReadyForQuery(in))).as("the portal holds its own plan").containsExactly('D', 'D', 'C', 'Z');

      sendParse(out, "cs3", "SELECT id FROM " + BIND_COUNT_TYPE + " ORDER BY id");
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

  private void runOnBindCountConnection(final ConnectionTest test) throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword
      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z');

      assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
        for (final String command : new String[] { "CREATE DOCUMENT TYPE " + BIND_COUNT_TYPE + " IF NOT EXISTS",
            "CREATE PROPERTY " + BIND_COUNT_TYPE + ".id IF NOT EXISTS INTEGER", "DELETE FROM " + BIND_COUNT_TYPE,
            "INSERT INTO " + BIND_COUNT_TYPE + " SET id = 1", "INSERT INTO " + BIND_COUNT_TYPE + " SET id = 2" }) {
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

  /*
   * Regression test for issue #5923: {@code bindCommand()} only advanced {@code paramsConsumed} to
   * {@code i + 1} <b>after</b> {@code PostgresType.deserialize()} returned successfully, even though the
   * parameter's length-prefixed value bytes were already fully read off the wire beforehand. When
   * {@code deserialize()} throws (e.g. a parameter declared with a type code the server doesn't recognize),
   * the error-recovery drain in the {@code catch} block restarts at the stale {@code paramsConsumed} index,
   * re-reading bytes that were already consumed and misinterpreting whatever comes next on the wire (here,
   * the real {@code resultFormatCount} field and the following Sync message) as a bogus 32-bit length. That
   * either desynchronizes the channel so badly the connection thread blocks waiting for millions of bytes
   * that will never arrive, or - depending on the misread bytes - trips the {@code POSTGRES_MAX_PARAM_SIZE}
   * guard and forces a connection close, in both cases turning a single malformed Bind message into a dead
   * connection instead of a clean per-statement error.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #5923
  @Test
  @DisplayName("[#5923] A Bind parameter that fails deserialization leaves the channel aligned for the next message")
  void bindDeserializeFailureDoesNotDesyncChannel() throws Exception {
    try (final Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword

      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      // Declare one parameter with a type code deserialize() cannot handle.
      sendParseWithTypes(out, "", "SELECT 1", UNSUPPORTED_TYPE_OID);
      readMessageOfType(in, '1'); // ParseComplete

      // A protocol-correct Bind: format codes, one text-format parameter value, then resultFormatCount.
      // No padding, no extra bytes - exactly what a real client sends. Immediately pipeline a Sync right
      // after it, as real clients commonly do.
      sendBind(out, "", "", "1");
      sendSync(out);
      out.flush();

      // With the channel correctly realigned, the very next message the server sends back must be the
      // ErrorResponse for the failed Bind, followed promptly by ReadyForQuery from the Sync. Before the
      // fix, the drain misreads the wire and the connection thread blocks (or the connection is dropped),
      // so no ErrorResponse/ReadyForQuery pair arrives within the timeout.
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        final int first = readMessageType(in);
        assertThat((char) first).as("ErrorResponse for the malformed Bind").isEqualTo('E');
        readMessageOfType(in, 'Z'); // ReadyForQuery from the pipelined Sync
      });

      // The connection must still be fully usable for a subsequent, unrelated statement - proof the
      // channel is realigned, not just that a single error happened to slip through.
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        sendSimpleQuery(out, "SELECT 1");
        final int responseType = readMessageType(in);
        assertThat((char) responseType).as("response to a well-formed query on the same connection")
            .isIn('T', 'C', 'D');
        readMessageOfType(in, 'Z');
      });
    }
  }

  /*
   * Regression test for issue #5894: the Postgres wire-protocol Bind message reads a client-supplied,
   * unbounded parameter length and allocated a byte array sized by it with no upper bound. An authenticated
   * client declaring a multi-gigabyte parameter could OOM or thrash the server before any payload byte
   * arrives. {@code arcadedb.postgres.maxParamSize} now rejects an oversized declared length before the
   * allocation happens.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #5894
  @Test
  @DisplayName("[#5894] Oversized Bind parameter length is rejected before allocation, with a graceful error instead of an OOM'd thread")
  void oversizedBindParameterIsRejectedGracefully() throws Exception {
    try (final Socket attacker = new Socket()) {
      attacker.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
      final DataOutputStream out = new DataOutputStream(attacker.getOutputStream());
      final DataInputStream in = new DataInputStream(attacker.getInputStream());

      sendStartupMessage(out, "root", getDatabaseName());
      readMessage(in); // AuthenticationCleartextPassword

      sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
      readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery

      sendParse(out, "", "SELECT 1");
      readMessage(in); // ParseComplete

      // Bind message declaring a single parameter whose 32-bit length claims ~2GB. The attacker never
      // sends anything resembling a 2GB payload - only the declared length - mirroring the report's
      // "declare a huge length, don't follow through" shape.
      sendMaliciousBind(out, 0x7FFFFFFF);
      out.flush();

      // Without a bound check, the server allocates `new byte[0x7FFFFFFF]`, which throws OutOfMemoryError
      // (an Error, not an Exception) - it is never caught by bindCommand's `catch (Exception e)`, so the
      // connection thread dies silently and the client never gets a response, hanging here. With the
      // bound check, the value bytes were never read (so the channel cannot be safely resynchronized
      // without risking the same unbounded/blocking read this fix closes), so the server replies with a
      // normal ErrorResponse ('E') and then closes the connection outright, well within the timeout.
      final int responseType = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> readMessageType(in));
      assertThat((char) responseType).isEqualTo('E');

      // The connection is not left half-aligned or hanging: the server closes it after the error, so a
      // further read reaches EOF (rather than blocking or returning garbage from a desynced channel).
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> in.readUnsignedByte())
            .isInstanceOf(java.io.EOFException.class);
      });
    }

    // The rejected connection must not have wedged the shared listener/thread pool: a fresh,
    // well-behaved client is still served promptly right after.
    assertTimeoutPreemptively(Duration.ofSeconds(15), () -> {
      try (final Socket client = new Socket()) {
        client.connect(new InetSocketAddress("localhost", getServerPostgresPort()), 2000);
        final DataOutputStream out = new DataOutputStream(client.getOutputStream());
        final DataInputStream in = new DataInputStream(client.getInputStream());
        sendStartupMessage(out, "root", getDatabaseName());
        readMessage(in); // AuthenticationCleartextPassword
        sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
        readMessageOfType(in, 'Z'); // AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
      }
    });
  }

  /**
   * Bind message for the unnamed portal/statement, declaring one parameter with the given (attacker
   * controlled) 32-bit length, then stopping - no payload bytes follow.
   */
  private static void sendMaliciousBind(final DataOutputStream out, final int declaredParamLength) throws Exception {
    final ByteArrayOutputStream body = new ByteArrayOutputStream();
    writeCString(body, ""); // portal name
    writeCString(body, ""); // statement name
    body.write(0);
    body.write(0); // int16 numParamFormatCodes = 0
    body.write(0);
    body.write(1); // int16 numParamValues = 1
    body.write((declaredParamLength >>> 24) & 0xFF);
    body.write((declaredParamLength >>> 16) & 0xFF);
    body.write((declaredParamLength >>> 8) & 0xFF);
    body.write(declaredParamLength & 0xFF);
    // no payload follows

    final byte[] bodyBytes = body.toByteArray();
    out.writeByte('B');
    out.writeInt(4 + bodyBytes.length);
    out.write(bodyBytes);
    out.flush();
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
