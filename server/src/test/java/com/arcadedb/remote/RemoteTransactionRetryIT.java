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
package com.arcadedb.remote;

import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.http.RetryLaterException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.net.ssl.SSLSession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * How {@link RemoteDatabase#transaction} and its {@code begin()}/{@code commit()} react to failures: a joined block that
 * throws, a failed attempt rolled back before the retry, a deflected (503) begin or commit retried within the budget, a
 * partially committed block never replayed, and the server-side session released on a deflected or failed commit.
 * <p>
 * Each nested class keeps its own server fixture (database, hooks), so they cannot share one.
 */
class RemoteTransactionRetryIT {

  /**
   * Issue #10135, remote side: a {@link RemoteDatabase#transaction} call that JOINED the caller's transaction owns
   * nothing, so whatever its block throws must leave the caller's server-side transaction - and the session id that
   * names it - alive. Before the fix the non-retryable arm rolled it back and cleared the session id.
   */
  @Nested
  class JoinedTransactionFailure extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-tx-10135";

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }

    static Stream<Arguments> failures() {
      return Stream.of(//
          Arguments.of("retryable", (Supplier<Throwable>) () -> new NeedRetryException("simulated conflict")),
          Arguments.of("duplicated key", (Supplier<Throwable>) () -> new DuplicatedKeyException("idx", "k", null)),
          Arguments.of("plain RuntimeException", (Supplier<Throwable>) () -> new IllegalStateException("boom")),
          Arguments.of("Error", (Supplier<Throwable>) () -> new AssertionError("fatal")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("failures")
    void aJoinedBlockThatThrowsLeavesTheCallersTransactionIntact(final String label, final Supplier<Throwable> failure) {
      try (final RollbackCountingDatabase database = newDatabase()) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        database.begin();
        try {
          database.newDocument("Person").set("name", "outer").save();
          final String sessionBefore = database.getSessionId();

          final Throwable expected = failure.get();
          assertThatThrownBy(() -> database.transaction(() -> {
            database.newDocument("Person").set("name", "inner").save();
            throwUnchecked(expected);
          }, true, 3)).isSameAs(expected);

          assertThat(database.rollbacks.get()).as("the caller's transaction must not be rolled back on a %s", label).isZero();
          assertThat(database.isTransactionActive()).isTrue();
          assertThat(database.getSessionId()).as("the caller's session must survive").isEqualTo(sessionBefore);

          database.newDocument("Person").set("name", "outer2").save();
          database.commit();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }

        assertThat(database.query("sql", "select from Person where name in ['outer', 'outer2']").stream().count())
            .as("the caller committed its own work").isEqualTo(2);
      }
    }

    @Test
    void theDefaultOverloadJoinsAndLeavesTheCallersTransactionIntact() {
      try (final RollbackCountingDatabase database = newDatabase()) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        database.transaction(() -> {
          database.newDocument("Person").set("name", "outer").save();
          try {
            database.transaction(() -> {
              throw new IllegalStateException("boom");
            });
          } catch (final IllegalStateException ignore) {
            // EXPECTED
          }
          assertThat(database.isTransactionActive()).isTrue();
          database.newDocument("Person").set("name", "outer2").save();
        });

        assertThat(database.countType("Person", false)).isEqualTo(2);
      }
    }

    /**
     * Not joining while the caller has a transaction open makes {@code begin()} refuse ("Transaction already begun").
     * The session it refused to replace is still the caller's, so it must not be rolled back either.
     */
    @Test
    void aRefusedBeginDoesNotRollBackTheCallersTransaction() {
      try (final RollbackCountingDatabase database = newDatabase()) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        final AtomicInteger executions = new AtomicInteger();

        database.begin();
        try {
          database.newDocument("Person").set("name", "outer").save();
          final String sessionBefore = database.getSessionId();

          assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3)).isInstanceOf(
              TransactionException.class);

          assertThat(executions.get()).isZero();
          assertThat(database.rollbacks.get()).isZero();
          assertThat(database.getSessionId()).isEqualTo(sessionBefore);

          database.commit();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }

        assertThat(database.countType("Person", false)).isEqualTo(1);
      }
    }

    /**
     * The transaction this call opened is still released on an {@link Error}, not only on an {@link Exception}.
     */
    @Test
    void anOwnedTransactionIsReleasedOnAnError() {
      try (final RollbackCountingDatabase database = newDatabase()) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        assertThatThrownBy(() -> database.transaction(() -> {
          database.newDocument("Person").set("name", "owned").save();
          throw new AssertionError("fatal");
        }, false, 3)).isInstanceOf(AssertionError.class);

        assertThat(database.rollbacks.get()).isEqualTo(1);
        assertThat(database.isTransactionActive()).isFalse();
        assertThat(database.countType("Person", false)).isZero();
      }
    }

    private RollbackCountingDatabase newDatabase() {
      return new RollbackCountingDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(final Throwable t) throws T {
      throw (T) t;
    }

    private static class RollbackCountingDatabase extends RemoteDatabase {
      final AtomicInteger rollbacks = new AtomicInteger();

      RollbackCountingDatabase(final String server, final int port, final String databaseName, final String userName,
          final String userPassword) {
        super(server, port, databaseName, userName, userPassword);
      }

      @Override
      public void rollback() {
        rollbacks.incrementAndGet();
        super.rollback();
      }
    }
  }

  /**
   * Regression tests for issue #7030: {@link RemoteDatabase#transaction} was missing the two guards
   * {@code LocalDatabase.transaction()} has - it retried a transaction owned by the caller (the guard issue #661
   * established), and it never rolled back a failed attempt before starting the next one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class FailedAttemptRollback extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-tx-7030";

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }

    /**
     * The bug: joining a transaction the caller owns and then hitting a {@link NeedRetryException} re-ran the block
     * inside that still-open transaction, which silently accumulated the partial effects of every failed attempt.
     * The block must run once and the exception must reach the owner of the transaction.
     */
    @Test
    void aJoinedTransactionIsNotRetried() {
      try (final RollbackCountingDatabase database = newDatabase()) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        final AtomicInteger executions = new AtomicInteger();

        database.begin();
        try {
          assertThatThrownBy(() -> database.transaction(() -> {
            executions.incrementAndGet();
            throw new NeedRetryException("simulated conflict");
          }, true, 5)).isInstanceOf(NeedRetryException.class).hasMessageContaining("simulated conflict");

          assertThat(executions.get()).as("a transaction owned by the caller must not be retried").isEqualTo(1);
          assertThat(database.rollbacks.get()).as("the caller's transaction must not be rolled back from here").isZero();
          assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      }
    }

    /**
     * The bug: a failed attempt left its server-side transaction open, so the next attempt contended with the locks
     * of the previous one until the server timed it out. Each failed attempt must be rolled back before the retry.
     */
    @Test
    void everyFailedAttemptIsRolledBackBeforeTheRetry() {
      try (final RollbackCountingDatabase database = newDatabase()) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        final AtomicInteger executions = new AtomicInteger();

        final boolean createdNewTx = database.transaction(() -> {
          database.newDocument("Person").set("name", "Alice").save();
          if (executions.incrementAndGet() < 3)
            throw new NeedRetryException("simulated conflict");
        }, false, 5);

        assertThat(createdNewTx).isTrue();
        assertThat(executions.get()).isEqualTo(3);
        assertThat(database.rollbacks.get()).as("each of the two failed attempts must be rolled back").isEqualTo(2);
        assertThat(database.countType("Person", false)).as("only the attempt that committed must leave a record")
            .isEqualTo(1);
      }
    }

    /**
     * The same on the arm that gives up: an exception the retry loop does not handle still has to release the
     * server-side transaction on the way out instead of leaving it open until its timeout.
     */
    @Test
    void aFatalFailureRollsBackBeforeRethrowing() {
      try (final RollbackCountingDatabase database = newDatabase()) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        assertThatThrownBy(() -> database.transaction(() -> {
          database.newDocument("Person").set("name", "Alice").save();
          throw new IllegalArgumentException("fatal");
        }, false, 5)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("fatal");

        assertThat(database.rollbacks.get()).isEqualTo(1);
        assertThat(database.isTransactionActive()).isFalse();
        assertThat(database.countType("Person", false)).isZero();
      }
    }

    private RollbackCountingDatabase newDatabase() {
      return new RollbackCountingDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    /**
     * Counts the {@code POST /rollback} the retry loop issues, which is what tells a failed attempt that released
     * its server-side transaction from one that abandoned it.
     */
    private static class RollbackCountingDatabase extends RemoteDatabase {
      final AtomicInteger rollbacks = new AtomicInteger();

      RollbackCountingDatabase(final String server, final int port, final String databaseName, final String userName,
          final String userPassword) {
        super(server, port, databaseName, userName, userPassword);
      }

      @Override
      public void rollback() {
        rollbacks.incrementAndGet();
        super.rollback();
      }
    }
  }

  /**
   * Issue #7780: {@link RemoteDatabase#begin()} and {@link RemoteDatabase#commit()} wrapped a retryable
   * {@link NeedRetryException} (the 503 a server answers while it installs a snapshot, a quorum it cannot reach yet, a
   * leader change) in a {@link TransactionException}, which {@link RemoteDatabase#transaction} does not retry. The
   * configured retry budget was therefore never spent, although the server had refused the request before running it.
   * <p>
   * The deflection is the real one: the test opens the server's snapshot-install window around exactly the request it
   * wants refused, so the answer comes from {@code AbstractServerHttpHandler}'s gate and is decoded by the client's own
   * {@code manageException}.
   */
  @Nested
  class DeflectedBeginAndCommit extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-tx-7780";

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }

    /**
     * The unambiguous half of the issue: a refused {@code /begin} created no transaction and ran nothing, so the retry
     * loop must spend its budget on it instead of failing on the first attempt.
     */
    @Test
    void aDeflectedBeginIsRetriedByTransaction() {
      try (final DeflectingDatabase database = newDatabase("begin", 1)) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        final AtomicInteger executions = new AtomicInteger();
        final boolean createdNewTx = database.transaction(() -> {
          executions.incrementAndGet();
          database.command("sql", "INSERT INTO Person SET name = 'Alice'");
        }, false, 3);

        assertThat(createdNewTx).isTrue();
        assertThat(database.deflected.get()).as("the first /begin must have been refused by the server").isEqualTo(1);
        assertThat(executions.get()).as("the block must not run for the refused begin, and must run once after it")
            .isEqualTo(1);
        assertThat(database.countType("Person", false)).isEqualTo(1);
      }
    }

    /**
     * A refused {@code /commit} is answered before the handler runs, so the transaction never committed: the retry must
     * run the block again and leave exactly one record, not zero and not two.
     */
    @Test
    void aDeflectedCommitIsRetriedByTransaction() {
      try (final DeflectingDatabase database = newDatabase("commit", 1)) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        final AtomicInteger executions = new AtomicInteger();
        database.transaction(() -> {
          executions.incrementAndGet();
          database.command("sql", "INSERT INTO Person SET name = 'Alice'");
        }, false, 3);

        assertThat(database.deflected.get()).as("the first /commit must have been refused by the server").isEqualTo(1);
        assertThat(executions.get()).isEqualTo(2);
        assertThat(database.countType("Person", false)).as("only the attempt that committed may leave a record")
            .isEqualTo(1);
      }
    }

    /**
     * The raw calls hand the retryable type to their own caller too, instead of burying it as the cause of a
     * {@link TransactionException}.
     */
    @Test
    void aDeflectedBeginSurfacesAsNeedRetryException() {
      try (final DeflectingDatabase database = newDatabase("begin", 1)) {
        assertThatThrownBy(database::begin).isInstanceOf(NeedRetryException.class)
            .isNotInstanceOf(TransactionException.class);
        assertThat(database.isTransactionActive()).isFalse();

        // The window is closed again: the next begin goes through.
        database.begin();
        assertThat(database.isTransactionActive()).isTrue();
        database.rollback();
      }
    }

    /**
     * The budget is still a budget: a server that keeps refusing is given up on after the configured attempts, with the
     * retryable exception rather than a wrapped one.
     */
    @Test
    void aServerThatKeepsRefusingExhaustsTheBudget() {
      try (final DeflectingDatabase database = newDatabase("begin", Integer.MAX_VALUE)) {
        final AtomicInteger executions = new AtomicInteger();
        assertThatThrownBy(() -> database.transaction(executions::incrementAndGet, false, 3))
            .isInstanceOf(NeedRetryException.class);

        assertThat(database.deflected.get()).isEqualTo(3);
        assertThat(executions.get()).isZero();
      }
    }

    /**
     * The typed refusals the server sends with 503 are rebuilt as NeedRetryException SUBCLASSES, not by the untyped
     * 503 fallback the tests above exercise: they must reach the retry loop too, on both routes.
     */
    @Test
    void typedRetryableRefusalsAreRetriedOnBeginAndCommit() {
      final JSONObject notTheLeader = typedBody(ServerIsNotTheLeaderException.class.getName(), "not the leader")
          .put("exceptionArgs", "127.0.0.1:" + getServerHttpPort());
      final JSONObject quorum = typedBody(QuorumNotReachedException.class.getName(), "quorum not reached");

      for (final String route : new String[] { "begin", "commit" })
        for (final JSONObject body : new JSONObject[] { notTheLeader, quorum }) {
          try (final ScriptedDatabase database = newScriptedDatabase(route, 503, body)) {
            database.command("sql", "CREATE DOCUMENT TYPE Person IF NOT EXISTS");
            database.command("sql", "DELETE FROM Person");

            final AtomicInteger executions = new AtomicInteger();
            database.transaction(() -> {
              executions.incrementAndGet();
              database.command("sql", "INSERT INTO Person SET name = 'Alice'");
            }, false, 3);

            final String label = route + " refused with " + body.getString("exception");
            assertThat(database.answered.get()).as(label).isEqualTo(1);
            assertThat(executions.get()).as(label).isEqualTo(route.equals("begin") ? 1 : 2);
            assertThat(database.countType("Person", false)).as(label).isEqualTo(1);
          }
        }
    }

    /**
     * The other side of the commit invariant: an outcome that MAY have landed (a replication entry dispatched to the
     * log whose quorum wait timed out, answered 500) or DID land (answered 409) must not be retried, or the write would
     * be applied twice.
     */
    @Test
    void aCommitOutcomeThatMayHaveLandedIsNotRetried() {
      final Object[][] outcomes = {
          { 500, typedBody("com.arcadedb.server.ha.raft.ReplicationDispatchedTimeoutException", "dispatched, timed out") },
          { 409, typedBody("com.arcadedb.exception.TransactionCommittedRemotelyException", "committed cluster-wide") } };

      for (final Object[] outcome : outcomes)
        try (final ScriptedDatabase database = newScriptedDatabase("commit", (Integer) outcome[0], (JSONObject) outcome[1])) {
          database.command("sql", "CREATE DOCUMENT TYPE Person IF NOT EXISTS");

          final AtomicInteger executions = new AtomicInteger();
          final Throwable thrown = catchThrowable(() -> database.transaction(() -> {
            executions.incrementAndGet();
            database.command("sql", "INSERT INTO Person SET name = 'Alice'");
          }, false, 3));

          final String label = "commit answered " + outcome[0];
          assertThat(thrown).as(label).isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class);
          assertThat(executions.get()).as(label).isEqualTo(1);
        }
    }

    private static JSONObject typedBody(final String exception, final String detail) {
      return new JSONObject().put("error", detail).put("detail", detail).put("exception", exception);
    }

    private ScriptedDatabase newScriptedDatabase(final String route, final int status, final JSONObject body) {
      return new ScriptedDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, route, status, body.toString());
    }

    private DeflectingDatabase newDatabase(final String route, final int deflections) {
      return new DeflectingDatabase(getServer(0), "127.0.0.1", getServerHttpPort(), DATABASE_NAME, route, deflections);
    }

    /**
     * Opens the server's snapshot-install window for the first {@code deflections} requests to one transaction route,
     * so the server itself answers them 503, and counts them.
     */
    private static class DeflectingDatabase extends RemoteDatabase {
      final         AtomicInteger  deflected = new AtomicInteger();
      private final ArcadeDBServer server;
      private final String         route;
      private final AtomicInteger  remaining;

      DeflectingDatabase(final ArcadeDBServer server, final String host, final int port, final String databaseName,
          final String route, final int deflections) {
        super(host, port, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
        this.server = server;
        this.route = "/api/v1/" + route + "/";
        this.remaining = new AtomicInteger(deflections);
      }

      @Override
      protected void sleepBeforeRetry(final long delayMs) {
        // THE PACING OF THE RETRIES IS PINNED BY RemoteTransactionRetryIT.CommitSessionRelease: WAITING OUT THE SERVER'S REAL 5 SECOND
        // Retry-After HERE WOULD ONLY SLOW THE SUITE DOWN (ISSUE #8617)
      }

      @Override
      HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
        if (!request.uri().getPath().contains(route) || remaining.getAndDecrement() <= 0)
          return super.sendWithWatchdog(request);

        server.setSnapshotInstallInProgress(true);
        try {
          final HttpResponse<String> response = super.sendWithWatchdog(request);
          assertThat(response.statusCode()).as("the server must have deflected the request").isEqualTo(503);
          deflected.incrementAndGet();
          return response;
        } finally {
          server.setSnapshotInstallInProgress(false);
        }
      }
    }

    /**
     * Answers the first request to one transaction route with a scripted error response instead of sending it, for the
     * typed refusals a single node without HA cannot produce. The request never reaches the server, so a scripted
     * {@code /commit} leaves the transaction its {@code /begin} opened on the server until the session times out (the
     * gap tracked as #8618): its insert is never committed, and each iteration clears the type first regardless.
     */
    private static class ScriptedDatabase extends RemoteDatabase {
      final         AtomicInteger answered = new AtomicInteger();
      private final String        route;
      private final int           status;
      private final String        body;

      ScriptedDatabase(final String host, final int port, final String databaseName, final String route, final int status,
          final String body) {
        super(host, port, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
        this.route = "/api/v1/" + route + "/";
        this.status = status;
        this.body = body;
      }

      @Override
      protected void sleepBeforeRetry(final long delayMs) {
        // THE PACING OF THE RETRIES IS PINNED BY RemoteTransactionRetryIT.CommitSessionRelease: WAITING OUT THE SERVER'S REAL 5 SECOND
        // Retry-After HERE WOULD ONLY SLOW THE SUITE DOWN (ISSUE #8617)
      }

      @Override
      HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
        if (!request.uri().getPath().contains(route) || answered.get() > 0)
          return super.sendWithWatchdog(request);
        answered.incrementAndGet();
        return new ScriptedResponse(request, status, body);
      }
    }

    private record ScriptedResponse(HttpRequest request, int statusCode, String body) implements HttpResponse<String> {
      @Override
      public Optional<HttpResponse<String>> previousResponse() {
        return Optional.empty();
      }

      @Override
      public HttpHeaders headers() {
        return HttpHeaders.of(Map.of(), (name, value) -> true);
      }

      @Override
      public Optional<SSLSession> sslSession() {
        return Optional.empty();
      }

      @Override
      public URI uri() {
        return request.uri();
      }

      @Override
      public HttpClient.Version version() {
        return HttpClient.Version.HTTP_1_1;
      }
    }
  }

  /**
   * Regression for issue #8062, the follow-up to #7916: {@code RemoteDatabase.transaction(txBlock, join, attempts, ...)}
   * is the THIRD retry loop of that shape and was the one left without the partial-commit guard.
   * <p>
   * A block containing a statement with an explicit batch boundary - {@code UPDATE/DELETE/MOVE VERTEX ... BATCH n} -
   * commits part of its work mid-execution and re-begins straight afterwards. The rollback the loop performs before the
   * next attempt cannot take that part back, and nothing the remote client holds can see that it happened: its whole
   * view of the transaction is the session id, which does not change. The second attempt used to apply the already
   * durable half a SECOND time and then report clean success.
   * <p>
   * The two embedded loops detect this locally with {@code TransactionContext.isPartiallyCommitted(...)}, which the
   * remote client cannot call because it holds no {@code TransactionContext}. The server evaluates that same predicate
   * on the session's transaction and reports it in the {@link RemoteDatabase#ARCADEDB_SESSION_PARTIAL_COMMIT} response
   * header, which is what these tests drive end to end over a real HTTP server.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PartialCommitNotReplayed extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-database-8062";
    private static final String ROWS          = "Issue8062Row";
    private static final int    SIZE          = 25;
    private static final int    ATTEMPTS      = 3;

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    /**
     * The defect itself: with a BATCH boundary crossed before the conflict, the retry used to double-apply the rows
     * already published. Now the block is not re-run at all, so every row is incremented exactly once, and the conflict
     * reaches the caller instead of being swallowed into a clean return.
     */
    @Test
    void aRemoteBlockThatCommittedPartOfItsWorkIsNotReplayed() {
      try (final RemoteDatabase db = remoteDatabase()) {
        populate(db);

        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger okCalls = new AtomicInteger();

        assertThatThrownBy(() -> db.transaction(() -> {
          attempts.incrementAndGet();
          // Commits every 10 of the 25 rows: 20 are durable by the time the statement ends.
          db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10");
          // ... and then the block fails, the way a genuine MVCC conflict on a later statement would.
          throw new ConcurrentModificationException("simulated conflict after the batch boundary");
        }, true, ATTEMPTS, okCalls::incrementAndGet, null)).isInstanceOf(NeedRetryException.class);

        assertThat(attempts.get()).as("the block must be run ONCE: its durable half cannot be replayed").isEqualTo(1);
        assertThat(okCalls.get()).as("and a block that failed must not report success").isZero();

        assertThat(countRowsWithSeq(db, 1)).as("the 20 rows the batch published are incremented exactly once").isEqualTo(20);
        assertThat(countRowsWithSeq(db, 0)).as("the remaining 5 were rolled back").isEqualTo(5);
        assertThat(countRowsWithSeq(db, 2)).as("no row may be incremented twice").isZero();
      }
    }

    /**
     * {@code DELETE ... BATCH n} goes through the same {@code BatchStep}, so it earns the same refusal. Driven through
     * its own entry point rather than assumed from the UPDATE above.
     */
    @Test
    void aRemoteBatchedDeleteThatCommittedPartOfItsWorkIsNotReplayed() {
      try (final RemoteDatabase db = remoteDatabase()) {
        populate(db);

        final AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> db.transaction(() -> {
          attempts.incrementAndGet();
          db.command("sql", "DELETE FROM " + ROWS + " BATCH 10");
          throw new ConcurrentModificationException("simulated conflict after the batch boundary");
        }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

        assertThat(attempts.get()).isEqualTo(1);
        assertThat(countRowsWithSeq(db, 0)).as("20 rows are durably gone, the last 5 deletes were rolled back").isEqualTo(5);
      }
    }

    /**
     * The guard must not disturb the ordinary case it sits next to: a block that publishes NOTHING before failing is
     * still retried the full number of attempts, because the rollback really did take everything back. This is the test
     * that fails if the header is latched on the wrong request or never cleared by {@code begin()}.
     */
    @Test
    void aRemoteBlockThatCommittedNothingIsStillRetried() {
      try (final RemoteDatabase db = remoteDatabase()) {
        populate(db);

        final AtomicInteger attempts = new AtomicInteger();

        assertThatThrownBy(() -> db.transaction(() -> {
          attempts.incrementAndGet();
          db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1");
          throw new ConcurrentModificationException("simulated conflict with nothing published");
        }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

        assertThat(attempts.get()).as("nothing was durable, so all the attempts are still available").isEqualTo(ATTEMPTS);
        assertThat(countRowsWithSeq(db, 0)).as("and every attempt's rollback took its work back").isEqualTo(SIZE);
      }
    }

    /**
     * And the verdict does not survive the transaction it was reached in: a block that crosses a batch boundary and
     * SUCCEEDS leaves the next transaction on the same client free to retry as it always could. Without the reset in
     * {@code begin()} the latch set here would silently disable retries for the rest of the connection's life.
     */
    @Test
    void aLaterTransactionOnTheSameClientStillRetries() {
      try (final RemoteDatabase db = remoteDatabase()) {
        populate(db);

        db.transaction(() -> db.command("sql", "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10").close());
        assertThat(countRowsWithSeq(db, 1)).isEqualTo(SIZE);

        final AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> db.transaction(() -> {
          attempts.incrementAndGet();
          throw new ConcurrentModificationException("simulated conflict in a brand new transaction");
        }, true, ATTEMPTS, null, null)).isInstanceOf(NeedRetryException.class);

        assertThat(attempts.get()).as("the new transaction published nothing, so its attempts are intact")
            .isEqualTo(ATTEMPTS);
      }
    }

    /**
     * The server half of the fix, driven on the wire rather than inferred from the client's behaviour: the
     * {@code /command} response of a statement with a BATCH boundary, run inside a session, carries the header -
     * and the {@code /commit} that ends the same session does NOT, although committing is exactly what moves the
     * counter the header is derived from (see {@code DatabaseAbstractHandler.reportsSessionPartialCommit()}).
     */
    @Test
    void theCommandResponseCarriesTheHeaderAndTheCommitResponseDoesNot() throws Exception {
      try (final RemoteDatabase db = remoteDatabase()) {
        populate(db);
      }

      final String sessionId = post("begin/" + DATABASE_NAME, null, null)
          .getHeaderField(RemoteDatabase.ARCADEDB_SESSION_ID);
      assertThat(sessionId).as("the /begin response names the session").isNotNull();

      final HttpURLConnection batched = post("command/" + DATABASE_NAME,
          "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10", sessionId.trim());
      assertThat(batched.getResponseCode()).isEqualTo(200);
      assertThat(batched.getHeaderField(RemoteDatabase.ARCADEDB_SESSION_PARTIAL_COMMIT))
          .as("the batch boundary published 20 of the 25 rows under the caller's transaction").isEqualTo("true");

      final HttpURLConnection committed = post("commit/" + DATABASE_NAME, null, sessionId.trim());
      assertThat(committed.getResponseCode()).isEqualTo(204);
      assertThat(committed.getHeaderField(RemoteDatabase.ARCADEDB_SESSION_PARTIAL_COMMIT))
          .as("committing in full is not a PARTIAL commit, however much it moves the counter").isNull();
    }

    /** A plain command OUTSIDE any session can never carry the header: there is no caller transaction to report on. */
    @Test
    void aSessionLessCommandNeverCarriesTheHeader() throws Exception {
      try (final RemoteDatabase db = remoteDatabase()) {
        populate(db);
      }

      final HttpURLConnection batched = post("command/" + DATABASE_NAME,
          "UPDATE " + ROWS + " SET seq = seq + 1 BATCH 10", null);
      assertThat(batched.getResponseCode()).isEqualTo(200);
      assertThat(batched.getHeaderField(RemoteDatabase.ARCADEDB_SESSION_PARTIAL_COMMIT)).isNull();
    }

    /**
     * A raw send rather than {@link RemoteDatabase}, because these two tests are about the HEADERS on the wire and
     * the driver does not expose them. The JSON body is built by {@link BaseGraphServerTest#formatPayload}, the
     * helper every other raw-HTTP test in this module uses; only the session header and the
     * {@code getServerHttpPort()} URL are assembled here, and neither has a helper to borrow.
     *
     * @param command the SQL to send as the request body, or null for a body-less endpoint such as /begin
     */
    private HttpURLConnection post(final String path, final String command, final String sessionId) throws Exception {
      final HttpURLConnection connection = (HttpURLConnection) URI.create(
          "http://127.0.0.1:" + getServerHttpPort() + "/api/v1/" + path).toURL().openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
          .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      if (sessionId != null)
        connection.setRequestProperty(RemoteDatabase.ARCADEDB_SESSION_ID, sessionId);
      if (command != null) {
        connection.setRequestProperty("Content-Type", "application/json");
        formatPayload(connection, "sql", command, null, null);
      }
      connection.connect();
      // Force the response to be read so the headers are populated whatever the status.
      connection.getResponseCode();
      return connection;
    }

    private void populate(final RemoteDatabase db) {
      // The database is created fresh for every test, so no IF NOT EXISTS guard is needed here.
      db.command("sql", "CREATE DOCUMENT TYPE " + ROWS).close();
      db.command("sql", "CREATE PROPERTY " + ROWS + ".seq INTEGER").close();
      db.transaction(() -> {
        for (int i = 0; i < SIZE; i++)
          db.command("sql", "INSERT INTO " + ROWS + " SET seq = 0").close();
      });
    }

    private long countRowsWithSeq(final RemoteDatabase db, final int seq) {
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + ROWS + " WHERE seq = ?", seq)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private RemoteDatabase remoteDatabase() {
      return new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }
  }

  /**
   * Issues #8618 and #8617 against a real server.
   * <p>
   * #8618: a {@code /commit} refused before its handler ran (the 503 a node installing a snapshot answers) left the
   * session's transaction open on the server, while {@link RemoteDatabase#commit()} cleared the client's session id, so the
   * retry loop could not release it: one abandoned session per deflected attempt, until the idle timeout. A commit that
   * failed INSIDE the handler left its session registered too, empty, for the same timeout.
   * <p>
   * #8617: the retry loop re-issued a deflected attempt immediately, ignoring the {@code Retry-After} the server sent.
   * <p>
   * The deflection is the real one, from {@code AbstractServerHttpHandler}'s gate, opened around exactly the request the
   * test wants refused. The pauses the retry loop asks for are recorded instead of slept.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CommitSessionRelease extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-tx-8618";

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }

    @Test
    void aDeflectedCommitLeavesNoSessionBehind() {
      try (final DeflectingDatabase database = newDatabase("commit", 1)) {
        database.command("sql", "CREATE DOCUMENT TYPE Person");

        final AtomicInteger executions = new AtomicInteger();
        database.transaction(() -> {
          executions.incrementAndGet();
          database.command("sql", "INSERT INTO Person SET name = 'Alice'");
        }, false, 3);

        assertThat(database.deflected.get()).isEqualTo(1);
        assertThat(executions.get()).isEqualTo(2);
        assertThat(database.countType("Person", false)).isEqualTo(1);
        assertThat(activeSessions()).as("the session of the refused commit must have been released").isZero();
        // THE SERVER'S Retry-After (#8617), READ FROM THE REAL ANSWER, SPREAD BY UP TO A TENTH OF IT
        assertRetryAfterPauses(database.pauses, 1);
      }
    }

    @Test
    void aDeflectedBeginWaitsTheRetryAfterTheServerSent() {
      try (final DeflectingDatabase database = newDatabase("begin", 2)) {
        final AtomicInteger executions = new AtomicInteger();
        database.transaction(executions::incrementAndGet, false, 3);

        assertThat(database.deflected.get()).isEqualTo(2);
        assertThat(executions.get()).isEqualTo(1);
        assertRetryAfterPauses(database.pauses, 2);
        assertThat(activeSessions()).isZero();
      }
    }

    /**
     * The server-side half: a commit that fails inside the handler ends its session, with no help from the client, and
     * says so in the answer, so the client sends no rollback to release it. The client here would not send one anyway.
     */
    @Test
    void aCommitThatFailsInsideTheHandlerEndsItsSession() {
      try (final DeflectingDatabase database = newDatabase("none", 0)) {
        database.command("sql", "CREATE DOCUMENT TYPE Account");
        database.command("sql", "CREATE PROPERTY Account.id INTEGER");
        database.command("sql", "CREATE INDEX ON Account (id) UNIQUE");

        database.releaseSessions = false;
        database.begin();
        database.command("sql", "INSERT INTO Account SET id = 1");
        assertThat(activeSessions()).isEqualTo(1);

        // ANOTHER CLIENT COMMITS THE SAME KEY FIRST
        try (final RemoteDatabase other = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
            DEFAULT_PASSWORD_FOR_TESTS)) {
          other.command("sql", "INSERT INTO Account SET id = 1");
        }

        assertThatThrownBy(database::commit).isInstanceOf(DuplicatedKeyException.class);
        assertThat(activeSessions()).as("a failed commit must end its session on the server").isZero();
        assertThat(database.lastCommitAnswer.headers().firstValue(RemoteDatabase.ARCADEDB_SESSION_CLOSED))
            .as("the failed commit's answer must say the session is closed").hasValue("true");
        assertThat(database.rollbacks.get()).as("the server said the session is closed: no rollback to send").isZero();
      }
    }

    private static void assertRetryAfterPauses(final List<Long> pauses, final int expected) {
      final long retryAfterMs = RetryLaterException.SNAPSHOT_INSTALL_RETRY_AFTER_SECONDS * 1_000L;
      assertThat(pauses).hasSize(expected);
      for (final long pause : pauses)
        assertThat(pause).isBetween(retryAfterMs, retryAfterMs + retryAfterMs / 10);
    }

    private int activeSessions() {
      return getServer(0).getHttpServer().getSessionManager().getActiveSessions();
    }

    private DeflectingDatabase newDatabase(final String route, final int deflections) {
      return new DeflectingDatabase(getServer(0), "127.0.0.1", getServerHttpPort(), DATABASE_NAME, route, deflections);
    }

    /**
     * Opens the server's snapshot-install window for the first {@code deflections} requests to one transaction route, so
     * the server itself answers them 503, and records the pauses of the retry loop instead of sleeping them.
     */
    private static class DeflectingDatabase extends RemoteDatabase {
      final         AtomicInteger  deflected        = new AtomicInteger();
      final         AtomicInteger  rollbacks        = new AtomicInteger();
      final         List<Long>     pauses           = new ArrayList<>();
      private final ArcadeDBServer server;
      private final String         route;
      private final AtomicInteger  remaining;
      boolean                      releaseSessions  = true;
      volatile HttpResponse<String> lastCommitAnswer;

      DeflectingDatabase(final ArcadeDBServer server, final String host, final int port, final String databaseName,
          final String route, final int deflections) {
        super(host, port, databaseName, "root", DEFAULT_PASSWORD_FOR_TESTS);
        this.server = server;
        this.route = "/api/v1/" + route + "/";
        this.remaining = new AtomicInteger(deflections);
      }

      @Override
      public void rollback() {
        rollbacks.incrementAndGet();
        if (releaseSessions) {
          super.rollback();
          return;
        }
        setSessionId(null);
      }

      @Override
      protected void sleepBeforeRetry(final long delayMs) {
        pauses.add(delayMs);
      }

      @Override
      HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
        if (!request.uri().getPath().contains(route) || remaining.getAndDecrement() <= 0) {
          final HttpResponse<String> response = super.sendWithWatchdog(request);
          if (request.uri().getPath().contains("/api/v1/commit/"))
            lastCommitAnswer = response;
          return response;
        }

        server.setSnapshotInstallInProgress(true);
        try {
          final HttpResponse<String> response = super.sendWithWatchdog(request);
          assertThat(response.statusCode()).as("the server must have deflected the request").isEqualTo(503);
          deflected.incrementAndGet();
          return response;
        } finally {
          server.setSnapshotInstallInProgress(false);
        }
      }
    }
  }
}
