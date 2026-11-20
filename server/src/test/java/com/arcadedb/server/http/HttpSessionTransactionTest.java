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
package com.arcadedb.server.http;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The HTTP transaction session ({@link HttpSession}, {@link HttpSessionManager}): its lifecycle, ownership, the memory
 * of committed sessions, and which refusals may or may not roll back the transaction the caller opened with {@code /begin}.
 */
class HttpSessionTransactionTest {
  // Regression: #10164 bounds and owner gate of the committed-session memory
  /**
   * Issue #10164: the session manager remembers, for a bounded time and number, which sessions a /commit ended, so a
   * retried commit of a committed session still answers 204 while a commit naming any other unresolvable session answers
   * 404. These pin the bounds and the owner gate of that memory.
   */
  @Nested
  class CommittedSessionMemory {
    private static final long TIMEOUT_MS = 600_000L;

    private HttpSessionManager sessionManager;

    private static ServerSecurityUser user(final String name) {
      return new ServerSecurityUser(null, new JSONObject().put("name", name));
    }

    @AfterEach
    void tearDown() {
      if (sessionManager != null)
        sessionManager.close();
    }

    @Test
    void rememberedOnlyForItsOwner() {
      sessionManager = new HttpSessionManager(TIMEOUT_MS);
      sessionManager.recordCommitted("AS-1", user("alice"));

      assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isTrue();
      assertThat(sessionManager.wasCommittedBy(user("bob"), "AS-1")).isFalse();
      assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-2")).isFalse();
      assertThat(sessionManager.wasCommittedBy(null, "AS-1")).isFalse();
    }

    @Test
    void forgottenAfterTheSessionTimeout() {
      sessionManager = new HttpSessionManager(TIMEOUT_MS);
      sessionManager.recordCommitted("AS-1", user("alice"));

      sessionManager.pruneCommittedSessions(System.currentTimeMillis() + TIMEOUT_MS + 1);

      assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isFalse();
    }

    @Test
    void theIdleSweepPrunesExpiredCommits() {
      // A short timeout, so the manager's own timer runs the sweep
      sessionManager = new HttpSessionManager(50L);
      sessionManager.recordCommitted("AS-1", user("alice"));

      await().atMost(10, TimeUnit.SECONDS).until(() -> sessionManager.getRememberedCommits() == 0);
    }

    @Test
    void boundedInNumber() {
      sessionManager = new HttpSessionManager(TIMEOUT_MS);
      for (int i = 0; i <= HttpSessionManager.MAX_REMEMBERED_COMMITS; i++)
        sessionManager.recordCommitted("AS-" + i, user("alice"));

      assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-0")).as("the oldest is evicted").isFalse();
      assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isTrue();
      assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-" + HttpSessionManager.MAX_REMEMBERED_COMMITS)).isTrue();
    }

    @Test
    void droppingTheUserForgetsItsCommits() {
      sessionManager = new HttpSessionManager(TIMEOUT_MS);
      sessionManager.recordCommitted("AS-1", user("alice"));
      sessionManager.recordCommitted("AS-2", user("bob"));

      sessionManager.removeSessionsForUser("alice");

      assertThat(sessionManager.wasCommittedBy(user("alice"), "AS-1")).isFalse();
      assertThat(sessionManager.wasCommittedBy(user("bob"), "AS-2")).isTrue();
    }
  }

  // Regression: #5026 swept session must not run its callback; sessions are owner-gated
  /**
   * Regression tests for GitHub issue #5026 (server audit, HTTP transaction-session lifecycle). Covers the
   * two lower-level defects that can be exercised directly against {@link HttpSessionManager}/{@link HttpSession}
   * without a running HTTP server:
   * <ul>
   *   <li>Defect 2: the idle-timeout sweep can remove a session between {@code getSessionById()} and
   *   {@link HttpSession#execute} acquiring the session lock; {@code execute} must re-validate the session is
   *   still registered and refuse to run its callback on a swept (rolled-back) transaction.</li>
   *   <li>Defect 3: {@link HttpSessionManager#getSessionById} must enforce ownership (a different user cannot
   *   resolve another user's session), and a user's live sessions must be invalidatable
   *   ({@link HttpSessionManager#removeSessionsForUser}) so a dropped-and-recreated same-name principal cannot
   *   adopt the prior principal's still-open session.</li>
   * </ul>
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/5026">GitHub Issue #5026</a>
   */
  @Nested
  class SessionLifecycle {
    private static final long TIMEOUT_MS = 600_000L;

    private DatabaseInternal   database;
    private File               databaseDirectory;
    private HttpSessionManager sessionManager;

    private DatabaseInternal createDatabase() {
      databaseDirectory = new File("./target/databases/HttpSessionTransactionTest-SessionLifecycle-" + UUID.randomUUID());
      try (final Database db = new DatabaseFactory(databaseDirectory.getPath()).create()) {
        db.getSchema().createDocumentType("Doc");
      }
      return (DatabaseInternal) new DatabaseFactory(databaseDirectory.getPath()).open();
    }

    // ServerSecurityUser.equals()/getName() (the only members the session manager touches) never dereference
    // the ArcadeDBServer field, so a real instance can be built without a running server.
    private ServerSecurityUser createUser(final String name) {
      return new ServerSecurityUser(null, new JSONObject().put("name", name));
    }

    private TransactionContext beginTx() {
      final TransactionContext tx = new TransactionContext(database);
      tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      return tx;
    }

    @AfterEach
    void tearDown() {
      if (sessionManager != null)
        sessionManager.close();
      if (database != null && database.isOpen())
        database.close();
      if (databaseDirectory != null)
        FileUtils.deleteRecursively(databaseDirectory);
    }

    // Defect 2: a session removed from the manager (e.g. by the idle sweep) between lookup and lock acquisition
    // must not have its callback run; execute() re-validates registration under the lock.
    @Test
    void executeRefusesToRunOnAnUnregisteredSession() throws Exception {
      database = createDatabase();
      sessionManager = new HttpSessionManager(TIMEOUT_MS);

      final TransactionContext tx = beginTx();
      final ServerSecurityUser user = createUser("alice");
      final HttpSession session = sessionManager.createSession(user, tx);

      // Simulate the idle sweep having rolled back and removed the session after this request looked it up.
      tx.rollback();
      sessionManager.removeSession(session.id);

      final AtomicBoolean callbackRan = new AtomicBoolean(false);
      assertThatThrownBy(() -> session.execute(user, () -> {
        callbackRan.set(true);
        return null;
      })).isInstanceOf(HttpSessionException.class);

      assertThat(callbackRan).as("callback must not run on a session that was removed before the lock was taken").isFalse();
    }

    // A session that is still registered runs its callback normally (guards against the re-validation being too
    // aggressive and breaking the happy path).
    @Test
    void executeRunsCallbackWhenSessionStillRegistered() throws Exception {
      database = createDatabase();
      sessionManager = new HttpSessionManager(TIMEOUT_MS);

      final TransactionContext tx = beginTx();
      final ServerSecurityUser user = createUser("alice");
      final HttpSession session = sessionManager.createSession(user, tx);

      final AtomicBoolean callbackRan = new AtomicBoolean(false);
      session.execute(user, () -> {
        callbackRan.set(true);
        return null;
      });

      assertThat(callbackRan).isTrue();
    }

    // Defect 3: getSessionById must not hand a session to a different user.
    @Test
    void getSessionByIdEnforcesOwnership() {
      database = createDatabase();
      sessionManager = new HttpSessionManager(TIMEOUT_MS);

      final ServerSecurityUser alice = createUser("alice");
      final ServerSecurityUser bob = createUser("bob");
      final HttpSession session = sessionManager.createSession(alice, beginTx());

      assertThat(sessionManager.getSessionById(alice, session.id)).as("owner resolves its own session").isSameAs(session);
      assertThat(sessionManager.getSessionById(bob, session.id)).as("a different user cannot resolve the session").isNull();
    }

    // Defect 3: removeSessionsForUser rolls back and removes only the target user's sessions.
    @Test
    void removeSessionsForUserInvalidatesOnlyThatUsersSessions() {
      database = createDatabase();
      sessionManager = new HttpSessionManager(TIMEOUT_MS);

      final ServerSecurityUser alice = createUser("alice");
      final ServerSecurityUser bob = createUser("bob");
      final TransactionContext aliceTx = beginTx();
      final TransactionContext bobTx = beginTx();
      final HttpSession aliceSession = sessionManager.createSession(alice, aliceTx);
      final HttpSession bobSession = sessionManager.createSession(bob, bobTx);

      final int removed = sessionManager.removeSessionsForUser("alice");

      assertThat(removed).as("only alice's session removed").isEqualTo(1);
      assertThat(aliceTx.isActive()).as("alice's transaction rolled back").isFalse();
      assertThat(bobTx.isActive()).as("bob's transaction untouched").isTrue();
      assertThat(sessionManager.getActiveSessions()).isEqualTo(1);
      assertThat(sessionManager.getSessionById(alice, aliceSession.id)).isNull();
      assertThat(sessionManager.getSessionById(bob, bobSession.id)).isSameAs(bobSession);
    }

    // Defect 3: after invalidation a recreated same-name principal (a fresh ServerSecurityUser instance) cannot
    // adopt the prior principal's session id.
    @Test
    void recreatedSameNameUserCannotAdoptPriorSession() {
      database = createDatabase();
      sessionManager = new HttpSessionManager(TIMEOUT_MS);

      final ServerSecurityUser alice = createUser("alice");
      final HttpSession aliceSession = sessionManager.createSession(alice, beginTx());

      // Ownership is by name, so BEFORE invalidation a same-name principal WOULD resolve the session. This
      // pins the invariant that invalidation-on-drop (removing the session), not the identity check, is the
      // real guard against same-name adoption.
      assertThat(sessionManager.getSessionById(createUser("alice"), aliceSession.id))
          .as("a same-name principal resolves the session until it is invalidated").isSameAs(aliceSession);

      // User is dropped: invalidate its live sessions.
      sessionManager.removeSessionsForUser("alice");

      // A brand new principal happens to reuse the same name.
      final ServerSecurityUser recreatedAlice = createUser("alice");
      assertThat(sessionManager.getSessionById(recreatedAlice, aliceSession.id))
          .as("recreated same-name user must not adopt the prior principal's session").isNull();
    }
  }

  // Regression: #9312 a rolled-back session autocommitted joining /ws inserts
  /**
   * Issue #9312: once a failed command rolled the session's transaction back, the refusal of every request that would run in it
   * was keyed on {@code rollbackOnFailure}, so the /ws insert session (which passes false, to not destroy a transaction on its
   * own refusals, yet writes into that transaction) autocommitted its rows into the gap. The refusal now keys on whether the
   * request joins the transaction.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RolledBackSessionRefusesJoiningWork {
    private DatabaseInternal   database;
    private File               databaseDirectory;
    private HttpSessionManager sessionManager;

    private HttpSession rolledBackSession(final ServerSecurityUser user) throws Exception {
      databaseDirectory = new File("./target/databases/Issue9312-" + UUID.randomUUID());
      try (final Database db = new DatabaseFactory(databaseDirectory.getPath()).create()) {
        db.getSchema().createDocumentType("Doc");
      }
      database = (DatabaseInternal) new DatabaseFactory(databaseDirectory.getPath()).open();
      sessionManager = new HttpSessionManager(600_000L);

      final TransactionContext tx = new TransactionContext(database);
      tx.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      final HttpSession session = sessionManager.createSession(user, tx);
      DatabaseContext.INSTANCE.init(database, tx);

      // a command that writes and then fails: the session rolls the transaction back
      assertThatThrownBy(() -> session.execute(user, () -> {
        database.newDocument("Doc").set("n", 1).save();
        throw new IllegalStateException("boom");
      })).isInstanceOf(IllegalStateException.class);
      assertThat(session.isRolledBackByFailure()).isTrue();
      return session;
    }

    @AfterEach
    void tearDown() {
      if (sessionManager != null)
        sessionManager.close();
      if (database != null && database.isOpen())
        database.close();
      if (databaseDirectory != null)
        FileUtils.deleteRecursively(databaseDirectory);
    }

    @Test
    void aRequestThatJoinsTheTransactionIsRefusedEvenWhenItDoesNotRollBackOnFailure() throws Exception {
      final ServerSecurityUser user = new ServerSecurityUser(null, new JSONObject().put("name", "u"));
      final HttpSession session = rolledBackSession(user);
      final AtomicInteger ran = new AtomicInteger();

      assertThatThrownBy(() -> session.execute(user, () -> {
        ran.incrementAndGet();
        return null;
      }, false, false, true)).isInstanceOf(HttpSessionException.class).hasMessageContaining("rolled back");

      assertThat(ran.get()).as("the work never ran, so nothing autocommitted").isZero();
    }

    @Test
    void aRequestThatDoesNotJoinTheTransactionStillRuns() throws Exception {
      final ServerSecurityUser user = new ServerSecurityUser(null, new JSONObject().put("name", "u"));
      final HttpSession session = rolledBackSession(user);
      final AtomicInteger ran = new AtomicInteger();

      session.execute(user, () -> {
        ran.incrementAndGet();
        return null;
      }, false, false, false);

      assertThat(ran.get()).isEqualTo(1);
    }

    @Test
    void theEndingRoutesStillRun() throws Exception {
      final ServerSecurityUser user = new ServerSecurityUser(null, new JSONObject().put("name", "u"));
      final HttpSession session = rolledBackSession(user);
      final AtomicInteger ran = new AtomicInteger();

      session.execute(user, () -> {
        ran.incrementAndGet();
        return null;
      }, true, true);

      assertThat(ran.get()).isEqualTo(1);
    }
  }

  // Regression: #9784 a failed query destroyed the caller's /begin transaction
  /**
   * Issue #9784: {@code GET /api/v1/query} and {@code POST /api/v1/query} inherited the default of
   * {@code DatabaseAbstractHandler.participatesInSessionTransaction()}, whose {@code true} makes
   * {@code HttpSession.execute} roll the caller's transaction back when the request fails. The justification of that default - the
   * command failed part way, so what it was running in cannot be trusted - does not hold for a request that provably wrote
   * nothing: a 413 raised after a read-only statement has run, and a 400 raised before the statement is even handed to the engine.
   * Both destroyed a transaction the client had opened with {@code /begin}, and since #9006 the later {@code /commit} was refused
   * instead of reporting success, which made the loss visible but did not give the work back.
   * <p>
   * Two questions were folded into one flag, and they are separate: whether a request RUNS INSIDE the caller's transaction (a query
   * must, for read-your-writes, and it must be refused once a failed command has rolled the transaction back, #9312), and whether
   * its failure DESTROYS that transaction. The query routes keep the first answer and give up the second.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class QueryRoutesKeepSessionTransaction extends BaseGraphServerTest {
    private static final String DOC_TYPE = "Doc9784";
    private static final String NDJSON   = "application/x-ndjson";

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static String rootAuth() {
      return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }

    private HttpResponse<String> send(final String method, final String path, final String sessionId, final String accept,
        final String body) throws IOException, InterruptedException {
      final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1" + path)))
          .timeout(Duration.ofSeconds(30)).header("Authorization", rootAuth());
      if (sessionId != null)
        builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
      if (accept != null)
        builder.header("Accept", accept);
      if (body != null)
        builder.header("Content-Type", "application/json");
      builder.method(method, body != null ? HttpRequest.BodyPublishers.ofString(body) : HttpRequest.BodyPublishers.noBody());
      return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String json(final String command, final Integer limit, final String serializer) {
      final JSONObject payload = new JSONObject().put("language", "sql").put("command", command);
      if (limit != null)
        payload.put("limit", limit);
      if (serializer != null)
        payload.put("serializer", serializer);
      return payload.toString();
    }

    private String begin() throws Exception {
      final HttpResponse<String> begun = send("POST", "/begin/" + getDatabaseName(), null, null, "");
      assertThat(begun.statusCode()).isEqualTo(204);
      final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
      assertThat(sessionId).isNotBlank();
      return sessionId;
    }

    private void rollback(final String sessionId) throws Exception {
      send("POST", "/rollback/" + getDatabaseName(), sessionId, null, "");
    }

    private HttpResponse<String> command(final String sessionId, final String sql) throws Exception {
      return send("POST", "/command/" + getDatabaseName(), sessionId, null, json(sql, null, null));
    }

    /** The transaction under test: three documents written inside the session, which a read inside it sees (read-your-writes). */
    private String beginWithWitnesses() throws Exception {
      assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);
      final String sessionId = begin();
      for (int i = 0; i < 3; i++)
        assertThat(command(sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'witness" + i + "'").statusCode()).isEqualTo(200);
      return sessionId;
    }

    private long countInSession(final String sessionId) throws Exception {
      final HttpResponse<String> response = command(sessionId, "SELECT count(*) AS cnt FROM " + DOC_TYPE);
      assertThat(response.statusCode()).as("the session must still be usable: %s", response.body()).isEqualTo(200);
      return new JSONObject(response.body()).getJSONArray("result").getJSONObject(0).getLong("cnt");
    }

    private long committedCount() {
      final Database database = getServerDatabase(0, getDatabaseName());
      return database.countType(DOC_TYPE, false);
    }

    /** The session still holds its three documents and commits them: the work was never lost. */
    private void assertTransactionIntactAndCommit(final String sessionId, final String why) throws Exception {
      assertThat(countInSession(sessionId)).as(why).isEqualTo(3L);
      final HttpResponse<String> committed = send("POST", "/commit/" + getDatabaseName(), sessionId, null, "");
      assertThat(committed.statusCode()).as("the commit must be accepted: %s", committed.body()).isEqualTo(204);
      assertThat(committedCount()).as("and the witnesses are in the database").isEqualTo(3L);
    }

    private static String encoded(final String text) {
      return URLEncoder.encode(text, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /** Runs {@code check} with the result ceiling at 2, below the three rows the session has, restoring it afterwards. */
    private void withLowCeiling(final ThrowingConsumer check) throws Exception {
      final int ceiling = getServer(0).getConfiguration().getValueAsInteger(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS);
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, 2);
      String sessionId = null;
      try {
        sessionId = beginWithWitnesses();
        check.accept(sessionId);
      } finally {
        getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_QUERY_MAX_RESULT_ROWS, ceiling);
        if (sessionId != null)
          rollback(sessionId);
      }
    }

    @Test
    void getQueryRefusedForTooManyRowsKeepsTheSessionTransaction() throws Exception {
      withLowCeiling(sessionId -> {
        final HttpResponse<String> refused = send("GET",
            "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT FROM " + DOC_TYPE) + "?limit=1000000", sessionId, null, null);
        assertThat(refused.statusCode()).as("the ceiling cuts the result: %s", refused.body()).isEqualTo(413);

        assertTransactionIntactAndCommit(sessionId, "a read that declines to answer must not destroy the write it was read inside");
      });
    }

    @Test
    void postQueryRefusedForTooManyRowsKeepsTheSessionTransaction() throws Exception {
      withLowCeiling(sessionId -> {
        final HttpResponse<String> refused = send("POST", "/query/" + getDatabaseName(), sessionId, null,
            json("SELECT FROM " + DOC_TYPE, 1_000_000, null));
        assertThat(refused.statusCode()).as("the ceiling cuts the result: %s", refused.body()).isEqualTo(413);

        assertTransactionIntactAndCommit(sessionId, "a read that declines to answer must not destroy the write it was read inside");
      });
    }

    @Test
    void getQueryRefusedBeforeTheStatementRunsKeepsTheSessionTransaction() throws Exception {
      final String sessionId = beginWithWitnesses();
      try {
        // the 'graph' serializer has no row stream: refused with a 400 before the query is handed to the engine
        final HttpResponse<String> refused = send("GET",
            "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT FROM " + DOC_TYPE) + "?serializer=graph", sessionId, NDJSON, null);
        assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

        assertTransactionIntactAndCommit(sessionId, "a request refused before the engine was touched must not destroy the transaction");
      } finally {
        rollback(sessionId);
      }
    }

    @Test
    void postQueryRefusedBeforeTheStatementRunsKeepsTheSessionTransaction() throws Exception {
      final String sessionId = beginWithWitnesses();
      try {
        final HttpResponse<String> refused = send("POST", "/query/" + getDatabaseName(), sessionId, NDJSON,
            json("SELECT FROM " + DOC_TYPE, null, "graph"));
        assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

        assertTransactionIntactAndCommit(sessionId, "a request refused before the engine was touched must not destroy the transaction");
      } finally {
        rollback(sessionId);
      }
    }

    /**
     * {@code POST /command} reaches the same two streaming preconditions before it hands the statement to the engine, and they are
     * the same kind of refusal: nothing ran, so nothing in the transaction can be untrustworthy.
     */
    @Test
    void commandRefusedBeforeTheStatementRunsKeepsTheSessionTransaction() throws Exception {
      final String sessionId = beginWithWitnesses();
      try {
        // a write cannot be streamed: refused up front, and the INSERT never runs
        final HttpResponse<String> refused = send("POST", "/command/" + getDatabaseName(), sessionId, NDJSON,
            json("INSERT INTO " + DOC_TYPE + " SET name = 'never'", null, null));
        assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

        assertTransactionIntactAndCommit(sessionId, "a command refused before the engine was touched must not destroy the transaction");
      } finally {
        rollback(sessionId);
      }
    }

    /** The fourth place a request is refused before the engine is touched: a streamed response has no envelope to profile in. */
    @Test
    void commandAskingToProfileAStreamedResponseIsRefusedAndKeepsTheSessionTransaction() throws Exception {
      final String sessionId = beginWithWitnesses();
      try {
        final String body = new JSONObject().put("language", "sql").put("command", "SELECT FROM " + DOC_TYPE)
            .put("profileExecution", "basic").toString();
        final HttpResponse<String> refused = send("POST", "/command/" + getDatabaseName(), sessionId, NDJSON, body);
        assertThat(refused.statusCode()).as("%s", refused.body()).isEqualTo(400);

        assertTransactionIntactAndCommit(sessionId, "a request refused before the engine was touched must not destroy the transaction");
      } finally {
        rollback(sessionId);
      }
    }

    /**
     * The invariant the query routes' lack of a rollback relies on: {@code database.query} accepts idempotent statements only, so a
     * query that calls a function which writes is refused and leaves NOTHING in the session transaction - not the three documents
     * the caller wrote before it destroyed, and not the one the function tried to add.
     */
    @Test
    void aQueryCallingAFunctionThatWritesIsRefusedAndLeavesTheSessionTransactionUntouched() throws Exception {
      assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);
      assertThat(command(null, "DEFINE FUNCTION fw9784.add \"INSERT INTO " + DOC_TYPE + " SET name = 'smuggled'\" LANGUAGE sql")
          .statusCode()).isEqualTo(200);
      final String sessionId = beginWithWitnesses();
      try {
        final HttpResponse<String> viaGet = send("GET",
            "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT fw9784.add() AS r"), sessionId, null, null);
        assertThat(viaGet.statusCode()).as("%s", viaGet.body()).isGreaterThanOrEqualTo(400);
        final HttpResponse<String> viaPost = send("POST", "/query/" + getDatabaseName(), sessionId, null,
            json("SELECT fw9784.add() AS r", null, null));
        assertThat(viaPost.statusCode()).as("%s", viaPost.body()).isGreaterThanOrEqualTo(400);

        // exactly the three witnesses: the function wrote nothing, and the refusals did not roll anything back
        assertTransactionIntactAndCommit(sessionId, "a refused query must neither write nor destroy what the caller wrote");
      } finally {
        rollback(sessionId);
      }
    }

    /**
     * The counter-case that keeps the tests above meaningful: a command that fails INSIDE the engine, after it was handed the
     * statement, still rolls the transaction back - that is what a half-applied command needs. And a query sent to a session in
     * that state is still refused (#9312) instead of running outside the transaction it believes it is in.
     */
    @Test
    void aCommandFailingInTheEngineStillRollsTheSessionTransactionBack() throws Exception {
      final String sessionId = beginWithWitnesses();
      try {
        final HttpResponse<String> failed = command(sessionId, "INSERT INTO NoSuchType9784 SET name = 'x'");
        assertThat(failed.statusCode()).as("%s", failed.body()).isGreaterThanOrEqualTo(400);

        final HttpResponse<String> query = send("GET",
            "/query/" + getDatabaseName() + "/sql/" + encoded("SELECT FROM " + DOC_TYPE), sessionId, null, null);
        assertThat(query.statusCode()).as("a query must not run outside a transaction that was rolled back: %s", query.body())
            .isGreaterThanOrEqualTo(400);

        final HttpResponse<String> commit = send("POST", "/commit/" + getDatabaseName(), sessionId, null, "");
        assertThat(commit.statusCode()).as("the commit must not report a success it cannot honour").isGreaterThanOrEqualTo(400);
        assertThat(committedCount()).isZero();
      } finally {
        rollback(sessionId);
      }
    }

    @FunctionalInterface
    private interface ThrowingConsumer {
      void accept(String sessionId) throws Exception;
    }
  }

  // Regression: #9852 a malformed command envelope rolled back the session transaction
  /**
   * Issue #9852: {@code POST /api/v1/command} refused a malformed envelope (a missing {@code command}, a {@code language} that is not a
   * string) with a plain {@link IllegalArgumentException}, and {@link HttpSession#execute} treats anything but a
   * {@link RequestRefusedBeforeExecutionException} as a command that failed part way: it rolled back the transaction the caller had
   * opened with {@code /begin}, although not one statement of the request had reached the engine (issue #9784 gave the same rule to the
   * streaming refusals of this route and left these behind).
   * <p>
   * The test is a table over EVERY refusal the route can raise from the request alone, and pins the rule rather than the cells: after
   * each one the session still sees the rows written before it, and its {@code /commit} is accepted. A refusal added to the route
   * later has to be added here, which is the point.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CommandEnvelopeRefusalKeepsSessionTransaction extends BaseGraphServerTest {
    private static final String DOC_TYPE = "Doc9852";

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    private static String rootAuth() {
      return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
    }

    private HttpResponse<String> send(final String path, final String sessionId, final String body) throws IOException, InterruptedException {
      final HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1" + path)))
          .timeout(Duration.ofSeconds(30)).header("Authorization", rootAuth()).header("Content-Type", "application/json");
      if (sessionId != null)
        builder.header(HttpSessionManager.ARCADEDB_SESSION_ID, sessionId);
      builder.POST(HttpRequest.BodyPublishers.ofString(body));
      return http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> command(final String sessionId, final String sql) throws Exception {
      return send("/command/" + getDatabaseName(), sessionId, new JSONObject().put("language", "sql").put("command", sql).toString());
    }

    /** Every malformed envelope the route can refuse before it reaches the engine, by the name the failure message carries. */
    private static Map<String, String> refusals() {
      final Map<String, String> table = new LinkedHashMap<>();
      table.put("no 'command' field", "{\"language\":\"sql\"}");
      table.put("'command' is null", "{\"language\":\"sql\",\"command\":null}");
      table.put("'command' is a number", "{\"language\":\"sql\",\"command\":5}");
      table.put("'command' is an object", "{\"language\":\"sql\",\"command\":{\"a\":1}}");
      table.put("'language' is a number", "{\"language\":1,\"command\":\"SELECT 1\"}");
      table.put("'language' is an array", "{\"language\":[\"sql\"],\"command\":\"SELECT 1\"}");
      table.put("'serializer' is a number", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"serializer\":3}");
      table.put("'profileExecution' is a number", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"profileExecution\":3}");
      table.put("'limit' is a string", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"limit\":\"ten\"}");
      table.put("'limit' above the int range", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"limit\":3000000000}");
      table.put("'limit' below the int range", "{\"language\":\"sql\",\"command\":\"SELECT 1\",\"limit\":-3000000000}");
      table.put("'$bytes' marker is a number", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$bytes\":5}}}");
      table.put("'$bytes' marker is not base64", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$bytes\":\"!!!\"}}}");
      table.put("'$int8' marker is not an array", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$int8\":\"x\"}}}");
      table.put("'$int8' element is out of range", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$int8\":[1,300]}}}");
      table.put("'$int8' element is fractional", "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":{\"$int8\":[1.5]}}}");
      table.put("parameters nested beyond the decoder's depth",
          "{\"language\":\"sql\",\"command\":\"SELECT :p\",\"params\":{\"p\":" + "[".repeat(40) + "]".repeat(40) + "}}");
      // These two answer 400 through a normal return, not through an exception: the controls that say the table is about the
      // outcome and not about the exception type
      table.put("'command' is empty", "{\"language\":\"sql\",\"command\":\"\"}");
      table.put("'language' is empty", "{\"language\":\"\",\"command\":\"SELECT 1\"}");
      return table;
    }

    @Test
    void everyMalformedEnvelopeIsRefusedAndKeepsTheSessionTransaction() throws Exception {
      assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);

      final HttpResponse<String> begun = send("/begin/" + getDatabaseName(), null, "");
      assertThat(begun.statusCode()).isEqualTo(204);
      final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
      assertThat(sessionId).isNotBlank();

      try {
        for (int i = 0; i < 3; i++)
          assertThat(command(sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'witness" + i + "'").statusCode()).isEqualTo(200);

        for (final Map.Entry<String, String> refusal : refusals().entrySet()) {
          final HttpResponse<String> refused = send("/command/" + getDatabaseName(), sessionId, refusal.getValue());
          assertThat(refused.statusCode()).as("%s must be refused with a 400: %s", refusal.getKey(), refused.body()).isEqualTo(400);

          final HttpResponse<String> count = command(sessionId, "SELECT count() AS cnt FROM " + DOC_TYPE);
          assertThat(count.statusCode()).as("after '%s' the session must still be usable: %s", refusal.getKey(), count.body()).isEqualTo(200);
          assertThat(new JSONObject(count.body()).getJSONArray("result").getJSONObject(0).getLong("cnt"))
              .as("after '%s' the session must still see the rows it wrote", refusal.getKey()).isEqualTo(3L);
        }

        final HttpResponse<String> committed = send("/commit/" + getDatabaseName(), sessionId, "");
        assertThat(committed.statusCode()).as("the commit must be accepted: %s", committed.body()).isEqualTo(204);

        final Database database = getServerDatabase(0, getDatabaseName());
        assertThat(database.countType(DOC_TYPE, false)).as("and the witnesses are in the database").isEqualTo(3L);
      } finally {
        send("/rollback/" + getDatabaseName(), sessionId, "");
      }
    }

    /**
     * The counter-case that keeps the table meaningful: a command the ENGINE refuses, after it was handed the statement, still rolls
     * the session's transaction back. Only the requests that never reached the engine are exempt.
     */
    @Test
    void aCommandFailingInTheEngineStillRollsTheSessionTransactionBack() throws Exception {
      assertThat(command(null, "CREATE DOCUMENT TYPE " + DOC_TYPE + " IF NOT EXISTS").statusCode()).isEqualTo(200);

      final HttpResponse<String> begun = send("/begin/" + getDatabaseName(), null, "");
      final String sessionId = begun.headers().firstValue(HttpSessionManager.ARCADEDB_SESSION_ID).orElse(null);
      assertThat(sessionId).isNotBlank();
      try {
        assertThat(command(sessionId, "INSERT INTO " + DOC_TYPE + " SET name = 'witness'").statusCode()).isEqualTo(200);

        assertThat(command(sessionId, "INSERT INTO NoSuchType9852 SET name = 'x'").statusCode()).isGreaterThanOrEqualTo(400);

        assertThat(command(sessionId, "SELECT FROM " + DOC_TYPE).statusCode()).as("the session was rolled back and refuses more work")
            .isGreaterThanOrEqualTo(400);
      } finally {
        send("/rollback/" + getDatabaseName(), sessionId, "");
      }
    }
  }
}
