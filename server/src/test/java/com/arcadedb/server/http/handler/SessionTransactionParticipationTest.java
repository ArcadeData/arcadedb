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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.TransactionContext;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Which routes run inside the caller's {@code /begin} session transaction, and which detach from it so that a failure
 * on them cannot roll it back.
 */
class SessionTransactionParticipationTest {
  /*
   * Issue #7859: issue #7734 gave {@code DatabaseAbstractHandler.participatesInSessionTransaction()} its
   * {@code false} answer on the three {@code /api/v1/ts} routes and left the Prometheus, Grafana and PromQL
   * routes on the inherited {@code true} - although they extend the same base class, resolve the same
   * {@code arcadedb-session-id}, publish the same "this does not participate in your transaction" contract, and
   * raise the same two pre-engine refusals: the 413 of {@code resultSetTooLarge} when the answer would exceed
   * {@code arcadedb.server.maxResultRows}, and the 400 of a malformed {@code start}/{@code end} timestamp. Either
   * of them destroyed a transaction the client had opened with {@code /begin} and still believed it owned, and
   * left the session registered so its later {@code /commit} reported nothing lost.
   * <p>
   * Read off the handlers rather than driven over HTTP: the rollback itself is
   * {@code HttpSession.execute}'s, pinned on the failure path by
   * {@link SessionTransactionParticipationTest.OnASessionWithATransaction}. What was wrong here is the ANSWER each handler
   * gives, and an answer inherited by accident is exactly what a per-handler assertion catches.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #7859 the PromQL, Grafana and Prometheus routes detach from the session transaction
  /** Every PromQL and Grafana route. All reads, all on {@link AbstractObservabilityHandler}. */
  @Test
  void everyPromQlAndGrafanaRouteDetachesFromTheSessionTransaction() {
    assertThat(new GetPromQLQueryHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLQueryRangeHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLSeriesHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLLabelsHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetPromQLLabelValuesHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new PostGrafanaQueryHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetGrafanaMetadataHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new GetGrafanaHealthHandler(null).participatesInSessionTransaction()).isFalse();
  }

  // Regression: #7859 the PromQL, Grafana and Prometheus routes detach from the session transaction
  /**
   * The two Prometheus routes, which answer the same way for the same reasons but cannot inherit it: their
   * binary body spends their one superclass on {@link AbstractBinaryHttpHandler}. The write route is included
   * for the reason {@code PostTimeSeriesWriteHandler} is - it commits through the shard's own transaction
   * whatever the caller has open - so it cannot acquire the exposure the moment it grows a refusal.
   */
  @Test
  void bothPrometheusRoutesDetachFromTheSessionTransaction() {
    assertThat(new PostPrometheusReadHandler(null).participatesInSessionTransaction()).isFalse();
    assertThat(new PostPrometheusWriteHandler(null).participatesInSessionTransaction()).isFalse();
  }

  // Regression: #7859 the PromQL, Grafana and Prometheus routes detach from the session transaction
  /**
   * The observability reads keep the {@code requiresTransaction()} answer they had before the shared base class
   * was introduced. It is what {@code rejectsUnresolvableSession()} derives from, so a silent change here would
   * turn a degraded session-less read into a refusal.
   */
  @Test
  void theObservabilityReadsStillWantNoAutoCommitWrapper() {
    assertThat(new GetPromQLQueryRangeHandler(null).requiresTransaction()).isFalse();
    assertThat(new PostGrafanaQueryHandler(null).requiresTransaction()).isFalse();
    assertThat(new GetGrafanaHealthHandler(null).requiresTransaction()).isFalse();
  }

  /*
   * {@code POST /query} runs without the auto-commit wrapper, so its scans can be parallel, but still refuses a session id
   * this server cannot resolve (#8775, #7402). The two flags are pinned here without a server, the behaviour in
   * {@code QueryEndpointReadOnlyTest}.
   */
  // Regression: #8775 POST /query has no auto-commit wrapper but refuses a stale session
  @Test
  void queryRouteHasNoAutoCommitWrapperButRefusesAStaleSession() {
    final PostQueryHandler handler = new PostQueryHandler(null);
    assertThat(handler.requiresTransaction()).isFalse();
    assertThat(handler.rejectsUnresolvableSession()).isTrue();
  }

  // Regression: #8775 POST /query has no auto-commit wrapper but refuses a stale session
  @Test
  void commandRouteKeepsTheWrapper() {
    assertThat(new PostCommandHandler(null).requiresTransaction()).isTrue();
  }

  /** A session holding an active transaction, driven through {@code HttpSession.execute}. */
  @Nested
  class OnASessionWithATransaction {
    /*
     * Issue #7734: issue #7402 moved the three {@code /api/v1/ts} routes from {@code AbstractServerHttpHandler} onto
     * {@link DatabaseAbstractHandler}, which runs a session-scoped request inside {@code HttpSession.execute} - and
     * that method's {@code catch} arm calls {@code rollbackIfActive()} on the session's transaction.
     * <p>
     * Before #7402 those routes never resolved {@code arcadedb-session-id}, so nothing they did could touch a
     * client's open transaction. Afterwards two of them could destroy it:
     * <ul>
     * <li>{@code POST /ts/{db}/write} answers 403 when {@code TimeSeriesGateway.write} refuses the append on the
     * per-type ACL ({@code tsType.checkAccess(CREATE_RECORD)}) - and the handler's own javadoc states the opposite
     * contract, that the samples are durable before the caller commits anything. <b>Not</b> the body parser, which
     * is where issue #7734 puts it: {@code LineProtocolParser.parseLine} catches every
     * {@code IllegalArgumentException} {@code readFieldValue} raises and has {@code parse} log-and-skip the line, so
     * an unterminated quoted field value answers 204.</li>
     * <li>{@code POST /ts/{db}/query} answers 413 when the caller's {@code limit} is above, or absent and therefore
     * lowered to, {@code arcadedb.server.httpQueryMaxResultRows}. A READ destroying a WRITE transaction, and the one
     * driven end to end in {@code TimeSeriesHttpSessionIT.TimeSeriesRoutes}.</li>
     * </ul>
     * The session stayed registered either way, so the client's later {@code /commit} found a session whose
     * transaction was already dead and did not report that anything had been lost.
     * <p>
     * {@code TimeSeriesHttpSessionIT.TimeSeriesRoutes} states the invariant - "the two time-series reads must detach from
     * the session, never commit or roll it back" - and asserted it only on the success path. This pins it on the
     * failure path, where it was false.
     *
     * @author Luca Garulli (l.garulli@arcadedata.com)
     */
    private HttpSessionManager manager;
    private ServerSecurityUser user;
    private TransactionContext transaction;
    private HttpSession        session;

    @BeforeEach
    void setUp() {
      manager = new HttpSessionManager(60_000);
      user = TestServerHelper.securityUser("tester");
      // A subclass mock, held to the #8851 inline-mock guard rather than #9464's: TransactionContext is an engine type
      transaction = mock(TransactionContext.class);
      when(transaction.isActive()).thenReturn(true);
      session = manager.createSession(user, transaction);
    }

    @AfterEach
    void tearDown() {
      if (manager != null)
        manager.close();
    }

    // Regression: #7734 a failure on a time-series route leaves the session transaction alone
    /**
     * The defect itself: a handler that does not participate in the session transaction throws, and the
     * transaction the client opened with {@code /begin} is still there afterwards.
     */
    @Test
    void aFailureOnANonParticipatingRouteLeavesTheSessionTransactionAlone() {
      assertThatThrownBy(() -> session.execute(user, () -> {
        throw new IllegalArgumentException("unterminated quoted field value");
      }, false)).isInstanceOf(IllegalArgumentException.class);

      verify(transaction, never()).rollback();
    }

    // Regression: #7734 a failure on a time-series route leaves the session transaction alone
    /**
     * The counter-case that keeps the one above meaningful: a handler that DOES run inside the caller's
     * transaction still rolls it back when it fails, which is what {@code HttpSession.execute} has always done and
     * what a half-applied command needs.
     */
    @Test
    void aFailureOnAParticipatingRouteStillRollsTheSessionTransactionBack() {
      assertThatThrownBy(() -> session.execute(user, () -> {
        throw new IllegalStateException("command failed part way");
      })).isInstanceOf(IllegalStateException.class);

      verify(transaction).rollback();
    }

    // Regression: #7734 a failure on a time-series route leaves the session transaction alone
    /** Neither choice may weaken the lock, the registration re-validation or the idle-clock refresh. */
    @Test
    void aSuccessfulNonParticipatingCallStillRefreshesTheIdleClock() throws Exception {
      final long before = session.elapsedFromLastUpdate();
      Thread.sleep(5);
      session.execute(user, () -> null, false);

      assertThat(session.elapsedFromLastUpdate()).isLessThanOrEqualTo(before + 5);
      verify(transaction, never()).rollback();
    }

    // Regression: #7734 a failure on a time-series route leaves the session transaction alone
    // Regression: #7859 the command route is the counter-case of the detached routes: it still participates
    /**
     * The three routes that declare themselves independent, and the ones that must not. Read off the handlers so
     * the wiring above cannot be right while the routes it exists for are still wired the old way.
     */
    @Test
    void onlyTheTimeSeriesRoutesDeclareThemselvesIndependentOfTheSessionTransaction() {
      assertThat(new PostTimeSeriesWriteHandler(null).participatesInSessionTransaction())
          .as("appends go through TimeSeriesShard's own begin/commit, never the caller's transaction").isFalse();
      assertThat(new PostTimeSeriesQueryHandler(null).participatesInSessionTransaction())
          .as("a read detaches from the session").isFalse();
      assertThat(new GetTimeSeriesLatestHandler(null).participatesInSessionTransaction())
          .as("a one-row read detaches from the session").isFalse();

      assertThat(new PostCommandHandler(null).participatesInSessionTransaction())
          .as("a command runs INSIDE the caller's transaction and a failure must roll it back").isTrue();
      assertThat(new PostCommandHandler(null).rollsBackSessionTransactionOnFailure()).isTrue();

      // The two query routes run inside the caller's transaction too (read-your-writes, and refused once a failed command rolled it
      // back) but cannot have written anything, so their failures stop destroying it (issue #9784). Two questions, two answers.
      assertThat(new GetQueryHandler(null).participatesInSessionTransaction()).isTrue();
      assertThat(new PostQueryHandler(null).participatesInSessionTransaction()).isTrue();
      assertThat(new GetQueryHandler(null).rollsBackSessionTransactionOnFailure())
          .as("a read that fails has nothing half applied to distrust").isFalse();
      assertThat(new PostQueryHandler(null).rollsBackSessionTransactionOnFailure())
          .as("a read that fails has nothing half applied to distrust").isFalse();
    }

    /*
     * Issue #8063: follow-up to #7859. The three {@code /api/v1/vector/{database}/*} routes extend
     * {@link DatabaseAbstractHandler} directly and never overrode {@link DatabaseAbstractHandler#participatesInSessionTransaction()},
     * so they inherited its default of {@code true} - the same gap #7859 closed for the TimeSeries and observability
     * routes. They are pure reads that raise {@link IllegalArgumentException} on malformed client input (a missing or
     * non-numeric {@code k}, an unknown index name) before anything reaches the engine, which
     * {@code AbstractServerHttpHandler}'s error mapping turns into a 400 - and on the inherited default, that 400 still
     * reaches {@code HttpSession.execute}'s rollback arm and destroys a transaction the client opened with
     * {@code /begin} and still believes it owns.
     *
     * @author Luca Garulli (l.garulli@arcadedata.com)
     */
    // Regression: #8063 the vector search routes detach from the session transaction
    @Test
    void theThreeVectorSearchRoutesDeclareThemselvesIndependentOfTheSessionTransaction() {
      assertThat(new PostVectorSearchHandler(null).participatesInSessionTransaction())
          .as("a kNN read detaches from the session").isFalse();
      assertThat(new PostVectorFullTextSearchHandler(null).participatesInSessionTransaction())
          .as("a full-text read detaches from the session").isFalse();
      assertThat(new PostVectorHybridSearchHandler(null).participatesInSessionTransaction())
          .as("a hybrid read detaches from the session").isFalse();
    }

    // Regression: #8063 the vector search routes detach from the session transaction
    /**
     * The defect itself, reproduced through the same {@code HttpSession.execute} path the routes run inside: a
     * malformed-input failure on a non-participating route must not roll back the caller's session transaction.
     */
    @Test
    void aMalformedRequestOnAVectorSearchRouteLeavesTheSessionTransactionAlone() {
      assertThatThrownBy(() -> session.execute(user, () -> {
        throw new IllegalArgumentException("k must be a positive integer");
      }, new PostVectorSearchHandler(null).participatesInSessionTransaction()))
          .isInstanceOf(IllegalArgumentException.class);

      verify(transaction, never()).rollback();
    }
  }
}
