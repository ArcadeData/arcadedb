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
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
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
 * driven end to end in {@code Issue7402TimeSeriesHttpSessionIT}.</li>
 * </ul>
 * The session stayed registered either way, so the client's later {@code /commit} found a session whose
 * transaction was already dead and did not report that anything had been lost.
 * <p>
 * {@code Issue7402TimeSeriesHttpSessionIT} states the invariant - "the two time-series reads must detach from
 * the session, never commit or roll it back" - and asserted it only on the success path. This pins it on the
 * failure path, where it was false.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7734TimeSeriesRouteKeepsSessionTransactionTest {

  private HttpSessionManager manager;
  private ServerSecurityUser user;
  private TransactionContext transaction;
  private HttpSession        session;

  @BeforeEach
  void setUp() {
    manager = new HttpSessionManager(60_000);
    user = mock(ServerSecurityUser.class);
    transaction = mock(TransactionContext.class);
    when(transaction.isActive()).thenReturn(true);
    session = manager.createSession(user, transaction);
  }

  @AfterEach
  void tearDown() {
    if (manager != null)
      manager.close();
  }

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

  /** Neither choice may weaken the lock, the registration re-validation or the idle-clock refresh. */
  @Test
  void aSuccessfulNonParticipatingCallStillRefreshesTheIdleClock() throws Exception {
    final long before = session.elapsedFromLastUpdate();
    Thread.sleep(5);
    session.execute(user, () -> null, false);

    assertThat(session.elapsedFromLastUpdate()).isLessThanOrEqualTo(before + 5);
    verify(transaction, never()).rollback();
  }

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
    assertThat(new GetQueryHandler(null).participatesInSessionTransaction()).isTrue();
    assertThat(new PostQueryHandler(null).participatesInSessionTransaction()).isTrue();
  }
}
