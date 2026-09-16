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

import com.arcadedb.server.http.HttpSessionManager;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7684: a buffered {@code GET /api/v1/query/{db}/{lang}/{cmd}} carrying {@code arcadedb-session-id} was
 * answered on the Undertow IO thread, and {@code DatabaseAbstractHandler.execute} runs a session-scoped request
 * inside {@code HttpSession.execute}, which waits up to five seconds on the session lock.
 * <p>
 * The class comment that put this handler on the IO thread is still right about what it says - a buffered GET is
 * short - but "short" is a property of the QUERY, and the lock wait is not. Reaching it takes only a client that
 * issues two requests on one session at the same time, or retries one whose predecessor is still running: the
 * second one blocks. An IO thread serves many connections, so that wait is not paid by the caller that queued
 * behind its own session; it is paid by every unrelated connection multiplexed onto the same thread, and a
 * client looping that way can hold several IO threads at once.
 * <p>
 * The assertion is the DISPATCH rather than a latency: what went wrong is which thread the wait happens on, and
 * a timing bound would say nothing about that while being a coin flip on a loaded machine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7684SessionRequestWorkerThreadDispatchTest {

  private static HttpServerExchange withSession() {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.getRequestHeaders().put(new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID), "AS-42");
    return exchange;
  }

  @Test
  void aBufferedGetQueryNamingASessionGoesToAWorkerThread() {
    assertThat(new GetQueryHandler(null).mustExecuteOnWorkerThread(withSession()))
        .as("the request runs inside HttpSession.execute, which waits up to 5s on the session lock")
        .isTrue();
  }

  /**
   * The counter-case, and the reason the fix is per-request rather than handler-wide: a session-less buffered
   * GET has no lock to wait on, so it keeps being answered on the IO thread exactly as before. Without this the
   * test above would pass just as well against {@code return true}, which would move every dashboard poll and
   * every health-check query onto a worker thread for nothing.
   */
  @Test
  void aBufferedSessionLessGetQueryStaysOnTheIoThread() {
    assertThat(new GetQueryHandler(null).mustExecuteOnWorkerThread(new HttpServerExchange(null)))
        .as("nothing on this path blocks, so the hand-off would be pure cost")
        .isFalse();
  }

  /**
   * The condition that was already there before #7684 and must survive it: a streamed answer writes blocking
   * output for as long as the client takes to read it, session or no session.
   */
  @Test
  void aStreamedGetQueryStillGoesToAWorkerThreadWithoutASession() {
    final HttpServerExchange ndJson = new HttpServerExchange(null);
    ndJson.getRequestHeaders().put(Headers.ACCEPT, "application/x-ndjson");

    assertThat(new GetQueryHandler(null).mustExecuteOnWorkerThread(ndJson)).isTrue();
  }

  /**
   * The second handler the sweep asked for found: {@code POST /api/v1/rollback} is on
   * {@code DatabaseAbstractHandler} and declared no override at all, so it inherited the {@code false} default -
   * on a route whose every meaningful request names a session, since a rollback with no session id rolls back
   * nothing and removes no session. Its two siblings {@code /begin} and {@code /commit} already answered true.
   */
  @Test
  void aRollbackNamingASessionGoesToAWorkerThread() {
    assertThat(new PostRollbackHandler(null).mustExecuteOnWorkerThread(withSession()))
        .as("rollback resolves the session and therefore waits on its lock")
        .isTrue();
    assertThat(new PostRollbackHandler(null).mustExecuteOnWorkerThread(new HttpServerExchange(null)))
        .as("a session-less rollback resolves nothing, so it has no lock to wait on")
        .isFalse();
  }

  /**
   * The sweep's result, pinned so a new {@code DatabaseAbstractHandler} subclass cannot quietly reintroduce the
   * defect: every handler that can answer a session-carrying request on the IO thread is listed here, and the
   * list is empty. Driven by construction rather than by reflection over the package, because a handler needs a
   * live {@code HttpServer} for anything but this one question and {@code null} is enough for it.
   */
  @Test
  void noDatabaseHandlerAnswersASessionCarryingRequestOnTheIoThread() {
    final HttpServerExchange session = withSession();

    final DatabaseAbstractHandler[] handlers = {
        new GetQueryHandler(null), new PostQueryHandler(null), new PostCommandHandler(null),
        new PostBeginHandler(null), new PostCommitHandler(null),
        new PostRollbackHandler(null), new GetTimeSeriesLatestHandler(null),
        new PostTimeSeriesQueryHandler(null), new PostTimeSeriesWriteHandler(null),
        new GetGrafanaHealthHandler(null), new GetGrafanaMetadataHandler(null), new PostGrafanaQueryHandler(null),
        new GetPromQLLabelsHandler(null), new GetPromQLLabelValuesHandler(null), new GetPromQLSeriesHandler(null),
        new GetPromQLQueryHandler(null), new GetPromQLQueryRangeHandler(null),
        new PostVectorSearchHandler(null), new PostVectorHybridSearchHandler(null),
        new PostVectorFullTextSearchHandler(null) };

    for (final DatabaseAbstractHandler handler : handlers)
      assertThat(handler.mustExecuteOnWorkerThread(session))
          .as("%s would wait on the session lock from an Undertow IO thread", handler.getClass().getSimpleName())
          .isTrue();
  }
}
