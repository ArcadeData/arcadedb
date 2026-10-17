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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionException;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;

import java.io.IOException;

public class PostCommitHandler extends DatabaseAbstractHandler {

  public PostCommitHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    // database.commit() below waits for the commit to be published (issue #7621): on a replicated database
    // that is a Raft round-trip, and it is on the data path rather than an admin one, so it is reachable far
    // more often than the cluster-admin handlers issue #7133 fixed. Running that wait on the Undertow IO
    // thread stalls every other connection on the same selector, including kubelet readiness/liveness probes.
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database,
      final JSONObject payload) throws IOException {
    final HttpSession session = findSession(exchange, user);
    boolean committed = false;
    try {
      if (session == null) {
        // The request names a session this server does not hold for this principal: never issued, rolled back, ended by
        // 'terminate transaction', expired, or another principal's. Nothing is committed, so the answer must not be the 204
        // of a commit (issue #10164). The one exception is a retry of a commit that DID end this session: its first answer
        // may never have reached the client, which must not be told now that its work is gone (issue #5026).
        final HeaderValues sessionId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
        if (sessionId != null && !sessionId.isEmpty() && !httpServer.getSessionManager().wasCommittedBy(user, sessionId.getFirst()))
          throw new HttpSessionException("Remote transaction '" + sessionId.getFirst() + "' not found or expired");
      }

      // A failed command rolled the session's transaction back and what the client wrote before it is gone: say so, instead of
      // answering 204 as for a commit (issue #9006). The finally below still ends the session.
      if (session != null && session.isRolledBackByFailure())
        throw new HttpSessionException("Remote transaction '" + session.id + "' was rolled back after a failed command: its changes were not committed");

      // Guard with isTransactionActive(): a commit with no session header runs outside any transaction, and has nothing to
      // commit.
      if (database.isTransactionActive())
        database.commit();
      committed = true;
    } finally {
      // Remembered BEFORE the session is removed, so a retry never finds the id neither open nor committed (issue #10164)
      if (committed && session != null)
        httpServer.getSessionManager().recordCommitted(session.id, user);

      // End the server-side session: after /commit its transaction is gone, so the session id must no longer
      // resolve. Leaving it registered let follow-up writes silently auto-commit and made a retried commit 500.
      // Ownership-gated so a request carrying another principal's session id cannot evict/orphan that session.
      //
      // In a finally, because a commit that FAILS ends the transaction just the same: HttpSession.execute rolls it
      // back on the way out. The session used to stay registered with nothing left in it until the idle timeout,
      // one abandoned session per failed commit, and a client retrying the block left one behind per attempt
      // (issue #8618). The client is told, so it does not send a /rollback to release what is already gone.
      if (removeSession(exchange, user))
        exchange.getResponseHeaders().put(SESSION_CLOSED_HEADER, "true");
      exchange.getResponseHeaders().remove(HttpSessionManager.ARCADEDB_SESSION_ID);
    }
    Metrics.counter("http.commit").increment();

    return new ExecutionResponse(204, "");
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  /**
   * This route IS the commit, so the session transaction's commit counter moves on every successful call. See
   * {@link DatabaseAbstractHandler#reportsSessionPartialCommit()} (issue #8062).
   */
  @Override
  protected boolean reportsSessionPartialCommit() {
    return false;
  }

  @Override
  protected boolean endsSession() {
    return true;
  }

  /**
   * A retry that arrived while the first commit of the same session was still running (a slow Raft round-trip, a client timeout
   * shorter than the commit) waited for the session lock and found the session ended. When the first commit is what ended it the
   * answer is the one the retry would get had it arrived afterwards: 204 (issue #10171). A session that was rolled back,
   * terminated or expired instead is still refused with 404.
   */
  @Override
  protected ExecutionResponse answerForEndedSession(final HttpServerExchange exchange, final ServerSecurityUser user,
      final String sessionId) {
    if (!httpServer.getSessionManager().wasCommittedBy(user, sessionId))
      return null;
    exchange.getResponseHeaders().remove(HttpSessionManager.ARCADEDB_SESSION_ID);
    return new ExecutionResponse(204, "");
  }
}
