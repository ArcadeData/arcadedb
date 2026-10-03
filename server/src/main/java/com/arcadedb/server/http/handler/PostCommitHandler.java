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
    try {
      // A failed command rolled the session's transaction back and what the client wrote before it is gone: say so, instead of
      // answering 204 as for a commit (issue #9006). The finally below still ends the session.
      final HttpSession session = findSession(exchange, user);
      if (session != null && session.isRolledBackByFailure())
        throw new HttpSessionException("Remote transaction '" + session.id + "' was rolled back after a failed command: its changes were not committed");

      // Guard with isTransactionActive() so a retried /commit whose session was already removed by the first
      // call is an idempotent no-op (204) instead of committing a non-existent transaction (which would 500).
      if (database.isTransactionActive())
        database.commit();
    } finally {
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
}
