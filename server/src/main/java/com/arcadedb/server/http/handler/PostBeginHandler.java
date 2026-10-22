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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.IsolationLevels;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.HttpSession;
import com.arcadedb.server.http.HttpSessionManager;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.io.IOException;

public class PostBeginHandler extends DatabaseAbstractHandler {

  public PostBeginHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database,
      final JSONObject payload) throws IOException {
    final HeaderValues txId = exchange.getRequestHeaders().get(HttpSessionManager.ARCADEDB_SESSION_ID);
    if (txId != null && !txId.isEmpty()) {
      final HttpSession tx = httpServer.getSessionManager().getSessionById(user, txId.getFirst());
      if (tx != null)
        // 409 Conflict (RFC 9110): re-issuing begin on an existing session is a state conflict, not an
        // authentication failure - returning 401 wrongly makes clients re-authenticate.
        return new ExecutionResponse(409, "{ \"error\" : \"Transaction already started\" }");
    }

    // Read before anything is created: the envelope already refused a label that cannot be one, and nothing below may fail
    // between publishing the session and handing its id to the client (issue #9775)
    final String tag = queryTag(exchange);
    // Resolved before the context is attached to this worker thread, so a refused value leaves nothing behind (issue #10295)
    final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel = isolationLevel(payload);

    DatabaseContext.INSTANCE.init((DatabaseInternal) database);

    if (isolationLevel != null)
      database.begin(isolationLevel);
    else
      database.begin();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
    // The client ends this transaction with its own /commit or /rollback: an HA replica runs its writes in it (issue #10040)
    tx.setClientManaged(true);

    final HttpSession session = httpServer.getSessionManager().createSession(user, tx);
    // The statements the session runs are listed with this label unless they carry their own (issue #9680)
    session.setTag(tag);

    // USE THE SESSION ID AS REQUESTER TO ALLOW UNLOCK FILES FROM A DIFFERENT THREAD (SAME SESSION ID)
    tx.setRequester(session.id);

    DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());

    exchange.getResponseHeaders().put(new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID), session.id);

    Metrics.counter("http.begin").increment();

    return new ExecutionResponse(204, "");
  }

  /**
   * The optional {@code isolationLevel} of the request body, the values the OpenAPI spec declares for it (issue #10295).
   * Absent (no body, or a body that omits it) means the database's default isolation and returns {@code null}. Anything
   * outside {@link Database.TRANSACTION_ISOLATION_LEVEL} is a client error, answered 400 by the
   * {@link IllegalArgumentException} arm with a message that names the parameter and the values it accepts.
   */
  static Database.TRANSACTION_ISOLATION_LEVEL isolationLevel(final JSONObject payload) {
    // opt(name) reads the one member without the full recursive toMap() copy on this hot path. Not getString(): it unwraps a
    // one-element array, so ["READ_COMMITTED"] would have begun a transaction, while the spec declares a string
    final Object value = payload != null ? payload.opt("isolationLevel") : null;
    if (value == null)
      return null;
    // Parsed where every surface parses it, so the accepted values and the refusal are the same everywhere (issue #10386)
    if (value instanceof String name)
      return IsolationLevels.parse(name);
    throw IsolationLevels.refusal("of type " + value.getClass().getSimpleName());
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  /** Manages a session or answers a probe: never held behind the queries in the admission gate (issue #9518). */
  @Override
  protected boolean goesThroughAdmissionGate() {
    return false;
  }
}
