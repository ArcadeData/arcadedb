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

    DatabaseContext.INSTANCE.init((DatabaseInternal) database);

    // isolationLevel is optional: when a payload is present but omits it, fall back to the default isolation.
    // getString(name, default) avoids the full recursive toMap() copy on this hot path; a scalar value stringifies
    // (no ClassCastException, unlike the old (String) cast), while a JSON object/array would still be rejected.
    final String isolationLevel = payload != null ? payload.getString("isolationLevel", null) : null;
    if (isolationLevel != null)
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.valueOf(isolationLevel));
    else
      database.begin();

    final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

    final HttpSession session = httpServer.getSessionManager().createSession(user, tx);

    // USE THE SESSION ID AS REQUESTER TO ALLOW UNLOCK FILES FROM A DIFFERENT THREAD (SAME SESSION ID)
    tx.setRequester(session.id);

    DatabaseContext.INSTANCE.removeContext(database.getDatabasePath());

    exchange.getResponseHeaders().put(new HttpString(HttpSessionManager.ARCADEDB_SESSION_ID), session.id);

    Metrics.counter("http.begin").increment();

    return new ExecutionResponse(204, "");
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
