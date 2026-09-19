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
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * Grafana health-check endpoint.
 * Endpoint: GET /api/v1/ts/{database}/grafana/health
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, for the reasons spelled out on
 * {@link PostGrafanaQueryHandler}: a request carrying {@code arcadedb-session-id} is answered inside that
 * session - under its lock, on its principal, refreshing the idle timer that decides when its transaction is
 * rolled back underneath it - and the base class subsumes the {@code checkAuthorizationOnDatabase} call this
 * handler used to make by hand.
 * <p>
 * Resolving the database is now the base class's job, which is also what this handler's own body used to do
 * one line later purely to make a missing database throw. The one difference that follows: a health check
 * against a database that does not exist is refused BEFORE the session is resolved rather than after, which is
 * the same 404 it answered before.
 */
public class GetGrafanaHealthHandler extends AbstractObservabilityHandler {

  public GetGrafanaHealthHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * A session-less health check is short enough to answer on the IO thread, which is what this handler has
   * always done. A request that names a session is not: see {@link DatabaseAbstractHandler#carriesSessionId}.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread(final HttpServerExchange exchange) {
    return carriesSessionId(exchange);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    final JSONObject result = new JSONObject();
    result.put("status", "ok");
    // The name as the caller spelled it in the path, which is what this response has always echoed. Taken from
    // the parameter rather than from db.getName() so the echo cannot start disagreeing with the request if the
    // server ever resolves a name through an alias.
    result.put("database", exchange.getQueryParameters().get("database").getFirst());

    return new ExecutionResponse(200, result.toString());
  }
}
