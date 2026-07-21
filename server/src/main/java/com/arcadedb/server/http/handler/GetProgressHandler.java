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

import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

/**
 * Returns the long-running maintenance operations (CHECK DATABASE, ...) currently in progress on this server
 * for one database, with their step-by-step progress (issue #5372). Polled by the console and Studio to render
 * a progress bar while a synchronous command runs. Deliberately reads only the lock-free
 * {@link OperationProgressRegistry} snapshot: no database access, no transaction, so polling is safe at any
 * frequency and cannot interfere with the operation being watched.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetProgressHandler extends AbstractServerHttpHandler {
  public GetProgressHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final Deque<String> databaseNameParam = exchange.getQueryParameters().get("database");
    final String databaseName = databaseNameParam == null || databaseNameParam.isEmpty() ? null : databaseNameParam.getFirst();

    // CONSOLIDATED PER-DATABASE AUTHORIZATION GATE (GHSA-x8mg-6r4p-87pf): throws IllegalArgumentException
    // (mapped to HTTP 400) on a missing database name and SecurityException (mapped to HTTP 403) when the
    // authenticated user cannot access the database.
    checkAuthorizationOnDatabase(user, databaseName);

    final JSONArray operations = new JSONArray();
    for (final OperationProgress op : OperationProgressRegistry.instance().getOperations(databaseName))
      operations.put(op.toJSON());

    final JSONObject response = new JSONObject();
    response.put("result", operations);
    return new ExecutionResponse(200, response.toString());
  }
}
