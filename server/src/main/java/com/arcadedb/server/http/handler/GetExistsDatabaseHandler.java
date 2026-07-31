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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public class GetExistsDatabaseHandler extends AbstractServerHttpHandler {
  public GetExistsDatabaseHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final Deque<String> databaseName = exchange.getQueryParameters().get("database");
    if (databaseName.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is null\"}");

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.exists-database").increment();

    // Deliberately not filterAuthorizedDatabases(): this route tests a single name, so building the whole
    // authorized set to look one entry up would allocate proportionally to the number of databases on the
    // server for a constant-time question. The conjuncts below are the same predicate that helper applies -
    // installed, and accessible to the caller - just evaluated for one name, including its null-user
    // contract, so the two paths cannot drift on what an unauthenticated route is allowed to report.
    final String requested = databaseName.getFirst();
    final boolean existsDatabase = server.getDatabaseNames().contains(requested)
        && (user == null || user.canAccessToDatabase(requested));

    final JSONObject response = new JSONObject();
    response.put("result", existsDatabase);

    return new ExecutionResponse(200, response.toString());
  }
}
