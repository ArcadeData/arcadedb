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
import io.undertow.server.HttpServerExchange;

import java.util.*;

public class GetExistsDatabaseHandler extends AbstractServerHttpHandler {
  public GetExistsDatabaseHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
    final Deque<String> databaseName = exchange.getQueryParameters().get("database");
    if (databaseName.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is null\"}");

    final ArcadeDBServer server = httpServer.getServer();
    server.getServerMetrics().meter("http.exists-database").hit();

    final Set<String> installedDatabases = new HashSet<>(server.getDatabaseNames());
    final Set<String> allowedDatabases = user.getAuthorizedDatabases();

    if (!allowedDatabases.contains("*"))
      installedDatabases.retainAll(allowedDatabases);

    final boolean existsDatabase = installedDatabases.contains(databaseName.getFirst());

    final JSONObject response = createResult(user, null);
    response.put("result", existsDatabase);

    return new ExecutionResponse(200, response.toString());
  }
}
