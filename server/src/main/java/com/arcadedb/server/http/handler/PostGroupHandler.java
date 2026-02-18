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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostGroupHandler extends AbstractServerHttpHandler {

  public PostGroupHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    if (payload == null)
      return new ExecutionResponse(400, "{\"error\":\"Request body is required\"}");

    final String database = payload.getString("database", "");
    if (database.isBlank())
      return new ExecutionResponse(400, "{\"error\":\"Database name is required\"}");

    final String name = payload.getString("name", "");
    if (name.isBlank())
      return new ExecutionResponse(400, "{\"error\":\"Group name is required\"}");

    // Build group config
    final JSONObject groupConfig = new JSONObject();
    groupConfig.put("resultSetLimit", payload.getLong("resultSetLimit", -1L));
    groupConfig.put("readTimeout", payload.getLong("readTimeout", -1L));
    groupConfig.put("access", payload.has("access") ? payload.getJSONArray("access") : new JSONArray());
    groupConfig.put("types", payload.has("types") ? payload.getJSONObject("types") : new JSONObject());

    final ServerSecurity security = httpServer.getServer().getSecurity();
    security.saveGroup(database, name, groupConfig);

    // Refresh permissions on affected open databases
    for (final String dbName : httpServer.getServer().getDatabaseNames()) {
      if ("*".equals(database) || dbName.equals(database)) {
        final DatabaseInternal db = (DatabaseInternal) httpServer.getServer().getDatabase(dbName);
        security.updateSchema(db);
      }
    }

    final JSONObject response = new JSONObject();
    response.put("result", "Group '" + name + "' saved for database '" + database + "'");
    return new ExecutionResponse(200, response.toString());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
