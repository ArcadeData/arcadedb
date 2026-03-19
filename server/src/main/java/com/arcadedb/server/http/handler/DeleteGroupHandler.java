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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DeleteGroupHandler extends AbstractServerHttpHandler {

  public DeleteGroupHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final String database = getQueryParameter(exchange, "database");
    if (database == null || database.isBlank())
      return new ExecutionResponse(400, new JSONObject().put("error", "Database parameter is required").toString());

    final String name = getQueryParameter(exchange, "name");
    if (name == null || name.isBlank())
      return new ExecutionResponse(400, new JSONObject().put("error", "Group name parameter is required").toString());

    if ("admin".equals(name) && "*".equals(database))
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Cannot delete the admin group from the default (*) database").toString());

    final ServerSecurity security = httpServer.getServer().getSecurity();
    final boolean deleted = security.deleteGroup(database, name);

    if (!deleted) {
      final JSONObject response = new JSONObject();
      response.put("error", "Group '" + name + "' not found in database '" + database + "'");
      return new ExecutionResponse(404, response.toString());
    }

    // Refresh permissions on affected open databases
    for (final String dbName : httpServer.getServer().getDatabaseNames()) {
      if ("*".equals(database) || dbName.equals(database)) {
        final DatabaseInternal db = (DatabaseInternal) httpServer.getServer().getDatabase(dbName);
        security.updateSchema(db);
      }
    }

    final JSONObject response = new JSONObject();
    response.put("result", "Group '" + name + "' deleted from database '" + database + "'");
    return new ExecutionResponse(200, response.toString());
  }
}
