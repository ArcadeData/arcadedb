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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class PostApiTokenHandler extends AbstractServerHttpHandler {

  public PostApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    final String name = payload.getString("name", "");
    if (name.isBlank())
      return new ExecutionResponse(400, new JSONObject().put("error", "Token name is required").toString());

    final String database = payload.getString("database", "*");
    final long expiresAt = payload.getLong("expiresAt", 0);
    final JSONObject permissions = payload.getJSONObject("permissions", new JSONObject());

    // Validate permissions structure
    final String validationError = validatePermissions(permissions);
    if (validationError != null)
      return new ExecutionResponse(400, new JSONObject().put("error", validationError).toString());

    final ApiTokenConfiguration tokenConfig = httpServer.getServer().getSecurity().getApiTokenConfiguration();
    final JSONObject tokenJson = tokenConfig.createToken(name, database, expiresAt, permissions);

    final JSONObject response = new JSONObject();
    response.put("result", tokenJson);
    return new ExecutionResponse(201, response.toString());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  private static final java.util.Set<String> VALID_ACCESS_VALUES = java.util.Set.of(
      "createRecord", "readRecord", "updateRecord", "deleteRecord");

  private static String validatePermissions(final JSONObject permissions) {
    if (permissions.has("types")) {
      final Object typesObj = permissions.get("types");
      if (!(typesObj instanceof JSONObject))
        return "'permissions.types' must be a JSON object";

      final JSONObject types = (JSONObject) typesObj;
      for (final String typeName : types.keySet()) {
        final Object typeDef = types.get(typeName);
        if (!(typeDef instanceof JSONObject))
          return "'permissions.types." + typeName + "' must be a JSON object";

        final JSONObject typeObj = (JSONObject) typeDef;
        if (typeObj.has("access")) {
          final Object accessObj = typeObj.get("access");
          if (!(accessObj instanceof JSONArray))
            return "'permissions.types." + typeName + ".access' must be a JSON array";

          final JSONArray access = (JSONArray) accessObj;
          for (int i = 0; i < access.length(); i++) {
            final String value = access.getString(i);
            if (!VALID_ACCESS_VALUES.contains(value))
              return "Invalid access value '" + value + "' in permissions.types." + typeName
                  + ". Valid values: " + VALID_ACCESS_VALUES;
          }
        }
      }
    }

    if (permissions.has("database")) {
      final Object dbObj = permissions.get("database");
      if (!(dbObj instanceof JSONArray))
        return "'permissions.database' must be a JSON array";
    }

    return null;
  }
}
