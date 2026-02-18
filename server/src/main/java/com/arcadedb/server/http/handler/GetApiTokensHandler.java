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

import java.util.List;

public class GetApiTokensHandler extends AbstractServerHttpHandler {

  public GetApiTokensHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final ApiTokenConfiguration tokenConfig = httpServer.getServer().getSecurity().getApiTokenConfiguration();
    final List<JSONObject> tokens = tokenConfig.listTokens();

    final JSONArray result = new JSONArray();
    for (final JSONObject token : tokens) {
      final JSONObject masked = new JSONObject();
      masked.put("name", token.getString("name"));
      masked.put("database", token.getString("database"));
      masked.put("expiresAt", token.getLong("expiresAt", 0));
      masked.put("createdAt", token.getLong("createdAt", 0));
      masked.put("permissions", token.getJSONObject("permissions"));

      // Mask the token value: show first 6 + last 4 chars
      final String tokenValue = token.getString("token");
      if (tokenValue.length() > 10)
        masked.put("token", tokenValue.substring(0, 6) + "..." + tokenValue.substring(tokenValue.length() - 4));
      else
        masked.put("token", "***");

      result.put(masked);
    }

    final JSONObject response = new JSONObject();
    response.put("result", result);
    response.put("count", tokens.size());
    return new ExecutionResponse(200, response.toString());
  }
}
