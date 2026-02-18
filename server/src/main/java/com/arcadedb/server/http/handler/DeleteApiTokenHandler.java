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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class DeleteApiTokenHandler extends AbstractServerHttpHandler {

  public DeleteApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final String token = getQueryParameter(exchange, "token");
    if (token == null || token.isBlank())
      return new ExecutionResponse(400, "{\"error\":\"Token parameter is required\"}");

    final ApiTokenConfiguration tokenConfig = httpServer.getServer().getSecurity().getApiTokenConfiguration();
    final boolean deleted = tokenConfig.deleteToken(token);

    final JSONObject response = new JSONObject();
    response.put("result", deleted ? "Token deleted" : "Token not found");
    return new ExecutionResponse(deleted ? 200 : 404, response.toString());
  }
}
