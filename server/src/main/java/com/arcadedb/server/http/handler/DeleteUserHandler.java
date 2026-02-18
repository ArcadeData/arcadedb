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
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class DeleteUserHandler extends AbstractServerHttpHandler {

  public DeleteUserHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final String name = getQueryParameter(exchange, "name");
    if (name == null || name.isBlank())
      return new ExecutionResponse(400, "{\"error\":\"User name parameter is required\"}");

    if ("root".equals(name))
      return new ExecutionResponse(400, "{\"error\":\"Cannot delete the root user\"}");

    final boolean deleted = httpServer.getServer().getSecurity().dropUser(name);

    final JSONObject response = new JSONObject();
    if (deleted) {
      response.put("result", "User '" + name + "' deleted");
      return new ExecutionResponse(200, response.toString());
    }

    response.put("error", "User '" + name + "' not found");
    return new ExecutionResponse(404, response.toString());
  }
}
