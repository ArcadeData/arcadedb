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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class PostUserHandler extends AbstractServerHttpHandler {

  public PostUserHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    if (payload == null)
      return new ExecutionResponse(400, "{\"error\":\"Request body is required\"}");

    final String name = payload.getString("name", "");
    if (name.isBlank())
      return new ExecutionResponse(400, "{\"error\":\"User name is required\"}");

    final String password = payload.getString("password", "");
    if (password.length() < 4)
      throw new ServerSecurityException("User password must be at least 4 characters");
    if (password.length() > 256)
      throw new ServerSecurityException("User password cannot be longer than 256 characters");

    final ServerSecurity security = httpServer.getServer().getSecurity();

    // Encode password and build user config
    final JSONObject userConfig = new JSONObject();
    userConfig.put("name", name);
    userConfig.put("password", security.encodePassword(password));

    if (payload.has("databases"))
      userConfig.put("databases", payload.getJSONObject("databases"));
    else
      userConfig.put("databases", new JSONObject());

    security.createUser(userConfig);

    final JSONObject response = new JSONObject();
    response.put("result", "User '" + name + "' created");
    return new ExecutionResponse(201, response.toString());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
