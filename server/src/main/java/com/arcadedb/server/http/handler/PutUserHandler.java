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
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PutUserHandler extends AbstractServerHttpHandler {

  public PutUserHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    final String name = getQueryParameter(exchange, "name");
    if (name == null || name.isBlank())
      return new ExecutionResponse(400, new JSONObject().put("error", "Query parameter 'name' is required").toString());

    final ServerSecurity security = httpServer.getServer().getSecurity();

    final ServerSecurityUser existingUser = security.getUser(name);
    if (existingUser == null)
      return new ExecutionResponse(404, new JSONObject().put("error", "User '" + name + "' not found").toString());

    // Build updated config from a copy to avoid mutating the live user object
    final JSONObject updatedConfig = existingUser.toJSON().copy();

    if (payload.has("password")) {
      final String password = payload.getString("password");
      if (password.length() < 8)
        return new ExecutionResponse(400, new JSONObject().put("error", "User password must be at least 8 characters").toString());
      if (password.length() > 256)
        return new ExecutionResponse(400, new JSONObject().put("error", "User password cannot be longer than 256 characters").toString());
      updatedConfig.put("password", security.encodePassword(password));
    }

    if (payload.has("databases"))
      updatedConfig.put("databases", payload.getJSONObject("databases"));

    security.updateUser(updatedConfig);

    final JSONObject response = new JSONObject();
    response.put("result", "User '" + name + "' updated");
    return new ExecutionResponse(200, response.toString());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
