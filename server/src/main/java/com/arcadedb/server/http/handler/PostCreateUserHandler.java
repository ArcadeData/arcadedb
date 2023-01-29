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

import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Creates a new server user.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @Deprecated Use the generic @see PostServerCommandHandler
 */
@Deprecated
public class PostCreateUserHandler extends AbstractHandler {
  public PostCreateUserHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
    checkRootUser(user);

    final String payload = parseRequestPayload(exchange);
    if (payload == null || payload.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Payload requested\"}");

    final JSONObject json = new JSONObject(payload);

    if (!json.has("name"))
      return new ExecutionResponse(400, "{ \"error\" : \"User name is null\"}");

    final String userPassword = json.getString("password");
    if (userPassword.length() < 4)
      throw new ServerSecurityException("User password must be 5 minimum characters");
    if (userPassword.length() > 256)
      throw new ServerSecurityException("User password cannot be longer than 256 characters");

    json.put("password", httpServer.getServer().getSecurity().encodePassword(userPassword));

    httpServer.getServer().getServerMetrics().meter("http.create-user").mark();

    httpServer.getServer().getSecurity().createUser(json);

    return new ExecutionResponse(204, "");
  }
}
