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
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * {@code POST /server/groups}: creates or replaces one group. The operation - normalizing the group
 * document and refreshing the permissions cached by every open database it applies to - lives in
 * {@link ServerControlPlane#saveGroup} so the gRPC {@code SaveGroup} RPC does both halves too
 * (issue #7309).
 */
public class PostGroupHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public PostGroupHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    final String database = payload.getString("database", "");
    final String name = payload.getString("name", "");

    try {
      controlPlane.saveGroup(database, name, payload);
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
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
