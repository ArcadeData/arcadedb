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

import java.io.IOException;

/**
 * {@code PUT /server/users?name=<user>}: updates an existing user's password, per-database groups, or
 * both. The operation itself lives in {@link ServerControlPlane#updateUser} so the gRPC
 * {@code UpdateUser} RPC runs this exact code rather than a second copy of it (issue #7309); what is
 * left here is reading the request and choosing the status code.
 * <p>
 * On an HA cluster the request is forwarded to the leader first: the update submits a Raft entry (through
 * {@link com.arcadedb.server.security.ServerSecurity#updateUserClusterWide}), which a follower must not do
 * (issue #7380).
 */
public class PutUserHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public PutUserHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws IOException {
    checkRootUser(user);

    // Before any validation, exactly as the POST /server command path forwards before parsing its target
    // (issue #7380).
    final ExecutionResponse forwarded = httpServer.getLeaderCommandForwarder()
        .forwardIfReplica(exchange, user, LeaderCommandForwarder.currentPathWithQuery(exchange),
            payload != null ? payload.toString() : null);
    if (forwarded != null)
      return forwarded;

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    final String name = getQueryParameter(exchange, "name");
    if (name == null || name.isBlank())
      return new ExecutionResponse(400, new JSONObject().put("error", "Query parameter 'name' is required").toString());

    // Absent means "leave this part of the user alone", which is why both are read as nullable rather
    // than defaulted: a PUT carrying only a password must not clear the user's grants.
    final String password = payload.has("password") ? payload.getString("password") : null;
    final JSONObject databases = payload.has("databases") ? payload.getJSONObject("databases") : null;

    try {
      controlPlane.updateUser(name, password, databases);
    } catch (final ServerControlPlane.NotFoundException e) {
      return new ExecutionResponse(404, new JSONObject().put("error", e.getMessage()).toString());
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
    }

    final JSONObject response = new JSONObject();
    response.put("result", "User '" + name + "' updated");
    return new ExecutionResponse(200, response.toString());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
