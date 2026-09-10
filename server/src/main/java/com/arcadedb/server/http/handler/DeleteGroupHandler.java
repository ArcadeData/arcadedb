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
 * {@code DELETE /server/groups?database=<db>&name=<group>}: drops one group. Delegates to
 * {@link ServerControlPlane#deleteGroup}, which also refuses to drop the {@code admin} group of the
 * default database and refreshes the permissions of every open database the group applied to, so the
 * gRPC {@code DeleteGroup} RPC gets both (issue #7309).
 */
public class DeleteGroupHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public DeleteGroupHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final String database = getQueryParameter(exchange, "database");
    final String name = getQueryParameter(exchange, "name");

    try {
      controlPlane.deleteGroup(database, name);
    } catch (final ServerControlPlane.NotFoundException e) {
      return new ExecutionResponse(404, new JSONObject().put("error", e.getMessage()).toString());
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
    }

    final JSONObject response = new JSONObject();
    response.put("result", "Group '" + name + "' deleted from database '" + database + "'");
    return new ExecutionResponse(200, response.toString());
  }
}
