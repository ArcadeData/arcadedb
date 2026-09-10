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
 * {@code POST /server/api-tokens}: mints a token and returns it, plaintext, exactly once. Minting and
 * the validation of the permission document live in {@link ServerControlPlane#createApiToken} so the
 * gRPC {@code CreateApiToken} RPC applies the same rules (issue #7309).
 * <p>
 * This route applies no transport check: it mints over whichever listener the request arrived on. The
 * gRPC RPC does apply one, because it was added with the gate rather than inheriting years of
 * behaviour. Bringing the two into line is issue #7372.
 */
public class PostApiTokenHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public PostApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    if (payload == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Request body is required").toString());

    final JSONObject tokenJson;
    try {
      tokenJson = controlPlane.createApiToken(
          payload.getString("name", ""),
          payload.getString("database", "*"),
          payload.getLong("expiresAt", 0),
          payload.getJSONObject("permissions", new JSONObject()));
    } catch (final ServerControlPlane.AlreadyExistsException e) {
      return new ExecutionResponse(409, new JSONObject().put("error", e.getMessage()).toString());
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
    }

    final JSONObject response = new JSONObject();
    response.put("result", tokenJson);
    return new ExecutionResponse(201, response.toString());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
