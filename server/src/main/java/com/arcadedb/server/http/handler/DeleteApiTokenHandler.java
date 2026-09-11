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
 * {@code DELETE /server/api-tokens?token=<hash>}: revokes a token by its hash. The refusal to accept
 * a plaintext token here - it would land in whatever logged the request, which is the exposure the
 * revocation is ending - lives in {@link ServerControlPlane#deleteApiToken} so the gRPC
 * {@code DeleteApiToken} RPC refuses it too (issue #7309).
 */
public class DeleteApiTokenHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public DeleteApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    try {
      controlPlane.deleteApiToken(getQueryParameter(exchange, "token"));
    } catch (final ServerControlPlane.NotFoundException e) {
      return new ExecutionResponse(404, new JSONObject().put("result", "Token not found").toString());
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
    }

    return new ExecutionResponse(200, new JSONObject().put("result", "Token deleted").toString());
  }
}
