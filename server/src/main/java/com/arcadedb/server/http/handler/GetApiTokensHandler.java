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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * {@code GET /server/api-tokens}: the issued tokens, as metadata plus the hash needed to revoke each
 * one. The projection - which never includes token material - lives in
 * {@link ServerControlPlane#listApiTokens} so the gRPC {@code ListApiTokens} RPC returns exactly the
 * same fields (issue #7309).
 */
public class GetApiTokensHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public GetApiTokensHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final JSONArray tokens = controlPlane.listApiTokens();

    final JSONObject response = new JSONObject();
    response.put("result", tokens);
    response.put("count", tokens.length());
    return new ExecutionResponse(200, response.toString());
  }
}
