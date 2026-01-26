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
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;

/**
 * Handles POST /api/v1/logout endpoint.
 * <p>
 * Invalidates an authentication token so it can no longer be used for requests.
 * The token must be provided via the Authorization: Bearer header.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1691">GitHub Issue #1691</a>
 */
public class PostLogoutHandler extends AbstractServerHttpHandler {
  private static final String AUTHORIZATION_BEARER = "Bearer";

  public PostLogoutHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    // Get token from Authorization: Bearer header
    final HeaderValues authorization = exchange.getRequestHeaders().get("Authorization");
    if (authorization != null && !authorization.isEmpty()) {
      final String auth = authorization.getFirst();
      if (auth.startsWith(AUTHORIZATION_BEARER)) {
        final String token = auth.substring(AUTHORIZATION_BEARER.length()).trim();
        httpServer.getAuthSessionManager().removeSession(token);
      }
    }

    Metrics.counter("http.logout").increment();

    return new ExecutionResponse(204, "");
  }
}
