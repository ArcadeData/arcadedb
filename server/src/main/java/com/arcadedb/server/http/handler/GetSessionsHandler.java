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
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.util.List;

/**
 * Handles GET /api/v1/sessions endpoint.
 * <p>
 * Returns the list of active authentication sessions. This endpoint is restricted
 * to root users only for security reasons.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/1691">GitHub Issue #1691</a>
 */
public class GetSessionsHandler extends AbstractServerHttpHandler {

  public GetSessionsHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    // Only root users can list sessions
    checkRootUser(user);

    final List<HttpAuthSession> sessions = httpServer.getAuthSessionManager().getActiveSessions();

    final JSONArray sessionsArray = new JSONArray();
    for (final HttpAuthSession session : sessions) {
      final JSONObject sessionJson = new JSONObject();
      sessionJson.put("token", session.getToken());
      sessionJson.put("user", session.getUser().getName());
      sessionJson.put("elapsedMs", session.elapsedFromLastUpdate());
      sessionsArray.put(sessionJson);
    }

    final JSONObject response = new JSONObject();
    response.put("result", sessionsArray);
    response.put("count", sessions.size());

    Metrics.counter("http.sessions").increment();

    return new ExecutionResponse(200, response.toString());
  }
}
