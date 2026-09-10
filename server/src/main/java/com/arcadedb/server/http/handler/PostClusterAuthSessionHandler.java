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
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

/**
 * Cluster-internal {@code POST /api/v1/cluster/auth-session}: the node that issued an authentication token answers
 * a peer that received it (issue #7424). {@code validate} confirms the session and counts as activity on it;
 * {@code revoke} drops it, which is how a logout served by one node reaches every other.
 * <p>
 * Only a peer may ask: the request must have come in through the cluster-token channel. A caller presenting user
 * credentials, root included, is refused with 403, because the answer discloses which principal a token belongs
 * to and the route exists for nodes, not for users. A copy held here on behalf of another issuer never vouches:
 * {@code validate} answers 404 for it, so a stale copy cannot keep a revoked session alive through a third node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostClusterAuthSessionHandler extends AbstractServerHttpHandler {
  static final String CLUSTER_TOKEN_HEADER = "X-ArcadeDB-Cluster-Token";

  public PostClusterAuthSessionHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * The body is parsed only for handlers that run on a worker thread; this one reads a JSON body.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    if (!exchange.getRequestHeaders().contains(CLUSTER_TOKEN_HEADER))
      return new ExecutionResponse(403,
          new JSONObject().put("error", "This route answers cluster peers only").toString());
    checkRootUser(user);

    final String token = payload != null ? payload.getString("token", null) : null;
    if (token == null || token.isBlank())
      return new ExecutionResponse(400, new JSONObject().put("error", "Missing token").toString());

    final String action = payload.getString("action", "validate");
    switch (action) {
    case "validate" -> {
      final HttpAuthSession session = httpServer.getAuthSessionManager().getSessionByToken(token);
      if (session == null || session.isRemote())
        return new ExecutionResponse(404,
            new JSONObject().put("error", "Invalid or expired authentication token").toString());
      return new ExecutionResponse(200, new JSONObject()
          .put("user", session.getUser().getName())
          .put("createdAt", session.getCreatedAt())
          .toString());
    }
    case "revoke" -> {
      httpServer.getAuthSessionManager().removeSession(token);
      return new ExecutionResponse(204, "");
    }
    default -> {
      return new ExecutionResponse(400, new JSONObject().put("error", "Unknown action '" + action + "'").toString());
    }
    }
  }
}
