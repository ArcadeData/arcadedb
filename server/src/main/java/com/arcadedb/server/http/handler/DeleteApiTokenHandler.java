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
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;

/**
 * {@code DELETE /server/api-tokens?token=<hash>}: revokes a token by its hash. The refusal to accept
 * a plaintext token here - it would land in whatever logged the request, which is the exposure the
 * revocation is ending - lives in {@link ServerControlPlane#deleteApiToken} so the gRPC
 * {@code DeleteApiToken} RPC refuses it too (issue #7309).
 * <p>
 * On an HA cluster the request is forwarded to the leader first, as {@code /server/users} is (issue #8109): see
 * {@link PostGroupHandler}.
 */
public class DeleteApiTokenHandler extends AbstractServerHttpHandler {
  private final ServerControlPlane controlPlane;

  public DeleteApiTokenHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    // deleteApiToken() below reaches the same Raft submit-and-wait with compare-and-set retries as
    // DeleteGroupHandler on a replicated database (issue #7621). Running that on the Undertow IO thread
    // stalls every other connection on the same selector, including kubelet readiness/liveness probes
    // (issue #7133).
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws IOException {
    checkRootUser(user);

    // A plaintext token is refused HERE, before the forward below: relaying it would copy the live token material
    // into a second node's request path and logs, which is the exposure the refusal exists to end.
    final String token = getQueryParameter(exchange, "token");
    if (token != null && ApiTokenConfiguration.isApiToken(token))
      return new ExecutionResponse(400,
          new JSONObject().put("error", ServerControlPlane.PLAINTEXT_TOKEN_DELETE_REFUSAL).toString());

    // Every other check runs on the leader (issue #8109). The token hash travels in the query string, so the leader
    // reads the same one.
    final ExecutionResponse forwarded = httpServer.getLeaderCommandForwarder()
        .forwardIfReplica(exchange, user, LeaderCommandForwarder.currentPathWithQuery(exchange), null);
    if (forwarded != null)
      return forwarded;

    try {
      controlPlane.deleteApiToken(token);
    } catch (final ServerControlPlane.NotFoundException e) {
      // 'error', not 'result': RemoteHttpComponent.manageException promotes 'error'/'detail'/'exception'
      // into the thrown exception's message and nothing else, so a refusal filed under 'result' reached a
      // Java caller as a bare "HTTP Error (httpErrorCode=404 ...)" with the sentence the server had
      // written stripped out. Every sibling handler on these routes already answers 'error' (issue #7372).
      // e.getMessage(), not the same sentence written out again: DeleteGroupHandler and PutUserHandler
      // forward theirs, and a literal copied from ServerControlPlane stops matching it the moment that
      // message grows the detail - the token hash, say - that made it worth changing.
      return new ExecutionResponse(404, new JSONObject().put("error", e.getMessage()).toString());
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
    }

    return new ExecutionResponse(200, new JSONObject().put("result", "Token deleted").toString());
  }
}
