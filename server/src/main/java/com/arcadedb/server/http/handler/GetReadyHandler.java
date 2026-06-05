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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

public class GetReadyHandler extends AbstractServerHttpHandler {
  public GetReadyHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    Metrics.counter("http.ready").increment();

    final ArcadeDBServer server = httpServer.getServer();
    if (server.getStatus() != ArcadeDBServer.STATUS.ONLINE)
      return new ExecutionResponse(503, "Server not started yet");

    if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA)
        && server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      final HAServerPlugin ha = server.getHA();
      // ELECTION_STATUS.DONE means a leader is known; it does not guarantee this follower has
      // replicated all committed log entries, so a slow follower may report ready before catch-up.
      if (ha == null || ha.getElectionStatus() != HAServerPlugin.ELECTION_STATUS.DONE)
        return new ExecutionResponse(503, "Node has not yet joined the Raft group");
    }

    return new ExecutionResponse(204, "");
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }
}
