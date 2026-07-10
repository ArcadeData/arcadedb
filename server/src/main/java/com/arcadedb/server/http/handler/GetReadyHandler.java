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
      // First gate: a leader must be known (election settled). Necessary but not sufficient - the deeper
      // consensus gate below also requires committed-config membership and follower catch-up.
      if (ha == null || ha.getElectionStatus() != HAServerPlugin.ELECTION_STATUS.DONE)
        return new ExecutionResponse(503, "Node has not yet joined the Raft group");

      // Deeper consensus gate: the node must be in the current Raft configuration and (for a follower) have
      // replayed the committed log to within a small bound. Without it, a restarted follower with a
      // wiped/lagging log would report Ready before catch-up and a rolling restart could drop the write
      // quorum. A null signal means the HA implementation provides none: no extra gating. The lag bound is
      // clamped to >= 0 so a misconfigured negative value cannot wedge a caught-up node out of readiness.
      final long maxLag = Math.max(0L, server.getConfiguration().getValueAsLong(GlobalConfiguration.SERVER_READINESS_HA_MAX_LAG));
      if (ha.getReadinessSignal(maxLag) == HAServerPlugin.READINESS_SIGNAL.NOT_READY)
        return new ExecutionResponse(503, "Node is not yet in the Raft configuration or has not caught up");
    }

    return new ExecutionResponse(204, "");
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }
}
