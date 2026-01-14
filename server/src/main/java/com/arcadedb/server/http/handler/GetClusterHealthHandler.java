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
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.Leader2ReplicaNetworkExecutor;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.HashMap;
import java.util.Map;

/**
 * Returns cluster health information for monitoring and debugging.
 * Provides details about leader/replica role, online replica count, and individual replica statuses.
 */
public class GetClusterHealthHandler extends AbstractServerHttpHandler {
  public GetClusterHealthHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final HAServer ha = httpServer.getServer().getHA();

    if (ha == null) {
      final JSONObject errorResponse = new JSONObject().put("error", "HA not enabled on this server");
      return new ExecutionResponse(404, errorResponse.toString());
    }

    // Build replica statuses map
    final Map<String, String> replicaStatuses = new HashMap<>();
    if (ha.isLeader()) {
      // Only leader has visibility into replica statuses
      for (var entry : ha.getReplicaStatuses().entrySet()) {
        replicaStatuses.put(entry.getKey(), entry.getValue().toString());
      }
    }

    // Calculate if majority quorum is available
    final int onlineServers = ha.getOnlineServers();
    final int configuredServers = ha.getConfiguredServers();
    final boolean quorumAvailable = onlineServers >= (configuredServers + 1) / 2;

    // Build health response
    final JSONObject health = new JSONObject();
    health.put("serverName", httpServer.getServer().getServerName());
    health.put("role", ha.isLeader() ? "Leader" : "Replica");
    health.put("configuredServers", configuredServers);
    health.put("onlineServers", onlineServers);
    health.put("onlineReplicas", ha.getOnlineReplicas());
    health.put("quorumAvailable", quorumAvailable);
    health.put("electionStatus", ha.getElectionStatus().toString());

    if (!replicaStatuses.isEmpty()) {
      health.put("replicaStatuses", replicaStatuses);
    }

    return new ExecutionResponse(200, health.toString());
  }
}
