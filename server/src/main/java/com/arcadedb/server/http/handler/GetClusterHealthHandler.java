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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

/**
 * Returns cluster health information including replica status and metrics.
 * Endpoint: GET /api/v1/cluster/health
 */
public class GetClusterHealthHandler extends AbstractServerHttpHandler {

  public GetClusterHealthHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange,
                                    final ServerSecurityUser user,
                                    final JSONObject payload) {
    Metrics.counter("http.cluster-health").increment();

    final HAServer ha = httpServer.getServer().getHA();
    if (ha == null) {
      return new ExecutionResponse(503, new JSONObject()
          .put("error", "HA not enabled")
          .put("message", "High Availability is not configured on this server")
          .toString());
    }

    final JSONObject response = new JSONObject();

    // Collect cluster health data
    response.put("status", "HEALTHY"); // TODO: Calculate actual health
    response.put("serverName", httpServer.getServer().getServerName());
    response.put("clusterName", ha.getClusterName());
    response.put("isLeader", ha.isLeader());
    response.put("leaderName", ha.getLeaderName());
    response.put("electionStatus", ha.getElectionStatus().toString());

    if (ha.isLeader()) {
      response.put("onlineReplicas", ha.getOnlineReplicas());
      response.put("configuredServers", ha.getConfiguredServers());

      // Add replica information
      // TODO: Collect replica metrics from Leader2ReplicaNetworkExecutor instances
      response.put("replicas", new JSONObject());
    }

    return new ExecutionResponse(200, response.toString());
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }
}
