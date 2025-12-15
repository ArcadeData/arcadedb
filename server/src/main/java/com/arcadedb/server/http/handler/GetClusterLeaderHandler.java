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
 * Returns current leader information for the cluster.
 * Endpoint: GET /api/v1/cluster/leader
 *
 * Returns JSON with:
 * - leader: Leader server info (host, port, alias)
 * - httpAddress: Leader's HTTP address
 * - electionStatus: Current election status
 * - isCurrentNode: Whether this node is the leader
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GetClusterLeaderHandler extends AbstractServerHttpHandler {
  public GetClusterLeaderHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    Metrics.counter("http.cluster-leader").increment();

    final HAServer ha = httpServer.getServer().getHA();
    if (ha == null) {
      return new ExecutionResponse(503, new JSONObject()
          .put("error", "HA not enabled")
          .put("message", "High Availability is not configured on this server")
          .toString());
    }

    final JSONObject response = new JSONObject();

    response.put("electionStatus", ha.getElectionStatus().toString());
    response.put("isCurrentNode", ha.isLeader());

    // Leader name (alias)
    final String leaderName = ha.getLeaderName();
    response.put("leaderName", leaderName);

    if (ha.isLeader()) {
      // Current node is the leader
      final HAServer.ServerInfo leaderInfo = ha.getServerAddress();
      response.put("leader", createServerInfoJSON(leaderInfo));

      // Get HTTP address for current server
      final String httpAddress = httpServer.getListeningAddress();
      if (httpAddress != null) {
        response.put("httpAddress", httpAddress);
      }
    } else if (ha.getLeader() != null) {
      // Another node is the leader - get leader address from cluster
      final String leaderAddress = ha.getLeader().getRemoteAddress();
      response.put("leaderAddress", leaderAddress);

      // Get HTTP address from leader connection
      final String httpAddress = ha.getLeader().getRemoteHTTPAddress();
      if (httpAddress != null) {
        response.put("httpAddress", httpAddress);
      }

      // Try to find leader ServerInfo from cluster
      if (ha.getCluster() != null) {
        final java.util.Optional<HAServer.ServerInfo> leaderInfo = ha.getCluster().findByAlias(leaderName);
        if (leaderInfo.isPresent()) {
          response.put("leader", createServerInfoJSON(leaderInfo.get()));
        }
      }
    } else {
      // No leader elected yet
      response.put("leader", JSONObject.NULL);
      response.put("httpAddress", JSONObject.NULL);
      response.put("message", "No leader elected yet");
    }

    return new ExecutionResponse(200, response.toString());
  }

  private JSONObject createServerInfoJSON(HAServer.ServerInfo serverInfo) {
    if (serverInfo == null) {
      return new JSONObject();
    }

    return new JSONObject()
        .put("host", serverInfo.host())
        .put("port", serverInfo.port())
        .put("alias", serverInfo.alias());
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }
}
