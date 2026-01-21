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
import com.arcadedb.server.ha.ReplicaConnectionMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

import java.util.Map;

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

    // Collect basic cluster data
    response.put("serverName", httpServer.getServer().getServerName());
    response.put("clusterName", ha.getClusterName());
    response.put("isLeader", ha.isLeader());
    response.put("role", ha.isLeader() ? "Leader" : "Replica");
    response.put("leaderName", ha.getLeaderName());
    response.put("electionStatus", ha.getElectionStatus().toString());

    final int configuredServers = ha.getConfiguredServers();
    final int onlineServers = ha.getOnlineServers();
    final int onlineReplicas = ha.getOnlineReplicas();

    response.put("configuredServers", configuredServers);
    response.put("onlineServers", onlineServers);
    response.put("onlineReplicas", onlineReplicas);
    response.put("quorumAvailable", onlineServers >= (configuredServers / 2 + 1));

    // Calculate cluster health status
    String healthStatus = "HEALTHY";

    if (ha.isLeader()) {
      final int expectedReplicas = configuredServers - 1; // Exclude leader

      // Check if we have all expected replicas online
      if (onlineReplicas < expectedReplicas) {
        healthStatus = "DEGRADED";
      }

      // Check if any replica has excessive failures
      for (Leader2ReplicaNetworkExecutor executor : ha.getReplicaConnections().values()) {
        if (executor.getMetrics().getConsecutiveFailures() > 5) {
          healthStatus = "DEGRADED";
        }
        if (executor.getStatus() == Leader2ReplicaNetworkExecutor.STATUS.FAILED) {
          healthStatus = "UNHEALTHY";
        }
      }

      // Check quorum - if less than majority, cluster is unhealthy
      if (onlineServers < (configuredServers / 2 + 1)) {
        healthStatus = "UNHEALTHY";
      }
    } else {
      // Replica health - check if connected to leader
      if (ha.getLeader() == null || !ha.getLeader().isAlive()) {
        healthStatus = "DEGRADED";
      }
    }

    response.put("status", healthStatus);

    if (ha.isLeader()) {
      // Collect replica metrics
      final JSONObject replicasJson = new JSONObject();
      final JSONObject replicaStatusesJson = new JSONObject(); // For backward compatibility
      int totalQueueSize = 0;

      for (Map.Entry<String, Leader2ReplicaNetworkExecutor> entry :
           ha.getReplicaConnections().entrySet()) {

        final String replicaName = entry.getKey();
        final Leader2ReplicaNetworkExecutor executor = entry.getValue();
        final ReplicaConnectionMetrics metrics = executor.getMetrics();

        final JSONObject replicaJson = new JSONObject();
        replicaJson.put("status", executor.getStatus().toString());
        replicaJson.put("queueSize", executor.getMessagesInQueue());
        replicaJson.put("consecutiveFailures", metrics.getConsecutiveFailures());
        replicaJson.put("totalReconnections", metrics.getTotalReconnections());
        replicaJson.put("transientFailures", metrics.getTransientNetworkFailures());
        replicaJson.put("leadershipChanges", metrics.getLeadershipChanges());
        replicaJson.put("circuitBreakerState", executor.getCircuitBreakerState().toString());

        replicasJson.put(replicaName, replicaJson);
        replicaStatusesJson.put(replicaName, executor.getStatus().toString());

        // Track aggregate metrics
        totalQueueSize += executor.getMessagesInQueue();
      }

      response.put("replicas", replicasJson);
      response.put("replicaStatuses", replicaStatusesJson); // For backward compatibility
      response.put("totalQueueSize", totalQueueSize);
    }

    return new ExecutionResponse(200, response.toString());
  }

  @Override
  public boolean isRequireAuthentication() {
    return false;
  }
}
