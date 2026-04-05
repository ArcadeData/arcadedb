/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Returns Raft cluster status: local peer, leader, and peer list with roles.
 * Registered at {@code GET /api/v1/cluster} by {@link RaftHAPlugin}.
 * <p>
 * Holds a reference to the plugin (not the server) because the endpoint is registered
 * during HTTP setup, before the Raft server is started. The actual {@link RaftHAServer}
 * is resolved lazily at request time via {@link RaftHAPlugin#getRaftHAServer()}.
 */
public class GetClusterHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public GetClusterHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(503, new JSONObject().put("error", "Raft HA not started yet").toString());

    final JSONObject response = new JSONObject();

    response.put("implementation", "raft");
    response.put("clusterName", httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.HA_CLUSTER_NAME));

    final RaftPeerId localPeerId = raftHAServer.getLocalPeerId();
    response.put("localPeerId", localPeerId.toString());

    final boolean isLeader = raftHAServer.isLeader();
    response.put("isLeader", isLeader);

    final RaftPeerId leaderId = raftHAServer.getLeaderId();
    response.put("leaderId", leaderId != null ? leaderId.toString() : JSONObject.NULL);

    final String leaderHttpAddress = raftHAServer.getLeaderHttpAddress();
    response.put("leaderHttpAddress", leaderHttpAddress != null ? leaderHttpAddress : JSONObject.NULL);

    final ArcadeStateMachine stateMachine = raftHAServer.getStateMachine();
    response.put("electionCount", stateMachine.getElectionCount());
    response.put("lastElectionTime", stateMachine.getLastElectionTime());
    response.put("uptime", System.currentTimeMillis() - stateMachine.getStartTime());

    final JSONArray peers = new JSONArray();
    for (final RaftPeer peer : raftHAServer.getRaftGroup().getPeers()) {
      final JSONObject peerJson = new JSONObject();
      peerJson.put("id", peer.getId().toString());
      peerJson.put("address", peer.getAddress());

      if (leaderId != null && peer.getId().equals(leaderId))
        peerJson.put("role", "LEADER");
      else
        peerJson.put("role", "FOLLOWER");

      peers.put(peerJson);
    }
    response.put("peers", peers);

    return new ExecutionResponse(200, response.toString());
  }
}
