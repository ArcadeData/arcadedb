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

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;

import io.undertow.server.HttpServerExchange;

import java.util.Deque;

public class DeletePeerHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public DeletePeerHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String relativePath = exchange.getRelativePath();
    final String peerId = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
    if (peerId.isEmpty())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing peerId in path").toString());

    final boolean force = isForce(exchange);
    try {
      raftHAServer.removePeer(peerId, force);
    } catch (final ConfigurationException e) {
      // Quorum guard refusal or a failed configuration change: surface it instead of a misleading 200.
      return new ExecutionResponse(409, new JSONObject().put("error", e.getMessage()).toString());
    }
    return new ExecutionResponse(200,
        new JSONObject().put("result", "Peer " + peerId + " removed").toString());
  }

  private static boolean isForce(final HttpServerExchange exchange) {
    final Deque<String> forceParam = exchange.getQueryParameters().get("force");
    return forceParam != null && !forceParam.isEmpty() && Boolean.parseBoolean(forceParam.getFirst());
  }
}
