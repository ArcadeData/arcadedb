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

/**
 * POST /api/v1/cluster/peer - adds a peer to the Raft cluster.
 * Body: {"peerId": "...", "address": "host:raftPort", "httpAddress": "host:httpPort"}
 * The httpAddress field is optional. If omitted, it is derived from the Raft address using the
 * local node's port offset (which may be incorrect if the new peer has a non-standard port layout).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostAddPeerHandler extends AbstractServerHttpHandler {
  private final RaftHAServer raftHA;

  public PostAddPeerHandler(final HttpServer httpServer, final RaftHAServer raftHA) {
    super(httpServer);
    this.raftHA = raftHA;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final String peerId = payload.getString("peerId");
    final String address = payload.getString("address");

    if (peerId == null || peerId.isEmpty() || address == null || address.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Both 'peerId' and 'address' are required\"}");

    try {
      RaftHAServer.validatePeerAddress(address);
    } catch (final ConfigurationException e) {
      return new ExecutionResponse(400, "{ \"error\" : \"Invalid peer address: " + e.getMessage() + "\"}");
    }

    final String httpAddress = payload.has("httpAddress") ? payload.getString("httpAddress") : null;
    raftHA.addPeer(peerId, address, httpAddress);

    final JSONObject response = new JSONObject();
    response.put("result", "Peer " + peerId + " added");
    return new ExecutionResponse(200, response.toString());
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }
}
