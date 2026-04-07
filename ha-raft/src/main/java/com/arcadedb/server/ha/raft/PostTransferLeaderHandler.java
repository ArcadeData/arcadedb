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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

public class PostTransferLeaderHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostTransferLeaderHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String peerId = payload.getString("peerId", "");
    final long timeoutMs = payload.getLong("timeoutMs", 30_000);

    if (peerId.isEmpty()) {
      // Transfer to any peer (Ratis picks the best candidate)
      final boolean success = raftHAServer.transferLeadership(timeoutMs);
      if (success)
        return new ExecutionResponse(200,
            new JSONObject().put("result", "Leadership transferred").toString());
      return new ExecutionResponse(500,
          new JSONObject().put("error", "Leadership transfer failed").toString());
    }

    raftHAServer.transferLeadership(peerId, timeoutMs);
    return new ExecutionResponse(200,
        new JSONObject().put("result", "Leadership transferred to " + peerId).toString());
  }
}
