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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.logging.Level;

public class PostAddPeerHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostAddPeerHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
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
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String peerId = payload.getString("peerId", "");
    final String address = payload.getString("address", "");
    final String name = payload.getString("name", "").trim();
    if (peerId.isEmpty() || address.isEmpty())
      return new ExecutionResponse(400,
          new JSONObject().put("error", "Missing required fields: peerId, address").toString());

    raftHAServer.addPeer(peerId, address, name.isEmpty() ? null : name);

    // Seed the newly-joined peer with the current security files. Snapshot install covers none of them
    // (they live under <server-root>/config/, outside the database directory), so without this explicit
    // seed the new peer would start with whatever its own files hold - a stale user set, a stale group
    // document, a stale token store - until the next mutation of that kind happens cluster-wide. The
    // groups and tokens half is issue #7373; the users half predates it.
    //
    // Best-effort, and each kind is seeded independently: a failure does not roll back the peer addition,
    // and one failing seed must not skip the other two.
    final ServerSecurity security = httpServer.getServer().getSecurity();
    seed(peerId, "users", () -> plugin.replicateSecurityUsers(security.getUsersJsonPayload()));
    seed(peerId, "groups", () -> plugin.replicateSecurityGroups(security.getGroupsJsonPayload()));
    seed(peerId, "API tokens", () -> plugin.replicateSecurityApiTokens(security.getApiTokensJsonPayload()));

    return new ExecutionResponse(200,
        new JSONObject().put("result", "Peer " + peerId + " added").toString());
  }

  private void seed(final String peerId, final String what, final Runnable seeding) {
    try {
      seeding.run();
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "%s seed to new peer '%s' failed (best-effort): %s", what, peerId, e.getMessage());
    }
  }
}
