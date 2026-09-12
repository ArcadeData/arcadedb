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
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.List;
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

    // Seed the newly-joined peer with the current security documents. Snapshot install covers none of them
    // (they live under <server-root>/config/, outside the database directory), so without this explicit
    // seed the new peer would start with whatever its own files hold - a stale user set, a stale group
    // document, a stale token store - until the next mutation of that kind happens cluster-wide. The
    // groups and tokens half is issue #7373; the users half predates it.
    //
    // Delegated to ServerSecurity so each document is READ and SUBMITTED under the security monitor. Reading
    // here and submitting afterwards would leave a window in which a revocation commits in between, and the
    // seed - which carries a whole document - would then put the revoked token, or the deleted group, back on
    // every node. addPeer is exactly when an operator is also likely to be rotating credentials.
    final List<String> failedSeeds = httpServer.getServer().getSecurity().seedSecurityStateClusterWide();

    // Still best-effort: a failed seed does not roll back the peer addition, because the peer is already a
    // cluster member and removing it again is a second failure mode rather than a repair. But the response says
    // so rather than reporting a flat success - the operator is the one who has to reissue the seed, and they
    // cannot do that if the only record is a WARNING in this node's log (issue #7521).
    final JSONObject response = new JSONObject().put("result", "Peer " + peerId + " added");
    if (!failedSeeds.isEmpty()) {
      response.put("warning", "Peer added, but the following security documents could NOT be seeded to it: "
          + String.join(", ", failedSeeds)
          + ". The new peer keeps its own copy of them until the next cluster-wide change of that kind; reissue "
          + "the change, or re-run addPeer, before treating the peer as consistent");
      LogManager.instance().log(this, Level.WARNING,
          "Peer '%s' was added but these security documents could not be seeded to it: %s", peerId,
          String.join(", ", failedSeeds));
    }

    return new ExecutionResponse(200, response.toString());
  }
}
