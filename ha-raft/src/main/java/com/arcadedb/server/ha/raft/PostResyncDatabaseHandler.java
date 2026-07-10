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

/**
 * POST /api/v1/cluster/resync/{database} - emergency recovery that forces THIS node to drop its
 * local copy of {@code database} and re-acquire a fresh full snapshot from the current leader.
 * <p>
 * Intended for a follower that has diverged from the leader (e.g. a {@code WALVersionGapException}
 * reporting "snapshot resync required"). The endpoint always operates on the node that receives the
 * request, so an operator points Studio (or curl) at the diverged follower and triggers it there.
 * Refuses to run on the leader, which holds the authoritative copy.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostResyncDatabaseHandler extends AbstractServerHttpHandler {

  private final RaftHAPlugin plugin;

  public PostResyncDatabaseHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    // Closing the database and downloading the snapshot from the leader is blocking I/O.
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    // Accepts an operator (root via Basic auth) or an inter-node call from the leader, which presents
    // the cluster token plus X-ArcadeDB-Forwarded-User: root (resolved to the root user by
    // AbstractServerHttpHandler before execute() runs). The leader uses this to force a persistently
    // STALLED replica to resync (issue #4728).
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    // Extract database name from path: /api/v1/cluster/resync/{database}
    final String path = exchange.getRelativePath();
    final String databaseName = (path.startsWith("/") ? path.substring(1) : path).trim();

    if (databaseName.isEmpty())
      return new ExecutionResponse(400, new JSONObject().put("error", "Database name is required in path").toString());

    if (!PostVerifyDatabaseHandler.VALID_DATABASE_NAME.matcher(databaseName).matches())
      return new ExecutionResponse(400, new JSONObject().put("error", "Invalid database name").toString());

    if (raftHAServer.isLeader())
      return new ExecutionResponse(400, new JSONObject().put("error",
          "Cannot resync database '" + databaseName
              + "' on the leader: the leader holds the authoritative copy. Run the resync on the diverged follower.").toString());

    if (raftHAServer.getLeaderHttpAddress() == null)
      return new ExecutionResponse(503, new JSONObject().put("error",
          "Cannot resync database '" + databaseName
              + "': the leader is currently unknown (election in progress?). Retry once a leader is elected.").toString());

    try {
      raftHAServer.getStateMachine().resyncDatabaseFromLeader(databaseName);
    } catch (final Exception e) {
      return new ExecutionResponse(500, new JSONObject().put("error",
          "Resync of database '" + databaseName + "' failed: " + e.getMessage()).toString());
    }

    return new ExecutionResponse(200, new JSONObject()
        .put("result", "Database '" + databaseName + "' resynced from leader")
        .put("database", databaseName)
        .put("localServer", httpServer.getServer().getServerName()).toString());
  }
}
