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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * POST /api/v1/cluster/accept-copy/{database} - the operator's override of issue #8641. Accepts the leader's closed copy
 * of {@code database}, marked {@link ArcadeDBServer#UNVERIFIED_CLOSED_COPY_FILE} because the last resync could not
 * verify it, as the cluster's copy without the other servers' confirmation (see {@link UnverifiedClosedCopyCheck}).
 * <p>
 * For a refusal no peer can lift: a peer permanently gone but still in the configuration never answers, and a copy
 * with no recorded applied index can never be ordered. Before this route the only ways out were removing the peer from
 * the configuration or deleting the marker file by hand, which left no trace of who accepted what. Here the acceptance
 * is logged at WARNING with the user, the caller's address, the copy's applied index and the refusal it overrode.
 * <p>
 * Root only, and only on the leader: on a follower the marker keeps a copy the leader does not hold from being
 * reopened (issue #8589), and the follower's copy is replaced by the leader's anyway. The copy is not reopened here; the
 * next request that names the database does, as it would for any closed database.
 */
public class PostAcceptCopyHandler extends AbstractServerHttpHandler {

  /** The route prefix; the database name follows it. */
  static final String ROUTE = "/api/v1/cluster/accept-copy/";

  private final RaftHAPlugin plugin;

  public PostAcceptCopyHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    // Waits for a round in flight on the same database, up to UnverifiedClosedCopyCheck.ROUND_TIMEOUT_MS, and deletes
    // a file.
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final RaftHAServer raftHAServer = plugin.getRaftHAServer();
    if (raftHAServer == null)
      return new ExecutionResponse(400, new JSONObject().put("error", "Raft HA is not enabled").toString());

    final String path = exchange != null ? exchange.getRelativePath() : "";
    final String databaseName = (path.startsWith("/") ? path.substring(1) : path).trim();
    if (databaseName.isEmpty())
      return new ExecutionResponse(400, new JSONObject().put("error", "Database name is required in path").toString());
    if (!PostVerifyDatabaseHandler.VALID_DATABASE_NAME.matcher(databaseName).matches())
      return new ExecutionResponse(400, new JSONObject().put("error", "Invalid database name").toString());

    return accept(raftHAServer, databaseName, describe(user, exchange));
  }

  /** The acceptance itself, past authentication and the parsing of the path. Package-private for tests. */
  ExecutionResponse accept(final RaftHAServer raftHAServer, final String databaseName, final String acceptedBy) {
    if (!raftHAServer.isLeader())
      return notLeader(databaseName);

    final UnverifiedClosedCopyCheck.Acceptance acceptance;
    try {
      acceptance = raftHAServer.getUnverifiedClosedCopyCheck().accept(databaseName, acceptedBy);
    } catch (final IllegalStateException e) {
      // The leadership moved while a round on the same database held the lock.
      return notLeader(databaseName);
    } catch (final IOException e) {
      return new ExecutionResponse(500, new JSONObject().put("error",
          "Could not remove the '" + ArcadeDBServer.UNVERIFIED_CLOSED_COPY_FILE + "' marker of database '" + databaseName
              + "': " + e.getMessage() + ". The copy stays closed").toString());
    }

    if (acceptance == null)
      return new ExecutionResponse(404, new JSONObject().put("error",
          "This server holds no copy of database '" + databaseName + "' waiting to be accepted: none is closed and "
              + "marked unverified here").toString());

    final JSONObject response = new JSONObject()
        .put("result", "Database '" + databaseName + "': this leader's copy is accepted as the cluster's copy. The next "
            + "request that names the database reopens it")
        .put("database", databaseName)
        .put("localServer", httpServer.getServer().getServerName())
        .put("appliedIndex", acceptance.appliedIndex());
    if (acceptance.standingRefusal() != null)
      response.put("overriddenRefusal", acceptance.standingRefusal());
    return new ExecutionResponse(200, response.toString());
  }

  private static ExecutionResponse notLeader(final String databaseName) {
    return new ExecutionResponse(400, new JSONObject().put("error",
        "Cannot accept the copy of database '" + databaseName + "' on this server: it is not the leader. Only the "
            + "leader's copy becomes the cluster's copy; run it on the leader").toString());
  }

  private static String describe(final ServerSecurityUser user, final HttpServerExchange exchange) {
    final String name = user != null ? "user '" + user.getName() + "'" : "an unknown user";
    final InetSocketAddress source = exchange != null ? exchange.getSourceAddress() : null;
    return source != null ? name + " (from " + source.getHostString() + ")" : name;
  }
}
