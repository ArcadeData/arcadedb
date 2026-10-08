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
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * POST /api/v1/cluster/accept-diverged/{database} - the operator's override of issue #9449. Lifts the quarantine standing
 * on {@code database} (and the read floor that goes with it) on a node that is the sole voter of its cluster, accepting
 * its copy as it is.
 * <p>
 * Since #9308 a sole voter no longer RAISES a quarantine, but one restored from disk (#7735) or raised while the node
 * still had peers keeps it not-ready and its Raft log un-checkpointed for good: nothing lifts a quarantine but a resync
 * from a peer or a DROP of the database, and a sole voter has no peer. Lifting it discards the guarantee that the entry
 * the quarantine skipped stays replayable, so it is an operator decision, logged at WARNING with the user, the caller's
 * address, the applied index and the cause. The same override is reachable over gRPC through
 * {@link com.arcadedb.server.HAServerPlugin#acceptDivergedDatabase}.
 * <p>
 * Root only. 404 when nothing stands on the database, 409 when this node is not the sole voter (the resync is the way
 * out there), 500 when the change could not be persisted (nothing is lifted then).
 */
public class PostAcceptDivergedHandler extends AbstractServerHttpHandler {

  /** The route prefix; the database name follows it. */
  static final String ROUTE = "/api/v1/cluster/accept-diverged/";

  private final RaftHAPlugin plugin;

  public PostAcceptDivergedHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    // Rewrites the applied-index file (fsync-free, but still a write and an atomic rename) under the lock the apply path
    // takes.
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    final String path = exchange != null ? exchange.getRelativePath() : "";
    final String databaseName = (path.startsWith("/") ? path.substring(1) : path).trim();
    if (databaseName.isEmpty())
      return error(400, "Database name is required in path");

    try {
      return new ExecutionResponse(200, plugin.acceptDivergedDatabase(databaseName, describe(user, exchange)).toString());
    } catch (final IllegalArgumentException e) {
      return error(400, e.getMessage());
    } catch (final ServerControlPlane.NotFoundException e) {
      return error(404, e.getMessage());
    } catch (final ServerControlPlane.OperationNotAvailableException e) {
      return error(409, e.getMessage());
    } catch (final IOException e) {
      return error(500, "Could not lift the quarantine of database '" + databaseName + "': " + e.getMessage()
          + ". The quarantine stays");
    }
  }

  private static ExecutionResponse error(final int code, final String message) {
    return new ExecutionResponse(code, new JSONObject().put("error", message).toString());
  }

  private static String describe(final ServerSecurityUser user, final HttpServerExchange exchange) {
    final String name = user != null ? "user '" + user.getName() + "'" : "an unknown user";
    final InetSocketAddress source = exchange != null ? exchange.getSourceAddress() : null;
    return source != null ? name + " (from " + source.getHostString() + ")" : name;
  }
}
