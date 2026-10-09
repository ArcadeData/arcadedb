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
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * POST /api/v1/cluster/accept-stale-snapshot - the operator's override of issue #9498. Lifts the node-wide stale-snapshot
 * read floor (issue #6111) on a node that is the sole voter of its cluster, accepting its databases as they are.
 * <p>
 * The floor is published when the Ratis snapshot marker runs ahead of the entries this node applied, keeps
 * {@code /api/v1/ready} at 503 and clamps every LINEARIZABLE read, and is lifted only by a full resync from a peer. A
 * leader refuses to resync from itself, and a sole voter is always the leader, so there the floor never lifts. Lifting it
 * by hand gives up the entries between the floor and the marker, which the Raft log no longer holds, so it is an
 * operator decision, logged at WARNING with the user, the caller's address, the floor and the marker index. The same
 * override is reachable over gRPC through {@link HAServerPlugin#acceptStaleSnapshot}.
 * <p>
 * Root only. 404 when no floor stands, 409 when a peer could still serve a resync or a download is running, 500 when the
 * change could not be persisted (nothing is lifted then).
 */
public class PostAcceptStaleSnapshotHandler extends AbstractServerHttpHandler {

  /** The route; it names no database, because the floor is node-wide. */
  static final String ROUTE = "/api/v1/cluster/accept-stale-snapshot";

  private final RaftHAPlugin plugin;

  public PostAcceptStaleSnapshotHandler(final HttpServer httpServer, final RaftHAPlugin plugin) {
    super(httpServer);
    this.plugin = plugin;
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    // Rewrites the applied-index file (an atomic rename) under the lock the apply path takes.
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) {
    checkRootUser(user);

    try {
      return new ExecutionResponse(200, plugin.acceptStaleSnapshot(describe(user, exchange)).toString());
    } catch (final ServerControlPlane.NotFoundException e) {
      return error(404, e.getMessage());
    } catch (final ServerControlPlane.OperationNotAvailableException e) {
      return error(409, e.getMessage());
    } catch (final IOException e) {
      return error(500, "Could not lift the stale-snapshot read floor: " + e.getMessage() + ". The floor stays");
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
