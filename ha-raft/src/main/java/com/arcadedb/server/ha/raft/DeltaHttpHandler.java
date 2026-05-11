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
import com.arcadedb.server.http.HttpServer;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.HeaderValues;

import java.security.MessageDigest;
import java.util.Deque;
import java.util.logging.Level;

/**
 * {@code GET /api/v1/ha/delta/{database}?fromTxId=N} — peer-to-peer transaction-delta resync endpoint.
 * <p>
 * Issue #4147 phase 6. The bootstrap mismatch path (and the runtime catch-up path) calls this on
 * the source peer to pull only the transactions for {@code (fromTxId+1 .. currentLastTxId)}
 * instead of a full database snapshot. The follower's installer applies the records via the
 * existing {@code TransactionManager.applyChanges} path.
 * <p>
 * <b>Status (phase 6a, scaffolding).</b> Ratis-log delta serving is not yet implemented. This
 * handler always returns {@code 412 Precondition Failed} with a clear body explaining the gap, so
 * the caller falls through to the existing leader-shipped full-snapshot path. The wire format is
 * forward-compatible: when Ratis-log delta serving lands, this handler starts returning
 * {@code 200} with the streamed transactions and the caller benefits without any client-side
 * change.
 * <p>
 * Error semantics (locked in now so the caller's fallback logic is correct):
 * <ul>
 *   <li>{@code 412 Precondition Failed} — the Ratis log no longer covers {@code fromTxId}
 *       (gap too big), or delta serving has not landed on this peer.
 *       Body is plain text, single line, prefixed {@code "no-delta:"} for machine inspection.</li>
 *   <li>{@code 204 No Content} — caller is already at or past the source's lastTxId; nothing to
 *       ship.</li>
 *   <li>{@code 401 Unauthorized} — missing or invalid {@code X-ArcadeDB-Cluster-Token}.</li>
 *   <li>{@code 404 Not Found} — HA not enabled, or the database doesn't exist on this peer.</li>
 *   <li>{@code 200 OK} — body is a stream of replicated transactions (only when Ratis-log delta serving ships).</li>
 * </ul>
 * Authentication is the same cluster-token check as {@link SnapshotHttpHandler}; we deliberately
 * do not accept end-user Basic auth on this endpoint because the response format is intended for
 * peer-to-peer consumption.
 */
public class DeltaHttpHandler implements HttpHandler {

  private final HttpServer httpServer;

  public DeltaHttpHandler(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  @Override
  public void handleRequest(final HttpServerExchange exchange) throws Exception {
    if (!authenticate(exchange)) {
      exchange.setStatusCode(401);
      exchange.getResponseSender().send("Missing or invalid X-ArcadeDB-Cluster-Token");
      return;
    }

    final String path = exchange.getRequestPath();
    final String prefix = "/api/v1/ha/delta/";
    if (!path.startsWith(prefix)) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Not found");
      return;
    }
    final String dbName = path.substring(prefix.length());
    if (dbName.isEmpty()) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Database name required");
      return;
    }

    if (!httpServer.getServer().existsDatabase(dbName)) {
      exchange.setStatusCode(404);
      exchange.getResponseSender().send("Database not found");
      return;
    }

    // Parse fromTxId. The caller's last-applied; we'd ship (fromTxId+1 .. currentLastTxId).
    final long fromTxId;
    final Deque<String> fromTxIdParam = exchange.getQueryParameters().get("fromTxId");
    if (fromTxIdParam == null || fromTxIdParam.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("fromTxId query parameter required");
      return;
    }
    try {
      fromTxId = Long.parseLong(fromTxIdParam.getFirst());
    } catch (final NumberFormatException e) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("fromTxId must be a long");
      return;
    }

    // Phase 6a scaffolding: Ratis-log delta serving is not yet implemented. We always answer 412
    // so the caller falls back to the full-snapshot path. The body is intentionally machine-
    // readable ("no-delta:" prefix) so the caller can log a clear reason without parsing English.
    exchange.setStatusCode(412);
    exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
    exchange.getResponseSender().send(
        "no-delta: Ratis-log delta serving not yet implemented (issue #4147 phase 6a scaffolding); fromTxId=" + fromTxId);
    LogManager.instance().log(this, Level.FINE,
        "Delta endpoint returned 412 (Ratis-log delta serving not implemented) for '%s' fromTxId=%d", dbName, fromTxId);
  }

  /**
   * Cluster-token only authentication. Same scheme as {@link SnapshotHttpHandler}: we are
   * peer-to-peer here, end-user credentials should not pass this boundary.
   */
  private boolean authenticate(final HttpServerExchange exchange) {
    final HeaderValues clusterTokenHeader = exchange.getRequestHeaders().get("X-ArcadeDB-Cluster-Token");
    if (clusterTokenHeader == null || clusterTokenHeader.isEmpty())
      return false;

    final var server = httpServer.getServer();
    final RaftHAPlugin haPlugin = server.getHA() instanceof RaftHAPlugin rp ? rp : null;
    final RaftHAServer raftHAServer = haPlugin != null ? haPlugin.getRaftHAServer() : null;
    final String expectedToken = raftHAServer != null ? raftHAServer.getClusterToken() : null;
    if (expectedToken == null || expectedToken.isEmpty())
      return false;
    return MessageDigest.isEqual(expectedToken.getBytes(), clusterTokenHeader.getFirst().getBytes());
  }

}
