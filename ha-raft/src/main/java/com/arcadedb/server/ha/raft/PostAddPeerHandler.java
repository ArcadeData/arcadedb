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
    //
    // With the configured retry budget (arcadedb.ha.securitySeedRetries), because the entries go through Raft
    // and the usual failure is a momentary loss of quorum - the same condition that makes adding a peer
    // interesting. Only the documents that failed are retried, and each retry re-reads under the monitor.
    final List<String> failedSeeds = httpServer.getServer().getSecurity().seedSecurityStateClusterWideWithRetry();

    return seedOutcomeResponse(peerId, failedSeeds);
  }

  /**
   * The response for a peer that was admitted and a seed that may or may not have landed (issue #7521).
   * <p>
   * A failed seed still does not roll back the peer addition: the peer is already a cluster member, and removing
   * it again is a second failure mode rather than a repair. What it does change is the <b>status</b>. Reporting a
   * partial failure as HTTP 200 with a {@code warning} field meant operator automation read a success and moved
   * on, while the peer kept authenticating and authorizing from its own copies of the three documents - for a
   * node re-added after having been out of the cluster, a snapshot of the security state from whenever it left,
   * so a user dropped since, a group narrowed since or a token revoked since is still in force there.
   * <p>
   * 503 rather than 500: the seed is submitted as a Raft entry and its usual failure is a momentary loss of
   * quorum, so the condition is transient and re-issuing the very same request is the fix. That request is
   * idempotent on the membership change - {@code RaftClusterManager.addPeer} treats an already-member peer as
   * success - and reissues the seed.
   * <p>
   * Static and package-private so the status/body contract is unit-testable without an
   * {@code HttpServerExchange}.
   */
  static ExecutionResponse seedOutcomeResponse(final String peerId, final List<String> failedSeeds) {
    final JSONObject response = new JSONObject().put("result", "Peer " + peerId + " added");
    if (failedSeeds.isEmpty())
      return new ExecutionResponse(200, response.toString());

    // 'error' short and 'detail' long, which is the shape AbstractServerHttpHandler.sendErrorResponse produces
    // and which Studio's globalNotifyError renders as a notification title plus body.
    final String documents = String.join(", ", failedSeeds);
    response.put("error", "Peer " + peerId + " added, but these security documents could NOT be seeded to it: "
        + documents);
    response.put("detail", "Until they are, peer " + peerId + " authenticates and authorizes from its own copies of "
        + "them - a user deleted, a group narrowed or an API token revoked since it last held them is still in "
        + "force there. Re-run this request: adding a peer that is already a member is a no-op and the seed is "
        + "reissued. Remove the peer if it cannot be seeded");
    LogManager.instance().log(PostAddPeerHandler.class, Level.SEVERE,
        "Peer '%s' was added but these security documents could not be seeded to it: %s. It serves requests with "
            + "its own copies of them until the seed is reissued", peerId, documents);

    return new ExecutionResponse(503, response.toString());
  }
}
