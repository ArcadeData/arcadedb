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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
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
    // Retried within a bounded budget rather than attempted once (issue #7521): the submit waits for a Raft
    // commit, so its usual failure is an absent quorum at this instant - transient, and the same condition
    // that makes an addPeer interesting in the first place.
    final long retryBudgetMs = httpServer.getServer().getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT);
    final List<String> failedSeeds = httpServer.getServer().getSecurity()
        .seedSecurityStateClusterWide(retryBudgetMs);

    if (!failedSeeds.isEmpty())
      LogManager.instance().log(this, Level.SEVERE,
          "Peer '%s' was added but these security documents could not be seeded to it: %s. It is a cluster member "
              + "serving requests against its own copy of them", peerId, String.join(", ", failedSeeds));

    return addPeerResponse(peerId, failedSeeds);
  }

  /**
   * The response for an admission whose membership change succeeded, given the security documents that could
   * not be seeded to the new peer.
   * <p>
   * A residual failure is <b>not</b> a 200 (issue #7521). The peer addition is not rolled back - the peer is
   * already a committed member and removing it again is a second failure mode rather than a repair - but the
   * operator is the one who has to reissue the seed, and an operator's automation reads the status code, not a
   * {@code warning} field inside a success body. 503 is the status that says "this ran, part of it did not
   * land, retry it": re-POSTing the same peer is idempotent on the membership change and reissues the seed.
   * <p>
   * {@code result} is still present and still says the peer was added, because that half did happen and a
   * caller that treats the whole call as a no-op would be wrong about the cluster's membership.
   *
   * @param failedSeeds the documents that did not commit, in the order {@code seedSecurityStateClusterWide}
   *                    reports them; empty for a clean admission
   */
  static ExecutionResponse addPeerResponse(final String peerId, final List<String> failedSeeds) {
    final JSONObject response = new JSONObject().put("result", "Peer " + peerId + " added");
    if (failedSeeds.isEmpty())
      return new ExecutionResponse(200, response.toString());

    // error/detail, not one long error: AbstractServerHttpHandler.error2json uses that split everywhere, and
    // Studio's globalNotifyError renders 'error' as the notification TITLE and 'detail' as its body.
    response.put("error", "Peer " + peerId + " was added, but these security documents could NOT be seeded to it: "
        + String.join(", ", failedSeeds));
    response.put("detail", "The new peer keeps its own copy of them - which for a node re-added after time out of "
        + "the cluster can still hold a user dropped since, a group narrowed since or a token revoked since - "
        + "until the next cluster-wide change of that kind. Re-POST the same peer to reissue the seed (the "
        + "membership change is idempotent), or reissue the change, before treating the peer as consistent. "
        + "Raise arcadedb.ha.securitySeedRetryTimeout if the cluster routinely needs longer to reach a quorum.");
    response.put("failedSeeds", new JSONArray(failedSeeds));
    return new ExecutionResponse(503, response.toString());
  }
}
