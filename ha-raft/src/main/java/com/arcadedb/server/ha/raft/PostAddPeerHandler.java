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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.IOException;
import java.math.BigDecimal;
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

    final RaftPeer peer;
    try {
      peer = peerFromPayload(peerId, address, payload);
    } catch (final IllegalArgumentException e) {
      return new ExecutionResponse(400, new JSONObject().put("error", e.getMessage()).toString());
    }

    raftHAServer.addPeer(peer, name.isEmpty() ? null : name);

    // Seed the newly-joined peer with the current security documents. Snapshot install covers none of them
    // (they live under <server-root>/config/, outside the database directory), so without this explicit
    // seed the new peer would start with whatever its own files hold - a stale user set, a stale group
    // document, a stale token store - until the next mutation of that kind happens cluster-wide. The
    // groups and tokens half is issue #7373; the users half predates it.
    //
    // ASKED FOR rather than run here (issue #7834). This node is not required to be the leader - the check
    // above is checkRootUser and nothing else, and RaftHAServer.addPeer routes only the membership change to
    // the leader - while the leader seeds every membership change of its own accord since issue #7531. Running
    // a second seed here made an admission put up to six entries in the Raft log from two different JVMs, each
    // holding only its own ServerSecurity monitor; that monitor is what keeps a revocation committing mid-seed
    // from being undone by the whole document a seed carries (issue #7373), so a revocation landing between the
    // two could be resurrected by whichever submit was second. One seeder, on the leader, is the fix.
    //
    // The report is unchanged and is the reason this is not simply deleted: issue #7521 made a residual seed
    // failure operator-facing, and addPeerResponse answers 503 with a failedSeeds array. It now describes the
    // leader's seed rather than this node's.
    //
    // The seed is still retried within a bounded budget (issue #7521): the submit waits for a Raft commit, so
    // its usual failure is an absent quorum at this instant - transient, and the same condition that makes an
    // addPeer interesting in the first place.
    final List<String> failedSeeds;
    try {
      // Through the plugin rather than through ServerSecurity: the seed runs on the leader. The orElseGet is
      // the interface's contract for an HA implementation with no leader-side seeder and is unreachable here -
      // this handler IS the Raft plugin's - but stating it keeps the two admission call sites identical.
      failedSeeds = plugin.seedSecurityStateForAdmission(peerId)
          .orElseGet(() -> httpServer.getServer().getSecurity().seedSecurityStateClusterWide(
              httpServer.getServer().getConfiguration()
                  .getValueAsLong(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT)));
    } catch (final IOException | IllegalStateException e) {
      // The peer IS a committed member by now, so this must not be answered as a failed add. What is unknown is
      // the seed, and "unknown" is reported as a failure of all three rather than as none: a 503 naming them
      // tells the operator to reissue, which is the action that repairs it either way.
      //
      // IllegalStateException as well as IOException (CodeRabbit on PR #7854): the first is what
      // seedSecurityNowAndReport raises when the seed could not be run or its outcome could not be read, and on
      // the leader that call is reached directly rather than over HTTP - so it is the LOCAL path's version of
      // exactly the same "the membership change stands, the seed is unknown" case.
      LogManager.instance().log(this, Level.SEVERE,
          "Peer '%s' was added but the leader could not be asked to seed the security documents: %s. It is a "
              + "cluster member serving requests against its own copy of them; re-POST the peer to retry the seed",
          e, peerId, e.getMessage());
      return addPeerResponse(peerId, List.of("users", "groups", "API tokens"));
    }

    if (!failedSeeds.isEmpty())
      LogManager.instance().log(this, Level.SEVERE,
          "Peer '%s' was added but these security documents could not be seeded to it: %s. It is a cluster member "
              + "serving requests against its own copy of them", peerId, String.join(", ", failedSeeds));

    return addPeerResponse(peerId, failedSeeds);
  }

  /**
   * The peer this payload names, carrying every field it declares (issue #7523).
   * <p>
   * Handed on as a whole {@link RaftPeer} rather than as {@code (id, address, priority)}, because that is the
   * shape {@code RaftClusterManager.addPeer(RaftPeer, String)} was given for exactly this reason (issue #7401):
   * a field a peer carries is then impossible to drop on the way down, instead of merely tested for. That is
   * also why this is not the fourth argument of a four-argument overload - the next field would need a fifth.
   * <p>
   * Package-private and static so a test can drive the construction the handler actually performs. Asserting on
   * {@link #readPriority} alone would leave the one step that matters - the priority reaching the peer object -
   * untested, which is the shape of the bug this fixes.
   *
   * @throws IllegalArgumentException from {@link #readPriority}, answered 400 by the caller
   */
  static RaftPeer peerFromPayload(final String peerId, final String address, final JSONObject payload) {
    return RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .setPriority(readPriority(payload))
        .build();
  }

  /**
   * The leader-election priority the payload asks for, {@code 0} when it names none (issue #7523).
   * <p>
   * Before this, a peer admitted through this route always got Ratis's default priority, while the same peer
   * declared in {@code arcadedb.ha.serverList} - or joined with {@code connect cluster}, which parses one such
   * entry - could name any. That is not a cosmetic difference: {@link RaftHAServer#selectStepDownTargets} and
   * Ratis's own election both read the live {@code RaftPeer.getPriority()}, and once ANY peer carries a positive
   * priority the priority-0 ones stop being electable. A witness added at runtime could therefore be elected
   * leader, which is the one thing declaring it a witness was meant to prevent.
   * <p>
   * {@code 0} is the default because it is Ratis's, so an omitted field keeps the behaviour every existing caller
   * already gets. On a cluster where nobody names a priority that leaves every peer equally electable - the
   * witness semantics appear only once some peer is given a positive one, which is the same rule
   * {@code selectStepDownTargets} applies.
   *
   * <b>Read through {@link BigDecimal#intValueExact()}, not {@code JSONObject.getInt}.</b> That method is
   * {@code Number.intValue()} underneath, which SILENTLY narrows: {@code {"priority":0.5}} would arrive as
   * {@code 0} and {@code {"priority":4294967296}} as {@code 0} again - and {@code 0} is not a harmless default
   * here, it is the value that declares a witness as soon as any other peer carries a positive one. An operator
   * who mistypes a priority would have been told the peer was added at the priority they asked for, and got the
   * one value with the opposite meaning. {@code intValueExact} refuses a fractional part and an out-of-range
   * magnitude in the same call, so both become a 400 naming the field.
   *
   * @throws IllegalArgumentException when the field is present but is not a number, is not a whole number, does
   *                                  not fit in an {@code int}, or is negative - Ratis rejects a negative
   *                                  priority, and answering 400 here names the field instead of surfacing it as
   *                                  a failed membership change
   */
  static int readPriority(final JSONObject payload) {
    if (!payload.has("priority") || payload.isNull("priority"))
      return 0;

    if (!(payload.get("priority") instanceof Number number))
      // Naming the value, like the three refusals below: an operator reading a log line needs to see what was
      // sent, not only which field was wrong.
      throw new IllegalArgumentException("Field 'priority' must be a non-negative integer, the peer's Raft "
          + "leader-election priority, but was " + payload.get("priority"));

    final int priority;
    try {
      // toString() rather than a doubleValue(): it is the one conversion that is lossless for every Number the
      // JSON parser produces - Integer, Long, Double and BigDecimal alike - so nothing is rounded on the way
      // into the check that exists to catch rounding.
      priority = new BigDecimal(number.toString()).intValueExact();
    } catch (final ArithmeticException | NumberFormatException e) {
      throw new IllegalArgumentException("Field 'priority' must be a whole number that fits in a 32-bit integer, "
          + "but was " + number + ". It is the peer's Raft leader-election priority, so a value rounded to fit "
          + "would silently change which nodes can take leadership");
    }

    if (priority < 0)
      throw new IllegalArgumentException("Field 'priority' must be a non-negative integer, but was " + priority
          + ". Use 0 for a witness that must never become leader, and a higher value for a preferred one");

    return priority;
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
