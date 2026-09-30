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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * Handles membership operations for the Raft cluster: adding and removing peers,
 * transferring leadership, and graceful cluster leave.
 * <p>
 * Delegates to {@link RaftHAServer} for shared state (live peers, leader status,
 * HTTP address map). All Raft configuration changes go
 * through {@link #setConfigurationWithRetry} which retries bounded times to
 * survive the window where a newly elected leader has not yet committed from
 * its current term.
 */
class RaftClusterManager {

  /**
   * How long {@link #setConfigurationWithRetry} keeps re-issuing a membership change before giving up, when the
   * operator has not set {@link GlobalConfiguration#HA_MEMBERSHIP_CHANGE_TIMEOUT}.
   * <p>
   * Since issue #7561 this IS the whole cost of a failed change, which it was not before. Two things made the
   * observed cost ~150 s against a 90 s budget:
   * <ul>
   *   <li>the shared {@link RaftClient} carries {@code RetryLimited(maxAttempts=60, sleepTime=1s)}, so a single
   *       {@code setConfiguration} call blocked for about a minute before throwing;</li>
   *   <li>the deadline was consulted only BETWEEN attempts, so an attempt that ended at t&#8776;89 s started
   *       another one that ran to t&#8776;150 s.</li>
   * </ul>
   * Both are closed: the membership change now runs on its own short-retry client
   * ({@link RaftHAServer#newMembershipClient()}), which caps one call at THREE attempts rather than sixty, and
   * {@link #setConfigurationWithRetry} refuses to START an attempt that the longest attempt so far says cannot
   * finish inside what is left. The probe in {@code RaftHAServer.ensurePeerReachable} (issue #7514) still keeps
   * the common mistake - naming a server that is not running - from reaching any of this at all.
   * <p>
   * <b>What one attempt can cost, in the units an operator has to budget in.</b> The common failure here is a
   * synchronous rejection, which comes back in milliseconds; the worst case is an RPC that hangs at the
   * transport, and there the ceiling is three attempts of {@link RaftHAServer#CLIENT_REQUEST_TIMEOUT_MS} plus
   * the two sleeps between them - about 31 seconds, not "a couple". So a budget set below that buys one attempt
   * and the report, which is the right trade for a load-balancer idle timeout but is worth knowing rather than
   * discovering.
   */
  static final long DEFAULT_SET_CONFIGURATION_BUDGET_MS = 90_000;

  /** How far {@link #isPermanent} follows a failure's cause chain. Deeper than any Ratis failure nests. */
  private static final int MAX_CAUSE_DEPTH = 16;

  private final RaftHAServer raftHAServer;
  private final long         setConfigurationBudgetMs;

  RaftClusterManager(final RaftHAServer raftHAServer) {
    this(raftHAServer, DEFAULT_SET_CONFIGURATION_BUDGET_MS);
  }

  /**
   * Package-private for tests that have to reach the give-up branch of {@link #setConfigurationWithRetry}
   * without waiting out the real budget.
   */
  RaftClusterManager(final RaftHAServer raftHAServer, final long setConfigurationBudgetMs) {
    this.raftHAServer = raftHAServer;
    this.setConfigurationBudgetMs = setConfigurationBudgetMs;
  }

  /**
   * <b>Test-only seam.</b> No production code reaches these two overloads any more: issue #7514 moved the
   * {@link RaftPeer} construction up into {@code RaftHAServer.addPeer(String, String, String)} so that both
   * of its overloads meet at {@code addPeer(RaftPeer, String)}, which is where the pre-flight reachability
   * probe lives. Every live entry point - {@code POST /api/v1/cluster/peer}, {@code connect cluster}, the
   * gRPC {@code ConnectCluster} RPC, the embedded {@code HAServerPlugin.addPeer} - therefore goes through
   * the probe, and anything calling these instead would bypass it.
   * <p>
   * They are kept because two tests exercise the membership change through them without standing up a
   * server ({@code RaftAtomicMembershipTest}, {@code Issue7514UnreachablePeerRefusalTest}). A new
   * production caller belongs on {@code RaftHAServer.addPeer}, not here.
   */
  void addPeer(final String peerId, final String address) {
    addPeer(peerId, address, null);
  }

  /** See {@link #addPeer(String, String)}: a test-only seam that bypasses the reachability probe. */
  void addPeer(final String peerId, final String address, final String name) {
    addPeer(RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build(), name);
  }

  /**
   * Adds {@code newPeer} as it was built, instead of rebuilding one from an id and an address.
   * <p>
   * The difference is a field that would otherwise be lost. A {@link RaftPeer} also carries its
   * leader-election {@code priority}, and {@code connect cluster} (issue #7401) is the first caller
   * that can name one: it parses a whole {@code arcadedb.ha.serverList} entry, and both the object
   * form and the four-field positional form declare a priority. Dropping it is not cosmetic -
   * {@link RaftHAServer#selectStepDownTargets} reads the live {@code getPriority()} of each peer and,
   * once any peer has a positive priority, skips the priority-0 ones as non-electable witnesses - so a
   * priority silently reset to the default changes which nodes can take leadership.
   * <p>
   * Taking the peer whole rather than adding a fourth argument is deliberate: it makes losing the
   * next field impossible instead of merely tested for. The three-argument overload above stays for
   * {@code POST /api/v1/cluster/peer}, whose payload has no priority to pass.
   */
  void addPeer(final RaftPeer newPeer, final String name) {
    addPeer(newPeer, name, null);
  }

  /**
   * {@link #addPeer(RaftPeer, String)} for a peer whose HTTP address was DECLARED by the caller - the
   * {@code host:raftPort:httpPort} or {@code http:} field of a {@code connect cluster} target (issue #8330).
   * <p>
   * A declared address is written <b>before</b> the membership change is submitted, not after it returns. The
   * configuration entry that change commits is what schedules the leader's security seed
   * ({@link MembershipSecuritySeeder}), and the seed's group and API-token entries pass the #7511 capability gate
   * only once the new peer has answered a capability probe sent to the address this map holds for it. Written
   * after the commit, the seed raced it; and the address written in between was the derived
   * {@code raftPort + offset} guess, which on a cluster whose ports are not in step names a socket nobody listens
   * on, so every probe of the seed's retry budget failed and the admission answered 503 for a peer that was up.
   * <p>
   * If the change does not commit, the previous entry is put back (or none left), so a failed join does not leave
   * an address behind for a peer that never became a member. Without a declared address the derived one is
   * written after the commit exactly as before.
   *
   * @param declaredHttpAddress the {@code host:port} the caller declared for the peer's HTTP listener, or
   *                            {@code null} to derive one
   */
  void addPeer(final RaftPeer newPeer, final String name, final String declaredHttpAddress) {
    final String peerId = newPeer.getId().toString();
    final String address = newPeer.getAddress();
    final Map<RaftPeerId, String> httpAddresses = raftHAServer.getHttpAddresses();

    final String previousHttpAddress = declaredHttpAddress != null
        ? httpAddresses.put(newPeer.getId(), declaredHttpAddress)
        : null;

    // Mode.ADD atomically appends this single peer to the CURRENT committed configuration, so two
    // near-simultaneous adds cannot clobber each other. A full setConfiguration(getLivePeers()+peer)
    // is read-modify-write last-write-wins and silently drops one of two concurrent adds (issue #4795),
    // which is exactly why the K8s auto-join path already uses Mode.ADD (see KubernetesAutoJoin).
    try {
      setConfigurationWithRetry(() -> buildAddArgs(peerId, newPeer), "add peer " + peerId + " at " + address,
          "The peer answered a connection but the Raft membership change did not commit: Ratis holds a Mode.ADD"
              + " uncommitted until the new peer has caught up with the leader's log. Check that the server at "
              + address + " is running as part of this cluster - same cluster name and cluster token - and is"
              + " not still replaying its own log.");
    } catch (final RuntimeException | Error e) {
      if (declaredHttpAddress != null) {
        if (previousHttpAddress != null)
          httpAddresses.put(newPeer.getId(), previousHttpAddress);
        else
          httpAddresses.remove(newPeer.getId(), declaredHttpAddress);
      }
      throw e;
    }

    final int colonIdx = address.lastIndexOf(':');
    if (declaredHttpAddress == null && colonIdx > 0) {
      final String host = address.substring(0, colonIdx);
      try {
        final int raftPort = Integer.parseInt(address.substring(colonIdx + 1));
        final int httpPortOffset = getHttpPortOffset();
        raftHAServer.getHttpAddresses().put(newPeer.getId(), host + ":" + (raftPort + httpPortOffset));
      } catch (final NumberFormatException ignored) {
      }
    }

    if (name != null && !name.isEmpty())
      raftHAServer.registerPeerDisplayName(newPeer.getId(), name);

    LogManager.instance().log(this, Level.INFO, "Peer %s added to Raft cluster at %s", peerId, address);
  }

  void removePeer(final String peerId) {
    removePeer(peerId, false);
  }

  void removePeer(final String peerId, final boolean force) {
    // Validate up-front so an unknown peer or a quorum breach fails immediately rather than after the
    // 90s retry budget. The retry loop re-validates on each attempt (see buildRemoveArgs).
    final Collection<RaftPeer> currentPeers = raftHAServer.getLivePeers();
    boolean found = false;
    for (final RaftPeer peer : currentPeers)
      if (peer.getId().toString().equals(peerId)) {
        found = true;
        break;
      }
    if (!found)
      throw new ConfigurationException("Peer " + peerId + " not found in cluster");

    ensureQuorumPreserved(peerId, currentPeers.size(), currentPeers.size() - 1, force);

    // Ratis 3.2.2 has no atomic REMOVE delta, so use COMPARE_AND_SET: the change commits only if the
    // leader's current configuration still matches the snapshot we computed the new list from. A
    // concurrent membership change invalidates the CAS, and the retry loop re-snapshots and rebuilds,
    // instead of the read-modify-write last-write-wins of a plain setConfiguration (issue #4795).
    setConfigurationWithRetry(() -> buildRemoveArgs(peerId, force), "remove peer " + peerId,
        "The Raft membership change did not commit. A removal needs a leader and a voting majority of the"
            + " CURRENT configuration, so check that the cluster still has one.");

    raftHAServer.getHttpAddresses().remove(RaftPeerId.valueOf(peerId));
    LogManager.instance().log(this, Level.INFO, "Peer %s removed from Raft cluster", peerId);
  }

  /**
   * Builds the {@link SetConfigurationRequest.Mode#ADD} arguments for adding {@code newPeer}. Returns
   * {@code null} when the peer is already a member, which the retry loop treats as success - this keeps
   * a retry after a lost success reply idempotent instead of spinning until the deadline.
   */
  private SetConfigurationRequest.Arguments buildAddArgs(final String peerId, final RaftPeer newPeer) {
    for (final RaftPeer peer : raftHAServer.getLivePeers())
      if (peer.getId().toString().equals(peerId))
        return null; // already a member

    return SetConfigurationRequest.Arguments.newBuilder()
        .setServersInNewConf(List.of(newPeer))
        .setMode(SetConfigurationRequest.Mode.ADD)
        .build();
  }

  /**
   * Builds the {@link SetConfigurationRequest.Mode#COMPARE_AND_SET} arguments for removing
   * {@code peerId}, snapshotting the current configuration as the CAS precondition. Returns
   * {@code null} when the peer is already absent (a concurrent removal won the race), which the
   * retry loop treats as success - the removal goal is already met.
   */
  private SetConfigurationRequest.Arguments buildRemoveArgs(final String peerId, final boolean force) {
    final List<RaftPeer> currentPeers = new ArrayList<>(raftHAServer.getLivePeers());
    final List<RaftPeer> newPeers = new ArrayList<>(currentPeers.size());
    for (final RaftPeer peer : currentPeers)
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);

    if (newPeers.size() == currentPeers.size())
      return null; // peer already gone

    ensureQuorumPreserved(peerId, currentPeers.size(), newPeers.size(), force);

    return SetConfigurationRequest.Arguments.newBuilder()
        .setServersInCurrentConf(currentPeers)
        .setServersInNewConf(newPeers)
        .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
        .build();
  }

  /**
   * Quorum size (voting majority) for a cluster of {@code total} members: {@code floor(total/2)+1}.
   */
  static int quorumOf(final int total) {
    return total / 2 + 1;
  }

  /**
   * Refuses a membership removal that would leave the cluster without a voting majority, unless
   * {@code force} is set. The resulting configuration must retain at least {@code quorumOf(total)}
   * voters of the current {@code total}; dropping below that loses fault tolerance or, worse, leaves
   * the cluster unable to commit the very configuration change (it needs the old majority), so the
   * node's belief and the committed config diverge or the cluster wedges with no leader (issue #4796).
   *
   * @throws ConfigurationException if the removal would breach quorum and {@code force} is false
   */
  static void ensureQuorumPreserved(final String peerId, final int total, final int remaining, final boolean force) {
    if (force)
      return;
    final int quorum = quorumOf(total);
    if (remaining < quorum)
      throw new ConfigurationException(String.format(
          "Refusing to remove peer %s: the cluster would drop to %d voter(s), below the quorum of %d required by the current %d-node configuration. Retry with force=true to override.",
          peerId, remaining, quorum, total));
  }

  /**
   * Hands leadership to another peer of the cluster's choosing. The peers {@link RaftHAServer#selectStepDownTargets}
   * ranks, among those {@link RaftHAServer#handoffReachablePeers()} proves reachable, are tried in order with the
   * targeted transfer, all within one {@code timeoutMs} budget and each with at most a slice of it
   * ({@link #candidateTransferBudgetMs}); only when none is eligible, or all of them fail, does it fall back to
   * {@link #stepDownWithoutTarget(long)}.
   * <p>
   * Except when a candidate is refused because another transfer is already pending on this leader (issue #8557): every
   * other candidate would be refused the same way, and the bare step-down would pull the leadership out from under
   * that pending transfer. The method then waits, within what is left of the budget, for that transfer to land.
   *
   * @return true only when leadership settled on a peer other than this one; false when this node is not the leader,
   *         leadership moved away on its own while the transfer was being attempted, or no handoff happened in time
   */
  boolean transferLeadership(final long timeoutMs) {
    return transferLeadership(timeoutMs, true);
  }

  /**
   * {@link #transferLeadership(long)} with the last resort made optional. With {@code bareStepDownFallback} false, a
   * leader with no eligible peer, or whose every targeted transfer failed, returns false instead of stepping down
   * with no target: the caller has a better way to give the leadership up, the way {@link #leaveCluster(boolean)}
   * does by removing this node from the configuration (issue #8592).
   */
  boolean transferLeadership(final long timeoutMs, final boolean bareStepDownFallback) {
    final RaftClient client = raftHAServer.getClient();
    if (client == null)
      return false;

    final RaftPeerId selfId = raftHAServer.getLocalPeerId();

    // Only the leader can hand off its own leadership. If this node is not the leader there is
    // nothing to transfer; returning success here (as the old !isLeader() heuristic did) would be a
    // false positive, and routing the request through Ratis could even trigger an unintended transfer
    // on the real leader (issue #4809).
    if (!raftHAServer.isLeader()) {
      LogManager.instance().log(this, Level.INFO,
          "Leadership transfer requested but this node (%s) is not the leader; nothing to transfer", selfId);
      // Returns false rather than throwing, unlike the targeted overload: this is a published contract (#4809,
      // and Issue4809NoTargetTransferLeadershipIT pins it) that embedded callers read as a boolean. The HTTP
      // endpoint gets its "wrong node" 409 from a guard in PostTransferLeaderHandler instead, so the two
      // branches of that endpoint still answer alike without changing what this method promises Java callers.
      //
      // So the invariant is enforced in two places for this one overload, and a NEW caller that needs to tell
      // "wrong node" apart from "the transfer failed" gets no structural cue from the boolean: it has to make
      // the isLeader() check itself, the way that handler does.
      return false;
    }

    // A real handoff first (issue #8480): the targeted Ratis transfer makes the chosen follower start its election at
    // once, so leadership settles on it within milliseconds and every follower learns the new leader from its first
    // heartbeat. The candidates and their order are the ones stepDown() uses.
    final long deadline = System.currentTimeMillis() + timeoutMs;
    final List<RaftPeer> candidates = RaftHAServer.selectStepDownTargets(raftHAServer.getLivePeers(), selfId,
        raftHAServer.getClusterMonitor(), raftHAServer.handoffReachablePeers());
    for (final RaftPeer candidate : candidates) {
      final long remaining = deadline - System.currentTimeMillis();
      if (remaining <= 0)
        return false;
      try {
        // A slice of the budget, not all of it (issue #8556): while a targeted transfer is pending Ratis refuses every
        // write on this leader, so a candidate that cannot win must not hold the whole budget, nor leave nothing for
        // the next one.
        transferLeadership(candidate.getId().toString(), candidateTransferBudgetMs(timeoutMs, remaining));
        return true;
      } catch (final NotTheLeaderRefusalException e) {
        // This node stopped being the leader between candidates: every remaining one would refuse the same way.
        // The previous candidate's attempt may be what moved it - a transfer that won while its RPC failed with
        // the client closed under it (#8487) - so report whether leadership settled elsewhere, as #4809 does for a
        // transfer whose reply was lost, rather than a flat false.
        LogManager.instance().log(this, Level.INFO,
            "This node (%s) stopped being the leader while transferring leadership to %s", selfId, candidate.getId());
        return confirmLeadershipMovedAway(selfId, confirmWindow(deadline));
      } catch (final LeadershipTransferInProgressException e) {
        // Another caller is handing this leadership over right now (issue #8557). The refusal says nothing about the
        // candidate: the next one would be refused identically, in microseconds, and the loop would then reach the
        // bare step-down with its budget unspent - which Ratis does not hold back for the pending transfer, so it
        // would make this node a follower under it and leave the cluster leaderless for an election timeout. The
        // pending transfer is the hand-off this caller wanted: report whether it lands.
        LogManager.instance().log(this, Level.INFO,
            "Leadership transfer to %s refused because another transfer is already in progress; waiting for it instead",
            candidate.getId());
        return confirmLeadershipMovedAway(selfId, confirmWindow(deadline));
      } catch (final Exception e) {
        // The same race: the candidate may have won although the call reported failure. Trying the next one would
        // then only be refused, or worse, start a second election against the leader just elected.
        if (!raftHAServer.isLeader())
          return confirmLeadershipMovedAway(selfId, confirmWindow(deadline));
        LogManager.instance().log(this, Level.WARNING, "Leadership transfer to %s failed, trying the next candidate: %s",
            candidate.getId(), e.getMessage());
      }
    }

    if (!bareStepDownFallback)
      return false;
    final long remaining = deadline - System.currentTimeMillis();
    if (remaining <= 0)
      return false;
    return stepDownWithoutTarget(remaining);
  }

  /** How many slices of the caller's budget one candidate of {@link #transferLeadership(long)} gets (issue #8556). */
  static final int  CANDIDATE_TRANSFER_BUDGET_SLICES = 4;
  /** The floor of one candidate's slice, so a short budget still leaves a transfer the time a healthy one needs. */
  static final long MIN_CANDIDATE_TRANSFER_BUDGET_MS = 1_000L;

  /**
   * The budget one candidate's targeted transfer gets out of the {@code timeoutMs} of {@link #transferLeadership(long)}
   * (issue #8556): {@link #CANDIDATE_TRANSFER_BUDGET_SLICES a quarter} of it, never less than
   * {@link #MIN_CANDIDATE_TRANSFER_BUDGET_MS}, never more than what is {@code remaining}.
   * <p>
   * Ratis keeps a targeted transfer pending until the target wins or the transfer's own timeout elapses, and while it
   * is pending the leader refuses every non-read-only request with {@code LeaderSteppingDownException}. A target that
   * has not answered cannot win, so the timeout is what it costs: all of it, in refused writes on every database. A
   * target that can win does so in about one round trip plus an election, far inside a slice.
   */
  static long candidateTransferBudgetMs(final long timeoutMs, final long remaining) {
    return Math.min(remaining, Math.max(timeoutMs / CANDIDATE_TRANSFER_BUDGET_SLICES, MIN_CANDIDATE_TRANSFER_BUDGET_MS));
  }

  /**
   * The last resort of {@link #transferLeadership(long)} and {@link RaftHAServer#stepDown()}, for when no peer is
   * eligible as an explicit target or every targeted transfer failed: asks Ratis to transfer leadership with no
   * target.
   * <p>
   * In Ratis that is NOT a transfer (issue #8480). A {@code null} target goes to {@code stepDownLeaderAsync}: the
   * leader becomes a follower at the same term, sends nothing to its peers, and replies success as soon as it has
   * stepped down. The followers keep naming it as the leader until their own election timers fire, 5 to 10 s later
   * with the default {@code arcadedb.ha.electionTimeoutMin/Max}, and that election frequently re-elects the same
   * node. So the reply proves nothing: success is reported only once a DIFFERENT peer is seen as the leader, whatever
   * Ratis answered, and the wait for it is the caller's remaining budget (never less than
   * {@link #leaderConfirmGraceMs}, the grace #4809 gave a transfer that raced its own client's close).
   *
   * @return true only when leadership settled on a peer other than this one
   */
  boolean stepDownWithoutTarget(final long timeoutMs) {
    final RaftClient client = raftHAServer.getClient();
    if (client == null)
      return false;

    final RaftPeerId selfId = raftHAServer.getLocalPeerId();
    // Re-checked here, not only by the callers: stepDown() reaches this after its candidates failed, and leadership
    // may have moved meanwhile. A no-target request sent through a FOLLOWER's client is routed to the real leader,
    // which would then step down although nobody asked it to (the #7134 hazard).
    if (!raftHAServer.isLeader())
      return false;
    // One budget for the RPC and the confirmation together: the confirmation gets what the RPC left of it.
    final long deadline = System.currentTimeMillis() + timeoutMs;
    // Never under a targeted transfer this node has in flight (issue #8557). Ratis routes a null target to
    // stepDownLeaderAsync, which - unlike the targeted path - is not refused while a transfer is pending: it would make
    // this node a follower at the same term, fail the pending transfer with it, and leave every follower naming the
    // ex-leader until its election timer fires. The transfer in flight is already the hand-off: wait for it instead.
    // The flag is raised BEFORE the counter is read, and the targeted overload raises its counter before it reads the
    // flag, so two racing callers cannot BOTH miss each other and both send. They can both see each other and both back
    // off: then neither RPC is sent, Ratis state does not move, and each caller reports that no hand-off happened (a
    // spurious failure the caller retries, never the leaderless window this guard exists to prevent).
    // The flag covers the RPC only, never the wait below: held through a back-off it would refuse, for seconds, targeted
    // transfers that nothing is racing (review of PR #8596).
    final boolean backedOff;
    bareStepDownsInFlight.incrementAndGet();
    try {
      backedOff = targetedTransfersInFlight.get() > 0;
      if (!backedOff)
        sendBareStepDown(client, timeoutMs);
    } finally {
      bareStepDownsInFlight.decrementAndGet();
    }
    if (backedOff)
      LogManager.instance().log(this, Level.INFO,
          "Not stepping down without a target: a targeted leadership transfer is in progress on this node; waiting for it");
    return confirmLeadershipMovedAway(selfId, confirmWindow(deadline));
  }

  private void sendBareStepDown(final RaftClient client, final long timeoutMs) {
    try {
      final RaftClientReply reply = client.admin().transferLeadership(null, timeoutMs);
      if (!reply.isSuccess())
        // Often because notifyLeaderChanged closed our client while the RPC was still in flight: confirm below
        // rather than trusting either the reply or a transient "we are no longer leader" state (issue #4809).
        LogManager.instance().log(this, Level.INFO, "No-target leadership transfer replied: %s", reply.getException());
    } catch (final Exception e) {
      // When leadership moves, notifyLeaderChanged calls refreshRaftClient() which closes the old client, so the
      // in-flight RPC fails with "is closed". Confirm an actual, settled handoff instead (issue #4809).
      LogManager.instance().log(this, Level.INFO, "No-target leadership transfer request: %s", e.getMessage());
    }
  }

  private static final long LEADER_CONFIRM_TIMEOUT_MS = 3_000;

  /**
   * The shortest wait for a different leader to settle before a handoff is declared not to have happened:
   * {@link #LEADER_CONFIRM_TIMEOUT_MS}, the grace #4809 gave a transfer that raced its own client's close.
   * Package-private and mutable only so unit tests can exercise the "no other leader appeared" outcome without
   * sleeping through the real grace.
   */
  long leaderConfirmGraceMs = LEADER_CONFIRM_TIMEOUT_MS;

  /** What is left of the caller's budget, never less than {@link #leaderConfirmGraceMs}. */
  private long confirmWindow(final long deadline) {
    return Math.max(deadline - System.currentTimeMillis(), leaderConfirmGraceMs);
  }

  /**
   * Whether leadership settled on a peer other than this one within the confirmation grace. For
   * {@link RaftHAServer#stepDown()}'s own candidate loop, which meets the same race as
   * {@link #transferLeadership(long)}: a targeted transfer whose call failed although the target won (#8487).
   */
  boolean leadershipMovedAway() {
    return confirmLeadershipMovedAway(raftHAServer.getLocalPeerId(), leaderConfirmGraceMs);
  }

  /**
   * Whether leadership settled on a peer other than this one within {@code waitMs} (never less than
   * {@link #leaderConfirmGraceMs}). For {@link RaftHAServer#stepDown()} when a transfer another caller started is
   * pending on this leader (issue #8557): that transfer has its own budget, and the grace alone would give up on it
   * too early.
   */
  boolean leadershipMovedAway(final long waitMs) {
    return confirmLeadershipMovedAway(raftHAServer.getLocalPeerId(), Math.max(waitMs, leaderConfirmGraceMs));
  }
  private static final long LEADER_CONFIRM_POLL_MS     = 50;

  /** The whole budget {@link #leaveCluster(boolean)} gives the leadership hand-off before it removes this node. */
  static final long LEAVE_HANDOFF_TIMEOUT_MS = 10_000;

  /**
   * Confirms that leadership has settled on a peer OTHER than {@code selfId} within {@code waitMs}. Used by
   * {@link #stepDownWithoutTarget(long)} so success is not reported merely
   * because this node transiently stopped being the leader (issue #4809): a real handoff ends with a
   * concrete, different peer established as leader, whereas a candidate / leaderless window leaves
   * {@link RaftHAServer#getLeaderId()} null or still pointing at this node.
   */
  private boolean confirmLeadershipMovedAway(final RaftPeerId selfId, final long waitMs) {
    final long deadline = System.currentTimeMillis() + waitMs;
    while (true) {
      final RaftPeerId leaderId = raftHAServer.getLeaderId();
      if (leaderId != null && !leaderId.equals(selfId))
        return true;
      if (System.currentTimeMillis() >= deadline)
        return false;
      try {
        Thread.sleep(LEADER_CONFIRM_POLL_MS);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  /**
   * Transfers leadership to a specific peer. Only the leader may do this.
   * <p>
   * The guard is not a local-capability check, it is the whole point (issue #7134): Ratis routes a
   * {@code TransferLeadershipRequest} submitted through a FOLLOWER's client to the current leader, so
   * without it a {@code POST /api/v1/cluster/leader} or {@code /cluster/stepdown} that landed on a follower -
   * which is what a Kubernetes ClusterIP Service does, since it load-balances across every ready endpoint -
   * forced an election on a leader that never asked for one, and answered 200 for an effect that landed on a
   * different node. #4809 added the same guard to the no-target {@link #transferLeadership(long)}; this is the
   * path it did not cover. It lives here rather than only in the handlers so no future caller can bypass it.
   * <p>
   * A failed call is settled, not sampled (issue #8487): the method waits, within {@code timeoutMs} (never less than
   * {@link #leaderConfirmGraceMs}), for a concrete leader and returns normally only when that leader is the target.
   *
   * @throws NotTheLeaderRefusalException          when this node is not the leader, naming the leader (when known) so
   *                                               the caller can retry against the right node
   * @throws LeadershipTransferInProgressException when Ratis refused it because another transfer, to a different
   *                                               peer, is already pending on this leader (issue #8557)
   * @throws ConfigurationException                when leadership did not settle on the target, naming the leader it
   *                                               settled on, or saying that none was elected in time
   */
  void transferLeadership(final String targetPeerId, final long timeoutMs) {
    if (!raftHAServer.isLeader())
      throw new NotTheLeaderRefusalException("Refusing to transfer leadership to " + targetPeerId,
          raftHAServer.getLeaderId());

    targetedTransfersInFlight.incrementAndGet();
    try {
      // The mirror of the guard in stepDownWithoutTarget (issue #8557): a bare step-down in flight is about to make this
      // node a follower, and a targeted transfer sent now would only race it. See there for why the order of the
      // increment and this read matters.
      if (bareStepDownsInFlight.get() > 0)
        throw new LeadershipTransferInProgressException(targetPeerId,
            new ConfigurationException("a leadership step-down without a target is in progress on this node"));
      sendTargetedTransfer(targetPeerId, timeoutMs);
    } finally {
      targetedTransfersInFlight.decrementAndGet();
    }
  }

  private void sendTargetedTransfer(final String targetPeerId, final long timeoutMs) {
    LogManager.instance().log(this, Level.INFO, "Transferring leadership to %s (timeout=%d ms)", targetPeerId, timeoutMs);
    final RaftPeerId targetId = RaftPeerId.valueOf(targetPeerId);
    final RaftPeerId selfId = raftHAServer.getLocalPeerId();
    // One budget for every attempt, not a fresh one per retry: a retry starts only before the deadline, each RPC gets
    // what is left of it, and each settle wait gets what is left floored at leaderConfirmGraceMs.
    final long deadline = System.currentTimeMillis() + timeoutMs;
    for (int attempt = 1; ; attempt++) {
      final Exception failure = sendTransfer(targetId, Math.max(deadline - System.currentTimeMillis(), 1));
      if (failure == null) {
        LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s", targetPeerId);
        return;
      }

      // A failed call does not mean a failed transfer, nor a successful one (issue #8487). Every leader change makes
      // notifyLeaderChanged -> refreshRaftClient() close the client the RPC went through, so the call fails with
      // "client-... is already CLOSED" whatever the election decided, and the leader view at the instant the failure
      // is caught is often still empty. Settle it: wait for a concrete leader, then judge by who that is.
      final RaftPeerId leaderId = awaitSettledLeader(selfId, confirmWindow(deadline));
      if (targetId.equals(leaderId)) {
        LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s (confirmed after the call failed: %s)",
            targetPeerId, failureMessage(failure));
        return;
      }
      if (leaderId == null)
        throw new ConfigurationException(
            "Failed to transfer leadership to " + targetPeerId + ": no leader was elected within the timeout ("
                + failureMessage(failure) + ")", failure);
      if (!leaderId.equals(selfId))
        throw new ConfigurationException(
            "Failed to transfer leadership to " + targetPeerId + ": leadership went to " + leaderId + " instead of "
                + targetPeerId + " (" + failureMessage(failure) + ")", failure);

      // This node is the leader, still or again. A refusal Ratis gave it is the answer. A closed client is not: the
      // refresh raced the call, or a failed election (the target losing a vote it was sent to win) re-elected this
      // node. Re-send through the fresh client while the budget lasts; to a transfer still pending for the same
      // target Ratis joins the new request rather than starting a second election.
      if (isTransferAlreadyPending(failure))
        throw new LeadershipTransferInProgressException(targetPeerId, failure);
      final boolean clientClosed = RaftGroupCommitter.isClientClosed(failure);
      if (!clientClosed)
        throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": " + failureMessage(failure),
            failure);
      if (attempt >= MAX_TRANSFER_ATTEMPTS || System.currentTimeMillis() >= deadline || !raftHAServer.isLeader())
        throw new ConfigurationException(
            "Failed to transfer leadership to " + targetPeerId + ": this node (" + selfId + ") is still the leader ("
                + failureMessage(failure) + ")", failure);
      LogManager.instance().log(this, Level.INFO, "Leadership transfer to %s interrupted by a client refresh (%s); retrying",
          targetPeerId, failureMessage(failure));
    }
  }

  /**
   * Targeted transfers this node is sending right now, from any caller (issue #8557). Read by
   * {@link #stepDownWithoutTarget(long)}, which must not pull the leadership out from under one of them.
   */
  private final AtomicInteger targetedTransfersInFlight = new AtomicInteger();

  /** Bare no-target step-downs this node is sending right now; the mirror of {@link #targetedTransfersInFlight}. */
  private final AtomicInteger bareStepDownsInFlight = new AtomicInteger();

  /**
   * Whether {@code t} (or a cause) is Ratis refusing a targeted transfer because another one, to a different peer, is
   * already pending on this leader (issue #8557). Ratis 3.3 builds that refusal in
   * {@code TransferLeadership.createReplyFutureFromPreviousRequest} as a {@code TransferLeadershipException} reading
   * {@code "<member>Failed to transfer leadership to <peer>: a previous <pending> exists"}, and returns it at once. A
   * transfer to the SAME peer is chained onto the pending one instead and never produces it. Matched by message, as
   * {@link RaftGroupCommitter#isClientClosed} is, because it reaches the client as a deserialised copy.
   */
  static boolean isTransferAlreadyPending(final Throwable t) {
    Throwable cur = t;
    for (int depth = 0; depth < 8 && cur != null; depth++) {
      final String msg = cur.getMessage();
      if (msg != null && msg.contains("Failed to transfer leadership to") && msg.contains(": a previous ")
          && msg.contains(" exists"))
        return true;
      cur = cur.getCause();
    }
    return false;
  }

  /** Attempts of one targeted transfer whose call failed only because its client was closed under it (#8487). */
  static final int MAX_TRANSFER_ATTEMPTS = 3;

  /** One targeted transfer RPC through the CURRENT client. Returns null on success, the failure otherwise. */
  private Exception sendTransfer(final RaftPeerId targetId, final long timeoutMs) {
    try {
      final RaftClientReply reply = raftHAServer.getClient().admin().transferLeadership(targetId, timeoutMs);
      if (reply.isSuccess())
        return null;
      final Exception failure = reply.getException();
      return failure != null ? failure : new ConfigurationException("Ratis refused the transfer without giving a reason");
    } catch (final IOException e) {
      return e;
    }
  }

  private static String failureMessage(final Exception failure) {
    final String message = failure.getMessage();
    return message != null && !message.isBlank() ? message : failure.toString();
  }

  /**
   * Waits up to {@code waitMs} for a concrete leader: another peer named by this node's view, or this node itself
   * once it actually holds the role (a view naming this node while it is no longer leader is stale). Returns null
   * when none settled in time.
   */
  private RaftPeerId awaitSettledLeader(final RaftPeerId selfId, final long waitMs) {
    final long deadline = System.currentTimeMillis() + waitMs;
    while (true) {
      final RaftPeerId leaderId = raftHAServer.getLeaderId();
      // Two reads, not one snapshot: this node can lose the role between them. Accepted - a stale "this node" verdict
      // only leads to the isLeader() re-check before a retry, never to a success being reported.
      if (leaderId != null && (!leaderId.equals(selfId) || raftHAServer.isLeader()))
        return leaderId;
      if (System.currentTimeMillis() >= deadline)
        return null;
      try {
        Thread.sleep(LEADER_CONFIRM_POLL_MS);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        return null;
      }
    }
  }

  void leaveCluster() {
    leaveCluster(false);
  }

  /**
   * Gracefully removes this node from the Raft group, transferring leadership first if this node is
   * the leader.
   * <p>
   * Unlike the previous implementation it does NOT swallow failures: a refusal to drop the cluster
   * below quorum (issue #4796) or a genuine {@code setConfiguration} failure propagates to the caller
   * so the operator (or the HTTP {@code /leave} endpoint) learns the node is still a committed member
   * rather than getting a false "left" acknowledgement. The leadership-transfer step stays best-effort:
   * if it fails the removal is still attempted (and the quorum guard still protects it).
   *
   * @param force when true, bypass the quorum guard (use only for an intentional scale-down to a
   *              cluster that may temporarily lose fault tolerance)
   */
  void leaveCluster(final boolean force) {
    if (raftHAServer.getClient() == null)
      return;

    final RaftPeerId localPeerId = raftHAServer.getLocalPeerId();

    final Collection<RaftPeer> currentPeers = raftHAServer.getLivePeers();
    if (currentPeers.size() <= 1) {
      HALog.log(this, HALog.BASIC, "Single-node cluster, skipping leave");
      return;
    }

    // Fail fast before transferring leadership if leaving would breach quorum, unless forced.
    ensureQuorumPreserved(localPeerId.toString(), currentPeers.size(), currentPeers.size() - 1, force);

    if (raftHAServer.isLeader()) {
      // The same ranked, screened candidates every other hand-off uses (RaftHAServer.selectStepDownTargets), not the
      // first peer in configuration order (issue #8592): a targeted transfer to a peer that cannot win stays pending
      // for its whole budget, and while it is pending this leader refuses every write on every database. With no
      // eligible peer, or when every candidate fails, no bare step-down follows: the removal below demotes this
      // leader itself once the new configuration commits, and the remaining voters elect among themselves.
      HALog.log(this, HALog.BASIC, "Leaving cluster: handing leadership off before removal");
      boolean moved = false;
      try {
        moved = transferLeadership(LEAVE_HANDOFF_TIMEOUT_MS, false);
      } catch (final Exception e) {
        HALog.log(this, HALog.BASIC, "Leadership transfer failed (%s), proceeding with removal", e.getMessage());
      }
      // transferLeadership() never lets an InterruptedException out: every wait it reaches catches it, restores the
      // flag and returns. That is what makes the flag, and not a catch, the way to learn the leave was interrupted.
      if (Thread.currentThread().isInterrupted())
        throw new ConfigurationException("Interrupted while leaving cluster");
      if (!moved)
        HALog.log(this, HALog.BASIC,
            "Leaving cluster: no peer took the leadership over, proceeding with removal (it demotes this leader)");
    }

    HALog.log(this, HALog.BASIC, "Leaving cluster: removing self (%s) from Raft group", localPeerId);
    removePeer(localPeerId.toString(), force);
    HALog.log(this, HALog.BASIC, "Successfully left the Raft cluster");
  }

  /**
   * Issues a {@code setConfiguration} call, retrying within {@link #setConfigurationBudgetMs}
   * ({@link GlobalConfiguration#HA_MEMBERSHIP_CHANGE_TIMEOUT}, 90 s by default) and re-evaluating
   * {@code argsSupplier} on every attempt.
   * <p>
   * Rebuilding the arguments each attempt is what makes {@code COMPARE_AND_SET} removals safe: a retry
   * after a CAS mismatch (a concurrent membership change) re-snapshots the current configuration so the
   * next attempt's precondition is fresh. It also survives the window on a fresh cluster where a newly
   * elected leader has not yet committed an entry from its own term and therefore transiently rejects
   * configuration changes. A {@code null} from the supplier means the goal is already met and the call
   * returns successfully.
   * <p>
   * The budget bounds the WHOLE call and not merely the gaps between attempts (issue #7561): see
   * {@link #canAttemptAgain} for the arm that stops the overshoot and {@link #isPermanent} for the failure
   * that is not waited out at all.
   */
  private void setConfigurationWithRetry(final Supplier<SetConfigurationRequest.Arguments> argsSupplier,
      final String operationDesc, final String hint) {
    // A client of this operation's own, so one setConfiguration call is bounded by MEMBERSHIP_RETRY_POLICY -
    // three attempts, so at worst three RPC timeouts - instead of by the shared client's RetryLimited(60, 1s),
    // which is what made the deadline below unobservable for a whole minute at a time (issue #7561). Null on a
    // harness that never started a Raft server, and then the shared client is the only one there is.
    //
    // Built inside the try so a failure to build it is reported as the ConfigurationException every other
    // failure of this method is (review of PR #7941): a caller catching that type must not have an unwrapped
    // runtime exception come out of one path and not the others.
    final RaftClient dedicated;
    try {
      dedicated = raftHAServer.newMembershipClient();
    } catch (final RuntimeException e) {
      throw new ConfigurationException("Failed to " + operationDesc
          + ": the Raft client for the membership change could not be built: " + describe(e), e);
    }

    try {
      final RaftClient client = dedicated != null ? dedicated : raftHAServer.getClient();
      if (client == null)
        // Both null means this node has no Raft client at all, which is a node whose Raft server has not
        // started. Said out loud rather than left to NPE out of client.admin() two frames down.
        throw new ConfigurationException("Failed to " + operationDesc
            + ": this node has no Raft client, so its Raft server has not started yet. Retry once the node has"
            + " joined the cluster.");
      setConfigurationWithRetry(client, argsSupplier, operationDesc, hint);
    } finally {
      if (dedicated != null)
        try {
          dedicated.close();
        } catch (final IOException e) {
          LogManager.instance().log(this, Level.FINE,
              "Could not close the membership-change client after %s: %s", operationDesc, e.getMessage());
        }
    }
  }

  private void setConfigurationWithRetry(final RaftClient client,
      final Supplier<SetConfigurationRequest.Arguments> argsSupplier, final String operationDesc, final String hint) {
    final long startedAt = System.currentTimeMillis();
    final long deadline = startedAt + setConfigurationBudgetMs;
    long sleepMs = 200;
    // The longest attempt observed so far. An attempt is only started when the budget still has room for one
    // that long: the deadline used to be checked only BETWEEN attempts, so an attempt beginning just inside it
    // ran to completion outside it and the caller waited out one whole extra attempt (issue #7561).
    long longestAttemptMs = 0;

    while (true) {
      try {
        // Rebuild the arguments on every attempt so COMPARE_AND_SET retries re-snapshot the current
        // configuration after a concurrent change (issue #4795). A null result means the goal is
        // already met (e.g. the peer was removed by a concurrent change).
        final SetConfigurationRequest.Arguments args = argsSupplier.get();
        if (args == null)
          return;

        final long attemptStartedAt = System.currentTimeMillis();
        final RaftClientReply reply;
        try {
          reply = client.admin().setConfiguration(args);
        } finally {
          longestAttemptMs = Math.max(longestAttemptMs, System.currentTimeMillis() - attemptStartedAt);
        }
        if (reply.isSuccess())
          return;

        final Exception failure = reply.getException();
        if (isPermanent(failure))
          throw new ConfigurationException(permanentMessage(operationDesc, startedAt, failure));

        if (canAttemptAgain(deadline, sleepMs, longestAttemptMs)) {
          LogManager.instance().log(this, Level.FINE,
              "setConfiguration failed for %s, retrying in %d ms: %s", operationDesc, sleepMs, failure);
          Thread.sleep(sleepMs);
          sleepMs = Math.min(sleepMs * 2, 2_000);
          continue;
        }
        throw new ConfigurationException(gaveUpMessage(operationDesc, hint, startedAt, failure));
      } catch (final IOException e) {
        if (isPermanent(e))
          throw new ConfigurationException(permanentMessage(operationDesc, startedAt, e), e);

        if (canAttemptAgain(deadline, sleepMs, longestAttemptMs)) {
          LogManager.instance().log(this, Level.FINE,
              "setConfiguration I/O error for %s, retrying in %d ms", operationDesc, sleepMs);
          try {
            Thread.sleep(sleepMs);
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ConfigurationException("Interrupted while waiting to " + operationDesc, ie);
          }
          sleepMs = Math.min(sleepMs * 2, 2_000);
          continue;
        }
        throw new ConfigurationException(gaveUpMessage(operationDesc, hint, startedAt, e), e);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConfigurationException("Interrupted while waiting to " + operationDesc, e);
      }
    }
  }

  /**
   * Whether the budget still has room for the backoff AND one more attempt as long as the longest one so far
   * (issue #7561).
   * <p>
   * The predicate is deliberately on the LONGEST attempt rather than the last: attempt durations here are
   * bimodal - a synchronous rejection returns in microseconds, a round that waits out the client's retry policy
   * takes seconds - and sizing the decision on a fast rejection would let the slow shape start again with no
   * room to finish, which is the overshoot this replaces. A zero {@code longestAttemptMs} (nothing has been
   * attempted yet, or the attempt was instantaneous) leaves the backoff and the deadline, so a zero budget still
   * reaches the give-up branch on the first pass exactly as it did before.
   */
  private static boolean canAttemptAgain(final long deadline, final long backoffMs, final long longestAttemptMs) {
    return System.currentTimeMillis() + backoffMs + longestAttemptMs < deadline;
  }

  /**
   * Whether {@code failure} says the membership change can NEVER commit, so retrying it until the budget runs
   * out only makes the operator wait (issue #7539, scope item 3).
   * <p>
   * Exactly one Ratis failure qualifies today, and the narrowness is the point. A
   * {@link GroupMismatchException} means the peer answered from a DIFFERENT Raft group - a node configured for
   * another cluster, which is one of the four causes issue #7539 lists and the only one of them that says so on
   * the wire. Everything else that arrives here is or may be progress: {@code ReconfigurationInProgressException}
   * is the leader holding a {@code Mode.ADD} open while the new peer catches up, {@code LeaderNotReadyException}
   * and {@code NotLeaderException} are an election settling, and a {@code SetConfigurationException} on the
   * removal path is the compare-and-set precondition that the next attempt re-snapshots. Classifying any of those
   * as permanent would turn a slow but succeeding join into a refusal.
   */
  private static boolean isPermanent(final Throwable failure) {
    // Bounded rather than walked to the end: a Ratis failure wraps a handful of causes at most, and a bound is
    // the one cycle guard that needs no identity comparison of two Throwables.
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; cause = cause.getCause(), depth++)
      if (cause instanceof GroupMismatchException)
        return true;
    return false;
  }

  /**
   * The report for a failure that cannot be retried into a success (issue #7539). Says how long the request
   * actually took, because the whole point of the classification is that it is a fraction of the budget.
   */
  private String permanentMessage(final String operationDesc, final long startedAt, final Throwable failure) {
    return "Failed to " + operationDesc + " after " + (System.currentTimeMillis() - startedAt)
        + " ms: the peer answered from a different Raft group, so it belongs to another cluster and this membership"
        + " change can never commit. Check that it is started with the same " + GlobalConfiguration.HA_CLUSTER_NAME.getKey()
        + " as this node. Not retried within the " + setConfigurationBudgetMs + " ms budget, because no number of"
        + " retries changes which cluster a peer belongs to. Raft reported: " + describe(failure);
  }

  /**
   * The sentence a membership change that ran out of budget is reported with (issue #7514).
   * <p>
   * What it replaces was {@code "Failed to " + operationDesc} with the Ratis failure as the cause, and the
   * message that reached the operator was therefore the serialized request object - {@code "Failed
   * SetConfigurationRequest:client-48FB...->localhost_2435@group-E6A6..., cid=11, seq=null, RW, null, ADD,
   * servers:[localhost_2436|localhost:2436], listeners:[] for 60 attempts with RetryLimited(maxAttempts=60,
   * sleepTime=1s)"}. Three things were missing from it and are here instead: what was being attempted in
   * words, how long it was attempted for, and what an operator should look at next.
   * <p>
   * The Ratis text is kept, last and labelled. It is genuinely diagnostic - the attempt count and the retry
   * policy are in it - and dropping it would trade one incomplete report for another.
   */
  private String gaveUpMessage(final String operationDesc, final String hint, final long startedAt,
      final Throwable ratisFailure) {
    final StringBuilder message = new StringBuilder(256);
    message.append("Failed to ").append(operationDesc)
        .append(": the Raft configuration change did not commit within ").append(setConfigurationBudgetMs)
        .append(" ms (gave up after ").append(System.currentTimeMillis() - startedAt).append(" ms).");
    if (hint != null && !hint.isBlank())
      message.append(' ').append(hint);
    message.append(" Raft reported: ").append(describe(ratisFailure));
    return message.toString();
  }

  /** The most readable form of a Ratis failure: its message when it has one, its type and message otherwise. */
  private static String describe(final Throwable failure) {
    if (failure == null)
      return "no error detail";
    final String failureMessage = failure.getMessage();
    return failureMessage != null && !failureMessage.isBlank() ? failureMessage : failure.toString();
  }

  private int getHttpPortOffset() {
    final Map<RaftPeerId, String> httpAddresses = raftHAServer.getHttpAddresses();
    for (final RaftPeer peer : raftHAServer.getRaftGroup().getPeers()) {
      final String httpAddr = httpAddresses.get(peer.getId());
      if (httpAddr != null) {
        try {
          final int httpPort = Integer.parseInt(httpAddr.substring(httpAddr.lastIndexOf(':') + 1));
          final int raftPort = Integer.parseInt(
              peer.getAddress().substring(peer.getAddress().lastIndexOf(':') + 1));
          return httpPort - raftPort;
        } catch (final NumberFormatException ignored) {
        }
      }
    }
    return 46;
  }
}
