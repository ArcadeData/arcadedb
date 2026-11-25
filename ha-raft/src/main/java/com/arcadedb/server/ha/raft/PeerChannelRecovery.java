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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.PeerRequest;
import com.arcadedb.server.ha.raft.RaftHAServer.QuarantineHandoff;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;
import java.util.logging.Level;
import javax.net.ssl.HttpsURLConnection;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;

/**
 * Recovers a peer whose replication has stalled, from the leader's side: resets the appender channel to it, asks it to
 * resync from a snapshot, and when the channel stays wedged or a database is quarantined here, hands leadership to a
 * peer that can serve - each step behind a cooldown, so a flapping peer cannot keep leadership moving.
 * <p>
 * Split out of {@link RaftHAServer}, whose health monitor decides when a peer needs it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PeerChannelRecovery {
  private final RaftHAServer ha;

  // Timeout for the leadership transfer that escalates an unrecoverable replication channel (issue #5346).
  // Matches the manual step-down timeout: the transfer either lands within a couple of election rounds or
  // is not going to.
  private static final long                  ESCALATION_TRANSFER_TIMEOUT_MS = 10_000L;

  // Minimum interval between two leadership-transfer escalations raised by THIS node for the SAME follower
  // (issue #5346). ClusterMonitor's per-streak latch is wiped by clusterMonitor.reset() on every leadership
  // acquisition, so without a node-local cooldown a follower that is permanently unreachable for a
  // node-independent reason would bounce leadership around the healthy peers forever: each new leader would
  // build its own appender, fail, and escalate onward. The cooldown bounds that to one transfer per healthy
  // peer per window, after which the cluster settles on the log-and-stop path.
  private static final long                  ESCALATION_COOLDOWN_MS = 30 * 60_000L;

  // Last escalation timestamp per follower id, for the cooldown above. Node-local and intentionally NOT
  // cleared on leadership change - that is precisely what makes it bound cross-node flapping.
  private final    Map<String, Long>         lastChannelEscalationAtMs = new ConcurrentHashMap<>();

  // Minimum interval between two leadership handoffs this node starts because it holds data it knows is behind the
  // committed log while it leads (issue #8483). The handoff is the only way such a leader heals - it cannot resync from
  // itself - but a database that EVERY peer quarantines (a deterministic apply error, say) would otherwise bounce
  // leadership around the cluster on every health tick. Node-local and kept across leadership changes for the same
  // reason as the channel-escalation cooldown above: it bounds that to one handoff per node per window.
  private static final long                  QUARANTINE_HANDOFF_COOLDOWN_MS = 10 * 60_000L;

  // Wall-clock of the last admitted quarantine handoff, for the cooldown above; 0 = none yet.
  final AtomicLong                lastQuarantineHandoffAtMs = new AtomicLong();

  // Wall-clock of the last "no other peer to hand leadership to" report, throttled to the same window; 0 = none yet.
  final AtomicLong                lastQuarantineNoPeerLogAtMs = new AtomicLong();

  PeerChannelRecovery(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Leader-driven recovery for a persistently STALLED replica (issue #4728). Invoked by
   * {@link ClusterMonitor} on the leader's lag-monitor thread once a replica's {@code matchIndex} has
   * not advanced for {@link GlobalConfiguration#HA_STALLED_REPLICA_RESYNC_DURATION_MS} while the
   * leader kept committing. The leader instructs the stuck follower to drop its local copy and
   * re-acquire a fresh full snapshot, the same operation an operator runs manually via
   * {@code POST /api/v1/cluster/resync/{database}} - reused here so the cluster self-heals instead of
   * logging the stall forever.
   * <p>
   * The HTTP work is done on a short-lived daemon thread so the lag-monitor thread is never blocked
   * on network I/O. Authenticated with the inter-node cluster token (the same trust mechanism used
   * for snapshot transfer), never with operator credentials.
   * <p>
   * The order carries the leader's view at decision time ({@link StalledResyncOrder}: term, the follower's
   * {@code matchIndex}, the leader's commit index) and is re-checked twice before anything is dropped (issue #8490):
   * here, before each database is requested, against this leader's live view ({@link #staleStalledResyncOrderReason}),
   * and on the follower, against its own term and applied index, by {@link PostResyncDatabaseHandler}.
   */
  void forceResyncStalledReplica(final String peerId) {
    if (peerId == null || !ha.isLeader())
      return;

    final RaftPeerId targetId = RaftPeerId.valueOf(peerId);
    if (targetId.equals(ha.localPeerId))
      return; // never resync the leader itself

    // Captured on the lag-monitor thread, in the same tick that decided the resync, so the order describes exactly
    // the state the decision was made on.
    final StalledResyncOrder order = new StalledResyncOrder(ha.getCurrentTerm(), ha.clusterMonitor.getReplicaMatchIndex(peerId),
        ha.clusterMonitor.getLeaderCommitIndex());
    final long stallGeneration = ha.clusterMonitor.getStallGeneration(peerId);

    // Through the guard, like every other unattended dial of a resolved peer address (issue #6221). This one
    // carries the most destructive payload of them all - "drop your copy of every database and download it
    // again" - so an address that names the wrong follower costs a healthy node its local copies, and the id
    // check above cannot see that: it says who we MEAN, not where the address points.
    final PeerDialAddress dial = PeerDialAddress.resolve(ha, targetId, "stalled replica");
    if (dial.refused()) {
      LogManager.instance().log(ha, Level.WARNING, "Cannot force resync of stalled replica '%s': %s", peerId,
          dial.refusal());
      return;
    }

    // On an SSL-enabled cluster the follower listens on HTTPS (a different port from plain HTTP), so
    // prefer its HTTPS endpoint; mirror SnapshotInstaller's behaviour and fall back to plain HTTP
    // only when no HTTPS endpoint is known.
    final boolean useSSL = ha.configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    final boolean https = useSSL && dial.httpsAddress() != null;
    final String followerAddr = https ? dial.httpsAddress() : dial.httpAddress();

    final String clusterToken = ha.getClusterToken();
    if (clusterToken == null || clusterToken.isEmpty()) {
      // The follower's resync endpoint only accepts inter-node calls authenticated with the cluster
      // token; without one the request is doomed to be rejected, so do not even attempt it.
      LogManager.instance().log(ha, Level.WARNING,
          "Cannot force resync of stalled replica '%s': cluster token is not configured", peerId);
      return;
    }

    final String address = followerAddr;
    final boolean useHttps = https;

    ha.stalledResyncExecutor.execute(() -> {
      // TODO (#4728): this resyncs EVERY non-reserved database on the follower. When a cluster hosts
      // many databases and only one is stuck, this forces unnecessary full snapshots of the others.
      // A per-database stall signal would let us scope the resync; acceptable as a first pass.
      for (final String dbName : ha.arcadeServer.getDatabaseNames()) {
        // Re-check leadership on each iteration: the outer guard ran before this task was queued, and
        // leadership may have moved since. A resync request from a non-leader is harmless (the
        // follower rejects it) but would log spurious warnings, so stop early instead.
        if (!ha.isLeader())
          return;
        if (ArcadeDBServer.isReservedDatabaseName(dbName))
          continue;
        // Issue #8490: the task may run long after the decision (queued behind another, or behind the previous
        // database's snapshot download), and each database is one more copy the follower drops. Re-check that the
        // stall the order was based on is still current before sending it.
        final String stale = staleStalledResyncOrderReason(ha.isLeader(), ha.getCurrentTerm(), ha.clusterMonitor, peerId,
            stallGeneration, order);
        if (stale != null) {
          LogManager.instance().log(ha, Level.INFO,
              "Not forcing a resync of database '%s' on replica '%s': %s", dbName, peerId, stale);
          return;
        }
        try {
          requestRemoteResync(address, dbName, clusterToken, useHttps, order);
          LogManager.instance().log(ha, Level.INFO,
              "Requested resync of database '%s' on stalled replica '%s' (%s)", dbName, peerId, address);
        } catch (final StalledResyncDeclinedException e) {
          // The follower checked the order against its own state and found it stale: nothing was dropped. Keep
          // going: the follower judges each database by its own trusted applied index, which a per-database read
          // floor can hold lower than the others', so one refusal does not answer for the rest.
          LogManager.instance().log(ha, Level.INFO,
              "Replica '%s' declined the forced resync of database '%s': %s", peerId, dbName, e.getMessage());
        } catch (final Exception e) {
          LogManager.instance().log(ha, Level.WARNING,
              "Failed to request resync of database '%s' on stalled replica '%s' (%s): %s",
              dbName, peerId, address, e.getMessage());
        }
      }
    });
  }

  /**
   * When each peer last answered over HTTP, as {@link ClusterMonitor#setPeerLastAnsweredAt} reads it (issue #8900): the
   * time of its fresh capability advertisement, or -1 when it has none. Package-private for the wiring test.
   */
  static ToLongFunction<String> peerLastAnsweredAt(final PeerCapabilityRegistry registry) {
    return peerId -> {
      final PeerCapabilityRegistry.Advertisement advertisement = registry.freshAdvertisementOf(peerId);
      return advertisement != null ? advertisement.observedAtMs() : -1L;
    };
  }

  /**
   * Leader-side recovery for a follower whose outbound replication gRPC channel has wedged on a stale
   * DNS result after the follower restarted with a new address (issue #4696, "Gap 1"). Invoked by
   * {@link ClusterMonitor} on the leader's lag-monitor thread once the follower has stayed continuously
   * unreachable (no successful RPC) for {@link GlobalConfiguration#HA_PEER_CHANNEL_RESET_DURATION}.
   * <p>
   * Ratis keeps one outbound {@code GrpcServerProtocolClient} per follower in the leader's server-RPC
   * {@code PeerProxyMap}. It wraps more than one gRPC {@code ManagedChannel} (issue #10374): the primary
   * one and, by Ratis default, a separate one for the heartbeat stream. When the follower's pod IP changes,
   * grpc-java can keep returning the stale/negative DNS result on those cached channels indefinitely -
   * Ratis's own error path only calls {@code resetConnectBackoff()}, which never recreates a channel
   * or re-resolves DNS - so the appender never reconnects. {@code resetProxy} closes that client, every
   * channel of it, and drops it from the map; the appender's next send rebuilds fresh channels against
   * the same peer DNS name, which re-resolve to the follower's current IP. Only this one follower's
   * channels are touched and leadership is unchanged, so there is no flapping risk (unlike a leadership
   * transfer).
   * <p>
   * This is the automatic, less-disruptive alternative to the manual {@code transferLeadership} lever
   * that Gap 1 was previously left to; when its bounded retry budget is exhausted the monitor escalates
   * to {@link #escalateWedgedPeerChannel} (issue #5346).
   * <p>
   * The reset runs on {@link RaftHAServer#channelRecoveryExecutor} rather than inline: closing a channel and resolving
   * the peer's address are both blocking operations, and the caller is the single lag-monitor thread that
   * classifies every replica.
   */
  void resetPeerReplicationChannel(final String peerId) {
    final RaftServer server = ha.raftServer;
    if (peerId == null || server == null || ha.shutdownRequested || !ha.isLeader())
      return;

    final RaftPeerId targetId = RaftPeerId.valueOf(peerId);
    if (targetId.equals(ha.localPeerId))
      return; // the leader keeps no appender channel to itself

    try {
      ha.channelRecoveryExecutor.execute(() -> {
        // Issue #5346: log what the peer name resolves to on both sides of the reset. When the channel is
        // wedged despite healthy DNS (the reported incident), this is the evidence that distinguishes "the
        // reset re-resolved to a new IP" from "re-resolution was never the problem".
        final String before = ha.resolvePeerAddresses(targetId);

        // ClusterMonitor already logs the operator-facing WARNING announcing the reset (with the attempt
        // count and how long the follower has been unreachable), so a success is only confirmed at FINE to
        // avoid a redundant second WARNING. A no-op is the surprising case worth surfacing at WARNING.
        if (RaftHAServer.resetPeerAppenderChannel(server.getServerRpc(), targetId))
          LogManager.instance().log(ha, Level.FINE,
              "Reset the replication gRPC channel to unreachable follower '%s' to force a fresh DNS re-resolution and "
                  + "reconnect (issue #4696). Resolved before=%s, after=%s.",
              peerId, before, ha.resolvePeerAddresses(targetId));
        else
          LogManager.instance().log(ha, Level.WARNING,
              "Requested a replication-channel reset for follower '%s' (resolved to %s) but the Raft server RPC is not "
                  + "proxy-based; cannot reset the channel.",
              peerId, before);
      });
    } catch (final RejectedExecutionException e) {
      // Not free: ClusterMonitor incremented channelResetCount before invoking this handler, so the dropped
      // task still consumed one of CHANNEL_RESET_MAX_ATTEMPTS. The monitor retries on its next interval, but
      // a run of drops can reach the escalation cap without five resets ever executing. Only reachable when
      // several followers wedge at once (the single worker is otherwise idle and a reset is fast), which is
      // why it is reported rather than compensated for.
      LogManager.instance().log(ha, Level.WARNING,
          "Replication-channel recovery queue is saturated; the reset for follower '%s' was dropped and its attempt "
              + "still counted against the retry budget.",
          peerId);
    }
  }

  /**
   * Terminal leader-side recovery for a follower whose replication channel is still dead after the bounded
   * {@link GlobalConfiguration#HA_PEER_CHANNEL_RESET_DURATION} reset budget has been exhausted (issue #5346).
   * <p>
   * Resetting the cached proxy is only half a fix: Ratis rebuilds the channel lazily, on the appender's next
   * send, so when the peer's {@code GrpcLogAppender} is itself wedged no send is ever attempted and the reset
   * is a silent no-op. In the reported incident all five attempts ran with healthy DNS and open TCP, the
   * follower stayed at {@code matchIndex = -1} for 5.5 minutes, and only restarting the leader recovered it -
   * because a new leader builds a brand-new appender. This method performs that appender rebuild without the
   * restart, by transferring leadership to a healthy peer.
   * <p>
   * The transfer target is chosen with the same rules as a manual step-down and is never the wedged follower
   * itself. When no healthy target exists (e.g. a two-node cluster whose only follower is the wedged one, or
   * every remaining peer is lagging) the previous behaviour is kept: log for operator intervention rather
   * than flap leadership onto a peer that cannot serve. Runs on {@link RaftHAServer#channelRecoveryExecutor} because the
   * transfer blocks on network I/O and the caller is the lag-monitor thread.
   * <p>
   * Thread-safety: the task reads {@link ClusterMonitor} state (via {@code selectChannelEscalationTarget} ->
   * {@code isReplicaLagging}) and the Raft configuration from the recovery thread while the lag-monitor
   * thread mutates that state. This is the same tolerated-staleness contract the existing
   * {@code stepDown()} path relies on - {@code replicaStates} is a {@link ConcurrentHashMap} and
   * {@code leaderCommitIndex} is volatile - and a target chosen from a tick-old snapshot is still valid,
   * because Ratis rejects a transfer to a peer that has since become unsuitable.
   */
  void escalateWedgedPeerChannel(final String peerId) {
    if (peerId == null || ha.raftServer == null || ha.shutdownRequested || !ha.isLeader())
      return;

    if (RaftPeerId.valueOf(peerId).equals(ha.localPeerId))
      return;

    try {
      ha.channelRecoveryExecutor.execute(() -> {
        // Leadership may have moved while this task sat in the queue. Re-check before choosing a target so the
        // choice is made against the configuration this node is actually leading - and note that this re-check
        // is load-bearing rather than cosmetic: Ratis does NOT reject a transfer submitted through a
        // non-leader's client, it routes it to the current leader, so a stale task would force an election on a
        // node that never asked for one (issue #7134).
        if (ha.shutdownRequested || !ha.isLeader())
          return;

        final RaftPeer target = selectChannelEscalationTarget(ha.getLivePeers(), ha.localPeerId, peerId, ha.clusterMonitor,
            ha.handoffReachablePeers());
        if (target == null) {
          LogManager.instance().log(ha, Level.SEVERE,
              "Follower '%s' has a permanently dead replication channel and no healthy peer is eligible to take over "
                  + "leadership; giving up automatic recovery - operator intervention is required.",
              peerId);
          return;
        }

        // Cooldown gate (see ESCALATION_COOLDOWN_MS): refuse to bounce leadership for the same follower again
        // so soon. Deliberately the LAST gate, immediately before the transfer, so the window is consumed only
        // when a transfer is actually attempted - a task dropped earlier (not leader, no eligible target) must
        // not burn it, or a genuinely recoverable escalation would be suppressed for 30 minutes once a healthy
        // peer rejoins.
        if (!admitChannelEscalation(lastChannelEscalationAtMs, peerId, System.currentTimeMillis())) {
          LogManager.instance().log(ha, Level.SEVERE,
              "Follower '%s' still has a dead replication channel, but this node already attempted a leadership transfer "
                  + "for it within the last %dms; not escalating again - the follower is unreachable for a reason a fresh "
                  + "appender does not fix, so operator intervention is required.",
              peerId, ESCALATION_COOLDOWN_MS);
          return;
        }

        final String targetId = target.getId().toString();
        LogManager.instance().log(ha, Level.WARNING,
            "Transferring leadership to '%s' to rebuild the replication appender for wedged follower '%s' (issue #5346).",
            targetId, peerId);
        try {
          // A failure below leaves the cooldown consumed, deliberately: the transfer was attempted, and a
          // node that cannot hand off leadership would otherwise re-attempt every reset cycle. The next
          // escalation for this follower waits out the window, or comes from another peer.
          ha.transferLeadership(targetId, ESCALATION_TRANSFER_TIMEOUT_MS);
        } catch (final Exception e) {
          LogManager.instance().log(ha, Level.SEVERE,
              "Leadership transfer to '%s' to recover wedged follower '%s' failed: %s; operator intervention is required.",
              targetId, peerId, e.getMessage());
        }
      });
    } catch (final RejectedExecutionException e) {
      // Unlike a reset, this is a ONE-SHOT: ClusterMonitor latches the give-up flag before invoking this
      // handler and fires it once per unreachable streak, so a dropped escalation never re-fires for that
      // streak. Surface it at SEVERE rather than letting the follower fall silently onto the
      // operator-intervention path with only a generic queue-full warning.
      LogManager.instance().log(ha, Level.SEVERE,
          "Follower '%s' has a permanently dead replication channel but the recovery queue is saturated, so the "
              + "leadership-transfer escalation was dropped and will not be retried for this outage; operator "
              + "intervention is required.",
          peerId);
    }
  }

  /**
   * Admits or refuses a leadership-transfer escalation for {@code peerId} at {@code now}, recording the
   * timestamp when it admits (issue #5346). Returns {@code true} at most once per
   * {@link #ESCALATION_COOLDOWN_MS} per follower.
   * <p>
   * This is what bounds cross-node leadership flapping. {@link ClusterMonitor}'s per-streak latch cannot do
   * it: {@code clusterMonitor.reset()} wipes every replica's channel state on each leadership acquisition
   * (issue #4841), so each new leader would otherwise arrive with a fresh escalation budget and hand the
   * problem on to the next peer indefinitely. Because this map is node-local and survives leadership
   * changes, a permanently unreachable follower costs at most one transfer per healthy peer per window and
   * then settles on the operator-intervention path.
   * <p>
   * Package-private so the cooldown can be unit-tested without a live cluster.
   */
  static boolean admitChannelEscalation(final Map<String, Long> lastEscalationAtMs, final String peerId,
      final long now) {
    // Compare-and-set rather than get-then-put: today the only caller is the single-worker
    // channelRecoveryExecutor, but relying on that invariant would silently allow a double transfer if a
    // future caller escalates from another thread.
    while (true) {
      final Long last = lastEscalationAtMs.putIfAbsent(peerId, now);
      if (last == null)
        return true; // first escalation for this follower
      if (now - last < ESCALATION_COOLDOWN_MS)
        return false; // inside the window
      if (lastEscalationAtMs.replace(peerId, last, now))
        return true;
      // Lost the race against a concurrent escalation for the same follower: re-read and re-decide.
    }
  }

  /**
   * The SEVERE line for a leader that holds a quarantine or a read floor and has no peer to hand the leadership to
   * (issue #8483). On a one-voter configuration no peer can ever become eligible, so promising a retry would be false:
   * the state is terminal until an operator acts, and the line names what it costs while it lasts (issue #9308).
   */
  // @VisibleForTesting
  static String noHandoffPeerReport(final String reason, final boolean soleVoter) {
    if (soleVoter)
      return "This leader holds " + reason + ", which it cannot resync from itself, and it is the only voter of the Raft "
          + "configuration, so no peer can ever take over leadership or serve the resync: this state is terminal until an "
          + "operator acts (issue #9308). While it lasts the node reports not-ready (GET /api/v1/ready fails) and the Raft "
          + "log is not checkpointed, so it grows until the volume fills. Restore the database from a backup, drop it, add a "
          + "peer so that leadership can move and this node can resync from it, or, only if this copy is known to be "
          + "acceptable as it is, lift the quarantine with POST " + PostAcceptDivergedHandler.ROUTE + "{database} on this "
          + "node (root only, issue #9449), or lift a node-wide read floor with POST " + PostAcceptStaleSnapshotHandler.ROUTE
          + " (root only, issue #9498).";
    return "This leader holds " + reason + ", which it cannot resync from itself, and no peer is eligible to take over "
        + "leadership (none other is configured, or every other one is lagging, unreachable or a priority-0 replica) "
        + "(issue #8483). The handoff is retried as soon as a peer is eligible. With no other peer, restore the database "
        + "from a backup or add a peer so that leadership can move and this node can resync from it.";
  }

  void transferLeadershipToResync(final String reason) {
    LogManager.instance().log(ha, Level.WARNING,
        "This leader holds %s, which it cannot resync from itself: handing leadership to a healthy peer so this "
            + "node can resync from it (issue #8483)", reason);
    try {
      if (ha.transferLeadership(ESCALATION_TRANSFER_TIMEOUT_MS))
        return;
    } catch (final Exception e) {
      LogManager.instance().log(ha, Level.WARNING, "Leadership handoff to resync %s failed: %s", reason,
          e.getMessage());
    }
    if (ha.isLeader())
      LogManager.instance().log(ha, Level.SEVERE,
          "Could not hand leadership off to resync %s; retrying in %d minutes. Transfer it manually with "
              + "POST /api/v1/cluster/leader (issue #8483).", reason, QUARANTINE_HANDOFF_COOLDOWN_MS / 60_000L);
  }

  /**
   * Decides what a queued quarantine handoff does, taking the cooldown window only when a transfer is actually about
   * to be attempted (issue #8483) - the same ordering {@link #escalateWedgedPeerChannel} keeps, for the same reason:
   * a handoff dropped because this node no longer leads, or because no other peer is configured yet, must not
   * suppress the one that becomes possible when a peer joins moments later. The no-peer report has a window of its
   * own, so a peer-less leader logs it once per window rather than on every health tick.
   * <p>
   * Package-private and static so the ordering can be unit-tested without a cluster.
   */
  static QuarantineHandoff decideQuarantineHandoff(final boolean leader, final boolean anotherPeer,
      final AtomicLong lastHandoffAtMs, final AtomicLong lastNoPeerReportAtMs, final long now) {
    if (!leader)
      return QuarantineHandoff.NOT_LEADER;
    if (!anotherPeer)
      return admitQuarantineHandoff(lastNoPeerReportAtMs, now) ? QuarantineHandoff.NO_PEER_REPORT : QuarantineHandoff.NO_PEER;
    return admitQuarantineHandoff(lastHandoffAtMs, now) ? QuarantineHandoff.TRANSFER : QuarantineHandoff.COOLDOWN;
  }

  static boolean withinQuarantineHandoffCooldown(final long last, final long now) {
    return last != 0L && now - last < QUARANTINE_HANDOFF_COOLDOWN_MS;
  }

  /**
   * Admits a quarantine handoff at {@code now} at most once per {@link #QUARANTINE_HANDOFF_COOLDOWN_MS}, recording the
   * time when it does (issue #8483). Package-private and static so the window can be unit-tested without a cluster.
   */
  static boolean admitQuarantineHandoff(final AtomicLong lastHandoffAtMs, final long now) {
    while (true) {
      final long last = lastHandoffAtMs.get();
      if (withinQuarantineHandoffCooldown(last, now))
        return false;
      if (lastHandoffAtMs.compareAndSet(last, now))
        return true;
    }
  }

  /**
   * Whether a peer is eligible to take leadership over, by the rules a manual step-down uses
   * ({@link StepDownTargets#selectStepDownTargets}: not this node, not lagging, not a priority-0 replica while real voters exist) and,
   * through the four-argument form production uses, proven reachable (issue #8556).
   * Asked before the handoff rather than left to {@link RaftHAServer#transferLeadership(long)}, whose no-target fallback would
   * otherwise put a cluster with no eligible peer through a step-down that re-elects this node (code review on PR
   * #8531), exactly what {@link #escalateWedgedPeerChannel} refuses to do for the same reason. Package-private for
   * unit tests.
   */
  static boolean hasHandoffTarget(final Collection<RaftPeer> livePeers, final RaftPeerId localPeerId,
      final ClusterMonitor clusterMonitor) {
    return hasHandoffTarget(livePeers, localPeerId, clusterMonitor, null);
  }

  /**
   * {@link #hasHandoffTarget(Collection, RaftPeerId, ClusterMonitor)} restricted to the peers in {@code reachable}
   * ({@link RaftHAServer#handoffReachablePeers()}), so a peer that is down does not count as eligible and suppress the report
   * that no peer can take over (issue #8556).
   */
  static boolean hasHandoffTarget(final Collection<RaftPeer> livePeers, final RaftPeerId localPeerId,
      final ClusterMonitor clusterMonitor, final Set<String> reachable) {
    return !StepDownTargets.selectStepDownTargets(livePeers, localPeerId, clusterMonitor, reachable).isEmpty();
  }

  /**
   * Picks the peer that should receive leadership when {@code wedgedPeerId}'s replication channel is
   * unrecoverable (issue #5346), or {@code null} when none is eligible.
   * <p>
   * Delegates the eligibility rules to {@link StepDownTargets#selectStepDownTargets} (excludes this node, priority-0
   * witnesses while real voters exist, and lagging followers; ordered strongest-first) and additionally
   * excludes the wedged follower. That exclusion is the point of the escalation: the wedged peer is
   * exactly the node that cannot be reached, so promoting it would trade a dead replication channel for a
   * dead cluster. It is normally filtered as lagging already, but a follower wedged at a high water mark
   * can present a small lag, so the exclusion is explicit rather than incidental.
   * <p>
   * Package-private and static so the selection can be unit-tested without a live cluster.
   */
  static RaftPeer selectChannelEscalationTarget(final Collection<RaftPeer> livePeers, final RaftPeerId localPeerId,
      final String wedgedPeerId, final ClusterMonitor clusterMonitor) {
    return selectChannelEscalationTarget(livePeers, localPeerId, wedgedPeerId, clusterMonitor, null);
  }

  /**
   * {@link #selectChannelEscalationTarget(Collection, RaftPeerId, String, ClusterMonitor)} restricted to the peers in
   * {@code reachable} ({@link RaftHAServer#handoffReachablePeers()}, issue #8556).
   */
  static RaftPeer selectChannelEscalationTarget(final Collection<RaftPeer> livePeers, final RaftPeerId localPeerId,
      final String wedgedPeerId, final ClusterMonitor clusterMonitor, final Set<String> reachable) {
    for (final RaftPeer candidate : StepDownTargets.selectStepDownTargets(livePeers, localPeerId, clusterMonitor, reachable))
      if (!candidate.getId().toString().equals(wedgedPeerId))
        return candidate;
    return null;
  }

  /**
   * Why a leader-driven resync order must not be sent any more, or {@code null} when it still stands (issue #8490):
   * this node lost leadership, the term moved on, or the stall it was decided on is over - the replica recovered,
   * went unreachable, or its {@code matchIndex} moved. Static and fed its inputs so it is testable without Ratis.
   */
  static String staleStalledResyncOrderReason(final boolean leader, final long currentTerm, final ClusterMonitor monitor,
      final String peerId, final long stallGeneration, final StalledResyncOrder order) {
    if (!leader)
      return "this node is no longer the leader";
    if (currentTerm != order.leaderTerm())
      return "the order was decided in term " + order.leaderTerm() + " and the term is now " + currentTerm;
    if (!monitor.isStalledResyncStillWarranted(peerId, stallGeneration, order.observedMatchIndex()))
      return "the stall it was decided on is over (the replica recovered, progressed or is unreachable, or a new "
          + "stall began)";
    return null;
  }

  /**
   * Sends {@code POST /api/v1/cluster/resync/{database}} to a follower, authenticated with the
   * cluster token. When {@code https} is true the connection uses the cluster SSL context (the same
   * one used for snapshot transfer). Visible for testing. Throws on a non-2xx response.
   */
  void requestRemoteResync(final String followerAddr, final String databaseName, final String clusterToken,
      final boolean https) throws IOException {
    requestRemoteResync(followerAddr, databaseName, clusterToken, https, null);
  }

  /**
   * Same as {@link #requestRemoteResync(String, String, String, boolean)}, carrying the leader's {@code order}
   * (issue #8490) for the follower to check before it drops anything; {@code null} sends an unconditional request,
   * the same as an operator's. Throws {@link StalledResyncDeclinedException} when the follower refuses the order
   * as stale ({@code 409}).
   */
  void requestRemoteResync(final String followerAddr, final String databaseName, final String clusterToken,
      final boolean https, final StalledResyncOrder order) throws IOException {
    final String url = (https ? "https://" : "http://") + followerAddr + "/api/v1/cluster/resync/"
        + URLEncoder.encode(databaseName, StandardCharsets.UTF_8);
    // The token with the root principal, together or not at all, and a plain dial on an SSL cluster said once like every
    // other peer dial (issue #9999). Validated by AbstractServerHttpHandler before the handler runs; no operator
    // credentials go over the wire.
    final HttpURLConnection conn = PeerRequest.openConnection(url,
        ha.configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL), RaftHAServer.class,
        "ordering a stalled replica to resync database '" + databaseName + "'", clusterToken, RaftHAServer.FORWARDED_ROOT_USER);
    try {
      if (conn instanceof final HttpsURLConnection httpsConn)
        httpsConn.setSSLSocketFactory(SnapshotInstaller.buildSSLContext(ha.arcadeServer).getSocketFactory());
      conn.setRequestMethod("POST");
      conn.setConnectTimeout(10_000);
      // The resync endpoint is synchronous: it downloads the full snapshot from the leader and only
      // then returns, so the read timeout must accommodate a snapshot transfer, not just a trigger.
      conn.setReadTimeout(120_000);
      conn.setDoOutput(true);
      conn.setRequestProperty("Content-Type", "application/json");
      try (final OutputStream os = conn.getOutputStream()) {
        os.write((order != null ? order.toJSON().toString() : "{}").getBytes(StandardCharsets.UTF_8));
      }
      final int code = conn.getResponseCode();
      if (order != null && code == PostResyncDatabaseHandler.STALE_ORDER_STATUS)
        throw new StalledResyncDeclinedException(readErrorMessage(conn));
      if (code < 200 || code >= 300) {
        drainErrorStream(conn);
        throw new IOException("resync endpoint returned HTTP " + code);
      }
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Drains and discards the error response body so the underlying keep-alive socket can be reused by
   * the JVM connection pool instead of being abandoned (and leaked).
   */
  private static void drainErrorStream(final HttpURLConnection conn) {
    try (final var err = conn.getErrorStream()) {
      if (err != null)
        err.readAllBytes();
    } catch (final IOException ignored) {
      // best-effort cleanup
    }
  }

  /**
   * The {@code error} field of a JSON error response, read to at most {@code 4 KiB} and then drained so the socket
   * can be reused; the raw text when it is not JSON, or a placeholder when there is no body.
   */
  private static String readErrorMessage(final HttpURLConnection conn) {
    try (final var err = conn.getErrorStream()) {
      if (err == null)
        return "no reason given";
      final String body = new String(err.readNBytes(4096), StandardCharsets.UTF_8);
      err.readAllBytes();
      try {
        return new JSONObject(body).getString("error", body);
      } catch (final Exception notJson) {
        return body;
      }
    } catch (final IOException e) {
      return "no reason given (" + e.getMessage() + ")";
    }
  }
}
