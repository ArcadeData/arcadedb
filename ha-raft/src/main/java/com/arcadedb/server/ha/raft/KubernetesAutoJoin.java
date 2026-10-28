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
import com.arcadedb.server.ArcadeDBServer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;

/**
 * Handles Kubernetes-specific cluster auto-join on StatefulSet scale-up, and doubles as the startup
 * membership self-check (issue #5275): it runs on every Kubernetes start - including a pod restarting
 * with persisted Raft storage - probes the <b>leader's</b> live Raft configuration, and re-adds this
 * node via an atomic {@code Mode.ADD} configuration change when that configuration no longer
 * contains it (e.g. the cluster shrank while the pod was down). When the node is still a member the
 * probe is a cheap no-op.
 * <p>
 * Only the leader's answer decides (issue #10760). A {@code GroupInfo} reply carries the answering server's OWN
 * configuration, so a follower that is behind (installing a snapshot, partitioned, replaying) still lists a node removed
 * since: believing it parked the pod as a non-member that no leader ever dials, for the life of the process, because
 * {@link Outcome#ALREADY_MEMBER} ends the retry loop. A follower's answer now only points the probe at the leader it
 * names; a round in which no peer answers as the leader is {@link Outcome#NO_LEADER_ANSWERED}, which is retried.
 * <p>
 * Jitter is applied before probing to spread traffic when multiple pods start simultaneously.
 * {@code Mode.ADD} is atomic, so concurrent joins from different pods are safe even without
 * additional coordination.
 * <p>
 * <b>Security documents are seeded by the leader, not from here</b> (issue #7531). A self-join has no
 * admitting node to run the seed the other two admission paths run, and this pod's own copies of
 * {@code server-users.jsonl}, {@code server-groups.json} and {@code server-api-tokens.json} are precisely
 * the stale ones - a snapshot install carries none of them. The {@code Mode.ADD} below therefore only has to
 * commit: the leader applies the resulting configuration entry, notices the new peer in
 * {@code ArcadeStateMachine.notifyConfigurationChanged} and seeds all three documents over Raft. See
 * {@link MembershipSecuritySeeder}.
 * <p>
 * <b>Security note:</b> Peer discovery uses DNS resolution of the headless service hostname, and the
 * Raft gRPC transport does not enforce cluster-token authentication. Secure the port with mTLS
 * ({@code arcadedb.ha.tls.*}, issue #3890) and restrict access to the StatefulSet pods with a
 * NetworkPolicy. The probe client below inherits the server's transport configuration, so it speaks TLS
 * exactly when the cluster does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class KubernetesAutoJoin {

  // Jitter before probing (to spread traffic when multiple pods start simultaneously).
  // The minimum is derived from the pod ordinal (last dash-separated segment of HOSTNAME):
  // pod-0 waits [0, MAX), pod-1 waits [SLOT, MAX), pod-2 waits [2*SLOT, MAX), etc.
  // This guarantees non-overlapping base windows for StatefulSet scale-up events.
  // Falls back to AUTO_JOIN_JITTER_FALLBACK_MIN_MS when ordinal cannot be parsed.
  private static final long AUTO_JOIN_JITTER_MAX_MS          = 3_000L;
  private static final long AUTO_JOIN_JITTER_ORDINAL_SLOT_MS = 500L;
  private static final long AUTO_JOIN_JITTER_FALLBACK_MIN_MS = 500L;
  // Short timeouts for the probe client to avoid blocking for the full default gRPC timeout
  private static final int  AUTO_JOIN_RPC_TIMEOUT_MIN_SECS   = 3;
  private static final int  AUTO_JOIN_RPC_TIMEOUT_MAX_SECS   = 5;
  // Retry backoff for tryAutoJoinWithRetry (issue #5268): the one-shot probe reliably lost the race
  // against the peers' allowlist DNS refresh on pod recreation, stranding the node forever.
  static final long RETRY_BACKOFF_INITIAL_MS = 2_000L;
  static final long RETRY_BACKOFF_MAX_MS     = 60_000L;
  // Bounded retry policy for the probe's temp RaftClient (issue #5973): RaftClient.Builder defaults to
  // RetryPolicies.retryForeverNoSleep() when none is set. A setConfiguration ADD racing an in-flight
  // reconfiguration on the peer fails fast with ReconfigurationInProgressException (a synchronous
  // rejection, not an RPC that can time out), so an unbounded no-sleep policy spins inside this single
  // blocking admin().setConfiguration() call as fast as the JVM can throw/catch/log, indefinitely - the
  // RPC timeouts set in buildProbeProperties() never get a chance to apply. Observed in CI hammering a
  // permanently-stuck reconfiguration for 5.5 hours straight (a 41GB log of the identical exception)
  // until the job's 6-hour wall-clock cap force-killed it. Bounding it here makes a stuck reconfiguration
  // surface as a probe failure within a couple of seconds; tryAutoJoinWithRetry's own exponential
  // backoff (up to RETRY_BACKOFF_MAX_MS) already covers retrying the probe over the longer term.
  static final RetryPolicy PROBE_RETRY_POLICY =
      RetryPolicies.retryUpToMaximumCountWithFixedSleep(5, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS));

  /** Result of a single auto-join probe round. */
  public enum Outcome {
    /** This node was added to the cluster's committed configuration. */
    JOINED,
    /** The live configuration already contains this node; nothing to do. */
    ALREADY_MEMBER,
    /** No reachable peer reported an existing cluster (cold start, or every probe failed/was rejected). */
    NO_CLUSTER_FOUND,
    /**
     * Peers answered, but none as the leader (issue #10760): a cold start still electing, or followers whose
     * configuration may predate this node's removal or addition. Their answers decide nothing, so the round is retried
     * until a leader answers.
     */
    NO_LEADER_ANSWERED,
    /** A peer accepted the probe but rejected the configuration change (e.g. no settled leader yet). */
    ADD_REJECTED,
    /** The probing thread was interrupted (shutdown). */
    INTERRUPTED
  }

  /** Pluggable sleeper so tests can drive the jitter/backoff deterministically. */
  @FunctionalInterface
  interface Sleeper {
    void sleep(long ms) throws InterruptedException;
  }

  /**
   * What one peer answered to the {@code GroupInfo} probe (issue #10760): whether it answered as the LEADER, the leader
   * it follows when it did not ({@code null} when it knows none), and the peers of ITS OWN configuration. Ratis builds
   * the reply's group from the answering server's {@code getRaftConf()}, so only a leader's members are the cluster's.
   */
  record GroupAnswer(boolean leader, RaftPeerId leaderId, List<RaftPeer> members) {
  }

  /** The transport of a probe round: a seam so the decision can be driven with scripted per-peer answers. */
  interface ClusterProbe {
    /**
     * Asks {@code peer} about the group.
     *
     * @return its answer, or {@code null} when it answered without success
     * @throws Exception when the peer cannot be reached
     */
    GroupAnswer ask(RaftPeer peer) throws Exception;

    /**
     * Submits the atomic {@code Mode.ADD} of {@code localPeer} to {@code leader}.
     *
     * @return {@code null} when the change committed, otherwise the reason it was rejected
     * @throws Exception when the leader cannot be reached
     */
    String addThrough(RaftPeer leader, RaftPeer localPeer) throws Exception;
  }

  /**
   * The configuration of the peer that answered as the LEADER, the only input membership is decided from (issue #10760).
   * Only {@link #of} builds one, and it refuses any other role, so {@link Outcome#ALREADY_MEMBER} and the {@code Mode.ADD}
   * cannot come from a follower whose configuration predates this node's removal (it would say "member" and park the pod
   * as a non-member nobody dials) or its addition (it would say "not a member" and re-add a member).
   */
  static final class LeaderConfiguration {
    private final RaftPeer       leader;
    private final List<RaftPeer> members;

    private LeaderConfiguration(final RaftPeer leader, final List<RaftPeer> members) {
      this.leader = leader;
      this.members = members;
    }

    /** The configuration {@code answer} carries when it is a leader's, otherwise {@code null}. */
    static LeaderConfiguration of(final RaftPeer respondent, final GroupAnswer answer) {
      return answer != null && answer.leader() ? new LeaderConfiguration(respondent, List.copyOf(answer.members())) : null;
    }

    RaftPeer leader() {
      return leader;
    }

    boolean contains(final RaftPeerId peerId) {
      for (final RaftPeer member : members)
        if (member.getId().equals(peerId))
          return true;
      return false;
    }
  }

  private final ArcadeDBServer server;
  private final RaftGroup      raftGroup;
  private final RaftPeerId     localPeerId;
  private final RaftProperties raftProperties;
  // Ratis transport parameters of the local server. Ratis carries the gRPC TLS configuration on
  // Parameters, NOT on RaftProperties, so the probe client below has to be handed these to reach a peer
  // whose Raft port requires TLS (issue #3890). Never null.
  private final Parameters     raftParameters;
  private       Sleeper        sleeper = Thread::sleep;
  private       ClusterProbe   probe   = new RaftClusterProbe();

  /**
   * Builds an auto-join with no transport parameters, so its probe client dials in plaintext. Only
   * correct for a cluster with {@code arcadedb.ha.tls.enabled=false}; production wiring goes through
   * {@link #KubernetesAutoJoin(ArcadeDBServer, RaftGroup, RaftPeerId, RaftProperties, Parameters)} with the
   * same {@link Parameters} the local Ratis server was built from.
   */
  public KubernetesAutoJoin(final ArcadeDBServer server, final RaftGroup raftGroup,
      final RaftPeerId localPeerId, final RaftProperties raftProperties) {
    this(server, raftGroup, localPeerId, raftProperties, null);
  }

  public KubernetesAutoJoin(final ArcadeDBServer server, final RaftGroup raftGroup,
      final RaftPeerId localPeerId, final RaftProperties raftProperties, final Parameters raftParameters) {
    this.server = server;
    this.raftGroup = raftGroup;
    this.localPeerId = localPeerId;
    this.raftProperties = raftProperties;
    this.raftParameters = raftParameters != null ? raftParameters : new Parameters();
  }

  /** Package-private test hook: replaces the real sleeps so retry tests run instantly. */
  void setSleeperForTesting(final Sleeper sleeper) {
    this.sleeper = sleeper;
  }

  /** Package-private test hook: replaces the Raft transport of the probe with scripted per-peer answers. */
  void setProbeForTesting(final ClusterProbe probe) {
    this.probe = probe;
  }

  /**
   * Runs {@link #tryAutoJoin()} with exponential backoff until the node has joined (or confirmed
   * membership), the thread is interrupted, or {@code keepRetrying} turns false (issue #5268). The
   * one-shot probe reliably lost the race against the peers' allowlist DNS refresh on pod recreation
   * and then parked forever with the node NOT_READY and nothing retrying on either side; the retry
   * makes the join path self-sufficient. The caller supplies the continuation condition - typically
   * "not shutting down and no leader is known yet": once a leader is visible this node is either a
   * functioning member or a cold-start election completed, and retrying is pointless.
   */
  public Outcome tryAutoJoinWithRetry(final BooleanSupplier keepRetrying) {
    long backoffMs = RETRY_BACKOFF_INITIAL_MS;
    for (int attempt = 1; ; attempt++) {
      final Outcome outcome = tryAutoJoin();
      if (outcome == Outcome.JOINED || outcome == Outcome.ALREADY_MEMBER || outcome == Outcome.INTERRUPTED)
        return outcome;
      if (!keepRetrying.getAsBoolean())
        return outcome;
      HALog.log(this, HALog.BASIC, "K8s auto-join: attempt %d did not join (%s); retrying in %dms",
          attempt, outcome, backoffMs);
      try {
        sleeper.sleep(backoffMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return Outcome.INTERRUPTED;
      }
      backoffMs = Math.min(backoffMs * 2, RETRY_BACKOFF_MAX_MS);
    }
  }

  /**
   * Runs one probe round: attempts to join an existing Raft cluster by contacting a peer and adding
   * this server. If no existing cluster is found (fresh cold-start deployment), this is a no-op -
   * the Raft server already has the full peer list from HA_SERVER_LIST, so normal Raft leader
   * election proceeds without risk of split-brain.
   *
   * @return the probe outcome; production callers go through {@link #tryAutoJoinWithRetry} so a
   *         transiently-lost race (issue #5268) is retried instead of stranding the node
   */
  public Outcome tryAutoJoin() {
    final long jitterMin = computeJitterMinMs();
    final long jitterMax = Math.max(jitterMin + 100, AUTO_JOIN_JITTER_MAX_MS);
    final long jitterMs = jitterMin < jitterMax ? ThreadLocalRandom.current().nextLong(jitterMin, jitterMax) : jitterMin;
    HALog.log(this, HALog.BASIC, "K8s auto-join: waiting %dms jitter before probing (min=%d, max=%d)...", jitterMs, jitterMin, jitterMax);
    try {
      sleeper.sleep(jitterMs);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return Outcome.INTERRUPTED;
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: attempting to join existing cluster...");

    // Per-peer failure reasons, reported in the final log line: the join failure used to be
    // completely silent on the affected node (the only evidence was the rejection log on the OTHER
    // pods), which is the observability gap called out in issue #5268.
    final Map<String, String> probeFailures = new LinkedHashMap<>();
    // The peers that answered, and how: kept for the report when none of them is the leader
    final Map<String, String> nonLeaderAnswers = new LinkedHashMap<>();

    final LeaderConfiguration leader = findLeader(probeFailures, nonLeaderAnswers);
    if (leader != null)
      return joinFrom(leader);

    if (!nonLeaderAnswers.isEmpty()) {
      // Not "no cluster": peers answered, but a follower's configuration is its own and may predate this node's removal
      // (it would list this node) or its addition (it would not). Retried until a leader answers (issue #10760).
      LogManager.instance().log(this, Level.INFO,
          "K8s auto-join: no peer answered as the Raft leader (answers: %s%s); membership is decided only from the "
              + "leader's configuration, so the probe is retried until a leader answers", nonLeaderAnswers,
          probeFailures.isEmpty() ? "" : "; unreachable: " + probeFailures);
      return Outcome.NO_LEADER_ANSWERED;
    }

    if (probeFailures.isEmpty())
      LogManager.instance().log(this, Level.INFO,
          """
          K8s auto-join: no existing cluster found. This node will participate in \
          Raft leader election with the configured peer group once peers are reachable""");
    else
      // At least one probe failed outright (unreachable peer, rejected connection, timeout): surface
      // the per-peer reasons at WARNING so the join failure is diagnosable on THIS node instead of
      // only via the rejection logs on the other pods (issue #5268).
      LogManager.instance().log(this, Level.WARNING,
          "K8s auto-join: could not join an existing cluster; probe failures per peer: %s. "
              + "This node will participate in Raft leader election with the configured peer group once peers are reachable",
          probeFailures);
    return Outcome.NO_CLUSTER_FOUND;
  }

  /**
   * Probes the declared peers until one answers as the LEADER (issue #10760). A follower that names its leader sends the
   * probe there next, even when that leader is not in this node's declared list (a member added since), using the
   * address the follower's own configuration gives it. Each peer is asked once per round.
   *
   * @return the leader's configuration, or {@code null} when no peer answered as the leader
   */
  private LeaderConfiguration findLeader(final Map<String, String> probeFailures,
      final Map<String, String> nonLeaderAnswers) {
    final Deque<RaftPeer> toAsk = new ArrayDeque<>();
    for (final RaftPeer peer : raftGroup.getPeers())
      if (!peer.getId().equals(localPeerId))
        toAsk.add(peer);

    final Set<RaftPeerId> asked = new HashSet<>();
    while (!toAsk.isEmpty()) {
      final RaftPeer peer = toAsk.poll();
      if (!asked.add(peer.getId()))
        continue;

      final GroupAnswer answer;
      try {
        answer = probe.ask(peer);
      } catch (final Exception e) {
        probeFailures.put(peer.getId().toString(), e.getMessage());
        HALog.log(this, HALog.DETAILED, "K8s auto-join: peer %s not reachable (%s), trying next...",
            peer.getId(), e.getMessage());
        continue;
      }
      if (answer == null)
        continue;

      final LeaderConfiguration leader = LeaderConfiguration.of(peer, answer);
      if (leader != null)
        return leader;

      nonLeaderAnswers.put(peer.getId().toString(),
          answer.leaderId() != null ? "follows " + answer.leaderId() : "knows no leader");
      // The leader this follower names is asked next, at the address the follower's configuration holds for it
      if (answer.leaderId() != null && !answer.leaderId().equals(localPeerId) && !asked.contains(answer.leaderId()))
        for (final RaftPeer member : answer.members())
          if (member.getId().equals(answer.leaderId())) {
            toAsk.addFirst(member);
            break;
          }
    }
    return null;
  }

  /**
   * Decides from the leader's configuration, and only from it (issue #10760): this node is a member, or it is added
   * through that leader with {@code Mode.ADD}.
   */
  private Outcome joinFrom(final LeaderConfiguration leader) {
    if (leader.contains(localPeerId)) {
      HALog.log(this, HALog.BASIC, "K8s auto-join: already a member of the cluster (leader %s)", leader.leader().getId());
      return Outcome.ALREADY_MEMBER;
    }

    // Find our peer definition in the configured group
    RaftPeer localPeer = null;
    for (final RaftPeer p : raftGroup.getPeers())
      if (p.getId().equals(localPeerId)) {
        localPeer = p;
        break;
      }
    if (localPeer == null) {
      LogManager.instance().log(this, Level.WARNING,
          "K8s auto-join: this node (%s) is not in its own configured peer group, so it has no address to join with",
          localPeerId);
      return Outcome.NO_CLUSTER_FOUND;
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: adding self (%s) to existing cluster via leader %s", localPeerId,
        leader.leader().getId());
    final String rejection;
    try {
      rejection = probe.addThrough(leader.leader(), localPeer);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "K8s auto-join: add peer through leader %s failed: %s",
          leader.leader().getId(), e.getMessage());
      return Outcome.ADD_REJECTED;
    }
    if (rejection != null) {
      LogManager.instance().log(this, Level.WARNING, "K8s auto-join: add peer rejected: %s", rejection);
      return Outcome.ADD_REJECTED;
    }
    HALog.log(this, HALog.BASIC, "K8s auto-join: successfully joined cluster via atomic add");
    return Outcome.JOINED;
  }

  /** The production {@link ClusterProbe}: a short-lived Raft client per peer, on this server's transport configuration. */
  private final class RaftClusterProbe implements ClusterProbe {
    @Override
    public GroupAnswer ask(final RaftPeer peer) throws Exception {
      try (final RaftClient tempClient = clientFor(peer)) {
        final GroupInfoReply groupInfo = tempClient.getGroupManagementApi(peer.getId()).info(raftGroup.getGroupId());
        if (groupInfo == null || !groupInfo.isSuccess())
          return null;

        // Membership is read from the reply's RaftGroup, which is always populated: GroupInfoReply.getConf() is never
        // filled over the gRPC transport in Ratis 3.2.2, so a conf-based check made every probe against a healthy cluster
        // fall through to "no existing cluster found" (issue #5275). That group is built from the ANSWERING server's own
        // configuration, so the role travelling beside it in the same reply says whether it is the cluster's (#10760).
        final RoleInfoProto role = groupInfo.getRoleInfoProto();
        final boolean leader = role != null && role.getRole() == RaftPeerRole.LEADER;
        RaftPeerId leaderId = null;
        if (role != null && role.hasFollowerInfo() && role.getFollowerInfo().hasLeaderInfo()) {
          final ByteString id = role.getFollowerInfo().getLeaderInfo().getId().getId();
          if (!id.isEmpty())
            leaderId = RaftPeerId.valueOf(id);
        }
        return new GroupAnswer(leader, leaderId, List.copyOf(groupInfo.getGroup().getPeers()));
      }
    }

    @Override
    public String addThrough(final RaftPeer leader, final RaftPeer localPeer) throws Exception {
      try (final RaftClient tempClient = clientFor(leader)) {
        // Mode.ADD atomically appends one peer to the current configuration. It is deliberately not the guarded
        // compare-and-set RaftClusterManager.addPeer issues (issue #7802): this pod adds ITSELF, under the id derived from
        // its own address. That does not make it airtight: if an operator already added a different id at this address,
        // Mode.ADD commits it (no predicate), and the peers-share-address cluster alert is what reports it afterwards.
        // Unlike setConfiguration() (last-write-wins), concurrent joins from different pods are safe.
        final SetConfigurationRequest.Arguments addArgs = SetConfigurationRequest.Arguments.newBuilder()
            .setServersInNewConf(List.of(localPeer))
            .setMode(SetConfigurationRequest.Mode.ADD)
            .build();
        final RaftClientReply joinReply = tempClient.admin().setConfiguration(addArgs);
        if (joinReply.isSuccess())
          return null;
        return joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown";
      }
    }

    private RaftClient clientFor(final RaftPeer peer) {
      return RaftClient.newBuilder()
          .setRaftGroup(RaftGroup.valueOf(raftGroup.getGroupId(), peer))
          .setProperties(buildProbeProperties())
          .setParameters(raftParameters)
          .setRetryPolicy(PROBE_RETRY_POLICY)
          .build();
    }
  }

  /**
   * Derives the jitter minimum from the pod ordinal embedded in the HOSTNAME environment variable.
   * StatefulSet pods are named {@code <name>-<ordinal>} (e.g. {@code arcadedb-2}), so the ordinal
   * is the integer after the last hyphen. Each ordinal is assigned its own time slot
   * ({@code ordinal * AUTO_JOIN_JITTER_ORDINAL_SLOT_MS}), guaranteeing non-overlapping base windows
   * when a StatefulSet scales up several pods simultaneously.
   * Falls back to {@code AUTO_JOIN_JITTER_FALLBACK_MIN_MS} when the hostname is absent or
   * does not end with a parseable integer.
   */
  private static long computeJitterMinMs() {
    return computeJitterMinMs(System.getenv("HOSTNAME"));
  }

  /**
   * Package-private for unit testing. The public no-arg form reads HOSTNAME from the process
   * environment, which cannot be mutated from Java; tests drive the logic directly through this
   * variant.
   */
  static long computeJitterMinMs(final String hostname) {
    if (hostname != null) {
      final int dash = hostname.lastIndexOf('-');
      if (dash >= 0 && dash < hostname.length() - 1) {
        try {
          final int ordinal = Integer.parseInt(hostname.substring(dash + 1));
          if (ordinal >= 0)
            return Math.min((long) ordinal * AUTO_JOIN_JITTER_ORDINAL_SLOT_MS, AUTO_JOIN_JITTER_MAX_MS - 100L);
        } catch (final NumberFormatException ignored) {
        }
      }
    }
    return AUTO_JOIN_JITTER_FALLBACK_MIN_MS;
  }

  // Package-private for unit testing of the TLS / flow-control inheritance contract.
  RaftProperties buildProbePropertiesForTest() {
    return buildProbeProperties();
  }

  /** Package-private for unit testing that the probe client inherits the server's transport configuration. */
  Parameters probeParametersForTest() {
    return raftParameters;
  }

  private RaftProperties buildProbeProperties() {
    // Clone the main server's RaftProperties so gRPC flow-control, message sizing, authentication and
    // any other operator-set knobs carry over into the short-lived probe client; only the probe-specific
    // timeouts are then overridden. Note that the gRPC TLS configuration is NOT among them - Ratis keeps
    // it on Parameters, which is why raftParameters is threaded in separately and set on the client.
    final RaftProperties props = new RaftProperties(raftProperties);
    props.set("raft.server.rpc.type", "GRPC");
    RaftServerConfigKeys.Rpc.setTimeoutMin(props,
        TimeDuration.valueOf(AUTO_JOIN_RPC_TIMEOUT_MIN_SECS, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(props,
        TimeDuration.valueOf(AUTO_JOIN_RPC_TIMEOUT_MAX_SECS, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(props,
        TimeDuration.valueOf(AUTO_JOIN_RPC_TIMEOUT_MAX_SECS, TimeUnit.SECONDS));
    return props;
  }
}
