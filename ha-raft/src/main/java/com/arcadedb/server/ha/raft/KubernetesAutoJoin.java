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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;

/**
 * Handles Kubernetes-specific cluster auto-join on StatefulSet scale-up, and doubles as the startup
 * membership self-check (issue #5275): it runs on every Kubernetes start - including a pod restarting
 * with persisted Raft storage - probes the peers' <b>live</b> Raft configuration, and re-adds this
 * node via an atomic {@code Mode.ADD} configuration change when the committed configuration no longer
 * contains it (e.g. the cluster shrank while the pod was down). When the node is still a member the
 * probe is a cheap no-op.
 * <p>
 * Jitter is applied before probing to spread traffic when multiple pods start simultaneously.
 * {@code Mode.ADD} is atomic, so concurrent joins from different pods are safe even without
 * additional coordination.
 * <p>
 * <b>Security note:</b> Peer discovery uses DNS resolution of the headless service hostname.
 * The Raft gRPC transport does not enforce cluster-token authentication. In production Kubernetes
 * deployments, restrict gRPC port access to only ArcadeDB StatefulSet pods via a NetworkPolicy.
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

  /** Result of a single auto-join probe round. */
  public enum Outcome {
    /** This node was added to the cluster's committed configuration. */
    JOINED,
    /** The live configuration already contains this node; nothing to do. */
    ALREADY_MEMBER,
    /** No reachable peer reported an existing cluster (cold start, or every probe failed/was rejected). */
    NO_CLUSTER_FOUND,
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

  private final ArcadeDBServer server;
  private final RaftGroup      raftGroup;
  private final RaftPeerId     localPeerId;
  private final RaftProperties raftProperties;
  private       Sleeper        sleeper = Thread::sleep;

  public KubernetesAutoJoin(final ArcadeDBServer server, final RaftGroup raftGroup,
      final RaftPeerId localPeerId, final RaftProperties raftProperties) {
    this.server = server;
    this.raftGroup = raftGroup;
    this.localPeerId = localPeerId;
    this.raftProperties = raftProperties;
  }

  /** Package-private test hook: replaces the real sleeps so retry tests run instantly. */
  void setSleeperForTesting(final Sleeper sleeper) {
    this.sleeper = sleeper;
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

    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (peer.getId().equals(localPeerId))
        continue;

      try {
        final RaftProperties tempProps = buildProbeProperties();
        final RaftGroup targetGroup = RaftGroup.valueOf(raftGroup.getGroupId(), peer);

        try (final RaftClient tempClient = RaftClient.newBuilder()
            .setRaftGroup(targetGroup)
            .setProperties(tempProps)
            .build()) {

          final var groupInfo = tempClient.getGroupManagementApi(peer.getId()).info(raftGroup.getGroupId());
          if (groupInfo == null || !groupInfo.isSuccess())
            continue;

          // Read membership from the reply's RaftGroup, which reflects the peer's LIVE committed
          // configuration and is always populated. GroupInfoReply.getConf() is never filled over the
          // gRPC transport in Ratis 3.2.2, so the previous conf-based check made every probe against a
          // healthy cluster fall through to "no existing cluster found" - the auto-join (and the
          // issue-#5275 membership self-check that now reuses it) silently never worked.
          boolean alreadyMember = false;
          for (final RaftPeer p : groupInfo.getGroup().getPeers())
            if (p.getId().equals(localPeerId)) {
              alreadyMember = true;
              break;
            }

          if (alreadyMember) {
            HALog.log(this, HALog.BASIC, "K8s auto-join: already a member of the cluster");
            return Outcome.ALREADY_MEMBER;
          }

          // Find our peer definition in the configured group
          RaftPeer localPeer = null;
          for (final RaftPeer p : raftGroup.getPeers())
            if (p.getId().equals(localPeerId)) {
              localPeer = p;
              break;
            }

          if (localPeer == null)
            continue;

          HALog.log(this, HALog.BASIC, "K8s auto-join: adding self (%s) to existing cluster via peer %s",
              localPeerId, peer.getId());

          // Mode.ADD atomically appends one peer to the current configuration.
          // Unlike setConfiguration() (last-write-wins), concurrent joins from different pods are safe.
          final SetConfigurationRequest.Arguments addArgs = SetConfigurationRequest.Arguments.newBuilder()
              .setServersInNewConf(List.of(localPeer))
              .setMode(SetConfigurationRequest.Mode.ADD)
              .build();

          final RaftClientReply joinReply = tempClient.admin().setConfiguration(addArgs);
          if (!joinReply.isSuccess()) {
            LogManager.instance().log(this, Level.WARNING, "K8s auto-join: add peer rejected: %s",
                joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown");
            return Outcome.ADD_REJECTED;
          }
          HALog.log(this, HALog.BASIC, "K8s auto-join: successfully joined cluster via atomic add");
          return Outcome.JOINED;
        }
      } catch (final Exception e) {
        probeFailures.put(peer.getId().toString(), e.getMessage());
        HALog.log(this, HALog.DETAILED, "K8s auto-join: peer %s not reachable (%s), trying next...",
            peer.getId(), e.getMessage());
      }
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

  private RaftProperties buildProbeProperties() {
    // Clone the main server's RaftProperties so TLS, gRPC flow-control, authentication and any
    // other operator-set security knobs carry over into the short-lived probe client. Starting
    // from a bare RaftProperties() would silently build a plaintext client that cannot talk to
    // a TLS-only peer (handshake failure) and - more dangerously - could downgrade a deployment
    // to plaintext if Ratis ever allowed an unencrypted fallback. Only the probe-specific
    // timeouts are then overridden.
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
