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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Handles Kubernetes-specific cluster auto-join on StatefulSet scale-up.
 * When a new pod starts without existing Raft storage, it probes running peers and adds itself
 * to the existing cluster via an atomic {@code Mode.ADD} configuration change.
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

  private final ArcadeDBServer server;
  private final RaftGroup      raftGroup;
  private final RaftPeerId     localPeerId;
  private final RaftProperties raftProperties;

  public KubernetesAutoJoin(final ArcadeDBServer server, final RaftGroup raftGroup,
      final RaftPeerId localPeerId, final RaftProperties raftProperties) {
    this.server = server;
    this.raftGroup = raftGroup;
    this.localPeerId = localPeerId;
    this.raftProperties = raftProperties;
  }

  /**
   * Attempts to join an existing Raft cluster by contacting a peer and adding this server.
   * If no existing cluster is found (fresh cold-start deployment), this is a no-op - the Raft
   * server already has the full peer list from HA_SERVER_LIST, so normal Raft leader election
   * proceeds without risk of split-brain.
   */
  public void tryAutoJoin() {
    final long jitterMin = computeJitterMinMs();
    final long jitterMax = Math.max(jitterMin + 100, AUTO_JOIN_JITTER_MAX_MS);
    final long jitterMs = jitterMin < jitterMax ? ThreadLocalRandom.current().nextLong(jitterMin, jitterMax) : jitterMin;
    HALog.log(this, HALog.BASIC, "K8s auto-join: waiting %dms jitter before probing (min=%d, max=%d)...", jitterMs, jitterMin, jitterMax);
    try {
      Thread.sleep(jitterMs);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: attempting to join existing cluster...");

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

          final var confOpt = groupInfo.getConf();
          if (confOpt.isEmpty())
            continue;

          final var conf = confOpt.get();
          boolean alreadyMember = false;
          for (final var p : conf.getPeersList())
            if (p.getId().toStringUtf8().equals(localPeerId.toString())) {
              alreadyMember = true;
              break;
            }

          if (alreadyMember) {
            HALog.log(this, HALog.BASIC, "K8s auto-join: already a member of the cluster");
            return;
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
          if (!joinReply.isSuccess())
            LogManager.instance().log(this, Level.WARNING, "K8s auto-join: add peer rejected: %s",
                joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown");
          else
            HALog.log(this, HALog.BASIC, "K8s auto-join: successfully joined cluster via atomic add");

          return;
        }
      } catch (final Exception e) {
        HALog.log(this, HALog.DETAILED, "K8s auto-join: peer %s not reachable (%s), trying next...",
            peer.getId(), e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.INFO,
        "K8s auto-join: no existing cluster found. This node will participate in "
            + "Raft leader election with the configured peer group once peers are reachable");
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
