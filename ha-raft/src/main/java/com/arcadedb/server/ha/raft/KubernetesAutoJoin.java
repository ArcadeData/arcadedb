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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Handles Kubernetes-mode cluster auto-join on startup.
 * <p>
 * When a node starts in a Kubernetes environment without existing Raft storage, it probes
 * all configured peers to discover whether a cluster already exists. If found, the node
 * adds itself to the existing membership. If no peer is reachable, the node starts a new
 * cluster. A random jitter delay (based on the local peer ID hash) avoids thundering-herd
 * join races when multiple pods start simultaneously.
 * <p>
 * <b>Security note:</b> When gRPC is bound to {@code 0.0.0.0}, any pod in the Kubernetes
 * cluster can connect to the Raft port. Authentication relies on Kubernetes NetworkPolicy.
 * Operators should restrict access to the Raft port in production.
 */
class KubernetesAutoJoin {

  private final RaftHAServer haServer;

  KubernetesAutoJoin(final RaftHAServer haServer) {
    this.haServer = haServer;
  }

  /**
   * Attempts to join an existing cluster by probing configured peers. If a peer with an
   * active cluster is found, this node adds itself to the membership. If none are reachable,
   * the node starts as a new cluster.
   */
  void tryAutoJoinCluster() {
    final RaftPeerId localPeerId = haServer.getLocalPeerId();
    final RaftGroup raftGroup = haServer.getRaftGroup();

    final long jitterMs = Math.abs(localPeerId.hashCode() % 3000L);
    if (jitterMs > 0) {
      HALog.log(this, HALog.BASIC, "K8s auto-join: waiting %dms jitter before probing...", jitterMs);
      try {
        Thread.sleep(jitterMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    HALog.log(this, HALog.BASIC, "K8s auto-join: attempting to join existing cluster...");

    for (final RaftPeer peer : raftGroup.getPeers()) {
      if (peer.getId().equals(localPeerId))
        continue;

      try {
        final RaftProperties tempProps = new RaftProperties();
        RaftServerConfigKeys.Rpc.setTimeoutMin(tempProps, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setRequestTimeout(tempProps, TimeDuration.valueOf(5, TimeUnit.SECONDS));

        final RaftGroup targetGroup = RaftGroup.valueOf(raftGroup.getGroupId(), peer);
        try (final RaftClient tempClient = RaftClient.newBuilder()
            .setRaftGroup(targetGroup)
            .setProperties(tempProps)
            .build()) {

          final var groupInfo = tempClient.getGroupManagementApi(peer.getId())
              .info(raftGroup.getGroupId());

          if (groupInfo != null && groupInfo.isSuccess()) {
            final var confOpt = groupInfo.getConf();
            if (confOpt.isPresent()) {
              final var conf = confOpt.get();
              boolean alreadyMember = false;
              for (final var p : conf.getPeersList())
                if (p.getId().toStringUtf8().equals(localPeerId.toString())) {
                  alreadyMember = true;
                  break;
                }

              if (!alreadyMember) {
                HALog.log(this, HALog.BASIC,
                    "K8s auto-join: adding self (%s) to existing cluster via peer %s",
                    localPeerId, peer.getId());

                RaftPeer localPeer = null;
                for (final RaftPeer p : raftGroup.getPeers())
                  if (p.getId().equals(localPeerId)) {
                    localPeer = p;
                    break;
                  }

                if (localPeer != null) {
                  final List<RaftPeer> newPeers = new ArrayList<>();
                  for (final var existingPeer : conf.getPeersList()) {
                    final String existingId = existingPeer.getId().toStringUtf8();
                    for (final RaftPeer p : raftGroup.getPeers())
                      if (p.getId().toString().equals(existingId)) {
                        newPeers.add(p);
                        break;
                      }
                  }
                  newPeers.add(localPeer);

                  final RaftClientReply joinReply = tempClient.admin().setConfiguration(newPeers);
                  if (!joinReply.isSuccess())
                    LogManager.instance().log(this, Level.WARNING,
                        "K8s auto-join: setConfiguration rejected: %s",
                        joinReply.getException() != null ? joinReply.getException().getMessage() : "unknown");
                  else
                    HALog.log(this, HALog.BASIC,
                        "K8s auto-join: successfully joined cluster with %d peers", newPeers.size());
                }
              } else {
                HALog.log(this, HALog.BASIC, "K8s auto-join: already a member of the cluster");
              }
            }
            return;
          }
        }
      } catch (final Exception e) {
        HALog.log(this, HALog.DETAILED,
            "K8s auto-join: peer %s not reachable (%s), trying next...",
            peer.getId(), e.getMessage());
      }
    }

    LogManager.instance().log(this, Level.WARNING,
        "K8s auto-join: no existing cluster found, starting as new cluster. "
            + "If other nodes exist but are unreachable, this may create a split-brain. Verify network connectivity");
  }
}
