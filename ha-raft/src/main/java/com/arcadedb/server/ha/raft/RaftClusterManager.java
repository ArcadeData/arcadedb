/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;

/**
 * Handles dynamic cluster membership: adding/removing peers, leadership transfer,
 * step-down, and graceful cluster leave.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftClusterManager {

  // Leadership transfer timeout (ms). Generous to allow log catch-up on the target peer
  // before it can accept the leadership role.
  private static final long LEADERSHIP_TRANSFER_TIMEOUT_MS = 10_000L;

  // After requesting leadership transfer, how long to wait for the leader change notification
  // before proceeding with shutdown. Short because the transfer itself has its own timeout.
  private static final long LEADERSHIP_CHANGE_WAIT_MS = 5_000L;

  private final RaftHAServer           haServer;
  private final RaftPeerAddressResolver addressResolver;
  private final ClusterMonitor         clusterMonitor;
  private final RaftProperties         raftProperties;
  private final Object                 leaderChangeNotifier = new Object();

  RaftClusterManager(final RaftHAServer haServer, final RaftPeerAddressResolver addressResolver,
                     final ClusterMonitor clusterMonitor, final RaftProperties raftProperties) {
    this.haServer = haServer;
    this.addressResolver = addressResolver;
    this.clusterMonitor = clusterMonitor;
    this.raftProperties = raftProperties;
  }

  /**
   * Wakes up any thread blocked in {@link #leaveCluster()} waiting for leadership transfer.
   * Called by {@link RaftHAServer#notifyLeaderChanged()} when a leader change is detected.
   */
  void notifyLeaderChangeForLeave() {
    synchronized (leaderChangeNotifier) {
      leaderChangeNotifier.notifyAll();
    }
  }

  /**
   * Gracefully removes this server from the Raft cluster. If this server is the leader,
   * transfers leadership to another peer first. Then contacts the cluster to remove this peer
   * from the configuration.
   * <p>
   * This is best-effort: errors are logged but don't prevent shutdown.
   */
  void leaveCluster() {
    if (haServer.getRaftServer() == null || haServer.getRaftClient() == null)
      return;

    try {
      final Collection<RaftPeer> livePeers = haServer.getLivePeers();
      if (livePeers.size() <= 1) {
        HALog.log(this, HALog.BASIC, "Single-node cluster, skipping leave");
        return;
      }

      // If we're the leader, transfer leadership first
      if (haServer.isLeader()) {
        for (final RaftPeer peer : livePeers) {
          if (!peer.getId().equals(haServer.getLocalPeerId())) {
            HALog.log(this, HALog.BASIC, "Leaving cluster: transferring leadership to %s before removal", peer.getId());
            try {
              transferLeadership(peer.getId().toString(), LEADERSHIP_TRANSFER_TIMEOUT_MS);
              // Wait for leadership change notification instead of polling
              final long deadline = System.currentTimeMillis() + LEADERSHIP_CHANGE_WAIT_MS;
              synchronized (leaderChangeNotifier) {
                while (haServer.isLeader()) {
                  final long remaining = deadline - System.currentTimeMillis();
                  if (remaining <= 0)
                    break;
                  leaderChangeNotifier.wait(remaining);
                }
              }
            } catch (final Exception e) {
              HALog.log(this, HALog.BASIC, "Leadership transfer failed (%s), proceeding with removal", e.getMessage());
            }
            break;
          }
        }
      }

      // Remove self from the cluster configuration
      final RaftPeerId localPeerId = haServer.getLocalPeerId();
      HALog.log(this, HALog.BASIC, "Leaving cluster: removing self (%s) from Raft group", localPeerId);
      removePeer(localPeerId.toString());
      HALog.log(this, HALog.BASIC, "Successfully left the Raft cluster");

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Failed to leave cluster gracefully: %s", e.getMessage());
    }
  }

  /**
   * Adds a new peer to the Raft cluster. Must be called on any server (Ratis routes to leader).
   * The new peer must already be running with the same cluster name and group ID.
   *
   * @param peerId      the new peer's ID (typically host_port)
   * @param address     the new peer's Raft RPC address (host:port)
   * @param httpAddress optional HTTP address (host:port). If null or empty, derived from the Raft
   *                    address using the local port offset (which may be incorrect if the peer uses
   *                    a non-standard port layout)
   */
  void addPeer(final String peerId, final String address, final String httpAddress) {
    final RaftPeer newPeer = RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build();

    try {
      final SetConfigurationRequest.Arguments addArgs = SetConfigurationRequest.Arguments.newBuilder()
          .setServersInNewConf(List.of(newPeer))
          .setMode(SetConfigurationRequest.Mode.ADD)
          .build();
      final RaftClient client = haServer.getRaftClient();
      if (client == null)
        throw new ConfigurationException("Failed to add peer " + peerId + ": RaftClient not available");
      final RaftClientReply reply = client.admin().setConfiguration(addArgs);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to add peer " + peerId + ": " + reply.getException());

      addressResolver.registerPeerHttpAddress(peerId, address, httpAddress);

      LogManager.instance().log(this, Level.INFO, "Peer %s added to Raft cluster", peerId);
    } catch (final IOException e) {
      throw new ConfigurationException("Failed to add peer " + peerId, e);
    }
  }

  /**
   * Convenience overload for backward compatibility (derives HTTP address from port offset).
   */
  void addPeer(final String peerId, final String address) {
    addPeer(peerId, address, null);
  }

  /**
   * Removes a peer from the Raft cluster. Must be called on any server (Ratis routes to leader).
   * The removed peer will step down automatically.
   *
   * @param peerId the peer ID to remove
   */
  void removePeer(final String peerId) {
    final Collection<RaftPeer> livePeers = haServer.getLivePeers();
    final List<RaftPeer> currentPeers = new ArrayList<>();
    final List<RaftPeer> newPeers = new ArrayList<>();
    for (final RaftPeer peer : livePeers) {
      currentPeers.add(peer);
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);
    }

    if (newPeers.size() == livePeers.size())
      throw new ConfigurationException("Peer " + peerId + " not found in cluster");

    try {
      // Use COMPARE_AND_SET to ensure no concurrent membership change happened between
      // reading livePeers and applying the removal.
      final SetConfigurationRequest.Arguments removeArgs = SetConfigurationRequest.Arguments.newBuilder()
          .setServersInCurrentConf(currentPeers)
          .setServersInNewConf(newPeers)
          .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
          .build();
      final RaftClient client = haServer.getRaftClient();
      if (client == null)
        throw new ConfigurationException("Failed to remove peer " + peerId + ": RaftClient not available");
      final RaftClientReply reply = client.admin().setConfiguration(removeArgs);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to remove peer " + peerId + ": " + reply.getException());
      LogManager.instance().log(this, Level.INFO, "Peer %s removed from Raft cluster", peerId);
      if (clusterMonitor != null)
        clusterMonitor.removeReplica(peerId);
    } catch (final IOException e) {
      throw new ConfigurationException("Failed to remove peer " + peerId, e);
    }
  }

  /**
   * Transfers leadership to the specified peer.
   *
   * @param targetPeerId the target peer to become leader
   * @param timeoutMs    timeout in milliseconds
   */
  void transferLeadership(final String targetPeerId, final long timeoutMs) {
    // Create a fresh client for the admin call to avoid "client is closed" errors.
    // The existing raftClient may have been closed after a prior leadership change.
    try (final RaftClient adminClient = RaftClient.newBuilder()
        .setRaftGroup(haServer.getRaftGroup())
        .setProperties(raftProperties)
        .build()) {
      final RaftClientReply reply = adminClient.admin().transferLeadership(
          RaftPeerId.valueOf(targetPeerId), timeoutMs);
      if (!reply.isSuccess())
        throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": " + reply.getException());
      LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s", targetPeerId);
    } catch (final IOException e) {
      throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId, e);
    }
  }

  /**
   * Steps down from leadership by transferring to any available peer.
   * If no peer is available or the transfer fails, logs at SEVERE but does not throw.
   */
  void stepDown() {
    final String leaderName = haServer.getLeaderName();
    for (final var peer : haServer.getLivePeers()) {
      if (!peer.getId().toString().equals(leaderName)) {
        try {
          transferLeadership(peer.getId().toString(), LEADERSHIP_TRANSFER_TIMEOUT_MS);
          return;
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.SEVERE,
              "Failed to step down (transfer to %s): %s", peer.getId(), e.getMessage());
        }
      }
    }
    LogManager.instance().log(this, Level.SEVERE,
        "Cannot step down: no other peer available for leadership transfer");
  }
}
