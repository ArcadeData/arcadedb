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

import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

/**
 * Handles membership operations for the Raft cluster: adding and removing peers,
 * transferring leadership, and graceful cluster leave.
 * <p>
 * Delegates to {@link RaftHAServer} for shared state (live peers, leader status,
 * HTTP address map, leader-change notifier). All Raft configuration changes go
 * through {@link #setConfigurationWithRetry} which retries bounded times to
 * survive the window where a newly elected leader has not yet committed from
 * its current term.
 */
class RaftClusterManager {

  private final RaftHAServer raftHAServer;

  RaftClusterManager(final RaftHAServer raftHAServer) {
    this.raftHAServer = raftHAServer;
  }

  void addPeer(final String peerId, final String address) {
    final RaftPeer newPeer = RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build();

    final List<RaftPeer> newPeers = new ArrayList<>(raftHAServer.getLivePeers());
    newPeers.add(newPeer);

    setConfigurationWithRetry(newPeers, "add peer " + peerId);

    final int colonIdx = address.lastIndexOf(':');
    if (colonIdx > 0) {
      final String host = address.substring(0, colonIdx);
      try {
        final int raftPort = Integer.parseInt(address.substring(colonIdx + 1));
        final int httpPortOffset = getHttpPortOffset();
        raftHAServer.getHttpAddresses().put(RaftPeerId.valueOf(peerId), host + ":" + (raftPort + httpPortOffset));
      } catch (final NumberFormatException ignored) {
      }
    }

    LogManager.instance().log(this, Level.INFO, "Peer %s added to Raft cluster at %s", peerId, address);
  }

  void removePeer(final String peerId) {
    final Collection<RaftPeer> livePeers = raftHAServer.getLivePeers();
    final List<RaftPeer> newPeers = new ArrayList<>();
    for (final RaftPeer peer : livePeers)
      if (!peer.getId().toString().equals(peerId))
        newPeers.add(peer);

    if (newPeers.size() == livePeers.size())
      throw new ConfigurationException("Peer " + peerId + " not found in cluster");

    setConfigurationWithRetry(newPeers, "remove peer " + peerId);

    raftHAServer.getHttpAddresses().remove(RaftPeerId.valueOf(peerId));
    LogManager.instance().log(this, Level.INFO, "Peer %s removed from Raft cluster", peerId);
  }

  boolean transferLeadership(final long timeoutMs) {
    final RaftClient client = raftHAServer.getClient();
    if (client == null)
      return false;
    try {
      final RaftClientReply reply = client.admin().transferLeadership(null, timeoutMs);
      return reply.isSuccess() || !raftHAServer.isLeader();
    } catch (final Exception e) {
      // When the transfer succeeds, notifyLeaderChanged calls refreshRaftClient() which
      // closes the old client. The in-flight RPC then fails with "is closed".
      // If we are no longer the leader, the transfer succeeded.
      if (!raftHAServer.isLeader())
        return true;
      LogManager.instance().log(this, Level.INFO, "Leadership transfer request: %s", e.getMessage());
      return false;
    }
  }

  void transferLeadership(final String targetPeerId, final long timeoutMs) {
    LogManager.instance().log(this, Level.INFO, "Transferring leadership to %s (timeout=%d ms)", targetPeerId, timeoutMs);
    final RaftClient client = raftHAServer.getClient();
    try {
      final RaftClientReply reply = client.admin().transferLeadership(
          RaftPeerId.valueOf(targetPeerId), timeoutMs);
      if (!reply.isSuccess()) {
        // The leader change notification fires refreshRaftClient(), which closes the old client
        // while this call is still in flight. If the leader actually changed to the target,
        // the transfer succeeded despite the error reply.
        if (isLeaderNow(targetPeerId)) {
          LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s (confirmed via leader check)", targetPeerId);
          return;
        }
        throw new ConfigurationException(
            "Failed to transfer leadership to " + targetPeerId + ": " + reply.getException());
      }
      LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s", targetPeerId);
    } catch (final IOException e) {
      // When the transfer succeeds, notifyLeaderChanged calls refreshRaftClient() which closes
      // the old RaftClient. The in-flight RPC then fails with "is closed". Verify the transfer
      // actually succeeded by checking who the leader is now.
      if (isLeaderNow(targetPeerId)) {
        LogManager.instance().log(this, Level.INFO, "Leadership transferred to %s (confirmed after IOException)", targetPeerId);
        return;
      }
      throw new ConfigurationException("Failed to transfer leadership to " + targetPeerId + ": " + e.getMessage(), e);
    }
  }

  void leaveCluster() {
    if (raftHAServer.getClient() == null)
      return;

    final RaftPeerId localPeerId = raftHAServer.getLocalPeerId();

    try {
      final Collection<RaftPeer> livePeers = raftHAServer.getLivePeers();
      if (livePeers.size() <= 1) {
        HALog.log(this, HALog.BASIC, "Single-node cluster, skipping leave");
        return;
      }

      if (raftHAServer.isLeader()) {
        final Object leaderChangeNotifier = raftHAServer.getLeaderChangeNotifier();
        for (final RaftPeer peer : livePeers) {
          if (!peer.getId().equals(localPeerId)) {
            HALog.log(this, HALog.BASIC,
                "Leaving cluster: transferring leadership to %s before removal", peer.getId());
            try {
              transferLeadership(peer.getId().toString(), 10_000);
              final long deadline = System.currentTimeMillis() + 5_000;
              synchronized (leaderChangeNotifier) {
                while (raftHAServer.isLeader()) {
                  final long remaining = deadline - System.currentTimeMillis();
                  if (remaining <= 0)
                    break;
                  leaderChangeNotifier.wait(remaining);
                }
              }
            } catch (final Exception e) {
              HALog.log(this, HALog.BASIC,
                  "Leadership transfer failed (%s), proceeding with removal", e.getMessage());
            }
            break;
          }
        }
      }

      HALog.log(this, HALog.BASIC, "Leaving cluster: removing self (%s) from Raft group", localPeerId);
      removePeer(localPeerId.toString());
      HALog.log(this, HALog.BASIC, "Successfully left the Raft cluster");

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING,
          "Failed to leave cluster gracefully: %s", e.getMessage());
    }
  }

  /**
   * Calls {@code setConfiguration} with bounded retry on {@link ReconfigurationInProgressException}.
   * <p>
   * On a fresh cluster the newly elected leader must commit an entry from its own term before it
   * can process configuration changes (Raft protocol requirement). This method sends a no-op
   * message first to ensure the leader has committed from its current term, then issues the
   * setConfiguration call with bounded retry.
   */
  private void setConfigurationWithRetry(final List<RaftPeer> peers, final String operationDesc) {
    final long deadline = System.currentTimeMillis() + 90_000;
    long sleepMs = 200;

    while (true) {
      try {
        final RaftClientReply reply = raftHAServer.getClient().admin().setConfiguration(peers);
        if (reply.isSuccess())
          return;

        if (System.currentTimeMillis() < deadline) {
          LogManager.instance().log(this, Level.FINE,
              "setConfiguration failed for %s, retrying in %d ms: %s", operationDesc, sleepMs, reply.getException());
          Thread.sleep(sleepMs);
          sleepMs = Math.min(sleepMs * 2, 2_000);
          continue;
        }
        throw new ConfigurationException("Failed to " + operationDesc + ": " + reply.getException());
      } catch (final IOException e) {
        if (System.currentTimeMillis() < deadline) {
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
        throw new ConfigurationException("Failed to " + operationDesc, e);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ConfigurationException("Interrupted while waiting to " + operationDesc, e);
      }
    }
  }

  private boolean isLeaderNow(final String expectedPeerId) {
    final RaftPeerId leaderId = raftHAServer.getLeaderId();
    return leaderId != null && leaderId.toString().equals(expectedPeerId);
  }

  private int getHttpPortOffset() {
    final Map<RaftPeerId, String> httpAddresses = raftHAServer.getHttpAddresses();
    for (final RaftPeer peer : raftHAServer.getRaftGroup().getPeers()) {
      final String httpAddr = httpAddresses.get(peer.getId());
      if (httpAddr != null) {
        try {
          final int httpPort = Integer.parseInt(httpAddr.substring(httpAddr.lastIndexOf(':') + 1));
          final int raftPort = Integer.parseInt(
              peer.getAddress().toString().substring(peer.getAddress().toString().lastIndexOf(':') + 1));
          return httpPort - raftPort;
        } catch (final NumberFormatException ignored) {
        }
      }
    }
    return 46;
  }
}
