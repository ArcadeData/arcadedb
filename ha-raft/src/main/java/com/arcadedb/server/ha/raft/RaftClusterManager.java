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
import org.apache.ratis.protocol.SetConfigurationRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
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
    addPeer(peerId, address, null);
  }

  void addPeer(final String peerId, final String address, final String name) {
    final RaftPeer newPeer = RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build();

    // Mode.ADD atomically appends this single peer to the CURRENT committed configuration, so two
    // near-simultaneous adds cannot clobber each other. A full setConfiguration(getLivePeers()+peer)
    // is read-modify-write last-write-wins and silently drops one of two concurrent adds (issue #4795),
    // which is exactly why the K8s auto-join path already uses Mode.ADD (see KubernetesAutoJoin).
    setConfigurationWithRetry(() -> buildAddArgs(peerId, newPeer), "add peer " + peerId);

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

    if (name != null && !name.isEmpty())
      raftHAServer.registerPeerDisplayName(RaftPeerId.valueOf(peerId), name);

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
    setConfigurationWithRetry(() -> buildRemoveArgs(peerId, force), "remove peer " + peerId);

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

  boolean transferLeadership(final long timeoutMs) {
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
      return false;
    }

    try {
      final RaftClientReply reply = client.admin().transferLeadership(null, timeoutMs);
      if (reply.isSuccess())
        return true;
      // Non-success reply (often because notifyLeaderChanged closed our client while the RPC was
      // still in flight). Only report success if leadership has actually settled on a DIFFERENT peer,
      // rather than inferring it from a transient "we are no longer leader" state - a candidate /
      // leaderless window, or an unrelated concurrent election (issue #4809).
      return confirmLeadershipMovedAway(selfId);
    } catch (final Exception e) {
      // When the transfer succeeds, notifyLeaderChanged calls refreshRaftClient() which closes the
      // old client, so the in-flight RPC fails with "is closed". Confirm an actual, settled handoff to
      // a different peer instead of trusting !isLeader() (issue #4809).
      if (confirmLeadershipMovedAway(selfId))
        return true;
      LogManager.instance().log(this, Level.INFO, "Leadership transfer request: %s", e.getMessage());
      return false;
    }
  }

  private static final long LEADER_CONFIRM_TIMEOUT_MS = 3_000;
  private static final long LEADER_CONFIRM_POLL_MS     = 50;

  /**
   * Confirms that leadership has settled on a peer OTHER than {@code selfId} within a short grace
   * window. Used by the no-target {@link #transferLeadership(long)} so success is not reported merely
   * because this node transiently stopped being the leader (issue #4809): a real handoff ends with a
   * concrete, different peer established as leader, whereas a candidate / leaderless window leaves
   * {@link RaftHAServer#getLeaderId()} null or still pointing at this node.
   */
  private boolean confirmLeadershipMovedAway(final RaftPeerId selfId) {
    final long deadline = System.currentTimeMillis() + LEADER_CONFIRM_TIMEOUT_MS;
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
      final Object leaderChangeNotifier = raftHAServer.getLeaderChangeNotifier();
      for (final RaftPeer peer : currentPeers) {
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
          } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ConfigurationException("Interrupted while leaving cluster", ie);
          } catch (final Exception e) {
            HALog.log(this, HALog.BASIC,
                "Leadership transfer failed (%s), proceeding with removal", e.getMessage());
          }
          break;
        }
      }
    }

    HALog.log(this, HALog.BASIC, "Leaving cluster: removing self (%s) from Raft group", localPeerId);
    removePeer(localPeerId.toString(), force);
    HALog.log(this, HALog.BASIC, "Successfully left the Raft cluster");
  }

  /**
   * Issues a {@code setConfiguration} call with bounded (90s) retry, re-evaluating {@code argsSupplier}
   * on every attempt.
   * <p>
   * Rebuilding the arguments each attempt is what makes {@code COMPARE_AND_SET} removals safe: a retry
   * after a CAS mismatch (a concurrent membership change) re-snapshots the current configuration so the
   * next attempt's precondition is fresh. It also survives the window on a fresh cluster where a newly
   * elected leader has not yet committed an entry from its own term and therefore transiently rejects
   * configuration changes. A {@code null} from the supplier means the goal is already met and the call
   * returns successfully.
   */
  private void setConfigurationWithRetry(final Supplier<SetConfigurationRequest.Arguments> argsSupplier,
      final String operationDesc) {
    final long deadline = System.currentTimeMillis() + 90_000;
    long sleepMs = 200;

    while (true) {
      try {
        // Rebuild the arguments on every attempt so COMPARE_AND_SET retries re-snapshot the current
        // configuration after a concurrent change (issue #4795). A null result means the goal is
        // already met (e.g. the peer was removed by a concurrent change).
        final SetConfigurationRequest.Arguments args = argsSupplier.get();
        if (args == null)
          return;

        final RaftClientReply reply = raftHAServer.getClient().admin().setConfiguration(args);
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
              peer.getAddress().substring(peer.getAddress().lastIndexOf(':') + 1));
          return httpPort - raftPort;
        } catch (final NumberFormatException ignored) {
        }
      }
    }
    return 46;
  }
}
