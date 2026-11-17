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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Chooses the peers a leader can hand leadership to when it steps down: peers that answered recently, are caught up and
 * serve every database, in the order the transfer should try them.
 * <p>
 * Split out of {@link RaftHAServer}, which steps down and transfers leadership to the first of them that accepts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class StepDownTargets {
  private StepDownTargets() {
  }

  /**
   * Selects, in preference order, the peers eligible to receive leadership when this leader steps
   * down (issue #4808). A peer is eligible when it is not this node, is not lagging behind the leader
   * ({@link ClusterMonitor#isReplicaLagging}), and is not a priority-0 witness/replica while peers with
   * a higher priority exist - Ratis is configured never to elect a priority-0 peer, so handing it
   * leadership would just bounce it straight back and prolong write unavailability. The list is ordered
   * by descending Raft priority so the strongest candidate is tried first; equal priorities keep the
   * cluster iteration order.
   *
   * @return an ordered list of candidate peers, possibly empty (the caller then delegates the choice to
   *         Ratis via the no-target transfer).
   */
  static List<RaftPeer> selectStepDownTargets(final Collection<RaftPeer> livePeers, final RaftPeerId localPeerId,
      final ClusterMonitor clusterMonitor) {
    return selectStepDownTargets(livePeers, localPeerId, clusterMonitor, null);
  }

  /**
   * {@link #selectStepDownTargets(Collection, RaftPeerId, ClusterMonitor)} that also drops every peer not in
   * {@code reachable}: the ids {@link RaftHAServer#handoffReachablePeers()} proved answered this leader recently (issue #8556).
   * {@code null} applies no reachability screen, which only unit tests of the other rules pass. Every production
   * caller passes {@link RaftHAServer#handoffReachablePeers()}.
   * <p>
   * The screen is not optional. Lag alone cannot tell a peer that is down from a healthy one: a follower that died at a
   * high water mark keeps a small lag on an idle cluster, and {@link ClusterMonitor#isReplicaLagging} answers false for
   * a peer it has no tick for, which is every peer right after {@link ClusterMonitor#reset()} - that is, right after
   * the election a hand-off typically reacts to. And a targeted Ratis transfer to a dead peer is not a harmless
   * no-op: Ratis holds it pending for the caller's whole timeout, and refuses every write on this leader, on every
   * database, while it does.
   */
  static List<RaftPeer> selectStepDownTargets(final Collection<RaftPeer> livePeers, final RaftPeerId localPeerId,
      final ClusterMonitor clusterMonitor, final Set<String> reachable) {
    final String localId = localPeerId.toString();

    // Highest priority among the other peers. When it is 0 the cluster runs with the default,
    // homogeneous priorities and every follower is equally electable; only when some peer carries an
    // explicit positive priority do the priority-0 peers become non-electable witnesses to be skipped.
    int maxPriority = 0;
    for (final RaftPeer peer : livePeers)
      if (!peer.getId().toString().equals(localId))
        maxPriority = Math.max(maxPriority, peer.getPriority());

    final List<RaftPeer> candidates = new ArrayList<>();
    for (final RaftPeer peer : livePeers) {
      final String peerId = peer.getId().toString();
      if (peerId.equals(localId))
        continue;
      // Priority-0 witness/replica while real voters exist: Ratis would never keep it as leader.
      if (maxPriority > 0 && peer.getPriority() <= 0)
        continue;
      // Lagging follower: promoting it would prolong write unavailability while it catches up.
      if (clusterMonitor != null && clusterMonitor.isReplicaLagging(peerId))
        continue;
      // STALLED follower (issue #8556): unreachable while caught up (#5291), never appended (#5295) or stuck behind an
      // install-snapshot loop (#8457). Each of those can carry a lag under the threshold, which the test above passes.
      if (clusterMonitor != null && clusterMonitor.getReplicaStatus(peerId) == ClusterMonitor.ReplicaStatus.STALLED)
        continue;
      // Not proven reachable (issue #8556): a targeted transfer to it would hold every write on this leader refused
      // for the whole transfer budget.
      if (reachable != null && !reachable.contains(peerId))
        continue;
      candidates.add(peer);
    }

    // Strongest (highest priority) first; List.sort is stable so equal priorities keep iteration order.
    candidates.sort((a, b) -> Integer.compare(b.getPriority(), a.getPriority()));
    return candidates;
  }

  /**
   * {@code reachable} minus the peers that reported a leader service gap of their own (issue #8665). Lag is judged from
   * the log index, and a gap does not show in it, so a peer that shares this leader's gap - a database missing on every
   * node, or a node-global stale-snapshot gap after a cluster-wide crash - passed every other screen, took leadership
   * and handed it on in turn. Such a peer could not serve what a leader must, and would be handed leadership only to
   * hold the same gap. With no eligible peer left the hand-off backs off instead of rotating.
   * <p>
   * The answer is as fresh as the capability poll ({@link PeerCapabilityRegistry#REFRESH_PERIOD_MS}, believed for
   * {@link PeerCapabilityRegistry#ADVERTISEMENT_TTL_MS}); a peer that developed a gap since its last answer is still
   * eligible, and the per-node back-off of {@code ArcadeStateMachine} bounds that residual case as before. Returns
   * {@code reachable} itself when nothing is excluded, which is every call on a healthy cluster.
   */
  static Set<String> withoutServiceGapPeers(final Set<String> reachable, final Set<String> gapped) {
    if (gapped.isEmpty() || reachable.isEmpty())
      return reachable;
    final Set<String> eligible = new HashSet<>(reachable);
    eligible.removeAll(gapped);
    return eligible;
  }

  /**
   * The peers of {@code followerStates} that are proven reachable from this leader (issue #8556). A peer qualifies
   * only when:
   * <ul>
   *   <li>its match index is known and not negative. Ratis creates each follower's record afresh, at match index -1,
   *   when this node becomes leader, and moves it only when the follower acknowledges an append. So -1 means the
   *   follower has not answered this leader once - a peer that is down right after the election is exactly this, and
   *   so is one still waiting for a snapshot, which a targeted transfer would also leave pending. A degraded entry
   *   with no match index (membership changing under the read, issue #4842) proves nothing and does not qualify
   *   either;</li>
   *   <li>its last RPC response is younger than {@code contactWindowMs}. The leader heartbeats every follower well
   *   inside an election timeout, so a live follower answers far more often than that.</li>
   * </ul>
   * Package-private and static so the rule can be unit-tested without a cluster.
   */
  static Set<String> handoffReachablePeerIds(final List<Map<String, Object>> followerStates, final long contactWindowMs) {
    final Set<String> reachable = new HashSet<>(followerStates.size() * 2);
    for (final Map<String, Object> entry : followerStates) {
      final Object peerId = entry.get("peerId");
      if (peerId == null)
        continue;
      final RaftClusterStatusExporter.FollowerReplicationState state = RaftClusterStatusExporter.FollowerReplicationState.of(
          entry);
      if (!state.matchIndexKnown || state.matchIndex < 0)
        continue;
      // An absent last-RPC time reads as -1 (followerStateIndex), so it fails the first test.
      if (state.lastRpcMs < 0 || state.lastRpcMs >= contactWindowMs)
        continue;
      reachable.add(peerId.toString());
    }
    return reachable;
  }

  /**
   * The window of {@link #handoffReachablePeerIds}: the minimum election timeout, the interval within which a follower
   * that stopped hearing from this leader would already have started an election, capped at the peer-unreachable
   * threshold when that is enabled and shorter.
   */
  static long handoffContactWindowMs(final long electionTimeoutMinMs, final long peerUnreachableThresholdMs) {
    final long window = electionTimeoutMinMs > 0 ? electionTimeoutMinMs : 5_000L;
    return peerUnreachableThresholdMs > 0 ? Math.min(window, peerUnreachableThresholdMs) : window;
  }
}
