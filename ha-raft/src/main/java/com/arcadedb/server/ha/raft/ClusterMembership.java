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

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The reconciliation of the two peer lists a node knows about: the <b>static</b> group built once at startup from
 * {@code arcadedb.ha.serverList}, and the <b>live</b> Raft configuration, which membership changes ({@code DELETE
 * /api/v1/cluster/peer/<id>}, {@code POST /api/v1/cluster/peer}, a shrink by a pre-#5275 build) move away from it
 * over the node's lifetime.
 * <p>
 * {@code /api/v1/cluster} used to build its peer list from the static group alone and label every entry
 * {@code FOLLOWER}, so a peer removed from the live configuration kept reading as a healthy member with an address,
 * indistinguishable from a working one (issue #7040). The status endpoint now reports the union - every peer the
 * operator declared plus every peer the cluster committed - and says for each whether it is in the configuration.
 * <p>
 * Pure value: the order of {@link #peers()} is the static order first (stable across polls, the order the operator
 * wrote), then the live-only peers in configuration order. For an id present in both lists the <b>live</b> entry
 * is kept: its address is the one the cluster committed and dials, and a peer that rejoined at a new address under
 * its old id would otherwise be reported at the declared, stale one.
 * <p>
 * {@link #configuredPeers()} is the view every "which peers are members" consumer in {@link RaftHAServer} reads -
 * the server stats, the replica address list and the client routing table - so a peer the configuration dropped
 * disappears from all of them at once instead of from the one that happened to be fixed (see the module's
 * {@code CLAUDE.md}, "Peer-list filtering").
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ClusterMembership {
  private final List<RaftPeer>   peers;
  private final Set<RaftPeerId>  configured;
  private final List<String>     notInConfiguration;
  private final List<String>     notInServerList;

  private ClusterMembership(final List<RaftPeer> peers, final Set<RaftPeerId> configured,
      final List<String> notInConfiguration, final List<String> notInServerList) {
    this.peers = peers;
    this.configured = configured;
    this.notInConfiguration = notInConfiguration;
    this.notInServerList = notInServerList;
  }

  /**
   * Reconciles the static group against the live configuration.
   *
   * @param staticPeers the peers of {@code arcadedb.ha.serverList}, in declaration order
   * @param livePeers   the peers of the current Raft configuration
   */
  static ClusterMembership of(final Collection<RaftPeer> staticPeers, final Collection<RaftPeer> livePeers) {
    final Map<RaftPeerId, RaftPeer> byId = new LinkedHashMap<>();
    for (final RaftPeer peer : staticPeers)
      byId.put(peer.getId(), peer);

    final Set<RaftPeerId> configured = new HashSet<>();
    final List<String> notInServerList = new ArrayList<>();
    for (final RaftPeer peer : livePeers) {
      configured.add(peer.getId());
      // put() keeps the declared position (LinkedHashMap does not reorder on replace) and takes the live entry.
      if (byId.put(peer.getId(), peer) == null)
        notInServerList.add(peer.getId().toString());
    }

    final List<String> notInConfiguration = new ArrayList<>();
    for (final RaftPeer peer : staticPeers)
      if (!configured.contains(peer.getId()))
        notInConfiguration.add(peer.getId().toString());

    return new ClusterMembership(Collections.unmodifiableList(new ArrayList<>(byId.values())), configured,
        Collections.unmodifiableList(notInConfiguration), Collections.unmodifiableList(notInServerList));
  }

  /** Every peer known to this node: the static list first, then the peers only the live configuration holds. */
  List<RaftPeer> peers() {
    return peers;
  }

  /** The members of the live Raft configuration, in the order of {@link #peers()}. */
  List<RaftPeer> configuredPeers() {
    final List<RaftPeer> result = new ArrayList<>(configured.size());
    for (final RaftPeer peer : peers)
      if (configured.contains(peer.getId()))
        result.add(peer);
    return result;
  }

  /** Whether {@code peerId} is a member of the live Raft configuration. */
  boolean isInConfiguration(final RaftPeerId peerId) {
    return configured.contains(peerId);
  }

  /** Ids of the declared peers that the live configuration no longer contains, in declaration order. */
  List<String> notInConfiguration() {
    return notInConfiguration;
  }

  /** Ids of the committed members that {@code arcadedb.ha.serverList} does not declare, in configuration order. */
  List<String> notInServerList() {
    return notInServerList;
  }

  /** Whether the two lists differ at all. */
  boolean diverged() {
    return !notInConfiguration.isEmpty() || !notInServerList.isEmpty();
  }
}
