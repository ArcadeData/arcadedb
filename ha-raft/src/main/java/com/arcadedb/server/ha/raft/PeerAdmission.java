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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Admits a peer to the Raft configuration and removes one: refuses an address that is this node, that another member
 * already uses, or that does not answer, before the membership change is proposed, and records the HTTP address and
 * display name of the peer it admits.
 * <p>
 * Split out of {@link RaftHAServer}, whose cluster management endpoints add and remove peers through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PeerAdmission {
  private final RaftHAServer ha;

  PeerAdmission(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Records the HTTP address another node declared for a peer it admitted, so this node's probes of that peer dial the
   * listener the operator named rather than a derived guess (issue #8689).
   * <p>
   * {@code connect cluster host:raftPort:httpPort} writes the declared address into the map of the node that served
   * it (issue #8330). When that node is a follower nothing else carries it: the Raft configuration entry has no field
   * for it, and the per-node map of the LEADER - where the security seed runs, and whose #7511 capability gate admits
   * the group and API-token entries only after the new peer answers a probe - holds nothing for a brand-new peer, or
   * for one it removed. The leader then derives the address from the peer's Raft host plus its own HTTP port, which on
   * a cluster whose ports are not in step names the wrong listener or the leader itself. The admitting node therefore
   * sends the address with its seed request, and the leader records it here.
   * <p>
   * Three refusals, each answering "not recorded":
   * <ul>
   * <li>the peer is not in the committed configuration - an address is never recorded for a node that is not a
   * member, so a seed request cannot be used to plant entries for arbitrary ids;</li>
   * <li>the peer is this node - its own address is its own listener, not somebody else's statement about it;</li>
   * <li>the entry this node holds is the one its own {@code arcadedb.ha.serverList} declared - the operator said that
   * here, and a declaration made on another node does not outrank it. A derived or runtime-written entry is replaced,
   * exactly as a {@code connect cluster} served by this node would have replaced it.</li>
   * </ul>
   * <b>Trust.</b> The only caller is {@code PostSecuritySeedHandler}, behind the root / cluster-token check every
   * peer-to-peer route has. A holder of the cluster token can therefore point this node's dials of a committed member
   * that its server list does not declare at a {@code host:port} of its choosing - dials that carry the token. That is
   * no wider than what the token already grants (the same caller can run {@code connect cluster} on this node, which
   * writes the same map), and the address is refused unless it is a plain authority. The host is deliberately not
   * required to equal the peer's Raft host: a node may serve HTTP on a different interface than Raft.
   * <p>
   * A node that has just become leader and not yet applied the configuration entry that added the peer refuses, as for
   * any non-member: the request then degrades to the pre-#8689 behaviour - an ordinary admission seed - rather than
   * failing, and a re-issued admission records the address once the entry is applied.
   *
   * @return {@code true} when the map now holds {@code httpAddress} for the peer and did not before, which is what
   * tells the caller that a seed run earlier probed a different address
   */
  boolean recordAdmittedPeerHttpAddress(final RaftPeerId peerId, final String httpAddress) {
    return recordAdmittedPeerHttpAddress(ha.localPeerId, ha.getCommittedPeersOrNull(), ha.httpAddresses, ha.serverListHttpAddresses,
        peerId, httpAddress);
  }

  /**
   * The body of {@link #recordAdmittedPeerHttpAddress(RaftPeerId, String)} over explicit state, so every refusal can be
   * pinned without a running Raft server. The map write is lock-free: a conditional write against the value just read,
   * retried if another writer got there in between, so a concurrent leave or add of the same peer is never overwritten
   * with a decision made against the state before it.
   *
   * @param committedPeers the committed configuration, or {@code null} when it cannot be read - which refuses
   */
  // @VisibleForTesting
  static boolean recordAdmittedPeerHttpAddress(final RaftPeerId localPeerId, final Collection<RaftPeer> committedPeers,
      final Map<RaftPeerId, String> httpAddresses, final Map<RaftPeerId, String> serverListHttpAddresses,
      final RaftPeerId peerId, final String httpAddress) {
    if (peerId == null || httpAddress == null || peerId.equals(localPeerId) || committedPeers == null)
      return false;
    boolean member = false;
    for (final RaftPeer peer : committedPeers)
      if (peer.getId().equals(peerId)) {
        member = true;
        break;
      }
    if (!member)
      return false;

    final String declaredHere = serverListHttpAddresses.get(peerId);
    while (true) {
      final String current = httpAddresses.get(peerId);
      if (httpAddress.equals(current))
        return false;
      if (current != null && current.equals(declaredHere))
        return false;
      if (current == null ? httpAddresses.putIfAbsent(peerId, httpAddress) == null
          : httpAddresses.replace(peerId, current, httpAddress))
        return true;
    }
  }

  public void addPeer(final String peerId, final String address) {
    addPeer(peerId, address, null);
  }

  public void addPeer(final String peerId, final String address, final String name) {
    // Built here rather than left to RaftClusterManager so both overloads meet at the one method the
    // reachability probe below guards, instead of the probe having to be repeated per overload.
    addPeer(RaftPeer.newBuilder()
        .setId(RaftPeerId.valueOf(peerId))
        .setAddress(address)
        .build(), name);
  }

  /**
   * See {@link RaftClusterManager#addPeer(RaftPeer, String)}: adds the peer with every field it carries.
   * <p>
   * This is the method every operator-facing entry point reaches - {@code POST /api/v1/cluster/peer} through
   * {@link PostAddPeerHandler}, {@code connect cluster} and the gRPC {@code ConnectCluster} RPC through
   * {@code RaftHAPlugin.connectCluster}, and the embedded {@code HAServerPlugin.addPeer} API - so it is where
   * the pre-flight reachability probe of issue #7514 lives. It sits HERE rather than inside
   * {@link RaftClusterManager} because the two are different jobs: the manager's is to issue a Raft
   * configuration change, and knowing whether an address answers a TCP connection is not part of it. This
   * object is also the one holding the {@link ContextConfiguration} the probe's budget comes from.
   */
  void addPeer(final RaftPeer newPeer, final String name) {
    addPeer(newPeer, name, null);
  }

  /**
   * {@link #addPeer(RaftPeer, String)} with the HTTP address the caller declared for the peer, which
   * {@link RaftClusterManager#addPeer(RaftPeer, String, String)} puts in place before the membership change commits
   * so the security seed that commit triggers probes the right listener (issue #8330).
   */
  void addPeer(final RaftPeer newPeer, final String name, final String declaredHttpAddress) {
    ensureNotSelf(ha.localPeerId, getLocalRaftAddress(), newPeer);
    // Both preconditions below read the configuration the compare-and-set is evaluated against, through the same bounded read
    // (issue #10937): getLivePeers() substitutes the declared server list while the division is unreadable, where a declared
    // peer that is not a member reads as one, both checks skip it, and the add then goes on for real. Refused with the reason
    // when it stays unreadable, as the change itself would be.
    final List<RaftPeer> committed = ha.clusterManager.readCommittedConfiguration(
        "add peer " + newPeer.getId() + " at " + newPeer.getAddress()).peers();
    ensureNoDuplicateAddress(committed, newPeer);
    ensurePeerReachable(newPeer, RaftHAServer.isPeerInConfig(committed, newPeer.getId()));
    ha.clusterManager.addPeer(newPeer, name, declaredHttpAddress);
  }

  /**
   * Refuses an add that would put a second peer id on an address the configuration already holds (issue #7515).
   * <p>
   * The generalization of {@link #ensureNotSelf}: that method answers for THIS node, this one for every member.
   * The hazard is the same and so is its cause - {@code RaftClusterManager.buildAddArgs} compares by id alone, so
   * an existing member's address submitted under a new id is not seen as a duplicate and the {@code Mode.ADD}
   * commits. {@link DuplicatePeerAddressException} describes what that leaves behind.
   * <p>
   * Ordered after the self check so the self case keeps its own, more specific refusal: this node is a member
   * too, so both would otherwise fire on it and the operator would get the message that explains less.
   * <p>
   * {@link #addPeer(RaftPeer, String, String)} hands it the COMMITTED configuration, the one {@code buildAddArgs} builds the
   * compare-and-set from and the reachability probe consults (issue #10937), so a peer that is already a member under its own
   * id still reaches the idempotent no-op rather than being refused here - {@code ensureNotSelf}'s sibling property, and what
   * keeps a repeated {@code connect cluster} working. Never {@link RaftHAServer#getLivePeers()}: the declared list it substitutes while the
   * division is unreadable makes a declared peer that is not a member read as one, and this check would skip it.
   * <p>
   * <b>An early refusal, not the guarantee.</b> The configuration is read here on the serving node, which may be a
   * follower and may lag, so this call alone could not stop two concurrent adds of one address under two ids. The
   * guarantee is the same check inside {@code RaftClusterManager.buildAddArgs}, rebuilt on every attempt of a
   * compare-and-set membership change: the leader applies the change only if its configuration is still the one that
   * check read, so of two racing adds one commits and the other is refused against the configuration holding the
   * winner (issue #7802). This copy just answers the common single-request mistake before the reachability probe.
   * A configuration that ALREADY holds a shared address, built before either check existed, is reported by the
   * {@code peers-share-address} alert instead of being refused.
   */
  static void ensureNoDuplicateAddress(final Collection<RaftPeer> committedPeers, final RaftPeer newPeer) {
    // Two passes, not one: a single loop that returns on the id match and throws on an address match answers
    // whichever it happens to reach first, so a re-add of an existing member would be refused or allowed
    // depending on the configuration's iteration order.
    if (RaftHAServer.isPeerInConfig(committedPeers, newPeer.getId()))
      // Already a member under this very id: the add is the documented idempotent no-op, not a duplicate.
      return;

    for (final RaftPeer peer : committedPeers)
      if (RaftHAServer.isSameHttpEndpoint(peer.getAddress(), newPeer.getAddress()))
        throw new DuplicatePeerAddressException(newPeer.getId().toString(), newPeer.getAddress(),
            peer.getId().toString());
  }

  /** This node's own Raft address as the cluster knows it, or {@code null} before {@code start()} resolved one. */
  String getLocalRaftAddress() {
    if (ha.localPeerId == null)
      return null;
    for (final RaftPeer peer : ha.getLivePeers())
      if (ha.localPeerId.equals(peer.getId()))
        return peer.getAddress();
    return null;
  }

  /**
   * Refuses an add that names this node itself (issue #7515), whether by its id or by its address.
   * <p>
   * First, ahead of the reachability probe: this node is by definition reachable, so the probe would pass and the
   * request would go on to be the silent no-op {@link SelfJoinNotSupportedException} describes. It is also the
   * cheaper check of the two, and it is the one that identifies the mistake - an operator who meant "make this
   * node join that cluster" - rather than a network condition.
   * <p>
   * <b>Both halves are needed because the two arrive independently.</b> {@code connect cluster} derives the id
   * from the address, so for that verb they cannot disagree - but {@code POST /api/v1/cluster/peer} takes
   * {@code peerId} and {@code address} as separate fields, and the embedded {@code HAServerPlugin.addPeer} the
   * same. An id check alone therefore passes a payload that names this node's Raft address under some OTHER id:
   * the reachability probe then succeeds, because this node is listening, and {@code RaftClusterManager.buildAddArgs}
   * - which also compares by id - does not recognize it as a member either, so the {@code Mode.ADD} commits and the
   * configuration ends up holding <b>two entries for one process</b>. That is precisely what
   * {@link RaftPeerAddressResolver#peerIdForAddress} exists to prevent ("or the cluster ends up with two
   * configuration entries for one process") and it is worse than the no-op this method was first written for,
   * because it commits.
   * <p>
   * The address comparison is {@link RaftHAServer#isSameHttpEndpoint}, the comparator {@link RaftHAServer#isOwnHttpAddress} already uses
   * for the identical question on the HTTP listener - case-insensitive, and treating the loopback spellings as one
   * socket, so {@code localhost:2434} and {@code 127.0.0.1:2434} are recognized as the same node. It deliberately
   * does not resolve hostnames: a declared host name is a statement about which node owns which port that this
   * method has no business second-guessing, and answering "that is me" for a name that is not would refuse a
   * legitimate join.
   * <p>
   * Static and package-private for the same reason {@link PeerReachability}'s refusal rule is: the decision is a
   * comparison of two ids and two addresses, and standing up a {@link RaftHAServer} to exercise it would test the
   * fixture instead. Both local values are null only before {@code start()} has resolved them, where there is no
   * "this node" to compare against and the add cannot reach a cluster anyway.
   */
  static void ensureNotSelf(final RaftPeerId localPeerId, final String localRaftAddress, final RaftPeer newPeer) {
    if (localPeerId == null)
      return;

    if (localPeerId.equals(newPeer.getId()))
      throw new SelfJoinNotSupportedException(localPeerId.toString(), newPeer.getAddress());

    if (localRaftAddress != null && RaftHAServer.isSameHttpEndpoint(localRaftAddress, newPeer.getAddress()))
      throw new SelfJoinNotSupportedException(localPeerId.toString(), newPeer.getAddress(),
          newPeer.getId().toString());
  }

  /**
   * Refuses an add whose target answers no TCP connection on its Raft address, before any configuration
   * change is issued (issue #7514).
   * <p>
   * Skipped for a peer that is already in the configuration this add would be evaluated against, because
   * {@code RaftClusterManager.buildAddArgs} treats that case as a no-op success. Adding the peer again is then idempotent -
   * which {@code connect cluster} documents and {@code Issue7401ConnectClusterJoinsPeerIT} pins - and a probe that refused it
   * would have broken that for any committed member that happens to be down. {@code alreadyAMember} is read by the caller from
   * the committed configuration, the one {@code buildAddArgs} reads (issue #10937): the declared list
   * {@link RaftHAServer#getLivePeers()} substitutes for an unreadable one would skip the probe for a declared peer that is not a member,
   * and the add would then wait out the whole membership budget on a peer that does not answer. This check has no second copy
   * inside the change, unlike the duplicate-address one.
   */
  private void ensurePeerReachable(final RaftPeer newPeer, final boolean alreadyAMember) {
    final long probeTimeoutMs = ha.configuration.getValueAsLong(GlobalConfiguration.HA_ADD_PEER_PROBE_TIMEOUT);
    final String reason = PeerReachability.addRefusalReason(alreadyAMember, probeTimeoutMs, newPeer.getAddress());
    if (reason != null)
      throw new UnreachablePeerException(newPeer.getId().toString(), newPeer.getAddress(), reason,
          ha.localPeerId != null ? ha.localPeerId.toString() : null, probeTimeoutMs);
  }

  public void removePeer(final String peerId) {
    ha.clusterManager.removePeer(peerId);
  }

  public void removePeer(final String peerId, final boolean force) {
    ha.clusterManager.removePeer(peerId, force);
  }

  /**
   * Registers or updates the display name for a peer. The stored value is
   * formatted as {@code name (httpAddress)} when the peer's HTTP address is known.
   */
  void registerPeerDisplayName(final RaftPeerId peerId, final String name) {
    if (name == null || name.isEmpty())
      return;
    final String httpAddr = ha.httpAddresses.get(peerId);
    ha.peerDisplayNames.put(peerId, httpAddr != null ? name + " (" + httpAddr + ")" : name);
  }
}
