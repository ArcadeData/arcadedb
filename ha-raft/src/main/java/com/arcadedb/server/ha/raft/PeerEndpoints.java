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
import com.arcadedb.log.LogManager;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.ha.raft.RaftHAServer.PeerHttpEndpoint;
import com.arcadedb.server.http.HttpServer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Resolves the HTTP, HTTPS, Bolt and gRPC address of each peer: from the declared server list, from what the peer
 * confirmed, or derived from its Raft host. An address two peers could claim is never handed out as one peer's - it is
 * reported once, with what to declare to remove the ambiguity.
 * <p>
 * Split out of {@link RaftHAServer}, which routes forwarded requests, snapshot pulls and client routing tables through it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PeerEndpoints {
  private final RaftHAServer ha;

  // Logged at most once: warns operators that HTTP addresses are derived (not explicitly configured).
  private final    AtomicBoolean           httpFallbackWarned = new AtomicBoolean(false);

  // Logged at most once: notes that peer HTTPS endpoints are derived from this node's local HTTPS port.
  private final    AtomicBoolean           httpsFallbackWarned = new AtomicBoolean(false);

  // The last ambiguity verdict logged for this protocol, or null when the most recent pass found none. Warns that
  // a peer-to-peer endpoint was WITHHELD because two peers resolved to it. Distinct from the two latches above,
  // which fire whenever an address is derived at all - which a healthy homogeneous StatefulSet also does (issue
  // #6267). Per node, like every other latch here: a test that builds several RaftHAServer instances gets a fresh
  // pair each time. See the {@code isNewAmbiguityVerdict} contract for why this is a remembered verdict rather
  // than a boolean (issue #6297).
  private final    AtomicReference<String> httpAmbiguityReported  = new AtomicReference<>();

  private final    AtomicReference<String> httpsAmbiguityReported = new AtomicReference<>();

  // Logged at most once per protocol: warns operators that routing addresses are derived, not configured.
  private final    Map<HAServerPlugin.ROUTING_PROTOCOL, AtomicBoolean> routingFallbackWarned = RaftHAServer.createRoutingProtocolLatches();

  // The last routing-ambiguity verdict logged per protocol: warns that peers resolved to a shared address and were
  // not advertised. Re-reported whenever the verdict changes, see {@code isNewAmbiguityVerdict} (issue #6297).
  private final    Map<HAServerPlugin.ROUTING_PROTOCOL, AtomicReference<String>> routingAmbiguityReported = RaftHAServer.createRoutingProtocolVerdicts();

  PeerEndpoints(final RaftHAServer ha) {
    this.ha = ha;
  }

  public String getReplicaAddresses() {
    final RaftPeerId leaderId = ha.getLeaderId();
    final RaftPeerId excludeId = leaderId != null ? leaderId : ha.localPeerId;
    final StringBuilder sb = new StringBuilder();
    for (final RaftPeer peer : ha.configuredPeers()) {
      if (!peer.getId().equals(excludeId)) {
        final String httpAddr = resolveHttpAddress(peer);
        if (httpAddr != null) {
          if (!sb.isEmpty())
            sb.append(",");
          sb.append(httpAddr);
        }
      }
    }
    return sb.toString();
  }

  /**
   * Builds a single-snapshot routing table for one client protocol (leader as writer, followers as readers)
   * from one {@link RaftHAServer#getLeaderId()} read, so a concurrent leader change cannot make the writer and reader sets
   * mutually inconsistent. Returns {@code null} when no leader is known, the leader has no resolvable
   * address for that protocol, or that address cannot be told apart from a follower's (see
   * {@link #selectUnambiguousRouting}). Readers reflect the live cluster membership ({@link RaftHAServer#configuredPeers()}),
   * matching {@link #getReplicaAddresses()}: a peer the configuration dropped is never handed to a client as a
   * read target (issue #7040). Peers whose address cannot be resolved, or resolved to an address another peer
   * claims too, are skipped.
   * <p>
   * The ambiguity filter is deliberately confined to the client-routing view and is <b>not</b> applied to the
   * HTTP addresses behind {@link #getReplicaAddresses()} or {@code getStats()}: those feed cluster reporting
   * and peer-to-peer snapshot transfer, where withholding a best-effort address does harm and no client ever
   * auto-redirects onto it. Here an address is handed to a client precisely so it can dial it unattended.
   */
  public HAServerPlugin.RoutingTable getRoutingTable(final HAServerPlugin.ROUTING_PROTOCOL protocol) {
    final RaftPeerId leaderId = ha.getLeaderId();
    if (leaderId == null)
      return null;
    return routingTableFor(protocol, leaderId);
  }

  /**
   * The body of {@link #getRoutingTable}, given the leader rather than reading it from Ratis. Split out so the
   * warn wiring below can be driven by a test without a live division - {@code getLeaderId()} answers null until
   * one exists, which is before this method is reached, so the half of the contract that a CLEAN pass carries
   * (clearing the verdict memory) would otherwise have nothing exercising it. Package-private for testing.
   */
  HAServerPlugin.RoutingTable routingTableFor(final HAServerPlugin.ROUTING_PROTOCOL protocol,
      final RaftPeerId leaderId) {
    // Read once and passed down: the "was this declared or derived?" flag below and the address the resolver
    // returns must come from the same map, or a future second source could make them disagree about a peer.
    final Map<RaftPeerId, String> declared = declaredRoutingAddresses(protocol);

    final String writer = resolveRoutingAddress(protocol, declared, leaderId);
    if (writer == null)
      return null;

    final Collection<RaftPeer> peers = ha.configuredPeers();

    // Index 0 is always the writer, so one pair of arrays carries the whole view and the ambiguity check can
    // see the leader and the followers at once. A peer that resolves to nothing is skipped, exactly as before.
    //
    // The +1 is not slack: the leader comes from live Ratis state read separately from the membership above,
    // and getLivePeers() falls back to the declared list when the division cannot be read, so a leader that
    // joined at runtime (addPeer, the Kubernetes auto-join) can be absent from `peers` and the writer occupies
    // a slot beyond it. Sizing to peers.size() would put an ArrayIndexOutOfBoundsException on the Bolt ROUTE
    // path in exactly that window.
    final String[] addresses = new String[peers.size() + 1];
    final boolean[] fromConfig = new boolean[addresses.length];
    // Carried alongside so the warning can name the peers that collided rather than only say that some did
    // (issue #6297) - which is also what makes the line usable as the "have I already said this?" key.
    final RaftPeerId[] owners = new RaftPeerId[addresses.length];
    addresses[0] = writer;
    fromConfig[0] = declared.containsKey(leaderId);
    owners[0] = leaderId;
    int resolved = 1;

    for (final RaftPeer peer : peers) {
      if (peer.getId().equals(leaderId))
        continue;
      // From the peer in hand, which is the live configuration's entry: resolving it by id against the server list gave
      // nothing for a peer that joined at runtime, and the reader was skipped without a word (issue #10861)
      final String reader = resolveRoutingAddress(protocol, declared, peer);
      if (reader == null)
        continue;
      addresses[resolved] = reader;
      fromConfig[resolved] = declared.containsKey(peer.getId());
      owners[resolved] = peer.getId();
      ++resolved;
    }

    // Counted once and handed to both, the way unambiguousPeerAddress already does it: the selection and the
    // warning ask the same question of the same view, so building the map twice is pure waste on a path that now
    // runs the warning on every call.
    final Map<String, int[]> claims = claimsByAddress(addresses, fromConfig, resolved);

    final HAServerPlugin.RoutingTable table = selectUnambiguousRouting(protocol, addresses, fromConfig, resolved,
        claims);
    // Unconditional, and that is the whole point: the verdict memory is only cleared by a pass that reaches it
    // with nothing to say, so guarding this call on "is it ambiguous now?" left the memory holding the last
    // collision forever. The operator would then fix that collision, hit it again later, and be told nothing -
    // the exact case issue #6297 exists to fix. A clean view renders to the empty verdict, which is what clears
    // the memory.
    warnAmbiguousRouting(protocol, addresses, fromConfig, owners, resolved, claims);
    return table;
  }

  /**
   * Drops from a resolved routing view every address that cannot be attributed to a single peer, and returns
   * {@code null} when the writer's own address is one of them.
   * <p>
   * Two peers can never legitimately answer on one {@code host:port} for one protocol - they would be fighting
   * over the socket - so an address claimed by two peers identifies at most one of them and the resolver has no
   * way to say which. That is not a hypothetical: with no {@code bolt:}/{@code grpc:} field declared, a peer's
   * endpoint is derived as its Raft host plus <em>this</em> node's port, so on a cluster whose nodes differ by
   * port rather than by host (several nodes on one machine, a dev or test deployment) every peer derives to the
   * same address and the "leader" a follower advertises is the follower itself. A caller that redirects
   * automatically - the point of the gRPC refusal trailers (issue #6091) - would dial the node that just
   * refused it, be refused again, and loop.
   * <p>
   * A <b>declared</b> address outranks a derived one when the two collide: the operator stated it, and the
   * collision only proves the guess wrong. Two declared addresses colliding is a configuration error with no
   * defensible winner, so neither is advertised.
   * <p>
   * {@code null} is the answer both callers already handle: Bolt's ROUTE falls back to advertising this node as
   * READ/ROUTE (never writer) and the gRPC refusal falls back to naming the leader's HTTP address. Both degrade
   * to what they did before the address could be resolved at all, which beats a confidently wrong one.
   * Package-private for testing.
   *
   * @param addresses  resolved addresses, the writer's at index 0
   * @param fromConfig whether the address at the same index was declared rather than derived
   * @param count      number of populated entries in both arrays
   */
  static HAServerPlugin.RoutingTable selectUnambiguousRouting(final HAServerPlugin.ROUTING_PROTOCOL protocol,
      final String[] addresses, final boolean[] fromConfig, final int count) {
    return selectUnambiguousRouting(protocol, addresses, fromConfig, count,
        claimsByAddress(addresses, fromConfig, count));
  }

  /**
   * The same, for a caller that has already counted the claims and would otherwise have this method count them a
   * second time over the same view.
   *
   * @param claims the {@link #claimsByAddress} counts for exactly this view
   */
  static HAServerPlugin.RoutingTable selectUnambiguousRouting(final HAServerPlugin.ROUTING_PROTOCOL protocol,
      final String[] addresses, final boolean[] fromConfig, final int count, final Map<String, int[]> claims) {
    if (!identifiesOnePeer(claims.get(addresses[0]), fromConfig[0]))
      return null;

    final List<String> readers = new ArrayList<>(count - 1);
    for (int i = 1; i < count; i++)
      if (identifiesOnePeer(claims.get(addresses[i]), fromConfig[i]))
        readers.add(addresses[i]);

    return new HAServerPlugin.RoutingTable(protocol, addresses[0], List.copyOf(readers));
  }

  /**
   * address -&gt; {peers claiming it, of which declared}. Sized for the group: every caller runs per ROUTE request,
   * per refusal or per snapshot install, never on a data path. Package-private for testing.
   */
  static Map<String, int[]> claimsByAddress(final String[] addresses, final boolean[] fromConfig,
      final int count) {
    final Map<String, int[]> claims = new HashMap<>(count * 2);
    for (int i = 0; i < count; i++) {
      final int[] claim = claims.computeIfAbsent(addresses[i], a -> new int[2]);
      ++claim[0];
      if (fromConfig[i])
        ++claim[1];
    }
    return claims;
  }

  /** True when the address this claim describes belongs to exactly one peer, or to the only one that declared it. */
  private static boolean identifiesOnePeer(final int[] claim, final boolean declared) {
    return claim[0] == 1 || (declared && claim[1] == 1);
  }

  /**
   * Every group peer's HTTP endpoint, each carrying the verdict {@link RaftHAServer#getUnambiguousPeerHttpAddress} would
   * give it, in one pass over the group rather than one pass per peer.
   * <p>
   * The per-peer accessors answer about one peer, and answering that question means resolving <em>all</em> of
   * them to see who else claims the address - so a caller that wants the answer for the whole group, as the
   * cluster-status endpoint does, would resolve the group once per peer and once more for the plain address:
   * O(peers²) for a question that is O(peers). Cluster sizes make that harmless today, which is exactly why it
   * is worth removing before the shape is copied somewhere it is not.
   * <p>
   * A peer whose address cannot be resolved at all is absent from the map rather than present with a
   * {@code null} address: it is the same "nothing to report" an unknown peer produces from the accessors.
   * The claim counting and the declared-beats-derived tie-break are the shared {@code claimsByAddress} /
   * {@code identifiesOnePeer} pair, so this cannot answer differently from the accessors, and an ambiguity
   * seen here trips the same one-time warning.
   */
  public Map<RaftPeerId, PeerHttpEndpoint> getPeerHttpEndpoints() {
    // Every known peer, not the server list alone: a peer that joined at runtime is reported by /api/v1/cluster too, and
    // without an httpAddress it was invisible to anything discovering the nodes there (issue #10861)
    final Collection<RaftPeer> peers = ha.knownPeers();
    final String[] addresses = new String[peers.size()];
    final boolean[] fromConfig = new boolean[addresses.length];
    final RaftPeerId[] owners = new RaftPeerId[addresses.length];
    int resolved = 0;

    for (final RaftPeer peer : peers) {
      final String address = resolveHttpAddress(peer);
      if (address == null)
        continue;
      addresses[resolved] = address;
      fromConfig[resolved] = ha.httpAddresses.containsKey(peer.getId());
      owners[resolved] = peer.getId();
      ++resolved;
    }

    final Map<String, int[]> claims = claimsByAddress(addresses, fromConfig, resolved);
    final Map<RaftPeerId, PeerHttpEndpoint> endpoints = new LinkedHashMap<>(resolved * 2);
    for (int i = 0; i < resolved; i++)
      endpoints.put(owners[i], new PeerHttpEndpoint(addresses[i],
          !identifiesOnePeer(claims.get(addresses[i]), fromConfig[i])));

    // Logs nothing unless something is ambiguous, and then names every collision this pass found. Handed the
    // clean views too: that is what resets the memory, so the same collision reappearing later is reported again.
    warnAmbiguousPeerAddress(false, addresses, fromConfig, owners, resolved, claims);

    return endpoints;
  }

  /**
   * Shared body of the two accessors above: an address is handed out only when the peer asked about is the only
   * one that resolves to it, or the only one that declared it. Parameterized by protocol rather than duplicated,
   * so the HTTPS arm cannot answer a different question from the HTTP one.
   */
  String unambiguousPeerAddress(final RaftPeerId peerId, final boolean https) {
    final String address = https ? resolveHttpsAddress(peerId) : resolveHttpAddress(peerId);
    if (address == null)
      return null;

    final Map<RaftPeerId, String> declared = https ? ha.httpsAddresses : ha.httpAddresses;
    // Every known peer can claim the address, a peer that joined at runtime included (issue #10861).
    final Collection<RaftPeer> peers = ha.knownPeers();
    // Index 0 is always the peer asked about. The +1 is not slack: the peer asked about need not be among the known
    // peers (one removed since the caller read its id), and then it occupies a slot beyond them - the same sizing
    // getRoutingTable uses, for the same reason.
    final String[] addresses = new String[peers.size() + 1];
    final boolean[] fromConfig = new boolean[addresses.length];
    final RaftPeerId[] owners = new RaftPeerId[addresses.length];
    addresses[0] = address;
    fromConfig[0] = declared.containsKey(peerId);
    owners[0] = peerId;
    int resolved = 1;

    for (final RaftPeer peer : peers) {
      if (peer.getId().equals(peerId))
        continue;
      final String other = https ? resolveHttpsAddress(peer) : resolveHttpAddress(peer);
      if (other == null)
        continue;
      addresses[resolved] = other;
      fromConfig[resolved] = declared.containsKey(peer.getId());
      owners[resolved] = peer.getId();
      ++resolved;
    }

    final Map<String, int[]> claims = claimsByAddress(addresses, fromConfig, resolved);
    // Before the verdict on the peer asked about, not after it, and for the same reason getRoutingTable calls it
    // unconditionally: a pass that finds nothing is the only thing that clears the memory, so returning early
    // here left it holding a stale collision that silently swallowed the same one when it came back. It also
    // matches what the warning already claims to be - a report of every collision the pass found, not of the one
    // address the caller happened to ask about - so a healthy peer in an unhealthy cluster now says so.
    warnAmbiguousPeerAddress(https, addresses, fromConfig, owners, resolved, claims);

    if (identifiesOnePeer(claims.get(address), fromConfig[0]))
      return address;
    return null;
  }

  /**
   * Tells the operator, once per protocol, that a peer-to-peer endpoint was <em>withheld</em> - which peers
   * could not be told apart, and what to write to fix it.
   * <p>
   * Without this the refusal is silent (issue #6267): {@link #unambiguousPeerAddress} answers {@code null} and
   * every caller decides for itself whether to say anything, so the misconfiguration is visible only where one
   * happens to log it. Neither existing warning covers it. {@link #deriveHttpAddressWithWarning} and its HTTPS
   * twin fire whenever an address is <em>derived at all</em>, which is also what a perfectly healthy homogeneous
   * Kubernetes StatefulSet does, so they cannot distinguish "deriving, and fine" from "deriving, and two peers
   * just collapsed onto one address"; {@link #warnAmbiguousRouting} says exactly this, but about the client
   * routing tables of issue #6183, not about the peer-to-peer endpoints a resync and a cluster verify dial.
   * <p>
   * Once per <em>verdict</em> rather than once per attempt: the resync and verify paths ask on every attempt, and
   * a misconfiguration that has not changed between attempts should not be re-reported on each one. The memory
   * lives on this node - one per {@code RaftHAServer}, which in a server process means for its lifetime - and it
   * holds the rendered collision list rather than a boolean, so the operator is told again when a
   * <em>different</em> set of peers collides (issue #6297). {@code httpAddressAmbiguous} on
   * {@code GET /api/v1/cluster} remains the always-current view, recomputed per request.
   * <p>
   * Since the one line is all an operator gets, it names <em>every</em> collision this pass found, not just the
   * one the caller asked about: a cluster can have two independent colliding pairs, and reporting one of them
   * would send the operator to declare two ports and leave the other pair withheld. That is also what makes the
   * verdict the right memory key - it is the whole picture, so any change to it is a change the operator has not
   * been told about yet.
   * <p>
   * Both callers hand over every resolved view, ambiguous or not, because a clean pass is the only thing that
   * clears the memory. Package-private for testing.
   */
  void warnAmbiguousPeerAddress(final boolean https, final String[] addresses, final boolean[] fromConfig,
      final RaftPeerId[] owners, final int count, final Map<String, int[]> claims) {
    final String collisions = describeAmbiguity(addresses, fromConfig, owners, count, claims);
    if (!isNewAmbiguityVerdict(https ? httpsAmbiguityReported : httpAmbiguityReported, collisions))
      return;

    final String protocol = https ? "HTTPS" : "HTTP";
    final String field = https ? "https" : "http";
    LogManager.instance().log(ha, Level.WARNING,
        "HA %s peer endpoints are ambiguous: %s. Two listening sockets cannot both own one address, so it identifies "
            + "at most one of the peers that resolve to it and nothing can say which. Their %s endpoint is withheld "
            + "rather than guessed, so a snapshot resync and a cluster verify refuse to dial them and report them "
            + "unverified instead of answering for the wrong node. Declare each node's %s port explicitly with the "
            + "'host:{raft:..,%s:..}' object syntax in %s.",
        protocol, collisions, protocol, protocol, field, GlobalConfiguration.HA_SERVER_LIST.getKey());
  }

  /**
   * Tells the operator which peers shared a routing address and what to write to fix it, on the same
   * report-when-the-verdict-changes contract as its peer-to-peer twin above (issue #6297).
   * <p>
   * Called on <em>every</em> resolved view, ambiguous or not, because a clean pass is the only thing that clears
   * the memory. Package-private for testing.
   */
  void warnAmbiguousRouting(final HAServerPlugin.ROUTING_PROTOCOL protocol, final String[] addresses,
      final boolean[] fromConfig, final RaftPeerId[] owners, final int count, final Map<String, int[]> claims) {
    final String collisions = describeAmbiguity(addresses, fromConfig, owners, count, claims);
    if (!isNewAmbiguityVerdict(routingAmbiguityReported.get(protocol), collisions))
      return;

    final String field = routingConfigField(protocol);
    LogManager.instance().log(ha, Level.WARNING,
        "HA %s routing is ambiguous: %s. Two listening sockets cannot both own one address, so the peers that "
            + "cannot be told apart are not advertised, and nothing is advertised at all when the leader is one of "
            + "them. Declare each node's %s port explicitly with the 'host:{raft:..,%s:..}' object syntax in %s.",
        protocol.name(), collisions, protocol.name(), field, GlobalConfiguration.HA_SERVER_LIST.getKey());
  }

  /**
   * Renders every address in a resolved view that fails to identify exactly one peer, as
   * {@code "peerA, peerB -> host:port; peerC, peerD -> host:port2"}, or the empty string when the view is clean.
   * <p>
   * A peer that DECLARED an address others merely derive to keeps it (declared beats derived), so it is not among
   * them and must not be named as withheld. Sorted, by address and then by peer, because the group arrives in
   * whatever order Ratis holds it: the operator gets the same line for the same misconfiguration whichever node
   * logs it, which is also what lets the line double as the memory key in {@link #isNewAmbiguityVerdict}.
   * Package-private for testing.
   */
  static String describeAmbiguity(final String[] addresses, final boolean[] fromConfig, final RaftPeerId[] owners,
      final int count, final Map<String, int[]> claims) {
    final Map<String, List<String>> withheld = new TreeMap<>();
    for (int i = 0; i < count; i++) {
      if (identifiesOnePeer(claims.get(addresses[i]), fromConfig[i]))
        continue;
      withheld.computeIfAbsent(addresses[i], a -> new ArrayList<>()).add(String.valueOf(owners[i]));
    }

    // The common case on a correctly declared cluster, and getPeerHttpEndpoints() asks on every request: answer it
    // without building a renderer that would render nothing.
    if (withheld.isEmpty())
      return "";

    final StringBuilder collisions = new StringBuilder();
    for (final Map.Entry<String, List<String>> collision : withheld.entrySet()) {
      Collections.sort(collision.getValue());
      if (!collisions.isEmpty())
        collisions.append("; ");
      collisions.append(String.join(", ", collision.getValue())).append(" -> ").append(collision.getKey());
    }
    return collisions.toString();
  }

  /**
   * Whether {@code collisions} - one ambiguity pass rendered by {@link #describeAmbiguity} - says something the
   * operator has not already been told through {@code lastReported}, which it updates.
   * <p>
   * The point of issue #6297: a plain "log this once" latch is right until the operator acts on it. They read the
   * line, declare the two ports it named, and a different pair of peers - or a peer added at runtime onto an
   * address already taken - now collides with nothing logged, because the latch is spent. Keying on the verdict
   * instead of on a membership-change event is both stricter and looser in the directions that matter: a
   * membership change that leaves the verdict identical says nothing new and stays quiet, and any change to the
   * verdict is reported whatever produced it. A pass that finds no collision clears the memory rather than
   * reporting, so a misconfiguration that is fixed and later reintroduced is announced again.
   * <p>
   * Deliberately a plain {@code getAndSet} and not a {@code compareAndSet} loop. Two threads arriving with two
   * DIFFERENT verdicts can both be told "changed" and both log; that is a duplicate warning about a
   * misconfiguration that really did change, which is the outcome this method exists to produce, and a CAS loop
   * would only decide which of the two the operator is not told about. The old {@code AtomicBoolean} had the same
   * race with a worse shape - the loser was silenced permanently rather than for one pass.
   * <p>
   * Package-private for testing.
   */
  static boolean isNewAmbiguityVerdict(final AtomicReference<String> lastReported, final String collisions) {
    if (collisions.isEmpty()) {
      lastReported.set(null);
      return false;
    }
    return !collisions.equals(lastReported.getAndSet(collisions));
  }

  /**
   * Resolves the HTTP address (host:port) of a peer. When the peer's HTTP port was declared
   * explicitly in {@link GlobalConfiguration#HA_SERVER_LIST} (the {@code host:raftPort:httpPort}
   * syntax) that value is returned. Otherwise a best-effort address is synthesized by combining the
   * peer's Raft host with <em>this</em> node's HTTP listening port. The fallback is correct only for
   * homogeneous deployments where every node listens on the same HTTP port (e.g. a Kubernetes
   * StatefulSet); a one-time WARNING is logged so operators can declare explicit HTTP ports when the
   * cluster is heterogeneous. Returns {@code null} when the peer is unknown or this node's HTTP port
   * is not yet available.
   */
  String resolveHttpAddress(final RaftPeerId peerId) {
    if (peerId == null)
      return null;
    final String configured = ha.httpAddresses.get(peerId);
    if (configured != null)
      return configured;
    return deriveHttpAddressWithWarning(ha.peerRaftAddress(peerId));
  }

  /** Overload that avoids the peer lookup when the {@link RaftPeer} is already in hand. */
  String resolveHttpAddress(final RaftPeer peer) {
    final String configured = ha.httpAddresses.get(peer.getId());
    if (configured != null)
      return configured;
    return deriveHttpAddressWithWarning(peer.getAddress());
  }

  /**
   * Resolves the client-reachable address (host:port) of a peer for one client protocol. Returns the address
   * declared with the matching object-form field ({@code bolt:} / {@code grpc:}) when present; otherwise
   * derives it from the peer's Raft host plus <em>this</em> node's local port for that protocol. The fallback
   * is correct only for homogeneous deployments where every node listens on the same port (e.g. a Kubernetes
   * StatefulSet); a one-time WARNING per protocol is logged so operators declare explicit ports for
   * heterogeneous clusters. Returns {@code null} when the peer is unknown or the local port is unavailable.
   *
   * @param declared the declared-address map for this protocol, passed in so the caller and this method cannot
   *                 disagree about which peers were declared
   */
  private String resolveRoutingAddress(final HAServerPlugin.ROUTING_PROTOCOL protocol,
      final Map<RaftPeerId, String> declared, final RaftPeerId peerId) {
    if (peerId == null)
      return null;
    final String configured = declared.get(peerId);
    if (configured != null)
      return configured;
    return deriveRoutingAddressWithWarning(protocol, ha.peerRaftAddress(peerId));
  }

  /** Overload that avoids the peer lookup when the {@link RaftPeer} is already in hand. */
  private String resolveRoutingAddress(final HAServerPlugin.ROUTING_PROTOCOL protocol,
      final Map<RaftPeerId, String> declared, final RaftPeer peer) {
    final String configured = declared.get(peer.getId());
    if (configured != null)
      return configured;
    return deriveRoutingAddressWithWarning(protocol, peer.getAddress());
  }

  private String deriveRoutingAddressWithWarning(final HAServerPlugin.ROUTING_PROTOCOL protocol, final String raftAddress) {
    final int localPort = localRoutingPort(protocol);
    final String derived = deriveRoutingAddress(raftAddress, localPort);
    if (derived != null && routingFallbackWarned.get(protocol).compareAndSet(false, true)) {
      final String field = routingConfigField(protocol);
      LogManager.instance().log(ha, Level.WARNING,
          "HA %s routing addresses are not configured in '%s': deriving every peer's %s endpoint from its Raft host plus this node's "
              + "own %s port (%d). That is correct only when every node listens on the same port (e.g. a Kubernetes StatefulSet); on a "
              + "cluster with heterogeneous ports, declare them explicitly with the 'host:{raft:..,%s:..}' object syntax in %s.",
          protocol.name(), GlobalConfiguration.HA_SERVER_LIST.getKey(), protocol.name(), protocol.name(), localPort,
          field, GlobalConfiguration.HA_SERVER_LIST.getKey());
    }
    return derived;
  }

  /** The explicitly declared addresses for one client protocol, as parsed from {@code HA_SERVER_LIST}. */
  private Map<RaftPeerId, String> declaredRoutingAddresses(final HAServerPlugin.ROUTING_PROTOCOL protocol) {
    return switch (protocol) {
      case BOLT -> ha.boltAddresses;
      case GRPC -> ha.grpcAddresses;
    };
  }

  /** This node's own listening port for one client protocol, used by the derive fallback. */
  private int localRoutingPort(final HAServerPlugin.ROUTING_PROTOCOL protocol) {
    return switch (protocol) {
      case BOLT -> ha.configuration.getValueAsInteger(GlobalConfiguration.BOLT_PORT);
      case GRPC -> ha.configuration.getValueAsInteger(GlobalConfiguration.GRPC_PORT);
    };
  }

  /** The {@code HA_SERVER_LIST} object-form field an operator writes to declare this protocol's port. */
  private static String routingConfigField(final HAServerPlugin.ROUTING_PROTOCOL protocol) {
    return protocol.name().toLowerCase(Locale.ENGLISH);
  }

  /**
   * Derives a client-reachable address (host:port) by combining a peer's Raft host with the given port.
   * Returns {@code null} when the port is not positive or the host cannot be extracted. Package-private
   * for testing.
   */
  static String deriveRoutingAddress(final String raftAddress, final int port) {
    if (raftAddress == null || port <= 0)
      return null;
    final String host = extractHost(raftAddress);
    return host != null ? host + ":" + port : null;
  }

  private String deriveHttpAddressWithWarning(final String raftAddress) {
    if (raftAddress == null)
      return null;
    final HttpServer httpServer = ha.arcadeServer.getHttpServer();
    final int httpPort = httpServer != null ? httpServer.getPort() : -1;
    final String derived = deriveHttpAddress(raftAddress, httpPort);
    if (derived != null && httpFallbackWarned.compareAndSet(false, true))
      LogManager.instance().log(ha, Level.WARNING,
          "HA HTTP addresses are not configured in '%s': deriving peer HTTP endpoints from each peer's Raft host plus this node's HTTP port (%d). "
              + "This is correct only when every node listens on the same HTTP port (e.g. a Kubernetes StatefulSet). For clusters with heterogeneous "
              + "HTTP ports, declare them explicitly using the 'host:raftPort:httpPort' syntax in %s.",
          GlobalConfiguration.HA_SERVER_LIST.getKey(), httpPort, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return derived;
  }

  /**
   * Resolves the HTTPS address (host:httpsPort) of a peer for encrypted snapshot transfer. Prefers
   * the explicit HTTPS endpoint from the server list (5th field); otherwise derives it from the
   * peer's Raft host plus this node's local HTTPS listening port. Returns {@code null} when SSL is
   * disabled (no local HTTPS listener), the peer is unknown, or the port is not yet available.
   */
  String resolveHttpsAddress(final RaftPeerId peerId) {
    if (peerId == null)
      return null;
    final String configured = ha.httpsAddresses.get(peerId);
    if (configured != null)
      return configured;
    return deriveHttpsAddressWithWarning(ha.peerRaftAddress(peerId));
  }

  /** Overload that avoids the peer lookup when the {@link RaftPeer} is already in hand. */
  String resolveHttpsAddress(final RaftPeer peer) {
    final String configured = ha.httpsAddresses.get(peer.getId());
    if (configured != null)
      return configured;
    return deriveHttpsAddressWithWarning(peer.getAddress());
  }

  private String deriveHttpsAddressWithWarning(final String raftAddress) {
    if (raftAddress == null)
      return null;
    final HttpServer httpServer = ha.arcadeServer.getHttpServer();
    final int httpsPort = httpServer != null ? httpServer.getHttpsPort() : -1;
    if (httpsPort <= 0)
      return null; // SSL disabled or no HTTPS listener: caller falls back to plain HTTP
    final String derived = deriveHttpAddress(raftAddress, httpsPort);
    if (derived != null && httpsFallbackWarned.compareAndSet(false, true))
      LogManager.instance().log(ha, Level.INFO,
          "HA HTTPS endpoints are not configured in '%s': deriving peer HTTPS endpoints for encrypted snapshot transfer "
              + "from each peer's Raft host plus this node's HTTPS port (%d). This is correct only when every node listens on the "
              + "same HTTPS port (e.g. a Kubernetes StatefulSet). For heterogeneous clusters, declare them explicitly using the "
              + "'host:raftPort:httpPort:priority:httpsPort' syntax in %s.",
          GlobalConfiguration.HA_SERVER_LIST.getKey(), httpsPort, GlobalConfiguration.HA_SERVER_LIST.getKey());
    return derived;
  }

  /**
   * Combines the host of a Raft address with the given HTTP port. Returns {@code null} when the port
   * is not yet known ({@code <= 0}) or the host cannot be extracted. Package-private for testing.
   */
  static String deriveHttpAddress(final String raftAddress, final int httpPort) {
    if (httpPort <= 0)
      return null;
    final String host = extractHost(raftAddress);
    return host != null ? host + ":" + httpPort : null;
  }

  /**
   * A <b>candidate</b> HTTP endpoint for a peer whose derived address collapsed onto this node's own, obtained by
   * carrying the peer's Raft-port offset over to the HTTP port (issue #7332).
   * <p>
   * It exists because the collapse is total on the very deployment shape the shared-endpoint recovery was written
   * for. With no {@code http} port declared, {@link #resolveHttpAddress(RaftPeerId)} derives every peer's endpoint
   * as <em>its</em> Raft host plus <em>this</em> node's HTTP port; on a cluster whose nodes differ by port rather
   * than by host that address is this node's own for every peer, so {@code sharedEndpointOf} withheld it - rightly,
   * dialling ourselves comes straight back - and the second pass never had an endpoint to probe at all.
   * <p>
   * The offset is the only signal left: a multi-node cluster on one host is configured by moving both ports
   * together ({@code 2424/2480}, {@code 2425/2481}, {@code 2426/2482} is what the docs, the examples and the test
   * fixtures all use), so {@code localHttpPort + (peerRaftPort - localRaftPort)} names the peer's listener whenever
   * that convention holds and names nothing at all when it does not.
   * <p>
   * <b>Only the capability probe's second pass may use it, and that is what makes a guess acceptable here.</b>
   * Every other caller acts on the peer it addressed and must never dial an address that might belong to someone
   * else. The second pass does not: it is read-only, its reply names its own author, and it credits the answer to
   * whoever actually answered - a peer of the current configuration, never this node, never one that already
   * answered for itself. A wrong guess therefore reaches a socket that is not in the cluster and fails, or reaches
   * one that is and is credited correctly. It is offered through {@link PeerDialAddress#sharedEndpoint()} for that
   * reason, and never through {@link PeerDialAddress#httpAddress()}.
   * <p>
   * {@code null} - no candidate - when the peer declared its HTTP port (there is nothing to guess, and a declared
   * address that still collides is a configuration fault to report rather than to work around), when either Raft
   * port is unknown, when the two Raft ports are equal (the peers differ by host, so the derivation did not
   * collapse them in the first place), when the arithmetic leaves the port range, or when the result is this
   * node's own address after all.
   */
  public String getPortOffsetPeerHttpAddress(final RaftPeerId peerId) {
    if (peerId == null || peerId.equals(ha.localPeerId) || ha.httpAddresses.containsKey(peerId))
      return null;

    final String peerRaft = ha.peerRaftAddress(peerId);
    final int peerRaftPort = extractPort(peerRaft);
    final int localRaftPort = extractPort(ha.peerRaftAddress(ha.localPeerId));
    if (peerRaftPort <= 0 || localRaftPort <= 0 || peerRaftPort == localRaftPort)
      return null;

    final HttpServer httpServer = ha.arcadeServer.getHttpServer();
    final int localHttpPort = httpServer != null ? httpServer.getPort() : -1;
    if (localHttpPort <= 0)
      return null;

    // long because extractPort returns whatever integer the address carries, port range or not: a peer whose
    // address ends in 2147483647 would wrap int arithmetic round to a small, plausible-looking port. Nothing
    // between here and there validates it, so the range check below is done in a type that cannot wrap first.
    final long candidatePort = (long) localHttpPort + peerRaftPort - localRaftPort;
    if (candidatePort <= 0 || candidatePort > 65535)
      return null;

    final String host = extractHost(peerRaft);
    if (host == null)
      return null;

    final String candidate = host + ":" + candidatePort;
    return RaftHAServer.isSameHttpEndpoint(ha.getLocalHttpAddress(), candidate) ? null : candidate;
  }

  /**
   * Extracts the port from a {@code host:port} or {@code [ipv6]:port} address, or {@code -1} when the address
   * carries none, is blank, or the port is not a number. The mirror of {@link #extractHost}, and it takes the
   * LAST colon for the same reason: an unbracketed IPv6 literal carries colons of its own. Package-private for
   * testing.
   */
  static int extractPort(final String address) {
    if (address == null || address.isEmpty())
      return -1;
    final int closeBracket = address.lastIndexOf(']');
    final int colon = address.lastIndexOf(':');
    if (colon <= 0 || colon < closeBracket)
      return -1;
    try {
      return Integer.parseInt(address.substring(colon + 1));
    } catch (final NumberFormatException e) {
      return -1;
    }
  }

  /**
   * Extracts the host portion from a {@code host:port} or {@code [ipv6]:port} address. Returns the
   * input unchanged when it carries no port, or {@code null} when blank. Package-private for testing.
   */
  static String extractHost(final String address) {
    if (address == null || address.isEmpty())
      return null;
    final int closeBracket = address.lastIndexOf(']');
    if (closeBracket >= 0)
      return address.substring(0, closeBracket + 1); // [ipv6] literal, strip any :port after the bracket
    final int colon = address.lastIndexOf(':');
    return colon > 0 ? address.substring(0, colon) : address;
  }
}
