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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.Grpc;
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Rejects inbound Raft gRPC transports whose remote IP does not resolve to one of the hosts
 * declared in {@code arcadedb.ha.serverList}. This closes the "any host that knows the port
 * can inject log entries" attack without requiring certificate provisioning.
 * <p>
 * The allowlist is recomputed lazily from DNS whenever an inbound connection from an unknown
 * address arrives, rate-limited by {@code refreshIntervalMs}, so that Kubernetes pod-IP churn
 * on restart does not permanently lock out a restarted peer.
 * <p>
 * To avoid a self-inflicted partition during the window where peer DNS is not yet usable
 * (issue #4471 - on Kubernetes a headless-service A record is only published once a pod is
 * Ready, so peers come up before each other's names resolve), the filter is hardened in three
 * ways:
 * <ul>
 *   <li><b>Bypass the rate limit while the allowlist is incomplete.</b> Until every peer host has
 *       resolved at least once, a miss re-resolves on a short floor instead of waiting the full
 *       {@code refreshIntervalMs}, so the allowlist converges quickly at startup.</li>
 *   <li><b>Sticky last-known-good IPs.</b> When a host that resolved before fails to resolve now
 *       (transient DNS outage, pod-IP churn mid-restart), its previous IPs are retained for
 *       {@code stickyTtlMs} rather than being evicted immediately.</li>
 *   <li><b>Startup fail-open grace.</b> Until a quorum (majority) of peer hosts has resolved, and for
 *       at most {@code startupGraceMs} from creation, an unmatched address is accepted with a
 *       warning instead of rejected. After a quorum has resolved once, or the window elapses,
 *       the filter enforces normally. The gate is a quorum rather than the full peer set (issue
 *       #4828): if one pod is permanently down at startup its DNS record never publishes, so an
 *       all-peers latch would never trip and the fail-open branch would stay active for the entire
 *       configured window. A Raft majority is enough to form the cluster, so once that many peers
 *       are known the allowlist is functional and fail-open ends - a returning peer's new IP is then
 *       picked up by the reactive miss path and the background {@link #proactiveRefresh()}.</li>
 * </ul>
 * <p>
 * The set of hosts is NOT frozen at boot (issue #7132). The hosts declared in {@code serverList} are the
 * <i>configured</i> ones and are the only ones the quorum and completeness latches above count, but a
 * caller may add more at runtime, and {@code RaftHAServer} does so on every health monitor tick from the
 * live Raft configuration. Without that, a peer that joined after this node started - through
 * {@code addPeer}, or through the Kubernetes StatefulSet scale-up auto-join of issue #4836 - was rejected
 * forever by every node that had already latched {@code everQuorumResolved}: the two features are
 * individually correct and were jointly broken, because nothing reconciled them. Learned hosts resolve
 * exactly like configured ones but are best-effort: a name that does not resolve contributes nothing,
 * is logged at FINE rather than WARNING, and never holds the latches back.
 * <p>
 * Runtime hosts arrive through two APIs with deliberately different lifetimes (issue #7225), because the
 * first version of #7132 had only the first and a host once learned was never unlearned - so a peer removed
 * from the cluster kept inbound Raft access, and a DNS lookup per refresh tick, for the rest of the process
 * lifetime:
 * <ul>
 *   <li>{@link #learnPeerHosts} <b>pins</b> a host for the life of the filter. Its one production caller
 *       seeds the Kubernetes headless-service domain, which is what admits a scale-up pod <i>before</i> it
 *       is a member and therefore must survive every membership shrink.</li>
 *   <li>{@link #setMemberHosts} <b>replaces</b> the membership-derived hosts wholesale with the hosts of the
 *       live Raft configuration, so a departed peer's IPs leave {@code allowedIps} - and its sticky
 *       last-known-good entries leave the retention maps - on the next reconciliation.</li>
 * </ul>
 * Neither ever shadows a configured host: {@code serverList} is configuration, not membership, and removing
 * a host from it is a configuration change rather than something a Raft configuration commit can do.
 * <p>
 * <b>The pin does not undo the unlearning</b> (issue #7302). The exemption above is what made it possible for it
 * to: on Kubernetes the pinned host is the headless service, whose A records are every pod backing the
 * StatefulSet - including one that is not Ready, since the service that publishes it sets
 * {@code publishNotReadyAddresses}. Kubernetes drops a pod's address when the POD terminates, not when Raft
 * removes it from the configuration, so expanding the pinned domain into the same set as the membership hosts
 * re-added a deliberately-removed-but-still-running pod's address on every resolve, and the INFO line written to
 * confirm the revocation reported one that had not happened on the one deployment shape this project targets.
 * So {@link #doResolve()} expands the pinned domain separately and subtracts the addresses of the peers the
 * membership dropped, until either the peer is admitted again for some other reason or the address stops being
 * published - at which point the pod is gone and a later reuse of the address belongs to a different one.
 * <p>
 * <b>The subtraction can only hold back an address it can name.</b> A departing peer's addresses come from its
 * last successful resolution, or from one lookup made as it leaves; a peer whose name has never resolved and
 * does not resolve now leaves nothing to subtract, and the pinned domain goes on admitting its pod until that pod
 * terminates. That needs DNS to have been down for the whole time the peer was a member AND at the moment it is
 * removed, which is why it is stated rather than worked around: any workaround would have to guess which of the
 * pinned domain's addresses belonged to the departing peer, and guessing wrong locks out a healthy pod.
 * <p>
 * Unlearning a host stops it being <i>admitted</i>, and since issue #7250 it also revokes what an
 * already-<i>established</i> transport of that host can still do. {@code ServerTransportFilter} is a one-shot
 * admission gate and gRPC hands it no handle on the transport it admitted, so every transport this filter admits
 * gets a {@link PeerTransportSession} stashed in its {@code Attributes}; a resolution that stops admitting the
 * session's address revokes it, {@link PeerAllowlistCallInterceptor} then refuses every further RPC on it, and the
 * RPCs already running on it are closed. The socket is not closed on the spot - gRPC's public API exposes no way to
 * close one established transport on demand - but it is no longer left to the peer either: since issue #7316 the
 * Raft listener carries {@code arcadedb.ha.grpcMaxConnectionIdleMs}, so a connection that stops carrying RPCs, which
 * a revoked one does as soon as it stops retrying, is closed with a graceful GOAWAY. A revoked peer that keeps
 * retrying never stops, and its refused RPCs push that window forward indefinitely; for that case issue #7339 adds
 * {@code arcadedb.ha.grpcMaxConnectionAgeMs}, an unconditional bound on the connection's whole life, off by default
 * because it recycles healthy connections too. The accurate operator-facing claim is that removing a peer revokes
 * its reach immediately and its connection once that connection falls idle - or, with the age window configured, no
 * later than that window whether it falls idle or not.
 * <p>
 * This is NOT a substitute for mTLS: it does not authenticate peer identity and does not
 * encrypt the traffic. See GitHub issue #3890. The bounded startup fail-open is an acceptable
 * trade-off for that reason; set {@code startupGraceMs=0} to disable it.
 */
final class PeerAddressAllowlistFilter extends ServerTransportFilter {

  // Minimum spacing between miss-triggered re-resolutions. Bounds DNS load under a connection flood
  // (at startup or from a non-peer) while letting the allowlist converge within ~1s.
  private static final long        MISS_RESOLVE_FLOOR_MS = 1_000L;

  // The session this filter attaches to every transport it admits, so PeerAllowlistCallInterceptor can find it again
  // on each RPC through ServerCall.getAttributes() - the transport attributes returned below are merged into it
  // (issue #7250).
  static final Attributes.Key<PeerTransportSession> TRANSPORT_SESSION =
      Attributes.Key.create("com.arcadedb.server.ha.raft.PeerAddressAllowlistFilter.transportSession");

  // Hosts declared in arcadedb.ha.serverList. Immutable, and the ONLY ones the quorum/completeness
  // latches below count: they describe the configured cluster, which is what #4828 reasoned about.
  private final List<String>                 peerHosts;
  // Hosts pinned after construction and never unlearned (issue #7132): on Kubernetes, the headless service
  // domain behind arcadedb.ha.k8sSuffix, which resolves to every pod of the StatefulSet including one that
  // is not Ready yet. Deliberately outside the membership reconciliation below.
  private volatile Set<String>               pinnedHosts  = Collections.emptySet();
  // Hosts of the live Raft configuration, REPLACED wholesale on every reconciliation (issue #7225) so a peer
  // removed from the cluster stops being resolved and stops being admitted.
  private volatile Set<String>               memberHosts  = Collections.emptySet();
  // pinnedHosts | memberHosts, republished whenever either changes. Reporting only: what an operator is shown
  // and what the tests read. Copy-on-write so nothing synchronises against a learner to read it.
  private volatile Set<String>               learnedHosts = Collections.emptySet();
  // Addresses of peers deliberately removed from the Raft configuration that a PINNED host still publishes, i.e.
  // the pods of a Kubernetes StatefulSet that are still running (issue #7302). Subtracted from the pinned
  // expansion in doResolve(), and dropped again as soon as the pinned domain stops publishing the address - at
  // which point the pod is gone and a later reuse of that address belongs to a different one. Mutated only under
  // this object's monitor, like the sticky maps below.
  private final Set<String>                  revokedPinnedIps = new HashSet<>();
  // Number of declared peer hosts that must resolve before the startup fail-open ends: a Raft
  // majority (floor(n/2)+1). Enough peers to form the cluster, so a single permanently-down peer
  // no longer holds the fail-open window open for its full duration (issue #4828).
  private final int                          resolveQuorum;
  private final long                         refreshIntervalMs;
  private final long                         startupGraceMs;
  private final long                         stickyTtlMs;
  private final long                         createdMs;
  private final LongSupplier                 clock;
  private final HostResolver                 resolver;
  private final AtomicReference<Set<String>> allowedIps = new AtomicReference<>(Collections.emptySet());
  // Per-host last successfully-resolved IPs and the time they were resolved, for sticky retention.
  // Only mutated inside the synchronized doResolve(); never read outside it.
  private final Map<String, Set<String>>     lastKnownIps = new HashMap<>();
  private final Map<String, Long>            lastKnownMs  = new HashMap<>();
  private volatile long                      lastResolveMs;
  // Latches true the first time every peer host is covered by the allowlist; drives the fast-converge
  // resolve floor so the still-missing minority is re-resolved aggressively at startup.
  private volatile boolean                   everCompletelyResolved;
  // Latches true the first time a quorum of peer hosts is covered; gates the fail-open grace so the
  // window ends as soon as a Raft majority is known, even if a peer is permanently down (issue #4828).
  private volatile boolean                   everQuorumResolved;
  // How many CONFIGURED hosts the last resolution covered. Reported by the startup fail-open message, which
  // used to count lastKnownIps - a map keyed by configured AND learned hosts, so the message could claim more
  // resolved hosts than there are configured ones (issue #7225). Also removes that message's unsynchronised
  // read of a plain HashMap.
  private volatile int                       resolvedPeerHosts;
  // The transports this filter has admitted, so a host that stops being admitted can have its established
  // transports revoked too (issue #7250). Entries are added by transportReady and removed by transportTerminated,
  // so nothing here outlives the transport it describes.
  private final Set<PeerTransportSession>    sessions           = ConcurrentHashMap.newKeySet();
  // Sessions marked revoked by the last resolution and not yet cut. doResolve() runs under this object's monitor and
  // must not call into gRPC while holding it, so it only flips the flag - which is what stops the NEXT RPC - and
  // leaves the closing of the in-flight ones to dispatchRevocations() outside the lock.
  private final Queue<PeerTransportSession>  pendingRevocations = new ConcurrentLinkedQueue<>();

  /**
   * Convenience form for a caller with no server configuration in reach - the tests; {@code RaftHAServer} passes
   * both windows explicitly, read from the server's own configuration. The empty {@link ContextConfiguration} says
   * exactly that: both settings are SCOPE.SERVER, so reading them off the {@link GlobalConfiguration} enum would
   * have been reading a value only a system property or an environment variable can have written (issue #7233).
   */
  PeerAddressAllowlistFilter(final List<String> peerHosts, final long refreshIntervalMs) {
    this(peerHosts, refreshIntervalMs, new ContextConfiguration());
  }

  PeerAddressAllowlistFilter(final List<String> peerHosts, final long refreshIntervalMs,
      final ContextConfiguration configuration) {
    this(peerHosts, refreshIntervalMs,
        configuration.getValueAsLong(GlobalConfiguration.HA_PEER_ALLOWLIST_STARTUP_GRACE_MS),
        configuration.getValueAsLong(GlobalConfiguration.HA_PEER_ALLOWLIST_STICKY_TTL_MS));
  }

  PeerAddressAllowlistFilter(final List<String> peerHosts, final long refreshIntervalMs, final long startupGraceMs,
      final long stickyTtlMs) {
    this(peerHosts, refreshIntervalMs, startupGraceMs, stickyTtlMs, System::currentTimeMillis, InetAddress::getAllByName);
  }

  /** Full constructor; the {@code clock} and {@code resolver} hooks make resolution deterministic in tests. */
  PeerAddressAllowlistFilter(final List<String> peerHosts, final long refreshIntervalMs, final long startupGraceMs,
      final long stickyTtlMs, final LongSupplier clock, final HostResolver resolver) {
    if (peerHosts == null || peerHosts.isEmpty())
      throw new IllegalArgumentException("Peer allowlist requires at least one host");
    this.peerHosts = List.copyOf(peerHosts);
    this.resolveQuorum = this.peerHosts.size() / 2 + 1;
    this.refreshIntervalMs = Math.max(0L, refreshIntervalMs);
    this.startupGraceMs = Math.max(0L, startupGraceMs);
    this.stickyTtlMs = Math.max(0L, stickyTtlMs);
    this.clock = clock;
    this.resolver = resolver;
    this.createdMs = clock.getAsLong();
    doResolve();
  }

  @Override
  public Attributes transportReady(final Attributes attrs) {
    final SocketAddress remote = attrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (!(remote instanceof InetSocketAddress inet))
      return attrs;

    final InetAddress address = inet.getAddress();
    if (address == null)
      return attrs;

    if (address.isLoopbackAddress())
      return attrs;

    final String ip = address.getHostAddress();

    // Register BEFORE deciding, not after (issue #7250). isAllowed() can re-resolve, and a reconciliation on another
    // thread can revoke in the middle of it; a session added afterwards would be invisible to that sweep and the
    // transport would be admitted with nothing left to revoke it. Registering first inverts the race into one this
    // method can see and refuse: whatever happens while the decision runs, the session is either still admitted or
    // has been marked revoked, and both are checked below.
    final PeerTransportSession session = new PeerTransportSession(ip);
    sessions.add(session);
    final boolean allowed;
    try {
      allowed = isAllowed(ip);
    } catch (final RuntimeException e) {
      sessions.remove(session);
      throw e;
    }
    if (!allowed || session.isRevoked()) {
      sessions.remove(session);
      throw new SecurityException("Remote address '" + ip + "' is not in the cluster peer allowlist");
    }

    session.markAdmitted();
    return Attributes.newBuilder(attrs).set(TRANSPORT_SESSION, session).build();
  }

  /**
   * Drops the session of a transport gRPC has torn down (issue #7250). This is the other half of the lifetime
   * contract the session's javadoc states: without it {@link #sessions} would grow by one entry per connection for
   * the life of the process, and the in-flight calls of a dead transport would be walked by every revocation.
   */
  @Override
  public void transportTerminated(final Attributes attrs) {
    final PeerTransportSession session = attrs == null ? null : attrs.get(TRANSPORT_SESSION);
    if (session == null)
      return;
    sessions.remove(session);
    session.forgetCalls();
  }

  /**
   * Decides whether {@code ip} may connect, applying the incomplete-allowlist bypass and the
   * startup fail-open grace (issue #4471). Package-private so it can be unit-tested without
   * constructing gRPC transport objects. Logs a warning on both fail-open and reject.
   */
  boolean isAllowed(final String ip) {
    if (ip == null)
      return true; // address not available; cannot evaluate, leave to other layers
    if (allowedIps.get().contains(ip))
      return true;

    // Miss: re-resolve to pick up restarted peers with new IPs, always on the short floor (issue
    // #5268). On Kubernetes a connection from an unknown IP is the NORMAL case for a recreated pod,
    // not an intrusion - and the DNS-gap resolutions during the pod recreation keep bumping the
    // last-resolve timestamp, so gating this path on the full refreshIntervalMs made the recreated
    // peer's one-shot join probe lose the race essentially every time. The floor still bounds DNS
    // load under a connection flood; the steady-state interval remains in force for the proactive
    // background refresh.
    resolveIfStale(missResolveFloor());
    if (allowedIps.get().contains(ip))
      return true;

    // Startup fail-open: a quorum of peers has never resolved and we are still within the grace
    // window. Accept rather than partition the cluster against itself while DNS catches up. The gate
    // is a quorum, not the full peer set, so a single permanently-down peer does not hold the window
    // open for its full duration (issue #4828).
    final long now = clock.getAsLong();
    if (!everQuorumResolved && startupGraceMs > 0 && now - createdMs < startupGraceMs) {
      LogManager.instance().log(this, Level.WARNING,
          "Accepting Raft gRPC connection from %s during startup grace: peer allowlist below quorum "
              + "(resolved %d/%d hosts, quorum=%d, allowed=%s). Will enforce once a quorum of peers resolves or after %dms.",
          ip, resolvedPeerHosts, peerHosts.size(), resolveQuorum, allowedIps.get(), startupGraceMs);
      return true;
    }

    // Named settings and the full host list, because this is logged on the RECEIVING node: an operator
    // looking at a peer that will not join sees nothing at all in ITS log (issue #7132).
    LogManager.instance().log(this, Level.WARNING,
        "Rejecting Raft gRPC connection from %s: the address is not in the cluster peer allowlist "
            + "(allowed=%s, configured hosts=%s, members of the live Raft configuration=%s, pinned hosts=%s). "
            + "If this is a legitimate peer, add it to 'arcadedb.ha.serverList' or check that its hostname "
            + "resolves from this node; 'arcadedb.ha.peerAllowlist.enabled=false' disables the check entirely. "
            + "A peer removed from the Raft configuration is expected to appear here.",
        ip, allowedIps.get(), peerHosts, memberHosts, pinnedHosts);
    return false;
  }

  /**
   * A snapshot of the transports currently admitted by this filter. Exposed for testing, and a copy rather than an
   * unmodifiable view of the live set so that a test reading it twice compares two stable values instead of racing
   * a transport that connected in between.
   */
  Set<PeerTransportSession> getSessions() {
    return Set.copyOf(sessions);
  }

  /** Returns an immutable snapshot of the currently allowed IPs. Exposed for testing. */
  Set<String> getAllowedIps() {
    return allowedIps.get();
  }

  /** True once every peer host has been covered by the allowlist at least once. Exposed for testing. */
  boolean isEverCompletelyResolved() {
    return everCompletelyResolved;
  }

  /** True once a quorum (majority) of peer hosts has been covered at least once. Exposed for testing. */
  boolean isQuorumResolved() {
    return everQuorumResolved;
  }

  /**
   * Pins hosts discovered after construction to the allowlist and re-resolves immediately when any of them is
   * new (issue #7132). A pinned host is never unlearned: the one production caller is
   * {@code RaftHAServer.installPeerAllowlist}, which seeds the Kubernetes headless service domain, and that
   * domain is precisely how a scale-up pod is admitted BEFORE it is a member of the Raft configuration - so it
   * cannot be subject to the membership reconciliation in {@link #setMemberHosts}. Idempotent: a call that
   * pins nothing new costs one set comparison and does not touch DNS.
   * <p>
   * <b>A pin beats membership, deliberately.</b> Pinning a host that is also a current member keeps it
   * admitted after that member leaves, which is the one way to defeat the unlearning this class exists to do.
   * The alternative - silently refusing to pin a host that happens to be a member today - is worse: the pin
   * would then evaporate at the next shrink, which is exactly when the caller wanted it. So the contract is
   * that a pin is permanent, and a caller that does not want permanence calls {@link #setMemberHosts}
   * instead. Nothing in the tree pins a peer hostname: {@code grep -rn "learnPeerHosts" --exclude-dir=target}
   * finds one production caller, {@code RaftHAServer.installPeerAllowlist}, and it pins the static Kubernetes
   * headless-service domain.
   * <p>
   * Learned hosts do not count towards {@link #isQuorumResolved()} or {@link #isEverCompletelyResolved()}:
   * those gates describe the CONFIGURED cluster (issue #4828), and a name that does not resolve yet must not
   * hold the fail-open window open, nor - once it does resolve - retroactively widen the quorum a returning
   * peer has to clear.
   *
   * @return true when at least one host was not already pinned, i.e. when a re-resolution was performed
   */
  boolean learnPeerHosts(final Collection<String> hosts) {
    if (hosts == null || hosts.isEmpty())
      return false;

    final Set<String> added = new HashSet<>();
    synchronized (this) {
      for (final String host : hosts) {
        final String trimmed = normalize(host);
        if (trimmed == null || peerHosts.contains(trimmed) || pinnedHosts.contains(trimmed))
          continue;
        added.add(trimmed);
      }
      if (added.isEmpty())
        return false;

      final Set<String> merged = new HashSet<>(pinnedHosts);
      merged.addAll(added);
      pinnedHosts = Collections.unmodifiableSet(merged);
      republishLearnedHosts();
      // Unconditional, NOT resolveIfStale: a newly learned host is exactly the case the refresh floors were
      // written to throttle away, and the connection it is meant to admit is usually already being retried.
      doResolve();
    }
    // A pin only ever widens the allowlist, so this is expected to find nothing. It runs anyway because the
    // re-resolution above went through the same doResolve() as every other path, and leaving one caller that can
    // enqueue a revocation without draining it would strand an already-flipped session's in-flight RPCs.
    dispatchRevocations();
    LogManager.instance().log(this, Level.FINE,
        "Raft gRPC peer allowlist pinned %d new host(s): %s", added.size(), added);
    return true;
  }

  /**
   * Replaces the membership-derived hosts with {@code hosts}, the hosts of the live Raft configuration, and
   * re-resolves when the set actually changed (issue #7225). Replace, not merge: the first version of the
   * #7132 reconciliation only ever added, so a peer removed from the cluster - by
   * {@code DELETE /api/v1/cluster/peer/{id}} or a StatefulSet scale-down - kept its IPs in the allowlist and
   * kept costing a DNS lookup on every refresh tick until the process restarted.
   * <p>
   * Two kinds of host are deliberately not touched here. Configured hosts are skipped on the way in, because
   * {@code arcadedb.ha.serverList} is configuration rather than membership. Pinned hosts
   * ({@link #learnPeerHosts}) survive every shrink, because the Kubernetes headless-service domain is what
   * admits a pod that is not a member yet.
   * <p>
   * An empty collection means "the membership is empty", and clears every membership-derived host. Deciding
   * that an unreadable or degenerate membership must NOT be applied is the caller's job - see
   * {@code RaftHAServer.reconcileAllowlistMembership}, which skips the call rather than passing nothing.
   *
   * @return true when the membership-derived host set changed, i.e. when a re-resolution was performed
   */
  boolean setMemberHosts(final Collection<String> hosts) {
    final Set<String> current = new HashSet<>();
    if (hosts != null)
      for (final String host : hosts) {
        final String trimmed = normalize(host);
        if (trimmed == null || peerHosts.contains(trimmed))
          continue;
        current.add(trimmed);
      }

    final Set<String> dropped;
    final Set<String> revoked;
    synchronized (this) {
      if (memberHosts.equals(current))
        return false;

      // What actually stopped being admitted, which is not the same as what left the membership: a host that
      // is also pinned stays in the allowlist, and reporting it as dropped would be a lie in the one log line
      // an operator reads to confirm a revocation landed.
      dropped = new HashSet<>(memberHosts);
      dropped.removeAll(current);
      dropped.removeAll(pinnedHosts);
      memberHosts = Collections.unmodifiableSet(current);
      // What a departed peer's name resolved to LAST time, captured before doResolve() prunes the sticky maps of
      // the host that no longer tracks it. On Kubernetes that address is also published by the pinned
      // headless-service domain for as long as the pod runs, so without this the resolution below re-adds it and
      // the revocation reported by the log line never happens (issue #7302).
      final Set<String> droppedIps = new HashSet<>();
      for (final String host : dropped) {
        final Set<String> ips = lastKnownIps.get(host);
        if (ips != null)
          droppedIps.addAll(ips);
        else
          // The host left the membership without ever having resolved - DNS was down for the whole time it was a
          // member - so there is no last-known address to revoke, and the pinned domain would readmit it. One
          // lookup here, on a membership change and never on the periodic tick (which is the cost #7225 removed),
          // covers the case where the name resolves again by the time the peer is removed. If it does not resolve
          // now either, this peer's address cannot be identified from anything this class has, and the pinned
          // domain keeps admitting it until its pod terminates - the limitation is stated on the class javadoc
          // rather than left for a reader to discover (PR #7314 review).
          droppedIps.addAll(resolveOnce(host));
      }
      revokedPinnedIps.addAll(droppedIps);
      republishLearnedHosts();
      // Unconditional for the same reason learnPeerHosts is: this is a membership change, not the periodic
      // DNS churn the refresh floors exist to throttle. doResolve() rebuilds the allowed set from the tracked
      // hosts, which is what actually evicts a departed peer's addresses.
      doResolve();
      // Read after the resolution, which is what settles the set: an address no pinned host publishes needs no
      // revoking and is dropped there, so the log line names what is actually being held back. Narrowed to THIS
      // removal's addresses, because the line is about the hosts named beside them - reporting the whole
      // revocation set would attribute an earlier removal's addresses to this one (PR #7314 review).
      droppedIps.retainAll(revokedPinnedIps);
      revoked = Set.copyOf(droppedIps);
    }
    dispatchRevocations();
    if (!dropped.isEmpty())
      // The revoked addresses are named only when there are any, which on Kubernetes is where the revocation is
      // actually decided: a pinned headless service goes on publishing a removed pod's address until the pod
      // terminates, and this half of the line is what says the allowlist is not honouring it (issue #7302).
      LogManager.instance().log(this, Level.INFO, revoked.isEmpty()
              ? "Raft gRPC peer allowlist no longer admits %d host(s) that left the Raft configuration: %s"
              : "Raft gRPC peer allowlist no longer admits %d host(s) that left the Raft configuration: %s. Their "
                  + "addresses %s stay out of it while a pinned host still publishes them",
          dropped.size(), dropped, revoked);
    return true;
  }

  /**
   * Republishes the union of the runtime hosts, for the reject log line and {@link #getLearnedHosts()}. The
   * resolver walks the two sets separately (see {@link #doResolve()}), because only one of them can readmit a
   * peer the membership just dropped. Callers hold this object's monitor.
   */
  private void republishLearnedHosts() {
    final Set<String> union = new HashSet<>(pinnedHosts);
    union.addAll(memberHosts);
    learnedHosts = Collections.unmodifiableSet(union);
  }

  /**
   * One best-effort resolution of {@code host}, touching none of the sticky retention state: for a host that is
   * leaving and therefore has no business being tracked, but whose addresses still have to be identified so the
   * pinned domain does not readmit them. Empty when the name does not resolve.
   * <p>
   * Called with this object's monitor held, and that is accepted rather than overlooked: {@code doResolve} and
   * {@code resolveIfStale} already resolve under the same lock, so a hung resolver stalls the health-monitor tick
   * that reaches here through {@code refreshPeerAllowlist} whichever of them it enters. What this adds is one more
   * lookup, only on a membership change and only for a host that never resolved while it was a member - never on
   * the periodic tick, which is the per-tick lookup #7225 removed. Resolving outside the lock would mean deciding
   * the revocation against a membership that could have moved on by the time the answer arrived, which is a worse
   * trade for a wait the class already takes on its other paths.
   */
  private Set<String> resolveOnce(final String host) {
    try {
      final InetAddress[] addrs = resolver.resolve(host);
      if (addrs == null)
        return Collections.emptySet();
      final Set<String> ips = new HashSet<>();
      for (final InetAddress a : addrs)
        ips.add(a.getHostAddress());
      return ips;
    } catch (final UnknownHostException e) {
      LogManager.instance().log(this, Level.FINE,
          "Cannot resolve departing cluster peer host '%s'; its addresses cannot be held back from the pinned "
              + "host expansion: %s", host, e.getMessage());
      return Collections.emptySet();
    }
  }

  /** Trimmed host, or null when there is nothing usable to resolve. */
  private static String normalize(final String host) {
    if (host == null)
      return null;
    final String trimmed = host.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  /** The hosts learned after construction, pinned and membership-derived alike. Exposed for testing. */
  Set<String> getLearnedHosts() {
    return learnedHosts;
  }

  /**
   * The addresses currently held back from the pinned host expansion, i.e. the removed-but-still-published pods
   * of issue #7302. Exposed for testing.
   */
  synchronized Set<String> getRevokedPinnedIps() {
    return Set.copyOf(revokedPinnedIps);
  }

  /** The hosts pinned by {@link #learnPeerHosts}, which no membership change unlearns. Exposed for testing. */
  Set<String> getPinnedHosts() {
    return pinnedHosts;
  }

  /** The hosts of the last reconciled Raft configuration. Exposed for testing. */
  Set<String> getMemberHosts() {
    return memberHosts;
  }

  /**
   * How many CONFIGURED hosts the last resolution covered; never more than the configured total. Exposed for
   * testing.
   */
  int getResolvedPeerHostCount() {
    return resolvedPeerHosts;
  }

  /** Triggers an immediate DNS re-resolution. Exposed for testing. */
  void refresh() {
    synchronized (this) {
      doResolve();
    }
    dispatchRevocations();
  }

  /**
   * Proactively re-resolves the allowlist on a background cadence, independent of any inbound
   * connection (issue #4696). The allowlist was previously refreshed only reactively, on a rejected
   * ("miss") connection, so a peer that restarted with a new pod IP was admitted only after first
   * being rejected - and on a leader whose outbound appender channel is also wedged, that inbound
   * connection may never arrive, stranding the peer indefinitely. A periodic caller (the Raft health
   * monitor tick, which runs on every node) invokes this so the allowlist reconciles a returned peer's
   * new IP on its own, dropping the stale pre-restart IP as soon as the name resolves to the new one.
   * <p>
   * Respects the steady-state {@code refreshIntervalMs} floor (and the short startup floor while the
   * allowlist is still converging) so frequent ticks do not hammer DNS.
   */
  void proactiveRefresh() {
    resolveIfStale(currentResolveFloor());
  }

  /** Re-resolution floor: the full interval once complete, a short floor while still converging at startup. */
  private long currentResolveFloor() {
    return everCompletelyResolved ? refreshIntervalMs : missResolveFloor();
  }

  /** Short floor for miss-triggered re-resolution: converge fast without letting a flood hammer DNS. */
  private long missResolveFloor() {
    return Math.min(refreshIntervalMs, MISS_RESOLVE_FLOOR_MS);
  }

  /**
   * Re-resolves only if at least {@code floor} ms have elapsed since the last resolution.
   * <p>
   * The early return below skips {@link #dispatchRevocations()} as well as the resolution, which is deliberate and
   * is the one asymmetry with this class's other three dispatch sites: a thread that finds the resolution still
   * fresh performed none, so it has enqueued nothing, and the thread that actually ran {@code doResolve()} falls
   * through the block and dispatches whatever that resolution revoked. Returning without dispatching is therefore
   * not a dropped revocation - but moving the dispatch inside the synchronized block, or adding a second early
   * return above it, would make it one.
   */
  private void resolveIfStale(final long floor) {
    synchronized (this) {
      if (clock.getAsLong() - lastResolveMs < floor)
        return; // another thread re-resolved recently; avoid a thundering herd under a connection flood
      doResolve();
    }
    dispatchRevocations();
  }

  private synchronized void doResolve() {
    final long now = clock.getAsLong();
    final Set<String> pinned = pinnedHosts;  // one read each: both change under this monitor
    final Set<String> members = memberHosts;
    final Set<String> effective = new HashSet<>(LoopbackHosts.IPS);
    int covered = 0;
    for (final String host : peerHosts)
      if (resolveHostInto(host, now, effective, true))
        covered++;
    // Learned hosts widen the allowlist but never the gates: see learnPeerHosts.
    for (final String host : members)
      resolveHostInto(host, now, effective, false);

    // The pinned domain is expanded SEPARATELY from everything above, because it is the one host whose expansion
    // can readmit a peer the membership just dropped (issue #7302). On Kubernetes the pinned host is the headless
    // service, whose A records are every pod backing the StatefulSet - a pod that is not Ready included, since the
    // service that publishes it sets publishNotReadyAddresses. Kubernetes drops a pod's address when the POD
    // terminates, not when Raft removes it from the configuration, so after
    // DELETE /api/v1/cluster/peer/{id} against a still-running pod its address is still published here.
    final Set<String> pinnedIps = new HashSet<>();
    for (final String host : pinned)
      resolveHostInto(host, now, pinnedIps, false);

    // A revocation lasts exactly as long as the two facts that justify it. It ends when the peer is admitted for
    // some other reason - back in the membership, or declared in serverList, which is configuration rather than
    // membership - and it ends when no pinned domain publishes the address any more, because the pod is then gone
    // and whatever gets that address next is a different one. Neither end is a timer: a wall-clock TTL would
    // either readmit a removed-but-running pod or lock out a scale-up pod that inherited its address.
    revokedPinnedIps.removeAll(effective);
    revokedPinnedIps.retainAll(pinnedIps);
    for (final String ip : pinnedIps)
      if (!revokedPinnedIps.contains(ip))
        effective.add(ip);

    // Forget the sticky retention of hosts nothing tracks any more (issue #7225). A peer removed from the
    // Raft configuration must not keep last-known-good IPs on standby: they would readmit it the moment
    // anything re-learned the name, from an entry that outlived the membership that created it.
    final Set<String> tracked = new HashSet<>(peerHosts);
    tracked.addAll(members);
    tracked.addAll(pinned);
    lastKnownIps.keySet().retainAll(tracked);
    lastKnownMs.keySet().retainAll(tracked);

    allowedIps.set(Collections.unmodifiableSet(effective));
    lastResolveMs = now;
    resolvedPeerHosts = covered;
    if (covered >= resolveQuorum)
      everQuorumResolved = true;
    if (covered == peerHosts.size())
      everCompletelyResolved = true;

    // Revoke the transports this resolution stopped admitting (issue #7250). Every way the allowed set can shrink -
    // a peer leaving the Raft configuration, a peer's DNS record losing an address, a sticky last-known-good entry
    // ageing out, the startup fail-open ending - lands here, because this is the only writer of allowedIps.
    // Only the flag is flipped under the monitor; dispatchRevocations() does the gRPC work with it released.
    for (final PeerTransportSession session : sessions)
      if (!admits(session.getRemoteIp(), now) && session.revoke())
        pendingRevocations.add(session);
  }

  /**
   * Whether {@code ip} is admitted as of right now, reaching neither DNS nor the sticky retention. It is
   * {@link #isAllowed}'s decision without the re-resolution that method triggers on a miss, and without its
   * null-address case: it is only ever called for a session's address, which {@link #transportReady} obtained from a
   * resolved {@link InetAddress} and is therefore never null. Called from inside {@link #doResolve()}, which has just
   * rebuilt both inputs, so re-resolving here would be redundant as well as re-entrant.
   */
  private boolean admits(final String ip, final long now) {
    if (allowedIps.get().contains(ip))
      return true;
    // The startup fail-open (#4471/#4828) admits an unmatched address while a quorum of configured peers has not
    // resolved. A transport admitted under it must not be cut while it is still in force, or the fix would
    // re-create the self-inflicted partition that window exists to prevent - it is revoked by the first resolution
    // after the window closes, when this returns false.
    return !everQuorumResolved && startupGraceMs > 0 && now - createdMs < startupGraceMs;
  }

  /**
   * Cuts the RPCs in flight on the transports the last resolution revoked. Called with this object's monitor
   * released: closing a {@code ServerCall} runs gRPC code, and holding the filter's lock across it would order this
   * monitor above gRPC's internals on one path and below them on the {@code transportReady} path.
   */
  private void dispatchRevocations() {
    PeerTransportSession session;
    while ((session = pendingRevocations.poll()) != null) {
      final int closed = session.closeLiveCalls();
      if (!session.isAdmitted())
        continue; // swept while transportReady was still deciding: it is being rejected, and that is logged there
      LogManager.instance().log(this, Level.INFO,
          "Revoked the established Raft gRPC transport of %s: the address is no longer in the peer allowlist. "
              + "%d in-flight RPC(s) closed; every further RPC on that transport is refused. gRPC exposes no way to "
              + "close one established transport on demand, so the connection is closed once it has been idle for "
              + "arcadedb.ha.grpcMaxConnectionIdleMs - a peer that keeps retrying keeps it open until it stops, "
              + "unless arcadedb.ha.grpcMaxConnectionAgeMs is set, which bounds its life regardless.",
          session.getRemoteIp(), closed);
    }
  }

  /**
   * Resolves one host into {@code effective}, applying the sticky last-known-good retention. Returns whether
   * the host ended up covered (freshly resolved, or served from its sticky entry).
   *
   * @param declared true for a host from {@code serverList}, whose failure to resolve is worth a WARNING;
   *                 false for a learned host, where a name that does not (yet) resolve is expected - a
   *                 headless service domain outside Kubernetes is the normal case.
   */
  private boolean resolveHostInto(final String host, final long now, final Set<String> effective,
      final boolean declared) {
    Set<String> fresh = null;
    try {
      final InetAddress[] addrs = resolver.resolve(host);
      if (addrs != null && addrs.length > 0) {
        fresh = new HashSet<>();
        for (final InetAddress a : addrs)
          fresh.add(a.getHostAddress());
      }
    } catch (final UnknownHostException e) {
      LogManager.instance().log(this, declared ? Level.WARNING : Level.FINE,
          "Cannot resolve cluster peer host '%s' for Raft gRPC allowlist: %s", host, e.getMessage());
    }

    if (fresh != null && !fresh.isEmpty()) {
      lastKnownIps.put(host, fresh);
      lastKnownMs.put(host, now);
      effective.addAll(fresh);
      return true;
    }

    // Resolution failed: keep the last-known-good IPs for a bounded time (sticky) so a transient
    // DNS outage or pod-IP churn does not evict a peer that resolved moments ago.
    final Set<String> prev = lastKnownIps.get(host);
    final Long prevMs = lastKnownMs.get(host);
    if (prev != null && prevMs != null && stickyTtlMs > 0 && now - prevMs <= stickyTtlMs) {
      effective.addAll(prev);
      return true;
    }
    lastKnownIps.remove(host);
    lastKnownMs.remove(host);
    return false;
  }

  /**
   * Extracts just the host component from every entry in the ArcadeDB HA server list.
   * Entries follow {@code [name@]host:raftPort[:httpPort[:priority[:httpsPort]]]}, the bracketed
   * IPv6 form, or the object form {@code [name@]host:{raft:..,http:..,https:..,priority:..}}.
   * The optional {@code name@} prefix (e.g. {@code frankfurt@10.0.0.1:2434:2480}) is stripped
   * before the host is extracted, mirroring {@link RaftPeerAddressResolver#parsePeerList}.
   * <p>
   * Entry splitting is brace-aware (via {@link RaftPeerAddressResolver#splitEntries}) so that the
   * commas inside an object-form {@code {raft:..,http:..}} block are not mistaken for entry
   * separators (issue #4470): a naive {@code split(",")} would otherwise extract {@code http} and
   * {@code https} as bogus peer hosts.
   * <p>
   * An empty or null serverList returns an empty list rather than throwing, so callers can
   * decide whether a missing list is an error.
   */
  static List<String> extractPeerHosts(final String serverList) {
    if (serverList == null || serverList.isBlank())
      return Collections.emptyList();

    final List<String> entries = RaftPeerAddressResolver.splitEntries(serverList);
    final List<String> hosts = new ArrayList<>(entries.size());
    for (final String entry : entries) {
      String trimmed = entry.trim();
      if (trimmed.isEmpty())
        continue;

      // Strip the optional human-readable "name@" prefix so it is not mistaken for the host.
      // parsePeerList allows only one '@'; here we defensively take the part after the first.
      final int atIdx = trimmed.indexOf('@');
      if (atIdx >= 0)
        trimmed = trimmed.substring(atIdx + 1).trim();
      if (trimmed.isEmpty())
        continue;

      // Parse host from host[:raftPort[:httpPort[:priority[:httpsPort]]]], the object form
      // host:{raft:..,http:..} or the bracketed IPv6 form.
      final String host;
      if (trimmed.startsWith("[")) {
        // IPv6 bracketed: [::1]:port - extract content between brackets
        final int closingBracket = trimmed.indexOf(']');
        if (closingBracket > 1)
          host = trimmed.substring(1, closingBracket);
        else
          host = trimmed; // malformed - pass through as-is
      } else {
        // Object form host:{...} - the host is everything before the '{' (its commas were already
        // protected by splitEntries). Otherwise it is the first colon-delimited token.
        final int braceIdx = trimmed.indexOf('{');
        final int colonIdx = trimmed.indexOf(':');
        if (braceIdx >= 0) {
          String h = trimmed.substring(0, braceIdx).trim();
          if (h.endsWith(":"))
            h = h.substring(0, h.length() - 1).trim();
          host = h;
        } else
          host = colonIdx > 0 ? trimmed.substring(0, colonIdx) : trimmed;
      }

      if (!host.isBlank())
        hosts.add(host);
    }
    return hosts;
  }

  @Override
  public String toString() {
    return "PeerAddressAllowlistFilter{peerHosts=" + peerHosts + ", pinnedHosts=" + pinnedHosts + ", memberHosts="
        + memberHosts + ", allowed=" + allowedIps.get() + "}";
  }

  /**
   * Pluggable host resolver so tests can drive resolution deterministically without real DNS.
   * <p>
   * Declared last rather than first so it does not sit between the class header and the fields: PMD's
   * {@code FieldDeclarationsShouldBeAtStartOfClass} counts a nested type as the start of the method section,
   * so every field declared after it is reported, and Codacy reports the ones a diff happens to touch.
   */
  @FunctionalInterface
  interface HostResolver {
    InetAddress[] resolve(String host) throws UnknownHostException;
  }
}
