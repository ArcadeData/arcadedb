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
import java.util.Set;
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
 * caller may {@link #learnPeerHosts add} more at runtime, and {@code RaftHAServer} does so on every health
 * monitor tick from the live Raft configuration. Without that, a peer that joined after this node started -
 * through {@code addPeer}, or through the Kubernetes StatefulSet scale-up auto-join of issue #4836 - was
 * rejected forever by every node that had already latched {@code everQuorumResolved}: the two features are
 * individually correct and were jointly broken, because nothing reconciled them. Learned hosts resolve
 * exactly like configured ones but are best-effort: a name that does not resolve contributes nothing,
 * is logged at FINE rather than WARNING, and never holds the latches back.
 * <p>
 * This is NOT a substitute for mTLS: it does not authenticate peer identity and does not
 * encrypt the traffic. See GitHub issue #3890. The bounded startup fail-open is an acceptable
 * trade-off for that reason; set {@code startupGraceMs=0} to disable it.
 */
final class PeerAddressAllowlistFilter extends ServerTransportFilter {

  /** Pluggable host resolver so tests can drive resolution deterministically without real DNS. */
  @FunctionalInterface
  interface HostResolver {
    InetAddress[] resolve(String host) throws UnknownHostException;
  }

  // Minimum spacing between miss-triggered re-resolutions. Bounds DNS load under a connection flood
  // (at startup or from a non-peer) while letting the allowlist converge within ~1s.
  private static final long        MISS_RESOLVE_FLOOR_MS = 1_000L;

  // Hosts declared in arcadedb.ha.serverList. Immutable, and the ONLY ones the quorum/completeness
  // latches below count: they describe the configured cluster, which is what #4828 reasoned about.
  private final List<String>                 peerHosts;
  // Hosts learned after construction (issue #7132): the members of the live Raft configuration, plus - on
  // Kubernetes - the headless service domain behind arcadedb.ha.k8sSuffix, which resolves to every pod of
  // the StatefulSet. Copy-on-write so isAllowed()'s hot path never synchronises against a learner.
  private volatile Set<String>               learnedHosts = Collections.emptySet();
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

  PeerAddressAllowlistFilter(final List<String> peerHosts, final long refreshIntervalMs) {
    this(peerHosts, refreshIntervalMs,
        GlobalConfiguration.HA_PEER_ALLOWLIST_STARTUP_GRACE_MS.getValueAsLong(),
        GlobalConfiguration.HA_PEER_ALLOWLIST_STICKY_TTL_MS.getValueAsLong());
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

    if (isAllowed(address.getHostAddress()))
      return attrs;

    throw new SecurityException("Remote address '" + address.getHostAddress() + "' is not in the cluster peer allowlist");
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
          ip, lastKnownIps.size(), peerHosts.size(), resolveQuorum, allowedIps.get(), startupGraceMs);
      return true;
    }

    // Named settings and the full host list, because this is logged on the RECEIVING node: an operator
    // looking at a peer that will not join sees nothing at all in ITS log (issue #7132).
    LogManager.instance().log(this, Level.WARNING,
        "Rejecting Raft gRPC connection from %s: the address is not in the cluster peer allowlist "
            + "(allowed=%s, configured hosts=%s, learned from the live Raft configuration=%s). If this is a "
            + "legitimate peer, add it to 'arcadedb.ha.serverList' or check that its hostname resolves from "
            + "this node; 'arcadedb.ha.peerAllowlist.enabled=false' disables the check entirely.",
        ip, allowedIps.get(), peerHosts, learnedHosts);
    return false;
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
   * Adds hosts discovered after construction to the allowlist and re-resolves immediately when any of them
   * is new (issue #7132). The caller is {@code RaftHAServer.refreshPeerAllowlist}, which passes the hosts of
   * the live Raft configuration on every health monitor tick, and {@code installPeerAllowlist}, which seeds
   * the Kubernetes headless service domain. Idempotent: a tick that discovers nothing new costs one set
   * comparison and does not touch DNS.
   * <p>
   * Learned hosts do not count towards {@link #isQuorumResolved()} or {@link #isEverCompletelyResolved()}:
   * those gates describe the CONFIGURED cluster (issue #4828), and a name that does not resolve yet must not
   * hold the fail-open window open, nor - once it does resolve - retroactively widen the quorum a returning
   * peer has to clear.
   *
   * @return true when at least one host was not already known, i.e. when a re-resolution was performed
   */
  boolean learnPeerHosts(final Collection<String> hosts) {
    if (hosts == null || hosts.isEmpty())
      return false;

    final Set<String> added = new HashSet<>();
    synchronized (this) {
      for (final String host : hosts) {
        if (host == null || host.isBlank())
          continue;
        final String trimmed = host.trim();
        if (peerHosts.contains(trimmed) || learnedHosts.contains(trimmed))
          continue;
        added.add(trimmed);
      }
      if (added.isEmpty())
        return false;

      final Set<String> merged = new HashSet<>(learnedHosts);
      merged.addAll(added);
      learnedHosts = Collections.unmodifiableSet(merged);
      // Unconditional, NOT resolveIfStale: a newly learned host is exactly the case the refresh floors were
      // written to throttle away, and the connection it is meant to admit is usually already being retried.
      doResolve();
    }
    LogManager.instance().log(this, Level.FINE,
        "Raft gRPC peer allowlist learned %d new host(s): %s", added.size(), added);
    return true;
  }

  /** The hosts learned after construction. Exposed for testing. */
  Set<String> getLearnedHosts() {
    return learnedHosts;
  }

  /** Triggers an immediate DNS re-resolution. Exposed for testing. */
  void refresh() {
    doResolve();
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

  /** Re-resolves only if at least {@code floor} ms have elapsed since the last resolution. */
  private synchronized void resolveIfStale(final long floor) {
    if (clock.getAsLong() - lastResolveMs < floor)
      return; // another thread re-resolved recently; avoid a thundering herd under a connection flood
    doResolve();
  }

  private synchronized void doResolve() {
    final long now = clock.getAsLong();
    final Set<String> effective = new HashSet<>(LoopbackHosts.IPS);
    int covered = 0;
    for (final String host : peerHosts)
      if (resolveHostInto(host, now, effective, true))
        covered++;
    // Learned hosts widen the allowlist but never the gates: see learnPeerHosts.
    for (final String host : learnedHosts)
      resolveHostInto(host, now, effective, false);

    allowedIps.set(Collections.unmodifiableSet(effective));
    lastResolveMs = now;
    if (covered >= resolveQuorum)
      everQuorumResolved = true;
    if (covered == peerHosts.size())
      everCompletelyResolved = true;
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
    return "PeerAddressAllowlistFilter{peerHosts=" + peerHosts + ", learnedHosts=" + learnedHosts + ", allowed="
        + allowedIps.get() + "}";
  }
}
