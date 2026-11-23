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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.raft.ratis.GuardedRaftServer;
import com.arcadedb.server.ha.raft.ratis.RemoteAddressCallInterceptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter;

/**
 * Sets up the Raft transport of this node: the gRPC parameters Ratis starts with, the server customisations that report
 * misdirected requests, and the peer allowlist that admits connections only from the members of the configuration,
 * kept in step with every membership change.
 * <p>
 * Split out of {@link RaftHAServer}, which builds the Ratis server with these parameters.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RaftTransportSetup {
  private final RaftHAServer ha;

  RaftTransportSetup(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Builds a {@link Parameters} instance with the gRPC services customizer installed.
   * The customizer adds a {@link PeerAddressAllowlistFilter} that rejects inbound Raft gRPC
   * connections from IPs not listed in {@code arcadedb.ha.serverList}.
   */
  // Package-private rather than private so RaftParametersPublicationTest can pin when
  // raftParameters becomes visible - the property this method exists to get right.
  Parameters buildParameters(final ContextConfiguration configuration) {
    ha.allowlistFilter = null;
    ha.allowlistInterceptor = null;
    final Parameters parameters = new Parameters();

    // mTLS first: it is the cryptographic peer identity the allowlist below cannot provide. Throws a
    // ConfigurationException at startup when enabled with an unusable cert/key/trust path (issue #3890),
    // which is one of the two reasons raftParameters is published only on the way out of this method.
    final GrpcTlsConfig tlsConfig = RaftPropertiesBuilder.applyTls(configuration, parameters);
    if (tlsConfig != null)
      LogManager.instance().log(ha, Level.INFO,
          "Raft gRPC transport secured with TLS (mutual authentication: %s)", tlsConfig.getMtlsEnabled());

    installGrpcServerCustomizations(configuration, parameters);

    // Read by FixedGrpcFactory, which hands it to the guard that refuses Raft RPCs addressed to another peer (#10016).
    parameters.put(GuardedRaftServer.MISDIRECTED_REQUEST_LISTENER_KEY, ha.misdirected::onMisdirectedRaftRequest,
        GuardedRaftServer.MisdirectedRequestListener.class);

    // Last line on every path: see the field's comment. Nothing above may publish a partially-configured
    // Parameters, and nothing below may add to it.
    ha.raftParameters = parameters;
    return parameters;
  }

  /**
   * Installs the one {@code GrpcServices.Customizer} the Raft listener gets: the peer allowlist's transport filter
   * and call interceptor (issues #7132, #7225, #7250), the connection-idle window (issue #7316), the
   * connection-age window (issue #7339) and the interceptor that hands the misdirected-request guard the remote address
   * of each call (issue #10225).
   * <p>
   * All three are configured independently, so this method is not gated on any of them: gating it on
   * {@code arcadedb.ha.peerAllowlist.enabled}, which is where the allowlist install used to live, would have made
   * {@code arcadedb.ha.grpcMaxConnectionIdleMs} a setting that silently does nothing on exactly the clusters that
   * turned the allowlist off. The remote-address interceptor is not a setting: it is what tells this node's own
   * request from one that only claims to be (issue #10225), so the customizer is always installed, and when nothing
   * else is configured it adds that interceptor alone.
   */
  private void installGrpcServerCustomizations(final ContextConfiguration configuration, final Parameters parameters) {
    final PeerAddressAllowlistFilter filter = buildPeerAllowlistFilter(configuration);
    final PeerAllowlistCallInterceptor interceptor = filter == null ? null : new PeerAllowlistCallInterceptor();
    // The filter gates a connection once, when it is established. The interceptor enforces the same decision on
    // every RPC, which is what revokes a transport already open when its address stopped being admitted (#7250):
    // it reads the session the filter attached to the transport and refuses the call when that session is revoked.
    ha.allowlistFilter = filter;
    ha.allowlistInterceptor = interceptor;

    // Neither surface can close the connection itself, so the socket of a revoked peer outlived its reach (#7316).
    final long maxConnectionIdleMs = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_IDLE_MS);
    // ...and the idle window only reaps a peer that goes quiet: gRPC restarts it from the last moment the
    // connection's active-stream count hit zero, so a peer that keeps starting RPCs - a removed peer still
    // campaigning, or a squatter - pushes it forward forever, refused RPCs included (#7339). The age window is the
    // unconditional bound for that case, off by default because it recycles healthy connections on the same period.
    final long maxConnectionAgeMs = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_MS);
    final long maxConnectionAgeGraceMs = configuration.getValueAsLong(
        GlobalConfiguration.HA_GRPC_MAX_CONNECTION_AGE_GRACE_MS);
    // Order does not matter: each only reads the call's attributes, and the context set here wraps the service handler.
    final ServerInterceptor remoteAddress = new RemoteAddressCallInterceptor();

    GrpcConfigKeys.Server.setServicesCustomizer(parameters, new RaftGrpcServicesCustomizer(
        filter == null ? new ServerTransportFilter[0] : new ServerTransportFilter[] { filter },
        interceptor == null ? new ServerInterceptor[] { remoteAddress } : new ServerInterceptor[] { interceptor, remoteAddress },
        maxConnectionIdleMs, maxConnectionAgeMs, maxConnectionAgeGraceMs));
  }

  /** The inbound peer allowlist filter, or {@code null} when it is disabled or has no host to admit. */
  private PeerAddressAllowlistFilter buildPeerAllowlistFilter(final ContextConfiguration configuration) {
    if (!configuration.getValueAsBoolean(GlobalConfiguration.HA_PEER_ALLOWLIST_ENABLED))
      return null;

    final String serverList = configuration.getValueAsString(GlobalConfiguration.HA_SERVER_LIST);
    final long refreshMs = configuration.getValueAsLong(GlobalConfiguration.HA_GRPC_ALLOWLIST_REFRESH_MS);
    final long startupGraceMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_ALLOWLIST_STARTUP_GRACE_MS);
    final long stickyTtlMs = configuration.getValueAsLong(GlobalConfiguration.HA_PEER_ALLOWLIST_STICKY_TTL_MS);
    // The same Kubernetes DNS suffix parsePeerList applies to the peer addresses (issue #7132). Without it the
    // allowlist tried to resolve the bare pod names written in the server list ("arcadedb-0"), which only
    // happen to resolve when the pod's DNS search list covers them - i.e. when the namespace and the headless
    // service share a name. Everywhere else no configured host resolved, the quorum latch never tripped, and
    // the filter fell open for the whole grace window and then rejected every peer.
    final String k8sDnsSuffix = configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)
        ? configuration.getValueAsString(GlobalConfiguration.HA_K8S_DNS_SUFFIX)
        : "";
    final List<String> peerHosts = PeerAddressAllowlistFilter.extractPeerHosts(serverList).stream()
        .map(h -> RaftPeerAddressResolver.applyDnsSuffix(h, k8sDnsSuffix)).toList();
    if (peerHosts.isEmpty()) {
      LogManager.instance().log(ha, Level.WARNING,
          "arcadedb.ha.peerAllowlist.enabled=true but arcadedb.ha.serverList is empty; allowlist not installed");
      return null;
    }
    final PeerAddressAllowlistFilter filter = new PeerAddressAllowlistFilter(peerHosts, refreshMs, startupGraceMs,
        stickyTtlMs);

    // Kubernetes scale-up (issue #7132 x #4836). The joiner synthesizes itself into the group and dials the
    // existing pods, but its host is in nobody's server list, and by then every existing pod has latched
    // everQuorumResolved - so the join was rejected on the RECEIVING side and the new pod stayed NOT_READY
    // forever. The headless service domain is the StatefulSet's own membership record: it resolves to the A
    // records of every pod backing it, at any replica count and without an ordinal to guess, and it covers a
    // pod that is not Ready yet because the service that publishes it sets publishNotReadyAddresses (which
    // the shipped manifest documents as required, since a pod is Ready only once it has joined).
    // learnPeerHosts PINS it: it is the one host that has to outlive a membership shrink, because admitting a
    // pod that is not a member yet is its entire purpose (issue #7225).
    final String serviceDomain = headlessServiceDomain(k8sDnsSuffix);
    if (serviceDomain != null)
      filter.learnPeerHosts(List.of(serviceDomain));

    return filter;
  }

  /**
   * The headless-service FQDN behind {@code arcadedb.ha.k8sSuffix}, i.e. the suffix without its leading dot,
   * or {@code null} when not running under Kubernetes or when no suffix is configured. Package-private for
   * testing.
   */
  static String headlessServiceDomain(final String k8sDnsSuffix) {
    if (k8sDnsSuffix == null || k8sDnsSuffix.isBlank())
      return null;
    final String trimmed = k8sDnsSuffix.trim();
    final String domain = trimmed.startsWith(".") ? trimmed.substring(1) : trimmed;
    return domain.isBlank() ? null : domain;
  }

  /** Package-private test hook: the transport configuration the Raft client builders read. */
  Parameters raftParametersForTest() {
    return ha.raftParameters;
  }

  /** Package-private test hook: the inbound Raft gRPC peer allowlist, or null when disabled. */
  PeerAddressAllowlistFilter allowlistFilterForTest() {
    return ha.allowlistFilter;
  }

  /** Package-private test hook: the per-RPC half of the peer allowlist (issue #7250), or null when disabled. */
  PeerAllowlistCallInterceptor allowlistInterceptorForTest() {
    return ha.allowlistInterceptor;
  }

  /**
   * Makes the allowlist's membership-derived hosts equal the hosts of {@code committedPeers} (issue #7225).
   * Package-private so a test can drive a membership change without a running Ratis division.
   * <p>
   * Replace, not merge: #7132 taught the allowlist to learn a peer that joined at runtime, but nothing ever
   * unlearned one, so a peer removed by {@code DELETE /api/v1/cluster/peer/&#123;id&#125;} or by a StatefulSet
   * scale-down kept inbound Raft gRPC access - and a DNS lookup per tick - until the process restarted.
   * <p>
   * The input is {@link RaftHAServer#getCommittedPeersOrNull()} rather than {@link RaftHAServer#getLivePeers()} exactly because the
   * two differ on the case replace semantics is sensitive to: {@code getLivePeers()} substitutes the DECLARED
   * server list when the division cannot be read (before startup, and throughout an in-place restart - issue
   * #5271), and replacing membership with the declared list would unlearn every runtime-joined peer every time
   * that window opens. {@code null} means "no membership information this tick", which is not the same as "no
   * members", and is the same distinction {@code RaftClusterStatusExporter.everCommitted} draws (issue #7136).
   * A peer list that reduces to no usable host is treated the same way: a committed configuration always
   * carries at least this node, so an empty one is a read that went wrong rather than an empty cluster.
   */
  void reconcileAllowlistMembership(final Collection<RaftPeer> committedPeers) {
    final PeerAddressAllowlistFilter filter = ha.allowlistFilter;
    if (filter == null || committedPeers == null)
      return;

    // committedHosts, not memberHosts: PeerAddressAllowlistFilter has a field by the latter name, and the two
    // classes' vocabulary overlaps enough now that reusing it here reads like the same thing.
    final List<String> committedHosts = new ArrayList<>(committedPeers.size());
    for (final RaftPeer peer : committedPeers) {
      final String host = allowlistHostOf(peer.getAddress());
      if (host != null)
        committedHosts.add(host);
    }
    // Defensive, and not expected to fire: a committed configuration carries at least the local node, and
    // allowlistHostOf only returns null for an address with no host part at all. It is here so that a peer
    // list this method cannot reduce to a single host - whatever produced it - can never be mistaken for an
    // empty cluster and wipe the membership the previous tick learned.
    // WARNING rather than FINE, matching the getCommittedPeersOrNull catch this method reads from: the guard
    // silently skipping the reconciliation is how a genuine bug in address parsing or in the membership read
    // would hide, and the unreadable-membership case - the one that IS expected - returns above without
    // reaching this line, so an ordinary #5271 restart window does not log here at all.
    if (committedHosts.isEmpty()) {
      LogManager.instance().log(ha, Level.WARNING,
          "The Raft configuration carried %d peer(s) but no usable host this tick; keeping the current peer "
              + "allowlist membership rather than treating it as an empty cluster", committedPeers.size());
      return;
    }

    filter.setMemberHosts(committedHosts);
  }

  /**
   * The host of a Raft peer address in the form the allowlist resolver wants: no port, and no brackets around
   * an IPv6 literal ({@link InetAddress#getAllByName} rejects those). Returns {@code null} when the address
   * carries no usable host. Package-private for testing.
   */
  static String allowlistHostOf(final String raftAddress) {
    final String host = PeerEndpoints.extractHost(raftAddress);
    if (host == null || host.isBlank())
      return null;
    if (host.startsWith("[") && host.endsWith("]"))
      return host.length() > 2 ? host.substring(1, host.length() - 1) : null;
    return host;
  }
}
