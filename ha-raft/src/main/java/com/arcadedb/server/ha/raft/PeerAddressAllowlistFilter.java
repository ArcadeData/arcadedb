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

import com.arcadedb.log.LogManager;
import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.Grpc;
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
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
 * This is NOT a substitute for mTLS: it does not authenticate peer identity and does not
 * encrypt the traffic. See GitHub issue #3890.
 */
final class PeerAddressAllowlistFilter extends ServerTransportFilter {

  private static final Set<String> LOOPBACK_IPS = Set.of("127.0.0.1", "0:0:0:0:0:0:0:1", "::1");

  private final List<String>                 peerHosts;
  private final long                         refreshIntervalMs;
  private final AtomicReference<Set<String>> allowedIps = new AtomicReference<>(Collections.emptySet());
  private volatile long                      lastResolveMs;

  PeerAddressAllowlistFilter(final List<String> peerHosts, final long refreshIntervalMs) {
    if (peerHosts == null || peerHosts.isEmpty())
      throw new IllegalArgumentException("Peer allowlist requires at least one host");
    this.peerHosts = List.copyOf(peerHosts);
    this.refreshIntervalMs = Math.max(0L, refreshIntervalMs);
    resolveNow();
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
    if (allowedIps.get().contains(ip))
      return attrs;

    // Miss: re-resolve (rate-limited) to pick up restarted peers with new IPs.
    if (System.currentTimeMillis() - lastResolveMs >= refreshIntervalMs) {
      resolveNow();
      if (allowedIps.get().contains(ip))
        return attrs;
    }

    LogManager.instance().log(this, Level.WARNING,
        "Rejecting Raft gRPC connection from non-peer address: %s (allowed=%s)", ip, allowedIps.get());
    throw new SecurityException("Remote address '" + ip + "' is not in the cluster peer allowlist");
  }

  /** Returns an immutable snapshot of the currently allowed IPs. Exposed for testing. */
  Set<String> getAllowedIps() {
    return allowedIps.get();
  }

  /** Triggers an immediate DNS re-resolution. Exposed for testing. */
  void refresh() {
    resolveNow();
  }

  private void resolveNow() {
    final Set<String> resolved = new HashSet<>();
    resolved.addAll(LOOPBACK_IPS);
    for (final String host : peerHosts) {
      try {
        final InetAddress[] addrs = InetAddress.getAllByName(host);
        for (final InetAddress a : addrs)
          resolved.add(a.getHostAddress());
      } catch (final UnknownHostException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Cannot resolve cluster peer host '%s' for Raft gRPC allowlist: %s", host, e.getMessage());
      }
    }
    allowedIps.set(Collections.unmodifiableSet(resolved));
    lastResolveMs = System.currentTimeMillis();
  }

  /**
   * Extracts just the host component from every entry in the ArcadeDB HA server list.
   * Entries follow {@code host:raftPort[:httpPort[:priority]]} or the bracketed IPv6 form.
   * <p>
   * An empty or null serverList returns an empty list rather than throwing, so callers can
   * decide whether a missing list is an error.
   */
  static List<String> extractPeerHosts(final String serverList) {
    if (serverList == null || serverList.isBlank())
      return Collections.emptyList();

    final String[] entries = serverList.split(",");
    final List<String> hosts = new ArrayList<>(entries.length);
    for (final String entry : entries) {
      final String trimmed = entry.trim();
      if (trimmed.isEmpty())
        continue;

      // Parse host from host[:raftPort[:httpPort[:priority]]] format.
      // IPv6 bracketed addresses look like [::1]:raftPort.
      final String host;
      if (trimmed.startsWith("[")) {
        // IPv6 bracketed: [::1]:port - extract content between brackets
        final int closingBracket = trimmed.indexOf(']');
        if (closingBracket > 1)
          host = trimmed.substring(1, closingBracket);
        else
          host = trimmed; // malformed - pass through as-is
      } else {
        // Standard host:port or bare host - first colon-delimited token is the host
        final int colonIdx = trimmed.indexOf(':');
        host = colonIdx > 0 ? trimmed.substring(0, colonIdx) : trimmed;
      }

      if (!host.isBlank())
        hosts.add(host);
    }
    return hosts;
  }

  @Override
  public String toString() {
    return "PeerAddressAllowlistFilter{peerHosts=" + peerHosts + ", allowed=" + allowedIps.get() + "}";
  }
}
