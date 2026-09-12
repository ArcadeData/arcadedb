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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.HAServerPlugin;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Where a forward to the cluster leader is dialled, on which scheme, and with which client (issue #7508).
 * <p>
 * Every follower-to-leader forward used to build its URL as {@code "http://" + ha.getLeaderAddress() + path},
 * because that address is the only leader endpoint {@link HAServerPlugin} exposed. Meanwhile five peer-to-peer
 * dials in the cluster - snapshot download ({@code SnapshotInstaller}), the capability probe
 * ({@code PeerCapabilityQuery}), the bootstrap-state query ({@code LeaderDatabaseQuery}), the auth-session RPC
 * ({@code PeerAuthSessionQuery}) and the stalled-replica resync ({@code RaftHAServer.forceResyncStalledReplica})
 * - already pick their scheme with one rule: prefer the peer's HTTPS endpoint when SSL is enabled and one
 * resolves. This puts the forward on that same rule, in one place rather than once per call site. Two dials that
 * are neither forwards nor on that rule are tracked separately in issue #7546.
 * <p>
 * Two things go wrong without it on a cluster with {@code arcadedb.ssl.enabled} set, and neither needs the plain
 * listener to be switched off (it cannot be - {@code HttpServer.buildUndertowServer} binds it unconditionally):
 * <ul>
 * <li><b>the forward travels in cleartext.</b> It relays either the client's own {@code Authorization} header or
 * the cluster token, plus the request body, so an operator who enabled SSL gets every peer-to-peer RPC encrypted
 * except this one;</li>
 * <li><b>on a cluster that declares {@code https} ports and not {@code http} ones the forward is refused
 * outright.</b> The HTTP address is then derived as the leader's Raft host plus THIS node's HTTP port, so
 * {@link HAServerPlugin#isOwnHttpAddress} answers true and the caller refuses - with a correctly declared HTTPS
 * endpoint sitting unused.</li>
 * </ul>
 *
 * @param address the {@code host:port} to dial
 * @param https   whether {@link #address} speaks TLS
 * @param client  the client to send on - the caller's own for plain HTTP, the plugin's trust-carrying one for HTTPS
 */
public record LeaderDial(String address, boolean https, HttpClient client) {

  /**
   * Said once per JVM: a plugin that advertises an HTTPS leader endpoint but cannot hand out a client for it has
   * a configuration fault worth naming, and the forward that fell back to cleartext would otherwise be silent.
   */
  private static final AtomicBoolean HTTPS_CLIENT_FALLBACK_WARNED = new AtomicBoolean(false);

  /** The URL this dial targets. {@code pathWithQuery} starts with {@code /} and may carry a query string. */
  public String url(final String pathWithQuery) {
    return (https ? "https://" : "http://") + address + pathWithQuery;
  }

  /**
   * Where to dial the current leader, or {@code null} when no leader address is known at all - which the caller
   * reports as "the leader address is unknown", exactly as it did when it read {@code getLeaderAddress()} itself.
   * <p>
   * The HTTPS endpoint wins when the plugin offers one AND can hand out a client that trusts the cluster's
   * certificates; anything else falls back to the plain-HTTP address, because that listener is the one that is
   * always bound. A plugin that offers no HTTPS endpoint - the interface default, and what {@code RaftHAPlugin}
   * answers whenever SSL is off - therefore leaves the dial exactly where it was.
   * <p>
   * <b>The caller still owns the self-address check, and only on the plain-HTTP branch.</b>
   * {@link HAServerPlugin#isOwnHttpAddress} compares against this node's HTTP listener and cannot speak for an
   * HTTPS endpoint; the plugin withholds an HTTPS address that is this node's own instead (see
   * {@link HAServerPlugin#getLeaderHttpsAddress()}).
   *
   * @param ha          the HA plugin, which must not be {@code null} - the caller has already established that
   *                    this node is an HA replica
   * @param plainClient the client to use when the dial resolves to plain HTTP
   */
  public static LeaderDial resolve(final HAServerPlugin ha, final HttpClient plainClient) {
    final String httpsAddress = ha.getLeaderHttpsAddress();
    if (httpsAddress != null) {
      try {
        final HttpClient httpsClient = ha.getPeerHttpsClient();
        if (httpsClient != null)
          return new LeaderDial(httpsAddress, true, httpsClient);
        warnHttpsFallback("it offers no HTTPS client to dial it with");
      } catch (final IOException e) {
        warnHttpsFallback("its trust material cannot be read (" + e.getMessage() + ")");
      }
    }

    final String httpAddress = ha.getLeaderAddress();
    return httpAddress == null || httpAddress.isBlank() ? null : new LeaderDial(httpAddress, false, plainClient);
  }

  private static void warnHttpsFallback(final String reason) {
    if (HTTPS_CLIENT_FALLBACK_WARNED.compareAndSet(false, true))
      LogManager.instance().log(LeaderDial.class, Level.WARNING,
          "The HA plugin names an HTTPS endpoint for the cluster leader but %s, so requests forwarded to the leader "
              + "fall back to the plain-HTTP endpoint and travel in cleartext together with the credentials they "
              + "relay. Check the truststore named by '%s'. This notice is logged only once.",
          reason, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE.getKey());
  }
}
