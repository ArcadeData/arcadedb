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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.HAServerPlugin;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
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
 * <b>Falling back is not the same as failing open.</b> A cluster that never declared its {@code https} ports
 * resolves no HTTPS endpoint, and dialling its plain listener is what it did before this class existed - refusing
 * there would break every SSL cluster that left the optional 5th field of {@code arcadedb.ha.serverList} out. But
 * a cluster that HAS named an HTTPS endpoint for the leader has said where this forward belongs, and if no client
 * can be built to reach it the forward is <b>refused</b> rather than sent in the clear:
 * {@code SnapshotInstaller.buildSSLContext} already falls back to the JVM default truststore when none is
 * configured, so failing to produce a client means the trust material itself could not be loaded.
 *
 * @param address the {@code host:port} to dial, or {@code null} on a refusal
 * @param https   whether {@link #address} speaks TLS
 * @param client  the client to send on - the caller's own for plain HTTP, the plugin's trust-carrying one for HTTPS
 * @param refusal why this forward may not be sent at all, or {@code null} when it may
 */
public record LeaderDial(String address, boolean https, HttpClient client, String refusal) {

  /**
   * A {@code Duration} of zero or less is rejected by both {@code HttpClient.Builder.connectTimeout} and
   * {@code HttpRequest.Builder.timeout}. Clamping to 1 ms rather than falling back to a default is
   * deliberate, matching {@code LeaderCommandForwarder.Transport}'s own clamp: 0 must not become a back door
   * to the unbounded behaviour {@link #newConnectTimeoutBoundedClient} and the response deadlines in
   * {@code RaftReplicatedDatabase}/{@code PostBatchHandler} (issues #7526/#7527/#7542/#7543) exist to remove.
   * Public and shared from here rather than one copy per site (code review finding on PR #7650), since
   * {@link #newConnectTimeoutBoundedClient} is already the shared home for the connect-timeout half of the
   * same reasoning.
   */
  public static final long MIN_FORWARD_TIMEOUT_MS = 1L;

  /**
   * Builds the plain-HTTP client a follower-to-leader forward dials on, with
   * {@link GlobalConfiguration#HA_PROXY_CONNECT_TIMEOUT} applied unconditionally (issues
   * #7526/#7527/#7542/#7543): a leader that never even accepts the connection has no bound without this,
   * distinct from - and unaffected by - whatever response deadline the caller applies per request. Shared by
   * every plain-HTTP forward site ({@code RaftReplicatedDatabase}, {@code PostBatchHandler}) rather than one
   * copy of the clamp per site.
   * <p>
   * Read once, at construction: a built {@code HttpClient}'s connect timeout cannot change afterward, so each
   * caller builds its own client once (e.g. in its constructor) rather than per request.
   */
  public static HttpClient newConnectTimeoutBoundedClient(final ContextConfiguration configuration) {
    final long configured = configuration.getValueAsLong(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT);
    return HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(Math.max(configured, MIN_FORWARD_TIMEOUT_MS)))
        .build();
  }

  /**
   * How long a server's shutdown path waits for an {@link HttpClient} it is releasing to finish unwinding
   * before it abandons the wait and carries on (issue #7985).
   * <p>
   * Seconds rather than the in-flight request's own deadline, and that difference is the whole point.
   * {@link HttpClient#close()} is an orderly shutdown that waits for every submitted operation to complete, so
   * on the forward clients the wait it really signs up for is
   * {@link GlobalConfiguration#HA_PROXY_COMMAND_TIMEOUT} - one hour at its default - or
   * {@link GlobalConfiguration#HA_PROXY_READ_TIMEOUT}/{@link GlobalConfiguration#HA_PROXY_LONG_COMMAND_TIMEOUT}
   * for a forward issued through {@code LeaderCommandForwarder}. A follower with one forward parked on a leader
   * that accepted the connection and went silent would hold its own shutdown for that long, and in Kubernetes
   * {@code terminationGracePeriodSeconds} expires first and SIGKILL takes the rest of the shutdown with it.
   * <p>
   * A forward whose answer this node will never read is not worth holding a shutdown for, so the release
   * cancels it instead of waiting it out. Five seconds is a ceiling, not a sleep: the wait ends as soon as the
   * client reports itself terminated, which for a client with nothing in flight is immediate.
   */
  public static final long CLIENT_RELEASE_GRACE_MS = 5_000L;

  /**
   * Releases {@code client} promptly and without blocking the caller for longer than
   * {@link #CLIENT_RELEASE_GRACE_MS} (issue #7985).
   *
   * @see #releaseBounded(HttpClient, long)
   */
  public static boolean releaseBounded(final HttpClient client) {
    return releaseBounded(client, CLIENT_RELEASE_GRACE_MS);
  }

  /**
   * Releases {@code client} so that it has <b>terminated</b> by the time this returns, waiting at most
   * {@code graceMs} for that (issue #7985).
   * <p>
   * The two halves are what a shutdown path needs and what neither JDK call gives on its own:
   * <ul>
   * <li>{@code shutdownNow()} is prompt - it cancels the in-flight exchanges rather than waiting them out - but
   * it only <em>requests</em> the shutdown: the selector thread and the executor unwind after it returns, so
   * {@code isTerminated()} read straight afterwards is a race (issue #7677, the intermittently red
   * {@code Issue7507ForwarderClientLifecycleTest});</li>
   * <li>{@code close()} does leave the client terminated, but it waits for every submitted operation to
   * complete first, with no bound of its own (issue #7739).</li>
   * </ul>
   * So: {@code shutdownNow()} for promptness, then a bounded {@code awaitTermination} for the guarantee.
   * <p>
   * The grace expiring is reported and not thrown: it means one selector thread outlives this call, which is a
   * smaller problem than a shutdown that does not finish, and every caller is already on its way down.
   *
   * @param client  the client to release; {@code null} is accepted, for a caller whose client was never built
   * @param graceMs how long to wait for termination, clamped to {@link #MIN_FORWARD_TIMEOUT_MS} like every
   *                other bound in this class
   *
   * @return whether the client terminated within the grace
   */
  public static boolean releaseBounded(final HttpClient client, final long graceMs) {
    if (client == null)
      return true;

    // Cancels what is in flight instead of waiting for it. An exchange this node is shutting down on is one
    // whose answer it will never read.
    client.shutdownNow();

    final long grace = Math.max(graceMs, MIN_FORWARD_TIMEOUT_MS);
    try {
      if (client.awaitTermination(Duration.ofMillis(grace)))
        return true;
    } catch (final InterruptedException e) {
      // Shutdown is already interrupt-driven (RaftHAServer.stop() interrupts the auto-join thread, the
      // executors are ended with shutdownNow). Restore the flag and report what the client actually is rather
      // than waiting again on a thread that has been told to stop.
      Thread.currentThread().interrupt();
      return client.isTerminated();
    }

    LogManager.instance().log(LeaderDial.class, Level.WARNING,
        "An HTTP client did not finish shutting down within %,d ms; its selector thread is abandoned so that the "
            + "rest of the shutdown can proceed.", grace);
    return false;
  }

  /**
   * Said once per JVM: a cluster that names an HTTPS leader endpoint but cannot hand out a client for it has a
   * configuration fault worth naming in the log of the node that found it, not only in the answer its client gets.
   * One latch per JVM rather than per server, matching {@code PLAIN_HTTP_FALLBACK_WARNED} in
   * {@code SnapshotInstaller}, {@code PeerCapabilityQuery} and {@code LeaderDatabaseQuery} - the three other
   * one-time notices about an SSL cluster's peer transport.
   */
  private static final AtomicBoolean HTTPS_CLIENT_UNAVAILABLE_WARNED = new AtomicBoolean(false);

  /** True when the cluster requires TLS for this forward and it cannot be established; {@link #refusal} says why. */
  public boolean refused() {
    return refusal != null;
  }

  /** The URL this dial targets. {@code pathWithQuery} starts with {@code /} and may carry a query string. */
  public String url(final String pathWithQuery) {
    return (https ? "https://" : "http://") + address + pathWithQuery;
  }

  /**
   * Where to dial the current leader; {@code null} when no leader address is known at all - which the caller
   * reports as "the leader address is unknown", exactly as it did when it read {@code getLeaderAddress()} itself -
   * and a {@link #refused()} dial when the cluster requires TLS for this forward and it cannot be established.
   * <p>
   * The HTTPS endpoint wins when the plugin names one AND can hand out a client that trusts the cluster's
   * certificates. A plugin that names no HTTPS endpoint - the interface default, and what {@code RaftHAPlugin}
   * answers whenever SSL is off or none resolves - leaves the dial on the plain-HTTP address, the listener that is
   * always bound, exactly where it was. A plugin that names one and cannot serve a client for it gets a refusal
   * rather than a downgrade; see the class note on why those two are not the same case.
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
          return new LeaderDial(httpsAddress, true, httpsClient, null);
        return refuse(httpsAddress, "no HTTPS client is available to dial it with");
      } catch (final IOException e) {
        return refuse(httpsAddress, "its trust material cannot be read (" + e.getMessage() + ")");
      }
    }

    final String httpAddress = ha.getLeaderAddress();
    return httpAddress == null || httpAddress.isBlank() ? null : new LeaderDial(httpAddress, false, plainClient, null);
  }

  /**
   * A refusal naming the endpoint the cluster asked for and why it could not be reached, phrased to be appended to
   * a caller's own "cannot forward ..." message. The same shape {@code PeerDialAddress.refuse} uses.
   */
  private static LeaderDial refuse(final String httpsAddress, final String reason) {
    if (HTTPS_CLIENT_UNAVAILABLE_WARNED.compareAndSet(false, true))
      LogManager.instance().log(LeaderDial.class, Level.WARNING,
          "The cluster names an HTTPS endpoint for the leader (%s) but %s, so requests that have to run on the leader "
              + "are refused rather than forwarded over the plain-HTTP endpoint, which would put what they relay on "
              + "the wire in cleartext. Check the truststore named by '%s'. This notice is logged only once.",
          httpsAddress, reason, GlobalConfiguration.NETWORK_SSL_TRUSTSTORE.getKey());

    return new LeaderDial(null, false, null,
        "the cluster requires the leader to be reached over TLS at " + httpsAddress + ", but " + reason
            + "; the request is refused rather than forwarded in cleartext. Check the truststore named by '"
            + GlobalConfiguration.NETWORK_SSL_TRUSTSTORE.getKey() + "'");
  }
}
