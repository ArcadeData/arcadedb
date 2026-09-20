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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7985, the half reported as #7739: {@link RaftHAServer#stop()} released its HTTP clients with
 * {@code HttpClient.close()}, which is an orderly shutdown that waits for every submitted operation to complete.
 * <p>
 * The comment beside {@code forwardHttpClient.close()} said the wait was "bounded by that request's own timeout",
 * which is true and reads as reassuring; the number is not. A forward's bound is
 * {@code arcadedb.ha.proxyCommandTimeout}, <b>one hour</b> at its default. And the close ran before
 * {@code stalledResyncExecutor.shutdownNow()}, {@code channelRecoveryExecutor.shutdownNow()},
 * {@code transactionBroker.stop()} and the Raft client/server close, so one forward parked on a leader that
 * accepted the connection and went silent held the <em>whole</em> HA shutdown - long past the
 * {@code terminationGracePeriodSeconds} after which Kubernetes SIGKILLs the pod and none of the rest runs.
 * <p>
 * Both tests park forwards on a peer that accepts and then says nothing, which is exactly that state. Against
 * the unfixed code they do not fail on an assertion: they hang until the {@code @Timeout} below fires, which is
 * the defect stated as precisely as it can be.
 */
class Issue7985BoundedClientReleaseOnStopTest {

  /** Parked exchanges per client: one is often cancelled fast enough to hide the race, four are not. */
  private static final int FORWARDS_PER_CLIENT = 4;

  /**
   * The deadline the parked forwards carry, standing in for {@code arcadedb.ha.proxyCommandTimeout}'s one-hour
   * default: long enough that a release which waits it out blows the {@code @Timeout} instead of being slow.
   */
  private static final Duration IN_FLIGHT_FORWARD_DEADLINE = Duration.ofMinutes(10);

  /**
   * The tripwire between a bounded release and one that waits out the parked forward. Generous on purpose - it
   * separates seconds from minutes, so widening it cannot turn a passing run red, and the assertion still fails
   * against the unfixed code, which cannot come back inside ten minutes.
   */
  private static final long SHUTDOWN_BOUND_MS = 60_000L;

  @Test
  @Timeout(value = 180, unit = TimeUnit.SECONDS)
  void stopReleasesTheForwardClientWithoutWaitingOutAParkedForward() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("localhost");

    final RaftHAServer raft = new RaftHAServer(mockServer, config);
    final HttpClient forwardClient = raft.getForwardHttpClient();

    try (final SilentPeer peer = SilentPeer.start()) {
      final List<CompletableFuture<HttpResponse<String>>> parked = parkForwards(forwardClient, peer);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      raft.stop();
      watch.assertGaveUpWithin(SHUTDOWN_BOUND_MS,
          "an HA shutdown bounded by LeaderDial.CLIENT_RELEASE_GRACE_MS from one that waits out a forward parked "
              + "on an unresponsive leader - the whole of stop() runs after this close");

      assertThat(forwardClient.isTerminated())
          .as("stop() must return with the shared forward client terminated, not merely told to stop")
          .isTrue();

      parked.forEach(f -> f.cancel(true));
    }
  }

  /**
   * The same bound on the two {@link TrustedHttpClientCache}s {@code stop()} closes just before it. Its own
   * javadoc claims the straggler that can delay this close is a capability probe, bounded by
   * {@code PeerCapabilityRegistry.PROBE_TIMEOUT_MS} - true of {@code capabilityHttpsClients} and not of
   * {@code forwardHttpsClients}, which carries the HTTPS half of exactly the forwards above. And the close is
   * {@code synchronized}, so the wait was held under the monitor a straggling caller needs.
   */
  @Test
  @Timeout(value = 180, unit = TimeUnit.SECONDS)
  void closingTheTrustedClientCacheDoesNotWaitOutAnInFlightRequest() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getConfiguration()).thenReturn(config);

    final TrustedHttpClientCache cache = new TrustedHttpClientCache();
    final HttpClient cached = cache.clientFor(mockServer);

    try (final SilentPeer peer = SilentPeer.start()) {
      final List<CompletableFuture<HttpResponse<String>>> parked = parkForwards(cached, peer);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      cache.close();
      watch.assertGaveUpWithin(SHUTDOWN_BOUND_MS,
          "a bounded release of the cached peer client from one that waits out an in-flight peer dial while "
              + "holding this cache's monitor");

      assertThat(cached.isTerminated())
          .as("close() must return with the cached client terminated")
          .isTrue();

      parked.forEach(f -> f.cancel(true));
    }
  }

  /**
   * Sends {@value #FORWARDS_PER_CLIENT} forwards to the silent peer and returns only once every one of them
   * has genuinely reached it, so the release under test always has an exchange to cancel.
   */
  private static List<CompletableFuture<HttpResponse<String>>> parkForwards(final HttpClient client,
      final SilentPeer peer) throws InterruptedException {
    peer.expect(FORWARDS_PER_CLIENT);

    final List<CompletableFuture<HttpResponse<String>>> parked = new ArrayList<>(FORWARDS_PER_CLIENT);
    for (int i = 0; i < FORWARDS_PER_CLIENT; i++)
      parked.add(client.sendAsync(
          HttpRequest.newBuilder(URI.create("http://" + peer.address() + "/api/v1/server?forward=" + i))
              .timeout(IN_FLIGHT_FORWARD_DEADLINE)
              .GET().build(),
          HttpResponse.BodyHandlers.ofString()));

    peer.awaitOnTheWire();
    return parked;
  }

  /**
   * A listener that accepts connections and then says nothing at all: the leader in a long stop-the-world pause,
   * behind a partition that does not RST, or behind an iptables DROP.
   * <p>
   * It also reports when a parked exchange is genuinely <b>on the wire</b>: every accepted connection gets a
   * reader that blocks for the first byte of the request and only then counts down the latch {@link #expect(int)}
   * armed. A fixed sleep in its place establishes nothing - on a slow runner the release under test would be
   * handed an idle client, which even the unfixed code releases correctly, so the regression test would pass
   * for the wrong reason (CodeRabbit review on PR #8026).
   */
  static final class SilentPeer implements Closeable {
    private final    ServerSocket   listener;
    private final    Thread         acceptor;
    private final    List<Socket>   accepted  = new ArrayList<>();
    private volatile CountDownLatch onTheWire = new CountDownLatch(0);

    private SilentPeer(final ServerSocket listener) {
      this.listener = listener;
      this.acceptor = new Thread(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            final Socket socket = listener.accept();
            synchronized (accepted) {
              accepted.add(socket);
            }
            // The latch is read HERE, not inside the reader: a connection accepted after expect() belongs to
            // the round expect() armed, and one accepted before it counts down the round it was opened for.
            countDownWhenRequestArrives(socket, onTheWire);
          }
        } catch (final IOException ignored) {
          // the listener was closed while this test was tearing down
        }
      }, "issue7985-silent-peer");
      this.acceptor.setDaemon(true);
      this.acceptor.start();
    }

    static SilentPeer start() throws IOException {
      return new SilentPeer(new ServerSocket(0, 256, InetAddress.getLoopbackAddress()));
    }

    String address() {
      return listener.getInetAddress().getHostAddress() + ":" + listener.getLocalPort();
    }

    /** Arms the latch for the next {@code exchanges} requests. Called before they are sent. */
    void expect(final int exchanges) {
      onTheWire = new CountDownLatch(exchanges);
    }

    /** Blocks until every request armed by {@link #expect(int)} has arrived here. */
    void awaitOnTheWire() throws InterruptedException {
      assertThat(onTheWire.await(30, TimeUnit.SECONDS))
          .as("every parked request reached the silent peer, so the release under test has an exchange to cancel")
          .isTrue();
    }

    /**
     * The peer never answers, so the only thing this reader does is prove the request was written: one blocking
     * read of its first byte, and no response ever.
     */
    private static void countDownWhenRequestArrives(final Socket socket, final CountDownLatch latch) {
      final Thread reader = new Thread(() -> {
        try {
          if (socket.getInputStream().read() >= 0)
            latch.countDown();
        } catch (final IOException ignored) {
          // closed by the release under test or by teardown; either way nothing of this test's is in flight
        }
      }, "issue7985-silent-peer-reader");
      reader.setDaemon(true);
      reader.start();
    }

    @Override
    public void close() throws IOException {
      listener.close();
      acceptor.interrupt();
      synchronized (accepted) {
        for (final Socket socket : accepted)
          try {
            socket.close();
          } catch (final IOException ignored) {
            // best effort: this test is finished with it either way
          }
      }
    }
  }
}
