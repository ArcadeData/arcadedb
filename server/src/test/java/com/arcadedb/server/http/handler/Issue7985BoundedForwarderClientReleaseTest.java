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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7985, the half reported as #7677: {@code Issue7507ForwarderClientLifecycleTest} asserts that
 * {@code server.stop()} returns only once the forwarder's {@link HttpClient} has <em>terminated</em>, and it was
 * intermittently red on {@code main} while passing locally.
 * <p>
 * {@code HttpClient.shutdownNow()} only <b>requests</b> the shutdown: it cancels the in-flight exchanges and
 * returns, and the selector thread and the client's executor unwind afterwards. So {@code isTerminated()} read
 * straight after it is a race - one an idle client usually wins and a client with exchanges to cancel usually
 * loses. Measured on a JDK 21 runtime with four forwards parked on a silent peer, {@code isTerminated()} was
 * false immediately after {@code shutdownNow()} in 31 of 50 cycles with the machine otherwise idle, and in 131
 * of 200 with every core busy - which is why CI saw it and a developer's laptop did not.
 * <p>
 * That is why {@link #closingTheTransportLeavesItsClientTerminatedEveryTime()} drives the release
 * {@value #RELEASE_CYCLES} times rather than once: a single cycle passes against the unfixed code most of the
 * time, and a regression test that reports a coin flip is what let this reach {@code main}.
 *
 * @see LeaderDial#releaseBounded(HttpClient, long)
 */
class Issue7985BoundedForwarderClientReleaseTest {

  /**
   * Enough cycles that the unfixed release - which terminates in time roughly two times in five - cannot pass by
   * luck, and few enough that the whole class costs under a second.
   */
  private static final int RELEASE_CYCLES = 25;

  /** Parked forwards per cycle: one exchange is often cancelled fast enough to hide the race, four are not. */
  private static final int FORWARDS_PER_CYCLE = 4;

  /**
   * The deadline the parked forwards carry. Ten minutes so that a release which waited them out instead of
   * cancelling them would blow the {@code @Timeout} rather than merely being slow - the shape of issue #7739,
   * where the real bound was {@code arcadedb.ha.proxyCommandTimeout}'s one-hour default.
   */
  private static final Duration IN_FLIGHT_FORWARD_DEADLINE = Duration.ofMinutes(10);

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void closingTheTransportLeavesItsClientTerminatedEveryTime() throws Exception {
    try (final SilentPeer peer = SilentPeer.start()) {
      int stillRunning = 0;

      for (int cycle = 0; cycle < RELEASE_CYCLES; cycle++) {
        final LeaderCommandForwarder.Transport transport =
            new LeaderCommandForwarder.Transport(new ContextConfiguration());
        final List<CompletableFuture<HttpResponse<String>>> parked = parkForwards(transport.client(), peer);

        transport.close();

        if (!transport.client().isTerminated())
          stillRunning++;

        parked.forEach(f -> f.cancel(true));
      }

      assertThat(stillRunning)
          .as("Transport.close() must leave the forwarder's HTTP client terminated, not merely told to stop, "
              + "on every one of %d cycles", RELEASE_CYCLES)
          .isZero();
    }
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void closingTheTransportIsBoundedByTheGraceNotByTheForwardsOwnDeadline() throws Exception {
    try (final SilentPeer peer = SilentPeer.start()) {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(new ContextConfiguration());
      final List<CompletableFuture<HttpResponse<String>>> parked = parkForwards(transport.client(), peer);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      transport.close();
      watch.assertGaveUpWithin(60_000L,
          "a release bounded by LeaderDial.CLIENT_RELEASE_GRACE_MS from one that waits out the parked forwards' "
              + "own " + IN_FLIGHT_FORWARD_DEADLINE.toMinutes() + "-minute deadline");

      parked.forEach(f -> f.cancel(true));
    }
  }

  /**
   * The grace is a ceiling on the wait, not a sleep: a client with nothing to cancel terminates well inside it,
   * so an orderly shutdown does not pay five seconds per client on the way down.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void releasingAnIdleClientDoesNotPayTheGrace() {
    final HttpClient client = LeaderDial.newConnectTimeoutBoundedClient(new ContextConfiguration());

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThat(LeaderDial.releaseBounded(client))
        .as("an idle client terminates inside the grace")
        .isTrue();
    watch.assertGaveUpWithin(LeaderDial.CLIENT_RELEASE_GRACE_MS,
        "a release that returns as soon as the client is terminated from one that always waits out the grace");

    assertThat(client.isTerminated()).as("released").isTrue();
  }

  /** A {@code null} client is a caller whose client was never built; releasing it is a no-op, not an NPE. */
  @Test
  void releasingAClientThatWasNeverBuiltIsANoOp() {
    assertThat(LeaderDial.releaseBounded(null)).isTrue();
  }

  private static List<CompletableFuture<HttpResponse<String>>> parkForwards(final HttpClient client,
      final SilentPeer peer) throws InterruptedException {
    final List<CompletableFuture<HttpResponse<String>>> parked = new ArrayList<>(FORWARDS_PER_CYCLE);
    for (int i = 0; i < FORWARDS_PER_CYCLE; i++)
      parked.add(client.sendAsync(
          HttpRequest.newBuilder(URI.create("http://" + peer.address() + "/api/v1/server?forward=" + i))
              .timeout(IN_FLIGHT_FORWARD_DEADLINE)
              .GET().build(),
          HttpResponse.BodyHandlers.ofString()));

    // Let the exchanges reach the peer, so the release below has something to cancel rather than an idle client.
    Thread.sleep(20);
    return parked;
  }

  /**
   * A listener that accepts connections and then says nothing at all - the leader in a long stop-the-world
   * pause, or behind a partition that does not RST, which is the state issue #7739 describes an operator
   * restarting into.
   */
  static final class SilentPeer implements Closeable {
    private final ServerSocket listener;
    private final Thread       acceptor;
    private final List<Socket> accepted = new ArrayList<>();

    private SilentPeer(final ServerSocket listener) {
      this.listener = listener;
      this.acceptor = new Thread(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            final Socket socket = listener.accept();
            synchronized (accepted) {
              accepted.add(socket);
            }
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
