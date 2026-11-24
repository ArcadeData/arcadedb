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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.StaticBaseServerTest;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.SilentPeer;
import com.arcadedb.server.http.ws.WebSocketEventBus;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The lifecycle of the {@code HttpClient}s the follower-to-leader forwarders own: each is released when the HTTP server stops.
 * <p>
 * One {@code @Nested} class per regression, each keeping the fixture and the Javadoc of the issue it pins.
 */
class ForwarderClientLifecycleTest {

  /**
   * Issue #7507: bounding the forward meant giving {@link LeaderCommandForwarder} its own {@code HttpClient}
   * (built with the server's configured connect timeout) in place of the JVM-wide static one it used before. A
   * client owns a selector thread and an executor, and there is now one per {@code HttpServer}, so the server has
   * to release it on the way down.
   */
  @Nested
  class StopReleasesForwarderClient {

    private static final String DATABASE_DIRECTORY = "./target/databases-issue7507";

    @Test
    void stoppingTheServerReleasesTheForwardersHttpClient() throws Exception {
      FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_issue7507");
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DATABASE_DIRECTORY);
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "DefaultPasswordForTests123!");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      // A free port, not the default 2480: this test must not fight whatever else is listening there.
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(StaticBaseServerTest.allocateFreePorts(1)[0]));

      final ArcadeDBServer server = new ArcadeDBServer(config);
      final LeaderCommandForwarder forwarder;
      try {
        server.start();
        forwarder = server.getHttpServer().getLeaderCommandForwarder();

        assertThat(forwarder.transport().client().connectTimeout())
            .as("the forwarder dials the leader with a bounded client")
            .isPresent();
        assertThat(forwarder.transport().client().isTerminated())
            .as("the client is live while the server is up")
            .isFalse();
      } finally {
        server.stop();
      }

      assertThat(forwarder.transport().client().isTerminated())
          .as("stopping the server releases the forwarder's HTTP client")
          .isTrue();

      FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
    }
  }

  /**
   * Issue #7985, the half reported as #7677: {@code ForwarderClientLifecycleTest.StopReleasesForwarderClient} asserts that
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
  @Nested
  class BoundedForwarderClientRelease {

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

    /**
     * Sends {@value #FORWARDS_PER_CYCLE} forwards to the silent peer and returns only once every one of them
     * has genuinely reached it, so the release under test always has an exchange to cancel.
     */
    private static List<CompletableFuture<HttpResponse<String>>> parkForwards(final HttpClient client,
        final SilentPeer peer) throws InterruptedException {
      peer.expect(FORWARDS_PER_CYCLE);

      final List<CompletableFuture<HttpResponse<String>>> parked = new ArrayList<>(FORWARDS_PER_CYCLE);
      for (int i = 0; i < FORWARDS_PER_CYCLE; i++)
        parked.add(client.sendAsync(
            HttpRequest.newBuilder(URI.create("http://" + peer.address() + "/api/v1/server?forward=" + i))
                .timeout(IN_FLIGHT_FORWARD_DEADLINE)
                .GET().build(),
            HttpResponse.BodyHandlers.ofString()));

      peer.awaitOnTheWire();
      return parked;
    }
  }

  /**
   * Issue #7985, the second root cause behind #7677: the forwarder's {@code HttpClient} was released by the LAST
   * statement of {@link HttpServer#stopService()}, and only {@code undertow.stop()} in front of it was guarded. A
   * throw from any earlier step skipped the release - and {@code ArcadeDBServer.stopInternal()} calls
   * {@code stopService()} inside {@code CodeUtils.executeIgnoringExceptions}, so the throw never reached a caller
   * either. The server reported a clean stop while holding the client, its connection pool and its selector thread
   * for the life of the JVM, which is precisely the symptom {@code ForwarderClientLifecycleTest.StopReleasesForwarderClient} reports.
   * <p>
   * The sibling test {@code ForwarderClientLifecycleTest.BoundedForwarderClientRelease} pins the other half - that the release,
   * once reached, leaves the client terminated within a bound. This one pins that it is reached at all.
   * <p>
   * <b>On the reflection.</b> Every collaborator {@code stopService()} touches is a {@code final} field assigned in
   * the {@code HttpServer} constructor, so there is no setter and no seam to pass a failing one through. The
   * failure is therefore injected by swapping in a real subclass of {@link WebSocketEventBus} that throws from
   * {@code stop()} - a real object of a real type rather than a mock, which is what this repository's testing
   * guidelines ask for; only the assignment needs reflection.
   */
  @Nested
  class StopServiceReleasesForwarderOnFailure {

    private static final String DATABASE_DIRECTORY = "./target/databases-issue7985";

    /**
     * The first step of {@link HttpServer#stopService()}. Anything that throws before the release would do; this
     * one is chosen because it is the furthest from the release, so it proves every step in between was guarded
     * rather than only the last one.
     */
    static final class FailingWebSocketEventBus extends WebSocketEventBus {
      FailingWebSocketEventBus(final ArcadeDBServer server) {
        super(server);
      }

      @Override
      public void stop() {
        throw new IllegalStateException("induced failure in the first step of HttpServer.stopService() (issue #7985)");
      }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void aThrowFromAnEarlierStepStillReleasesTheForwardersHttpClient() throws Exception {
      FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_NAME, "ArcadeDB_issue7985");
      config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, DATABASE_DIRECTORY);
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, "DefaultPasswordForTests123!");
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, "localhost");
      // A free port, not the default 2480: this test must not fight whatever else is listening there.
      config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(StaticBaseServerTest.allocateFreePorts(1)[0]));

      final ArcadeDBServer server = new ArcadeDBServer(config);
      final LeaderCommandForwarder forwarder;
      try {
        server.start();

        final HttpServer httpServer = server.getHttpServer();
        forwarder = httpServer.getLeaderCommandForwarder();
        assertThat(forwarder.transport().client().isTerminated())
            .as("the client is live while the server is up")
            .isFalse();

        failTheFirstStepOfStopService(httpServer, server);
      } finally {
        // stopInternal() swallows whatever stopService() throws, so this returns either way. What must not happen
        // is that it returns with the client still running.
        server.stop();
      }

      assertThat(forwarder.transport().client().isTerminated())
          .as("a failing step earlier in stopService() must not skip the release of the forwarder's HTTP client")
          .isTrue();

      FileUtils.deleteRecursively(new File(DATABASE_DIRECTORY));
    }

    private static void failTheFirstStepOfStopService(final HttpServer httpServer, final ArcadeDBServer server)
        throws ReflectiveOperationException {
      final Field field = HttpServer.class.getDeclaredField("webSocketEventBus");
      field.setAccessible(true);
      field.set(httpServer, new FailingWebSocketEventBus(server));
    }
  }

  /**
   * Issue #8024: {@link PostBatchHandler} builds its own follower-to-leader {@link HttpClient} - which owns a selector
   * thread, an executor and a connection pool - and nothing ever released it. One handler is built per
   * {@link HttpServer} start, so every start/stop cycle of an in-process server leaked one client for the life of
   * the JVM. This pins the release at the {@code HttpServer} lifecycle level: the client the live
   * {@code /api/v1/batch} route dials on is terminated once the server stops, and is so even when an earlier step
   * of {@link HttpServer#stopService()} throws (the #7985 guard shape).
   * <p>
   * <b>On the reflection.</b> The handler is registered inline on the route table, and its client is a private
   * field; neither is an API anyone else needs, so the test reads both reflectively rather than widening either.
   */
  @Nested
  class BatchHandlerClientLifecycle extends BaseGraphServerTest {

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void stoppingTheServerReleasesTheBatchHandlersHttpClient() throws Exception {
      final ArcadeDBServer server = getServer(0);
      final HttpClient client = batchClientOf(server.getHttpServer());
      assertThat(client.isTerminated()).as("the batch forward client is live while the server is up").isFalse();

      server.stop();

      assertThat(client.isTerminated()).as("stopping the server releases the batch handler's HTTP client").isTrue();

      // A restart builds a fresh HttpServer and a fresh handler: the route must not be left dialing on the client
      // the previous stop released.
      server.start();
      final HttpClient restarted = batchClientOf(server.getHttpServer());
      assertThat(restarted).isNotSameAs(client);
      assertThat(restarted.isTerminated()).as("the restarted server's batch forward client is live").isFalse();
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void aThrowFromAnEarlierStepStillReleasesTheBatchHandlersHttpClient() throws Exception {
      final ArcadeDBServer server = getServer(0);
      final HttpServer httpServer = server.getHttpServer();
      final HttpClient client = batchClientOf(httpServer);
      assertThat(client.isTerminated()).isFalse();

      final Field bus = HttpServer.class.getDeclaredField("webSocketEventBus");
      bus.setAccessible(true);
      bus.set(httpServer, new ForwarderClientLifecycleTest.StopServiceReleasesForwarderOnFailure.FailingWebSocketEventBus(server));

      // stopInternal() swallows whatever stopService() throws, so this returns either way.
      server.stop();

      assertThat(client.isTerminated())
          .as("a failing step earlier in stopService() must not skip the release of the batch handler's HTTP client")
          .isTrue();
      // No restart here: BaseGraphServerTest.endTest() restarts any server that is down before its teardown checks,
      // and does so whether or not an assertion above failed.
    }

    private static HttpClient batchClientOf(final HttpServer httpServer) throws ReflectiveOperationException {
      final Field handlerField = HttpServer.class.getDeclaredField("postBatchHandler");
      handlerField.setAccessible(true);
      final PostBatchHandler handler = (PostBatchHandler) handlerField.get(httpServer);
      assertThat(handler).as("the HttpServer keeps the /batch handler it registered").isNotNull();

      final Field clientField = PostBatchHandler.class.getDeclaredField("httpClient");
      clientField.setAccessible(true);
      return (HttpClient) clientField.get(handler);
    }
  }
}
