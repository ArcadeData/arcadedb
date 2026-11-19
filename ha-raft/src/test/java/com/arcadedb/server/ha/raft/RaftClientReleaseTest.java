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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.SilentPeer;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Release of the HTTP and Raft clients a node replaces or drops: refresh in a leader-change callback, stop, and the trusted client cache.
 */
class RaftClientReleaseTest {

  /**
   * Regression test for issue #12142: {@code refreshRaftClient()}, which {@code notifyLeaderChanged} calls inside Ratis's
   * {@code setLeader} with the division's monitor held, closed the client it replaced inline. Closing a Ratis gRPC client
   * waits up to 3 s for the calls still in flight on its channels, so a leader change under load held the new leader (no
   * heartbeat, no commit) and every follower (no append, no vote) for 3 s.
   * <p>
   * The call in flight here is real: the client's peers are sockets that accept the connection and never answer, so the
   * call stays pending in the channel exactly as one waiting on a busy or paused peer does, and the channel's graceful
   * shutdown has something to wait for.
   */
  @Nested
  class RefreshDoesNotWaitForTheRetiredClient {

    private final List<ServerSocket> silentPeers = new CopyOnWriteArrayList<>();
    private final List<Socket>       accepted    = new CopyOnWriteArrayList<>();
    private       String             serverList;

    @BeforeEach
    void startSilentPeers() throws IOException {
      final int[] ports = allocateFreePorts(4);
      final StringBuilder list = new StringBuilder();
      for (int i = 0; i < 2; i++) {
        final ServerSocket peer = new ServerSocket(ports[i], 50, InetAddress.getLoopbackAddress());
        silentPeers.add(peer);
        Thread.ofVirtual().name("silent-raft-peer-" + i).start(() -> {
          try {
            while (!peer.isClosed())
              accepted.add(peer.accept());
          } catch (final IOException e) {
            // closed by the test
          }
        });
        if (i > 0)
          list.append(',');
        list.append("127.0.0.1:").append(ports[i]).append(':').append(ports[i + 2]);
      }
      serverList = list.toString();
    }

    @AfterEach
    void stopSilentPeers() throws IOException {
      for (final ServerSocket peer : silentPeers)
        peer.close();
      for (final Socket socket : accepted)
        socket.close();
    }

    // Regression for #12142: RefreshDoesNotWaitForTheRetiredClient
    @Test
    void aRefreshDoesNotWaitForTheCallsInFlightOnTheClientItReplaces() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);
      final RaftHAServer server = new RaftHAServer(TestServerHelper.unstartedServer("ArcadeDB_0"), config);
      server.buildParameters(config);
      server.setRaftPropertiesForTest(RaftPropertiesBuilder.build(config));
      server.refreshRaftClient();

      final RaftClient retired = server.getClient();
      final CompletableFuture<RaftClientReply> inFlight = retired.async().send(Message.valueOf("entry"));
      Awaitility.await("the call reached a peer").atMost(30, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS)
          .until(() -> !accepted.isEmpty());
      assertThat(inFlight).as("the peer never answers, so the call is still in flight").isNotDone();

      final StallAwareStopwatch clock = StallAwareStopwatch.start();
      server.refreshRaftClient();
      clock.assertGaveUpWithin(1_500L,
          "a refresh that leaves the retired client's close to another thread from one that waits out its 3 s graceful channel shutdown");

      assertThat(server.getClient()).isNotSameAs(retired);
      // The retired client is still closed, only not on this thread: its call ends instead of waiting out the retry policy.
      Awaitility.await("the retired client is closed").atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
          .until(inFlight::isDone);
      assertThat(inFlight).isCompletedExceptionally();
      server.getClient().close();
    }
  }

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
  @Nested
  class BoundedClientReleaseOnStop {

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

    // Regression for #7985: BoundedClientReleaseOnStop
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void stopReleasesTheForwardClientWithoutWaitingOutAParkedForward() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");

      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("localhost");

      final RaftHAServer raft = new RaftHAServer(arcadeServer, config);
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
    // Regression for #7985: BoundedClientReleaseOnStop
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void closingTheTrustedClientCacheDoesNotWaitOutAnInFlightRequest() throws Exception {
      final ContextConfiguration config = new ContextConfiguration();
      final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer((String) null, config);

      final TrustedHttpClientCache cache = new TrustedHttpClientCache();
      final HttpClient cached = cache.clientFor(arcadeServer);

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
  }

  /**
   * Issue #8025: {@link TrustedHttpClientCache#clientFor} released the client it was replacing with
   * {@code HttpClient.close()} - an orderly shutdown that waits for every submitted exchange to complete - while
   * holding the cache's monitor.
   * <p>
   * Its javadoc argued that was safe because "the one production caller is sequential on a single scheduled
   * thread". {@code RaftHAServer} holds the class twice, and neither instance has that property any more: the
   * forward instance is asked by every HTTP worker thread that forwards a request to the leader, and the capability
   * instance is also asked by {@code PeerAuthSessionQuery} on the request threads that validate or revoke an
   * authentication session. So a truststore rotation made the first thread to notice it wait out every other
   * thread's in-flight exchange - bounded only by the forward's own deadline, one hour at its default - while every
   * thread arriving meanwhile queued on the monitor behind it.
   * <p>
   * The parked exchanges here sit on a peer that accepts and then says nothing, which is exactly that state.
   * Against the unfixed code the rebuild does not come back at all: the test hangs until {@code @Timeout} fires.
   */
  @Nested
  class TrustedClientRebuildDoesNotBlock {

    private static final String TRUSTSTORE_DIR = "./target/test-truststore-8025";

    /** Parked exchanges on the client being replaced: one can hide a race, four do not. */
    private static final int IN_FLIGHT = 4;

    /** Stands in for {@code arcadedb.ha.proxyCommandTimeout}'s one hour: long enough to blow the @Timeout. */
    private static final Duration IN_FLIGHT_DEADLINE = Duration.ofMinutes(10);

    /**
     * The tripwire between a rebuild that does not wait for anything and one that waits out the parked exchanges.
     * It separates seconds from minutes, so widening it cannot turn a passing run red.
     */
    private static final long REBUILD_BOUND_MS = 60_000L;

    @AfterEach
    void cleanup() {
      FileUtils.deleteRecursively(new File(TRUSTSTORE_DIR));
    }

    /**
     * The defect on the class itself: a rebuild with exchanges in flight on the previous client returns promptly,
     * and does so WITHOUT cancelling them. The live-path alternative the issue suggested - the shutdown path's
     * {@code LeaderDial.releaseBounded}, which cancels - would abort forwards that other threads are still waiting
     * on, turning a certificate rotation into a burst of failed writes whose outcome on the leader is unknown.
     */
    // Regression for #8025: TrustedClientRebuildDoesNotBlock
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void aRotationDoesNotWaitForNorCancelTheExchangesInFlightOnThePreviousClient() throws Exception {
      final File truststore = writeEmptyTruststore();
      final ArcadeDBServer server = serverWithTruststore(truststore);
      final TrustedHttpClientCache cache = new TrustedHttpClientCache();
      try {
        assertRotationDoesNotBlock(() -> cache.clientFor(server), truststore);
      } finally {
        cache.close();
      }
    }

    /**
     * The same through {@code RaftHAServer.getForwardHttpsClient()} - the instance that {@code LeaderDial.resolve}
     * asks on every follower-to-leader forward, from many worker threads at once.
     */
    // Regression for #8025: TrustedClientRebuildDoesNotBlock
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void theLeaderForwardClientRebuildDoesNotBlock() throws Exception {
      final File truststore = writeEmptyTruststore();
      final RaftHAServer raft = raftServerWithTruststore(truststore);
      try {
        assertRotationDoesNotBlock(raft::getForwardHttpsClient, truststore);
      } finally {
        raft.stop();
      }
    }

    /**
     * The same through {@code RaftHAServer.getHttpsClients()} - the capability probe's instance, which
     * {@code PeerAuthSessionQuery} also asks from the request threads validating or revoking a session, so its
     * "single scheduled thread" was not true either.
     */
    // Regression for #8025: TrustedClientRebuildDoesNotBlock
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void thePeerRpcClientRebuildDoesNotBlock() throws Exception {
      final File truststore = writeEmptyTruststore();
      final RaftHAServer raft = raftServerWithTruststore(truststore);
      try {
        assertRotationDoesNotBlock(() -> raft.getHttpsClients().clientFor(raft.getServer()), truststore);
      } finally {
        raft.stop();
      }
    }

    /**
     * A client retired by a rotation is still this cache's to release: the server's {@code stop()} makes one
     * {@code close()} call, and a retired client still draining a straggler must be terminated by it rather than
     * left holding a selector thread until the straggler's own deadline - the leak #7985 bounded on the current
     * client.
     */
    // Regression for #8025: TrustedClientRebuildDoesNotBlock
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void closeAlsoReleasesAClientRetiredByARotationThatIsStillDraining() throws Exception {
      final File truststore = writeEmptyTruststore();
      final ArcadeDBServer server = serverWithTruststore(truststore);
      final TrustedHttpClientCache cache = new TrustedHttpClientCache();

      final HttpClient previous = cache.clientFor(server);
      try (final SilentPeer peer = SilentPeer.start()) {
        final List<CompletableFuture<HttpResponse<String>>> parked = park(previous, peer);

        rotate(truststore);
        final HttpClient current = cache.clientFor(server);
        assertThat(current).isNotSameAs(previous);
        assertThat(previous.isTerminated()).as("still draining the parked exchanges").isFalse();

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        cache.close();
        watch.assertGaveUpWithin(REBUILD_BOUND_MS,
            "a close bounded by LeaderDial.CLIENT_RELEASE_GRACE_MS from one that waits out a retired client's straggler");

        assertThat(current.isTerminated()).as("the current client is released").isTrue();
        assertThat(previous.isTerminated())
            .as("the client retired by the rotation is released by the same close(), not left to its straggler")
            .isTrue();
        // The JDK completes a shut-down client's pending exchanges on the common pool, after and independently of its
        // termination, so they are awaited rather than read at once (issue #8696). The bound only detects a hang: an
        // exchange nothing ever completes is the straggler this test is about
        assertThat(CompletableFuture.allOf(parked.toArray(new CompletableFuture[0])).exceptionally(e -> null))
            .as("every exchange parked on the retired client is completed once it is released")
            .succeedsWithin(Duration.ofSeconds(30));
      }
    }

    /**
     * Retired clients that have finished draining are dropped from the cache rather than accumulated, so an
     * operator who rotates certificates repeatedly does not grow the cache for the life of the server.
     */
    // Regression for #8025: TrustedClientRebuildDoesNotBlock
    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void aRetiredClientWithNothingInFlightTerminatesOnItsOwn() throws Exception {
      final File truststore = writeEmptyTruststore();
      final ArcadeDBServer server = serverWithTruststore(truststore);
      final TrustedHttpClientCache cache = new TrustedHttpClientCache();
      try {
        HttpClient previous = cache.clientFor(server);
        for (int i = 1; i <= 3; i++) {
          rotate(truststore);
          final HttpClient current = cache.clientFor(server);
          assertThat(current).isNotSameAs(previous);
          assertThat(previous.awaitTermination(Duration.ofSeconds(30)))
              .as("an idle retired client terminates without anyone closing it")
              .isTrue();
          previous = current;
        }
        assertThat(cache.retiredCount()).as("terminated retired clients are not kept").isLessThanOrEqualTo(1);
      } finally {
        cache.close();
      }
    }

    @FunctionalInterface
    private interface ClientSource {
      HttpClient get() throws Exception;
    }

    private static void assertRotationDoesNotBlock(final ClientSource source, final File truststore) throws Exception {
      final HttpClient previous = source.get();
      try (final SilentPeer peer = SilentPeer.start()) {
        final List<CompletableFuture<HttpResponse<String>>> parked = park(previous, peer);

        rotate(truststore);
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final HttpClient rebuilt = source.get();
        watch.assertGaveUpWithin(REBUILD_BOUND_MS,
            "a rebuild that hands back the new client at once from one that holds the cache's monitor until every "
                + "exchange in flight on the previous client has reached its own deadline");

        assertThat(rebuilt).as("the rotation is picked up").isNotSameAs(previous);
        assertThat(source.get()).as("and the new client is what later callers get").isSameAs(rebuilt);
        assertThat(parked)
            .as("the exchanges other threads are still waiting on are left to finish, not cancelled")
            .noneMatch(CompletableFuture::isDone);

        // Once the stragglers finish, the retired client unwinds on its own.
        parked.forEach(f -> f.cancel(true));
        assertThat(previous.awaitTermination(Duration.ofSeconds(30)))
            .as("the retired client terminates once its last exchange completes")
            .isTrue();
      }
    }

    private static List<CompletableFuture<HttpResponse<String>>> park(final HttpClient client, final SilentPeer peer)
        throws InterruptedException {
      peer.expect(IN_FLIGHT);
      final List<CompletableFuture<HttpResponse<String>>> parked = new ArrayList<>(IN_FLIGHT);
      for (int i = 0; i < IN_FLIGHT; i++)
        parked.add(client.sendAsync(
            HttpRequest.newBuilder(URI.create("http://" + peer.address() + "/api/v1/server?forward=" + i))
                .timeout(IN_FLIGHT_DEADLINE)
                .GET().build(),
            HttpResponse.BodyHandlers.ofString()));
      peer.awaitOnTheWire();
      return parked;
    }

    /** Same path, new timestamp: what an operator's certificate rotation produces, and what the cache detects. */
    private static void rotate(final File truststore) {
      assertThat(truststore.setLastModified(truststore.lastModified() + 5_000L)).isTrue();
    }

    private static File writeEmptyTruststore() throws Exception {
      final File dir = new File(TRUSTSTORE_DIR);
      dir.mkdirs();
      final File store = new File(dir, "truststore.jks");
      final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(null, null);
      try (final OutputStream out = Files.newOutputStream(store.toPath())) {
        ks.store(out, "changeit".toCharArray());
      }
      return store;
    }

    private static ContextConfiguration configurationWithTruststore(final File truststore) {
      final ContextConfiguration configuration = new ContextConfiguration();
      configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, truststore.getAbsolutePath());
      configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, "changeit");
      return configuration;
    }

    private static ArcadeDBServer serverWithTruststore(final File truststore) {
      final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, configurationWithTruststore(truststore));
      return server;
    }

    /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
    private static RaftHAServer raftServerWithTruststore(final File truststore) {
      final ContextConfiguration configuration = configurationWithTruststore(truststore);
      configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");

      final ArcadeDBServer server = TestServerHelper.unstartedServer("localhost", configuration);
      return new RaftHAServer(server, configuration);
    }
  }
}
