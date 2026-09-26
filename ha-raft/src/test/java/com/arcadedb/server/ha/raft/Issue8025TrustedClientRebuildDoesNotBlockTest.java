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
import com.arcadedb.server.http.SilentPeer;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.OutputStream;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
class Issue8025TrustedClientRebuildDoesNotBlockTest {

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
      assertThat(parked).allMatch(CompletableFuture::isDone);
    }
  }

  /**
   * Retired clients that have finished draining are dropped from the cache rather than accumulated, so an
   * operator who rotates certificates repeatedly does not grow the cache for the life of the server.
   */
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
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configurationWithTruststore(truststore));
    return server;
  }

  /** A {@link RaftHAServer} whose constructor has run but whose Ratis server was never started. */
  private static RaftHAServer raftServerWithTruststore(final File truststore) {
    final ContextConfiguration configuration = configurationWithTruststore(truststore);
    configuration.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getServerName()).thenReturn("localhost");
    when(server.getConfiguration()).thenReturn(configuration);
    return new RaftHAServer(server, configuration);
  }
}
