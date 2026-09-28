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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import com.sun.net.httpserver.HttpServer;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8472, the peer-to-peer queries #8325 left out: each one bounded the peer with the JDK
 * request timeout alone and read its answer with {@code BodyHandlers.ofString()}. On JDK 21-25 that timeout stops at
 * the response headers, so a peer that sent {@code 200} with a {@code Content-Length} and then stalled inside its body
 * parked the calling thread with no bound at all. Each entry point is driven through its own code here, against a peer
 * that does exactly that; without the fix every one of them waits on it until the hang detector gives up.
 * <p>
 * Every site dials its peer over plain HTTP or over HTTPS, and the HTTPS branch builds or borrows a client of its own
 * - one it may then close, which is where a hang could hide behind a deadline that did fire - so each site is driven
 * over both, the HTTPS one against a peer that completes the handshake and then stalls. The HTTPS branch of
 * {@code LeaderDatabaseQuery}, which #8325 bounded without a test of its own, is driven here as well.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8472PeerQueryBodyDeadlineTest {

  /** The tripwire between "the deadline fired" and "the call is unbounded" (forever, without the fix). */
  private static final long GAVE_UP_BOUND_MS = 15_000L;
  /** The deadline every query under test carries. */
  private static final long TIMEOUT_MS       = 1_000L;

  private static RaftTestPki pki;

  @BeforeAll
  static void generatePki() throws Exception {
    pki = RaftTestPki.create(Path.of("target", "test-pki-8472"), "stall");
  }

  // ---- PeerAuthSessionQuery.validate: the HTTP worker authenticating a token another node issued ----------------

  @Test
  void anAuthSessionQueryWhoseIssuerStallsInsideItsBodyIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer issuer = new StallingBodyPeer()) {
      final RaftPeerId issuerId = RaftPeerId.valueOf("issuer");
      final RaftHAServer raft = raftDialling(issuerId, plainServer(), issuer.address(), null, null);
      assertGivenUpOn(issuer, () -> PeerAuthSessionQuery.validate(raft, issuerId, "session-token", TIMEOUT_MS));
    }
  }

  @Test
  void anAuthSessionQueryWhoseIssuerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
    final TrustedHttpClientCache clients = new TrustedHttpClientCache();
    try (final StallingBodyPeer issuer = new StallingBodyPeer(tls())) {
      final RaftPeerId issuerId = RaftPeerId.valueOf("issuer");
      // The plain address is never dialled: SSL is on and the issuer has an HTTPS endpoint of its own.
      final RaftHAServer raft = raftDialling(issuerId, tlsServer(), "127.0.0.1:1", issuer.address(), clients);
      assertGivenUpOn(issuer, () -> PeerAuthSessionQuery.validate(raft, issuerId, "session-token", TIMEOUT_MS));
    } finally {
      clients.close();
    }
  }

  /**
   * {@code PeerAuthSessionQuery.revokeEverywhere}: the logout fan-out. Its caller was always bounded, but a send the
   * deadline gave up on was left running, holding a connection to a peer stalled inside its body for as long as the
   * peer kept it open - one more on every logout (found in review of the #8472 fix).
   */
  @Test
  void aRevocationWhosePeerStallsInsideItsBodyReleasesTheConnection() throws Exception {
    try (final StallingBodyPeer peer = new StallingBodyPeer()) {
      final RaftPeerId peerId = RaftPeerId.valueOf("peer-1");
      final RaftHAServer raft = raftDialling(peerId, plainServer(), peer.address(), null, null);
      when(raft.getRaftGroup()).thenReturn(
          RaftGroup.valueOf(RaftGroupId.randomId(), RaftPeer.newBuilder().setId(peerId).build()));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      peer.callWithin(() -> {
        PeerAuthSessionQuery.revokeEverywhere(raft, "session-token", TIMEOUT_MS);
        return null;
      });
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s fan-out deadline from an unbounded wait");

      assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  // ---- PeerCapabilityQuery: the capability probe ------------------------------------------------------------------

  @Test
  void aCapabilityProbeWhosePeerStallsInsideItsBodyIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer peer = new StallingBodyPeer()) {
      assertGivenUpOn(peer,
          () -> PeerCapabilityQuery.fetch("peer-1", peer.address(), null, "test-token", TIMEOUT_MS, null, null));
    }
  }

  @Test
  void aSharedEndpointCapabilityProbeWhosePeerStallsInsideItsBodyIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer peer = new StallingBodyPeer()) {
      assertGivenUpOn(peer,
          () -> PeerCapabilityQuery.fetchFromSharedEndpoint(peer.address(), null, "test-token", TIMEOUT_MS, null, null));
    }
  }

  @Test
  void aCapabilityProbeWhosePeerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
    final TrustedHttpClientCache clients = new TrustedHttpClientCache();
    try (final StallingBodyPeer peer = new StallingBodyPeer(tls())) {
      final ArcadeDBServer server = tlsServer();
      assertGivenUpOn(peer, () -> PeerCapabilityQuery.fetch("peer-1", "127.0.0.1:1", peer.address(), "test-token",
          TIMEOUT_MS, server, clients));
    } finally {
      clients.close();
    }
  }

  /** The bound must not cost a healthy peer its answer: the capability probe still reads a whole reply. */
  @Test
  void aCapabilityProbeStillReadsAWholeAnswer() throws Exception {
    try (final AnsweringPeer peer = new AnsweringPeer(
        "{\"peerId\":\"peer-1\",\"version\":\"x\",\"capabilities\":[\"a\"]}")) {
      final PeerCapabilityQuery.Advertisement ad = PeerCapabilityQuery.fetch("peer-1", peer.address(), null,
          "test-token", TIMEOUT_MS * 10, null, null);
      assertThat(ad.peerId()).isEqualTo("peer-1");
      assertThat(ad.capabilities()).containsExactly("a");
    }
  }

  // ---- BootstrapElection.fetchBootstrapState: the state-machine thread re-verifying a diverged database ----------

  /** A failed probe answers {@code null} rather than throwing, and the caller retries on a later tick. */
  @Test
  void aBootstrapStateProbeWhosePeerStallsInsideItsBodyAnswersNoResult() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer()) {
      assertAnsweredNothing(leader, () -> BootstrapElection.fetchBootstrapState(null, leader.address(), null,
          "test-token", Set.of("db"), TIMEOUT_MS));
    }
  }

  /** Over HTTPS the probe builds a client per call and closes it after the deadline, which must not wait it out. */
  @Test
  void aBootstrapStateProbeWhosePeerStallsInsideItsBodyOverTlsAnswersNoResult() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer(tls())) {
      final ArcadeDBServer server = tlsServer();
      assertAnsweredNothing(leader, () -> BootstrapElection.fetchBootstrapState(server, "127.0.0.1:1",
          leader.address(), "test-token", Set.of("db"), TIMEOUT_MS));
    }
  }

  /**
   * {@code BootstrapElection.concludePass}: the fan-out telling every peer a bootstrap pass finished. Its HTTPS client
   * is built per pass and closed once the fan-out's deadline expires, and {@code close()} waits for every exchange
   * still running on it - so one that nothing cancelled kept the leader's bootstrap thread on a peer stalled inside
   * its body, past the deadline that had already fired (found in review of the #8472 fix).
   */
  @Test
  void aPassConclusionWhosePeerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer peer = new StallingBodyPeer(tls())) {
      final RaftPeerId peerId = RaftPeerId.valueOf("peer-1");
      final RaftHAServer raft = mock(RaftHAServer.class);
      when(raft.getLocalPeerId()).thenReturn(RaftPeerId.valueOf("self"));
      when(raft.getLivePeers()).thenReturn(List.of(RaftPeer.newBuilder().setId(peerId).build()));
      when(raft.getHttpAddresses()).thenReturn(Map.of(peerId, "127.0.0.1:1"));
      // The fan-out reads the guarded accessor, not the raw resolver (issue #8033).
      when(raft.getUnambiguousPeerHttpsAddress(peerId)).thenReturn(peer.address());
      when(raft.getClusterToken()).thenReturn("test-token");
      final BootstrapElection election = new BootstrapElection(raft, tlsServer());
      election.probeAttemptTimeoutMs = TIMEOUT_MS;

      final Method conclude = BootstrapElection.class.getDeclaredMethod("concludePass", String.class, List.class);
      conclude.setAccessible(true);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      peer.callWithin(() -> conclude.invoke(election, "pass-1", List.of()));
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s fan-out deadline from a close() waiting on the stalled body");

      assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  // ---- LeaderDatabaseQuery.fetch over HTTPS: the branch #8325 bounded without a test of its own -------------------

  @Test
  void aLeaderDatabaseQueryWhosePeerStallsInsideItsBodyOverTlsIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer(tls())) {
      final ArcadeDBServer server = tlsServer();
      assertGivenUpOn(leader,
          () -> LeaderDatabaseQuery.fetch("127.0.0.1:1", leader.address(), "test-token", TIMEOUT_MS, server));
    }
  }

  // ------------------------------------------------------------------------------------------------------------

  private static void assertGivenUpOn(final StallingBodyPeer peer, final Callable<?> call) throws Exception {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> peer.callWithin(call)).isInstanceOf(HttpTimeoutException.class);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

    assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
  }

  private static void assertAnsweredNothing(final StallingBodyPeer peer, final Callable<?> call) throws Exception {
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Object answer = peer.callWithin(call);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

    assertThat(answer).isNull();
    assertThat(peer.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
  }

  private static SSLContext tls() throws Exception {
    return RaftTestPki.serverContext(pki);
  }

  private static ArcadeDBServer plainServer() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());
    return server;
  }

  /** A server with SSL on that trusts the stalling peer's certificate, so the dial reaches the stall. */
  private static ArcadeDBServer tlsServer() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.NETWORK_USE_SSL, true);
    cfg.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, pki.trustStore().toAbsolutePath().toString());
    cfg.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, RaftTestPki.password());
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(cfg);
    return server;
  }

  private static RaftHAServer raftDialling(final RaftPeerId peerId, final ArcadeDBServer server,
      final String httpAddress, final String httpsAddress, final TrustedHttpClientCache clients) {
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getServer()).thenReturn(server);
    when(raft.getLocalPeerId()).thenReturn(RaftPeerId.valueOf("self"));
    when(raft.getLocalHttpAddress()).thenReturn("127.0.0.1:2");
    when(raft.getUnambiguousPeerHttpAddress(peerId)).thenReturn(httpAddress);
    when(raft.getUnambiguousPeerHttpsAddress(peerId)).thenReturn(httpsAddress);
    when(raft.getHttpsClients()).thenReturn(clients);
    when(raft.getClusterToken()).thenReturn("test-token");
    return raft;
  }

  /** A peer that answers every request with a complete {@code 200}. */
  private static final class AnsweringPeer implements AutoCloseable {
    private final HttpServer server;

    AnsweringPeer(final String body) throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext("/", exchange -> {
        exchange.getRequestBody().readAllBytes();
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
      });
      server.start();
    }

    String address() {
      return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
