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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The scheme the two dials that were still hardcoded to {@code http://} now choose (issue #7546, folded into
 * issue #7563): the bootstrap-state probe and the remote-shutdown command.
 * <p>
 * Both carry the cluster token - the probe in {@code X-ArcadeDB-Cluster-Token}, the shutdown in
 * {@code Authorization: Bearer} - so on a cluster with {@code arcadedb.ssl.enabled} set they were putting it
 * on the wire in clear text while every sibling dial was already encrypted. What is pinned here is the
 * DECISION in isolation; that a decision of {@code https} is then carried by a real TLS socket, against a
 * certificate issued for the name dialled and validated through the configured truststore, is pinned by
 * {@link Issue7563HaTlsPeerDialIT} against a live two-node cluster.
 * <p>
 * The three rows of each table are the three states an operator can be in, and the middle one is the one that
 * must NOT be a refusal: an SSL cluster that never declared the optional 5th field of
 * {@code arcadedb.ha.serverList} has only the plain listener to be reached on, and every sibling
 * ({@code LeaderDial}, {@link PeerCapabilityQuery#chooseUrl}, {@link LeaderDatabaseQuery#chooseEndpoint})
 * falls back there rather than breaking the cluster.
 */
class Issue7563PlaintextPeerDialSchemeTest {

  private static final String HTTP_ADDR  = "peer-1:2480";
  private static final String HTTPS_ADDR = "peer-1:2490";

  private static final RaftPeerId PEER = RaftPeerId.valueOf("peer-1");
  /** Small enough that the retryable control finishes fast, large enough to fit several backoffs. */
  private static final long       BUDGET_MS  = 300L;
  private static final long       BACKOFF_MS = 10L;

  // ------------------------------------------------------------------ bootstrap-state probe

  @Test
  void theBootstrapProbeTakesTheEncryptedEndpointWhenSslIsOnAndOneResolves() {
    assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, true))
        .isEqualTo("https://peer-1:2490/api/v1/cluster/bootstrap-state");
  }

  @Test
  void theBootstrapProbeFallsBackToThePlainListenerWhenNoHttpsEndpointResolves() {
    assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, null, true))
        .as("an SSL cluster that declared no 'https' ports must still be able to bootstrap")
        .isEqualTo("http://peer-1:2480/api/v1/cluster/bootstrap-state");
  }

  @Test
  void theBootstrapProbeIgnoresAnHttpsEndpointWhenSslIsOff() {
    assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, false))
        .isEqualTo("http://peer-1:2480/api/v1/cluster/bootstrap-state");
  }

  @Test
  void aPeerWithNoAddressAtAllYieldsNoUrlRatherThanAMalformedOne() {
    assertThat(BootstrapElection.chooseUrl(null, null, true)).isNull();
    assertThat(BootstrapElection.chooseUrl(null, null, false)).isNull();
  }

  @Test
  void theEncryptedProbeCarriesExactlyTheCredentialsThePlainOneDid() {
    final HttpRequest request = BootstrapElection.bootstrapStateRequestTo(
        BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, true), "the-token", 1234L);

    assertThat(request.uri().getScheme()).isEqualTo("https");
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("the-token");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("root");
  }

  // ------------------------------------------------------------------ remote shutdown

  @Test
  void theRemoteShutdownTakesTheEncryptedEndpointWhenSslIsOnAndOneResolves() {
    assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, HTTPS_ADDR, null, null), true))
        .isEqualTo("https://peer-1:2490/api/v1/server");
  }

  @Test
  void theRemoteShutdownFallsBackToThePlainListenerWhenNoHttpsEndpointResolves() {
    assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, null, null, null), true))
        .isEqualTo("http://peer-1:2480/api/v1/server");
  }

  @Test
  void theRemoteShutdownIgnoresAnHttpsEndpointWhenSslIsOff() {
    assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, HTTPS_ADDR, null, null), false))
        .isEqualTo("http://peer-1:2480/api/v1/server");
  }

  // ------------------------------------------------------------------ "cannot happen twice" is FATAL

  /**
   * The branch itself: an HTTPS probe with no client to send it on answers {@code FATAL}, not
   * {@code RETRYABLE} (code review on PR #7838), and - the other half of the security property - it answers
   * rather than quietly falling back to the plain listener.
   * <p>
   * Driven through {@code queryPeer}, which is where the decision is made. The no-client branch answers
   * before the election object is otherwise touched, so a bare instance is enough to reach it.
   */
  @Test
  void anHttpsProbeWithNoClientIsFatalRatherThanRetriedOrDowngraded() throws Exception {
    final BootstrapElection election = new BootstrapElection(null, null);

    final BootstrapElection.ProbeOutcome outcome = election.queryPeer(PEER,
        "https://peer-1:2490/api/v1/cluster/bootstrap-state", Set.of("db"), 1_000L, null).get();

    assertThat(outcome.result())
        .as("the client is built once per fan-out and never rebuilt, so retrying cannot change this")
        .isEqualTo(BootstrapElection.ProbeResult.FATAL);
    assertThat(outcome.detail()).contains("truststore");
  }

  /**
   * Why that distinction is worth a test: {@code collectRemoteStatesWithRetry} drops a {@code FATAL} peer
   * immediately and re-probes a {@code RETRYABLE} one until {@code HA_BOOTSTRAP_TIMEOUT_MS} runs out, so
   * reporting the outcome above as retryable cost the full budget - two minutes by default - on every
   * election of a cluster with a misconfigured {@code arcadedb.ssl.trustStore}, to reach the conclusion that
   * was available on the first attempt.
   * <p>
   * The assertion counts <b>probe attempts</b> rather than elapsed time: the count is what the two outcomes
   * actually differ by, and it says the same thing on a loaded runner as on an idle one.
   */
  @Test
  void anOutcomeThatCannotChangeOnRetryIsNotRetried() {
    final AtomicInteger fatalAttempts = new AtomicInteger();
    final List<RaftPeerId> fatalAssumedEmpty = new ArrayList<>();
    BootstrapElection.collectRemoteStatesWithRetry(
        Map.of(PEER, "https://peer-1:2490/api/v1/cluster/bootstrap-state"),
        (peerId, url, attemptMs) -> {
          fatalAttempts.incrementAndGet();
          return CompletableFuture.completedFuture(
              BootstrapElection.ProbeOutcome.fatal("no HTTPS client could be built from the cluster truststore"));
        },
        BUDGET_MS, BUDGET_MS, BACKOFF_MS, () -> true, fatalAssumedEmpty);

    assertThat(fatalAttempts)
        .as("a peer whose probe can never succeed must be probed once, not until the budget runs out")
        .hasValue(1);
    assertThat(fatalAssumedEmpty)
        .as("it still reaches the SEVERE line that names the peers the election had to assume empty")
        .containsExactly(PEER);

    // The control: the SAME budget, the same backoff, an outcome that says "try again" - and it does.
    final AtomicInteger retryableAttempts = new AtomicInteger();
    BootstrapElection.collectRemoteStatesWithRetry(
        Map.of(PEER, "https://peer-1:2490/api/v1/cluster/bootstrap-state"),
        (peerId, url, attemptMs) -> {
          retryableAttempts.incrementAndGet();
          return CompletableFuture.completedFuture(BootstrapElection.ProbeOutcome.retryable("HTTP 503"));
        },
        BUDGET_MS, BUDGET_MS, BACKOFF_MS, () -> true, new ArrayList<>());

    assertThat(retryableAttempts.get())
        .as("a transient failure is still retried, so the two outcomes are genuinely distinguished")
        .isGreaterThan(1);
  }
}
