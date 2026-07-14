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

import com.arcadedb.server.ha.raft.BootstrapElection.PeerState;
import com.arcadedb.server.ha.raft.BootstrapElection.ProbeFunction;
import com.arcadedb.server.ha.raft.BootstrapElection.ProbeOutcome;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the retrying bootstrap-state probe collection introduced for issue #5273.
 * <p>
 * Before this change a peer that answered the {@code /bootstrap-state} probe with HTTP 401 (its
 * security stack still initializing during a parallel cold boot) was treated as a definitive
 * "no state" and silently dropped from the election, risking selection of a stale baseline. The
 * election now retries transient failures (401/403/5xx/unreachable) within the overall bootstrap
 * timeout budget, and only assumes "no local data" for a peer once the budget is exhausted - logging
 * exactly what it assumed. These tests pin the retry/backoff/deadline logic deterministically with an
 * injected probe (no real HTTP).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BootstrapProbeRetryTest {

  private static final RaftPeerId A = RaftPeerId.valueOf("nodeA");
  private static final RaftPeerId B = RaftPeerId.valueOf("nodeB");
  private static final RaftPeerId C = RaftPeerId.valueOf("nodeC");

  private static Map<String, PeerState> data(final RaftPeerId peer, final long lastTxId) {
    return Map.of("db", new PeerState(peer, "db", "fp-" + peer, lastTxId));
  }

  @Test
  void okPeerResolvedInASingleRound() {
    final AtomicInteger calls = new AtomicInteger();
    final ProbeFunction prober = (pid, addr, t) -> {
      calls.incrementAndGet();
      return CompletableFuture.completedFuture(ProbeOutcome.ok(data(pid, 7L)));
    };

    final List<RaftPeerId> assumedEmpty = new ArrayList<>();
    final Map<RaftPeerId, Map<String, PeerState>> result = BootstrapElection.collectRemoteStatesWithRetry(
        Map.of(A, "addrA"), prober, 5_000L, 1_000L, 1L, () -> true, assumedEmpty);

    assertThat(calls.get()).isEqualTo(1);
    assertThat(result).containsKey(A);
    assertThat(result.get(A).get("db").lastTxId()).isEqualTo(7L);
    assertThat(assumedEmpty).isEmpty();
  }

  @Test
  void transientlyFailingPeerIsRetriedUntilItSucceeds() {
    // The reporter's 401 race: the first two probes fail transiently (peer still coming up), the
    // third succeeds. The peer's real state must make it into the election instead of being dropped.
    final AtomicInteger calls = new AtomicInteger();
    final ProbeFunction prober = (pid, addr, t) -> {
      final int n = calls.incrementAndGet();
      return CompletableFuture.completedFuture(
          n < 3 ? ProbeOutcome.retryable("HTTP 401") : ProbeOutcome.ok(data(pid, 99L)));
    };

    final List<RaftPeerId> assumedEmpty = new ArrayList<>();
    final Map<RaftPeerId, Map<String, PeerState>> result = BootstrapElection.collectRemoteStatesWithRetry(
        Map.of(A, "addrA"), prober, 5_000L, 1_000L, 1L, () -> true, assumedEmpty);

    assertThat(calls.get()).isEqualTo(3);
    assertThat(result).containsKey(A);
    assertThat(result.get(A).get("db").lastTxId()).isEqualTo(99L);
    assertThat(assumedEmpty).isEmpty();
  }

  @Test
  void persistentlyTransientPeerIsGivenUpAndAssumedEmptyAfterBudget() {
    // A peer that never recovers within the budget must be given up on (not retried forever) and
    // reported as assumed-empty so the election can log what it assumed.
    final AtomicInteger calls = new AtomicInteger();
    final ProbeFunction prober = (pid, addr, t) -> {
      calls.incrementAndGet();
      return CompletableFuture.completedFuture(ProbeOutcome.retryable("HTTP 401"));
    };

    final List<RaftPeerId> assumedEmpty = new ArrayList<>();
    final Map<RaftPeerId, Map<String, PeerState>> result = BootstrapElection.collectRemoteStatesWithRetry(
        Map.of(A, "addrA"), prober, 60L, 20L, 5L, () -> true, assumedEmpty);

    assertThat(result).doesNotContainKey(A);
    assertThat(assumedEmpty).containsExactly(A);
    assertThat(calls.get()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void fatalPeerIsNotRetried() {
    // A definitive answer (e.g. HTTP 400 "Raft HA not enabled") won't change on retry: probe once,
    // then assume empty.
    final AtomicInteger calls = new AtomicInteger();
    final ProbeFunction prober = (pid, addr, t) -> {
      calls.incrementAndGet();
      return CompletableFuture.completedFuture(ProbeOutcome.fatal("HTTP 400"));
    };

    final List<RaftPeerId> assumedEmpty = new ArrayList<>();
    final Map<RaftPeerId, Map<String, PeerState>> result = BootstrapElection.collectRemoteStatesWithRetry(
        Map.of(A, "addrA"), prober, 5_000L, 1_000L, 1L, () -> true, assumedEmpty);

    assertThat(calls.get()).isEqualTo(1);
    assertThat(result).doesNotContainKey(A);
    assertThat(assumedEmpty).containsExactly(A);
  }

  @Test
  void stopsImmediatelyWhenNoLongerLeader() {
    // If this node loses leadership while collecting, the loop must stop without probing and assume
    // empty for every unresolved peer.
    final AtomicInteger calls = new AtomicInteger();
    final ProbeFunction prober = (pid, addr, t) -> {
      calls.incrementAndGet();
      return CompletableFuture.completedFuture(ProbeOutcome.retryable("HTTP 401"));
    };

    final List<RaftPeerId> assumedEmpty = new ArrayList<>();
    final Map<RaftPeerId, Map<String, PeerState>> result = BootstrapElection.collectRemoteStatesWithRetry(
        Map.of(A, "addrA"), prober, 5_000L, 1_000L, 1L, () -> false, assumedEmpty);

    assertThat(calls.get()).isZero();
    assertThat(result).isEmpty();
    assertThat(assumedEmpty).containsExactly(A);
  }

  @Test
  void mixedPeersResolveIndependently() {
    // A resolves immediately, B recovers after one retry, C is definitively fatal. A and B end up in
    // the results with their data; C is assumed empty. Order-independent.
    final AtomicInteger bCalls = new AtomicInteger();
    final ProbeFunction prober = (pid, addr, t) -> {
      if (pid.equals(A))
        return CompletableFuture.completedFuture(ProbeOutcome.ok(data(A, 10L)));
      if (pid.equals(B))
        return CompletableFuture.completedFuture(
            bCalls.incrementAndGet() < 2 ? ProbeOutcome.retryable("HTTP 503") : ProbeOutcome.ok(data(B, 20L)));
      return CompletableFuture.completedFuture(ProbeOutcome.fatal("HTTP 400"));
    };

    final Map<RaftPeerId, String> peers = new LinkedHashMap<>();
    peers.put(A, "addrA");
    peers.put(B, "addrB");
    peers.put(C, "addrC");

    final List<RaftPeerId> assumedEmpty = new ArrayList<>();
    final Map<RaftPeerId, Map<String, PeerState>> result = BootstrapElection.collectRemoteStatesWithRetry(
        peers, prober, 5_000L, 1_000L, 1L, () -> true, assumedEmpty);

    assertThat(result).containsKeys(A, B);
    assertThat(result.get(A).get("db").lastTxId()).isEqualTo(10L);
    assertThat(result.get(B).get("db").lastTxId()).isEqualTo(20L);
    assertThat(result).doesNotContainKey(C);
    assertThat(assumedEmpty).containsExactly(C);
  }

  @Test
  void retryableStatusClassification() {
    // 401/403 (auth race), 408/425/429 (too-early/throttled) and any 5xx are transient -> retry.
    assertThat(BootstrapElection.isRetryableProbeStatus(401)).isTrue();
    assertThat(BootstrapElection.isRetryableProbeStatus(403)).isTrue();
    assertThat(BootstrapElection.isRetryableProbeStatus(408)).isTrue();
    assertThat(BootstrapElection.isRetryableProbeStatus(425)).isTrue();
    assertThat(BootstrapElection.isRetryableProbeStatus(429)).isTrue();
    assertThat(BootstrapElection.isRetryableProbeStatus(500)).isTrue();
    assertThat(BootstrapElection.isRetryableProbeStatus(503)).isTrue();
    // 200 success and 400/404 (definitive) are not retried.
    assertThat(BootstrapElection.isRetryableProbeStatus(200)).isFalse();
    assertThat(BootstrapElection.isRetryableProbeStatus(400)).isFalse();
    assertThat(BootstrapElection.isRetryableProbeStatus(404)).isFalse();
  }
}
