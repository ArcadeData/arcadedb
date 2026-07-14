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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KubernetesAutoJoin#tryAutoJoinWithRetry} (issue #5268): the one-shot join
 * probe reliably lost the race against the peers' allowlist DNS refresh on pod recreation and then
 * parked forever. The retry loop must keep probing with capped exponential backoff until the node
 * joins (or confirms membership), the caller's condition turns false, or the thread is interrupted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class KubernetesAutoJoinRetryTest {

  /** Builds an auto-join whose probe outcomes are scripted and whose sleeps are recorded, not slept. */
  private static KubernetesAutoJoin scripted(final Deque<KubernetesAutoJoin.Outcome> outcomes, final List<Long> sleeps) {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId());
    final KubernetesAutoJoin autoJoin = new KubernetesAutoJoin(null, group, RaftPeerId.valueOf("test_1"),
        new RaftProperties()) {
      @Override
      public Outcome tryAutoJoin() {
        return outcomes.pop();
      }
    };
    autoJoin.setSleeperForTesting(sleeps::add);
    return autoJoin;
  }

  @Test
  void retriesWithExponentialBackoffUntilJoined() {
    final Deque<KubernetesAutoJoin.Outcome> outcomes = new ArrayDeque<>(List.of(
        KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND,
        KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND,
        KubernetesAutoJoin.Outcome.JOINED));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(sleeps).containsExactly(KubernetesAutoJoin.RETRY_BACKOFF_INITIAL_MS,
        KubernetesAutoJoin.RETRY_BACKOFF_INITIAL_MS * 2);
  }

  @Test
  void alreadyMemberStopsImmediately() {
    final Deque<KubernetesAutoJoin.Outcome> outcomes =
        new ArrayDeque<>(List.of(KubernetesAutoJoin.Outcome.ALREADY_MEMBER));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.ALREADY_MEMBER);
    assertThat(sleeps).isEmpty();
  }

  @Test
  void stopsWithoutSleepingWhenConditionTurnsFalse() {
    // The typical exit on a cold-start deployment: the probe finds no cluster, but by the time the
    // retry decision is made a leader has been elected (the caller's condition turns false).
    final Deque<KubernetesAutoJoin.Outcome> outcomes =
        new ArrayDeque<>(List.of(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> false);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND);
    assertThat(sleeps).isEmpty();
  }

  @Test
  void addRejectedIsRetried() {
    // A peer that accepts the probe but rejects the configuration change (e.g. no settled leader
    // yet) is a transient condition and must be retried, not treated as terminal.
    final Deque<KubernetesAutoJoin.Outcome> outcomes = new ArrayDeque<>(List.of(
        KubernetesAutoJoin.Outcome.ADD_REJECTED,
        KubernetesAutoJoin.Outcome.JOINED));
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(outcomes, sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(sleeps).hasSize(1);
  }

  @Test
  void backoffIsCappedAtMaximum() {
    final List<KubernetesAutoJoin.Outcome> script = new ArrayList<>();
    for (int i = 0; i < 8; i++)
      script.add(KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND);
    script.add(KubernetesAutoJoin.Outcome.JOINED);
    final List<Long> sleeps = new ArrayList<>();

    final KubernetesAutoJoin.Outcome result = scripted(new ArrayDeque<>(script), sleeps).tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.JOINED);
    assertThat(sleeps).containsExactly(2_000L, 4_000L, 8_000L, 16_000L, 32_000L, 60_000L, 60_000L, 60_000L);
  }

  @Test
  void interruptDuringBackoffStopsTheLoop() {
    final Deque<KubernetesAutoJoin.Outcome> outcomes = new ArrayDeque<>(List.of(
        KubernetesAutoJoin.Outcome.NO_CLUSTER_FOUND,
        KubernetesAutoJoin.Outcome.JOINED));
    final KubernetesAutoJoin autoJoin = scripted(outcomes, new ArrayList<>());
    autoJoin.setSleeperForTesting(ms -> {
      throw new InterruptedException("shutdown");
    });

    final KubernetesAutoJoin.Outcome result = autoJoin.tryAutoJoinWithRetry(() -> true);

    assertThat(result).isEqualTo(KubernetesAutoJoin.Outcome.INTERRUPTED);
    // The interrupt flag must be restored for the owning thread's shutdown handling.
    assertThat(Thread.interrupted()).isTrue(); // also clears it so it does not leak into other tests
  }
}
