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
package com.arcadedb.server.ha.raft.ratis;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11458: a follower with a large Raft apply backlog keeps every unapplied entry decoded on the heap, because
 * Ratis only evicts below the applied index. The leader therefore holds back new entries from a follower whose
 * estimated unapplied bytes exceed a budget.
 */
class Issue11458ApplyBackPressureTest {

  @Test
  void pausesWhenEstimatedUnappliedBytesExceedTheBudget() {
    // 30k entries of ~59KB is the backlog of the chaos run in the issue: about 1.7GB
    assertThat(ApplyBackPressure.exceedsBudget(30_000L, 0L, 59_000L, 256L << 20)).isTrue();
  }

  @Test
  void doesNotPauseWithinTheBudget() {
    assertThat(ApplyBackPressure.exceedsBudget(1_000L, 0L, 59_000L, 256L << 20)).isFalse();
  }

  @Test
  void anUnknownAppliedIndexNeverPauses() {
    assertThat(ApplyBackPressure.exceedsBudget(1_000_000L, -1L, 59_000L, 1L)).isFalse();
  }

  @Test
  void anUnknownEntrySizeNeverPauses() {
    assertThat(ApplyBackPressure.exceedsBudget(1_000_000L, 0L, 0L, 1L)).isFalse();
  }

  @Test
  void aFollowerAheadOfWhatWasSentNeverPauses() {
    assertThat(ApplyBackPressure.exceedsBudget(10L, 50L, 59_000L, 1L)).isFalse();
  }

  @Test
  void hugeInputsDoNotOverflow() {
    assertThat(ApplyBackPressure.exceedsBudget(Long.MAX_VALUE, 0L, Long.MAX_VALUE, Long.MAX_VALUE)).isTrue();
    assertThat(ApplyBackPressure.exceedsBudget(1L, 0L, Long.MAX_VALUE, Long.MAX_VALUE)).isFalse();
  }

  @Test
  void averageEntrySizeFollowsTheBatches() {
    assertThat(ApplyBackPressure.nextAverage(0L, 10_000L, 10)).isEqualTo(1_000L);
    // an exponential moving average: the new batch weighs 1/8
    assertThat(ApplyBackPressure.nextAverage(1_000L, 8_000L, 1)).isEqualTo(1_875L);
    assertThat(ApplyBackPressure.nextAverage(1_000L, 0L, 0)).isEqualTo(1_000L);
  }

  @Test
  void theBudgetDefaultsToAQuarterOfTheHeap() {
    assertThat(ApplyBackPressure.resolveBudgetBytes(new ContextConfiguration(), 4L << 30)).isEqualTo(1L << 30);
  }

  @Test
  void theBudgetHasAFloor() {
    assertThat(ApplyBackPressure.resolveBudgetBytes(new ContextConfiguration(), 64L << 20)).isEqualTo(ApplyBackPressure.MIN_BUDGET_BYTES);
  }

  @Test
  void theBudgetCanBeConfiguredAndDisabled() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_APPLY_BACKLOG_MAX, "128MB");
    assertThat(ApplyBackPressure.resolveBudgetBytes(configuration, 4L << 30)).isEqualTo(128L << 20);
    configuration.setValue(GlobalConfiguration.HA_APPLY_BACKLOG_MAX, "0");
    assertThat(ApplyBackPressure.resolveBudgetBytes(configuration, 4L << 30)).isZero();
  }

  @Test
  void shouldPauseReadsThePerFollowerAppliedIndex() {
    final AtomicLong applied = new AtomicLong(-1L);
    final ApplyBackPressure backPressure = new ApplyBackPressure(1_000_000L, peer -> "slow".equals(peer) ? applied.get() : -1L);

    assertThat(backPressure.shouldPause("slow", 10_000L, 1_000L)).as("applied index not reported yet").isFalse();
    applied.set(0L);
    assertThat(backPressure.shouldPause("slow", 10_000L, 1_000L)).as("10MB unapplied over a 1MB budget").isTrue();
    assertThat(backPressure.shouldPause("other", 10_000L, 1_000L)).as("another follower reported nothing").isFalse();
    applied.set(9_500L);
    assertThat(backPressure.shouldPause("slow", 10_000L, 1_000L)).as("the follower caught up").isFalse();
  }

  @Test
  void aDisabledBudgetNeverPauses() {
    final ApplyBackPressure backPressure = new ApplyBackPressure(0L, peer -> 0L);
    assertThat(backPressure.shouldPause("slow", 10_000_000L, 1_000_000L)).isFalse();
  }

  @Test
  void registryIsKeyedByTheLeaderAndUnregisterOnlyRemovesItsOwn() {
    final ApplyBackPressure first = new ApplyBackPressure(1L, peer -> 0L);
    final ApplyBackPressure second = new ApplyBackPressure(2L, peer -> 0L);
    ApplyBackPressure.register("leader-11458", first);
    try {
      assertThat(ApplyBackPressure.of("leader-11458")).isSameAs(first);
      ApplyBackPressure.register("leader-11458", second);
      ApplyBackPressure.unregister("leader-11458", first);
      assertThat(ApplyBackPressure.of("leader-11458")).as("an older server must not drop its successor").isSameAs(second);
    } finally {
      ApplyBackPressure.unregister("leader-11458", second);
    }
    assertThat(ApplyBackPressure.of("leader-11458")).isNull();
  }
}
