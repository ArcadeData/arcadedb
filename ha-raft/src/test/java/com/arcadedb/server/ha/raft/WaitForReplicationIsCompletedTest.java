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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7518: {@link BaseRaftHATest#waitForReplicationIsCompleted(int)} had two exits that returned
 * exactly as though replication had completed.
 * <ol>
 *   <li>No leader with a positive applied index inside 3 s: it returned without waiting at all and without a
 *       line in the log.</li>
 *   <li>The target server did not catch up inside the budget: it logged a bare WARNING nobody could grep for with
 *       the instrument's marker, and returned.</li>
 * </ol>
 * Both are now a {@code false} answer from {@link BaseRaftHATest#awaitAppliedIndex} and a {@code GAVE UP} line
 * carrying {@link BaseRaftHATest#SLOW_WAIT_MARKER}, so a CI log can be grepped for them like every other wait in
 * that class. They stay non-fatal on purpose: ~100 ITs call this wait through {@code waitForAllServers()} and
 * {@code assertClusterConsistency()}, some with a server deliberately stopped.
 * <p>
 * The wait is driven here through suppliers standing in for the leader's and the target's applied index, so each
 * exit can be provoked deterministically without a three-node cluster.
 */
class WaitForReplicationIsCompletedTest {

  private static final long SHORT_BUDGET_MS = 300;
  private static final long LONG_BUDGET_MS  = 10_000;

  @Test
  void noLeaderIsAGiveUpNotACompletedReplication() {
    final AtomicInteger targetReads = new AtomicInteger();
    final CapturingTestLogger logger = CapturingTestLogger.install();
    final boolean caughtUp;
    try {
      caughtUp = BaseRaftHATest.awaitAppliedIndex(this, "replication on server 1", () -> BaseRaftHATest.NO_APPLIED_INDEX,
          () -> {
            targetReads.incrementAndGet();
            return 0;
          }, SHORT_BUDGET_MS);
    } finally {
      logger.uninstall();
    }

    assertThat(caughtUp).isFalse();
    // With no leader there is no target to compare against, so the server's own index is never consulted.
    assertThat(targetReads.get()).isZero();

    final List<String> warnings = logger.formattedAt(Level.WARNING);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains(BaseRaftHATest.SLOW_WAIT_MARKER);
    assertThat(warnings.getFirst()).contains("GAVE UP");
    assertThat(warnings.getFirst()).contains("replication on server 1");
    assertThat(warnings.getFirst()).contains("no leader");
    assertThat(warnings.getFirst()).contains(SHORT_BUDGET_MS + " ms budget");
  }

  @Test
  void aLaggingServerIsAGiveUpReportedThroughTheInstrument() {
    final CapturingTestLogger logger = CapturingTestLogger.install();
    final boolean caughtUp;
    try {
      caughtUp = BaseRaftHATest.awaitAppliedIndex(this, "replication on server 2", () -> 17, () -> 5, SHORT_BUDGET_MS);
    } finally {
      logger.uninstall();
    }

    assertThat(caughtUp).isFalse();

    // Exactly one warning, and it is the instrument's: the old bare "Timeout waiting for server" line is gone
    // rather than emitted alongside.
    final List<String> warnings = logger.formattedAt(Level.WARNING);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains(BaseRaftHATest.SLOW_WAIT_MARKER);
    assertThat(warnings.getFirst()).contains("GAVE UP");
    assertThat(warnings.getFirst()).contains("replication on server 2");
    assertThat(warnings.getFirst()).contains("index 17");
    assertThat(warnings.getFirst()).contains("at index 5");
    assertThat(warnings.getFirst()).contains(SHORT_BUDGET_MS + " ms budget");
  }

  @Test
  void aServerAlreadyAtTheLeadersIndexIsCaughtUpAndSilent() {
    final CapturingTestLogger logger = CapturingTestLogger.install();
    final boolean caughtUp;
    try {
      caughtUp = BaseRaftHATest.awaitAppliedIndex(this, "replication on server 0", () -> 42, () -> 42, LONG_BUDGET_MS);
    } finally {
      logger.uninstall();
    }

    assertThat(caughtUp).isTrue();
    assertThat(logger.formattedAt(Level.WARNING)).isEmpty();
  }

  /**
   * Index 0 is the first entry a Ratis log holds, so a leader reporting it has applied one entry. The old loop read
   * {@code <= 0} as "no leader" and returned after 3 s without waiting for anything.
   */
  @Test
  void aLeaderAtIndexZeroIsALeaderAndIsWaitedFor() {
    final AtomicInteger targetReads = new AtomicInteger();
    final boolean caughtUp = BaseRaftHATest.awaitAppliedIndex(this, "replication on server 1", () -> 0,
        () -> targetReads.incrementAndGet() < 3 ? BaseRaftHATest.NO_APPLIED_INDEX : 0, LONG_BUDGET_MS);

    assertThat(caughtUp).isTrue();
    assertThat(targetReads.get()).isEqualTo(3);
  }

  @Test
  void aLeaderThatAppearsLateIsWaitedForRatherThanSkipped() {
    final AtomicInteger leaderReads = new AtomicInteger();
    final LongSupplier leader = () -> leaderReads.incrementAndGet() < 4 ? BaseRaftHATest.NO_APPLIED_INDEX : 9;

    final boolean caughtUp = BaseRaftHATest.awaitAppliedIndex(this, "replication on server 1", leader, () -> 9, LONG_BUDGET_MS);

    assertThat(caughtUp).isTrue();
    assertThat(leaderReads.get()).isEqualTo(4);
  }

  @Test
  void aServerThatCatchesUpInsideTheBudgetIsCaughtUp() {
    final AtomicLong applied = new AtomicLong(0);

    final boolean caughtUp = BaseRaftHATest.awaitAppliedIndex(this, "replication on server 2", () -> 5,
        applied::getAndIncrement, LONG_BUDGET_MS);

    assertThat(caughtUp).isTrue();
    assertThat(applied.get()).isEqualTo(6);
  }

  /**
   * An interrupted wait did not see replication complete either, so it answers {@code false}; and it gives up at
   * once rather than spinning on an already-interrupted thread for the whole budget.
   */
  @Test
  void anInterruptedWaitIsNotACompletedReplication() {
    Thread.currentThread().interrupt();
    final CapturingTestLogger logger = CapturingTestLogger.install();
    final boolean caughtUp;
    try {
      caughtUp = BaseRaftHATest.awaitAppliedIndex(this, "replication on server 1", () -> 17, () -> 5, LONG_BUDGET_MS);
    } finally {
      logger.uninstall();
      assertThat(Thread.interrupted()).as("the interrupt must be preserved for the caller").isTrue();
    }

    assertThat(caughtUp).isFalse();
    final List<String> warnings = logger.formattedAt(Level.WARNING);
    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains("GAVE UP");
  }

  /**
   * A give-up is always worth a line, however quickly it happened: the report threshold exists to keep ordinary
   * satisfied waits out of the log, and a wait that did not see what it waited for is never ordinary.
   */
  @Test
  void aGiveUpIsReportedEvenBelowTheThreshold() {
    assertThat(BaseRaftHATest.slowWaitReport("replication on server 1", 0, true)).isNull();
    assertThat(BaseRaftHATest.slowWaitReport("replication on server 1", 0, false)).contains("GAVE UP");
  }

  @Test
  void theReportNamesTheBudgetOfTheWaitItDescribes() {
    assertThat(BaseRaftHATest.slowWaitReport("replication on server 1", 31_000, false, 30_000))
        .contains("31000 ms of the 30000 ms budget");
  }
}
