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

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8901: the stale-term divergence reformat must not fire on a follower whose Ratis division was restarted in
 * place and has not taken a single replicated entry since, under the same leader term. That is the dead replication
 * path of #8898 - the leader's append stream still bound to the closed server instance - not a divergence, and the
 * reformat turned a node that had only been paused into a voter with an empty log.
 */
class Issue8901DeadReplicationPathNoReformatTest {

  private static final long DURATION_MS = 5_000L;
  private static final long INTERVAL_MS = 1_000L;

  static final class FakeTarget implements HealthMonitor.HealthTarget {
    final    AtomicInteger divergenceRecover = new AtomicInteger();
    volatile boolean       stuckDiverged     = true;
    volatile boolean       pathUnproven      = true;

    @Override
    public LifeCycle.State getRaftLifeCycleState() {
      return LifeCycle.State.RUNNING;
    }

    @Override
    public boolean isShutdownRequested() {
      return false;
    }

    @Override
    public void restartRatisIfNeeded() {
    }

    @Override
    public boolean isFollowerStuckDiverged() {
      return stuckDiverged;
    }

    @Override
    public void recoverFromDivergence() {
      divergenceRecover.incrementAndGet();
    }

    @Override
    public boolean isReplicationPathUnprovenSinceRestart() {
      return pathUnproven;
    }
  }

  private static HealthMonitor monitor(final FakeTarget target, final AtomicLong clock, final boolean recovery) {
    final HealthMonitor monitor = new HealthMonitor(target, INTERVAL_MS, 0L, DURATION_MS, recovery, 3, 0, DURATION_MS);
    monitor.setClock(clock::get);
    return monitor;
  }

  private static void tickFor(final HealthMonitor monitor, final AtomicLong clock, final long durationMs) {
    for (long elapsed = 0; elapsed < durationMs; elapsed += INTERVAL_MS) {
      monitor.tick();
      clock.addAndGet(INTERVAL_MS);
    }
  }

  @Test
  void noReformatWhileNoEntryReachedTheRestartedDivision() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, true);

    tickFor(monitor, clock, DURATION_MS * 10);

    assertThat(target.divergenceRecover.get())
        .as("a restarted division that has taken no entry under the same term is a dead path, not a divergence")
        .isZero();
    assertThat(monitor.isFollowerStuckDivergedConfirmed())
        .as("the stuck signature is still reported to the operator: only the destructive action is held back")
        .isTrue();
  }

  @Test
  void theWindowStartsOverOnceTheReplicationPathIsProven() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, true);

    tickFor(monitor, clock, DURATION_MS * 3);
    assertThat(target.divergenceRecover.get()).isZero();

    // An entry landed (or the term moved): from here on a persisting signature is a divergence again, but the
    // whole window has to elapse first. A leader whose first append after a reconnect is rejected and corrected
    // must not be reformatted on the very tick the path came back.
    target.pathUnproven = false;
    tickFor(monitor, clock, DURATION_MS - INTERVAL_MS);
    assertThat(target.divergenceRecover.get())
        .as("the time spent on a dead path does not count toward the divergence window")
        .isZero();

    tickFor(monitor, clock, 2 * INTERVAL_MS);
    assertThat(target.divergenceRecover.get())
        .as("a divergence that persists for the full window once appends reach the division is still reformatted")
        .isEqualTo(1);
  }

  @Test
  void aDivisionNeverRestartedInPlaceKeepsTheReformat() {
    final FakeTarget target = new FakeTarget();
    target.pathUnproven = false;
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, true);

    tickFor(monitor, clock, DURATION_MS + 2 * INTERVAL_MS);

    assertThat(target.divergenceRecover.get())
        .as("issue #4741: a divergence persisted across a process restart must still be reformatted")
        .isEqualTo(1);
  }

  @Test
  void theDeadPathIsReportedWithRecoveryDisabled() {
    final FakeTarget target = new FakeTarget();
    final AtomicLong clock = new AtomicLong(0L);
    final HealthMonitor monitor = monitor(target, clock, false);

    tickFor(monitor, clock, DURATION_MS * 3);

    assertThat(target.divergenceRecover.get()).isZero();
    assertThat(monitor.isFollowerStuckDivergedConfirmed()).isTrue();
  }

  @Test
  void pathUnprovenOnlyAfterAnInPlaceRestartWithNoEntryAndTheSameTerm() {
    final TermIndex last = TermIndex.valueOf(27, 1_564_566);
    final RaftHAServer.InPlaceRestartBaseline baseline = new RaftHAServer.InPlaceRestartBaseline(last, 28);

    assertThat(RaftHAServer.replicationPathUnproven(null, last, 28))
        .as("no in-place restart in this process: no old server instance can hold the leader's stream")
        .isFalse();
    assertThat(RaftHAServer.replicationPathUnproven(baseline, last, 28))
        .as("the #8898 shape: same last entry, same term as right after the restart")
        .isTrue();
    assertThat(RaftHAServer.replicationPathUnproven(baseline, TermIndex.valueOf(28, 1_564_567), 28))
        .as("an entry was appended since the restart: the leader's appends reach this division")
        .isFalse();
    assertThat(RaftHAServer.replicationPathUnproven(baseline, TermIndex.valueOf(26, 1_564_566), 28))
        .as("a truncate-and-append back to the same index is still an append that reached the division")
        .isFalse();
    assertThat(RaftHAServer.replicationPathUnproven(baseline, last, 29))
        .as("a newer term means a new leader, whose appenders dial the running server")
        .isFalse();
    assertThat(RaftHAServer.replicationPathUnproven(new RaftHAServer.InPlaceRestartBaseline(null, 28), null, 28))
        .as("a reformatted, still empty log that took nothing is unproven too")
        .isTrue();
    assertThat(RaftHAServer.replicationPathUnproven(baseline, last, -1))
        .as("an unreadable term is no proof: hold the destructive action")
        .isTrue();
  }
}
