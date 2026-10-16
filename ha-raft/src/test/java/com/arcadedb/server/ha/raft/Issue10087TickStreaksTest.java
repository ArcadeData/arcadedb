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

import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10087: a streak counter means "seen on every tick since it started", so a tick that did
 * not evaluate the signal cannot extend it. The dead-updater and closed-log streaks only reset on the line after their
 * own check, so every early return above them (shutdown, {@code CLOSING}, {@code CLOSED}/{@code EXCEPTION}, a dead
 * updater) left the streak standing and two sightings separated by such a tick fired a restart that no two consecutive
 * ticks had earned. The table is over pairs of tick states rather than one test per counter, so a counter added to
 * {@code tick()} later is covered by construction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10087TickStreaksTest {

  /** What one tick of the monitor can find. */
  enum Tick {
    HEALTHY,
    SHUTDOWN_REQUESTED,
    CLOSING,
    CLOSED,
    DEAD_UPDATER,
    LOG_CLOSED
  }

  /** The signals whose streak needs the guard of consecutive ticks, i.e. those that must be seen twice to act. */
  enum Streak {
    CLOSING_STATE(Tick.CLOSING),
    DEAD_UPDATER_THREAD(Tick.DEAD_UPDATER),
    CLOSED_RAFT_LOG(Tick.LOG_CLOSED);

    final Tick tick;

    Streak(final Tick tick) {
      this.tick = tick;
    }
  }

  /** A target whose findings are scripted tick by tick. */
  private static final class ScriptedTarget extends HealthMonitorTest.FakeHealthTarget {
    volatile boolean logClosed = false;

    void apply(final Tick tick) {
      state.set(tick == Tick.CLOSING ? LifeCycle.State.CLOSING : tick == Tick.CLOSED ? LifeCycle.State.CLOSED : LifeCycle.State.RUNNING);
      shutdownRequested = tick == Tick.SHUTDOWN_REQUESTED;
      deadUpdater = tick == Tick.DEAD_UPDATER ? "the Ratis StateMachineUpdater thread 'x' has terminated" : null;
      logClosed = tick == Tick.LOG_CLOSED;
    }

    @Override
    public boolean isRaftLogClosed() {
      return logClosed;
    }
  }

  private static int restartsAfter(final List<Tick> ticks) {
    final ScriptedTarget target = new ScriptedTarget();
    final HealthMonitor monitor = new HealthMonitor(target, 0);
    for (final Tick tick : ticks) {
      target.apply(tick);
      monitor.tick();
    }
    return target.recoveryCalls.get();
  }

  /** What the in-between tick does on its own: a CLOSED division is restarted at once, nothing else acts on one tick. */
  private static int restartsOfOneTickAlone(final Tick tick) {
    return tick == Tick.CLOSED ? 1 : 0;
  }

  @ParameterizedTest
  @EnumSource(Streak.class)
  void theSignalOnTwoConsecutiveTicksStillActs(final Streak streak) {
    assertThat(restartsAfter(List.of(streak.tick, streak.tick))).as("%s on consecutive ticks", streak).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(Streak.class)
  void aTickThatDidNotEvaluateTheSignalDropsItsStreak(final Streak streak) {
    final List<String> offenders = new ArrayList<>();
    for (final Tick between : Tick.values()) {
      if (between == streak.tick)
        continue;
      final int restarts = restartsAfter(List.of(streak.tick, between, streak.tick));
      if (restarts != restartsOfOneTickAlone(between))
        offenders.add(between + " -> " + restarts + " restarts, expected " + restartsOfOneTickAlone(between));
    }
    assertThat(offenders).as("%s seen, then a tick in between, then %s again: no two consecutive ticks saw it", streak, streak)
        .isEmpty();
  }

  @Test
  void aClosedLogReopenedByAnInPlaceRestartIsNotMistakenForAZombie() {
    // (log closed), (division CLOSED then recovered by the monitor), (log closed again): the restart in between is the
    // one the CLOSED tick asked for, and the third tick is the first sighting of a new streak
    assertThat(restartsAfter(List.of(Tick.LOG_CLOSED, Tick.CLOSED, Tick.LOG_CLOSED))).isEqualTo(1);
  }

  @Test
  void aStreakSurvivesNothingThatDroppedItAcrossSeveralTicks() {
    // the dropped streak starts again from one, it does not resume where it was: a fresh pair still acts
    assertThat(restartsAfter(List.of(Tick.DEAD_UPDATER, Tick.SHUTDOWN_REQUESTED, Tick.DEAD_UPDATER, Tick.DEAD_UPDATER))).isEqualTo(1);
    assertThat(restartsAfter(List.of(Tick.LOG_CLOSED, Tick.CLOSING, Tick.LOG_CLOSED, Tick.LOG_CLOSED))).isEqualTo(1);
  }
}
