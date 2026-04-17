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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link ArcadeDBStateMachine#computeSnapshotWatchdogTimeoutMs}. The watchdog must never
 * fire before Raft elections can realistically complete on the configured cluster, otherwise
 * a WAN follower would launch a redundant snapshot download every time an election is in flight.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SnapshotWatchdogTimeoutTest {

  private static final int DEFAULT_CONFIGURED = (Integer) GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.getDefValue();
  private static final int DEFAULT_ELECTION_MAX = (Integer) GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getDefValue();

  /**
   * With the shipped defaults (30s watchdog, 5s election max), the configured value wins and the
   * effective watchdog is 30s - preserving pre-existing behavior.
   */
  @Test
  void defaultsGive30SecondWatchdog() {
    assertThat(ArcadeDBStateMachine.computeSnapshotWatchdogTimeoutMs(new ContextConfiguration()))
        .isEqualTo(DEFAULT_CONFIGURED)
        .isGreaterThanOrEqualTo((long) DEFAULT_ELECTION_MAX * ArcadeDBStateMachine.WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER);
  }

  /**
   * If the operator raises {@code HA_ELECTION_TIMEOUT_MAX} to 20s for a WAN cluster but leaves
   * the watchdog at the 30s default, the floor (4 x 20s = 80s) must win so the watchdog doesn't
   * trigger a redundant snapshot download mid-election.
   */
  @Test
  void largeElectionTimeoutExpandsWatchdogViaFloor() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 20_000);

    final long expectedFloor = 20_000L * ArcadeDBStateMachine.WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER;
    assertThat(ArcadeDBStateMachine.computeSnapshotWatchdogTimeoutMs(cfg))
        .isEqualTo(expectedFloor);
  }

  /**
   * When the operator explicitly sets a watchdog larger than the floor, their value wins.
   */
  @Test
  void explicitWatchdogWinsOverFloor() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 10_000); // floor = 40s
    cfg.setValue(GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT, 120_000); // 2 minutes

    assertThat(ArcadeDBStateMachine.computeSnapshotWatchdogTimeoutMs(cfg))
        .isEqualTo(120_000L);
  }

  /**
   * When both the configured watchdog and the election timeout are tiny, the small floor applies
   * (no artificial lower bound beyond the multiplier-derived floor). Documents the current
   * behavior so any future change is deliberate.
   */
  @Test
  void lowValuesAreRespected() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX, 500);
    cfg.setValue(GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT, 1_000);

    assertThat(ArcadeDBStateMachine.computeSnapshotWatchdogTimeoutMs(cfg))
        .isEqualTo(Math.max(1_000L, 500L * ArcadeDBStateMachine.WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER));
  }

  /**
   * Null configuration falls back to the shipped defaults without throwing. Guards the boot
   * path where the state machine may run outside a fully wired {@code ArcadeDBServer}.
   */
  @Test
  void nullConfigurationFallsBackToDefaults() {
    assertThat(ArcadeDBStateMachine.computeSnapshotWatchdogTimeoutMs(null))
        .isEqualTo(Math.max(
            (long) DEFAULT_CONFIGURED,
            (long) DEFAULT_ELECTION_MAX * ArcadeDBStateMachine.WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER));
  }
}
