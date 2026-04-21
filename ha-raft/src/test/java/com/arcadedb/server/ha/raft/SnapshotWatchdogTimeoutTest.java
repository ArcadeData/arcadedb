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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ArcadeStateMachine#computeSnapshotWatchdogTimeoutMs()} floor computation
 * and the {@link ArcadeStateMachine#WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER} constant.
 * <p>
 * Uses global configuration defaults (server is null in the state machine) so no server instance
 * is required. Each test saves and restores the relevant global config values.
 */
class SnapshotWatchdogTimeoutTest {

  private long savedWatchdogTimeout;
  private int  savedElectionTimeoutMax;

  @BeforeEach
  void saveDefaults() {
    savedWatchdogTimeout = GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.getValueAsLong();
    savedElectionTimeoutMax = GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getValueAsInteger();
  }

  @AfterEach
  void restoreDefaults() {
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(savedWatchdogTimeout);
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(savedElectionTimeoutMax);
  }

  @Test
  void watchdogElectionTimeoutMultiplier_isFour() {
    assertThat(ArcadeStateMachine.WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER).isEqualTo(4);
  }

  @Test
  void computeWatchdogTimeout_usesDefaultsWithNullServer() {
    // Default HA_ELECTION_TIMEOUT_MAX = 5000ms, floor = 4 * 5000 = 20_000ms
    // Default HA_SNAPSHOT_WATCHDOG_TIMEOUT = 30_000ms -> above floor, so 30_000 wins
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final long timeout = sm.computeSnapshotWatchdogTimeoutMs();
      final long expectedFloor = GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.getValueAsInteger()
          * ArcadeStateMachine.WATCHDOG_ELECTION_TIMEOUT_MULTIPLIER;
      final long configuredTimeout = GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.getValueAsLong();
      assertThat(timeout).isEqualTo(Math.max(configuredTimeout, expectedFloor));
    } finally {
      try { sm.close(); } catch (final Exception ignored) {}
    }
  }

  @Test
  void computeWatchdogTimeout_flooredWhenConfiguredBelowElectionMultiple() {
    // Set watchdog timeout below floor
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(1_000L);
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(5_000);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final long timeout = sm.computeSnapshotWatchdogTimeoutMs();
      // Floor = 4 * 5_000 = 20_000ms; configured is 1_000ms, so floor wins
      assertThat(timeout).isEqualTo(20_000L);
    } finally {
      try { sm.close(); } catch (final Exception ignored) {}
    }
  }

  @Test
  void computeWatchdogTimeout_usesConfiguredValueWhenAboveFloor() {
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(60_000L);
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(5_000);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final long timeout = sm.computeSnapshotWatchdogTimeoutMs();
      // Floor = 4 * 5_000 = 20_000ms; configured is 60_000ms, so configured wins
      assertThat(timeout).isEqualTo(60_000L);
    } finally {
      try { sm.close(); } catch (final Exception ignored) {}
    }
  }

  @Test
  void computeWatchdogTimeout_exactlyAtFloor() {
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(5_000);
    // Configured == floor: 4 * 5_000 = 20_000ms
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(20_000L);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final long timeout = sm.computeSnapshotWatchdogTimeoutMs();
      assertThat(timeout).isEqualTo(20_000L);
    } finally {
      try { sm.close(); } catch (final Exception ignored) {}
    }
  }

  @Test
  void computeWatchdogTimeout_highElectionTimeoutRaisesFloor() {
    // WAN cluster with very high election timeout
    GlobalConfiguration.HA_ELECTION_TIMEOUT_MAX.setValue(30_000);
    GlobalConfiguration.HA_SNAPSHOT_WATCHDOG_TIMEOUT.setValue(30_000L);

    final ArcadeStateMachine sm = new ArcadeStateMachine();
    try {
      final long timeout = sm.computeSnapshotWatchdogTimeoutMs();
      // Floor = 4 * 30_000 = 120_000ms; configured 30_000ms < floor
      assertThat(timeout).isEqualTo(120_000L);
    } finally {
      try { sm.close(); } catch (final Exception ignored) {}
    }
  }
}
