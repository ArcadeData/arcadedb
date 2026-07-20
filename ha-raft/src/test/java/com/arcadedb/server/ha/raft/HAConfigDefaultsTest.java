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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates that all HA-related {@link GlobalConfiguration} entries have sensible defaults.
 * Ported from apache-ratis branch, adapted to include new snapshot retry entries.
 */
class HAConfigDefaultsTest {

  @Test
  void allHAEntriesHaveNonNullDefaults() {
    for (final GlobalConfiguration config : GlobalConfiguration.values()) {
      if (config.getKey().startsWith("arcadedb.ha."))
        assertThat(config.getDefValue())
            .as("HA config '%s' should have a non-null default", config.getKey())
            .isNotNull();
    }
  }

  @Test
  void snapshotInstallRetriesDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES.getDefValue()).isEqualTo(3);
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRIES.getType()).isEqualTo(Integer.class);
  }

  @Test
  void snapshotInstallRetryBaseMsDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS.getDefValue()).isEqualTo(5000L);
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INSTALL_RETRY_BASE_MS.getType()).isEqualTo(Long.class);
  }

  @Test
  void snapshotMaxConcurrentDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getDefValue()).isEqualTo(2);
  }

  @Test
  void quorumTimeoutDefault() {
    assertThat(GlobalConfiguration.HA_QUORUM_TIMEOUT.getDefValue()).isEqualTo(10000);
  }

  @Test
  void replicationLagWarningDefault() {
    assertThat(GlobalConfiguration.HA_REPLICATION_LAG_WARNING.getDefValue()).isEqualTo(1000L);
  }

  /**
   * The periodic snapshot trigger (issue #5345) must be enabled by default: with only the count-based
   * HA_SNAPSHOT_THRESHOLD, a low-write cluster never purges its Raft log and eventually fills the volume.
   */
  @Test
  void snapshotIntervalDefaultIsEnabled() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INTERVAL.getDefValue()).isEqualTo(300_000L);
    assertThat(GlobalConfiguration.HA_SNAPSHOT_INTERVAL.getType()).isEqualTo(Long.class);
  }

  @Test
  void snapshotMinEntriesDefault() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_MIN_ENTRIES.getDefValue()).isEqualTo(64L);
    assertThat(GlobalConfiguration.HA_SNAPSHOT_MIN_ENTRIES.getType()).isEqualTo(Long.class);
  }

  @Test
  void raftStorageMinFreeSpacePercDefault() {
    assertThat(GlobalConfiguration.HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC.getDefValue()).isEqualTo(20);
    assertThat(GlobalConfiguration.HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC.getType()).isEqualTo(Integer.class);
  }
}
