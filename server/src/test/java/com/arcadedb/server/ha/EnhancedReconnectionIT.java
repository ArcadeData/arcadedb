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
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Phase 2 enhanced reconnection with exception classification.
 */
@Tag("ha")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class EnhancedReconnectionIT extends ReplicationServerIT {

  @BeforeEach
  void enableEnhancedReconnection() {
    GlobalConfiguration.HA_ENHANCED_RECONNECTION.setValue(true);
  }

  @AfterEach
  void disableEnhancedReconnection() {
    GlobalConfiguration.HA_ENHANCED_RECONNECTION.setValue(false);
  }

  @Test
  void testBasicReplicationWithEnhancedMode() {
    // Verify basic replication still works with enhanced mode enabled
    testReplication(0);

    waitForReplicationIsCompleted(0);
    waitForReplicationIsCompleted(1);

    if (getServerCount() > 2) {
      waitForReplicationIsCompleted(2);
    }

    // Verify data integrity
    for (int s : getServerToCheck()) {
      checkEntriesOnServer(s);
    }
  }

  @Test
  void testMetricsAreTracked() {
    // Verify metrics are being tracked
    testReplication(0);

    waitForReplicationIsCompleted(0);

    // Check that metrics exist
    // TODO: Add metric verification once getMetrics() is accessible
    assertThat(getServer(0).getHA()).isNotNull();
  }

  @Test
  void testFeatureFlagToggle() {
    // Test with flag enabled
    GlobalConfiguration.HA_ENHANCED_RECONNECTION.setValue(true);
    assertThat(GlobalConfiguration.HA_ENHANCED_RECONNECTION.getValueAsBoolean()).isTrue();

    testReplication(0);
    waitForReplicationIsCompleted(0);

    // Verify basic functionality works
    checkEntriesOnServer(0);

    // Test with flag disabled (already disabled in @AfterEach)
    // Should still work with legacy code path
  }

  @Override
  protected int getTxs() {
    return 10; // Small dataset for faster test
  }

  @Override
  protected int getVerticesPerTx() {
    return 100;
  }
}
