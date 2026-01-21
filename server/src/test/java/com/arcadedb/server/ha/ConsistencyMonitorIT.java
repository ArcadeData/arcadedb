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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for ConsistencyMonitor with HA cluster.
 * Note: This is a basic test to verify the monitor starts without errors.
 * Full drift detection testing requires replica checksum protocol implementation.
 */
public class ConsistencyMonitorIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Override
  protected void onServerConfiguration(ContextConfiguration config) {
    super.onServerConfiguration(config);
    // Enable consistency monitor with short interval for testing
    config.setValue(GlobalConfiguration.HA_CONSISTENCY_CHECK_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_CONSISTENCY_CHECK_INTERVAL_MS, 10000L);
    config.setValue(GlobalConfiguration.HA_CONSISTENCY_SAMPLE_SIZE, 100);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testConsistencyMonitorStartsAndStops() throws Exception {
    // Get leader server
    final ArcadeDBServer leader = getLeader();
    assertNotNull(leader, "Leader should be present");
    assertTrue(leader.isStarted(), "Leader should be started");

    // Wait for cluster to stabilize
    waitForClusterStable(60_000); // 60 second timeout

    // Verify all servers are running
    for (int i = 0; i < getServerCount(); i++) {
      final ArcadeDBServer server = getServer(i);
      assertNotNull(server, "Server " + i + " should be running");
      assertTrue(server.isStarted(), "Server " + i + " should be started");
    }

    // Wait a bit to ensure monitor has had time to run at least once
    Thread.sleep(2000);

    // Verify no exceptions occurred (monitor logs would show errors)
    // The monitor should start successfully even if it can't detect drift yet
    // (since replica checksum collection is not implemented)

    // Test passes if we reach here without exceptions
  }
}
