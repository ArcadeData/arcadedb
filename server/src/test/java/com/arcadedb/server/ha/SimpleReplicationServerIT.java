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

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple reference test demonstrating Phase 1 HA test patterns.
 *
 * <p>This test serves as a template for converting other HA tests to use:
 * <ul>
 *   <li>@Timeout annotations instead of manual timeout checks
 *   <li>waitForClusterStable() instead of Thread.sleep()
 *   <li>Clear, well-commented test structure
 * </ul>
 *
 * <p>Pattern to follow for other test conversions:
 * <pre>
 * 1. Add @Timeout annotation (5 minutes for simple tests, 15 for complex)
 * 2. Replace all Thread.sleep() with waitForClusterStable()
 * 3. Add comments explaining the test flow
 * 4. Verify stability with multiple test runs
 * </pre>
 */

@Tag("ha")
public class SimpleReplicationServerIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * Simple replication test demonstrating the correct Phase 1 patterns.
   *
   * <p>This test creates a small number of vertices on the leader and verifies
   * they replicate correctly to all replicas using proper stabilization waits.
   *
   * <p><b>Phase 1 Pattern Demonstration:</b>
   * <ul>
   *   <li>Use @Timeout(5 minutes) for simple tests - provides clear failure indication
   *   <li>Use waitForClusterStable() after data operations - ensures all
   *       servers are online, connected, and replication queues are drained
   *   <li>Keep test simple and focused - 10-20 vertices, not thousands
   *   <li>Add clear comments for maintainability
   * </ul>
   */
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void simpleReplicationTest() throws Exception {
    // 1. Get leader server and open database
    // Note: BaseGraphServerTest.beginTest() already sets up the cluster
    final ArcadeDBServer leader = getLeader();
    final Database db = getServerDatabase(0, getDatabaseName());

    // Ensure clean transaction state
    db.rollbackAllNested();

    // 2. Create a small set of vertices in a transaction
    // Keep it simple: 10 vertices is enough to verify replication works
    db.transaction(() -> {
      for (int i = 1; i <= 10; i++) {
        final MutableVertex v = db.newVertex(VERTEX1_TYPE_NAME);
        v.set("id", i);
        v.set("name", "test-vertex-" + i);
        v.save();
      }
    });

    // 3. Wait for cluster to stabilize - THIS IS THE KEY PHASE 1 PATTERN
    // This replaces Thread.sleep() with condition-based waiting:
    // - Phase 1: All servers are ONLINE
    // - Phase 2: Leader is elected and ready
    // - Phase 3: All replicas are connected to the leader
    // Using HATestHelpers for consistent cluster stabilization across all tests
    HATestHelpers.waitForClusterStable(getServers(), getServerCount() - 1);

    // 4. Verify replication on all servers
    // Expected: 1 vertex from setup + 10 new vertices = 11 total
    for (int i = 0; i < getServerCount(); i++) {
      final int serverIndex = i;
      final Database serverDb = getServerDatabase(i, getDatabaseName());
      serverDb.transaction(() -> {
        final long count = serverDb.countType(VERTEX1_TYPE_NAME, true);
        assertThat(count)
            .withFailMessage("Server " + serverIndex + " should have 11 vertices (1 from setup + 10 new)")
            .isEqualTo(11);
      });
    }
  }
}
