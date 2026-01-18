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

import com.arcadedb.log.LogManager;
import com.arcadedb.utility.CodeUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

@Tag("ha")
class ReplicationServerWriteAgainstReplicaIT extends ReplicationServerIT {

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  void testReplication() {
    // Ensure all servers are fully connected and synchronized before writing against replica
    // This is critical because we're writing against server 1 (replica) which must forward
    // writes to the leader (server 0)

    LogManager.instance().log(this, Level.INFO,
        "TEST: Waiting for all servers to be fully connected before writing against replica...");

    // Wait for cluster to be fully established
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> {
          // Check that server 1 (replica) is connected to the leader
          if (getServer(1).getHA() != null) {
            final String leaderName = getServer(1).getHA().getLeaderName();
            if (leaderName != null && !leaderName.isEmpty()) {
              LogManager.instance().log(this, Level.INFO,
                  "TEST: Server 1 connected to leader: " + leaderName);
              return true;
            }
          }
          return false;
        });

    // Additional wait to ensure connection is stable
    CodeUtils.sleep(2000);

    // Ensure all servers have empty replication queues
    for (int i = 0; i < getServerCount(); i++) {
      waitForReplicationIsCompleted(i);
    }

    LogManager.instance().log(this, Level.INFO,
        "TEST: Starting write operations against replica (server 1)...");

    // Now perform the test writing against server 1 (replica)
    testReplication(1);

    // Wait for replication to complete on all servers
    waitForReplicationIsCompleted(1);
    waitForReplicationIsCompleted(0);

    // Also wait for server 2 if it exists (3-server cluster)
    if (getServerCount() > 2) {
      waitForReplicationIsCompleted(2);
    }
  }

  @Override
  protected int getTxs() {
    // Reduced from 200 to 100 for replica write testing
    // Writing against a replica adds overhead as writes are forwarded to the leader
    return 100;
  }

  @Override
  protected int getVerticesPerTx() {
    // Reduced from 5000 to 1000 to reduce load when writing through replica
    // Total: 100 * 1000 = 100,000 vertices is sufficient for testing replica write behavior
    return 1000;
  }

  @Override
  protected void waitForReplicationIsCompleted(final int serverNumber) {
    // When writing against a replica, operations are forwarded to the leader and then
    // replicated back to all replicas. This adds extra latency and queue processing.
    // Using a longer timeout to accommodate this additional hop.
    Awaitility.await()
        .atMost(7, TimeUnit.MINUTES)  // Increased from default 5 minutes for replica write forwarding
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> getServer(serverNumber).getHA().getMessagesInQueue() == 0);
  }
}
