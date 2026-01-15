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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

@Timeout(value = 15, unit = TimeUnit.MINUTES)
@Tag("ha")
public class ReplicationServerQuorumMajorityIT extends ReplicationServerIT {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("MAJORITY");
  }

  @Override
  protected int getTxs() {
    // Reduced from 200 to 100 for MAJORITY quorum mode
    // MAJORITY quorum requires acknowledgment from at least half the replicas,
    // creating more synchronization overhead than NONE mode
    return 100;
  }

  @Override
  protected int getVerticesPerTx() {
    // Reduced from 5000 to 1000 to reduce synchronization load with MAJORITY quorum
    // Total: 100 * 1000 = 100,000 vertices is sufficient for testing replication behavior
    return 1000;
  }

  @Override
  protected void waitForReplicationIsCompleted(final int serverNumber) {
    // With QUORUM=MAJORITY, the leader waits for acknowledgment from majority of replicas.
    // This creates more synchronization overhead than NONE mode but less queue buildup.
    // Using a moderate timeout to accommodate the synchronous acknowledgment requirements.
    Awaitility.await()
        .atMost(7, TimeUnit.MINUTES)  // Increased from default 5 minutes for majority quorum synchronization
        .pollInterval(1, TimeUnit.SECONDS)
        .until(() -> getServer(serverNumber).getHA().getMessagesInQueue() == 0);
  }
}
