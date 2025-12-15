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
import com.arcadedb.utility.CodeUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

public class ReplicationServerQuorumNoneIT extends ReplicationServerIT {
  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.HA_QUORUM.setValue("NONE");
  }

  @Override
  protected int getTxs() {
    // Reduced from 200 to align with use case: async replication is about behavior testing,
    // not large volume stress testing. With QUORUM=NONE, messages queue up asynchronously
    // and take much longer to drain than synchronous modes.
    return 100;
  }

  @Override
  protected int getVerticesPerTx() {
    // Reduced from 5000 to 1000 to prevent excessive queue buildup during async replication
    // Total: 100 * 1000 = 100,000 vertices is still sufficient to test async replication behavior
    return 1000;
  }

  @Override
  protected void waitForReplicationIsCompleted(final int serverNumber) {
    // With QUORUM=NONE (asynchronous replication), the leader doesn't wait for replica acknowledgment.
    // Messages are queued and processed asynchronously, which can take longer than synchronous modes.
    // Using a longer timeout to accommodate the async message queue processing.
    Awaitility.await()
        .atMost(10, TimeUnit.MINUTES)  // Increased from default 5 minutes for async queue draining
        .pollInterval(2, TimeUnit.SECONDS)
        .until(() -> getServer(serverNumber).getHA().getMessagesInQueue() == 0);
  }

  @AfterEach
  @Override
  public void endTest() {
    CodeUtils.sleep(5000);
    super.endTest();
    GlobalConfiguration.HA_QUORUM.setValue("MAJORITY");
  }
}
