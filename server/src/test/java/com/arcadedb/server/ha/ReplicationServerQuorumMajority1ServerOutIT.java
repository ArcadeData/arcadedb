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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ReplicationCallback;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ReplicationServerQuorumMajority1ServerOutIT extends ReplicationServerIT {
  private final AtomicInteger messages = new AtomicInteger();

  @Override
  protected void onBeforeStarting(final ArcadeDBServer server) {
    if (server.getServerName().equals("ArcadeDB_2"))
      server.registerTestEventListener(new ReplicationCallback() {
        @Override
        public void onEvent(final Type type, final Object object, final ArcadeDBServer server) {
          if (!serversSynchronized)
            return;

          if (type == Type.REPLICA_MSG_RECEIVED) {
            if (messages.incrementAndGet() > 100) {
              LogManager.instance().log(this, Level.FINE, "TEST: Stopping Replica 2...");
              getServer(2).stop();
            }
          }
        }
      });
  }

  protected int[] getServerToCheck() {
    return new int[] { 0, 1 };
  }

  @Override
  protected int getTxs() {
    return 300;
  }
}
