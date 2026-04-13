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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Periodically checks whether the Ratis server is in a healthy lifecycle state and triggers
 * a restart via the provided callback if it has entered CLOSED or CLOSING state (e.g., after
 * a network partition caused gRPC connection failures).
 * <p>
 * The 3-second interval balances quick partition recovery against CPU overhead of the lifecycle
 * check. Checks are skipped when the ArcadeDB server is not fully online.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class HealthMonitor {

  // 3-second interval balances quick recovery against CPU overhead of the lifecycle check.
  private static final int INITIAL_DELAY_SECS = 5;
  private static final int INTERVAL_SECS      = 3;

  private final ArcadeDBServer           server;
  private final Runnable                 checkAndRestart;
  private       ScheduledExecutorService executor;

  /**
   * @param server          ArcadeDB server instance (used to skip checks when not online)
   * @param checkAndRestart callback that performs the actual health check and restart logic
   */
  public HealthMonitor(final ArcadeDBServer server, final Runnable checkAndRestart) {
    this.server = server;
    this.checkAndRestart = checkAndRestart;
  }

  /**
   * Starts the periodic health check on a dedicated daemon thread.
   */
  public void start() {
    executor = Executors.newSingleThreadScheduledExecutor(r -> {
      final Thread t = new Thread(r, "arcadedb-ratis-health-monitor");
      t.setDaemon(true);
      return t;
    });
    executor.scheduleAtFixedRate(() -> {
      if (server.getStatus() != ArcadeDBServer.STATUS.ONLINE)
        return;
      try {
        checkAndRestart.run();
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Health monitor error: %s", e.getMessage());
      }
    }, INITIAL_DELAY_SECS, INTERVAL_SECS, TimeUnit.SECONDS);
  }

  /**
   * Stops the periodic health check and releases the scheduler thread.
   */
  public void stop() {
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }
}
