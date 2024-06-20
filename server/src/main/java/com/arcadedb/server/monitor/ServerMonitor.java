/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.monitor;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.event.ServerEventLog;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Monitor ArcadeDB's server health.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerMonitor {
  private static final int            INTERVAL_TIME                = 10_000;
  private static final int            MINS_30                      = 30 * 60 * 1_000;
  private static final int            HOURS_24                     = 24 * 60 * 60 * 1_000;
  private final        ArcadeDBServer server;
  private volatile     Thread         checker;
  private              AtomicBoolean  running                      = new AtomicBoolean(false);
  private              long           lastHotspotSafepointTime     = 0L;
  private              long           lastHotspotSafepointCount    = 0L;
  private              long           lastHeapWarningReported      = 0L;
  private              long           lastDiskSpaceWarningReported = 0L;

  public ServerMonitor(final ArcadeDBServer server) {
    this.server = server;
  }

  public void start() {
    running.set(true);
    checker = new Thread(() -> monitor());
    checker.start();
  }

  private void monitor() {
    while (running.get()) {
      try {
        checkDiskSpace();
        checkHeapRAM();
//        checkJVMHotSpot();

        Thread.sleep(INTERVAL_TIME);
      } catch (InterruptedException e) {
        break;
      } catch (Exception e) {
        // IGNORE IT
        e.printStackTrace();
      }
    }
  }

  private void checkDiskSpace() {
    if (System.currentTimeMillis() - lastDiskSpaceWarningReported < HOURS_24)
      // REPORT ONLY EVERY 24H FROM THE LAST WARNING
      return;

    final long freeSpace = new File(".").getFreeSpace();
    final long totalSpace = new File(".").getTotalSpace();
    final float freeSpacePerc = freeSpace * 100F / totalSpace;
    if (freeSpacePerc < 20) {
      // REPORT THE SPIKE
      server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null, "Available space on disk is only " + ((int) freeSpacePerc) + "%");
      lastDiskSpaceWarningReported = System.currentTimeMillis();
    }
  }

  private void checkHeapRAM() {
    if (System.currentTimeMillis() - lastHeapWarningReported < MINS_30)
      // REPORT ONLY EVERY 30 MINS FROM THE LAST WARNING
      return;

    final Runtime runtime = Runtime.getRuntime();
    final long heapUsed = runtime.totalMemory() - runtime.freeMemory();
    final long heapMax = runtime.maxMemory();
    final float heapAvailablePerc = (heapMax - heapUsed) * 100F / heapMax;

    if (heapAvailablePerc < 20) {
      // REPORT THE SPIKE
      server.getEventLog()
          .reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null, "Server overloaded: available heap RAM is only " + ((int) heapAvailablePerc) + "%");
      lastHeapWarningReported = System.currentTimeMillis();
    }
  }

  private void checkJVMHotSpot() {
    final sun.management.HotspotRuntimeMBean runtime = sun.management.ManagementFactoryHelper.getHotspotRuntimeMBean();


    final long hotspotSafepointTime = runtime.getTotalSafepointTime();
    final long hotspotSafepointCount = runtime.getSafepointCount();

    if (lastHotspotSafepointCount > 0) {
      final float lastAvgSafepointTime = lastHotspotSafepointTime / (float) lastHotspotSafepointCount;
      final float avgSafepointTime = hotspotSafepointTime / (float) hotspotSafepointCount;
      final float deltaPerc = (avgSafepointTime - lastAvgSafepointTime) * 100 / lastAvgSafepointTime;

      if (deltaPerc > 20) {
        // REPORT THE SPIKE
        server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null,
            "Server overloaded: JVM Safepoint spiked up " + ((int) deltaPerc) + "% from the last sampling");
      }
    }

    lastHotspotSafepointTime = hotspotSafepointTime;
    lastHotspotSafepointCount = hotspotSafepointCount;
  }

  public void stop() {
    running.set(false);

    if (checker != null) {
      try {
        checker.interrupt();
        checker.join(INTERVAL_TIME + 100);
      } catch (Exception e) {
        // IGNORE IT, ASSUMING THE THREAD STOPPED
      }
    }
  }

}
