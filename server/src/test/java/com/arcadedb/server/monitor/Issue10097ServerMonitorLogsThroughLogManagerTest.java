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
package com.arcadedb.server.monitor;

import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10097: {@link ServerMonitor} emitted its dozen records, its two low-resource WARNINGs among them, through a
 * private {@code java.util.logging} logger. With {@code arcadedb.log.impl=slf4j} they never reached the configured
 * backend; they went to JUL's own console handler at the JDK's default format. They go through {@link LogManager} now, so
 * whichever {@link Logger} is installed receives them.
 * <p>
 * A monitor built without a server cannot read the configuration to find the disk to measure, so its first check fails and the
 * loop reports it at WARNING ({@code Error during server monitoring}) before it carries on: a record the monitor emits with no
 * degraded host and no particular JDK (whether the HotSpot probe logs anything depends on the JDK).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10097ServerMonitorLogsThroughLogManagerTest {

  @Test
  void theMonitorsRecordsReachTheInstalledLogger() throws Exception {
    final Logger previous = LogManager.instance().getLogger();
    final CapturingLogger installed = new CapturingLogger();
    LogManager.instance().setLogger(installed);
    final ServerMonitor monitor = new ServerMonitor(null);
    try {
      monitor.start();
      assertThat(installed.monitoringError.await(30, TimeUnit.SECONDS))
          .as("the failed first check is reported through the installed logger").isTrue();
    } finally {
      monitor.stop();
      LogManager.instance().setLogger(previous);
    }

    assertThat(installed.records).anyMatch(
        r -> r.equals("WARNING:" + ServerMonitor.class.getName() + ":Error during server monitoring"));
  }

  /** Accepts every level and keeps {@code level:requester class:message} of what it is handed. */
  private static final class CapturingLogger implements Logger {
    final List<String>   records         = new CopyOnWriteArrayList<>();
    final CountDownLatch monitoringError = new CountDownLatch(1);

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(requester, level, message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(requester, level, message);
    }

    private void capture(final Object requester, final Level level, final String message) {
      records.add(level + ":" + (requester != null ? requester.getClass().getName() : null) + ":" + message);
      if (level == Level.WARNING && "Error during server monitoring".equals(message))
        monitoringError.countDown();
    }

    @Override
    public void flush() {
      // NOTHING TO FLUSH
    }
  }
}
