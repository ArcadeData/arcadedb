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
package com.arcadedb.utility;

import com.arcadedb.exception.ArcadeDBException;
import com.sun.management.HotSpotDiagnosticMXBean;

import javax.management.MBeanServer;
import java.io.*;
import java.lang.management.*;

public class JVMUtils {
  // This is the name of the HotSpot Diagnostic MBean
  private static final    String                  HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";
  private static volatile HotSpotDiagnosticMXBean hotspotMBean;

  public static String generateThreadDump(final String filterInclude, final String filterExclude) {
    final String[] filterIncludeItems =
        filterInclude != null && !filterInclude.trim().isEmpty() ? filterInclude.trim().split(",") : null;
    final String[] filterExcludeItems =
        filterExclude != null && !filterExclude.trim().isEmpty() ? filterExclude.trim().split(",") : null;

    final StringBuilder totalOutput = new StringBuilder();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (final ThreadInfo threadInfo : threadInfos) {
      if (threadInfo == null)
        continue;

      final StringBuilder output = new StringBuilder();

      output.append('"');
      output.append(threadInfo.getThreadName());
      output.append("\" ");

      output.append(
          String.format("%nWaited %d times = %dms - Blocked %d times = %dms - Locked monitors=%d synchronizers=%d - InNative=%s",//
              threadInfo.getWaitedCount(), threadInfo.getWaitedTime(), threadInfo.getBlockedCount(), threadInfo.getBlockedTime(),//
              threadInfo.getLockedMonitors().length, threadInfo.getLockedSynchronizers().length, threadInfo.isInNative()));

      if (threadInfo.getLockInfo() != null) {
        output.append(String.format("%nWaiting for lock %s", threadInfo.getLockName()));
        if (threadInfo.getLockOwnerName() != null)
          output.append(String.format(" owned by %s(%s)", threadInfo.getLockOwnerName(), threadInfo.getLockOwnerId()));
      }

      final Thread.State state = threadInfo.getThreadState();
      output.append("\n   java.lang.Thread.State: ");
      output.append(state);

      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        output.append("\n        at ");
        output.append(stackTraceElement);
      }

      if (filterIncludeItems != null || filterExcludeItems != null) {
        boolean found = filterExcludeItems != null;

        if (find(output.toString(), filterIncludeItems))
          found = true;

        if (find(output.toString(), filterExcludeItems))
          found = false;

        if (!found)
          continue;
      }

      totalOutput.append(output).append("\n\n");
    }
    return totalOutput.toString();
  }

  public static String dumpHeap(final boolean live) {
    if (hotspotMBean == null) {
      synchronized (JVMUtils.class) {
        if (hotspotMBean == null) {
          try {
            final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            hotspotMBean = ManagementFactory.newPlatformMXBeanProxy(server, HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
          } catch (final RuntimeException re) {
            throw re;
          } catch (final Exception exp) {
            throw new ArcadeDBException(exp);
          }
        }
      }
    }

    try {
      final File file = File.createTempFile("arcadedb.heapdump.bin", null);
      hotspotMBean.dumpHeap(file.getAbsolutePath(), live);

      final String content = FileUtils.readFileAsString(file);

      file.delete();

      return content;

    } catch (final RuntimeException re) {
      throw re;
    } catch (final Exception exp) {
      throw new ArcadeDBException(exp);
    }
  }

  private static boolean find(final String text, final String[] items) {
    if (items != null)
      for (int i = 0; i < items.length; i++)
        if (text.contains(items[i]))
          return true;

    return false;
  }
}
