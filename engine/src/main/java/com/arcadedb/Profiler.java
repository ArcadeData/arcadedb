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
package com.arcadedb;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.*;
import java.util.*;

public class Profiler {
  public static final Profiler INSTANCE = new Profiler();

  private final Set<DatabaseInternal> databases = new LinkedHashSet<>();

  protected Profiler() {
  }

  public synchronized void registerDatabase(final DatabaseInternal database) {
    databases.add(database);
  }

  public void unregisterDatabase(final DatabaseInternal database) {
    databases.remove(database);
  }

  public synchronized JSONObject toJSON() {
    final JSONObject json = new JSONObject();

    long readCacheUsed = 0;
    long writeCacheUsed = 0;
    long cacheMax = 0;
    long pagesRead = 0;
    long pagesWritten = 0;
    long pagesReadSize = 0;
    long pagesWrittenSize = 0;
    long pageFlushQueueLength = 0;
    long asyncQueueLength = 0;
    int asyncParallelLevel = 0;
    long pageCacheHits = 0;
    long pageCacheMiss = 0;
    long totalOpenFiles = 0;
    long maxOpenFiles = 0;
    long walPagesWritten = 0;
    long walBytesWritten = 0;
    long walTotalFiles = 0;
    long concurrentModificationExceptions = 0;

    long txCommits = 0;
    long txRollbacks = 0;
    long createRecord = 0;
    long readRecord = 0;
    long updateRecord = 0;
    long deleteRecord = 0;
    long queries = 0;
    long commands = 0;
    long scanType = 0;
    long scanBucket = 0;
    long iterateType = 0;
    long iterateBucket = 0;
    long countType = 0;
    long countBucket = 0;
    long evictionRuns = 0;
    long pagesEvicted = 0;
    int readCachePages = 0;
    int writeCachePages = 0;
    long indexCompactions = 0;

    for (final DatabaseInternal db : databases) {
      final Map<String, Object> dbStats = db.getStats();
      txCommits += (long) dbStats.get("txCommits");
      txRollbacks += (long) dbStats.get("txRollbacks");
      createRecord += (long) dbStats.get("createRecord");
      readRecord += (long) dbStats.get("readRecord");
      updateRecord += (long) dbStats.get("updateRecord");
      deleteRecord += (long) dbStats.get("deleteRecord");
      queries += (long) dbStats.get("queries");
      commands += (long) dbStats.get("commands");
      scanType += (long) dbStats.get("scanType");
      scanBucket += (long) dbStats.get("scanBucket");
      iterateType += (long) dbStats.get("iterateType");
      iterateBucket += (long) dbStats.get("iterateBucket");
      countType += (long) dbStats.get("countType");
      countBucket += (long) dbStats.get("countBucket");
      indexCompactions += (long) dbStats.get("indexCompactions");

      final PageManager.PPageManagerStats pStats = db.getPageManager().getStats();
      readCacheUsed += pStats.readCacheRAM;
      writeCacheUsed += pStats.writeCacheRAM;
      cacheMax += pStats.maxRAM;
      pagesRead += pStats.pagesRead;
      pagesReadSize += pStats.pagesReadSize;
      pagesWritten += pStats.pagesWritten;
      pagesWrittenSize += pStats.pagesWrittenSize;
      pageFlushQueueLength += pStats.pageFlushQueueLength;
      pageCacheHits += pStats.cacheHits;
      pageCacheMiss += pStats.cacheMiss;
      concurrentModificationExceptions += pStats.concurrentModificationExceptions;
      evictionRuns += pStats.evictionRuns;
      pagesEvicted += pStats.pagesEvicted;
      readCachePages += pStats.readCachePages;

      final FileManager.FileManagerStats fStats = db.getFileManager().getStats();
      totalOpenFiles += fStats.totalOpenFiles;
      maxOpenFiles += fStats.maxOpenFiles;

      final DatabaseAsyncExecutorImpl.DBAsyncStats aStats = ((DatabaseAsyncExecutorImpl) db.async()).getStats();
      asyncQueueLength += aStats.queueSize;
      asyncParallelLevel = db.async().getParallelLevel();

      final Map<String, Object> walStats = db.getTransactionManager().getStats();
      walPagesWritten += (Long) walStats.get("pagesWritten");
      walBytesWritten += (Long) walStats.get("bytesWritten");
      walTotalFiles += (Long) walStats.get("logFiles");
    }

    json.put("readCacheUsed", new JSONObject().put("space", readCacheUsed));
    json.put("writeCacheUsed", new JSONObject().put("space", writeCacheUsed));
    json.put("cacheMax", new JSONObject().put("space", cacheMax));
    json.put("pagesRead", new JSONObject().put("count", pagesRead));
    json.put("pagesWritten", new JSONObject().put("count", pagesWritten));
    json.put("pagesReadSize", new JSONObject().put("space", pagesReadSize));
    json.put("pagesWrittenSize", new JSONObject().put("space", pagesWrittenSize));
    json.put("pageFlushQueueLength", new JSONObject().put("value", pageFlushQueueLength));
    json.put("asyncQueueLength", new JSONObject().put("value", asyncQueueLength));
    json.put("asyncParallelLevel", new JSONObject().put("count", asyncParallelLevel));
    json.put("pageCacheHits", new JSONObject().put("count", pageCacheHits));
    json.put("pageCacheMiss", new JSONObject().put("count", pageCacheMiss));
    json.put("totalOpenFiles", new JSONObject().put("count", totalOpenFiles));
    json.put("maxOpenFiles", new JSONObject().put("count", maxOpenFiles));
    json.put("walPagesWritten", new JSONObject().put("count", walPagesWritten));
    json.put("walBytesWritten", new JSONObject().put("space", walBytesWritten));
    json.put("walTotalFiles", walTotalFiles);
    json.put("concurrentModificationExceptions", new JSONObject().put("count", concurrentModificationExceptions));

    json.put("txCommits", new JSONObject().put("count", txCommits));
    json.put("txRollbacks", new JSONObject().put("count", txRollbacks));
    json.put("createRecord", new JSONObject().put("count", createRecord));
    json.put("readRecord", new JSONObject().put("count", readRecord));
    json.put("updateRecord", new JSONObject().put("count", updateRecord));
    json.put("deleteRecord", new JSONObject().put("count", deleteRecord));
    json.put("queries", new JSONObject().put("count", queries));
    json.put("commands", new JSONObject().put("count", commands));
    json.put("scanType", new JSONObject().put("count", scanType));
    json.put("scanBucket", new JSONObject().put("count", scanBucket));
    json.put("iterateType", new JSONObject().put("count", iterateType));
    json.put("iterateBucket", new JSONObject().put("count", iterateBucket));
    json.put("countType", new JSONObject().put("count", countType));
    json.put("countBucket", new JSONObject().put("count", countBucket));
    json.put("evictionRuns", new JSONObject().put("count", evictionRuns));
    json.put("pagesEvicted", new JSONObject().put("count", pagesEvicted));
    json.put("readCachePages", new JSONObject().put("count", readCachePages));
    json.put("writeCachePages", new JSONObject().put("count", writeCachePages));
    json.put("indexCompactions", new JSONObject().put("count", indexCompactions));

    final long freeSpace = new File(".").getFreeSpace();
    final long totalSpace = new File(".").getTotalSpace();
    final float freeSpacePerc = freeSpace * 100F / totalSpace;

    json.put("diskFreeSpace", new JSONObject().put("space", freeSpace));
    json.put("diskTotalSpace", new JSONObject().put("space", totalSpace));
    json.put("diskFreeSpacePerc", new JSONObject().put("perc", freeSpacePerc));

    json.put("gcTime", new JSONObject().put("count", getGarbageCollectionTime()));

    final Runtime runtime = Runtime.getRuntime();
    json.put("ramHeapUsed", new JSONObject().put("space", runtime.totalMemory() - runtime.freeMemory()));
    json.put("ramHeapMax", new JSONObject().put("space", runtime.maxMemory()));
    json.put("ramHeapAvailablePerc",
        new JSONObject().put("perc", (runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 100F / runtime.maxMemory()));

    try {
      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName osMBeanName = ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);

      if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
        final long osTotalMem = ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
        final long osUsedMem = osTotalMem - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

        json.put("ramOsUsed", new JSONObject().put("space", osUsedMem));
        json.put("ramOsTotal", new JSONObject().put("space", osTotalMem));

        final OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        json.put("cpuLoad", new JSONObject().put("perc", bean.getSystemLoadAverage()));
      }

      final sun.management.HotspotRuntimeMBean hotSpotRunTime = sun.management.ManagementFactoryHelper.getHotspotRuntimeMBean();
      json.put("jvmSafePointTime", new JSONObject().put("value", hotSpotRunTime.getTotalSafepointTime()));
      json.put("jvmSafePointCount", new JSONObject().put("count", hotSpotRunTime.getSafepointCount()));
      json.put("jvmAvgSafePointTime", new JSONObject().put("value", hotSpotRunTime.getTotalSafepointTime() / (float) hotSpotRunTime.getSafepointCount()));

    } catch (final Throwable e) {
      // JMX NOT AVAILABLE, AVOID OS DATA
    }

    json.put("totalDatabases", new JSONObject().put("count", databases.size()));
    json.put("cpuCores", new JSONObject().put("count", Runtime.getRuntime().availableProcessors()));

    final String osName = System.getProperty("os.name");
    final String osVersion = System.getProperty("os.version");
    final String vmName = System.getProperty("java.vm.name");
    final String vmVendorVersion = System.getProperty("java.vendor.version");
    final String vmVersion = System.getProperty("java.version");
    json.put("configuration", new JSONObject().put("description",
        osName + " " + osVersion + " - " + (vmName != null ? vmName : "Java") + " " + vmVersion + " " + (vmVendorVersion != null ?
            "(" + vmVendorVersion + ")" :
            "")));

    return json;
  }

  public synchronized void dumpMetrics(final PrintStream out) {

    final StringBuilder buffer = new StringBuilder("\n");

    final long freeSpaceInMB = new File(".").getFreeSpace();
    final long totalSpaceInMB = new File(".").getTotalSpace();

    long readCacheUsed = 0;
    long writeCacheUsed = 0;
    long cacheMax = 0;
    long pagesRead = 0;
    long pagesWritten = 0;
    long pagesReadSize = 0;
    long pagesWrittenSize = 0;
    long pageFlushQueueLength = 0;
    long asyncQueueLength = 0;
    int asyncParallelLevel = 0;
    long pageCacheHits = 0;
    long pageCacheMiss = 0;
    long totalOpenFiles = 0;
    long maxOpenFiles = 0;
    long walPagesWritten = 0;
    long walBytesWritten = 0;
    long walTotalFiles = 0;
    long concurrentModificationExceptions = 0;

    long txCommits = 0;
    long txRollbacks = 0;
    long createRecord = 0;
    long readRecord = 0;
    long updateRecord = 0;
    long deleteRecord = 0;
    long queries = 0;
    long commands = 0;
    long scanType = 0;
    long scanBucket = 0;
    long iterateType = 0;
    long iterateBucket = 0;
    long countType = 0;
    long countBucket = 0;
    long evictionRuns = 0;
    long pagesEvicted = 0;
    int readCachePages = 0;
    int writeCachePages = 0;
    long indexCompactions = 0;

    try {
      for (final DatabaseInternal db : databases) {
        final Map<String, Object> dbStats = db.getStats();
        txCommits += (long) dbStats.get("txCommits");
        txRollbacks += (long) dbStats.get("txRollbacks");
        createRecord += (long) dbStats.get("createRecord");
        readRecord += (long) dbStats.get("readRecord");
        updateRecord += (long) dbStats.get("updateRecord");
        deleteRecord += (long) dbStats.get("deleteRecord");
        queries += (long) dbStats.get("queries");
        commands += (long) dbStats.get("commands");
        scanType += (long) dbStats.get("scanType");
        scanBucket += (long) dbStats.get("scanBucket");
        iterateType += (long) dbStats.get("iterateType");
        iterateBucket += (long) dbStats.get("iterateBucket");
        countType += (long) dbStats.get("countType");
        countBucket += (long) dbStats.get("countBucket");
        indexCompactions += (long) dbStats.get("indexCompactions");

        final PageManager.PPageManagerStats pStats = db.getPageManager().getStats();
        readCacheUsed += pStats.readCacheRAM;
        writeCacheUsed += pStats.writeCacheRAM;
        cacheMax += pStats.maxRAM;
        pagesRead += pStats.pagesRead;
        pagesReadSize += pStats.pagesReadSize;
        pagesWritten += pStats.pagesWritten;
        pagesWrittenSize += pStats.pagesWrittenSize;
        pageFlushQueueLength += pStats.pageFlushQueueLength;
        pageCacheHits += pStats.cacheHits;
        pageCacheMiss += pStats.cacheMiss;
        concurrentModificationExceptions += pStats.concurrentModificationExceptions;
        evictionRuns += pStats.evictionRuns;
        pagesEvicted += pStats.pagesEvicted;
        readCachePages += pStats.readCachePages;

        final FileManager.FileManagerStats fStats = db.getFileManager().getStats();
        totalOpenFiles += fStats.totalOpenFiles;
        maxOpenFiles += fStats.maxOpenFiles;

        final DatabaseAsyncExecutorImpl.DBAsyncStats aStats = ((DatabaseAsyncExecutorImpl) db.async()).getStats();
        asyncQueueLength += aStats.queueSize;
        asyncParallelLevel = db.async().getParallelLevel();

        final Map<String, Object> walStats = db.getTransactionManager().getStats();
        walPagesWritten += (Long) walStats.get("pagesWritten");
        walBytesWritten += (Long) walStats.get("bytesWritten");
        walTotalFiles += (Long) walStats.get("logFiles");
      }

      buffer.append(String.format("ARCADEDB %s Profiler", Constants.getRawVersion()));

      final Runtime runtime = Runtime.getRuntime();

      final long gcTime = getGarbageCollectionTime();

      boolean dumpWithJmx = false;
      try {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        final ObjectName osMBeanName = ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);

        if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
          final long osTotalMem = ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
          final long osUsedMem = osTotalMem - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

          buffer.append(String.format("%n JVM heap=%s/%s os=%s/%s gc=%dms", FileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
              FileUtils.getSizeAsString(runtime.maxMemory()), FileUtils.getSizeAsString(osUsedMem), FileUtils.getSizeAsString(osTotalMem), gcTime));

          dumpWithJmx = true;
        }
      } catch (final Exception e) {
        // JMX NOT AVAILABLE, AVOID OS DATA
      }

      if (!dumpWithJmx)
        buffer.append(String.format("%n JVM heap=%s/%s gc=%dms", FileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
            FileUtils.getSizeAsString(runtime.maxMemory()), gcTime));

      buffer.append(String.format("%n PAGE-CACHE read=%s (pages=%d) write=%s (pages=%d) max=%s readOps=%d (%s) writeOps=%d (%s)",
          FileUtils.getSizeAsString(readCacheUsed), readCachePages, FileUtils.getSizeAsString(writeCacheUsed), writeCachePages,
          FileUtils.getSizeAsString(cacheMax), pagesRead, FileUtils.getSizeAsString(pagesReadSize), pagesWritten, FileUtils.getSizeAsString(pagesWrittenSize)));

      buffer.append(String.format("%n DB databases=%d asyncParallelLevel=%d asyncQueue=%d txCommits=%d txRollbacks=%d queries=%d commands=%d", databases.size(),
          asyncParallelLevel, asyncQueueLength, txCommits, txRollbacks, queries, commands));
      buffer.append(String.format("%n    createRecord=%d readRecord=%d updateRecord=%d deleteRecord=%d", createRecord, readRecord, updateRecord, deleteRecord));
      buffer.append(
          String.format("%n    scanType=%d scanBucket=%d iterateType=%d iterateBucket=%d countType=%d countBucket=%d", scanType, scanBucket, iterateType,
              iterateBucket, countType, countBucket));

      buffer.append(String.format("%n INDEXES compactions=%d", indexCompactions));

      buffer.append(
          String.format("%n PAGE-MANAGER flushQueue=%d cacheHits=%d cacheMiss=%d concModExceptions=%d evictionRuns=%d pagesEvicted=%d", pageFlushQueueLength,
              pageCacheHits, pageCacheMiss, concurrentModificationExceptions, evictionRuns, pagesEvicted));

      buffer.append(
          String.format("%n WAL totalFiles=%d pagesWritten=%d bytesWritten=%s", walTotalFiles, walPagesWritten, FileUtils.getSizeAsString(walBytesWritten)));

      buffer.append(String.format("%n FILE-MANAGER FS=%s/%s openFiles=%d maxFilesOpened=%d", FileUtils.getSizeAsString(freeSpaceInMB),
          FileUtils.getSizeAsString(totalSpaceInMB), totalOpenFiles, maxOpenFiles));

      out.println(buffer);
    } catch (final Exception e) {
      out.println("Error on displaying metrics (" + e + ")");
    }
  }

  private static long getGarbageCollectionTime() {
    long collectionTime = 0;
    for (final GarbageCollectorMXBean garbageCollectorMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      collectionTime += garbageCollectorMXBean.getCollectionTime();
    }
    return collectionTime;
  }
}
