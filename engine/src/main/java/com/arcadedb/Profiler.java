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
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.sun.management.OperatingSystemMXBean;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.*;
import java.lang.management.*;
import java.util.*;
import java.util.logging.*;

public class Profiler {
  public static final Profiler INSTANCE = new Profiler();

  /**
   * Keys of the per-database counters that only ever grow while a database is open, in the order of the accumulator
   * array below. Read from {@link DatabaseInternal#getStats()}.
   */
  private static final String[] DB_STAT_KEYS = { "writeTx", "readTx", "txRollbacks", "createRecord", "readRecord",
      "updateRecord", "deleteRecord", "queries", "commands", "scanType", "scanBucket", "iterateType", "iterateBucket",
      "countType", "countBucket", "indexCompactions" };

  // Indexes into the per-database accumulator array. Entries [0, MONOTONIC_STATS) are the ones carried across a
  // database close (see retainedStats); the rest are instantaneous and are re-read from the open databases only.
  private static final int STAT_WRITE_TX          = 0;
  private static final int STAT_READ_TX           = 1;
  private static final int STAT_TX_ROLLBACKS      = 2;
  private static final int STAT_CREATE_RECORD     = 3;
  private static final int STAT_READ_RECORD       = 4;
  private static final int STAT_UPDATE_RECORD     = 5;
  private static final int STAT_DELETE_RECORD     = 6;
  private static final int STAT_QUERIES           = 7;
  private static final int STAT_COMMANDS          = 8;
  private static final int STAT_SCAN_TYPE         = 9;
  private static final int STAT_SCAN_BUCKET       = 10;
  private static final int STAT_ITERATE_TYPE      = 11;
  private static final int STAT_ITERATE_BUCKET    = 12;
  private static final int STAT_COUNT_TYPE        = 13;
  private static final int STAT_COUNT_BUCKET      = 14;
  private static final int STAT_INDEX_COMPACTIONS = 15;
  private static final int STAT_WAL_PAGES_WRITTEN = 16;
  private static final int STAT_WAL_BYTES_WRITTEN = 17;
  private static final int MONOTONIC_STATS        = 18;
  private static final int STAT_WAL_TOTAL_FILES   = 18;
  private static final int STAT_OPEN_FILES        = 19;
  private static final int STAT_MAX_OPEN_FILES    = 20;
  private static final int STAT_ASYNC_QUEUE       = 21;
  private static final int STAT_ASYNC_PARALLEL    = 22;
  private static final int STATS_COUNT            = 23;

  /**
   * Registered database INSTANCES, compared by identity.
   * <p>
   * Not a {@link LinkedHashSet}: {@code LocalDatabase.equals}/{@code hashCode} are derived from the database PATH, so
   * an equals-based set treats a closed instance and a freshly reopened one on the same path as the same element.
   * That breaks this class in both directions - {@link #registerDatabase} would silently no-op on the reopened
   * instance while a stale one was still present (its counters then never counted at all), and
   * {@link #unregisterDatabase} would fold the stale instance's counters a second time and evict the LIVE database
   * from the registry. What the profiler tracks is instances, so the set has to compare by identity. Iteration order
   * is now unspecified, which no reader depends on: every use is a sum or a size.
   */
  private final Set<DatabaseInternal> databases = Collections.newSetFromMap(new IdentityHashMap<>());

  /**
   * The monotonic contribution of every database that has been closed or dropped since JVM start.
   * <p>
   * #5636: summing only the currently registered databases made the JVM-wide totals go BACKWARDS on a close, and a
   * Prometheus counter that decreases is read as a counter <i>reset</i> - so every database close fabricated a rate()
   * spike on the next scrape, and Studio showed the query/transaction counters visibly drop. Folding the departing
   * database's counters in here keeps each exported total monotonic for the JVM's lifetime, which is what
   * {@code arcadedb.engine.*} being exported as Micrometer counters requires.
   */
  private final long[] retainedStats = new long[MONOTONIC_STATS];

  protected Profiler() {
  }

  public synchronized void registerDatabase(final DatabaseInternal database) {
    databases.add(database);
  }

  /**
   * Synchronized like the rest of the class: {@code databases} is a plain {@link LinkedHashSet}, so removing from it
   * while {@link #toJSON()} iterates it was a data race.
   * <p>
   * This does mean a close can now wait on an in-flight snapshot. That exposure is not new in kind - the paired
   * {@link #registerDatabase} on the open path has always had it - and every stat source read under the monitor is
   * lock-free, so no database lock can be on the other side of the wait.
   */
  public synchronized void unregisterDatabase(final DatabaseInternal database) {
    if (!databases.contains(database))
      // ALREADY UNREGISTERED: DO NOT FOLD ITS COUNTERS IN TWICE
      return;

    // CAPTURE BEFORE REMOVING, NOT AFTER. Removing first and folding second leaves a window in which a throw from
    // accumulateMonotonic drops the database out of `databases` with its contribution never folded into
    // retainedStats - so the next scrape is LOWER than the last, which a Prometheus counter reads as a reset and
    // turns into a fabricated rate() spike. That is the exact artifact this baseline exists to prevent, so the
    // ordering below closes the window rather than relying on the read not to fail.
    final long[] departing = new long[STATS_COUNT];
    boolean captured = false;
    try {
      accumulateMonotonic(departing, database);
      captured = true;
    } catch (final Exception e) {
      // THE CALLER IS MID-CLOSE: A STAT SOURCE ALREADY TORN DOWN MUST NOT TAKE THE CLOSE PATH DOWN WITH IT. The
      // database still has to leave the registry - keeping it would make every later toJSON() throw on the same
      // dead source - so this one case genuinely does decrease the totals, and on a typed counter that reads as a
      // reset. The loss is PERMANENT, not transient: this database's contribution never enters the baseline, so
      // every later total is lower by it forever. That is still monotonic - the totals only ever grow from there -
      // so it costs accuracy, not the counter contract. Logged at WARNING, not FINE: it is the only path that can
      // still produce the artifact, and an operator seeing an unexplained rate spike needs this line to explain it.
      LogManager.instance()
          .log(this, Level.WARNING, "Could not retain the profiler counters of a closing database: the engine "
              + "totals will step back by its contribution, which appears as a counter reset in Prometheus", e);
    }

    databases.remove(database);

    if (captured)
      for (int i = 0; i < MONOTONIC_STATS; i++)
        retainedStats[i] += departing[i];
  }

  /**
   * Sums the per-database counters of every open database on top of the retained baseline of the closed ones.
   * Shared by {@link #toJSON()} and {@link #dumpMetrics(PrintStream)} so the two cannot drift apart.
   * <p>
   * Deliberately NOT guarded per database, unlike the fold in {@link #unregisterDatabase}. Skipping a database whose
   * stat sources are mid-teardown would drop its contribution from this one snapshot and restore it on the next -
   * a transient DIP, which is exactly the counter-reset artifact this whole change exists to eliminate. Letting the
   * read throw instead surfaces as a failed scrape, which Prometheus already handles by carrying the last value
   * forward. A wrong number is worse than a missing one here.
   */
  private long[] collectDatabaseStats() {
    final long[] acc = new long[STATS_COUNT];
    System.arraycopy(retainedStats, 0, acc, 0, MONOTONIC_STATS);

    for (final DatabaseInternal db : databases) {
      // The WAL map comes back from the fold rather than being re-read: TransactionManager.getStats() allocates a
      // fresh HashMap and walks the active WAL pool on every call, and logFiles lives in the same map as the two
      // monotonic WAL counters.
      final Map<String, Object> walStats = accumulateMonotonic(acc, db);
      acc[STAT_WAL_TOTAL_FILES] += statOf(walStats, "logFiles");

      final FileManager.FileManagerStats fStats = db.getFileManager().getStats();
      acc[STAT_OPEN_FILES] += fStats.totalOpenFiles;
      acc[STAT_MAX_OPEN_FILES] += fStats.maxOpenFiles;

      acc[STAT_ASYNC_QUEUE] += ((DatabaseAsyncExecutorImpl) db.async()).getStats().queueSize;
      acc[STAT_ASYNC_PARALLEL] = db.async().getParallelLevel();
    }
    return acc;
  }

  /**
   * Adds one database's monotonic counters into {@code acc}. Deliberately touches only the two stat sources that stay
   * readable after a close ({@link DatabaseInternal#getStats()} reads a plain counter holder, and
   * {@code TransactionManager.getStats()} guards a retired WAL pool), so {@link #unregisterDatabase} can reuse it.
   *
   * @return the WAL stat map it read, so a caller that also needs the instantaneous {@code logFiles} count out of it
   *         does not pay for a second {@code TransactionManager.getStats()}. {@code logFiles} cannot join the fold
   *         itself - it is a current count, not a total, so carrying it across a close would inflate it forever.
   */
  private Map<String, Object> accumulateMonotonic(final long[] acc, final DatabaseInternal db) {
    final Map<String, Object> dbStats = db.getStats();
    for (int i = 0; i < DB_STAT_KEYS.length; i++)
      acc[i] += statOf(dbStats, DB_STAT_KEYS[i]);

    final Map<String, Object> walStats = db.getTransactionManager().getStats();
    acc[STAT_WAL_PAGES_WRITTEN] += statOf(walStats, "pagesWritten");
    acc[STAT_WAL_BYTES_WRITTEN] += statOf(walStats, "bytesWritten");
    return walStats;
  }

  /**
   * Reads one numeric out of a stat map. Every accumulated stat goes through here so none of them can be the one
   * written with a hard {@code (Long)} cast: {@link #collectDatabaseStats()} is called from {@link #toJSON()}, which
   * has no try/catch around it, so a stat source that ever changed its boxed type would take a metrics scrape down
   * rather than misreport a number.
   */
  private static long statOf(final Map<String, Object> stats, final String key) {
    final Object value = stats.get(key);
    return value instanceof Number n ? n.longValue() : 0L;
  }

  public synchronized JSONObject toJSON() {
    final JSONObject json = new JSONObject();

    final long[] dbStats = collectDatabaseStats();
    final long totalOpenFiles = dbStats[STAT_OPEN_FILES];
    final long maxOpenFiles = dbStats[STAT_MAX_OPEN_FILES];
    final long walPagesWritten = dbStats[STAT_WAL_PAGES_WRITTEN];
    final long walBytesWritten = dbStats[STAT_WAL_BYTES_WRITTEN];
    final long walTotalFiles = dbStats[STAT_WAL_TOTAL_FILES];
    final long asyncQueueLength = dbStats[STAT_ASYNC_QUEUE];
    final long asyncParallelLevel = dbStats[STAT_ASYNC_PARALLEL];

    final long writeTx = dbStats[STAT_WRITE_TX];
    final long readTx = dbStats[STAT_READ_TX];
    final long txRollbacks = dbStats[STAT_TX_ROLLBACKS];
    final long createRecord = dbStats[STAT_CREATE_RECORD];
    final long readRecord = dbStats[STAT_READ_RECORD];
    final long updateRecord = dbStats[STAT_UPDATE_RECORD];
    final long deleteRecord = dbStats[STAT_DELETE_RECORD];
    final long queries = dbStats[STAT_QUERIES];
    final long commands = dbStats[STAT_COMMANDS];
    final long scanType = dbStats[STAT_SCAN_TYPE];
    final long scanBucket = dbStats[STAT_SCAN_BUCKET];
    final long iterateType = dbStats[STAT_ITERATE_TYPE];
    final long iterateBucket = dbStats[STAT_ITERATE_BUCKET];
    final long countType = dbStats[STAT_COUNT_TYPE];
    final long countBucket = dbStats[STAT_COUNT_BUCKET];
    final long indexCompactions = dbStats[STAT_INDEX_COMPACTIONS];

    // PageManager is a JVM-wide singleton; counters are global, not per-DB.
    // Reading them once outside the loop avoids multiplying by databases.size().
    final PageManager.PPageManagerStats pStats = PageManager.INSTANCE.getStats();
    final long readCacheUsed = pStats.readCacheRAM;
    final long cacheMax = pStats.maxRAM;
    final long pagesRead = pStats.pagesRead;
    final long pagesReadSize = pStats.pagesReadSize;
    final long pagesWritten = pStats.pagesWritten;
    final long pagesWrittenSize = pStats.pagesWrittenSize;
    final int pageFlushQueueLength = pStats.pageFlushQueueLength;
    final long pageCacheHits = pStats.cacheHits;
    final long pageCacheMiss = pStats.cacheMiss;
    final long concurrentModificationExceptions = pStats.concurrentModificationExceptions;
    final long edgeAppendMerges = pStats.edgeAppendMerges;
    final long txPageSlotMerges = pStats.txPageSlotMerges;
    final long mergesDeclinedByCoverage = pStats.mergesDeclinedByCoverage;
    final long evictionRuns = pStats.evictionRuns;
    final long pagesEvicted = pStats.pagesEvicted;
    final int readCachePages = pStats.readCachePages;
    final int writeCachePages = 0;

    json.put("readCacheUsed", new JSONObject().put("space", readCacheUsed));
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
    // #5608: the three page-merge counters travel WITH concurrentModificationExceptions on purpose - a merge that
    // stops firing shows up as conflicts here and declines there, and neither half means anything alone.
    json.put("edgeAppendMerges", new JSONObject().put("count", edgeAppendMerges));
    json.put("txPageSlotMerges", new JSONObject().put("count", txPageSlotMerges));
    json.put("mergesDeclinedByCoverage", new JSONObject().put("count", mergesDeclinedByCoverage));

    json.put("writeTx", new JSONObject().put("count", writeTx));
    json.put("readTx", new JSONObject().put("count", readTx));
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
        new JSONObject().put("perc",
            (runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 100F / runtime.maxMemory()));

    try {
      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final ObjectName osMBeanName = ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);

      if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
        final long osTotalMem = ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
        final long osUsedMem = osTotalMem - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

        json.put("ramOsUsed", new JSONObject().put("space", osUsedMem));
        json.put("ramOsTotal", new JSONObject().put("space", osTotalMem));

        final double cpuLoad = ManagementFactory.getPlatformMXBean(
            OperatingSystemMXBean.class).getCpuLoad();
        json.put("cpuLoad", new JSONObject().put("perc", cpuLoad * 100));
      }

      final sun.management.HotspotRuntimeMBean hotSpotRunTime = sun.management.ManagementFactoryHelper.getHotspotRuntimeMBean();
      json.put("jvmSafePointTime", new JSONObject().put("value", hotSpotRunTime.getTotalSafepointTime()));
      json.put("jvmSafePointCount", new JSONObject().put("count", hotSpotRunTime.getSafepointCount()));
      json.put("jvmAvgSafePointTime",
          new JSONObject().put("value", hotSpotRunTime.getTotalSafepointTime() / (float) hotSpotRunTime.getSafepointCount()));

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

    try {
      final long[] dbStats = collectDatabaseStats();
      final long asyncQueueLength = dbStats[STAT_ASYNC_QUEUE];
      final long asyncParallelLevel = dbStats[STAT_ASYNC_PARALLEL];
      final long totalOpenFiles = dbStats[STAT_OPEN_FILES];
      final long maxOpenFiles = dbStats[STAT_MAX_OPEN_FILES];
      final long walPagesWritten = dbStats[STAT_WAL_PAGES_WRITTEN];
      final long walBytesWritten = dbStats[STAT_WAL_BYTES_WRITTEN];
      final long walTotalFiles = dbStats[STAT_WAL_TOTAL_FILES];

      final long writeTx = dbStats[STAT_WRITE_TX];
      final long readTx = dbStats[STAT_READ_TX];
      final long txRollbacks = dbStats[STAT_TX_ROLLBACKS];
      final long createRecord = dbStats[STAT_CREATE_RECORD];
      final long readRecord = dbStats[STAT_READ_RECORD];
      final long updateRecord = dbStats[STAT_UPDATE_RECORD];
      final long deleteRecord = dbStats[STAT_DELETE_RECORD];
      final long queries = dbStats[STAT_QUERIES];
      final long commands = dbStats[STAT_COMMANDS];
      final long scanType = dbStats[STAT_SCAN_TYPE];
      final long scanBucket = dbStats[STAT_SCAN_BUCKET];
      final long iterateType = dbStats[STAT_ITERATE_TYPE];
      final long iterateBucket = dbStats[STAT_ITERATE_BUCKET];
      final long countType = dbStats[STAT_COUNT_TYPE];
      final long countBucket = dbStats[STAT_COUNT_BUCKET];
      final long indexCompactions = dbStats[STAT_INDEX_COMPACTIONS];

      // PageManager is a JVM-wide singleton; read once, not per-DB.
      final PageManager.PPageManagerStats pStats = PageManager.INSTANCE.getStats();
      final long readCacheUsed = pStats.readCacheRAM;
      final long cacheMax = pStats.maxRAM;
      final long pagesRead = pStats.pagesRead;
      final long pagesReadSize = pStats.pagesReadSize;
      final long pagesWritten = pStats.pagesWritten;
      final long pagesWrittenSize = pStats.pagesWrittenSize;
      final int pageFlushQueueLength = pStats.pageFlushQueueLength;
      final long pageCacheHits = pStats.cacheHits;
      final long pageCacheMiss = pStats.cacheMiss;
      final long concurrentModificationExceptions = pStats.concurrentModificationExceptions;
      final long edgeAppendMerges = pStats.edgeAppendMerges;
      final long txPageSlotMerges = pStats.txPageSlotMerges;
      final long mergesDeclinedByCoverage = pStats.mergesDeclinedByCoverage;
      final long evictionRuns = pStats.evictionRuns;
      final long pagesEvicted = pStats.pagesEvicted;
      final int readCachePages = pStats.readCachePages;

      buffer.append("ARCADEDB %s Profiler".formatted(Constants.getRawVersion()));

      final Runtime runtime = Runtime.getRuntime();

      final long gcTime = getGarbageCollectionTime();

      boolean dumpWithJmx = false;
      try {
        final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        final ObjectName osMBeanName = ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);

        if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
          final long osTotalMem = ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
          final long osUsedMem = osTotalMem - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

          buffer.append("%n JVM heap=%s/%s os=%s/%s gc=%dms".formatted(
            FileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
            FileUtils.getSizeAsString(runtime.maxMemory()), FileUtils.getSizeAsString(osUsedMem),
            FileUtils.getSizeAsString(osTotalMem), gcTime));

          dumpWithJmx = true;
        }
      } catch (final Exception e) {
        // JMX NOT AVAILABLE, AVOID OS DATA
      }

      if (!dumpWithJmx)
        buffer.append(
          "%n JVM heap=%s/%s gc=%dms".formatted(FileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
            FileUtils.getSizeAsString(runtime.maxMemory()), gcTime));

      buffer.append("%n PAGE-CACHE read=%s (pages=%d) max=%s readOps=%d (%s) writeOps=%d (%s)".formatted(
        FileUtils.getSizeAsString(readCacheUsed), readCachePages,
        FileUtils.getSizeAsString(cacheMax), pagesRead, FileUtils.getSizeAsString(pagesReadSize), pagesWritten,
        FileUtils.getSizeAsString(pagesWrittenSize)));

      buffer.append(
        "%n DB databases=%d asyncParallelLevel=%d asyncQueue=%d writeTx=%d readTx=%d txRollbacks=%d queries=%d commands=%d".formatted(
          databases.size(),
          asyncParallelLevel, asyncQueueLength, writeTx, readTx, txRollbacks, queries, commands));
      buffer.append("%n    createRecord=%d readRecord=%d updateRecord=%d deleteRecord=%d".formatted(createRecord, readRecord,
        updateRecord, deleteRecord));
      buffer.append(
        "%n    scanType=%d scanBucket=%d iterateType=%d iterateBucket=%d countType=%d countBucket=%d".formatted(scanType,
          scanBucket, iterateType,
          iterateBucket, countType, countBucket));

      buffer.append("%n INDEXES compactions=%d".formatted(indexCompactions));

      buffer.append(
        "%n PAGE-MANAGER flushQueue=%d cacheHits=%d cacheMiss=%d concModExceptions=%d evictionRuns=%d pagesEvicted=%d".formatted(
          pageFlushQueueLength,
          pageCacheHits, pageCacheMiss, concurrentModificationExceptions, evictionRuns, pagesEvicted));

      // #5608: read this line together with concModExceptions above. Contention absorbed by a merge never becomes a
      // retry; a jump in mergesDeclined with a dip in the two merge counters is a writer dirtying a mergeable page
      // without declaring its coverage (see MutablePage.beginCoveredWrite).
      buffer.append("%n    edgeAppendMerges=%d txPageSlotMerges=%d mergesDeclinedByCoverage=%d".formatted(
          edgeAppendMerges, txPageSlotMerges, mergesDeclinedByCoverage));

      buffer.append(
        "%n WAL totalFiles=%d pagesWritten=%d bytesWritten=%s".formatted(walTotalFiles, walPagesWritten,
          FileUtils.getSizeAsString(walBytesWritten)));

      buffer.append(
        "%n FILE-MANAGER FS=%s/%s openFiles=%d maxFilesOpened=%d".formatted(FileUtils.getSizeAsString(freeSpaceInMB),
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
