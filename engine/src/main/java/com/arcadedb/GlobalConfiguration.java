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

import com.arcadedb.database.Database;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IPAddressBlocklist;
import com.arcadedb.utility.SystemVariableResolver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 */
public enum GlobalConfiguration {
  // ENVIRONMENT
  DUMP_CONFIG_AT_STARTUP("arcadedb.dumpConfigAtStartup", SCOPE.JVM, "Dumps the configuration at startup", Boolean.class, false,
      value -> {
        //dumpConfiguration(System.out);

        try {
          final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          dumpConfiguration(new PrintStream(buffer));
          if (LogManager.instance() != null)
            LogManager.instance().log(buffer, Level.WARNING, new String(buffer.toByteArray()));
          else
            System.out.println(new String(buffer.toByteArray()));

          buffer.close();
        } catch (IOException e) {
          System.out.println("Error on printing initial configuration to log (error=" + e + ")");
        }

        return value;
      }),

  DUMP_METRICS_EVERY("arcadedb.dumpMetricsEvery", SCOPE.JVM,
      "Dumps the metrics at startup, shutdown and every configurable amount of time (in seconds)", Long.class, 0, new Callable<>() {
    @Override
    public Object call(final Object value) {
      final long time = (long) value * 1000;
      if (time > 0) {
        Profiler.INSTANCE.dumpMetrics(System.out);

        TIMER.schedule(new TimerTask() {
          @Override
          public void run() {
            Profiler.INSTANCE.dumpMetrics(System.out);
          }
        }, time, time);
      }
      return value;
    }
  }),

  PROFILE("arcadedb.profile", SCOPE.JVM, "Specify the preferred profile among: default, high-performance, low-ram, low-cpu",
      String.class, "default", new Callable<>() {
    @Override
    public Object call(final Object value) {
      final int cores = Runtime.getRuntime().availableProcessors();

      final String v = value.toString();
      if ("default".equalsIgnoreCase(v)) {
        // NOT MUCH TO DO HERE, THIS IS THE DEFAULT OPTION
      } else if ("high-performance".equalsIgnoreCase(v)) {
        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("fast");
        VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(-1);
        VECTOR_INDEX_SEARCH_CACHE_MAX_HEAP_PERCENT.setValue(50);
        VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT.setValue(50);

        if (cores > 1)
          // USE ONLY HALF OF THE CORES MINUS ONE
          ASYNC_WORKER_THREADS.setValue((cores / 2) - 1);
        else
          ASYNC_WORKER_THREADS.setValue(1);

      } else if ("low-ram".equalsIgnoreCase(v)) {
        MAX_PAGE_RAM.setValue(16); // 16 MB OF RAM FOR PAGE CACHE
        INDEX_COMPACTION_RAM_MB.setValue(16);
        INITIAL_PAGE_CACHE_SIZE.setValue(256);
        FREE_PAGE_RAM.setValue(80);
        ASYNC_OPERATIONS_QUEUE_SIZE.setValue(8);
        ASYNC_TX_BATCH_SIZE.setValue(8);
        PAGE_FLUSH_QUEUE.setValue(8);
        SQL_STATEMENT_CACHE.setValue(16);
        OPENCYPHER_STATEMENT_CACHE.setValue(16);
        OPENCYPHER_PLAN_CACHE.setValue(16);

        ASYNC_WORKER_THREADS.setValue(1);
        TX_WAL_FILES.setValue(1);

        QUERY_PARALLELISM_POOL_THREADS.setValue(2);
        QUERY_PARALLELISM_QUEUE_SIZE.setValue(64);
        SPARSE_VECTOR_SCORING_POOL_THREADS.setValue(1);
        SPARSE_VECTOR_SCORING_QUEUE_SIZE.setValue(64);

        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("standard");
        SERVER_HTTP_IO_THREADS.setValue(cores > 8 ? 4 : 2);
        SERVER_HTTP_WORKER_THREADS.setValue(16);
        VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(10_000);
        // VECTOR_INDEX_LOCATION_CACHE_SIZE is deliberately NOT capped here: it is not a cache, and bounding it
        // made this profile drop live vectors from searches (issue #5568).
        VECTOR_INDEX_SEARCH_CACHE_SIZE.setValue(10_000);

        POLYGLOT_ENGINE_ENABLED.setValue(false);

        PageManager.INSTANCE.configure();

      } else if ("low-cpu".equalsIgnoreCase(v)) {
        ASYNC_WORKER_THREADS.setValue(1);
        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("standard");
        SERVER_HTTP_IO_THREADS.setValue(cores > 8 ? 4 : 2);
      } else
        throw new IllegalArgumentException("Profile '" + v + "' not available");

      return value;
    }
  }, null, Set.of("default", "high-performance", "low-ram", "low-cpu")),

  TEST("arcadedb.test", SCOPE.JVM,
      "Tells if it is running in test mode. This enables the calling of callbacks for testing purpose", Boolean.class, false),

  // UNUSUAL AMONG THE SETTINGS IN THAT IT INSTALLS SOMETHING. reset() RUNS NO CALLBACK, SO IT RESTORES THE VALUE BUT
  // LEAVES THE LAST INSTALLED LOGGER IN PLACE: A CALLER THAT WANTS THE PREVIOUS ONE BACK KEEPS LogManager.getLogger()
  LOG_IMPL("arcadedb.log.impl", SCOPE.JVM,
      "Logger implementation: 'default' uses java.util.logging, 'slf4j' routes the logs through the SLF4J facade so an embedding application receives them in its own backend. An unrecognized value is reported and falls back to 'default'",
      String.class, "default", value -> {
    // STORE THE SPELLING createLogger() MATCHES ON, SO dumpConfiguration() AND toJSON() DO NOT REPORT 'SLF4J' OR
    // ' slf4j '. AN UNRECOGNIZED VALUE IS KEPT VERBATIM: IT FALLS BACK TO 'default', BUT REWRITING IT WOULD HIDE THE TYPO
    final String impl = value == null || value.toString().isBlank() ?
        "default" :
        value.toString().trim().toLowerCase(Locale.ROOT); // SAME LOCALE createLogger() NORMALIZES WITH

    final LogManager logManager = LogManager.instance();
    // NULL ONLY IF THIS RUNS RE-ENTRANTLY FROM THE LOG MANAGER'S STATIC INITIALIZER, WHICH READS THE SYSTEM PROPERTY ON ITS OWN
    if (logManager != null)
      logManager.setLogger(LogManager.createLogger(impl));

    return impl;
  }),

  MAX_PAGE_RAM("arcadedb.maxPageRAM", SCOPE.DATABASE, "Maximum amount of pages (in MB) to keep in RAM", Long.class, 4 * 1024, // 4GB
      new Callable<>() {
        @Override
        public Object call(final Object value) {
          final long maxRAM = ((long) value) * 1024 * 1024; // VALUE IN MB

          if (maxRAM > Runtime.getRuntime().maxMemory() * 80 / 100) {
            final long newValueBytes = Runtime.getRuntime().maxMemory() / 2;
            final long newValueMB = newValueBytes / 1024 / 1024;
            if (LogManager.instance() != null)
              LogManager.instance()
                  .log(this, Level.WARNING, "Setting '%s=%s' is > than 80%% of maximum heap (%s). Decreasing it to %s",
                      MAX_PAGE_RAM.key, FileUtils.getSizeAsString(maxRAM),
                      FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), FileUtils.getSizeAsString(newValueBytes));
            else
              System.out.println(
                  "Setting '%s=%s' is > than 80%% of maximum heap (%s). Decreasing it to %s".formatted(MAX_PAGE_RAM.key,
                      FileUtils.getSizeAsString(maxRAM), FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()),
                      FileUtils.getSizeAsString(newValueBytes)));

            return newValueMB;
          }
          return value;
        }
      }, value -> Runtime.getRuntime().maxMemory() / 4 / 1024 / 1024),

  INITIAL_PAGE_CACHE_SIZE("arcadedb.initialPageCacheSize", SCOPE.DATABASE, "Initial number of entries for page cache",
      Integer.class, 65535),

  DATE_IMPLEMENTATION("arcadedb.dateImplementation", SCOPE.DATABASE,
      "Default date implementation to use on deserialization. By default java.time.LocalDate is used, but the following are supported: java.util.Date, java.util.Calendar, java.time.LocalDate",
      Class.class, LocalDate.class, value -> {
    if (value instanceof String string) {
      try {
        return Class.forName(string);
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException("Date implementation '" + value + "' not found", e);
      }
    }
    return value;
  }),

  DATE_FORMAT("arcadedb.dateFormat", SCOPE.DATABASE, "Default date format using Java SimpleDateFormat syntax", String.class,
      "yyyy-MM-dd"),

  DATE_TIME_IMPLEMENTATION("arcadedb.dateTimeImplementation", SCOPE.DATABASE,
      "Default datetime implementation to use on deserialization. By default java.time.LocalDateTime is used, but the following are supported: java.util.Date, java.util.Calendar, java.time.LocalDateTime, java.time.ZonedDateTime, java.time.Instant",
      Class.class, LocalDateTime.class, value -> {
    if (value instanceof String string) {
      try {
        return Class.forName(string);
      } catch (ClassNotFoundException e) {
        throw new ConfigurationException("Date implementation '" + value + "' not found", e);
      }
    }
    return value;
  }),

  DATE_TIME_FORMAT("arcadedb.dateTimeFormat", SCOPE.DATABASE, "Default date time format using Java SimpleDateFormat syntax",
      String.class, "yyyy-MM-dd HH:mm:ss"),

  TX_WAL("arcadedb.txWAL", SCOPE.DATABASE, "Uses the WAL", Boolean.class, true),

  TX_WAL_FLUSH("arcadedb.txWalFlush", SCOPE.DATABASE,
      "Flushes the WAL on disk at commit time. It can be 0 = no flush, 1 = flush without metadata and 2 = full flush (fsync)",
      Integer.class, 0),

  TX_WAL_FILES("arcadedb.txWalFiles", SCOPE.DATABASE,
      "Number of concurrent files to use for tx log. 0 (default) = available cores", Integer.class,
      Math.max(Runtime.getRuntime().availableProcessors(), 1)),

  FREE_PAGE_RAM("arcadedb.freePageRAM", SCOPE.DATABASE, "Percentage (0-100) of memory to free when Page RAM is full", Integer.class,
      50),

  TYPE_DEFAULT_BUCKETS("arcadedb.typeDefaultBuckets", SCOPE.DATABASE, "Default number of buckets to create per type", Integer.class,
      1),

  BUCKET_DEFAULT_PAGE_SIZE("arcadedb.bucketDefaultPageSize", SCOPE.DATABASE,
      "Default page size in bytes for buckets. Default is 64KB", Integer.class, 65_536),

  EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE("arcadedb.externalPropertyBucketDefaultPageSize", SCOPE.DATABASE,
      "Default page size in bytes for paired external-property buckets. They hold heavy property payloads (vector embeddings, large strings, embedded JSON) so the default is larger than for primary buckets to reduce multi-page chunking. Matches the LSM-index default (256KB)",
      Integer.class, 262_144),

  EXTERNAL_PROPERTY_BUCKET_PATH("arcadedb.externalPropertyBucketPath", SCOPE.DATABASE,
      "Filesystem directory where new paired external-property buckets are created. If empty (default), external buckets sit alongside primary buckets in the database directory. Set to a path on cheaper/slower storage (HDD, network mount) to tier the heavy payloads away from the topology files. The directory must exist and be writable. Existing external buckets are not relocated when this changes.",
      String.class, ""),

  TIMESERIES_TAG_DICTIONARY_MAX_SIZE("arcadedb.timeSeriesTagDictionaryMaxSize", SCOPE.DATABASE,
      "Maximum number of distinct values one TimeSeries type's tag dictionary may hold. TAG columns are dictionary-encoded in the mutable row so each occupies a 4-byte id instead of a reserved 258-byte slot; the dictionary is kept in RAM, so this caps its footprint and turns a mis-declared high-cardinality TAG into a clear error instead of unbounded growth. Default is 1M distinct values, roughly 100MB",
      Integer.class, 1_000_000),

  BUCKET_REUSE_SPACE_MODE("arcadedb.bucketReuseSpaceMode", SCOPE.DATABASE,
      "How to reuse space in pages. 'high' = more space saved, but slower opening and update/delete time. 'medium' to still reuse space without the initial scan at opening time. 'low' for faster performance, but less space reused. Default is 'high'",
      String.class, "high", Set.of("low", "medium", "high")),

  BUCKET_WIPEOUT_ONDELETE("arcadedb.bucketWipeOutOnDelete", SCOPE.DATABASE,
      "Wipe out record content on delete. If enabled, assures deleted records cannot be analyzed by parsing the raw files and backups will be more compressed, but it also makes deletes a little bit slower",
      Boolean.class, true),

  ASYNC_WORKER_THREADS("arcadedb.asyncWorkerThreads", SCOPE.DATABASE,
      "Number of asynchronous worker threads. 0 (default) = available cores minus 1", Integer.class,
      Runtime.getRuntime().availableProcessors() > 1 ? Runtime.getRuntime().availableProcessors() - 1 : 1),

  QUERY_PARALLELISM_POOL_THREADS("arcadedb.queryParallelismPoolThreads", SCOPE.JVM,
      """
      Maximum number of threads in the JVM-wide pool that backs query-time parallelism \
      (graph algorithms parallelForRange, parallel index scans, etc.). The same pool also \
      serves any future feature that wants to fork query work; sizing it explicitly is the \
      alternative to the JDK common ForkJoinPool, which is shared with user code and has no \
      back-pressure. 0 = available cores (min 2)""",
      Integer.class, 0),

  PARALLEL_SCAN_PRODUCER_POOL_THREADS("arcadedb.parallelScanProducerPoolThreads", SCOPE.JVM,
      """
      Maximum number of threads in the JVM-wide pool that runs the (blocking) producer tasks of \
      parallel bucket scans. Kept separate from the query-parallelism pool because scan producers \
      block on each query's bounded result queue and would starve non-blocking compute work. \
      0 = available cores (min 2); consider capping explicitly on very high core-count machines""",
      Integer.class, 0),

  FLUSH_ALL_PAGES_TIMEOUT("arcadedb.flushAllPagesTimeout", SCOPE.DATABASE,
      """
      Milliseconds of NO FLUSH PROGRESS after which waiting for all of a database's pages to reach the \
      disk (on close, rename, backup-suspend) gives up with a SEVERE log instead of hanging forever on a \
      wedged flush. The window resets whenever the pending-page count decreases, so a healthy but slow \
      backlog never trips it. NOTE: the window is per-database no-progress time, not total flush time - \
      on a heavily loaded multi-database server a database starved by its siblings can give up, turning \
      that close into recovery-on-next-open. A close that gives up preserves the WAL files and the lock \
      file, so the next open runs recovery and replays the unflushed pages. 0 waits forever \
      (pre-26.7.2 behavior)""",
      Long.class, 60_000L),

  ASYNC_CLOSE_TIMEOUT("arcadedb.asyncCloseTimeout", SCOPE.DATABASE,
      """
      Milliseconds to wait for in-flight asynchronous tasks to drain when closing or dropping a database \
      before giving up with a WARNING and forcing the async workers down. Without a bound, a worker wedged \
      inside a user task or callback made close()/drop() hang forever (#5080). Giving up here is safe: the \
      forced shutdown interrupts the workers and notifies completion, and any task that never ran is simply \
      not applied. 0 waits forever (pre-26.7.2 behavior)""",
      Long.class, 60_000L),

  PARALLEL_SCAN_ABANDONED_TIMEOUT("arcadedb.parallelScanAbandonedTimeout", SCOPE.DATABASE,
      """
      Milliseconds a parallel-scan producer keeps waiting on a full result queue with NO consumer \
      activity before declaring the ResultSet abandoned: it then frees its pool thread and the query \
      fails on the next access instead of silently returning fewer rows. Raise it for workloads that \
      hold cursors open with long idle pauses (e.g. Postgres/Bolt wire portals); 0 disables the \
      timeout entirely (producers park until the ResultSet is closed)""",
      Long.class, 600_000L),

  QUERY_PARALLELISM_QUEUE_SIZE("arcadedb.queryParallelismQueueSize", SCOPE.JVM,
      """
      Maximum number of tasks that can wait in the QueryEngineManager pool's queue before the \
      rejection policy fires. The default of 1024 lets bursts (e.g. dozens of concurrent graph \
      algorithms forking thousands of chunks) absorb gracefully, while still bounding heap \
      usage if a runaway producer overwhelms the workers. Once the queue is full, the \
      rejection policy is CallerRuns: the submitter executes the task inline, which degrades \
      parallelism but never fails the query.""",
      Integer.class, 1024),

  SPARSE_VECTOR_SCORING_POOL_THREADS("arcadedb.sparseVectorScoringPoolThreads", SCOPE.JVM,
      """
      Maximum number of threads in the JVM-wide pool that backs LSM_SPARSE_VECTOR top-K \
      fan-out: per-bucket parallel scoring on partitioned types and types with multiple \
      buckets, and the RID-range split of a single index's traversal (see \
      arcadedb.sparseVectorScoringMaxPartitions). Kept on its own pool rather than sharing \
      the QueryEngineManager pool so \
      long-running graph algorithms never queue scoring tasks behind seconds-long graph \
      chunks. 0 = available cores (min 2). REQUIRES JVM RESTART: the pool is a lazy \
      singleton constructed once on first use; later changes to this value have no effect \
      until the JVM restarts.""",
      Integer.class, 0),

  SPARSE_VECTOR_SCORING_QUEUE_SIZE("arcadedb.sparseVectorScoringQueueSize", SCOPE.JVM,
      """
      Maximum number of tasks that can wait in the sparse-vector scoring pool's queue before \
      the CallerRuns rejection policy fires. Scoring fan-out is fine-grained (per-bucket \
      topK calls), so the default of 1024 covers a wide range of workloads. Once the \
      queue is full, the submitter executes the task inline, which degrades parallelism \
      but never fails the query. REQUIRES JVM RESTART: same singleton lifecycle as \
      SPARSE_VECTOR_SCORING_POOL_THREADS.""",
      Integer.class, 1024),

  SPARSE_VECTOR_SCORING_MAX_PARTITIONS("arcadedb.sparseVectorScoringMaxPartitions", SCOPE.JVM,
      """
      Maximum number of RID ranges a single LSM_SPARSE_VECTOR top-K query is split into for \
      parallel scoring. 1 disables intra-query parallelism and keeps every query on the caller \
      thread; 0 (default) lets the engine decide per query. Splitting buys latency with CPU: each \
      range prunes against its own top-K watermark, which rises more slowly than the global one, \
      so a range does more work than its share (measured at roughly 1.9x total CPU for an 8-way \
      split on a learned-sparse corpus). On the default the engine claims only workers no other \
      query needs, and does not split at all once enough queries are already in flight to keep \
      the pool busy on their own - so an idle server spends spare cores on latency while a \
      saturated one keeps its throughput. One cost of the default worth knowing: in a narrow band \
      of moderate concurrency, where some queries claim a wide split and others get none, the \
      spread between the two shows up as tail latency. Measured at 4 concurrent clients on an \
      18-thread pool, p99 was about 1.2x serial's while the median was 3.4x better (7.6 ms against \
      26.2). The band closes on both sides - below it every query splits, above it none does - and \
      the median win is large enough that the default keeps the trade rather than flattening it. Measured on an 18-worker box at 500k documents: one \
      client 13.7 -> 3.1 ms p50, four clients 13.6 -> 4.1 ms with 61% more throughput, sixteen \
      clients within 5% of serial throughput. Any explicit value above 1 opts out of that \
      self-throttling and splits regardless of load. That is a throughput risk on a busy server, \
      and it is paid by the forcing query too. Measured at 16 concurrent clients on an 18-thread \
      pool, 1M documents: a forced 8-way split returned 0.52x the throughput of no split at all, a \
      median 1.85x worse and a p99 2.5x worse, for 1.9x the CPU per query. Above light concurrency \
      there is no regime where forcing wins - not median, not throughput, not tail - because a \
      query's own ranges start queueing behind its neighbours'. It is worth setting only when a \
      single query at a time must be as fast as possible and nothing else is running; for anything \
      else 0 is faster on every measure. The crossover is hardware and workload specific, so no \
      number for it is given here. Re-read on every query.""",
      Integer.class, 0),

  SPARSE_VECTOR_SCORING_MIN_POSTINGS_FOR_PARTITIONING("arcadedb.sparseVectorScoringMinPostingsForPartitioning", SCOPE.JVM,
      """
      Minimum number of postings a LSM_SPARSE_VECTOR top-K query has to traverse before it is \
      worth splitting into parallel ranges. Below this the fan-out (per-range cursor stacks, task \
      dispatch, result merge) costs more than the traversal it parallelises, so the query stays on \
      the caller thread. Counted as the summed document frequency of the query's dims, which is \
      available from segment metadata without reading a page. Re-read on every query.""",
      Long.class, 200_000L),

  SPARSE_VECTOR_SCORING_TIMEOUT_SECONDS("arcadedb.sparseVectorScoringTimeoutSeconds", SCOPE.JVM,
      """
      Wall-clock deadline for the parallel sparse-vector top-K fan-out. Computed once before \
      the drain loop and shared across all per-bucket futures, so the worst case for N \
      wedged buckets is a single timeoutSeconds (not N * timeoutSeconds). On expiry every \
      still-pending future is cancelled and the query fails with a descriptive error. \
      Catches the case where a bucket's index is stuck on a write lock during compaction, \
      an HA replication race wedged a segment open, or a JVM-level pause stalled the worker \
      thread. Set to 0 to disable the timeout (caller will block indefinitely; not \
      recommended for production). Re-read on every query, so changes take effect without \
      restart (unlike the pool sizing knobs above). Minimum recommended value: 5 seconds. \
      Very short configured timeouts (e.g. 1-2s for integration tests) can produce \
      spurious failures on a saturated host - a JVM GC pause or OS scheduling delay \
      between deadline computation and the first future drain can consume the whole budget \
      before any work runs.""",
      Integer.class, 30),

  ASYNC_OPERATIONS_QUEUE_IMPL("arcadedb.asyncOperationsQueueImpl", SCOPE.DATABASE,
      "Queue implementation to use between 'standard' and 'fast'. 'standard' consumes less CPU than the 'fast' implementation, but it could be slower with high loads",
      String.class, "standard", Set.of("standard", "fast")),

  ASYNC_OPERATIONS_QUEUE_SIZE("arcadedb.asyncOperationsQueueSize", SCOPE.DATABASE,
      "Size of the total asynchronous operation queues (it is divided by the number of parallel threads in the pool)",
      Integer.class, 1024),

  ASYNC_TX_BATCH_SIZE("arcadedb.asyncTxBatchSize", SCOPE.DATABASE,
      "Maximum number of operations to commit in batch by async thread", Integer.class, 1024 * 10),

  ASYNC_COMMAND_POOL_THREADS("arcadedb.asyncCommandPoolThreads", SCOPE.JVM,
      """
      Maximum number of threads in the JVM-wide pool that runs the DDL dispatched through the asynchronous API - \
      notably a CREATE INDEX or REBUILD INDEX sent over HTTP POST /command with awaitResponse=false. Only DDL is \
      routed here, because only DDL cannot run on a per-database async worker: it has to quiesce those very workers \
      to scan the data, which cannot be done from one of them. Everything else keeps running on \
      arcadedb.asyncWorkerThreads. 0 = available cores (min 2)""",
      Integer.class, 0),

  ASYNC_COMMAND_QUEUE_SIZE("arcadedb.asyncCommandQueueSize", SCOPE.JVM,
      """
      Size of the bounded queue in front of the asynchronous DDL pool. When it is full the statement runs on the \
      submitting thread instead of being refused, so a client that asked not to wait may end up waiting; the \
      pool=async_command caller-runs gauge is where that shows up. 0 = 1024""",
      Integer.class, 1024),

  REBUILD_REPARTITION_MAX_BUFFERED_RIDS("arcadedb.rebuild.repartition.maxBufferedRids", SCOPE.DATABASE,
      """
      Maximum number of misplaced RIDs the REBUILD TYPE WITH repartition = true command may buffer in heap \
      before refusing to continue. The scan must capture every misplaced RID before the move phase can run \
      (delete+insert during the scan would break iterator stability), so heap usage scales linearly with the \
      number of misplaced records. Each entry costs ~16 bytes (ArrayList overhead included), so the default \
      10M caps the buffer at ~160MB. If the cap is exceeded the command throws with an actionable error \
      pointing the operator at smaller-batch alternatives.""",
      Integer.class, 10_000_000),

  ASYNC_BACK_PRESSURE("arcadedb.asyncBackPressure", SCOPE.DATABASE,
      "When the asynchronous queue is full at a certain percentage, back pressure is applied", Integer.class, 0),

  TRUNCATE_BATCH_SIZE("arcadedb.truncateBatchSize", SCOPE.DATABASE,
      """
      Number of records TRUNCATE TYPE/BUCKET deletes per committed transaction. Each batch is committed as one \
      transaction, which in HA becomes one Raft log entry: keeping the batch small keeps that entry small so the \
      leader's per-follower append pipeline returns to sending heartbeats between batches instead of stalling on a \
      single multi-MB entry (issue #4817, which caused leader churn, an interrupted commit and a partial truncate). \
      Larger values reduce commit overhead on single-node setups at the cost of bigger transactions. Ignored when \
      TRUNCATE runs inside a transaction the caller opened: there the deletes belong to that transaction and are \
      committed by it, so a ROLLBACK puts every record back (issue #6220).""",
      Integer.class, 1000),

  CHECK_DATABASE_REPAIR_BATCH_PAGES("arcadedb.checkDatabaseRepairBatchPages", SCOPE.DATABASE,
      """
      Number of modified pages CHECK DATABASE ... FIX accumulates before committing its repair and opening the next \
      transaction. Same rationale as arcadedb.truncateBatchSize, and the same failure it avoids (issue #6128): the \
      repair of one type - every reconnected edge and every deleted record - used to be a SINGLE transaction, which \
      in HA is a SINGLE Raft log entry, and a transaction entry has no splitter the way a schema entry has had since \
      issue #4743. Above min(arcadedb.ha.appendBufferSize, arcadedb.ha.grpcMessageSizeMax) the entry is rejected with \
      ReplicatedEntryTooLargeException - not a NeedRetryException, so nothing retries it - and a repair that had run \
      for hours was rolled back whole. Counted in PAGES rather than records because pages are what the entry \
      contains: how many records a repair touched says nothing about how many distinct pages it dirtied. Sizing the \
      default of 256, without rounding the arithmetic in its own favour: 256 pages at the 64KB bucket default is \
      16MB of PAGES, half the 32MB appendBufferSize default, which is margin rather than comfort once the soft \
      ceiling below is taken into account. The entry itself is far smaller in practice - the WAL carries each \
      page's CHANGED RANGE rather than the whole page, and is compressed on top of that, so a repair reconnecting \
      1500 edges was measured well under 128KB - but that is a property of typical repairs, not a bound. If a \
      deployment raises the bucket page size or lowers appendBufferSize, re-check this against BOTH rather than \
      trusting the default. Raising it lowers commit overhead on an embedded database at the cost of bigger \
      transactions; 0 \
      disables batching entirely, restoring the all-or-nothing repair semantics of a single transaction. \
      ALSO BOUNDS CHECK DATABASE ... COMPRESS, whose per-transaction page count was a hardcoded 10 - one Raft round \
      trip per ten pages, which on a replicated database of any size does not finish; COMPRESS keeps that 10 when \
      this is set to 0, since one transaction over every page in the database helps nobody. \
      A SOFT ceiling, not a hard cap: the budget is checked between units of repair work, so a transaction can \
      exceed it by whatever the unit in flight dirties (reconnecting one very wide adjacency list, or a hub record \
      spanning several pages). Leave headroom when picking a value close to the replicated-entry limit.""",
      Integer.class, 256),

  CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES("arcadedb.checkDatabaseAdjacencyCacheEntries", SCOPE.DATABASE,
      """
      Maximum number of adjacency-list entries CHECK DATABASE keeps materialised while answering its back-reference \
      probes (issue #6062). The check asks, once per edge, whether the NEIGHBOUR's edge list names that edge \
      (checkEdges) and whether the far vertex of every adjacency entry points back (checkVertices). Both questions \
      are a linear walk of the neighbour's chunk chain, and nothing used to be remembered between two probes of the \
      same list, so a hub of degree D was walked D times by each of them: O(D²) on exactly the vertex the check \
      exists to survive, with every walk risking a page read once the graph exceeds the page cache. A 657GB database \
      with hubs in the hundreds of thousands of edges measured 80 hours for one CHECK DATABASE FIX. With the cache, \
      the first probe of a list materialises it into primitive hash sets and every later probe of it is a hash \
      lookup, so a pass walks each list once instead of once per incident edge. Counted in ENTRIES rather than in \
      lists because that is what the memory is proportional to and because one super-node can be larger than every \
      other list in the database put together; the least-recently-probed list is evicted when the budget is \
      exceeded, so hubs - the lists that pay for themselves - are the ones that stay. A single list larger than the \
      whole budget is never cached and is answered by the original walk. At the default of 1 million entries the \
      footprint is roughly 20MB. 0 disables the cache and restores the pre-#6062 probe, which is the escape hatch \
      if the memory is unwelcome on a small heap; the check reports what the setting bought under the \
      adjacencyProbes / adjacencyProbeListWalks / adjacencyEntriesScanned keys.""",
      Integer.class, 1_000_000),

  PAGE_FLUSH_QUEUE("arcadedb.pageFlushQueue", SCOPE.DATABASE,
      """
      Maximum number of page batches EACH database may have waiting in the asynchronous flush pipeline. A committer of \
      a database that has reached it waits - before taking the page-manager lock, never inside it - until one of that \
      database's own batches is written; the committers of every other database are admitted straight through. This \
      was a single JVM-wide bound before issue #6281, which is what let one database's write burst against a slow \
      volume throttle the commits of unrelated databases on idle ones. Values below 1 are raised to 1: this is now \
      the only bound on the pipeline, so a budget of 0 would refuse every publication for ever.\
      """, Integer.class, 512),

  FLUSH_SUSPEND_MAX_DEFERRED_RAM("arcadedb.flushSuspendMaxDeferredRAM", SCOPE.DATABASE,
      """
      Maximum amount of RAM (in MB) of dirty pages the page-flush thread may defer in memory while flushing \
      is suspended (during an HA snapshot ship or a full backup, when the on-disk files must stay stable). \
      Once the deferred backlog crosses this cap the committing threads of the SUSPENDED databases are \
      throttled, instead of the deferred backlog growing without limit and exhausting the heap (issue #4728: \
      a busy leader shipping a multi-GB snapshot OOM'd). The cap is JVM-wide because the heap it bounds is, \
      but the throttling is not: a database that is not suspended is never held by it, since its pages go \
      straight to disk and relieve the backlog rather than add to it (issue #6200). Set to 0 to disable the \
      cap (unbounded, pre-4728 behavior).""",
      Long.class, 512),

  PAGE_SNAPSHOT_ENABLED("arcadedb.pageSnapshotEnabled", SCOPE.DATABASE,
      """
      Serve point-in-time readers (full backup, HA database verify, HA snapshot ship) from the page-level \
      copy-on-write shadow instead of freezing the data files with FLUSH_SUSPEND_MAX_DEFERRED_RAM-bounded flush \
      suspension (issue #6075). With the shadow the only stall is one bounded flush-queue drain when the window \
      opens; after that writers run at full speed for the whole operation and index compaction is no longer \
      postponed. Set to false to fall back to the historical suspend-and-freeze path, which is also selected \
      automatically when a shadow breaches PAGE_SNAPSHOT_MAX_SIZE.""",
      Boolean.class, true),

  PAGE_SNAPSHOT_MAX_RAM("arcadedb.pageSnapshotMaxRAM", SCOPE.DATABASE,
      """
      Maximum amount of RAM (in MB) of page pre-images a snapshot window keeps in memory before spilling the rest \
      to a scratch file next to the database (issue #6075). The shadow only ever holds the pages DIRTIED while the \
      window is open, once each, so a short backup on a moderately busy database often never touches the disk at \
      all - which is the point of making it RAM first. Raise it to trade heap for keeping longer windows in memory.""",
      Long.class, 64),

  PAGE_SNAPSHOT_MAX_SIZE("arcadedb.pageSnapshotMaxSize", SCOPE.DATABASE,
      """
      Hard cap (in MB, RAM plus spill file) on the size a single snapshot shadow may reach before the window is \
      declared overflowed (issue #6075, challenge C4). On breach the window stops capturing and every reader fails \
      loudly, so the consumer can fall back to the suspend-and-freeze path - never a silently truncated or torn \
      snapshot. The default -1 sizes the cap AUTOMATICALLY when the window opens (issue #6125), as the smaller of \
      the ceiling the shadow provably cannot exceed - one pre-image per page that existed at t0, so the t0 size of \
      the page files - and half the space still usable on the volume holding the spill file. A flat number cannot \
      do that: measurements on a 128 MB database show the shadow reaching 100% of the database under a flat-out \
      writer, so any fixed default is simply the database size above which backups silently start falling back to \
      throttling the writers. Set a positive value to pin an absolute cap in MB, or 0 for no cap at all; any \
      negative value means automatic, so -1 is the spelling to use rather than the only one accepted.""",
      Long.class, -1),

  PAGE_SNAPSHOT_SPILL_PATH("arcadedb.pageSnapshotSpillPath", SCOPE.DATABASE,
      """
      Directory holding the scratch spill file of a snapshot shadow, empty (the default) to keep it in the database \
      directory (issue #6125). A shadow can grow to the size of the database, so on a volume sized for the data \
      alone it competes for space with the very files it is protecting; pointing this at another volume removes \
      that coupling, and the automatic PAGE_SNAPSHOT_MAX_SIZE is then measured against the free space THERE. The \
      file is pure scratch - it is created when the RAM budget is exhausted, deleted when the window closes, and \
      never read by recovery.""",
      String.class, ""),

  EXPLICIT_LOCK_TIMEOUT("arcadedb.explicitLockTimeout", SCOPE.DATABASE, "Timeout in ms to lock resources on explicit lock",
      Long.class, 5000),

  COMMIT_LOCK_TIMEOUT("arcadedb.commitLockTimeout", SCOPE.DATABASE, "Timeout in ms to lock resources during commit", Long.class,
      5000),

  TX_RETRIES("arcadedb.txRetries", SCOPE.DATABASE, "Number of retries in case of MVCC exception", Integer.class, 3),

  TX_RETRY_DELAY("arcadedb.txRetryDelay", SCOPE.DATABASE,
      "Cap in milliseconds on the random wait before the next transaction retry (issue #5587: exponential backoff with full jitter, min(this cap, TX_RETRY_DELAY_BASE * 2^attempt)). Helpful in case of high concurrency on the same pages (multi-thread insertion over the same bucket). Set to 0 to disable the delay entirely",
      Integer.class, 100),

  TX_RETRY_DELAY_BASE("arcadedb.txRetryDelayBase", SCOPE.DATABASE,
      "Starting size in milliseconds of the transaction retry backoff window, before doubling on each further attempt up to the TX_RETRY_DELAY cap (issue #5587). Kept small by default so light contention (which resolves in a handful of attempts) sees a shorter wait than before, while a long-running retry loop still saturates at the same worst-case cap as a single flat TX_RETRY_DELAY window",
      Integer.class, 2),

  DELETE_TOLERATE_BROKEN_CHAIN("arcadedb.deleteTolerateBrokenChain", SCOPE.DATABASE,
      "When deleting a record whose own multi-page chunk chain is structurally broken, complete the deletion anyway instead of failing (for a vertex, this also disconnects its edges best-effort, which can leave dangling edges if some cannot be reached). Disabled by default: such a delete fails loudly instead, requiring an explicit CHECK DATABASE FIX to repair or remove the broken record deliberately - CHECK DATABASE FIX itself is unaffected by this setting either way, so the record is never permanently stuck (issues #4420/#4432). Enable only to restore the older behavior of a normal DELETE silently forcing through instead",
      Boolean.class, false),

  GRAPH_EDGE_APPEND_MERGE("arcadedb.graph.edgeAppendMerge", SCOPE.DATABASE,
      "At commit, when the only conflict on an edge-list page is concurrent in-chunk edge appends (which commute), re-apply the appends on top of the newer page version instead of failing the whole transaction with a ConcurrentModificationException. Removes the retry storm on super-node (hot vertex) edge insertion",
      Boolean.class, true),

  TX_PAGE_SLOT_MERGE("arcadedb.txPageSlotMerge", SCOPE.DATABASE,
      "Generalization of GRAPH_EDGE_APPEND_MERGE to arbitrary records. At commit, when a bucket page conflicts only because concurrent transactions touched DIFFERENT record slots on it (logically-unrelated records sharing a page), re-apply this transaction's slot writes on top of the newer committed page instead of failing the whole transaction with a ConcurrentModificationException. Covers new-record inserts into free slots, every update that stays inside the page - an overwrite of the same size or smaller (e.g. the vertex edge-list head-pointer flip on super-node insertion) as well as a record growth the page can host by shifting the records that follow - the delete of a plain in-place record, which only frees its own slot, and the shapes a record takes once it outgrows its page: the content record behind a placeholder pointer, the head chunk of a multi-page record, and the spill that turns a plain record into that head chunk. A genuine same-record conflict, or any change that is not confined to one slot (a placeholder pointer being created or rebuilt, the continuation chunks of a multi-page record), still raises the exception so it is retried",
      Boolean.class, true),

  TX_PAGE_SLOT_MERGE_MAX_BYTES("arcadedb.txPageSlotMergeMaxBytes", SCOPE.DATABASE,
      "Per-transaction soft cap (in bytes) on the record pre-images/final images retained for the disjoint-slot merge (TX_PAGE_SLOT_MERGE). When a transaction's tracked images exceed this, the merge is disabled for the rest of that transaction and its conflicting pages fall back to a normal retry - bounding heap on a very large transaction (e.g. a bulk in-place update) instead of retaining ~2x every touched record until commit",
      Long.class, 16L * 1024 * 1024),

  GRAPH_SUPERNODE_THRESHOLD("arcadedb.graph.supernodeThreshold", SCOPE.DATABASE,
      "Approximate number of edges (per vertex, per direction) after which the vertex's edge list is promoted to the striped super-node layout, spreading further appends over multiple files so concurrent insertions on the same hot vertex do not contend. FORWARD-INCOMPATIBLE ON FIRST USE: promotion writes a new record type (the stripe directory), so once any vertex promotes, the database can no longer be opened by releases older than 26.8.1; promotion is one-way. This ordering guarantee applies only to the OLTP edge-list read walks (edgeIterator/vertexIterator/ridIterator): iteration order on promoted vertices is APPROXIMATELY newest-first instead of exactly newest-first, the stripe chains are interleaved so the newest edge is always within the first 'supernodeStripes' entries and an edge of recency rank r is returned at a position of order r, but only the order WITHIN a stripe is exact - an application needing an exact order must sort or use an index. That rank-fidelity holds for the whole read: the first 'supernodeInterleaveRounds x supernodeStripes' entries are taken one per stripe per turn and past that the rotation widens into geometrically growing batches, which costs the position of an entry a bounded factor rather than the relation to its rank (see GRAPH_SUPERNODE_INTERLEAVE_ROUNDS). It does NOT hold for a query the planner routes through a GraphAnalyticalView (e.g. GAVExpandAll): a view returns neighbours ordered by internal dense node ID, which carries no relationship to recency. 0 disables promotion entirely (databases stay fully readable by older versions)",
      Integer.class, 4096),

  GRAPH_EDGE_LIST_INITIAL_CHUNK_SIZE("arcadedb.graph.edgeListInitialChunkSize", SCOPE.DATABASE,
      "Size in bytes of the FIRST chunk of a vertex's edge list. Each further chunk doubles the previous one up to 8192 bytes, so the total a vertex allocates is the sum of the series - which means a SMALLER first chunk does not necessarily use less space, it just takes more chunks to reach the same capacity and adds a record header per chunk. Tune with a measured degree distribution: a value close to the bytes a typical vertex's edges occupy avoids both the slack of an oversized first chunk and the extra chunks of an undersized one",
      Integer.class, 64),

  GRAPH_SUPERNODE_STRIPES("arcadedb.graph.supernodeStripes", SCOPE.DATABASE,
      "Number of stripes (separate edge-list files) a super-node's edge list is spread over at promotion. The stripes are hosted in a per-type bucket pool of this many files, created once per type at its first promotion (types without super-nodes cost no files). Write parallelism saturates at the number of concurrent writers, so values beyond the CPU cores rarely help. Values below 2 disable promotion entirely. Recorded per vertex at promotion time",
      Integer.class, 16),

  GRAPH_SUPERNODE_INTERLEAVE_ROUNDS("arcadedb.graph.supernodeInterleaveRounds", SCOPE.DATABASE,
      "Number of round-robin ROUNDS (one entry taken from every stripe chain per round) a super-node read walk (edgeIterator/vertexIterator/ridIterator) keeps at one entry per turn before WIDENING the rotation. Taking one entry per turn reconstructs the approximately newest-first order but keeps a resident chunk page per stripe and hops between files on every entry, which a caller reading a bounded prefix (paging, a query with a small LIMIT) never notices and a full walk pays for its whole degree (#6048). Past 'rounds x supernodeStripes' entries of a generation the walk therefore keeps rotating but takes 'rounds' entries from each chain per turn, doubling that batch on every completed round: the chain switches over a walk of D entries drop from D to about supernodeStripes x log(D), and each visit becomes a sequential run through a chain once the batch outgrows a chunk, while an edge of recency rank r still comes back at a position of order r for the WHOLE walk instead of only for the first 'rounds x supernodeStripes' entries (#6064) - so a large-but-bounded LIMIT keeps the ordering in proportion to what it asked for without the threshold having to be raised globally. The widening point scales with the live stripe count of the generation being walked, not with the vertex's total degree, so the extra cost a full walk pays for the ordered prefix is bounded regardless of how large the super-node grows. 0 disables interleaving entirely (immediate concatenation, the pre-#6044 order); a negative value behaves the same as 0 rather than being rejected",
      Integer.class, 64),

  BACKUP_ENABLED("arcadedb.backup.enabled", SCOPE.DATABASE,
      "Allow a database to be backup. Disabling backup gives a huge boost in performance because no lock will be used for every operations",
      Boolean.class, true),

  BACKUP_COMPRESSION_LEVEL("arcadedb.backup.compressionLevel", SCOPE.DATABASE,
      """
      Deflate level (0 = store, 1 = fastest, 9 = smallest) used to compress a full backup. The backup is CPU bound, \
      not I/O bound, so this is the single most effective knob on its duration - and the backup's duration is also \
      the window during which page flushing is suspended and committing threads are throttled \
      (FLUSH_SUSPEND_MAX_DEFERRED_RAM), so a shorter backup is a shorter stall for writers. The default was lowered \
      from 9 to 1 on measurement, not intuition: on a 1.25 GB database it is 3.1x faster for a 7.5% bigger archive \
      (323 MB at level 9, 348 MB at level 1). Raise it when the archive size matters more than both the backup \
      duration and its impact on concurrent writers - level 6 is a good middle, roughly half the cost of 9 at the \
      same ratio.""",
      Integer.class, 1, integerRangeAsStrings(0, 9)),

  BACKUP_COMPRESSION_THREADS("arcadedb.backup.compressionThreads", SCOPE.DATABASE,
      """
      Number of threads used to compress a full backup. Each entry is cut into chunks that are deflated in parallel \
      and concatenated back in order, so the parallelism applies WITHIN a file too and a database made of one \
      dominant file still scales. -1 (the default) sizes the pool automatically at half the available processors, \
      capped at 8, leaving room for the live workload the backup is running alongside; 0 selects the legacy \
      single-threaded java.util.zip.ZipOutputStream writer, kept as an escape hatch. The archive is an ordinary ZIP \
      whichever value is used: old backups restore and new backups restore with the unchanged restore path. Peak heap \
      for the parallel path is bounded by construction at two chunks in flight per thread, each holding one input and \
      one output buffer of about the 1 MB chunk size - so roughly 4 MB per thread, ~32 MB of buffers at 8 threads. That \
      is the compressor's own footprint, not the process total: measured heap during an 8-thread backup is ~45 MB, the \
      ~32 MB of buffers on top of a ~12 MB baseline.""",
      // THE 256 IS THE SAME BOUND AS BackupSettings.MAX_COMPRESSION_THREADS, WHICH THE CLI, THE Backup API AND SQL
      // VALIDATE AGAINST. IT HAS TO BE REPEATED HERE RATHER THAN REFERENCED BECAUSE THE ENGINE CANNOT DEPEND ON THE
      // INTEGRATION MODULE: CHANGE ONE AND CHANGE THE OTHER
      Integer.class, -1, integerRangeAsStrings(-1, 256)),

  BACKUP_MAX_MB_PER_SECOND("arcadedb.backup.maxMBPerSecond", SCOPE.DATABASE,
      """
      Optional cap, in MB/s, on the rate at which a full backup reads the database files, so a backup cannot \
      saturate the production disk. It is deliberately applied to the read side: that is the I/O competing with the \
      live workload, while the archive is smaller and normally written to another device. 0 (the default) means no \
      limit. Note the trade-off with the flush suspension: throttling makes the backup last longer, and writers are \
      throttled for the whole of it, so this is for deployments where read I/O, not commit latency, is the scarce \
      resource. Unlike the other two backup settings this one carries no allowed-value set, because it needs none: \
      the range is open-ended upwards, and any non-positive value simply disables the throttle rather than being \
      invalid.""",
      Integer.class, 0),

  RESTORE_THREADS("arcadedb.restore.threads", SCOPE.JVM,
      """
      Number of threads used to restore a full backup. ZIP entries are independent files, so they are inflated and \
      written concurrently, one entry per thread. -1 (the default) sizes the pool automatically at the available \
      processors capped at 8; 0 selects the legacy single-threaded stream walk, kept as an escape hatch. Unlike a \
      backup, a restore does not run alongside the database it is working on - that database does not exist yet - \
      which is why the automatic sizing claims whole cores rather than half of them. The parallel path needs random \
      access to the archive and is therefore used only for a plain local file: an archive read over http(s) is a \
      one-shot stream and an encrypted one is a single cipher stream, and both fall back to the sequential walk \
      automatically, whatever this setting says. Peak heap is bounded by construction at one copy buffer per thread \
      (256 KB) plus the JDK inflater's own buffer, so under 3 MB at 8 threads. Parallelism is per entry: a database \
      made of one dominant file cannot be split, because a ZIP entry is a single deflate stream that has to be \
      inflated serially.""",
      // SAME BOUND AS RestoreSettings.MAX_RESTORE_THREADS, WHICH THE CLI AND THE Restore API VALIDATE AGAINST. THE
      // ENGINE CANNOT DEPEND ON THE INTEGRATION MODULE, SO THE LITERAL IS REPEATED: CHANGE ONE AND CHANGE THE OTHER
      Integer.class, -1, integerRangeAsStrings(-1, 256)),

  // SQL
  SQL_STATEMENT_CACHE("arcadedb.sqlStatementCache", SCOPE.DATABASE, "Maximum number of parsed statements to keep in cache",
      Integer.class, 300),

  SQL_MAX_EXPRESSION_DEPTH("arcadedb.sql.maxExpressionDepth", SCOPE.DATABASE,
      """
      Maximum nesting depth allowed for parentheses in a single SQL statement (WHERE conditions, sub-expressions, \
      nested function/statement calls, ...). The ANTLR-generated SQL parser resolves ambiguity between several \
      grammar rules that all start with '(' (a parenthesized expression, condition, or sub-statement) by first \
      trying a fast SLL prediction and falling back to full ALL(*) prediction on failure; for a query with enough \
      nested parentheses that fallback's cost grows so steeply that a query of only a few KB can tie up a worker \
      thread for minutes without ever crashing, which is worse than a fast failure since it is not distinguishable \
      from a slow legitimate query. This is checked on the token stream before any parse is attempted, so a query \
      past the limit is rejected in O(n) time with a normal parse error. Real-world queries rarely nest more than a \
      handful of parentheses, so the default is deliberately generous. Raise it only if a legitimate, deeply-nested \
      or generated query needs it.""",
      Integer.class, 200),

  // OPENCYPHER
  OPENCYPHER_STATEMENT_CACHE("arcadedb.opencypher.statementCache", SCOPE.DATABASE,
      "Maximum number of parsed OpenCypher statements to keep in cache", Integer.class, 300),

  OPENCYPHER_PLAN_CACHE("arcadedb.opencypher.planCache", SCOPE.DATABASE,
      "Maximum number of OpenCypher execution plans to keep in cache (frequency-based eviction)", Integer.class, 300),

  OPENCYPHER_BULK_CREATE_BATCH_SIZE("arcadedb.opencypher.bulkCreateBatchSize", SCOPE.DATABASE,
      """
      Batch size for bulk CREATE operations. When a CREATE follows an UNWIND producing multiple rows, records are accumulated and created in batches to reduce transaction overhead. \
      Higher values improve performance but consume more memory. Default: 20000. Recommended range: 10000-100000. Set to 0 to disable batching.""",
      Integer.class, 20_000),

  OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS("arcadedb.opencypher.loadCsv.allowFileUrls", SCOPE.DATABASE,
      """
      Allow LOAD CSV to access local files via file:/// URLs and bare file paths. \
      Disable for security in multi-tenant server deployments. This is only the outer switch: on a server the \
      local-file branch also requires the administrative 'updateSecurity' permission (the same privilege IMPORT \
      DATABASE requires), and 'production' server mode force-disables this setting at startup.""",
      Boolean.class, true),

  OPENCYPHER_LOAD_CSV_IMPORT_DIRECTORY("arcadedb.opencypher.loadCsv.importDirectory", SCOPE.DATABASE,
      """
      Root directory for LOAD CSV file:/// URLs. When set, file paths are resolved relative to this \
      directory and path traversal (../) is blocked. Empty string means no restriction.""",
      String.class, ""),

  OPENCYPHER_LOAD_CSV_ALLOW_REMOTE_URLS("arcadedb.opencypher.loadCsv.allowRemoteUrls", SCOPE.DATABASE,
      """
      Allow LOAD CSV to fetch data from remote http:// and https:// URLs. When enabled (default), remote fetches are still \
      restricted by arcadedb.opencypher.loadCsv.blockedIpRanges to prevent Server-Side Request Forgery (SSRF) against internal \
      services. Disable to block all remote URL access in locked-down or multi-tenant deployments.""",
      Boolean.class, true),

  OPENCYPHER_LOAD_CSV_BLOCKED_IP_RANGES("arcadedb.opencypher.loadCsv.blockedIpRanges", SCOPE.DATABASE,
      """
      Comma-separated list of CIDR ranges that LOAD CSV remote http(s) fetches are NOT allowed to reach. Enforced against the \
      resolved IP address of the target host and re-checked on every redirect hop to prevent Server-Side Request Forgery (SSRF). \
      Defaults to loopback, private (RFC 1918), link-local (including the cloud metadata address 169.254.169.254), carrier-grade \
      NAT, multicast and reserved ranges. Set to an empty string to disable IP filtering (not recommended).""",
      String.class, IPAddressBlocklist.DEFAULT_RESERVED_RANGES),

  OPENCYPHER_LABEL_WRITE_DEGREE_WARNING("arcadedb.opencypher.labelWriteDegreeWarning", SCOPE.DATABASE,
      """
      Degree above which a Cypher label write (SET n:Label / REMOVE n:Label) logs a warning. A record's type comes from the \
      bucket it lives in, so a label change is not a metadata edit: the vertex is rewritten under the new type and every \
      incident edge is re-created in both directions, which makes the write O(degree) and gives the vertex and all of its \
      edges new RIDs. On a supernode that is the difference between a millisecond and a stall, and nothing in the query says \
      so. The warning reports the node, the two types and how many edges were rewritten. Set to 0 to disable it.""",
      Integer.class, 10_000),

  OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT("arcadedb.opencypher.labelWriteDegreeLimit", SCOPE.DATABASE,
      """
      Maximum degree of a vertex a Cypher label write (SET n:Label / REMOVE n:Label) is allowed to rewrite. Above it the \
      command fails instead of paying the O(degree) rewrite described in arcadedb.opencypher.labelWriteDegreeWarning. \
      Disabled by default (0): the rewrite is slow, not wrong, so refusing it is opt-in. Enabling it costs one extra \
      edge-list walk per label write, to answer the question before any record has moved.""",
      Integer.class, 0),

  OPENCYPHER_ID_BUCKET_BITS("arcadedb.opencypher.idBucketBits", SCOPE.JVM,
      """
      Number of bits reserved for the bucketId when packing a RID into the numeric value returned by the OpenCypher id() function (and SQL's .asCypherRID() method). \
      Out of the 63 usable bits (the sign bit is always kept clear to preserve the Neo4j id(n) >= 0 semantics), this many go to the bucketId and the rest to the \
      record position within the bucket. The default of 16 allows up to 65536 buckets and ~1.4e14 positions per bucket, covering the vast majority of use cases. \
      Increase it for databases with many buckets, decrease it for buckets holding a very high number of records. Must be between 1 and 31. \
      Changing this value alters the numeric id() output, so encode and decode must use the same setting.""",
      Integer.class, 16, integerRangeAsStrings(1, 31)),

  // COMMAND
  COMMAND_TIMEOUT("arcadedb.command.timeout", SCOPE.DATABASE, """
      Maximum time in ms a single command may run before being aborted with a TimeoutException. The deadline is \
      taken once, when execution starts, and is shared by everything the statement does - a CALL subquery, a \
      correlated COUNT { }, a UNION branch and a CALL algo.* procedure all run against the same instant rather than \
      each starting a fresh budget. It is checked inside the scan, expansion and filter loops, so a statement that \
      produces no row for minutes is bounded too, not only one that streams rows. Covers SQL and openCypher, \
      including SELECT/UPDATE/DELETE/MATCH/TRAVERSE and the openCypher algo.* procedures. A per-statement SQL \
      TIMEOUT clause is enforced alongside it and the earlier of the two wins, so a statement may ask for less \
      time than this setting allows but not for more. Gremlin and the other polyglot scripting \
      engines are NOT covered - they have their own arcadedb.polyglotCommand.timeout - and neither is regular \
      expression backtracking, which arcadedb.command.regexTimeout bounds separately. Set to 0 (the default) to \
      disable.""",
      Long.class, 0),

  COMMAND_REGEX_TIMEOUT("arcadedb.command.regexTimeout", SCOPE.DATABASE, """
      Maximum time in ms a single regular expression evaluation may run before being aborted (an entire scan, for a \
      MATCHES/=~/LIKE/ILIKE/full-text/PromQL query - see below). Covers SQL MATCHES, openCypher =~, SQL LIKE/ILIKE, the \
      text.regexReplace() and .normalize() functions, full-text search's RegexpQuery/WildcardQuery, PromQL's =~/!~ label \
      matchers, and schema-level REGEXP property validation. java.util.regex backtracking does not poll interrupts or \
      deadlines, so a pathological pattern (catastrophic backtracking) keeps its worker thread busy regardless of \
      arcadedb.command.timeout; this dedicated bound protects against that even when arcadedb.command.timeout is disabled \
      (0), which is the default. Every entry point reached through a command context - MATCHES, =~, PromQL's matchers \
      and the text.regexReplace()/.normalize() functions - shares ONE deadline for the whole command: not one per row, \
      not one per function, and not one per worker of a parallel type scan. Full-text search and REGEXP property \
      validation run outside a command context and share one deadline across an entire scan (not per item). A large, \
      legitimately slow (non-catastrophic) operation can hit this bound too, so raise it for workloads that need more \
      than 1s. Set to 0 to disable (not recommended).""",
      Long.class, 1000),

  COMMAND_WARNINGS_EVERY("arcadedb.command.warningsEvery", SCOPE.JVM,
      "Reduce warnings in commands to print in console only every X occurrences. Use 0 to disable warnings with commands",
      Integer.class, 100),

  GREMLIN_ENGINE("arcadedb.gremlin.engine", SCOPE.DATABASE,
      """
      Gremlin engine to use. 'java' (default, secure) uses the native Gremlin parser - recommended for production. \
      'groovy' enables the legacy Groovy engine with security restrictions (use only if needed for compatibility). \
      'auto' attempts Java first, falls back to Groovy if needed (not recommended for security-critical deployments).""",
      String.class, "java", Set.of("auto", "groovy", "java")),

  /**
   * Not in use anymore after removing Gremlin Executor
   */
  @Deprecated GREMLIN_COMMAND_TIMEOUT("arcadedb.gremlin.timeout", SCOPE.DATABASE, "Default timeout for gremlin commands (in ms)",
      Long.class, 30_000),

  // USER CODE
  POLYGLOT_COMMAND_TIMEOUT("arcadedb.polyglotCommand.timeout", SCOPE.DATABASE, "Default timeout for polyglot commands (in ms)",
      Long.class, 10_000),

  POLYGLOT_ENGINE_ENABLED("arcadedb.polyglotEngineEnabled", SCOPE.JVM,
      """
      Enable the GraalVM Polyglot Engine used to register scripting languages (js, python, ...) as query engines. \
      When true (default), the shared Engine is created lazily on first use and all GraalVM languages found on \
      the classpath are registered. When false, the Polyglot engine is not initialised and no polyglot language \
      is registered: this saves tens of MB of heap and class-loading work on small footprints. The 'low-ram' \
      profile sets this to false.""",
      Boolean.class, true),

  QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP("arcadedb.queryMaxHeapElementsAllowedPerOp", SCOPE.DATABASE, """
      Maximum number of elements (records/groups) allowed in a single query for memory-intensive operations (eg. ORDER BY, GROUP BY \
      and DISTINCT in heap). If exceeded, the query fails with a CommandExecutionException. Negative number means no limit. \
      This setting is intended as a safety measure against excessive resource consumption from a single query (eg. prevent OutOfMemory). \
      When left at the default it auto-scales with the JVM max heap (roughly one element every 2KB of heap, never below 500000), so \
      large-cardinality analytical queries (eg. top-N-by-aggregate over millions of distinct keys) complete out of the box on servers \
      with a big heap while small footprints stay protected. Set an explicit value to override the auto-scaling.""",
      Long.class, 500_000L, null, value -> {
        // Auto-scale the default with the JVM max heap: roughly one element every 2KB, never below the historical 500000 floor.
        final long maxHeap = Runtime.getRuntime().maxMemory();
        if (maxHeap == Long.MAX_VALUE)
          // Heap is unbounded (no -Xmx): keep the conservative floor rather than an effectively unlimited cap.
          return 500_000L;
        return Math.max(500_000L, maxHeap / 2048);
      }),

  QUERY_MAX_RANGE_SIZE("arcadedb.queryMaxRangeSize", SCOPE.DATABASE, """
      Maximum number of elements a range() expression is allowed to produce. If exceeded, the query is rejected with a \
      client error before any element is generated. Negative number means no limit (the hard limit of 2147483647 elements, \
      the maximum size of a Java list, still applies). The range itself is lazy and takes no heap, and the operations \
      whose answer is still an arithmetic progression keep it that way: slicing, tail(), reverse(), coll.sort(), \
      coll.distinct(), coll.toSet(), coll.flatten() and cutting either end with coll.remove(). Its elements are \
      materialised only when the answer cannot be a range - inserting into one, merging two of them, concatenating with \
      +, or serialising the range in a response - so this setting caps the memory a single query can request that way. \
      When left at the default it auto-scales with the JVM max heap (never below 1000000 elements), keeping the \
      worst-case materialisation of a range to a fraction of the heap.""",
      Long.class, 10_000_000L, null, value -> {
        // Auto-scale the default with the JVM max heap. A materialised element costs ~24 bytes (boxed Long plus the
        // reference that holds it) and rendering it in a JSON response costs about as much again, so heap/160 keeps
        // the worst case around a quarter of the heap. Never below the 1000000 floor, so the common
        // "UNWIND range(1, 1000000)" data-generation idiom keeps working on small footprints.
        final long maxHeap = Runtime.getRuntime().maxMemory();
        if (maxHeap == Long.MAX_VALUE)
          // Heap is unbounded (no -Xmx): keep a conservative cap rather than an effectively unlimited one.
          return 10_000_000L;
        return Math.max(1_000_000L, Math.min(Integer.MAX_VALUE, maxHeap / 160));
      }),

  QUERY_PARALLEL_SCAN("arcadedb.queryParallelScan", SCOPE.DATABASE,
      """
      Enable parallel scanning of multiple buckets during full table scans. \
      When true, each bucket is scanned in a separate thread for improved throughput on multi-core systems""",
      Boolean.class, true),

  QUERY_PARALLEL_SCAN_MIN_BUCKETS("arcadedb.queryParallelScanMinBuckets", SCOPE.DATABASE,
      """
      Minimum number of buckets required to trigger parallel scanning. \
      If the type has fewer buckets than this threshold, sequential scanning is used""",
      Integer.class, 2),

  // CYPHER
  CYPHER_STATEMENT_CACHE("arcadedb.cypher.statementCache", SCOPE.DATABASE,
      "Max number of entries in the cypher statement cache. Use 0 to disable. Caching statements speeds up execution of the same cypher queries",
      Integer.class, 1000),

  CYPHER_MAX_EXPRESSION_DEPTH("arcadedb.cypher.maxExpressionDepth", SCOPE.DATABASE,
      """
      Maximum nesting depth allowed for a single Cypher expression, for example parentheses, list/map literals \
      or function arguments nested inside one another, and the depth of a chain of AND/OR/string-concatenation \
      terms in the resulting expression tree. The ANTLR-generated parser re-enters its expression grammar rule \
      roughly ten Java stack frames per nesting level, so a few thousand levels is enough to exhaust the default \
      JVM thread stack with a payload of only a few KB; a query past this limit is rejected as a normal parse \
      error instead of crashing the worker thread with a StackOverflowError. Real-world queries rarely nest \
      more than a handful of levels, so the default is deliberately generous while staying far below the point \
      where the stack is at risk. Raise it only if a legitimate, deeply-nested or very long generated query needs it.""",
      Integer.class, 200),

  CYPHER_ALGO_MAX_WORKING_MEMORY("arcadedb.cypher.algoMaxWorkingMemory", SCOPE.DATABASE,
      """
      Maximum heap, in bytes, that a single call to an OpenCypher algorithm procedure may reserve for the dense \
      working set it builds beside the graph: the random-walk buffers of algo.node2vec (walksPerNode x nodeCount \
      walks of walkLength steps) and algo.randomWalk (a single walk of steps entries), algo.slpa's label memory \
      (one row of iterations entries per node), the nodeCount x dimension embedding matrices of algo.node2vec, \
      algo.fastrp, algo.hashgnn and algo.graphsage, the nodeCount x nodeCount matrices of algo.apsp, \
      algo.simRank, algo.maxFlow and algo.kShortestPaths, and the terminals x nodeCount tables and \
      terminal-pair arrays of algo.steinerTree. None of these has a graph-derived ceiling to clamp against - \
      unlike a top-k bound, which is capped by the node count - so a large but perfectly in-range int, a \
      terminal list of any length, or simply a large graph, would otherwise reach the allocator unchecked, or \
      wrap the int product on the way there and surface as a NegativeArraySizeException from inside the \
      algorithm. Every estimate is computed in saturating long arithmetic and reserved BEFORE anything is \
      allocated, and reservations accumulate over the call, so what is bounded is the working set of the whole \
      call rather than one allocation of it: a call over the budget is rejected as a client error naming the \
      component and the knobs that produced the estimate, together with this setting. Negative number means no \
      limit. When left at the default it auto-scales with the JVM max heap (one eighth of it, never below 64MB), \
      so the working set of a legitimate large run stays a fraction of the heap it shares with the rest of the \
      query.""",
      Long.class, 64 * 1024 * 1024L, null, value -> {
        // Auto-scale the default with the JVM max heap: one eighth of it, never below the 64MB floor so that a
        // typical run (a few million walk entries, or a mid-sized graph at the default embedding dimension)
        // keeps working on small footprints.
        final long maxHeap = Runtime.getRuntime().maxMemory();
        if (maxHeap == Long.MAX_VALUE)
          // Heap is unbounded (no -Xmx): keep the conservative floor rather than an effectively unlimited cap.
          return 64 * 1024 * 1024L;
        return Math.max(64 * 1024 * 1024L, maxHeap / 8);
      }),

  // GRAPHQL
  GRAPHQL_MAX_NESTING_DEPTH("arcadedb.graphql.maxNestingDepth", SCOPE.DATABASE,
      """
      Maximum nesting depth allowed for '{' ... '}' or '[' ... ']' in a single GraphQL document - selection \
      sets (fields nested inside one another), object/interface/input type bodies, and list values/types all \
      use one of these two delimiter pairs and count toward the same limit. The JavaCC-generated parser \
      re-enters its SelectionSet/Field grammar rules, or the mutually-recursive Value/ListValue rules, once per \
      nesting level, so a few thousand levels is enough to exhaust the default JVM thread stack with a payload \
      of only a few KB; a document past this limit is rejected as a normal parse error instead of crashing the \
      worker thread with a StackOverflowError. Real-world documents rarely nest more than a handful of levels, \
      so the default is deliberately generous while staying far below the point where the stack is at risk. \
      Raise it only if a legitimate, deeply-nested or very long generated document needs it.""",
      Integer.class, 200),

  // INDEXES
  INDEX_BUILD_CHUNK_SIZE_MB("arcadedb.index.buildChunkSizeMB", SCOPE.DATABASE,
      """
      Size in MB for transaction chunks during bulk index creation with WAL disabled. \
      Larger chunks reduce commit overhead but use more memory. \
      Smaller chunks reduce memory pressure but add commit overhead. \
      Recommended: 50MB for typical workloads, 100MB for high-memory systems, 25MB for constrained environments.""",
      Long.class, 50L),

  INDEX_COMPACTION_RAM_MB("arcadedb.indexCompactionRAM", SCOPE.DATABASE, "Maximum amount of RAM to use for index compaction, in MB",
      Long.class, 300),

  INDEX_COMPACTION_MIN_PAGES_SCHEDULE("arcadedb.indexCompactionMinPagesSchedule", SCOPE.DATABASE,
      "Minimum number of mutable pages for an index to be schedule for automatic compaction. 0 = disabled", Integer.class, 10),

  INDEX_COMPACTION_FULL_SERIES("arcadedb.indexCompactionFullSeriesThreshold", SCOPE.DATABASE,
      "Number of compacted series at which an index compaction runs as a full compaction: every existing series is merged together with the mutable pages into a single fresh series, deletions are resolved and dead entries dropped. Keeps delete-heavy indexes from accumulating unbounded tombstone runs and series. 0 = disabled",
      Integer.class, 10),

  INDEX_BLOOM_FILTER_RATE("arcadedb.indexBloomFilterRate", SCOPE.DATABASE,
      """
      Target false-positive rate of the bloom filter an index compaction writes for each compacted series of an LSM \
      index, letting a point lookup skip a series that provably cannot hold the key without reading any of its pages. \
      Default 0.01 (1%), enabled. Set to 0 to disable.
      WHAT IT DOES. A lookup walks every compacted series from newest to oldest. A series' root page already rules out \
      a key outside its key RANGE, but not a key inside the range that the series simply does not hold - so without a \
      filter that series still costs a root-page search and a data-page read to discover nothing. The filter answers \
      that question from a single 8 KB page instead.
      WHEN IT HELPS. Most when the compacted series OVERLAP in key range, which is what any key that does not arrive \
      already sorted produces: an email, a UUID, a business id. The extreme case is a bulk load into a UNIQUE index, \
      where the duplicate check for every incoming record misses in every series by definition. Measured on 2M keys \
      over 9 series: absent-key lookups about 2x faster and 2x fewer pages read; keys that ARE present also gain, \
      because a key lives in one series and the filters spare the reader the others.
      WHEN IT DOES NOT. Keys inserted in ascending order (a counter, a timestamp) give each series a disjoint key \
      slice that its root page already rules out for free, leaving the filters little to save. Range scans and cursors \
      never consult them at all: a filter can answer for one key, never for an interval.
      WHAT IT COSTS. About 1.2 bytes per key on disk at 1% (roughly 3% of the index it describes), in a separate \
      '<index>_bf.bfidx' paginated component alongside the compacted index. A false positive costs only the page read \
      that would have happened anyway. During compaction it holds 8 bytes of transient heap per key of the series \
      being written. Lower rates cost more bytes and more probes per lookup; higher rates save space and read more \
      series. Below roughly 0.001 the extra bytes stop paying for themselves.
      OPERATIONAL NOTES. Filters are written by compaction only, so an existing index gains them at its next \
      compaction and never needs a rebuild. They are replicated over HA and included in backups like any other \
      component, and are dropped with the index. An older ArcadeDB does not recognise the file and ignores it, so \
      downgrading is safe for reading - but a downgraded build compacts without maintaining the filters, so prefer \
      running a full compaction (or this setting at 0) if you downgrade and come back. \
      A directory page holds ~255 series; beyond that further series are published without a filter until a full \
      compaction collapses the count, which the default arcadedb.indexCompactionFullSeriesThreshold=10 does long \
      before it can happen. Watch 'bloomSkippedSeries' and 'bloomProbedSeries' in the index statistics to see what \
      the filters are actually saving.
      DISABLING. 0 stops new filters being written immediately and stops existing ones being consulted from the next \
      database open. Lookups then behave exactly as they did before the feature existed.""",
      Float.class, 0.01f),

  VECTOR_INDEX_LOCATION_CACHE_SIZE("arcadedb.vectorIndex.locationCacheSize", SCOPE.DATABASE,
      """
      DEPRECATED and ignored since issue #5568: the location index is always unlimited, and a positive value is \
      reported once per index at WARNING. A vector location is the only record of which record a vector id belongs \
      to and where its entry sits in the index file, and nothing on disk maps a vector id back to an offset, so \
      evicting one destroyed that mapping rather than spilling it to a slower tier: the index under-reported its \
      size, and any reader resolving an evicted id read it as deleted. The limit existed when the index held one \
      location per write; issue #5516 made a tombstoned id release \
      its location, so residency is now proportional to the live vectors (~32 bytes each since issue #5588 laid \
      them out in primitive arrays) instead of to the write history. Issue #5559 removed the bounded backend altogether, so nothing can evict a location any more, and \
      the per-index 'locationCacheSize' METADATA key is now REFUSED rather than ignored - this global setting stays \
      tolerated only so that an existing startup line does not stop a server booting.""",
      Integer.class, -1),

  VECTOR_INDEX_COMPACTION_BLOAT_FACTOR("arcadedb.vectorIndex.compactionBloatFactor", SCOPE.DATABASE,
      """
      How much bigger than its live vectors an LSM_VECTOR data file may get before it compacts itself. \
      The file is append-only - an update writes a new vector plus a tombstone for the one it replaces, a delete \
      writes a tombstone - so it grows with the number of writes while the live set stays the same size; a \
      compaction rewrites it holding the live vectors only. 3 means "reclaim once about two thirds of the file is \
      garbage". Lower reclaims sooner and rewrites more often, higher lets the file grow further and rewrites in \
      bigger, rarer passes. Each pass is a full graph rebuild plus a sequential copy of the live vectors, and it \
      holds the index write lock for that copy. Set to 0 to compact only on an explicit COMPACT INDEX; \
      arcadedb.indexCompactionMinPagesSchedule gates it too, and 0 there disables automatic compaction for every \
      index type. Both are read per commit, so either one takes effect at runtime.""",
      Integer.class, 3),

  VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE("arcadedb.vectorIndex.graphBuildCacheSize", SCOPE.DATABASE,
      """
      Maximum number of vectors to cache in memory during HNSW graph building. \
      Higher values speed up construction but use more RAM. \
      RAM usage = cacheSize * (dimensions * 4 + 64) bytes. \
      0 (default) sizes it automatically: an index whose vectors live in the documents (no quantization, or \
      PRODUCT) caches the whole set when it fits arcadedb.vectorIndex.graphBuildCacheMaxHeapPercent, because \
      every miss costs a record read; an inline-quantized index (INT8/BINARY) reads a miss straight from an \
      index page and keeps a small bound instead.""",
      Integer.class, 0),

  VECTOR_INDEX_GRAPH_BUILD_CACHE_MAX_HEAP_PERCENT("arcadedb.vectorIndex.graphBuildCacheMaxHeapPercent", SCOPE.DATABASE,
      """
      Maximum share of the JVM heap (percentage) the auto-sized graph-build cache may use. Only applies when \
      arcadedb.vectorIndex.graphBuildCacheSize is left at 0. A corpus larger than this budget still builds: \
      the cache evicts instead of holding everything. Measured against the heap currently AVAILABLE rather than \
      against the ceiling, so a rebuild that is holding the old graph resident asks for less (issue #6503). \
      Values above 90 are clamped to 90: no cache is allowed to plan on the whole heap.""",
      Integer.class, 25),

  VECTOR_INDEX_SEARCH_CACHE_SIZE("arcadedb.vectorIndex.searchCacheSize", SCOPE.DATABASE,
      """
      Maximum number of vectors kept in the per-index search cache. The cache is shared by every query on the \
      index and survives across queries, so a working set that fits stays resident instead of being re-read from \
      the documents (or from the quantized index pages) on every beam-search hop. \
      RAM usage = cacheSize * (dimensions * 4 + 64) bytes. \
      0 (default) sizes it automatically from the number of indexed vectors, capped by \
      arcadedb.vectorIndex.searchCacheMaxHeapPercent. -1 disables the cache entirely.""",
      Integer.class, 0),

  VECTOR_INDEX_SEARCH_CACHE_MAX_HEAP_PERCENT("arcadedb.vectorIndex.searchCacheMaxHeapPercent", SCOPE.DATABASE,
      """
      Upper bound, as a percentage of the JVM heap currently AVAILABLE rather than of the ceiling (issue #6503), \
      on the RAM an automatically sized per-index search cache may use (see arcadedb.vectorIndex.searchCacheSize). \
      Ignored when the cache size is set explicitly. Values above 90 are clamped to 90: no cache is allowed to \
      plan on the whole heap.""",
      Integer.class, 25),

  VECTOR_INDEX_SEARCHER_POOL_SIZE("arcadedb.vectorIndex.searcherPoolSize", SCOPE.DATABASE,
      """
      Maximum number of JVector graph searchers kept alive per vector index for reuse across queries. Each \
      searcher owns the beam-search scratch state (candidate heap, result heaps, visited set) plus a graph view; \
      allocating one per query made that scratch state the largest single source of garbage on a dense-search \
      workload, and the resulting young-GC frequency dominated the query tail latency. \
      0 (default) sizes the pool automatically as 2x the available cores. -1 disables pooling and allocates a \
      searcher per query.""",
      Integer.class, 0),

  VECTOR_INDEX_PREFILTER_MAX_SELECTIVITY("arcadedb.vectorIndex.prefilterMaxSelectivity", SCOPE.DATABASE,
      """
      Maximum fraction of the index's live vectors that a query's RID allow-list may cover and still take the \
      pre-filter plan instead of a Bits-filtered HNSW graph walk, on the plain k-NN path (issue #6502) and the \
      groupBy path (issue #6514) - see vectorIndex.prefilterApproximateMaxSelectivity for the separate threshold \
      the PQ-approximate path uses. The graph's Bits filter only rejects a node once it is popped from \
      the beam - it cannot make the walk itself shrink with a narrower filter - so a selective allow-list makes the \
      beam admit almost nothing and the walk keeps expanding trying to fill k, the search space growing smaller in \
      principle and larger in practice: at the limit, 5 candidates cost more than 20,000. Below this fraction the \
      query instead resolves the allow-list to its ordinals and scores them directly via the index's regular \
      (exact) scoring function, which is O(allow-list) and exact by construction. Above it the allow-list barely \
      narrows the search, the graph walk is already cheap, and resolving every allowed RID up front would cost \
      more than the walk it replaces. Set to 0 to disable the pre-filter plan and always use the graph walk. \
      Shared between the plain and groupBy paths rather than tuned separately: both score candidates the same way \
      (the groupBy path only adds cap bookkeeping on top), and benchmarking found no crossover far enough apart to \
      justify a second setting (see Issue6502PrefilterLatencyBenchmark / Issue6514GroupedPrefilterBenchmark) - \
      unlike the PQ-approximate path below, whose per-candidate cost is a different shape entirely.""",
      Float.class, 0.2f),

  VECTOR_INDEX_PREFILTER_APPROXIMATE_MAX_SELECTIVITY("arcadedb.vectorIndex.prefilterApproximateMaxSelectivity", SCOPE.DATABASE,
      """
      The vectorIndex.prefilterMaxSelectivity threshold, but for findNeighborsFromVectorApproximate (issue #6514's \
      extension of issue #6502 to the zero-disk-I/O PQ search path) - kept as a separate setting, not \
      folded into that one, because the two paths' pre-filter plans have measurably different crossovers. PQ scores a \
      candidate from in-memory codes, which Issue6514ApproximatePrefilterBenchmark measured at roughly an order of \
      magnitude cheaper per candidate than the exact plan's page/document read; a Bits-filtered PQ graph walk is \
      correspondingly cheaper too, so it stays the better plan for longer as the allow-list narrows. That benchmark \
      put the actual crossover between the two plans at roughly 6-7% selectivity on a 20,000-vector, 128-dimension \
      index - reusing the plain/groupBy paths' 20% default here would route every query between 7% and 20% \
      selectivity through the more expensive of the two plans, 2-8x slower than simply keeping the graph walk. \
      This default is set conservatively below the measured crossover rather than exactly on it, since the \
      crossover itself will shift with dimension count, graph shape and PQ subspace count in ways a single fixed \
      benchmark cannot cover; a deployment with an unusually cheap or expensive PQ scoring function should \
      re-measure with that benchmark and tune this independently of the exact-path setting. Set to 0 to disable the \
      pre-filter plan and always use the graph walk.""",
      Float.class, 0.05f),

  VECTOR_INDEX_GRAPH_BUILD_PARALLELISM("arcadedb.vectorIndex.graphBuildParallelism", SCOPE.DATABASE,
      """
      Number of threads in the dedicated pool that builds the HNSW graph of a vector index. Graph construction \
      is by far the most expensive part of building a dense index - on a 10M-vector corpus it is over 90% of the \
      total - and it parallelizes well, so this setting is the main lever on build time. \
      0 (default) sizes the pool automatically as the available cores minus one, which leaves a core for request, \
      I/O and GC threads so a rebuild triggered on a live index cannot starve concurrent query traffic. \
      Raise it to the full core count when build time matters more than query headroom, for example during a bulk \
      import; lower it to protect a latency-sensitive workload from an online rebuild. A value above the core count \
      only oversubscribes a CPU-bound phase and is logged as a warning; anything above what ForkJoinPool accepts is \
      clamped rather than failing the build.""",
      Integer.class, 0),

  VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD("arcadedb.vectorIndex.mutationsBeforeRebuild", SCOPE.DATABASE,
      """
      Number of mutations (inserts/updates/deletes) before rebuilding the HNSW graph index. \
      Higher values reduce rebuild cost but may return slightly stale results in queries. \
      Lower values provide fresher results but rebuild more frequently. \
      Recommended: 50-200 for read-heavy, 200-500 for write-heavy workloads.""",
      Integer.class, 100),

  VECTOR_INDEX_REBUILD_GRAPH_RATIO("arcadedb.vectorIndex.rebuildGraphRatio", SCOPE.DATABASE,
      """
      Fraction of the current graph size that must accumulate as pending mutations before the HNSW graph is \
      rebuilt, on top of the absolute mutationsBeforeRebuild floor. A rebuild always re-indexes the whole graph, \
      so a fixed absolute threshold makes it cost O(index size) for every few new vectors and turns bulk \
      ingestion quadratic. Scaling the threshold with the graph amortizes rebuilds geometrically. \
      Pending vectors stay exactly searchable through the in-memory delta buffer meanwhile, so a higher ratio \
      trades a slightly longer per-query delta scan for far less rebuild CPU. Set to 0 to disable scaling and \
      use only the absolute threshold.""",
      Float.class, 0.2f),

  VECTOR_INDEX_MAX_PENDING_MUTATIONS("arcadedb.vectorIndex.maxPendingMutations", SCOPE.DATABASE,
      """
      Hard ceiling on the rebuild threshold computed from rebuildGraphRatio. Set to 0 for no ceiling. \
      This caps the RATIO-DERIVED term only, and it is read where a rebuild is decided, not where the delta \
      buffer grows, so it does NOT bound that buffer: writes keep appending between rebuilds regardless, and \
      rebuilds are asynchronous, so sustained ingest outruns them. Measured with this set to 500, the buffer \
      reached 45,000 entries, and 42,504 even with the rebuild trigger firing continuously. Note also that \
      mutationsBeforeRebuild is applied as a floor afterwards, so an explicit value above this ceiling wins. \
      Nothing here bounds the buffer. mutationsBeforeRebuild and rebuildGraphRatio only change how OFTEN a \
      rebuild drains it, so its peak follows from how much is written between rebuilds. \
      This ceiling is also a fixed count, so it stops scaling once rebuildGraphRatio x graph size reaches it \
      (at the defaults, 250,000 vectors), from which point the same absolute scan cost lands on an index of any \
      size (issue #6797). maxDeltaScanRatio is the size-independent bound on that cost and is the knob to reach \
      for when query latency, rather than rebuild frequency, is what needs protecting.""",
      Integer.class, 50_000),

  VECTOR_INDEX_MAX_DELTA_SCAN_RATIO("arcadedb.vectorIndex.maxDeltaScanRatio", SCOPE.DATABASE,
      """
      How much brute-force work a query may spend on the in-memory delta buffer, expressed as a multiple of the \
      work its HNSW graph walk already does, before a rebuild is triggered to drain the buffer. Vectors ingested \
      since the last rebuild are answered by a linear scan of that buffer, so this term of a query grows with the \
      buffer while the graph walk it supplements grows only logarithmically with the corpus: at the default \
      maxPendingMutations the scan has been measured at four fifths of query time (issue #6797). The count-based \
      thresholds cannot bound it - mutationsBeforeRebuild and rebuildGraphRatio are denominated in mutations and \
      maxPendingMutations is a fixed number that stops scaling - so this one is denominated in the quantity that \
      actually matters and is measured, not assumed: the engine records how many nodes its graph walks actually \
      visit and compares the buffer against that. 1.0 (the default) lets the scan cost about as much as the walk. \
      Lower it for latency-sensitive read-heavy workloads, raise it to defer rebuilds further, set 0 to disable \
      and leave the count thresholds as the only trigger. \
      Because it is evaluated on the search path against a measured walk cost, a pure-ingest workload never \
      triggers it and keeps the geometric rebuild amortization of rebuildGraphRatio intact; only a workload that \
      is actually paying the scan pays for the extra rebuilds that remove it. The absolute mutationsBeforeRebuild \
      floor still applies, so this can never rebuild more eagerly than that setting allows. \
      A second condition guards it, so that a buffer refilling immediately after a rebuild cannot ask for \
      another one straight away: the brute-force work the scans have actually performed since the last build \
      must have reached what the next build is estimated to cost, both counted in similarity computations. \
      That bounds the extra rebuild CPU this setting can introduce by the query CPU it removes, at any index \
      size and any ingest rate. Both conditions are reported by the index statistics as deltaScanBudget, \
      deltaScanWorkSinceRebuild and estimatedRebuildWork. \
      Note that this is on by default, so an existing deployment that is both query-heavy and ingest-heavy will \
      see more background rebuild activity after upgrading - that being the point, since it is the same \
      deployment that was losing the query time. Set 0 to keep the previous behaviour exactly. \
      Note also that a query answered by the narrow-allow-list pre-filter plan never walks the graph, so it \
      produces nothing to measure and leaves the budget unset; an index queried only that way is not covered by \
      this setting and falls back on the count thresholds.""",
      Float.class, 1.0f),

  VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS("arcadedb.vectorIndex.inactivityRebuildTimeoutMs", SCOPE.DATABASE,
      """
      Inactivity timeout in milliseconds before flushing buffered vectors and rebuilding the HNSW graph. \
      When mutations exist but have not reached the rebuild threshold, a timer starts after the last mutation. \
      If no new mutations arrive within this window, the graph is rebuilt asynchronously. \
      Set to 0 to disable (vectors are only flushed when the mutation threshold is reached). \
      Recommended: 10000-30000 for low-volume ingestion workloads.""",
      Integer.class, 15_000),

    VECTOR_INDEX_GRAPH_BUILD_DIAGNOSTICS("arcadedb.vectorIndex.graphBuildDiagnostics", SCOPE.DATABASE,
      """
      Enable diagnostic logging during vector graph build progress (heap/off-heap memory and index file sizes). \
      This provides visibility during graph construction; disable if any logging overhead is a concern.""",
        Boolean.class, true),

  VECTOR_INDEX_MAX_CONCURRENT_REBUILDS("arcadedb.vectorIndex.maxConcurrentRebuilds", SCOPE.JVM,
      """
      Maximum number of vector index graph rebuilds that can run concurrently across all databases. \
      Concurrent rebuilds are memory-intensive; running too many in parallel can cause OOM kills. \
      Set to 1 to serialize all rebuilds (safest for memory). Higher values trade memory for throughput.""",
      Integer.class, 1),

  VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT("arcadedb.vectorIndex.rebuildMaxHeapPercent", SCOPE.DATABASE,
      """
      Share of the currently AVAILABLE heap (percentage) that an online vector graph rebuild's estimated peak \
      footprint may occupy before the rebuild is deferred instead of attempted. An online rebuild keeps the old \
      graph resident so searches keep working, and pays for a full new build's working set on top of it, so it \
      costs roughly 1.7x what building the same corpus from nothing costs. With no gate at all it simply attempts \
      the rebuild and dies with an OutOfMemoryError when it does not fit. A deferred cycle is not lost: the next \
      mutation-threshold or inactivity trigger retries it, and pending vectors stay exactly searchable through the \
      in-memory delta buffer meanwhile, so the cost of deferring is a longer delta scan per query rather than \
      wrong or missing results. The estimate is deliberately coarse, so the default is generous: it refuses only \
      when a rebuild is confidently too large, not whenever one looks tight. Applies to online rebuilds only - a \
      first build, a rebuild on close, an explicit REBUILD INDEX and a COMPACT INDEX are never declined. \
      Set to 0 to disable the gate and restore the attempt-and-hope behaviour. Values above 90 are clamped to 90: \
      a rebuild is never allowed to plan on the whole heap, since the request, I/O and GC threads need some too.""",
      Integer.class, 90),

  VECTOR_INDEX_REBUILD_DEFERRAL_COOLDOWN_MS("arcadedb.vectorIndex.rebuildDeferralCooldownMs", SCOPE.DATABASE,
      """
      How long, in milliseconds, an online vector graph rebuild that was deferred for lack of heap \
      (see arcadedb.vectorIndex.rebuildMaxHeapPercent) waits before another one may be attempted. \
      A deferral does not consume the pending mutations that triggered it - only a successful build does - so the \
      trigger condition is still true the instant the deferred cycle ends. Without a cooldown the next search \
      re-triggers immediately, and since a search checks on every query, a large heap-constrained index would \
      spawn a rebuild thread, take and release the JVM-wide rebuild permit and log a warning once PER QUERY: \
      thread churn and lock contention added precisely when the JVM is already short of memory, which works \
      against the deferral's own purpose. \
      The wait is not lost time: pending vectors stay exactly searchable through the in-memory delta buffer \
      throughout, so the cost is a slightly longer delta scan per query, which is what a deferral costs anyway. \
      Set to 0 to retry as soon as the next trigger fires.""",
      Integer.class, 30_000),

  VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS("arcadedb.vectorIndex.rebuildPermitTimeoutMs", SCOPE.JVM,
      """
      Maximum time in milliseconds an async vector index rebuild waits to acquire a JVM-wide rebuild permit \
      (see arcadedb.vectorIndex.maxConcurrentRebuilds) before giving up on that rebuild cycle. Without this \
      bound, a single rebuild that never returns its permit (e.g. one whose worker threads do not respond to \
      interruption) would starve every other vector index's rebuild across the whole process indefinitely. \
      A skipped cycle is not lost: the next mutation-threshold or inactivity trigger retries it.""",
      Integer.class, 600_000),

  // NETWORK
  NETWORK_SAME_SERVER_ERROR_RETRIES("arcadedb.network.sameServerErrorRetry", SCOPE.SERVER,
      "Number of automatic retries in case of IO errors with a specific server. If replica servers are configured, look also at HA_ERROR_RETRY setting. 0 (default) = no retry",
      Integer.class, 0),

  NETWORK_SOCKET_TIMEOUT("arcadedb.network.socketTimeout", SCOPE.SERVER, "TCP/IP Socket timeout (in ms)", Integer.class, 30000),

  NETWORK_SOCKET_KEEP_ALIVE("arcadedb.network.socketKeepAlive", SCOPE.SERVER, """
      Enable TCP keepalive (SO_KEEPALIVE) on every wire-protocol socket. The Postgres and Redis executors drop the
      socket read timeout to infinite once a connection is authenticated, because an authenticated client legitimately
      holds an idle connection open, and those protocols carry no application-level heartbeat. With keepalive off, a
      peer that dies without a FIN/RST (host crash, silent partition) leaves the server thread blocked in read()
      forever, leaking a thread and a file descriptor per event. Keepalive lets the OS discover the dead peer and fail
      the read.""", Boolean.class, true),

  NETWORK_SOCKET_KEEP_ALIVE_IDLE("arcadedb.network.socketKeepAliveIdle", SCOPE.SERVER, """
      Seconds an authenticated connection may sit idle before the OS sends the first TCP keepalive probe. Only applied
      when the JDK and the platform expose TCP_KEEPIDLE (Linux and macOS do); elsewhere the system-wide default
      applies, which is typically 2 hours. 0 leaves the system default in place.""", Integer.class, 120),

  NETWORK_SOCKET_KEEP_ALIVE_INTERVAL("arcadedb.network.socketKeepAliveInterval", SCOPE.SERVER, """
      Seconds between TCP keepalive probes once the first one has gone unanswered (TCP_KEEPINTERVAL). 0 leaves the
      system default in place.""", Integer.class, 15),

  NETWORK_SOCKET_KEEP_ALIVE_COUNT("arcadedb.network.socketKeepAliveCount", SCOPE.SERVER, """
      Number of unanswered TCP keepalive probes after which the connection is declared dead (TCP_KEEPCOUNT). With the
      defaults a dead peer is detected about 3 minutes after the connection goes idle. 0 leaves the system default in
      place.""", Integer.class, 4),

  NETWORK_MAX_PREAUTH_CONNECTIONS("arcadedb.network.maxPreAuthConnections", SCOPE.SERVER, """
      Maximum number of connections a binary wire-protocol listener (Postgres, Redis, BOLT) may hold in the phase
      before authentication. Each accepted socket costs one thread and one file descriptor before the client has
      proved who it is, and the handshake timeout (arcadedb.network.socketTimeout) only bounds how long each one
      may stay there, not how many there can be. Past this cap the listener closes further connections
      immediately and goes back to accepting. The cap is per listener, so a flood against one protocol cannot use
      up the budget that lets clients of another log in. 0 means unlimited.""", Integer.class, 500),

  NETWORK_USE_SSL("arcadedb.ssl.enabled", SCOPE.SERVER, "Use SSL for client connections", Boolean.class, false),

  NETWORK_SSL_KEYSTORE("arcadedb.ssl.keyStore", SCOPE.SERVER, "Path where the SSL certificates are stored", String.class, null),

  NETWORK_SSL_KEYSTORE_PASSWORD("arcadedb.ssl.keyStorePassword", SCOPE.SERVER, "Password to open the SSL key store", String.class,
      null),

  NETWORK_SSL_TRUSTSTORE("arcadedb.ssl.trustStore", SCOPE.SERVER, "Path to the SSL trust store", String.class, null),

  NETWORK_SSL_TRUSTSTORE_PASSWORD("arcadedb.ssl.trustStorePassword", SCOPE.SERVER, "Password to open the SSL trust store",
      String.class, null),

  // SERVER
  SERVER_NAME("arcadedb.server.name", SCOPE.SERVER, "Server name", String.class, Constants.PRODUCT + "_0"),

  SERVER_ROOT_PASSWORD("arcadedb.server.rootPassword", SCOPE.SERVER,
      "Password for root user to use at first startup of the server. Set this to avoid asking the password to the user",
      String.class, null),

  SERVER_ROOT_PASSWORD_PATH("arcadedb.server.rootPasswordPath", SCOPE.SERVER,
      "Path to file with password for root user to use at first startup of the server. Set this to avoid asking the password to the user",
      String.class, null),

  SERVER_MODE("arcadedb.server.mode", SCOPE.SERVER, "Server mode between 'development', 'test' and 'production'", String.class,
      "development", Set.of((Object[]) new String[]{"development", "test", "production"})),

  STUDIO_ENABLED("arcadedb.studio.enabled", SCOPE.SERVER,
      """
      Force-enable the Studio web tool (static content) even when the server runs in 'production' mode. In 'development' and \
      'test' mode Studio is always served; in 'production' mode it is disabled by default and this setting can re-enable it""",
      Boolean.class, false),

  SERVER_SHUTDOWN_TIMEOUT("arcadedb.server.shutdownTimeout", SCOPE.SERVER,
      """
      Milliseconds the JVM shutdown hook waits for the server lifecycle lock before giving up and letting \
      the JVM exit WITHOUT a graceful stop. It only matters when another thread is inside start()/stop() \
      when the shutdown signal arrives: normally the hook takes the lock immediately and this value is \
      never reached. Giving up leaves databases as a kill would - the next open replays the WAL - which is \
      the lesser evil, because a hook that waits forever can make the process unkillable (issue #5418). \
      Raise it if a legitimate shutdown of very large databases needs longer than the default.""",
      Long.class, 60_000L),

  // Metrics
  SERVER_METRICS("arcadedb.serverMetrics", SCOPE.SERVER, "True to enable metrics", Boolean.class, true),

  SERVER_METRICS_LOGGING("arcadedb.serverMetrics.logging", SCOPE.SERVER, "True to enable metrics logging", Boolean.class, false),

  SERVER_METRICS_TRACING_ENABLED("arcadedb.serverMetrics.tracing.enabled", SCOPE.SERVER,
      "Enable OpenTelemetry distributed tracing (requires the optional tracing plugin on the classpath). Note: query/command spans include the statement text as the db.statement span attribute, which may contain sensitive data, so secure the OTLP collector endpoint",
      Boolean.class, false),

  SERVER_METRICS_TRACING_ENDPOINT("arcadedb.serverMetrics.tracing.endpoint", SCOPE.SERVER, "OTLP trace export endpoint", String.class,
      "http://localhost:4317"),

  SERVER_METRICS_TRACING_SAMPLING_RATE("arcadedb.serverMetrics.tracing.samplingRate", SCOPE.SERVER,
      "Parent-based trace sampling ratio in [0.0,1.0]", Float.class, 0.0f),

  SERVER_READINESS_REQUIRES_HA("arcadedb.server.readinessRequiresHA", SCOPE.SERVER,
      "When true and HA is active, /api/v1/ready also requires the node to have joined the Raft group and be caught up. Default false preserves current readiness behavior.",
      Boolean.class, false),

  SERVER_READINESS_HA_MAX_LAG("arcadedb.server.readinessHAMaxLag", SCOPE.SERVER,
      "When SERVER_READINESS_REQUIRES_HA is true, the maximum number of Raft log entries a follower may lag behind the commit index (commitIndex - lastAppliedIndex) and still report Ready. Keeps /api/v1/ready returning 503 until a (re)joined follower has replayed the committed log, so a rolling restart does not drop the write quorum.",
      Long.class, 100L),

  SERVER_LOG_FORMAT("arcadedb.server.logFormat", SCOPE.SERVER,
      "Console log format: 'text' (default, human-readable) or 'json' (one JSON object per line with correlation fields)",
      String.class, "text"),

  SERVER_LOG_INCLUDE_TRACE("arcadedb.server.logIncludeTrace", SCOPE.SERVER,
      "In text log mode, append [traceId=...] to each line while a trace is active. Default false preserves current text output.",
      Boolean.class, false),

  //paths
  SERVER_ROOT_PATH("arcadedb.server.rootPath", SCOPE.SERVER,
      "Root path in the file system where the server is looking for files. By default is the current directory", String.class,
      null),

  // Default must stay in sync with DefaultLogger.DEFAULT_LOG_DIR so the resolver fallback and the config default agree.
  SERVER_LOGS_DIRECTORY("arcadedb.server.logsDirectory", SCOPE.JVM,
      "Directory where the server writes log files, referenced as ${arcadedb.server.logsDirectory} in arcadedb-log.properties. Defaults to './log'; set to an absolute writable path for read-only root filesystems.",
      String.class, "./log"),

  SERVER_DATABASE_DIRECTORY("arcadedb.server.databaseDirectory", SCOPE.JVM, "Directory containing the database", String.class,
      "${arcadedb.server.rootPath}/databases"),

  SERVER_BACKUP_DIRECTORY("arcadedb.server.backupDirectory", SCOPE.JVM, "Directory containing the backups", String.class,
      "${arcadedb.server.rootPath}/backups"),

  SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS("arcadedb.server.restoreImportAllowLocalUrls", SCOPE.SERVER,
      "Allow the 'restore database' and 'import database' server commands to fetch from local-file ('file://') URLs and from private, loopback or link-local network hosts. Disabled by default to prevent SSRF and local-file-read via a client-supplied URL; enable only when the operator explicitly trusts these sources",
      Boolean.class, false),

  SERVER_DATABASE_LOADATSTARTUP("arcadedb.server.databaseLoadAtStartup", SCOPE.SERVER,
      "Open all the available databases at server startup", Boolean.class, true),

  SERVER_DEFAULT_DATABASES("arcadedb.server.defaultDatabases", SCOPE.SERVER, """
      The default databases created when the server starts. The format is `(<database-name>[(<user-name>:<user-passwd>[:<user-group>])[,]*])[{import|restore:<URL>}][;]*'. Pay attention on using `;`\
       to separate databases and `,` to separate credentials. The supported actions are `import` and `restore`. Example: `Universe[albert:einstein:admin];Amiga[Jay:Miner,Jack:Tramiel]{import:/tmp/movies.tgz}`""",
      String.class, ""),

  SERVER_DEFAULT_DATABASE_MODE("arcadedb.server.defaultDatabaseMode", SCOPE.SERVER, """
      The default mode to load pre-existing databases. The value must match a com.arcadedb.engine.PaginatedFile.MODE enum value: {READ_ONLY, READ_WRITE}\
      Databases which are newly created will always be opened READ_WRITE.""", String.class, "READ_WRITE",
      Set.of((Object[]) new String[]{"read_only", "read_write"})),

  SERVER_PLUGINS("arcadedb.server.plugins", SCOPE.SERVER,
      "List of server plugins to install. The format to load a plugin is: `<pluginName>:<pluginFullClass>`", String.class, ""),

  // SERVER HTTP
  SERVER_HTTP_INCOMING_HOST("arcadedb.server.httpIncomingHost", SCOPE.SERVER, "TCP/IP host name used for incoming HTTP connections",
      String.class, "0.0.0.0"),

  SERVER_HTTP_INCOMING_PORT("arcadedb.server.httpIncomingPort", SCOPE.SERVER,
      "TCP/IP port number used for incoming HTTP connections. Specify a single port or a range `<from-<to>`. Default is 2480-2489 to accept a range of ports in case they are occupied.",
      String.class, "2480-2489"),

  SERVER_HTTPS_INCOMING_PORT("arcadedb.server.httpsIncomingPort", SCOPE.SERVER,
      "TCP/IP port number used for incoming HTTPS connections. Specify a single port or a range `<from-<to>`. Default is 2490-2499 to accept a range of ports in case they are occupied.",
      String.class, "2490-2499"),

  SERVER_HTTP_IO_THREADS("arcadedb.server.httpsIoThreads", SCOPE.SERVER,
      "Number of threads to use in the HTTP servers. The default number for most of the use cases is 2 threads per cpus (or 1 per virtual core)",
      Integer.class, 0, null, value -> Runtime.getRuntime().availableProcessors()),

  SERVER_HTTP_WORKER_THREADS("arcadedb.server.httpWorkerThreads", SCOPE.SERVER,
      """
      Maximum number of worker threads used by the embedded Undertow HTTP server to process blocking requests. \
      Each idle thread reserves a stack (~512KB-1MB) and Thread metadata in heap, so lowering the value reduces \
      memory footprint on small deployments. Default is 500 to preserve the legacy behaviour; the 'low-ram' \
      profile lowers it to 16.""",
      Integer.class, 500),

  SERVER_HTTP_SESSION_EXPIRE_TIMEOUT("arcadedb.server.httpSessionExpireTimeout", SCOPE.SERVER,
      "Timeout in seconds for a HTTP session (managing a transaction) to expire. This timeout is computed from the latest command against the session",
      Long.class, 5), // 5 SECONDS DEFAULT

  SERVER_HTTP_AUTH_SESSION_EXPIRE_TIMEOUT("arcadedb.server.httpAuthSessionExpireTimeout", SCOPE.SERVER,
      "Timeout in seconds for a HTTP authentication session to expire. This timeout is computed from the latest request using the auth token. Default is 30 minutes",
      Long.class, 1800), // 30 MINUTES DEFAULT

  SERVER_HTTP_AUTH_SESSION_ABSOLUTE_TIMEOUT("arcadedb.server.httpAuthSessionAbsoluteTimeout", SCOPE.SERVER,
      "Absolute timeout in seconds for a HTTP authentication session to expire from its creation time, regardless of activity. Set to 0 to disable (unlimited). Default is 0 (disabled)",
      Long.class, 0), // 0 = DISABLED/UNLIMITED BY DEFAULT

  SERVER_HTTP_BODY_CONTENT_MAX_SIZE("arcadedb.server.httpBodyContentMaxSize", SCOPE.SERVER,
      "Maximum size in bytes for HTTP request body content. Set to -1 for unlimited size (WARNING: removes DoS protection). Default is 100MB",
      Long.class, 100L * 1024 * 1024), // 100MB DEFAULT

  SERVER_HTTP_QUERY_DEFAULT_LIMIT("arcadedb.server.httpQueryDefaultLimit", SCOPE.SERVER,
      """
      Default maximum number of rows the HTTP query/command endpoints serialize into a single response when \
      the caller states no limit of its own. A request that carries a `limit` field, and a query that carries \
      its own LIMIT clause, are both honored as written and are never capped by this value. When this default \
      does cut a result short the response reports `"truncated": true` next to `returned` and `limit`, and the \
      server logs a warning: the truncation is never silent (issue #5711). Set to -1 or 0 for unlimited \
      (WARNING: removes the protection against materializing an unbounded result set in memory). A value above \
      'arcadedb.server.httpQueryMaxResultRows' - including unlimited - is lowered to it, so the two settings \
      cannot disagree and a caller that states no limit is never refused, only truncated. Default is 20000""",
      Integer.class, 20_000),

  SERVER_HTTP_QUERY_MAX_RESULT_ROWS("arcadedb.server.httpQueryMaxResultRows", SCOPE.SERVER,
      """
      Hard ceiling on the number of rows the HTTP query/command endpoints materialize into a single response. \
      Where 'arcadedb.server.httpQueryDefaultLimit' caps only the callers that state no limit of their own, this \
      ceiling also bounds the ones that do: a request `limit` at or below it - and a LIMIT the query itself \
      carries - is honored as written, while a larger value, or an explicitly unlimited `limit` of -1/0, cannot \
      push a single response past it. A result that would exceed the ceiling fails the request with HTTP 413 \
      naming this setting, exactly as the gRPC unary ExecuteQuery path answers RESOURCE_EXHAUSTED, rather than \
      silently truncating: a truncated response indistinguishable from a complete one is the defect issue #5711 \
      fixed. A caller that states no limit at all is never refused: 'arcadedb.server.httpQueryDefaultLimit' is \
      itself lowered to this ceiling, so such a caller gets the ordinary reported truncation instead. Note that \
      this bounds the SIZE of a response, not the peak cost of refusing one - the rows are serialized up to the \
      ceiling before the result is found to exceed it - nor the work a command does before returning: a write \
      that returns its rows applies to every matching record, and is then rolled back with the refusal. Set to \
      -1 or 0 for unlimited (WARNING: removes the protection against materializing an unbounded result set in \
      memory). Default is 1000000""",
      Integer.class, 1_000_000),

  SERVER_HTTP_STREAMING_READ_TIMEOUT("arcadedb.server.httpStreamingReadTimeout", SCOPE.SERVER,
      """
      Budget in milliseconds granted to endpoints that consume the request body while working (today only \
      the bulk-load /api/v1/batch endpoint). Undertow kills a connection when no read() is issued for \
      'arcadedb.network.socketTimeout' milliseconds, which on a streaming upload also counts the time the \
      server spends committing instead of reading: a long index compaction or the replication of a large \
      entry then aborts the upload mid-stream (issue #5470). This setting only relaxes that asynchronous \
      watchdog; a client that really stops sending is still cut off after 'arcadedb.network.socketTimeout' \
      because each blocking read keeps its own timeout. Set to 0 to disable the relaxation. Default is 10 minutes""",
      Integer.class, 600_000), // 10 MINUTES DEFAULT

  // SERVER gRPC
  SERVER_GRPC_QUERY_MAX_RESULT_ROWS("arcadedb.server.grpcQueryMaxResultRows", SCOPE.SERVER,
      """
      Hard ceiling on the number of rows the gRPC unary ExecuteQuery materializes. A request limit at or below \
      this cap is honored; a result that would exceed it fails the call with RESOURCE_EXHAUSTED (consistent with \
      the StreamQuery MATERIALIZE_ALL path) rather than silently truncating, and a client cannot bypass it with a \
      larger limit. Bounds heap usage and protects against limitless-query DoS. The default is lower than \
      grpcStreamMaxMaterializedRows because the unary response is built and returned as a single gRPC message \
      (also bounded by the max inbound/outbound message size), whereas StreamQuery emits incrementally. \
      Set to -1 or 0 for unlimited (WARNING: removes DoS protection). Default is 100000.""",
      Integer.class, 100_000),

  SERVER_GRPC_STREAM_MAX_MATERIALIZED_ROWS("arcadedb.server.grpcStreamMaxMaterializedRows", SCOPE.SERVER,
      """
      Maximum number of rows the gRPC StreamQuery MATERIALIZE_ALL retrieval mode buffers in memory before \
      emitting. Exceeding the cap fails the call with RESOURCE_EXHAUSTED so clients fall back to CURSOR/PAGED \
      streaming instead of running the server out of memory. Set to -1 or 0 for unlimited (WARNING: removes DoS \
      protection). Default is 1000000.""",
      Integer.class, 1_000_000),

  SERVER_GRPC_STREAM_WRITE_TIMEOUT_MS("arcadedb.server.grpcStreamWriteTimeoutMs", SCOPE.SERVER,
      """
      Maximum time in milliseconds a gRPC StreamQuery worker waits for the client transport to become ready to \
      accept the next batch before aborting the stream. Prevents a slow or abandoned client from pinning the \
      worker thread (and the open ResultSet/transaction) indefinitely. Set to -1 to wait forever (WARNING: \
      removes DoS protection). Default is 60000 (60s).""",
      Long.class, 60_000L),

  // SERVER WS
  SERVER_WS_EVENT_BUS_QUEUE_SIZE("arcadedb.server.eventBusQueueSize", SCOPE.SERVER,
      "Size of the queue used as a buffer for unserviced database change events.", Integer.class, 1000),

  SERVER_WS_EVENT_BUS_MAX_PENDING_BYTES("arcadedb.server.eventBusMaxPendingBytes", SCOPE.SERVER, """
      Maximum number of bytes of change-stream frames that may be outstanding towards a single WebSocket subscriber
      before it is evicted. Frames are sent asynchronously, so a subscriber that never reads accumulates them in the
      server's send buffer: the producer-side queue is bounded but a slow consumer is charged to the server's heap,
      not to its own. Past this cap the subscription is dropped and the channel closed, which is what the client
      would experience anyway. 0 disables the cap (the pre-26.9.1 behaviour).""", Long.class, 16 * 1024 * 1024L),

  // SERVER SECURITY
  SERVER_SECURITY_ALGORITHM("arcadedb.server.securityAlgorithm", SCOPE.SERVER,
      "Default encryption algorithm used for passwords hashing", String.class, "PBKDF2WithHmacSHA256"),

  SERVER_SECURITY_RELOAD_EVERY("arcadedb.server.reloadEvery", SCOPE.SERVER,
      "Time in milliseconds of checking if the server security files have been modified to be reloaded", Integer.class, 5_000),

  SERVER_SECURITY_SALT_CACHE_SIZE("arcadedb.server.securitySaltCacheSize", SCOPE.SERVER,
      "Cache size of hashed salt passwords. The cache works as LRU. Use 0 to disable the cache", Integer.class, 64),

  SERVER_SECURITY_SALT_ITERATIONS("arcadedb.server.saltIterations", SCOPE.SERVER,
      "Number of iterations to generate the salt or user password. Changing this setting does not affect stored passwords",
      Integer.class, 65536),

  SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS("arcadedb.server.security.importBlockLocalNetworks", SCOPE.SERVER,
      "When enabled (default), the SQL `IMPORT DATABASE` command refuses HTTP(S) URLs that resolve to loopback, link-local, "
          + "private (site-local), wildcard or multicast addresses. This mitigates Server-Side Request Forgery (SSRF) against "
          + "cloud metadata endpoints (e.g. 169.254.169.254) and internal services. Disable only in trusted environments that "
          + "legitimately import from internal hosts", Boolean.class, true),

  SERVER_SECURITY_IMPORT_ALLOWED_LOCAL_PATHS("arcadedb.server.security.importAllowedLocalPaths", SCOPE.SERVER,
      "Comma-separated list of directories the SQL `IMPORT DATABASE` command is allowed to read local files from (`file://` "
          + "and plain paths). When empty (default) no restriction is applied. When set, any import from a path outside the "
          + "listed directories is rejected, mitigating arbitrary local file read. `classpath://` resources are always allowed",
      String.class, ""),

  // HA
  HA_ENABLED("arcadedb.ha.enabled", SCOPE.SERVER, "True if HA is enabled for the current server", Boolean.class, false),

  HA_ERROR_RETRIES("arcadedb.ha.errorRetries", SCOPE.SERVER,
      "Number of automatic retries in case of IO errors with a specific server. If replica servers are configured, the operation will be retried a specific amount of times on the next server in the list. 0 (default) is to retry against all the configured servers",
      Integer.class, 0),

  HA_CLUSTER_NAME("arcadedb.ha.clusterName", SCOPE.SERVER,
      "Cluster name. By default is 'arcadedb'. Useful in case of multiple clusters in the same network", String.class,
      Constants.PRODUCT.toLowerCase(Locale.ENGLISH)),

  HA_SERVER_LIST("arcadedb.ha.serverList", SCOPE.SERVER,
      """
      Servers in the cluster, comma-separated. Each entry can use either the positional form \
      <hostname/ip-address:raftPort:httpPort[:priority[:httpsPort]]> or the more readable object form \
      <hostname/ip-address:{raft:2434,http:2480,https:2490,priority:10}> (fields unordered, all optional except raft defaults to the configured Raft port). Both forms may be mixed and prefixed with an optional 'name@'. \
      The httpPort is required for replica-to-leader HTTP command forwarding. \
      The optional priority (integer, default 0) sets the preferred leader: the node with the highest priority is preferred during elections. \
      The optional httpsPort is used for encrypted peer-to-peer transfers (e.g. snapshot download) when 'arcadedb.ssl.enabled' is true; when omitted on a homogeneous cluster it is derived from this node's local HTTPS listening port. \
      Examples: localhost:2434:2480:10:2490,192.168.0.1:2434:2480:0:2490 or localhost:{raft:2434,http:2480,https:2490,priority:10},192.168.0.1:{raft:2434,http:2480,https:2490}""",
      String.class, ""),

  HA_SERVER_ROLE("arcadedb.ha.serverRole", SCOPE.SERVER,
      """
      Enforces a role in a cluster. 'any' (default) means this node can be elected leader. \
      'replica' sets the Raft peer priority to 0 so the node is never elected leader \
      (useful for read-scale or witness deployments).""",
      String.class, "any", Set.of("any", "replica")),

  HA_QUORUM("arcadedb.ha.quorum", SCOPE.SERVER,
      """
      Write quorum: 'majority' (standard Raft, default) or 'all' (every configured peer must acknowledge). \
      Legacy values 'none', 'one', 'two', 'three' are no longer supported.""",
      String.class, "majority", Set.of("majority", "all")),

  HA_QUORUM_TIMEOUT("arcadedb.ha.quorumTimeout", SCOPE.SERVER, "Timeout waiting for the quorum", Long.class, 10000),

  HA_ELECTION_TIMEOUT_MIN("arcadedb.ha.electionTimeoutMin", SCOPE.SERVER,
      """
      Minimum election timeout in milliseconds: a follower starts a new election if it has not heard from \
      the leader for this many ms. Default of 5000ms is a balance between fast failover and resilience to \
      heartbeat blips under heavy ingest. Bump higher for WAN clusters or sustained bulk-load workloads where \
      leader appender threads compete with replication.""",
      Integer.class, 5000),

  HA_ELECTION_TIMEOUT_MAX("arcadedb.ha.electionTimeoutMax", SCOPE.SERVER,
      """
      Maximum election timeout in milliseconds. Default of 10000ms is a balance between fast failover and \
      resilience to heartbeat blips under heavy ingest. Bump higher for WAN clusters or sustained bulk-load \
      workloads where leader appender threads compete with replication.""",
      Integer.class, 10_000),

  HA_LOG_SEGMENT_SIZE("arcadedb.ha.logSegmentSize", SCOPE.SERVER,
      "Maximum Raft log segment size (e.g. '64MB', '128MB')", String.class, "64MB"),

  HA_APPEND_BUFFER_SIZE("arcadedb.ha.appendBufferSize", SCOPE.SERVER,
      """
      AppendEntries batch byte limit for replication (e.g. '32MB'). Ratis applies this limit per ENTRY as \
      well as per batch, so it is also the HARD MAXIMUM SIZE OF A SINGLE REPLICATED TRANSACTION - usually \
      lower than arcadedb.ha.grpcMessageSizeMax and therefore the limit that actually binds. An entry above \
      it is rejected with a state-machine error that makes the leader step down; the write then fails with \
      ReplicatedEntryTooLargeException naming this setting. The size compared against it is the COMPRESSED \
      WAL of the transaction, so text/JSON payloads shrink far below their raw size while incompressible \
      ones (binary blobs, base64, encrypted fields, float vectors) map roughly 1:1. Compression does NOT \
      make the transaction unbounded, though (issue #5933): every node has to materialize the WAL in full to \
      apply it, so a second, fixed ceiling of 64MB applies to the UNCOMPRESSED WAL of one entry regardless of \
      this setting, and a transaction above it is rejected with the same exception. Raise it when single \
      transactions or records are bigger than the default, and raise arcadedb.ha.writeBufferSize with it \
      (it must stay >= this value + 8 bytes). Cost of raising it: a directly-allocated write buffer of \
      writeBufferSize per server, plus up to this many bytes of heap per follower appender during catch-up. \
      Cost of LOWERING it (issue #6136): an index rebuild ships its WAL to the followers in instalments of half \
      this size, and each instalment is a quorum round trip taken while the database write lock is held, so a \
      smaller value means more round trips and a longer window in which every writer on that database waits - \
      up to arcadedb.ha.quorumTimeout per instalment if a quorum member is slow or briefly partitioned.""",
      String.class, "32MB"),

  HA_APPEND_ELEMENT_LIMIT("arcadedb.ha.appendElementLimit", SCOPE.SERVER,
      """
      Maximum number of Raft log entries per AppendEntries batch. Bounds the per-batch in-memory \
      footprint on the follower during catch-up resync, where many batches may queue before the \
      state machine can apply them. Lowering this value reduces peak heap pressure on followers \
      catching up from a far-behind state. The byte limit (arcadedb.ha.appendBufferSize) remains \
      the dominant per-batch heap bound; this element count is the secondary cap that governs when \
      entries are small enough that many fit under the byte limit. Must be a positive integer (>= 1).""",
      Integer.class, 64),

  HA_WRITE_BUFFER_SIZE("arcadedb.ha.writeBufferSize", SCOPE.SERVER,
      """
      Raft log write buffer size (e.g. '40MB'). Must be at least appendBufferSize + 8 bytes, otherwise the \
      server fails to start with ConfigurationException. Ratis allocates this as a DIRECT ByteBuffer, once \
      per server, so it is off-heap memory reserved at startup - keep it just above appendBufferSize rather \
      than generously oversized.""",
      String.class, "40MB"),

  HA_LOG_PURGE_GAP("arcadedb.ha.logPurgeGap", SCOPE.SERVER,
      """
      Number of Raft log entries retained after a snapshot as a buffer for slightly lagging followers. \
      Lower values free disk faster but raise the chance a slow follower needs a full snapshot resync.""",
      Integer.class, 1024),

  HA_LOG_PURGE_UPTO_SNAPSHOT("arcadedb.ha.logPurgeUptoSnapshot", SCOPE.SERVER,
      """
      When true (default), deletes old Raft log segments after each snapshot to bound disk growth. \
      Set to false to retain full log history for debugging/auditing.""",
      Boolean.class, true),

  HA_REPLICATION_CHUNK_MAXSIZE("arcadedb.ha.replicationChunkMaxSize", SCOPE.SERVER,
      "Maximum channel chunk size for replicating messages between servers. Default is 16777216", Integer.class, 16384 * 1024),

  // KUBERNETES
  HA_K8S("arcadedb.ha.k8s", SCOPE.SERVER, "The server is running inside Kubernetes", Boolean.class, false),

  HA_K8S_DNS_SUFFIX("arcadedb.ha.k8sSuffix", SCOPE.SERVER,
      "When running inside Kubernetes use this suffix to reach the other servers. Example: arcadedb.default.svc.cluster.local",
      String.class, ""),

  HA_READ_CONSISTENCY("arcadedb.ha.readConsistency", SCOPE.SERVER,
      "Default read consistency for follower reads: eventual, read_your_writes, linearizable",
      String.class, "read_your_writes",
      Set.of((Object[]) new String[] { "eventual", "read_your_writes", "linearizable" })),

  // RAFT HA
  HA_REPLICATION_LAG_WARNING("arcadedb.ha.replicationLagWarning", SCOPE.SERVER,
      "Raft log index gap threshold for replication lag warnings. When a replica falls behind by more than this many entries, a warning is logged",
      Long.class, 1000L),

  HA_RAFT_PORT("arcadedb.ha.raftPort", SCOPE.SERVER,
      "TCP/IP port for Raft gRPC communication. Used as the default port when HA_SERVER_LIST entries do not specify an explicit port",
      Integer.class, 2434),

  HA_RAFT_PERSIST_STORAGE("arcadedb.ha.raftPersistStorage", SCOPE.SERVER,
      """
      If true, the Raft storage directory is preserved across server restarts, enabling node rejoin \
      by replaying the persisted log instead of forcing a full snapshot resync. Defaults to true \
      (durable): wiping the Raft log on every restart turns a follower that was merely lagging into a \
      permanently diverged node (WAL version gaps) on a full-cluster cold restart. Set to false only \
      for throwaway/test clusters that intentionally want ephemeral storage.""",
      Boolean.class, true),

  HA_RAFT_STORAGE_DIRECTORY("arcadedb.ha.raftStorageDirectory", SCOPE.SERVER,
      """
      Parent directory where Raft storage sub-folders (raft-storage-<nodeName>) are created. \
      When empty (the default), Raft storage is placed under the database directory \
      (<databaseDirectory>/.raft-storage), so persisting the database directory - which every durable \
      deployment already does - persists the Raft log too. This avoids losing all Raft state on pod \
      recreation in Kubernetes, where only the database directory is on a PersistentVolume while the \
      server root path is ephemeral. A legacy raft-storage-<nodeName> directory already present under the \
      server root path (pre-fix layout) is still reused for backward compatibility. \
      Set to an absolute path (e.g. /var/lib/arcadedb/raft) to decouple Raft persistence from the \
      database directory, which is required for Kubernetes readOnlyRootFilesystem deployments with a \
      dedicated Raft volume.""",
      String.class, ""),

  HA_SNAPSHOT_THRESHOLD("arcadedb.ha.snapshotThreshold", SCOPE.SERVER,
      """
      Number of Raft log entries after which the leader automatically takes a snapshot. \
      Lower values cause more frequent snapshots and earlier log compaction.""",
      Long.class, 100_000L),

  HA_SNAPSHOT_INTERVAL("arcadedb.ha.snapshotInterval", SCOPE.SERVER,
      """
      Interval in milliseconds between periodic Raft snapshot checkpoints on every node. \
      HA_SNAPSHOT_THRESHOLD alone counts entries, so a low-write cluster can run for weeks without ever \
      reaching it: the snapshot index stays frozen, no log segment is ever purged, and the Raft log grows \
      until the volume is full. This time-based trigger bounds the retained log by wall-clock age instead. \
      An ArcadeDB snapshot is a zero-byte marker (the database files on disk are the durable state), so a \
      tick is cheap; it is additionally a no-op when fewer than HA_SNAPSHOT_MIN_ENTRIES entries were \
      applied since the last snapshot. Set to 0 to disable and rely on HA_SNAPSHOT_THRESHOLD only. \
      Note this interval also bounds the reaction time to disk pressure, not just steady-state log \
      retention: the free-space escalation described in HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC fires on the \
      next tick, so a volume that fills faster than one interval needs a shorter interval.""",
      Long.class, 300_000L),

  HA_SNAPSHOT_MIN_ENTRIES("arcadedb.ha.snapshotMinEntries", SCOPE.SERVER,
      """
      Minimum number of Raft log entries applied since the last snapshot before a periodic \
      HA_SNAPSHOT_INTERVAL tick actually takes one. Keeps an idle cluster from rewriting a snapshot marker \
      that would not advance the purge point. Values below 1 are clamped to 1.""",
      Long.class, 64L),

  HA_RAFT_STORAGE_MIN_FREE_SPACE_PERC("arcadedb.ha.raftStorageMinFreeSpacePerc", SCOPE.SERVER,
      """
      Percentage of free space on the volume hosting HA_RAFT_STORAGE_DIRECTORY below which the periodic \
      snapshot tick escalates: it forces a snapshot and log purge regardless of HA_SNAPSHOT_MIN_ENTRIES and \
      logs a throttled WARNING. Guards against the Raft log filling the volume, after which Ratis marks the \
      log permanently failed and the node rejects every append until restarted. Set to 0 to disable the check.""",
      Integer.class, 20),

  HA_LOG_VERBOSE("arcadedb.ha.logVerbose", SCOPE.SERVER,
      "HA verbose logging level: 0=off, 1=basic (elections, leader changes), 2=detailed (replication, forwarding), 3=trace (every state machine apply)",
      Integer.class, 0),

  HA_GROUP_COMMIT_BATCH_SIZE("arcadedb.ha.groupCommitBatchSize", SCOPE.SERVER,
      """
      Maximum number of Raft log entries to batch in a single group commit flush. \
      Higher values improve throughput under concurrent load.""",
      Integer.class, 500),

  HA_GROUP_COMMIT_QUEUE_SIZE("arcadedb.ha.groupCommitQueueSize", SCOPE.SERVER,
      """
      Maximum pending transactions allowed in the Raft group-commit queue. \
      When the queue is full, the server applies backpressure by throwing ReplicationQueueFullException \
      (a NeedRetryException that clients can retry).""",
      Integer.class, 10_000),

  HA_GROUP_COMMIT_OFFER_TIMEOUT("arcadedb.ha.groupCommitOfferTimeout", SCOPE.SERVER,
      "Timeout in ms waiting for space in the group-commit queue before throwing ReplicationQueueFullException.",
      Integer.class, 100),

  HA_GROUP_COMMIT_MAX_QUEUED_BYTES("arcadedb.ha.groupCommitMaxQueuedBytes", SCOPE.SERVER,
      """
      Maximum total bytes of pending (not-yet-dispatched) transactions allowed in the Raft group-commit \
      queue. This is a memory backpressure bound that complements the entry-count bound \
      (arcadedb.ha.groupCommitQueueSize): because a single transaction can be up to \
      arcadedb.ha.grpcMessageSizeMax (128MB by default), a count-only bound would let a flood of large \
      transactions exhaust the heap before backpressure engages. When adding a transaction would exceed \
      this byte budget, the server waits up to arcadedb.ha.groupCommitOfferTimeout and then throws \
      ReplicationQueueFullException (a retryable NeedRetryException) so heavy ingest backpressures \
      instead of running the leader out of memory. Must be at least arcadedb.ha.grpcMessageSizeMax so a \
      single maximum-size transaction can always be enqueued. Default 256MB.""",
      Long.class, 256L * 1024 * 1024),

  HA_CLUSTER_TOKEN("arcadedb.ha.clusterToken", SCOPE.SERVER,
      """
      Shared secret for inter-node request forwarding authentication. \
      Must be identical on all cluster nodes. \
      If empty, a random token is auto-generated and stored in raft-storage at startup. \
      SECURITY: set an explicit high-entropy value in production. When left empty the effective token may be \
      derived from the cluster name and root password with a fixed public salt, so a weak root password plus a \
      reachable replication HTTP port could let an attacker forge the token and impersonate the root user via \
      forwarded-user authentication. The replication HTTP port must never be exposed to untrusted networks.""",
      String.class, ""),

  HA_CLUSTER_TOKEN_PATH("arcadedb.ha.clusterTokenPath", SCOPE.SERVER,
      """
      Path to a file containing the shared secret for inter-node request forwarding authentication. \
      Used to keep the secret off the command line (e.g. a Kubernetes Secret mounted on tmpfs). \
      Read only when arcadedb.ha.clusterToken is not set; the file content is trimmed of surrounding whitespace.""",
      String.class, ""),

  HA_HEALTH_CHECK_INTERVAL("arcadedb.ha.healthCheckInterval", SCOPE.SERVER,
      "Interval in milliseconds for the Raft health monitor to check for CLOSED/EXCEPTION state and auto-recover. 0 disables.",
      Long.class, 3000L),

  HA_RESYNC_PROGRESS_LOGGING("arcadedb.ha.resyncProgressLogging", SCOPE.SERVER,
      """
      When true (default), the leader emits a concise per-follower unreachable/reconnected narrative and a \
      restarting follower logs its resync progress (Raft log catch-up and full snapshot download). Set to false \
      to disable that narrative. Note: this flag does NOT control the raw Apache Ratis retry flood - that is \
      suppressed unconditionally by the org.apache.ratis.grpc.server.GrpcLogAppender level in \
      arcadedb-log.properties, which is the switch to change to see those raw lines again.""",
      Boolean.class, true),

  HA_RESYNC_PROGRESS_INTERVAL("arcadedb.ha.resyncProgressInterval", SCOPE.SERVER,
      "Minimum interval in milliseconds between follower resync progress log lines (Raft log catch-up and snapshot download). Throttles progress output so a fast resync logs only start and finish.",
      Long.class, 5000L),

  HA_PEER_UNREACHABLE_THRESHOLD("arcadedb.ha.peerUnreachableThreshold", SCOPE.SERVER,
      "Time in milliseconds since the last successful RPC to a follower before the leader reports it as unreachable in the resync narrative. Does not change Raft membership or quorum.",
      Long.class, 10000L),

  HA_PEER_CHANNEL_RESET_DURATION("arcadedb.ha.peerChannelResetDuration", SCOPE.SERVER,
      "Time in milliseconds a follower must stay continuously unreachable (no successful RPC, beyond HA_PEER_UNREACHABLE_THRESHOLD) before the leader resets that one follower's replication gRPC channel, closing the wedged channel so the next send re-resolves DNS and reconnects. Recovers a leader appender channel stuck on a stale DNS result after a follower restarts with a new address (e.g. a Kubernetes pod-IP change, issue #4696) without a leadership transfer, so there is no flapping risk. Only the unreachable peer's channel is touched. While the follower stays unreachable the reset is retried once per interval, up to a small bounded number of attempts, after which the leader gives up and logs for operator intervention; the counter re-arms when the follower reconnects. Requires HA_PEER_UNREACHABLE_THRESHOLD > 0 (its 'unreachable' signal). Set to 0 to disable the automatic channel reset (the manual leadership transfer remains available).",
      Long.class, 60000L),

  HA_PEER_CHANNEL_RESET_ESCALATION("arcadedb.ha.peerChannelResetEscalation", SCOPE.SERVER,
      "When the bounded HA_PEER_CHANNEL_RESET_DURATION retry budget is exhausted and a follower's replication channel is still dead, transfer leadership to a healthy peer so the new leader builds a fresh appender to that follower (issue #5346). Without it the leader stays wedged until an operator restarts the process, because the reset streak only re-arms when the follower becomes reachable again. The target is chosen with the same rules as a manual step-down and is never the wedged follower itself; when no healthy target exists the leader keeps the previous behaviour and logs for operator intervention. When the follower is unreachable for a reason a fresh appender cannot fix, each healthy peer escalates it at most once per 30-minute cooldown before the cluster settles on the operator-intervention path, so the leadership churn is bounded rather than perpetual. Set to false to only log.",
      Boolean.class, true),

  HA_RESYNC_CATCHUP_LAG_THRESHOLD("arcadedb.ha.resyncCatchupLagThreshold", SCOPE.SERVER,
      "Minimum apply backlog (Raft log entries a follower has committed/received but not yet applied to its state machine) before the catch-up resync narrative is logged. This is a locally observable signal, not the distance from the leader's commit index. Keeps the small steady-state apply backlog under write load from being narrated; only a genuine post-restart burst crosses this threshold. The narrative finishes once the backlog drains to within a tenth of it.",
      Long.class, 1000L),

  HA_GRPC_FLOW_CONTROL_WINDOW("arcadedb.ha.grpcFlowControlWindow", SCOPE.SERVER,
      "gRPC flow control window size in bytes for Ratis append-entries traffic. Larger values help catch-up replication after partitions.",
      Long.class, 4L * 1024 * 1024),

  HA_GRPC_MESSAGE_SIZE_MAX("arcadedb.ha.grpcMessageSizeMax", SCOPE.SERVER,
      """
      Maximum size in bytes of a single Raft gRPC message (a replicated transaction or schema entry). \
      Defaults to 128MB, higher than Ratis's 64MB stock default, so reasonable bulk-load batches do not get rejected. \
      Lower it to bound memory exposure on hostile inputs; raise it if a single transaction legitimately exceeds 128MB.""",
      Long.class, 128L * 1024 * 1024),

  HA_BOOTSTRAP_FROM_LOCAL_DATABASE("arcadedb.ha.bootstrapFromLocalDatabase", SCOPE.SERVER,
      """
      When true (the default) and every peer's Raft log is empty at first cluster formation, peers exchange a \
      (fingerprint, lastTxId) tuple per database; the peer with the highest lastTxId is elected as the bootstrap \
      source via leadership transfer, and the others either bootstrap locally (matching fingerprint) or \
      catch up via the existing leader-shipped snapshot path. Lets operators pre-stage 1+GB databases on every \
      pod (init container, image bake, NFS) so the cluster forms in seconds instead of waiting on HTTP snapshot \
      transfer. Safe to leave on: gating on empty Raft log + fingerprint check rules out silent divergence.""",
      Boolean.class, true),

  HA_BOOTSTRAP_TIMEOUT_MS("arcadedb.ha.bootstrapTimeoutMs", SCOPE.SERVER,
      """
      Maximum time in milliseconds the bootstrap leader waits for every configured peer to report its \
      (fingerprint, lastTxId) before falling back to majority. A SEVERE log is emitted on timeout so the operator \
      knows which peer was unreachable.""",
      Long.class, 120_000L),

  HA_AUTO_ACQUIRE_DATABASES("arcadedb.ha.autoAcquireDatabases", SCOPE.SERVER,
      """
      When true (the default), a node that joins the cluster reconciles its local database set against the leader's \
      and auto-pulls (full snapshot install) any database it has never seen on disk - so an empty/new node \
      (e.g. a StatefulSet scaled up) becomes a full replica with zero manual steps. When false, the node only \
      refreshes databases already present locally (the legacy behavior) and never acquires unseen ones. This is a \
      per-node local policy, read live on each reconcile (not stored in Raft); acquisition is additive and never \
      drops a database the leader is missing, so a mixed cluster is safe. Note: a database whose snapshot \
      persistently fails to install is retried up to a small bounded number of times, and because a failed install \
      makes Ratis re-trigger the whole InstallSnapshot, each retry re-downloads the other databases on this node \
      too; the retry count is capped so this cannot loop indefinitely.""",
      Boolean.class, true),

  HA_SNAPSHOT_MAX_CONCURRENT("arcadedb.ha.snapshotMaxConcurrent", SCOPE.SERVER,
      "Maximum number of concurrent snapshot downloads served by the leader. Requests over this limit receive HTTP 503.",
      Integer.class, 2),

  HA_SNAPSHOT_DOWNLOAD_TIMEOUT("arcadedb.ha.snapshotDownloadTimeout", SCOPE.SERVER,
      "Read timeout in ms for downloading a database snapshot from the leader during follower resync.",
      Integer.class, 300_000),

  HA_SNAPSHOT_INSTALL_RETRIES("arcadedb.ha.snapshotInstallRetries", SCOPE.SERVER,
      "Maximum retry attempts for snapshot download from the leader during snapshot installation.",
      Integer.class, 3),

  HA_SNAPSHOT_INSTALL_RETRY_BASE_MS("arcadedb.ha.snapshotInstallRetryBaseMs", SCOPE.SERVER,
      "Base delay in milliseconds for exponential backoff between snapshot download retries. Actual delay is baseMs * 2^attempt.",
      Long.class, 5000L),

  HA_PROXY_READ_TIMEOUT("arcadedb.ha.proxyReadTimeout", SCOPE.SERVER,
      "Read timeout in milliseconds for the leader proxy in AbstractServerHttpHandler. Covers long-running queries proxied from a follower to the leader.",
      Long.class, 30000L),

  HA_PROXY_CONNECT_TIMEOUT("arcadedb.ha.proxyConnectTimeout", SCOPE.SERVER,
      "Connect timeout in milliseconds for the leader proxy in AbstractServerHttpHandler.",
      Long.class, 5000L),

  HA_PROXY_MAX_BODY_SIZE("arcadedb.ha.proxyMaxBodySize", SCOPE.SERVER,
      "Maximum request body size in bytes that the leader proxy will buffer and forward. Larger requests fall back to HTTP 400.",
      Integer.class, 16 * 1024 * 1024),

  HA_CLIENT_ELECTION_RETRY_COUNT("arcadedb.ha.clientElectionRetryCount", SCOPE.SERVER,
      "Number of retries performed by RemoteDatabase after receiving HTTP 503 NeedRetryException during an election.",
      Integer.class, 3),

  HA_CLIENT_ELECTION_RETRY_DELAY_MS("arcadedb.ha.clientElectionRetryDelayMs", SCOPE.SERVER,
      "Delay in milliseconds between RemoteDatabase election retries.",
      Long.class, 2000L),

  HA_FORWARD_LEADER_WAIT_TIMEOUT_MS("arcadedb.ha.forwardLeaderWaitTimeoutMs", SCOPE.SERVER,
      """
      Maximum time in milliseconds a follower waits for a leader to be (re)elected before failing a write \
      command it has to forward to the leader. During cluster startup or a leader change there is a window \
      with no elected leader; without this wait a forwarded write fails immediately with "leader HTTP address \
      is not available" and the caller's transaction is lost (issue #4728 follow-up). The follower polls for \
      the leader and forwards as soon as one appears. Set to 0 to restore the previous fail-fast behavior. \
      Default 20000 comfortably covers a first-election window (which can exceed 10s on cluster startup).""",
      Long.class, 20000L),

  HA_RATIS_RESTART_MAX_RETRIES("arcadedb.ha.ratisRestartMaxRetries", SCOPE.SERVER,
      """
      Maximum consecutive Ratis restart attempts by the health monitor before the server shuts down \
      for cluster-level recovery. Raise when partition-recovery scenarios cause legitimate rapid restarts. \
      Also bounds the crash-loop escalation: when a RECOVER restart keeps returning to CLOSED (e.g. a \
      term-inverted persisted Raft log or a poisoned snapshot-install) without the restart itself failing, \
      the health monitor escalates after this many non-sticking restarts (reformat + rejoin once, then give \
      up with a SEVERE alert) instead of restarting forever (issue #5291).""",
      Integer.class, 10),

  HA_STOP_SERVER_ON_REPLICATION_FAILURE("arcadedb.ha.stopServerOnReplicationFailure", SCOPE.SERVER,
      """
      After a phase-2 local commit fails on the leader while followers have applied the entry, step-down \
      is attempted first. If every step-down fails and this flag is true, the JVM exits so an \
      orchestrator can restart and let Raft log replay correct the state. \
      Default is false: the server keeps running and logs CRITICAL, useful for debugging without an orchestrator.""",
      Boolean.class, false),

  HA_SNAPSHOT_WRITE_TIMEOUT("arcadedb.ha.snapshotWriteTimeout", SCOPE.SERVER,
      """
      Idle timeout in milliseconds for writing a snapshot to a follower. The connection is force-closed \
      to free the semaphore slot only when NO bytes have been written for this duration (a stall), not on \
      total transfer time, so a large but actively-progressing snapshot is never killed mid-stream.""",
      Long.class, 300_000L),

  HA_TS_MAX_SEALED_INLINE_SIZE("arcadedb.ha.tsMaxSealedInlineSize", SCOPE.SERVER,
      """
      Maximum size in bytes of a TimeSeries sealed-store file that may be shipped inline inside a single \
      Raft SCHEMA_ENTRY during compaction. When the projected sealed-store size would exceed this cap, the \
      leader skips compacting that shard (data stays in the fully replicated mutable bucket) instead of \
      producing an entry too large for the Raft transport. Always clamped down to the real per-entry ceiling, \
      min(arcadedb.ha.grpcMessageSizeMax, arcadedb.ha.appendBufferSize), so a value above that has no effect.""",
      Long.class, 48 * 1024 * 1024L),

  HA_SNAPSHOT_WATCHDOG_TIMEOUT("arcadedb.ha.snapshotWatchdogTimeout", SCOPE.SERVER,
      """
      Delay in milliseconds before the snapshot-gap watchdog triggers a download. \
      Floored at 4x HA_ELECTION_TIMEOUT_MAX to avoid premature firing on WAN clusters.""",
      Long.class, 30_000L),

  HA_SNAPSHOT_GAP_TOLERANCE("arcadedb.ha.snapshotGapTolerance", SCOPE.SERVER,
      "Maximum acceptable gap between the snapshot index and persisted applied index before triggering a snapshot download.",
      Long.class, 10L),

  HA_STALE_FOLLOWER_LAG_THRESHOLD("arcadedb.ha.staleFollowerLagThreshold", SCOPE.SERVER,
      """
      Number of Raft log entries a follower may lag behind the commit index, while NOT actively catching up, before the \
      health monitor re-arms a snapshot download from the leader. Guards against a follower that diverged (apply failure) \
      and whose snapshot download also failed on a quiet cluster, where no new entry arrives to re-trigger recovery. \
      UPGRADE NOTE: this defaults to 10000 (was 0/disabled before 26.7.1), well below the default HA_SNAPSHOT_THRESHOLD \
      (100000), so a genuinely stuck follower self-heals without operator action. The value must stay below \
      HA_SNAPSHOT_THRESHOLD so recovery is attempted before the leader compacts the entries the follower still needs. \
      Set to 0 to restore the previous behaviour (follower-side stale recovery disabled; node restart is the only \
      mitigation) if a deployment prefers to avoid automatic snapshot downloads.""",
      Long.class, 10_000L),

  HA_STALE_FOLLOWER_RECOVERY_DURATION_MS("arcadedb.ha.staleFollowerRecoveryDurationMs", SCOPE.SERVER,
      """
      How long in milliseconds the lag described by HA_STALE_FOLLOWER_LAG_THRESHOLD must persist continuously \
      (across consecutive health-monitor ticks) before recovery is triggered. Avoids acting on transient catch-up lag.""",
      Long.class, 60_000L),

  HA_DIVERGED_FOLLOWER_RECOVERY("arcadedb.ha.divergedFollowerRecovery", SCOPE.SERVER,
      """
      When true (default), a follower that detects it is stuck at a stale term against the leader (it recognizes a leader \
      at a newer term and has applied everything it could locally commit, yet its last-applied entry is from an older \
      term) automatically reformats its Raft storage and rejoins as a fresh peer, letting the leader reconcile it via the \
      snapshot-install path. This covers issue #4741: a tiny (1-2 entry) Raft-log divergence on an otherwise idle \
      cluster, where the leader's log is never compacted, so neither the follower-side stale recovery \
      (HA_STALE_FOLLOWER_LAG_THRESHOLD) nor the leader-driven stalled-replica resync \
      (HA_STALLED_REPLICA_RESYNC_DURATION_MS) ever fire - both need a large lag - and the leader's appender otherwise \
      loops on INCONSISTENCY forever until an operator restarts a node. The stuck condition must persist for \
      HA_STALE_FOLLOWER_RECOVERY_DURATION_MS before recovery triggers, and HA_DIVERGED_FOLLOWER_MAX_REFORMATS bounds how \
      often it retries. \
      DESTRUCTIVE: this deletes the local Raft storage automatically (the database files are preserved and re-synced \
      from the leader). The signature is "stuck at a stale term", which a genuine log divergence satisfies but so can a \
      sustained (> HA_STALE_FOLLOWER_RECOVERY_DURATION_MS) one-sided network outage where heartbeats arrive but the \
      leader's current-term entries do not; in that case the reformat is wasteful (no data loss - the leader holds \
      everything) but does not fix the connectivity. \
      No cross-follower coordination: if a systemic condition makes several followers satisfy the signature at once they \
      may reformat within the same window, briefly costing quorum while they re-sync. This is bounded (each reformat is \
      non-data-losing and HA_DIVERGED_FOLLOWER_MAX_REFORMATS caps retries) and a leader-coordinated one-at-a-time variant \
      is deferred to a follow-up; set this to false to fall back to a manual node restart as the only #4741 mitigation.""",
      Boolean.class, true),

  HA_DIVERGED_FOLLOWER_MAX_REFORMATS("arcadedb.ha.divergedFollowerMaxReformats", SCOPE.SERVER,
      """
      Maximum number of automatic Raft-storage reformats (HA_DIVERGED_FOLLOWER_RECOVERY) allowed within one divergence \
      episode before the follower gives up and logs a SEVERE message for operator intervention, instead of reformatting \
      and full-snapshot-installing every HA_STALE_FOLLOWER_RECOVERY_DURATION_MS forever. A clean reformat resets the \
      shared Ratis restart-retry budget, so without this cap a node whose divergence keeps reproducing would loop \
      silently. The budget re-arms once the follower has looked healthy for 5x the recovery duration (the episode is \
      considered resolved). Set to 0 for unbounded reformats (no breaker).""",
      Integer.class, 5),

  HA_STALLED_REPLICA_RESYNC_DURATION_MS("arcadedb.ha.stalledReplicaResyncDurationMs", SCOPE.SERVER,
      """
      How long in milliseconds a replica must stay continuously STALLED (its matchIndex not advancing while the leader \
      keeps committing - e.g. stuck at -1 after a rolling upgrade) before the LEADER actively forces it to resync from \
      the leader. This is the leader-driven counterpart to HA_STALE_FOLLOWER_LAG_THRESHOLD: it covers the case where the \
      follower cannot self-detect the stall because its own commit index never advances. A follower still at the \
      never-appended sentinel (matchIndex = -1 while the leader holds committed entries, issue #5295) is treated as \
      STALLED regardless of the numeric lag, so it is recovered even when the leader is only a few entries ahead (where \
      the lag stays below HA_REPLICATION_LAG_WARNING); the same duration doubles as the grace before its status flips \
      from HEALTHY to STALLED, so a brief join / snapshot-install window is not misreported. Defaults to 60000; set to 0 \
      to disable leader-driven stalled-replica recovery (the STALLED condition is still detected and logged).""",
      Long.class, 60_000L),

  HA_SNAPSHOT_MAX_ENTRY_SIZE("arcadedb.ha.snapshotMaxEntrySize", SCOPE.SERVER,
      "Maximum uncompressed size in bytes for a single entry in a snapshot ZIP file. Protects against decompression bombs.",
      Long.class, 10_737_418_240L),

  HA_IDEMPOTENCY_CACHE_TTL_MS("arcadedb.ha.idempotencyCacheTtlMs", SCOPE.SERVER,
      "Time-to-live in milliseconds for entries in the HTTP idempotency cache.",
      Long.class, 60_000L),

  HA_IDEMPOTENCY_CACHE_MAX_ENTRIES("arcadedb.ha.idempotencyCacheMaxEntries", SCOPE.SERVER,
      "Maximum number of entries in the HTTP idempotency cache. Oldest entry is evicted when full.",
      Integer.class, 10_000),

  HA_IDEMPOTENCY_CACHE_MAX_BYTES("arcadedb.ha.idempotencyCacheMaxBytes", SCOPE.SERVER,
      "Maximum total size in bytes of the cached response bodies in the HTTP idempotency cache. Oldest entries are evicted when exceeded.",
      Long.class, 67_108_864L),

  HA_IDEMPOTENCY_CACHE_MAX_BODY_BYTES("arcadedb.ha.idempotencyCacheMaxBodyBytes", SCOPE.SERVER,
      "Maximum size in bytes of a single response body eligible for caching in the HTTP idempotency cache. Larger responses are not cached.",
      Long.class, 1_048_576L),

  HA_PEER_ALLOWLIST_ENABLED("arcadedb.ha.peerAllowlist.enabled", SCOPE.SERVER,
      """
      Reject inbound Raft gRPC connections whose remote address does not resolve to a host in \
      arcadedb.ha.serverList. Loopback is always allowed. Does not provide peer identity or encryption: \
      use mTLS on untrusted networks.""",
      Boolean.class, true),

  HA_GRPC_ALLOWLIST_REFRESH_MS("arcadedb.ha.grpcAllowlistRefreshMs", SCOPE.SERVER,
      "Rate-limiting interval in milliseconds for DNS re-resolution in the gRPC peer address allowlist filter.",
      Long.class, 30_000L),

  HA_PEER_ALLOWLIST_STARTUP_GRACE_MS("arcadedb.ha.peerAllowlistStartupGraceMs", SCOPE.SERVER,
      """
      Startup grace window in milliseconds during which the gRPC peer allowlist filter fails OPEN (accepts and logs a \
      warning) for an inbound address it cannot yet match, as long as a quorum (majority) of the hosts in \
      arcadedb.ha.serverList has never resolved at least once. This prevents a self-inflicted partition on Kubernetes, \
      where a peer's headless-service DNS record is only published once its pod is Ready, so a legitimately-restarting \
      peer connects before its own name resolves. Measured from filter creation. Once a quorum of peer hosts has \
      resolved at least once, or the window elapses, the filter enforces normally; the gate is a quorum rather than the \
      full peer set so a single permanently-down peer does not hold the window open for its full duration (issue #4828). \
      Set to 0 to disable fail-open (strict from the first connection); the filter is not an mTLS substitute (see issue \
      #3890), so a bounded fail-open window is the safer default.""",
      Long.class, 60_000L),

  HA_PEER_ALLOWLIST_STICKY_TTL_MS("arcadedb.ha.peerAllowlistStickyTtlMs", SCOPE.SERVER,
      """
      How long in milliseconds the gRPC peer allowlist filter keeps the last successfully-resolved IPs of a peer host \
      when a later DNS re-resolution of that host fails. Bridges transient DNS outages and pod-IP churn so a peer that \
      resolved moments ago is not evicted from the allowlist by a momentary lookup failure. Set to 0 to disable \
      stickiness (drop a host from the allowlist as soon as it stops resolving).""",
      Long.class, 300_000L),

  // POSTGRES
  POSTGRES_PORT("arcadedb.postgres.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Postgres plugin. Default is 5432", Integer.class, 5432),

  POSTGRES_HOST("arcadedb.postgres.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Postgres plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  POSTGRES_DEBUG("arcadedb.postgres.debug", SCOPE.SERVER,
      "Enables the printing of Postgres protocol to the console. Default is false", Boolean.class, false),

  POSTGRES_QUOTED_IDENTIFIERS("arcadedb.postgres.quotedIdentifiers", SCOPE.SERVER, """
      Interprets double-quoted tokens in SQL statements received through the Postgres wire protocol as identifiers, as \
      PostgreSQL and the SQL standard mandate, instead of as string literals. Set to false to restore the legacy \
      behaviour where a double-quoted token is a string literal. Default is true""", Boolean.class, true),

  POSTGRES_MAX_PARAM_SIZE("arcadedb.postgres.maxParamSize", SCOPE.SERVER,
      "Maximum size in bytes accepted for a single bind-message parameter value on the Postgres wire protocol. Values declaring a larger size are rejected before allocation. Default is 16MB",
      Integer.class, 16 * 1024 * 1024),

  POSTGRES_SIMPLE_QUERY_MAX_ROWS("arcadedb.postgres.simpleQueryMaxRows", SCOPE.SERVER, """
      Maximum number of rows a simple-query protocol ('Q' message) SELECT is allowed to buffer server-side before \
      the first row is sent. Unlike the extended query protocol, the simple-query protocol has no client-driven \
      cursor/max-rows mechanism and always expects the complete result set in one response, so the server has to \
      hold it in memory to determine the row description (column set and types) before streaming it. A SELECT whose \
      result exceeds this limit is refused with an error instead of risking an OutOfMemoryError; the client should \
      use the extended query protocol with a bounded portal fetch size for very large result sets. Default is \
      1000000""", Integer.class, 1_000_000),

  // BOLT (Neo4j)
  BOLT_PORT("arcadedb.bolt.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for BOLT plugin. Default is 7687", Integer.class, 7687),

  BOLT_HOST("arcadedb.bolt.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for BOLT plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  BOLT_DEBUG("arcadedb.bolt.debug", SCOPE.SERVER,
      "Enables the printing of BOLT protocol to the console. Default is false", Boolean.class, false),

  BOLT_ROUTING_TTL("arcadedb.bolt.routing.ttl", SCOPE.SERVER,
      "Time-to-live (in seconds) for BOLT routing table entries. Default is 300 (5 minutes)", Long.class, 300L),

  BOLT_DEFAULT_DATABASE("arcadedb.bolt.defaultDatabase", SCOPE.SERVER,
      "Default database name for BOLT connections when not specified in connection string. If not set, uses the first available database", String.class, null),

  BOLT_MAX_CONNECTIONS("arcadedb.bolt.maxConnections", SCOPE.SERVER,
      "Maximum number of concurrent BOLT connections. 0 means unlimited. Default is 0", Integer.class, 0),

  BOLT_SSL("arcadedb.bolt.ssl", SCOPE.SERVER,
      "TLS mode for BOLT connections: DISABLED (no TLS, default), OPTIONAL (auto-detect TLS or plaintext), REQUIRED (TLS only)",
      String.class, "DISABLED"),

  BOLT_WEBSOCKET_MAX_FRAME_SIZE("arcadedb.bolt.websocket.maxFrameSize", SCOPE.SERVER,
      "Maximum payload size in bytes accepted for a single BOLT WebSocket frame. Frames declaring a larger size are rejected before allocation, "
          + "since the length is read off the wire before authentication. Default is 16MB",
      Integer.class, 16 * 1024 * 1024),

  // The three *_SIZE/*_LENGTH settings below are defense in depth at different layers of the same BOLT ingest
  // path, not redundant: BOLT_MAX_MESSAGE_SIZE bounds the whole reassembled message before it is even handed to
  // the PackStream decoder, while BOLT_PACKSTREAM_MAX_VALUE_LENGTH bounds one BYTES/STRING value within an
  // already-accepted message. They share the same 16MB default because a single field legitimately consuming
  // the entire message budget is a real (if unusual) case, not because the two checks are meant to be identical.

  BOLT_MAX_MESSAGE_SIZE("arcadedb.bolt.maxMessageSize", SCOPE.SERVER, """
      Maximum total size in bytes accepted for a single BOLT protocol message after chunk reassembly. BOLT frames \
      a message as a sequence of chunks terminated by a zero-length chunk; without a bound on the reassembled \
      total, a client that never sends the terminator grows the reassembly buffer unbounded, before the BOLT \
      handshake or authentication. Default is 16MB""",
      Integer.class, 16 * 1024 * 1024),

  BOLT_PACKSTREAM_MAX_VALUE_LENGTH("arcadedb.bolt.packstream.maxValueLength", SCOPE.SERVER, """
      Maximum length in bytes accepted for a single PackStream BYTES/STRING value on the BOLT wire protocol. \
      A declared length above this bound, or larger than the bytes actually remaining in the message, is rejected \
      before allocation, since the length is read off the wire before authentication. Default is 16MB""",
      Integer.class, 16 * 1024 * 1024),

  BOLT_PACKSTREAM_MAX_ELEMENTS("arcadedb.bolt.packstream.maxElements", SCOPE.SERVER, """
      Maximum element/entry/field count accepted for a single PackStream list/map/structure declared size on the \
      BOLT wire protocol. Guards against a client-declared count (e.g. a handful of bytes claiming billions of \
      items) being trusted before any element is actually read. Default is 1048576""",
      Integer.class, 1_048_576),

  BOLT_PACKSTREAM_MAX_DEPTH("arcadedb.bolt.packstream.maxDepth", SCOPE.SERVER, """
      Maximum nesting depth accepted when decoding a PackStream value (list/map/structure) on the BOLT wire \
      protocol. The decoder builds nested containers on an explicit heap-allocated stack rather than JVM \
      recursion, so this bounds nesting complexity/memory rather than guarding against a stack overflow; \
      without it, an unauthenticated client could grow that stack unboundedly with a stream of nesting markers. \
      Default is 1000, generous for any real BOLT message.""",
      Integer.class, 1000),

  BOLT_MAX_OPEN_STREAMS("arcadedb.bolt.maxOpenStreams", SCOPE.SERVER, """
      Maximum number of result streams one BOLT connection may hold open at the same time. BOLT 4.0+ lets a client \
      open several streams inside a single explicit transaction, told apart by the qid a RUN returns, and each one \
      pins an engine result set (cursors, pages) for as long as that transaction lives - while nothing in the \
      protocol obliges the client to ever consume one. No real driver holds more than a handful open. \
      Default is 1024""",
      Integer.class, 1024),

  // gRPC
  GRPC_PORT("arcadedb.grpc.port", SCOPE.SERVER, """
      TCP/IP port number used for incoming connections for the gRPC plugin. Registered here, rather than read as a \
      bare key by the plugin alone, because HA has to know the port a peer's gRPC endpoint listens on to advertise a \
      dialable address for it (see the 'grpc' field of arcadedb.ha.serverList). Default is 50051""",
      Integer.class, 50051),

  // REDIS
  REDIS_PORT("arcadedb.redis.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Redis plugin. Default is 6379", Integer.class, 6379),

  REDIS_HOST("arcadedb.redis.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Redis plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  REDIS_DEFAULT_DATABASE("arcadedb.redis.defaultDatabase", SCOPE.SERVER,
      "Default database name for Redis protocol connections. If set, RAM commands (SET, GET, etc.) will use this database's globalVariables. Empty means no default (requires SELECT command or key prefix)", String.class, ""),

  REDIS_TLS("arcadedb.redis.tls", SCOPE.SERVER,
      "When true, the Redis wire-protocol listener accepts only TLS connections, using the shared SSL key/trust store settings (arcadedb.ssl.*). The AUTH credentials are then encrypted in transit. Default is false",
      Boolean.class, false),

  REDIS_MAX_MULTIBULK_DEPTH("arcadedb.redis.maxMultiBulkDepth", SCOPE.SERVER, """
      Maximum nesting depth of a RESP array accepted by the Redis wire-protocol listener. A RESP array element can \
      itself be an array, and the parser recurses once per nesting level, so an unbounded value lets an \
      unauthenticated client overflow the connection thread's JVM stack with a few tens of KB of input. Default is \
      32, generous for any real command.""",
      Integer.class, 32),

  REDIS_MAX_MULTIBULK_LENGTH("arcadedb.redis.maxMultiBulkLength", SCOPE.SERVER, """
      Maximum number of elements accepted in a single RESP array by the Redis wire-protocol listener, matching \
      Redis' own hard limit on multibulk requests. Guards against a client-declared array length (e.g. \
      *2000000000\\r\\n) starting a parse loop with billions of iterations. Default is 1048576.""",
      Integer.class, 1_048_576),

  REDIS_MAX_BULK_LENGTH("arcadedb.redis.maxBulkLength", SCOPE.SERVER, """
      Maximum length in bytes accepted for a single RESP bulk string ($) by the Redis wire-protocol listener, \
      matching Redis' own proto-max-bulk-len. Without a bound, a client-declared length (e.g. $2000000000\\r\\n) \
      can tie up a connection thread indefinitely by trickling bytes, or grow the parse buffer unbounded if the \
      declared bytes are actually sent. Default is 536870912 (512MB).""",
      Integer.class, 536_870_912),

  // MONGO
  MONGO_PORT("arcadedb.mongo.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Mongo plugin. Default is 27017", Integer.class, 27017),

  MONGO_HOST("arcadedb.mongo.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Mongo plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  GAV_USE_WHEN_STALE("arcadedb.gavUseWhenStale", SCOPE.DATABASE,
      """
      When true, the query planner uses stale Graph Analytical Views (GAV/CSR) for traversals instead of falling back to OLTP. \
      Stale data is faster but may not reflect the latest committed changes""", Boolean.class, false),

  GAV_RESTORE_AWAIT_TIMEOUT("arcadedb.gavRestoreAwaitTimeout", SCOPE.DATABASE,
      """
      Milliseconds database open() blocks waiting for Graph Analytical Views (GAV/CSR) restored from persisted \
      definitions to reach READY before returning. 0 (default) does not wait: when no persisted CSR plausibly \
      applies, the full rebuild is still triggered immediately in the background, but open() returns before it \
      completes and queries issued right after run unaccelerated until it does; when a persisted CSR does plausibly \
      apply (see arcadedb.gavPersistCsr), nothing is even started at open() time as of v26.9.1 - it is deferred to \
      whichever query touches the view first, so the fast path costs nothing for a session that never queries it. \
      A positive value here forces the wait either way, trading a slower open() for the restored/rebuilt view being \
      usable by the query that triggered the reopen""",
      Long.class, 0L),

  GAV_PERSIST_CSR("arcadedb.gavPersistCsr", SCOPE.DATABASE,
      """
      When true (default), a Graph Analytical View (GAV/CSR) that is READY (with no pending overlay changes) when \
      the database closes cleanly writes its CSR to disk alongside a freshness certificate (the database's last \
      committed transaction id at build time). If nothing was committed to the database between that close and the \
      next open, the certificate still matches and the persisted CSR is reused as-is instead of being rebuilt by a \
      full graph scan (issue #6583). Any commit in between invalidates the certificate and falls back to the \
      previous behavior: an async rebuild triggered on open. Set to false to disable persisting the CSR file \
      (e.g. to avoid its disk footprint or the extra write at close)""",
      Boolean.class, true),
  ;

  /**
   * Place holder for the "undefined" value of setting.
   */
  private final Object nullValue = new Object();

  private final        String                   key;
  private final        Object                   defValue;
  private final        Class<?>                 type;
  private final        SCOPE                    scope;
  private final        Callable<Object, Object> callback;
  private final        Callable<Object, Object> callbackIfNoSet;
  private volatile     Object                   value          = nullValue;
  private volatile     boolean                  explicitlySet  = false;
  private final        String                   description;
  private final        Boolean                  canChangeAtRuntime;
  private final        boolean                  hidden;
  private final        Set<Object>              allowed;
  public final static  String                   PREFIX = "arcadedb.";
  private static final Timer                    TIMER;

  public enum SCOPE {JVM, SERVER, DATABASE}

  static {
    TIMER = new Timer(true);
    readConfiguration();
  }

  GlobalConfiguration(final String iKey, final SCOPE scope, final String iDescription, final Class<?> iType,
                      final Object iDefValue) {
    this(iKey, scope, iDescription, iType, iDefValue, null, null, null);
  }

  GlobalConfiguration(final String iKey, final SCOPE scope, final String iDescription, final Class<?> iType, final Object iDefValue,
                      final Set<Object> allowed) {
    this(iKey, scope, iDescription, iType, iDefValue, null, null, allowed);
  }

  GlobalConfiguration(final String iKey, final SCOPE scope, final String iDescription, final Class<?> iType, final Object iDefValue,
                      final Callable<Object, Object> callback) {
    this(iKey, scope, iDescription, iType, iDefValue, callback, null, null);
  }

  GlobalConfiguration(final String iKey, final SCOPE scope, final String iDescription, final Class<?> iType, final Object iDefValue,
                      final Callable<Object, Object> callback, final Callable<Object, Object> callbackIfNoSet) {
    this(iKey, scope, iDescription, iType, iDefValue, callback, callbackIfNoSet, null);
  }

  GlobalConfiguration(final String iKey, final SCOPE scope, final String iDescription, final Class<?> iType, final Object iDefValue,
                      final Callable<Object, Object> callback, final Callable<Object, Object> callbackIfNoSet, final Set<Object> allowed) {
    this.key = iKey;
    this.scope = scope;
    this.description = iDescription;
    this.defValue = iDefValue;
    this.type = iType;
    this.canChangeAtRuntime = true;
    this.hidden = false;
    this.callback = callback;
    this.callbackIfNoSet = callbackIfNoSet;
    this.allowed = allowed;
  }

  /**
   * Reset all the configurations to the default values.
   */
  public static void resetAll() {
    for (final GlobalConfiguration v : values())
      v.reset();
  }

  /**
   * Reset the configuration to the default value.
   */
  public void reset() {
    if (callbackIfNoSet != null)
      value = callbackIfNoSet.call(null);
    else
      value = defValue;
    explicitlySet = false;
  }

  /**
   * Builds the set of allowed values for an integer option constrained to the inclusive range {@code [fromInclusive, toInclusive]}. The values are stored as
   * strings because {@link #setValue(Object)} validates against {@code value.toString()}.
   */
  private static Set<Object> integerRangeAsStrings(final int fromInclusive, final int toInclusive) {
    final Set<Object> set = new HashSet<>();
    for (int i = fromInclusive; i <= toInclusive; i++)
      set.add(Integer.toString(i));
    return set;
  }

  public static void dumpConfiguration(final PrintStream out) {
    out.print("ARCADEDB ");
    out.print(Constants.getRawVersion());
    out.println(" configuration:");

    String lastSection = "";
    for (final GlobalConfiguration v : Arrays.stream(values()).sorted(Comparator.comparing(Enum::name)).toList()) {
      final String section = v.key.substring(0, v.key.indexOf('.'));

      if (!lastSection.equals(section)) {
        out.print("- ");
        out.println(section.toUpperCase(Locale.ENGLISH));
        lastSection = section;
      }
      out.print("  + ");
      out.print(v.key);
      out.print(" = ");
      out.println(v.isHidden() ? "<hidden>" : String.valueOf((Object) v.getValue()));
    }
    out.flush();
  }

  public static void fromJSON(final String input) {
    if (input == null)
      return;

    final JSONObject json = new JSONObject(input);
    final JSONObject cfg = json.getJSONObject("configuration");
    for (final String k : cfg.keySet()) {
      final GlobalConfiguration cfgEntry = findByKey(GlobalConfiguration.PREFIX + k);
      if (cfgEntry != null) {
        cfgEntry.setValue(cfg.get(k));
      }
    }
  }

  public static String toJSON() {
    final JSONObject json = new JSONObject();

    final JSONObject cfg = new JSONObject();
    json.put("configuration", cfg);

    for (final GlobalConfiguration k : values()) {
      Object v = (Object) k.getValue();
      if (v instanceof Class<?> class1)
        v = class1.getName();
      cfg.put(k.key.substring(PREFIX.length()), v);
    }

    return json.toString();
  }

  /**
   * Find the OGlobalConfiguration instance by the key. Key is case insensitive.
   *
   * @param iKey Key to find. It's case insensitive.
   * @return OGlobalConfiguration instance if found, otherwise null
   */
  public static GlobalConfiguration findByKey(final String iKey) {
    String key = iKey;
    if (!key.startsWith(PREFIX))
      key = PREFIX + iKey;
    for (final GlobalConfiguration v : values()) {
      if (v.getKey().equalsIgnoreCase(key))
        return v;
    }
    return null;
  }

  /**
   * Maximum size, in bytes, of a SINGLE replicated Raft log entry: the smaller of
   * {@link #HA_GRPC_MESSAGE_SIZE_MAX} and {@link #HA_APPEND_BUFFER_SIZE}.
   * <p>
   * Issue #4743: the gRPC frame cap is NOT the effective ceiling. Ratis also enforces the appender
   * buffer byte limit per entry, and rejects an entry above it with a {@code StateMachineException}
   * whose {@code leaderShouldStepDown()} is {@code true} - so the LEADER STEPS DOWN, the caller retries
   * the same oversized entry against the next leader and topples it too, and the cluster churns
   * elections while the write never lands. Since the appender default (4MB) is far below the gRPC
   * default (128MB), in practice the appender limit is what binds.
   * <p>
   * Every component that produces or projects the size of a replicated entry must measure itself
   * against this value, not against either knob alone.
   */
  public static long maxReplicatedRaftEntrySize(final ContextConfiguration configuration) {
    final long grpcMessageSizeMax = configuration.getValueAsLong(HA_GRPC_MESSAGE_SIZE_MAX);
    final long appendBufferSize = FileUtils.getSizeAsNumber(configuration.getValueAsString(HA_APPEND_BUFFER_SIZE));
    return Math.min(grpcMessageSizeMax, appendBufferSize);
  }

  /**
   * Changes the configuration values in one shot by passing a Map of values. Keys can be the Java ENUM names or the string
   * representation of configuration values
   */
  public static void setConfiguration(final Map<String, Object> iConfig) {
    for (final Map.Entry<String, Object> config : iConfig.entrySet()) {
      for (final GlobalConfiguration v : values()) {
        if (BinaryComparator.equalsString(v.getKey(), config.getKey())) {
          v.setValue(config.getValue());
          break;
        } else if (BinaryComparator.equalsString(v.name(), config.getKey())) {
          v.setValue(config.getValue());
          break;
        }
      }
    }
  }

  /**
   * Assign configuration values by reading system properties.
   */
  public static void readConfiguration() {
    String prop;

    for (final GlobalConfiguration config : values()) {
      prop = System.getProperty(config.key);
      if (prop == null)
        prop = System.getenv(config.key);

      if (prop != null)
        config.setValue(prop);
      else if (config.callbackIfNoSet != null) {
        config.setValue(config.callbackIfNoSet.call(null));
      }
    }
  }

  public <T> T getValue() {
    //noinspection unchecked
    return (T) (value != nullValue && value != null ? value : defValue);
  }

  /**
   * Resolves this {@code SCOPE.DATABASE} setting's value against {@code database}'s per-database overrides,
   * falling back to the compiled-in default when {@code database} is {@code null}. Several call sites read a
   * {@code SCOPE.DATABASE} setting (e.g. {@code arcadedb.command.regexTimeout}) from code that isn't guaranteed
   * a bound database - a standalone SQL function or operator that tests, or a future caller, may invoke directly
   * with a {@code null}/disconnected database - and repeating the same null-safe fallback at each of those call
   * sites risks drifting out of sync; this centralizes it.
   *
   * @param database the database to resolve a per-database override from, or {@code null} to use the default
   *
   * @return the resolved value as a {@code long}
   */
  public long getValueAsLong(final Database database) {
    return (database != null ? database.getConfiguration() : new ContextConfiguration()).getValueAsLong(this);
  }

  /**
   * @return {@literal true} if configuration was changed from default value and {@literal false} otherwise.
   */
  public boolean isChanged() {
    return explicitlySet;
  }

  /**
   * @return Value of configuration parameter stored as enumeration if such one exists.
   * @throws ClassCastException       if stored value can not be casted and parsed from string to passed in enumeration class.
   * @throws IllegalArgumentException if value associated with configuration parameter is a string bug can not be converted to
   *                                  instance of passed in enumeration class.
   */
  public <T extends Enum<T>> T getValueAsEnum(final Class<T> enumType) {
    final Object value = getValue();

    if (value == null)
      return null;

    if (enumType.isAssignableFrom(value.getClass())) {
      return enumType.cast(value);
    } else if (value instanceof String) {
      final String presentation = value.toString();
      return Enum.valueOf(enumType, presentation);
    } else {
      throw new ClassCastException("Value " + value + " can not be cast to enumeration " + enumType.getSimpleName());
    }
  }

  /**
   * Converts {@code iValue} to this setting's declared {@link #getType() type}, or throws
   * {@link IllegalArgumentException} naming the key, the type and the offending value when it does not parse.
   * <p>
   * Issue #6875: this is the single place a value is turned into a setting's type, so that {@link #setValue(Object)}
   * and the administrative writers that store into a {@link ContextConfiguration} instead
   * ({@code set_server_setting} and {@code POST /api/v1/server "set server setting"}) accept and refuse exactly
   * the same strings. Before it existed the writers stored whatever they were handed and the
   * {@code NumberFormatException} surfaced later, inside whichever component read the setting next.
   * <p>
   * The integral parse is {@link FileUtils#getSizeAsNumber(Object)} on the trimmed text, which is what
   * {@link #getValueAsInteger()} and {@link #getValueAsLong()} have always used to read one back; it is a strict
   * superset of {@code Integer.parseInt}/{@code Long.parseLong}, so nothing that parsed before stops parsing.
   * <p>
   * {@code Boolean} stays as permissive as {@code Boolean.parseBoolean}, deliberately: {@link #readConfiguration()}
   * feeds every system property and environment variable through here during this class's static initializer, so
   * turning a boolean typo into a throw would turn it into an {@code ExceptionInInitializerError} that takes the
   * whole engine down instead of the setting.
   *
   * @param iValue the value to convert, or {@code null}
   *
   * @return the value as an instance of {@link #getType()}, or {@code null} when {@code iValue} is {@code null}
   *
   * @throws IllegalArgumentException if {@code iValue} cannot be represented as this setting's type
   */
  public Object coerce(final Object iValue) {
    if (iValue == null)
      return null;

    try {
      if (type == Boolean.class)
        return iValue instanceof Boolean b ? b : Boolean.parseBoolean(iValue.toString().trim());

      if (type == Integer.class)
        // the range check has to cover a boxed Number too, not just the parsed-from-text path: Number.intValue()
        // keeps the low 32 bits, so a Long outside the int range would be stored silently truncated - where the
        // Integer.parseInt this method replaced threw. Every input reaches the same bound.
        return narrowToInteger(iValue instanceof Number n ? n.longValue() : FileUtils.getSizeAsNumber(iValue.toString().trim()));

      if (type == Long.class)
        return iValue instanceof Number n ? n.longValue() : FileUtils.getSizeAsNumber(iValue.toString().trim());

      if (type == Float.class)
        return iValue instanceof Number n ? n.floatValue() : Float.parseFloat(iValue.toString().trim());

      if (type == String.class)
        return iValue.toString();

      if (type.isEnum()) {
        if (type.isInstance(iValue))
          return iValue;

        if (iValue instanceof String string)
          for (final Object constant : type.getEnumConstants())
            if (((Enum<?>) constant).name().equalsIgnoreCase(string))
              return constant;

        throw new IllegalArgumentException("Invalid value of `" + key + "` option");
      }

      return iValue;
    } catch (final RuntimeException e) {
      final String message =
          "Value '" + iValue + "' is not valid for setting '" + key + "' of type " + type.getSimpleName();

      // A numeric setting keeps reporting a bad value as a NumberFormatException. It is a subclass of
      // IllegalArgumentException, so a caller that catches the general form is unaffected either way, while one
      // that distinguishes a malformed number - GlobalConfigurationTest.typeConversion does - still can. The
      // message is the enriched one regardless: 'For input string: "A"', which is what FileUtils.getSizeAsNumber
      // surfaces for "abc", names neither the setting nor what was actually sent.
      final IllegalArgumentException failure = type == Integer.class || type == Long.class || type == Float.class ?
          new NumberFormatException(message) :
          new IllegalArgumentException(message);
      failure.initCause(e);
      throw failure;
    }
  }

  /**
   * Narrows an integral value to the {@code int} an {@code Integer} setting holds, refusing rather than truncating
   * one outside the range.
   * <p>
   * Issue #6875: this is shared by {@link #coerce(Object)}, {@link #getValueAsInteger()} and
   * {@link ContextConfiguration#getValueAsInteger(GlobalConfiguration)} so that the bound holds on the READ side
   * too. Not every value reaches a configuration map through {@code coerce}:
   * {@link ContextConfiguration#setValue(GlobalConfiguration, Object)} is a plain map put, so a boxed {@code Long}
   * outside the {@code int} range can be stored, and {@code Number.intValue()} would then hand back its
   * wrapped-around low 32 bits instead of failing.
   */
  int narrowToInteger(final long value) {
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
      throw new NumberFormatException(
          "Value '" + value + "' is not valid for setting '" + key + "' of type Integer: outside the range of an Integer");
    return (int) value;
  }

  public void setValue(final Object iValue) {
    final Object oldValue = value;
    explicitlySet = true;

    try {
      value = coerce(iValue);

      if (callback != null)
        try {
          final Object newValue = callback.call(value);
          if (newValue != value)
            // OVERWRITE IT
            value = newValue;
        } catch (final Exception e) {
          if (LogManager.instance() != null)
            LogManager.instance().log(this, Level.SEVERE, "Error during setting property %s=%s", e, key, value);
        }

      if (allowed != null && value != null)
        if (!allowed.contains(value.toString().toLowerCase(Locale.ENGLISH)))
          throw new IllegalArgumentException(
              "Global setting '" + key + "=" + value + "' is not valid. Allowed values are " + allowed);

    } catch (final Exception e) {
      // RESTORE THE PREVIOUS VALUE
      value = oldValue;
      throw e;
    }
  }

  public boolean getValueAsBoolean() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
  }

  public String getValueAsString() {
    return value != nullValue && value != null ?
        SystemVariableResolver.INSTANCE.resolveSystemVariables(value.toString(), "") :
        defValue != null ? SystemVariableResolver.INSTANCE.resolveSystemVariables(defValue.toString(), "") : null;
  }

  /**
   * Issue #6875: the trim, and the {@link Number} test widened from {@code Float}, keep this accessor and
   * {@link ContextConfiguration#getValueAsInteger(GlobalConfiguration)} on exactly one parse. A value that reaches
   * either holder without passing through {@link #coerce(Object)} - {@code ContextConfiguration.fromJSON}, the
   * {@code Map} constructor - used to read back differently depending on which of the two a component happened to call.
   */
  public int getValueAsInteger() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return narrowToInteger(v instanceof Number n ? n.longValue() : FileUtils.getSizeAsNumber(v.toString().trim()));
  }

  public long getValueAsLong() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Number n ? n.longValue() : FileUtils.getSizeAsNumber(v.toString().trim());
  }

  public float getValueAsFloat() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Number n ? n.floatValue() : Float.parseFloat(v.toString().trim());
  }

  public String getKey() {
    return key;
  }

  public Boolean isChangeableAtRuntime() {
    return canChangeAtRuntime;
  }

  public boolean isHidden() {
    return hidden || key.contains("clusterToken") || key.contains("Password") || key.contains("password");
  }

  public Object getDefValue() {
    return defValue;
  }

  public Class<?> getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }

  public SCOPE getScope() {
    return scope;
  }
}
