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

import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
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
        VECTOR_INDEX_LOCATION_CACHE_SIZE.setValue(-1);

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
        VECTOR_INDEX_LOCATION_CACHE_SIZE.setValue(10_000);

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
      "Default datetime implementation to use on deserialization. By default java.time.LocalDateTime is used, but the following are supported: java.util.Date, java.util.Calendar, java.time.LocalDateTime, java.time.ZonedDateTime",
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
      fan-out (per-bucket parallel scoring on partitioned types and types with multiple \
      buckets). Kept on its own pool rather than sharing the QueryEngineManager pool so \
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
      Larger values reduce commit overhead on single-node setups at the cost of bigger transactions.""",
      Integer.class, 1000),

  PAGE_FLUSH_QUEUE("arcadedb.pageFlushQueue", SCOPE.DATABASE, "Size of the asynchronous page flush queue", Integer.class, 512),

  FLUSH_SUSPEND_MAX_DEFERRED_RAM("arcadedb.flushSuspendMaxDeferredRAM", SCOPE.DATABASE,
      """
      Maximum amount of RAM (in MB) of dirty pages the page-flush thread may defer in memory while flushing \
      is suspended (during an HA snapshot ship or a full backup, when the on-disk files must stay stable). \
      Once the deferred backlog crosses this cap the flush thread stops draining its bounded queue, so \
      committing threads are throttled instead of the deferred backlog growing without limit and exhausting \
      the heap (issue #4728: a busy leader shipping a multi-GB snapshot OOM'd). Set to 0 to disable the cap \
      (unbounded, pre-4728 behavior).""",
      Long.class, 512),

  EXPLICIT_LOCK_TIMEOUT("arcadedb.explicitLockTimeout", SCOPE.DATABASE, "Timeout in ms to lock resources on explicit lock",
      Long.class, 5000),

  COMMIT_LOCK_TIMEOUT("arcadedb.commitLockTimeout", SCOPE.DATABASE, "Timeout in ms to lock resources during commit", Long.class,
      5000),

  TX_RETRIES("arcadedb.txRetries", SCOPE.DATABASE, "Number of retries in case of MVCC exception", Integer.class, 3),

  TX_RETRY_DELAY("arcadedb.txRetryDelay", SCOPE.DATABASE,
      "Maximum amount of milliseconds to compute a random number to wait for the next retry. This setting is helpful in case of high concurrency on the same pages (multi-thread insertion over the same bucket)",
      Integer.class, 100),

  GRAPH_EDGE_APPEND_MERGE("arcadedb.graph.edgeAppendMerge", SCOPE.DATABASE,
      "At commit, when the only conflict on an edge-list page is concurrent in-chunk edge appends (which commute), re-apply the appends on top of the newer page version instead of failing the whole transaction with a ConcurrentModificationException. Removes the retry storm on super-node (hot vertex) edge insertion",
      Boolean.class, true),

  GRAPH_SUPERNODE_THRESHOLD("arcadedb.graph.supernodeThreshold", SCOPE.DATABASE,
      "Approximate number of edges (per vertex, per direction) after which the vertex's edge list is promoted to the striped super-node layout, spreading further appends over multiple files so concurrent insertions on the same hot vertex do not contend. FORWARD-INCOMPATIBLE ON FIRST USE: promotion writes a new record type (the stripe directory), so once any vertex promotes, the database can no longer be opened by releases older than 26.8.1; promotion is one-way. Iteration order on promoted vertices is approximate (newest-generation-first) instead of strict reverse-insertion. 0 disables promotion entirely (databases stay fully readable by older versions)",
      Integer.class, 4096),

  GRAPH_SUPERNODE_STRIPES("arcadedb.graph.supernodeStripes", SCOPE.DATABASE,
      "Number of stripes (separate edge-list files) a super-node's edge list is spread over at promotion. The stripes are hosted in a per-type bucket pool of this many files, created once per type at its first promotion (types without super-nodes cost no files). Write parallelism saturates at the number of concurrent writers, so values beyond the CPU cores rarely help. Recorded per vertex at promotion time",
      Integer.class, 16),

  BACKUP_ENABLED("arcadedb.backup.enabled", SCOPE.DATABASE,
      "Allow a database to be backup. Disabling backup gives a huge boost in performance because no lock will be used for every operations",
      Boolean.class, true),

  // SQL
  SQL_STATEMENT_CACHE("arcadedb.sqlStatementCache", SCOPE.DATABASE, "Maximum number of parsed statements to keep in cache",
      Integer.class, 300),

  SQL_PARSER_IMPLEMENTATION("arcadedb.sql.parserImplementation", SCOPE.DATABASE,
      "Deprecated, has no effect. The ANTLR4-based SQL parser is always used.",
      String.class, "antlr"),

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
      Disable for security in multi-tenant server deployments.""",
      Boolean.class, true),

  OPENCYPHER_LOAD_CSV_IMPORT_DIRECTORY("arcadedb.opencypher.loadCsv.importDirectory", SCOPE.DATABASE,
      """
      Root directory for LOAD CSV file:/// URLs. When set, file paths are resolved relative to this \
      directory and path traversal (../) is blocked. Empty string means no restriction.""",
      String.class, ""),

  OPENCYPHER_ID_BUCKET_BITS("arcadedb.opencypher.idBucketBits", SCOPE.JVM,
      """
      Number of bits reserved for the bucketId when packing a RID into the numeric value returned by the OpenCypher id() function (and SQL's .asCypherRID() method). \
      Out of the 63 usable bits (the sign bit is always kept clear to preserve the Neo4j id(n) >= 0 semantics), this many go to the bucketId and the rest to the \
      record position within the bucket. The default of 16 allows up to 65536 buckets and ~1.4e14 positions per bucket, covering the vast majority of use cases. \
      Increase it for databases with many buckets, decrease it for buckets holding a very high number of records. Must be between 1 and 31. \
      Changing this value alters the numeric id() output, so encode and decode must use the same setting.""",
      Integer.class, 16, integerRangeAsStrings(1, 31)),

  // COMMAND
  COMMAND_TIMEOUT("arcadedb.command.timeout", SCOPE.DATABASE, "Default timeout for commands (in ms)", Long.class, 0),

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
      Maximum number of elements (records) allowed in a single query for memory-intensive operations (eg. ORDER BY in heap). \
      If exceeded, the query fails with an OCommandExecutionException. Negative number means no limit.\
      This setting is intended as a safety measure against excessive resource consumption from a single query (eg. prevent OutOfMemory)""",
      Long.class, 500_000),

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

  VECTOR_INDEX_LOCATION_CACHE_SIZE("arcadedb.vectorIndex.locationCacheSize", SCOPE.DATABASE,
      """
      Maximum number of vector locations to cache in memory per vector index. \
      Set to -1 for unlimited (backward compatible). \
      Each entry uses ~56 bytes. Recommended: 100000 for datasets with 1M+ vectors (~5.6MB), \
      -1 for smaller datasets.""",
      Integer.class, -1),

  VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE("arcadedb.vectorIndex.graphBuildCacheSize", SCOPE.DATABASE,
      """
      Maximum number of vectors to cache in memory during HNSW graph building. \
      Higher values speed up construction but use more RAM. \
      RAM usage = cacheSize * (dimensions * 4 + 64) bytes. \
      Recommended: 100000 for 768-dim vectors (~30MB), scale based on dimensionality.""",
      Integer.class, 100_000),

  VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD("arcadedb.vectorIndex.mutationsBeforeRebuild", SCOPE.DATABASE,
      """
      Number of mutations (inserts/updates/deletes) before rebuilding the HNSW graph index. \
      Higher values reduce rebuild cost but may return slightly stale results in queries. \
      Lower values provide fresher results but rebuild more frequently. \
      Recommended: 50-200 for read-heavy, 200-500 for write-heavy workloads.""",
      Integer.class, 100),

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

  // NETWORK
  NETWORK_SAME_SERVER_ERROR_RETRIES("arcadedb.network.sameServerErrorRetry", SCOPE.SERVER,
      "Number of automatic retries in case of IO errors with a specific server. If replica servers are configured, look also at HA_ERROR_RETRY setting. 0 (default) = no retry",
      Integer.class, 0),

  NETWORK_SOCKET_TIMEOUT("arcadedb.network.socketTimeout", SCOPE.SERVER, "TCP/IP Socket timeout (in ms)", Integer.class, 30000),

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
      "AppendEntries batch byte limit for replication (e.g. '4MB')", String.class, "4MB"),

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
      Raft log write buffer size (e.g. '8MB'). Must be at least appendBufferSize + 8 bytes, \
      otherwise the server fails to start with ConfigurationException.""",
      String.class, "8MB"),

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
      When empty (the default), the server root path is used, preserving the previous default layout. \
      Set to an absolute path (e.g. /var/lib/arcadedb/raft) to decouple Raft persistence from \
      the server installation directory, which is required for Kubernetes readOnlyRootFilesystem deployments.""",
      String.class, ""),

  HA_SNAPSHOT_THRESHOLD("arcadedb.ha.snapshotThreshold", SCOPE.SERVER,
      """
      Number of Raft log entries after which the leader automatically takes a snapshot. \
      Lower values cause more frequent snapshots and earlier log compaction.""",
      Long.class, 100_000L),

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
      for cluster-level recovery. Raise when partition-recovery scenarios cause legitimate rapid restarts.""",
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
      producing an entry too large for the Raft transport. Kept below the Raft message size cap (64MB) with \
      headroom for the schema JSON and the mutable-bucket clear WAL.""",
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
      follower cannot self-detect the stall because its own commit index never advances. Defaults to 60000; set to 0 to \
      disable leader-driven stalled-replica recovery (the STALLED condition is still detected and logged).""",
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

  // REDIS
  REDIS_PORT("arcadedb.redis.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Redis plugin. Default is 6379", Integer.class, 6379),

  REDIS_HOST("arcadedb.redis.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Redis plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  REDIS_DEFAULT_DATABASE("arcadedb.redis.defaultDatabase", SCOPE.SERVER,
      "Default database name for Redis protocol connections. If set, RAM commands (SET, GET, etc.) will use this database's globalVariables. Empty means no default (requires SELECT command or key prefix)", String.class, ""),

  // MONGO
  MONGO_PORT("arcadedb.mongo.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Mongo plugin. Default is 27017", Integer.class, 27017),

  MONGO_HOST("arcadedb.mongo.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Mongo plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  GAV_USE_WHEN_STALE("arcadedb.gavUseWhenStale", SCOPE.DATABASE,
      """
      When true, the query planner uses stale Graph Analytical Views (GAV/CSR) for traversals instead of falling back to OLTP. \
      Stale data is faster but may not reflect the latest committed changes""", Boolean.class, false),
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

  public void setValue(final Object iValue) {
    final Object oldValue = value;
    explicitlySet = true;

    try {
      if (iValue == null)
        value = null;
      else if (type == Boolean.class)
        value = Boolean.parseBoolean(iValue.toString());
      else if (type == Integer.class)
        value = Integer.parseInt(iValue.toString());
      else if (type == Long.class)
        value = Long.parseLong(iValue.toString());
      else if (type == Float.class)
        value = Float.parseFloat(iValue.toString());
      else if (type == String.class)
        value = iValue.toString();
      else if (type.isEnum()) {
        boolean accepted = false;

        if (type.isInstance(iValue)) {
          value = iValue;
          accepted = true;
        } else if (iValue instanceof String string) {

          for (final Object constant : type.getEnumConstants()) {
            final Enum<?> enumConstant = (Enum<?>) constant;

            if (enumConstant.name().equalsIgnoreCase(string)) {
              value = enumConstant;
              accepted = true;
              break;
            }
          }
        }

        if (!accepted)
          throw new IllegalArgumentException("Invalid value of `" + key + "` option");
      } else
        value = iValue;

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

  public int getValueAsInteger() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return (int) (v instanceof Number n ? n.intValue() : FileUtils.getSizeAsNumber(v.toString()));
  }

  public long getValueAsLong() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Number n ? n.longValue() : FileUtils.getSizeAsNumber(v.toString());
  }

  public float getValueAsFloat() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Float f ? f : Float.parseFloat(v.toString());
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
