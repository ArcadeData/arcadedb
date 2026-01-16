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

import java.io.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.*;

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
      if (v.equalsIgnoreCase("default")) {
        // NOT MUCH TO DO HERE, THIS IS THE DEFAULT OPTION
      } else if (v.equalsIgnoreCase("high-performance")) {
        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("fast");
        VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(-1);
        VECTOR_INDEX_LOCATION_CACHE_SIZE.setValue(-1);

        if (cores > 1)
          // USE ONLY HALF OF THE CORES MINUS ONE
          ASYNC_WORKER_THREADS.setValue((cores / 2) - 1);
        else
          ASYNC_WORKER_THREADS.setValue(1);

      } else if (v.equalsIgnoreCase("low-ram")) {
        MAX_PAGE_RAM.setValue(16); // 16 MB OF RAM FOR PAGE CACHE
        INDEX_COMPACTION_RAM_MB.setValue(16);
        INITIAL_PAGE_CACHE_SIZE.setValue(256);
        FREE_PAGE_RAM.setValue(80);
        ASYNC_OPERATIONS_QUEUE_SIZE.setValue(8);
        ASYNC_TX_BATCH_SIZE.setValue(8);
        PAGE_FLUSH_QUEUE.setValue(8);
        SQL_STATEMENT_CACHE.setValue(16);
        HA_REPLICATION_QUEUE_SIZE.setValue(8);
        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("standard");
        SERVER_HTTP_IO_THREADS.setValue(cores > 8 ? 4 : 2);
        VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(10_000);
        VECTOR_INDEX_LOCATION_CACHE_SIZE.setValue(10_000);

        PageManager.INSTANCE.configure();

      } else if (v.equalsIgnoreCase("low-cpu")) {
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
            final long newValue = Runtime.getRuntime().maxMemory() / 2;
            if (LogManager.instance() != null)
              LogManager.instance()
                  .log(this, Level.WARNING, "Setting '%s=%s' is > than 80%% of maximum heap (%s). Decreasing it to %s",
                      MAX_PAGE_RAM.key, FileUtils.getSizeAsString(maxRAM),
                      FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), FileUtils.getSizeAsString(newValue));
            else
              System.out.println(
                  "Setting '%s=%s' is > than 80%% of maximum heap (%s). Decreasing it to %s".formatted(MAX_PAGE_RAM.key,
                      FileUtils.getSizeAsString(maxRAM), FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()),
                      FileUtils.getSizeAsString(newValue)));

            return newValue;
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

  BUCKET_REUSE_SPACE_MODE("arcadedb.bucketReuseSpaceMode", SCOPE.DATABASE,
      "How to reuse space in pages. 'high' = more space saved, but slower opening and update/delete time. 'medium' to still reuse space without the initial scan at opening time. 'low' for faster performance, but less space reused. Default is 'high'",
      String.class, "high", Set.of("low", "medium", "high")),

  BUCKET_WIPEOUT_ONDELETE("arcadedb.bucketWipeOutOnDelete", SCOPE.DATABASE,
      "Wipe out record content on delete. If enabled, assures deleted records cannot be analyzed by parsing the raw files and backups will be more compressed, but it also makes deletes a little bit slower",
      Boolean.class, true),

  ASYNC_WORKER_THREADS("arcadedb.asyncWorkerThreads", SCOPE.DATABASE,
      "Number of asynchronous worker threads. 0 (default) = available cores minus 1", Integer.class,
      Runtime.getRuntime().availableProcessors() > 1 ? Runtime.getRuntime().availableProcessors() - 1 : 1),

  ASYNC_OPERATIONS_QUEUE_IMPL("arcadedb.asyncOperationsQueueImpl", SCOPE.DATABASE,
      "Queue implementation to use between 'standard' and 'fast'. 'standard' consumes less CPU than the 'fast' implementation, but it could be slower with high loads",
      String.class, "standard", Set.of("standard", "fast")),

  ASYNC_OPERATIONS_QUEUE_SIZE("arcadedb.asyncOperationsQueueSize", SCOPE.DATABASE,
      "Size of the total asynchronous operation queues (it is divided by the number of parallel threads in the pool)",
      Integer.class, 1024),

  ASYNC_TX_BATCH_SIZE("arcadedb.asyncTxBatchSize", SCOPE.DATABASE,
      "Maximum number of operations to commit in batch by async thread", Integer.class, 1024 * 10),

  ASYNC_BACK_PRESSURE("arcadedb.asyncBackPressure", SCOPE.DATABASE,
      "When the asynchronous queue is full at a certain percentage, back pressure is applied", Integer.class, 0),

  PAGE_FLUSH_QUEUE("arcadedb.pageFlushQueue", SCOPE.DATABASE, "Size of the asynchronous page flush queue", Integer.class, 512),

  EXPLICIT_LOCK_TIMEOUT("arcadedb.explicitLockTimeout", SCOPE.DATABASE, "Timeout in ms to lock resources on explicit lock",
      Long.class, 5000),

  COMMIT_LOCK_TIMEOUT("arcadedb.commitLockTimeout", SCOPE.DATABASE, "Timeout in ms to lock resources during commit", Long.class,
      5000),

  TX_RETRIES("arcadedb.txRetries", SCOPE.DATABASE, "Number of retries in case of MVCC exception", Integer.class, 3),

  TX_RETRY_DELAY("arcadedb.txRetryDelay", SCOPE.DATABASE,
      "Maximum amount of milliseconds to compute a random number to wait for the next retry. This setting is helpful in case of high concurrency on the same pages (multi-thread insertion over the same bucket)",
      Integer.class, 100),

  BACKUP_ENABLED("arcadedb.backup.enabled", SCOPE.DATABASE,
      "Allow a database to be backup. Disabling backup gives a huge boost in performance because no lock will be used for every operations",
      Boolean.class, true),

  // SQL
  SQL_STATEMENT_CACHE("arcadedb.sqlStatementCache", SCOPE.DATABASE, "Maximum number of parsed statements to keep in cache",
      Integer.class, 300),

  // OPENCYPHER
  OPENCYPHER_STATEMENT_CACHE("arcadedb.opencypher.statementCache", SCOPE.DATABASE,
      "Maximum number of parsed OpenCypher statements to keep in cache", Integer.class, 300),

  OPENCYPHER_PLAN_CACHE("arcadedb.opencypher.planCache", SCOPE.DATABASE,
      "Maximum number of OpenCypher execution plans to keep in cache (frequency-based eviction)", Integer.class, 300),

  // COMMAND
  COMMAND_TIMEOUT("arcadedb.command.timeout", SCOPE.DATABASE, "Default timeout for commands (in ms)", Long.class, 0),

  COMMAND_WARNINGS_EVERY("arcadedb.command.warningsEvery", SCOPE.JVM,
      "Reduce warnings in commands to print in console only every X occurrences. Use 0 to disable warnings with commands",
      Integer.class, 100),

  GREMLIN_ENGINE("arcadedb.gremlin.engine", SCOPE.DATABASE,
      "Gremlin engine to use. 'java' (default, secure) uses the native Gremlin parser - recommended for production. " +
      "'groovy' enables the legacy Groovy engine with security restrictions (use only if needed for compatibility). " +
      "'auto' attempts Java first, falls back to Groovy if needed (not recommended for security-critical deployments).",
      String.class, "java", Set.of("auto", "groovy", "java")),

  /**
   * Not in use anymore after removing Gremlin Executor
   */
  @Deprecated GREMLIN_COMMAND_TIMEOUT("arcadedb.gremlin.timeout", SCOPE.DATABASE, "Default timeout for gremlin commands (in ms)",
      Long.class, 30_000),

  // USER CODE
  POLYGLOT_COMMAND_TIMEOUT("arcadedb.polyglotCommand.timeout", SCOPE.DATABASE, "Default timeout for polyglot commands (in ms)",
      Long.class, 10_000),

  QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP("arcadedb.queryMaxHeapElementsAllowedPerOp", SCOPE.DATABASE, """
      Maximum number of elements (records) allowed in a single query for memory-intensive operations (eg. ORDER BY in heap). \
      If exceeded, the query fails with an OCommandExecutionException. Negative number means no limit.\
      This setting is intended as a safety measure against excessive resource consumption from a single query (eg. prevent OutOfMemory)""",
      Long.class, 500_000),

  // CYPHER
  CYPHER_STATEMENT_CACHE("arcadedb.cypher.statementCache", SCOPE.DATABASE,
      "Max number of entries in the cypher statement cache. Use 0 to disable. Caching statements speeds up execution of the same cypher queries",
      Integer.class, 1000),

  // INDEXES
  INDEX_BUILD_CHUNK_SIZE_MB("arcadedb.index.buildChunkSizeMB", SCOPE.DATABASE,
          "Size in MB for transaction chunks during bulk index creation with WAL disabled. " +
                  "Larger chunks reduce commit overhead but use more memory. " +
                  "Smaller chunks reduce memory pressure but add commit overhead. " +
                  "Recommended: 50MB for typical workloads, 100MB for high-memory systems, 25MB for constrained environments.",
          Long.class, 50L),

  INDEX_COMPACTION_RAM_MB("arcadedb.indexCompactionRAM", SCOPE.DATABASE, "Maximum amount of RAM to use for index compaction, in MB",
      Long.class, 300),

  INDEX_COMPACTION_MIN_PAGES_SCHEDULE("arcadedb.indexCompactionMinPagesSchedule", SCOPE.DATABASE,
      "Minimum number of mutable pages for an index to be schedule for automatic compaction. 0 = disabled", Integer.class, 10),

  VECTOR_INDEX_LOCATION_CACHE_SIZE("arcadedb.vectorIndex.locationCacheSize", SCOPE.DATABASE,
      "Maximum number of vector locations to cache in memory per vector index. " +
      "Set to -1 for unlimited (backward compatible). " +
      "Each entry uses ~56 bytes. Recommended: 100000 for datasets with 1M+ vectors (~5.6MB), " +
      "-1 for smaller datasets.",
      Integer.class, -1),

  VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE("arcadedb.vectorIndex.graphBuildCacheSize", SCOPE.DATABASE,
      "Maximum number of vectors to cache in memory during HNSW graph building. " +
      "Higher values speed up construction but use more RAM. " +
      "RAM usage = cacheSize * (dimensions * 4 + 64) bytes. " +
      "Recommended: 100000 for 768-dim vectors (~30MB), scale based on dimensionality.",
      Integer.class, 100_000),

  VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD("arcadedb.vectorIndex.mutationsBeforeRebuild", SCOPE.DATABASE,
      "Number of mutations (inserts/updates/deletes) before rebuilding the HNSW graph index. " +
      "Higher values reduce rebuild cost but may return slightly stale results in queries. " +
      "Lower values provide fresher results but rebuild more frequently. " +
      "Recommended: 50-200 for read-heavy, 200-500 for write-heavy workloads.",
      Integer.class, 100),

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
      "development", Set.of((Object[]) new String[] { "development", "test", "production" })),

  // Metrics
  SERVER_METRICS("arcadedb.serverMetrics", SCOPE.SERVER, "True to enable metrics", Boolean.class, true),

  SERVER_METRICS_LOGGING("arcadedb.serverMetrics.logging", SCOPE.SERVER, "True to enable metrics logging", Boolean.class, false),

  //paths
  SERVER_ROOT_PATH("arcadedb.server.rootPath", SCOPE.SERVER,
      "Root path in the file system where the server is looking for files. By default is the current directory", String.class,
      null),

  SERVER_DATABASE_DIRECTORY("arcadedb.server.databaseDirectory", SCOPE.JVM, "Directory containing the database", String.class,
      "${arcadedb.server.rootPath}/databases"),

  SERVER_BACKUP_DIRECTORY("arcadedb.server.backupDirectory", SCOPE.JVM, "Directory containing the backups", String.class,
      "${arcadedb.server.rootPath}/backups"),

  SERVER_DATABASE_LOADATSTARTUP("arcadedb.server.databaseLoadAtStartup", SCOPE.SERVER,
      "Open all the available databases at server startup", Boolean.class, true),

  SERVER_DEFAULT_DATABASES("arcadedb.server.defaultDatabases", SCOPE.SERVER, """
      The default databases created when the server starts. The format is `(<database-name>[(<user-name>:<user-passwd>[:<user-group>])[,]*])[{import|restore:<URL>}][;]*'. Pay attention on using `;`\
       to separate databases and `,` to separate credentials. The supported actions are `import` and `restore`. Example: `Universe[albert:einstein:admin];Amiga[Jay:Miner,Jack:Tramiel]{import:/tmp/movies.tgz}`""",
      String.class, ""),

  SERVER_DEFAULT_DATABASE_MODE("arcadedb.server.defaultDatabaseMode", SCOPE.SERVER, """
      The default mode to load pre-existing databases. The value must match a com.arcadedb.engine.PaginatedFile.MODE enum value: {READ_ONLY, READ_WRITE}\
      Databases which are newly created will always be opened READ_WRITE.""", String.class, "READ_WRITE",
      Set.of((Object[]) new String[] { "read_only", "read_write" })),

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
      Integer.class, 0, null, (value) -> Runtime.getRuntime().availableProcessors()),

  SERVER_HTTP_SESSION_EXPIRE_TIMEOUT("arcadedb.server.httpSessionExpireTimeout", SCOPE.SERVER,
      "Timeout in seconds for a HTTP session (managing a transaction) to expire. This timeout is computed from the latest command against the session",
      Long.class, 5), // 5 SECONDS DEFAULT

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

  // HA
  HA_ENABLED("arcadedb.ha.enabled", SCOPE.SERVER, "True if HA is enabled for the current server", Boolean.class, false),

  HA_ERROR_RETRIES("arcadedb.ha.errorRetries", SCOPE.SERVER,
      "Number of automatic retries in case of IO errors with a specific server. If replica servers are configured, the operation will be retried a specific amount of times on the next server in the list. 0 (default) is to retry against all the configured servers",
      Integer.class, 0),

  HA_SERVER_ROLE("arcadedb.ha.serverRole", SCOPE.SERVER,
      "Server role between ANY (default) OR REPLICA to configure replica only servers", String.class, "any",
      Set.of((Object[]) new String[] { "any", "replica" })),

  HA_CLUSTER_NAME("arcadedb.ha.clusterName", SCOPE.SERVER,
      "Cluster name. By default is 'arcadedb'. Useful in case of multiple clusters in the same network", String.class,
      Constants.PRODUCT.toLowerCase(Locale.ENGLISH)),

  HA_SERVER_LIST("arcadedb.ha.serverList", SCOPE.SERVER,
      "Servers in the cluster as a list of <hostname/ip-address:port> items separated by comma. Example: localhost:2424,192.168.0.1:2424",
      String.class, ""),

  HA_QUORUM("arcadedb.ha.quorum", SCOPE.SERVER,
      "Default quorum between 'none', one, two, three, 'majority' and 'all' servers. Default is majority", String.class, "majority",
      Set.of(new String[] { "none", "one", "two", "three", "majority", "all" })),

  HA_QUORUM_TIMEOUT("arcadedb.ha.quorumTimeout", SCOPE.SERVER, "Timeout waiting for the quorum", Long.class, 10000),

  HA_REPLICATION_QUEUE_SIZE("arcadedb.ha.replicationQueueSize", SCOPE.SERVER, "Queue size for replicating messages between servers",
      Integer.class, 512),

  // TODO: USE THIS FOR CREATING NEW FILES
  HA_REPLICATION_FILE_MAXSIZE("arcadedb.ha.replicationFileMaxSize", SCOPE.SERVER,
      "Maximum file size for replicating messages between servers. Default is 1GB", Long.class, 1024 * 1024 * 1024),

  HA_REPLICATION_CHUNK_MAXSIZE("arcadedb.ha.replicationChunkMaxSize", SCOPE.SERVER,
      "Maximum channel chunk size for replicating messages between servers. Default is 16777216", Integer.class, 16384 * 1024),

  HA_REPLICATION_INCOMING_HOST("arcadedb.ha.replicationIncomingHost", SCOPE.SERVER,
      "TCP/IP host name used for incoming replication connections. By default is 0.0.0.0 (listens to all the configured network interfaces)",
      String.class, "0.0.0.0"),

  HA_REPLICATION_INCOMING_PORTS("arcadedb.ha.replicationIncomingPorts", SCOPE.SERVER,
      "TCP/IP port number used for incoming replication connections", String.class, "2424-2433"),

  // KUBERNETES
  HA_K8S("arcadedb.ha.k8s", SCOPE.SERVER, "The server is running inside Kubernetes", Boolean.class, false),

  HA_K8S_DNS_SUFFIX("arcadedb.ha.k8sSuffix", SCOPE.SERVER,
      "When running inside Kubernetes use this suffix to reach the other servers. Example: arcadedb.default.svc.cluster.local",
      String.class, ""),

  // POSTGRES
  POSTGRES_PORT("arcadedb.postgres.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Postgres plugin. Default is 5432", Integer.class, 5432),

  POSTGRES_HOST("arcadedb.postgres.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Postgres plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  POSTGRES_DEBUG("arcadedb.postgres.debug", SCOPE.SERVER,
      "Enables the printing of Postgres protocol to the console. Default is false", Boolean.class, false),

  // REDIS
  REDIS_PORT("arcadedb.redis.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Redis plugin. Default is 6379", Integer.class, 6379),

  REDIS_HOST("arcadedb.redis.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Redis plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  // MONGO
  MONGO_PORT("arcadedb.mongo.port", SCOPE.SERVER,
      "TCP/IP port number used for incoming connections for Mongo plugin. Default is 27017", Integer.class, 27017),

  MONGO_HOST("arcadedb.mongo.host", SCOPE.SERVER,
      "TCP/IP host name used for incoming connections for Mongo plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),
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
  private volatile     Object                   value  = nullValue;
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
   *
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
    return value != defValue;
  }

  /**
   * @return Value of configuration parameter stored as enumeration if such one exists.
   *
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
    return hidden;
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
