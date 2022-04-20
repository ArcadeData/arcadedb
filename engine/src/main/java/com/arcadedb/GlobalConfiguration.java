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

import com.arcadedb.engine.Bucket;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.utility.Callable;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.SystemVariableResolver;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.logging.*;

/**
 * Keeps all configuration settings. At startup assigns the configuration values by reading system properties.
 */
public enum GlobalConfiguration {
  // ENVIRONMENT
  DUMP_CONFIG_AT_STARTUP("arcadedb.dumpConfigAtStartup", "Dumps the configuration at startup", Boolean.class, false, value -> {
    dumpConfiguration(System.out);
    return value;
  }),

  DUMP_METRICS_EVERY("arcadedb.dumpMetricsEvery", "Dumps the metrics at startup, shutdown and every configurable amount of time (in seconds)", Long.class, 0,
      new Callable<>() {
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

  PROFILE("arcadedb.profile", "Specify the preferred profile among: default, high-performance, low-ram, low-cpu", String.class, "default", new Callable<>() {
    @Override
    public Object call(final Object value) {
      final String v = value.toString();
      if (v.equalsIgnoreCase("default")) {
        // NOT MUCH TO DO HERE, THIS IS THE DEFAULT OPTION
      } else if (v.equalsIgnoreCase("high-performance")) {
        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("fast");

        final int cores = Runtime.getRuntime().availableProcessors();
        if (cores > 1)
          // USE ONLY HALF OF THE CORES MINUS ONE
          ASYNC_WORKER_THREADS.setValue((cores / 2) - 1);
        else
          ASYNC_WORKER_THREADS.setValue(1);

      } else if (v.equalsIgnoreCase("low-ram")) {
        MAX_PAGE_RAM.setValue(16); // 16 MB OF RAM FOR PAGE CACHE
        INDEX_COMPACTION_RAM_MB.setValue(16);
        INITIAL_PAGE_CACHE_SIZE.setValue(256);
        FREE_PAGE_RAM.setValue(100);
        ASYNC_OPERATIONS_QUEUE_SIZE.setValue(8);
        ASYNC_TX_BATCH_SIZE.setValue(8);
        PAGE_FLUSH_QUEUE.setValue(8);
        SQL_STATEMENT_CACHE.setValue(16);
        HA_REPLICATION_QUEUE_SIZE.setValue(8);
        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("standard");

      } else if (v.equalsIgnoreCase("low-cpu")) {
        ASYNC_WORKER_THREADS.setValue(1);
        ASYNC_OPERATIONS_QUEUE_IMPL.setValue("standard");
      } else
        throw new IllegalArgumentException("Profile '" + v + "' not available");

      return value;
    }
  }),

  TEST("arcadedb.test", "Tells if it is running in test mode. This enables the calling of callbacks for testing purpose ", Boolean.class, false),

  MAX_PAGE_RAM("arcadedb.maxPageRAM", "Maximum amount of pages (in MB) to keep in RAM", Long.class, 4 * 1024, new Callable<>() {
    @Override
    public Object call(final Object value) {
      final long maxRAM = ((long) value) * 1024 * 1024; // VALUE IN MB

      if (maxRAM > Runtime.getRuntime().maxMemory() * 80 / 100) {
        final long newValue = Runtime.getRuntime().maxMemory() / 2;
        if (LogManager.instance() != null)
          LogManager.instance().log(this, Level.WARNING, "Setting '%s=%s' is > than 80%% of maximum heap (%s). Decreasing it to %s", MAX_PAGE_RAM.key,
              FileUtils.getSizeAsString(maxRAM), FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), FileUtils.getSizeAsString(newValue));
        else
          System.out.println(
              String.format("Setting '%s=%s' is > than 80%% of maximum heap (%s). Decreasing it to %s", MAX_PAGE_RAM.key, FileUtils.getSizeAsString(maxRAM),
                  FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()), FileUtils.getSizeAsString(newValue)));

        return newValue;
      }
      return value;
    }
  }, value -> Runtime.getRuntime().maxMemory() / 4 / 1024 / 1024),

  INITIAL_PAGE_CACHE_SIZE("arcadedb.initialPageCacheSize", "Initial number of entries for page cache", Integer.class, 65535),

  FLUSH_ONLY_AT_CLOSE("arcadedb.flushOnlyAtClose", "Never flushes pages on disk until the database closing", Boolean.class, false),

  TX_WAL("arcadedb.txWAL", "Uses the WAL", Boolean.class, true),

  TX_WAL_FLUSH("arcadedb.txWalFlush", "Flushes the WAL on disk at commit time. It can be 0 = no flush, 1 = flush without metadata and 2 = full flush (fsync)",
      Integer.class, 0),

  FREE_PAGE_RAM("arcadedb.freePageRAM", "Percentage (0-100) of memory to free when Page RAM is full", Integer.class, 50),

  TYPE_DEFAULT_BUCKETS("arcadedb.typeDefaultBuckets", "Default number of buckets to create per type", Integer.class, 8),

  BUCKET_DEFAULT_PAGE_SIZE("arcadedb.bucketDefaultPageSize", "Default page size in bytes for buckets. Default is " + Bucket.DEF_PAGE_SIZE, Integer.class,
      Bucket.DEF_PAGE_SIZE),

  ASYNC_WORKER_THREADS("arcadedb.asyncWorkerThreads", "Number of asynchronous worker threads. 0 (default) = available cores minus 1", Integer.class,
      Runtime.getRuntime().availableProcessors() > 1 ? Runtime.getRuntime().availableProcessors() - 1 : 1),

  ASYNC_OPERATIONS_QUEUE_IMPL("arcadedb.asyncOperationsQueueImpl",
      "Queue implementation to use between 'standard' and 'fast'. 'standard' consumes less CPU than the 'fast' implementation, but it could be slower with high loads",
      String.class, "standard"),

  ASYNC_OPERATIONS_QUEUE_SIZE("arcadedb.asyncOperationsQueueSize",
      "Size of the total asynchronous operation queues (it is divided by the number of parallel threads in the pool)", Integer.class, 1024),

  ASYNC_TX_BATCH_SIZE("arcadedb.asyncTxBatchSize", "Maximum number of operations to commit in batch by async thread", Integer.class, 1024 * 10),

  PAGE_FLUSH_QUEUE("arcadedb.pageFlushQueue", "Size of the asynchronous page flush queue", Integer.class, 512),

  COMMIT_LOCK_TIMEOUT("arcadedb.commitLockTimeout", "Timeout in ms to lock resources during commit", Long.class, 5000),

  TX_RETRIES("arcadedb.txRetries", "Number of retries in case of MVCC exception", Integer.class, 3),

  // SQL
  SQL_STATEMENT_CACHE("arcadedb.sqlStatementCache", "Maximum number of parsed statements to keep in cache", Integer.class, 300),

  // COMMAND
  COMMAND_TIMEOUT("arcadedb.command.timeout", "Default timeout for commands (in ms)", Long.class, 0),

  // USER CODE
  POLYGLOT_COMMAND_TIMEOUT("arcadedb.polyglotCommand.timeout", "Default timeout for polyglot commands (in ms)", Long.class, 10_000),

  QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP("arcadedb.queryMaxHeapElementsAllowedPerOp",
      "Maximum number of elements (records) allowed in a single query for memory-intensive operations (eg. ORDER BY in heap). "
          + "If exceeded, the query fails with an OCommandExecutionException. Negative number means no limit."
          + "This setting is intended as a safety measure against excessive resource consumption from a single query (eg. prevent OutOfMemory)", Long.class,
      500_000),

  // INDEXES
  INDEX_COMPACTION_RAM_MB("arcadedb.indexCompactionRAM", "Maximum amount of RAM to use for index compaction, in MB", Long.class, 300),

  INDEX_COMPACTION_MIN_PAGES_SCHEDULE("arcadedb.indexCompactionMinPagesSchedule",
      "Minimum number of mutable pages for an index to be schedule for automatic compaction. 0 = disabled", Integer.class, 10),

  // NETWORK
  NETWORK_SOCKET_BUFFER_SIZE("arcadedb.network.socketBufferSize", "TCP/IP Socket buffer size, if 0 use the OS default", Integer.class, 0),

  NETWORK_SOCKET_TIMEOUT("arcadedb.network.socketTimeout", "TCP/IP Socket timeout (in ms)", Integer.class, 30000),

  NETWORK_USE_SSL("arcadedb.ssl.enabled", "Use SSL for client connections", Boolean.class, false),

  NETWORK_SSL_KEYSTORE("arcadedb.ssl.keyStore", "Path where the SSL certificates are stored", String.class, null),

  NETWORK_SSL_KEYSTORE_PASSWORD("arcadedb.ssl.keyStorePass", "Password to open the SSL key store", String.class, null),

  NETWORK_SSL_TRUSTSTORE("arcadedb.ssl.trustStore", "Path to the SSL trust store", String.class, null),

  NETWORK_SSL_TRUSTSTORE_PASSWORD("arcadedb.ssl.trustStorePass", "Password to open the SSL trust store", String.class, null),

  // SERVER
  SERVER_NAME("arcadedb.server.name", "Server name", String.class, Constants.PRODUCT + "_0"),

  SERVER_ROOT_PASSWORD("arcadedb.server.rootPassword",
      "Password for root user to use at first startup of the server. Set this to avoid asking the password to the user", String.class, null),

  SERVER_MODE("arcadedb.server.mode", "Server mode between development, test and production", String.class, "development"),

  SERVER_METRICS("arcadedb.serverMetrics", "True to enable metrics", Boolean.class, true),

  SERVER_ROOT_PATH("arcadedb.server.rootPath", "Root path in the file system where the server is looking for files. By default is the current directory",
      String.class, null),

  SERVER_DATABASE_DIRECTORY("arcadedb.server.databaseDirectory", "Directory containing the database", String.class, "${arcadedb.server.rootPath}/databases"),

  SERVER_PLUGINS("arcadedb.server.plugins", "List of server plugins to install. The format to load a plugin is: `<pluginName>:<pluginFullClass>`", String.class,
      ""),

  SERVER_DEFAULT_DATABASES("arcadedb.server.defaultDatabases",
      "The default databases created when the server starts. The format is `(<database-name>[(<user-name>:<user-passwd>[:<user-group>])[,]*])[{import|restore:<URL>}][;]*'. Pay attention on using `;`"
          + " to separate databases and `,` to separate credentials. The supported actions are `import` and `restore`. Example: `Universe[elon:musk:admin];Amiga[Jay:Miner,Jack:Tramiel]{import:/tmp/movies.tgz}`",
      String.class, ""),

  // SERVER HTTP
  SERVER_HTTP_INCOMING_HOST("arcadedb.server.httpIncomingHost", "TCP/IP host name used for incoming HTTP connections", String.class, "0.0.0.0"),

  SERVER_HTTP_INCOMING_PORT("arcadedb.server.httpIncomingPort",
      "TCP/IP port number used for incoming HTTP connections. Specify a single port or a range `<from-<to>`. Default is 2480-2489 to accept a range of ports in case they are occupied.",
      String.class, "2480-2489"),

  SERVER_HTTP_TX_EXPIRE_TIMEOUT("arcadedb.server.httpTxExpireTimeout",
      "Timeout in seconds for a HTTP transaction to expire. This timeout is computed from the latest command against the transaction", Long.class, 30),

  // SERVER WS
  SERVER_WS_EVENT_BUS_QUEUE_SIZE("arcadedb.server.eventBusQueueSize", "Size of the queue used as a buffer for unserviced database change events.",
      Integer.class, 1000),

  // SERVER SECURITY
  SERVER_SECURITY_ALGORITHM("arcadedb.server.securityAlgorithm", "Default encryption algorithm used for passwords hashing", String.class,
      "PBKDF2WithHmacSHA256"),

  SERVER_SECURITY_SALT_CACHE_SIZE("arcadedb.server.securitySaltCacheSize",
      "Cache size of hashed salt passwords. The cache works as LRU. Use 0 to disable the cache", Integer.class, 64),

  SERVER_SECURITY_SALT_ITERATIONS("arcadedb.server.saltIterations",
      "Number of iterations to generate the salt or user password. Changing this setting does not affect stored passwords", Integer.class, 65536),

  // HA
  HA_ENABLED("arcadedb.ha.enabled", "True if HA is enabled for the current server", Boolean.class, false),

  HA_CLUSTER_NAME("arcadedb.ha.clusterName", "Cluster name. By default is 'arcadedb'. Useful in case of multiple clusters in the same network", String.class,
      Constants.PRODUCT.toLowerCase()),

  HA_SERVER_LIST("arcadedb.ha.serverList",
      "Servers in the cluster as a list of <hostname/ip-address:port> items separated by comma. Example: localhost:2424,192.168.0.1:2424", String.class, ""),

  HA_QUORUM("arcadedb.ha.quorum", "Default quorum between 'none', 1, 2, 3, 'majority' and 'all' servers. Default is majority", String.class, "MAJORITY"),

  HA_QUORUM_TIMEOUT("arcadedb.ha.quorumTimeout", "Timeout waiting for the quorum", Long.class, 10000),

  HA_REPLICATION_QUEUE_SIZE("arcadedb.ha.replicationQueueSize", "Queue size for replicating messages between servers", Integer.class, 512),

  // TODO: USE THIS FOR CREATING NEW FILES
  HA_REPLICATION_FILE_MAXSIZE("arcadedb.ha.replicationFileMaxSize", "Maximum file size for replicating messages between servers. Default is 1GB", Long.class,
      1024 * 1024 * 1024),

  HA_REPLICATION_CHUNK_MAXSIZE("arcadedb.ha.replicationChunkMaxSize",
      "Maximum channel chunk size for replicating messages between servers. Default is 16777216", Integer.class, 16384 * 1024),

  HA_REPLICATION_INCOMING_HOST("arcadedb.ha.replicationIncomingHost",
      "TCP/IP host name used for incoming replication connections. By default is 0.0.0.0 (listens to all the configured network interfaces)", String.class,
      "0.0.0.0"),

  HA_REPLICATION_INCOMING_PORTS("arcadedb.ha.replicationIncomingPorts", "TCP/IP port number used for incoming replication connections", String.class,
      "2424-2433"),

  HA_K8S("arcadedb.ha.k8s", "The server is running inside Kubernetes", Boolean.class, false),

  HA_K8S_DNS_SUFFIX("arcadedb.ha.k8sSuffix",
      "When running inside Kubernetes use this suffix to reach the other servers. Example: arcadedb.default.svc.cluster.local", String.class, ""),

  // CYPHER
  CYPHER_STATEMENT_CACHE("arcadedb.cypher.statementCache",
      "Max number of entries in the cypher statement cache. Use 0 to disable. Caching statements speeds up execution of the same cypher queries", Integer.class,
      1000),

  // GREMLIN

  // POSTGRES
  POSTGRES_PORT("arcadedb.postgres.port", "TCP/IP port number used for incoming connections for Postgres plugin. Default is 5432", Integer.class, 5432),

  POSTGRES_HOST("arcadedb.postgres.host", "TCP/IP host name used for incoming connections for Postgres plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),

  POSTGRES_DEBUG("arcadedb.postgres.debug", "Enables the printing of Postgres protocol to the console. Default is false", Boolean.class, false),

  // REDIS
  REDIS_PORT("arcadedb.redis.port", "TCP/IP port number used for incoming connections for Redis plugin. Default is 6379", Integer.class, 6379),

  REDIS_HOST("arcadedb.redis.host", "TCP/IP host name used for incoming connections for Redis plugin. Default is '0.0.0.0'", String.class, "0.0.0.0"),
  ;

  /**
   * Place holder for the "undefined" value of setting.
   */
  private final Object nullValue = new Object();

  private final       String                   key;
  private final       Object                   defValue;
  private final       Class<?>                 type;
  private final       Callable<Object, Object> callback;
  private final       Callable<Object, Object> callbackIfNoSet;
  private volatile    Object                   value  = nullValue;
  private final       String                   description;
  private final       Boolean                  canChangeAtRuntime;
  private final       boolean                  hidden;
  public final static String                   PREFIX = "arcadedb.";

  private static final Timer TIMER;

  static {
    TIMER = new Timer(true);
    readConfiguration();
  }

  GlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue) {
    this(iKey, iDescription, iType, iDefValue, null);
  }

  GlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue, final Callable<Object, Object> callback) {
    this.key = iKey;
    this.description = iDescription;
    this.defValue = iDefValue;
    this.type = iType;
    this.canChangeAtRuntime = true;
    this.hidden = false;
    this.callback = callback;
    this.callbackIfNoSet = null;
  }

  GlobalConfiguration(final String iKey, final String iDescription, final Class<?> iType, final Object iDefValue, final Callable<Object, Object> callback,
      final Callable<Object, Object> callbackIfNoSet) {
    this.key = iKey;
    this.description = iDescription;
    this.defValue = iDefValue;
    this.type = iType;
    this.canChangeAtRuntime = true;
    this.hidden = false;
    this.callback = callback;
    this.callbackIfNoSet = callbackIfNoSet;
  }

  /**
   * Reset all the configurations to the default values.
   */
  public static void resetAll() {
    for (GlobalConfiguration v : values())
      v.reset();
  }

  /**
   * Reset the configuration to the default value.
   */
  public void reset() {
    value = defValue;
  }

  public static void dumpConfiguration(final PrintStream out) {
    out.print("ARCADEDB ");
    out.print(Constants.getRawVersion());
    out.println(" configuration:");

    String lastSection = "";
    for (GlobalConfiguration v : values()) {
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
  }

  public static void fromJSON(final String input) {
    if (input == null)
      return;

    final JSONObject json = new JSONObject(input);
    final JSONObject cfg = json.getJSONObject("configuration");
    for (String k : cfg.keySet()) {
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

    for (GlobalConfiguration k : values()) {
      cfg.put(k.key.substring(PREFIX.length()), (Object) k.getValue());
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
    for (GlobalConfiguration v : values()) {
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
    for (Map.Entry<String, Object> config : iConfig.entrySet()) {
      for (GlobalConfiguration v : values()) {
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
  private static void readConfiguration() {
    String prop;

    for (GlobalConfiguration config : values()) {
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
    return value != nullValue;
  }

  /**
   * @return Value of configuration parameter stored as enumeration if such one exists.
   *
   * @throws ClassCastException       if stored value can not be casted and parsed from string to passed in enumeration class.
   * @throws IllegalArgumentException if value associated with configuration parameter is a string bug can not be converted to
   *                                  instance of passed in enumeration class.
   */
  public <T extends Enum<T>> T getValueAsEnum(Class<T> enumType) {
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
      } else if (iValue instanceof String) {
        final String string = (String) iValue;

        for (Object constant : type.getEnumConstants()) {
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
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE, "Error during setting property %s=%s", e, key, value);
      }
  }

  public boolean getValueAsBoolean() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Boolean ? (Boolean) v : Boolean.parseBoolean(v.toString());
  }

  public String getValueAsString() {
    return value != nullValue && value != null ?
        SystemVariableResolver.INSTANCE.resolveSystemVariables(value.toString(), "") :
        defValue != null ? SystemVariableResolver.INSTANCE.resolveSystemVariables(defValue.toString(), "") : null;
  }

  public int getValueAsInteger() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return (int) (v instanceof Number ? ((Number) v).intValue() : FileUtils.getSizeAsNumber(v.toString()));
  }

  public long getValueAsLong() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Number ? ((Number) v).longValue() : FileUtils.getSizeAsNumber(v.toString());
  }

  public float getValueAsFloat() {
    final Object v = value != nullValue && value != null ? value : defValue;
    return v instanceof Float ? (Float) v : Float.parseFloat(v.toString());
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
}
