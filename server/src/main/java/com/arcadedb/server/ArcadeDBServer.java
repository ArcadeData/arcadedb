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
package com.arcadedb.server;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinary;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ai.AiConfiguration;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.server.event.FileServerEventLog;
import com.arcadedb.server.event.ServerEventLog;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import com.arcadedb.database.QueryMetricsRecorder;
import com.arcadedb.database.QueryTracer;
import com.arcadedb.server.monitor.EngineMetricsBinder;
import com.arcadedb.server.monitor.MicrometerQueryMetricsRecorder;
import com.arcadedb.server.monitor.MicrometerQueryTracer;
import com.arcadedb.server.monitor.HAReplicationMetrics;
import com.arcadedb.server.monitor.PoolMetrics;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.plugin.PluginManager;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.ServerPathUtils;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.logging.Level;

import static com.arcadedb.engine.ComponentFile.MODE.READ_ONLY;
import static com.arcadedb.engine.ComponentFile.MODE.READ_WRITE;

public class ArcadeDBServer {
  public enum STATUS {OFFLINE, STARTING, ONLINE, SHUTTING_DOWN}

  public static final String                                CONFIG_SERVER_CONFIGURATION_FILENAME = "config/server-configuration.json";

  /**
   * Prefix that marks reserved internal databases (e.g. the Raft control directory {@code .raft}).
   * Reserved databases may live under the server database directory but are not user databases: they
   * must not be registered at startup, nor exposed through the server/cluster status APIs.
   */
  public static final String                                RESERVED_DATABASE_PREFIX             = ".";

  /**
   * How long the shutdown hook waits for the lifecycle lock when the server is still {@code STARTING} and has not
   * opened any database yet (issues #5418, #7025). Short on purpose: with no database open there is nothing worth
   * flushing, so the wait only has to be long enough to lose a benign race with a start that is about to complete.
   * Once {@code loadDatabases()} has run - which happens well before the status turns {@code ONLINE}, ahead of the
   * HTTP service and every plugin phase - the bound is {@code arcadedb.server.shutdownTimeout} instead, because
   * cutting the wait short then leaves every open database to a WAL replay on the next start.
   */
  // @VisibleForTesting
  static final         long                                 SHUTDOWN_HOOK_STARTING_TIMEOUT_MS    = 2_000;

  /**
   * Which bound the shutdown hook applies while waiting for the lifecycle lock (issue #7025). Chosen by
   * {@link #chooseShutdownHookBound}, and reported by name in the WARNING the hook emits when the wait expires, so
   * the operator is told which setting - if any - governs the wait they just saw.
   */
  enum ShutdownHookBound {
    /**
     * The thread holding the lock is itself parked inside {@code System.exit()}, waiting for this very hook to
     * finish (issue #5418: Apache Ratis exits from inside {@code start()} on a port conflict). It can never release
     * the lock, so waiting any time at all only delays the exit.
     */
    HOLDER_EXITING,
    /** {@code STARTING} with no database open yet: nothing to flush, wait only {@link #SHUTDOWN_HOOK_STARTING_TIMEOUT_MS}. */
    STARTING_NO_DATABASES,
    /** Databases are open, or the status left {@code STARTING}: wait up to {@code arcadedb.server.shutdownTimeout}. */
    CONFIGURED
  }
  private final       ContextConfiguration                  configuration;
  private final       String                                serverName;
  private             String                                hostAddress;
  private final       boolean                               replicationLifecycleEventsEnabled;
  // Shared spine for Micrometer Observations. Starts with no handlers, so Observations are no-ops
  // (zero overhead) until the optional tracing plugin attaches a tracing handler. Metrics continue
  // to be recorded by the dedicated Micrometer timers, independently of this registry.
  private final       ObservationRegistry                   observationRegistry                  = ObservationRegistry.create();
  private             FileServerEventLog                    eventLog;
  private             PluginManager                         pluginManager;
  private             String                                serverRootPath;
  // volatile: written by the HA plugin during startPlugins(AFTER_HTTP_ON), i.e. after httpServer.startService()
  // has begun accepting requests. Undertow worker threads reading these (readiness/cluster handlers, getDatabase's
  // HA wrapping) need a happens-before with those writes, otherwise under the JMM they may observe null indefinitely.
  private volatile    HAServerPlugin                        haServer;
  private volatile    ServerSecurity                        security;
  private volatile    HttpServer                            httpServer;
  private             AiConfiguration                       aiConfiguration;
  private             ServerQueryProfiler                   queryProfiler;
  // Admission for backups of a database, shared by every entry point that can start one on this server: the
  // auto-backup schedule, its immediate trigger, and the HTTP "trigger backup" command (issue #6753). Created with
  // the server rather than with the auto-backup plugin, because the HTTP command backs a database up whether or not
  // that plugin is enabled.
  private final       BackupCoordinator                     backupCoordinator                    = new BackupCoordinator();
  private final       ConcurrentMap<String, ServerDatabase> databases                            = new ConcurrentHashMap<>();
  // Monitor serialising every check-then-act on the database registry (load, create, register, reopen). The HA
  // snapshot installer also holds it across close->file-swap->reopen so no concurrent open observes the transient
  // half-swapped directory or reopens it from disk mid-swap (issue #4832). Kept distinct from the map so it can be
  // exposed via getDatabasesLock() without leaking the registry itself.
  private final       Object                                databasesLock                        = new Object();
  // Serialises start() and stop(). Deliberately an explicit lock rather than `synchronized` on the
  // instance: the JVM shutdown hook must be able to give up on it (see stopFromShutdownHook), because a
  // startup failure that calls System.exit() from inside start() would otherwise deadlock the JVM
  // permanently (issue #5418). Reentrant because start() calls stop() on its failure path.
  private final       ReentrantLock                         lifecycleLock                        = new ReentrantLock();
  /**
   * The thread currently holding {@link #lifecycleLock} through {@link #start()} or {@link #stop()}, or null. Read
   * by the shutdown hook (issue #7025): when that thread is parked inside {@code System.exit()} it is waiting for the
   * hook itself and will never release the lock, so the hook does not wait for it at all.
   */
  private volatile    Thread                                lifecycleOwner;
  private final       List<ReplicationCallback>             testEventListeners                   = new ArrayList<>();
  private volatile    STATUS                                status                               = STATUS.OFFLINE;
  private final       AtomicBoolean snapshotInstallInProgress            = new AtomicBoolean(false);
  private volatile    Function<LocalDatabase, DatabaseInternal> databaseWrapper;
  // Micrometer's global composite registry, the meter binders and the per-tuple timer caches of
  // MicrometerQueryMetricsRecorder/AbstractServerHttpHandler are JVM-wide, while a start()/stop() pair is
  // not: a start that adds a backing registry and a stop that removes none grows the composite by one
  // child per in-process restart. A composite meter answers count() from a single, arbitrarily chosen
  // child, and a child added after a recording starts at zero, so meters fed by an earlier generation
  // still resolve through find() but read back 0 (issue #5565). The install is therefore reference
  // counted - HA and embedded setups run several servers in one JVM - and the last one out dismantles it.
  private static final Object       METRICS_INSTALL_MUTEX = new Object();
  private static       int          metricsInstalls;
  private static       boolean      metricsFiltersInstalled;
  private static       Set<Meter.Id> metersBeforeInstall;
  private              boolean       metricsInstalled;
  private              MeterRegistry metricsRegistry;
  private              MeterRegistry metricsLoggingRegistry;
  private              JvmGcMetrics  metricsJvmGc;
  // Holds the per-follower gauge refresh scheduler open; must be closed on stop or the daemon
  // thread it starts leaks one instance per restart (issue #5850).
  private              HAReplicationMetrics haReplicationMetrics;
//  private             ServerMonitor                         serverMonitor;

  static {
    // must be called before any Logger method is used.
    System.setProperty("java.util.logging.manager", ServerLogManager.class.getName());
    LogManager.instance();
    ServerLogManager.enableReset(false);
  }

  public ArcadeDBServer() {
    this.configuration = new ContextConfiguration();
    serverRootPath = ServerPathUtils.setRootPath(configuration);
    loadConfiguration();
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.replicationLifecycleEventsEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
    init();
  }

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.configuration = configuration;
    serverRootPath = ServerPathUtils.setRootPath(configuration);
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.replicationLifecycleEventsEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
    init();
  }

  public static void main(final String[] args) {
    new ArcadeDBServer().start();
  }

  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  /**
   * Shared Micrometer {@link ObservationRegistry} used to instrument server hot paths once and emit
   * both metrics and (when the optional tracing plugin registers a tracer) spans. With no tracer
   * attached the registry has no handlers and Observations are no-ops.
   */
  public ObservationRegistry getObservationRegistry() {
    return observationRegistry;
  }

  public void setSnapshotInstallInProgress(final boolean inProgress) {
    snapshotInstallInProgress.set(inProgress);
  }

  public boolean isSnapshotInstallInProgress() {
    return snapshotInstallInProgress.get();
  }

  public void start() {
    // #5418: the lifecycle mutex is an explicit ReentrantLock rather than `synchronized`, so the JVM
    // shutdown hook can wait for it with a TIMEOUT. See stopFromShutdownHook() for why that matters.
    // Reentrant because start() calls stop() on its own failure path.
    lifecycleLock.lock();
    try {
      recordLifecycleOwner();
      startInternal();
    } catch (final RuntimeException | Error e) {
      // A start that fails after the metrics install (a plugin, the HTTP service, the databases) does not
      // necessarily reach stop(): callers own that decision. The reference count must be released anyway,
      // otherwise it never returns to zero and the last-one-out teardown is disabled for the life of the
      // JVM - the shared meters, timer caches and recorder of a server that never came up would survive
      // every later stop. Only the metrics install is undone here; the rest of the failure path is
      // unchanged, and a stop() that follows finds nothing left to dismantle.
      CodeUtils.executeIgnoringExceptions(this::stopMetrics, "Error on stopping the metrics collection", false);
      throw e;
    } finally {
      clearLifecycleOwner();
      lifecycleLock.unlock();
    }
  }

  /** Publishes the current thread as {@link #lifecycleOwner} on the outermost acquisition of the lifecycle lock. */
  private void recordLifecycleOwner() {
    if (lifecycleLock.getHoldCount() == 1)
      lifecycleOwner = Thread.currentThread();
  }

  /** Counterpart of {@link #recordLifecycleOwner()}, called before the outermost release. */
  private void clearLifecycleOwner() {
    if (lifecycleLock.getHoldCount() == 1)
      lifecycleOwner = null;
  }

  private void startInternal() {
    LogManager.instance().setContext(getServerName());

    welcomeBanner();

    if (status != STATUS.OFFLINE)
      return;

    status = STATUS.STARTING;

    eventLog.start();

    try {
      lifecycleEvent(ReplicationCallback.TYPE.SERVER_STARTING, null);
    } catch (final Exception e) {
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }

    // Discover plugins from lib/plugins directory
    pluginManager.discoverPlugins();

    LogManager.instance().log(this, Level.INFO, "Starting ArcadeDB Server in %s mode with plugins %s ...",
        GlobalConfiguration.SERVER_MODE.getValueAsString(),
        pluginManager != null && !pluginManager.getPluginNames().isEmpty() ?
            pluginManager.getPluginNames() : getAllPluginNames());

    // IN PRODUCTION MODE, APPLY SAFE DEFAULTS
    if ("production".equals(GlobalConfiguration.SERVER_MODE.getValueAsString())) {
      // WAL FLUSH: DEFAULT TO 1 FOR DURABILITY
      if (!GlobalConfiguration.TX_WAL_FLUSH.isChanged()) {
        GlobalConfiguration.TX_WAL_FLUSH.setValue(1);
        LogManager.instance().log(this, Level.INFO,
            """
            Production mode: WAL flush automatically set to 1 (flush without metadata) for durability. \
            Set arcadedb.txWalFlush=0 explicitly to disable or =2 for full fsync""");
      } else if (GlobalConfiguration.TX_WAL_FLUSH.getValueAsInteger() == 0) {
        LogManager.instance().log(this, Level.WARNING,
            """
            WAL flush is explicitly disabled (arcadedb.txWalFlush=0) in production mode. \
            Committed transactions may be lost on power failure or OS crash \
            unless your storage has battery-backed write cache or power-loss protection. \
            Set arcadedb.txWalFlush=1 or =2 for durability on standard hardware""");
      }

      // LOAD CSV: DISABLE LOCAL FILE ACCESS FOR SECURITY
      if (!GlobalConfiguration.OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS.isChanged()) {
        GlobalConfiguration.OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS.setValue(false);
        LogManager.instance().log(this, Level.INFO,
            """
            Production mode: LOAD CSV file access automatically disabled for security. \
            Set arcadedb.opencypher.loadCsv.allowFileUrls=true explicitly to enable""");
      }
    }

    // START METRICS & CONNECTED JMX REPORTER
    if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS))
      startMetrics();

    // Register the engine-boundary query tracer regardless of the metrics flag: it opens a span only
    // when the optional tracing plugin has attached a tracer to the ObservationRegistry, and is a
    // zero-cost no-op otherwise. This gives every wire protocol query/command spans from one point.
    QueryTracer.Holder.register(new MicrometerQueryTracer(observationRegistry));

    security = new ServerSecurity(this, configuration, serverRootPath + "/config");
    security.startService();

    createDirectories();

    loadDatabases();

    security.loadUsers();

    // INITIALIZE AI CONFIGURATION (always available, inactive until subscription token is set)
    aiConfiguration = new AiConfiguration(serverRootPath);
    aiConfiguration.load();

    // START HTTP SERVER IMMEDIATELY. THE HTTP ADDRESS WILL BE USED BY HA
    httpServer = new HttpServer(this);

    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON);

    httpServer.startService();

    // HA is started via the plugin manager (RaftHAPlugin discovered via ServiceLoader)

    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.AFTER_HTTP_ON);

    // HA WAS REQUESTED (EXPLICITLY VIA ha.enabled=true OR IMPLICITLY VIA A NON-BLANK ha.serverList) BUT NO
    // HAServerPlugin REGISTERED ITSELF. THE MOST COMMON CAUSE FOR EMBEDDED DEPLOYMENTS IS A MISSING
    // arcadedb-ha-raft DEPENDENCY: SINCE THE APACHE RATIS MIGRATION THE HA IMPLEMENTATION LIVES IN A
    // SEPARATE MODULE AND IS DISCOVERED VIA ServiceLoader, SO EMBEDDING arcadedb-server ALONE NO LONGER
    // ENABLES HA. WARN LOUDLY INSTEAD OF SILENTLY RUNNING STANDALONE.
    if ((configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) || configuration.isHAImplicitlyEnabled())
        && haServer == null) {
      final String haWarning = """
          High availability was requested but no HA plugin was found on the classpath. \
          The node will run STANDALONE (no replication). Since the Apache Ratis migration the HA implementation \
          is in the separate 'arcadedb-ha-raft' module: add the 'arcadedb-ha-raft' dependency to your \
          (embedded) application, or use the official server distribution/Docker image which bundles it.""";
      LogManager.instance().log(this, Level.WARNING, haWarning);
      getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "HA", null, haWarning);
    }

    loadDefaultDatabases();

    // RELOAD DATABASE IF A PLUGIN REGISTERED A NEW DATABASE (LIKE THE GREMLIN SERVER)
    loadDatabases();

    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.AFTER_DATABASES_OPEN);

    status = STATUS.ONLINE;

    LogManager.instance().log(this, Level.INFO, "Available query languages: %s",
        QueryEngineManager.getInstance().getAvailableLanguages());

    final String mode = GlobalConfiguration.SERVER_MODE.getValueAsString();

    final String msg = "ArcadeDB Server started in '%s' mode (CPUs=%d MAXRAM=%s)".formatted(mode,
        Runtime.getRuntime().availableProcessors(), FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()));
    LogManager.instance().log(this, Level.INFO, msg);

    getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Server", null, msg);

    LogManager.instance().log(this, Level.INFO, Constants.SPONSOR_MSG);

    if ("production".equals(mode))
      logProductionChecklist();

    if (!"production".equals(mode) || GlobalConfiguration.STUDIO_ENABLED.getValueAsBoolean()) {
      final InputStream file = getClass().getClassLoader().getResourceAsStream("static/index.html");
      if (file != null) {
        final String studioHost = getStudioDisplayHost();
        LogManager.instance()
            .log(this, Level.INFO, "Studio web tool available at http://%s:%d ", studioHost, httpServer.getPort());
      }
    }

    try {
      lifecycleEvent(ReplicationCallback.TYPE.SERVER_UP, null);
    } catch (final Exception e) {
      stop();
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }
  }

  private void logProductionChecklist() {
    LogManager.instance().log(this, Level.INFO, "Production checklist:");

    // WAL FLUSH
    final int walFlush = GlobalConfiguration.TX_WAL_FLUSH.getValueAsInteger();
    if (walFlush > 0)
      LogManager.instance().log(this, Level.INFO, "  - WAL flush: %d (%s) [OK]", walFlush,
          walFlush == 1 ? "flush without metadata" : "full fsync");
    else
      LogManager.instance().log(this, Level.WARNING, "  - WAL flush: disabled [WARNING]");

    // SSL
    final boolean ssl = configuration.getValueAsBoolean(GlobalConfiguration.NETWORK_USE_SSL);
    if (ssl)
      LogManager.instance().log(this, Level.INFO, "  - SSL: enabled [OK]");
    else
      LogManager.instance().log(this, Level.INFO, "  - SSL: disabled. Consider enabling for encrypted connections");

    // HA
    final boolean ha = configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED);
    if (ha)
      LogManager.instance().log(this, Level.INFO, "  - High availability: enabled [OK]");
    else
      LogManager.instance().log(this, Level.INFO, "  - High availability: disabled. Consider enabling for fault tolerance");

    // LOAD CSV FILE ACCESS
    final boolean loadCsvFiles = GlobalConfiguration.OPENCYPHER_LOAD_CSV_ALLOW_FILE_URLS.getValueAsBoolean();
    if (!loadCsvFiles)
      LogManager.instance().log(this, Level.INFO, "  - LOAD CSV file access: disabled [OK]");
    else
      LogManager.instance().log(this, Level.WARNING,
          "  - LOAD CSV file access: enabled [WARNING]. Consider disabling for multi-tenant security");

    // BACKUP
    final boolean backup = configuration.getValueAsBoolean(GlobalConfiguration.BACKUP_ENABLED);
    if (backup)
      LogManager.instance().log(this, Level.INFO, "  - Backup support: enabled [OK]");
    else
      LogManager.instance().log(this, Level.WARNING,
          "  - Backup support: disabled [WARNING]. Set arcadedb.backup.enabled=true for production safety");

    // DEFAULT ROOT PASSWORD
    final String rootPassword = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    if (rootPassword != null && !rootPassword.isEmpty())
      LogManager.instance().log(this, Level.WARNING,
          "  - Root password: set via configuration [WARNING]. Consider using server-users.json instead");

    // STUDIO
    if (GlobalConfiguration.STUDIO_ENABLED.getValueAsBoolean())
      LogManager.instance().log(this, Level.WARNING,
          "  - Studio web tool: force-enabled (arcadedb.studio.enabled=true) [WARNING]. Restrict network access to it");
    else
      LogManager.instance().log(this, Level.INFO, "  - Studio web tool: disabled [OK]");
  }

  private void createDirectories() {

    LogManager.instance().log(this, Level.INFO, "Paths - server root: %s - databases: %s - backups: %s",
        configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH),
        configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY),
        configuration.getValueAsString(GlobalConfiguration.SERVER_BACKUP_DIRECTORY));

    final File databaseDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    if (!databaseDir.exists()) {
      if (!databaseDir.mkdirs()) {
        LogManager.instance().log(this, Level.SEVERE, "Failed to create databases directory: %s",
            databaseDir.getAbsolutePath());
        throw new ServerException("Unable to create databases directory: " + databaseDir.getAbsolutePath());
      }
    }

    final File backupsDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_BACKUP_DIRECTORY));
    if (!backupsDir.exists()) {
      if (!backupsDir.mkdirs()) {
        LogManager.instance().log(this, Level.SEVERE, "Failed to create backups directory: %s",
            backupsDir.getAbsolutePath());
        throw new ServerException("Unable to create backups directory: " + backupsDir.getAbsolutePath());
      }
    }

  }

  private void welcomeBanner() {
    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server v" + Constants.getVersion() + " is starting up...");

    final String osName = System.getProperty("os.name");
    final String osVersion = System.getProperty("os.version");
    final String vmName = System.getProperty("java.vm.name");
    final String vmVendorVersion = System.getProperty("java.vendor.version");
    final String vmVersion = System.getProperty("java.version");
    LogManager.instance().log(this, Level.INFO,
        "Running on " + osName + " " + osVersion + " - " + (vmName != null ? vmName : "Java") + " " + vmVersion + " " + (
            vmVendorVersion != null ?
                "(" + vmVendorVersion + ")" :
                ""));
  }

  private Set<String> getPluginNames() {
    final Set<String> result = new LinkedHashSet<>();
    final String registeredPlugins = configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    if (registeredPlugins != null && !registeredPlugins.isEmpty()) {
      final String[] pluginEntries = registeredPlugins.split(",");
      for (final String p : pluginEntries) {
        final String[] pluginPair = p.split(":");
        final String pluginName = pluginPair[0];
        result.add(pluginName);
      }
    }
    return result;
  }

  private Set<String> getAllPluginNames() {
    final Set<String> result = new LinkedHashSet<>();
    // Add legacy plugins
    result.addAll(getPluginNames());
    // Add PluginManager plugins
    if (pluginManager != null)
      result.addAll(pluginManager.getPluginNames());
    return result;
  }

  public void stop() {
    lifecycleLock.lock();
    try {
      recordLifecycleOwner();
      stopInternal();
    } finally {
      clearLifecycleOwner();
      lifecycleLock.unlock();
    }
  }

  /**
   * Stops the server from the JVM shutdown hook, and NEVER blocks the JVM from exiting (issue #5418).
   * <p>
   * A shutdown hook that waits unconditionally for the lifecycle mutex can hang the process for good.
   * The observed path: {@link #start()} holds the mutex while Apache Ratis tries to bind its gRPC port;
   * if the port is taken, Ratis reports the failure by calling {@code System.exit()} from that very
   * thread. {@code System.exit()} runs the shutdown hooks and waits for them, so the starting thread is
   * now parked inside {@code Shutdown.exit()} still holding the mutex, while this hook waits for a mutex
   * that can never be released - a deadlock in which the JVM cannot exit and only SIGKILL ends it. Any
   * startup port conflict was enough to trigger it.
   * <p>
   * So the wait is bounded, and the bound depends on what the mutex holder is likely doing - see
   * {@link ShutdownHookBound} and {@link #chooseShutdownHookBound} (issue #7025):
   * <ul>
   *   <li>the holder is parked inside {@code System.exit()}: it is waiting for this hook and can never
   *       release the mutex, so there is nothing to wait for at all;</li>
   *   <li>{@code STARTING} with no database open yet: nothing worth flushing, so wait only briefly, for
   *       the benign race where a concurrent start is about to finish;</li>
   *   <li>anything else - databases are open, even though the status may still read {@code STARTING}
   *       because {@code loadDatabases()} runs long before the HTTP service, the plugins and the HA
   *       bring-up - a legitimate concurrent {@code stop()} may be flushing them, and so may the tail of a
   *       {@code start()} that has not released the mutex yet. Cutting that short would cost a WAL
   *       recovery on every open database at the next start (the #7025 report: a liveness probe killing
   *       a pod mid-start left three databases to replay their WAL on every restart, each one slower than
   *       the last). Wait up to {@code arcadedb.server.shutdownTimeout}.</li>
   * </ul>
   * If the mutex never arrives, the databases are left as they are: the next open replays the WAL,
   * exactly as after a kill. That is strictly better than a process that cannot be stopped.
   */
  // @VisibleForTesting
  void stopFromShutdownHook() {
    // Snapshot the status once: start() can flip it STARTING -> ONLINE while still holding the
    // lifecycle lock, so re-reading it after the bounded wait could describe a bound that was not
    // the one actually applied. The warning must be built from the same snapshot that chose the bound.
    final STATUS observedStatus = status;
    final Thread owner = lifecycleOwner;
    final boolean holderExiting =
        owner != null && !owner.equals(Thread.currentThread()) && isRunningShutdownHooks(owner.getStackTrace());
    final ShutdownHookBound bound = chooseShutdownHookBound(observedStatus, !databases.isEmpty(), holderExiting);
    final long timeout = shutdownHookTimeoutMs(bound);

    if (!awaitLifecycleLock(lifecycleLock, timeout)) {
      LogManager.instance().log(this, Level.WARNING, shutdownHookLockTimeoutWarning(timeout, observedStatus, bound));
      return;
    }

    try {
      stopInternal();
    } finally {
      lifecycleLock.unlock();
    }
  }

  /**
   * Picks the bound the shutdown hook waits under (issue #7025). The question that decides it is not "is the
   * status {@code STARTING}?" but "is there an open database to flush?": databases open at the start of
   * {@code startInternal()}, long before the status turns {@code ONLINE}, so a status-only rule applied the short
   * fixed bound to a server that had everything worth flushing already open. Static and side-effect free so every
   * cell of the decision can be pinned without driving a real JVM shutdown.
   *
   * @param status        the server status observed when the hook started
   * @param databasesOpen whether at least one database is registered
   * @param holderExiting whether the lifecycle lock holder is parked in {@code System.exit()} - see
   *                      {@link #isRunningShutdownHooks}
   */
  // @VisibleForTesting
  static ShutdownHookBound chooseShutdownHookBound(final STATUS status, final boolean databasesOpen,
      final boolean holderExiting) {
    if (holderExiting)
      return ShutdownHookBound.HOLDER_EXITING;
    if (status == STATUS.STARTING && !databasesOpen)
      return ShutdownHookBound.STARTING_NO_DATABASES;
    return ShutdownHookBound.CONFIGURED;
  }

  private long shutdownHookTimeoutMs(final ShutdownHookBound bound) {
    return switch (bound) {
      case HOLDER_EXITING -> 0;
      case STARTING_NO_DATABASES -> SHUTDOWN_HOOK_STARTING_TIMEOUT_MS;
      case CONFIGURED -> configuration.getValueAsLong(GlobalConfiguration.SERVER_SHUTDOWN_TIMEOUT);
    };
  }

  /**
   * Whether the given stack belongs to a thread that is running the JVM shutdown hooks, i.e. one parked inside
   * {@code System.exit()} / {@code Runtime.exit()} / {@code Shutdown.exit()} waiting for the hooks - this one
   * included - to finish. Such a thread holding the lifecycle lock is the #5418 deadlock shape, and it will never
   * release the lock however long the hook waits.
   */
  // @VisibleForTesting
  static boolean isRunningShutdownHooks(final StackTraceElement[] stack) {
    for (final StackTraceElement frame : stack) {
      final String className = frame.getClassName();
      if ("java.lang.Shutdown".equals(className))
        return true;
      if ("exit".equals(frame.getMethodName()) && ("java.lang.Runtime".equals(className) || "java.lang.System".equals(
          className)))
        return true;
    }
    return false;
  }

  /**
   * Builds the WARNING for a shutdown hook that could not acquire the lifecycle lock. The advice depends
   * on which bound was in effect: under {@link ShutdownHookBound#STARTING_NO_DATABASES} the wait is the fixed
   * {@link #SHUTDOWN_HOOK_STARTING_TIMEOUT_MS}, so pointing the operator at
   * {@code arcadedb.server.shutdownTimeout} would name the one setting that provably had no effect on
   * that branch (issue #6981); under {@link ShutdownHookBound#HOLDER_EXITING} no wait could have helped at all.
   * Extracted so the message can be tested without driving a real JVM shutdown.
   */
  // @VisibleForTesting
  static String shutdownHookLockTimeoutWarning(final long timeout, final STATUS status, final ShutdownHookBound bound) {
    final String hint = switch (bound) {
      case HOLDER_EXITING -> "The thread holding the lock is itself inside System.exit() waiting for this hook, so no "
          + "wait could have acquired it: the hook did not wait.";
      case STARTING_NO_DATABASES -> "This wait during STARTING with no database open yet is a fixed "
          + SHUTDOWN_HOOK_STARTING_TIMEOUT_MS + "ms bound not governed by arcadedb.server.shutdownTimeout.";
      case CONFIGURED -> "Raise arcadedb.server.shutdownTimeout if a legitimate shutdown needs longer.";
    };
    return """
        Could not acquire the server lifecycle lock within %dms while status is %s, so the shutdown hook is \
        returning WITHOUT a graceful stop and the JVM will exit. Another thread is inside start()/stop() and \
        did not release it - typically a startup failure that called System.exit() from inside start() (e.g. a \
        port already in use). Open databases were not closed cleanly; the next open replays the WAL. %s""".formatted(
        timeout, status, hint);
  }

  /**
   * Bounded acquisition of the lifecycle lock, preserving the interrupt flag. Extracted so the
   * shutdown-hook guard can be tested without driving a real JVM shutdown.
   *
   * @return {@code true} when the lock was acquired and the caller must unlock it
   */
  // @VisibleForTesting
  static boolean awaitLifecycleLock(final ReentrantLock lock, final long timeoutMs) {
    try {
      return lock.tryLock(Math.max(0, timeoutMs), TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private void startMetrics() {
    synchronized (METRICS_INSTALL_MUTEX) {
      if (!metricsFiltersInstalled) {
        // Backstop against a path-tag cardinality explosion on arcadedb.http.requests. The handler already
        // collapses unmatched URIs to a constant, but this caps the number of distinct "path" tag values and
        // denies further ones so a future route or regression can never grow the registry without bound.
        // A MeterFilter cannot be removed from a registry, so it is installed once per JVM, and before any
        // meter exists as Micrometer requires MeterFilters to precede the meters they govern.
        Metrics.globalRegistry.config().meterFilter(
            MeterFilter.maximumAllowableTags("arcadedb.http.requests", "path", 100, MeterFilter.deny()));
        // Same backstop for the "db" half of the tuple (issue #6805). The handler already collapses a
        // {database} path parameter naming no existing database onto a constant, but database name churn -
        // or a future route exposing another free segment as "db" - must not be able to grow the registry
        // either. The limit comes from the handler that produces the tag, so the registry-side bound and the
        // handler's own timer-cache bound stay tied to one number.
        Metrics.globalRegistry.config().meterFilter(
            MeterFilter.maximumAllowableTags("arcadedb.http.requests", "db",
                AbstractServerHttpHandler.MAX_DB_TAG_VALUES + AbstractServerHttpHandler.RESERVED_DB_TAG_VALUES,
                MeterFilter.deny()));
        metricsFiltersInstalled = true;
      }

      if (metricsInstalls == 0)
        // Meters already registered belong to somebody else (typically the embedding application) and are
        // left alone by the shutdown. The flip side is the contract: from here until the last server stops,
        // the server owns every meter registered on the global registry, so an embedded application that
        // wants its own meters to outlive the server must register them before starting it, or on a registry
        // of its own added to the composite (what the Prometheus and OTLP plugins do). Taken before the
        // increment, so a failure here leaves nothing to release and no half-installed count behind.
        metersBeforeInstall = currentMeterIds();

      // Paired with the decrement in stopMetrics() through this flag, set in the same critical section as the
      // increment: anything that throws further down must still be able to release the count, or it never
      // returns to zero and the last-one-out teardown is disabled for the life of the JVM.
      metricsInstalled = true;
      metricsInstalls++;
    }

    // The registry and the binders below are deliberately installed outside the mutex: it guards only the
    // shared install state (counter, snapshot, filter). Two servers starting concurrently in one JVM can
    // interleave here safely, because registering an already-registered meter id is a no-op in Micrometer
    // and the counter transitions that decide the snapshot and the teardown are serialised above. Holding
    // the mutex across MXBean binding would serialise unrelated server startups for no benefit.

    metricsRegistry = new SimpleMeterRegistry();
    Metrics.addRegistry(metricsRegistry);

    new ClassLoaderMetrics().bindTo(Metrics.globalRegistry);
    new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
    // Registers notification listeners on the GC MXBeans, so it must be closed on shutdown.
    metricsJvmGc = new JvmGcMetrics();
    metricsJvmGc.bindTo(Metrics.globalRegistry);
    new ProcessorMetrics().bindTo(Metrics.globalRegistry);
    new JvmThreadMetrics().bindTo(Metrics.globalRegistry);
    // Engine executor pools (query parallelism + sparse-vector scoring) - exposes
    // arcadedb.executor.pool.{size,active,...} gauges tagged with pool=<name>.
    new PoolMetrics().bindTo(Metrics.globalRegistry);
    // Engine-wide Profiler stats (cache hit/miss, pages, WAL, MVCC conflicts, open files, tx,
    // queries) - exposes arcadedb.engine.* gauges read live from the Profiler on each scrape.
    new EngineMetricsBinder().bindTo(Metrics.globalRegistry);
    // HA replication health (heartbeat lag + replication lag) sourced live from the HA plugin -
    // exposes arcadedb.ha.* gauges; harmless no-op (all -1/0) when HA is disabled. Kept in a field
    // and closed in stopMetrics(), or the per-follower gauge refresh scheduler it starts leaks one
    // daemon thread per restart (issue #5850).
    haReplicationMetrics = new HAReplicationMetrics(this);
    haReplicationMetrics.bindTo(Metrics.globalRegistry);
    QueryMetricsRecorder.Holder.register(new MicrometerQueryMetricsRecorder());

    if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS_LOGGING)) {
      LogManager.instance().log(this, Level.INFO, "- Logging metrics enabled...");
      metricsLoggingRegistry = new LoggingMeterRegistry();
      Metrics.addRegistry(metricsLoggingRegistry);
    }
    LogManager.instance().log(this, Level.INFO, "Metrics Collection Started");
  }

  private void stopMetrics() {
    if (!metricsInstalled)
      // Metrics were disabled for this server, or already dismantled: nothing of ours to release.
      return;
    metricsInstalled = false;

    LogManager.instance().log(this, Level.INFO, "- Stop metrics collection");

    if (metricsJvmGc != null) {
      CodeUtils.executeIgnoringExceptions(metricsJvmGc::close, "Error on closing the JVM GC metrics", false);
      metricsJvmGc = null;
    }
    if (haReplicationMetrics != null) {
      CodeUtils.executeIgnoringExceptions(haReplicationMetrics::close, "Error on closing the HA replication metrics", false);
      haReplicationMetrics = null;
    }
    if (metricsLoggingRegistry != null) {
      removeMeterRegistry(metricsLoggingRegistry);
      metricsLoggingRegistry = null;
    }
    // Null when the install threw before getting this far: the count below is released either way.
    if (metricsRegistry != null) {
      removeMeterRegistry(metricsRegistry);
      metricsRegistry = null;
    }

    synchronized (METRICS_INSTALL_MUTEX) {
      if (--metricsInstalls > 0)
        // A sibling server in this JVM still publishes to the shared registry: leave its meters in place.
        return;

      // Last one out: the engine must stop timing queries, the cached timers must not outlive the
      // registries that backed them, and no meter may survive to be read from a later generation that
      // never recorded into it.
      QueryMetricsRecorder.Holder.register(QueryMetricsRecorder.NO_OP);
      MicrometerQueryMetricsRecorder.invalidateTimerCache();
      AbstractServerHttpHandler.invalidateTimerCache();

      for (final Meter meter : Metrics.globalRegistry.getMeters())
        if (!metersBeforeInstall.contains(meter.getId()))
          Metrics.globalRegistry.remove(meter);

      metersBeforeInstall = null;
    }
  }

  private static void removeMeterRegistry(final MeterRegistry registry) {
    Metrics.removeRegistry(registry);
    CodeUtils.executeIgnoringExceptions(registry::close, "Error on closing the metrics registry", false);
  }

  private static Set<Meter.Id> currentMeterIds() {
    final Set<Meter.Id> ids = new HashSet<>();
    Metrics.globalRegistry.forEachMeter(meter -> ids.add(meter.getId()));
    return ids;
  }

  private void stopInternal() {
    if (status == STATUS.OFFLINE || status == STATUS.SHUTTING_DOWN)
      return;

    LogManager.instance().log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    try {
      lifecycleEvent(ReplicationCallback.TYPE.SERVER_SHUTTING_DOWN, null);
    } catch (final Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    status = STATUS.SHUTTING_DOWN;

    // Stop plugins managed by PluginManager first
    if (pluginManager != null)
      pluginManager.stopPlugins();

    if (haServer != null)
      CodeUtils.executeIgnoringExceptions(haServer::stopService, "Error on stopping HA service", false);

    if (httpServer != null)
      CodeUtils.executeIgnoringExceptions(httpServer::stopService, "Error on stopping HTTP service", false);

    if (security != null)
      CodeUtils.executeIgnoringExceptions(security::stopService, "Error on stopping Security service", false);

    for (final ServerDatabase db : databases.values())
      CodeUtils.executeIgnoringExceptions(db.getEmbedded()::close, "Error closing database '" + db.getName() + "'",
          false);
    databases.clear();

    // Deliberately last: the timer caches are cleared and the meters removed without holding off recording
    // threads, so this must run once the plugins, the HTTP service and the databases are down and nothing is
    // left to repopulate them. Keep it after those shutdowns.
    CodeUtils.executeIgnoringExceptions(this::stopMetrics, "Error on stopping the metrics collection", false);

    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server is down");

    try {
      lifecycleEvent(ReplicationCallback.TYPE.SERVER_DOWN, null);
    } catch (final Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().setContext(null);
    status = STATUS.OFFLINE;

    getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Server", null, "Server shutdown correctly");

    ServerLogManager.resetFinally();
  }

  public Collection<ServerPlugin> getPlugins() {
    return Collections.unmodifiableCollection(pluginManager.getPlugins());
  }

  public ServerDatabase getDatabase(final String databaseName) {
    return getDatabase(databaseName, false, true);
  }

  public ServerDatabase getOrCreateDatabase(final String databaseName) {
    return getDatabase(databaseName, true, true);
  }

  public FileServerEventLog getEventLog() {
    return eventLog;
  }

  public boolean isStarted() {
    return status == STATUS.ONLINE;
  }

  public STATUS getStatus() {
    return status;
  }

  public boolean existsDatabase(final String databaseName) {
    return databases.containsKey(databaseName);
  }

  /**
   * Returns the monitor that serialises every check-then-act on the database registry (load, create, register,
   * reopen). Callers that must make a multi-step registry mutation atomic with respect to concurrent
   * {@link #getDatabase} / {@link #createDatabase} calls synchronize on this object. The HA snapshot installer
   * holds it across its close-&gt;file-swap-&gt;reopen sequence so no concurrent open observes the transient
   * half-swapped on-disk directory (issue #4832).
   */
  public Object getDatabasesLock() {
    return databasesLock;
  }

  public ServerDatabase createDatabase(final String databaseName, final ComponentFile.MODE mode) {
    checkDatabaseNameIsValid(databaseName);

    ServerDatabase serverDatabase;
    synchronized (databasesLock) {
      serverDatabase = databases.get(databaseName);
      if (serverDatabase != null)
        throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

      final DatabaseFactory factory = new DatabaseFactory(
          configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator
              + databaseName).setAutoTransaction(true);

      factory.setSecurity(getSecurity());

      if (factory.exists())
        throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

      DatabaseInternal embeddedDatabase = (DatabaseInternal) factory.create();

      if (mode == READ_ONLY) {
        embeddedDatabase.close();
        embeddedDatabase = (DatabaseInternal) factory.open(mode);
      }

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) && databaseWrapper != null)
        embeddedDatabase = databaseWrapper.apply((LocalDatabase) embeddedDatabase);

      serverDatabase = new ServerDatabase(this, embeddedDatabase);

      // FORCE LOADING INTO THE SERVER
      databases.put(databaseName, serverDatabase);
    }

    notifyPlugins(databaseName, true);
    return serverDatabase;
  }

  public Set<String> getDatabaseNames() {
    return Collections.unmodifiableSet(databases.keySet());
  }

  /**
   * Returns {@code true} if the given name belongs to a reserved internal database (e.g. the Raft
   * control directory {@code .raft}) that must not be exposed as a user database.
   */
  public static boolean isReservedDatabaseName(final String databaseName) {
    return databaseName != null && databaseName.startsWith(RESERVED_DATABASE_PREFIX);
  }

  /**
   * Validates a caller-supplied database name before it is used to build an on-disk path, preventing path
   * traversal outside the configured {@link GlobalConfiguration#SERVER_DATABASE_DIRECTORY} (GHSA-qwgr-2c45-63xx).
   * A database name maps directly to a directory name under the database directory, so it must never contain
   * filesystem path separators, parent-directory references or null bytes, and the resolved path must remain
   * inside the configured base directory.
   *
   * @throws IllegalArgumentException if the name is empty or would escape the database directory
   */
  public void checkDatabaseNameIsValid(final String databaseName) {
    if (databaseName == null || databaseName.trim().isEmpty())
      throw new IllegalArgumentException("Database name is empty");

    if (databaseName.indexOf('/') > -1 || databaseName.indexOf('\\') > -1 || databaseName.indexOf('\0') > -1
        || databaseName.equals(".") || databaseName.equals("..") || databaseName.contains(".."))
      throw new IllegalArgumentException("Database name '" + databaseName + "' contains invalid characters");

    // Defence in depth: resolve the final path and verify it does not escape the configured base directory.
    final Path base = Paths.get(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY))
        .toAbsolutePath().normalize();
    final Path resolved = base.resolve(databaseName).normalize();
    if (!resolved.startsWith(base))
      throw new IllegalArgumentException("Database name '" + databaseName + "' resolves outside the database directory");
  }

  public ServerDatabase registerDatabase(final String databaseName, final DatabaseInternal database) {
    // Serialise with getDatabase/createDatabase/rewrapDatabases on databasesLock so a concurrent open-from-disk
    // cannot interleave with this registration and end up with two DatabaseInternal instances over the same directory.
    final ServerDatabase serverDatabase;
    synchronized (databasesLock) {
      serverDatabase = new ServerDatabase(this, database);
      final ServerDatabase existing = databases.putIfAbsent(databaseName, serverDatabase);
      if (existing != null)
        throw new IllegalArgumentException("Database '" + databaseName + "' already registered");
    }

    notifyPlugins(databaseName, true);
    return serverDatabase;
  }

  public void removeDatabase(final String databaseName) {
    final ServerDatabase removed;
    synchronized (databasesLock) {
      removed = databases.remove(databaseName);
    }

    if (removed != null)
      notifyPlugins(databaseName, false);
  }

  /**
   * Notifies every installed plugin that a database entered or left the server registry, so per-database plugin state
   * (a backup schedule, for instance) stays in step with the registry instead of outliving its database (issue #6752).
   * <p>
   * Called after the registry mutation, on the thread that performed it. That thread may still hold
   * {@link #databasesLock} - the HA snapshot installer holds it across its whole close-&gt;swap-&gt;reopen sequence -
   * so an implementation has to be short and non-blocking.
   * <p>
   * Dispatching here rather than inside the lock keeps plugin code off the monitor that every database open
   * contends on, at the cost of ordering: two threads mutating the same name concurrently can deliver their
   * callbacks in either order. Plugins are therefore told to reconcile against the registry as it is now instead of
   * replaying the event, which is what {@code AutoBackupSchedulerPlugin} does. A plugin that throws is logged and
   * skipped: the mutation has already happened and cannot be undone by a failing listener.
   * <p>
   * Only the plugins that have been configured are notified. Discovery installs every plugin instance up front, but
   * {@link #loadDatabases()} runs before {@code startPlugins(AFTER_DATABASES_OPEN)}, so a plugin of that priority was
   * being handed a registration per pre-existing database before it had even been given this server (issue #6852).
   */
  private void notifyPlugins(final String databaseName, final boolean registered) {
    if (pluginManager == null)
      return;

    for (final ServerPlugin plugin : pluginManager.getInitializedPlugins()) {
      try {
        if (registered)
          plugin.onDatabaseRegistered(databaseName);
        else
          plugin.onDatabaseUnregistered(databaseName);
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Error on notifying plugin '%s' that database '%s' was %s", e, plugin.getName(), databaseName,
            registered ? "registered" : "unregistered");
      }
    }
  }

  public String getServerName() {
    return serverName;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public HAServerPlugin getHA() {
    return haServer;
  }

  public void setHA(final HAServerPlugin ha) {
    this.haServer = ha;
  }

  /**
   * Sets a custom database wrapper function used by HA plugins (e.g., Raft) to wrap
   * LocalDatabase instances with a replicated database implementation.
   */
  public void setDatabaseWrapper(final Function<LocalDatabase, DatabaseInternal> wrapper) {
    this.databaseWrapper = wrapper;
  }

  /**
   * Re-wraps all already-loaded databases using the current databaseWrapper.
   * Called by HA plugins (e.g., Raft) that start after databases are already loaded,
   * to replace the plain LocalDatabase with a replicated database implementation.
   */
  public void rewrapDatabases() {
    if (databaseWrapper == null)
      return;

    synchronized (databasesLock) {
      for (final var entry : databases.entrySet()) {
        final ServerDatabase serverDb = entry.getValue();
        final DatabaseInternal wrapped = serverDb.getWrappedDatabaseInstance();

        // Get the underlying LocalDatabase, whether it's plain or already wrapped by an HA layer.
        // On server restart the old wrapper may hold a stale/null raftHAServer reference, so we
        // must always unwrap to the LocalDatabase and re-wrap with the current wrapper.
        final LocalDatabase localDb;
        if (wrapped instanceof LocalDatabase ld)
          localDb = ld;
        else if (wrapped.getEmbedded() instanceof LocalDatabase ld)
          localDb = ld;
        else
          localDb = null;

        if (localDb == null)
          continue;

        final DatabaseInternal newWrapped = databaseWrapper.apply(localDb);
        final ServerDatabase newServerDb = new ServerDatabase(this, newWrapped);
        databases.put(entry.getKey(), newServerDb);
        LogManager.instance().log(this, Level.INFO, "Re-wrapped database '%s' with HA wrapper", entry.getKey());
      }
    }
  }

  public ServerSecurity getSecurity() {
    return security;
  }

  public AiConfiguration getAiConfiguration() {
    return aiConfiguration;
  }

  public ServerQueryProfiler getQueryProfiler() {
    return queryProfiler;
  }

  /**
   * Admits one backup at a time per database, across every backup entry point this server has, and names the archives
   * they write (issue #6753).
   */
  public BackupCoordinator getBackupCoordinator() {
    return backupCoordinator;
  }

  public void registerTestEventListener(final ReplicationCallback callback) {
    testEventListeners.add(callback);
  }

  public void lifecycleEvent(final ReplicationCallback.TYPE type, final Object object) throws Exception {
    if (replicationLifecycleEventsEnabled)
      for (final ReplicationCallback c : testEventListeners)
        c.onEvent(type, object, this);
  }

  public String getRootPath() {
    return serverRootPath;
  }

  public HttpServer getHttpServer() {
    return httpServer;
  }

  @Override
  public String toString() {
    return getServerName();
  }

  public ServerDatabase getDatabase(final String databaseName, final boolean createIfNotExists,
      final boolean allowLoad) {
    if (databaseName == null || databaseName.trim().isEmpty())
      throw new IllegalArgumentException("Invalid database name " + databaseName);

    // Lock-free fast path: an already-open database is served straight off the ConcurrentMap without entering
    // databasesLock. This keeps the request hot path off the JVM-wide monitor that createDatabase/open and the HA
    // snapshot installer hold across long I/O. Only the miss (absent or closed) falls through to the locked slow
    // path, which re-checks under the lock, so createIfNotExists/allowLoad semantics are preserved.
    //
    // The fast path is gated on STATUS.ONLINE. During startup (STATUS.STARTING) the HTTP server is already
    // accepting requests while the HA plugin still has to run rewrapDatabases() under databasesLock to swap the
    // plain LocalDatabase entries for HA-wrapped ones. Bypassing the lock in that window could hand a caller the
    // pre-wrap (non-replicated) instance mid-swap. status flips to ONLINE only after that re-wrapping completes,
    // so restricting the fast path to ONLINE makes concurrent startup lookups fall through to the locked slow path
    // and block until the wrapped instances are published - exactly the pre-fast-path behaviour.
    //
    // The other lock holder is the runtime HA snapshot installer (close->swap->reopen while ONLINE). The HTTP request
    // path is normally deflected with 503 during an install (snapshotInstallInProgress), though that is a check at
    // request entry rather than a hard guarantee for the whole request or for non-HTTP callers. Once the installer
    // closes and removeDatabase-s the entry, databases.get() returns null/closed here and the lookup falls through to
    // the locked slow path - preserving the #4832 guarantee of never reopening a half-swapped directory.
    ServerDatabase db = databases.get(databaseName);
    if (status == STATUS.ONLINE && db != null && db.isOpen())
      return db;

    boolean loaded = false;
    synchronized (databasesLock) {
      db = databases.get(databaseName);

      if (db == null || !db.isOpen()) {
        if (!allowLoad)
          throw new DatabaseNotAvailableException("Database '" + databaseName + "' is not available");

        checkDatabaseNameIsValid(databaseName);

        final String path =
            configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator + databaseName;

        final DatabaseFactory factory = new DatabaseFactory(path).setAutoTransaction(true);

        factory.setSecurity(getSecurity());

        ComponentFile.MODE defaultDbMode =
            configuration.getValueAsEnum(GlobalConfiguration.SERVER_DEFAULT_DATABASE_MODE,
                ComponentFile.MODE.class);
        if (defaultDbMode == null)
          defaultDbMode = READ_WRITE;

        DatabaseInternal embDatabase;
        if (createIfNotExists)
          embDatabase = (DatabaseInternal) (factory.exists() ? factory.open(defaultDbMode) : factory.create());
        else {
          final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();
          if (!activeDatabases.isEmpty()) {
            embDatabase = null;
            for (Database existentDatabase : activeDatabases) {
              if (existentDatabase.getDatabasePath().equals(path)) {
                // REUSE THE OPEN DATABASE. THIS TYPICALLY HAPPENS WHEN A SERVER PLUGIN OPENS THE DATABASE AT STARTUP
                embDatabase = (DatabaseInternal) existentDatabase;
                break;
              }
            }

            if (embDatabase == null)
              // OPEN A NEW DATABASE. THIS IS MOSTLY FOR TESTS WHERE MULTIPLE SERVERS SHARE THE SAME JVM
              embDatabase = (DatabaseInternal) factory.open(defaultDbMode);

          } else
            embDatabase = (DatabaseInternal) factory.open(defaultDbMode);
        }

        if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED) && databaseWrapper != null)
          embDatabase = databaseWrapper.apply((LocalDatabase) embDatabase);

        db = new ServerDatabase(this, embDatabase);

        databases.put(databaseName, db);
        loaded = true;
      }
    }

    if (loaded)
      notifyPlugins(databaseName, true);

    return db;
  }

  private void loadDatabases() {
    final File databaseDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    if (!databaseDir.exists()) {
      databaseDir.mkdirs();
    } else {
      if (!databaseDir.isDirectory())
        throw new ConfigurationException("Configured database directory '" + databaseDir + "' is not a directory on " +
            "file system");

      if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_DATABASE_LOADATSTARTUP)) {
        final File[] databaseDirectories = databaseDir.listFiles(File::isDirectory);
        for (final File f : databaseDirectories)
          // Skip reserved internal databases (e.g. the Raft control directory '.raft'): they are not
          // user databases and must not be registered nor leak into the server/cluster status APIs.
          if (!isReservedDatabaseName(f.getName()))
            getDatabase(f.getName());
      }
    }
  }

  private void loadDefaultDatabases() {
    final String defaultDatabases = configuration.getValueAsString(GlobalConfiguration.SERVER_DEFAULT_DATABASES);
    if (defaultDatabases != null && !defaultDatabases.isEmpty()) {
      ComponentFile.MODE defaultDbMode = configuration.getValueAsEnum(GlobalConfiguration.SERVER_DEFAULT_DATABASE_MODE,
          ComponentFile.MODE.class);
      if (defaultDbMode == null)
        defaultDbMode = READ_WRITE;

      // CREATE DEFAULT DATABASES
      final String[] dbs = defaultDatabases.split(";");
      for (final String db : dbs) {
        final int credentialBegin = db.indexOf('[');
        if (credentialBegin < 0) {
          LogManager.instance().log(this, Level.WARNING, "Error in default databases format: '%s'", defaultDatabases);
          break;
        }

        final String dbName = db.substring(0, credentialBegin);
        final int credentialEnd = db.indexOf(']', credentialBegin);
        final String credentials = db.substring(credentialBegin + 1, credentialEnd);

        if (!credentials.isEmpty())
          parseCredentials(dbName, credentials);

        Database database = existsDatabase(dbName) ? getDatabase(dbName) : null;

        if (credentialEnd < db.length() - 1 && db.charAt(credentialEnd + 1) == '{') {
          // PARSE IMPORTS
          final int commandsEnd = db.indexOf('}', credentialEnd + 2);
          if (commandsEnd < 0) {
            LogManager.instance().log(this, Level.WARNING, "Missing closing '}' in default databases format: '%s'",
                db);
            break;
          }
          final String commands = db.substring(credentialEnd + 2, commandsEnd);

          final String[] commandParts = commands.split(",");
          for (final String command : commandParts) {
            final int commandSeparator = command.indexOf(":");
            if (commandSeparator < 0) {
              LogManager.instance().log(this, Level.WARNING, "Error in startup command configuration format: '%s'",
                  commands);
              break;
            }
            final String commandType = command.substring(0, commandSeparator).toLowerCase(Locale.ENGLISH);
            final String commandParams = command.substring(commandSeparator + 1);

            switch (commandType) {
            case "restore":
              // DROP THE DATABASE BECAUSE THE RESTORE OPERATION WILL TAKE CARE OF CREATING A NEW DATABASE
              if (database != null) {
                ((DatabaseInternal) database).getEmbedded().drop();
                // Route through removeDatabase so this registry mutation also runs under databasesLock, keeping the
                // defect-2 invariant consistent (this path is startup-single-threaded, but consistency is cheaper
                // to keep than to reason about the exception).
                removeDatabase(dbName);
              }
              final String dbPath =
                  configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator + dbName;
//              new Restore(commandParams, dbPath).restoreDatabase();

              try {
                final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
                final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(commandParams,
                    dbPath);

                clazz.getMethod("restoreDatabase").invoke(restorer);

              } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException |
                             InstantiationException e) {
                throw new CommandExecutionException("""
                    Error on restoring database, restore libs not found in \
                    classpath""", e);
              } catch (final InvocationTargetException e) {
                throw new CommandExecutionException("Error on restoring database", e.getTargetException());
              }

              getDatabase(dbName);
              break;

            case "import":
              if (database == null) {
                // CREATE THE DATABASE
                LogManager.instance().log(this, Level.INFO, "Creating default database '%s'...", null, dbName);
                database = createDatabase(dbName, defaultDbMode);
              }
              try (final var rs = database.command("sql", "import database " + commandParams)) {
                // drain not needed: the import command produces no rows we consume here.
                // try-with-resources ensures the result set / execution plan is released.
              }
              break;

            default:
              LogManager.instance().log(this, Level.SEVERE, "Unsupported command %s in startup command: '%s'", null
                  , commandType);
            }
          }
        } else {
          if (database == null) {
            // CREATE THE DATABASE
            LogManager.instance().log(this, Level.INFO, "Creating default database '%s'...", null, dbName);
            createDatabase(dbName, defaultDbMode);
          }
        }
      }
    }
  }

  private void parseCredentials(final String dbName, final String credentials) {
    final String[] credentialPairs = credentials.split(",");
    for (final String credential : credentialPairs) {

      final String[] credentialParts = credential.split(":");

      if (credentialParts.length < 2) {
        if (!security.existsUser(credential)) {
          LogManager.instance()
              .log(this, Level.WARNING, """
                      Cannot create user '%s' to access database '%s' because the user does not \
                      exist""", null,
                  credential, dbName);
        }
        //FIXME: else if user exists, should we give him access to the dbName?
      } else {
        final String userName = credentialParts[0];
        final String userPassword = credentialParts[1];
        final String userGroup = credentialParts.length > 2 ? credentialParts[2] : null;

        if (security.existsUser(userName)) {
          // EXISTING USER: CHECK CREDENTIALS
          ServerSecurityUser user = security.getUser(userName);
          if (user.canAccessToDatabase(dbName)) {
            try {
              user = security.authenticate(userName, userPassword, dbName);

              // UPDATE DB LIST + GROUP
              user.addDatabase(dbName, new String[] { userGroup });
              security.saveUsers();

            } catch (final ServerSecurityException e) {
              LogManager.instance().log(this, Level.WARNING,
                  "Cannot create database '%s' because the user '%s' already exists with a different password", null,
                  dbName,
                  userName);
            }
          } else {
            // UPDATE DB LIST
            user.addDatabase(dbName, new String[] { userGroup });
            security.saveUsers();
          }
        } else {
          // CREATE A NEW USER
          security.createUser(new JSONObject().put("name", userName)//
              .put("password", security.encodePassword(userPassword))//
              .put("databases", new JSONObject().put(dbName, new JSONArray())));

          // UPDATE DB LIST + GROUP
          ServerSecurityUser user = security.getUser(userName);
          user.addDatabase(dbName, new String[] { userGroup });
          security.saveUsers();
        }
      }
    }
  }

  private void loadConfiguration() {
    final File file = new File(getRootPath() + File.separator + CONFIG_SERVER_CONFIGURATION_FILENAME);
    if (file.exists()) {
      try {
        final String content = FileUtils.readFileAsString(file);
        configuration.reset();
        configuration.fromJSON(content);

        // fromJSON writes into this ContextConfiguration only - it never reaches GlobalConfiguration, so the
        // SERVER_LOG_FORMAT set-callback that swaps the console formatter cannot fire from here. The console
        // formatter was already chosen on the first log record the JVM emitted, long before this file was read,
        // so the server applies the value it just loaded itself (issue #7121).
        DefaultLogger.refreshConsoleFormatter(configuration.getValueAsString(GlobalConfiguration.SERVER_LOG_FORMAT));
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading configuration from file '%s'", e, file);
      }
    }
  }

  private void init() {
    eventLog = new FileServerEventLog(this);
    queryProfiler = new ServerQueryProfiler(this);
    String configuredPlugins = configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);

    configuredPlugins = "AutoBackupSchedulerPlugin," + configuredPlugins;

    configuration.setValue(GlobalConfiguration.SERVER_PLUGINS, configuredPlugins);
    configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    pluginManager = new PluginManager(this, configuration);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Mark logger as shutting down to prevent NPE when handlers are closed (issue #2813)
      DefaultLogger.setShuttingDown(true);
      try {
        LogManager.instance().log(this, Level.SEVERE, "Received shutdown signal. The server will be halted");
      } catch (Throwable t) {
        //IGNORE
      }
      try {
        stopFromShutdownHook();
      } catch (final Throwable t) {
        // A shutdown hook must never let anything escape (issue #5418, second deadlock). Apache Ratis
        // installs a global UncaughtExceptionHandler that calls System.exit(); when it fires on THIS
        // thread, that System.exit() blocks forever on the java.lang.Shutdown class monitor already held
        // by the thread that is running the hooks - so a stray exception during shutdown hangs the JVM
        // just as surely as the lock wait this method was written to bound. Swallow and let the exit
        // proceed: the databases are then left as a kill would leave them, and the next open replays
        // the WAL.
        try {
          LogManager.instance().log(this, Level.SEVERE,
              "Error during the shutdown hook; the JVM will exit anyway and open databases will be recovered "
                  + "from the WAL on the next start", t);
        } catch (final Throwable ignored) {
          // the logger may already be closing down
        }
      }
    }, "arcadedb-shutdown-hook"));

    hostAddress = assignHostAddress();
  }

  private String getStudioDisplayHost() {
    final String httpHost = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
    if ("0.0.0.0".equals(httpHost))
      return "localhost";
    return httpHost;
  }

  private String assignHostAddress() {
    String hostAddress;

    // GET THE HOST NAME FROM ENV VARIABLE
    String hostNameEnvVariable = System.getenv("HOSTNAME");
    if (hostNameEnvVariable != null && !hostNameEnvVariable.trim().isEmpty())
      hostNameEnvVariable = hostNameEnvVariable.trim();
    else
      hostNameEnvVariable = null;

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_K8S)) {
      if (hostNameEnvVariable == null) {
        LogManager.instance().log(this, Level.SEVERE,
            """
                Error: HOSTNAME environment variable not found but needed when running inside Kubernetes. The server \
                will be halted""");
        stop();
        System.exit(1);
        return null;
      }

      hostAddress = hostNameEnvVariable + configuration.getValueAsString(GlobalConfiguration.HA_K8S_DNS_SUFFIX);
      LogManager.instance().log(this, Level.INFO, "Server is running inside Kubernetes. Hostname: %s", null,
          hostAddress);

    } else if (hostNameEnvVariable != null) {
      hostAddress = hostNameEnvVariable;
    } else {
      // READ HOST FROM NETWORK INTERFACE
      hostAddress = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
      if ("0.0.0.0".equals(hostAddress)) {
        try {
          hostAddress = ChannelBinary.getLocalIpAddress(true);
        } catch (Exception e) {
          // IGNORE IT
          hostAddress = "localhost";
        }
      }
    }
    return hostAddress;
  }
}
