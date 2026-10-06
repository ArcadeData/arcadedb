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
package com.arcadedb.server.gremlin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import io.netty.channel.Channel;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class GremlinServerPlugin implements ServerPlugin {
  private static final long                 START_TIMEOUT_SECONDS      = 60;
  private static final String               CONFIG_GREMLIN_SERVER_YAML = "gremlin-server.yaml";
  private static final String               IO_REGISTRIES_KEY          = "ioRegistries";
  private static final String               ARCADE_IO_REGISTRY         = ArcadeIoRegistry.class.getName();

  // SERIALIZERS CONFIGURED WHEN NONE ARE DECLARED IN gremlin-server.yaml. THEY MIRROR THE SHIPPED CONFIGURATION SO THAT
  // ARCADEDB TYPES (VERTICES, EDGES AND RAW RIDs - SEE ISSUE #5309) ARE ALWAYS SERIALIZABLE, EVEN WITH DEFAULT SETTINGS.
  private static final String[]             DEFAULT_SERIALIZERS        = {
      "org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1",
      "org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3",
      "org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2" };
  private              ArcadeDBServer       server;
  private              ContextConfiguration configuration;
  private              GremlinServer        gremlinServer;
  private              ExecutorService      gremlinExecutorService;
  private volatile     int                  boundPort;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
  }

  @Override
  public void startService() {
    // Set the server instance for dynamic database registration
    ArcadeGraphManager.setServer(server);

    Settings settings = null;
    // Issue #7415: read from the server configuration directory, which is not necessarily <root>/config.
    final File confFile = new File(server.getConfigPath(), CONFIG_GREMLIN_SERVER_YAML);
    if (confFile.exists()) {
      try (final FileInputStream is = new FileInputStream(confFile.getAbsolutePath())) {
        settings = ArcadeDBGremlinSettings.read(is);
      } catch (final Exception e) {
        LogManager.instance()
            .log(this, Level.INFO, "Error on loading Gremlin Server configuration file '%s'. Using default configuration", confFile);
      }
    } else
      LogManager.instance()
          .log(this, Level.INFO, "Cannot find Gremlin Server configuration file '%s'. Using default configuration", confFile);

    if (settings == null)
      // DEFAULT CONFIGURATION
      settings = new Settings();

    // Use ArcadeDB's custom GraphManager for dynamic database registration
    settings.graphManager = ArcadeGraphManager.class.getName();

    // Disable Gremlin sessions: SessionOpProcessor runs on a per-session executor that the principal
    // binding does not cover, so the engine's per-database/per-type ACLs cannot be enforced for it
    // (GHSA-c287-v325-j5jx). ArcadeDB does not use sessions and the processor is deprecated in TinkerPop.
    if (settings.processors != null)
      settings.processors.removeIf(p -> p.className != null && p.className.toLowerCase(Locale.ENGLISH).contains("session"));

    // OVERWRITE AUTHENTICATION USING THE SERVER SECURITY
    settings.authentication = new Settings.AuthenticationSettings();
    settings.authentication.authenticator = GremlinServerAuthenticator.class.getName();
    settings.authentication.config = new HashMap<>(1);
    settings.authentication.config.put("server", server);

    // ENFORCE PER-DATABASE AUTHORIZATION (canAccessToDatabase). Authentication alone validates the
    // credential; without this gate any valid credential could reach ANY database by naming it as the
    // traversal-source alias (GHSA-c287-v325-j5jx).
    settings.authorization = new Settings.AuthorizationSettings();
    settings.authorization.authorizer = ArcadeGremlinAuthorizer.class.getName();
    settings.authorization.config = new HashMap<>(1);
    settings.authorization.config.put("server", server);

    for (final String key : configuration.getContextKeys())
      if (key.startsWith("gremlin."))
        applyServerSetting(settings, key.substring("gremlin.".length()), configuration.getValue(key, null));

    // Ensure databases referenced in the graphs section of gremlin-server.yaml are created/opened.
    // This restores the pre-2026.2.1 behaviour where a static `graphs:` entry in gremlin-server.yaml
    // would cause ArcadeGraph to create the database on first access (issue #3661).
    initPreConfiguredDatabases(settings);

    // GUARANTEE THAT ARCADEDB TYPES (VERTICES, EDGES AND RAW RIDs) CAN BE SERIALIZED REGARDLESS OF THE (POSSIBLY
    // ABSENT OR CUSTOM) gremlin-server.yaml, BY ENSURING ArcadeIoRegistry IS REGISTERED ON EVERY SERIALIZER (#5309).
    ensureArcadeIoRegistry(settings);

    // Supply the Gremlin execution pool ourselves so we can bind the authenticated principal into the
    // engine on the worker thread that actually runs each traversal (both bytecode and script paths use
    // this single pool). This is what makes ArcadeDB's per-user ACLs enforce for Gremlin (GHSA-c287).
    final int poolSize = settings.gremlinPool > 0 ? settings.gremlinPool : Runtime.getRuntime().availableProcessors();
    gremlinExecutorService = new GremlinPrincipalPropagatingExecutorService(
        Executors.newFixedThreadPool(poolSize, newGremlinThreadFactory()), server);

    gremlinServer = new GremlinServer(settings, gremlinExecutorService);
    try {
      // start() only reports a failure of the bootstrap itself: the bind is asynchronous, and an address already in use
      // arrives as an exceptional completion of the returned future. Waiting on it makes a failed bind fail the start
      // like the Bolt, Postgres, Redis, MongoDB and HTTP listeners do, instead of leaving the plugin "started" and its
      // port advertised with nothing listening on it (issue #9319).
      within(gremlinServer::start);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      releaseAfterFailedStart();
      throw new ServerException("Interrupted while starting the GremlinServer plugin", e);
    } catch (final ExecutionException e) {
      releaseAfterFailedStart();
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new ServerException("Error on starting GremlinServer plugin on " + settings.host + ":" + settings.port + ": "
          + cause.getMessage(), cause);
    } catch (final TimeoutException e) {
      releaseAfterFailedStart();
      throw new ServerException("The GremlinServer plugin did not finish starting on " + settings.host + ":" + settings.port
          + " within " + START_TIMEOUT_SECONDS + " seconds", e);
    } catch (final Exception e) {
      releaseAfterFailedStart();
      throw new ServerException("Error on starting GremlinServer plugin", e);
    }
    try {
      boundPort = resolveBoundPort(gremlinServer, settings.port);
    } catch (final ServerException e) {
      releaseAfterFailedStart();
      throw e;
    }

    // SCRIPTS MUST SEE THE DATABASES CREATED AFTER THE START, NOT ONLY THE ONES THERE WHEN THE EXECUTOR WAS BUILT (#9147)
    if (gremlinServer.getServerGremlinExecutor().getGraphManager() instanceof ArcadeGraphManager arcadeGraphManager) {
      final Set<String> engineNames = new HashSet<>(Set.of("gremlin-groovy", "gremlin-lang"));
      if (settings.scriptEngines != null)
        engineNames.addAll(settings.scriptEngines.keySet());
      arcadeGraphManager.bindScriptEnginesLive(gremlinServer.getServerGremlinExecutor().getGremlinExecutor(), engineNames);
    }
  }

  /**
   * The port the channel is really bound to, which is the only answer when the setting is {@code 0} (the OS picks one).
   * GremlinServer keeps the channel in a private field and never writes the port back to its settings, so it is read from
   * there (verified against gremlin-server 3.8.2, field {@code serverSocketChannel}; {@code Issue9319GremlinBindFailureTest}
   * fails if an upgrade renames it), falling back to the configured port if that cannot be done.
   */
  private static int resolveBoundPort(final GremlinServer gremlinServer, final int configuredPort) {
    try {
      final Field field = GremlinServer.class.getDeclaredField("serverSocketChannel");
      field.setAccessible(true);
      final Channel channel = (Channel) field.get(gremlinServer);
      if (channel != null && channel.localAddress() instanceof InetSocketAddress address && address.getPort() > 0)
        return address.getPort();
    } catch (final ReflectiveOperationException | RuntimeException e) {
      if (configuredPort <= 0)
        // A listener nobody can find is worse than no listener: the OS-chosen port is the only way to reach it
        throw new ServerException("The Gremlin Server is listening on an OS-chosen port that cannot be determined (a "
            + "TinkerPop upgrade may have changed GremlinServer internals): " + e, e);
      LogManager.instance().log(GremlinServerPlugin.class, Level.WARNING,
          "Cannot read the port the Gremlin Server is bound to, advertising the configured port %d (%s)", null, configuredPort,
          e.toString());
    }
    return configuredPort;
  }

  /**
   * Runs a lifecycle operation of the Gremlin Server and waits for its future, the whole of it under one deadline: in
   * TinkerPop 3.8.2 {@code start()} and {@code stop()} run hooks and close processors before they return the future, so a
   * deadline applied only to the future never starts counting if the call itself blocks.
   */
  private static <T> T within(final Callable<CompletableFuture<T>> operation)
      throws InterruptedException, ExecutionException, TimeoutException {
    final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
      final Thread thread = new Thread(runnable, "arcadedb-gremlin-lifecycle");
      thread.setDaemon(true);
      return thread;
    });
    try {
      final Future<T> future = executor.submit(() -> operation.call().get());
      try {
        return future.get(START_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (final TimeoutException e) {
        future.cancel(true);
        throw e;
      } catch (final ExecutionException e) {
        // the operation's own failure arrives wrapped once by each future
        Throwable cause = e;
        while (cause instanceof ExecutionException && cause.getCause() != null)
          cause = cause.getCause();
        throw new ExecutionException(cause);
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /** The failed start leaves nothing running: the half-started server and its executor are stopped here, not left to the caller. */
  private void releaseAfterFailedStart() {
    try {
      stopService();
    } catch (final Exception e) {
      // BEST EFFORT: THE START FAILURE IS WHAT THE CALLER MUST SEE
      LogManager.instance().log(this, Level.WARNING, "Error releasing the Gremlin Server after a failed start: %s", null,
          e.toString());
    }
  }

  /** Active only while the Gremlin Server is listening, so a plugin whose bind failed is not reported as started. */
  @Override
  public boolean isActive() {
    return boundPort > 0;
  }

  /**
   * The port the Gremlin Server listens on (issue #8578), so the remote {@code ArcadeGraph} reaches it wherever it was
   * configured instead of assuming TinkerPop's default.
   */
  @Override
  public Map<String, Integer> getAdvertisedPorts() {
    final int port = boundPort;
    return port > 0 ? Map.of("gremlin", port) : Map.of();
  }

  /**
   * Copies one {@code gremlin.*} server-configuration key onto the Gremlin Server settings. A name that is not a setting
   * is ignored, as the same keys are shared with other configuration. A value that cannot be converted to the setting's
   * type fails the start: a mistyped port or listener setting must not leave the server on the default, where it would
   * also be advertised to clients.
   */
  static void applyServerSetting(final Settings settings, final String name, final Object value) {
    final Field field;
    try {
      field = settings.getClass().getField(name);
    } catch (final NoSuchFieldException e) {
      // NOT A GREMLIN SERVER SETTING
      return;
    }
    try {
      field.set(settings, coerce(field.getType(), value));
    } catch (final IllegalAccessException | IllegalArgumentException e) {
      if (!isScalar(field.getType())) {
        // A TEXT VALUE FOR A LIST, MAP OR NESTED SETTING CANNOT BE EXPRESSED AS A FLAT KEY: IT WAS ALWAYS SKIPPED
        LogManager.instance().log(GremlinServerPlugin.class, Level.WARNING,
            "Ignoring the Gremlin Server setting 'gremlin.%s': a value of type %s cannot be set from a flat key", null, name,
            field.getType().getSimpleName());
        return;
      }
      throw new ServerException("Invalid Gremlin Server setting 'gremlin." + name + "' with value '" + value + "': " + e.getMessage(),
          e);
    }
  }

  private static boolean isScalar(final Class<?> type) {
    return type.isPrimitive() || Number.class.isAssignableFrom(type) || type == Boolean.class || type == String.class;
  }

  /**
   * A {@code gremlin.*} server setting reaches this plugin as text when it comes from a system property or the
   * command line, and {@link Field#set} refuses to store text into an {@code int} field: the setting was then dropped
   * without a trace and the server started on the default. Numbers and booleans are converted to the field's type.
   */
  private static Object coerce(final Class<?> fieldType, final Object value) {
    if (!(value instanceof String text))
      return value;
    if (fieldType == int.class || fieldType == Integer.class)
      return Integer.valueOf(text.trim());
    if (fieldType == long.class || fieldType == Long.class)
      return Long.valueOf(text.trim());
    if (fieldType == short.class || fieldType == Short.class)
      return Short.valueOf(text.trim());
    if (fieldType == double.class || fieldType == Double.class)
      return Double.valueOf(text.trim());
    if (fieldType == float.class || fieldType == Float.class)
      return Float.valueOf(text.trim());
    if (fieldType == boolean.class || fieldType == Boolean.class) {
      // Boolean.valueOf() turns "yes" or "1" into false without a word: only the two spellings are a boolean
      final String trimmed = text.trim();
      if (!"true".equalsIgnoreCase(trimmed) && !"false".equalsIgnoreCase(trimmed))
        throw new IllegalArgumentException("'" + text + "' is neither true nor false");
      return Boolean.valueOf(trimmed);
    }
    return value;
  }

  private static ThreadFactory newGremlinThreadFactory() {
    final AtomicInteger counter = new AtomicInteger();
    return runnable -> {
      final Thread thread = new Thread(runnable, "arcadedb-gremlin-exec-" + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }

  /**
   * Ensures that {@link ArcadeIoRegistry} is registered on every configured serializer so that ArcadeDB types
   * (vertices, edges and raw RIDs) round-trip correctly. When {@code gremlin-server.yaml} declares no serializers, the
   * shipped default set (GraphBinary v1, GraphSON v3, GraphSON v2) is installed; when it does, each entry is augmented
   * with the registry if missing. Without this, a traversal result carrying a raw RID (e.g. the value of a LINK
   * property) fails to be serialized with "Serializer for type com.arcadedb.database.DatabaseRID not found" (#5309).
   */
  private void ensureArcadeIoRegistry(final Settings settings) {
    if (settings.serializers == null || settings.serializers.isEmpty()) {
      final List<Settings.SerializerSettings> serializers = new ArrayList<>(DEFAULT_SERIALIZERS.length);
      for (final String className : DEFAULT_SERIALIZERS) {
        final Settings.SerializerSettings serializer = new Settings.SerializerSettings();
        serializer.className = className;
        serializer.config = new HashMap<>();
        addArcadeIoRegistry(serializer);
        serializers.add(serializer);
      }
      settings.serializers = serializers;
    } else {
      for (final Settings.SerializerSettings serializer : settings.serializers)
        addArcadeIoRegistry(serializer);
    }
  }

  @SuppressWarnings("unchecked")
  private void addArcadeIoRegistry(final Settings.SerializerSettings serializer) {
    if (serializer.config == null)
      serializer.config = new HashMap<>();

    final Object existing = serializer.config.get(IO_REGISTRIES_KEY);
    final List<Object> registries = existing instanceof List ? new ArrayList<>((List<Object>) existing) : new ArrayList<>();

    if (!registries.contains(ARCADE_IO_REGISTRY))
      registries.add(ARCADE_IO_REGISTRY);

    serializer.config.put(IO_REGISTRIES_KEY, registries);
  }

  /**
   * For every graph declared in the Gremlin settings' {@code graphs} section, reads the matching
   * {@code .properties} file, extracts {@value ArcadeGraph#CONFIG_DIRECTORY}, and makes sure the
   * database is registered with (and, if absent, created by) ArcadeDBServer before the Gremlin
   * server starts.
   */
  private void initPreConfiguredDatabases(final Settings settings) {
    if (settings.graphs == null || settings.graphs.isEmpty())
      return;

    for (final Map.Entry<String, String> entry : settings.graphs.entrySet()) {
      final String graphName = entry.getKey();
      final String propertiesPath = entry.getValue();

      try {
        final String resolvedPath = resolveConfigPath(server.getRootPath(), propertiesPath);
        final File propertiesFile = new File(resolvedPath);
        if (!propertiesFile.exists()) {
          LogManager.instance().log(this, Level.WARNING,
              "Gremlin graph '%s': properties file '%s' not found — skipping database init", graphName, resolvedPath);
          continue;
        }

        final Properties props = new Properties();
        try (final FileInputStream fis = new FileInputStream(propertiesFile)) {
          props.load(fis);
        }

        final String dbDirectory = props.getProperty(ArcadeGraph.CONFIG_DIRECTORY);
        if (dbDirectory == null) {
          LogManager.instance().log(this, Level.WARNING,
              "Gremlin graph '%s': property '%s' not found in '%s' — skipping database init",
              graphName, ArcadeGraph.CONFIG_DIRECTORY, resolvedPath);
          continue;
        }

        // Derive the database name from the last path component (e.g. "./databases/graph" → "graph")
        final String dbName = new File(dbDirectory.replaceAll("/+$", "")).getName();
        if (dbName.isEmpty()) {
          LogManager.instance().log(this, Level.WARNING,
              "Gremlin graph '%s': cannot derive database name from directory '%s' — skipping",
              graphName, dbDirectory);
          continue;
        }

        if (!server.existsDatabase(dbName)) {
          // createIfNotExists=true: creates the database if it doesn't exist yet
          server.getDatabase(dbName, true, true);
          LogManager.instance().log(this, Level.INFO,
              "Gremlin graph '%s': created/opened database '%s'", graphName, dbName);
        }
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING, "Gremlin graph '%s': error initializing database", e, graphName);
      }
    }
  }

  private static String resolveConfigPath(final String rootPath, final String path) {
    final File f = new File(path);
    if (f.isAbsolute())
      return path;
    return new File(rootPath, path).getPath();
  }

  @Override
  public void stopService() {
    boundPort = 0;
    if (gremlinServer != null) {
      // Close all dynamically created ArcadeGraph instances
      final var graphManager = gremlinServer.getServerGremlinExecutor().getGraphManager();
      if (graphManager instanceof ArcadeGraphManager) {
        ((ArcadeGraphManager) graphManager).closeAll();
      }
      boolean stopped = false;
      try {
        // Bounded like the start: a server whose bind failed must not be able to hang the shutdown
        within(gremlinServer::stop);
        stopped = true;
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (final ExecutionException | TimeoutException e) {
        LogManager.instance().log(this, Level.SEVERE,
            "Error or timeout stopping the Gremlin Server, which may still be running and holding its port: %s", null,
            e.toString());
      }
      // A server that did not confirm its stop keeps its handle, so a later stopService() can try again
      if (stopped)
        gremlinServer = null;
    }
    if (gremlinExecutorService != null) {
      gremlinExecutorService.shutdownNow();
      gremlinExecutorService = null;
    }
  }
}
