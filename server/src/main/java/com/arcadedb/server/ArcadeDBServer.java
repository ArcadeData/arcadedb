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
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.ServerPathUtils;
import com.arcadedb.network.binary.ChannelBinary;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.event.FileServerEventLog;
import com.arcadedb.server.event.ServerEventLog;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.mcp.MCPConfiguration;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.plugin.PluginManager;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.ServerPathUtils;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.arcadedb.engine.ComponentFile.MODE.READ_ONLY;
import static com.arcadedb.engine.ComponentFile.MODE.READ_WRITE;

public class ArcadeDBServer {
  public enum Status {OFFLINE, STARTING, ONLINE, SHUTTING_DOWN}

  public static final String                                CONFIG_SERVER_CONFIGURATION_FILENAME = "config/server-configuration.json";
  private volatile    Status                                status                               = Status.OFFLINE;
  private final       ContextConfiguration                  configuration;
  private final       String                                serverName;
  private final       boolean                               replicationLifecycleEventsEnabled;
  private final       Map<String, ServerPlugin>             plugins                              = new LinkedHashMap<>();
  private final       ConcurrentMap<String, ServerDatabase> databases                            = new ConcurrentHashMap<>();
  private final       List<ReplicationCallback>             testEventListeners                   = new ArrayList<>();
  private             String                                hostAddress;
  private             FileServerEventLog                    eventLog;
  private final       Map<String, ServerPlugin>             plugins                              = new LinkedHashMap<>();
  private             PluginManager                         pluginManager;
  private             String                                serverRootPath;
  private             HAServer                              haServer;
  private             ServerSecurity                        security;
  private             HttpServer                            httpServer;
  private             MCPConfiguration                      mcpConfiguration;
  private             ServerQueryProfiler                   queryProfiler;
  private final       ConcurrentMap<String, ServerDatabase> databases                            = new ConcurrentHashMap<>();
  private final       List<ReplicationCallback>             testEventListeners                   = new ArrayList<>();
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

  public synchronized void start() {
    LogManager.instance().setContext(getServerName());

    welcomeBanner();

    if (status != Status.OFFLINE)
      return;

    status = Status.STARTING;

    eventLog.start();

    try {
      lifecycleEvent(ReplicationCallback.Type.SERVER_STARTING, null);
    } catch (final Exception e) {
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }

    // Discover plugins from lib/plugins directory
    pluginManager.discoverPlugins();

    LogManager.instance().log(this, Level.INFO, "Starting ArcadeDB Server in %s mode with plugins %s ...",
        GlobalConfiguration.SERVER_MODE.getValueAsString(), getAllPluginNames());

    // START METRICS & CONNECTED JMX REPORTER
    if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS)) {
      Metrics.addRegistry(new SimpleMeterRegistry());

      new ClassLoaderMetrics().bindTo(Metrics.globalRegistry);
      new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
      new JvmGcMetrics().bindTo(Metrics.globalRegistry);
      new ProcessorMetrics().bindTo(Metrics.globalRegistry);
      new JvmThreadMetrics().bindTo(Metrics.globalRegistry);

      if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS_LOGGING)) {
        LogManager.instance().log(this, Level.INFO, "- Logging metrics enabled...");
        Metrics.addRegistry(new LoggingMeterRegistry());
      }
      LogManager.instance().log(this, Level.INFO, "- Metrics Collection Started...");
    }

    security = new ServerSecurity(this, configuration, serverRootPath + "/config");
    security.startService();

    createDirectories();

    loadDatabases();

    security.loadUsers();

    // INITIALIZE MCP CONFIGURATION (always available, disabled by default)
    mcpConfiguration = new MCPConfiguration(serverRootPath);
    mcpConfiguration.load();

    // START HTTP SERVER IMMEDIATELY. THE HTTP ADDRESS WILL BE USED BY HA
    httpServer = new HttpServer(this);

//    registerPlugins(ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON);
    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.BEFORE_HTTP_ON);

    httpServer.startService();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      haServer = new HAServer(this, configuration);
      haServer.startService();
    }

    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.AFTER_HTTP_ON);

    loadDefaultDatabases();

    // RELOAD DATABASE IF A PLUGIN REGISTERED A NEW DATABASE (LIKE THE GREMLIN SERVER)
    loadDatabases();

    pluginManager.startPlugins(ServerPlugin.PluginInstallationPriority.AFTER_DATABASES_OPEN);

    status = Status.ONLINE;

    LogManager.instance().log(this, Level.INFO, "Available query languages: %s",
        QueryEngineManager.getInstance().getAvailableLanguages());

    final String mode = GlobalConfiguration.SERVER_MODE.getValueAsString();

    final String msg = "ArcadeDB Server started in '%s' mode (CPUs=%d MAXRAM=%s)".formatted(mode,
        Runtime.getRuntime().availableProcessors(), FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()));
    LogManager.instance().log(this, Level.INFO, msg);

    getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Server", null, msg);

    if (!"production".equals(mode)) {
      final InputStream file = getClass().getClassLoader().getResourceAsStream("static/index.html");
      if (file != null)
        LogManager.instance()
            .log(this, Level.INFO, "Studio web tool available at http://%s:%d ", hostAddress, httpServer.getPort());
    }

    try {
      lifecycleEvent(ReplicationCallback.Type.SERVER_UP, null);
    } catch (final Exception e) {
      stop();
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }

//    serverMonitor.start();
  }

  private void createDirectories() {

    LogManager.instance().log(this, Level.INFO, "Server root path: %s", configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH));
    LogManager.instance().log(this, Level.INFO, "Databases directory: %s", configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    final File databaseDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    if (!databaseDir.exists()) {
      if (!databaseDir.mkdirs()) {
        LogManager.instance().log(this, Level.SEVERE, "Failed to create databases directory: %s", databaseDir.getAbsolutePath());
        throw new ServerException("Unable to create databases directory: " + databaseDir.getAbsolutePath());
      }
    }

    LogManager.instance().log(this, Level.INFO, "Backups directory: %s", configuration.getValueAsString(GlobalConfiguration.SERVER_BACKUP_DIRECTORY));
    final File backupsDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_BACKUP_DIRECTORY));
    if (!backupsDir.exists()) {
      if (!backupsDir.mkdirs()) {
        LogManager.instance().log(this, Level.SEVERE, "Failed to create backups directory: %s", backupsDir.getAbsolutePath());
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

  private void registerPlugins(final ServerPlugin.PluginInstallationPriority installationPriority) {
    final String registeredPlugins = configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    if (registeredPlugins != null && !registeredPlugins.isEmpty()) {
      final String[] pluginEntries = registeredPlugins.split(",");
      for (final String p : pluginEntries) {
        try {
          final String[] pluginPair = p.split(":");

          final String pluginName = pluginPair[0];
          final String pluginClass = pluginPair.length > 1 ? pluginPair[1] : pluginPair[0];

          final Class<ServerPlugin> c = (Class<ServerPlugin>) Class.forName(pluginClass);
          final ServerPlugin pluginInstance = c.getConstructor().newInstance();

          if (pluginInstance.getInstallationPriority() != installationPriority)
            continue;

          pluginInstance.configure(this, configuration);

          pluginInstance.startService();

          plugins.put(pluginName, pluginInstance);

          LogManager.instance().log(this, Level.INFO, "- %s plugin started", pluginName);

        } catch (final Exception e) {
          throw new ServerException("Error on loading plugin from class '" + p + ";", e);
        }
      }
    }

    // Auto-register backup scheduler plugin if backup.json exists and not already registered
    if (installationPriority == ServerPlugin.PluginInstallationPriority.AFTER_DATABASES_OPEN
        && !plugins.containsKey("auto-backup")) {
      registerAutoBackupPluginIfConfigured();
    }
  }

  private void registerAutoBackupPluginIfConfigured() {
    final File backupConfigFile = Paths.get(serverRootPath, "config", "backup.json").toFile();
    if (backupConfigFile.exists()) {
      try {
        final Class<ServerPlugin> c = (Class<ServerPlugin>) Class.forName(
            "com.arcadedb.server.backup.AutoBackupSchedulerPlugin");
        final ServerPlugin pluginInstance = c.getConstructor().newInstance();

        pluginInstance.configure(this, configuration);
        pluginInstance.startService();

        plugins.put("auto-backup", pluginInstance);

        LogManager.instance().log(this, Level.INFO, "- auto-backup plugin started (auto-detected config/backup.json)");

      } catch (final ClassNotFoundException e) {
        // Plugin class not available, skip silently
        LogManager.instance().log(this, Level.FINE,
            "Auto-backup plugin class not found, skipping auto-registration");
      } catch (final Exception e) {
        LogManager.instance().log(this, Level.WARNING,
            "Error auto-registering backup plugin", e);
      }
    }
  }

  public synchronized void stop() {
    if (status == Status.OFFLINE || status == Status.SHUTTING_DOWN)
      return;

    LogManager.instance().log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    try {
      lifecycleEvent(ReplicationCallback.Type.SERVER_SHUTTING_DOWN, null);
    } catch (final Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    status = Status.SHUTTING_DOWN;

    // Stop plugins managed by PluginManager first
    if (pluginManager != null)
      pluginManager.stopPlugins();

    // Stop legacy plugins
    for (final Map.Entry<String, ServerPlugin> pEntry : plugins.entrySet()) {
      LogManager.instance().log(this, Level.INFO, "- Stop %s plugin", pEntry.getKey());
      CodeUtils.executeIgnoringExceptions(() -> pEntry.getValue().stopService(),
          "Error on halting '" + pEntry.getKey() + "' plugin", false);
    }

    if (haServer != null)
      CodeUtils.executeIgnoringExceptions(haServer::stopService, "Error on stopping HA service", false);

    if (httpServer != null)
      CodeUtils.executeIgnoringExceptions(httpServer::stopService, "Error on stopping HTTP service", false);

    if (security != null)
      CodeUtils.executeIgnoringExceptions(security::stopService, "Error on stopping Security service", false);

    for (final ServerDatabase db : databases.values())
      CodeUtils.executeIgnoringExceptions(db.getEmbedded()::close, "Error closing database '" + db.getName() + "'", false);
    databases.clear();

    CodeUtils.executeIgnoringExceptions(() -> {
      LogManager.instance().log(this, Level.INFO, "- Stop JMX Metrics");
    }, "Error on stopping JMX Metrics", false);

    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server is down");

    try {
      lifecycleEvent(ReplicationCallback.Type.SERVER_DOWN, null);
    } catch (final Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().setContext(null);
    status = Status.OFFLINE;

    getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Server", null, "Server shutdown correctly");

    ServerLogManager.resetFinally();
  }

  public Collection<ServerPlugin> getPlugins() {
    final List<ServerPlugin> allPlugins = new ArrayList<>(plugins.values());
    if (pluginManager != null)
      allPlugins.addAll(pluginManager.getPlugins());
    return Collections.unmodifiableCollection(allPlugins);
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
    return status == Status.ONLINE;
  }

  public Status getStatus() {
    return status;
  }

  public boolean existsDatabase(final String databaseName) {
    return databases.containsKey(databaseName);
  }

  public ServerDatabase createDatabase(final String databaseName, final ComponentFile.MODE mode) {
    ServerDatabase serverDatabase;
    synchronized (databases) {
      serverDatabase = databases.get(databaseName);
      if (serverDatabase != null)
        throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

      final DatabaseFactory factory = new DatabaseFactory(
          configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator
              + databaseName).setAutoTransaction(true);

      if (factory.exists())
        throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

      DatabaseInternal embeddedDatabase = (DatabaseInternal) factory.create();

      if (mode == READ_ONLY) {
        embeddedDatabase.close();
        embeddedDatabase = (DatabaseInternal) factory.open(mode);
      }

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        embeddedDatabase = new ReplicatedDatabase(this, (LocalDatabase) embeddedDatabase);

      serverDatabase = new ServerDatabase(this, embeddedDatabase);

      // FORCE LOADING INTO THE SERVER
      databases.put(databaseName, serverDatabase);
      return serverDatabase;
    }
  }

  public Set<String> getDatabaseNames() {
    return Collections.unmodifiableSet(databases.keySet());
  }

  public ServerDatabase registerDatabase(final String databaseName, final DatabaseInternal database) {
    final ServerDatabase serverDatabase = new ServerDatabase(this, database);
    final ServerDatabase existing = databases.putIfAbsent(databaseName, serverDatabase);
    if (existing != null)
      throw new IllegalArgumentException("Database '" + databaseName + "' already registered");
    return serverDatabase;
  }

  public void removeDatabase(final String databaseName) {
    databases.remove(databaseName);
  }

  public String getServerName() {
    return serverName;
  }

  public String getHostAddress() {
    return hostAddress;
  }

  public HAServer getHA() {
    return haServer;
  }

  public ServerSecurity getSecurity() {
    return security;
  }

  public MCPConfiguration getMCPConfiguration() {
    return mcpConfiguration;
  }

  public ServerQueryProfiler getQueryProfiler() {
    return queryProfiler;
  }

  public void registerTestEventListener(final ReplicationCallback callback) {
    testEventListeners.add(callback);
  }

  public void lifecycleEvent(final ReplicationCallback.Type type, final Object object) throws Exception {
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

  public ServerDatabase getDatabase(final String databaseName, final boolean createIfNotExists, final boolean allowLoad) {
    if (databaseName == null || databaseName.trim().isEmpty())
      throw new IllegalArgumentException("Invalid database name " + databaseName);

    ServerDatabase db;
    synchronized (databases) {
      db = databases.get(databaseName);

      if (db == null || !db.isOpen()) {
        if (!allowLoad)
          throw new DatabaseOperationException("Database '" + databaseName + "' is not available");

        final String path =
            configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator + databaseName;

        final DatabaseFactory factory = new DatabaseFactory(path).setAutoTransaction(true);

        factory.setSecurity(getSecurity());

        ComponentFile.MODE defaultDbMode = configuration.getValueAsEnum(GlobalConfiguration.SERVER_DEFAULT_DATABASE_MODE,
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

        if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
          embDatabase = new ReplicatedDatabase(this, (LocalDatabase) embDatabase);

        db = new ServerDatabase(this, embDatabase);

        databases.put(databaseName, db);
      }
    }
    return db;
  }

  private void loadDatabases() {
    final File databaseDir = new File(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    if (!databaseDir.exists()) {
      databaseDir.mkdirs();
    } else {
      if (!databaseDir.isDirectory())
        throw new ConfigurationException("Configured database directory '" + databaseDir + "' is not a directory on file system");

      if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_DATABASE_LOADATSTARTUP)) {
        final File[] databaseDirectories = databaseDir.listFiles(File::isDirectory);
        for (final File f : databaseDirectories)
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
          final String commands = db.substring(credentialEnd + 2, db.length() - 1);

          final String[] commandParts = commands.split(",");
          for (final String command : commandParts) {
            final int commandSeparator = command.indexOf(":");
            if (commandSeparator < 0) {
              LogManager.instance().log(this, Level.WARNING, "Error in startup command configuration format: '%s'", commands);
              break;
            }
            final String commandType = command.substring(0, commandSeparator).toLowerCase(Locale.ENGLISH);
            final String commandParams = command.substring(commandSeparator + 1);

            switch (commandType) {
            case "restore":
              // DROP THE DATABASE BECAUSE THE RESTORE OPERATION WILL TAKE CARE OF CREATING A NEW DATABASE
              if (database != null) {
                ((DatabaseInternal) database).getEmbedded().drop();
                databases.remove(dbName);
              }
              final String dbPath =
                  configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator + dbName;
//              new Restore(commandParams, dbPath).restoreDatabase();

              try {
                final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
                final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(commandParams, dbPath);

                clazz.getMethod("restoreDatabase").invoke(restorer);

              } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
                throw new CommandExecutionException("Error on restoring database, restore libs not found in classpath", e);
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
              database.command("sql", "import database " + commandParams);
              break;

            default:
              LogManager.instance().log(this, Level.SEVERE, "Unsupported command %s in startup command: '%s'", null, commandType);
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
              .log(this, Level.WARNING, "Cannot create user '%s' to access database '%s' because the user does not exist", null,
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
                  "Cannot create database '%s' because the user '%s' already exists with a different password", null, dbName,
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

      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading configuration from file '%s'", e, file);
      }
    }
  }

  private void init() {
    eventLog = new FileServerEventLog(this);
    queryProfiler = new ServerQueryProfiler(this);
    pluginManager = new PluginManager(this, configuration);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      DefaultLogger.setShuttingDown(true);
        LogManager.instance().log(this, Level.SEVERE, "Received shutdown signal. The server will be halted");
      stop();
    }));

    hostAddress = assignHostAddress();
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
            "Error: HOSTNAME environment variable not found but needed when running inside Kubernetes. The server will be halted");
        stop();
        System.exit(1);
        return null;
      }

      hostAddress = hostNameEnvVariable + configuration.getValueAsString(GlobalConfiguration.HA_K8S_DNS_SUFFIX);
      LogManager.instance().log(this, Level.INFO, "Server is running inside Kubernetes. Hostname: %s", null, hostAddress);

    } else if (hostNameEnvVariable != null) {
      hostAddress = hostNameEnvVariable;
    } else {
      // READ HOST FROM NETWORK INTERFACE
      hostAddress = configuration.getValueAsString(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST);
      if (hostAddress.equals("0.0.0.0")) {
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
