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
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.integration.misc.IntegrationUtils;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ChannelBinary;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.event.FileServerEventLog;
import com.arcadedb.server.event.ServerEventLog;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.PostServerCommandHandler;
import com.arcadedb.server.monitor.DefaultServerMetrics;
import com.arcadedb.server.monitor.ServerMetrics;
import com.arcadedb.server.monitor.ServerMonitor;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static com.arcadedb.engine.PaginatedFile.MODE.READ_ONLY;
import static com.arcadedb.engine.PaginatedFile.MODE.READ_WRITE;

public class ArcadeDBServer {
  public enum STATUS {OFFLINE, STARTING, ONLINE, SHUTTING_DOWN}

  public static final String                                  CONFIG_SERVER_CONFIGURATION_FILENAME = "config/server-configuration.json";
  private final       ContextConfiguration                    configuration;
  private final       String                                  serverName;
  private             String                                  hostAddress;
  private final       boolean                                 testEnabled;
  private             FileServerEventLog                      eventLog;
  private final       Map<String, ServerPlugin>               plugins                              = new LinkedHashMap<>();
  private             String                                  serverRootPath;
  private             HAServer                                haServer;
  private             ServerSecurity                          security;
  private             HttpServer                              httpServer;
  private final       ConcurrentMap<String, DatabaseInternal> databases                            = new ConcurrentHashMap<>();
  private final       List<TestCallback>                      testEventListeners                   = new ArrayList<>();
  private volatile    STATUS                                  status                               = STATUS.OFFLINE;
  private             ServerMetrics                           serverMetrics                        = new DefaultServerMetrics();
  private             ServerMonitor                           serverMonitor;

  public ArcadeDBServer() {
    this.configuration = new ContextConfiguration();
    serverRootPath = IntegrationUtils.setRootPath(configuration);
    loadConfiguration();
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
    init();
  }

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.configuration = configuration;
    serverRootPath = IntegrationUtils.setRootPath(configuration);
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
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

    if (status != STATUS.OFFLINE)
      return;

    status = STATUS.STARTING;

    eventLog.start();

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_STARTING, null);
    } catch (final Exception e) {
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }

    LogManager.instance()
        .log(this, Level.INFO, "Starting ArcadeDB Server in %s mode with plugins %s ...", GlobalConfiguration.SERVER_MODE.getValueAsString(), getPluginNames());

    // START METRICS & CONNECTED JMX REPORTER
    if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS)) {
      if (serverMetrics != null)
        serverMetrics.stop();
      serverMetrics = new DefaultServerMetrics();
      LogManager.instance().log(this, Level.INFO, "- Metrics Collection Started...");
    }

    security = new ServerSecurity(this, configuration, serverRootPath + "/config");
    security.startService();

    loadDatabases();

    security.loadUsers();

    httpServer = new HttpServer(this);

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      haServer = new HAServer(this, configuration);
      haServer.startService();
    }

    loadDefaultDatabases();

    registerPlugins();

    // RELOAD DATABASE IF A PLUGIN REGISTERED A NEW DATABASE (LIKE THE GREMLIN SERVER)
    loadDatabases();

    loadDefaultFoodDatabase();

    httpServer.startService();

    status = STATUS.ONLINE;

    LogManager.instance().log(this, Level.INFO, "Available query languages: %s", new QueryEngineManager().getAvailableLanguages());

    final String mode = GlobalConfiguration.SERVER_MODE.getValueAsString();

    final String msg = String.format("ArcadeDB Server started in '%s' mode (CPUs=%d MAXRAM=%s)", mode, Runtime.getRuntime().availableProcessors(),
        FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()));
    LogManager.instance().log(this, Level.INFO, msg);

    getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Server", null, msg);

    if (!"production".equals(mode))
      LogManager.instance().log(this, Level.INFO, "Studio web tool available at http://%s:%d ", hostAddress, httpServer.getPort());

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_UP, null);
    } catch (final Exception e) {
      stop();
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }

    serverMonitor.start();
  }

  private void loadDefaultFoodDatabase() {
    if (getDatabaseNames() == null || !getDatabaseNames().contains("Food_Demo")) {
      LogManager.instance().log(this, Level.INFO, "Food demo database not found, creating...");
      Database database = createDatabase("Food_Demo", READ_WRITE, "U", "admin", true, false);

      try {
        InputStream inputStream = PostServerCommandHandler.class.getResourceAsStream("/importFoodDemoDatasetCommand.txt");
        String data = readFromInputStream(inputStream);
        database.command("sql", data);
      } catch (Exception e) {
        throw new ServerException("Error creating food demo dataset: " + e.getMessage());
      }
    } else {
      LogManager.instance().log(this, Level.INFO, "Food demo database found, skipping...");
    }
  }

  private String readFromInputStream(InputStream inputStream) throws IOException {
    StringBuilder resultStringBuilder = new StringBuilder();
    try (BufferedReader br
      = new BufferedReader(new InputStreamReader(inputStream))) {
        String line;
        while ((line = br.readLine()) != null) {
            resultStringBuilder.append(line).append("\n");
        }
    }
    return resultStringBuilder.toString();
  }

  private void welcomeBanner() {
    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server v" + Constants.getVersion() + " is starting up...");

    final String osName = System.getProperty("os.name");
    final String osVersion = System.getProperty("os.version");
    final String vmName = System.getProperty("java.vm.name");
    final String vmVendorVersion = System.getProperty("java.vendor.version");
    final String vmVersion = System.getProperty("java.version");
    LogManager.instance().log(this, Level.INFO,
        "Running on " + osName + " " + osVersion + " - " + (vmName != null ? vmName : "Java") + " " + vmVersion + " " + (vmVendorVersion != null ?
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

  private void registerPlugins() {
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
          pluginInstance.configure(this, configuration);

          pluginInstance.startService();

          plugins.put(pluginName, pluginInstance);

          LogManager.instance().log(this, Level.INFO, "- %s plugin started", pluginName);

        } catch (final Exception e) {
          throw new ServerException("Error on loading plugin from class '" + p + ";", e);
        }
      }
    }
  }

  public synchronized void stop() {
    if (status == STATUS.OFFLINE || status == STATUS.SHUTTING_DOWN)
      return;

    if (serverMonitor != null)
      serverMonitor.stop();

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_SHUTTING_DOWN, null);
    } catch (final Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    status = STATUS.SHUTTING_DOWN;

    for (final Map.Entry<String, ServerPlugin> pEntry : plugins.entrySet()) {
      LogManager.instance().log(this, Level.INFO, "- Stop %s plugin", pEntry.getKey());
      CodeUtils.executeIgnoringExceptions(() -> pEntry.getValue().stopService(), "Error on halting '" + pEntry.getKey() + "' plugin", false);
    }

    if (haServer != null)
      CodeUtils.executeIgnoringExceptions(haServer::stopService, "Error on stopping HA service", false);

    if (httpServer != null)
      CodeUtils.executeIgnoringExceptions(httpServer::stopService, "Error on stopping HTTP service", false);

    if (security != null)
      CodeUtils.executeIgnoringExceptions(security::stopService, "Error on stopping Security service", false);

    for (final Database db : databases.values())
      CodeUtils.executeIgnoringExceptions(db::close, "Error closing database '" + db.getName() + "'", false);
    databases.clear();

    CodeUtils.executeIgnoringExceptions(() -> {
      LogManager.instance().log(this, Level.INFO, "- Stop JMX Metrics");
      serverMetrics.stop();
      serverMetrics = new DefaultServerMetrics();
    }, "Error on stopping JMX Metrics", false);

    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server is down");

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_DOWN, null);
    } catch (final Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().setContext(null);
    status = STATUS.OFFLINE;

    getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.INFO, "Server", null, "Server shutdown correctly");
  }

  public Collection<ServerPlugin> getPlugins() {
    return Collections.unmodifiableCollection(plugins.values());
  }

  public ServerMetrics getServerMetrics() {
    return serverMetrics;
  }

  public Database getDatabase(final String databaseName) {
    return getDatabase(databaseName, false, true);
  }

  public Database getOrCreateDatabase(final String databaseName) {
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

  public synchronized boolean existsDatabase(final String databaseName) {
    return databases.containsKey(databaseName);
  }

  public synchronized DatabaseInternal createDatabase(final String databaseName, final PaginatedFile.MODE mode) { 
    return createDatabase(databaseName, mode, databaseName, null, false, true);
  }

  public synchronized DatabaseInternal createDatabase(final String databaseName, final PaginatedFile.MODE mode,
         String classification, String owner, boolean isPublic, boolean isClassificationValidationEnabled) {   DatabaseInternal db = databases.get(databaseName);
    if (db != null)
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    final DatabaseFactory factory = new DatabaseFactory(
                configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator + databaseName)
        .setAutoTransaction(true)
        .setPublic(isPublic)
        .setClassification(classification)
        .setOwner(owner)
        .setClassificationValidationEnabled(isClassificationValidationEnabled);
    
    if (factory.exists())
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    db = (DatabaseInternal) factory.create();

    if (mode == READ_ONLY) {
      db.close();
      db = (DatabaseInternal) factory.open(mode);
    }

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

    // FORCE LOADING INTO THE SERVER
    databases.put(databaseName, db);

    return db;
  }

  public Set<String> getDatabaseNames() {
    return Collections.unmodifiableSet(databases.keySet());
  }

  public synchronized void removeDatabase(final String databaseName) {
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

  public void registerTestEventListener(final TestCallback callback) {
    testEventListeners.add(callback);
  }

  public void lifecycleEvent(final TestCallback.TYPE type, final Object object) throws Exception {
    if (testEnabled)
      for (final TestCallback c : testEventListeners)
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

  public synchronized Database getDatabase(final String databaseName, final boolean createIfNotExists, final boolean allowLoad) {
    if (databaseName == null || databaseName.trim().isEmpty())
      throw new IllegalArgumentException("Invalid database name " + databaseName);

    DatabaseInternal db = databases.get(databaseName);

    if (db == null || !db.isOpen()) {
      if (!allowLoad)
        throw new DatabaseIsClosedException("Database '" + databaseName + "' is not available");

      final String path = configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator + databaseName;

      final DatabaseFactory factory = new DatabaseFactory(path).setAutoTransaction(true);

      factory.setSecurity(getSecurity());

      PaginatedFile.MODE defaultDbMode = configuration.getValueAsEnum(GlobalConfiguration.SERVER_DEFAULT_DATABASE_MODE, PaginatedFile.MODE.class);
      if (defaultDbMode == null)
        defaultDbMode = READ_WRITE;

      if (createIfNotExists)
        db = (DatabaseInternal) (factory.exists() ? factory.open(defaultDbMode) : factory.create());
      else {
        final Collection<Database> activeDatabases = DatabaseFactory.getActiveDatabaseInstances();
        if (!activeDatabases.isEmpty()) {
          final Database existentDatabase = activeDatabases.iterator().next();
          if (existentDatabase.getDatabasePath().equals(path))
            // REUSE THE OPEN DATABASE. THIS TYPICALLY HAPPENS WHEN A SERVER PLUGIN OPENS THE DATABASE AT STARTUP
            db = (DatabaseInternal) existentDatabase;
          else
            // SAME NAME, BUT DIFFERENT PATH< OPEN A NEW DATABASE. THIS IS MOSTLY FOR TESTS WHERE MULTIPLE SERVERS SHARE THE SAME JVM
            db = (DatabaseInternal) factory.open(defaultDbMode);
        } else
          db = (DatabaseInternal) factory.open(defaultDbMode);
      }

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

      databases.put(databaseName, db);
    }

    return new ServerDatabase(db);
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
      PaginatedFile.MODE defaultDbMode = configuration.getValueAsEnum(GlobalConfiguration.SERVER_DEFAULT_DATABASE_MODE, PaginatedFile.MODE.class);
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
            final String commandType = command.substring(0, commandSeparator).toLowerCase();
            final String commandParams = command.substring(commandSeparator + 1);

            switch (commandType) {
            case "restore":
              // DROP THE DATABASE BECAUSE THE RESTORE OPERATION WILL TAKE CARE OF CREATING A NEW DATABASE
              if (database != null) {
                ((DatabaseInternal) database).getEmbedded().drop();
                databases.remove(dbName);
              }
              final String dbPath = configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator + dbName;
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
              .log(this, Level.WARNING, "Cannot create user '%s' to access database '%s' because the user does not exist", null, credential, dbName);
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
              if (GlobalConfiguration.OIDC_AUTH.getValueAsBoolean()) {
                user = security.authenticate(userName, dbName);
              } else {
                user = security.authenticate(userName, userPassword, dbName);
              }

              // UPDATE DB LIST + GROUP
              user.addDatabase(dbName, new String[] { userGroup });
              security.saveUsers();

            } catch (final ServerSecurityException e) {
              LogManager.instance()
                  .log(this, Level.WARNING, "Cannot create database '%s' because the user '%s' already exists with a different password", null, dbName,
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
        final String content = FileUtils.readFileAsString(file, "UTF8");
        configuration.reset();
        configuration.fromJSON(content);

      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading configuration from file '%s'", e, file);
      }
    }
  }

  private void init() {
    eventLog = new FileServerEventLog(this);

    // SERVER DOES NOT NEED ASYNC WORKERS
    GlobalConfiguration.ASYNC_WORKER_THREADS.setValue(1);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      stop();
    }));

    hostAddress = assignHostAddress();
    serverMonitor = new ServerMonitor(this);
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
        LogManager.instance()
            .log(this, Level.SEVERE, "Error: HOSTNAME environment variable not found but needed when running inside Kubernetes. The server will be halted");
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
