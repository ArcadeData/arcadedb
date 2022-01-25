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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseIsClosedException;
//import com.arcadedb.integration.restore.Restore;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngineManager;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.CodeUtils;
import com.arcadedb.utility.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class ArcadeDBServer {
  public enum STATUS {OFFLINE, STARTING, ONLINE, SHUTTING_DOWN}

  public static final String                                  CONFIG_SERVER_CONFIGURATION_FILENAME = "config/server-configuration.json";
  private final       ContextConfiguration                    configuration;
  private final       String                                  serverName;
  private final       boolean                                 testEnabled;
  private final       Map<String, ServerPlugin>               plugins                              = new LinkedHashMap<>();
  private             String                                  serverRootPath;
  private             HAServer                                haServer;
  private             ServerSecurity                          security;
  private             HttpServer                              httpServer;
  private final       ConcurrentMap<String, DatabaseInternal> databases                            = new ConcurrentHashMap<>();
  private final       List<TestCallback>                      testEventListeners                   = new ArrayList<>();
  private volatile    STATUS                                  status                               = STATUS.OFFLINE;
  private             ServerMetrics                           serverMetrics                        = new NoServerMetrics();

  public ArcadeDBServer() {
    this.configuration = new ContextConfiguration();

    setRootPath(configuration);

    loadConfiguration();

    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
  }

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.configuration = configuration;
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);

    setRootPath(configuration);
  }

  public static void main(final String[] args) {
    new ArcadeDBServer().start();
  }

  public ContextConfiguration getConfiguration() {
    return configuration;
  }

  public synchronized void start() {
    LogManager.instance().setContext(getServerName());

    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server v" + Constants.getVersion() + " is starting up...");

    if (status != STATUS.OFFLINE)
      return;

    status = STATUS.STARTING;

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_STARTING, null);
    } catch (Exception e) {
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }

    LogManager.instance().log(this, Level.INFO, "Starting ArcadeDB Server with plugins %s ...", getPluginNames());

    // START METRICS & CONNECTED JMX REPORTER
    if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS)) {
      serverMetrics.stop();
      serverMetrics = new JMXServerMetrics();
      LogManager.instance().log(this, Level.INFO, "- JMX Metrics Started...");
    }

    security = new ServerSecurity(this, configuration, serverRootPath + "/config");
    security.startService();

    loadDatabases();

    security.loadUsers();

    loadDefaultDatabases();

    httpServer = new HttpServer(this);

    registerPlugins();

    httpServer.startService();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      haServer = new HAServer(this, configuration);
      haServer.startService();
    }

    status = STATUS.ONLINE;

    LogManager.instance().log(this, Level.INFO, "Available query languages: %s", new QueryEngineManager().getAvailableLanguages());

    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server started (CPUs=%d MAXRAM=%s)", Runtime.getRuntime().availableProcessors(),
        FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()));

    LogManager.instance().log(this, Level.INFO, "Studio web tool available at http://localhost:%d ", httpServer.getPort());

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_UP, null);
    } catch (Exception e) {
      stop();
      throw new ServerException("Error on starting the server '" + serverName + "'");
    }
  }

  private Set<String> getPluginNames() {
    final Set<String> result = new LinkedHashSet<>();
    final String registeredPlugins = configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);
    if (registeredPlugins != null && !registeredPlugins.isEmpty()) {
      final String[] pluginEntries = registeredPlugins.split(",");
      for (String p : pluginEntries) {
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
      for (String p : pluginEntries) {
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

        } catch (Exception e) {
          throw new ServerException("Error on loading plugin from class '" + p + ";", e);
        }
      }
    }
  }

  public synchronized void stop() {
    if (status == STATUS.OFFLINE || status == STATUS.SHUTTING_DOWN)
      return;

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_SHUTTING_DOWN, null);
    } catch (Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    status = STATUS.SHUTTING_DOWN;

    for (Map.Entry<String, ServerPlugin> pEntry : plugins.entrySet()) {
      LogManager.instance().log(this, Level.INFO, "- Stop %s plugin", pEntry.getKey());
      CodeUtils.executeIgnoringExceptions(() -> pEntry.getValue().stopService(), "Error on halting '" + pEntry.getKey() + "' plugin");
    }

    if (haServer != null)
      CodeUtils.executeIgnoringExceptions(haServer::stopService, "Error on stopping HA service");

    if (httpServer != null)
      CodeUtils.executeIgnoringExceptions(httpServer::stopService, "Error on stopping HTTP service");

    if (security != null)
      CodeUtils.executeIgnoringExceptions(security::stopService, "Error on stopping Security service");

    for (Database db : databases.values())
      CodeUtils.executeIgnoringExceptions(db::close, "Error closing database '" + db.getName() + "'");
    databases.clear();

    CodeUtils.executeIgnoringExceptions(() -> {
      LogManager.instance().log(this, Level.INFO, "- Stop JMX Metrics");
      serverMetrics.stop();
      serverMetrics = new NoServerMetrics();
    }, "Error on stopping JMX Metrics");

    LogManager.instance().log(this, Level.INFO, "ArcadeDB Server is down");

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_DOWN, null);
    } catch (Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().setContext(null);
    status = STATUS.OFFLINE;
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

  public boolean isStarted() {
    return status == STATUS.ONLINE;
  }

  public STATUS getStatus() {
    return status;
  }

  public synchronized boolean existsDatabase(final String databaseName) {
    return databases.containsKey(databaseName);
  }

  public synchronized DatabaseInternal createDatabase(final String databaseName) {
    DatabaseInternal db = databases.get(databaseName);
    if (db != null)
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    final DatabaseFactory factory = new DatabaseFactory(
        configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName).setAutoTransaction(true);

    if (factory.exists())
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    db = (DatabaseInternal) factory.create();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

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
      for (TestCallback c : testEventListeners)
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
    DatabaseInternal db = databases.get(databaseName);

    if (db == null || !db.isOpen()) {
      if (!allowLoad)
        throw new DatabaseIsClosedException("Database '" + databaseName + "' is not available");

      final String path = configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName;

      final DatabaseFactory factory = new DatabaseFactory(path).setAutoTransaction(true);

      factory.setSecurity(getSecurity());

      if (createIfNotExists)
        db = (DatabaseInternal) (factory.exists() ? factory.open() : factory.create());
      else
        db = (DatabaseInternal) factory.open();

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

      final File[] databaseDirectories = databaseDir.listFiles(File::isDirectory);
      for (File f : databaseDirectories)
        getDatabase(f.getName());
    }
  }

  private void loadDefaultDatabases() {
    final String defaultDatabases = configuration.getValueAsString(GlobalConfiguration.SERVER_DEFAULT_DATABASES);
    if (defaultDatabases != null && !defaultDatabases.isEmpty()) {
      // CREATE DEFAULT DATABASES
      final String[] dbs = defaultDatabases.split(";");
      for (String db : dbs) {
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
          for (String command : commandParts) {
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
              String dbPath = configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + dbName;
//              new Restore(commandParams, dbPath).restoreDatabase();

              try {
                final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
                final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(commandParams, dbPath);

                clazz.getMethod("restoreDatabase").invoke(restorer);

              } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
                throw new CommandExecutionException("Error on restoring database, restore libs not found in classpath", e);
              } catch (InvocationTargetException e) {
                throw new CommandExecutionException("Error on restoring database", e.getTargetException());
              }



              getDatabase(dbName);
              break;

            case "import":
              if (database == null) {
                // CREATE THE DATABASE
                LogManager.instance().log(this, Level.INFO, "Creating default database '%s'...", null, dbName);
                database = createDatabase(dbName);
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
            createDatabase(dbName);
          }
        }
      }
    }
  }

  private void parseCredentials(final String dbName, final String credentials) {
    final String[] credentialPairs = credentials.split(",");
    for (String credential : credentialPairs) {

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
        final String userRole = credentialParts.length > 2 ? credentialParts[2] : null;

        if (security.existsUser(userName)) {
          // EXISTING USER: CHECK CREDENTIALS
          try {
            final ServerSecurityUser user = security.authenticate(userName, userPassword, dbName);
            if (!user.getAuthorizedDatabases().contains(dbName)) {
              // UPDATE DB LIST
              user.addDatabase(dbName, new String[] { userRole });
              security.saveUsers();
            }

          } catch (ServerSecurityException e) {
            LogManager.instance()
                .log(this, Level.WARNING, "Cannot create database '%s' because the user '%s' already exists with a different password", null, dbName, userName);
          }
        } else {
          // CREATE A NEW USER
          security.createUser(new JSONObject().put("name", userName)//
              .put("password", security.encodePassword(userPassword))//
              .put("databases", new JSONObject().put(dbName, new JSONArray())));
        }
      }
    }
  }

  private void loadConfiguration() {
    final File file = new File(getRootPath() + "/" + CONFIG_SERVER_CONFIGURATION_FILENAME);
    if (file.exists()) {
      try {
        final String content = FileUtils.readFileAsString(file, "UTF8");
        configuration.reset();
        configuration.fromJSON(content);

      } catch (IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on loading configuration from file '%s'", e, file);
      }
    }
  }

  private void setRootPath(final ContextConfiguration configuration) {
    serverRootPath = configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH);
    if (serverRootPath == null) {
      serverRootPath = new File("config").exists() ? "." : new File("../config").exists() ? ".." : ".";
      configuration.setValue(GlobalConfiguration.SERVER_ROOT_PATH, serverRootPath);
    }
  }

}
