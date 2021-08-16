/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server;

import com.arcadedb.Constants;
import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.ReplicatedDatabase;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.log.ServerLogger;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class ArcadeDBServer implements ServerLogger {
  public enum STATUS {OFFLINE, STARTING, ONLINE, SHUTTING_DOWN}

  public static final String                                  CONFIG_SERVER_CONFIGURATION_FILENAME = "config/server-configuration.json";
  private final       ContextConfiguration                    configuration;
  private final       boolean                                 fileConfiguration;
  private final       String                                  serverName;
  private final       boolean                                 testEnabled;
  private final       Map<String, ServerPlugin>               plugins                              = new HashMap<>();
  private             HAServer                                haServer;
  private             ServerSecurity                          security;
  private             HttpServer                              httpServer;
  private             ConcurrentMap<String, DatabaseInternal> databases                            = new ConcurrentHashMap<>();
  private             List<TestCallback>                      testEventListeners                   = new ArrayList<>();
  private volatile    STATUS                                  status                               = STATUS.OFFLINE;
  private             ServerMetrics                           serverMetrics                        = new NoServerMetrics();

  public ArcadeDBServer() {
    this.configuration = new ContextConfiguration();
    this.fileConfiguration = true;
    loadConfiguration();

    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
  }

  public ArcadeDBServer(final ContextConfiguration configuration) {
    this.fileConfiguration = false;
    this.configuration = configuration;
    this.serverName = configuration.getValueAsString(GlobalConfiguration.SERVER_NAME);
    this.testEnabled = configuration.getValueAsBoolean(GlobalConfiguration.TEST);
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

    log(this, Level.INFO, "Starting ArcadeDB Server...");

    // START METRICS & CONNECTED JMX REPORTER
    if (configuration.getValueAsBoolean(GlobalConfiguration.SERVER_METRICS)) {
      serverMetrics.stop();
      serverMetrics = new JMXServerMetrics();
      log(this, Level.INFO, "- JMX Metrics Started...");
    }

    security = new ServerSecurity(configuration, "config");
    security.startService();

    loadDatabases();

    httpServer = new HttpServer(this);
    httpServer.startService();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      haServer = new HAServer(this, configuration);
      haServer.startService();
    }

    final String registeredPlugins = configuration.getValueAsString(GlobalConfiguration.SERVER_PLUGINS);

    if (registeredPlugins != null && !registeredPlugins.isEmpty()) {
      final String[] pluginEntries = registeredPlugins.split(",");
      for (String p : pluginEntries) {
        try {
          final String[] pluginPair = p.split(":");

          final String pluginName = pluginPair[0];
          final String pluginClass = pluginPair.length > 1 ? pluginPair[1] : pluginPair[0];

          final Class<ServerPlugin> c = (Class<ServerPlugin>) Class.forName(pluginClass);
          final ServerPlugin pluginInstance = c.newInstance();
          pluginInstance.configure(this, configuration);

          pluginInstance.startService();

          plugins.put(pluginName, pluginInstance);

          log(this, Level.INFO, "- %s plugin started", pluginName);

        } catch (Exception e) {
          throw new ServerException("Error on loading plugin from class '" + p + ";", e);
        }
      }
    }

    status = STATUS.ONLINE;

    log(this, Level.INFO, "ArcadeDB Server started (CPUs=%d MAXRAM=%s)", Runtime.getRuntime().availableProcessors(),
        FileUtils.getSizeAsString(Runtime.getRuntime().maxMemory()));

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_UP, null);
    } catch (Exception e) {
      stop();
      throw new ServerException("Error on starting the server '" + serverName + "'");
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

    log(this, Level.INFO, "Shutting down ArcadeDB Server...");

    status = STATUS.SHUTTING_DOWN;

    for (Map.Entry<String, ServerPlugin> pEntry : plugins.entrySet()) {
      log(this, Level.INFO, "- Stop %s plugin", pEntry.getKey());
      try {
        pEntry.getValue().stopService();
      } catch (Exception e) {
        log(this, Level.SEVERE, "Error on halting %s plugin (error=%s)", pEntry.getKey(), e);
      }
    }

    if (haServer != null)
      haServer.stopService();

    if (httpServer != null)
      httpServer.stopService();

    if (security != null)
      security.stopService();

    for (Database db : databases.values())
      db.close();
    databases.clear();

    log(this, Level.INFO, "- Stop JMX Metrics");
    serverMetrics.stop();
    serverMetrics = new NoServerMetrics();

    log(this, Level.INFO, "ArcadeDB Server is down");

    try {
      lifecycleEvent(TestCallback.TYPE.SERVER_DOWN, null);
    } catch (Exception e) {
      throw new ServerException("Error on stopping the server '" + serverName + "'");
    }

    LogManager.instance().setContext(null);
    status = STATUS.OFFLINE;
  }

  public ServerMetrics getServerMetrics() {
    return serverMetrics;
  }

  public Database getDatabase(final String databaseName) {
    return getDatabase(databaseName, false);
  }

  public Database getOrCreateDatabase(final String databaseName) {
    return getDatabase(databaseName, true);
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

    final DatabaseFactory factory = new DatabaseFactory(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName)
        .setAutoTransaction(true);

    if (factory.exists())
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    db = (DatabaseInternal) factory.create();

    if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
      db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

    databases.put(databaseName, db);

    return db;
  }

  public Set<String> getDatabaseNames() {
    return databases.keySet();
  }

  public void log(final Object requester, final Level level, final String message) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);
    LogManager.instance().log(requester, level, message, null);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);
    LogManager.instance().log(requester, level, message, null, arg1);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);
    LogManager.instance().log(requester, level, message, null, arg1, arg2);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);
    LogManager.instance().log(requester, level, message, null, arg1, arg2, arg3);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3, final Object arg4) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);
    LogManager.instance().log(requester, level, message, null, arg1, arg2, arg3, arg4);
  }

  public void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
      final Object arg5) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);
    LogManager.instance().log(requester, level, message, null, arg1, arg2, arg3, arg4, arg5);
  }

  public void log(final Object requester, final Level level, final String message, final Object... args) {
    if (!serverName.equals(LogManager.instance().getContext()))
      LogManager.instance().setContext(serverName);

    LogManager.instance().log(requester, level, message, null, args);
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
    return new File(configuration.getValueAsString(GlobalConfiguration.SERVER_ROOT_PATH)).getAbsolutePath();
  }

  public HttpServer getHttpServer() {
    return httpServer;
  }

  @Override
  public String toString() {
    return getServerName();
  }

  private synchronized Database getDatabase(final String databaseName, final boolean createIfNotExists) {
    DatabaseInternal db = databases.get(databaseName);
    if (db == null || !db.isOpen()) {

      final DatabaseFactory factory = new DatabaseFactory(configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName)
          .setAutoTransaction(true);

      if (createIfNotExists)
        db = (DatabaseInternal) (factory.exists() ? factory.open() : factory.create());
      else
        db = (DatabaseInternal) factory.open();

      if (configuration.getValueAsBoolean(GlobalConfiguration.HA_ENABLED))
        db = new ReplicatedDatabase(this, (EmbeddedDatabase) db);

      databases.put(databaseName, db);
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

      final File[] databaseDirectories = databaseDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File pathname) {
          return pathname.isDirectory();
        }
      });

      for (File f : databaseDirectories)
        getDatabase(f.getName());
    }

    final String defaultDatabases = configuration.getValueAsString(GlobalConfiguration.SERVER_DEFAULT_DATABASES);
    if (defaultDatabases != null && !defaultDatabases.isEmpty()) {
      // CREATE DEFAULT DATABASES
      final String[] dbs = defaultDatabases.split(";");
      for (String db : dbs) {
        final int credentialPos = db.indexOf('[');
        if (credentialPos < 0) {
          LogManager.instance().log(this, Level.WARNING, "Error in default databases format: '%s'", null, defaultDatabases);
          break;
        }

        final String dbName = db.substring(0, credentialPos);
        final String credentials = db.substring(credentialPos + 1, db.length() - 1);

        final String[] credentialPairs = credentials.split(",");
        for (String credential : credentialPairs) {

          final int passwordSeparator = credential.indexOf(":");

          if (passwordSeparator < 0) {
            if (!security.existsUser(credential)) {
              LogManager.instance()
                  .log(this, Level.WARNING, "Cannot create user '%s' to access database '%s' because the user does not exist", null, credential, dbName);
              continue;
            }
            //FIXME: else if user exists, should we give him access to the dbName?
          } else {
            final String userName = credential.substring(0, passwordSeparator);
            final String userPassword = credential.substring(passwordSeparator + 1);

            if (security.existsUser(userName)) {
              // EXISTING USER: CHECK CREDENTIALS
              try {
                final ServerSecurity.ServerUser user = security.authenticate(userName, userPassword);
                if (!user.databaseBlackList && !user.databases.contains(dbName)) {
                  // UPDATE DB LIST
                  user.databases.add(dbName);
                  try {
                    security.saveConfiguration();
                  } catch (IOException e) {
                    LogManager.instance().log(this, Level.SEVERE, "Cannot create database '%s' because security configuration cannot be saved", e, dbName);
                    continue;
                  }
                }

              } catch (ServerSecurityException e) {
                LogManager.instance()
                    .log(this, Level.WARNING, "Cannot create database '%s' because the user '%s' already exists with a different password", null, dbName,
                        userName);
                continue;
              }
            } else {
              // CREATE A NEW USER
              try {
                security.createUser(userName, userPassword, false, Collections.singletonList(dbName));

              } catch (IOException e) {
                LogManager.instance().log(this, Level.SEVERE, "Cannot create database '%s' because the new user '%s' cannot be saved", e, dbName, userName);
                continue;
              }
            }
          }
        }

        // CREATE THE DATABASE
        if (!existsDatabase(dbName)) {
          LogManager.instance().log(this, Level.INFO, "Creating default database '%s'...", null, dbName);
          createDatabase(dbName);
        }
      }
    }
  }

  private void loadConfiguration() {
    final File file = new File(CONFIG_SERVER_CONFIGURATION_FILENAME);
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

  private void saveConfiguration() {
    final File file = new File(CONFIG_SERVER_CONFIGURATION_FILENAME);
    try {
      FileUtils.writeFile(file, configuration.toJSON());
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving configuration to file '%s'", e, file);
    }
  }
}
