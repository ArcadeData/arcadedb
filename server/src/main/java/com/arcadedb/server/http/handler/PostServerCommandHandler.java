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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.backup.AutoBackupConfig;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.server.backup.BackupRetentionManager;
import com.arcadedb.server.backup.DatabaseBackupConfig;
import com.arcadedb.server.ha.ratis.RaftHAServer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.rmi.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;

public class PostServerCommandHandler extends AbstractServerHttpHandler {
  private static final String LIST_DATABASES       = "list databases";
  private static final String SHUTDOWN             = "shutdown";
  private static final String CREATE_DATABASE      = "create database";
  private static final String DROP_DATABASE        = "drop database";
  private static final String CLOSE_DATABASE       = "close database";
  private static final String OPEN_DATABASE        = "open database";
  private static final String CREATE_USER          = "create user";
  private static final String DROP_USER            = "drop user";
  private static final String CONNECT_CLUSTER      = "connect cluster";
  private static final String DISCONNECT_CLUSTER   = "disconnect cluster";
  private static final String SET_DATABASE_SETTING = "set database setting";
  private static final String SET_SERVER_SETTING   = "set server setting";
  private static final String GET_SERVER_EVENTS    = "get server events";
  private static final String ALIGN_DATABASE       = "align database";
  private static final String GET_BACKUP_CONFIG    = "get backup config";
  private static final String SET_BACKUP_CONFIG    = "set backup config";
  private static final String LIST_BACKUPS         = "list backups";
  private static final String TRIGGER_BACKUP       = "trigger backup";
  private static final String RESTORE_DATABASE     = "restore database";
  private static final String IMPORT_DATABASE      = "import database";
  private static final String PROFILER             = "profiler";
  private static final String HA_ADD_PEER          = "ha add peer";
  private static final String HA_REMOVE_PEER       = "ha remove peer";
  private static final String HA_TRANSFER_LEADER   = "ha transfer leader";
  private static final String HA_VERIFY_DATABASE   = "ha verify database";
  private static final String HA_STEP_DOWN        = "ha step down";
  private static final String HA_LEAVE            = "ha leave";

  public PostServerCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws IOException {

    final String command = payload.has("command") ? payload.getString("command").trim() : null;
    if (command == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Server command is null\"}");

    final JSONObject response = new JSONObject().put("result", "ok");

    final String command_lc = command.toLowerCase(Locale.ENGLISH).trim();

    if (command_lc.equals(LIST_DATABASES))
      return listDatabases(user);
    else
      checkRootUser(user);

    if (command_lc.startsWith(SHUTDOWN))
      shutdownServer(extractTarget(command, SHUTDOWN));
    else if (command_lc.startsWith(CREATE_DATABASE))
      createDatabase(extractTarget(command, CREATE_DATABASE));
    else if (command_lc.startsWith(DROP_DATABASE))
      dropDatabase(extractTarget(command, DROP_DATABASE));
    else if (command_lc.startsWith(CLOSE_DATABASE))
      closeDatabase(extractTarget(command, CLOSE_DATABASE));
    else if (command_lc.startsWith(OPEN_DATABASE))
      openDatabase(extractTarget(command, OPEN_DATABASE));
    else if (command_lc.startsWith(CREATE_USER))
      createUser(extractTarget(command, CREATE_USER));
    else if (command_lc.startsWith(DROP_USER))
      dropUser(extractTarget(command, DROP_USER));
    else if (command_lc.startsWith(CONNECT_CLUSTER)) {
      if (!connectCluster(extractTarget(command, CONNECT_CLUSTER), exchange))
        return null;
    } else if (command_lc.equals(DISCONNECT_CLUSTER))
      disconnectCluster();
    else if (command_lc.startsWith(SET_DATABASE_SETTING))
      setDatabaseSetting(extractTarget(command, SET_DATABASE_SETTING));
    else if (command_lc.startsWith(SET_SERVER_SETTING))
      setServerSetting(extractTarget(command, SET_SERVER_SETTING));
    else if (command_lc.startsWith(GET_SERVER_EVENTS))
      response.put("result", getServerEvents(extractTarget(command, GET_SERVER_EVENTS)));
    else if (command_lc.startsWith(ALIGN_DATABASE))
      alignDatabase(extractTarget(command, ALIGN_DATABASE));
    else if (command_lc.equals(GET_BACKUP_CONFIG))
      return getBackupConfig();
    else if (command_lc.equals(SET_BACKUP_CONFIG))
      return setBackupConfig(payload);
    else if (command_lc.startsWith(LIST_BACKUPS))
      return listBackups(extractTarget(command, LIST_BACKUPS));
    else if (command_lc.startsWith(TRIGGER_BACKUP))
      return triggerBackup(extractTarget(command, TRIGGER_BACKUP));
    else if (command_lc.startsWith(RESTORE_DATABASE))
      return restoreDatabase(extractTarget(command, RESTORE_DATABASE), exchange);
    else if (command_lc.startsWith(IMPORT_DATABASE))
      return importDatabase(extractTarget(command, IMPORT_DATABASE), exchange);
    else if (command_lc.startsWith(PROFILER))
      return handleProfilerCommand(extractTarget(command, PROFILER));
    else if (command_lc.startsWith(HA_ADD_PEER))
      return haAddPeer(extractTarget(command, HA_ADD_PEER), response);
    else if (command_lc.startsWith(HA_REMOVE_PEER))
      return haRemovePeer(extractTarget(command, HA_REMOVE_PEER), response);
    else if (command_lc.startsWith(HA_TRANSFER_LEADER))
      return haTransferLeader(extractTarget(command, HA_TRANSFER_LEADER), response);
    else if (command_lc.startsWith(HA_VERIFY_DATABASE))
      return haVerifyDatabase(extractTarget(command, HA_VERIFY_DATABASE), response);
    else if (command_lc.equals(HA_STEP_DOWN))
      return haStepDown(response);
    else if (command_lc.equals(HA_LEAVE))
      return haLeave(response);
    else {
      Metrics.counter("http.server-command.invalid").increment();

      return new ExecutionResponse(400, "{ \"error\" : \"Server command not valid\"}");
    }

    return new ExecutionResponse(200, response.toString());
  }

  private String extractTarget(String command, String keyword) {
    final int pos = command.toLowerCase().indexOf(keyword);
    if (pos == -1)
      return "";

    return command.substring(pos + keyword.length()).trim();
  }

  private ExecutionResponse listDatabases(final ServerSecurityUser user) {
    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.list-databases").increment();

    final Set<String> installedDatabases = new HashSet<>(server.getDatabaseNames());
    final Set<String> allowedDatabases = user.getAuthorizedDatabases();

    if (!allowedDatabases.contains("*"))
      installedDatabases.retainAll(allowedDatabases);

    final JSONObject response = new JSONObject().put("result", new JSONArray(installedDatabases));

    return new ExecutionResponse(200, response.toString());
  }

  private void shutdownServer(final String serverName) throws IOException {
    Metrics.counter("http.server-shutdown").increment();

    if (serverName.isEmpty()) {
      // SHUTDOWN CURRENT SERVER
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          httpServer.getServer().stop();
          System.exit(0);
        }
      }, 1000);
    } else
      throw new ServerException(
          "Remote server shutdown via HA is not supported. Use the HTTP API on server '" + serverName + "' directly");
  }

  private void createDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.create-database").increment();

    final ServerDatabase db = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);

    // In Ratis HA mode, new databases are replicated via the Raft state machine
  }

  /**
   * Restores a database from a backup URL. Format: {@code restore database <name> <url>}.
   * Supports SSE progress streaming when the client sends {@code Accept: text/event-stream}.
   */
  private ExecutionResponse restoreDatabase(final String args, final HttpServerExchange exchange) {
    final int space = args.indexOf(' ');
    if (space <= 0)
      throw new IllegalArgumentException("Usage: restore database <name> <url>");

    final String databaseName = args.substring(0, space).trim();
    final String url = args.substring(space + 1).trim();

    if (databaseName.isEmpty() || url.isEmpty())
      throw new IllegalArgumentException("Usage: restore database <name> <url>");

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.restore-database").increment();

    final String dbPath = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + databaseName;

    if (new File(dbPath).exists())
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    if (isSSERequested(exchange)) {
      startSSE(exchange);
      final OutputStream out = exchange.getOutputStream();

      try {
        final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
        final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(url, dbPath);

        // Set a logger with SSE callback for progress
        final Class<?> loggerClass = Class.forName("com.arcadedb.integration.importer.ConsoleLogger");
        final Class<?> listenerClass = Class.forName("com.arcadedb.integration.importer.ConsoleLogger$LogListener");
        final Object listener = java.lang.reflect.Proxy.newProxyInstance(
            listenerClass.getClassLoader(), new Class<?>[]{ listenerClass },
            (proxy, method, methodArgs) -> {
              if ("onLogLine".equals(method.getName()))
                sendSSE(out, new JSONObject().put("status", "progress").put("message", (String) methodArgs[0]));
              return null;
            });
        final Object logger = loggerClass.getConstructor(int.class, listenerClass).newInstance(2, listener);
        clazz.getMethod("setLogger", loggerClass).invoke(restorer, logger);

        sendSSE(out, new JSONObject().put("status", "progress").put("message", "Downloading and restoring " + databaseName + "..."));
        clazz.getMethod("restoreDatabase").invoke(restorer);
        server.getDatabase(databaseName);
        sendSSE(out, new JSONObject().put("status", "completed").put("message", databaseName + " restored successfully"));
      } catch (final Exception e) {
        final Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
        sendSSE(out, new JSONObject().put("status", "error").put("message", cause.getMessage()));
      } finally {
        closeSSE(out);
      }
      return null; // response already sent via SSE
    }

    // Synchronous fallback (no SSE)
    try {
      final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
      final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(url, dbPath);
      clazz.getMethod("restoreDatabase").invoke(restorer);
    } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException
                   | InstantiationException e) {
      throw new CommandExecutionException("Restore libs not found in classpath", e);
    } catch (final java.lang.reflect.InvocationTargetException e) {
      throw new CommandExecutionException("Error restoring database", e.getTargetException());
    }

    server.getDatabase(databaseName);
    return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());
  }

  /**
   * Creates and imports a database in one step. Format: {@code import database <name> <url>}.
   * Supports SSE progress streaming when the client sends {@code Accept: text/event-stream}.
   */
  private ExecutionResponse importDatabase(final String args, final HttpServerExchange exchange) {
    final int space = args.indexOf(' ');
    if (space <= 0)
      throw new IllegalArgumentException("Usage: import database <name> <url>");

    final String databaseName = args.substring(0, space).trim();
    final String url = args.substring(space + 1).trim();

    if (databaseName.isEmpty() || url.isEmpty())
      throw new IllegalArgumentException("Usage: import database <name> <url>");

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.import-database").increment();

    // Create the database
    server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    final Database database = server.getDatabase(databaseName);

    if (isSSERequested(exchange)) {
      startSSE(exchange);
      final OutputStream out = exchange.getOutputStream();

      try {
        final Class<?> clazz = Class.forName("com.arcadedb.integration.importer.Importer");
        final Object importer = clazz.getConstructor(Database.class, String.class).newInstance(database, url);

        // Set a logger with SSE callback
        final Class<?> loggerClass = Class.forName("com.arcadedb.integration.importer.ConsoleLogger");
        final Class<?> listenerClass = Class.forName("com.arcadedb.integration.importer.ConsoleLogger$LogListener");
        final Object listener = java.lang.reflect.Proxy.newProxyInstance(
            listenerClass.getClassLoader(), new Class<?>[]{ listenerClass },
            (proxy, method, methodArgs) -> {
              if ("onLogLine".equals(method.getName()))
                sendSSE(out, new JSONObject().put("status", "progress").put("message", (String) methodArgs[0]));
              return null;
            });
        final Object logger = loggerClass.getConstructor(int.class, listenerClass).newInstance(2, listener);
        clazz.getMethod("setLogger", loggerClass).invoke(importer, logger);

        sendSSE(out, new JSONObject().put("status", "progress").put("message", "Importing " + databaseName + "..."));

        // Start import in current thread (we're already on a worker thread)
        // Poll ImporterContext for structured progress every second in a separate thread
        final AtomicReference<Object> contextRef = new AtomicReference<>();
        try { contextRef.set(clazz.getMethod("getContext").invoke(importer)); } catch (final Exception ignored) {}

        final Timer progressTimer = new Timer(true);
        if (contextRef.get() != null) {
          progressTimer.schedule(new TimerTask() {
            @Override
            public void run() {
              try {
                final Object ctx = contextRef.get();
                final Class<?> ctxClass = ctx.getClass();
                final long vertices = ((AtomicLong) ctxClass.getField("createdVertices").get(ctx)).get();
                final long edges = ((AtomicLong) ctxClass.getField("createdEdges").get(ctx)).get();
                final long parsed = ((AtomicLong) ctxClass.getField("parsed").get(ctx)).get();
                if (parsed > 0)
                  sendSSE(out, new JSONObject().put("status", "progress")
                      .put("parsed", parsed).put("vertices", vertices).put("edges", edges));
              } catch (final Exception ignored) {}
            }
          }, 1000, 1000);
        }

        try {
          @SuppressWarnings("unchecked")
          final Map<String, Object> result = (Map<String, Object>) clazz.getMethod("load").invoke(importer);
          progressTimer.cancel();
          final JSONObject done = new JSONObject().put("status", "completed")
              .put("message", databaseName + " imported successfully");
          if (result != null)
            for (final Map.Entry<String, Object> e : result.entrySet())
              done.put(e.getKey(), e.getValue());
          sendSSE(out, done);
        } catch (final Exception e) {
          progressTimer.cancel();
          final Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
          sendSSE(out, new JSONObject().put("status", "error").put("message", cause.getMessage()));
        }
      } catch (final Exception e) {
        final Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ? e.getCause() : e;
        sendSSE(out, new JSONObject().put("status", "error").put("message", cause.getMessage()));
      } finally {
        closeSSE(out);
      }
      return null;
    }

    // Synchronous fallback
    database.command("sql", "import database " + url);
    return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());
  }

  // ═══════════════════════════════════════════════════════════════════
  //  SSE helpers
  // ═══════════════════════════════════════════════════════════════════

  private static boolean isSSERequested(final HttpServerExchange exchange) {
    final String accept = exchange.getRequestHeaders().getFirst("Accept");
    return accept != null && accept.contains("text/event-stream");
  }

  private static void startSSE(final HttpServerExchange exchange) {
    exchange.getResponseHeaders().put(new HttpString("Content-Type"), "text/event-stream");
    exchange.getResponseHeaders().put(new HttpString("Cache-Control"), "no-cache");
    exchange.getResponseHeaders().put(new HttpString("X-Accel-Buffering"), "no");
    exchange.setStatusCode(200);
    if (!exchange.isBlocking())
      exchange.startBlocking();
  }

  private static void sendSSE(final OutputStream out, final JSONObject data) {
    try {
      out.write(("data: " + data + "\n\n").getBytes(StandardCharsets.UTF_8));
      out.flush();
    } catch (final IOException ignored) {
      // Client disconnected
    }
  }

  private static void closeSSE(final OutputStream out) {
    try { out.close(); } catch (final IOException ignored) {}
  }

  private void dropDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final ServerDatabase database = httpServer.getServer().getDatabase(databaseName);

    Metrics.counter("http.drop-database").increment();

    database.getEmbedded().drop();
    httpServer.getServer().removeDatabase(database.getName());
  }

  private void closeDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final ServerDatabase database = httpServer.getServer().getDatabase(databaseName);
    database.getEmbedded().close();

    Metrics.counter("http.close-database").increment();

    httpServer.getServer().removeDatabase(database.getName());
  }

  private void openDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    httpServer.getServer().getDatabase(databaseName);
    Metrics.counter("http.open-database").increment();
  }

  private void createUser(final String payload) {
    final JSONObject json = new JSONObject(payload);

    if (!json.has("name"))
      throw new IllegalArgumentException("User name is null");

    final String userPassword = json.getString("password");
    if (userPassword.length() < 4)
      throw new ServerSecurityException("User password must be 5 minimum characters");
    if (userPassword.length() > 256)
      throw new ServerSecurityException("User password cannot be longer than 256 characters");

    json.put("password", httpServer.getServer().getSecurity().encodePassword(userPassword));

    Metrics.counter("http.create-user").increment();

    httpServer.getServer().getSecurity().createUser(json);
  }

  private void dropUser(final String userName) {
    if (userName.isEmpty())
      throw new IllegalArgumentException("User name was missing");

    Metrics.counter("http.drop-user").increment();

    final boolean result = httpServer.getServer().getSecurity().dropUser(userName);
    if (!result)
      throw new IllegalArgumentException("User '" + userName + "' not found on server");
  }

  private boolean connectCluster(final String serverAddress, final HttpServerExchange exchange) {
    Metrics.counter("http.connect-cluster").increment();
    // With Ratis, cluster membership is managed via 'ha add peer' / 'ha remove peer' commands
    throw new UnsupportedOperationException(
        "Use 'ha add peer <id> <address>' to manage cluster membership with Ratis HA");
  }

  private void disconnectCluster() {
    Metrics.counter("http.server-disconnect").increment();
    // With Ratis, the server can be removed via 'ha remove peer'
    throw new UnsupportedOperationException(
        "Use 'ha remove peer <id>' to manage cluster membership with Ratis HA");
  }

  private void setDatabaseSetting(final String triple) throws IOException {

    final String tripleTrimmed = triple.trim();
    final Integer firstSpace = tripleTrimmed.indexOf(" ");
    if (firstSpace == -1)
      throw new IllegalArgumentException("Expected <database> <key> <value>");

    final String pairTrimmed = tripleTrimmed.substring(firstSpace).trim();
    final Integer secondSpace = pairTrimmed.indexOf(" ");
    if (secondSpace == -1)
      throw new IllegalArgumentException("Expected <database> <key> <value>");

    final String db = tripleTrimmed.substring(0, firstSpace);
    final String key = pairTrimmed.substring(0, secondSpace);
    final String value = pairTrimmed.substring(secondSpace);

    final DatabaseInternal database = httpServer.getServer().getDatabase(db);
    database.getConfiguration().setValue(key, value);
    database.saveConfiguration();
  }

  private void setServerSetting(final String pair) {

    final String pairTrimmed = pair.trim();

    final Integer firstSpace = pairTrimmed.indexOf(" ");
    if (firstSpace == -1)
      throw new IllegalArgumentException("Expected <key> <value>");

    final String key = pairTrimmed.substring(0, firstSpace);
    final String value = pairTrimmed.substring(firstSpace);

    httpServer.getServer().getConfiguration().setValue(key, value);
  }

  private JSONObject getServerEvents(final String fileName) {
    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.get-server-events").increment();

    final JSONArray events = fileName.isEmpty() ?
        server.getEventLog().getCurrentEvents() :
        server.getEventLog().getEvents(fileName);
    final JSONArray files = server.getEventLog().getFiles();

    return new JSONObject().put("events", events).put("files", files);
  }

  private void alignDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    final Database database = httpServer.getServer().getDatabase(databaseName);

    Metrics.counter("http.align-database").increment();

    database.command("sql", "align database");
  }

  private ExecutionResponse getBackupConfig() {
    Metrics.counter("http.get-backup-config").increment();

    final ArcadeDBServer server = httpServer.getServer();
    final AutoBackupSchedulerPlugin plugin = getBackupPlugin(server);

    final JSONObject response = new JSONObject();

    if (plugin != null && plugin.isEnabled()) {
      response.put("enabled", true);
      final AutoBackupConfig config = plugin.getBackupConfig();
      response.put("config", config != null ? config.toJSON() : JSONObject.NULL);
    } else {
      // Plugin not enabled at startup - try to read config from file directly
      final Path configPath = Paths.get(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);
      if (Files.exists(configPath)) {
        try {
          final String content = Files.readString(configPath);
          final JSONObject configJson = new JSONObject(content);
          response.put("enabled", false); // Plugin not running, but config exists
          response.put("config", configJson);
          response.put("message", "Configuration saved but requires server restart to take effect");
        } catch (final IOException e) {
          response.put("enabled", false);
          response.put("config", JSONObject.NULL);
        }
      } else {
        response.put("enabled", false);
        response.put("config", JSONObject.NULL);
      }
    }

    return new ExecutionResponse(200, response.toString());
  }

  private ExecutionResponse setBackupConfig(final JSONObject payload) throws IOException {
    Metrics.counter("http.set-backup-config").increment();

    if (!payload.has("config"))
      throw new IllegalArgumentException("Missing 'config' in payload");

    final JSONObject configJson = payload.getJSONObject("config");

    // Validate backup directory - must be relative path without traversal
    if (configJson.has("backupDirectory")) {
      final String backupDir = configJson.getString("backupDirectory");
      validateBackupDirectory(backupDir);
    }

    final ArcadeDBServer server = httpServer.getServer();

    // Save configuration to file
    final Path configPath = Paths.get(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);

    // Ensure config directory exists
    final Path configDir = configPath.getParent();
    if (!Files.exists(configDir))
      Files.createDirectories(configDir);

    // Write configuration
    Files.writeString(configPath, configJson.toString(2));

    // Reload configuration in the plugin
    final AutoBackupSchedulerPlugin plugin = getBackupPlugin(server);
    if (plugin != null && plugin.isEnabled())
      plugin.reloadConfiguration();

    final JSONObject response = new JSONObject().put("result", "ok");
    return new ExecutionResponse(200, response.toString());
  }

  private void validateBackupDirectory(final String backupDir) {
    // Use consolidated validation from AutoBackupSchedulerPlugin
    final Path serverRoot = Paths.get(httpServer.getServer().getRootPath()).toAbsolutePath().normalize();
    AutoBackupSchedulerPlugin.validateAndResolveBackupPath(backupDir, serverRoot);
  }

  private ExecutionResponse listBackups(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    Metrics.counter("http.list-backups").increment();

    final ArcadeDBServer server = httpServer.getServer();
    final AutoBackupSchedulerPlugin plugin = getBackupPlugin(server);

    final JSONArray backups = new JSONArray();

    if (plugin != null && plugin.isEnabled()) {
      final AutoBackupConfig config = plugin.getBackupConfig();
      if (config != null) {
        // Resolve backup directory
        String backupDirectory = config.getBackupDirectory();
        final Path backupPath = Paths.get(backupDirectory);
        if (!backupPath.isAbsolute())
          backupDirectory = Paths.get(server.getRootPath(), backupDirectory).toString();

        final Path dbBackupDir = Paths.get(backupDirectory, databaseName);
        if (Files.exists(dbBackupDir) && Files.isDirectory(dbBackupDir)) {
          final Pattern pattern = Pattern.compile(".*-backup-(\\d{8})-(\\d{6})\\.zip$");
          final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

          try (var stream = Files.list(dbBackupDir)) {
            stream.filter(p -> p.toString().endsWith(".zip") && p.getFileName().toString().contains("-backup-"))
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                  final JSONObject backup = new JSONObject();
                  backup.put("fileName", p.getFileName().toString());
                  try {
                    backup.put("size", Files.size(p));
                    backup.put("lastModified", Files.getLastModifiedTime(p).toMillis());
                  } catch (final IOException e) {
                    backup.put("size", 0);
                    backup.put("lastModified", 0);
                  }

                  // Parse timestamp from filename
                  final Matcher matcher = pattern.matcher(p.getFileName().toString());
                  if (matcher.matches()) {
                    try {
                      final String timestampStr = matcher.group(1) + "-" + matcher.group(2);
                      final LocalDateTime timestamp = LocalDateTime.parse(timestampStr, formatter);
                      backup.put("timestamp", timestamp.toString());
                    } catch (final Exception e) {
                      backup.put("timestamp", JSONObject.NULL);
                    }
                  }

                  backups.put(backup);
                });
          } catch (final IOException e) {
            throw new RuntimeException("Error listing backups for database '" + databaseName + "'", e);
          }
        }
      }
    }

    final JSONObject response = new JSONObject();
    response.put("database", databaseName);
    response.put("backups", backups);

    // Get retention manager stats if available
    if (plugin != null && plugin.getRetentionManager() != null) {
      final BackupRetentionManager retentionManager = plugin.getRetentionManager();
      response.put("totalSize", retentionManager.getBackupSizeBytes(databaseName));
      response.put("totalCount", retentionManager.getBackupCount(databaseName));
    }

    return new ExecutionResponse(200, response.toString());
  }

  private ExecutionResponse triggerBackup(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    Metrics.counter("http.trigger-backup").increment();

    final ArcadeDBServer server = httpServer.getServer();
    final AutoBackupSchedulerPlugin plugin = getBackupPlugin(server);

    // Try to get backup directory from config (plugin or file)
    String backupDirectory = null;

    if (plugin != null && plugin.isEnabled()) {
      final AutoBackupConfig config = plugin.getBackupConfig();
      backupDirectory = config != null ? config.getBackupDirectory() : null;
    }

    // If plugin not enabled, try to read from config file directly
    if (backupDirectory == null) {
      final Path configPath = Paths.get(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);
      if (Files.exists(configPath)) {
        try {
          final String content = Files.readString(configPath);
          final JSONObject configJson = new JSONObject(content);
          if (configJson.has("backupDirectory"))
            backupDirectory = configJson.getString("backupDirectory");
        } catch (final IOException ignored) {
        }
      }
    }

    // Use config directory if available
    if (backupDirectory != null) {
      try {
        // Validate the directory
        validateBackupDirectory(backupDirectory);

        // Resolve relative path
        final Path backupPath = Paths.get(backupDirectory);
        if (!backupPath.isAbsolute())
          backupDirectory = Paths.get(server.getRootPath(), backupDirectory).toString();

        // Perform backup using reflection (same as BackupTask)
        final Database database = server.getDatabase(databaseName);
        final Class<?> clazz = Class.forName("com.arcadedb.integration.backup.Backup");

        final String timestamp = LocalDateTime.now()
            .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        final String backupFileName = databaseName + "-backup-" + timestamp + ".zip";

        final Path dbBackupPath = Paths.get(backupDirectory, databaseName);
        // Use Files.createDirectories to avoid TOCTOU race condition
        Files.createDirectories(dbBackupPath);
        final String dbBackupDir = dbBackupPath.toString();

        final Object backup = clazz.getConstructor(Database.class, String.class)
            .newInstance(database, backupFileName);
        clazz.getMethod("setDirectory", String.class).invoke(backup, dbBackupDir);
        clazz.getMethod("setVerboseLevel", Integer.TYPE).invoke(backup, 1);

        final String backupFile = (String) clazz.getMethod("backupDatabase").invoke(backup);

        final JSONObject response = new JSONObject();
        response.put("result", "ok");
        response.put("backupFile", backupFile);
        return new ExecutionResponse(200, response.toString());

      } catch (final ClassNotFoundException e) {
        throw new RuntimeException("Backup libs not found in classpath. Make sure arcadedb-integration module is included.", e);
      } catch (final Exception e) {
        final Throwable cause = e.getCause() != null ? e.getCause() : e;
        throw new RuntimeException("Error triggering backup for database '" + databaseName + "': " + cause.getMessage(), cause);
      }
    }

    // Fallback: use SQL command (uses GlobalConfiguration.SERVER_BACKUP_DIRECTORY)
    try {
      final Database database = server.getDatabase(databaseName);
      final Object result = database.command("sql", "backup database");

      final JSONObject response = new JSONObject();
      response.put("result", "ok");
      if (result instanceof Iterable) {
        for (final Object r : (Iterable<?>) result) {
          if (r instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) r;
            if (map.containsKey("backupFile"))
              response.put("backupFile", map.get("backupFile").toString());
          }
        }
      }
      return new ExecutionResponse(200, response.toString());
    } catch (final Exception e) {
      throw new RuntimeException("Error triggering backup for database '" + databaseName + "': " + e.getMessage(), e);
    }
  }

  private ExecutionResponse handleProfilerCommand(final String subCommand) {
    final ServerQueryProfiler profiler = httpServer.getServer().getQueryProfiler();
    final String sub = subCommand.toLowerCase(Locale.ENGLISH).trim();

    if (sub.equals("start") || sub.startsWith("start ")) {
      final String timeoutStr = sub.substring(5).trim();
      if (!timeoutStr.isEmpty()) {
        try {
          profiler.start(Integer.parseInt(timeoutStr));
        } catch (final NumberFormatException e) {
          return new ExecutionResponse(400, "{ \"error\" : \"Invalid timeout value: " + timeoutStr + "\"}");
        }
      } else
        profiler.start();
      return new ExecutionResponse(200, new JSONObject().put("result", "ok").put("recording", true).toString());

    } else if (sub.equals("stop")) {
      final JSONObject results = profiler.stop();
      return new ExecutionResponse(200, results != null ? results.toString() : new JSONObject().put("result", "ok").toString());

    } else if (sub.equals("reset")) {
      profiler.reset();
      return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());

    } else if (sub.equals("results")) {
      final JSONObject results = profiler.getResults();
      return new ExecutionResponse(200, results != null ? results.toString() : new JSONObject().put("result", "ok").toString());

    } else if (sub.equals("list")) {
      final JSONArray files = profiler.listSavedRuns();
      return new ExecutionResponse(200, new JSONObject().put("result", files).toString());

    } else if (sub.startsWith("load ")) {
      final String fileName = sub.substring(5).trim();
      final JSONObject run = profiler.loadSavedRun(fileName);
      return new ExecutionResponse(200, run.toString());

    } else {
      return new ExecutionResponse(400, "{ \"error\" : \"Unknown profiler command: " + subCommand + "\"}");
    }
  }

  private AutoBackupSchedulerPlugin getBackupPlugin(final ArcadeDBServer server) {
    for (final ServerPlugin plugin : server.getPlugins()) {
      if (plugin instanceof AutoBackupSchedulerPlugin)
        return (AutoBackupSchedulerPlugin) plugin;
    }
    return null;
  }

  private ExecutionResponse haAddPeer(final String target, final JSONObject response) {
    final var raftHA = httpServer.getServer().getRaftHA();
    if (raftHA == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Ratis HA is not enabled\"}");

    // Format: "peerId address" e.g. "localhost:2426 localhost:2426"
    final String[] parts = target.split("\\s+");
    if (parts.length < 2)
      return new ExecutionResponse(400, "{ \"error\" : \"Usage: ha add peer <peerId> <address>\"}");

    raftHA.addPeer(parts[0], parts[1]);
    response.put("result", "Peer " + parts[0] + " added");
    return new ExecutionResponse(200, response.toString());
  }

  private ExecutionResponse haRemovePeer(final String target, final JSONObject response) {
    final var raftHA = httpServer.getServer().getRaftHA();
    if (raftHA == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Ratis HA is not enabled\"}");

    if (target.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Usage: ha remove peer <peerId>\"}");

    raftHA.removePeer(target.trim());
    response.put("result", "Peer " + target.trim() + " removed");
    return new ExecutionResponse(200, response.toString());
  }

  private ExecutionResponse haTransferLeader(final String target, final JSONObject response) {
    final var raftHA = httpServer.getServer().getRaftHA();
    if (raftHA == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Ratis HA is not enabled\"}");

    if (target.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Usage: ha transfer leader <peerId>\"}");

    raftHA.transferLeadership(target.trim(), 30_000);
    response.put("result", "Leadership transferred to " + target.trim());
    return new ExecutionResponse(200, response.toString());
  }

  private ExecutionResponse haStepDown(final JSONObject response) {
    final var raftHA = httpServer.getServer().getRaftHA();
    if (raftHA == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Ratis HA is not enabled\"}");

    // Find the current leader and transfer to a random non-leader peer.
    // Works from any server - Ratis client auto-routes to the leader.
    final String leaderName = raftHA.getLeaderName();
    for (final var peer : raftHA.getRaftGroup().getPeers()) {
      if (!peer.getId().toString().equals(leaderName)) {
        raftHA.transferLeadership(peer.getId().toString(), 30_000);
        response.put("result", "Leadership transfer initiated from " + leaderName + " to " + peer.getId());
        return new ExecutionResponse(200, response.toString());
      }
    }
    return new ExecutionResponse(400, "{ \"error\" : \"No other peer available for leadership transfer\"}");
  }

  private ExecutionResponse haLeave(final JSONObject response) {
    final var raftHA = httpServer.getServer().getRaftHA();
    if (raftHA == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Ratis HA is not enabled\"}");

    raftHA.leaveCluster();
    response.put("result", "Server " + raftHA.getLocalPeerId() + " leaving cluster");
    return new ExecutionResponse(200, response.toString());
  }

  /**
   * Verifies that all nodes in the Ratis cluster have identical database files by comparing CRC checksums.
   * This is a diagnostic command - with Ratis, alignment is automatic, but this confirms it.
   * <p>
   * Usage: {@code ha verify database <name>}
   * <p>
   * Returns per-peer checksum comparison: which files match and which differ.
   */
  private ExecutionResponse haVerifyDatabase(final String databaseName, final JSONObject response) {
    final var raftHA = httpServer.getServer().getRaftHA();
    if (raftHA == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Ratis HA is not enabled\"}");

    if (databaseName.trim().isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Usage: ha verify database <name>\"}");

    final var server = httpServer.getServer();
    if (!server.existsDatabase(databaseName.trim()))
      return new ExecutionResponse(404, "{ \"error\" : \"Database '" + databaseName.trim() + "' not found\"}");

    Metrics.counter("http.ha-verify-database").increment();

    final var db = (com.arcadedb.database.DatabaseInternal) server.getDatabase(databaseName.trim());

    // Compute local checksums with file type categorization
    final JSONObject localChecksums = new JSONObject();
    final JSONArray localFiles = new JSONArray();
    db.executeInReadLock(() -> {
      db.getPageManager().suspendFlushAndExecute(db, () -> {
        for (final var file : db.getFileManager().getFiles())
          if (file != null) {
            final String name = file.getFileName();
            final long crc = file.calculateChecksum();
            localChecksums.put(name, crc);

            final JSONObject fileInfo = new JSONObject();
            fileInfo.put("name", name);
            fileInfo.put("checksum", crc);
            fileInfo.put("size", file.getSize());
            fileInfo.put("type", categorizeFile(name));
            localFiles.put(fileInfo);
          }
      });
      return null;
    });

    // Non-leader: return local checksums only. The leader calls this internally for comparison.
    // When called from Studio on a follower, the JS handles the UX (shows checksums or redirects).
    if (!raftHA.isLeader()) {
      response.put("localChecksums", localChecksums);
      response.put("files", localFiles);
      response.put("localServer", server.getServerName());
      return new ExecutionResponse(200, response.toString());
    }

    final JSONObject result = new JSONObject();
    result.put("database", databaseName.trim());
    result.put("files", localFiles);
    result.put("localServer", server.getServerName());
    result.put("localPeerId", raftHA.getLocalPeerId().toString());
    result.put("localChecksums", localChecksums);

    // Query each remote peer for their checksums via HTTP
    final JSONArray peerResults = new JSONArray();
    for (final var peer : raftHA.getRaftGroup().getPeers()) {
      if (peer.getId().equals(raftHA.getLocalPeerId()))
        continue;

      final JSONObject peerResult = new JSONObject();
      peerResult.put("peerId", peer.getId().toString());
      peerResult.put("httpAddress", raftHA.getPeerHTTPAddress(peer.getId()));

      try {
        final String peerHttpAddr = raftHA.getPeerHTTPAddress(peer.getId());
        final String url = "http://" + peerHttpAddr + "/api/v1/server";

        final var conn = (java.net.HttpURLConnection) new java.net.URI(url).toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");

        // Forward auth from the current request
        final var authHeader = httpServer.getServer().getSecurity() != null ? "root" : null;
        conn.setRequestProperty("Authorization",
            httpServer.getServer().getConfiguration().getValueAsString(
                com.arcadedb.GlobalConfiguration.SERVER_ROOT_PASSWORD) != null ?
                "Basic " + java.util.Base64.getEncoder().encodeToString(
                    ("root:" + com.arcadedb.GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString()).getBytes()) : "");

        conn.setDoOutput(true);
        try (final var os = conn.getOutputStream()) {
          os.write(("{\"command\":\"ha verify database " + databaseName.trim() + "\"}").getBytes());
        }

        if (conn.getResponseCode() == 200) {
          final String body = new String(conn.getInputStream().readAllBytes());
          final JSONObject peerResponse = new JSONObject(body);

          if (peerResponse.has("localChecksums")) {
            final JSONObject remoteChecksums = peerResponse.getJSONObject("localChecksums");

            // Compare checksums
            int matchCount = 0;
            int mismatchCount = 0;
            final JSONArray mismatches = new JSONArray();

            for (final String fileName : localChecksums.keySet()) {
              final long localCrc = localChecksums.getLong(fileName);
              if (remoteChecksums.has(fileName)) {
                final long remoteCrc = remoteChecksums.getLong(fileName);
                if (localCrc == remoteCrc)
                  matchCount++;
                else {
                  mismatchCount++;
                  mismatches.put(new JSONObject()
                      .put("file", fileName)
                      .put("type", categorizeFile(fileName))
                      .put("localChecksum", localCrc)
                      .put("remoteChecksum", remoteCrc));
                }
              } else {
                mismatchCount++;
                mismatches.put(new JSONObject()
                    .put("file", fileName)
                    .put("type", categorizeFile(fileName))
                    .put("localChecksum", localCrc)
                    .put("remoteChecksum", "MISSING"));
              }
            }

            peerResult.put("status", mismatchCount == 0 ? "CONSISTENT" : "INCONSISTENT");
            peerResult.put("matchingFiles", matchCount);
            peerResult.put("mismatchedFiles", mismatchCount);
            if (mismatchCount > 0)
              peerResult.put("mismatches", mismatches);
          }
        } else {
          peerResult.put("status", "ERROR");
          peerResult.put("error", "HTTP " + conn.getResponseCode());
        }
        conn.disconnect();

      } catch (final Exception e) {
        peerResult.put("status", "ERROR");
        peerResult.put("error", e.getMessage());
      }

      peerResults.put(peerResult);
    }

    result.put("peers", peerResults);

    // Overall status
    boolean allConsistent = true;
    for (int i = 0; i < peerResults.length(); i++)
      if (!"CONSISTENT".equals(peerResults.getJSONObject(i).getString("status")))
        allConsistent = false;

    result.put("overallStatus", allConsistent ? "ALL_CONSISTENT" : "INCONSISTENCY_DETECTED");
    response.put("result", result);
    return new ExecutionResponse(200, response.toString());
  }

  private void checkServerIsLeaderIfInHA() {
    final RaftHAServer ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderHTTPAddress());
  }

  private static String categorizeFile(final String fileName) {
    if (fileName == null) return "unknown";
    final String lower = fileName.toLowerCase();
    if (lower.endsWith(".json") || lower.equals("configuration") || lower.contains("schema"))
      return "config";
    if (lower.contains("index") || lower.contains(".idx") || lower.contains(".ridx") || lower.contains(".notunique")
        || lower.contains(".unique") || lower.contains(".dictionary"))
      return "index";
    if (lower.contains("bucket") || lower.contains(".pcf"))
      return "bucket";
    return "data";
  }

  private RaftHAServer getHA() {
    final RaftHAServer ha = httpServer.getServer().getHA();
    if (ha == null)
      throw new CommandExecutionException(
          "ArcadeDB is not running with High Availability module enabled. Please add this setting at startup: -Darcadedb.ha.enabled=true");
    return ha;
  }
}
