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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.*;
import com.arcadedb.server.backup.AutoBackupConfig;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.server.backup.BackupRetentionManager;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ServerSecurityException;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;

import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostServerCommandHandler extends AbstractServerHttpHandler {
  private static final HttpClient HTTP_CLIENT           = HttpClient.newHttpClient();
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
  private static final String RESTORE_BACKUP       = "restore backup";
  private static final String DELETE_BACKUP        = "delete backup";
  private static final String RESTORE_DATABASE     = "restore database";
  private static final String IMPORT_DATABASE      = "import database";
  private static final String PROFILER             = "profiler";

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

    if (LIST_DATABASES.equals(command_lc))
      return listDatabases(user);
    else
      checkRootUser(user);

    // Write commands that must run on the leader: forward if this node is a replica
    if (command_lc.startsWith(CREATE_DATABASE) || command_lc.startsWith(DROP_DATABASE) ||
        command_lc.startsWith(CREATE_USER) || command_lc.startsWith(DROP_USER) ||
        command_lc.startsWith(RESTORE_BACKUP) || command_lc.startsWith(RESTORE_DATABASE) ||
        command_lc.startsWith(IMPORT_DATABASE)) {
      final ExecutionResponse forwarded = forwardToLeaderIfReplica(exchange, payload, user);
      if (forwarded != null)
        return forwarded;
    }

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
    } else if (DISCONNECT_CLUSTER.equals(command_lc))
      disconnectCluster();
    else if (command_lc.startsWith(SET_DATABASE_SETTING))
      setDatabaseSetting(extractTarget(command, SET_DATABASE_SETTING));
    else if (command_lc.startsWith(SET_SERVER_SETTING))
      setServerSetting(extractTarget(command, SET_SERVER_SETTING));
    else if (command_lc.startsWith(GET_SERVER_EVENTS))
      response.put("result", getServerEvents(extractTarget(command, GET_SERVER_EVENTS)));
    else if (command_lc.startsWith(ALIGN_DATABASE))
      alignDatabase(extractTarget(command, ALIGN_DATABASE));
    else if (GET_BACKUP_CONFIG.equals(command_lc))
      return getBackupConfig();
    else if (SET_BACKUP_CONFIG.equals(command_lc))
      return setBackupConfig(payload);
    else if (command_lc.startsWith(LIST_BACKUPS))
      return listBackups(extractTarget(command, LIST_BACKUPS));
    else if (command_lc.startsWith(TRIGGER_BACKUP))
      return triggerBackup(extractTarget(command, TRIGGER_BACKUP));
    else if (command_lc.startsWith(RESTORE_BACKUP))
      return restoreBackup(extractTarget(command, RESTORE_BACKUP), payload, exchange);
    else if (command_lc.startsWith(DELETE_BACKUP))
      return deleteBackup(extractTarget(command, DELETE_BACKUP));
    else if (command_lc.startsWith(RESTORE_DATABASE))
      return restoreDatabase(extractTarget(command, RESTORE_DATABASE), exchange);
    else if (command_lc.startsWith(IMPORT_DATABASE))
      return importDatabase(extractTarget(command, IMPORT_DATABASE), exchange);
    else if (command_lc.startsWith(PROFILER))
      return handleProfilerCommand(extractTarget(command, PROFILER));
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
    } else {
      getHA().shutdownRemoteServer(serverName);
    }
  }

  private void createDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.create-database").increment();

    final ServerDatabase db = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);

    final DatabaseInternal wrappedDb = db.getWrappedDatabaseInstance();
    if (wrappedDb instanceof HAReplicatedDatabase haDb)
      haDb.createInReplicas();
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

    validateClientRestoreImportUrl(url);

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.restore-database").increment();

    final String dbPath = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + databaseName;

    if (new File(dbPath).exists())
      throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

    return performRestore(databaseName, dbPath, url, exchange);
  }

  /**
   * Restores a previously created backup file into a database. The backup file is resolved
   * server-side from the configured auto-backup directory, so the client never supplies a
   * filesystem path. Format: {@code restore backup <database> <fileName> as <targetDatabase>}.
   * The optional payload flag {@code "overwrite": true} drops the target database first if it
   * already exists; without it the command fails when the target database exists.
   */
  private ExecutionResponse restoreBackup(final String args, final JSONObject payload, final HttpServerExchange exchange) {
    final int asIdx = args.toLowerCase(Locale.ENGLISH).lastIndexOf(" as ");
    if (asIdx <= 0)
      throw new IllegalArgumentException("Usage: restore backup <database> <fileName> as <targetDatabase>");

    final String head = args.substring(0, asIdx).trim();
    final String targetDatabase = args.substring(asIdx + 4).trim();
    final int space = head.indexOf(' ');
    if (space <= 0 || targetDatabase.isEmpty())
      throw new IllegalArgumentException("Usage: restore backup <database> <fileName> as <targetDatabase>");

    final String databaseName = head.substring(0, space).trim();
    final String fileName = head.substring(space + 1).trim();
    if (databaseName.isEmpty() || fileName.isEmpty())
      throw new IllegalArgumentException("Usage: restore backup <database> <fileName> as <targetDatabase>");

    final boolean overwrite = payload.getBoolean("overwrite", false);

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.restore-backup").increment();

    final Path backupFile = resolveBackupFile(server, databaseName, fileName);

    final String dbPath = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY)
        + File.separator + targetDatabase;

    final boolean exists = server.existsDatabase(targetDatabase) || new File(dbPath).exists();
    if (exists && !overwrite)
      throw new IllegalArgumentException(
          "Database '" + targetDatabase + "' already exists. Enable overwrite to replace it with the backup");

    // Note: any existing target is dropped by performRestore only AFTER the restore into a temporary
    // directory succeeds, so a failed restore leaves the original database intact (issue #5027).
    // The backup file is resolved server-side, so this internal file:// URL bypasses the SSRF guard.
    final String url = "file://" + backupFile.toAbsolutePath();
    return performRestore(targetDatabase, dbPath, url, exchange);
  }

  /**
   * Deletes a single backup file from the configured auto-backup directory. The file name is
   * validated and resolved server-side to prevent path traversal. Format:
   * {@code delete backup <database> <fileName>}.
   */
  private ExecutionResponse deleteBackup(final String args) {
    final int space = args.indexOf(' ');
    if (space <= 0)
      throw new IllegalArgumentException("Usage: delete backup <database> <fileName>");

    final String databaseName = args.substring(0, space).trim();
    final String fileName = args.substring(space + 1).trim();
    if (databaseName.isEmpty() || fileName.isEmpty())
      throw new IllegalArgumentException("Usage: delete backup <database> <fileName>");

    Metrics.counter("http.delete-backup").increment();

    final ArcadeDBServer server = httpServer.getServer();
    final Path backupFile = resolveBackupFile(server, databaseName, fileName);

    try {
      Files.delete(backupFile);
    } catch (final IOException e) {
      throw new CommandExecutionException("Error deleting backup file '" + fileName + "'", e);
    }

    return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());
  }

  /**
   * Resolves a backup file name to an absolute path inside the configured auto-backup directory for
   * the given database, rejecting any name that contains path separators or traversal sequences.
   */
  private Path resolveBackupFile(final ArcadeDBServer server, final String databaseName, final String fileName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    // Reject anything that is not a plain backup file name.
    if (fileName.contains("/") || fileName.contains("\\") || fileName.contains("..") || fileName.isBlank()
        || !fileName.endsWith(".zip") || !fileName.contains("-backup-"))
      throw new IllegalArgumentException("Invalid backup file name: " + fileName);

    final AutoBackupSchedulerPlugin plugin = getBackupPlugin(server);
    if (plugin == null || !plugin.isEnabled() || plugin.getBackupConfig() == null)
      throw new IllegalArgumentException("Auto-backup is not configured");

    String backupDirectory = plugin.getBackupConfig().getBackupDirectory();
    final Path backupPath = Path.of(backupDirectory);
    if (!backupPath.isAbsolute())
      backupDirectory = Path.of(server.getRootPath(), backupDirectory).toString();

    final Path dbBackupDir = Path.of(backupDirectory, databaseName).normalize();
    final Path resolved = dbBackupDir.resolve(fileName).normalize();

    // Defence in depth: the resolved file must still live inside the database backup directory.
    if (!resolved.startsWith(dbBackupDir))
      throw new IllegalArgumentException("Invalid backup file path");

    if (!Files.exists(resolved) || !Files.isRegularFile(resolved))
      throw new IllegalArgumentException("Backup file not found: " + fileName);

    return resolved;
  }

  /**
   * Shared restore execution used by {@code restore database} and {@code restore backup}. Performs
   * the actual restore from {@code url} into {@code dbPath}, streaming progress via SSE when
   * requested and replicating the restored database in HA mode. The caller is responsible for any
   * pre-restore existence/overwrite checks.
   */
  private ExecutionResponse performRestore(final String databaseName, final String dbPath, final String url,
      final HttpServerExchange exchange) {
    final ArcadeDBServer server = httpServer.getServer();

    // Restore into a temporary sibling directory first, then atomically swap it into place only on
    // success. This keeps an existing target database intact when the restore fails (issue #5027).
    // The temp directory name is prefixed with the reserved-database marker ('.') so that, if the
    // process dies mid-restore, loadDatabases() skips the orphan at next startup instead of trying
    // to open it as a user database.
    final File finalDir = new File(dbPath);
    final File tempDir = new File(finalDir.getParentFile(),
        ArcadeDBServer.RESERVED_DATABASE_PREFIX + "restore-tmp-" + databaseName + "-" + System.nanoTime());

    if (isSSERequested(exchange)) {
      startSSE(exchange);
      final OutputStream out = exchange.getOutputStream();

      try {
        final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
        final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(url, tempDir.getAbsolutePath());

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
        swapRestoredDatabase(server, databaseName, finalDir, tempDir);
        final ServerDatabase restoredSse = server.getDatabase(databaseName);
        replicateRestoredDatabase(server, restoredSse, databaseName);
        sendSSE(out, new JSONObject().put("status", "completed").put("message", databaseName + " restored successfully"));
      } catch (final Exception e) {
        FileUtils.deleteRecursively(tempDir);
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
      final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(url, tempDir.getAbsolutePath());
      clazz.getMethod("restoreDatabase").invoke(restorer);
    } catch (final ClassNotFoundException | NoSuchMethodException | IllegalAccessException
                   | InstantiationException e) {
      FileUtils.deleteRecursively(tempDir);
      throw new CommandExecutionException("Restore libs not found in classpath", e);
    } catch (final java.lang.reflect.InvocationTargetException e) {
      FileUtils.deleteRecursively(tempDir);
      throw new CommandExecutionException("Error restoring database", e.getTargetException());
    }

    swapRestoredDatabase(server, databaseName, finalDir, tempDir);
    final ServerDatabase restored = server.getDatabase(databaseName);
    replicateRestoredDatabase(server, restored, databaseName);
    return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());
  }

  /**
   * Swaps a freshly-restored temporary directory into the final database directory. The existing
   * target database (if any) is dropped only now that the restore into {@code tempDir} has
   * succeeded, so a failed restore never destroys the original data (issue #5027).
   */
  private void swapRestoredDatabase(final ArcadeDBServer server, final String databaseName, final File finalDir,
      final File tempDir) {
    try {
      // Drop the previous target (HA-aware) BEFORE taking the registry lock and only after a
      // successful restore into tempDir. In HA mode dropDatabase() round-trips through Raft and the
      // apply thread itself acquires databasesLock, so holding that lock here would deadlock; only the
      // pure-local file swap below runs under the lock, matching the snapshot-installer pattern (#4832).
      if (server.existsDatabase(databaseName))
        dropDatabase(databaseName);

      // Serialise the on-disk swap against concurrent getDatabase / createDatabase so no concurrent
      // open observes the transient half-swapped directory.
      synchronized (server.getDatabasesLock()) {
        if (finalDir.exists())
          FileUtils.deleteRecursively(finalDir);

        try {
          Files.move(tempDir.toPath(), finalDir.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (final AtomicMoveNotSupportedException e) {
          Files.move(tempDir.toPath(), finalDir.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      }
    } catch (final CommandExecutionException e) {
      FileUtils.deleteRecursively(tempDir);
      throw e;
    } catch (final Exception e) {
      FileUtils.deleteRecursively(tempDir);
      throw new CommandExecutionException("Error activating restored database '" + databaseName + "'", e);
    }
  }

  /**
   * Validates a client-supplied restore/import URL to prevent SSRF and local-file reads. Unless the
   * operator enables {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS}, only
   * {@code http}/{@code https} URLs to non-private hosts are accepted; {@code file://} and any
   * private, loopback, link-local, site-local, multicast, wildcard or unresolvable host is rejected.
   */
  private void validateClientRestoreImportUrl(final String url) {
    if (httpServer.getServer().getConfiguration().getValueAsBoolean(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS))
      return;

    final URI uri;
    try {
      uri = URI.create(url.trim());
    } catch (final IllegalArgumentException e) {
      throw new SecurityException("Invalid restore/import URL");
    }

    final String scheme = uri.getScheme() == null ? null : uri.getScheme().toLowerCase(Locale.ENGLISH);
    if (scheme == null)
      throw new SecurityException("Restore/import URL must use the 'http' or 'https' scheme");

    if (!"http".equals(scheme) && !"https".equals(scheme))
      throw new SecurityException("Restore/import URL scheme '" + scheme + "' is not allowed. Enable '"
          + GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getKey()
          + "' to permit local-file and non-HTTP URLs");

    final String host = uri.getHost();
    if (host == null || host.isBlank())
      throw new SecurityException("Restore/import URL host is missing");

    if (isBlockedHost(host))
      throw new SecurityException("Restore/import from private, loopback or link-local hosts is blocked. Enable '"
          + GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getKey() + "' to override");
  }

  /**
   * Returns true when {@code host} resolves to (or is) an address in a range that must not be reached
   * from a client-supplied restore/import URL. Every resolved address is checked so a hostname that
   * resolves to a mix of public and private addresses is still rejected. An unresolvable host is
   * treated as blocked.
   * <p>
   * The blocked-range logic mirrors {@code ImportSecurityValidator.isBlockedAddress} in the
   * (test-scoped, reflectively-loaded) integration module; keep the two in sync when adding ranges.
   */
  private static boolean isBlockedHost(final String host) {
    try {
      for (final InetAddress addr : InetAddress.getAllByName(host)) {
        if (addr.isLoopbackAddress() || addr.isAnyLocalAddress() || addr.isLinkLocalAddress()
            || addr.isSiteLocalAddress() || addr.isMulticastAddress())
          return true;

        // InetAddress flags miss two modern private ranges, so check the raw bytes explicitly.
        final byte[] bytes = addr.getAddress();
        // IPv6 Unique Local Addresses (ULA) fc00::/7 (RFC 4193).
        if (bytes.length == 16 && (bytes[0] & 0xfe) == 0xfc)
          return true;
        // IPv4 Carrier-Grade NAT (CGNAT) 100.64.0.0/10 (RFC 6598).
        if (bytes.length == 4 && (bytes[0] & 0xff) == 100 && (bytes[1] & 0xc0) == 64)
          return true;
      }
      return false;
    } catch (final UnknownHostException e) {
      return true;
    }
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

    // Validate BEFORE creating the database so a rejected URL leaves no empty database behind.
    validateClientRestoreImportUrl(url);

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.import-database").increment();

    // Create the database cluster-wide. In HA mode this submits an INSTALL_DATABASE_ENTRY
    // via Raft so every replica creates the database locally before we start importing.
    // The importer's subsequent transactions then replicate as normal TX_ENTRY stream.
    final ServerDatabase createdDb = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
    final DatabaseInternal wrapped = createdDb.getWrappedDatabaseInstance();
    if (wrapped instanceof HAReplicatedDatabase haDb) {
      try {
        haDb.createInReplicas();
      } catch (final RuntimeException e) {
        // Compensate: drop the just-created local database so the operator can retry cleanly.
        try {
          createdDb.getEmbedded().drop();
          server.removeDatabase(databaseName);
        } catch (final Exception ignored) {
          // best-effort
        }
        throw e;
      }
    }
    final Database database = createdDb;

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
    try (final var rs = database.command("sql", "import database " + url)) {
      // try-with-resources releases the execution-plan state held by the import ResultSet.
    }
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

    checkServerIsLeaderIfInHA();

    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.drop-database").increment();

    if (!server.existsDatabase(databaseName))
      throw new IllegalArgumentException("Database '" + databaseName + "' does not exist");

    final ServerDatabase database = server.getDatabase(databaseName);
    final DatabaseInternal wrappedDb = database.getWrappedDatabaseInstance();

    if (wrappedDb instanceof HAReplicatedDatabase haDb) {
      // Raft-first: do NOT drop locally. The state machine apply on every peer
      // (including this leader) performs the actual drop once the entry is committed.
      haDb.dropInReplicas();
    } else {
      // Non-HA mode: drop locally as before.
      database.getEmbedded().drop();
      server.removeDatabase(databaseName);
    }
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

    final ArcadeDBServer server = httpServer.getServer();
    // Enforce the single shared credentials policy (min length 8, correct message) used by the REST
    // create-user path, instead of a divergent off-by-one length check.
    server.getSecurity().getCredentialsValidator().validateCredentials(json.getString("name"), userPassword);

    json.put("password", server.getSecurity().encodePassword(userPassword));

    Metrics.counter("http.create-user").increment();

    final HAServerPlugin ha = server.getHA();
    if (ha == null) {
      // Non-HA mode: direct local mutation.
      server.getSecurity().createUser(json);
      return;
    }

    // HA mode: compute the new users payload and submit a Raft entry.
    // The `synchronized` block serialises the read-compute-submit sequence with
    // any other user mutation running on this leader, so two concurrent createUser
    // calls cannot overwrite each other's in-flight changes.
    synchronized (server.getSecurity()) {
      if (server.getSecurity().getUser(json.getString("name")) != null)
        throw new ServerSecurityException("User '" + json.getString("name") + "' already exists");

      final JSONArray currentUsers = new JSONArray(server.getSecurity().getUsersJsonPayload());
      currentUsers.put(json);
      ha.replicateSecurityUsers(currentUsers.toString());
    }
  }

  private void dropUser(final String userName) {
    if (userName.isEmpty())
      throw new IllegalArgumentException("User name was missing");

    Metrics.counter("http.drop-user").increment();

    final ArcadeDBServer server = httpServer.getServer();
    final HAServerPlugin ha = server.getHA();

    if (ha == null) {
      // Non-HA mode: direct local mutation.
      final boolean result = server.getSecurity().dropUser(userName);
      if (!result)
        throw new IllegalArgumentException("User '" + userName + "' not found on server");
      return;
    }

    // HA mode: compute the new users payload and submit a Raft entry.
    // Serialises read-compute-submit with other user mutations on this leader so
    // two concurrent drops (or a concurrent createUser) cannot lose each other's changes.
    synchronized (server.getSecurity()) {
      if (server.getSecurity().getUser(userName) == null)
        throw new IllegalArgumentException("User '" + userName + "' not found on server");

      final JSONArray currentUsers = new JSONArray(server.getSecurity().getUsersJsonPayload());
      final JSONArray filtered = new JSONArray();
      for (int i = 0; i < currentUsers.length(); i++) {
        final JSONObject user = currentUsers.getJSONObject(i);
        if (!userName.equals(user.getString("name", "")))
          filtered.put(user);
      }
      ha.replicateSecurityUsers(filtered.toString());
    }
  }

  private boolean connectCluster(final String serverAddress, final HttpServerExchange exchange) {
    Metrics.counter("http.connect-cluster").increment();

    throw new CommandExecutionException(
        "Connect cluster operation is not supported by the current HA implementation. Use the cluster configuration to join nodes.");
  }

  private void disconnectCluster() {
    Metrics.counter("http.server-disconnect").increment();

    getHA().disconnectCluster();
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

    try (final var rs = database.command("sql", "align database")) {
      // align database is fire-and-forget here; close releases the ResultSet's plan state.
    }
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
      final Path configPath = Path.of(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);
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
    final Path configPath = Path.of(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);

    // Write configuration atomically so a crash mid-write leaves the previous valid file intact.
    // atomicWriteFile also creates the parent config directory if needed.
    FileUtils.atomicWriteFile(configPath.toFile(), configJson.toString(2));

    // Reload configuration in the plugin
    final AutoBackupSchedulerPlugin plugin = getBackupPlugin(server);
    if (plugin != null && plugin.isEnabled())
      plugin.reloadConfiguration();

    final JSONObject response = new JSONObject().put("result", "ok");
    return new ExecutionResponse(200, response.toString());
  }

  private void validateBackupDirectory(final String backupDir) {
    // Use consolidated validation from AutoBackupSchedulerPlugin
    final Path serverRoot = Path.of(httpServer.getServer().getRootPath()).toAbsolutePath().normalize();
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
        final Path backupPath = Path.of(backupDirectory);
        if (!backupPath.isAbsolute())
          backupDirectory = Path.of(server.getRootPath(), backupDirectory).toString();

        final Path dbBackupDir = Path.of(backupDirectory, databaseName);
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
      final Path configPath = Path.of(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);
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
        final Path backupPath = Path.of(backupDirectory);
        if (!backupPath.isAbsolute())
          backupDirectory = Path.of(server.getRootPath(), backupDirectory).toString();

        // Perform backup using reflection (same as BackupTask)
        final Database database = server.getDatabase(databaseName);
        final Class<?> clazz = Class.forName("com.arcadedb.integration.backup.Backup");

        final String timestamp = LocalDateTime.now()
            .format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        final String backupFileName = databaseName + "-backup-" + timestamp + ".zip";

        final Path dbBackupPath = Path.of(backupDirectory, databaseName);
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
      try (final var result = database.command("sql", "backup database")) {

        final JSONObject response = new JSONObject();
        response.put("result", "ok");
        // The SQL "backup database" command sets backupFile as a property on a Result row
        // (see BackupDatabaseStatement). Read it via Result.getProperty rather than the
        // pre-existing dead instanceof Map check, which never matched.
        while (result.hasNext()) {
          final var row = result.next();
          final Object backupFile = row.getProperty("backupFile");
          if (backupFile != null) {
            response.put("backupFile", backupFile.toString());
            break;
          }
        }
        return new ExecutionResponse(200, response.toString());
      }
    } catch (final Exception e) {
      throw new RuntimeException("Error triggering backup for database '" + databaseName + "': " + e.getMessage(), e);
    }
  }

  private ExecutionResponse handleProfilerCommand(final String subCommand) {
    final ServerQueryProfiler profiler = httpServer.getServer().getQueryProfiler();
    final String sub = subCommand.toLowerCase(Locale.ENGLISH).trim();

    if ("start".equals(sub) || sub.startsWith("start ")) {
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

    } else if ("stop".equals(sub)) {
      final JSONObject results = profiler.stop();
      return new ExecutionResponse(200, results != null ? results.toString() : new JSONObject().put("result", "ok").toString());

    } else if ("reset".equals(sub)) {
      profiler.reset();
      return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());

    } else if ("results".equals(sub)) {
      final JSONObject results = profiler.getResults();
      return new ExecutionResponse(200, results != null ? results.toString() : new JSONObject().put("result", "ok").toString());

    } else if ("list".equals(sub)) {
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
      if (plugin instanceof AutoBackupSchedulerPlugin schedulerPlugin)
        return schedulerPlugin;
    }
    return null;
  }

  /**
   * If this node is an HA replica, forwards the server command to the leader and returns its response.
   * Returns null if this node is the leader or HA is not enabled (caller should execute locally).
   */
  private ExecutionResponse forwardToLeaderIfReplica(final HttpServerExchange exchange, final JSONObject payload,
      final ServerSecurityUser user) throws IOException {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha == null || ha.isLeader())
      return null;

    final String leaderHttpAddress = ha.getLeaderAddress();
    if (leaderHttpAddress == null)
      throw new ServerIsNotTheLeaderException("Leader address is unknown", ha.getLeaderName());

    final HeaderValues authValues = exchange.getRequestHeaders().get("Authorization");
    final String authHeader = authValues != null ? authValues.getFirst() : null;

    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create("http://" + leaderHttpAddress + "/api/v1/server"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(payload.toString()));

    if (authHeader != null && authHeader.startsWith("Bearer AU-")) {
      // Per-node session token: convert to cluster-internal identity headers
      final String clusterToken = httpServer.getServer().getConfiguration()
          .getValueAsString(GlobalConfiguration.HA_CLUSTER_TOKEN);
      final String userName = user != null ? user.getName() : null;
      if (userName != null)
        builder.header("X-ArcadeDB-Forwarded-User", userName);
      if (clusterToken != null && !clusterToken.isBlank())
        builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
    } else if (authHeader != null) {
      // Basic or API token: stateless, forward as-is
      builder.header("Authorization", authHeader);
    }

    try {
      final HttpResponse<String> response = HTTP_CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      return new ExecutionResponse(response.statusCode(), response.body());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while forwarding server command to leader at " + leaderHttpAddress, e);
    }
  }

  private void checkServerIsLeaderIfInHA() {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderName());
  }

  private HAServerPlugin getHA() {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha == null)
      throw new CommandExecutionException(
          "ArcadeDB is not running with High Availability module enabled. Please add this setting at startup: -Darcadedb.ha.enabled=true");
    return ha;
  }

  /**
   * Post-restore HA hook. In HA mode, submits an install-database Raft entry with
   * forceSnapshot=true so every replica pulls the restored files. On any failure,
   * drops the just-restored local database so the operator can retry cleanly.
   */
  private void replicateRestoredDatabase(final ArcadeDBServer server, final ServerDatabase restored,
      final String databaseName) {
    if (!(restored.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb))
      return;

    try {
      haDb.createInReplicas(true);
    } catch (final RuntimeException e) {
      // Compensate: drop the locally-restored database so the operator can retry cleanly.
      try {
        restored.getEmbedded().drop();
        server.removeDatabase(databaseName);
      } catch (final Exception inner) {
        LogManager.instance().log(this, Level.SEVERE,
            "Compensating drop after failed restore replication failed for '%s'", inner, databaseName);
      }
      throw e;
    }
  }
}
