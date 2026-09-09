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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.HAReplicatedDatabase;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IPAddressBlocklist;
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
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class PostServerCommandHandler extends AbstractServerHttpHandler {
  private static final HttpClient HTTP_CLIENT           = HttpClient.newHttpClient();

  /**
   * Emits the "a peer forwarded a server command here and this node is not the leader either" notice only
   * once (issue #6191). Per handler instance, so each server in an in-process cluster says it once.
   */
  private final AtomicBoolean forwardedAgainWarned = new AtomicBoolean(false);

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

  private static final IPAddressBlocklist RESERVED_ADDRESSES = IPAddressBlocklist.defaultReservedRanges();

  /**
   * The transport-independent implementation of these commands, shared with gRPC's
   * {@code ArcadeDbAdminService} so the two protocols cannot drift apart on what an administrative
   * operation does (issue #7304). What stays in this handler is the command-string grammar, the
   * leader forwarding, the mapping onto HTTP status codes, and the operations that stream progress
   * over the exchange.
   */
  private final ServerControlPlane controlPlane;

  public PostServerCommandHandler(final HttpServer httpServer) {
    super(httpServer);
    this.controlPlane = new ServerControlPlane(httpServer.getServer());
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
      count("http.server-shutdown").shutdownServer(extractTarget(command, SHUTDOWN));
    else if (command_lc.startsWith(CREATE_DATABASE))
      createDatabase(extractTarget(command, CREATE_DATABASE));
    else if (command_lc.startsWith(DROP_DATABASE))
      dropDatabase(extractTarget(command, DROP_DATABASE));
    else if (command_lc.startsWith(CLOSE_DATABASE))
      count("http.close-database").closeDatabase(extractTarget(command, CLOSE_DATABASE));
    else if (command_lc.startsWith(OPEN_DATABASE))
      count("http.open-database").openDatabase(extractTarget(command, OPEN_DATABASE));
    else if (command_lc.startsWith(CREATE_USER))
      count("http.create-user").createUser(new JSONObject(extractTarget(command, CREATE_USER)));
    else if (command_lc.startsWith(DROP_USER))
      count("http.drop-user").dropUser(extractTarget(command, DROP_USER));
    else if (command_lc.startsWith(CONNECT_CLUSTER))
      count("http.connect-cluster").connectCluster(extractTarget(command, CONNECT_CLUSTER));
    else if (DISCONNECT_CLUSTER.equals(command_lc))
      count("http.server-disconnect").disconnectCluster();
    else if (command_lc.startsWith(SET_DATABASE_SETTING))
      setDatabaseSetting(extractTarget(command, SET_DATABASE_SETTING));
    else if (command_lc.startsWith(SET_SERVER_SETTING))
      setServerSetting(extractTarget(command, SET_SERVER_SETTING));
    else if (command_lc.startsWith(GET_SERVER_EVENTS))
      response.put("result", count("http.get-server-events").getServerEvents(extractTarget(command, GET_SERVER_EVENTS)));
    else if (command_lc.startsWith(ALIGN_DATABASE))
      count("http.align-database").alignDatabase(extractTarget(command, ALIGN_DATABASE));
    else if (GET_BACKUP_CONFIG.equals(command_lc))
      return new ExecutionResponse(200, count("http.get-backup-config").getBackupConfig().toString());
    else if (SET_BACKUP_CONFIG.equals(command_lc))
      return setBackupConfig(payload);
    else if (command_lc.startsWith(LIST_BACKUPS))
      return new ExecutionResponse(200, count("http.list-backups").listBackups(extractTarget(command, LIST_BACKUPS)).toString());
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

  /**
   * Counts one HTTP server command and hands back the shared implementation to run it.
   * <p>
   * The {@code http.*} counters stay on this side of the split (issue #7304): they count HTTP
   * requests, and {@link ServerControlPlane} now serves gRPC as well, so incrementing them there
   * would silently fold gRPC admin traffic into the HTTP dashboards. gRPC's own admin calls are
   * counted per method by {@code GrpcMetricsInterceptor}.
   */
  private ServerControlPlane count(final String metric) {
    Metrics.counter(metric).increment();
    return controlPlane;
  }

  private String extractTarget(String command, String keyword) {
    final int pos = command.toLowerCase().indexOf(keyword);
    if (pos == -1)
      return "";

    return command.substring(pos + keyword.length()).trim();
  }

  /**
   * {@code set database setting <database> <key> <value>}. The command is tokenized on the first
   * space(s) before {@link ServerControlPlane#applySetting} strips quotes, so quoting cannot make a
   * space part of the database name or of the key.
   */
  private void setDatabaseSetting(final String triple) throws IOException {
    final String tripleTrimmed = triple.trim();
    final int firstSpace = tripleTrimmed.indexOf(" ");
    if (firstSpace == -1)
      throw new IllegalArgumentException("Expected <database> <key> <value>");

    final String pairTrimmed = tripleTrimmed.substring(firstSpace).trim();
    final int secondSpace = pairTrimmed.indexOf(" ");
    if (secondSpace == -1)
      throw new IllegalArgumentException("Expected <database> <key> <value>");

    controlPlane.setDatabaseSetting(tripleTrimmed.substring(0, firstSpace), pairTrimmed.substring(0, secondSpace),
        pairTrimmed.substring(secondSpace + 1));
  }

  /**
   * {@code set server setting <key> <value>}.
   */
  private void setServerSetting(final String pair) {
    final String pairTrimmed = pair.trim();

    final int firstSpace = pairTrimmed.indexOf(" ");
    if (firstSpace == -1)
      throw new IllegalArgumentException("Expected <key> <value>");

    controlPlane.setServerSetting(pairTrimmed.substring(0, firstSpace), pairTrimmed.substring(firstSpace + 1));
  }

  private ExecutionResponse setBackupConfig(final JSONObject payload) throws IOException {
    if (!payload.has("config"))
      throw new IllegalArgumentException("Missing 'config' in payload");

    return new ExecutionResponse(200, count("http.set-backup-config").setBackupConfig(payload.getJSONObject("config")).toString());
  }

  /**
   * {@code trigger backup <database>}. A backup already running for the same database is a 409 here
   * and an {@code ABORTED} on gRPC; both carry the message the shared implementation raised.
   */
  private ExecutionResponse triggerBackup(final String databaseName) {
    try {
      return new ExecutionResponse(200, count("http.trigger-backup").triggerBackup(databaseName).toString());
    } catch (final ServerControlPlane.BackupInProgressException e) {
      return new ExecutionResponse(409, new JSONObject().put("error", e.getMessage()).toString());
    }
  }

  /**
   * {@code delete backup <database> <fileName>}.
   */
  private ExecutionResponse deleteBackup(final String args) {
    final int space = args.indexOf(' ');
    if (space <= 0)
      throw new IllegalArgumentException("Usage: delete backup <database> <fileName>");

    final String databaseName = args.substring(0, space).trim();
    final String fileName = args.substring(space + 1).trim();

    return new ExecutionResponse(200, count("http.delete-backup").deleteBackup(databaseName, fileName).toString());
  }

  /**
   * {@code profiler <start [timeout] | stop | reset | results | list | load <file>>}. Only the
   * sub-command grammar lives here; every branch runs the shared implementation.
   */
  private ExecutionResponse handleProfilerCommand(final String subCommand) {
    final String sub = subCommand.toLowerCase(Locale.ENGLISH).trim();

    if ("start".equals(sub) || sub.startsWith("start ")) {
      final String timeoutStr = sub.substring(5).trim();
      int timeoutSec = 0;
      if (!timeoutStr.isEmpty()) {
        try {
          timeoutSec = Integer.parseInt(timeoutStr);
        } catch (final NumberFormatException e) {
          return new ExecutionResponse(400, "{ \"error\" : \"Invalid timeout value: " + timeoutStr + "\"}");
        }
      }
      return new ExecutionResponse(200, controlPlane.profilerStart(timeoutSec).toString());

    } else if ("stop".equals(sub))
      return new ExecutionResponse(200, controlPlane.profilerStop().toString());
    else if ("reset".equals(sub))
      return new ExecutionResponse(200, controlPlane.profilerReset().toString());
    else if ("results".equals(sub))
      return new ExecutionResponse(200, controlPlane.profilerResults().toString());
    else if ("list".equals(sub))
      return new ExecutionResponse(200, new JSONObject().put("result", controlPlane.profilerList()).toString());
    else if (sub.startsWith("load "))
      return new ExecutionResponse(200, controlPlane.profilerLoad(sub.substring(5).trim()).toString());
    else
      return new ExecutionResponse(400, "{ \"error\" : \"Unknown profiler command: " + subCommand + "\"}");
  }

  private ExecutionResponse listDatabases(final ServerSecurityUser user) {
    final ArcadeDBServer server = httpServer.getServer();
    Metrics.counter("http.list-databases").increment();

    final Set<String> installedDatabases = filterAuthorizedDatabases(user, server.getDatabaseNames());

    final JSONObject response = new JSONObject().put("result", new JSONArray(installedDatabases));

    return new ExecutionResponse(200, response.toString());
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

    // Prevent path traversal via the caller-supplied database name (GHSA-qwgr-2c45-63xx).
    server.checkDatabaseNameIsValid(databaseName);

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

    final Path backupFile = controlPlane.resolveBackupFile(databaseName, fileName);

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
        // SAME BOOLEAN validateClientRestoreImportUrl ALREADY VALIDATED THIS URL AGAINST: THE FETCH INSIDE
        // FullRestoreFormat MUST AGREE WITH THIS SERVER'S OWN CONFIGURATION RATHER THAN FALLING BACK TO THE STATIC
        // DEFAULT, OR A PER-SERVER OVERRIDE THAT LET THE COMMAND THROUGH WOULD STILL HAVE THE FETCH REFUSE IT.
        clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(restorer, isRestoreImportLocalUrlsAllowed());

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
      clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(restorer, isRestoreImportLocalUrlsAllowed());
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
    if (isRestoreImportLocalUrlsAllowed())
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
   * The single source of truth for {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} on this server
   * instance, read from the server's own (possibly per-instance-overridden) {@link
   * com.arcadedb.ContextConfiguration} rather than the static default. {@link #validateClientRestoreImportUrl} and
   * {@link #performRestore} must agree on this value: the pre-check here decides whether to accept the command at
   * all, and the actual fetch inside {@code FullRestoreFormat} decides whether to follow it, and letting them read
   * from two different configuration sources would let one permit what the other refuses on the very same server.
   */
  private boolean isRestoreImportLocalUrlsAllowed() {
    return httpServer.getServer().getConfiguration().getValueAsBoolean(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS);
  }

  /**
   * Returns true when {@code host} resolves to (or is) an address in a range that must not be reached
   * from a client-supplied restore/import URL. Every resolved address is checked so a hostname that
   * resolves to a mix of public and private addresses is still rejected. An unresolvable host is
   * treated as blocked.
   * <p>
   * Delegates to {@link IPAddressBlocklist#defaultReservedRanges()}, the single shared implementation also used
   * by {@code ImportSecurityValidator.isBlockedAddress} in the integration module and by {@code LOAD CSV}. A
   * previous version duplicated this logic ad-hoc; see {@code ImportSecurityValidator.isBlockedAddress} for why
   * that was the root cause of GHSA-67m7-7w7g-mpmh. Package-private for direct unit testing.
   */
  static boolean isBlockedHost(final String host) {
    try {
      for (final InetAddress addr : InetAddress.getAllByName(host))
        if (RESERVED_ADDRESSES.isBlocked(addr))
          return true;
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
        // SAME BOOLEAN validateClientRestoreImportUrl ALREADY VALIDATED THIS URL AGAINST: THE FETCH INSIDE
        // SourceDiscovery MUST AGREE WITH THIS SERVER'S OWN CONFIGURATION RATHER THAN FALLING BACK TO THE STATIC
        // DEFAULT, OR A PER-SERVER OVERRIDE THAT LET THE COMMAND THROUGH WOULD STILL HAVE THE FETCH REFUSE IT (#6474).
        clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(importer, isRestoreImportLocalUrlsAllowed());

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

    // Synchronous fallback. Threads the same resolved boolean validateClientRestoreImportUrl() already validated
    // this URL against into the SQL command's execution context, so ImportDatabaseStatement's deep fetch cannot
    // disagree with the pre-check that accepted the command (#6474, mirroring the setAllowLocalUrls call above).
    final ContextConfiguration importConfiguration = new ContextConfiguration();
    importConfiguration.setValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS, !isRestoreImportLocalUrlsAllowed());
    try (final var rs = database.command("sql", "import database " + url, importConfiguration, Map.of())) {
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

  /**
   * An SSE event is two writes plus a flush, and the stream it goes to is the exchange's own - shared, and not safe
   * for concurrent use. More than one thread reaches here: the import path polls {@code ImporterContext} for progress
   * on a thread of its own while the importer logs on another, and since #6086 a parallel restore logs one line per
   * archive entry from its worker pool. Without the lock two events interleave into a single corrupt {@code data:}
   * frame - which the client cannot parse - rather than merely arriving in an unexpected order.
   */
  private static void sendSSE(final OutputStream out, final JSONObject data) {
    try {
      synchronized (out) {
        out.write(("data: " + data + "\n\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
      }
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



  /**
   * If this node is an HA replica, forwards the server command to the leader and returns its response.
   * Returns null if this node is the leader or HA is not enabled (caller should execute locally).
   */
  private ExecutionResponse forwardToLeaderIfReplica(final HttpServerExchange exchange, final JSONObject payload,
      final ServerSecurityUser user) throws IOException {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha == null || ha.isLeader())
      return null;

    // A peer already forwarded this command to what it believed was the leader and it arrived here, on a node
    // that is not the leader either. Forwarding it on would send it round the cycle that wrong address
    // created; refuse in one hop with the typed error instead (issue #6191).
    if (LeaderForwardContext.isAlreadyForwarded()) {
      // Also said once in this node's log: the refusal goes back to the peer and from there to the client, so
      // otherwise the only node that can name the misconfiguration never mentions it.
      if (forwardedAgainWarned.compareAndSet(false, true))
        LogManager.instance().log(this, Level.WARNING,
            "A cluster peer forwarded a server command to this node as the leader, but this node is not the leader. "
                + "Unless leadership just moved, the HTTP address that peer resolved for the leader does not identify "
                + "it: declare every node's HTTP port explicitly with the 'host:raftPort:httpPort' syntax in %s. The "
                + "command is refused rather than forwarded on. This notice is logged only once.",
            GlobalConfiguration.HA_SERVER_LIST.getKey());
      throw new ServerIsNotTheLeaderException(
          "Refusing to forward a server command that a cluster peer already forwarded to the leader: it arrived on "
              + "this node, which is not the leader. Either leadership moved while the request was in flight - retry - "
              + "or the HTTP address that peer resolved for the leader does not identify it, which is what declaring "
              + "every node's HTTP port ('host:raftPort:httpPort') in " + GlobalConfiguration.HA_SERVER_LIST.getKey()
              + " prevents", ha.getLeaderName());
    }

    final String leaderHttpAddress = ha.getLeaderAddress();
    if (leaderHttpAddress == null)
      throw new ServerIsNotTheLeaderException("Leader address is unknown", ha.getLeaderName());

    // Dialing an address that resolves to this node comes straight back here, and this node is not the
    // leader. That is what the derive fallback produces when the peers share a host and no HTTP port is
    // declared: it pairs the leader's Raft host with THIS node's HTTP port (issue #6191).
    if (ha.isOwnHttpAddress(leaderHttpAddress))
      throw new ServerIsNotTheLeaderException(
          "Cannot forward the server command: the HTTP address resolved for the leader (" + leaderHttpAddress
              + ") is this node's own, and this node is not the leader. Declare every node's HTTP port explicitly with "
              + "the 'host:raftPort:httpPort' syntax in " + GlobalConfiguration.HA_SERVER_LIST.getKey(),
          ha.getLeaderName());

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
      if (clusterToken != null && !clusterToken.isBlank()) {
        builder.header("X-ArcadeDB-Cluster-Token", clusterToken);
        // One hop only: whichever node this address really names refuses the command if it is not the leader,
        // instead of resolving the same address and forwarding it again (issue #6191). Sent only alongside
        // the cluster token, because that is the only form in which the receiving node trusts it - see
        // LeaderForwardContext. The other branch below relays the client's own credentials and carries no
        // marker; its loop protection is the self-address check above.
        builder.header(LeaderForwardContext.FORWARDED_TO_LEADER_HEADER, "true");
      }
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
