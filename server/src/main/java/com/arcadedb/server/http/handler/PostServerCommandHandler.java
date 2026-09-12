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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerControlPlane;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

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
  private static final String RESTORE_BACKUP       = "restore backup";
  private static final String DELETE_BACKUP        = "delete backup";
  private static final String RESTORE_DATABASE     = "restore database";
  private static final String IMPORT_DATABASE      = "import database";
  private static final String PROFILER             = "profiler";

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
    else if (command_lc.startsWith(CONNECT_CLUSTER))
      connectCluster(extractTarget(command, CONNECT_CLUSTER));
    else if (DISCONNECT_CLUSTER.equals(command_lc))
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

  // ---------------------------------------------------------------------------------------------
  // The commands whose implementation moved to ServerControlPlane (issue #7304). Each wrapper runs
  // the shared implementation and then increments this command's http.* counter.
  //
  // The counters stay on this side of the split because they count HTTP requests, and
  // ServerControlPlane now serves gRPC as well: incrementing them there would fold gRPC admin
  // traffic into the HTTP dashboards. gRPC counts its own admin calls per method in
  // GrpcMetricsInterceptor.
  //
  // The increment comes AFTER the call, never before it. Every one of these counters used to be
  // incremented inside the moved method, past that method's own validation, so a command rejected
  // for an empty database name or a password the policy refuses was never counted. Chaining the
  // increment onto the receiver would have counted attempts instead, because Java evaluates the
  // receiver first.
  // ---------------------------------------------------------------------------------------------

  private void shutdownServer(final String serverName) throws IOException {
    controlPlane.shutdownServer(serverName);
    Metrics.counter("http.server-shutdown").increment();
  }

  private void closeDatabase(final String databaseName) {
    controlPlane.closeDatabase(databaseName);
    Metrics.counter("http.close-database").increment();
  }

  private void openDatabase(final String databaseName) {
    controlPlane.openDatabase(databaseName);
    Metrics.counter("http.open-database").increment();
  }

  private void createUser(final String payload) {
    controlPlane.createUser(new JSONObject(payload));
    Metrics.counter("http.create-user").increment();
  }

  private void dropUser(final String userName) {
    controlPlane.dropUser(userName);
    Metrics.counter("http.drop-user").increment();
  }

  private void connectCluster(final String serverAddress) {
    controlPlane.connectCluster(serverAddress);
    Metrics.counter("http.connect-cluster").increment();
  }

  private void disconnectCluster() {
    controlPlane.disconnectCluster();
    Metrics.counter("http.server-disconnect").increment();
  }

  private void alignDatabase(final String databaseName) {
    controlPlane.alignDatabase(databaseName);
    Metrics.counter("http.align-database").increment();
  }

  private JSONObject getServerEvents(final String fileName) {
    final JSONObject events = controlPlane.getServerEvents(fileName);
    Metrics.counter("http.get-server-events").increment();
    return events;
  }

  private ExecutionResponse getBackupConfig() {
    final JSONObject config = controlPlane.getBackupConfig();
    Metrics.counter("http.get-backup-config").increment();
    return new ExecutionResponse(200, config.toString());
  }

  private ExecutionResponse listBackups(final String databaseName) {
    final JSONObject backups = controlPlane.listBackups(databaseName);
    Metrics.counter("http.list-backups").increment();
    return new ExecutionResponse(200, backups.toString());
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

    final JSONObject result = controlPlane.setBackupConfig(payload.getJSONObject("config"));
    Metrics.counter("http.set-backup-config").increment();
    return new ExecutionResponse(200, result.toString());
  }

  /**
   * {@code trigger backup <database>}. A backup - or, since #7384, a restore - already running for the
   * same database is a 409 here and an {@code ABORTED} on gRPC; both carry the message the shared
   * implementation raised.
   */
  private ExecutionResponse triggerBackup(final String databaseName) {
    try {
      final JSONObject result = controlPlane.triggerBackup(databaseName);
      Metrics.counter("http.trigger-backup").increment();
      return new ExecutionResponse(200, result.toString());
    } catch (final ServerControlPlane.OperationInProgressException e) {
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

    final JSONObject result = controlPlane.deleteBackup(databaseName, fileName);
    Metrics.counter("http.delete-backup").increment();
    return new ExecutionResponse(200, result.toString());
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

    Metrics.counter("http.create-database").increment();

    // The cluster-wide create lives in the control plane so gRPC's CreateDatabase runs the same
    // thing rather than a second implementation that forgot the replication half (issue #7389).
    controlPlane.createDatabase(databaseName);
  }

  /**
   * Restores a database from a backup URL. Format: {@code restore database <name> <url>}.
   * Streams progress as SSE when the client sends {@code Accept: text/event-stream}.
   * <p>
   * The restore itself lives in {@link ServerControlPlane#restoreDatabase}, shared with the gRPC
   * {@code RestoreDatabase} RPC (issue #7308); what is left here is the command grammar and the
   * choice of progress sink.
   */
  private ExecutionResponse restoreDatabase(final String args, final HttpServerExchange exchange) {
    final int space = args.indexOf(' ');
    if (space <= 0)
      throw new IllegalArgumentException("Usage: restore database <name> <url>");

    final String databaseName = args.substring(0, space).trim();
    final String url = args.substring(space + 1).trim();

    if (databaseName.isEmpty() || url.isEmpty())
      throw new IllegalArgumentException("Usage: restore database <name> <url>");

    // The leader gate now precedes the URL guard, which the shared implementation applies (it used to
    // run here, first). Deliberate: a node that is going to refuse the command should not first
    // resolve a hostname the caller chose, and the caller has to take the command to the leader
    // either way, where the URL refusal is what they will get. Issue #7308.
    checkServerIsLeaderIfInHA();
    Metrics.counter("http.restore-database").increment();

    return streamOrRun(exchange, databaseName + " restored successfully",
        listener -> { controlPlane.restoreDatabase(databaseName, url, listener); return null; });
  }

  /**
   * Restores a previously created backup file into a database. The backup file is resolved
   * server-side from the configured auto-backup directory, so the client never supplies a
   * filesystem path. Format: {@code restore backup <database> <fileName> as <targetDatabase>}.
   * The optional payload flag {@code "overwrite": true} drops the target database first if it
   * already exists; without it the command fails when the target database exists.
   * <p>
   * Shared with the gRPC {@code RestoreBackup} RPC through {@link ServerControlPlane#restoreBackup}.
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
    Metrics.counter("http.restore-backup").increment();

    return streamOrRun(exchange, targetDatabase + " restored successfully",
        listener -> { controlPlane.restoreBackup(databaseName, fileName, targetDatabase, overwrite, listener); return null; });
  }

  /**
   * Deletes a single backup file from the configured auto-backup directory. The file name is
   * validated and resolved server-side to prevent path traversal. Format:
   * {@code delete backup <database> <fileName>}.
   */
  /**
   * Returns true when {@code host} resolves to (or is) an address in a range that must not be reached
   * from a client-supplied restore/import URL.
   * <p>
   * Delegates to {@link ServerControlPlane#isBlockedRestoreImportHost}, which is where the guard moved
   * when restore and import became transport-independent (issue #7308). Kept here, package-private, so
   * the SSRF unit test keeps naming the class whose command surface the guard protects.
   */
  static boolean isBlockedHost(final String host) {
    return ServerControlPlane.isBlockedRestoreImportHost(host);
  }

  /**
   * Creates and imports a database in one step. Format: {@code import database <name> <url>}.
   * Streams progress as SSE when the client sends {@code Accept: text/event-stream}.
   * <p>
   * The import itself lives in {@link ServerControlPlane#importDatabase}, shared with the gRPC
   * {@code ImportDatabase} RPC (issue #7308).
   */
  private ExecutionResponse importDatabase(final String args, final HttpServerExchange exchange) {
    final int space = args.indexOf(' ');
    if (space <= 0)
      throw new IllegalArgumentException("Usage: import database <name> <url>");

    final String databaseName = args.substring(0, space).trim();
    final String url = args.substring(space + 1).trim();

    if (databaseName.isEmpty() || url.isEmpty())
      throw new IllegalArgumentException("Usage: import database <name> <url>");

    // The leader gate now precedes the URL guard, which the shared implementation applies (it used to
    // run here, first). Deliberate: a node that is going to refuse the command should not first
    // resolve a hostname the caller chose, and the caller has to take the command to the leader
    // either way, where the URL refusal is what they will get. Issue #7308.
    checkServerIsLeaderIfInHA();
    Metrics.counter("http.import-database").increment();

    return streamOrRun(exchange, databaseName + " imported successfully",
        listener -> controlPlane.importDatabase(databaseName, url, listener));
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Progress streaming (SSE)
  // ═══════════════════════════════════════════════════════════════════

  /** One long-running control-plane operation, run against a progress sink this handler supplies. */
  @FunctionalInterface
  private interface ProgressingOperation {
    /** @return the operation's final report, merged into the completion event, or null when it has none. */
    JSONObject run(ServerControlPlane.ProgressListener listener);
  }

  /**
   * Runs a restore or import, reporting its progress the way the client asked for it (issue #7308).
   * <p>
   * Without {@code Accept: text/event-stream} the operation runs to completion and answers with the
   * usual JSON body. With it, every progress event becomes an SSE frame and the answer is the stream
   * itself.
   * <p>
   * The stream is started <b>lazily</b>, on the first event rather than up front, which is what keeps
   * a rejected request an HTTP error instead of a 200 carrying an error frame: an invalid URL, an
   * invalid database name or an existing target all fail before the operation produces its first
   * progress line, so they still propagate to {@code AbstractServerHttpHandler}'s status mapping. A
   * failure <i>during</i> the restore or import - by which point the response has already begun - can
   * only be reported inside the stream, and becomes an {@code error} frame as before.
   */
  private ExecutionResponse streamOrRun(final HttpServerExchange exchange, final String completionMessage,
      final ProgressingOperation operation) {
    if (!isSSERequested(exchange)) {
      // Nothing to catch: a failure propagates to AbstractServerHttpHandler, which is what maps it
      // onto a status code, and every control-plane failure is unchecked.
      operation.run(ServerControlPlane.ProgressListener.NOOP);
      return new ExecutionResponse(200, new JSONObject().put("result", "ok").toString());
    }

    final SSEProgressSink sink = new SSEProgressSink(exchange);
    try {
      final JSONObject report = operation.run(sink);

      final JSONObject completed = new JSONObject().put("status", "completed").put("message", completionMessage);
      if (report != null)
        for (final String key : report.keySet())
          completed.put(key, report.get(key));
      sink.send(completed);
    } catch (final RuntimeException e) {
      // Nothing has been written yet, so the request can still be answered with a status code.
      if (!sink.started())
        throw e;
      sink.send(new JSONObject().put("status", "error").put("message", failureMessage(e)));
    } finally {
      sink.close();
    }
    return null; // response already sent via SSE
  }

  /**
   * The message an SSE {@code error} frame carries. The control plane wraps a failure raised inside
   * the restore or import machinery in a {@link CommandExecutionException}, so the cause is the one
   * that names what actually went wrong - the same message the pre-#7308 handler read straight off
   * the {@code InvocationTargetException}.
   */
  private static String failureMessage(final RuntimeException e) {
    final Throwable cause = e.getCause();
    return cause != null && cause.getMessage() != null ? cause.getMessage() : e.getMessage();
  }

  /**
   * A {@link ServerControlPlane.ProgressListener} that writes each event to the exchange as an SSE
   * frame, starting the stream on the first event.
   * <p>
   * An SSE event is two writes plus a flush, and the stream it goes to is the exchange's own -
   * shared, and not safe for concurrent use. More than one thread reaches here: an import polls
   * {@code ImporterContext} for counters on a timer thread while the importer logs on the calling
   * thread, and since #6086 a parallel restore logs one line per archive entry from its worker pool.
   * Without the lock two events interleave into a single corrupt {@code data:} frame, which the
   * client cannot parse, rather than merely arriving in an unexpected order.
   */
  private static final class SSEProgressSink implements ServerControlPlane.ProgressListener {
    private final HttpServerExchange exchange;
    private       OutputStream       out;

    private SSEProgressSink(final HttpServerExchange exchange) {
      this.exchange = exchange;
    }

    @Override
    public void onProgress(final String message) {
      send(new JSONObject().put("status", "progress").put("message", message));
    }

    @Override
    public void onImportCounters(final long parsed, final long vertices, final long edges) {
      send(new JSONObject().put("status", "progress").put("parsed", parsed).put("vertices", vertices).put("edges", edges));
    }

    synchronized boolean started() {
      return out != null;
    }

    synchronized void send(final JSONObject data) {
      try {
        if (out == null) {
          exchange.getResponseHeaders().put(new HttpString("Content-Type"), "text/event-stream");
          exchange.getResponseHeaders().put(new HttpString("Cache-Control"), "no-cache");
          exchange.getResponseHeaders().put(new HttpString("X-Accel-Buffering"), "no");
          exchange.setStatusCode(200);
          if (!exchange.isBlocking())
            exchange.startBlocking();
          out = exchange.getOutputStream();
        }
        out.write(("data: " + data + "\n\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
      } catch (final IOException ignored) {
        // Client disconnected
      }
    }

    synchronized void close() {
      if (out == null)
        return;
      try {
        out.close();
      } catch (final IOException ignored) {
        // Client disconnected
      }
    }
  }

  private static boolean isSSERequested(final HttpServerExchange exchange) {
    final String accept = exchange.getRequestHeaders().getFirst("Accept");
    return accept != null && accept.contains("text/event-stream");
  }

  private void dropDatabase(final String databaseName) {
    if (databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");

    checkServerIsLeaderIfInHA();

    Metrics.counter("http.drop-database").increment();

    // Raft-first on a replicated database, local otherwise: shared with gRPC's DropDatabase, which
    // took the local branch unconditionally until issue #7389.
    controlPlane.dropDatabase(databaseName);
  }



  /**
   * If this node is an HA replica, forwards the server command to the leader and returns its response.
   * Returns null if this node is the leader or HA is not enabled (caller should execute locally).
   * <p>
   * The mechanics live in {@link LeaderCommandForwarder}, shared with the REST {@code /server/users} routes
   * that perform the same operations and used to run them wherever the request landed (issue #7380).
   */
  private ExecutionResponse forwardToLeaderIfReplica(final HttpServerExchange exchange, final JSONObject payload,
      final ServerSecurityUser user) throws IOException {
    return httpServer.getLeaderCommandForwarder()
        .forwardIfReplica(exchange, user, LeaderCommandForwarder.currentPathWithQuery(exchange), payload.toString());
  }

  private void checkServerIsLeaderIfInHA() {
    final HAServerPlugin ha = httpServer.getServer().getHA();
    if (ha != null && !ha.isLeader())
      throw new ServerIsNotTheLeaderException("Creation of database can be executed only on the leader server", ha.getLeaderName());
  }
}
