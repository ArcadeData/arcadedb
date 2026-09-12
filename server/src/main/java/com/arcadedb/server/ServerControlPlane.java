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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.OperationProgress;
import com.arcadedb.engine.OperationProgressRegistry;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.backup.AutoBackupConfig;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.server.backup.BackupCoordinator.Operation;
import com.arcadedb.server.http.HttpAuthSession;
import com.arcadedb.server.http.HttpAuthSessionManager;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IPAddressBlocklist;
import com.arcadedb.utility.ProgressCallback;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * The server's control plane: the administrative operations that do not depend on the transport
 * that asked for them.
 * <p>
 * Every method here used to be a private method of
 * {@code com.arcadedb.server.http.handler.PostServerCommandHandler}, reachable only by parsing a
 * {@code POST /server} command string. It moved so that gRPC's {@code ArcadeDbAdminService} runs
 * the same code rather than a second implementation of the same semantics (issue #7304): the two
 * protocols differ in how a caller names an operation and how a failure is reported, not in what
 * the operation does.
 * <p>
 * What stays behind in the HTTP handler is what is genuinely HTTP: parsing the command string,
 * forwarding a write to the leader, and mapping a result to a status code. The three operations
 * that report progress while they run - restore backup, restore database, import database - live
 * here too since issue #7308; the transport supplies a {@link ProgressListener} and decides whether
 * an event becomes an SSE frame or a message on a server-streaming RPC.
 * <p>
 * <b>This class performs no authorization, and no leader routing.</b> Each transport supplies both:
 * HTTP through {@code AbstractServerHttpHandler.checkRootUser} and
 * {@code PostServerCommandHandler.forwardToLeaderIfReplica}, gRPC through
 * {@code ArcadeDbGrpcAdminService}'s {@code requireServerAdmin} and {@code requireLeader}. Adding a
 * caller means adding both gates.
 * <p>
 * <b>It also emits no metrics.</b> The {@code http.*} counters these operations carried stayed with
 * the HTTP handler, because they count HTTP requests; folding gRPC admin traffic into them would
 * have been invisible in a dashboard. gRPC counts its admin calls per method in
 * {@code GrpcMetricsInterceptor}.
 */
public class ServerControlPlane {
  private static final IPAddressBlocklist RESERVED_ADDRESSES = IPAddressBlocklist.defaultReservedRanges();

  private final ArcadeDBServer server;

  public ServerControlPlane(final ArcadeDBServer server) {
    this.server = server;
  }

  // ---------------------------------------------------------------------------------------------
  // Server lifecycle
  // ---------------------------------------------------------------------------------------------

  /**
   * Stops this server (empty {@code serverName}) or asks HA to stop a named peer. The local branch
   * schedules the stop a second out so the caller's own response can still be written before the
   * JVM exits.
   */
  public void shutdownServer(final String serverName) throws IOException {

    if (serverName.isEmpty()) {
      // SHUTDOWN CURRENT SERVER
      new Timer().schedule(new TimerTask() {
        @Override
        public void run() {
          server.stop();
          System.exit(0);
        }
      }, 1000);
    } else {
      requireHA().shutdownRemoteServer(serverName);
    }
  }

  public JSONObject getServerEvents(final String fileName) {

    final JSONArray events = fileName.isEmpty() ?
        server.getEventLog().getCurrentEvents() :
        server.getEventLog().getEvents(fileName);
    final JSONArray files = server.getEventLog().getFiles();

    return new JSONObject().put("events", events).put("files", files);
  }

  public void disconnectCluster() {

    requireHA().disconnectCluster();
  }

  /**
   * The {@code connect cluster} command, shared by the HTTP verb and the gRPC {@code ConnectCluster}
   * RPC (issue #7400). Kept here with the rest so the command set is complete in one place; it has
   * never been implemented by the current HA stack, and whether to implement the join or retire the
   * verb is issue #7401.
   * <p>
   * The refusal names the address the caller asked for, as the rest of this class names the user,
   * group or backup file a command could not act on. That is the caller's own argument echoed back,
   * so it tells an operator which of several join attempts failed - and it is the only thing that
   * makes the argument observable from outside while the operation itself does nothing with it, so
   * a transport that dropped the parameter on the way here cannot do so unnoticed.
   * <p>
   * Not null-guarded. Both callers found by
   * {@code grep -rn --include='*.java' "\.connectCluster(" --exclude-dir=target .} on the main
   * sources pass a value that cannot be null: {@code PostServerCommandHandler} passes
   * {@code extractTarget}'s result, which is {@code ""} or a substring, and
   * {@code ArcadeDbGrpcAdminService} passes a protobuf string field, which defaults to {@code ""}.
   * A null from some future caller renders as {@code 'null'} in the message rather than throwing,
   * so the guard would buy nothing.
   */
  public void connectCluster(final String serverAddress) {

    throw new OperationNotAvailableException("Connect cluster to '" + serverAddress
        + "' is not supported by the current HA implementation. Use the cluster configuration to join nodes.");
  }

  // ---------------------------------------------------------------------------------------------
  // Probes
  // ---------------------------------------------------------------------------------------------

  /**
   * Liveness. Reaching a request handler at all proves the process is live, so this deliberately
   * does not consult server status: a node still warming up must not be killed.
   */
  public boolean isLive() {
    return true;
  }

  /**
   * Readiness, as {@code GET /ready} computes it: {@code null} when the node is ready to serve, and
   * otherwise the reason it is not. Returning the reason rather than a boolean keeps the HTTP body
   * and the gRPC status description identical.
   */
  public String notReadyReason() {
    if (server.getStatus() != ArcadeDBServer.STATUS.ONLINE)
      return "Server not started yet";

    if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA)
        && server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_ENABLED)) {
      final HAServerPlugin ha = server.getHA();
      if (ha == null || ha.getElectionStatus() != HAServerPlugin.ELECTION_STATUS.DONE)
        return "Node has not yet joined the Raft group";

      final long maxLag = Math.max(0L, server.getConfiguration().getValueAsLong(GlobalConfiguration.SERVER_READINESS_HA_MAX_LAG));
      if (ha.getReadinessSignal(maxLag) == HAServerPlugin.READINESS_SIGNAL.NOT_READY)
        return "Node is not yet in the Raft configuration or has not caught up";
    }

    return null;
  }

  // ---------------------------------------------------------------------------------------------
  // Discovery
  // ---------------------------------------------------------------------------------------------

  /**
   * The databases {@code user} is allowed to see, in the server's own order. A null user - a server
   * with security disabled - sees all of them.
   * <p>
   * Listing is the one control-plane read that is not root-only on either transport, so the answer
   * has to be narrowed to the caller instead: over HTTP, {@code list databases} and
   * {@code GET /api/v1/databases} both filter here, and the gRPC {@code ListDatabases} does too
   * since issue #7304. {@code ExistsDatabase} deliberately does not - it tests one name the caller
   * already knows, and {@code GetExistsDatabaseHandler} says the same of its HTTP counterpart.
   */
  public static Set<String> filterAuthorizedDatabases(final ServerSecurityUser user, final Collection<String> databaseNames) {
    final Set<String> authorized = new LinkedHashSet<>(databaseNames.size());
    for (final String databaseName : databaseNames)
      if (user == null || user.canAccessToDatabase(databaseName))
        authorized.add(databaseName);
    return authorized;
  }

  /**
   * The databases {@code user} is allowed to see on this server.
   */
  public Set<String> listAuthorizedDatabases(final ServerSecurityUser user) {
    return filterAuthorizedDatabases(user, server.getDatabaseNames());
  }

  /**
   * The long-running maintenance operations (CHECK DATABASE, REBUILD INDEX, COMPACT INDEX, backup,
   * import) this server is running for {@code databaseName}, oldest first. Reads only the lock-free
   * {@link OperationProgressRegistry} snapshot - no database access, no transaction - so it is safe to
   * poll at any frequency and cannot interfere with the operation being watched.
   * <p>
   * The registry is keyed by database name, which is why a missing name is refused rather than answered
   * with an empty list: "nothing is running" would be a lie told to a caller that named nothing. HTTP
   * maps the refusal to 400 and gRPC to {@code INVALID_ARGUMENT}.
   * <p>
   * Deliberately process-local, as the registry is: each node reports what it is doing, which is what an
   * operator polling that node wants to see.
   */
  public List<OperationProgress> getProgress(final String databaseName) {
    if (databaseName == null || databaseName.isEmpty())
      throw new IllegalArgumentException("Database parameter is null");

    return OperationProgressRegistry.instance().getOperations(databaseName);
  }

  /**
   * The server's open HTTP authentication sessions, the administrative view {@code GET /api/v1/sessions}
   * gives root and, since issue #7310, the gRPC {@code ListSessions} gives it too.
   * <p>
   * Empty on a server running without the HTTP listener: {@link ArcadeDBServer#getHttpServer()} is then
   * null - the state {@code GrpcServerPlugin} already handles when it wires the gRPC auth interceptor -
   * and a server with no HTTP listener genuinely has no HTTP sessions. That is an answer, not a failure,
   * so a gRPC-only operator gets an empty list rather than an error it cannot act on.
   */
  public List<HttpAuthSession> listHttpSessions() {
    final HttpServer httpServer = server.getHttpServer();
    if (httpServer == null)
      return List.of();

    final HttpAuthSessionManager sessionManager = httpServer.getAuthSessionManager();
    return sessionManager == null ? List.of() : sessionManager.getActiveSessions();
  }

  // ---------------------------------------------------------------------------------------------
  // Database lifecycle
  // ---------------------------------------------------------------------------------------------

  /**
   * Creates {@code databaseName} on the whole cluster: locally first, then - when the resulting
   * database is replicated - through the Raft install-database entry, so every peer installs it too.
   * <p>
   * Lives here rather than in a transport because a create that skips the second half is a cluster
   * that disagrees about which databases exist, with no error reported to the caller. gRPC's
   * {@code CreateDatabase} called {@code server.createDatabase} alone until issue #7389, so the same
   * operation replicated over HTTP and did not over gRPC.
   *
   * @return the created database, so a caller that has more to do with it - the {@code graph} variant
   * of the gRPC RPC, which initialises the default {@code V} and {@code E} types - does not have to
   * look it up again
   */
  public ServerDatabase createDatabase(final String databaseName) {
    requireDatabaseName(databaseName);

    final ServerDatabase database = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);

    if (database.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb)
      haDb.createInReplicas();

    return database;
  }

  /**
   * Drops {@code databaseName} on the whole cluster.
   * <p>
   * On a replicated database the drop is Raft-first: the entry is submitted and the state machine
   * apply performs the actual delete on every peer, this one included, once it commits. Deleting the
   * files here instead - which is what {@code getEmbedded().drop()} does, unwrapping past the Raft
   * wrapper - takes the database out from under the cluster and leaves it on every follower
   * (issue #7389).
   *
   * @throws IllegalArgumentException when no database by that name is registered on this server
   */
  public void dropDatabase(final String databaseName) {
    requireDatabaseName(databaseName);

    if (!server.existsDatabase(databaseName))
      throw new IllegalArgumentException("Database '" + databaseName + "' does not exist");

    dropDatabaseClusterWide(server.getDatabase(databaseName), databaseName);
  }

  /**
   * The HA-aware half of {@link #dropDatabase}, without the name and existence checks, for the
   * callers that have already resolved the database - {@link #dropDatabase} itself and the drop a
   * restore performs on the database it is about to replace.
   */
  private void dropDatabaseClusterWide(final ServerDatabase database, final String databaseName) {
    if (database.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb)
      haDb.dropInReplicas();
    else {
      // Non-HA: there is no cluster to tell, so delete the files here.
      database.getEmbedded().drop();
      server.removeDatabase(databaseName);
    }
  }

  public void openDatabase(final String databaseName) {
    requireDatabaseName(databaseName);

    server.getDatabase(databaseName);
  }

  public void closeDatabase(final String databaseName) {
    requireDatabaseName(databaseName);

    final ServerDatabase database = server.getDatabase(databaseName);
    database.getEmbedded().close();


    server.removeDatabase(database.getName());
  }

  public void alignDatabase(final String databaseName) {
    requireDatabaseName(databaseName);

    final Database database = server.getDatabase(databaseName);


    try (final var rs = database.command("sql", "align database")) {
      // align database is fire-and-forget here; close releases the ResultSet's plan state.
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Settings
  // ---------------------------------------------------------------------------------------------

  public void setDatabaseSetting(final String databaseName, final String key, final String value) throws IOException {
    requireDatabaseName(databaseName);

    final DatabaseInternal database = server.getDatabase(databaseName);
    applySetting(database.getConfiguration(), key, value);
    database.saveConfiguration();
  }

  public void setServerSetting(final String key, final String value) {
    applySetting(server.getConfiguration(), key, value);
  }

  /**
   * Stores one {@code <key> <value>} pair of a "set ... setting" command into {@code configuration}.
   * <p>
   * Issue #6875: both callers used to hand the raw tokens to {@link ContextConfiguration#setValue(String, Object)},
   * which is a plain map put. Three things went wrong there and are fixed here:
   * <ul>
   * <li>the value kept the separating space that {@code substring(firstSpace)} left on it, so every value was
   * stored with a leading blank;</li>
   * <li>the tokens kept the backticks and quotes the documented command syntax uses
   * ({@code SET SERVER SETTING `arcadedb.foo` 10}), so a quoted key was stored under a name nothing reads - a
   * silent no-op answered with a 200;</li>
   * <li>nothing checked the value against the setting's declared type, so an unparseable one was accepted here and
   * threw later, inside whichever component read the setting next.</li>
   * </ul>
   * A key that names no declared setting is still stored verbatim, as it always has been: it carries no type to
   * validate against, and rejecting it would change behaviour this endpoint has long allowed. The {@code
   * set_server_setting} MCP tool is stricter on that point only - for a DECLARED setting the two now accept and
   * refuse exactly the same values, both through {@link GlobalConfiguration#coerceFromAdminCommand(Object)}.
   * <p>
   * Issue #7124: that conversion is the STRICT one. A typo in a {@code Boolean} value used to reach
   * {@code Boolean.parseBoolean} and read as {@code false}, so {@code ... requireAuthentication ture} was answered
   * with a 200 and quietly published the metrics endpoint unauthenticated. Every other type already refused what it
   * could not read; a boolean now does too, with the same 400.
   * <p>
   * The HTTP command is still tokenized on the first space(s) BEFORE the quotes are stripped, so quoting does not
   * make a space part of a token: a database name or a setting key containing one would split wrong. That is
   * unchanged here and harmless for what the grammar can address - every {@link GlobalConfiguration} key is a dotted
   * identifier - and only the trailing {@code <value>}, which is whatever remains after the last split, can hold a
   * quoted space. gRPC callers pass key and value as separate fields and are not subject to that tokenization at
   * all, but they go through the same stripping and coercion so a declared setting accepts and refuses the same
   * values on both transports.
   */
  public static void applySetting(final ContextConfiguration configuration, final String rawKey, final String rawValue) {
    final String key = FileUtils.getStringContent(rawKey.trim());
    final String value = FileUtils.getStringContent(rawValue.trim());

    final GlobalConfiguration setting = GlobalConfiguration.findByKey(key);
    if (setting == null) {
      configuration.setValue(key, value);
      return;
    }

    if (value.isEmpty() && setting.getType() != String.class)
      throw new IllegalArgumentException(
          "'value' must not be empty for setting '" + setting.getKey() + "' of type " + setting.getType().getSimpleName());

    // setValue also runs the side effect of a declared SCOPE.SERVER setting, so one whose effect is not a value
    // somebody later reads - arcadedb.server.logFormat swapping the console formatter - takes effect here too
    // rather than being stored and ignored (issue #7121).
    configuration.setValue(setting.getKey(), setting.coerceFromAdminCommand(value));
  }

  // ---------------------------------------------------------------------------------------------
  // Security
  // ---------------------------------------------------------------------------------------------

  /**
   * The users known to this server, without their password hashes - the projection
   * {@code GET /api/v1/server/users} returns.
   */
  public JSONArray listUsers() {
    final ServerSecurity security = server.getSecurity();
    final JSONArray usersArray = new JSONArray();

    for (final JSONObject userJson : security.usersToJSON()) {
      final JSONObject userInfo = new JSONObject();
      userInfo.put("name", userJson.getString("name"));
      // Never expose password hashes
      if (userJson.has("databases"))
        userInfo.put("databases", userJson.getJSONObject("databases"));
      else
        userInfo.put("databases", new JSONObject());
      usersArray.put(userInfo);
    }

    return usersArray;
  }

  /**
   * Creates a user from the same JSON document the HTTP {@code create user} command takes:
   * {@code name}, {@code password} and an optional {@code databases} map. The document is copied
   * before the password is replaced by its hash, so a caller's object is not mutated.
   */
  public void createUser(final JSONObject user) {
    if (!user.has("name"))
      throw new IllegalArgumentException("User name is null");

    final JSONObject json = new JSONObject(user.toString());

    final String userPassword = json.getString("password");

    // Enforce the single shared credentials policy (min length 8, correct message) used by the REST
    // create-user path, instead of a divergent off-by-one length check.
    server.getSecurity().getCredentialsValidator().validateCredentials(json.getString("name"), userPassword);

    json.put("password", server.getSecurity().encodePassword(userPassword));


    // The HA-vs-local decision, and the read-compute-submit serialisation it needs, live in ServerSecurity
    // so the REST /api/v1/server/users handlers replicate through exactly the same path (issue #6808).
    server.getSecurity().createUserClusterWide(json);
  }

  public void dropUser(final String userName) {
    if (userName.isEmpty())
      throw new IllegalArgumentException("User name was missing");


    if (!server.getSecurity().dropUserClusterWide(userName))
      throw new IllegalArgumentException("User '" + userName + "' not found on server");
  }

  /**
   * Applies a partial update to an existing user, the operation {@code PUT /server/users} performs.
   * <p>
   * {@code newPassword} and {@code newDatabases} are independent and each may be null, meaning "leave
   * this part of the user alone". That is the distinction the HTTP body draws by omitting a key, and
   * it matters: an update that always rewrote both would let a password change silently drop the
   * user's grants. A non-null-but-empty {@code newDatabases} does clear them - that is a caller
   * saying so, not a caller staying silent.
   * <p>
   * The update is composed onto a <b>copy</b> of the stored user, so a failure part way through
   * leaves the live object untouched, and is applied through
   * {@link ServerSecurity#updateUserClusterWide} so an HA cluster converges on it (issue #6808).
   *
   * @throws NotFoundException        when no user by that name exists
   * @throws IllegalArgumentException when the new password violates the length policy
   */
  public void updateUser(final String userName, final String newPassword, final JSONObject newDatabases) {
    if (userName == null || userName.isBlank())
      throw new IllegalArgumentException("User name was missing");

    final ServerSecurity security = server.getSecurity();

    final ServerSecurityUser existingUser = security.getUser(userName);
    if (existingUser == null)
      throw new NotFoundException("User '" + userName + "' not found");

    final JSONObject updatedConfig = existingUser.toJSON().copy();

    if (newPassword != null) {
      validatePasswordLength(newPassword);
      updatedConfig.put("password", security.encodePassword(newPassword));
    }

    if (newDatabases != null)
      updatedConfig.put("databases", newDatabases);

    security.updateUserClusterWide(updatedConfig);
  }

  /**
   * The password policy {@code PUT /server/users} has always applied to an update. It is deliberately
   * NOT {@code CredentialsValidator}, which {@link #createUser} uses: the validator also checks the
   * password against the user name, and this method is reached with the name of a user that already
   * exists, so routing an update through it would start refusing passwords that creation accepted.
   * Changing that is a policy decision, not a refactor, so the bound stays where it was.
   */
  private static void validatePasswordLength(final String password) {
    if (password.length() < 8)
      throw new IllegalArgumentException("User password must be at least 8 characters");
    if (password.length() > 256)
      throw new IllegalArgumentException("User password cannot be longer than 256 characters");
  }

  // ---------------------------------------------------------------------------------------------
  // Security: groups
  // ---------------------------------------------------------------------------------------------

  /**
   * The whole group document, as {@code GET /server/groups} returns it. It carries no secret: a group
   * is a set of permissions, and the users holding it are named in the user documents, not here.
   */
  public JSONObject listGroups() {
    return server.getSecurity().groupsToJSON();
  }

  /**
   * Creates or replaces one group and refreshes the permissions of every open database it applies to.
   * <p>
   * The refresh is the half that is easy to leave out and impossible to notice from the response: the
   * group document on disk is the durable state, but each open {@code DatabaseInternal} caches the
   * permissions derived from it, so without {@link ServerSecurity#updateSchema} a saved group takes
   * effect only for databases opened afterwards. It lives here rather than in the HTTP handler for
   * exactly that reason - a second transport calling only {@code saveGroup} would have written the
   * file and changed nothing.
   *
   * @param groupConfig the group definition, replacing any existing one of the same name. Missing
   *                    keys are defaulted the way the HTTP body defaults them.
   */
  public void saveGroup(final String database, final String name, final JSONObject groupConfig) {
    if (database == null || database.isBlank())
      throw new IllegalArgumentException("Database name is required");
    if (name == null || name.isBlank())
      throw new IllegalArgumentException("Group name is required");

    final JSONObject normalized = new JSONObject();
    normalized.put("resultSetLimit", groupConfig.getLong("resultSetLimit", -1L));
    normalized.put("readTimeout", groupConfig.getLong("readTimeout", -1L));
    normalized.put("access", groupConfig.has("access") ? groupConfig.getJSONArray("access") : new JSONArray());
    normalized.put("types", groupConfig.has("types") ? groupConfig.getJSONObject("types") : new JSONObject());

    server.getSecurity().saveGroupClusterWide(database, name, normalized);
    refreshPermissionsOf(database);
  }

  /**
   * Drops a group and refreshes the permissions of every open database it applied to.
   *
   * @throws IllegalArgumentException when asked for the {@code admin} group of the default
   *                                  {@code "*"} database, which every deployment relies on
   * @throws NotFoundException        when no such group exists on that database
   */
  public void deleteGroup(final String database, final String name) {
    if (database == null || database.isBlank())
      throw new IllegalArgumentException("Database parameter is required");
    if (name == null || name.isBlank())
      throw new IllegalArgumentException("Group name parameter is required");

    if ("admin".equals(name) && "*".equals(database))
      throw new IllegalArgumentException("Cannot delete the admin group from the default (*) database");

    if (!server.getSecurity().deleteGroupClusterWide(database, name))
      throw new NotFoundException("Group '" + name + "' not found in database '" + database + "'");

    refreshPermissionsOf(database);
  }

  /**
   * Re-derives the cached permissions of every open database the group change applies to - all of
   * them when the change was made against {@code "*"}.
   */
  private void refreshPermissionsOf(final String database) {
    final ServerSecurity security = server.getSecurity();
    for (final String databaseName : server.getDatabaseNames()) {
      if ("*".equals(database) || databaseName.equals(database))
        security.updateSchema((DatabaseInternal) server.getDatabase(databaseName));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Security: API tokens
  // ---------------------------------------------------------------------------------------------

  /**
   * The issued tokens, in the projection {@code GET /server/api-tokens} returns: each token's
   * metadata, its SHA-256 hash - the handle needed to revoke it - and its last four characters.
   * <p>
   * The projection is explicit, field by field, rather than a copy of the stored document with the
   * secret removed. The stored document happens to hold no plaintext today, but a listing built by
   * subtraction is one added field away from disclosing one, and this list is the thing an operator
   * reads.
   */
  public JSONArray listApiTokens() {
    final JSONArray result = new JSONArray();
    for (final JSONObject token : server.getSecurity().getApiTokenConfiguration().listTokens()) {
      final JSONObject entry = new JSONObject();
      entry.put("name", token.getString("name"));
      entry.put("database", token.getString("database"));
      entry.put("expiresAt", token.getLong("expiresAt", 0));
      entry.put("createdAt", token.getLong("createdAt", 0));
      entry.put("permissions", token.getJSONObject("permissions"));
      entry.put("tokenHash", token.getString("tokenHash"));
      entry.put("tokenSuffix", token.getString("tokenSuffix", ""));
      result.put(entry);
    }
    return result;
  }

  /**
   * Mints an API token and returns it <b>including the plaintext token under {@code "token"}</b>. The
   * server keeps only the hash, so this is the one and only time that value exists outside the
   * caller's hands.
   * <p>
   * <b>This method performs no transport check.</b> It cannot: it does not know how the caller
   * arrived. Deciding whether the answer may be written back is the transport's job. gRPC does it,
   * through {@code GrpcTransportSecurityInterceptor}: the mint is refused unless the call arrived
   * over TLS or from a loopback peer. <b>HTTP does not</b> - {@code POST /server/api-tokens} mints a
   * token over a cleartext listener to any host, as it always has - which is filed as issue #7372
   * rather than changed here, because tightening a route that already behaves this way is a
   * compatibility decision and not part of adding a second transport.
   *
   * A blank {@code database} means {@code "*"}, every database. That is what an omitted key has always
   * meant over HTTP; it now also covers an explicitly empty one, which previously stored a token scoped
   * to the database named "" - a scope no database can match, so the token could never have been used.
   * gRPC needs the same normalization for a different reason: an unset proto3 string is "" and cannot
   * be told from an omitted one.
   *
   * @throws IllegalArgumentException when the name is missing or the permission document is malformed
   * @throws AlreadyExistsException   when a token of that name has already been issued
   */
  public JSONObject createApiToken(final String name, final String database, final long expiresAt,
      final JSONObject permissions) {
    if (name == null || name.isBlank())
      throw new IllegalArgumentException("Token name is required");

    final JSONObject effectivePermissions = permissions != null ? permissions : new JSONObject();

    final String validationError = validateTokenPermissions(effectivePermissions);
    if (validationError != null)
      throw new IllegalArgumentException(validationError);

    final String effectiveDatabase = database == null || database.isBlank() ? "*" : database;

    try {
      return server.getSecurity()
          .createApiTokenClusterWide(name, effectiveDatabase, expiresAt, effectivePermissions);
    } catch (final IllegalArgumentException e) {
      // ApiTokenConfiguration.createToken raises this for exactly one reason - a duplicate name - and
      // that is a conflict (409 / ALREADY_EXISTS), not a malformed request. Re-typing it here keeps
      // both transports from having to tell the two apart by matching on the message text.
      throw new AlreadyExistsException(e.getMessage());
    }
  }

  /**
   * Revokes a token by its SHA-256 hash.
   *
   * @throws IllegalArgumentException when handed a plaintext token instead of a hash. Accepting one
   *                                  would put live token material into whatever logged the call,
   *                                  which is the exposure the revocation is trying to end.
   * @throws NotFoundException        when no token has that hash
   */
  public void deleteApiToken(final String tokenHash) {
    if (tokenHash == null || tokenHash.isBlank())
      throw new IllegalArgumentException("Token hash parameter is required");

    if (ApiTokenConfiguration.isApiToken(tokenHash))
      throw new IllegalArgumentException("Use token hash (from list endpoint) instead of plaintext token for deletion");

    if (!server.getSecurity().deleteApiTokenClusterWide(tokenHash))
      throw new NotFoundException("Token not found");
  }

  private static final Set<String> VALID_TOKEN_ACCESS_VALUES = Set.of(
      "createRecord", "readRecord", "updateRecord", "deleteRecord");

  /**
   * Checks the shape of a token's permission document, returning the complaint or null. It is a
   * shape check, not a semantic one: an unknown type name is allowed - the type may be created later -
   * while an access verb outside the four the engine defines is not, because it would silently grant
   * nothing.
   */
  private static String validateTokenPermissions(final JSONObject permissions) {
    if (permissions.has("types")) {
      if (!(permissions.get("types") instanceof final JSONObject types))
        return "'permissions.types' must be a JSON object";

      for (final String typeName : types.keySet()) {
        if (!(types.get(typeName) instanceof final JSONObject typeObj))
          return "'permissions.types." + typeName + "' must be a JSON object";

        if (typeObj.has("access")) {
          if (!(typeObj.get("access") instanceof final JSONArray access))
            return "'permissions.types." + typeName + ".access' must be a JSON array";

          for (int i = 0; i < access.length(); i++) {
            final String value = access.getString(i);
            if (!VALID_TOKEN_ACCESS_VALUES.contains(value))
              return "Invalid access value '" + value + "' in permissions.types." + typeName
                  + ". Valid values: " + VALID_TOKEN_ACCESS_VALUES;
          }
        }
      }
    }

    if (permissions.has("database") && !(permissions.get("database") instanceof JSONArray))
      return "'permissions.database' must be a JSON array";

    return null;
  }

  /**
   * Raised when the operation names something that does not exist - a user, a group, a token. HTTP
   * answers it 404 and gRPC {@code NOT_FOUND}; both carry this message verbatim.
   * <p>
   * It is a distinct type rather than an {@link IllegalArgumentException} because the two mean
   * different things to a caller: a malformed argument is worth fixing and retrying, an absent target
   * is not.
   */
  public static class NotFoundException extends RuntimeException {
    public NotFoundException(final String message) {
      super(message);
    }
  }

  /**
   * Raised when the operation would create something whose identity is already taken. HTTP answers it
   * 409 and gRPC {@code ALREADY_EXISTS}.
   */
  public static class AlreadyExistsException extends RuntimeException {
    public AlreadyExistsException(final String message) {
      super(message);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Backup
  // ---------------------------------------------------------------------------------------------

  public JSONObject getBackupConfig() {

    final AutoBackupSchedulerPlugin plugin = getBackupPlugin();

    final JSONObject response = new JSONObject();
    // The directory every backup command reads and writes, whichever source it came from (issue #7392). A directory
    // that fails validation is reported rather than thrown: this is the command an operator uses to see what is
    // wrong before fixing it with 'set backup config'.
    try {
      response.put("backupDirectory", resolveBackupDirectory().toString());
    } catch (final IllegalArgumentException e) {
      response.put("backupDirectoryError", e.getMessage());
    }

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
        } catch (final IOException | JSONException e) {
          // Unreadable or malformed: this is the command the operator uses to find out, so answer with the reason
          // rather than an internal error.
          response.put("enabled", false);
          response.put("config", JSONObject.NULL);
          response.put("message", "Cannot read " + configPath + ": " + e.getMessage());
        }
      } else {
        response.put("enabled", false);
        response.put("config", JSONObject.NULL);
      }
    }

    return response;
  }

  public JSONObject setBackupConfig(final JSONObject configJson) throws IOException {

    // Validate backup directory - must be relative path without traversal
    if (configJson.has("backupDirectory"))
      validateBackupDirectory(configJson.getString("backupDirectory"));

    // Save configuration to file
    final Path configPath = Paths.get(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);

    // Write configuration atomically so a crash mid-write leaves the previous valid file intact.
    // atomicWriteFile also creates the parent config directory if needed.
    FileUtils.atomicWriteFile(configPath.toFile(), configJson.toString(2));

    // Reload configuration in the plugin
    final AutoBackupSchedulerPlugin plugin = getBackupPlugin();
    if (plugin != null && plugin.isEnabled())
      plugin.reloadConfiguration();

    return new JSONObject().put("result", "ok");
  }

  /**
   * The archives of {@code databaseName} under {@link #resolveBackupDirectory() the backup directory}, newest
   * first, with the count and the total size of the same files. Both were previously read off the scheduler's
   * retention manager and left out when the plugin was off, so the listing carried a count only when the
   * scheduler was running (issue #7392).
   */
  public JSONObject listBackups(final String databaseName) {
    requireBackupDatabaseName(databaseName);

    final JSONArray backups = new JSONArray();
    long totalSize = 0;

    final Path dbBackupDir = resolveBackupDirectory().resolve(databaseName);
    if (Files.isDirectory(dbBackupDir)) {
      try (final var stream = Files.list(dbBackupDir)) {
        for (final Path p : stream.filter(ServerControlPlane::isBackupArchive).sorted(Comparator.reverseOrder()).toList()) {
          final JSONObject backup = new JSONObject();
          backup.put("fileName", p.getFileName().toString());
          try {
            final long size = Files.size(p);
            totalSize += size;
            backup.put("size", size);
            backup.put("lastModified", Files.getLastModifiedTime(p).toMillis());
          } catch (final IOException e) {
            backup.put("size", 0);
            backup.put("lastModified", 0);
          }

          // Parse timestamp from filename, through the same convention that wrote it (issue #6753)
          final LocalDateTime timestamp = BackupCoordinator.parseArchiveTimestamp(p.getFileName().toString());
          backup.put("timestamp", timestamp != null ? timestamp.toString() : JSONObject.NULL);

          backups.put(backup);
        }
      } catch (final IOException e) {
        throw new RuntimeException("Error listing backups for database '" + databaseName + "'", e);
      }
    }

    final JSONObject response = new JSONObject();
    response.put("database", databaseName);
    response.put("backups", backups);
    response.put("totalSize", totalSize);
    response.put("totalCount", backups.length());
    return response;
  }

  /**
   * The database name is a path segment under the backup directory for every backup command, and {@code trigger}
   * creates that directory, so a name with a separator or a {@code ..} must be refused before any path is built
   * rather than caught by the file-level {@code startsWith} check that only guards the archive name.
   */
  private void requireBackupDatabaseName(final String databaseName) {
    requireDatabaseName(databaseName);
    server.checkDatabaseNameIsValid(databaseName);
  }

  private static boolean isBackupArchive(final Path p) {
    final String name = p.getFileName().toString();
    return name.endsWith(".zip") && name.contains("-backup-");
  }

  /**
   * Runs a full backup inline and returns {@code {"result":"ok","backupFile":...}}.
   *
   * @throws BackupInProgressException    when another backup of the same database is already running.
   *                                      This command runs the backup inline, so it is one of the entry points that
   *                                      can have a database being backed up at the same time as the auto-backup
   *                                      schedule does - down to resolving to the same archive name and writing into
   *                                      the same file. Refusing outright is the honest answer to "back up a database
   *                                      that is already being backed up": a second full backup of the same data
   *                                      produces nothing the first one will not, and the caller gets told rather
   *                                      than silently handed the other run's archive (issue #6753).
   * @throws OperationInProgressException when a restore of the same database is running instead. A restore drops and
   *                                      replaces the database directory, so backing it up at the same time reads a
   *                                      directory that is about to be deleted (issue #7384).
   */
  public JSONObject triggerBackup(final String databaseName) {
    requireBackupDatabaseName(databaseName);

    final BackupCoordinator coordinator = server.getBackupCoordinator();
    final Operation running = coordinator.begin(databaseName, Operation.BACKUP);
    if (running != null)
      // The pre-#7384 message, verbatim, for the pre-#7384 case: only a restore holding the slot is new.
      throw running == Operation.BACKUP ?
          new BackupInProgressException("A backup of database '" + databaseName + "' is already in progress") :
          new OperationInProgressException(refusal(Operation.BACKUP, databaseName, running));

    try {
      return executeImmediateBackup(databaseName);
    } finally {
      coordinator.end(databaseName, Operation.BACKUP);
    }
  }

  /**
   * Takes the per-database slot for {@code operation}, or refuses naming the operation that already holds it.
   * <p>
   * Every caller must release it with {@link BackupCoordinator#end(String, Operation)} from a {@code finally}: a
   * leaked reservation blocks every later backup, restore and import of that database until the server restarts.
   */
  private void beginExclusive(final String databaseName, final Operation operation) {
    final Operation running = server.getBackupCoordinator().begin(databaseName, operation);
    if (running != null)
      throw new OperationInProgressException(refusal(operation, databaseName, running));
  }

  private static String refusal(final Operation refused, final String databaseName, final Operation running) {
    return "Cannot " + refused.verb() + " database '" + databaseName + "': " + running.phrase()
        + " of it is already in progress";
  }

  public JSONObject deleteBackup(final String databaseName, final String fileName) {
    if (databaseName.isEmpty() || fileName.isEmpty())
      throw new IllegalArgumentException("Usage: delete backup <database> <fileName>");


    final Path backupFile = resolveBackupFile(databaseName, fileName);

    try {
      Files.delete(backupFile);
    } catch (final IOException e) {
      throw new CommandExecutionException("Error deleting backup file '" + fileName + "'", e);
    }

    return new JSONObject().put("result", "ok");
  }

  public void validateBackupDirectory(final String backupDir) {
    // Use consolidated validation from AutoBackupSchedulerPlugin
    final Path serverRoot = Paths.get(server.getRootPath()).toAbsolutePath().normalize();
    AutoBackupSchedulerPlugin.validateAndResolveBackupPath(backupDir, serverRoot);
  }

  /**
   * Resolves a backup file name to an absolute path inside {@link #resolveBackupDirectory() the backup directory}
   * for the given database, rejecting any name that contains path separators or traversal sequences. Used by
   * {@code delete backup} and {@code restore backup}, so both can address exactly what {@code trigger backup}
   * wrote (issue #7392).
   */
  public Path resolveBackupFile(final String databaseName, final String fileName) {
    requireBackupDatabaseName(databaseName);

    // Reject anything that is not a plain backup file name.
    if (fileName.contains("/") || fileName.contains("\\") || fileName.contains("..") || fileName.isBlank()
        || !fileName.endsWith(".zip") || !fileName.contains("-backup-"))
      throw new IllegalArgumentException("Invalid backup file name: " + fileName);

    final Path dbBackupDir = resolveBackupDirectory().resolve(databaseName).normalize();
    final Path resolved = dbBackupDir.resolve(fileName).normalize();

    // Defence in depth: the resolved file must still live inside the database backup directory.
    if (!resolved.startsWith(dbBackupDir))
      throw new IllegalArgumentException("Invalid backup file path");

    if (!Files.exists(resolved) || !Files.isRegularFile(resolved))
      throw new IllegalArgumentException("Backup file not found: " + fileName);

    return resolved;
  }

  private JSONObject executeImmediateBackup(final String databaseName) {
    final Path dbBackupPath = resolveBackupDirectory().resolve(databaseName);
    try {
      // Perform backup using reflection (same as BackupTask)
      final Database database = server.getDatabase(databaseName);
      final Class<?> clazz = Class.forName("com.arcadedb.integration.backup.Backup");

      final String backupFileName = server.getBackupCoordinator().newArchiveName(databaseName);

      // Use Files.createDirectories to avoid TOCTOU race condition
      Files.createDirectories(dbBackupPath);

      final Object backup = clazz.getConstructor(Database.class, String.class)
          .newInstance(database, backupFileName);
      clazz.getMethod("setDirectory", String.class).invoke(backup, dbBackupPath.toString());
      clazz.getMethod("setVerboseLevel", Integer.TYPE).invoke(backup, 1);

      final String backupFile = (String) clazz.getMethod("backupDatabase").invoke(backup);

      final JSONObject response = new JSONObject();
      response.put("result", "ok");
      response.put("backupFile", backupFile);
      return response;

    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Backup libs not found in classpath. Make sure arcadedb-integration module is included.", e);
    } catch (final Exception e) {
      final Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new RuntimeException("Error triggering backup for database '" + databaseName + "': " + cause.getMessage(), cause);
    }
  }

  /**
   * Where this server keeps its backups: the one definition every backup command - {@code trigger}, {@code list},
   * {@code delete}, {@code restore} and {@code get backup config} - resolves through, so an archive one of them
   * writes the others can see. Before this, only {@code trigger backup} fell back past the live scheduler, so an
   * on-demand archive taken with auto-backup off was invisible to {@code list}, refused by {@code delete} and
   * {@code restore}, and left on disk for good (issue #7392).
   * <p>
   * The chain, first hit wins:
   * <ol>
   *   <li>the running auto-backup plugin's configuration;</li>
   *   <li>{@code config/backup.json} on disk, whether or not it enables the scheduler - an operator who set a
   *       directory there and turned the schedule off still means that directory;</li>
   *   <li>{@link GlobalConfiguration#SERVER_BACKUP_DIRECTORY}, the server-wide setting the SQL
   *       {@code BACKUP DATABASE} statement writes to.</li>
   * </ol>
   * The first two are caller-supplied through {@code set backup config} and are re-validated here as relative
   * paths inside the server root, exactly as the scheduler validates them at start-up. The third is a server
   * setting, not client input, and is taken as configured. Every archive lives under {@code <directory>/<database>}.
   * A configured directory that fails validation is an {@link IllegalArgumentException} out of every command that
   * needs it, deliberately: listing nothing for a directory the server refuses to use is what hid the archives in
   * the first place, and {@link #getBackupConfig()} reports the same failure as {@code backupDirectoryError} so the
   * operator can see it without triggering anything.
   * <p>
   * Retention pruning is the scheduler's job and runs only while it is enabled; with the scheduler off an
   * on-demand archive stays until {@code delete backup} removes it, which is now possible.
   */
  public Path resolveBackupDirectory() {
    final Path serverRoot = Paths.get(server.getRootPath()).toAbsolutePath().normalize();

    final AutoBackupSchedulerPlugin plugin = getBackupPlugin();
    if (plugin != null && plugin.isEnabled() && plugin.getBackupConfig() != null) {
      final String configured = plugin.getBackupConfig().getBackupDirectory();
      if (configured != null && !configured.isBlank())
        return AutoBackupSchedulerPlugin.validateAndResolveBackupPath(configured, serverRoot);
    }

    final Path configPath = Paths.get(server.getRootPath(), "config", AutoBackupConfig.CONFIG_FILE_NAME);
    if (Files.exists(configPath)) {
      try {
        final String configured = new JSONObject(Files.readString(configPath)).getString("backupDirectory", null);
        if (configured != null && !configured.isBlank())
          return AutoBackupSchedulerPlugin.validateAndResolveBackupPath(configured, serverRoot);
      } catch (final IOException | JSONException e) {
        // Unreadable or malformed: the scheduler ignores such a file at start-up too, so fall through to the server
        // setting with a warning rather than answer every backup command with an internal error.
        LogManager.instance().log(this, Level.WARNING, "Cannot read '%s', falling back to the server backup directory: %s",
            configPath, e.getMessage());
      }
    }

    return Paths.get(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_BACKUP_DIRECTORY))
        .toAbsolutePath().normalize();
  }

  private AutoBackupSchedulerPlugin getBackupPlugin() {
    for (final ServerPlugin plugin : server.getPlugins()) {
      if (plugin instanceof AutoBackupSchedulerPlugin autoBackup)
        return autoBackup;
    }
    return null;
  }

  /**
   * Raised when the operation cannot run in this server's current configuration at all - HA is not
   * enabled, or the HA implementation does not support joining a node this way - as opposed to having
   * been attempted and failed.
   * <p>
   * It extends {@link CommandExecutionException} so the HTTP protocol answers it exactly as it did
   * before this type existed: a 500, through the {@code CommandExecutionException} arm of
   * {@code AbstractServerHttpHandler}. gRPC needs a distinction the HTTP protocol does not draw -
   * this is its {@code FAILED_PRECONDITION}, while every other {@code CommandExecutionException}
   * raised here, such as a backup archive that could not be deleted, is a server-side fault and stays
   * {@code INTERNAL}.
   */
  public static class OperationNotAvailableException extends CommandExecutionException {
    public OperationNotAvailableException(final String message) {
      super(message);
    }
  }

  /**
   * Raised by {@link #validateClientRestoreImportUrl(String)} when a caller-supplied restore or
   * import URL is not one this server will fetch - the SSRF and local-file guard.
   * <p>
   * A {@link SecurityException} so that HTTP keeps answering it 403, which is what
   * {@code AbstractServerHttpHandler} does with every security failure. A <i>distinct</i> one so
   * gRPC can answer {@code PERMISSION_DENIED} rather than the {@code UNAUTHENTICATED} it uses for a
   * failed login: the caller's credentials are fine and re-sending them will not help, it is the URL
   * that is refused (issue #7308).
   */
  public static class RestoreImportUrlNotAllowedException extends SecurityException {
    public RestoreImportUrlNotAllowedException(final String message) {
      super(message);
    }
  }

  /**
   * Raised when a backup, restore or import of a database is refused because another whole-database
   * operation on it is already running - the per-database slot {@link BackupCoordinator} hands out.
   * HTTP answers it with a 409 and gRPC with {@code ABORTED}; both carry this message verbatim.
   * <p>
   * The request is well formed and authorized, and retrying once the other operation finishes is the
   * fix, which is what separates it from every other refusal these commands can produce (issue #7384).
   */
  public static class OperationInProgressException extends RuntimeException {
    public OperationInProgressException(final String message) {
      super(message);
    }
  }

  /**
   * The {@link OperationInProgressException} {@link #triggerBackup(String)} raises when it is another
   * <i>backup</i> holding the slot - the only case that existed before restores took one too. Kept as
   * its own type, and kept being thrown for that case, so code written against it before #7384 still
   * catches what it always caught.
   */
  public static class BackupInProgressException extends OperationInProgressException {
    public BackupInProgressException(final String message) {
      super(message);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Query profiler
  // ---------------------------------------------------------------------------------------------

  /**
   * Starts recording, for {@code timeoutSec} seconds or - when that is not positive, which is what the HTTP
   * {@code profiler start} command sends when it carries no timeout - for the profiler's own default.
   * <p>
   * That default is <b>not</b> "until stopped": {@link ServerQueryProfiler#start(int)} substitutes 60 seconds
   * and arms an auto-stop timer, and this javadoc used to claim the opposite (issue #7394). Rather than remove
   * the bound - a recording nobody stops keeps every server query on the {@code ProfilingResultSet} wrapping
   * path - the response reports the timeout that will actually apply, so neither transport has to know the
   * default to tell a caller when its recording ends.
   *
   * @return {@code result}, {@code recording}, and {@code timeoutSeconds}: the effective bound, which is the
   * live recording's own when a recording was already running (a second start is a no-op)
   */
  public JSONObject profilerStart(final int timeoutSec) {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    if (timeoutSec > 0)
      profiler.start(timeoutSec);
    else
      profiler.start();

    return new JSONObject().put("result", "ok").put("recording", true)
        .put("timeoutSeconds", profiler.getTimeoutSeconds());
  }

  public JSONObject profilerStop() {
    final JSONObject results = server.getQueryProfiler().stop();
    return results != null ? results : new JSONObject().put("result", "ok");
  }

  public JSONObject profilerReset() {
    server.getQueryProfiler().reset();
    return new JSONObject().put("result", "ok");
  }

  public JSONObject profilerResults() {
    final JSONObject results = server.getQueryProfiler().getResults();
    return results != null ? results : new JSONObject().put("result", "ok");
  }

  public JSONArray profilerList() {
    return server.getQueryProfiler().listSavedRuns();
  }

  public JSONObject profilerLoad(final String fileName) {
    return server.getQueryProfiler().loadSavedRun(fileName);
  }

  // ---------------------------------------------------------------------------------------------
  // Restore and import
  // ---------------------------------------------------------------------------------------------

  /**
   * A server-side restore is three phases, not one, and the middle and last are not cheap: the swap drops the
   * database the restore replaces, and in HA the replication makes every replica pull the restored files. They
   * are published as separate steps so the progress endpoint keeps saying something after the archive has been
   * extracted (issue #7385).
   */
  private static final int    RESTORE_STEPS          = 3;
  /**
   * Deliberately a copy of {@code AbstractRestoreFormat.RESTORE_STEP_NAME} rather than a reference to it:
   * {@code arcadedb-integration} is an optional dependency reached only reflectively, so the server cannot name
   * its constants at compile time. The two only have to agree so that the marker published before the restorer
   * exists reads the same as the reports the restorer then sends; a drift costs a changed label mid-step and
   * nothing more.
   */
  private static final String RESTORE_STEP_EXTRACT   = "Restoring files";
  private static final String RESTORE_STEP_ACTIVATE  = "Activating database";
  private static final String RESTORE_STEP_REPLICATE = "Replicating to the cluster";

  /**
   * The sink a transport supplies for the progress of a long-running control-plane operation
   * (issue #7308). The HTTP handler turns each event into an SSE frame on the {@code
   * HttpServerExchange}; {@code ArcadeDbGrpcAdminService} turns it into a message on a
   * server-streaming RPC. The operation itself knows about neither.
   * <p>
   * <b>Implementations must tolerate concurrent calls.</b> More than one thread reports here: an
   * import polls {@code ImporterContext} for counters on a timer thread while the importer logs on
   * the calling thread, and since issue #6086 a parallel restore logs one line per archive entry
   * from its worker pool.
   * <p>
   * There is no {@code onError}: a failure is thrown, not reported, so that a transport cannot
   * quietly turn one into a success. Each transport decides what a thrown failure looks like on the
   * wire.
   */
  public interface ProgressListener {
    /** A free-form progress line produced by the underlying restore or import machinery. */
    void onProgress(String message);

    /**
     * Structured import counters, sampled once a second while an import runs. Never called by a
     * restore, which has no equivalent counters to report.
     */
    default void onImportCounters(final long parsed, final long vertices, final long edges) {
      // opt-in: a transport that only renders log lines ignores these
    }

    /** Discards every event, for a caller that wants only the final outcome. */
    ProgressListener NOOP = message -> {
    };
  }

  /**
   * Restores a database from a backup archive at {@code url}, under the name {@code databaseName}.
   * The caller supplies the URL, so it is validated against
   * {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} first.
   * <p>
   * A target name that is already taken - see {@link #databaseNameIsTaken} - is refused outright:
   * this command has no {@code overwrite} flag, so there is no way for the caller to say they meant
   * it. Use {@code restore backup ... as &lt;name&gt;} with {@code overwrite}, or drop the database
   * first.
   * <p>
   * Serialised against every other backup, restore and import of the same database by the per-database
   * slot {@link BackupCoordinator} hands out. The slot is taken <b>before</b> the existence pre-check,
   * which is the only thing that made the check meaningful: two restores could both pass it, both
   * restore into their own temporary directory and both reach the swap, and the loser's caller was
   * still told it had succeeded (issue #7384).
   *
   * @throws IllegalArgumentException     when the name is invalid or the database already exists
   * @throws SecurityException            when the URL is not one this server accepts from a client
   * @throws OperationInProgressException when a backup, restore or import of the same database is already running
   */
  public void restoreDatabase(final String databaseName, final String url, final ProgressListener listener) {
    if (databaseName == null || databaseName.isEmpty() || url == null || url.isEmpty())
      throw new IllegalArgumentException("Usage: restore database <name> <url>");

    validateClientRestoreImportUrl(url);

    // Prevent path traversal via the caller-supplied database name (GHSA-qwgr-2c45-63xx).
    server.checkDatabaseNameIsValid(databaseName);

    beginExclusive(databaseName, Operation.RESTORE);
    try {
      final String dbPath = databaseDirectory(databaseName);
      if (databaseNameIsTaken(databaseName, dbPath))
        throw new IllegalArgumentException("Database '" + databaseName + "' already exists");

      performRestore(databaseName, dbPath, url, "restore database", listener);
    } finally {
      server.getBackupCoordinator().end(databaseName, Operation.RESTORE);
    }
  }

  /**
   * Restores a backup archive that this server produced into {@code targetDatabase}. The archive is
   * resolved server-side out of the configured auto-backup directory by
   * {@link #resolveBackupFile}, so the caller never supplies a filesystem path and the resulting
   * {@code file://} URL needs no SSRF check.
   * <p>
   * {@code overwrite} decides what happens when the target name is already taken, as
   * {@link #databaseNameIsTaken} defines it. Even with it set, the existing database is dropped only
   * once the restore into a temporary directory has succeeded (issue #5027).
   * <p>
   * The slot is taken on the <b>target</b>, which is the database this writes, and covers the
   * overwrite pre-check as well as the restore itself (issue #7384). The source is only read - its
   * archive is a file this server wrote, and a concurrent backup of it writes a different archive -
   * so it is not reserved, which also keeps this from needing a lock order between two names.
   *
   * @throws IllegalArgumentException     when a name is invalid, or the target exists and {@code overwrite} is false
   * @throws OperationInProgressException when a backup, restore or import of the target is already running
   */
  public void restoreBackup(final String databaseName, final String fileName, final String targetDatabase,
      final boolean overwrite, final ProgressListener listener) {
    if (databaseName == null || databaseName.isEmpty() || fileName == null || fileName.isEmpty()
        || targetDatabase == null || targetDatabase.isEmpty())
      throw new IllegalArgumentException("Usage: restore backup <database> <fileName> as <targetDatabase>");

    // The target is a caller-supplied name that becomes a directory under SERVER_DATABASE_DIRECTORY,
    // exactly as 'restore database' name is; validating only the latter left this one able to escape
    // the directory (issue #7308).
    server.checkDatabaseNameIsValid(targetDatabase);

    final Path backupFile = resolveBackupFile(databaseName, fileName);

    beginExclusive(targetDatabase, Operation.RESTORE);
    try {
      final String dbPath = databaseDirectory(targetDatabase);
      if (databaseNameIsTaken(targetDatabase, dbPath) && !overwrite)
        throw new IllegalArgumentException(
            "Database '" + targetDatabase + "' already exists. Enable overwrite to replace it with the backup");

      performRestore(targetDatabase, dbPath, "file://" + backupFile.toAbsolutePath(), "restore backup", listener);
    } finally {
      server.getBackupCoordinator().end(targetDatabase, Operation.RESTORE);
    }
  }

  /**
   * The single question both restore entry points ask about their target: is the name already taken
   * on this server? It is taken when the server has a database of that name registered <b>or</b> when
   * a directory of that name is present under {@code SERVER_DATABASE_DIRECTORY}.
   * <p>
   * The two halves are independent. A registered database whose directory has gone - removed out of
   * band, or dropped through the embedded instance without {@link ArcadeDBServer#removeDatabase},
   * which {@link #dropQuietly} has to call as a separate second step - satisfies the first and not
   * the second. A directory left behind by a half-finished operation satisfies the second and not the
   * first.
   * <p>
   * Until issue #7395 the two commands disagreed here: {@code restore backup} asked both halves and
   * {@code restore database} only the filesystem, so a registered-but-absent database was "not there"
   * to one and "there" to the other. Both now ask the same question, which is also the one
   * {@link ArcadeDBServer#createDatabase} already asked - the registry first, then the files.
   */
  private boolean databaseNameIsTaken(final String databaseName, final String dbPath) {
    return server.existsDatabase(databaseName) || new File(dbPath).exists();
  }

  /**
   * Creates {@code databaseName} and imports {@code url} into it in one step, returning the
   * importer's own final report. The database is created cluster-wide first, so in HA every replica
   * has it before the import's transactions start replicating; a failure to create it on the
   * replicas drops the local one again so the operator can retry cleanly.
   *
   * The import holds the per-database slot for its whole duration, so a restore cannot drop and replace
   * the directory it is loading into (issue #7384). It does <b>not</b> exclude a backup: an import is
   * ordinary transactions against a live database, and backing a live database up is exactly what the
   * auto-backup schedule does - see {@link Operation#conflictsWith}.
   *
   * @throws SecurityException            when the URL is not one this server accepts from a client
   * @throws OperationInProgressException when a restore or another import of the same database is already running
   */
  public JSONObject importDatabase(final String databaseName, final String url, final ProgressListener listener) {
    if (databaseName == null || databaseName.isEmpty() || url == null || url.isEmpty())
      throw new IllegalArgumentException("Usage: import database <name> <url>");

    // Validate BEFORE creating the database so a rejected URL leaves no empty database behind.
    validateClientRestoreImportUrl(url);

    server.checkDatabaseNameIsValid(databaseName);

    beginExclusive(databaseName, Operation.IMPORT);
    try {
      final ServerDatabase createdDb = server.createDatabase(databaseName, ComponentFile.MODE.READ_WRITE);
      if (createdDb.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb) {
        try {
          haDb.createInReplicas();
        } catch (final RuntimeException e) {
          dropQuietly(createdDb, databaseName);
          throw e;
        }
      }

      // A failed import deliberately leaves the created database in place, as both HTTP branches do:
      // dropping it would destroy whatever was imported before the failure, and the operator needs to
      // see it to decide whether to retry or to drop it.
      return runImport(createdDb, databaseName, url, listener);
    } finally {
      server.getBackupCoordinator().end(databaseName, Operation.IMPORT);
    }
  }

  /**
   * Runs the importer against an already-created database, reporting log lines and, once a second,
   * the {@code ImporterContext} counters. Reflective because the {@code arcadedb-integration}
   * module is optional on the server's classpath.
   */
  private JSONObject runImport(final Database database, final String databaseName, final String url,
      final ProgressListener listener) {
    final Timer progressTimer = new Timer("import-progress-" + databaseName, true);
    // Published to GET /api/v1/progress/{database}, the console and Studio while the import runs, as
    // ImportDatabaseStatement does for the SQL form (issue #5376). The HTTP command used to inherit
    // this only on its synchronous branch, which went through SQL; both transports get it now.
    final OperationProgress progress = OperationProgressRegistry.instance().register(databaseName, "import database");
    progress.onProgress("Importing database", 1, 1, 0, -1);
    try {
      final Class<?> clazz = Class.forName("com.arcadedb.integration.importer.Importer");
      // The outermost wrapper (HAReplicatedDatabase in HA mode), so the importer's commits are
      // intercepted for replication - the same reason ImportDatabaseStatement unwraps before handing
      // the database to the importer. Outside HA this is the database itself.
      final Database effectiveDb =
          database instanceof DatabaseInternal internal ? internal.getWrappedDatabaseInstance() : database;
      final Object importer = clazz.getConstructor(Database.class, String.class).newInstance(effectiveDb, url);
      // The same boolean validateClientRestoreImportUrl() already validated this URL against: the fetch inside
      // SourceDiscovery must agree with this server's own configuration rather than falling back to the static
      // default, or a per-server override that let the command through would still have the fetch refuse it (#6474).
      clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(importer, isRestoreImportLocalUrlsAllowed());
      clazz.getMethod("setLogger", loggerClass()).invoke(importer, progressLogger(listener));

      listener.onProgress("Importing " + databaseName + "...");

      scheduleImportCounters(progressTimer, importerContext(clazz, importer), listener);

      @SuppressWarnings("unchecked")
      final Map<String, Object> result = (Map<String, Object>) clazz.getMethod("load").invoke(importer);

      final JSONObject report = new JSONObject();
      if (result != null)
        for (final Map.Entry<String, Object> e : result.entrySet())
          report.put(e.getKey(), e.getValue());
      return report;
    } catch (final InvocationTargetException e) {
      throw new CommandExecutionException("Error importing database '" + databaseName + "'", e.getTargetException());
    } catch (final ReflectiveOperationException e) {
      throw new CommandExecutionException("Import libs not found in classpath", e);
    } finally {
      progressTimer.cancel();
      OperationProgressRegistry.instance().unregister(progress);
    }
  }

  /** The {@code ImporterContext} of a running import, or null when this importer build exposes none. */
  private static Object importerContext(final Class<?> importerClass, final Object importer) {
    try {
      return importerClass.getMethod("getContext").invoke(importer);
    } catch (final Exception ignored) {
      return null;
    }
  }

  /**
   * Samples the importer's counters once a second for as long as the import runs.
   * <p>
   * The three {@code AtomicLong}s are resolved <b>once</b>, here, rather than on every tick: neither
   * the context nor its class changes for the life of the import, so a reflective lookup per field
   * per second is work with no answer that could differ. A build whose {@code ImporterContext} does
   * not carry all three schedules nothing at all, rather than failing quietly every second.
   */
  private static void scheduleImportCounters(final Timer timer, final Object context, final ProgressListener listener) {
    if (context == null)
      return;

    final AtomicLong parsedCounter;
    final AtomicLong vertexCounter;
    final AtomicLong edgeCounter;
    try {
      final Class<?> ctxClass = context.getClass();
      parsedCounter = (AtomicLong) ctxClass.getField("parsed").get(context);
      vertexCounter = (AtomicLong) ctxClass.getField("createdVertices").get(context);
      edgeCounter = (AtomicLong) ctxClass.getField("createdEdges").get(context);
    } catch (final Exception ignored) {
      // A counter this importer build does not carry. Report nothing rather than fail the import:
      // progress is a convenience, and the import itself is what the caller asked for.
      return;
    }

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        final long parsed = parsedCounter.get();
        if (parsed > 0)
          listener.onImportCounters(parsed, vertexCounter.get(), edgeCounter.get());
      }
    }, 1000, 1000);
  }

  /**
   * The restore both {@code restore database} and {@code restore backup} run.
   * <p>
   * Restores into a temporary sibling directory first and swaps it into place only on success, so a
   * failed restore leaves an existing target database intact (issue #5027). The temp directory name
   * carries the reserved-database marker so that, if the process dies mid-restore,
   * {@code loadDatabases()} skips the orphan at next startup instead of opening it as a user
   * database.
   * <p>
   * The caller is responsible for the pre-restore existence and overwrite checks.
   *
   * @param operation the label the operation is published under - the command the operator typed, so that a
   *                  reader of the progress endpoint sees {@code restore backup} or {@code restore database}
   *                  rather than one name standing for both
   */
  private void performRestore(final String databaseName, final String dbPath, final String url,
      final String operation, final ProgressListener listener) {
    final File finalDir = new File(dbPath);
    final File tempDir = new File(finalDir.getParentFile(),
        ArcadeDBServer.RESERVED_DATABASE_PREFIX + "restore-tmp-" + databaseName + "-" + System.nanoTime());

    // Published to GET /api/v1/progress/{database}, the console and Studio for the WHOLE restore - extraction,
    // swap and replication alike - as runImport publishes the import (issue #7385). Before this, a restore that
    // ran for minutes left the endpoint reporting an idle database that was in fact being replaced. Unlike the
    // import, which has no record total to count against and can only publish a coarse marker, the restore
    // reports real counters: see restoreProgressCallback below. Always retired in the finally.
    final OperationProgress progress = OperationProgressRegistry.instance().register(databaseName, operation);
    progress.onProgress(RESTORE_STEP_EXTRACT, 1, RESTORE_STEPS, 0, -1);
    try {
      try {
        final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
        final Object restorer = clazz.getConstructor(String.class, String.class).newInstance(url, tempDir.getAbsolutePath());
        // The same boolean validateClientRestoreImportUrl() already validated this URL against: the fetch inside
        // FullRestoreFormat must agree with this server's own configuration rather than falling back to the static
        // default, or a per-server override that let the command through would still have the fetch refuse it.
        clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(restorer, isRestoreImportLocalUrlsAllowed());
        clazz.getMethod("setLogger", loggerClass()).invoke(restorer, progressLogger(listener));
        installRestoreProgressCallback(clazz, restorer, progress);

        listener.onProgress("Downloading and restoring " + databaseName + "...");
        clazz.getMethod("restoreDatabase").invoke(restorer);
      } catch (final InvocationTargetException e) {
        FileUtils.deleteRecursively(tempDir);
        throw new CommandExecutionException("Error restoring database", e.getTargetException());
      } catch (final ReflectiveOperationException e) {
        FileUtils.deleteRecursively(tempDir);
        throw new CommandExecutionException("Restore libs not found in classpath", e);
      } catch (final RuntimeException e) {
        FileUtils.deleteRecursively(tempDir);
        throw e;
      }

      progress.onProgress(RESTORE_STEP_ACTIVATE, 2, RESTORE_STEPS, 0, -1);
      swapRestoredDatabase(databaseName, finalDir, tempDir);

      // A no-op outside HA, and minutes inside it: forceSnapshot makes every replica pull the restored files.
      progress.onProgress(RESTORE_STEP_REPLICATE, 3, RESTORE_STEPS, 0, -1);
      replicateRestoredDatabase(server.getDatabase(databaseName), databaseName);
      // Completion is the transport's to announce, not this method's: HTTP writes a 'completed' SSE
      // frame, gRPC a final message with completed=true, and a synchronous caller just returns.
    } finally {
      OperationProgressRegistry.instance().unregister(progress);
    }
  }

  /**
   * Installs {@code progress} as the restorer's progress callback, renumbering the format's own step - which is
   * always 1 of 1, because the integration module knows nothing of the swap and replicate phases that follow it -
   * into step 1 of this method's three (issue #7385).
   * <p>
   * Best-effort, like {@code importerContext}: a build of {@code arcadedb-integration} without the setter reports
   * no counters rather than failing the restore. Progress is a convenience; the restore is what the caller asked
   * for.
   */
  private static void installRestoreProgressCallback(final Class<?> restoreClass, final Object restorer,
      final OperationProgress progress) {
    final ProgressCallback callback =
        (stepName, stepIndex, totalSteps, done, total) -> progress.onProgress(stepName, 1, RESTORE_STEPS, done, total);
    try {
      restoreClass.getMethod("setProgressCallback", ProgressCallback.class).invoke(restorer, callback);
    } catch (final ReflectiveOperationException ignored) {
      // No setter on this build: the coarse step markers above are still published.
    }
  }

  /**
   * Swaps a freshly-restored temporary directory into the final database directory. The existing
   * target database (if any) is dropped only now that the restore into {@code tempDir} has
   * succeeded, so a failed restore never destroys the original data (issue #5027).
   */
  private void swapRestoredDatabase(final String databaseName, final File finalDir, final File tempDir) {
    try {
      // Drop the previous target (HA-aware) BEFORE taking the registry lock and only after a
      // successful restore into tempDir. In HA mode dropDatabase() round-trips through Raft and the
      // apply thread itself acquires databasesLock, so holding that lock here would deadlock; only the
      // pure-local file swap below runs under the lock, matching the snapshot-installer pattern (#4832).
      if (server.existsDatabase(databaseName))
        dropDatabaseForRestore(databaseName);

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
   * Drops the database a restore is about to replace. HA-aware in the same way the {@code drop
   * database} command is: on a replicated database the drop is submitted through Raft and applied on
   * every peer including this one, rather than performed locally behind the cluster's back.
   */
  private void dropDatabaseForRestore(final String databaseName) {
    dropDatabaseClusterWide(server.getDatabase(databaseName), databaseName);
  }

  /**
   * Post-restore HA hook. In HA mode, submits an install-database Raft entry with
   * {@code forceSnapshot=true} so every replica pulls the restored files. On any failure, drops the
   * just-restored local database so the operator can retry cleanly.
   */
  private void replicateRestoredDatabase(final ServerDatabase restored, final String databaseName) {
    if (!(restored.getWrappedDatabaseInstance() instanceof HAReplicatedDatabase haDb))
      return;

    try {
      haDb.createInReplicas(true);
    } catch (final RuntimeException e) {
      dropQuietly(restored, databaseName);
      throw e;
    }
  }

  /** Best-effort compensating drop after a failed restore or import. Never masks the original failure. */
  private void dropQuietly(final ServerDatabase database, final String databaseName) {
    try {
      database.getEmbedded().drop();
      server.removeDatabase(databaseName);
    } catch (final Exception inner) {
      LogManager.instance().log(this, Level.SEVERE, "Compensating drop for '%s' failed", inner, databaseName);
    }
  }

  private String databaseDirectory(final String databaseName) {
    return server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + File.separator
        + databaseName;
  }

  private static Class<?> loggerClass() throws ClassNotFoundException {
    return Class.forName("com.arcadedb.integration.importer.ConsoleLogger");
  }

  /**
   * A {@code ConsoleLogger} whose every line goes to {@code listener} instead of stdout. Built
   * reflectively because {@code arcadedb-integration} is an optional dependency of the server.
   */
  private static Object progressLogger(final ProgressListener listener) throws ReflectiveOperationException {
    final Class<?> listenerClass = Class.forName("com.arcadedb.integration.importer.ConsoleLogger$LogListener");
    final Object logListener = Proxy.newProxyInstance(listenerClass.getClassLoader(), new Class<?>[] { listenerClass },
        (proxy, method, methodArgs) -> {
          if ("onLogLine".equals(method.getName()))
            listener.onProgress((String) methodArgs[0]);
          return null;
        });
    return loggerClass().getConstructor(int.class, listenerClass).newInstance(2, logListener);
  }

  /**
   * Validates a client-supplied restore/import URL to prevent SSRF and local-file reads. Unless the
   * operator enables {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS}, only
   * {@code http}/{@code https} URLs to non-private hosts are accepted; {@code file://} and any
   * private, loopback, link-local, site-local, multicast, wildcard or unresolvable host is rejected.
   */
  public void validateClientRestoreImportUrl(final String url) {
    if (isRestoreImportLocalUrlsAllowed())
      return;

    final URI uri;
    try {
      uri = URI.create(url.trim());
    } catch (final IllegalArgumentException e) {
      throw new RestoreImportUrlNotAllowedException("Invalid restore/import URL");
    }

    final String scheme = uri.getScheme() == null ? null : uri.getScheme().toLowerCase(Locale.ENGLISH);
    if (scheme == null)
      throw new RestoreImportUrlNotAllowedException("Restore/import URL must use the 'http' or 'https' scheme");

    if (!"http".equals(scheme) && !"https".equals(scheme))
      throw new RestoreImportUrlNotAllowedException("Restore/import URL scheme '" + scheme + "' is not allowed. Enable '"
          + GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getKey()
          + "' to permit local-file and non-HTTP URLs");

    final String host = uri.getHost();
    if (host == null || host.isBlank())
      throw new RestoreImportUrlNotAllowedException("Restore/import URL host is missing");

    if (isBlockedRestoreImportHost(host))
      throw new RestoreImportUrlNotAllowedException("Restore/import from private, loopback or link-local hosts is blocked. Enable '"
          + GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getKey() + "' to override");
  }

  /**
   * The single source of truth for {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} on this server
   * instance, read from the server's own (possibly per-instance-overridden) {@link ContextConfiguration} rather than
   * the static default. {@link #validateClientRestoreImportUrl} and {@link #performRestore} must agree on this value:
   * the pre-check decides whether to accept the command at all, and the actual fetch inside {@code FullRestoreFormat}
   * decides whether to follow it, and letting them read from two different configuration sources would let one permit
   * what the other refuses on the very same server.
   */
  public boolean isRestoreImportLocalUrlsAllowed() {
    return server.getConfiguration().getValueAsBoolean(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS);
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
   * that was the root cause of GHSA-67m7-7w7g-mpmh.
   */
  public static boolean isBlockedRestoreImportHost(final String host) {
    try {
      for (final InetAddress addr : InetAddress.getAllByName(host))
        if (RESERVED_ADDRESSES.isBlocked(addr))
          return true;
      return false;
    } catch (final UnknownHostException e) {
      return true;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static void requireDatabaseName(final String databaseName) {
    if (databaseName == null || databaseName.isEmpty())
      throw new IllegalArgumentException("Database name empty");
  }

  private HAServerPlugin requireHA() {
    final HAServerPlugin ha = server.getHA();
    if (ha == null)
      throw new OperationNotAvailableException(
          "ArcadeDB is not running with High Availability module enabled. Please add this setting at startup: -Darcadedb.ha.enabled=true");
    return ha;
  }
}
