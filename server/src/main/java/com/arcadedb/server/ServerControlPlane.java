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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.backup.AutoBackupConfig;
import com.arcadedb.server.backup.AutoBackupSchedulerPlugin;
import com.arcadedb.server.backup.BackupCoordinator;
import com.arcadedb.server.backup.BackupRetentionManager;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.server.security.ApiTokenConfiguration;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityUser;
import com.arcadedb.utility.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

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
 * What stayed behind in the HTTP handler is what is genuinely HTTP: parsing the command string,
 * forwarding a write to the leader, mapping a result to a status code, and the three operations
 * that stream progress over the {@code HttpServerExchange} itself (restore backup, restore
 * database, import database - tracked for gRPC in issue #7308).
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
   * The {@code connect cluster} command of the HTTP control plane. Kept here with the rest so the
   * command set is complete in one place; it has never been implemented by the current HA stack.
   */
  public void connectCluster(final String serverAddress) {

    throw new OperationNotAvailableException(
        "Connect cluster operation is not supported by the current HA implementation. Use the cluster configuration to join nodes.");
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

  // ---------------------------------------------------------------------------------------------
  // Database lifecycle
  // ---------------------------------------------------------------------------------------------

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

    server.getSecurity().saveGroup(database, name, normalized);
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

    if (!server.getSecurity().deleteGroup(database, name))
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
      return server.getSecurity().getApiTokenConfiguration()
          .createToken(name, effectiveDatabase, expiresAt, effectivePermissions);
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

    if (!server.getSecurity().getApiTokenConfiguration().deleteToken(tokenHash))
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

  public JSONObject listBackups(final String databaseName) {
    requireDatabaseName(databaseName);


    final AutoBackupSchedulerPlugin plugin = getBackupPlugin();

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

                  // Parse timestamp from filename, through the same convention that wrote it (issue #6753)
                  final LocalDateTime timestamp = BackupCoordinator.parseArchiveTimestamp(p.getFileName().toString());
                  backup.put("timestamp", timestamp != null ? timestamp.toString() : JSONObject.NULL);

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

    return response;
  }

  /**
   * Runs a full backup inline and returns {@code {"result":"ok","backupFile":...}}.
   *
   * @throws BackupInProgressException when another backup of the same database is already running.
   *                                   This command runs the backup inline, so it is one of the entry points that can
   *                                   have a database being backed up at the same time as the auto-backup schedule
   *                                   does - down to resolving to the same archive name and writing into the same
   *                                   file. Refusing outright is the honest answer to "back up a database that is
   *                                   already being backed up": a second full backup of the same data produces
   *                                   nothing the first one will not, and the caller gets told rather than silently
   *                                   handed the other run's archive (issue #6753).
   */
  public JSONObject triggerBackup(final String databaseName) {
    requireDatabaseName(databaseName);


    final BackupCoordinator coordinator = server.getBackupCoordinator();
    if (!coordinator.begin(databaseName))
      throw new BackupInProgressException("A backup of database '" + databaseName + "' is already in progress");

    try {
      return executeImmediateBackup(databaseName);
    } finally {
      coordinator.end(databaseName);
    }
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
   * Resolves a backup file name to an absolute path inside the configured auto-backup directory for
   * the given database, rejecting any name that contains path separators or traversal sequences.
   */
  public Path resolveBackupFile(final String databaseName, final String fileName) {
    requireDatabaseName(databaseName);

    // Reject anything that is not a plain backup file name.
    if (fileName.contains("/") || fileName.contains("\\") || fileName.contains("..") || fileName.isBlank()
        || !fileName.endsWith(".zip") || !fileName.contains("-backup-"))
      throw new IllegalArgumentException("Invalid backup file name: " + fileName);

    final AutoBackupSchedulerPlugin plugin = getBackupPlugin();
    if (plugin == null || !plugin.isEnabled() || plugin.getBackupConfig() == null)
      throw new IllegalArgumentException("Auto-backup is not configured");

    String backupDirectory = plugin.getBackupConfig().getBackupDirectory();
    final Path backupPath = Paths.get(backupDirectory);
    if (!backupPath.isAbsolute())
      backupDirectory = Paths.get(server.getRootPath(), backupDirectory).toString();

    final Path dbBackupDir = Paths.get(backupDirectory, databaseName).normalize();
    final Path resolved = dbBackupDir.resolve(fileName).normalize();

    // Defence in depth: the resolved file must still live inside the database backup directory.
    if (!resolved.startsWith(dbBackupDir))
      throw new IllegalArgumentException("Invalid backup file path");

    if (!Files.exists(resolved) || !Files.isRegularFile(resolved))
      throw new IllegalArgumentException("Backup file not found: " + fileName);

    return resolved;
  }

  private JSONObject executeImmediateBackup(final String databaseName) {
    final AutoBackupSchedulerPlugin plugin = getBackupPlugin();

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

        final String backupFileName = server.getBackupCoordinator().newArchiveName(databaseName);

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
        return response;

      } catch (final ClassNotFoundException e) {
        throw new RuntimeException("Backup libs not found in classpath. Make sure arcadedb-integration module is included.", e);
      } catch (final Exception e) {
        final Throwable cause = e.getCause() != null ? e.getCause() : e;
        throw new RuntimeException("Error triggering backup for database '" + databaseName + "': " + cause.getMessage(), cause);
      }
    }

    // Fallback: use SQL command (uses GlobalConfiguration.SERVER_BACKUP_DIRECTORY). This one does not name the
    // archive through the coordinator: with no target the SQL statement lets BackupSettings apply its own default,
    // which is the same convention down to the milliseconds - the coordinator is where that convention was copied
    // from in the first place.
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
        return response;
      }
    } catch (final Exception e) {
      throw new RuntimeException("Error triggering backup for database '" + databaseName + "': " + e.getMessage(), e);
    }
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
   * Raised by {@link #triggerBackup(String)} when the database is already being backed up. HTTP
   * answers it with a 409 and gRPC with {@code ABORTED}; both carry this message verbatim.
   */
  public static class BackupInProgressException extends RuntimeException {
    public BackupInProgressException(final String message) {
      super(message);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Query profiler
  // ---------------------------------------------------------------------------------------------

  /**
   * Starts recording. A {@code timeoutSec} of zero or less starts an open-ended recording, which is
   * what the HTTP {@code profiler start} command does when it carries no timeout.
   */
  public JSONObject profilerStart(final int timeoutSec) {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    if (timeoutSec > 0)
      profiler.start(timeoutSec);
    else
      profiler.start();

    return new JSONObject().put("result", "ok").put("recording", true);
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
