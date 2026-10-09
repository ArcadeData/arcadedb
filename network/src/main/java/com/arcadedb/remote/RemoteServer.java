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
package com.arcadedb.remote;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.network.HostUtil;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Remote Database implementation. It's not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteServer extends RemoteHttpComponent {
  /**
   * Whether {@link #createApiToken} may ask a server reached over cleartext HTTP, on a host that is not
   * loopback, to write a plaintext token back. Off by default: the token authenticates its holder and the
   * server cannot reissue it, so the one request in this client that carries secret material back is the
   * one request that checks how it would travel (issue #7372).
   * <p>
   * Volatile for the same reason {@code maxResultRows} is: this class is documented as not thread safe,
   * but a connection-wide knob an application may flip at any time has to be visible to the thread that
   * then makes the call, and a flag whose whole job is to relax a security guard is the wrong one to let
   * a caller set without effect.
   */
  private volatile boolean allowInsecureApiTokenTransport = false;

  public RemoteServer(final String server, final int port, final String userName, final String userPassword) {
    this(server, port, userName, userPassword, new ContextConfiguration());
  }

  public RemoteServer(final String server, final int port, final String userName, final String userPassword,
      final ContextConfiguration configuration) {
    super(server, port, userName, userPassword, configuration);
  }

  public void create(final String databaseName) {
    serverCommand("POST", "create database " + databaseName, true, true, null);
  }

  public List<String> databases() {
    return (List<String>) serverCommand("POST", "list databases", "list databases", true, true, true,
        (connection, response) -> response.getJSONArray("result").toList());
  }

  public boolean exists(final String databaseName) {
    return (boolean) httpCommand("GET", databaseName, "exists", "SQL", null, null, false, true,
        (connection, response) -> response.getBoolean("result"));
  }

  public void drop(final String databaseName) {
    serverCommand("POST", "drop database " + databaseName, true, true, null);
  }

  @Override
  public String toString() {
    return protocol + "://" + currentServer + ":" + currentPort;
  }

  /**
   * The error label names the user only: the command text carries the password.
   */
  public void createUser(final String userName, final String password, final Map<String, String> databases) {
    final JSONObject jsonUser = new JSONObject();
    jsonUser.put("name", userName);
    jsonUser.put("password", password);
    if (databases != null && !databases.isEmpty()) {
      final JSONObject databasesJson = new JSONObject();
      for (final Map.Entry<String, String> entry : databases.entrySet())
        databasesJson.put(entry.getKey(), new String[] { entry.getValue() });
      jsonUser.put("databases", databasesJson);
    }

    serverCommand("POST", "create user " + jsonUser, "create user " + userName, true, true, null);
  }

  public void createUser(final String userName, final String password, final List<String> databases) {
    Map<String,String> databasesWithGroups = new HashMap<String, String>();

    for (final String dbName : databases)
      databasesWithGroups.put(dbName, "admin");

    createUser(userName, password, databasesWithGroups);
  }

  public void dropUser(final String userName) {
    serverCommand("POST", "drop user " + userName, true, true, null);
  }


  // ---------------------------------------------------------------------------------------------
  // Security control plane: /server/users, /server/groups, /server/api-tokens (issue #7372)
  //
  // The method names are the ones RemoteGrpcServer uses for the same nine routes, so the two clients
  // are readable against each other; the shapes are JSON because this client speaks the JSON the
  // routes already define, and adding a parallel POJO layer would be a second contract to keep in
  // step with the first.
  // ---------------------------------------------------------------------------------------------

  /**
   * The server's users, as {@code GET /server/users} returns them: one document per user carrying
   * {@code name} and {@code databases} ({@code database -> [groups]}). Password hashes are never
   * included - the route does not return them.
   */
  public List<JSONObject> listUsers() {
    return toDocumentList(securityRequest("GET", "server/users", null, "list users").getJSONArray("result"));
  }

  /**
   * Updates an existing user through {@code PUT /server/users?name=<user>}. Both arguments are
   * independently optional: a null leaves that part of the user alone, so changing a password does not
   * clear the user's grants and vice versa. Passing an empty (non-null) map DOES clear them - that is
   * the caller saying so.
   *
   * @param databases database name (or {@code "*"}) to the groups the user holds on it
   */
  public void updateUser(final String userName, final String password, final Map<String, List<String>> databases) {
    if (userName == null || userName.isBlank())
      throw new IllegalArgumentException("User name is required");

    final JSONObject body = new JSONObject();
    if (password != null)
      body.put("password", password);
    if (databases != null)
      body.put("databases", toDatabasesDocument(databases));

    securityRequest("PUT", "server/users?name=" + encodeQueryValue(userName), body, "update user");
  }

  /**
   * Changes only a user's password, leaving its per-database grants as they are.
   */
  public void updateUserPassword(final String userName, final String password) {
    updateUser(userName, Objects.requireNonNull(password, "password"), null);
  }

  /**
   * Replaces only a user's per-database grants, leaving its password as it is.
   */
  public void updateUserGrants(final String userName, final Map<String, List<String>> databases) {
    updateUser(userName, null, Objects.requireNonNull(databases, "databases"));
  }

  /**
   * The whole group/permission document, as {@code GET /server/groups} returns it. It carries no
   * secret: a group is a set of permissions, and the users holding it are named in the user documents.
   */
  public JSONObject listGroups() {
    return securityRequest("GET", "server/groups", null, "list groups").getJSONObject("result");
  }

  /**
   * Creates or replaces one group on {@code database} ({@code "*"} for every database) through
   * {@code POST /server/groups}. Replaces rather than merges: an existing group of the same name is
   * overwritten, not updated field by field.
   * <p>
   * The server keeps four keys of {@code groupConfig} - {@code resultSetLimit}, {@code readTimeout},
   * {@code access} and {@code types} - and defaults the ones it does not find; anything else in the
   * document is dropped, {@code database} and {@code name} included, which is why those two are
   * arguments here rather than keys the caller is expected to set.
   */
  public void saveGroup(final String database, final String name, final JSONObject groupConfig) {
    if (database == null || database.isBlank())
      throw new IllegalArgumentException("Database name is required");
    if (name == null || name.isBlank())
      throw new IllegalArgumentException("Group name is required");

    final JSONObject body = groupConfig != null ? groupConfig.copy() : new JSONObject();
    body.put("database", database);
    body.put("name", name);

    securityRequest("POST", "server/groups", body, "save group");
  }

  /**
   * Drops one group through {@code DELETE /server/groups?database=<db>&amp;name=<group>}.
   */
  public void deleteGroup(final String database, final String name) {
    if (database == null || database.isBlank())
      throw new IllegalArgumentException("Database name is required");
    if (name == null || name.isBlank())
      throw new IllegalArgumentException("Group name is required");

    securityRequest("DELETE",
        "server/groups?database=" + encodeQueryValue(database) + "&name=" + encodeQueryValue(name), null, "delete group");
  }

  /**
   * The issued API tokens, as {@code GET /server/api-tokens} returns them: metadata plus each token's
   * {@code tokenHash}, which is the handle {@link #deleteApiToken(String)} takes. Never the token
   * material - the server does not keep it.
   */
  public List<JSONObject> listApiTokens() {
    return toDocumentList(securityRequest("GET", "server/api-tokens", null, "list api tokens").getJSONArray("result"));
  }

  /**
   * Mints an API token through {@code POST /server/api-tokens}. <b>The returned {@code token} field is
   * the only copy of the token that will ever exist</b>: the server keeps its SHA-256 and cannot
   * produce the plaintext again.
   * <p>
   * This call refuses to leave the process over a connection that would put that token on the wire in
   * the clear - a {@code http://} URL to a host that is not loopback - which is the client-side half of
   * what {@code RemoteGrpcServer} refuses for the same reason (issue #7309). Use {@code https://},
   * connect to the server over loopback, or opt out explicitly with
   * {@link #setAllowInsecureApiTokenTransport(boolean)}. The server applies a refusal of its own only
   * when {@code arcadedb.server.apiTokenRequireSecureTransport} is on, so this guard is what protects a
   * caller talking to a default-configured server.
   *
   * @param database    the database the token is scoped to, or {@code "*"}/null for every database
   * @param expiresAt   epoch millis at which the token stops working; 0 for a token that does not expire
   * @param permissions the permission document, or null for none
   *
   * @return the created token document, including the plaintext {@code token}
   */
  public JSONObject createApiToken(final String name, final String database, final long expiresAt,
      final JSONObject permissions) {
    if (name == null || name.isBlank())
      throw new IllegalArgumentException("Token name is required");

    final JSONObject body = new JSONObject();
    body.put("name", name);
    body.put("database", database == null || database.isBlank() ? "*" : database);
    body.put("expiresAt", expiresAt);
    body.put("permissions", permissions != null ? permissions : new JSONObject());

    // Checked against each URL the request is about to be sent to: failover changes the host that writes the token back.
    return controlPlaneRequest("POST", "server/api-tokens", body, "create api token", this::checkTransportCarriesSecrets)
        .getJSONObject("result");
  }

  /**
   * Revokes a token by its SHA-256 hash - the {@code tokenHash} of a {@link #listApiTokens()} entry, or
   * of a {@link #createApiToken} response. The plaintext token is deliberately not accepted by the
   * server: it would land in whatever logged the request, which is the exposure the revocation is ending.
   */
  public void deleteApiToken(final String tokenHash) {
    if (tokenHash == null || tokenHash.isBlank())
      throw new IllegalArgumentException("Token hash is required");

    securityRequest("DELETE", "server/api-tokens?token=" + encodeQueryValue(tokenHash), null, "delete api token");
  }

  /**
   * Lifts the quarantine standing on {@code databaseName} of a server that is the sole voter of its cluster, accepting its
   * copy as it is WITHOUT a resync, through {@code POST /api/v1/cluster/accept-diverged/{database}} (root only, issue
   * #9449). The entry the quarantine skipped is not replayed. The server refuses with 404 when nothing stands on the
   * database and with 409 when it is not the sole voter, where a resync from a peer is the way out.
   *
   * @return {@code {result, database, localServer, appliedIndex}}, plus {@code divergenceCause} and {@code readFloor}
   * when they stood
   */
  public JSONObject acceptDivergedDatabase(final String databaseName) {
    if (databaseName == null || databaseName.isBlank())
      throw new IllegalArgumentException("Database name is required");

    return controlPlaneRequest("POST", "cluster/accept-diverged/" + encodeQueryValue(databaseName), new JSONObject(),
        "accept diverged database", null);
  }

  /**
   * Lifts the node-wide stale-snapshot read floor of a server that is the sole voter of its cluster, accepting its
   * databases as they are WITHOUT a resync, through {@code POST /api/v1/cluster/accept-stale-snapshot} (root only, issue
   * #9498). The entries between the floor and the snapshot marker are not replayed. The server refuses with 404 when no
   * floor stands and with 409 when it is not the sole voter, where a resync from a peer is the way out.
   *
   * @return {@code {result, localServer, readFloor, snapshotIndex, appliedIndex}}
   */
  public JSONObject acceptStaleSnapshot() {
    return controlPlaneRequest("POST", "cluster/accept-stale-snapshot", new JSONObject(), "accept stale snapshot", null);
  }

  /**
   * Starts "connect this server to the ArcadeDB customer portal" through {@code POST /server/support/connect} (root only): the
   * server asks the portal for a code, a person who administers a workspace approves it in the portal, and the server then
   * receives and stores the workspace key itself. This is the console and curl twin of Studio's button.
   *
   * @return {@code {userCode, verifyUrl, expiresIn}}; the key and the device code never travel through this connection
   */
  public JSONObject startPortalConnect() {
    return controlPlaneRequest("POST", "server/support/connect", new JSONObject(), "connect to the portal", null);
  }

  /**
   * The state of the last portal connection, as {@code GET /server/support/connect} answers it:
   * {@code {status: none|pending|connected|expired|denied|error|cancelled, ...}}. Once {@code connected} it carries
   * {@code workspaceName} and {@code registration} (what registering this server as an installation did).
   */
  public JSONObject portalConnectStatus() {
    return controlPlaneRequest("GET", "server/support/connect", null, "get the portal connection state", null);
  }

  /** Stops waiting for the approval ({@code DELETE /server/support/connect}). A key already received stays registered. */
  public void cancelPortalConnect() {
    controlPlaneRequest("DELETE", "server/support/connect", null, "cancel the portal connection", null);
  }

  /**
   * Whether {@link #createApiToken} may ask for a token over a cleartext connection to a host that is
   * not loopback. Off by default, and the only way past the client-side guard; it is the counterpart of
   * {@code RemoteGrpcServer}'s {@code allowInsecureCredentials}. Turning it on does not make the
   * transport safe - it states that something outside this process (an SSH tunnel, a VPN, a service
   * mesh) already protects it.
   */
  public void setAllowInsecureApiTokenTransport(final boolean allowInsecureApiTokenTransport) {
    this.allowInsecureApiTokenTransport = allowInsecureApiTokenTransport;
  }

  public boolean isAllowInsecureApiTokenTransport() {
    return allowInsecureApiTokenTransport;
  }

  /**
   * Refuses the call when the URL it would be sent to neither encrypts the response nor keeps it on the
   * loopback interface. Applied to the request URL rather than to the configured server name because
   * the two differ: a {@code STICKY} pin or a leader hand-off changes which host the request actually
   * reaches, and the host that receives it is the one that writes the token back.
   */
  void checkTransportCarriesSecrets(final String url) {
    if (allowInsecureApiTokenTransport)
      return;

    final URI uri = URI.create(url);
    if ("https".equalsIgnoreCase(uri.getScheme()) || HostUtil.isLoopbackHost(uri.getHost()))
      return;

    throw new SecurityException(
        "Refusing to request an API token over a cleartext connection to non-loopback host '" + uri.getHost()
            + "': the token would be readable on the wire. Use https://, connect over loopback, or opt in explicitly "
            + "with setAllowInsecureApiTokenTransport(true)");
  }

  /**
   * Sends one request against a {@code /server/*} route and returns its parsed body, empty rather than null when the
   * route answered with none.
   * <p>
   * It goes through {@link #controlPlaneRequest}, the {@code httpCommand} loop every server command uses, so a node
   * that is mid-election is waited out, and one that is down is replaced when {@code NETWORK_SAME_SERVER_ERROR_RETRIES}
   * allows more than one attempt, exactly as for {@code createUser} (issue #8710), and a write
   * whose answer was lost is not sent again. The leader is preferred; the {@code /server/users} routes forward to it
   * themselves (issue #7380) and the group and API-token routes submit a Raft entry, so reaching a follower still works.
   *
   * @param path the route and query string, relative to {@code /api/v<n>/}
   */
  private JSONObject securityRequest(final String method, final String path, final JSONObject body,
      final String operation) {
    return controlPlaneRequest(method, path, body, operation, null);
  }

  private static JSONObject toDatabasesDocument(final Map<String, List<String>> databases) {
    final JSONObject document = new JSONObject();
    for (final Map.Entry<String, List<String>> entry : databases.entrySet())
      document.put(entry.getKey(), new JSONArray(entry.getValue() != null ? entry.getValue() : List.of()));
    return document;
  }

  private static List<JSONObject> toDocumentList(final JSONArray array) {
    final List<JSONObject> result = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      result.add(array.getJSONObject(i));
    return result;
  }

  private static String encodeQueryValue(final String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  /**
   * Every server command goes through {@code httpCommand}, for its election retry, failover and typed exceptions.
   */
  private Object serverCommand(final String method, final String command, final boolean leaderIsPreferable,
      final boolean autoReconnect, final Callback callback) {
    return serverCommand(method, command, command, leaderIsPreferable, autoReconnect, callback);
  }

  /**
   * As above, naming the command as {@code errorOperation} in an error message, for a command carrying a secret.
   */
  private Object serverCommand(final String method, final String command, final String errorOperation,
      final boolean leaderIsPreferable, final boolean autoReconnect, final Callback callback) {
    return serverCommand(method, command, errorOperation, leaderIsPreferable, autoReconnect, false, callback);
  }

  /**
   * As above, stating that the command is read-only so a transport failure after it was sent does not stop the
   * failover loop (issue #8570). Every other server command may write, so it defaults to not replayable.
   */
  private Object serverCommand(final String method, final String command, final String errorOperation,
      final boolean leaderIsPreferable, final boolean autoReconnect, final boolean replayable, final Callback callback) {
    return httpCommand(method, null, "server", null, command, null, leaderIsPreferable, autoReconnect, callback,
        errorOperation, replayable);
  }
}
