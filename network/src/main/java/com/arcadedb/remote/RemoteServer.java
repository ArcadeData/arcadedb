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

import java.net.InetAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.exception.DatabaseOperationException;
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
    return (List<String>) serverCommand("POST", "list databases", true, true,
        (connection, response) -> response.getJSONArray("result").toList());
  }

  public boolean exists(final String databaseName) {
    return (boolean) httpCommand("GET", databaseName, "exists", "SQL", null, null, false, true,
        (connection, response) -> response.getBoolean("result"));
  }

  public void drop(final String databaseName) {
    try {
      final JSONObject jsonRequest = new JSONObject().put("command", "drop database " + databaseName);
      String payload = getRequestPayload(jsonRequest);

      HttpRequest request = createRequestBuilder("POST", getUrl("server"))
          .POST(HttpRequest.BodyPublishers.ofString(payload))
          .header("Content-Type", "application/json")
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        final Exception detail = manageException(response, "drop database");
        throw new RemoteException("Error on deleting database", detail);
      }

    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on deleting database", e);
    }
  }

  @Override
  public String toString() {
    return protocol + "://" + currentServer + ":" + currentPort;
  }

  public void createUser(final String userName, final String password, final Map<String,String> databases) {
    try {
      final JSONObject jsonUser = new JSONObject();
      jsonUser.put("name", userName);
      jsonUser.put("password", password);
      if (databases != null && !databases.isEmpty()) {
        final JSONObject databasesJson = new JSONObject();
        for (Map.Entry<String, String> entry : databases.entrySet())
          databasesJson.put(entry.getKey(), new String[] { entry.getValue() });
        jsonUser.put("databases", databasesJson);
      }

      final JSONObject jsonRequest = new JSONObject().put("command", "create user " + jsonUser);
      String payload = getRequestPayload(jsonRequest);

      HttpRequest request = createRequestBuilder("POST", getUrl("server"))
          .POST(HttpRequest.BodyPublishers.ofString(payload))
          .header("Content-Type", "application/json")
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        final Exception detail = manageException(response, "create user");
        throw new SecurityException("Error on creating user", detail);
      }

    } catch (final Exception e) {
      throw new DatabaseOperationException("Error on creating user", e);
    }
  }

  public void createUser(final String userName, final String password, final List<String> databases) {
    Map<String,String> databasesWithGroups = new HashMap<String, String>();

    for (final String dbName : databases)
      databasesWithGroups.put(dbName, "admin");

    createUser(userName, password, databasesWithGroups);
  }

  public void dropUser(final String userName) {
    try {
      final JSONObject jsonRequest = new JSONObject().put("command", "drop user " + userName);
      String payload = getRequestPayload(jsonRequest);

      HttpRequest request = createRequestBuilder("POST", getUrl("server"))
          .POST(HttpRequest.BodyPublishers.ofString(payload))
          .header("Content-Type", "application/json")
          .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != 200) {
        final Exception detail = manageException(response, "drop user");
        throw new RemoteException("Error on deleting user", detail);
      }

    } catch (final Exception e) {
      throw new RemoteException("Error on deleting user", e);
    }
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

    final String url = getUrl("server/api-tokens");
    checkTransportCarriesSecrets(url);

    return securityRequestTo(url, "POST", body, "create api token").getJSONObject("result");
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
    if ("https".equalsIgnoreCase(uri.getScheme()) || isLoopbackHost(uri.getHost()))
      return;

    throw new SecurityException(
        "Refusing to request an API token over a cleartext connection to non-loopback host '" + uri.getHost()
            + "': the token would be readable on the wire. Use https://, connect over loopback, or opt in explicitly "
            + "with setAllowInsecureApiTokenTransport(true)");
  }

  /**
   * Whether {@code host} names this machine's loopback interface. Fails closed on a name that does not
   * resolve: an unresolvable host is not a host known to be local.
   * <p>
   * <b>It answers for the resolution it performs, not for the one the socket will use.</b> A name is
   * resolved here and resolved again, independently, by the JDK {@code HttpClient} when it dials - so a
   * host name whose DNS answer an attacker can influence (rebinding, split horizon, a one-second TTL)
   * could read as loopback here and carry the token somewhere else in the clear. Pinning the resolved
   * address into the URL would close it and break TLS SNI and virtual hosting in exchange, which is a bad
   * trade for a guard that is already the second of two. Configure the client with a literal address or
   * with {@code localhost} and the gap does not arise; where it might, the transport that actually
   * protects the token is TLS, and the server-side gate
   * ({@code arcadedb.server.apiTokenRequireSecureTransport}) is the check that reads the live connection
   * rather than a name.
   */
  static boolean isLoopbackHost(final String host) {
    if (host == null || host.isBlank())
      return false;

    final String trimmed = host.trim();
    if (trimmed.equalsIgnoreCase("localhost"))
      return true;

    try {
      return InetAddress.getByName(trimmed).isLoopbackAddress();
    } catch (final UnknownHostException e) {
      return false;
    }
  }

  /**
   * Sends one request against a {@code /server/*} route and returns its parsed body.
   * <p>
   * The request goes to the node this client is connected to, with no leader preference of its own,
   * because each route already settles its own cluster semantics: the {@code /server/users} routes
   * forward to the leader themselves (issue #7380), and the group and API-token routes submit a Raft
   * entry through the group committer, which reaches the leader without the client choosing it.
   *
   * @param path the route and query string, relative to {@code /api/v<n>/}
   */
  private JSONObject securityRequest(final String method, final String path, final JSONObject body,
      final String operation) {
    return securityRequestTo(getUrl(path), method, body, operation);
  }

  /**
   * Sends one request against a {@code /server/*} route and returns its parsed body, empty rather than
   * null when the route answered with none.
   * <p>
   * It goes through {@link #sendWithWatchdog}, not through {@code httpClient.send}: an admin call that
   * hangs must be bounded by the same budget as every other request this driver makes (issue #5847).
   */
  private JSONObject securityRequestTo(final String url, final String method, final JSONObject body,
      final String operation) {
    try {
      HttpRequest.Builder builder = createRequestBuilder(method, url);

      if (body != null)
        builder = builder.method(method, HttpRequest.BodyPublishers.ofString(getRequestPayload(body)))
            .header("Content-Type", "application/json");
      else if ("GET".equals(method))
        builder = builder.GET();
      else
        builder = builder.method(method, HttpRequest.BodyPublishers.noBody());

      final HttpResponse<String> response = sendWithWatchdog(builder.build());

      // 200 and 201 are both success on these routes: POST /server/users and POST /server/api-tokens
      // answer 201, every other route answers 200.
      if (response.statusCode() != 200 && response.statusCode() != 201) {
        final Exception detail = manageException(response, operation);
        if (detail instanceof final RuntimeException runtime)
          throw runtime;
        throw new RemoteException("Error on executing '" + operation + "'", detail);
      }

      final String payload = response.body();
      return payload == null || payload.isBlank() ? new JSONObject() : new JSONObject(payload);

    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RemoteException("Error on executing '" + operation + "': interrupted", e);
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new RemoteException("Error on executing '" + operation + "'", e);
    }
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

  private Object serverCommand(final String method, final String command, final boolean leaderIsPreferable,
      final boolean autoReconnect, final Callback callback) {
    return httpCommand(method, null, "server", null, command, null, leaderIsPreferable, autoReconnect, callback);
  }
}
