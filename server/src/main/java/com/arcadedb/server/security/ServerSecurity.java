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
package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DefaultConsoleReader;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.credential.CredentialsValidator;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import com.arcadedb.utility.AnsiCode;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_ALGORITHM;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_RELOAD_EVERY;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS;

public class ServerSecurity implements ServerPlugin, SecurityManager {

  public static final  int                             LATEST_VERSION       = 2;
  private final        ArcadeDBServer                  server;
  private final        SecurityUserFileRepository      usersRepository;
  private final        SecurityGroupFileRepository     groupRepository;
  private final        String                          algorithm;
  private final        SecretKeyFactory                secretKeyFactory;
  private final        ConcurrentSaltCache             saltCache;
  private final        int                             saltIteration;
  // Volatile reference to an immutable-by-convention snapshot: readers (authenticate on Undertow worker
  // threads) grab the current map lock-free, while loadUsers/applyReplicatedUsers build a fresh map and
  // publish it in one atomic reference swap so a read never observes a clear()->repopulate empty window.
  private volatile     Map<String, ServerSecurityUser> users                = new ConcurrentHashMap<>();
  private final        int                             checkConfigReloadEveryMs;
  private              CredentialsValidator            credentialsValidator = new DefaultCredentialsValidator();
  private static final SecureRandom                    RANDOM               = new SecureRandom();
  public static final  int                             SALT_SIZE            = 32;

  // Reused per thread so the Basic-auth hot path avoids a getInstance provider lookup on every call.
  private static final ThreadLocal<MessageDigest>      SHA256_DIGEST        = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new ServerSecurityException("SHA-256 not available for salt cache key", e);
    }
  });
  private              Timer                           reloadConfigurationTimer;
  private final        ApiTokenConfiguration           apiTokenConfig;
  private static final int                                MAX_TOKEN_FAILURES   = 5;
  private static final long                               TOKEN_LOCKOUT_MS     = 30_000;
  private final        ConcurrentHashMap<String, long[]>  tokenFailures        = new ConcurrentHashMap<>();
  private              Timer                               tokenFailureCleanupTimer;
  private static final int                                MAX_PASSWORD_FAILURES = 5;
  private static final long                               PASSWORD_LOCKOUT_MS   = 30_000;
  private final        ConcurrentHashMap<String, long[]>  passwordFailures      = new ConcurrentHashMap<>();

  public ServerSecurity(final ArcadeDBServer server, final ContextConfiguration configuration, final String configPath) {
    this.server = server;
    this.algorithm = configuration.getValueAsString(SERVER_SECURITY_ALGORITHM);
    this.checkConfigReloadEveryMs = configuration.getValueAsInteger(SERVER_SECURITY_RELOAD_EVERY);

    final int cacheSize = configuration.getValueAsInteger(SERVER_SECURITY_SALT_CACHE_SIZE);

    saltCache = cacheSize > 0 ? new ConcurrentSaltCache(cacheSize) : null;

    saltIteration = configuration.getValueAsInteger(SERVER_SECURITY_SALT_ITERATIONS);

    usersRepository = new SecurityUserFileRepository(configPath);
    groupRepository = new SecurityGroupFileRepository(configPath, checkConfigReloadEveryMs).onReload(latestConfiguration -> {
      for (final String databaseName : server.getDatabaseNames()) {
        updateSchema(server.getDatabase(databaseName));
      }
      return null;
    });

    apiTokenConfig = new ApiTokenConfiguration(configPath);

    try {
      secretKeyFactory = SecretKeyFactory.getInstance(algorithm);
    } catch (final NoSuchAlgorithmException e) {
      LogManager.instance().log(this, Level.SEVERE, "Security algorithm '%s' not available (error=%s)", e, algorithm);
      throw new ServerSecurityException("Security algorithm '" + algorithm + "' not available", e);
    }
  }

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
  }

  @Override
  public void startService() {
    // NO ACTION
  }

  public void loadUsers() {
    try {
      final Map<String, ServerSecurityUser> newUsers = new ConcurrentHashMap<>();

      try {
        for (final JSONObject userJson : usersRepository.getUsers()) {
          final ServerSecurityUser user = new ServerSecurityUser(server, userJson);
          newUsers.put(user.getName(), user);
        }
      } catch (final JSONException e) {
        groupRepository.saveInError(e);
        for (final JSONObject userJson : SecurityUserFileRepository.createDefault()) {
          final ServerSecurityUser user = new ServerSecurityUser(server, userJson);
          newUsers.put(user.getName(), user);
        }
      }

      // Publish the freshly-built map in a single atomic reference swap: concurrent readers see either the
      // previous complete map or this one, never an empty intermediate state.
      this.users = newUsers;

      if (newUsers.isEmpty() || (newUsers.containsKey("root") && newUsers.get("root").getPassword() == null))
        askForRootPassword();

      apiTokenConfig.load();

      // Schedule periodic cleanup of stale token failure entries
      if (tokenFailureCleanupTimer == null) {
        tokenFailureCleanupTimer = new Timer(true);
        tokenFailureCleanupTimer.schedule(new TimerTask() {
          @Override
          public void run() {
            final long now = System.currentTimeMillis();
            tokenFailures.entrySet().removeIf(e -> now - e.getValue()[1] > TOKEN_LOCKOUT_MS);
            passwordFailures.entrySet().removeIf(e -> now - e.getValue()[1] > PASSWORD_LOCKOUT_MS);
          }
        }, TOKEN_LOCKOUT_MS, TOKEN_LOCKOUT_MS);
      }

      final long fileLastModified = usersRepository.getFileLastModified();
      if (fileLastModified > -1 && reloadConfigurationTimer == null) {
        reloadConfigurationTimer = new Timer();
        reloadConfigurationTimer.schedule(new TimerTask() {
          @Override
          public void run() {
            if (usersRepository.isUserFileChanged()) {
              LogManager.instance().log(this, Level.INFO, "Reloading user files...");
              loadUsers();
            }
          }
        }, checkConfigReloadEveryMs, checkConfigReloadEveryMs);
      }

    } catch (final IOException e) {
      throw new ServerException("Error on starting Security service", e);
    }
  }

  @Override
  public void stopService() {
    if (reloadConfigurationTimer != null)
      reloadConfigurationTimer.cancel();
    if (tokenFailureCleanupTimer != null) {
      tokenFailureCleanupTimer.cancel();
      tokenFailureCleanupTimer = null;
    }

    users = new ConcurrentHashMap<>();
    if (groupRepository != null)
      groupRepository.stop();
  }

  public ServerSecurityUser authenticate(final String userName, final String userPassword, final String databaseName) {
    // Brute-force protection for password (Basic-auth / login) authentication, mirroring the API-token
    // path: reject fast when a principal has exceeded the failure threshold within the lockout window.
    // The failure map is keyed by a bounded hash prefix of the user name rather than the raw name, so an
    // attacker flooding arbitrary-length user names cannot inflate per-entry memory, and plaintext user
    // names are not retained as reachable heap keys.
    final String failureKey = passwordFailureKey(userName);
    final long[] existing = passwordFailures.get(failureKey);
    if (existing != null && existing[0] >= MAX_PASSWORD_FAILURES && System.currentTimeMillis() - existing[1] < PASSWORD_LOCKOUT_MS)
      throw new ServerSecurityException("Too many failed authentication attempts. Try again later.");

    final ServerSecurityUser su = users.get(userName);
    // A user with no stored password (e.g. before first-run root initialization) can never authenticate;
    // guard here so passwordMatch is not handed a null hash to split.
    if (su == null || su.getPassword() == null || !passwordMatch(userPassword, su.getPassword())) {
      recordPasswordFailure(failureKey);
      throw new ServerSecurityException("User/Password not valid");
    }

    // Successful authentication: clear the failure counter for this principal.
    passwordFailures.remove(failureKey);

    if (databaseName != null) {
      final Set<String> allowedDatabases = su.getAuthorizedDatabases();
      if (!allowedDatabases.contains(SecurityManager.ANY) && !su.getAuthorizedDatabases().contains(databaseName))
        throw new ServerSecurityException("User has not access to database '" + databaseName + "'");
    }

    return su;
  }

  /**
   * Records a failed password authentication attempt for the given key, atomically incrementing the
   * failure counter (or restarting it once the lockout window has elapsed). Each update publishes a
   * fresh {@code long[]} instance rather than mutating the existing one in place, so concurrent readers
   * in {@link #authenticate} always observe a safely-published, internally-consistent snapshot.
   */
  private void recordPasswordFailure(final String failureKey) {
    passwordFailures.compute(failureKey, (k, entry) -> {
      final long ts = System.currentTimeMillis();
      if (entry == null || ts - entry[1] > PASSWORD_LOCKOUT_MS)
        return new long[] { 1, ts };
      // Preserve the FIRST-failure timestamp on increment (index 1). The lockout window is therefore
      // measured from the first failure in the window, not the last - matching the API-token path.
      return new long[] { entry[0] + 1, entry[1] };
    });
  }

  /**
   * Builds the brute-force failure-map key as a bounded (16 hex chars / 64-bit) SHA-256 prefix of the
   * user name. Keeps per-entry memory constant regardless of user-name length and avoids retaining
   * plaintext user names as reachable map keys, mirroring the salt-cache and API-token key strategy.
   */
  private static String passwordFailureKey(final String userName) {
    final MessageDigest digest = SHA256_DIGEST.get();
    final byte[] hash = digest.digest(userName.getBytes(StandardCharsets.UTF_8));
    return HexFormat.of().formatHex(hash, 0, 8);
  }

  /**
   * Override the default @{@link CredentialsValidator} implementation (@{@link DefaultCredentialsValidator}) providing a custom one.
   */
  public void setCredentialsValidator(final CredentialsValidator credentialsValidator) {
    this.credentialsValidator = credentialsValidator;
  }

  /**
   * Returns the active credentials validator so that all create-user paths can enforce a single
   * password/username policy.
   */
  public CredentialsValidator getCredentialsValidator() {
    return credentialsValidator;
  }

  public boolean existsUser(final String userName) {
    return users.containsKey(userName);
  }

  public Set<String> getUsers() {
    return users.keySet();
  }

  public ServerSecurityUser getUser(final String userName) {
    return users.get(userName);
  }

  public synchronized ServerSecurityUser createUser(final JSONObject userConfiguration) {
    final String name = userConfiguration.getString("name");
    if (users.containsKey(name))
      throw new SecurityException("User '" + name + "' already exists");

    final ServerSecurityUser user = new ServerSecurityUser(server, userConfiguration);
    users.put(name, user);
    saveUsers();
    return user;
  }

  public synchronized ServerSecurityUser updateUser(final JSONObject userConfiguration) {
    final String name = userConfiguration.getString("name");
    if (!users.containsKey(name))
      throw new ServerSecurityException("User '" + name + "' not found");

    final ServerSecurityUser user = new ServerSecurityUser(server, userConfiguration);
    users.put(name, user);
    saveUsers();
    // Note: a metadata update does NOT force-rollback the principal's open transactions - per-request
    // authorization is still re-checked on every command, so a narrowed grant is enforced without tearing
    // down unrelated in-flight work. Only drop and password change (below) invalidate live sessions.
    return user;
  }

  public synchronized boolean dropUser(final String userName) {
    if (users.remove(userName) != null) {
      saveUsers();
      // Invalidate the dropped principal's live HTTP transaction sessions so a recreated same-name principal
      // cannot adopt (and commit) the prior principal's still-open session.
      invalidateHttpSessions(userName);
      return true;
    }
    return false;
  }

  /**
   * Invalidates every live HTTP transaction session owned by the named principal (rolling back its open
   * transaction). No-op when the HTTP server is not running (e.g. embedded use).
   */
  private void invalidateHttpSessions(final String userName) {
    final HttpServer httpServer = server != null ? server.getHttpServer() : null;
    if (httpServer != null)
      httpServer.getSessionManager().removeSessionsForUser(userName);
  }

  @Override
  public Map<String, Object> getUserInfo(final String userName) {
    final ServerSecurityUser user = users.get(userName);
    if (user == null)
      return null;
    final Map<String, Object> info = new HashMap<>();
    info.put("name", user.getName());
    info.put("databases", user.getAuthorizedDatabases());
    return info;
  }

  @Override
  public void createUser(final String name, final String password) {
    final String encodedPassword = encodePassword(password);
    final JSONObject config = new JSONObject();
    config.put("name", name);
    config.put("password", encodedPassword);
    config.put("databases", new JSONObject().put("*", new JSONArray().put("admin")));
    createUser(config);
  }

  @Override
  public void setUserPassword(final String userName, final String password) {
    final ServerSecurityUser user = users.get(userName);
    if (user == null)
      throw new ServerSecurityException("User '" + userName + "' not found");
    user.setPassword(encodePassword(password));
    saveUsers();
    // A password change re-authenticates the principal: drop its live HTTP transaction sessions.
    invalidateHttpSessions(userName);
  }

  @Override
  public void updateSchema(final DatabaseInternal database) {
    if (database == null)
      return;

    for (final ServerSecurityUser user : users.values()) {
      final ServerSecurityDatabaseUser databaseUser = user.getDatabaseUser(database);
      if (databaseUser != null) {
        final JSONObject groupConfiguration = getDatabaseGroupsConfiguration(database.getName());
        if (groupConfiguration == null)
          continue;

        databaseUser.updateFileAccess(database, groupConfiguration);
      }
    }
  }

  public String getEncodedHash(final String password, final String salt, final int iterations) {
    // Returns only the last part of whole encoded password
    final KeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt.getBytes(StandardCharsets.UTF_8), iterations, 256);
    final SecretKey secret;
    try {
      secret = secretKeyFactory.generateSecret(keySpec);
    } catch (final InvalidKeySpecException e) {
      throw new ServerSecurityException("Error on generating security key", e);
    }

    final byte[] rawHash = secret.getEncoded();
    final byte[] hashBase64 = Base64.getEncoder().encode(rawHash);

    return new String(hashBase64, DatabaseFactory.getDefaultCharset());
  }

  protected String encodePassword(final String password, final String salt) {
    return this.encodePassword(password, salt, saltIteration);
  }

  public String encodePassword(final String userPassword) {
    return encodePassword(userPassword, ServerSecurity.generateRandomSalt());
  }

  public boolean passwordMatch(final String password, final String hashedPassword) {
    // hashedPassword consist of: ALGORITHM, ITERATIONS_NUMBER, SALT and
    // HASH; parts are joined with dollar character ("$")
    final String[] parts = hashedPassword.split("\\$");
    if (parts.length != 4)
      // wrong hash format
      return false;

    final int iterations;
    try {
      iterations = Integer.parseInt(parts[1]);
    } catch (final NumberFormatException e) {
      // malformed iteration count in the stored hash: treat as a non-match rather than throwing
      return false;
    }
    final String salt = parts[2];
    final String hash = encodePassword(password, salt, iterations);

    // Constant-time comparison to avoid leaking how many leading bytes matched via timing.
    return MessageDigest.isEqual(hash.getBytes(StandardCharsets.UTF_8), hashedPassword.getBytes(StandardCharsets.UTF_8));
  }

  protected static String generateRandomSalt() {
    final byte[] salt = new byte[SALT_SIZE];
    RANDOM.nextBytes(salt);
    return new String(Base64.getEncoder().encode(salt), DatabaseFactory.getDefaultCharset());
  }

  protected String encodePassword(final String password, final String salt, final int iterations) {
    // Key the cache on a SHA-256 digest of the plaintext+salt+iterations rather than the raw
    // password, so cleartext credentials are never retained as reachable map keys in the heap.
    final String cacheKey = saltCache != null ? saltCacheKey(password, salt, iterations) : null;

    if (cacheKey != null) {
      final String encoded = saltCache.get(cacheKey);
      if (encoded != null)
        // FOUND CACHED
        return encoded;
    }

    final String hash = getEncodedHash(password, salt, iterations);
    final String encoded = "%s$%d$%s$%s".formatted(algorithm, iterations, salt, hash);

    // CACHE IT
    if (cacheKey != null)
      saltCache.put(cacheKey, encoded);

    return encoded;
  }

  /**
   * Builds the salt-cache key as a SHA-256 hex digest of {@code password$salt$iterations}. The raw
   * password is never used as a key, so casual string-scanning of a heap/core dump or swap file
   * cannot recover plaintext credentials from the cache.
   * <p>
   * Residual risk (by design, not a defect): the salt and iteration count are also embedded in the
   * cached <em>value</em> (and the salt is not secret), so a full heap dump still lets an attacker
   * brute-force low-entropy passwords by computing one fast SHA-256 per guess. That is inherent to
   * caching a slow KDF under any recomputable-without-plaintext key; it is strictly better than the
   * previous plaintext-key exposure but weaker than the PBKDF2 hash it sits next to. Prefer
   * session-token auth for hot clients, and disable the cache
   * ({@code arcadedb.server.securitySaltCacheSize=0}) when even this residual exposure is
   * unacceptable.
   * <p>
   * The {@link MessageDigest} is held in a {@link ThreadLocal} to avoid a per-call
   * {@code getInstance} provider lookup on the Basic-auth hot path.
   */
  private static String saltCacheKey(final String password, final String salt, final int iterations) {
    // digest(byte[]) is a one-shot that resets the instance afterwards, so the reused ThreadLocal
    // digest is always clean at entry on this update-free path.
    final MessageDigest digest = SHA256_DIGEST.get();
    final byte[] hash = digest.digest((password + "$" + salt + "$" + iterations).getBytes(StandardCharsets.UTF_8));
    return HexFormat.of().formatHex(hash);
  }

  /**
   * Returns a snapshot of the current salt-cache keys, or an empty set when the cache is disabled.
   * Package-private: intended for tests that assert plaintext passwords are never retained as keys.
   */
  Set<String> getSaltCacheKeysSnapshot() {
    return saltCache != null ? saltCache.keys() : Set.of();
  }

  public List<JSONObject> usersToJSON() {
    final List<JSONObject> jsonl = new ArrayList<>(users.size());

    for (final ServerSecurityUser user : users.values())
      jsonl.add(user.toJSON());

    return jsonl;
  }

  public JSONObject groupsToJSON() {
    final JSONObject json = new JSONObject();

    // DATABASES TAKE FROM PREVIOUS CONFIGURATION
    json.put("databases", groupRepository.getGroups().getJSONObject("databases"));
    json.put("version", LATEST_VERSION);

    return json;
  }

  public void saveUsers() {
    try {
      usersRepository.save(usersToJSON());
    } catch (final IOException e) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Error on saving security configuration to file '%s'", e, SecurityUserFileRepository.FILE_NAME);
      // Propagate so an HTTP create/update/drop user request fails instead of falsely returning success
      // while nothing was persisted (the change would silently vanish on restart).
      throw new ServerSecurityException(
          "Error on saving security configuration to file '" + SecurityUserFileRepository.FILE_NAME + "'", e);
    }
  }

  /**
   * Returns the current users list as a JSON array string, suitable for HA
   * replication via {@code HAServerPlugin.replicateSecurityUsers}. The output is
   * the canonical representation of the in-memory users map.
   * <p>
   * Intentionally NOT {@code synchronized}: callers on the HTTP handler side use
   * {@code synchronized(server.getSecurity())} to serialise read-compute-submit
   * sequences. The state machine apply thread (which calls {@link #applyReplicatedUsers})
   * must NOT touch that monitor or it would deadlock with a handler thread waiting
   * for {@code submitAndWait} to complete.
   */
  public String getUsersJsonPayload() {
    final JSONArray array = new JSONArray();
    for (final JSONObject userJson : usersToJSON())
      array.put(userJson);
    return array.toString();
  }

  /**
   * Applies a replicated users payload: writes it to {@code server-users.jsonl}
   * and populates the in-memory users map directly from the payload.
   * Called from the Raft state machine on every peer when a SECURITY_USERS_ENTRY
   * is applied.
   * <p>
   * The in-memory map is built from the Raft payload rather than re-reading from
   * disk. In multi-server test setups (and potentially in embedded deployments),
   * multiple in-process servers may share the same config directory. Reading from
   * the file after writing would race with concurrent writes from other servers'
   * state machine threads, causing a server to load another server's (possibly
   * earlier) snapshot and permanently lose users.
   * <p>
   * Intentionally NOT {@code synchronized} on the ServerSecurity monitor: the
   * HTTP handler holds that monitor while waiting for {@code submitAndWait} to
   * complete, and the state machine apply thread needs to call this method to
   * unblock the wait. Raft state-machine apply is single-threaded per group, so
   * there is no internal contention here.
   */
  public void applyReplicatedUsers(final String usersJsonArray) {
    final JSONArray array = new JSONArray(usersJsonArray);
    final List<JSONObject> list = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      list.add(array.getJSONObject(i));

    try {
      usersRepository.save(list);
    } catch (final IOException e) {
      throw new ServerException("Failed to save replicated users file", e);
    }

    // Build in-memory map from the authoritative Raft payload, not from the file, and publish it in a
    // single atomic reference swap so concurrent readers never observe an empty/torn window.
    final Map<String, ServerSecurityUser> newUsers = new ConcurrentHashMap<>();
    for (final JSONObject userJson : list)
      newUsers.put(userJson.getString("name"), new ServerSecurityUser(server, userJson));
    this.users = newUsers;
  }

  public void saveGroups() {
    try {
      groupRepository.save(groupsToJSON());
    } catch (final IOException e) {
      LogManager.instance()
          .log(this, Level.SEVERE, "Error on saving security configuration to file '%s'", e, SecurityGroupFileRepository.FILE_NAME);
    }
  }

  public synchronized void saveGroup(final String database, final String name, final JSONObject groupConfig) {
    final JSONObject root = groupRepository.getGroups().copy();
    final JSONObject databases = root.getJSONObject("databases");

    if (!databases.has(database))
      databases.put(database, new JSONObject().put("groups", new JSONObject()));

    final JSONObject dbEntry = databases.getJSONObject(database);
    if (!dbEntry.has("groups"))
      dbEntry.put("groups", new JSONObject());

    dbEntry.getJSONObject("groups").put(name, groupConfig);

    root.put("version", LATEST_VERSION);
    try {
      groupRepository.save(root);
    } catch (final IOException e) {
      throw new ServerSecurityException("Error saving group configuration", e);
    }
  }

  public synchronized boolean deleteGroup(final String database, final String name) {
    final JSONObject root = groupRepository.getGroups().copy();
    final JSONObject databases = root.getJSONObject("databases");

    if (!databases.has(database))
      return false;

    final JSONObject dbEntry = databases.getJSONObject(database);
    if (!dbEntry.has("groups"))
      return false;

    final JSONObject groups = dbEntry.getJSONObject("groups");
    if (!groups.has(name))
      return false;

    groups.remove(name);

    root.put("version", LATEST_VERSION);
    try {
      groupRepository.save(root);
    } catch (final IOException e) {
      throw new ServerSecurityException("Error saving group configuration", e);
    }
    return true;
  }

  protected void askForRootPassword() throws IOException {
    String rootPassword = server != null ?
        server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD) :
        GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();

    if (rootPassword == null) {
      final String rootPasswordPath = server != null ?
          server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH) :
          GlobalConfiguration.SERVER_ROOT_PASSWORD_PATH.getValueAsString();

      if (rootPasswordPath != null) {
        if (Files.isReadable(Path.of(rootPasswordPath)))
          rootPassword = Files.readString(Path.of(rootPasswordPath));
        else
          throw new ServerSecurityException("Error reading password file at path '" + rootPasswordPath + "'");
      }
    }

    if (rootPassword == null) {
      if (server != null ?
          server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_K8S) :
          GlobalConfiguration.HA_K8S.getValueAsBoolean()) {
        // UNDER KUBERNETES IF THE ROOT PASSWORD IS NOT SET (USUALLY WITH A SECRET) THE POD MUST TERMINATE
        LogManager.instance().log(this, Level.SEVERE,
            "Unable to start a server under Kubernetes if the environment variable `arcadedb.server.rootPassword` is not set");
        throw new ServerSecurityException(
            "Unable to start a server under Kubernetes if the environment variable `arcadedb.server.rootPassword` is not set");
      }

      LogManager.instance().flush();
      System.err.flush();
      System.out.flush();

      System.out.println();
      System.out.println();
      System.out.println(AnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------+}"));
      System.out.println(AnsiCode.format("$ANSI{yellow |                WARNING: FIRST RUN CONFIGURATION                    |}"));
      System.out.println(AnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------+}"));
      System.out.println(AnsiCode.format("$ANSI{yellow | This is the first time the server is running. Please type a        |}"));
      System.out.println(AnsiCode.format("$ANSI{yellow | password of your choice for the 'root' user or leave it blank      |}"));
      System.out.println(AnsiCode.format("$ANSI{yellow | to auto-generate it.                                               |}"));
      System.out.println(AnsiCode.format("$ANSI{yellow |                                                                    |}"));
      System.out.println(AnsiCode.format("$ANSI{yellow | To avoid this message set the environment variable or JVM          |}"));
      System.out.println(AnsiCode.format("$ANSI{yellow | setting `arcadedb.server.rootPassword` to the root password to use.|}"));
      System.out.println(AnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------+}"));

      final DefaultConsoleReader console = new DefaultConsoleReader();

      // ASK FOR PASSWORD + CONFIRM
      do {
        System.out.print(AnsiCode.format("\n$ANSI{yellow Root password [BLANK=auto generate it]: }"));
        rootPassword = console.readPassword();

        if (rootPassword != null) {
          rootPassword = rootPassword.trim();
          if (rootPassword.isEmpty())
            rootPassword = null;
        }

        if (rootPassword == null) {
          rootPassword = credentialsValidator.generateRandomPassword();
          System.out.print(AnsiCode.format(
              "Automatic generated password: $ANSI{green " + rootPassword + "}. Please save it in a safe place.\n"));
        }

        if (rootPassword != null) {
          System.out.print(
              AnsiCode.format("$ANSI{yellow Please type the root password for confirmation (copy and paste will not work): }"));

          String rootConfirmPassword = console.readPassword();
          if (rootConfirmPassword != null) {
            rootConfirmPassword = rootConfirmPassword.trim();
            if (rootConfirmPassword.isEmpty())
              rootConfirmPassword = null;
          }

          if (!rootPassword.equals(rootConfirmPassword)) {
            System.out.println(AnsiCode.format(
                "$ANSI{red ERROR: Passwords do not match, please reinsert both of them, or press ENTER to auto generate it}"));
            try {
              Thread.sleep(500);
            } catch (final InterruptedException e) {
              return;
            }
            rootPassword = null;
          } else
            // PASSWORDS MATCH

            try {
              credentialsValidator.validateCredentials("root", rootPassword);
              // PASSWORD IS STRONG ENOUGH
              break;
            } catch (final ServerSecurityException ex) {
              System.out.println(AnsiCode.format(
                  "$ANSI{red ERROR: Root password does not match the password policies" + (ex.getMessage() != null ?
                      ": " + ex.getMessage() :
                      "") + "}"));
              try {
                Thread.sleep(500);
              } catch (final InterruptedException e) {
                return;
              }
              rootPassword = null;
            }
        }

      } while (rootPassword == null);
    } else
      LogManager.instance().log(this, Level.INFO, "Creating root user with the provided password");

    credentialsValidator.validateCredentials("root", rootPassword);

    final String encodedPassword = encodePassword(rootPassword, ServerSecurity.generateRandomSalt());

    if (existsUser("root")) {
      getUser("root").setPassword(encodedPassword);
      saveUsers();
    } else
      createUser(new JSONObject().put("name", "root").put("password", encodedPassword));
  }

  public ApiTokenConfiguration getApiTokenConfiguration() {
    return apiTokenConfig;
  }

  public ServerSecurityUser authenticateByApiToken(final String tokenValue) {
    // Brute-force protection: track failed attempts by token hash prefix.
    // Check lockout before looking up the token so locked-out attackers are rejected fast.
    final String attemptKey = ApiTokenConfiguration.hashToken(tokenValue).substring(0, 8);
    final long now = System.currentTimeMillis();

    final long[] existing = tokenFailures.get(attemptKey);
    if (existing != null && existing[0] >= MAX_TOKEN_FAILURES && now - existing[1] < TOKEN_LOCKOUT_MS)
      throw new ServerSecurityException("Too many failed authentication attempts. Try again later.");

    final JSONObject tokenJson = apiTokenConfig.getToken(tokenValue);
    if (tokenJson == null) {
      // Token not found: increment failure count atomically.
      tokenFailures.compute(attemptKey, (k, entry) -> {
        final long ts = System.currentTimeMillis();
        if (entry == null)
          return new long[] { 1, ts };
        if (ts - entry[1] > TOKEN_LOCKOUT_MS) {
          entry[0] = 1;
          entry[1] = ts;
        } else
          entry[0]++;
        return entry;
      });
      throw new ServerSecurityException("Invalid or expired API token");
    }

    // Clear failure count on successful authentication
    tokenFailures.remove(attemptKey);

    final String database = tokenJson.getString("database");
    final String tokenName = tokenJson.getString("name");
    final String tokenHash = tokenJson.getString("tokenHash");
    final JSONObject permissions = tokenJson.getJSONObject("permissions");

    // Build synthetic group config from token permissions (use hash prefix for collision resistance)
    final String syntheticGroupName = "_apitoken_" + tokenHash.substring(0, 16);
    final JSONObject groupDef = new JSONObject();

    // Types permissions
    if (permissions.has("types"))
      groupDef.put("types", permissions.getJSONObject("types"));
    else
      groupDef.put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray())));

    // Database-level access
    if (permissions.has("database"))
      groupDef.put("access", permissions.getJSONArray("database"));
    else
      groupDef.put("access", new JSONArray());

    groupDef.put("resultSetLimit", -1L);
    groupDef.put("readTimeout", -1L);

    final JSONObject syntheticGroupConfig = new JSONObject();
    syntheticGroupConfig.put(syntheticGroupName, groupDef);

    // Build synthetic user configuration
    final JSONObject userConfig = new JSONObject();
    userConfig.put("name", "apitoken:" + tokenName);
    userConfig.put("databases", new JSONObject().put(database, new JSONArray().put(syntheticGroupName)));

    final ServerSecurityUser user = new ServerSecurityUser(server, userConfig);
    user.withSyntheticGroupConfig(syntheticGroupConfig);
    return user;
  }

  protected JSONObject getDatabaseGroupsConfiguration(final String databaseName) {
    final JSONObject groupDatabases = groupRepository.getGroups().getJSONObject("databases");

    // When the caller asks for the wildcard itself, return the wildcard live reference directly — callers
    // (setup helpers, reload tests) rely on mutating the returned object.
    if (SecurityManager.ANY.equals(databaseName)) {
      final JSONObject wildcard = groupDatabases.has(SecurityManager.ANY) ? groupDatabases.getJSONObject("*") : null;
      return wildcard != null && wildcard.has("groups") ? wildcard.getJSONObject("groups") : null;
    }

    final JSONObject wildcardConfiguration = groupDatabases.has(SecurityManager.ANY) ? groupDatabases.getJSONObject("*") : null;
    final JSONObject specificConfiguration = groupDatabases.has(databaseName) ? groupDatabases.getJSONObject(databaseName) : null;

    final JSONObject wildcardGroups =
        wildcardConfiguration != null && wildcardConfiguration.has("groups") ? wildcardConfiguration.getJSONObject("groups") : null;
    final JSONObject specificGroups =
        specificConfiguration != null && specificConfiguration.has("groups") ? specificConfiguration.getJSONObject("groups") : null;

    if (wildcardGroups == null && specificGroups == null)
      return null;

    // When there is no db-specific entry, return the wildcard groups live reference so existing callers that mutate it
    // (e.g. setup helpers doing getDatabaseGroupsConfiguration(db).put(groupName, ...)) keep working.
    if (specificGroups == null)
      return wildcardGroups;
    if (wildcardGroups == null)
      return specificGroups;

    // MERGE: wildcard groups are the baseline, db-specific entries override same-named groups. The returned object is
    // a snapshot copy; callers that need to persist groups must go through saveGroup/deleteGroup.
    final JSONObject merged = new JSONObject();
    for (final String groupName : wildcardGroups.keySet()) {
      final Object value = wildcardGroups.get(groupName);
      if (value instanceof JSONObject)
        merged.put(groupName, value);
    }
    for (final String groupName : specificGroups.keySet()) {
      final Object value = specificGroups.get(groupName);
      if (value instanceof JSONObject)
        merged.put(groupName, value);
    }
    return merged;
  }
}
