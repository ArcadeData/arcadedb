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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.DefaultConsoleReader;
import com.arcadedb.server.ServerException;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.security.credential.CredentialsValidator;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import com.arcadedb.server.security.oidc.ArcadeRole;
import com.arcadedb.server.security.oidc.Group;
import com.arcadedb.server.security.oidc.GroupMap;
import com.arcadedb.server.security.oidc.GroupTypeAccess;
import com.arcadedb.server.security.oidc.KeycloakClient;
import com.arcadedb.server.security.oidc.KeycloakUser;
import com.arcadedb.server.security.oidc.User;
import com.arcadedb.server.security.oidc.role.DatabaseAdminRole;
import com.arcadedb.server.security.oidc.role.RoleType;
import com.arcadedb.server.security.oidc.role.ServerAdminRole;
import com.arcadedb.utility.AnsiCode;
import com.arcadedb.utility.LRUCache;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;

import lombok.extern.slf4j.Slf4j;

import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import java.io.*;
import java.nio.charset.*;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.*;
import java.util.stream.Collectors;

import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_ALGORITHM;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS;

@Slf4j
public class ServerSecurity implements ServerPlugin, com.arcadedb.security.SecurityManager {

  public static final int LATEST_VERSION = 1;
  private static final int CHECK_USER_RELOAD_EVERY_MS = 5_000;
  private final ArcadeDBServer server;
  private final SecurityUserFileRepository usersRepository;
  private final SecurityGroupFileRepository groupRepository;
  private final String algorithm;
  private final SecretKeyFactory secretKeyFactory;
  private final Map<String, String> saltCache;
  private final int saltIteration;
  private final ConcurrentHashMap<String, ServerSecurityUser> users = new ConcurrentHashMap<>();
  private CredentialsValidator credentialsValidator = new DefaultCredentialsValidator();
  private static final Random RANDOM = new SecureRandom();
  public static final int SALT_SIZE = 32;
  private Timer reloadConfigurationTimer;

  /**
   * 1. request comes in
   * 2. check cache for user
   * 3. if user not in cache, check keycloak for user
   * 4. if user not in keycloak, throw error
   * 5. if user in keycloak, check if user is authorized to hit arcade
   * 6. if user is authorized to hit arcade, get arcade roles from jwt
   * 7. convert arcade roles to groups
   * 8. get current groups
   * 9. compare current groups to needed groups
   * 10. create missing groups
   * 11. create user if doesn't already exist
   * 12. overwrite user group memberships
   * 13. put user in local map
   * 14. return new/updated user
   */

  // TODO create job to refresh user permissions from keycloak frequently

  // TODO create job to remove users from map after extended inactivity

  // TODO listen to kafka events for role updates, update local users

  // TODO use PG database repo for users and groups

  // TODO periodically save users and groups to file repositories

  private ConcurrentHashMap<String, GroupMap> groups = new ConcurrentHashMap<>();

  private ConcurrentHashMap<String, List<ArcadeRole>> userArcadeRoles = new ConcurrentHashMap<>();

  public ServerSecurity(final ArcadeDBServer server, final ContextConfiguration configuration,
      final String configPath) {
    this.server = server;
    this.algorithm = configuration.getValueAsString(SERVER_SECURITY_ALGORITHM);

    final int cacheSize = configuration.getValueAsInteger(SERVER_SECURITY_SALT_CACHE_SIZE);

    if (cacheSize > 0)
      saltCache = Collections.synchronizedMap(new LRUCache<>(cacheSize));
    else
      saltCache = Collections.emptyMap();

    saltIteration = configuration.getValueAsInteger(SERVER_SECURITY_SALT_ITERATIONS);

    usersRepository = new SecurityUserFileRepository(configPath);
    groupRepository = new SecurityGroupFileRepository(configPath).onReload((latestConfiguration) -> {
      for (final String databaseName : server.getDatabaseNames()) {
        updateSchema((DatabaseInternal) server.getDatabase(databaseName));
      }
      return null;
    });

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
      users.clear();

      try {
        for (final JSONObject userJson : usersRepository.getUsers()) {
          final ServerSecurityUser user = new ServerSecurityUser(server, userJson);
          users.put(user.getName(), user);
        }
      } catch (final JSONException e) {
        groupRepository.saveInError(e);
        for (final JSONObject userJson : SecurityUserFileRepository.createDefault()) {
          final ServerSecurityUser user = new ServerSecurityUser(server, userJson);
          users.put(user.getName(), user);
        }
      }

      // TODO eagerly load users from keycloak?
      // add to cache

      if (users.isEmpty() || (users.containsKey("root") && users.get("root").getPassword() == null))
        askForRootPassword();

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
        }, CHECK_USER_RELOAD_EVERY_MS, CHECK_USER_RELOAD_EVERY_MS);
      }

    } catch (final IOException e) {
      throw new ServerException("Error on starting Security service", e);
    }
  }

  @Override
  public void stopService() {
    if (reloadConfigurationTimer != null)
      reloadConfigurationTimer.cancel();

    users.clear();
    if (groupRepository != null)
      groupRepository.stop();
  }

  private KeycloakUser createKeycloakUser(String username) {
    List<String> roles = KeycloakClient.getUserClientRoles(username);
    return new KeycloakUser(username, roles);
  }

  /**
   * Parse ArcadeRole objects from jwt role name strings
   * 
   * @param jwtRoles
   * @return
   */
  private List<ArcadeRole> getArcadeRolesFromJwtRoles(List<String> jwtRoles) {
    List<ArcadeRole> arcadeRoles = new ArrayList<>();
    for (String role : jwtRoles) {
      if (ArcadeRole.isArcadeRole(role)) {
        var arcadeRole = ArcadeRole.valueOf(role);
        if (arcadeRole != null) {
          arcadeRoles.add(arcadeRole);
        }
      }
    }
    return arcadeRoles;
  }

  /**
   * Create a Group object from an ArcadeRole object such that it can be added to
   * the arcade ACLs
   * 
   * @param arcadeRole
   * @return
   */
  private Group getGroupFromArcadeRole(ArcadeRole arcadeRole) {
    log.debug("getGroupFromArcadeRole role {}", arcadeRole.toString());
    Group group = new Group();
    group.setName(arcadeRole.getName());

    if (arcadeRole.getDatabase() != null) {
      group.setDatabase(arcadeRole.getDatabase());
    }

    // TODO test this
    if (arcadeRole.getDatabaseAdminRole() != null) {
      group.setAccess(List.of(arcadeRole.getDatabaseAdminRole().getArcadeName()));
    }
    group.setReadTimeout(arcadeRole.getReadTimeout());
    group.setResultSetLimit(arcadeRole.getResultSetLimit());

    if (arcadeRole.getRoleType() == RoleType.USER) {
      if (arcadeRole.getDatabase() != null) {
        GroupTypeAccess typeAccess = new GroupTypeAccess(arcadeRole.getCrudPermissionsAsArcadeNames());
        // TODO update this to grab all matching types from arcade, and create type
        // entry for each
        group.setTypes(Map.of(arcadeRole.getTableRegex(), typeAccess));
      }
    } else if (arcadeRole.getRoleType() == RoleType.DATABASE_ADMIN) {
      if (arcadeRole.getDatabaseAdminRole().getKeycloakName().equals(DatabaseAdminRole.ALL.getKeycloakName())) {
        var rolesToAssign = List.of(DatabaseAdminRole.values())
            .stream()
            .filter(r -> !r.getKeycloakName().equals(DatabaseAdminRole.ALL.getKeycloakName()))
            .map(r -> r.getArcadeName())
            .collect(Collectors.toList());
        group.setAccess(rolesToAssign);
      } else {
        group.setAccess(List.of(arcadeRole.getDatabaseAdminRole().getArcadeName()));
      }
    }

    // Server admin roles grant permissions to server comamnds exposed by the REST
    // API

    log.debug("getGroupFromArcadeRole group {}", arcadeRole.toString());
    // TODO add type regex support
    // need to get all types for database from arcade, and apply the regex match to
    // get the types that match
    // then add the types to the group
    // skip regex support for now
    return group;
  }

  /**
   * Returns a user from local cache if found, or creates a new user from keycloak
   * 
   * @param username
   * @return
   * @throws JsonProcessingException
   */
  private ServerSecurityUser getOrCreateUser(final String username) {

    log.debug("getOrCreateUser {}", username);
    // Return user from local cache if found. If not, continue
    // TOOD update this to check for all users in cache, not just root
    if (users.containsKey(username) && username.equals("root")) {
      return users.get(username);
    }

    // 1. get user from keycloak
    KeycloakUser keycloakUser = createKeycloakUser(username);

    if (keycloakUser == null) {
      // user doesn't exist in keycloak
      throw new ServerSecurityException("User not found");
    }

    log.debug("returned user {}", keycloakUser.toString());

    // 2. Check if user is authoried to hit arcade
    List<String> arcadeJwtRoles = keycloakUser.getRoles()
        .stream()
        .filter(r -> ArcadeRole.isArcadeRole(r))
        .collect(Collectors.toList());

    if (arcadeJwtRoles.isEmpty()) {
      // not an authorized arcade user
      throw new ServerSecurityException("User not authorized");
    }

    // 3. get any arcade roles from jwt
    List<ArcadeRole> arcadeRoles = getArcadeRolesFromJwtRoles(arcadeJwtRoles);
    userArcadeRoles.put(username, arcadeRoles);

    log.debug("getOrCreateuser - parsed arcade roles {}", arcadeRoles.toString());

    // 4. Convert arcade roles to groups
    List<Group> neededGroups = arcadeRoles.stream()
        .map(this::getGroupFromArcadeRole)
        .collect(Collectors.toList());

    // 5. Create and add any missing groups
    neededGroups.stream().forEach(g -> {
      // if group isn't in currentGroups, add it
      if (g.getDatabase() != null) {
        if (groups.containsKey(g.getDatabase())) {
          groups.get(g.getDatabase()).getGroups().put(g.getArcadeName(), g);
        } else {
          GroupMap groupMap = new GroupMap();
          groupMap.getGroups().put(g.getArcadeName(), g);
          groups.put(g.getDatabase(), groupMap);
        }
      }
      // creating an edge is technically 2 operations: (1) create a new edge record
      // and (2) update the vertices with
      // the reference. For this reason, if you want to allow a user to create edges,
      // you have to grant the createRecord
      // permission on the edge type and updateRecord on the vertex type.
    });

    // 6. Create new user config
    Map<String, List<String>> groupMap = new HashMap<>();
    for (Group group : neededGroups) {
      if (!groupMap.containsKey(group.getDatabase())) {
        groupMap.put(group.getDatabase(), new ArrayList<>());
      }
      groupMap.get(group.getDatabase()).add(group.getArcadeName());
    }
    User user = new User(keycloakUser.getUsername(), groupMap);

    JSONObject userJson = new JSONObject();
    userJson.put("name", user.getName());

    JSONObject databases = new JSONObject();
    for (String database : user.getDatabaseGroups().keySet()) {
      databases.put(database, user.getDatabaseGroups().get(database));
    }
    userJson.put("databases", databases);

    log.debug("getOrCreateUser userJson {}", userJson.toString());
    ServerSecurityUser serverSecurityUser = new ServerSecurityUser(server, userJson, arcadeRoles);
    users.put(username, serverSecurityUser);

    return serverSecurityUser;

    // TODO improvement. Cache local perms. Listen for kafka events for role
    // updates?
    // or remove users on logout, and recreate. probably much easier
  }

  // TODO more improvements
  // 0. figure out service account usage from this app
  // 0.5. implement service account usage from this app
  // 0.7 helm chart update to enable SA usage? could hardcode for now

  // TODO create logout method

  // TODO add prop for last activity
  // TODO create job to remove users from map after extended inactivity

  // TODO store hash of user roles in jwt in the local hashmap. Skip user if hash
  // matches and database schema last updated is older than expiration time.

  public ServerSecurityUser authenticate(final String userName, final String databaseName) {

    final ServerSecurityUser su = getOrCreateUser(userName);
    if (su == null) {
      throw new ServerSecurityException("User not valid");
    }

    if (databaseName != null) {
      final Set<String> allowedDatabases = su.getAuthorizedDatabases();
      if (!allowedDatabases.contains(SecurityManager.ANY) && !su.getAuthorizedDatabases().contains(databaseName)) {
        throw new ServerSecurityException("User does not have access to database '" + databaseName + "'");
      }
    }

    return su;
  }

  public ServerSecurityUser authenticate(final String userName, final String userPassword, final String databaseName) {

    final ServerSecurityUser su = authenticate(userName, databaseName);
    if (!passwordMatch(userPassword, su.getPassword())) {
      throw new ServerSecurityException("User/Password not valid");
    }

    users.put(su.getName(), su);

    return su;
  }

  /**
   * Override the default @{@link CredentialsValidator} implementation
   * (@{@link DefaultCredentialsValidator}) providing a custom one.
   */
  public void setCredentialsValidator(final CredentialsValidator credentialsValidator) {
    this.credentialsValidator = credentialsValidator;
  }

  public boolean existsUser(final String userName) {
    return users.containsKey(userName);
  }

  // TODO update this to pull from cache first, and only hit keycloak on a cache
  // miss
  public ServerSecurityUser getUser(final String userName) {
    return getOrCreateUser(userName);
    // return users.get(userName);
  }

  public ServerSecurityUser createUser(final JSONObject userConfiguration) {
    log.debug("create user {}", userConfiguration.toString());
    final String name = userConfiguration.getString("name");
    if (users.containsKey(name))
      throw new SecurityException("User '" + name + "' already exists");

    final ServerSecurityUser user = new ServerSecurityUser(server, userConfiguration);
    users.put(name, user);
    saveUsers();
    return user;
  }

  public boolean dropUser(final String userName) {
    if (users.remove(userName) != null) {
      saveUsers();
      return true;
    }
    return false;
  }

  @Override
  public void updateSchema(final DatabaseInternal database) {
    if (database == null)
      return;

    for (final ServerSecurityUser user : users.values()) {
      final ServerSecurityDatabaseUser databaseUser = user.getDatabaseUser(database);
      if (databaseUser != null) {

        // TODO update
        final JSONObject groupConfiguration = getDatabaseGroupsConfiguration(database.getName());
        if (groupConfiguration == null)
          continue;
        databaseUser.updateFileAccess(database, groupConfiguration);
      }
    }
  }

  public String getEncodedHash(final String password, final String salt, final int iterations) {
    // Returns only the last part of whole encoded password
    final KeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt.getBytes(StandardCharsets.UTF_8), iterations,
        256);
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

    final int iterations = Integer.parseInt(parts[1]);
    final String salt = parts[2];
    final String hash = encodePassword(password, salt, iterations);

    return hash.equals(hashedPassword);
  }

  protected static String generateRandomSalt() {
    final byte[] salt = new byte[SALT_SIZE];
    RANDOM.nextBytes(salt);
    return new String(Base64.getEncoder().encode(salt), DatabaseFactory.getDefaultCharset());
  }

  protected String encodePassword(final String password, final String salt, final int iterations) {
    if (!saltCache.isEmpty()) {
      final String encoded = saltCache.get(password + "$" + salt + "$" + iterations);
      if (encoded != null)
        // FOUND CACHED
        return encoded;
    }

    final String hash = getEncodedHash(password, salt, iterations);
    final String encoded = String.format("%s$%d$%s$%s", algorithm, iterations, salt, hash);

    // CACHE IT
    saltCache.put(password + "$" + salt + "$" + iterations, encoded);

    return encoded;
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
      LogManager.instance().log(this, Level.SEVERE, "Error on saving security configuration to file '%s'", e,
          SecurityUserFileRepository.FILE_NAME);
    }
  }

  public void saveGroups() {
    try {
      groupRepository.save(groupsToJSON());
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving security configuration to file '%s'", e,
          SecurityGroupFileRepository.FILE_NAME);
    }
  }

  protected void askForRootPassword() throws IOException {
    String rootPassword = server != null
        ? server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD)
        : GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();

    if (rootPassword == null) {
      if (server != null ? server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_K8S)
          : GlobalConfiguration.HA_K8S.getValueAsBoolean()) {
        // UNDER KUBERNETES IF THE ROOT PASSWORD IS NOT SET (USUALLY WITH A SECRET) THE
        // POD MUST TERMINATE
        LogManager.instance()
            .log(this, Level.SEVERE,
                "Unable to start a server under Kubernetes if the environment variable `arcadedb.server.rootPassword` is not set");
        throw new ServerSecurityException(
            "Unable to start a server under Kubernetes if the environment variable `arcadedb.server.rootPassword` is not set");
      }

      LogManager.instance().flush();
      System.err.flush();
      System.out.flush();

      System.out.println();
      System.out.println();
      System.out.println(
          AnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------+}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow |                WARNING: FIRST RUN CONFIGURATION                    |}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------+}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow | This is the first time the server is running. Please type a        |}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow | password of your choice for the 'root' user or leave it blank      |}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow | to auto-generate it.                                               |}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow |                                                                    |}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow | To avoid this message set the environment variable or JVM          |}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow | setting `arcadedb.server.rootPassword` to the root password to use.|}"));
      System.out.println(
          AnsiCode.format("$ANSI{yellow +--------------------------------------------------------------------+}"));

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
          System.out.print(AnsiCode
              .format("$ANSI{yellow Please type the root password for confirmation (copy and paste will not work): }"));

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
                  "$ANSI{red ERROR: Root password does not match the password policies"
                      + (ex.getMessage() != null ? ": " + ex.getMessage() : "") + "}"));
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

  protected JSONObject getDatabaseGroupsConfiguration(final String databaseName) {
    Gson gson = new Gson();
    String jsonString = gson.toJson(groups);
    log.debug("s getDatabaseGroupsConfiguration jsonString {} ", jsonString);
    final JSONObject groupDatabases = new JSONObject(jsonString);

    JSONObject databaseConfiguration = groupDatabases.has(databaseName) ? groupDatabases.getJSONObject(databaseName)
        : null;

    if (databaseConfiguration == null)
      // GET DEFAULT (*) DATABASE GROUPS
      databaseConfiguration = groupDatabases.has(SecurityManager.ANY) ? groupDatabases.getJSONObject("*") : null;
    if (databaseConfiguration == null || !databaseConfiguration.has("groups"))
      return null;
    return databaseConfiguration.getJSONObject("groups");
  }

  /**
   * Temp method- checks if the user is the built in root user before proceeding
   * with the check
   * TODO delete this when we stop using the built in root user. Need to make sure
   * spark jobs don't use root user.
   * 
   * @param user
   * @return
   */
  private boolean preRolePermissionCheck(final ServerSecurityUser user) {
    if (user == null)
      throw new ServerSecurityException("User not authenticated");

    // temp allow root to do anything
    // TODO remove this, or make and reference configuration enabling root user
    if (user.getName().equals("root")) {
      return true;
    }

    return false;
  }

  /**
   * Checks if the user has any of the permissions in the list necessary for the
   * current action they're attempting.
   * 
   * @param user
   * @param roles
   * @return
   */
  public boolean checkUserHasAnyServerAdminRole(final ServerSecurityUser user, List<ServerAdminRole> roles) {
    if (preRolePermissionCheck(user)) {
      return true;
    }

    if (userArcadeRoles.containsKey(user.getName())) {
      return userArcadeRoles.get(user.getName())
          .stream().anyMatch(r -> r.getRoleType() == RoleType.SERVER_ADMIN && roles.contains(r.getServerAdminRole()));
    }

    return false;
  }

  public void appendArcadeRoleToUserCache(String username, String role) {
    userArcadeRoles.computeIfAbsent(username, u -> new CopyOnWriteArrayList<>()).add(ArcadeRole.valueOf(role));
  }
}
