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
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.credential.CredentialsValidator;
import com.arcadedb.server.security.credential.DefaultCredentialsValidator;
import com.arcadedb.utility.AnsiCode;
import com.arcadedb.utility.LRUCache;
import io.undertow.server.handlers.PathHandler;
import org.json.JSONException;
import org.json.JSONObject;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.*;
import java.nio.charset.*;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import java.util.logging.*;

import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_ALGORITHM;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS;

public class ServerSecurity implements ServerPlugin, com.arcadedb.security.SecurityManager {

  public static final  int                             LATEST_VERSION             = 1;
  private static final int                             CHECK_USER_RELOAD_EVERY_MS = 5_000;
  private final        ArcadeDBServer                  server;
  private final        SecurityUserFileRepository      usersRepository;
  private final        SecurityGroupFileRepository     groupRepository;
  private final        String                          algorithm;
  private final        SecretKeyFactory                secretKeyFactory;
  private final        Map<String, String>             saltCache;
  private final        int                             saltIteration;
  private final        Map<String, ServerSecurityUser> users                      = new HashMap<>();
  private              CredentialsValidator            credentialsValidator       = new DefaultCredentialsValidator();
  private static final Random                          RANDOM                     = new SecureRandom();
  public static final  int                             SALT_SIZE                  = 32;
  private              Timer                           reloadConfigurationTimer;

  public ServerSecurity(final ArcadeDBServer server, final ContextConfiguration configuration, final String configPath) {
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
      for (String databaseName : server.getDatabaseNames()) {
        updateSchema((DatabaseInternal) server.getDatabase(databaseName));
      }
      return null;
    });

    try {
      secretKeyFactory = SecretKeyFactory.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      LogManager.instance().log(this, Level.SEVERE, "Security algorithm '%s' not available (error=%s)", e, algorithm);
      throw new ServerSecurityException("Security algorithm '" + algorithm + "' not available", e);
    }
  }

  @Override
  public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
  }

  @Override
  public void startService() {
  }

  public void loadUsers() {
    try {
      users.clear();

      try {
        for (JSONObject userJson : usersRepository.getUsers()) {
          final ServerSecurityUser user = new ServerSecurityUser(server, userJson);
          users.put(user.getName(), user);
        }
      } catch (JSONException e) {
        groupRepository.saveInError(e);
        for (JSONObject userJson : SecurityUserFileRepository.createDefault()) {
          final ServerSecurityUser user = new ServerSecurityUser(server, userJson);
          users.put(user.getName(), user);
        }
      }

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

    } catch (IOException e) {
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

  public ServerSecurityUser authenticate(final String userName, final String userPassword, final String databaseName) {

    final ServerSecurityUser su = users.get(userName);
    if (su == null)
      throw new ServerSecurityException("User/Password not valid");

    if (!passwordMatch(userPassword, su.getPassword()))
      throw new ServerSecurityException("User/Password not valid");

    if (databaseName != null) {
      final Set<String> allowedDatabases = su.getAuthorizedDatabases();
      if (!allowedDatabases.contains(SecurityManager.ANY) && !su.getAuthorizedDatabases().contains(databaseName))
        throw new ServerSecurityException("User has not access to database '" + databaseName + "'");
    }

    return su;
  }

  /**
   * Override the default @{@link CredentialsValidator} implementation (@{@link DefaultCredentialsValidator}) providing a custom one.
   */
  public void setCredentialsValidator(final CredentialsValidator credentialsValidator) {
    this.credentialsValidator = credentialsValidator;
  }

  public boolean existsUser(final String userName) {
    return users.containsKey(userName);
  }

  public ServerSecurityUser getUser(final String userName) {
    return users.get(userName);
  }

  public ServerSecurityUser createUser(final JSONObject userConfiguration) {
    final String name = userConfiguration.getString("name");
    if (users.containsKey(name))
      throw new SecurityException("User '" + name + "' already exists");

    final ServerSecurityUser user = new ServerSecurityUser(server, userConfiguration);
    users.put(name, user);
    saveUsers();
    return user;
  }

  public void dropUser(final String userName) {
    if (users.remove(userName) != null)
      saveUsers();
  }

  @Override
  public void updateSchema(final DatabaseInternal database) {
    if (database == null)
      return;

    for (ServerSecurityUser user : users.values()) {
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
    } catch (InvalidKeySpecException e) {
      throw new ServerSecurityException("Error on generating security key", e);
    }

    final byte[] rawHash = secret.getEncoded();
    final byte[] hashBase64 = Base64.getEncoder().encode(rawHash);

    return new String(hashBase64, DatabaseFactory.getDefaultCharset());
  }

  @Override
  public void registerAPI(HttpServer httpServer, final PathHandler routes) {
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

    for (ServerSecurityUser user : users.values())
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
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving security configuration to file '%s'", e, SecurityUserFileRepository.FILE_NAME);
    }
  }

  public void saveGroups() {
    try {
      groupRepository.save(groupsToJSON());
    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving security configuration to file '%s'", e, SecurityGroupFileRepository.FILE_NAME);
    }
  }

  protected void askForRootPassword() throws IOException {
    String rootPassword = server != null ?
        server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD) :
        GlobalConfiguration.SERVER_ROOT_PASSWORD.getValueAsString();

    if (rootPassword == null) {
      if (server != null ? server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_K8S) : GlobalConfiguration.HA_K8S.getValueAsBoolean()) {
        // UNDER KUBERNETES IF THE ROOT PASSWORD IS NOT SET (USUALLY WITH A SECRET) THE POD MUST TERMINATE
        LogManager.instance()
            .log(this, Level.SEVERE, "Unable to start a server under Kubernetes if the environment variable `arcadedb.server.rootPassword` is not set");
        throw new ServerSecurityException("Unable to start a server under Kubernetes if the environment variable `arcadedb.server.rootPassword` is not set");
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
          System.out.print(AnsiCode.format("Automatic generated password: $ANSI{green " + rootPassword + "}. Please save it in a safe place.\n"));
        }

        if (rootPassword != null) {
          System.out.print(AnsiCode.format("$ANSI{yellow Please type the root password for confirmation (copy and paste will not work): }"));

          String rootConfirmPassword = console.readPassword();
          if (rootConfirmPassword != null) {
            rootConfirmPassword = rootConfirmPassword.trim();
            if (rootConfirmPassword.isEmpty())
              rootConfirmPassword = null;
          }

          if (!rootPassword.equals(rootConfirmPassword)) {
            System.out.println(AnsiCode.format("$ANSI{red ERROR: Passwords don't match, please reinsert both of them, or press ENTER to auto generate it}"));
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              return;
            }
            rootPassword = null;
          } else
            // PASSWORDS MATCH

            try {
              credentialsValidator.validateCredentials("root", rootPassword);
              // PASSWORD IS STRONG ENOUGH
              break;
            } catch (ServerSecurityException ex) {
              System.out.println(AnsiCode.format(
                  "$ANSI{red ERROR: Root password does not match the password policies" + (ex.getMessage() != null ? ": " + ex.getMessage() : "") + "}"));
              try {
                Thread.sleep(500);
              } catch (InterruptedException e) {
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
    final JSONObject groupDatabases = groupRepository.getGroups().getJSONObject("databases");
    JSONObject databaseConfiguration = groupDatabases.has(databaseName) ? groupDatabases.getJSONObject(databaseName) : null;
    if (databaseConfiguration == null)
      // GET DEFAULT (*) DATABASE GROUPS
      databaseConfiguration = groupDatabases.has(SecurityManager.ANY) ? groupDatabases.getJSONObject("*") : null;
    if (databaseConfiguration == null || !databaseConfiguration.has("groups"))
      return null;
    return databaseConfiguration.getJSONObject("groups");
  }
}
