/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.security;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
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

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.*;
import java.nio.charset.*;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_ALGORITHM;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE;
import static com.arcadedb.GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS;

public class ServerSecurity implements ServerPlugin {

  private final        ArcadeDBServer                    server;
  private final        ServerSecurityFileRepository      securityRepository;
  private final        ConcurrentMap<String, ServerUser> users                = new ConcurrentHashMap<>();
  private final        String                            algorithm;
  private final        SecretKeyFactory                  secretKeyFactory;
  private final        Map<String, String>               saltCache;
  private final        int                               saltIteration;
  private              CredentialsValidator              credentialsValidator = new DefaultCredentialsValidator();
  private static final Random                            RANDOM               = new SecureRandom();
  public static final  String                            FILE_NAME            = "security.json";
  public static final  int                               SALT_SIZE            = 32;

  public static class ServerUser {
    public final String      name;
    public final String      password;
    public final boolean     databaseBlackList;
    public final Set<String> databases = new HashSet<>();

    public ServerUser(final String name, final String password, final boolean databaseBlackList, final Collection<String> databases) {
      this.name = name;
      this.password = password;
      this.databaseBlackList = databaseBlackList;
      if (databases != null)
        this.databases.addAll(databases);
    }
  }

  public ServerSecurity(final ArcadeDBServer server, final ContextConfiguration configuration, final String configPath) {
    this.server = server;
    this.algorithm = configuration.getValueAsString(SERVER_SECURITY_ALGORITHM);

    final int cacheSize = configuration.getValueAsInteger(SERVER_SECURITY_SALT_CACHE_SIZE);

    if (cacheSize > 0)
      saltCache = Collections.synchronizedMap(new LRUCache<>(cacheSize));
    else
      saltCache = Collections.emptyMap();

    saltIteration = configuration.getValueAsInteger(SERVER_SECURITY_SALT_ITERATIONS);

    securityRepository = new ServerSecurityFileRepository(configPath + "/" + FILE_NAME);
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
    try {
      users.putAll(securityRepository.loadConfiguration());

      if (users.isEmpty())
        createDefaultSecurity();

    } catch (IOException e) {
      throw new ServerException("Error on starting Security service", e);
    }
  }
  // end::contains[]

  @Override
  public void stopService() {
    users.clear();
  }

  public ServerUser authenticate(final String userName, final String userPassword) {
    final ServerUser su = users.get(userName);
    if (su == null)
      throw new ServerSecurityException("User/Password not valid");

    if (!passwordMatch(userPassword, su.password))
      throw new ServerSecurityException("User/Password not valid");

    return su;
  }

  public Set<String> userDatabases(final ServerUser user) {
    final Set<String> dbs = new HashSet<>(server.getDatabaseNames());
    if (user.databaseBlackList)
      dbs.removeAll(user.databases);
    else
      dbs.retainAll(user.databases);
    return dbs;
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

  public void createUser(final String name, final String password, final boolean databaseBlackList, final Collection<String> databases) throws IOException {
    users.put(name, new ServerUser(name, this.encode(password, generateRandomSalt()), databaseBlackList, databases));
    securityRepository.saveConfiguration(users);
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

  protected String encode(final String password, final String salt) {
    return this.encode(password, salt, saltIteration);
  }

  protected boolean passwordMatch(final String password, final String hashedPassword) {
    // hashedPassword consist of: ALGORITHM, ITERATIONS_NUMBER, SALT and
    // HASH; parts are joined with dollar character ("$")
    final String[] parts = hashedPassword.split("\\$");
    if (parts.length != 4)
      // wrong hash format
      return false;

    final Integer iterations = Integer.parseInt(parts[1]);
    final String salt = parts[2];
    final String hash = encode(password, salt, iterations);

    return hash.equals(hashedPassword);
  }

  protected static String generateRandomSalt() {
    final byte[] salt = new byte[SALT_SIZE];
    RANDOM.nextBytes(salt);
    return new String(Base64.getEncoder().encode(salt), DatabaseFactory.getDefaultCharset());
  }

  protected String encode(final String password, final String salt, final int iterations) {
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

  public void saveConfiguration() throws IOException {
    securityRepository.saveConfiguration(users);
  }

  protected void createDefaultSecurity() throws IOException {
    String rootPassword = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_ROOT_PASSWORD);
    if (rootPassword == null) {
      if (server.getConfiguration().getValueAsBoolean(GlobalConfiguration.HA_K8S)) {
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
      LogManager.instance().log(this, Level.INFO, "Creating root user with provided password");

    credentialsValidator.validateCredentials("root", rootPassword);

    createUser("root", rootPassword, true, null);
  }
}
