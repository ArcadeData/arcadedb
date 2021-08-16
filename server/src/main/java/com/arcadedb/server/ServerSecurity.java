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

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.LRUCache;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.arcadedb.GlobalConfiguration.*;

public class ServerSecurity implements ServerPlugin {

  private final ServerSecurityFileRepository securityRepository;

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

  private final        String                            configPath;
  private final        ConcurrentMap<String, ServerUser> users     = new ConcurrentHashMap<>();
  private final        String                            algorithm;
  private final        SecretKeyFactory                  secretKeyFactory;
  private final        Map<String, String>               saltCache;
  private final        int                               saltIteration;
  private static final Random                            RANDOM    = new SecureRandom();
  public static final  String                            FILE_NAME = "security.json";
  public static final  int                               SALT_SIZE = 32;

  public ServerSecurity(final ContextConfiguration configuration, final String configPath) {
    this.configPath = configPath;
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

  // tag::contains[]

  @Override
  public void startService() {
    try {
      if (!securityRepository.isSecurityConfPresent()) {
        createDefaultSecurity();
      }

      users.putAll(securityRepository.loadConfiguration());
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

    if (!checkPassword(userPassword, su.password))
      throw new ServerSecurityException("User/Password not valid");

    return su;
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
    final SecretKeyFactory keyFactory;
    try {
      keyFactory = SecretKeyFactory.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new ServerSecurityException("Could NOT retrieve '" + algorithm + "' algorithm", e);
    }

    final KeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt.getBytes(Charset.forName("UTF-8")), iterations, 256);
    final SecretKey secret;
    try {
      secret = keyFactory.generateSecret(keySpec);
    } catch (InvalidKeySpecException e) {
      throw new ServerSecurityException("Error on generating security key", e);
    }

    final byte[] rawHash = secret.getEncoded();
    final byte[] hashBase64 = Base64.getEncoder().encode(rawHash);

    return new String(hashBase64);
  }

  protected String encode(final String password, final String salt) {
    return this.encode(password, salt, saltIteration);
  }

  protected boolean checkPassword(final String password, final String hashedPassword) {
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
    return new String(Base64.getEncoder().encode(salt));
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

  protected void createDefaultSecurity() throws IOException {
    createUser("root", "root", true, null);
  }

  public void saveConfiguration() throws IOException {
    securityRepository.saveConfiguration(users);
  }

}
