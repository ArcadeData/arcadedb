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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ApiTokenConfiguration {
  public static final  String                                FILE_NAME    = "server-api-tokens.json";
  private static final String                                TOKEN_PREFIX = "at-";
  private static final int                                   SUFFIX_LEN   = 4;
  private static final int                                   TOKEN_BYTES  = 32;
  private static final SecureRandom                          SECURE_RANDOM = new SecureRandom();
  private final        String                                filePath;
  private final        ConcurrentHashMap<String, JSONObject> tokens       = new ConcurrentHashMap<>();

  public ApiTokenConfiguration(final String configPath) {
    this.filePath = Paths.get(configPath, FILE_NAME).toString();
  }

  public synchronized void load() {
    tokens.clear();
    final File file = new File(filePath);
    if (!file.exists())
      return;

    try (final FileInputStream fis = new FileInputStream(file)) {
      final JSONObject json = new JSONObject(FileUtils.readStreamAsString(fis, "UTF-8"));
      if (!json.has("tokens"))
        return;

      final JSONArray tokenArray = json.getJSONArray("tokens");
      final long now = System.currentTimeMillis();
      boolean needsSave = false;

      for (int i = 0; i < tokenArray.length(); i++) {
        final JSONObject tokenJson = tokenArray.getJSONObject(i);
        final long expiresAt = tokenJson.getLong("expiresAt", 0);
        if (expiresAt > 0 && expiresAt < now) {
          needsSave = true;
          continue;
        }

        // Backward compatibility: migrate plaintext tokens to hashed
        if (tokenJson.has("token") && !tokenJson.has("tokenHash")) {
          final String plaintext = tokenJson.getString("token");
          final String hash = hashToken(plaintext);
          tokenJson.put("tokenHash", hash);
          tokenJson.put("tokenSuffix", plaintext.length() > SUFFIX_LEN
              ? plaintext.substring(plaintext.length() - SUFFIX_LEN) : plaintext);
          tokenJson.remove("token");
          tokenJson.remove("tokenPrefix");
          needsSave = true;
        }

        tokens.put(tokenJson.getString("tokenHash"), tokenJson);
      }

      if (needsSave)
        save();

    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error loading API tokens from '%s'", e, filePath);
    }
  }

  public synchronized void save() {
    final File file = new File(filePath);
    if (!file.getParentFile().exists())
      file.getParentFile().mkdirs();

    final JSONObject json = new JSONObject();
    json.put("version", 1);

    final JSONArray tokenArray = new JSONArray();
    for (final JSONObject tokenJson : tokens.values())
      tokenArray.put(tokenJson);

    json.put("tokens", tokenArray);

    try (final FileWriter writer = new FileWriter(file, DatabaseFactory.getDefaultCharset())) {
      writer.write(json.toString(2));
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error saving API tokens to '%s'", e, filePath);
    }

    // Set file permissions to owner-only (mode 600) on POSIX systems
    try {
      final PosixFileAttributeView posixView = Files.getFileAttributeView(file.toPath(), PosixFileAttributeView.class);
      if (posixView != null)
        posixView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    } catch (final IOException | UnsupportedOperationException e) {
      // Non-POSIX system (e.g., Windows) — skip
    }
  }

  public JSONObject createToken(final String name, final String database, final long expiresAt, final JSONObject permissions) {
    final byte[] randomBytes = new byte[TOKEN_BYTES];
    SECURE_RANDOM.nextBytes(randomBytes);
    final StringBuilder tokenBuilder = new StringBuilder(TOKEN_PREFIX);
    for (final byte b : randomBytes)
      tokenBuilder.append(String.format("%02x", b));
    final String tokenValue = tokenBuilder.toString();
    final String hash = hashToken(tokenValue);
    final String suffix = tokenValue.substring(tokenValue.length() - SUFFIX_LEN);

    final JSONObject tokenJson = new JSONObject();
    tokenJson.put("tokenHash", hash);
    tokenJson.put("tokenSuffix", suffix);
    tokenJson.put("name", name);
    tokenJson.put("database", database);
    tokenJson.put("expiresAt", expiresAt);
    tokenJson.put("createdAt", System.currentTimeMillis());
    tokenJson.put("permissions", permissions);

    tokens.put(hash, tokenJson);
    save();

    // Return a response that includes the plaintext token (one-time display)
    final JSONObject response = tokenJson.copy();
    response.put("token", tokenValue);
    return response;
  }

  public boolean deleteToken(final String identifier) {
    // Accept both plaintext token (starts with at-) and hash
    final String key = identifier.startsWith(TOKEN_PREFIX) ? hashToken(identifier) : identifier;
    if (tokens.remove(key) != null) {
      save();
      return true;
    }
    return false;
  }

  public JSONObject getToken(final String plaintextToken) {
    final String hash = hashToken(plaintextToken);
    final JSONObject tokenJson = tokens.get(hash);
    if (tokenJson == null)
      return null;

    final long expiresAt = tokenJson.getLong("expiresAt", 0);
    if (expiresAt > 0 && expiresAt < System.currentTimeMillis()) {
      tokens.remove(hash);
      return null;
    }

    return tokenJson;
  }

  public List<JSONObject> listTokens() {
    return new ArrayList<>(tokens.values());
  }

  public synchronized void cleanupExpired() {
    final long now = System.currentTimeMillis();
    final boolean removed = tokens.entrySet().removeIf(entry -> {
      final long expiresAt = entry.getValue().getLong("expiresAt", 0);
      return expiresAt > 0 && expiresAt < now;
    });
    if (removed)
      save();
  }

  public static boolean isApiToken(final String token) {
    return token != null && token.startsWith(TOKEN_PREFIX);
  }

  public static String hashToken(final String plaintext) {
    try {
      final MessageDigest digest = MessageDigest.getInstance("SHA-256");
      final byte[] hash = digest.digest(plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      final StringBuilder hex = new StringBuilder(hash.length * 2);
      for (final byte b : hash)
        hex.append(String.format("%02x", b));
      return hex.toString();
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
  }
}
