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
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class ApiTokenConfiguration {
  public static final  String                          FILE_NAME = "server-api-tokens.json";
  private static final String                          TOKEN_PREFIX = "at-";
  private final        String                          filePath;
  private final        ConcurrentHashMap<String, JSONObject> tokens = new ConcurrentHashMap<>();

  public ApiTokenConfiguration(final String configPath) {
    String path = configPath;
    if (!path.endsWith(File.separator))
      path += File.separator;
    this.filePath = path + FILE_NAME;
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
        tokens.put(tokenJson.getString("token"), tokenJson);
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
  }

  public JSONObject createToken(final String name, final String database, final long expiresAt, final JSONObject permissions) {
    final String tokenValue = TOKEN_PREFIX + UUID.randomUUID();

    final JSONObject tokenJson = new JSONObject();
    tokenJson.put("token", tokenValue);
    tokenJson.put("name", name);
    tokenJson.put("database", database);
    tokenJson.put("expiresAt", expiresAt);
    tokenJson.put("createdAt", System.currentTimeMillis());
    tokenJson.put("permissions", permissions);

    tokens.put(tokenValue, tokenJson);
    save();
    return tokenJson;
  }

  public boolean deleteToken(final String tokenValue) {
    if (tokens.remove(tokenValue) != null) {
      save();
      return true;
    }
    return false;
  }

  public JSONObject getToken(final String tokenValue) {
    final JSONObject tokenJson = tokens.get(tokenValue);
    if (tokenJson == null)
      return null;

    final long expiresAt = tokenJson.getLong("expiresAt", 0);
    if (expiresAt > 0 && expiresAt < System.currentTimeMillis()) {
      tokens.remove(tokenValue);
      save();
      return null;
    }

    return tokenJson;
  }

  public List<JSONObject> listTokens() {
    return new ArrayList<>(tokens.values());
  }

  public void cleanupExpired() {
    final long now = System.currentTimeMillis();
    boolean removed = false;
    for (final Map.Entry<String, JSONObject> entry : tokens.entrySet()) {
      final long expiresAt = entry.getValue().getLong("expiresAt", 0);
      if (expiresAt > 0 && expiresAt < now) {
        tokens.remove(entry.getKey());
        removed = true;
      }
    }
    if (removed)
      save();
  }

  public static boolean isApiToken(final String token) {
    return token != null && token.startsWith(TOKEN_PREFIX);
  }
}
