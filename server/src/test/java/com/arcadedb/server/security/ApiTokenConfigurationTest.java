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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

class ApiTokenConfigurationTest {
  private static final String TEST_CONFIG_PATH = "target/test-api-tokens";
  private ApiTokenConfiguration config;

  @BeforeEach
  void setUp() {
    final File dir = new File(TEST_CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    dir.mkdirs();
    config = new ApiTokenConfiguration(TEST_CONFIG_PATH);
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(TEST_CONFIG_PATH));
  }

  @Test
  void createToken() {
    final JSONObject permissions = new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord"))))
        .put("database", new JSONArray());

    final JSONObject token = config.createToken("Test Token", "mydb", 0, permissions);

    assertThat(token.getString("token")).startsWith("at-");
    assertThat(token.getString("name")).isEqualTo("Test Token");
    assertThat(token.getString("database")).isEqualTo("mydb");
    assertThat(token.getLong("expiresAt")).isEqualTo(0);
    assertThat(token.getLong("createdAt")).isGreaterThan(0);
  }

  @Test
  void getToken() {
    final JSONObject permissions = new JSONObject();
    final JSONObject created = config.createToken("Token1", "db1", 0, permissions);
    final String tokenValue = created.getString("token");

    final JSONObject retrieved = config.getToken(tokenValue);
    assertThat(retrieved).isNotNull();
    assertThat(retrieved.getString("name")).isEqualTo("Token1");
  }

  @Test
  void getTokenNotFound() {
    assertThat(config.getToken("at-nonexistent")).isNull();
  }

  @Test
  void deleteToken() {
    final JSONObject created = config.createToken("Token1", "db1", 0, new JSONObject());
    final String tokenValue = created.getString("token");
    final String tokenHash = ApiTokenConfiguration.hashToken(tokenValue);

    assertThat(config.deleteToken(tokenHash)).isTrue();
    assertThat(config.getToken(tokenValue)).isNull();
  }

  @Test
  void deleteTokenRejectsPlaintext() {
    final JSONObject created = config.createToken("Token1", "db1", 0, new JSONObject());
    final String tokenValue = created.getString("token");

    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> config.deleteToken(tokenValue));
  }

  @Test
  void deleteTokenNotFound() {
    assertThat(config.deleteToken("nonexistent-hash-that-does-not-start-with-prefix")).isFalse();
  }

  @Test
  void listTokens() {
    config.createToken("Token1", "db1", 0, new JSONObject());
    config.createToken("Token2", "db2", 0, new JSONObject());

    final List<JSONObject> tokens = config.listTokens();
    assertThat(tokens).hasSize(2);
  }

  @Test
  void expiredTokenReturnsNull() {
    final long pastTime = System.currentTimeMillis() - 10000;
    final JSONObject created = config.createToken("Expired", "db1", pastTime, new JSONObject());
    final String tokenValue = created.getString("token");

    assertThat(config.getToken(tokenValue)).isNull();
  }

  @Test
  void loadSaveRoundTrip() {
    config.createToken("Persistent", "db1", 0, new JSONObject()
        .put("types", new JSONObject()
            .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));

    // Create new config instance pointing to same path and load
    final ApiTokenConfiguration config2 = new ApiTokenConfiguration(TEST_CONFIG_PATH);
    config2.load();

    final List<JSONObject> tokens = config2.listTokens();
    assertThat(tokens).hasSize(1);
    assertThat(tokens.get(0).getString("name")).isEqualTo("Persistent");
  }

  @Test
  void loadRemovesExpiredTokens() {
    // Create a token that will expire immediately
    final long pastTime = System.currentTimeMillis() - 1000;
    config.createToken("WillExpire", "db1", pastTime, new JSONObject());
    config.createToken("StillValid", "db2", 0, new JSONObject());

    // Reload
    final ApiTokenConfiguration config2 = new ApiTokenConfiguration(TEST_CONFIG_PATH);
    config2.load();

    final List<JSONObject> tokens = config2.listTokens();
    assertThat(tokens).hasSize(1);
    assertThat(tokens.get(0).getString("name")).isEqualTo("StillValid");
  }

  @Test
  void duplicateNameThrows() {
    config.createToken("SameName", "db1", 0, new JSONObject());

    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> config.createToken("SameName", "db2", 0, new JSONObject()));

    // Only one token should exist
    assertThat(config.listTokens()).hasSize(1);
  }

  @Test
  void isApiToken() {
    assertThat(ApiTokenConfiguration.isApiToken("at-550e8400-e29b-41d4-a716-446655440000")).isTrue();
    assertThat(ApiTokenConfiguration.isApiToken("AU-some-session-token")).isFalse();
    assertThat(ApiTokenConfiguration.isApiToken(null)).isFalse();
    assertThat(ApiTokenConfiguration.isApiToken("")).isFalse();
  }
}
