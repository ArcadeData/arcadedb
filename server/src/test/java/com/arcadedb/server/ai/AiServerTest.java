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
package com.arcadedb.server.ai;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class AiServerTest extends BaseGraphServerTest {

  private String getAiUrl(final String path) {
    return "http://127.0.0.1:" + getServer(0).getHttpServer().getPort() + "/api/v1/ai" + path;
  }

  private static String getBasicAuth() {
    return "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void getConfigReturnsNotConfigured() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getAiUrl("/config")).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = readBody(connection);
      final JSONObject config = new JSONObject(body);
      assertThat(config.getBoolean("configured")).isFalse();
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void listChatsReturnsEmpty() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getAiUrl("/chats")).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      final String body = readBody(connection);
      final JSONObject result = new JSONObject(body);
      assertThat(result.getJSONArray("chats").length()).isZero();
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void chatEndpointRejectsWhenNotConfigured() throws Exception {
    final JSONObject payload = new JSONObject()
        .put("database", "graph")
        .put("message", "Hello AI");

    final HttpURLConnection connection = (HttpURLConnection) new URI(getAiUrl("/chat")).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(payload.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      final String body = readErrorBody(connection);
      assertThat(body).contains("not configured");
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void getNonExistentChatReturns404() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getAiUrl("/chats/nonexistent-id")).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization", getBasicAuth());
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(404);
    } finally {
      connection.disconnect();
    }
  }

  @Test
  void requiresAuthentication() throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getAiUrl("/config")).toURL().openConnection();
    connection.setRequestMethod("GET");
    // No authorization header
    connection.connect();

    try {
      assertThat(connection.getResponseCode()).isEqualTo(401);
    } finally {
      connection.disconnect();
    }
  }

  private String readBody(final HttpURLConnection connection) throws Exception {
    try (final InputStream is = connection.getInputStream()) {
      return new String(is.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private String readErrorBody(final HttpURLConnection connection) throws Exception {
    try (final InputStream is = connection.getErrorStream()) {
      return is != null ? new String(is.readAllBytes(), StandardCharsets.UTF_8) : "";
    }
  }
}
