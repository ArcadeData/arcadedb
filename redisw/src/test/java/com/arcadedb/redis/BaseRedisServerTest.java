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
package com.arcadedb.redis;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerPlugin;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Base for the tests that start the Redis plugin. The plugin is started on port {@code 0}, so the operating system
 * assigns a free port, and every test must connect to {@link #getServerRedisPort()} rather than to 6379 or to the
 * configured {@link GlobalConfiguration#REDIS_PORT}: anything already listening on the production default (a
 * developer's own Redis, a second concurrent build, another agent) used to fail the module at {@code @BeforeEach} or,
 * worse, answer the test's connections (issue #8209).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class BaseRedisServerTest extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // After resetAll(), which super.setTestConfiguration() runs: set earlier the value would be discarded before the
    // server reads it. A subclass that needs a fixed port sets its own after calling this method.
    GlobalConfiguration.REDIS_PORT.setValue(0);
  }

  /**
   * The Redis port the first server ACTUALLY bound.
   */
  protected int getServerRedisPort() {
    return getServerRedisPort(0);
  }

  /**
   * The Redis port server {@code serverIndex} ACTUALLY bound, the counterpart of
   * {@link BaseGraphServerTest#getServerHttpPort(int)}.
   */
  protected int getServerRedisPort(final int serverIndex) {
    return getServerRedisPort(getServer(serverIndex));
  }

  /**
   * @throws IllegalStateException when the server is not started or does not run the Redis plugin, because there is
   *                               no port to answer with and a guess would reintroduce the defect this method exists
   *                               to remove
   */
  static int getServerRedisPort(final ArcadeDBServer server) {
    if (server != null)
      for (final ServerPlugin plugin : server.getPlugins())
        if (plugin instanceof RedisProtocolPlugin p && p.getPort() > 0)
          return p.getPort();
    throw new IllegalStateException("The Redis plugin is not listening: it has not bound a port, so there is none to address");
  }

  // --- HTTP HELPERS FOR THE "redis" QUERY LANGUAGE, SHARED SO A TEST DRIVING BOTH THE RESP WIRE PATH (Jedis) AND
  // THE QUERY LANGUAGE (HTTP) FOR THE SAME COMMAND DOES NOT HAVE TO CARRY ITS OWN COPY (#8271) ---

  protected JSONObject executeQuery(final int serverIndex, final String language, final String command) throws Exception {
    return executeHttp(serverIndex, "query", language, command);
  }

  protected JSONObject executeCommand(final int serverIndex, final String language, final String command) throws Exception {
    return executeHttp(serverIndex, "command", language, command);
  }

  private JSONObject executeHttp(final int serverIndex, final String endpoint, final String language, final String command)
      throws Exception {
    // Ask the server which port it actually bound (issue #6560), rather than assuming the 2480+serverIndex
    // default: SERVER_HTTP_INCOMING_PORT is a range (2480-2489 by default) and binds the first free port in
    // it, so with 2480 already held by anything else - another local ArcadeDB instance, an IDE debug session
    // for a different project - this test's own server listens elsewhere. A port-less/wrong-port URL would
    // then reach that foreign server instead, and every test in this class would fail with a confusing
    // "403 Too many failed authentication attempts" / "User/Password not valid" that reads as an auth bug
    // rather than a port collision. Same pattern as #6437's fix for ConsoleAsyncInsertTest.
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:" + getServer(serverIndex).getHttpServer().getPort() + "/api/v1/" + endpoint + "/" + getDatabaseName())
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    final JSONObject request = new JSONObject();
    request.put("language", language);
    request.put("command", command);

    try (OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
    }

    final int responseCode = connection.getResponseCode();
    if (responseCode != 200) {
      final String error = new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      throw new RuntimeException("HTTP " + responseCode + ": " + error);
    }

    final String response = new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    return new JSONObject(response);
  }

  /**
   * Extracts the "value" property from the first result in the response. The response format is:
   * {@code {"result": [{"value": <result>}]}}
   */
  protected Object getResultValue(final JSONObject response) {
    final JSONArray results = response.getJSONArray("result");
    if (results.isEmpty())
      return null;
    final JSONObject firstResult = results.getJSONObject(0);
    if (firstResult.isNull("value"))
      return null;
    return firstResult.get("value");
  }

  protected int getResultValueAsInt(final JSONObject response) {
    final Object value = getResultValue(response);
    if (value instanceof Number number)
      return number.intValue();
    return Integer.parseInt(value.toString());
  }

  protected long getResultValueAsLong(final JSONObject response) {
    final Object value = getResultValue(response);
    if (value instanceof Number number)
      return number.longValue();
    return Long.parseLong(value.toString());
  }
}
