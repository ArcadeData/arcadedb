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
package com.arcadedb.mcp;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8749: production mode conceals the engine's error text on HTTP, gRPC, PostgreSQL, MongoDB and Gremlin, but an
 * MCP tool error carried the raw exception message, so an {@code execute_command} refused on a unique index handed the
 * stored key value back. The tools' own argument validation is text the server words about the request and stays.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8749MCPProductionErrorConcealmentTest extends BaseGraphServerTest {
  private static final String TYPE   = "Unique8749";
  private static final String SECRET = "stored-secret-8749";

  @BeforeEach
  void enableMCPAndSeed() throws Exception {
    saveMCPConfig(new JSONObject()
        .put("enabled", true)
        .put("allowReads", true)
        .put("allowInsert", true)
        .put("profile", "all")
        .put("allowedUsers", new JSONArray().put("root")));

    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType(TYPE)) {
      database.command("sql", "CREATE DOCUMENT TYPE " + TYPE);
      database.command("sql", "CREATE PROPERTY " + TYPE + ".k STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE + " (k) UNIQUE");
      database.transaction(() -> database.newDocument(TYPE).set("k", SECRET).save());
    }
  }

  @Test
  void duplicatedKeyIsConcealedInProductionMode() throws Exception {
    final JSONObject response = withMode("production", this::duplicatedInsert);

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(textOf(response)).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(response.toString()).doesNotContain(SECRET);
  }

  @Test
  void developmentModeKeepsTheFullMessage() throws Exception {
    final JSONObject response = withMode("development", this::duplicatedInsert);

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(textOf(response)).contains(SECRET);
  }

  /** The tools' own argument validation is what the caller needs to correct the call: kept in production. */
  @Test
  void toolArgumentValidationIsKeptInProductionMode() throws Exception {
    final JSONObject response = withMode("production", () -> callTool("sample_records", new JSONObject()
        .put("database", getDatabaseName())
        .put("types", new JSONArray().put(TYPE))
        .put("limit", 21)));

    assertThat(response.getBoolean("isError", false)).isTrue();
    assertThat(textOf(response)).contains("limit").contains("20");
  }

  /**
   * An {@link IllegalArgumentException} the ENGINE raises is engine text, whatever its JDK class: {@code duration()}
   * echoes the string it could not parse. Only the MCP layer's own argument refusals are kept.
   */
  @Test
  void engineIllegalArgumentIsConcealedInProductionMode() throws Exception {
    final Callable<JSONObject> query = () -> callTool("query", new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "opencypher")
        .put("query", "RETURN duration('" + SECRET + "') AS d"));

    final JSONObject production = withMode("production", query);
    assertThat(production.getBoolean("isError", false)).isTrue();
    assertThat(textOf(production)).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(production.toString()).doesNotContain(SECRET);

    final JSONObject development = withMode("development", query);
    assertThat(development.getBoolean("isError", false)).isTrue();
    assertThat(textOf(development)).contains(SECRET);
  }

  private JSONObject duplicatedInsert() throws Exception {
    return callTool("execute_command", new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("command", "INSERT INTO " + TYPE + " SET k = '" + SECRET + "'"));
  }

  private static String textOf(final JSONObject response) {
    return response.getJSONArray("content").getJSONObject(0).getString("text");
  }

  private <T> T withMode(final String mode, final Callable<T> work) throws Exception {
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, mode);
    try {
      return work.call();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }

  private JSONObject callTool(final String toolName, final JSONObject arguments) throws Exception {
    final JSONObject response = post("/api/v1/mcp", new JSONObject()
        .put("jsonrpc", "2.0")
        .put("id", 10)
        .put("method", "tools/call")
        .put("params", new JSONObject().put("name", toolName).put("arguments", arguments)));
    assertThat(response.has("result")).isTrue();
    return response.getJSONObject("result");
  }

  private void saveMCPConfig(final JSONObject config) throws Exception {
    post("/api/v1/mcp/config", config);
  }

  private JSONObject post(final String path, final JSONObject payload) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(payload.toString().getBytes(StandardCharsets.UTF_8));
    }
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(FileUtils.readStreamAsString(connection.getInputStream(), "utf8"));
    } finally {
      connection.disconnect();
    }
  }
}
