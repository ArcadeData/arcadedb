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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link ToolDispatcher} executes the gateway's tool requests
 * locally against a real ArcadeDB server, returning JSON in the shape the LLM
 * expects (matching the contract that the AI gateway previously fetched over HTTP).
 */
class ToolDispatcherTest extends BaseGraphServerTest {

  private ServerSecurityUser rootUser() {
    return getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void getSchemaReturnsTypesArray() {
    final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());

    final String json = dispatcher.execute("get_schema", new JSONObject().put("database", getDatabaseName()));
    final JSONObject result = new JSONObject(json);

    assertThat(result.getString("database", null)).isEqualTo(getDatabaseName());
    final JSONArray types = result.getJSONArray("types");
    assertThat(types.length()).isGreaterThan(0);
  }

  @Test
  void queryDatabaseReturnsRecords() {
    final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());

    final String json = dispatcher.execute("query_database", new JSONObject()
        .put("language", "sql")
        .put("command", "SELECT FROM " + VERTEX1_TYPE_NAME));
    final JSONObject result = new JSONObject(json);

    assertThat(result.has("error")).isFalse();
    assertThat(result.getString("user", null)).isEqualTo("root");
    assertThat(result.getJSONArray("result").length()).isGreaterThan(0);
  }

  @Test
  void queryDatabaseRejectsWritesAsError() {
    final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());

    final String json = dispatcher.execute("query_database", new JSONObject()
        .put("language", "sql")
        .put("command", "INSERT INTO " + VERTEX1_TYPE_NAME + " SET id = 99"));
    final JSONObject result = new JSONObject(json);

    // INSERT is not idempotent so database.query() must reject it. The dispatcher
    // wraps that as an error so the LLM can recover by returning a code block.
    assertThat(result.has("error")).isTrue();
  }

  @Test
  void queryDatabaseMissingCommandReturnsError() {
    final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());

    final String json = dispatcher.execute("query_database", new JSONObject().put("language", "sql"));
    final JSONObject result = new JSONObject(json);

    assertThat(result.has("error")).isTrue();
    assertThat(result.getString("error", "")).contains("command");
  }

  @Test
  void unknownToolReturnsError() {
    final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());

    final String json = dispatcher.execute("delete_universe", new JSONObject());
    final JSONObject result = new JSONObject(json);

    assertThat(result.getString("error", "")).contains("Unknown tool");
  }

  @Test
  void serverInfoReportsVersionAndDatabases() {
    final ToolDispatcher dispatcher = new ToolDispatcher(getServer(0), rootUser(), getDatabaseName());

    final String json = dispatcher.execute("get_server_info", new JSONObject());
    final JSONObject result = new JSONObject(json);

    assertThat(result.has("error")).isFalse();
    assertThat(result.getString("version", null)).isNotBlank();
    assertThat(result.getJSONArray("databases").length()).isGreaterThan(0);
  }
}
