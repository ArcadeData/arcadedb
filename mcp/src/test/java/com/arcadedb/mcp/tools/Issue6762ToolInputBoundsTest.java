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
package com.arcadedb.mcp.tools;

import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6762: the declared JSON-Schema range is advisory - the MCP client is the one that would enforce it - so
 * every tool has to re-check its own numeric inputs server-side, the way {@code SampleRecordsTool},
 * {@code FullTextSearchTool} and {@code VectorSearchTool} already do. {@code profiler_start} declared a range and
 * checked nothing, and {@code query} / {@code execute_command} accumulate every row into an in-memory JSONArray
 * with no upper bound on 'limit' at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6762ToolInputBoundsTest extends BaseGraphServerTest {
  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowAdmin(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void profilerStartRejectsATimeoutOutsideItsDeclaredRange() {
    assertThatThrownBy(() -> ProfilerStartTool.execute(getServer(0), user,
        new JSONObject().put("timeoutSeconds", 0), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'timeoutSeconds' must be between 1 and 3600");

    assertThatThrownBy(() -> ProfilerStartTool.execute(getServer(0), user,
        new JSONObject().put("timeoutSeconds", -1), config))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> ProfilerStartTool.execute(getServer(0), user,
        new JSONObject().put("timeoutSeconds", 86_400), config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void queryRejectsALimitOutsideItsWindow() {
    assertThatThrownBy(() -> QueryTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1 AS value")
        .put("limit", Integer.MAX_VALUE), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'limit' must be between 1 and");

    assertThatThrownBy(() -> QueryTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1 AS value")
        .put("limit", 0), config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void executeCommandRejectsALimitOutsideItsWindow() {
    assertThatThrownBy(() -> ExecuteCommandTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("command", "SELECT 1 AS value")
        .put("limit", Integer.MAX_VALUE), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'limit' must be between 1 and");
  }

  /** Control: a limit inside the window is untouched, so the bound costs a legitimate caller nothing. */
  @Test
  void aLimitInsideTheWindowStillWorks() {
    final JSONObject result = QueryTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1 AS value")
        .put("limit", 10), config);

    assertThat(result.getInt("count")).isEqualTo(1);
  }

  /** And the declared schema advertises the same window the server enforces, so a client can pre-validate. */
  @Test
  void theDeclaredSchemaMatchesTheEnforcedWindow() {
    final JSONObject limit = QueryTool.getDefinition().getJSONObject("inputSchema")
        .getJSONObject("properties").getJSONObject("limit");
    assertThat(limit.getInt("minimum")).isEqualTo(1);
    assertThat(limit.getInt("maximum")).isEqualTo(100_000);
  }
}
