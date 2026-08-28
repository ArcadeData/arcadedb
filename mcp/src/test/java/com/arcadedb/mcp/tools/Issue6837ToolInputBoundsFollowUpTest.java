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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6837, a follow-up to #6762: the #6762 hardening described the full-text tool as capping its window, but
 * {@code FullTextSearchTool} only rejected {@code limit < 1}, so an unbounded limit still degenerated to "load,
 * serialize and accumulate every match". {@code set_server_setting} declared {@code value} required and then read
 * it with an empty-string default, so a call omitting it succeeded and left {@code ""} in the ContextConfiguration
 * for a numeric key - a NumberFormatException deferred to whichever component reads it next.
 * <p>
 * The schema-wide test below is the part that prevents the next instance: every numeric tool input that declares a
 * lower bound must also declare an upper one, and the declared window must be the one the server enforces.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6837ToolInputBoundsFollowUpTest extends BaseGraphServerTest {
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
  void fullTextSearchRejectsALimitAboveItsWindow() {
    assertThatThrownBy(() -> FullTextSearchTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Person[name]")
        .put("queryText", "a")
        .put("limit", Integer.MAX_VALUE), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'limit' must be between 1 and " + FullTextSearchTool.MAX_LIMIT);

    assertThatThrownBy(() -> FullTextSearchTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Person[name]")
        .put("queryText", "a")
        .put("limit", FullTextSearchTool.MAX_LIMIT + 1), config))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * The lower bound stays rejected, and the check happens before the index is resolved, so it is the limit the
   * caller hears about rather than a downstream addressing error.
   */
  @Test
  void fullTextSearchStillRejectsANonPositiveLimit() {
    assertThatThrownBy(() -> FullTextSearchTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Person[name]")
        .put("queryText", "a")
        .put("limit", 0), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'limit' must be between 1 and");
  }

  @Test
  void vectorSearchRejectsAnEfSearchAboveItsWindow() {
    assertThatThrownBy(() -> VectorSearchTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "Doc[embedding]")
        .put("queryVector", new JSONArray().put(1.0f))
        .put("efSearch", Integer.MAX_VALUE), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'efSearch' must be between 1 and " + MCPVectorLeg.MAX_EF_SEARCH);
  }

  @Test
  void setServerSettingRejectsAnAbsentValue() {
    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("key", "arcadedb.asyncWorkerThreads"), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'value' is required");
  }

  /** An explicit empty string is not a value for a numeric setting either: it is what the caller must not store. */
  @Test
  void setServerSettingRejectsAnEmptyValueForANonStringSetting() {
    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("key", "arcadedb.asyncWorkerThreads").put("value", ""), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be empty");

    // and the setting is untouched, so the rejected call cannot have been the one that broke a later read
    assertThatCode(() -> getServer(0).getConfiguration().getValueAsInteger(
        GlobalConfiguration.ASYNC_WORKER_THREADS)).doesNotThrowAnyException();
  }

  /** A String-typed setting keeps accepting "", which is how a caller clears one. */
  @Test
  void setServerSettingStillAcceptsAnEmptyValueForAStringSetting() {
    final String key = GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey();
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES);
    try {
      final JSONObject result = SetServerSettingTool.execute(getServer(0), user,
          new JSONObject().put("key", key).put("value", ""), config);

      assertThat(result.getString("newValue")).isEmpty();
      assertThat(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DEFAULT_DATABASES)).isEmpty();
    } finally {
      getServer(0).getConfiguration().setValue(key, previous);
    }
  }

  @Test
  void setServerSettingRejectsAnAbsentKey() {
    assertThatThrownBy(() -> SetServerSettingTool.execute(getServer(0), user,
        new JSONObject().put("value", "8"), config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'key' is required");
  }

  /**
   * The rule #6762 asserted for {@code query} alone, applied to every tool: a numeric input that declares a
   * {@code minimum} must declare a {@code maximum} too. An input bounded on one side only is the exact shape both
   * {@code full_text_search} and {@code efSearch} had - a window a client cannot pre-validate and, historically,
   * one the server did not enforce either.
   */
  @Test
  void everyNumericToolInputDeclaresBothEndsOfItsWindow() {
    final List<Supplier<JSONObject>> definitions = List.of(
        QueryTool::getDefinition, ExecuteCommandTool::getDefinition, SampleRecordsTool::getDefinition,
        VectorSearchTool::getDefinition, FullTextSearchTool::getDefinition, HybridSearchTool::getDefinition,
        ProfilerStartTool::getDefinition, UpsertEntityTool::getDefinition, UpsertRelationshipTool::getDefinition,
        GetSchemaTool::getDefinition, ListDatabasesTool::getDefinition, ServerStatusTool::getDefinition,
        GetServerSettingsTool::getDefinition, SetServerSettingTool::getDefinition, ProfilerStopTool::getDefinition,
        ProfilerStatusTool::getDefinition);

    final List<String> unbounded = new ArrayList<>();
    for (final Supplier<JSONObject> definition : definitions) {
      final JSONObject tool = definition.get();
      collectHalfBoundedNumbers(tool.getString("name"), tool.getJSONObject("inputSchema"), unbounded);
    }

    assertThat(unbounded).isEmpty();
  }

  /** And full_text_search advertises exactly the window it enforces, so a client can reject the call itself. */
  @Test
  void fullTextSearchDeclaresTheWindowItEnforces() {
    final JSONObject limit = FullTextSearchTool.getDefinition().getJSONObject("inputSchema")
        .getJSONObject("properties").getJSONObject("limit");

    assertThat(limit.getInt("minimum")).isEqualTo(1);
    assertThat(limit.getInt("maximum")).isEqualTo(FullTextSearchTool.MAX_LIMIT);
  }

  /**
   * Walks a JSON-Schema fragment and records every numeric input that declares a 'minimum' without a 'maximum',
   * recursing into nested objects so a bound inside a structured input (filter.graphExpansion.maxDepth) counts too.
   * Array 'items' are deliberately not walked: an element of 'queryIndices' is a coordinate into the caller's own
   * vector, whose upper bound is the index dimension and so cannot be a constant in the schema. What bounds an
   * array input is its length, which the vector leg checks against the resolved index dimensions.
   */
  private static void collectHalfBoundedNumbers(final String path, final JSONObject schema, final List<String> unbounded) {
    final String type = schema.getString("type", "");
    if (("integer".equals(type) || "number".equals(type)) && schema.has("minimum") && !schema.has("maximum"))
      unbounded.add(path);

    final JSONObject properties = schema.getJSONObject("properties", null);
    if (properties != null)
      for (final String name : properties.keySet())
        collectHalfBoundedNumbers(path + "." + name, properties.getJSONObject(name), unbounded);
  }
}
