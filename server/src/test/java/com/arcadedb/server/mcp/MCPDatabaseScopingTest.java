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
package com.arcadedb.server.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.mcp.tools.ExecuteCommandTool;
import com.arcadedb.server.mcp.tools.FullTextSearchTool;
import com.arcadedb.server.mcp.tools.GetSchemaTool;
import com.arcadedb.server.mcp.tools.ListDatabasesTool;
import com.arcadedb.server.mcp.tools.QueryTool;
import com.arcadedb.server.mcp.tools.ServerStatusTool;
import com.arcadedb.server.mcp.tools.UpsertEntityTool;
import com.arcadedb.server.mcp.tools.UpsertRelationshipTool;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MCPDatabaseScopingTest extends BaseGraphServerTest {
  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = getServer(0).getMCPConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.setAllowDelete(true);
    config.setAllowSchemaChange(true);
    config.setAllowAdmin(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void noOverridePreservesGlobalReadBehavior() {
    final JSONObject result = QueryTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1 AS value"), config);

    assertThat(result.getInt("count")).isEqualTo(1);
    assertThat(contains(ListDatabasesTool.execute(getServer(0), user, new JSONObject(), config)
        .getJSONArray("databases"), getDatabaseName())).isTrue();
    assertThat(containsResource(MCPResources.list(getServer(0), user, config)
        .getJSONArray("resources"), MCPResources.schemaURI(getDatabaseName()))).isTrue();
  }

  @Test
  void readOverrideRestrictsEveryDatabaseReadSurface() {
    setDatabaseOverride(new JSONObject().put("allowReads", false));

    assertReadDenied(() -> QueryTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1"), config));
    assertReadDenied(() -> GetSchemaTool.execute(getServer(0), user,
        new JSONObject().put("database", getDatabaseName()), config));
    assertReadDenied(() -> FullTextSearchTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("indexName", "missing")
        .put("queryText", "term"), config));

    assertThat(contains(ListDatabasesTool.execute(getServer(0), user, new JSONObject(), config)
        .getJSONArray("databases"), getDatabaseName())).isFalse();
    assertThat(contains(ServerStatusTool.execute(getServer(0), user, new JSONObject(), config)
        .getJSONArray("databases"), getDatabaseName())).isFalse();
    assertThat(containsResource(MCPResources.list(getServer(0), user, config)
        .getJSONArray("resources"), MCPResources.schemaURI(getDatabaseName()))).isFalse();
    assertThatThrownBy(() -> MCPResources.read(getServer(0), user, config,
        MCPResources.schemaURI(getDatabaseName())))
        .isInstanceOf(MCPResourceNotFoundException.class);
  }

  @Test
  void writeOverrideRestrictsCommandAndUpsertTools() {
    setDatabaseOverride(new JSONObject()
        .put("allowInsert", false)
        .put("allowUpdate", false)
        .put("allowDelete", false)
        .put("allowSchemaChange", false)
        .put("allowAdmin", false));

    assertThatThrownBy(() -> ExecuteCommandTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("command", "CREATE VERTEX V"), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Insert operations");

    assertThatThrownBy(() -> UpsertEntityTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "V")
        .put("matchKeys", new JSONObject().put("name", "scoped")), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("operations are not allowed");

    assertThatThrownBy(() -> UpsertRelationshipTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("fromType", "V")
        .put("fromMatchKeys", new JSONObject().put("name", "from"))
        .put("toType", "V")
        .put("toMatchKeys", new JSONObject().put("name", "to"))
        .put("relType", "E"), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("operations are not allowed");
  }

  @Test
  void databaseAllowedUsersHideAndRejectTheDatabase() {
    setDatabaseOverride(new JSONObject()
        .put("allowedUsers", new JSONArray().put("another-user")));

    assertThatThrownBy(() -> QueryTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("query", "SELECT 1"), config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not authorized for MCP access");
    assertThat(contains(ListDatabasesTool.execute(getServer(0), user, new JSONObject(), config)
        .getJSONArray("databases"), getDatabaseName())).isFalse();
    assertThat(containsResource(MCPResources.list(getServer(0), user, config)
        .getJSONArray("resources"), MCPResources.schemaURI(getDatabaseName()))).isFalse();
  }

  private void setDatabaseOverride(final JSONObject override) {
    config.updateFrom(new JSONObject()
        .put("databases", new JSONObject().put(getDatabaseName(), override)));
  }

  private static void assertReadDenied(final Runnable operation) {
    assertThatThrownBy(operation::run)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Read operations");
  }

  private static boolean contains(final JSONArray values, final String expected) {
    for (int i = 0; i < values.length(); i++)
      if (expected.equals(values.getString(i)))
        return true;
    return false;
  }

  private static boolean containsResource(final JSONArray resources, final String uri) {
    for (int i = 0; i < resources.length(); i++)
      if (uri.equals(resources.getJSONObject(i).getString("uri")))
        return true;
    return false;
  }
}
