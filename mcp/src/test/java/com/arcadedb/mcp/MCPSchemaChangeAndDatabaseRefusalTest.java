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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.mcp.tools.ExecuteCommandTool;
import com.arcadedb.mcp.tools.GetSchemaTool;
import com.arcadedb.mcp.tools.QueryTool;
import com.arcadedb.mcp.tools.UpsertEntityTool;
import com.arcadedb.mcp.tools.UpsertRelationshipTool;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Two guarantees of the MCP surface:
 * <ul>
 * <li>{@code allowSchemaChange=false} means no tool changes the schema. The operation analysis can only judge the statement
 * text, and a Cypher {@code MERGE} or {@code CREATE} naming a type that does not exist analyzes as a write, not as a schema
 * change, yet creates the type: the two upsert tools, and {@code execute_command} with such a statement, created types the
 * configuration refused. The engine now refuses it, through the narrowed principal the tools run as.</li>
 * <li>A database the caller may not reach is answered exactly as one that does not exist, so the refusal does not tell the
 * caller which names exist on the server.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MCPSchemaChangeAndDatabaseRefusalTest extends BaseGraphServerTest {
  private static final String PROBE_USER = "mcpprobe";

  private MCPConfiguration   config;
  private ServerSecurityUser root;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.setAllowDelete(true);
    config.setAllowSchemaChange(false);
    config.setAllowAdmin(false);
    config.setAllowedUsers(List.of("root", PROBE_USER));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    root = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @AfterEach
  void unbind() {
    // the tools bind their principal on the calling thread; the dispatcher clears it, a direct call does not
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    if (getServer(0).getSecurity().existsUser(PROBE_USER))
      getServer(0).getSecurity().dropUser(PROBE_USER);
  }

  @Test
  void upsertEntityCannotCreateATypeWhenSchemaChangesAreRefused() {
    assertRefusedWithoutSchemaChange(() -> UpsertEntityTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "McpNewEntityType")
        .put("matchKeys", new JSONObject().put("id", "x1")), config), "McpNewEntityType");
  }

  @Test
  void upsertRelationshipCannotCreateItsTypesWhenSchemaChangesAreRefused() {
    assertRefusedWithoutSchemaChange(() -> UpsertRelationshipTool.execute(getServer(0), root, new JSONObject()
            .put("database", getDatabaseName())
            .put("fromType", "McpNewFrom")
            .put("fromMatchKeys", new JSONObject().put("id", "a"))
            .put("toType", "McpNewTo")
            .put("toMatchKeys", new JSONObject().put("id", "b"))
            .put("relType", "McpNewRel"), config),
        "McpNewFrom", "McpNewTo", "McpNewRel");
  }

  @Test
  void upsertRelationshipCannotCreateTheEdgeTypeBetweenExistingVertexTypes() {
    getServerDatabase(0, getDatabaseName()).getSchema().createVertexType("McpExistingFrom");
    getServerDatabase(0, getDatabaseName()).getSchema().createVertexType("McpExistingTo");

    assertRefusedWithoutSchemaChange(() -> UpsertRelationshipTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("fromType", "McpExistingFrom")
        .put("fromMatchKeys", new JSONObject().put("id", "a"))
        .put("toType", "McpExistingTo")
        .put("toMatchKeys", new JSONObject().put("id", "b"))
        .put("relType", "McpNewEdgeOnly"), config), "McpNewEdgeOnly");
  }

  @Test
  void executeCommandCannotCreateATypeThroughACypherCreate() {
    // analyzes as CREATE, which allowInsert permits, and used to create the type
    assertRefusedWithoutSchemaChange(() -> ExecuteCommandTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "cypher")
        .put("command", "CREATE (n:McpNewViaCreate {id: 1}) RETURN n"), config), "McpNewViaCreate");
  }

  @Test
  void writesToTypesThatExistStillWork() {
    getServerDatabase(0, getDatabaseName()).getSchema().createVertexType("McpPerson");
    getServerDatabase(0, getDatabaseName()).getSchema().createEdgeType("McpKnows");

    final JSONObject entity = UpsertEntityTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "McpPerson")
        .put("matchKeys", new JSONObject().put("id", "p1")), config);
    assertThat(entity.getInt("count")).isEqualTo(1);

    final JSONObject relationship = UpsertRelationshipTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("fromType", "McpPerson")
        .put("fromMatchKeys", new JSONObject().put("id", "p1"))
        .put("toType", "McpPerson")
        .put("toMatchKeys", new JSONObject().put("id", "p2"))
        .put("relType", "McpKnows"), config);
    assertThat(relationship.getInt("count")).isEqualTo(1);

    final JSONObject created = ExecuteCommandTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "cypher")
        .put("command", "CREATE (n:McpPerson {id: 'p3'}) RETURN n"), config);
    assertThat(created.getInt("count")).isEqualTo(1);
  }

  @Test
  void theTypeIsCreatedWhenSchemaChangesAreAllowed() {
    config.setAllowSchemaChange(true);

    final JSONObject result = UpsertEntityTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("typeName", "McpAllowedNewType")
        .put("matchKeys", new JSONObject().put("id", "x1")), config);

    assertThat(result.getInt("count")).isEqualTo(1);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType("McpAllowedNewType")).isTrue();
  }

  @Test
  void aDatabaseTheCallerMayNotReachIsAnsweredAsOneThatDoesNotExist() {
    // granted only a database that does not exist here, so the test database is one it may not reach
    getServer(0).getSecurity().createUser(new JSONObject()
        .put("name", PROBE_USER)
        .put("password", getServer(0).getSecurity().encodePassword("probepass"))
        .put("databases", new JSONObject().put("mcpProbeOnly", new JSONArray().put("admin"))));
    final ServerSecurityUser probe = getServer(0).getSecurity().authenticate(PROBE_USER, "probepass", null);

    for (final String tool : List.of("query", "get_schema", "upsert_entity")) {
      final String forbidden = refusal(tool, probe, getDatabaseName());
      final String missing = refusal(tool, probe, "mcpDoesNotExist");

      assertThat(forbidden).as(tool).contains("does not exist").doesNotContain("not authorized");
      assertThat(forbidden.replace(getDatabaseName(), "<db>")).as(tool + ": the two refusals must not tell the names apart")
          .isEqualTo(missing.replace("mcpDoesNotExist", "<db>"));
    }

    // and the same holds for a database the user is granted but MCP is not opened to
    config.updateFrom(new JSONObject().put("databases",
        new JSONObject().put(getDatabaseName(), new JSONObject().put("allowedUsers", new JSONArray().put("someone-else")))));
    final String closedToMcp = refusal("query", root, getDatabaseName());
    assertThat(closedToMcp.replace(getDatabaseName(), "<db>"))
        .isEqualTo(refusal("query", root, "mcpDoesNotExist").replace("mcpDoesNotExist", "<db>"));
  }

  private String refusal(final String tool, final ServerSecurityUser user, final String database) {
    final JSONObject args = new JSONObject().put("database", database);
    final Throwable thrown = catchThrowable(() -> {
      switch (tool) {
      case "query" -> QueryTool.execute(getServer(0), user, args.put("language", "sql").put("query", "SELECT 1"), config);
      case "get_schema" -> GetSchemaTool.execute(getServer(0), user, args, config);
      case "upsert_entity" -> UpsertEntityTool.execute(getServer(0), user,
          args.put("typeName", "V").put("matchKeys", new JSONObject().put("id", "x")), config);
      default -> throw new IllegalArgumentException(tool);
      }
    });
    assertThat(thrown).as(tool + " on " + database).isInstanceOf(MCPToolArgumentException.class);
    return thrown.getMessage();
  }

  private void assertRefusedWithoutSchemaChange(final Supplier<JSONObject> call, final String... typesThatMustNotExist) {
    final int typesBefore = getServerDatabase(0, getDatabaseName()).getSchema().getTypes().size();

    final Throwable thrown = catchThrowable(call::get);

    assertThat(thrown).as("the schema change must be refused").isNotNull();
    assertThat(hasSecurityCause(thrown)).as("refused as a security decision: " + thrown).isTrue();
    for (final String type : typesThatMustNotExist)
      assertThat(getServerDatabase(0, getDatabaseName()).getSchema().existsType(type)).as(type).isFalse();
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getTypes()).hasSize(typesBefore);
  }

  private static boolean hasSecurityCause(Throwable t) {
    while (t != null) {
      if (t instanceof SecurityException)
        return true;
      t = t.getCause();
    }
    return false;
  }
}
