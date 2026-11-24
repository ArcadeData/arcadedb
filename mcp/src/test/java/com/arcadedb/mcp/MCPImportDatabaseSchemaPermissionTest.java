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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.mcp.tools.ExecuteCommandTool;
import com.arcadedb.mcp.tools.MCPToolUtils;
import com.arcadedb.query.OperationType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@code allowSchemaChange=false} holds for a restore of a file too: an {@code IMPORT DATABASE} of a JSONL export recreates the
 * schema it carries (types, settings, triggers, function libraries, extensions), so it is a schema change for the MCP
 * configuration and for the engine, whichever way the statement reaches it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MCPImportDatabaseSchemaPermissionTest extends BaseGraphServerTest {
  private final File source = new File("target/mcp-restore-source.jsonl");

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
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    root = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @AfterEach
  void cleanUp() {
    // the tools bind their principal on the calling thread; the dispatcher clears it, a direct call does not
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
    source.delete();
  }

  @Test
  void importDatabaseDeclaresASchemaChange() {
    assertThat(ExecuteCommandTool.getOperationTypes(getDatabase(0), "IMPORT DATABASE file://" + source.getAbsolutePath(), "sql"))
        .contains(OperationType.SCHEMA);
    assertThat(ExecuteCommandTool.getOperationTypes(getDatabase(0),
        "IMPORT DATABASE file://" + source.getAbsolutePath() + " WITH vertexType = 'Imported'", "sql")).contains(OperationType.SCHEMA);
  }

  @Test
  void anImportCarryingTriggersIsRefusedWhenSchemaChangesAreRefused() throws IOException {
    writeSource("SQL", "SELECT 1");

    final Throwable thrown = catchThrowable(() -> importThroughTheTool());

    // refused by the configuration, before the statement runs
    assertThat(thrown).isInstanceOf(SecurityException.class).hasMessageContaining("Schema change operations are not allowed");
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getEmbedded().existsTrigger("restoredTrigger")).isFalse();
  }

  @Test
  void anImportCarryingHostCodeTriggersIsRefusedWhenSchemaChangesAreRefused() throws IOException {
    writeSource("JAVASCRIPT", "true;");

    final Throwable thrown = catchThrowable(() -> importThroughTheTool());

    assertThat(thrown).isInstanceOf(SecurityException.class).hasMessageContaining("Schema change operations are not allowed");
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getEmbedded().existsTrigger("restoredTrigger")).isFalse();
  }

  @Test
  void theEngineRefusesTheMembersOfAFileToThePrincipalTheToolsRunAs() {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    MCPToolUtils.bindCurrentUser(database, root, config.getPermissionsForDatabase(getDatabaseName()));
    final LocalSchema schema = database.getSchema().getEmbedded();

    final Throwable thrown = catchThrowable(
        () -> schema.restoreSchemaMembersFromJSON(schemaDeclaringTrigger("SQL", "SELECT 1"), LocalSchema.SchemaMemberSource.IMPORTED_FILE));

    assertThat(hasSecurityCause(thrown)).as("refused as a permission decision: " + thrown).isTrue();
    assertThat(schema.existsTrigger("restoredTrigger")).isFalse();
  }

  @Test
  void theEngineRefusesTheSettingsOfTheSchemaToThePrincipalTheToolsRunAs() {
    final DatabaseInternal database = (DatabaseInternal) getServerDatabase(0, getDatabaseName());
    final String dateFormat = database.getSchema().getDateFormat();
    MCPToolUtils.bindCurrentUser(database, root, config.getPermissionsForDatabase(getDatabaseName()));

    final Throwable thrown = catchThrowable(() -> database.getSchema().setDateFormat("dd/MM/yyyy"));

    assertThat(hasSecurityCause(thrown)).as("refused as a permission decision: " + thrown).isTrue();
    assertThat(database.getSchema().getDateFormat()).isEqualTo(dateFormat);
  }

  private void importThroughTheTool() {
    ExecuteCommandTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("command", "IMPORT DATABASE file://" + source.getAbsolutePath()), config);
  }

  /** A restore file whose schema line declares no type and one trigger bound to a type the database already has. */
  private void writeSource(final String actionType, final String actionCode) throws IOException {
    final JSONObject schemaLine = new JSONObject().put("t", "schema").put("c", schemaDeclaringTrigger(actionType, actionCode)
        .put("settings", new JSONObject()
            .put("dateFormat", "yyyy-MM-dd")
            .put("dateTimeFormat", "yyyy-MM-dd HH:mm:ss")
            .put("zoneId", "UTC"))
        .put("types", new JSONObject()));
    Files.writeString(source.toPath(), schemaLine + "\n", StandardCharsets.UTF_8);
  }

  private static JSONObject schemaDeclaringTrigger(final String actionType, final String actionCode) {
    return new JSONObject().put("triggers", new JSONObject().put("restoredTrigger", new JSONObject()
        .put("name", "restoredTrigger")
        .put("typeName", VERTEX1_TYPE_NAME)
        .put("timing", "AFTER")
        .put("event", "CREATE")
        .put("actionType", actionType)
        .put("actionCode", actionCode)));
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
