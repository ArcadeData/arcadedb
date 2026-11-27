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
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityDatabaseUser;
import com.arcadedb.server.security.ServerSecurityTestAccess;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The text an MCP answer carries for a refused call depends on who worded it. A refusal the MCP layer words itself (a tool
 * the profile does not offer, a read the configuration does not allow, an administration call by a user that is not root)
 * is what the caller needs to correct the call and is kept in every server mode. A refusal the engine raises names the
 * schema objects of the database (the type behind a record id, the type of a statement), so in production mode it is
 * answered with the placeholder every other surface uses and the detail goes to the server log.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MCPRefusalTextByServerModeTest extends BaseGraphServerTest {
  private static final String SCOPED_USER = "mcp-scoped-reader";
  private static final String SCOPED_PWD  = "mcpscopedreader1";
  private static final String OPEN_TYPE   = "OpenDoc";
  private static final String CLOSED_TYPE = "ClosedDoc";

  private MCPConfiguration   config;
  private ServerSecurityUser scoped;
  private ServerSecurityUser root;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowAdmin(true);
    config.setToolProfile(MCPConfiguration.ToolProfile.ALL);
    config.setAllowedUsers(List.of("*"));
    final JSONObject reset = new JSONObject();
    reset.put("principalProfiles", (Object) null);
    reset.put("databases", (Object) null);
    config.updateFrom(reset);

    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType(OPEN_TYPE)) {
      database.command("sql", "CREATE DOCUMENT TYPE " + OPEN_TYPE);
      database.command("sql", "CREATE DOCUMENT TYPE " + CLOSED_TYPE);
      database.transaction(() -> {
        database.newDocument(OPEN_TYPE).set("v", 1).save();
        database.newDocument(CLOSED_TYPE).set("v", 2).save();
      });
    }

    createScopedUser();
    scoped = getServer(0).getSecurity().authenticate(SCOPED_USER, SCOPED_PWD, null);
    root = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @AfterEach
  void dropScopedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
  }

  @Test
  void engineRefusalOnARecordReadIsConcealedInProductionMode() throws Exception {
    final String rid = "#" + closedBucketId() + ":0";

    final String text = withMode("production", () -> toolText("query", queryArguments("SELECT FROM " + rid)));

    assertThat(text).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(text).doesNotContain(CLOSED_TYPE);
  }

  @Test
  void engineRefusalOnATypeReadIsConcealedInProductionMode() throws Exception {
    final String text = withMode("production", () -> toolText("query", queryArguments("SELECT FROM " + CLOSED_TYPE)));

    assertThat(text).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(text).doesNotContain(CLOSED_TYPE);
  }

  @Test
  void developmentModeKeepsTheEngineRefusalText() throws Exception {
    final String rid = "#" + closedBucketId() + ":0";

    final String text = withMode("development", () -> toolText("query", queryArguments("SELECT FROM " + rid)));

    assertThat(text).contains(CLOSED_TYPE);
  }

  @Test
  void allowedReadStillWorksInProductionMode() throws Exception {
    final JSONObject result = withMode("production", () -> call("query", queryArguments("SELECT FROM " + OPEN_TYPE)));

    assertThat(result.getJSONObject("result").getBoolean("isError", false)).isFalse();
  }

  @Test
  void engineRefusalOnAResourceReadIsConcealedInProductionMode() throws Exception {
    final ServerSecurityUser refused = new RefusingUser();

    final JSONObject production = withMode("production", () -> dispatch(refused, "resources/read",
        new JSONObject().put("uri", MCPResources.schemaURI(getDatabaseName()))));
    assertThat(production.getJSONObject("error").getString("message")).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);

    final JSONObject development = withMode("development", () -> dispatch(refused, "resources/read",
        new JSONObject().put("uri", MCPResources.schemaURI(getDatabaseName()))));
    assertThat(development.getJSONObject("error").getString("message")).contains(CLOSED_TYPE);
  }

  @Test
  void engineRefusalWhileBindingThePrincipalIsConcealedInProductionMode() throws Exception {
    final ServerSecurityUser refused = new RefusingUser();

    final JSONObject production = withMode("production", () -> dispatch(refused, "tools/call",
        new JSONObject().put("name", "query").put("arguments", queryArguments("SELECT 1"))));
    assertThat(production.getJSONObject("result").getBoolean("isError", false)).isTrue();
    assertThat(textOf(production.getJSONObject("result"))).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void refusalByTheProfileKeepsItsTextInProductionMode() throws Exception {
    config.setToolProfile(MCPConfiguration.ToolProfile.RAG);

    final String text = withMode("production", () -> toolText("execute_command", new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "sql")
        .put("command", "SELECT 1")));

    assertThat(text).contains("Tool 'execute_command' is not available in");
  }

  @Test
  void refusalByTheConfigurationKeepsItsTextInProductionMode() throws Exception {
    config.setAllowReads(false);

    final String tool = withMode("production", () -> toolText("query", queryArguments("SELECT 1")));
    assertThat(tool).isEqualTo("Read operations are not allowed by MCP configuration");

    final JSONObject resource = withMode("production", () -> dispatch(root, "resources/read",
        new JSONObject().put("uri", MCPResources.schemaURI(getDatabaseName()))));
    assertThat(resource.getJSONObject("error").getString("message")).isEqualTo("Read operations are not allowed by MCP configuration");

    final JSONObject prompt = withMode("production", () -> dispatch(root, "prompts/get",
        new JSONObject().put("name", "graphrag_query")));
    assertThat(prompt.getJSONObject("error").getString("message")).contains("is not available with the current MCP tool profile");
  }

  @Test
  void refusalOfAnAdministrationCallKeepsItsTextInProductionMode() throws Exception {
    final String text = withMode("production", () -> toolText("profiler_status", new JSONObject(), scoped));

    assertThat(text).contains("Only the root user is authorized to execute the server administration operation 'profiler_status'");
  }

  @Test
  void refusalOfAQueryLanguageKeepsItsTextInProductionMode() throws Exception {
    final String text = withMode("production", () -> toolText("query", new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "js")
        .put("query", "1")));

    assertThat(text).contains("Language 'js' is not available through MCP");
  }

  private void createScopedUser() {
    final ServerSecurity security = getServer(0).getSecurity();

    // readRecord on every type by the "*" default, nothing at all on CLOSED_TYPE: a listed type overrides the default
    ServerSecurityTestAccess.databaseGroups(security, getDatabaseName()).put("mcpScopedReaders",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
                .put(CLOSED_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    security.createUser(new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", security.encodePassword(SCOPED_PWD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("mcpScopedReaders"))));
  }

  private int closedBucketId() {
    return getServerDatabase(0, getDatabaseName()).getSchema().getType(CLOSED_TYPE).getBucketIds(false).getFirst();
  }

  private JSONObject queryArguments(final String query) {
    return new JSONObject().put("database", getDatabaseName()).put("language", "sql").put("query", query);
  }

  private String toolText(final String tool, final JSONObject arguments) {
    return toolText(tool, arguments, scoped);
  }

  private String toolText(final String tool, final JSONObject arguments, final ServerSecurityUser caller) {
    final JSONObject response = dispatch(caller, "tools/call", new JSONObject().put("name", tool).put("arguments", arguments));
    final JSONObject result = response.getJSONObject("result");
    assertThat(result.getBoolean("isError", false)).as(tool).isTrue();
    return textOf(result);
  }

  private JSONObject call(final String tool, final JSONObject arguments) {
    return dispatch(scoped, "tools/call", new JSONObject().put("name", tool).put("arguments", arguments));
  }

  private JSONObject dispatch(final ServerSecurityUser caller, final String method, final JSONObject params) {
    final JSONObject request = new JSONObject().put("jsonrpc", "2.0").put("id", 1).put("method", method).put("params", params);
    return new MCPDispatcher(getServer(0), config, "http").dispatch(request, caller, MCPStdioServer.STDIO_CLIENT).json();
  }

  private static String textOf(final JSONObject result) {
    return result.getJSONArray("content").getJSONObject(0).getString("text");
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

  /** A principal whose database user cannot be bound: the refusal comes from the security layer, with the text the engine words. */
  private class RefusingUser extends ServerSecurityUser {
    RefusingUser() {
      super(getServer(0), new JSONObject().put("name", "refused-reader")
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray())));
    }

    @Override
    public ServerSecurityDatabaseUser getDatabaseUser(final Database database) {
      throw new SecurityException("User 'refused-reader' is not allowed to read record on type '" + CLOSED_TYPE + "'");
    }
  }
}
