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
import com.arcadedb.mcp.MCPToolArgumentException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issues #12202 and #12232 over MCP: {@code execute_command} runs each call in a transaction of its own, so a SQL {@code BEGIN},
 * {@code COMMIT} or {@code ROLLBACK}, or an openCypher {@code START TRANSACTION}, {@code COMMIT} or {@code ROLLBACK}, acted on that
 * one call alone and was answered OK, while a later {@code ROLLBACK} undid nothing. It is refused now with an argument error that
 * says so, as the HTTP and gRPC command entry points refuse it.
 */
class Issue12232MCPStandaloneTransactionControlTest extends BaseGraphServerTest {
  private static final String TYPE = "Mcp12232";

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(true);
    config.setAllowDelete(true);
    config.setAllowSchemaChange(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @ParameterizedTest
  @CsvSource({ "sql,begin", "sql,COMMIT", "sql,ROLLBACK", "sql,PROFILE BEGIN", "cypher,START TRANSACTION", "opencypher,COMMIT",
      "opencypher,rollback", "opencypher,PROFILE START TRANSACTION" })
  void aTransactionControlAsACommandOfItsOwnIsRefused(final String language, final String statement) {
    assertThatThrownBy(() -> run(language, statement)).isInstanceOf(MCPToolArgumentException.class)
        .hasMessageContaining("cannot run as a command of its own").hasMessageContaining("execute_command");
  }

  @Test
  void anOrdinaryCommandStillRuns() {
    run("sql", "CREATE DOCUMENT TYPE " + TYPE);

    final JSONObject inserted = run("sql", "INSERT INTO " + TYPE + " SET k = 1");
    assertThat(inserted.getInt("count")).isEqualTo(1);

    final JSONObject counted = run("sql", "SELECT count(@rid) AS c FROM " + TYPE);
    assertThat(counted.getJSONArray("records").getJSONObject(0).getLong("c")).isEqualTo(1L);
  }

  private JSONObject run(final String language, final String command) {
    return ExecuteCommandTool.execute(getServer(0), user,
        new JSONObject().put("database", getDatabaseName()).put("language", language).put("command", command), config);
  }
}
