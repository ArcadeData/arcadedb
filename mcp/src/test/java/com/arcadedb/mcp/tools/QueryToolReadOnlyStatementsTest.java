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

import com.arcadedb.database.DatabaseContext;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code query} tool answers statements that declare a read and nothing else. A statement that declares another operation
 * (the backup statement) is refused, before it runs, in every language that can wrap it, with the pointer to
 * {@code execute_command} that every other write gets, and plain reads are still answered.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class QueryToolReadOnlyStatementsTest extends BaseGraphServerTest {
  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(false);
    config.setAllowUpdate(false);
    config.setAllowDelete(false);
    config.setAllowSchemaChange(false);
    config.setAllowAdmin(false);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @AfterEach
  void unbind() {
    // the tools bind their principal on the calling thread; the dispatcher clears it, a direct call does not
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
  }

  private JSONObject query(final String language, final String text) {
    return QueryTool.execute(getServer(0), user,
        new JSONObject().put("database", getDatabaseName()).put("language", language).put("query", text), config);
  }

  @Test
  void aStatementDeclaringAnotherOperationThanAReadIsRefusedWhateverTheLanguage() {
    final String[][] statements = {//
        { "sql", "BACKUP DATABASE" },//
        { "sql", "backup   database" },//
        { "sqlscript", "BACKUP DATABASE;" },//
        { "sqlscript", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1; BACKUP DATABASE;" },//
        { "sqlscript", "IF (1 = 1) { BACKUP DATABASE; }" } };

    for (final String[] statement : statements)
      assertThatThrownBy(() -> query(statement[0], statement[1]))
          .as("%s: %s", statement[0], statement[1])
          .isInstanceOf(SecurityException.class)
          .hasMessageContaining("execute_command");
  }

  @Test
  void theCommandToolRefusesTheSameStatementUnderTheSameConfiguration() {
    assertThatThrownBy(() -> ExecuteCommandTool.execute(getServer(0), user,
        new JSONObject().put("database", getDatabaseName()).put("language", "sql").put("command", "BACKUP DATABASE"), config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void plainReadsAreStillAnswered() {
    assertThat(query("sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1").getInt("count")).isEqualTo(1);
    assertThat(query("sqlscript", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1; SELECT count(*) AS c FROM "
        + VERTEX1_TYPE_NAME + ";").getInt("count")).isEqualTo(1);
    assertThat(query("cypher", "MATCH (n:" + VERTEX1_TYPE_NAME + ") RETURN n LIMIT 1").getInt("count")).isEqualTo(1);
    assertThat(query("sql", "EXPLAIN SELECT FROM " + VERTEX1_TYPE_NAME).getInt("count")).isGreaterThanOrEqualTo(0);
  }
}
