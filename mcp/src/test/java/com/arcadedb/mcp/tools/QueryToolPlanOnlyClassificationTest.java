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

import com.arcadedb.database.Database;
import com.arcadedb.mcp.MCPConfiguration;
import com.arcadedb.mcp.MCPPlugin;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code query} tool answers what an identity allowed to read may run. An openCypher EXPLAIN plans the statement it wraps
 * and, while planning, evaluates the expressions its steps need up front, so a call to a function that is not a built-in one
 * (a {@code DEFINE FUNCTION} body can write) is classified as the statement it wraps is, and refused like it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class QueryToolPlanOnlyClassificationTest extends BaseGraphServerTest {
  private static final String FUNCTION_CALL = "toolplan.bump()";

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
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);

    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType("ToolPlanLog");
    // Answers 1 and records that it ran
    database.command("sql", "DEFINE FUNCTION toolplan.bump \"BEGIN; INSERT INTO ToolPlanLog SET ran = true; COMMIT; RETURN 1;\" LANGUAGE sql");
  }

  private JSONObject query(final String query) {
    return QueryTool.execute(getServer(0), user,
        new JSONObject().put("database", getDatabaseName()).put("language", "opencypher").put("query", query), config);
  }

  private long storedLogs() {
    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT count() AS c FROM ToolPlanLog")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void anExplainedFunctionCallIsRefusedAndRunsNothing() {
    for (final String query : new String[] {//
        "EXPLAIN MATCH (n) RETURN n LIMIT " + FUNCTION_CALL,//
        "EXPLAIN MATCH (n) RETURN n SKIP " + FUNCTION_CALL,//
        "EXPLAIN MATCH (n) RETURN n ORDER BY n.id LIMIT " + FUNCTION_CALL,//
        "EXPLAIN MATCH (n) WITH n LIMIT " + FUNCTION_CALL + " RETURN n",//
        "EXPLAIN WHEN " + FUNCTION_CALL + " = 1 THEN MATCH (n) RETURN n AS x ELSE MATCH (m) RETURN m AS x" })
      assertThatThrownBy(() -> query(query)).as(query).isInstanceOf(SecurityException.class).hasMessageContaining("write operations");

    assertThat(storedLogs()).isZero();
  }

  @Test
  void aConditionCallingAFunctionIsRefusedAndRunsNothing() {
    assertThatThrownBy(() -> query("WHEN " + FUNCTION_CALL + " = 1 THEN MATCH (n) RETURN n AS x ELSE MATCH (m) RETURN m AS x"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("write operations");

    assertThat(storedLogs()).isZero();
  }

  @Test
  void anExplainOfAStatementWithoutSuchACallIsStillAnswered() {
    assertThat(query("EXPLAIN MATCH (n) RETURN n LIMIT 5").getInt("count")).isGreaterThanOrEqualTo(0);
    assertThat(query("EXPLAIN MATCH (n) RETURN n LIMIT toInteger('5')").getInt("count")).isGreaterThanOrEqualTo(0);
    assertThat(storedLogs()).isZero();
  }
}
