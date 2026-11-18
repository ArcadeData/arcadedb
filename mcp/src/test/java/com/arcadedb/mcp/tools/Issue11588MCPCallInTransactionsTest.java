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

/**
 * Issue #11588 over MCP: {@code execute_command} runs its statement in the transaction
 * {@link MCPToolUtils#collectInTransaction} opens around the tool call, an implicit transaction, so CALL { ... } IN
 * TRANSACTIONS keeps running in batches there instead of being refused as inside a transaction a client owns.
 */
class Issue11588MCPCallInTransactionsTest extends BaseGraphServerTest {
  private static final String LABEL = "Mcp11588";

  private MCPConfiguration   config;
  private ServerSecurityUser user;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(true);
    // The classifier does not look inside the CALL { ... } subquery and falls back to every write kind
    config.setAllowUpdate(true);
    config.setAllowDelete(true);
    config.setAllowSchemaChange(true);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    user = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @Test
  void executeCommandRunsCallInTransactionsInBatches() {
    final JSONObject result = ExecuteCommandTool.execute(getServer(0), user, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "opencypher")
        .put("command", "UNWIND range(1, 25) AS i CALL { WITH i CREATE (:" + LABEL + " {i: i}) } IN TRANSACTIONS OF 10 ROWS"),
        config);

    assertThat(result.getInt("count")).isZero();
    assertThat(getServerDatabase(0, getDatabaseName()).query("opencypher", "MATCH (x:" + LABEL + ") RETURN count(x) AS c").next()
        .<Number>getProperty("c").longValue()).isEqualTo(25);
  }
}
