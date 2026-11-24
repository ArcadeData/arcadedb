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
 * The write permissions of {@code execute_command} apply to every write an openCypher statement can run, not only to the ones
 * written at the top level of the statement: a write inside a {@code CALL { ... }} subquery, a {@code FOREACH} body, a branch of
 * a {@code UNION} or a conditional query, or a write procedure called next to a {@code CREATE} needs its own permission.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MCPCypherNestedWritePermissionsTest extends BaseGraphServerTest {
  private static final String MARKER_PROPERTY = "nestedWriteMarker";

  private MCPConfiguration   config;
  private ServerSecurityUser root;

  @BeforeEach
  void setupMCP() {
    config = MCPPlugin.of(getServer(0)).getConfiguration();
    config.setEnabled(true);
    config.setAllowReads(true);
    config.setAllowInsert(true);
    config.setAllowUpdate(false);
    config.setAllowDelete(false);
    config.setAllowSchemaChange(false);
    config.setAllowAdmin(false);
    config.setAllowedUsers(List.of("root"));
    final JSONObject clearOverrides = new JSONObject();
    clearOverrides.put("databases", (Object) null);
    config.updateFrom(clearOverrides);
    root = getServer(0).getSecurity().authenticate("root", DEFAULT_PASSWORD_FOR_TESTS, null);
  }

  @AfterEach
  void unbind() {
    // the tools bind their principal on the calling thread; the dispatcher clears it, a direct call does not
    DatabaseContext.INSTANCE.removeCurrentThreadContexts();
  }

  private JSONObject execute(final String command) {
    return ExecuteCommandTool.execute(getServer(0), root, new JSONObject()
        .put("database", getDatabaseName())
        .put("language", "cypher")
        .put("command", command), config);
  }

  private long count(final String match) {
    return getServerDatabase(0, getDatabaseName()).query("opencypher", match + " RETURN count(n) AS c").next()
        .<Number>getProperty("c").longValue();
  }

  /** Runs the command and checks that it is refused whole: nothing of it ran, so the data is what it was. */
  private void assertRefusedWithoutChange(final String command, final String message) {
    final long vertices = count("MATCH (n:" + VERTEX1_TYPE_NAME + ")");
    assertThat(vertices).isPositive();

    assertThatThrownBy(() -> execute(command)).as(command).isInstanceOf(SecurityException.class).hasMessageContaining(message);

    assertThat(count("MATCH (n:" + VERTEX1_TYPE_NAME + ")")).as(command).isEqualTo(vertices);
    assertThat(count("MATCH (n:" + VERTEX1_TYPE_NAME + ") WHERE n." + MARKER_PROPERTY + " IS NOT NULL")).as(command).isZero();
  }

  @Test
  void anInsertOnlyConfigurationRefusesNestedUpdatesAndDeletes() {
    final String created = "CREATE (a:" + VERTEX1_TYPE_NAME + " {" + MARKER_PROPERTY + ": 1}) ";
    for (final String command : new String[] { //
        created + "WITH a CALL { MATCH (b:" + VERTEX1_TYPE_NAME + ") DETACH DELETE b } RETURN a", //
        created + "WITH a CALL { MATCH (b:" + VERTEX1_TYPE_NAME + ") SET b.name = 'changed' } RETURN a", //
        created + "WITH a CALL { CALL { MATCH (b:" + VERTEX1_TYPE_NAME + ") DELETE b } } RETURN a", //
        created + "WITH a CALL { MATCH (b:" + VERTEX1_TYPE_NAME + ") DELETE b } IN TRANSACTIONS RETURN a", //
        created + "WITH a FOREACH (i IN [1] | SET a.name = 'changed') RETURN a", //
        created + "WITH a FOREACH (i IN [a] | DELETE i) RETURN 1 AS one", //
        created + "WITH a FOREACH (i IN [1] | FOREACH (j IN [a] | REMOVE j." + MARKER_PROPERTY + ")) RETURN a", //
        created + "RETURN a AS n UNION MATCH (m:" + VERTEX1_TYPE_NAME + ") CALL (m) { DETACH DELETE m } RETURN m AS n", //
        created + "RETURN a AS n UNION MATCH (m:" + VERTEX1_TYPE_NAME + ") FOREACH (i IN [1] | SET m.name = 'changed') RETURN m AS n", //
        "WHEN true THEN " + created + "RETURN a AS n ELSE MATCH (m:" + VERTEX1_TYPE_NAME
            + ") CALL (m) { DETACH DELETE m } RETURN m AS n", //
        created + "WITH a CALL merge.node(['" + VERTEX1_TYPE_NAME + "'], {" + MARKER_PROPERTY + ": 2}, {}) YIELD node RETURN node" })
      assertRefusedWithoutChange(command, "not allowed by MCP configuration");
  }

  @Test
  void aNestedDeleteNeedsTheDeletePermissionAlone() {
    config.setAllowUpdate(true);
    final String created = "CREATE (a:" + VERTEX1_TYPE_NAME + " {" + MARKER_PROPERTY + ": 1}) ";
    assertRefusedWithoutChange(created + "WITH a CALL { MATCH (b:" + VERTEX1_TYPE_NAME + ") DETACH DELETE b } RETURN a",
        "Delete operations are not allowed");
    assertRefusedWithoutChange(created + "WITH a FOREACH (i IN [a] | DELETE i) RETURN 1 AS one",
        "Delete operations are not allowed");
  }

  @Test
  void aNestedUpdateNeedsTheUpdatePermissionAlone() {
    config.setAllowDelete(true);
    final String created = "CREATE (a:" + VERTEX1_TYPE_NAME + " {" + MARKER_PROPERTY + ": 1}) ";
    assertRefusedWithoutChange(created + "WITH a CALL { MATCH (b:" + VERTEX1_TYPE_NAME + ") SET b.name = 'changed' } RETURN a",
        "Update operations are not allowed");
    assertRefusedWithoutChange(created + "WITH a FOREACH (i IN [1] | SET a.name = 'changed') RETURN a",
        "Update operations are not allowed");
  }

  @Test
  void aNestedInsertRunsWithTheInsertPermissionAlone() {
    final JSONObject result = execute("CREATE (a:" + VERTEX1_TYPE_NAME + " {" + MARKER_PROPERTY + ": 1}) WITH a CALL (a) { CREATE (:"
        + VERTEX1_TYPE_NAME + " {" + MARKER_PROPERTY + ": 2}) } RETURN a");

    assertThat(result.getInt("count")).isEqualTo(1);
    assertThat(count("MATCH (n:" + VERTEX1_TYPE_NAME + ") WHERE n." + MARKER_PROPERTY + " IS NOT NULL")).isEqualTo(2);
  }

  @Test
  void everyNestedWriteRunsWhenEveryWritePermissionIsGranted() {
    config.setAllowUpdate(true);
    config.setAllowDelete(true);

    execute("CREATE (a:" + VERTEX1_TYPE_NAME + " {" + MARKER_PROPERTY + ": 1}) WITH a CALL (a) { SET a." + MARKER_PROPERTY
        + " = 5 } RETURN a");
    assertThat(count("MATCH (n:" + VERTEX1_TYPE_NAME + ") WHERE n." + MARKER_PROPERTY + " = 5")).isEqualTo(1);

    execute("MATCH (a:" + VERTEX1_TYPE_NAME + ") WHERE a." + MARKER_PROPERTY + " = 5 FOREACH (i IN [a] | DELETE i)");
    assertThat(count("MATCH (n:" + VERTEX1_TYPE_NAME + ") WHERE n." + MARKER_PROPERTY + " IS NOT NULL")).isZero();
  }
}
