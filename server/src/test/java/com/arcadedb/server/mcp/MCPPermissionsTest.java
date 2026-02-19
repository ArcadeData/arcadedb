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

import com.arcadedb.server.mcp.tools.ExecuteCommandTool;
import org.junit.jupiter.api.Test;

import static com.arcadedb.server.mcp.tools.ExecuteCommandTool.OperationType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MCPPermissionsTest {

  @Test
  void testSqlInsertDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("INSERT INTO Person SET name='John'", "sql"))
        .isEqualTo(OperationType.INSERT);
  }

  @Test
  void testSqlUpdateDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("UPDATE Person SET name='Jane' WHERE name='John'", "sql"))
        .isEqualTo(OperationType.UPDATE);
  }

  @Test
  void testSqlDeleteDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("DELETE FROM Person WHERE name='John'", "sql"))
        .isEqualTo(OperationType.DELETE);
  }

  @Test
  void testSqlCreateTypeDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("CREATE VERTEX TYPE MyVertex", "sql"))
        .isEqualTo(OperationType.SCHEMA);
  }

  @Test
  void testSqlCreateEdgeTypeDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("CREATE EDGE TYPE MyEdge", "sql"))
        .isEqualTo(OperationType.SCHEMA);
  }

  @Test
  void testSqlAlterTypeDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("ALTER TYPE Person", "sql"))
        .isEqualTo(OperationType.SCHEMA);
  }

  @Test
  void testSqlDropTypeDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("DROP TYPE Person", "sql"))
        .isEqualTo(OperationType.SCHEMA);
  }

  @Test
  void testSqlCreateIndexDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("CREATE INDEX ON Person (name) UNIQUE", "sql"))
        .isEqualTo(OperationType.SCHEMA);
  }

  @Test
  void testSqlCreatePropertyDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("CREATE PROPERTY Person.age INTEGER", "sql"))
        .isEqualTo(OperationType.SCHEMA);
  }

  @Test
  void testCypherCreateNodeDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("CREATE (n:Person {name: 'John'})", "cypher"))
        .isEqualTo(OperationType.INSERT);
  }

  @Test
  void testCypherDeleteDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("MATCH (n:Person) DELETE n", "cypher"))
        .isEqualTo(OperationType.DELETE);
  }

  @Test
  void testCypherSetDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("MATCH (n:Person) SET n.name = 'Jane'", "cypher"))
        .isEqualTo(OperationType.UPDATE);
  }

  @Test
  void testCypherMergeDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("MERGE (n:Person {name: 'John'})", "cypher"))
        .isEqualTo(OperationType.UPDATE);
  }

  @Test
  void testCypherCreateIndexDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("CREATE INDEX FOR (n:Person) ON (n.name)", "cypher"))
        .isEqualTo(OperationType.SCHEMA);
  }

  @Test
  void testPermissionCheckDeniesInsert() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowInsert(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission("INSERT INTO Person SET name='John'", "sql", config))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void testPermissionCheckAllowsInsert() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowInsert(true);

    // Should not throw
    ExecuteCommandTool.checkPermission("INSERT INTO Person SET name='John'", "sql", config);
  }

  @Test
  void testPermissionCheckDeniesDelete() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowDelete(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission("DELETE FROM Person", "sql", config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void testPermissionCheckDeniesSchemaChange() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowSchemaChange(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission("CREATE VERTEX TYPE NewType", "sql", config))
        .isInstanceOf(SecurityException.class);
  }

  @Test
  void testCaseSensitivity() {
    assertThat(ExecuteCommandTool.detectOperationType("  insert into Person set name='John'", "sql"))
        .isEqualTo(OperationType.INSERT);
  }

  @Test
  void testCypherRemoveDetection() {
    assertThat(ExecuteCommandTool.detectOperationType("MATCH (n:Person) REMOVE n.age", "cypher"))
        .isEqualTo(OperationType.UPDATE);
  }

  @Test
  void testCypherKeywordInStringLiteral() {
    // Keywords inside string literals should not trigger false positives with word-boundary matching
    assertThat(ExecuteCommandTool.detectOperationType("MATCH (n) WHERE n.name = 'DELETED' RETURN n", "cypher"))
        .isEqualTo(OperationType.UNKNOWN);
  }
}
