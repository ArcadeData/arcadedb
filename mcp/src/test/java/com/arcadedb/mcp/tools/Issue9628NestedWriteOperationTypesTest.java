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
import com.arcadedb.query.OperationType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9628: the MCP permission gate checks the operation types the statement declares. A write nested in a LET, a
 * projection or a FROM target was declared {@code [READ]}, so an identity allowed only to read ran inserts, deletes and
 * schema changes; a DDL inside an IF was declared a data write, so {@code allowSchemaChange} was never consulted.
 */
class Issue9628NestedWriteOperationTypesTest extends BaseGraphServerTest {

  private static MCPConfiguration readOnly() {
    final MCPConfiguration config = new MCPConfiguration("./target/test");
    config.setAllowReads(true);
    config.setAllowInsert(false);
    config.setAllowUpdate(false);
    config.setAllowDelete(false);
    config.setAllowSchemaChange(false);
    config.setAllowAdmin(false);
    return config;
  }

  @Test
  void nestedWritesDeclareTheirOperation() {
    final Database database = getDatabase(0);
    assertThat(ExecuteCommandTool.getOperationTypes(database, "SELECT FROM " + VERTEX1_TYPE_NAME + " LET $y = (INSERT INTO "
        + VERTEX1_TYPE_NAME + " SET a = 2)", "sql")).contains(OperationType.CREATE);
    assertThat(ExecuteCommandTool.getOperationTypes(database, "LET $x = (DELETE FROM " + VERTEX1_TYPE_NAME + ")", "sql"))
        .contains(OperationType.DELETE);
    assertThat(ExecuteCommandTool.getOperationTypes(database, "LET $x = (CREATE DOCUMENT TYPE Sneak)", "sql"))
        .containsExactly(OperationType.SCHEMA);
    assertThat(ExecuteCommandTool.getOperationTypes(database, "IF (1=1) { CREATE VERTEX TYPE Pwned; }", "sqlscript"))
        .containsExactly(OperationType.SCHEMA);
  }

  @Test
  void aReadOnlyIdentityIsRefusedANestedWrite() {
    final Database database = getDatabase(0);
    for (final String command : new String[] { //
        "SELECT FROM " + VERTEX1_TYPE_NAME + " LET $y = (INSERT INTO " + VERTEX1_TYPE_NAME + " SET a = 2)", //
        "SELECT FROM (INSERT INTO " + VERTEX1_TYPE_NAME + " SET a = 6)", //
        "LET $x = (DELETE FROM " + VERTEX1_TYPE_NAME + ")", //
        "LET $x = (CREATE DOCUMENT TYPE Sneak)" })
      assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(database, command, "sql", readOnly()))
          .as(command)
          .isInstanceOf(SecurityException.class)
          .hasMessageContaining("not allowed");
  }

  /** An identity that may insert but not change the schema is refused a DDL wrapped in a block. */
  @Test
  void aDdlInABlockNeedsSchemaPermission() {
    final Database database = getDatabase(0);
    final MCPConfiguration dataWriter = readOnly();
    dataWriter.setAllowInsert(true);
    dataWriter.setAllowUpdate(true);
    dataWriter.setAllowDelete(true);
    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(database, "IF (1=1) { CREATE VERTEX TYPE Pwned; }", "sqlscript", dataWriter))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Schema change");
  }

  @Test
  void aNestedReadIsStillAllowedForAReadOnlyIdentity() {
    final Database database = getDatabase(0);
    ExecuteCommandTool.checkPermission(database, "SELECT $y FROM " + VERTEX1_TYPE_NAME + " LET $y = (SELECT count(*) FROM "
        + VERTEX1_TYPE_NAME + ")", "sql", readOnly());
  }
}
