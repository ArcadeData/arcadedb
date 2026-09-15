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

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7576: {@code SQLScriptQueryEngine.analyze()} did not override {@code getOperationTypes()}, so it inherited
 * the {@link com.arcadedb.query.QueryEngine.AnalyzedQuery} default that derives the set from {@code isIdempotent()}
 * alone. {@code BACKUP DATABASE} is idempotent but writes an archive to the server filesystem - under
 * {@code sql} its own {@code getOperationTypes()} override reports {@code {READ, CREATE}}; under {@code sqlscript}
 * that override was discarded and only {@code READ} reached the caller.
 * <p>
 * {@link ExecuteCommandTool#getOperationTypes} is exactly the permission-check input {@code ExecuteCommandTool}
 * uses ({@code language} is caller-supplied and reaches {@code database.getQueryEngine(language)} directly), so a
 * {@code sqlscript} caller whose identity is read-only used to pass the check for a command that writes to disk.
 */
class Issue7576SqlScriptOperationTypesTest extends BaseGraphServerTest {

  @Test
  void sqlScriptBackupDatabaseReportsTheWriteNotJustRead() {
    final Database database = getDatabase(0);
    final Set<OperationType> operations = ExecuteCommandTool.getOperationTypes(database, "BACKUP DATABASE;", "sqlscript");
    assertThat(operations).containsExactlyInAnyOrder(OperationType.READ, OperationType.CREATE);
  }

  /**
   * The consequence that makes this more than a streaming inconsistency: a read-only MCP identity must not be
   * able to run {@code BACKUP DATABASE} through {@code sqlscript} just because the language wraps the statement.
   */
  @Test
  void aReadOnlyIdentityIsRefusedBackupDatabaseThroughSqlScript() {
    final Database database = getDatabase(0);
    final MCPConfiguration readOnly = new MCPConfiguration("./target/test");
    readOnly.setAllowReads(true);
    readOnly.setAllowInsert(false);
    readOnly.setAllowUpdate(false);
    readOnly.setAllowDelete(false);
    readOnly.setAllowSchemaChange(false);
    readOnly.setAllowAdmin(false);

    assertThatThrownBy(() -> ExecuteCommandTool.checkPermission(database, "BACKUP DATABASE;", "sqlscript", readOnly))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("not allowed");
  }

  @Test
  void aScriptOfPlainReadsIsStillAllowedForAReadOnlyIdentity() {
    final Database database = getDatabase(0);
    final MCPConfiguration readOnly = new MCPConfiguration("./target/test");
    readOnly.setAllowReads(true);
    readOnly.setAllowInsert(false);
    readOnly.setAllowUpdate(false);
    readOnly.setAllowDelete(false);
    readOnly.setAllowSchemaChange(false);
    readOnly.setAllowAdmin(false);

    // Should not throw: a script made only of SELECT statements is genuinely read-only.
    ExecuteCommandTool.checkPermission(database, "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1; SELECT count(*) FROM "
        + VERTEX1_TYPE_NAME + ";", "sqlscript", readOnly);
  }
}
