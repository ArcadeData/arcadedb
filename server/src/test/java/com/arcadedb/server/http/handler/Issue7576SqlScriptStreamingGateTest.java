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
package com.arcadedb.server.http.handler;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7576: {@link AbstractQueryHandler#requireStreamableStatement} reads {@code getOperationTypes()} to decide
 * whether a statement may reach the {@code application/x-ndjson} streaming encoding (issue #7306, extended to
 * {@code GET /query} by #7571). Every SQL statement gets this right on its own, but a {@code sqlscript} that wraps
 * {@code BACKUP DATABASE} used to report only {@code READ} through {@code SQLScriptQueryEngine.analyze()}, so the
 * gate - identical for every language - let it through under {@code sqlscript} while refusing it under {@code sql}.
 */
class Issue7576SqlScriptStreamingGateTest extends TestHelper {

  @Test
  void refusesBackupDatabaseThroughSqlScriptExactlyAsThroughPlainSql() {
    assertThatThrownBy(() -> AbstractQueryHandler.requireStreamableStatement(database, "sqlscript", "BACKUP DATABASE;"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("read-only statement");

    assertThatThrownBy(() -> AbstractQueryHandler.requireStreamableStatement(database, "sql", "BACKUP DATABASE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("read-only statement");
  }

  @Test
  void refusesNoSpellingOfBackupDatabaseThroughSqlScript() {
    for (final String spelling : new String[] { "backup database;", "BaCkUp   DaTaBaSe;", "  BACKUP DATABASE  ;" })
      assertThatThrownBy(() -> AbstractQueryHandler.requireStreamableStatement(database, "sqlscript", spelling))
          .as("spelling '%s'", spelling)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("read-only statement");
  }

  @Test
  void stillAllowsAScriptOfPlainReadsThroughSqlScript() {
    assertThatCode(() -> AbstractQueryHandler.requireStreamableStatement(database, "sqlscript",
        "SELECT FROM V; SELECT count(*) FROM V;")).doesNotThrowAnyException();
  }

  @Test
  void refusesAScriptThatMixesAReadWithAWriteThroughSqlScript() {
    assertThatThrownBy(() -> AbstractQueryHandler.requireStreamableStatement(database, "sqlscript",
        "SELECT FROM V; INSERT INTO V SET name = 'test';"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("read-only statement");
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
  }
}
