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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code SELECT FROM schema:triggers} - the catalog listing that closes the hole found while sweeping #7654's
 * exception messages.
 * <p>
 * {@code RemoteSchema.existsTrigger()} already issued this exact query, so on a remote database it answered
 * {@code UnsupportedOperationException: Invalid metadata: triggers} instead of true or false: the target was never
 * registered in {@code SelectExecutionPlanner.handleSchemaAsTarget}. {@code CREATE TRIGGER} and {@code DROP TRIGGER}
 * were both reachable over the wire while nothing could list what they had produced.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7654SchemaTriggersCatalogTest extends TestHelper {

  private static final String TYPE = "Issue7654Audited";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE).createProperty("v", Type.INTEGER);
  }

  @Test
  void theCatalogListsEveryTriggerAndItsDefinition() {
    database.command("sql", "CREATE TRIGGER bumpOnCreate BEFORE CREATE ON TYPE " + TYPE + " EXECUTE SQL 'SELECT 1'");
    database.command("sql", "CREATE TRIGGER auditOnUpdate AFTER UPDATE ON TYPE " + TYPE + " EXECUTE SQL 'SELECT 2'");

    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:triggers")) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(2);

      // Ordered by name, case-insensitively, like every sibling catalog listing.
      assertThat(rows.stream().map(r -> r.<String>getProperty("name")).toList())
          .containsExactly("auditOnUpdate", "bumpOnCreate");

      final Result audit = rows.getFirst();
      assertThat(audit.<String>getProperty("typeName")).isEqualTo(TYPE);
      assertThat(audit.<String>getProperty("timing")).isEqualTo("AFTER");
      assertThat(audit.<String>getProperty("event")).isEqualTo("UPDATE");
      assertThat(audit.<String>getProperty("actionType")).isEqualTo("SQL");
      assertThat(audit.<String>getProperty("actionCode")).contains("SELECT 2");
    }
  }

  /**
   * The shape {@code RemoteSchema.existsTrigger()} depends on: before this target existed, the query it issues threw
   * rather than answering, so the remote method could never return false either.
   */
  @Test
  void theCatalogAnswersTheExistenceCheckTheRemoteClientIssues() {
    try (final ResultSet none = database.query("sql", "SELECT FROM schema:triggers WHERE name = 'absent'")) {
      assertThat(none.hasNext()).isFalse();
    }

    database.command("sql", "CREATE TRIGGER presentTrigger BEFORE CREATE ON TYPE " + TYPE + " EXECUTE SQL 'SELECT 1'");

    try (final ResultSet found = database.query("sql", "SELECT FROM schema:triggers WHERE name = 'presentTrigger'")) {
      assertThat(found.hasNext()).isTrue();
    }

    database.command("sql", "DROP TRIGGER presentTrigger");

    try (final ResultSet gone = database.query("sql", "SELECT FROM schema:triggers WHERE name = 'presentTrigger'")) {
      assertThat(gone.hasNext()).isFalse();
    }
  }

  @Test
  void theCatalogIsEmptyRatherThanBrokenOnADatabaseWithNoTriggers() {
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:triggers")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
