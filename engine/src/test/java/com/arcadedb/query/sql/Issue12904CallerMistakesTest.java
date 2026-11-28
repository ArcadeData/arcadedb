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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12904: caller mistakes that the HTTP layer answered 500, because the engine raised them as a type no arm of the
 * error classifier names as the caller's. A function used as a FROM target that returns no record, a polyglot statement sent
 * as a read-only query, and a CREATE TRIGGER whose body does not parse - which was accepted, and then failed every write on the
 * type.
 */
class Issue12904CallerMistakesTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE DOCUMENT TYPE AuditLog");
  }

  @Test
  void aFunctionReturningNoRecordUsedAsTargetIsASemanticError() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM date('2024-01-01', 'yyyy-MM-dd')")) {
        rs.hasNext();
      }
    }).isInstanceOf(CommandSemanticException.class).hasMessageContaining("did not return a record or RID");
  }

  @Test
  void aPolyglotStatementSentAsAQueryIsRefusedAsNotIdempotent() {
    assertThatThrownBy(() -> database.query("js", "1 + 1")).isInstanceOf(QueryNotIdempotentException.class)
        .hasMessageContaining("Use command instead");
  }

  @Test
  void createTriggerRefusesAnSqlBodyThatDoesNotParse() {
    assertThatThrownBy(() -> database.command("sql", "CREATE TRIGGER badt BEFORE CREATE ON TYPE P EXECUTE SQL 'THIS IS NOT SQL'"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("badt");

    assertThat(database.getSchema().existsTrigger("badt")).isFalse();

    database.transaction(() -> database.command("sql", "INSERT INTO P SET k = 1"));
    assertThat(database.countType("P", false)).isEqualTo(1);
  }

  @Test
  void createTriggerStillAcceptsAnSqlBodyThatParses() {
    database.command("sql", "CREATE TRIGGER audit AFTER CREATE ON TYPE P EXECUTE SQL \"INSERT INTO AuditLog SET what = 'created'\"");
    assertThat(database.getSchema().existsTrigger("audit")).isTrue();

    database.transaction(() -> database.command("sql", "INSERT INTO P SET k = 1"));
    assertThat(database.countType("AuditLog", false)).isEqualTo(1);
  }

  @Test
  void createTriggerRefusesAJavaScriptBodyThatDoesNotParse() {
    assertThatThrownBy(() -> database.command("sql", "CREATE TRIGGER badjs BEFORE CREATE ON TYPE P EXECUTE JAVASCRIPT 'var = ;'"))
        .isInstanceOf(CommandParsingException.class).hasMessageContaining("badjs");

    assertThat(database.getSchema().existsTrigger("badjs")).isFalse();

    database.transaction(() -> database.command("sql", "INSERT INTO P SET k = 1"));
    assertThat(database.countType("P", false)).isEqualTo(1);
  }

  @Test
  void createTriggerDoesNotRunTheJavaScriptBodyItChecks() {
    database.command("sql", "CREATE TRIGGER okjs BEFORE CREATE ON TYPE P EXECUTE JAVASCRIPT 'throw new Error(\"ran\");'");
    assertThat(database.getSchema().existsTrigger("okjs")).isTrue();
  }

  /**
   * A body that does not parse and is already stored - written before the check existed - must not keep the database from
   * opening: the check is made where a trigger is created, not where one is loaded.
   */
  @Test
  void aStoredSqlBodyThatDoesNotParseStillLetsTheDatabaseOpen() throws Exception {
    database.command("sql", "CREATE TRIGGER audit AFTER CREATE ON TYPE P EXECUTE SQL 'INSERT INTO AuditLog SET what = 1'");
    database.close();

    final File schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    root.getJSONObject("triggers").getJSONObject("audit").put("actionCode", "THIS IS NOT SQL");
    Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);

    database = factory.open();
    assertThat(database.getSchema().existsTrigger("audit")).isTrue();
    assertThat(database.getSchema().getTrigger("audit").getActionCode()).isEqualTo("THIS IS NOT SQL");
  }
}
