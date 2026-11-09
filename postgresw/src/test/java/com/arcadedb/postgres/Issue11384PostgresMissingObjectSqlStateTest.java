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
package com.arcadedb.postgres;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.UnknownProcedureException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11384: a statement naming an index, a trigger, a view, a bucket, a property or a type that does not exist answered
 * {@code XX000} internal_error, as did a method or a procedure nothing answers, while the sibling statements that reach the same name
 * through the schema answered {@code 42P01}. They answer the code of the missing object now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11384PostgresMissingObjectSqlStateTest extends TestHelper {

  @Test
  void aMissingSchemaObjectIsUndefinedTable() {
    database.getSchema().createVertexType("VV");
    for (final String statement : List.of("DROP INDEX nosuchindex", "DROP TRIGGER nosuchtrg", "DROP MATERIALIZED VIEW nosuchmv",
        "DROP CONTINUOUS AGGREGATE nosuchca", "DROP GRAPH ANALYTICAL VIEW nosuchgav", "TRUNCATE BUCKET nosuchbucket",
        "DROP PROPERTY VV.nosuch", "CREATE VERTEX TYPE VX EXTENDS nosuchtype", "SELECT FROM bucket:nosuch", "SELECT FROM schema:nosuch",
        // the siblings that already answered it
        "REFRESH MATERIALIZED VIEW nosuchmv", "DROP BUCKET nosuchbucket"))
      assertThat(PostgresNetworkExecutor.sqlStateFor(failure("sql", statement))).as(statement).isEqualTo("42P01");
  }

  @Test
  void aMethodOrAProcedureNothingAnswersIsUndefinedFunction() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(failure("sql", "SELECT 'a'.nosuch()"))).isEqualTo("42883");
    assertThat(PostgresNetworkExecutor.sqlStateFor(failure("opencypher", "CALL db.nosuch()"))).isEqualTo("42883");
    // through the wrapper an engine puts around what it raises
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandExecutionException("Error", new UnknownProcedureException("db.x", "x"))))
        .isEqualTo("42883");
  }

  @Test
  void aSettingNoOneDeclaredIsInvalidParameterValue() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(failure("sql", "ALTER DATABASE `nosuchsetting` 1"))).isEqualTo("22023");
  }

  private Throwable failure(final String language, final String statement) {
    final Throwable error = catchThrowable(() -> {
      try (final ResultSet rs = database.command(language, statement)) {
        rs.stream().count();
      }
    });
    assertThat(error).as(statement).isNotNull();
    return error;
  }
}
