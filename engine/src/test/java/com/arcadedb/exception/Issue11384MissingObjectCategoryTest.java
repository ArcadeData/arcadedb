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
package com.arcadedb.exception;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11384: about sixteen SQL and Cypher statements raised a plain {@link CommandExecutionException} for a named object that
 * does not exist, which no {@link ErrorCategory} rule classifies, so the Postgres wire answered {@code XX000} internal_error and
 * Bolt {@code UnknownError} for what their sibling statements, which go through the schema, answered as the caller's mistake.
 * Every statement of the report, and of its "read only, not reached" list where a statement reaches it, must now be classified
 * as what it is: a missing schema object, a method or a procedure nothing answers, a setting no one declared.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11384MissingObjectCategoryTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("VV");
    database.getSchema().createDocumentType("DD");
  }

  @Test
  void everyMissingSchemaObjectIsASchemaFailure() {
    final List<String> statements = List.of(//
        "DROP INDEX nosuchindex",//
        "REBUILD INDEX nosuchindex",//
        "DROP TRIGGER nosuchtrg",//
        "DROP MATERIALIZED VIEW nosuchmv",//
        "REFRESH MATERIALIZED VIEW nosuchmv",// the sibling that already answered SCHEMA
        "DROP CONTINUOUS AGGREGATE nosuchca",//
        "DROP GRAPH ANALYTICAL VIEW nosuchgav",//
        "REBUILD GRAPH ANALYTICAL VIEW nosuchgav",//
        "ALTER GRAPH ANALYTICAL VIEW nosuchgav UPDATE MODE OFF",//
        "TRUNCATE BUCKET nosuchbucket",//
        "DROP BUCKET nosuchbucket",// the sibling that already answered SCHEMA
        "DROP PROPERTY VV.nosuch",//
        "ALTER PROPERTY VV.nosuch NAME x",// the sibling that already answered SCHEMA
        "CREATE PROPERTY nosuchtype.p STRING",//
        "CREATE VERTEX TYPE VX EXTENDS nosuchtype",//
        "CREATE TRIGGER t11384 AFTER CREATE ON TYPE nosuchtype EXECUTE SQL \"SELECT 1\"",//
        "DROP TYPE nosuchtype",//
        "ALTER TYPE nosuchtype CUSTOM a = 1",//
        "ALTER TYPE VV SUPERTYPE +nosuchtype",//
        "REBUILD TYPE nosuchtype",//
        "TRUNCATE TYPE nosuchtype",//
        "SELECT FROM bucket:nosuch",//
        "SELECT FROM bucket:[nosuch]",//
        "SELECT FROM schema:nosuch",//
        "SELECT FROM index:nosuch",//
        "DELETE FROM index:nosuch",//
        "SELECT FROM nosuchtype",//
        "SELECT count(*) FROM nosuchtype",//
        "MATCH {type: nosuchtype, as: n} RETURN n",//
        "INSERT INTO bucket:nosuch SET a = 1");

    final List<String> wrong = new ArrayList<>();
    for (final String statement : statements) {
      final Throwable error = run("sql", statement);
      if (ErrorCategory.of(error) != ErrorCategory.SCHEMA)
        wrong.add(statement + " -> " + ErrorCategory.of(error) + " (" + describe(error) + ")");
    }
    assertThat(wrong).as("a missing schema object is the caller's mistake, never a server fault").isEmpty();
  }

  @Test
  void aMethodNothingAnswersIsAnUnknownFunction() {
    database.transaction(() -> database.newDocument("DD").set("name", "x").save());
    for (final String statement : List.of("SELECT 'a'.nosuch()", "SELECT 'a'.nosuchmethod(1)", "SELECT FROM DD WHERE name.nosuch() = 1")) {
      final Throwable error = run("sql", statement);
      assertThat(ErrorCategory.of(error)).as(statement + ": " + describe(error)).isEqualTo(ErrorCategory.PARSING);
      assertThat(CauseChain.find(error, UnknownFunctionException.class)).as(statement).isNotNull();
    }
  }

  @Test
  void aProcedureNothingAnswersIsAnUnknownProcedureOptionalOrNot() {
    for (final String statement : List.of("CALL db.nosuch()", "CALL nosuch.proc(1) YIELD x RETURN x", "OPTIONAL CALL db.nosuch() RETURN 1")) {
      final Throwable error = run("opencypher", statement);
      assertThat(ErrorCategory.of(error)).as(statement + ": " + describe(error)).isEqualTo(ErrorCategory.PARSING);
      assertThat(CauseChain.find(error, UnknownProcedureException.class)).as(statement + ": " + describe(error)).isNotNull();
    }
  }

  @Test
  void aCustomFunctionAnsweringNullIsCalledNotRefusedAsUnknown() {
    database.command("sql", "DEFINE FUNCTION lib11384.nothing 'SELECT null AS result' LANGUAGE sql");
    // a CALL of a function that exists runs it, whatever it answers: a null answer used to fall through to the SQL functions
    try (final ResultSet rs = database.command("opencypher", "CALL lib11384.nothing()")) {
      rs.stream().count();
    }
  }

  @Test
  void aSettingNoOneDeclaredIsAValidationFailure() {
    for (final String statement : List.of("ALTER DATABASE `nosuchsetting` 1", "ALTER DATABASE `arcadedb.server.name` 'x'")) {
      final Throwable error = run("sql", statement);
      assertThat(ErrorCategory.of(error)).as(statement + ": " + describe(error)).isEqualTo(ErrorCategory.VALIDATION);
    }
  }

  private Throwable run(final String language, final String statement) {
    final Throwable error = catchThrowable(() -> {
      try (final ResultSet rs = database.command(language, statement)) {
        rs.stream().count();
      }
    });
    assertThat(error).as(statement + " must be refused").isNotNull();
    return error;
  }

  private static String describe(final Throwable error) {
    final StringBuilder text = new StringBuilder();
    for (Throwable t = error; t != null && text.length() < 400; t = t.getCause() == t ? null : t.getCause())
      text.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" <- ");
    return text.toString();
  }
}
