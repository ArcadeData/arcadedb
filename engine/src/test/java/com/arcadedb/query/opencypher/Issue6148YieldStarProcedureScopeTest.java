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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6148: {@code CALL proc() YIELD *} never put any of the procedure's yielded output names into scope, so a
 * named reference to one of them - or a following {@code RETURN *} - failed even though the executor projects the
 * fields correctly once the query is allowed to run at all.
 * <p>
 * Root cause: {@code CypherSemanticValidator}'s {@code buildVarTypes}, {@code validateVariableScope} and
 * {@code statementDeclaresAnyVariable} (added for issue #6135 / PR #6138) all iterated {@code callClause.getYieldItems()}
 * to learn what a {@code CALL ... YIELD} exports, but that list is deliberately empty for {@code YIELD *}
 * ({@code isYieldAll()} is the only signal). Neo4j (the openCypher reference implementation) treats {@code YIELD *}
 * as an ordinary {@code YIELD} of every field the procedure declares.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6148YieldStarProcedureScopeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:P {name: 'a'})-[:KNOWS]->(:P {name: 'b'})");
    // db.propertyKeys() reads the schema's declared properties, not the schemaless documents themselves
    database.command("sql", "CREATE PROPERTY P.name STRING");
  }

  @Test
  void yieldStarBindsANamedBuiltInProcedureOutput() {
    try (final ResultSet rs = database.query("opencypher", "CALL db.labels() YIELD * RETURN label")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.getPropertyNames()).containsExactly("label");
      assertThat(row.<String>getProperty("label")).isEqualTo("P");
    }
  }

  @Test
  void yieldStarLetsReturnStarSeeTheBuiltInProcedureOutput() {
    try (final ResultSet rs = database.query("opencypher", "CALL db.labels() YIELD * RETURN *")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().getPropertyNames()).containsExactly("label");
    }
  }

  @Test
  void yieldStarBindsDbRelationshipTypesOutput() {
    try (final ResultSet rs = database.query("opencypher", "CALL db.relationshiptypes() YIELD * RETURN relationshipType")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("relationshipType")).isEqualTo("KNOWS");
    }
  }

  @Test
  void yieldStarBindsDbPropertyKeysOutput() {
    try (final ResultSet rs = database.query("opencypher", "CALL db.propertykeys() YIELD * RETURN propertyKey")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  @Test
  void yieldStarBindsARegisteredProcedureOutput() {
    try (final ResultSet rs = database.query("opencypher", "CALL meta.stats() YIELD * RETURN value")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Object>getProperty("value")).isNotNull();
    }
  }

  @Test
  void yieldStarInsideACallSubqueryStillBindsTheOutput() {
    try (final ResultSet rs = database.query("opencypher", "CALL { CALL db.labels() YIELD * RETURN label } RETURN label")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("label")).isEqualTo("P");
    }
  }

  @Test
  void yieldStarBindsDbSchemaOutputUnderEitherRegisteredName() {
    try (final ResultSet rs = database.query("opencypher", "CALL db.schema() YIELD * RETURN name, type, properties")) {
      assertThat(rs.hasNext()).isTrue();
    }
    try (final ResultSet rs = database.query("opencypher", "CALL db.schema.visualization() YIELD * RETURN name, type, properties")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /**
   * Routing the hardcoded {@code db.*} built-ins through the normal {@code CypherProcedure} dispatch (issue #6148's
   * fix) means they now go through {@code CypherProcedure#validateArgs} like every other procedure - previously the
   * hardcoded {@code switch} in {@code CallStep} ignored {@code args} entirely, so {@code CALL db.labels(1)} silently
   * succeeded. Pinning this down so a future refactor cannot silently reintroduce the old, more permissive behavior.
   */
  @Test
  void dbLabelsNowRejectsAnUnexpectedArgument() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", "CALL db.labels(1) YIELD label RETURN label")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(CommandSemanticException.class);
  }

  /**
   * A custom {@code DEFINE FUNCTION} has no statically declared output signature - unlike a registered
   * {@link com.arcadedb.query.opencypher.procedures.CypherProcedure}, {@code CypherProcedureRegistry} does not know
   * it at all, so {@code resolveYieldAllFieldNames} returns {@code null} for it. This pins down that an unresolvable
   * {@code YIELD *} keeps exactly today's (pre-fix) behavior instead of the fix guessing at a shape it cannot know.
   */
  @Test
  void yieldStarOnAnUnresolvableProcedureNameKeepsPriorBehavior() {
    database.command("sql", "DEFINE FUNCTION math.double \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");

    assertThatThrownBy(() -> database.query("opencypher", "CALL math.double(4) YIELD * RETURN value").close())
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable");

    assertThatThrownBy(() -> database.query("opencypher", "CALL math.double(4) YIELD * RETURN *").close())
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("NoVariablesInScope");
  }
}
