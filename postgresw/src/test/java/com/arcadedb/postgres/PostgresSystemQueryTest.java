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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Recognition of the emulated PostgreSQL system-information queries (issue #5290). Before this, each
 * function was matched by string equality against one exact spelling, so a client that aliased the call or
 * left a space before the semicolon fell through to ArcadeDB's own SQL engine.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresSystemQueryTest {

  @Test
  void bareCallUsesPostgresOwnColumnName() {
    final PostgresSystemQuery query = PostgresSystemQuery.parse("SELECT current_schema()");
    assertThat(query).isNotNull();
    assertThat(query.function).isEqualTo(PostgresSystemQuery.Function.CURRENT_SCHEMA);
    assertThat(query.columnName).isEqualTo("current_schema");
  }

  @Test
  void aliasBecomesTheColumnName() {
    // The query Beekeeper sends at connect time. "schema" is a reserved word in ArcadeDB SQL, so falling
    // through to the engine failed to parse and the tool never got a connection open.
    final PostgresSystemQuery query = PostgresSystemQuery.parse("SELECT CURRENT_SCHEMA() AS schema");
    assertThat(query).isNotNull();
    assertThat(query.function).isEqualTo(PostgresSystemQuery.Function.CURRENT_SCHEMA);
    assertThat(query.columnName).isEqualTo("schema");
  }

  @Test
  void unquotedAliasIsFoldedToLowerCaseAsPostgresFoldsIdentifiers() {
    assertThat(PostgresSystemQuery.parse("SELECT current_schema() AS MySchema").columnName).isEqualTo("myschema");
  }

  @Test
  void quotedAliasKeepsItsCase() {
    assertThat(PostgresSystemQuery.parse("SELECT current_schema() AS \"MySchema\"").columnName).isEqualTo("MySchema");
  }

  @Test
  void anExplicitlyEmptyQuotedAliasIsAColumnNamedEmpty() {
    // PostgreSQL allows AS "" and names the column "". That is not the same as supplying no alias at all,
    // which is what an emptiness check on the quoted group used to turn it into.
    assertThat(PostgresSystemQuery.parse("SELECT current_schema() AS \"\"").columnName).isEmpty();
  }

  @Test
  void aliasWithoutTheAsKeyword() {
    assertThat(PostgresSystemQuery.parse("SELECT version() v").columnName).isEqualTo("v");
  }

  @Test
  void versionAliasedStillMeansTheEmulatedVersion() {
    // ArcadeDB has a version() function of its own that answers the build string. Aliasing the call used to
    // be enough to reach it, so a client parsing the server version read one PostgreSQL never had.
    final PostgresSystemQuery query = PostgresSystemQuery.parse("SELECT version() AS v");
    assertThat(query).isNotNull();
    assertThat(query.function).isEqualTo(PostgresSystemQuery.Function.VERSION);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SELECT current_schema();", "SELECT current_schema() ;", "  SELECT current_schema()  ;  ",
      "SELECT current_schema() ;;" })
  void statementTerminatorAndSurroundingSpaceAreIgnored(final String query) {
    assertThat(PostgresSystemQuery.parse(query)).isNotNull();
    assertThat(PostgresSystemQuery.parse(query).function).isEqualTo(PostgresSystemQuery.Function.CURRENT_SCHEMA);
  }

  @ParameterizedTest
  @ValueSource(strings = { "SELECT pg_catalog.version()", "SELECT PG_CATALOG.VERSION()", "select pg_catalog . version ()" })
  void catalogQualifiedSpellingsAreTheSameFunction(final String query) {
    assertThat(PostgresSystemQuery.parse(query).function).isEqualTo(PostgresSystemQuery.Function.VERSION);
  }

  @Test
  void keywordFunctionsNeedNoParentheses() {
    assertThat(PostgresSystemQuery.parse("SELECT current_user").function).isEqualTo(PostgresSystemQuery.Function.CURRENT_USER);
    assertThat(PostgresSystemQuery.parse("SELECT session_user").function).isEqualTo(PostgresSystemQuery.Function.SESSION_USER);
    assertThat(PostgresSystemQuery.parse("SELECT current_catalog").function).isEqualTo(PostgresSystemQuery.Function.CURRENT_CATALOG);
  }

  @Test
  void everyFunctionCarriesPostgresDefaultOutputName() {
    for (final PostgresSystemQuery.Function function : PostgresSystemQuery.Function.values())
      assertThat(PostgresSystemQuery.parse("SELECT " + function.name()).columnName).isEqualTo(function.defaultColumnName);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // The word boundary after the function name is what keeps a column that merely starts with one of
      // these names from being read as the function plus an alias.
      "SELECT user_id",
      "SELECT versions",
      "SELECT current_schema_name",
      // Anything beyond the single expression is not a shape this class can answer.
      "SELECT version(), 1",
      "SELECT version FROM sometype",
      "SELECT current_schema() WHERE 1 = 1",
      "SELECT max(version) FROM t",
      "UPDATE t SET version = 1",
      "" })
  void queriesThatAreNotASingleSystemCallAreDeclined(final String query) {
    assertThat(PostgresSystemQuery.parse(query)).isNull();
  }

  @Test
  void nullIsDeclinedRatherThanThrown() {
    assertThat(PostgresSystemQuery.parse(null)).isNull();
  }
}
