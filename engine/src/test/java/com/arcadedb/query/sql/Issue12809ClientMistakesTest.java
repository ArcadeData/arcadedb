/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #12809: property-attribute values, index definitions, script statements and procedure arguments that the statement alone
 * decides are wrong are refused with a type every protocol answers as the caller's mistake (HTTP 400, a constraint the
 * stored records violate included), not as a server fault. A value that needs a record ({@code @rid}) evaluated where there is none is refused
 * with a message, not a {@code NullPointerException}.
 */
class Issue12809ClientMistakesTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.command("sql", "CREATE PROPERTY Q.k INTEGER");
    database.command("sql", "CREATE PROPERTY Q.s STRING");
    database.command("sql", "CREATE PROPERTY Q.m MAP");
    database.command("sql", "CREATE INDEX ON Q (s) NOTUNIQUE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Q SET k = 1, s = 'a'");
      database.command("sql", "INSERT INTO Q SET k = 2, s = null");
      database.command("sql", "INSERT INTO Q SET k = 3, s = 'c'");
    });
  }

  @Test
  void aFlagThatIsNotABooleanIsAnInvalidArgument() {
    for (final String flag : new String[] { "MANDATORY", "NOTNULL", "HIDDEN", "READONLY" })
      assertRefused("sql", "ALTER PROPERTY Q.k " + flag + " 'x'", InvalidArgumentException.class,
          flag + " takes true or false, not `x`");
    assertRefused("sql", "ALTER PROPERTY Q.k NOTNULL null", InvalidArgumentException.class, "NOTNULL takes true or false, not null");
  }

  @Test
  void anUnreadableBoundIsAnInvalidArgument() {
    assertRefused("sql", "ALTER PROPERTY Q.k MIN 1.5", InvalidArgumentException.class,
        "The MIN '1.5' of property 'Q.k' cannot be read as INTEGER");
    assertRefused("sql", "ALTER PROPERTY Q.k MIN true", InvalidArgumentException.class,
        "The MIN 'true' of property 'Q.k' cannot be read as INTEGER");
    assertRefused("sql", "ALTER PROPERTY Q.s MAX -1", InvalidArgumentException.class,
        "The MAX '-1' of property 'Q.s' cannot be a negative number of characters");
    assertRefused("sql", "ALTER PROPERTY Q.s MIN ''", InvalidArgumentException.class,
        "The MIN '' of property 'Q.s' cannot be read as a number of characters");
    assertRefused("sql", "ALTER PROPERTY Q.k MAX nine", InvalidArgumentException.class, "The value `nine` of MAX is a field reference");
  }

  @Test
  void anUnusableRegexpIsAnInvalidArgument() {
    assertRefused("sql", "ALTER PROPERTY Q.k REGEXP '[a-z]+'", InvalidArgumentException.class,
        "The REGEXP '[a-z]+' of property 'Q.k' matches no text a value of INTEGER can be written as");
    assertRefused("sql", "ALTER PROPERTY Q.s REGEXP '[a-'", InvalidArgumentException.class, "Invalid regular expression '[a-'");
  }

  @Test
  void anUnknownCreatePropertyAttributeIsRefusedAsTheAlterSiblingIs() {
    for (final String attribute : new String[] { "custom", "name", "type", "bogus" })
      assertRefused("sql", "CREATE PROPERTY Q.p1 STRING (" + attribute + " 'x')", CommandSQLParsingException.class,
          "Invalid attribute definition: '" + attribute + "'");
    assertThat(database.getSchema().getType("Q").existsProperty("p1")).isFalse();
  }

  @Test
  void aCreatePropertyAttributeRefusalIsNotRepeated() {
    final Throwable e = assertRefused("sql", "CREATE PROPERTY Q.p4 BYTE (mandatory -1)", InvalidArgumentException.class,
        "MANDATORY takes true or false, not `-1`");
    assertThat(e).hasMessage("MANDATORY takes true or false, not `-1`").hasNoCause();
    assertThat(database.getSchema().getType("Q").existsProperty("p4")).isFalse();
  }

  @Test
  void aColumnarDefinitionItCannotTakeIsAnIllegalArgument() {
    assertRefused("sql", "CREATE INDEX ON Q (m BY KEY) COLUMNAR", IllegalArgumentException.class,
        "A COLUMNAR index does not support BY KEY or BY VALUE, got 'm by key'");
    assertRefused("sql", "CREATE INDEX ON Q (s, k) COLUMNAR", IllegalArgumentException.class,
        "A COLUMNAR index is defined on exactly one property (one index per property), got [s, k]");
  }

  @Test
  void aFullTextOrGeospatialIndexOnANonStringPropertyIsAnIllegalArgument() {
    assertRefused("sql", "CREATE INDEX ON Q (k) FULL_TEXT", IllegalArgumentException.class,
        "Full text index can only be defined on STRING properties, found: INTEGER");
    assertRefused("sql", "CREATE INDEX ON Q (k) GEOSPATIAL", IllegalArgumentException.class,
        "Geospatial index can only be defined on STRING properties, found: INTEGER");
    assertThat(database.getSchema().getType("Q").getAllIndexes(false)).hasSize(1);
  }

  @Test
  void aCommitRetryOfZeroIsRefusedAsAScriptMistake() {
    assertRefused("sqlscript", "BEGIN; INSERT INTO Q SET k = 1; COMMIT RETRY 0;", CommandSQLParsingException.class,
        "Invalid retry number 0");
  }

  @Test
  void aValueThatNeedsARecordIsRefusedAsNotAConstant() {
    final String[][] statements = {
        { "sqlscript", "CONSOLE.log @rid; RETURN 1;" },
        { "sql", "ALTER PROPERTY Q.k mandatory @rid" },
        { "sql", "ALTER PROPERTY Q.k max @rid" },
        { "sql", "ALTER PROPERTY Q.k min @rid" },
        { "sql", "ALTER PROPERTY Q.k readonly @rid" },
        { "sql", "ALTER PROPERTY Q.k hidden @rid" },
        { "sql", "ALTER PROPERTY Q.k notnull @rid" },
        { "sql", "ALTER PROPERTY Q.k regexp @rid" },
        { "sql", "ALTER PROPERTY Q.k CUSTOM x = @rid" },
        { "sql", "CREATE PROPERTY Q.p1 LONG (max @rid)" },
        { "sql", "ALTER TYPE Q CUSTOM x = @rid" } };
    for (final String[] statement : statements)
      assertRefused(statement[0], statement[1], CommandSemanticException.class, "the value must be a constant");
    assertThat(database.getSchema().getType("Q").existsProperty("p1")).isFalse();
  }

  @Test
  void aNullDateFormatIsAnIllegalArgument() {
    assertRefused("sql", "ALTER DATABASE `arcadedb.dateFormat` null", IllegalArgumentException.class, "arcadedb.dateFormat");
    assertRefused("sql", "ALTER DATABASE `arcadedb.dateTimeFormat` null", IllegalArgumentException.class, "arcadedb.dateTimeFormat");
    assertThat(database.getSchema().getDateFormat()).isNotNull();
    assertThat(database.getSchema().getDateTimeFormat()).isNotNull();
  }

  @Test
  void aConstraintTheStoredRecordsViolateIsAValidationRefusalWithItsKind() {
    // the SQL spellings name the kind of the constraint the record violates (issue #12436)
    assertViolates("sql", "CREATE PROPERTY Q.p2 BOOLEAN (MANDATORY true)", ValidationException.Kind.MANDATORY,
        "Cannot create property 'Q.p2': record #");
    assertThat(database.getSchema().getType("Q").existsProperty("p2")).isFalse();
    assertViolates("sql", "ALTER PROPERTY Q.s notnull true", ValidationException.Kind.NOT_NULL, "Cannot set NOTNULL: record #");
    // m is set on no record
    assertViolates("sql", "ALTER PROPERTY Q.m mandatory true", ValidationException.Kind.MANDATORY, "Cannot set MANDATORY: record #");
    assertThat(database.getSchema().getType("Q").getProperty("s").isNotNull()).isFalse();
    // the openCypher spelling of the same refusal: a type constraint that a stored value of an undeclared property does not satisfy
    // was a plain CommandExecutionException, a server fault to every protocol
    database.transaction(() -> database.command("sql", "UPDATE Q SET loose = 'x' WHERE k = 1"));
    assertViolates("opencypher", "CREATE CONSTRAINT q_loose_int FOR (n:Q) REQUIRE n.loose IS :: INTEGER", ValidationException.Kind.TYPE,
        "Cannot require Q.loose IS :: INTEGER: record #");
    assertThat(database.getSchema().getType("Q").existsProperty("loose")).isFalse();
  }

  @Test
  void aFullTextProcedureOverAnotherIndexIsAnInvalidArgument() {
    assertRefusedInProcedure("CALL db.index.fulltext.queryNodes('Q[s]', 'abc')", "Index 'Q[s]' is not a full-text index");
  }

  @Test
  void aProcedureRefusesANullArgumentByName() {
    assertRefusedInProcedure("CALL db.index.fulltext.queryNodes('Q[s]', null)",
        "db.index.fulltext.queryNodes(): queryString cannot be null");
    assertRefusedInProcedure("CALL db.index.fulltext.queryNodes(null, 'x')", "db.index.fulltext.queryNodes(): indexName cannot be null");
    assertRefusedInProcedure("CALL db.index.fulltext.queryRelationships(null, 'x')",
        "db.index.fulltext.queryRelationships(): indexName cannot be null");
    assertRefusedInProcedure("CALL db.index.fulltext.queryRelationships('Q[s]', null)",
        "db.index.fulltext.queryRelationships(): queryString cannot be null");
    assertRefusedInProcedure("CALL db.index.vector.queryNodes(null, 3, [1.0])", "db.index.vector.queryNodes(): indexName cannot be null");
  }

  /** The counter-case: the same statements written as they are meant to be still run. */
  @Test
  void theWellFormedStatementsStillRun() {
    database.command("sql", "ALTER PROPERTY Q.k MANDATORY true");
    database.command("sql", "ALTER PROPERTY Q.k MIN 0");
    database.command("sql", "ALTER PROPERTY Q.s REGEXP '[a-z]+'");
    database.command("sql", "CREATE PROPERTY Q.p4 BYTE (mandatory false)");
    database.command("sql", "ALTER TYPE Q CUSTOM x = 'y'");
    database.command("sqlscript", "LET $x = 1; CONSOLE.log $x; BEGIN; INSERT INTO Q SET k = 4, s = 'd'; COMMIT RETRY 1; RETURN 1;").close();
    assertThat(database.getSchema().getType("Q").getProperty("k").isMandatory()).isTrue();
    assertThat(database.getSchema().getType("Q").getCustomValue("x")).isEqualTo("y");
  }

  private Throwable assertRefused(final String language, final String statement, final Class<? extends Throwable> expectedType,
      final String expectedMessage) {
    final Throwable e = catchThrowable(() -> database.command(language, statement).close());
    assertThat(e).as(statement).isInstanceOf(expectedType).hasMessageContaining(expectedMessage);
    assertThat(ErrorCategory.of(e)).as(statement).isNotEqualTo(ErrorCategory.SERVER);
    return e;
  }

  /** The refusal, wherever it sits in the chain (openCypher wraps it), is a ValidationException of that kind. */
  private void assertViolates(final String language, final String statement, final ValidationException.Kind expectedKind,
      final String expectedMessage) {
    final Throwable e = catchThrowable(() -> database.command(language, statement).close());
    assertThat(e).as(statement).isNotNull();
    assertThat(ErrorCategory.of(e)).as(statement).isEqualTo(ErrorCategory.VALIDATION);
    assertThat(ErrorCategory.classifiedCause(e)).as(statement).isInstanceOf(ValidationException.class)
        .hasMessageContaining(expectedMessage);
    assertThat(((ValidationException) ErrorCategory.classifiedCause(e)).getKind()).as(statement).isEqualTo(expectedKind);
  }

  private void assertRefusedInProcedure(final String statement, final String expectedMessage) {
    assertThatThrownBy(() -> database.command("opencypher", statement).close()).as(statement)
        .hasCauseInstanceOf(InvalidArgumentException.class).cause().hasMessage(expectedMessage);
  }
}
