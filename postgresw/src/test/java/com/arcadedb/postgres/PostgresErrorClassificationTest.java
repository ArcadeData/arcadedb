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
import com.arcadedb.database.RID;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.EquivalentIndexException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.UnknownProcedureException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.exception.ValueOutOfRangeException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * SQLSTATE mapping for the Postgres wire protocol (issue #5628). Before this, every failure that was not a parsing
 * error reported as {@code XX000} internal_error, so a caller's own mistake - a division by zero, a duplicate key -
 * looked to their driver like the server had broken, and a retryable conflict carried a code no driver retries.
 */
class PostgresErrorClassificationTest {

  @Test
  void aDivisionByZeroIsADataExceptionNotAServerFault() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("/ by zero"))).isEqualTo("22012");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("% by zero"))).isEqualTo("22012");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("Cannot divide duration by zero")))
        .isEqualTo("22012");
  }

  @Test
  void anOverflowIsOutOfRange() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("long overflow"))).isEqualTo("22003");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("duration overflow"))).isEqualTo("22003");
  }

  @Test
  void anUnrecognisedArithmeticMessageStaysInTheDataExceptionClass() {
    // The split between 22012 and 22003 is a nicety; what must not drift is the class-22 verdict that tells the
    // driver this was the caller's data, not a server fault.
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException(null))).startsWith("22");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ArithmeticErrorException("some future arithmetic failure")))
        .startsWith("22");
  }

  @Test
  void aWrappedArithmeticErrorIsStillADataException() {
    // A statement reaching Postgres through the auto-commit path arrives wrapped; inspecting only the outermost
    // throwable is what reported these as XX000.
    final Throwable wrapped = new TransactionException("Error on transaction commit",
        new ArithmeticErrorException("/ by zero"));

    assertThat(PostgresNetworkExecutor.sqlStateFor(wrapped)).isEqualTo("22012");
  }

  @Test
  void aConflictGetsTheCodeDriversRetryOn() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ConcurrentModificationException("page version changed")))
        .isEqualTo("40001");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new LockTimeoutException("lock timeout"))).isEqualTo("40001");
  }

  @Test
  void aConflictWinsOverAnArithmeticErrorInTheSameChain() {
    final Throwable both = new LockTimeoutException("lock timeout", new ArithmeticErrorException("long overflow"));

    assertThat(PostgresNetworkExecutor.sqlStateFor(both)).isEqualTo("40001");
  }

  @Test
  void theRemainingCategoriesGetTheirPostgresCodes() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new DuplicatedKeyException("idx", "k", new RID(1, 1))))
        .isEqualTo("23505");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new RecordNotFoundException("gone", new RID(1, 1)))).isEqualTo("P0002");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SchemaException("Type with name 'Nope' was not found"))).isEqualTo("42P01");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SecurityException("denied"))).isEqualTo("42501");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ValidationException("mandatory property"))).isEqualTo("22023");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new QueryNotIdempotentException("writes on a query"))).isEqualTo("22023");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("bad syntax"))).isEqualTo("42601");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new TimeoutException("too slow"))).isEqualTo("57014");
  }

  @Test
  void aParsingWrapperDoesNotHideTheRealFailure() {
    // Both execution arms used to hardcode 42601 for CommandParsingException, which made the
    // ARITHMETIC-before-PARSING ordering in ErrorCategory dead on this path: a query engine that wraps an
    // execution failure as a parsing exception - as GraphQL did until this change - would have reported a
    // division by zero as a syntax error. Routing the arm through sqlStateFor keeps genuine parse errors at
    // 42601 while letting the real cause win.
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandParsingException("Error on executing query", new ArithmeticErrorException("/ by zero"))))
        .isEqualTo("22012");
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandParsingException("Error on executing query", new ConcurrentModificationException("conflict"))))
        .isEqualTo("40001");
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandParsingException("Error on executing query", new SchemaException("Type 'Nope' was not found"))))
        .isEqualTo("42P01");

    // A parse error that is genuinely just a parse error keeps the code it always had.
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("unexpected token"))).isEqualTo("42601");
  }

  @Test
  void anUnknownTypeIsNotAServerFault() {
    // SELECT FROM NonExistentType used to report XX000 internal_error, telling the driver the server had broken.
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SchemaException("Type with name 'DoesNotExist' was not found")))
        .isEqualTo("42P01")
        .isNotEqualTo("XX000");
  }

  @Test
  void agenuineServerFaultIsStillInternalError() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IOException("disk gone"))).isEqualTo("XX000");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandExecutionException("something broke"))).isEqualTo("XX000");
    assertThat(PostgresNetworkExecutor.sqlStateFor(null)).isEqualTo("XX000");
  }

  /*
   * Issue #10153: a {@code CREATE} on a taken name reached a Postgres client as {@code XX000} internal_error, or as {@code 42P01}
   * undefined_table - the opposite of the refusal - for the ones raised as a {@code SchemaException}. It is the SQLSTATE Postgres
   * answers the same statement with now: {@code 42P07} for a relation, {@code 42701} for a column, {@code 42710} for anything else.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #10153
  @Test
  void aTakenRelationNameIsDuplicateTable() {
    for (final AlreadyExistsException.Kind kind : new AlreadyExistsException.Kind[] { AlreadyExistsException.Kind.TYPE,
        AlreadyExistsException.Kind.INDEX, AlreadyExistsException.Kind.MATERIALIZED_VIEW,
        AlreadyExistsException.Kind.CONTINUOUS_AGGREGATE })
      assertThat(PostgresNetworkExecutor.sqlStateFor(taken(kind))).as("%s", kind).isEqualTo("42P07");
  }

  // Regression: #10153
  @Test
  void aTakenPropertyNameIsDuplicateColumn() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(taken(AlreadyExistsException.Kind.PROPERTY))).isEqualTo("42701");
  }

  // Regression: #10153
  @Test
  void anyOtherTakenNameIsDuplicateObject() {
    for (final AlreadyExistsException.Kind kind : new AlreadyExistsException.Kind[] { AlreadyExistsException.Kind.CONSTRAINT,
        AlreadyExistsException.Kind.BUCKET, AlreadyExistsException.Kind.TRIGGER, AlreadyExistsException.Kind.FUNCTION,
        AlreadyExistsException.Kind.FUNCTION_LIBRARY, AlreadyExistsException.Kind.GRAPH_ANALYTICAL_VIEW, null })
      assertThat(PostgresNetworkExecutor.sqlStateFor(taken(kind))).as("%s", kind).isEqualTo("42710");
  }

  // Regression: #10153
  @Test
  void theCodeIsTheSameThroughAWrapperAndAMissingTypeKeepsItsOwn() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandExecutionException("Error on executing the statement", taken(AlreadyExistsException.Kind.TYPE)))).isEqualTo("42P07");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new SchemaException("Type 'X' was not found"))).isEqualTo("42P01");
  }

  private static AlreadyExistsException taken(final AlreadyExistsException.Kind kind) {
    return new AlreadyExistsException(kind, "taken");
  }

  /*
   * Issue #11314: every refused value reached a Postgres client as {@code 22023} invalid_parameter_value and a call of an unknown
   * SQL function as {@code XX000} internal_error. They are the SQLSTATEs PostgreSQL answers the same refusal with now.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final Map<ValidationException.Kind, String> EXPECTED = new EnumMap<>(Map.of(//
      ValidationException.Kind.MANDATORY, "23502", //
      ValidationException.Kind.NOT_NULL, "23502", //
      ValidationException.Kind.MIN, "23514", //
      ValidationException.Kind.MAX, "23514", //
      ValidationException.Kind.REGEXP, "23514", //
      ValidationException.Kind.READONLY, "23514", //
      ValidationException.Kind.MAX_LENGTH, "22001", //
      ValidationException.Kind.TYPE, "42804", //
      ValidationException.Kind.ENCODING, "22021"));

  // Regression: #11314
  @Test
  void everyConstraintKindHasThePostgresCodeOfTheSameViolation() {
    // ranges over the enum, so a kind added later without a code here is a red build rather than a silent 22023
    for (final ValidationException.Kind kind : ValidationException.Kind.values()) {
      assertThat(EXPECTED).as("no SQLSTATE expected for %s", kind).containsKey(kind);
      assertThat(PostgresNetworkExecutor.sqlStateFor(new ValidationException(kind, "refused"))).as("%s", kind).isEqualTo(EXPECTED.get(kind));
      // the same through the wrapper an engine puts around what it raises
      assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandExecutionException("Error", new ValidationException(kind, "refused"))))
          .as("%s wrapped", kind).isEqualTo(EXPECTED.get(kind));
    }
  }

  // Regression: #11314
  @Test
  void aRefusedConversionHasTheCodeOfWhatItFailed() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Cannot convert",
        new ValueOutOfRangeException("Value '99999999999' is out of range for type INTEGER")))).isEqualTo("22003");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Cannot convert",
        new NumberFormatException("For input string: \"abc\"")))).isEqualTo("22P02");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Cannot convert",
        new IllegalArgumentException("Error in conversion", new DateTimeParseException("Text 'x' could not be parsed", "x", 0)))))
        .isEqualTo("22007");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new TypeMismatchException("Cannot read a property of a list"))).isEqualTo("42804");
  }

  // Regression: #11314
  @Test
  void aRefusalThatIsNoDeclaredConstraintKeepsInvalidParameterValue() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ValidationException("refused"))).isEqualTo("22023");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("bad argument"))).isEqualTo("22023");
  }

  // Regression: #11314
  @Test
  void anUnknownFunctionIsUndefinedFunctionAndAnyOtherParseFailureASyntaxError() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new UnknownFunctionException("foo", "Unknown function name 'foo'"))).isEqualTo("42883");
    // openCypher's parse-time refusal carries the same cause
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("UnknownFunction: Unknown function 'foo'",
        new UnknownFunctionException("foo", "UnknownFunction: Unknown function 'foo'")))).isEqualTo("42883");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("syntax error"))).isEqualTo("42601");
  }

  /*
   * Issue #11392: an index equivalent to one the type already has under another name reached a Postgres client as {@code 22023}
   * invalid_parameter_value, where the same-name duplicate answers {@code 42P07} (#10153). An index is a relation, so the
   * equivalent one is {@code 42P07} too; another definition on the same properties stays the argument error it is.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #11392
  @Test
  void anEquivalentIndexIsDuplicateTable() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new EquivalentIndexException("equivalent"))).isEqualTo("42P07");
    assertThat(PostgresNetworkExecutor.sqlStateFor(
        new CommandExecutionException("Error on executing the statement", new EquivalentIndexException("equivalent"))))
        .isEqualTo("42P07");
  }

  // Regression: #11392
  @Test
  void anotherDefinitionStaysAnInvalidParameter() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Drop it first if the definition has to change")))
        .isEqualTo("22023");
  }

  /**
   * Issue #11384: a statement naming an index, a trigger, a view, a bucket, a property or a type that does not exist answered
   * {@code XX000} internal_error, as did a method or a procedure nothing answers, while the sibling statements that reach the same name
   * through the schema answered {@code 42P01}. They answer the code of the missing object now.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MissingObjectSqlState extends TestHelper {
    // Regression: #11384
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

    // Regression: #11384
    @Test
    void aMethodOrAProcedureNothingAnswersIsUndefinedFunction() {
      assertThat(PostgresNetworkExecutor.sqlStateFor(failure("sql", "SELECT 'a'.nosuch()"))).isEqualTo("42883");
      assertThat(PostgresNetworkExecutor.sqlStateFor(failure("opencypher", "CALL db.nosuch()"))).isEqualTo("42883");
      // through the wrapper an engine puts around what it raises
      assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandExecutionException("Error", new UnknownProcedureException("db.x", "x"))))
          .isEqualTo("42883");
    }

    // Regression: #11384
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

  /**
   * Issue #11803: a statement refused for what the database holds now - a TRUNCATE or DROP of a type that still has records, a DROP
   * of an indexed property - answered {@code XX000} internal_error. It answers {@code 55006} object_in_use, and the other caller's
   * mistakes of the issue answer a class-22 or class-42 code instead of the server fault.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ObjectInUseSqlState extends TestHelper {
    // Regression: #11803
    @Test
    void aStatementRefusedForWhatTheDatabaseHoldsIsObjectInUse() {
      database.command("sql", "CREATE VERTEX TYPE V11803");
      database.command("sql", "CREATE PROPERTY V11803.id INTEGER");
      database.command("sql", "CREATE INDEX ON V11803 (id) UNIQUE");
      database.transaction(() -> database.command("sql", "INSERT INTO V11803 SET id = 1"));

      for (final String statement : List.of("TRUNCATE TYPE V11803", "DROP TYPE V11803", "DROP PROPERTY V11803.id"))
        assertThat(PostgresNetworkExecutor.sqlStateFor(failure(statement))).as(statement).isEqualTo("55006");

      assertThat(PostgresNetworkExecutor.sqlStateFor(failure("SELECT FROM V11803 LIMIT 'a'"))).isEqualTo("42804");
      assertThat(PostgresNetworkExecutor.sqlStateFor(failure("ALTER PROPERTY V11803.id TYPE LONG"))).isEqualTo("42601");
      assertThat(PostgresNetworkExecutor.sqlStateFor(failure("REBUILD INDEX nosuch11803"))).isEqualTo("42P01");
      assertThat(PostgresNetworkExecutor.sqlStateFor(failure("CREATE EDGE V11803 FROM (SELECT FROM V11803) TO (SELECT FROM V11803)")))
          .isEqualTo("42P01");
    }

    private Throwable failure(final String statement) {
      final Throwable error = catchThrowable(() -> {
        try (final ResultSet rs = database.command("sql", statement)) {
          rs.stream().count();
        }
      });
      assertThat(error).as(statement).isNotNull();
      return error;
    }
  }
}
