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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.time.DateTimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #11314: every refusal of a declared property constraint was the same bare {@link ValidationException}, and a misspelled SQL
 * function a plain {@link CommandExecutionException}, which no {@link ErrorCategory} rule classifies, so the Postgres wire
 * answered {@code 22023} for a NOT NULL violation and a value over a maximum length alike, and {@code XX000} (a server fault)
 * for a call of a function that does not exist. The refusal now carries the constraint it violates
 * ({@link ValidationException#getKind()}), a number the target type cannot hold is a {@link ValueOutOfRangeException}, and an
 * unknown function an {@link UnknownFunctionException} classified {@link ErrorCategory#PARSING}, in SQL and openCypher alike.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314ConstraintKindTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE DOCUMENT TYPE Ty");
    database.command("sql", "CREATE PROPERTY Ty.i INTEGER (mandatory true, min 1, max 10)");
    database.command("sql", "CREATE PROPERTY Ty.nn STRING (notnull true)");
    database.command("sql", "CREATE PROPERTY Ty.rx STRING (regexp '[a-z]+')");
    database.command("sql", "CREATE PROPERTY Ty.mx STRING (max 3)");
    database.command("sql", "CREATE PROPERTY Ty.mn STRING (min 2)");
    database.command("sql", "CREATE PROPERTY Ty.bn BINARY (max 2)");
    database.command("sql", "CREATE PROPERTY Ty.ls LIST (max 2)");
    database.command("sql", "CREATE PROPERTY Ty.ro STRING (readonly true)");
    database.command("sql", "CREATE PROPERTY Ty.d DATE");
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    database.command("sql", "CREATE PROPERTY Ty.lk LINK OF Ty");
  }

  @Test
  void everyDeclaredConstraintNamesItsKind() {
    assertKind(() -> insert("i = 1, nn = null"), ValidationException.Kind.NOT_NULL);
    assertKind(() -> insert("nn = 'a'"), ValidationException.Kind.MANDATORY);
    assertKind(() -> insert("i = 11, nn = 'a'"), ValidationException.Kind.MAX);
    assertKind(() -> insert("i = 0, nn = 'a'"), ValidationException.Kind.MIN);
    assertKind(() -> insert("i = 1, nn = 'a', rx = 'ABC'"), ValidationException.Kind.REGEXP);
    assertKind(() -> insert("i = 1, nn = 'a', mx = 'abcdef'"), ValidationException.Kind.MAX_LENGTH);
    // a length under the MIN is a rule on the value like any other bound, not the truncation a varchar(n) refuses
    assertKind(() -> insert("i = 1, nn = 'a', mn = 'a'"), ValidationException.Kind.MIN);
    // the items of a collection are counted, not truncated: a bound like a number's
    assertKind(() -> insert("i = 1, nn = 'a', ls = [1, 2, 3]"), ValidationException.Kind.MAX);

    assertKind(() -> database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Ty").set("i", 1).set("nn", "a").set("bn", new byte[] { 1, 2, 3 });
      doc.save();
    }), ValidationException.Kind.MAX_LENGTH);

    assertKind(() -> database.transaction(() -> {
      final MutableDocument other = database.newDocument("Other");
      other.save();
      database.newDocument("Ty").set("i", 1).set("nn", "a").set("lk", other.getIdentity()).save();
    }), ValidationException.Kind.TYPE);
  }

  @Test
  void aChangeToAReadonlyPropertyIsAReadonlyViolation() {
    database.transaction(() -> insert("i = 1, nn = 'a', ro = 'first'"));
    assertKind(() -> database.transaction(() -> database.command("sql", "UPDATE Ty SET ro = 'second'")), ValidationException.Kind.READONLY);
  }

  @Test
  void aNumberTheTypeCannotHoldIsOutOfRangeAndTextThatIsNoNumberIsNot() {
    final Throwable overflow = catchThrowable(() -> database.transaction(() -> insert("i = 99999999999, nn = 'a'")));
    assertThat(CauseChain.find(overflow, ValueOutOfRangeException.class)).as("got %s", overflow).isNotNull();
    assertThat(ErrorCategory.of(overflow)).isEqualTo(ErrorCategory.VALIDATION);

    final Throwable notANumber = catchThrowable(() -> database.transaction(() -> insert("i = 'abc', nn = 'a'")));
    assertThat(CauseChain.find(notANumber, ValueOutOfRangeException.class)).as("got %s", notANumber).isNull();
    assertThat(CauseChain.find(notANumber, NumberFormatException.class)).as("got %s", notANumber).isNotNull();
    assertThat(ErrorCategory.of(notANumber)).isEqualTo(ErrorCategory.VALIDATION);

    final Throwable notADate = catchThrowable(() -> database.transaction(() -> insert("i = 1, nn = 'a', d = 'notadate'")));
    assertThat(CauseChain.find(notADate, DateTimeException.class)).as("got %s", notADate).isNotNull();
    assertThat(ErrorCategory.of(notADate)).isEqualTo(ErrorCategory.VALIDATION);
  }

  @Test
  void aNumberOutOfRangeIsStillAnIllegalArgumentAndIsKeptByTheLenientConversion() {
    final DocumentType type = database.getSchema().getType("Ty");
    final Throwable refused = catchThrowable(() -> Type.convert(database, 99999999999L, Integer.class, type.getProperty("i")));
    assertThat(refused).isInstanceOf(ValueOutOfRangeException.class).isInstanceOf(IllegalArgumentException.class);

    // a reader with no schema to blame keeps what arrived, as it keeps every other value the declared type cannot take
    assertThat(Type.convertOrKeep(database, 99999999999L, Integer.class, type.getProperty("i"))).isEqualTo(99999999999L);
  }

  @Test
  void anUnknownSqlFunctionIsTheCallersMistake() {
    for (final String query : new String[] { "SELECT foo(1)", "SELECT now()" }) {
      final Throwable refused = catchThrowable(() -> database.query("sql", query).stream().count());
      final UnknownFunctionException unknown = CauseChain.find(refused, UnknownFunctionException.class);
      assertThat(unknown).as("%s answered %s", query, refused).isNotNull();
      assertThat(unknown.getMessage()).startsWith("Unknown function name '");
      assertThat(ErrorCategory.of(refused)).as(query).isEqualTo(ErrorCategory.PARSING);
    }
    final UnknownFunctionException named = CauseChain.find(catchThrowable(() -> database.query("sql", "SELECT foo(1)").stream().count()),
        UnknownFunctionException.class);
    assertThat(named.getFunctionName()).isEqualTo("foo");
    // still the CommandExecutionException every caller of a function lookup catches
    assertThat(named).isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void anUnknownCypherFunctionIsTheSameMistakeAndStillASyntaxError() {
    final Throwable refused = catchThrowable(() -> database.query("opencypher", "RETURN noSuchFunction(1) AS x").stream().count());
    assertThat(refused).isInstanceOf(CommandParsingException.class).isNotInstanceOf(CommandSemanticException.class);
    assertThat(refused.getMessage()).contains("Unknown function 'noSuchFunction'");
    assertThat(CauseChain.find(refused, UnknownFunctionException.class)).as("got %s", refused).isNotNull();
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.PARSING);
  }

  private void insert(final String assignments) {
    database.command("sql", "INSERT INTO Ty SET " + assignments);
  }

  private void assertKind(final Executable write, final ValidationException.Kind expected) {
    final Throwable refused = catchThrowable(() -> {
      try {
        database.transaction(() -> {
          try {
            write.execute();
          } catch (final RuntimeException e) {
            throw e;
          } catch (final Throwable e) {
            throw new RuntimeException(e);
          }
        });
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    });
    final ValidationException validation = CauseChain.find(refused, ValidationException.class);
    assertThat(validation).as("expected a %s violation, got %s", expected, refused).isNotNull();
    assertThat(validation.getKind()).as(validation.getMessage()).isEqualTo(expected);
    assertThat(ErrorCategory.of(refused)).isEqualTo(ErrorCategory.VALIDATION);
  }
}
