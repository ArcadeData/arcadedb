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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.LtOperator;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11893: SQL script statements failed with a server error (NullPointerException, ClassCastException,
 * TransactionException, a plain CommandExecutionException) for a mistake in the script's own values or order. Each is now
 * refused with an exception every protocol reports as a client error.
 */
class Issue11893ScriptUserErrorsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE S");
      database.command("sql", "INSERT INTO S SET k = 1");
    });
  }

  @Test
  void sleepOnNullIsATypeMismatch() {
    assertClientError("SLEEP $nothing;", TypeMismatchException.class, "Invalid value for SLEEP: null");
  }

  @Test
  void sleepOnAStringIsATypeMismatch() {
    assertClientError("SLEEP \"a\";", TypeMismatchException.class, "Invalid value for SLEEP: a");
  }

  @Test
  void sleepOnAListIsATypeMismatch() {
    assertClientError("LET $x = [1]; SLEEP $x;", TypeMismatchException.class, "SLEEP takes a number of milliseconds");
  }

  @Test
  void sleepOnANumberStillSleeps() {
    try (final ResultSet rs = database.command("sqlscript", "LET $x = 1; SLEEP $x; RETURN 5;")) {
      assertThat(rs.next().<Integer>getProperty("value")).isEqualTo(5);
    }
  }

  @Test
  void unknownConsoleLevelIsRefusedBeforeTheScriptRuns() {
    assertClientError("INSERT INTO S SET k = 2; CONSOLE.nope \"x\";", CommandSQLParsingException.class,
        "Unsupported CONSOLE level 'nope'");
    // refused at parse time: the INSERT before it did not run
    assertThat(database.countType("S", false)).isEqualTo(1);
  }

  @Test
  void knownConsoleLevelsStillWork() {
    for (final String level : new String[] { "log", "output", "error", "warn", "debug", "LOG" })
      try (final ResultSet rs = database.command("sqlscript", "CONSOLE." + level + " \"x\";")) {
        assertThat(rs.next().<String>getProperty("operation")).isEqualTo("console");
      }
  }

  @Test
  void lockAfterAModificationIsASemanticError() {
    database.begin();
    try {
      assertClientError("INSERT INTO S SET k = 2; LOCK TYPE S;", CommandSemanticException.class,
          "Explicit lock must be acquired before any modification");
    } finally {
      database.rollback();
    }
  }

  @Test
  void secondLockIsASemanticError() {
    database.begin();
    try {
      assertClientError("LOCK TYPE S; LOCK TYPE S;", CommandSemanticException.class, "Explicit lock already acquired");
    } finally {
      database.rollback();
    }
  }

  @Test
  void lockBeforeAnyModificationStillLocks() {
    database.begin();
    try {
      database.command("sqlscript", "LOCK TYPE S; INSERT INTO S SET k = 2;").close();
    } finally {
      database.rollback();
    }
  }

  @Test
  void lockAfterTheScriptEndedItsTransactionIsASemanticError() {
    // the COMMIT inside the IF ends the caller's transaction, so the LOCK finds none: the script's order, as for a second LOCK
    database.begin();
    try {
      assertClientError("IF (1 = 1) { COMMIT; } LOCK TYPE S;", CommandSemanticException.class, "Transaction not begun");
    } finally {
      rollbackAll();
    }
  }

  @Test
  void lessThanBetweenRowsIsATypeMismatch() {
    assertClientError("LET $x = SELECT FROM S LIMIT 1; IF ($x[0] < $x[0]) { RETURN 1; }", TypeMismatchException.class,
        "Cannot compare a record with a record using '<'");
  }

  @Test
  void greaterOrEqualBetweenTwoRowsIsATypeMismatch() {
    // two different rows: GeOperator answers true for the very same instance on both sides before it compares anything
    database.transaction(() -> assertClientError(
        "INSERT INTO S SET k = 2; LET $x = SELECT FROM S ORDER BY k; IF ($x[0] >= $x[1]) { RETURN 1; }",
        TypeMismatchException.class, "using '>='"));
  }

  @Test
  void lessThanBetweenRowsInAWhereIsATypeMismatch() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM S WHERE $current < $current").stream().count())
        .isInstanceOf(TypeMismatchException.class);
  }

  @Test
  void betweenTwoRowsIsATypeMismatch() {
    // x BETWEEN a AND b is x >= a AND x <= b, so it refuses rows as >= and <= do
    database.transaction(() -> assertClientError(
        "INSERT INTO S SET k = 2; LET $x = SELECT FROM S ORDER BY k; IF ($x[0] BETWEEN $x[1] AND $x[1]) { RETURN 1; }",
        TypeMismatchException.class, "using 'BETWEEN'"));
  }

  @Test
  void aClassCastExceptionOfAnOperatorWithNoOrderIsATypeMismatch() {
    final BinaryCondition condition = conditionOver(new LtOperator());
    assertThatThrownBy(() -> condition.evaluate((Identifiable) null, null)).isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("using '<'").hasCauseInstanceOf(ClassCastException.class);
  }

  @Test
  void aClassCastExceptionOfAnOperatorThatIsNoOrderingIsNotRelabelled() {
    // a ClassCastException out of = or LIKE is a fault of the engine, not a statement comparing what has no order
    final BinaryCondition condition = conditionOver(new FaultyOperator());
    assertThatThrownBy(() -> condition.evaluate((Identifiable) null, null)).isExactlyInstanceOf(ClassCastException.class)
        .hasMessage("a fault of the operator");
  }

  @Test
  void commitInsideAnIfWithoutABeginIsRefusedAsAtTheTopLevel() {
    assertClientError("IF (1 = 1) { COMMIT; }", CommandSQLParsingException.class, "Found COMMIT statement without a BEGIN");
  }

  @Test
  void commitInALoopWithoutABeginIsRefusedOnceNoTransactionIsLeft() {
    // the first COMMIT ends the caller's transaction, as it did before; the second has none left to end
    database.begin();
    try {
      assertClientError("LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; COMMIT; }", CommandSQLParsingException.class,
          "Found COMMIT statement without a BEGIN");
    } finally {
      rollbackAll();
    }
  }

  @Test
  void commitRetryInALoopWithoutABeginIsRefused() {
    assertClientError(
        "LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; COMMIT RETRY 3 ELSE { INSERT INTO S SET k = -9; } AND CONTINUE; }",
        CommandSQLParsingException.class, "Found COMMIT statement without a BEGIN");
  }

  @Test
  void beginAndCommitInsideALoopStillWork() {
    try (final ResultSet rs = database.command("sqlscript",
        "LET $i = 0; WHILE ($i < 2) { LET $i = $i + 1; BEGIN; INSERT INTO S SET k = 5; COMMIT; } RETURN 1;")) {
      rs.stream().count();
    }
    assertThat(database.countType("S", false)).isEqualTo(3);
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void beginPastTheNestedTransactionLimitIsASemanticError() {
    try {
      assertClientError("LET $i = 0; WHILE ($i < 5) { LET $i = $i + 1; BEGIN; }", CommandSemanticException.class,
          "Exceeded number of 3 nested transactions");
    } finally {
      rollbackAll();
    }
  }

  /** A condition over two operands that have no order: one object each, which nothing can compare. */
  private static BinaryCondition conditionOver(final BinaryCompareOperator operator) {
    final BinaryCondition condition = new BinaryCondition();
    condition.left = new ObjectExpression();
    condition.right = new ObjectExpression();
    condition.operator = operator;
    return condition;
  }

  /** An operand that evaluates to a new plain object, whatever the record. */
  private static class ObjectExpression extends Expression {
    @Override
    public Object execute(final Identifiable currentRecord, final CommandContext context) {
      return new Object();
    }

    @Override
    public Object execute(final Result currentRecord, final CommandContext context) {
      return new Object();
    }
  }

  /** An operator that is not an ordering one and fails with a ClassCastException of its own. */
  private static class FaultyOperator implements BinaryCompareOperator {
    @Override
    public boolean execute(final DatabaseInternal database, final Object left, final Object right) {
      throw new ClassCastException("a fault of the operator");
    }

    @Override
    public BinaryCompareOperator copy() {
      return this;
    }

    @Override
    public boolean isLess() {
      return false;
    }

    @Override
    public boolean isGreater() {
      return false;
    }

    @Override
    public boolean isInclude() {
      return false;
    }
  }

  private void rollbackAll() {
    while (database.isTransactionActive())
      database.rollback();
  }

  private void assertClientError(final String script, final Class<? extends Throwable> type, final String message) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("sqlscript", script)) {
        rs.stream().count();
      }
    }).isInstanceOf(type).hasMessageContaining(message).satisfies(e -> assertThat(ErrorCategory.of(e))
        .isIn(ErrorCategory.VALIDATION, ErrorCategory.PARSING));
  }
}
