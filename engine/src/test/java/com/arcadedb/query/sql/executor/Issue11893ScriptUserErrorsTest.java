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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.TypeMismatchException;
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
