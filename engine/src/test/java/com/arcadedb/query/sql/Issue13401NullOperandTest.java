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
import com.arcadedb.database.RID;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13401: {@code Set.of(null)} answered a bare {@code NullPointerException}, with no message, in two SQL statements. A
 * {@code CREATE EDGE} whose FROM or TO evaluates to null (a variable that is not set, a LET that holds null, a LET of an earlier
 * command) is now refused with a {@link TypeMismatchException} that names the side, as a null element of a list already was.
 * {@code <collection> CONTAINS <null>} no longer throws: the loop that compares the operands already treats two nulls as equal,
 * so a collection without a null element does not contain it and the row does not match.
 */
class Issue13401NullOperandTest extends TestHelper {
  private RID a;
  private RID b;

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.n INTEGER");
    database.command("sql", "CREATE EDGE TYPE E");
    database.transaction(() -> {
      a = database.command("sql", "CREATE VERTEX V SET n = 1").next().getIdentity().get();
      b = database.command("sql", "CREATE VERTEX V SET n = 2").next().getIdentity().get();
      database.command("sql", "CREATE EDGE E FROM " + a + " TO " + b);
    });
  }

  @Test
  void createEdgeFromAVariableThatIsNotSetIsRefusedWithAMessage() {
    database.begin();
    try {
      assertThatThrownBy(() -> database.command("sql", "CREATE EDGE E FROM $nothing TO (SELECT FROM V)")) //
          .isInstanceOf(TypeMismatchException.class) //
          .hasMessageContaining("FROM expression evaluated to null");
    } finally {
      database.rollback();
    }
    assertThat(edgeCount()).isEqualTo(1);
  }

  @Test
  void createEdgeToAVariableThatIsNotSetIsRefusedWithAMessage() {
    database.begin();
    try {
      assertThatThrownBy(() -> database.command("sql", "CREATE EDGE E FROM (SELECT FROM V) TO $nothing")) //
          .isInstanceOf(TypeMismatchException.class) //
          .hasMessageContaining("TO expression evaluated to null");
    } finally {
      database.rollback();
    }
    assertThat(edgeCount()).isEqualTo(1);
  }

  @Test
  void aLetOfAnEarlierCommandIsNullInTheNextOne() {
    // the benchmark's tool ran a script one statement per command: a LET does not outlive its own command
    database.begin();
    try {
      database.command("sql", "LET m = CREATE VERTEX V SET n = 3");
      assertThatThrownBy(() -> database.command("sql", "CREATE EDGE E FROM $m TO (SELECT FROM V WHERE n = 1)")) //
          .isInstanceOf(TypeMismatchException.class) //
          .hasMessageContaining("FROM expression evaluated to null");
    } finally {
      database.rollback();
    }
  }

  @Test
  void aScriptLetThatHoldsNullIsRefusedAndNothingIsCommitted() {
    assertThatThrownBy(() -> database.command("sqlscript",
        "BEGIN; LET x = (SELECT FROM V WHERE n = 99)[0]; CREATE EDGE E FROM $x TO (SELECT FROM V); COMMIT")) //
        .isInstanceOf(TypeMismatchException.class) //
        .hasMessageContaining("FROM expression evaluated to null");
    if (database.isTransactionActive())
      database.rollback();
    assertThat(edgeCount()).isEqualTo(1);
  }

  @Test
  void anEmptySubqueryStillCreatesNoEdgeAndIsNotAnError() {
    database.begin();
    try {
      try (final ResultSet rs = database.command("sql", "CREATE EDGE E FROM (SELECT FROM V WHERE n = 99) TO (SELECT FROM V)")) {
        assertThat(rs.hasNext()).isFalse();
      }
    } finally {
      database.rollback();
    }
    assertThat(edgeCount()).isEqualTo(1);
  }

  @Test
  void aCollectionContainsNullOnlyWhenItHoldsANull() {
    for (final String operand : new String[] { "$nothing", "null" }) {
      try (final ResultSet rs = database.query("sql", "SELECT n FROM V WHERE out('E') CONTAINS " + operand)) {
        assertThat(rs.hasNext()).as("out('E') CONTAINS %s", operand).isFalse();
      }
    }
  }

  @Test
  void aCollectionStillContainsARecordItHolds() {
    try (final ResultSet rs = database.query("sql", "SELECT n FROM V WHERE out('E') CONTAINS :b", Map.of("b", b))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Integer>getProperty("n")).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private long edgeCount() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM E")) {
      return rs.next().<Long>getProperty("c");
    }
  }
}
