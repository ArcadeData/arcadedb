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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapBudgetTestSupport;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The heap budget (issue #8591) of a {@code CALL { }} body. The body runs on a context of its own, so what its
 * aggregation collected is reserved from the budget until the body's result set is closed; the step gives it back when it
 * returns the rows, as the budget does for a subquery written inline in an expression ({@code OperationHeapLimit}), and
 * must close every body it opened whatever happens to it.
 * <p>
 * The first test is the leak: a streamed body that failed on its first pull left its reservation behind after the outer
 * query closed, and nothing gave it back until the garbage collector found the abandoned query. The second guards the
 * collected path (a body that writes), which closes its result set whatever happens and is green before the fix too. The
 * last two guard the rule the fix must not break: a body run for every row of a large input holds one list at a time, so
 * the budget is not refused for the sum of the lists.
 */
class CypherCallSubqueryHeapReleaseTest extends TestHelper {
  private static final int    ROWS      = 3_000;
  private static final int    OUTER     = 40;
  private static final String FILLER    = "x".repeat(80);
  private static final long   MEGABYTES = 8L;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("P");
    database.getSchema().createVertexType("W");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("P").set("id", i, "name", "name-%06d-%s".formatted(i, FILLER)).save();
    });
    // From here the counter moves only with what the test runs
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
  }

  @Test
  void aStreamedCallBodyThatFailsOnItsFirstPullGivesItsHeapBackWhenTheOuterQueryCloses() {
    // THE BODY COLLECTS EVERYTHING, THEN FAILS ON THE FIRST ROW IT HANDS ON: IT HAD NOT BEEN REGISTERED ON THE STEP YET
    final ResultSet rs = database.query("opencypher",
        "CALL { MATCH (p:P) WITH collect(p.name) AS names UNWIND names AS n RETURN 1 / (size(n) - size(n)) AS x } "
            + "RETURN x");
    assertThatThrownBy(rs::hasNext).isInstanceOf(ArithmeticErrorException.class);
    // WHAT THE BODY COLLECTED IS RESERVED WHEN IT FAILS: THE TEST MEASURES SOMETHING, NOT AN EMPTY BUDGET
    assertThat(QueryHeapBudget.getReservedBytes()).isGreaterThan(0L);
    rs.close();
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  @Test
  void aCollectedCallBodyThatFailsWhileCollectingGivesItsHeapBack() {
    // A BODY THAT WRITES IS COLLECTED WHOLE, NOT STREAMED. GREEN BEFORE THE FIX TOO: ITS RESULT SET IS CLOSED WHATEVER HAPPENS
    database.transaction(() -> assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "CALL { MATCH (p:P) WITH collect(p.name) AS names CREATE (:W {k: 1}) WITH names UNWIND names AS n "
              + "RETURN 1 / (size(n) - size(n)) AS x } RETURN x")) {
        rs.hasNext();
      }
    }).isInstanceOf(ArithmeticErrorException.class));
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  @Test
  void aStreamedCallBodyRunForEveryOuterRowHoldsOneListAtATime() {
    // EACH LIST RESERVES FROM THE BUDGET, THE SUM OF THEM DOES NOT FIT IT: THE QUERY HOLDS ONE AT A TIME
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(MEGABYTES);
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", "UNWIND range(1, " + OUTER + ") AS i "
        + "CALL { WITH i MATCH (p:P) RETURN collect(p.name) AS names } RETURN i, size(names) AS n")) {
      while (rs.hasNext()) {
        assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(ROWS);
        ++rows;
      }
    }
    assertThat(rows).isEqualTo(OUTER);
  }

  @Test
  void aCollectedCallBodyRunForEveryOuterRowHoldsOneListAtATime() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(MEGABYTES);
    final long[] rows = { 0L };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "UNWIND range(1, " + OUTER + ") AS i "
          + "CALL { WITH i MATCH (p:P) WITH i, collect(p.name) AS names CREATE (:W {k: i}) RETURN size(names) AS n } "
          + "RETURN i, n")) {
        while (rs.hasNext()) {
          assertThat(rs.next().<Number>getProperty("n").intValue()).isEqualTo(ROWS);
          ++rows[0];
        }
      }
    });
    assertThat(rows[0]).isEqualTo(OUTER);
  }
}
