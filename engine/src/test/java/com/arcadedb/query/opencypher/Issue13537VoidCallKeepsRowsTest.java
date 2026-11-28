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
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13537: a CALL that declares no output column (a void procedure, or a function that answered nothing) leaves its input
 * rows alone when another clause follows it, as in Neo4j, where {@code CALL apoc.util.sleep(10) RETURN 1 AS x} answers one row.
 * It used to remove every row. A void CALL that ends the query keeps answering no row, and a procedure that declares output
 * columns and answers no row keeps removing the row.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13537VoidCallKeepsRowsTest extends TestHelper {
  private VoidProcedure voidProcedure;
  private EmptyProcedure emptyProcedure;

  @BeforeEach
  void registerProcedures() {
    voidProcedure = new VoidProcedure();
    emptyProcedure = new EmptyProcedure();
    CypherProcedureRegistry.registerOrReplace(voidProcedure);
    CypherProcedureRegistry.registerOrReplace(emptyProcedure);
  }

  @AfterEach
  void unregisterProcedures() {
    CypherProcedureRegistry.unregister(VoidProcedure.NAME);
    CypherProcedureRegistry.unregister(EmptyProcedure.NAME);
  }

  // ---- the statements of the issue -------------------------------------------------------------------------------------------

  @Test
  void aVoidCallAsFirstClauseAnswersTheRowOfTheReturn() {
    final List<Result> rows = query("CALL apoc.util.sleep(10) RETURN 1 AS x");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("x")).intValue()).isEqualTo(1);
  }

  @Test
  void aVoidCallKeepsEveryInputRow() {
    assertThat(column(query("UNWIND [1, 2, 3] AS i CALL apoc.util.sleep(1) RETURN i"), "i")).containsExactly(1, 2, 3);
  }

  @Test
  void aVoidCallAfterAFilteringWithKeepsTheRowsThatPassed() {
    assertThat(column(query("UNWIND [1, 2, 3] AS i WITH i WHERE i > 1 CALL apoc.util.sleep(1) RETURN i ORDER BY i"), "i"))
        .containsExactly(2, 3);
  }

  @Test
  void aVoidCallWithParametersAnswersTheReturnedParameters() {
    // the neomodel statement that read result[0] and failed with IndexError
    try (final ResultSet rs = database.query("opencypher", "CALL apoc.util.sleep($delay_ms) RETURN $task_id AS task_id, $delay_ms AS slept",
        Map.of("delay_ms", 1, "task_id", 7))) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("task_id")).intValue()).isEqualTo(7);
      assertThat(((Number) row.getProperty("slept")).intValue()).isEqualTo(1);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ---- a void procedure ------------------------------------------------------------------------------------------------------

  @Test
  void aVoidProcedureRunsOncePerInputRowAndKeepsIt() {
    assertThat(column(query("UNWIND [1, 2, 3] AS i CALL test.voidProc(i) RETURN i"), "i")).containsExactly(1, 2, 3);
    assertThat(voidProcedure.executed.get()).isEqualTo(3);
    assertThat(voidProcedure.closed.get()).as("every stream is closed").isEqualTo(3);
  }

  @Test
  void aVoidProcedureAsFirstClauseAnswersOneRow() {
    assertThat(query("CALL test.voidProc() RETURN 1 AS x")).hasSize(1);
    assertThat(voidProcedure.executed.get()).isEqualTo(1);
    assertThat(voidProcedure.closed.get()).isEqualTo(1);
  }

  @Test
  void aVoidCallBetweenMatchAndWithKeepsTheRowsAndTheirVariables() {
    database.transaction(() -> {
      database.getSchema().createVertexType("V13537");
      for (int i = 0; i < 3; i++)
        database.newVertex("V13537").set("n", i).save();
    });
    final List<Result> rows = query(
        "MATCH (v:V13537) CALL test.voidProc(v.n) WITH v, v.n * 10 AS t CALL apoc.util.sleep(0) RETURN v.n AS n, t ORDER BY n");
    assertThat(column(rows, "n")).containsExactly(0, 1, 2);
    assertThat(column(rows, "t")).containsExactly(0, 10, 20);
    assertThat(voidProcedure.executed.get()).isEqualTo(3);
  }

  @Test
  void aVoidCallInAWriteStatementKeepsTheRows() {
    try (final ResultSet rs = database.command("opencypher", "UNWIND [1, 2] AS i CREATE (n:W13537 {i: i}) WITH n CALL test.voidProc() RETURN n.i AS i")) {
      final List<Integer> values = new ArrayList<>();
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("i")).intValue());
      assertThat(values).containsExactlyInAnyOrder(1, 2);
    }
    assertThat(voidProcedure.executed.get()).isEqualTo(2);
  }

  @Test
  void aVoidCallInsideASubqueryKeepsTheOuterRows() {
    assertThat(column(query("UNWIND [1, 2] AS i CALL { WITH i CALL apoc.util.sleep(0) RETURN i * 2 AS j } RETURN j ORDER BY j"), "j"))
        .containsExactly(2, 4);
  }

  // ---- count(*) --------------------------------------------------------------------------------------------------------------

  @Test
  void aCountAfterAVoidCallCountsTheInputRows() {
    assertThat(count("CALL apoc.util.sleep(0) RETURN count(*) AS c")).isEqualTo(1);
    assertThat(count("UNWIND [1, 2, 3] AS i CALL apoc.util.sleep(0) RETURN count(*) AS c")).isEqualTo(3);
    assertThat(count("CALL test.voidProc() RETURN count(*) AS c")).isEqualTo(1);
    assertThat(count("UNWIND [1, 2, 3] AS i CALL test.voidProc() RETURN count(*) AS c")).isEqualTo(3);
    assertThat(voidProcedure.executed.get()).isEqualTo(4);
    assertThat(voidProcedure.closed.get()).isEqualTo(4);
  }

  // ---- what does not change --------------------------------------------------------------------------------------------------

  @Test
  void aVoidCallThatEndsTheQueryAnswersNoRowButStillRuns() {
    assertThat(query("CALL test.voidProc()")).isEmpty();
    assertThat(query("UNWIND [1, 2, 3] AS i CALL test.voidProc(i)")).isEmpty();
    assertThat(query("CALL apoc.util.sleep(0)")).isEmpty();
    assertThat(voidProcedure.executed.get()).isEqualTo(4);
    assertThat(voidProcedure.closed.get()).isEqualTo(4);
  }

  @Test
  void anOptionalVoidCallKeepsEveryInputRow() {
    assertThat(column(query("UNWIND [1, 2] AS i OPTIONAL CALL apoc.util.sleep(0) RETURN i"), "i")).containsExactly(1, 2);
    assertThat(column(query("UNWIND [1, 2] AS i OPTIONAL CALL test.voidProc() RETURN i"), "i")).containsExactly(1, 2);
  }

  @Test
  void aProcedureWithOutputColumnsAndNoRowStillRemovesTheRow() {
    assertThat(query("UNWIND [1, 2, 3] AS i CALL test.emptyProc() YIELD out RETURN i")).isEmpty();
    assertThat(query("CALL test.emptyProc() YIELD out RETURN 1 AS x")).isEmpty();
    assertThat(count("UNWIND [1, 2, 3] AS i CALL test.emptyProc() YIELD out RETURN count(*) AS c")).isEqualTo(0);
    assertThat(emptyProcedure.executed.get()).isEqualTo(7);
  }

  @Test
  void aFunctionThatAnswersAValueStillAnswersItsRow() {
    final List<Result> rows = query("UNWIND [1, 2] AS i CALL abs(-3) YIELD value RETURN i, value");
    assertThat(column(rows, "i")).containsExactly(1, 2);
    assertThat(column(rows, "value")).containsExactly(3, 3);
  }

  @Test
  void aFunctionThatAnswersNullUnderYieldStillRemovesTheRow() {
    // YIELD asks for a column, which a null answer does not hold: that is not a void call
    assertThat(query("UNWIND [1, 2] AS i CALL sql.coalesce(null) YIELD value RETURN i")).isEmpty();
  }

  // ---- helpers ---------------------------------------------------------------------------------------------------------------

  private List<Result> query(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private long count(final String cypher) {
    final List<Result> rows = query(cypher);
    assertThat(rows).hasSize(1);
    return ((Number) rows.getFirst().getProperty("c")).longValue();
  }

  private static List<Integer> column(final List<Result> rows, final String name) {
    final List<Integer> values = new ArrayList<>(rows.size());
    for (final Result row : rows)
      values.add(((Number) row.getProperty(name)).intValue());
    return values;
  }

  /** Declares no output column, as Neo4j's void procedures do. */
  private static final class VoidProcedure implements CypherProcedure {
    private static final String        NAME     = "test.voidProc";
    private final        AtomicInteger executed = new AtomicInteger();
    private final        AtomicInteger closed   = new AtomicInteger();

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return 1;
    }

    @Override
    public String getDescription() {
      return "Void test procedure";
    }

    @Override
    public List<String> getYieldFields() {
      return List.of();
    }

    @Override
    public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
      executed.incrementAndGet();
      return Stream.<Result>empty().onClose(closed::incrementAndGet);
    }
  }

  /** Declares an output column and answers no row. */
  private static final class EmptyProcedure implements CypherProcedure {
    private static final String        NAME     = "test.emptyProc";
    private final        AtomicInteger executed = new AtomicInteger();

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public int getMinArgs() {
      return 0;
    }

    @Override
    public int getMaxArgs() {
      return 0;
    }

    @Override
    public String getDescription() {
      return "Empty test procedure";
    }

    @Override
    public List<String> getYieldFields() {
      return List.of("out");
    }

    @Override
    public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
      executed.incrementAndGet();
      return Stream.empty();
    }
  }
}
