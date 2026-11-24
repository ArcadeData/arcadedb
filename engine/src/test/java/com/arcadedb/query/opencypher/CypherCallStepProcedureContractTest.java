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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.UnknownProcedureException;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What {@code CallStep} promises about a procedure call, observed through a probe procedure that yields {@code a} (the row
 * number) and {@code b} (its text): which of its declared fields the procedure is told to produce, the count answered without
 * reading the rows and when it must not be, the streams the step closes, what OPTIONAL CALL turns into a row of nulls and what it
 * must not hide, and the variables a chained call keeps.
 */
class CypherCallStepProcedureContractTest extends TestHelper {
  private Probe read;
  private Probe write;

  @BeforeEach
  void registerProbes() {
    read = new Probe("test.probeRead", false);
    write = new Probe("test.probeWrite", true);
    CypherProcedureRegistry.registerOrReplace(read);
    CypherProcedureRegistry.registerOrReplace(write);
  }

  @AfterEach
  void unregisterProbes() {
    CypherProcedureRegistry.unregister("test.probeRead");
    CypherProcedureRegistry.unregister("test.probeWrite");
  }

  // ---- the fields the procedure is asked to produce -----------------------------------------------------------------------------

  @Test
  void theProcedureIsToldTheFieldsTheYieldReads() {
    run("CALL test.probeRead() YIELD a, b RETURN a");
    run("CALL test.probeRead() YIELD a RETURN a");
    run("CALL test.probeRead() YIELD b AS renamed RETURN renamed");
    run("CALL test.probeRead() YIELD a WHERE a > 0 RETURN a");
    run("CALL test.probeRead() YIELD * RETURN *");
    run("CALL test.probeRead()");
    run("CALL test.probeRead() YIELD a, b RETURN count(*) AS c");

    assertThat(read.requested).containsExactly(
        Set.of("a", "b"),
        Set.of("a"),
        Set.of("b"),
        Set.of("a"),
        Set.of("a", "b"),
        Set.of("a", "b"),
        Set.of("a", "b"));
  }

  @Test
  void aChainedCallAsksForTheSameFieldsOnEveryInputRow() {
    final List<Result> rows = run("UNWIND [1, 2, 3] AS x CALL test.probeRead() YIELD a RETURN x, a");
    assertThat(rows).hasSize(9);
    assertThat(read.requested).containsExactly(Set.of("a"), Set.of("a"), Set.of("a"));
  }

  // ---- the count answered without reading the rows --------------------------------------------------------------------------------

  @Test
  void aCountOfAProcedureIsTheNumberOfItsRowsAndTheStreamIsClosed() {
    read.rows = 3;
    assertThat(count("CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(3);
    assertThat(read.closed.get()).as("the count never reads the rows, the stream is closed all the same").isEqualTo(1);

    assertThat(count("UNWIND [1, 2] AS x CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(6);
    assertThat(read.closed.get()).isEqualTo(3);
  }

  @Test
  void aYieldWhereDropsRowsTheStreamSizeStillCounts() {
    read.rows = 3; // a = 0, 1, 2
    assertThat(count("CALL test.probeRead() YIELD a WHERE a >= 1 RETURN count(*) AS c")).isEqualTo(2);
    assertThat(count("UNWIND [0, 1, 2, 3] AS lim CALL test.probeRead() YIELD a WHERE a >= lim RETURN count(*) AS c")).isEqualTo(3 + 2 + 1);
    assertThat(read.closed.get()).as("every stream closed, read or counted").isEqualTo(5);
  }

  @Test
  void aCountOfAWriteProcedureRunsItAndCountsItsRows() {
    write.rows = 2;
    try (final ResultSet rs = database.command("opencypher", "UNWIND [1, 2, 3] AS x CALL test.probeWrite() YIELD a RETURN count(*) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(6);
    }
    assertThat(write.executed.get()).isEqualTo(3);
    assertThat(write.closed.get()).isEqualTo(3);
  }

  @Test
  void aProcedureWithNoRowsCountsZeroAndOptionalCountsTheNullRow() {
    read.rows = 0;
    assertThat(count("CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(0);
    assertThat(count("OPTIONAL CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(1);
    assertThat(count("UNWIND [1, 2] AS x OPTIONAL CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(2);

    // a mix: the input rows whose call has rows count them, the others count the one null row
    read.rows = 2;
    assertThat(count("UNWIND [1, 2] AS x OPTIONAL CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(4);
  }

  @Test
  void aStreamOfUnknownSizeIsCountedByReadingIt() {
    read.rows = 4;
    read.sized = false;
    assertThat(count("CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(4);
    assertThat(count("UNWIND [1, 2] AS x CALL test.probeRead() YIELD a RETURN count(*) AS c")).isEqualTo(8);
    assertThat(read.closed.get()).isEqualTo(3);
  }

  @Test
  void aCountOfAColumnReadsTheRows() {
    read.rows = 3;
    try (final ResultSet rs = database.query("opencypher", "CALL test.probeRead() YIELD a RETURN count(DISTINCT a) AS c, max(a) AS m")) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("c")).intValue()).isEqualTo(3);
      assertThat(((Number) r.getProperty("m")).intValue()).isEqualTo(2);
    }
  }

  // ---- the streams the step closes ----------------------------------------------------------------------------------------------

  @Test
  void everyStreamIsClosedOnceItsRowsAreRead() {
    read.rows = 2;
    assertThat(run("UNWIND [1, 2, 3] AS x CALL test.probeRead() YIELD a RETURN x, a")).hasSize(6);
    assertThat(read.executed.get()).isEqualTo(3);
    assertThat(read.closed.get()).isEqualTo(3);
  }

  @Test
  void aStreamThatFailsWhileItsRowsAreReadIsClosedAndTheFailureSurfaces() {
    read.rows = 4;
    read.failAtRow = 2;
    assertThatThrownBy(() -> run("CALL test.probeRead() YIELD a RETURN a")).hasMessageContaining("row 2 refused");
    assertThat(read.closed.get()).isEqualTo(1);
  }

  // ---- what an OPTIONAL CALL turns into a row and what it must not hide ----------------------------------------------------------

  @Test
  void aFailureIsWrappedWithTheProcedureNameAndItsMessage() {
    read.failOnExecute = new IllegalStateException("boom");
    assertThatThrownBy(() -> run("CALL test.probeRead() YIELD a RETURN a")).isInstanceOf(CommandExecutionException.class)
        .hasMessage("Error executing procedure: test.probeRead: boom").hasCauseInstanceOf(IllegalStateException.class);

    read.failOnExecute = new IllegalStateException();
    assertThatThrownBy(() -> run("CALL test.probeRead() YIELD a RETURN a")).isInstanceOf(CommandExecutionException.class)
        .hasMessage("Error executing procedure: test.probeRead");
  }

  @Test
  void anOptionalCallAnswersARowOfNullsForAFailure() {
    read.failOnExecute = new IllegalStateException("boom");
    final List<Result> rows = run("OPTIONAL CALL test.probeRead() YIELD a, b RETURN a, b");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("a")).isNull();
    assertThat((Object) rows.getFirst().getProperty("b")).isNull();

    final List<Result> kept = run("UNWIND [7, 8] AS x OPTIONAL CALL test.probeRead() YIELD a RETURN x, a");
    assertThat(kept).hasSize(2);
    assertThat(kept).allSatisfy(r -> assertThat((Object) r.getProperty("a")).isNull());
    assertThat(kept.stream().map(r -> ((Number) r.getProperty("x")).intValue()).toList()).containsExactly(7, 8);
  }

  @Test
  void anOptionalCallNamesEveryDeclaredFieldInItsRowOfNulls() {
    read.rows = 0;
    final List<Result> starred = run("OPTIONAL CALL test.probeRead() YIELD * RETURN *");
    assertThat(starred).hasSize(1);
    assertThat(starred.getFirst().getPropertyNames()).containsExactlyInAnyOrder("a", "b");
    assertThat((Object) starred.getFirst().getProperty("a")).isNull();

    final List<Result> renamed = run("OPTIONAL CALL test.probeRead() YIELD b AS text RETURN text");
    assertThat(renamed).hasSize(1);
    assertThat(renamed.getFirst().getPropertyNames()).containsExactly("text");
  }

  @Test
  void aConflictAndAnUnknownProcedureAreNotHiddenByOptional() {
    read.failOnExecute = new ConcurrentModificationException("page conflict");
    assertThatThrownBy(() -> run("OPTIONAL CALL test.probeRead() YIELD a RETURN a")).isInstanceOf(ConcurrentModificationException.class);
    assertThatThrownBy(() -> run("CALL test.probeRead() YIELD a RETURN a")).isInstanceOf(ConcurrentModificationException.class);

    // a name nothing answers could be a write procedure, so it runs as a command
    assertThatThrownBy(() -> command("CALL no.such.procedure() YIELD x RETURN x")).isInstanceOf(UnknownProcedureException.class)
        .hasMessageContaining("no.such.procedure");
    assertThatThrownBy(() -> command("OPTIONAL CALL no.such.procedure() YIELD x RETURN x")).isInstanceOf(UnknownProcedureException.class);
  }

  @Test
  void aYieldWhereThatRejectsEveryRowOfAnOptionalCallLeavesTheNullRow() {
    read.rows = 3;
    final List<Result> rows = run("UNWIND [5, 1] AS lim OPTIONAL CALL test.probeRead() YIELD a WHERE a >= lim RETURN lim, a");
    // lim 5: no row passes, so one row of nulls; lim 1: the rows 1 and 2
    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).<Number>getProperty("lim").intValue()).isEqualTo(5);
    assertThat((Object) rows.get(0).getProperty("a")).isNull();
    assertThat(rows.subList(1, 3).stream().map(r -> ((Number) r.getProperty("a")).intValue()).toList()).containsExactly(1, 2);
  }

  // ---- the variables a chained call keeps -----------------------------------------------------------------------------------------

  @Test
  void aChainedCallKeepsTheVariablesOfTheRowsBeforeIt() {
    read.rows = 2;
    final List<Result> rows = run("UNWIND [10, 20] AS x WITH x, x * 2 AS y CALL test.probeRead() YIELD a, b RETURN x, y, a, b ORDER BY x, a");
    assertThat(rows).hasSize(4);
    assertThat(rows.stream().map(r -> r.<Number>getProperty("x").intValue() + ":" + r.<Number>getProperty("y").intValue() + ":"
        + r.<Number>getProperty("a").intValue() + ":" + r.getProperty("b")).toList())
        .containsExactly("10:20:0:b0", "10:20:1:b1", "20:40:0:b0", "20:40:1:b1");
  }

  @Test
  void aYieldWhereSeesTheVariablesOfTheRowsBeforeIt() {
    read.rows = 3;
    final List<Result> rows = run("UNWIND [0, 1, 2] AS lim CALL test.probeRead() YIELD a WHERE a >= lim RETURN lim, a ORDER BY lim, a");
    assertThat(rows.stream().map(r -> r.<Number>getProperty("lim").intValue() + ":" + r.<Number>getProperty("a").intValue()).toList())
        .containsExactly("0:0", "0:1", "0:2", "1:1", "1:2", "2:2");
  }

  @Test
  void theArgumentsAreEvaluatedAgainstTheInputRow() {
    final List<Result> rows = run("UNWIND [1, 2] AS x CALL test.probeRead(x) YIELD a RETURN x, a");
    assertThat(read.args).hasSize(2);
    assertThat(((Number) read.args.get(0)[0]).intValue()).isEqualTo(1);
    assertThat(((Number) read.args.get(1)[0]).intValue()).isEqualTo(2);
    assertThat(rows).hasSize(6);
  }

  // ---- a function answered instead of a procedure ------------------------------------------------------------------------------------

  @Test
  void aDefinedFunctionNamesItsValueByTheFirstYieldItem() {
    database.command("sql", "DEFINE FUNCTION math.double \"SELECT :x * 2\" PARAMETERS [x] LANGUAGE sql");
    try (final ResultSet rs = database.command("opencypher", "CALL math.double(4) YIELD result RETURN result")) {
      assertThat(((Number) rs.next().getProperty("result")).intValue()).isEqualTo(8);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void aSqlFunctionAnswersItsValue() {
    try (final ResultSet rs = database.query("opencypher", "CALL abs(-3) YIELD value RETURN value")) {
      assertThat(((Number) rs.next().getProperty("value")).intValue()).isEqualTo(3);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  // ---- helpers ----------------------------------------------------------------------------------------------------------------------------

  private List<Result> run(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private List<Result> command(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private int count(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      final int c = ((Number) rs.next().getProperty("c")).intValue();
      assertThat(rs.hasNext()).isFalse();
      return c;
    }
  }

  /** Yields {@code rows} rows of a (the row number) and b (its text), and records what the step asked of it. */
  private static final class Probe implements CypherProcedure {
    private final String                 name;
    private final boolean                write;
    private final List<Set<String>>      requested = new CopyOnWriteArrayList<>();
    private final List<Object[]>         args      = new CopyOnWriteArrayList<>();
    private final AtomicInteger          executed  = new AtomicInteger();
    private final AtomicInteger          closed    = new AtomicInteger();
    private volatile int                 rows      = 3;
    private volatile boolean             sized     = true;
    private volatile int                 failAtRow = -1;
    private volatile RuntimeException    failOnExecute;

    private Probe(final String name, final boolean write) {
      this.name = name;
      this.write = write;
    }

    @Override
    public String getName() {
      return name;
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
      return "Test probe";
    }

    @Override
    public List<String> getYieldFields() {
      return List.of("a", "b");
    }

    @Override
    public boolean isWriteProcedure() {
      return write;
    }

    private static Result row(final int i, final int failAt) {
      if (i == failAt)
        throw new IllegalStateException("row " + i + " refused");
      final ResultInternal r = new ResultInternal();
      r.setProperty("a", i);
      r.setProperty("b", "b" + i);
      return r;
    }

    @Override
    public Stream<Result> execute(final Object[] arguments, final Result inputRow, final CommandContext context) {
      return execute(arguments, inputRow, context, Set.of("a", "b"));
    }

    @Override
    public Stream<Result> execute(final Object[] arguments, final Result inputRow, final CommandContext context,
        final Set<String> requestedYieldFields) {
      executed.incrementAndGet();
      requested.add(Set.copyOf(requestedYieldFields));
      args.add(arguments);
      if (failOnExecute != null)
        throw failOnExecute;
      final int failAt = failAtRow;
      final int count = rows;
      // rows made as they are read, so that a failure is one of the read; a List's stream knows its size, the other one does not
      final Stream<Result> stream = sized ?
          IntStream.range(0, count).mapToObj(i -> row(i, failAt)) :
          Stream.iterate(0, i -> i + 1).limit(count).map(i -> row(i, failAt));
      return stream.onClose(closed::incrementAndGet);
    }
  }

}
