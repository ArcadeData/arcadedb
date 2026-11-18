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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.PartialResultTimeoutException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.antlr.SQLASTBuilder;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.CommandTimeoutOverride;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.StallAwareStopwatch;
import com.arcadedb.utility.TimeBoundRegex;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consolidated regression tests, one nested class per original issue.
 */
class CommandTimeoutTest {
  /**
   * Regression for #6266: arcadedb.command.timeout is honoured by every command path, not only SQL SELECT.
   * <p>
   * Regression tests for issue #6266 - {@code arcadedb.command.timeout} was documented as "the timeout for commands"
   * but honoured by exactly two code paths: the SQL SELECT planner and, since #6216, the openCypher {@code algo.*}
   * procedures. Everything else ignored it, so an operator who set it to bound a runaway query got that bound for
   * {@code SELECT} and did not get it for an openCypher {@code MATCH} - the shape most likely to run away on a graph
   * workload.
   * <p>
   * The tests below are all the same experiment: a statement that needs far longer than the deadline it is given.
   * Without enforcement each one runs to completion and returns a result; with it each one aborts naming the setting,
   * which is the assertion - the message carries {@code arcadedb.command.timeout}, so a statement that stopped for any
   * other reason (including its own {@code TIMEOUT} clause, whose message names the clause instead) fails the test.
   * <p>
   * The 50 ms deadline is not a latency budget, it is a value far below what the statement costs. Measured on the
   * reference machine with the bound raised out of the way, the five openCypher statements each run for more than 6 s,
   * SQL TRAVERSE for 2.2 s, SQL MATCH for 1.2 s and the rejecting SELECT for 1.6 s - between 24x and 120x the deadline.
   * If one of these ever starts passing for the wrong reason, the thing to check is that its statement still costs
   * orders of magnitude more than 50 ms, not that the deadline is generous enough.
   * <p>
   * {@link #theCheckSitsInsideTheLoopNotBetweenTwoBatches} is the one assertion that is about elapsed time, and says why.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CommandTimeoutCoverage {
    /**
     * Sized by the cheapest statement that has to run long, not by the most expensive one. A self-join is quadratic
     * and would be unmistakable at any size, but TRAVERSE dedups and is therefore linear in the graph, so the graph
     * has to be big enough for one linear pass to outlast the deadline by a wide margin.
     */
    private static final int NODES = 20_000;

    private static final String DB_PATH = "./target/databases/test-issue-6266-command-timeout";

    private static Database database;

    @BeforeAll
    static void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final MutableVertex[] vertices = new MutableVertex[NODES];
        for (int i = 0; i < NODES; i++)
          vertices[i] = database.newVertex("Node").set("v", i).set("name", "n" + i).save();

        // A ring plus two chords per node: every node has out-degree 3, so a variable-length expansion of depth 8
        // has 3^8 paths to walk per starting node and cannot terminate early.
        for (int i = 0; i < NODES; i++) {
          vertices[i].newEdge("LINK", vertices[(i + 1) % NODES], true, (Object[]) null).save();
          vertices[i].newEdge("LINK", vertices[(i + 7) % NODES], true, (Object[]) null).save();
          vertices[i].newEdge("LINK", vertices[(i + 31) % NODES], true, (Object[]) null).save();
        }
      });
    }

    @AfterAll
    static void teardown() {
      if (database != null)
        database.drop();
    }

    @AfterEach
    void resetTimeout() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    }

    // ── openCypher ───────────────────────────────────────────────────────────

    @Test
    void openCypherSelfJoinHonoursTheCommandTimeout() {
      setTimeout(50);

      assertThatThrownBy(() -> drainCypher("MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 RETURN a.v, b.v"))
          .as("an openCypher MATCH is exactly the shape the setting was believed to cover, and did not")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void openCypherAggregationHonoursTheCommandTimeout() {
      // An aggregation emits its single row only after consuming everything, so nothing downstream of the pipeline
      // can notice a deadline on the pipeline's behalf. A bound that only wrapped the returned ResultSet would pass
      // the previous test and fail this one.
      setTimeout(50);

      assertThatThrownBy(() -> drainCypher("MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 RETURN count(*) AS c"))
          .as("a blocking aggregation must be bounded by the work it consumes, not by the row it emits")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void openCypherVariableLengthExpansionHonoursTheCommandTimeout() {
      setTimeout(50);

      assertThatThrownBy(() -> drainCypher("MATCH (a:Node)-[:LINK*1..8]->(b:Node) RETURN b.v"))
          .as("a variable-length expansion is unbounded in the data, which is what the setting exists for")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void openCypherWriteStatementHonoursTheCommandTimeout() {
      setTimeout(50);

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("opencypher", "MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 SET a.touched = true")))
          .as("the write path materializes the result set itself, so it needs its own check")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void aNestedSubqueryDoesNotRestartTheBudget() {
      // Each nested plan builds a CommandContext of its own. Computing the deadline there would hand every CALL { }
      // body a fresh budget, which is how a statement buys itself unlimited time by nesting.
      setTimeout(50);

      assertThatThrownBy(() -> drainCypher(
          "MATCH (a:Node) CALL { WITH a MATCH (b:Node) WHERE b.v + a.v = -1 RETURN b } RETURN a.v, b.v"))
          .as("a CALL subquery shares the outer command's deadline instead of starting a new one")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    // ── SQL beyond SELECT ────────────────────────────────────────────────────

    @Test
    void sqlTraverseHonoursTheCommandTimeout() {
      setTimeout(50);

      // TRAVERSE dedups, so the traversal itself is linear; the per-entry-point projections are what make one
      // pass expensive - six chained hops is 3^6 edge walks for each of the 20k vertices.
      assertThatThrownBy(() -> drainSql(
          "TRAVERSE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK') FROM Node WHILE $depth < 12"))
          .as("TRAVERSE never went through the SELECT planner, so it never saw the setting")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void sqlMatchHonoursTheCommandTimeout() {
      setTimeout(50);

      assertThatThrownBy(() -> drainSql(
          "MATCH {type: Node, as: a}-LINK->{as: b}-LINK->{as: c}-LINK->{as: d}-LINK->{as: e} RETURN a, e"))
          .as("SQL MATCH has no TIMEOUT clause of its own and ignored the global one")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void sqlSelectWithARejectingFilterHonoursTheCommandTimeout() {
      // The granularity hole named in the issue: a filter that rejects every record spends the whole scan inside one
      // hasNext(), and TimeoutStep is not re-entered until that call returns. The self-join below makes that single
      // call long enough to be unmistakable.
      setTimeout(50);

      assertThatThrownBy(() -> drainSql(
          "SELECT FROM Node WHERE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').size() > 1000000"))
          .as("the SELECT deadline has to be tested inside the filter loop, not between two batches")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    // ── The per-statement TIMEOUT clause ─────────────────────────────────────

    @Test
    void anExplicitSelectTimeoutClauseReachesTheInLoopChecks() {
      // The clause is the more common way to bound one query, and it is resolved by the planner rather than from
      // the configuration - so the guards, which read the deadline off the CommandContext, were blind to it. With
      // the global setting left disabled this is exactly the scenario the PR fixes, expressed the other way round.
      assertThatThrownBy(() -> drainSql(
          "SELECT FROM Node WHERE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').size() > 1000000 "
              + "TIMEOUT 50"))
          .as("a statement's own TIMEOUT must bound the scan loop, not only the gap between two batches")
          .hasStackTraceContaining("TIMEOUT clause of 50ms");
    }

    @Test
    void anExplicitUpdateTimeoutClauseReachesTheInLoopChecks() {
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
          "UPDATE Node SET touched = true WHERE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK')"
              + ".size() > 1000000 TIMEOUT 50")))
          .as("UPDATE ... TIMEOUT relies on the same filter loop and must be bounded by it too")
          .isInstanceOf(TimeoutException.class);
    }

    @Test
    void theReturnFailureStrategyStillReturnsRatherThanThrowing() {
      // TIMEOUT n RETURN asks for the rows produced so far instead of an exception. The deadline IS pinned for
      // that strategy, and the guard raises a PartialResultTimeoutException the owning step converts into the end
      // of its result set (issue #6304) - so widening the enforcement to the scan loop must still not turn a
      // documented "return what you have" into a failure. TimeoutFollowUps asserts the other half:
      // that it now stops early rather than grinding to the end of the scan first.
      assertThatCode(() -> drainSql(
          "SELECT FROM Node WHERE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').size() > 1000000 "
              + "TIMEOUT 50 RETURN"))
          .as("RETURN means return, at whatever granularity the deadline is observed")
          .doesNotThrowAnyException();
    }

    @Test
    void theGlobalCeilingStillAppliesUnderALooserStatementClause() {
      // A statement cannot lift the operator's safety net by asking for more time than the operator allows: the
      // two bounds are both in force and the earlier one wins.
      setTimeout(50);

      assertThatThrownBy(() -> drainSql(
          "SELECT FROM Node WHERE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').size() > 1000000 "
              + "TIMEOUT 3600000"))
          .as("the global deadline is a ceiling, not a default the statement may raise")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    // ── The default, and the shape of the bound ──────────────────────────────

    @Test
    void theDisabledDefaultLeavesEveryStatementAlone() {
      assertThat(database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT))
          .as("the setting is off by default, so none of the checks added for this issue may fire")
          .isZero();

      assertThatCode(() -> {
        assertThat(drainCypher("MATCH (a:Node) RETURN count(a) AS c")).hasSize(1);
        assertThat(drainCypher("MATCH (a:Node)-[:LINK*1..2]->(b:Node) RETURN b.v")).isNotEmpty();
        assertThat(drainSql("TRAVERSE out('LINK') FROM Node WHILE $depth < 2")).isNotEmpty();
        assertThat(drainSql("MATCH {type: Node, as: a}-LINK->{as: b} RETURN a, b")).isNotEmpty();
      }).doesNotThrowAnyException();
    }

    @Test
    void theCheckSitsInsideTheLoopNotBetweenTwoBatches() {
      // "It throws" is not enough on its own here: a check placed only between two batches would also eventually
      // throw, after grinding through the whole join. What separates the two is latency, so this is the one
      // assertion that has to be about time.
      //
      // The bound is a tripwire between a bounded operation and an unbounded one, not a performance budget:
      // the guarded run gives up in tens of milliseconds and the unguarded one takes seconds, so 10 s sits far
      // above the passing case and still cannot be reached without the check being effective.
      setTimeout(50);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> drainCypher("MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 RETURN count(*) AS c"))
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(10_000L, "a deadline observed inside the join loop from one observed after it");
    }

    @Test
    void theDeadlineIsTakenOnceAndSurvivesAContextCopy() {
      // FetchFromTypeExecutionStep.syncPullParallel hands each bucket-scan worker its own copy() of the context.
      // A copy that recomputed the deadline would let a type scanned across N buckets run for N times the budget.
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

      final CommandContext context = new BasicCommandContext().setDatabase(database);
      final long deadline = context.getCommandDeadline();

      assertThat(deadline).isNotEqualTo(Long.MAX_VALUE);
      assertThat(context.getCommandDeadline()).as("the clock is read once, not on every call").isEqualTo(deadline);
      assertThat(context.copy().getCommandDeadline()).as("a copy shares the budget, it does not restart it")
          .isEqualTo(deadline);

      final BasicCommandContext child = new BasicCommandContext();
      child.setParent(context);
      assertThat(child.getCommandDeadline()).as("a child context inherits rather than starting afresh")
          .isEqualTo(deadline);
    }

    @Test
    void aPinnedDeadlineIsHonouredWhateverItsValue() {
      // The "not resolved yet" marker sits outside the value domain rather than on a plausible instant, so no
      // value a caller pins can be mistaken for it. 0 is the one that matters: it means "already expired", and a
      // 0-as-unresolved marker would have discarded it and re-resolved from the configuration on the next read.
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);

      final CommandContext expired = new BasicCommandContext().setDatabase(database);
      expired.setCommandDeadline(0L, "a pinned deadline of 0ms");
      assertThat(expired.getCommandDeadline()).as("a pinned deadline in the past stays in the past").isZero();
      assertThatThrownBy(() -> WorkGuard.forCommandDeadline(expired).check())
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("a pinned deadline of 0ms");

      final CommandContext lifted = new BasicCommandContext().setDatabase(database);
      lifted.setCommandDeadline(Long.MAX_VALUE, "no bound");
      assertThatCode(() -> WorkGuard.forCommandDeadline(lifted).check())
          .as("and a pinned MAX_VALUE lifts the bound even though the setting is on")
          .doesNotThrowAnyException();
    }

    @Test
    void aDisabledTimeoutYieldsNoDeadlineAtAll() {
      final CommandContext context = new BasicCommandContext().setDatabase(database);

      assertThat(context.getCommandTimeout()).isZero();
      assertThat(context.getCommandDeadline())
          .as("the default must cost a comparison against a constant, never a clock read")
          .isEqualTo(Long.MAX_VALUE);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static void setTimeout(final long millis) {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, millis);
    }

    private static List<Result> drainCypher(final String query) {
      return drain("opencypher", query);
    }

    private static List<Result> drainSql(final String query) {
      return drain("sql", query);
    }

    private static List<Result> drain(final String language, final String query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query(language, query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Regression for #6304: follow-ups of the command timeout coverage (TIMEOUT RETURN, SELECT/UPDATE bound, MATCH/TRAVERSE clause, regex deadline).
   * <p>
   * Regression tests for the follow-ups filed as issue #6304 against #6266's fix.
   * <ul>
   *   <li><b>Item 2.</b> {@code TIMEOUT n RETURN} was the one clause shape left with the granularity hole #6266 set
   *   out to close: its deadline was deliberately not published to the in-loop guards, because a guard can only
   *   throw and {@code RETURN} promises no exception. It also never truncated anything - the step marked itself
   *   timed out and then went on returning rows.</li>
   *   <li><b>Item 3.</b> {@code SELECT ... TIMEOUT n} charged only time spent inside the pipeline while
   *   {@code UPDATE ... TIMEOUT n} was wall clock. Same syntax, two bounds.</li>
   *   <li><b>Item 4.</b> SQL {@code MATCH} and {@code TRAVERSE} accepted no {@code TIMEOUT} clause at all.</li>
   *   <li><b>Item 5.</b> The regex deadline lived in the context's opaque cache, which {@code copy()} does not
   *   carry, so every parallel bucket-scan worker started a fresh budget.</li>
   * </ul>
   * The counting SQL function is what makes item 2 assertable without measuring elapsed time: it counts the rows
   * the scan actually reached, so "stopped early" is a number rather than a stopwatch reading. A JVM stall can only
   * push that number down, never up, so the bound cannot flake in the direction that fails.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeoutFollowUps {
    /**
     * Enough rows that {@code ROWS * <cost of one tick>} is far longer than the 50 ms clause, so a scan that ran to
     * the end is unmistakable in the counter.
     */
    private static final int ROWS = 60_000;

    private static final String DB_PATH = "./target/databases/test-issue-6304-timeout-followups";

    private static final AtomicInteger TICKS = new AtomicInteger();

    /** Consumes what the burn loop produces, so the JIT cannot delete a computation nobody reads. */
    private static volatile long SINK;

    private static Database database;

    @BeforeAll
    static void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final MutableVertex[] vertices = new MutableVertex[ROWS];
        for (int i = 0; i < ROWS; i++)
          vertices[i] = database.newVertex("Node").set("v", i).save();
        // A small out-degree on the first slice is all MATCH and TRAVERSE need to have something to walk.
        for (int i = 0; i < 2_000; i++) {
          vertices[i].newEdge("LINK", vertices[(i + 1) % 2_000], true, (Object[]) null).save();
          vertices[i].newEdge("LINK", vertices[(i + 7) % 2_000], true, (Object[]) null).save();
          vertices[i].newEdge("LINK", vertices[(i + 31) % 2_000], true, (Object[]) null).save();
        }
      });

      ((SQLQueryEngine) database.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract("tick6304") {
        @Override
        public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult,
            final Object[] params, final CommandContext context) {
          // Deliberately burns a fixed slice of CPU: the clause has to become reachable long before the scan
          // ends, and a filter whose per-row cost rounds to nothing would finish 60k rows before any deadline
          // could be observed. A dependent chain rather than a sum, so no amount of unrolling collapses it.
          long sink = TICKS.incrementAndGet();
          for (int i = 0; i < 20_000; i++)
            sink = sink * 31 + i;
          SINK += sink;
          return params[0];
        }

        @Override
        public String getSyntax() {
          return "tick6304(<pass>)";
        }
      });
    }

    @AfterAll
    static void teardown() {
      if (database != null)
        database.drop();
    }

    @AfterEach
    void resetTimeout() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT,
          GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getDefValue());
    }

    // ── Item 2: TIMEOUT n RETURN ─────────────────────────────────────────────

    @Test
    void aReturnClauseStopsInsideTheScanRatherThanAfterIt() {
      // A filter that rejects every row yields nothing, so the whole scan happens inside one hasNext() and the
      // step owning the clause is not re-entered until it returns. "It came back without throwing" was already
      // true before the fix - after grinding through all 60k rows. The counter is what separates the two.
      TICKS.set(0);

      final List<Result> rows = drainSql("SELECT FROM Node WHERE tick6304(false) = true TIMEOUT 50 RETURN");

      assertThat(rows).as("a rejecting filter yields nothing whether or not it was cut short").isEmpty();
      assertThat(TICKS.get())
          .as("the RETURN clause has to be observed inside the filter loop, not only between two batches")
          .isLessThan(ROWS / 4);
    }

    @Test
    void aReturnClauseTruncatesTheRowsItHasAlreadyProduced() {
      // The other half of the same clause: when rows DO pass the filter, "return what you have" means the result
      // set ends. The step used to mark itself timed out and then go on returning every remaining row.
      TICKS.set(0);

      final List<Result> rows = drainSql("SELECT FROM Node WHERE tick6304(true) = true TIMEOUT 50 RETURN");

      assertThat(rows).as("RETURN returns the rows produced so far, which is fewer than all of them")
          .hasSizeLessThan(ROWS / 4);
    }

    @Test
    void aReturnClauseStillDoesNotThrow() {
      assertThatCode(() -> drainSql("SELECT FROM Node WHERE tick6304(true) = true TIMEOUT 50 RETURN"))
          .as("RETURN means return: enforcing it inside the loop must not turn it into a failure")
          .doesNotThrowAnyException();
    }

    @Test
    void anExceptionClauseOverTheSameQueryStillThrows() {
      assertThatThrownBy(() -> drainSql("SELECT FROM Node WHERE tick6304(true) = true TIMEOUT 50"))
          .as("the two strategies differ in what happens at the deadline, not in when it is observed")
          .hasStackTraceContaining("TIMEOUT clause of 50ms");
    }

    @Test
    void aPartialDeadlineMakesTheGuardYieldRatherThanFail() {
      final CommandContext yielding = new BasicCommandContext().setDatabase(database);
      yielding.setCommandDeadline(0L, "TIMEOUT clause of 50ms", true);
      assertThat(yielding.isCommandDeadlinePartial()).isTrue();
      assertThatThrownBy(() -> WorkGuard.forCommandDeadline(yielding).check())
          .as("only the owning step may see this one, and only it knows to end the result set")
          .isInstanceOf(PartialResultTimeoutException.class);

      final CommandContext failing = new BasicCommandContext().setDatabase(database);
      failing.setCommandDeadline(0L, "arcadedb.command.timeout of 50ms");
      assertThat(failing.isCommandDeadlinePartial()).isFalse();
      assertThatThrownBy(() -> WorkGuard.forCommandDeadline(failing).check())
          .as("every other bound keeps failing, and must not be swallowed into a truncated answer")
          .isInstanceOf(TimeoutException.class)
          .isNotInstanceOf(PartialResultTimeoutException.class);
    }

    @Test
    void whatTheDeadlineMeansIsInheritedWithTheDeadline() {
      // A nested plan gets a context of its own and inherits the instant; inheriting the instant without what it
      // means would abort the subquery with the exception the clause promised not to raise.
      final CommandContext parent = new BasicCommandContext().setDatabase(database);
      parent.setCommandDeadline(0L, "TIMEOUT clause of 50ms", true);

      final BasicCommandContext child = new BasicCommandContext();
      child.setParent(parent);
      assertThat(child.isCommandDeadlinePartial()).isTrue();
      assertThat(child.getCommandDeadline()).isZero();

      assertThat(parent.copy().isCommandDeadlinePartial()).as("and it survives the copy a parallel worker gets")
          .isTrue();
    }

    @Test
    void aStricterCeilingIsNotSoftenedByALooserReturnClause() {
      // The global setting is a ceiling over every statement. A RETURN clause asking for more time than it allows
      // must not replace "fail at the ceiling" with "quietly return fewer rows".
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 50L);

      assertThatThrownBy(() -> drainSql("SELECT FROM Node WHERE tick6304(true) = true TIMEOUT 3600000 RETURN"))
          .as("the operator's bound still fails, whatever failure strategy the statement asked for")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void aClauseDoesNotOutliveTheScriptLineThatWroteIt() {
      // Every line of a script is planned against the same CommandContext, so the instant a TIMEOUT clause pins
      // lands exactly where the following lines read it - and those have no TimeoutStep of their own to catch
      // anything. The second line below carries no clause and must not inherit the first line's.
      //
      // The first line is LIMIT 1 so that it cannot reach its own clause: what is under test is the pin it leaves
      // behind, not whether it hits it.
      TICKS.set(0);

      assertThatCode(() -> drainScript(
          "SELECT FROM Node LIMIT 1 TIMEOUT 50;\nSELECT FROM Node WHERE tick6304(true) = true;\n"))
          .as("a bound belonging to a statement that has already finished must not abort the next one")
          .doesNotThrowAnyException();

      assertThat(TICKS.get()).as("and the second line runs in full, rather than being cut short by it")
          .isEqualTo(ROWS);
    }

    @Test
    void aScriptLineIsStillBoundedByTheGlobalCeiling() {
      // Restoring the line's snapshot must put back the command's own deadline, not clear it: the script is one
      // command, and arcadedb.command.timeout bounds all of it.
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 50L);

      assertThatThrownBy(() -> drainScript(
          "SELECT FROM Node LIMIT 1 TIMEOUT 40;\nSELECT FROM Node WHERE tick6304(true) = true;\n"))
          .as("the operator's ceiling survives the line that narrowed it for itself")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void aRetryBlockDoesNotRetryAnExpiredReturnClause() {
      // RETRY catches TimeoutException because the usual source is transient lock contention on commit. A deadline
      // that has already passed is not transient, and a RETURN clause asks to yield rather than to fail: retried,
      // it would roll back, spend the whole retry budget on attempts that expire on their first check, and hand
      // back an empty result set instead of the rows the clause promised. The body of a RETRY block is a script of
      // its own, so a clause on one of its lines is scoped the same way.
      TICKS.set(0);

      assertThatCode(() -> drainScript("BEGIN;\n"
          + "SELECT FROM Node LIMIT 1 TIMEOUT 50 RETURN;\n"
          + "SELECT FROM Node WHERE tick6304(true) = true;\n"
          + "COMMIT RETRY 10;\n"))
          .doesNotThrowAnyException();

      assertThat(TICKS.get())
          .as("ten retries of an already-expired deadline would each re-scan; one pass is what this must cost")
          .isEqualTo(ROWS);
    }

    // ── Item 3: one meaning for the word ─────────────────────────────────────

    @Test
    void aSelectClauseIsWallClockLikeEveryOtherBoundInTheEngine() throws Exception {
      // SELECT used to charge only the time spent inside the pipeline, so a consumer that paused between two
      // fetches was not billed for the pause while the identical number on an UPDATE was. Sleeping longer than the
      // clause can only make this assertion more true, so a JVM stall cannot flake it.
      try (final ResultSet rs = database.query("sql", "SELECT FROM Node TIMEOUT 300")) {
        assertThat(rs.hasNext()).isTrue();
        rs.next();
        Thread.sleep(600);

        assertThatThrownBy(() -> {
          while (rs.hasNext())
            rs.next();
        }).as("the clause means wall clock, the same as arcadedb.command.timeout and as UPDATE ... TIMEOUT")
            .isInstanceOf(TimeoutException.class);
      }
    }

    @Test
    void everyStatementKindRendersTheSameStep() {
      assertThat(explainOf("SELECT FROM Node WHERE v > 0 TIMEOUT 1234")).contains("+ TIMEOUT (1234ms)");
      assertThat(explainOf("UPDATE Node SET touched = true WHERE v < 0 TIMEOUT 1234")).contains("+ TIMEOUT (1234ms)");
    }

    @Test
    void theGlobalSettingIsNotRenderedAsAClauseNobodyWrote() {
      // SELECT used to synthesize a Timeout out of arcadedb.command.timeout when the statement carried none - the
      // only way the setting reached SELECT before #6266. It made SELECT the odd one out twice over: EXPLAIN
      // showed a step for it and not for the other statement kinds under the same setting, and when the bound
      // fired the message called it a "TIMEOUT clause" on a statement whose author wrote no clause.
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 50L);

      assertThat(explainOf("SELECT FROM Node WHERE v > 0")).doesNotContain("+ TIMEOUT");

      assertThatThrownBy(() -> drainSql("SELECT FROM Node WHERE tick6304(true) = true"))
          .as("the setting still bounds the statement, and now says so in its own name")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
    }

    @Test
    void aZeroClauseDisablesTheBoundOnEveryStatementKind() {
      // 0 disables, as it does for arcadedb.command.timeout and arcadedb.command.regexTimeout. SELECT used to be
      // the exception - it chained the step for any clause at all, so its deadline landed in the past and the
      // first pull failed - while UPDATE ... TIMEOUT 0 ran unbounded. One rule now, and it is the one that cannot
      // turn a working statement into a failing one.
      for (final String query : new String[] {
          "SELECT FROM Node WHERE v = 1 TIMEOUT 0",
          "MATCH {type: Node, as: a}-LINK->{as: b} RETURN a, b TIMEOUT 0",
          "TRAVERSE out('LINK') FROM Node WHILE $depth < 2 TIMEOUT 0" })
        assertThat(explainOf(query)).as(query).doesNotContain("+ TIMEOUT");

      assertThatCode(() -> drainSql("SELECT FROM Node WHERE v = 1 TIMEOUT 0"))
          .as("a clause that bounds nothing must not abort the statement it is written on")
          .doesNotThrowAnyException();
    }

    // ── Item 4: MATCH and TRAVERSE accept the clause ─────────────────────────

    @Test
    void sqlMatchAcceptsATimeoutClause() {
      assertThat(explainOf(
          "MATCH {type: Node, as: a}-LINK->{as: b}-LINK->{as: c} RETURN a, c TIMEOUT 1234"))
          .as("the clause has to reach the plan, not merely parse")
          .contains("+ TIMEOUT (1234ms)");

      assertThatThrownBy(() -> drainSql(
          "MATCH {type: Node, as: a}-LINK->{as: b}-LINK->{as: c}-LINK->{as: d}-LINK->{as: e} RETURN a, e TIMEOUT 50"))
          .as("bounding one expensive MATCH used to mean changing a database-wide setting")
          .hasStackTraceContaining("TIMEOUT clause of 50ms");
    }

    @Test
    void sqlTraverseAcceptsATimeoutClause() {
      assertThat(explainOf("TRAVERSE out('LINK') FROM Node WHILE $depth < 2 TIMEOUT 1234"))
          .contains("+ TIMEOUT (1234ms)");

      assertThatThrownBy(() -> drainSql(
          "TRAVERSE out('LINK').out('LINK').out('LINK').out('LINK').out('LINK').out('LINK') FROM Node "
              + "WHILE $depth < 12 TIMEOUT 50"))
          .hasStackTraceContaining("TIMEOUT clause of 50ms");
    }

    @Test
    void theClauseSurvivesTheStatementsOwnRoundTrip() {
      // Statement.toString() is what the execution-plan cache and the profiler render, and a clause the AST holds
      // but cannot print would come back changed.
      assertThat(reparse("MATCH {type: Node, as: a}-LINK->{as: b} RETURN a, b TIMEOUT 1234"))
          .contains("TIMEOUT 1234");
      assertThat(reparse("TRAVERSE out('LINK') FROM Node WHILE $depth < 2 TIMEOUT 1234 RETURN"))
          .contains("TIMEOUT 1234 RETURN");
    }

    @Test
    void aMatchThatYieldsTruncatesInsteadOfRaising() {
      // The one shape that reaches the deadline from inside next() rather than hasNext(): MatchStep.next() pulls
      // the following candidate before returning the current one, and that pull is guarded. A RETURN clause has to
      // end the result set there too - not raise, and not report NoSuchElementException to a caller that
      // hasNext() has just promised a row to.
      final String pattern = "MATCH {type: Node, as: a}-LINK->{as: b}-LINK->{as: c}-LINK->{as: d} RETURN a, d";

      final List<Result> truncated = new ArrayList<>();
      assertThatCode(() -> truncated.addAll(drainSql(pattern + " TIMEOUT 50 RETURN")))
          .as("a MATCH that runs past its own RETURN clause yields what it has")
          .doesNotThrowAnyException();

      assertThatThrownBy(() -> drainSql(pattern + " TIMEOUT 50"))
          .as("and the same MATCH under the default strategy still raises, so the clause was really reached")
          .hasStackTraceContaining("TIMEOUT clause of 50ms");
    }

    // ── Item 5: one regex budget per command ─────────────────────────────────

    @Test
    void theRegexDeadlineIsTakenOnceAndSurvivesAContextCopy() {
      // FetchFromTypeExecutionStep.syncPullParallel hands each bucket-scan worker its own copy() of the context.
      // The deadline used to live in the opaque value cache, which copy() does not carry, so a type scanned across
      // N buckets was bounded by N * regexTimeout.
      final CommandContext context = new BasicCommandContext().setDatabase(database);
      final long deadline = context.getRegexDeadline();

      assertThat(deadline).as("the setting is enabled by default, so there is a real deadline to share")
          .isNotEqualTo(Long.MAX_VALUE);
      assertThat(context.getRegexDeadline()).as("the clock is read once, not on every call").isEqualTo(deadline);
      assertThat(context.copy().getRegexDeadline()).as("a worker shares the budget, it does not restart it")
          .isEqualTo(deadline);

      final BasicCommandContext child = new BasicCommandContext();
      child.setParent(context);
      assertThat(child.getRegexDeadline()).as("a nested plan inherits rather than starting afresh")
          .isEqualTo(deadline);
    }

    @Test
    void everyRegexEntryPointOfOneCommandSharesOneBudget() {
      // One deadline per command, not one per feature: the value is a field of the context now, so two call sites
      // that used to pick their own cache keys cannot end up with two budgets.
      final CommandContext context = new BasicCommandContext().setDatabase(database);
      assertThat(context.getRegexDeadline()).isEqualTo(context.getRegexDeadline());
    }

    @Test
    void aDisabledRegexTimeoutCostsNoClockRead() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_REGEX_TIMEOUT, 0L);

      final CommandContext context = new BasicCommandContext().setDatabase(database);
      assertThat(context.getRegexDeadline())
          .as("TimeBoundRegex reads no clock for a non-positive timeout, so neither does resolving it")
          .isEqualTo(TimeBoundRegex.newDeadline(0));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static List<Result> drainSql(final String query) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }

    private static List<Result> drainScript(final String script) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.command("sqlscript", script)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }

    private static String explainOf(final String query) {
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
        assertThat(rs.hasNext()).as(query).isTrue();
        return rs.next().getProperty("executionPlanAsString");
      }
    }

    /** The statement as the parser renders it back, which is how a plan cache and the profiler see it. */
    private static String reparse(final String query) {
      final SQLLexer lexer = new SQLLexer(CharStreams.fromString(query));
      final SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
      return new SQLASTBuilder().visitParse(parser.parse()).toString();
    }
  }

  /**
   * Regression for #6459: graph path-finding functions honour the command timeout.
   * <p>
   * Regression tests for issue #6459 - {@code arcadedb.command.timeout} is purely cooperative, but the graph
   * path-finding functions consulted it nowhere: {@code astar()}/{@code dijkstra()} had no deadline or interrupt
   * check at all, SQL {@code shortestPath()} checked only a thread interrupt (never the deadline), and the Cypher
   * constrained {@code shortestPath()}/{@code allShortestPaths()} BFS (the edge-property / inline-{@code WHERE}
   * fallback) did the same - unlike the unconstrained BFS right next to it, which was already guarded.
   * <p>
   * Each test below reuses the graph shape proven effective by {@link CommandTimeoutCoverage}: a
   * 20,000-node ring with two chords per node (out-degree 3, so every node has both directions once the edges are
   * made bidirectional). A check placed inside the loop aborts almost immediately, while one placed nowhere (or only
   * between unrelated checkpoints) runs to graph exhaustion first, which is exactly the distinction the issue is
   * about.
   * <p>
   * The deadline has to stay well under what one exhausting pass costs, and that cost is not a constant: the first
   * pass over this fixture pays for the page cache, later ones do not. A warm exhaustion of one 20,000-node
   * component measures ~55 ms here, so the 50 ms this class used to arm was not a bound at all for whichever test
   * ran second - {@code astar()} aborted only because it ran first and cold, and {@code dijkstra()} right behind it
   * drained all 20,000 nodes and returned normally with 5 ms to spare. {@link #ABORT_DEADLINE_MS} is an order of
   * magnitude below the warm figure so the guard fires whatever the cache holds, and the assertions name the guard
   * that has to fire rather than only the setting, so a deadline tripped somewhere else in the plan cannot stand in
   * for the check under test.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphPathfindingCommandTimeout {
    private static final int NODES = 20_000;
    /** See the class comment: an order of magnitude below a warm exhausting pass over one component (~55 ms), so
     *  the outcome does not depend on whether an earlier test already warmed the page cache. An implementation that
     *  consults no deadline still runs to exhaustion and returns normally, so the regression stays caught. */
    private static final long ABORT_DEADLINE_MS = 5;
    /** A vertex with no edges at all: astar()/dijkstra() can never reach it, so they have to exhaust the whole
     *  20,000-node component before giving up - unless the deadline stops them first. */
    private static final int UNREACHABLE_IDX = NODES;

    private static final String DB_PATH = "./target/databases/test-issue-6459-pathfinding-command-timeout";

    private static Database database;
    private static RID[]     nodeRid;
    private static RID       unreachableRid;
    /** A second ring, the same size and shape as the first but sharing no edge with it, so a bidirectional
     *  meet-in-the-middle BFS between the two components can never meet: each side has to exhaust its own
     *  20,000-node component before its frontier goes empty. A far-but-reachable pair on ONE chorded ring does
     *  not do this - the out-degree-3 branching plus the two long chords give it small-world diameter, so a
     *  bidirectional search finds a real path within a handful of layers regardless of the guard. */
    private static RID[]     otherComponentRid;

    @BeforeAll
    static void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node").createProperty("v", Type.INTEGER);
      database.getSchema().createEdgeType("LINK");
      database.getSchema().getType("Node").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "v");

      nodeRid = new RID[NODES];
      otherComponentRid = new RID[NODES];

      database.transaction(() -> {
        nodeRid = buildRing(NODES, 0);
        otherComponentRid = buildRing(NODES, NODES + 1);

        unreachableRid = database.newVertex("Node").set("v", UNREACHABLE_IDX).save().getIdentity();
      });
    }

    /**
     * Ring plus two chords per node, exactly as {@link CommandTimeoutCoverage}: out-degree 3, and
     * bidirectional (the {@code true} argument) so every node also has in-edges. {@code vBase} offsets the {@code v}
     * property so two rings built back to back get disjoint values and share no vertex.
     */
    private static RID[] buildRing(final int size, final int vBase) {
      final MutableVertex[] vertices = new MutableVertex[size];
      for (int i = 0; i < size; i++)
        vertices[i] = database.newVertex("Node").set("v", vBase + i).save();

      for (int i = 0; i < size; i++) {
        vertices[i].newEdge("LINK", vertices[(i + 1) % size], true, new Object[] { "w", 1.0 }).save();
        vertices[i].newEdge("LINK", vertices[(i + 7) % size], true, new Object[] { "w", 1.0 }).save();
        vertices[i].newEdge("LINK", vertices[(i + 31) % size], true, new Object[] { "w", 1.0 }).save();
      }

      final RID[] rids = new RID[size];
      for (int i = 0; i < size; i++)
        rids[i] = vertices[i].getIdentity();
      return rids;
    }

    @AfterAll
    static void teardown() {
      if (database != null)
        database.drop();
    }

    @AfterEach
    void resetTimeout() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
      // A test that arms the interrupt flag must not leave it set for whatever runs next on this thread.
      Thread.interrupted();
    }

    // ── astar() / dijkstra(): had no timeout or interrupt check at all ────────

    @Test
    @Tag("slow")
    @Timeout(120)
    void astarHonoursTheCommandTimeoutOnAnUnreachableDestination() {
      setTimeout(ABORT_DEADLINE_MS);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> drainSql(
          "select expand(astar(" + nodeRid[0] + ", " + unreachableRid + ", 'w'))"))
          .as("astar() previously consulted neither the deadline nor a thread interrupt, so it explored the "
              + "whole reachable component before ever answering")
          // Naming the guard, not just the setting: every other deadline check in the plan aborts as "the command",
          // so only this spelling says the check inside astar()'s own loop is the one that fired.
          .hasStackTraceContaining("astar() exceeded the " + GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(60_000L, "astar() aborted from inside its main loop, not after exhausting the graph");
    }

    @Test
    @Tag("slow")
    @Timeout(120)
    void dijkstraHonoursTheCommandTimeoutOnAnUnreachableDestination() {
      // dijkstra() delegates straight to a fresh SQLFunctionAstar, so it shares the fix as well as the bug - and
      // the guard it inherits names itself astar() when it aborts.
      setTimeout(ABORT_DEADLINE_MS);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> drainSql(
          "select expand(dijkstra(" + nodeRid[0] + ", " + unreachableRid + ", 'w'))"))
          .as("dijkstra() must not survive its own unreachable-destination search past the deadline")
          .hasStackTraceContaining("astar() exceeded the " + GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(60_000L, "dijkstra() aborted from inside astar()'s main loop");
    }

    @Test
    void astarStillReturnsTheCorrectPathWhenNothingAborts() {
      final List<Result> rows = drainSql("select expand(astar(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))");
      assertThat(rows).as("a healthy call must still find a path once the guard is in place").isNotEmpty();
      assertThat(rows.getFirst().getIdentity()).contains(nodeRid[0]);
    }

    @Test
    void dijkstraStillReturnsTheCorrectPathWhenNothingAborts() {
      final List<Result> rows = drainSql("select expand(dijkstra(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))");
      assertThat(rows).as("a healthy call must still find a path once the guard is in place").isNotEmpty();
      assertThat(rows.getFirst().getIdentity()).contains(nodeRid[0]);
    }

    @Test
    @Timeout(30)
    void astarStopsOnAThreadInterruptEvenWithoutADeadline() {
      // Before this fix astar() consulted no interrupt either - a caller had no way at all to cancel a run.
      Thread.currentThread().interrupt();

      assertThatThrownBy(() -> drainSql("select expand(astar(" + nodeRid[0] + ", " + unreachableRid + ", 'w'))"))
          .as("astar() must be at least abortable via an explicit cancel")
          .hasStackTraceContaining("astar() has been interrupted");
    }

    // ── SQL shortestPath(): checked Thread.interrupted() but never the deadline ─

    @Test
    @Tag("slow")
    @Timeout(120)
    void sqlShortestPathHonoursTheCommandTimeoutOnAnUnreachableDestination() {
      setTimeout(50);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      // Both endpoints sit on a fully separate ring: the search can never meet, so walkLeft/walkRight together
      // have to exhaust both 20,000-node components before the outer loop notices the frontiers are empty -
      // unless the deadline stops them first.
      assertThatThrownBy(() -> drainSql(
          "select expand(shortestPath(" + nodeRid[0] + ", " + otherComponentRid[0] + "))"))
          .as("shortestPath() checked Thread.interrupted() but never arcadedb.command.timeout")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(60_000L, "the bidirectional BFS aborted from inside walkLeft/walkRight's caller");
    }

    @Test
    void sqlShortestPathStillReturnsAPathWhenNothingAborts() {
      final List<Result> rows = drainSql("select expand(shortestPath(" + nodeRid[0] + ", " + nodeRid[3] + "))");
      assertThat(rows).as("a healthy call must still find a path once the guard is in place").isNotEmpty();
    }

    // ── Cypher MATCH shortestPath()/allShortestPaths(): the constrained BFS ────
    // fallback (an inline WHERE or property map on the relationship) had neither check, unlike the unconstrained
    // BFS right next to it.

    @Test
    @Tag("slow")
    @Timeout(120)
    void cypherConstrainedShortestPathHonoursTheCommandTimeout() {
      setTimeout(50);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> drainCypher(
          "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = shortestPath((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
          Map.of("src", 0, "dst", NODES / 2)))
          .as("an inline WHERE forces the edge-aware BFS (computeFilteredShortestPath), which consulted neither "
              + "check before this fix")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(60_000L, "the constrained frontier walk aborted from inside its own loop");
    }

    @Test
    @Tag("slow")
    @Timeout(120)
    void cypherConstrainedAllShortestPathsHonoursTheCommandTimeout() {
      setTimeout(50);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> drainCypher(
          "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = allShortestPaths((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
          Map.of("src", 0, "dst", NODES / 2)))
          .as("computeFilteredAllShortestPaths shares the same gap as its computeFilteredShortestPath sibling")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(60_000L, "the constrained layered BFS aborted from inside its own loop");
    }

    @Test
    @Tag("slow")
    @Timeout(120)
    void cypherAllShortestPathsExpressionHonoursTheCommandTimeout() {
      setTimeout(50);

      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> drainCypher(
          "MATCH (a:Node {v: $src}), (b:Node {v: $dst}) RETURN allShortestPaths((a)-[:LINK*]-(b)) AS p",
          Map.of("src", 0, "dst", NODES / 2)))
          .as("the RETURN-position allShortestPaths() runs the same guarded layered BFS as the MATCH form (issue #8995)")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());
      stopwatch.assertGaveUpWithin(60_000L, "the expression-form layered BFS aborted from inside its own loop");
    }

    @Test
    void cypherShortestPathFormsStillReturnCorrectPathsWhenNothingAborts() {
      assertThat(drainCypher(
          "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = shortestPath((a)-[:LINK*]-(b)) RETURN p",
          Map.of("src", 0, "dst", 3)))
          .as("the unconstrained MATCH form must still find a path")
          .isNotEmpty();

      assertThat(drainCypher(
          "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = shortestPath((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
          Map.of("src", 0, "dst", 3)))
          .as("the constrained MATCH form (inline WHERE) must still find a path")
          .isNotEmpty();

      assertThat(drainCypher(
          "MATCH (a:Node {v: $src}), (b:Node {v: $dst}), p = allShortestPaths((a)-[r:LINK* WHERE r.w > 0]-(b)) RETURN p",
          Map.of("src", 0, "dst", 3)))
          .as("the constrained allShortestPaths() form must still find at least one path")
          .isNotEmpty();

      assertThat(drainCypher(
          "MATCH (a:Node {v: $src}), (b:Node {v: $dst}) RETURN shortestPath((a)-[r:LINK* WHERE r.w > 0]-(b)) AS p",
          Map.of("src", 0, "dst", 3)))
          .as("the RETURN-position expression form (ShortestPathExpression) shares the same fix")
          .isNotEmpty();
    }

    // ── The default, disabled by default, leaves every form alone ─────────────

    @Test
    void theDisabledDefaultLeavesEveryFormAlone() {
      assertThat(database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT))
          .as("the setting is off by default, so none of the checks added for this issue may fire")
          .isZero();

      assertThat(drainSql("select expand(astar(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))")).isNotEmpty();
      assertThat(drainSql("select expand(dijkstra(" + nodeRid[0] + ", " + nodeRid[3] + ", 'w'))")).isNotEmpty();
      assertThat(drainSql("select expand(shortestPath(" + nodeRid[0] + ", " + nodeRid[3] + "))")).isNotEmpty();
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static void setTimeout(final long millis) {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, millis);
    }

    private static List<Result> drainSql(final String query) {
      return drain("sql", query, Map.of());
    }

    private static List<Result> drainCypher(final String query) {
      return drain("opencypher", query, Map.of());
    }

    private static List<Result> drainCypher(final String query, final Map<String, Object> params) {
      return drain("opencypher", query, params);
    }

    private static List<Result> drain(final String language, final String query, final Map<String, Object> params) {
      final List<Result> rows = new java.util.ArrayList<>();
      try (final ResultSet rs = params.isEmpty() ? database.query(language, query) : database.query(language, query, params)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Regression for #8313: a forwarded command timeout budget overrides the database setting.
   * <p>
   * Issue #8313: a write a follower forwards to the leader waits for the {@code arcadedb.command.timeout} budget the
   * follower resolved, and the leader used to enforce whatever its OWN database configuration held. The budget now
   * travels with the forward and the leader publishes it through {@link CommandTimeoutOverride}, which every command
   * context reads at its root in place of the database setting. These cases pin the engine half: the published budget
   * is the one a command runs under, whatever the language, and nothing of it outlives {@link CommandTimeoutOverride#clear()}.
   * <p>
   * The self-join below walks {@code NODES^2} pairs, orders of magnitude more than the 50 ms budget it is given; the
   * assertion is on the setting the abort names, not on elapsed time.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ForwardedCommandTimeoutOverride {
    private static final int    NODES   = 3_000;
    private static final String DB_PATH = "./target/databases/test-issue-8313-command-timeout-override";

    private static Database database;

    @BeforeAll
    static void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.transaction(() -> {
        for (int i = 0; i < NODES; i++)
          database.newVertex("Node").set("v", i).save();
      });
    }

    @AfterAll
    static void teardown() {
      if (database != null)
        database.drop();
    }

    @AfterEach
    void clear() {
      CommandTimeoutOverride.clear();
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    }

    @Test
    void thePublishedBudgetTakesThePlaceOfTheDatabaseSetting() {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);
      CommandTimeoutOverride.set(1_234L);

      assertThat(new BasicCommandContext().setDatabase((DatabaseInternal) database).getCommandTimeout()).isEqualTo(1_234L);
      assertThat(CommandTimeoutOverride.effectiveTimeout((DatabaseInternal) database)).isEqualTo(1_234L);

      CommandTimeoutOverride.clear();
      assertThat(new BasicCommandContext().setDatabase((DatabaseInternal) database).getCommandTimeout())
          .as("once cleared, the database's own setting applies again")
          .isEqualTo(60_000L);
    }

    @Test
    void aSqlCommandRunsUnderThePublishedBudgetWhenTheDatabaseHasNone() {
      CommandTimeoutOverride.set(50L);

      assertThatThrownBy(() -> drain("sql",
          "MATCH {type: Node, as: a}, {type: Node, as: b, where: (v + $matched.a.v = -1)} RETURN a.v, b.v"))
          .as("the leader has no budget of its own, and must still enforce the one the follower waits for")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of 50ms");
    }

    @Test
    void anOpenCypherCommandRunsUnderThePublishedBudgetToo() {
      CommandTimeoutOverride.set(50L);

      assertThatThrownBy(() -> drain("opencypher", "MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 RETURN count(*) AS c"))
          .as("the budget is read where every command context resolves it, not by one language only")
          .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey() + " of 50ms");
    }

    @Test
    void onlyAPositiveBudgetIsAccepted() {
      assertThat(CommandTimeoutOverride.parse("250")).isEqualTo(250L);
      assertThat(CommandTimeoutOverride.parse(" 250 ")).isEqualTo(250L);
      assertThat(CommandTimeoutOverride.parse("0")).isEqualTo(-1L);
      assertThat(CommandTimeoutOverride.parse("-5")).isEqualTo(-1L);
      assertThat(CommandTimeoutOverride.parse("soon")).isEqualTo(-1L);
      assertThat(CommandTimeoutOverride.parse(null)).isEqualTo(-1L);

      CommandTimeoutOverride.set(0L);
      assertThat(CommandTimeoutOverride.get()).as("0 is 'unbounded' for the setting, never a budget to publish").isEqualTo(-1L);
    }

    private static void drain(final String language, final String query) {
      try (final ResultSet rs = database.query(language, query)) {
        while (rs.hasNext())
          rs.next();
      }
    }
  }

  /**
   * Regression for #10609: the command context captures the forwarded timeout on the thread that builds it.
   * <p>
   * Issue #10609: the budget a request publishes with {@link CommandTimeoutOverride} (the Postgres {@code statement_timeout}, the
   * budget a follower forwards) lives in a thread-local, and a command context resolves it lazily, on the first thread that asks.
   * A parallel scan hands the context to workers that carry no budget, so a worker that was first to ask resolved "no timeout" for
   * the whole command: a 1 ms {@code statement_timeout} was answered with the result of a 9 second statement. The context captures
   * the budget when the thread that serves the request builds it, as it does for the registry entry of the statement (#9680).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ForwardedTimeoutCapturedByTheBuilder extends TestHelper {

    @AfterEach
    void clear() {
      CommandTimeoutOverride.clear();
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    }

    @Test
    void aWorkerThatResolvesTheContextFirstSeesTheBudgetOfTheThreadThatBuiltIt() throws Exception {
      CommandTimeoutOverride.set(1_234L);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      // the request ends on its own thread without anything having asked the context for its budget
      CommandTimeoutOverride.clear();

      final AtomicLong timeout = new AtomicLong();
      final AtomicLong deadline = new AtomicLong();
      final Thread worker = new Thread(() -> {
        timeout.set(context.getCommandTimeout());
        deadline.set(context.getCommandDeadline());
      });
      worker.start();
      worker.join();

      assertThat(timeout.get()).isEqualTo(1_234L);
      assertThat(deadline.get()).as("a deadline, not 'no deadline'").isNotEqualTo(Long.MAX_VALUE);
      assertThat(context.getCommandTimeout()).as("one value for every thread afterwards").isEqualTo(1_234L);
    }

    @Test
    void aCopyMadeByAWorkerKeepsTheBudgetToo() throws Exception {
      CommandTimeoutOverride.set(2_345L);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      CommandTimeoutOverride.clear();

      final AtomicLong copyTimeout = new AtomicLong();
      final Thread worker = new Thread(() -> {
        final CommandContext copy = context.copy();
        copyTimeout.set(copy.getCommandTimeout());
      });
      worker.start();
      worker.join();

      assertThat(copyTimeout.get()).isEqualTo(2_345L);
    }

    @Test
    void aContextBuiltWithoutABudgetFallsBackToTheDatabaseSetting() throws Exception {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 60_000L);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);

      final AtomicLong timeout = new AtomicLong();
      final Thread worker = new Thread(() -> timeout.set(context.getCommandTimeout()));
      worker.start();
      worker.join();

      assertThat(timeout.get()).isEqualTo(60_000L);
    }

    @Test
    void theBudgetOfTheBuilderBeatsTheLaterBudgetOfTheResolvingThread() throws Exception {
      CommandTimeoutOverride.set(500L);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);

      final AtomicLong timeout = new AtomicLong();
      final Thread worker = new Thread(() -> {
        CommandTimeoutOverride.set(9_999L);
        try {
          timeout.set(context.getCommandTimeout());
        } finally {
          CommandTimeoutOverride.clear();
        }
      });
      worker.start();
      worker.join();

      assertThat(timeout.get()).as("the command is the request of the thread that built it, whoever asks").isEqualTo(500L);
    }
  }
}
