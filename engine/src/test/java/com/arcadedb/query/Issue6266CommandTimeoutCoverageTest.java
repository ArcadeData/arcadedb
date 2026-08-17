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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue6266CommandTimeoutCoverageTest {
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
    // documented "return what you have" into a failure. Issue6304TimeoutFollowUpsTest asserts the other half:
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
