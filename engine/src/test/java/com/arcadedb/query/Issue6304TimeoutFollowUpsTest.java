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
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.PartialResultTimeoutException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.antlr.SQLASTBuilder;
import com.arcadedb.query.sql.grammar.SQLLexer;
import com.arcadedb.query.sql.grammar.SQLParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.TimeBoundRegex;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue6304TimeoutFollowUpsTest {
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
