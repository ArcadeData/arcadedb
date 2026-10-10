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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9680: a statement registered in a {@link RunningQueryRegistry} stops when it is terminated, wherever its work
 * happens to be - an UNWIND feeding an aggregation, a MATCH, a correlated subquery, a statement carrying its own
 * {@code TIMEOUT ... RETURN} - and what it wrote is not committed.
 * <p>
 * Every statement here needs far longer than the test waits before terminating it (seconds to tens of seconds when left
 * alone), so a run that ends in time and with {@link QueryTerminatedException} can only have been stopped by the
 * termination.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9680RunningQueryTerminationTest {
  private static final int    NODES   = 5_000;
  private static final String DB_PATH = "./target/databases/test-issue-9680-running-query-termination";

  /** The statement of the issue: about 16 s on one core, all of it inside the aggregation's drain of the two UNWINDs. */
  private static final String CYPHER_UNWIND_SUM =
      "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

  private static Database             database;
  private final  RunningQueryRegistry registry = new RunningQueryRegistry();

  @BeforeAll
  static void setup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createDocumentType("Written");
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
  void resetTimeout() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
  }

  @Test
  void cypherUnwindIntoAggregationStopsWhenTerminated() {
    terminateWhileRunning("opencypher", CYPHER_UNWIND_SUM, q -> drain(database.query("opencypher", CYPHER_UNWIND_SUM)));
  }

  @Test
  void cypherCartesianMatchStopsWhenTerminated() {
    final String query = "MATCH (a:Node), (b:Node) WHERE a.v + b.v = -1 RETURN count(*) AS c";
    terminateWhileRunning("opencypher", query, q -> drain(database.query("opencypher", query)));
  }

  @Test
  void cypherForeachStopsWhenTerminatedAndWritesNothing() {
    // Every element runs the inner SET on a plan of its own: nothing loops over the list but the FOREACH itself
    final String query = "MATCH (n:Node {v: 0}) FOREACH (i IN range(1, 50000000) | SET n.x9680 = i)";
    terminateWhileRunning("opencypher", query, q -> database.transaction(() -> drain(database.command("opencypher", query))));
    try (final ResultSet rs = database.query("sql", "SELECT x9680 FROM Node WHERE v = 0")) {
      assertThat(rs.next().<Object>getProperty("x9680")).as("the terminated statement must not commit its writes").isNull();
    }
  }

  @Test
  void sqlMatchStopsWhenTerminated() {
    final String query = "MATCH {type: Node, as: a}, {type: Node, as: b, where: (v + $matched.a.v = -1)} RETURN a.v";
    terminateWhileRunning("sql", query, q -> drain(database.query("sql", query)));
  }

  @Test
  void sqlCorrelatedSubqueryStopsWhenTerminated() {
    // One full scan of Node per outer row: the work is inside the subquery, on contexts of its own
    final String query =
        "SELECT FROM Node WHERE (SELECT count(*) AS c FROM Node WHERE v = $parent.$current.v + 1000000)[0].c > 0";
    terminateWhileRunning("sql", query, q -> drain(database.query("sql", query)));
  }

  @Test
  void timeoutReturnClauseDoesNotTurnTerminationIntoPartialResults() {
    // A TIMEOUT ... RETURN clause answers its own deadline with the rows produced so far. A termination is not that
    // deadline: the statement must fail, not return as if it had been given what it asked for.
    final String query = "SELECT FROM Node WHERE (SELECT count(*) AS c FROM Node WHERE v = $parent.$current.v + 1000000)[0].c > 0"
        + " TIMEOUT 600000 RETURN";
    terminateWhileRunning("sql", query, q -> drain(database.query("sql", query)));
  }

  @Test
  void terminatedStatementCommitsNothing() {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null)) {
      q.terminate("root");
      try {
        database.transaction(() -> database.newDocument("Written").set("x", 1).save());
      } catch (final Throwable e) {
        failure.set(e);
      }
      assertThat(q.getOutcome()).isEqualTo(RunningQuery.Outcome.RUNNING);
    }

    assertThat(failure.get()).as("the commit of a terminated statement must be refused").isNotNull();
    assertThat(causeOfType(failure.get(), QueryTerminatedException.class)).isNotNull();
    assertThat(database.countType("Written", false)).isZero();
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void statementThatEndsBeforeSeeingTheTerminationReportsSo() throws Exception {
    final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null);
    try {
      drain(database.query("opencypher", "RETURN 42 AS answer"));
      q.terminate("root");
    } finally {
      q.close();
    }
    assertThat(q.awaitEnd(0)).isTrue();
    assertThat(q.getOutcome()).isEqualTo(RunningQuery.Outcome.COMPLETED_BEFORE_TERMINATION);
    assertThat(registry.size()).isZero();
  }

  @Test
  void contextsOnOtherThreadsObeyTheTerminationOfTheStatement() throws Exception {
    // A parallel worker runs on a pooled thread with no entry published, on a COPY of the statement's context or on a
    // context derived from it: both must still stop with the statement
    try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null)) {
      final BasicCommandContext root = new BasicCommandContext();
      root.setDatabase((DatabaseInternal) database);

      final AtomicReference<Throwable> fromCopy = new AtomicReference<>();
      final AtomicReference<Throwable> fromChild = new AtomicReference<>();
      final Thread worker = new Thread(() -> {
        assertThat(RunningQuery.current()).isNull();
        final CommandContext copy = root.copy();
        final BasicCommandContext child = new BasicCommandContext();
        child.setParent(root);
        final WorkGuard copyGuard = WorkGuard.forCommandDeadline(copy);
        final WorkGuard childGuard = WorkGuard.forCommand(child, "the worker");
        copyGuard.check();
        childGuard.check();
        q.terminate("root");
        try {
          copyGuard.check();
        } catch (final Throwable e) {
          fromCopy.set(e);
        }
        try {
          childGuard.check();
        } catch (final Throwable e) {
          fromChild.set(e);
        }
      }, "issue-9680-worker");
      worker.start();
      worker.join(10_000);

      assertThat(fromCopy.get()).isInstanceOf(QueryTerminatedException.class);
      assertThat(fromChild.get()).isInstanceOf(QueryTerminatedException.class).hasMessageContaining("the worker");
    }
  }

  @Test
  void unregisteredStatementIsUnaffected() {
    assertThat(RunningQuery.current()).isNull();
    try (final ResultSet rs = database.query("opencypher", "UNWIND range(1, 100) AS i RETURN sum(i) AS total")) {
      assertThat(rs.next().<Number>getProperty("total").longValue()).isEqualTo(5050L);
    }
  }

  @Test
  void commandTimeoutStopsUnwindIntoAggregation() {
    // The same gap seen from the deadline: the aggregation's drain of the UNWINDs had no check at all, so the setting
    // could not stop the statement either, and it ran its full 16 s.
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 200L);
    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> drain(database.query("opencypher", CYPHER_UNWIND_SUM)))
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey())
        .satisfies(e -> assertThat(causeOfType(e, TimeoutException.class)).isNotNull());
    watch.assertGaveUpWithin(10_000, "a 200 ms command deadline against a statement that runs for about 16 s");
  }

  @Test
  void credentialsAreMaskedInTheListedText() {
    try (final RunningQuery q = registry.register("db", "root", "http", null, null)) {
      q.setStatement("sql", "CREATE USER bob IDENTIFIED BY s3cr3t ROLE admin");
      assertThat(q.getText()).isEqualTo("CREATE USER bob IDENTIFIED BY *** ROLE admin");
      q.setStatement("sql", "ALTER USER bob SET password = 'my pass', token: \"abc\"");
      assertThat(q.getText()).isEqualTo("ALTER USER bob SET password = ***, token: ***");

      // JSON form: the closing quote of the key sits between the keyword and the separator
      q.setStatement("sql", "INSERT INTO Account CONTENT {\"name\": \"bob\", \"password\":\"secret\", \"apiToken\" : \"t-1\"}");
      assertThat(q.getText()).doesNotContain("secret").contains("\"password\":***").contains("\"name\": \"bob\"");

      // Masked before it is cut: a credential across the length limit does not survive the cut
      final String padding = "x".repeat(RunningQuery.MAX_TEXT_LENGTH - 20);
      q.setStatement("sql", "SELECT '" + padding + "' FROM V WHERE password = 'secret-across-the-cut-0123456789'");
      assertThat(q.getText()).doesNotContain("secret").hasSize(RunningQuery.MAX_TEXT_LENGTH + 3);

      q.setStatement("opencypher", "MATCH (n) RETURN n.name");
      assertThat(q.getText()).isEqualTo("MATCH (n) RETURN n.name");
    }
  }

  @Test
  void anEntryIsPublishedOnlyOnceRegistered() {
    assertThat(RunningQuery.current()).isNull();
    final RunningQuery q = registry.register("db", "root", "http", null, null);
    try {
      assertThat(RunningQuery.current()).isSameAs(q);
      // a nested entry gives the outer one back when it closes
      try (final RunningQuery nested = registry.register("db", "root", "http", null, null)) {
        assertThat(RunningQuery.current()).isSameAs(nested);
      }
      assertThat(RunningQuery.current()).isSameAs(q);
    } finally {
      q.close();
    }
    assertThat(RunningQuery.current()).isNull();
  }

  @Test
  void registryResolvesIdsAndForgetsEndedStatements() {
    final RunningQuery q = registry.register("db", "admin", "http", "AS-1", "bench-1");
    try {
      assertThat(registry.get(q.getId())).isSameAs(q);
      assertThat(registry.get(q.getId().substring(1))).isSameAs(q);
      assertThat(registry.get("not-an-id")).isNull();
      assertThat(registry.get(null)).isNull();
      assertThat(RunningQuery.current()).isSameAs(q);
      assertThat(q.toJSON().getString("tag")).isEqualTo("bench-1");
      assertThat(q.toJSON().getString("sessionId")).isEqualTo("AS-1");
    } finally {
      q.close();
    }
    assertThat(RunningQuery.current()).isNull();
    assertThat(registry.get(q.getId())).isNull();
    assertThat(q.getOutcome()).isEqualTo(RunningQuery.Outcome.COMPLETED);
  }

  /**
   * Runs {@code work} on a thread of its own, registered, then terminates it once it is well into its work and checks that
   * it failed with the termination, promptly, and left the registry.
   */
  private void terminateWhileRunning(final String language, final String text, final Consumer<RunningQuery> work) {
    final AtomicReference<RunningQuery> entry = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread runner = new Thread(() -> {
      try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, "tag-9680")) {
        q.setStatement(language, text);
        entry.set(q);
        work.accept(q);
      } catch (final Throwable e) {
        failure.set(e);
      }
    }, "issue-9680-runner");
    runner.start();
    try {
      while (entry.get() == null)
        Thread.sleep(5);
      // Well into the work: every statement here needs seconds when left alone
      Thread.sleep(300);
      assertThat(registry.getRunning()).as("the statement must still be running when it is terminated").containsExactly(entry.get());

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThat(entry.get().terminate("root")).isTrue();
      assertThat(entry.get().awaitEnd(60_000)).as("the statement must end once terminated").isTrue();
      watch.assertGaveUpWithin(10_000, "a terminated statement stopping at its next check, against one that runs to the end");
      runner.join(10_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    assertThat(failure.get()).as("a terminated statement must fail, not return a result").isNotNull();
    assertThat(causeOfType(failure.get(), QueryTerminatedException.class))
        .as("the failure must be the termination, got: %s", failure.get()).isNotNull();
    assertThat(entry.get().getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
    assertThat(registry.size()).isZero();
  }

  private static void drain(final ResultSet resultSet) {
    try (resultSet) {
      while (resultSet.hasNext())
        resultSet.next();
    }
  }

  private static <T extends Throwable> T causeOfType(final Throwable e, final Class<T> type) {
    for (Throwable t = e; t != null; t = t.getCause())
      if (type.isInstance(t))
        return type.cast(t);
    return null;
  }
}
