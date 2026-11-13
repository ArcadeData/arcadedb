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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consolidated regression tests, one nested class per original issue.
 */
class RunningQueryTest {
  /**
   * Regression for #9680: a registered statement stops when terminated, wherever its work happens.
   * <p>
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
  @Nested
  class RunningQueryTermination {
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
      // Every element runs the inner SET on a plan of its own: nothing loops over the lists but the FOREACHes themselves.
      // Two nested ranges of at most 1,000,000 elements rather than one of 50,000,000: arcadedb.queryMaxRangeSize scales with
      // the heap and never goes below that floor, and a single range above it is refused at once on a small CI runner - the
      // statement had then ended before the test terminated it
      final String query = "MATCH (n:Node {v: 0}) FOREACH (i IN range(1, 1000000) | FOREACH (j IN range(1, 1000) | SET n.x9680 = j))";
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
      try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
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

      // The thread serves the next request clean: nothing of the terminated entry is left on it to obey
      assertThat(RunningQuery.current()).isNull();
      database.transaction(() -> database.newDocument("Written").set("x", 2).save());
      assertThat(database.countType("Written", false)).isEqualTo(1);
      database.transaction(() -> database.command("sql", "DELETE FROM Written"));
    }

    @Test
    void statementThatEndsBeforeSeeingTheTerminationReportsSo() throws Exception {
      final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE);
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
      try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
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
      try (final RunningQuery q = registry.register("db", "root", "http", null, null, RunningQuery.Client.NONE)) {
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

        q.setStatement("sql", "INSERT INTO Cfg SET secret = 'x1', api_key = 'x2', apiKey: 'x3', passwordHash = 'kept'");
        assertThat(q.getText()).isEqualTo("INSERT INTO Cfg SET secret = ***, api_key = ***, apiKey: ***, passwordHash = 'kept'");

        // The scheme word is not the credential: the token after it is
        q.setStatement("sql", "SELECT http('GET', 'u', {headers: {authorization: Bearer abc123}}), 'Authorization Basic dXNlcg=='");
        assertThat(q.getText()).doesNotContain("abc123").doesNotContain("dXNlcg").contains("authorization: Bearer ***");

        q.setStatement("sql", "UPDATE Cfg SET pwd = 'x4', passwd = 'x5'");
        assertThat(q.getText()).isEqualTo("UPDATE Cfg SET pwd = ***, passwd = ***");

        q.setStatement("opencypher", "MATCH (n) RETURN n.name");
        assertThat(q.getText()).isEqualTo("MATCH (n) RETURN n.name");

        // A long run of blanks after a keyword, with no value to end it, is scanned once rather than backtracked over, and a
        // huge statement is scanned only as far as it can be shown
        q.setStatement("sql", "SELECT password" + " ".repeat(200_000) + ",");
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        for (int i = 0; i < 20; i++)
          assertThat(q.getText()).hasSize(RunningQuery.MAX_TEXT_LENGTH + 3);
        watch.assertStayedUnder(2_000, "masking is linear in the shown text: a backtracking pattern takes minutes on this input");
      }
    }

    @Test
    void anEntryIsPublishedOnlyOnceRegistered() {
      assertThat(RunningQuery.current()).isNull();
      final RunningQuery q = registry.register("db", "root", "http", null, null, RunningQuery.Client.NONE);
      try {
        assertThat(RunningQuery.current()).isSameAs(q);
        // a nested entry gives the outer one back when it closes
        try (final RunningQuery nested = registry.register("db", "root", "http", null, null, RunningQuery.Client.NONE)) {
          assertThat(RunningQuery.current()).isSameAs(nested);
        }
        assertThat(RunningQuery.current()).isSameAs(q);
      } finally {
        q.close();
      }
      assertThat(RunningQuery.current()).isNull();
    }

    @Test
    void anEntryClosedOutOfTurnIsNeverLeftBehind() {
      final RunningQuery outer = registry.register("db", "root", "http", null, null, RunningQuery.Client.NONE);
      final RunningQuery inner = registry.register("db", "root", "http", null, null, RunningQuery.Client.NONE);
      outer.terminate("root");
      // The outer entry ends first: the inner one still runs and is still the thread's
      outer.close();
      assertThat(RunningQuery.current()).isSameAs(inner);
      // Closing the inner one does not bring the ended (and terminated) outer one back
      inner.close();
      assertThat(RunningQuery.current()).isNull();
      // So a later statement on this thread, embedded or not, is not failed by a termination meant for another
      database.transaction(() -> database.newDocument("Written").set("x", 3).save());
      database.transaction(() -> database.command("sql", "DELETE FROM Written"));
      // And an ended entry no longer fails anybody
      outer.checkNotTerminated("the check");
    }

    @Test
    void registryResolvesIdsAndForgetsEndedStatements() {
      final RunningQuery q = registry.register("db", "admin", "http", "AS-1", "bench-1", RunningQuery.Client.NONE);
      try {
        assertThat(registry.get(q.getId())).isSameAs(q);
        assertThat(registry.get(" " + q.getId().toUpperCase() + " ")).isSameAs(q);
        // Only an id this registry issued: a bare number, or the same number issued by another node, finds nothing here
        assertThat(registry.get(q.getId().substring(0, q.getId().indexOf('-')))).isNull();
        final RunningQueryRegistry otherNode = new RunningQueryRegistry();
        try (final RunningQuery foreign = otherNode.register("db", "admin", "http", null, null, RunningQuery.Client.NONE)) {
          assertThat(foreign.getId()).startsWith(q.getId().substring(0, q.getId().indexOf('-') + 1));
          assertThat(registry.get(foreign.getId())).isNull();
        }
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
        try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, "tag-9680", RunningQuery.Client.NONE)) {
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

  /**
   * Regression for #9689: follow-ups of the running-statement registry (SHOW/TERMINATE TRANSACTIONS, listeners, error category).
   * <p>
   * Issue #9689, the engine side of the running-statement registry's follow-ups: Cypher {@code SHOW TRANSACTIONS} /
   * {@code TERMINATE TRANSACTIONS} over the registry with the same visibility rule as the HTTP commands, an entry bound to
   * the thread its work runs on rather than the one that opened it, termination listeners, guest-language scripts that a
   * terminate interrupts, and the error category every wire protocol reports a termination with.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RunningQueryFollowUps {
    private static final String DB_PATH   = "./target/databases/test-issue-9689-running-query-follow-ups";
    /** About 16 s on one core when left alone, all of it inside one aggregation. */
    private static final String LONG_CYPHER =
        "UNWIND range(1, 10000) AS i UNWIND range(1, 10000) AS j RETURN sum(sin(toFloat(i * j))) AS total";

    private static Database database;

    @BeforeAll
    static void setup() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterAll
    static void teardown() {
      if (database != null)
        database.drop();
    }

    private static RunningQueryRegistry newRegistry() {
      final RunningQueryRegistry registry = new RunningQueryRegistry();
      registry.setAdministrator("root"::equals);
      return registry;
    }

    @Test
    void showTransactionsListsWhatTheUserMaySee() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      try (final Background alice = Background.run(registry, "alice", "tag-alice", q -> drain(database.query("opencypher", LONG_CYPHER)))) {
        // Root sees alice's statement, with Neo4j's default columns
        final List<Result> asRoot = cypherAs(registry, "root", "SHOW TRANSACTIONS", Map.of());
        final Result row = findById(asRoot, alice.entry.getId());
        assertThat(row).as("root sees every statement").isNotNull();
        assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("database", "transactionId", "currentQueryId", "connectionId",
            "clientAddress", "username", "currentQuery", "startTime", "status", "elapsedTime");
        assertThat(row.<String>getProperty("username")).isEqualTo("alice");
        assertThat(row.<String>getProperty("currentQuery")).isEqualTo(LONG_CYPHER);
        assertThat(row.<String>getProperty("database")).isEqualTo(database.getName());
        assertThat(row.<String>getProperty("status")).isEqualTo("Running");
        assertThat(row.<Object>getProperty("elapsedTime")).isInstanceOf(CypherDuration.class);
        // The SHOW itself is a running statement too, and lists itself
        assertThat(asRoot).hasSize(2);

        // Bob sees only his own: the SHOW he is running
        final List<Result> asBob = cypherAs(registry, "bob", "SHOW TRANSACTIONS", Map.of());
        assertThat(asBob).hasSize(1);
        assertThat(asBob.getFirst().<String>getProperty("username")).isEqualTo("bob");

        // YIELD reaches the columns a bare SHOW leaves out, and WHERE filters on them
        final List<Result> yielded = cypherAs(registry, "root",
            "SHOW TRANSACTIONS YIELD transactionId, protocol, language, metaData WHERE metaData.tag = 'tag-alice'", Map.of());
        assertThat(yielded).hasSize(1);
        assertThat(yielded.getFirst().<String>getProperty("transactionId")).isEqualTo(alice.entry.getId());
        assertThat(yielded.getFirst().<String>getProperty("protocol")).isEqualTo("test");
        assertThat(yielded.getFirst().<String>getProperty("language")).isEqualTo("opencypher");

        // By id, as a literal or a parameter
        assertThat(cypherAs(registry, "root", "SHOW TRANSACTIONS '" + alice.entry.getId() + "'", Map.of())).hasSize(1);
        assertThat(cypherAs(registry, "root", "SHOW TRANSACTIONS $ids", Map.of("ids", List.of(alice.entry.getId(), "q0-0"))))
            .hasSize(1);
      }
    }

    @Test
    void terminateTransactionsStopsTheStatementOnlyForWhoMaySeeIt() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      try (final Background alice = Background.run(registry, "alice", null, q -> drain(database.query("opencypher", LONG_CYPHER)))) {
        final String id = alice.entry.getId();

        // Another user's statement is not found: its existence is not given away, and it keeps running
        final List<Result> byBob = cypherAs(registry, "bob", "TERMINATE TRANSACTIONS '" + id + "'", Map.of());
        assertThat(byBob).hasSize(1);
        assertThat(byBob.getFirst().<String>getProperty("message")).isEqualTo("Transaction not found.");
        assertThat(alice.entry.isTerminated()).isFalse();

        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final List<Result> byRoot = cypherAs(registry, "root", "TERMINATE TRANSACTIONS '" + id + "'", Map.of());
        assertThat(byRoot).hasSize(1);
        assertThat(byRoot.getFirst().getPropertyNames()).containsExactly("transactionId", "username", "message");
        assertThat(byRoot.getFirst().<String>getProperty("transactionId")).isEqualTo(id);
        assertThat(byRoot.getFirst().<String>getProperty("username")).isEqualTo("alice");
        assertThat(byRoot.getFirst().<String>getProperty("message")).isEqualTo("Transaction terminated.");

        alice.awaitTerminated();
        watch.assertGaveUpWithin(10_000, "a terminated statement stopping at its next check, against one that runs for 16 s");
        assertThat(alice.entry.getTerminatedBy()).isEqualTo("root");
      }
    }

    @Test
    void theUserStopsTheirOwnStatementByParameter() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      try (final Background alice = Background.run(registry, "alice", null, q -> drain(database.query("opencypher", LONG_CYPHER)))) {
        final List<Result> rows = cypherAs(registry, "alice", "TERMINATE TRANSACTIONS $ids YIELD transactionId, message",
            Map.of("ids", List.of(alice.entry.getId())));
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<String>getProperty("message")).isEqualTo("Transaction terminated.");
        alice.awaitTerminated();
      }
    }

    @Test
    void terminateTransactionsFindsAStatementByTheIdItHasOnTheServerThatForwardedIt() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      try (final Background forwarded = Background.run(registry, "alice", null, q -> {
        // As the leader records a statement a replica forwarded to it
        q.setForwardedFrom("q7-0ff1ce");
        drain(database.query("opencypher", LONG_CYPHER));
      })) {
        final List<Result> rows = cypherAs(registry, "root", "TERMINATE TRANSACTIONS 'q7-0ff1ce'", Map.of());
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<String>getProperty("message")).isEqualTo("Transaction terminated.");
        forwarded.awaitTerminated();
      }
    }

    @Test
    void showComposedWithTerminateStopsWhatTheShowSelected() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      try (final Background tagged = Background.run(registry, "alice", "stop-me", q -> drain(database.query("opencypher", LONG_CYPHER)));
          final Background other = Background.run(registry, "alice", "keep-me", q -> drain(database.query("opencypher", LONG_CYPHER)))) {
        final List<Result> rows = cypherAs(registry, "root",
            "SHOW TRANSACTIONS YIELD transactionId AS txId, metaData WHERE metaData.tag = 'stop-me' "
                + "TERMINATE TRANSACTIONS txId YIELD transactionId, message RETURN transactionId, message", Map.of());
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().<String>getProperty("transactionId")).isEqualTo(tagged.entry.getId());
        assertThat(rows.getFirst().<String>getProperty("message")).isEqualTo("Transaction terminated.");
        tagged.awaitTerminated();
        assertThat(other.entry.isTerminated()).isFalse();
        other.entry.terminate("root");
        other.awaitTerminated();
      }
    }

    @Test
    void transactionsCommandsNeedAServerAndRunWhereTheyAreSent() {
      assertThat(RunningQuery.current()).isNull();
      assertThatThrownBy(() -> drain(database.query("opencypher", "SHOW TRANSACTIONS")))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("require a server");

      // Each server answers for its own statements: an HA replica runs them rather than forwarding them to the leader
      final QueryEngine engine = database.getQueryEngine("opencypher");
      assertThat(engine.analyze("SHOW TRANSACTIONS").isIdempotent()).isTrue();
      assertThat(engine.analyze("TERMINATE TRANSACTIONS 'q1-abc'").isIdempotent()).isTrue();
      assertThat(engine.analyze("SHOW USERS").isIdempotent()).isFalse();
    }

    @Test
    void aBoundEntryReachesWorkOnAnotherThreadAndLeavesItClean() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      final RunningQuery query = registry.open(database.getName(), "alice", "test", null, null, RunningQuery.Client.NONE);
      try {
        // Opened, not published: the opening thread does not carry it
        assertThat(RunningQuery.current()).isNull();
        assertThat(registry.getRunning()).containsExactly(query);

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<RunningQuery> afterwards = new AtomicReference<>();
        final Thread worker = new Thread(() -> {
          try (final RunningQuery.Binding ignored = query.bind()) {
            final BasicCommandContext context = new BasicCommandContext();
            context.setDatabase((DatabaseInternal) database);
            assertThat(context.getRunningQuery()).isSameAs(query);
            query.terminate("root");
            WorkGuard.forCommand(context, "the worker").check();
          } catch (final Throwable e) {
            failure.set(e);
          }
          afterwards.set(RunningQuery.current());
        }, "issue-9689-worker");
        worker.start();
        worker.join(10_000);

        assertThat(failure.get()).isInstanceOf(QueryTerminatedException.class);
        assertThat(afterwards.get()).as("the binding gives the thread back what it had").isNull();
        // A binding is not the end of the entry: only closing it is
        assertThat(query.isEnded()).isFalse();
      } finally {
        query.close();
      }
      assertThat(registry.size()).isZero();
    }

    @Test
    void terminationListenersRunOnceAndCanBeWithdrawn() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      try (final RunningQuery query = registry.open("db", "alice", "test", null, null, RunningQuery.Client.NONE)) {
        final AtomicInteger notified = new AtomicInteger();
        final AtomicInteger withdrawn = new AtomicInteger();
        query.onTerminate(notified::incrementAndGet);
        query.onTerminate(withdrawn::incrementAndGet).close();
        query.onTerminate(() -> {
          throw new IllegalStateException("a failing listener must not stop the others");
        });

        assertThat(query.terminate("root")).isTrue();
        assertThat(query.terminate("root")).isFalse();
        assertThat(notified.get()).isEqualTo(1);
        assertThat(withdrawn.get()).isZero();

        // Registered after the termination: runs at once
        query.onTerminate(notified::incrementAndGet);
        assertThat(notified.get()).isEqualTo(2);
      }
    }

    /**
     * Issue #9962: the client is part of opening an entry, so the first listing that can see the entry already sees its
     * client, from either entry point; and a transport that reports no peer lists no address rather than {@code "null"}.
     */
    @Test
    void anEntryIsListedWithItsClientFromTheStart() {
      final RunningQueryRegistry registry = newRegistry();
      try (final RunningQuery published = registry.register("db", "alice", "test", null, null,
          new RunningQuery.Client("c-1", "10.0.0.1:5000"));
          final RunningQuery opened = registry.open("db", "alice", "test", null, null,
              RunningQuery.Client.of("c-2", new InetSocketAddress("10.0.0.2", 5000)))) {
        assertThat(registry.getRunning()).extracting(RunningQuery::getConnectionId).containsExactly("c-1", "c-2");
        assertThat(registry.getRunning()).extracting(RunningQuery::getClientAddress).containsExactly("10.0.0.1:5000", "/10.0.0.2:5000");
        assertThat(published.toJSON().getString("clientAddress")).isEqualTo("10.0.0.1:5000");
      }

      // A peer the transport does not report, and work from no connection at all: the fields are left out, never "null"
      try (final RunningQuery noPeer = registry.register("db", "alice", "test", null, null, RunningQuery.Client.of("c-3", null));
          final RunningQuery none = registry.register("db", "alice", "test", null, null, RunningQuery.Client.NONE)) {
        assertThat(noPeer.getConnectionId()).isEqualTo("c-3");
        assertThat(noPeer.toJSON().has("clientAddress")).isFalse();
        assertThat(none.toJSON().has("connectionId")).isFalse();
        assertThat(none.toJSON().has("clientAddress")).isFalse();
      }

      // Forgetting the client is refused at once rather than listed as an entry nobody can attribute
      assertThatThrownBy(() -> registry.open("db", "alice", "test", null, null, null)).isInstanceOf(NullPointerException.class);
      assertThat(registry.getRunning()).isEmpty();
    }

    @Test
    void visibilityIsTheRegistrysOneRule() {
      final RunningQueryRegistry registry = newRegistry();
      try (final RunningQuery query = registry.open("db", "alice", "test", null, null, new RunningQuery.Client("pg-42", "10.0.0.1:5000"))) {
        assertThat(registry.isVisible("root", query)).isTrue();
        assertThat(registry.isVisible("alice", query)).isTrue();
        assertThat(registry.isVisible("bob", query)).isFalse();
        assertThat(registry.isVisible(null, query)).isFalse();
        // The same rule for what a user owns without a statement running - a session, an idle connection
        assertThat(registry.isVisible("root", "alice")).isTrue();
        assertThat(registry.isVisible("alice", "alice")).isTrue();
        assertThat(registry.isVisible("bob", "alice")).isFalse();
        assertThat(registry.isVisible((String) null, "alice")).isFalse();
        // A registry nobody set an administrator for has none
        final RunningQueryRegistry plain = new RunningQueryRegistry();
        try (final RunningQuery other = plain.open("db", "alice", "test", null, null, RunningQuery.Client.NONE)) {
          assertThat(plain.isVisible("root", other)).isFalse();
        }

        query.setForwardedFrom("q7-abc");
        assertThat(query.toJSON().getString("connectionId")).isEqualTo("pg-42");
        assertThat(query.toJSON().getString("clientAddress")).isEqualTo("10.0.0.1:5000");
        assertThat(query.toJSON().getString("forwardedFrom")).isEqualTo("q7-abc");
      }
    }

    @Test
    void aTerminationIsItsOwnErrorCategory() {
      final QueryTerminatedException terminated = new QueryTerminatedException("stopped");
      assertThat(ErrorCategory.of(terminated)).isEqualTo(ErrorCategory.TERMINATED);
      // Whatever wraps it: a wire protocol must never report it as something a client retries
      assertThat(ErrorCategory.of(new TransactionException("commit failed", terminated))).isEqualTo(ErrorCategory.TERMINATED);
      assertThat(ErrorCategory.of(new CommandExecutionException("wrapped", terminated))).isEqualTo(ErrorCategory.TERMINATED);
    }

    @Test
    void aTerminatedScriptIsInterrupted() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      // A guest loop never reaches a WorkGuard: only Context.interrupt stops it
      try (final Background script = Background.run(registry, "alice", null, q -> drain(database.command("js", "while (true) {}")))) {
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        script.entry.terminate("root");
        script.awaitTerminated();
        watch.assertGaveUpWithin(15_000, "a script interrupted on termination, against one that loops forever");
      }
      // The shared script context serves the next caller
      try (final ResultSet rs = database.command("js", "3 + 5")) {
        assertThat(rs.next().<Number>getProperty("value").intValue()).isEqualTo(8);
      }
    }

    @Test
    void aTerminatedFunctionIsInterrupted() throws Exception {
      final RunningQueryRegistry registry = newRegistry();
      database.command("sqlscript", "DEFINE FUNCTION issue9689.spin \"while (true) {} return 1;\" LANGUAGE js;");
      database.command("sqlscript", "DEFINE FUNCTION issue9689.answer \"return 42;\" LANGUAGE js;");
      try {
        try (final Background call = Background.run(registry, "alice", null,
            q -> drain(database.query("sql", "SELECT `issue9689.spin`() AS v")))) {
          call.entry.terminate("root");
          call.awaitTerminated();
        }
        try (final ResultSet rs = database.query("sql", "SELECT `issue9689.answer`() AS v")) {
          assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(42);
        }
      } finally {
        database.getSchema().unregisterFunctionLibrary("issue9689");
      }
    }

    // ---------------------------------------------------------------------------------------------

    /** Runs {@code text} as Cypher under an entry of {@code user}'s, as a server protocol would. */
    private static List<Result> cypherAs(final RunningQueryRegistry registry, final String user, final String text,
        final Map<String, Object> parameters) {
      try (final RunningQuery ignored = registry.register(database.getName(), user, "test", null, null, RunningQuery.Client.NONE)) {
        ignored.setStatement("opencypher", text);
        final List<Result> rows = new ArrayList<>();
        try (final ResultSet rs = database.query("opencypher", text, parameters)) {
          while (rs.hasNext())
            rows.add(rs.next());
        }
        return rows;
      }
    }

    private static Result findById(final List<Result> rows, final String id) {
      for (final Result row : rows)
        if (id.equals(row.getProperty("transactionId")))
          return row;
      return null;
    }

    private static void drain(final ResultSet resultSet) {
      try (resultSet) {
        while (resultSet.hasNext())
          resultSet.next();
      }
    }

    /** A statement running on a thread of its own, registered as {@code user}'s, that is expected to be terminated. */
    private static final class Background implements AutoCloseable {
      private final CountDownLatch           started = new CountDownLatch(1);
      private final AtomicReference<Throwable> failure = new AtomicReference<>();
      private       Thread                   thread;
      private       RunningQuery             entry;

      static Background run(final RunningQueryRegistry registry, final String user, final String tag,
          final Consumer<RunningQuery> work) throws InterruptedException {
        final Background background = new Background();
        background.thread = new Thread(() -> {
          try (final RunningQuery q = registry.register(database.getName(), user, "test", null, tag, RunningQuery.Client.NONE)) {
            q.setStatement("opencypher", LONG_CYPHER);
            background.entry = q;
            background.started.countDown();
            work.accept(q);
          } catch (final Throwable e) {
            background.failure.set(e);
          }
        }, "issue-9689-" + user);
        background.thread.start();
        assertThat(background.started.await(10, TimeUnit.SECONDS)).isTrue();
        // Well into its work, which takes seconds when left alone
        Thread.sleep(300);
        return background;
      }

      void awaitTerminated() throws InterruptedException {
        assertThat(entry.awaitEnd(30_000)).as("the statement must end once terminated").isTrue();
        thread.join(10_000);
        assertThat(failure.get()).as("a terminated statement must fail, not return a result").isNotNull();
        Throwable t = failure.get();
        while (t != null && !(t instanceof QueryTerminatedException))
          t = t.getCause();
        assertThat(t).as("the failure must be the termination, got: %s", failure.get()).isNotNull();
        assertThat(entry.getOutcome()).isEqualTo(RunningQuery.Outcome.TERMINATED);
      }

      @Override
      public void close() throws InterruptedException {
        // A test that failed before terminating the statement must not leave it running for the next one
        if (entry != null && !entry.isEnded()) {
          entry.terminate("cleanup");
          entry.awaitEnd(30_000);
        }
        thread.join(10_000);
      }
    }
  }
}
