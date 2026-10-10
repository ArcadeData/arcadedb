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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
 * Issue #9689, the engine side of the running-statement registry's follow-ups: Cypher {@code SHOW TRANSACTIONS} /
 * {@code TERMINATE TRANSACTIONS} over the registry with the same visibility rule as the HTTP commands, an entry bound to
 * the thread its work runs on rather than the one that opened it, termination listeners, guest-language scripts that a
 * terminate interrupts, and the error category every wire protocol reports a termination with.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9689RunningQueryFollowUpsTest {
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
    final RunningQuery query = registry.open(database.getName(), "alice", "test", null, null);
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
    try (final RunningQuery query = registry.open("db", "alice", "test", null, null)) {
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

  @Test
  void visibilityIsTheRegistrysOneRule() {
    final RunningQueryRegistry registry = newRegistry();
    try (final RunningQuery query = registry.open("db", "alice", "test", null, null)) {
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
      try (final RunningQuery other = plain.open("db", "alice", "test", null, null)) {
        assertThat(plain.isVisible("root", other)).isFalse();
      }

      query.setConnection("pg-42", "10.0.0.1:5000");
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
    try (final RunningQuery ignored = registry.register(database.getName(), user, "test", null, null)) {
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
        try (final RunningQuery q = registry.register(database.getName(), user, "test", null, tag)) {
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
