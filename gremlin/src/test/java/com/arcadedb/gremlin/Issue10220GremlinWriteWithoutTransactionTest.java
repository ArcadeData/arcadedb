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
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10220: a Gremlin write run through {@code db.command("gremlin", ...)} with no transaction open began a transaction and left it
 * open in the calling thread (the TinkerPop AUTO model of {@link ArcadeGraph}). The write was visible to that thread only, the partial
 * write of a traversal that failed stayed in it and an unrelated {@code commit()} published it, and a write whose result was closed
 * unread never ran, since the traversal runs as its rows are pulled. As an openCypher write does since #10190, it is now one transaction:
 * run to the end inside the call, committed once, rolled back when it fails. A read keeps streaming, and a write inside a transaction
 * the caller owns joins it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue10220GremlinWriteWithoutTransactionTest {
  private static final String PATH = "./target/test-gremlin-10220-write-without-transaction";

  private ArcadeGraph     graph;
  private Database        database;
  private ExecutorService otherThread;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(PATH));
    graph = ArcadeGraph.open(PATH);
    database = (Database) graph.getDatabase();
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.k STRING");
    database.command("sql", "CREATE INDEX ON N (k) UNIQUE");
    database.transaction(() -> database.newVertex("N").set("k", "seed").save());
    otherThread = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void teardown() {
    otherThread.shutdownNow();
    if (graph != null) {
      if (database.isOpen() && database.isTransactionActive())
        database.rollback();
      graph.drop();
    }
    FileUtils.deleteRecursively(new File(PATH));
  }

  @Test
  void aWriteWithNoTransactionIsOneCommittedTransaction() throws Exception {
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.command("gremlin", "g.addV('N').property('k', 'a')")) {
      while (rs.hasNext())
        rs.next();
    }

    assertThat(database.isTransactionActive()).as("the write left its transaction open").isFalse();
    assertThat(count("a")).as("this thread").isEqualTo(1);
    assertThat(committedCount("a")).as("another thread: the write is committed").isEqualTo(1);
  }

  @Test
  void aWriteWhoseResultIsClosedUnreadStillRuns() throws Exception {
    database.command("gremlin", "g.addV('N').property('k', 'z')").close();

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("z")).isEqualTo(1);
  }

  @Test
  void theRowsOfAWriteAreAnsweredOncePersisted() {
    try (final ResultSet rs = database.command("gremlin", "g.addV('N').property('k', 'r')")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.isVertex()).isTrue();
      assertThat(row.<String>getProperty("k")).isEqualTo("r");
      assertThat(row.getIdentity()).as("the vertex was committed, so it has its record id").isPresent();
      assertThat(rs.hasNext()).isFalse();
    }
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void aTerminalStepRunsInTheTransactionToo() throws Exception {
    try (final ResultSet rs = database.command("gremlin", "g.addV('N').property('k', 'e').toList()")) {
      assertThat(rs.hasNext()).isTrue();
    }

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("e")).isEqualTo(1);
  }

  /**
   * The reported sequence: a failed traversal's vertex stayed in the open transaction and a later {@code commit()} published it,
   * past a reopen.
   */
  @Test
  void aFailedTraversalLeavesNothingAndAnUnrelatedCommitDoesNotPublishIt() throws Exception {
    database.command("gremlin", "g.addV('N').property('k', 'a')").close();

    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("gremlin", "g.addV('N').property('k', 'b').fail('x')")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(RuntimeException.class).hasStackTraceContaining("FailException");

    assertThat(database.isTransactionActive()).as("the failed traversal left its transaction open").isFalse();
    assertThat(count("b")).isZero();

    // an unrelated write and commit of the caller
    database.transaction(() -> database.newVertex("N").set("k", "c").save());

    assertThat(committedCount("a")).isEqualTo(1);
    assertThat(committedCount("b")).as("the failed traversal's vertex was published by a later commit").isZero();
    assertThat(committedCount("c")).isEqualTo(1);

    database.close();
    final Database reopened = new DatabaseFactory(PATH).open();
    try {
      assertThat(countIn(reopened, "a")).isEqualTo(1);
      assertThat(countIn(reopened, "b")).as("the failed traversal's vertex survived a reopen").isZero();
      assertThat(countIn(reopened, "c")).isEqualTo(1);
    } finally {
      reopened.close();
    }
  }

  @Test
  void aUniqueViolationMidTraversalRollsEverythingBack() throws Exception {
    // the first vertex is fine, the second takes the key of the one committed in setup()
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.command("gremlin", "g.addV('N').property('k', 'p').addV('N').property('k', 'seed')")) {
        while (rs.hasNext())
          rs.next();
      }
    }).isInstanceOf(RuntimeException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("p")).as("the part of the traversal before the violation").isZero();
    assertThat(committedCount("seed")).isEqualTo(1);
  }

  /**
   * Groovy is code that only running tells what it does, so the analysis takes it for a write: it runs in the transaction of the
   * statement too, and it is still the execution that refuses it to a user who is not the server administrator.
   */
  @Test
  void aGroovyScriptIsOneTransactionToo() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "auto");

    readToTheEnd("g.getGraph().addVertex('N').property('k', 'groovy'); g.V().has('k', 'groovy')");

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("groovy")).isEqualTo(1);
  }

  @Test
  void theProfileOfAWriteIsThatOfTheRunThatWasDrained() throws Exception {
    try (final ResultSet rs = database.command("gremlin", "g.addV('N').property('k', 'prof')", Map.of("$profileExecution", true))) {
      while (rs.hasNext())
        rs.next();
      assertThat(rs.getExecutionPlan()).as("the plan of the run").isPresent();
    }

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("prof")).as("the statement ran once, not once for the plan and once for the rows").isEqualTo(1);
  }

  @Test
  void aConflictRunsTheWholeStatementAgain() throws Exception {
    final AtomicInteger creations = new AtomicInteger();
    final BeforeRecordCreateListener conflictOnce = record -> {
      if (creations.getAndIncrement() == 0)
        throw new ConcurrentModificationException("simulated conflict with a concurrent transaction");
      return true;
    };
    database.getEvents().registerListener(conflictOnce);
    try {
      database.command("gremlin", "g.addV('N').property('k', 'retried')").close();
    } finally {
      database.getEvents().unregisterListener(conflictOnce);
    }

    assertThat(creations.get()).as("the first attempt conflicted, the second ran").isEqualTo(2);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("retried")).as("one run of the statement stands").isEqualTo(1);
  }

  @Test
  void aReadKeepsStreaming() {
    try (final ResultSet rs = database.command("gremlin", "g.V().hasLabel('N').values('k')")) {
      // committed after command() returned: a traversal that runs while it is read sees it, one drained by the call does not
      database.transaction(() -> database.newVertex("N").set("k", "late").save());

      final List<Object> keys = new ArrayList<>();
      while (rs.hasNext())
        keys.add(rs.next().getProperty("result"));
      assertThat(keys).containsExactlyInAnyOrder("seed", "late");
    }
    assertThat(database.isTransactionActive()).as("a read has no transaction of its own to leave open").isFalse();
  }

  @Test
  void aWriteInsideACallersTransactionJoinsItAndLeavesItToTheCaller() throws Exception {
    database.begin();
    readToTheEnd("g.addV('N').property('k', 'j')");

    assertThat(database.isTransactionActive()).as("the caller's transaction is still there").isTrue();
    assertThat(count("j")).as("this thread reads its own write").isEqualTo(1);
    assertThat(committedCount("j")).as("not committed yet").isZero();

    database.commit();
    assertThat(committedCount("j")).isEqualTo(1);

    database.begin();
    readToTheEnd("g.addV('N').property('k', 'k2')");
    database.rollback();
    assertThat(committedCount("k2")).as("the caller rolled back").isZero();
  }

  @Test
  void aWriteInsideTransactionBlockCommitsWithTheBlock() throws Exception {
    database.transaction(() -> {
      readToTheEnd("g.addV('N').property('k', 'blk')");
      assertThat(database.isTransactionActive()).isTrue();
    });

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("blk")).isEqualTo(1);
  }

  @Test
  void queryStillRefusesAWrite() throws Exception {
    assertThatThrownBy(() -> database.query("gremlin", "g.addV('N').property('k', 'q')")).isInstanceOf(QueryNotIdempotentException.class);

    assertThat(database.isTransactionActive()).isFalse();
    assertThat(committedCount("q")).isZero();
  }

  /**
   * The commit of the statement goes through the instance the database is served through, which under HA is the replicated wrapper: a
   * commit on the raw instance is applied on the leader and never reaches the followers (#5492).
   */
  @Test
  void theStatementCommitsThroughTheWrappedDatabaseInstance() throws Exception {
    final LocalDatabase local = (LocalDatabase) DatabaseInternal.unwrap(database);
    final DatabaseInternal original = local.getWrappedDatabaseInstance();
    final AtomicInteger commits = new AtomicInteger();
    final DatabaseInternal counting = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if (method.getName().equals("commit") && method.getParameterCount() == 0)
            commits.incrementAndGet();
          try {
            return method.invoke(original, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });

    local.setWrappedDatabaseInstance(counting);
    try {
      database.command("gremlin", "g.addV('N').property('k', 'w')").close();
    } finally {
      local.setWrappedDatabaseInstance(original);
    }

    assertThat(commits.get()).as("the one commit of the statement went through the wrapper").isEqualTo(1);
    assertThat(committedCount("w")).isEqualTo(1);
  }

  private void readToTheEnd(final String gremlin) {
    try (final ResultSet rs = database.command("gremlin", gremlin)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private long count(final String key) {
    return countIn(database, key);
  }

  private static long countIn(final Database db, final String key) {
    long count = 0;
    try (final ResultSet rs = db.query("sql", "SELECT FROM N WHERE k = ?", key)) {
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
    }
    return count;
  }

  /** What another thread reads: only what is committed. */
  private long committedCount(final String key) throws Exception {
    return otherThread.submit(() -> count(key)).get(30, TimeUnit.SECONDS);
  }
}
