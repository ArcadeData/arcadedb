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
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Concurrent Cypher writes: MERGE races, lost updates and automatic retry on write conflicts.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherConcurrentWriteTest {
  /**
   * Regression test for issue #6367 ("Small self-loop CREATE/MERGE query returns DeadlockDetected").
   * <p>
   * {@code CreateStep} wraps its writes in {@code database.transaction(..., true)}, which both joins
   * an already-active transaction (an explicit {@code BEGIN}, or the HTTP auto-commit wrapper in
   * {@code DatabaseAbstractHandler}) <em>and</em>, when it owns the transaction itself, retries
   * automatically on {@code NeedRetryException}/{@code ConcurrentModificationException} up to
   * {@link GlobalConfiguration#TX_RETRIES}. {@code MergeStep} (and {@code SetStep}, {@code DeleteStep},
   * {@code RemoveStep}, {@code ForeachStep}) instead call {@code begin()}/{@code commit()} directly, with
   * no retry at all.
   * <p>
   * Over HTTP this gap is invisible: {@code DatabaseAbstractHandler.executeInTransaction()} wraps the
   * *entire* autocommit command in one outer retry-capable transaction, so every write clause simply
   * joins it and a conflict retries the whole request. Bolt's {@code BoltNetworkExecutor.handleRun()}
   * calls {@code database.command(...)} directly with no such wrapper, so a write clause that owns its
   * own mini-transaction (the normal case for an autocommit Bolt {@code RUN}) has nothing to retry an
   * MVCC conflict with: the raw {@code NeedRetryException} propagates straight out and is reported to
   * the Bolt client as {@code Neo.TransientError.Transaction.DeadlockDetected} instead of being retried
   * transparently, the way {@code CreateStep} (and HTTP) already do.
   * <p>
   * This test drives concurrent, <b>unwrapped</b> {@code database.command("opencypher", ...)} calls
   * containing a standalone find-or-create {@code MERGE} against the same key - exactly the shape
   * {@code BoltNetworkExecutor.handleRun} uses for an autocommit write, and the most common real-world
   * use of {@code MERGE}. Before the fix this reliably surfaces
   * {@code NeedRetryException}/{@code ConcurrentModificationException} to the caller under contention;
   * after the fix every call succeeds because {@code MergeStep} retries on its own, exactly like
   * {@code CreateStep} already does - and MERGE's own match-or-create semantics (unaffected by this
   * change) still guarantee exactly one vertex is ever created.
   */
  @Nested
  class Issue6367MergeStepAutoRetry {
    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-6367-merge-retry-test");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      // A handful of extra attempts keeps this reliable under CI contention without masking a genuine
      // regression: pre-fix, MergeStep does not retry at all, so raising the attempt count here changes
      // nothing for the unfixed code (every attempt is really attempt #1) but avoids a false failure
      // from ordinary retry exhaustion post-fix on a slow runner.
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 20);
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void mergeFindOrCreateRetriesOnMvccConflictWithoutAnOuterTransactionWrapper() throws Exception {
      // A UNIQUE index on the matched property is what gives MERGE's find-or-create an atomicity guarantee
      // under concurrency in the first place (mirrors MergeInsertSlowdownTest's schema): without it, two
      // threads can both legitimately find no match and both create, since nothing conflicts. With it, a
      // losing thread's create collides on the index and mergeSingleNodeAll() re-runs the match, which is
      // the scenario this test drives contention through.
      database.command("sql", "CREATE VERTEX TYPE Cnt6367 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY Cnt6367.id IF NOT EXISTS INTEGER");
      database.command("sql", "CREATE INDEX Cnt6367_PK IF NOT EXISTS ON Cnt6367 (id) UNIQUE");

      final int threads = 8;
      final int iterations = 25;

      final AtomicLong successCount = new AtomicLong();
      final AtomicLong conflictErrors = new AtomicLong();
      final AtomicLong otherErrors = new AtomicLong();

      final CountDownLatch start = new CountDownLatch(1);
      final CountDownLatch done = new CountDownLatch(threads);

      for (int t = 0; t < threads; t++) {
        final Thread worker = new Thread(() -> {
          try {
            start.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          for (int i = 0; i < iterations; i++) {
            try {
              // Mirrors BoltNetworkExecutor.handleRun: a bare, unwrapped autocommit command - no outer
              // database.transaction() call here, exactly like Bolt's RUN handling for a write query.
              // Every thread races to find-or-create the SAME node: only one iteration across every
              // thread actually creates it, the rest match it.
              try (final ResultSet rs = database.command("opencypher",
                  "MERGE (a:Cnt6367 {id:1}) RETURN a")) {
                assertThat(rs.hasNext()).isTrue();
                rs.next();
              }
              successCount.incrementAndGet();
            } catch (final Exception e) {
              if (CauseChain.find(e, NeedRetryException.class) != null)
                // Pre-fix: MergeStep has no retry of its own, so ordinary contention on the shared node
                // surfaces here directly (wrapped in CommandExecutionException by
                // OpenCypherQueryEngine.command()) - the same shape of failure Bolt reports as
                // DeadlockDetected. Bolt's own BoltNetworkExecutor.isRetryableConflict() classifies
                // conflicts the same way, via CauseChain, so this mirrors what actually reaches the wire.
                conflictErrors.incrementAndGet();
              else
                otherErrors.incrementAndGet();
            }
          }
          done.countDown();
        }, "issue-6367-merge-worker-" + t);
        worker.start();
      }

      start.countDown();
      done.await();

      assertThat(otherErrors.get()).as("unexpected non-conflict errors").isEqualTo(0);
      assertThat(conflictErrors.get())
          .as("MergeStep must retry an MVCC conflict on its own mini-transaction, exactly like CreateStep, "
              + "instead of surfacing it to an unwrapped autocommit caller (issue #6367)")
          .isEqualTo(0);
      assertThat(successCount.get()).isEqualTo((long) threads * iterations);

      final long vertexCount;
      try (final ResultSet rs = database.query("cypher", "MATCH (a:Cnt6367) RETURN count(a) AS c")) {
        vertexCount = ((Number) rs.next().getProperty("c")).longValue();
      }
      assertThat(vertexCount)
          .as("MERGE's match-or-create semantics must still hold under contention: exactly one vertex, never a duplicate")
          .isEqualTo(1);
    }
  }

  /**
   * Issue #8538: {@code MERGE (c:C {id: $id}) ON MATCH SET c.n = c.n + 1} lost an increment under READ_COMMITTED, with
   * both transactions committing. The MERGE action evaluated the right-hand side against the record the MERGE matched,
   * and only then called {@code modify()}, which pins the vertex page and silently reloads the record when that page
   * moved on. A transaction that committed between the match and the write was therefore overwritten with a value
   * computed from the older snapshot, and the reload had already made the record image the commit checks look current.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // A hang detector, not a latency bound: an interleaving that deadlocks must fail the class, not stall the build
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @Nested
  class Issue8538MergeOnMatchLostUpdate {
    private static final String MERGE_ONE  =
        "MERGE (c:C {id: $id}) ON CREATE SET c.n = 1 ON MATCH SET c.n = c.n + 1 RETURN c.n AS n";
    private static final String MERGE_MANY =
        "UNWIND $ids AS id MERGE (c:C {id: id}) ON CREATE SET c.n = 1 ON MATCH SET c.n = c.n + 1 RETURN c.id AS id, c.n AS n";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-8538-merge-lost-update");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.command("sqlscript", "CREATE VERTEX TYPE C; CREATE PROPERTY C.id STRING; CREATE INDEX ON C (id) UNIQUE;");
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    /**
     * Deterministic interleaving: a concurrent increment commits right after the MERGE has matched the record and before
     * its ON MATCH SET runs. The MERGE must build on that commit, never write back the value computed from the record it
     * matched. A retryable conflict is accepted too (the MERGE then owes no increment): both are correct outcomes, so do
     * not tighten the assertion to one of them.
     */
    @Test
    void onMatchSetSeesTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final AtomicInteger written = new AtomicInteger();
      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> {
        try (final ResultSet rs = database.command("cypher", MERGE_ONE, Map.of("id", "c0"))) {
          written.set(((Number) rs.next().getProperty("n")).intValue());
        }
      });

      // The reload happens before the write pins anything, so the MERGE builds on the concurrent commit and commits: a
      // refusal here would be correct but needlessly costly for the hot MERGE path, so it is asserted against
      assertThat(committed).isTrue();
      assertThat(written.get()).as("the MERGE must have built on the concurrent increment").isEqualTo(2);
      assertThat(readN("c0")).isEqualTo(2);
    }

    /**
     * An expression target names the record through the row, so it must be reloaded as a plain variable target is: the
     * right-hand side reads the same record through {@code c}.
     */
    @Test
    void onMatchSetOnAnExpressionTargetSeesTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MERGE (c:C {id: 'c0'}) ON MATCH SET (CASE WHEN true THEN c END).n = c.n + 1", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * The same for a stand-alone SET, whose expression targets were not reloaded either.
     */
    @Test
    void setOnAnExpressionTargetSeesTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MATCH (c:C {id: 'c0'}) SET (CASE WHEN true THEN c END).n = c.n + 1", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * A MERGE action writing two records, where the first pass judges one of them a no-op only because it read the other
     * one stale: once the reload of the other one changes the right-hand side, that record is written too, so it must be
     * reloaded as well before the value written to it is evaluated.
     */
    @Test
    void multiTargetOnMatchSetReloadsATargetTheSecondPassWrites() {
      // d0 lives in another type, so on another page: pinning c0's page does not cover it (a record on the SAME page is
      // already refused by the #6950 image check, since only the first record of a page is reloaded on modify()).
      database.command("sql", "CREATE VERTEX TYPE D");
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', k: 0}), (:D {id: 'd0', n: 0})"));

      // The MATCH reads d0 before the MERGE reads c0, the first C record read that triggers the concurrent commit: the
      // MERGE holds a stale image of both.
      final boolean committed = runWithConcurrentCommitAfterRead(() -> database.command("cypher",
              "MATCH (d:D {id: 'd0'}) MERGE (c:C {id: 'c0'}) ON MATCH SET c.k = c.k + 1, d.n = d.n + c.k", Map.of()).close(), 1,
          "MATCH (c:C {id: 'c0'}), (d:D {id: 'd0'}) SET c.k = c.k + 1, d.n = d.n + 1");

      if (committed) {
        assertThat(readProperty("C", "c0", "k")).isEqualTo(2);
        assertThat(readProperty("D", "d0", "n")).as("d.n = the concurrent 1 + the reloaded c.k 1").isEqualTo(2);
      } else {
        assertThat(readProperty("C", "c0", "k")).isEqualTo(1);
        assertThat(readProperty("D", "d0", "n")).isEqualTo(1);
      }
    }

    /**
     * An expression target that is not a row variable loads a fresh copy of its record at each evaluation, so its reload
     * never "settles": the clause must still end, and write the value computed from the latest committed record.
     */
    @Test
    void expressionTargetLoadedAtEachEvaluationTerminates() {
      database.command("sql", "CREATE EDGE TYPE R");
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})-[:R]->(:C {id: 'c1', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MATCH (:C {id: 'c0'})-[r:R]->() SET (startNode(r)).n = startNode(r).n + 1", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * An expression target reached through a collection ({@code docs[0]}) is bound by no alias the reload can replace,
     * so its right-hand side may keep reading the stale copy: the write must then be refused as a conflict, never commit
     * a value computed from that copy.
     */
    @Test
    void expressionTargetInsideACollectionNeverLosesTheConcurrentIncrement() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MERGE (c:C {id: 'c0'}) WITH [c] AS docs SET (docs[0]).n = docs[0].n + 1", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * The same through a MERGE action.
     */
    @Test
    void onMatchSetOnATargetInsideACollectionNeverLosesTheConcurrentIncrement() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MATCH (x:C {id: 'c0'}) WITH [x] AS docs MERGE (c:C {id: 'c0'}) ON MATCH SET (docs[0]).n = docs[0].n + 1",
          Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * An expression target whose choice depends on a record the reload refreshes: re-evaluated against the reloaded c,
     * the CASE switches to d, which must then be reloaded too before its value is computed, however late in the rounds
     * the switch happens.
     */
    @Test
    void expressionTargetSwitchingToAnotherRecordReloadsItToo() {
      database.command("sql", "CREATE VERTEX TYPE D");
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', k: 0, n: 0}), (:D {id: 'd0', n: 0})"));

      // The MATCH reads d0 before the MERGE reads c0, which triggers the concurrent commit: both images are stale.
      final boolean committed = runWithConcurrentCommitAfterRead(() -> database.command("cypher",
              "MATCH (d:D {id: 'd0'}) MERGE (c:C {id: 'c0'}) "
                  + "ON MATCH SET (CASE WHEN c.k = 0 THEN c ELSE d END).n = CASE WHEN c.k = 0 THEN c.n ELSE d.n END + 1",
              Map.of()).close(), 1,
          "MATCH (c:C {id: 'c0'}), (d:D {id: 'd0'}) SET c.k = 1, d.n = d.n + 1");

      assertThat(readProperty("C", "c0", "n")).as("c0 is no longer the target").isEqualTo(0);
      assertThat(readProperty("D", "d0", "n")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * A dynamic key that is null against the stale record but not against the reloaded one: the re-evaluation must
     * write it.
     */
    @Test
    void dynamicKeyNullOnlyAgainstTheStaleRecordIsWritten() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0, k: 0})"));

      final boolean committed = runWithConcurrentCommitAfterRead(() -> database.command("cypher",
              "MERGE (c:C {id: 'c0'}) ON MATCH SET c.n = c.n + 1, c[CASE WHEN c.k = 0 THEN null ELSE 'm' END] = 5",
              Map.of()).close(), 1,
          "MATCH (c:C {id: 'c0'}) SET c.k = 1, c.n = c.n + 1");

      assertThat(committed).isTrue();
      assertThat(readN("c0")).isEqualTo(2);
      assertThat(readProperty("C", "c0", "m")).isEqualTo(5);
    }

    /**
     * A row binding the record twice (c and its alias d): the reload must reach the alias the right-hand side reads.
     */
    @Test
    void setReadingTheTargetThroughAnAliasSeesTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MATCH (c:C {id: 'c0'}) WITH c, c AS d SET c.n = d.n + 1", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * The same through a MERGE action, whose record is also bound by an earlier MATCH.
     */
    @Test
    void onMatchSetReadingTheTargetThroughAnAliasSeesTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MATCH (d:C {id: 'c0'}) MERGE (c:C {id: 'c0'}) ON MATCH SET c.n = d.n + 1", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * A map item ({@code +=}) in a MERGE action is a write too, so its right-hand side must be evaluated against the
     * reloaded record.
     */
    @Test
    void onMatchMapMergeSeesTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(() -> database.command("cypher",
          "MERGE (c:C {id: 'c0'}) ON MATCH SET c += {n: c.n + 1}", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(committed ? 2 : 1);
    }

    /**
     * A label write on a vertex deleted concurrently since the read must not resurrect it from the copy the row holds.
     */
    @Test
    void labelWriteOnAConcurrentlyDeletedVertexDoesNotResurrectIt() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      // The rewrite copies the latest committed record, which no longer exists: the write is refused as a retryable
      // conflict, which the helper reports as not committed
      final boolean committed = runWithConcurrentCommitAfterRead(
          () -> database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c:Hot", Map.of()).close(), 1,
          "MATCH (c:C {id: 'c0'}) DETACH DELETE c");
      assertThat(committed).isFalse();

      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM C")) {
        assertThat(((Number) rs.next().getProperty("total")).longValue()).isZero();
      }
    }

    /**
     * The documented price of #4474: the no-op decision is taken on the record the MERGE read, so when a concurrent commit
     * changed the property in between, the concurrent value stays. No committed write is lost; this pins the behavior so
     * that it is changed on purpose, not by accident.
     */
    @Test
    void unchangedOnMatchSetKeepsAValueCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0, kind: 'const'})"));

      final boolean committed = runWithConcurrentCommitAfterRead(
          () -> database.command("cypher", "MERGE (c:C {id: 'c0'}) ON MATCH SET c.kind = 'const'", Map.of()).close(), 1,
          "MATCH (c:C {id: 'c0'}) SET c.kind = 'other'");

      assertThat(committed).isTrue();
      try (final ResultSet rs = database.query("sql", "SELECT kind FROM C WHERE id = 'c0'")) {
        assertThat(rs.next().<String>getProperty("kind")).isEqualTo("other");
      }
    }

    /**
     * The engine contract SetClauseApplier.reloadDocument() relies on to tell a stale row from a current one: when the
     * page of a vertex moved on since it was read, modify() reloads it by REPLACING its buffer. If modify() ever refreshed
     * the buffer in place instead, the applier would stop re-evaluating after a reload and the lost update would be back;
     * this test fails first, and names the cause.
     */
    @Test
    void modifyReplacesTheBufferOfAVertexWhosePageMovedOn() throws Exception {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      try {
        final Vertex read;
        try (final ResultSet rs = database.query("sql", "SELECT FROM C WHERE id = 'c0'")) {
          read = rs.next().getVertex().orElseThrow();
        }
        final Binary readImage = ((BaseRecord) read).getBuffer();

        final Thread concurrent = new Thread(
            () -> database.transaction(() -> database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c.n = c.n + 1")));
        concurrent.start();
        concurrent.join();

        final MutableVertex mutable = read.modify();
        assertThat(((BaseRecord) read).getBuffer()).isNotSameAs(readImage);
        assertThat(mutable.getInteger("n")).isEqualTo(1);
      } finally {
        database.rollback();
      }
    }

    /**
     * A label write on a vertex this transaction already modified: modify() answers with the transaction's own copy, so
     * the rewrite must carry that copy's properties, not the committed ones.
     */
    @Test
    void labelWriteCarriesAPropertyWrittenEarlierInTheSameTransaction() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      database.transaction(() -> {
        database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c.n = 5").close();
        database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c:Hot").close();
      });

      assertThat(readN("c0")).isEqualTo(5);
      assertThat(countHot()).isEqualTo(1);
    }

    /**
     * A relationship MERGE: an edge does not pin its page on modify() nor reload there, so a stale edge is caught by the
     * save-time #6950 record-image check instead. The increment must survive either way.
     */
    @Test
    void relationshipOnMatchSetNeverLosesTheConcurrentIncrement() {
      database.command("sql", "CREATE EDGE TYPE R");
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0'})-[:R {n: 0}]->(:C {id: 'c1'})"));

      final boolean committed = runWithConcurrentCommitAfterRead(() -> database.command("cypher",
              "MATCH (a:C {id: 'c0'}), (b:C {id: 'c1'}) MERGE (a)-[r:R]->(b) ON MATCH SET r.n = r.n + 1", Map.of()).close(),
          "R", 1, "MATCH (:C {id: 'c0'})-[r:R]->() SET r.n = r.n + 1");

      try (final ResultSet rs = database.query("cypher", "MATCH ()-[r:R]->() RETURN r.n AS n")) {
        assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(committed ? 2 : 1);
      }
    }

    /**
     * A record found through an index carries no page version, so modify() reloads it on every write. The right-hand side
     * must still be evaluated once when nothing changed (the reload read back the same bytes), and again only when a
     * concurrent commit really changed the record.
     */
    @Test
    void rightHandSideIsEvaluatedAgainOnlyWhenTheReloadChangedTheRecord() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));
      final CountingPlusOne plusOne = new CountingPlusOne();
      database.getSchema().registerFunctionLibrary(new SingleFunctionLibrary(plusOne));
      try {
        final String merge = "MERGE (c:C {id: 'c0'}) ON MATCH SET c.n = " + COUNTING_LIBRARY + ".plusOne(c.n)";

        database.transaction(() -> database.command("cypher", merge).close());
        assertThat(plusOne.invocations.get()).as("uncontended write").isEqualTo(1);
        assertThat(readN("c0")).isEqualTo(1);

        plusOne.invocations.set(0);
        final boolean committed = runWithConcurrentIncrementAfterFirstRead(
            () -> database.command("cypher", merge).close());
        assertThat(committed).isTrue();
        assertThat(plusOne.invocations.get()).as("write over a concurrent commit").isEqualTo(2);
        assertThat(readN("c0")).isEqualTo(3);
      } finally {
        database.getSchema().unregisterFunctionLibrary(COUNTING_LIBRARY);
      }
    }

    /**
     * A label write rewrites the vertex under a new type, copying its properties: the copy must be taken from the latest
     * committed record, or the concurrent increment vanishes with the deleted original.
     */
    @Test
    void onMatchLabelWriteKeepsTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(
          () -> database.command("cypher", "MERGE (c:C {id: 'c0'}) ON MATCH SET c:Hot", Map.of()).close());

      // Committed or refused, the concurrent increment must survive either way. The rewritten vertex is still found
      // through C: the composite type of C + Hot extends C.
      assertThat(readN("c0")).isEqualTo(1);
      assertThat(countHot()).isEqualTo(committed ? 1 : 0);
    }

    /**
     * The same for a stand-alone SET of a label.
     */
    @Test
    void labelWriteKeepsTheIncrementCommittedAfterTheMatch() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(
          () -> database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c:Hot", Map.of()).close());

      assertThat(readN("c0")).isEqualTo(1);
      assertThat(countHot()).isEqualTo(committed ? 1 : 0);
    }

    /**
     * Issue #4474 must survive the fix: an ON MATCH SET that re-asserts the value the record already holds writes nothing
     * and pins nothing, so a concurrent commit to the same record never turns it into a conflict.
     */
    @Test
    void unchangedOnMatchSetStaysConflictFree() {
      database.transaction(() -> database.command("cypher", "CREATE (:C {id: 'c0', n: 0, kind: 'const'})"));

      final boolean committed = runWithConcurrentIncrementAfterFirstRead(
          () -> database.command("cypher", "MERGE (c:C {id: 'c0'}) ON MATCH SET c.kind = 'const'", Map.of()).close());

      assertThat(committed).as("a no-op MERGE action must not conflict").isTrue();
      assertThat(readN("c0")).isEqualTo(1);
    }

    /**
     * Runs {@code body} while, right after its first record read, another transaction increments {@code c0.n} and commits.
     */
    private boolean runWithConcurrentIncrementAfterFirstRead(final Runnable body) {
      return runWithConcurrentCommitAfterRead(body, "C", 1, "MATCH (c:C {id: 'c0'}) SET c.n = c.n + 1");
    }

    /**
     * Runs {@code body} in a READ_COMMITTED transaction of this thread. Right after the {@code readNumber}-th C record read
     * by {@code body}, another transaction runs {@code concurrentCommand} and commits before {@code body} goes on.
     *
     * @return whether the transaction committed; {@code false} when it was refused with a retryable conflict
     */
    private boolean runWithConcurrentCommitAfterRead(final Runnable body, final int readNumber,
        final String concurrentCommand) {
      return runWithConcurrentCommitAfterRead(body, "C", readNumber, concurrentCommand);
    }

    /**
     * As {@link #runWithConcurrentCommitAfterRead(Runnable, int, String)}, counting the reads of {@code typeName} records.
     */
    private boolean runWithConcurrentCommitAfterRead(final Runnable body, final String typeName, final int readNumber,
        final String concurrentCommand) {
      final Thread bodyThread = Thread.currentThread();
      final AtomicBoolean armed = new AtomicBoolean(true);
      final AtomicInteger reads = new AtomicInteger();
      final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
      // the commit is made on its own thread, and waited for
      final Runnable concurrentCommit = () -> {
        final Thread concurrent = new Thread(() -> {
          try {
            database.transaction(() -> database.command("cypher", concurrentCommand));
          } catch (final Throwable t) {
            concurrentFailure.set(t);
          }
        });
        concurrent.start();
        try {
          concurrent.join();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      };
      // a read made by an index lookup is part of its consistency window (#9397): a commit there is seen by the lookup's
      // retry and is no stale read. The commit of such a read is made once the lookup has returned its records instead
      final AtomicBoolean afterLookup = new AtomicBoolean();
      ConsistentKeyLookup.setAfterLookupObserver(() -> {
        if (Thread.currentThread() == bodyThread && afterLookup.get() && armed.compareAndSet(true, false))
          concurrentCommit.run();
      });
      final AfterRecordReadListener interleave = record -> {
        if (Thread.currentThread() == bodyThread && reads.incrementAndGet() >= readNumber && armed.get()) {
          if (StackWalker.getInstance().walk(frames -> frames.anyMatch(f -> f.getClassName().equals(ConsistentKeyLookup.class.getName()))))
            afterLookup.set(true);
          else if (armed.compareAndSet(true, false))
            concurrentCommit.run();
        }
        return record;
      };
      database.getSchema().getType(typeName).getEvents().registerListener(interleave);

      boolean committed;
      try {
        database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
        body.run();
        database.commit();
        committed = true;
      } catch (final RuntimeException e) {
        // The engine may report the conflict wrapped in the command's own exception
        if (!isConflict(e)) {
          if (concurrentFailure.get() != null)
            e.addSuppressed(concurrentFailure.get());
          throw e;
        }
        committed = false;
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        database.getSchema().getType(typeName).getEvents().unregisterListener(interleave);
        ConsistentKeyLookup.setAfterLookupObserver(null);
      }

      assertThat(armed.get()).as("the concurrent increment must have been interleaved").isFalse();
      assertThat(concurrentFailure.get()).isNull();
      return committed;
    }

    /**
     * The reporter's shape: concurrent explicit READ_COMMITTED transactions, each running one MERGE over the same ids and
     * retried on a conflict. Every committed transaction increments every id once, and no two committed transactions
     * may write the same value to the same record.
     */
    @Test
    @Tag("slow") // contention-bound retries on one hot record: the deterministic interleavings above cover the regression
    void concurrentMergeIncrementsAreNeverLost() throws Exception {
      final int writers = 8;
      final int batches = 50;
      // One hot record: the one each transaction touches first, which is the record the reporter saw lose. The query
      // keeps the reporter's UNWIND shape.
      final Map<String, Object> params = Map.of("ids", List.of("c0"));

      for (int round = 0; round < 2; round++) {
        database.transaction(() -> database.command("sql", "DELETE FROM C"));

        final AtomicInteger committedTx = new AtomicInteger();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final List<Integer> writtenValues = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();

        for (int w = 0; w < writers; w++) {
          final Thread thread = new Thread(() -> {
            try {
              start.await();
              for (int b = 0; b < batches; b++) {
                for (int attempt = 0; ; attempt++) {
                  try {
                    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
                    final int written;
                    try (final ResultSet rs = database.command("cypher", MERGE_MANY, params)) {
                      written = ((Number) rs.next().getProperty("n")).intValue();
                    }
                    database.commit();
                    committedTx.incrementAndGet();
                    writtenValues.add(written);
                    break;
                  } catch (final NeedRetryException | DuplicatedKeyException e) {
                    if (database.isTransactionActive())
                      database.rollback();
                    if (attempt > 10_000)
                      throw e;
                  }
                }
              }
            } catch (final Throwable t) {
              failure.compareAndSet(null, t);
              if (database.isTransactionActive())
                database.rollback();
            }
          });
          threads.add(thread);
          thread.start();
        }

        start.countDown();
        for (final Thread thread : threads)
          thread.join();

        assertThat(failure.get()).isNull();
        assertThat(committedTx.get()).isEqualTo(writers * batches);
        assertThat(writtenValues).as("values committed to c0").doesNotHaveDuplicates();
        assertThat(readN("c0")).isEqualTo(writers * batches);
      }
    }

    private static final String COUNTING_LIBRARY = "issue8538";

    /**
     * {@code issue8538.plusOne(x)}: returns x + 1, counting its invocations.
     */
    private static final class CountingPlusOne implements FunctionDefinition {
      private final AtomicInteger invocations = new AtomicInteger();

      @Override
      public String getName() {
        return "plusOne";
      }

      @Override
      public Object execute(final Object... parameters) {
        invocations.incrementAndGet();
        return ((Number) parameters[0]).longValue() + 1;
      }
    }

    private record SingleFunctionLibrary(FunctionDefinition function) implements FunctionLibraryDefinition<FunctionDefinition> {
      @Override
      public String getName() {
        return COUNTING_LIBRARY;
      }

      @Override
      public Iterable<FunctionDefinition> getFunctions() {
        return List.of(function);
      }

      @Override
      public FunctionDefinition getFunction(final String functionName) {
        if (!function.getName().equals(functionName))
          throw new IllegalArgumentException("Function '" + functionName + "' not defined");
        return function;
      }

      @Override
      public boolean hasFunction(final String functionName) {
        return function.getName().equals(functionName);
      }

      @Override
      public FunctionLibraryDefinition<FunctionDefinition> registerFunction(final FunctionDefinition registerFunction) {
        throw new UnsupportedOperationException();
      }

      @Override
      public FunctionLibraryDefinition<FunctionDefinition> unregisterFunction(final String functionName) {
        throw new UnsupportedOperationException();
      }
    }

    private static boolean isConflict(final Throwable e) {
      for (Throwable t = e; t != null; t = t.getCause())
        if (t instanceof ConcurrentModificationException)
          return true;
      return false;
    }

    private int readN(final String id) {
      return readProperty("C", id, "n");
    }

    private int readProperty(final String type, final String id, final String property) {
      try (final ResultSet rs = database.query("sql", "SELECT " + property + " AS value FROM " + type + " WHERE id = ?", id)) {
        return ((Number) rs.next().getProperty("value")).intValue();
      }
    }

    private long countHot() {
      try (final ResultSet rs = database.query("cypher", "MATCH (c:Hot {id: 'c0'}) RETURN count(c) AS total")) {
        return ((Number) rs.next().getProperty("total")).longValue();
      }
    }
  }

  /**
   * Issue #9487: a write conflict raised while an openCypher {@code SET} was executing escaped
   * {@code Database.transaction(tx, true, retries)} as a {@code CommandExecutionException} wrapping the
   * {@code ConcurrentModificationException}, which is not a {@code NeedRetryException}, so the retry loop never saw it.
   * The same SQL {@code UPDATE} commits every call.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue9487CypherWriteConflictRetry extends TestHelper {
    private static final int THREADS = 4;
    private static final int CALLS   = 300;

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Product.views INTEGER");
      database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          database.command("sql", "INSERT INTO Product SET pid = " + i + ", views = 0");
      });
    }

    @Test
    void sqlUpdateRetriesEveryConflict() throws InterruptedException {
      contendOnTheSameProducts("sql", "UPDATE Product SET views = views + 1 WHERE pid IN [0, 1, 2]");
    }

    @Test
    void cypherSetRetriesEveryConflict() throws InterruptedException {
      contendOnTheSameProducts("opencypher", "MATCH (n:Product) WHERE n.pid IN [0, 1, 2] SET n.views = n.views + 1");
    }

    @Test
    void cypherMergeWithSetRetriesEveryConflict() throws InterruptedException {
      contendOnTheSameProducts("opencypher",
          "MERGE (n:Product {pid: 0}) ON MATCH SET n.views = n.views + 1");
    }

    private void contendOnTheSameProducts(final String language, final String update) throws InterruptedException {
      final Map<String, Integer> escaped = new TreeMap<>();
      final AtomicInteger ok = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();
      for (int t = 0; t < THREADS; t++) {
        final Thread thread = new Thread(() -> {
          for (int i = 0; i < CALLS; i++) {
            try {
              database.transaction(() -> database.command(language, update), true, 50);
              ok.incrementAndGet();
            } catch (final Exception e) {
              final String key = e.getClass().getName() + (e.getCause() == null ? "" : " <- " + e.getCause().getClass().getName());
              synchronized (escaped) {
                escaped.merge(key, 1, Integer::sum);
              }
            }
          }
        });
        threads.add(thread);
        thread.start();
      }
      for (final Thread thread : threads)
        thread.join();

      assertThat(escaped).as("exceptions that escaped the retry loop").isEmpty();
      assertThat(ok.get()).isEqualTo(THREADS * CALLS);
      try (final ResultSet rs = database.query("sql", "SELECT views AS v FROM Product WHERE pid = 0")) {
        assertThat(((Number) rs.next().getProperty("v")).intValue()).isEqualTo(THREADS * CALLS);
      }
    }
  }

  /**
   * Issue #11482: several transactions MERGing the SAME relationship at the same time stored it more than once when its endpoints
   * already had edges. The existence check of a MERGE reads the edge list, and the append that follows commutes with a concurrent
   * append at commit (the edge-append and slot merges), so the second MERGE never learned that the first one had created what it
   * was about to create. Every MERGE of a relationship between two existing vertices is now checked at commit against the ones
   * committed since the transaction began, and a transaction whose MERGE would duplicate one is refused with a retryable conflict:
   * its retry finds the edge.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11482ConcurrentMergeRelationship {
    private static final int THREADS       = 4;
    private static final int RELATIONSHIPS = 60;
    private static final int MAX_ATTEMPTS  = 200;

    private Database database;

    @BeforeEach
    void setUp() {
      database = new DatabaseFactory("./target/databases/Issue11482ConcurrentMergeRelationship").create();
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("R");
      database.getSchema().createEdgeType("S");
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:N {uuid: 'n1'}), (:N {uuid: 'n2'})");
        // An existing edge list on both endpoints: the first edge of a direction rewrites the vertex and was always exact
        database.command("opencypher", "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) CREATE (a)-[:R {uuid: 'seed'}]->(b)");
        // One target per relationship for the tests that need a vertex pair each, every one with an edge list already
        database.command("opencypher",
            "MATCH (hub:N {uuid: 'n1'}) UNWIND range(0, $n - 1) AS i CREATE (hub)-[:S]->(:N {uuid: 't' + toString(i)})",
            Map.of("n", RELATIONSHIPS));
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
        database = null;
      }
    }

    @Test
    void concurrentMergeOfTheSameRelationshipStoresItOnce() throws Exception {
      runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]->(b)");

      assertEachUuidStoredOnce();
    }

    @Test
    void concurrentUndirectedMergeWrittenFromEitherEndStoresItOnce() throws Exception {
      // Half of the clients write the pattern from the other end: an undirected MERGE matches the edge whichever way it points
      runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]-(b)",
          i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (b)-[e:R {uuid: 'e" + i + "'}]-(a)");

      assertEachUuidStoredOnce();
    }

    @Test
    void concurrentMergeWithoutPropertiesStoresOneEdgePerPair() throws Exception {
      // One relationship per vertex pair, identified by its type only: a pair of vertices per index, all sharing a hub
      runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 't" + i + "'}) MERGE (a)-[e:R]->(b)");

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:N {uuid: 'n1'})-[e:R]->(b) WHERE b.uuid STARTS WITH 't' WITH b.uuid AS target, count(e) AS c RETURN count(target) AS targets, sum(c) AS edges")) {
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("targets")).intValue()).isEqualTo(RELATIONSHIPS);
        assertThat(((Number) row.getProperty("edges")).intValue()).isEqualTo(RELATIONSHIPS);
      }
    }

    @Test
    void concurrentMergeMatchingAnyPropertiesFindsAPropertyMerge() throws Exception {
      // Every relationship is merged once with its property and once without any: the second pattern matches the first one's
      // edge, so whichever commits second must find it. Serially, at most one edge per index has no property
      runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 't" + i + "'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]->(b)",
          i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 't" + i + "'}) MERGE (a)-[e:R]->(b)");

      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:N {uuid: 'n1'})-[e:R]->(b) WHERE b.uuid STARTS WITH 't' WITH b.uuid AS target, count(e) AS c "
              + "RETURN count(target) AS targets, max(c) AS most")) {
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("targets")).intValue()).isEqualTo(RELATIONSHIPS);
        // The property-less MERGE committed first leaves room for the property MERGE's own edge, never for a third one
        assertThat(((Number) row.getProperty("most")).intValue()).isLessThanOrEqualTo(2);
      }
    }

    @Test
    void concurrentSqlCreateEdgeIfNotExistsStoresOneEdge() throws Exception {
      runConcurrently(i -> "sql:CREATE EDGE R FROM (SELECT FROM N WHERE uuid = 'n1') TO (SELECT FROM N WHERE uuid = 't" + i
          + "') IF NOT EXISTS");

      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM R WHERE out.uuid = 'n1' AND in.uuid LIKE 't%'")) {
        assertThat(((Number) rs.next().getProperty("c")).intValue()).isEqualTo(RELATIONSHIPS);
      }
    }

    @Test
    void concurrentMergeRelationshipProcedureStoresItOnce() throws Exception {
      runConcurrently(i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) CALL merge.relationship(a, 'R', {uuid: 'e" + i
          + "'}, {}, b) YIELD rel RETURN rel");

      assertEachUuidStoredOnce();
    }

    @Test
    void aMergeThatFindsItsEdgeIsNeverRefused() throws Exception {
      // Everything already exists: the concurrent MERGEs only match, so no attempt may need a retry
      database.transaction(() -> {
        for (int i = 0; i < RELATIONSHIPS; i++)
          database.command("opencypher", "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) CREATE (a)-[:R {uuid: 'e" + i + "'}]->(b)");
      });

      final int retries = runConcurrently(
          i -> "MATCH (a:N {uuid: 'n1'}), (b:N {uuid: 'n2'}) MERGE (a)-[e:R {uuid: 'e" + i + "'}]->(b)");

      assertThat(retries).isZero();
      assertEachUuidStoredOnce();
    }

    private void assertEachUuidStoredOnce() {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH ()-[e:R]->() WHERE e.uuid STARTS WITH 'e' WITH e.uuid AS u, count(*) AS c "
              + "RETURN count(u) AS uuids, sum(c) AS edges, sum(CASE WHEN c > 1 THEN 1 ELSE 0 END) AS duplicated")) {
        final Result row = rs.next();
        assertThat(((Number) row.getProperty("duplicated")).intValue()).isZero();
        assertThat(((Number) row.getProperty("uuids")).intValue()).isEqualTo(RELATIONSHIPS);
        assertThat(((Number) row.getProperty("edges")).intValue()).isEqualTo(RELATIONSHIPS);
      }
    }

    /**
     * Every relationship is written by {@link #THREADS} clients released together by a barrier, each in a transaction of its own that
     * is retried on a retryable conflict, as a driver retries one. The statements alternate between the given spellings by thread.
     *
     * @return how many attempts had to be retried
     */
    @SafeVarargs
    private int runConcurrently(final IntFunction<String>... statements) throws Exception {
      final CyclicBarrier barrier = new CyclicBarrier(THREADS);
      final ExecutorService executor = Executors.newFixedThreadPool(THREADS);
      try {
        final List<Future<Integer>> futures = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
          final IntFunction<String> statement = statements[t % statements.length];
          futures.add(executor.submit(() -> {
            int retries = 0;
            for (int i = 0; i < RELATIONSHIPS; i++) {
              barrier.await(30, TimeUnit.SECONDS);
              final String text = statement.apply(i);
              final String language = text.startsWith("sql:") ? "sql" : "opencypher";
              final String command = text.startsWith("sql:") ? text.substring(4) : text;
              for (int attempt = 0; ; attempt++) {
                try {
                  database.transaction(() -> database.command(language, command), false, 1);
                  break;
                } catch (final NeedRetryException e) {
                  if (attempt >= MAX_ATTEMPTS)
                    throw e;
                  ++retries;
                }
              }
            }
            return retries;
          }));
        }
        int retries = 0;
        for (final Future<Integer> future : futures)
          retries += future.get(5, TimeUnit.MINUTES);
        return retries;
      } finally {
        executor.shutdownNow();
        executor.awaitTermination(30, TimeUnit.SECONDS);
      }
    }
  }
}
