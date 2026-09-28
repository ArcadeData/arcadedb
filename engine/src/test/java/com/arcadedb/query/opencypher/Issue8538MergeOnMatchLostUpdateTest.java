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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8538: {@code MERGE (c:C {id: $id}) ON MATCH SET c.n = c.n + 1} lost an increment under READ_COMMITTED, with
 * both transactions committing. The MERGE action evaluated the right-hand side against the record the MERGE matched,
 * and only then called {@code modify()}, which pins the vertex page and silently reloads the record when that page
 * moved on. A transaction that committed between the match and the write was therefore overwritten with a value
 * computed from the older snapshot, and the reload had already made the record image the commit checks look current.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8538MergeOnMatchLostUpdateTest {
  private static final String MERGE_ONE  = "MERGE (c:C {id: $id}) ON CREATE SET c.n = 1 ON MATCH SET c.n = c.n + 1 RETURN c.n AS n";
  private static final String MERGE_MANY = "UNWIND $ids AS id MERGE (c:C {id: id}) ON CREATE SET c.n = 1 ON MATCH SET c.n = c.n + 1 RETURN c.id AS id, c.n AS n";

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

    if (committed) {
      assertThat(written.get()).as("the MERGE must have built on the concurrent increment").isEqualTo(2);
      assertThat(readN("c0")).isEqualTo(2);
    } else
      assertThat(readN("c0")).isEqualTo(1);
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

    try {
      runWithConcurrentCommitAfterRead(() -> database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c:Hot", Map.of())
          .close(), 1, "MATCH (c:C {id: 'c0'}) DETACH DELETE c");
    } catch (final RuntimeException e) {
      // Refusing the write is the expected outcome; what must never happen is the vertex coming back
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM C")) {
      assertThat(((Number) rs.next().getProperty("total")).longValue()).isZero();
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
    return runWithConcurrentCommitAfterRead(body, 1, "MATCH (c:C {id: 'c0'}) SET c.n = c.n + 1");
  }

  /**
   * Runs {@code body} in a READ_COMMITTED transaction of this thread. Right after the {@code readNumber}-th C record read
   * by {@code body}, another transaction runs {@code concurrentCommand} and commits before {@code body} goes on.
   *
   * @return whether the transaction committed; {@code false} when it was refused with a retryable conflict
   */
  private boolean runWithConcurrentCommitAfterRead(final Runnable body, final int readNumber,
      final String concurrentCommand) {
    final Thread bodyThread = Thread.currentThread();
    final AtomicBoolean armed = new AtomicBoolean(true);
    final AtomicInteger reads = new AtomicInteger();
    final AtomicReference<Throwable> concurrentFailure = new AtomicReference<>();
    final AfterRecordReadListener interleave = record -> {
      if (Thread.currentThread() == bodyThread && reads.incrementAndGet() >= readNumber && armed.compareAndSet(true,
          false)) {
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
      }
      return record;
    };
    database.getSchema().getType("C").getEvents().registerListener(interleave);

    boolean committed;
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
      body.run();
      database.commit();
      committed = true;
    } catch (final RuntimeException e) {
      // The engine may report the conflict wrapped in the command's own exception
      if (!isConflict(e))
        throw e;
      committed = false;
    } finally {
      if (database.isTransactionActive())
        database.rollback();
      database.getSchema().getType("C").getEvents().unregisterListener(interleave);
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
  void concurrentMergeIncrementsAreNeverLost() throws Exception {
    final int writers = 8;
    final int batches = 50;
    // One hot record: the one each transaction touches first, which is the record the reporter saw lose. The query
    // keeps the reporter's UNWIND shape.
    final List<String> ids = List.of("c0");

    for (int round = 0; round < 2; round++) {
      database.transaction(() -> database.command("sql", "DELETE FROM C"));

      final AtomicInteger committedTx = new AtomicInteger();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Map<String, List<Integer>> writtenValues = new ConcurrentHashMap<>();
      final CountDownLatch start = new CountDownLatch(1);
      final List<Thread> threads = new ArrayList<>();

      for (int w = 0; w < writers; w++) {
        final Thread thread = new Thread(() -> {
          try {
            start.await();
            for (int b = 0; b < batches; b++) {
              for (int attempt = 0; ; attempt++) {
                final Map<String, Integer> written = new HashMap<>();
                try {
                  database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
                  try (final ResultSet rs = database.command("cypher", MERGE_MANY, Map.of("ids", ids))) {
                    while (rs.hasNext()) {
                      final Result row = rs.next();
                      written.put(row.getProperty("id"), ((Number) row.getProperty("n")).intValue());
                    }
                  }
                  database.commit();
                  committedTx.incrementAndGet();
                  for (final Map.Entry<String, Integer> e : written.entrySet())
                    writtenValues.computeIfAbsent(e.getKey(), k -> Collections.synchronizedList(new ArrayList<>()))
                        .add(e.getValue());
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
      for (final String id : ids) {
        assertThat(writtenValues.get(id)).as("values committed to %s", id).doesNotHaveDuplicates();
        assertThat(readN(id)).as("final n of %s", id).isEqualTo(writers * batches);
      }
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
