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

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue8538MergeOnMatchLostUpdateTest {
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

    // The rewrite copies the latest committed record, which no longer exists: the write is refused
    assertThatThrownBy(() -> runWithConcurrentCommitAfterRead(
        () -> database.command("cypher", "MATCH (c:C {id: 'c0'}) SET c:Hot", Map.of()).close(), 1,
        "MATCH (c:C {id: 'c0'}) DETACH DELETE c")).hasCauseInstanceOf(RecordNotFoundException.class);

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
