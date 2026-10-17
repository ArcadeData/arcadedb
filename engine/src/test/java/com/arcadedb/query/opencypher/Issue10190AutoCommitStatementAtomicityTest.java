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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10190: a Cypher write statement run through the embedded API with no transaction open is ONE transaction, as it
 * is over HTTP and as Neo4j's implicit transaction is. The write steps used to begin and commit their own, row by row, so
 * a failing statement kept the writes made before the error, a MERGE over several pairs conflicted with its own earlier
 * rows, and a label round trip never returned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10190AutoCommitStatementAtomicityTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      database.getSchema().createEdgeType("R");
      database.getSchema().createEdgeType("S");
    });
  }

  @Test
  void aFailingCreateLeavesNothingBehind() {
    assertThatThrownBy(() -> drain("CREATE (:A {k: 1}) WITH 1 AS z RETURN 1 / (z - 1) AS e")).hasMessageContaining("/ by zero");
    assertThat(graph()).isEmpty();
    assertThat(database.isTransactionActive()).isFalse();
  }

  @Test
  void aSetFailingOnALaterRowUndoesTheEarlierRows() {
    setup("CREATE (:A {k: 0}), (:A {k: 1})");
    assertThatThrownBy(() -> drain("MATCH (n:A) WITH n ORDER BY n.k SET n.i = 10 / (n.k - 1)")).hasMessageContaining("/ by zero");
    assertThat(graph()).containsExactly("[A]{k=0}", "[A]{k=1}");
  }

  @Test
  void aDeleteRefusedOnALaterRowUndoesTheEarlierRows() {
    setup("CREATE (:A {k: 0}), (:A {k: 1})-[:R]->(:A {k: 2})");
    assertThatThrownBy(() -> drain("MATCH (n:A) DELETE n")).hasMessageContaining("still has relationships");
    assertThat(graph()).containsExactly("[A]{k=0}", "[A]{k=1}", "[A]{k=2}");
    assertThat(relationships()).isEqualTo(1);
  }

  @Test
  void aMergeOverPairsCreatesEveryRelationship() {
    setup("CREATE (:A {k: 0}), (:A {k: 1}), (:A {k: 2})");
    drain("MATCH (a:A), (b:A) WHERE a.k < b.k MERGE (a)-[:S]->(b)");
    assertThat(relationships()).isEqualTo(3);
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS) // A HANG DETECTOR: THE STATEMENT USED TO NEVER RETURN
  void aLabelRoundTripReturnsAndKeepsTheLabel() {
    setup("CREATE (:A {k: 0})");
    final List<Result> rows = drain("MATCH (n:A) REMOVE n:A SET n:A RETURN labels(n) AS l");
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().<List<String>>getProperty("l")).containsExactly("A");
    assertThat(graph()).containsExactly("[A]{k=0}");
  }

  @Test
  void aSucceedingStatementIsCommittedAsOneTransaction() {
    setup("CREATE (:A {k: 0}), (:A {k: 1}), (:A {k: 2})");
    // THE SAME STATEMENT IN AN EXPLICIT TRANSACTION IS THE YARDSTICK: ONE PER ROW USED TO BE COMMITTED WITHOUT ONE
    final long beforeExplicit = writeTransactions();
    database.transaction(() -> drain("MATCH (n:A) SET n.j = n.k"));
    final long explicitCommits = writeTransactions() - beforeExplicit;

    final long before = writeTransactions();
    drain("MATCH (n:A) SET n.i = n.k * 10");
    assertThat(writeTransactions() - before).isEqualTo(explicitCommits);
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(graph()).containsExactly("[A]{i=0, j=0, k=0}", "[A]{i=10, j=1, k=1}", "[A]{i=20, j=2, k=2}");
  }

  @Test
  void aStatementInsideAnOpenTransactionIsLeftToItsOwner() {
    setup("CREATE (:A {k: 0})");
    database.begin();
    try {
      drain("MATCH (n:A) SET n.i = 1");
      // THE CALLER'S TRANSACTION IS STILL OPEN AND STILL OWNS THE WRITE
      assertThat(database.isTransactionActive()).isTrue();
    } finally {
      database.rollback();
    }
    assertThat(graph()).containsExactly("[A]{k=0}");
  }

  @Test
  void callInTransactionsStillCommitsItsBatches() {
    setup("UNWIND range(1, 10) AS i CREATE (:A {k: i})");
    drain("MATCH (n:A) CALL { WITH n SET n.done = true } IN TRANSACTIONS OF 3 ROWS");
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) WHERE n.done RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(10L);
    }
    assertThat(database.isTransactionActive()).isFalse();
  }

  /**
   * The commit follows the drain of the write statement, so every write shape must have run before it: each is committed
   * (seen from ANOTHER thread, which has no view of this thread's transaction) and nothing is left open.
   */
  @Test
  void everyWriteShapeIsDrainedBeforeTheCommit() throws Exception {
    final String[][] shapes = {
        { "UNWIND range(1, 3) AS i CREATE (n:B {k: i}) RETURN n.k AS k", "MATCH (n:B) RETURN count(n) AS c", "3" },
        { "MATCH (n:B) WITH n WHERE n.k > 1 SET n.big = true RETURN n", "MATCH (n:B) WHERE n.big RETURN count(n) AS c", "2" },
        { "MATCH (n:B) WITH collect(n) AS ns FOREACH (x IN ns | SET x.f = 1)", "MATCH (n:B) WHERE n.f = 1 RETURN count(n) AS c", "3" },
        { "CREATE (:A {k: 100}) RETURN 1 AS x UNION ALL CREATE (:A {k: 101}) RETURN 2 AS x",
            "MATCH (n:A) WHERE n.k >= 100 RETURN count(n) AS c", "2" },
        { "CALL merge.node(['A'], {k: 200}, {}, {}) YIELD node RETURN node", "MATCH (n:A {k: 200}) RETURN count(n) AS c", "1" },
        { "MATCH (a:A {k: 100}), (b:A {k: 101}) CREATE (a)-[r:R]->(b) RETURN r LIMIT 1", "MATCH ()-[r:R]->() RETURN count(r) AS c", "1" },
        { "CALL { CREATE (:A {k: 300}) } RETURN 1 AS x", "MATCH (n:A {k: 300}) RETURN count(n) AS c", "1" } };

    for (final String[] shape : shapes) {
      final List<Result> rows = drain(shape[0]);
      assertThat(database.isTransactionActive()).as(shape[0]).isFalse();
      assertThat(countFromAnotherThread(shape[1])).as(shape[0] + " (" + rows.size() + " rows)").isEqualTo(Long.parseLong(shape[2]));
    }
  }

  /** The rows are read only after command() returned, or never: the writes are committed either way, PROFILE included. */
  @Test
  void theWritesAreCommittedBeforeTheResultIsRead() throws Exception {
    final ResultSet unread = database.command("opencypher", "UNWIND range(1, 4) AS i CREATE (n:B {k: i}) RETURN n");
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countFromAnotherThread("MATCH (n:B) RETURN count(n) AS c")).isEqualTo(4L);
    int rows = 0;
    while (unread.hasNext()) {
      assertThat(unread.next().<Object>getProperty("n")).isNotNull();
      ++rows;
    }
    unread.close();
    assertThat(rows).isEqualTo(4);

    database.command("opencypher", "MATCH (n:B) SET n.seen = true RETURN n").close();
    assertThat(countFromAnotherThread("MATCH (n:B) WHERE n.seen RETURN count(n) AS c")).isEqualTo(4L);

    database.command("opencypher", "PROFILE MATCH (n:B) SET n.profiled = true RETURN n").close();
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countFromAnotherThread("MATCH (n:B) WHERE n.profiled RETURN count(n) AS c")).isEqualTo(4L);
  }

  /**
   * Concurrent auto-commit statements on one record conflict, and a conflict re-runs the whole statement from committed
   * state: every increment that reports success lands exactly once, none is lost and none is applied twice.
   */
  @Test
  void aConflictReRunsTheWholeStatementOnce() throws Exception {
    setup("CREATE (:A {k: 0, v: 0})");
    final int threads = 4;
    final int perThread = 25;
    final AtomicInteger succeeded = new AtomicInteger();
    final AtomicInteger refused = new AtomicInteger();
    final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final Thread worker = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < perThread; i++)
            try {
              database.command("opencypher", "MATCH (n:A {k: 0}) SET n.v = n.v + 1").close();
              succeeded.incrementAndGet();
            } catch (final NeedRetryException e) {
              // ALL THE ATTEMPTS CONFLICTED: THE INCREMENT IS NOT APPLIED, AND THE CALLER IS TOLD
              refused.incrementAndGet();
            }
        } catch (final Throwable e) {
          unexpected.add(e);
        }
      });
      workers.add(worker);
      worker.start();
    }
    start.countDown();
    for (final Thread worker : workers)
      worker.join();

    assertThat(unexpected).isEmpty();
    assertThat(succeeded.get() + refused.get()).isEqualTo(threads * perThread);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:A {k: 0}) RETURN n.v AS v")) {
      assertThat(rs.next().<Number>getProperty("v").intValue()).isEqualTo(succeeded.get());
    }
  }

  /** A UNION of writes is one statement: a failing branch takes back what the other branches wrote. */
  @Test
  void aUnionOfWritesFailingInOneBranchLeavesNothingBehind() throws Exception {
    assertThatThrownBy(() -> drain("CREATE (:A {k: 1}) RETURN 1 AS x UNION ALL CREATE (:A {k: 2}) WITH 0 AS z RETURN 1 / z AS x"))
        .hasMessageContaining("/ by zero");
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countFromAnotherThread("MATCH (n:A) RETURN count(n) AS c")).isZero();
  }

  /** A statement refused by an existence constraint checked at its end is rolled back before the error reaches the caller. */
  @Test
  void aFailedExistenceCheckAtTheEndOfTheStatementRollsItBack() throws Exception {
    database.command("opencypher", "CREATE CONSTRAINT b_k_exists IF NOT EXISTS FOR (n:B) REQUIRE n.k IS NOT NULL").close();
    setup("CREATE (:A {k: 0})");

    assertThatThrownBy(() -> drain("MATCH (a:A) SET a.touched = true CREATE (:B {name: 'no k'})")).hasMessageContaining("k");
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countFromAnotherThread("MATCH (n:B) RETURN count(n) AS c")).isZero();
    assertThat(countFromAnotherThread("MATCH (n:A) WHERE n.touched RETURN count(n) AS c")).isZero();

    // PROFILE runs the write the same way: in the statement's transaction, with the constraints checked at its end
    assertThatThrownBy(() -> drain("PROFILE MATCH (a:A) SET a.touched = true CREATE (:B {name: 'no k'})")).hasMessageContaining("k");
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countFromAnotherThread("MATCH (n:B) RETURN count(n) AS c")).isZero();
    assertThat(countFromAnotherThread("MATCH (n:A) WHERE n.touched RETURN count(n) AS c")).isZero();
  }

  private long countFromAnotherThread(final String cypher) throws Exception {
    final long[] count = new long[1];
    final Throwable[] failure = new Throwable[1];
    final Thread reader = new Thread(() -> {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        count[0] = rs.next().<Number>getProperty("c").longValue();
      } catch (final Throwable t) {
        failure[0] = t;
      }
    });
    reader.start();
    reader.join();
    if (failure[0] != null)
      throw new AssertionError("The read from another thread failed: " + cypher, failure[0]);
    return count[0];
  }

  private void setup(final String cypher) {
    database.transaction(() -> database.command("opencypher", cypher).close());
  }

  private List<Result> drain(final String cypher) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private List<String> graph() {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN labels(n) AS l, properties(n) AS p ORDER BY n.k")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty("l") + "" + new TreeMap<>(r.<Map<String, Object>>getProperty("p")));
      }
    }
    return out;
  }

  private long relationships() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long writeTransactions() {
    return ((Number) database.getStats().get("writeTx")).longValue();
  }
}
