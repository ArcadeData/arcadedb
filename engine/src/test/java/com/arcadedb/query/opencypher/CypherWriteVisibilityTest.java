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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.StripedEdgeList;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Read/write visibility inside one statement: write barriers, Halloween protection, autocommit atomicity and deleting while walking.
 * <p>
 * Each nested class holds the regression tests of the GitHub issue its name carries.
 */
class CypherWriteVisibilityTest {
  /**
   * Issues #8826 and #8827: a no-op boundary (a {@code CALL (*) { RETURN 0 }} or an empty {@code OPTIONAL MATCH}) between a
   * write clause and a {@code LIMIT} must not change what the write does to the graph.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue8826Issue8827CypherWriteBoundary extends TestHelper {

    private long count(final String cypher) {
      try (final ResultSet rs = database.query("opencypher", cypher)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private void drain(final String cypher) {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", cypher)) {
          while (rs.hasNext())
            rs.next();
        }
      });
    }

    private void setup8826() {
      drain("CREATE (b:B {k: 'x'}), (a1:A {id: 1}), (a2:A {id: 2}), (c:C {id: 3}), (b)-[:R]->(a1), (b)-[:R]->(a2), (b)-[:S]->(c)");
      drain("UNWIND range(1, 10) AS i CREATE (:D {id: i}), (f:F {id: i})-[:T]->(e:E {id: i})");
    }

    private static final String MATCH_8826 = "MATCH p0 = TRAIL (n0) <-[]- (n1:B {k: 'x'}) -[:S]-> (x:C), (n2:D), (n3:E) <-[:T]- (y:F) "
        + "WHERE n1.k =~ '.*' FOREACH (i IN range(0, 9) | CREATE (:Z {k: i})) WITH n0, n1, n2, n3, p0 LIMIT 1 ";

    @Test
    void noOpCallDoesNotChangeForeachCardinality() {
      setup8826();
      drain(MATCH_8826 + "RETURN n0, n1, n2, n3, p0");
      final long control = count("MATCH (z:Z) RETURN count(z) AS c");
      drain("MATCH (n) DETACH DELETE n");
      setup8826();

      drain(MATCH_8826 + "WITH n0, n1, n2, n3, p0 CALL (*) { RETURN 0 AS barrier } WITH n0, n1, n2, n3, p0 RETURN n0, n1, n2, n3, p0");
      final long withCall = count("MATCH (z:Z) RETURN count(z) AS c");
      assertThat(withCall).isEqualTo(control).isEqualTo(2000L);
    }

    @Test
    void foreachRunsForEveryRowBeforeLimit() {
      setup8826();
      final long rows = count("MATCH p0 = TRAIL (n0) <-[]- (n1:B {k: 'x'}) -[:S]-> (x:C), (n2:D), (n3:E) <-[:T]- (y:F) RETURN count(*) AS c");
      drain(MATCH_8826 + "RETURN n0");
      assertThat(count("MATCH (z:Z) RETURN count(z) AS c")).isEqualTo(rows * 10);
    }

    private void setup8827() {
      drain("UNWIND range(1, 5) AS i CREATE (a:A {id: i})-[:R]->(b:B {id: i}), (p:P {k: 0})-[:S]->(q:Q {id: i}), (m:M {k: 'm'})-[:T]->(z:Z {k: 'z'})");
    }

    private static final String HEAD_8827 = "OPTIONAL MATCH (n0:A)-[r0:R]->(), p0 = (p:P {k: 0})-[:S]->(), p1 = (m:M {k: 'm'})-[:T]->({k: 'z'}) "
        + "WHERE n0.id IS NOT NULL AND p0 IS NOT NULL AND p1 IS NOT NULL ";
    private static final String CALL_8827 = "CALL (n0, p0, p1) { SET n0.k = NULL DETACH DELETE n0 RETURN DISTINCT max('pma') AS alias0 } ";

    @Test
    void emptyOptionalMatchDoesNotChangeDetachDeleteState() {
      setup8827();
      drain(HEAD_8827 + "WITH * WHERE n0 IS NOT NULL AND p0 IS NOT NULL AND p1 IS NOT NULL " + CALL_8827 + "WITH alias0 LIMIT 1 RETURN alias0");
      final long control = count("MATCH (n) RETURN count(n) AS c");
      final long controlEdges = count("MATCH ()-[r]->() RETURN count(r) AS c");
      drain("MATCH (n) DETACH DELETE n");

      setup8827();
      final String barrier = "OPTIONAL MATCH (__barrier:NoSuchType) WHERE false ";
      drain(HEAD_8827 + barrier + "WITH n0, r0, p0, p1 WITH * WHERE n0 IS NOT NULL AND p0 IS NOT NULL AND p1 IS NOT NULL " + barrier
          + "WITH n0, r0, p0, p1 " + CALL_8827 + barrier + "WITH alias0 WITH alias0 LIMIT 1 RETURN alias0");
      assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(control).isEqualTo(25L);
      assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).isEqualTo(controlEdges).isEqualTo(10L);
    }

    @Test
    void createRunsForEveryRowBeforeFinalLimit() {
      drain("UNWIND range(1, 500) AS i CREATE (n:Item {i: i}) RETURN n LIMIT 1");
      assertThat(count("MATCH (n:Item) RETURN count(n) AS c")).isEqualTo(500L);
    }

    @Test
    void createRunsForEveryRowBeforeWithLimit() {
      drain("UNWIND range(1, 500) AS i CREATE (n:Item {i: i}) WITH n LIMIT 1 RETURN n");
      assertThat(count("MATCH (n:Item) RETURN count(n) AS c")).isEqualTo(500L);
    }

    private void seedItems() {
      drain("UNWIND range(1, 500) AS i CREATE (:Src {i: i})");
    }

    @Test
    void labeledMatchCreateBeforeReturnLimit() {
      seedItems();
      drain("MATCH (a:Src) CREATE (:Out {i: a.i}) RETURN a LIMIT 1");
      assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
    }

    @Test
    void labeledMatchCreateBeforeWithLimit() {
      seedItems();
      drain("MATCH (a:Src) CREATE (:Out {i: a.i}) WITH a LIMIT 1 RETURN a");
      assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
    }

    @Test
    void labeledMatchSetBeforeWithLimit() {
      seedItems();
      drain("MATCH (a:Src) SET a.seen = true WITH a LIMIT 1 RETURN a");
      assertThat(count("MATCH (a:Src) WHERE a.seen = true RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void labeledMatchDeleteBeforeWithLimit() {
      seedItems();
      drain("MATCH (a:Src) DELETE a WITH 1 AS x LIMIT 1 RETURN x");
      assertThat(count("MATCH (a:Src) RETURN count(a) AS c")).isZero();
    }

    @Test
    void mergeBeforeSkipAndLimit() {
      drain("UNWIND range(1, 500) AS i MERGE (:Mg {i: i}) WITH i SKIP 1 LIMIT 1 RETURN i");
      assertThat(count("MATCH (a:Mg) RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void removeBeforeWithLimit() {
      drain("UNWIND range(1, 500) AS i CREATE (:Rm {i: i, t: 1})");
      drain("MATCH (a:Rm) REMOVE a.t WITH a LIMIT 1 RETURN a");
      assertThat(count("MATCH (a:Rm) WHERE a.t IS NULL RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void limitZeroAfterWriteStillWrites() {
      drain("UNWIND range(1, 500) AS i CREATE (:Lz {i: i}) WITH i LIMIT 0 RETURN i");
      assertThat(count("MATCH (a:Lz) RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void readOnlyLimitKeepsStreaming() {
      seedItems();
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (a:Src) RETURN a LIMIT 1")) {
        assertThat(rs.next().toString()).doesNotContain("EagerStep");
      }
    }

    @Test
    void standaloneForeachBeforeWithAndReturnLimit() {
      seedItems();
      drain("MATCH (a:Src) FOREACH (i IN [1, 2] | CREATE (:Fe)) WITH a LIMIT 1 RETURN a");
      assertThat(count("MATCH (f:Fe) RETURN count(f) AS c")).isEqualTo(1000L);
      drain("MATCH (a:Src) FOREACH (i IN [1] | CREATE (:Fr)) RETURN a LIMIT 1");
      assertThat(count("MATCH (f:Fr) RETURN count(f) AS c")).isEqualTo(500L);
    }

    @Test
    void writingSubqueryBeforeLimit() {
      seedItems();
      drain("MATCH (a:Src) CALL (a) { CREATE (:Sq {i: a.i}) RETURN 1 AS one } WITH a LIMIT 1 RETURN a");
      assertThat(count("MATCH (s:Sq) RETURN count(s) AS c")).isEqualTo(500L);
    }

    @Test
    void orderByAndAggregationLimitStillWriteForEveryRow() {
      seedItems();
      drain("MATCH (a:Src) CREATE (:Ob {i: a.i}) WITH a ORDER BY a.i LIMIT 1 RETURN a");
      assertThat(count("MATCH (o:Ob) RETURN count(o) AS c")).isEqualTo(500L);
      drain("MATCH (a:Src) CREATE (:Ag {i: a.i}) RETURN count(a) AS n LIMIT 1");
      assertThat(count("MATCH (o:Ag) RETURN count(o) AS c")).isEqualTo(500L);
    }

    @Test
    void writeBeforeLimitPlansTheBarrier() {
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (a:Src) SET a.x = 1 RETURN a LIMIT 1")) {
        assertThat(rs.next().toString()).contains("EagerStep");
      }
    }

    @Test
    void parameterLimitAfterWriteKeepsTheFirstRowsAndWritesAll() {
      seedItems();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "MATCH (a:Src) CREATE (:Out {i: a.i}) WITH a SKIP 2 LIMIT $n RETURN a.i AS i",
            Map.of("n", 3))) {
          int rows = 0;
          while (rs.hasNext()) {
            rs.next();
            rows++;
          }
          assertThat(rows).isEqualTo(3);
        }
      });
      assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
    }

    @Test
    void filteredLimitAfterWriteStillReturnsMatchingRows() {
      seedItems();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "MATCH (a:Src) CREATE (:Out {i: a.i}) WITH a WHERE a.i > 400 LIMIT 5 RETURN a.i AS i")) {
          int rows = 0;
          while (rs.hasNext()) {
            rs.next();
            rows++;
          }
          assertThat(rows).isEqualTo(5);
        }
      });
      assertThat(count("MATCH (o:Out) RETURN count(o) AS c")).isEqualTo(500L);
    }

    @Test
    void repeatedLimitsAfterWriteStillWriteForEveryRow() {
      drain("UNWIND range(1, 500) AS i CREATE (:Lz {i: i}) WITH i LIMIT 1 WITH i LIMIT 0 RETURN i");
      assertThat(count("MATCH (a:Lz) RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void returnLimitZeroAfterWriteStillWrites() {
      drain("UNWIND range(1, 500) AS i CREATE (:Rz {i: i}) RETURN i LIMIT 0");
      assertThat(count("MATCH (a:Rz) RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void distinctLimitAfterWriteStillWritesAndDeduplicates() {
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", "UNWIND range(1, 500) AS i CREATE (:Dt {i: i}) WITH DISTINCT i % 7 AS k LIMIT 3 RETURN k")) {
          int rows = 0;
          while (rs.hasNext()) {
            rs.next();
            rows++;
          }
          assertThat(rows).isEqualTo(3);
        }
      });
      assertThat(count("MATCH (a:Dt) RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void skipWithoutLimitAfterWriteStillWrites() {
      drain("UNWIND range(1, 500) AS i CREATE (:Sk {i: i}) WITH i SKIP 10 RETURN i");
      assertThat(count("MATCH (a:Sk) RETURN count(a) AS c")).isEqualTo(500L);
    }

    @Test
    void writeProcedureBeforeLimitRunsForEveryRow() {
      drain("UNWIND range(1, 300) AS i CREATE (:Wp {i: i})");
      drain("CREATE (:Hub)");
      drain("MATCH (h:Hub), (w:Wp) CALL merge.relationship(h, 'LINKS', {}, {}, w) YIELD rel WITH rel LIMIT 1 RETURN rel");
      assertThat(count("MATCH (:Hub)-[r:LINKS]->(:Wp) RETURN count(r) AS c")).isEqualTo(300L);
    }

    @Test
    void limitZeroWithOrderByAfterWriteStillWrites() {
      drain("UNWIND range(1, 500) AS i CREATE (:Ol {i: i}) WITH i ORDER BY i LIMIT 0 RETURN i");
      assertThat(count("MATCH (a:Ol) RETURN count(a) AS c")).isEqualTo(500L);
    }
  }

  /**
   * Issue #10190: a Cypher write statement run through the embedded API with no transaction open is ONE transaction, as it
   * is over HTTP and as Neo4j's implicit transaction is. The write steps used to begin and commit their own, row by row, so
   * a failing statement kept the writes made before the error, a MERGE over several pairs conflicted with its own earlier
   * rows, and a label round trip never returned.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10190AutoCommitStatementAtomicity extends TestHelper {

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

  /**
   * Issue #10251 (the Halloween problem): an openCypher write that moves the key of the index its MATCH walks must update
   * every matched record exactly once. The range scan used to meet the keys the statement had just written ahead of its
   * cursor, update the same records again and again, and end in an {@code OutOfMemoryError}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue10251SetIndexedRangeHalloween extends TestHelper {
    private static final int N = 300;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType("A").createProperty("k", Integer.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "A", "k");
      });
      database.transaction(() -> database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:A {k: i})").close());
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD) // A HANG DETECTOR: THE STATEMENT USED TO NEVER RETURN
    void autoCommitSetOfTheRangeKeyRaisesEveryKeyOnce() {
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000").close();
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void setOfTheRangeKeyInsideATransactionRaisesEveryKeyOnce() {
      database.transaction(() -> database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000").close());
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void lowerBoundedDescendingMoveRaisesEveryKeyOnce() {
      // THE KEYS MOVE DOWN, BEHIND AN ASCENDING CURSOR BUT AHEAD OF A DESCENDING ONE
      database.command("opencypher", "MATCH (n:A) WHERE n.k <= " + N + " WITH n ORDER BY n.k DESC SET n.k = n.k - 1000").close();
      assertRaisedOnce(-1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void boundedRangeReturnsEachRowOnce() {
      final List<Result> rows = drain("MATCH (n:A) WHERE n.k >= 1 AND n.k <= " + N + " SET n.k = n.k + 1 RETURN n.k AS k");
      assertThat(rows).hasSize(N);
      // EVERY KEY EXACTLY ONCE: A NODE SKIPPED AND ANOTHER RAISED TWICE WOULD KEEP THE COUNT AND THE BOUNDS
      final List<Integer> returned = new ArrayList<>();
      for (final Result row : rows)
        returned.add(row.<Number>getProperty("k").intValue());
      assertThat(returned).containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(2, N + 1).boxed().toList());
      final List<Integer> stored = new ArrayList<>();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN n.k AS k")) {
        while (rs.hasNext())
          stored.add(rs.next().<Number>getProperty("k").intValue());
      }
      assertThat(stored).containsExactlyInAnyOrderElementsOf(IntStream.rangeClosed(2, N + 1).boxed().toList());
      assertRaisedOnce(1);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void mapMergeOfTheRangeKeyRaisesEveryKeyOnce() {
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n += {k: n.k + 1000}").close();
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void setThroughAWithRaisesEveryKeyOnce() {
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 WITH n, n.k AS old SET n.k = old + 1000").close();
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void mergeOnCreateSetOfTheRangeKeyRaisesEveryKeyOnce() {
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 MERGE (n)-[:R]->(m:B {id: n.k}) ON CREATE SET n.k = n.k + 1000").close();
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void mergeOnMatchSetOfTheRangeKeyRaisesEveryKeyOnce() {
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 MERGE (m:A {k: n.k}) ON MATCH SET m.k = m.k + 1000").close();
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void legacyPipelineRaisesEveryKeyOnce() {
      // AN UNWIND BEFORE THE MATCH TAKES THE ORDINARY PIPELINE, WHICH READS NO INDEX RANGE
      database.command("opencypher", "UNWIND [1] AS x MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000 * x").close();
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void indexOrderedDescendingReturnRaisesEveryKeyOnceInOrder() {
      final List<Result> rows = drain("MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000 RETURN n.k AS k ORDER BY k DESC");
      assertThat(rows).hasSize(N);
      assertThat(rows.getFirst().<Number>getProperty("k").intValue()).isEqualTo(N + 1000);
      assertThat(rows.getLast().<Number>getProperty("k").intValue()).isEqualTo(1001);
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void indexOrderedScanWithNullKeysRaisesEveryKeyOnce() {
      // AN INDEX-ORDERED SCAN OF THE WHOLE LABEL ALSO READS THE VERTICES THAT HAVE NO KEY, AFTER THE INDEX
      database.transaction(() -> database.command("opencypher", "UNWIND range(1, 10) AS i CREATE (:A {other: i})").close());
      database.command("opencypher", "MATCH (n:A) WITH n ORDER BY n.k SET n.k = coalesce(n.k, 0) + 1000").close();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N + 10);
        assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1000);
        assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 1000);
      }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void joinOnACompositePrefixRaisesEveryKeyOnce() {
      // THE INNER SIDE OF AN INDEX NESTED-LOOP JOIN SEEKS A PREFIX PER LEFT ROW
      database.transaction(() -> {
        database.getSchema().createVertexType("C").createProperty("g", Integer.class);
        database.getSchema().getType("C").createProperty("k", Integer.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "C", "g", "k");
        database.getSchema().createVertexType("G").createProperty("g", Integer.class);
      });
      database.transaction(() -> {
        database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:C {g: i % 2, k: i})").close();
        database.command("opencypher", "CREATE (:G {g: 0}), (:G {g: 1})").close();
      });

      final String join = "MATCH (x:G), (n:C) WHERE n.g = x.g SET n.k = n.k + 1000";
      assertThat(explain(join)).contains("IndexNestedLoopJoin");
      database.command("opencypher", join).close();

      try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N);
        assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1001);
        assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 1000);
      }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void theAddressesReadFirstAreChargedToTheHeapBudgetAndReleased() {
      final long baseline = QueryHeapBudget.getReservedBytes();
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000").close();
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);

      // 150,000 addresses take about 2MB, past a budget of 1MB: the statement fails, rolls back and gives the budget back
      database.transaction(() -> database.command("opencypher", "UNWIND range(" + (N + 1) + ", 150000) AS i CREATE (:A {k: i})").close());
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
      try {
        assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:A) WHERE n.k > 1000 SET n.k = n.k + 1000000").close())
            .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
        assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
      } finally {
        GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
      }
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) WHERE n.k > 1000000 RETURN count(n) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
      }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void setInsideACallSubqueryRaisesEveryKeyOnce() {
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 CALL { WITH n SET n.k = n.k + 1000 }").close();
      assertRaisedOnce(1000);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void thePrefixSeekGivesBackTheHeapItCharged() {
      database.transaction(() -> {
        database.getSchema().createVertexType("C").createProperty("g", Integer.class);
        database.getSchema().getType("C").createProperty("k", Integer.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "C", "g", "k");
      });
      database.transaction(() -> database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:C {g: 1, k: i})").close());
      final long baseline = QueryHeapBudget.getReservedBytes();
      database.command("opencypher", "MATCH (n:C {g: 1}) SET n.k = n.k + 1000").close();
      database.command("opencypher", "MATCH (n:C) WHERE n.g IN [1, 2] SET n.k = n.k + 1000").close();
      assertThat(QueryHeapBudget.getReservedBytes()).isLessThanOrEqualTo(baseline);
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN min(n.k) AS lo, max(n.k) AS hi")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(2001);
        assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 2000);
      }
    }

    @Test
    void aWriteOfAnotherKeyKeepsStreamingAndWritesEveryNode() {
      database.command("opencypher", "MATCH (n:A) WHERE n.k > 0 SET n.other = n.k * 2").close();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) WHERE n.other = n.k * 2 RETURN count(n) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(N);
      }
      assertRaisedOnce(0);
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void createInTheScannedTypeCreatesOnePerMatchedRow() {
      // THE SAME PROBLEM WITHOUT AN INDEX: THE NEW NODES LAND IN THE TYPE THE MATCH SCANS
      database.command("opencypher", "MATCH (n:A) CREATE (:A {k: n.k + 1000})").close();
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(2L * N);
      }
    }

    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void setOfTheTrailingKeyOfACompositePrefixSeekRaisesEveryKeyOnce() {
      // THE SEEK OF A PREFIX WALKS THE ENTRIES SHARING IT: RAISING THE TRAILING KEY MOVES EACH ONE AHEAD OF THE CURSOR
      database.transaction(() -> {
        database.getSchema().createVertexType("C").createProperty("g", Integer.class);
        database.getSchema().getType("C").createProperty("k", Integer.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "C", "g", "k");
      });
      database.transaction(() -> database.command("opencypher", "UNWIND range(1, " + N + ") AS i CREATE (:C {g: 1, k: i})").close());

      database.command("opencypher", "MATCH (n:C {g: 1}) SET n.k = n.k + 1000").close();

      try (final ResultSet rs = database.query("opencypher", "MATCH (n:C) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N);
        assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1001);
        assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + 1000);
      }
    }

    @Test
    void theRangeIsReadFirstOnlyWhenTheStatementWritesItsKey() {
      assertThat(explain("MATCH (n:A) WHERE n.k > 0 SET n.k = n.k + 1000")).contains("range read first");
      assertThat(explain("MATCH (n:A) WHERE n.k > 0 SET n += {k: 1}")).contains("range read first");
      // A WRITE OF ANOTHER KEY KEEPS THE STREAMING SCAN
      assertThat(explain("MATCH (n:A) WHERE n.k > 0 SET n.other = 1")).doesNotContain("range read first");
      assertThat(explain("MATCH (n:A) WHERE n.k > 0 RETURN n")).doesNotContain("range read first");
    }

    private String explain(final String cypher) {
      try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + cypher)) {
        return rs.getExecutionPlan().get().prettyPrint(0, 2);
      }
    }

    private void assertRaisedOnce(final int delta) {
      try (final ResultSet rs = database.query("opencypher", "MATCH (n:A) RETURN count(n) AS c, min(n.k) AS lo, max(n.k) AS hi")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("c").longValue()).isEqualTo(N);
        assertThat(r.<Number>getProperty("lo").intValue()).isEqualTo(1 + delta);
        assertThat(r.<Number>getProperty("hi").intValue()).isEqualTo(N + delta);
      }
      // THE INDEX AGREES WITH THE RECORDS: EVERY KEY IS FOUND, ONCE
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM A WHERE k >= " + (1 + delta) + " AND k <= " + (N + delta))) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(N);
      }
    }

    private List<Result> drain(final String cypher) {
      final List<Result> rows = new ArrayList<>();
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
      return rows;
    }
  }

  /**
   * Issue #11173: the filter of a MATCH must read the graph as it was before a later clause of the same query writes the key it
   * filters on, whatever the pull batch size. A write that names the key waits for the MATCH of every row; an opaque map write
   * keeps streaming, so the upsert shapes {@code MATCH (n {id: row.id}) SET n += row} are not made eager.
   */
  @Nested
  class Issue11173MatchFilterSeesLaterSet extends TestHelper {
    private static final String EAGER = "EAGER";

    @Test
    void matchWhereMatchesEveryRowBeforeTheSetRuns() {
      assertThat(count("""
          CREATE (b:B {k: 1})
          WITH b
          UNWIND range(1, 150) AS i
          CREATE (a:A {i: i})-[:R]->(b)
          WITH a, b
          MATCH (a)-[:R]->(c:B) WHERE c.k = 1
          SET c.k = 2
          RETURN count(*) AS c""")).isEqualTo(150L);
    }

    @Test
    void matchInlinePropertyMatchesEveryRowBeforeTheRemoveRuns() {
      assertThat(count("""
          CREATE (b:B {k: 1})
          WITH b
          UNWIND range(1, 150) AS i
          CREATE (a:A {i: i})-[:R]->(b)
          WITH a, b
          MATCH (a)-[:R]->(c:B {k: 1})
          REMOVE c.k
          RETURN count(*) AS c""")).isEqualTo(150L);
    }

    @Test
    void matchWhereMatchesEveryRowBeforeASetOfAMapLiteralRuns() {
      assertThat(count("""
          CREATE (b:B {k: 1})
          WITH b
          UNWIND range(1, 150) AS i
          CREATE (a:A {i: i})-[:R]->(b)
          WITH a, b
          MATCH (a)-[:R]->(c:B) WHERE c.k = 1
          SET c += {k: 2}
          RETURN count(*) AS c""")).isEqualTo(150L);
    }

    @Test
    void matchWhereMatchesEveryRowBeforeAMergeActionRuns() {
      assertThat(count("""
          CREATE (b:B {k: 1})
          WITH b
          UNWIND range(1, 150) AS i
          CREATE (a:A {i: i})-[:R]->(b)
          WITH a, b
          MATCH (a)-[:R]->(c:B) WHERE c.k = 1
          MERGE (m:M {id: 1}) ON CREATE SET c.k = 2 ON MATCH SET c.k = 2
          RETURN count(*) AS c""")).isEqualTo(150L);
    }

    @Test
    void barrierIsPlannedWhereAWriteNamesAKeyTheMatchFiltersOn() {
      assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 SET b.k = 2")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b {k: 1}) REMOVE b.k")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 SET b += {k: 2}")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 FOREACH (x IN [1] | SET b.k = 2)")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 CALL (b) { SET b.k = 2 }")).contains(EAGER);
      assertThat(explain("MATCH (a)-[:R]->(b) WHERE b.k = 1 CALL (b) { SET b.other = 2 }")).doesNotContain(EAGER);
    }

    @Test
    void upsertShapesKeepStreaming() {
      // an opaque map write may rewrite the lookup key, but holding the whole input for it is not worth it
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
      assertThat(explain("LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
      // a write of another key, and a statement that starts with a one-node MATCH (a different node on every row)
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n.name = row.name")).doesNotContain(EAGER);
      assertThat(explain("MATCH (n:P) WHERE n.k = 1 SET n.k = 2")).doesNotContain(EAGER);
    }

    private String explain(final String query) {
      try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
        return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private long count(final String query) {
      try (final ResultSet resultSet = database.command("opencypher", query)) {
        final Result row = resultSet.next();
        return ((Number) row.getProperty("c")).longValue();
      }
    }
  }

  /**
   * Issue #11230: an opaque map write ({@code SET n += map}, {@code SET n = map}) that can rewrite the key a MATCH looked the node up
   * by must wait for the MATCH of every row, or the MATCH of a later duplicate lookup sees the write of an earlier row and the row
   * count depends on the pull batch. The upsert shape, where the written map is the very row the lookup key is read from
   * ({@code MATCH (n {id: row.id}) SET n += row}), rewrites the key with the value it was found by and keeps streaming.
   */
  @Nested
  class Issue11230OpaqueMapWriteLookupKey extends TestHelper {
    private static final String EAGER = "EAGER";
    private static final int    ROWS  = 150;

    @Test
    void duplicateLookupsMatchBeforeAnOpaqueWriteOfAnotherMapRewritesTheKey() {
      database.command("opencypher", "CREATE (:P {id: 1})");
      final List<Object> rows = new ArrayList<>();
      for (int i = 0; i < ROWS; i++)
        rows.add(Map.of("id", 1, "props", Map.of("id", 2)));
      assertThat(count("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row.props RETURN count(*) AS c", Map.of("rows", rows)))
          .isEqualTo((long) ROWS);
    }

    @Test
    void duplicateLookupsMatchBeforeAWholeMapReplacementRewritesTheKey() {
      database.command("opencypher", "CREATE (:P {id: 1})");
      final List<Object> rows = new ArrayList<>();
      for (int i = 0; i < ROWS; i++)
        rows.add(Map.of("id", 1, "props", Map.of("id", 2)));
      assertThat(count("UNWIND $rows AS row MATCH (n:P) WHERE n.id = row.id SET n = row.props RETURN count(*) AS c", Map.of("rows", rows)))
          .isEqualTo((long) ROWS);
    }

    @Test
    void theUpsertShapeStillAnswersEveryRow() {
      database.command("opencypher", "CREATE (:P {id: 1})");
      final List<Object> rows = new ArrayList<>();
      for (int i = 0; i < ROWS; i++)
        rows.add(Map.of("id", 1, "name", "x"));
      assertThat(count("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row RETURN count(*) AS c", Map.of("rows", rows)))
          .isEqualTo((long) ROWS);
    }

    @Test
    void barrierIsPlannedWhereTheWrittenMapIsNotTheRowTheKeyComesFrom() {
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row.props")).contains(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n = row.props")).contains(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += $other")).contains(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE n.id = row.id SET n += row.props")).contains(EAGER);
      // the key is read from another property of the row than the one the map is written to
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.oldId}) SET n += row")).contains(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE n.id > row.id SET n += row")).contains(EAGER);
      // the map is a row, but not the one the lookup was fed from
      assertThat(explain("UNWIND $rows AS row UNWIND $others AS other MATCH (n:P {id: row.id}) SET n += other")).contains(EAGER);
      // a name rebound by a WITH is not the row the key was read from
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) WITH n, {id: 7} AS row SET n += row")).contains(EAGER);
      // a key projected from another property of the row than the one it is looked up by
      assertThat(explain("UNWIND [{id: 1, o: 2}] AS row WITH row, row.o AS id MATCH (n:P {id: id}) SET n += row")).contains(EAGER);
      // the write goes through another variable than the one the key was looked up by
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}), (m:P {id: row.other}) SET m += row")).contains(EAGER);
      // a subquery whose body writes an opaque map
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) CALL (n, row) { SET n += row.props }")).contains(EAGER);
      // a key computed per row
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n[row.key] = 1")).contains(EAGER);
    }

    @Test
    void upsertShapesKeepStreaming() {
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) SET n = row")).doesNotContain(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE n.id = row.id SET n += row")).doesNotContain(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE row.id = n.id SET n += row")).doesNotContain(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P) WHERE row.id = n.id AND n.id > 0 SET n += row")).contains(EAGER);
      assertThat(explain("UNWIND $rows AS row MATCH (n:P {id: row.id}) WITH n, row SET n += row")).doesNotContain(EAGER);
      assertThat(explain("LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row MATCH (n:P {id: row.id}) SET n += row")).doesNotContain(EAGER);
      assertThat(explain("LOAD CSV WITH HEADERS FROM 'file:///x.csv' AS row WITH row, row.id AS id MATCH (n:P {id: id}) SET n += row"))
          .doesNotContain(EAGER);
      assertThat(explain("MATCH (n:P) WHERE n.k = 1 SET n += $map")).doesNotContain(EAGER);
    }

    private String explain(final String query) {
      try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
        return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private long count(final String query, final Map<String, Object> parameters) {
      try (final ResultSet resultSet = database.command("opencypher", query, parameters)) {
        final Result row = resultSet.next();
        return ((Number) row.getProperty("c")).longValue();
      }
    }
  }

  /**
   * Issue #11234: one {@code MATCH (a)-[e]->(b) ... DELETE e} statement that deletes many of the OLDEST edges of a vertex failed with
   * {@code RecordNotFoundException}: the statement emptied a segment of the edge list, the removal deleted that segment, and a walk
   * of the same statement that was still streaming then stepped onto it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11234DeleteOldestEdgesWhileWalking extends TestHelper {

    private static final String DELETE = """
        UNWIND range($from, $to) AS i
        MATCH (a:Entity {external_id: 'HUB'})-[e:RELATES_TO]->(b:Entity {external_id: 'E' + i})
        DELETE e RETURN b.external_id AS b""";

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE Entity");
      database.command("sql", "CREATE PROPERTY Entity.external_id STRING");
      database.command("sql", "CREATE INDEX ON Entity (external_id) UNIQUE");
      database.command("sql", "CREATE EDGE TYPE RELATES_TO");
    }

    @Test
    void deletingTheOldestEdgesInOneStatementDeletesThemAll() {
      buildHub(3000);

      assertThat(deleteRange(0, 99)).isEqualTo(100);
      assertThat(hubDegree()).isEqualTo(2900);
      assertThat(hubDegreeTo("E0")).isZero();
      assertThat(hubDegreeTo("E99")).isZero();
      assertThat(hubDegreeTo("E100")).isEqualTo(1);

      assertThat(deleteRange(100, 1099)).isEqualTo(1000);
      assertThat(hubDegree()).isEqualTo(1900);
      assertThat(checkDatabaseErrors()).isZero();
    }

    @Test
    void aSmallerHubFailsTheSameWay() {
      buildHub(300);

      assertThat(deleteRange(0, 99)).isEqualTo(100);
      assertThat(hubDegree()).isEqualTo(200);
      assertThat(checkDatabaseErrors()).isZero();
    }

    @Test
    void deletingEveryEdgeOldestFirstEmptiesTheList() {
      buildHub(3000);

      assertThat(deleteRange(0, 2999)).isEqualTo(3000);
      assertThat(hubDegree()).isZero();
      assertThat(checkDatabaseErrors()).isZero();

      // THE LIST IS STILL USABLE AFTERWARDS
      database.command("opencypher", "MATCH (a:Entity {external_id: 'HUB'}), (b:Entity {external_id: 'E5'}) CREATE (a)-[:RELATES_TO]->(b)");
      assertThat(hubDegree()).isEqualTo(1);
    }

    @Test
    void aSuperNodeHubFailsTheSameWay() {
      // THE HUB IS PROMOTED TO THE STRIPED LAYOUT HALF WAY: THE OLDEST EDGES STAY ON THE CLASSIC CHAIN, THE NEWEST GO TO THE STRIPES
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 256);
      buildHub(3000);
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("Entity", 0))).isTrue();

      assertThat(deleteRange(0, 999)).isEqualTo(1000);
      assertThat(hubDegree()).isEqualTo(2000);
      assertThat(deleteRange(1000, 2999)).isEqualTo(2000);
      assertThat(hubDegree()).isZero();
      assertThat(checkDatabaseErrors()).isZero();
    }

    @Test
    void aStatementTakenBackLeavesNoStaleRelinkForTheNextOne() {
      buildHub(3000);

      database.transaction(() -> {
        // THE FIRST STATEMENT EMPTIES AND DELETES THE OLD SEGMENTS, THEN FAILS: ONLY IT IS TAKEN BACK, THE TRANSACTION GOES ON
        assertThatThrownBy(() -> database.command("opencypher", """
            UNWIND range(0, 999) AS i
            MATCH (a:Entity {external_id: 'HUB'})-[e:RELATES_TO]->(b:Entity {external_id: 'E' + i})
            DELETE e WITH count(*) AS n RETURN 1 / (n - n) AS boom""").stream().count()).hasMessageContaining("/ by zero");
        assertThat(hubDegree()).isEqualTo(3000);

        // THE SAME SEGMENTS ARE EMPTIED AND DELETED AGAIN, BY A STATEMENT THAT STREAMS ITS MATCH INTO ITS DELETE
        assertThat(deleteRange(0, 999)).isEqualTo(1000);
        assertThat(hubDegree()).isEqualTo(2000);
        assertThat(deleteRange(1000, 1999)).isEqualTo(1000);
        assertThat(hubDegree()).isEqualTo(1000);
      });
      assertThat(hubDegree()).isEqualTo(1000);
      assertThat(checkDatabaseErrors()).isZero();
    }

    @Test
    void deletingTheNewestEdgesStillWorks() {
      buildHub(3000);

      assertThat(deleteRange(2500, 2999)).isEqualTo(500);
      assertThat(hubDegree()).isEqualTo(2500);
      assertThat(checkDatabaseErrors()).isZero();
    }

    private void buildHub(final int edges) {
      database.command("opencypher", "CREATE (:Entity {external_id: 'HUB'})");
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Entity {external_id: 'E' + i})", Map.of("n", edges));
      for (int from = 0; from < edges; from += 500)
        database.command("opencypher", """
            UNWIND range($from, $to) AS i
            MATCH (a:Entity {external_id: 'HUB'}) MATCH (b:Entity {external_id: 'E' + i})
            CREATE (a)-[:RELATES_TO]->(b)""", Map.of("from", from, "to", Math.min(from + 499, edges - 1)));
      assertThat(hubDegree()).isEqualTo(edges);
    }

    private long deleteRange(final int from, final int to) {
      try (final ResultSet rs = database.command("opencypher", DELETE, Map.of("from", from, "to", to))) {
        return rs.stream().count();
      }
    }

    private long hubDegree() {
      try (final ResultSet rs = database.query("opencypher", "MATCH (:Entity {external_id: 'HUB'})-[e:RELATES_TO]->() RETURN count(e) AS n")) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private long hubDegreeTo(final String target) {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (:Entity {external_id: 'HUB'})-[e:RELATES_TO]->(:Entity {external_id: $t}) RETURN count(e) AS n", Map.of("t", target))) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private long checkDatabaseErrors() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        return ((Number) rs.next().getProperty("totalErrors")).longValue();
      }
    }
  }

  /**
   * Regression tests for GitHub issue #11463: a read that lives inside a {@code CALL} subquery body, or inside a
   * {@code COUNT {}} / {@code EXISTS {}} subquery or a pattern expression of a clause, was invisible to the read/write barrier
   * analysis. A later {@code CREATE} or {@code MERGE} of an entity those reads match therefore streamed behind them, and the
   * rows of the second pull batch (the first 100 rows are pulled before the write runs once) already saw what the write of the
   * first batch had created: the reporter's query answered 100 rows instead of 121, and an unrelated aggregating
   * {@code CALL} subquery in front of the MERGE "fixed" it by draining the rows.
   * <p>
   * openCypher requires a query's reads to be unaffected by that query's own writes; Neo4j plants an {@code Eager} in front of
   * the MERGE and answers 121 rows to both of the reporter's queries.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Issue11463SubqueryReadsNeedWriteBarrier {
    private static final String SETUP_EDGE = "CREATE (n:l2 {k10: 1912098346, k7: false})-[:rt1 {k1: 926826381, k10: 328995302}]->(:Z)";

    private static final String LEFT = """
        UNWIND range(1, 11) AS alias5
        UNWIND range(1, 11) AS alias6
        WITH alias5, alias6 ORDER BY alias5, alias6
        CALL (alias5, alias6) {
          MATCH () <-[:rt1 {k1: 926826381, k10: 328995302}]- (:l2 {k10: 1912098346, k7: false})
          WHERE COUNT {
            MATCH (:l2 {k7: false})
            RETURN alias5, alias6
          } = 1
          RETURN alias5, alias6
        }
        MERGE (:l11:l2:l10:l0 {k0: false, k7: false, id: 128})
        RETURN alias5, alias6, COUNT { MATCH (n) } AS graphNodeCount""";

    private static final String RIGHT = """
        UNWIND range(1, 11) AS alias5
        UNWIND range(1, 11) AS alias6
        WITH alias5, alias6 ORDER BY alias5, alias6
        CALL (alias5, alias6) {
          MATCH () <-[:rt1 {k1: 926826381, k10: 328995302}]- (:l2 {k10: 1912098346, k7: false})
          WHERE COUNT {
            MATCH (:l2 {k7: false})
            RETURN alias5, alias6
          } = 1
          RETURN alias5, alias6
        }
        CALL () {
          CALL db.index.vector.queryNodes('__ArcadeVectorBarrierProbe[embedding]', 1, [1.0, 0.0, 0.0]) YIELD node, score
          RETURN count(*) AS __discard
        }
        WITH alias5, alias6
        MERGE (:l11:l2:l10:l0 {k0: false, k7: false, id: 128})
        RETURN alias5, alias6, COUNT { MATCH (n) } AS graphNodeCount""";

    private static final String EAGER = "EAGER (read/write barrier)";

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue11463");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
    }

    @AfterEach
    void tearDown() {
      if (database != null) {
        database.drop();
        database = null;
      }
    }

    @Test
    void reportersLeftQueryReturnsEveryRow() {
      database.transaction(() -> database.command("opencypher", SETUP_EDGE).close());

      final List<Result> rows = run(LEFT);

      assertThat(rows).hasSize(121);
      for (final Result row : rows)
        assertThat(((Number) row.getProperty("graphNodeCount")).longValue()).isEqualTo(3L);
    }

    @Test
    void reportersLeftAndRightQueriesAgree() {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (:__ArcadeVectorBarrierProbe {embedding: [1.0, 0.0, 0.0]})").close();
        database.command("opencypher", """
            CREATE VECTOR INDEX __ArcadeVectorBarrierProbeEmbeddingIndex FOR (n:__ArcadeVectorBarrierProbe) ON n.embedding \
            OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}""").close();
        database.command("opencypher", SETUP_EDGE).close();
      });

      final List<Result> left = run(LEFT);
      database.transaction(() -> database.command("opencypher", "MATCH (n:l11) DETACH DELETE n").close());
      final List<Result> right = run(RIGHT);

      assertThat(left).hasSize(121);
      assertThat(right).hasSize(121);
      assertThat(pairs(left)).containsExactlyElementsOf(pairs(right));
    }

    /**
     * The reads of a CALL subquery body's MATCH are reads of the outer query: a later write they match needs the barrier. Each
     * MERGE creates a node of its own ({@code id: i}), so without the barrier the rows of the second pull batch count the nodes
     * the first one merged.
     */
    @Test
    void callSubqueryBodyMatchIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:A)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          CALL (i) { MATCH (a:A) RETURN count(a) AS c }
          MERGE (:A {id: i})
          RETURN i, c""";

      assertThat(explain(query)).contains(EAGER);
      final List<Result> rows = run(query);

      assertThat(rows).hasSize(150);
      for (final Result row : rows)
        assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
      assertThat(countOf("A")).isEqualTo(151L);
    }

    /** The CREATE of a CALL body is not a read, its MATCH is: a write of what only the CREATE names keeps streaming. */
    @Test
    void callSubqueryBodyWritePatternIsNotARead() {
      assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { CREATE (:A {id: i}) } MERGE (:A {id: i})")).doesNotContain(EAGER);
      assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { MATCH (b:B) CREATE (:A {id: i}) } MERGE (:A {id: i})")).doesNotContain(EAGER);
      assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { MATCH (b:A) CREATE (:B {id: i}) } MERGE (:A {id: i})")).contains(EAGER);
    }

    /** A CALL body's MATCH filtered by a COUNT subquery: both read the graph. */
    @Test
    void callSubqueryBodyWhereCountSubqueryIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:X), (:A)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          CALL (i) { MATCH (x:X) WHERE COUNT { MATCH (:A) } = 1 RETURN i AS j }
          MERGE (:A {id: 2})
          RETURN j""";

      assertThat(explain(query)).contains(EAGER);
      assertThat(run(query)).hasSize(150);
    }

    /** A COUNT subquery in an outer WITH ... WHERE reads the graph once per row. */
    @Test
    void countSubqueryInWithWhereIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:B)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          WITH i WHERE COUNT { MATCH (:B) } = 1
          MERGE (:B {id: i})
          RETURN i""";

      assertThat(explain(query)).contains(EAGER);
      assertThat(run(query)).hasSize(150);
      assertThat(countOf("B")).isEqualTo(151L);
    }

    /** A COUNT subquery projected by a WITH reads the graph once per row. */
    @Test
    void countSubqueryInWithProjectionIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:B)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          WITH i, COUNT { MATCH (:B) } AS c
          MERGE (:B {id: i})
          RETURN c""";

      assertThat(explain(query)).contains(EAGER);
      final List<Result> rows = run(query);
      assertThat(rows).hasSize(150);
      for (final Result row : rows)
        assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
    }

    /** An EXISTS subquery in the WHERE of an outer MATCH reads the graph once per row. */
    @Test
    void existsSubqueryInMatchWhereIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:C)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          MATCH (c:C) WHERE NOT EXISTS { MATCH (:D) }
          MERGE (:D {id: i})
          RETURN i""";

      assertThat(explain(query)).contains(EAGER);
      assertThat(run(query)).hasSize(150);
      assertThat(countOf("D")).isEqualTo(150L);
    }

    /** A pattern predicate in a WITH ... WHERE reads the graph once per row. */
    @Test
    void patternPredicateIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:F)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          MATCH (f:F)
          WITH i, f WHERE NOT (f)-[:R]->(:E)
          MERGE (:E {id: i})<-[:R]-(:G)
          RETURN i""";

      assertThat(explain(query)).contains(EAGER);
      assertThat(run(query)).hasSize(150);
    }

    /** A pattern comprehension projected by a WITH reads the graph once per row. */
    @Test
    void patternComprehensionIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:F)-[:R]->(:E)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          WITH i, size([(:F)-[:R]->(e:E) | e]) AS c
          MERGE (:F {id: i})-[:R]->(:E {id: i})
          RETURN c""";

      assertThat(explain(query)).contains(EAGER);
      final List<Result> rows = run(query);
      assertThat(rows).hasSize(150);
      for (final Result row : rows)
        assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
    }

    /** A read nested two levels deep (a COUNT inside an EXISTS inside a CALL inside a CALL) joins the footprint too. */
    @Test
    void deeplyNestedSubqueryReadIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:X), (:A), (:H)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          CALL (i) {
            CALL (i) {
              MATCH (x:X) WHERE EXISTS { MATCH (a:A) WHERE COUNT { MATCH (:H) } = 1 }
              RETURN i AS j
            }
            RETURN j
          }
          MERGE (:H {id: i})
          RETURN j""";

      assertThat(explain(query)).contains(EAGER);
      assertThat(run(query)).hasSize(150);
      assertThat(countOf("H")).isEqualTo(151L);
    }

    /** The relationship a quantified path pattern repeats is a read of its type: a later write of that type needs the barrier. */
    @Test
    void quantifiedPathPatternInCallBodyIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:Q)-[:S]->(:Q)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          CALL (i) { MATCH (:Q) ((:Q)-[:S]->(:Q)){1,2} (:Q) RETURN count(*) AS c }
          MERGE (:Q {id: i})-[:S]->(:Q {id: -i})
          RETURN c""";

      assertThat(explain(query)).contains(EAGER);
      final List<Result> rows = run(query);
      assertThat(rows).hasSize(150);
      final Object first = rows.getFirst().getProperty("c");
      for (final Result row : rows)
        assertThat((Object) row.getProperty("c")).isEqualTo(first);
    }

    /**
     * A quantified path pattern reads the types of the relationship it repeats, not every type: the untyped placeholder that stands
     * for the group in its enclosing pattern must not count as a read of any relationship.
     */
    @Test
    void quantifiedPathPatternReadsOnlyTheTypesItRepeats() {
      assertThat(explain("""
          UNWIND range(1, 150) AS i
          CALL (i) { MATCH (:Q) ((:Q)-[:S]->(:Q)){1,2} (:Q) RETURN count(*) AS c }
          MERGE (:Other {id: i})-[:T]->(:Other {id: -i})
          RETURN c""")).doesNotContain(EAGER);
      assertThat(explain("""
          UNWIND range(1, 150) AS i
          MATCH (:Q) ((:Q)-[:S]->(:Q)){1,2} (:Q)
          MERGE (:Other {id: i})-[:T]->(:Other {id: -i})""")).doesNotContain(EAGER);
    }

    /** A pattern subquery in the inline WHERE of a node pattern reads the graph once per row too. */
    @Test
    void subqueryInInlineNodePredicateIsARead() {
      database.transaction(() -> database.command("opencypher", "CREATE (:C)").close());
      final String query = """
          UNWIND range(1, 150) AS i
          MATCH (c:C WHERE NOT EXISTS { MATCH (:D) })
          MERGE (:D {id: i})
          RETURN i""";

      assertThat(explain(query)).contains(EAGER);
      assertThat(run(query)).hasSize(150);
      assertThat(countOf("D")).isEqualTo(150L);
    }

    /** A write the subquery's reads cannot match keeps streaming: no barrier is planned for it. */
    @Test
    void unrelatedWriteKeepsStreaming() {
      assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { MATCH (a:A) RETURN count(a) AS c } MERGE (:Other {id: i}) RETURN i, c"))
          .doesNotContain(EAGER);
      assertThat(explain("UNWIND range(1, 150) AS i WITH i WHERE COUNT { MATCH (:B) } = 1 MERGE (:Other {id: i})")).doesNotContain(EAGER);
      // two upserting MERGE clauses read nothing the footprint holds: a MERGE's own lookup is not a read of it
      assertThat(explain("UNWIND range(1, 150) AS i MERGE (a:P {id: i}) MERGE (b:P {id: i + 1}) MERGE (a)-[:R]->(b)")).doesNotContain(EAGER);
    }

    private List<Result> run(final String query) {
      final List<Result> rows = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet resultSet = database.command("opencypher", query)) {
          while (resultSet.hasNext())
            rows.add(resultSet.next());
        }
      });
      return rows;
    }

    private String explain(final String query) {
      try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
        return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      }
    }

    private long countOf(final String label) {
      try (final ResultSet resultSet = database.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c")) {
        return ((Number) resultSet.next().getProperty("c")).longValue();
      }
    }

    private static List<String> pairs(final List<Result> rows) {
      final List<String> pairs = new ArrayList<>(rows.size());
      for (final Result row : rows)
        pairs.add(row.getProperty("alias5") + ":" + row.getProperty("alias6") + ":" + row.getProperty("graphNodeCount"));
      return pairs;
    }
  }
}
