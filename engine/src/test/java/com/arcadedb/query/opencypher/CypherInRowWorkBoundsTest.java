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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.CypherFunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The work done while ONE row is evaluated answers to the command deadline and to the termination of the running statement
 * as the work between two rows does: the walk of a list by a comprehension, a reduce or a list predicate, the match of a
 * pattern comprehension, and the match phase of a MERGE.
 * <p>
 * The work is counted, never timed. A probe function (or a read listener, for the work that reads records) terminates the
 * registered statement at its tenth call, and counts what it is asked to do afterwards: with the checks in place that is a
 * handful of check intervals at most, without them it is everything the statement had left.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherInRowWorkBoundsTest {
  private static final String DB_PATH = "./target/databases/cypher-in-row-work-bounds";
  private static final String PROBE   = "rowbounds.probe";

  /** Elements of each data set: far more than the interval between two checks of a loop, so the checks are what stops it. */
  private static final int ELEMENTS = 12_000;
  /** The probe, or the listener, stops the statement at this call. */
  private static final int STOP_AT  = 10;
  /** Two intervals of a guarded loop (1,024 iterations each): what a stopped statement may still do before it notices. */
  private static final int ALLOWED_AFTER_STOP = 2_048;

  private static Database database;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  @BeforeAll
  static void createData() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.getSchema().createVertexType("Item");
    database.getSchema().createVertexType("Hub");
    database.getSchema().createVertexType("Root");
    database.getSchema().createVertexType("Leaf");
    database.getSchema().createVertexType("IdxLeaf").createProperty("k", Integer.class);
    database.getSchema().createVertexType("PNode");
    database.getSchema().createEdgeType("LINKS");
    database.getSchema().createEdgeType("HAS");
    database.getSchema().createEdgeType("HAS2");
    database.getSchema().createEdgeType("OWNS");
    database.getSchema().createEdgeType("TO");
    database.getSchema().createEdgeType("PLINK");
    database.getSchema().createEdgeType("RIDX").createProperty("w", Integer.class);
    database.command("sql", "CREATE INDEX ON IdxLeaf (k) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON RIDX (w) NOTUNIQUE");

    database.transaction(() -> {
      final MutableVertex[] items = new MutableVertex[ELEMENTS];
      for (int i = 0; i < ELEMENTS; i++)
        items[i] = database.newVertex("Item").set("v", i).save();
      for (int i = 0; i < ELEMENTS; i++)
        items[i].newEdge("LINKS", items[(i + 1) % ELEMENTS], "w", i).save();
      // The relationships of an indexed key, between vertices other than the two a statement binds
      for (int i = 2; i < ELEMENTS - 1; i++)
        items[i].newEdge("RIDX", items[i + 1], "w", 5).save();

      final MutableVertex hub = database.newVertex("Hub").set("id", 1).save();
      final MutableVertex root = database.newVertex("Root").set("id", 1).save();
      hub.newEdge("OWNS", root).save();
      for (int i = 0; i < ELEMENTS; i++) {
        final MutableVertex leaf = database.newVertex("Leaf").set("k", i).save();
        hub.newEdge("HAS", leaf).save();
        leaf.newEdge("TO", hub).save();
        database.newVertex("IdxLeaf").set("k", 7).save();
      }

      final int nodes = 400;
      final int fanOut = 100;
      final MutableVertex[] ring = new MutableVertex[nodes];
      for (int i = 0; i < nodes; i++)
        ring[i] = database.newVertex("PNode").set("v", i).save();
      for (int i = 0; i < nodes; i++)
        for (int j = 1; j <= fanOut; j++)
          ring[i].newEdge("PLINK", ring[(i + j) % nodes]).save();
    });
    database.command("opencypher", "UNWIND range(1, " + ELEMENTS + ") AS i CREATE (:Alpha:Beta {v: i})");
  }

  @AfterAll
  static void dropData() {
    if (database != null)
      database.drop();
  }

  @BeforeEach
  void noDeadline() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
  }

  @AfterEach
  void unregisterProbe() {
    CypherFunctionRegistry.unregister(PROBE);
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
  }

  // ---- the walk of a list inside one row ----

  @Test
  void aListComprehensionStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN size([x IN range(1, 100000) | rowbounds.probe(x)]) AS n");
  }

  @Test
  void aListComprehensionFilterStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN size([x IN range(1, 100000) WHERE rowbounds.probe(x) < 0 | x]) AS n");
  }

  @Test
  void aReduceStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN reduce(s = 0, x IN range(1, 100000) | s + rowbounds.probe(x)) AS n");
  }

  @Test
  void anAllReduceStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN allReduce(s = 0, x IN range(1, 100000) | s + x, rowbounds.probe(s) >= 0) AS n");
  }

  @Test
  void anAllPredicateStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN all(x IN range(1, 100000) WHERE rowbounds.probe(x) > 0) AS n");
  }

  @Test
  void anAnyPredicateStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN any(x IN range(1, 100000) WHERE rowbounds.probe(x) < 0) AS n");
  }

  @Test
  void aNonePredicateStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN none(x IN range(1, 100000) WHERE rowbounds.probe(x) < 0) AS n");
  }

  @Test
  void aSinglePredicateStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN single(x IN range(1, 100000) WHERE rowbounds.probe(x) < 0) AS n");
  }

  @Test
  void nestedShortListsAreCheckedAtEveryEntry() {
    // No list of the 810,000 steps is longer than 30 elements, so no counter of a single loop ever reaches an interval:
    // what stops it is the check made when each of the lists is entered
    final Run run = run(
        "RETURN size([a IN range(1, 30) | [b IN range(1, 30) | [c IN range(1, 30) | [d IN range(1, 30) | rowbounds.probe(d)]]]]) AS n");

    assertThat(causeOfType(run.failure, QueryTerminatedException.class)).as("the statement fails as terminated").isNotNull();
    assertThat(run.callsAfterStop).as("steps taken after the statement was terminated").isLessThanOrEqualTo(30);
  }

  @Test
  void aCommandDeadlineStopsTheNestedListsOfOneRow() {
    final Counter counter = registerCountingProbe();
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 20L);

    final Throwable failure = failureOf("RETURN size([x IN range(1, 3000) | size([y IN range(1, 3000) | rowbounds.probe(y)])]) AS n");

    assertThat(causeOfType(failure, TimeoutException.class)).as("the statement fails with the timeout").isNotNull();
    // 9,000,000 steps when nothing stops it: the statement ended before the last of them
    assertThat(counter.calls.get()).as("steps taken before the deadline stopped the statement").isLessThan(9_000_000L);
  }

  @Test
  void listsAnswerTheSameWhileAStatementIsRegisteredAndNotTerminated() {
    try (final RunningQuery ignored = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      registerCountingProbe();
      final List<Result> rows = drain("RETURN size([x IN range(1, 5000) | rowbounds.probe(x)]) AS a, "
          + "reduce(s = 0, x IN range(1, 5000) | s + x) AS b, "
          + "allReduce(s = 0, x IN range(1, 5000) | s + x, s >= 0) AS c, "
          + "all(x IN range(1, 5000) WHERE x > 0) AS d, any(x IN range(1, 5000) WHERE x = 4999) AS e, "
          + "none(x IN range(1, 5000) WHERE x < 0) AS f, single(x IN range(1, 5000) WHERE x = 7) AS g");

      assertThat(rows).hasSize(1);
      final Result row = rows.getFirst();
      assertThat(row.<Number>getProperty("a").longValue()).isEqualTo(5000L);
      assertThat(row.<Number>getProperty("b").longValue()).isEqualTo(12_502_500L);
      assertThat(row.<Boolean>getProperty("c")).isTrue();
      assertThat(row.<Boolean>getProperty("d")).isTrue();
      assertThat(row.<Boolean>getProperty("e")).isTrue();
      assertThat(row.<Boolean>getProperty("f")).isTrue();
      assertThat(row.<Boolean>getProperty("g")).isTrue();
    }
  }

  // ---- the match of a pattern comprehension ----

  @Test
  void aPatternComprehensionOverEveryVertexOfATypeStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow("RETURN size([(a:PNode)-[:PLINK]->(b:PNode) WHERE rowbounds.probe(b.v) < 0 | b]) AS n");
  }

  @Test
  void aPatternComprehensionOfAVariableLengthHopStopsWhenTheStatementIsTerminated() {
    assertStopsInsideTheRow(
        "MATCH (a:PNode {v: 0}) RETURN size([(a)-[:PLINK*1..2]->(b) WHERE rowbounds.probe(b.v) < 0 | b]) AS n");
  }

  @Test
  void aPatternComprehensionAnswersTheSameWhileAStatementIsRegisteredAndNotTerminated() {
    try (final RunningQuery ignored = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      final List<Result> rows = drain("MATCH (a:PNode {v: 0}) RETURN size([(a)-[:PLINK*1..2]->(b) WHERE b.v >= 0 | b]) AS n");

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Number>getProperty("n").longValue()).isEqualTo(100L + 100L * 100L);
    }
  }

  // ---- the match phase of a MERGE ----

  @Test
  void aMergeScanningATypeStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("Item", "MERGE (n:Item {v: -1}) RETURN n.v AS v");
  }

  @Test
  void aMergeScanningTheTypesOfSeveralLabelsStopsWhenTheStatementIsTerminated() {
    final String type = database.query("sql", "SELECT @type AS t FROM Alpha LIMIT 1").next().getProperty("t");
    assertStopsAfterTheNthRead(type, "MERGE (n:Alpha:Beta {v: -1}) RETURN n.v AS v");
  }

  @Test
  void aMergeReadingAnIndexStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("IdxLeaf", "MERGE (n:IdxLeaf {k: 7}) RETURN n.k AS k");
  }

  @Test
  void aMergeScanningTheRelationshipsOfATypeStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("LINKS", "MERGE (a:Item)-[r:LINKS {w: -1}]->(b:Item) RETURN r.w AS w");
  }

  @Test
  void aMergeWalkingTheRelationshipsOfABoundNodeStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("Leaf", "MATCH (h:Hub) MERGE (h)-[:HAS]->(l:Leaf {k: -1}) RETURN l.k AS k");
  }

  @Test
  void aMergeWalkingToTheRightOfABoundNodeStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("Leaf", "MATCH (h:Hub) MERGE (r:Root)-[:OWNS]->(h)-[:HAS]->(l:Leaf {k: -1}) RETURN l.k AS k");
  }

  @Test
  void aMergeWalkingToTheLeftOfABoundNodeStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("Leaf", "MATCH (h:Hub) MERGE (l:Leaf {k: -1})-[:TO]->(h)-[:OWNS]->(r:Root) RETURN l.k AS k");
  }

  @Test
  void aMergeSeekingCandidatesInAnIndexStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("IdxLeaf", "MATCH (h:Hub) MERGE (h)-[:HAS2]->(l:IdxLeaf {k: 7}) RETURN l.k AS k");
  }

  @Test
  void aMergeReadingTheIndexOfARelationshipStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("RIDX",
        "MATCH (a:Item {v: 0}), (b:Item {v: 1}) MERGE (a)-[r:RIDX {w: 5}]->(b) RETURN r.w AS w");
  }

  @Test
  void aMergeStillMatchesWhileAStatementIsRegisteredAndNotTerminated() {
    try (final RunningQuery ignored = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      final List<Result> rows = drain("MERGE (n:Item {v: 5}) RETURN n.v AS v");

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Number>getProperty("v").intValue()).isEqualTo(5);
      assertThat(drain("MATCH (n:Item) RETURN count(n) AS c").getFirst().<Number>getProperty("c").longValue()).isEqualTo(ELEMENTS);
    }
  }

  // ---- the edge-list walk of a pattern predicate ----

  @Test
  void aPatternPredicateOnTheRelationshipsOfANodeStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("Leaf", "MATCH (h:Hub) RETURN exists((h)-[:HAS]->(:Leaf {k: -1})) AS e");
  }

  @Test
  void aPatternPredicateBetweenTwoNodesStopsWhenTheStatementIsTerminated() {
    assertStopsAfterTheNthRead("HAS", "MATCH (h:Hub), (l:Leaf {k: 5}) RETURN exists((h)-[:HAS {w: 1}]->(l)) AS e");
  }

  @Test
  void aPatternPredicateAnswersTheSameWhileAStatementIsRegisteredAndNotTerminated() {
    try (final RunningQuery ignored = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      final List<Result> rows = drain("MATCH (h:Hub) RETURN exists((h)-[:HAS]->(:Leaf {k: 5})) AS found, "
          + "exists((h)-[:HAS]->(:Leaf {k: -1})) AS missing");

      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().<Boolean>getProperty("found")).isTrue();
      assertThat(rows.getFirst().<Boolean>getProperty("missing")).isFalse();
    }
  }

  // ---- the removal of elements from a list ----

  @Test
  void removingManyElementsFromAListCostsOneShift() {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    final List<Result> rows = drain("RETURN size(coll.remove([x IN range(1, 600000) | x], 10, 400000)) AS n");

    assertThat(rows.getFirst().<Number>getProperty("n").longValue()).isEqualTo(200_000L);
    stopwatch.assertStayedUnder(5_000L, "removing a span of a list shifts the elements behind it once, not once per removed element");
  }

  @Test
  void removingElementsFromAListKeepsTheRestInOrder() {
    final List<Result> rows = drain("RETURN coll.remove([x IN range(1, 10) | x], 2, 3) AS a, coll.remove([x IN range(1, 10) | x], 8, 5) AS b, "
        + "coll.remove([x IN range(1, 10) | x], 4, 0) AS c, coll.remove([x IN range(1, 10) | x], 4) AS d");

    final Result row = rows.getFirst();
    assertThat(row.<List<Object>>getProperty("a")).containsExactly(1L, 2L, 6L, 7L, 8L, 9L, 10L);
    assertThat(row.<List<Object>>getProperty("b")).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L);
    assertThat(row.<List<Object>>getProperty("c")).containsExactly(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
    assertThat(row.<List<Object>>getProperty("d")).containsExactly(1L, 2L, 3L, 4L, 6L, 7L, 8L, 9L, 10L);
  }

  // ---- helpers ----

  /** What a run did: the failure it ended with (null when it ended in a result) and the work done after the stop. */
  private record Run(Throwable failure, long calls, long callsAfterStop) {
  }

  /** Calls a function answers, with the call that terminates the statement and the calls after it. */
  private static final class Counter {
    final AtomicLong calls          = new AtomicLong();
    final AtomicLong callsAfterStop = new AtomicLong();
    final long       stopAt;
    volatile RunningQuery statement;

    Counter(final long stopAt) {
      this.stopAt = stopAt;
    }

    void count() {
      final long call = calls.incrementAndGet();
      if (call == stopAt)
        statement.terminate("root");
      else if (call > stopAt)
        callsAfterStop.incrementAndGet();
    }
  }

  private Counter registerCountingProbe() {
    return registerProbe(Long.MAX_VALUE);
  }

  private Counter registerProbe(final long stopAt) {
    final Counter counter = new Counter(stopAt);
    CypherFunctionRegistry.registerOrReplace(new StatelessFunction() {
      @Override
      public String getName() {
        return PROBE;
      }

      @Override
      public int getMinArgs() {
        return 1;
      }

      @Override
      public int getMaxArgs() {
        return 1;
      }

      @Override
      public String getDescription() {
        return "answers its argument, counting the calls";
      }

      @Override
      public Object execute(final Object[] args, final CommandContext context) {
        counter.count();
        return args[0];
      }
    });
    return counter;
  }

  private void assertStopsInsideTheRow(final String query) {
    final Run run = run(query);

    assertThat(causeOfType(run.failure, QueryTerminatedException.class)).as("the statement fails as terminated").isNotNull();
    assertThat(run.calls).as("the probe was reached").isGreaterThanOrEqualTo(STOP_AT);
    assertThat(run.callsAfterStop).as("steps taken after the statement was terminated").isLessThanOrEqualTo(ALLOWED_AFTER_STOP);
  }

  /** Runs {@code query} as a registered statement that the probe terminates at its tenth call. */
  private Run run(final String query) {
    final Counter counter = registerProbe(STOP_AT);
    try (final RunningQuery statement = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      counter.statement = statement;
      final Throwable failure = failureOf(query);
      return new Run(failure, counter.calls.get(), counter.callsAfterStop.get());
    }
  }

  /** Runs a statement that the tenth read of a record of {@code type} terminates, as a registered one. */
  private void assertStopsAfterTheNthRead(final String type, final String query) {
    final Counter counter = new Counter(STOP_AT);
    final AfterRecordReadListener listener = record -> {
      counter.count();
      return record;
    };

    database.getSchema().getType(type).getEvents().registerListener(listener);
    try (final RunningQuery statement = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      counter.statement = statement;
      final Throwable failure = failureOf(query);

      assertThat(causeOfType(failure, QueryTerminatedException.class)).as("the statement fails as terminated").isNotNull();
      assertThat(counter.calls.get()).as("records of the type were read").isGreaterThanOrEqualTo(STOP_AT);
      assertThat(counter.callsAfterStop.get()).as("records read after the statement was terminated").isLessThanOrEqualTo(ALLOWED_AFTER_STOP);
    } finally {
      database.getSchema().getType(type).getEvents().unregisterListener(listener);
    }
  }

  private Throwable failureOf(final String query) {
    try {
      drain(query);
      return null;
    } catch (final Throwable failure) {
      return failure;
    }
  }

  private List<Result> drain(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet resultSet = database.command("opencypher", query)) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }

  private static <T extends Throwable> T causeOfType(final Throwable failure, final Class<T> type) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause())
      if (type.isInstance(cause))
        return type.cast(cause);
    return null;
  }
}
