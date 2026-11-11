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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11501: a SQL MATCH path item with {@code while:} or {@code maxDepth:} built EVERY path into one list before the
 * step returned its first row, so {@code LIMIT} bounded nothing, the command deadline was never checked inside the walk and
 * no heap limit was charged: a 42-vertex "ladder" (2^21 paths from one start) exhausted a 1 GB heap with {@code LIMIT 1}.
 * The walk now streams its paths depth first, holding only the vertices of the current path and their pending
 * neighbours, checks the deadline per vertex reached and charges what it holds to the operation's heap limit.
 * <p>
 * The {@code @Timeout}s are hang detectors, not latency bounds: before the fix these queries ran until the heap was gone.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11501MatchWhileLazyPathsTest extends TestHelper {
  /** 2^(LAYERS+1)-1 paths from one start: far more than any test heap can hold as path objects. */
  private static final int LAYERS = 40;
  private static final int HUB_DEGREE = 5_000;
  private static final String TRACKER_PROBE = "issue11501TrackerProbe";

  @Override
  public void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE VERTEX TYPE C");
    database.command("sql", "CREATE EDGE TYPE CE");
    database.command("sql", "CREATE VERTEX TYPE H");
    database.command("sql", "CREATE EDGE TYPE HE");
    database.transaction(() -> {
      // A LADDER: LAYERS + 1 LAYERS OF 2 VERTICES, EVERY VERTEX -E-> BOTH VERTICES OF THE NEXT LAYER
      MutableVertex[] previous = null;
      for (int layer = 0; layer <= LAYERS; layer++) {
        final MutableVertex[] current = new MutableVertex[] {
            database.newVertex("N").set("layer", layer, "k", 0).save(),
            database.newVertex("N").set("layer", layer, "k", 1).save() };
        if (previous != null)
          for (final MutableVertex from : previous)
            for (final MutableVertex to : current)
              from.newEdge("E", to).save();
        previous = current;
      }

      // A 2-VERTEX CYCLE: c0 -CE-> c1 -CE-> c0
      final MutableVertex c0 = database.newVertex("C").set("id", 0).save();
      final MutableVertex c1 = database.newVertex("C").set("id", 1).save();
      c0.newEdge("CE", c1).save();
      c1.newEdge("CE", c0).save();

      // A HUB WITH MANY NEIGHBOURS: ITS FRAME HOLDS THEM ALL, ENOUGH TO RESERVE A SHARE OF THE HEAP BUDGET
      final MutableVertex hub = database.newVertex("H").set("id", 0).save();
      for (int i = 0; i < HUB_DEGREE; i++)
        hub.newEdge("HE", database.newVertex("H").set("id", i + 1).save()).save();
    });
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void limitStopsAWhileTraversalBeforeEveryPathIsBuilt() {
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){while: (true), as: b} RETURN b.layer AS layer LIMIT 1")) {
      assertThat(rs.hasNext()).isTrue();
      // THE START ITSELF IS THE FIRST PATH (DEPTH 0) OF A WHILE ITEM
      assertThat(rs.next().<Integer>getProperty("layer")).isEqualTo(0);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void limitStopsAMaxDepthTraversalBeforeEveryPathIsBuilt() {
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){maxDepth: " + LAYERS
            + ", where: (layer = " + LAYERS + "), as: b} RETURN b.layer AS layer LIMIT 3")) {
      final List<Integer> layers = new ArrayList<>();
      rs.stream().forEach(r -> layers.add(r.getProperty("layer")));
      // THE FIRST DESCENT REACHES THE DEEPEST LAYER AT ONCE: ITS LEAVES ARE SIBLINGS IN WALK ORDER
      assertThat(layers).containsExactly(LAYERS, LAYERS, LAYERS);
    }
  }

  /**
   * The filter rejects every vertex, so the walk spends the whole query inside one {@code hasNext()} without emitting a row:
   * only a deadline checked inside the walk can stop it.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void theCommandDeadlineIsCheckedInsideTheWalk() {
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 300L);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){while: (true), where: (layer < 0), as: b} RETURN b")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(TimeoutException.class);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);
    }
  }

  /** A {@code while: (true)} around a cycle has infinitely many paths: {@code LIMIT} takes the first ones in walk order. */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aWhileAroundACycleStreamsItsFirstPaths() {
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (true), as: b, depthAlias: d} RETURN b.id AS id, d LIMIT 5")) {
      final List<String> rows = new ArrayList<>();
      rs.stream().forEach(r -> rows.add(r.<Integer>getProperty("id") + "@" + r.<Integer>getProperty("d")));
      assertThat(rows).containsExactly("0@0", "1@1", "0@2", "1@3", "0@4");
    }
  }

  /**
   * Without a bound, the walk around a cycle only gets deeper: what it holds is charged to the operation's element cap, so the
   * query fails with the usual over-budget error instead of a {@code StackOverflowError} or an {@code OutOfMemoryError}.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void anUnboundedWalkAroundACycleFailsOnTheHeapLimit() {
    final Object previousCap = database.getConfiguration().getValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (true), where: (id < 0), as: b} RETURN b")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, previousCap);
    }
  }

  /**
   * A walk stopped by {@code LIMIT} still holds its current path and the pending neighbours of the hub: closing the result set
   * gives that heap back to the query's share of the budget, instead of leaving it reserved until the garbage collector finds
   * the abandoned query. Read on the query's own tracker, not on the JVM-wide budget other tests share.
   */
  @Test
  void closingAWalkStoppedByLimitGivesItsHeapBack() {
    // THE BUDGET IS ON: THE STACK OF THE WALK IS CHARGED TO IT. TestHelper PUTS THE SETTING BACK AFTER THE TEST
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L << 30);
    final Set<QueryHeapTracker> trackers = ConcurrentHashMap.newKeySet();
    final AtomicLong peakUsed = new AtomicLong();
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(TRACKER_PROBE) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        // EVALUATED AT EVERY VERTEX THE WALK EXPANDS FROM: WHAT THE QUERY HOLDS WHILE THE WALK IS IN FLIGHT
        final QueryHeapTracker tracker = context.getQueryHeapTracker();
        trackers.add(tracker);
        peakUsed.accumulateAndGet(tracker.getUsedBytes(), Math::max);
        return true;
      }

      @Override
      public String getSyntax() {
        return TRACKER_PROBE + "()";
      }
    });
    try {
      try (final ResultSet rs = database.query("sql",
          "MATCH {type: H, where: (id = 0), as: a}.out('HE'){while: (" + TRACKER_PROBE + "()), as: b} RETURN b.id AS id LIMIT 1")) {
        // THE START ITSELF IS THE FIRST PATH: THE WHILE CONDITION WAS TAKEN, NOT DROPPED (A SINGLE HOP WOULD START AT A NEIGHBOUR)
        assertThat(rs.next().<Integer>getProperty("id")).isEqualTo(0);
      }
      assertThat(trackers).hasSize(1);
      // THE CONTROL: THE WALK DID CHARGE THE HUB'S PENDING NEIGHBOURS, SO THE ASSERTIONS BELOW CANNOT PASS ON A WALK THAT NEVER
      // CHARGED ANYTHING
      assertThat(peakUsed.get()).as("the walk held the hub's pending neighbours").isGreaterThanOrEqualTo((long) HUB_DEGREE * 64);
      final QueryHeapTracker tracker = trackers.iterator().next();
      assertThat(tracker.getUsedBytes()).as("closing the ResultSet gave the walk's heap back").isZero();
      assertThat(tracker.getReservedBytes()).as("and its share of the shared budget with it").isZero();
    } finally {
      sql.getFunctionFactory().unregister(TRACKER_PROBE);
    }
  }

  /**
   * A {@code where:} or {@code while:} whose parenthesized value is not a comparison - a function call, a bare property - was
   * dropped by the parser: the filter filtered nothing and the while item ran as a single hop. Both are now tested as
   * {@code <value> = true}, as a SQL {@code WHERE fn()} is.
   */
  @Test
  void aWhereOrWhileThatIsAPlainExpressionIsNotDropped() {
    database.transaction(() -> database.command("sql", "UPDATE C SET flag = (id = 1)"));

    // where: (property): ONLY c1 HAS flag = true
    assertThat(ids("MATCH {type: C, where: (flag), as: b} RETURN b.id AS id")).containsExactly(1);
    // where: (function call)
    assertThat(ids("MATCH {type: C, where: (ifnull(flag, false)), as: b} RETURN b.id AS id")).containsExactly(1);
    // while: (function call): c0 HAS flag = false, SO ONLY THE START IS RETURNED. A DROPPED WHILE WOULD ANSWER THE SINGLE HOP [1]
    assertThat(ids("MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (ifnull(flag, true)), as: b} RETURN b.id AS id"))
        .containsExactly(0);
    // while: (property): c0 HAS flag = false, SO ONLY THE START IS RETURNED, NOT EXPANDED
    assertThat(ids("MATCH {type: C, where: (id = 0), as: a}.out('CE'){while: (flag), as: b} RETURN b.id AS id")).containsExactly(0);
  }

  private List<Integer> ids(final String query) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> ids.add(r.getProperty("id")));
    }
    return ids;
  }

  /**
   * A multi-item path {@code .(out(){while: ...})} has to collect the paths of its while item, because the next sub-item
   * starts from each of them: that list is held under the element cap too.
   */
  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  void aMultiItemWhileCollectsUnderTheHeapLimit() {
    final Object previousCap = database.getConfiguration().getValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 1000L);
    try {
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql",
            "MATCH {type: C, where: (id = 0), as: a}.(out('CE'){while: (true)}){as: b} RETURN b")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, previousCap);
    }

    // BOUNDED, THE SAME SHAPE ANSWERS: c0 (DEPTH 0), c1 (1), c0 (2)
    try (final ResultSet rs = database.query("sql",
        "MATCH {type: C, where: (id = 0), as: a}.(out('CE'){while: ($depth < 2)}){as: b} RETURN b.id AS id")) {
      final List<Integer> ids = new ArrayList<>();
      rs.stream().forEach(r -> ids.add(r.getProperty("id")));
      assertThat(ids).containsExactly(0, 1, 0);
    }
  }

  /**
   * The streamed walk answers exactly what the materialized one did, in the same depth-first pre-order, with the same
   * {@code depthAlias} and {@code pathAlias} values.
   */
  @Test
  void theStreamedWalkKeepsOrderDepthAndPath() {
    final String query = "MATCH {type: N, where: (layer = 0 AND k = 0), as: a}.out('E'){while: ($depth < 2), as: b, "
        + "depthAlias: d, pathAlias: p} RETURN b.layer AS layer, b.k AS k, d, p";
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.stream().forEach(r -> {
        final List<String> path = new ArrayList<>();
        for (final Object step : r.<List<?>>getProperty("p")) {
          final RID rid = ((Identifiable) step).getIdentity();
          final Result v = database.query("sql", "SELECT layer, k FROM " + rid).next();
          path.add(v.<Integer>getProperty("layer") + "." + v.<Integer>getProperty("k"));
        }
        rows.add(r.<Integer>getProperty("layer") + "." + r.<Integer>getProperty("k") + "@" + r.<Integer>getProperty("d") + path);
      });
    }
    assertThat(rows).containsExactly(//
        "0.0@0[]", //
        "1.1@1[1.1]", "2.1@2[1.1, 2.1]", "2.0@2[1.1, 2.0]", //
        "1.0@1[1.0]", "2.1@2[1.0, 2.1]", "2.0@2[1.0, 2.0]");
  }
}
