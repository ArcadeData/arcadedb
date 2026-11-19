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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10115: a TRAVERSE charged the heap budget ({@code arcadedb.queryMaxHeapRAM}) 24 bytes or so per record visited and
 * nothing for its frontier, on the ground that the frontier never holds a record the visited set does not. That holds in count,
 * not in bytes: a frontier entry is a row holding its record and its own {@code $path} and {@code $stack} lists, one identity per
 * hop, so a wide frontier at the end of long paths held megabytes the budget never saw.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue10115TraverseFrontierHeapTest extends TestHelper {
  private static final List<String> STRATEGIES = List.of("DEPTH_FIRST", "BREADTH_FIRST");

  @AfterEach
  void resetBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  /**
   * A chain of 500 vertices ending on a hub of 500 leaves: the visited set takes some 24KB breadth first and 170KB depth first,
   * while the frontier, once the hub is expanded, holds 500 rows each carrying two lists of 500 identities, about 4MB. Under a
   * 1MB budget the traversal used to run; it now fails on the budget, and gives back what it took.
   */
  @Test
  void aWideFrontierAtTheEndOfLongPathsIsChargedToTheBudget() {
    final int chain = 500;
    final int leaves = 500;
    createChain(chain, leaves);

    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    final long baseline = QueryHeapBudget.getReservedBytes();

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    for (final String strategy : STRATEGIES) {
      assertThatThrownBy(() -> drain(traverse(strategy))).as(strategy)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).as(strategy).isLessThanOrEqualTo(baseline);
    }

    // with room, the same traversal answers what it always did, and gives everything back once closed
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
    for (final String strategy : STRATEGIES) {
      assertThat(drain(traverse(strategy))).as(strategy).isEqualTo(chain + leaves);
      assertThat(QueryHeapBudget.getReservedBytes()).as(strategy).isLessThanOrEqualTo(baseline);
    }
  }

  /**
   * The frontier gives back what an entry took as soon as the entry leaves it: a chain of 2,000 vertices holds one or two entries
   * at a time, each with a path of up to 2,000 identities. Charged and never released, the entries would add up to some 30MB; the
   * traversal fits a 1MB budget because the frontier is charged for what it holds, not for what went through it.
   */
  @Test
  void theFrontierIsChargedForWhatItHoldsNotForWhatWentThroughIt() {
    final int chain = 2_000;
    createChain(chain, 0);

    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    final long baseline = QueryHeapBudget.getReservedBytes();

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1L);
    for (final String strategy : STRATEGIES) {
      assertThat(drain(traverse(strategy))).as(strategy).isEqualTo(chain);
      assertThat(QueryHeapBudget.getReservedBytes()).as(strategy).isLessThanOrEqualTo(baseline);
    }
  }

  /**
   * A row handed downstream leaves the frontier: a client reading the rows one by one does not hold the traversal's charge for
   * the rows it already read, and closing the result set before the end gives back what the frontier still holds.
   */
  @Test
  void aResultSetClosedBeforeTheEndGivesTheFrontierBack() {
    createChain(500, 500);

    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    final long baseline = QueryHeapBudget.getReservedBytes();

    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(256L);
    for (final String strategy : STRATEGIES) {
      try (final ResultSet rs = database.query("sql", "TRAVERSE out('Next') FROM (SELECT FROM Link WHERE k = 0) STRATEGY " + strategy)) {
        for (int i = 0; i < 600; i++) {
          assertThat(rs.hasNext()).as(strategy).isTrue();
          rs.next();
        }
        // past the hub, its leaves fill the frontier: about 4MB, beyond what a query holds before reserving
        assertThat(QueryHeapBudget.getReservedBytes()).as(strategy).isGreaterThan(baseline + QueryHeapTracker.UNRESERVED_BYTES);
      }
      assertThat(QueryHeapBudget.getReservedBytes()).as(strategy).isLessThanOrEqualTo(baseline);
    }
  }

  /**
   * The identities a {@code $path} or a {@code $stack} lists are the records' own, shared by every entry whose path runs through
   * them: a frontier entry is charged a slot per hop for each list, not the identity in it, which the general estimate of a row
   * would count again for every entry.
   */
  @Test
  void aFrontierEntryCountsItsPathBySlots() {
    final List<Object> shortPath = new ArrayList<>();
    final List<Object> longPath = new ArrayList<>();
    for (int i = 0; i < 1_000; i++) {
      final RID rid = new RID(3, i);
      if (i < 10)
        shortPath.add(rid);
      longPath.add(rid);
    }
    final long shortEntry = HeapEstimator.estimateTraverseEntry(entry(shortPath));
    final long longEntry = HeapEstimator.estimateTraverseEntry(entry(longPath));
    assertThat(longEntry - shortEntry).isEqualTo(2L * HeapEstimator.REFERENCE_BYTES * (longPath.size() - shortPath.size()));
    // the general estimate counts every identity in full
    assertThat(HeapEstimator.estimate(entry(longPath))).isGreaterThan(longEntry);
    // what is not a path is counted as any other metadata
    final TraverseResult withOther = entry(shortPath);
    withOther.setMetadata("other", longPath);
    assertThat(HeapEstimator.estimateTraverseEntry(withOther)).isGreaterThan(longEntry);
  }

  private static TraverseResult entry(final List<Object> path) {
    final TraverseResult entry = new TraverseResult();
    entry.setMetadata("$depth", path.size() - 1);
    entry.setMetadata("$path", new ArrayList<>(path));
    final List<Object> stack = new ArrayList<>(path);
    Collections.reverse(stack);
    entry.setMetadata("$stack", stack);
    return entry;
  }

  private static String traverse(final String strategy) {
    return "SELECT count(*) AS c FROM (TRAVERSE out('Next') FROM (SELECT FROM Link WHERE k = 0) STRATEGY " + strategy + ")";
  }

  /** {@code chain} vertices linked one after the other, the last one linked to {@code leaves} leaves. */
  private void createChain(final int chain, final int leaves) {
    database.command("sql", "CREATE VERTEX TYPE Link");
    database.command("sql", "CREATE VERTEX TYPE Leaf");
    database.command("sql", "CREATE EDGE TYPE Next");
    database.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < chain; i++) {
        final MutableVertex v = database.newVertex("Link").set("k", i).save();
        if (previous != null)
          previous.newEdge("Next", v);
        previous = v;
      }
      for (int i = 0; i < leaves; i++)
        previous.newEdge("Next", database.newVertex("Leaf").set("k", i).save());
    });
  }

  private long drain(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
