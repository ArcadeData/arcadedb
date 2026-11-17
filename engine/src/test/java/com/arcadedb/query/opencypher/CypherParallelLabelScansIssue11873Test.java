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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.query.sql.executor.QueryHeapBudgetTestSupport;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11873: three shapes of openCypher label scan ran on the calling thread where SQL, and openCypher's own filtered
 * label scan, ran in the workers of a parallel scan: a filter calling a built-in function, a large index range served by a
 * label scan, and a top-K ORDER BY no index serves. Every parallel answer is checked against the same query with parallel
 * scans disabled, rows and order.
 */
class CypherParallelLabelScansIssue11873Test extends TestHelper {
  private static final int ROWS = 20_000;

  @Override
  protected void beginTest() {
    // Small units, so the fixture's few hundred pages are cut in many of them
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);
    final VertexType type = database.getSchema().createVertexType("T", 4);
    type.createProperty("k", Type.INTEGER);
    type.createProperty("g", Type.INTEGER);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "g");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newVertex("T").set("k", i, "g", i % 1000, "h", i % 97, "name", "N" + (i % 50)).save();
    });
  }

  @Test
  void aFilterCallingABuiltInFunctionRunsInParallel() {
    for (final String predicate : new String[] { "toString(n.k) STARTS WITH '12'", "abs(n.k - 10000) < 50",
        "toLower(n.name) = 'n7'", "size(n.name) = 2 AND n.h = 3", "coalesce(n.missing, n.h) = 5", "sqrt(n.k) > 140.5",
        "substring(toString(n.g), 0, 2) = '99'" }) {
      final String query = "MATCH (n:T) WHERE " + predicate + " RETURN n.k AS k";
      assertThat(profile(query)).as(query).contains("NodeByLabelScan").contains("[parallel]");
      assertThat(column(query)).as(query).isNotEmpty().isEqualTo(sequential(query));
    }

    final String count = "MATCH (n:T) WHERE toString(n.k) STARTS WITH '12' RETURN count(*) AS c";
    assertThat(profile(count)).contains("[parallel]");
    assertThat(column(count)).containsExactly(1111L).isEqualTo(sequential(count));
  }

  @Test
  void aFilterCallingAFunctionTheWorkersCannotRunStaysSequential() {
    for (final String predicate : new String[] { "sql.abs(n.h) = 5", "rand() < 2" }) {
      final String query = "MATCH (n:T) WHERE " + predicate + " RETURN n.k AS k";
      assertThat(profile(query)).as(query).doesNotContain("[parallel]");
    }
    final String query = "MATCH (n:T) WHERE sql.abs(n.h) = 5 RETURN n.k AS k";
    assertThat(column(query)).isNotEmpty().isEqualTo(sequential(query));
  }

  @Test
  void aLargeRangeServedByALabelScanRunsInParallel() {
    for (final String query : new String[] {
        "MATCH (n:T) WHERE n.k > 2000 RETURN count(n) AS c",
        "MATCH (n:T) WHERE n.k > 2000 AND n.h < 50 RETURN sum(n.g) AS c",
        "MATCH (n:T) WHERE n.k >= 1000 RETURN n.k AS k ORDER BY k" }) {
      final String plan = profile(query);
      assertThat(plan).as(query).contains("served by label scan: large range").contains("[parallel]").doesNotContain("Filter");
      assertThat(column(query)).as(query).isNotEmpty().isEqualTo(sequential(query));
    }
    assertThat(column("MATCH (n:T) WHERE n.k > 2000 RETURN count(n) AS c")).containsExactly(17_999L);
  }

  @Test
  void aLargeRangeWithAPredicateTheWorkersCannotRunStaysSequential() {
    final String query = "MATCH (n:T) WHERE n.k > 2000 AND sql.abs(n.h) = 5 RETURN count(n) AS c";
    final String plan = profile(query);
    assertThat(plan).contains("served by label scan: large range").doesNotContain("[parallel]");
    assertThat(column(query)).isEqualTo(sequential(query));
  }

  @Test
  void aSmallRangeIsStillServedByTheIndex() {
    final String query = "MATCH (n:T) WHERE n.k < 100 RETURN count(n) AS c";
    assertThat(profile(query)).contains("served in physical order").doesNotContain("[parallel]");
    assertThat(column(query)).containsExactly(100L);
  }

  @Test
  void aTopKOrderByRunsInParallel() {
    for (final String query : new String[] {
        "MATCH (n:T) RETURN n.k AS k ORDER BY n.g DESC, n.k LIMIT 10",
        "MATCH (n:T) RETURN n.k AS k ORDER BY n.h, n.k DESC SKIP 5 LIMIT 10",
        "MATCH (n:T) WHERE n.name STARTS WITH 'N1' RETURN n.k AS k ORDER BY n.h DESC, n.k LIMIT 7",
        "MATCH (n:T) RETURN toString(n.k) AS s ORDER BY n.h, s LIMIT 5",
        "MATCH (n:T) WHERE n.k > 2000 RETURN n.k AS k ORDER BY n.h DESC, n.k LIMIT 10" }) {
      assertThat(profile(query)).as(query).contains("parallel: ").contains(" workers)");
      assertThat(column(query)).as(query).isNotEmpty().isEqualTo(sequential(query));
    }
  }

  /**
   * Among rows with equal keys the top K keeps the ones the scan meets first, in the order it meets them, sequential or in
   * parallel: a stable sort of every row cut at K.
   */
  @Test
  void aTopKWithTiesKeepsTheRowsAStableSortKeeps() {
    final List<Result> all = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:T) RETURN n.k AS k, n.h AS h")) {
      while (rs.hasNext())
        all.add(rs.next());
    }
    final List<Object> expected = all.stream()
        .sorted(Comparator.comparing((Result r) -> (Integer) r.getProperty("h")).reversed())
        .limit(25).map(r -> r.getProperty("k")).toList();

    final String query = "MATCH (n:T) RETURN n.k AS k ORDER BY n.h DESC LIMIT 25";
    assertThat(sequential(query)).isEqualTo(expected);
    assertThat(column(query)).isEqualTo(expected);
    assertThat(profile(query)).contains("parallel: ");
  }

  @Test
  void aTopKTheWorkersCannotRunStaysSequential() {
    for (final String query : new String[] {
        "MATCH (n:T) RETURN n.k AS k ORDER BY sql.abs(n.h), n.k LIMIT 5",
        "MATCH (n:T) RETURN DISTINCT n.h AS h ORDER BY h LIMIT 5",
        "MATCH (n:T) RETURN n.k AS k ORDER BY rand() LIMIT 5" }) {
      assertThat(profile(query)).as(query).doesNotContain("parallel: ");
    }
    final String query = "MATCH (n:T) RETURN DISTINCT n.h AS h ORDER BY h LIMIT 5";
    assertThat(column(query)).containsExactly(0, 1, 2, 3, 4);
  }

  /**
   * A top-K heap keeps every row's sort keys beside it, in the workers and on the calling thread, and charges them to the
   * query's heap budget with the row: a key computed for the sort can be far larger than the row returned. Here 100 rows of
   * one integer each sort on keys of 32 KB, over 3 MB the 1 MB budget refuses, which it never saw when only the rows were
   * charged.
   */
  @Test
  void aTopKChargesTheSortKeysItKeeps() {
    final String query = "MATCH (n:T) RETURN n.k AS k ORDER BY $pad + toString(n.k) DESC LIMIT 100";
    final Map<String, Object> parameters = Map.of("pad", "x".repeat(32 * 1024));
    assertThat(profile(query, parameters)).contains("parallel: ");
    final List<Object> expected = sequential(query, parameters);
    assertThat(expected).hasSize(100).first().isEqualTo(9_999);

    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1);
    for (final boolean parallel : new boolean[] { true, false }) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
      assertThatThrownBy(() -> column(query, parameters)).as("parallel scan " + parallel)
          .isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
      assertThat(QueryHeapBudget.getReservedBytes()).as("the refused query gave its keys back").isZero();
    }
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);

    // The keys go with the heap: the rows returned are all the query holds once the sort is done
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
    assertThat(column(query, parameters)).isEqualTo(expected);
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  /**
   * A row that replaces the worst one of a full top-K heap is charged the difference of row AND keys in one step: here the row
   * kept first is small with a 700 KB key and the row replacing it 700 KB with a small key, so the heap holds about 700 KB before
   * and after, under the 1 MB budget. Charging the larger row before giving back the larger key held 1.4 MB for a moment, which
   * the budget refused.
   */
  @Test
  void aTopKReplacementIsChargedTheNetChangeOfRowAndKeys() {
    final String query = "UNWIND [0, 1] AS i RETURN i AS i, CASE i WHEN 1 THEN $big ELSE '' END AS payload "
        + "ORDER BY CASE i WHEN 0 THEN $big + 'b' ELSE 'a' END LIMIT 1";
    final Map<String, Object> parameters = Map.of("big", "x".repeat(700 * 1024));

    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(1);
    assertThat(column(query, parameters)).containsExactly(1L);
    assertThat(QueryHeapBudget.getReservedBytes()).isZero();
  }

  /**
   * The element cap holds for the merged answer of a parallel top K as it does for the sequential heap. Here about 200 rows
   * match, spread over the workers so that each keeps fewer than the cap of 100, while the LIMIT of 150 has the sequential heap
   * hold 150: the merge must be refused as the sequential heap is, not answer the 150 rows no single worker was refused for.
   */
  @Test
  void aParallelTopKIsRefusedByTheElementCapAsTheSequentialOneIs() {
    final String query = "MATCH (n:T) WHERE n.h = 3 RETURN n.k AS k ORDER BY n.g DESC, n.k LIMIT 150";
    assertThat(profile(query)).contains("parallel: ");
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP, 100);
    for (final boolean parallel : new boolean[] { false, true }) {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
      assertThatThrownBy(() -> column(query)).as("parallel scan " + parallel)
          .isInstanceOf(HeapLimitExceededException.class)
          .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey());
    }
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);

    // Within the cap the parallel answer is the sequential one
    final String withinCap = "MATCH (n:T) WHERE n.h = 3 RETURN n.k AS k ORDER BY n.g DESC, n.k LIMIT 100";
    assertThat(column(withinCap)).hasSize(100).isEqualTo(sequential(withinCap));
  }

  /**
   * The tie-break of a parallel top K follows the order the sequential label scan meets the buckets, which is the order of the
   * type's bucket list, not the order of the bucket ids. Here the type's last bucket is older than its others (its id is
   * lower): every row ties on the key, so the top K is the first K rows the scan meets, and the parallel answer must be the
   * sequential one.
   */
  @Test
  void aParallelTopKBreaksTiesInTheOrderOfTheTypesBucketList() {
    final Bucket older = database.getSchema().createBucket("older");
    final VertexType type = database.getSchema().createVertexType("U", 2);
    type.addBucket(older);
    assertThat(type.getBuckets(false).getLast().getFileId()).isLessThan(type.getBuckets(false).getFirst().getFileId());
    database.transaction(() -> {
      for (int i = 0; i < 6_000; i++) {
        final MutableVertex vertex = database.newVertex("U").set("id", i, "t", 0, "pad", "p".repeat(64));
        if (i % 3 == 0)
          vertex.save("older");
        else
          vertex.save();
      }
    });

    final String query = "MATCH (n:U) RETURN n.id AS id ORDER BY n.t LIMIT 20";
    assertThat(profile(query)).contains("parallel: ");
    final List<Object> expected = sequential(query);
    assertThat(expected).hasSize(20);
    assertThat(column(query)).isEqualTo(expected);
  }

  private List<Object> sequential(final String query) {
    return sequential(query, Map.of());
  }

  private List<Object> sequential(final String query, final Map<String, Object> parameters) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, false);
    try {
      return column(query, parameters);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private List<Object> column(final String query) {
    return column(query, Map.of());
  }

  private List<Object> column(final String query, final Map<String, Object> parameters) {
    final List<Object> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, parameters)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        values.add(row.getProperty(row.getPropertyNames().iterator().next()));
      }
    }
    return values;
  }

  private String profile(final String query) {
    return profile(query, Map.of());
  }

  private String profile(final String query, final Map<String, Object> parameters) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
