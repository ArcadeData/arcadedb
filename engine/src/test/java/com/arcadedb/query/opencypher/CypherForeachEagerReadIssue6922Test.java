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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #6922: a clause that reads the graph after a FOREACH saw the
 * writes of a whole 100-row pull batch rather than either the pre- or the post-FOREACH graph, so the
 * value it reported depended on where the batch boundaries happened to fall.
 * <p>
 * {@code CALL meta.stats() YIELD value} after {@code UNWIND range(0, 100) AS i FOREACH (j IN
 * range(0, 9) | CREATE (:L1 ...))} reported {@code nodeCount = 1000} for the first 100 rows and
 * {@code 1010} for the 101st, and adding a row-preserving {@code collect()}/{@code UNWIND} behind it
 * changed the already-yielded numbers. FOREACH is now eager with respect to a following read, so
 * every row sees the same, complete post-FOREACH graph.
 */
class CypherForeachEagerReadIssue6922Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-foreach-eager-read-6922").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void metaStatsAfterForeachIsUnaffectedByDownstreamCollectUnwind() {
    final List<Object> control = queryColumn("""
        UNWIND range(0, 100) AS i
        FOREACH (j IN range(0, 9) | CREATE (:L1 {k5: j}))
        CALL meta.stats() YIELD value AS stats
        RETURN stats.nodeCount AS nodeCount""", "nodeCount");

    assertThat(control).as("one row per UNWIND value").hasSize(101);
    assertThat(totalNodes()).isEqualTo(1010L);
    assertThat(control)
        .as("every row must observe the whole FOREACH, not a 100-row pull batch of it")
        .containsOnly(1010L);

    // Fresh database: the materialized variant must produce exactly the same values.
    database.drop();
    database = new DatabaseFactory("./target/databases/testopencypher-foreach-eager-read-6922").create();

    final List<Object> materialized = queryColumn("""
        UNWIND range(0, 100) AS i
        FOREACH (j IN range(0, 9) | CREATE (:L1 {k5: j}))
        CALL meta.stats() YIELD value AS stats
        WITH stats.nodeCount AS nodeCount
        WITH collect(nodeCount) AS values
        UNWIND values AS nodeCount
        RETURN nodeCount""", "nodeCount");

    assertThat(totalNodes()).isEqualTo(1010L);
    assertThat(materialized)
        .as("a row-preserving collect()/UNWIND must not change an already-yielded value")
        .isEqualTo(control);
  }

  @Test
  void metaStatsAfterForeachDoesNotDependOnThePullBatchSize() {
    // 250 rows spans three 100-row pull batches: before the fix this returned 200 for the first 100
    // rows, 400 for the next 100 and 500 for the last 50 - three values chosen by the batch size.
    final List<Object> counts = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        CALL meta.stats() YIELD value AS stats
        RETURN stats.nodeCount AS nodeCount""", "nodeCount");

    assertThat(counts).hasSize(250);
    assertThat(totalNodes()).isEqualTo(500L);
    assertThat(counts).containsOnly(500L);
  }

  @Test
  void matchAfterForeachSeesEveryCreatedNode() {
    // Same read/write conflict reached through MATCH instead of CALL: the MATCH used to re-scan the
    // graph once per pull batch and count only the nodes created so far.
    final List<Object> counts = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        WITH i
        MATCH (n:L1)
        WITH i, count(n) AS visible
        RETURN visible""", "visible");

    assertThat(counts).hasSize(250);
    assertThat(counts).containsOnly(500L);
  }

  @Test
  void foreachWithoutAFollowingReadStillPassesEveryRowThrough() {
    // The eager mode must not be armed when nothing downstream reads the graph: the rows still flow
    // through unchanged and every write lands.
    final List<Object> rows = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        RETURN i AS value""", "value");

    assertThat(rows).hasSize(250);
    assertThat(rows).containsOnlyOnceElementsOf(rows);
    assertThat(rows.getFirst()).isEqualTo(1L);
    assertThat(rows.getLast()).isEqualTo(250L);
    assertThat(totalNodes()).isEqualTo(500L);
  }

  private List<Object> queryColumn(final String cypher, final String column) {
    final List<Object> values = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet resultSet = database.command("opencypher", cypher);
      while (resultSet.hasNext())
        values.add(resultSet.next().getProperty(column));
    });
    return values;
  }

  private long totalNodes() {
    final ResultSet resultSet = database.query("opencypher", "MATCH (n) RETURN count(n) AS nodes");
    return ((Number) resultSet.next().getProperty("nodes")).longValue();
  }
}
