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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
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
  /** 250 rows spans three of the pipeline's 100-row pull batches, and creates 500 nodes in total. */
  private static final String BATCH_VISIBILITY_QUERY = """
      UNWIND range(1, 250) AS i
      FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
      CALL meta.stats() YIELD value AS stats
      RETURN stats.nodeCount AS nodeCount""";

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
    // Before the fix this returned 200 for the first 100 rows, 400 for the next 100 and 500 for the
    // last 50 - three values chosen by the pull batch size rather than by the graph.
    final List<Object> counts = queryColumn(BATCH_VISIBILITY_QUERY, "nodeCount");

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
  void countSubqueryExpressionInReturnSeesEveryCreatedNode() {
    // The read is an expression inside a RETURN item, not a clause of its own: a scan that classifies
    // a later clause by its type alone waves the RETURN through and leaves the FOREACH streaming.
    final List<Object> counts = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        RETURN count { MATCH (n:L1) } AS visible""", "visible");

    assertThat(counts).hasSize(250);
    assertThat(counts).containsOnly(500L);
  }

  @Test
  void collectSubqueryExpressionNestedInAFunctionCallSeesEveryCreatedNode() {
    // Same read, one level deeper: the COLLECT subquery is an argument of size(), so finding it needs
    // the walk to descend through the function call.
    final List<Object> counts = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        RETURN size(collect { MATCH (n:L1) RETURN n.i }) AS visible""", "visible");

    assertThat(counts).hasSize(250);
    assertThat(counts).containsOnly(500L);
  }

  @Test
  void existsSubqueryExpressionInWithWhereSeesEveryCreatedNode() {
    // The read is in a WITH's WHERE, reached through the boolean coercion the predicate is wrapped in.
    // It asks for the node the FOREACH only creates on the last row, so a streaming pipeline filtered
    // out every row produced before that write landed and returned 50 of the 250.
    final List<Object> rows = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN [0] | CREATE (:L1 {i: i}))
        WITH i WHERE exists { MATCH (n:L1 {i: 250}) }
        RETURN i AS value""", "value");

    assertThat(rows).hasSize(250);
    assertThat(rows.getFirst()).isEqualTo(1L);
    assertThat(rows.getLast()).isEqualTo(250L);
  }

  @Test
  void graphReadNestedInsideAContainerExpressionArmsEagerness() {
    // The subquery is buried in a CASE branch and in a list literal. Both are containers whose walk
    // has to descend into them; stopping at the top of the RETURN item finds nothing.
    final List<Object> fromCase = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        RETURN CASE WHEN true THEN count { MATCH (n:L1) } ELSE 0 END AS visible""", "visible");

    assertThat(fromCase).hasSize(250);
    assertThat(fromCase).containsOnly(500L);

    database.drop();
    database = new DatabaseFactory("./target/databases/testopencypher-foreach-eager-read-6922").create();

    final List<Object> fromList = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        RETURN [count { MATCH (n:L1) }] AS visible""", "visible");

    assertThat(fromList).hasSize(250);
    assertThat(fromList).containsOnly(List.of(500L));
  }

  @Test
  void graphReadInALaterForeachListExpressionArmsEagerness() {
    // A FOREACH's driving list is an expression like any other and is evaluated per row against the
    // live graph. Here it filters on a node the first FOREACH only creates on its last row, so a
    // streaming pipeline produced an empty list - and no :Marker - for every row before that write.
    final List<Object> rows = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN [1] | CREATE (:L1 {i: i}))
        FOREACH (n IN [x IN range(1, 1) WHERE exists { MATCH (m:L1 {i: 250}) } | x] | CREATE (:Marker {i: i}))
        RETURN i AS value""", "value");

    assertThat(rows).hasSize(250);
    assertThat(countOf("Marker")).as("every row's FOREACH body ran").isEqualTo(250L);
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

  @Test
  void mergeAfterForeachMatchesANodeTheForeachCreatedForALaterRow() {
    // MERGE is a read as well as a write, and it is the only triggering clause here - adding a MATCH
    // or CALL to observe the graph would arm the eager mode by itself and prove nothing about MERGE.
    // The FOREACH gives every row its own (:L1 {i}); MERGE then asks for the very last one, which
    // only a fully applied FOREACH can already hold. Streaming batches emit the first row while
    // i = 250 is still 150 rows away, so MERGE creates a 251st node instead of matching.
    final List<Object> rows = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN [0] | CREATE (:L1 {i: i}))
        MERGE (last:L1 {i: 250})
        RETURN i AS value""", "value");

    assertThat(rows).hasSize(250);
    assertThat(countOf("L1")).as("MERGE matched the existing node instead of creating a 251st").isEqualTo(250L);
  }

  @Test
  void mergeInsideALaterForeachAlsoArmsEagerness() {
    // Same failure as mergeAfterForeachMatchesANodeTheForeachCreatedForALaterRow, only the MERGE is
    // tucked inside a second FOREACH. A scan that looks at the later clause's own type alone sees a
    // FOREACH, calls it a write, and leaves the first FOREACH streaming - so the nested MERGE created
    // a 251st node.
    final List<Object> rows = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN [1] | CREATE (:L1 {i: i}))
        FOREACH (j IN [1] | MERGE (last:L1 {i: 250}))
        RETURN i AS value""", "value");

    assertThat(rows).hasSize(250);
    assertThat(countOf("L1")).as("the nested MERGE matched instead of creating a 251st node").isEqualTo(250L);
  }

  @Test
  void callSubqueryAfterForeachSeesEveryCreatedNode() {
    // A CALL {} subquery is opaque to the clause scan but can hold a MATCH, so it counts as a read.
    final List<Object> counts = queryColumn("""
        UNWIND range(1, 250) AS i
        FOREACH (j IN range(0, 1) | CREATE (:L1 {i: i, j: j}))
        CALL { MATCH (n:L1) RETURN count(n) AS visible }
        RETURN visible""", "visible");

    assertThat(counts).hasSize(250);
    assertThat(counts).containsOnly(500L);
  }

  @Test
  void eagerReadIsTradedBackForStreamingPerDatabase() {
    // arcadedb.opencypher.foreachEagerRead is the escape hatch for a bulk query whose input does not
    // fit in memory. It is SCOPE.DATABASE, so it is read off this database's ContextConfiguration:
    // turning it off here must not reach a second database that never set it.
    database.getConfiguration().setValue(GlobalConfiguration.OPENCYPHER_FOREACH_EAGER_READ, false);

    final List<Object> streamed = queryColumn(BATCH_VISIBILITY_QUERY, "nodeCount");
    assertThat(streamed).hasSize(250);
    assertThat(totalNodes()).as("every write still lands").isEqualTo(500L);
    assertThat(new LinkedHashSet<>(streamed))
        .as("streaming visibility restored: the rows disagree again")
        .hasSizeGreaterThan(1);

    final Database other = new DatabaseFactory("./target/databases/testopencypher-foreach-eager-read-6922-other").create();
    try {
      final List<Object> eager = new ArrayList<>();
      other.transaction(() -> {
        final ResultSet resultSet = other.command("opencypher", BATCH_VISIBILITY_QUERY);
        while (resultSet.hasNext())
          eager.add(resultSet.next().getProperty("nodeCount"));
      });
      assertThat(eager).as("a database that never set the flag keeps the eager default").containsOnly(500L);
    } finally {
      other.drop();
    }
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

  private long countOf(final String label) {
    final ResultSet resultSet = database.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS nodes");
    return ((Number) resultSet.next().getProperty("nodes")).longValue();
  }
}
