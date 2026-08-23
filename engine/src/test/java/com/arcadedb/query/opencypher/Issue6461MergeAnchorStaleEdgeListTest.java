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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6461: MERGE of a relationship duplicates it (and, for an unbound far
 * endpoint, its far endpoint) when the anchor vertex was bound earlier in the same query.
 * <p>
 * {@code MergeStep.executeMerge} wraps each row it processes in {@code database.transaction(..., true)}
 * (see the class Javadoc on that method / issue #6367). When the caller has no outer transaction open -
 * an unwrapped autocommit call, exactly the shape {@code BoltNetworkExecutor.handleRun} uses, and the
 * shape a raw engine {@code database.command(...)} call has - each row's MERGE owns and commits its own
 * transaction. The anchor vertex instance the row carries (bound by an earlier {@code MATCH}, or reused
 * across {@code UNWIND} rows of the same MERGE) was loaded before that per-row transaction started, so
 * once a prior row's MERGE appends the FIRST edge in a direction to it - the one write that rewrites the
 * vertex record's edge-list head pointer - the row's own instance still reflects the pre-append state.
 * {@code findAllMatchingPaths}/{@code traverseFromNode} trusted that instance and enumerated its edges
 * directly, missing the edge/node a previous row already created and creating a duplicate.
 * <p>
 * Over HTTP this is invisible: {@code DatabaseAbstractHandler.executeInTransaction()} wraps the whole
 * autocommit command in one outer transaction, so every row's MERGE simply joins it and shares its
 * transaction-local record cache. These tests reproduce the unwrapped shape directly against the engine.
 */
class Issue6461MergeAnchorStaleEdgeListTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-6461-merge-anchor-stale-edge-list").create();
    database.getSchema().createVertexType("Par");
    database.getSchema().createVertexType("Chi");
    database.getSchema().createVertexType("A");
    database.getSchema().createEdgeType("HAS");
    database.getSchema().createEdgeType("L");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * Realistic repro from the issue: an anchor bound once by MATCH, then reused by MERGE across several
   * UNWIND rows, some of which resolve to the same not-yet-existing child. The second `cid=10` row must
   * see the child/edge the first row created instead of creating a duplicate.
   */
  @Test
  void unwindMergeReusingBoundAnchorDoesNotDuplicateEdgeOrFarEndpoint() {
    database.command("opencypher", "CREATE (p:Par {id:1})");

    database.command("opencypher",
        "MATCH (p:Par {id:1}) UNWIND [10,10,20] AS cid MERGE (p)-[:HAS]->(c:Chi {id:cid})");

    final ResultSet edgeCount = database.query("opencypher", "MATCH (:Par)-[r:HAS]->(:Chi) RETURN count(r) AS c");
    assertThat(edgeCount.next().<Number>getProperty("c").longValue()).isEqualTo(2);

    final Map<Object, Long> childCountsById = new HashMap<>();
    final ResultSet children = database.query("opencypher", "MATCH (c:Chi) RETURN c.id AS id, count(*) AS c");
    while (children.hasNext()) {
      final Result r = children.next();
      childCountsById.put(r.getProperty("id"), ((Number) r.getProperty("c")).longValue());
    }
    assertThat(childCountsById).containsEntry(10L, 1L).containsEntry(20L, 1L);
  }

  /**
   * Minimal form from the issue: two MERGE clauses on the same already-bound pair in one query must
   * match the same edge the second time round, not create a second one.
   */
  @Test
  void repeatedMergeOnSameBoundPairDoesNotDuplicateEdge() {
    database.command("opencypher", "CREATE (a:A {n:'a'})");
    database.command("opencypher", "CREATE (b:A {n:'b'})");

    database.command("opencypher",
        "MATCH (a:A {n:'a'}),(b:A {n:'b'}) MERGE (a)-[:L]->(b) MERGE (a)-[:L]->(b)");

    final ResultSet edgeCount = database.query("opencypher", "MATCH (:A)-[r:L]->(:A) RETURN count(r) AS c");
    assertThat(edgeCount.next().<Number>getProperty("c").longValue()).isEqualTo(1);
  }
}
