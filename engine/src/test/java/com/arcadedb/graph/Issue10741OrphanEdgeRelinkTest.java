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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10741: a {@code GraphBatch} that dies before {@code close()} (a {@code kill -9}, an OOM kill) leaves the edge
 * records its flushes committed in the database, while the vertices' head-chunk pointers to them were still deferred in
 * memory, so no adjacency list reaches them. {@code CHECK DATABASE} reported them as ORPHAN RECORDs, a plain {@code FIX}
 * changed nothing and {@code FIX DELETE ORPHANS} deleted them: the committed edges were lost, or loaded again. A plain
 * {@code FIX} now relinks every orphan whose endpoints are live vertices, which loses nothing - the record names both
 * endpoints, and a type scan returns it already - and the graph traverses as the batch would have left it.
 * <p>
 * The process death is reproduced by abandoning the batch after its flushes: it is never closed, the database is closed
 * and reopened, which is what a new process finds after the kill.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10741OrphanEdgeRelinkTest extends TestHelper {
  private static final int VERTICES = 1_000;
  private static final int EDGES    = 2_000;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // THE ABANDONED BATCH IS THE DAMAGE UNDER TEST
    return false;
  }

  @Test
  void plainFixRelinksTheEdgesOfABatchThatDiedBeforeClose() {
    database.getSchema().createVertexType("V").createProperty("id", Integer.class);
    database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();
    database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();

    final GraphBatch batch = database.batch().build();
    final RID[] vertices = batch.createVertices("V", VERTICES);
    for (int i = 0; i < EDGES; i++) {
      batch.newEdge(vertices[(i * 7) % VERTICES], "E", vertices[(i * 13 + 1) % VERTICES], "eid", i);
      batch.newEdge(vertices[(i * 11) % VERTICES], "U", vertices[(i * 3 + 2) % VERTICES], "eid", i);
      if (i % 500 == 499)
        batch.flush();
    }
    // THE PROCESS DIES HERE: close() IS NEVER REACHED
    reopenDatabase();

    assertThat(count("SELECT count(@rid) AS c FROM E")).isEqualTo(EDGES);
    assertThat(count("SELECT count(@rid) AS c FROM U")).isEqualTo(EDGES);

    final Result report = check("CHECK DATABASE");
    final long orphans = report.<Long>getProperty("unreachableEdgeRecords");
    assertThat(orphans).as("the abandoned batch must leave orphan edge records, or this test proves nothing").isPositive();
    assertThat(count("SELECT count(@rid) AS c FROM E")).as("a report changes nothing").isEqualTo(EDGES);

    final Result fix = check("CHECK DATABASE FIX");
    assertThat(fix.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(orphans);
    assertThat(fix.<Long>getProperty("removedRecords")).isZero();

    // EVERY EDGE RECORD IS REACHABLE FROM ITS SOURCE, AND A BIDIRECTIONAL ONE FROM ITS TARGET TOO, EACH EXACTLY ONCE
    assertThat(count("SELECT count(@rid) AS c FROM E")).isEqualTo(EDGES);
    assertThat(count("SELECT sum(outE('E').size()) AS c FROM V")).isEqualTo(EDGES);
    assertThat(count("SELECT sum(inE('E').size()) AS c FROM V")).isEqualTo(EDGES);
    assertThat(count("SELECT sum(outE('U').size()) AS c FROM V")).isEqualTo(EDGES);
    assertThat(distinctEids("SELECT expand(outE('E')) FROM V")).isEqualTo(EDGES);
    assertThat(distinctEids("SELECT expand(inE('E')) FROM V")).isEqualTo(EDGES);
    assertThat(distinctEids("SELECT expand(outE('U')) FROM V")).isEqualTo(EDGES);

    // THE EDGES ARE WHERE THE BATCH PUT THEM: SOURCE AND TARGET OF EACH ONE AS IT WAS CREATED
    try (final ResultSet rs = database.query("sql", "SELECT eid, @out AS src, @in AS dst FROM E")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final int i = row.<Integer>getProperty("eid");
        assertThat(row.<RID>getProperty("src")).isEqualTo(vertices[(i * 7) % VERTICES]);
        assertThat(row.<RID>getProperty("dst")).isEqualTo(vertices[(i * 13 + 1) % VERTICES]);
      }
    }

    final Result after = check("CHECK DATABASE");
    assertThat(after.<Long>getProperty("unreachableEdgeRecords")).as("nothing is left to relink").isZero();
    assertThat(after.<Long>getProperty("edgesMissingOutReference")).isZero();
    assertThat(after.<Long>getProperty("edgesMissingInReference")).isZero();
  }

  /**
   * A self-loop orphan of a bidirectional type goes back into both lists of its one vertex, each exactly once, and one of a
   * unidirectional type into the OUT list only.
   */
  @Test
  void aSelfLoopOrphanIsRelinkedOncePerList() {
    database.getSchema().createVertexType("V");
    database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();
    database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();

    final GraphBatch batch = database.batch().build();
    final RID[] vertices = batch.createVertices("V", 2);
    batch.newEdge(vertices[0], "E", vertices[0], "eid", 1);
    batch.newEdge(vertices[1], "U", vertices[1], "eid", 2);
    batch.flush();
    reopenDatabase();

    final long orphans = check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords");
    assertThat(orphans).as("the abandoned batch must leave orphan edge records, or this test proves nothing").isEqualTo(2L);

    final Result fix = check("CHECK DATABASE FIX");
    assertThat(fix.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(2L);
    // THE BIDIRECTIONAL ONE TWO ENTRIES (OUT AND IN OF THE SAME VERTEX), THE UNIDIRECTIONAL ONE ONE
    assertThat(fix.<Long>getProperty("reconnectedEdges")).isEqualTo(3L);

    final Vertex loop = database.lookupByRID(vertices[0], true).asVertex();
    assertThat(loop.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
    assertThat(loop.countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
    assertThat(database.lookupByRID(vertices[1], true).asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isEqualTo(1);

    final Result after = check("CHECK DATABASE");
    assertThat(after.<Long>getProperty("unreachableEdgeRecords")).isZero();
    assertThat(after.<Long>getProperty("edgesMissingInReference")).isZero();
  }

  /**
   * The relink plan is bounded (one FIX run relinks at most a budget of orphans, so an interrupted load of tens of millions
   * of edges cannot exhaust the heap): what a run leaves is reported, and the next run relinks it.
   */
  @Test
  void aRunOverTheBudgetLeavesTheRestForTheNextRun() {
    database.getSchema().createVertexType("V");
    database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();

    final GraphBatch batch = database.batch().build();
    final RID[] vertices = batch.createVertices("V", 10);
    for (int i = 0; i < 10; i++)
      batch.newEdge(vertices[i], "E", vertices[(i + 1) % 10], "eid", i);
    batch.flush();
    reopenDatabase();

    final long orphans = check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords");
    assertThat(orphans).as("the abandoned batch must leave more orphans than the budget, or this test proves nothing")
        .isGreaterThan(4L);

    GraphDatabaseChecker.orphanRelinkBudgetForTesting = 4;
    try {
      final Result first = check("CHECK DATABASE FIX");
      assertThat(first.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(4L);
      assertThat(first.<Long>getProperty("orphanEdgeRecordsLeftToRelink")).isEqualTo(orphans - 4);

      long left = orphans - 4;
      while (left > 0) {
        final Result next = check("CHECK DATABASE FIX");
        assertThat(next.<Long>getProperty("relinkedOrphanEdgeRecords")).isEqualTo(Math.min(4L, left));
        left -= next.<Long>getProperty("relinkedOrphanEdgeRecords");
        assertThat(next.<Long>getProperty("orphanEdgeRecordsLeftToRelink")).isEqualTo(left);
      }
    } finally {
      GraphDatabaseChecker.orphanRelinkBudgetForTesting = 0;
    }

    assertThat(check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords")).isZero();
    assertThat(distinctEids("SELECT expand(outE('E')) FROM V")).isEqualTo(10);
    assertThat(distinctEids("SELECT expand(inE('E')) FROM V")).isEqualTo(10);
  }

  /** {@code DELETE ORPHANS} still wins: the operator who asked for the records to go gets them removed, not relinked. */
  @Test
  void fixDeleteOrphansStillRemovesThem() {
    database.getSchema().createVertexType("V");
    database.getSchema().buildEdgeType().withName("E").withBidirectional(true).create();

    final GraphBatch batch = database.batch().build();
    final RID[] vertices = batch.createVertices("V", 10);
    for (int i = 0; i < 10; i++)
      batch.newEdge(vertices[i], "E", vertices[(i + 1) % 10]);
    batch.flush();
    reopenDatabase();

    final long orphans = check("CHECK DATABASE").<Long>getProperty("unreachableEdgeRecords");
    assertThat(orphans).as("the abandoned batch must leave orphan edge records, or this test proves nothing").isPositive();

    final Result fix = check("CHECK DATABASE FIX DELETE ORPHANS");
    assertThat(fix.<Long>getProperty("relinkedOrphanEdgeRecords")).isZero();
    assertThat(fix.<Long>getProperty("removedRecords")).isEqualTo(orphans);
    assertThat(count("SELECT count(@rid) AS c FROM E")).isEqualTo(10 - orphans);
  }

  private Result check(final String command) {
    try (final ResultSet rs = database.command("sql", command)) {
      return rs.next();
    }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long distinctEids(final String sql) {
    final Set<Integer> eids = new HashSet<>();
    long rows = 0;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        eids.add(rs.next().<Integer>getProperty("eid"));
        rows++;
      }
    }
    assertThat(rows).as("no edge is reachable twice from the same side").isEqualTo(eids.size());
    return eids.size();
  }
}
