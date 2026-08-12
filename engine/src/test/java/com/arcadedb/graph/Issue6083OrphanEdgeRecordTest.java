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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6083 items 2 and 4.
 * <p>
 * A {@code GraphBatch} edge is written in two steps that do not share a transaction: the record is created in
 * PHASE 1 of {@link GraphBatch#flush()}, and PHASE 3 links it into its source vertex's edge list. Both flush
 * paths commit in between - the parallel one must (the record pages have to be released before the per-bucket
 * tasks touch them), the sequential one does whenever {@code commitEvery > 0}. A connect pass that dies
 * part-way therefore used to leave edge RECORDS behind that no vertex points at: {@code countType()} counted
 * them, no traversal reached them, and nothing ever reclaimed them.
 * <p>
 * The same two-step split made {@code getTotalEdgesCreated()} a lower bound rather than a count: it advanced a
 * whole flush at a time, so a load dying mid-flush reported zero for a flush that had in fact made some of its
 * edges durable.
 * <p>
 * These tests pin both: after a failed flush, {@code countType} must agree with what a traversal can reach, and
 * the counter must equal that same number.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6083OrphanEdgeRecordTest extends TestHelper {

  private static final String VERTEX_TYPE = "Issue6083Node";
  private static final String EDGE_TYPE   = "Issue6083Link";

  /**
   * A source bucket id no bucket in this database will ever be assigned. The connect pass loads the source
   * vertex to find its edge-list head, so an edge from here fails PHASE 3 while its own record has already been
   * created - the exact shape the issue reports. Deliberately high so the counting sort in
   * {@code partitionBySourceBucket} orders it last, after the groups that succeed.
   */
  private static final int MISSING_BUCKET_ID = 31_212;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE).createProperty("tag", Type.STRING);
    });
  }

  /**
   * Parallel flush (the default): the edge records are committed before the per-bucket connect tasks run, so a
   * bucket whose task fails leaves every one of its records unconnected.
   */
  @Test
  void aFailedParallelFlushLeavesNoUnreachableEdgeRecord() {
    assertFailedFlushLeavesNoOrphan(true, 0);
  }

  /**
   * Sequential flush with {@code commitEvery} small enough that the pass commits before reaching the bad group:
   * that first commit makes PHASE 1's records durable, so the groups after it are left unconnected.
   */
  @Test
  void aFailedSequentialFlushLeavesNoUnreachableEdgeRecord() {
    assertFailedFlushLeavesNoOrphan(false, 1);
  }

  private void assertFailedFlushLeavesNoOrphan(final boolean parallelFlush, final int commitEvery) {
    final RID[] vertices = createVertices(4);

    // Not try-with-resources: the counters have to be read AFTER close() has run the final flush that fails, and
    // close() rethrows that failure.
    final GraphBatch batch = GraphBatch.builder(database)
        .withLightEdges(false)
        .withParallelFlush(parallelFlush)
        .withCommitEvery(commitEvery)
        .build();

    // Two edges that can be connected, from two DIFFERENT source vertices so the parallel path has more than one
    // group to schedule, and one that cannot: its source names a bucket that does not exist.
    batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good-0");
    batch.newEdge(vertices[1], EDGE_TYPE, vertices[2], "tag", "good-1");
    batch.newEdge(new RID(MISSING_BUCKET_ID, 0L), EDGE_TYPE, vertices[3], "tag", "doomed");

    assertThatThrownBy(batch::close).as("an edge whose source bucket does not exist must fail the load")
        .isInstanceOf(RuntimeException.class);

    final long counted = batch.getTotalEdgesCreated();
    final long reclaimed = batch.getOrphanEdgeRecordsReclaimed();

    final long stored = countType(EDGE_TYPE);
    final long reachable = reachableEdgeCount();

    // The invariant the issue is about. Before the fix the doomed edge's record survived as invisible garbage,
    // so countType() answered 3 (or, on the sequential path, 1 more than the traversal) while the traversal
    // reached fewer. Asserting equality rather than an upper bound is what makes this a real check: an
    // implementation that simply stopped creating the records would also have to keep the good edges.
    assertThat(stored).as("every stored edge record must be reachable from a vertex").isEqualTo(reachable);

    // Precondition: the failure really did happen after some edges were durably connected. Without this the
    // assertion above passes vacuously on a load where nothing at all was written.
    assertThat(reachable).as("the flush must have connected some edges before it failed").isPositive();
    assertThat(reclaimed).as("the unconnected record must have been reclaimed, not simply never created")
        .isPositive();

    // Issue #6083 item 4: the counter must be the number of edges the graph holds, not a lower bound.
    assertThat(counted).as("getTotalEdgesCreated() must be exact after a partial flush").isEqualTo(reachable);
  }

  /**
   * The parallel path reclaims per FAILED BUCKET, so more than one failing at once exercises the loop rather
   * than a single iteration of it. Each bucket is an independent async transaction: one failing must not take a
   * sibling's committed edges with it, and both failures must be reclaimed.
   */
  @Test
  void aParallelFlushReclaimsEveryFailedBucketNotJustTheFirst() {
    final RID[] vertices = createVertices(6);

    final GraphBatch batch = GraphBatch.builder(database)
        .withLightEdges(false)
        .withParallelFlush(true)
        .build();

    batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good-0");
    batch.newEdge(vertices[1], EDGE_TYPE, vertices[2], "tag", "good-1");
    // Two DISTINCT missing source buckets, so the partition puts them in two separate bucket tasks and two
    // separate reclaim calls - one failing bucket would only prove the loop runs once.
    batch.newEdge(new RID(MISSING_BUCKET_ID, 0L), EDGE_TYPE, vertices[3], "tag", "doomed-a");
    batch.newEdge(new RID(MISSING_BUCKET_ID + 7, 0L), EDGE_TYPE, vertices[4], "tag", "doomed-b");

    assertThatThrownBy(batch::close).isInstanceOf(RuntimeException.class);

    final long reachable = reachableEdgeCount();

    assertThat(batch.getOrphanEdgeRecordsReclaimed())
        .as("both failed buckets must be reclaimed, not just the first one the loop reaches").isEqualTo(2);
    assertThat(batch.getOrphanEdgeRecordsLeaked()).isZero();
    assertThat(countType(EDGE_TYPE)).as("no unreachable edge record may survive either failure")
        .isEqualTo(reachable);
    assertThat(reachable).as("the buckets that succeeded keep their edges").isEqualTo(2);
    assertThat(batch.getTotalEdgesCreated()).as("and only those are claimed").isEqualTo(reachable);
  }

  /**
   * A batch whose FIRST commit is the one that fails has nothing durable to reclaim: the rollback already
   * removed PHASE 1's records. The cleanup must not try to delete them again, and the counter must say zero.
   */
  @Test
  void aFlushThatFailsBeforeItsFirstCommitReclaimsNothing() {
    final RID[] vertices = createVertices(2);

    final GraphBatch batch = GraphBatch.builder(database)
        .withLightEdges(false)
        .withParallelFlush(false)
        .withCommitEvery(0)
        .build();

    batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good");
    batch.newEdge(new RID(MISSING_BUCKET_ID, 0L), EDGE_TYPE, vertices[1], "tag", "doomed");

    assertThatThrownBy(batch::close).isInstanceOf(RuntimeException.class);

    assertThat(countType(EDGE_TYPE)).as("a single-commit flush rolls back whole, so no edge record survives")
        .isZero();
    assertThat(reachableEdgeCount()).isZero();
    assertThat(batch.getTotalEdgesCreated()).as("nothing was made durable, so nothing may be claimed").isZero();
    assertThat(batch.getOrphanEdgeRecordsReclaimed())
        .as("the rollback already removed the records; the cleanup must find nothing to do").isZero();
    assertThat(batch.getOrphanEdgeRecordsLeaked()).as("and it must not fail trying").isZero();
  }

  /**
   * The doubly-unlucky case: the flush fails, AND the cleanup after it fails partway through its own commits.
   * <p>
   * With {@code commitEvery > 0} the reclaim pass commits several times. Folding its counters into the totals
   * only once, at the end, threw away the reclaims an earlier commit had already made durable when a later one
   * threw, and the outer handler then blamed the whole range as leaked - under-reporting reclaims and counting
   * records that were in fact already gone. Both counters must instead describe what the database really holds.
   */
  @Test
  void aReclaimPassThatFailsPartwayKeepsWhatItAlreadyCommitted() {
    final RID[] vertices = createVertices(6);

    // Fail the SECOND reclaim commit: the first has then already durably deleted one orphan.
    GraphBatch.TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK = attempt -> {
      if (attempt == 2)
        throw new IllegalStateException("Issue6083: simulated failure inside the orphan reclaim pass");
    };

    final GraphBatch batch;
    try {
      batch = GraphBatch.builder(database)
          .withLightEdges(false)
          .withParallelFlush(false)
          // One record per reclaim transaction, so the pass needs several commits and the hook can fail a later
          // one. It also makes the flush itself commit per group, so the good edges land durably first.
          .withCommitEvery(1)
          .build();

      batch.newEdge(vertices[0], EDGE_TYPE, vertices[1], "tag", "good");
      // Three doomed edges from the same missing bucket: all three are orphan candidates, so the reclaim pass
      // has more than one commit to make and the injected failure lands in the middle of it.
      for (int i = 0; i < 3; i++)
        batch.newEdge(new RID(MISSING_BUCKET_ID, i), EDGE_TYPE, vertices[2 + i], "tag", "doomed" + i);

      assertThatThrownBy(batch::close).isInstanceOf(RuntimeException.class);
    } finally {
      GraphBatch.TEST_BEFORE_ORPHAN_RECLAIM_COMMIT_HOOK = null;
    }

    final long reclaimed = batch.getOrphanEdgeRecordsReclaimed();
    final long leaked = batch.getOrphanEdgeRecordsLeaked();
    final long stored = countType(EDGE_TYPE);
    final long reachable = reachableEdgeCount();

    // Precondition: the injected failure really did interrupt the pass after it had committed something.
    assertThat(reclaimed).as("the first reclaim commit succeeded, so its record must stay counted as reclaimed")
        .isPositive();
    assertThat(leaked).as("the interrupted slice is what leaked").isPositive();

    // The accounting must describe the database: every orphan is either reclaimed or still stored.
    assertThat(stored - reachable).as("the records still connected to nothing are exactly the leaked ones")
        .isEqualTo(leaked);
    assertThat(reclaimed + leaked).as("every orphan candidate is accounted for exactly once, never twice")
        .isEqualTo(3);

    // This test deliberately leaves unreclaimed orphans behind - that is the state it exists to measure - and
    // they trip the integrity check TestHelper runs after every test. Not because they are orphans (CHECK
    // DATABASE has no finding for that, which is the whole premise of item 2) but because the bogus source
    // bucket used to induce the failure makes them dangling links. Remove them so the shared teardown sees a
    // clean database.
    database.transaction(() -> database.command("sql", "DELETE FROM " + EDGE_TYPE + " WHERE tag LIKE 'doomed%'"));
    assertThat(countType(EDGE_TYPE)).as("only the good edge is left after the cleanup").isEqualTo(reachable);
  }

  /**
   * The counter stays exact across a successful load too - the change for item 4 moved where it is incremented,
   * so this pins that the ordinary path still counts every edge exactly once.
   */
  @Test
  void aSuccessfulLoadStillCountsEveryEdgeExactlyOnce() {
    final RID[] vertices = createVertices(50);

    final long counted;
    try (final GraphBatch batch = GraphBatch.builder(database)
        .withLightEdges(false)
        // Forces several flushes, and several internal commits inside each of them.
        .withBatchSize(7)
        .withCommitEvery(3)
        .withParallelFlush(false)
        .build()) {
      for (int i = 0; i < vertices.length - 1; i++)
        batch.newEdge(vertices[i], EDGE_TYPE, vertices[i + 1], "tag", "e" + i);
      counted = batch.getTotalEdgesCreated();
    }

    assertThat(counted).isEqualTo(vertices.length - 1);
    assertThat(countType(EDGE_TYPE)).isEqualTo(vertices.length - 1);
    assertThat(reachableEdgeCount()).isEqualTo(vertices.length - 1);
  }

  private RID[] createVertices(final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids[i] = database.newVertex(VERTEX_TYPE).set("id", i).save().getIdentity();
    });
    return rids;
  }

  private long countType(final String typeName) {
    final long[] count = new long[1];
    database.transaction(() -> count[0] = database.countType(typeName, true));
    return count[0];
  }

  /** Edges an OUT traversal from the vertices actually reaches - the ones the graph really holds. */
  private long reachableEdgeCount() {
    final long[] reachable = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + VERTEX_TYPE)) {
        rs.forEachRemaining(r -> reachable[0] += r.getVertex().get().countEdges(Vertex.DIRECTION.OUT, EDGE_TYPE));
      }
    });
    return reachable[0];
  }
}
