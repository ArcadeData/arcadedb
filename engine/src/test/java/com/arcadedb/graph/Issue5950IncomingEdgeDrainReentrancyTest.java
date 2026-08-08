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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the PR #5950 review cycle 3 finding: {@link GraphBatch#flush()}'s early
 * incoming-edge drain (issue #5664) calls {@link GraphBatch#connectDeferredIncomingEdges()}
 * directly. That method commits the deferred incoming-edge buffer in slices - the sequential path
 * every {@code commitEvery} edges, the parallel path one commit per destination-bucket task - but
 * only resets {@code inEdgeCount} and the buffer arrays AFTER the whole method returns
 * successfully.
 * <p>
 * If a slice fails partway through (a {@code NeedRetryException} that exhausts retries, a
 * unique-index violation, disk-full, etc.), earlier slices are already durably committed, but the
 * buffer is left holding the FULL original set, including the already-committed entries.
 * {@link GraphBatch#flush()}'s catch block resets only the OUTGOING buffer, not the incoming one.
 * The class's own documented {@code try (GraphBatch batch = ...) { ... }} usage then triggers
 * {@link GraphBatch#close()} on unwind, which unconditionally re-runs
 * {@code connectDeferredIncomingEdges()} over the SAME stale buffer - reprocessing (and duplicating)
 * the slices that already committed in the first, failed attempt.
 * <p>
 * The fix tracks a resume cursor ({@code inEdgesResumeSortIndex}) for the sequential path and a set
 * of completed destination buckets ({@code completedIncomingBuckets}) for the parallel path, so a
 * retry only reprocesses what genuinely did not complete.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5950IncomingEdgeDrainReentrancyTest extends TestHelper {

  private static final String SRC_TYPE   = "Issue5950Src";
  private static final String DST_A_TYPE = "Issue5950DstA";
  private static final String DST_B_TYPE = "Issue5950DstB";
  private static final String DST_C_TYPE = "Issue5950DstC";
  private static final String EDGE_TYPE  = "ISSUE5950_KNOWS";

  @Override
  protected void beginTest() {
    // Creation order fixes the bucket ids, which the counting sort partitions by: DST_A < DST_B < DST_C.
    database.transaction(() -> {
      database.getSchema().createVertexType(SRC_TYPE);
      database.getSchema().createVertexType(DST_A_TYPE);
      database.getSchema().createVertexType(DST_B_TYPE);
      database.getSchema().createVertexType(DST_C_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });
  }

  @AfterEach
  void clearHook() {
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = null;
  }

  /**
   * Sequential path: 10 destination vertices (10 single-edge groups), {@code commitEvery=2} so the
   * drain commits in 5 internal slices plus a final commit. The hook throws exactly once, on the
   * 3rd commit (after 2 slices / 4 groups already durably committed), simulating the drain failing
   * partway through. The resulting exception unwinds through flush()/close() exactly like
   * try-with-resources would. Without the fix, close()'s retry reprocesses all 10 groups from
   * scratch, duplicating the 4 that already committed.
   */
  @Test
  void sequentialRetryDoesNotDuplicateAlreadyCommittedGroups() {
    final int destCount = 10;

    final RID[] dstRIDs = createVertices(DST_A_TYPE, destCount);
    final RID[] srcRIDs = createVertices(SRC_TYPE, destCount);

    final AtomicInteger calls = new AtomicInteger();
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
      calls.incrementAndGet();
      if (attempt == 3)
        throw new RuntimeException("simulated commit failure partway through the incoming-edge drain");
    };

    final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(1_000_000)
        .withCommitEvery(2)
        .withMaxDeferredIncomingEdges(destCount) // force the early drain once all 10 edges are buffered
        .withParallelFlush(false)
        .build();

    RuntimeException flushFailure = null;
    try {
      for (int i = 0; i < destCount; i++)
        importer.newEdge(srcRIDs[i], EDGE_TYPE, dstRIDs[i]);

      importer.flush();
    } catch (final RuntimeException e) {
      flushFailure = e;
    } finally {
      importer.close();
    }

    assertThat(flushFailure)
        .as("the injected fault must have surfaced from flush(), proving the early drain actually failed partway")
        .isNotNull();
    assertThat(calls.get())
        .as("at least the failing attempt plus subsequent retry commits must have run")
        .isGreaterThanOrEqualTo(3);

    assertEachDestinationHasExactlyOneIncomingEdge(dstRIDs);
  }

  /**
   * Parallel path: 2 distinct destination buckets (DST_A, DST_B), each with its own destination
   * vertex, so {@code connectIncomingEdgesParallel} schedules exactly 2 bucket tasks.
   * {@code database.async().setParallelLevel(1)} pins both tasks to the same slot so they execute
   * strictly sequentially, making the fault injection deterministic: the hook lets the first bucket
   * task's commit succeed and fails the second's. That failure unwinds through flush()/close();
   * close()'s retry must skip the already-committed bucket and only reprocess the failed one.
   */
  @Test
  void parallelRetryDoesNotDuplicateAlreadyCommittedBuckets() {
    final RID[] dstARIDs = createVertices(DST_A_TYPE, 3);
    final RID[] dstBRIDs = createVertices(DST_B_TYPE, 3);
    final RID[] srcRIDs  = createVertices(SRC_TYPE, 6);

    // Not restored afterwards, and it cannot leak: parallelLevel is per-DatabaseAsyncExecutor, TestHelper
    // builds a fresh database (hence a fresh executor) in its constructor for every test method, and drops
    // it in @AfterEach. Restoring here would only churn the thread pool of a database about to be dropped.
    database.async().setParallelLevel(1);

    final AtomicInteger calls = new AtomicInteger();
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
      calls.incrementAndGet();
      if (attempt == 2)
        throw new RuntimeException("simulated commit failure on the second bucket task");
    };

    final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(1_000_000)
        .withMaxDeferredIncomingEdges(6) // force the early drain once all 6 edges are buffered
        .withParallelFlush(true)
        .build();

    RuntimeException flushFailure = null;
    try {
      for (int i = 0; i < 3; i++)
        importer.newEdge(srcRIDs[i], EDGE_TYPE, dstARIDs[i]);
      for (int i = 0; i < 3; i++)
        importer.newEdge(srcRIDs[3 + i], EDGE_TYPE, dstBRIDs[i]);

      importer.flush();
    } catch (final RuntimeException e) {
      flushFailure = e;
    } finally {
      importer.close();
    }

    assertThat(flushFailure)
        .as("the injected fault must have surfaced from flush(), proving the early drain actually failed partway")
        .isNotNull();
    assertThat(calls.get())
        .as("one bucket succeeds, one fails, then the failed one is retried")
        .isGreaterThanOrEqualTo(2);

    final RID[] allDst = new RID[6];
    System.arraycopy(dstARIDs, 0, allDst, 0, 3);
    System.arraycopy(dstBRIDs, 0, allDst, 3, 3);
    assertEachDestinationHasExactlyOneIncomingEdge(allDst);
  }

  /**
   * Review cycle 4: a caller that catches the exception from a failed early drain and keeps streaming
   * ({@code newEdge()} again) grows the deferred buffer between the failed attempt and the retry. Because
   * the sort index is a counting sort over the buffer, that changes what a given raw index - and a given
   * bucket range - refers to, so applying the recorded resume state to the rebuilt index silently skips
   * groups (edge loss) or reprocesses them (duplicates). Nothing in the class forbids this call sequence.
   * <p>
   * The fix pins the row count each drain pass covers, so a retry rebuilds an identically-shaped index
   * over the same pinned prefix, and rows appended past the pin are drained as a separate pass.
   */
  @Test
  void newEdgesAfterAFailedDrainAreNotLostOrDuplicatedSequential() {
    assertStreamingContinuesAfterFailedDrain(false);
  }

  /** Same scenario on the parallel flush path, where the resume state is a set of bucket ids. */
  @Test
  void newEdgesAfterAFailedDrainAreNotLostOrDuplicatedParallel() {
    assertStreamingContinuesAfterFailedDrain(true);
  }

  private void assertStreamingContinuesAfterFailedDrain(final boolean parallelFlush) {
    // Bucket ids follow schema creation order, so DST_A < DST_B < DST_C. The waves are laid out to
    // perturb the rebuilt sort index in BOTH ways the resume state is vulnerable to:
    //   - wave 2 writes to DST_A, a bucket that sorts BEFORE everything wave 1 touched, which shifts the
    //     positions the sequential path's raw resume cursor points at;
    //   - wave 2 also writes to DST_B, the bucket that committed successfully during wave 1, so the
    //     parallel path's "skip completed buckets" would drop those new rows entirely.
    // Wave 1 spans two buckets (B and C) so the parallel path schedules two commits and the fault can land
    // on the second one, leaving the first durably committed.
    final RID[] dstA = createVertices(DST_A_TYPE, 2);
    final RID[] dstB = createVertices(DST_B_TYPE, 5); // 3 in wave 1, 2 in wave 2
    final RID[] dstC = createVertices(DST_C_TYPE, 3);
    final RID[] src  = createVertices(SRC_TYPE, 10);

    if (parallelFlush)
      database.async().setParallelLevel(1); // deterministic bucket-task ordering: ascending bucket id

    final AtomicInteger calls = new AtomicInteger();
    GraphBatch.TEST_BEFORE_INCOMING_EDGE_COMMIT_HOOK = attempt -> {
      calls.incrementAndGet();
      // Fail once, partway through the FIRST drain, leaving an already-committed prefix behind.
      if (attempt == 2)
        throw new RuntimeException("simulated commit failure partway through the first early drain");
    };

    final GraphBatch importer = GraphBatch.builder(database)
        .withBatchSize(1_000_000)
        .withCommitEvery(2)
        .withMaxDeferredIncomingEdges(6) // early drain fires once wave 1 (6 edges) is buffered
        .withParallelFlush(parallelFlush)
        .build();

    RuntimeException firstFailure = null;
    try {
      // Wave 1: 3 edges into bucket B, 3 into bucket C.
      for (int i = 0; i < 3; i++)
        importer.newEdge(src[i], EDGE_TYPE, dstB[i]);
      for (int i = 0; i < 3; i++)
        importer.newEdge(src[3 + i], EDGE_TYPE, dstC[i]);

      try {
        importer.flush();
      } catch (final RuntimeException e) {
        firstFailure = e;
      }

      assertThat(firstFailure)
          .as("the injected fault must have surfaced, leaving a partially-drained buffer behind")
          .isNotNull();
      assertThat(importer.getDeferredIncomingEdgeCount())
          .as("the failed drain must leave the deferred buffer intact for the retry")
          .isEqualTo(6);

      // The caller absorbs the transient failure and KEEPS STREAMING - the sequence the resume state used
      // to silently assume could not happen. These rows are appended onto the still-undrained buffer.
      for (int i = 0; i < 2; i++)
        importer.newEdge(src[6 + i], EDGE_TYPE, dstA[i]);       // bucket BELOW wave 1's buckets
      for (int i = 0; i < 2; i++)
        importer.newEdge(src[8 + i], EDGE_TYPE, dstB[3 + i]);   // bucket already marked complete
    } finally {
      importer.close();
    }

    assertThat(calls.get()).isGreaterThanOrEqualTo(2);

    // Every destination from BOTH waves must end up with exactly one incoming edge: none lost by resuming
    // past the wrong offset or skipping a bucket that gained rows, none duplicated by reprocessing an
    // already-committed group.
    final RID[] allDst = new RID[10];
    System.arraycopy(dstA, 0, allDst, 0, 2);
    System.arraycopy(dstB, 0, allDst, 2, 5);
    System.arraycopy(dstC, 0, allDst, 7, 3);
    assertEachDestinationHasExactlyOneIncomingEdge(allDst);
  }

  private RID[] createVertices(final String type, final int count) {
    final RID[] rids = new RID[count];
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final MutableVertex v = database.newVertex(type);
        v.set("id", i);
        v.save();
        rids[i] = v.getIdentity();
      }
    });
    return rids;
  }

  private void assertEachDestinationHasExactlyOneIncomingEdge(final RID[] dstRIDs) {
    database.transaction(() -> {
      for (final RID dstRID : dstRIDs) {
        final Vertex dstVertex = dstRID.asVertex();
        int inCount = 0;
        for (final Edge ignored : dstVertex.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
          inCount++;
        assertThat(inCount)
            .as("destination vertex %s must have exactly one incoming edge, not duplicated or missing", dstRID)
            .isEqualTo(1);
      }
    });
  }
}
