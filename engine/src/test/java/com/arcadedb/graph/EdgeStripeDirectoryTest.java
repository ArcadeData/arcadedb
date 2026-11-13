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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.Pair;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the EdgeStripeDirectory area, one nested class per former stand-alone regression class.
 */
class EdgeStripeDirectoryTest {
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
  @Nested
  class IncomingEdgeDrainReentrancy extends TestHelper {

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

  /**
   * Regression test for the PR #5950 review cycle 4 finding on the OUTGOING edge direction.
   * <p>
   * {@code connectOutgoingEdgesParallel} schedules each source bucket as {@code async.transaction(..., 3, ...)},
   * and {@code DatabaseAsyncTransaction.execute()} retries the <b>same lambda</b> up to three times on a
   * {@link ConcurrentModificationException}, rolling back and re-invoking {@code connectOutEdgesRangeLocal}
   * over the same range. Page-level CME is a normal, expected occurrence on this exact code path (two edges
   * appended into the same edge-list page), which is precisely why the async layer retries it transparently.
   * <p>
   * {@code getOrCreateOutEdgeChunk()} used to persist a brand-new segment AND immediately write its RID into
   * the class-level {@code outChunkRIDCache} (plus flip the vertex head pointer) before the surrounding
   * transaction committed. That cache write survived the rollback, so the retry read the cache first and
   * dereferenced a RID belonging to a transaction that had been rolled back - turning a transparently
   * retryable transient conflict into a hard {@code RecordNotFoundException} batch failure.
   * <p>
   * The fix routes new-segment creation through the per-bucket local maps that are merged into the shared
   * caches only after the bucket's commit succeeds, mirroring what {@code connectIncomingEdgesRangeLocal}
   * already does for the IN direction.
   * <p>
   * The source vertices here are created with plain {@code database.newVertex()} rather than
   * {@code GraphBatch.createVertices()} on purpose: only then are they absent from
   * {@code knownNewVertexKeys}, which is what routes them into the vertex-loading fallback branch that
   * called {@code getOrCreateOutEdgeChunk()}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class OutgoingEdgeRetryCache extends TestHelper {

    private static final String VERTEX_TYPE = "Issue5950RetrySrc";
    private static final String EDGE_TYPE   = "ISSUE5950_RETRY_KNOWS";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().createEdgeType(EDGE_TYPE);
      });
    }

    @AfterEach
    void clearHook() {
      GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = null;
    }

    /**
     * A CME on the first attempt of an outgoing-edge bucket task must be absorbed by the async layer's own
     * retry and produce a complete, correct graph - not a {@code RecordNotFoundException} from the retry
     * reading a rolled-back segment RID out of the shared head-chunk cache.
     */
    @Test
    void concurrentModificationRetryDoesNotReadRolledBackSegmentRID() {
      final int vertices = 8;

      // Plain newVertex(), NOT GraphBatch.createVertices(): keeps these out of knownNewVertexKeys so the
      // edge connect takes the vertex-loading fallback branch this fix changed.
      final RID[] rids = new RID[vertices];
      database.transaction(() -> {
        for (int i = 0; i < vertices; i++) {
          final MutableVertex v = database.newVertex(VERTEX_TYPE);
          v.set("id", i);
          v.save();
          rids[i] = v.getIdentity();
        }
      });

      final AtomicInteger calls = new AtomicInteger();
      GraphBatch.TEST_BEFORE_OUTGOING_EDGE_COMMIT_HOOK = attempt -> {
        calls.incrementAndGet();
        // Fail the first bucket attempt only; the async executor's CME retry must then succeed.
        if (attempt == 1)
          throw new ConcurrentModificationException("simulated page conflict on the first attempt");
      };

      try (final GraphBatch importer = GraphBatch.builder(database)
          .withBatchSize(1_000_000)
          .withParallelFlush(true)
          .build()) {
        // A chain, so every vertex is a source exactly once.
        for (int i = 0; i < vertices - 1; i++)
          importer.newEdge(rids[i], EDGE_TYPE, rids[i + 1]);
      }

      assertThat(calls.get())
          .as("the injected CME plus at least one retry attempt must have run")
          .isGreaterThanOrEqualTo(2);

      // Every edge must be present exactly once in both directions: the rolled-back attempt must have left
      // nothing behind, and the retry must have recreated the segment cleanly.
      database.transaction(() -> {
        int totalOut = 0;
        int totalIn = 0;
        for (int i = 0; i < vertices; i++) {
          final Vertex v = rids[i].asVertex();
          for (final Edge ignored : v.getEdges(Vertex.DIRECTION.OUT, EDGE_TYPE))
            totalOut++;
          for (final Edge ignored : v.getEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
            totalIn++;
        }
        assertThat(totalOut).as("every outgoing edge exactly once after the CME retry").isEqualTo(vertices - 1);
        assertThat(totalIn).as("every incoming edge exactly once after the CME retry").isEqualTo(vertices - 1);
      });
    }
  }

  /**
   * #6044: super-node promotion (#5156) must not silently turn the vertex's edge iteration order into an
   * arbitrary one. The classic layout iterates exactly newest-first; the striped layout composes one chain per
   * stripe and used to CONCATENATE them, so the whole of stripe 0 came before the whole of stripe 1 and the
   * global order became a function of {@code hash(neighbour RID)} - with an error proportional to the vertex
   * DEGREE, i.e. worst on exactly the vertices promotion targets.
   * <p>
   * The chains are now INTERLEAVED (round-robin, one entry per chain per turn) within each generation, which
   * reconstructs an approximate newest-first order whose error is proportional to the RANK asked for rather
   * than to the degree. These tests pin the two properties applications actually rely on:
   * <ol>
   * <li>the newest edge is inside the first {@code stripes} entries;</li>
   * <li>an edge of global rank {@code r} shows up at a position bounded by a small multiple of {@code r}.</li>
   * </ol>
   * Generations stay CONCATENATED newest-generation-first: generation 0 is the pre-promotion chain and holds
   * the OLDEST edges, so folding it into the rotation would put ancient entries in the first positions.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StripeIterationOrder extends TestHelper {
    private static final int STRIPES   = 16;
    private static final int THRESHOLD = 64;
    private static final int TOTAL     = 2_000;

    private int savedThreshold;
    private int savedStripes;

    @BeforeEach
    void saveConfig() {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
    }

    @Test
    void newestEdgesStayAtTheHeadOfAPromotedVertexIteration() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, TOTAL);

      // PRECONDITION: THE HUB MUST REALLY HAVE BEEN PROMOTED, OTHERWISE THE TEST PROVES NOTHING
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);

      // NOTHING LOST, NOTHING DUPLICATED
      assertThat(positionOf).hasSize(TOTAL);
      assertThat(positionOf.keySet()).containsAll(sources);

      // (a) THE NEWEST EDGE IS THE HEAD OF ITS OWN CHAIN, SO ROUND-ROBIN MUST EMIT IT WITHIN THE FIRST TURN
      final int newestPosition = positionOf.get(sources.getLast());
      assertThat(newestPosition).isLessThan(STRIPES);

      // (b) RANK-BOUNDED ERROR: every one of the newest 50 edges lands in the first 400 entries. Concatenation
      // scatters them over the whole list (one cluster per stripe, ~degree/stripes apart), so it cannot pass.
      //
      // The two bounds below are DETERMINISTIC, not sampled: the fixture inserts a fixed number of vertices in a
      // fixed order into a fresh database, so the neighbour RIDs - and therefore StripeDirectory.stripeOf's
      // placement of every edge - are the same on every run. They are stated loosely on purpose, well clear of the
      // measured values (worst-placed ~140, ~88 of the newest 100 in the first 200), so that a change in bucket
      // allocation that shifts the RIDs moves the numbers without failing the test; only a real regression in the
      // rank/position relationship can cross them. Concatenation misses by an order of magnitude (worst ~1762).
      int worstOfTheNewest = 0;
      for (int rank = 0; rank < 50; rank++)
        worstOfTheNewest = Math.max(worstOfTheNewest, positionOf.get(sources.get(TOTAL - 1 - rank)));
      assertThat(worstOfTheNewest).isLessThan(400);

      // AND MOST OF THE NEWEST 100 ARE STILL IN THE FIRST 200 - the "page the newest N" use case
      int newest100InFirst200 = 0;
      for (int rank = 0; rank < 100; rank++)
        if (positionOf.get(sources.get(TOTAL - 1 - rank)) < 200)
          newest100InFirst200++;
      assertThat(newest100InFirst200).isGreaterThanOrEqualTo(75);

      // (c) GENERATION 0 (THE PRE-PROMOTION CHAIN, THE OLDEST EDGES) STAYS AT THE TAIL: the rotation is
      // per-generation, so the oldest edges must NOT be interleaved into the first positions. Generation 0 is
      // one chain walked newest-first and it is walked last, hence the very oldest edge is dead last.
      assertThat(positionOf.get(sources.getFirst())).isEqualTo(TOTAL - 1);
    }

    /**
     * The same guarantee through the edge iterator (and after a reopen, so the whole thing is read back from
     * disk rather than from the transaction's record cache).
     */
    @Test
    void newestEdgeIsAtTheHeadOfTheEdgeIteratorAfterReopen() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, 600);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      reopenDatabase();

      final RID hub2 = new RID(hubRID.getBucketId(), hubRID.getPosition());
      final RID newestSrc = new RID(sources.getLast().getBucketId(), sources.getLast().getPosition());

      database.transaction(() -> {
        final Vertex hub = hub2.asVertex(true);

        int position = 0;
        int newestAt = -1;
        final Set<RID> seen = new HashSet<>();
        for (final Iterator<Edge> it = hub.getEdges(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); position++) {
          final RID out = it.next().getOut();
          assertThat(seen.add(out)).isTrue();
          if (out.equals(newestSrc))
            newestAt = position;
        }
        assertThat(seen).hasSize(600);
        assertThat(newestAt).isBetween(0, STRIPES - 1);
      });

      database.transaction(() -> hub2.asVertex(true).delete());
    }

    /** Interleaving must not change what a count sees, nor what the RID-only walk sees. */
    @Test
    void interleavingPreservesCountAndTheRidWalk() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);

      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, 500);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      database.transaction(() -> {
        final Vertex hub = hubRID.asVertex(true);
        assertThat(hub.countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(500);
        assertThat(hub.getVertices(Vertex.DIRECTION.IN, "LINK").size()).isEqualTo(500);

        final Set<RID> viaVertices = new HashSet<>();
        for (final Vertex v : hub.getVertices(Vertex.DIRECTION.IN, "LINK"))
          assertThat(viaVertices.add(v.getIdentity())).isTrue();
        assertThat(viaVertices).containsExactlyInAnyOrderElementsOf(sources);
      });

      database.transaction(() -> hubRID.asVertex(true).delete());
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 8);
        database.getSchema().createEdgeType("LINK", 8);
      });
    }

    private RID createHub() {
      final MutableVertex[] holder = new MutableVertex[1];
      database.transaction(() -> {
        holder[0] = database.newVertex("Hub");
        holder[0].save();
      });
      return holder[0].getIdentity();
    }

    /** Inserts {@code count} edges Src-&gt;Hub, one transaction each, returning the source RIDs in insertion order. */
    private List<RID> insertEdges(final RID hubRID, final int count) {
      final List<RID> sources = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final RID[] srcHolder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          src.newEdge("LINK", hubRID);
          srcHolder[0] = src.getIdentity();
        });
        sources.add(srcHolder[0]);
      }
      return sources;
    }

    private Record loadInHead(final RID hubRID) {
      final Record[] head = new Record[1];
      database.transaction(() -> {
        final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
        head[0] = database.lookupByRID(headRID, true);
      });
      return head[0];
    }

    /** Maps every incoming neighbour of the hub to the position it is returned at. */
    private Map<RID, Integer> positionsOfNeighbours(final RID hubRID) {
      final Map<RID, Integer> positions = new HashMap<>();
      database.transaction(() -> {
        int position = 0;
        for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator();
             it.hasNext(); position++)
          assertThat(positions.put(it.next().getIdentity(), position)).isNull();
      });
      return positions;
    }
  }

  /**
   * #6048: {@code StripedEdgeList}'s read walks (edgeIterator/vertexIterator/ridIterator) interleave the stripe
   * chains of a promoted super-node so the first entries are approximately newest-first (#6044). That interleaving
   * keeps one resident chunk page PER STRIPE and hops between files every {@code stripes} entries - a locality
   * cost worth paying only while a caller is actually paging (small LIMIT). A caller that reads the whole list -
   * an ordinary {@code MATCH (h)-[:LINK]->(x) RETURN x} with no LIMIT, a Gremlin {@code out()}, an export - was
   * paying that cost for the FULL degree even though it never consults the order.
   * <p>
   * {@link GlobalConfiguration#GRAPH_SUPERNODE_INTERLEAVE_ROUNDS} bounds it: past {@code rounds x stripes} entries
   * of a generation, the walk degrades to plain concatenation of what is left in each chain. These tests pin:
   * <ol>
   * <li>correctness first - nothing lost or duplicated by the degrade, at a threshold that lands well inside the
   *     walk, exactly on a stripe boundary, and past the whole list;</li>
   * <li>the ordering guarantee still holds for the prefix within the threshold;</li>
   * <li>a threshold of 0 reproduces the pre-#6044 concatenated order exactly, since a full walk with no rounds of
   *     interleaving is definitionally the classic layout's degree-dependent chain grouping;</li>
   * <li>the threshold does not leak across the generation boundary - the pre-promotion generation (walked last)
   *     is unaffected by however far the promoted generation's rotation degraded before it.</li>
   * </ol>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SupernodeFullWalkDegrade extends TestHelper {
    private static final int STRIPES   = 16;
    private static final int THRESHOLD = 64;
    private static final int TOTAL     = 2_000;

    private int savedThreshold;
    private int savedStripes;
    private int savedRounds;

    @BeforeEach
    void saveConfig() {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
      savedRounds = GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.getValueAsInteger();
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(savedRounds);
    }

    /** 3 rounds (mid-walk), exactly 1 round, and 0 (from the very first entry): every one still has to return the
     * complete, duplicate-free neighbour set. One fresh database per value, via TestHelper's per-test lifecycle. */
    @ParameterizedTest
    @ValueSource(ints = { 3, 1, 0 })
    void fullWalkLosesNothingWhicheverRoundTheDegradeFallsIn(final int rounds) {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(rounds);

      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, TOTAL);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      final Set<RID> seen = new HashSet<>();
      database.transaction(() -> {
        for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator(); it.hasNext(); )
          assertThat(seen.add(it.next().getIdentity())).as("rounds=%d must not duplicate an edge", rounds).isTrue();
      });
      assertThat(seen).as("rounds=%d must return every edge", rounds).hasSize(TOTAL).containsExactlyInAnyOrderElementsOf(sources);
    }

    @Test
    void orderedPrefixGuaranteeStillHoldsWithinTheDegradeThreshold() {
      // 8 rounds x 16 stripes = 128: comfortably larger than the #6044 test's own bound (worst-of-newest-50 < 400
      // positions), so the prefix this test checks never crosses into the degraded, order-agnostic tail.
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(8);

      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, TOTAL);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);
      assertThat(positionOf).hasSize(TOTAL);

      // THE NEWEST EDGE IS THE HEAD OF ITS OWN CHAIN: round-robin must emit it within the first turn.
      assertThat(positionOf.get(sources.getLast())).isLessThan(STRIPES);

      // RANK-BOUNDED ERROR for the newest 50, same bound as EdgeStripeDirectoryTest.StripeIterationOrder: the degrade
      // threshold (128) has not been reached yet at these positions.
      int worstOfTheNewest = 0;
      for (int rank = 0; rank < 50; rank++)
        worstOfTheNewest = Math.max(worstOfTheNewest, positionOf.get(sources.get(TOTAL - 1 - rank)));
      assertThat(worstOfTheNewest).isLessThan(400);
    }

    /**
     * A threshold of 0 must actually take effect, not be a no-op left over from a wiring mistake: it should
     * reproduce the exact failure signature #6044 fixed - the newest edges scattered by {@code hash(neighbour
     * RID)} instead of clustered near the front. Same fixture and same bound EdgeStripeDirectoryTest.StripeIterationOrder
     * uses for the INTERLEAVED case (worst-of-newest-50 &lt; 400 positions); here the assertion is inverted.
     */
    @Test
    void zeroRoundsReproducesTheClassicPreInterleavingScatter() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(0);

      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, TOTAL);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);
      assertThat(positionOf).hasSize(TOTAL);
      assertThat(positionOf.keySet()).containsAll(sources);

      int worstOfTheNewest = 0;
      for (int rank = 0; rank < 50; rank++)
        worstOfTheNewest = Math.max(worstOfTheNewest, positionOf.get(sources.get(TOTAL - 1 - rank)));
      assertThat(worstOfTheNewest).as("rounds=0 must disable interleaving, not merely shrink its window").isGreaterThan(1000);
    }

    /**
     * The degrade threshold is per generation ({@code interleaved()} builds a fresh {@code InterleavedIterator}
     * per generation, so {@code browsed} starts at 0 for each), not accumulated across the whole vertex. Generation
     * 0 - the pre-promotion classic chain, holding only the very first edge ever inserted here - is walked LAST
     * and, being a single chain, bypasses {@code InterleavedIterator} entirely (the "size==1" unwrap in
     * {@code interleaved()}): there is no degrade state to leak into it even in principle, but this pins the
     * observable consequence. With rounds=1 (16-entry threshold), generation 1's ~1,936 entries degrade to
     * concatenation almost immediately; the oldest edge must still land exactly at the tail, unaffected by
     * whatever generation 1's rotation did before it.
     */
    @Test
    void degradeThresholdDoesNotCarryOverToTheNextGeneration() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(1);

      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, TOTAL);
      assertThat(loadInHead(hubRID)).isInstanceOf(StripeDirectory.class);

      final Map<RID, Integer> positionOf = positionsOfNeighbours(hubRID);
      assertThat(positionOf).hasSize(TOTAL);

      // GENERATION 0 (pre-promotion, oldest edges) STAYS AT THE TAIL regardless of how generation 1 degraded.
      assertThat(positionOf.get(sources.getFirst())).isEqualTo(TOTAL - 1);

      // GENERATION 1's OWN NEWEST EDGE IS STILL WITHIN THE FIRST TURN: its rotation started fresh, not partway
      // through some carried-over count.
      assertThat(positionOf.get(sources.getLast())).isLessThan(STRIPES);
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 8);
        database.getSchema().createEdgeType("LINK", 8);
      });
    }

    private RID createHub() {
      final MutableVertex[] holder = new MutableVertex[1];
      database.transaction(() -> {
        holder[0] = database.newVertex("Hub");
        holder[0].save();
      });
      return holder[0].getIdentity();
    }

    private List<RID> insertEdges(final RID hubRID, final int count) {
      final List<RID> sources = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final RID[] srcHolder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          src.newEdge("LINK", hubRID);
          srcHolder[0] = src.getIdentity();
        });
        sources.add(srcHolder[0]);
      }
      return sources;
    }

    private Record loadInHead(final RID hubRID) {
      final Record[] head = new Record[1];
      database.transaction(() -> {
        final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
        head[0] = database.lookupByRID(headRID, true);
      });
      return head[0];
    }

    private Map<RID, Integer> positionsOfNeighbours(final RID hubRID) {
      final Map<RID, Integer> positions = new HashMap<>();
      database.transaction(() -> {
        int position = 0;
        for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator();
             it.hasNext(); position++)
          assertThat(positions.put(it.next().getIdentity(), position)).isNull();
      });
      return positions;
    }
  }

  /**
   * Regression tests for issue #6062: {@code CHECK DATABASE} paid an unconditional O(degree) linear walk of a
   * neighbour's adjacency list PER EDGE, so a super-node of degree D cost O(D²) and a real database was measured at
   * 80 hours.
   * <p>
   * Two independent probes produced that shape and both are covered here: {@code checkEdges} asked both endpoints of
   * every edge whether their list names the record ({@code EdgeLinkedList.containsEdge}), and {@code checkVertices}
   * asked the far vertex of every adjacency entry whether it points back ({@code isConnectedTo} →
   * {@code EdgeLinkedList.containsVertex}). Neither remembered anything between two probes of the SAME list, so a hub
   * referenced by D edges had its list walked D times.
   * <p>
   * The assertions are on counters the check now publishes, never on elapsed time: {@code adjacencyProbes} is how many
   * back-reference questions the run asked, {@code adjacencyProbeListWalks} how many of them had to touch an adjacency
   * list to answer, and {@code adjacencyEntriesScanned} how many list entries that cost in total. The fix makes the
   * second grow with the number of DISTINCT lists instead of with the number of edges, and the third linear in the size
   * of the graph instead of quadratic in the degree.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AdjacencyProbeCache extends TestHelper {
    private static final String VERTEX_TYPE = "Issue6062Node";
    private static final String EDGE_TYPE   = "Issue6062Link";
    private static final int    DEGREE      = 400;

    /**
     * One test deliberately leaves a half-linked edge behind - that is the state under measurement - and asserts the
     * check result itself. The blanket teardown run would only re-assert the same database with the opposite
     * expectation.
     */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().buildEdgeType().withName(EDGE_TYPE).withBidirectional(true).create();
      });
    }

    @AfterEach
    void restoreDefaults() {
      GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.reset();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.reset();
    }

    /**
     * The headline case, in its purest form: TWO vertices joined by many parallel edges, so every probe in the run
     * asks about one of exactly two adjacency lists. The number of lists the run has to walk must therefore stay in
     * the single digits however many edges there are - before the fix it was one walk per probe, which is one per
     * edge per endpoint.
     */
    @Test
    void theSameAdjacencyListIsNotRewalkedOncePerEdge() {
      createParallelEdgesBetweenTwoVertices();

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

      final long probes = (Long) result.get("adjacencyProbes");
      final long walks = (Long) result.get("adjacencyProbeListWalks");

      assertThat(probes).as("both passes probe both endpoints of every edge").isGreaterThanOrEqualTo(2L * DEGREE);
      assertThat(walks)
          .as("only two adjacency lists exist in this graph, so a run must not walk one per probe")
          .isLessThanOrEqualTo(16L);
    }

    /**
     * The control: with the cache disabled the run falls back to the pre-fix behaviour, and the counter the test
     * above reads goes back to one walk per probe. Without this a cache that never engaged - or a counter that
     * measured something else - would satisfy the assertion above on its own.
     */
    @Test
    void disablingTheCacheRestoresOneListWalkPerProbe() {
      createParallelEdgesBetweenTwoVertices();

      GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

      final long probes = (Long) result.get("adjacencyProbes");
      final long walks = (Long) result.get("adjacencyProbeListWalks");

      assertThat(probes).isGreaterThanOrEqualTo(2L * DEGREE);
      assertThat(walks).as("with the cache off every probe walks a list, which is the shape #6062 reports")
          .isEqualTo(probes);
    }

    /**
     * The super-node shape the issue actually describes: one hub with many DISTINCT neighbours. Here the number of
     * lists is proportional to the edge count, so the walk counter cannot discriminate - the cost is in how many
     * ENTRIES each walk visits, which is what makes a hub quadratic. Linear in the graph after the fix; before it,
     * every one of the {@code DEGREE} probes of the hub scanned ~{@code DEGREE/2} of its entries.
     */
    @Test
    void aHubIsScannedLinearlyRatherThanOncePerIncidentEdge() {
      createHubWithDistinctSpokes();

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

      final long entriesScanned = (Long) result.get("adjacencyEntriesScanned");

      assertThat(entriesScanned)
          .as("the hub's list must be materialised once per pass, not re-scanned per incident edge")
          .isGreaterThan(0L)
          .isLessThanOrEqualTo(10L * DEGREE);
      assertThat((Long) result.get("adjacencyProbeListWalks"))
          .as("and the probes of the hub after the first must be answered without touching its list at all")
          .isLessThan((Long) result.get("adjacencyProbes"));
    }

    /**
     * The cache must not change a single finding. A HALF-LINKED edge - present in its source's OUT list, missing
     * from its target's IN list - is the finding the back-reference probe exists to make, and it is reported
     * identically whether the probe reads a materialised list or walks the chain.
     */
    @Test
    void theCachedAndTheDirectProbeAgreeOnAHalfLinkedEdge() {
      final RID target = createHubWithDistinctSpokes();

      // Drop the hub's IN list: every spoke's edge is now missing from its target's IN list.
      database.transaction(() -> {
        final MutableVertex hub = target.asVertex(true).modify();
        hub.setInEdgesHeadChunk(null);
        hub.save();
      });

      final Map<String, Object> cached = new DatabaseChecker(database).setVerboseLevel(0).check();

      GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);
      final Map<String, Object> direct = new DatabaseChecker(database).setVerboseLevel(0).check();

      assertThat(cached.get("edgesMissingInReference")).isEqualTo((long) DEGREE);
      assertThat(cached.get("edgesMissingInReference")).isEqualTo(direct.get("edgesMissingInReference"));
      assertThat(cached.get("edgesMissingOutReference")).isEqualTo(direct.get("edgesMissingOutReference"));
      assertThat(cached.get("unreachableEdgeRecords")).isEqualTo(direct.get("unreachableEdgeRecords"));
      assertThat(cached.get("missingReferenceBack")).isEqualTo(direct.get("missingReferenceBack"));
      assertThat(cached.get("totalCorruptedRecords")).isEqualTo(direct.get("totalCorruptedRecords"));
      assertThat(warnings(cached)).containsExactlyInAnyOrderElementsOf(warnings(direct));
    }

    /**
     * The PROMOTED super-node, where getting the memoisation wrong is not merely slow but wrong. A neighbour-keyed
     * probe of a {@link StripedEdgeList} reads only the stripes that can hold that neighbour and is strict about a
     * stripe head it cannot resolve; an image built from the whole list would skip such a chain silently. The cache
     * therefore memoises per CHAIN and leaves the selection to
     * {@link StripedEdgeList#chainsForNeighbourProbe(RID)} - this test is what reaches that override, and it fails if
     * the two paths ever disagree about a healthy striped hub.
     */
    @Test
    void aPromotedSuperNodeIsProbedThroughItsOwnStripeSelection() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      final RID hub = createHubWithDistinctSpokes();

      // PRECONDITION: the hub really is striped, or the test proves nothing about the striped path.
      database.transaction(() -> {
        final RID head = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
        assertThat(database.lookupByRID(head, true)).isInstanceOf(StripeDirectory.class);
      });

      final Map<String, Object> cached = new DatabaseChecker(database).setVerboseLevel(0).check();

      GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);
      final Map<String, Object> direct = new DatabaseChecker(database).setVerboseLevel(0).check();

      assertThat(warnings(cached)).isEmpty();
      assertThat(warnings(cached)).containsExactlyInAnyOrderElementsOf(warnings(direct));
      assertThat(cached.get("edgesMissingInReference")).isEqualTo(direct.get("edgesMissingInReference"));
      assertThat(cached.get("edgesMissingOutReference")).isEqualTo(direct.get("edgesMissingOutReference"));
      assertThat(cached.get("missingReferenceBack")).isEqualTo(direct.get("missingReferenceBack"));
      assertThat((Long) cached.get("adjacencyProbeListWalks"))
          .as("the striped hub's chains must still be memoised, not re-walked per incident edge")
          .isLessThan((Long) direct.get("adjacencyProbeListWalks"));
    }

    /**
     * The FALLBACK path: a budget smaller than the hub's degree. That list cannot be represented at all, so it is
     * walked to the budget, abandoned, and answered by the original {@code EdgeLinkedList} probe - and the verdict is
     * remembered, so the abandoned walk happens once rather than once per probe. What must not change is a single
     * finding, which is what this asserts against both the fully cached and the fully disabled run.
     */
    @Test
    void aListLargerThanTheWholeBudgetFallsBackWithoutChangingAFinding() {
      createHubWithDistinctSpokes();

      final Map<String, Object> cached = new DatabaseChecker(database).setVerboseLevel(0).check();

      // Below the hub's degree, above a spoke's: the hub falls back, the spokes still cache.
      GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(10);
      final Map<String, Object> partial = new DatabaseChecker(database).setVerboseLevel(0).check();

      GlobalConfiguration.CHECK_DATABASE_ADJACENCY_CACHE_ENTRIES.setValue(0);
      final Map<String, Object> direct = new DatabaseChecker(database).setVerboseLevel(0).check();

      assertThat(warnings(partial)).isEmpty();
      assertThat(partial.get("edgesMissingInReference")).isEqualTo(cached.get("edgesMissingInReference"));
      assertThat(partial.get("edgesMissingOutReference")).isEqualTo(cached.get("edgesMissingOutReference"));
      assertThat(partial.get("missingReferenceBack")).isEqualTo(direct.get("missingReferenceBack"));
      assertThat(partial.get("unreachableEdgeRecords")).isEqualTo(direct.get("unreachableEdgeRecords"));
      assertThat(partial.get("totalCorruptedRecords")).isEqualTo(direct.get("totalCorruptedRecords"));

      // The verdict on the hub is REMEMBERED: its list is abandoned once, not once per incident edge, so the run
      // stays far below the "walk the hub per probe" volume the fully disabled run pays.
      assertThat((Long) partial.get("adjacencyEntriesScanned"))
          .as("the over-budget list must be walked to the budget once, not once per probe")
          .isLessThanOrEqualTo(5L * DEGREE);
    }

    /**
     * A CHUNK POINTING AT ITSELF must end the walk, not feed it forever.
     * <p>
     * {@code EdgeLinkedList.containsEdge}/{@code containsVertex} have always broken out of such a chain; the iterator
     * behind {@code entryIterator()} did not - and the checker walks a vertex's own edge list through it, so this
     * shape of corruption meant a {@code CHECK DATABASE} that never returned. This PR gives the same iterator a second
     * consumer (the probe cache materialises through it), which is why the guard belongs here now.
     * <p>
     * Asserted by walking under a HARD CAP rather than by running the check under a timeout: an unterminating walk
     * does not fail a test, it hangs the build, and a regression has to come back as a red test. Without the guard the
     * walk reaches the cap and the assertion fails in milliseconds.
     */
    @Test
    void aSelfReferencingChunkEndsTheWalkInsteadOfFeedingItForever() {
      final RID hub = createHubWithDistinctSpokes();

      // Point the hub's IN head chunk at itself.
      database.transaction(() -> {
        final RID head = ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
        final MutableEdgeSegment segment = (MutableEdgeSegment) database.lookupByRID(head, true);
        segment.setPrevious(segment);
        ((DatabaseInternal) database).updateRecord(segment);
      });

      final int cap = 8 * DEGREE;
      final int[] walked = new int[1];
      database.transaction(() -> {
        final EdgeLinkedList list = ((DatabaseInternal) database).getGraphEngine()
            .getEdgeHeadChunk((VertexInternal) hub.asVertex(true), Vertex.DIRECTION.IN);
        final Iterator<Pair<RID, RID>> entries = list.entryIterator();
        while (walked[0] <= cap && entries.hasNext()) {
          entries.next();
          ++walked[0];
        }
      });

      assertThat(walked[0])
          .as("the self-loop must end the walk; without the guard it yields the chunk's entries forever")
          .isLessThanOrEqualTo(DEGREE);
    }

    /** A healthy graph stays healthy: the cache must not invent a finding either. */
    @Test
    void aHealthyGraphReportsNothing() {
      createHubWithDistinctSpokes();

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

      assertThat(warnings(result)).isEmpty();
      assertThat((Long) result.get("totalCorruptedRecords")).isZero();
      assertThat((Long) result.get("unreachableEdgeRecords")).isZero();
      assertThat((Long) result.get("edgesMissingInReference")).isZero();
      assertThat((Long) result.get("edgesMissingOutReference")).isZero();
    }

    /** Two vertices, {@code DEGREE} parallel edges between them: exactly two adjacency lists in the whole database. */
    private void createParallelEdgesBetweenTwoVertices() {
      database.transaction(() -> {
        final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
        final MutableVertex destination = database.newVertex(VERTEX_TYPE).set("name", "destination").save();
        for (int i = 0; i < DEGREE; i++)
          source.newEdge(EDGE_TYPE, destination);
      });
    }

    /** One hub with {@code DEGREE} distinct spokes, each pointing at it: the super-node shape from the issue. */
    private RID createHubWithDistinctSpokes() {
      final RID[] hub = new RID[1];
      database.transaction(() -> hub[0] = database.newVertex(VERTEX_TYPE).set("name", "hub").save().getIdentity());
      database.transaction(() -> {
        final MutableVertex target = hub[0].asVertex(true).modify();
        for (int i = 0; i < DEGREE; i++)
          database.newVertex(VERTEX_TYPE).set("i", i).save().newEdge(EDGE_TYPE, target);
      });
      return hub[0];
    }

    @SuppressWarnings("unchecked")
    private Collection<String> warnings(final Map<String, Object> result) {
      return (Collection<String>) result.get("warnings");
    }
  }

  /**
   * #6064: {@code GRAPH_SUPERNODE_INTERLEAVE_ROUNDS} used to be a CLIFF. Up to {@code rounds x stripes} entries a
   * promoted super-node's read walk was round-robin (approximately newest-first, #6044); the very next entry froze
   * the rotation and drained the remaining chains one after another (#6048), so an edge's returned position stopped
   * having anything to do with its recency rank and the error grew with the vertex DEGREE again - exactly the
   * failure #6044 fixed, pushed past a constant.
   * <p>
   * A caller reading a SMALL bounded prefix never notices; a caller reading a LARGE but still bounded one - an
   * export, a batch job, {@code ... RETURN x LIMIT 5000} - falls off the cliff silently, and the only lever it had
   * was raising the threshold for every unbounded read in the database too.
   * <p>
   * The walk now WIDENS instead of degrading: past the threshold it keeps rotating, taking a geometrically growing
   * batch from each chain per turn (see {@code InterleavedIterator}). Both properties survive:
   * <ul>
   * <li>full-walk locality - the number of chain switches stays logarithmic in the degree rather than proportional
   *     to it, which is what #6048 was about;</li>
   * <li>rank fidelity at EVERY depth - an entry of recency rank {@code r} comes back at a position of order
   *     {@code r} for the whole walk, not only for the first {@code rounds x stripes} entries.</li>
   * </ul>
   * These tests pin the second one, which is what the cliff took away. Both fail against the pre-#6064 code by a
   * wide margin (worst position/rank ratio in the hundreds, not the single digits).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BoundedLimitOrdering extends TestHelper {
    private static final int STRIPES   = 16;
    private static final int ROUNDS    = 4;
    /** Promotion threshold: everything past this lives in the striped generation the rotation walks. */
    private static final int THRESHOLD = 64;
    private static final int TOTAL     = 3_000;

    private int savedThreshold;
    private int savedStripes;
    private int savedRounds;

    @BeforeEach
    void saveConfig() {
      savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      savedStripes = GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.getValueAsInteger();
      savedRounds = GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.getValueAsInteger();

      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(THRESHOLD);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(STRIPES);
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(ROUNDS);
    }

    @AfterEach
    void restoreConfig() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      GlobalConfiguration.GRAPH_SUPERNODE_STRIPES.setValue(savedStripes);
      GlobalConfiguration.GRAPH_SUPERNODE_INTERLEAVE_ROUNDS.setValue(savedRounds);
    }

    /**
     * The bounded-but-large LIMIT of the issue: a caller stopping at the 1,000th of 3,000 neighbours must get
     * mostly-newest ones. The newest 1,000 edges have to fit inside the first 2,000 positions - a factor-2 slack
     * over their own count, which the widening rotation satisfies at every depth.
     * <p>
     * Pre-#6064 the rotation froze at entry 64 and drained one chain at a time, so the newest edge of the chain
     * drained LAST came back near position {@code TOTAL}: the newest 1,000 were spread over the whole walk.
     */
    @Test
    void aLargeBoundedLimitStillGetsTheNewestEdges() {
      final Fixture fixture = build();

      int worstPosition = 0;
      for (int rank = 0; rank < 1_000; rank++)
        worstPosition = Math.max(worstPosition, fixture.positionOfRank(rank));

      assertThat(worstPosition)
          .as("the newest 1000 of %d edges must come back inside the first 2000 positions, worst was %d", TOTAL,
              worstPosition)
          .isLessThan(2_000);
    }

    /**
     * The guarantee stated as it is documented: the position an edge of recency rank {@code r} is returned at is of
     * ORDER {@code r} - here, within a factor of 3 plus a constant - for the WHOLE walk and not only for the first
     * {@code rounds x stripes} entries.
     * <p>
     * The constant absorbs the rotation's own width (an entry of rank 0 can legitimately come back at position
     * {@code stripes - 1}) and the unevenness of {@code hash(neighbour RID)} across the stripes. The factor absorbs
     * the geometric batch: an entry reached in the batch that doubled at depth {@code d} is emitted no later than
     * the end of that batch, at most twice as deep as its own rank.
     */
    @Test
    void everyRankComesBackAtAPositionOfOrderOfThatRank() {
      final Fixture fixture = build();

      final int leeway = 6 * STRIPES;
      int worstRank = -1;
      double worstRatio = 0;
      for (int rank = 0; rank < TOTAL; rank++) {
        final int position = fixture.positionOfRank(rank);
        final double ratio = (double) (position + 1) / (rank + 1);
        if (position > 3 * rank + leeway && ratio > worstRatio) {
          worstRatio = ratio;
          worstRank = rank;
        }
      }

      assertThat(worstRank)
          .as("rank %d came back at position %d (ratio %.1f), past 3 x rank + %d", worstRank,
              worstRank < 0 ? -1 : fixture.positionOfRank(worstRank), worstRatio, leeway)
          .isEqualTo(-1);
    }

    /** The walk is still complete: the widening reorders nothing away and duplicates nothing. */
    @Test
    void theWideningWalkStillReturnsEveryEdgeExactlyOnce() {
      final Fixture fixture = build();
      assertThat(fixture.positionOf).hasSize(TOTAL).containsOnlyKeys(fixture.sources.toArray(new RID[0]));
      assertThat(new ArrayList<>(fixture.positionOf.values())).doesNotHaveDuplicates();
    }

    private Fixture build() {
      createSchema();
      final RID hubRID = createHub();
      final List<RID> sources = insertEdges(hubRID, TOTAL);
      assertThat(loadInHead(hubRID)).as("the hub must have been promoted to the striped layout")
          .isInstanceOf(StripeDirectory.class);
      return new Fixture(sources, positionsOfNeighbours(hubRID));
    }

    /** Insertion order plus the walk's output positions, with rank 0 = newest edge inserted. */
    private record Fixture(List<RID> sources, Map<RID, Integer> positionOf) {
      private int positionOfRank(final int rank) {
        return positionOf.get(sources.get(sources.size() - 1 - rank));
      }
    }

    private void createSchema() {
      database.transaction(() -> {
        database.getSchema().createVertexType("Hub", 1);
        database.getSchema().createVertexType("Src", 8);
        database.getSchema().createEdgeType("LINK", 8);
      });
    }

    private RID createHub() {
      final MutableVertex[] holder = new MutableVertex[1];
      database.transaction(() -> {
        holder[0] = database.newVertex("Hub");
        holder[0].save();
      });
      return holder[0].getIdentity();
    }

    private List<RID> insertEdges(final RID hubRID, final int count) {
      final List<RID> sources = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final RID[] srcHolder = new RID[1];
        database.transaction(() -> {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          src.newEdge("LINK", hubRID);
          srcHolder[0] = src.getIdentity();
        });
        sources.add(srcHolder[0]);
      }
      return sources;
    }

    private Record loadInHead(final RID hubRID) {
      final Record[] head = new Record[1];
      database.transaction(() -> {
        final RID headRID = ((VertexInternal) hubRID.asVertex(true)).getInEdgesHeadChunk();
        head[0] = database.lookupByRID(headRID, true);
      });
      return head[0];
    }

    private Map<RID, Integer> positionsOfNeighbours(final RID hubRID) {
      final Map<RID, Integer> positions = new HashMap<>();
      database.transaction(() -> {
        int position = 0;
        for (final Iterator<Vertex> it = hubRID.asVertex(true).getVertices(Vertex.DIRECTION.IN, "LINK").iterator();
             it.hasNext(); position++)
          assertThat(positions.put(it.next().getIdentity(), position)).isNull();
      });
      return positions;
    }
  }

  /**
   * Issue #8417: a type-filtered walk over a vertex whose edge list is dominated by ANOTHER edge type ("a hub for one
   * type, a handful of another") must reject the foreign entries on the raw bucket number stored in the segment, without
   * decoding each one into a pair of {@link RID} objects first.
   * <p>
   * The counting tests drive the filtered iterators over a segment proxy that counts {@link EdgeSegment#getRID} calls:
   * before the fix every entry of the list cost two decodes whatever its type, so a list of {@code HUB} foreign entries
   * plus one match cost {@code 2 * (HUB + 1)}; after it only the match is decoded.
   */
  @Nested
  class TypeFilteredScan {
    private static final String DB_PATH = "./target/databases/issue8417TypeFilteredScan";
    private static final int    HUB     = 500;

    private Database database;

    @BeforeEach
    void setUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.transaction(() -> {
        database.getSchema().createVertexType("Unit");
        database.getSchema().createVertexType("Case");
        database.getSchema().createEdgeType("Case_unit");
        database.getSchema().createEdgeType("Parent");
        database.getSchema().createEdgeType("SubParent").addSuperType("Parent");
      });
    }

    @AfterEach
    void tearDown() {
      if (database != null && database.isOpen())
        database.drop();
    }

    @Test
    void filteredRidWalkDecodesOnlyTheMatchingEntry() {
      final RID[] ids = buildHub(HUB, false);
      database.transaction(() -> {
        final AtomicInteger decodes = new AtomicInteger();
        final EdgeSegment head = countingHead(ids[0], decodes);

        final List<RID> found = new ArrayList<>();
        new RIDIteratorFilter((DatabaseInternal) database, head, new String[] { "Parent" }).forEachRemaining(found::add);

        assertThat(found).containsExactly(ids[1]);
        // ONLY THE VERTEX OF THE ONE MATCH: THE EDGE IS STEPPED OVER WITH skipRID. BEFORE #8417 THIS WAS 2 * (HUB + 1)
        assertThat(decodes.get()).isEqualTo(1);
      });
    }

    @Test
    void filteredVertexAndEdgeWalksDecodeOnlyTheMatchingEntry() {
      final RID[] ids = buildHub(HUB, false);
      database.transaction(() -> {
        final AtomicInteger decodes = new AtomicInteger();

        final List<RID> vertices = new ArrayList<>();
        new VertexIteratorFilter((DatabaseInternal) database, countingHead(ids[0], decodes), new String[] { "Parent" }, ids[0],
            Vertex.DIRECTION.IN)
            .forEachRemaining(v -> vertices.add(v.getIdentity()));
        assertThat(vertices).containsExactly(ids[1]);
        assertThat(decodes.get()).isEqualTo(2);

        decodes.set(0);
        final Vertex unit = ids[0].asVertex();
        final List<RID> edges = new ArrayList<>();
        new EdgeIteratorFilter((DatabaseInternal) database, unit, Vertex.DIRECTION.IN, countingHead(ids[0], decodes),
            new String[] { "Parent" }).forEachRemaining(e -> edges.add(e.getIdentity()));
        assertThat(edges).containsExactly(ids[2]);
        assertThat(decodes.get()).isEqualTo(2);

        decodes.set(0);
        final List<Pair<RID, RID>> entries = new ArrayList<>();
        new EdgeVertexIteratorFilter((DatabaseInternal) database, countingHead(ids[0], decodes), new String[] { "Parent" }, ids[0],
            Vertex.DIRECTION.IN)
            .forEachRemaining(entries::add);
        assertThat(entries).hasSize(1);
        assertThat(entries.getFirst().getFirst()).isEqualTo(ids[2]);
        assertThat(entries.getFirst().getSecond()).isEqualTo(ids[1]);
        assertThat(decodes.get()).isEqualTo(2);
      });
    }

    @Test
    void publicApiReturnsExactlyTheRequestedTypeAcrossChunks() {
      final RID[] ids = buildHub(HUB, false);
      database.transaction(() -> {
        final Vertex unit = ids[0].asVertex();

        // THE FIXTURE MUST REALLY SPAN SEVERAL CHUNKS, WITH THE MATCH IN AN OLDER ONE THAN THE HEAD: THE SKIP HAS TO
        // EXHAUST A CHUNK OF FOREIGN ENTRIES AND HOP BEFORE IT FINDS ANYTHING
        final EdgeSegment head = (EdgeSegment) database.lookupByRID(((VertexInternal) unit).getInEdgesHeadChunk(), true);
        assertThat(head.getPrevious()).isNotNull();
        assertThat(head.count(EdgeBucketMask.of((DatabaseInternal) database, new String[] { "Parent" }))).isZero();

        assertThat(ridsOf(unit.getVertices(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(ids[1]);
        assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(ids[2]);
        assertThat(unit.countEdges(Vertex.DIRECTION.IN, "Parent")).isEqualTo(1);
        // THE DOMINANT TYPE STILL COMES BACK WHOLE: THE SKIP MUST NOT LOSE MATCHES WHEN ALMOST EVERYTHING MATCHES
        assertThat(ridsOf(unit.getVertices(Vertex.DIRECTION.IN, "Case_unit").iterator())).hasSize(HUB);
        assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "Case_unit", "Parent").iterator())).hasSize(HUB + 1);

        try (final ResultSet rs = database.query("sql", "SELECT expand(in('Parent')) FROM " + ids[0])) {
          assertThat(rs.next().getIdentity().get()).isEqualTo(ids[1]);
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void polymorphicTypeMatchesItsSubtypeBuckets() {
      final RID[] ids = buildHub(HUB, true);
      database.transaction(() -> {
        final Vertex unit = ids[0].asVertex();
        // ids[3] IS A SubParent EDGE: ASKING FOR "Parent" MUST STILL FIND IT, ASKING FOR "SubParent" ONLY IT
        assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactlyInAnyOrder(ids[2], ids[3]);
        assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "SubParent").iterator())).containsExactly(ids[3]);
      });
    }

    @Test
    void lightweightEdgesAreFilteredOnTheirTypeBucket() {
      final RID[] unit = new RID[1];
      final RID[] parent = new RID[1];
      database.transaction(() -> {
        final MutableVertex u = database.newVertex("Unit").save();
        unit[0] = u.getIdentity();
        for (int i = 0; i < HUB; i++)
          database.newVertex("Case").save().newLightEdge("Case_unit", u);
        final MutableVertex p = database.newVertex("Unit").save();
        parent[0] = p.getIdentity();
        p.newLightEdge("Parent", u);
        for (int i = 0; i < 10; i++)
          database.newVertex("Case").save().newLightEdge("Case_unit", u);
      });
      database.transaction(() -> {
        final Vertex u = unit[0].asVertex();
        assertThat(ridsOf(u.getVertices(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(parent[0]);
        final List<Edge> edges = new ArrayList<>();
        u.getEdges(Vertex.DIRECTION.IN, "Parent").forEach(edges::add);
        assertThat(edges).hasSize(1);
        assertThat(edges.getFirst().getOut()).isEqualTo(parent[0]);
      });
    }

    @Test
    void neighbourFilterStillAppliesAfterTheBucketSkip() {
      final RID[] ids = buildHub(HUB, false);
      database.transaction(() -> {
        final Vertex unit = ids[0].asVertex();
        final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
        assertThat(ridsOf(engine.getEdgesConnectedTo((VertexInternal) unit, Vertex.DIRECTION.IN, ids[1], "Parent")))
            .containsExactly(ids[2]);
        // A Case_unit NEIGHBOUR ASKED FOR WITH THE Parent FILTER: THE BUCKET SKIP DROPS IT, NOTHING COMES BACK
        final RID caseVertex = unit.getVertices(Vertex.DIRECTION.IN, "Case_unit").iterator().next().getIdentity();
        assertThat(ridsOf(engine.getEdgesConnectedTo((VertexInternal) unit, Vertex.DIRECTION.IN, caseVertex, "Parent")))
            .isEmpty();
      });
    }

    @Test
    void removeThroughTheFilteredIteratorRemovesTheMatchedEntry() {
      final RID[] ids = buildHub(HUB, false);
      database.transaction(() -> {
        final RID headRID = ((VertexInternal) ids[0].asVertex()).getInEdgesHeadChunk();
        final VertexIteratorFilter it = new VertexIteratorFilter((DatabaseInternal) database,
            (EdgeSegment) database.lookupByRID(headRID, true), new String[] { "Parent" }, ids[0], Vertex.DIRECTION.IN);
        // remove() deletes the edge of the entry the iterator returned last, which the skip must keep pointing at
        assertThat(it.next().getIdentity()).isEqualTo(ids[1]);
        it.remove();
        assertThat(it.hasNext()).isFalse();
      });
      database.transaction(() -> {
        final Vertex unit = ids[0].asVertex();
        assertThat(unit.countEdges(Vertex.DIRECTION.IN, "Parent")).isZero();
        assertThat(unit.countEdges(Vertex.DIRECTION.IN, "Case_unit")).isEqualTo(HUB);
        // the edge is deleted, not only unlinked from the walked list (#9779)
        assertThat(ids[1].asVertex().countEdges(Vertex.DIRECTION.OUT, "Parent")).isZero();
      });
    }

    @Test
    void bucketMaskMatchesExactlyTheRequestedBucketsAcrossAGap() {
      database.transaction(() -> {
        // DOCUMENT TYPES CREATED BETWEEN Parent AND ITS LATE SUBTYPE OPEN A GAP OF FOREIGN BUCKETS INSIDE THE MASK'S RANGE
        for (int i = 0; i < 5; i++)
          database.getSchema().createDocumentType("Filler" + i);
        database.getSchema().createEdgeType("LateParent").addSuperType("Parent");
      });

      final DatabaseInternal db = (DatabaseInternal) database;
      final EdgeBucketMask mask = EdgeBucketMask.of(db, new String[] { "Parent" });
      final List<Integer> accepted = database.getSchema().getType("Parent").getBucketIds(true);
      for (final Integer bucketId : accepted)
        assertThat(mask.matches(bucketId)).isTrue();
      for (int i = 0; i < 5; i++)
        for (final Integer bucketId : database.getSchema().getType("Filler" + i).getBucketIds(false))
          assertThat(mask.matches(bucketId)).isFalse();
      for (final Integer bucketId : database.getSchema().getType("Case_unit").getBucketIds(false))
        assertThat(mask.matches(bucketId)).isFalse();
      assertThat(mask.matches(-1)).isFalse();
      assertThat(mask.matches(Integer.MAX_VALUE)).isFalse();
      assertThat(mask.matches(Long.MAX_VALUE)).isFalse();

      // NAMES THAT ARE NOT EDGE TYPES MATCH NOTHING (#5194), AND ALONE THEY LEAVE NO MASK AT ALL
      assertThat(EdgeBucketMask.of(db, new String[] { "Unit", "NoSuchType" })).isNull();
      final EdgeBucketMask mixed = EdgeBucketMask.of(db, new String[] { "Unit", "Case_unit" });
      for (final Integer bucketId : database.getSchema().getType("Unit").getBucketIds(false))
        assertThat(mixed.matches(bucketId)).isFalse();
    }

    @Test
    void wideBucketSpanSwitchesToTheSortedShapeAndStillMatchesExactly() {
      // A NARROW SPAN IS THE ONE-ARRAY-READ SHAPE
      final EdgeBucketMask narrow = EdgeBucketMask.ofBucketIds(new int[] { 12, 10, 14 });
      assertThat(narrow.isDense()).isTrue();
      assertThat(narrow.matches(10)).isTrue();
      assertThat(narrow.matches(11)).isFalse();
      assertThat(narrow.matches(14)).isTrue();
      assertThat(narrow.matches(15)).isFalse();

      // A TYPE WHOSE SUBTYPE WAS CREATED THOUSANDS OF BUCKETS LATER: A DENSE MASK WOULD ALLOCATE THE WHOLE GAP PER HOP
      final int far = 10 + EdgeBucketMask.MAX_DENSE_SPAN * 20;
      final EdgeBucketMask wide = EdgeBucketMask.ofBucketIds(new int[] { far, 10, 11, far + 1 });
      assertThat(wide.isDense()).isFalse();
      assertThat(wide.matches(10)).isTrue();
      assertThat(wide.matches(11)).isTrue();
      assertThat(wide.matches(12)).isFalse();
      assertThat(wide.matches(far - 1)).isFalse();
      assertThat(wide.matches(far)).isTrue();
      assertThat(wide.matches(far + 1)).isTrue();
      assertThat(wide.matches(far + 2)).isFalse();
      assertThat(wide.matches(9)).isFalse();
      // A CORRUPT NUMBER THAT WOULD WRAP INTO A VALID ID IF NARROWED FIRST
      assertThat(wide.matches((1L << 32) + 10)).isFalse();

      // THE BOUNDARY: THE WIDEST SPAN STILL SERVED DENSE, AND ONE MORE
      assertThat(EdgeBucketMask.ofBucketIds(new int[] { 0, EdgeBucketMask.MAX_DENSE_SPAN - 1 }).isDense()).isTrue();
      assertThat(EdgeBucketMask.ofBucketIds(new int[] { 0, EdgeBucketMask.MAX_DENSE_SPAN }).isDense()).isFalse();

      assertThat(EdgeBucketMask.ofBucketIds(new int[0])).isNull();
    }

    @Test
    void stripedSuperNodeFindsTheRareTypeInEveryStripe() {
      final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      try {
        final RID[] ids = buildHub(HUB, false);
        database.transaction(() -> {
          final Vertex unit = ids[0].asVertex();
          assertThat(database.lookupByRID(((VertexInternal) unit).getInEdgesHeadChunk(), true)).isInstanceOf(StripeDirectory.class);

          assertThat(ridsOf(unit.getVertices(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(ids[1]);
          assertThat(ridsOf(unit.getEdges(Vertex.DIRECTION.IN, "Parent").iterator())).containsExactly(ids[2]);
          final List<RID> rids = new ArrayList<>();
          ((DatabaseInternal) database).getGraphEngine().getEdgeHeadChunk((VertexInternal) unit, Vertex.DIRECTION.IN)
              .ridIterator("Parent").forEachRemaining(rids::add);
          assertThat(rids).containsExactly(ids[1]);
          assertThat(ridsOf(unit.getVertices(Vertex.DIRECTION.IN, "Case_unit").iterator())).hasSize(HUB);
        });
      } finally {
        GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
      }
    }

    /**
     * Builds the shape from the issue: one Unit with {@code hub} incoming Case_unit edges, one incoming Parent edge in the
     * middle of the list, and optionally one SubParent edge. Returns [unit, parentVertex, parentEdge, subParentEdge].
     */
    private RID[] buildHub(final int hub, final boolean withSubParent) {
      final RID[] ids = new RID[4];
      database.transaction(() -> {
        final MutableVertex unit = database.newVertex("Unit").set("name", "local").save();
        ids[0] = unit.getIdentity();
        for (int i = 0; i < hub / 2; i++)
          database.newVertex("Case").save().newEdge("Case_unit", unit);
        final MutableVertex region = database.newVertex("Unit").set("name", "region").save();
        ids[1] = region.getIdentity();
        ids[2] = region.newEdge("Parent", unit).getIdentity();
        if (withSubParent)
          ids[3] = database.newVertex("Unit").save().newEdge("SubParent", unit).getIdentity();
        for (int i = hub / 2; i < hub; i++)
          database.newVertex("Case").save().newEdge("Case_unit", unit);
      });
      return ids;
    }

    /** The head chunk of the vertex's IN list, wrapped so every {@link EdgeSegment#getRID} call (on any chunk) is counted. */
    private EdgeSegment countingHead(final RID vertex, final AtomicInteger decodes) {
      final RID headRID = ((VertexInternal) vertex.asVertex()).getInEdgesHeadChunk();
      return counting((EdgeSegment) database.lookupByRID(headRID, true), decodes);
    }

    private static EdgeSegment counting(final EdgeSegment delegate, final AtomicInteger decodes) {
      return (EdgeSegment) Proxy.newProxyInstance(EdgeSegment.class.getClassLoader(), new Class<?>[] { EdgeSegment.class },
          (proxy, method, args) -> {
            if (method.getName().equals("getRID"))
              decodes.incrementAndGet();
            try {
              final Object result = method.invoke(delegate, args);
              if (method.getName().equals("getPrevious") && result != null)
                return counting((EdgeSegment) result, decodes);
              return result;
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }

    private static List<RID> ridsOf(final Iterator<? extends Identifiable> it) {
      final List<RID> rids = new ArrayList<>();
      while (it.hasNext())
        rids.add(it.next().getIdentity());
      return rids;
    }
  }

  /**
   * Issue #8868 (prerequisite step): the {@link StripeDirectory} placement-hash version byte (header byte 1) was
   * written but never read. Entries of a promoted vertex are located by RE-HASHING the neighbour RID, so a directory
   * written by a future release with a different placement function would be read by this release with the version-0
   * hash: {@code isConnectedTo} / {@code containsVertex} / {@code removeVertex} would look in the wrong stripe and
   * silently answer "not connected" or miss a removal. The directory must FAIL CLOSED on an unknown version instead,
   * on every path that builds one from stored bytes.
   */
  @Nested
  class StripeDirectoryHashVersion extends TestHelper {
    private static final String VERTEX_TYPE = "Issue8868Node";
    private static final String EDGE_TYPE   = "Issue8868Link";
    private static final int    DEGREE      = 300;
    private static final byte   FUTURE_HASH = StripeDirectory.HASH_VERSION + 1;

    private boolean reopened;

    /** The tests deliberately write a directory this release cannot read, which is the state under test. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @AfterEach
    void restoreDefaults() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.reset();
    }

    @Test
    void currentVersionDirectoryStaysReadableAfterReopen() {
      final RID hub = reopen(createPromotedHub());

      database.transaction(() -> {
        final StripeDirectory directory = loadDirectory(hub);
        assertThat(directory.getGenerationCount()).isEqualTo(2);
        assertThat(hub.asVertex().countEdges(Vertex.DIRECTION.IN, EDGE_TYPE)).isEqualTo(DEGREE);
      });
    }

    @Test
    void constructorRejectsUnknownHashVersion() {
      final RID hub = createPromotedHub();

      database.transaction(() -> {
        final RID dirRID = inHead(hub);
        final Binary content = bucketOf(dirRID).getRecord(dirRID).copyOfContent();
        content.putByte(1, FUTURE_HASH);

        assertThatThrownBy(() -> new StripeDirectory(database, dirRID, content))
            .isInstanceOf(DatabaseMetadataException.class)
            .hasMessageContaining("hash version " + FUTURE_HASH)
            .hasMessageContaining(dirRID.toString());
      });
    }

    @Test
    void lazyPlaceholderRejectsUnknownHashVersionOnFirstAccess() {
      final RID created = createPromotedHub();
      final RID dirRID = reopen(writeFutureHashVersion(created));

      database.transaction(() -> {
        final StripeDirectory lazy = (StripeDirectory) ((DatabaseInternal) database).getRecordFactory()
            .newImmutableRecord(database, null, dirRID, StripeDirectory.RECORD_TYPE);
        assertThatThrownBy(lazy::getGenerationCount)
            .isInstanceOf(DatabaseMetadataException.class)
            .hasMessageContaining("hash version " + FUTURE_HASH);
        // A rejected load must not leave the content behind: the second access fails again instead of reading it.
        assertThatThrownBy(() -> lazy.getHead(1, 0))
            .isInstanceOf(DatabaseMetadataException.class)
            .hasMessageContaining("hash version " + FUTURE_HASH);
      });
    }

    @Test
    void readsOfAFutureVersionDirectoryFailLoudly() {
      final RID created = createPromotedHub();
      final RID neighbourBefore = anyNeighbour(created);
      final RID dirRID = reopen(writeFutureHashVersion(created));
      final RID hub = reopen(created);
      final RID neighbour = reopen(neighbourBefore);

      database.transaction(() -> {
        // RecordFactory content path (lookupByRID)
        assertFailsClosed(() -> database.lookupByRID(dirRID, true));

        final Vertex v = hub.asVertex();
        assertFailsClosed(() -> v.countEdges(Vertex.DIRECTION.IN, EDGE_TYPE));
        // The silent wrong answer the version byte exists to prevent: a re-hashed lookup in the wrong stripe.
        assertFailsClosed(() -> v.isConnectedTo(neighbour, Vertex.DIRECTION.IN));
      });
    }

    /**
     * Pins the user-visible outcome of appending to a hub whose directory carries an unknown version: the transaction
     * fails closed and nothing is written over the directory.
     * <p>
     * What it does NOT isolate (#9266): the database is reopened, so the append resolves the hub's head through the
     * {@code RecordFactory} content path ({@code lookupByRID}), whose directory constructor already throws on the FIRST
     * append - the loop never reaches a second iteration, and its 2,000 bound is not what the test relies on.
     * {@code StripedEdgeList.loadDirectoryForWrite}, the reload a long-lived edge list performs when a stripe head fills,
     * is covered on its own by {@link #writePathReloadOfAnAlreadyLoadedEdgeListRejectsUnknownHashVersion}.
     */
    @Test
    void appendsToAFutureVersionDirectoryFailLoudly() {
      final RID created = createPromotedHub();
      final RID dirRID = reopen(writeFutureHashVersion(created));
      final RID hub = reopen(created);

      assertFailsClosed(() -> database.transaction(() -> {
        for (int i = 0; i < 2_000; i++) {
          final MutableVertex src = database.newVertex(VERTEX_TYPE);
          src.save();
          src.newEdge(EDGE_TYPE, hub);
        }
      }));

      // Nothing was written over the future-format directory.
      database.transaction(() -> {
        final Binary raw = bucketOf(dirRID).getRecord(dirRID);
        assertThat(raw.getByte(1)).isEqualTo(FUTURE_HASH);
      });
    }

    /**
     * The write path's own reload, {@code StripedEdgeList.loadDirectoryForWrite}, isolated from the factory: an
     * edge list holding a directory loaded BEFORE the version changed (a long-lived instance) re-reads the page when a
     * stripe head fills or a stripe is first used. That re-read must reject the new bytes instead of rewriting a slot
     * of a directory it cannot place entries in.
     */
    @Test
    void writePathReloadOfAnAlreadyLoadedEdgeListRejectsUnknownHashVersion() throws Exception {
      final RID hub = createPromotedHub();
      final StripeDirectory[] loadedBefore = new StripeDirectory[1];
      database.transaction(() -> loadedBefore[0] = loadDirectory(hub));
      assertThat(loadedBefore[0].getGenerationCount()).isEqualTo(2);

      writeFutureHashVersion(hub);

      final Method loadDirectoryForWrite = StripedEdgeList.class.getDeclaredMethod("loadDirectoryForWrite");
      loadDirectoryForWrite.setAccessible(true);
      database.transaction(() -> {
        final StripedEdgeList list = new StripedEdgeList(hub.asVertex(), Vertex.DIRECTION.IN, loadedBefore[0]);
        assertFailsClosed(() -> loadDirectoryForWrite.invoke(list));
      });
    }

    @Test
    void checkDatabaseReportsTheVertexAndLeavesTheDirectoryAndItsChainsUntouched() {
      final RID created = createPromotedHub();
      final List<RID> chainHeads = new ArrayList<>();
      database.transaction(() -> {
        final StripeDirectory directory = loadDirectory(created);
        for (int g = 0; g < directory.getGenerationCount(); g++)
          for (int s = 0; s < directory.getStripes(g); s++) {
            final RID head = directory.getHead(g, s);
            if (head != null)
              chainHeads.add(head);
          }
      });
      assertThat(chainHeads).isNotEmpty();

      writeFutureHashVersion(created);
      final RID hub = reopen(created);

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();
      assertThat(result.get("warnings").toString()).contains(hub.toString());

      // Without FIX nothing changes: the directory is still there, still in the future format, and every stripe
      // chain it points to still exists.
      database.transaction(() -> {
        final RID dirRID = inHead(hub);
        assertThat(bucketOf(dirRID).getRecord(dirRID).getByte(1)).isEqualTo(FUTURE_HASH);
        for (final RID head : chainHeads) {
          final RID rebound = new RID(head.getBucketId(), head.getPosition());
          assertThat(bucketOf(rebound).existsRecord(rebound)).as("stripe chain head " + rebound).isTrue();
        }
      });
    }

    /**
     * {@code BaseRecord.size()} lazily loads through {@code reload()} without going through the accessors. It must
     * validate too: otherwise it leaves the rejected content loaded and the next accessor skips the check.
     */
    @Test
    void sizeOnALazyPlaceholderDoesNotLeaveUnvalidatedContentBehind() {
      final RID created = createPromotedHub();
      final RID dirRID = reopen(writeFutureHashVersion(created));

      database.transaction(() -> {
        final StripeDirectory lazy = (StripeDirectory) ((DatabaseInternal) database).getRecordFactory()
            .newImmutableRecord(database, null, dirRID, StripeDirectory.RECORD_TYPE);
        assertThatThrownBy(lazy::size)
            .isInstanceOf(DatabaseMetadataException.class)
            .hasMessageContaining("hash version " + FUTURE_HASH);
        assertThatThrownBy(() -> lazy.getHead(1, 0))
            .isInstanceOf(DatabaseMetadataException.class)
            .hasMessageContaining("hash version " + FUTURE_HASH);
      });
    }

    @Test
    void truncatedDirectoryIsRejectedWithAClearMessage() {
      final Binary truncated = new Binary(1);
      truncated.putByte(0, StripeDirectory.RECORD_TYPE);
      assertThatThrownBy(() -> new StripeDirectory(database, new RID(1, 0), truncated))
          .isInstanceOf(DatabaseMetadataException.class)
          .hasMessageContaining("is corrupt");
    }

    /** Bit pattern {@code 0xFF}: a negative byte, so the check must be an equality, not a "greater than". */
    @Test
    void corruptNegativeHashVersionIsRejectedToo() {
      final RID hub = createPromotedHub();

      database.transaction(() -> {
        final RID dirRID = inHead(hub);
        final Binary content = bucketOf(dirRID).getRecord(dirRID).copyOfContent();
        content.putByte(1, (byte) 0xFF);

        assertThatThrownBy(() -> new StripeDirectory(database, dirRID, content))
            .isInstanceOf(DatabaseMetadataException.class)
            .hasMessageContaining("hash version -1");
      });
    }

    private RID createPromotedHub() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      database.transaction(() -> {
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().createEdgeType(EDGE_TYPE);
      });

      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex(VERTEX_TYPE);
        hub.save();
        holder[0] = hub.getIdentity();
      });
      for (int batch = 0; batch < DEGREE / 50; batch++)
        database.transaction(() -> {
          for (int i = 0; i < 50; i++) {
            final MutableVertex src = database.newVertex(VERTEX_TYPE);
            src.save();
            src.newEdge(EDGE_TYPE, holder[0]);
          }
        });

      database.transaction(() -> assertThat(database.lookupByRID(inHead(holder[0]), true))
          .as("hub must be promoted to the striped layout, otherwise these tests prove nothing")
          .isInstanceOf(StripeDirectory.class));
      return holder[0];
    }

    /** Rewrites the hub's directory in place with an unknown hash version, as a future release would. */
    private RID writeFutureHashVersion(final RID hub) {
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final RID dirRID = inHead(hub);
        final StripeDirectory copy = new StripeDirectory(database, dirRID, bucketOf(dirRID).getRecord(dirRID).copyOfContent());
        copy.getContent().putByte(1, FUTURE_HASH);
        ((DatabaseInternal) database).updateRecord(copy);
        holder[0] = dirRID;
      });
      return holder[0];
    }

    /** The operation must throw a {@link DatabaseMetadataException}, either directly or wrapped by a caller. */
    private static void assertFailsClosed(final ThrowingCallable operation) {
      final Throwable thrown = catchThrowable(operation);
      assertThat(thrown).as("an unknown placement hash version must fail closed, not answer with the version-0 hash").isNotNull();
      for (Throwable t = thrown; t != null; t = t.getCause())
        if (t instanceof DatabaseMetadataException) {
          assertThat(t).hasMessageContaining("hash version " + FUTURE_HASH);
          return;
        }
      throw new AssertionError("expected a DatabaseMetadataException in the cause chain", thrown);
    }

    /**
     * Closes and reopens the database the FIRST time it is called in a test (drops every cached record, so the next
     * read comes from the stored bytes), and rebinds {@code rid} to the open instance. Later calls in the same test
     * only rebind, so every RID a test needs after the reopen goes through this method, after the last write.
     */
    private RID reopen(final RID rid) {
      if (!reopened) {
        reopenDatabase();
        reopened = true;
      }
      return new RID(rid.getBucketId(), rid.getPosition());
    }

    private RID anyNeighbour(final RID hub) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = hub.asVertex().getVertices(Vertex.DIRECTION.IN, EDGE_TYPE).iterator().next().getIdentity());
      return holder[0];
    }

    private StripeDirectory loadDirectory(final RID hub) {
      return (StripeDirectory) database.lookupByRID(inHead(hub), true);
    }

    private RID inHead(final RID hub) {
      return ((VertexInternal) hub.asVertex(true)).getInEdgesHeadChunk();
    }

    private LocalBucket bucketOf(final RID rid) {
      return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    }
  }

  /**
   * Issue #9266, follow-ups of #9211 (#8868 roadmap):
   * <ul>
   *   <li>{@link StripeDirectory} accepts a SET of supported placement hash versions instead of an equality on
   *   {@link StripeDirectory#HASH_VERSION}, so a second version is one array entry away;</li>
   *   <li>{@code CHECK DATABASE} reports a stripe directory it cannot read - unknown version, truncated header or body -
   *   as a per-record finding ({@code unreadableStripeDirectories}) with the reason, never as an exception and never as
   *   a corrupted record (fix mode raw-deletes those), and {@code FIX} rebuilds the owning vertex's edge list from the
   *   surviving edge records and reclaims the directory it supersedes.</li>
   * </ul>
   * The hub is its own vertex type, so a check restricted to the neighbours' type or to the edge type reaches the
   * directory only through a back-reference probe: each pass and each arm is driven separately.
   */
  @Nested
  class StripeDirectoryCheckDatabase extends TestHelper {
    private static final String HUB_TYPE    = "Issue9266Hub";
    private static final String VERTEX_TYPE = "Issue9266Node";
    private static final String EDGE_TYPE   = "Issue9266Link";
    private static final int    DEGREE      = 300;
    private static final byte   FUTURE_HASH = StripeDirectory.HASH_VERSION + 1;
    /** Header (3) + generation 0 (count + 1 slot = 16) + generation 1's stripe count (4): every gen-1 slot is cut off. */
    private static final int    TRUNCATED   = 3 + 16 + 4;

    /** Which of the hub's two lists is promoted and damaged: IN (neighbours point at the hub) unless a test says OUT. */
    private Vertex.DIRECTION hubSide = Vertex.DIRECTION.IN;

    /** The tests deliberately leave a directory this release cannot read, which is the state under test. */
    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @AfterEach
    void restoreDefaults() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.reset();
    }

    @Test
    void supportedHashVersionsAreASetLookupNotAnEquality() {
      assertThat(StripeDirectory.isSupportedHashVersion(StripeDirectory.HASH_VERSION)).isTrue();
      assertThat(StripeDirectory.isSupportedHashVersion(FUTURE_HASH)).isFalse();
      assertThat(StripeDirectory.isSupportedHashVersion((byte) -1)).isFalse();
    }

    @Test
    void aBodyTooShortForItsGenerationsIsRejectedOnLoad() {
      final RID hub = createPromotedHub();

      database.transaction(() -> {
        final RID dirRID = head(hub);
        final byte[] full = bucketOf(dirRID).getRecord(dirRID).copyOfContent().toByteArray();
        final Binary truncated = new Binary(Arrays.copyOf(full, TRUNCATED), TRUNCATED);

        assertThat(StripeDirectory.describeUnreadableContent(new Binary(full, full.length))).isNull();
        assertThat(StripeDirectory.describeUnreadableContent(truncated)).startsWith("is truncated");
        assertThatThrownBy(() -> new StripeDirectory(database, dirRID, truncated))
            .isInstanceOf(DatabaseMetadataException.class)
            .hasMessageContaining(dirRID.toString())
            .hasMessageContaining("is truncated");
      });
    }

    /** A header declaring no generation at all is what a zero-padded short record looks like: corrupt, not empty. */
    @Test
    void aHeaderWithNoGenerationIsRejected() {
      final Binary content = new Binary(new byte[] { StripeDirectory.RECORD_TYPE, StripeDirectory.HASH_VERSION, 0, 0, 0 }, 5);
      assertThat(StripeDirectory.describeUnreadableContent(content)).contains("0 generations");
    }

    @Test
    void checkReportsAFutureVersionDirectoryAsAPerRecordFindingAndChangesNothing() {
      final RID created = createPromotedHub();
      final RID written = rewriteDirectory(created, false);
      reopenDatabase();
      final RID dirRID = rebind(written);
      final RID hub = rebind(created);

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

      assertThat(unreadable(result)).containsExactly(dirRID);
      assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("hash version " + FUTURE_HASH));
      // Never a corrupted record: fix mode raw-deletes those, and neither the hub nor its directory is what to delete.
      assertThat(corrupted(result)).doesNotContain(hub, dirRID);

      database.transaction(() -> assertThat(bucketOf(dirRID).getRecord(dirRID).getByte(1)).isEqualTo(FUTURE_HASH));
    }

    @Test
    void checkReportsATruncatedDirectoryAsAPerRecordFinding() {
      final RID created = createPromotedHub();
      final RID written = rewriteDirectory(created, true);
      reopenDatabase();
      final RID dirRID = rebind(written);

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

      assertThat(unreadable(result)).containsExactly(dirRID);
      assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("is truncated"));
    }

    /**
     * The silent case the layout validation closes: a directory cut down to a bare header is zero-padded by the bucket
     * to {@code MINIMUM_RECORD_SIZE} and used to read as ZERO generations, i.e. an empty edge list - CHECK then blamed
     * every edge ("missing from that vertex's IN list") and never the directory. It must be reported as the directory.
     */
    @Test
    void checkReportsAZeroGenerationDirectoryAsTheDirectoryNotAsAnEmptyList() {
      final RID created = createPromotedHub();
      final RID written = rewriteDirectory(created, full -> new byte[] { StripeDirectory.RECORD_TYPE, StripeDirectory.HASH_VERSION, 0, 0, 0 });
      reopenDatabase();
      final RID dirRID = rebind(written);

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

      assertThat(unreadable(result)).containsExactly(dirRID);
      assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("0 generations"));
      assertThat(warnings(result)).noneMatch(w -> w.contains("missing from that vertex's"));
    }

    @Test
    void fixRebuildsTheHubOverAFutureVersionDirectoryAndReclaimsIt() {
      assertFixRepairs(false);
    }

    @Test
    void fixRebuildsTheHubOverATruncatedDirectoryAndReclaimsIt() {
      assertFixRepairs(true);
    }

    @Test
    void fixRebuildsAPromotedOutgoingListToo() {
      hubSide = Vertex.DIRECTION.OUT;
      assertFixRepairs(false);
    }

    /** Vertex pass, owner arm: only the hub's own type is checked. */
    @Test
    void theHubsOwnVertexCheckReportsTheDirectory() {
      assertScopedCheckReports(c -> c.setTypes(Set.of(HUB_TYPE)));
    }

    /** Vertex pass, far-endpoint probe arm: only the neighbours are checked, the hub is reached through their edges. */
    @Test
    void theNeighboursVertexCheckReportsTheDirectoryThroughItsProbe() {
      assertScopedCheckReports(c -> c.setTypes(Set.of(VERTEX_TYPE)));
    }

    /** Edge pass: the back-reference probe of each edge into the hub's IN list. */
    @Test
    void theEdgeCheckReportsTheDirectoryThroughItsProbe() {
      assertScopedCheckReports(c -> c.setTypes(Set.of(EDGE_TYPE)));
    }

    /** Vertex pass, owner arm, outgoing side. */
    @Test
    void theHubsOwnVertexCheckReportsAnOutgoingDirectory() {
      hubSide = Vertex.DIRECTION.OUT;
      assertScopedCheckReports(c -> c.setTypes(Set.of(HUB_TYPE)));
    }

    /** Vertex pass, far-endpoint probe arm into the hub's OUT list. */
    @Test
    void theNeighboursVertexCheckReportsAnOutgoingDirectoryThroughItsProbe() {
      hubSide = Vertex.DIRECTION.OUT;
      assertScopedCheckReports(c -> c.setTypes(Set.of(VERTEX_TYPE)));
    }

    /** Edge pass: the back-reference probe of each edge into the hub's OUT list. */
    @Test
    void theEdgeCheckReportsAnOutgoingDirectoryThroughItsProbe() {
      hubSide = Vertex.DIRECTION.OUT;
      assertScopedCheckReports(c -> c.setTypes(Set.of(EDGE_TYPE)));
    }

    /** {@code CHECK DATABASE RECORD <hub>}: the scoped vertex arm merges the finding too. */
    @Test
    void theRecordScopedCheckReportsTheDirectory() {
      final RID created = createPromotedHub();
      final RID written = rewriteDirectory(created, false);
      reopenDatabase();
      final RID dirRID = rebind(written);
      final RID hub = rebind(created);

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).setRecords(Set.of(hub)).check();
      assertThat(unreadable(result)).containsExactly(dirRID);
    }

    /** {@code CHECK DATABASE RECORD <edge>}: the scoped edge arm reaches the directory through its probe and merges it. */
    @Test
    void theRecordScopedCheckOfAnEdgeReportsTheDirectory() {
      final RID created = createPromotedHub();
      final RID[] edge = new RID[1];
      database.transaction(() -> edge[0] = created.asVertex().getEdges(hubSide, EDGE_TYPE).iterator().next().getIdentity());
      final RID written = rewriteDirectory(created, false);
      reopenDatabase();
      final RID dirRID = rebind(written);

      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).setRecords(Set.of(rebind(edge[0])))
          .check();
      assertThat(unreadable(result)).containsExactly(dirRID);
    }

    private void assertScopedCheckReports(final Consumer<DatabaseChecker> scope) {
      final RID created = createPromotedHub();
      final RID written = rewriteDirectory(created, false);
      reopenDatabase();
      final RID dirRID = rebind(written);

      final DatabaseChecker checker = new DatabaseChecker(database).setVerboseLevel(0);
      scope.accept(checker);
      final Map<String, Object> result = checker.check();

      assertThat(unreadable(result)).containsExactly(dirRID);
      assertThat(warnings(result)).anyMatch(w -> w.contains(dirRID.toString()) && w.contains("cannot read"));
    }

    private void assertFixRepairs(final boolean truncate) {
      final RID created = createPromotedHub();
      final RID neighbourBefore = anyNeighbour(created);
      final RID written = rewriteDirectory(created, truncate);
      reopenDatabase();
      final RID dirRID = rebind(written);
      final RID hub = rebind(created);
      final RID neighbour = rebind(neighbourBefore);

      final Map<String, Object> fixed = new DatabaseChecker(database).setVerboseLevel(0).setFix(true).check();
      assertThat(unreadable(fixed)).containsExactly(dirRID);
      assertThat(fixed.get("reconnectedEdges")).isEqualTo((long) DEGREE);
      // The vertex the directory belonged to survives: only its edge list was rebuilt.
      assertThat((Collection<RID>) fixed.get("deletedRecordsAfterFix")).doesNotContain(hub);

      database.transaction(() -> {
        assertThat(head(hub)).isNotEqualTo(dirRID);
        assertThat(hub.asVertex().countEdges(hubSide, EDGE_TYPE)).isEqualTo(DEGREE);
        assertThat(hub.asVertex().isConnectedTo(neighbour, hubSide)).isTrue();
        // Superseded and unreachable: reclaimed by the orphan segment pass.
        assertThat(bucketOf(dirRID).existsRecord(dirRID)).isFalse();
      });

      final Map<String, Object> after = new DatabaseChecker(database).setVerboseLevel(0).check();
      assertThat(unreadable(after)).isEmpty();
      assertThat(after.get("totalWarnings")).isEqualTo(0L);
    }

    private RID createPromotedHub() {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);
      database.transaction(() -> {
        database.getSchema().createVertexType(HUB_TYPE);
        database.getSchema().createVertexType(VERTEX_TYPE);
        database.getSchema().createEdgeType(EDGE_TYPE);
      });

      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex hub = database.newVertex(HUB_TYPE);
        hub.save();
        holder[0] = hub.getIdentity();
      });
      for (int batch = 0; batch < DEGREE / 50; batch++)
        database.transaction(() -> {
          for (int i = 0; i < 50; i++) {
            final MutableVertex neighbour = database.newVertex(VERTEX_TYPE);
            neighbour.save();
            if (hubSide == Vertex.DIRECTION.IN)
              neighbour.newEdge(EDGE_TYPE, holder[0]);
            else
              holder[0].asVertex().newEdge(EDGE_TYPE, neighbour);
          }
        });

      database.transaction(() -> assertThat(database.lookupByRID(head(holder[0]), true))
          .as("hub must be promoted to the striped layout, otherwise these tests prove nothing")
          .isInstanceOf(StripeDirectory.class));
      return holder[0];
    }

    /**
     * Rewrites the hub's directory in place, either with an unknown hash version (as a future release would) or cut
     * short after generation 1's stripe count. The bytes go through a subclass because the directory's own buffer is
     * fixed-size: a truncated body cannot be produced through its API.
     */
    private RID rewriteDirectory(final RID hub, final boolean truncate) {
      return rewriteDirectory(hub, full -> {
        if (truncate)
          return Arrays.copyOf(full, TRUNCATED);
        full[1] = FUTURE_HASH;
        return full;
      });
    }

    private RID rewriteDirectory(final RID hub, final UnaryOperator<byte[]> transform) {
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final RID dirRID = head(hub);
        final byte[] bytes = transform.apply(bucketOf(dirRID).getRecord(dirRID).copyOfContent().toByteArray());
        // Resizable: the bucket pads a record cut shorter than a pointer's footprint (#10859)
        final Binary rewritten = new Binary(bytes, bytes.length).setAutoResizable(true);
        final StripeDirectory copy = new StripeDirectory(database, dirRID, bucketOf(dirRID).getRecord(dirRID).copyOfContent()) {
          @Override
          public Binary getContent() {
            rewritten.position(0);
            return rewritten;
          }
        };
        ((DatabaseInternal) database).updateRecord(copy);
        holder[0] = dirRID;
      });
      return holder[0];
    }

    /** Rebinds a RID to the reopened database instance. */
    private static RID rebind(final RID rid) {
      return new RID(rid.getBucketId(), rid.getPosition());
    }

    private RID anyNeighbour(final RID hub) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = hub.asVertex().getVertices(hubSide, EDGE_TYPE).iterator().next().getIdentity());
      return holder[0];
    }

    private RID head(final RID hub) {
      final VertexInternal vertex = (VertexInternal) hub.asVertex(true);
      return hubSide == Vertex.DIRECTION.IN ? vertex.getInEdgesHeadChunk() : vertex.getOutEdgesHeadChunk();
    }

    private LocalBucket bucketOf(final RID rid) {
      return (LocalBucket) database.getSchema().getBucketById(rid.getBucketId());
    }

    @SuppressWarnings("unchecked")
    private static Collection<RID> unreadable(final Map<String, Object> result) {
      return (Collection<RID>) result.get("unreadableStripeDirectories");
    }

    @SuppressWarnings("unchecked")
    private static Collection<RID> corrupted(final Map<String, Object> result) {
      return (Collection<RID>) result.get("corruptedRecords");
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> warnings(final Map<String, Object> result) {
      return (Collection<String>) result.get("warnings");
    }
  }

  /**
   * Issue #9780: the super-node stripe pool of a vertex type ({@code <type>_sn_stripe_<slot>}) was in no list the type
   * answers with, so {@link DocumentType#getInvolvedBuckets()} left it out. The per-file permission map built from that
   * list resolved no type for the pool's files (default-allow for every grant), and an explicit type lock did not lock them,
   * so an explicitly locked append to a promoted vertex failed the commit-time lock coverage check.
   * <p>
   * The pool is named after its type, so a type rename must carry it along: left behind under the old name it would drop
   * out of the type's involved buckets again, and be adopted by the next type created under that name.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SuperNodeStripeInvolved extends TestHelper {
    private static final int STRIPES = 4;

    @Override
    protected void beginTest() {
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD, 16);
      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES, STRIPES);
      database.getSchema().createVertexType("V", 1);
      database.getSchema().createEdgeType("E");
    }

    @Test
    void stripeBucketsResolveToTheirVertexType() {
      final RID hub = createPromotedHub("V");

      assertStripePoolInvolved("V");
      assertThat(countOut(hub)).isEqualTo(400);
    }

    @Test
    void stripeBucketsStayMappedAcrossReopen() {
      createPromotedHub("V");

      reopenDatabase();

      assertStripePoolInvolved("V");
    }

    /** The issue's lock half: one edge appended to a promoted hub under an explicit lock of its vertex and edge types. */
    @Test
    void explicitTypeLockCoversTheStripePool() {
      final RID hub = createPromotedHub("V");

      database.transaction(() -> {
        database.acquireLock().type("V").type("E").lock();
        final MutableVertex other = database.newVertex("V").save();
        for (int i = 0; i < 2 * STRIPES; i++)
          hub.asVertex().newEdge("E", other);
      });

      assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
    }

    @Test
    void aSubtypePoolResolvesToTheSubtype() {
      database.getSchema().buildVertexType().withName("Child").withSuperType("V").withTotalBuckets(1).create();
      createPromotedHub("Child");

      assertStripePoolInvolved("Child");
      assertThat(database.getSchema().getType("V").getInvolvedBuckets().stream().map(Bucket::getName))
          .as("the parent does not claim its subtype's pool").noneMatch(n -> n.startsWith("Child_sn_stripe_"));
    }

    /**
     * Locking a parent type locks its subtypes' records: the edge-list buckets and the stripe pool of a subtype's vertices come
     * with them, or an edge appended to a subtype vertex under the parent's lock fails the commit.
     */
    @Test
    void explicitParentTypeLockCoversTheSubtypeEdgeBucketsAndStripePool() {
      database.getSchema().buildVertexType().withName("Child").withSuperType("V").withTotalBuckets(1).create();
      final RID hub = createPromotedHub("Child");

      database.transaction(() -> {
        database.acquireLock().type("V").type("E").lock();
        final MutableVertex plain = database.newVertex("Child").save();
        final MutableVertex other = database.newVertex("Child").save();
        plain.newEdge("E", other);
        for (int i = 0; i < 2 * STRIPES; i++)
          hub.asVertex().newEdge("E", other);
      });

      assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
    }

    /**
     * The parent's lock reaches every level below it, and a type reached through two supertypes (a diamond) is covered once:
     * edges appended to promoted hubs two levels down commit under a lock of the root type alone.
     */
    @Test
    void explicitRootTypeLockCoversAMultiLevelAndDiamondHierarchy() {
      final Schema schema = database.getSchema();
      schema.buildVertexType().withName("Left").withSuperType("V").withTotalBuckets(1).create();
      schema.buildVertexType().withName("Right").withSuperType("V").withTotalBuckets(1).create();
      schema.buildVertexType().withName("Bottom").withSuperType("Left").withTotalBuckets(1).create().addSuperType("Right");
      assertThat(schema.getType("Bottom").getSuperTypes()).hasSize(2);

      final RID hub = createPromotedHub("Bottom");

      database.transaction(() -> {
        database.acquireLock().type("V").type("E").lock();
        final MutableVertex other = database.newVertex("Bottom").save();
        for (int i = 0; i < 2 * STRIPES; i++)
          hub.asVertex().newEdge("E", other);
      });

      assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
    }

    /**
     * A rename moves the pool under the new name, so it stays the type's own, keeps serving the promoted vertices, and is
     * dropped with the type rather than left behind for a later type created under the old name.
     */
    @Test
    void aRenamedTypeCarriesItsStripePool() {
      final RID hub = createPromotedHub("V");

      database.getSchema().getType("V").rename("W");

      for (int i = 0; i < STRIPES; i++) {
        assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("V", i))).isFalse();
        assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("W", i))).isTrue();
      }
      assertStripePoolInvolved("W");

      database.transaction(() -> {
        database.acquireLock().type("W").type("E").lock();
        final MutableVertex other = database.newVertex("W").save();
        for (int i = 0; i < 2 * STRIPES; i++)
          hub.asVertex().newEdge("E", other);
      });
      assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);

      reopenDatabase();
      assertStripePoolInvolved("W");
      assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);

      database.transaction(() -> database.command("sql", "DELETE FROM W"));
      database.getSchema().dropType("E");
      database.getSchema().dropType("W");
      for (int i = 0; i < STRIPES; i++)
        assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName("W", i)))
            .as("the pool is dropped with its type").isFalse();
    }

    /**
     * A bucket already holding one of the pool's new names refuses the rename before anything moves: the type keeps its name
     * and its pool, and the bucket in the way is left alone.
     */
    @Test
    void aRenameThatWouldCollideWithAStripeNameLeavesEverythingInPlace() {
      final RID hub = createPromotedHub("V");
      final Bucket inTheWay = database.getSchema().createBucket(StripedEdgeList.stripeBucketName("W", 1));

      assertThatThrownBy(() -> database.getSchema().getType("V").rename("W")).isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().existsType("V")).isTrue();
      assertThat(database.getSchema().existsType("W")).isFalse();
      assertStripePoolInvolved("V");
      assertThat(database.getSchema().getBucketByName(StripedEdgeList.stripeBucketName("W", 1)).getFileId())
          .isEqualTo(inTheWay.getFileId());
      assertThat(database.getSchema().getInvolvedTypeByBucketId(inTheWay.getFileId())).isNull();
      assertThat(countOut(hub)).isEqualTo(400);
    }

    /**
     * The pool is walked past the configured stripe count for as long as it stays contiguous, so lowering
     * {@code arcadedb.graph.supernodeStripes} after a promotion does not take the extra slots out of the type's grants or lock.
     */
    @Test
    void aShrunkStripeCountStillCoversTheWholePool() {
      final RID hub = createPromotedHub("V");

      database.getConfiguration().setValue(GlobalConfiguration.GRAPH_SUPERNODE_STRIPES, 2);
      // Any schema change rebuilds the per-file maps, now under the shrunk count
      database.getSchema().createDocumentType("Unrelated");

      assertStripePoolInvolved("V");
      database.transaction(() -> {
        database.acquireLock().type("V").type("E").lock();
        final MutableVertex other = database.newVertex("V").save();
        for (int i = 0; i < 2 * STRIPES; i++)
          hub.asVertex().newEdge("E", other);
      });
      assertThat(countOut(hub)).isEqualTo(400 + 2 * STRIPES);
    }

    /** The one parser of the pool's naming convention: a rename's rollback reads the slot back from a moved bucket's name. */
    @Test
    void stripeSlotIsReadOnlyFromAPoolName() {
      assertThat(StripedEdgeList.stripeSlotOf("V", StripedEdgeList.stripeBucketName("V", 0))).isZero();
      assertThat(StripedEdgeList.stripeSlotOf("V", StripedEdgeList.stripeBucketName("V", 13))).isEqualTo(13);
      assertThat(StripedEdgeList.stripeSlotOf("V", StripedEdgeList.stripeBucketName("W", 1))).isEqualTo(-1);
      assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_")).isEqualTo(-1);
      assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_01")).isEqualTo(-1);
      assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_1_out_edges")).isEqualTo(-1);
      assertThat(StripedEdgeList.stripeSlotOf("V", "V_sn_stripe_99999999999")).isEqualTo(-1);
      assertThat(StripedEdgeList.stripeSlotOf("V", "V_0_out_edges")).isEqualTo(-1);
    }

    private RID createPromotedHub(final String typeName) {
      final RID[] hub = new RID[1];
      database.transaction(() -> {
        final MutableVertex h = database.newVertex(typeName).save();
        hub[0] = h.getIdentity();
        for (int i = 0; i < 400; i++)
          h.newEdge("E", database.newVertex(typeName).save());
      });
      assertThat(database.getSchema().existsBucket(StripedEdgeList.stripeBucketName(typeName, 0)))
          .as("the hub of '%s' was promoted to the striped layout", typeName).isTrue();
      return hub[0];
    }

    private long countOut(final RID hub) {
      return database.lookupByRID(hub, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "E");
    }

    private void assertStripePoolInvolved(final String typeName) {
      final Schema schema = database.getSchema();
      final DocumentType type = schema.getType(typeName);
      final List<Integer> involved = type.getInvolvedBuckets().stream().map(Bucket::getFileId).toList();
      for (int i = 0; i < STRIPES; i++) {
        final Bucket stripe = schema.getBucketByName(StripedEdgeList.stripeBucketName(typeName, i));
        assertThat(involved).as("stripe %d is one of the type's involved buckets", i).contains(stripe.getFileId());
        assertThat(schema.getInvolvedTypeByBucketId(stripe.getFileId()))
            .as("the per-file permission map resolves stripe %d to its type", i).isSameAs(type);
        assertThat(schema.getTypeByBucketId(stripe.getFileId()))
            .as("the stripe stays out of the primary-bucket map: the pool's collision guard relies on it").isNull();
      }
    }
  }
}
