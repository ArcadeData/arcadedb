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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6797: every rebuild trigger an {@code LSM_VECTOR} index had was denominated in mutations, and no number
 * of mutations bounds what a query pays for them.
 * <p>
 * Vectors ingested since the last rebuild are answered by a linear scan of the in-memory delta buffer, so that
 * term of a query is {@code O(buffer)} while the HNSW walk it supplements is {@code O(log corpus)}.
 * {@code rebuildGraphRatio} sizes the buffer as a fixed fraction of the corpus - asymptotically the wrong shape,
 * since a fifth of the corpus scanned linearly dominates a logarithmic walk at every scale - and
 * {@code maxPendingMutations} caps that at a fixed count, so from 250,000 vectors upward (0.2 x 250,000 = 50,000)
 * the threshold stops scaling altogether and the same absolute scan cost lands on an index of any size. Measured
 * at that cap on a 200,000-vector index, the scan was roughly four fifths of query time.
 * <p>
 * The fix adds a trigger denominated in the quantity that actually matters, and measured rather than assumed:
 * {@code maxDeltaScanRatio} bounds the buffer against how many nodes this index's graph walks are observed to
 * visit. Because it is evaluated on the search path against a cost only searches produce, an ingest-only workload
 * never reaches it and keeps the geometric amortization {@code rebuildGraphRatio} exists to provide - only a
 * workload actually paying the scan pays for the rebuilds that remove it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue6797DeltaScanScalingTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  // Must be >= LSMVectorIndex.ASYNC_REBUILD_MIN_GRAPH_SIZE (1000) so a rebuild goes through the async, threshold
  // -gated path rather than the unconditional synchronous one small graphs take.
  private static final int BASE_VECTORS   = 1100;

  private static final Duration REBUILD_SETTLE_TIMEOUT =
      Duration.ofMillis(GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong() + 60_000L);

  /**
   * The defect itself, as a pure function of the four inputs the engine feeds it. The table is the one reported in
   * the issue: the ratio-derived term is the right shape, but the ceiling on it is a constant, so past
   * {@code maxPendingMutations / rebuildGraphRatio} vectors the threshold is the same on a million-vector index
   * as on a ten-million-vector one.
   */
  @Test
  void rebuildThresholdStopsScalingOnceTheFixedCeilingBinds() {
    final int absolute = GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.getDefValue() instanceof Integer i ?
        i : 100;
    final float ratio = 0.2f;
    final int maxPending = 50_000;

    assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000, ratio, maxPending))
        .as("below the ceiling the threshold tracks the graph").isEqualTo(2_000);
    assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 100_000, ratio, maxPending))
        .isEqualTo(20_000);
    assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 250_000, ratio, maxPending))
        .as("the ceiling binds exactly here").isEqualTo(50_000);

    // From here on the graph grows by 40x and the permitted buffer does not move at all.
    assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 1_000_000, ratio, maxPending))
        .isEqualTo(50_000);
    assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000_000, ratio, maxPending))
        .as("issue #6797: 0.2 x 10M is 2,000,000, but the fixed ceiling pins it at the same 50,000 a 250,000"
            + "-vector index gets").isEqualTo(50_000);

    // The two escape hatches the arithmetic already had, pinned so the extraction cannot have changed them.
    assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000_000, 0f, maxPending))
        .as("ratio 0 disables scaling and leaves the absolute floor").isEqualTo(absolute);
    assertThat(LSMVectorIndex.computeRebuildThreshold(absolute, 10_000_000, ratio, 0))
        .as("maxPending 0 removes the ceiling").isEqualTo(2_000_000);
    assertThat(LSMVectorIndex.computeRebuildThreshold(120_000, 250_000, ratio, maxPending))
        .as("the absolute floor is applied last, so an explicit value above the ceiling still wins")
        .isEqualTo(120_000);
  }

  /**
   * The damping term. A budget denominated in the cost of one query knows nothing about what draining the buffer
   * costs, so on a large index under steady ingest it would be exceeded again seconds after a rebuild that took
   * minutes, and would ask for another - a background thread turned permanently busy, which would be a worse
   * problem than the one being fixed. The guard is the amortized argument in the currency both sides can be
   * counted in, so it is asserted as arithmetic rather than as a latency.
   */
  @Test
  void aRebuildIsOnlyWorthTriggeringOnceTheScansHavePaidForIt() {
    final long rebuildWork = 1_000_000L;

    assertThat(LSMVectorIndex.deltaScanPaysForARebuild(0L, rebuildWork))
        .as("no query has scanned anything - nothing has been lost to the buffer yet").isFalse();
    assertThat(LSMVectorIndex.deltaScanPaysForARebuild(rebuildWork - 1, rebuildWork))
        .as("still cheaper to keep scanning than to rebuild").isFalse();
    assertThat(LSMVectorIndex.deltaScanPaysForARebuild(rebuildWork, rebuildWork))
        .as("the scans have now cost what the rebuild will, which is the 2-competitive bound this gives: the "
            + "extra rebuild CPU the policy adds can never exceed the query CPU it removes").isTrue();
    assertThat(LSMVectorIndex.deltaScanPaysForARebuild(rebuildWork * 100, rebuildWork)).isTrue();

    // A graph that costs nothing to rebuild is always worth rebuilding; the caller keeps the "is there a graph
    // at all" question to itself, because a rebuild that has no graph to walk is not this policy's decision.
    assertThat(LSMVectorIndex.deltaScanPaysForARebuild(5L, 0L)).isTrue();
  }

  /**
   * The fix, end to end: with every mutation-count trigger placed out of reach, a buffer that outgrows the
   * measured graph-walk cost must still be drained.
   */
  @Test
  void deltaScanOutgrowingTheGraphWalkTriggersARebuild() {
    disableEveryCountBasedTrigger();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 1.0f);

    final Random random = new Random(6797);
    final LSMVectorIndex index = createSettledIndex(random);

    // One query to measure the walk. Until a search has actually walked this graph there is no latency to protect
    // and the policy deliberately does nothing.
    index.findNeighborsFromVector(randomUnitVector(random), 10);

    final long walkCost = index.getStats().get("graphWalkVisitedAvg");
    assertThat(walkCost).as("a graph walk over %d vectors must have visited something", BASE_VECTORS)
        .isGreaterThan(0L);

    final long budget = index.getStats().get("deltaScanBudget");
    assertThat(budget).as("the budget must be a real number, not the disabled sentinel")
        .isLessThan(Long.MAX_VALUE);
    assertThat(budget).as("at ratio 1.0 the budget IS the measured walk cost, not the absolute floor - if it were "
            + "the floor, this test would be pinning mutationsBeforeRebuild rather than the new policy")
        .isEqualTo(walkCost)
        .isGreaterThan(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.getValueAsInteger());

    // Fill the buffer past the budget. Nothing here can reach a mutation-count trigger: the effective count
    // threshold is over a hundred thousand and the inactivity timer is off.
    final int overBudget = (int) Math.min(budget + 50, 5_000);
    insertVectors(random, overBudget);

    assertThat(index.getStats().get("mutationsSinceRebuild"))
        .as("well below the count threshold of %d - only the scan budget can fire here",
            index.getStats().get("effectiveMutationsThreshold"))
        .isLessThan(index.getStats().get("effectiveMutationsThreshold"));
    assertThat(index.getStats().get("deltaVectorsCount"))
        .as("the buffer is over the measured budget of %d", budget)
        .isGreaterThanOrEqualTo(budget);

    // ...but not yet enough scanning to have paid for the rebuild: the buffer being over budget is only half
    // the trigger. This is what stops the policy asking for a rebuild the instant the buffer refills.
    assertThat(index.getStats().get("deltaScanWorkSinceRebuild"))
        .as("no query has scanned the new buffer yet")
        .isLessThan(index.getStats().get("estimatedRebuildWork"));

    // The trigger lives on the search path, so searches are what must notice - and enough of them that the scan
    // they have collectively paid for exceeds what the rebuild will cost.
    index.findNeighborsFromVector(randomUnitVector(random), 10);

    Awaitility.await("a delta buffer that outgrew the graph walk is drained by a rebuild (issue #6797)")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          index.findNeighborsFromVector(randomUnitVector(random), 10);
          assertThat(index.getStats().get("deltaVectorsCount")).isZero();
        });
  }

  /**
   * The control, and the opt-out: with {@code maxDeltaScanRatio} at 0 the engine behaves exactly as it did before
   * this issue - the count thresholds are the only trigger, and a buffer well past any measured walk cost is
   * scanned query after query without anything draining it.
   */
  @Test
  void ratioZeroLeavesTheCountThresholdsAsTheOnlyTrigger() throws Exception {
    disableEveryCountBasedTrigger();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);

    final Random random = new Random(6797);
    final LSMVectorIndex index = createSettledIndex(random);

    index.findNeighborsFromVector(randomUnitVector(random), 10);
    assertThat(index.getStats().get("deltaScanBudget"))
        .as("ratio 0 reports the disabled sentinel").isEqualTo(Long.MAX_VALUE);

    final int buffered = 600;
    insertVectors(random, buffered);

    for (int i = 0; i < 5; i++)
      index.findNeighborsFromVector(randomUnitVector(random), 10);

    // Long enough for an async rebuild to have started and drained the buffer had anything triggered one.
    Thread.sleep(1_000);

    assertThat(index.getStats().get("deltaVectorsCount"))
        .as("with the policy off, nothing bounds the scan - which is precisely the behaviour issue #6797 reports")
        .isEqualTo((long) buffered);
  }

  /**
   * The scan's own answer must not have changed. Pruning against the k-th best distance before the duplicate and
   * tombstone checks rather than after admits exactly the same rows - the heap only advances when a row is added,
   * and neither check can make a row admissible - and this pins that against distances computed by hand.
   */
  @Test
  void deltaScanStillRanksExactly() {
    disableEveryCountBasedTrigger();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);

    final Random random = new Random(6797);
    final LSMVectorIndex index = createSettledIndex(random);
    index.findNeighborsFromVector(randomUnitVector(random), 10);

    // Buffered vectors placed strictly nearer the query than anything the base corpus holds: the base vectors are
    // random unit vectors, these sit on the first axis a hair away from the query. So the answer is decided
    // entirely by the delta scan and can be checked against distances computed here.
    final float[] query = new float[EMBEDDING_DIM];
    query[0] = 1f;

    final List<float[]> planted = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      final float[] vector = new float[EMBEDDING_DIM];
      vector[0] = 1f;
      vector[1] = 0.001f * (i + 1);
      planted.add(vector);
    }
    database.transaction(() -> {
      for (final float[] vector : planted)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) vector);
    });

    assertThat(index.getStats().get("deltaVectorsCount")).isEqualTo((long) planted.size());

    final int k = 5;
    final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, k);
    assertThat(results).hasSize(k);

    // Ascending by distance, and the k nearest planted vectors in the order their euclidean distances dictate.
    for (int i = 1; i < results.size(); i++)
      assertThat(results.get(i).getSecond()).isGreaterThanOrEqualTo(results.get(i - 1).getSecond());

    for (int i = 0; i < k; i++) {
      final float[] stored = (float[]) results.get(i).getFirst().asVertex().get("vector");
      assertThat(stored[1]).as("row %d must be the %d-th nearest planted vector", i, i)
          .isEqualTo(0.001f * (i + 1));
    }
  }

  /**
   * The one state in which the delta merge is offered a RID the graph walk already returned - a rebuild leaves a
   * node unreachable and re-queues its vector into the buffer while the node itself survives in the graph. The
   * duplicate check that catches it moved below the distance prune, so pin that it still catches it.
   */
  @Test
  void aRidInBothTheGraphAndTheBufferIsReturnedOnce() {
    disableEveryCountBasedTrigger();
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);

    final Random random = new Random(6797);
    final LSMVectorIndex index = createSettledIndex(random);
    final float[] query = randomUnitVector(random);

    final List<Pair<RID, Float>> before = index.findNeighborsFromVector(query, 10);
    assertThat(before).isNotEmpty();

    // Put the nearest row into the delta buffer as well, without removing its graph node.
    final RID duplicated = before.getFirst().getFirst();
    assertThat(index.requeueIntoDeltaBufferForTest(duplicated))
        .as("the row must still carry a live graph node").isNotEqualTo(-1);

    final List<Pair<RID, Float>> after = index.findNeighborsFromVector(query, 10);
    final Set<RID> distinct = new HashSet<>();
    for (final Pair<RID, Float> row : after)
      assertThat(distinct.add(row.getFirst())).as("%s returned twice", row.getFirst()).isTrue();
    assertThat(after).hasSameSizeAs(before);
  }

  /**
   * Puts every mutation-count trigger out of reach so only the scan budget can fire. The count threshold is
   * {@code max(absolute, min(ratio x graphSize, maxPending))}, so an enormous ratio with the ceiling removed
   * leaves it at six figures on a 1100-vector graph, while the absolute floor - which is also the floor the scan
   * budget is held to - stays at its default.
   */
  private void disableEveryCountBasedTrigger() {
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 100f);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_PENDING_MUTATIONS, 0);
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);
  }

  private LSMVectorIndex createSettledIndex(final Random random) {
    database.transaction(() -> {
      database.getSchema().createVertexType("Embedding");
      database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN"
          }""".formatted(EMBEDDING_DIM));
    });

    insertVectors(random, BASE_VECTORS);

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Embedding[vector]");
    final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // The first search on an empty graph builds it synchronously whatever the thresholds say, which is what
    // settles the buffer and gets the graph past ASYNC_REBUILD_MIN_GRAPH_SIZE.
    assertThat(index.findNeighborsFromVector(randomUnitVector(random), 10)).isNotEmpty();
    Awaitility.await("the initial synchronous build settles the buffer")
        .atMost(REBUILD_SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> assertThat(index.getStats().get("deltaVectorsCount")).isZero());
    assertThat(index.getStats().get("graphNodeCount")).isGreaterThanOrEqualTo(1000L);
    return index;
  }

  private void insertVectors(final Random random, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) randomUnitVector(random));
    });
  }

  private float[] randomUnitVector(final Random random) {
    final float[] vector = new float[EMBEDDING_DIM];
    float norm = 0;
    for (int i = 0; i < EMBEDDING_DIM; i++) {
      vector[i] = random.nextFloat() * 2 - 1;
      norm += vector[i] * vector[i];
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0)
      for (int i = 0; i < EMBEDDING_DIM; i++)
        vector[i] /= norm;
    return vector;
  }
}
