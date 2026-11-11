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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.VectorGraphBuildRefusedException;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #11497: a graph build a SEARCH needs, which the heap gate has judged does not fit, must not be
 * built anyway.
 * <p>
 * The reported incident: eight search-triggered builds of 145,000-165,000 vectors at 768 dimensions ran one after another
 * on a heap whose live set kept climbing. For two of them the gate logged that the build did not fit even after giving up
 * the whole page cache, said "It is built anyway", and built; 19 seconds after the second one the JVM threw
 * {@link OutOfMemoryError} on a query thread and the pod was killed during the heap dump, with every database on it.
 * <p>
 * What the gate does now, in order: give up page cache, then run with a smaller build cache (the cache only saves the
 * build page reads), and when even the graph alone does not fit, fail the query with a retryable
 * {@link VectorGraphBuildRefusedException} and build nothing. Never answer from the delta buffer alone, which for a
 * persisted corpus is an incomplete result presented as a complete one.
 * <p>
 * The heap is pinned through {@code pinSearchBuildHeapReadingForTest} rather than filled: the gate's decision is a function
 * of the readings it takes, and the fixture reaches the real search path, the real permit and the real build with them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue11497SearchBuildHeapGateTest {
  private static final String DB_ROOT     = "target/test-databases/Issue11497SearchBuildHeapGateTest";
  private static final int    DIMENSIONS  = 16;
  /** Above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}: a smaller build runs inline, takes no permit and is not gated. */
  private static final int    NUM_VECTORS = 1_200;
  private static final long   MB          = 1024L * 1024L;

  private String dbPath;
  private long   originalPermitTimeoutMs;

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
    // A build that gives up waiting for a permit proceeds without one; at the ten-minute default a permit this test leaked
    // would turn into a ten-minute hang rather than a failure of the searchRebuildsWithoutPermit assertion
    originalPermitTimeoutMs = GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong();
    GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(60_000);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(originalPermitTimeoutMs);
    FileUtils.deleteRecursively(new File(dbPath));
  }

  /**
   * The bug: the gate answered "does not fit" and the search built anyway. Now the search fails with a retryable error,
   * nothing is built, the permit goes back, and the next search on a heap with room gets the complete answer.
   */
  @Test
  void aSearchWhoseGraphDoesNotFitTheHeapIsRefusedAndNothingIsBuilt() {
    populateAndCrashWithoutPersistingAGraph(0);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("persistedGraphNodeCount"))
            .as("precondition: no graph on disk, so the first search has to build one").isZero();

        // 1,200 nodes need about 1.3 MB for the graph alone; 1 MB available and nothing reclaimable cannot hold it
        index.pinSearchBuildHeapReadingForTest(MB, 0L);

        final Throwable refused = catchThrowable(() -> index.findNeighborsFromVector(embedding(7), 5, 64));
        assertThat(refused).isInstanceOf(VectorGraphBuildRefusedException.class);
        assertThat(refused.getMessage())
            .contains(GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT.getKey())
            .contains("1200 vectors");
        assertThat(ErrorCategory.of(refused)).as("a refusal of the heap is retryable, not a server fault")
            .isEqualTo(ErrorCategory.RETRY);
        assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isEqualTo(1L);
        assertThat(index.getStats().get("graphNodeCount")).as("nothing may have been built").isZero();
        assertThat(index.getStats().get("lastGraphBuildCacheSize")).as("no build ran at all").isZero();

        // Not answered from the delta buffer on the next attempt either: the query is refused again, the same way
        final Throwable refusedAgain = catchThrowable(() -> index.findNeighborsFromVector(embedding(7), 5, 64));
        assertThat(refusedAgain).isInstanceOf(VectorGraphBuildRefusedException.class);
        assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isEqualTo(2L);

        // Through SQL the refusal keeps its retryable classification, whatever the function wraps it in
        final Throwable refusedBySql = catchThrowable(() -> {
          try (final var rs = db.query("sql", "SELECT expand(vectorNeighbors('Doc[vector]', ?, 5))", embedding(7))) {
            while (rs.hasNext())
              rs.next();
          }
        });
        assertThat(CauseChain.contains(refusedBySql, VectorGraphBuildRefusedException.class)).isTrue();
        assertThat(ErrorCategory.of(refusedBySql)).isEqualTo(ErrorCategory.RETRY);
        // The HTTP handler answers 503 for a QueryHeapBudgetExceededException found on the exception itself or one generic
        // wrapper (CommandExecutionException, TransactionException) down, and no deeper: the refusal must be there
        assertThat(refusedBySql instanceof VectorGraphBuildRefusedException
            || (refusedBySql instanceof CommandExecutionException && refusedBySql.getCause() instanceof VectorGraphBuildRefusedException))
            .as("the refusal must reach the HTTP status mapping, got %s", refusedBySql).isTrue();

        // The heap has room again: the query is its own retry, and gets the whole corpus, not a part of it
        index.pinSearchBuildHeapReadingForTest(-1L, -1L);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), 5, 64);
        assertThat(results).hasSize(5);
        assertThat(results.getFirst().getFirst()).as("the exact match is in the graph").isEqualTo(ridOf(db, 7));
        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
        assertThat(index.getStats().get("searchRebuildsWithoutPermit"))
            .as("the refused builds must have handed their permit back").isZero();
        assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isEqualTo(3L);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * A refusal settles nothing about the graph on disk, so it must leave the question open for the next search (PR #11499
   * review). In a session that wrote before it searched, {@code graphState} is already MUTABLE, and once the latch is
   * cleared the only thing left saying "no graph yet" is {@code ensureGraphAvailable()}'s own latch: the approximate (PQ)
   * search reaches the build through that method alone, and would answer from the delta buffer, which holds just the
   * vectors written since the reopen.
   */
  @Test
  void aRefusalInASessionThatWroteFirstLeavesTheGraphUnresolved() {
    populateAndCrashWithoutPersistingAGraph(0);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        // One write before any search: the index leaves LOADING for MUTABLE with no graph resident
        db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", embedding(NUM_VECTORS)).save());
        assertThat(index.getStats().get("graphState")).as("precondition: the write made the index MUTABLE")
            .isEqualTo((long) LSMVectorIndex.GraphState.MUTABLE.ordinal());
        assertThat(index.graphNotYetMaterialisedForTest()).as("precondition: no graph resolved yet").isTrue();

        index.pinSearchBuildHeapReadingForTest(MB, 0L);
        assertThat(catchThrowable(() -> index.findNeighborsFromVector(embedding(7), 5, 64)))
            .isInstanceOf(VectorGraphBuildRefusedException.class);

        assertThat(index.graphNotYetMaterialisedForTest())
            .as("a refused build must leave the graph unresolved, or the next PQ search answers from the delta buffer alone")
            .isTrue();

        // And the question is answered once the heap has room: the whole corpus, the new vector included
        index.pinSearchBuildHeapReadingForTest(-1L, -1L);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), 5, 64);
        assertThat(results.getFirst().getFirst()).isEqualTo(ridOf(db, 7));
        assertThat(index.graphNotYetMaterialisedForTest()).isFalse();
        assertThat(index.getStats().get("graphNodeCount") + index.getStats().get("deltaVectorsCount"))
            .as("every vector is searchable").isGreaterThanOrEqualTo(NUM_VECTORS + 1L);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The cache is the one term of the estimate a build can do without, so a build that does not fit with its full cache but
   * does with a smaller one runs with the smaller one, instead of being refused. The cap must reach the real build.
   */
  @Test
  void aSearchWhoseBuildFitsOnlyWithASmallerCacheRunsWithThatCache() {
    // An explicit cache of 200,000 vectors: well above the corpus, so the floor the gate may cut it to (100,000) is too
    populateAndCrashWithoutPersistingAGraph(200_000);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final int percent = db.getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT);

        // About 26.9 MB with the full cache (200,000 x 128 bytes plus the graph), about 14.1 MB with the floor: 20 MB of
        // available heap at 90% is 18 MB, which holds the second and not the first
        final long available = 20 * MB;
        final long fullEstimate = VectorHeapBudget.estimateRebuildHeapBytes(NUM_VECTORS, DIMENSIONS, 200_000, false);
        assertThat(VectorHeapBudget.reclaimNeededFor(fullEstimate, percent, available, 0L))
            .as("precondition: the build must not fit with its full cache").isEqualTo(-1L);
        final long expectedCap = VectorHeapBudget.largestBuildCacheThatFits(NUM_VECTORS, DIMENSIONS, percent, available, 0L);
        assertThat(expectedCap).as("precondition: but must fit with the floor cache").isBetween(100_000L, 199_999L);

        index.pinSearchBuildHeapReadingForTest(available, 0L);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(11), 5, 64);

        assertThat(results).hasSize(5);
        assertThat(results.getFirst().getFirst()).isEqualTo(ridOf(db, 11));
        assertThat(index.getStats().get("searchRebuildsWithReducedCache")).isEqualTo(1L);
        assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isZero();
        assertThat(index.getStats().get("lastGraphBuildCacheSize"))
            .as("the build must have run with the cache the gate measured, not the configured one").isEqualTo(expectedCap);
        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
      } finally {
        db.drop();
      }
    }
  }

  /**
   * {@code rebuildMaxHeapPercent = 0} disables the gate for this path as it does for the online one: the operator's escape
   * hatch to build regardless.
   */
  @Test
  void aDisabledGateBuildsWhateverTheHeapReads() {
    populateAndCrashWithoutPersistingAGraph(0);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT, 0);
        final LSMVectorIndex index = vectorIndex(db);
        index.pinSearchBuildHeapReadingForTest(MB, 0L);

        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(3), 5, 64);

        assertThat(results).hasSize(5);
        assertThat(results.getFirst().getFirst()).isEqualTo(ridOf(db, 3));
        assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isZero();
        assertThat(index.getStats().get("searchRebuildsWithReducedCache")).isZero();
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The gate's decisions on the readings of the incident, at its 768 dimensions and the operator's
   * {@code rebuildMaxHeapPercent = 25}. The page cache is folded into the available figure (and none is left to reclaim)
   * because a test heap has no gigabytes of cache to give up, and the pinned overload reclaims from the real one.
   */
  @Test
  void theReadingsOfTheIncidentAreDecidedWithoutBuildingAnything() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT, 25);
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Entity");
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          // An explicit cache size makes the full estimate independent of the heap of the JVM running the test
          db.command("sql", """
              CREATE INDEX ON Entity (vector) LSM_VECTOR METADATA { "dimensions": 768, "similarity": "COSINE", \
              "graphBuildCacheSize": 160000 }""");
        });
        final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType("Entity")
            .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];

        // Entity_4, 04:05:24: 154,528 vectors against 596 MB available plus 1,044 MB of page cache. The graph alone is
        // about 163 MB and the floor cache another 299 MB, against 25% of 1,640 MB: refused, where it used to be built
        final Throwable refused = catchThrowable(() -> index.fitSearchBuildToHeap(154_528, (596 + 1_044) * MB, 0L));
        assertThat(refused).isInstanceOf(VectorGraphBuildRefusedException.class);
        assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isEqualTo(1L);

        // Entity_13, 04:03:57: 148,727 vectors against 1,824 MB plus 2,774 MB. The whole build fits: no cap
        assertThat(index.fitSearchBuildToHeap(148_727, (1_824 + 2_774) * MB, 0L)).isZero();

        // In between, the full cache does not fit and the floor does: built with the largest cache that fits
        final long available = 2_000 * MB;
        final int cap = index.fitSearchBuildToHeap(154_528, available, 0L);
        assertThat(cap).isBetween(100_000, 159_999);
        assertThat(VectorHeapBudget.reclaimNeededFor(VectorHeapBudget.estimateRebuildHeapBytes(154_528, 768, cap, false), 25,
            available, 0L)).as("the cut-down build fits").isZero();
        assertThat(VectorHeapBudget.reclaimNeededFor(VectorHeapBudget.estimateRebuildHeapBytes(154_528, 768, cap + 1, false),
            25, available, 0L)).as("and it is the largest cache that does").isEqualTo(-1L);
        assertThat(index.getStats().get("searchRebuildsWithReducedCache")).isEqualTo(1L);
        assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isEqualTo(1L);

        // A short reclaim: the full build fits only by giving up about 1,040 MB of page cache, and the cache of a test JVM
        // holds nowhere near that, so the reclaim delivers less than it was asked for. The build is judged again against what
        // was delivered - with nothing delivered, 1,500 MB at 25% leaves room for about 73,000 cached vectors beside the graph,
        // under the floor, so it is refused; some 320 MB delivered would make it a reduced-cache build instead - where it used
        // to proceed with its full cache "on heap it was not actually given"
        final long shortAvailable = 1_500 * MB;
        final long claimedReclaimable = 2_774 * MB;
        final long fullEstimate = VectorHeapBudget.estimateRebuildHeapBytes(148_727, 768, 160_000, false);
        assertThat(VectorHeapBudget.reclaimNeededFor(fullEstimate, 25, shortAvailable, claimedReclaimable))
            .as("precondition: the full build fits only with the claimed reclaim").isBetween(1_000 * MB, 1_200 * MB);
        // What the reclaim really delivers depends on what the process-wide cache holds, which other tests of the same JVM
        // decide, so the verdict is checked against what it delivered rather than against an assumption about the cache
        final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
        final long cacheBefore = pageManager.getReadCacheRAM();
        final int[] shortReclaimCap = new int[1];
        final Throwable outcome = catchThrowable(
            () -> shortReclaimCap[0] = index.fitSearchBuildToHeap(148_727, shortAvailable, claimedReclaimable));
        final long delivered = Math.max(0L, cacheBefore - pageManager.getReadCacheRAM());
        assertThat(delivered).as("precondition: no test cache holds the 1 GB asked for, so the reclaim falls short")
            .isLessThan(1_000 * MB);

        // Judged against the delivered bytes, never the claimed ones: on the claim the full cache fitted (cap 0), which is
        // what the build used to run with "on heap it was not actually given"
        final long fitsOnDelivered = VectorHeapBudget.largestBuildCacheThatFits(148_727, 768, 25, shortAvailable, delivered);
        if (fitsOnDelivered < 100_000) {
          assertThat(outcome).isInstanceOf(VectorGraphBuildRefusedException.class);
          assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isEqualTo(2L);
        } else {
          assertThat(outcome).isNull();
          assertThat(shortReclaimCap[0]).as("cut to what the delivered bytes hold").isEqualTo((int) Math.min(160_000, fitsOnDelivered));
        }
      } finally {
        db.drop();
      }
    }
  }

  /**
   * {@link VectorHeapBudget#largestBuildCacheThatFits} must agree with {@link VectorHeapBudget#reclaimNeededFor}, the
   * function the rest of the gate decides with: the cache it answers fits, and one more vector does not.
   */
  @Test
  void theLargestCacheThatFitsAgreesWithTheGate() {
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(1_000, 768, 0, 0L, 0L)).as("gate disabled").isEqualTo(Long.MAX_VALUE);
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(0, 768, 90, 0L, 0L)).as("nothing to build").isEqualTo(Long.MAX_VALUE);
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(1_000_000, 768, 90, 100 * MB, 0L))
        .as("a graph of a million nodes does not fit 90 MB").isEqualTo(-1L);
    assertThat(VectorHeapBudget.largestBuildCacheThatFits(1_000, 768, 90, Long.MAX_VALUE, Long.MAX_VALUE))
        .as("saturated rather than wrapped").isPositive();

    final Random random = new Random(11497L);
    for (int i = 0; i < 10_000; i++) {
      final long nodes = 1 + random.nextInt(2_000_000);
      final int dimensions = 1 + random.nextInt(4_096);
      final int percent = 1 + random.nextInt(100);
      final long available = (long) (random.nextDouble() * 64L * 1024 * MB);
      final long reclaimable = random.nextBoolean() ? 0L : (long) (random.nextDouble() * 8L * 1024 * MB);

      final long cache = VectorHeapBudget.largestBuildCacheThatFits(nodes, dimensions, percent, available, reclaimable);
      if (cache < 0) {
        assertThat(VectorHeapBudget.reclaimNeededFor(VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, 0L, false),
            percent, available, reclaimable)).as("the graph alone does not fit").isEqualTo(-1L);
        continue;
      }
      assertThat(VectorHeapBudget.reclaimNeededFor(VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, cache, false),
          percent, available, reclaimable)).as("the answered cache fits (case %d)", i).isGreaterThanOrEqualTo(0L);
    }
  }

  /**
   * Writes the vectors and kills the database rather than closing it, so no graph is persisted beside them: the state of
   * an index whose first query after a restart has to build its graph.
   *
   * @param graphBuildCacheSize the index's explicit build cache size, or 0 to leave it to the automatic sizing
   */
  private void populateAndCrashWithoutPersistingAGraph(final int graphBuildCacheSize) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\"" + (graphBuildCacheSize > 0 ? ", \"graphBuildCacheSize\": " + graphBuildCacheSize : "")
              + " }");
        });
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          if (i % 500 == 499) {
            db.commit();
            db.begin();
          }
        }
        db.commit();

        // kill() leaves the graph unpersisted, which is the point; close() after it takes the instance out of the
        // factory's registry so the next session can open the same path
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x11497L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static RID ridOf(final Database db, final int id) {
    try (final var rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      assertThat(rs.hasNext()).as("record %d must exist", id).isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
