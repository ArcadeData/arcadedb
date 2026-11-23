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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.VectorGraphBuildRefusedException;
import com.arcadedb.index.IndexException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.within;

/**
 * Oracle tests for the heap admission of graph builds that the vector tests of the memory budget leave unexecuted: the
 * online rebuild gate when the page read cache really has to give bytes back, the demotion of the resident graph to its
 * on-disk twin when that reclaim is short or unaffordable, the cooldown after a deferral, the sizing of the caches by explicit
 * settings, and the failure a search-decided build hands back to the search that waits for it.
 * <p>
 * The heap readings are pinned through the package-private overloads, so the decisions are a function of numbers the test
 * derives from the same estimates the gate uses (no magic constants), while the page cache the gate reclaims from is the real
 * one of the database. The oracle is the observable outcome of the decision (admitted, deferred, graph kept on the heap or
 * demoted to its on-disk twin, the page cache really smaller) and, for every search, brute-force exact kNN over the stored
 * vectors.
 */
@Tag("vector")
@Timeout(180)
class VectorMemoryPressureOracleTest {
  private static final String DB_ROOT     = "target/test-databases/VectorMemoryPressureOracleTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 300;
  private static final int    BUILD_CACHE = 64;
  private static final long   MB          = 1024L * 1024L;

  private String dbPath;

  /**
   * Every setting these tests change is read through the configuration of the database ({@code db.getConfiguration()}), and is set
   * there: it dies with the database, so no process-wide setting is touched and nothing has to be restored.
   */
  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(dbPath));
  }

  // ---------------------------------------------------------------- explicit sizes win over the heap arithmetic

  @Test
  void anExplicitGraphBuildCacheSizeIsTheCapacityWhateverTheHeapReads() {
    withIndex((db, index, vectors) -> {
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE, 123);

      assertThat(index.computeGraphBuildCacheCapacity(1_000_000)).as("the explicit size of the one-argument form").isEqualTo(123);
      assertThat(index.computeGraphBuildCacheCapacity(1_000_000, 8L * 1024 * MB, 0L))
          .as("an explicit size is not capped by a heap reading of zero").isEqualTo(123);
      assertThat(index.computeGraphBuildCacheCapacity(5, 8L * 1024 * MB, 4L * 1024 * MB))
          .as("nor shrunk to a small corpus: it is what the operator asked for").isEqualTo(123);
    });
  }

  @Test
  void aZeroHeapShareForTheDeltaPayloadsBuffersNoPayload() {
    withIndex((db, index, vectors) -> {
      final ContextConfiguration configuration = db.getConfiguration();
      configuration.setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE, 0);
      configuration.setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_MAX_HEAP_PERCENT, 0);

      assertThat(index.computeDeltaPayloadCapacity(8L * 1024 * MB, 4L * 1024 * MB))
          .as("an automatic size with no heap share to take is a buffer that keeps no payload, whatever the heap reads")
          .isZero();

      configuration.setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_MAX_HEAP_PERCENT, 10);
      assertThat(index.computeDeltaPayloadCapacity(8L * 1024 * MB, 4L * 1024 * MB))
          .as("control: with a share of the heap the same readings buy payloads").isPositive();

      configuration.setValue(GlobalConfiguration.VECTOR_INDEX_DELTA_CACHE_SIZE, 7);
      assertThat(index.computeDeltaPayloadCapacity(8L * 1024 * MB, 0L)).as("an explicit size is the capacity").isEqualTo(7);
    });
  }

  @Test
  void anExplicitSearchCacheSizeSizesTheVectorCacheAndANegativeOneDisablesIt() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        final List<float[]> vectors = populate(db);
        final LSMVectorIndex index = vectorIndex(db);
        final ContextConfiguration configuration = db.getConfiguration();
        configuration.setValue(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE, 77);

        final FlatVectorCache cache = index.getSearchVectorCache();
        assertThat(cache).as("an explicit size creates the cache").isNotNull();
        assertThat(cache.capacity()).as("with exactly the entries asked for, not the heap's share").isEqualTo(77);
        assertThat(index.findNeighborsFromVector(vectors.get(5), 3, 64).getFirst().getFirst())
            .as("a search through a cache far smaller than the corpus still finds the exact match").isEqualTo(ridOf(db, 5));

        // The cache is decided again under the sizing lock: a setting that turned the cache off between the two decisions leaves
        // the cache the index holds in place, it does not drop it
        configuration.setValue(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE, 0);
        assertThat(index.getSearchVectorCache(() -> {
          configuration.setValue(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE, -1);
          return 8L * 1024 * MB;
        })).as("the decision under the lock found the cache disabled: the held cache is kept").isSameAs(cache);

        assertThat(index.getSearchVectorCache(() -> 8L * 1024 * MB)).as("a negative size disables the cache").isNull();
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void anAsyncRebuildWaitsForThePermitAnotherHolderTookFromTheJvmWideSemaphoreAndRunsOnceItIsReleased() {
    withIndex((db, index, vectors) -> {
      final Semaphore permits = LSMVectorIndex.rebuildSemaphoreForTesting();
      assertThat(permits.availablePermits()).as("precondition: no other test holds a rebuild permit").isPositive();
      final long deferredBefore = index.getStats().get("rebuildsDeferredForMemory");

      LSMVectorIndex.acquireAllRebuildPermitsForTest();
      try {
        assertThat(permits.availablePermits()).as("the accessor is the semaphore the permit hooks take").isZero();
        Internals.startAsyncGraphRebuild(index);
        assertThat(index.getStats().get("asyncRebuildInProgress")).as("the rebuild was started").isEqualTo(1L);

        // A rebuild that took a permit of its own would run to its end in this window and clear the flag; this one cannot get past
        // the held permit. The window only observes: it can make the test pass wrongly, never fail it
        for (int i = 0; i < 20; i++)
          sleepMillis(10);
        assertThat(index.getStats().get("asyncRebuildInProgress")).as("still waiting for the permit of the other holder").isEqualTo(1L);
      } finally {
        LSMVectorIndex.releaseAllRebuildPermitsForTest();
      }

      waitUntil(() -> index.getStats().get("asyncRebuildInProgress") == 0L, "the rebuild that got the released permit finishes");
      assertThat(permits.availablePermits()).as("and hands the permit back").isPositive();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).as("it was admitted, not deferred").isEqualTo(deferredBefore);
      assertEveryVectorIsItsOwnNearestNeighbour(index, vectors);
    });
  }

  // ---------------------------------------------------------------- the online gate reclaims the page cache for real

  @Test
  void anOnlineRebuildThatFitsOnlyByGivingUpPageCacheReallyGivesItUpAndIsAdmittedWithTheGraphOnTheHeap() {
    withIndex((db, index, vectors) -> {
      final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
      warmPageCache(db);
      final long cached = pageManager.getReadCacheRAM();
      assertThat(cached).as("precondition: reading the records left pages in the read cache").isPositive();

      final Sizes sizes = sizesOf(index);
      final long shortfall = sizes.boundedShortfall(sizes.estimateKeepingOnHeap(), cached / 2);
      final long availableHeap = sizes.availableLeavingShort(sizes.estimateKeepingOnHeap(), shortfall);

      assertThat(index.admitOnlineRebuild(availableHeap, cached, true))
          .as("the build fits once the cache gives back the shortfall, and the cache has it: admitted").isTrue();

      assertThat(pageManager.getReadCacheRAM())
          .as("admitted BECAUSE the cache counts as reclaimable, so it must really have been reclaimed before the build allocates")
          .isLessThanOrEqualTo(cached - shortfall);
      assertThat(index.getGraphIndex()).as("the cache sufficed, so the graph is not demoted").isInstanceOf(OnHeapGraphIndex.class);
      assertThat(index.getStats().get("residentGraphDemotions")).isZero();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isZero();
      assertEveryVectorIsItsOwnNearestNeighbour(index, vectors);
    });
  }

  @Test
  void aReclaimThatDeliversNothingFallsThroughToDemotingTheGraphInsteadOfAdmittingOnBytesItNeverGot() {
    withIndex((db, index, vectors) -> {
      final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
      final Sizes sizes = sizesOf(index);
      final long claimed = Math.max(2L, sizes.onHeapBytes());
      // Short of the on-heap shape by half of what the cache is claimed to hold. The credit of the on-heap graph's own bytes
      // pays for the demoted shape, so only the demotion can admit this build
      final long availableHeap = sizes.availableLeavingShort(sizes.estimateKeepingOnHeap(), claimed / 2);
      drainPageCache(pageManager);

      assertThat(index.admitOnlineRebuild(availableHeap, claimed, true))
          .as("the cache gave up nothing, so the build is not admitted as it stands, but it fits with the graph demoted")
          .isTrue();

      assertThat(index.getGraphIndex()).as("the admission came from the swap to the on-disk twin, not from bytes never freed")
          .isInstanceOf(OnDiskGraphIndex.class);
      assertThat(index.getStats().get("residentGraphDemotions")).isEqualTo(1L);
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isZero();
      assertEveryVectorIsItsOwnNearestNeighbour(index, vectors);
    });
  }

  @Test
  void aDemotionThatNeedsPageCacheReallyReclaimsItAndAdmitsTheRebuild() {
    withIndex((db, index, vectors) -> {
      final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
      warmPageCache(db);
      final Sizes sizes = sizesOf(index);
      final long claimed = Math.max(2L, sizes.onHeapBytes());
      final long cached = pageManager.getReadCacheRAM();

      // The on-heap shape does not fit even with all the claimed cache (so the first stage declines), and the demoted shape fits
      // with the on-heap graph's bytes credited and some of the claimed cache given up. The net heap reading is exactly zero, not clamped
      // from below; the reclaim asserted below is the one the gate computes for the demoted shape, not a figure of the test's own
      final long estimateDemoted = sizes.estimateKeepingOnDisk();
      final long availableHeap = sizes.availableLeavingShort(estimateDemoted, sizes.neededAvailable(estimateDemoted));
      final long netAvailable = availableHeap - index.searchCachesPlannedBytes();
      assertThat(VectorHeapBudget.reclaimNeededFor(sizes.estimateKeepingOnHeap(), sizes.percent(), netAvailable, claimed))
          .as("precondition: the first stage declines").isEqualTo(-1L);
      final long reclaimedByTheGate = VectorHeapBudget.reclaimNeededFor(estimateDemoted, sizes.percent(),
          Math.min(netAvailable + sizes.onHeapBytes(), VectorHeapBudget.maxHeapBytes()), claimed);
      assertThat(reclaimedByTheGate).as("precondition: the demoted shape needs page cache, and the claim covers it").isPositive();
      assertThat(cached).as("precondition: the cache holds what the demotion will ask of it").isGreaterThanOrEqualTo(reclaimedByTheGate);

      assertThat(index.admitOnlineRebuild(availableHeap, claimed, true))
          .as("the demoted shape fits after reclaiming part of the claimed cache, which the cache holds").isTrue();

      assertThat(index.getGraphIndex()).isInstanceOf(OnDiskGraphIndex.class);
      assertThat(index.getStats().get("residentGraphDemotions")).isEqualTo(1L);
      assertThat(pageManager.getReadCacheRAM())
          .as("the demotion was admitted on page cache it was told it could have, so at least what it asked for must have been given up")
          .isLessThanOrEqualTo(cached - reclaimedByTheGate);
      assertEveryVectorIsItsOwnNearestNeighbour(index, vectors);
    });
  }

  @Test
  void aDemotionWhoseReclaimFallsShortDefersTheRebuildAndLeavesTheGraphOnTheHeap() {
    withIndex((db, index, vectors) -> {
      final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
      // A build cache this large makes the build need more heap than any page cache of a test holds
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE, 10_000_000);
      final Sizes sizes = sizesOf(index);
      // Claims far more cache than the database has, against no heap at all: both stages ask for bytes the cache cannot give
      final long claimed = 64L * 1024 * MB;
      drainPageCache(pageManager);

      assertThat(index.admitOnlineRebuild(0L, claimed, true))
          .as("neither the first stage nor the demotion got the bytes it was admitted on: deferred").isFalse();

      assertThat(index.getGraphIndex()).as("a refused demotion must not have swapped the graph")
          .isInstanceOf(OnHeapGraphIndex.class);
      assertThat(index.getStats().get("residentGraphDemotions")).isZero();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isEqualTo(1L);
      assertThat(sizes.onHeapBytes()).isPositive();
      assertEveryVectorIsItsOwnNearestNeighbour(index, vectors);
    });
  }

  @Test
  void aPersistedGraphThatCannotBeReadBackIsNotDemotedToAndTheRebuildIsDeferred() {
    withIndex((db, index, vectors) -> {
      // A recorded length that outlived the pages it described: the load answers no graph at all
      Internals.writeLong(index.getGraphFile(), "lastWrittenGraphBytes", 1L << 40);

      assertThat(index.admitOnlineRebuild(0L, 0L, true)).as("no certified twin to swap in: deferred").isFalse();

      assertThat(index.getGraphIndex()).isInstanceOf(OnHeapGraphIndex.class);
      assertThat(index.getStats().get("residentGraphDemotions")).isZero();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isEqualTo(1L);
      assertEveryVectorIsItsOwnNearestNeighbour(index, vectors);
    });
  }

  @Test
  void aPersistedGraphWhoseLoadFailsIsNotDemotedToAndTheRebuildIsDeferred() {
    withIndex((db, index, vectors) -> {
      // A recorded length shorter than the payload: the footer of the graph is not where the load looks for it
      Internals.writeLong(index.getGraphFile(), "lastWrittenGraphBytes", 100L);

      assertThat(index.admitOnlineRebuild(0L, 0L, true)).as("the twin cannot be loaded: deferred, not an exception").isFalse();
      assertThat(index.admitOnlineRebuild(0L, 0L, true)).as("and again on the next trigger").isFalse();

      assertThat(index.getGraphIndex()).isInstanceOf(OnHeapGraphIndex.class);
      assertThat(index.getStats().get("residentGraphDemotions")).isZero();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isEqualTo(2L);
      assertEveryVectorIsItsOwnNearestNeighbour(index, vectors);
    });
  }

  @Test
  void aBuildThatPublishedAnotherGraphWhileTheDemotionWaitedForTheLockIsNotOverwrittenByTheTwin() {
    withIndex((db, index, vectors) -> {
      final Sizes sizes = sizesOf(index);
      final long availableHeap = sizes.availableLeavingShort(sizes.estimateKeepingOnHeap(), 1L);
      final ImmutableGraphIndex other;
      try {
        other = index.getGraphFile().loadGraph();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
      assertThat(other).isNotNull();

      final ReentrantReadWriteLock lock = Internals.lockOf(index);
      final AtomicReference<Boolean> admitted = new AtomicReference<>();
      final Thread admitter = new Thread(() -> admitted.set(index.admitOnlineRebuild(availableHeap, 0L, true)));
      lock.writeLock().lock();
      try {
        admitter.start();
        waitUntil(lock::hasQueuedThreads, "the demotion decided and is waiting to swap");
        // The build publishes its graph in the window between the decision and the swap
        Internals.write(index, "graphIndex", other);
      } finally {
        lock.writeLock().unlock();
      }
      joinQuietly(admitter);

      assertThat(admitted.get()).as("the decision was about the graph the build replaced: not admitted on it").isFalse();
      assertThat(index.getGraphIndex()).as("the published graph stays").isSameAs(other);
      assertThat(index.getStats().get("residentGraphDemotions")).isZero();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isEqualTo(1L);
    });
  }

  @Test
  void aResidentGraphWhoseMemoryCannotBeMeasuredIsChargedTheFlatPerNodeEstimate() {
    withIndex((db, index, vectors) -> {
      final int nodes = index.getGraphIndex().getIdUpperBound();
      Internals.write(index, "graphIndex", graphWhoseMemoryCannotBeMeasured(nodes));
      final int percent = Math.min(
          db.getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT), 90);
      final long cache = index.computeGraphBuildCacheCapacity(nodes);
      final float overflow = index.metadata.neighborOverflowFactor;
      final long flat = VectorHeapBudget.estimateGraphBytes(nodes);
      final long withFlat = VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, DIMENSIONS, cache, flat, nodes, false, overflow);
      final long withNothing = VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, DIMENSIONS, cache, 0L, nodes, false, overflow);
      assertThat(withFlat).as("precondition: the flat estimate of the graph is part of the price").isGreaterThan(withNothing);
      final long neededWithFlat = (withFlat * 100 + percent - 1) / percent;
      final long neededWithNothing = (withNothing * 100 + percent - 1) / percent;
      assertThat(neededWithNothing).as("precondition: a heap can sit between the two").isLessThan(neededWithFlat);

      assertThat(index.admitOnlineRebuild(neededWithFlat - 1 + index.searchCachesPlannedBytes(), 0L, true))
          .as("one byte short of the price with the flat estimate: a graph that cannot say what it costs is not free").isFalse();
      assertThat(index.admitOnlineRebuild(neededWithFlat + index.searchCachesPlannedBytes(), 0L, true))
          .as("control: the heap that covers the flat estimate admits it").isTrue();
    });
  }

  @Test
  void aRefusedSearchBuildLeavesTheOnDiskGraphUnresolvedForASessionThatWroteFirst() {
    final int count = 1_200;
    final List<float[]> vectors = populateAndCrashWithoutPersistingAGraph(count);
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        db.transaction(() -> db.newDocument("Doc").set("id", count).set("vector", randomVector(new Random(5))).save());
        assertThat(index.graphNotYetMaterialisedForTest()).as("precondition: the write left no graph resolved").isTrue();

        index.pinSearchBuildHeapReadingForTest(MB, 0L);
        assertThat(catchThrowable(() -> index.findNeighborsFromVector(vectors.get(7), 5, 64)))
            .isInstanceOf(VectorGraphBuildRefusedException.class);

        assertThat(index.graphNotYetMaterialisedForTest())
            .as("the gate refused before the build settled anything: the question stays open for the next search").isTrue();
        assertThat(Internals.read(index, "searchBuildThread")).as("the refused build handed its thread slot back").isNull();

        index.pinSearchBuildHeapReadingForTest(-1L, -1L);
        assertThat(index.findNeighborsFromVector(vectors.get(7), 5, 64).getFirst().getFirst()).isEqualTo(ridOf(db, 7));
        assertThat(index.graphNotYetMaterialisedForTest()).isFalse();
        assertThat(Internals.read(index, "searchBuildThread")).as("and so did the build that ran").isNull();
      } finally {
        db.drop();
      }
    }
  }

  // ---------------------------------------------------------------- the cooldown after a deferral ends

  @Test
  void aDeferredRebuildIsSkippedWhileTheCooldownRunsAndRetriedOnceItHasElapsed() {
    withIndex((db, index, vectors) -> {
      final ContextConfiguration configuration = db.getConfiguration();
      // An hour: no clock of a test can run it out by itself, so the outcome does not depend on how long a step takes
      configuration.setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_DEFERRAL_COOLDOWN_MS, 3_600_000L);
      // A build cache this large makes the real gate decline whatever the heap reads
      configuration.setValue(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE, Integer.MAX_VALUE / 2);
      assertThat(index.admitOnlineRebuild(0L, 0L, true)).as("precondition: a deferral starts the cooldown").isFalse();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isEqualTo(1L);

      Internals.startAsyncGraphRebuild(index);
      assertThat(index.getStats().get("asyncRebuildInProgress")).as("inside the cooldown no thread is started").isZero();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).as("and the gate is not asked again").isEqualTo(1L);

      // The cooldown is over once the deferral is older than it: the same trigger now reaches the gate
      Internals.writeLong(index, "lastRebuildDeferralMs", System.currentTimeMillis() - 7_200_000L);
      Internals.startAsyncGraphRebuild(index);
      waitUntil(() -> index.getStats().get("asyncRebuildInProgress") == 0L, "the retried rebuild reaches the gate and ends");

      assertThat(index.getStats().get("rebuildsDeferredForMemory")).as("the retry reached the gate, which declined again").isEqualTo(2L);
      assertThat(index.getGraphIndex()).isInstanceOf(OnHeapGraphIndex.class);
    });
  }

  // ---------------------------------------------------------------- the build a search decides on

  @Test
  void aSearchReceivesTheErrorItsBuildFailedWithAsItIsAndTheNextSearchBuildsTheWholeCorpus() {
    final int count = 1_200;
    final List<float[]> vectors = populateAndCrashWithoutPersistingAGraph(count);
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        index.setAfterLiveSetReadForTest(() -> {
          throw new OutOfMemoryError("injected into the graph build a search started");
        });

        final Throwable thrown = catchThrowable(() -> index.findNeighborsFromVector(vectors.get(7), 5, 64));

        assertThat(thrown).as("an Error stays the Error it was: the search is not told about an IndexException wrapper")
            .isInstanceOf(OutOfMemoryError.class).hasMessageContaining("injected");
        assertThat(thrown.getSuppressed()).as("with the frames of the waiting search attached").isNotEmpty();
        assertThat(index.graphNotYetMaterialisedForTest()).as("nothing was built").isTrue();

        index.setAfterLiveSetReadForTest(null);
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(vectors.get(7), 5, 64);
        assertThat(results).hasSize(5);
        assertThat(results.getFirst().getFirst()).as("the exact match of the whole corpus").isEqualTo(ridOf(db, 7));
        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) count);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aSearchReceivesTheRuntimeExceptionItsBuildFailedWithAsItIs() {
    final int count = 1_100;
    final List<float[]> vectors = populateAndCrashWithoutPersistingAGraph(count);
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        index.setAfterLiveSetReadForTest(() -> {
          throw new IllegalStateException("injected into the graph build a search started");
        });

        assertThat(catchThrowable(() -> index.findNeighborsFromVector(vectors.get(3), 5, 64)))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("injected");

        index.setAfterLiveSetReadForTest(null);
        assertThat(index.findNeighborsFromVector(vectors.get(3), 5, 64).getFirst().getFirst()).isEqualTo(ridOf(db, 3));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aSearchInterruptedWhileItWaitsForTheBuildItStartedKeepsItsInterruptAndTheBuildStillCompletes() {
    final int count = 1_200;
    final List<float[]> vectors = populateAndCrashWithoutPersistingAGraph(count);
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        index.setAfterLiveSetReadForTest(() -> {
          entered.countDown();
          awaitQuietly(release);
        });
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean interruptKept = new AtomicBoolean();
        try {
          final Thread searcher = new Thread(() -> {
            try {
              index.findNeighborsFromVector(vectors.get(9), 5, 64);
            } catch (final Throwable t) {
              failure.set(t);
              interruptKept.set(Thread.currentThread().isInterrupted());
            }
          });
          searcher.start();
          try {
            assertThat(entered.await(30, TimeUnit.SECONDS)).as("the build the search started is running").isTrue();
            searcher.interrupt();
            searcher.join(30_000);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }

          assertThat(failure.get()).as("the interrupted search is told so").isInstanceOf(IndexException.class)
              .hasMessageContaining("Interrupted while waiting for the graph of vector index");
          assertThat(interruptKept.get()).as("and the thread keeps its interrupt for whoever owns it").isTrue();
        } finally {
          // The paused build goes on whatever the assertions above found: the database is not dropped under it
          index.setAfterLiveSetReadForTest(null);
          release.countDown();
        }

        waitUntil(() -> index.getStats().get("graphNodeCount") == count, "the build the search left behind goes on and publishes");
        assertThat(index.findNeighborsFromVector(vectors.get(9), 5, 64).getFirst().getFirst()).isEqualTo(ridOf(db, 9));
        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) count);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aSecondSearchInterruptedWhileTheGraphBuildLockIsHeldKeepsItsInterrupt() {
    final int count = 1_100;
    final List<float[]> vectors = populateAndCrashWithoutPersistingAGraph(count);
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        index.setAfterLiveSetReadForTest(() -> {
          entered.countDown();
          awaitQuietly(release);
        });
        final AtomicReference<List<Pair<RID, Float>>> firstAnswer = new AtomicReference<>();
        final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        final Thread first = new Thread(() -> {
          try {
            firstAnswer.set(index.findNeighborsFromVector(vectors.get(2), 5, 64));
          } catch (final Throwable t) {
            firstFailure.set(t);
          }
        });
        first.start();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicBoolean interruptKept = new AtomicBoolean();
        try {
          assertThat(entered.await(30, TimeUnit.SECONDS)).as("the first search's build holds the graph build lock").isTrue();
          assertThat(index.graphBuildLockHeldForTest()).isTrue();
          final Thread second = new Thread(() -> {
            try {
              index.findNeighborsFromVector(vectors.get(4), 5, 64);
            } catch (final Throwable t) {
              failure.set(t);
              interruptKept.set(Thread.currentThread().isInterrupted());
            }
          });
          second.start();
          waitUntil(() -> second.getState() == Thread.State.TIMED_WAITING, "the second search parks on the graph build lock");
          second.interrupt();
          second.join(30_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } finally {
          index.setAfterLiveSetReadForTest(null);
          release.countDown();
        }

        assertThat(failure.get()).as("the second search gave up its wait for the lock").isInstanceOf(IndexException.class)
            .hasMessageContaining("graph build lock");
        assertThat(interruptKept.get()).isTrue();
        joinQuietly(first);
        assertThat(first.isAlive()).as("the first search finished once its build was released").isFalse();
        assertThat(firstFailure.get()).as("and the interrupt of the second one did not fail it").isNull();
        assertThat(firstAnswer.get()).as("it answered from the graph it built").hasSize(5);
        assertThat(firstAnswer.get().getFirst().getFirst()).isEqualTo(ridOf(db, 2));
        assertThat(index.findNeighborsFromVector(vectors.get(4), 5, 64).getFirst().getFirst()).isEqualTo(ridOf(db, 4));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aSearchBuildThatFitsOnlyByGivingUpPageCacheReallyGivesItUpAndRunsWithItsFullCache() {
    withIndex((db, index, vectors) -> {
      final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
      warmPageCache(db);
      final long cached = pageManager.getReadCacheRAM();
      assertThat(cached).isPositive();
      final int percent = db.getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT);
      final long estimate = VectorHeapBudget.estimateRebuildHeapBytes(NUM_VECTORS, DIMENSIONS, BUILD_CACHE, false);
      final long needed = (estimate * 100 + Math.min(percent, 90) - 1) / Math.min(percent, 90);
      // At most half of what the build needs, so that the heap reading is never clamped at zero and the gate asks for exactly this
      final long shortfall = Math.max(1L, Math.min(cached / 2, needed / 2));
      final long availableHeap = needed - shortfall + index.searchCachesPlannedBytes();

      assertThat(index.fitSearchBuildToHeap(NUM_VECTORS, availableHeap, cached))
          .as("the full build fits once the cache gives back the shortfall: no cap on the build cache").isZero();

      assertThat(pageManager.getReadCacheRAM()).as("the bytes the build was admitted on were really given up")
          .isLessThanOrEqualTo(cached - shortfall);
      assertThat(index.getStats().get("searchRebuildsWithReducedCache")).isZero();
      assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isZero();
    });
  }

  @Test
  void aSearchBuildWhoseReclaimDeliversNothingIsRefusedInsteadOfRunningOnBytesItNeverGot() {
    withIndex((db, index, vectors) -> {
      final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
      drainPageCache(pageManager);

      assertThat(catchThrowable(() -> index.fitSearchBuildToHeap(NUM_VECTORS, 0L, 64L * 1024 * MB)))
          .as("claimed cache that is not there: judged again against what the reclaim delivered, which is nothing")
          .isInstanceOf(VectorGraphBuildRefusedException.class);
      assertThat(index.getStats().get("searchRebuildsRefusedForMemory")).isEqualTo(1L);
    });
  }

  // ---------------------------------------------------------------- fixture

  private interface IndexBody {
    void accept(Database db, LSMVectorIndex index, List<float[]> vectors);
  }

  /** A database with a built, persisted graph: what a search of an index of {@link #NUM_VECTORS} vectors leaves. */
  private void withIndex(final IndexBody body) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE, BUILD_CACHE);
        final List<float[]> vectors = populate(db);
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.findNeighborsFromVector(vectors.get(0), 5, 64)).hasSize(5);
        assertThat(index.persistedGraphSourceForTest()).as("the build persisted the graph it published").isNotNull();
        body.accept(db, index, vectors);
      } finally {
        db.drop();
      }
    }
  }

  private List<float[]> populate(final Database db) {
    final Random random = new Random(0x5EEDL);
    final List<float[]> vectors = new ArrayList<>();
    db.transaction(() -> {
      final DocumentType type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\" }");
    });
    db.begin();
    for (int i = 0; i < NUM_VECTORS; i++) {
      final float[] vector = randomVector(random);
      vectors.add(vector);
      db.newDocument("Doc").set("id", i).set("vector", vector).save();
    }
    db.commit();
    return vectors;
  }

  /** Writes the corpus, then kills the database before any graph is persisted: the reopened index has vectors and no graph. */
  private List<float[]> populateAndCrashWithoutPersistingAGraph(final int count) {
    final Random random = new Random(0xC0FFEEL);
    final List<float[]> vectors = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.begin();
        for (int i = 0; i < count; i++) {
          final float[] vector = randomVector(random);
          vectors.add(vector);
          db.newDocument("Doc").set("id", i).set("vector", vector).save();
          if (i % 500 == 499) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
    return vectors;
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat() - 0.5f;
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }

  private static RID ridOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      assertThat(rs.hasNext()).as("record %d must exist", id).isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  /** Reads every record back so the pages of the type and of the index sit in the process-wide read cache. */
  private static void warmPageCache(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT id, vector FROM Doc")) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private static void drainPageCache(final PageManager pageManager) {
    pageManager.reclaimReadCacheRAM(Long.MAX_VALUE / 2);
    assertThat(pageManager.getReadCacheRAM()).as("precondition: the read cache is empty").isZero();
  }

  /**
   * Exact kNN oracle: for sampled stored vectors asked for as the query, the ordered top three RIDs equal the brute-force cosine
   * ranking over every stored vector (the query itself first), and the distances are the cosine distances (1 - cosine), within a
   * two-sided tolerance. The beam is as wide as the corpus, so the graph search is exhaustive and the ranking is exact.
   */
  private static void assertEveryVectorIsItsOwnNearestNeighbour(final LSMVectorIndex index, final List<float[]> vectors) {
    final Random random = new Random(42);
    final RidToId ridToId = new RidToId(index);
    for (int n = 0; n < 12; n++) {
      final int id = random.nextInt(vectors.size());
      final float[] query = vectors.get(id);
      final double[] cosine = new double[vectors.size()];
      final Integer[] rank = new Integer[vectors.size()];
      for (int i = 0; i < cosine.length; i++) {
        cosine[i] = cosine(query, vectors.get(i));
        rank[i] = i;
      }
      Arrays.sort(rank, (x, y) -> Double.compare(cosine[y], cosine[x]));

      final List<Pair<RID, Float>> results = index.findNeighborsFromVector(query, 3, vectors.size());
      assertThat(results).as("kNN of the stored vector %d", id).hasSize(3);
      for (int r = 0; r < 3; r++) {
        assertThat(ridToId.idOf(results.get(r).getFirst())).as("rank %d of the query of vector %d", r, id).isEqualTo(rank[r]);
        assertThat((double) results.get(r).getSecond()).as("distance of rank %d of vector %d", r, id)
            .isCloseTo(1d - cosine[rank[r]], within(1e-4d));
      }
    }
  }

  private static double cosine(final float[] a, final float[] b) {
    double dot = 0d;
    double na = 0d;
    double nb = 0d;
    for (int d = 0; d < a.length; d++) {
      dot += (double) a[d] * b[d];
      na += (double) a[d] * a[d];
      nb += (double) b[d] * b[d];
    }
    return dot / Math.sqrt(na * nb);
  }

  /** RID to id of the documents of the index's database. */
  private static final class RidToId {
    private final Map<RID, Integer> ids = new HashMap<>();

    RidToId(final LSMVectorIndex index) {
      final Database db = index.getDatabase();
      try (final ResultSet rs = db.query("sql", "SELECT @rid AS rid, id FROM Doc")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          ids.put(row.<RID>getProperty("rid"), ((Number) row.getProperty("id")).intValue());
        }
      }
    }

    int idOf(final RID rid) {
      return ids.getOrDefault(rid, -1);
    }
  }

  /** The numbers the gate compares, derived with the same pieces it uses so the test pins no magic constants. */
  private record Sizes(LSMVectorIndex index, long onHeapBytes, long onDiskBytes, long nodes, long buildCacheCapacity,
                       float overflow, int percent) {
    long estimateKeepingOnHeap() {
      return VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, DIMENSIONS, buildCacheCapacity, onHeapBytes, nodes, true,
          overflow);
    }

    long estimateKeepingOnDisk() {
      return VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, DIMENSIONS, buildCacheCapacity, onHeapBytes, nodes, true,
          overflow, onDiskBytes);
    }

    /**
     * The available-heap reading that leaves {@code estimate} exactly {@code shortfall} bytes short of what the gate needs. The
     * gate takes the database's search caches' plan off the reading, so the reading carries it on top.
     */
    long availableLeavingShort(final long estimate, final long shortfall) {
      final int p = Math.min(percent, 90);
      final long needed = (estimate * 100 + p - 1) / p;
      assertThat(shortfall).as("a shortfall past what the estimate needs would be clamped: the gate would ask for less").isLessThanOrEqualTo(needed);
      return Math.max(0L, needed - shortfall) + index.searchCachesPlannedBytes();
    }

    /** The available heap at which {@code estimate} fits {@code percent} of it: the largest shortfall that is not clamped. */
    long neededAvailable(final long estimate) {
      final int p = Math.min(percent, 90);
      return (estimate * 100 + p - 1) / p;
    }

    /** {@code wanted} bytes of shortfall, cut to half of what the estimate needs and to at least one byte. */
    long boundedShortfall(final long estimate, final long wanted) {
      final int p = Math.min(percent, 90);
      final long needed = (estimate * 100 + p - 1) / p;
      return Math.max(1L, Math.min(wanted, needed / 2));
    }
  }

  private static Sizes sizesOf(final LSMVectorIndex index) {
    final ImmutableGraphIndex resident = index.getGraphIndex();
    assertThat(resident).as("the fixture must hold the freshly built on-heap graph").isInstanceOf(OnHeapGraphIndex.class);
    final long onHeap = resident.ramBytesUsed();
    final long onDisk;
    try (final OnDiskGraphIndex twin = index.getGraphFile().loadGraph()) {
      assertThat(twin).as("the build persists the graph it publishes").isNotNull();
      onDisk = twin.ramBytesUsed();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    final long nodes = resident.getIdUpperBound();
    final Sizes sizes = new Sizes(index, onHeap, onDisk, nodes, index.computeGraphBuildCacheCapacity((int) nodes),
        index.metadata.neighborOverflowFactor,
        index.getDatabase().getConfiguration().getValueAsInteger(GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT));
    assertThat(sizes.estimateKeepingOnDisk()).as("the premise of the fallback: the twin is cheaper to keep")
        .isLessThan(sizes.estimateKeepingOnHeap());
    return sizes;
  }

  /** Every reach into a private member, in one place: a rename fails the test with a message that says what to update. */
  private static final class Internals {
    private static Field field(final Class<?> owner, final String name) {
      try {
        final Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (final NoSuchFieldException e) {
        throw new AssertionError(owner.getSimpleName() + "." + name + " was renamed or removed: update VectorMemoryPressureOracleTest.Internals", e);
      }
    }

    static Object read(final Object target, final String name) {
      try {
        return field(target.getClass(), name).get(target);
      } catch (final IllegalAccessException e) {
        throw new AssertionError("cannot read " + name, e);
      }
    }

    static void write(final Object target, final String name, final Object value) {
      try {
        field(target.getClass(), name).set(target, value);
      } catch (final IllegalAccessException e) {
        throw new AssertionError("cannot write " + name, e);
      }
    }

    static void writeLong(final Object target, final String name, final long value) {
      try {
        field(target.getClass(), name).setLong(target, value);
      } catch (final IllegalAccessException e) {
        throw new AssertionError("cannot write " + name, e);
      }
    }

    static ReentrantReadWriteLock lockOf(final LSMVectorIndex index) {
      return (ReentrantReadWriteLock) read(index, "lock");
    }

    static void startAsyncGraphRebuild(final LSMVectorIndex index) {
      try {
        final Method start = LSMVectorIndex.class.getDeclaredMethod("startAsyncGraphRebuild");
        start.setAccessible(true);
        start.invoke(index);
      } catch (final NoSuchMethodException e) {
        throw new AssertionError("LSMVectorIndex.startAsyncGraphRebuild was renamed or removed: update VectorMemoryPressureOracleTest.Internals", e);
      } catch (final ReflectiveOperationException e) {
        throw new AssertionError("startAsyncGraphRebuild failed", e);
      }
    }
  }

  /** A resident graph of {@code nodes} nodes that cannot account for its memory, as a future implementation might. */
  private static ImmutableGraphIndex graphWhoseMemoryCannotBeMeasured(final int nodes) {
    return (ImmutableGraphIndex) Proxy.newProxyInstance(ImmutableGraphIndex.class.getClassLoader(),
        new Class<?>[] { ImmutableGraphIndex.class }, (proxy, method, args) -> {
          switch (method.getName()) {
          case "ramBytesUsed":
            throw new IllegalStateException("cannot measure");
          case "getIdUpperBound", "size":
            return nodes;
          case "hashCode":
            return System.identityHashCode(proxy);
          case "equals":
            return proxy == args[0];
          case "toString":
            return "graph whose memory cannot be measured";
          default:
            final Class<?> type = method.getReturnType();
            if (type == boolean.class)
              return false;
            if (type == int.class)
              return 0;
            if (type == long.class)
              return 0L;
            if (type == double.class)
              return 0d;
            return null;
          }
        });
  }

  /** Polls {@code condition} with a short sleep; the deadline only detects a hang, no outcome depends on how long it takes. */
  private static void waitUntil(final BooleanSupplier condition, final String what) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline)
        throw new AssertionError("timed out waiting for: " + what);
      sleepMillis(2);
    }
  }

  private static void awaitQuietly(final CountDownLatch latch) {
    try {
      latch.await(60, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void joinQuietly(final Thread thread) {
    try {
      thread.join(20_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void sleepMillis(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
