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
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7260: at 10M vectors the rebuild after an admitted one was deferred again, because the
 * estimate charged the freshly built ON-HEAP graph as resident for the whole next build, and once the delta buffer
 * had grown past ~10% of the corpus no heap of about 2x the graph could ever admit it - so the delta scan grew
 * without bound.
 * <p>
 * Every graph a build publishes is persisted right after, so a resident on-heap graph has a certified on-disk twin
 * with the same ordinals. When the estimate does not fit with the on-heap graph kept, the gate now swaps in that
 * twin (searches keep working, reading the topology from pages) and re-prices the build against what the twin
 * costs, instead of deferring indefinitely. The heap figures are pinned through the package-private overload, so
 * the 24 GB shape needs no 24 GB fixture.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7260ResidentGraphDemotionTest {
  private static final String DB_PATH     = "target/test-databases/Issue7260ResidentGraphDemotionTest";
  private static final int    DIMENSIONS  = 16;
  private static final int    NUM_VECTORS = 300;
  private static final int    BUILD_CACHE = 64;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(BUILD_CACHE);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getDefValue());
    GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT.setValue(
        GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT.getDefValue());
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aRebuildThatOnlyFitsWithoutTheOnHeapGraphIsAdmittedByDemotingIt() {
    withIndex(index -> {
      final Sizes sizes = sizesOf(index);
      final List<RID> before = searchIds(index);

      // Short of what the on-heap shape needs, by more than the demotion hands back.
      final long availableHeap = sizes.availableHeapJustBelow(sizes.estimateKeepingOnHeap());

      assertThat(index.admitOnlineRebuild(availableHeap, 0L, true))
          .as("the build fits once the on-heap graph is swapped for its on-disk twin, so it must be admitted "
              + "rather than deferred - a deferral is what left the delta scan unbounded in issue #7260")
          .isTrue();

      assertThat(index.getGraphIndex())
          .as("the resident graph is now the persisted twin: its topology no longer occupies the heap")
          .isInstanceOf(OnDiskGraphIndex.class);
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isZero();
      assertThat(index.getStats().get("residentGraphDemotions")).isEqualTo(1L);
      assertThat(searchIds(index))
          .as("searches keep answering from the demoted graph with the same ordinals")
          .isEqualTo(before);
    });
  }

  @Test
  void theRebuildAfterADemotionCompletesAndReplacesTheOnDiskGraph() {
    withIndex(index -> {
      final Sizes sizes = sizesOf(index);
      assertThat(index.admitOnlineRebuild(sizes.availableHeapJustBelow(sizes.estimateKeepingOnHeap()), 0L, true)).isTrue();
      final ImmutableGraphIndex demoted = index.getGraphIndex();
      assertThat(demoted).isInstanceOf(OnDiskGraphIndex.class);

      // A real trigger, against the real heap: the demoted graph is cheap to keep, so it is admitted and runs.
      try {
        final Method start = LSMVectorIndex.class.getDeclaredMethod("startAsyncGraphRebuild");
        start.setAccessible(true);
        start.invoke(index);
      } catch (final ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
      final long deadline = System.currentTimeMillis() + 30_000;
      while (index.getStats().get("asyncRebuildInProgress") != 0 && System.currentTimeMillis() < deadline)
        Thread.onSpinWait();

      assertThat(index.getStats().get("asyncRebuildInProgress")).isZero();
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isZero();
      assertThat(index.getGraphIndex())
          .as("the rebuild published its replacement, so the demotion is not sticky")
          .isNotSameAs(demoted)
          .isInstanceOf(OnHeapGraphIndex.class);
      assertThat(searchIds(index)).hasSize(5);
    });
  }

  @Test
  void noCreditIsGivenForAnOnHeapGraphTheHeapReadingNeverCounted() {
    withIndex(index -> {
      final Sizes sizes = sizesOf(index);

      // Short of even the demoted shape: only a credit for the on-heap graph's bytes could make it fit.
      final long availableHeap = sizes.availableHeapJustBelow(sizes.estimateKeepingOnDisk());
      assertThat(index.admitOnlineRebuild(availableHeap, 0L, true))
          .as("control: with the credit the same reading is admitted")
          .isTrue();
    });
    withIndex(index -> {
      final Sizes sizes = sizesOf(index);
      assertThat(index.admitOnlineRebuild(sizes.availableHeapJustBelow(sizes.estimateKeepingOnDisk()), 0L, false))
          .as("no collection ended since the graph was published, so crediting its bytes would count them twice")
          .isFalse();
      assertThat(index.getGraphIndex()).isInstanceOf(OnHeapGraphIndex.class);
    });
  }

  @Test
  void aRebuildThatFitsWithTheOnHeapGraphKeepsIt() {
    withIndex(index -> {
      assertThat(index.admitOnlineRebuild(Long.MAX_VALUE / 4, 0L, true)).isTrue();

      assertThat(index.getGraphIndex())
          .as("demotion costs the search latency of reading pages: it is a fallback, never the first choice")
          .isInstanceOf(OnHeapGraphIndex.class);
      assertThat(index.getStats().get("residentGraphDemotions")).isZero();
    });
  }

  @Test
  void aRebuildThatDoesNotFitEvenDemotedIsDeferredAndTheGraphStaysOnHeap() {
    withIndex(index -> {
      final List<RID> before = searchIds(index);
      assertThat(index.admitOnlineRebuild(0L, 0L, true)).isFalse();
      assertThat(searchIds(index))
          .as("declining closes the twin it loaded on the side, which must not disturb the live graph")
          .isEqualTo(before);

      assertThat(index.getGraphIndex())
          .as("a deferral must not have degraded the searches it was meant to protect")
          .isInstanceOf(OnHeapGraphIndex.class);
      assertThat(index.getStats().get("rebuildsDeferredForMemory")).isEqualTo(1L);
      assertThat(index.getStats().get("residentGraphDemotions")).isZero();
    });
  }

  @Test
  void aGraphThatWasNeverPersistedIsNotDemoted() {
    withIndex(index -> {
      final Sizes sizes = sizesOf(index);
      index.forgetPersistedGraphSourceForTest();

      assertThat(index.admitOnlineRebuild(sizes.availableHeapJustBelow(sizes.estimateKeepingOnHeap()), 0L, true))
          .as("with no certified twin there is nothing to swap in, and guessing one would serve wrong ordinals")
          .isFalse();
      assertThat(index.getGraphIndex()).isInstanceOf(OnHeapGraphIndex.class);
    });
  }

  /** The numbers the gate compares, derived with the same pieces it uses so the test pins no magic constants. */
  private record Sizes(long onHeapBytes, long onDiskBytes, long nodes, long buildCacheCapacity, int dimensions,
                       float overflow, int percent) {
    long estimateKeepingOnHeap() {
      return VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, dimensions, buildCacheCapacity, onHeapBytes, nodes,
          true, overflow);
    }

    long estimateKeepingOnDisk() {
      return VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, dimensions, buildCacheCapacity, onHeapBytes,
          nodes, true, overflow, onDiskBytes);
    }

    /** An available-heap reading the on-heap estimate overshoots by a margin, but the demoted one fits. */
    long availableHeapJustBelow(final long estimate) {
      final long needed = (estimate * 100 + percent - 1) / percent;
      return Math.max(1L, needed - Math.max(1L, needed / 100));
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
    final int percent = GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT.getValueAsInteger();
    final long nodes = resident.getIdUpperBound();
    final Sizes sizes = new Sizes(onHeap, onDisk, nodes, index.computeGraphBuildCacheCapacity((int) nodes),
        DIMENSIONS, index.metadata.neighborOverflowFactor, percent);
    assertThat(sizes.estimateKeepingOnDisk())
        .as("the premise of the fallback: the twin must be cheaper to keep than the on-heap graph")
        .isLessThan(sizes.estimateKeepingOnHeap());
    return sizes;
  }

  private static List<RID> searchIds(final LSMVectorIndex index) {
    return index.findNeighborsFromVector(queryVector(), 5, 64).stream().map(Pair::getFirst).toList();
  }

  private void withIndex(final Consumer<LSMVectorIndex> body) {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        populate(db);
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        body.accept(index);
      } finally {
        db.drop();
      }
    }
  }

  private static void populate(final Database db) {
    final Random rnd = new Random(7);
    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\" }");
    });
    db.begin();
    for (int i = 0; i < NUM_VECTORS; i++)
      db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
    db.commit();
  }

  private static float[] randomVector(final Random rnd) {
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = rnd.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }

  private static float[] queryVector() {
    return randomVector(new Random(99));
  }
}
