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
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.VectorGraphBuildRefusedException;
import com.arcadedb.index.IndexException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.graph.ImmutableGraphIndex;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Heap budget and admission control of the graph build. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorBuildMemoryTest {
  /**
   * Regression for #6503: graph persist out of memory.
   * <p>
   * Regression test for issue #6503: an {@link OutOfMemoryError} raised while a graph rebuild persists is an
   * {@link Error}, not an {@link Exception}, so it used to escape the {@code catch (final Exception e)} guard around
   * the persist block in {@code LSMVectorIndex.buildGraphFromScratchExclusively()}. That guard is the only place that
   * calls {@code manifest.markUnusable()} on a failed persist - skip it and the manifest is left vouching for
   * whatever the pages happen to hold after a write that died partway through, so the next database open trusts a
   * possibly half-rewritten graph on node-count agreement alone (issue #6106 is exactly the check this exists to
   * satisfy). The transaction rollback in the same block is skipped too, leaving a dangling transaction.
   * <p>
   * The fix widens that one catch to {@code Throwable}. This test does not need an actual heap exhaustion to prove
   * it: it injects a real {@link OutOfMemoryError} through a {@link LSMVectorIndex.GraphBuildCallback}, at the same
   * point in the persist block ("persisting" phase completion, right after the graph pages are written but before
   * the transaction commits or the manifest is certified) where a genuine allocation failure would strike.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphPersistOutOfMemory {
    private static final String DB_ROOT     = "target/test-databases/Issue6503GraphPersistOutOfMemoryTest";
    private static final int    DIMENSIONS  = 16;
    private static final int    NUM_VECTORS = 50;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void oomDuringPersistStillMarksTheManifestUnusableAndRollsBack() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);

          // Simulates an allocation failure at the exact point in the persist block a real one would strike: the
          // graph pages are already written, but the transaction has not committed and the manifest has not been
          // certified yet.
          assertThatCode(() -> index.buildVectorGraphNow((phase, processedNodes, totalNodes, vectorAccesses) -> {
            if ("persisting".equals(phase) && processedNodes > 0 && processedNodes == totalNodes)
              throw new OutOfMemoryError("simulated for issue #6503 test");
          }))
              .as("the persist block must swallow the OOM the same way it already swallows an ordinary Exception "
                  + "there, not let it escape as an unhandled Error")
              .doesNotThrowAnyException();

          final LSMVectorIndexGraphManifest.Content manifest = graphManifestField(index).read();
          assertThat(manifest)
              .as("the manifest must exist and refuse the pages, not be silently absent (issue #6106) or left "
                  + "vouching for a persist that died mid-write")
              .isNotNull();
          assertThat(manifest.vectorCount())
              .as("an unusable manifest is marked with a negative vector count - no live set can match it")
              .isNegative();

          // The rollback in the same catch block must have run too: no transaction left dangling on the thread.
          assertThat(db.isTransactionActive())
              .as("the failed persist must have rolled back its own transaction, not left one open")
              .isFalse();

          // The in-memory graph itself is unaffected - only the disk persist failed, so the index built the new
          // graph successfully before the injected failure. It must keep answering correctly, and normal writes
          // must keep working (proves nothing is left wedged by the aborted persist transaction).
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
          db.transaction(() -> db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", randomVector(new Random(1))).save());
        } finally {
          db.drop();
        }
      }
    }

    private static LSMVectorIndexGraphManifest graphManifestField(final LSMVectorIndex index) throws Exception {
      final Field field = LSMVectorIndex.class.getDeclaredField("graphFile");
      field.setAccessible(true);
      final LSMVectorIndexGraphFile graphFile = (LSMVectorIndexGraphFile) field.get(index);
      return graphFile.getManifest();
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

  /**
   * Regression for #6503: rebuild admission control.
   * <p>
   * Regression test for issue #6503, item 3: nothing consulted memory before starting a graph rebuild. The only gate
   * was {@code REBUILD_SEMAPHORE}, which bounds how many rebuilds run at once, not how much heap one needs - a
   * rebuild that could not fit was simply attempted, and died with an {@link OutOfMemoryError}. There was no path
   * that deferred, degraded, or declined.
   * <p>
   * A rebuild is now admitted only when its estimated peak footprint fits
   * {@code arcadedb.vectorIndex.rebuildMaxHeapPercent} of the currently available heap, and only the ONLINE path is
   * gated: a first build, a rebuild on close, {@code REBUILD INDEX} and {@code COMPACT INDEX} have no later trigger
   * to retry them, so declining one would turn "slower" into "never".
   * <p>
   * The oversized-footprint cases below drive the estimate past any plausible heap with an explicit build-cache
   * size rather than by allocating anything, so the outcome does not depend on the {@code -Xmx} the suite happens to
   * run under.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RebuildAdmissionControl {
    private static final String DB_ROOT     = "target/test-databases/Issue6503RebuildAdmissionControlTest";
    private static final int    DIMENSIONS  = 16;
    private static final int    NUM_VECTORS = 200;

    /**
     * An explicit build-cache size of ~1.07e9 entries at (16*4 + 64) bytes each estimates to ~137 GB, which is past
     * 90% of any heap a JVM can be given, so the outcome does not depend on the {@code -Xmx} the suite runs under.
     * Nothing is ever allocated at this size: only the estimate reads it, and every test resets the setting before
     * letting a real build run.
     */
    private static final int UNAFFORDABLE_BUILD_CACHE_SIZE = Integer.MAX_VALUE / 2;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT.setValue(
          GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT.getDefValue());
      GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(
          GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getDefValue());
      GlobalConfiguration.VECTOR_INDEX_REBUILD_DEFERRAL_COOLDOWN_MS.setValue(
          GlobalConfiguration.VECTOR_INDEX_REBUILD_DEFERRAL_COOLDOWN_MS.getDefValue());
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void anOnlineRebuildThatCannotFitIsDeferredAndCounted() {
      withIndex(index -> {
        final long deferredBefore = index.getStats().get("rebuildsDeferredForMemory");

        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);

        assertThat(index.admitOnlineRebuild())
            .as("a rebuild whose estimated footprint dwarfs the available heap must be declined, not attempted - "
                + "attempting it is what produced the OutOfMemoryError in issue #6503")
            .isFalse();

        assertThat(index.getStats().get("rebuildsDeferredForMemory"))
            .as("a deferral must be visible in the stats, not only in the log: an index that never fits goes stale "
                + "silently otherwise")
            .isEqualTo(deferredBefore + 1);
      });
    }

    @Test
    void anOnlineRebuildThatFitsIsAdmitted() {
      withIndex(index -> {
        // Default configuration, a 200-vector 16-dimension index: this cannot fail to fit on any machine that can
        // run the test suite at all. Guards against a gate that declines everything, which would pass the test
        // above while quietly disabling rebuilds across the engine.
        assertThat(index.admitOnlineRebuild())
            .as("a small rebuild with the default budget must be admitted")
            .isTrue();
        assertThat(index.getStats().get("rebuildsDeferredForMemory")).isZero();
      });
    }

    @Test
    void theGateCanBeDisabledByConfiguration() {
      withIndex(index -> {
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);
        GlobalConfiguration.VECTOR_INDEX_REBUILD_MAX_HEAP_PERCENT.setValue(0);

        assertThat(index.admitOnlineRebuild())
            .as("0 restores the previous attempt-and-hope behaviour for an operator who knows better than the "
                + "estimate")
            .isTrue();
        assertThat(index.getStats().get("rebuildsDeferredForMemory"))
            .as("a disabled gate must not count deferrals it did not make")
            .isZero();
      });
    }

    /**
     * The close path, {@code REBUILD INDEX} and {@code COMPACT INDEX} must go through regardless: nothing retries
     * them, and the close path has already released the old graph, so it is not even the expensive shape the gate
     * exists to refuse. Only the async/online path consults it.
     */
    @Test
    void anExplicitRebuildIsNeverDeclinedNoMatterWhatTheGateWouldSay() {
      withIndex(index -> {
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);
        assertThat(index.admitOnlineRebuild()).as("precondition: the gate would decline an online rebuild here")
            .isFalse();

        // ...and REBUILD INDEX still runs to completion and leaves a searchable graph.
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(
            GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getDefValue());
        final long rebuildsBefore = index.getStats().get("graphRebuildCount");
        index.buildVectorGraphNow(null);
        assertThat(index.getStats().get("graphRebuildCount")).isGreaterThan(rebuildsBefore);
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
      });
    }

    /**
     * Wiring, not policy: the async rebuild thread must consult the gate AFTER taking its permit and must hand
     * every piece of state back on the way out - the permit, the in-progress flag and the thread reference - or a
     * single deferral would wedge the index out of ever rebuilding again.
     */
    @Test
    void aDeferredAsyncRebuildReleasesItsPermitAndInProgressFlag() throws Exception {
      withIndex(index -> {
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);

        final long rebuildsBefore = index.getStats().get("graphRebuildCount");
        try {
          final Method start = LSMVectorIndex.class.getDeclaredMethod("startAsyncGraphRebuild");
          start.setAccessible(true);
          start.invoke(index);
        } catch (final ReflectiveOperationException e) {
          throw new RuntimeException(e);
        }

        // The rebuild thread is a daemon; poll rather than sleep a fixed amount. Both conditions, not just the
        // counter: the counter is incremented inside the gate and the flag is cleared by the thread's finally block
        // a moment later, so exiting on the counter alone would read the flag while it is legitimately still set.
        final long deadline = System.currentTimeMillis() + 30_000;
        while ((index.getStats().get("rebuildsDeferredForMemory") == 0
            || index.getStats().get("asyncRebuildInProgress") != 0)
            && System.currentTimeMillis() < deadline)
          Thread.onSpinWait();

        assertThat(index.getStats().get("rebuildsDeferredForMemory"))
            .as("the async path must consult the gate, not bypass it").isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("a declined cycle must not have rebuilt anything").isEqualTo(rebuildsBefore);
        assertThat(index.getStats().get("asyncRebuildInProgress"))
            .as("the in-progress flag must be cleared on the deferral path too, or the index can never schedule "
                + "another rebuild")
            .isZero();

        // The permit must be back: a later rebuild, once it fits, has to be able to take it.
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(
            GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getDefValue());
        index.buildVectorGraphNow(null);
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
      });
    }

    /**
     * A deferral does not consume the mutations that triggered it - only a successful build decrements
     * {@code mutationsSinceSerialize} - so the trigger condition is true again the instant the deferred cycle ends,
     * and {@code rebuildGraphBeforeSearch()} evaluates it on EVERY query. Without a cooldown a large,
     * heap-constrained index therefore spawns a rebuild thread, takes and releases the JVM-wide permit and logs a
     * WARNING once per search - thread churn and contention added exactly when memory is already tight, which works
     * against the deferral's own purpose.
     */
    @Test
    void repeatedTriggersWithinTheCooldownDoNotEachAttemptARebuild() {
      withIndex(index -> {
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);
        GlobalConfiguration.VECTOR_INDEX_REBUILD_DEFERRAL_COOLDOWN_MS.setValue(60_000);

        // First trigger: genuinely attempts, and is genuinely declined.
        triggerAsyncRebuildAndSettle(index);
        assertThat(index.getStats().get("rebuildsDeferredForMemory"))
            .as("the first trigger must actually attempt the rebuild and be declined").isEqualTo(1L);

        // Every subsequent trigger inside the cooldown must be skipped before any of the work an attempt costs.
        for (int i = 0; i < 25; i++)
          triggerAsyncRebuildAndSettle(index);

        assertThat(index.getStats().get("rebuildsDeferredForMemory"))
            .as("25 further triggers inside the cooldown must not each spawn a rebuild thread, take the JVM-wide "
                + "permit and log a warning: a search evaluates this trigger on every query")
            .isEqualTo(1L);
      });
    }

    /** The suppression above must come from the cooldown, not from some other state that stopped triggering. */
    @Test
    void withTheCooldownDisabledEveryTriggerAttemptsAgain() {
      withIndex(index -> {
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);
        GlobalConfiguration.VECTOR_INDEX_REBUILD_DEFERRAL_COOLDOWN_MS.setValue(0);

        for (int i = 0; i < 3; i++)
          triggerAsyncRebuildAndSettle(index);

        assertThat(index.getStats().get("rebuildsDeferredForMemory"))
            .as("0 disables the cooldown, so each trigger attempts and is declined again - which is also what "
                + "proves the test above is measuring the cooldown and nothing else")
            .isEqualTo(3L);
      });
    }

    /** A successful build clears the cooldown, so a later shortage is not gated by a stale deferral. */
    @Test
    void aSuccessfulBuildClearsTheCooldown() {
      withIndex(index -> {
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);
        GlobalConfiguration.VECTOR_INDEX_REBUILD_DEFERRAL_COOLDOWN_MS.setValue(60_000);

        triggerAsyncRebuildAndSettle(index);
        assertThat(index.getStats().get("rebuildsDeferredForMemory")).isEqualTo(1L);

        // An explicit rebuild is never declined, and completing one means the shortage is no longer in the way.
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(
            GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.getDefValue());
        index.buildVectorGraphNow(null);

        // Back under pressure, the very next trigger must attempt again rather than sit out the original cooldown.
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_CACHE_SIZE.setValue(UNAFFORDABLE_BUILD_CACHE_SIZE);
        triggerAsyncRebuildAndSettle(index);
        assertThat(index.getStats().get("rebuildsDeferredForMemory"))
            .as("a completed build must clear the cooldown a previous deferral set")
            .isEqualTo(2L);
      });
    }

    /**
     * Drives one online-rebuild trigger the way a search does and waits for the background thread to settle, so the
     * assertions above read a stable state rather than racing the daemon.
     */
    private static void triggerAsyncRebuildAndSettle(final LSMVectorIndex index) {
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
    }

    private void withIndex(final java.util.function.Consumer<LSMVectorIndex> body) {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);
          // Make the graph resident: the gate deliberately admits an index whose graph was never built, since that
          // is a first build in all but name.
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

  /**
   * Regression for #6503: vector heap budget.
   * <p>
   * The heap arithmetic behind issue #6503, on its own: every decision that used to be made against TOTAL heap and
   * now has to be made against the heap actually available, plus the footprint estimate the rebuild admission gate
   * compares to it. Cheaper to pin here than through a database, and each of these is a property no realistic
   * fixture would exercise from both sides.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class HeapBudgetFormulas {

    @Test
    void availableHeapNeverExceedsTheCeilingAndIsNeverNegative() {
      final long max = VectorHeapBudget.maxHeapBytes();
      final long available = VectorHeapBudget.availableHeapBytes();

      assertThat(max).as("a JVM always reports a heap ceiling").isPositive();
      assertThat(available)
          .as("available heap must stay inside [0, -Xmx]: a negative budget would size a cache to nothing and a "
              + "budget past the ceiling would be the total-heap bug this replaces")
          .isBetween(0L, max);
    }

    /**
     * A JVM that has not collected yet, or a GC that does not publish a collection usage, must fall back to the
     * whole ceiling - which reproduces exactly the previous total-heap behaviour. Assuming zero live heap would be
     * the same thing; assuming a FULL heap would silently disable every auto-sized cache in the engine.
     */
    @Test
    void unknownLiveHeapFallsBackToTheWholeCeilingRatherThanToZeroBudget() {
      final long live = VectorHeapBudget.liveHeapBytes();
      if (live < 0)
        assertThat(VectorHeapBudget.availableHeapBytes()).isEqualTo(VectorHeapBudget.maxHeapBytes());
      else
        assertThat(live).as("a published live-heap reading is an occupancy, so it cannot be negative").isNotNegative();
    }

    @Test
    void budgetIsAShareOfAvailableHeapAndIsCappedAtNinetyPercent() {
      assertThat(VectorHeapBudget.budgetBytes(0)).as("0 means 'no budget', not 'unlimited'").isZero();
      assertThat(VectorHeapBudget.budgetBytes(-5)).as("a negative percentage is not a licence to overcommit").isZero();

      final long available = VectorHeapBudget.availableHeapBytes();
      assertThat(VectorHeapBudget.budgetBytes(50)).isLessThanOrEqualTo(available / 2 + 1);

      // 100% is clamped to 90: a build must never plan on the whole heap, since the request, I/O and GC threads
      // need some of it too.
      assertThat(VectorHeapBudget.budgetBytes(100)).isEqualTo(VectorHeapBudget.budgetBytes(90));
      assertThat(VectorHeapBudget.budgetBytes(90)).isLessThan(available + 1);
    }

    @Test
    void budgetGrowsMonotonicallyWithTheRequestedShare() {
      // Not a tautology: budgetBytes() clamps and divides, and an implementation that clamped in the wrong order
      // (or divided before multiplying by a clamped value) could easily invert this for a share above the cap.
      assertThat(VectorHeapBudget.budgetBytes(10)).isLessThanOrEqualTo(VectorHeapBudget.budgetBytes(25));
      assertThat(VectorHeapBudget.budgetBytes(25)).isLessThanOrEqualTo(VectorHeapBudget.budgetBytes(50));
      assertThat(VectorHeapBudget.budgetBytes(50)).isLessThanOrEqualTo(VectorHeapBudget.budgetBytes(90));
    }

    @Test
    void anEmptyGraphCostsNothingAndNodeCostScalesLinearly() {
      assertThat(VectorHeapBudget.estimateGraphBytes(0)).isZero();
      assertThat(VectorHeapBudget.estimateGraphBytes(-1)).as("a negative node count is not a negative footprint")
          .isZero();

      final long one = VectorHeapBudget.estimateGraphBytes(1);
      assertThat(one).isGreaterThanOrEqualTo(VectorHeapBudget.APPROX_GRAPH_BYTES_PER_NODE);
      assertThat(VectorHeapBudget.estimateGraphBytes(1_000)).isEqualTo(one * 1_000);
    }

    /**
     * This is the 1.73x the issue measured (1,700 MB for a rebuild against 959 MB for the first build of the same
     * 50,000 x 128 corpus). The estimate does not have to reproduce that ratio exactly - it is deliberately coarse -
     * but a rebuild that retains the old graph must come out strictly more expensive than one that does not, or the
     * admission gate is measuring the wrong thing entirely.
     */
    @Test
    void keepingTheOldGraphResidentCostsStrictlyMoreThanReleasingIt() {
      final long nodes = 50_000;
      final int dimensions = 128;
      final long cacheCapacity = nodes;

      final long online = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, cacheCapacity, true);
      final long onClose = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, cacheCapacity, false);

      assertThat(online).isGreaterThan(onClose);
      assertThat(online - onClose)
          .as("the difference is exactly one resident graph generation - what the close path releases up front")
          .isEqualTo(VectorHeapBudget.estimateGraphBytes(nodes));
    }

    @Test
    void theEstimateAccountsForTheBuildCacheAsWellAsTheGraphs() {
      final long nodes = 10_000;
      final int dimensions = 256;

      final long withoutCache = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, 0, false);
      final long withCache = VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, nodes, false);

      assertThat(withCache - withoutCache)
          .as("a build cache holding the whole corpus costs one cached vector per node")
          .isEqualTo(nodes * VectorHeapBudget.bytesPerCachedVector(dimensions));
      // A negative capacity must not credit the estimate with heap it is not going to give back.
      assertThat(VectorHeapBudget.estimateRebuildHeapBytes(nodes, dimensions, -1_000, false)).isEqualTo(withoutCache);
    }

    @Test
    void cachedVectorCostCoversThePayloadPlusTheWrapperOverhead() {
      assertThat(VectorHeapBudget.bytesPerCachedVector(128))
          .as("the float payload alone would be 512 bytes; the wrapper, cache entry and array slot are the rest")
          .isGreaterThan(128L * Float.BYTES);
      // Linear in the dimension count, so a wide-vector index is charged for being wide.
      assertThat(VectorHeapBudget.bytesPerCachedVector(256) - VectorHeapBudget.bytesPerCachedVector(128))
          .isEqualTo(128L * Float.BYTES);
    }

    @Test
    void onlyCollectorsThatRefreshTheOldGenerationReadingCountAsTenured() {
      // G1's young collector lists the old pool among its own, yet never refreshes its post-collection usage (#7260).
      assertThat(VectorHeapBudget.isTenuredCollector("G1 Young Generation")).isFalse();
      assertThat(VectorHeapBudget.isTenuredCollector("PS Scavenge")).isFalse();
      assertThat(VectorHeapBudget.isTenuredCollector("Copy")).isFalse();
      assertThat(VectorHeapBudget.isTenuredCollector("ParNew")).isFalse();
      assertThat(VectorHeapBudget.isTenuredCollector("ZGC Minor Cycles")).isFalse();
      assertThat(VectorHeapBudget.isTenuredCollector(null)).isFalse();
      assertThat(VectorHeapBudget.isTenuredCollector("SomeFutureCollector")).as("unknown withholds the credit").isFalse();
      assertThat(VectorHeapBudget.isTenuredCollector("G1 Old Generation")).isTrue();
      assertThat(VectorHeapBudget.isTenuredCollector("G1 Concurrent GC")).isTrue();
      assertThat(VectorHeapBudget.isTenuredCollector("PS MarkSweep")).isTrue();
      assertThat(VectorHeapBudget.isTenuredCollector("MarkSweepCompact")).isTrue();
      assertThat(VectorHeapBudget.isTenuredCollector("ZGC Major Cycles")).isTrue();
      assertThat(VectorHeapBudget.isTenuredCollector("Shenandoah Cycles")).isTrue();
    }
  }

  /**
   * Regression for #6503: vector rebuild resident graph release.
   * <p>
   * Regression test for issue #6503: a from-scratch graph rebuild keeps the OLD graph (and the shared search cache)
   * resident for the whole build, on top of the new build's own working set (build cache, JVector builder, ordinal
   * map). On the close path that doubling is pure waste - {@code flush()} has already CAS-ed the index to
   * {@code UNAVAILABLE} and a closing database does not hand it any more requests, so nothing can still be reading
   * either the old graph or the old cache by the time the rebuild starts. Measured in the issue at 50,000 x 128:
   * peak heap 1,700 MB for a rebuild against 959 MB for a first build of the same corpus - a factor of 1.73x that
   * raising {@code -Xmx} does not fix, because both caches size themselves off total heap, not free heap.
   * <p>
   * {@link LSMVectorIndex#buildGraphFromScratchReleasingResidentGraph(LSMVectorIndex.GraphBuildCallback)} is the
   * fix: it releases the resident graph, the search cache AND the pooled searchers before starting, and
   * {@code flush()} now calls it instead of the ordinary {@code buildGraphFromScratch()}. All three matter - a
   * pooled searcher holds the graph it was pooled under, and {@code LocalDatabase.closeDurableParts()} runs
   * {@code flush()} BEFORE {@code releaseBackgroundResources()}, so on a real close the pool is still populated
   * when the rebuild starts and would pin the old graph on its own.
   * <p>
   * It must not be used anywhere else - an online rebuild ({@code rebuildGraphBeforeSearch()},
   * {@code startAsyncGraphRebuild()}, an explicit {@code buildVectorGraphNow()}, or {@code compact()}) has to keep
   * serving the old graph to concurrent searches, since none of those gate searches on {@code status}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorRebuildResidentGraphRelease {
    private static final String DB_ROOT     = "target/test-databases/Issue6503VectorRebuildResidentGraphReleaseTest";
    private static final int    DIMENSIONS  = 16;
    private static final int    NUM_VECTORS = 200;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void closeStyleRebuildReleasesTheResidentGraphAndSearchCacheBeforeBuilding() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);

          // A search makes both the graph and the shared search cache resident, same as any index that has
          // actually answered a query - which is the state a close-triggered rebuild finds them in.
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
          assertThat(index.getGraphIndex()).as("graph must be resident before the rebuild under test").isNotNull();
          assertThat(searchVectorCacheField(index))
              .as("search cache must be resident before the rebuild under test").isNotNull();

          final AtomicReference<Boolean> graphReleasedOnFirstSample = new AtomicReference<>();
          final AtomicReference<Object>  searchCacheOnFirstSample   = new AtomicReference<>();
          final AtomicInteger            pooledSearchersOnFirstSample = new AtomicInteger(-1);

          index.buildGraphFromScratchReleasingResidentGraph((phase, processedNodes, totalNodes, vectorAccesses) -> {
            if (graphReleasedOnFirstSample.get() != null)
              return; // only the first sample matters: it is chronologically the earliest observation point
            graphReleasedOnFirstSample.set(index.getGraphIndex() == null);
            pooledSearchersOnFirstSample.set(index.getStats().get("pooledGraphSearchers").intValue());
            try {
              searchCacheOnFirstSample.set(searchVectorCacheField(index));
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          });

          assertThat(graphReleasedOnFirstSample.get())
              .as("the resident graph must already be released by the time the rebuild reports its first progress "
                  + "sample - otherwise the old graph and the new build's working set are both resident at once")
              .isTrue();
          assertThat(searchCacheOnFirstSample.get())
              .as("the resident search cache must already be released by the time the rebuild reports its first "
                  + "progress sample")
              .isNull();
          // Nulling graphIndex alone releases nothing while a pooled searcher still holds the graph it was pooled
          // under. This is not hypothetical on the path under test: LocalDatabase.closeDurableParts() runs
          // index.flush() BEFORE index.releaseBackgroundResources(), so on a real database close the pool is still
          // populated when the rebuild starts, and the old graph would stay reachable through it.
          assertThat(pooledSearchersOnFirstSample.get())
              .as("the searcher pool must be emptied too, or every pooled searcher keeps the old graph alive and "
                  + "the release frees nothing")
              .isZero();

          // ...and the rebuild still produces a correct, searchable graph.
          assertThat(index.getGraphIndex()).as("the rebuild must have published a new graph").isNotNull();
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void onlineRebuildDoesNotReleaseTheResidentGraphBeforeBuilding() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);

          final Object residentGraphBeforeRebuild = index.getGraphIndex();
          assertThat(residentGraphBeforeRebuild).isNotNull();

          final AtomicReference<Object> graphOnFirstSample = new AtomicReference<>();
          // buildVectorGraphNow() is what an explicit REBUILD INDEX drives while the database may still be
          // serving concurrent traffic: nothing here gates searches on status, so the old graph must stay put.
          index.buildVectorGraphNow((phase, processedNodes, totalNodes, vectorAccesses) -> {
            if (graphOnFirstSample.get() == null)
              graphOnFirstSample.set(index.getGraphIndex());
          });

          assertThat(graphOnFirstSample.get())
              .as("an explicit/online rebuild must keep serving the OLD graph until the new one is ready - it must "
                  + "not take the close-path shortcut that releases it up front")
              .isSameAs(residentGraphBeforeRebuild);

          // The rebuild still completes correctly; only the timing of the release differs from the close path.
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void closingWithPendingMutationsRebuildsAndReopensToACorrectGraph() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        populate(db);

        // One more mutation than the initial build absorbed, so graphState is MUTABLE and flush() takes the
        // rebuild branch under test instead of the "nothing to do" one.
        db.transaction(() -> {
          final float[] vector = randomVector(new Random(123));
          db.newDocument("Doc").set("id", NUM_VECTORS).set("vector", vector).save();
        });

        assertThat(vectorIndex(db).getStats().get("graphState"))
            .as("an extra insert past the initial build must leave the graph MUTABLE, otherwise close() below "
                + "would skip the rebuild this test exercises")
            .isEqualTo(2L); // GraphState.MUTABLE ordinal, see getStats()

        db.close();
      }

      // Reopen and confirm the close-triggered rebuild persisted a correct, searchable graph over all
      // NUM_VECTORS + 1 vectors - including the one added right before close().
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        try (final Database db = factory.open()) {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
          assertThat(index.getStats().get("activeVectors")).isEqualTo((long) NUM_VECTORS + 1);
        }
      } finally {
        FileUtils.deleteRecursively(new File(dbPath));
      }
    }

    private static FlatVectorCache searchVectorCacheField(final LSMVectorIndex index) throws Exception {
      final Field field = LSMVectorIndex.class.getDeclaredField("searchVectorCache");
      field.setAccessible(true);
      return (FlatVectorCache) field.get(index);
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
      for (int i = 0; i < NUM_VECTORS; i++) {
        db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
        if (i % 1000 == 999) {
          db.commit();
          db.begin();
        }
      }
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

  /**
   * Regression for #11497: search build heap gate.
   * <p>
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
  @Nested
  @Tag("vector")
  class SearchBuildHeapGate {
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
          // wrapper (CommandExecutionException, TransactionException) down, and no deeper: the SQL path delivers it unwrapped,
          // so the one level the server's auto-commit wrapper may add still reaches it (Issue6201ErrorStatusParityTest)
          assertThat(refusedBySql).as("the refusal must reach the HTTP status mapping").isInstanceOf(VectorGraphBuildRefusedException.class);

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

  /**
   * Regression for #7146: HNSW build cache budget accounts for the corpus already held on the heap.
   * <p>
   * Issue #7146: {@code graphBuildCacheMaxHeapPercent} sized the HNSW build cache from leftover heap at
   * build start. A served ingest that already held the corpus in the same JVM therefore chose a third of
   * the cache an embedded build of the same corpus would get. The numbers below are the DEEP-10M snapshot
   * from the report (24 GB {@code -Xmx}, 96-d, 9,990,000 vectors, served leftover ~6.6 GB).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class GraphBuildCacheBudget {

    private static final int  DIMENSIONS          = 96;
    private static final int  CORPUS              = 9_990_000;
    private static final int  DEFAULT_PERCENT     = 25;
    /** 24 GiB, the {@code -Xmx} both deployments used. */
    private static final long MAX_HEAP            = 24L * 1024 * 1024 * 1024;
    /**
     * Leftover heap the engine saw on the served arm: working back from the log line
     * {@code cache enabled: size=3674697} at 448 bytes/vector and a 25% share of available heap.
     */
    private static final long SERVED_AVAILABLE    = 6_585_057_024L;
    /** Embedded arm: corpus lives in numpy outside the JVM, so almost the whole ceiling is free. */
    private static final long EMBEDDED_AVAILABLE  = MAX_HEAP - 512L * 1024 * 1024;

    @Test
    void theOldPercentOfAvailableFormulaChoseAThirdOfTheCorpusOnTheServedArm() {
      final long bytesPer = VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);
      assertThat(bytesPer).isEqualTo(448L);

      // This is budgetBytes(): percent of leftover, not of the ceiling.
      final long oldServedBudget = SERVED_AVAILABLE / 100 * DEFAULT_PERCENT;
      final long oldServedCapacity = oldServedBudget / bytesPer;

      assertThat(oldServedCapacity)
          .as("the size=3674697 log line the served default actually chose")
          .isBetween(3_600_000L, 3_700_000L);
      assertThat(oldServedCapacity).isLessThan(CORPUS);

      final long oldEmbeddedBudget = EMBEDDED_AVAILABLE / 100 * DEFAULT_PERCENT;
      final long oldEmbeddedCapacity = oldEmbeddedBudget / bytesPer;
      assertThat(oldEmbeddedCapacity)
          .as("embedded 25% of ~23.5 GB pays for more than the corpus, so it was capped at N")
          .isGreaterThanOrEqualTo(CORPUS);
    }

    @Test
    void theDefaultPercentOfTheCeilingPaysForTheWholeDeep10mCorpusOnBothArms() {
      final long bytesPer = VectorHeapBudget.bytesPerCachedVector(DIMENSIONS);

      final long servedBudget = VectorHeapBudget.buildCacheBudgetBytes(DEFAULT_PERCENT, MAX_HEAP, SERVED_AVAILABLE);
      final long servedCapacity = servedBudget / bytesPer;
      assertThat(servedCapacity)
          .as("served must no longer land on the steep side of the cache curve at the default")
          .isGreaterThanOrEqualTo(CORPUS);

      final long embeddedBudget = VectorHeapBudget.buildCacheBudgetBytes(DEFAULT_PERCENT, MAX_HEAP, EMBEDDED_AVAILABLE);
      final long embeddedCapacity = embeddedBudget / bytesPer;
      assertThat(embeddedCapacity).isGreaterThanOrEqualTo(CORPUS);

      // Served leftover is the cap (~90% of 6.6 GB), embedded leftover is not; both still clear the corpus.
      assertThat(servedBudget).isLessThanOrEqualTo(embeddedBudget);
    }

    @Test
    void anOnlineRebuildWithLittleLeftoverStillCapsAtAvailableHeap() {
      // Old graph resident: ~2 GB free. The cache must not ask for 25% of 24 GB on top of that (issue #6503).
      final long tightAvailable = 2L * 1024 * 1024 * 1024;
      final long budget = VectorHeapBudget.buildCacheBudgetBytes(DEFAULT_PERCENT, MAX_HEAP, tightAvailable);

      assertThat(budget).isEqualTo(tightAvailable / 100 * 90);
      assertThat(budget).isLessThan(MAX_HEAP / 100 * DEFAULT_PERCENT);
    }

    @Test
    void buildCacheBudgetIsNeverSmallerThanTheAdmissionGateShareOfTheSameLeftover() {
      // Admission control (budgetBytes) stays on leftover heap. The build cache may claim more than that
      // share of leftover, but never less: otherwise we would reintroduce the served under-size.
      for (final int percent : new int[] { 10, 25, 50, 75, 90, 100 }) {
        final long admission = SERVED_AVAILABLE / 100 * Math.min(percent, 90);
        final long cache = VectorHeapBudget.buildCacheBudgetBytes(percent, MAX_HEAP, SERVED_AVAILABLE);
        assertThat(cache)
            .as("build-cache budget at %d%% must not undershoot the admission-gate share of leftover", percent)
            .isGreaterThanOrEqualTo(admission);
      }
    }

    @Test
    void aZeroOrNegativePercentIsNoBudgetAndValuesAboveNinetyClamp() {
      assertThat(VectorHeapBudget.buildCacheBudgetBytes(0, MAX_HEAP, SERVED_AVAILABLE)).isZero();
      assertThat(VectorHeapBudget.buildCacheBudgetBytes(-5, MAX_HEAP, SERVED_AVAILABLE)).isZero();
      assertThat(VectorHeapBudget.buildCacheBudgetBytes(100, MAX_HEAP, EMBEDDED_AVAILABLE))
          .isEqualTo(VectorHeapBudget.buildCacheBudgetBytes(90, MAX_HEAP, EMBEDDED_AVAILABLE));
    }

    /**
     * The live overload reads {@link VectorHeapBudget#availableHeapBytes()} itself, and that reading moves whenever
     * the JVM collects - so comparing it against a second reading taken here would be a coin flip on whether a GC
     * landed between the two, which in a full-suite run it regularly does. What is invariant, and is the whole
     * point of the change, is the upper bound: a share of a ceiling that cannot move for the life of the JVM.
     */
    @Test
    void theLiveJvmOverloadNeverExceedsTheShareOfTheCeilingItIsGiven() {
      final int percent = 25;
      final long budget = VectorHeapBudget.buildCacheBudgetBytes(percent);

      assertThat(budget).isNotNegative();
      assertThat(budget)
          .as("the share of -Xmx is the most the build cache may claim, whatever the live heap happens to say")
          .isLessThanOrEqualTo(VectorHeapBudget.maxHeapBytes() / 100 * percent);
    }
  }

  /**
   * Regression for #7184: the readings the online-rebuild admission gate compares.
   * <p>
   * The two readings the online-rebuild admission gate compares, and the eviction that makes one of them honest
   * (issue #7184).
   * <p>
   * At 10M vectors in a 24 GB heap the gate of issue #6503 refused the rebuild at every single attempt, so the delta
   * buffer those pending vectors sit in grew without bound and every query paid a linear scan of it - 42 ms at 2.5M
   * buffered, indistinguishable from the curve with every rebuild trigger disabled. Both sides of its comparison were
   * wrong in the same direction:
   * <ul>
   *   <li>the COST side charged a second full on-heap graph for the graph being kept resident. A session that reopened
   *   the database serves an {@code OnDiskGraphIndex}, whose topology is in pages - so that term is now measured
   *   instead of assumed, and a disk-backed graph is charged what it actually retains;</li>
   *   <li>the HEAP side judged the rebuild against a post-collection live-set reading that counts ArcadeDB's own page
   *   read cache as occupied. It is occupied, and it is evictable. The gate now asks how much of it has to be given up
   *   and gives that up before admitting, rather than counting it as free and hoping - which would be the
   *   {@link OutOfMemoryError} the gate exists to prevent, in the other direction.</li>
   * </ul>
   * The arithmetic is pinned here with supplied figures, the way issue #7146's build-cache budget is, because the
   * shapes that matter (24 GB of heap, 10M nodes) are not shapes a test can allocate.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RebuildAdmissionHeapReading {
    private static final String DB_ROOT    = "target/test-databases/RebuildAdmissionHeapReading";
    private static final long   GB         = 1024L * 1024 * 1024;
    private static final int    DIMENSIONS = 16;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    // ---------------------------------------------------------------- the heap side

    @Test
    void anAllocationThatAlreadyFitsAsksForNoEviction() {
      assertThat(VectorHeapBudget.reclaimNeededFor(GB, 90, 8 * GB, 4 * GB))
          .as("1 GB inside 90% of 8 GB available: nothing has to be given up")
          .isZero();
    }

    @Test
    void anAllocationShortByLessThanThePageCacheAsksForExactlyTheShortfall() {
      // 90% of 10 GB is 9 GB, so a 9.9 GB request needs 11 GB of available heap: 1 GB short.
      final long shortfall = VectorHeapBudget.reclaimNeededFor(9_900L * 1024 * 1024, 90, 10 * GB, 4 * GB);

      assertThat(shortfall).as("the gap must be closed by eviction, not by wishful accounting").isPositive();
      assertThat(shortfall)
          .as("and only the gap: evicting the whole cache would cost every other reader for no reason")
          .isLessThan(2 * GB);
    }

    @Test
    void anAllocationTheWholePageCacheCannotCoverIsRefused() {
      assertThat(VectorHeapBudget.reclaimNeededFor(40 * GB, 90, 10 * GB, 4 * GB))
          .as("40 GB into a 10 GB reading with 4 GB of evictable pages does not fit, and must be said so")
          .isEqualTo(-1L);
    }

    @Test
    void withNoReclaimablePagesTheAnswerIsTheOldOne() {
      assertThat(VectorHeapBudget.reclaimNeededFor(20 * GB, 90, 10 * GB, 0L))
          .as("this is the pre-#7184 behaviour: no page cache to give up, so the rebuild is deferred")
          .isEqualTo(-1L);
      assertThat(VectorHeapBudget.reclaimNeededFor(GB, 90, 10 * GB, 0L))
          .as("...and one that fits is still admitted with nothing evicted")
          .isZero();
    }

    @Test
    void aDisabledGateNeverAsksForEviction() {
      assertThat(VectorHeapBudget.reclaimNeededFor(400 * GB, 0, GB, GB))
          .as("percent 0 disables the gate, so it must not evict anything on its way past")
          .isZero();
    }

    /** An estimate large enough to overflow the percent arithmetic must read as "does not fit", never as "fits". */
    @Test
    void anAbsurdEstimateIsRefusedRatherThanWrappingAround() {
      assertThat(VectorHeapBudget.reclaimNeededFor(Long.MAX_VALUE / 2, 90, 10 * GB, 4 * GB)).isEqualTo(-1L);
    }

    // ---------------------------------------------------------------- the cost side

    /**
     * The shape from the report: 10M nodes, 24 GB heap. The old estimate charged two on-heap graphs and asked for about
     * 24 GB, which no 24 GB heap can give it. A reopened session's resident graph is disk-backed and retains a few
     * hundred MB, so the honest figure is one graph plus the build cache.
     */
    @Test
    void aDiskBackedResidentGraphIsNotChargedAsASecondOnHeapGraph() {
      final long nodes = 10_000_000L;
      final long buildCache = 2_000_000L;
      final long residentOnDisk = 200L * 1024 * 1024; // what an OnDiskGraphIndex actually retains

      final long twoGraphs = VectorHeapBudget.estimateRebuildHeapBytes(nodes, 64, buildCache, true);
      final long measured = VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, 64, buildCache, residentOnDisk,
          nodes, false, 1.2f);

      assertThat(measured)
          .as("charging a phantom second on-heap graph is what refused a rebuild the close path runs unconditionally")
          .isLessThan(twoGraphs);
      assertThat(measured)
          .as("and the difference must be about one graph's worth")
          .isLessThan(twoGraphs - VectorHeapBudget.estimateGraphBytes(nodes) + 512L * 1024 * 1024);
      assertThat(measured)
          .as("the resident graph is still charged what it does retain")
          .isGreaterThan(residentOnDisk);
    }

    @Test
    void anOnHeapResidentGraphIsChargedWhatItMeasuresAndPredictsTheNextBuildWithIt() {
      final long nodes = 1_000_000L;
      final long residentBytes = 400L * nodes; // measured: 400 bytes/node, well under the flat constant

      final long estimate = VectorHeapBudget.estimateOnlineRebuildHeapBytes(nodes, 64, 0L, residentBytes, nodes, true,
          1.0f);

      assertThat(estimate)
          .as("a measurement of THIS index beats a constant measured on a 128-dimension index in issue #6503")
          .isLessThan(VectorHeapBudget.estimateRebuildHeapBytes(nodes, 64, 0L, true));
      assertThat(estimate).as("and both graphs are still charged")
          .isGreaterThan(2 * residentBytes - VectorHeapBudget.estimateGraphBytes(1));
    }

    @Test
    void theBuildIsChargedTheNeighbourOverflowAGraphHoldsBeforeCleanup() {
      final long perNodeFlat = VectorHeapBudget.buildBytesPerNode(0L, 0L, true, 1.2f);
      final long perNodeMeasured = VectorHeapBudget.buildBytesPerNode(1_000L * 1_000L, 1_000L, true, 1.5f);

      assertThat(perNodeFlat)
          .as("with nothing to measure the flat constant stands, which is the pre-#7184 behaviour")
          .isEqualTo(VectorHeapBudget.APPROX_GRAPH_BYTES_PER_NODE);
      assertThat(perNodeMeasured)
          .as("a build holds up to the overflow factor times the final out-degree per node before cleanup() trims it")
          .isEqualTo(1_500L);
      assertThat(VectorHeapBudget.buildBytesPerNode(1_000L * 1_000L, 1_000L, false, 1.5f))
          .as("a disk-backed graph's per-node cost says nothing about an on-heap build")
          .isEqualTo(VectorHeapBudget.APPROX_GRAPH_BYTES_PER_NODE);
      assertThat(VectorHeapBudget.buildBytesPerNode(1_000L * 1_000L, 1_000L, true, 0f))
          .as("a nonsensical overflow factor must not shrink the estimate below the measurement")
          .isEqualTo(1_000L);
    }

    // ---------------------------------------------------------------- the eviction itself

    /**
     * The gate's promise: what it counted as reclaimable, it reclaims. Cached pages are strongly referenced, so a
     * collection would never have handed them to the rebuild on its own.
     */
    @Test
    void thePageReadCacheReportsItsSizeAndGivesItUpOnRequest() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          // Read everything back so the pages are in the READ cache rather than only in the write path.
          try (final ResultSet rs = db.query("sql", "SELECT count(*) as total FROM Doc")) {
            assertThat(rs.next().<Long>getProperty("total")).isEqualTo(20_000L);
          }

          final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
          final long cached = pageManager.getReadCacheRAM();
          assertThat(cached).as("precondition: reading 20,000 records has to leave pages in the read cache")
              .isPositive();

          final long request = cached / 4;
          final long freed = pageManager.reclaimReadCacheRAM(request);

          assertThat(freed).as("a caller told the cache was reclaimable must actually get the bytes back")
              .isGreaterThanOrEqualTo(request);
          assertThat(pageManager.getReadCacheRAM()).as("...and the cache must report the smaller size")
              .isLessThan(cached);

          // The pages come back from disk: eviction costs I/O, never correctness.
          try (final ResultSet rs = db.query("sql", "SELECT count(*) as total FROM Doc")) {
            assertThat(rs.next().<Long>getProperty("total")).isEqualTo(20_000L);
          }
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void reclaimingNothingIsANoOp() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
          assertThat(pageManager.reclaimReadCacheRAM(0L)).isZero();
          assertThat(pageManager.reclaimReadCacheRAM(-1L)).isZero();
        } finally {
          db.drop();
        }
      }
    }

    private static void populate(final Database db) {
      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("payload", Type.STRING);
      });

      final String payload = "x".repeat(512);
      db.begin();
      for (int i = 0; i < 20_000; i++) {
        db.newDocument("Doc").set("id", i).set("payload", payload).save();
        if (i % 5_000 == 4_999) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }
  }

  /**
   * Regression for #7260: a freshly built on-heap graph is not charged as resident for the next build.
   * <p>
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
  @Nested
  class ResidentGraphDemotion {
    private static final String DB_PATH     = "target/test-databases/ResidentGraphDemotion";
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
          sleepQuietly();

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

    /** The deadline above is a hang detector, not a latency bound: the poll itself must not burn a core. */
    private static void sleepQuietly() {
      try {
        Thread.sleep(10);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
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

  /**
   * Regression for #7814: the synchronous build a search falls through to honours the rebuild permit.
   * <p>
   * Regression test for issue #7814: the synchronous graph build a SEARCH falls through to must honour the JVM-wide
   * rebuild permit, the same way an async rebuild has since issue #3868.
   * <p>
   * The reported incident is what happens when it does not: a reopened database whose vector indexes have no usable
   * graph on disk runs one full, unbounded build per index, each on the request thread that first queried it, all at
   * once. Eight of them pinned a 39 GB heap at its ceiling and had the node restarted under it. Nothing in the class
   * prevented that - {@code REBUILD_SEMAPHORE} was acquired only by the background rebuild paths.
   * <p>
   * The fixture reaches that build the shortest honest way: write the vectors, then crash before anything persists a
   * graph next to them. The reopened index then has a full corpus and no graph, which is exactly the state every
   * index in that incident was in after the pods restarted.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class SearchRebuildAdmission {
    private static final String DB_ROOT     = "target/test-databases/SearchRebuildAdmission";
    private static final int    DIMENSIONS  = 16;
    /** Above {@code ASYNC_REBUILD_MIN_GRAPH_SIZE}, which is the threshold below which a build takes no permit. */
    private static final int    NUM_VECTORS = 1_200;
    /** Below it, so the exemption for a build too small to threaten the heap is exercised too. */
    private static final int    FEW_VECTORS = 200;

    /**
     * How long a parked search is given to reach the permit it is waiting for. A convergence wait, not a latency
     * bound: all it has to do first is materialise the location index and find no graph beside it. Generous because
     * a wider value cannot turn a passing run red - only a test that has already failed waits this long.
     */
    private static final Duration REACHES_THE_PERMIT = Duration.ofSeconds(30);

    /**
     * And how long the build itself is given once the permit is handed over. Sized for the worst case of the
     * {@code @Tag("vector")} convoy described in CLAUDE.md rather than for this corpus, which builds in well under a
     * second.
     */
    private static final Duration COMPLETES_THE_BUILD = Duration.ofMinutes(2);

    private String dbPath;
    private long   originalPermitTimeoutMs;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
      // A build that gives up waiting proceeds without a permit, by design - a query cannot be answered without a
      // graph. At the ten-minute default that turns a regression here into a ten-minute hang instead of a failure,
      // so the bound is shortened for the run and restored after it. Still far longer than the window these tests
      // observe, so the give-up path is never the one being measured.
      originalPermitTimeoutMs = GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong();
      GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(60_000);
    }

    @AfterEach
    void tearDown() {
      GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(originalPermitTimeoutMs);
      FileUtils.deleteRecursively(new File(dbPath));
    }

    /**
     * The bug itself: with every rebuild permit held, a search that has to build the graph from scratch used to
     * build it anyway, immediately, however many other indexes were doing the same thing.
     */
    @Test
    void aSearchThatMustBuildTheGraphWaitsForTheJvmWideRebuildPermit() throws Exception {
      populateAndCrashWithoutPersistingAGraph(NUM_VECTORS);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("persistedGraphNodeCount"))
              .as("precondition: the crash must have left no graph on disk, otherwise the search reuses one and "
                  + "never reaches a build at all")
              .isZero();
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: the corpus must be above the threshold below which a build takes no permit")
              .isGreaterThanOrEqualTo(1_000L);

          final CountDownLatch searchReturned = new CountDownLatch(1);
          final AtomicReference<List<Pair<RID, Float>>> results = new AtomicReference<>();
          final AtomicReference<Throwable> failure = new AtomicReference<>();

          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          final Thread searcher = new Thread(() -> {
            try {
              results.set(index.findNeighborsFromVector(embedding(7), 5, 64));
            } catch (final Throwable t) {
              failure.set(t);
            } finally {
              searchReturned.countDown();
            }
          }, "Issue7814-search");
          searcher.setDaemon(true);

          boolean permitsHeld = true;
          try {
            searcher.start();

            Awaitility.await("the search reaches the rebuild permit and queues for it")
                .atMost(REACHES_THE_PERMIT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                    .as("a search that has to build the graph must go through the same JVM-wide permit an async "
                        + "rebuild takes (issue #7814)")
                    .isEqualTo(1L));

            assertThat(searchReturned.getCount())
                .as("and it must still be waiting: with no permit free, the build it needs has not run")
                .isEqualTo(1L);
            assertThat(index.getStats().get("graphRebuildCount"))
                .as("no graph may have been built while every permit was held - this is the count that was 8 "
                    + "concurrent builds in the reported incident")
                .isZero();
            assertThat(index.getStats().get("searchRebuildsWithoutPermit"))
                .as("and it must be waiting for the permit, not have given up on it").isZero();

            // The ordering invariant the wait depends on. If the search held graphBuildLock while queueing, the
            // async rebuild path - which takes the permit first and this lock second - would deadlock against it
            // until the permit timeout expired.
            assertThat(index.graphBuildLockHeldForTest())
                .as("a search waiting for a rebuild permit must not be holding the per-index build lock that the "
                    + "permit's holder acquires next")
                .isFalse();

            LSMVectorIndex.releaseAllRebuildPermitsForTest();
            permitsHeld = false;

            assertThat(searchReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS))
                .as("once a permit is free the build runs and the search is answered - waiting for the permit must "
                    + "delay the answer, never withhold it")
                .isTrue();
          } finally {
            if (permitsHeld)
              LSMVectorIndex.releaseAllRebuildPermitsForTest();
            searcher.interrupt();
            searcher.join(COMPLETES_THE_BUILD.toMillis());
          }

          assertThat(failure.get()).as("the search must not have failed").isNull();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("the build the search was queueing for must have run once the permit freed").isEqualTo(1L);
          assertThat(results.get())
              .as("and it must return the neighbours of a corpus that was fully rebuilt, not an empty result set")
              .isNotEmpty();
          assertThat(results.get().stream().map(Pair::getFirst))
              .as("the query vector's own record is its nearest neighbour")
              .contains(ridOf(db, 7));
        } finally {
          db.drop();
        }
      }
    }

    /**
     * Two searches arriving on the same index while it has no graph must produce ONE build between them.
     * <p>
     * This is the window the permit wait opens: the deciding thread has to release the per-index build lock before it
     * queues, so without something holding the decision the second search would repeat the whole validation walk,
     * reach the same answer, and queue for a second full build of the same corpus - the cost this gate exists to
     * bound, paid twice.
     */
    @Test
    void twoSearchesRacingTheSameMissingGraphProduceExactlyOneBuild() throws Exception {
      populateAndCrashWithoutPersistingAGraph(NUM_VECTORS);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final CountDownLatch bothReturned = new CountDownLatch(2);
          final AtomicReference<Throwable> failure = new AtomicReference<>();

          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          final Thread[] searchers = new Thread[2];
          for (int i = 0; i < searchers.length; i++) {
            final int id = i;
            searchers[i] = new Thread(() -> {
              try {
                index.findNeighborsFromVector(embedding(id), 5, 64);
              } catch (final Throwable t) {
                failure.compareAndSet(null, t);
              } finally {
                bothReturned.countDown();
              }
            }, "Issue7814-search-" + i);
            searchers[i].setDaemon(true);
          }

          boolean permitsHeld = true;
          try {
            for (final Thread searcher : searchers)
              searcher.start();

            Awaitility.await("one of the two searches queues for the permit")
                .atMost(REACHES_THE_PERMIT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                    .isEqualTo(1L));

            // Whichever search lost the decision must be waiting on the winner rather than queueing for a permit of
            // its own: one build is owed here, not two.
            assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                .as("only the search that owns the build may queue for a permit; the other waits for its result")
                .isEqualTo(1L);

            LSMVectorIndex.releaseAllRebuildPermitsForTest();
            permitsHeld = false;

            assertThat(bothReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS))
                .as("both searches must be answered, neither wedged behind the other")
                .isTrue();
          } finally {
            if (permitsHeld)
              LSMVectorIndex.releaseAllRebuildPermitsForTest();
            for (final Thread searcher : searchers) {
              searcher.interrupt();
              searcher.join(COMPLETES_THE_BUILD.toMillis());
            }
          }

          assertThat(failure.get()).as("neither search may fail").isNull();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("the two searches must share one build of the corpus, not run one each")
              .isEqualTo(1L);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The same one-build guarantee for the OTHER synchronous build site, which concurrent searches reach without
     * ever going through {@code ensureGraphAvailable()}.
     * <p>
     * An index whose graph has already been resolved once - an empty one that was searched before anything was
     * written to it - leaves no resident graph and nothing left to resolve, so every later query walks straight
     * past that method into {@code rebuildGraphBeforeSearch()}'s synchronous arm. Each one would decide there,
     * independently, to build the whole corpus written since; at {@code maxConcurrentRebuilds} above 1 they would
     * do it at the same time, which is the failure this issue is about, reached from a different call site than the
     * one it was reported from.
     */
    @Test
    void concurrentSearchesAfterAnIngestShareOneBuildAtTheOtherSynchronousSite() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db);

          final LSMVectorIndex index = vectorIndex(db);
          // Resolves the (empty) index and clears the latch that would otherwise send the searches below through
          // ensureGraphAvailable() instead, which the earlier tests already cover.
          index.findNeighborsFromVector(embedding(0), 5, 64);

          insert(db, NUM_VECTORS);

          final CountDownLatch bothReturned = new CountDownLatch(2);
          final AtomicReference<Throwable> failure = new AtomicReference<>();

          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          final Thread[] searchers = new Thread[2];
          for (int i = 0; i < searchers.length; i++) {
            final int id = i;
            searchers[i] = new Thread(() -> {
              try {
                index.findNeighborsFromVector(embedding(id), 5, 64);
              } catch (final Throwable t) {
                failure.compareAndSet(null, t);
              } finally {
                bothReturned.countDown();
              }
            }, "Issue7814-ingest-search-" + id);
            searchers[i].setDaemon(true);
          }

          boolean permitsHeld = true;
          try {
            for (final Thread searcher : searchers)
              searcher.start();

            Awaitility.await("one of the two searches queues for the permit")
                .atMost(REACHES_THE_PERMIT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                    .as("this arm must reach the permit too: the corpus written since the index was resolved is a "
                        + "full build, whatever the resident graph's size says")
                    .isEqualTo(1L));

            assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                .as("and only one of them may own it; the other waits for its result")
                .isEqualTo(1L);

            LSMVectorIndex.releaseAllRebuildPermitsForTest();
            permitsHeld = false;

            assertThat(bothReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS))
                .as("both searches must be answered").isTrue();
          } finally {
            if (permitsHeld)
              LSMVectorIndex.releaseAllRebuildPermitsForTest();
            for (final Thread searcher : searchers) {
              searcher.interrupt();
              searcher.join(COMPLETES_THE_BUILD.toMillis());
            }
          }

          assertThat(failure.get()).as("neither search may fail").isNull();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("one build of the ingested corpus between the two searches, not one each")
              .isEqualTo(1L);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * A search interrupted while waiting for another thread's build must fail loudly rather than take the build over.
     * <p>
     * Falling through would let it decide to build the same corpus a second time, and at
     * {@code maxConcurrentRebuilds} above 1 there is a second permit for it to take - so both would build at once,
     * which is the doubled cost the pending flag exists to prevent. The alternatives to failing are that duplicate
     * build or the empty result set a search with no graph returns, so the exception is the only honest answer.
     */
    @Test
    void aSearchInterruptedWhileWaitingForAnothersBuildFailsRatherThanStartingItsOwn() throws Exception {
      populateAndCrashWithoutPersistingAGraph(NUM_VECTORS);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final CountDownLatch ownerReturned = new CountDownLatch(1);
          final CountDownLatch waiterReturned = new CountDownLatch(1);
          final AtomicReference<List<Pair<RID, Float>>> ownerResults = new AtomicReference<>();
          final AtomicReference<Throwable> ownerFailure = new AtomicReference<>();
          final AtomicReference<Throwable> waiterFailure = new AtomicReference<>();

          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          final Thread owner = new Thread(() -> {
            try {
              ownerResults.set(index.findNeighborsFromVector(embedding(1), 5, 64));
            } catch (final Throwable t) {
              ownerFailure.set(t);
            } finally {
              ownerReturned.countDown();
            }
          }, "Issue7814-owner");
          owner.setDaemon(true);

          final Thread waiter = new Thread(() -> {
            try {
              index.findNeighborsFromVector(embedding(2), 5, 64);
            } catch (final Throwable t) {
              waiterFailure.set(t);
            } finally {
              waiterReturned.countDown();
            }
          }, "Issue7814-waiter");
          waiter.setDaemon(true);

          boolean permitsHeld = true;
          try {
            owner.start();
            // Only once the owner has taken the decision and is queueing for the permit can the second search park
            // on it rather than race it to the decision.
            Awaitility.await("the owning search queues for the permit")
                .atMost(REACHES_THE_PERMIT)
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                    .isEqualTo(1L));

            waiter.start();
            Awaitility.await("the second search parks on the owner's build")
                .atMost(REACHES_THE_PERMIT)
                .pollInterval(Duration.ofMillis(50))
                // TIMED_WAITING: the wait is cut in slices that check the statement's guard (issue #9896)
                .untilAsserted(() -> assertThat(waiter.getState()).isEqualTo(Thread.State.TIMED_WAITING));

            waiter.interrupt();
            assertThat(waiterReturned.await(REACHES_THE_PERMIT.toMillis(), TimeUnit.MILLISECONDS))
                .as("the interrupted search must return rather than keep waiting").isTrue();
            assertThat(waiterFailure.get())
                .as("and it must say why, instead of silently returning the empty result set a search with no graph "
                    + "produces")
                .isInstanceOf(IndexException.class);
            assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                .as("the interrupted search must not have taken the build over: only the owner ever queued")
                .isEqualTo(1L);

            LSMVectorIndex.releaseAllRebuildPermitsForTest();
            permitsHeld = false;

            assertThat(ownerReturned.await(COMPLETES_THE_BUILD.toMillis(), TimeUnit.MILLISECONDS))
                .as("the owning search must still be answered").isTrue();
          } finally {
            if (permitsHeld)
              LSMVectorIndex.releaseAllRebuildPermitsForTest();
            owner.interrupt();
            owner.join(COMPLETES_THE_BUILD.toMillis());
            waiter.join(COMPLETES_THE_BUILD.toMillis());
          }

          assertThat(ownerFailure.get()).as("the owning search must not have failed").isNull();
          // The interrupted waiter must not have disturbed the owner's own bookkeeping on its way out: the latch
          // that says the on-disk graph is still undecided belongs to the thread that decided, and a waiter that
          // cleared it would let a third search past the wait while the graph it needs is still being built - and
          // a search with no graph answers with an empty result set rather than an error.
          assertThat(ownerResults.get())
              .as("the owning search must still be answered off a fully built graph").isNotEmpty();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("exactly one build of the corpus, not one per search thread").isEqualTo(1L);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The exemption: an index too small to threaten the heap must not queue behind another index's rebuild.
     * <p>
     * Serializing every build regardless of size would trade milliseconds of work for however long the permit holder
     * takes, which is the wrong bargain for a corpus that cannot cause the failure the permit exists to prevent.
     */
    @Test
    void aBuildTooSmallToThreatenTheHeapDoesNotQueueForAPermit() throws Exception {
      populateAndCrashWithoutPersistingAGraph(FEW_VECTORS);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: the corpus must be below the threshold this exemption is keyed on")
              .isLessThan(1_000L);

          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(3), 5, 64);

            assertThat(results)
                .as("a small index must answer while every rebuild permit is held elsewhere")
                .isNotEmpty();
            assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
                .as("and must not have queued for one").isZero();
            assertThat(index.getStats().get("graphRebuildCount"))
                .as("it builds inline, as it always has").isEqualTo(1L);
          } finally {
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }
        } finally {
          db.drop();
        }
      }
    }

    /**
     * Writes {@code count} vectors and kills the database rather than closing it, so no graph is ever persisted
     * beside them - the state every index of the reported cluster was in when its pods came back up.
     */
    private void populateAndCrashWithoutPersistingAGraph(final int count) {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          createSchema(db);
          insert(db, count);

          // kill() leaves the graph unpersisted, which is the point; close() after it is what takes the instance
          // out of the factory's active registry so the next session can open the same path.
          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static void createSchema(final Database db) {
      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\" }");
      });
    }

    private static void insert(final Database db, final int count) {
      db.begin();
      for (int i = 0; i < count; i++) {
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        if (i % 500 == 499) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    private static float[] embedding(final int id) {
      final Random random = new Random(0x7814L * 31 + id);
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
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #7930: the small-build exemption from the rebuild permit is bounded in aggregate.
   * <p>
   * Regression test for issue #7930: the exemption that lets a SMALL from-scratch graph build past the JVM-wide
   * rebuild permit must be bounded in aggregate.
   * <p>
   * {@code buildGraphFromScratchUnderRebuildPermit()} lets a build below {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} skip the
   * permit, which is right per build - queueing a millisecond of work behind a multi-minute rebuild is the wrong trade,
   * and a thousand vectors cannot exhaust a heap. It is a per-build test though, and says nothing about how many such
   * builds run at once: a database of hundreds of small vector indexes reopening together starts one build per index,
   * one request thread each, bounded by nothing. Issue #7814's permit narrowed the unbounded set; this is what was
   * left of it.
   * <p>
   * The fix charges each exempt build's scope against a process-wide budget, so that N small builds eventually look
   * like the one large build the permit exists for and take it. The two tests below pin the two halves: the admission
   * rule itself, and the fact that a real search-driven small build honours it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class SmallRebuildBudget {
    private static final String DB_ROOT     = "target/test-databases/SmallRebuildBudget";
    private static final int    DIMENSIONS  = 8;
    /** Comfortably under ASYNC_REBUILD_MIN_GRAPH_SIZE, so the build under test really is on the exempt path. */
    private static final int    NUM_VECTORS = 40;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    /**
     * The bound itself. Charging is by SCOPE rather than by build count, which is what keeps many tiny indexes free
     * while a handful of near-threshold ones already add up to the large build the permit exists for.
     */
    @Test
    void theBudgetAdmitsUntilItIsSpentAndNotPast() {
      final long budget = LSMVectorIndex.smallRebuildBudgetVectors();
      assertThat(budget).as("a budget of zero would send every small build to the permit").isPositive();
      assertThat(LSMVectorIndex.smallRebuildVectorsInFlight())
          .as("no build is in flight, so nothing may be charged").isZero();

      final int half = (int) (budget / 2);
      assertThat(LSMVectorIndex.chargeSmallRebuildBudget(half)).isTrue();
      assertThat(LSMVectorIndex.chargeSmallRebuildBudget(half)).isTrue();
      assertThat(LSMVectorIndex.smallRebuildVectorsInFlight()).isEqualTo(2L * half);

      try {
        // The budget is spent (exactly, for an even budget): the next build is no longer negligible in aggregate.
        assertThat(LSMVectorIndex.chargeSmallRebuildBudget((int) (budget - 2L * half) + 1))
            .as("a charge that would exceed the budget must be refused, not merely logged")
            .isFalse();
        assertThat(LSMVectorIndex.smallRebuildVectorsInFlight())
            .as("a refused charge must leave the budget exactly as it found it")
            .isEqualTo(2L * half);

        // A build with nothing to do consumes nothing, and must never be made to queue for a permit.
        assertThat(LSMVectorIndex.chargeSmallRebuildBudget(0))
            .as("an empty scope has to be admitted whatever the budget holds").isTrue();
        LSMVectorIndex.releaseSmallRebuildBudget(0);
      } finally {
        LSMVectorIndex.releaseSmallRebuildBudget(half);
        LSMVectorIndex.releaseSmallRebuildBudget(half);
      }

      assertThat(LSMVectorIndex.smallRebuildVectorsInFlight())
          .as("every charge is paired with a release, so the budget returns to whole").isZero();
      assertThat(LSMVectorIndex.chargeSmallRebuildBudget((int) budget))
          .as("and the whole of it is available again").isTrue();
      LSMVectorIndex.releaseSmallRebuildBudget((int) budget);
    }

    /**
     * The wiring: a search-driven build small enough to be exempt must still take the permit once the budget is spent.
     * <p>
     * Observed through the permit rather than through a timer: the budget is spent up front from this thread, and
     * every JVM-wide permit is parked, so a small build that honours the budget can only reach its graph by queueing
     * for a permit and timing out - which is counted, and which nothing on the exempt path ever does. Before the fix
     * the build took neither, and both counters stayed at zero.
     */
    @Test
    void aSmallBuildTakesThePermitOnceTheBudgetIsSpent() throws Exception {
      final long previousTimeout = GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong();
      // The wait is MEANT to expire here - the permits are parked for the whole window - so this is sized as the
      // shortest wait that is unambiguously a wait, not as a latency bound.
      GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(250L);
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("the fixture must start with no graph, so the search below has to build one").isZero();

          final int budget = (int) LSMVectorIndex.smallRebuildBudgetVectors();
          assertThat(LSMVectorIndex.chargeSmallRebuildBudget(budget))
              .as("stand in for the other small indexes of a database reopening all at once").isTrue();
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(embedding(1), 5, 64);
            assertThat(hits).as("the search must still be answered: the permit path waits, it never declines")
                .isNotEmpty();
          } finally {
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
            LSMVectorIndex.releaseSmallRebuildBudget(budget);
          }

          assertThat(index.getStats().get("smallRebuildsOverBudget"))
              .as("a small build refused the budget must say so, and by its own name: the remedy is not the one "
                  + "searchRebuildsQueuedForPermit points at")
              .isEqualTo(1L);
          assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
              .as("and it must actually have queued for a permit, which is the bound the exemption was skipping")
              .isEqualTo(1L);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("the build it queued for still has to have happened: the permit path waits, it never declines")
              .isEqualTo(1L);
        } finally {
          if (db.isOpen())
            db.drop();
        }
      } finally {
        GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.setValue(previousTimeout);
      }
    }

    /** The exemption still works when the budget is free: no permit, no counter, just the build. */
    @Test
    void aSmallBuildWithBudgetLeftTakesNoPermit() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);

          final LSMVectorIndex index = vectorIndex(db);

          // Every permit is parked. A build that queued for one would wait out the full timeout; the exempt path
          // never touches the semaphore, so this returns immediately.
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            assertThat(index.findNeighborsFromVector(embedding(1), 5, 64)).isNotEmpty();
          } finally {
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }

          assertThat(index.getStats().get("smallRebuildsOverBudget"))
              .as("the budget was free, so this build stays exempt").isZero();
          assertThat(index.getStats().get("searchRebuildsQueuedForPermit"))
              .as("and must not have queued for a permit that was deliberately unavailable").isZero();
          assertThat(LSMVectorIndex.smallRebuildVectorsInFlight())
              .as("the charge it took for the duration must have been returned").isZero();
        } finally {
          if (db.isOpen())
            db.drop();
        }
      }
    }

    // ---------- harness ----------

    private static void populate(final Database db) {
      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\" }");
      });

      db.transaction(() -> {
        for (int i = 0; i < NUM_VECTORS; i++)
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
      });
    }

    private static float[] embedding(final int id) {
      final Random random = new Random(0x7930L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }
}
