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
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue6503RebuildAdmissionControlTest {
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
