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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6713, a follow-up from #6712's code review.
 * <p>
 * {@code ensureGraphAvailable()}'s persisted-graph reuse path rebuilds {@code ordinalToVectorId} by re-validating
 * every LIVE vector - for a non-quantized (or PRODUCT-quantized) index this is one document lookup plus one
 * property read per vector - to make sure the graph on disk still describes exactly the live set (issues #3722,
 * #6106). That validation is {@code O(live vector count)} and used to run entirely under {@code lock.writeLock()},
 * the same global lock every insert and every search also needs. On an index large enough that the validation takes
 * real time, the first search after a reopen that finds a persisted graph therefore stalled every OTHER reader and
 * writer of the index for the whole scan - exactly the stall issue #5391 already moved
 * {@code buildGraphFromScratchExclusively}'s own (much larger) full-rebuild validation off of. This test targets the
 * up-to-date-reuse branch specifically: the persisted graph matches the live set exactly, so no rebuild is needed at
 * all, only the validation - the one path #5391 never touched because it predates #5391 and sits in a different
 * method.
 * <p>
 * Reproduction: a large index (600,000 vectors of 128 dimensions - vector count, not dimension, is what drives the
 * validation loop long enough to matter here, since each vector still costs one document lookup regardless of its
 * width; a higher dimension was tried first and reproduced the same locking bug, but its ~7x larger heap footprint
 * (600,000 x 1536 floats vs x 128) turned out to make the fixed-size validation workload itself GC-pressure-bound on
 * a memory-constrained CI runner - confirmed locally by reproducing the same order-of-magnitude slowdown, and
 * outright OutOfMemoryError, under a matching -Xmx2g. 128 dimensions keeps the same reproduction memory-safe) is
 * built, persisted and closed cleanly, so the persisted graph and its manifest describe exactly the live set. On
 * reopen, a background thread runs the first search, which drives {@code ensureGraphAvailable()} into the
 * validation loop. While that is in flight, the main thread inserts one more vector - an operation that only ever
 * needs {@code lock.writeLock()} briefly - and times it. Before the fix this insert would queue behind the whole
 * validation loop; after the fix it returns promptly regardless of how long the validation takes, because the loop
 * no longer holds that lock. The search thread is joined with a generous (not tight) timeout: how long the search
 * itself takes to finish is a function of hardware speed, not of what this fix changed, so only insert latency -
 * measured with {@link StallAwareStopwatch} - is asserted against a tight bound.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue6713EnsureGraphAvailableUnlockedValidationTest {
  private static final String DB_ROOT         = "target/test-databases/Issue6713EnsureGraphAvailableUnlockedValidationTest";
  private static final int    DIMENSIONS      = 128;
  private static final int    NUM_VECTORS     = 600_000;
  private static final int    MAX_CONNECTIONS = 8;
  private static final int    BEAM_WIDTH      = 16;

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
  void concurrentInsertIsNotBlockedByTheUnlockedPersistedGraphValidation() throws Exception {
    // Session 1: build and persist a graph over exactly NUM_VECTORS, then close cleanly - the persisted graph and
    // its manifest end up describing precisely the live set, so the next open's validation finds nothing stale.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db, 0, NUM_VECTORS);
        vectorIndex(db).buildVectorGraphNow();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Session 2: reopen. The graph is lazy-loaded on first search (GraphState.LOADING), which is what routes the
    // first search into ensureGraphAvailable()'s persisted-graph validation - the code path this issue targets.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("graphState")).as("precondition: reopen leaves the graph unloaded")
            .isEqualTo(0L); // GraphState.LOADING
        assertThat(index.getStats().get("totalVectors"))
            .as("precondition: large enough that the validation loop takes real time")
            .isEqualTo((long) NUM_VECTORS);

        final CountDownLatch searchStarted = new CountDownLatch(1);
        final AtomicReference<Throwable> searchFailure = new AtomicReference<>();
        final Thread searchThread = new Thread(() -> {
          searchStarted.countDown();
          try {
            index.findNeighborsFromVector(embedding(0), 5);
          } catch (final Throwable t) {
            searchFailure.set(t);
          }
        }, "issue-6713-first-search");
        // Measures the search thread's own total duration, not a fixed millisecond guess: how long an
        // O(NUM_VECTORS) validation loop takes is a function of hardware speed (a >100x gap was measured between a
        // fast workstation and a memory-constrained CI runner for the identical fixture), so the bound the
        // concurrent insert below is measured against is a FRACTION of this run's own search duration rather than
        // an absolute number - self-calibrating to whatever this machine turns out to be.
        final StallAwareStopwatch searchStopwatch = StallAwareStopwatch.start();
        searchThread.start();
        assertThat(searchStarted.await(10, TimeUnit.SECONDS)).as("the search thread must have started").isTrue();
        // A head start into ensureGraphAvailable()'s validation loop: negligible next to the loop's own total
        // duration on any hardware where this fixture is even worth running.
        Thread.sleep(50);

        // A concurrent insert only ever needs lock.writeLock() briefly. Before the fix, ensureGraphAvailable() held
        // that same lock for its ENTIRE persisted-graph validation, so this insert would queue behind the search
        // thread for as long as the validation loop over NUM_VECTORS took - effectively all of searchMs below, not
        // a small fraction of it.
        final StallAwareStopwatch insertStopwatch = StallAwareStopwatch.start();
        db.transaction(() -> db.command("sql", "INSERT INTO Doc SET id = ?, vector = ?", NUM_VECTORS,
            embedding(NUM_VECTORS)));
        final long insertMs = insertStopwatch.effectiveMs();

        // Generous, not tight: how long the search itself takes to finish is a function of hardware speed for an
        // O(NUM_VECTORS) validation loop, not of what this fix changed (the fix is about the LOCK, not the loop's
        // own duration) - a fixed tight bound here previously flaked on a slower CI runner even though nothing was
        // stuck.
        searchThread.join(TimeUnit.MINUTES.toMillis(5));
        assertThat(searchThread.isAlive()).as("the search must complete on its own, not hang").isFalse();
        assertThat(searchFailure.get()).as("the search must not have failed").isNull();
        final long searchMs = searchStopwatch.effectiveMs();

        assertThat(searchMs)
            .as("precondition: NUM_VECTORS must be large enough that the validation loop takes real time on this "
                + "hardware, or the comparison below has no discriminating power")
            .isGreaterThanOrEqualTo(100L);

        // The self-calibrating tripwire: an insert that only ever needs the write lock briefly must take a small
        // FRACTION of however long this run's own validation search took - not scale with it the way a lock held
        // for the whole validation would. A fixed millisecond bound cannot separate the two cases across hardware
        // that differs by two orders of magnitude on the identical fixture; this ratio does, because both
        // measurements come from the same run on the same machine (PR #6720 review: the original fixed 400ms/30s
        // bounds flaked on a CI runner where the whole fixture ran ~100x slower than on a fast workstation).
        assertThat(insertMs)
            .as("an insert that only ever needs the write lock briefly, out of a search that took %,d ms on this "
                + "hardware - not one queued behind the whole persisted-graph validation loop", searchMs)
            .isLessThan(searchMs / 3);

        // Not asserted here: graphRebuildCount. The concurrent insert above is itself a mutation, which - once
        // published - is exactly what schedules the ordinary async rebuild any mutation on a MUTABLE large graph
        // does; whether that background rebuild has completed by the time this line runs is a race against this
        // test's own write, not a signal about which branch ensureGraphAvailable() took. That branch (reuse over
        // rebuild, for a persisted graph that still matches the live set) is covered without a concurrent writer by
        // Issue6106StaleVectorGraphTest.anUntouchedGraphIsStillReusedAcrossARestart().
        assertThat(index.getStats().get("graphState"))
            .as("the graph must have been published (either straight to IMMUTABLE, or to MUTABLE if the concurrent "
                + "insert above raced ahead of the publish) - never left LOADING")
            .isNotEqualTo(0L);
      } finally {
        db.drop();
      }
    }
  }

  private static void populate(final Database db, final int fromInclusive, final int toExclusive) {
    db.transaction(() -> {
      if (db.getSchema().existsType("Doc"))
        return;
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
          + ", \"beamWidth\": " + BEAM_WIDTH + " }");
    });

    db.begin();
    for (int i = fromInclusive; i < toExclusive; i++) {
      db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
      if ((i - fromInclusive) % 2000 == 1999) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  /** Deterministic per-id embedding, so any record's own vector can be reconstructed independently at query time. */
  private static float[] embedding(final int id) {
    final Random random = new Random(0x6713L * 31 + id);
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
