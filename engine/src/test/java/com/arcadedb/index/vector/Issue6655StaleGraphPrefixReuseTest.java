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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6655: a follow-up to #6067's close-time deferral. {@code ensureGraphAvailable()}'s
 * stale-persisted-graph fallback (issue #3722 - a persisted graph with fewer nodes than there are live vectors,
 * because vectors were added after the graph was last built and persisted) called {@code buildGraphFromScratch()}
 * unconditionally and SYNCHRONOUSLY, with no size-based deferral of its own - unlike
 * {@link LSMVectorIndex#rebuildGraphBeforeSearch()}, which already prefers an async rebuild above
 * {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} (1000) vectors for the identical staleness reached mid-session.
 * <p>
 * Net effect before the fix: for a large index, the FIRST search after a reopen that finds a stale persisted graph
 * pays the full rebuild cost inline, on the calling search thread - the same wall-clock cost #6067 moved off
 * {@code close()}, just relocated to whichever query happens to run first. The fix reuses the persisted graph
 * immediately as a verified PREFIX of the live set (manifest-verified, no deletions) instead, queues the vectors
 * past it into the delta buffer so nothing is silently missing from the answer, and kicks the same async rebuild
 * {@code rebuildGraphBeforeSearch()} would use mid-session.
 * <p>
 * Two things are pinned, because either alone would pass against a half-fix: the first search must return promptly
 * (not merely "eventually", which an async-but-still-blocking variant could also satisfy) AND it must already
 * answer correctly for a vector written after the persisted build - proving the fix does not trade away the very
 * data the synchronous rebuild used to guarantee was found.
 * <p>
 * Same per-vector build parameters and vector count as {@code Issue6067DeferredCloseRebuildTest} (128 dimensions,
 * {@code maxConnections=64}, {@code beamWidth=400}, 20,000 vectors), for the same reason: a full rebuild has to
 * reliably take multiple real seconds, or the timing tripwire below cannot fail on the pre-fix code by accident.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
// Spends most of its time waiting on the async rebuild LSMVectorIndex.REBUILD_SEMAPHORE serializes
// JVM-wide (see CLAUDE.md's @Tag("vector") note) - the vector lane is where that convoy costs nobody
// else anything; the slow lane still has other REBUILD_SEMAPHORE waiters this one would convoy against.
@Tag("vector")
class Issue6655StaleGraphPrefixReuseTest {
  private static final String DB_ROOT         = "target/test-databases/Issue6655StaleGraphPrefixReuseTest";
  private static final int    DIMENSIONS      = 128;
  private static final int    NUM_VECTORS     = 20_000;
  private static final int    GAP_VECTORS     = 200;
  private static final int    MAX_CONNECTIONS = 64;
  private static final int    BEAM_WIDTH      = 400;

  // How long to let the async rebuild of 20,200 x 128 vectors converge. Sized for the worst case rather
  // than for a developer machine, which is what the @Tag("vector") note in CLAUDE.md asks for: the rebuild
  // has to wait out whatever else in this JVM holds the sole LSMVectorIndex.REBUILD_SEMAPHORE permit before
  // it can even start, and then runs on however many cores the runner has. At 60s a green local run
  // (~10s for this whole class) still went red on a 4-vCPU CI runner with the rebuild reporting itself
  // still in flight (run 33161892670). This is a convergence wait, not a latency bound - it asserts that
  // the rebuild happens, not that it is fast - so a generous value cannot turn a passing run red, and the
  // assertion below distinguishes "still working" from "never ran" for whoever reads the next timeout.
  private static final Duration ASYNC_REBUILD_TIMEOUT = Duration.ofMinutes(4);

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
  void reopenWithMoreVectorsThanThePersistedGraphReusesItAsAPrefixInsteadOfBlockingOnAFullRebuild() throws Exception {
    // Session 1: build and persist a graph over exactly NUM_VECTORS, then close cleanly - the persisted graph and
    // its manifest end up describing precisely the first NUM_VECTORS records.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db, 0, NUM_VECTORS);
        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();
        assertThat(index.getStats().get("graphState")).as("precondition: graph must be built and IMMUTABLE first")
            .isEqualTo(1L); // GraphState.IMMUTABLE
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Session 2: write GAP_VECTORS more records (committed, so WAL-durable), then crash instead of closing
    // cleanly. On main, close()/flush() would rebuild the graph synchronously and erase the very staleness this
    // test needs; kill() skips that, leaving the persisted graph and manifest exactly as session 1 left them -
    // still describing only the first NUM_VECTORS records - while the live vector count is now NUM_VECTORS +
    // GAP_VECTORS.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        populate(db, NUM_VECTORS, NUM_VECTORS + GAP_VECTORS);
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("graphState"))
            .as("precondition: MUTABLE, otherwise this session never reaches a state kill() can usefully crash")
            .isEqualTo(2L); // GraphState.MUTABLE

        // Crash: the on-disk state (files, WAL) is left exactly as an abrupt kill leaves it - the persisted graph
        // and manifest untouched, the extra records only in the WAL. close() right after, on this same already-
        // killed instance, only clears the process-wide active-instance bookkeeping DatabaseFactory.open() checks
        // (issue #6655's test setup, same pattern as UnflushedDictionaryRecoveryTest) - it does not get a chance
        // to flush or rebuild anything the crash already bypassed.
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Session 3: reopen. WAL recovery restores all NUM_VECTORS + GAP_VECTORS records; the persisted graph and
    // manifest still describe only the first NUM_VECTORS. This is the reproduction: a stale persisted graph, more
    // live vectors than it covers, no deletions, a manifest-verifiable prefix.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc")) {
          assertThat(rs.next().<Long>getProperty("cnt"))
              .as("precondition: WAL recovery must have restored every record from the crashed session")
              .isEqualTo((long) (NUM_VECTORS + GAP_VECTORS));
        }

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("precondition: nothing must have rebuilt yet in this fresh session").isZero();
        assertThat(index.getStats().get("totalVectors"))
            .as("precondition: above the async-rebuild threshold, otherwise this is the pre-existing small-graph "
                + "synchronous path, unaffected by this fix")
            .isGreaterThanOrEqualTo(1000L);

        // The query is one of the gap records' own embedding: a record is always its own nearest neighbour, so
        // finding it in the answer proves the gap vectors are searchable from the very first query - not only
        // once the deferred rebuild eventually runs.
        final int gapDocId = NUM_VECTORS; // first record written after the persisted build
        final RID gapDocRid = ridOf(db, gapDocId);

        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(gapDocId), 5, 64);
        // Measured on this fixture: a prefix reuse returns in well under a second; a full synchronous rebuild of
        // 20,200 vectors at these build parameters takes several real seconds (see Issue6067DeferredCloseRebuildTest
        // for the same measurement on an equivalent fixture). The bound is a tripwire between the two, wide on
        // both sides.
        stopwatch.assertGaveUpWithin(2_000,
            "a search that reuses a stale prefix graph from one that ran a full synchronous rebuild to completion");

        assertThat(results.stream().map(Pair::getFirst))
            .as("the gap record must be found by its own embedding on the very first query, proving the queued "
                + "delta entries - not only the reused graph - are already searchable")
            .contains(gapDocRid);

        assertThat(index.getStats().get("graphState"))
            .as("the reuse must leave the index MUTABLE (graph + queued delta), not a synchronously-rebuilt "
                + "IMMUTABLE graph")
            .isEqualTo(2L); // GraphState.MUTABLE
        assertThat(index.getStats().get("stalePrefixGraphReuses"))
            .as("the reuse path introduced by this fix must be the one that actually ran").isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("no synchronous rebuild may have run on the calling thread for this search to have returned "
                + "already").isZero();

        // The async rebuild kicked off by the reuse eventually folds the gap into the graph proper.
        Awaitility.await("the async rebuild kicked off by the stale-prefix reuse completes")
            .atMost(ASYNC_REBUILD_TIMEOUT)
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount"))
                .as("graphRebuildCount, with asyncRebuildInProgress=%s: a 1 there means the rebuild is still "
                        + "running or still parked on the JVM-wide REBUILD_SEMAPHORE and this wait is simply "
                        + "too short; a 0 means it was never started or was skipped, which is a real defect",
                    index.getStats().get("asyncRebuildInProgress"))
                .isEqualTo(1L));

        assertThat(index.getStats().get("graphState"))
            .as("once the deferred rebuild completes the graph goes back to IMMUTABLE").isEqualTo(1L);

        final List<Pair<RID, Float>> afterRebuild = index.findNeighborsFromVector(embedding(gapDocId), 5, 64);
        assertThat(afterRebuild.stream().map(Pair::getFirst))
            .as("the gap record must still be found once it is folded into the graph proper, not only while it "
                + "sat in the delta buffer")
            .contains(gapDocRid);
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
      if ((i - fromInclusive) % 1000 == 999) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  /** Deterministic per-id embedding, so any record's own vector can be reconstructed independently at query time. */
  private static float[] embedding(final int id) {
    final Random random = new Random(0x6655L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static RID ridOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
      assertThat(rs.hasNext()).as("the gap record must exist").isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
