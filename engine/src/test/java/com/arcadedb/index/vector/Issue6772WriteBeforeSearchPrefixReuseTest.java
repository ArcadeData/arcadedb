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
 * Regression test for issue #6772: the ingest-then-query arm of issue #6655's stale-prefix reuse.
 * <p>
 * {@code put()}/{@code putBatch()}/{@code remove()} promote {@code graphState} from {@code LOADING} straight to
 * {@code MUTABLE} on the first write of a session. {@code ensureGraphAvailable()} used to return at its first
 * statement on anything but {@code LOADING}, so a session that wrote before it searched never loaded the persisted
 * graph at all: {@code graphIndex} stayed null, #6655's prefix test was unreachable - not because any of its guards
 * failed, but because the method exited before reaching them - and {@code rebuildGraphBeforeSearch()} then read that
 * null as "small graph" and rebuilt the WHOLE index synchronously on the search thread. Which is the ordinary
 * ingest-then-query shape, and the one #6655's own test does not cover: its searches follow writes made by a
 * PREVIOUS session, never one in the searching session.
 * <p>
 * Three things are pinned, because no one of them alone would fail on the pre-fix code for the right reason:
 * <ul>
 *   <li>the reuse counter and the graph's node count, which say the persisted prefix was reused rather than the
 *       whole live set rebuilt - the two state facts the issue reports as stable across runs while wall clock is
 *       not;</li>
 *   <li>the delta buffer holding each pending vector exactly once, which is what a naive "make the gate reachable"
 *       fix gets wrong: this session's own writes are already in the buffer AND inside the gap past the persisted
 *       prefix, so queuing the gap wholesale would duplicate every one of them;</li>
 *   <li>the answer itself - both a vector written by the crashed session and one written by THIS session, before
 *       the search, must come back from the very first query.</li>
 * </ul>
 * <p>
 * Same fixture as {@code Issue6655StaleGraphPrefixReuseTest} (128 dimensions, {@code maxConnections=64},
 * {@code beamWidth=400}, 20,000 vectors), for the same reason: a full rebuild has to reliably take multiple real
 * seconds, or the timing tripwire cannot fail on the pre-fix code by accident.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
// Spends most of its time waiting on the async rebuild LSMVectorIndex.REBUILD_SEMAPHORE serializes
// JVM-wide (see CLAUDE.md's @Tag("vector") note) - the vector lane is where that convoy costs nobody
// else anything; the slow lane still has other REBUILD_SEMAPHORE waiters this one would convoy against.
@Tag("vector")
class Issue6772WriteBeforeSearchPrefixReuseTest {
  private static final String DB_ROOT         = "target/test-databases/Issue6772WriteBeforeSearchPrefixReuseTest";
  private static final int    DIMENSIONS      = 128;
  private static final int    NUM_VECTORS     = 20_000;
  private static final int    GAP_VECTORS     = 200;
  private static final int    MAX_CONNECTIONS = 64;
  private static final int    BEAM_WIDTH      = 400;

  /** The single record the searching session writes before it searches - the whole point of this test. */
  private static final int IN_SESSION_DOC_ID = NUM_VECTORS + GAP_VECTORS;

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
  void aSessionThatInsertsBeforeItSearchesStillReusesThePersistedGraphAsAPrefix() throws Exception {
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
    // cleanly, so the persisted graph and manifest stay exactly as session 1 left them - describing only the
    // first NUM_VECTORS records - while the live vector count grows. Same setup as
    // Issue6655StaleGraphPrefixReuseTest, which is deliberate: this test differs from that one in session 3 only.
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        populate(db, NUM_VECTORS, NUM_VECTORS + GAP_VECTORS);
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    // Session 3: reopen, INSERT ONE VECTOR, and only then search. That single insert is the difference between
    // this test and Issue6655StaleGraphPrefixReuseTest, and on the pre-fix code it is enough to lose the reuse
    // entirely: the write promotes graphState to MUTABLE, ensureGraphAvailable() returns at its first statement
    // for the rest of the session, and the first search rebuilds all 20,201 vectors synchronously.
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
        assertThat(index.getStats().get("graphState"))
            .as("precondition: the graph is still unloaded - no search has run in this session yet")
            .isEqualTo(0L); // GraphState.LOADING

        populate(db, IN_SESSION_DOC_ID, IN_SESSION_DOC_ID + 1);

        assertThat(index.getStats().get("graphState"))
            .as("precondition: the in-session write must have promoted the state past LOADING - that promotion "
                + "is exactly what used to make the persisted graph unreachable")
            .isEqualTo(2L); // GraphState.MUTABLE
        assertThat(index.getStats().get("deltaVectorsCount"))
            .as("precondition: only this session's own write is buffered so far").isEqualTo(1L);
        assertThat(index.getStats().get("totalVectors"))
            .as("precondition: above the async-rebuild threshold, otherwise this is the pre-existing small-graph "
                + "synchronous path, unaffected by this fix")
            .isGreaterThanOrEqualTo(1000L);

        final RID crashedSessionDocRid = ridOf(db, NUM_VECTORS); // first record written after the persisted build
        final RID inSessionDocRid = ridOf(db, IN_SESSION_DOC_ID);

        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(IN_SESSION_DOC_ID), 5, 64);
        // The stopwatch keeps running across the cheap getStats() assertions below and is asserted on after
        // them - see the comment there.
        // The state facts are asserted before the wall clock, deliberately: the issue reports the elapsed time
        // moving between runs (7.5x to 15.9x) while these two counters do not, so they are what should name the
        // regression when this test goes red, with the timing tripwire below as the corroborating - not the
        // leading - signal.
        assertThat(index.getStats().get("stalePrefixGraphReuses"))
            .as("the reuse path must run for a session that wrote before it searched, exactly as it does for one "
                + "that did not (issue #6772)")
            .isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("no synchronous rebuild may have run on the calling thread for this search to have returned "
                + "already").isZero();
        assertThat(index.getStats().get("graphNodeCount"))
            .as("the graph in memory must be the persisted PREFIX, not a graph rebuilt over the whole live set")
            .isEqualTo((long) NUM_VECTORS);

        // The duplicate check. This session's own write is already in the delta buffer AND inside the gap past
        // the persisted prefix, so a reuse that queued the gap wholesale would buffer it twice - and count it
        // twice in mutationsSinceRebuild.
        assertThat(index.getStats().get("deltaVectorsCount"))
            .as("every vector missing from the persisted prefix must be buffered exactly once: the %d written by "
                + "the crashed session plus the 1 written by this one, with no duplicate of this session's own "
                + "write", GAP_VECTORS)
            .isEqualTo((long) (GAP_VECTORS + 1));
        assertThat(index.getStats().get("mutationsSinceRebuild"))
            .as("and counted exactly once each, for the same reason")
            .isEqualTo((long) (GAP_VECTORS + 1));

        // Measured on this fixture: a prefix reuse returns in well under a second; a full synchronous rebuild of
        // 20,201 vectors at these build parameters takes several real seconds. The bound is a tripwire between
        // the two, wide on both sides.
        stopwatch.assertGaveUpWithin(2_000,
            "a search that reuses a stale prefix graph from one that ran a full synchronous rebuild to completion");

        // Both kinds of pending vector must already be findable by their own embedding - a record is always its
        // own nearest neighbour - proving the reuse did not trade correctness for the latency win.
        assertThat(results.stream().map(Pair::getFirst))
            .as("the vector this session wrote before searching must be found by the very first query")
            .contains(inSessionDocRid);
        assertThat(index.findNeighborsFromVector(embedding(NUM_VECTORS), 5, 64).stream().map(Pair::getFirst))
            .as("so must a vector written by the crashed session, which only the queued gap makes searchable")
            .contains(crashedSessionDocRid);

        // The async rebuild kicked off by the reuse eventually folds everything into the graph proper.
        Awaitility.await("the async rebuild kicked off by the stale-prefix reuse completes")
            .atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L));

        assertThat(index.getStats().get("graphNodeCount"))
            .as("once the deferred rebuild completes the graph covers every live vector")
            .isEqualTo((long) (NUM_VECTORS + GAP_VECTORS + 1));

        final List<Pair<RID, Float>> afterRebuild = index.findNeighborsFromVector(embedding(IN_SESSION_DOC_ID), 5,
            64);
        assertThat(afterRebuild.stream().map(Pair::getFirst))
            .as("and the in-session vector must still be found once it is folded into the graph proper, not only "
                + "while it sat in the delta buffer")
            .contains(inSessionDocRid);
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
      assertThat(rs.hasNext()).as("the record must exist").isTrue();
      return rs.next().getProperty("@rid");
    }
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
