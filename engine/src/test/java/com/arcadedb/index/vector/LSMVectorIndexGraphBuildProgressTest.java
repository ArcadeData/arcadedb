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
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5577: the graph-build progress meter reported JVector's
 * {@code getIdUpperBound()}, which is "highest node id touched so far + 1". Insertion runs as a parallel stream over
 * the whole ordinal range, so a worker reaches the top of that range almost immediately and the meter pinned at 100%
 * with nearly all the work still ahead. On a 10M-vector corpus the very first progress line read 90.7% and the last
 * one claimed completion 23 minutes early, which made the build look like it had a second, undocumented phase.
 * <p>
 * The invariant that separates the two implementations is {@code processedNodes + insertsInProgress <= totalNodes}:
 * a node is counted once its insertion has returned, so it can never be counted and in flight at the same time. The
 * old meter violated it as soon as the range was split - the reporter's own log line read
 * {@code 9990000/9990000 (vector accesses=9990006)}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMVectorIndexGraphBuildProgressTest {
  private static final String DB_ROOT = "target/test-databases/LSMVectorIndexGraphBuildProgressTest";

  /** One database per test. They build the same index concurrently enough that sharing a path makes them interfere. */
  private String dbPath;
  private static final int    DIMENSIONS  = 128;
  private static final int    NUM_VECTORS = 4_000;

  /** Index shape used by the progress test only, chosen so one build lasts well beyond the 100ms progress poll. */
  private static final int    SLOW_BUILD_MAX_CONNECTIONS = 64;
  private static final int    SLOW_BUILD_BEAM_WIDTH      = 400;

  private record Sample(String phase, int processedNodes, int totalNodes, long vectorAccesses) {
  }

  @BeforeEach
  void setUp(final TestInfo testInfo) {
    dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
    FileUtils.deleteRecursively(new File(dbPath));
    // Ingest must not trigger its own rebuilds: every one of them is a full graph build, and on this corpus that
    // turns a two-second test into a half-hour one. The tests here drive the single build they measure explicitly.
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(Integer.MAX_VALUE);
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(0);
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.setValue(
        GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD.getDefValue());
    GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.setValue(
        GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS.getDefValue());
    FileUtils.deleteRecursively(new File(dbPath));
  }

  @Test
  void progressNeverReportsMoreWorkDoneThanTheBuildHasActuallyDone() {
    final List<Sample> samples = new CopyOnWriteArrayList<>();

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      try (final Database db = factory.create()) {
        // A wide beam and a high degree make each insertion expensive, so the build spans several 100ms progress
        // polls even on a warm JVM. Without that the whole insertion finishes inside one poll and the only samples
        // are the two boundary ones, which the old meter would have passed - the guard below is what catches that.
        populate(db, NUM_VECTORS, SLOW_BUILD_MAX_CONNECTIONS, SLOW_BUILD_BEAM_WIDTH);

        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow(
            (phase, processedNodes, totalNodes, vectorAccesses) -> samples.add(
                new Sample(phase, processedNodes, totalNodes, vectorAccesses)));

        final List<Sample> building = samples.stream().filter(s -> "building".equals(s.phase())).toList();
        assertThat(building).as("the build must have reported at least one 'building' sample").isNotEmpty();

        // Guard against a vacuous pass. The old meter is only observably wrong on a sample taken while insertion is
        // still running, so a test that only ever saw the boundary samples would pass against it. If this fires, the
        // build finished inside one 100ms poll and the corpus needs to grow.
        assertThat(building)
            .as("at least one sample must have been taken mid-insertion, otherwise nothing here is discriminating")
            .anyMatch(s -> s.processedNodes() > 0 && s.processedNodes() < s.totalNodes());

        for (final Sample s : building) {
          assertThat(s.processedNodes()).as("processed %d of %d", s.processedNodes(), s.totalNodes())
              .isBetween(0, s.totalNodes());
          // The old meter failed exactly here: it reported the whole corpus as done while inserts were still in
          // flight, so accesses ran past the corpus size.
          //
          // MAINTENANCE: this strict bound also encodes a JVector timing guarantee. It holds because
          // GraphIndexBuilder.addGraphNode does insertionsInProgress.remove(nodeLevel) in a finally block, so a node
          // has left insertsInProgress() by the time the engine counts it - never both at once. Verified against
          // 4.0.0-rc.9; re-read it along with build() when bumping jvector.version, since a version that dropped the
          // in-flight marker after returning would make a single sample read totalNodes + 1 and flake this.
          assertThat(s.vectorAccesses()).as("vector accesses %d of %d nodes", s.vectorAccesses(), s.totalNodes())
              .isLessThanOrEqualTo(s.totalNodes());
        }

        // Progress only ever moves forward, and the last "building" sample accounts for the whole corpus.
        int previous = -1;
        for (final Sample s : building) {
          assertThat(s.processedNodes()).isGreaterThanOrEqualTo(previous);
          previous = s.processedNodes();
        }
        assertThat(building.get(building.size() - 1).processedNodes()).isEqualTo(building.get(building.size() - 1).totalNodes());

        // Only the documented phases are reported, and JVector's post-insertion pass is one of them: it used to be
        // invisible, which is what let a phase worth half the wall clock of a large build go unnoticed.
        final Set<String> phases = samples.stream().map(Sample::phase).collect(Collectors.toSet());
        assertThat(phases).isSubsetOf(Set.of("validating", "building", "optimizing", "persisting"));
        assertThat(phases).contains("optimizing");

        // ...and the index still answers after a build driven through the unrolled insertion + cleanup path.
        assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
      }
    }
  }

  @Test
  void buildPoolWidthFollowsTheConfiguredParallelism() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      try (final Database db = factory.create()) {
        // A small corpus and the default index shape are enough here: this test only checks the pool width, not
        // the progress stream, so it does not need a build long enough to span several progress polls.
        populate(db, 500, 16, 100);

        final LSMVectorIndex index = vectorIndex(db);

        // 0 (the default) means "all cores but one", so a rebuild on a live index cannot occupy every core the
        // request, I/O and GC threads need. It is no longer availableProcessors()/2, which cost 17.1% of a
        // DEEP-10M build for no measured benefit (issue #5577).
        assertThat(index.getStats().get("graphBuildParallelism"))
            .isEqualTo((long) Math.max(1, Runtime.getRuntime().availableProcessors() - 1));

        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.setValue(1);
        try {
          assertThat(index.getStats().get("graphBuildParallelism")).isEqualTo(1L);

          // A single-threaded pool must still produce a working graph: the insertion is driven through the same
          // parallel stream, it just has one worker.
          index.buildVectorGraphNow(null);
          assertThat(index.findNeighborsFromVector(queryVector(), 5, 64)).hasSize(5);
        } finally {
          GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.setValue(
              GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.getDefValue());
        }

        assertThat(index.getStats().get("graphBuildParallelism"))
            .isEqualTo((long) Math.max(1, Runtime.getRuntime().availableProcessors() - 1));

        // A typo in the setting must not turn every rebuild into an IllegalArgumentException from the pool
        // constructor: ForkJoinPool refuses a parallelism above 0x7fff, so the configured value is clamped.
        GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.setValue(1_000_000);
        try {
          assertThat(index.getStats().get("graphBuildParallelism")).isEqualTo(0x7fffL);
        } finally {
          GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.setValue(
              GlobalConfiguration.VECTOR_INDEX_GRAPH_BUILD_PARALLELISM.getDefValue());
        }
      }
    }
  }

  private static void populate(final Database db, final int vectors, final int maxConnections, final int beamWidth) {
    final Random rnd = new Random(7);

    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"maxConnections\": " + maxConnections
          + ", \"beamWidth\": " + beamWidth + " }");
    });

    db.begin();
    for (int i = 0; i < vectors; i++) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      db.newDocument("Doc").set("id", i).set("vector", vector).save();
      if (i % 1000 == 999) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }

  private static float[] queryVector() {
    final float[] query = new float[DIMENSIONS];
    final Random rnd = new Random(99);
    for (int d = 0; d < DIMENSIONS; d++)
      query[d] = rnd.nextFloat();
    return query;
  }
}
