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
import com.arcadedb.engine.PageManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
class Issue7184RebuildAdmissionHeapReadingTest {
  private static final String DB_ROOT    = "target/test-databases/Issue7184RebuildAdmissionHeapReadingTest";
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
