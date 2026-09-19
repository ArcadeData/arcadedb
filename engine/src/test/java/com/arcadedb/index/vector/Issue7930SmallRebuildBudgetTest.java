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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
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
@Tag("vector")
class Issue7930SmallRebuildBudgetTest {
  private static final String DB_ROOT     = "target/test-databases/Issue7930SmallRebuildBudgetTest";
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
