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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.time.Duration;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The first search after a mutation below the rebuild threshold must not start a full async graph rebuild
 * (issue #7183).
 * <p>
 * Scenario, from the report: a persisted graph over N vectors; a session inserts ONE vector and closes without
 * searching; the next session searches once. {@code ensureGraphAvailable()} correctly reuses the persisted graph as
 * a stale prefix (issue #6655) and queues that one vector into the delta buffer, and then
 * {@code reuseStalePrefixGraph()} used to call {@code startAsyncGraphRebuild()} unconditionally - which logged
 * "accumulated 1 mutations, threshold: 100" and rebuilt every node in the graph anyway. {@code close()} then joined
 * that thread for up to five seconds, and because the rebuild was cancelled before it could persist, the next
 * session found the same one vector queued and did it all again. So a process that opens, searches once and exits
 * paid a full rebuild plus a five-second close, every time, for one mutation. The inactivity timer had the same
 * shape and learned to respect the threshold in #6857; this pins the search-path door.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue7183StalePrefixReuseRebuildThresholdTest {
  private static final String   DB_ROOT    = "target/test-databases/Issue7183StalePrefixReuseRebuildThresholdTest";
  private static final int      DIMENSIONS = 32;
  private static final int      COUNT      = 1_500;
  private static final int      THRESHOLD  = 100;
  private static final Duration REBUILD_SETTLE_TIMEOUT =
      Duration.ofMillis(GlobalConfiguration.VECTOR_INDEX_REBUILD_PERMIT_TIMEOUT_MS.getValueAsLong() + 60_000L);

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
  void firstSearchAfterOneInsertMustNotRebuildTheWholeGraph() {
    buildAndPersistFixture();
    insertWithoutSearching(1);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      configure(db);
      try {
        search(db);

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("stalePrefixGraphReuses"))
            .as("precondition: the search reused the persisted graph as a stale prefix")
            .isEqualTo(1L);
        assertThat(index.getStats().get("mutationsSinceRebuild"))
            .as("precondition: exactly one mutation is pending, well under the threshold of " + THRESHOLD)
            .isEqualTo(1L);

        // The defect. One queued vector is served from the delta scan; nothing about it justifies rebuilding all
        // COUNT nodes, and the mutation threshold says so.
        assertThat(index.getStats().get("asyncRebuildInProgress"))
            .as("one mutation below the threshold must not start an async rebuild on the first search")
            .isZero();
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("and must not have completed one either")
            .isZero();

        // The consequence a user sees: close() joins the rebuild thread for up to 5 s. Generous bound, and
        // deliberately not a latency assertion - the point is "not five seconds", so a stall cannot make it wrong
        // in the direction that matters.
        final long startedAt = System.nanoTime();
        db.close();
        assertThat((System.nanoTime() - startedAt) / 1_000_000L)
            .as("close() must not wait on a rebuild that had no reason to start")
            .isLessThan(4_000L);
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  /**
   * The vector queued by the reuse stays searchable throughout: the point of the fix is that the delta scan serves
   * it, not that it is dropped. Without this, "no rebuild" could be passing for the wrong reason.
   */
  @Test
  void theOneQueuedVectorIsStillFoundWithoutARebuild() {
    buildAndPersistFixture();
    insertWithoutSearching(1);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      configure(db);
      try {
        assertThat(idsNear(db, embedding(COUNT), 5))
            .as("the vector the write-only session added must come back from the delta scan")
            .contains(COUNT);
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("deltaVectorsCount"))
            .as("...which is where it is: queued, not folded into the graph")
            .isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  /**
   * Positive control: with pending mutations AT the threshold the first search is entitled to rebuild, and does.
   * Without this the assertions above would hold just as well on a build where the async path never ran at all.
   */
  @Test
  void firstSearchAtTheThresholdStillRebuilds() {
    buildAndPersistFixture();
    insertWithoutSearching(THRESHOLD);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      configure(db);
      try {
        search(db);
        final LSMVectorIndex index = vectorIndex(db);
        Awaitility.await("a search with the threshold reached rebuilds the graph")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isPositive());
        Awaitility.await("the rebuild settles before the close")
            .atMost(REBUILD_SETTLE_TIMEOUT)
            .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress")).isZero());
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private void buildAndPersistFixture() {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      configure(db);
      try {
        insert(db, 0, COUNT);
        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();
        assertThat(index.getStats().get("graphState"))
            .as("precondition: the graph must be built and IMMUTABLE before the close that persists it")
            .isEqualTo(1L); // GraphState.IMMUTABLE
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  /** A session that writes and leaves: the shape of a batch job, or of a client that inserts and exits. */
  private void insertWithoutSearching(final int howMany) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      configure(db);
      try {
        insert(db, COUNT, COUNT + howMany);
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static void search(final Database db) {
    assertThat(idsNear(db, halfway(), 5)).as("the search returns neighbours").isNotEmpty();
  }

  private static java.util.List<Integer> idsNear(final Database db, final float[] query, final int k) {
    final StringBuilder vector = new StringBuilder();
    for (int d = 0; d < DIMENSIONS; d++)
      vector.append(d == 0 ? "" : ", ").append(query[d]);

    final java.util.List<Integer> ids = new java.util.ArrayList<>(k);
    try (final ResultSet rs = db.query("sql",
        "SELECT id FROM (SELECT expand(vectorNeighbors('Doc[vector]', [" + vector + "], " + k + ")))")) {
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
    }
    return ids;
  }

  private static float[] halfway() {
    final float[] query = new float[DIMENSIONS];
    java.util.Arrays.fill(query, 0.5f);
    return query;
  }

  private static void configure(final Database db) {
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, THRESHOLD);
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
    // Keep the inactivity timer out of the picture: this test is about the search path.
    db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 600_000);
  }

  private static void insert(final Database db, final int fromInclusive, final int toExclusive) {
    db.transaction(() -> {
      if (db.getSchema().existsType("Doc"))
        return;
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"EUCLIDEAN\" }");
    });
    db.begin();
    for (int i = fromInclusive; i < toExclusive; i++) {
      db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
      if ((i - fromInclusive) % 500 == 499) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x7150L * 31 + id);
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
