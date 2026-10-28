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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.quantization.MutablePQVectors;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10568: a PRODUCT-quantized index with nothing deleted must reopen through the ordinal map
 * recorded next to its graph (issue #8852) instead of the per-vector document walk, and must not log that vectors were
 * deleted when none were. With a deletion the PRODUCT index keeps the rebuild (issue #7842), and an index persisted
 * without a map still opens.
 */
@Tag("vector")
class Issue10568ProductReopenOrdinalMapTest {
  private static final String DB_ROOT     = "target/test-databases/Issue10568ProductReopenOrdinalMapTest";
  private static final int    DIMENSIONS  = 32;
  private static final int    NUM_VECTORS = 2_000;
  private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

  private static final String FALSE_LINE  = "Deleted vectors detected";
  private static final String WALK_LINE   = "Loaded graph from disk for index";

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
  void aProductIndexWithNothingDeletedReopensThroughItsOrdinalMap() {
    final Built built = build("PRODUCT");

    assertThat(built.ordinalMapSize).as("precondition: a PRODUCT index records the ordinal map of its graph")
        .isEqualTo(NUM_VECTORS);
    assertThat(built.pqFileExists).as("precondition: and has its PQ on disk").isTrue();

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

        assertThat(neighbours(results)).as("the answer after the reopen is the one the session that built it gave")
            .isEqualTo(built.neighbours);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
            .as("the ordinal map must replace the per-vector document walk for a PRODUCT index too").isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
        assertThat(logs).as("nothing was deleted, so nothing may say so").noneMatch(m -> m.contains(FALSE_LINE));
        assertThat(logs).as("and the walk that re-reads every vector's document must not have run")
            .noneMatch(m -> m.contains(WALK_LINE));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aProductIndexWithADeletionStillRebuildsAndSaysWhy() {
    build("PRODUCT");

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        db.begin();
        db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
        db.commit();

        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("deletedVectors"))
            .as("precondition: read before the first search, which is what discards the tombstone set").isEqualTo(1L);

        final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 5, 64));

        assertThat(results).hasSize(5);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
            .as("a deletion must never take the exact-cover path").isZero();
        assertThat(index.getStats().get("graphReusesWithTombstonedNodes"))
            .as("a PRODUCT index must never take the tombstone-tolerant reuse path").isZero();
        assertThat(index.getStats().get("graphRebuildCount"))
            .as("it pays the full rebuild, which keeps its codebook and its graph on one dense ordinal space")
            .isEqualTo(1L);
        assertThat(logs).as("and the line about the deletion is still there")
            .anyMatch(m -> m.contains(FALSE_LINE) && m.contains("PRODUCT quantization"));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aProductIndexPersistedWithoutAMapStillOpensThroughTheWalk() throws IOException {
    final Built built = build("PRODUCT");

    // What an index persisted before the map was recorded for PRODUCT looks like: the same manifest, no .vecordmap
    final Path map = new LSMVectorIndexGraphManifest(built.graphPath.toString()).getOrdinalMapFilePath();
    assertThat(Files.deleteIfExists(map)).as("precondition: there was a map to remove").isTrue();

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

        assertThat(neighbours(results)).as("the walk answers as it always did").isEqualTo(built.neighbours);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("there is no map to load through").isZero();
        assertThat(index.getStats().get("graphRebuildCount")).as("the persisted graph is reused, not rebuilt").isZero();
        assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
        assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
        assertThat(logs).as("and nothing was deleted, so nothing may say so").noneMatch(m -> m.contains(FALSE_LINE));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void aNonQuantizedIndexStillReopensThroughItsOrdinalMap() {
    final Built built = build("NONE");

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

        assertThat(neighbours(results)).isEqualTo(built.neighbours);
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).isEqualTo(1L);
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(logs).noneMatch(m -> m.contains(FALSE_LINE)).noneMatch(m -> m.contains(WALK_LINE));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void pqCodesNotOnePerOrdinalOfTheMapTakeTheWalk() throws IOException {
    final Built built = build("PRODUCT");

    // A PQ file of another generation of the graph that happens to carry the right identity: only the count tells
    final LSMVectorIndexPQFile pqFile = pqFileOf(built);
    assertThat(pqFile.loadPQ()).isTrue();
    final ProductQuantization pq = pqFile.getProductQuantization();
    final Long fingerprint = pqFile.graphFingerprintOf(pqFile.getPQVectors());
    assertThat(fingerprint).as("precondition: a PQ written with its graph records which graph that is").isNotNull();
    final MutablePQVectors fewer = new MutablePQVectors(pq);
    for (int i = 0; i < NUM_VECTORS - 100; i++)
      fewer.encodeAndSet(i, VTS.createFloatVector(embedding(i)));
    pqFile.writePQ(pq, fewer, fingerprint);

    final List<String> logs = new ArrayList<>();
    reopenAndSearch(logs, (index, results) -> {
      assertThat(results).hasSize(10);
      assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("the count says these codes are not the map's")
          .isZero();
      assertThat(logs).anyMatch(m -> m.contains("PQ codes of index") && m.contains("not built for it"));
    });
  }

  @Test
  void pqCodesOfAnotherGraphOfTheSameSizeTakeTheWalk() throws IOException {
    final Built built = build("PRODUCT");

    // Same count, same codes, but recorded as built for a different assignment of ordinals
    final LSMVectorIndexPQFile pqFile = pqFileOf(built);
    assertThat(pqFile.loadPQ()).isTrue();
    final PQVectors codes = pqFile.getPQVectors();
    final long fingerprint = pqFile.graphFingerprintOf(codes);
    pqFile.writePQ(pqFile.getProductQuantization(), codes, fingerprint ^ 1L);

    final List<String> logs = new ArrayList<>();
    reopenAndSearch(logs, (index, results) -> {
      assertThat(neighbours(results)).isEqualTo(built.neighbours);
      assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
          .as("a count that matches is not an identity that matches").isZero();
      assertThat(index.getStats().get("graphRebuildCount")).as("the persisted graph is still reused, by the walk").isZero();
      assertThat(logs).anyMatch(m -> m.contains("were built for another graph than the persisted one"));
      assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
    });
  }

  @Test
  void aPqFileWithoutARecordedGraphTakesTheWalk() throws IOException {
    final Built built = build("PRODUCT");

    // What a PQ file written before the identity was recorded looks like: the same bytes without the 16-byte trailer
    final Path pq = built.pqPath;
    final byte[] bytes = Files.readAllBytes(pq);
    Files.write(pq, Arrays.copyOf(bytes, bytes.length - LSMVectorIndexPQFile.GRAPH_IDENTITY_BYTES));
    final LSMVectorIndexPQFile check = pqFileOf(built);
    assertThat(check.loadPQ()).as("precondition: it still loads, the trailer is not part of what JVector reads").isTrue();
    assertThat(check.graphFingerprintOf(check.getPQVectors())).isNull();

    final List<String> logs = new ArrayList<>();
    reopenAndSearch(logs, (index, results) -> {
      assertThat(neighbours(results)).isEqualTo(built.neighbours);
      assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).isZero();
      assertThat(index.getStats().get("graphRebuildCount")).isZero();
      assertThat(logs).anyMatch(m -> m.contains("record no graph they were built for"));
      assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
    });
  }

  @Test
  void aVectorDeletedAfterTheCountWasReadTakesTheWalk() {
    build("PRODUCT");

    // The exact-cover reuse is only called when nothing is deleted. A delete landing after that decision is the one a
    // test cannot time, so this makes it land first and calls the reuse directly: the validation of the map must see it
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        index.findNeighborsFromVector(embedding(1), 5, 64); // the persisted graph and its PQ are loaded
        db.begin();
        db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
        db.commit();
        assertThat(index.getStats().get("deletedVectors")).as("precondition: one tombstone").isEqualTo(1L);

        final List<String> logs = new ArrayList<>();
        final boolean answered = capturingLogs(logs, index::exactCoverReopenAnswersForTest);

        assertThat(answered).as("with a hole in the dense ordinal space the PQ pairing no longer holds").isFalse();
        assertThat(logs).anyMatch(m -> m.contains("was deleted while its persisted graph was being validated"));
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void anIndexCreatedOverExistingDataRecordsItsGraphAndReopensThroughTheMap() throws IOException {
    createOverExistingData(true);
    assertPqRecordsItsGraphAndReopenTakesTheMap();
  }

  @Test
  void anIndexCreatedOverExistingDataAndClosedBeforeAnySearchRecordsItsGraphToo() throws IOException {
    // No search in the creating session: the PQ is whatever the build and the close left behind
    createOverExistingData(false);
    assertThat(Files.exists(pqPathAfterCreate)).as("precondition: the creating session left a PQ file").isTrue();
    assertPqRecordsItsGraphAndReopenTakesTheMap();
  }

  @Test
  void aGraphRebuiltForAMissingPqFileRecordsItsGraphWithTheNewCodes() throws IOException {
    final Built built = build("PRODUCT");

    // No PQ file: the next open rebuilds the graph and the codes together, and the codes have to be recorded as built
    // for the graph that rebuild certified, not for whichever manifest happens to be on disk
    assertThat(Files.deleteIfExists(built.pqPath)).as("precondition: there was a PQ file to remove").isTrue();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final List<String> logs = new ArrayList<>();
        final LSMVectorIndex index = vectorIndex(db);
        final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));
        assertThat(results).hasSize(10);
        assertThat(index.getStats().get("graphRebuildCount")).as("graph and codes are rebuilt together").isEqualTo(1L);
        assertThat(logs).anyMatch(m -> m.contains("there is no usable PQ file"));
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    final LSMVectorIndexPQFile pqFile = pqFileOf(built);
    assertThat(pqFile.loadPQ()).as("precondition: the rebuild persisted its codes").isTrue();
    final LSMVectorIndexGraphManifest.Content manifest = new LSMVectorIndexGraphManifest(built.graphPath.toString()).read();
    assertThat(manifest).isNotNull();
    assertThat(pqFile.graphFingerprintOf(pqFile.getPQVectors()))
        .as("the rebuilt codes record the fingerprint certified for the rebuilt graph").isEqualTo(manifest.fingerprint());

    final List<String> logs = new ArrayList<>();
    reopenAndSearch(logs, (index, results) -> {
      assertThat(results).hasSize(10);
      assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("so the next reopen takes the ordinal map").isEqualTo(1L);
      assertThat(index.getStats().get("graphRebuildCount")).isZero();
      assertThat(logs).as("and not the walk").noneMatch(m -> m.contains(WALK_LINE));
    });
  }

  private Path pqPathAfterCreate;
  private Path graphPathAfterCreate;

  /** CREATE INDEX over documents that are already there, the common case; the database is left closed. */
  private void createOverExistingData(final boolean searchBeforeClosing) {
    final Path graphPath;
    final Path pqPath;
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        });
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          if (i % 1000 == 999) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
        db.transaction(() -> db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\", \"quantization\": \"PRODUCT\" }"));

        final LSMVectorIndex index = vectorIndex(db);
        if (searchBeforeClosing)
          assertThat(index.findNeighborsFromVector(embedding(1), 10, 64)).hasSize(10);
        graphPath = index.getGraphFilePathForTest();
        pqPath = index.getProductQuantizationFilePath();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
    graphPathAfterCreate = graphPath;
    pqPathAfterCreate = pqPath;
  }

  private void assertPqRecordsItsGraphAndReopenTakesTheMap() throws IOException {
    final Path graphPath = graphPathAfterCreate;
    final Path pqPath = pqPathAfterCreate;
    final Built built = new Built(Set.of(), graphPath, NUM_VECTORS, Files.exists(pqPath), pqPath);
    assertThat(built.pqFileExists).as("precondition: the first session left a PQ file").isTrue();
    final LSMVectorIndexPQFile pqFile = pqFileOf(built);
    assertThat(pqFile.loadPQ()).isTrue();
    final Long recorded = pqFile.graphFingerprintOf(pqFile.getPQVectors());
    final LSMVectorIndexGraphManifest.Content manifest = new LSMVectorIndexGraphManifest(graphPath.toString()).read();
    assertThat(manifest).isNotNull();
    assertThat(recorded).as("the PQ of an index created over existing data records the graph it was built for").isNotNull();
    assertThat(recorded).isEqualTo(manifest.fingerprint());

    final List<String> logs = new ArrayList<>();
    reopenAndSearch(logs, (index, results) -> {
      assertThat(results).hasSize(10);
      assertThat(index.getStats().get("graphLoadsFromOrdinalMap")).as("the reopen takes the ordinal map").isEqualTo(1L);
      assertThat(index.getStats().get("graphRebuildCount")).isZero();
      assertThat(logs).as("and not the walk").noneMatch(m -> m.contains(WALK_LINE));
    });
  }

  private LSMVectorIndexPQFile pqFileOf(final Built built) {
    final String file = built.pqPath.toString();
    return new LSMVectorIndexPQFile(file.substring(0, file.length() - ("." + LSMVectorIndexPQFile.FILE_EXT).length()));
  }

  private void reopenAndSearch(final List<String> logs,
      final BiConsumer<LSMVectorIndex, List<Pair<RID, Float>>> checks) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final List<Pair<RID, Float>> results = capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));
        // Whichever route the reopen took, codes it kept or rebuilt are bound to the graph it published: codes left bound
        // to no map would silently turn every approximate search into an exact one (issue #10786)
        if (index.isPQSearchAvailable())
          assertThat(index.pqCodesForPublishedGraphForTest()).as("the PQ codes serve the graph the reopen published")
              .isNotNull();
        checks.accept(index, results);
      } finally {
        db.drop();
      }
    }
  }

  /** What the building session leaves behind and what it answered, for the reopens to be compared with. */
  private record Built(Set<RID> neighbours, Path graphPath, int ordinalMapSize, boolean pqFileExists, Path pqPath) {
  }

  private Built build(final String quantization) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quantization + "\" }");
        });
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          if (i % 1000 == 999) {
            db.commit();
            db.begin();
          }
        }
        db.commit();

        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();
        final Set<RID> neighbours = neighbours(index.findNeighborsFromVector(embedding(1), 10, 64));
        final Path graphPath = index.getGraphFilePathForTest();
        final LSMVectorIndexOrdinalMapFile.Content map = new LSMVectorIndexGraphManifest(graphPath.toString()).readOrdinalMap();
        final Path pq = index.getProductQuantizationFilePath();
        return new Built(neighbours, graphPath, map != null ? map.size() : -1, pq != null && Files.exists(pq), pq);
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static Set<RID> neighbours(final List<Pair<RID, Float>> results) {
    return results.stream().map(Pair::getFirst).collect(Collectors.toSet());
  }

  private static <T> T capturingLogs(final List<String> into, final Supplier<T> action) {
    final Logger previous = LogManager.instance().getLogger();
    try {
      LogManager.instance().setLogger(new Collector(into));
      return action.get();
    } finally {
      LogManager.instance().setLogger(previous);
    }
  }

  private record Collector(List<String> lines) implements Logger {
    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6, final Object arg7,
        final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12, final Object arg13,
        final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      log(requester, level, message, exception, context, new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
          arg12, arg13, arg14, arg15, arg16, arg17 });
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      if (message == null || level.intValue() < Level.INFO.intValue())
        return;
      final List<Object> bound = new ArrayList<>();
      if (args != null)
        for (final Object arg : args)
          if (arg != null)
            bound.add(arg);
      String formatted;
      try {
        formatted = bound.isEmpty() ? message : String.format(message, bound.toArray());
      } catch (final RuntimeException e) {
        formatted = message;
      }
      synchronized (lines) {
        lines.add(formatted);
      }
    }

    @Override
    public void flush() {
    }
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x10568L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
  }
}
