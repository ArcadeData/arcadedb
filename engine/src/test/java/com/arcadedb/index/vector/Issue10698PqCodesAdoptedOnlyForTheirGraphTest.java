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
import com.arcadedb.query.sql.executor.ResultSet;
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
import java.util.function.Supplier;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10698: the PQ codes of a PRODUCT-quantized index record which graph they were built for
 * (issue #10584), and every reopen route that publishes a persisted graph must adopt them only for that graph. The
 * exact-cover reopen already asked; the ordinary walk and the stale-prefix reuse published the same persisted graph and
 * kept whatever codes the PQ file held. Each test puts codes that are NOT the graph's next to it and checks that the
 * reopen rebuilds them for the graph it publishes, without rebuilding the graph itself.
 */
@Tag("vector")
class Issue10698PqCodesAdoptedOnlyForTheirGraphTest {
  private static final String            DB_ROOT     = "target/test-databases/Issue10698PqCodesAdoptedOnlyForTheirGraphTest";
  private static final int               DIMENSIONS  = 32;
  private static final int               NUM_VECTORS = 2_000;
  private static final VectorTypeSupport VTS         = VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final String            WALK_LINE   = "Loaded graph from disk for index";
  private static final String            REBUILT     = "building PQ for the";

  private String dbPath;
  private Path   graphPath;
  private Path   pqPath;

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
   * The issue's own repro: a PQ sidecar saved aside, a vector added and graph and PQ persisted again, the old sidecar put
   * back. The codes number one less than the graph's ordinals, so the exact-cover reopen refuses them, and the walk used
   * to publish the graph with them anyway - leaving every approximate search on the exact path for good (issue #5417).
   */
  @Test
  void anOlderPqSidecarRestoredBesideANewerGraphIsRebuiltByTheWalk() throws IOException {
    build(NUM_VECTORS);
    final byte[] olderSidecar = Files.readAllBytes(pqPath);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        insert(db, NUM_VECTORS, NUM_VECTORS + 1);
        vectorIndex(db).buildVectorGraphNow();
      } finally {
        db.close();
      }
    }
    assertThat(manifest().vectorCount()).as("precondition: the newer graph covers the added vector").isEqualTo(NUM_VECTORS + 1);
    Files.write(pqPath, olderSidecar);

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

        assertThat(logs).as("the walk published the persisted graph").anyMatch(m -> m.contains(WALK_LINE));
        assertThat(index.getStats().get("graphRebuildCount")).as("the graph itself is reused, not rebuilt").isZero();
        assertThat(index.getPQVectorCount()).as("the codes are the newer graph's, one per ordinal").isEqualTo(NUM_VECTORS + 1);
        assertThat(index.pqCodesGraphFingerprintForTest()).as("and record the graph the manifest certifies")
            .isEqualTo(manifest().fingerprint());
        assertThat(logs).anyMatch(m -> m.contains(REBUILT));
      } finally {
        db.close();
      }
    }

    assertTheNextReopenTakesTheOrdinalMap();
  }

  /**
   * Same count, codes of another assignment of ordinals: every vector's code is the code of another vector. Searching
   * with them is a wrong-but-plausible answer; the walk must rebuild them before the graph is searched through them.
   */
  @Test
  void codesOfAnotherGraphOfTheSameSizeAreRebuiltByTheWalk() throws IOException {
    build(NUM_VECTORS);
    final LSMVectorIndexPQFile pqFile = pqFile();
    assertThat(pqFile.loadPQ()).isTrue();
    final ProductQuantization pq = pqFile.getProductQuantization();
    final long fingerprint = pqFile.graphFingerprintOf(pqFile.getPQVectors());
    final MutablePQVectors shuffled = new MutablePQVectors(pq);
    for (int i = 0; i < NUM_VECTORS; i++)
      shuffled.encodeAndSet(i, VTS.createFloatVector(embedding(NUM_VECTORS - 1 - i)));
    pqFile.writePQ(pq, shuffled, fingerprint ^ 1L);

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        final int probe = 1;
        final List<Pair<RID, Float>> results = capturingLogs(logs,
            () -> index.findNeighborsFromVectorApproximate(embedding(probe), 10));

        assertThat(logs).as("the walk published the persisted graph").anyMatch(m -> m.contains(WALK_LINE));
        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(index.pqCodesGraphFingerprintForTest()).isEqualTo(manifest().fingerprint());
        assertThat(results.stream().map(Pair::getFirst)).as("a vector is its own nearest neighbour once its code is its own")
            .contains(ridOf(db, probe));
        assertThat(logs).anyMatch(m -> m.contains("were built for another graph than the persisted one") && m.contains(REBUILT));
      } finally {
        db.close();
      }
    }

    assertTheNextReopenTakesTheOrdinalMap();
  }

  /**
   * A PQ file written before the identity was recorded: the state of every PRODUCT index upgraded from an earlier build.
   * The walk cannot vouch for those codes, so it rebuilds them once, recorded, and the next reopen is the fast one.
   */
  @Test
  void codesThatRecordNoGraphAreRebuiltOnceAndTheNextReopenTakesTheOrdinalMap() throws IOException {
    build(NUM_VECTORS);
    final byte[] bytes = Files.readAllBytes(pqPath);
    Files.write(pqPath, Arrays.copyOf(bytes, bytes.length - LSMVectorIndexPQFile.GRAPH_IDENTITY_BYTES));

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

        assertThat(index.getStats().get("graphRebuildCount")).isZero();
        assertThat(index.pqCodesGraphFingerprintForTest()).isEqualTo(manifest().fingerprint());
        assertThat(logs).anyMatch(m -> m.contains("record no graph they were built for") && m.contains(REBUILT));
      } finally {
        db.close();
      }
    }

    assertTheNextReopenTakesTheOrdinalMap();
  }

  /** Codes that are the graph's must be kept by the walk: no PQ rebuild on a reopen that does not need one. */
  @Test
  void codesOfThePersistedGraphAreKeptByTheWalk() throws IOException {
    build(NUM_VECTORS);
    // No ordinal map: the reopen takes the walk, with codes that ARE the graph's
    Files.delete(new LSMVectorIndexGraphManifest(graphPath.toString()).getOrdinalMapFilePath());

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));

        assertThat(logs).as("the walk ran").anyMatch(m -> m.contains(WALK_LINE));
        assertThat(index.getPQVectorCount()).isEqualTo(NUM_VECTORS);
        assertThat(logs).as("and kept the codes").noneMatch(m -> m.contains(REBUILT))
            .noneMatch(m -> m.contains("Building Product Quantization"));
      } finally {
        db.drop();
      }
    }
  }

  /**
   * The stale-prefix reuse (issue #6655): vectors added after the graph was persisted and the session crashed, so the
   * reopen reuses the graph as a prefix. Codes that are not that graph's must be rebuilt for the prefix it publishes.
   */
  @Test
  void codesOfAnotherGraphAreRebuiltByTheStalePrefixReuse() throws IOException {
    build(NUM_VECTORS);

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        insert(db, NUM_VECTORS, NUM_VECTORS + 50);
        ((DatabaseInternal) db).kill();
        db.close();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }

    final LSMVectorIndexPQFile pqFile = pqFile();
    assertThat(pqFile.loadPQ()).isTrue();
    final PQVectors codes = pqFile.getPQVectors();
    pqFile.writePQ(pqFile.getProductQuantization(), codes, pqFile.graphFingerprintOf(codes) ^ 1L);

    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc")) {
        assertThat(rs.next().<Long>getProperty("cnt")).as("precondition: the crashed session's records were recovered")
            .isEqualTo((long) NUM_VECTORS + 50);
        final LSMVectorIndex index = vectorIndex(db);
        LSMVectorIndex.acquireAllRebuildPermitsForTest();
        try {
          final List<Pair<RID, Float>> results = capturingLogs(logs,
              () -> index.findNeighborsFromVector(embedding(NUM_VECTORS + 10), 5, 64));

          assertThat(index.getStats().get("stalePrefixGraphReuses")).as("precondition: the prefix reuse ran").isEqualTo(1L);
          assertThat(index.getStats().get("graphRebuildCount")).isZero();
          assertThat(index.getPQVectorCount()).as("one code per ordinal of the reused graph").isEqualTo(NUM_VECTORS);
          assertThat(index.pqCodesGraphFingerprintForTest()).as("recording the graph the manifest certifies")
              .isEqualTo(manifest().fingerprint());
          assertThat(logs).anyMatch(m -> m.contains("were built for another graph than the persisted one")
              && m.contains("reused graph prefix"));
          assertThat(results.stream().map(Pair::getFirst)).as("the gap is still searchable")
              .contains(ridOf(db, NUM_VECTORS + 10));
        } finally {
          LSMVectorIndex.releaseAllRebuildPermitsForTest();
        }
      } finally {
        db.drop();
      }
    }
  }

  private void assertTheNextReopenTakesTheOrdinalMap() {
    final List<String> logs = new ArrayList<>();
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex index = vectorIndex(db);
        capturingLogs(logs, () -> index.findNeighborsFromVector(embedding(1), 10, 64));
        assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
            .as("the rebuilt codes were persisted with their graph's identity, so the next reopen is the fast one").isEqualTo(1L);
        assertThat(logs).noneMatch(m -> m.contains(WALK_LINE)).noneMatch(m -> m.contains(REBUILT));
      } finally {
        db.drop();
      }
    }
  }

  private LSMVectorIndexGraphManifest.Content manifest() {
    final LSMVectorIndexGraphManifest.Content content = new LSMVectorIndexGraphManifest(graphPath.toString()).read();
    assertThat(content).isNotNull();
    return content;
  }

  private LSMVectorIndexPQFile pqFile() {
    final String file = pqPath.toString();
    return new LSMVectorIndexPQFile(file.substring(0, file.length() - ("." + LSMVectorIndexPQFile.FILE_EXT).length()));
  }

  private void build(final int count) {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\", \"quantization\": \"PRODUCT\" }");
        });
        insert(db, 0, count);
        final LSMVectorIndex index = vectorIndex(db);
        index.buildVectorGraphNow();
        graphPath = index.getGraphFilePathForTest();
        pqPath = index.getProductQuantizationFilePath();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
    assertThat(Files.exists(pqPath)).as("precondition: the build persisted its PQ").isTrue();
  }

  private static void insert(final Database db, final int from, final int to) {
    db.begin();
    for (int i = from; i < to; i++) {
      db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
      if (i % 1000 == 999) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static RID ridOf(final Database db, final int id) {
    try (final ResultSet rs = db.query("sql", "SELECT @rid as rid FROM Doc WHERE id = ?", id)) {
      return rs.next().getProperty("rid");
    }
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
    final Random random = new Random(0x10698L * 31 + id);
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
