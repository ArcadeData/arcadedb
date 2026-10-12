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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9796 at the index level: what {@code LSMVectorIndex} does around the PQ file now that
 * {@code LSMVectorIndexPQFile.writePQ} never destroys the file it replaces.
 * <ul>
 *   <li>A failed persist must not leave the previous file standing. It describes the previous graph's ordinals and the
 *   new graph is already certified, so the next open would pair the two. The file is dropped (the next open rebuilds
 *   graph and PQ together) and the codes in hand are still published to this session.</li>
 *   <li>A file that is on disk but cannot be read is no PQ data: the open rebuilds, and ends with a loadable file.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue9796PQFileIndexTest extends TestHelper {

  private static final int EMBEDDING_DIM = 32;
  private static final int VECTORS       = 1100;

  private static final Duration SETTLE_TIMEOUT = Duration.ofMinutes(2);

  @Test
  void aFailedPersistDropsThePreviousFileAndStillPublishesTheCodesInHand() throws Exception {
    assertFailedPersistDropsThePreviousFile(null);
  }

  @Test
  void aRuntimeFailureOfThePersistDropsThePreviousFileToo() throws Exception {
    assertFailedPersistDropsThePreviousFile(new IllegalStateException("injected runtime failure"));
  }

  @Test
  void anErrorDuringThePersistDropsThePreviousFileBeforeItPropagates() throws Exception {
    // The build's handler catches Throwable and carries on: whatever it decides, the stale file must already be gone
    final Throwable thrown = assertFailedPersistDropsThePreviousFile(new OutOfMemoryError("injected"));
    assertThat(thrown).isInstanceOf(OutOfMemoryError.class);
  }

  /**
   * @param failure what the codes' write throws after streaming part of itself, or {@code null} for an I/O error
   *
   * @return what {@code persistProductQuantization} let escape, or {@code null} when it absorbed the failure
   */
  private Throwable assertFailedPersistDropsThePreviousFile(final Throwable failure) throws Exception {
    final LSMVectorIndex index = buildIndexWithPersistedPQ();
    final LSMVectorIndexPQFile pqFile = pqFileOf(index);
    final Path pqPath = pqFile.getFilePath();
    assertThat(pqPath).exists();

    final ProductQuantization pq = (ProductQuantization) field("productQuantization").get(index);
    final PQVectors liveCodes = (PQVectors) field("pqVectors").get(index);
    assertThat(pq).isNotNull();
    assertThat(liveCodes).isNotNull();

    // Codes whose write dies halfway through: the persist after a rebuild whose disk gave out.
    final Issue9796PQFileAtomicRewriteTest.FailingPQVectors failing = failure == null ?
        new Issue9796PQFileAtomicRewriteTest.FailingPQVectors(pq, liveCodes.count(), 64) :
        new Issue9796PQFileAtomicRewriteTest.FailingPQVectors(pq, liveCodes.count(), 64, failure);

    final Method persist = LSMVectorIndex.class.getDeclaredMethod("persistProductQuantization", ProductQuantization.class,
        PQVectors.class);
    persist.setAccessible(true);
    Throwable escaped = null;
    try {
      persist.invoke(index, pq, failing);
    } catch (final InvocationTargetException e) {
      escaped = e.getCause();
    }

    assertThat(pqPath)
        .as("the previous file belongs to the previous graph: it must not survive a failed persist")
        .doesNotExist();
    assertThat(temporariesNextTo(pqPath)).isEmpty();
    assertThat(pqFile.hasUsableData()).isFalse();
    if (escaped == null)
      assertThat(field("pqVectors").get(index)).as("the codes in hand are right for the graph in memory").isSameAs(failing);
    return escaped;
  }

  @Test
  void anUnreadablePQFileIsRebuiltOnReopen() throws Exception {
    final LSMVectorIndex index = buildIndexWithPersistedPQ();
    final Path pqPath = pqFileOf(index).getFilePath();
    final long completeSize = Files.size(pqPath);
    assertThat(completeSize).isGreaterThan(0L);

    // The fragment the in-place rewrite used to leave behind, on a database that is closed.
    database.close();
    final byte[] whole = Files.readAllBytes(pqPath);
    Files.write(pqPath, Arrays.copyOf(whole, whole.length / 3));
    database = factory.open();

    final LSMVectorIndex reopened = indexOf(database);

    // The open found the fragment, tried to load it and could not: "is there PQ data?" must say no, although the file
    // is there. This is the answer needsGraphRebuildForPQ keys on.
    reopened.countEntries();
    final LSMVectorIndexPQFile fragment = pqFileOf(reopened);
    assertThat(fragment.exists()).as("the fragment is on disk").isTrue();
    assertThat(fragment.isPQReady()).isFalse();
    assertThat(fragment.hasUsableData()).as("but it is not PQ data").isFalse();

    final Random random = new Random(9796);
    Awaitility.await("the PQ is rebuilt next to the graph after the reopen")
        .atMost(SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          reopened.findNeighborsFromVector(randomUnitVector(random), 10);
          assertThat(reopened.isPQSearchAvailable()).isTrue();
          assertThat(PendingDeltaVectors.of(reopened)).isZero();
        });

    assertThat(reopened.findNeighborsFromVectorApproximate(randomUnitVector(random), 10)).isNotEmpty();

    final LSMVectorIndexPQFile fresh = new LSMVectorIndexPQFile(stripExtension(pqPath));
    assertThat(fresh.loadPQ()).as("the file on disk must load after the rebuild").isTrue();
    assertThat(fresh.getPQVectors().count()).isGreaterThanOrEqualTo(VECTORS);
    assertThat(temporariesNextTo(pqPath)).isEmpty();
  }

  /**
   * The async rebuild path reaches the persisted-graph decision without the deferred vector load having run, and the
   * PQ is loaded by that load: a fragment nobody has tried to read yet answers "usable". The decision has to try the
   * load itself before it asks.
   */
  @Test
  void theGraphDecisionTriesToLoadThePQBeforeAskingWhetherItIsUsable() throws Exception {
    final LSMVectorIndex index = buildIndexWithPersistedPQ();
    final Path pqPath = pqFileOf(index).getFilePath();

    database.close();
    final byte[] whole = Files.readAllBytes(pqPath);
    Files.write(pqPath, Arrays.copyOf(whole, whole.length / 3));
    database = factory.open();

    // Nothing has touched the reopened index: the locations and the PQ are still to be loaded
    final LSMVectorIndex reopened = indexOf(database);
    final LSMVectorIndexPQFile fragment = pqFileOf(reopened);
    assertThat(fragment.exists()).isTrue();
    assertThat(fragment.isPQReady()).isFalse();
    assertThat(fragment.hasUsableData()).as("no load has been tried, so the fragment still reads as usable").isTrue();

    final Object graphFile = field("graphFile").get(reopened);
    assertThat(graphFile).as("the scenario needs a persisted graph to decide about").isNotNull();
    final Method hasPersistedGraph = graphFile.getClass().getDeclaredMethod("hasPersistedGraph");
    hasPersistedGraph.setAccessible(true);
    assertThat((boolean) hasPersistedGraph.invoke(graphFile)).isTrue();

    final ReentrantLock graphBuildLock = (ReentrantLock) field("graphBuildLock").get(reopened);
    final Method decide = LSMVectorIndex.class.getDeclaredMethod("loadPersistedGraphOrDecidePrefix");
    decide.setAccessible(true);
    final Object check;
    graphBuildLock.lock();
    try {
      check = decide.invoke(reopened);
    } finally {
      graphBuildLock.unlock();
    }

    final Method fullyLoaded = check.getClass().getDeclaredMethod("fullyLoaded");
    fullyLoaded.setAccessible(true);
    final Method prefix = check.getClass().getDeclaredMethod("prefix");
    prefix.setAccessible(true);
    assertThat((boolean) fullyLoaded.invoke(check))
        .as("PRODUCT quantization without PQ data to use must rebuild the graph from scratch, not load it")
        .isFalse();
    assertThat(prefix.invoke(check)).isNull();
    assertThat(fragment.hasUsableData()).as("the decision tried the load, and the fragment failed it").isFalse();
  }

  private LSMVectorIndex buildIndexWithPersistedPQ() throws Exception {
    final Random random = new Random(9796);

    database.transaction(() -> {
      database.getSchema().createVertexType("Embedding");
      database.getSchema().getType("Embedding").createProperty("vector", Type.ARRAY_OF_FLOATS);
      database.command("sql", """
          CREATE INDEX ON Embedding (vector) LSM_VECTOR
          METADATA {
              "dimensions": %d,
              "similarity": "EUCLIDEAN",
              "quantization": "PRODUCT",
              "pqClusters": 32
          }""".formatted(EMBEDDING_DIM));
    });

    database.transaction(() -> {
      for (int i = 0; i < VECTORS; i++)
        database.command("sql", "INSERT INTO Embedding SET vector = ?", (Object) randomUnitVector(random));
    });

    final LSMVectorIndex index = indexOf(database);
    Awaitility.await("the graph and its PQ settle")
        .atMost(SETTLE_TIMEOUT)
        .pollInterval(Duration.ofMillis(50))
        .untilAsserted(() -> {
          index.findNeighborsFromVector(randomUnitVector(random), 10);
          assertThat(PendingDeltaVectors.of(index)).isZero();
          assertThat(index.isPQSearchAvailable()).isTrue();
          assertThat(pqFileOf(index).getFilePath()).exists();
        });
    return index;
  }

  private static LSMVectorIndex indexOf(final Database db) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Embedding[vector]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static LSMVectorIndexPQFile pqFileOf(final LSMVectorIndex index) throws Exception {
    return (LSMVectorIndexPQFile) field("pqFile").get(index);
  }

  private static Field field(final String name) throws Exception {
    final Field field = LSMVectorIndex.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }

  private static String stripExtension(final Path pqPath) {
    final String name = pqPath.toString();
    return name.substring(0, name.length() - ("." + LSMVectorIndexPQFile.FILE_EXT).length());
  }

  private static List<Path> temporariesNextTo(final Path pqPath) throws IOException {
    final List<Path> temporaries = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(pqPath.getParent(), pqPath.getFileName() + ".*.tmp")) {
      stream.forEach(temporaries::add);
    }
    return temporaries;
  }

  private float[] randomUnitVector(final Random random) {
    final float[] vector = new float[EMBEDDING_DIM];
    float norm = 0;
    for (int i = 0; i < EMBEDDING_DIM; i++) {
      vector[i] = random.nextFloat() * 2 - 1;
      norm += vector[i] * vector[i];
    }
    norm = (float) Math.sqrt(norm);
    if (norm > 0)
      for (int i = 0; i < EMBEDDING_DIM; i++)
        vector[i] /= norm;
    return vector;
  }
}
