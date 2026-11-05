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
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #11298: the rerank of a BINARY search read every candidate's vector through the record layer, a lazy record looked
 * up and then loaded by a second lookup, a copy of the whole record and a fresh {@code float[]} per candidate, for the 100
 * candidates a default search reranks. The vector is now read where it lies on its bucket page, into one scratch vector
 * per search, and the record layer answers only the shapes that read cannot vouch for: a record the transaction wrote, a
 * type with a read listener, a record that does not lie whole in its slot, a value that is not a float array.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11298BinaryRerankReadsInPlaceTest extends TestHelper {
  private static final VectorTypeSupport VTS        = VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final int               DIMENSIONS = 64;

  @AfterEach
  void restoreTheFloatArrayForm() {
    serializer().setFixedWidthFloatArrays(null);
  }

  @Test
  void documentRerankReadsNoRecordThroughTheDatabase() {
    final Corpus corpus = createCorpus("Doc", "DOCUMENT", 1500, 3);
    assertRerankIsExactAndInPlace(corpus);
  }

  @Test
  void vertexRerankReadsNoRecordThroughTheDatabase() {
    final Corpus corpus = createCorpus("Vtx", "VERTEX", 1500, 5);
    assertRerankIsExactAndInPlace(corpus);
  }

  /** Vectors written in the varint form, as every build before #10856 wrote them, are read in place too. */
  @Test
  void varintFloatArraysAreReadInPlace() {
    serializer().setFixedWidthFloatArrays(() -> false);
    final Corpus corpus = createCorpus("Varint", "DOCUMENT", 1500, 7);
    serializer().setFixedWidthFloatArrays(null);
    assertRerankIsExactAndInPlace(corpus);
  }

  /** The grouped rerank reads its candidates the same way, and ranks them on the same exact distances. */
  @Test
  void groupedRerankScoresTheStoredVectors() {
    final Corpus corpus = createCorpus("Grouped", "DOCUMENT", 1500, 9);
    final LSMVectorIndex index = index(corpus.type);
    final float[] query = corpus.vectors.get(42);
    final List<Pair<RID, Float>> hits = index.findNeighborsFromVectorGrouped(query, 5, 1, 50, null, rid -> rid.getPosition() % 10);
    assertThat(hits).isNotEmpty();
    assertThat(hits.getFirst().getFirst()).isEqualTo(corpus.rids.get(42));
    for (final Pair<RID, Float> hit : hits)
      assertThat(hit.getSecond()).isCloseTo(exactDistance(query, corpus.vectorOf(hit.getFirst())), within(1e-6f));
  }

  /** A vector the calling transaction rewrote is reranked on what the transaction wrote, not on what the page holds. */
  @Test
  void pendingWriteOfTheTransactionIsReranked() {
    final Corpus corpus = createCorpus("Pending", "DOCUMENT", 1500, 11);
    final LSMVectorIndex index = index(corpus.type);
    index.findNeighborsFromVector(corpus.vectors.getFirst(), 10, 50); // builds the graph

    final RID target = corpus.rids.get(7);
    final float[] old = corpus.vectors.get(7);
    final float[] rewritten = randomVector(new Random(13));
    database.begin();
    try {
      target.asDocument().modify().set("vector", rewritten).save();

      final long readsBefore = recordReads();
      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(rewritten, 5, 50);
      assertThat(hits.getFirst().getFirst()).isEqualTo(target);
      assertThat(hits.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
      assertThat(recordReads() - readsBefore).as("the record the transaction wrote is read through the database")
          .isGreaterThanOrEqualTo(1);

      // The committed vector, still on the page, is no longer the record's: a search for it never finds the record at 0
      for (final Pair<RID, Float> hit : index.findNeighborsFromVector(old, 10, 50))
        if (hit.getFirst().equals(target))
          assertThat(hit.getSecond()).isCloseTo(exactDistance(old, rewritten), within(1e-6f));
    } finally {
      database.rollback();
    }
  }

  /** A type with a read listener has its candidates read through the database, so the listener still sees them. */
  @Test
  void readListenerStillSeesTheCandidates() {
    final Corpus corpus = createCorpus("Listened", "DOCUMENT", 1500, 17);
    final LSMVectorIndex index = index(corpus.type);
    index.findNeighborsFromVector(corpus.vectors.getFirst(), 10, 50); // builds the graph

    final AtomicInteger reads = new AtomicInteger();
    final AfterRecordReadListener listener = record -> {
      reads.incrementAndGet();
      return record;
    };
    final RecordEvents events = database.getSchema().getType(corpus.type).getEvents();
    events.registerListener(listener);
    try {
      final float[] query = corpus.vectors.get(21);
      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(query, 10, 50);
      assertThat(hits.getFirst().getFirst()).isEqualTo(corpus.rids.get(21));
      assertThat(reads.get()).as("the listener runs for the reranked candidates").isGreaterThanOrEqualTo(hits.size());
      for (final Pair<RID, Float> hit : hits)
        assertThat(hit.getSecond()).isCloseTo(exactDistance(query, corpus.vectorOf(hit.getFirst())), within(1e-6f));
    } finally {
      events.unregisterListener(listener);
    }
  }

  /**
   * A reader lives for a whole search or build: a listener registered while it lives sees the reads that follow, because the
   * reader asks for listeners on every read, not once per bucket.
   */
  @Test
  void listenerRegisteredWhileAReaderLivesSeesTheReadsThatFollow() {
    final Corpus corpus = createCorpus("LateListener", "DOCUMENT", 50, 37);
    final LSMVectorIndex index = index(corpus.type);
    final RID rid = corpus.rids.get(3);
    final StoredVectorReader reader = index.storedVectorReader("vector", DIMENSIONS);
    assertThat(index.readStoredVectorInPlace(reader, rid)).as("no listener: read in place").containsExactly(corpus.vectors.get(3));

    final AfterRecordReadListener listener = record -> record;
    final RecordEvents events = database.getSchema().getType(corpus.type).getEvents();
    events.registerListener(listener);
    try {
      assertThat(index.readStoredVectorInPlace(reader, rid)).as("a listener now: left to the database, which runs it").isNull();
    } finally {
      events.unregisterListener(listener);
    }
    assertThat(index.readStoredVectorInPlace(reader, rid)).as("the listener gone: in place again").containsExactly(corpus.vectors.get(3));
  }

  /** A record larger than its page is a chunk chain, not a record of its own slot: it is read through the database. */
  @Test
  void recordLargerThanItsPageIsReranked() {
    final Corpus corpus = createCorpus("Chunked", "DOCUMENT", 1500, 19);
    final LSMVectorIndex index = index(corpus.type);
    index.findNeighborsFromVector(corpus.vectors.getFirst(), 10, 50); // builds the graph

    final float[] vector = randomVector(new Random(23));
    final RID[] big = new RID[1];
    database.transaction(() -> big[0] = database.newDocument(corpus.type).set("vector", vector).set("payload", "x".repeat(200_000))
        .save().getIdentity());

    final long readsBefore = recordReads();
    final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(vector, 5, 50);
    assertThat(hits.getFirst().getFirst()).isEqualTo(big[0]);
    assertThat(hits.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
    assertThat(recordReads() - readsBefore).as("the chunked record is read through the database").isGreaterThanOrEqualTo(1);
  }

  /**
   * The walk of an unquantized index that holds no vector of its own reads each one it scores from its document, which goes
   * the same in-place way: with the search cache off every hop of the walk fetches from the documents, none through the
   * record layer.
   */
  @Test
  void unquantizedWalkReadsTheDocumentsInPlace() {
    ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE, -1);
    database.command("sql", "CREATE DOCUMENT TYPE Plain BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Plain.vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON Plain (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"COSINE\" }");
    final Random random = new Random(29);
    final List<float[]> vectors = new ArrayList<>();
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 1000; i++) {
        final float[] vector = randomVector(random);
        vectors.add(vector);
        rids.add(database.newDocument("Plain").set("vector", vector).save().getIdentity());
      }
    });
    reopenDatabase();
    ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_SEARCH_CACHE_SIZE, -1);
    final LSMVectorIndex index = index("Plain");
    index.findNeighborsFromVector(vectors.getFirst(), 10, 50); // builds the graph

    final long readsBefore = recordReads();
    final long fetchesBefore = index.getStats().getOrDefault("vectorFetchFromDocuments", 0L);
    for (int q = 0; q < 10; q++) {
      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(vectors.get(q * 97), 10, 50);
      assertThat(hits.getFirst().getFirst()).isEqualTo(rids.get(q * 97));
      assertThat(hits.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
    }
    assertThat(index.getStats().getOrDefault("vectorFetchFromDocuments", 0L) - fetchesBefore)
        .as("the walk fetched the vectors it scored from the documents").isPositive();
    assertThat(recordReads() - readsBefore).as("records the walk read through the database").isZero();
  }

  /**
   * The workers of a graph build have no context of the database until their first record read sets one up, and a build
   * whose cache does not hold a vector reads it from the document on such a thread (a renumbering compaction does, see
   * {@code Issue8957CompactionSearchCacheTest}). The in-place read leaves that read to the record layer instead of failing on
   * the missing transaction: failing there turned every such vector into the deleted placeholder, and built a graph that
   * could not find its own records.
   */
  @Test
  void threadWithoutADatabaseContextLeavesTheReadToTheRecordLayer() throws Exception {
    final Corpus corpus = createCorpus("NoContext", "DOCUMENT", 50, 31);
    final LSMVectorIndex index = index(corpus.type);
    final RID rid = corpus.rids.get(7);

    final AtomicReference<Object> outcome = new AtomicReference<>();
    final Thread worker = new Thread(() -> {
      try {
        final StoredVectorReader reader = index.storedVectorReader("vector", DIMENSIONS);
        outcome.set(index.readStoredVectorInPlace(reader, rid) == null ? "left to the record layer" : "read");
      } catch (final Throwable t) {
        outcome.set(t);
      }
    });
    worker.start();
    worker.join();
    assertThat(outcome.get()).isEqualTo("left to the record layer");

    // A thread with a context reads it in place, with a reader of its own
    final StoredVectorReader reader = index.storedVectorReader("vector", DIMENSIONS);
    assertThat(reader.isOwnedByCurrentThread()).isTrue();
    assertThat(index.readStoredVectorInPlace(reader, rid)).containsExactly(corpus.vectors.get(7));
  }

  /**
   * The absolute bulk read the in-place read copies a vector with: it reads at an index without moving the position, so threads
   * sharing a page view do not disturb each other, and refuses a count past the content even when the backing array has spare
   * capacity past it, as the relative read of #10856 does.
   */
  @Test
  void absoluteFloatArrayReadIsBoundedAndKeepsThePosition() {
    final Binary buffer = new Binary(1024);
    buffer.putByte((byte) 7);
    buffer.putFloatArray(new float[] { 1f, -2f, 3.5f });
    buffer.position(0);

    final float[] values = new float[4];
    buffer.getFloatArrayAt(1, values, 3);
    assertThat(values).containsExactly(1f, -2f, 3.5f, 0f);
    assertThat(buffer.position()).isZero();

    assertThatThrownBy(() -> buffer.getFloatArrayAt(1, values, 4)).isInstanceOf(BufferUnderflowException.class);
    assertThatThrownBy(() -> buffer.getFloatArrayAt(-1, values, 1)).isInstanceOf(BufferUnderflowException.class);
    assertThatThrownBy(() -> buffer.getFloatArrayAt(1, new float[2], 3)).isInstanceOf(BufferUnderflowException.class);
    assertThatThrownBy(() -> buffer.getFloatArrayAt(1, values, -1)).isInstanceOf(IllegalArgumentException.class);
    assertThat(buffer.position()).isZero();
  }

  private void assertRerankIsExactAndInPlace(final Corpus corpus) {
    final LSMVectorIndex index = index(corpus.type);
    index.findNeighborsFromVector(corpus.vectors.getFirst(), 10, 50); // builds the graph

    final long readsBefore = recordReads();
    for (int q = 0; q < 20; q++) {
      final int row = q * 61 + 3;
      final float[] query = corpus.vectors.get(row);
      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(query, 10, 50);

      assertThat(hits).hasSize(10);
      assertThat(hits.getFirst().getFirst()).isEqualTo(corpus.rids.get(row));
      assertThat(hits.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
      float previous = -Float.MAX_VALUE;
      for (final Pair<RID, Float> hit : hits) {
        // The distance of the stored vector, computed here with nothing of the index's read path
        assertThat(hit.getSecond()).isCloseTo(exactDistance(query, corpus.vectorOf(hit.getFirst())), within(1e-6f));
        assertThat(hit.getSecond()).isGreaterThanOrEqualTo(previous);
        previous = hit.getSecond();
      }
    }
    assertThat(recordReads() - readsBefore).as("records the 20 searches read through the database to rerank 100 candidates each")
        .isZero();
  }

  private long recordReads() {
    return ((Number) ((DatabaseInternal) database).getStats().get("readRecord")).longValue();
  }

  private static float exactDistance(final float[] query, final float[] stored) {
    return LSMVectorIndex.scoreToDistance(VectorSimilarityFunction.COSINE,
        VectorSimilarityFunction.COSINE.compare(VTS.createFloatVector(query), VTS.createFloatVector(stored)));
  }

  private BinarySerializer serializer() {
    return ((DatabaseInternal) database).getSerializer();
  }

  private record Corpus(String type, List<float[]> vectors, List<RID> rids) {
    float[] vectorOf(final RID rid) {
      return vectors.get(rids.indexOf(rid));
    }
  }

  private Corpus createCorpus(final String type, final String kind, final int size, final long seed) {
    database.command("sql", "CREATE " + kind + " TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
    database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
        + ", \"similarity\": \"COSINE\", \"quantization\": \"BINARY\" }");

    final Random random = new Random(seed);
    final List<float[]> vectors = new ArrayList<>(size);
    for (int i = 0; i < size; i++)
      vectors.add(randomVector(random));
    final List<RID> rids = new ArrayList<>(size);
    database.transaction(() -> {
      for (int i = 0; i < size; i++)
        // A property before the vector and one after it, so the vector is not the first value of the record
        rids.add((kind.equals("VERTEX") ? database.newVertex(type) : database.newDocument(type)).set("name", "row-" + i)
            .set("vector", vectors.get(i)).set("tail", i).save().getIdentity());
    });
    return new Corpus(type, vectors, rids);
  }

  private LSMVectorIndex index(final String type) {
    return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[vector]")).getIndexesOnBuckets()[0];
  }

  private static float[] randomVector(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = -1.0f + random.nextFloat() * 2.0f;
    return vector;
  }
}
