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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Search on binary quantized codes. Each nested class keeps the fixture of the regression test it was folded from.
 */
class BinaryQuantizedSearchTest {
  /**
   * Regression for #10968: binary code search.
   * <p>
   * Issue #10968: a BINARY index scored its graph walk on a float reconstruction of every candidate, rebuilt from the
   * one-bit row and cached as a {@code float[dimensions]}, so it did the work of an unquantized index per candidate and
   * was slower than one; and it reranked a fixed {@code 4 * k} candidates, so a wider efSearch cost more and found nothing
   * more. The walk now scores the codes with a Hamming distance, and the rerank takes {@code k * rerankOversample}
   * candidates, or the whole beam when efSearch asks for more.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BinaryCodeSearch extends TestHelper {
    private static final VectorTypeSupport VTS = VectorizationProvider.getInstance().getVectorTypeSupport();

    /**
     * The Hamming similarity of two codes is the number the configured similarity gives their reconstructions, so the codes
     * rank the walk exactly as the reconstructions did and every other scorer of a BINARY search agrees with them.
     */
    @Test
    void hammingSimilarityIsTheSimilarityOfTheReconstructions() {
      final Random random = new Random(3);
      for (final int dimensions : new int[] { 4, 63, 64, 65, 200, 1536 })
        for (final VectorSimilarityFunction function : VectorSimilarityFunction.values())
          for (int pair = 0; pair < 20; pair++) {
            final float[] a = randomVector(random, dimensions, pair % 7 == 0 ? 0.0f : -1.0f);
            // A constant vector is stored with every bit set and stands for the sign of its constant: both signs
            final float[] b = pair == 3 ? constant(dimensions, -2.5f) : pair == 4 ? constant(dimensions, 1.5f) :
                randomVector(random, dimensions, -1.0f);

            final long[] codeA = code(a);
            final long[] codeB = code(b);
            final int hamming = BinaryCodeCache.hammingDistance(codeA, codeB, BinaryCodeCache.wordsFor(dimensions));

            final float expected = function.compare(VTS.createFloatVector(reconstruct(a)), VTS.createFloatVector(reconstruct(b)));
            assertThat(BinaryCodeCache.similarity(function, hamming, dimensions)).as("%s at %d dimensions", function, dimensions)
                .isCloseTo(expected, within(1e-5f));
          }
    }

    @Test
    void medianIsTheMiddleOfTheSortedValues() {
      final Random random = new Random(11);
      for (int length = 1; length < 300; length++) {
        final float[] values = new float[length];
        for (int i = 0; i < length; i++)
          values[i] = switch (random.nextInt(6)) {
            case 0 -> 0.0f;
            case 1 -> -0.0f;
            case 2 -> random.nextInt(3); // duplicates
            default -> (float) random.nextGaussian();
          };
        if (length % 50 == 0)
          values[random.nextInt(length)] = Float.NaN;
        final float[] copy = values.clone();

        final float[] sorted = values.clone();
        Arrays.sort(sorted);
        final float expected = sorted.length % 2 == 0 ?
            (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0f :
            sorted[sorted.length / 2];

        assertThat(Float.floatToIntBits(VectorUtils.median(values))).as("length %d", length).isEqualTo(Float.floatToIntBits(expected));
        assertThat(values).as("the input is left untouched").isEqualTo(copy);
      }

      // Already sorted and reverse sorted, the inputs a naive pivot degrades on
      final float[] ascending = new float[10_001];
      for (int i = 0; i < ascending.length; i++)
        ascending[i] = i;
      assertThat(VectorUtils.median(ascending)).isEqualTo(5000f);
      for (int i = 0; i < ascending.length; i++)
        ascending[i] = -i;
      assertThat(VectorUtils.median(ascending)).isEqualTo(-5000f);
    }

    @Test
    void codeCacheAnswersOnlyForTheVectorItHolds() {
      final int dimensions = 130;
      final BinaryCodeCache cache = new BinaryCodeCache(dimensions, 4);
      final Random random = new Random(5);
      final long[] code = code(randomVector(random, dimensions, -1.0f));
      final long[] query = code(randomVector(random, dimensions, -1.0f));
      final int words = BinaryCodeCache.wordsFor(dimensions);

      assertThat(cache.hammingDistance(1, query)).isEqualTo(-1);
      cache.put(1, code, 0);
      assertThat(cache.hammingDistance(1, query)).isEqualTo(BinaryCodeCache.hammingDistance(code, query, words));
      // Same slot, another vector: a miss, never the code of the vector in the slot
      assertThat(cache.hammingDistance(1 + cache.capacity(), query)).isEqualTo(-1);

      cache.put(1 + cache.capacity(), query, 0);
      assertThat(cache.hammingDistance(1, query)).isEqualTo(-1);
      assertThat(cache.hammingDistance(1 + cache.capacity(), query)).isZero();

      cache.remove(1 + cache.capacity());
      assertThat(cache.hammingDistance(1 + cache.capacity(), query)).isEqualTo(-1);

      cache.put(2, code, 0);
      assertThat(cache.size()).isEqualTo(1);
      cache.clear();
      assertThat(cache.size()).isZero();
    }

    /**
     * Writers racing on the same slots never let a reader score a torn code: every distance a reader gets is the distance to
     * the code of the vector it asked for, or a miss.
     */
    @Test
    void concurrentWritersNeverHandOutATornCode() throws Exception {
      final int dimensions = 1536;
      final int words = BinaryCodeCache.wordsFor(dimensions);
      final BinaryCodeCache cache = new BinaryCodeCache(dimensions, 8);
      final int ids = 64; // eight vectors per slot, so writers keep evicting each other
      final long[][] codes = new long[ids][];
      final Random random = new Random(37);
      for (int id = 0; id < ids; id++)
        codes[id] = code(randomVector(random, dimensions, -1.0f));
      final long[] query = code(randomVector(random, dimensions, -1.0f));
      final int[] expected = new int[ids];
      for (int id = 0; id < ids; id++)
        expected[id] = BinaryCodeCache.hammingDistance(query, codes[id], words);

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger hits = new AtomicInteger();
      final AtomicInteger wrong = new AtomicInteger();
      final List<Thread> threads = new ArrayList<>();
      for (int t = 0; t < 3; t++) {
        final int seed = t;
        threads.add(new Thread(() -> {
          final Random r = new Random(seed);
          while (!stop.get()) {
            final int id = r.nextInt(ids);
            cache.put(id, codes[id], 0);
            if (r.nextInt(16) == 0)
              cache.remove(r.nextInt(ids));
          }
        }));
      }
      for (int t = 0; t < 3; t++) {
        final int seed = 100 + t;
        threads.add(new Thread(() -> {
          final Random r = new Random(seed);
          while (!stop.get()) {
            final int id = r.nextInt(ids);
            final int distance = cache.hammingDistance(id, query);
            if (distance >= 0) {
              hits.incrementAndGet();
              if (distance != expected[id])
                wrong.incrementAndGet();
            }
          }
        }));
      }
      threads.forEach(Thread::start);
      // Bounded by the reads it takes, not by a clock: stop once readers have seen enough hits to have raced the writers
      while (hits.get() < 200_000 && wrong.get() == 0)
        Thread.onSpinWait();
      stop.set(true);
      for (final Thread thread : threads)
        thread.join();

      assertThat(wrong.get()).isZero();
    }

    /**
     * The walk reads the codes, not reconstructed vectors: a BINARY search fills the code cache and leaves the float vector
     * cache empty, where it used to fill it with one float array per candidate it scored.
     */
    @Test
    void walkScoresTheCodesAndMaterializesNoVector() {
      final Corpus corpus = createCorpus("Codes", 64, 2000, 7);
      final LSMVectorIndex index = index("Codes");

      for (int q = 0; q < 20; q++) {
        final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(corpus.vectors.get(q * 50), 10, 100);
        assertThat(hits.getFirst().getFirst()).isEqualTo(corpus.rids.get(q * 50));
        assertThat(hits.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
      }

      assertThat(index.getSearchBinaryCodes().size()).isGreaterThan(0);
      assertThat(index.getSearchVectorCache().size()).isZero();
    }

    /**
     * Before, a beam wider than {@code 4 * k} was walked and then cut back to {@code 4 * k} candidates, so recall did not move
     * with efSearch.
     */
    @Test
    void widerBeamBuysRecall() {
      final Corpus corpus = createCorpus("Beam", 64, 3000, 13);
      final LSMVectorIndex index = index("Beam");

      final double narrow = recall(corpus, (query) -> index.findNeighbors(query, 10, 10, 1, null, WorkGuard.forCommandDeadline(null)));
      final double wide = recall(corpus, (query) -> index.findNeighborsFromVector(query, 10, 300));

      assertThat(wide).as("recall@10 with efSearch 300").isGreaterThan(0.85);
      assertThat(wide).as("recall@10 with efSearch 300 against 10 candidates").isGreaterThan(narrow + 0.1);
    }

    /** The default reranks {@code k * rerankOversample} candidates, the knob a PRODUCT index reranks by. */
    @Test
    void rerankOversampleSizesTheCandidates() {
      final Corpus corpus = createCorpus("Oversample", 64, 3000, 17);
      final LSMVectorIndex index = index("Oversample");

      final double one = recall(corpus, (query) -> index.findNeighbors(query, 10, -1, 1, null, WorkGuard.forCommandDeadline(null)));
      final double twenty = recall(corpus, (query) -> index.findNeighbors(query, 10, -1, 20, null, WorkGuard.forCommandDeadline(null)));
      final double byDefault = recall(corpus, (query) -> index.findNeighborsFromVector(query, 10, -1));
      final double byIndexSetting = recall(corpus, (query) -> index.findNeighbors(query, 10, -1, -1, null, WorkGuard.forCommandDeadline(null)));

      assertThat(twenty).isGreaterThan(one + 0.1);
      assertThat(byDefault).isEqualTo(byIndexSetting);
      assertThat(byDefault).isGreaterThan(one);
    }

    /** The {@code rerank} option of {@code vector.neighbors} reaches a BINARY index, as it reaches a PRODUCT one. */
    @Test
    void sqlRerankOptionSizesTheCandidates() {
      final Corpus corpus = createCorpus("Sql", 64, 3000, 31);

      final double one = recall(corpus, (query) -> sqlNeighbors(query, 1));
      final double twenty = recall(corpus, (query) -> sqlNeighbors(query, 20));

      assertThat(twenty).isGreaterThan(one + 0.1);
      final List<Pair<RID, Float>> self = sqlNeighbors(corpus.vectors.get(5), 20);
      assertThat(self.getFirst().getFirst()).isEqualTo(corpus.rids.get(5));
      assertThat(self.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
    }

    private List<Pair<RID, Float>> sqlNeighbors(final float[] query, final int rerank) {
      final List<Pair<RID, Float>> hits = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(`vector.neighbors`('Sql[vector]', ?, 10, { rerank: " + rerank + " }))", query)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          hits.add(new Pair<>(row.getIdentity().get(), row.<Float>getProperty("distance")));
        }
      }
      return hits;
    }

    /**
     * The rows the graph does not hold yet - committed since the last build, and the calling transaction's own - are scored
     * on their codes too, and the rerank gives each its exact distance.
     */
    @Test
    void deltaRowsAreFoundAtTheirExactDistance() {
      final Corpus corpus = createCorpus("Delta", 64, 1500, 19);
      final LSMVectorIndex index = index("Delta");
      index.findNeighborsFromVector(corpus.vectors.getFirst(), 10, 50); // builds the graph

      final Random random = new Random(23);
      final float[] committed = randomVector(random, 64, -1.0f);
      final RID[] committedRid = new RID[1];
      database.transaction(() -> committedRid[0] = database.newDocument("Delta").set("vector", committed).save().getIdentity());

      List<Pair<RID, Float>> hits = index.findNeighborsFromVector(committed, 5, 50);
      assertThat(hits.getFirst().getFirst()).isEqualTo(committedRid[0]);
      assertThat(hits.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));

      final float[] pending = randomVector(random, 64, -1.0f);
      database.begin();
      try {
        final RID pendingRid = database.newDocument("Delta").set("vector", pending).save().getIdentity();
        hits = index.findNeighborsFromVector(pending, 5, 50);
        assertThat(hits.getFirst().getFirst()).isEqualTo(pendingRid);
        assertThat(hits.getFirst().getSecond()).isCloseTo(0f, within(1e-5f));
      } finally {
        database.rollback();
      }
    }

    /** A deleted record leaves the answer and the code cache. */
    @Test
    void deletedRecordIsNeitherReturnedNorScored() {
      final Corpus corpus = createCorpus("Deleted", 64, 1500, 29);
      final LSMVectorIndex index = index("Deleted");
      final float[] query = corpus.vectors.get(100);
      assertThat(index.findNeighborsFromVector(query, 5, 50).getFirst().getFirst()).isEqualTo(corpus.rids.get(100));

      database.transaction(() -> corpus.rids.get(100).asDocument().delete());

      final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(query, 5, 50);
      assertThat(hits).hasSize(5);
      assertThat(hits).extracting(Pair::getFirst).doesNotContain(corpus.rids.get(100));
    }

    private interface Search {
      List<Pair<RID, Float>> run(float[] query);
    }

    private record Corpus(List<float[]> vectors, List<RID> rids) {
    }

    private Corpus createCorpus(final String type, final int dimensions, final int size, final long seed) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
      database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": " + dimensions
          + ", \"similarity\": \"COSINE\", \"quantization\": \"BINARY\" }");

      final Random random = new Random(seed);
      final List<float[]> vectors = new ArrayList<>(size);
      for (int i = 0; i < size; i++)
        vectors.add(randomVector(random, dimensions, -1.0f));
      final List<RID> rids = new ArrayList<>(size);
      database.transaction(() -> {
        for (final float[] vector : vectors)
          rids.add(database.newDocument(type).set("vector", vector).save().getIdentity());
      });
      return new Corpus(vectors, rids);
    }

    private static double recall(final Corpus corpus, final Search search) {
      int found = 0;
      final int queries = 50;
      for (int q = 0; q < queries; q++) {
        final float[] query = corpus.vectors.get(q * 37 + 1);
        final List<Integer> order = new ArrayList<>(corpus.vectors.size());
        for (int i = 0; i < corpus.vectors.size(); i++)
          order.add(i);
        order.sort(Comparator.comparingDouble(i -> -cosine(query, corpus.vectors.get(i))));
        final Set<RID> exact = new HashSet<>();
        for (int i = 0; i < 10; i++)
          exact.add(corpus.rids.get(order.get(i)));
        for (final Pair<RID, Float> hit : search.run(query))
          if (exact.contains(hit.getFirst()))
            found++;
      }
      return found / (double) (queries * 10);
    }

    private LSMVectorIndex index(final String type) {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[vector]")).getIndexesOnBuckets()[0];
    }

    /** The code of a vector as the index pages store it: the quantization of the SQL function, packed. */
    private static long[] code(final float[] vector) {
      final float median = VectorUtils.median(vector);
      final byte[] packed = new byte[(vector.length + 7) / 8];
      for (int i = 0; i < vector.length; i++)
        if (vector[i] >= median)
          packed[i / 8] |= (byte) (1 << (i % 8));
      final long[] code = new long[BinaryCodeCache.wordsFor(vector.length)];
      BinaryCodeCache.pack(packed, 0, vector.length, median, code, 0);
      return code;
    }

    private static float[] reconstruct(final float[] vector) {
      final float median = VectorUtils.median(vector);
      final byte[] packed = new byte[(vector.length + 7) / 8];
      for (int i = 0; i < vector.length; i++)
        if (vector[i] >= median)
          packed[i / 8] |= (byte) (1 << (i % 8));
      final float[] reconstructed = new float[vector.length];
      QuantizedBuildVectors.dequantizeBinary(packed, 0, vector.length, median, reconstructed);
      return reconstructed;
    }

    private static float[] randomVector(final Random random, final int dimensions, final float low) {
      final float[] vector = new float[dimensions];
      for (int i = 0; i < dimensions; i++)
        vector[i] = low + random.nextFloat() * (1.0f - low);
      return vector;
    }

    private static float[] constant(final int dimensions, final float value) {
      final float[] vector = new float[dimensions];
      Arrays.fill(vector, value);
      return vector;
    }

    private static double cosine(final float[] a, final float[] b) {
      double dot = 0, na = 0, nb = 0;
      for (int i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
      }
      return dot / Math.sqrt(na * nb);
    }
  }

  /**
   * Regression for #11298: binary rerank reads in place.
   * <p>
   * Issue #11298: the rerank of a BINARY search read every candidate's vector through the record layer, a lazy record looked
   * up and then loaded by a second lookup, a copy of the whole record and a fresh {@code float[]} per candidate, for the 100
   * candidates a default search reranks. The vector is now read where it lies on its bucket page, into one scratch vector
   * per search, and the record layer answers only the shapes that read cannot vouch for: a record the transaction wrote, a
   * type with a read listener, a record that does not lie whole in its slot, a value that is not a float array.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BinaryRerankReadsInPlace extends TestHelper {
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
     * {@code VectorSearchCacheTest.CompactionSearchCache}). The in-place read leaves that read to the record layer instead of failing on
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

  /**
   * Regression for #8960: BINARY quantization keeps the sign of zero-centred data.
   * <p>
   * Issue #8960: BINARY quantization reconstructed a vector as {@code bit ? median : 0}, which loses the sign of data
   * centred on zero, so a record queried by its own vector came back on the far side of the sphere (distance 1.46) and
   * behind other records; recall@10 on random vectors was 0.19 where INT8 had 0.98.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BinaryQuantizationSelfMatch extends TestHelper {

    @Test
    void recordQueriedByItsOwnVectorComesFirstAtZeroDistance() {
      final String type = createType("SmallBinary", 4);
      database.transaction(() -> {
        database.newDocument(type).set("name", "A", "vector", new float[] { 0.9f, -0.1f, -0.2f, -0.8f }).save();
        database.newDocument(type).set("name", "B", "vector", new float[] { 0.9f, 0.5f, 0.1f, 0.2f }).save();
      });

      final List<Pair<RID, Float>> hits = index(type).findNeighborsFromVector(new float[] { 0.9f, -0.1f, -0.2f, -0.8f }, 2, 100);

      assertThat(hits).hasSize(2);
      assertThat(hits.get(0).getFirst().asDocument().getString("name")).isEqualTo("A");
      assertThat(hits.get(0).getSecond()).isCloseTo(0f, within(1e-5f));
      assertThat(hits.get(1).getFirst().asDocument().getString("name")).isEqualTo("B");
      assertThat(hits.get(1).getSecond()).isCloseTo(0.5505f, within(1e-3f));
    }

    @Test
    void nonZeroVectorWithZeroMedianIsNotStoredAsTheZeroVector() {
      final String type = createType("SparseBinary", 4);
      database.transaction(() -> {
        database.newDocument(type).set("name", "sparse", "vector", new float[] { 0f, 0f, 0f, 5f }).save();
        database.newDocument(type).set("name", "other", "vector", new float[] { 1f, 1f, 1f, -1f }).save();
      });

      final List<Pair<RID, Float>> hits = index(type).findNeighborsFromVector(new float[] { 0f, 0f, 0f, 5f }, 2, 100);

      assertThat(hits).hasSize(2);
      assertThat(hits.get(0).getFirst().asDocument().getString("name")).isEqualTo("sparse");
    }

    @Test
    void kLargerThanTheIndexReturnsEveryRecord() {
      final String type = createType("TinyBinary", 4);
      database.transaction(() -> {
        database.newDocument(type).set("name", "A", "vector", new float[] { 0.9f, -0.1f, -0.2f, -0.8f }).save();
        database.newDocument(type).set("name", "B", "vector", new float[] { 0.9f, 0.5f, 0.1f, 0.2f }).save();
      });

      assertThat(index(type).findNeighborsFromVector(new float[] { 0.9f, -0.1f, -0.2f, -0.8f }, 50, 100)).hasSize(2);
    }

    @Test
    void ownVectorFirstUnderEuclideanAndDotProduct() {
      for (final String similarity : List.of("EUCLIDEAN", "DOT_PRODUCT")) {
        final String type = "Bin" + similarity;
        database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
        database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": 4, \"similarity\": \""
            + similarity + "\", \"quantization\": \"BINARY\" }");
        database.transaction(() -> {
          database.newDocument(type).set("name", "A", "vector", new float[] { 0.5f, -0.5f, 0.5f, -0.5f }).save();
          database.newDocument(type).set("name", "B", "vector", new float[] { 0.5f, 0.5f, 0.5f, 0.5f }).save();
          database.newDocument(type).set("name", "C", "vector", new float[] { -0.5f, 0.5f, -0.5f, 0.5f }).save();
        });

        final List<Pair<RID, Float>> hits = index(type).findNeighborsFromVector(new float[] { 0.5f, -0.5f, 0.5f, -0.5f }, 3, 100);

        assertThat(hits).as(similarity).hasSize(3);
        assertThat(hits.get(0).getFirst().asDocument().getString("name")).as(similarity).isEqualTo("A");
        assertThat(hits.get(2).getFirst().asDocument().getString("name")).as(similarity).isEqualTo("C");
      }
    }

    @Test
    void ownVectorFirstAndUsableRecallOnRandomVectors() {
      final Random random = new Random(42);
      final List<float[]> vectors = new ArrayList<>();
      for (int i = 0; i < 2000; i++) {
        final float[] v = new float[64];
        for (int d = 0; d < 64; d++)
          v[d] = random.nextFloat() * 2 - 1;
        vectors.add(v);
      }

      final String type = createType("DocBinary", 64);
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (final float[] v : vectors)
          rids.add(database.newDocument(type).set("vector", v).save().getIdentity());
      });

      int selfFirst = 0;
      int hitsInExactTop10 = 0;
      for (int q = 0; q < 100; q++) {
        final float[] query = vectors.get(q * 20);
        final List<Pair<RID, Float>> hits = index(type).findNeighborsFromVector(query, 10, 200);
        if (hits.get(0).getFirst().equals(rids.get(q * 20)))
          selfFirst++;

        final List<Integer> order = new ArrayList<>();
        for (int i = 0; i < vectors.size(); i++)
          order.add(i);
        order.sort(Comparator.comparingDouble(i -> -cosine(query, vectors.get(i))));
        final Set<RID> exactTop10 = new HashSet<>();
        for (int i = 0; i < 10; i++)
          exactTop10.add(rids.get(order.get(i)));
        for (final Pair<RID, Float> hit : hits)
          if (exactTop10.contains(hit.getFirst()))
            hitsInExactTop10++;
      }

      assertThat(selfFirst).isEqualTo(100);
      assertThat(hitsInExactTop10 / 1000.0).as("recall@10").isGreaterThan(0.6);
    }

    private String createType(final String type, final int dimensions) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
      database.command("sql", "CREATE PROPERTY " + type + ".vector ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON " + type + " (vector) LSM_VECTOR METADATA { \"dimensions\": " + dimensions
          + ", \"similarity\": \"COSINE\", \"quantization\": \"BINARY\" }");
      return type;
    }

    private LSMVectorIndex index(final String type) {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[vector]")).getIndexesOnBuckets()[0];
    }

    private static double cosine(final float[] a, final float[] b) {
      double dot = 0, na = 0, nb = 0;
      for (int i = 0; i < a.length; i++) {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
      }
      return dot / Math.sqrt(na * nb);
    }
  }

  /**
   * Regression for #9396: a BINARY search does not answer empty while commits delete the nearest records.
   * <p>
   * Issue #9396: a BINARY quantized index answered an EMPTY list while another thread committed transactions that
   * deleted the records nearest to the query and inserted new ones near it. The search fetches an oversampled
   * candidate list and reranks it on the stored vectors, dropping the candidates whose record is gone: a commit landing
   * between the two steps deleted exactly those candidates, so every one of them was dropped although 3,000 records
   * were live at every instant.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BinaryRerankEmptyUnderCommits extends TestHelper {
    private static final int DIM = 32;

    @Test
    void searchNeverAnswersFewerThanKWhileTheNearestRecordsAreReplaced() throws Exception {
      final Random r = new Random(500);
      database.command("sql", "CREATE DOCUMENT TYPE Product BUCKETS 1");
      database.command("sql", "CREATE PROPERTY Product.pid INTEGER");
      database.command("sql", "CREATE PROPERTY Product.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON Product (pid) UNIQUE_HASH");
      final Map<Long, float[]> live = new HashMap<>();
      database.begin();
      for (int i = 0; i < 3000; i++) {
        final float[] v = unit(r);
        database.newDocument("Product").set("pid", (long) i, "embedding", v).save();
        live.put((long) i, v);
      }
      database.commit();
      database.command("sql", "CREATE INDEX ON Product (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIM
          + ", \"similarity\": \"EUCLIDEAN\", \"beamWidth\": 100, \"quantization\": \"BINARY\" }");

      final LSMVectorIndex lsm = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Product[embedding]")).getIndexesOnBuckets()[0];
      final float[] q = unit(new Random(4242));
      final int[] ks = { 1, 5, 10 };

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicLong searches = new AtomicLong();
      final AtomicLong shortAnswers = new AtomicLong();
      final Thread reader = new Thread(() -> {
        int step = 0;
        while (!stop.get()) {
          final int k = ks[step++ % ks.length];
          if (lsm.findNeighborsFromVector(q, k, 100).size() < k)
            shortAnswers.incrementAndGet();
          searches.incrementAndGet();
        }
      });
      reader.setDaemon(true);
      reader.start();
      try {
        long nextPid = 100000;
        for (int round = 0; round < 40; round++) {
          final List<Long> pids = new ArrayList<>(live.keySet());
          pids.sort(Comparator.comparingDouble(p -> dist(live.get(p), q)));
          final List<Long> victims = new ArrayList<>(pids.subList(0, 100));
          database.begin();
          database.command("sql", "DELETE FROM Product WHERE pid IN :ids", Map.of("ids", victims)).close();
          for (final long p : victims)
            live.remove(p);
          for (int i = 0; i < 100; i++) {
            final float[] v = near(q, r);
            database.newDocument("Product").set("pid", nextPid, "embedding", v).save();
            live.put(nextPid++, v);
          }
          database.commit();
        }
      } finally {
        stop.set(true);
        reader.join();
      }

      assertThat(searches.get()).isPositive();
      assertThat(shortAnswers.get()).as("searches with fewer than k RIDs out of " + searches.get()).isZero();
    }

    private static float[] unit(final Random r) {
      final float[] v = new float[DIM];
      double s = 0;
      for (int i = 0; i < DIM; i++) {
        v[i] = (float) r.nextGaussian();
        s += v[i] * v[i];
      }
      s = Math.sqrt(s);
      for (int i = 0; i < DIM; i++)
        v[i] /= (float) s;
      return v;
    }

    private static float[] near(final float[] q, final Random r) {
      final float[] v = new float[DIM];
      double s = 0;
      for (int i = 0; i < DIM; i++) {
        v[i] = (float) (q[i] + 0.05 * r.nextGaussian());
        s += v[i] * v[i];
      }
      s = Math.sqrt(s);
      for (int i = 0; i < DIM; i++)
        v[i] /= (float) s;
      return v;
    }

    private static double dist(final float[] a, final float[] b) {
      double s = 0;
      for (int i = 0; i < DIM; i++)
        s += (a[i] - b[i]) * (a[i] - b[i]);
      return s;
    }
  }
}
