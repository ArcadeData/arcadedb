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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.utility.Pair;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #10968: a BINARY index scored its graph walk on a float reconstruction of every candidate, rebuilt from the
 * one-bit row and cached as a {@code float[dimensions]}, so it did the work of an unquantized index per candidate and
 * was slower than one; and it reranked a fixed {@code 4 * k} candidates, so a wider efSearch cost more and found nothing
 * more. The walk now scores the codes with a Hamming distance, and the rerank takes {@code k * rerankOversample}
 * candidates, or the whole beam when efSearch asks for more.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10968BinaryCodeSearchTest extends TestHelper {
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
