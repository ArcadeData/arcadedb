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
package com.arcadedb.bench.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SplittableRandom;

/**
 * Deterministic data of the workloads: the same seed always gives the same rows, vectors and text.
 */
public final class DatasetGenerator {
  public static final String CSV_HEADER = "id,name,age,city,score";

  private static final String[] CITIES = { "Rome", "Milan", "Paris", "London", "Berlin", "Madrid", "Lisbon", "Vienna", "Oslo",
      "Dublin", "Prague", "Warsaw" };

  private static volatile String[] words;

  private final long seed;

  public DatasetGenerator(final long seed) {
    this.seed = seed;
  }

  /**
   * The header line followed by {@code n} rows of the columns {@code id,name,age,city,score}. Each call restarts the sequence.
   */
  public Iterator<String> csvRows(final int n) {
    final SplittableRandom random = new SplittableRandom(seed);
    return new Iterator<>() {
      private int next = -1;

      @Override
      public boolean hasNext() {
        return next < n;
      }

      @Override
      public String next() {
        if (next >= n)
          throw new NoSuchElementException();
        if (next < 0) {
          next++;
          return CSV_HEADER;
        }
        final int id = next++;
        return id + ",name" + random.nextInt(1_000_000) + "," + (18 + random.nextInt(63)) + "," + CITIES[random.nextInt(CITIES.length)]
            + "," + (random.nextInt(1_000_000) / 100.0);
      }
    };
  }

  /**
   * {@code n} unit-normalized vectors of {@code dim} components, drawn from a Gaussian so the directions are uniform.
   */
  public float[][] vectors(final int n, final int dim) {
    final SplittableRandom random = new SplittableRandom(seed);
    final float[][] result = new float[n][dim];
    final double[] tmp = new double[dim];
    for (int i = 0; i < n; i++) {
      double sum = 0;
      for (int j = 0; j < dim; j++) {
        final double x = gaussian(random);
        tmp[j] = x;
        sum += x * x;
      }
      final double norm = StrictMath.sqrt(sum);
      final float[] v = result[i];
      for (int j = 0; j < dim; j++)
        v[j] = (float) (tmp[j] / norm);
    }
    return result;
  }

  /** Clusters of an {@link EmbeddingSpace}. */
  static final int    EMBEDDING_CLUSTERS = 64;
  /** Dimensions the points of a cluster actually vary in, whatever the dimension of the vectors. */
  static final int    EMBEDDING_LATENT   = 16;
  /** Typical distance of a point from its cluster center, the centers being unit vectors. */
  static final double EMBEDDING_SPREAD   = 0.5;
  /** Typical length of the isotropic noise added to every point, in all the dimensions. */
  static final double EMBEDDING_NOISE    = 0.05;

  /**
   * The shape shared by a set of embeddings and the queries run against them: cluster centers and the projection of the latent
   * space into the vector space. Base vectors and queries must be drawn from the same space.
   */
  public record EmbeddingSpace(float[][] centers, float[][] projection) {
    public int dimensions() {
      return projection.length;
    }
  }

  /**
   * An embedding space of {@code dim} dimensions: {@link #EMBEDDING_CLUSTERS} uniform unit centers, and a random projection
   * from {@link #EMBEDDING_LATENT} latent dimensions scaled so a latent point lands about {@link #EMBEDDING_SPREAD} from its
   * center.
   */
  public EmbeddingSpace embeddingSpace(final int dim) {
    final float[][] centers = vectors(EMBEDDING_CLUSTERS, dim);
    final SplittableRandom random = new SplittableRandom(seed + 1);
    final double scale = EMBEDDING_SPREAD / StrictMath.sqrt((double) dim * EMBEDDING_LATENT);
    final float[][] projection = new float[dim][EMBEDDING_LATENT];
    for (final float[] row : projection)
      for (int k = 0; k < EMBEDDING_LATENT; k++)
        row[k] = (float) (gaussian(random) * scale);
    return new EmbeddingSpace(centers, projection);
  }

  /**
   * {@code n} unit-normalized vectors drawn like real embeddings rather than uniformly: a random center of {@code space}, plus
   * a Gaussian latent point projected into the vector space, plus a little isotropic noise. Uniform directions in a high
   * dimension are all nearly orthogonal, so their nearest neighbors are barely nearer than any other point; these vectors
   * have a low intrinsic dimension and neighbors an approximate index can find, as embeddings do.
   */
  public float[][] embeddings(final int n, final EmbeddingSpace space) {
    final int dim = space.dimensions();
    final SplittableRandom random = new SplittableRandom(seed);
    final double noise = EMBEDDING_NOISE / StrictMath.sqrt(dim);
    final float[][] result = new float[n][dim];
    final double[] latent = new double[EMBEDDING_LATENT];
    final double[] tmp = new double[dim];
    for (int i = 0; i < n; i++) {
      final float[] center = space.centers()[random.nextInt(space.centers().length)];
      for (int k = 0; k < EMBEDDING_LATENT; k++)
        latent[k] = gaussian(random);
      double sum = 0;
      for (int j = 0; j < dim; j++) {
        final float[] row = space.projection()[j];
        double x = center[j] + gaussian(random) * noise;
        for (int k = 0; k < EMBEDDING_LATENT; k++)
          x += row[k] * latent[k];
        tmp[j] = x;
        sum += x * x;
      }
      final double norm = StrictMath.sqrt(sum);
      final float[] v = result[i];
      for (int j = 0; j < dim; j++)
        v[j] = (float) (tmp[j] / norm);
    }
    return result;
  }

  /**
   * {@code words} space-separated words of {@code english_words.txt}.
   */
  public String text(final int words) {
    final String[] dictionary = dictionary();
    final SplittableRandom random = new SplittableRandom(seed);
    final StringBuilder sb = new StringBuilder(words * 8);
    for (int i = 0; i < words; i++) {
      if (i > 0)
        sb.append(' ');
      sb.append(dictionary[random.nextInt(dictionary.length)]);
    }
    return sb.toString();
  }

  /** The terms of the broad full-text corpus: few enough that a query of {@link #BROAD_QUERY_TERMS} of them matches about 45%. */
  static final String[] BROAD_VOCABULARY = broadVocabulary();
  static final int      BROAD_QUERY_TERMS = 6;
  private static final int BROAD_DELETED_PERCENT = 10;

  private static String[] broadVocabulary() {
    final String[] result = new String[100];
    for (int i = 0; i < result.length; i++)
      result[i] = "tk" + (i < 10 ? "0" : "") + i;
    return result;
  }

  /** {@code words} terms drawn uniformly, with repetition, from {@link #BROAD_VOCABULARY}. */
  public String broadText(final int words) {
    final SplittableRandom random = new SplittableRandom(seed);
    final StringBuilder sb = new StringBuilder(words * 5);
    for (int i = 0; i < words; i++) {
      if (i > 0)
        sb.append(' ');
      sb.append(BROAD_VOCABULARY[random.nextInt(BROAD_VOCABULARY.length)]);
    }
    return sb.toString();
  }

  /** An OR of {@link #BROAD_QUERY_TERMS} distinct terms of the broad vocabulary. */
  public static String broadQuery(final SplittableRandom random) {
    final boolean[] used = new boolean[BROAD_VOCABULARY.length];
    final StringBuilder sb = new StringBuilder(BROAD_QUERY_TERMS * 8);
    for (int n = 0; n < BROAD_QUERY_TERMS; ) {
      final int term = random.nextInt(BROAD_VOCABULARY.length);
      if (used[term])
        continue;
      used[term] = true;
      if (n++ > 0)
        sb.append(" OR ");
      sb.append(BROAD_VOCABULARY[term]);
    }
    return sb.toString();
  }

  /** Whether the post with this number has {@code deleted_at} set: about {@value #BROAD_DELETED_PERCENT}% of them. */
  public static boolean broadDeleted(final long seed, final int post) {
    return new SplittableRandom(seed * 31 + post).nextInt(100) < BROAD_DELETED_PERCENT;
  }

  // Box-Muller
  private static double gaussian(final SplittableRandom random) {
    final double u1 = 1.0 - random.nextDouble();
    final double u2 = random.nextDouble();
    return StrictMath.sqrt(-2.0 * StrictMath.log(u1)) * StrictMath.cos(2 * Math.PI * u2);
  }

  private static String[] dictionary() {
    String[] result = words;
    if (result == null) {
      synchronized (DatasetGenerator.class) {
        result = words;
        if (result == null)
          words = result = loadWords();
      }
    }
    return result;
  }

  private static String[] loadWords() {
    try (final InputStream in = DatasetGenerator.class.getResourceAsStream("/english_words.txt")) {
      if (in == null)
        throw new IllegalStateException("english_words.txt not found on the classpath (load-tests test-jar)");
      final List<String> list = new ArrayList<>();
      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          final String word = line.trim();
          if (!word.isEmpty())
            list.add(word);
        }
      }
      return list.toArray(new String[0]);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
