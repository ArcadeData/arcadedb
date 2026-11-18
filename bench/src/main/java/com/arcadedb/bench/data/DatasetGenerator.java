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
